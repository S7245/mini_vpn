use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    io,
    io::IoSliceMut,
    mem,
    net::{SocketAddr, SocketAddrV6},
    pin::Pin,
    str,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

#[cfg(all(not(wasm_browser), any(feature = "aws-lc-rs", feature = "ring")))]
use crate::runtime::default_runtime;
use crate::{
    runtime::{AsyncUdpSocket, Runtime},
    udp_transmit, Instant,
};
use bytes::{Bytes, BytesMut};
use pin_project_lite::pin_project;
use proto::{
    self as proto, ClientConfig, ConnectError, ConnectionError, ConnectionHandle, DatagramEvent,
    ServerConfig,
};
use rustc_hash::{FxHashMap, FxHashSet};
#[cfg(all(not(wasm_browser), any(feature = "aws-lc-rs", feature = "ring"),))]
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::{futures::Notified, mpsc, Notify};
use tracing::{Instrument, Span};
use udp::{RecvMeta, BATCH_SIZE};

use crate::{
    connection::Connecting, incoming::Incoming, work_limiter::WorkLimiter, ConnectionEvent,
    ConnectionToEndpointEvent, EndpointConfig, VarInt, IO_LOOP_BOUND, RECV_TIME_BOUND,
};

/// A QUIC endpoint.
///
/// An endpoint corresponds to a single UDP socket, may host many connections, and may act as both
/// client and server for different connections.
///
/// May be cloned to obtain another handle to the same endpoint.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub(crate) inner: EndpointRef,
    pub(crate) default_client_config: Option<ClientConfig>,
    runtime: Arc<dyn Runtime>,
}

impl Endpoint {
    /// Helper to construct an endpoint for use with outgoing connections only
    ///
    /// Note that `addr` is the *local* address to bind to, which should usually be a wildcard
    /// address like `0.0.0.0:0` or `[::]:0`, which allow communication with any reachable IPv4 or
    /// IPv6 address respectively from an OS-assigned port.
    ///
    /// If an IPv6 address is provided, attempts to make the socket dual-stack so as to allow
    /// communication with both IPv4 and IPv6 addresses. As such, calling `Endpoint::client` with
    /// the address `[::]:0` is a reasonable default to maximize the ability to connect to other
    /// address. For example:
    ///
    /// ```
    /// quinn::Endpoint::client((std::net::Ipv6Addr::UNSPECIFIED, 0).into());
    /// ```
    ///
    /// Some environments may not allow creation of dual-stack sockets, in which case an IPv6
    /// client will only be able to connect to IPv6 servers. An IPv4 client is never dual-stack.
    #[cfg(all(not(wasm_browser), any(feature = "aws-lc-rs", feature = "ring")))] // `EndpointConfig::default()` is only available with these
    pub fn client(addr: SocketAddr) -> io::Result<Self> {
        let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
        if addr.is_ipv6() {
            if let Err(e) = socket.set_only_v6(false) {
                tracing::debug!(%e, "unable to make socket dual-stack");
            }
        }
        socket.bind(&addr.into())?;
        let runtime =
            default_runtime().ok_or_else(|| io::Error::other("no async runtime found"))?;
        Self::new_with_abstract_socket(
            EndpointConfig::default(),
            None,
            runtime.wrap_udp_socket(socket.into())?,
            runtime,
        )
    }

    /// Returns relevant stats from this Endpoint
    pub fn stats(&self) -> EndpointStats {
        self.inner.state.lock().unwrap().stats
    }

    /// Return endpoint pacing conservation state when the optional service is configured.
    #[doc(hidden)]
    pub fn endpoint_pacing_snapshot(&self) -> Option<proto::EndpointPacingSnapshot> {
        self.inner
            .state
            .lock()
            .unwrap()
            .inner
            .endpoint_pacing_snapshot()
    }

    /// Helper to construct an endpoint for use with both incoming and outgoing connections
    ///
    /// Platform defaults for dual-stack sockets vary. For example, any socket bound to a wildcard
    /// IPv6 address on Windows will not by default be able to communicate with IPv4
    /// addresses. Portable applications should bind an address that matches the family they wish to
    /// communicate within.
    #[cfg(all(not(wasm_browser), any(feature = "aws-lc-rs", feature = "ring")))] // `EndpointConfig::default()` is only available with these
    pub fn server(config: ServerConfig, addr: SocketAddr) -> io::Result<Self> {
        let socket = std::net::UdpSocket::bind(addr)?;
        let runtime =
            default_runtime().ok_or_else(|| io::Error::other("no async runtime found"))?;
        Self::new_with_abstract_socket(
            EndpointConfig::default(),
            Some(config),
            runtime.wrap_udp_socket(socket)?,
            runtime,
        )
    }

    /// Construct an endpoint with arbitrary configuration and socket
    #[cfg(not(wasm_browser))]
    pub fn new(
        config: EndpointConfig,
        server_config: Option<ServerConfig>,
        socket: std::net::UdpSocket,
        runtime: Arc<dyn Runtime>,
    ) -> io::Result<Self> {
        let socket = runtime.wrap_udp_socket(socket)?;
        Self::new_with_abstract_socket(config, server_config, socket, runtime)
    }

    /// Construct an endpoint with arbitrary configuration and pre-constructed abstract socket
    ///
    /// Useful when `socket` has additional state (e.g. sidechannels) attached for which shared
    /// ownership is needed.
    pub fn new_with_abstract_socket(
        config: EndpointConfig,
        server_config: Option<ServerConfig>,
        socket: Arc<dyn AsyncUdpSocket>,
        runtime: Arc<dyn Runtime>,
    ) -> io::Result<Self> {
        let addr = socket.local_addr()?;
        let allow_mtud = !socket.may_fragment();
        let rc = EndpointRef::new(
            socket,
            proto::Endpoint::new(
                Arc::new(config),
                server_config.map(Arc::new),
                allow_mtud,
                None,
            ),
            addr.is_ipv6(),
            runtime.clone(),
        );
        let driver = EndpointDriver(rc.clone());
        runtime.spawn(Box::pin(
            async {
                if let Err(e) = driver.await {
                    tracing::error!("I/O error: {}", e);
                }
            }
            .instrument(Span::current()),
        ));
        Ok(Self {
            inner: rc,
            default_client_config: None,
            runtime,
        })
    }

    /// Get the next incoming connection attempt from a client
    ///
    /// Yields [`Incoming`]s, or `None` if the endpoint is [`close`](Self::close)d. [`Incoming`]
    /// can be `await`ed to obtain the final [`Connection`](crate::Connection), or used to e.g.
    /// filter connection attempts or force address validation, or converted into an intermediate
    /// `Connecting` future which can be used to e.g. send 0.5-RTT data.
    pub fn accept(&self) -> Accept<'_> {
        Accept {
            endpoint: self,
            notify: self.inner.shared.incoming.notified(),
        }
    }

    /// Set the client configuration used by `connect`
    pub fn set_default_client_config(&mut self, config: ClientConfig) {
        self.default_client_config = Some(config);
    }

    /// Connect to a remote endpoint
    ///
    /// `server_name` must be covered by the certificate presented by the server. This prevents a
    /// connection from being intercepted by an attacker with a valid certificate for some other
    /// server.
    ///
    /// May fail immediately due to configuration errors, or in the future if the connection could
    /// not be established.
    pub fn connect(&self, addr: SocketAddr, server_name: &str) -> Result<Connecting, ConnectError> {
        let config = match &self.default_client_config {
            Some(config) => config.clone(),
            None => return Err(ConnectError::NoDefaultClientConfig),
        };

        self.connect_with(config, addr, server_name)
    }

    /// Connect to a remote endpoint using a custom configuration.
    ///
    /// See [`connect()`] for details.
    ///
    /// [`connect()`]: Endpoint::connect
    pub fn connect_with(
        &self,
        config: ClientConfig,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<Connecting, ConnectError> {
        let mut endpoint = self.inner.state.lock().unwrap();
        if endpoint.driver_lost || endpoint.recv_state.connections.close.is_some() {
            return Err(ConnectError::EndpointStopping);
        }
        if addr.is_ipv6() && !endpoint.ipv6 {
            return Err(ConnectError::InvalidRemoteAddress(addr));
        }
        let addr = if endpoint.ipv6 {
            SocketAddr::V6(ensure_ipv6(addr))
        } else {
            addr
        };

        let (ch, conn) = endpoint
            .inner
            .connect(self.runtime.now(), config, addr, server_name)?;

        let socket = endpoint.socket.clone();
        endpoint.stats.outgoing_handshakes += 1;
        Ok(endpoint
            .recv_state
            .connections
            .insert(ch, conn, socket, self.runtime.clone()))
    }

    /// Switch to a new UDP socket
    ///
    /// See [`Endpoint::rebind_abstract()`] for details.
    #[cfg(not(wasm_browser))]
    pub fn rebind(&self, socket: std::net::UdpSocket) -> io::Result<()> {
        self.rebind_abstract(self.runtime.wrap_udp_socket(socket)?)
    }

    /// Switch to a new UDP socket
    ///
    /// Allows the endpoint's address to be updated live, affecting all active connections. Incoming
    /// connections and connections to servers unreachable from the new address will be lost.
    ///
    /// On error, the old UDP socket is retained.
    pub fn rebind_abstract(&self, socket: Arc<dyn AsyncUdpSocket>) -> io::Result<()> {
        let addr = socket.local_addr()?;
        let mut inner = self.inner.state.lock().unwrap();
        let previous_socket = mem::replace(&mut inner.socket, socket);
        let rebind_generation = inner.stats.socket_rebinds.saturating_add(1);
        inner.prev_socket_connections = PreviousSocketConnections::at_rebind(
            rebind_generation,
            inner.recv_state.connections.senders.keys().copied(),
        );
        inner.prev_socket = inner
            .prev_socket_connections
            .must_retain_previous_socket()
            .then_some(previous_socket);
        inner.ipv6 = addr.is_ipv6();
        inner.stats.socket_rebinds = rebind_generation;

        // Update connection socket references
        for sender in inner.recv_state.connections.senders.values() {
            // Ignoring errors from dropped connections
            let _ = sender.send(ConnectionEvent::Rebind(inner.socket.clone()));
        }
        if let Some(driver) = inner.driver.take() {
            // Ensure the driver can register for wake-ups from the new socket
            driver.wake();
        }

        Ok(())
    }

    /// Replace the server configuration, affecting new incoming connections only
    ///
    /// Useful for e.g. refreshing TLS certificates without disrupting existing connections.
    pub fn set_server_config(&self, server_config: Option<ServerConfig>) {
        self.inner
            .state
            .lock()
            .unwrap()
            .inner
            .set_server_config(server_config.map(Arc::new))
    }

    /// Get the local `SocketAddr` the underlying socket is bound to
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.state.lock().unwrap().socket.local_addr()
    }

    /// Get the number of connections that are currently open
    pub fn open_connections(&self) -> usize {
        self.inner.state.lock().unwrap().inner.open_connections()
    }

    /// Close all of this endpoint's connections immediately and cease accepting new connections.
    ///
    /// See [`Connection::close()`] for details.
    ///
    /// [`Connection::close()`]: crate::Connection::close
    pub fn close(&self, error_code: VarInt, reason: &[u8]) {
        let reason = Bytes::copy_from_slice(reason);
        let mut endpoint = self.inner.state.lock().unwrap();
        endpoint.recv_state.connections.close = Some((error_code, reason.clone()));
        for sender in endpoint.recv_state.connections.senders.values() {
            // Ignoring errors from dropped connections
            let _ = sender.send(ConnectionEvent::Close {
                error_code,
                reason: reason.clone(),
            });
        }
        self.inner.shared.incoming.notify_waiters();
    }

    /// Wait for all connections on the endpoint to be cleanly shut down
    ///
    /// Waiting for this condition before exiting ensures that a good-faith effort is made to notify
    /// peers of recent connection closes, whereas exiting immediately could force them to wait out
    /// the idle timeout period.
    ///
    /// Does not proactively close existing connections or cause incoming connections to be
    /// rejected. Consider calling [`close()`] if that is desired.
    ///
    /// [`close()`]: Endpoint::close
    pub async fn wait_idle(&self) {
        loop {
            {
                let endpoint = &mut *self.inner.state.lock().unwrap();
                if endpoint.recv_state.connections.is_empty() {
                    break;
                }
                // Construct future while lock is held to avoid race
                self.inner.shared.idle.notified()
            }
            .await;
        }
    }
}

/// Statistics on [Endpoint] activity
#[non_exhaustive]
#[derive(Debug, Default, Copy, Clone)]
pub struct EndpointStats {
    /// Cummulative number of Quic handshakes accepted by this [Endpoint]
    pub accepted_handshakes: u64,
    /// Cummulative number of Quic handshakees sent from this [Endpoint]
    pub outgoing_handshakes: u64,
    /// Cummulative number of Quic handshakes refused on this [Endpoint]
    pub refused_handshakes: u64,
    /// Cummulative number of Quic handshakes ignored on this [Endpoint]
    pub ignored_handshakes: u64,
    /// Cumulative number of successful live UDP socket rebinds.
    pub socket_rebinds: u64,
    /// Latest rebind generation for which a connection authenticated a packet from the current
    /// socket.
    ///
    /// Packets received on the temporarily retained previous socket do not advance this value.
    pub current_socket_rx_rebind_generation: u64,
}

/// A future that drives IO on an endpoint
///
/// This task functions as the switch point between the UDP socket object and the
/// `Endpoint` responsible for routing datagrams to their owning `Connection`.
/// In order to do so, it also facilitates the exchange of different types of events
/// flowing between the `Endpoint` and the tasks managing `Connection`s. As such,
/// running this task is necessary to keep the endpoint's connections running.
///
/// `EndpointDriver` futures terminate when all clones of the `Endpoint` have been dropped, or when
/// an I/O error occurs.
#[must_use = "endpoint drivers must be spawned for I/O to occur"]
#[derive(Debug)]
pub(crate) struct EndpointDriver(pub(crate) EndpointRef);

impl Future for EndpointDriver {
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut endpoint = self.0.state.lock().unwrap();
        if endpoint.driver.is_none() {
            endpoint.driver = Some(cx.waker().clone());
        }

        let now = endpoint.runtime.now();
        let mut keep_going = false;
        keep_going |= endpoint.drive_recv(cx, now)?;
        keep_going |= endpoint.handle_events(cx, &self.0.shared);

        if !endpoint.recv_state.incoming.is_empty() {
            self.0.shared.incoming.notify_waiters();
        }

        if self.0.shared.ref_count.load(Ordering::Relaxed) == 0
            && endpoint.recv_state.connections.is_empty()
        {
            Poll::Ready(Ok(()))
        } else {
            drop(endpoint);
            // If there is more work to do schedule the endpoint task again.
            // `wake_by_ref()` is called outside the lock to minimize
            // lock contention on a multithreaded runtime.
            if keep_going {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}

impl Drop for EndpointDriver {
    fn drop(&mut self) {
        let mut endpoint = self.0.state.lock().unwrap();
        endpoint.driver_lost = true;
        self.0.shared.incoming.notify_waiters();
        // Drop all outgoing channels, signaling the termination of the endpoint to the associated
        // connections.
        endpoint.recv_state.connections.senders.clear();
    }
}

#[derive(Debug)]
pub(crate) struct EndpointInner {
    pub(crate) state: Mutex<State>,
    pub(crate) shared: Shared,
}

impl EndpointInner {
    pub(crate) fn accept(
        &self,
        incoming: proto::Incoming,
        server_config: Option<Arc<ServerConfig>>,
    ) -> Result<Connecting, ConnectionError> {
        let mut state = self.state.lock().unwrap();
        let mut response_buffer = Vec::new();
        let now = state.runtime.now();
        match state
            .inner
            .accept(incoming, now, &mut response_buffer, server_config)
        {
            Ok((handle, conn)) => {
                state.stats.accepted_handshakes += 1;
                let socket = state.socket.clone();
                let runtime = state.runtime.clone();
                Ok(state
                    .recv_state
                    .connections
                    .insert(handle, conn, socket, runtime))
            }
            Err(error) => {
                if let Some(transmit) = error.response {
                    let socket = state.socket.clone();
                    respond(transmit, &response_buffer, &*socket, &state.inner, now);
                }
                Err(error.cause)
            }
        }
    }

    pub(crate) fn refuse(&self, incoming: proto::Incoming) {
        let mut state = self.state.lock().unwrap();
        state.stats.refused_handshakes += 1;
        let now = state.runtime.now();
        let mut response_buffer = Vec::new();
        let transmit = state.inner.refuse(incoming, &mut response_buffer);
        let socket = state.socket.clone();
        respond(transmit, &response_buffer, &*socket, &state.inner, now);
    }

    pub(crate) fn retry(&self, incoming: proto::Incoming) -> Result<(), proto::RetryError> {
        let mut state = self.state.lock().unwrap();
        let now = state.runtime.now();
        let mut response_buffer = Vec::new();
        let transmit = state.inner.retry(incoming, &mut response_buffer)?;
        let socket = state.socket.clone();
        respond(transmit, &response_buffer, &*socket, &state.inner, now);
        Ok(())
    }

    pub(crate) fn ignore(&self, incoming: proto::Incoming) {
        let mut state = self.state.lock().unwrap();
        state.stats.ignored_handshakes += 1;
        state.inner.ignore(incoming);
    }
}

#[derive(Debug)]
pub(crate) struct State {
    socket: Arc<dyn AsyncUdpSocket>,
    /// During active migration, the previous socket receives traffic until every connection that
    /// was live at rebind has authenticated a packet from the current socket or drained.
    prev_socket: Option<Arc<dyn AsyncUdpSocket>>,
    prev_socket_connections: PreviousSocketConnections,
    inner: proto::Endpoint,
    recv_state: RecvState,
    driver: Option<Waker>,
    ipv6: bool,
    events: mpsc::UnboundedReceiver<(ConnectionHandle, ConnectionToEndpointEvent)>,
    driver_lost: bool,
    runtime: Arc<dyn Runtime>,
    stats: EndpointStats,
}

#[derive(Debug, Default)]
struct PreviousSocketConnections {
    generation: u64,
    pending: FxHashSet<ConnectionHandle>,
}

impl PreviousSocketConnections {
    fn at_rebind(
        generation: u64,
        connections: impl IntoIterator<Item = ConnectionHandle>,
    ) -> Self {
        Self {
            generation,
            pending: connections.into_iter().collect(),
        }
    }

    fn note_authenticated_current_socket_packet(
        &mut self,
        connection: ConnectionHandle,
        generation: u64,
    ) {
        if generation == self.generation {
            self.pending.remove(&connection);
        }
    }

    fn note_connection_drained(&mut self, connection: ConnectionHandle) {
        self.pending.remove(&connection);
    }

    fn must_retain_previous_socket(&self) -> bool {
        !self.pending.is_empty()
    }

    #[cfg(test)]
    fn contains(&self, connection: ConnectionHandle) -> bool {
        self.pending.contains(&connection)
    }
}

#[derive(Debug)]
pub(crate) struct Shared {
    incoming: Notify,
    idle: Notify,
    /// Number of live handles that can be used to initiate or handle I/O; excludes the driver
    ref_count: AtomicUsize,
}

impl State {
    fn drive_recv(&mut self, cx: &mut Context, now: Instant) -> Result<bool, io::Error> {
        let get_time = || self.runtime.now();
        self.recv_state.recv_limiter.start_cycle(get_time);
        if let Some(socket) = &self.prev_socket {
            // We don't care about the `PollProgress` from old sockets.
            let poll_res = self.recv_state.poll_socket(
                cx,
                &mut self.inner,
                &**socket,
                &*self.runtime,
                now,
                None,
            );
            if poll_res.is_err() {
                self.prev_socket = None;
                self.prev_socket_connections = PreviousSocketConnections::default();
            }
        };
        let poll_res = self.recv_state.poll_socket(
            cx,
            &mut self.inner,
            &*self.socket,
            &*self.runtime,
            now,
            Some(self.stats.socket_rebinds),
        );
        self.recv_state.recv_limiter.finish_cycle(get_time);
        let poll_res = poll_res?;
        Ok(poll_res.keep_going)
    }

    fn handle_events(&mut self, cx: &mut Context, shared: &Shared) -> bool {
        for _ in 0..IO_LOOP_BOUND {
            let (ch, event) = match self.events.poll_recv(cx) {
                Poll::Ready(Some(x)) => x,
                Poll::Ready(None) => unreachable!("EndpointInner owns one sender"),
                Poll::Pending => {
                    return false;
                }
            };

            let event = match event {
                ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(generation) => {
                    self.stats.current_socket_rx_rebind_generation = self
                        .stats
                        .current_socket_rx_rebind_generation
                        .max(generation);
                    self.prev_socket_connections
                        .note_authenticated_current_socket_packet(ch, generation);
                    if !self.prev_socket_connections.must_retain_previous_socket() {
                        self.prev_socket = None;
                    }
                    continue;
                }
                ConnectionToEndpointEvent::Proto(event) => event,
            };

            if event.is_drained() {
                self.recv_state.connections.senders.remove(&ch);
                self.prev_socket_connections.note_connection_drained(ch);
                if !self.prev_socket_connections.must_retain_previous_socket() {
                    self.prev_socket = None;
                }
                if self.recv_state.connections.is_empty() {
                    shared.idle.notify_waiters();
                }
            }
            let Some(event) = self.inner.handle_event(ch, event) else {
                continue;
            };
            // Ignoring errors from dropped connections that haven't yet been cleaned up
            let _ = self
                .recv_state
                .connections
                .senders
                .get_mut(&ch)
                .unwrap()
                .send(ConnectionEvent::Proto {
                    event,
                    current_socket_rx_rebind_generation: None,
                });
        }

        true
    }
}

impl Drop for State {
    fn drop(&mut self) {
        for incoming in self.recv_state.incoming.drain(..) {
            self.inner.ignore(incoming);
        }
    }
}

fn respond(
    transmit: proto::Transmit,
    response_buffer: &[u8],
    socket: &dyn AsyncUdpSocket,
    endpoint: &proto::Endpoint,
    now: Instant,
) {
    // Send if there's kernel buffer space; otherwise, drop it
    //
    // As an endpoint-generated packet, we know this is an
    // immediate, stateless response to an unconnected peer,
    // one of:
    //
    // - A version negotiation response due to an unknown version
    // - A `CLOSE` due to a malformed or unwanted connection attempt
    // - A stateless reset due to an unrecognized connection
    // - A `Retry` packet due to a connection attempt when
    //   `use_retry` is set
    //
    // In each case, a well-behaved peer can be trusted to retry a
    // few times, which is guaranteed to produce the same response
    // from us. Repeated failures might at worst cause a peer's new
    // connection attempt to time out, which is acceptable if we're
    // under such heavy load that there's never room for this code
    // to transmit. This is morally equivalent to the packet getting
    // lost due to congestion further along the link, which
    // similarly relies on peer retries for recovery.
    let pacing =
        match endpoint.endpoint_pacing_reserve_stateless_response(now, transmit.size as u64) {
            Ok(reservation) => reservation,
            Err(_) => return,
        };
    let result = socket.try_send(&udp_transmit(&transmit, &response_buffer[..transmit.size]));
    if let Some(reservation) = pacing {
        if result.is_ok() {
            reservation.note_sent(now);
        } else {
            reservation.note_abandoned(now);
        }
    }
}

#[inline]
fn proto_ecn(ecn: udp::EcnCodepoint) -> proto::EcnCodepoint {
    match ecn {
        udp::EcnCodepoint::Ect0 => proto::EcnCodepoint::Ect0,
        udp::EcnCodepoint::Ect1 => proto::EcnCodepoint::Ect1,
        udp::EcnCodepoint::Ce => proto::EcnCodepoint::Ce,
    }
}

#[derive(Debug)]
struct ConnectionSet {
    /// Senders for communicating with the endpoint's connections
    senders: FxHashMap<ConnectionHandle, mpsc::UnboundedSender<ConnectionEvent>>,
    /// Stored to give out clones to new ConnectionInners
    sender: mpsc::UnboundedSender<(ConnectionHandle, ConnectionToEndpointEvent)>,
    /// Set if the endpoint has been manually closed
    close: Option<(VarInt, Bytes)>,
}

impl ConnectionSet {
    fn insert(
        &mut self,
        handle: ConnectionHandle,
        conn: proto::Connection,
        socket: Arc<dyn AsyncUdpSocket>,
        runtime: Arc<dyn Runtime>,
    ) -> Connecting {
        let (send, recv) = mpsc::unbounded_channel();
        if let Some((error_code, ref reason)) = self.close {
            send.send(ConnectionEvent::Close {
                error_code,
                reason: reason.clone(),
            })
            .unwrap();
        }
        self.senders.insert(handle, send);
        Connecting::new(handle, conn, self.sender.clone(), recv, socket, runtime)
    }

    fn is_empty(&self) -> bool {
        self.senders.is_empty()
    }
}

fn ensure_ipv6(x: SocketAddr) -> SocketAddrV6 {
    match x {
        SocketAddr::V6(x) => x,
        SocketAddr::V4(x) => SocketAddrV6::new(x.ip().to_ipv6_mapped(), x.port(), 0, 0),
    }
}

pin_project! {
    /// Future produced by [`Endpoint::accept`]
    pub struct Accept<'a> {
        endpoint: &'a Endpoint,
        #[pin]
        notify: Notified<'a>,
    }
}

impl Future for Accept<'_> {
    type Output = Option<Incoming>;
    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let mut endpoint = this.endpoint.inner.state.lock().unwrap();
        if endpoint.driver_lost {
            return Poll::Ready(None);
        }
        if let Some(incoming) = endpoint.recv_state.incoming.pop_front() {
            // Release the mutex lock on endpoint so cloning it doesn't deadlock
            drop(endpoint);
            let incoming = Incoming::new(incoming, this.endpoint.inner.clone());
            return Poll::Ready(Some(incoming));
        }
        if endpoint.recv_state.connections.close.is_some() {
            return Poll::Ready(None);
        }
        loop {
            match this.notify.as_mut().poll(ctx) {
                // `state` lock ensures we didn't race with readiness
                Poll::Pending => return Poll::Pending,
                // Spurious wakeup, get a new future
                Poll::Ready(()) => this
                    .notify
                    .set(this.endpoint.inner.shared.incoming.notified()),
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct EndpointRef(Arc<EndpointInner>);

impl EndpointRef {
    pub(crate) fn new(
        socket: Arc<dyn AsyncUdpSocket>,
        inner: proto::Endpoint,
        ipv6: bool,
        runtime: Arc<dyn Runtime>,
    ) -> Self {
        let (sender, events) = mpsc::unbounded_channel();
        let recv_state = RecvState::new(sender, socket.max_receive_segments(), &inner);
        Self(Arc::new(EndpointInner {
            shared: Shared {
                incoming: Notify::new(),
                idle: Notify::new(),
                ref_count: AtomicUsize::new(0),
            },
            state: Mutex::new(State {
                socket,
                prev_socket: None,
                prev_socket_connections: PreviousSocketConnections::default(),
                inner,
                ipv6,
                events,
                driver: None,
                driver_lost: false,
                recv_state,
                runtime,
                stats: EndpointStats::default(),
            }),
        }))
    }
}

impl Clone for EndpointRef {
    fn clone(&self) -> Self {
        self.0.shared.ref_count.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }
}

impl Drop for EndpointRef {
    fn drop(&mut self) {
        if self.shared.ref_count.fetch_sub(1, Ordering::Relaxed) > 1 {
            return;
        }

        let endpoint = &mut *self.0.state.lock().unwrap();
        // If the driver is about to be on its own, ensure it can shut down if the last
        // connection is gone.
        if let Some(task) = endpoint.driver.take() {
            task.wake();
        }
    }
}

impl std::ops::Deref for EndpointRef {
    type Target = EndpointInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// State directly involved in handling incoming packets
struct RecvState {
    incoming: VecDeque<proto::Incoming>,
    connections: ConnectionSet,
    recv_buf: Box<[u8]>,
    recv_limiter: WorkLimiter,
}

impl RecvState {
    fn new(
        sender: mpsc::UnboundedSender<(ConnectionHandle, ConnectionToEndpointEvent)>,
        max_receive_segments: usize,
        endpoint: &proto::Endpoint,
    ) -> Self {
        let recv_buf = vec![
            0;
            endpoint.config().get_max_udp_payload_size().min(64 * 1024) as usize
                * max_receive_segments
                * BATCH_SIZE
        ];
        Self {
            connections: ConnectionSet {
                senders: FxHashMap::default(),
                sender,
                close: None,
            },
            incoming: VecDeque::new(),
            recv_buf: recv_buf.into(),
            recv_limiter: WorkLimiter::new(RECV_TIME_BOUND),
        }
    }

    fn poll_socket(
        &mut self,
        cx: &mut Context,
        endpoint: &mut proto::Endpoint,
        socket: &dyn AsyncUdpSocket,
        runtime: &dyn Runtime,
        now: Instant,
        current_socket_rx_rebind_generation: Option<u64>,
    ) -> Result<PollProgress, io::Error> {
        let mut metas = [RecvMeta::default(); BATCH_SIZE];
        let mut iovs: [IoSliceMut; BATCH_SIZE] = {
            let mut bufs = self
                .recv_buf
                .chunks_mut(self.recv_buf.len() / BATCH_SIZE)
                .map(IoSliceMut::new);

            // expect() safe as self.recv_buf is chunked into BATCH_SIZE items
            // and iovs will be of size BATCH_SIZE, thus from_fn is called
            // exactly BATCH_SIZE times.
            std::array::from_fn(|_| bufs.next().expect("BATCH_SIZE elements"))
        };
        loop {
            match socket.poll_recv(cx, &mut iovs, &mut metas) {
                Poll::Ready(Ok(msgs)) => {
                    self.recv_limiter.record_work(msgs);
                    for (meta, buf) in metas.iter().zip(iovs.iter()).take(msgs) {
                        let mut data: BytesMut = buf[0..meta.len].into();
                        while !data.is_empty() {
                            let buf = data.split_to(meta.stride.min(data.len()));
                            let mut response_buffer = Vec::new();
                            match endpoint.handle(
                                now,
                                meta.addr,
                                meta.dst_ip,
                                meta.ecn.map(proto_ecn),
                                buf,
                                &mut response_buffer,
                            ) {
                                Some(DatagramEvent::NewConnection(incoming)) => {
                                    if self.connections.close.is_none() {
                                        self.incoming.push_back(incoming);
                                    } else {
                                        let transmit =
                                            endpoint.refuse(incoming, &mut response_buffer);
                                        respond(transmit, &response_buffer, socket, endpoint, now);
                                    }
                                }
                                Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                                    // Ignoring errors from dropped connections that haven't yet been cleaned up
                                    let _ =
                                        self.connections.senders.get_mut(&handle).unwrap().send(
                                            ConnectionEvent::Proto {
                                                event,
                                                current_socket_rx_rebind_generation,
                                            },
                                        );
                                }
                                Some(DatagramEvent::Response(transmit)) => {
                                    respond(transmit, &response_buffer, socket, endpoint, now);
                                }
                                None => {}
                            }
                        }
                    }
                }
                Poll::Pending => {
                    return Ok(PollProgress {
                        keep_going: false,
                    });
                }
                // Ignore ECONNRESET as it's undefined in QUIC and may be injected by an
                // attacker
                Poll::Ready(Err(ref e)) if e.kind() == io::ErrorKind::ConnectionReset => {
                    continue;
                }
                Poll::Ready(Err(e)) => {
                    return Err(e);
                }
            }
            if !self.recv_limiter.allow_work(|| runtime.now()) {
                return Ok(PollProgress {
                    keep_going: true,
                });
            }
        }
    }
}

impl fmt::Debug for RecvState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RecvState")
            .field("incoming", &self.incoming)
            .field("connections", &self.connections)
            // recv_buf too large
            .field("recv_limiter", &self.recv_limiter)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct PollProgress {
    /// Whether datagram handling was interrupted early by the work limiter for fairness
    keep_going: bool,
}

#[cfg(test)]
mod endpoint_pacing_stateless_tests {
    use std::{
        collections::VecDeque,
        io::{self, IoSliceMut},
        net::{Ipv4Addr, SocketAddr},
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex,
        },
        task::{Context, Poll},
    };

    use crate::{AsyncUdpSocket, EndpointPacingServiceConfig, UdpPoller};
    use udp::{RecvMeta, Transmit};

    use super::{proto, respond, Instant};

    #[derive(Clone, Copy, Debug)]
    enum SendStep {
        Success,
        WouldBlock,
    }

    #[derive(Debug)]
    struct CountingSocket {
        attempts: AtomicUsize,
        steps: Mutex<VecDeque<SendStep>>,
    }

    impl CountingSocket {
        fn new(steps: impl IntoIterator<Item = SendStep>) -> Self {
            Self {
                attempts: AtomicUsize::new(0),
                steps: Mutex::new(steps.into_iter().collect()),
            }
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }
    }

    impl AsyncUdpSocket for CountingSocket {
        fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
            Box::pin(AlwaysWritable)
        }

        fn try_send(&self, _transmit: &Transmit<'_>) -> io::Result<()> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            match self
                .steps
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(SendStep::Success)
            {
                SendStep::Success => Ok(()),
                SendStep::WouldBlock => Err(io::ErrorKind::WouldBlock.into()),
            }
        }

        fn poll_recv(
            &self,
            _cx: &mut Context,
            _bufs: &mut [IoSliceMut<'_>],
            _meta: &mut [RecvMeta],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(SocketAddr::from((Ipv4Addr::LOCALHOST, 50001)))
        }
    }

    #[derive(Debug)]
    struct AlwaysWritable;

    impl UdpPoller for AlwaysWritable {
        fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn endpoint(configured: bool) -> proto::Endpoint {
        let mut config = proto::EndpointConfig::default();
        if configured {
            config.endpoint_pacing_service(Some(EndpointPacingServiceConfig::endpoint_window_v1()));
        }
        proto::Endpoint::new(Arc::new(config), None, false, None)
    }

    fn transmit(size: usize) -> proto::Transmit {
        proto::Transmit {
            destination: SocketAddr::from((Ipv4Addr::LOCALHOST, 4433)),
            ecn: None,
            size,
            segment_size: None,
            src_ip: None,
        }
    }

    #[test]
    fn configured_stateless_responses_require_immediate_service() {
        let endpoint = endpoint(true);
        let socket = CountingSocket::new([]);
        let response = vec![0; 1_200];
        let now = Instant::now();

        for _ in 0..52 {
            respond(transmit(1_200), &response, &socket, &endpoint, now);
        }

        assert_eq!(socket.attempts(), 51);
        let snapshot = endpoint.endpoint_pacing_snapshot().unwrap();
        assert_eq!(snapshot.available_tokens, 240);
        assert_eq!(snapshot.outstanding_bytes, 0);
        assert_eq!(snapshot.stateless_responses_sent, 51);
        assert_eq!(snapshot.stateless_responses_dropped, 1);
    }

    #[test]
    fn configured_stateless_socket_would_block_is_counted_as_dropped() {
        let endpoint = endpoint(true);
        let socket = CountingSocket::new([SendStep::WouldBlock]);
        let response = vec![0; 1_200];

        respond(
            transmit(1_200),
            &response,
            &socket,
            &endpoint,
            Instant::now(),
        );

        assert_eq!(socket.attempts(), 1);
        let snapshot = endpoint.endpoint_pacing_snapshot().unwrap();
        assert_eq!(snapshot.outstanding_bytes, 0);
        assert_eq!(snapshot.stateless_responses_sent, 0);
        assert_eq!(snapshot.stateless_responses_dropped, 1);
    }

    #[test]
    fn default_stateless_response_keeps_the_single_socket_attempt_path() {
        let endpoint = endpoint(false);
        let socket = CountingSocket::new([SendStep::WouldBlock]);
        let response = vec![0; 1_200];

        respond(
            transmit(1_200),
            &response,
            &socket,
            &endpoint,
            Instant::now(),
        );

        assert_eq!(socket.attempts(), 1);
        assert!(endpoint.endpoint_pacing_snapshot().is_none());
    }
}

#[cfg(test)]
mod multi_connection_rebind_state_tests {
    use super::{ConnectionHandle, PreviousSocketConnections};

    #[test]
    fn first_current_socket_recovery_retains_previous_socket_for_other_connections() {
        let first = ConnectionHandle(11);
        let second = ConnectionHandle(12);
        let mut pending = PreviousSocketConnections::at_rebind(1, [first, second]);

        pending.note_authenticated_current_socket_packet(first, 1);

        assert!(pending.must_retain_previous_socket());
        assert!(!pending.contains(first));
        assert!(pending.contains(second));
    }

    #[test]
    fn all_current_socket_recoveries_release_previous_socket() {
        let first = ConnectionHandle(11);
        let second = ConnectionHandle(12);
        let mut pending = PreviousSocketConnections::at_rebind(1, [first, second]);

        pending.note_authenticated_current_socket_packet(first, 1);
        pending.note_authenticated_current_socket_packet(second, 1);

        assert!(!pending.must_retain_previous_socket());
    }

    #[test]
    fn drained_pending_connection_releases_previous_socket() {
        let connection = ConnectionHandle(11);
        let mut pending = PreviousSocketConnections::at_rebind(1, [connection]);

        pending.note_connection_drained(connection);

        assert!(!pending.must_retain_previous_socket());
    }

    #[test]
    fn post_rebind_connection_does_not_extend_previous_socket_lifetime() {
        let existing = ConnectionHandle(11);
        let post_rebind = ConnectionHandle(12);
        let mut pending = PreviousSocketConnections::at_rebind(1, [existing]);

        pending.note_authenticated_current_socket_packet(post_rebind, 1);

        assert!(pending.contains(existing));
        assert!(!pending.contains(post_rebind));
    }

    #[test]
    fn next_rebind_replaces_the_previous_pending_snapshot() {
        let first_generation = ConnectionHandle(11);
        let second_generation = ConnectionHandle(12);
        let pending = PreviousSocketConnections::at_rebind(1, [first_generation]);
        assert!(pending.contains(first_generation));

        let pending = PreviousSocketConnections::at_rebind(2, [second_generation]);

        assert!(!pending.contains(first_generation));
        assert!(pending.contains(second_generation));
    }

    #[test]
    fn stale_authenticated_generation_cannot_complete_a_later_rebind() {
        let connection = ConnectionHandle(11);
        let mut pending = PreviousSocketConnections::at_rebind(2, [connection]);

        pending.note_authenticated_current_socket_packet(connection, 1);

        assert!(pending.contains(connection));
        assert!(pending.must_retain_previous_socket());
    }
}

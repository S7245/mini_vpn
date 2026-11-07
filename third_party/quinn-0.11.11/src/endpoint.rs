use std::{
    alloc::{alloc_zeroed, Layout},
    collections::VecDeque,
    fmt,
    future::Future,
    io,
    io::IoSliceMut,
    mem,
    net::{SocketAddr, SocketAddrV6},
    pin::Pin,
    ptr, str,
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

pub(crate) mod budgeted_events;
mod catalog;
mod construction;
mod fanout;
mod native_connect;
mod rebind;
use budgeted_events::{ForwardControlPermit, ForwardSender};
use bytes::{BackingBudgetReservation, BackingMeter, Bytes, BytesMut, MeteredCopyError};
use catalog::Catalog;
use pin_project_lite::pin_project;
use proto::owned_events::ControlKind;
use proto::{
    self as proto, ClientConfig, ConnectError, ConnectionError, ConnectionHandle, DatagramEvent,
    ServerConfig,
};
use rebind::PreviousSocketInventory;
use rustc_hash::{FxHashMap, FxHashSet};
#[cfg(all(not(wasm_browser), any(feature = "aws-lc-rs", feature = "ring"),))]
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::{futures::Notified, mpsc, oneshot, Notify};
use tracing::{Instrument, Span};
use udp::{RecvMeta, BATCH_SIZE};

use crate::{
    connection::Connecting, incoming::Incoming, work_limiter::WorkLimiter, ConnectionEvent,
    ConnectionToEndpointEvent, EndpointConfig, VarInt, IO_LOOP_BOUND, RECV_TIME_BOUND,
};

const BUDGETED_FORWARD_ORDINARY_CAPACITY: usize = 64;
const BUDGETED_CONNECTION_LIMIT: usize = proto::BUDGETED_CONNECTION_LIMIT;

/// A QUIC endpoint.
///
/// An endpoint corresponds to a single UDP socket, may host many connections, and may act as both
/// client and server for different connections.
///
/// May be cloned to obtain another handle to the same endpoint.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub(crate) inner: EndpointRef,
    receive_backing_meter: Option<BackingMeter>,
    pub(crate) default_client_config: Option<ClientConfig>,
    runtime: Arc<dyn Runtime>,
    driver_drain: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
}

/// A single-use witness that one exact endpoint I/O driver completed normally.
///
/// The witness cannot complete while a clone of its [`Endpoint`] remains live.
/// An I/O error, panic, task abort, or runtime shutdown drops the completion
/// sender and therefore fails [`drained`](Self::drained) closed.
#[derive(Debug)]
#[doc(hidden)]
pub struct EndpointDriverDrain {
    driver_released: oneshot::Receiver<()>,
}

impl EndpointDriverDrain {
    /// Wait for normal completion of the exact endpoint I/O driver.
    ///
    /// The handle is consumed so the completion witness has only one waiter.
    #[doc(hidden)]
    pub async fn drained(self) -> bool {
        self.driver_released.await.is_ok()
    }
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
        let state = self.inner.lock_state();
        let mut stats = state.stats;
        stats.budget_rejected_datagrams = state.recv_state.budget_rejected_datagrams;
        stats.receive_backing_rejected_datagrams = state.recv_state.receive_backing_rejected_datagrams;
        stats.owned_event_rejected_datagrams = state.recv_state.owned_event_rejected_datagrams;
        stats
    }

    /// Return endpoint pacing conservation state when the optional service is configured.
    #[doc(hidden)]
    pub fn endpoint_pacing_snapshot(&self) -> Option<proto::EndpointPacingSnapshot> {
        self.inner.lock_state().inner.endpoint_pacing_snapshot()
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
        Self::new_with_receive_meter(config, server_config, socket, runtime, None)
    }

    /// Construct an endpoint with its receive meter installed before driver spawn.
    #[doc(hidden)]
    pub fn new_with_metered_abstract_socket(
        config: EndpointConfig,
        server_config: Option<ServerConfig>,
        socket: Arc<dyn AsyncUdpSocket>,
        runtime: Arc<dyn Runtime>,
        meter: BackingMeter,
    ) -> io::Result<Self> {
        Self::new_with_receive_meter(config, server_config, socket, runtime, Some(meter))
    }

    /// Authority installed through the metered endpoint constructor before
    /// its driver starts; ordinary constructors return None.
    #[doc(hidden)]
    pub fn receive_backing_meter(&self) -> Option<BackingMeter> {
        self.receive_backing_meter.clone()
    }

    /// Take the sole normal-completion witness for this endpoint's I/O driver.
    ///
    /// All endpoint clones share one slot, so at most one caller can obtain the
    /// witness. The returned handle does not retain an endpoint reference.
    #[doc(hidden)]
    pub fn take_driver_drain_handle(&mut self) -> Option<EndpointDriverDrain> {
        self.driver_drain
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .map(|driver_released| EndpointDriverDrain { driver_released })
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
        let now = self.runtime.now();
        let mut endpoint = self.inner.lock_state();
        if endpoint.driver_lost || endpoint.recv_state.connections.close.is_some() {
            return Err(ConnectError::EndpointStopping);
        }
        if addr.is_ipv6() && !endpoint.ipv6 {
            return Err(ConnectError::InvalidRemoteAddress(addr));
        }
        if endpoint.inner.native_registration() {
            drop(endpoint);
            return self.connect_native(now, config, addr, server_name);
        }
        let addr = if endpoint.ipv6 {
            SocketAddr::V6(ensure_ipv6(addr))
        } else {
            addr
        };
        if endpoint
            .recv_state
            .backing_meter
            .as_ref()
            .and_then(BackingMeter::budget_limit)
            .is_some()
            && endpoint.recv_state.connections.catalog.len() >= BUDGETED_CONNECTION_LIMIT
        {
            return Err(ConnectError::EventCapacityUnavailable);
        }

        // Reserve the entire per-connection event lane before Proto inserts a
        // CID or creates TLS/connection state. A rejected budget leaves Proto
        // and the old connection set untouched.
        let forward = budgeted_events::connection_channels(
            endpoint.recv_state.backing_meter.as_ref(),
            BUDGETED_FORWARD_ORDINARY_CAPACITY,
        )
        .map_err(|_| ConnectError::EventCapacityUnavailable)?;

        let (ch, conn) = endpoint.inner.connect(now, config, addr, server_name)?;

        let socket = endpoint.socket.clone();
        endpoint.stats.outgoing_handshakes += 1;
        let registration = (
            self.inner.self_weak.clone(),
            endpoint.inner.registration_observer(),
        );
        let (connecting, start) = endpoint.recv_state.connections.insert(
            ch,
            conn,
            socket,
            self.runtime.clone(),
            forward,
            registration,
        )?;
        drop(endpoint);
        start.start();
        Ok(connecting)
    }

    /// Switch to a new UDP socket
    ///
    /// See [`Endpoint::rebind_abstract()`] for details.
    #[cfg(not(wasm_browser))]
    pub fn rebind(&self, socket: std::net::UdpSocket) -> io::Result<()> {
        self.rebind_abstract(self.runtime.wrap_udp_socket(socket)?)
    }

    /// Replace the server configuration, affecting new incoming connections only
    ///
    /// Useful for e.g. refreshing TLS certificates without disrupting existing connections.
    pub fn set_server_config(&self, server_config: Option<ServerConfig>) {
        let mut state = self.inner.lock_state();
        if state.driver_lost {
            return;
        }
        state.inner.set_server_config(server_config.map(Arc::new));
    }

    /// Get the local `SocketAddr` the underlying socket is bound to
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.lock_state().socket.local_addr()
    }

    /// Get the number of connections that are currently open
    pub fn open_connections(&self) -> usize {
        self.inner.lock_state().inner.open_connections()
    }

    /// Close all of this endpoint's connections immediately and cease accepting new connections.
    ///
    /// See [`Connection::close()`] for details.
    ///
    /// [`Connection::close()`]: crate::Connection::close
    pub fn close(&self, error_code: VarInt, reason: &[u8]) {
        let mut fanout = fanout::Fanout::new();
        let mut endpoint = self.inner.lock_state();
        let meter = endpoint.recv_state.backing_meter.clone();
        fanout.meter = meter.clone();
        fanout.driver = endpoint.driver.take();
        let _panic = fanout::RootPanic(&meter);
        if endpoint.driver_lost || meter.as_ref().is_some_and(BackingMeter::is_quarantined) {
            return;
        }
        if let Some(meter) = meter
            .as_ref()
            .filter(|meter| meter.budget_limit().is_some())
        {
            if endpoint.recv_state.connections.close.is_some() {
                return;
            }
            if !endpoint.registration_inventory_valid() {
                meter.quarantine();
                return;
            }
            let Some(reason) = Bytes::copy_from_slice_metered(reason, meter) else {
                meter.quarantine();
                return;
            };
            if fanout
                .prepare(&endpoint, ControlKind::Close, || ConnectionEvent::Close {
                    error_code,
                    reason: reason.clone(),
                })
                .is_err()
            {
                meter.quarantine();
                return;
            }
            if fanout
                .apply(&mut endpoint, ControlKind::Close, |endpoint, _| {
                    endpoint.recv_state.connections.close = Some((error_code, reason));
                    [None, None]
                })
                .is_err()
            {
                meter.quarantine();
                return;
            }
        } else {
            let reason = Bytes::copy_from_slice(reason);
            endpoint.recv_state.connections.close = Some((error_code, reason.clone()));
            for sender in endpoint.recv_state.connections.catalog.values() {
                let _ = sender.send_control(
                    ForwardControlPermit::Legacy,
                    ConnectionEvent::Close {
                        error_code,
                        reason: reason.clone(),
                    },
                );
            }
        }
        drop(endpoint);
        fanout.finish();
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
                let endpoint = &mut *self.inner.lock_state();
                if endpoint.recv_state.connections.is_empty() {
                    break;
                }
                // Construct future while lock is held to avoid race
                self.inner.shared.idle.notified()
            }
            .await;
        }
    }

    /// Wait for all connections to be removed, failing closed if the endpoint
    /// driver exits before that condition can be observed.
    #[doc(hidden)]
    pub async fn wait_idle_or_driver_lost(&self) -> bool {
        loop {
            let notified = {
                let endpoint = &mut *self.inner.lock_state();
                if let Some(idle) = idle_or_driver_lost(
                    endpoint.driver_lost,
                    endpoint.recv_state.connections.is_empty(),
                ) {
                    return idle;
                }
                // Construct the future while locked to avoid losing either an
                // idle transition or a driver-loss notification.
                self.inner.shared.idle.notified()
            };
            notified.await;
        }
    }
}

fn idle_or_driver_lost(driver_lost: bool, connections_empty: bool) -> Option<bool> {
    if driver_lost {
        Some(false)
    } else if connections_empty {
        Some(true)
    } else {
        None
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
    /// Unadmitted UDP datagrams rejected for backing or owned-event capacity.
    /// Retains the existing combined counter's semantics.
    pub budget_rejected_datagrams: u64,
    /// GRO segments rejected before Proto because receive backing was full.
    pub receive_backing_rejected_datagrams: u64,
    /// Datagrams rejected by owned-event admission (slots or an outstanding reservation).
    pub owned_event_rejected_datagrams: u64,
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
        let mut endpoint = self.0.lock_state();
        // A future may be moved between tasks after its first poll. Last-handle
        // Drop must wake the current driver, not the original polling task.
        match &mut endpoint.driver {
            Some(waker) => waker.clone_from(cx.waker()),
            None => endpoint.driver = Some(cx.waker().clone()),
        }

        if endpoint.driver_lost {
            return Poll::Ready(Err(io::Error::other("endpoint state unavailable")));
        }
        if endpoint
            .recv_state
            .backing_meter
            .as_ref()
            .is_some_and(BackingMeter::is_quarantined)
        {
            return Poll::Ready(Err(io::Error::other(
                "endpoint receive authority quarantined",
            )));
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
        let mut endpoint = self.0.lock_state();
        endpoint.driver_lost = true;
        // A queued Drained witness may already own its ACK sender, so no
        // subsequent Connection Drop is guaranteed to report its loss. Record
        // quarantine at this owner before discarding any live settlement.
        if !endpoint.recv_state.connections.is_empty() {
            if let Some(meter) = endpoint.recv_state.backing_meter.as_ref() {
                meter.quarantine();
            }
        }
        self.0.shared.incoming.notify_waiters();
        self.0.shared.idle.notify_waiters();
        // No endpoint event can be acknowledged once this driver is gone.
        // Close and drain the receiver so queued acknowledgement senders are
        // dropped now and all later sends fail, allowing exact cleanup waiters
        // to fail closed instead of waiting forever.
        endpoint.events.close();
        while endpoint.events.try_recv().is_ok() {}
        // A reverse event held for forward capacity may carry the one-shot
        // release witness. Drop it now so its waiter sees driver loss.
        endpoint.pending_reverse_event = None;
        // Drop all outgoing channels, signaling the termination of the endpoint to the associated
        // connections.
        endpoint.recv_state.connections.catalog.clear();
    }
}

#[derive(Debug)]
pub(crate) struct EndpointInner {
    pub(crate) self_weak: std::sync::Weak<EndpointInner>,
    pub(crate) state: Mutex<State>,
    pub(crate) shared: Shared,
}

impl EndpointInner {
    /// Recover only to the original fatal authority, never to ordinary publication.
    /// Drop and observations must not panic again and hide an opaque constructor panic.
    fn lock_state(&self) -> std::sync::MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                if let Some(meter) = &state.recv_state.backing_meter {
                    meter.quarantine();
                }
                state.driver_lost = true;
                state
            }
        }
    }

    /// Control-only reverse settlement. The caller holds its Connection lock;
    /// this path must never acquire a Connection lock or wait for a DATA slot.
    pub(crate) fn settle_connection_event(
        &self,
        ch: ConnectionHandle,
        event: ConnectionToEndpointEvent,
    ) -> Result<Option<proto::ConnectionEvent>, ()> {
        let mut state = self.state.lock().map_err(|_| ())?;
        if !state.registration_inventory_valid() {
            if let Some(meter) = &state.recv_state.backing_meter {
                meter.quarantine();
            }
            return Err(());
        }
        if state.driver_lost || !state.recv_state.connections.catalog.contains_key(&ch) {
            return Err(());
        }
        match event {
            ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(key, generation) => {
                if !state.inner.registration_matches(ch, key) {
                    if key.is_none() && state.inner.native_registration() {
                        return Err(());
                    }
                    return Ok(None);
                }
                state.note_authenticated_socket(ch, key, generation);
                Ok(None)
            }
            ConnectionToEndpointEvent::Proto {
                event,
                endpoint_released: None,
            } if !event.is_drained() => {
                let outcome = state.inner.handle_event_checked(ch, event);
                if outcome.disposition == proto::EndpointEventDisposition::Rejected {
                    Err(())
                } else {
                    Ok(outcome.response)
                }
            }
            _ => Err(()),
        }
    }
    /// Same control Root, with the real Connection exclusively loaned by its
    /// State lock. Every reply admission/Pool lock precedes route publication.
    pub(crate) fn settle_connection_protocol(
        &self,
        ch: ConnectionHandle,
        event: proto::EndpointEvent,
        connection: &mut proto::Connection,
    ) -> Result<(), ()> {
        let mut state = self.lock_state();
        if !state.registration_inventory_valid() {
            if let Some(meter) = &state.recv_state.backing_meter {
                meter.quarantine();
            }
            return Err(());
        }
        if state.driver_lost || !state.recv_state.connections.catalog.contains_key(&ch) {
            return Err(());
        }
        let outcome = state.inner.handle_connection_event(ch, event, connection);
        if outcome.disposition == proto::EndpointEventDisposition::Rejected
            || outcome.response.is_some()
        {
            Err(())
        } else {
            Ok(())
        }
    }
    pub(crate) fn accept(
        &self,
        incoming: proto::Incoming,
        server_config: Option<Arc<ServerConfig>>,
    ) -> Result<Connecting, ConnectionError> {
        let mut state = self.lock_state();
        if state.driver_lost {
            return Err(ConnectionError::LocallyClosed);
        }
        let mut response_buffer = Vec::new();
        let now = state.runtime.now();
        if state
            .recv_state
            .backing_meter
            .as_ref()
            .and_then(BackingMeter::budget_limit)
            .is_some()
            && state.recv_state.connections.catalog.len() >= BUDGETED_CONNECTION_LIMIT
        {
            return Err(ConnectionError::LocallyClosed);
        }
        let forward = budgeted_events::connection_channels(
            state.recv_state.backing_meter.as_ref(),
            BUDGETED_FORWARD_ORDINARY_CAPACITY,
        )
        .map_err(|_| ConnectionError::LocallyClosed)?;
        match state
            .inner
            .accept(incoming, now, &mut response_buffer, server_config)
        {
            Ok((handle, conn)) => {
                state.stats.accepted_handshakes += 1;
                let socket = state.socket.clone();
                let runtime = state.runtime.clone();
                let registration = (self.self_weak.clone(), state.inner.registration_observer());
                let (connecting, start) = state
                    .recv_state
                    .connections
                    .insert(handle, conn, socket, runtime, forward, registration)
                    .map_err(|_| ConnectionError::LocallyClosed)?;
                drop(state);
                start.start();
                Ok(connecting)
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
        let mut state = self.lock_state();
        state.stats.refused_handshakes += 1;
        let now = state.runtime.now();
        let mut response_buffer = Vec::new();
        let transmit = state.inner.refuse(incoming, &mut response_buffer);
        let socket = state.socket.clone();
        respond(transmit, &response_buffer, &*socket, &state.inner, now);
    }

    pub(crate) fn retry(&self, incoming: proto::Incoming) -> Result<(), proto::RetryError> {
        let mut state = self.lock_state();
        let now = state.runtime.now();
        let mut response_buffer = Vec::new();
        let transmit = state.inner.retry(incoming, &mut response_buffer)?;
        let socket = state.socket.clone();
        respond(transmit, &response_buffer, &*socket, &state.inner, now);
        Ok(())
    }

    pub(crate) fn ignore(&self, incoming: proto::Incoming) {
        let mut state = self.lock_state();
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
    prev_socket_connections: PreviousSocketInventory,
    inner: proto::Endpoint,
    recv_state: RecvState,
    driver: Option<Waker>,
    ipv6: bool,
    events: mpsc::UnboundedReceiver<(ConnectionHandle, ConnectionToEndpointEvent)>,
    pending_reverse_event: Option<(ConnectionHandle, ConnectionToEndpointEvent)>,
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
    fn at_rebind(generation: u64, connections: impl IntoIterator<Item = ConnectionHandle>) -> Self {
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
        self.recv_state
            .recv_limiter
            .start_cycle(|| self.runtime.now());
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
                self.discard_failed_previous_socket();
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
        self.recv_state
            .recv_limiter
            .finish_cycle(|| self.runtime.now());
        let poll_res = poll_res?;
        Ok(poll_res.keep_going)
    }

    fn handle_events(&mut self, cx: &mut Context, shared: &Shared) -> bool {
        // Native ordinary replies settle synchronously with the actual receiver.
        // Do not take a corrupt Legacy pending value before checking its owner.
        if self.inner.native_registration() && self.pending_reverse_event.is_some() {
            if let Some(meter) = &self.recv_state.backing_meter {
                meter.quarantine();
            }
            return false;
        }
        for _ in 0..IO_LOOP_BOUND {
            let (ch, event) = match self
                .pending_reverse_event
                .take()
                .map(|event| Poll::Ready(Some(event)))
                .unwrap_or_else(|| {
                    let connections = &mut self.recv_state.connections;
                    for _ in 0..2 {
                        if let Some(index) = connections.terminal_ready.take() {
                            if let Some(event) = connections.catalog.poll_terminal(index, cx) {
                                return Poll::Ready(Some(event));
                            }
                        }
                        if connections.terminal_wait.as_mut().poll(cx).is_pending() {
                            break;
                        }
                        connections
                            .terminal_wait
                            .set(connections.terminal_ready.notify.clone().notified_owned());
                    }
                    self.events.poll_recv(cx)
                }) {
                Poll::Ready(Some(x)) => x,
                Poll::Ready(None) => unreachable!("EndpointInner owns one sender"),
                Poll::Pending => {
                    return false;
                }
            };

            if !self.registration_inventory_valid() {
                if let Some(meter) = &self.recv_state.backing_meter {
                    meter.quarantine();
                }
                continue;
            }
            if self.inner.native_registration()
                && !matches!(&event, ConnectionToEndpointEvent::Proto { event, .. } if event.is_drained())
            {
                if let Some(meter) = &self.recv_state.backing_meter {
                    meter.quarantine();
                }
                continue;
            }
            let (event, endpoint_released) = match event {
                ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(key, generation) => {
                    if !self.inner.registration_matches(ch, key) {
                        if key.is_none() && self.inner.native_registration() {
                            if let Some(meter) = &self.recv_state.backing_meter {
                                meter.quarantine();
                            }
                        }
                        continue;
                    }
                    self.note_authenticated_socket(ch, key, generation);
                    continue;
                }
                ConnectionToEndpointEvent::Proto {
                    event,
                    endpoint_released,
                } => (event, endpoint_released),
            };
            let mut permit = None;
            if !event.is_drained() {
                if let Some(sender) = self.recv_state.connections.catalog.get(&ch) {
                    match sender.poll_reserve_proto(cx) {
                        Poll::Ready(Ok(reserved)) => permit = Some(reserved),
                        Poll::Pending => {
                            self.pending_reverse_event = Some((
                                ch,
                                ConnectionToEndpointEvent::Proto {
                                    event,
                                    endpoint_released,
                                },
                            ));
                            return false;
                        }
                        Poll::Ready(Err(_)) => {
                            if let Some(meter) = self.recv_state.backing_meter.as_ref() {
                                meter.quarantine();
                            }
                        }
                    }
                } else if let Some(meter) = self.recv_state.backing_meter.as_ref() {
                    meter.quarantine();
                }
            }
            let was_settled = self
                .inner
                .registration_was_settled(event.registration_key());
            let mut outcome = self.inner.handle_event_checked(ch, event);
            if outcome.disposition == proto::EndpointEventDisposition::Accepted && outcome.terminal
            {
                // Only a matched Proto retirement permits host/terminal-key settlement.
                if !self
                    .recv_state
                    .connections
                    .catalog
                    .remove(&ch, outcome.registration)
                {
                    if let Some(meter) = &self.recv_state.backing_meter {
                        meter.quarantine();
                    }
                    continue;
                }
                self.note_socket_connection_drained(ch);
                outcome.finish_settlement();
                if self.recv_state.connections.is_empty() {
                    shared.idle.notify_waiters();
                }
            }
            if let Some(endpoint_released) = endpoint_released {
                if outcome.disposition == proto::EndpointEventDisposition::Accepted || was_settled {
                    let _ = endpoint_released.send(());
                }
            }
            let event = outcome.response;
            let Some(event) = event else {
                continue;
            };
            // Ignoring errors from dropped connections that haven't yet been cleaned up
            if let (Some(sender), Some(permit)) =
                (self.recv_state.connections.catalog.get(&ch), permit)
            {
                if sender
                    .send_proto(
                        permit,
                        ConnectionEvent::Proto {
                            event,
                            current_socket_rx_rebind_generation: None,
                        },
                    )
                    .is_err()
                {
                    if let Some(meter) = self.recv_state.backing_meter.as_ref() {
                        meter.quarantine();
                    }
                }
            } else if let Some(meter) = self.recv_state.backing_meter.as_ref() {
                meter.quarantine();
            }
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
    catalog: Catalog,
    terminal_ready: Arc<budgeted_events::TerminalReady>,
    terminal_wait: Pin<Box<tokio::sync::futures::OwnedNotified>>,
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
        forward: budgeted_events::ConnectionChannels,
        registration: (
            std::sync::Weak<EndpointInner>,
            Option<proto::RegistrationObserver>,
        ),
    ) -> Result<(Connecting, crate::connection::ConnectionStart), ConnectError> {
        let (owner, registration_observer) = registration;
        let key = conn.registration_key();
        let (send, recv) = forward.forward;
        let mut ready = None;
        let terminal = forward.terminal.map(|(sender, receiver)| {
            ready = Some((self.terminal_ready.clone(), handle.0));
            (sender, receiver)
        });
        if let Some((error_code, ref reason)) = self.close {
            if let Ok(permit) = send.reserve_control(ControlKind::Close) {
                let _ = send.send_control(
                    permit,
                    ConnectionEvent::Close {
                        error_code,
                        reason: reason.clone(),
                    },
                );
            }
        }
        let owner = send.is_budgeted().then_some(owner);
        let meter = conn.receive_backing_meter();
        let (terminal_sender, terminal_receiver) = match terminal {
            Some((sender, receiver)) => (Some(sender), Some(receiver)),
            None => (None, None),
        };
        let entry = catalog::Entry {
            handle,
            key,
            sender: send,
            terminal: terminal_receiver,
            previous_socket: None,
        };
        if (entry.terminal.is_some() && !key.is_some_and(|key| self.catalog.reserve(key)))
            || !self.catalog.can_publish(&entry)
        {
            if let Some(meter) = &meter {
                meter.quarantine();
            }
            return Err(ConnectError::EventCapacityUnavailable);
        }
        self.catalog.publish(entry);
        let mut events = budgeted_events::EndpointEventSender::new(
            self.sender.clone(),
            owner,
            meter,
            terminal_sender,
        );
        events.ready = ready;
        if let Some(observer) = registration_observer {
            events.prepare_registration();
            if let Some(key) = conn.registration_key() {
                events.bind_registration(observer.bind(key));
            }
        }
        Ok(Connecting::new(handle, conn, events, recv, socket, runtime))
    }

    fn is_empty(&self) -> bool {
        self.catalog.is_empty()
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
        let mut endpoint = this.endpoint.inner.lock_state();
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
        backing_meter: Option<BackingMeter>,
        recv_buffer: PreparedRecvBuffer,
    ) -> Self {
        let (sender, events) = mpsc::unbounded_channel();
        let recv_state = RecvState::new(sender, backing_meter, recv_buffer);
        let prev_socket_connections =
            PreviousSocketInventory::new(recv_state.connections.catalog.is_native());
        Self(Arc::new_cyclic(|owner| EndpointInner {
            self_weak: owner.clone(),
            shared: Shared {
                incoming: Notify::new(),
                idle: Notify::new(),
                ref_count: AtomicUsize::new(0),
            },
            state: Mutex::new(State {
                socket,
                prev_socket: None,
                prev_socket_connections,
                inner,
                ipv6,
                events,
                pending_reverse_event: None,
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

        let endpoint = &mut *self.0.lock_state();
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
    /// Drops after recv_buf, returning its exact cold allocation layout.
    recv_buf_reservation: Option<BackingBudgetReservation>,
    response_scratch: Option<proto::ResponseScratch>,
    max_receive_segments: usize,
    recv_limiter: WorkLimiter,
    backing_meter: Option<BackingMeter>,
    budget_rejected_datagrams: u64,
    receive_backing_rejected_datagrams: u64,
    owned_event_rejected_datagrams: u64,
}

struct PreparedRecvBuffer {
    buf: Box<[u8]>,
    reservation: Option<BackingBudgetReservation>,
    max_receive_segments: usize,
    catalog: Catalog,
    response_scratch: Option<proto::ResponseScratch>,
}

impl RecvState {
    fn prepare_buffer(
        config: &EndpointConfig,
        socket: &dyn AsyncUdpSocket,
        meter: Option<&BackingMeter>,
        client_only: bool,
    ) -> io::Result<PreparedRecvBuffer> {
        let max_receive_segments = socket.max_receive_segments();
        let slot_len = (config.get_max_udp_payload_size().min(64 * 1024) as usize)
            .checked_mul(max_receive_segments)
            .filter(|len| *len > 0)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "receive slot layout"))?;
        let total = slot_len
            .checked_mul(BATCH_SIZE)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "receive batch layout"))?;
        let native = client_only && meter.is_some_and(|meter| meter.budget_limit().is_some());
        let response_layout = if native {
            proto::ResponseScratch::layout_bytes(slot_len)?
        } else {
            0
        };
        if let Some(limit) = meter.and_then(BackingMeter::budget_limit) {
            let one_datagram = BytesMut::metered_copy_layout_for_len(slot_len)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "receive copy layout"))?;
            if total
                .checked_add(one_datagram)
                .and_then(|needed| needed.checked_add(response_layout))
                .is_none_or(|needed| needed > limit)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "receive budget cannot hold batch and one legal datagram",
                ));
            }
        }
        let reservation = meter
            .map(|meter| meter.reserve_cold_layout(total))
            .transpose()
            .map_err(|error| io::Error::other(format!("receive buffer admission {error:?}")))?
            .flatten();
        let buf = if reservation.is_some() {
            let layout = Layout::array::<u8>(total).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "receive buffer layout")
            })?;
            let raw = unsafe { alloc_zeroed(layout) };
            if raw.is_null() {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "receive buffer allocation",
                ));
            }
            // SAFETY: raw was allocated zeroed with exactly the [u8; total]
            // layout, which Box<[u8]> uses to deallocate after the driver drops.
            unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(raw, total)) }
        } else {
            vec![0; total].into()
        };
        let catalog = Catalog::prepare(meter)?;
        let response_scratch = if native {
            meter
                .map(|meter| proto::ResponseScratch::prepare(slot_len, meter))
                .transpose()?
        } else {
            None
        };
        Ok(PreparedRecvBuffer {
            catalog,
            buf,
            reservation,
            max_receive_segments,
            response_scratch,
        })
    }

    fn new(
        sender: mpsc::UnboundedSender<(ConnectionHandle, ConnectionToEndpointEvent)>,
        backing_meter: Option<BackingMeter>,
        prepared: PreparedRecvBuffer,
    ) -> Self {
        let terminal_ready = Arc::new(budgeted_events::TerminalReady::default());
        let terminal_wait = Box::pin(terminal_ready.notify.clone().notified_owned());
        Self {
            connections: ConnectionSet {
                catalog: prepared.catalog,
                terminal_ready,
                terminal_wait,
                sender,
                close: None,
            },
            incoming: VecDeque::new(),
            recv_buf: prepared.buf,
            recv_buf_reservation: prepared.reservation,
            response_scratch: prepared.response_scratch,
            max_receive_segments: prepared.max_receive_segments,
            recv_limiter: WorkLimiter::new(RECV_TIME_BOUND),
            backing_meter,
            budget_rejected_datagrams: 0,
            receive_backing_rejected_datagrams: 0,
            owned_event_rejected_datagrams: 0,
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
        // Covers decode/signing and the synchronous socket callback, including
        // the default configuration without a logical pacing reservation.
        let _response_panic = fanout::RootPanic(&self.backing_meter);
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
                        if meta.len == 0 {
                            continue;
                        }
                        let mut data = if let Some(meter) = &self.backing_meter {
                            match BytesMut::try_copy_from_slice_metered(&buf[..meta.len], meter) {
                                Ok(data) => data,
                                Err(MeteredCopyError::BudgetFull) => {
                                    // This UDP datagram has not entered Proto or Root custody.
                                    // QUIC may retransmit stream frames; DATAGRAM loss is counted.
                                    let segments = meta.len.div_ceil(meta.stride.max(1));
                                    let rejected = u64::try_from(segments).unwrap_or(u64::MAX);
                                    self.budget_rejected_datagrams =
                                        self.budget_rejected_datagrams.saturating_add(rejected);
                                    self.receive_backing_rejected_datagrams = self
                                        .receive_backing_rejected_datagrams
                                        .saturating_add(rejected);
                                    continue;
                                }
                                Err(error) => {
                                    return Err(io::Error::other(format!(
                                        "receive backing admission {error:?}"
                                    )));
                                }
                            }
                        } else {
                            BytesMut::from(&buf[..meta.len])
                        };
                        while !data.is_empty() {
                            let buf = data.split_to(meta.stride.min(data.len()));
                            if let Some(meter) = &self.backing_meter {
                                if !buf.charge_shared_backing(meter) {
                                    return Err(io::Error::other("receive backing charge failed"));
                                }
                            }
                            let mut response_buffer = Vec::new();
                            let mut queue_full = false;
                            let mut missing_sender = false;
                            let senders = &self.connections.catalog;
                            let response = if endpoint.native_registration() {
                                proto::ResponseBuffer::from(
                                    self.response_scratch.as_mut().ok_or_else(|| {
                                        io::Error::other("native response authority unavailable")
                                    })?,
                                )
                            } else {
                                proto::ResponseBuffer::from(&mut response_buffer)
                            };
                            let admitted = endpoint.handle_with_admission(
                                now,
                                meta.addr,
                                meta.dst_ip,
                                meta.ecn.map(proto_ecn),
                                buf,
                                response,
                                |handle| match senders.get(&handle) {
                                    Some(sender) => match sender.reserve_proto() {
                                        Ok(permit) => Some(permit),
                                        Err(proto::owned_events::ReserveError::Full) => {
                                            queue_full = true;
                                            None
                                        }
                                        Err(proto::owned_events::ReserveError::Closed) => {
                                            missing_sender = true;
                                            None
                                        }
                                    },
                                    None => {
                                        missing_sender = true;
                                        None
                                    }
                                },
                            );
                            if queue_full {
                                self.budget_rejected_datagrams =
                                    self.budget_rejected_datagrams.saturating_add(1);
                                self.owned_event_rejected_datagrams =
                                    self.owned_event_rejected_datagrams.saturating_add(1);
                            }
                            if missing_sender {
                                if let Some(meter) = self.backing_meter.as_ref() {
                                    meter.quarantine();
                                }
                            }
                            match admitted {
                                Some(proto::AdmittedDatagram::Other(
                                    DatagramEvent::NewConnection(incoming),
                                )) => {
                                    if self.connections.close.is_none() {
                                        self.incoming.push_back(incoming);
                                    } else {
                                        let transmit =
                                            endpoint.refuse(incoming, &mut response_buffer);
                                        respond(transmit, &response_buffer, socket, endpoint, now);
                                    }
                                }
                                Some(proto::AdmittedDatagram::Connection(
                                    handle,
                                    event,
                                    permit,
                                )) => {
                                    let sent = self.connections.catalog.get(&handle).is_some_and(
                                        |sender| {
                                            sender
                                                .send_proto(
                                                    permit,
                                                    ConnectionEvent::Proto {
                                                        event,
                                                        current_socket_rx_rebind_generation,
                                                    },
                                                )
                                                .is_ok()
                                        },
                                    );
                                    if !sent {
                                        if let Some(meter) = self.backing_meter.as_ref() {
                                            meter.quarantine();
                                        }
                                    }
                                }
                                Some(proto::AdmittedDatagram::Other(DatagramEvent::Response(
                                    transmit,
                                ))) => {
                                    let bytes = if endpoint.native_registration() {
                                        self.response_scratch
                                            .as_ref()
                                            .map_or(&[][..], proto::ResponseScratch::as_slice)
                                    } else {
                                        &response_buffer
                                    };
                                    respond(transmit, bytes, socket, endpoint, now);
                                }
                                Some(proto::AdmittedDatagram::Other(
                                    DatagramEvent::ConnectionEvent(..),
                                )) => unreachable!("admitted connection has its own variant"),
                                None => {}
                            }
                        }
                    }
                }
                Poll::Pending => {
                    return Ok(PollProgress { keep_going: false });
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
                return Ok(PollProgress { keep_going: true });
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
mod endpoint_driver_loss_tests {
    use super::idle_or_driver_lost;

    #[test]
    fn driver_loss_is_fail_closed_even_after_connection_map_is_empty() {
        assert_eq!(idle_or_driver_lost(true, true), Some(false));
        assert_eq!(idle_or_driver_lost(false, true), Some(true));
        assert_eq!(idle_or_driver_lost(false, false), None);
    }
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

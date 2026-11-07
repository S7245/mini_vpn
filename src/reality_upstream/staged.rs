//! Retained REALITY open authority: DNS, TCP, borrowed handshake and retirement.
//! Does not own DATA scheduling or Failover policy. Entry is the staged trait;
//! tests use real local TCP and the existing REALITY peer in `tests`.

use super::*;
use crate::upstream::{
    OpenedTcpRelay, TcpOpenAttempt, TcpOpenCancel, TcpOpenPhase, TcpOpenRequest,
    TcpOpenUnavailable, TcpOpenView,
};
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::VecDeque;
use std::future::Future;
use std::net::{Shutdown, SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Waker;
use std::time::{Duration, Instant};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

const DNS_JOBS: usize = 128;
const DNS_ADDRESSES: usize = 32;
const RETIREMENT: Duration = Duration::from_secs(5);
const OWNED: u8 = 0;
const LIVE: u8 = 1;
const DESTRUCTING: u8 = 2;
const RELEASED: u8 = 3;
const BROKEN: u8 = 4;

fn dns_slots() -> &'static Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(Semaphore::new(DNS_JOBS)))
}

fn bounded_addresses(
    addresses: impl Iterator<Item = SocketAddr>,
) -> io::Result<VecDeque<SocketAddr>> {
    let values: VecDeque<_> = addresses.take(DNS_ADDRESSES + 1).collect();
    if values.is_empty() || values.len() > DNS_ADDRESSES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "REALITY DNS address capacity",
        ));
    }
    Ok(values)
}

pub(super) fn create(
    upstream: Arc<RealityUpstream>,
    request: TcpOpenRequest,
) -> Result<Attempt, TcpOpenUnavailable> {
    if matches!(&request.target, TargetAddr::DomainPort { host, .. } if host.is_empty() || host.len() > 255)
    {
        return Err(TcpOpenUnavailable::InvalidTarget);
    }
    Ok(Attempt {
        upstream: Some(upstream),
        request,
        state: State::Cold,
        addresses: VecDeque::new(),
        witness: None,
        release: Arc::new(Release {
            phase: AtomicU8::new(OWNED),
            wake: Mutex::new(None),
        }),
        failure: None,
        dns_failure: None,
        connect_error: None,
        connect_deadline: None,
        handshake_deadline: None,
        settle_deadline: None,
        cancelled: false,
        quarantined: false,
        interrupted: false,
        shutdown: None,
        #[cfg(test)]
        dns_gate: None,
        #[cfg(test)]
        shutdown_fault: false,
        #[cfg(test)]
        return_observation: None,
    })
}

type Handshake = Pin<
    Box<dyn Future<Output = Result<(TcpStream, handshake::HandshakeOutput), ClientError>> + Send>,
>;

enum State {
    Cold,
    Dns(DnsJob),
    NextAddress,
    Connect(TcpStream),
    Handshake(Handshake),
    Ready(Carrier),
    Transferred,
    Retiring,
    Settled,
}

struct DnsJob {
    task: JoinHandle<io::Result<VecDeque<SocketAddr>>>,
    _slot: Arc<OwnedSemaphorePermit>,
}

/// Owns control resources before the next turn, including an unfinished resolver.
/// `interrupted` is installed before fallible/destructive transitions; unwinding
/// cannot turn unknown ownership into zero. Q retains this original authority.
pub(super) struct Attempt {
    upstream: Option<Arc<RealityUpstream>>,
    request: TcpOpenRequest,
    state: State,
    addresses: VecDeque<SocketAddr>,
    witness: Option<Socket>,
    release: Arc<Release>,
    failure: Option<ClientError>,
    dns_failure: Option<ClientError>,
    connect_error: Option<io::Error>,
    connect_deadline: Option<Instant>,
    handshake_deadline: Option<Instant>,
    settle_deadline: Option<Instant>,
    cancelled: bool,
    quarantined: bool,
    interrupted: bool,
    shutdown: Option<Result<(), Arc<io::Error>>>,
    #[cfg(test)]
    dns_gate: Option<Arc<tests::DnsGate>>,
    #[cfg(test)]
    shutdown_fault: bool,
    #[cfg(test)]
    return_observation: Option<tests::ReturnObservation>,
}

struct Carrier {
    inner: Option<RealityStream<OwnedReadHalf, OwnedWriteHalf>>,
    release: Option<Arc<Release>>,
}

struct Release {
    phase: AtomicU8,
    wake: Mutex<Option<Waker>>,
}

/// Also wakes during unwind, after returning the original carrier's Arc. The
/// observer sees both the marker and physical shell ownership, never one alone.
struct ReleaseGuard(Option<Arc<Release>>);
impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        let Some(release) = self.0.take() else {
            return;
        };
        let wake = release
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(release);
        if let Some(wake) = wake {
            wake.wake();
        }
    }
}

impl Drop for Carrier {
    fn drop(&mut self) {
        let Some(release) = self.release.take() else {
            return;
        };
        let guard = ReleaseGuard(Some(release));
        let Some(release) = &guard.0 else {
            return;
        };
        if release
            .phase
            .compare_exchange(LIVE, DESTRUCTING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            release.phase.store(BROKEN, Ordering::Release);
            return;
        }
        // A marker before Drop is not a receipt. If inner destruction unwinds,
        // DESTRUCTING and the provider's physical socket witness survive.
        drop(self.inner.take());
        release.phase.store(RELEASED, Ordering::Release);
    }
}

impl AsyncRead for Carrier {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.inner {
            Some(inner) => Pin::new(inner).poll_read(cx, buf),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "REALITY carrier released",
            ))),
        }
    }
}

impl AsyncWrite for Carrier {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.inner {
            Some(inner) => Pin::new(inner).poll_write(cx, buf),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "REALITY carrier released",
            ))),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.inner {
            Some(inner) => Pin::new(inner).poll_flush(cx),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "REALITY carrier released",
            ))),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.inner {
            Some(inner) => Pin::new(inner).poll_shutdown(cx),
            None => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "REALITY carrier released",
            ))),
        }
    }
}

fn connect_in_progress(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    ) {
        return true;
    }
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::EINPROGRESS)
    }
    #[cfg(windows)]
    {
        error.raw_os_error() == Some(10036) /* WSAEINPROGRESS */
    }
    #[cfg(not(any(unix, windows)))]
    {
        false
    }
}

impl Attempt {
    fn begin(&mut self) -> bool {
        if self.interrupted || self.quarantined {
            self.quarantine("interrupted REALITY ownership transition");
            return false;
        }
        self.interrupted = true;
        true
    }
    fn retain(&mut self, error: ClientError) {
        if self.failure.is_none() {
            self.failure = Some(error);
        }
    }
    fn quarantine(&mut self, message: &'static str) {
        self.retain(ClientError::Io(io::Error::other(message)));
        self.quarantined = true;
    }
    fn dns_failed(&mut self, error: tokio::task::JoinError) {
        let error = ClientError::Io(io::Error::other(error));
        if self.failure.is_none() {
            self.failure = Some(error);
        } else {
            self.dns_failure = Some(error);
        }
        self.quarantined = true;
    }
    fn tighten(&mut self, deadline: Instant) {
        self.settle_deadline = Some(
            self.settle_deadline
                .map_or(deadline, |old| old.min(deadline)),
        );
    }
    fn fail(&mut self, error: ClientError, now: Instant) {
        self.retain(error);
        self.cancelled = true;
        self.tighten(now + RETIREMENT);
    }
    fn note_cancel(&mut self, reason: TcpOpenCancel, deadline: Instant) {
        if matches!(self.state, State::Settled) {
            return;
        }
        self.cancelled = true;
        self.tighten(deadline);
        self.retain(ClientError::Io(io::Error::new(
            if reason == TcpOpenCancel::Deadline {
                io::ErrorKind::TimedOut
            } else {
                io::ErrorKind::Interrupted
            },
            format!("REALITY open {reason:?}"),
        )));
    }

    fn resolve(&mut self, now: Instant) {
        let Some(upstream) = &self.upstream else {
            self.quarantine("REALITY configuration owner missing");
            return;
        };
        self.connect_deadline = Some(
            self.request
                .deadline
                .min(now + Duration::from_secs(REALITY_HANDSHAKE_TIMEOUT_SECS)),
        );
        if let Ok(address) = upstream.cfg.server.parse::<SocketAddr>() {
            self.addresses.push_back(address);
            self.state = State::NextAddress;
            return;
        }
        let Ok(slot) = dns_slots().clone().try_acquire_owned() else {
            self.fail(
                ClientError::Io(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "REALITY DNS jobs full",
                )),
                now,
            );
            return;
        };
        let server = upstream.cfg.server.clone();
        #[cfg(test)]
        let gate = self.dns_gate.clone();
        let slot = Arc::new(slot);
        let worker_slot = slot.clone();
        let task = tokio::task::spawn_blocking(move || {
            let _original_slot = worker_slot;
            #[cfg(test)]
            if let Some(gate) = gate {
                gate.wait();
            }
            bounded_addresses(server.to_socket_addrs()?)
        });
        self.state = State::Dns(DnsJob { task, _slot: slot });
    }

    fn connect(&mut self, address: SocketAddr) -> io::Result<()> {
        let socket = Socket::new(
            Domain::for_address(address),
            Type::STREAM,
            Some(Protocol::TCP),
        )?;
        socket.set_nonblocking(true)?;
        // Duplicate before connect. No pending connect or handshake can escape
        // the provider's physical close authority, including cancellation.
        self.witness = Some(socket.try_clone()?);
        let result = socket.connect(&address.into());
        let stream = TcpStream::from_std(socket.into())?;
        self.state = State::Connect(stream);
        match result {
            Ok(()) => Ok(()),
            Err(error) if connect_in_progress(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn close_witness(&mut self) -> bool {
        if let Some(witness) = &self.witness {
            let result = self.shutdown_socket(witness).map_err(Arc::new);
            let closed = result.is_ok()
                || result
                    .as_ref()
                    .is_err_and(|error| error.kind() == io::ErrorKind::NotConnected);
            if !closed {
                if let Err(error) = &result {
                    self.retain(ClientError::Io(io::Error::other(error.clone())));
                }
                self.shutdown = Some(result);
                self.quarantined = true;
                return false;
            }
            self.shutdown = Some(result);
            // All original socket users have returned before this point. This
            // closes the last typed descriptor after explicit kernel shutdown.
            drop(self.witness.take());
        }
        true
    }

    fn shutdown_socket(&self, witness: &Socket) -> io::Result<()> {
        #[cfg(test)]
        if self.shutdown_fault {
            return Err(io::Error::other(
                "injected REALITY shutdown receipt failure",
            ));
        }
        witness.shutdown(Shutdown::Both)
    }

    fn advance(&mut self, cx: &mut Context<'_>, now: Instant) {
        if matches!(self.state, State::Transferred | State::Settled) {
            return;
        }
        let phase_expired = match self.state {
            State::Cold => false,
            State::Dns(_) | State::NextAddress | State::Connect(_) => {
                self.connect_deadline.is_some_and(|d| now >= d)
            }
            State::Handshake(_) => self.handshake_deadline.is_some_and(|d| now >= d),
            _ => false,
        };
        if !self.cancelled && (now >= self.request.deadline || phase_expired) {
            self.note_cancel(TcpOpenCancel::Deadline, now + RETIREMENT);
        }
        if self.cancelled {
            self.retire(cx, now);
            return;
        }
        match &mut self.state {
            State::Cold => self.resolve(now),
            State::Dns(job) => {
                if let Poll::Ready(result) = Pin::new(&mut job.task).poll(cx) {
                    match result {
                        Ok(Ok(addresses)) => {
                            self.addresses = addresses;
                            self.state = State::NextAddress;
                        }
                        Ok(Err(error)) => {
                            self.state = State::Retiring;
                            self.fail(ClientError::Io(error), now);
                        }
                        Err(error) => self.dns_failed(error),
                    }
                }
            }
            State::NextAddress => {
                if self.witness.is_some() {
                    self.quarantine("REALITY previous candidate still owns a socket");
                    return;
                }
                if let Some(address) = self.addresses.pop_front() {
                    if let Err(error) = self.connect(address) {
                        self.next_address(error);
                    }
                } else {
                    let error = self.connect_error.take().unwrap_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::AddrNotAvailable,
                            "REALITY no resolved address",
                        )
                    });
                    self.fail(ClientError::Io(error), now);
                }
            }
            State::Connect(stream) => {
                let result = match stream.poll_write_ready(cx) {
                    Poll::Pending => return,
                    Poll::Ready(Err(error)) => Err(error),
                    Poll::Ready(Ok(())) => stream.take_error().and_then(|error| match error {
                        Some(error) => Err(error),
                        None => stream.peer_addr().map(|_| ()),
                    }),
                };
                if let Err(error) = result {
                    self.next_address(error);
                    return;
                }
                let Some(upstream) = self.upstream.clone() else {
                    self.quarantine("REALITY handshake configuration missing");
                    return;
                };
                let State::Connect(mut stream) =
                    std::mem::replace(&mut self.state, State::Retiring)
                else {
                    return;
                };
                let target = self.request.target.clone();
                self.handshake_deadline = Some(
                    self.request
                        .deadline
                        .min(now + Duration::from_secs(REALITY_HANDSHAKE_TIMEOUT_SECS)),
                );
                self.state = State::Handshake(Box::pin(async move {
                    let result = upstream.establish(&mut stream, &target).await;
                    result.map(|output| (stream, output))
                }));
            }
            State::Handshake(future) => {
                if let Poll::Ready(result) = future.as_mut().poll(cx) {
                    // Drop the completed future shell before publishing the carrier.
                    let old = std::mem::replace(&mut self.state, State::Retiring);
                    drop(old);
                    match result {
                        Ok((stream, out)) => {
                            let (rh, wh) = stream.into_split();
                            let inner = RealityStream::new(
                                rh,
                                wh,
                                out.recv_keys,
                                out.send_keys,
                                out.leftover,
                                out.s_ap_secret,
                                out.c_ap_secret,
                            );
                            self.release.phase.store(LIVE, Ordering::Release);
                            self.state = State::Ready(Carrier {
                                inner: Some(inner),
                                release: Some(self.release.clone()),
                            });
                            self.upstream = None;
                        }
                        Err(error) => self.fail(error, now),
                    }
                }
            }
            State::Ready(_) | State::Retiring | State::Transferred | State::Settled => {}
        }
    }

    fn next_address(&mut self, error: io::Error) {
        self.connect_error = Some(error);
        let old = std::mem::replace(&mut self.state, State::Retiring);
        drop(old);
        if self.close_witness() {
            self.state = State::NextAddress;
        }
    }

    fn ready_valid(&self) -> bool {
        let State::Ready(carrier) = &self.state else {
            return false;
        };
        self.witness.is_some()
            && carrier.inner.is_some()
            && carrier
                .release
                .as_ref()
                .is_some_and(|release| Arc::ptr_eq(release, &self.release))
            && self.release.phase.load(Ordering::Acquire) == LIVE
            && Arc::strong_count(&self.release) == 2
    }

    fn retire(&mut self, cx: &mut Context<'_>, now: Instant) {
        if matches!(self.state, State::Settled) {
            return;
        }
        let debt = self.witness.is_some()
            || matches!(self.state, State::Dns(_))
            || self.release.phase.load(Ordering::Acquire) == LIVE
            || Arc::strong_count(&self.release) != 1;
        if debt && self.settle_deadline.is_some_and(|deadline| now >= deadline) {
            self.quarantine("REALITY physical retirement deadline");
            return;
        }
        if matches!(self.state, State::Ready(_)) && !self.ready_valid() {
            self.quarantine("REALITY Ready ownership inventory mismatch");
            return;
        }
        // A blocking resolver cannot be aborted. Keep its original JoinHandle,
        // even beyond observation timeout; a late result must never open TCP.
        if let State::Dns(job) = &mut self.state {
            match Pin::new(&mut job.task).poll(cx) {
                Poll::Pending => {
                    if self.settle_deadline.is_some_and(|d| now >= d) {
                        self.quarantine("REALITY DNS retirement deadline");
                    }
                    return;
                }
                Poll::Ready(Ok(_)) => self.state = State::Retiring,
                Poll::Ready(Err(error)) => {
                    self.dns_failed(error);
                    return;
                }
            }
        }
        // Install retirement before dropping a handshake/Ready carrier. The
        // witness stays in this owner if any destructor interrupts the turn.
        let old = std::mem::replace(&mut self.state, State::Retiring);
        drop(old);
        self.addresses.clear();
        self.upstream = None;
        *self
            .release
            .wake
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(cx.waker().clone());
        let refs = Arc::strong_count(&self.release);
        #[cfg(test)]
        if let Some(observation) = self.return_observation.take() {
            observation.observe();
        }
        // strong_count is a Relaxed load. Observing the carrier's Release Arc
        // decrement needs an Acquire fence before trusting its final marker.
        if refs == 1 {
            std::sync::atomic::fence(Ordering::Acquire);
        }
        let released = self.release.phase.load(Ordering::Acquire);
        if refs > 2 {
            self.quarantine("REALITY carrier ownership inventory mismatch");
            return;
        }
        if refs != 1 {
            if self.settle_deadline.is_some_and(|d| now >= d) {
                self.quarantine("REALITY carrier retirement deadline");
            }
            return;
        }
        if !matches!(released, OWNED | RELEASED) {
            self.quarantine("REALITY carrier destruction unknown");
            return;
        }
        if self.witness.is_some() && self.settle_deadline.is_some_and(|d| now >= d) {
            self.quarantine("REALITY physical retirement deadline");
            return;
        }
        if self.close_witness() {
            self.state = State::Settled;
        }
    }
}

impl TcpOpenAttempt<OpenedTcpRelay> for Attempt {
    fn step(&mut self, cx: &mut Context<'_>, now: Instant) {
        if !self.begin() {
            return;
        }
        self.advance(cx, now);
        self.interrupted = false;
    }
    fn view(&self) -> TcpOpenView {
        let phase = if self.interrupted || self.quarantined {
            TcpOpenPhase::Quarantined
        } else {
            match self.state {
                State::Cold => TcpOpenPhase::Unstarted,
                State::Dns(_) => TcpOpenPhase::Acquiring,
                State::NextAddress => TcpOpenPhase::Opening,
                State::Connect(_) => TcpOpenPhase::Connecting,
                State::Handshake(_) => TcpOpenPhase::Preparing,
                State::Ready(_) => TcpOpenPhase::Ready,
                State::Transferred => TcpOpenPhase::Transferred,
                State::Retiring => TcpOpenPhase::Retiring,
                State::Settled => TcpOpenPhase::Settled,
            }
        };
        TcpOpenView {
            phase,
            lease_held: self.interrupted
                || self.quarantined
                || self.witness.is_some()
                || matches!(self.state, State::Dns(_)),
            retirement_pending: self.interrupted
                || self.quarantined
                || self.witness.is_some()
                || matches!(self.state, State::Dns(_)),
            connect_bytes: 0,
            connect_written: 0,
        }
    }
    fn cancel(&mut self, reason: TcpOpenCancel, deadline: Instant) {
        if !self.begin() {
            return;
        }
        self.note_cancel(reason, deadline);
        self.interrupted = false;
    }
    fn take_carrier(&mut self, now: Instant) -> Option<OpenedTcpRelay> {
        if !self.begin() {
            return None;
        }
        if now >= self.request.deadline {
            self.note_cancel(TcpOpenCancel::Deadline, now + RETIREMENT);
        }
        if matches!(self.state, State::Ready(_)) && !self.ready_valid() {
            self.quarantine("REALITY Ready ownership inventory mismatch");
            self.interrupted = false;
            return None;
        }
        let result = if !self.cancelled && matches!(self.state, State::Ready(_)) {
            match std::mem::replace(&mut self.state, State::Transferred) {
                State::Ready(carrier) => Some(OpenedTcpRelay::Generic(Box::new(carrier))),
                old => {
                    self.state = old;
                    self.quarantine("REALITY Ready carrier missing");
                    None
                }
            }
        } else {
            None
        };
        self.interrupted = false;
        result
    }
    fn settle(&mut self, cx: &mut Context<'_>, now: Instant, deadline: Instant) {
        if !self.begin() {
            return;
        }
        self.tighten(deadline);
        if !matches!(
            self.state,
            State::Transferred | State::Retiring | State::Settled
        ) {
            self.note_cancel(TcpOpenCancel::Cancelled, deadline);
        }
        self.retire(cx, now);
        self.interrupted = false;
    }
    fn failure(&self) -> Option<&ClientError> {
        self.failure.as_ref()
    }
}

#[cfg(test)]
mod tests;

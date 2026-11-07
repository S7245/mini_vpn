//! Owns one real server UDP ciphertext slot and a bounded loss transaction.
//! It does not inspect QUIC plaintext, alter a stream, own a task or change time.
//! Forward quotas commit only after real socket acceptance. Restore retains the
//! same slot across WouldBlock/cancellation and publishes Pass only after send.
//! The gap and direct fatal tracers own scenario control and acceptance.
use quinn::udp::{EcnCodepoint, RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, Runtime, UdpPoller};
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

const MAX_UDP_BYTES: usize = 65535;
const SUFFIX_DATAGRAMS: u8 = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Mode {
    Pass,
    Prefix,
    Burst(u8),
    Suppress,
    Flood,
    RxDrop,
}
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Counts {
    pub(super) attempts: u64,
    pub(super) forwarded: u64,
    pub(super) captured: u64,
    pub(super) dropped: u64,
    pub(super) dropped_bytes: u64,
    pub(super) restored: u64,
    pub(super) restored_bytes: u64,
    pub(super) rx_dropped_datagrams: u64,
    pub(super) rx_dropped_bytes: u64,
}
impl Counts {
    fn room(self) -> io::Result<()> {
        let bound = u64::MAX - MAX_UDP_BYTES as u64;
        if [
            self.attempts,
            self.forwarded,
            self.captured,
            self.dropped,
            self.dropped_bytes,
            self.restored,
            self.restored_bytes,
        ]
        .iter()
        .all(|n| *n <= bound)
        {
            Ok(())
        } else {
            Err(io::Error::other("bounded wire counter overflow"))
        }
    }
}
#[derive(Debug)]
struct Held {
    bytes: Box<[u8]>,
    destination: SocketAddr,
    ecn: Option<EcnCodepoint>,
    segment_size: Option<usize>,
    src_ip: Option<IpAddr>,
}
impl Held {
    fn transmit(&self) -> Transmit<'_> {
        Transmit {
            destination: self.destination,
            ecn: self.ecn,
            contents: &self.bytes,
            segment_size: self.segment_size,
            src_ip: self.src_ip,
        }
    }
}
#[derive(Debug)]
struct State {
    mode: Mode,
    held: Option<Held>,
    counts: Counts,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct Snapshot {
    pub(super) mode: Mode,
    pub(super) held_bytes: usize,
    pub(super) held_pointer: Option<usize>,
    pub(super) counts: Counts,
}
#[derive(Debug)]
pub(super) struct FaultSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    state: Mutex<State>,
}
impl FaultSocket {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) fn snapshot(&self) -> Snapshot {
        let state = self.state();
        Snapshot {
            mode: state.mode,
            held_bytes: state.held.as_ref().map_or(0, |h| h.bytes.len()),
            held_pointer: state.held.as_ref().map(|h| h.bytes.as_ptr() as usize),
            counts: state.counts,
        }
    }
    pub(super) fn prefix(&self) -> io::Result<()> {
        let mut s = self.state();
        if s.mode != Mode::Pass || s.held.is_some() || s.counts.captured != 0 {
            return Err(io::Error::other("wire prefix must arm once"));
        }
        s.mode = Mode::Prefix;
        Ok(())
    }
    pub(super) fn suffix(&self) -> io::Result<()> {
        let mut s = self.state();
        if s.mode != Mode::Prefix || s.held.is_none() {
            return Err(io::Error::other(
                "suffix requires the actual retained ciphertext",
            ));
        }
        s.mode = Mode::Burst(SUFFIX_DATAGRAMS);
        Ok(())
    }
    pub(super) fn suppress(&self) -> io::Result<()> {
        let mut s = self.state();
        if !matches!(s.mode, Mode::Burst(_) | Mode::Suppress) || s.held.is_none() {
            return Err(io::Error::other("gap suppression requires an armed burst"));
        }
        s.mode = Mode::Suppress;
        Ok(())
    }
    pub(super) fn flood(&self) -> io::Result<()> {
        let mut s = self.state();
        if s.mode != Mode::Prefix || s.held.is_none() {
            return Err(io::Error::other(
                "flood requires original held prefix transaction",
            ));
        }
        s.mode = Mode::Flood;
        Ok(())
    }
    pub(super) fn rx_drop(&self) -> io::Result<()> {
        let mut s = self.state();
        if s.mode != Mode::Pass || s.held.is_some() {
            return Err(io::Error::other("RX drop requires an unheld pass socket"));
        }
        s.mode = Mode::RxDrop;
        Ok(())
    }
    fn try_restore(&self) -> io::Result<()> {
        let mut s = self.state();
        s.counts.room()?;
        let Some(held) = s.held.as_ref() else {
            s.mode = Mode::Pass;
            return Ok(());
        };
        let bytes = held.bytes.len() as u64;
        // Borrow the same box. Neither WouldBlock nor observer cancellation takes it.
        self.inner.try_send(&held.transmit())?;
        s.counts.restored += 1;
        s.counts.restored_bytes += bytes;
        s.held = None;
        s.mode = Mode::Pass;
        Ok(())
    }
    pub(super) async fn restore(&self) -> io::Result<()> {
        let mut poller = self.inner.clone().create_io_poller();
        loop {
            match self.try_restore() {
                Ok(()) => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    std::future::poll_fn(|cx| poller.as_mut().poll_writable(cx)).await?;
                }
                Err(e) => return Err(e),
            }
        }
    }
}
impl AsyncUdpSocket for FaultSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        self.inner.clone().create_io_poller()
    }
    fn try_send(&self, t: &Transmit<'_>) -> io::Result<()> {
        if t.contents.is_empty()
            || t.contents.len() > MAX_UDP_BYTES
            || t.segment_size
                .is_some_and(|n| n == 0 || t.contents.len() > n)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "one bounded real UDP datagram required",
            ));
        }
        let mut s = self.state();
        s.counts.room()?;
        s.counts.attempts += 1;
        match s.mode {
            Mode::Pass | Mode::Flood | Mode::RxDrop => {
                self.inner.try_send(t)?;
                s.counts.forwarded += 1;
            }
            Mode::Burst(remaining) if remaining > 0 => {
                self.inner.try_send(t)?;
                s.counts.forwarded += 1;
                s.mode = if remaining == 1 {
                    Mode::Suppress
                } else {
                    Mode::Burst(remaining - 1)
                };
            }
            Mode::Prefix if s.held.is_none() => {
                s.held = Some(Held {
                    bytes: t.contents.to_vec().into_boxed_slice(),
                    destination: t.destination,
                    ecn: t.ecn,
                    segment_size: t.segment_size,
                    src_ip: t.src_ip,
                });
                s.counts.captured += 1;
            }
            Mode::Prefix | Mode::Suppress | Mode::Burst(_) => {
                s.counts.dropped += 1;
                s.counts.dropped_bytes += t.contents.len() as u64;
            }
        }
        Ok(())
    }
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        if !matches!(self.snapshot().mode, Mode::Flood | Mode::RxDrop) {
            return self.inner.poll_recv(cx, bufs, meta);
        }
        // Real receive/drop, not invented readiness. Original buffers only;
        // GRO metadata counts actual datagrams, no retained ACK or packet queue.
        for _ in 0..8 {
            match self.inner.poll_recv(cx, bufs, meta) {
                Poll::Ready(Ok(n)) => {
                    let mut datagrams = 0u64;
                    let mut bytes = 0u64;
                    for m in &meta[..n] {
                        if m.len == 0 {
                            continue;
                        }
                        if m.stride == 0 {
                            return Poll::Ready(Err(io::Error::other("invalid real GRO stride")));
                        }
                        let Some(d) = datagrams.checked_add(m.len.div_ceil(m.stride) as u64) else {
                            return Poll::Ready(Err(io::Error::other(
                                "receive datagram accounting overflow",
                            )));
                        };
                        let Some(b) = bytes.checked_add(m.len as u64) else {
                            return Poll::Ready(Err(io::Error::other(
                                "receive byte accounting overflow",
                            )));
                        };
                        datagrams = d;
                        bytes = b;
                    }
                    let mut s = self.state();
                    let Some(d) = s.counts.rx_dropped_datagrams.checked_add(datagrams) else {
                        return Poll::Ready(Err(io::Error::other(
                            "bounded dropped datagram overflow",
                        )));
                    };
                    let Some(b) = s.counts.rx_dropped_bytes.checked_add(bytes) else {
                        return Poll::Ready(Err(io::Error::other("bounded dropped byte overflow")));
                    };
                    s.counts.rx_dropped_datagrams = d;
                    s.counts.rx_dropped_bytes = b;
                }
                other => return other,
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn max_transmit_segments(&self) -> usize {
        1
    }
    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }
    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}
pub(super) fn server(config: quinn::ServerConfig) -> (quinn::Endpoint, Arc<FaultSocket>) {
    let runtime: Arc<dyn Runtime> = Arc::new(quinn::TokioRuntime);
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    udp.set_nonblocking(true).unwrap();
    let inner = runtime.wrap_udp_socket(udp).unwrap();
    let fault = Arc::new(FaultSocket {
        inner,
        state: Mutex::new(State {
            mode: Mode::Pass,
            held: None,
            counts: Counts::default(),
        }),
    });
    let endpoint = quinn::Endpoint::new_with_abstract_socket(
        Default::default(),
        Some(config),
        fault.clone(),
        runtime,
    )
    .unwrap();
    (endpoint, fault)
}

//! Aggregate, bounded QUIC UDP egress service for a single client endpoint.

use quinn::{AsyncTimer, AsyncUdpSocket, Runtime, UdpPoller};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

pub(crate) const BOUNDED_SEND_DATAGRAMS: usize = 48;
pub(crate) const BOUNDED_SEND_COOLDOWN: Duration = Duration::from_millis(2);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct QuicUdpSendServiceSnapshot {
    pub accepted_bytes: u64,
    pub accepted_datagrams: u64,
    pub accepted_bytes_per_sec: u64,
    pub accepted_datagrams_per_sec: u64,
    pub service_elapsed_ns: u64,
    pub gate_would_block: u64,
    pub batch_closes: u64,
    pub timer_blocked_polls: u64,
    pub cooldown_rearms: u64,
    pub total_rearm_lateness_ns: u64,
    pub max_rearm_lateness_ns: u64,
    pub wasted_datagrams: u64,
    pub inner_would_block: u64,
    pub inner_errors: u64,
    pub invalid_transmits: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct QuicUdpSendServiceStats {
    shared: Arc<Mutex<SharedState>>,
}

impl QuicUdpSendServiceStats {
    pub(crate) fn snapshot(&self) -> QuicUdpSendServiceSnapshot {
        lock_unpoisoned(&self.shared).snapshot()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchDecision {
    Granted,
    WaitUntil(Instant),
    TooLarge,
}

#[derive(Debug)]
struct SendBatch {
    capacity: usize,
    remaining: usize,
    cooldown: Duration,
    blocked_until: Option<Instant>,
}

impl SendBatch {
    fn new(capacity: usize, cooldown: Duration) -> Self {
        debug_assert!(capacity > 0);
        Self {
            capacity,
            remaining: capacity,
            cooldown,
            blocked_until: None,
        }
    }

    #[cfg(test)]
    fn try_reserve(&mut self, now: Instant, datagrams: usize) -> BatchDecision {
        if datagrams == 0 || datagrams > self.capacity {
            return BatchDecision::TooLarge;
        }
        self.refresh(now);
        self.try_reserve_ready(now, datagrams)
    }

    fn refresh(&mut self, now: Instant) -> Option<Duration> {
        if let Some(deadline) = self.blocked_until.filter(|deadline| now >= *deadline) {
            self.remaining = self.capacity;
            self.blocked_until = None;
            Some(now.saturating_duration_since(deadline))
        } else {
            None
        }
    }

    fn try_reserve_ready(&mut self, now: Instant, datagrams: usize) -> BatchDecision {
        if datagrams == 0 || datagrams > self.capacity {
            return BatchDecision::TooLarge;
        }
        if let Some(deadline) = self.blocked_until {
            return BatchDecision::WaitUntil(deadline);
        }
        if datagrams > self.remaining {
            self.remaining = 0;
            let deadline = now + self.cooldown;
            self.blocked_until = Some(deadline);
            return BatchDecision::WaitUntil(deadline);
        }
        self.remaining -= datagrams;
        if self.remaining == 0 {
            self.blocked_until = Some(now + self.cooldown);
        }
        BatchDecision::Granted
    }

    fn refund(&mut self, datagrams: usize) {
        self.remaining = self.remaining.saturating_add(datagrams).min(self.capacity);
        self.blocked_until = None;
    }
}

#[derive(Debug)]
struct SharedState {
    batch: SendBatch,
    stats: QuicUdpSendServiceSnapshot,
    service_started_at: Option<Instant>,
    service_last_accepted_at: Option<Instant>,
}

impl SharedState {
    fn new() -> Self {
        Self {
            batch: SendBatch::new(BOUNDED_SEND_DATAGRAMS, BOUNDED_SEND_COOLDOWN),
            stats: QuicUdpSendServiceSnapshot::default(),
            service_started_at: None,
            service_last_accepted_at: None,
        }
    }

    fn refresh(&mut self, now: Instant) {
        if let Some(lateness) = self.batch.refresh(now) {
            let lateness_ns = duration_ns(lateness);
            self.stats.cooldown_rearms = self.stats.cooldown_rearms.saturating_add(1);
            self.stats.total_rearm_lateness_ns = self
                .stats
                .total_rearm_lateness_ns
                .saturating_add(lateness_ns);
            self.stats.max_rearm_lateness_ns = self.stats.max_rearm_lateness_ns.max(lateness_ns);
        }
    }

    fn record_accepted(&mut self, now: Instant, bytes: usize, datagrams: usize) {
        self.service_started_at.get_or_insert(now);
        self.service_last_accepted_at = Some(now);
        self.stats.accepted_bytes = self.stats.accepted_bytes.saturating_add(bytes as u64);
        self.stats.accepted_datagrams = self
            .stats
            .accepted_datagrams
            .saturating_add(datagrams as u64);
    }

    fn snapshot(&self) -> QuicUdpSendServiceSnapshot {
        let mut snapshot = self.stats;
        let elapsed_ns = self
            .service_started_at
            .zip(self.service_last_accepted_at)
            .map(|(started, ended)| duration_ns(ended.saturating_duration_since(started)))
            .unwrap_or(0);
        snapshot.service_elapsed_ns = elapsed_ns;
        snapshot.accepted_bytes_per_sec = per_second(snapshot.accepted_bytes, elapsed_ns);
        snapshot.accepted_datagrams_per_sec = per_second(snapshot.accepted_datagrams, elapsed_ns);
        snapshot
    }
}

#[derive(Debug)]
struct BoundedUdpSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    runtime: Arc<dyn Runtime>,
    shared: Arc<Mutex<SharedState>>,
}

pub(crate) fn bounded_udp_socket(
    inner: Arc<dyn AsyncUdpSocket>,
    runtime: Arc<dyn Runtime>,
) -> (Arc<dyn AsyncUdpSocket>, QuicUdpSendServiceStats) {
    let shared = Arc::new(Mutex::new(SharedState::new()));
    let stats = QuicUdpSendServiceStats {
        shared: shared.clone(),
    };
    (
        Arc::new(BoundedUdpSocket {
            inner,
            runtime,
            shared,
        }),
        stats,
    )
}

impl BoundedUdpSocket {
    fn blocked_deadline(&self, now: Instant) -> Option<Instant> {
        let mut shared = lock_unpoisoned(&self.shared);
        shared.refresh(now);
        let deadline = shared.batch.blocked_until;
        if deadline.is_some() {
            shared.stats.timer_blocked_polls = shared.stats.timer_blocked_polls.saturating_add(1);
        }
        deadline
    }
}

impl AsyncUdpSocket for BoundedUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        let now = self.runtime.now();
        Box::pin(BoundedUdpPoller {
            inner: self.inner.clone().create_io_poller(),
            timer: Mutex::new(PollTimer {
                timer: self.runtime.new_timer(now),
                deadline: None,
            }),
            socket: self,
        })
    }

    fn try_send(&self, transmit: &quinn::udp::Transmit<'_>) -> io::Result<()> {
        let datagrams = match wire_datagrams(transmit) {
            Ok(datagrams) => datagrams,
            Err(error) => {
                let mut shared = lock_unpoisoned(&self.shared);
                shared.stats.invalid_transmits = shared.stats.invalid_transmits.saturating_add(1);
                return Err(error);
            }
        };
        let now = self.runtime.now();
        let mut shared = lock_unpoisoned(&self.shared);
        shared.refresh(now);
        let remaining_before = shared.batch.remaining;
        match shared.batch.try_reserve_ready(now, datagrams) {
            BatchDecision::Granted => match self.inner.try_send(transmit) {
                Ok(()) => {
                    if shared.batch.blocked_until.is_some() {
                        shared.stats.batch_closes = shared.stats.batch_closes.saturating_add(1);
                    }
                    shared.record_accepted(now, transmit.contents.len(), datagrams);
                    Ok(())
                }
                Err(error) => {
                    shared.batch.refund(datagrams);
                    if error.kind() == io::ErrorKind::WouldBlock {
                        shared.stats.inner_would_block =
                            shared.stats.inner_would_block.saturating_add(1);
                    } else {
                        shared.stats.inner_errors = shared.stats.inner_errors.saturating_add(1);
                    }
                    Err(error)
                }
            },
            BatchDecision::WaitUntil(_) => {
                shared.stats.gate_would_block = shared.stats.gate_would_block.saturating_add(1);
                if remaining_before > 0 && datagrams > remaining_before {
                    shared.stats.batch_closes = shared.stats.batch_closes.saturating_add(1);
                    shared.stats.wasted_datagrams = shared
                        .stats
                        .wasted_datagrams
                        .saturating_add(remaining_before as u64);
                }
                Err(io::Error::from(io::ErrorKind::WouldBlock))
            }
            BatchDecision::TooLarge => {
                shared.stats.invalid_transmits = shared.stats.invalid_transmits.saturating_add(1);
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "QUIC UDP transmit exceeds bounded send batch",
                ))
            }
        }
    }

    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_recv(cx, bufs, meta)
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.inner.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

#[derive(Debug)]
struct PollTimer {
    timer: Pin<Box<dyn AsyncTimer>>,
    deadline: Option<Instant>,
}

#[derive(Debug)]
struct BoundedUdpPoller {
    inner: Pin<Box<dyn UdpPoller>>,
    timer: Mutex<PollTimer>,
    socket: Arc<BoundedUdpSocket>,
}

impl UdpPoller for BoundedUdpPoller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            let Some(deadline) = this.socket.blocked_deadline(this.socket.runtime.now()) else {
                return this.inner.as_mut().poll_writable(cx);
            };
            let mut timer = lock_unpoisoned(&this.timer);
            if timer.deadline != Some(deadline) {
                timer.timer.as_mut().reset(deadline);
                timer.deadline = Some(deadline);
            }
            match timer.timer.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {
                    timer.deadline = None;
                    drop(timer);
                }
            }
        }
    }
}

fn wire_datagrams(transmit: &quinn::udp::Transmit<'_>) -> io::Result<usize> {
    match transmit.segment_size {
        None => Ok(1),
        Some(0) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "QUIC UDP transmit has zero segment size",
        )),
        Some(segment_size) => Ok(transmit.contents.len().max(1).div_ceil(segment_size)),
    }
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

fn per_second(total: u64, elapsed_ns: u64) -> u64 {
    if elapsed_ns == 0 {
        return 0;
    }
    ((total as u128).saturating_mul(1_000_000_000) / elapsed_ns as u128).min(u64::MAX as u128)
        as u64
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use quinn::{AsyncUdpSocket, UdpPoller};
    use std::io;
    use std::net::SocketAddr;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Wake, Waker};

    #[derive(Debug, Default)]
    struct ReadyMockSocket {
        accepted: AtomicUsize,
        writable_polls: Arc<AtomicUsize>,
        would_block_remaining: AtomicUsize,
        records: Mutex<Vec<RecordedTransmit>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedTransmit {
        destination: SocketAddr,
        ecn: Option<quinn::udp::EcnCodepoint>,
        contents: Vec<u8>,
        segment_size: Option<usize>,
        src_ip: Option<std::net::IpAddr>,
    }

    impl AsyncUdpSocket for ReadyMockSocket {
        fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
            Box::pin(ReadyMockPoller {
                polls: self.writable_polls.clone(),
            })
        }

        fn try_send(&self, transmit: &quinn::udp::Transmit<'_>) -> io::Result<()> {
            if self
                .would_block_remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            self.accepted.fetch_add(1, Ordering::Relaxed);
            self.records
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(RecordedTransmit {
                    destination: transmit.destination,
                    ecn: transmit.ecn,
                    contents: transmit.contents.to_vec(),
                    segment_size: transmit.segment_size,
                    src_ip: transmit.src_ip,
                });
            Ok(())
        }

        fn poll_recv(
            &self,
            _cx: &mut Context<'_>,
            _bufs: &mut [io::IoSliceMut<'_>],
            _meta: &mut [quinn::udp::RecvMeta],
        ) -> Poll<io::Result<usize>> {
            Poll::Pending
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok("127.0.0.1:0".parse().expect("valid mock address"))
        }

        fn max_transmit_segments(&self) -> usize {
            10
        }
    }

    #[derive(Debug)]
    struct ReadyMockPoller {
        polls: Arc<AtomicUsize>,
    }

    impl UdpPoller for ReadyMockPoller {
        fn poll_writable(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Debug, Default)]
    struct CountWake(AtomicUsize);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn transmit<'a>(contents: &'a [u8], segment_size: Option<usize>) -> quinn::udp::Transmit<'a> {
        quinn::udp::Transmit {
            destination: "127.0.0.1:443".parse().expect("valid destination"),
            ecn: None,
            contents,
            segment_size,
            src_ip: None,
        }
    }
    #[test]
    fn bounded_send_batch_closes_after_48_datagram_equivalents() {
        let started = Instant::now();
        let mut batch = SendBatch::new(48, Duration::from_millis(2));

        for _ in 0..48 {
            assert_eq!(batch.try_reserve(started, 1), BatchDecision::Granted);
        }
        assert_eq!(
            batch.try_reserve(started, 1),
            BatchDecision::WaitUntil(started + Duration::from_millis(2))
        );
        assert_eq!(
            batch.try_reserve(started + Duration::from_millis(1), 1),
            BatchDecision::WaitUntil(started + Duration::from_millis(2))
        );
        assert_eq!(
            batch.try_reserve(started + Duration::from_millis(2), 1),
            BatchDecision::Granted
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_udp_send_service_waits_after_48_datagram_equivalents() {
        let inner = Arc::new(ReadyMockSocket::default());
        let inner_dyn: Arc<dyn AsyncUdpSocket> = inner.clone();
        let runtime: Arc<dyn quinn::Runtime> = Arc::new(quinn::TokioRuntime);
        let (socket, stats) = bounded_udp_socket(inner_dyn, runtime);
        let payload = [0x5a; 1200];
        let packet = transmit(&payload, None);

        for _ in 0..48 {
            socket.try_send(&packet).expect("within shared batch");
        }
        let error = socket.try_send(&packet).expect_err("batch must close");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);

        let mut poller = socket.clone().create_io_poller();
        let wake = Arc::new(CountWake::default());
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(poller.as_mut().poll_writable(&mut cx).is_pending());
        assert_eq!(inner.writable_polls.load(Ordering::Relaxed), 0);
        assert_eq!(wake.0.load(Ordering::Relaxed), 0);

        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(wake.0.load(Ordering::Relaxed), 0);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(wake.0.load(Ordering::Relaxed), 1);
        assert!(poller.as_mut().poll_writable(&mut cx).is_ready());
        socket.try_send(&packet).expect("new batch after cooldown");

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.accepted_datagrams, 49);
        assert_eq!(snapshot.accepted_bytes, 49 * payload.len() as u64);
        assert_eq!(snapshot.gate_would_block, 1);
        assert_eq!(snapshot.cooldown_rearms, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_udp_send_service_reports_actual_rearm_lateness_and_service_rate() {
        let inner = Arc::new(ReadyMockSocket::default());
        let runtime: Arc<dyn quinn::Runtime> = Arc::new(quinn::TokioRuntime);
        let (socket, stats) = bounded_udp_socket(inner, runtime);
        let payload = [0x5a; 1200];
        let packet = transmit(&payload, None);

        for _ in 0..48 {
            socket.try_send(&packet).expect("first shared batch");
        }
        assert_eq!(
            socket.try_send(&packet).expect_err("closed batch").kind(),
            io::ErrorKind::WouldBlock
        );

        let mut poller = socket.clone().create_io_poller();
        let wake = Arc::new(CountWake::default());
        let waker = Waker::from(wake);
        let mut cx = Context::from_waker(&waker);
        assert!(poller.as_mut().poll_writable(&mut cx).is_pending());

        tokio::time::advance(Duration::from_millis(3)).await;
        assert!(poller.as_mut().poll_writable(&mut cx).is_ready());
        socket
            .try_send(&packet)
            .expect("first send after late rearm");

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.batch_closes, 1);
        assert_eq!(snapshot.timer_blocked_polls, 1);
        assert_eq!(snapshot.cooldown_rearms, 1);
        assert_eq!(snapshot.total_rearm_lateness_ns, 1_000_000);
        assert_eq!(snapshot.max_rearm_lateness_ns, 1_000_000);
        assert_eq!(snapshot.service_elapsed_ns, 3_000_000);
        assert_eq!(snapshot.accepted_datagrams_per_sec, 16_333);
        assert_eq!(snapshot.accepted_bytes_per_sec, 19_600_000);
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_udp_send_service_shares_budget_across_two_pollers() {
        let inner = Arc::new(ReadyMockSocket::default());
        let runtime: Arc<dyn quinn::Runtime> = Arc::new(quinn::TokioRuntime);
        let (socket, stats) = bounded_udp_socket(inner.clone(), runtime);
        let payload = [0x5a; 1200];
        let packet = transmit(&payload, None);
        let mut first = socket.clone().create_io_poller();
        let mut second = socket.clone().create_io_poller();

        for _ in 0..24 {
            socket.try_send(&packet).expect("first connection share");
        }
        for _ in 0..24 {
            socket.try_send(&packet).expect("second connection share");
        }
        assert_eq!(
            socket.try_send(&packet).expect_err("aggregate gate").kind(),
            io::ErrorKind::WouldBlock
        );

        let first_wake = Arc::new(CountWake::default());
        let second_wake = Arc::new(CountWake::default());
        let first_waker = Waker::from(first_wake.clone());
        let second_waker = Waker::from(second_wake.clone());
        let mut first_cx = Context::from_waker(&first_waker);
        let mut second_cx = Context::from_waker(&second_waker);
        assert!(first.as_mut().poll_writable(&mut first_cx).is_pending());
        assert!(second.as_mut().poll_writable(&mut second_cx).is_pending());
        assert_eq!(inner.writable_polls.load(Ordering::Relaxed), 0);

        tokio::time::advance(Duration::from_millis(2)).await;
        assert_eq!(first_wake.0.load(Ordering::Relaxed), 1);
        assert_eq!(second_wake.0.load(Ordering::Relaxed), 1);
        assert!(first.as_mut().poll_writable(&mut first_cx).is_ready());
        assert!(second.as_mut().poll_writable(&mut second_cx).is_ready());
        assert_eq!(stats.snapshot().accepted_datagrams, 48);
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_udp_send_service_counts_gso_segments_without_busy_wake() {
        let inner = Arc::new(ReadyMockSocket::default());
        let runtime: Arc<dyn quinn::Runtime> = Arc::new(quinn::TokioRuntime);
        let (socket, stats) = bounded_udp_socket(inner.clone(), runtime);
        let payload = [0x33; 12_000];
        let packet = transmit(&payload, Some(1200));

        for _ in 0..4 {
            socket.try_send(&packet).expect("40 datagram equivalents");
        }
        assert_eq!(
            socket
                .try_send(&packet)
                .expect_err("8 cannot fit 10")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let mut poller = socket.clone().create_io_poller();
        let wake = Arc::new(CountWake::default());
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(poller.as_mut().poll_writable(&mut cx).is_pending());
        assert_eq!(wake.0.load(Ordering::Relaxed), 0);
        assert_eq!(inner.writable_polls.load(Ordering::Relaxed), 0);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.accepted_datagrams, 40);
        assert_eq!(snapshot.wasted_datagrams, 8);
        assert_eq!(snapshot.gate_would_block, 1);
        tokio::time::advance(Duration::from_millis(2)).await;
        assert_eq!(wake.0.load(Ordering::Relaxed), 1);
        assert!(poller.as_mut().poll_writable(&mut cx).is_ready());
        socket
            .try_send(&packet)
            .expect("whole GSO transmit after rearm");
        assert_eq!(stats.snapshot().accepted_datagrams, 50);
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_udp_send_service_preserves_inner_would_block_without_debit() {
        let inner = Arc::new(ReadyMockSocket::default());
        inner.would_block_remaining.store(1, Ordering::Relaxed);
        let runtime: Arc<dyn quinn::Runtime> = Arc::new(quinn::TokioRuntime);
        let (socket, stats) = bounded_udp_socket(inner.clone(), runtime);
        let payload = [0x44; 1200];
        let packet = transmit(&payload, None);

        assert_eq!(
            socket
                .try_send(&packet)
                .expect_err("mock inner backpressure")
                .kind(),
            io::ErrorKind::WouldBlock
        );
        let mut poller = socket.clone().create_io_poller();
        let wake = Arc::new(CountWake::default());
        let waker = Waker::from(wake);
        let mut cx = Context::from_waker(&waker);
        assert!(poller.as_mut().poll_writable(&mut cx).is_ready());
        for _ in 0..48 {
            socket
                .try_send(&packet)
                .expect("refunded budget remains available");
        }
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.inner_would_block, 1);
        assert_eq!(snapshot.accepted_datagrams, 48);
        assert_eq!(snapshot.gate_would_block, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn bounded_udp_send_service_conserves_accepted_bytes_and_metadata() {
        let inner = Arc::new(ReadyMockSocket::default());
        let runtime: Arc<dyn quinn::Runtime> = Arc::new(quinn::TokioRuntime);
        let (socket, stats) = bounded_udp_socket(inner.clone(), runtime);
        let payload = vec![0xa5; 3000];
        let expected = quinn::udp::Transmit {
            destination: "192.0.2.8:8443".parse().unwrap(),
            ecn: Some(quinn::udp::EcnCodepoint::Ect0),
            contents: &payload,
            segment_size: Some(1200),
            src_ip: Some("192.0.2.9".parse().unwrap()),
        };

        socket.try_send(&expected).expect("mock accepts transmit");
        let records = inner
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].destination, expected.destination);
        assert_eq!(records[0].ecn, expected.ecn);
        assert_eq!(records[0].contents, expected.contents);
        assert_eq!(records[0].segment_size, expected.segment_size);
        assert_eq!(records[0].src_ip, expected.src_ip);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.accepted_bytes, payload.len() as u64);
        assert_eq!(snapshot.accepted_datagrams, 3);
    }
}

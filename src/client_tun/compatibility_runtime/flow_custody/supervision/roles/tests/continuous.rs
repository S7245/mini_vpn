//! Actual four-role Continuous lifecycle and original-channel wake faults.
//! Private duplex tests do not claim kernel/TUN ACKs or throughput.
use super::super::super::receive_window::{ReceiveWindow, WINDOW_BYTES};
use super::super::continuous::{self, DispatchOutcome};
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Wake, Waker};
use tokio::io::AsyncWriteExt;

fn handle() -> SocketHandle {
    let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
    sockets.add(smoltcp::socket::tcp::Socket::new(
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
    ))
}
struct Fixture {
    roles: FlowRoles,
    remote: tokio::io::DuplexStream,
    commands: crate::client_tun::uplink::Sender,
    back: crate::client_tun::relay_channel::Receiver<(SocketHandle, RelayEvent)>,
    window: ReceiveWindow,
    _credit: watch::Sender<RelayReadCredit>,
}
fn fixture(back_capacity: usize, prefill: bool, paused: bool) -> Fixture {
    fixture_engine(
        back_capacity,
        prefill,
        paused,
        TcpRelayEngine::ContinuousPump,
    )
}
fn fixture_engine(
    back_capacity: usize,
    prefill: bool,
    paused: bool,
    engine: TcpRelayEngine,
) -> Fixture {
    let (local, remote) = tokio::io::duplex(6 * 1024 * 1024);
    let (commands, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, back) = crate::client_tun::relay_channel::channel(back_capacity);
    let handle = handle();
    if prefill {
        back_tx
            .try_send((handle, RelayEvent::DataReady { epoch: 999 }))
            .unwrap();
    }
    let (credit, read_credit_rx) = watch::channel(RelayReadCredit {
        paused,
        max_batch_bytes: if paused { 0 } else { WINDOW_BYTES },
    });
    let mut window = ReceiveWindow::new();
    let mut roles = FlowRoles::new(GenericLaunchRequest {
        engine,
        handle,
        epoch: 7,
        stream: Box::new(local),
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure: DownlinkBackpressureConfig::default(),
    });
    roles.prepare(window.take_reader().unwrap());
    Fixture {
        roles,
        remote,
        commands,
        back,
        window,
        _credit: credit,
    }
}
async fn until(roles: &mut FlowRoles, producer_stopped: bool, done: impl Fn(&FlowRoles) -> bool) {
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        std::future::poll_fn(|cx| {
            roles.poll(cx, producer_stopped);
            if done(roles) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    )
    .await
    .unwrap();
}
async fn next_data(back: &mut crate::client_tun::relay_channel::Receiver<(SocketHandle, RelayEvent)>) -> Vec<u8> {
    loop {
        let item = tokio::time::timeout(std::time::Duration::from_secs(2), back.recv())
            .await
            .unwrap()
            .unwrap();
        if let RelayEvent::Data {
            epoch: 7, bytes, ..
        } = item.1
        {
            return bytes.to_vec();
        }
    }
}
fn outcome(roles: &FlowRoles) -> &continuous::Outcome {
    let Role::Joined(DispatchOutcome::Continuous(outcome)) = &roles.dispatcher else {
        panic!("actual original dispatcher join");
    };
    outcome
}
#[tokio::test]
async fn fifo_eof_follows_all_data_before_four_actual_joins() {
    let mut f = fixture(4, false, false);
    let expected: Vec<u8> = (0..128 * 1024).map(|n| (n % 251) as u8).collect();
    f.remote.write_all(&expected).await.unwrap();
    f.remote.shutdown().await.unwrap();
    f.roles.start();
    let first = next_data(&mut f.back).await;
    assert_eq!(first, expected[..WINDOW_BYTES]);
    until(&mut f.roles, false, |r| matches!(r.reader, Role::Joined(_))).await;
    for _ in 0..4 {
        let pending = tokio::time::timeout(
            std::time::Duration::from_millis(2),
            std::future::poll_fn(|cx| {
                f.roles.poll(cx, false);
                assert!(f.roles.closed().is_none() && !f.roles.quarantined());
                assert!(f.back.try_recv().is_err());
                Poll::<()>::Pending
            }),
        )
        .await;
        assert!(pending.is_err());
    }
    f.window.admit(WINDOW_BYTES, 0, first.len());
    f.window.publish();
    assert_eq!(next_data(&mut f.back).await, expected[WINDOW_BYTES..]);
    until(&mut f.roles, false, |r| {
        r.closed() == Some(("remote_to_local", "remote_eof"))
    })
    .await;
    f.roles.stop();
    drop(f.commands);
    until(&mut f.roles, true, FlowRoles::settled).await;
    assert_eq!(f.roles.join_count(), 4);
    assert_eq!(f.roles.published().unwrap().bytes, expected.len());
    assert_eq!(f.roles.reader_receipt.unwrap().unpublished_bytes, 0);
    let receipt = outcome(&f.roles).receipt.unwrap();
    assert_eq!(receipt.queue.emitted, expected.len());
    assert_eq!(receipt.queue.consumed, expected.len());
    assert_eq!(receipt.queue.discarded_bytes, 0);
    assert_eq!(receipt.unpublished, 0);
    assert!(f.roles.take_error().is_none());
}
#[tokio::test]
async fn actual_full_four_mib_and_main_reserve_stop_join_before_guarded_disposal() {
    let mut f = fixture(1, true, true);
    f.remote.write_all(&vec![9; 5 * 1024 * 1024]).await.unwrap();
    f.roles.start();
    let queue = f.roles.continuous_queue.as_ref().unwrap().clone();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let (physical, custody) = queue.custody_snapshot().await;
            if physical.queued_bytes == CONTINUOUS_TCP_RELAY_QUEUE_BYTES && custody.batches > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Stop the original reader/full-queue wait and original Main reserve. The
    // read-only queue clone observes the real FIFO; no synthetic queue replaces it.
    f.roles.stop();
    drop(f.commands);
    until(&mut f.roles, false, |r| r.join_count() == 4).await;
    assert!(!f.roles.settled() && outcome(&f.roles).receipt.is_none());
    assert_eq!(
        queue.snapshot().await.queued_bytes,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES
    );
    until(&mut f.roles, true, FlowRoles::settled).await;
    let out = outcome(&f.roles);
    let receipt = out.receipt.unwrap();
    assert_eq!(receipt.queue.frozen_bytes, CONTINUOUS_TCP_RELAY_QUEUE_BYTES);
    assert_eq!(
        receipt.queue.discarded_bytes,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES
    );
    assert_eq!(receipt.queue.discarded_chunks, receipt.queue.frozen_chunks);
    assert!(receipt.queue.discarded_allocation >= receipt.queue.discarded_bytes);
    assert_eq!(out.published.bytes, 0);
    assert_eq!(receipt.unpublished, out.consumed.bytes);
    assert!(receipt.held_allocation >= receipt.held_bytes && receipt.held_bytes > 0);
    assert!(receipt.segment_capacity >= receipt.segment_bytes && receipt.segment_bytes > 0);
    assert_eq!(queue.snapshot().await.queued_bytes, 0);
    assert_eq!(f.roles.join_count(), 4);
    assert_eq!(f.roles.reader_receipt.unwrap().unpublished_bytes, 0);
    assert!(f.roles.take_error().is_none());
}
#[tokio::test]
async fn stopped_before_start_consumes_all_four_handles_without_read_or_publish() {
    let mut f = fixture(1, false, false);
    f.roles.stop();
    drop(f.commands);
    until(&mut f.roles, true, FlowRoles::settled).await;
    assert_eq!(f.roles.join_count(), 4);
    assert_eq!(f.roles.published().unwrap().bytes, 0);
    assert_eq!(outcome(&f.roles).receipt.unwrap().queue.emitted, 0);
    assert!(f.back.try_recv().is_err());
}
struct PanicWake(AtomicBool);
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self.0.swap(false, Ordering::SeqCst) {
            panic!("actual Continuous Main send wake");
        }
    }
}
#[tokio::test]
async fn committed_main_wake_panic_keeps_real_data_and_unknown_q_without_republish() {
    let shared = continuous::Shared::new(TcpRelayEngine::ContinuousPump).unwrap();
    shared
        .queue
        .push_custodied(&mut Some(vec![5; WINDOW_BYTES]))
        .await
        .unwrap();
    let (back_tx, mut back) = crate::client_tun::relay_channel::channel(1);
    let (signal_tx, _signals) = mpsc::channel(2);
    let (_stop, stop_rx) = oneshot::channel();
    let (start, gate) = oneshot::channel();
    let mut window = ReceiveWindow::new();
    let request = continuous::Request {
        handle: handle(),
        epoch: 7,
        shared,
        back: Some(back_tx),
        signals: Some(signal_tx),
        window: window.take_reader().unwrap(),
        stop: Some(stop_rx),
        gate,
    };
    let waker = Waker::from(Arc::new(PanicWake(AtomicBool::new(true))));
    let mut cx = Context::from_waker(&waker);
    assert!(back.poll_recv(&mut cx).is_pending());
    start.send(()).unwrap();
    let mut out = continuous::run(request).await;
    assert!(out.quarantined() && out.panics[0].is_some());
    assert_eq!(out.window_read(), WINDOW_BYTES);
    assert_eq!(out.published.bytes, 0);
    assert_eq!(next_data(&mut back).await, vec![5; WINDOW_BYTES]);
    out.dispose(&mut cx);
    assert!(out.receipt.is_none() && out.quarantined());
    assert!(back.try_recv().is_err());
}

#[derive(Debug)]
struct OriginalError(u64);
impl std::fmt::Display for OriginalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "original Continuous read failure {}", self.0)
    }
}
impl std::error::Error for OriginalError {}
struct FailedRead(Option<std::io::Error>);
impl tokio::io::AsyncRead for FailedRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.0.take() {
            Some(error) => Poll::Ready(Err(error)),
            None => Poll::Pending,
        }
    }
}
impl tokio::io::AsyncWrite for FailedRead {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn original_read_error_and_ordinal_survive_actual_queue_close_wake_panic() {
    let shared = continuous::Shared::new(TcpRelayEngine::ContinuousPump).unwrap();
    let error = std::io::Error::other(OriginalError(73));
    let identity = error.get_ref().unwrap() as *const _ as *const () as usize;
    let stream: RelayStream = Box::new(FailedRead(Some(error)));
    let (reader, _writer) = tokio::io::split(stream);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let (signals, _signal_rx) = mpsc::channel(2);
    let (_stop, stop_rx) = oneshot::channel();
    let mut held = continuous::Held::new(&shared.queue);
    let mut waiting = Box::pin(shared.queue.recv_custodied(WINDOW_BYTES, &mut held));
    let waker = Waker::from(Arc::new(PanicWake(AtomicBool::new(true))));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let failures = Arc::new(crate::shared::errors::FailureOrder::default());
    let request = ReaderRequest {
        handle: handle(),
        epoch: 7,
        remote_reader: reader,
        back_tx: ReaderOutput::Continuous(shared.clone()),
        read_credit_rx,
        signal_tx: signals,
        local_finish_seen: Arc::new(AtomicBool::new(false)),
        stop_rx,
    };
    let out = run_custodied_relay_reader_gated(request, None, Some(failures.clone()), None).await;
    assert_eq!(out.error_order, Some(0));
    assert_eq!(failures.load(Ordering::SeqCst), 1);
    assert_eq!(
        out.failure.as_ref().unwrap().get_ref().unwrap() as *const _ as *const () as usize,
        identity
    );
    assert!(out.notification.closed_sent && out.panics[0].is_some());
    assert!(out.remainder.quarantined());
    assert_eq!(out.remainder.remote_total(), 0);
    drop(waiting);
}

#[tokio::test]
async fn original_main_closed_rejection_keeps_unsent_batch_until_four_join_disposal() {
    let mut f = fixture(1, false, false);
    f.remote.write_all(&vec![3; WINDOW_BYTES]).await.unwrap();
    drop(f.back);
    f.roles.start();
    until(&mut f.roles, false, |r| {
        r.closed() == Some(("remote_to_local", "global_rx_closed"))
    })
    .await;
    f.roles.stop();
    drop(f.commands);
    until(&mut f.roles, true, FlowRoles::settled).await;
    assert_eq!(f.roles.join_count(), 4);
    let out = outcome(&f.roles);
    assert_eq!(out.published.bytes, 0);
    let receipt = out.receipt.unwrap();
    assert_eq!(receipt.held_bytes, WINDOW_BYTES);
    assert_eq!(receipt.unpublished, WINDOW_BYTES);
    assert_eq!(receipt.segment_bytes, WINDOW_BYTES);
    assert_eq!(receipt.queue.emitted, WINDOW_BYTES);
    assert!(f.roles.take_error().is_none());
}

#[tokio::test]
async fn permit_four_actual_roles_keep_fifo_eof_and_real_admission_permits() {
    let mut f = fixture_engine(4, false, true, TcpRelayEngine::PermitPump);
    assert!(
        !f.roles.quarantined() && f.roles.running_count() == 4,
        "Permit must retain all four actual original roles before publication"
    );
    let expected: Vec<u8> = (0..2 * WINDOW_BYTES).map(|n| (n % 251) as u8).collect();
    f.remote.write_all(&expected).await.unwrap();
    f.remote.shutdown().await.unwrap();
    f.roles.start();
    let mut received = 0;
    while received < expected.len() {
        let item = tokio::time::timeout(std::time::Duration::from_secs(2), f.back.recv())
            .await
            .unwrap()
            .unwrap();
        let RelayEvent::Data {
            epoch,
            bytes,
            downstream_permit,
            ..
        } = item.1
        else {
            continue;
        };
        let mut permit = downstream_permit.expect("actual original leased DATA permit");
        assert_eq!(epoch, 7);
        assert!(!bytes.is_empty() && bytes.len() <= WINDOW_BYTES);
        assert_eq!(bytes, expected[received..received + bytes.len()]);
        assert_eq!(
            permit.release_mode(),
            crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnAdmission
        );
        assert_eq!(permit.remaining_bytes(), bytes.len());
        received += bytes.len();
        f.window.admit(bytes.len(), 0, bytes.len());
        assert_eq!(permit.release(bytes.len()), bytes.len());
        assert_eq!(permit.remaining_bytes(), 0);
        f.window.publish();
    }
    until(&mut f.roles, false, |r| {
        r.closed() == Some(("remote_to_local", "remote_eof"))
    })
    .await;
    f.roles.stop();
    drop(f.commands);
    until(&mut f.roles, true, FlowRoles::settled).await;
    assert_eq!(f.roles.join_count(), 4);
    assert_eq!(f.roles.published().unwrap().bytes, expected.len());
    assert!(f.roles.take_error().is_none());
}

#[tokio::test]
async fn permit_lifecycle_full_leased_window_cancelled_observer_then_actual_four_join_disposal() {
    let mut f = fixture_engine(1, true, true, TcpRelayEngine::PermitPump);
    let queue = f.roles.permit_queue().unwrap().clone();
    f.remote.write_all(&vec![9; 5 * 1024 * 1024]).await.unwrap();
    f.roles.start();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let snapshot = queue.snapshot_now();
            if snapshot.owned_bytes() == CONTINUOUS_TCP_RELAY_QUEUE_BYTES
                && snapshot.leased_bytes > 0
            {
                assert!(snapshot.leased_bytes <= 128 * 1024);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.roles.stop();
    drop(f.commands);
    until(&mut f.roles, false, |r| r.join_count() == 4).await;
    assert!(!f.roles.settled() && outcome(&f.roles).receipt.is_none());
    let producer_stopped = Arc::new(AtomicBool::new(false));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut observer = Box::pin(tokio::time::timeout_at(
        deadline,
        std::future::poll_fn(|cx| {
            f.roles.poll(cx, producer_stopped.load(Ordering::Acquire));
            if f.roles.settled() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    ));
    for _ in 0..4 {
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(2), observer.as_mut())
                .await
                .is_err()
        );
        assert_eq!(
            queue.snapshot_now().owned_bytes(),
            CONTINUOUS_TCP_RELAY_QUEUE_BYTES
        );
        assert!(!queue.physical_zero());
    }
    producer_stopped.store(true, Ordering::Release);
    observer.await.unwrap();
    assert_eq!(f.roles.join_count(), 4);
    assert_eq!(f.roles.published().unwrap().bytes, 0);
    let receipt = outcome(&f.roles).receipt.unwrap();
    assert_eq!(receipt.queue.emitted, CONTINUOUS_TCP_RELAY_QUEUE_BYTES);
    assert_eq!(
        receipt.queue.discarded_bytes + receipt.unpublished,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES
    );
    assert!(queue.physical_zero());
    assert!(f.roles.take_error().is_none());
}

#[tokio::test]
async fn permit_lifecycle_cancelled_start_gates_consume_four_original_handles() {
    let mut f = fixture_engine(1, false, false, TcpRelayEngine::PermitPump);
    let queue = f.roles.permit_queue().unwrap().clone();
    f.roles.stop();
    drop(f.commands);
    until(&mut f.roles, true, FlowRoles::settled).await;
    assert_eq!(f.roles.join_count(), 4);
    assert_eq!(f.roles.published().unwrap().bytes, 0);
    assert_eq!(outcome(&f.roles).receipt.unwrap().queue.emitted, 0);
    assert!(queue.physical_zero());
    assert!(f.back.try_recv().is_err());
}

#[tokio::test]
async fn permit_lifecycle_main_closed_retains_original_lease_until_joined_disposal() {
    let mut f = fixture_engine(1, false, false, TcpRelayEngine::PermitPump);
    let queue = f.roles.permit_queue().unwrap().clone();
    f.remote.write_all(&vec![3; WINDOW_BYTES]).await.unwrap();
    drop(f.back);
    f.roles.start();
    until(&mut f.roles, false, |r| {
        r.closed() == Some(("remote_to_local", "global_rx_closed"))
    })
    .await;
    assert_eq!(queue.snapshot_now().leased_bytes, WINDOW_BYTES);
    assert!(!queue.physical_zero());
    f.roles.stop();
    drop(f.commands);
    until(&mut f.roles, false, |r| r.join_count() == 4).await;
    assert!(!f.roles.settled() && outcome(&f.roles).receipt.is_none());
    assert_eq!(queue.snapshot_now().leased_bytes, WINDOW_BYTES);
    until(&mut f.roles, true, FlowRoles::settled).await;
    assert_eq!(f.roles.join_count(), 4);
    let out = outcome(&f.roles);
    assert_eq!(out.published.bytes, 0);
    assert_eq!(out.receipt.unwrap().unpublished, WINDOW_BYTES);
    assert_eq!(queue.snapshot_now().leased_bytes, 0);
    assert!(queue.physical_zero());
}

#[tokio::test]
async fn permit_lifecycle_main_send_wake_panic_keeps_actual_original_lease_charged() {
    let shared = continuous::Shared::new(TcpRelayEngine::PermitPump).unwrap();
    let queue = shared.queue.leased_queue().unwrap().clone();
    shared
        .queue
        .push_custodied(&mut Some(vec![5; WINDOW_BYTES]))
        .await
        .unwrap();
    let (back_tx, mut back) = crate::client_tun::relay_channel::channel(1);
    let (signal_tx, _signals) = mpsc::channel(2);
    let (_stop, stop_rx) = oneshot::channel();
    let (start, gate) = oneshot::channel();
    let mut window = ReceiveWindow::new();
    let request = continuous::Request {
        handle: handle(),
        epoch: 7,
        shared,
        back: Some(back_tx),
        signals: Some(signal_tx),
        window: window.take_reader().unwrap(),
        stop: Some(stop_rx),
        gate,
    };
    let waker = Waker::from(Arc::new(PanicWake(AtomicBool::new(true))));
    let mut cx = Context::from_waker(&waker);
    assert!(back.poll_recv(&mut cx).is_pending());
    start.send(()).unwrap();
    let mut out = continuous::run(request).await;
    assert!(out.quarantined() && out.panics[0].is_some());
    assert_eq!(out.window_read(), WINDOW_BYTES);
    assert_eq!(out.published.bytes, 0); // Send disposition remains unknown after actual wake panic.
    let Some((
        _,
        RelayEvent::Data {
            epoch: 7,
            bytes,
            downstream_permit: Some(permit),
            ..
        },
    )) = back.try_recv().ok()
    else {
        panic!("original committed Main DATA and permit must remain physical");
    };
    assert_eq!(bytes, vec![5; WINDOW_BYTES]);
    assert!(queue.owns_permit(&permit, WINDOW_BYTES));
    assert_eq!(queue.snapshot_now().leased_bytes, WINDOW_BYTES);
    out.dispose(&mut cx);
    assert!(out.receipt.is_none() && out.quarantined() && !queue.physical_zero());
    assert!(back.try_recv().is_err());
    // Test forced destruction below is not production settlement or a join.
    drop(permit);
}

#[tokio::test]
async fn permit_lifecycle_original_io_heap_and_ordinal_survive_actual_close_wake_panic() {
    let shared = continuous::Shared::new(TcpRelayEngine::PermitPump).unwrap();
    let queue = shared.queue.leased_queue().unwrap().clone();
    let error = std::io::Error::other(OriginalError(97));
    let identity = error.get_ref().unwrap() as *const _ as *const () as usize;
    let stream: RelayStream = Box::new(FailedRead(Some(error)));
    let (reader, _writer) = tokio::io::split(stream);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let (signals, _signal_rx) = mpsc::channel(2);
    let (_stop, stop_rx) = oneshot::channel();
    let mut held = continuous::Held::new(&shared.queue);
    let mut waiting = Box::pin(shared.queue.recv_custodied(WINDOW_BYTES, &mut held));
    let waker = Waker::from(Arc::new(PanicWake(AtomicBool::new(true))));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let failures = Arc::new(crate::shared::errors::FailureOrder::default());
    let request = ReaderRequest {
        handle: handle(),
        epoch: 7,
        remote_reader: reader,
        back_tx: ReaderOutput::Continuous(shared.clone()),
        read_credit_rx,
        signal_tx: signals,
        local_finish_seen: Arc::new(AtomicBool::new(false)),
        stop_rx,
    };
    let out = run_custodied_relay_reader_gated(request, None, Some(failures.clone()), None).await;
    assert_eq!(out.error_order, Some(0));
    assert_eq!(failures.load(Ordering::SeqCst), 1);
    assert_eq!(
        out.failure.as_ref().unwrap().get_ref().unwrap() as *const _ as *const () as usize,
        identity
    );
    assert!(out.notification.closed_sent && out.panics[0].is_some());
    assert!(out.remainder.quarantined() && !queue.physical_zero());
    drop(waiting);
}

struct NativePanicWake(Arc<usize>);
impl Wake for NativePanicWake {
    fn wake(self: Arc<Self>) { std::panic::panic_any(self.0.clone()); }
    fn wake_by_ref(self: &Arc<Self>) { std::panic::panic_any(self.0.clone()); }
}

#[tokio::test]
async fn native_typed_main_send_keeps_original_backing_first_panic_and_immediate_q() {
    use crate::tcp_downlink_pump::OwnedPayload;
    for engine in [TcpRelayEngine::ContinuousPump, TcpRelayEngine::OrderedEgressPermitPump] {
        let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let shared = continuous::Shared::configured_metered(engine, DownlinkBackpressureConfig::default(), Some(meter.clone())).unwrap();
        let (clock, _facts) = native::stock_tests::bound_clock(&meter);
        assert!(shared.bind_failures(clock));
        let cold = budget.used();
        let bytes = bytes::Bytes::copy_from_slice_metered(b"original backing", &meter).unwrap();
        let identity = bytes.allocation_identity();
        shared.queue.push_custodied(&mut Some(OwnedPayload::Native((bytes).into()))).await.unwrap();
        let (back_tx, mut back) = crate::client_tun::relay_channel::channel(1);
        let (signals, _signals) = mpsc::channel(2);
        let (_stop, stop) = oneshot::channel();
        let (start, gate) = oneshot::channel();
        let mut window = ReceiveWindow::new();
        let first = Arc::new(91usize);
        let waker = Waker::from(Arc::new(NativePanicWake(first.clone())));
        let mut cx = Context::from_waker(&waker);
        assert!(back.poll_recv(&mut cx).is_pending());
        start.send(()).unwrap();
        let mut out = continuous::run(continuous::Request {
            handle: handle(), epoch: 7, shared, back: Some(back_tx), signals: Some(signals),
            window: window.take_reader().unwrap(), stop: Some(stop), gate,
        }).await;
        assert!(budget.quarantined(), "the original Root has not polled recovery");
        let original = out.panics[0].as_ref().unwrap().downcast_ref::<Arc<usize>>().unwrap();
        assert!(Arc::ptr_eq(original, &first));
        assert!(out.panics[1..].iter().all(Option::is_none));
        assert!(out.quarantined());
        assert_eq!(out.published.bytes, 0);
        let event = back.try_recv().unwrap();
        let RelayEvent::Data { bytes: OwnedPayload::Native(bytes), .. } = &event.1 else { panic!("actual typed Main prefix"); };
        assert_eq!(bytes.allocation_identity(), identity);
        assert_eq!(bytes.as_ref(), b"original backing");
        out.dispose(&mut cx);
        assert!(out.receipt.is_none());
        drop((out, back));
        assert!(budget.used() > 1, "actual selected payload still owns physical layout");
        drop(event);
        assert_eq!(budget.used(), cold, "Q retains the actual fixed callback inventory");
    }
}

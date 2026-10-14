//! Deterministic ingress-pump ownership and shutdown tracers.

use super::*;
use crate::shared::errors::FailureClass;

struct ObservedReader<R> {
    inner: R,
    pending: Arc<tokio::sync::Notify>,
}

impl<R: AsyncRead + Unpin> AsyncRead for ObservedReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        if result.is_pending() {
            this.pending.notify_one();
        }
        result
    }
}

#[tokio::test]
async fn ingress_consumer_close_wakes_blocked_read_without_packet() {
    let (writer, reader) = tokio::io::duplex(4096);
    let pending = Arc::new(tokio::sync::Notify::new());
    let reader = ObservedReader {
        inner: reader,
        pending: Arc::clone(&pending),
    };
    let mut pump = spawn_tun_ingress_pump(reader, 1200, 4);
    tokio::time::timeout(Duration::from_secs(1), pending.notified())
        .await
        .expect("the real reader must reach Pending before consumer closure");
    pump.receiver.close();
    let joined = tokio::time::timeout(Duration::from_millis(50), pump.observe_join()).await;
    let woke = joined.is_ok();
    if let Ok(exit) = joined {
        assert_eq!(exit, PumpExit::ConsumerClosed);
    } else {
        if let Some(task) = task_handle(&pump) {
            task.abort();
        }
        pump.observe_join().await;
    }
    drop(writer);
    assert!(
        woke,
        "closing the consumer must wake a blocked read without an extra packet"
    );
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bounded test condition should become observable");
}

fn task_handle(pump: &TunIngressPump) -> Option<&JoinHandle<PumpExit>> {
    match &pump.task {
        PumpTask::Running(task) => Some(task),
        PumpTask::Observed(_) => None,
    }
}

async fn join_pump(pump: &mut TunIngressPump) -> PumpExit {
    match tokio::time::timeout(Duration::from_secs(1), pump.observe_join()).await {
        Ok(exit) => exit,
        Err(_) => {
            if let Some(task) = task_handle(&pump) {
                task.abort();
            }
            pump.observe_join().await;
            panic!("pump join exceeded the deterministic test deadline");
        }
    }
}

#[tokio::test]
async fn ingress_stop_wakes_read_and_reserve_with_exact_backing_custody() {
    let (writer, reader) = tokio::io::duplex(4096);
    let pending = Arc::new(tokio::sync::Notify::new());
    let mut reading = spawn_tun_ingress_pump(
        ObservedReader {
            inner: reader,
            pending: Arc::clone(&pending),
        },
        1200,
        1,
    );
    pending.notified().await;
    let backing = rx_buffer_capacity_for_mtu(1200) as u64;
    assert_eq!(
        reading.control.producer_backing.load(Ordering::Relaxed),
        backing
    );
    reading.request_stop();
    assert_eq!(reading.observe_join().await, PumpExit::StopRequested);
    assert!(task_handle(&reading).is_none());
    assert_eq!(reading.control.producer_backing.load(Ordering::Relaxed), 0);
    assert_eq!(reading.control.queued_backing.load(Ordering::Relaxed), 0);
    drop(writer);

    let (mut writer, reader) = tokio::io::duplex(4096);
    let mut reserving = spawn_tun_ingress_pump(reader, 1200, 1);
    writer.write_all(&[0x11; 16]).await.unwrap();
    wait_until(|| reserving.control.stats.packets.load(Ordering::Relaxed) == 1).await;
    writer.write_all(&[0x22; 16]).await.unwrap();
    wait_until(|| reserving.control.stats.full_waits.load(Ordering::Relaxed) == 1).await;
    assert_eq!(reserving.receiver.len(), 1);
    assert_eq!(
        reserving.control.queued_backing.load(Ordering::Relaxed),
        backing
    );
    assert_eq!(
        reserving.control.producer_backing.load(Ordering::Relaxed),
        backing
    );
    // Both 16-byte payloads still own their original MTU-sized backing allocations.
    reserving.request_stop();
    assert_eq!(reserving.observe_join().await, PumpExit::StopRequested);
    assert_eq!(
        reserving.control.producer_backing.load(Ordering::Relaxed),
        0
    );
    reserving.receiver.close();
    assert_eq!(
        reserving.control.queued_backing.load(Ordering::Relaxed),
        backing
    );
    let packet = reserving.recv().await.unwrap().unwrap();
    assert_eq!(&packet[..], &[0x11; 16]);
    assert_eq!(packet.capacity() as u64, backing);
    assert_eq!(reserving.control.queued_backing.load(Ordering::Relaxed), 0);
    // Only the pump is settled: the receiving caller still owns this allocation.
    drop(packet);
    reserving.drain();
    assert_eq!(reserving.observe_join().await, PumpExit::StopRequested);
    assert_eq!(reserving.control.stats.closed.load(Ordering::Relaxed), 1);
    drop(writer);
}

#[tokio::test]
async fn ingress_cancelled_join_observation_keeps_the_same_outstanding_handle() {
    let (writer, reader) = tokio::io::duplex(4096);
    let pending = Arc::new(tokio::sync::Notify::new());
    let mut pump = spawn_tun_ingress_pump(
        ObservedReader {
            inner: reader,
            pending: Arc::clone(&pending),
        },
        1200,
        1,
    );
    pending.notified().await;
    let original_id = task_handle(&pump).unwrap().id();
    for _ in 0..8 {
        {
            let observation = pump.observe_join();
            tokio::pin!(observation);
            std::future::poll_fn(|cx| {
                assert!(observation.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            // Dropping the observation future does not drop, take or abort the handle.
        }
        assert_eq!(task_handle(&pump).unwrap().id(), original_id);
        assert_eq!(pump.control.stats.closed.load(Ordering::Relaxed), 0);
    }
    pump.request_stop();
    wait_until(|| task_handle(&pump).unwrap().is_finished()).await;
    assert_eq!(task_handle(&pump).unwrap().id(), original_id);
    assert_eq!(pump.control.stats.closed.load(Ordering::Relaxed), 1);
    assert_eq!(join_pump(&mut pump).await, PumpExit::StopRequested);
    assert!(task_handle(&pump).is_none());
    assert_eq!(join_pump(&mut pump).await, PumpExit::StopRequested);
    drop(writer);
}

struct OpaqueReadFailure {
    drops: Arc<AtomicU64>,
    _payload: Vec<u8>,
}

impl std::fmt::Debug for OpaqueReadFailure {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("opaque error Debug must never be called");
    }
}

impl std::fmt::Display for OpaqueReadFailure {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("opaque error Display must never be called");
    }
}

impl std::error::Error for OpaqueReadFailure {}

impl Drop for OpaqueReadFailure {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

struct PacketThenError {
    sent: bool,
    payload_drops: Arc<AtomicU64>,
    reader_drops: Arc<AtomicU64>,
}

impl AsyncRead for PacketThenError {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.sent {
            this.sent = true;
            buf.put_slice(&[0x41; 16]);
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(std::io::Error::new(
                ErrorKind::InvalidData,
                OpaqueReadFailure {
                    drops: Arc::clone(&this.payload_drops),
                    _payload: vec![0; 64 * 1024],
                },
            )))
        }
    }
}

impl Drop for PacketThenError {
    fn drop(&mut self) {
        self.reader_drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn ingress_full_queue_read_error_is_retained_without_blocking_on_notification() {
    let payload_drops = Arc::new(AtomicU64::new(0));
    let reader_drops = Arc::new(AtomicU64::new(0));
    let mut pump = spawn_tun_ingress_pump(
        PacketThenError {
            sent: false,
            payload_drops: Arc::clone(&payload_drops),
            reader_drops: Arc::clone(&reader_drops),
        },
        1200,
        1,
    );
    wait_until(|| task_handle(&pump).unwrap().is_finished()).await;
    assert_eq!(payload_drops.load(Ordering::Relaxed), 1);
    assert_eq!(reader_drops.load(Ordering::Relaxed), 1);
    assert_eq!(pump.receiver.len(), 1);
    assert_eq!(pump.control.producer_backing.load(Ordering::Relaxed), 0);
    assert_eq!(
        pump.control.queued_backing.load(Ordering::Relaxed),
        rx_buffer_capacity_for_mtu(1200) as u64
    );
    assert_eq!(join_pump(&mut pump).await, PumpExit::Failed(PumpFailureId));
    let failure = pump.take_failure().expect("first physical role slot");
    assert_eq!(failure.facts().class, FailureClass::Io);
    assert_eq!(failure.facts().io_kind, Some(ErrorKind::InvalidData));
    assert!(failure.facts().opaque);
    assert!(failure.facts().source_content_omitted);
    assert_eq!(failure.text_bytes(), b"");
    assert!(pump.take_failure().is_none());
    assert_eq!(join_pump(&mut pump).await, PumpExit::Failed(PumpFailureId));
    let packet = pump.recv().await.unwrap().unwrap();
    assert_eq!(&packet[..], &[0x41; 16]);
    assert_eq!(pump.control.queued_backing.load(Ordering::Relaxed), 0);
    drop(packet);
    assert_eq!(
        pump.try_recv().unwrap().unwrap_err().kind(),
        ErrorKind::InvalidData
    );
    assert!(pump.recv().await.is_none());
    assert_eq!(pump.control.stats.read_errors.load(Ordering::Relaxed), 1);
}

struct PanickingReader {
    panic_on_read: bool,
    pending: Arc<tokio::sync::Notify>,
    payload_drops: Arc<AtomicU64>,
    reader_drops: Arc<AtomicU64>,
}

impl PanickingReader {
    fn payload(&self) -> OpaqueReadFailure {
        OpaqueReadFailure {
            drops: Arc::clone(&self.payload_drops),
            _payload: vec![0; 64 * 1024],
        }
    }
}

impl AsyncRead for PanickingReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.panic_on_read {
            std::panic::panic_any(self.payload());
        }
        self.pending.notify_one();
        Poll::Pending
    }
}

impl Drop for PanickingReader {
    fn drop(&mut self) {
        self.reader_drops.fetch_add(1, Ordering::Relaxed);
        if !self.panic_on_read {
            std::panic::panic_any(self.payload());
        }
    }
}

#[tokio::test]
async fn ingress_poll_and_reader_drop_panics_release_payload_before_join_observation() {
    for (panic_on_read, abort_child) in [(true, false), (false, false), (false, true)] {
        let pending = Arc::new(tokio::sync::Notify::new());
        let payload_drops = Arc::new(AtomicU64::new(0));
        let reader_drops = Arc::new(AtomicU64::new(0));
        let mut pump = spawn_tun_ingress_pump(
            PanickingReader {
                panic_on_read,
                pending: Arc::clone(&pending),
                payload_drops: Arc::clone(&payload_drops),
                reader_drops: Arc::clone(&reader_drops),
            },
            1200,
            1,
        );
        if !panic_on_read {
            pending.notified().await;
            if abort_child {
                task_handle(&pump).unwrap().abort();
            } else {
                pump.request_stop();
            }
        }
        wait_until(|| task_handle(&pump).unwrap().is_finished()).await;
        // The unobserved completed task owns no opaque payload or live reader.
        assert_eq!(payload_drops.load(Ordering::Relaxed), 1);
        assert_eq!(reader_drops.load(Ordering::Relaxed), 1);
        assert_eq!(pump.control.stats.closed.load(Ordering::Relaxed), 1);
        assert_eq!(pump.control.producer_backing.load(Ordering::Relaxed), 0);
        let failure = pump
            .take_failure()
            .expect("panic captured at child boundary");
        assert_eq!(failure.facts().class, FailureClass::Panic);
        assert!(failure.facts().opaque);
        assert_eq!(failure.text_bytes(), b"");
        if abort_child {
            assert_eq!(failure.facts().origin.phase, FailurePhase::DeviceJoin);
        }
        assert_eq!(join_pump(&mut pump).await, PumpExit::Failed(PumpFailureId));
        assert!(task_handle(&pump).is_none());
        assert!(pump.take_failure().is_none());
        assert_eq!(
            pump.control.additional_failures.load(Ordering::Relaxed),
            u64::from(abort_child)
        );
    }
}

#[tokio::test]
async fn ingress_unrequested_eof_is_sticky_failed_after_record_transfer() {
    let mut pump = spawn_tun_ingress_pump(tokio::io::empty(), 1200, 1);
    assert_eq!(join_pump(&mut pump).await, PumpExit::Failed(PumpFailureId));
    assert!(task_handle(&pump).is_none());
    let failure = pump.take_failure().expect("EOF remains a retained failure");
    assert_eq!(failure.facts().class, FailureClass::Io);
    assert_eq!(failure.facts().io_kind, Some(ErrorKind::UnexpectedEof));
    assert_eq!(failure.facts().origin.phase, FailurePhase::DeviceRead);
    assert_eq!(
        pump.try_recv().unwrap().unwrap_err().kind(),
        ErrorKind::UnexpectedEof
    );
    assert!(pump.take_failure().is_none());
    assert_eq!(join_pump(&mut pump).await, PumpExit::Failed(PumpFailureId));
    assert!(pump.recv().await.is_none());
    assert_eq!(pump.control.stats.read_errors.load(Ordering::Relaxed), 0);
    assert_eq!(pump.control.stats.closed.load(Ordering::Relaxed), 1);
    assert_eq!(pump.control.producer_backing.load(Ordering::Relaxed), 0);
    assert_eq!(pump.control.queued_backing.load(Ordering::Relaxed), 0);

    // An explicit stop already present before the read wins over a ready EOF.
    let mut stopped = spawn_tun_ingress_pump(tokio::io::empty(), 1200, 1);
    stopped.request_stop();
    assert_eq!(join_pump(&mut stopped).await, PumpExit::StopRequested);
    assert!(stopped.take_failure().is_none());
}

#[tokio::test]
async fn ingress_stop_request_and_consumer_close_keep_distinct_terminal_reasons() {
    let (stop_writer, stop_reader) = tokio::io::duplex(4096);
    let (close_writer, close_reader) = tokio::io::duplex(4096);
    let mut stopped = spawn_tun_ingress_pump(stop_reader, 1200, 1);
    let mut closed = spawn_tun_ingress_pump(close_reader, 1200, 1);
    stopped.request_stop();
    closed.receiver.close();
    let stop_exit = join_pump(&mut stopped).await;
    let close_exit = join_pump(&mut closed).await;
    drop((stop_writer, close_writer));
    assert_eq!(stop_exit, PumpExit::StopRequested);
    assert_eq!(close_exit, PumpExit::ConsumerClosed);
    assert_ne!(
        stop_exit, close_exit,
        "explicit stop and consumer closure must retain distinct terminal reasons"
    );
    assert_eq!(join_pump(&mut stopped).await, stop_exit);
    assert_eq!(join_pump(&mut closed).await, close_exit);
}

struct FiniteInterruptedReader {
    polls: Arc<AtomicU64>,
    first_read: Arc<tokio::sync::Notify>,
}

impl AsyncRead for FiniteInterruptedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let count = self.polls.fetch_add(1, Ordering::Relaxed) + 1;
        self.first_read.notify_one();
        if count <= 256 {
            Poll::Ready(Err(std::io::Error::from(ErrorKind::Interrupted)))
        } else {
            // The finite fallback prevents this regression test from pinning the runtime.
            Poll::Pending
        }
    }
}

#[tokio::test]
async fn ingress_interrupted_reads_yield_to_the_stop_observer() {
    let polls = Arc::new(AtomicU64::new(0));
    let first_read = Arc::new(tokio::sync::Notify::new());
    let mut pump = spawn_tun_ingress_pump(
        FiniteInterruptedReader {
            polls: Arc::clone(&polls),
            first_read: Arc::clone(&first_read),
        },
        1200,
        1,
    );
    first_read.notified().await;
    let polls_before_stopper = polls.load(Ordering::Relaxed);
    pump.request_stop();
    assert_eq!(join_pump(&mut pump).await, PumpExit::StopRequested);
    assert_eq!(pump.control.producer_backing.load(Ordering::Relaxed), 0);
    assert_eq!(pump.control.stats.read_errors.load(Ordering::Relaxed), 0);
    assert!(
        polls_before_stopper < 257,
        "Interrupted retries must yield before exhausting the finite reader"
    );
}

#[tokio::test]
async fn ingress_owner_drop_requests_stop_and_releases_both_packet_custodies() {
    let (mut writer, reader) = tokio::io::duplex(4096);
    let pump = spawn_tun_ingress_pump(reader, 1200, 1);
    let control = Arc::clone(&pump.control);
    writer.write_all(&[0x11; 16]).await.unwrap();
    wait_until(|| control.stats.packets.load(Ordering::Relaxed) == 1).await;
    writer.write_all(&[0x22; 16]).await.unwrap();
    wait_until(|| control.stats.full_waits.load(Ordering::Relaxed) == 1).await;
    let backing = rx_buffer_capacity_for_mtu(1200) as u64;
    assert_eq!(control.queued_backing.load(Ordering::Relaxed), backing);
    assert_eq!(control.producer_backing.load(Ordering::Relaxed), backing);
    drop(pump);
    assert!(control.stop.load(Ordering::Acquire));
    wait_until(|| control.stats.closed.load(Ordering::Relaxed) == 1).await;
    assert_eq!(control.producer_backing.load(Ordering::Relaxed), 0);
    assert_eq!(control.queued_backing.load(Ordering::Relaxed), 0);
    assert!(!control.failed.load(Ordering::Acquire));
    // Drop requests cleanup; these probes do not claim that a join was observed.
    drop(writer);
}

#[tokio::test]
async fn ingress_queued_packet_precedes_terminal_error_for_async_and_try_receive() {
    let mut orders = Vec::new();
    for asynchronous in [true, false] {
        let mut pump = spawn_tun_ingress_pump(
            PacketThenError {
                sent: false,
                payload_drops: Arc::new(AtomicU64::new(0)),
                reader_drops: Arc::new(AtomicU64::new(0)),
            },
            1200,
            1,
        );
        assert_eq!(join_pump(&mut pump).await, PumpExit::Failed(PumpFailureId));
        let (first, second) = if asynchronous {
            (pump.recv().await.unwrap(), pump.recv().await.unwrap())
        } else {
            (pump.try_recv().unwrap(), pump.try_recv().unwrap())
        };
        orders.push((
            first.as_ref().is_ok_and(|packet| &packet[..] == [0x41; 16]),
            second
                .as_ref()
                .is_err_and(|error| error.kind() == ErrorKind::InvalidData),
        ));
        assert!(pump.recv().await.is_none());
        assert_eq!(pump.control.queued_backing.load(Ordering::Relaxed), 0);
    }
    assert!(
        orders.iter().all(|(packet, error)| *packet && *error),
        "a queued packet must precede the terminal read error"
    );
}

#[tokio::test]
async fn ingress_cancelled_child_retains_failure_and_drains_backing_once() {
    let (mut writer, reader) = tokio::io::duplex(4096);
    let pending = Arc::new(tokio::sync::Notify::new());
    let mut pump = spawn_tun_ingress_pump(
        ObservedReader {
            inner: reader,
            pending: Arc::clone(&pending),
        },
        1200,
        4,
    );
    pending.notified().await;
    writer.write_all(&[0x77; 16]).await.unwrap();
    wait_until(|| pump.control.stats.packets.load(Ordering::Relaxed) == 1).await;
    pending.notified().await;
    task_handle(&pump).unwrap().abort();
    assert_eq!(join_pump(&mut pump).await, PumpExit::Failed(PumpFailureId));
    assert_eq!(pump.control.producer_backing.load(Ordering::Relaxed), 0);
    assert_eq!(
        pump.control.queued_backing.load(Ordering::Relaxed),
        rx_buffer_capacity_for_mtu(1200) as u64
    );
    let failure = pump
        .take_failure()
        .expect("cancelled child is not a clean stop");
    assert_eq!(failure.facts().class, FailureClass::Cancelled);
    assert_eq!(failure.facts().origin.phase, FailurePhase::DeviceJoin);
    pump.receiver.close();
    assert_eq!(pump.receiver.len(), 1);
    pump.drain();
    assert_eq!(pump.receiver.len(), 0);
    assert_eq!(pump.control.queued_backing.load(Ordering::Relaxed), 0);
    pump.drain();
    assert_eq!(pump.control.queued_backing.load(Ordering::Relaxed), 0);
    assert!(pump.take_failure().is_none());
    assert_eq!(join_pump(&mut pump).await, PumpExit::Failed(PumpFailureId));
    assert_eq!(pump.control.additional_failures.load(Ordering::Relaxed), 0);
    drop(writer);
}

#[tokio::test]
async fn device_shutdown_transfers_io_and_cancelled_failures_once_after_join_and_drain() {
    let mut failed = spawn_tun_ingress_pump(
        PacketThenError {
            sent: false,
            payload_drops: Arc::new(AtomicU64::new(0)),
            reader_drops: Arc::new(AtomicU64::new(0)),
        },
        1200,
        1,
    );
    wait_until(|| task_handle(&failed).unwrap().is_finished()).await;
    let (writer, reader) = tokio::io::duplex(4096);
    let pending = Arc::new(tokio::sync::Notify::new());
    let mut cancelled = spawn_tun_ingress_pump(
        ObservedReader {
            inner: reader,
            pending: Arc::clone(&pending),
        },
        1200,
        1,
    );
    tokio::time::timeout(Duration::from_secs(1), pending.notified())
        .await
        .expect("cancelled reader must first reach Pending");
    task_handle(&cancelled).unwrap().abort();
    assert!(failed.control.queued_backing.load(Ordering::Relaxed) > 0);
    assert!(cancelled.control.producer_backing.load(Ordering::Relaxed) > 0);

    for (pump, class, kind) in [
        (&mut failed, FailureClass::Io, ErrorKind::InvalidData),
        (&mut cancelled, FailureClass::Cancelled, ErrorKind::Other),
    ] {
        let control = Arc::clone(&pump.control);
        let mut result = tokio::time::timeout(
            Duration::from_secs(1),
            std::future::poll_fn(|cx| pump.poll_shutdown(cx)),
        )
        .await
        .expect("shutdown must observe the actual join before settling");
        assert!(task_handle(pump).is_none());
        assert_eq!(control.producer_backing.load(Ordering::Relaxed), 0);
        assert_eq!(control.queued_backing.load(Ordering::Relaxed), 0);
        assert_eq!(result.state(), DeviceShutdownState::FailedSettled);
        assert_eq!(result.io_kind(), Some(kind));
        assert_eq!(result.take_failure().unwrap().facts().class, class);
        assert!(result.take_failure().is_none());
        assert_eq!(result.state(), DeviceShutdownState::FailedSettled);
        assert_eq!(result.io_kind(), Some(kind));
        let mut repeated = std::future::poll_fn(|cx| pump.poll_shutdown(cx)).await;
        assert_eq!(repeated.state(), DeviceShutdownState::FailedSettled);
        assert_eq!(repeated.io_kind(), Some(kind));
        assert!(repeated.take_failure().is_none());
        assert_eq!(control.producer_backing.load(Ordering::Relaxed), 0);
        assert_eq!(control.queued_backing.load(Ordering::Relaxed), 0);
    }
    drop(writer);
}

#[test]
fn device_shutdown_public_failure_constructor_keeps_fixed_failed_state() {
    let mut result = DeviceShutdown::failed(ErrorKind::PermissionDenied);
    assert_eq!(result.state(), DeviceShutdownState::FailedSettled);
    assert_eq!(result.io_kind(), Some(ErrorKind::PermissionDenied));
    assert!(result.take_failure().is_none());
    assert_eq!(result.state(), DeviceShutdownState::FailedSettled);
    assert_eq!(result.io_kind(), Some(ErrorKind::PermissionDenied));
}

#[tokio::test]
async fn device_shutdown_moves_additional_failure_delta_after_first_record_transfer() {
    let pending = Arc::new(tokio::sync::Notify::new());
    let payload_drops = Arc::new(AtomicU64::new(0));
    let reader_drops = Arc::new(AtomicU64::new(0));
    let mut pump = spawn_tun_ingress_pump(
        PanickingReader {
            panic_on_read: false,
            pending: Arc::clone(&pending),
            payload_drops: Arc::clone(&payload_drops),
            reader_drops: Arc::clone(&reader_drops),
        },
        1200,
        1,
    );
    tokio::time::timeout(Duration::from_secs(1), pending.notified())
        .await
        .expect("reader must reach Pending before abort");
    task_handle(&pump).unwrap().abort();
    wait_until(|| task_handle(&pump).unwrap().is_finished()).await;
    let control = Arc::clone(&pump.control);
    assert_eq!(payload_drops.load(Ordering::Relaxed), 1);
    assert_eq!(reader_drops.load(Ordering::Relaxed), 1);
    let first = pump
        .take_failure()
        .expect("destructor panic must be retained");
    assert_eq!(first.facts().class, FailureClass::Panic);
    drop(first);

    let mut result = tokio::time::timeout(
        Duration::from_secs(1),
        std::future::poll_fn(|cx| pump.poll_shutdown(cx)),
    )
    .await
    .expect("cancelled child must be joined");
    assert!(task_handle(&pump).is_none());
    assert_eq!(control.producer_backing.load(Ordering::Relaxed), 0);
    assert_eq!(control.queued_backing.load(Ordering::Relaxed), 0);
    assert_eq!(result.state(), DeviceShutdownState::FailedSettled);
    assert!(result.take_failure().is_none());
    assert_eq!(result.take_additional_failures(), 1);
    assert_eq!(result.take_additional_failures(), 0);
    assert_eq!(control.additional_failures.load(Ordering::Relaxed), 0);
    let mut repeated = std::future::poll_fn(|cx| pump.poll_shutdown(cx)).await;
    assert_eq!(repeated.state(), DeviceShutdownState::FailedSettled);
    assert_eq!(repeated.io_kind(), Some(ErrorKind::Other));
    assert!(repeated.take_failure().is_none());
    assert_eq!(repeated.take_additional_failures(), 0);
}

#[tokio::test]
async fn settled_virtual_shell_releases_reinserted_backing_and_rejects_io() {
    // This is a stopped diagnostic shell, not a real TUN/backend acceptance fixture.
    let mut device = VirtualTunDevice {
        backend: VirtualTunBackend::Stopped,
        stopping: true,
        shutdown_failure: None,
        interface_name: Some("closed-test-shell".into()),
        mtu: 1200,
        rx_buffer: Some(BytesMut::with_capacity(192)),
        iface_rx_queue: VecDeque::from([BytesMut::zeroed(64)]),
        tx_queue: VecDeque::from([BytesMut::zeroed(96)]),
        flushed_tcp_packets: Vec::with_capacity(4),
    };
    assert!(device.rx_buffer.as_ref().unwrap().capacity() >= 192);
    assert!(device.iface_rx_queue.capacity() > 0);
    assert!(device.flushed_tcp_packets.capacity() > 0);
    assert_eq!(
        device.wait_for_rx().await.unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );
    assert_eq!(
        device.try_recv_rx().unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );
    assert_eq!(
        device.flush_tx().await.unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );
    assert!(device.rx_peek().is_none());
    assert!(device.rx_take().is_none());
    assert!(!device.stage_rx_for_iface());
    let now = smoltcp::time::Instant::from_millis(0);
    assert!(Device::receive(&mut device, now).is_none());
    assert!(Device::transmit(&mut device, now).is_none());
    let queued = device.queued_tx_bytes();
    device.inject_ip_packet(&[1; 16]);
    assert_eq!(device.queued_tx_bytes(), queued);

    for round in 0..2 {
        let result = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        assert_eq!(result.state(), DeviceShutdownState::Settled);
        assert!(matches!(device.backend, VirtualTunBackend::Stopped));
        assert!(device.rx_buffer.is_none());
        assert_eq!(device.iface_rx_queue.capacity(), 0);
        assert_eq!(device.tx_queue.capacity(), 0);
        assert_eq!(device.flushed_tcp_packets.capacity(), 0);
        assert_eq!(device.queued_tx_bytes(), 0);
        assert_eq!(device.interface_name(), Some("closed-test-shell"));
        if round == 0 {
            // Existing public fields can be written after the first settled observation.
            device.rx_buffer = Some(BytesMut::with_capacity(256));
            device.tx_queue.reserve(4);
            device.tx_queue.push_back(BytesMut::with_capacity(512));
            assert!(device.rx_buffer.as_ref().unwrap().capacity() >= 256);
            assert!(device.tx_queue.capacity() > 0);
            assert!(device.tx_queue.front().unwrap().capacity() >= 512);
        }
    }
}

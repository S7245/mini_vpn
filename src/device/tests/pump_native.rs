use super::*;
use crate::device::pump_native::{Prepared, Source};
use bytes::{BackingBudget, BackingMeter};

fn budget() -> (BackingBudget, BackingMeter) {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    (budget, meter)
}
async fn condition(mut f: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !f() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn shutdown(pump: &mut TunIngressPump) -> DeviceShutdown {
    tokio::time::timeout(
        Duration::from_secs(1),
        std::future::poll_fn(|cx| pump.poll_shutdown(cx)),
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn native_cold_lane_full_precedes_read_and_cancellation_refunds_real_layout() {
    let layout = quinn::owned_events::slot_layout::<PumpPacket>(2)
        .unwrap()
        .size() + std::mem::size_of::<PumpControl>();
    let tiny = BackingBudget::new(layout - 1).unwrap();
    assert!(Prepared::new(2, &BackingMeter::with_budget(&tiny)).is_err());
    assert_eq!(tiny.used(), 0);
    let (budget, meter) = budget();
    let cold = Prepared::new(2, &meter).unwrap();
    assert_eq!(budget.used(), layout);
    drop(cold);
    assert_eq!(budget.used(), 0);
    let exact = BackingBudget::new(layout).unwrap();
    let meter = BackingMeter::with_budget(&exact);
    let (_writer, reader) = tokio::io::duplex(128);
    let mut pump = Prepared::new(2, &meter).unwrap().spawn_reader(reader, 1200);
    let exit = tokio::time::timeout(Duration::from_secs(1), pump.observe_join())
        .await
        .unwrap();
    assert_eq!(exit, PumpExit::Failed(PumpFailureId));
    assert_eq!(
        pump.take_failure().unwrap().facts().io_kind,
        Some(ErrorKind::WouldBlock)
    );
    assert_eq!(pump.snapshot().packets, 0);
    assert_eq!(
        shutdown(&mut pump).await.state(),
        DeviceShutdownState::FailedSettled
    );
    drop(pump);
    assert_eq!(exact.used(), 0);
    assert!(!exact.quarantined());
}
#[tokio::test]
async fn native_real_read_c_plus_one_cut_resume_same_actor_and_last_alias_charge() {
    let (budget, meter) = budget();
    let (mut writer, reader) = tokio::io::duplex(4096);
    let mut pump = Prepared::new(1, &meter).unwrap().spawn_reader(reader, 1200);
    let actor = pump.original_actor_id().unwrap();
    writer.write_all(b"first").await.unwrap();
    condition(|| pump.snapshot().packets == 1).await;
    writer.write_all(b"second").await.unwrap();
    condition(|| pump.snapshot().full_waits == 1).await;
    assert_eq!(pump.receiver.len(), 1);
    let size = rx_buffer_capacity_for_mtu(1200) as u64;
    assert_eq!(pump.control.producer_backing.load(Ordering::Relaxed), size);
    assert_eq!(pump.control.queued_backing.load(Ordering::Relaxed), size);
    let discarded = pump.cut().await.unwrap();
    assert_eq!(discarded, (2, 2 * size));
    assert_eq!(pump.original_actor_id(), Some(actor));
    pump.validate_resume().unwrap();
    pump.resume();
    writer.write_all(b"third").await.unwrap();
    let packet = pump.recv().await.unwrap().unwrap();
    assert!(
        !budget.quarantined(),
        "known typed transfer must not audit a moved-out backing"
    );
    assert_eq!(&packet[..], b"third");
    assert!(packet.precharged_backing_matches(&meter));
    let alias = packet.into_raw().unwrap().freeze();
    assert_eq!(
        shutdown(&mut pump).await.state(),
        DeviceShutdownState::Settled
    );
    drop(pump);
    assert_eq!(
        budget.used(),
        BytesMut::metered_copy_layout_for_len(size as usize).unwrap()
    );
    drop(alias);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
struct PanicPayload(Arc<AtomicU64>);
impl Drop for PanicPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
        panic!("opaque payload must stay retained");
    }
}
struct NativePanicSource {
    payloads: Arc<AtomicU64>,
    drops: Arc<AtomicU64>,
    io_error: bool,
}
impl Source for NativePanicSource {
    fn poll_packet(
        self: Pin<&mut Self>,
        packet: &mut PumpPacket,
        _: &mut Context<'_>,
    ) -> Poll<std::io::Result<SourceRead>> {
        packet.prepare_native(128).unwrap();
        if self.io_error {
            return Poll::Ready(Err(std::io::Error::new(
                ErrorKind::InvalidData,
                IoDrop(self.payloads.clone()),
            )));
        }
        std::panic::panic_any(PanicPayload(self.payloads.clone()));
    }
}
impl Drop for NativePanicSource {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
        std::panic::panic_any(PanicPayload(self.payloads.clone()));
    }
}
#[tokio::test]
async fn native_poll_then_source_drop_preserves_two_original_boxes_and_real_join() {
    let (budget, meter) = budget();
    let payloads = Arc::new(AtomicU64::new(0));
    let drops = Arc::new(AtomicU64::new(0));
    let mut pump = Prepared::new(1, &meter).unwrap().spawn(NativePanicSource {
        payloads: payloads.clone(),
        drops: drops.clone(),
        io_error: false,
    });
    condition(|| pump.control.failed.load(Ordering::Acquire)).await;
    let result = shutdown(&mut pump).await;
    assert_eq!(result.state(), DeviceShutdownState::FailedSettled);
    assert!(matches!(pump.task, PumpTask::Observed(_)));
    let mut result = result;
    assert_eq!(
        result.take_failure().unwrap().facts().origin.phase,
        FailurePhase::DeviceRead
    );
    assert_eq!(result.take_additional_failures(), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(payloads.load(Ordering::Relaxed), 0);
    assert_eq!(pump.snapshot().packets, 0);
    drop(pump);
    assert_eq!(budget.used(), 1);
    assert_eq!(payloads.load(Ordering::Relaxed), 0);
}

#[derive(Debug)]
struct IoDrop(Arc<AtomicU64>);
impl std::fmt::Display for IoDrop {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("opaque IO is never formatted");
    }
}
impl std::error::Error for IoDrop {}
impl Drop for IoDrop {
    fn drop(&mut self) {
        std::panic::panic_any(PanicPayload(self.0.clone()));
    }
}
#[tokio::test]
async fn native_original_io_precedes_error_drop_and_source_drop_payloads() {
    let (budget, meter) = budget();
    let payloads = Arc::new(AtomicU64::new(0));
    let drops = Arc::new(AtomicU64::new(0));
    let mut pump = Prepared::new(1, &meter).unwrap().spawn(NativePanicSource {
        payloads: payloads.clone(),
        drops: drops.clone(),
        io_error: true,
    });
    condition(|| pump.control.failed.load(Ordering::Acquire)).await;
    let mut result = shutdown(&mut pump).await;
    let failure = result.take_failure().unwrap();
    assert_eq!(failure.facts().io_kind, Some(ErrorKind::InvalidData));
    assert_eq!(failure.facts().origin.phase, FailurePhase::DeviceRead);
    assert_eq!(result.take_additional_failures(), 2);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(pump);
    assert_eq!(payloads.load(Ordering::Relaxed), 0);
    assert_eq!(budget.used(), 1);
}

struct WakeFailure {
    control: std::sync::Weak<PumpControl>,
    payloads: Arc<AtomicU64>,
    wakes: Arc<AtomicU64>,
}
impl std::task::Wake for WakeFailure {
    fn wake(self: Arc<Self>) {
        let control = self.control.upgrade().unwrap();
        assert!(!matches!(
            control.first_failure.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        self.wakes.fetch_add(1, Ordering::Relaxed);
        std::panic::panic_any(PanicPayload(self.payloads.clone()));
    }
}
#[tokio::test]
async fn native_sealed_notification_aborts_original_parked_handle_and_joins() {
    let (budget, meter) = budget();
    let (_writer, reader) = tokio::io::duplex(128);
    let mut pump = Prepared::new(1, &meter).unwrap().spawn_reader(reader, 1200);
    condition(|| pump.control.producer_backing.load(Ordering::Relaxed) != 0).await;
    let original = pump.original_actor_id().unwrap();
    let control = pump.control.clone();
    let payloads = Arc::new(AtomicU64::new(0));
    let wakes = Arc::new(AtomicU64::new(0));
    let waker = std::task::Waker::from(Arc::new(WakeFailure {
        control: Arc::downgrade(&control),
        payloads: payloads.clone(),
        wakes: wakes.clone(),
    }));
    let mut wait = Box::pin(control.stop_wake.notified());
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    pump.request_stop();
    assert!(budget.quarantined());
    assert_eq!(pump.original_actor_id(), Some(original));
    assert_eq!(wakes.load(Ordering::Relaxed), 1);
    assert_eq!(
        shutdown(&mut pump).await.state(),
        DeviceShutdownState::FailedSettled
    );
    assert!(matches!(pump.task, PumpTask::Observed(_)));
    control.notify(crate::device::pump_control::STOP | crate::device::pump_control::PAUSE);
    assert_eq!(wakes.load(Ordering::Relaxed), 1);
    drop(wait);
    drop(pump);
    drop(control);
    drop(waker);
    assert_eq!(payloads.load(Ordering::Relaxed), 0);
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn native_poison_and_forced_drop_never_make_clean_zero() {
    for poison in [false, true] {
        let (budget, meter) = budget();
        let (_writer, reader) = tokio::io::duplex(128);
        let mut pump = Prepared::new(1, &meter).unwrap().spawn_reader(reader, 1200);
        let control = pump.control.clone();
        condition(|| control.producer_backing.load(Ordering::Relaxed) != 0).await;
        if poison {
            assert!(
                catch_unwind(AssertUnwindSafe(|| {
                    let _lock = control.first_failure.lock().unwrap();
                    panic!("physical first-error ledger poison");
                }))
                .is_err()
            );
            assert_eq!(control.failure_kind(), ErrorKind::Other);
            assert!(budget.quarantined());
            assert_eq!(
                shutdown(&mut pump).await.state(),
                DeviceShutdownState::FailedSettled
            );
        }
        drop(pump);
        assert!(budget.quarantined());
        condition(|| control.stats.closed.load(Ordering::Relaxed) == 1).await;
        drop(control);
        assert_eq!(budget.used(), 1);
    }
}
#[tokio::test]
async fn native_external_q_retains_prefix_until_shutdown_and_never_reports_clean_eof() {
    let (budget, meter) = budget();
    let (mut writer, reader) = tokio::io::duplex(128);
    let mut pump = Prepared::new(1, &meter).unwrap().spawn_reader(reader, 1200);
    writer.write_all(b"held").await.unwrap();
    condition(|| pump.snapshot().packets == 1).await;
    meter.quarantine();
    assert_eq!(pump.receiver.len(), 1);
    assert!(pump.recv().await.unwrap().is_err());
    assert_eq!(pump.receiver.len(), 1);
    assert_eq!(
        shutdown(&mut pump).await.state(),
        DeviceShutdownState::FailedSettled
    );
    assert_eq!(pump.receiver.len(), 0);
    drop(pump);
    assert_eq!(budget.used(), 1);
}

struct InvalidSource(u8);
impl Source for InvalidSource {
    fn poll_packet(
        self: Pin<&mut Self>,
        packet: &mut PumpPacket,
        _: &mut Context<'_>,
    ) -> Poll<std::io::Result<SourceRead>> {
        match self.0 {
            0 => packet.adopt(BytesMut::from(&b"raw"[..])),
            1 => {
                packet.prepare_native(128)?;
                packet.finish_native(4)?;
                packet.quarantine_native();
            }
            2 => {
                packet.prepare_native(128)?;
                return Poll::Pending;
            }
            _ => (),
        }
        Poll::Ready(Ok(SourceRead::Packet))
    }
    fn cancel_read(self: Pin<&mut Self>) -> std::io::Result<()> {
        // A source may fail its own authority without unwinding. The actor must
        // recheck before publishing the paused witness, just as for DATA.
        Ok(())
    }
}
#[tokio::test]
async fn native_final_publication_gate_rejects_raw_missing_and_post_read_q() {
    for mode in [0, 1, 3] {
        let (budget, meter) = budget();
        let mut pump = Prepared::new(1, &meter).unwrap().spawn(InvalidSource(mode));
        tokio::time::timeout(Duration::from_secs(1), pump.observe_join())
            .await
            .unwrap();
        assert_eq!(pump.snapshot().packets, 0);
        assert_eq!(pump.receiver.len(), 0);
        assert_eq!(
            shutdown(&mut pump).await.state(),
            DeviceShutdownState::FailedSettled
        );
        drop(pump);
        assert_eq!(budget.used(), 1);
    }
}
struct CancelQuarantine(BackingMeter);
impl Source for CancelQuarantine {
    fn poll_packet(
        self: Pin<&mut Self>,
        packet: &mut PumpPacket,
        _: &mut Context<'_>,
    ) -> Poll<std::io::Result<SourceRead>> {
        packet.prepare_native(128)?;
        Poll::Pending
    }
    fn cancel_read(self: Pin<&mut Self>) -> std::io::Result<()> {
        self.0.quarantine();
        Ok(())
    }
}
#[tokio::test]
async fn native_cancel_post_q_never_publishes_a_paused_witness() {
    let (budget, meter) = budget();
    let mut pump = Prepared::new(1, &meter)
        .unwrap()
        .spawn(CancelQuarantine(meter.clone()));
    condition(|| pump.control.producer_backing.load(Ordering::Relaxed) != 0).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), pump.cut())
            .await
            .unwrap()
            .is_err()
    );
    assert!(!pump.control.pause.paused.load(Ordering::Acquire));
    assert_eq!(
        shutdown(&mut pump).await.state(),
        DeviceShutdownState::FailedSettled
    );
    drop(pump);
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn native_pause_ack_notification_failure_finishes_actor_without_another_stop() {
    let (budget, meter) = budget();
    let (_writer, reader) = tokio::io::duplex(128);
    let mut pump = Prepared::new(1, &meter).unwrap().spawn_reader(reader, 1200);
    let control = pump.control.clone();
    condition(|| control.producer_backing.load(Ordering::Relaxed) != 0).await;
    let mut cut = Box::pin(pump.cut());
    assert!(
        cut.as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    // Request notification is already complete. Only the real actor's PAUSED
    // acknowledgement can invoke this newly registered failing waiter.
    let payloads = Arc::new(AtomicU64::new(0));
    let wakes = Arc::new(AtomicU64::new(0));
    let waker = std::task::Waker::from(Arc::new(WakeFailure {
        control: Arc::downgrade(&control),
        payloads: payloads.clone(),
        wakes: wakes.clone(),
    }));
    let mut ack = Box::pin(control.pause.changed.notified());
    assert!(
        ack.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let finished = tokio::time::timeout(Duration::from_millis(100), async {
        while control.stats.closed.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let cut_result = tokio::time::timeout(Duration::from_millis(100), &mut cut).await;
    let refused = matches!(cut_result, Ok(Err(_)));
    drop(cut);
    // Preserve all observations, then settle the original handle before asserting.
    let result = shutdown(&mut pump).await;
    assert!(matches!(pump.task, PumpTask::Observed(_)));
    drop(ack);
    drop(pump);
    drop(control);
    drop(waker);
    assert!(
        finished,
        "Q/SEALED actor stayed Pending after its actual PAUSE ack notification failed"
    );
    assert!(refused);
    assert_eq!(result.state(), DeviceShutdownState::FailedSettled);
    assert_eq!(wakes.load(Ordering::Relaxed), 1);
    assert_eq!(payloads.load(Ordering::Relaxed), 0);
    assert_eq!(budget.used(), 1);
}

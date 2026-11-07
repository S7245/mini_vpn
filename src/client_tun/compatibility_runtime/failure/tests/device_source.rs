//! Actual Device Core -> original Session -> terminal IO disposition.
use super::*;
use crate::device::egress::CallbackCustody;
use std::{future::Future, task::{Context, Poll, Waker}, sync::{Weak, atomic::AtomicUsize}};

#[derive(Debug)]
struct OriginalIo {
    owner: Weak<SessionControl>,
    drops: Arc<AtomicUsize>,
    payload: Option<Arc<usize>>,
}
impl std::fmt::Display for OriginalIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("original ingress IO") }
}
impl std::error::Error for OriginalIo {}
impl Drop for OriginalIo {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            assert!(owner.ledger.try_lock().is_ok(), "actual original IO Drop held Session ledger");
        }
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(payload) = self.payload.take() { std::panic::panic_any(payload); }
    }
}
fn session(meter: &bytes::BackingMeter) -> Arc<SessionControl> {
    let mut owner = SessionControl::default();
    owner.app_meter = Some(meter.clone());
    Arc::new(owner)
}
#[tokio::test(start_paused = true)]
async fn device_original_io_alias_waits_then_drops_unlocked_before_clean() {
    for panic in [false, true] {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let owner = session(&meter);
        let deadline = owner.request_stop();
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
        let (tx, rx) = CallbackCustody::test_pair(&meter);
        let cold = budget.used();
        let drops = Arc::new(AtomicUsize::new(0));
        let payload = Arc::new(73usize);
        rx.note_write(Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, OriginalIo {
            owner: Arc::downgrade(&owner), drops: drops.clone(), payload: panic.then(|| payload.clone()),
        })));
        let error = rx.finish().unwrap_err();
        let original = crate::device::egress::callback_original_io(&error).unwrap();
        owner.record_device_read(error);
        owner.record_device_read(rx.finish().unwrap_err());
        owner.accept_device_report(tx.test_terminal_report().await);
        assert!(owner.ledger().first_egress_io.is_none(), "RX IO is not a TX failure gate");
        assert!(Arc::ptr_eq(owner.ledger().first_ingress_io.as_ref().unwrap(), &original));
        assert_eq!(owner.ledger().additional_failures, 0);
        drop((tx, rx));
        let mut work = Box::pin(owner.retire_device_sources());
        assert!(work.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(budget.used(), cold);
        assert!(!budget.quarantined());
        drop(original);
        tokio::time::advance(std::time::Duration::from_millis(5)).await;
        assert_eq!(work.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(()));
        drop(work);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let ledger = owner.ledger();
        assert_eq!(ledger.first_failure.as_ref().unwrap().facts().io_kind, Some(std::io::ErrorKind::ConnectionReset));
        assert_eq!(ledger.first_failure.as_ref().unwrap().facts().origin.phase, FailurePhase::DeviceRead);
        assert_eq!(ledger.shutdown_deadline, Some(deadline));
        assert_eq!(ledger.additional_failures, u64::from(panic));
        assert_eq!(ledger.quarantined, panic);
        assert!(ledger.first_ingress_io.is_none());
        drop(ledger);
        assert_eq!(budget.used(), if panic { cold } else { 0 });
        assert_eq!(budget.quarantined(), panic);
        assert_eq!(Arc::strong_count(&payload), if panic { 2 } else { 1 });
    }
}
#[tokio::test(start_paused = true)]
async fn cancelled_original_io_disposal_retains_actual_source_and_exclusive_turn() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = session(&meter);
    let deadline = owner.request_stop();
    let (tx, rx) = CallbackCustody::test_pair(&meter);
    let cold = budget.used();
    let drops = Arc::new(AtomicUsize::new(0));
    rx.note_write(Err(std::io::Error::new(std::io::ErrorKind::Other, OriginalIo {
        owner: Arc::downgrade(&owner), drops: drops.clone(), payload: None,
    })));
    let error = rx.finish().unwrap_err();
    let original = crate::device::egress::callback_original_io(&error).unwrap();
    let identity = Arc::downgrade(&original);
    owner.record_device_read(error);
    owner.accept_device_report(tx.test_terminal_report().await);
    drop((tx, rx));
    let mut work = Box::pin(owner.retire_device_sources());
    assert!(work.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    let mut competitor = Box::pin(owner.retire_device_sources());
    assert!(competitor.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    drop(competitor); // A non-owner must not cancel the actual disposition turn.
    assert!(!budget.quarantined());
    drop(work);
    drop(original);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), cold);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(identity.upgrade().is_some());
    assert!(owner.ledger().device_source_disposal);
    assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
}

#[tokio::test(start_paused = true)]
async fn original_io_alias_cannot_outlive_original_session_deadline_without_quiet() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = session(&meter);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    owner.ledger().shutdown_deadline = Some(deadline);
    let (tx, rx) = CallbackCustody::test_pair(&meter);
    rx.note_write(Err(std::io::ErrorKind::ConnectionReset.into()));
    let original = crate::device::egress::callback_original_io(&rx.finish().unwrap_err()).unwrap();
    owner.accept_device_report(tx.test_terminal_report().await);
    drop((tx, rx));
    let mut work = Box::pin(owner.retire_device_sources());
    assert!(work.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    assert!(work.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    assert!(owner.ledger().quarantined, "original deadline must quiet even while actual IO alias survives");
    assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
    assert_eq!(owner.ledger().additional_failures, 0);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), CallbackCustody::test_cold_bytes(2));
    drop((work, original));
}

#[tokio::test]
async fn actual_started_pump_main_error_and_observer_share_original_rx_fact() {
    use crate::{harness::{LoopbackTunDevice, PacketLink}, device::{DeviceBinding, TunIo}};
    for observer_first in [false, true] {
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let owner = session(&meter);
        let inbound = PacketLink::bounded(1);
        let mut device = LoopbackTunDevice::native_with_factory(1, 1500, &meter,
            || Ok((inbound.clone(), PacketLink::bounded(1)))).unwrap();
        device.bind_backing_meter(DeviceBinding::Native { meter: &meter, slots: 1, mtu: 1500 }).unwrap();
        let observer = device.ingress_failure_observer().unwrap();
        inbound.push(bytes::BytesMut::zeroed(65540)); // Actual started Source rejects this packet.
        let error = device.wait_for_rx().await.unwrap_err();
        let original = crate::device::egress::callback_original_io(&error).unwrap();
        if observer_first { owner.accept_device_report(observer.claim().unwrap()); }
        owner.note_loop_error(&error); // Original Main fatal-loop consumer.
        if !observer_first {
            if let Some(report) = observer.claim() { owner.accept_device_report(report); }
        }
        let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        owner.accept_device_report(receipt.settle_callbacks(SessionControl::origin(FailurePhase::DeviceShutdown)).await.unwrap());
        assert_eq!(owner.ledger().additional_failures, 0, "Main/Observer/Tail must claim once from actual Core");
        assert!(Arc::ptr_eq(owner.ledger().first_ingress_io.as_ref().unwrap(), &original));
        assert!(owner.ledger().first_egress_io.is_none());
        assert_eq!(owner.ledger().first_failure.as_ref().unwrap().facts().io_kind, Some(std::io::ErrorKind::InvalidInput));
        drop((original, error, observer, receipt, device));
        owner.retire_device_sources().await;
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn main_device_callback_keeps_original_raw_io_or_panic_and_no_second_fact() {
    for panic in [false, true] {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let owner = session(&meter);
        let (tx, rx) = CallbackCustody::test_pair(&meter);
        let payload = Arc::new(83usize);
        if panic { rx.retain_secondary(Box::new(payload.clone())); }
        else { rx.note_write(Err(std::io::Error::from_raw_os_error(5))); }
        let error = rx.finish().unwrap_err();
        owner.note_loop_error(&error);
        owner.note_loop_error(&error);
        let report = tx.claim(SessionControl::origin(FailurePhase::DeviceShutdown));
        owner.accept_device_report(report);
        let ledger = owner.ledger();
        let first = ledger.first_failure.as_ref().unwrap().facts();
        assert_eq!(first.class, if panic { crate::shared::errors::FailureClass::Panic } else { crate::shared::errors::FailureClass::Io });
        assert_eq!(first.raw_os_code, if panic { None } else { Some(5) });
        assert_eq!(ledger.additional_failures, 0);
        assert_eq!(ledger.quarantined, panic);
        assert!(ledger.first_egress_io.is_none());
        assert_eq!(Arc::strong_count(&payload), if panic { 2 } else { 1 });
    }
}

#[tokio::test(start_paused = true)]
async fn original_started_rx_readonly_quarantine_has_no_new_io_fact() {
    use crate::{harness::{LoopbackTunDevice, PacketLink}, device::{DeviceBinding, TunIo}};
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = session(&meter);
    let deadline = owner.request_stop();
    let mut device = LoopbackTunDevice::native_with_factory(1, 1500, &meter,
        || Ok((PacketLink::bounded(1), PacketLink::bounded(1)))).unwrap();
    device.bind_backing_meter(DeviceBinding::Native { meter: &meter, slots: 1, mtu: 1500 }).unwrap();
    tokio::task::yield_now().await;
    let observer = device.ingress_failure_observer().unwrap();
    meter.quarantine();
    let error = device.wait_for_rx().await.unwrap_err();
    owner.note_loop_error(&error);
    let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
    let report = receipt.settle_callbacks(SessionControl::origin(FailurePhase::DeviceShutdown)).await.unwrap();
    owner.accept_device_report(report);
    if let Some(report) = observer.claim() { owner.accept_device_report(report); }
    assert!(owner.ledger().first_failure.is_none(), "existing owner Q is not a new backend IO");
    assert_eq!(owner.ledger().additional_failures, 0);
    assert!(owner.ledger().quarantined);
    assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
    assert_ne!(receipt.state(), crate::device::DeviceShutdownState::PendingCallbacks);
    drop((error, receipt, observer, device, owner));
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}

#[tokio::test(start_paused = true)]
async fn actual_native_pump_poll_and_source_drop_panic_reach_main_as_original_panic() {
    struct Reader { poll: Arc<usize>, disposal: Arc<usize> }
    impl tokio::io::AsyncRead for Reader {
        fn poll_read(self: std::pin::Pin<&mut Self>, _: &mut Context<'_>, _: &mut tokio::io::ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            std::panic::panic_any(self.poll.clone());
        }
    }
    impl Drop for Reader { fn drop(&mut self) { std::panic::panic_any(self.disposal.clone()); } }
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = session(&meter);
    let deadline = owner.request_stop();
    let poll = Arc::new(89usize);
    let disposal = Arc::new(97usize);
    let mut pump = crate::device::pump_native::Prepared::new(1, &meter).unwrap()
        .spawn_reader(Reader { poll: poll.clone(), disposal: disposal.clone() }, 1500);
    let observer = pump.ingress_failure_observer();
    let error = tokio::time::timeout(std::time::Duration::from_secs(1), pump.recv()).await.unwrap().unwrap().unwrap_err();
    owner.note_loop_error(&error);
    if let Some(report) = observer.claim() { owner.accept_device_report(report); }
    let mut receipt = std::future::poll_fn(|cx| pump.poll_shutdown(cx)).await;
    if let Some(failure) = receipt.take_failure() { owner.record(failure); }
    owner.ledger().additional_failures += receipt.take_additional_failures();
    let ledger = owner.ledger();
    assert_eq!(ledger.first_failure.as_ref().unwrap().facts().class, crate::shared::errors::FailureClass::Panic);
    assert_eq!(ledger.additional_failures, 1, "two original panics, no fabricated IO");
    assert_eq!(ledger.shutdown_deadline, Some(deadline));
    assert!(ledger.quarantined);
    drop(ledger);
    assert_eq!(Arc::strong_count(&poll), 2);
    assert_eq!(Arc::strong_count(&disposal), 2);
    drop((error, receipt, observer, pump));
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}

#[tokio::test(start_paused = true)]
async fn empty_original_io_disposal_after_deadline_does_not_create_a_quarantine() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = session(&meter);
    let deadline = owner.request_stop();
    tokio::time::advance(std::time::Duration::from_secs(16)).await;
    let mut work = Box::pin(owner.retire_device_sources());
    let result = work.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    assert_eq!(result, Poll::Ready(()), "no actual guarded IO remains to await or quarantine");
    drop(work);
    let ledger = owner.ledger();
    assert!(!ledger.device_source_disposal);
    assert!(!ledger.quarantined);
    assert_eq!(ledger.shutdown_deadline, Some(deadline));
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}

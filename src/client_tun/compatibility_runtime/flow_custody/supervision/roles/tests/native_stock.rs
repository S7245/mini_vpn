//! Real cold bill for Source fixtures; preserve their old free headroom and
//! saturating-pressure checks after adding mandatory physical inventory.
use super::*;

pub(in crate::client_tun::compatibility_runtime::flow_custody::supervision::roles)
fn budget_with_old_headroom(old: usize) -> bytes::BackingBudget {
    use crate::shared::errors::{FailureOrder, FailureOrigin, FailurePhase, FailureResource, QuietFailureSink, RetainedFailure};
    struct Facts;
    impl QuietFailureSink for Facts {
        fn record_quiet(&self, _: RetainedFailure) { panic!("cold probe failure"); }
        fn quarantine_quiet(&self) { panic!("cold probe Q"); }
    }
    let probe = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&probe);
    let facts: Arc<dyn QuietFailureSink> = Arc::new(Facts);
    let clock = Arc::new(FailureOrder::default());
    clock.bind(Arc::downgrade(&facts), FailureOrigin { phase: FailurePhase::TcpOpen,
        resource: FailureResource::TcpOpen, resource_id: 0, attempt_id: Some(1) }, Some(meter.clone())).unwrap();
    let legacy = super::super::super::continuous::Shared::try_configured_metered(
        TcpRelayEngine::ContinuousPump, DownlinkBackpressureConfig::default(), Some(meter.clone()),
    ).unwrap();
    assert!(legacy.bind_failures(clock.clone()));
    let before = probe.used();
    drop(legacy);
    let (shared, output) = super::super::super::continuous::Shared::prepare_native(
        TcpRelayEngine::ContinuousPump, DownlinkBackpressureConfig::default(), Some(meter.clone()), clock.clone(),
    ).unwrap();
    let inventory = stock::Inventory::prepare(output.unwrap(), &meter, clock.clone()).unwrap();
    let extra = probe.used().checked_sub(before).unwrap();
    eprintln!("[N1-Source-cold-increment] actual_queue_stock_and_retirement_future_extra={extra} original_headroom={old}");
    drop(inventory);
    drop(shared);
    drop(clock);
    assert_eq!(probe.used(), 0);
    assert!(!probe.quarantined());
    bytes::BackingBudget::new(old.checked_add(extra).unwrap()).unwrap()
}

#[test]
fn actual_source_cold_increment_preserves_fixture_headroom_and_rolls_back() {
    let budget = budget_with_old_headroom(65536);
    assert!(budget.limit() > 65536);
    assert_eq!(budget.used(), 0);
}

#[test]
fn actual_retirement_future_full_rolls_back_prepared_stock_and_producer() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = bound_clock(&meter);
    let prepare = || super::super::super::continuous::Shared::prepare_native(
        TcpRelayEngine::ContinuousPump, DownlinkBackpressureConfig::default(), Some(meter.clone()), clock.clone()).unwrap();
    let (shared, output) = prepare();
    let before = budget.used();
    let inventory = stock::Inventory::prepare(output.unwrap(), &meter, clock.clone()).unwrap();
    let waiting_layout = budget.used().checked_sub(before).unwrap();
    assert!(waiting_layout > 0);
    eprintln!("[N1-actual-retirement-future-layout] bytes={waiting_layout} original_Layout_for_value_and_real_box=true");
    drop((inventory, shared));
    let cold_clock = budget.used();
    let (shared, output) = prepare();
    let fill = meter.reserve_cold_layout(budget.limit() - budget.used() - (waiting_layout - 1)).unwrap();
    let fill_bytes = budget.used() - cold_clock;
    let refused = stock::Inventory::prepare(output.unwrap(), &meter, clock.clone());
    assert!(matches!(refused, Err(bytes::MeteredCopyError::BudgetFull)));
    assert!(!budget.quarantined());
    assert!(budget.used() < cold_clock + fill_bytes, "real stock rollback preceded the typed refusal");
    drop((fill, shared, clock));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

pub(in crate::client_tun::compatibility_runtime::flow_custody::supervision::roles)
fn bound_clock(meter: &bytes::BackingMeter) -> (FailureClock, Arc<dyn crate::shared::errors::QuietFailureSink>) {
    use crate::shared::errors::*;
    struct Facts;
    impl QuietFailureSink for Facts { fn record_quiet(&self, _: RetainedFailure) {} }
    let sink: Arc<dyn QuietFailureSink> = Arc::new(Facts);
    let clock = Arc::new(FailureOrder::default());
    clock.bind(Arc::downgrade(&sink), FailureOrigin { phase: FailurePhase::TcpOpen,
        resource: FailureResource::TcpOpen, resource_id: 0, attempt_id: Some(1) }, Some(meter.clone())).unwrap();
    (clock, sink)
}

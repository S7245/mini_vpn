use super::*;
use crate::shared::errors::*;
use std::sync::Arc;
#[derive(Default)]
struct Facts(std::sync::Mutex<Vec<RetainedFailure>>);
impl QuietFailureSink for Facts {
    fn record_quiet(&self, failure: RetainedFailure) {
        self.0.lock().unwrap().push(failure);
    }
}
fn clock(meter: &bytes::BackingMeter) -> (FailureClock, Arc<Facts>) {
    let facts = Arc::new(Facts::default());
    let owner: Arc<dyn QuietFailureSink> = facts.clone();
    let clock = Arc::new(FailureOrder::default());
    clock
        .bind(
            Arc::downgrade(&owner),
            FailureOrigin {
                phase: FailurePhase::UdpJoin,
                resource: FailureResource::UdpDriver,
                resource_id: 0,
                attempt_id: None,
            },
            Some(meter.clone()),
        )
        .unwrap();
    (clock, facts)
}
#[test]
fn exact_cold_inventory_and_typed_role_capacity() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let baseline = budget.used();
    let stock = Callbacks::prepare(&meter, clock.clone()).unwrap();
    assert_eq!(budget.used() - baseline, Callbacks::layout_bytes());
    let parent = stock.acquire(Role::Parent).unwrap();
    assert!(matches!(
        stock.acquire(Role::Parent),
        Err(CallbackError::Capacity)
    ));
    let mut children = Vec::new();
    for _ in 0..256 {
        children.push(stock.acquire(Role::Child).unwrap());
    }
    assert!(matches!(
        stock.acquire(Role::Child),
        Err(CallbackError::Capacity)
    ));
    let monitor = stock.acquire(Role::Monitor).unwrap();
    let source = stock.acquire(Role::Source).unwrap();
    let recovery = stock.acquire(Role::Recovery).unwrap();
    let shutdown = stock.acquire(Role::Shutdown).unwrap();
    assert!(!stock.quiescent());
    drop((parent, children, monitor, source, recovery, shutdown));
    assert!(stock.quiescent());
    drop((stock, clock));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

mod lifecycle;

mod panics;
mod source;

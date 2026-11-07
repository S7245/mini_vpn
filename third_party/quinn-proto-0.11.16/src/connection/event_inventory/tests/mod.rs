use super::*;
use crate::Instant;
use bytes::{BackingBudget, BackingMeter};

#[test]
fn real_inventory_full_preserves_old_events_and_keeps_control_and_terminal_capacity() {
    let budget = BackingBudget::new(512 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut config = EndpointConfig::default();
    config.receive_backing_meter(meter);
    config.receive_datagram_limit(1200).unwrap();
    let mut inventory = EventInventory::prepare(&config).unwrap();
    let layout = owned_events::slot_layout::<EndpointEventInner>(1217)
        .unwrap()
        .size();
    assert_eq!(budget.used(), layout);
    assert!(inventory.begin(Some(1200)));
    let now = Instant::now();
    for count in 0..1208 {
        inventory.push_back(EndpointEventInner::NeedIdentifiers(now, count));
    }
    inventory.finish();
    assert!(
        !inventory.begin(Some(1200)),
        "old physical event inventory must block a new whole burst"
    );
    assert!(inventory.begin_control(2));
    inventory.push_back(EndpointEventInner::NeedIdentifiers(now, 9000));
    inventory.push_back(EndpointEventInner::Drained);
    inventory.finish();
    for expected in 0..1208 {
        assert_eq!(
            inventory.pop_front(),
            Some(EndpointEventInner::NeedIdentifiers(now, expected))
        );
    }
    assert_eq!(
        inventory.pop_front(),
        Some(EndpointEventInner::NeedIdentifiers(now, 9000))
    );
    assert_eq!(inventory.pop_front(), Some(EndpointEventInner::Drained));
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), layout);
    drop(inventory);
    assert_eq!(budget.used(), 0);
}

#[test]
fn interrupted_operation_keeps_quarantine_while_event_slots_remain_owned() {
    let budget = BackingBudget::new(512 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut config = EndpointConfig::default();
    config.receive_backing_meter(meter.clone());
    config.receive_datagram_limit(1200).unwrap();
    let mut inventory = EventInventory::prepare(&config).unwrap();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert!(inventory.begin(Some(1200)));
        let _guard = inventory.guard();
        inventory.push_back(EndpointEventInner::NeedIdentifiers(Instant::now(), 1));
        panic!("operation interruption stimulus");
    }));
    assert!(failed.is_err());
    assert!(meter.snapshot().quarantined);
    assert!(budget.used() > 0);
    assert!(!inventory.begin(Some(1200)));
    drop(inventory);
    assert!(budget.quarantined());
}

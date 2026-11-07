//! A2 Route 2: protocol-only top tier on one budget identity.
use bytes::{BackingBudget, BackingMeter, BytesMut, MeteredCopyError};

#[test]
fn reserve_must_leave_a_non_empty_app_tier() {
    assert!(BackingBudget::with_protocol_reserve(100, 100).is_none());
    assert!(BackingBudget::with_protocol_reserve(100, 99).is_some());
    assert_eq!(BackingBudget::new(100).unwrap().protocol_reserve(), 0);
}

#[test]
fn app_tier_stops_below_reserve_while_protocol_tier_reaches_limit() {
    let budget = BackingBudget::with_protocol_reserve(1000, 400).unwrap();
    let app = BackingMeter::with_budget(&budget);
    let protocol = BackingMeter::with_protocol_budget(&budget);
    assert!(app.same_budget(&protocol));

    let held = app.reserve_cold_layout(600).unwrap().expect("app tier fits");
    assert_eq!(app.reserve_cold_layout(1).unwrap_err(), MeteredCopyError::BudgetFull);
    assert_eq!(app.budget_headroom(), Ok(Some(0)));
    assert_eq!(protocol.budget_headroom(), Ok(Some(400)));

    let proto = protocol.reserve_cold_layout(400).unwrap().expect("protocol tier fits");
    assert_eq!(
        protocol.reserve_cold_layout(1).unwrap_err(),
        MeteredCopyError::BudgetFull
    );
    assert_eq!(budget.used(), 1000);
    drop(held);
    // Protocol bytes above the app ceiling leave no app headroom, not Q.
    assert_eq!(app.budget_headroom(), Ok(Some(200)));
    drop(proto);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn app_request_larger_than_app_tier_is_too_large_not_full() {
    let budget = BackingBudget::with_protocol_reserve(1000, 400).unwrap();
    let app = BackingMeter::with_budget(&budget);
    assert_eq!(app.reserve_cold_layout(601).unwrap_err(), MeteredCopyError::TooLarge);
    assert!(budget.try_reserve(601).is_none());
    let protocol = BackingMeter::with_protocol_budget(&budget);
    drop(protocol.reserve_cold_layout(601).unwrap());
    assert_eq!(budget.used(), 0);
}

#[test]
fn metered_copies_obey_their_meter_tier() {
    let budget = BackingBudget::with_protocol_reserve(16 * 1024, 8 * 1024).unwrap();
    let app = BackingMeter::with_budget(&budget);
    let protocol = BackingMeter::with_protocol_budget(&budget);
    let data = vec![7u8; 6 * 1024];
    let a = BytesMut::copy_from_slice_metered(&data, &app).expect("app copy fits");
    assert!(BytesMut::copy_from_slice_metered(&data, &app).is_none());
    let p = BytesMut::copy_from_slice_metered(&data, &protocol).expect("protocol copy fits");
    drop((a, p));
    assert_eq!(budget.used(), 0);
}

#[test]
fn app_tier_view_admits_at_app_ceiling_on_the_exact_protocol_authority() {
    let budget = BackingBudget::with_protocol_reserve(16 * 1024, 8 * 1024).unwrap();
    let protocol = BackingMeter::with_protocol_budget(&budget);
    let view = protocol.app_tier_view();
    // Same authority both ways and through a view of the view.
    assert!(view.same_authority(&protocol) && protocol.same_authority(&view));
    assert!(view.app_tier_view().same_authority(&protocol));
    assert!(view.same_budget(&protocol));
    // An App or unbudgeted meter is its own view.
    let app = BackingMeter::with_budget(&budget);
    assert!(app.app_tier_view().same_authority(&app));
    assert!(!app.same_authority(&protocol));

    let data = vec![7u8; 6 * 1024];
    let a = BytesMut::copy_from_slice_metered(&data, &view).expect("app copy fits");
    assert!(BytesMut::copy_from_slice_metered(&data, &view).is_none(), "view stops at App ceiling");
    let used = budget.used();
    assert_eq!(view.budget_headroom(), Ok(Some(8 * 1024 - used)));
    assert_eq!(protocol.budget_headroom(), Ok(Some(16 * 1024 - used)));
    // Counters and Shared-header identity belong to the protocol core.
    assert_eq!(protocol.snapshot(), view.snapshot());
    assert_eq!(protocol.snapshot().allocations, 1);
    assert!(a.precharged_backing_matches(&protocol) && a.precharged_backing_matches(&view));
    let frozen = a.freeze();
    assert!(frozen.backing_meter().unwrap().same_authority(&protocol));
    let p = BytesMut::copy_from_slice_metered(&data, &protocol).expect("protocol tier still fits");
    drop((frozen, p));
    assert_eq!(protocol.snapshot().allocations, 0);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    // Quarantine through a view is the core's sticky Q.
    view.quarantine();
    assert!(protocol.is_quarantined() && protocol.snapshot().quarantined);
    assert!(budget.quarantined());
}

use super::*;
#[test]
fn known_cancellation_destroys_nonempty_original_fragment_stock() {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap() + 4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut fragments = Fragments::prepare(&meter).unwrap();
    fragments
        .accept(frame(&meter, 3, 2, 0, b"cancelled-prefix"), 2)
        .unwrap();
    assert!(budget.used() > Fragments::layout_bytes().unwrap());
    drop(fragments); // Actual nonempty owner cancellation, not a prior clear.
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn delivered_last_reference_outlives_fragment_owner_and_keeps_its_charge() {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap() + 4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut fragments = Fragments::prepare(&meter).unwrap();
    let (_, output) = fragments
        .accept(frame(&meter, 3, 1, 0, b"original-last"), 2)
        .unwrap()
        .unwrap();
    let used = budget.used();
    let last = output.clone();
    let identity = output.allocation_identity();
    drop(fragments);
    assert_eq!(budget.used(), used - Fragments::layout_bytes().unwrap());
    assert!(budget.used() > 0);
    drop(output);
    assert_eq!(last.allocation_identity(), identity);
    assert_eq!(last.as_ref(), b"original-last");
    assert!(last.backing_meter().unwrap().same_authority(&meter));
    drop(last);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

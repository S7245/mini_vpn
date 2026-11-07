use super::*;
use bytes::BackingBudget;

#[test]
fn exact_layout_rejects_before_allocation_and_clear_keeps_cold_storage() {
    let layout = ArrayRangeSet::cold_layout(65).unwrap();
    let denied = BackingBudget::new(layout.size() - 1).unwrap();
    assert!(ArrayRangeSet::cold(&BackingMeter::with_budget(&denied), 65).is_err());
    assert_eq!(denied.used(), 0);
    assert!(!denied.quarantined());
    let budget = BackingBudget::new(layout.size()).unwrap();
    let mut ranges = ArrayRangeSet::cold(&BackingMeter::with_budget(&budget), 65).unwrap();
    assert_eq!(budget.used(), layout.size());
    for pn in (0..130).step_by(2) {
        assert!(ranges.insert_one(pn));
    }
    assert_eq!(ranges.cold_inventory(), (Some(65), 65));
    ranges.pop_min();
    assert!(ranges.insert_one(130));
    assert_eq!(ranges.len(), 65);
    ranges.clear();
    assert!(ranges.is_empty());
    assert_eq!(ranges.cold_inventory(), (Some(65), 65));
    assert_eq!(budget.used(), layout.size());
    assert!(ranges.insert_one(200));
    drop(ranges);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn impossible_extra_range_refuses_without_reallocation_and_retains_q() {
    let layout = ArrayRangeSet::cold_layout(65).unwrap();
    let budget = BackingBudget::new(layout.size()).unwrap();
    let mut ranges = ArrayRangeSet::cold(&BackingMeter::with_budget(&budget), 65).unwrap();
    for pn in (0..130).step_by(2) {
        assert!(ranges.insert_one(pn));
    }
    assert!(!ranges.insert_one(130));
    assert_eq!(ranges.len(), 65);
    assert!(!ranges.contains(130));
    assert_eq!(ranges.cold_inventory(), (Some(65), 65));
    assert!(budget.quarantined());
    drop(ranges);
    assert_eq!(budget.used(), 1);
}

#[test]
fn checked_duplicate_is_noop_but_shared_q_refuses_in_place_extension() {
    let layout = ArrayRangeSet::cold_layout(65).unwrap();
    let budget = BackingBudget::new(layout.size()).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let sibling = BackingMeter::with_budget(&budget);
    let mut ranges = ArrayRangeSet::cold(&meter, 65).unwrap();
    assert!(ranges.try_insert_one(2).unwrap());
    assert!(!ranges.try_insert_one(2).unwrap());
    assert!(!budget.quarantined());
    sibling.quarantine();
    assert!(ranges.try_insert_one(3).is_err());
    assert_eq!(ranges.iter().collect::<Vec<_>>(), [2..3]);
    assert_eq!(ranges.cold_inventory(), (Some(65), 1));
    drop(ranges);
    assert_eq!(budget.used(), 1);
}

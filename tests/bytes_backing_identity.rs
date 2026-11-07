//! Public backing-identity contract for the existing Bytes allocation.
//! The key must survive split/clone/freeze without creating an audit buffer.

use bytes::{Bytes, BytesMut};

#[test]
fn shared_payload_keeps_one_exact_allocation_identity_across_slices() {
    let mut payload = BytesMut::with_capacity(256);
    payload.extend_from_slice(b"abcdefgh");
    let original_capacity = payload.capacity();
    let first = payload.split_to(3).freeze();
    let second = payload.freeze();
    let clone = first.clone();
    let a = first.allocation_identity().expect("first backing");
    assert_eq!(a, second.allocation_identity().expect("second backing"));
    assert_eq!(a, clone.allocation_identity().expect("clone backing"));
    assert_eq!(a.capacity, original_capacity);
    assert_ne!(a.base, 0);
}

#[test]
fn static_and_distinct_backings_do_not_invent_shared_charge() {
    assert!(Bytes::from_static(b"static").allocation_identity().is_none());
    let first = Bytes::from(vec![1u8; 17]);
    let second = Bytes::from(vec![2u8; 17]);
    assert_ne!(first.allocation_identity(), second.allocation_identity());
}

#[test]
fn mutable_precharged_identity_is_read_only_and_survives_empty_split_aliases() {
    let budget = bytes::BackingBudget::new(8192).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let foreign = bytes::BackingMeter::with_budget(&budget);
    let mut original = BytesMut::try_zeroed_metered(128, &meter).unwrap();
    let charged = budget.used();
    assert!(original.precharged_backing_matches(&meter));
    assert!(!original.precharged_backing_matches(&foreign));
    let mut alias = original.split_off(4);
    original.truncate(0);
    assert!(original.precharged_backing_matches(&meter));
    assert!(alias.precharged_backing_matches(&meter));
    let mut raw = BytesMut::with_capacity(128);
    raw.extend_from_slice(b"raw");
    assert!(!raw.precharged_backing_matches(&meter));
    assert!(!BytesMut::new().precharged_backing_matches(&meter));
    assert_eq!(budget.used(), charged);
    assert!(!budget.quarantined(), "readonly mismatches neither charge nor quarantine either authority");
    drop(original);
    assert_eq!(budget.used(), charged);
    alias.truncate(0);
    assert!(alias.precharged_backing_matches(&meter));
    drop(alias);
    assert_eq!(budget.used(), 0);
}

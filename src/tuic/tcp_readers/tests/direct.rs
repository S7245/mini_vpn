//! Direct Native Quinn chunk authority checks at the reader's existing seam.
use super::super::*;

#[test]
fn direct_chunk_foreign_backing_quarantines_both_authorities() {
    let expected = bytes::BackingMeter::new();
    let foreign = bytes::BackingMeter::new();
    let mut backing = bytes::BytesMut::with_capacity(128);
    backing.extend_from_slice(b"foreign");
    let chunk = backing.split_to(7);
    assert!(chunk.charge_shared_backing(&foreign));
    drop(backing);
    let bytes = chunk.freeze();
    assert!(validate_native_direct_chunk(Some(&expected), &bytes).is_err());
    assert!(expected.snapshot().quarantined);
    assert!(foreign.snapshot().quarantined);
    assert_eq!(foreign.snapshot().allocations, 1);
    drop(bytes);
    assert_eq!(foreign.snapshot().allocations, 0);
}

#[test]
fn direct_chunk_unmetered_backing_quarantines_expected_authority() {
    let expected = bytes::BackingMeter::new();
    assert!(
        validate_native_direct_chunk(Some(&expected), &bytes::Bytes::from_static(b"raw")).is_err()
    );
    assert!(expected.snapshot().quarantined);
}

#[test]
fn direct_chunk_same_backing_retains_one_charge_until_last_reference() {
    let expected = bytes::BackingMeter::new();
    let mut backing = bytes::BytesMut::with_capacity(128);
    backing.extend_from_slice(b"accepted");
    let chunk = backing.split_to(8);
    assert!(chunk.charge_shared_backing(&expected));
    drop(backing);
    let bytes = chunk.freeze();
    let clone = bytes.clone();
    assert!(validate_native_direct_chunk(Some(&expected), &bytes).is_ok());
    assert_eq!(expected.snapshot().allocations, 1);
    drop(bytes);
    assert_eq!(expected.snapshot().allocations, 1);
    drop(clone);
    assert_eq!(expected.snapshot().allocations, 0);
    assert!(!expected.snapshot().quarantined);
}

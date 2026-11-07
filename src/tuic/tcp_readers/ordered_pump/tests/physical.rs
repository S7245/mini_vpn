//! The original ordered source must retain one Node charge for a copied batch.
use super::*;
use bytes::{BackingMeter, Bytes, BytesMut};

fn received(meter: &BackingMeter, payload: &[u8]) -> Bytes {
    let mut packet = BytesMut::with_capacity(256);
    packet.extend_from_slice(payload);
    let chunk = packet.split_to(payload.len());
    assert!(chunk.charge_shared_backing(meter));
    drop(packet);
    chunk.freeze()
}

#[test]
fn copied_ordered_batch_retains_the_original_node_backing_authority() {
    let meter = BackingMeter::new();
    let first = received(&meter, b"ab");
    let second = received(&meter, b"cde");
    let mut next = 0;
    let combined =
        combine_ordered_native_chunks(&mut next, &[first.clone(), second.clone()], Some(&meter))
            .unwrap()
            .unwrap();
    assert_eq!(&combined.bytes[..], b"abcde");
    assert_eq!(next, 5);
    assert!(
        combined
            .bytes
            .backing_meter()
            .is_some_and(|actual| meter.same_authority(&actual))
    );
    assert_eq!(meter.snapshot().allocations, 3);
    assert!(meter.snapshot().physical_bytes > meter.snapshot().payload_bytes);
    drop(combined);
    drop(first);
    drop(second);
    assert_eq!(meter.snapshot().allocations, 0);
    assert_eq!(meter.snapshot().physical_bytes, 0);
}

#[test]
fn copied_batch_rejects_foreign_authority_before_offset_commit() {
    let expected = BackingMeter::new();
    let foreign = BackingMeter::new();
    let first = received(&expected, b"a");
    let second = received(&foreign, b"b");
    let mut next = 17;
    assert!(combine_ordered_native_chunks(&mut next, &[first, second], Some(&expected)).is_err());
    assert_eq!(next, 17);
    assert!(expected.snapshot().quarantined);
    assert!(foreign.snapshot().quarantined);
}

#[test]
fn copied_batch_rejects_offset_overflow_without_new_backing() {
    let meter = BackingMeter::new();
    let first = received(&meter, b"a");
    let second = received(&meter, b"b");
    let mut next = u64::MAX;
    assert!(combine_ordered_native_chunks(&mut next, &[first, second], Some(&meter)).is_err());
    assert_eq!(next, u64::MAX);
    assert_eq!(meter.snapshot().allocations, 0);
}

#[test]
fn inline_backing_slices_share_one_charge_until_final_drop() {
    let meter = BackingMeter::new();
    let first = received(&meter, b"abc");
    let second = received(&meter, b"def");
    let combined = combine_ordered_native_chunks(&mut 0, &[first, second], Some(&meter))
        .unwrap()
        .unwrap();
    let identity = combined.bytes.allocation_identity();
    let tail = combined.bytes.slice(3..);
    assert_eq!(tail.allocation_identity(), identity);
    assert_eq!(meter.snapshot().allocations, 1);
    drop(combined);
    assert_eq!(meter.snapshot().allocations, 1);
    drop(tail);
    assert_eq!(meter.snapshot().allocations, 0);
    assert_eq!(meter.snapshot().physical_bytes, 0);
    assert!(!meter.snapshot().quarantined);
}

#[test]
fn inline_backing_vec_escape_is_sticky_quarantine() {
    let meter = BackingMeter::new();
    let first = received(&meter, b"a");
    let second = received(&meter, b"b");
    let combined = combine_ordered_native_chunks(&mut 0, &[first, second], Some(&meter))
        .unwrap()
        .unwrap();
    let copied: Vec<u8> = combined.bytes.into();
    assert_eq!(copied, b"ab");
    assert!(meter.snapshot().quarantined);
}

#[test]
fn inline_backing_mut_escape_is_sticky_quarantine() {
    let meter = BackingMeter::new();
    let first = received(&meter, b"a");
    let second = received(&meter, b"b");
    let combined = combine_ordered_native_chunks(&mut 0, &[first, second], Some(&meter))
        .unwrap()
        .unwrap();
    let copied: BytesMut = combined.bytes.into();
    assert_eq!(&copied[..], b"ab");
    assert!(meter.snapshot().quarantined);
}

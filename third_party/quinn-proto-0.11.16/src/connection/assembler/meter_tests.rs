//! The original Node charge survives a QUIC assembler replacement backing.

use super::*;

fn received(meter: &BackingMeter, data: &[u8]) -> Bytes {
    let mut packet = BytesMut::with_capacity(256);
    packet.extend_from_slice(data);
    let chunk = packet.split_to(data.len());
    assert!(chunk.charge_shared_backing(meter));
    drop(packet);
    chunk.freeze()
}

#[test]
fn defragment_replaces_two_original_backings_with_one_charged_backing() {
    let meter = BackingMeter::new();
    let mut assembler = Assembler::new();
    assembler
        .insert_checked(0, received(&meter, b"abcd"), 256, Some(&meter))
        .unwrap();
    assembler
        .insert_checked(4, received(&meter, b"efgh"), 256, Some(&meter))
        .unwrap();
    assert_eq!(meter.snapshot().allocations, 2);
    assembler.defragment().unwrap();
    let snapshot = meter.snapshot();
    assert_eq!(snapshot.registered_total, 3);
    assert_eq!(snapshot.allocations, 1);
    assert!(!snapshot.quarantined);
    drop(assembler);
    let settled = meter.snapshot();
    assert_eq!((settled.allocations, settled.payload_bytes), (0, 0));
    assert!(!settled.quarantined);
}

#[test]
fn unmetered_nonempty_input_is_rejected_before_assembler_state_changes() {
    let meter = BackingMeter::new();
    let mut assembler = Assembler::new();
    assert!(matches!(
        assembler.insert_checked(0, Bytes::from_static(b"abcd"), 4, Some(&meter)),
        Err(InsertError::AuthorityMismatch)
    ));
    assert_eq!(assembler.progress().buffered_bytes, 0);
    assert_eq!(assembler.progress().highest_received_offset, 0);
    assert!(meter.snapshot().quarantined);
}

#[test]
fn foreign_input_rejects_and_quarantines_both_authorities() {
    let expected = BackingMeter::new();
    let foreign = BackingMeter::new();
    let mut assembler = Assembler::new();
    assert!(matches!(
        assembler.insert_checked(0, received(&foreign, b"abcd"), 256, Some(&expected)),
        Err(InsertError::AuthorityMismatch)
    ));
    assert_eq!(assembler.progress().buffered_bytes, 0);
    assert!(expected.snapshot().quarantined);
    assert!(foreign.snapshot().quarantined);
}

#[test]
fn defragment_rejects_foreign_retained_chunk_before_replacing_originals() {
    let expected = BackingMeter::new();
    let foreign = BackingMeter::new();
    let mut assembler = Assembler::new();
    assembler
        .insert_checked(0, received(&expected, b"abcd"), 256, Some(&expected))
        .unwrap();
    assembler
        .data
        .push(Buffer::new(4, received(&foreign, b"efgh"), 256));
    assembler.buffered += 4;
    assembler.allocated += 256;
    assert!(matches!(
        assembler.defragment(),
        Err(InsertError::AuthorityMismatch)
    ));
    assert_eq!(assembler.data.len(), 2);
    assert!(expected.snapshot().quarantined);
    assert!(foreign.snapshot().quarantined);
}

#[test]
fn replacement_charge_failure_cannot_publish_unmetered_bytes() {
    let expected = BackingMeter::new();
    let foreign = BackingMeter::new();
    let mut buffer = BytesMut::with_capacity(256);
    buffer.extend_from_slice(b"abcd");
    let mut split = buffer.split_to(4);
    assert!(split.charge_shared_backing(&foreign));
    drop(buffer);
    assert!(matches!(
        freeze_metered(&mut split, Some(&expected)),
        Err(InsertError::AuthorityMismatch)
    ));
    assert!(expected.snapshot().quarantined);
    assert!(foreign.snapshot().quarantined);
}

#[test]
fn second_replacement_charge_failure_preserves_all_original_chunks_and_counters() {
    let meter = BackingMeter::new();
    let mut assembler = Assembler::new();
    for (offset, data) in [
        (0, b"aaaa".as_slice()),
        (8, b"bbbb".as_slice()),
        (16, b"cccc".as_slice()),
    ] {
        assembler
            .insert_checked(offset, received(&meter, data), 256, Some(&meter))
            .unwrap();
    }
    let before = assembler.progress();
    let mut attempts = 0;
    assert!(matches!(
        assembler.defragment_with(|buffer, authority| {
            attempts += 1;
            if attempts == 2 {
                authority.unwrap().quarantine();
                return Err(InsertError::AuthorityMismatch);
            }
            freeze_metered(buffer, authority)
        }),
        Err(InsertError::AuthorityMismatch)
    ));
    assert_eq!(attempts, 2);
    assert_eq!(assembler.progress(), before);
    assert_eq!(assembler.data.len(), 3);
    assert_eq!(meter.snapshot().allocations, 3);
    assert!(meter.snapshot().quarantined);
    assert_eq!(
        assembler.read(4, true).unwrap().bytes,
        Bytes::from_static(b"aaaa")
    );
}

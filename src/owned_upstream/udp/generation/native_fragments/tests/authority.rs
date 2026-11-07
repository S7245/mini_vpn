use super::*;
#[derive(Clone, Copy, Debug)]
enum Mutation {
    Complete,
    Replace,
    Resize,
    Oversize,
    Evict,
}
fn rejects_existing_foreign(mutation: Mutation) {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap() + (1 << 20)).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let foreign = BackingMeter::with_budget(&budget);
    let mut fragments = Fragments::prepare(&meter).unwrap();
    fragments.accept(frame(&meter, 3, 2, 0, b"old"), 0).unwrap();
    let slot = fragments.root.find((7, 3)).unwrap().unwrap();
    let replacement = Bytes::copy_from_slice_metered(b"bad", &foreign).unwrap();
    let identity = replacement.allocation_identity();
    let original = fragments.root.slots[slot].fragments[0]
        .replace(replacement)
        .unwrap();
    if matches!(mutation, Mutation::Evict) {
        for packet in 0..CAPACITY as u16 {
            if packet != 3 {
                fragments
                    .accept(frame(&meter, packet, 2, 0, b"x"), 1)
                    .unwrap();
            }
        }
    }
    let before_len = fragments.root.len;
    let input = match mutation {
        Mutation::Complete => frame(&meter, 3, 2, 1, b"end"),
        Mutation::Replace => frame(&meter, 3, 2, 0, b"new"),
        Mutation::Resize => frame(&meter, 3, 3, 0, b"new"),
        Mutation::Oversize => frame(&meter, 3, 2, 1, &vec![0; MAX_IPV4_UDP_PAYLOAD_BYTES + 1]),
        Mutation::Evict => frame(&meter, 300, 2, 0, b"new"),
    };
    let input_identity = input.allocation_identity();
    let rejected = fragments.accept(input, 9).unwrap_err();
    assert_eq!(
        rejected.error,
        MeteredCopyError::Quarantined,
        "{mutation:?}"
    );
    assert_eq!(rejected.frame.allocation_identity(), input_identity);
    assert_eq!(fragments.root.len, before_len);
    let partial = &fragments.root.slots[slot];
    assert_eq!(
        partial.fragments[0].as_ref().unwrap().allocation_identity(),
        identity
    );
    assert!(
        partial.fragments[0]
            .as_ref()
            .unwrap()
            .backing_meter()
            .unwrap()
            .same_authority(&foreign)
    );
    assert_eq!(
        (
            partial.total,
            partial.received,
            partial.bytes,
            partial.first_seen
        ),
        (2, 1, 3, 0)
    );
    drop((original, rejected, fragments));
    assert!(budget.quarantined());
    assert!(budget.used() > 1);
}
#[test]
fn completion_cannot_copy_and_hide_foreign_original_descriptor() {
    rejects_existing_foreign(Mutation::Complete);
}
#[test]
fn replacement_cannot_erase_foreign_original_descriptor() {
    rejects_existing_foreign(Mutation::Replace);
}
#[test]
fn total_change_cannot_erase_foreign_original_descriptor() {
    rejects_existing_foreign(Mutation::Resize);
}
#[test]
fn oversized_input_cannot_erase_foreign_original_descriptor() {
    rejects_existing_foreign(Mutation::Oversize);
}
#[test]
fn lru_eviction_cannot_erase_foreign_original_descriptor() {
    rejects_existing_foreign(Mutation::Evict);
}
#[test]
fn empty_wire_has_no_backing_and_is_ignored_without_quarantine() {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap()).unwrap();
    let mut fragments = Fragments::prepare(&BackingMeter::with_budget(&budget)).unwrap();
    assert!(fragments.accept(Bytes::new(), 0).unwrap().is_none());
    drop(fragments);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

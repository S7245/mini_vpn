use super::*;
fn frame(meter: &BackingMeter, packet: u16, total: u8, id: u8, data: &[u8]) -> Bytes {
    let mut wire = vec![5, 2, 0, 7];
    wire.extend_from_slice(&packet.to_be_bytes());
    wire.extend_from_slice(&[total, id]);
    wire.extend_from_slice(&(data.len() as u16).to_be_bytes());
    wire.push(0xff); // Original TUIC ATYP_NONE.
    wire.extend_from_slice(data);
    Bytes::copy_from_slice_metered(&wire, meter).unwrap()
}
#[test]
fn cold_exact_stock_and_original_single_frame_identity() {
    let size = Fragments::layout_bytes().unwrap();
    let short = bytes::BackingBudget::new(size - 1).unwrap();
    assert!(Fragments::prepare(&BackingMeter::with_budget(&short)).is_err());
    assert_eq!(short.used(), 0);
    assert!(!short.quarantined());
    let budget = bytes::BackingBudget::new(size + 4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut fragments = Fragments::prepare(&meter).unwrap();
    assert_eq!(budget.used(), size);
    let input = frame(&meter, 3, 1, 0, b"original");
    let identity = input.allocation_identity();
    let (assoc, output) = fragments.accept(input, 0).unwrap().unwrap();
    assert_eq!(assoc, 7);
    assert_eq!(output.as_ref(), b"original");
    assert_eq!(output.allocation_identity(), identity);
    assert!(output.backing_meter().unwrap().same_authority(&meter));
    drop(output);
    assert!(fragments.retire_generation());
    drop(fragments);
    assert_eq!(budget.used(), 0);
}
#[test]
fn completing_copy_pressure_keeps_original_partial_clock_and_frame() {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap() + 4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut fragments = Fragments::prepare(&meter).unwrap();
    assert!(
        fragments
            .accept(frame(&meter, 3, 2, 0, b"old"), 2)
            .unwrap()
            .is_none()
    );
    let tail = frame(&meter, 3, 2, 1, b"tail");
    let identity = tail.allocation_identity();
    let filler = meter
        .reserve_cold_layout(budget.limit() - budget.used())
        .unwrap();
    let rejected = fragments.accept(tail, 9).unwrap_err();
    assert_eq!(rejected.error, MeteredCopyError::BudgetFull);
    assert_eq!(rejected.frame.allocation_identity(), identity);
    let slot = fragments.root.find((7, 3)).unwrap().unwrap();
    assert_eq!(fragments.root.slots[slot].first_seen, 2);
    assert_eq!(fragments.root.slots[slot].received, 1);
    assert_eq!(fragments.root.slots[slot].bytes, 3);
    assert!(!budget.quarantined());
    drop(filler);
    let (_, output) = fragments.accept(rejected.frame, 10).unwrap().unwrap();
    assert_eq!(output.as_ref(), b"oldtail");
    assert!(output.backing_meter().unwrap().same_authority(&meter));
    drop(output);
    assert!(fragments.retire_generation());
    drop(fragments);
    assert_eq!(budget.used(), 0);
}
#[test]
fn duplicate_resize_lru_and_ttl_keep_original_semantics() {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap() + (1 << 20)).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut fragments = Fragments::prepare(&meter).unwrap();
    assert!(
        fragments
            .accept(frame(&meter, 3, 3, 0, b"old"), 1)
            .unwrap()
            .is_none()
    );
    assert!(
        fragments
            .accept(frame(&meter, 3, 3, 0, b"new"), 2)
            .unwrap()
            .is_none()
    );
    assert!(
        fragments
            .accept(frame(&meter, 3, 2, 0, b"resize"), 3)
            .unwrap()
            .is_none()
    );
    let (_, output) = fragments
        .accept(frame(&meter, 3, 2, 1, b"tail"), 4)
        .unwrap()
        .unwrap();
    assert_eq!(output.as_ref(), b"resizetail");
    drop(output);
    for key in 0..CAPACITY as u16 {
        assert!(
            fragments
                .accept(frame(&meter, key, 2, 0, b"a"), 10)
                .unwrap()
                .is_none()
        );
    }
    assert!(
        fragments
            .accept(frame(&meter, 300, 2, 0, b"b"), 10)
            .unwrap()
            .is_none()
    );
    assert!(fragments.root.find((7, 0)).unwrap().is_none());
    assert!(fragments.root.find((7, 1)).unwrap().is_some());
    assert!(fragments.sweep(20, 10));
    assert_eq!(fragments.root.len, CAPACITY);
    assert!(fragments.sweep(21, 10));
    assert_eq!(fragments.root.len, 0);
    drop(fragments);
    assert_eq!(budget.used(), 0);
}
#[test]
fn control_inventory_mismatch_retains_actual_payload_and_metadata_under_q() {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap() + 4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut fragments = Fragments::prepare(&meter).unwrap();
    fragments
        .accept(frame(&meter, 3, 2, 0, b"retained"), 0)
        .unwrap();
    let slot = fragments.root.find((7, 3)).unwrap().unwrap();
    fragments.root.slots[slot].bytes += 1;
    let before = budget.used();
    assert!(!fragments.retire_generation());
    drop(fragments);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), before);
}

#[test]
fn resize_rejects_duplicate_physical_slot_before_removing_old_partial() {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap() + 4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut fragments = Fragments::prepare(&meter).unwrap();
    fragments
        .accept(frame(&meter, 3, 2, 0, b"first"), 2)
        .unwrap();
    fragments
        .accept(frame(&meter, 4, 2, 0, b"original"), 3)
        .unwrap();
    let slot = fragments.root.find((7, 4)).unwrap().unwrap();
    let identity = fragments.root.slots[slot].fragments[0]
        .as_ref()
        .unwrap()
        .allocation_identity();
    fragments.root.index[0].slot = slot; // Duplicated physical slot, still two keys.
    let rejected = fragments
        .accept(frame(&meter, 4, 3, 0, b"replacement"), 9)
        .unwrap_err();
    assert_eq!(rejected.error, MeteredCopyError::Quarantined);
    assert_eq!(
        fragments.root.len, 2,
        "invalid transaction must not partially remove an index entry"
    );
    assert_eq!(
        fragments.root.slots[slot].fragments[0]
            .as_ref()
            .map(Bytes::allocation_identity),
        Some(identity)
    );
    assert_eq!(fragments.root.slots[slot].first_seen, 3);
    assert_eq!(fragments.root.slots[slot].bytes, 8);
    drop((rejected, fragments));
    assert!(budget.quarantined());
    assert!(budget.used() > 1);
}

#[test]
fn empty_completion_still_checks_every_original_descriptor() {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap() + 4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut fragments = Fragments::prepare(&meter).unwrap();
    fragments.accept(frame(&meter, 3, 3, 0, b""), 2).unwrap();
    let slot = fragments.root.find((7, 3)).unwrap().unwrap();
    fragments.root.slots[slot].received = 2; // Missing physical fragment 1.
    let rejected = fragments
        .accept(frame(&meter, 3, 3, 2, b""), 9)
        .unwrap_err();
    assert_eq!(rejected.error, MeteredCopyError::Quarantined);
    assert_eq!(fragments.root.len, 1);
    assert!(fragments.root.slots[slot].fragments[0].is_some());
    assert!(fragments.root.slots[slot].fragments[1].is_none());
    assert_eq!(fragments.root.slots[slot].first_seen, 2);
    drop((rejected, fragments));
    assert!(budget.quarantined());
    assert!(budget.used() > 1);
}

#[test]
fn legal_empty_completion_and_original_reassembler_differential() {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap() + (1 << 20)).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut native = Fragments::prepare(&meter).unwrap();
    let mut legacy = crate::tuic::FragReassembler::with_cap(CAPACITY);
    for id in 0..3 {
        let wire = frame(&meter, 3, 3, id, b"");
        let expected = legacy.accept(&decode_packet_meta(&wire).unwrap(), 1);
        let actual = native.accept(wire, 1).unwrap();
        assert_eq!(actual.map(|(_, bytes)| bytes.to_vec()), expected);
    }
    let mut seed = 0x7724_0fea_88a1_3984u64;
    for step in 0..2000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let packet = (seed % 300) as u16;
        let total = ((seed >> 12) % 5) as u8;
        let id = ((seed >> 20) % 5) as u8;
        let data = vec![(seed >> 40) as u8; ((seed >> 32) % 16) as usize];
        let now = step / 20;
        let wire = frame(&meter, packet, total, id, &data);
        let expected = legacy.accept(&decode_packet_meta(&wire).unwrap(), now);
        let actual = native.accept(wire, now).unwrap();
        if let Some((_, payload)) = &actual {
            assert!(payload.is_empty() || payload.backing_meter().unwrap().same_authority(&meter));
        }
        assert_eq!(
            actual.map(|(_, bytes)| bytes.to_vec()),
            expected,
            "original semantics at {step}"
        );
        if step % 17 == 0 {
            legacy.sweep(now, 10);
            assert!(native.sweep(now, 10));
        }
    }
    assert!(native.retire_generation());
    assert!(legacy.retire_generation());
    drop(native);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn whole_packet_limit_and_other_authority_have_explicit_dispositions() {
    let budget = bytes::BackingBudget::new(Fragments::layout_bytes().unwrap() + (1 << 20)).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut fragments = Fragments::prepare(&meter).unwrap();
    fragments
        .accept(frame(&meter, 3, 2, 0, b"unrelated"), 2)
        .unwrap();
    let large = vec![0x57; MAX_IPV4_UDP_PAYLOAD_BYTES];
    assert!(
        fragments
            .accept(frame(&meter, 4, 2, 0, &large), 3)
            .unwrap()
            .is_none()
    );
    assert!(
        fragments
            .accept(frame(&meter, 4, 2, 1, b"x"), 4)
            .unwrap()
            .is_none()
    );
    assert!(fragments.root.find((7, 4)).unwrap().is_none());
    assert!(fragments.root.find((7, 3)).unwrap().is_some());
    let (_, output) = fragments
        .accept(frame(&meter, 3, 2, 1, b"tail"), 5)
        .unwrap()
        .unwrap();
    assert_eq!(output.as_ref(), b"unrelatedtail");
    drop(output);
    let (_, output) = fragments
        .accept(frame(&meter, 5, 1, 0, &large), 6)
        .unwrap()
        .unwrap();
    assert_eq!(output.len(), MAX_IPV4_UDP_PAYLOAD_BYTES);
    drop(output);
    assert!(fragments.retire_generation());
    drop(fragments);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    let mut fragments = Fragments::prepare(&meter).unwrap();
    let foreign = BackingMeter::with_budget(&budget); // Same budget is not the original Endpoint authority.
    let input = frame(&foreign, 6, 1, 0, b"foreign");
    let identity = input.allocation_identity();
    let rejected = fragments.accept(input, 7).unwrap_err();
    assert_eq!(rejected.error, MeteredCopyError::Quarantined);
    assert_eq!(rejected.frame.allocation_identity(), identity);
    assert_eq!(fragments.root.len, 0);
    drop((rejected, fragments));
    assert!(budget.quarantined());
    assert!(budget.used() > 1);
}

mod retirement;

mod authority;

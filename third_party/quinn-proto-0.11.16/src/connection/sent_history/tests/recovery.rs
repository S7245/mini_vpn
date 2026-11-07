use super::*;

#[test]
fn known_close_retires_only_its_selection_and_keeps_actual_packets_and_other_kind() {
    let (mut space, budget) = space_with_records();
    let baseline = budget.used();
    let mut ack = space.sent_packets.begin_recovery(RecoveryKind::Ack).unwrap();
    space.sent_packets.select_recovery(&mut ack, ..).unwrap();
    ack.begin_commit().unwrap();
    drop(space.take_recovery(&mut ack, 0).unwrap().unwrap());
    let mut loss = space.sent_packets.begin_recovery(RecoveryKind::Loss).unwrap();
    space.sent_packets.select_recovery(&mut loss, 1..=2).unwrap();
    loss.begin_commit().unwrap();
    let physical = |space: &PacketSpace| space.sent_packets.iter()
        .map(|(&pn, info)| (pn, info as *const SentPacket as usize, info.size)).collect::<Vec<_>>();
    let before = physical(&space);
    space.sent_packets.close_recovery(ack, &crate::connection::State::Draining).unwrap();
    assert_eq!(physical(&space), before);
    assert_eq!(budget.used(), baseline);
    assert_eq!(space.sent_packets.iter().map(|(&pn, p)| (pn, p.recovery_selection)).collect::<Vec<_>>(),
        [(1, 2), (2, 2), (3, 0), (4, 0)]);
    space.sent_packets.close_recovery(loss, &crate::connection::State::Draining).unwrap();
    assert_eq!(physical(&space), before);
    assert!(space.sent_packets.iter().all(|(_, p)| p.recovery_selection == 0));
    assert!(!budget.quarantined());
    drop(space);
    assert_eq!(budget.used(), 0);
}

#[test]
fn pn_zero_and_sparse_selection_preserve_merged_ecn_range_count() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let mut space = PacketSpace::new(Instant::now());
    space.sent_packets = History::cold(&BackingMeter::with_budget(&budget), 5, 2).unwrap();
    for pn in [0, 2, 7, 8, 19] {
        let mut permit = space.sent_packets.reserve(PacketClass::Control).unwrap();
        permit.begin_encoding().unwrap();
        assert!(space
            .sent_packets
            .insert_prepared(pn, packet(), permit)
            .is_ok());
    }
    let mut selected = space
        .sent_packets
        .begin_recovery(RecoveryKind::Ack)
        .unwrap();
    space
        .sent_packets
        .select_recovery(&mut selected, 0..=0)
        .unwrap();
    space
        .sent_packets
        .select_recovery(&mut selected, 7..=19)
        .unwrap();
    assert_eq!(
        space.sent_packets.recovery_ranges(&selected).unwrap(),
        3,
        "ECN uses ranges {{0}}, {{7,8}}, {{19}}, not four packets"
    );
    selected.begin_commit().unwrap();
    let mut cursor = 0;
    let mut actual = Vec::new();
    while let Some((pn, _)) = space.take_recovery(&mut selected, cursor).unwrap() {
        actual.push(pn);
        cursor = pn + 1;
    }
    selected.finish().unwrap();
    assert_eq!(actual, [0, 7, 8, 19]);
    assert_eq!(
        space
            .sent_packets
            .iter()
            .map(|(&pn, _)| pn)
            .collect::<Vec<_>>(),
        [2]
    );
    drop(space);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

fn space_with_records() -> (PacketSpace, BackingBudget) {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let mut space = PacketSpace::new(Instant::now());
    space.sent_packets = History::cold(&BackingMeter::with_budget(&budget), 3, 2).unwrap();
    for pn in 0..5 {
        let mut permit = space.sent_packets.reserve(PacketClass::Control).unwrap();
        permit.begin_encoding().unwrap();
        assert!(space
            .sent_packets
            .insert_prepared(pn, packet(), permit)
            .is_ok());
    }
    (space, budget)
}
#[test]
fn prepared_cancel_preserves_full_stock_then_ack_and_loss_can_nest() {
    let (mut space, budget) = space_with_records();
    let baseline = budget.used();
    let mut cancelled = space
        .sent_packets
        .begin_recovery(RecoveryKind::Ack)
        .unwrap();
    space
        .sent_packets
        .select_recovery(&mut cancelled, 0..=4)
        .unwrap();
    space.sent_packets.cancel_recovery(cancelled).unwrap();
    assert_eq!(space.sent_packets.len(), 5);
    assert_eq!(budget.used(), baseline);
    assert!(!space.sent_packets.ordinary_ready());
    assert!(!budget.quarantined());
    let mut ack = space
        .sent_packets
        .begin_recovery(RecoveryKind::Ack)
        .unwrap();
    space.sent_packets.select_recovery(&mut ack, 0..=0).unwrap();
    ack.begin_commit().unwrap();
    assert_eq!(space.take_recovery(&mut ack, 0).unwrap().unwrap().0, 0);
    let mut loss = space
        .sent_packets
        .begin_recovery(RecoveryKind::Loss)
        .unwrap();
    space
        .sent_packets
        .select_recovery(&mut loss, 1..=4)
        .unwrap();
    loss.begin_commit().unwrap();
    for cursor in 1..=4 {
        assert_eq!(
            space.take_recovery(&mut loss, cursor).unwrap().unwrap().0,
            cursor
        );
    }
    assert!(space.take_recovery(&mut loss, 5).unwrap().is_none());
    loss.finish().unwrap();
    ack.finish().unwrap();
    assert_eq!(space.sent_packets.occupied(), 0);
    assert!(space.sent_packets.ordinary_ready());
    assert_eq!(
        budget.used(),
        baseline,
        "record disposal does not destroy fixed layout"
    );
    drop(space);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn interrupted_recovery_after_one_actual_take_keeps_unknown_charge() {
    let (mut space, budget) = space_with_records();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut selected = space
            .sent_packets
            .begin_recovery(RecoveryKind::Ack)
            .unwrap();
        space
            .sent_packets
            .select_recovery(&mut selected, 0..=4)
            .unwrap();
        selected.begin_commit().unwrap();
        let first = space.take_recovery(&mut selected, 0).unwrap().unwrap();
        assert_eq!(first.0, 0);
        drop(first);
        panic!("actual recovery callback interruption stimulus");
    }));
    assert!(result.is_err());
    assert!(budget.quarantined());
    assert_eq!(space.sent_packets.len(), 4);
    assert!(space
        .sent_packets
        .begin_recovery(RecoveryKind::Ack)
        .is_err());
    drop(space);
    assert_eq!(budget.used(), 1);
}
#[test]
fn foreign_selection_refuses_before_any_record_mark_or_take() {
    let (owner, first_budget) = space_with_records();
    let (mut foreign, second_budget) = space_with_records();
    let mut selected = owner
        .sent_packets
        .begin_recovery(RecoveryKind::Ack)
        .unwrap();
    assert!(foreign
        .sent_packets
        .select_recovery(&mut selected, 0..=4)
        .is_err());
    assert_eq!(owner.sent_packets.len(), 5);
    assert_eq!(foreign.sent_packets.len(), 5);
    assert!(first_budget.quarantined());
    assert!(second_budget.quarantined());
    drop(selected);
    drop(owner);
    drop(foreign);
    assert_eq!(first_budget.used(), 1);
    assert_eq!(second_budget.used(), 1);
}

#[test]
fn projection_cancels_exact_marks_at_full_without_allocating_or_moving_packets() {
    let (mut space, budget) = space_with_records();
    let before: Vec<_> = space.sent_packets.iter().map(|(&pn, p)| (pn, p as *const _ as usize)).collect();
    let full = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert_eq!(crate::tests::allocations_in(|| {
        let mut projected = space.sent_packets.project_recovery().unwrap();
        projected.mark_where(1..=3, |_, _| true);
        assert!(projected.get(2).is_none());
        assert!(projected.get(4).is_some());
        projected.mark_where(.., |_, _| true);
    }), 0);
    assert!(space.sent_packets.iter().all(|(_, p)| p.recovery_selection == 0));
    assert_eq!(space.sent_packets.iter().map(|(&pn, p)| (pn, p as *const _ as usize)).collect::<Vec<_>>(), before);
    let selection = space.sent_packets.begin_recovery(RecoveryKind::Ack).unwrap();
    space.sent_packets.cancel_recovery(selection).unwrap();
    assert!(!budget.quarantined());
    drop((full, space));
    assert_eq!(budget.used(), 0);
}
#[test]
fn projection_unwind_restores_marks_but_retains_original_quarantine() {
    let (mut space, budget) = space_with_records();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut projected = space.sent_packets.project_recovery().unwrap();
        projected.mark_where(.., |_, _| true);
        panic!("interrupt unpublished recovery projection");
    }));
    assert!(result.is_err());
    assert!(space.sent_packets.iter().all(|(_, p)| p.recovery_selection == 0));
    assert!(budget.quarantined());
    drop(space);
    assert!(budget.used() > 0);
}

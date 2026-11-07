use super::*;
use bytes::{BackingBudget, BackingMeter};

fn packet() -> SentPacket {
    SentPacket {
        recovery_selection: 0,
        path_generation: 0,
        time_sent: Instant::now(),
        size: 30,
        ack_eliciting: true,
        successor_service_turn: false,
        transport_write_demand: false,
        largest_acked: None,
        retransmits: ThinRetransmits::default(),
        stream_frames: Default::default(),
    }
}

#[test]
fn ring_preserves_skipped_order_holes_and_reserved_custody() {
    let layout = History::layout(5).unwrap();
    let denied = BackingBudget::new(layout.size() - 1).unwrap();
    assert!(History::cold(&BackingMeter::with_budget(&denied), 3, 2).is_err());
    assert_eq!(denied.used(), 0);
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let mut history = History::cold(&BackingMeter::with_budget(&budget), 3, 2).unwrap();
    for pn in [2, 7, 19] {
        let mut permit = history.reserve(PacketClass::Control).unwrap();
        permit.begin_encoding().unwrap();
        assert!(history.insert_prepared(pn, packet(), permit).is_ok());
    }
    assert!(!history.ordinary_ready());
    assert_eq!(
        history.range(3..20).map(|(&pn, _)| pn).collect::<Vec<_>>(),
        [7, 19]
    );
    drop(history.remove(&7));
    assert_eq!(history.len(), 2);
    assert_eq!(history.occupied(), 3, "interior tombstone remains charged");
    let held = history.reserve(PacketClass::Control).unwrap();
    let held2 = history.reserve(PacketClass::Control).unwrap();
    assert!(matches!(
        history.reserve(PacketClass::Control),
        Err(ReserveError::Full)
    ));
    drop(held);
    drop(held2);
    assert_eq!(history.occupied(), 3);
    drop(history.remove(&2));
    assert_eq!(history.occupied(), 1, "head advancement reclaims the hole");
    assert!(history.ordinary_ready());
    for pn in [25, 80, 90, 100] {
        let mut permit = history.reserve(PacketClass::Control).unwrap();
        permit.begin_encoding().unwrap();
        assert!(history.insert_prepared(pn, packet(), permit).is_ok());
    }
    assert_eq!(
        history.iter().map(|(&pn, _)| pn).collect::<Vec<_>>(),
        [19, 25, 80, 90, 100]
    );
    drop(history.remove(&90));
    assert_eq!(history.occupied(), 5);
    assert_eq!(
        history
            .range(20..100)
            .map(|(&pn, _)| pn)
            .collect::<Vec<_>>(),
        [25, 80]
    );
    let mut drained = Vec::new();
    while let Some((&pn, _)) = history.iter().next() {
        drained.push(pn);
        assert!(history.drain_next().is_some());
    }
    assert_eq!(drained, [19, 25, 80, 100]);
    assert_eq!(history.occupied(), 0);
    assert!(
        budget.used() >= layout.size(),
        "cold layout lives through the empty state"
    );
    assert!(!budget.quarantined());
    drop(history);
    assert_eq!(budget.used(), 0);
}

#[test]
fn ordinary_reservations_cannot_consume_control_credit() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let history = History::cold(&BackingMeter::with_budget(&budget), 2, 2).unwrap();
    let first = history.reserve(PacketClass::Ordinary).unwrap();
    let second = history.reserve(PacketClass::Ordinary).unwrap();
    assert!(matches!(
        history.reserve(PacketClass::Ordinary),
        Err(ReserveError::Full)
    ));
    assert!(first.allows_data());
    let control = history.reserve(PacketClass::Control).unwrap();
    assert!(!control.allows_data());
    let control2 = history.reserve(PacketClass::Control).unwrap();
    assert!(matches!(
        history.reserve(PacketClass::Control),
        Err(ReserveError::Full)
    ));
    drop((first, second, control, control2));
    assert_eq!(history.occupied(), 0);
    drop(history);
    assert_eq!(budget.used(), 0);
}

#[test]
fn shared_quarantine_fences_reserved_encoding_and_record_commit() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let sibling = BackingMeter::with_budget(&budget);
    let mut history = History::cold(&meter, 2, 2).unwrap();
    let mut untouched = history.reserve(PacketClass::Ordinary).unwrap();
    let mut encoding = history.reserve(PacketClass::Control).unwrap();
    encoding.begin_encoding().unwrap();
    sibling.quarantine();
    assert!(matches!(
        untouched.begin_encoding(),
        Err(ReserveError::Quarantined)
    ));
    assert!(matches!(
        history.reserve(PacketClass::Control),
        Err(ReserveError::Quarantined)
    ));
    assert!(history.insert_prepared(1, packet(), encoding).is_err());
    drop(untouched);
    assert_eq!(history.len(), 0);
    assert_eq!(
        history.occupied(),
        1,
        "dirty interrupted preparation is not refunded"
    );
    drop(history);
    assert_eq!(budget.used(), 1);
}

#[test]
fn foreign_space_permit_cannot_commit_or_turn_quarantine_clean() {
    let left = BackingBudget::new(64 * 1024).unwrap();
    let right = BackingBudget::new(64 * 1024).unwrap();
    let mut a = History::cold(&BackingMeter::with_budget(&left), 2, 2).unwrap();
    let b = History::cold(&BackingMeter::with_budget(&right), 2, 2).unwrap();
    let mut token = b.reserve(PacketClass::Control).unwrap();
    token.begin_encoding().unwrap();
    assert!(a.insert_prepared(2, packet(), token).is_err());
    assert_eq!(a.len(), 0);
    assert!(left.quarantined() && right.quarantined());
    drop((a, b));
    assert_eq!(left.used(), 1);
    assert_eq!(right.used(), 1);
}

#[test]
fn last_untouched_permit_keeps_layout_until_actual_cancellation() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let history = History::cold(&BackingMeter::with_budget(&budget), 2, 2).unwrap();
    let layout = History::layout(4).unwrap().size();
    let held = history.reserve(PacketClass::Control).unwrap();
    drop(history);
    assert_eq!(budget.used(), layout);
    assert!(!budget.quarantined());
    drop(held);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn encoding_unwind_and_owner_shutdown_retain_unknown_charge() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let history = History::cold(&BackingMeter::with_budget(&budget), 2, 2).unwrap();
    let mut held = history.reserve(PacketClass::Control).unwrap();
    held.begin_encoding().unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _custody = held;
        panic!("sent permit interruption stimulus");
    }));
    assert!(result.is_err());
    assert!(budget.quarantined());
    assert_eq!(
        history.occupied(),
        1,
        "uncertain encoded record cannot cancel"
    );
    drop(history);
    assert_eq!(
        budget.used(),
        1,
        "only destroyed known layout refunds; unknown remains"
    );
    assert!(budget.quarantined());
}

mod recovery;
#[cfg(feature = "sent-history-observe")]
mod observe;

//! Finite observer compression only; real socket/capability tests remain upstream.
use super::super::*;

fn native_ledger() -> EgressPermitLedger {
    let meter = bytes::BackingMeter::with_budget(&bytes::BackingBudget::new(128 * 1024).unwrap());
    let (mut pending, mut caps, mut ledger) = (PendingBytes::default(), PermitStock::default(), EgressPermitLedger::default());
    storage::prepare(&mut pending, &mut caps, &mut ledger, &meter).unwrap();
    ledger
}

fn extension(start: u64, end: u64, before: usize, after: usize) -> EgressLedgerEvent {
    EgressLedgerEvent { kind: EgressLedgerEventKind::Admit, source: None, epoch: 7,
        start, end, cap_ordinal: 2, cap_before: before, cap_after: after }
}

#[test]
fn event_runs_preserve_creation_and_exact_running_bounds() {
    let mut ledger = native_ledger();
    ledger.record_event(extension(0, 1, 1, 1)); // Actual creation is separate.
    for n in 1..100 { ledger.record_event(extension(n, n + 1, n as usize, n as usize + 1)); }
    assert!(!ledger.event_overflow);
    assert_eq!(ledger.event_count, 2);
    assert_eq!(ledger.events[0], Some(extension(0, 1, 1, 1)));
    assert_eq!(ledger.events[1], Some(extension(1, 100, 1, 100)));
    for n in 0..100 {
        let mut event = extension(n, n + 1, 100 - n as usize, 99 - n as usize);
        event.kind = EgressLedgerEventKind::Refund;
        event.source = Some(EgressRefundSource::PacketFlush);
        ledger.record_event(event);
    }
    assert_eq!(ledger.event_count, 3);
    let refund = ledger.events[2].unwrap();
    assert_eq!((refund.start, refund.end, refund.cap_before, refund.cap_after), (0, 100, 100, 0));
    assert!(!ledger.event_overflow);
}

#[test]
fn event_runs_never_hide_gap_overlap_wrong_source_or_false_bytes() {
    let first = extension(1, 2, 1, 2);
    let next = extension(2, 3, 2, 3);
    for fault in 0..9 {
        let mut bad = next;
        match fault {
            0 => bad.start += 1,
            1 => bad.start -= 1,
            2 => bad.cap_before += 1,
            3 => bad.cap_after += 1,
            4 => bad.epoch += 1,
            5 => bad.cap_ordinal += 1,
            6 => bad.source = Some(EgressRefundSource::AckDrain),
            7 => bad.end = bad.start,
            _ => bad.end = u64::MAX,
        }
        let mut ledger = native_ledger();
        ledger.record_event(first);
        ledger.record_event(bad);
        assert_eq!(ledger.event_count, 2, "fault {fault}");
        assert_eq!(ledger.events[..2], [Some(first), Some(bad)]);
    }
    let mut ledger = native_ledger();
    for _ in 0..33 { ledger.record_event(first); }
    assert!(ledger.event_overflow);
    assert_eq!(ledger.event_count, 32);
}

#[test]
fn refund_runs_never_merge_sources_or_cross_retired_caps() {
    let mut first = extension(0, 1, 2, 1);
    first.kind = EgressLedgerEventKind::Refund;
    first.source = Some(EgressRefundSource::PacketFlush);
    let mut next = first;
    next.start = 1; next.end = 2; next.cap_before = 1; next.cap_after = 0;
    next.source = Some(EgressRefundSource::AckDrain);
    let mut ledger = native_ledger();
    ledger.record_event(first); ledger.record_event(next);
    assert_eq!(ledger.event_count, 2);
    let mut retired = next;
    retired.start = 2; retired.end = 3; retired.cap_before = 0;
    ledger.record_event(retired);
    assert_eq!(ledger.event_count, 3);
}

#[test]
fn legacy_refunds_remain_individual_actual_operations() {
    let mut ledger = EgressPermitLedger::default();
    for n in 0..2 {
        let mut event = extension(n, n + 1, 2 - n as usize, 1 - n as usize);
        event.kind = EgressLedgerEventKind::Refund;
        event.source = Some(EgressRefundSource::PacketFlush);
        ledger.record_event(event);
    }
    assert_eq!(ledger.event_count, 2);
}

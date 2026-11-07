//! Actual command lane and writer batching: original allocations, credits and
//! observer retirement. Whole Service Full is in native_complete.
use super::*;
use std::sync::Arc;
use crate::shared::errors::{FailureOrder, FailureOrigin, FailurePhase, FailureResource, QuietFailureSink};
struct Sink;
impl QuietFailureSink for Sink { fn record_quiet(&self, _: crate::shared::errors::RetainedFailure) {} }
fn bound(meter: &bytes::BackingMeter, sink: &Arc<dyn QuietFailureSink>) -> crate::shared::errors::FailureClock {
    let clock = Arc::new(FailureOrder::default());
    clock.bind(Arc::downgrade(sink), FailureOrigin { phase: FailurePhase::TcpOpen,
        resource: FailureResource::TcpOpen, resource_id: 0, attempt_id: Some(1) }, Some(meter.clone())).unwrap();
    clock
}
#[test]
fn native_merge_funds_actual_combined_allocation_and_keeps_old_alias_charges() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let sink: Arc<dyn QuietFailureSink> = Arc::new(Sink);
    let clock = bound(&meter, &sink);
    let (tx, mut rx, observer) = lane::native_with(lane::NativeCapacity { messages: 1024, waiters: 1 }, &meter, clock.clone(), &mut Some(())).unwrap();
    let a = Payload::copy_native(b"first", &meter).unwrap();
    let b = Payload::copy_native(b"second", &meter).unwrap();
    let (alias_a, alias_b) = (a.clone(), b.clone());
    let before = budget.used();
    tx.try_reserve().unwrap().send(b.into_command());
    tx.try_reserve().unwrap().send(RelayCommand::Finish);
    let result = coalesce_relay_payload(a, &mut rx);
    assert_eq!(result.payload.as_slice(), b"firstsecond");
    assert!(result.finish_after && result.deferred.is_none());
    let Payload::Native(value) = &result.payload else { panic!("Native merge cannot create Vec"); };
    assert!(value.backing_meter().unwrap().same_authority(&meter));
    assert_ne!(value.as_ptr(), alias_a.as_ptr());
    assert_ne!(value.as_ptr(), alias_b.as_ptr());
    assert_eq!(budget.used(), before + bytes::BytesMut::metered_copy_layout_for_len(11).unwrap());
    drop(result);
    assert_eq!(budget.used(), before);
    drop((alias_a, alias_b, tx, rx));
    assert!(observer.finish().is_ok());
    drop(clock);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn native_merge_full_preserves_both_originals_and_finish_order() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let sink: Arc<dyn QuietFailureSink> = Arc::new(Sink);
    let clock = bound(&meter, &sink);
    let (tx, mut rx, observer) = lane::native_with(lane::NativeCapacity { messages: 1024, waiters: 1 }, &meter, clock.clone(), &mut Some(())).unwrap();
    let a = Payload::copy_native(b"first", &meter).unwrap();
    let b = Payload::copy_native(b"second", &meter).unwrap();
    let (pa, pb) = (a.as_ptr(), b.as_ptr());
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    tx.try_reserve().unwrap().send(b.into_command());
    tx.try_reserve().unwrap().send(RelayCommand::Finish);
    let result = coalesce_relay_payload(a, &mut rx);
    assert_eq!(result.payload.as_ptr(), pa);
    assert_eq!(result.payload.as_slice(), b"first");
    assert!(!result.finish_after && result.failure.is_none());
    let next = Payload::from_command(result.deferred.unwrap());
    assert_eq!(next.as_ptr(), pb);
    assert_eq!(budget.used(), budget.limit());
    assert!(!budget.quarantined());
    assert_eq!(tx.capacity(), 1023, "one queued FIN; dequeue returned only slots");
    drop(result.payload); // Real current completes before the deferred value.
    let result = coalesce_relay_payload(next, &mut rx);
    assert_eq!(result.payload.as_ptr(), pb);
    assert!(result.finish_after && result.deferred.is_none() && result.failure.is_none());
    drop((result, pressure, tx, rx));
    assert!(observer.finish().is_ok());
    drop(clock);
    assert_eq!(budget.used(), 0);
}

#[test]
fn native_pressure_is_one_bounded_turn_and_cannot_start_fin() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let sink: Arc<dyn QuietFailureSink> = Arc::new(Sink);
    let clock = bound(&meter, &sink);
    let (tx, rx, observer) = lane::native_with(lane::NativeCapacity { messages: 1024, waiters: 1 }, &meter, clock.clone(), &mut Some(())).unwrap();
    let mut ctx = SocketCtx::new(443);
    ctx.uplink_tx = Some(tx);
    let mut calls = 0;
    assert!(matches!(pump_established_uplink(&mut ctx, TcpState::CloseWait, 500, |actual| {
        assert!(actual.unwrap().same_authority(&meter)); calls += 1; Extraction::Pressure
    }), EstablishedUplink::Handled));
    assert_eq!(calls, 1);
    assert!(!ctx.local_fin_sent && ctx.local_fin_pending_since_secs.is_none());
    assert_eq!(ctx.uplink_tx.as_ref().unwrap().capacity(), 1024);
    assert_eq!(rx.len(), 0);
    drop((ctx, rx));
    assert!(observer.finish().is_ok());
    drop(clock);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

thread_local! { static REFUSE_MERGE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub(in crate::client_tun) fn refuse_next_merge() { REFUSE_MERGE.with(|value| assert!(!value.replace(true))); }
pub(super) fn refuse_merge_now() -> bool { REFUSE_MERGE.with(|value| value.replace(false)) }

#[test]
fn actual_native_uplink_lane_cold_layout_and_retirement() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let sink: Arc<dyn QuietFailureSink> = Arc::new(Sink);
    let clock = bound(&meter, &sink);
    let before = budget.used();
    let limits = lane::NativeCapacity { messages: 1024, waiters: 1 };
    let layout = lane::native_layout_with::<RelayCommand>(limits).unwrap();
    let (tx, rx, observer) = lane::native_with::<RelayCommand>(limits, &meter, clock.clone(), &mut Some(())).unwrap();
    let charged = budget.used() - before;
    assert_eq!(charged, layout);
    assert_eq!(charged, 50_152, "fixed cold-cost compensation must be reviewed if layout changes");
    assert_eq!(tx.capacity(), 1024);
    assert_eq!(rx.max_capacity(), 1024);
    eprintln!("[N1-uplink-cold-layout] messages=1024 producer_records=1 actual_lane_charge={charged} two_Flow_increment={} fixed_Roles_Main_clock_subtotal={} F4096_same_partial={} not_whole_Flow_or_process_peak=true", charged * 2, 135392 + charged, (135392 + charged) * 4096);
    drop(tx);
    assert!(!observer.settled(), "actual receiver still owns the lane");
    drop(rx);
    assert!(observer.finish().is_ok());
    assert_eq!(budget.used(), before);
    drop(clock);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

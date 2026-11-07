//! Passive corruption seams; actual bytes/capabilities remain in the wire fixture.
use super::*;
impl Batch {
    pub(in crate::client_tun) fn test_forget_index(&mut self) {
        self.head = None;
        self.count = 0;
        self.armed = false;
    }
}
impl Slot {
    pub(in crate::client_tun) fn test_state(&self) -> (usize, bool) {
        (self.records.as_ptr() as usize, self.active())
    }
}

thread_local! { static Q_AFTER_ADMISSION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub(in crate::client_tun) fn arm_q_after_capture_admission() {
    Q_AFTER_ADMISSION.with(|armed| assert!(!armed.replace(true)));
}
pub(super) fn after_admission(meter: &BackingMeter) {
    if Q_AFTER_ADMISSION.with(|armed| armed.replace(false)) {
        meter.quarantine();
    }
}

// Bounded passive first-refusal observations. Never reruns a mutating check.
pub(super) fn rejected_batch(batch: &Batch) {
    eprintln!("[N1-capture-first-reject] phase={:?} refusal={:?} count={} head={:?} armed={} prior_Q={}",
        batch.phase, batch.refusal, batch.count, batch.head, batch.armed, batch.meter.is_quarantined());
}
pub(super) fn rejected_begin(meter: &BackingMeter, sockets: &SocketSet<'_>, contexts: &HashMap<SocketHandle, SocketCtx>, valid: bool) {
    eprintln!("[N1-capture-begin-reject] cold_valid={valid} contexts={} prior_Q={}", contexts.len(), meter.is_quarantined());
    for (handle, ctx) in contexts {
        let slot = &ctx.downlink_inflight_permits.capture;
        let checks = (physical_socket(sockets, *handle, ctx), storage::capture_inventory(ctx, meter),
            slot.idle_for(meter), slot.authorizes(*handle, ctx), pending_mode_matches(ctx),
            source_inventory_matches(*handle, ctx));
        let native = storage::any_native(ctx);
        let pristine = storage::pristine(ctx);
        let cold_ok = checks.0 && if native {
            checks.1 && checks.2 && checks.3 && !ctx.downlink_inflight_permits.quarantined()
        } else { pristine };
        if (!valid && !cold_ok) || (valid && ctx.downlink_inflight_permits.fault().is_some()) {
            eprintln!("[N1-capture-begin-physical] pristine={pristine} pending_capacity={} pending_cap_capacity={} inflight_cap_capacity={} record_len={} record_capacity={} owner={:?} state={:?}",
                ctx.downlink_pending.capacity(), ctx.downlink_pending_permits.capacity(),
                ctx.downlink_inflight_permits.permits.capacity(), slot.records.len(), slot.records.capacity(), slot.owner(), slot.records.first().map(|r| &r.state));
            eprintln!("[N1-capture-begin-context] handle={handle:?} epoch={} native={} checks(physical,layout,idle,owner,mode,source)={checks:?} fault={:?} pending={} caps={} inflight={}",
                ctx.conn_epoch, storage::any_native(ctx), ctx.downlink_inflight_permits.fault(),
                ctx.downlink_pending.len(), ctx.downlink_pending_permits.len(), ctx.downlink_inflight_permit_bytes());
            break;
        }
    }
}

#[test]
fn empty_clear_requires_actual_absence_of_capture_records_and_allocation() {
    for retain_record in [false, true] {
        let budget = bytes::BackingBudget::new(4096).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut slot = Slot::prepare(&meter, Owner::Standalone).unwrap();
        // Retain the real allowance until the actual record allocation drops.
        let cold = slot.cold.take();
        if !retain_record { slot.records.clear(); }
        let mut ctx = SocketCtx::new(443);
        ctx.downlink_inflight_permits.capture = slot;
        assert!(!storage::pristine(&ctx));
        assert!(budget.used() > 0);
        drop(ctx);
        drop(cold);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

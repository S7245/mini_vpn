//! Passive exposure of the original minted receipt for actual provider reentry.
//! No receipt, Flow row, endpoint or provider result is fabricated.
use super::super::*;
use super::super::super::rearm::FlowPhysicalReceipt;
use std::cell::Cell;
thread_local! {
    static ACTIVE: Cell<Option<FlowPhysicalReceipt>> = const { Cell::new(None) };
    static FULL_AUDITS: Cell<usize> = const { Cell::new(0) };
    static LAST_AUDITS: Cell<usize> = const { Cell::new(0) };
    static BEFORE_QUERY_Q: Cell<bool> = const { Cell::new(false) };
    static BEFORE_QUERY_FLOW_Q: Cell<bool> = const { Cell::new(false) };
}
pub(in crate::client_tun::compatibility_runtime::flow_custody::transaction) struct Scope;
pub(in crate::client_tun::compatibility_runtime::flow_custody::transaction) fn expose(receipt: FlowPhysicalReceipt) -> Scope {
    ACTIVE.with(|active| assert!(active.replace(Some(receipt)).is_none()));
    FULL_AUDITS.with(|count| count.set(0));
    Scope
}
impl Drop for Scope { fn drop(&mut self) {
    ACTIVE.with(|active| active.set(None));
    LAST_AUDITS.with(|last| last.set(FULL_AUDITS.with(Cell::get)));
} }
pub(in crate::client_tun::compatibility_runtime::flow_custody::transaction) fn full_audit() {
    if ACTIVE.with(|active| active.get().is_some()) { FULL_AUDITS.with(|count| count.set(count.get() + 1)); }
}
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn assert_release_reentry(self: &Arc<Self>) {
        let receipt = ACTIVE.with(|active| active.get().expect("original active release receipt"));
        let before = {
            let mut opens = self.opens();
            assert!(opens.audit_flows());
            let flow = opens.flow_mut(receipt.identity()).unwrap();
            assert!(flow.preparing && flow.upstream.is_none());
            (flow.w_held, flow.f_held, flow.reserved, flow.escrow_bytes)
        };
        let (dispatch, _rx) = OpenDispatch::channel(1, Some(self.clone()));
        assert!(!dispatch.release_flow(receipt), "exclusive release cannot issue a second ACK");
        let mut opens = self.opens();
        assert!(opens.audit_flows());
        let flow = opens.flow_mut(receipt.identity()).unwrap();
        assert_eq!(before, (flow.w_held, flow.f_held, flow.reserved, flow.escrow_bytes));
        assert_eq!(flow.stage, FlowStage::Rearming);
        assert!(!self.ledger().quarantined());
    }
}
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn assert_release_fault<T: 'static>(
        &self, address: usize, deadline: tokio::time::Instant, attempt_retained: bool,
    ) {
        let opens = self.opens();
        assert!(opens.audit_flows());
        let accounting = opens.flow.as_ref().unwrap();
        assert_eq!((accounting.released, accounting.f), (0, 1));
        assert!(accounting.first_provider_failure.is_none());
        let mut rows = opens.slots.iter().flatten();
        let OpenPhase::Flow(flow) = &rows.next().unwrap().phase else { panic!("original Flow"); };
        assert!(rows.next().is_none());
        assert_eq!(flow.stage, FlowStage::Quarantined);
        assert!(flow.preparing && flow.physical && flow.roles.is_some() && flow.upstream.is_some());
        assert_eq!(flow.attempt.is_some(), attempt_retained);
        assert!(!flow.release_escrow);
        assert_eq!(flow.stop_deadline, Some(deadline.into_std()));
        let payload = flow.panic.as_ref().unwrap().downcast_ref::<Box<T>>().unwrap();
        assert_eq!(&**payload as *const T as usize, address, "the original panic Box survives");
        drop(opens);
        let ledger = self.ledger();
        assert_eq!(ledger.shutdown_deadline, Some(deadline));
        assert!(ledger.quarantined());
        assert_eq!(ledger.first_failure.as_ref().unwrap().facts().class, crate::shared::errors::FailureClass::Panic);
        assert_eq!(ledger.additional_failures, 0);
    }
}
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn invalidate_release_epoch(&self) {
        let receipt = ACTIVE.with(|active| active.get().unwrap());
        let mut opens = self.opens();
        let slot = opens.slots.get_mut(receipt.identity().index).unwrap();
        assert!(slot.matches_physical(receipt.identity()));
        slot.epoch += 1;
    }
    pub(in crate::client_tun::compatibility_runtime) fn assert_lost_release_epoch(&self) {
        let opens = self.opens();
        let accounting = opens.flow.as_ref().unwrap();
        assert_eq!((accounting.released, accounting.f, opens.live), (0, 1, 1));
        let slot = opens.slots.iter().flatten().next().unwrap();
        let OpenPhase::Flow(flow) = &slot.phase else { panic!("actual Flow"); };
        assert_eq!(slot.epoch, flow.id.epoch + 1);
        assert!(flow.preparing && flow.upstream.is_none() && flow.attempt.is_none() && flow.roles.is_none());
        assert!(flow.panic.is_none());
        assert_eq!(flow.escrow.len(), flow.escrow_bytes);
        drop(opens);
        assert!(self.ledger().quarantined());
        assert!(self.ledger().first_failure.is_none());
    }
}
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn assert_release_drop_q(&self, deadline: tokio::time::Instant, attempt_retained: bool) {
        let opens = self.opens();
        assert!(opens.audit_flows());
        let accounting = opens.flow.as_ref().unwrap();
        assert_eq!((accounting.released, accounting.f), (0, 1));
        let slot = opens.slots.iter().flatten().next().unwrap();
        let OpenPhase::Flow(flow) = &slot.phase else { panic!("original Flow"); };
        assert!(slot.matches_physical(flow.id));
        assert!(flow.preparing && flow.attempt.is_some() == attempt_retained && flow.upstream.is_some() && flow.roles.is_some());
        assert_eq!(flow.stage, FlowStage::Quarantined);
        assert!(flow.panic.is_none() && !flow.release_escrow);
        assert_eq!(flow.stop_deadline, Some(deadline.into_std()));
        drop(opens);
        let ledger = self.ledger();
        assert!(ledger.quarantined() && ledger.first_failure.is_none());
        assert_eq!(ledger.shutdown_deadline, Some(deadline));
    }
}
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn invalidate_release_accounting(&self) {
        let receipt = ACTIVE.with(|active| active.get().unwrap());
        self.opens().flow_mut(receipt.identity()).unwrap().reserved += 1;
    }
    pub(in crate::client_tun::compatibility_runtime) fn assert_invalid_release_accounting(&self) {
        let opens = self.opens();
        assert!(!opens.audit_flows());
        let accounting = opens.flow.as_ref().unwrap();
        assert_eq!((accounting.released, accounting.f, opens.live), (0, 1, 1));
        let slot = opens.slots.iter().flatten().next().unwrap();
        let OpenPhase::Flow(flow) = &slot.phase else { panic!("actual Flow"); };
        assert!(slot.matches_physical(flow.id));
        assert!(flow.preparing && flow.attempt.is_some() && flow.upstream.is_some() && flow.roles.is_some());
        assert_eq!(flow.stage, FlowStage::Quarantined);
        assert!(flow.panic.is_none() && !flow.release_escrow);
        drop(opens);
        let ledger = self.ledger();
        assert!(ledger.quarantined() && ledger.first_failure.is_none());
    }
}
#[tokio::test]
async fn soft_release_quarantine_preserves_original_session_stop_policy() {
    let owner = std::sync::Arc::new(SessionControl::default());
    let index = super::soft_q::admit_one(&owner).await;
    assert!(owner.app_meter.is_none());
    assert!(!owner.stop.stopping.load(Ordering::Acquire));
    assert!(owner.ledger().shutdown_deadline.is_none());
    {
        let mut opens = owner.opens();
        let OpenPhase::Flow(flow) = &mut opens.slots.get_mut(index).unwrap().phase else { panic!("actual Flow") };
        owner.quarantine_release(flow); // F6: the release fault names its held row.
        assert_eq!(flow.stage, FlowStage::Quarantined);
    }
    // D210: Soft release Q is Flow scope: sticky Q, no Session refusal.
    assert!(owner.ledger().quarantined() && !owner.ledger().session_quarantined());
    assert_eq!(owner.state(), crate::client_tun::compatibility_runtime::SessionDeviceState::Running);
    assert!(!owner.stop.stopping.load(Ordering::Acquire), "Soft's original owner marks Q without stopping unrelated work");
    assert!(owner.ledger().shutdown_deadline.is_none(), "Soft Q does not mint a new Session deadline");
}

pub(in crate::client_tun::compatibility_runtime::flow_custody::transaction) fn before_query(owner: &SessionControl) {
    if BEFORE_QUERY_Q.with(|flag| flag.replace(false)) { owner.quarantine_quiet(); }
    if BEFORE_QUERY_FLOW_Q.with(|flag| flag.replace(false)) { owner.quarantine_flow(FlowQRow::unheld_for_test()); }
}
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn arm_before_release_query_q(&self) {
        BEFORE_QUERY_Q.with(|flag| assert!(!flag.replace(true)));
    }
    /// B560 under D210: a Flow-scope Q raised inside the release Turn.
    pub(in crate::client_tun::compatibility_runtime) fn arm_before_release_query_flow_q(&self) {
        BEFORE_QUERY_FLOW_Q.with(|flag| assert!(!flag.replace(true)));
    }
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn assert_bounded_release_audits() {
        let count = LAST_AUDITS.with(Cell::get);
        assert_eq!(count, 2, "only the two actual provider reentries invalidate this synchronous release: {count}");
    }
    pub(in crate::client_tun::compatibility_runtime) fn replace_release_root(&self) {
        let mut opens = self.opens();
        let mut original = std::mem::replace(&mut *opens, OpenCustody::with_flows());
        // The entire actual inventory leaves and re-enters the original guard.
        // Corrupt only accounting; preserve every original opaque value.
        original.flow.as_mut().unwrap().bytes += 1;
        *opens = original;
    }
}

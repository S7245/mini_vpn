//! Final original Flow release transaction. The original Session owns the row,
//! W/F/escrow and final receipt. One synchronous turn owns the original provider
//! and actor fields while `preparing` excludes all other turns. Unknown loss
//! restores custody under Q; it cannot grant release or refund the row.
//! Entry: OpenDispatch::release_flow. Tests: native_complete/send_inventory and
//! transaction physical receipt lifecycle tests.
use super::*;
use std::mem::ManuallyDrop;
use super::super::rearm::FlowPhysicalReceipt;

type Attempt = Box<dyn TcpOpenAttempt<OpenedOwnedTcp>>;
type Panic = Box<dyn std::any::Any + Send>;
struct Turn {
    owner: Arc<SessionControl>,
    receipt: FlowPhysicalReceipt,
    attempt: ManuallyDrop<Option<Attempt>>,
    upstream: ManuallyDrop<Option<Arc<dyn OwnedUpstream>>>,
    roles: ManuallyDrop<Option<FlowRoles>>,
    escrow: ManuallyDrop<Option<crate::client_tun::uplink::Payload>>,
    panic: ManuallyDrop<Option<Panic>>,
    rejection: Option<FlowRejection>,
    joined: usize,
    armed: bool,
    audited_revision: std::cell::Cell<Option<u64>>,
}
impl Turn {
    fn matches(&self, flow: &FlowTransaction) -> bool {
        flow.id == self.receipt.identity() && flow.preparing
            && flow.attempt.is_none() && flow.upstream.is_none() && flow.roles.is_none()
            && flow.carrier.is_none() && flow.cold_retained.is_none() && flow.panic.is_none()
            && flow.stage == FlowStage::Rearming && flow.producer_stopped && flow.physical
    }
    fn qualified(&self, opens: &custody::Guard<'_>) -> bool {
        let revision = opens.revision();
        if revision.is_none() || self.audited_revision.get() != revision {
            self.audited_revision.set(None);
            #[cfg(test)]
            tests::release::full_audit();
            if !opens.audit_flows() { return false; }
            self.audited_revision.set(revision);
        }
        // This evidence belongs only to this synchronous Turn and its owner.
        // Every mutable access invalidates it before exposing the original row.
        // Opaque calls may still change Q without touching opens: check it fresh.
        let id = self.receipt.identity();
        opens.slots.get(id.index).and_then(Option::as_ref).is_some_and(|slot| {
            slot.matches_physical(id)
                && matches!(&slot.phase, OpenPhase::Flow(flow) if self.matches(flow))
        }) && !self.owner.ledger().quarantined
            && !self.owner.app_meter.as_ref().is_some_and(bytes::BackingMeter::is_quarantined)
    }
    fn current(&self) -> bool { self.qualified(&self.owner.opens()) }
    // The caller retains each remaining field before the first opaque call.
    // A panic terminates this turn; it does not trigger automatic sibling Drop.
    fn guard<T>(&mut self, operation: impl FnOnce() -> T) -> Option<T> {
        match catch_unwind(AssertUnwindSafe(operation)) {
            Ok(value) => Some(value),
            Err(payload) => { *self.panic = Some(payload); None }
        }
    }
    fn run(&mut self) -> bool {
        if !self.current() { return false; }
        let id = self.receipt.identity();
        #[cfg(test)]
        let _receipt_scope = tests::release::expose(self.receipt);
        let facts = catch_unwind(AssertUnwindSafe(|| {
            self.attempt.as_ref().and_then(|a| a.failure()).map(|failure| {
                let origin = open_origin(id.index, id.epoch, FailurePhase::TcpOpen);
                let admission = match failure {
                    crate::shared::ClientError::Io(io) => io.get_ref()
                        .and_then(|e| e.downcast_ref::<quinn::owned_events::AdmissionError>()).map(|e| e.0),
                    _ => None,
                };
                let retained = if admission.is_some() { RetainedFailure::from_client_ref(origin, failure) }
                    else { RetainedFailure::from_io(origin, std::io::ErrorKind::Other.into()) };
                (admission, retained)
            })
        }));
        let facts = match facts {
            Ok(facts) => facts,
            Err(payload) => { *self.panic = Some(payload); return false; }
        };
        if facts.as_ref().is_some_and(|(cause, _)| *cause == Some(bytes::MeteredCopyError::Quarantined)) {
            return false;
        }
        let origin = open_origin(id.index, id.epoch, FailurePhase::TcpOpen);
        if let Some(rejection) = self.rejection {
            let kind = match rejection {
                FlowRejection::ReportedNativeUplink(_) => None,
                FlowRejection::Unavailable(crate::upstream::TcpOpenUnavailable::Unsupported) => Some(std::io::ErrorKind::Unsupported),
                FlowRejection::Unavailable(crate::upstream::TcpOpenUnavailable::InvalidTarget) => Some(std::io::ErrorKind::InvalidInput),
                FlowRejection::Deadline => Some(std::io::ErrorKind::TimedOut),
            };
            if let Some(kind) = kind { self.owner.record(RetainedFailure::from_io(origin, kind.into())); }
        } else if let Some((_, failure)) = &facts { self.owner.record(failure.clone()); }
        if facts.is_some() {
            let mut opens = self.owner.opens();
            if !self.qualified(&opens) { return false; }
            let Some(accounting) = opens.flow.as_mut() else { return false; };
            // Another Flow may have won during the one lock-free query. Its
            // original first Box is immutable; our excess Box drops below.
            if accounting.first_provider_failure.is_none() {
                accounting.first_provider_failure = self.attempt.take();
            }
        }
        if !self.current() { return false; }
        let attempt = self.attempt.take();
        if self.guard(|| drop(attempt)).is_none() { return false; }
        if !self.current() { return false; }
        let upstream = self.upstream.take();
        if self.guard(|| drop(upstream)).is_none() { return false; }
        // Original actors have already disposed their opaque fields separately.
        // This final gate checks those dispositions, not merely task JOIN.
        if !self.current() { return false; }
        if self.roles.as_ref().is_some_and(|roles| !roles.release_ready()) { return false; }
        let roles = self.roles.take();
        if self.guard(|| drop(roles)).is_none() { return false; }
        {
            let mut opens = self.owner.opens();
            if !self.qualified(&opens) { return false; }
            let Some(flow) = opens.flow_mut(id) else { return false; };
            if flow.release_escrow || flow.escrow.len() != flow.escrow_bytes { return false; }
            *self.escrow = Some(std::mem::replace(&mut flow.escrow, Vec::new().into()));
            flow.release_escrow = true;
            // This same exclusive guard changed only the audited escrow shape:
            // preparing + empty escrow + release_escrow. No opaque call occurred.
            self.audited_revision.set(opens.revision());
        }
        if !self.current() { return false; }
        let escrow = self.escrow.take();
        if self.guard(|| drop(escrow)).is_none() { return false; }
        let mut opens = self.owner.opens();
        if !self.qualified(&opens) { return false; }
        let Some(flow) = opens.flow_mut(id).filter(|flow| self.matches(flow) && flow.release_escrow) else { return false; };
        let (w, f, bytes, escrow, published) = (flow.w_held, flow.f_held, flow.reserved, flow.escrow_bytes, flow.published);
        let Some(accounting) = opens.flow.as_mut() else { return false; };
        if accounting.first_rejection.is_none() { accounting.first_rejection = self.rejection; }
        accounting.w -= usize::from(w);
        accounting.f -= usize::from(f);
        accounting.bytes -= bytes;
        accounting.escrow_bytes -= escrow;
        accounting.last_data_disposition = Some(self.receipt.data_disposition());
        #[cfg(all(test, feature = "harness"))]
        { accounting.last_permit_reset = self.receipt.permit_reset_observation(); }
        #[cfg(test)]
        { accounting.last_d16_disposition = self.receipt.d16_disposition(); }
        accounting.released += 1;
        accounting.joined += self.joined as u64;
        if published { accounting.retry = (accounting.retry + 1).min(OPEN_LIMIT); }
        // Only value fields, an empty escrow, and the plain Box allocation
        // remain. No provider/actor/last Arc is destroyed under opens.
        let row = opens.slots.take(id.index);
        opens.live -= 1;
        self.armed = false;
        drop(opens);
        drop(row);
        true
    }
}
impl Drop for Turn {
    fn drop(&mut self) {
        if !self.armed { return; }
        // No further opaque operation runs. Restore the surviving originals
        // under the same full-id qualification; foreign occupants are untouched.
        let id = self.receipt.identity();
        let panicked = self.panic.is_some();
        self.owner.quarantine_release();
        let deadline = self.owner.app_meter.as_ref().filter(|meter| meter.budget_limit().is_some())
            .and_then(|_| crate::shared::errors::QuietFailureSink::deadline_quiet(&*self.owner));
        {
            let mut opens = self.owner.opens();
            let original = opens.slots.get(id.index).and_then(Option::as_ref)
                .is_some_and(|slot| slot.matches_physical(id));
            if let Some(flow) = opens.flow_mut(id).filter(|flow| {
                original && flow.preparing && flow.attempt.is_none() && flow.upstream.is_none() && flow.roles.is_none()
            }) {
                flow.attempt = self.attempt.take();
                flow.upstream = self.upstream.take();
                flow.roles = self.roles.take();
                if let Some(escrow) = self.escrow.take() {
                    // The actual original was never dropped; restore its slot.
                    flow.escrow = escrow;
                    flow.release_escrow = false;
                }
                if let Some(payload) = self.panic.take() { flow.retain_unwind(payload); }
                flow.stage = FlowStage::Quarantined;
                if let Some(deadline) = deadline {
                    flow.stop_deadline.limit(deadline.into_std());
                }
                // Keep exclusive qualification after partial actual disposal.
            }
        }
        if panicked { self.owner.note_flow_unwind(id); }
    }
}
impl OpenDispatch {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn release_flow(&self, receipt: FlowPhysicalReceipt) -> bool {
        let Some(owner) = &self.owner else { return false; };
        let id = receipt.identity();
        let mut opens = owner.opens();
        #[cfg(test)]
        let Some(receipt) = tests::receipt_gate::intercept(&mut opens, receipt) else { return false; };
        if opens.flow_mut(id).is_some_and(|flow| flow.preparing) { return false; }
        if !opens.audit_flows() {
            if let Some(flow) = opens.flow_mut(id) { flow.stage = FlowStage::Quarantined; }
            drop(opens); owner.quarantine_release(); return false;
        }
        let Some(flow) = opens.flow_mut(id) else { drop(opens); owner.quarantine_release(); return false; };
        if flow.cold_retained.is_some() || flow.panic.is_some() || flow.release_escrow
            || !receipt.matches_flow(flow) || flow.stage != FlowStage::Rearming
            || !flow.producer_stopped || !flow.physical || flow.carrier.is_some()
            || flow.roles.as_ref().is_some_and(|roles| !roles.settled()) {
            flow.stage = FlowStage::Quarantined;
            drop(opens); owner.quarantine_release(); return false;
        }
        if let Some(roles) = &mut flow.roles {
            if let Some(disposal) = roles.take_original_disposal(owner, id) {
                drop(opens);
                disposal.dispose(owner, &mut std::task::Context::from_waker(std::task::Waker::noop()));
                return false;
            }
        }
        flow.preparing = true;
        let mut turn = Turn {
            owner: owner.clone(), receipt, rejection: flow.rejection,
            joined: flow.roles.as_ref().map_or(0, FlowRoles::join_count),
            attempt: ManuallyDrop::new(flow.attempt.take()),
            upstream: ManuallyDrop::new(flow.upstream.take()),
            roles: ManuallyDrop::new(flow.roles.take()),
            escrow: ManuallyDrop::new(None), panic: ManuallyDrop::new(None), armed: true,
            audited_revision: std::cell::Cell::new(None),
        };
        // The full audit above survives this own move: identity/counters/escrow
        // are unchanged, and preparing permits upstream to reside in this Turn.
        // Poison/exhaustion remain None; every later foreign write invalidates it.
        turn.audited_revision.set(opens.revision());
        drop(opens);
        #[cfg(test)]
        tests::release::before_query(owner);
        turn.run()
    }
}

impl SessionControl {
    pub(super) fn quarantine_release(&self) {
        if self.app_meter.as_ref().is_some_and(|meter| meter.budget_limit().is_some()) {
            self.quarantine_quiet();
        } else {
            // Original Soft provider catch records Q without widening its
            // failure into a Session stop or inventing a new deadline.
            self.ledger().quarantined = true;
        }
    }
}

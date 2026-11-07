//! Cold binding from original roles to the existing Session failure ledger.
//! Early IO facts do not stop the Session; panic records quiet stop/deadline/Q.
//! No notification, JOIN or refund is performed by early publication.
//! Original error and panic objects stay in their role until real settlement.
use super::*;
use crate::client_tun::compatibility_runtime::SessionControl;
use crate::shared::errors::{FailurePhase, QuietFailureSink, RetainedFailure};
use crate::client_tun::{SocketCtx, compatibility_runtime::flow_custody::rearm::physical_flow::FlowMirror};

impl FlowRoles {
    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn bind_failure_owner(
        &self,
        owner: &Arc<SessionControl>,
        id: super::super::super::transaction::FlowIdentity,
    ) -> bool {
        if !self.is_native() {
            return true;
        }
        let sink: Arc<dyn QuietFailureSink> = owner.clone();
        self.failures
            .bind(
                Arc::downgrade(&sink),
                super::super::super::transaction::open_origin(
                    id.index,
                    id.epoch,
                    FailurePhase::TcpOpen,
                ),
                owner.app_meter.clone(),
            )
            .is_ok()
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn prepare_failure_owner(
        owner: &Arc<SessionControl>,
        id: super::super::super::transaction::FlowIdentity,
    ) -> Result<FailureClock, bytes::MeteredCopyError> {
        let clock = Arc::new(crate::shared::errors::FailureOrder::default());
        if owner.app_meter.is_some() {
            let sink: Arc<dyn QuietFailureSink> = owner.clone();
            clock.bind(
                Arc::downgrade(&sink),
                super::super::super::transaction::open_origin(
                    id.index,
                    id.epoch,
                    FailurePhase::TcpOpen,
                ),
                owner.app_meter.clone(),
            )?;
        }
        Ok(clock)
    }
}

/// One original Flow disposition turn, shared by IO and Native joined actors.
/// The original row retains its explicit JOIN/exclusive witness. No opaque
/// destructor is reachable from token Drop, including wrong-epoch return.
#[must_use]
pub(in crate::client_tun::compatibility_runtime::flow_custody) struct OriginalDisposal {
    action: Option<Action>,
    clock: FailureClock,
    owner: std::sync::Weak<SessionControl>,
    id: super::super::super::transaction::FlowIdentity,
    deadline: Option<std::time::Instant>,
    armed: bool,
}
enum Action { Io(std::mem::ManuallyDrop<std::io::Error>), Native(native::Work) }
impl OriginalDisposal {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn is_stock_retirement(&self) -> bool {
        matches!(&self.action, Some(Action::Native(work)) if work.is_stock_retirement())
    }
    fn valid(&self, opens: &mut super::super::super::transaction::OpenCustody) -> bool {
        let identity = opens.slots.get(self.id.index).and_then(Option::as_ref)
            .is_some_and(|slot| slot.handle == self.id.handle && slot.epoch == self.id.epoch
                && (!matches!(&self.action, Some(Action::Native(_))) || slot.matches_physical(self.id)));
        identity && opens.flow_mut(self.id).and_then(|flow| flow.roles.as_ref()).is_some_and(|roles| {
            roles.failure_disposal && Arc::ptr_eq(&roles.failures, &self.clock)
                && match &self.action { Some(Action::Native(work)) => roles.native.as_ref().is_some_and(|native| work.matches(native)), Some(Action::Io(_)) => true, None => false }
        })
    }
    fn earliest(&self, owner: &SessionControl) -> Option<std::time::Instant> {
        self.deadline.into_iter()
            .chain(owner.deadline_quiet().map(|d| d.into_std()))
            .chain(self.clock.parent_deadline_for_stop().map(|d| d.into_std())).min()
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn dispose(
        self, owner: &SessionControl, cx: &mut Context<'_>,
    ) -> bool { self.dispose_inner(owner, cx, None) }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn dispose_producers(
        self, owner: &SessionControl, cx: &mut Context<'_>, ctx: &mut SocketCtx,
    ) -> bool { self.dispose_inner(owner, cx, Some(ctx)) }
    fn producer_context_matches(&self, ctx: Option<&SocketCtx>) -> bool {
        if matches!(&self.action, Some(Action::Native(work)) if work.is_producers()) {
            ctx.is_some_and(|ctx| FlowMirror::producer_return_matches(ctx, self.id))
        } else { ctx.is_none() }
    }
    fn dispose_inner(
        mut self, owner: &SessionControl, cx: &mut Context<'_>, mut ctx: Option<&mut SocketCtx>,
    ) -> bool {
        #[cfg(test)]
        if matches!(&self.action, Some(Action::Native(work)) if work.is_source_for_test())
            && owner.native_source_disposal_fault(self.id) { return false; }
        if !self.valid(&mut owner.opens()) || !self.producer_context_matches(ctx.as_deref()) { return false; }
        let native = matches!(&self.action, Some(Action::Native(_)));
        let result = catch_unwind(AssertUnwindSafe(|| match self.action.as_mut() {
            Some(Action::Io(error)) => drop(unsafe { std::mem::ManuallyDrop::take(error) }),
            Some(Action::Native(work)) => work.run(cx),
            None => {},
        }));
        #[cfg(test)]
        if let Some(Action::Native(work)) = &self.action {
            if let Some(allocation) = work.pending_stock_for_test() {
                if owner.native_stock_disposal_fault(self.id, allocation) { return false; }
            }
        }
        if result.is_err() {
            if native { self.clock.panic(); }
            else { owner.record_quiet(RetainedFailure::opaque_panic(
                super::super::super::transaction::open_origin(self.id.index, self.id.epoch, FailurePhase::TcpOpen),
            )); }
        }
        if matches!(&self.action, Some(Action::Native(work)) if work.failed()) { self.clock.quarantine(); }
        let deadline = self.earliest(owner);
        let mut opens = owner.opens();
        if !self.valid(&mut opens) || !self.producer_context_matches(ctx.as_deref()) {
            if let Err(payload) = result { std::mem::forget(payload); }
            drop(opens); return false;
        }
        let progressed = result.is_ok() && match &self.action { Some(Action::Native(work)) => work.progressed(), Some(Action::Io(_)) => true, None => false };
        let producers = matches!(&self.action, Some(Action::Native(work)) if work.is_producers());
        let producers_done = matches!(&self.action, Some(Action::Native(work)) if work.producers_done());
        let flow = opens.flow_mut(self.id).unwrap(); // valid() has qualified this exact row.
        if let Some(deadline) = deadline { flow.stop_deadline = Some(flow.stop_deadline.map_or(deadline, |old| old.min(deadline))); }
        let roles = flow.roles.as_mut().unwrap();
        if let Some(Action::Native(work)) = self.action.take() { work.restore(roles.native.as_mut().unwrap()); }
        let mut additional = None;
        match result {
            Ok(()) if !producers => roles.failure_disposal = false,
            Ok(()) if producers_done => {
                // Full original row/clock/context qualification remains held
                // until the physical owner and Flow commit together.
                if ctx.as_deref_mut().is_some_and(|ctx| FlowMirror::commit_producer_return(ctx, self.id)) {
                    flow.producer_stopped = true;
                    roles.failure_disposal = false;
                }
            }
            Ok(()) => {}, // Partial field Drop keeps the original exclusive qualification.
            Err(payload) if roles.panic.is_none() => roles.panic = Some(payload),
            Err(payload) => additional = Some(payload),
        }
        self.armed = false;
        drop(opens);
        if let Some(payload) = additional { std::mem::forget(payload); self.clock.quarantine(); }
        progressed
    }
}
impl Drop for OriginalDisposal {
    fn drop(&mut self) {
        if !self.armed { return; }
        self.clock.quarantine();
        if let Some(owner) = self.owner.upgrade() {
            owner.quarantine_quiet();
            let deadline = self.earliest(&owner);
            let mut opens = owner.opens();
            if let Some(flow) = opens.flow_mut(self.id).filter(|flow| flow.roles.as_ref().is_some_and(|roles|
                roles.failure_disposal && Arc::ptr_eq(&roles.failures, &self.clock))) {
                    flow.stage = super::super::super::transaction::FlowStage::Quarantined;
                    if let Some(deadline) = deadline { flow.stop_deadline = Some(flow.stop_deadline.map_or(deadline, |old| old.min(deadline))); }
            }
            drop(opens);
        }
    }
}
impl FlowRoles {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn retain_control_unwind(&mut self, payload: Box<dyn std::any::Any + Send>) {
        // This original role slot now permanently fences further callbacks.
        self.failure_disposal = true;
        if self.panic.is_none() { self.panic = Some(payload); }
        else { std::mem::forget(payload); }
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn hard_source_producers(&self) -> bool {
        self.native.as_ref().is_some_and(native::Roles::has_hard_source)
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn take_native_producers(
        &mut self, owner: &Arc<SessionControl>, id: super::super::super::transaction::FlowIdentity,
        ctx: &mut SocketCtx, deadline: Option<std::time::Instant>,
    ) -> Option<OriginalDisposal> {
        if self.failure_disposal || self.panic.is_some() || !FlowMirror::producer_admission_matches(ctx, id) { return None; }
        let work = self.native.as_mut()?.take_producers(&mut ctx.uplink_tx, &mut ctx.relay_read_credit_tx)?;
        self.failure_disposal = true;
        Some(OriginalDisposal { action: Some(Action::Native(work)), clock: self.failures.clone(), owner: Arc::downgrade(owner), id, deadline, armed: true })
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn take_native_start(
        &mut self, owner: &Arc<SessionControl>, id: super::super::super::transaction::FlowIdentity,
        deadline: Option<std::time::Instant>,
    ) -> Option<OriginalDisposal> {
        if self.failure_disposal || self.panic.is_some() { return None; }
        let work = self.native.as_mut()?.take_start_disposal()?;
        self.failure_disposal = true;
        Some(OriginalDisposal { action: Some(Action::Native(work)), clock: self.failures.clone(), owner: Arc::downgrade(owner), id, deadline, armed: true })
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn take_native_disposal(
        &mut self, owner: &Arc<SessionControl>, id: super::super::super::transaction::FlowIdentity,
        producer_stopped: bool, deadline: Option<std::time::Instant>,
    ) -> Option<OriginalDisposal> {
        if self.failure_disposal || self.panic.is_some() { return None; }
        let work = self.native.as_mut()?.take_disposal(producer_stopped)?;
        self.failure_disposal = true;
        Some(OriginalDisposal { action: Some(Action::Native(work)), clock: self.failures.clone(), owner: Arc::downgrade(owner), id, deadline, armed: true })
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn take_original_disposal(
        &mut self, owner: &Arc<SessionControl>, id: super::super::super::transaction::FlowIdentity,
    ) -> Option<OriginalDisposal> {
        if self.failure_disposal || self.panic.is_some()
            || self.failures.load(std::sync::atomic::Ordering::SeqCst) == usize::MAX { return None; }
        while let Some(failure) = self.take_original_failure() {
            let origin = super::super::super::transaction::open_origin(id.index, id.epoch, FailurePhase::TcpOpen);
            let error = match failure {
                OriginalFailure::Io(error) => { owner.record(RetainedFailure::from_io_ref(origin, &error)); Some(error) }
                OriginalFailure::ReportedIo(error) => Some(error),
                OriginalFailure::OpaquePanic => { owner.record(RetainedFailure::opaque_panic(origin)); None }
                OriginalFailure::ReportedPanic => None,
            };
            if let Some(error) = error {
                self.failure_disposal = true;
                return Some(OriginalDisposal { action: Some(Action::Io(std::mem::ManuallyDrop::new(error))),
                    clock: self.failures.clone(), owner: Arc::downgrade(owner), id, deadline: None, armed: true });
            }
        }
        None
    }
}

//! Flow-scope Q root ledger (D210 scoped Q, D211).
//!
//! Owns: the per-row sticky Q marker ([`RowQ`]) and the root counter
//! ([`RootQ`]); both have private fields so only this module changes them.
//! A row is registered once, under the root lock, the first time its stage is
//! seen `Quarantined`. Does not own: row stages, the Session Q (see
//! `compatibility_runtime::quarantine`), release refusal (release.rs).
//! Invariants (checked by `audit`, both ways): Σ row markers == root counter,
//! and a marked row stays `Quarantined` (a regressed stage fails the audit ⇒
//! Session scope). Counter > [`FLOW_Q_LIMIT`] ⇒ Session scope. Bounded control
//! path only: registration is one compare per visited row, no DATA cost.
//! Flow scope requires a row (F6): [`FlowQRow`] is constructed only here,
//! from the row being marked, so a Flow raise without a held row is
//! unrepresentable. D216 admission: the caller proves the fault is fully
//! retained in that row (Box/stream/lease kept, root audit re-run on every
//! release); shared-state uncertainty uses `quarantine_quiet` instead.
//! D222 (user): in the hard profile a Q row still holding shared budget bytes
//! (native escrow, or a native roles meter with live allocations) is Session
//! scope at both entries, so a sticky row cannot starve siblings while Running.
//! Entry: `FlowTransaction::quarantine_flow`, `OpenCustody::{register_flow_q,
//! audit_flow_q}`. Tests: transaction/tests/soft_q.rs.
use super::*;

/// Held Flow-Q rows before the fault becomes Session scope: half of W, so
/// held unpublished rows alone can never close admission.
pub(super) const FLOW_Q_LIMIT: usize = OPEN_LIMIT / 2;

#[derive(Default)]
pub(in crate::client_tun::compatibility_runtime) struct RowQ(bool);

#[derive(Default)]
pub(in crate::client_tun::compatibility_runtime) struct RootQ(usize);

/// Witness that a Flow-scope raise names one row held under the root lock.
pub(in crate::client_tun::compatibility_runtime) struct FlowQRow(());

impl FlowQRow {
    /// Test hooks model another row's raise without building that row.
    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime::flow_custody::transaction) fn unheld_for_test() -> Self {
        Self(())
    }
}

impl FlowTransaction {
    /// Flow scope (D216 "Row-held ⇒ Flow"): this row retains the uncertain
    /// custody; it becomes sticky `Quarantined` and the raise is published.
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn quarantine_flow(&mut self, owner: &SessionControl) {
        self.stage = FlowStage::Quarantined;
        owner.quarantine_flow(FlowQRow(()));
        if self.holds_hard_budget(owner) { owner.quarantine_quiet(); }
    }

    /// D222: hard profile and this row still holds shared `BackingBudget`
    /// bytes. A roles meter is counted while it has any live allocation
    /// (over-approximation routes to Session, the fail-closed direction).
    fn holds_hard_budget(&self, owner: &SessionControl) -> bool {
        owner.app_meter.as_ref().is_some_and(|meter| meter.budget_limit().is_some())
            && (self.escrow_bytes != 0
                || self.roles.as_ref().and_then(FlowRoles::native_backing_meter).is_some_and(|meter| {
                    let held = meter.snapshot();
                    held.allocations != 0 || held.physical_bytes != 0
                }))
    }

    /// A sticky, root-registered Flow-scope Q row.
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn flow_q_registered(&self) -> bool {
        self.q_mark.0 && self.stage == FlowStage::Quarantined
    }
}

impl OpenCustody {
    /// Registers row `index` if its stage is `Quarantined` and it is not yet
    /// counted. Returns false when the Flow-Q bound is exceeded or the row
    /// holds hard budget bytes (D222; the caller raises Session scope). A Flow Q raise is published for the release
    /// Turn's fresh check.
    pub(super) fn register_flow_q(&mut self, index: usize, owner: &SessionControl) -> bool {
        let OpenCustody { slots, flow: accounting, .. } = self;
        let (Some(slot), Some(accounting)) = (slots.get_mut(index), accounting) else {
            return true;
        };
        let OpenPhase::Flow(flow) = &mut slot.phase else {
            return true;
        };
        if flow.stage != FlowStage::Quarantined || flow.q_mark.0 {
            return true;
        }
        flow.q_mark.0 = true;
        accounting.quarantined_rows.0 += 1;
        owner.quarantine_flow(FlowQRow(()));
        accounting.quarantined_rows.0 <= FLOW_Q_LIMIT && !flow.holds_hard_budget(owner)
    }

    /// R2 test seam: the counter stands for `n` sibling rows already
    /// registered, so the real bound check is reached with one actual row.
    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime::flow_custody::transaction) fn preset_flow_q_rows_for_test(&mut self, n: usize) {
        if let Some(accounting) = &mut self.flow { accounting.quarantined_rows.0 = n; }
    }

    /// Registered Flow-Q rows (root counter).
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn flow_q_rows(&self) -> usize {
        self.flow.as_ref().map_or(0, |accounting| accounting.quarantined_rows.0)
    }

    /// Bidirectional row-marker ↔ root-counter check.
    pub(super) fn audit_flow_q(&self) -> bool {
        let Some(accounting) = &self.flow else {
            return true;
        };
        let mut marked = 0usize;
        for slot in self.slots.iter().flatten() {
            if let OpenPhase::Flow(flow) = &slot.phase {
                if flow.q_mark.0 {
                    if flow.stage != FlowStage::Quarantined {
                        return false;
                    }
                    marked += 1;
                }
            }
        }
        marked == accounting.quarantined_rows.0
    }
}

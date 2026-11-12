//! Typed Session quarantine authority (D210 scoped Q, D211).
//!
//! Owns: the only writers of the Session Q state ([`SessionQ`], private
//! fields) and its two scopes. Session scope = Q + stop + bounded deadline +
//! meter Q (`quarantine_quiet`); Q without stop/deadline is unrepresentable.
//! Flow scope = a row-contained fault: the row stays sticky `Quarantined`
//! (registered once in the root counter, audited both ways by
//! `OpenCustody::audit_flows`), the session keeps serving, and only a release
//! Turn that observes a Flow Q raised after it began refuses (B560).
//! Does not own: row stages, the root counter (flow_custody), the hard meter.
//! Invariants: Session Q ⇒ stopping ∧ deadline; any Q is sticky until the
//! owner is dropped; `quarantined()` (any scope) ⇒ the session ends Incomplete.
//! Threading (F7): every Flow raise and every release Turn run synchronously
//! on the session driver thread, so `flow_raised` counts observations (a held
//! row may be re-observed per pass) and a Turn's epoch can only move by a
//! raise inside that same Turn (B560). It is not a count of distinct rows.
//! Entry: `SessionControl::{quarantine_quiet, quarantine_flow}`. Tests:
//! flow_custody/transaction/tests/soft_q.rs and transaction/tests/release.rs.
use super::*;

/// Session Q. Written only by this module's scoped entries.
#[derive(Default)]
pub(super) struct SessionQ {
    session: bool,
    flow_raised: u64,
}

impl SessionLedger {
    /// Any scope: the session holds a quarantine and ends Incomplete.
    pub(super) fn quarantined(&self) -> bool {
        self.q.session || self.q.flow_raised != 0
    }

    /// Session scope only: refuses every release and has stopped the session.
    pub(super) fn session_quarantined(&self) -> bool {
        self.q.session
    }

    /// Monotone count of Flow-scope raises; a release Turn compares it.
    pub(super) fn flow_q_epoch(&self) -> u64 {
        self.q.flow_raised
    }

    /// A release Turn may proceed: no Session Q and no Flow Q since `epoch`.
    pub(super) fn release_q_clear(&self, epoch: u64) -> bool {
        !self.q.session && self.q.flow_raised == epoch
    }

    /// Session scope under an already held ledger. The caller's entry also
    /// sets `stopping`; this fixes the bounded deadline in the same step.
    fn raise_session(&mut self, parent: Option<tokio::time::Instant>) {
        let proposed = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
        self.q.session = true;
        self.shutdown_deadline = Some(
            parent
                .into_iter()
                .chain(self.shutdown_deadline)
                .fold(proposed, std::cmp::min),
        );
    }
}

impl SessionControl {
    pub(super) fn parent_stop_deadline(&self) -> Option<tokio::time::Instant> {
        self.parent_deadline.get().and_then(|deadline| {
            *deadline
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        })
    }

    /// Session scope: root-level or uncertain custody. Visible to `state()`.
    pub(super) fn quarantine_quiet(&self) {
        let parent = self.parent_stop_deadline();
        let mut ledger = self.ledger();
        self.quarantine_session_locked(&mut ledger, parent);
    }

    /// Session scope while the caller already holds the ledger.
    pub(super) fn quarantine_session_locked(
        &self,
        ledger: &mut SessionLedger,
        parent: Option<tokio::time::Instant>,
    ) {
        ledger.raise_session(parent);
        self.stop.stopping.store(true, Ordering::Release);
        if let Some(meter) = &self.app_meter {
            meter.quarantine();
        }
    }

    /// Session scope at forced root drop (exclusive access, no lock).
    pub(super) fn quarantine_session_owned(&mut self) {
        let parent = self.parent_stop_deadline();
        self.stop.stopping.store(true, Ordering::Release);
        if let Some(meter) = &self.app_meter {
            meter.quarantine();
        }
        self.ledger
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .raise_session(parent);
    }

    /// Flow scope: only `FlowTransaction::quarantine_flow` and the root
    /// registration hold a [`FlowQRow`], i.e. a row marked under the root lock.
    pub(in crate::client_tun::compatibility_runtime) fn quarantine_flow(&self, _row: super::flow_custody::FlowQRow) {
        let mut ledger = self.ledger();
        ledger.q.flow_raised = ledger.q.flow_raised.saturating_add(1);
    }
}

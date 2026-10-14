//! Exact controller/output inventory construction and zero predicates.
//! Owns only value aggregation; resource/transaction Authority stays in owner.
//! Parent builders preserve their owner-only scope; tests remain in owner/tests.

use super::*;
use crate::owned_upstream::owner_target::error_inventory::ErrorAuditFailure;

impl QuinnOwnerOutputRootInventory {
    pub(super) fn from_sources(
        drain: Option<OwnerOutputDrainInventory>,
        drain_ledgers: u8,
        drain_audit_failed: bool,
        phase_markers: u8,
        continuation_markers: u8,
        ordinary_transactions: u8,
        continuation_transactions: u8,
        phase_drain_consistent: bool,
        quarantined: bool,
        lifecycle: OwnerOutputLifecycleInventory,
        rejected_page: Option<OwnerOutputAdmissionInventory>,
    ) -> Self {
        Self {
            drain,
            drain_ledgers,
            drain_audit_failed,
            phase_markers,
            continuation_markers,
            ordinary_transactions,
            continuation_transactions,
            recovery_transactions: 0,
            terminal_transactions: 0,
            terminal_residuals: 0,
            terminal_stamps: 0,
            phase_drain_consistent,
            quarantined,
            recovery_quarantined: false,
            terminal_quarantined: false,
            controller_failures: 0,
            controller_error_payload: Err(ErrorAuditFailure::Invariant),
            controller_cold_audit_failed: false,
            controller_cold_exact_zero: false,
            controller_snapshot: None,
            controller_terminal_correlation_valid: false,
            standby_terminal_correlation_valid: true,
            lifecycle,
            rejected_page,
        }
    }

    pub(super) fn with_recovery_service(mut self, transactions: u8, quarantined: bool) -> Self {
        self.recovery_transactions = transactions;
        self.recovery_quarantined = quarantined;
        self
    }

    pub(super) fn with_controller_error_payload(mut self, payload: TargetErrorColdAudit) -> Self {
        self.controller_cold_audit_failed |= payload.is_err();
        self.controller_error_payload = payload;
        self
    }

    pub(crate) const fn controller_error_payload(self) -> TargetErrorColdAudit {
        self.controller_error_payload
    }

    pub(super) fn with_controller_terminal(
        mut self,
        snapshot: OwnerSessionControllerSnapshot,
        cold_exact_zero: bool,
        cold_audit_failed: bool,
        controller_failures: u8,
        terminal_transactions: u8,
        terminal_residuals: u8,
        terminal_stamps: u8,
        terminal_quarantined: bool,
        controller_terminal_correlation_valid: bool,
    ) -> Self {
        self.controller_snapshot = Some(snapshot);
        self.controller_cold_exact_zero = cold_exact_zero;
        self.controller_cold_audit_failed = cold_audit_failed;
        self.controller_failures = controller_failures;
        self.terminal_transactions = terminal_transactions;
        self.terminal_residuals = terminal_residuals;
        self.terminal_stamps = terminal_stamps;
        self.terminal_quarantined = terminal_quarantined;
        self.controller_terminal_correlation_valid = controller_terminal_correlation_valid;
        self
    }

    pub(super) fn with_standby_terminal_correlation(mut self, valid: bool) -> Self {
        self.standby_terminal_correlation_valid = valid;
        self
    }

    pub(crate) const fn drain(self) -> Option<OwnerOutputDrainInventory> {
        self.drain
    }

    pub(crate) const fn drain_ledgers(self) -> u8 {
        self.drain_ledgers
    }

    pub(crate) const fn drain_audit_failed(self) -> bool {
        self.drain_audit_failed
    }

    pub(crate) const fn phase_markers(self) -> u8 {
        self.phase_markers
    }

    pub(crate) const fn continuation_markers(self) -> u8 {
        self.continuation_markers
    }

    pub(crate) const fn ordinary_transactions(self) -> u8 {
        self.ordinary_transactions
    }

    pub(crate) const fn continuation_transactions(self) -> u8 {
        self.continuation_transactions
    }

    pub(crate) const fn recovery_transactions(self) -> u8 {
        self.recovery_transactions
    }

    pub(crate) const fn terminal_transactions(self) -> u8 {
        self.terminal_transactions
    }

    pub(crate) const fn terminal_residuals(self) -> u8 {
        self.terminal_residuals
    }

    pub(crate) const fn terminal_stamps(self) -> u8 {
        self.terminal_stamps
    }

    pub(crate) const fn phase_drain_consistent(self) -> bool {
        self.phase_drain_consistent
    }

    pub(crate) const fn quarantined(self) -> bool {
        self.quarantined
    }

    pub(crate) const fn recovery_quarantined(self) -> bool {
        self.recovery_quarantined
    }

    pub(crate) const fn terminal_quarantined(self) -> bool {
        self.terminal_quarantined
    }

    pub(crate) const fn controller_failures(self) -> u8 {
        self.controller_failures
    }

    pub(crate) const fn controller_cold_audit_failed(self) -> bool {
        self.controller_cold_audit_failed
    }

    pub(crate) const fn controller_snapshot(self) -> Option<OwnerSessionControllerSnapshot> {
        self.controller_snapshot
    }

    pub(crate) const fn controller_terminal_correlation_valid(self) -> bool {
        self.controller_terminal_correlation_valid
    }

    pub(crate) const fn standby_terminal_correlation_valid(self) -> bool {
        self.standby_terminal_correlation_valid
    }

    pub(crate) const fn installed_lifecycle_actions(self) -> usize {
        self.lifecycle.pending_actions()
    }

    pub(crate) const fn lifecycle(self) -> OwnerOutputLifecycleInventory {
        self.lifecycle
    }

    pub(crate) const fn rejected_page(self) -> Option<OwnerOutputAdmissionInventory> {
        self.rejected_page
    }

    pub(crate) const fn is_output_exact_zero(self) -> bool {
        self.drain_ledgers == 1
            && !self.drain_audit_failed
            && self.phase_markers == 0
            && self.continuation_markers == 0
            && self.ordinary_transactions == 0
            && self.continuation_transactions == 0
            && self.recovery_transactions == 0
            && self.phase_drain_consistent
            && !self.quarantined
            && !self.recovery_quarantined
            && matches!(self.drain, Some(inventory) if inventory.is_exact_zero())
            && self.lifecycle.is_exact_zero()
            && self.rejected_page.is_none()
    }

    /// Exact zero for the converged controller/output/terminal subroot only.
    /// Actor/FIFO/ACK/replay/timer/endpoint/session authority is deliberately
    /// outside this predicate and must be summed by the future physical root.
    pub(crate) const fn is_controller_output_terminal_exact_zero(self) -> bool {
        self.is_output_exact_zero()
            && self.controller_failures == 0
            && matches!(self.controller_error_payload, Ok(payload) if payload.is_exact_zero())
            && !self.controller_cold_audit_failed
            && self.controller_cold_exact_zero
            && self.terminal_transactions == 0
            && self.terminal_residuals == 0
            && self.terminal_stamps == 0
            && !self.terminal_quarantined
            && self.controller_terminal_correlation_valid
            && self.standby_terminal_correlation_valid
            && matches!(self.controller_snapshot, Some(snapshot) if controller_snapshot_is_live_exact_zero(snapshot))
    }
}

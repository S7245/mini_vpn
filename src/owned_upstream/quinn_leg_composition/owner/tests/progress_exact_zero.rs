//! Characterizes controller snapshot exact-zero classification.
//!
//! Owns the empty snapshot fixture and failed/expired phase regression below.
//! It does not own production terminal or physical-root lifecycle behavior.

use super::*;

fn empty_controller_snapshot() -> OwnerSessionControllerSnapshot {
    OwnerSessionControllerSnapshot {
        progress_entries: 0,
        session_expired: false,
        failed: false,
        retained_outputs: 0,
        retained_observations: 0,
        pending_continuations: 0,
        retained_continuations: 0,
        pending_recoveries: 0,
        pending_attach_statuses: 0,
        retained_recoveries: 0,
        retained_recovery_cursors: 0,
        retained_recovery_outputs: 0,
        retained_recovery_observations: 0,
        retained_recovery_attached: 0,
        recovery_needs_acceptance: false,
        recovery_needs_execute: false,
        recovery_needs_publish: false,
        recovery_handoff_ready: false,
        recovery_needs_terminal_discard: false,
        recovery_terminal_quarantined: 0,
        pending_legless_high_waters: 0,
        attach_status_needs_enqueue: false,
        attach_status_needs_delivery: false,
    }
}

#[test]
fn controller_exact_zero_rejects_failed_and_expired_phases() {
    let snapshot = empty_controller_snapshot();
    assert!(controller_snapshot_is_live_exact_zero(snapshot));

    let mut failed = snapshot;
    failed.failed = true;
    assert!(!controller_snapshot_is_live_exact_zero(failed));

    let mut expired = snapshot;
    expired.session_expired = true;
    assert!(!controller_snapshot_is_live_exact_zero(expired));
}

#[test]
fn controller_exact_zero_rejects_nonzero_progress_entries() {
    let mut snapshot = empty_controller_snapshot();
    snapshot.progress_entries = 1;

    assert!(!controller_snapshot_is_live_exact_zero(snapshot));
}

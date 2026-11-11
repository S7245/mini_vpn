//! D166: the single owner of one Flow's close-tail deadline.
//!
//! Owns: the flow row's `stop_deadline` value and its four transitions —
//! `open` (close, 15s tail), `rearm` (App progress, max), `grant` (the drive
//! expiry's one abort/A1 extension) and `limit` (every external clamp: Session
//! parent/shutdown limits, uplink/cold/disposal settlement).
//! Does not own: the expiry decision (`FlowTransaction::drive`), the provider
//! deadline (`TcpOpenAttempt`) or the Session clocks (`SessionControl`).
//! Invariant: no writer outside this type exists (read-only `Deref`); a grant
//! is never undone by a limit at or before the expiry it answered, while a
//! later limit still shortens it, and App progress never re-arms past an
//! applied limit (F4, fail-closed).
//! Model: docs/current/n4-close-tail-model-20261110.md. Tests:
//! `transaction/tests/close_tail.rs`.
use std::time::Instant;

#[derive(Clone, Copy, Default)]
pub(in crate::client_tun::compatibility_runtime::flow_custody) struct StopDeadline {
    at: Option<Instant>,
    granted_against: Option<Instant>,
    /// The earliest applied (not consumed) external limit.
    limit: Option<Instant>,
}

impl StopDeadline {
    /// The close starts the tail; an open tail is never restarted.
    pub(super) fn open(&mut self, at: Instant) {
        self.at.get_or_insert(at);
    }

    /// App progress re-arms an open tail (never shortens it).
    pub(super) fn rearm(&mut self, at: Instant) {
        if let Some(deadline) = &mut self.at {
            *deadline = (*deadline).max(at).min(self.limit.unwrap_or(at));
        }
    }

    /// The drive expiry's one grant (abort or A1 extension) answers the
    /// deadline that just expired.
    pub(super) fn grant(&mut self, until: Instant) {
        self.granted_against = self.at;
        // A limit at or before the answered expiry is consumed by this grant.
        self.limit = self.limit.filter(|limit| self.at.is_some_and(|at| *limit > at));
        self.at = Some(until);
    }

    /// An external limit. Before a grant it only shortens; after a grant a
    /// limit at or before the answered expiry is already consumed.
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn limit(
        &mut self,
        limit: Instant,
    ) {
        if self
            .granted_against
            .is_some_and(|answered| limit <= answered)
        {
            return;
        }
        self.limit = Some(self.limit.map_or(limit, |old| old.min(limit)));
        self.at = Some(self.at.map_or(limit, |old| old.min(limit)));
    }
}

impl std::ops::Deref for StopDeadline {
    type Target = Option<Instant>;
    fn deref(&self) -> &Option<Instant> {
        &self.at
    }
}

impl PartialEq<Option<Instant>> for StopDeadline {
    fn eq(&self, other: &Option<Instant>) -> bool {
        self.at == *other
    }
}

impl std::fmt::Debug for StopDeadline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.at.fmt(f)
    }
}

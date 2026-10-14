//! Controller-local observed turns and original terminal birth windows.
//!
//! Owns cold Z backing, fixed policy, birth indexing and known observation
//! settlement. C's original aborted-page slot also owns returned pages until
//! progress commits. Returned Target errors share that slot through settlement;
//! original Supervisor/Executor journals precede this boundary. Exact action
//! installation and original expiry retain their action until output owns the
//! returned page. The complete timed consumer remains draft pending its matrix.
//! Private tests cover deferred birth and exact policy rejection; parent tests
//! cover progress, ACK, continuation and existing failure paths.

use super::*;

#[cfg(test)]
#[path = "tests/terminal_grace.rs"]
mod tests;

use crate::owned_upstream::owner_output_drain::{
    OwnerOutputLifecycleDrive, OwnerOutputLifecycleInstalledSlot, OwnerOutputLifecycleKind,
    OwnerOutputLifecyclePort, OwnerOutputLifecycleSettlement, OwnerTerminalTimerRegistration,
};
use crate::owned_upstream::owner_target::OwnerTargetFlowDirectionTerminal;
use std::alloc::Layout;
use std::time::Duration;

impl<T: TargetIo, C> OwnerSessionControllerCore<T, C> {
    // Inspect only existing scalar metadata through the original rollback
    // transaction. No terminal getter, raw action or new callback seam escapes.
    fn terminal_action_position(
        &mut self,
        port: &mut OwnerOutputLifecyclePort,
    ) -> Result<Option<usize>, TerminalGraceError> {
        let mut kind = None;
        let inspected = port.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            kind = transaction.kind();
            transaction.retain()
        });
        match inspected {
            OwnerOutputLifecycleDrive::Inert => return Ok(None),
            OwnerOutputLifecycleDrive::Retained { .. } => {}
            _ => return Err(TerminalGraceError::Invariant),
        }
        let Some(OwnerOutputLifecycleKind::TerminalGraceStarted {
            session_id,
            flow_id,
            direction,
        }) = kind
        else {
            return Ok(None);
        };
        let OwnerTerminalGraceState::Bound(ledger) = &self.terminal_grace else {
            return Err(TerminalGraceError::Invariant);
        };
        let mut cursor = ledger.root;
        while let Some(index) = cursor {
            let slot = &ledger.slots[index];
            let TerminalSlotState::AwaitingOutput { fact, .. } = &slot.state else {
                return Err(TerminalGraceError::Invariant);
            };
            match flow_id.cmp(&fact.flow_id()) {
                std::cmp::Ordering::Less => cursor = slot.left,
                std::cmp::Ordering::Greater => cursor = slot.right,
                std::cmp::Ordering::Equal => {
                    if fact.session_id() != session_id || fact.direction() != direction {
                        return Err(TerminalGraceError::Invariant);
                    }
                    return Ok(Some(index));
                }
            }
        }
        Err(TerminalGraceError::Invariant)
    }

    pub(in crate::owned_upstream) fn install_terminal_action(
        &mut self,
        port: &mut OwnerOutputLifecyclePort,
    ) -> Result<bool, OwnerSessionControllerError> {
        let outcome = (|| {
            let Some(index) = self.terminal_action_position(port)? else {
                return Ok(false);
            };
            let OwnerTerminalGraceState::Bound(ledger) = &mut self.terminal_grace else {
                return Err(TerminalGraceError::Invariant);
            };
            let entry = &mut ledger.slots[index];
            let TerminalSlotState::AwaitingOutput { fact, .. } = &entry.state else {
                return Err(TerminalGraceError::Invariant);
            };
            if !matches!(entry.lifecycle, TerminalLifecycle::Awaiting) {
                return Err(TerminalGraceError::Invariant);
            }
            entry.lifecycle =
                TerminalLifecycle::Installing(OwnerOutputLifecycleInstalledSlot::new());
            let TerminalLifecycle::Installing(slot) = &mut entry.lifecycle else {
                return Err(TerminalGraceError::Invariant);
            };
            let installed = port.install_exact_terminal(*fact.terminal(), slot);
            if !matches!(
                installed,
                OwnerOutputLifecycleDrive::Committed {
                    settlement: OwnerOutputLifecycleSettlement::Install
                }
            ) {
                return Err(TerminalGraceError::Invariant);
            }
            // Both exact owners have committed. Only an infallible local move
            // remains; unknown earlier execution retains Installing and its slot.
            let phase = std::mem::replace(&mut entry.lifecycle, TerminalLifecycle::Awaiting);
            if let TerminalLifecycle::Installing(slot) = phase {
                entry.lifecycle = TerminalLifecycle::Installed {
                    slot,
                    expiry: TerminalExpiry::Waiting,
                };
                Ok(true)
            } else {
                entry.lifecycle = phase;
                Err(TerminalGraceError::Invariant)
            }
        })();
        outcome.map_err(|source| {
            self.phase = OwnerSessionControllerPhase::Failed;
            OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::PostCommitProgress(
                OwnerSessionProgressError::TerminalGrace(source),
            ))
        })
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(in crate::owned_upstream) enum TerminalGraceError {
    #[error("terminal grace clock rejected the original window: {0}")]
    Clock(#[from] SessionClockError),
    #[error("checked construction cannot import untimed live terminals")]
    UntimedTerminal,
    #[error("terminal grace backing layout is not representable")]
    Layout,
    #[error("terminal grace backing could not be reserved exactly")]
    Allocation,
    #[error("terminal fact, reservation or birth ordering is inconsistent")]
    Invariant,
}
pub(super) enum OwnerTerminalGraceState {
    Bound(OwnerTerminalGraceLedger),
    #[cfg(test)]
    LegacyUntracked,
}
pub(super) struct OwnerTerminalGraceLedger {
    // The enclosing C owns the same identity-checked Executor for this ledger.
    interval: Duration,
    slots: Vec<TerminalSlot>,
    free: Option<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    root: Option<usize>,
    count: usize,
    turn: Option<TerminalTurn>,
    #[cfg(test)]
    panic_before_returned_page_progress: bool,
}
struct TerminalTurn {
    window: TerminalWindow,
    reserved: Option<usize>,
}
#[derive(Clone, Copy)]
struct TerminalWindow {
    observed_at: SimTime,
    deadline: SimTime,
    absolute: tokio::time::Instant,
}
struct TerminalSlot {
    state: TerminalSlotState,
    lifecycle: TerminalLifecycle,
    next: Option<usize>,
    left: Option<usize>,
    right: Option<usize>,
    height: usize,
}
enum TerminalSlotState {
    Vacant,
    AwaitingOutput {
        fact: OwnerTargetFlowDirectionTerminal,
        window: TerminalWindow,
    },
}
enum TerminalLifecycle {
    Awaiting,
    Installing(OwnerOutputLifecycleInstalledSlot),
    Installed {
        slot: OwnerOutputLifecycleInstalledSlot,
        expiry: TerminalExpiry,
    },
}
enum TerminalExpiry {
    Waiting,
    Entered,
    Returned,
}

impl<T: TargetIo> OwnerSessionControllerCore<T, CheckedOwnerControllerCapacity> {
    fn terminal_expiry_admissible(&self) -> bool {
        matches!(self.phase, OwnerSessionControllerPhase::Active)
            && !self.terminal_grace.has_turn()
            && self.recovery.is_none()
            && self.pending_continuation.is_none()
            && matches!(self.capacity.receipt, OwnerReadyReceipt::Vacant)
    }

    pub(in crate::owned_upstream) fn terminal_timer_requested(
        &self,
        timer: &OwnerTerminalTimerRegistration,
    ) -> bool {
        if !self.terminal_expiry_admissible() {
            return false;
        }
        let OwnerTerminalGraceState::Bound(ledger) = &self.terminal_grace else {
            return false;
        };
        let Some(index) = ledger.head else {
            return false;
        };
        let entry = &ledger.slots[index];
        match (&entry.state, &entry.lifecycle) {
            (
                TerminalSlotState::AwaitingOutput { fact, window },
                TerminalLifecycle::Installed {
                    expiry: TerminalExpiry::Waiting,
                    ..
                },
            ) => timer.requested(*fact.terminal(), window.absolute),
            _ => false,
        }
    }

    pub(in crate::owned_upstream) fn poll_terminal_timer(
        &mut self,
        timer: &mut OwnerTerminalTimerRegistration,
        context: &mut std::task::Context<'_>,
    ) -> Result<std::task::Poll<()>, OwnerSessionControllerError> {
        if !self.terminal_expiry_admissible() {
            return Ok(std::task::Poll::Pending);
        }
        let (terminal, window) = self.terminal_expiry_head()?;
        let now = self.clock.now().map_err(|source| {
            OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Clock(source))
        })?;
        timer
            .poll_original(terminal, window.absolute, now >= window.deadline, context)
            .map_err(|()| Self::terminal_expiry_invariant())
    }

    fn terminal_expiry_invariant() -> OwnerSessionControllerError {
        OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::PostCommitProgress(
            OwnerSessionProgressError::TerminalGrace(TerminalGraceError::Invariant),
        ))
    }

    fn terminal_expiry_head(
        &self,
    ) -> Result<(TerminalGrace, TerminalWindow), OwnerSessionControllerError> {
        let OwnerTerminalGraceState::Bound(ledger) = &self.terminal_grace else {
            return Err(Self::terminal_expiry_invariant());
        };
        let Some(index) = ledger.head else {
            return Err(Self::terminal_expiry_invariant());
        };
        match (&ledger.slots[index].state, &ledger.slots[index].lifecycle) {
            (
                TerminalSlotState::AwaitingOutput { fact, window },
                TerminalLifecycle::Installed { slot, .. },
            ) if slot.inventory().installed_actions() == 1 => Ok((*fact.terminal(), *window)),
            _ => Err(Self::terminal_expiry_invariant()),
        }
    }

    fn set_terminal_expiry(&mut self, phase: TerminalExpiry) {
        if let OwnerTerminalGraceState::Bound(ledger) = &mut self.terminal_grace {
            if let Some(index) = ledger.head {
                if let TerminalLifecycle::Installed { expiry, .. } =
                    &mut ledger.slots[index].lifecycle
                {
                    *expiry = phase;
                }
            }
        }
    }

    pub(in crate::owned_upstream) fn expire_original_terminal(
        &mut self,
        timer: &OwnerTerminalTimerRegistration,
    ) -> Result<OwnerSessionDrive, OwnerSessionControllerError> {
        if !self.terminal_expiry_admissible() {
            return Err(Self::terminal_expiry_invariant());
        }
        let (terminal, original_window) = self.terminal_expiry_head()?;
        if !timer.holds_due(terminal) {
            return Err(Self::terminal_expiry_invariant());
        }
        let now = self.begin_terminal_grace_turn().map_err(|source| {
            OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Clock(source))
        })?;
        if now < original_window.deadline {
            self.finish_terminal_grace_turn();
            return Err(Self::terminal_expiry_invariant());
        }
        // Original fact and installed action remain root-held throughout the
        // reducer/Target call. Interruption never restores an unentered timer.
        self.set_terminal_expiry(TerminalExpiry::Entered);
        match self
            .executor
            .apply_event_observed(SessionEvent::TerminalGraceExpired { terminal })
        {
            Ok(observed) => {
                let drive =
                    self.finish_observed_page(observed, now, OwnerContinuationOrigin::Local)?;
                self.set_terminal_expiry(TerminalExpiry::Returned);
                Ok(drive)
            }
            Err(error @ OwnerTargetError::RejectedEvent { .. }) => {
                if matches!(&error, OwnerTargetError::RejectedEvent { event, .. }
                    if matches!(event.as_ref(), SessionEvent::TerminalGraceExpired { terminal: returned } if *returned == terminal))
                {
                    self.finish_terminal_grace_turn();
                    self.set_terminal_expiry(TerminalExpiry::Waiting);
                } else {
                    self.phase = OwnerSessionControllerPhase::Failed;
                }
                // Preserve the actual carrying rejection, not just its kind.
                Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::Target(error),
                ))
            }
            Err(error) => Err(self.finish_target_error(error, now, OwnerContinuationOrigin::Local)),
        }
    }

    /// Called only after the returned original page is owned by output.
    /// Every fallible correlation check precedes the fused AVL/free-list move.
    pub(in crate::owned_upstream) fn settle_terminal_expiry_output(
        &mut self,
        timer: &mut OwnerTerminalTimerRegistration,
    ) -> Result<(), OwnerSessionControllerError> {
        let (terminal, _) = self.terminal_expiry_head()?;
        if !timer.holds_due(terminal) {
            return Err(Self::terminal_expiry_invariant());
        }
        let OwnerTerminalGraceState::Bound(ledger) = &mut self.terminal_grace else {
            return Err(Self::terminal_expiry_invariant());
        };
        let (Some(index), Some(root)) = (ledger.head, ledger.root) else {
            return Err(Self::terminal_expiry_invariant());
        };
        if ledger.count == 0
            || !matches!(
                ledger.slots[index].lifecycle,
                TerminalLifecycle::Installed {
                    expiry: TerminalExpiry::Returned,
                    ..
                }
            )
            || !ledger.contains_exact(root, index, terminal.flow_id())
        {
            return Err(Self::terminal_expiry_invariant());
        }
        timer
            .settle_original(terminal)
            .map_err(|()| Self::terminal_expiry_invariant())?;
        ledger.root = ledger.remove(root, terminal.flow_id());
        ledger.head = ledger.slots[index].next;
        if ledger.head.is_none() {
            ledger.tail = None;
        }
        ledger.slots[index] = TerminalSlot {
            state: TerminalSlotState::Vacant,
            lifecycle: TerminalLifecycle::Awaiting,
            next: ledger.free,
            left: None,
            right: None,
            height: 1,
        };
        ledger.free = Some(index);
        ledger.count -= 1;
        Ok(())
    }
}

impl OwnerTerminalGraceLedger {
    fn contains_exact(&self, mut root: usize, expected: usize, key: SessionFlowId) -> bool {
        loop {
            let node = &self.slots[root];
            let TerminalSlotState::AwaitingOutput { fact, .. } = &node.state else {
                return false;
            };
            let next = match key.cmp(&fact.flow_id()) {
                std::cmp::Ordering::Equal => return root == expected,
                std::cmp::Ordering::Less => node.left,
                std::cmp::Ordering::Greater => node.right,
            };
            match next {
                Some(next) => root = next,
                None => return false,
            }
        }
    }

    fn rebalance(&mut self, root: usize) -> usize {
        self.refresh(root);
        let left = self.slots[root].left;
        let right = self.slots[root].right;
        if self.height(left) > self.height(right) + 1 {
            if let Some(left) = left {
                if self.height(self.slots[left].right) > self.height(self.slots[left].left) {
                    if let Some(inner) = self.slots[left].right {
                        self.slots[root].left = Some(self.rotate_left(left, inner));
                    }
                }
                if let Some(left) = self.slots[root].left {
                    return self.rotate_right(root, left);
                }
            }
        } else if self.height(right) > self.height(left) + 1 {
            if let Some(right) = right {
                if self.height(self.slots[right].left) > self.height(self.slots[right].right) {
                    if let Some(inner) = self.slots[right].left {
                        self.slots[root].right = Some(self.rotate_right(right, inner));
                    }
                }
                if let Some(right) = self.slots[root].right {
                    return self.rotate_left(root, right);
                }
            }
        }
        root
    }

    fn take_min(&mut self, root: usize) -> (usize, Option<usize>) {
        let Some(left) = self.slots[root].left else {
            return (root, self.slots[root].right);
        };
        let (minimum, remainder) = self.take_min(left);
        self.slots[root].left = remainder;
        (minimum, Some(self.rebalance(root)))
    }

    fn remove(&mut self, root: usize, key: SessionFlowId) -> Option<usize> {
        let TerminalSlotState::AwaitingOutput { fact, .. } = &self.slots[root].state else {
            return Some(root);
        };
        match key.cmp(&fact.flow_id()) {
            std::cmp::Ordering::Less => {
                if let Some(left) = self.slots[root].left {
                    self.slots[root].left = self.remove(left, key);
                }
            }
            std::cmp::Ordering::Greater => {
                if let Some(right) = self.slots[root].right {
                    self.slots[root].right = self.remove(right, key);
                }
            }
            std::cmp::Ordering::Equal => {
                let (left, right) = (self.slots[root].left, self.slots[root].right);
                let Some(right) = right else { return left };
                let (successor, rest) = self.take_min(right);
                self.slots[successor].left = left;
                self.slots[successor].right = rest;
                return Some(self.rebalance(successor));
            }
        }
        Some(self.rebalance(root))
    }
}

impl OwnerTerminalGraceState {
    pub(super) fn checked<T: TargetIo>(
        executor: &OwnerTargetExecutor<T>,
        clock: &SessionMonotonicClock,
        interval: Duration,
    ) -> Result<Self, TerminalGraceError> {
        clock.deadline_window(interval)?;
        let snapshot = executor.snapshot().session.session;
        if snapshot.terminal_tombstones() != 0 {
            return Err(TerminalGraceError::UntimedTerminal);
        }
        let z = executor
            .installed_session_config()
            .max_terminal_tombstones();
        let backing = Layout::array::<TerminalSlot>(z).map_err(|_| TerminalGraceError::Layout)?;
        Layout::new::<OwnerSessionController<T>>()
            .extend(backing)
            .map_err(|_| TerminalGraceError::Layout)?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(z)
            .map_err(|_| TerminalGraceError::Allocation)?;
        if slots.capacity() != z {
            return Err(TerminalGraceError::Allocation);
        }
        for i in 0..z {
            slots.push(TerminalSlot {
                state: TerminalSlotState::Vacant,
                lifecycle: TerminalLifecycle::Awaiting,
                next: (i + 1 < z).then_some(i + 1),
                left: None,
                right: None,
                height: 1,
            });
        }
        Ok(Self::Bound(OwnerTerminalGraceLedger {
            interval,
            slots,
            free: (z != 0).then_some(0),
            head: None,
            tail: None,
            root: None,
            count: 0,
            turn: None,
            #[cfg(test)]
            panic_before_returned_page_progress: false,
        }))
    }
    pub(super) fn fact_count(&self) -> usize {
        match self {
            Self::Bound(ledger) => ledger.count,
            #[cfg(test)]
            Self::LegacyUntracked => 0,
        }
    }
    pub(super) fn has_turn(&self) -> bool {
        match self {
            Self::Bound(ledger) => ledger.turn.is_some(),
            #[cfg(test)]
            Self::LegacyUntracked => false,
        }
    }
    fn finish_turn(&mut self) {
        match self {
            Self::Bound(ledger) => ledger.turn = None,
            #[cfg(test)]
            Self::LegacyUntracked => {}
        }
    }
}

impl OwnerTerminalGraceLedger {
    fn preflight(
        &self,
        observations: &OwnerTargetReducerObservations,
    ) -> Result<Option<(usize, TerminalWindow)>, TerminalGraceError> {
        let Some(fact) = observations.flow_terminal() else {
            return Ok(None);
        };
        let turn = self.turn.as_ref().ok_or(TerminalGraceError::Invariant)?;
        let window = turn.window;
        let free = turn.reserved.ok_or(TerminalGraceError::Invariant)?;
        if self.free != Some(free)
            || !matches!(
                self.slots.get(free).map(|slot| &slot.state),
                Some(TerminalSlotState::Vacant)
            )
            || self.count >= self.slots.len()
        {
            return Err(TerminalGraceError::Invariant);
        }
        if let Some(tail) = self.tail {
            let Some(TerminalSlot {
                state: TerminalSlotState::AwaitingOutput { window: old, .. },
                ..
            }) = self.slots.get(tail)
            else {
                return Err(TerminalGraceError::Invariant);
            };
            if old.observed_at > window.observed_at || old.deadline > window.deadline {
                return Err(TerminalGraceError::Invariant);
            }
        }
        let mut cursor = self.root;
        while let Some(index) = cursor {
            let Some(slot) = self.slots.get(index) else {
                return Err(TerminalGraceError::Invariant);
            };
            let TerminalSlotState::AwaitingOutput { fact: existing, .. } = &slot.state else {
                return Err(TerminalGraceError::Invariant);
            };
            cursor = match fact.flow_id().cmp(&existing.flow_id()) {
                std::cmp::Ordering::Less => slot.left,
                std::cmp::Ordering::Greater => slot.right,
                std::cmp::Ordering::Equal => return Err(TerminalGraceError::Invariant),
            };
        }
        Ok(Some((free, window)))
    }
    fn height(&self, node: Option<usize>) -> usize {
        node.map_or(0, |index| self.slots[index].height)
    }
    fn refresh(&mut self, node: usize) {
        self.slots[node].height = 1 + self
            .height(self.slots[node].left)
            .max(self.height(self.slots[node].right));
    }
    fn rotate_left(&mut self, node: usize, right: usize) -> usize {
        self.slots[node].right = self.slots[right].left;
        self.slots[right].left = Some(node);
        self.refresh(node);
        self.refresh(right);
        right
    }
    fn rotate_right(&mut self, node: usize, left: usize) -> usize {
        self.slots[node].left = self.slots[left].right;
        self.slots[left].right = Some(node);
        self.refresh(node);
        self.refresh(left);
        left
    }
    fn insert(&mut self, root: Option<usize>, fresh: usize, key: SessionFlowId) -> usize {
        let Some(root) = root else {
            return fresh;
        };
        let TerminalSlotState::AwaitingOutput { fact, .. } = &self.slots[root].state else {
            return root;
        }; // preflight visits only occupied nodes.
        if key < fact.flow_id() {
            self.slots[root].left = Some(self.insert(self.slots[root].left, fresh, key));
        } else {
            self.slots[root].right = Some(self.insert(self.slots[root].right, fresh, key));
        }
        self.refresh(root);
        let left = self.slots[root].left;
        let right = self.slots[root].right;
        if self.height(left) > self.height(right) + 1 {
            if let Some(left) = left {
                if self.height(self.slots[left].right) > self.height(self.slots[left].left) {
                    if let Some(inner) = self.slots[left].right {
                        self.slots[root].left = Some(self.rotate_left(left, inner));
                    }
                }
                if let Some(left) = self.slots[root].left {
                    return self.rotate_right(root, left);
                }
            }
        } else if self.height(right) > self.height(left) + 1 {
            if let Some(right) = right {
                if self.height(self.slots[right].left) > self.height(self.slots[right].right) {
                    if let Some(inner) = self.slots[right].left {
                        self.slots[root].right = Some(self.rotate_right(right, inner));
                    }
                }
                if let Some(right) = self.slots[root].right {
                    return self.rotate_left(root, right);
                }
            }
        }
        root
    }
    fn commit(
        &mut self,
        slot: usize,
        window: TerminalWindow,
        fact: OwnerTargetFlowDirectionTerminal,
    ) {
        let key = fact.flow_id();
        self.free = self.slots[slot].next;
        self.slots[slot].state = TerminalSlotState::AwaitingOutput { fact, window };
        self.slots[slot].next = None;
        if let Some(tail) = self.tail {
            self.slots[tail].next = Some(slot);
        } else {
            self.head = Some(slot);
        }
        self.tail = Some(slot);
        self.root = Some(self.insert(self.root, slot, key));
        self.count += 1;
    }
}

impl<T: TargetIo, C> OwnerSessionControllerCore<T, C> {
    // All callers have passed original admission before this same-clock sample.
    // A retained window blocks their reentry; output deferral never updates an entry.
    pub(super) fn begin_terminal_grace_turn(&mut self) -> Result<SimTime, SessionClockError> {
        match &mut self.terminal_grace {
            OwnerTerminalGraceState::Bound(ledger) => {
                let (observed_at, deadline, absolute) =
                    self.clock.deadline_window(ledger.interval)?;
                ledger.turn = Some(TerminalTurn {
                    window: TerminalWindow {
                        observed_at,
                        deadline,
                        absolute,
                    },
                    reserved: ledger.free,
                });
                Ok(observed_at)
            }
            #[cfg(test)]
            OwnerTerminalGraceState::LegacyUntracked => self.clock.now(),
        }
    }
    pub(super) fn finish_terminal_grace_turn(&mut self) {
        self.terminal_grace.finish_turn();
    }
    fn retain_terminal_fact(
        &mut self,
        observations: OwnerTargetReducerObservations,
        prepared: Option<(usize, TerminalWindow)>,
    ) {
        if let (OwnerTerminalGraceState::Bound(ledger), Some((slot, window))) =
            (&mut self.terminal_grace, prepared)
        {
            if let Some(fact) = observations.into_terminal_fact() {
                ledger.commit(slot, window, fact);
            }
        }
        self.finish_terminal_grace_turn();
    }
}

impl<T: TargetIo, C> OwnerSessionControllerCore<T, C> {
    pub(super) fn finish_observed_page(
        &mut self,
        observed: OwnerTargetObservedApply,
        observed_at: SimTime,
        origin: OwnerContinuationOrigin,
    ) -> Result<OwnerSessionDrive, OwnerSessionControllerError> {
        // Original admission excludes an unsettled turn/Failed phase. With the
        // sole mutable C borrow no caller can produce a second page into this
        // slot. The original Vec/bundle enter custody before any page check.
        let (outputs, observations) = observed.into_progress_parts();
        self.aborted_turn = Some(AbortedOwnerControllerTurn::new(
            outputs,
            observations,
            None,
            origin,
        ));
        let page = self.aborted_turn.as_mut().ok_or_else(|| {
            OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::InvalidContinuationPage,
            )
        })?;
        page.continuation = match Self::take_page_continuation(&mut page.outputs) {
            Ok(continuation) if self.pending_continuation.is_none() => continuation,
            Ok(continuation) => {
                page.continuation = continuation;
                self.phase = OwnerSessionControllerPhase::Failed;
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::InvalidContinuationPage,
                ));
            }
            Err(()) => {
                self.phase = OwnerSessionControllerPhase::Failed;
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::InvalidContinuationPage,
                ));
            }
        };
        #[cfg(test)]
        if let OwnerTerminalGraceState::Bound(ledger) = &mut self.terminal_grace {
            if std::mem::take(&mut ledger.panic_before_returned_page_progress) {
                panic!("interrupted before returned-page progress settlement");
            }
        }
        let prepared = match &self.terminal_grace {
            OwnerTerminalGraceState::Bound(ledger) => ledger
                .preflight(&page.observations)
                .map_err(OwnerSessionProgressError::TerminalGrace),
            #[cfg(test)]
            OwnerTerminalGraceState::LegacyUntracked => Ok(None),
        };
        let prepared = prepared.and_then(|prepared| {
            #[cfg(test)]
            if std::mem::take(&mut self.fail_next_continuation_progress) {
                return Err(OwnerSessionProgressError::MissingActorArrival);
            }
            ControllerObservationSettlement {
                executor: &self.executor,
                progress: &mut self.progress,
                phase: &mut self.phase,
            }
            .consume(&page.observations, observed_at, origin.actor_arrived_at())?;
            Ok(prepared)
        });
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(source) => {
                self.phase = OwnerSessionControllerPhase::Failed;
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::PostCommitProgress(source),
                ));
            }
        };
        // No fallible/user operation remains once the owning page leaves C.
        let Some(page) = self.aborted_turn.take() else {
            self.phase = OwnerSessionControllerPhase::Failed;
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::InvalidContinuationPage,
            ));
        };
        self.retain_terminal_fact(page.observations, prepared);
        self.pending_continuation = page
            .continuation
            .map(|resume| PendingOwnerContinuation { resume, origin });
        if self.phase == OwnerSessionControllerPhase::SessionExpiring
            && self.pending_continuation.is_none()
        {
            self.phase = OwnerSessionControllerPhase::SessionExpired;
        }
        Ok(OwnerSessionDrive {
            outputs: page.outputs,
            needs_continue: self.pending_continuation.is_some(),
        })
    }

    pub(super) fn quarantine_invalid_continuation_page(
        &mut self,
        outputs: Vec<OwnerTargetOutput>,
        observations: OwnerTargetReducerObservations,
        origin: OwnerContinuationOrigin,
    ) {
        self.phase = OwnerSessionControllerPhase::Failed;
        self.aborted_turn = Some(AbortedOwnerControllerTurn::new(
            outputs,
            observations,
            None,
            origin,
        ));
    }

    pub(super) fn take_page_continuation(
        outputs: &mut Vec<OwnerTargetOutput>,
    ) -> Result<Option<OwnerTargetResume>, ()> {
        let tail_is_resume = matches!(outputs.last(), Some(OwnerTargetOutput::NeedsResume { .. }));
        let prefix_len = outputs.len().saturating_sub(usize::from(tail_is_resume));
        if outputs[..prefix_len]
            .iter()
            .any(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
        {
            return Err(());
        }
        if !tail_is_resume {
            return Ok(None);
        }
        let Some(OwnerTargetOutput::NeedsResume { resume }) = outputs.pop() else {
            return Err(());
        };
        Ok(Some(resume))
    }

    pub(super) fn finish_target_error(
        &mut self,
        error: OwnerTargetError,
        observed_at: SimTime,
        origin: OwnerContinuationOrigin,
    ) -> OwnerSessionControllerError {
        // Original admission proves this sole slot vacant. No callback or
        // fallible work occurs before the exact error enters the owning page.
        self.aborted_turn = Some(AbortedOwnerControllerTurn::new(
            Vec::new(),
            OwnerTargetReducerObservations::default(),
            None,
            origin,
        ));
        let Some(page) = self.aborted_turn.as_mut() else {
            self.phase = OwnerSessionControllerPhase::Failed;
            return OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Target(
                error,
            ));
        };
        page.target_error = Some(error);
        let mut settled_phase = self.phase;
        self.phase = OwnerSessionControllerPhase::Failed;
        let target_snapshot = self.executor.snapshot();
        let target_failed = target_snapshot.aborted_effects > 0 || target_snapshot.session.poisoned;
        let had_observations =
            if let Some(observations) = self.executor.take_retained_observations_after_failure() {
                page.observations = observations;
                true
            } else {
                false
            };
        #[cfg(test)]
        if let OwnerTerminalGraceState::Bound(ledger) = &mut self.terminal_grace {
            if std::mem::take(&mut ledger.panic_before_returned_page_progress) {
                panic!("interrupted before returned-error progress settlement");
            }
        }
        let prepared = if had_observations {
            let prepared = match &self.terminal_grace {
                OwnerTerminalGraceState::Bound(ledger) => ledger
                    .preflight(&page.observations)
                    .map_err(OwnerSessionProgressError::TerminalGrace),
                #[cfg(test)]
                OwnerTerminalGraceState::LegacyUntracked => Ok(None),
            };
            prepared.and_then(|prepared| {
                #[cfg(test)]
                if std::mem::take(&mut self.fail_next_continuation_progress) {
                    return Err(OwnerSessionProgressError::MissingActorArrival);
                }
                ControllerObservationSettlement {
                    executor: &self.executor,
                    progress: &mut self.progress,
                    phase: &mut settled_phase,
                }
                .consume(
                    &page.observations,
                    observed_at,
                    origin.actor_arrived_at(),
                )?;
                Ok(prepared)
            })
        } else {
            Ok(None)
        };
        // All fallible observation work is complete. Unknown interruption above
        // leaves the original error and bundle in C with Failed phase.
        let Some(error) = page.target_error.take() else {
            return OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::InvalidContinuationPage,
            );
        };
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(progress) => {
                return OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::TargetAndProgress {
                        target: error,
                        progress,
                    },
                );
            }
        };
        let Some(page) = self.aborted_turn.take() else {
            return OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Target(
                error,
            ));
        };
        if had_observations {
            self.retain_terminal_fact(page.observations, prepared);
        } else if !target_failed {
            self.finish_terminal_grace_turn();
        }
        self.phase = if target_failed {
            OwnerSessionControllerPhase::Failed
        } else {
            settled_phase
        };
        OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Target(error))
    }

    pub(super) fn consume_observations(
        &mut self,
        observations: &OwnerTargetReducerObservations,
        observed_at: SimTime,
        actor_arrived_at: Option<SimTime>,
    ) -> Result<(), OwnerSessionProgressError> {
        ControllerObservationSettlement {
            executor: &self.executor,
            progress: &mut self.progress,
            phase: &mut self.phase,
        }
        .consume(observations, observed_at, actor_arrived_at)
    }
}

// These disjoint borrows leave the owning returned-page journal in C while
// preserving the original progress/generation checks and recovery call seam.
struct ControllerObservationSettlement<'a, T> {
    executor: &'a OwnerTargetExecutor<T>,
    progress: &'a mut OwnerReverseProgress,
    phase: &'a mut OwnerSessionControllerPhase,
}
impl<T: TargetIo> ControllerObservationSettlement<'_, T> {
    fn consume(
        &mut self,
        observations: &OwnerTargetReducerObservations,
        observed_at: SimTime,
        actor_arrived_at: Option<SimTime>,
    ) -> Result<(), OwnerSessionProgressError> {
        if let Some(outstanding) = observations.replay_became_outstanding() {
            self.require_committed_generation(outstanding.generation())?;
            self.progress.register_live(
                outstanding.generation(),
                outstanding.flow_id(),
                outstanding.direction(),
            )?;
            self.progress.observe_outstanding(
                outstanding.generation(),
                outstanding.flow_id(),
                outstanding.direction(),
                outstanding.start(),
                observed_at,
            )?;
        }

        if let Some(ack) = observations.application_ack() {
            self.require_committed_generation(ack.generation())?;
            let arrived_at =
                actor_arrived_at.ok_or(OwnerSessionProgressError::MissingActorArrival)?;
            let positive_ack = self.progress.observe_positive_ack(
                ack.generation(),
                ack.flow_id(),
                ack.direction(),
                ack.next_accepted(),
                arrived_at,
            );
            if positive_ack == Err(OwnerProgressError::GenerationMismatch) {
                self.progress
                    .rebase_direction(ack.generation(), ack.flow_id(), ack.direction())?;
                self.progress.observe_positive_ack(
                    ack.generation(),
                    ack.flow_id(),
                    ack.direction(),
                    ack.next_accepted(),
                    arrived_at,
                )?;
            } else {
                positive_ack?;
            }
            match ack.remaining_oldest() {
                Some(oldest) => self.progress.observe_outstanding(
                    ack.generation(),
                    ack.flow_id(),
                    ack.direction(),
                    oldest,
                    arrived_at,
                )?,
                None => self.progress.clear_outstanding(
                    ack.generation(),
                    ack.flow_id(),
                    ack.direction(),
                )?,
            }
        }

        if let Some(terminal) = observations.flow_terminal() {
            self.require_committed_generation(terminal.generation())?;
            let registered = self.progress.register_live(
                terminal.generation(),
                terminal.flow_id(),
                terminal.direction(),
            );
            if registered == Err(OwnerProgressError::FlowAlreadyRegistered) {
                self.progress.rebase_direction(
                    terminal.generation(),
                    terminal.flow_id(),
                    terminal.direction(),
                )?;
            } else {
                registered?;
            }
            self.progress.clear_outstanding(
                terminal.generation(),
                terminal.flow_id(),
                terminal.direction(),
            )?;
            self.progress.mark_terminal(
                terminal.generation(),
                terminal.flow_id(),
                terminal.direction(),
            )?;
        }

        if let Some(retired) = observations.flow_retired() {
            self.require_committed_generation(retired.generation())?;
            self.progress
                .retire_flow_after_committed_generation(retired.generation(), retired.flow_id())?;
        }

        if observations.session_expired().is_some() {
            self.progress.reset_for_session_expiry();
            *self.phase = OwnerSessionControllerPhase::SessionExpiring;
        }
        Ok(())
    }

    fn require_committed_generation(
        &self,
        observed: LegGeneration,
    ) -> Result<(), OwnerSessionProgressError> {
        let committed = self.executor.snapshot().session.session.generation();
        if observed != committed {
            return Err(
                OwnerSessionProgressError::ObservationGenerationNotCommitted {
                    committed,
                    observed,
                },
            );
        }
        Ok(())
    }
}

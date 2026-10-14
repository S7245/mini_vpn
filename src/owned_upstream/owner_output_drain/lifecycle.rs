//! Exact owner output lifecycle transactions and typed-slot settlement.
//!
//! Owns action conversion, rollback guards, installation, rejection and stop
//! accounting. Parent retains declaration/storage anchors and FIFO-page I/O.
//! Existing consuming methods preserve one original action through each branch;
//! A single cold physical Sleep holds only registration/correlation; C owns
//! the original terminal ledger and complete expiry, not this Adapter.
//! Entry operations are the original port/installed-slot methods; tests remain
//! in the parent private tests Module.

use super::*;

impl OwnerTerminalTimerRegistration {
    /// Cold only. Reportable layout failure is distinct from the process
    /// allocator-abort model. The current runtime must have a time driver.
    pub(in crate::owned_upstream) fn cold() -> Result<Self, std::alloc::LayoutError> {
        std::alloc::Layout::new::<Self>().extend(std::alloc::Layout::new::<tokio::time::Sleep>())?;
        Ok(Self {
            sleep: Box::pin(tokio::time::sleep_until(tokio::time::Instant::now())),
            state: OwnerTerminalTimerState::Dormant,
        })
    }

    pub(in crate::owned_upstream) fn requested(
        &self,
        terminal: TerminalGrace,
        deadline: tokio::time::Instant,
    ) -> bool {
        match self.state {
            OwnerTerminalTimerState::Dormant => true,
            OwnerTerminalTimerState::Armed {
                terminal: held,
                deadline: at,
                needs_poll,
            } => held != terminal || at != deadline || needs_poll || self.sleep.is_elapsed(),
            OwnerTerminalTimerState::DueHeld { .. } => true,
        }
    }

    pub(in crate::owned_upstream) fn poll_original(
        &mut self,
        terminal: TerminalGrace,
        deadline: tokio::time::Instant,
        already_due: bool,
        context: &mut std::task::Context<'_>,
    ) -> Result<std::task::Poll<()>, ()> {
        use std::future::Future;
        use std::task::Poll;
        match self.state {
            OwnerTerminalTimerState::Dormant => {
                self.sleep.as_mut().reset(deadline);
                self.state = OwnerTerminalTimerState::Armed {
                    terminal,
                    deadline,
                    needs_poll: true,
                };
            }
            OwnerTerminalTimerState::Armed {
                terminal: held,
                deadline: at,
                ..
            } if held == terminal && at == deadline => {}
            OwnerTerminalTimerState::DueHeld { terminal: held } if held == terminal => {
                return Ok(Poll::Ready(()));
            }
            _ => return Err(()),
        }
        if already_due || self.sleep.as_mut().poll(context).is_ready() {
            // A completed Sleep is never polled again, even under exclusion.
            self.state = OwnerTerminalTimerState::DueHeld { terminal };
            Ok(Poll::Ready(()))
        } else {
            self.state = OwnerTerminalTimerState::Armed {
                terminal,
                deadline,
                needs_poll: false,
            };
            Ok(Poll::Pending)
        }
    }

    pub(in crate::owned_upstream) fn holds_due(&self, terminal: TerminalGrace) -> bool {
        matches!(self.state, OwnerTerminalTimerState::DueHeld { terminal: held } if held == terminal)
    }

    pub(in crate::owned_upstream) fn inventory(&self) -> (usize, usize) {
        (
            1,
            usize::from(!matches!(self.state, OwnerTerminalTimerState::Dormant)),
        )
    }

    pub(in crate::owned_upstream) fn settle_original(
        &mut self,
        terminal: TerminalGrace,
    ) -> Result<(), ()> {
        if !self.holds_due(terminal) {
            return Err(());
        }
        self.state = OwnerTerminalTimerState::Dormant;
        Ok(())
    }
}

impl OwnerOutputLifecycleAction {
    pub(super) fn try_from_output(output: OwnerTargetOutput) -> Result<Self, OwnerTargetOutput> {
        let inner = match output {
            OwnerTargetOutput::LegActivated { generation } => {
                OwnerOutputLifecycleActionInner::LegActivated { generation }
            }
            OwnerTargetOutput::ResumeGraceStarted { generation } => {
                OwnerOutputLifecycleActionInner::ResumeGraceStarted { generation }
            }
            OwnerTargetOutput::TerminalGraceStarted { terminal } => {
                OwnerOutputLifecycleActionInner::TerminalGraceStarted { terminal }
            }
            OwnerTargetOutput::SessionExpired => OwnerOutputLifecycleActionInner::SessionExpired,
            OwnerTargetOutput::Transmit(frame) => {
                return Err(OwnerTargetOutput::Transmit(frame));
            }
            OwnerTargetOutput::NeedsResume { resume } => {
                return Err(OwnerTargetOutput::NeedsResume { resume });
            }
        };
        Ok(Self {
            inner,
            _authority: OwnerOutputLifecycleAuthority { _private: () },
        })
    }

    pub(super) fn into_output(self) -> OwnerTargetOutput {
        match self.inner {
            OwnerOutputLifecycleActionInner::LegActivated { generation } => {
                OwnerTargetOutput::LegActivated { generation }
            }
            OwnerOutputLifecycleActionInner::ResumeGraceStarted { generation } => {
                OwnerTargetOutput::ResumeGraceStarted { generation }
            }
            OwnerOutputLifecycleActionInner::TerminalGraceStarted { terminal } => {
                OwnerTargetOutput::TerminalGraceStarted { terminal }
            }
            OwnerOutputLifecycleActionInner::SessionExpired => OwnerTargetOutput::SessionExpired,
        }
    }

    pub(crate) const fn kind(&self) -> OwnerOutputLifecycleKind {
        match &self.inner {
            OwnerOutputLifecycleActionInner::LegActivated { generation } => {
                OwnerOutputLifecycleKind::LegActivated {
                    generation: *generation,
                }
            }
            OwnerOutputLifecycleActionInner::ResumeGraceStarted { generation } => {
                OwnerOutputLifecycleKind::ResumeGraceStarted {
                    generation: *generation,
                }
            }
            OwnerOutputLifecycleActionInner::TerminalGraceStarted { terminal } => {
                OwnerOutputLifecycleKind::TerminalGraceStarted {
                    session_id: terminal.session_id(),
                    flow_id: terminal.flow_id(),
                    direction: terminal.direction(),
                }
            }
            OwnerOutputLifecycleActionInner::SessionExpired => {
                OwnerOutputLifecycleKind::SessionExpired
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn leg_activated_for_test(generation: LegGeneration) -> Self {
        Self {
            inner: OwnerOutputLifecycleActionInner::LegActivated { generation },
            _authority: OwnerOutputLifecycleAuthority { _private: () },
        }
    }

    #[cfg(test)]
    pub(crate) fn resume_grace_started_for_test(generation: LegGeneration) -> Self {
        Self {
            inner: OwnerOutputLifecycleActionInner::ResumeGraceStarted { generation },
            _authority: OwnerOutputLifecycleAuthority { _private: () },
        }
    }

    #[cfg(test)]
    pub(crate) fn session_expired_for_test() -> Self {
        Self {
            inner: OwnerOutputLifecycleActionInner::SessionExpired,
            _authority: OwnerOutputLifecycleAuthority { _private: () },
        }
    }

    #[cfg(test)]
    pub(crate) fn terminal_grace_started_for_test(terminal: TerminalGrace) -> Self {
        Self {
            inner: OwnerOutputLifecycleActionInner::TerminalGraceStarted { terminal },
            _authority: OwnerOutputLifecycleAuthority { _private: () },
        }
    }
}

impl<'port> OwnerOutputLifecycleTransaction<'port> {
    pub(crate) fn kind(&self) -> Option<OwnerOutputLifecycleKind> {
        self.action.as_ref().map(OwnerOutputLifecycleAction::kind)
    }

    pub(crate) const fn settlement(&self) -> OwnerOutputLifecycleSettlement {
        self.settlement
    }

    pub(crate) fn retain<'slot>(self) -> OwnerOutputLifecycleResolution<'port, 'slot> {
        OwnerOutputLifecycleResolution {
            transaction: self,
            disposition: OwnerOutputLifecycleResolutionDisposition::Retain,
        }
    }

    pub(crate) fn install_into<'slot>(
        self,
        slot: &'slot mut OwnerOutputLifecycleInstalledSlot,
    ) -> OwnerOutputLifecycleResolution<'port, 'slot> {
        OwnerOutputLifecycleResolution {
            transaction: self,
            disposition: OwnerOutputLifecycleResolutionDisposition::Install(slot),
        }
    }

    pub(crate) fn discard<'slot>(self) -> OwnerOutputLifecycleResolution<'port, 'slot> {
        OwnerOutputLifecycleResolution {
            transaction: self,
            disposition: OwnerOutputLifecycleResolutionDisposition::Discard,
        }
    }
}

impl Drop for OwnerOutputLifecycleTransaction<'_> {
    fn drop(&mut self) {
        if let Some(action) = self.action.take() {
            // `port` is exclusively borrowed by this guard and was emptied
            // immediately before construction. No callback can mutate it.
            self.port.pending = Some(action);
        }
    }
}

impl OwnerOutputLifecycleResolution<'_, '_> {
    pub(super) fn finalize(mut self) -> OwnerOutputLifecycleDrive {
        let settlement = self.transaction.settlement;
        match &mut self.disposition {
            OwnerOutputLifecycleResolutionDisposition::Retain => {
                drop(self);
                OwnerOutputLifecycleDrive::Retained { settlement }
            }
            OwnerOutputLifecycleResolutionDisposition::Install(slot) => {
                if settlement != OwnerOutputLifecycleSettlement::Install {
                    drop(self);
                    return OwnerOutputLifecycleDrive::WrongPhase;
                }
                if slot.action.is_some() {
                    drop(self);
                    return OwnerOutputLifecycleDrive::Retained { settlement };
                }
                let Some(action) = self.transaction.action.take() else {
                    self.transaction.port.audit_failed = true;
                    return OwnerOutputLifecycleDrive::AuditFailed;
                };
                slot.action = Some(action);
                self.transaction.port.record(settlement);
                OwnerOutputLifecycleDrive::Committed { settlement }
            }
            OwnerOutputLifecycleResolutionDisposition::Discard => {
                if !matches!(settlement, OwnerOutputLifecycleSettlement::Discard { .. }) {
                    drop(self);
                    return OwnerOutputLifecycleDrive::WrongPhase;
                }
                let Some(action) = self.transaction.action.take() else {
                    self.transaction.port.audit_failed = true;
                    return OwnerOutputLifecycleDrive::AuditFailed;
                };
                drop(action);
                self.transaction.port.record(settlement);
                OwnerOutputLifecycleDrive::Committed { settlement }
            }
        }
    }
}

impl OwnerOutputLifecycleInstalledInventory {
    pub(crate) const fn installed_actions(self) -> usize {
        self.installed_actions
    }

    pub(crate) const fn is_exact_zero(self) -> bool {
        self.installed_actions == 0
    }
}

impl OwnerOutputLifecycleInstalledSlot {
    pub(crate) const fn new() -> Self {
        Self { action: None }
    }

    pub(crate) fn kind(&self) -> Option<OwnerOutputLifecycleKind> {
        self.action.as_ref().map(OwnerOutputLifecycleAction::kind)
    }

    pub(crate) const fn inventory(&self) -> OwnerOutputLifecycleInstalledInventory {
        OwnerOutputLifecycleInstalledInventory {
            installed_actions: if self.action.is_some() { 1 } else { 0 },
        }
    }

    /// Moves the exact opaque action into its typed physical root slot. Every
    /// rejection returns this complete installed slot; callers never rebuild
    /// work from the copyable `kind` metadata.
    pub(crate) fn apply_into(
        mut self,
        slots: &mut OwnerPhysicalLifecycleSlots,
    ) -> Result<OwnerPhysicalLifecycleApplied, OwnerPhysicalLifecycleApplyFailure> {
        let Some(action) = self.action.take() else {
            return Err(OwnerPhysicalLifecycleApplyFailure {
                kind: OwnerPhysicalLifecycleApplyErrorKind::InstalledSlotEmpty,
                installed: self,
            });
        };
        match slots.try_apply(action) {
            Ok(applied) => Ok(applied),
            Err((action, kind)) => {
                self.action = Some(action);
                Err(OwnerPhysicalLifecycleApplyFailure {
                    kind,
                    installed: self,
                })
            }
        }
    }
}

impl OwnerPhysicalLifecycleApplyFailure {
    pub(crate) const fn kind(&self) -> OwnerPhysicalLifecycleApplyErrorKind {
        self.kind
    }

    pub(crate) fn into_installed_slot(self) -> OwnerOutputLifecycleInstalledSlot {
        self.installed
    }
}

impl std::fmt::Debug for OwnerPhysicalLifecycleApplyFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnerPhysicalLifecycleApplyFailure")
            .field("kind", &self.kind)
            .field("installed", &"[REDACTED]")
            .finish()
    }
}

impl OwnerPhysicalLifecycleApplied {
    pub(crate) const fn kind(self) -> OwnerOutputLifecycleKind {
        self.kind
    }
}

impl OwnerPhysicalLifecycleHistory {
    pub(crate) const fn leg_activated_applied(self) -> u64 {
        self.leg_activated_applied
    }

    pub(crate) const fn resume_grace_applied(self) -> u64 {
        self.resume_grace_applied
    }

    pub(crate) const fn terminal_grace_applied(self) -> u64 {
        self.terminal_grace_applied
    }

    pub(crate) const fn session_expired_applied(self) -> u64 {
        self.session_expired_applied
    }

    pub(crate) const fn fatal_discards(self) -> u64 {
        self.fatal_discards
    }

    pub(crate) const fn cancelled_discards(self) -> u64 {
        self.cancelled_discards
    }

    pub(crate) const fn shutdown_discards(self) -> u64 {
        self.shutdown_discards
    }
}

impl OwnerPhysicalLifecycleSlotsInventory {
    pub(crate) const fn leg_activated_phases(self) -> usize {
        self.leg_activated_phases
    }

    pub(crate) const fn resume_grace_timers(self) -> usize {
        self.resume_grace_timers
    }

    pub(crate) const fn terminal_grace_timers(self) -> usize {
        self.terminal_grace_timers
    }

    pub(crate) const fn terminal_grace_capacity(self) -> usize {
        self.terminal_grace_capacity
    }

    pub(crate) const fn session_expired_phases(self) -> usize {
        self.session_expired_phases
    }

    pub(crate) const fn stopped_reason(self) -> Option<OwnerOutputDrainStopReason> {
        self.stopped_reason
    }

    pub(crate) const fn audit_failed(self) -> bool {
        self.audit_failed
    }

    pub(crate) const fn live_actions(self) -> usize {
        self.leg_activated_phases
            + self.resume_grace_timers
            + self.terminal_grace_timers
            + self.session_expired_phases
    }

    pub(crate) const fn is_exact_zero(self) -> bool {
        self.live_actions() == 0 && !self.audit_failed
    }
}

impl OwnerPhysicalLifecycleSlots {
    pub(crate) fn try_new(
        session_id: SessionId,
        current_generation: LegGeneration,
        terminal_grace_capacity: usize,
    ) -> Result<Self, OwnerPhysicalLifecycleSlotsConfigError> {
        if terminal_grace_capacity == 0 {
            return Err(OwnerPhysicalLifecycleSlotsConfigError::ZeroTerminalGraceCapacity);
        }
        let mut terminal_graces = Vec::new();
        terminal_graces
            .try_reserve_exact(terminal_grace_capacity)
            .map_err(|_| {
                OwnerPhysicalLifecycleSlotsConfigError::TerminalGraceReservationFailed {
                    requested: terminal_grace_capacity,
                }
            })?;
        Ok(Self {
            session_id,
            current_generation,
            terminal_grace_capacity,
            leg_activated: None,
            resume_grace: None,
            terminal_graces,
            session_expired: None,
            stopped_reason: None,
            audit_failed: false,
            history: OwnerPhysicalLifecycleHistory::default(),
        })
    }

    pub(crate) const fn inventory(&self) -> OwnerPhysicalLifecycleSlotsInventory {
        OwnerPhysicalLifecycleSlotsInventory {
            leg_activated_phases: if self.leg_activated.is_some() { 1 } else { 0 },
            resume_grace_timers: if self.resume_grace.is_some() { 1 } else { 0 },
            terminal_grace_timers: self.terminal_graces.len(),
            terminal_grace_capacity: self.terminal_grace_capacity,
            session_expired_phases: if self.session_expired.is_some() { 1 } else { 0 },
            stopped_reason: self.stopped_reason,
            audit_failed: self.audit_failed,
        }
    }

    pub(crate) const fn history(&self) -> OwnerPhysicalLifecycleHistory {
        self.history
    }

    /// Terminally settles every live typed action under the first sticky root
    /// reason. The historical counter is preflighted before any exact action is
    /// dropped, so an audit failure leaves the complete live inventory intact.
    pub(crate) fn settle_for_stop(
        &mut self,
        reason: OwnerOutputDrainStopReason,
    ) -> OwnerPhysicalLifecycleStopDrive {
        if self.audit_failed {
            return OwnerPhysicalLifecycleStopDrive::AuditFailed {
                inventory: self.inventory(),
            };
        }
        if let Some(reason) = self.stopped_reason {
            return OwnerPhysicalLifecycleStopDrive::AlreadySettled { reason };
        }
        let discarded = self.inventory();
        let Ok(live_actions) = u64::try_from(discarded.live_actions()) else {
            self.audit_failed = true;
            return OwnerPhysicalLifecycleStopDrive::AuditFailed {
                inventory: self.inventory(),
            };
        };
        let next = match reason {
            OwnerOutputDrainStopReason::Fatal => {
                self.history.fatal_discards.checked_add(live_actions)
            }
            OwnerOutputDrainStopReason::Cancelled => {
                self.history.cancelled_discards.checked_add(live_actions)
            }
            OwnerOutputDrainStopReason::Shutdown => {
                self.history.shutdown_discards.checked_add(live_actions)
            }
        };
        let Some(next) = next else {
            self.audit_failed = true;
            return OwnerPhysicalLifecycleStopDrive::AuditFailed {
                inventory: self.inventory(),
            };
        };

        self.leg_activated = None;
        self.resume_grace = None;
        self.terminal_graces.clear();
        self.session_expired = None;
        match reason {
            OwnerOutputDrainStopReason::Fatal => self.history.fatal_discards = next,
            OwnerOutputDrainStopReason::Cancelled => self.history.cancelled_discards = next,
            OwnerOutputDrainStopReason::Shutdown => self.history.shutdown_discards = next,
        }
        self.stopped_reason = Some(reason);
        OwnerPhysicalLifecycleStopDrive::Settled { reason, discarded }
    }

    pub(super) fn try_apply(
        &mut self,
        action: OwnerOutputLifecycleAction,
    ) -> Result<
        OwnerPhysicalLifecycleApplied,
        (
            OwnerOutputLifecycleAction,
            OwnerPhysicalLifecycleApplyErrorKind,
        ),
    > {
        if self.audit_failed {
            return Err((
                action,
                OwnerPhysicalLifecycleApplyErrorKind::HistoryOverflow,
            ));
        }
        if let Some(reason) = self.stopped_reason {
            return Err((
                action,
                OwnerPhysicalLifecycleApplyErrorKind::Stopped { reason },
            ));
        }
        let kind = action.kind();
        let error = match &action.inner {
            OwnerOutputLifecycleActionInner::LegActivated { generation } => {
                if *generation != self.current_generation {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::WrongGeneration {
                        expected: self.current_generation,
                        actual: *generation,
                    })
                } else if self.leg_activated.is_some() {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::SlotOccupied {
                        slot: OwnerPhysicalLifecycleSlotKind::LegActivated,
                    })
                } else if self.history.leg_activated_applied == u64::MAX {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::HistoryOverflow)
                } else {
                    None
                }
            }
            OwnerOutputLifecycleActionInner::ResumeGraceStarted { generation } => {
                if *generation != self.current_generation {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::WrongGeneration {
                        expected: self.current_generation,
                        actual: *generation,
                    })
                } else if self.resume_grace.is_some() {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::SlotOccupied {
                        slot: OwnerPhysicalLifecycleSlotKind::ResumeGrace,
                    })
                } else if self.history.resume_grace_applied == u64::MAX {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::HistoryOverflow)
                } else {
                    None
                }
            }
            OwnerOutputLifecycleActionInner::TerminalGraceStarted { terminal } => {
                if terminal.session_id() != self.session_id {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::WrongSession {
                        expected: self.session_id,
                        actual: terminal.session_id(),
                    })
                } else if self.terminal_graces.len() >= self.terminal_grace_capacity {
                    Some(
                        OwnerPhysicalLifecycleApplyErrorKind::TerminalGraceCapacityExceeded {
                            max: self.terminal_grace_capacity,
                        },
                    )
                } else if self.terminal_graces.iter().any(|installed| {
                    matches!(
                        &installed.inner,
                        OwnerOutputLifecycleActionInner::TerminalGraceStarted { terminal: live }
                            if live == terminal
                    )
                }) {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::DuplicateTerminalGrace)
                } else if self.history.terminal_grace_applied == u64::MAX {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::HistoryOverflow)
                } else {
                    None
                }
            }
            OwnerOutputLifecycleActionInner::SessionExpired => {
                if self.session_expired.is_some() {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::SlotOccupied {
                        slot: OwnerPhysicalLifecycleSlotKind::SessionExpired,
                    })
                } else if self.history.session_expired_applied == u64::MAX {
                    Some(OwnerPhysicalLifecycleApplyErrorKind::HistoryOverflow)
                } else {
                    None
                }
            }
        };
        if let Some(error) = error {
            return Err((action, error));
        }

        match kind {
            OwnerOutputLifecycleKind::LegActivated { .. } => {
                self.leg_activated = Some(action);
                self.history.leg_activated_applied += 1;
            }
            OwnerOutputLifecycleKind::ResumeGraceStarted { .. } => {
                self.resume_grace = Some(action);
                self.history.resume_grace_applied += 1;
            }
            OwnerOutputLifecycleKind::TerminalGraceStarted { .. } => {
                self.terminal_graces.push(action);
                self.history.terminal_grace_applied += 1;
            }
            OwnerOutputLifecycleKind::SessionExpired => {
                self.session_expired = Some(action);
                self.history.session_expired_applied += 1;
            }
        }
        Ok(OwnerPhysicalLifecycleApplied { kind })
    }
}

impl OwnerOutputLifecycleInventory {
    pub(crate) const fn pending_actions(self) -> usize {
        self.pending_actions
    }

    pub(crate) const fn installed_actions(self) -> u64 {
        self.installed_actions
    }

    pub(crate) const fn fatal_discards(self) -> u64 {
        self.fatal_discards
    }

    pub(crate) const fn cancelled_discards(self) -> u64 {
        self.cancelled_discards
    }

    pub(crate) const fn shutdown_discards(self) -> u64 {
        self.shutdown_discards
    }

    pub(crate) const fn audit_failed(self) -> bool {
        self.audit_failed
    }

    pub(crate) const fn is_exact_zero(self) -> bool {
        self.pending_actions == 0 && !self.audit_failed
    }
}

impl OwnerOutputLifecyclePort {
    /// Pair the opaque action with the original C fact before any owning move.
    /// A scalar kind is not sufficient: LocalFlow and tombstone identity must
    /// match too. Rejection leaves both the port and supplied slot unchanged.
    pub(crate) fn install_exact_terminal(
        &mut self,
        expected: crate::resumable::TerminalGrace,
        installed: &mut OwnerOutputLifecycleInstalledSlot,
    ) -> OwnerOutputLifecycleDrive {
        if !matches!(self.pending.as_ref().map(|action| &action.inner),
            Some(OwnerOutputLifecycleActionInner::TerminalGraceStarted { terminal })
                if *terminal == expected)
            || installed.action.is_some()
        {
            return OwnerOutputLifecycleDrive::WrongPhase;
        }
        self.transact(OwnerOutputLifecycleSettlement::Install, |transaction| {
            transaction.install_into(installed)
        })
    }

    pub(crate) const fn new() -> Self {
        Self {
            pending: None,
            installed_actions: 0,
            fatal_discards: 0,
            cancelled_discards: 0,
            shutdown_discards: 0,
            audit_failed: false,
        }
    }

    pub(crate) const fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub(crate) fn try_install(
        &mut self,
        action: OwnerOutputLifecycleAction,
    ) -> Result<(), OwnerOutputLifecycleAction> {
        if self.pending.is_some() || self.audit_failed {
            return Err(action);
        }
        self.pending = Some(action);
        Ok(())
    }

    pub(crate) const fn inventory(&self) -> OwnerOutputLifecycleInventory {
        OwnerOutputLifecycleInventory {
            pending_actions: if self.pending.is_some() {
                OWNER_OUTPUT_LIFECYCLE_PORT_CAPACITY
            } else {
                0
            },
            installed_actions: self.installed_actions,
            fatal_discards: self.fatal_discards,
            cancelled_discards: self.cancelled_discards,
            shutdown_discards: self.shutdown_discards,
            audit_failed: self.audit_failed,
        }
    }

    pub(crate) fn transact<'port, 'slot>(
        &'port mut self,
        settlement: OwnerOutputLifecycleSettlement,
        commit: impl FnOnce(
            OwnerOutputLifecycleTransaction<'port>,
        ) -> OwnerOutputLifecycleResolution<'port, 'slot>,
    ) -> OwnerOutputLifecycleDrive {
        if self.audit_failed {
            return OwnerOutputLifecycleDrive::AuditFailed;
        }
        if self.pending.is_none() {
            return OwnerOutputLifecycleDrive::Inert;
        }
        if !self.can_record(settlement) {
            self.audit_failed = true;
            return OwnerOutputLifecycleDrive::AuditFailed;
        }
        let Some(action) = self.pending.take() else {
            self.audit_failed = true;
            return OwnerOutputLifecycleDrive::AuditFailed;
        };
        let transaction = OwnerOutputLifecycleTransaction {
            port: self,
            action: Some(action),
            settlement,
        };
        commit(transaction).finalize()
    }

    pub(super) fn can_record(&self, settlement: OwnerOutputLifecycleSettlement) -> bool {
        match settlement {
            OwnerOutputLifecycleSettlement::Install => self.installed_actions < u64::MAX,
            OwnerOutputLifecycleSettlement::Discard {
                reason: OwnerOutputDrainStopReason::Fatal,
            } => self.fatal_discards < u64::MAX,
            OwnerOutputLifecycleSettlement::Discard {
                reason: OwnerOutputDrainStopReason::Cancelled,
            } => self.cancelled_discards < u64::MAX,
            OwnerOutputLifecycleSettlement::Discard {
                reason: OwnerOutputDrainStopReason::Shutdown,
            } => self.shutdown_discards < u64::MAX,
        }
    }

    pub(super) fn record(&mut self, settlement: OwnerOutputLifecycleSettlement) {
        let counter = match settlement {
            OwnerOutputLifecycleSettlement::Install => &mut self.installed_actions,
            OwnerOutputLifecycleSettlement::Discard {
                reason: OwnerOutputDrainStopReason::Fatal,
            } => &mut self.fatal_discards,
            OwnerOutputLifecycleSettlement::Discard {
                reason: OwnerOutputDrainStopReason::Cancelled,
            } => &mut self.cancelled_discards,
            OwnerOutputLifecycleSettlement::Discard {
                reason: OwnerOutputDrainStopReason::Shutdown,
            } => &mut self.shutdown_discards,
        };
        // `can_record` ran before the callback and the port stays exclusively
        // borrowed by the rollback guard, so this addition is exact.
        *counter += 1;
    }
}

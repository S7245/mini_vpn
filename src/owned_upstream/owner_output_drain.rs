//! Bounded FIFO ownership for one owner Target output page.

mod lifecycle;

/// One physical registration, borrowed by C's original terminal transaction.
/// Correlation is private and cannot be used as another terminal ledger.
pub(in crate::owned_upstream) struct OwnerTerminalTimerRegistration {
    sleep: std::pin::Pin<Box<tokio::time::Sleep>>,
    state: OwnerTerminalTimerState,
}

enum OwnerTerminalTimerState {
    Dormant,
    Armed {
        terminal: TerminalGrace,
        deadline: tokio::time::Instant,
        needs_poll: bool,
    },
    DueHeld {
        terminal: TerminalGrace,
    },
}

use std::collections::VecDeque;

use thiserror::Error;

use crate::resumable::{Direction, LegGeneration, SessionFlowId, SessionId, TerminalGrace};

use super::leg_io::{LegOutboundQueue, LegOutboundQueueErrorKind};
use super::owner_target::OwnerTargetOutput;

/// The physical owner can retain exactly one lifecycle action between an
/// output-FIFO turn and the full root's typed timer/phase installation turn.
pub(crate) const OWNER_OUTPUT_LIFECYCLE_PORT_CAPACITY: usize = 1;

/// Copyable metadata for one lifecycle action. This deliberately omits the
/// reducer's exact `TerminalGrace` capability and the private linear witness;
/// a scheduler may inspect this value but cannot use it to reconstruct work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerOutputLifecycleKind {
    LegActivated {
        generation: LegGeneration,
    },
    ResumeGraceStarted {
        generation: LegGeneration,
    },
    TerminalGraceStarted {
        session_id: SessionId,
        flow_id: SessionFlowId,
        direction: Direction,
    },
    SessionExpired,
}

#[derive(Debug)]
enum OwnerOutputLifecycleActionInner {
    LegActivated { generation: LegGeneration },
    ResumeGraceStarted { generation: LegGeneration },
    TerminalGraceStarted { terminal: TerminalGrace },
    SessionExpired,
}

/// Private non-Clone/non-Copy authority minted only while removing a real
/// Target output from the bounded FIFO. Even actions whose reducer metadata is
/// scalar retain this witness, so another crate module cannot reconstruct the
/// exact install authority from a borrowed kind.
#[derive(Debug)]
struct OwnerOutputLifecycleAuthority {
    _private: (),
}

/// Lifecycle work that has left the root output FIFO and must be applied by
/// the future full owner root exactly once. Fields and variants stay private:
/// the only production mint is `try_from_output`, and installation moves this
/// complete non-Clone/non-Copy value into the typed capacity-one slot.
#[derive(Debug)]
pub(crate) struct OwnerOutputLifecycleAction {
    inner: OwnerOutputLifecycleActionInner,
    _authority: OwnerOutputLifecycleAuthority,
}

/// Root-selected disposition for the exact lifecycle action retained by the
/// physical owner.  A live install creates the matching typed timer/phase slot;
/// a terminal discard records why that slot must no longer be scheduled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerOutputLifecycleSettlement {
    Install,
    Discard { reason: OwnerOutputDrainStopReason },
}

/// Non-cloneable rollback guard for the exact action temporarily removed from
/// the port. The callback sees only copyable metadata. Until its prepared
/// resolution is finalized, dropping or unwinding this guard restores the
/// exact action to the borrowed port without consulting caller state.
pub(crate) struct OwnerOutputLifecycleTransaction<'a> {
    port: &'a mut OwnerOutputLifecyclePort,
    action: Option<OwnerOutputLifecycleAction>,
    settlement: OwnerOutputLifecycleSettlement,
}

enum OwnerOutputLifecycleResolutionDisposition<'slot> {
    Retain,
    Install(&'slot mut OwnerOutputLifecycleInstalledSlot),
    Discard,
}

/// Unforgeable prepared callback result. The action remains inside its
/// rollback guard until `OwnerOutputLifecyclePort::transact` regains control
/// and finalizes this value, so a panic after preparation still rolls back.
pub(crate) struct OwnerOutputLifecycleResolution<'port, 'slot> {
    transaction: OwnerOutputLifecycleTransaction<'port>,
    disposition: OwnerOutputLifecycleResolutionDisposition<'slot>,
}

/// Cold inventory for the root-owned installed lifecycle slot. This is live
/// ownership, not a historical counter, and must be joined into whole-root
/// fatal/cancel/shutdown accounting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerOutputLifecycleInstalledInventory {
    installed_actions: usize,
}

/// Capacity-one typed root slot that owns the exact non-Copy lifecycle action
/// after commit. Metadata stays borrow-only; later full-root timer/phase APIs
/// must consume this slot rather than reconstructing reducer capabilities.
#[must_use = "an installed owner lifecycle action requires typed root settlement"]
pub(crate) struct OwnerOutputLifecycleInstalledSlot {
    action: Option<OwnerOutputLifecycleAction>,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerPhysicalLifecycleSlotsConfigError {
    #[error("terminal-grace slot capacity must be nonzero")]
    ZeroTerminalGraceCapacity,
    #[error("failed to reserve {requested} terminal-grace slots")]
    TerminalGraceReservationFailed { requested: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerPhysicalLifecycleSlotKind {
    LegActivated,
    ResumeGrace,
    TerminalGrace,
    SessionExpired,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerPhysicalLifecycleApplyErrorKind {
    #[error("installed lifecycle slot is empty")]
    InstalledSlotEmpty,
    #[error("physical lifecycle root is stopped for {reason:?}")]
    Stopped { reason: OwnerOutputDrainStopReason },
    #[error("lifecycle action belongs to another session")]
    WrongSession {
        expected: SessionId,
        actual: SessionId,
    },
    #[error("lifecycle generation does not match the current physical generation")]
    WrongGeneration {
        expected: LegGeneration,
        actual: LegGeneration,
    },
    #[error("typed lifecycle slot is occupied")]
    SlotOccupied {
        slot: OwnerPhysicalLifecycleSlotKind,
    },
    #[error("terminal-grace registry reached its logical capacity {max}")]
    TerminalGraceCapacityExceeded { max: usize },
    #[error("terminal-grace registry already owns this exact token")]
    DuplicateTerminalGrace,
    #[error("lifecycle history counter overflowed")]
    HistoryOverflow,
}

#[must_use = "a rejected physical lifecycle apply still owns the exact installed action"]
pub(crate) struct OwnerPhysicalLifecycleApplyFailure {
    kind: OwnerPhysicalLifecycleApplyErrorKind,
    installed: OwnerOutputLifecycleInstalledSlot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerPhysicalLifecycleApplied {
    kind: OwnerOutputLifecycleKind,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OwnerPhysicalLifecycleHistory {
    leg_activated_applied: u64,
    resume_grace_applied: u64,
    terminal_grace_applied: u64,
    session_expired_applied: u64,
    fatal_discards: u64,
    cancelled_discards: u64,
    shutdown_discards: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerPhysicalLifecycleSlotsInventory {
    leg_activated_phases: usize,
    resume_grace_timers: usize,
    terminal_grace_timers: usize,
    terminal_grace_capacity: usize,
    session_expired_phases: usize,
    stopped_reason: Option<OwnerOutputDrainStopReason>,
    audit_failed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerPhysicalLifecycleStopDrive {
    Settled {
        reason: OwnerOutputDrainStopReason,
        discarded: OwnerPhysicalLifecycleSlotsInventory,
    },
    AlreadySettled {
        reason: OwnerOutputDrainStopReason,
    },
    AuditFailed {
        inventory: OwnerPhysicalLifecycleSlotsInventory,
    },
}

/// Bounded typed lifecycle storage owned by the physical owner root. The four
/// domains remain structurally separate: scalar phase facts cannot consume a
/// timer slot, and terminal-grace capacity is the explicit reducer tombstone
/// bound rather than allocator `Vec::capacity()`.
#[must_use = "typed physical lifecycle slots require explicit application or stop settlement"]
pub(crate) struct OwnerPhysicalLifecycleSlots {
    session_id: SessionId,
    current_generation: LegGeneration,
    terminal_grace_capacity: usize,
    leg_activated: Option<OwnerOutputLifecycleAction>,
    resume_grace: Option<OwnerOutputLifecycleAction>,
    terminal_graces: Vec<OwnerOutputLifecycleAction>,
    session_expired: Option<OwnerOutputLifecycleAction>,
    stopped_reason: Option<OwnerOutputDrainStopReason>,
    audit_failed: bool,
    history: OwnerPhysicalLifecycleHistory,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use = "the lifecycle drive decides whether exact action ownership was retained"]
pub(crate) enum OwnerOutputLifecycleDrive {
    Inert,
    WrongPhase,
    Retained {
        settlement: OwnerOutputLifecycleSettlement,
    },
    Committed {
        settlement: OwnerOutputLifecycleSettlement,
    },
    AuditFailed,
}

/// Cold ownership/audit witness for the one-slot physical owner lifecycle
/// port. Historical settlement counters do not own work, but their checked
/// sums prove which terminal reason consumed a pending action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerOutputLifecycleInventory {
    pending_actions: usize,
    installed_actions: u64,
    fatal_discards: u64,
    cancelled_discards: u64,
    shutdown_discards: u64,
    audit_failed: bool,
}

/// One bounded lifecycle handoff between the owner output FIFO and its future
/// full root. Installation takes ownership of a non-cloneable action. Clearing
/// it requires a synchronous callback commit; pressure, cancellation of the
/// caller, or callback rejection leaves the exact action in this slot.
#[must_use = "dropping the owner lifecycle port may discard a live root action"]
pub(crate) struct OwnerOutputLifecyclePort {
    pending: Option<OwnerOutputLifecycleAction>,
    installed_actions: u64,
    fatal_discards: u64,
    cancelled_discards: u64,
    shutdown_discards: u64,
    audit_failed: bool,
}

/// Exact boundary between draining one Target page and scheduling its retained
/// controller continuation.  The scheduling fact is emitted only after every
/// output in the page has left this FIFO.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerOutputPageDrained {
    needs_continue: bool,
}

impl OwnerOutputPageDrained {
    pub(crate) const fn new(needs_continue: bool) -> Self {
        Self { needs_continue }
    }

    pub(crate) const fn needs_continue(self) -> bool {
        self.needs_continue
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerOutputDrainBlock {
    QueueUnavailable,
    Queue(LegOutboundQueueErrorKind),
}

impl OwnerOutputDrainBlock {
    /// Pressure and an ordered attach/recovery transaction may clear without
    /// replacing the retained output.  All other queue rejections are hard
    /// failures for the composition arbiter.
    pub(crate) fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::QueueUnavailable
                | Self::Queue(
                    LegOutboundQueueErrorKind::CapacityExceeded { .. }
                        | LegOutboundQueueErrorKind::AttachAcceptanceReserved
                        | LegOutboundQueueErrorKind::AttachRequestPending
                        | LegOutboundQueueErrorKind::AttachStatusPending
                        | LegOutboundQueueErrorKind::ClientReplacementRecoveryPending
                )
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerOutputDrainDrive {
    Idle,
    Blocked {
        reason: OwnerOutputDrainBlock,
    },
    Transmitted {
        page_drained: Option<OwnerOutputPageDrained>,
    },
    LifecycleInstallBlocked,
    LifecycleInstalled {
        page_drained: Option<OwnerOutputPageDrained>,
    },
    PageDrained(OwnerOutputPageDrained),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerOutputDrainConfigError {
    #[error("owner output FIFO frame capacity must be nonzero")]
    ZeroOutputs,
    #[error("owner output FIFO encoded-byte capacity must be nonzero")]
    ZeroEncodedBytes,
    #[error("owner output FIFO/scratch storage allocation failed")]
    StorageUnavailable,
}

struct PendingOwnerOutput {
    output: OwnerTargetOutput,
    encoded_bytes: usize,
}

/// One bounded page FIFO between the serialized Target controller and the
/// physical owner composition. It pre-reserves both FIFO entries and their
/// checked wire-length scratch at construction, and never clones an output:
/// an unsuccessful transport admission puts the exact returned frame back at
/// the head.
#[must_use = "dropping the owner output FIFO may discard live Target ownership"]
pub(crate) struct OwnerOutputDrain {
    max_outputs: usize,
    max_encoded_bytes: usize,
    owned_encoded_bytes: usize,
    pending: VecDeque<PendingOwnerOutput>,
    admission_encoded_lengths: Vec<usize>,
    page_needs_continue: Option<bool>,
}

impl OwnerOutputDrain {
    pub(crate) fn new(
        max_outputs: usize,
        max_encoded_bytes: usize,
    ) -> Result<Self, OwnerOutputDrainConfigError> {
        if max_outputs == 0 {
            return Err(OwnerOutputDrainConfigError::ZeroOutputs);
        }
        if max_encoded_bytes == 0 {
            return Err(OwnerOutputDrainConfigError::ZeroEncodedBytes);
        }
        let mut pending = VecDeque::new();
        pending
            .try_reserve_exact(max_outputs)
            .map_err(|_| OwnerOutputDrainConfigError::StorageUnavailable)?;
        let mut admission_encoded_lengths = Vec::new();
        admission_encoded_lengths
            .try_reserve_exact(max_outputs)
            .map_err(|_| OwnerOutputDrainConfigError::StorageUnavailable)?;
        Ok(Self {
            max_outputs,
            max_encoded_bytes,
            owned_encoded_bytes: 0,
            pending,
            admission_encoded_lengths,
            page_needs_continue: None,
        })
    }

    pub(crate) fn admit_outputs(
        &mut self,
        outputs: Vec<OwnerTargetOutput>,
        needs_continue: bool,
    ) -> Result<(), OwnerOutputAdmissionFailure> {
        // Admission runs after the controller has produced this exact page.
        // Keep allocator failure out of that transaction: construction
        // reserves one length slot for every logically admissible output and
        // every return path leaves the reusable scratch empty.
        self.admission_encoded_lengths.clear();
        if self.page_needs_continue.is_some() {
            return Err(OwnerOutputAdmissionFailure::new(
                outputs,
                needs_continue,
                OwnerOutputAdmissionErrorKind::PagePending,
            ));
        }
        if outputs.len() > self.max_outputs {
            let incoming_outputs = outputs.len();
            return Err(OwnerOutputAdmissionFailure::new(
                outputs,
                needs_continue,
                OwnerOutputAdmissionErrorKind::OutputCapacityExceeded {
                    max_outputs: self.max_outputs,
                    incoming_outputs,
                },
            ));
        }

        debug_assert!(self.admission_encoded_lengths.capacity() >= self.max_outputs);
        let mut incoming_encoded_bytes = 0_usize;
        for output in &outputs {
            let encoded_bytes = match output {
                OwnerTargetOutput::Transmit(frame) => match frame.encoded_len_checked() {
                    Ok(encoded_len) => encoded_len,
                    Err(source) => {
                        self.admission_encoded_lengths.clear();
                        return Err(OwnerOutputAdmissionFailure::new(
                            outputs,
                            needs_continue,
                            OwnerOutputAdmissionErrorKind::InvalidFrame(source),
                        ));
                    }
                },
                OwnerTargetOutput::LegActivated { .. }
                | OwnerTargetOutput::ResumeGraceStarted { .. }
                | OwnerTargetOutput::TerminalGraceStarted { .. }
                | OwnerTargetOutput::SessionExpired => 0,
                OwnerTargetOutput::NeedsResume { .. } => {
                    self.admission_encoded_lengths.clear();
                    return Err(OwnerOutputAdmissionFailure::new(
                        outputs,
                        needs_continue,
                        OwnerOutputAdmissionErrorKind::RawContinuationEscaped,
                    ));
                }
            };
            let Some(next) = incoming_encoded_bytes.checked_add(encoded_bytes) else {
                self.admission_encoded_lengths.clear();
                return Err(OwnerOutputAdmissionFailure::new(
                    outputs,
                    needs_continue,
                    OwnerOutputAdmissionErrorKind::EncodedByteCountOverflow,
                ));
            };
            incoming_encoded_bytes = next;
            self.admission_encoded_lengths.push(encoded_bytes);
        }
        if incoming_encoded_bytes > self.max_encoded_bytes {
            self.admission_encoded_lengths.clear();
            return Err(OwnerOutputAdmissionFailure::new(
                outputs,
                needs_continue,
                OwnerOutputAdmissionErrorKind::EncodedByteCapacityExceeded {
                    max_encoded_bytes: self.max_encoded_bytes,
                    incoming_encoded_bytes,
                },
            ));
        }

        debug_assert!(self.pending.is_empty());
        debug_assert_eq!(self.owned_encoded_bytes, 0);
        debug_assert!(self.pending.capacity() >= outputs.len());
        for (output, encoded_bytes) in outputs
            .into_iter()
            .zip(self.admission_encoded_lengths.drain(..))
        {
            self.pending.push_back(PendingOwnerOutput {
                output,
                encoded_bytes,
            });
        }
        debug_assert!(self.admission_encoded_lengths.is_empty());
        self.owned_encoded_bytes = incoming_encoded_bytes;
        self.page_needs_continue = Some(needs_continue);
        Ok(())
    }

    pub(crate) fn drive_one(
        &mut self,
        queue: Option<&mut LegOutboundQueue>,
        lifecycle: &mut OwnerOutputLifecyclePort,
    ) -> Result<OwnerOutputDrainDrive, OwnerOutputDrainAuditError> {
        let page_drained = match self.pending.front() {
            Some(pending) => {
                if matches!(pending.output, OwnerTargetOutput::NeedsResume { .. }) {
                    return Err(OwnerOutputDrainAuditError::RawContinuationEscaped);
                }
                self.preflight_finish_output(pending.encoded_bytes)?
            }
            None => {
                return Ok(match self.page_needs_continue.take() {
                    Some(needs_continue) => OwnerOutputDrainDrive::PageDrained(
                        OwnerOutputPageDrained::new(needs_continue),
                    ),
                    None => OwnerOutputDrainDrive::Idle,
                });
            }
        };
        let pending = self
            .pending
            .pop_front()
            .ok_or(OwnerOutputDrainAuditError::MissingPage)?;
        let PendingOwnerOutput {
            output,
            encoded_bytes,
        } = pending;
        match OwnerOutputLifecycleAction::try_from_output(output) {
            Ok(action) => match lifecycle.try_install(action) {
                Ok(()) => {
                    self.commit_finished_output(encoded_bytes, page_drained);
                    Ok(OwnerOutputDrainDrive::LifecycleInstalled { page_drained })
                }
                Err(action) => {
                    self.pending.push_front(PendingOwnerOutput {
                        output: action.into_output(),
                        encoded_bytes,
                    });
                    Ok(OwnerOutputDrainDrive::LifecycleInstallBlocked)
                }
            },
            Err(OwnerTargetOutput::Transmit(frame)) => {
                let Some(queue) = queue else {
                    self.pending.push_front(PendingOwnerOutput {
                        output: OwnerTargetOutput::Transmit(frame),
                        encoded_bytes,
                    });
                    return Ok(OwnerOutputDrainDrive::Blocked {
                        reason: OwnerOutputDrainBlock::QueueUnavailable,
                    });
                };
                if let Err(error) = queue.push(frame) {
                    let reason = OwnerOutputDrainBlock::Queue(error.kind().clone());
                    self.pending.push_front(PendingOwnerOutput {
                        output: OwnerTargetOutput::Transmit(error.into_frame()),
                        encoded_bytes,
                    });
                    return Ok(OwnerOutputDrainDrive::Blocked { reason });
                }
                self.commit_finished_output(encoded_bytes, page_drained);
                Ok(OwnerOutputDrainDrive::Transmitted { page_drained })
            }
            Err(OwnerTargetOutput::NeedsResume { resume }) => {
                self.pending.push_front(PendingOwnerOutput {
                    output: OwnerTargetOutput::NeedsResume { resume },
                    encoded_bytes,
                });
                Err(OwnerOutputDrainAuditError::RawContinuationEscaped)
            }
            Err(
                output @ (OwnerTargetOutput::LegActivated { .. }
                | OwnerTargetOutput::ResumeGraceStarted { .. }
                | OwnerTargetOutput::TerminalGraceStarted { .. }
                | OwnerTargetOutput::SessionExpired),
            ) => {
                self.pending.push_front(PendingOwnerOutput {
                    output,
                    encoded_bytes,
                });
                Err(OwnerOutputDrainAuditError::MissingPage)
            }
        }
    }

    fn preflight_finish_output(
        &self,
        encoded_bytes: usize,
    ) -> Result<Option<OwnerOutputPageDrained>, OwnerOutputDrainAuditError> {
        let needs_continue = self
            .page_needs_continue
            .ok_or(OwnerOutputDrainAuditError::MissingPage)?;
        let Some(next_bytes) = self.owned_encoded_bytes.checked_sub(encoded_bytes) else {
            return Err(OwnerOutputDrainAuditError::StoredByteCountMismatch);
        };
        if self.pending.len() > 1 {
            return Ok(None);
        }
        if next_bytes != 0 {
            return Err(OwnerOutputDrainAuditError::StoredByteCountMismatch);
        }
        Ok(Some(OwnerOutputPageDrained::new(needs_continue)))
    }

    fn commit_finished_output(
        &mut self,
        encoded_bytes: usize,
        page_drained: Option<OwnerOutputPageDrained>,
    ) {
        self.owned_encoded_bytes -= encoded_bytes;
        if page_drained.is_some() {
            debug_assert!(self.pending.is_empty());
            debug_assert_eq!(self.owned_encoded_bytes, 0);
            let removed = self.page_needs_continue.take();
            debug_assert_eq!(
                removed,
                page_drained.map(OwnerOutputPageDrained::needs_continue)
            );
        }
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerOutputAdmissionErrorKind {
    #[error("an owner output page is already pending")]
    PagePending,
    #[error("owner output page has {incoming_outputs} outputs, capacity is {max_outputs}")]
    OutputCapacityExceeded {
        max_outputs: usize,
        incoming_outputs: usize,
    },
    #[error(
        "owner output page has {incoming_encoded_bytes} encoded bytes, capacity is {max_encoded_bytes}"
    )]
    EncodedByteCapacityExceeded {
        max_encoded_bytes: usize,
        incoming_encoded_bytes: usize,
    },
    #[error("owner output encoded-byte count overflowed")]
    EncodedByteCountOverflow,
    #[error("owner output frame is invalid: {0}")]
    InvalidFrame(crate::resumable::ProtocolError),
    #[error("raw Target continuation escaped the controller")]
    RawContinuationEscaped,
}

/// Atomic admission rejection.  It owns the exact unconsumed page and the
/// continuation scheduling fact; neither is copied into the drain on failure.
#[must_use = "owner output admission failure returns the exact page ownership"]
pub(crate) struct OwnerOutputAdmissionFailure {
    outputs: Vec<OwnerTargetOutput>,
    needs_continue: bool,
    kind: OwnerOutputAdmissionErrorKind,
}

/// Cold fail-closed witness for a page that could not enter the bounded FIFO.
/// `known_encoded_bytes` is exact only when both audit flags are zero; either
/// flag still keeps `is_exact_zero` false without inventing a byte count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerOutputAdmissionInventory {
    pending_outputs: usize,
    known_encoded_bytes: usize,
    unencodable_frames: usize,
    raw_continuations: usize,
    byte_count_overflowed: bool,
    page_open: bool,
    continuation_needed: bool,
}

impl OwnerOutputAdmissionInventory {
    pub(crate) const fn pending_outputs(self) -> usize {
        self.pending_outputs
    }

    pub(crate) const fn known_encoded_bytes(self) -> usize {
        self.known_encoded_bytes
    }

    pub(crate) const fn unencodable_frames(self) -> usize {
        self.unencodable_frames
    }

    pub(crate) const fn raw_continuations(self) -> usize {
        self.raw_continuations
    }

    pub(crate) const fn byte_count_overflowed(self) -> bool {
        self.byte_count_overflowed
    }

    pub(crate) const fn page_open(self) -> bool {
        self.page_open
    }

    pub(crate) const fn continuation_needed(self) -> bool {
        self.continuation_needed
    }

    pub(crate) const fn is_exact_zero(self) -> bool {
        self.pending_outputs == 0
            && self.known_encoded_bytes == 0
            && self.unencodable_frames == 0
            && self.raw_continuations == 0
            && !self.byte_count_overflowed
            && !self.page_open
            && !self.continuation_needed
    }
}

impl OwnerOutputAdmissionFailure {
    fn new(
        outputs: Vec<OwnerTargetOutput>,
        needs_continue: bool,
        kind: OwnerOutputAdmissionErrorKind,
    ) -> Self {
        Self {
            outputs,
            needs_continue,
            kind,
        }
    }

    pub(crate) const fn kind(&self) -> &OwnerOutputAdmissionErrorKind {
        &self.kind
    }

    pub(crate) fn inventory(&self) -> OwnerOutputAdmissionInventory {
        let mut known_encoded_bytes = 0_usize;
        let mut unencodable_frames = 0_usize;
        let mut raw_continuations = 0_usize;
        let mut byte_count_overflowed = false;
        for output in &self.outputs {
            match output {
                OwnerTargetOutput::Transmit(frame) => match frame.encoded_len_checked() {
                    Ok(encoded_len) => {
                        if !byte_count_overflowed {
                            match known_encoded_bytes.checked_add(encoded_len) {
                                Some(next) => known_encoded_bytes = next,
                                None => byte_count_overflowed = true,
                            }
                        }
                    }
                    Err(_) => unencodable_frames += 1,
                },
                OwnerTargetOutput::NeedsResume { .. } => raw_continuations += 1,
                OwnerTargetOutput::LegActivated { .. }
                | OwnerTargetOutput::ResumeGraceStarted { .. }
                | OwnerTargetOutput::TerminalGraceStarted { .. }
                | OwnerTargetOutput::SessionExpired => {}
            }
        }
        OwnerOutputAdmissionInventory {
            pending_outputs: self.outputs.len(),
            known_encoded_bytes,
            unencodable_frames,
            raw_continuations,
            byte_count_overflowed,
            page_open: true,
            continuation_needed: self.needs_continue,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (Vec<OwnerTargetOutput>, bool, OwnerOutputAdmissionErrorKind) {
        (self.outputs, self.needs_continue, self.kind)
    }
}

impl std::fmt::Debug for OwnerOutputAdmissionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnerOutputAdmissionFailure")
            .field("outputs", &self.outputs.len())
            .field("needs_continue", &self.needs_continue)
            .field("kind", &self.kind)
            .finish()
    }
}

impl std::fmt::Display for OwnerOutputAdmissionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.kind.fmt(formatter)
    }
}

impl std::error::Error for OwnerOutputAdmissionFailure {}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerOutputDrainAuditError {
    #[error("owner output stored byte count does not match its exact outputs")]
    StoredByteCountMismatch,
    #[error("owner output page marker is missing")]
    MissingPage,
    #[error("raw Target continuation escaped the controller")]
    RawContinuationEscaped,
    #[error("owner output frame became invalid while retained: {0}")]
    InvalidRetainedFrame(crate::resumable::ProtocolError),
    #[error("owner output FIFO exceeds its configured capacity")]
    CapacityExceeded,
}

/// Cold, independently recomputed ownership witness for the root output FIFO.
/// A page marker is live ownership even when its output vector is empty,
/// because it carries the sole continuation scheduling fact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerOutputDrainInventory {
    pending_outputs: usize,
    summed_encoded_bytes: usize,
    stored_encoded_bytes: usize,
    page_open: bool,
    continuation_needed: bool,
}

impl OwnerOutputDrainInventory {
    pub(crate) const fn pending_outputs(self) -> usize {
        self.pending_outputs
    }

    pub(crate) const fn summed_encoded_bytes(self) -> usize {
        self.summed_encoded_bytes
    }

    pub(crate) const fn stored_encoded_bytes(self) -> usize {
        self.stored_encoded_bytes
    }

    pub(crate) const fn page_open(self) -> bool {
        self.page_open
    }

    pub(crate) const fn continuation_needed(self) -> bool {
        self.continuation_needed
    }

    pub(crate) const fn is_exact_zero(self) -> bool {
        self.pending_outputs == 0
            && self.summed_encoded_bytes == 0
            && self.stored_encoded_bytes == 0
            && !self.page_open
            && !self.continuation_needed
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerOutputDrainStopReason {
    Fatal,
    Cancelled,
    Shutdown,
}

/// Exact residual returned when the composition stops driving this FIFO.  It
/// owns every undelivered `OwnerTargetOutput` in FIFO order plus the page-level
/// continuation fact, so terminal/cancel/shutdown cannot report zero by merely
/// dropping the drain object.
#[must_use = "stopped owner output ownership must enter the root terminal ledger"]
pub(crate) struct StoppedOwnerOutputDrain {
    pending: VecDeque<PendingOwnerOutput>,
    page_needs_continue: Option<bool>,
    reason: OwnerOutputDrainStopReason,
    inventory: OwnerOutputDrainInventory,
}

impl StoppedOwnerOutputDrain {
    pub(crate) const fn reason(&self) -> OwnerOutputDrainStopReason {
        self.reason
    }

    pub(crate) const fn inventory(&self) -> OwnerOutputDrainInventory {
        self.inventory
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Vec<OwnerTargetOutput>,
        Option<OwnerOutputPageDrained>,
        OwnerOutputDrainStopReason,
        OwnerOutputDrainInventory,
    ) {
        let page = self.page_needs_continue.map(OwnerOutputPageDrained::new);
        (
            self.pending
                .into_iter()
                .map(|pending| pending.output)
                .collect(),
            page,
            self.reason,
            self.inventory,
        )
    }
}

impl std::fmt::Debug for StoppedOwnerOutputDrain {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoppedOwnerOutputDrain")
            .field("reason", &self.reason)
            .field("inventory", &self.inventory)
            .field("outputs", &"[REDACTED]")
            .finish()
    }
}

#[must_use = "a failed stop audit still owns the complete live output drain"]
pub(crate) struct OwnerOutputDrainStopFailure {
    drain: OwnerOutputDrain,
    source: OwnerOutputDrainAuditError,
}

impl OwnerOutputDrainStopFailure {
    pub(crate) const fn source_kind(&self) -> &OwnerOutputDrainAuditError {
        &self.source
    }

    pub(crate) fn into_parts(self) -> (OwnerOutputDrain, OwnerOutputDrainAuditError) {
        (self.drain, self.source)
    }
}

impl std::fmt::Debug for OwnerOutputDrainStopFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OwnerOutputDrainStopFailure")
            .field("source", &self.source)
            .field("drain", &"[REDACTED]")
            .finish()
    }
}

impl OwnerOutputDrain {
    /// Recomputes every retained frame's checked wire length without encoding
    /// or allocation, then compares the independent sum with both per-entry
    /// lengths and the stored aggregate. This is deliberately cold-path work
    /// for terminal/shutdown/no-oracle accounting.
    pub(crate) fn inventory(
        &self,
    ) -> Result<OwnerOutputDrainInventory, OwnerOutputDrainAuditError> {
        if self.pending.len() > self.max_outputs
            || self.owned_encoded_bytes > self.max_encoded_bytes
        {
            return Err(OwnerOutputDrainAuditError::CapacityExceeded);
        }
        if self.page_needs_continue.is_none()
            && (!self.pending.is_empty() || self.owned_encoded_bytes != 0)
        {
            return Err(OwnerOutputDrainAuditError::MissingPage);
        }
        let mut summed_encoded_bytes = 0_usize;
        for pending in &self.pending {
            let actual = match &pending.output {
                OwnerTargetOutput::Transmit(frame) => frame
                    .encoded_len_checked()
                    .map_err(OwnerOutputDrainAuditError::InvalidRetainedFrame)?,
                OwnerTargetOutput::LegActivated { .. }
                | OwnerTargetOutput::ResumeGraceStarted { .. }
                | OwnerTargetOutput::TerminalGraceStarted { .. }
                | OwnerTargetOutput::SessionExpired => 0,
                OwnerTargetOutput::NeedsResume { .. } => {
                    return Err(OwnerOutputDrainAuditError::RawContinuationEscaped);
                }
            };
            if actual != pending.encoded_bytes {
                return Err(OwnerOutputDrainAuditError::StoredByteCountMismatch);
            }
            summed_encoded_bytes = summed_encoded_bytes
                .checked_add(actual)
                .ok_or(OwnerOutputDrainAuditError::StoredByteCountMismatch)?;
        }
        if summed_encoded_bytes != self.owned_encoded_bytes {
            return Err(OwnerOutputDrainAuditError::StoredByteCountMismatch);
        }
        let continuation_needed = self.page_needs_continue.unwrap_or(false);
        Ok(OwnerOutputDrainInventory {
            pending_outputs: self.pending.len(),
            summed_encoded_bytes,
            stored_encoded_bytes: self.owned_encoded_bytes,
            page_open: self.page_needs_continue.is_some(),
            continuation_needed,
        })
    }

    pub(crate) fn stop(
        self,
        reason: OwnerOutputDrainStopReason,
    ) -> Result<StoppedOwnerOutputDrain, OwnerOutputDrainStopFailure> {
        let inventory = match self.inventory() {
            Ok(inventory) => inventory,
            Err(source) => {
                return Err(OwnerOutputDrainStopFailure {
                    drain: self,
                    source,
                });
            }
        };
        let Self {
            pending,
            page_needs_continue,
            ..
        } = self;
        Ok(StoppedOwnerOutputDrain {
            pending,
            page_needs_continue,
            reason,
            inventory,
        })
    }
}

#[cfg(test)]
mod tests;

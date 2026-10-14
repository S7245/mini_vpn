//! Bounded policy kernel for reverse-path replacement.
//!
//! This module owns policy state only. Its crate-private constructors currently
//! accept trusted typed inputs: transport actors must still be wired as the
//! physical source of standby identity/arrival time, and the session reducer
//! must still be wired as the source of replay/application-ACK observations.
//! That provenance wiring is deliberately outside this isolated kernel, so
//! local kernel tests do not by themselves prove a no-oracle composition.

mod progress;

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use thiserror::Error;

use crate::resumable::{
    AttachNonce, ByteOffset, Direction, FeatureSet, HintSequence, LegControlFrame,
    LegControlRecord, LegGeneration, SessionFlowId, SessionId, StandbyNonce, SwitchHintCause,
};

use super::leg_io::{LegControlEnqueued, LegOutboundQueue};
use super::quinn_leg_composition::QuinnLegCompositionGate;
use super::standby::{
    OwnerStandbyHintBinding, OwnerStandbyHintEnqueueErrorKind, OwnerStandbyHintEvidenceError,
    OwnerStandbySlot,
};
use super::two_leg::{SimTime, SimTimeError};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DirectionalFlowKey {
    flow_id: SessionFlowId,
    direction: Direction,
}

impl DirectionalFlowKey {
    const fn new(flow_id: SessionFlowId, direction: Direction) -> Self {
        Self { flow_id, direction }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PositiveApplicationAck {
    offset: ByteOffset,
    arrived_at: SimTime,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OwnerDirectionalProgress {
    generation: LegGeneration,
    outstanding_episode: u64,
    oldest_unacknowledged: Option<ByteOffset>,
    outstanding_since: Option<SimTime>,
    latest_positive_ack: Option<PositiveApplicationAck>,
    terminal: bool,
}

/// Exact oldest replay and latest ACK per direction, without history.
pub(crate) struct OwnerReverseProgress {
    capacity: OwnerProgressCapacity,
    entries: progress::OwnerProgressStorage,
    revision: u64,
}

/// Two directions for live plus grace-retained flows; live slots can be reused
/// before their tombstones expire. This count witness is not a storage Layout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerProgressCapacity {
    max_entries: usize,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerProgressCapacityError {
    #[error("owner live-flow plus terminal-tombstone capacity overflows usize")]
    TrackedFlowCapacityOverflow,
    #[error("two directions per tracked owner flow overflows usize")]
    DirectionalEntryCapacityOverflow,
}

/// Read-only current policy state for one exact live direction. This is not a
/// reducer observation or history: it is the bounded state already retained
/// by [`OwnerReverseProgress`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct OwnerReverseDirectionSnapshot {
    oldest_unacknowledged: Option<ByteOffset>,
    outstanding_since: Option<SimTime>,
    latest_positive_ack: Option<(ByteOffset, SimTime)>,
    terminal: bool,
}

impl OwnerReverseDirectionSnapshot {
    pub(crate) const fn oldest_unacknowledged(self) -> Option<ByteOffset> {
        self.oldest_unacknowledged
    }

    pub(crate) const fn outstanding_since(self) -> Option<SimTime> {
        self.outstanding_since
    }

    pub(crate) const fn latest_positive_ack(self) -> Option<(ByteOffset, SimTime)> {
        self.latest_positive_ack
    }

    pub(crate) const fn terminal(self) -> bool {
        self.terminal
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnerProgressError {
    #[error("2 * max_flows overflows the exact progress-entry capacity")]
    CapacityLimitOverflow,
    #[error("reverse-progress concrete storage Layout is unrepresentable")]
    StorageLayout,
    #[error("reverse-progress storage partition or ordered index is corrupt")]
    StorageCorrupt,
    #[error("reverse-progress capacity is exhausted at {max_entries} entries")]
    CapacityExceeded { max_entries: usize },
    #[error("the flow direction is already registered under another lifecycle")]
    FlowAlreadyRegistered,
    #[error("the flow direction is not registered")]
    UnknownFlowDirection,
    #[error("the observation generation does not match the live flow")]
    GenerationMismatch,
    #[error("reverse-progress generation did not advance from {previous:?} to {observed:?}")]
    GenerationDidNotAdvance {
        previous: LegGeneration,
        observed: LegGeneration,
    },
    #[error("the flow direction is terminal")]
    TerminalFlowDirection,
    #[error("the flow direction has no outstanding reducer replay")]
    NoOutstandingReplay,
    #[error("the outstanding-replay episode counter is exhausted")]
    OutstandingEpisodeExhausted,
    #[error("the reverse-progress revision counter is exhausted")]
    RevisionExhausted,
    #[error("the bounded reverse-stall scan entry counter overflowed")]
    ScanCountOverflow,
    #[error("the bounded reverse-stall scan lost its controller-owned cursor")]
    ScanCursorInvariant,
    #[error("outstanding replay is missing its exact admission timestamp")]
    MissingOutstandingAdmissionTime,
    #[error("oldest outstanding offset regressed from {previous:?} to {observed:?}")]
    OldestOutstandingRegressed {
        previous: ByteOffset,
        observed: ByteOffset,
    },
    #[error("application ACK did not advance from {previous:?} to {observed:?}")]
    ApplicationAckDidNotAdvance {
        previous: ByteOffset,
        observed: ByteOffset,
    },
    #[error("reverse-progress clock evidence is invalid: {0}")]
    Clock(SimTimeError),
}

/// Process-local identity of the exact registered B actor.
#[derive(Clone)]
pub(crate) struct StandbyActorSeal(Arc<()>);

impl StandbyActorSeal {
    #[cfg(test)]
    pub(crate) fn mint() -> Self {
        Self(Arc::new(()))
    }

    /// Mints the process-local identity for the exact B incarnation retained
    /// by the physical composition. The unforgeable borrowed gate prevents a
    /// sibling policy caller from creating an ambient B identity.
    pub(super) fn mint_for_exact_b(_gate: &QuinnLegCompositionGate) -> Self {
        Self(Arc::new(()))
    }

    fn same_actor(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl fmt::Debug for StandbyActorSeal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StandbyActorSeal([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ProbeFreshness {
    acknowledged_at: SimTime,
    fresh_through: SimTime,
}

/// Correlation and bounded probe freshness for one authenticated standby.
#[derive(Clone)]
pub(crate) struct RegisteredStandby {
    session_id: SessionId,
    generation: LegGeneration,
    standby_nonce: StandbyNonce,
    actor_seal: StandbyActorSeal,
    live: bool,
    freshness: Option<ProbeFreshness>,
}

impl RegisteredStandby {
    pub(super) fn new(
        session_id: SessionId,
        generation: LegGeneration,
        standby_nonce: StandbyNonce,
        actor_seal: StandbyActorSeal,
    ) -> Self {
        Self {
            session_id,
            generation,
            standby_nonce,
            actor_seal,
            live: true,
            freshness: None,
        }
    }

    pub(crate) fn observe_probe_ack(
        &mut self,
        actor_seal: &StandbyActorSeal,
        session_id: SessionId,
        generation: LegGeneration,
        standby_nonce: StandbyNonce,
        acknowledged_at: SimTime,
        fresh_for: Duration,
    ) -> Result<(), StandbyEvidenceError> {
        self.require_exact(actor_seal, session_id, generation, standby_nonce)?;
        if !self.live {
            return Err(StandbyEvidenceError::NotLive);
        }
        if let Some(previous) = self.freshness {
            acknowledged_at
                .checked_duration_since(previous.acknowledged_at)
                .map_err(StandbyEvidenceError::Clock)?;
        }
        let fresh_through = acknowledged_at
            .checked_add(fresh_for)
            .map_err(StandbyEvidenceError::Clock)?;
        self.freshness = Some(ProbeFreshness {
            acknowledged_at,
            fresh_through,
        });
        Ok(())
    }

    pub(crate) fn mark_not_live(
        &mut self,
        actor_seal: &StandbyActorSeal,
    ) -> Result<(), StandbyEvidenceError> {
        if !self.actor_seal.same_actor(actor_seal) {
            return Err(StandbyEvidenceError::WrongActor);
        }
        self.live = false;
        Ok(())
    }

    fn require_exact(
        &self,
        actor_seal: &StandbyActorSeal,
        session_id: SessionId,
        generation: LegGeneration,
        standby_nonce: StandbyNonce,
    ) -> Result<(), StandbyEvidenceError> {
        if !self.actor_seal.same_actor(actor_seal) {
            return Err(StandbyEvidenceError::WrongActor);
        }
        if self.session_id != session_id {
            return Err(StandbyEvidenceError::WrongSession);
        }
        if self.generation != generation {
            return Err(StandbyEvidenceError::WrongGeneration);
        }
        if self.standby_nonce != standby_nonce {
            return Err(StandbyEvidenceError::WrongNonce);
        }
        Ok(())
    }

    fn is_exact_and_fresh_at(
        &self,
        actor_seal: &StandbyActorSeal,
        session_id: SessionId,
        generation: LegGeneration,
        standby_nonce: StandbyNonce,
        observed_at: SimTime,
    ) -> Result<(), StandbyEvidenceError> {
        self.require_exact(actor_seal, session_id, generation, standby_nonce)?;
        if !self.live {
            return Err(StandbyEvidenceError::NotLive);
        }
        let freshness = self
            .freshness
            .ok_or(StandbyEvidenceError::NoMatchingProbeAck)?;
        if observed_at < freshness.acknowledged_at || observed_at > freshness.fresh_through {
            return Err(StandbyEvidenceError::Stale);
        }
        Ok(())
    }
}

impl fmt::Debug for RegisteredStandby {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegisteredStandby")
            .field("correlation", &"[REDACTED]")
            .field("generation", &self.generation)
            .field("live", &self.live)
            .field("freshness", &self.freshness)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum StandbyEvidenceError {
    #[error("standby evidence came from another physical actor")]
    WrongActor,
    #[error("standby evidence names another session")]
    WrongSession,
    #[error("standby evidence names another generation")]
    WrongGeneration,
    #[error("standby evidence names another registration nonce")]
    WrongNonce,
    #[error("the registered standby is not live")]
    NotLive,
    #[error("no matching positive standby probe ACK exists")]
    NoMatchingProbeAck,
    #[error("the matching standby probe ACK is stale at the observed arrival")]
    Stale,
    #[error("standby freshness clock evidence is invalid: {0}")]
    Clock(SimTimeError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StallFingerprint {
    generation: LegGeneration,
    flow_id: SessionFlowId,
    direction: Direction,
    outstanding_episode: u64,
    oldest_unacknowledged: ByteOffset,
    baseline_offset: Option<ByteOffset>,
    baseline_at: SimTime,
}

/// One exact hint retained by the owner while B's bounded control queue is
/// pressured. The frame never crosses the issuer interface by borrow: the
/// admission transaction moves it once into the queue adapter and requires an
/// exact rejected frame back before retry can be restored.
#[derive(Debug, PartialEq, Eq)]
struct PendingOwnerSwitchHint {
    frame: LegControlFrame,
    sequence: HintSequence,
    fingerprint: StallFingerprint,
    progress_revision: u64,
}

#[cfg(test)]
impl PendingOwnerSwitchHint {
    fn frame(&self) -> &LegControlFrame {
        &self.frame
    }

    fn hint_sequence(&self) -> HintSequence {
        self.sequence
    }
}

/// Positive result of moving the one pending hint into a queue adapter.
///
/// `R` is deliberately retained rather than discarded so the physical root
/// can keep the queue's non-cloneable enqueue receipt in its exact ownership
/// ledger. The value-only kernel tests use a small deterministic receipt; that
/// does not claim physical queue provenance.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "the physical root must retain the queue admission receipt"]
pub(crate) struct AdmittedOwnerSwitchHint<R> {
    sequence: HintSequence,
    receipt: R,
}

impl<R> AdmittedOwnerSwitchHint<R> {
    pub(crate) const fn hint_sequence(&self) -> HintSequence {
        self.sequence
    }

    pub(crate) fn into_receipt(self) -> R {
        self.receipt
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum OwnerHintAdmissionError {
    NoPendingHint,
    PolicyInvalidated(OwnerHintError),
    PhysicalMismatch(OwnerStandbyHintEvidenceError),
    PhysicalInvariant(OwnerStandbyHintEnqueueErrorKind),
    QueueRejected(OwnerStandbyHintEnqueueErrorKind),
}

#[cfg(test)]
enum TestOwnerHintAdmission<R> {
    Admit(R),
    Reject(OwnerStandbyHintEnqueueErrorKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OwnerHintPreparation {
    Prepared { hint_sequence: HintSequence },
    Retry { hint_sequence: HintSequence },
    Replaced { hint_sequence: HintSequence },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OwnerReverseStallCandidate {
    generation: LegGeneration,
    flow_id: SessionFlowId,
    direction: Direction,
    oldest_unacknowledged: ByteOffset,
    baseline_offset: Option<ByteOffset>,
    baseline_at: SimTime,
    outstanding_episode: u64,
    deadline: SimTime,
}

impl OwnerReverseStallCandidate {
    const fn fingerprint(self) -> StallFingerprint {
        StallFingerprint {
            generation: self.generation,
            flow_id: self.flow_id,
            direction: self.direction,
            outstanding_episode: self.outstanding_episode,
            oldest_unacknowledged: self.oldest_unacknowledged,
            baseline_offset: self.baseline_offset,
            baseline_at: self.baseline_at,
        }
    }
}

const OWNER_REVERSE_SCAN_BUDGET_PER_TURN: usize = 32;

/// Exact number of root turns needed for one complete production reverse
/// progress scan. Keeping the ceiling calculation beside the selector's
/// `.take` bound prevents harness capacity math from copying the private
/// production quantum.
pub(crate) const fn owner_reverse_scan_turns(direction_entries: usize) -> usize {
    let partial = if direction_entries % OWNER_REVERSE_SCAN_BUDGET_PER_TURN == 0 {
        0
    } else {
        1
    };
    direction_entries / OWNER_REVERSE_SCAN_BUDGET_PER_TURN + partial
}

struct OwnerReverseStallScanCursor {
    revision: u64,
    last_key: Option<DirectionalFlowKey>,
    scanned_entries: usize,
    earliest: Option<OwnerReverseStallCandidate>,
}

impl OwnerReverseStallScanCursor {
    const fn new(revision: u64) -> Self {
        Self {
            revision,
            last_key: None,
            scanned_entries: 0,
            earliest: None,
        }
    }
}

enum OwnerReverseStallScan {
    Scanning,
    Complete(Option<OwnerReverseStallCandidate>),
}

/// Opaque result of the issuer's bounded aggregate ACK-stall scan.
///
/// A due value retains the exact reducer-derived candidate privately.  The
/// controller may move it into the immediately following physical-B prepare
/// transaction, but no caller can choose or inspect its flow, direction,
/// offset, ACK baseline, or outstanding episode.
pub(super) enum OwnerHintDueCursor {
    Scanning,
    Inert,
    Pending { deadline: SimTime },
    Due(OwnerHintDueCandidate),
}

pub(super) struct OwnerHintDueCandidate {
    candidate: OwnerReverseStallCandidate,
    observed_at: SimTime,
    progress_revision: u64,
}

/// O(1) owner policy state for authenticated reverse ACK-stall hints.
pub(crate) struct OwnerSwitchHintIssuer {
    session_id: SessionId,
    generation: LegGeneration,
    features: FeatureSet,
    standby_nonce: StandbyNonce,
    binding: OwnerStandbyHintBinding,
    stall_after: Duration,
    scan: Option<OwnerReverseStallScanCursor>,
    next_sequence: Option<HintSequence>,
    pending: Option<PendingOwnerSwitchHint>,
    admitted: Option<StallFingerprint>,
    last_published_sequence: Option<HintSequence>,
}

impl OwnerSwitchHintIssuer {
    pub(super) fn from_exact_binding(
        binding: OwnerStandbyHintBinding,
        stall_after: Duration,
        first_sequence: HintSequence,
    ) -> Result<Self, OwnerSwitchHintBuildFailure> {
        let contract = binding.contract();
        let session_id = contract.session_id();
        let generation = contract.generation();
        let features = contract.features();
        if !features.contains(FeatureSet::STANDBY_CONTROL_V1) {
            return Err(OwnerSwitchHintBuildFailure {
                binding,
                kind: OwnerHintError::FeatureNotNegotiated,
            });
        }
        Ok(Self {
            session_id,
            generation,
            features,
            standby_nonce: contract.standby_nonce(),
            binding,
            stall_after,
            scan: None,
            next_sequence: Some(first_sequence),
            pending: None,
            admitted: None,
            last_published_sequence: None,
        })
    }

    #[cfg(test)]
    pub(super) fn new(
        session_id: SessionId,
        generation: LegGeneration,
        features: FeatureSet,
        standby: RegisteredStandby,
        stall_after: Duration,
        first_sequence: HintSequence,
    ) -> Result<Self, OwnerHintError> {
        if standby.session_id != session_id || standby.generation != generation {
            return Err(OwnerHintError::StandbyCorrelationMismatch);
        }
        Self::from_exact_binding(
            OwnerStandbyHintBinding::for_policy_test(
                session_id,
                generation,
                standby.standby_nonce,
                features,
            ),
            stall_after,
            first_sequence,
        )
        .map_err(OwnerSwitchHintBuildFailure::into_kind)
    }

    fn validate_physical_standby(
        &mut self,
        standby_slot: &OwnerStandbySlot,
        now: SimTime,
    ) -> Result<(), OwnerHintError> {
        match standby_slot.validate_exact_hint_binding_at(&self.binding, now) {
            Ok(()) => Ok(()),
            Err(error) => Err(self.settle_prepare_validation_failure(error)),
        }
    }

    fn settle_prepare_validation_failure(
        &mut self,
        error: OwnerStandbyHintEvidenceError,
    ) -> OwnerHintError {
        match error {
            OwnerStandbyHintEvidenceError::WrongRegistrationBinding => {}
            OwnerStandbyHintEvidenceError::NoRegisteredStandby
            | OwnerStandbyHintEvidenceError::EndpointOrQueueLost
            | OwnerStandbyHintEvidenceError::ProbeNotFresh => self.pending = None,
        }
        OwnerHintError::PhysicalStandby(error)
    }

    /// Selects the aggregate owner ACK-stall work using only reducer-owned
    /// bounded progress and the controller-captured session time.  Inert or
    /// refreshed-not-due progress invalidates an unadmitted pending frame;
    /// queue-pressure retry remains exact while the same episode stays due.
    pub(super) fn select_next_due_hint(
        &mut self,
        progress: &OwnerReverseProgress,
        observed_at: SimTime,
    ) -> Result<OwnerHintDueCursor, OwnerHintError> {
        if self.admitted.is_some() {
            return Ok(OwnerHintDueCursor::Inert);
        }
        if let Some(pending) = self.pending.as_ref()
            && pending.progress_revision == progress.revision()
        {
            let candidate = progress
                .exact_reverse_stall_candidate(pending.fingerprint, self.stall_after)
                .map_err(OwnerHintError::Progress)?;
            if let Some(candidate) = candidate
                && candidate.fingerprint() == pending.fingerprint
                && observed_at >= candidate.deadline
            {
                return Ok(OwnerHintDueCursor::Due(OwnerHintDueCandidate {
                    candidate,
                    observed_at,
                    progress_revision: progress.revision(),
                }));
            }
            self.pending = None;
        }
        match progress
            .scan_reverse_stall(self.generation, self.stall_after, &mut self.scan)
            .map_err(OwnerHintError::Progress)?
        {
            OwnerReverseStallScan::Scanning => Ok(OwnerHintDueCursor::Scanning),
            OwnerReverseStallScan::Complete(None) => {
                self.pending = None;
                Ok(OwnerHintDueCursor::Inert)
            }
            OwnerReverseStallScan::Complete(Some(candidate))
                if observed_at < candidate.deadline =>
            {
                self.pending = None;
                Ok(OwnerHintDueCursor::Pending {
                    deadline: candidate.deadline,
                })
            }
            OwnerReverseStallScan::Complete(Some(candidate)) => {
                Ok(OwnerHintDueCursor::Due(OwnerHintDueCandidate {
                    candidate,
                    observed_at,
                    progress_revision: progress.revision(),
                }))
            }
        }
    }

    /// Converts one controller-selected due cursor into the exact pending
    /// frame only after the registered physical B remains fresh at the same
    /// captured time.  The cursor is non-cloneable and exposes no scalar
    /// constructor, so a harness cannot redirect the selected work.
    pub(super) fn prepare_selected_due_hint_for_exact_slot(
        &mut self,
        selected: OwnerHintDueCandidate,
        standby_slot: &OwnerStandbySlot,
    ) -> Result<OwnerHintPreparation, OwnerHintError> {
        if self.admitted.is_some() {
            return Err(OwnerHintError::HintAlreadyAdmitted);
        }
        if selected.candidate.generation != self.generation {
            return Err(OwnerHintError::Progress(
                OwnerProgressError::GenerationMismatch,
            ));
        }
        self.validate_physical_standby(standby_slot, selected.observed_at)?;
        self.prepare_candidate(selected.candidate, selected.progress_revision)
    }

    /// Production hint selection is driven only by reducer-owned bounded
    /// progress. No caller-selected flow/direction/oldest value crosses this
    /// interface.
    pub(super) fn prepare_next_due_hint_for_exact_slot(
        &mut self,
        progress: &OwnerReverseProgress,
        standby_slot: &OwnerStandbySlot,
        now: SimTime,
    ) -> Result<OwnerHintPreparation, OwnerHintError> {
        match self.select_next_due_hint(progress, now)? {
            OwnerHintDueCursor::Scanning => Err(OwnerHintError::ScanInProgress),
            OwnerHintDueCursor::Inert => Err(OwnerHintError::NoOutstandingReplay),
            OwnerHintDueCursor::Pending { .. } => Err(OwnerHintError::AckStallNotDue),
            OwnerHintDueCursor::Due(selected) => {
                self.prepare_selected_due_hint_for_exact_slot(selected, standby_slot)
            }
        }
    }

    /// Value-only selection retained only for the isolated necessary-policy
    /// tests.  Production must use `prepare_next_due_hint_for_exact_slot` so
    /// frame construction cannot precede physical B revalidation.
    #[cfg(test)]
    pub(super) fn prepare_next_due_hint(
        &mut self,
        progress: &OwnerReverseProgress,
        now: SimTime,
    ) -> Result<OwnerHintPreparation, OwnerHintError> {
        if self.admitted.is_some() {
            return Err(OwnerHintError::HintAlreadyAdmitted);
        }
        loop {
            match self.select_next_due_hint(progress, now)? {
                OwnerHintDueCursor::Scanning => continue,
                OwnerHintDueCursor::Inert => return Err(OwnerHintError::NoOutstandingReplay),
                OwnerHintDueCursor::Pending { .. } => {
                    return Err(OwnerHintError::AckStallNotDue);
                }
                OwnerHintDueCursor::Due(selected) => {
                    return self.prepare_candidate(selected.candidate, selected.progress_revision);
                }
            }
        }
    }

    /// Exact scalar selection remains available only to the isolated policy
    /// tests. Production callers must use `prepare_next_due_hint`.
    #[cfg(test)]
    pub(super) fn prepare_hint(
        &mut self,
        progress: &OwnerReverseProgress,
        flow_id: SessionFlowId,
        direction: Direction,
        claimed_oldest: ByteOffset,
        now: SimTime,
    ) -> Result<OwnerHintPreparation, OwnerHintError> {
        if self.admitted.is_some() {
            return Err(OwnerHintError::HintAlreadyAdmitted);
        }
        if direction != Direction::TargetToClient {
            return Err(OwnerHintError::WrongDirection);
        }
        let entry = progress
            .exact_entry(self.generation, flow_id, direction)
            .map_err(OwnerHintError::Progress)?;
        if entry.terminal {
            return Err(OwnerHintError::TerminalFlowDirection);
        }
        let exact_oldest = entry
            .oldest_unacknowledged
            .ok_or(OwnerHintError::NoOutstandingReplay)?;
        if claimed_oldest != exact_oldest {
            return Err(OwnerHintError::WrongOldest {
                expected: exact_oldest,
                claimed: claimed_oldest,
            });
        }
        let outstanding_since = entry
            .outstanding_since
            .ok_or(OwnerHintError::MissingOutstandingAdmissionTime)?;
        let (baseline_offset, baseline_at) = match entry.latest_positive_ack {
            Some(ack) if ack.arrived_at >= outstanding_since => (Some(ack.offset), ack.arrived_at),
            _ => (None, outstanding_since),
        };
        let deadline = baseline_at
            .checked_add(self.stall_after)
            .map_err(OwnerHintError::Clock)?;
        if now < deadline {
            return Err(OwnerHintError::AckStallNotDue);
        }
        self.prepare_candidate(
            OwnerReverseStallCandidate {
                generation: self.generation,
                flow_id,
                direction,
                oldest_unacknowledged: exact_oldest,
                baseline_offset,
                baseline_at,
                outstanding_episode: entry.outstanding_episode,
                deadline,
            },
            progress.revision(),
        )
    }

    fn prepare_candidate(
        &mut self,
        candidate: OwnerReverseStallCandidate,
        progress_revision: u64,
    ) -> Result<OwnerHintPreparation, OwnerHintError> {
        let fingerprint = candidate.fingerprint();
        if let Some(pending) = &self.pending
            && pending.fingerprint == fingerprint
            && pending.progress_revision == progress_revision
        {
            return Ok(OwnerHintPreparation::Retry {
                hint_sequence: pending.sequence,
            });
        }
        if self.admitted.is_some() {
            return Err(OwnerHintError::HintAlreadyAdmitted);
        }
        let sequence = self
            .next_sequence
            .ok_or(OwnerHintError::HintSequenceExhausted)?;
        let frame = LegControlFrame::try_new(
            self.generation,
            LegControlRecord::SwitchHint {
                session_id: self.session_id,
                standby_nonce: self.standby_nonce,
                hint_sequence: sequence,
                flow_id: candidate.flow_id,
                direction: candidate.direction,
                oldest_unacknowledged: candidate.oldest_unacknowledged,
                cause: SwitchHintCause::ReverseApplicationAckStall,
            },
            self.features,
        )
        .map_err(|_| OwnerHintError::ProtocolConstruction)?;
        let disposition = if self.pending.is_some() {
            OwnerHintPreparation::Replaced {
                hint_sequence: sequence,
            }
        } else {
            OwnerHintPreparation::Prepared {
                hint_sequence: sequence,
            }
        };
        let pending = PendingOwnerSwitchHint {
            frame,
            sequence,
            fingerprint,
            progress_revision,
        };
        self.pending = Some(pending);
        Ok(disposition)
    }

    /// Revalidates and moves the exact pending frame into the authenticated
    /// physical B slot's sole FIFO.
    ///
    /// Both the reducer-owned episode and the physical slot's exact probe
    /// freshness are checked in this synchronous turn. Invalidated work is
    /// settled before the FIFO can observe a frame. The only production
    /// enqueue call returns either its non-cloneable receipt or the exact
    /// unconsumed frame nested in a private failure, so clone-and-enqueue plus
    /// false rejection and substitute-frame return are not representable at
    /// this boundary.
    pub(super) fn try_admit_pending(
        &mut self,
        progress: &OwnerReverseProgress,
        now: SimTime,
        standby_slot: &OwnerStandbySlot,
        queue: &mut LegOutboundQueue,
    ) -> Result<AdmittedOwnerSwitchHint<LegControlEnqueued>, OwnerHintAdmissionError> {
        let pending = self.take_revalidated_pending(progress, now)?;
        let PendingOwnerSwitchHint {
            frame,
            sequence,
            fingerprint,
            progress_revision,
        } = pending;
        match standby_slot.enqueue_exact_switch_hint(&self.binding, now, frame, queue) {
            Ok(receipt) => Ok(self.commit_admitted(sequence, fingerprint, receipt)),
            Err(failure) => {
                let (frame, kind) = failure.into_parts();
                match kind {
                    OwnerStandbyHintEnqueueErrorKind::Evidence(
                        OwnerStandbyHintEvidenceError::WrongRegistrationBinding,
                    ) => {
                        self.pending = Some(PendingOwnerSwitchHint {
                            frame,
                            sequence,
                            fingerprint,
                            progress_revision,
                        });
                        Err(OwnerHintAdmissionError::PhysicalMismatch(
                            OwnerStandbyHintEvidenceError::WrongRegistrationBinding,
                        ))
                    }
                    OwnerStandbyHintEnqueueErrorKind::Evidence(error) => {
                        let _policy_invalidated_frame = frame;
                        Err(OwnerHintAdmissionError::PolicyInvalidated(
                            OwnerHintError::PhysicalStandby(error),
                        ))
                    }
                    kind @ OwnerStandbyHintEnqueueErrorKind::Queue(_) => {
                        self.pending = Some(PendingOwnerSwitchHint {
                            frame,
                            sequence,
                            fingerprint,
                            progress_revision,
                        });
                        Err(OwnerHintAdmissionError::QueueRejected(kind))
                    }
                    kind @ OwnerStandbyHintEnqueueErrorKind::WrongCorrelation => {
                        self.pending = Some(PendingOwnerSwitchHint {
                            frame,
                            sequence,
                            fingerprint,
                            progress_revision,
                        });
                        Err(OwnerHintAdmissionError::PhysicalInvariant(kind))
                    }
                }
            }
        }
    }

    #[cfg(test)]
    fn try_admit_pending_for_test<R>(
        &mut self,
        progress: &OwnerReverseProgress,
        now: SimTime,
        admission: TestOwnerHintAdmission<R>,
    ) -> Result<AdmittedOwnerSwitchHint<R>, OwnerHintAdmissionError> {
        let pending = self.take_revalidated_pending(progress, now)?;
        match admission {
            TestOwnerHintAdmission::Admit(receipt) => {
                let PendingOwnerSwitchHint {
                    sequence,
                    fingerprint,
                    ..
                } = pending;
                Ok(self.commit_admitted(sequence, fingerprint, receipt))
            }
            TestOwnerHintAdmission::Reject(kind) => {
                self.pending = Some(pending);
                Err(OwnerHintAdmissionError::QueueRejected(kind))
            }
        }
    }

    fn take_revalidated_pending(
        &mut self,
        progress: &OwnerReverseProgress,
        now: SimTime,
    ) -> Result<PendingOwnerSwitchHint, OwnerHintAdmissionError> {
        let pending_fingerprint = self
            .pending
            .as_ref()
            .map(|pending| pending.fingerprint)
            .ok_or(OwnerHintAdmissionError::NoPendingHint)?;
        // A registry-wide revision is only a scan invalidation token.  It is
        // not proof that this exact reducer-owned episode changed: an ACK or
        // lifecycle transition for another flow may advance it.  Revalidate
        // the pending hint's exact fingerprint below so the caller receives
        // the precise policy result and unrelated progress cannot discard an
        // otherwise current physical frame.
        let candidate =
            match progress.exact_reverse_stall_candidate(pending_fingerprint, self.stall_after) {
                Ok(Some(candidate)) if now >= candidate.deadline => candidate,
                Ok(Some(_)) => {
                    self.pending = None;
                    return Err(OwnerHintAdmissionError::PolicyInvalidated(
                        OwnerHintError::AckStallNotDue,
                    ));
                }
                Ok(None) => {
                    self.pending = None;
                    return Err(OwnerHintAdmissionError::PolicyInvalidated(
                        OwnerHintError::NoOutstandingReplay,
                    ));
                }
                Err(error) => {
                    self.pending = None;
                    return Err(OwnerHintAdmissionError::PolicyInvalidated(
                        OwnerHintError::Progress(error),
                    ));
                }
            };
        if candidate.fingerprint() != pending_fingerprint {
            self.pending = None;
            return Err(OwnerHintAdmissionError::PolicyInvalidated(
                OwnerHintError::PendingHintNoLongerCurrent,
            ));
        }
        let pending = self
            .pending
            .take()
            .ok_or(OwnerHintAdmissionError::NoPendingHint)?;
        Ok(pending)
    }

    fn commit_admitted<R>(
        &mut self,
        sequence: HintSequence,
        fingerprint: StallFingerprint,
        receipt: R,
    ) -> AdmittedOwnerSwitchHint<R> {
        self.admitted = Some(fingerprint);
        self.last_published_sequence = Some(sequence);
        self.next_sequence = sequence.checked_next().ok();
        AdmittedOwnerSwitchHint { sequence, receipt }
    }

    #[cfg(test)]
    fn pending_hint(&self) -> Option<&PendingOwnerSwitchHint> {
        self.pending.as_ref()
    }

    pub(crate) const fn last_published_sequence(&self) -> Option<HintSequence> {
        self.last_published_sequence
    }
}

/// Issuer construction failure that retains the exact once-claimed physical
/// registration authority.  The future root can explicitly settle or retain
/// that authority; unsupported feature negotiation must not silently drop it.
pub(super) struct OwnerSwitchHintBuildFailure {
    binding: OwnerStandbyHintBinding,
    kind: OwnerHintError,
}

impl OwnerSwitchHintBuildFailure {
    pub(super) const fn kind(&self) -> OwnerHintError {
        self.kind
    }

    pub(super) const fn binding(&self) -> &OwnerStandbyHintBinding {
        &self.binding
    }

    pub(super) fn into_binding(self) -> OwnerStandbyHintBinding {
        self.binding
    }

    #[cfg(test)]
    fn into_kind(self) -> OwnerHintError {
        self.kind
    }
}

impl fmt::Debug for OwnerSwitchHintBuildFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSwitchHintBuildFailure")
            .field("kind", &self.kind)
            .field("binding", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(super) enum OwnerHintError {
    #[error("STANDBY_CONTROL_V1 was not negotiated")]
    FeatureNotNegotiated,
    #[error("the issuer and registered standby correlations differ")]
    StandbyCorrelationMismatch,
    #[error("switch hints are restricted to Target-to-client replay")]
    WrongDirection,
    #[error("the reverse flow direction is terminal")]
    TerminalFlowDirection,
    #[error("the reverse flow direction has no outstanding replay")]
    NoOutstandingReplay,
    #[error("outstanding replay is missing its exact admission timestamp")]
    MissingOutstandingAdmissionTime,
    #[error("claimed oldest offset {claimed:?} differs from reducer-owned {expected:?}")]
    WrongOldest {
        expected: ByteOffset,
        claimed: ByteOffset,
    },
    #[error("the positive application-ACK stall deadline is not due")]
    AckStallNotDue,
    #[error("the bounded reverse-stall scan has more fixed-budget work")]
    ScanInProgress,
    #[error("this switch epoch already admitted a hint")]
    HintAlreadyAdmitted,
    #[error("the pending hint no longer names the selected outstanding episode")]
    PendingHintNoLongerCurrent,
    #[error("the monotonic hint sequence is exhausted")]
    HintSequenceExhausted,
    #[error("protocol rejected a policy-constructed SWITCH_HINT")]
    ProtocolConstruction,
    #[error("owner progress rejected the hint request: {0}")]
    Progress(OwnerProgressError),
    #[error("exact physical owner standby rejected the hint request: {0}")]
    PhysicalStandby(OwnerStandbyHintEvidenceError),
    #[error("owner hint clock evidence is invalid: {0}")]
    Clock(SimTimeError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClientFlowState {
    generation: LegGeneration,
    accepted_frontier: ByteOffset,
    terminal: bool,
}

#[derive(Debug)]
pub(crate) struct StandbyHintArrival {
    actor_seal: StandbyActorSeal,
    frame: LegControlFrame,
    arrived_at: SimTime,
}

impl StandbyHintArrival {
    #[cfg(test)]
    pub(crate) fn new(
        actor_seal: StandbyActorSeal,
        frame: LegControlFrame,
        arrived_at: SimTime,
    ) -> Self {
        Self {
            actor_seal,
            frame,
            arrived_at,
        }
    }
}

/// One validator-issued capability for one authenticated B hint.
///
/// The value is deliberately non-cloneable: successful validation advances
/// the monotonic sequence before returning it, and the arbiter consumes that
/// one admission capability by value.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AuthenticatedSwitchHint {
    session_id: SessionId,
    generation: LegGeneration,
    standby_nonce: StandbyNonce,
    hint_sequence: HintSequence,
    flow_id: SessionFlowId,
    oldest_unacknowledged: ByteOffset,
    arrived_at: SimTime,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClientHintSnapshot {
    generation: LegGeneration,
    entries: BTreeMap<DirectionalFlowKey, ClientFlowState>,
    last_accepted_sequence: Option<HintSequence>,
}

/// Snapshot-inert validator for hints arriving through the exact registered B.
pub(crate) struct ClientHintValidator {
    session_id: SessionId,
    generation: LegGeneration,
    features: FeatureSet,
    standby: RegisteredStandby,
    max_entries: usize,
    entries: BTreeMap<DirectionalFlowKey, ClientFlowState>,
    last_accepted_sequence: Option<HintSequence>,
}

impl ClientHintValidator {
    pub(crate) fn new(
        session_id: SessionId,
        generation: LegGeneration,
        features: FeatureSet,
        standby: RegisteredStandby,
        max_flows: usize,
    ) -> Result<Self, ClientHintError> {
        if !features.contains(FeatureSet::STANDBY_CONTROL_V1) {
            return Err(ClientHintError::FeatureNotNegotiated);
        }
        if standby.session_id != session_id || standby.generation != generation {
            return Err(ClientHintError::StandbyCorrelationMismatch);
        }
        let max_entries = max_flows
            .checked_mul(2)
            .ok_or(ClientHintError::CapacityLimitOverflow)?;
        Ok(Self {
            session_id,
            generation,
            features,
            standby,
            max_entries,
            entries: BTreeMap::new(),
            last_accepted_sequence: None,
        })
    }

    pub(crate) fn observe_standby_probe_ack(
        &mut self,
        actor_seal: &StandbyActorSeal,
        session_id: SessionId,
        generation: LegGeneration,
        standby_nonce: StandbyNonce,
        acknowledged_at: SimTime,
        fresh_for: Duration,
    ) -> Result<(), StandbyEvidenceError> {
        self.standby.observe_probe_ack(
            actor_seal,
            session_id,
            generation,
            standby_nonce,
            acknowledged_at,
            fresh_for,
        )
    }

    pub(crate) fn register_live_flow(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
        accepted_frontier: ByteOffset,
    ) -> Result<(), ClientHintError> {
        if generation != self.generation {
            return Err(ClientHintError::WrongGeneration);
        }
        let key = DirectionalFlowKey::new(flow_id, direction);
        if let Some(existing) = self.entries.get(&key) {
            return if *existing
                == (ClientFlowState {
                    generation,
                    accepted_frontier,
                    terminal: false,
                }) {
                Ok(())
            } else {
                Err(ClientHintError::FlowAlreadyRegistered)
            };
        }
        if self.entries.len() == self.max_entries {
            return Err(ClientHintError::CapacityExceeded {
                max_entries: self.max_entries,
            });
        }
        self.entries.insert(
            key,
            ClientFlowState {
                generation,
                accepted_frontier,
                terminal: false,
            },
        );
        Ok(())
    }

    pub(crate) fn advance_accepted_frontier(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
        accepted_frontier: ByteOffset,
    ) -> Result<(), ClientHintError> {
        let entry = self.exact_flow_mut(generation, flow_id, direction)?;
        if entry.terminal {
            return Err(ClientHintError::TerminalFlowDirection);
        }
        if accepted_frontier < entry.accepted_frontier {
            return Err(ClientHintError::AcceptedFrontierRegressed);
        }
        entry.accepted_frontier = accepted_frontier;
        Ok(())
    }

    pub(crate) fn mark_terminal(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<(), ClientHintError> {
        self.exact_flow_mut(generation, flow_id, direction)?
            .terminal = true;
        Ok(())
    }

    /// Releases all directional hint state only after the physical root proves
    /// that flow-wide terminal grace committed. Until then terminal entries
    /// remain present so late hints are rejected as terminal rather than being
    /// misclassified as unknown. An already retired flow is idempotent.
    pub(crate) fn retire_flow_after_terminal_grace(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
    ) -> Result<usize, ClientHintError> {
        if generation != self.generation {
            return Err(ClientHintError::WrongGeneration);
        }
        let mut tracked = false;
        for entry in self
            .entries
            .iter()
            .filter_map(|(key, entry)| (key.flow_id == flow_id).then_some(entry))
        {
            tracked = true;
            if !entry.terminal {
                return Err(ClientHintError::FlowNotFullyTerminal);
            }
        }
        if !tracked {
            return Ok(0);
        }
        let before = self.entries.len();
        self.entries.retain(|key, _| key.flow_id != flow_id);
        Ok(before - self.entries.len())
    }

    fn validate_arrival(
        &self,
        arrival: StandbyHintArrival,
    ) -> Result<AuthenticatedSwitchHint, ClientHintError> {
        if !self.features.contains(FeatureSet::STANDBY_CONTROL_V1) {
            return Err(ClientHintError::FeatureNotNegotiated);
        }
        if !self.standby.actor_seal.same_actor(&arrival.actor_seal) {
            return Err(ClientHintError::WrongStandbyActor);
        }
        if arrival.frame.leg_generation() != self.generation {
            return Err(ClientHintError::WrongGeneration);
        }
        let LegControlRecord::SwitchHint {
            session_id,
            standby_nonce,
            hint_sequence,
            flow_id,
            direction,
            oldest_unacknowledged,
            cause,
        } = arrival.frame.record()
        else {
            return Err(ClientHintError::WrongRecordKind);
        };
        if *session_id != self.session_id {
            return Err(ClientHintError::WrongSession);
        }
        if *standby_nonce != self.standby.standby_nonce {
            return Err(ClientHintError::WrongStandbyNonce);
        }
        if *cause != SwitchHintCause::ReverseApplicationAckStall {
            return Err(ClientHintError::WrongCause);
        }
        if *direction != Direction::TargetToClient {
            return Err(ClientHintError::WrongDirection);
        }
        let flow = self
            .entries
            .get(&DirectionalFlowKey::new(*flow_id, *direction))
            .ok_or(ClientHintError::UnknownFlowDirection)?;
        if flow.generation != self.generation {
            return Err(ClientHintError::WrongGeneration);
        }
        if flow.terminal {
            return Err(ClientHintError::TerminalFlowDirection);
        }
        if *oldest_unacknowledged > flow.accepted_frontier {
            return Err(ClientHintError::HintAheadOfAcceptedFrontier {
                accepted: flow.accepted_frontier,
                hinted: *oldest_unacknowledged,
            });
        }
        self.standby
            .is_exact_and_fresh_at(
                &arrival.actor_seal,
                *session_id,
                self.generation,
                *standby_nonce,
                arrival.arrived_at,
            )
            .map_err(ClientHintError::Standby)?;
        if let Some(last) = self.last_accepted_sequence
            && *hint_sequence <= last
        {
            return Err(ClientHintError::HintSequenceNotIncreasing {
                last,
                observed: *hint_sequence,
            });
        }

        Ok(AuthenticatedSwitchHint {
            session_id: *session_id,
            generation: self.generation,
            standby_nonce: *standby_nonce,
            hint_sequence: *hint_sequence,
            flow_id: *flow_id,
            oldest_unacknowledged: *oldest_unacknowledged,
            arrived_at: arrival.arrived_at,
        })
    }

    /// Validates the exact B arrival and routes it through the one switch
    /// arbiter in the same synchronous turn.  The monotonic hint sequence is
    /// committed only after the arbiter accepts the trigger; admission-closed
    /// or correlation rejection therefore leaves the validator retryable.
    pub(crate) fn transact_arrival_with_arbiter(
        &mut self,
        arrival: StandbyHintArrival,
        arbiter: &mut ClientSwitchArbiter,
    ) -> Result<SwitchTriggerOutcome, ClientHintArbiterError> {
        let accepted = self
            .validate_arrival(arrival)
            .map_err(ClientHintArbiterError::Hint)?;
        let accepted_sequence = accepted.hint_sequence;
        let outcome = arbiter
            .admit_authenticated_hint(accepted)
            .map_err(ClientHintArbiterError::Arbiter)?;
        self.last_accepted_sequence = Some(accepted_sequence);
        Ok(outcome)
    }

    /// Value-only policy adapter retained only by the isolated kernel tests.
    /// Production must use `transact_arrival_with_arbiter` so validation cannot
    /// advance sequence state before downstream switch admission succeeds.
    #[cfg(test)]
    pub(crate) fn validate(
        &mut self,
        arrival: StandbyHintArrival,
    ) -> Result<AuthenticatedSwitchHint, ClientHintError> {
        let accepted = self.validate_arrival(arrival)?;
        self.last_accepted_sequence = Some(accepted.hint_sequence);
        Ok(accepted)
    }

    pub(crate) fn snapshot(&self) -> ClientHintSnapshot {
        ClientHintSnapshot {
            generation: self.generation,
            entries: self.entries.clone(),
            last_accepted_sequence: self.last_accepted_sequence,
        }
    }

    fn exact_flow_mut(
        &mut self,
        generation: LegGeneration,
        flow_id: SessionFlowId,
        direction: Direction,
    ) -> Result<&mut ClientFlowState, ClientHintError> {
        if generation != self.generation {
            return Err(ClientHintError::WrongGeneration);
        }
        self.entries
            .get_mut(&DirectionalFlowKey::new(flow_id, direction))
            .ok_or(ClientHintError::UnknownFlowDirection)
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum ClientHintError {
    #[error("STANDBY_CONTROL_V1 was not negotiated")]
    FeatureNotNegotiated,
    #[error("validator and registered standby correlations differ")]
    StandbyCorrelationMismatch,
    #[error("2 * max_flows overflows the exact client-flow capacity")]
    CapacityLimitOverflow,
    #[error("client hint-flow capacity is exhausted at {max_entries} entries")]
    CapacityExceeded { max_entries: usize },
    #[error("the client flow direction is already registered under different state")]
    FlowAlreadyRegistered,
    #[error("hint arrived through another physical standby actor")]
    WrongStandbyActor,
    #[error("hint names another session")]
    WrongSession,
    #[error("hint names another generation")]
    WrongGeneration,
    #[error("hint names another standby registration nonce")]
    WrongStandbyNonce,
    #[error("the classified B record is not SWITCH_HINT")]
    WrongRecordKind,
    #[error("the switch-hint cause is not the closed reverse ACK-stall cause")]
    WrongCause,
    #[error("switch hint is not for Target-to-client replay")]
    WrongDirection,
    #[error("the hinted client flow direction is unknown")]
    UnknownFlowDirection,
    #[error("the hinted client flow direction is terminal")]
    TerminalFlowDirection,
    #[error("flow-wide terminal grace cannot retire while a direction is still live")]
    FlowNotFullyTerminal,
    #[error("hint offset {hinted:?} is ahead of local accepted frontier {accepted:?}")]
    HintAheadOfAcceptedFrontier {
        accepted: ByteOffset,
        hinted: ByteOffset,
    },
    #[error("hint sequence {observed:?} does not advance beyond {last:?}")]
    HintSequenceNotIncreasing {
        last: HintSequence,
        observed: HintSequence,
    },
    #[error("accepted frontier regressed")]
    AcceptedFrontierRegressed,
    #[error("registered standby rejected the hint arrival: {0}")]
    Standby(StandbyEvidenceError),
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum ClientHintArbiterError {
    #[error("client physical hint validation failed: {0}")]
    Hint(ClientHintError),
    #[error("the single switch arbiter rejected the authenticated hint: {0}")]
    Arbiter(SwitchArbiterError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SwitchEpoch(NonZeroU64);

impl SwitchEpoch {
    pub(crate) fn new(value: u64) -> Result<Self, SwitchArbiterError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(SwitchArbiterError::ZeroSwitchEpoch)
    }

    pub(crate) fn get(self) -> u64 {
        self.0.get()
    }
}

/// One exact hard-terminal observation consumed by the switch arbiter.
///
/// The physical actor-terminal adapter is intentionally still unwired. Until
/// an opaque adapter can consume the actor's exact terminal witness, only
/// isolated policy tests may construct this value.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct HardFailureTrigger {
    session_id: SessionId,
    generation: LegGeneration,
    failed_at: SimTime,
}

impl HardFailureTrigger {
    #[cfg(test)]
    const fn new(session_id: SessionId, generation: LegGeneration, failed_at: SimTime) -> Self {
        Self {
            session_id,
            generation,
            failed_at,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalAckStallEpisodeState {
    Available,
    TriggerIssued,
    TriggerConsumed,
}

struct ClientLocalAckStallEpisode {
    seal: Arc<()>,
    outstanding_admitted_at: SimTime,
    baseline_at: SimTime,
    stall_after: Duration,
    due_at: SimTime,
    state: LocalAckStallEpisodeState,
}

/// Bounded source of one exact local application-ACK stall trigger.
///
/// The source owns at most one outstanding episode. Clearing that episode or
/// refreshing its positive-ACK baseline rotates the private seal, so a trigger
/// already selected by the scheduler becomes typed stale evidence rather than
/// ambient scalar authority.
pub(crate) struct ClientLocalAckStallSource {
    session_id: SessionId,
    generation: LegGeneration,
    last_observed_at: Option<SimTime>,
    episode: Option<ClientLocalAckStallEpisode>,
}

impl ClientLocalAckStallSource {
    pub(crate) const fn new(session_id: SessionId, generation: LegGeneration) -> Self {
        Self {
            session_id,
            generation,
            last_observed_at: None,
            episode: None,
        }
    }

    /// Read-only correlation used by the production root when it receives a
    /// later reducer or actor observation. The opaque episode seal remains
    /// private to this source.
    pub(crate) const fn session_id(&self) -> SessionId {
        self.session_id
    }

    /// Read-only correlation used by the production root. Generation changes
    /// still require replacing the whole bounded source rather than mutating
    /// its identity in place.
    pub(crate) const fn generation(&self) -> LegGeneration {
        self.generation
    }

    /// The scheduler may borrow the current deadline without gaining trigger
    /// authority. Only `take_due_trigger` can mint the opaque trigger.
    pub(crate) const fn due_at(&self) -> Option<SimTime> {
        match self.episode.as_ref() {
            Some(episode) => Some(episode.due_at),
            None => None,
        }
    }

    pub(crate) fn begin_outstanding(
        &mut self,
        admitted_at: SimTime,
        stall_after: Duration,
    ) -> Result<(), ClientLocalAckStallError> {
        if self.episode.is_some() {
            return Err(ClientLocalAckStallError::OutstandingEpisodeAlreadyActive);
        }
        if let Some(previous) = self.last_observed_at {
            admitted_at
                .checked_duration_since(previous)
                .map_err(ClientLocalAckStallError::Clock)?;
        }
        let due_at = admitted_at
            .checked_add(stall_after)
            .map_err(ClientLocalAckStallError::Clock)?;
        self.episode = Some(ClientLocalAckStallEpisode {
            seal: Arc::new(()),
            outstanding_admitted_at: admitted_at,
            baseline_at: admitted_at,
            stall_after,
            due_at,
            state: LocalAckStallEpisodeState::Available,
        });
        self.last_observed_at = Some(admitted_at);
        Ok(())
    }

    pub(crate) fn observe_positive_ack(
        &mut self,
        arrived_at: SimTime,
    ) -> Result<(), ClientLocalAckStallError> {
        let episode = self
            .episode
            .as_mut()
            .ok_or(ClientLocalAckStallError::NoOutstandingEpisode)?;
        if episode.state == LocalAckStallEpisodeState::TriggerConsumed {
            return Err(ClientLocalAckStallError::TriggerAlreadyConsumed);
        }
        arrived_at
            .checked_duration_since(episode.baseline_at)
            .map_err(ClientLocalAckStallError::Clock)?;
        let due_at = arrived_at
            .checked_add(episode.stall_after)
            .map_err(ClientLocalAckStallError::Clock)?;
        episode.seal = Arc::new(());
        episode.baseline_at = arrived_at;
        episode.due_at = due_at;
        episode.state = LocalAckStallEpisodeState::Available;
        self.last_observed_at = Some(arrived_at);
        Ok(())
    }

    pub(crate) fn clear_outstanding(&mut self) -> bool {
        self.episode.take().is_some()
    }

    pub(crate) fn take_due_trigger(
        &mut self,
        now: SimTime,
    ) -> Result<LocalAckStallTrigger, ClientLocalAckStallError> {
        let episode = self
            .episode
            .as_mut()
            .ok_or(ClientLocalAckStallError::NoOutstandingEpisode)?;
        now.checked_duration_since(episode.baseline_at)
            .map_err(ClientLocalAckStallError::Clock)?;
        if now < episode.due_at {
            return Err(ClientLocalAckStallError::DeadlineNotDue {
                due_at: episode.due_at,
            });
        }
        match episode.state {
            LocalAckStallEpisodeState::Available => {}
            LocalAckStallEpisodeState::TriggerIssued => {
                return Err(ClientLocalAckStallError::TriggerAlreadyIssued);
            }
            LocalAckStallEpisodeState::TriggerConsumed => {
                return Err(ClientLocalAckStallError::TriggerAlreadyConsumed);
            }
        }
        episode.state = LocalAckStallEpisodeState::TriggerIssued;
        Ok(LocalAckStallTrigger {
            session_id: self.session_id,
            generation: self.generation,
            episode_seal: Arc::clone(&episode.seal),
            outstanding_admitted_at: episode.outstanding_admitted_at,
            baseline_at: episode.baseline_at,
            due_at: episode.due_at,
        })
    }

    fn consume_trigger(
        &mut self,
        trigger: &LocalAckStallTrigger,
    ) -> Result<(), ClientLocalAckStallError> {
        if trigger.session_id != self.session_id || trigger.generation != self.generation {
            return Err(ClientLocalAckStallError::WrongSourceCorrelation);
        }
        let episode = self
            .episode
            .as_mut()
            .ok_or(ClientLocalAckStallError::StaleOutstandingEpisode)?;
        if !Arc::ptr_eq(&episode.seal, &trigger.episode_seal)
            || episode.outstanding_admitted_at != trigger.outstanding_admitted_at
            || episode.baseline_at != trigger.baseline_at
            || episode.due_at != trigger.due_at
        {
            return Err(ClientLocalAckStallError::StaleOutstandingEpisode);
        }
        match episode.state {
            LocalAckStallEpisodeState::TriggerIssued => {
                episode.state = LocalAckStallEpisodeState::TriggerConsumed;
                Ok(())
            }
            LocalAckStallEpisodeState::Available => {
                Err(ClientLocalAckStallError::TriggerWasNotIssued)
            }
            LocalAckStallEpisodeState::TriggerConsumed => {
                Err(ClientLocalAckStallError::TriggerAlreadyConsumed)
            }
        }
    }

    /// Restores one exact issued trigger after the destination arbiter rejects
    /// its session/generation correlation. The trigger is still source-bound,
    /// so a mismatched or stale proof cannot reopen another episode.
    fn restore_trigger_after_correlation_rejection(
        &mut self,
        trigger: &LocalAckStallTrigger,
    ) -> Result<(), ClientLocalAckStallError> {
        if trigger.session_id != self.session_id || trigger.generation != self.generation {
            return Err(ClientLocalAckStallError::WrongSourceCorrelation);
        }
        let episode = self
            .episode
            .as_mut()
            .ok_or(ClientLocalAckStallError::StaleOutstandingEpisode)?;
        if !Arc::ptr_eq(&episode.seal, &trigger.episode_seal)
            || episode.outstanding_admitted_at != trigger.outstanding_admitted_at
            || episode.baseline_at != trigger.baseline_at
            || episode.due_at != trigger.due_at
        {
            return Err(ClientLocalAckStallError::StaleOutstandingEpisode);
        }
        match episode.state {
            LocalAckStallEpisodeState::TriggerIssued => {
                episode.state = LocalAckStallEpisodeState::Available;
                Ok(())
            }
            LocalAckStallEpisodeState::Available => {
                Err(ClientLocalAckStallError::TriggerWasNotIssued)
            }
            LocalAckStallEpisodeState::TriggerConsumed => {
                Err(ClientLocalAckStallError::TriggerAlreadyConsumed)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum ClientLocalAckStallError {
    #[error("another local outstanding-replay episode is already active")]
    OutstandingEpisodeAlreadyActive,
    #[error("no local outstanding-replay episode is active")]
    NoOutstandingEpisode,
    #[error("local application-ACK stall deadline {due_at:?} is not due")]
    DeadlineNotDue { due_at: SimTime },
    #[error("one trigger from this local outstanding episode is already issued")]
    TriggerAlreadyIssued,
    #[error("the local outstanding episode trigger was already consumed")]
    TriggerAlreadyConsumed,
    #[error("the local stall trigger did not come from this source")]
    WrongSourceCorrelation,
    #[error("the local stall trigger names a stale outstanding episode or ACK baseline")]
    StaleOutstandingEpisode,
    #[error("the local stall trigger was not issued by its source")]
    TriggerWasNotIssued,
    #[error("local ACK-stall clock evidence is invalid: {0}")]
    Clock(SimTimeError),
}

/// Non-cloneable proof that one exact current local outstanding episode
/// reached its absolute application-ACK stall deadline.
pub(crate) struct LocalAckStallTrigger {
    session_id: SessionId,
    generation: LegGeneration,
    episode_seal: Arc<()>,
    outstanding_admitted_at: SimTime,
    baseline_at: SimTime,
    due_at: SimTime,
}

impl fmt::Debug for LocalAckStallTrigger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalAckStallTrigger")
            .field("correlation", &"[REDACTED]")
            .field("outstanding_admitted_at", &self.outstanding_admitted_at)
            .field("baseline_at", &self.baseline_at)
            .field("due_at", &self.due_at)
            .finish()
    }
}

impl PartialEq for LocalAckStallTrigger {
    fn eq(&self, other: &Self) -> bool {
        self.session_id == other.session_id
            && self.generation == other.generation
            && Arc::ptr_eq(&self.episode_seal, &other.episode_seal)
            && self.outstanding_admitted_at == other.outstanding_admitted_at
            && self.baseline_at == other.baseline_at
            && self.due_at == other.due_at
    }
}

impl Eq for LocalAckStallTrigger {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SwitchTriggerKind {
    HardFailure,
    LocalAckStall,
    AuthenticatedHint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AttachIntent {
    session_id: SessionId,
    current_generation: LegGeneration,
    requested_generation: LegGeneration,
    standby_nonce: StandbyNonce,
    attach_nonce: AttachNonce,
    switch_epoch: SwitchEpoch,
    initial_trigger: SwitchTriggerKind,
    post_loss: bool,
}

impl AttachIntent {
    pub(crate) const fn session_id(self) -> SessionId {
        self.session_id
    }

    pub(crate) const fn current_generation(self) -> LegGeneration {
        self.current_generation
    }

    pub(crate) const fn requested_generation(self) -> LegGeneration {
        self.requested_generation
    }

    pub(crate) const fn standby_nonce(self) -> StandbyNonce {
        self.standby_nonce
    }

    pub(crate) const fn attach_nonce(self) -> AttachNonce {
        self.attach_nonce
    }

    pub(crate) const fn switch_epoch(self) -> SwitchEpoch {
        self.switch_epoch
    }

    pub(crate) const fn initial_trigger(self) -> SwitchTriggerKind {
        self.initial_trigger
    }

    pub(crate) const fn is_post_loss(self) -> bool {
        self.post_loss
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SwitchTriggerDisposition {
    Started,
    Deduplicated,
    UpgradedToPostLoss,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SwitchShutdownDisposition {
    ClosedIdle,
    ClosedPendingSettlement,
    ClosedCommitted,
    AlreadyClosed,
}

#[derive(Debug)]
struct SwitchAttemptIdentity {
    post_loss: AtomicBool,
}

impl SwitchAttemptIdentity {
    fn new(post_loss: bool) -> Self {
        Self {
            post_loss: AtomicBool::new(post_loss),
        }
    }

    fn upgrade_to_post_loss(&self) {
        self.post_loss.store(true, Ordering::Release);
    }

    fn is_post_loss(&self) -> bool {
        self.post_loss.load(Ordering::Acquire)
    }

    fn current_intent(&self, mut original: AttachIntent) -> AttachIntent {
        original.post_loss = self.is_post_loss();
        original
    }
}

/// Unique physical admission for the one attach attempt started by this
/// arbiter.  A copy-only `AttachIntent` is observation; this non-cloneable
/// value is the capability that must move into either an exact acceptance or
/// an explicit shutdown settlement.
#[derive(Debug)]
#[must_use = "the physical root must retain and settle the exact attach permit"]
pub(super) struct PendingPhysicalAttachPermit {
    identity: Arc<SwitchAttemptIdentity>,
    intent: AttachIntent,
}

impl PendingPhysicalAttachPermit {
    /// Reads the attempt's identity-bound current mode. A hard terminal may
    /// upgrade the same permit after a soft trigger handed it to the physical
    /// owner, so the copy captured at initial admission is not authoritative.
    pub(super) fn intent(&self) -> AttachIntent {
        self.identity.current_intent(self.intent)
    }

    pub(super) fn into_correlated_accepted(self) -> CorrelatedAttachAccepted {
        CorrelatedAttachAccepted {
            identity: self.identity,
            session_id: self.intent.session_id,
            generation: self.intent.requested_generation,
            attach_nonce: self.intent.attach_nonce,
        }
    }

    pub(super) fn into_shutdown_settlement(
        self,
        stage: PhysicalPendingAttachStage,
    ) -> PhysicalPendingAttachSettlement {
        PhysicalPendingAttachSettlement {
            identity: self.identity,
            stage,
        }
    }

    #[cfg(test)]
    fn into_accepted_for_test(self) -> CorrelatedAttachAccepted {
        self.into_correlated_accepted()
    }

    #[cfg(test)]
    fn into_shutdown_settlement_for_test(
        self,
        stage: PhysicalPendingAttachStage,
    ) -> PhysicalPendingAttachSettlement {
        self.into_shutdown_settlement(stage)
    }
}

/// Non-copy outcome: only the first `Started` branch carries one physical
/// admission.  Dedupe and soft-to-hard upgrade never mint another permit.
#[derive(Debug)]
pub(crate) struct SwitchTriggerOutcome {
    disposition: SwitchTriggerDisposition,
    intent: AttachIntent,
    physical_permit: Option<PendingPhysicalAttachPermit>,
}

impl SwitchTriggerOutcome {
    pub(crate) const fn disposition(&self) -> SwitchTriggerDisposition {
        self.disposition
    }

    pub(crate) const fn intent(&self) -> AttachIntent {
        self.intent
    }

    pub(super) fn into_physical_permit(self) -> Option<PendingPhysicalAttachPermit> {
        self.physical_permit
    }
}

#[derive(Debug)]
pub(crate) struct CorrelatedAttachAccepted {
    identity: Arc<SwitchAttemptIdentity>,
    session_id: SessionId,
    generation: LegGeneration,
    attach_nonce: AttachNonce,
}

impl CorrelatedAttachAccepted {
    pub(super) const fn generation(&self) -> LegGeneration {
        self.generation
    }

    pub(super) fn into_shutdown_settlement(
        self,
        stage: PhysicalPendingAttachStage,
    ) -> PhysicalPendingAttachSettlement {
        PhysicalPendingAttachSettlement {
            identity: self.identity,
            stage,
        }
    }

    #[cfg(test)]
    fn into_shutdown_settlement_for_test(
        self,
        stage: PhysicalPendingAttachStage,
    ) -> PhysicalPendingAttachSettlement {
        self.into_shutdown_settlement(stage)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PhysicalPendingAttachStage {
    Unbegun,
    AttachPending,
    AwaitingAttach,
    Accepted,
    GenerationStatus,
}

/// Proof that the future physical attach owner consumed the one exact attempt
/// during shutdown.  Production construction remains deliberately absent
/// until that owner exists; tests can derive it only by consuming the unique
/// permit, never from scalar correlation.
#[derive(Debug)]
#[must_use = "the policy arbiter must consume the exact physical settlement"]
pub(super) struct PhysicalPendingAttachSettlement {
    identity: Arc<SwitchAttemptIdentity>,
    stage: PhysicalPendingAttachStage,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SwitchDiscardAudit {
    session_id: SessionId,
    current_generation: LegGeneration,
    requested_generation: LegGeneration,
    standby_nonce: StandbyNonce,
    attach_nonce: AttachNonce,
    switch_epoch: SwitchEpoch,
    initial_trigger: Option<SwitchTriggerKind>,
    post_loss: bool,
    physical_stage: Option<PhysicalPendingAttachStage>,
    discarded_intents: usize,
    discarded_attach_nonces: usize,
    discarded_switch_epochs: usize,
}

impl SwitchDiscardAudit {
    pub(crate) const fn discarded_intents(self) -> usize {
        self.discarded_intents
    }

    pub(crate) const fn discarded_attach_nonces(self) -> usize {
        self.discarded_attach_nonces
    }

    pub(crate) const fn discarded_switch_epochs(self) -> usize {
        self.discarded_switch_epochs
    }

    pub(super) const fn physical_stage(self) -> Option<PhysicalPendingAttachStage> {
        self.physical_stage
    }

    pub(crate) const fn is_post_loss(self) -> bool {
        self.post_loss
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CommittedSwitch {
    generation: LegGeneration,
    switch_epoch: SwitchEpoch,
    post_loss: bool,
}

impl CommittedSwitch {
    pub(crate) const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn switch_epoch(self) -> SwitchEpoch {
        self.switch_epoch
    }

    pub(crate) const fn is_post_loss(self) -> bool {
        self.post_loss
    }
}

/// Copy-only plan produced by exact-acceptance preflight.  This is deliberately
/// not a committed outcome: only [`PreparedSwitchCommit::finalize`] can mint a
/// [`CommittedSwitch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PreparedSwitchPlan {
    generation: LegGeneration,
    switch_epoch: SwitchEpoch,
    post_loss: bool,
}

impl PreparedSwitchPlan {
    pub(super) const fn generation(self) -> LegGeneration {
        self.generation
    }

    pub(super) const fn switch_epoch(self) -> SwitchEpoch {
        self.switch_epoch
    }

    pub(super) const fn is_post_loss(self) -> bool {
        self.post_loss
    }
}

/// Snapshot-inert proof that one exact attach acceptance may commit this
/// arbiter. The guard borrows only the arbiter; the caller retains the exact
/// acceptance until finalization, so dropping an unresolved guard cannot
/// strand a pending switch without its shutdown settlement authority.
#[must_use = "a prepared switch commit must be finalized before physical delivery commits"]
pub(super) struct PreparedSwitchCommit<'arbiter, 'accepted> {
    arbiter: &'arbiter mut ClientSwitchArbiter,
    _accepted: &'accepted CorrelatedAttachAccepted,
    plan: PreparedSwitchPlan,
}

impl PreparedSwitchCommit<'_, '_> {
    pub(super) const fn plan(&self) -> PreparedSwitchPlan {
        self.plan
    }

    /// Commits the already validated policy transition. The guard keeps both
    /// the arbiter and the exact acceptance borrowed from preflight through
    /// every fallible physical install, so this final state change is
    /// synchronous and infallible. The caller consumes the acceptance after
    /// this method ends its borrow.
    pub(super) fn finalize(self) -> CommittedSwitch {
        let Self {
            arbiter,
            _accepted: _,
            plan,
        } = self;
        let committed = CommittedSwitch {
            generation: plan.generation,
            switch_epoch: plan.switch_epoch,
            post_loss: plan.post_loss,
        };
        arbiter.current_generation = committed.generation;
        arbiter.state = OwnedArbiterState::Committed(committed);
        committed
    }
}

#[derive(Debug)]
struct PendingSwitchAttempt {
    identity: Arc<SwitchAttemptIdentity>,
    intent: AttachIntent,
}

impl PendingSwitchAttempt {
    fn current_intent(&self) -> AttachIntent {
        self.identity.current_intent(self.intent)
    }
}

#[derive(Debug)]
struct ReadySwitchSeed {
    attach_nonce: AttachNonce,
    switch_epoch: SwitchEpoch,
}

#[derive(Debug)]
enum OwnedArbiterState {
    Ready(ReadySwitchSeed),
    Pending(PendingSwitchAttempt),
    ShutdownPending(PendingSwitchAttempt),
    Committed(CommittedSwitch),
    ShutdownCommitted(CommittedSwitch),
    Terminal(SwitchDiscardAudit),
    /// Synchronous move placeholder. No public call returns with this state.
    Transitioning,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArbiterState {
    Ready,
    Pending(AttachIntent),
    ShutdownPending(AttachIntent),
    Committed(CommittedSwitch),
    Terminal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SwitchArbiterInventory {
    live_intents: usize,
    live_attach_nonces: usize,
    live_switch_epochs: usize,
    pending_physical_settlements: usize,
    historical_commits: usize,
    historical_discards: usize,
}

impl SwitchArbiterInventory {
    pub(crate) const fn live_intents(self) -> usize {
        self.live_intents
    }

    pub(crate) const fn live_attach_nonces(self) -> usize {
        self.live_attach_nonces
    }

    pub(crate) const fn live_switch_epochs(self) -> usize {
        self.live_switch_epochs
    }

    pub(crate) const fn pending_physical_settlements(self) -> usize {
        self.pending_physical_settlements
    }

    pub(crate) const fn historical_discards(self) -> usize {
        self.historical_discards
    }

    pub(crate) const fn is_live_exact_zero(self) -> bool {
        self.live_intents == 0
            && self.live_attach_nonces == 0
            && self.live_switch_epochs == 0
            && self.pending_physical_settlements == 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SwitchArbiterSnapshot {
    current_generation: LegGeneration,
    state: ArbiterState,
    switch_admission_closed: bool,
    inventory: SwitchArbiterInventory,
}

/// One epoch/nonce/attach arbiter shared by all local replacement triggers.
pub(crate) struct ClientSwitchArbiter {
    session_id: SessionId,
    current_generation: LegGeneration,
    requested_generation: LegGeneration,
    standby_nonce: StandbyNonce,
    state: OwnedArbiterState,
}

impl ClientSwitchArbiter {
    pub(crate) fn new(
        session_id: SessionId,
        current_generation: LegGeneration,
        standby_nonce: StandbyNonce,
        attach_nonce: AttachNonce,
        switch_epoch: SwitchEpoch,
    ) -> Result<Self, SwitchArbiterError> {
        let next = current_generation
            .get()
            .checked_add(1)
            .ok_or(SwitchArbiterError::GenerationExhausted)?;
        let requested_generation =
            LegGeneration::new(next).map_err(|_| SwitchArbiterError::GenerationExhausted)?;
        Ok(Self {
            session_id,
            current_generation,
            requested_generation,
            standby_nonce,
            state: OwnedArbiterState::Ready(ReadySwitchSeed {
                attach_nonce,
                switch_epoch,
            }),
        })
    }

    /// Linear value-level cutoff for new switch admission.
    ///
    /// A pending intent remains visible for exact terminal settlement, but no
    /// trigger may start or upgrade it and no acceptance may commit after this
    /// cutoff. The physical root must invoke this under its lifecycle seam;
    /// this kernel does not by itself prove that production linearization.
    pub(crate) fn close_switch_admission_for_shutdown(&mut self) -> SwitchShutdownDisposition {
        let state = std::mem::replace(&mut self.state, OwnedArbiterState::Transitioning);
        let (state, disposition) = match state {
            OwnedArbiterState::Ready(seed) => (
                OwnedArbiterState::Terminal(SwitchDiscardAudit {
                    session_id: self.session_id,
                    current_generation: self.current_generation,
                    requested_generation: self.requested_generation,
                    standby_nonce: self.standby_nonce,
                    attach_nonce: seed.attach_nonce,
                    switch_epoch: seed.switch_epoch,
                    initial_trigger: None,
                    post_loss: false,
                    physical_stage: None,
                    discarded_intents: 0,
                    discarded_attach_nonces: 1,
                    discarded_switch_epochs: 1,
                }),
                SwitchShutdownDisposition::ClosedIdle,
            ),
            OwnedArbiterState::Pending(pending) => (
                OwnedArbiterState::ShutdownPending(pending),
                SwitchShutdownDisposition::ClosedPendingSettlement,
            ),
            OwnedArbiterState::Committed(committed) => (
                OwnedArbiterState::ShutdownCommitted(committed),
                SwitchShutdownDisposition::ClosedCommitted,
            ),
            state @ (OwnedArbiterState::ShutdownPending(_)
            | OwnedArbiterState::ShutdownCommitted(_)
            | OwnedArbiterState::Terminal(_)
            | OwnedArbiterState::Transitioning) => {
                (state, SwitchShutdownDisposition::AlreadyClosed)
            }
        };
        self.state = state;
        disposition
    }

    pub(crate) fn on_hard_failure(
        &mut self,
        trigger: HardFailureTrigger,
    ) -> Result<SwitchTriggerOutcome, SwitchArbiterError> {
        self.require_trigger_correlation(trigger.session_id, trigger.generation)?;
        let _terminal_observed_at = trigger.failed_at;
        self.begin_or_dedupe(SwitchTriggerKind::HardFailure, true)
    }

    pub(crate) fn on_local_ack_stall(
        &mut self,
        source: &mut ClientLocalAckStallSource,
        trigger: LocalAckStallTrigger,
    ) -> Result<SwitchTriggerOutcome, SwitchArbiterError> {
        // Wrong destination correlation must be reversible: restore the exact
        // source episode before dropping the moved trigger. Once correlation
        // matches, consume the source before an admission-closed rejection so
        // shutdown cannot leave an exact trigger permanently issued.
        if let Err(correlation) =
            self.require_trigger_identity(trigger.session_id, trigger.generation)
        {
            source
                .restore_trigger_after_correlation_rejection(&trigger)
                .map_err(SwitchArbiterError::LocalAckStallEvidence)?;
            return Err(correlation);
        }
        source
            .consume_trigger(&trigger)
            .map_err(SwitchArbiterError::LocalAckStallEvidence)?;
        self.require_switch_admission_open()?;
        self.begin_or_dedupe(SwitchTriggerKind::LocalAckStall, false)
    }

    fn admit_authenticated_hint(
        &mut self,
        trigger: AuthenticatedSwitchHint,
    ) -> Result<SwitchTriggerOutcome, SwitchArbiterError> {
        self.require_trigger_correlation(trigger.session_id, trigger.generation)?;
        if trigger.standby_nonce != self.standby_nonce {
            return Err(SwitchArbiterError::WrongStandbyNonce);
        }
        let _validated_hint = (
            trigger.hint_sequence,
            trigger.flow_id,
            trigger.oldest_unacknowledged,
            trigger.arrived_at,
        );
        self.begin_or_dedupe(SwitchTriggerKind::AuthenticatedHint, false)
    }

    #[cfg(test)]
    pub(crate) fn on_authenticated_hint(
        &mut self,
        trigger: AuthenticatedSwitchHint,
    ) -> Result<SwitchTriggerOutcome, SwitchArbiterError> {
        self.admit_authenticated_hint(trigger)
    }

    /// Snapshot-inert exact-acceptance preflight. The returned guard retains
    /// the exclusive arbiter borrow, so shutdown, trigger upgrades, and a
    /// second commit cannot interleave before it is dropped or finalized.
    /// The exact acceptance remains with the caller throughout preparation.
    pub(super) fn preflight_commit<'accepted>(
        &mut self,
        accepted: &'accepted CorrelatedAttachAccepted,
    ) -> Result<PreparedSwitchCommit<'_, 'accepted>, SwitchArbiterError> {
        let intent = self.validate_pending_acceptance(accepted)?;
        let plan = PreparedSwitchPlan {
            generation: intent.requested_generation,
            switch_epoch: intent.switch_epoch,
            post_loss: intent.post_loss,
        };
        Ok(PreparedSwitchCommit {
            arbiter: self,
            _accepted: accepted,
            plan,
        })
    }

    fn validate_pending_acceptance(
        &self,
        accepted: &CorrelatedAttachAccepted,
    ) -> Result<AttachIntent, SwitchArbiterError> {
        let OwnedArbiterState::Pending(pending) = &self.state else {
            return Err(
                if matches!(
                    self.state,
                    OwnedArbiterState::ShutdownPending(_)
                        | OwnedArbiterState::ShutdownCommitted(_)
                        | OwnedArbiterState::Terminal(_)
                ) {
                    SwitchArbiterError::SwitchAdmissionClosedForShutdown
                } else {
                    SwitchArbiterError::EpochClosed
                },
            );
        };
        let intent = pending.current_intent();
        if !Arc::ptr_eq(&accepted.identity, &pending.identity)
            || accepted.session_id != intent.session_id
            || accepted.generation != intent.requested_generation
            || accepted.attach_nonce != intent.attach_nonce
        {
            return Err(SwitchArbiterError::WrongAttachAcceptance);
        }
        Ok(intent)
    }

    #[cfg(test)]
    pub(crate) fn commit(
        &mut self,
        accepted: CorrelatedAttachAccepted,
    ) -> Result<CommittedSwitch, SwitchCommitFailure> {
        let prepared = match self.preflight_commit(&accepted) {
            Ok(prepared) => prepared,
            Err(kind) => return Err(SwitchCommitFailure { kind, accepted }),
        };
        let committed = prepared.finalize();
        drop(accepted);
        Ok(committed)
    }

    /// Snapshot-inert physical preflight for the one exact admission minted
    /// by this arbiter. Scalar-equal attempts from another arbiter cannot
    /// borrow this pending epoch, nonce, or generation.
    pub(super) fn preflight_physical_permit(
        &self,
        permit: &PendingPhysicalAttachPermit,
    ) -> Result<AttachIntent, SwitchArbiterError> {
        let OwnedArbiterState::Pending(pending) = &self.state else {
            return Err(
                if matches!(
                    self.state,
                    OwnedArbiterState::ShutdownPending(_)
                        | OwnedArbiterState::ShutdownCommitted(_)
                        | OwnedArbiterState::Terminal(_)
                ) {
                    SwitchArbiterError::SwitchAdmissionClosedForShutdown
                } else {
                    SwitchArbiterError::EpochClosed
                },
            );
        };
        let intent = permit.intent();
        let pending_intent = pending.current_intent();
        if !Arc::ptr_eq(&permit.identity, &pending.identity) || intent != pending_intent {
            return Err(SwitchArbiterError::WrongPhysicalAttachPermit);
        }
        Ok(intent)
    }

    pub(super) fn settle_shutdown_pending(
        &mut self,
        settlement: PhysicalPendingAttachSettlement,
    ) -> Result<SwitchDiscardAudit, SwitchShutdownSettlementFailure> {
        let OwnedArbiterState::ShutdownPending(pending) = &self.state else {
            let kind = if matches!(self.state, OwnedArbiterState::Terminal(_)) {
                SwitchArbiterError::EpochClosed
            } else {
                SwitchArbiterError::ShutdownSettlementNotPending
            };
            return Err(SwitchShutdownSettlementFailure { kind, settlement });
        };
        if !Arc::ptr_eq(&pending.identity, &settlement.identity) {
            return Err(SwitchShutdownSettlementFailure {
                kind: SwitchArbiterError::WrongPhysicalAttachSettlement,
                settlement,
            });
        }
        let intent = pending.current_intent();
        let audit = SwitchDiscardAudit {
            session_id: intent.session_id,
            current_generation: intent.current_generation,
            requested_generation: intent.requested_generation,
            standby_nonce: intent.standby_nonce,
            attach_nonce: intent.attach_nonce,
            switch_epoch: intent.switch_epoch,
            initial_trigger: Some(intent.initial_trigger),
            post_loss: intent.post_loss,
            physical_stage: Some(settlement.stage),
            discarded_intents: 1,
            discarded_attach_nonces: 1,
            discarded_switch_epochs: 1,
        };
        drop(settlement);
        self.state = OwnedArbiterState::Terminal(audit);
        Ok(audit)
    }

    pub(crate) fn discard_audit(&self) -> Option<SwitchDiscardAudit> {
        match self.state {
            OwnedArbiterState::Terminal(audit) => Some(audit),
            _ => None,
        }
    }

    pub(crate) fn inventory(&self) -> SwitchArbiterInventory {
        match self.state {
            OwnedArbiterState::Ready(_) => SwitchArbiterInventory {
                live_intents: 0,
                live_attach_nonces: 1,
                live_switch_epochs: 1,
                pending_physical_settlements: 0,
                historical_commits: 0,
                historical_discards: 0,
            },
            OwnedArbiterState::Pending(_) | OwnedArbiterState::ShutdownPending(_) => {
                SwitchArbiterInventory {
                    live_intents: 1,
                    live_attach_nonces: 1,
                    live_switch_epochs: 1,
                    pending_physical_settlements: 1,
                    historical_commits: 0,
                    historical_discards: 0,
                }
            }
            OwnedArbiterState::Committed(_) | OwnedArbiterState::ShutdownCommitted(_) => {
                SwitchArbiterInventory {
                    live_intents: 0,
                    live_attach_nonces: 0,
                    live_switch_epochs: 0,
                    pending_physical_settlements: 0,
                    historical_commits: 1,
                    historical_discards: 0,
                }
            }
            OwnedArbiterState::Terminal(_) => SwitchArbiterInventory {
                live_intents: 0,
                live_attach_nonces: 0,
                live_switch_epochs: 0,
                pending_physical_settlements: 0,
                historical_commits: 0,
                historical_discards: 1,
            },
            OwnedArbiterState::Transitioning => SwitchArbiterInventory {
                // Conservatively nonzero if an impossible re-entrant observer
                // ever sees the synchronous move placeholder.
                live_intents: 1,
                live_attach_nonces: 1,
                live_switch_epochs: 1,
                pending_physical_settlements: 1,
                historical_commits: 0,
                historical_discards: 0,
            },
        }
    }

    pub(crate) fn snapshot(&self) -> SwitchArbiterSnapshot {
        let (state, switch_admission_closed) = match self.state {
            OwnedArbiterState::Ready(_) => (ArbiterState::Ready, false),
            OwnedArbiterState::Pending(ref pending) => {
                (ArbiterState::Pending(pending.current_intent()), false)
            }
            OwnedArbiterState::ShutdownPending(ref pending) => (
                ArbiterState::ShutdownPending(pending.current_intent()),
                true,
            ),
            OwnedArbiterState::Committed(committed) => (ArbiterState::Committed(committed), false),
            OwnedArbiterState::ShutdownCommitted(committed) => {
                (ArbiterState::Committed(committed), true)
            }
            OwnedArbiterState::Terminal(_) => (ArbiterState::Terminal, true),
            OwnedArbiterState::Transitioning => (ArbiterState::Terminal, true),
        };
        SwitchArbiterSnapshot {
            current_generation: self.current_generation,
            state,
            switch_admission_closed,
            inventory: self.inventory(),
        }
    }

    fn require_trigger_correlation(
        &self,
        session_id: SessionId,
        generation: LegGeneration,
    ) -> Result<(), SwitchArbiterError> {
        self.require_trigger_identity(session_id, generation)?;
        self.require_switch_admission_open()
    }

    fn require_trigger_identity(
        &self,
        session_id: SessionId,
        generation: LegGeneration,
    ) -> Result<(), SwitchArbiterError> {
        if session_id != self.session_id {
            return Err(SwitchArbiterError::WrongSession);
        }
        if generation != self.current_generation {
            return Err(SwitchArbiterError::WrongGeneration);
        }
        Ok(())
    }

    fn require_switch_admission_open(&self) -> Result<(), SwitchArbiterError> {
        match self.state {
            OwnedArbiterState::ShutdownPending(_)
            | OwnedArbiterState::ShutdownCommitted(_)
            | OwnedArbiterState::Terminal(_) => {
                return Err(SwitchArbiterError::SwitchAdmissionClosedForShutdown);
            }
            OwnedArbiterState::Committed(_) => return Err(SwitchArbiterError::EpochClosed),
            OwnedArbiterState::Ready(_) | OwnedArbiterState::Pending(_) => {}
            OwnedArbiterState::Transitioning => {
                return Err(SwitchArbiterError::ArbiterTransitionInProgress);
            }
        }
        Ok(())
    }

    fn begin_or_dedupe(
        &mut self,
        trigger: SwitchTriggerKind,
        hard: bool,
    ) -> Result<SwitchTriggerOutcome, SwitchArbiterError> {
        let state = std::mem::replace(&mut self.state, OwnedArbiterState::Transitioning);
        match state {
            OwnedArbiterState::Ready(seed) => {
                let intent = AttachIntent {
                    session_id: self.session_id,
                    current_generation: self.current_generation,
                    requested_generation: self.requested_generation,
                    standby_nonce: self.standby_nonce,
                    attach_nonce: seed.attach_nonce,
                    switch_epoch: seed.switch_epoch,
                    initial_trigger: trigger,
                    // Mutable switch mode belongs only to the exact attempt
                    // identity. Every observable copy is overlaid from that
                    // identity so a soft-to-hard upgrade has one source.
                    post_loss: false,
                };
                let identity = Arc::new(SwitchAttemptIdentity::new(hard));
                let current_intent = identity.current_intent(intent);
                let physical_permit = PendingPhysicalAttachPermit {
                    identity: Arc::clone(&identity),
                    intent,
                };
                self.state = OwnedArbiterState::Pending(PendingSwitchAttempt { identity, intent });
                Ok(SwitchTriggerOutcome {
                    disposition: SwitchTriggerDisposition::Started,
                    intent: current_intent,
                    physical_permit: Some(physical_permit),
                })
            }
            OwnedArbiterState::Pending(pending) if hard && !pending.identity.is_post_loss() => {
                pending.identity.upgrade_to_post_loss();
                let intent = pending.current_intent();
                self.state = OwnedArbiterState::Pending(pending);
                Ok(SwitchTriggerOutcome {
                    disposition: SwitchTriggerDisposition::UpgradedToPostLoss,
                    intent,
                    physical_permit: None,
                })
            }
            OwnedArbiterState::Pending(pending) => {
                let intent = pending.current_intent();
                self.state = OwnedArbiterState::Pending(pending);
                Ok(SwitchTriggerOutcome {
                    disposition: SwitchTriggerDisposition::Deduplicated,
                    intent,
                    physical_permit: None,
                })
            }
            state @ OwnedArbiterState::Committed(_) => {
                self.state = state;
                Err(SwitchArbiterError::EpochClosed)
            }
            state @ (OwnedArbiterState::ShutdownPending(_)
            | OwnedArbiterState::ShutdownCommitted(_)
            | OwnedArbiterState::Terminal(_)) => {
                self.state = state;
                Err(SwitchArbiterError::SwitchAdmissionClosedForShutdown)
            }
            OwnedArbiterState::Transitioning => {
                self.state = OwnedArbiterState::Transitioning;
                Err(SwitchArbiterError::ArbiterTransitionInProgress)
            }
        }
    }
}

pub(crate) struct SwitchCommitFailure {
    kind: SwitchArbiterError,
    accepted: CorrelatedAttachAccepted,
}

impl SwitchCommitFailure {
    pub(crate) const fn kind(&self) -> SwitchArbiterError {
        self.kind
    }

    pub(crate) fn into_accepted(self) -> CorrelatedAttachAccepted {
        self.accepted
    }
}

impl fmt::Debug for SwitchCommitFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SwitchCommitFailure")
            .field("kind", &self.kind)
            .field("accepted", &"[REDACTED]")
            .finish()
    }
}

pub(super) struct SwitchShutdownSettlementFailure {
    kind: SwitchArbiterError,
    settlement: PhysicalPendingAttachSettlement,
}

impl SwitchShutdownSettlementFailure {
    pub(super) const fn kind(&self) -> SwitchArbiterError {
        self.kind
    }

    pub(super) fn into_settlement(self) -> PhysicalPendingAttachSettlement {
        self.settlement
    }
}

impl fmt::Debug for SwitchShutdownSettlementFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SwitchShutdownSettlementFailure")
            .field("kind", &self.kind)
            .field("settlement", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum SwitchArbiterError {
    #[error("switch epoch must be nonzero")]
    ZeroSwitchEpoch,
    #[error("the exact-next generation cannot be represented")]
    GenerationExhausted,
    #[error("trigger names another session")]
    WrongSession,
    #[error("trigger names another generation")]
    WrongGeneration,
    #[error("authenticated hint names another standby nonce")]
    WrongStandbyNonce,
    #[error("local ACK-stall evidence was rejected: {0}")]
    LocalAckStallEvidence(ClientLocalAckStallError),
    #[error("the switch epoch has already committed or is otherwise closed")]
    EpochClosed,
    #[error("shutdown already closed new switch admission")]
    SwitchAdmissionClosedForShutdown,
    #[error("ATTACH_ACCEPTED does not correlate to the one pending intent")]
    WrongAttachAcceptance,
    #[error("physical ATTACH admission belongs to another pending attempt")]
    WrongPhysicalAttachPermit,
    #[error("shutdown has no physical pending-attach settlement to consume")]
    ShutdownSettlementNotPending,
    #[error("physical pending-attach settlement belongs to another attempt")]
    WrongPhysicalAttachSettlement,
    #[error("switch arbiter was observed during an internal ownership transition")]
    ArbiterTransitionInProgress,
}

#[cfg(test)]
mod tests;

//! First closed owner-root slice for the exact initial physical A.
//!
//! The root keeps the actor, sole owner controller/Target executor, active
//! handoff, and the
//! record-specific acceptance receipt in one ownership domain. A shutdown
//! request closes root admission immediately, but physical cancellation is
//! deferred until that exact receipt reports DELIVERED and is synchronously
//! settled. This file deliberately stops at structural actor join: the later
//! full owner finalizer must consume the retained active handoff, actor
//! terminal, executor, Target state, and session ledger before claiming
//! whole-root zero ownership.

use std::{fmt, sync::Arc};

use thiserror::Error;

use super::QuinnLegCompositionGate;
use crate::owned_upstream::leg::{BoundInbound, EstablishedLeg, LegTransportTerminalReason};
use crate::owned_upstream::leg_io::{LegIo, LegOutboundQueue, PhysicalLegRuntimeIncarnation};
use crate::owned_upstream::owner_session_controller::{
    OwnerInitialStandbyRegistrationBlock, OwnerOutputPageCapacity, OwnerSessionController,
    OwnerSessionControllerError, OwnerSessionControllerSnapshot,
    OwnerSessionRecoveryTerminalBindErrorKind, OwnerSessionRecoveryTerminalBound,
    OwnerSessionRecoveryTerminalDiscarded,
};
use crate::owned_upstream::owner_target::{
    OwnerTargetError, OwnerTargetInitialAcceptanceDelivery,
    OwnerTargetInitialAcceptanceSettlementDisposition, OwnerTargetInitialHandoff,
    OwnerTargetInitialHandoffInventory, OwnerTargetInputBlock,
};
#[cfg(test)]
use crate::owned_upstream::quinn_leg_actor::QuinnLegActorJoinBlock;
use crate::owned_upstream::quinn_leg_actor::{
    ActorTerminalStampWitness, QuinnLegActorError, QuinnLegActorJoined,
    QuinnLegActorReportedTerminal, QuinnLegActorReportedTerminalPoll,
    QuinnLegActorShutdownWaitDrive, StartedQuinnLegActor,
};
use crate::owned_upstream::standby::ExactAuthenticatedStandby;
use crate::owned_upstream::target::TargetIo;
use crate::resumable::LegGeneration;

#[derive(Debug)]
pub(crate) enum QuinnOwnerInitialAdoptErrorKind {
    Actor(QuinnLegActorError),
    WrongSessionClock,
    Handoff(OwnerTargetInputBlock),
}

enum QuinnOwnerInitialStandbyAuthenticationFailureKind {
    Blocked {
        block: OwnerInitialStandbyRegistrationBlock,
        inbound: Box<BoundInbound>,
    },
    Target(OwnerTargetError),
}

/// A rejected registration retains both the sole accepted-A root and the
/// exact classified B wire fact.  Dropping this value is therefore an
/// explicit discard; retry can never reconstruct either authority.
pub(crate) struct QuinnOwnerInitialStandbyAuthenticationFailure<T: TargetIo> {
    root: QuinnOwnerInitialAcceptedRootParts<T>,
    kind: QuinnOwnerInitialStandbyAuthenticationFailureKind,
}

impl<T: TargetIo> QuinnOwnerInitialStandbyAuthenticationFailure<T> {
    pub(crate) fn into_root(self) -> QuinnOwnerInitialAcceptedRootParts<T> {
        self.root
    }
}

impl<T: TargetIo> fmt::Debug for QuinnOwnerInitialStandbyAuthenticationFailure<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("QuinnOwnerInitialStandbyAuthenticationFailure");
        match &self.kind {
            QuinnOwnerInitialStandbyAuthenticationFailureKind::Blocked { block, inbound } => {
                let _ = inbound;
                debug.field("kind", &"Blocked").field("block", block);
            }
            QuinnOwnerInitialStandbyAuthenticationFailureKind::Target(error) => {
                debug.field("kind", &"Target").field("error", error);
            }
        }
        debug.field("root", &"[REDACTED]").finish()
    }
}

/// Failed controller-first adoption preserves all three non-cloneable owners.
/// This is the branch-by-abstraction seam consumed by the future full physical
/// owner root; it never decomposes the controller's Target executor.
pub(crate) struct QuinnOwnerInitialControllerAdoptFailure<T: TargetIo> {
    kind: QuinnOwnerInitialAdoptErrorKind,
    ownership: Box<(
        StartedQuinnLegActor,
        OwnerSessionController<T>,
        OwnerTargetInitialHandoff,
    )>,
}

impl<T: TargetIo> QuinnOwnerInitialControllerAdoptFailure<T> {
    pub(crate) const fn kind(&self) -> &QuinnOwnerInitialAdoptErrorKind {
        &self.kind
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        StartedQuinnLegActor,
        OwnerSessionController<T>,
        OwnerTargetInitialHandoff,
    ) {
        *self.ownership
    }
}

impl<T: TargetIo> fmt::Debug for QuinnOwnerInitialControllerAdoptFailure<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnOwnerInitialControllerAdoptFailure")
            .field("kind", &self.kind)
            .field("ownership", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerInitialAcceptanceDrive {
    Pending(OwnerTargetInitialHandoffInventory),
    Settled {
        disposition: OwnerTargetInitialAcceptanceSettlementDisposition,
        inventory: OwnerTargetInitialHandoffInventory,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerInitialRootPartsBlock {
    ShutdownRequested,
    AcceptanceNotSettled(OwnerTargetInitialHandoffInventory),
    CurrentActorNotRunning,
    CompositionInvariant,
}

/// Failed typestate transfer returns the complete initial composition.  No
/// actor, controller, handoff, receipt, or physical binding is copied out of
/// the rejected branch.
#[must_use = "a rejected initial root-parts transfer retains the complete root"]
pub(crate) struct QuinnOwnerInitialRootPartsFailure<T: TargetIo> {
    block: QuinnOwnerInitialRootPartsBlock,
    root: QuinnOwnerInitialSessionComposition<T>,
}

impl<T: TargetIo> QuinnOwnerInitialRootPartsFailure<T> {
    pub(crate) const fn block(&self) -> QuinnOwnerInitialRootPartsBlock {
        self.block
    }

    pub(crate) const fn inventory(&self) -> Option<OwnerTargetInitialHandoffInventory> {
        match self.block {
            QuinnOwnerInitialRootPartsBlock::AcceptanceNotSettled(inventory) => Some(inventory),
            QuinnOwnerInitialRootPartsBlock::ShutdownRequested
            | QuinnOwnerInitialRootPartsBlock::CurrentActorNotRunning
            | QuinnOwnerInitialRootPartsBlock::CompositionInvariant => None,
        }
    }

    pub(crate) fn into_root(self) -> QuinnOwnerInitialSessionComposition<T> {
        self.root
    }
}

impl<T: TargetIo> fmt::Debug for QuinnOwnerInitialRootPartsFailure<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnOwnerInitialRootPartsFailure")
            .field("block", &self.block)
            .field("root", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Error)]
pub(crate) enum QuinnOwnerInitialShutdownError {
    #[error("initial acceptance delivery is still pending")]
    AcceptancePending(OwnerTargetInitialHandoffInventory),
    #[error("initial owner actor failed: {0}")]
    Actor(QuinnLegActorError),
    #[error("initial owner handoff was rejected: {0:?}")]
    Handoff(OwnerTargetInputBlock),
    #[error(
        "initial owner actor retains outbound ownership: frames={queued_frames}, bytes={queued_bytes}, wait={ordered_delivery_wait}"
    )]
    OutboundResidual {
        queued_frames: usize,
        queued_bytes: usize,
        ordered_delivery_wait: bool,
    },
    #[error("initial owner actor join runtime is unavailable: {0}")]
    RuntimeUnavailable(Arc<str>),
    #[error("initial owner root ownership invariant failed")]
    CompositionInvariant,
}

/// Structural actor result plus the still-live logical owners deliberately
/// retained for the next full-root finalizer. A zero actor count is therefore
/// never presented as whole-session zero ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnOwnerInitialActorJoined {
    actor_owned_messages: usize,
    actor_owned_bytes: usize,
    actor_owned_terminals: usize,
    actor_terminal_failure: bool,
    retained_controllers: usize,
    retained_executors: usize,
    retained_attached_authorities: usize,
    retained_acceptance_receipts: usize,
    retained_physical_actor_identities: usize,
}

impl QuinnOwnerInitialActorJoined {
    fn observe<T: TargetIo>(
        joined: &QuinnLegActorJoined,
        controller: &OwnerSessionController<T>,
        handoff: &OwnerTargetInitialHandoff,
    ) -> Self {
        let _controller_snapshot = controller.snapshot();
        Self {
            actor_owned_messages: joined.owned_messages(),
            actor_owned_bytes: joined.owned_bytes(),
            actor_owned_terminals: joined.owned_terminals(),
            actor_terminal_failure: joined.terminal_failure().is_some(),
            retained_controllers: 1,
            retained_executors: 1,
            retained_attached_authorities: 1,
            retained_acceptance_receipts: handoff.retained_acceptance_receipts(),
            retained_physical_actor_identities: handoff.physical_actor_identities(),
        }
    }

    pub(crate) const fn actor_owned_messages(self) -> usize {
        self.actor_owned_messages
    }

    pub(crate) const fn actor_owned_bytes(self) -> usize {
        self.actor_owned_bytes
    }

    pub(crate) const fn actor_owned_terminals(self) -> usize {
        self.actor_owned_terminals
    }

    pub(crate) const fn actor_terminal_failure(self) -> bool {
        self.actor_terminal_failure
    }

    pub(crate) const fn retained_executors(self) -> usize {
        self.retained_executors
    }

    pub(crate) const fn retained_controllers(self) -> usize {
        self.retained_controllers
    }

    pub(crate) const fn retained_attached_authorities(self) -> usize {
        self.retained_attached_authorities
    }

    pub(crate) const fn retained_acceptance_receipts(self) -> usize {
        self.retained_acceptance_receipts
    }

    pub(crate) const fn retained_physical_actor_identities(self) -> usize {
        self.retained_physical_actor_identities
    }
}

enum QuinnOwnerInitialActorOwner {
    Running(Box<StartedQuinnLegActor>),
    Joining(tokio::task::JoinHandle<QuinnOwnerInitialJoinOutcome>),
    Joined(Box<QuinnLegActorJoined>),
    TaskLost(Arc<str>),
}

/// Drop guard for a possibly in-flight structural join.  Moving an accepted
/// Running actor into a typed current-actor port first empties this slot; all
/// other composition drops retain the previous task-abort behavior.
struct QuinnOwnerInitialActorSlot(Option<QuinnOwnerInitialActorOwner>);

impl QuinnOwnerInitialActorSlot {
    fn running(actor: StartedQuinnLegActor) -> Self {
        Self(Some(QuinnOwnerInitialActorOwner::Running(Box::new(actor))))
    }

    fn as_ref(&self) -> Option<&QuinnOwnerInitialActorOwner> {
        self.0.as_ref()
    }

    fn as_mut(&mut self) -> Option<&mut QuinnOwnerInitialActorOwner> {
        self.0.as_mut()
    }

    fn take(&mut self) -> Option<QuinnOwnerInitialActorOwner> {
        self.0.take()
    }

    fn set(&mut self, owner: QuinnOwnerInitialActorOwner) {
        debug_assert!(self.0.is_none());
        self.0 = Some(owner);
    }
}

impl Drop for QuinnOwnerInitialActorSlot {
    fn drop(&mut self) {
        if let Some(QuinnOwnerInitialActorOwner::Joining(task)) = self.0.as_mut() {
            task.abort();
        }
    }
}

struct QuinnOwnerInitialJoinOutcome {
    actor: StartedQuinnLegActor,
    joined: Result<QuinnLegActorJoined, QuinnLegActorError>,
}

fn spawn_owner_initial_join(
    runtime: &tokio::runtime::Handle,
    mut actor: StartedQuinnLegActor,
) -> tokio::task::JoinHandle<QuinnOwnerInitialJoinOutcome> {
    runtime.spawn(async move {
        let joined = actor.join().await;
        QuinnOwnerInitialJoinOutcome { actor, joined }
    })
}

/// Accepted initial current-actor port.  The exact physical A and its
/// executor-bound AttachedLeg handoff stay indivisible while the controller is
/// held beside the port by [`QuinnOwnerInitialAcceptedRootParts`].
struct QuinnOwnerInitialCurrentActorPort {
    actor: StartedQuinnLegActor,
    handoff: OwnerTargetInitialHandoff,
}

/// Consuming branch-by-abstraction descriptor for the full owner physical
/// root.  No field is independently public, so the root can transfer the
/// exact A actor, initial handoff, and checked controller only as one value.
#[must_use = "physical root seed retains current A and the sole owner controller"]
pub(super) struct QuinnOwnerInitialPhysicalRootSeed<T: TargetIo> {
    current: QuinnOwnerInitialCurrentActorPort,
    controller: OwnerSessionController<T>,
}

/// One-shot B authentication transaction.  Consuming accepted-A root parts
/// into this value closes the max-two-actor phase: no second B/C candidate can
/// be authenticated while the first exact candidate remains live.
#[must_use = "authenticated B remains paired with the sole accepted-A root"]
pub(crate) struct QuinnOwnerInitialAuthenticatedStandby<T: TargetIo> {
    seed: QuinnOwnerInitialPhysicalRootSeed<T>,
    candidate: ExactAuthenticatedStandby,
}

impl<T: TargetIo> QuinnOwnerInitialAuthenticatedStandby<T> {
    pub(super) fn into_parts(
        self,
    ) -> (
        QuinnOwnerInitialPhysicalRootSeed<T>,
        ExactAuthenticatedStandby,
    ) {
        (self.seed, self.candidate)
    }

    pub(super) fn from_parts(
        seed: QuinnOwnerInitialPhysicalRootSeed<T>,
        candidate: ExactAuthenticatedStandby,
    ) -> Self {
        Self { seed, candidate }
    }
}

impl<T: TargetIo> fmt::Debug for QuinnOwnerInitialAuthenticatedStandby<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnOwnerInitialAuthenticatedStandby")
            .field("seed", &"[REDACTED]")
            .field("candidate", &"[REDACTED]")
            .finish()
    }
}

impl<T: TargetIo> QuinnOwnerInitialPhysicalRootSeed<T> {
    pub(super) fn into_parts(
        self,
    ) -> (
        StartedQuinnLegActor,
        OwnerTargetInitialHandoff,
        OwnerSessionController<T>,
    ) {
        let Self {
            current: QuinnOwnerInitialCurrentActorPort { actor, handoff },
            controller,
        } = self;
        (actor, handoff, controller)
    }

    pub(super) fn from_parts(
        actor: StartedQuinnLegActor,
        handoff: OwnerTargetInitialHandoff,
        controller: OwnerSessionController<T>,
    ) -> Self {
        Self {
            current: QuinnOwnerInitialCurrentActorPort { actor, handoff },
            controller,
        }
    }
}

/// First ownership-preserving branch shared with the future whole-session
/// physical root.  It is intentionally not a B-only root: this value still
/// owns exact physical A, the sole owner controller/Target executor, and the
/// accepted initial active handoff.
#[must_use = "accepted physical owner root parts retain the sole current actor and controller"]
pub(crate) struct QuinnOwnerInitialAcceptedRootParts<T: TargetIo> {
    current: QuinnOwnerInitialCurrentActorPort,
    controller: OwnerSessionController<T>,
}

enum QuinnOwnerInitialCurrentTerminalState {
    Unbound,
    Prepared,
    Quarantined {
        ownership: QuinnOwnerInitialTerminalQuarantineOwnership,
        cause: QuinnOwnerInitialTerminalQuarantineCause,
    },
    Applied {
        discarded: OwnerSessionRecoveryTerminalDiscarded,
        handoff_stamp: ActorTerminalStampWitness,
    },
    JoinedCorrelated {
        generation: LegGeneration,
        reason: LegTransportTerminalReason,
    },
}

enum QuinnOwnerInitialTerminalQuarantineOwnership {
    Unknown,
    Bound {
        terminal: OwnerSessionRecoveryTerminalBound,
        handoff_stamp: ActorTerminalStampWitness,
    },
    Discarded {
        terminal: OwnerSessionRecoveryTerminalDiscarded,
        handoff_stamp: ActorTerminalStampWitness,
    },
}

enum QuinnOwnerInitialTerminalQuarantineCause {
    ActorTerminalEntered,
    ActorTerminalAlreadyConsumed,
    ControllerBoundUnsettled,
    ControllerDiscard { _error: OwnerSessionControllerError },
    DiscardIdentityMismatch,
    JoinStampMismatch,
    RestoredCompositionInvariant,
    CompositionInvariant,
}

struct QuinnOwnerInitialControllerTerminalRoot<T: TargetIo> {
    controller: OwnerSessionController<T>,
    active: Option<OwnerTargetInitialHandoff>,
    state: QuinnOwnerInitialCurrentTerminalState,
}

struct QuinnOwnerInitialTerminalTransaction<'root, T: TargetIo> {
    root: &'root mut QuinnOwnerInitialControllerTerminalRoot<T>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuinnOwnerInitialTerminalCommit {
    Applied {
        generation: LegGeneration,
        reason: LegTransportTerminalReason,
    },
    ControllerDiscardFailed,
    CompositionInvariant,
}

impl QuinnOwnerInitialCurrentTerminalState {
    fn identity(&self) -> Option<(LegGeneration, LegTransportTerminalReason)> {
        match self {
            Self::Applied { discarded, .. } => Some((discarded.generation(), discarded.reason())),
            Self::JoinedCorrelated { generation, reason } => Some((*generation, *reason)),
            Self::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Bound { terminal, .. },
                ..
            } => Some((terminal.generation(), terminal.reason())),
            Self::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Discarded { terminal, .. },
                ..
            } => Some((terminal.generation(), terminal.reason())),
            Self::Unbound
            | Self::Prepared
            | Self::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Unknown,
                ..
            } => None,
        }
    }

    fn handoff_stamp(&self) -> Option<&ActorTerminalStampWitness> {
        match self {
            Self::Applied { handoff_stamp, .. }
            | Self::Quarantined {
                ownership:
                    QuinnOwnerInitialTerminalQuarantineOwnership::Bound { handoff_stamp, .. }
                    | QuinnOwnerInitialTerminalQuarantineOwnership::Discarded { handoff_stamp, .. },
                ..
            } => Some(handoff_stamp),
            Self::Unbound
            | Self::Prepared
            | Self::JoinedCorrelated { .. }
            | Self::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Unknown,
                ..
            } => None,
        }
    }

    const fn actor_terminal_may_be_consumed(&self) -> bool {
        match self {
            Self::Quarantined {
                cause: QuinnOwnerInitialTerminalQuarantineCause::RestoredCompositionInvariant,
                ..
            }
            | Self::Unbound
            | Self::Prepared => false,
            Self::Quarantined { .. } | Self::Applied { .. } | Self::JoinedCorrelated { .. } => true,
        }
    }

    const fn is_quarantined(&self) -> bool {
        matches!(self, Self::Quarantined { .. })
    }

    const fn is_controller_failed(&self) -> bool {
        matches!(
            self,
            Self::Quarantined {
                cause: QuinnOwnerInitialTerminalQuarantineCause::ControllerDiscard { .. },
                ..
            }
        )
    }

    const fn stamp_pair_verified(&self) -> bool {
        matches!(self, Self::JoinedCorrelated { .. })
    }
}

impl fmt::Debug for QuinnOwnerInitialCurrentTerminalState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unbound => formatter.write_str("Unbound"),
            Self::Prepared => formatter.write_str("Prepared"),
            Self::Quarantined { ownership, cause } => formatter
                .debug_struct("Quarantined")
                .field(
                    "identity",
                    &match ownership {
                        QuinnOwnerInitialTerminalQuarantineOwnership::Unknown => None,
                        QuinnOwnerInitialTerminalQuarantineOwnership::Bound {
                            terminal, ..
                        } => Some((terminal.generation(), terminal.reason())),
                        QuinnOwnerInitialTerminalQuarantineOwnership::Discarded {
                            terminal,
                            ..
                        } => Some((terminal.generation(), terminal.reason())),
                    },
                )
                .field("handoff_stamp", &self.handoff_stamp().is_some())
                .field("cause", &cause.label())
                .finish(),
            Self::Applied {
                discarded,
                handoff_stamp: _,
            } => formatter
                .debug_struct("Applied")
                .field("generation", &discarded.generation())
                .field("reason", &discarded.reason())
                .field("handoff_stamp", &true)
                .finish(),
            Self::JoinedCorrelated { generation, reason } => formatter
                .debug_struct("JoinedCorrelated")
                .field("generation", generation)
                .field("reason", reason)
                .finish(),
        }
    }
}

impl QuinnOwnerInitialTerminalQuarantineCause {
    const fn label(&self) -> &'static str {
        match self {
            Self::ActorTerminalEntered => "ActorTerminalEntered",
            Self::ActorTerminalAlreadyConsumed => "ActorTerminalAlreadyConsumed",
            Self::ControllerBoundUnsettled => "ControllerBoundUnsettled",
            Self::ControllerDiscard { .. } => "ControllerDiscard",
            Self::DiscardIdentityMismatch => "DiscardIdentityMismatch",
            Self::JoinStampMismatch => "JoinStampMismatch",
            Self::RestoredCompositionInvariant => "RestoredCompositionInvariant",
            Self::CompositionInvariant => "CompositionInvariant",
        }
    }
}

impl<T: TargetIo> QuinnOwnerInitialControllerTerminalRoot<T> {
    fn begin_terminal_transaction(
        &mut self,
    ) -> Result<QuinnOwnerInitialTerminalTransaction<'_, T>, QuinnOwnerInitialCurrentTerminalError>
    {
        if !matches!(&self.state, QuinnOwnerInitialCurrentTerminalState::Unbound) {
            return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
        }
        self.state = QuinnOwnerInitialCurrentTerminalState::Prepared;
        Ok(QuinnOwnerInitialTerminalTransaction { root: self })
    }

    fn retained_active_handoffs(&self) -> usize {
        usize::from(self.active.is_some())
    }

    fn retained_handoff_stamps(&self) -> usize {
        usize::from(self.state.handoff_stamp().is_some())
    }
}

impl<T: TargetIo> QuinnOwnerInitialTerminalTransaction<'_, T> {
    fn enter_actor_callback(&mut self) {
        if matches!(
            &self.root.state,
            QuinnOwnerInitialCurrentTerminalState::Prepared
        ) {
            self.root.state = QuinnOwnerInitialCurrentTerminalState::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Unknown,
                cause: QuinnOwnerInitialTerminalQuarantineCause::ActorTerminalEntered,
            };
        }
    }

    fn restore_actor_rejection(&mut self) {
        if matches!(
            &self.root.state,
            QuinnOwnerInitialCurrentTerminalState::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Unknown,
                cause: QuinnOwnerInitialTerminalQuarantineCause::ActorTerminalEntered,
            }
        ) {
            self.root.state = QuinnOwnerInitialCurrentTerminalState::Unbound;
        }
    }

    fn quarantine_actor_already_consumed(&mut self) {
        self.root.state = QuinnOwnerInitialCurrentTerminalState::Quarantined {
            ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Unknown,
            cause: QuinnOwnerInitialTerminalQuarantineCause::ActorTerminalAlreadyConsumed,
        };
    }

    fn quarantine_restored_composition_invariant(&mut self) {
        if matches!(
            &self.root.state,
            QuinnOwnerInitialCurrentTerminalState::Unbound
        ) {
            self.root.state = QuinnOwnerInitialCurrentTerminalState::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Unknown,
                cause: QuinnOwnerInitialTerminalQuarantineCause::RestoredCompositionInvariant,
            };
        }
    }

    fn record_bound_terminal(
        &mut self,
        terminal: OwnerSessionRecoveryTerminalBound,
        handoff_stamp: ActorTerminalStampWitness,
    ) {
        self.root.state = QuinnOwnerInitialCurrentTerminalState::Quarantined {
            ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Bound {
                terminal,
                handoff_stamp,
            },
            cause: QuinnOwnerInitialTerminalQuarantineCause::ControllerBoundUnsettled,
        };
    }

    fn quarantine_controller_discard(&mut self, error: OwnerSessionControllerError) {
        if let QuinnOwnerInitialCurrentTerminalState::Quarantined { cause, .. } =
            &mut self.root.state
        {
            *cause = QuinnOwnerInitialTerminalQuarantineCause::ControllerDiscard { _error: error };
        } else {
            self.root.state = QuinnOwnerInitialCurrentTerminalState::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Unknown,
                cause: QuinnOwnerInitialTerminalQuarantineCause::ControllerDiscard {
                    _error: error,
                },
            };
        }
    }

    fn apply_discarded_terminal(
        &mut self,
        discarded: OwnerSessionRecoveryTerminalDiscarded,
    ) -> QuinnOwnerInitialTerminalCommit {
        let expected_bound = match &self.root.state {
            QuinnOwnerInitialCurrentTerminalState::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Bound { terminal, .. },
                ..
            } => *terminal,
            _ => return QuinnOwnerInitialTerminalCommit::CompositionInvariant,
        };
        let previous = std::mem::replace(
            &mut self.root.state,
            QuinnOwnerInitialCurrentTerminalState::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Unknown,
                cause: QuinnOwnerInitialTerminalQuarantineCause::CompositionInvariant,
            },
        );
        let QuinnOwnerInitialCurrentTerminalState::Quarantined {
            ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Bound { handoff_stamp, .. },
            ..
        } = previous
        else {
            return QuinnOwnerInitialTerminalCommit::CompositionInvariant;
        };
        let generation = discarded.generation();
        let reason = discarded.reason();
        if generation != expected_bound.generation() || reason != expected_bound.reason() {
            self.root.state = QuinnOwnerInitialCurrentTerminalState::Quarantined {
                ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Discarded {
                    terminal: discarded,
                    handoff_stamp,
                },
                cause: QuinnOwnerInitialTerminalQuarantineCause::DiscardIdentityMismatch,
            };
            return QuinnOwnerInitialTerminalCommit::CompositionInvariant;
        }
        self.root.state = QuinnOwnerInitialCurrentTerminalState::Applied {
            discarded,
            handoff_stamp,
        };
        QuinnOwnerInitialTerminalCommit::Applied { generation, reason }
    }

    fn apply_reported_terminal(
        &mut self,
        runtime: &PhysicalLegRuntimeIncarnation,
        io: &LegIo,
        queue: &mut LegOutboundQueue,
        reported: QuinnLegActorReportedTerminal,
        gate: &QuinnLegCompositionGate,
    ) -> Result<
        QuinnOwnerInitialTerminalCommit,
        (
            QuinnLegActorReportedTerminal,
            QuinnOwnerInitialCurrentTerminalError,
        ),
    > {
        self.enter_actor_callback();
        if reported.reason() == LegTransportTerminalReason::LocalCancelled {
            return Err((
                reported,
                QuinnOwnerInitialCurrentTerminalError::UnexpectedLocalCancellation,
            ));
        }
        if reported.failure_observed_at().is_none() {
            return Err((
                reported,
                QuinnOwnerInitialCurrentTerminalError::MissingFailureTimestamp,
            ));
        }
        let snapshot = self.root.controller.snapshot();
        if self.root.active.is_none()
            || snapshot.pending_recoveries != 0
            || snapshot.pending_legless_high_waters != 0
            || self.root.state.is_controller_failed()
        {
            return Err((
                reported,
                QuinnOwnerInitialCurrentTerminalError::CompositionInvariant,
            ));
        }
        let (terminal, terminal_stamp) = reported.into_parts();
        let bound = match self.root.controller.bind_initial_terminal(
            &mut self.root.active,
            terminal,
            runtime,
            io,
            queue,
            gate,
        ) {
            Ok(bound) => bound,
            Err(failure) => {
                let kind = failure.kind();
                return Err((
                    QuinnLegActorReportedTerminal::restore(failure.into_terminal(), terminal_stamp),
                    QuinnOwnerInitialCurrentTerminalError::ControllerBind(kind),
                ));
            }
        };
        self.record_bound_terminal(bound, terminal_stamp);
        let discarded = match self.root.controller.discard_recovery_terminal(gate) {
            Ok(discarded) => discarded,
            Err(error) => {
                self.quarantine_controller_discard(error);
                return Ok(QuinnOwnerInitialTerminalCommit::ControllerDiscardFailed);
            }
        };
        Ok(self.apply_discarded_terminal(discarded))
    }
}

impl<T: TargetIo> Drop for QuinnOwnerInitialTerminalTransaction<'_, T> {
    fn drop(&mut self) {
        if matches!(
            &self.root.state,
            QuinnOwnerInitialCurrentTerminalState::Prepared
        ) {
            self.root.state = QuinnOwnerInitialCurrentTerminalState::Unbound;
        }
    }
}

/// Value-only result of one current-A terminal turn.  Unlike the temporary
/// B-recovery adapter, this closed type has no impossible standby-retirement
/// branch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerInitialCurrentTerminalDrive {
    Pending,
    Applied {
        generation: LegGeneration,
        reason: LegTransportTerminalReason,
    },
    AlreadyApplied {
        generation: LegGeneration,
        reason: LegTransportTerminalReason,
    },
}

impl QuinnOwnerInitialCurrentTerminalDrive {
    pub(crate) const fn is_pending(self) -> bool {
        matches!(self, Self::Pending)
    }

    pub(crate) const fn newly_applied_identity(
        self,
    ) -> Option<(LegGeneration, LegTransportTerminalReason)> {
        match self {
            Self::Applied { generation, reason } => Some((generation, reason)),
            Self::Pending | Self::AlreadyApplied { .. } => None,
        }
    }

    pub(crate) const fn already_applied_identity(
        self,
    ) -> Option<(LegGeneration, LegTransportTerminalReason)> {
        match self {
            Self::AlreadyApplied { generation, reason } => Some((generation, reason)),
            Self::Pending | Self::Applied { .. } => None,
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum QuinnOwnerInitialCurrentTerminalError {
    #[error("initial current owner actor failed: {0}")]
    Actor(#[from] QuinnLegActorError),
    #[error("initial current owner local cancellation belongs to the shutdown path")]
    UnexpectedLocalCancellation,
    #[error("initial current owner hard terminal lacks its first-failure timestamp")]
    MissingFailureTimestamp,
    #[error("initial current owner terminal bind was rejected: {0:?}")]
    ControllerBind(OwnerSessionRecoveryTerminalBindErrorKind),
    #[error("initial current owner terminal discard entered sticky controller failure")]
    ControllerDiscard,
    #[error("initial current owner terminal/join composition invariant failed")]
    CompositionInvariant,
    #[error("initial current owner join runtime is unavailable: {0}")]
    RuntimeUnavailable(Arc<str>),
}

/// Cancellation-safe current-A terminal branch.  This is deliberately not a
/// complete owner root: it owns no registered B, output FIFO, timers, hint
/// policy, or root outcome arbiter.  It does keep the exact physical A,
/// initial handoff, sole controller, terminal stamp, and sticky controller
/// failure in one non-cloneable ownership domain until structural join.
#[must_use = "the terminal-tracked current actor must be joined or retained for root cleanup"]
pub(crate) struct QuinnOwnerInitialTerminalTrackedCurrent<T: TargetIo> {
    actor: QuinnOwnerInitialActorSlot,
    terminal: QuinnOwnerInitialControllerTerminalRoot<T>,
}

/// Structural current-A join observation.  Controller and high-water counts
/// remain nonzero by design, so this value cannot be mistaken for whole-root
/// zero ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnOwnerInitialTerminalJoined {
    actor_owned_messages: usize,
    actor_owned_bytes: usize,
    actor_owned_terminals: usize,
    actor_terminal_failure: bool,
    retained_controllers: usize,
    retained_legless_high_waters: usize,
    retained_terminal_stamps: usize,
    terminal_stamp_pair_verified: bool,
}

impl QuinnOwnerInitialTerminalJoined {
    fn observe<T: TargetIo>(
        joined: &QuinnLegActorJoined,
        terminal: &QuinnOwnerInitialControllerTerminalRoot<T>,
    ) -> Self {
        let snapshot = terminal.controller.snapshot();
        Self {
            actor_owned_messages: joined.owned_messages(),
            actor_owned_bytes: joined.owned_bytes(),
            actor_owned_terminals: joined.owned_terminals(),
            actor_terminal_failure: joined.terminal_failure().is_some(),
            retained_controllers: 1,
            retained_legless_high_waters: snapshot.pending_legless_high_waters,
            retained_terminal_stamps: usize::from(joined.terminal_stamp().is_some())
                + terminal.retained_handoff_stamps(),
            terminal_stamp_pair_verified: terminal.state.stamp_pair_verified(),
        }
    }

    pub(crate) const fn actor_owned_messages(self) -> usize {
        self.actor_owned_messages
    }

    pub(crate) const fn actor_owned_bytes(self) -> usize {
        self.actor_owned_bytes
    }

    pub(crate) const fn actor_owned_terminals(self) -> usize {
        self.actor_owned_terminals
    }

    pub(crate) const fn actor_terminal_failure(self) -> bool {
        self.actor_terminal_failure
    }

    pub(crate) const fn retained_controllers(self) -> usize {
        self.retained_controllers
    }

    pub(crate) const fn retained_legless_high_waters(self) -> usize {
        self.retained_legless_high_waters
    }

    pub(crate) const fn retained_terminal_stamps(self) -> usize {
        self.retained_terminal_stamps
    }

    pub(crate) const fn terminal_stamp_pair_verified(self) -> bool {
        self.terminal_stamp_pair_verified
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnOwnerInitialLeglessTransferBlock {
    TerminalNotApplied,
    ActiveHandoffRetained,
    CurrentActorNotJoined,
    TerminalStampMissing,
    TerminalStampMismatch,
    ControllerFailure,
    ControllerInventory,
    CompositionInvariant,
}

#[must_use = "a rejected legless transfer returns the complete terminal-tracked owner"]
pub(crate) struct QuinnOwnerInitialLeglessTransferFailure<T: TargetIo> {
    block: QuinnOwnerInitialLeglessTransferBlock,
    root: QuinnOwnerInitialTerminalTrackedCurrent<T>,
}

impl<T: TargetIo> QuinnOwnerInitialLeglessTransferFailure<T> {
    pub(crate) const fn block(&self) -> QuinnOwnerInitialLeglessTransferBlock {
        self.block
    }

    pub(crate) fn into_root(self) -> QuinnOwnerInitialTerminalTrackedCurrent<T> {
        self.root
    }
}

impl<T: TargetIo> fmt::Debug for QuinnOwnerInitialLeglessTransferFailure<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnOwnerInitialLeglessTransferFailure")
            .field("block", &self.block)
            .field("root", &"[REDACTED]")
            .finish()
    }
}

/// Joined-A legless branch handed to the future full owner root.  It still
/// owns one controller/executor, one exact legless high-water and the actor's
/// non-cloneable terminal stamp; those nonzero capabilities are explicit in
/// its inventory and must be finalized before any exact-zero claim.
struct QuinnOwnerInitialTerminalStampPairWitness;

#[must_use = "legless owner parts retain controller high-water and terminal ownership"]
pub(crate) struct QuinnOwnerInitialLeglessRootParts<T: TargetIo> {
    joined: Box<QuinnLegActorJoined>,
    controller: OwnerSessionController<T>,
    terminal_stamp_pair: QuinnOwnerInitialTerminalStampPairWitness,
    generation: LegGeneration,
    reason: LegTransportTerminalReason,
}

impl<T: TargetIo> QuinnOwnerInitialLeglessRootParts<T> {
    pub(crate) const fn retained_current_actors(&self) -> usize {
        0
    }

    pub(crate) const fn retained_joined_actors(&self) -> usize {
        1
    }

    pub(crate) const fn retained_controllers(&self) -> usize {
        1
    }

    pub(crate) const fn retained_executors(&self) -> usize {
        1
    }

    pub(crate) const fn retained_active_handoffs(&self) -> usize {
        0
    }

    pub(crate) fn retained_legless_high_waters(&self) -> usize {
        self.controller.snapshot().pending_legless_high_waters
    }

    pub(crate) const fn retained_terminal_stamps(&self) -> usize {
        if self.joined.terminal_stamp().is_some() {
            1
        } else {
            0
        }
    }

    pub(crate) const fn terminal_stamp_pair_verified(&self) -> bool {
        let _ = &self.terminal_stamp_pair;
        true
    }

    pub(crate) fn joined_actor_owned_messages(&self) -> usize {
        self.joined.owned_messages()
    }

    pub(crate) fn joined_actor_owned_bytes(&self) -> usize {
        self.joined.owned_bytes()
    }

    pub(crate) fn joined_actor_owned_terminals(&self) -> usize {
        self.joined.owned_terminals()
    }

    pub(crate) fn joined_actor_terminal_failure(&self) -> bool {
        self.joined.terminal_failure().is_some()
    }

    pub(crate) const fn terminal_generation(&self) -> LegGeneration {
        self.generation
    }

    pub(crate) const fn terminal_reason(&self) -> LegTransportTerminalReason {
        self.reason
    }
}

impl<T: TargetIo> QuinnOwnerInitialAcceptedRootParts<T> {
    pub(crate) const fn retained_current_actors(&self) -> usize {
        1
    }

    pub(crate) const fn retained_controllers(&self) -> usize {
        1
    }

    pub(crate) const fn retained_executors(&self) -> usize {
        1
    }

    pub(crate) const fn retained_attached_authorities(&self) -> usize {
        1
    }

    pub(crate) const fn retained_acceptance_receipts(&self) -> usize {
        self.current.handoff.retained_acceptance_receipts()
    }

    pub(crate) const fn retained_physical_actor_identities(&self) -> usize {
        self.current.handoff.physical_actor_identities()
    }

    pub(crate) fn controller_shares_current_actor_clock(&self) -> bool {
        let gate = QuinnLegCompositionGate::mint();
        let actor_clock = self.current.actor.session_clock(&gate);
        self.controller.shares_session_clock(&actor_clock, &gate)
    }

    pub(crate) fn current_actor_inventory(
        &mut self,
    ) -> Result<OwnerTargetInitialHandoffInventory, QuinnOwnerInitialShutdownError> {
        let gate = QuinnLegCompositionGate::mint();
        self.current
            .actor
            .with_controller_parts_mut(&gate, |_io, queue| self.current.handoff.inventory(queue))
            .ok_or(QuinnOwnerInitialShutdownError::CompositionInvariant)?
            .map_err(QuinnOwnerInitialShutdownError::Handoff)
    }

    /// Authenticates exactly one physical B registration while A remains the
    /// exact accepted current handoff.  Success consumes these root parts into
    /// a one-shot transaction, so the same A can never mint independent B and
    /// C candidates outside the max-two-actor root.
    pub(crate) fn authenticate_initial_standby(
        self,
        candidate: &EstablishedLeg,
        inbound: BoundInbound,
    ) -> Result<
        QuinnOwnerInitialAuthenticatedStandby<T>,
        QuinnOwnerInitialStandbyAuthenticationFailure<T>,
    > {
        let gate = QuinnLegCompositionGate::mint();
        let ready = match self
            .controller
            .preflight_initial_standby_registration(&self.current.handoff, &gate)
        {
            Ok(ready) => ready,
            Err(block) => {
                return Err(QuinnOwnerInitialStandbyAuthenticationFailure {
                    root: self,
                    kind: QuinnOwnerInitialStandbyAuthenticationFailureKind::Blocked {
                        block,
                        inbound: Box::new(inbound),
                    },
                });
            }
        };
        let authenticated = match self.controller.authenticate_initial_standby(
            ready,
            &self.current.handoff,
            candidate,
            inbound,
            &gate,
        ) {
            Ok(authenticated) => authenticated,
            Err(error) => {
                return Err(QuinnOwnerInitialStandbyAuthenticationFailure {
                    root: self,
                    kind: QuinnOwnerInitialStandbyAuthenticationFailureKind::Target(error),
                });
            }
        };
        Ok(QuinnOwnerInitialAuthenticatedStandby {
            seed: self.into_physical_root_seed(),
            candidate: authenticated,
        })
    }

    /// Consumes the already-settled accepted branch into current-terminal
    /// tracking.  The legacy adapter below remains available only on the
    /// pre-terminal type, so a consumed AttachedLeg can never be reconstructed
    /// as an initial active handoff.
    pub(crate) fn into_terminal_tracked_current(
        self,
    ) -> QuinnOwnerInitialTerminalTrackedCurrent<T> {
        let Self {
            current: QuinnOwnerInitialCurrentActorPort { actor, handoff },
            controller,
        } = self;
        QuinnOwnerInitialTerminalTrackedCurrent {
            actor: QuinnOwnerInitialActorSlot::running(actor),
            terminal: QuinnOwnerInitialControllerTerminalRoot {
                controller,
                active: Some(handoff),
                state: QuinnOwnerInitialCurrentTerminalState::Unbound,
            },
        }
    }

    /// Transfers the accepted current-A/controller pair into the full
    /// physical root without exposing a naked AttachedLeg or executor.
    pub(super) fn into_physical_root_seed(self) -> QuinnOwnerInitialPhysicalRootSeed<T> {
        let Self {
            current,
            controller,
        } = self;
        QuinnOwnerInitialPhysicalRootSeed {
            current,
            controller,
        }
    }

    /// Compatibility adapter for the already-accepted initial shutdown path.
    /// The transfer is consuming, so it cannot mint a second actor/controller
    /// pair or reconstruct an acceptance receipt.
    pub(crate) fn into_initial_composition(self) -> QuinnOwnerInitialSessionComposition<T> {
        let Self {
            current: QuinnOwnerInitialCurrentActorPort { actor, handoff },
            controller,
        } = self;
        QuinnOwnerInitialSessionComposition {
            actor: QuinnOwnerInitialActorSlot::running(actor),
            controller,
            handoff: Some(handoff),
            shutdown_requested: false,
            acceptance_settlement: Some(
                OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow,
            ),
        }
    }
}

impl<T: TargetIo> fmt::Debug for QuinnOwnerInitialAcceptedRootParts<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnOwnerInitialAcceptedRootParts")
            .field("current", &"[REDACTED]")
            .field("controller", &"[REDACTED]")
            .finish()
    }
}

impl<T: TargetIo> QuinnOwnerInitialTerminalTrackedCurrent<T> {
    pub(crate) fn retained_active_handoffs(&self) -> usize {
        self.terminal.retained_active_handoffs()
    }

    pub(crate) fn retained_terminal_stamps(&self) -> usize {
        let handoff = self.terminal.retained_handoff_stamps();
        let terminal_was_consumed = self.terminal.state.actor_terminal_may_be_consumed();
        let root_outcome = match self.actor.as_ref() {
            Some(QuinnOwnerInitialActorOwner::Running(_))
            | Some(QuinnOwnerInitialActorOwner::Joining(_)) => usize::from(terminal_was_consumed),
            Some(QuinnOwnerInitialActorOwner::Joined(joined)) => {
                usize::from(joined.terminal_stamp().is_some())
            }
            Some(QuinnOwnerInitialActorOwner::TaskLost(_)) | None => 0,
        };
        handoff + root_outcome
    }

    pub(crate) fn controller_snapshot(&self) -> OwnerSessionControllerSnapshot {
        self.terminal.controller.snapshot()
    }

    /// Test-only state-machine probe. It never touches the actor, controller,
    /// handoff, or terminal envelope; resetting between cases is therefore not
    /// a production settlement bypass.
    #[cfg(test)]
    pub(crate) fn terminal_transaction_unwind_matrix_for_test(&mut self) -> (bool, bool, bool) {
        let prepared_drop_restored = {
            let transaction = self
                .terminal
                .begin_terminal_transaction()
                .expect("fresh current-A terminal root prepares exactly once");
            drop(transaction);
            matches!(
                &self.terminal.state,
                QuinnOwnerInitialCurrentTerminalState::Unbound
            )
        };

        let entered_unwind_quarantined = {
            let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut transaction = self
                    .terminal
                    .begin_terminal_transaction()
                    .expect("prepared-drop probe restored the current-A root");
                transaction.enter_actor_callback();
                panic!("synthetic current-A post-entry unwind");
            }));
            unwind.is_err()
                && self.terminal.state.is_quarantined()
                && self.terminal.state.actor_terminal_may_be_consumed()
        };
        self.terminal.state = QuinnOwnerInitialCurrentTerminalState::Unbound;

        let entered_forget_quarantined = {
            let mut transaction = self
                .terminal
                .begin_terminal_transaction()
                .expect("test-only reset restored the current-A root");
            transaction.enter_actor_callback();
            std::mem::forget(transaction);
            self.terminal.state.is_quarantined()
                && self.terminal.state.actor_terminal_may_be_consumed()
        };
        self.terminal.state = QuinnOwnerInitialCurrentTerminalState::Unbound;

        (
            prepared_drop_restored,
            entered_unwind_quarantined,
            entered_forget_quarantined,
        )
    }

    /// Moves the actor's exact hard-terminal envelope through binding and
    /// discard in one synchronous actor callback. Pre-bind rejection restores
    /// the envelope unchanged. Once callback entry consumes the actor slot,
    /// unwind or controller failure remains visibly quarantined; there is no
    /// un-woken retry state between binding and legless high-water creation.
    pub(crate) fn try_drive_reported_terminal(
        &mut self,
    ) -> Result<QuinnOwnerInitialCurrentTerminalDrive, QuinnOwnerInitialCurrentTerminalError> {
        match &self.terminal.state {
            QuinnOwnerInitialCurrentTerminalState::Applied { .. }
            | QuinnOwnerInitialCurrentTerminalState::JoinedCorrelated { .. } => {
                let Some((generation, reason)) = self.terminal.state.identity() else {
                    return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
                };
                return Ok(QuinnOwnerInitialCurrentTerminalDrive::AlreadyApplied {
                    generation,
                    reason,
                });
            }
            QuinnOwnerInitialCurrentTerminalState::Quarantined { .. } => {
                return if self.terminal.state.is_controller_failed() {
                    Err(QuinnOwnerInitialCurrentTerminalError::ControllerDiscard)
                } else {
                    Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant)
                };
            }
            QuinnOwnerInitialCurrentTerminalState::Prepared => {
                return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
            }
            QuinnOwnerInitialCurrentTerminalState::Unbound => {}
        }
        let gate = QuinnLegCompositionGate::mint();
        let Self { actor, terminal } = self;
        let actor = match actor.as_mut() {
            Some(QuinnOwnerInitialActorOwner::Running(actor)) => actor.as_mut(),
            Some(QuinnOwnerInitialActorOwner::Joining(_)) => {
                return Err(QuinnOwnerInitialCurrentTerminalError::Actor(
                    QuinnLegActorError::Closing,
                ));
            }
            Some(QuinnOwnerInitialActorOwner::Joined(_)) => {
                return Err(QuinnOwnerInitialCurrentTerminalError::Actor(
                    QuinnLegActorError::AlreadyJoined,
                ));
            }
            Some(QuinnOwnerInitialActorOwner::TaskLost(cause)) => {
                return Err(QuinnOwnerInitialCurrentTerminalError::Actor(
                    QuinnLegActorError::TaskJoin(cause.to_string()),
                ));
            }
            None => return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant),
        };
        let mut terminal_transaction = terminal.begin_terminal_transaction()?;
        let transaction = actor.try_transact_reported_terminal_with_parts(
            &gate,
            |runtime, io, queue, reported| {
                terminal_transaction.apply_reported_terminal(runtime, io, queue, reported, &gate)
            },
        )?;
        match transaction {
            QuinnLegActorReportedTerminalPoll::Pending => {
                Ok(QuinnOwnerInitialCurrentTerminalDrive::Pending)
            }
            QuinnLegActorReportedTerminalPoll::Rejected(error) => {
                let composition_invariant = matches!(
                    &error,
                    QuinnOwnerInitialCurrentTerminalError::CompositionInvariant
                );
                terminal_transaction.restore_actor_rejection();
                if composition_invariant {
                    terminal_transaction.quarantine_restored_composition_invariant();
                }
                Err(error)
            }
            QuinnLegActorReportedTerminalPoll::AlreadyConsumed => {
                terminal_transaction.quarantine_actor_already_consumed();
                Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant)
            }
            QuinnLegActorReportedTerminalPoll::Applied(commit) => match commit {
                QuinnOwnerInitialTerminalCommit::Applied { generation, reason } => {
                    Ok(QuinnOwnerInitialCurrentTerminalDrive::Applied { generation, reason })
                }
                QuinnOwnerInitialTerminalCommit::ControllerDiscardFailed => {
                    Err(QuinnOwnerInitialCurrentTerminalError::ControllerDiscard)
                }
                QuinnOwnerInitialTerminalCommit::CompositionInvariant => {
                    Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant)
                }
            },
        }
    }

    fn start_join_after_terminal(&mut self) -> Result<(), QuinnOwnerInitialCurrentTerminalError> {
        let stamp_state_valid = matches!(
            &self.terminal.state,
            QuinnOwnerInitialCurrentTerminalState::Applied { .. }
                | QuinnOwnerInitialCurrentTerminalState::JoinedCorrelated { .. }
        );
        if self.terminal.active.is_some() || !stamp_state_valid {
            return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
        }
        let snapshot = self.terminal.controller.snapshot();
        if snapshot.failed
            || snapshot.pending_recoveries != 0
            || snapshot.pending_legless_high_waters != 1
        {
            return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
        }
        match self.actor.as_ref() {
            Some(QuinnOwnerInitialActorOwner::Joining(_))
            | Some(QuinnOwnerInitialActorOwner::Joined(_)) => return Ok(()),
            Some(QuinnOwnerInitialActorOwner::TaskLost(cause)) => {
                return Err(QuinnOwnerInitialCurrentTerminalError::Actor(
                    QuinnLegActorError::TaskJoin(cause.to_string()),
                ));
            }
            Some(QuinnOwnerInitialActorOwner::Running(_)) => {}
            None => return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant),
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            QuinnOwnerInitialCurrentTerminalError::RuntimeUnavailable(Arc::from(error.to_string()))
        })?;
        let owner = self
            .actor
            .take()
            .ok_or(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant)?;
        let QuinnOwnerInitialActorOwner::Running(actor) = owner else {
            self.actor.set(owner);
            return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
        };
        self.actor.set(QuinnOwnerInitialActorOwner::Joining(
            spawn_owner_initial_join(&runtime, *actor),
        ));
        Ok(())
    }

    fn finish_joined_terminal_correlation(
        &mut self,
    ) -> Result<QuinnOwnerInitialTerminalJoined, QuinnOwnerInitialCurrentTerminalError> {
        let owner = self
            .actor
            .take()
            .ok_or(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant)?;
        let QuinnOwnerInitialActorOwner::Joined(joined) = owner else {
            self.actor.set(owner);
            return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
        };
        match &self.terminal.state {
            QuinnOwnerInitialCurrentTerminalState::Applied { handoff_stamp, .. } => {
                let stamps_match = joined
                    .terminal_stamp()
                    .is_some_and(|root_outcome| root_outcome.same_source(handoff_stamp));
                if !stamps_match {
                    let previous = std::mem::replace(
                        &mut self.terminal.state,
                        QuinnOwnerInitialCurrentTerminalState::Quarantined {
                            ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Unknown,
                            cause: QuinnOwnerInitialTerminalQuarantineCause::CompositionInvariant,
                        },
                    );
                    if let QuinnOwnerInitialCurrentTerminalState::Applied {
                        discarded,
                        handoff_stamp,
                    } = previous
                    {
                        self.terminal.state = QuinnOwnerInitialCurrentTerminalState::Quarantined {
                            ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Discarded {
                                terminal: discarded,
                                handoff_stamp,
                            },
                            cause: QuinnOwnerInitialTerminalQuarantineCause::JoinStampMismatch,
                        };
                    }
                    self.actor.set(QuinnOwnerInitialActorOwner::Joined(joined));
                    return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
                }
                let previous = std::mem::replace(
                    &mut self.terminal.state,
                    QuinnOwnerInitialCurrentTerminalState::Quarantined {
                        ownership: QuinnOwnerInitialTerminalQuarantineOwnership::Unknown,
                        cause: QuinnOwnerInitialTerminalQuarantineCause::CompositionInvariant,
                    },
                );
                let QuinnOwnerInitialCurrentTerminalState::Applied {
                    discarded,
                    handoff_stamp,
                } = previous
                else {
                    self.actor.set(QuinnOwnerInitialActorOwner::Joined(joined));
                    return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
                };
                let generation = discarded.generation();
                let reason = discarded.reason();
                drop(handoff_stamp);
                drop(discarded);
                self.terminal.state =
                    QuinnOwnerInitialCurrentTerminalState::JoinedCorrelated { generation, reason };
            }
            QuinnOwnerInitialCurrentTerminalState::JoinedCorrelated { .. } => {}
            QuinnOwnerInitialCurrentTerminalState::Unbound
            | QuinnOwnerInitialCurrentTerminalState::Prepared
            | QuinnOwnerInitialCurrentTerminalState::Quarantined { .. } => {
                self.actor.set(QuinnOwnerInitialActorOwner::Joined(joined));
                return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
            }
        }
        let observation = QuinnOwnerInitialTerminalJoined::observe(&joined, &self.terminal);
        self.actor.set(QuinnOwnerInitialActorOwner::Joined(joined));
        if observation.retained_terminal_stamps() != 1
            || !observation.terminal_stamp_pair_verified()
        {
            return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
        }
        Ok(observation)
    }

    /// Starts the unique join task synchronously before awaiting it.  A
    /// cancelled waiter leaves that handle in this root; actor join failure
    /// restores the exact terminal-consumed actor for a bounded retry.
    pub(crate) async fn join_current_after_terminal(
        &mut self,
    ) -> Result<QuinnOwnerInitialTerminalJoined, QuinnOwnerInitialCurrentTerminalError> {
        self.start_join_after_terminal()?;
        if matches!(
            self.actor.as_ref(),
            Some(QuinnOwnerInitialActorOwner::Joined(_))
        ) {
            return self.finish_joined_terminal_correlation();
        }
        let outcome = {
            let Some(QuinnOwnerInitialActorOwner::Joining(task)) = self.actor.as_mut() else {
                return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
            };
            task.await
        };
        let owner = self
            .actor
            .take()
            .ok_or(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant)?;
        if !matches!(&owner, QuinnOwnerInitialActorOwner::Joining(_)) {
            self.actor.set(owner);
            return Err(QuinnOwnerInitialCurrentTerminalError::CompositionInvariant);
        }
        drop(owner);
        match outcome {
            Ok(QuinnOwnerInitialJoinOutcome { actor, joined }) => match joined {
                Ok(joined) => {
                    drop(actor);
                    self.actor
                        .set(QuinnOwnerInitialActorOwner::Joined(Box::new(joined)));
                    self.finish_joined_terminal_correlation()
                }
                Err(error) => {
                    self.actor
                        .set(QuinnOwnerInitialActorOwner::Running(Box::new(actor)));
                    Err(QuinnOwnerInitialCurrentTerminalError::Actor(error))
                }
            },
            Err(error) => {
                let cause: Arc<str> = Arc::from(error.to_string());
                self.actor
                    .set(QuinnOwnerInitialActorOwner::TaskLost(Arc::clone(&cause)));
                Err(QuinnOwnerInitialCurrentTerminalError::Actor(
                    QuinnLegActorError::TaskJoin(cause.to_string()),
                ))
            }
        }
    }

    /// Consumes only a structurally joined, terminal-applied current branch.
    /// The resulting legless parts retain every nonzero logical owner instead
    /// of reporting the actor's zero inventory as whole-session zero.
    pub(crate) fn try_into_legless_root_parts(
        mut self,
    ) -> Result<QuinnOwnerInitialLeglessRootParts<T>, QuinnOwnerInitialLeglessTransferFailure<T>>
    {
        let (generation, reason) = match &self.terminal.state {
            QuinnOwnerInitialCurrentTerminalState::JoinedCorrelated { generation, reason } => {
                (*generation, *reason)
            }
            QuinnOwnerInitialCurrentTerminalState::Unbound
            | QuinnOwnerInitialCurrentTerminalState::Prepared
            | QuinnOwnerInitialCurrentTerminalState::Quarantined { .. }
            | QuinnOwnerInitialCurrentTerminalState::Applied { .. } => {
                let block = if self.terminal.state.is_quarantined() {
                    QuinnOwnerInitialLeglessTransferBlock::ControllerFailure
                } else {
                    QuinnOwnerInitialLeglessTransferBlock::TerminalNotApplied
                };
                return Err(QuinnOwnerInitialLeglessTransferFailure { block, root: self });
            }
        };
        if self.terminal.active.is_some() {
            return Err(QuinnOwnerInitialLeglessTransferFailure {
                block: QuinnOwnerInitialLeglessTransferBlock::ActiveHandoffRetained,
                root: self,
            });
        }
        let snapshot = self.terminal.controller.snapshot();
        if snapshot.failed
            || snapshot.pending_recoveries != 0
            || snapshot.pending_legless_high_waters != 1
        {
            return Err(QuinnOwnerInitialLeglessTransferFailure {
                block: QuinnOwnerInitialLeglessTransferBlock::ControllerInventory,
                root: self,
            });
        }
        let owner = self.actor.take();
        let joined = match owner {
            Some(QuinnOwnerInitialActorOwner::Joined(joined)) => joined,
            Some(owner) => {
                self.actor.set(owner);
                return Err(QuinnOwnerInitialLeglessTransferFailure {
                    block: QuinnOwnerInitialLeglessTransferBlock::CurrentActorNotJoined,
                    root: self,
                });
            }
            None => {
                return Err(QuinnOwnerInitialLeglessTransferFailure {
                    block: QuinnOwnerInitialLeglessTransferBlock::CompositionInvariant,
                    root: self,
                });
            }
        };
        if joined.terminal_stamp().is_none() {
            self.actor.set(QuinnOwnerInitialActorOwner::Joined(joined));
            return Err(QuinnOwnerInitialLeglessTransferFailure {
                block: QuinnOwnerInitialLeglessTransferBlock::TerminalStampMissing,
                root: self,
            });
        }
        let Self { terminal, .. } = self;
        let QuinnOwnerInitialControllerTerminalRoot { controller, .. } = terminal;
        Ok(QuinnOwnerInitialLeglessRootParts {
            joined,
            controller,
            terminal_stamp_pair: QuinnOwnerInitialTerminalStampPairWitness,
            generation,
            reason,
        })
    }
}

impl<T: TargetIo> fmt::Debug for QuinnOwnerInitialTerminalTrackedCurrent<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuinnOwnerInitialTerminalTrackedCurrent")
            .field("terminal_state", &self.terminal.state)
            .field("active", &self.terminal.active.is_some())
            .finish_non_exhaustive()
    }
}

pub(crate) struct QuinnOwnerInitialSessionComposition<T: TargetIo> {
    actor: QuinnOwnerInitialActorSlot,
    controller: OwnerSessionController<T>,
    handoff: Option<OwnerTargetInitialHandoff>,
    shutdown_requested: bool,
    acceptance_settlement: Option<OwnerTargetInitialAcceptanceSettlementDisposition>,
}

impl<T: TargetIo> QuinnOwnerInitialSessionComposition<T> {
    /// Controller-first adoption used by the future full physical owner root.
    /// The actor clock, controller, executor-bound handoff, runtime incarnation,
    /// policy leg and exact FIFO are validated and installed in one synchronous
    /// turn. Every rejection returns all three unique owners unchanged.
    pub(crate) fn adopt_controller(
        mut actor: StartedQuinnLegActor,
        controller: OwnerSessionController<T>,
        handoff: OwnerTargetInitialHandoff,
    ) -> Result<Self, QuinnOwnerInitialControllerAdoptFailure<T>> {
        let gate = QuinnLegCompositionGate::mint();
        let actor_clock = actor.session_clock(&gate);
        if !controller.shares_session_clock(&actor_clock, &gate) {
            return Err(QuinnOwnerInitialControllerAdoptFailure {
                kind: QuinnOwnerInitialAdoptErrorKind::WrongSessionClock,
                ownership: Box::new((actor, controller, handoff)),
            });
        }
        let bound = actor.with_physical_controller_parts_mut_input(
            &gate,
            handoff,
            |handoff, runtime, io, queue| {
                controller
                    .bind_initial_handoff_to_physical_actor(handoff, runtime, io, queue, &gate)
            },
        );
        let handoff = match bound {
            Ok(Ok(handoff)) => handoff,
            Ok(Err(failure)) => {
                let kind = QuinnOwnerInitialAdoptErrorKind::Handoff(failure.block());
                return Err(QuinnOwnerInitialControllerAdoptFailure {
                    kind,
                    ownership: Box::new((actor, controller, failure.into_handoff())),
                });
            }
            Err((handoff, error)) => {
                return Err(QuinnOwnerInitialControllerAdoptFailure {
                    kind: QuinnOwnerInitialAdoptErrorKind::Actor(error),
                    ownership: Box::new((actor, controller, handoff)),
                });
            }
        };
        Ok(Self {
            actor: QuinnOwnerInitialActorSlot::running(actor),
            controller,
            handoff: Some(handoff),
            shutdown_requested: false,
            acceptance_settlement: None,
        })
    }

    pub(crate) const fn shutdown_requested(&self) -> bool {
        self.shutdown_requested
    }

    pub(crate) fn controller_snapshot(&self) -> OwnerSessionControllerSnapshot {
        self.controller.snapshot()
    }

    pub(crate) const fn output_page_capacity(&self) -> OwnerOutputPageCapacity {
        self.controller.output_page_capacity()
    }

    /// Narrows a live initial composition into the accepted current-actor
    /// branch used by the future whole-session root.  Admission or physical
    /// delivery alone is insufficient: the exact acceptance receipt must have
    /// been committed and synchronously settled through this controller.
    /// Every rejected branch returns the complete composition unchanged.
    pub(crate) fn try_into_accepted_root_parts(
        mut self,
    ) -> Result<QuinnOwnerInitialAcceptedRootParts<T>, QuinnOwnerInitialRootPartsFailure<T>> {
        if self.shutdown_requested {
            return Err(QuinnOwnerInitialRootPartsFailure {
                block: QuinnOwnerInitialRootPartsBlock::ShutdownRequested,
                root: self,
            });
        }
        let inventory = match self.handoff_inventory() {
            Ok(inventory) => inventory,
            Err(_error) => {
                return Err(QuinnOwnerInitialRootPartsFailure {
                    block: QuinnOwnerInitialRootPartsBlock::CompositionInvariant,
                    root: self,
                });
            }
        };
        if self.acceptance_settlement
            != Some(OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow)
            || inventory.acceptance_receipts() != 0
            || inventory.delivery() != OwnerTargetInitialAcceptanceDelivery::Settled
        {
            return Err(QuinnOwnerInitialRootPartsFailure {
                block: QuinnOwnerInitialRootPartsBlock::AcceptanceNotSettled(inventory),
                root: self,
            });
        }
        if inventory.attached_authorities() != 1
            || !inventory.executor_identity_live()
            || inventory.physical_actor_identities() != 1
            || !inventory.physical_actor_identity_live()
        {
            return Err(QuinnOwnerInitialRootPartsFailure {
                block: QuinnOwnerInitialRootPartsBlock::CompositionInvariant,
                root: self,
            });
        }
        let actor_clock = match self.actor.as_ref() {
            Some(QuinnOwnerInitialActorOwner::Running(actor)) => {
                let gate = QuinnLegCompositionGate::mint();
                actor.session_clock(&gate)
            }
            Some(QuinnOwnerInitialActorOwner::Joining(_))
            | Some(QuinnOwnerInitialActorOwner::Joined(_))
            | Some(QuinnOwnerInitialActorOwner::TaskLost(_))
            | None => {
                return Err(QuinnOwnerInitialRootPartsFailure {
                    block: QuinnOwnerInitialRootPartsBlock::CurrentActorNotRunning,
                    root: self,
                });
            }
        };
        let gate = QuinnLegCompositionGate::mint();
        if !self.controller.shares_session_clock(&actor_clock, &gate) {
            return Err(QuinnOwnerInitialRootPartsFailure {
                block: QuinnOwnerInitialRootPartsBlock::CompositionInvariant,
                root: self,
            });
        }

        let actor_owner = self.actor.take();
        let handoff = self.handoff.take();
        match (actor_owner, handoff) {
            (Some(QuinnOwnerInitialActorOwner::Running(actor)), Some(handoff)) => {
                let Self { controller, .. } = self;
                Ok(QuinnOwnerInitialAcceptedRootParts {
                    current: QuinnOwnerInitialCurrentActorPort {
                        actor: *actor,
                        handoff,
                    },
                    controller,
                })
            }
            (actor_owner, handoff) => {
                if let Some(actor_owner) = actor_owner {
                    self.actor.set(actor_owner);
                }
                self.handoff = handoff;
                Err(QuinnOwnerInitialRootPartsFailure {
                    block: QuinnOwnerInitialRootPartsBlock::CompositionInvariant,
                    root: self,
                })
            }
        }
    }

    pub(crate) fn handoff_inventory(
        &mut self,
    ) -> Result<OwnerTargetInitialHandoffInventory, QuinnOwnerInitialShutdownError> {
        let Self { actor, handoff, .. } = self;
        let handoff = handoff
            .as_ref()
            .ok_or(QuinnOwnerInitialShutdownError::CompositionInvariant)?;
        let Some(QuinnOwnerInitialActorOwner::Running(actor)) = actor.as_mut() else {
            return Err(QuinnOwnerInitialShutdownError::CompositionInvariant);
        };
        let gate = QuinnLegCompositionGate::mint();
        actor
            .with_controller_parts_mut(&gate, |_io, queue| handoff.inventory(queue))
            .ok_or(QuinnOwnerInitialShutdownError::CompositionInvariant)?
            .map_err(QuinnOwnerInitialShutdownError::Handoff)
    }

    pub(crate) fn drive_initial_acceptance(
        &mut self,
    ) -> Result<QuinnOwnerInitialAcceptanceDrive, QuinnOwnerInitialShutdownError> {
        let handoff = self
            .handoff
            .take()
            .ok_or(QuinnOwnerInitialShutdownError::CompositionInvariant)?;
        let Some(QuinnOwnerInitialActorOwner::Running(actor)) = self.actor.as_mut() else {
            self.handoff = Some(handoff);
            return Err(QuinnOwnerInitialShutdownError::CompositionInvariant);
        };
        let gate = QuinnLegCompositionGate::mint();
        let attempt = actor.with_physical_controller_parts_mut_input(
            &gate,
            handoff,
            |handoff, runtime, io, queue| match self
                .controller
                .settle_initial_acceptance_delivery(handoff, runtime, io, queue, &gate)
            {
                Ok(settlement) => {
                    let disposition = settlement.disposition();
                    let handoff = settlement.into_handoff();
                    match handoff.inventory(queue) {
                        Ok(inventory) => Ok((handoff, disposition, inventory)),
                        Err(block) => Err((handoff, block, None)),
                    }
                }
                Err(failure) => {
                    let block = failure.block();
                    let handoff = failure.into_handoff();
                    let inventory = handoff.inventory(queue).ok();
                    Err((handoff, block, inventory))
                }
            },
        );
        match attempt {
            Ok(Ok((handoff, disposition, inventory))) => {
                self.handoff = Some(handoff);
                match (self.acceptance_settlement, disposition) {
                    (None, OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow) => {
                        self.acceptance_settlement = Some(disposition);
                    }
                    (
                        Some(OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow),
                        OwnerTargetInitialAcceptanceSettlementDisposition::AlreadySettled,
                    ) => {}
                    _ => return Err(QuinnOwnerInitialShutdownError::CompositionInvariant),
                }
                Ok(QuinnOwnerInitialAcceptanceDrive::Settled {
                    disposition,
                    inventory,
                })
            }
            Ok(Err((handoff, block, inventory))) => {
                self.handoff = Some(handoff);
                if block == OwnerTargetInputBlock::InitialAcceptanceDeliveryPending {
                    let inventory =
                        inventory.ok_or(QuinnOwnerInitialShutdownError::CompositionInvariant)?;
                    Ok(QuinnOwnerInitialAcceptanceDrive::Pending(inventory))
                } else {
                    Err(QuinnOwnerInitialShutdownError::Handoff(block))
                }
            }
            Err((handoff, error)) => {
                self.handoff = Some(handoff);
                Err(QuinnOwnerInitialShutdownError::Actor(error))
            }
        }
    }

    fn start_join_for_shutdown(&mut self) -> Result<(), QuinnOwnerInitialShutdownError> {
        self.shutdown_requested = true;
        match self.actor.as_ref() {
            Some(QuinnOwnerInitialActorOwner::Joining(_))
            | Some(QuinnOwnerInitialActorOwner::Joined(_)) => return Ok(()),
            Some(QuinnOwnerInitialActorOwner::TaskLost(cause)) => {
                return Err(QuinnOwnerInitialShutdownError::Actor(
                    QuinnLegActorError::TaskJoin(cause.to_string()),
                ));
            }
            Some(QuinnOwnerInitialActorOwner::Running(_)) => {}
            None => return Err(QuinnOwnerInitialShutdownError::CompositionInvariant),
        }
        match self.drive_initial_acceptance()? {
            QuinnOwnerInitialAcceptanceDrive::Pending(inventory) => {
                return Err(QuinnOwnerInitialShutdownError::AcceptancePending(inventory));
            }
            QuinnOwnerInitialAcceptanceDrive::Settled { inventory, .. } => {
                if inventory.acceptance_receipts() != 0 {
                    return Err(QuinnOwnerInitialShutdownError::CompositionInvariant);
                }
            }
        }
        let gate = QuinnLegCompositionGate::mint();
        let Some(QuinnOwnerInitialActorOwner::Running(actor)) = self.actor.as_mut() else {
            return Err(QuinnOwnerInitialShutdownError::CompositionInvariant);
        };
        match actor
            .try_retire_delivered_wait_for_shutdown(&gate)
            .map_err(QuinnOwnerInitialShutdownError::Actor)?
        {
            QuinnLegActorShutdownWaitDrive::Clean
            | QuinnLegActorShutdownWaitDrive::RetiredDelivered => {}
            QuinnLegActorShutdownWaitDrive::Pending => {
                return Err(QuinnOwnerInitialShutdownError::OutboundResidual {
                    queued_frames: 0,
                    queued_bytes: 0,
                    ordered_delivery_wait: true,
                });
            }
            QuinnLegActorShutdownWaitDrive::Residual {
                queued_frames,
                queued_bytes,
                ordered_delivery_wait,
            } => {
                return Err(QuinnOwnerInitialShutdownError::OutboundResidual {
                    queued_frames,
                    queued_bytes,
                    ordered_delivery_wait,
                });
            }
        }
        let (messages, bytes) = actor
            .shutdown_outbound_inventory(&gate)
            .map_err(QuinnOwnerInitialShutdownError::Actor)?;
        if messages != 0 || bytes != 0 {
            return Err(QuinnOwnerInitialShutdownError::OutboundResidual {
                queued_frames: messages,
                queued_bytes: bytes,
                ordered_delivery_wait: false,
            });
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
            QuinnOwnerInitialShutdownError::RuntimeUnavailable(Arc::from(error.to_string()))
        })?;
        let owner = self
            .actor
            .take()
            .ok_or(QuinnOwnerInitialShutdownError::CompositionInvariant)?;
        let QuinnOwnerInitialActorOwner::Running(actor) = owner else {
            self.actor.set(owner);
            return Err(QuinnOwnerInitialShutdownError::CompositionInvariant);
        };
        let mut actor = *actor;
        actor.request_cancel();
        self.actor.set(QuinnOwnerInitialActorOwner::Joining(
            spawn_owner_initial_join(&runtime, actor),
        ));
        Ok(())
    }

    pub(crate) async fn join_for_shutdown(
        &mut self,
    ) -> Result<QuinnOwnerInitialActorJoined, QuinnOwnerInitialShutdownError> {
        self.start_join_for_shutdown()?;
        if let Some(QuinnOwnerInitialActorOwner::Joined(joined)) = self.actor.as_ref() {
            let handoff = self
                .handoff
                .as_ref()
                .ok_or(QuinnOwnerInitialShutdownError::CompositionInvariant)?;
            return Ok(QuinnOwnerInitialActorJoined::observe(
                joined,
                &self.controller,
                handoff,
            ));
        }
        let outcome = {
            let Some(QuinnOwnerInitialActorOwner::Joining(task)) = self.actor.as_mut() else {
                return Err(QuinnOwnerInitialShutdownError::CompositionInvariant);
            };
            task.await
        };
        let owner = self
            .actor
            .take()
            .ok_or(QuinnOwnerInitialShutdownError::CompositionInvariant)?;
        if !matches!(&owner, QuinnOwnerInitialActorOwner::Joining(_)) {
            self.actor.set(owner);
            return Err(QuinnOwnerInitialShutdownError::CompositionInvariant);
        }
        drop(owner);
        match outcome {
            Ok(QuinnOwnerInitialJoinOutcome { actor, joined }) => match joined {
                Ok(joined) => {
                    drop(actor);
                    let handoff = self
                        .handoff
                        .as_ref()
                        .ok_or(QuinnOwnerInitialShutdownError::CompositionInvariant)?;
                    let observation =
                        QuinnOwnerInitialActorJoined::observe(&joined, &self.controller, handoff);
                    self.actor
                        .set(QuinnOwnerInitialActorOwner::Joined(Box::new(joined)));
                    Ok(observation)
                }
                Err(error) => {
                    self.actor
                        .set(QuinnOwnerInitialActorOwner::Running(Box::new(actor)));
                    Err(QuinnOwnerInitialShutdownError::Actor(error))
                }
            },
            Err(error) => {
                let cause: Arc<str> = Arc::from(error.to_string());
                self.actor
                    .set(QuinnOwnerInitialActorOwner::TaskLost(Arc::clone(&cause)));
                Err(QuinnOwnerInitialShutdownError::Actor(
                    QuinnLegActorError::TaskJoin(cause.to_string()),
                ))
            }
        }
    }

    #[cfg(test)]
    pub(in crate::owned_upstream) fn arm_join_block_for_test(
        &self,
    ) -> Option<QuinnLegActorJoinBlock> {
        match self.actor.as_ref()? {
            QuinnOwnerInitialActorOwner::Running(actor) => Some(actor.arm_join_block_for_test()),
            QuinnOwnerInitialActorOwner::Joining(_)
            | QuinnOwnerInitialActorOwner::Joined(_)
            | QuinnOwnerInitialActorOwner::TaskLost(_) => None,
        }
    }
}

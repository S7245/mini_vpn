//! Production physical-session Authority for the owner role.
//!
//! This deep Module owns the exact initial-A seed-to-root transaction and its
//! structural current-actor shutdown and the C-owned terminal timer Adapter.
//! Physical B, restart credit, public routing and whole-root zero remain open.
//! Construction must keep rollback before the fused CurrentOnly commit edge.
//! Focused tests live under `physical_session_root/tests/`.

use crate::owned_upstream::owner_output_drain::{
    OwnerOutputDrainConfigError, OwnerTerminalTimerRegistration,
};
use crate::owned_upstream::owner_session_controller::{
    OwnerSessionCapacityWitness, OwnerSessionController,
};
use crate::owned_upstream::owner_target::{
    OwnerTargetInitialAcceptanceDelivery, OwnerTargetInitialAcceptanceSettlementDisposition,
    OwnerTargetInitialHandoff, OwnerTargetInitialHandoffInventory, OwnerTargetInputBlock,
};
use crate::owned_upstream::quinn_leg_actor::{
    QuinnLegActorCapacityWitness, QuinnLegActorError, QuinnLegActorJoined,
    QuinnLegActorShutdownWaitDrive, StartedQuinnLegActor,
};
use crate::owned_upstream::target::TargetIo;

use super::super::QuinnLegCompositionGate;
use super::super::owner_initial::QuinnOwnerInitialPhysicalRootSeed;
use super::continuation::QuinnOwnerControllerOutputRoot;

mod service;

#[derive(Clone, Debug, PartialEq, Eq)]
enum OwnerPhysicalSessionRootBuildBlock {
    RuntimeUnavailable,
    ClockMismatch,
    ControllerNotExactZero,
    CapacityOverflow,
    ActorUnavailable,
    ActorFifoExceedsCapacity { messages: usize, bytes: usize },
    Handoff(OwnerTargetInputBlock),
    HandoffInventoryMismatch,
    AcceptanceNotSettled,
    Output(OwnerOutputDrainConfigError),
}

#[must_use = "rejected CurrentOnly construction retains the exact initial seed"]
struct OwnerPhysicalSessionRootBuildFailure<T: TargetIo> {
    seed: QuinnOwnerInitialPhysicalRootSeed<T>,
    block: OwnerPhysicalSessionRootBuildBlock,
}

impl<T: TargetIo> OwnerPhysicalSessionRootBuildFailure<T> {
    fn into_parts(
        self,
    ) -> (
        QuinnOwnerInitialPhysicalRootSeed<T>,
        OwnerPhysicalSessionRootBuildBlock,
    ) {
        (self.seed, self.block)
    }
}

struct OwnerPhysicalSessionRootConstructor<T: TargetIo> {
    actor: StartedQuinnLegActor,
    handoff: OwnerTargetInitialHandoff,
    controller: OwnerSessionController<T>,
}

struct OwnerPhysicalCurrentOnlyPrepared<T: TargetIo> {
    actor: StartedQuinnLegActor,
    handoff: OwnerTargetInitialHandoff,
    controller: OwnerSessionController<T>,
    capacity: OwnerPhysicalCurrentOnlyCapacity,
    restart_credit: OwnerPhysicalRestartCredit,
    terminal_timer: OwnerTerminalTimerRegistration,
}

#[derive(Clone, Copy)]
struct OwnerPhysicalCurrentOnlyCapacity {
    actor: QuinnLegActorCapacityWitness,
    controller: OwnerSessionCapacityWitness,
    max_owned_messages: usize,
    max_owned_bytes: usize,
}

impl OwnerPhysicalCurrentOnlyCapacity {
    fn checked(
        actor: QuinnLegActorCapacityWitness,
        controller: OwnerSessionCapacityWitness,
    ) -> Option<Self> {
        let output = controller.output_page();
        let target = controller.target();
        let max_owned_messages = actor
            .max_owned_messages()
            .checked_add(controller.max_progress_entries())?
            .checked_add(output.max_outputs())?
            .checked_add(controller.source_session_channel_slots())?
            .checked_add(target.max_write_directives())?;
        let max_owned_bytes = actor
            .max_owned_bytes()
            .checked_add(controller.replay_persistent_backing_bytes())?
            .checked_add(controller.receive_persistent_backing_bytes())?
            .checked_add(controller.quarantine_staging_bytes())?
            .checked_add(output.max_encoded_bytes())?
            .checked_add(target.max_buffered_bytes())?;
        Some(Self {
            actor,
            controller,
            max_owned_messages,
            max_owned_bytes,
        })
    }

    fn matches_actor(self, actor: &StartedQuinnLegActor, gate: &QuinnLegCompositionGate) -> bool {
        self.actor == actor.capacity_witness(gate)
            && self.controller.output_page().max_outputs() != 0
            && self.max_owned_messages != 0
            && self.max_owned_bytes != 0
    }
}

#[derive(Clone, Copy)]
enum OwnerPhysicalRestartCredit {
    NotInstalled,
}

struct OwnerPhysicalCurrentActorCustody {
    actor: StartedQuinnLegActor,
    phase: OwnerPhysicalCurrentActorPhase,
}

enum OwnerPhysicalCurrentActorPhase {
    Running,
    Joining {
        admitted_inventory: OwnerPhysicalCurrentOnlyInventory,
    },
    Joined {
        admitted_inventory: OwnerPhysicalCurrentOnlyInventory,
        exact_joined: QuinnLegActorJoined,
    },
    Failed {
        admitted_inventory: OwnerPhysicalCurrentOnlyInventory,
        exact_error: QuinnLegActorError,
    },
}

impl OwnerPhysicalCurrentActorPhase {
    fn observed_error(&self) -> OwnerPhysicalSessionRootError {
        match self {
            Self::Running => OwnerPhysicalSessionRootError::CompositionInvariant,
            Self::Joining { .. } => {
                OwnerPhysicalSessionRootError::Actor(QuinnLegActorError::Closing)
            }
            Self::Joined { .. } => {
                OwnerPhysicalSessionRootError::Actor(QuinnLegActorError::AlreadyJoined)
            }
            Self::Failed { exact_error, .. } => {
                let _retained_exact_error = exact_error;
                OwnerPhysicalSessionRootError::ActorJoinQuarantined
            }
        }
    }
}

struct OwnerPhysicalCurrentOnly<T: TargetIo> {
    actor: OwnerPhysicalCurrentActorCustody,
    handoff: OwnerTargetInitialHandoff,
    output: QuinnOwnerControllerOutputRoot<T>,
    capacity: OwnerPhysicalCurrentOnlyCapacity,
    restart_credit: OwnerPhysicalRestartCredit,
    ready: OwnerPhysicalReadyState,
    terminal_timer: OwnerTerminalTimerRegistration,
    output_cursor: usize,
    #[cfg(test)]
    panic_after_joining_before_direct_join_once: bool,
}

enum OwnerPhysicalReadyState {
    Unactivated,
    Active {
        output: std::task::Waker,
        publication: service::OwnerPhysicalPublication,
        flush: service::OwnerPhysicalFlush,
        failure: Option<service::OwnerPhysicalServiceFailure>,
    },
}

pub(super) struct OwnerPhysicalReadyActivationFailure {
    cause: OwnerPhysicalReadyActivationCause,
}

enum OwnerPhysicalReadyActivationCause {
    AlreadyActive,
    Physical(OwnerPhysicalSessionRootError),
    Controller(crate::owned_upstream::owner_session_controller::OwnerReadyActivationBlock),
}

pub(super) enum OwnerPhysicalSessionRoot<T: TargetIo> {
    CurrentOnly(OwnerPhysicalCurrentOnly<T>),
}

#[derive(Debug)]
enum OwnerPhysicalSessionRootError {
    Actor(QuinnLegActorError),
    ActorJoinQuarantined,
    Handoff(OwnerTargetInputBlock),
    OutboundResidual {
        queued_frames: usize,
        queued_bytes: usize,
        ordered_delivery_wait: bool,
    },
    CompositionInvariant,
}

#[derive(Clone, Copy)]
struct OwnerPhysicalCurrentOnlyInventory {
    current_actors: usize,
    retained_handoffs: usize,
    retained_controllers: usize,
    retained_executors: usize,
    retained_attached_authorities: usize,
    retained_acceptance_receipts: usize,
    actor_fifo_messages: usize,
    actor_fifo_bytes: usize,
    retained_timer_registrations: usize,
    pending_terminal_timers: usize,
    controller_output_is_exact_zero: bool,
    restart_credit_is_not_installed: bool,
}

impl OwnerPhysicalCurrentOnlyInventory {
    const fn current_actors(self) -> usize {
        self.current_actors
    }

    const fn retained_handoffs(self) -> usize {
        self.retained_handoffs
    }

    const fn retained_controllers(self) -> usize {
        self.retained_controllers
    }

    const fn retained_executors(self) -> usize {
        self.retained_executors
    }

    const fn retained_attached_authorities(self) -> usize {
        self.retained_attached_authorities
    }

    const fn retained_acceptance_receipts(self) -> usize {
        self.retained_acceptance_receipts
    }

    const fn actor_fifo_messages(self) -> usize {
        self.actor_fifo_messages
    }

    const fn actor_fifo_bytes(self) -> usize {
        self.actor_fifo_bytes
    }

    const fn controller_output_is_exact_zero(self) -> bool {
        self.controller_output_is_exact_zero
    }

    const fn restart_credit_is_not_installed(self) -> bool {
        self.restart_credit_is_not_installed
    }

    const fn is_whole_root_exact_zero(self) -> bool {
        self.current_actors == 0
            && self.retained_handoffs == 0
            && self.retained_controllers == 0
            && self.retained_executors == 0
            && self.retained_attached_authorities == 0
            && self.retained_acceptance_receipts == 0
            && self.actor_fifo_messages == 0
            && self.actor_fifo_bytes == 0
            && self.retained_timer_registrations == 0
            && self.pending_terminal_timers == 0
            && self.controller_output_is_exact_zero
            && !self.restart_credit_is_not_installed
    }
}

#[derive(Clone, Copy)]
struct OwnerPhysicalCurrentJoined {
    actor_owned_messages: usize,
    actor_owned_bytes: usize,
    retained_controllers: usize,
    retained_executors: usize,
    retained_handoffs: usize,
    retained_output_roots: usize,
    retained_timer_registrations: usize,
    pending_terminal_timers: usize,
}

impl OwnerPhysicalCurrentJoined {
    fn observe<T: TargetIo>(
        joined: &QuinnLegActorJoined,
        current: &OwnerPhysicalCurrentOnly<T>,
    ) -> Self {
        let _retained = (&current.handoff, &current.output);
        Self {
            actor_owned_messages: joined.owned_messages(),
            actor_owned_bytes: joined.owned_bytes(),
            retained_controllers: 1,
            retained_executors: 1,
            retained_handoffs: 1,
            retained_output_roots: 1,
            retained_timer_registrations: current.terminal_timer.inventory().0,
            pending_terminal_timers: current.terminal_timer.inventory().1,
        }
    }

    const fn actor_owned_messages(self) -> usize {
        self.actor_owned_messages
    }

    const fn actor_owned_bytes(self) -> usize {
        self.actor_owned_bytes
    }

    const fn retained_controllers(self) -> usize {
        self.retained_controllers
    }

    const fn retained_executors(self) -> usize {
        self.retained_executors
    }

    const fn retained_handoffs(self) -> usize {
        self.retained_handoffs
    }

    const fn retained_output_roots(self) -> usize {
        self.retained_output_roots
    }

    const fn is_whole_root_exact_zero(self) -> bool {
        self.actor_owned_messages == 0
            && self.actor_owned_bytes == 0
            && self.retained_controllers == 0
            && self.retained_executors == 0
            && self.retained_handoffs == 0
            && self.retained_output_roots == 0
            && self.retained_timer_registrations == 0
            && self.pending_terminal_timers == 0
    }
}

fn initial_handoff_is_settled(inventory: OwnerTargetInitialHandoffInventory) -> bool {
    inventory.attached_authorities() == 1
        && inventory.acceptance_receipts() == 0
        && inventory.executor_identity_live()
        && inventory.physical_actor_identities() == 1
        && inventory.physical_actor_identity_live()
        && !inventory.acceptance_queue_live()
        && inventory.delivery() == OwnerTargetInitialAcceptanceDelivery::Settled
}

fn exact_build_failure<T: TargetIo>(
    actor: StartedQuinnLegActor,
    handoff: OwnerTargetInitialHandoff,
    controller: OwnerSessionController<T>,
    block: OwnerPhysicalSessionRootBuildBlock,
) -> OwnerPhysicalSessionRootBuildFailure<T> {
    OwnerPhysicalSessionRootBuildFailure {
        seed: QuinnOwnerInitialPhysicalRootSeed::from_parts(actor, handoff, controller),
        block,
    }
}

impl<T: TargetIo> OwnerPhysicalSessionRootConstructor<T> {
    fn from_seed(seed: QuinnOwnerInitialPhysicalRootSeed<T>) -> Self {
        let (actor, handoff, controller) = seed.into_parts();
        Self {
            actor,
            handoff,
            controller,
        }
    }

    fn preflight(
        self,
    ) -> Result<OwnerPhysicalCurrentOnlyPrepared<T>, OwnerPhysicalSessionRootBuildFailure<T>> {
        let Self {
            mut actor,
            handoff,
            controller,
        } = self;
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(exact_build_failure(
                actor,
                handoff,
                controller,
                OwnerPhysicalSessionRootBuildBlock::RuntimeUnavailable,
            ));
        }
        let gate = QuinnLegCompositionGate::mint();
        let actor_clock = actor.session_clock(&gate);
        if !controller.shares_session_clock(&actor_clock, &gate) {
            return Err(exact_build_failure(
                actor,
                handoff,
                controller,
                OwnerPhysicalSessionRootBuildBlock::ClockMismatch,
            ));
        }
        if !super::controller_snapshot_is_live_exact_zero(controller.snapshot())
            || !matches!(controller.cold_inventory(), Ok(inventory) if inventory.is_exact_zero())
        {
            return Err(exact_build_failure(
                actor,
                handoff,
                controller,
                OwnerPhysicalSessionRootBuildBlock::ControllerNotExactZero,
            ));
        }
        let actor_capacity = actor.capacity_witness(&gate);
        let capacity = match OwnerPhysicalCurrentOnlyCapacity::checked(
            actor_capacity,
            controller.capacity_witness(),
        ) {
            Some(capacity) => capacity,
            None => {
                return Err(exact_build_failure(
                    actor,
                    handoff,
                    controller,
                    OwnerPhysicalSessionRootBuildBlock::CapacityOverflow,
                ));
            }
        };
        let handoff = match actor.with_physical_controller_parts_mut_input(
            &gate,
            handoff,
            |handoff, runtime, io, queue| {
                let inventory = match handoff.inventory(queue) {
                    Ok(inventory) => inventory,
                    Err(block) => {
                        return Err((handoff, OwnerPhysicalSessionRootBuildBlock::Handoff(block)));
                    }
                };
                if !initial_handoff_is_settled(inventory) {
                    return Err((
                        handoff,
                        OwnerPhysicalSessionRootBuildBlock::HandoffInventoryMismatch,
                    ));
                }
                match controller.settle_initial_acceptance_delivery(
                    handoff, runtime, io, queue, &gate,
                ) {
                    Ok(settlement)
                        if settlement.disposition()
                            == OwnerTargetInitialAcceptanceSettlementDisposition::AlreadySettled =>
                    {
                        Ok(settlement.into_handoff())
                    }
                    Ok(settlement) => Err((
                        settlement.into_handoff(),
                        OwnerPhysicalSessionRootBuildBlock::AcceptanceNotSettled,
                    )),
                    Err(failure) => {
                        let block = failure.block();
                        Err((
                            failure.into_handoff(),
                            OwnerPhysicalSessionRootBuildBlock::Handoff(block),
                        ))
                    }
                }
            },
        ) {
            Ok(Ok(handoff)) => handoff,
            Ok(Err((handoff, block))) => {
                return Err(exact_build_failure(actor, handoff, controller, block));
            }
            Err((handoff, _error)) => {
                return Err(exact_build_failure(
                    actor,
                    handoff,
                    controller,
                    OwnerPhysicalSessionRootBuildBlock::ActorUnavailable,
                ));
            }
        };
        let (messages, bytes) = match actor.shutdown_outbound_inventory(&gate) {
            Ok(inventory) => inventory,
            Err(_) => {
                return Err(exact_build_failure(
                    actor,
                    handoff,
                    controller,
                    OwnerPhysicalSessionRootBuildBlock::ActorUnavailable,
                ));
            }
        };
        if messages > actor_capacity.max_owned_messages()
            || bytes > actor_capacity.max_owned_bytes()
        {
            return Err(exact_build_failure(
                actor,
                handoff,
                controller,
                OwnerPhysicalSessionRootBuildBlock::ActorFifoExceedsCapacity { messages, bytes },
            ));
        }
        if std::alloc::Layout::new::<OwnerPhysicalCurrentOnly<T>>()
            .extend(std::alloc::Layout::new::<tokio::time::Sleep>())
            .is_err()
        {
            return Err(exact_build_failure(
                actor,
                handoff,
                controller,
                OwnerPhysicalSessionRootBuildBlock::CapacityOverflow,
            ));
        }
        let terminal_timer = match OwnerTerminalTimerRegistration::cold() {
            Ok(timer) => timer,
            Err(_) => {
                return Err(exact_build_failure(
                    actor,
                    handoff,
                    controller,
                    OwnerPhysicalSessionRootBuildBlock::CapacityOverflow,
                ));
            }
        };
        Ok(OwnerPhysicalCurrentOnlyPrepared {
            actor,
            handoff,
            controller,
            capacity,
            restart_credit: OwnerPhysicalRestartCredit::NotInstalled,
            terminal_timer,
        })
    }
}

impl<T: TargetIo> OwnerPhysicalSessionRoot<T> {
    fn from_initial_seed(
        seed: QuinnOwnerInitialPhysicalRootSeed<T>,
    ) -> Result<Self, OwnerPhysicalSessionRootBuildFailure<T>> {
        let prepared = OwnerPhysicalSessionRootConstructor::from_seed(seed).preflight()?;
        let OwnerPhysicalCurrentOnlyPrepared {
            actor,
            handoff,
            controller,
            capacity,
            restart_credit,
            terminal_timer,
        } = prepared;
        match QuinnOwnerControllerOutputRoot::new(controller) {
            Ok(output) => Ok(Self::CurrentOnly(OwnerPhysicalCurrentOnly {
                actor: OwnerPhysicalCurrentActorCustody {
                    actor,
                    phase: OwnerPhysicalCurrentActorPhase::Running,
                },
                handoff,
                output,
                capacity,
                restart_credit,
                ready: OwnerPhysicalReadyState::Unactivated,
                terminal_timer,
                output_cursor: 0,
                #[cfg(test)]
                panic_after_joining_before_direct_join_once: false,
            })),
            Err(failure) => {
                let (controller, source) = failure.into_parts();
                Err(exact_build_failure(
                    actor,
                    handoff,
                    controller,
                    OwnerPhysicalSessionRootBuildBlock::Output(source),
                ))
            }
        }
    }

    fn current_only_inventory(
        &mut self,
    ) -> Result<OwnerPhysicalCurrentOnlyInventory, OwnerPhysicalSessionRootError> {
        match self {
            Self::CurrentOnly(current) => current.inventory(),
        }
    }

    #[cfg(test)]
    fn arm_joining_pre_cancel_panic_for_test(&mut self) {
        match self {
            Self::CurrentOnly(current) => {
                current.panic_after_joining_before_direct_join_once = true;
            }
        }
    }

    async fn join_current_for_shutdown(
        &mut self,
    ) -> Result<OwnerPhysicalCurrentJoined, OwnerPhysicalSessionRootError> {
        match self {
            Self::CurrentOnly(current) => current.join_for_shutdown().await,
        }
    }
}

impl<T: TargetIo> OwnerPhysicalCurrentOnly<T> {
    fn live_handoff_inventory(
        &mut self,
        gate: &QuinnLegCompositionGate,
    ) -> Result<OwnerTargetInitialHandoffInventory, OwnerPhysicalSessionRootError> {
        if !matches!(self.actor.phase, OwnerPhysicalCurrentActorPhase::Running) {
            return Err(self.actor.phase.observed_error());
        }
        match self.actor.actor.with_physical_controller_parts_mut_input(
            gate,
            (),
            |(), _runtime, _io, queue| self.handoff.inventory(queue),
        ) {
            Ok(Ok(inventory)) if initial_handoff_is_settled(inventory) => Ok(inventory),
            Ok(Ok(_)) => Err(OwnerPhysicalSessionRootError::CompositionInvariant),
            Ok(Err(block)) => Err(OwnerPhysicalSessionRootError::Handoff(block)),
            Err(((), error)) => Err(OwnerPhysicalSessionRootError::Actor(error)),
        }
    }

    fn inventory(
        &mut self,
    ) -> Result<OwnerPhysicalCurrentOnlyInventory, OwnerPhysicalSessionRootError> {
        match &self.actor.phase {
            OwnerPhysicalCurrentActorPhase::Running => {}
            OwnerPhysicalCurrentActorPhase::Joining { admitted_inventory }
            | OwnerPhysicalCurrentActorPhase::Failed {
                admitted_inventory, ..
            } => return Ok(*admitted_inventory),
            OwnerPhysicalCurrentActorPhase::Joined { .. } => {
                return Err(OwnerPhysicalSessionRootError::Actor(
                    QuinnLegActorError::AlreadyJoined,
                ));
            }
        }
        let gate = QuinnLegCompositionGate::mint();
        let handoff = self.live_handoff_inventory(&gate)?;
        if !self.capacity.matches_actor(&self.actor.actor, &gate)
            || !self
                .output
                .shares_session_clock(&self.actor.actor.session_clock(&gate), &gate)
        {
            return Err(OwnerPhysicalSessionRootError::CompositionInvariant);
        }
        let (actor_fifo_messages, actor_fifo_bytes) = self
            .actor
            .actor
            .shutdown_outbound_inventory(&gate)
            .map_err(OwnerPhysicalSessionRootError::Actor)?;
        let output = self.output.output_inventory();
        Ok(OwnerPhysicalCurrentOnlyInventory {
            current_actors: 1,
            retained_handoffs: 1,
            retained_controllers: 1,
            retained_executors: usize::from(handoff.executor_identity_live()),
            retained_attached_authorities: handoff.attached_authorities(),
            retained_acceptance_receipts: handoff.acceptance_receipts(),
            actor_fifo_messages,
            actor_fifo_bytes,
            retained_timer_registrations: self.terminal_timer.inventory().0,
            pending_terminal_timers: self.terminal_timer.inventory().1,
            controller_output_is_exact_zero: output.is_controller_output_terminal_exact_zero(),
            restart_credit_is_not_installed: matches!(
                self.restart_credit,
                OwnerPhysicalRestartCredit::NotInstalled
            ),
        })
    }

    fn start_join_for_shutdown(&mut self) -> Result<(), OwnerPhysicalSessionRootError> {
        let needs_admission = match &self.actor.phase {
            OwnerPhysicalCurrentActorPhase::Running => true,
            OwnerPhysicalCurrentActorPhase::Joining { .. } => false,
            OwnerPhysicalCurrentActorPhase::Joined { .. } => return Ok(()),
            OwnerPhysicalCurrentActorPhase::Failed { .. } => {
                return Err(self.actor.phase.observed_error());
            }
        };
        if needs_admission {
            let gate = QuinnLegCompositionGate::mint();
            let handoff = self.live_handoff_inventory(&gate)?;
            if handoff.acceptance_receipts() != 0 {
                return Err(OwnerPhysicalSessionRootError::CompositionInvariant);
            }
            match self
                .actor
                .actor
                .try_retire_delivered_wait_for_shutdown(&gate)
                .map_err(OwnerPhysicalSessionRootError::Actor)?
            {
                QuinnLegActorShutdownWaitDrive::Clean
                | QuinnLegActorShutdownWaitDrive::RetiredDelivered => {}
                QuinnLegActorShutdownWaitDrive::Pending => {
                    return Err(OwnerPhysicalSessionRootError::OutboundResidual {
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
                    return Err(OwnerPhysicalSessionRootError::OutboundResidual {
                        queued_frames,
                        queued_bytes,
                        ordered_delivery_wait,
                    });
                }
            }
            let (messages, bytes) = self
                .actor
                .actor
                .shutdown_outbound_inventory(&gate)
                .map_err(OwnerPhysicalSessionRootError::Actor)?;
            if messages != 0 || bytes != 0 {
                return Err(OwnerPhysicalSessionRootError::OutboundResidual {
                    queued_frames: messages,
                    queued_bytes: bytes,
                    ordered_delivery_wait: false,
                });
            }
            let admitted_inventory = self.inventory()?;
            self.actor.phase = OwnerPhysicalCurrentActorPhase::Joining { admitted_inventory };
            #[cfg(test)]
            if std::mem::take(&mut self.panic_after_joining_before_direct_join_once) {
                panic!("test-only Joining/pre-cancel direct-join failpoint");
            }
        }
        self.actor.actor.request_cancel();
        Ok(())
    }

    fn joined_observation(
        &self,
    ) -> Result<OwnerPhysicalCurrentJoined, OwnerPhysicalSessionRootError> {
        match &self.actor.phase {
            OwnerPhysicalCurrentActorPhase::Joined {
                admitted_inventory,
                exact_joined,
            } => {
                let _retained_admitted_inventory = admitted_inventory;
                Ok(OwnerPhysicalCurrentJoined::observe(exact_joined, self))
            }
            OwnerPhysicalCurrentActorPhase::Failed { .. } => Err(self.actor.phase.observed_error()),
            _ => Err(OwnerPhysicalSessionRootError::CompositionInvariant),
        }
    }

    async fn join_for_shutdown(
        &mut self,
    ) -> Result<OwnerPhysicalCurrentJoined, OwnerPhysicalSessionRootError> {
        self.start_join_for_shutdown()?;
        let admitted_inventory = match &self.actor.phase {
            OwnerPhysicalCurrentActorPhase::Joining { admitted_inventory } => *admitted_inventory,
            OwnerPhysicalCurrentActorPhase::Joined { .. }
            | OwnerPhysicalCurrentActorPhase::Failed { .. } => {
                return self.joined_observation();
            }
            OwnerPhysicalCurrentActorPhase::Running => {
                return Err(OwnerPhysicalSessionRootError::CompositionInvariant);
            }
        };
        let exact_result = self.actor.actor.join().await;
        self.actor.phase = match exact_result {
            Ok(exact_joined) => OwnerPhysicalCurrentActorPhase::Joined {
                admitted_inventory,
                exact_joined,
            },
            Err(exact_error) => OwnerPhysicalCurrentActorPhase::Failed {
                admitted_inventory,
                exact_error,
            },
        };
        self.joined_observation()
    }
}

#[cfg(test)]
mod tests;

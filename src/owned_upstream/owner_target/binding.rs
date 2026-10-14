//! Checked source identity and single-pair installation at Executor authority.
//!
//! Owns the cold preparation interval over this original Supervisor/factory;
//! it never remints sources, consumes replay, polls Target, or exports origins.
//! Existing live/Legless/recovery state is admissible when its actual source
//! correlation is healthy. The caller's existing capacity derivation stays first.
//! Identity success is used inside installation, not returned as a reusable seal.
//! Entry will be the consuming install_owner_ready operation; its tests belong
//! in commands/tests/bound.rs and the real CurrentOnly Bound tracer.

use super::commands::SessionCommands;
use super::service::{
    OwnerFlowServiceBacking, OwnerFlowServicePosition, OwnerFlowServiceSlot,
    OwnerTargetServiceState,
};
use super::{
    OwnerReadyInstallError, OwnerTargetColdPreparationGate, OwnerTargetExecutor, OwnerTargetWork,
    PendingTargetCompletion, PendingTargetSink,
};
use crate::owned_upstream::owner_ready::{
    OwnerReadyFlowAssociation, OwnerReadyFlowImport, OwnerReadyNoSourcePhase,
    OwnerReadyReservationError, OwnerReadySessionReservation,
};
use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
use crate::owned_upstream::session::SessionOwnerInbox;
use crate::owned_upstream::supervisor::OwnerSourceIdentityError;
use std::alloc::Layout;

// Cold scratch indexes actual queued terminal effects, not a copied lifecycle
// capability. One bounded effect walk marks positions in the original Flow order.
struct RetainedFlowEffect {
    flow_id: crate::resumable::SessionFlowId,
    finish_pending: bool,
}

impl<T> OwnerTargetExecutor<T> {
    /// Sole checked-constructor installation. The plan is configuration, not
    /// a detached capacity certificate; C derives and retains its full witness
    /// against this same Executor before invoking this consuming operation.
    pub(in crate::owned_upstream) fn install_owner_ready<'r>(
        mut self,
        ready: OwnerReadySessionReservation<'r>,
        plan: OwnerSessionCapacityPlan,
    ) -> Result<
        (Self, SessionOwnerInbox),
        (
            Self,
            OwnerReadySessionReservation<'r>,
            OwnerReadyInstallError,
        ),
    > {
        macro_rules! reject {
            ($cause:expr) => {
                return Err((self, ready, $cause))
            };
        }
        if !matches!(self.service, OwnerTargetServiceState::Unbound) {
            reject!(OwnerReadyInstallError::AlreadyBound);
        }
        if let Err(cause) = self.validate_ready_source(&ready) {
            reject!(OwnerReadyInstallError::Source(cause));
        }
        let max_flows = plan.session_config().max_flows();
        if self.flows.len() > max_flows {
            reject!(OwnerReadyInstallError::Reservation(
                OwnerReadyReservationError::DeclaredLimitsMismatch
            ));
        }
        if Layout::array::<OwnerFlowServiceSlot>(max_flows).is_err()
            || Layout::array::<OwnerReadyFlowImport>(max_flows).is_err()
            || Layout::array::<RetainedFlowEffect>(max_flows).is_err()
        {
            reject!(OwnerReadyInstallError::Layout);
        }
        let mut slots = Vec::new();
        let mut imports = Vec::new();
        let mut effects = Vec::new();
        if slots.try_reserve_exact(max_flows).is_err()
            || imports.try_reserve_exact(self.flows.len()).is_err()
            || effects.try_reserve_exact(self.flows.len()).is_err()
        {
            reject!(OwnerReadyInstallError::Reserve);
        }
        slots.resize_with(max_flows, || OwnerFlowServiceSlot::Free);
        effects.extend(self.flows.keys().map(|&flow_id| RetainedFlowEffect {
            flow_id,
            finish_pending: false,
        }));
        for work in self.pending_effects.iter().chain(
            self.aborted_effects
                .iter()
                .flat_map(|aborted| aborted.effects.iter()),
        ) {
            if let OwnerTargetWork::Effect(crate::resumable::SessionEffect::FlowFinished {
                flow_id,
                ..
            }) = work
            {
                if let Ok(position) = effects.binary_search_by_key(flow_id, |effect| effect.flow_id)
                {
                    effects[position].finish_pending = true;
                }
            }
        }
        for ((&flow_id, flow), effect) in self.flows.iter().zip(&effects) {
            if flow.service_position.is_some() {
                reject!(OwnerReadyInstallError::FlowState(flow_id));
            }
            let association = match &flow.source {
                Some(source) => {
                    let local = source.flow.local_flow();
                    if local != source.driver.local_flow() || local.flow_id() != flow_id {
                        reject!(OwnerReadyInstallError::FlowState(flow_id));
                    }
                    if let Err(cause) = ready.compare_declared_session(local.session_id()) {
                        reject!(OwnerReadyInstallError::Reservation(cause));
                    }
                    OwnerReadyFlowAssociation::Source(local)
                }
                None => {
                    let phase = if flow
                        .pending_completion
                        .as_ref()
                        .is_some_and(PendingTargetCompletion::is_open_pending)
                    {
                        OwnerReadyNoSourcePhase::OpenCompletion
                    } else if flow
                        .pending_completion
                        .as_ref()
                        .is_some_and(PendingTargetCompletion::is_open_finish_pending)
                    {
                        if !effect.finish_pending {
                            reject!(OwnerReadyInstallError::FlowState(flow_id));
                        }
                        OwnerReadyNoSourcePhase::PendingFinishEffect
                    } else if matches!(
                        flow.pending_completion,
                        Some(PendingTargetCompletion::Open(_))
                    ) {
                        reject!(OwnerReadyInstallError::FlowState(flow_id));
                    } else if flow.terminal.is_some() {
                        OwnerReadyNoSourcePhase::TargetTerminal
                    } else if flow.pending_completion.is_some() {
                        OwnerReadyNoSourcePhase::RetainedCompletion
                    } else if flow.join_pending || flow.retire_after_join {
                        OwnerReadyNoSourcePhase::Retirement
                    } else if effect.finish_pending {
                        OwnerReadyNoSourcePhase::PendingFinishEffect
                    } else {
                        reject!(OwnerReadyInstallError::FlowState(flow_id));
                    };
                    OwnerReadyFlowAssociation::NoSource { flow_id, phase }
                }
            };
            imports.push(OwnerReadyFlowImport {
                association,
                target_present: flow.target_present,
                read_interest: flow.target_present
                    && flow.source.is_some()
                    && flow.terminal.is_none()
                    && !flow.retire_after_join,
                write_interest: matches!(flow.pending_sink, Some(PendingTargetSink::Write(_))),
                join_interest: flow.join_pending,
                completion: flow.pending_completion.is_some(),
                pending_open: flow
                    .pending_completion
                    .as_ref()
                    .is_some_and(PendingTargetCompletion::is_open_pending),
                retiring: flow.retire_after_join,
            });
        }
        let prepared = match ready.prepare_flow_backing(max_flows, &imports) {
            Ok(prepared) => prepared,
            Err(cause) => reject!(OwnerReadyInstallError::Reservation(cause)),
        };
        let [control, ordered] = prepared.mailbox_capacities;
        let pair = match SessionCommands::prepare_pair(control, ordered) {
            Ok(pair) => pair,
            Err(cause) => reject!(OwnerReadyInstallError::Mailbox(cause)),
        };
        let installed = match ready.bind_prepared(prepared) {
            Ok(installed) => installed,
            Err((ready, _prepared, cause)) => {
                return Err((self, ready, OwnerReadyInstallError::Reservation(cause)));
            }
        };
        // No validation/allocation/callback after binding. Both traversals use
        // the unchanged original BTreeMap order, and the closed preparation
        // created exactly one ticket owner for each imported occupied position.
        for (position, (((_, flow), slot), (import, tickets))) in self
            .flows
            .iter_mut()
            .zip(slots.iter_mut())
            .zip(imports.into_iter().zip(installed.tickets))
            .enumerate()
        {
            flow.service_position = Some(position);
            let record = OwnerFlowServicePosition {
                association: import.association,
                generation: 1,
                transit: 0,
                tickets,
                wait: None,
            };
            *slot = if import.retiring {
                OwnerFlowServiceSlot::Retiring(record)
            } else {
                OwnerFlowServiceSlot::Live(record)
            };
        }
        self.service = OwnerTargetServiceState::Bound {
            commands: pair.commands,
            flows: OwnerFlowServiceBacking {
                slots,
                session: installed.session,
                waits: Default::default(),
            },
            binding: installed.binding,
        };
        Ok((self, pair.inbox))
    }

    fn validate_ready_source(
        &self,
        ready: &OwnerReadySessionReservation<'_>,
    ) -> Result<(), OwnerSourceIdentityError> {
        let gate = OwnerTargetColdPreparationGate(());
        self.supervisor
            .validate_owner_source_factory(&gate, ready, &self.source_factory)
    }
}

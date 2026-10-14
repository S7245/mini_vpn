//! Owner Target read transaction inside the existing Executor Authority.
//!
//! Owns reservation, DATA/EOF source dequeue and observed reducer settlement.
//! Storage stays in the parent; failure/RESET and completion live in sibling completion.
//! Reserve before Target read; consume the same Driver input exactly once.
//! Entries are try_read_target and try_read_target_observed; characterization
//! remains in the parent test tree and owner_target/tests/r7_owner_ack.rs.

use super::{
    DriverInput, OwnerTargetError, OwnerTargetExecutor, OwnerTargetObservedApply,
    OwnerTargetOutput, OwnerTargetReducerObservations, OwnerTargetWork, SessionEvent,
    SessionFlowId, SessionOwnerCommand, TargetIo, TargetIoOperation, TargetSourceFact,
};
use crate::owned_upstream::target::TargetReadCompletion;
use std::collections::VecDeque;

pub(super) enum BoundTargetRead {
    Data,
    Eof,
    WouldBlock,
    CreditBlocked,
    FailureStaged,
}

impl<T: TargetIo> OwnerTargetExecutor<T> {
    /// The original Bound owner reserves before the single real read. It never
    /// dequeues its Driver or runs the reducer; later charged sources do that.
    pub(super) fn read_ready_target(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<BoundTargetRead, OwnerTargetError> {
        use super::FlowPortError;
        if !matches!(
            self.service,
            super::service::OwnerTargetServiceState::Bound { .. }
        ) {
            return Err(OwnerTargetError::BoundSourceRequiresSelectedService);
        }
        self.require_no_pending_effects()?;
        self.require_no_pending_target_completion(flow_id)?;
        let reservation = match self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .source
            .as_ref()
            .ok_or(OwnerTargetError::MissingTargetReadSource { flow_id })?
            .flow
            .try_reserve_uplink_source(self.config.max_target_read_bytes)
        {
            Ok(reservation) => reservation,
            Err(
                FlowPortError::UplinkMessageLaneFull
                | FlowPortError::UplinkByteBudgetExhausted { .. }
                | FlowPortError::UplinkGlobalByteBudgetExhausted { .. }
                | FlowPortError::UplinkSegmentBudgetExhausted { .. }
                | FlowPortError::UplinkGlobalSegmentBudgetExhausted { .. },
            ) => return Ok(BoundTargetRead::CreditBlocked),
            Err(error) => return Err(OwnerTargetError::FlowPort(error)),
        };
        self.prepare_effect_entry(3)?;
        let read = match self.target.read(flow_id, self.config.max_target_read_bytes) {
            Ok(read) => read,
            Err(error) => {
                drop(reservation);
                return self.abort_direct_target_io(flow_id, TargetIoOperation::Read, error);
            }
        };
        let outcome = match read {
            TargetReadCompletion::Data(bytes) => {
                if let Err(error) = reservation.commit(bytes) {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::ReadCompletion,
                        OwnerTargetError::FlowPort(error),
                    );
                }
                BoundTargetRead::Data
            }
            TargetReadCompletion::Eof => {
                if let Err(error) = reservation.commit_eof() {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::ReadCompletion,
                        OwnerTargetError::FlowPort(error),
                    );
                }
                BoundTargetRead::Eof
            }
            TargetReadCompletion::WouldBlock => {
                drop(reservation);
                return Ok(BoundTargetRead::WouldBlock);
            }
            TargetReadCompletion::Failed(cause) => {
                if let Err((_, error)) = self.retain_bound_target_failure(
                    flow_id,
                    cause,
                    OwnerTargetWork::TargetIoInvariant {
                        flow_id,
                        operation: TargetIoOperation::Read,
                    },
                ) {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::FailureCompletion,
                        error,
                    );
                }
                // Failed work/cause already belong to the original Flow when
                // reservation release publishes progress and may unwind.
                drop(reservation);
                return match self.stage_retained_failure_reset(flow_id) {
                    Ok(()) => Ok(BoundTargetRead::FailureStaged),
                    Err(error) => self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::FailureCompletion,
                        error,
                    ),
                };
            }
        };
        // The original source FIFO owns committed DATA/CLOSE before this
        // controlled publisher can observe it. It does not release permits.
        self.request_bound_driver(
            flow_id,
            crate::owned_upstream::owner_ready::SendLane::OrderedSource,
        )?;
        Ok(outcome)
    }

    /// Reserves the source FIFO slot and maximum byte permits before touching
    /// `TargetIo`.  A positive read shrinks that reservation to the exact
    /// bytes and reaches the reducer only through the non-cloneable driver
    /// command. EOF releases the read reservation before entering the same
    /// ordered source lane as DATA.
    pub(crate) fn try_read_target(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        self.try_read_target_observed(flow_id)
            .map(OwnerTargetObservedApply::into_outputs)
    }

    pub(crate) fn try_read_target_observed(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<OwnerTargetObservedApply, OwnerTargetError> {
        if matches!(
            self.service,
            super::service::OwnerTargetServiceState::Bound { .. }
        ) {
            return Err(OwnerTargetError::BoundSourceRequiresSelectedService);
        }
        self.require_no_pending_effects()?;
        self.require_no_pending_target_completion(flow_id)?;
        let reservation = self
            .flows
            .get(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .source
            .as_ref()
            .ok_or(OwnerTargetError::MissingTargetReadSource { flow_id })?
            .flow
            .try_reserve_uplink_source(self.config.max_target_read_bytes)?;
        self.prepare_effect_entry(3)?;
        let read = match self.target.read(flow_id, self.config.max_target_read_bytes) {
            Ok(read) => read,
            Err(error) => {
                drop(reservation);
                return self.abort_direct_target_io(flow_id, TargetIoOperation::Read, error);
            }
        };
        let source_fact = match read {
            TargetReadCompletion::Data(bytes) => {
                if let Err(error) = reservation.commit(bytes) {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::ReadCompletion,
                        OwnerTargetError::FlowPort(error),
                    );
                }
                TargetSourceFact::Data
            }
            TargetReadCompletion::Eof => {
                if let Err(error) = reservation.commit_eof() {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::ReadCompletion,
                        OwnerTargetError::FlowPort(error),
                    );
                }
                TargetSourceFact::Eof
            }
            TargetReadCompletion::WouldBlock => {
                drop(reservation);
                return Ok(OwnerTargetObservedApply {
                    outputs: Vec::new(),
                    observations: OwnerTargetReducerObservations::default(),
                });
            }
            TargetReadCompletion::Failed(cause) => {
                drop(reservation);
                let mut effects = VecDeque::new();
                let mut outputs = Vec::new();
                let mut observations = OwnerTargetReducerObservations::default();
                if let Err(error) = self.apply_target_failure(
                    flow_id,
                    (
                        cause,
                        OwnerTargetWork::TargetIoInvariant {
                            flow_id,
                            operation: TargetIoOperation::FailureCompletion,
                        },
                    ),
                    &mut effects,
                    &mut outputs,
                    &mut observations,
                ) {
                    return self.retain_observations_on_failure(Err(error), observations);
                }
                return self.execute_queue_observed_page(effects, outputs, observations);
            }
        };
        let input_result = match self.flows.get_mut(&flow_id) {
            Some(flow) => match flow.source.as_mut() {
                Some(source) => source.driver.try_recv_next(),
                None => {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::ReadCompletion,
                        OwnerTargetError::MissingTargetReadSource { flow_id },
                    );
                }
            },
            None => {
                return self.abort_direct_owner_error(
                    flow_id,
                    TargetIoOperation::ReadCompletion,
                    OwnerTargetError::UnknownTargetFlow { flow_id },
                );
            }
        };
        let input = match input_result {
            Ok(Some(input)) => input,
            Ok(None) => {
                return self.abort_direct_owner_error(
                    flow_id,
                    TargetIoOperation::ReadCompletion,
                    OwnerTargetError::MissingTargetReadDriverInput { flow_id },
                );
            }
            Err(error) => {
                return self.abort_direct_owner_error(
                    flow_id,
                    TargetIoOperation::ReadCompletion,
                    OwnerTargetError::FlowPort(error),
                );
            }
        };
        let exact_source_fact = match (&input, source_fact) {
            (DriverInput::Data(data), TargetSourceFact::Data) => data.flow().flow_id() == flow_id,
            (DriverInput::Control(SessionEvent::LocalClose { flow }), TargetSourceFact::Eof) => {
                flow.flow_id() == flow_id
            }
            _ => false,
        };
        if !exact_source_fact {
            return self.abort_direct_owner_error(
                flow_id,
                TargetIoOperation::ReadCompletion,
                OwnerTargetError::UnexpectedTargetReadDriverInput { flow_id },
            );
        }
        let (effects, observations) =
            match self.reduce_owner_command_observed(SessionOwnerCommand::Driver(input)) {
                Ok(observed) => observed,
                Err(error) => {
                    return self.abort_direct_owner_error(
                        flow_id,
                        TargetIoOperation::ReadCompletion,
                        OwnerTargetError::Supervisor(error),
                    );
                }
            };
        self.execute_effects_observed(effects, observations)
    }
}

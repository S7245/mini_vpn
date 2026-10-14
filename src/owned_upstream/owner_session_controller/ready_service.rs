//! Checked Controller admission and selected ready-service ownership.
//!
//! Owns phase/receipt/continuation eligibility at the original Controller;
//! Executor retains business resources and the actual ready binding. No inbox,
//! receipt or mutable Controller is exported. Activation rejection preserves
//! every field; the inward commit is the final fallible operation.
//! Entry: activate_owner_ready; tests belong to tests/ready_service.rs and the
//! real CurrentOnly Bound tracer. Legacy capacity cannot call this operation.

use super::*;
use crate::owned_upstream::owner_ready::{
    OwnerPublicationAdmissionFailure, OwnerPublicationTurn, OwnerReadyContinuationSelection,
    OwnerReadyReadResult, SendLane, SessionCommandVisit,
};
use crate::owned_upstream::owner_ready::{OwnerReadyActivatedSession, ReadyIndex};
use crate::owned_upstream::session::SessionMailboxLane;
use std::task::{Context, Poll};

impl<T: TargetIo> OwnerSessionControllerCore<T, CheckedOwnerControllerCapacity> {
    fn require_ready_source(&self) -> Result<(), OwnerSessionControllerError> {
        self.require_active()?;
        self.require_no_recovery()?;
        if self.pending_continuation.is_some() {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::ContinueRequired,
            ));
        }
        if !matches!(self.capacity.receipt, OwnerReadyReceipt::Vacant) {
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::ReadyReceipt(
                    OwnerTargetInputBlock::ReadyReceiptInvariant,
                ),
            ));
        }
        Ok(())
    }

    pub(in crate::owned_upstream) fn begin_ready_publication(
        &self,
    ) -> Result<OwnerPublicationTurn, OwnerPublicationAdmissionFailure> {
        self.executor.begin_ready_publication()
    }

    pub(in crate::owned_upstream) fn select_ready_continuation(
        &self,
        eligible: usize,
        requested: usize,
    ) -> Result<Option<OwnerReadyContinuationSelection>, OwnerSessionControllerError> {
        self.executor
            .select_ready_continuation(eligible, requested)
            .map_err(|block| {
                OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::ReadyReceipt(
                    block,
                ))
            })
    }

    pub(in crate::owned_upstream) fn finish_ready_continuation(
        &mut self,
        selected: &OwnerReadyContinuationSelection,
        result: OwnerReadyReadResult,
        eligible: usize,
    ) -> Result<usize, OwnerSessionControllerError> {
        self.executor
            .finish_ready_continuation(selected, result, eligible)
            .map_err(|block| {
                OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::ReadyReceipt(
                    block,
                ))
            })
    }

    pub(in crate::owned_upstream) fn service_ready_send(
        &mut self,
        lane: SendLane,
    ) -> Result<SessionCommandVisit, OwnerSessionControllerError> {
        self.require_ready_source()?;
        self.executor.visit_ready_send(lane).map_err(|block| {
            OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::ReadyReceipt(block))
        })
    }

    pub(in crate::owned_upstream) fn service_ready_target(
        &mut self,
        selected: &OwnerReadyContinuationSelection,
    ) -> Result<OwnerReadyReadResult, OwnerSessionControllerError> {
        self.require_ready_source()?;
        let now = self.begin_terminal_grace_turn().map_err(|source| {
            OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Clock(source))
        })?;
        match self.executor.service_ready_target(selected) {
            Ok(result) => {
                self.finish_terminal_grace_turn();
                Ok(result)
            }
            Err(error) => Err(self.finish_target_error(error, now, OwnerContinuationOrigin::Local)),
        }
    }

    pub(in crate::owned_upstream) fn apply_ready_completion(
        &mut self,
        selected: &OwnerReadyContinuationSelection,
    ) -> Result<(OwnerSessionDrive, OwnerReadyReadResult), OwnerSessionControllerError> {
        self.require_ready_source()?;
        let now = self.begin_terminal_grace_turn().map_err(|source| {
            OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Clock(source))
        })?;
        match self.executor.apply_ready_completion_observed(selected) {
            Ok((observed, result)) => self
                .finish_observed_page(observed, now, OwnerContinuationOrigin::Local)
                .map(|drive| (drive, result)),
            Err(error) => Err(self.finish_target_error(error, now, OwnerContinuationOrigin::Local)),
        }
    }

    /// Used only inside the complete output ordinary transaction. The paired
    /// inbox owns prefetch across its callback; C owns every returned receipt
    /// before identity validation or sampling its original session clock.
    pub(in crate::owned_upstream) fn poll_ready_receipt(
        &mut self,
        lane: SessionMailboxLane,
    ) -> Poll<Result<Option<SimTime>, OwnerSessionControllerError>> {
        let prepared = (|| {
            self.require_active()?;
            self.require_no_recovery()?;
            if self.pending_continuation.is_some() {
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::ContinueRequired,
                ));
            }
            if !matches!(self.capacity.receipt, OwnerReadyReceipt::Vacant) {
                return Err(OwnerSessionControllerError::new(
                    OwnerSessionControllerErrorKind::ReadyReceipt(
                        OwnerTargetInputBlock::ReadyReceiptInvariant,
                    ),
                ));
            }
            self.executor.ready_inbox_waker(lane).map_err(|block| {
                OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::ReadyReceipt(
                    block,
                ))
            })
        })();
        let waker = match prepared {
            Ok(waker) => waker,
            Err(error) => return Poll::Ready(Err(error)),
        };
        let mut context = Context::from_waker(&waker);
        let command = match self.capacity.inbox.poll_selected(lane, &mut context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(Ok(None)),
            Poll::Ready(Some(command)) => command,
        };
        self.capacity.receipt = OwnerReadyReceipt::Held(command);
        let OwnerReadyReceipt::Held(command) = &self.capacity.receipt else {
            return Poll::Ready(Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::ReadyReceipt(
                    OwnerTargetInputBlock::ReadyReceiptInvariant,
                ),
            )));
        };
        let validated = self
            .executor
            .preflight_ready_receipt(command)
            .map_err(|block| {
                OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::ReadyReceipt(
                    block,
                ))
            });
        if let Err(error) = validated {
            return Poll::Ready(Err(error));
        }
        Poll::Ready(
            self.begin_terminal_grace_turn()
                .map(Some)
                .map_err(|source| {
                    OwnerSessionControllerError::new(OwnerSessionControllerErrorKind::Clock(source))
                }),
        )
    }

    /// Caller has entered its existing output ordinary guard. Unknown is
    /// sticky on unwind; it never permits another receive. Returned effects,
    /// observations and errors follow the original C/output owning ledgers.
    pub(in crate::owned_upstream) fn apply_ready_receipt(
        &mut self,
        observed_at: SimTime,
    ) -> Result<OwnerSessionDrive, OwnerSessionControllerError> {
        let receipt = std::mem::replace(&mut self.capacity.receipt, OwnerReadyReceipt::Unknown);
        let OwnerReadyReceipt::Held(command) = receipt else {
            self.capacity.receipt = receipt;
            self.phase = OwnerSessionControllerPhase::Failed;
            return Err(OwnerSessionControllerError::new(
                OwnerSessionControllerErrorKind::ReadyReceipt(
                    OwnerTargetInputBlock::ReadyReceiptInvariant,
                ),
            ));
        };
        let result = match self.executor.apply_ready_receipt_observed(command) {
            Ok(observed) => {
                self.finish_observed_page(observed, observed_at, OwnerContinuationOrigin::Local)
            }
            Err(error) => {
                Err(self.finish_target_error(error, observed_at, OwnerContinuationOrigin::Local))
            }
        };
        // Every modeled return has moved actual custody to its original
        // destination. The outer guard must retain result before publication.
        self.capacity.receipt = OwnerReadyReceipt::Vacant;
        result
    }

    pub(in crate::owned_upstream) fn activate_owner_ready(
        &mut self,
        ready: &mut ReadyIndex,
        position: usize,
        handoff: &OwnerTargetInitialHandoff,
        runtime: &PhysicalLegRuntimeIncarnation,
        io: &LegIo,
        queue: &LegOutboundQueue,
    ) -> Result<OwnerReadyActivatedSession, OwnerReadyActivationBlock> {
        use OwnerReadyActivationBlock as Block;
        if self.phase != OwnerSessionControllerPhase::Active || self.aborted_turn.is_some() {
            return Err(Block::ControllerUnavailable);
        }
        if self.pending_continuation.is_some() {
            return Err(Block::PendingContinuation);
        }
        if self.recovery.is_some()
            || self.legless_high_water.is_some()
            || self.terminal_quarantine.is_some()
            || self.attach_status.is_some()
            || self.aborted_recovery.is_some()
        {
            return Err(Block::RecoveryRetained);
        }
        if !matches!(self.capacity.receipt, OwnerReadyReceipt::Vacant) {
            return Err(Block::ReceiptRetained);
        }
        self.executor
            .activate_owner_ready(ready, position, handoff, runtime, io, queue)
            .map_err(Block::Target)
    }
}

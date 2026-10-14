//! Original committed receipt batch and observation custody in Supervisor.
//!
//! The one inline journal moves the original Vec; in-place filtering preserves
//! current/unprocessed effects across error or unwind. Poison forbids replay of
//! partially settled receipts. The original replay-candidate allocations remain.
//! Snapshot/ACK derivation/catch-up cleanup finish before the sole success take.
//! Owner DATA and event callers install original facts before the final take.
//! Entry: consume_after_model_commit_observed; real Bound C tests live in its
//! existing terminal_grace test leaf. No timer or physical-zero claim.

use super::*;

use crate::resumable::effect_storage::SessionEffectBatch;

pub(super) enum OrdinaryEffectStorage {
    Uninstalled,
    Idle(SessionEffectBatch),
    Entered,
    Transferred,
}
impl OrdinaryEffectStorage {
    pub(super) fn is_entered_or_transferred(&self) -> bool {
        matches!(self, Self::Entered | Self::Transferred)
    }
}

impl SessionSupervisor {
    /// Only the checked Executor calls this after its sole factory mint.
    pub(in crate::owned_upstream) fn install_ordinary_effect_storage(
        &mut self,
        effects: SessionEffectBatch,
    ) {
        self.ordinary_effects = OrdinaryEffectStorage::Idle(effects);
    }

    /// Complete normal transfer returns the same empty backing before another
    /// callback. Borrow keeps the batch at its caller's entered slot on error.
    pub(in crate::owned_upstream) fn reclaim_effect_batch(
        &mut self,
        effects: &mut SessionEffectBatch,
    ) -> Result<(), SessionSupervisorError> {
        if !effects.is_empty() {
            return Err(SessionSupervisorError::PoisonedOwnershipInvariant);
        }
        match self.ordinary_effects {
            OrdinaryEffectStorage::Transferred if effects.capacity() == 3 && !self.poisoned => {
                effects
                    .rearm_after_transfer()
                    .map_err(|source| SessionSupervisorError::Reducer(source.into()))?;
                self.ordinary_effects = OrdinaryEffectStorage::Idle(std::mem::replace(
                    effects,
                    SessionEffectBatch::empty(),
                ));
            }
            OrdinaryEffectStorage::Uninstalled | OrdinaryEffectStorage::Idle(_)
                if !self.poisoned =>
            {
                // Recovery owns a separate original allocation; ordinary3 stays idle.
                *effects = SessionEffectBatch::empty();
            }
            _ => return Err(SessionSupervisorError::PoisonedOwnershipInvariant),
        }
        Ok(())
    }

    pub(super) fn reduce_into_receipt_journal(
        &mut self,
        event: SessionEvent,
    ) -> Result<Result<(), SessionError>, SessionSupervisorError> {
        if matches!(self.ordinary_effects, OrdinaryEffectStorage::Uninstalled) {
            let effects = match self.model.reduce(event) {
                Ok(effects) => effects,
                Err(error) => return Ok(Err(error)),
            };
            self.receipt_journal = Some(SupervisorReceiptJournal {
                effects: SessionEffectBatch::from_vec(effects),
                observations: ReducerOwnerObservations::default(),
                sink_accepted_frontier: None,
            });
            self.poisoned = true;
            return Ok(Ok(()));
        }
        if !matches!(self.ordinary_effects, OrdinaryEffectStorage::Idle(_))
            || self.receipt_journal.is_some()
            || self.poisoned
        {
            return Err(SessionSupervisorError::PoisonedOwnershipInvariant);
        }
        let effects =
            match std::mem::replace(&mut self.ordinary_effects, OrdinaryEffectStorage::Entered) {
                OrdinaryEffectStorage::Idle(effects) => effects,
                other => {
                    self.ordinary_effects = other;
                    return Err(SessionSupervisorError::PoisonedOwnershipInvariant);
                }
            };
        self.receipt_journal = Some(SupervisorReceiptJournal {
            effects,
            observations: ReducerOwnerObservations::default(),
            sink_accepted_frontier: None,
        });
        self.poisoned = true;
        // Concrete backing is journal-owned BEFORE reducer mutation/unwind.
        let journal = self
            .receipt_journal
            .as_mut()
            .ok_or(SessionSupervisorError::PoisonedOwnershipInvariant)?;
        Ok(self.model.reduce_into(event, &mut journal.effects))
    }

    fn return_uncommitted_reduction(&mut self) {
        // Invoked only for an actual normally-returned Model Err. Neither an
        // empty postcommit batch nor an unwind is evidence of precommit failure.
        let safe = matches!(self.ordinary_effects, OrdinaryEffectStorage::Entered)
            && self.poisoned
            && self.receipt_journal.as_ref().is_some_and(|journal| {
                journal.effects.is_empty()
                    && !journal.effects.mutation_started()
                    && journal.effects.capacity() == 3
                    && journal.observations.is_empty()
                    && journal.sink_accepted_frontier.is_none()
            });
        if safe && let Some(journal) = self.receipt_journal.take() {
            self.ordinary_effects = OrdinaryEffectStorage::Idle(journal.effects);
            self.poisoned = false;
        }
    }
}

impl SessionSupervisor {
    pub(super) fn apply_event_observed_inner(
        &mut self,
        event: SessionEvent,
    ) -> Result<ReducerObservedApply, SessionSupervisorError> {
        self.require_healthy()?;
        if matches!(event, SessionEvent::LocalData { .. }) {
            return Err(SessionSupervisorError::UnownedLocalData);
        }
        if matches!(
            event,
            SessionEvent::ReplacementAttached { .. } | SessionEvent::ReplacementCaughtUp { .. }
        ) {
            return Err(SessionSupervisorError::ReplacementProvenanceRequired);
        }
        let sink_accepted = match &event {
            SessionEvent::SinkAccepted { offer, bytes } if *bytes > 0 => offer
                .offset()
                .checked_advance(*bytes)
                .ok()
                .map(|next_accepted| (*offer, next_accepted)),
            _ => None,
        };
        let retry = event.clone();
        match self.reduce_into_receipt_journal(event)? {
            Ok(()) => (),
            Err(error) => {
                self.return_uncommitted_reduction();
                if let Some(block) = ReducerAdmissionBlock::from_error(&error) {
                    return Err(SessionSupervisorError::RejectedEvent {
                        block,
                        event: Box::new(retry),
                    });
                }
                return Err(SessionSupervisorError::Reducer(error));
            }
        };
        if let Some((offer, next_accepted)) = sink_accepted {
            let frontier = ReducerSinkAcceptedFrontier {
                session_id: offer.session_id(),
                flow_id: offer.flow_id(),
                generation: self.model.snapshot().generation(),
                direction: offer.direction(),
                previous_accepted: offer.offset(),
                next_accepted,
            };
            let journal = self
                .receipt_journal
                .as_mut()
                .ok_or(SessionSupervisorError::PoisonedOwnershipInvariant)?;
            if journal.sink_accepted_frontier.is_some() {
                return Err(SessionSupervisorError::DuplicateSinkAcceptedObservation);
            }
            journal.sink_accepted_frontier = Some(frontier);
        }
        self.finish_receipt_journal()
    }

    pub(super) fn consume_after_model_commit(
        &mut self,
        effects: Vec<SessionEffect>,
    ) -> Result<Vec<SessionEffect>, SessionSupervisorError> {
        self.consume_after_model_commit_observed(effects)
            .map(ReducerObservedApply::into_effects)
    }

    pub(super) fn consume_after_model_commit_observed(
        &mut self,
        effects: Vec<SessionEffect>,
    ) -> Result<ReducerObservedApply, SessionSupervisorError> {
        self.begin_receipt_journal(effects)?;
        self.finish_receipt_journal()
    }

    pub(super) fn begin_receipt_journal(
        &mut self,
        effects: Vec<SessionEffect>,
    ) -> Result<(), SessionSupervisorError> {
        // All admitted callers prove health before model commit. This check
        // never clears an older poison or reuses an interrupted journal.
        self.require_healthy()?;
        self.receipt_journal = Some(SupervisorReceiptJournal {
            effects: SessionEffectBatch::from_vec(effects),
            observations: ReducerOwnerObservations::default(),
            sink_accepted_frontier: None,
        });
        self.poisoned = true;
        Ok(())
    }

    pub(super) fn finish_receipt_journal(
        &mut self,
    ) -> Result<ReducerObservedApply, SessionSupervisorError> {
        if let Err(error) = self.consume_internal_receipts() {
            self.owner_catch_up_permit = None;
            return Err(error);
        }
        self.invalidate_owner_catch_up_permit();
        #[cfg(test)]
        if self.owner_data_unwind_once == Some(OwnerDataUnwindPoint::ReceiptSinkAccepted)
            && self
                .receipt_journal
                .as_ref()
                .is_some_and(|journal| journal.sink_accepted_frontier.is_some())
        {
            self.owner_data_unwind_once = None;
            panic!("single-use actual SinkAccepted journal unwind");
        }
        let journal = self
            .receipt_journal
            .take()
            .ok_or(SessionSupervisorError::PoisonedOwnershipInvariant)?;
        if matches!(self.ordinary_effects, OrdinaryEffectStorage::Entered) {
            self.ordinary_effects = OrdinaryEffectStorage::Transferred;
        }
        self.poisoned = false;
        Ok(ReducerObservedApply {
            effects: journal.effects,
            observations: journal.observations,
            sink_accepted_frontier: journal.sink_accepted_frontier,
            session_has_outstanding_replay: !self.uplink_replay.is_empty(),
        })
    }

    pub(super) fn invalidate_owner_catch_up_permit(&mut self) {
        let snapshot = self.model.snapshot();
        if self
            .owner_catch_up_permit
            .as_ref()
            .is_some_and(|permit| !permit.matches_current(snapshot))
        {
            self.owner_catch_up_permit = None;
        }
    }
}

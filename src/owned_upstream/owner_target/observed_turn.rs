//! Observed effect turns within the sole OwnerTarget executor authority.
//!
//! Owns reduction observations, bounded page/resume execution and original
//! entered/failed page custody. Original queue, facts, output prefix and temporary
//! OPEN Source remain executor-held through Target callbacks and completions.
//! Storage and physical lifecycle stay at their existing seams; no second queue.
//! Entries are reduction, resume and effect-page operations. Existing tests
//! live in the parent tests and tests/r7_observed_pages.rs.

use super::effect_storage::OwnerTargetIncomingReduction;
use super::*;
use crate::resumable::effect_storage::SessionEffectBatch;

impl AbortedOwnerTargetEffects {
    pub(super) fn retained_inventory(&self) -> (usize, usize, usize) {
        let bundles = self
            .observations
            .iter()
            .chain(self.incoming.iter().map(|incoming| &incoming.observations));
        let (count, facts) = bundles.fold((0, 0), |(count, facts), bundle| {
            (count + 1, facts + bundle.fact_count())
        });
        (
            self.effects.len()
                + usize::from(self.current.is_some())
                + self.incoming.as_ref().map_or(0, |i| i.effects.len()),
            count,
            facts,
        )
    }
}

impl<T: TargetIo> OwnerTargetExecutor<T> {
    /// Executes one exact post-attach continuation without publishing any of
    /// its outputs. Invalid or stale capabilities are returned intact; once
    /// execution begins, every fatal path quarantines the attach guard with
    /// the executor's exact aborted page state.
    pub(crate) fn resume_attached_recovery_effects_observed(
        &mut self,
        recovery: OwnerTargetRecoveryResume,
        queue: &LegOutboundQueue,
    ) -> Result<OwnerTargetObservedRecoveryPage, OwnerTargetError> {
        if let Some(block) = self.recovery_owner_block(&recovery.resume.owner) {
            return Err(OwnerTargetError::RejectedAttachedRecoveryResume {
                block,
                recovery: Box::new(recovery),
            });
        }
        if let Some(block) = self.recovery_admission_block_allow_pending_turn() {
            return Err(OwnerTargetError::RejectedAttachedRecoveryResume {
                block,
                recovery: Box::new(recovery),
            });
        }
        if !recovery.acceptance.queue_is_live() {
            return Err(OwnerTargetError::RejectedAttachedRecoveryResume {
                block: OwnerTargetInputBlock::AcceptanceQueueLost,
                recovery: Box::new(recovery),
            });
        }
        if !recovery.acceptance.belongs_to_queue(queue) {
            return Err(OwnerTargetError::RejectedAttachedRecoveryResume {
                block: OwnerTargetInputBlock::WrongAcceptanceQueue,
                recovery: Box::new(recovery),
            });
        }
        let Some(expected) = self.outstanding_resume else {
            return Err(OwnerTargetError::RejectedAttachedRecoveryResume {
                block: OwnerTargetInputBlock::NoPendingEffectTurn,
                recovery: Box::new(recovery),
            });
        };
        if recovery.resume.id != expected {
            let actual = recovery.resume.id;
            return Err(OwnerTargetError::RejectedAttachedRecoveryResume {
                block: OwnerTargetInputBlock::InvalidEffectResume { expected, actual },
                recovery: Box::new(recovery),
            });
        }
        if let Err(source) = self.prepare_effect_entry(0) {
            return Err(OwnerTargetError::RejectedAttachedRecoveryResume {
                block: OwnerTargetInputBlock::EffectStorage(source),
                recovery: Box::new(recovery),
            });
        }
        let OwnerTargetRecoveryResume {
            attached,
            acceptance,
            resume,
        } = recovery;
        let guard = OwnerTargetRecoveryGuard {
            owner: resume.owner.clone(),
            attached,
            acceptance,
        };
        self.outstanding_resume = None;
        let effects = std::mem::take(&mut self.pending_effects);
        let outputs = std::mem::take(&mut self.prepared_outputs);
        self.begin_observed_page(
            effects,
            outputs,
            OwnerTargetReducerObservations::default(),
            Some(guard.into_publish_cursor(Vec::new())),
        )?;
        self.run_entered_queue()?;
        self.finish_observed_recovery()
    }

    pub(super) fn reduce_owner_command_observed(
        &mut self,
        command: SessionOwnerCommand,
    ) -> Result<(SessionEffectBatch, OwnerTargetReducerObservations), SessionSupervisorError> {
        let observation_gate = OwnerTargetObservedApplyGate::mint();
        let observed = self
            .supervisor
            .apply_owner_command_observed(command, &observation_gate)?;
        let (effects, observations) = observed.into_owner_parts();
        Ok((
            effects,
            OwnerTargetReducerObservations::from_supervisor(observations),
        ))
    }

    /// Consumes the exact wakeup minted by the previous bounded turn.
    pub(crate) fn resume_pending_effects(
        &mut self,
        resume: OwnerTargetResume,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        self.resume_pending_effects_observed(resume)
            .map(OwnerTargetObservedApply::into_outputs)
    }

    pub(crate) fn resume_pending_effects_observed(
        &mut self,
        resume: OwnerTargetResume,
    ) -> Result<OwnerTargetObservedApply, OwnerTargetError> {
        if self.attach_barrier_pending {
            return Err(OwnerTargetError::RejectedEffectResume {
                block: OwnerTargetInputBlock::AttachBarrierPending,
                resume: Box::new(resume),
            });
        }
        self.resume_pending_effects_inner_observed(resume)
    }

    pub(super) fn resume_pending_effects_inner(
        &mut self,
        resume: OwnerTargetResume,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        self.resume_pending_effects_inner_observed(resume)
            .map(OwnerTargetObservedApply::into_outputs)
    }

    pub(super) fn resume_pending_effects_inner_observed(
        &mut self,
        resume: OwnerTargetResume,
    ) -> Result<OwnerTargetObservedApply, OwnerTargetError> {
        if let Some(block) = self.recovery_owner_block(&resume.owner) {
            return Err(OwnerTargetError::RejectedEffectResume {
                block,
                resume: Box::new(resume),
            });
        }
        let Some(expected) = self.outstanding_resume else {
            return Err(OwnerTargetError::RejectedEffectResume {
                block: OwnerTargetInputBlock::NoPendingEffectTurn,
                resume: Box::new(resume),
            });
        };
        if resume.id != expected {
            let actual = resume.id;
            return Err(OwnerTargetError::RejectedEffectResume {
                block: OwnerTargetInputBlock::InvalidEffectResume { expected, actual },
                resume: Box::new(resume),
            });
        }
        if let Err(source) = self.prepare_effect_entry(0) {
            return Err(OwnerTargetError::RejectedEffectResume {
                block: OwnerTargetInputBlock::EffectStorage(source),
                resume: Box::new(resume),
            });
        }
        self.outstanding_resume = None;
        let effects = std::mem::take(&mut self.pending_effects);
        let outputs = std::mem::take(&mut self.prepared_outputs);
        self.execute_queue_observed_page(
            effects,
            outputs,
            OwnerTargetReducerObservations::default(),
        )
    }

    pub(super) fn execute_effects_observed(
        &mut self,
        effects: SessionEffectBatch,
        observations: OwnerTargetReducerObservations,
    ) -> Result<OwnerTargetObservedApply, OwnerTargetError> {
        if let Err(error) = self.require_no_pending_effects() {
            return Err(OwnerTargetError::PostCommitObservationRetentionInvariant {
                error: Box::new(error),
                observations: Box::new(observations),
                effects,
            });
        }
        self.begin_observed_effects(effects, observations)?;
        self.run_entered_effects()?;
        self.finish_observed_apply()
    }

    /// Enter before the first post-reducer callback. This is the original
    /// pending/aborted position, not a second K-sized backing or a poison token.
    pub(super) fn begin_observed_effects(
        &mut self,
        effects: SessionEffectBatch,
        observations: OwnerTargetReducerObservations,
    ) -> Result<(), OwnerTargetError> {
        if self.aborted_effects.is_some() {
            return Err(OwnerTargetError::PostCommitObservationRetentionInvariant {
                error: Box::new(OwnerTargetError::AbortedEffectInvariant),
                observations: Box::new(observations),
                effects,
            });
        }
        let work = std::mem::take(&mut self.pending_effects);
        let outputs = std::mem::take(&mut self.prepared_outputs);
        self.begin_observed_page(
            work,
            outputs,
            OwnerTargetReducerObservations::default(),
            None,
        )?;
        let bounds = self.stage_entered_reduction(effects, observations)?;
        self.finish_entered_reduction(bounds)
    }

    pub(super) fn begin_observed_page(
        &mut self,
        mut effects: VecDeque<OwnerTargetWork>,
        mut outputs: Vec<OwnerTargetOutput>,
        observations: OwnerTargetReducerObservations,
        recovery: Option<OwnerTargetRecoveryDrain>,
    ) -> Result<(), OwnerTargetError> {
        if self.aborted_effects.is_some() {
            return Err(OwnerTargetError::AbortedEffectInvariant);
        }
        // Legacy empty-page adapters borrow the same prepared original backing.
        // This transfer does not allocate after their already-entered work.
        if effects.capacity() == 0 && self.pending_effects.is_empty() {
            std::mem::swap(&mut effects, &mut self.pending_effects);
        }
        if outputs.capacity() == 0 {
            std::mem::swap(&mut outputs, &mut self.prepared_outputs);
        }
        self.aborted_effects = Some(AbortedOwnerTargetEffects {
            effects,
            outputs,
            observations: (!observations.is_empty()).then_some(observations),
            recovery,
            ..AbortedOwnerTargetEffects::default()
        });
        Ok(())
    }

    fn entered_page(&mut self) -> Result<&mut AbortedOwnerTargetEffects, OwnerTargetError> {
        self.aborted_effects
            .as_mut()
            .ok_or(OwnerTargetError::AbortedEffectInvariant)
    }

    /// Append the actual new batch and reserve its original incoming bundle
    /// before notification/drop work. K+3 is the old transient bound; the
    /// current is inline; the suffix has exactly the old popped-current length.
    pub(super) fn stage_entered_reduction(
        &mut self,
        generated: SessionEffectBatch,
        committed: OwnerTargetReducerObservations,
    ) -> Result<(usize, usize), OwnerTargetError> {
        let page = match self.entered_page() {
            Ok(page) => page,
            Err(error) => {
                return Err(OwnerTargetError::PostCommitObservationRetentionInvariant {
                    error: Box::new(error),
                    observations: Box::new(committed),
                    effects: generated,
                });
            }
        };
        if page.incoming.is_some() {
            return Err(OwnerTargetError::PostCommitObservationRetentionInvariant {
                error: Box::new(OwnerTargetError::AbortedEffectInvariant),
                observations: Box::new(committed),
                effects: generated,
            });
        }
        let generated_len = generated.len();
        page.incoming = Some(OwnerTargetIncomingReduction {
            effects: generated,
            observations: committed,
        });
        let incoming = page
            .incoming
            .as_mut()
            .ok_or(OwnerTargetError::AbortedEffectInvariant)?;
        let attempted = page.effects.len().checked_add(generated_len).ok_or(
            OwnerTargetError::EffectStorage(
                crate::resumable::effect_storage::EffectStorageError::Layout,
            ),
        )?;
        if incoming.effects.has_overflow() || attempted > page.effects.capacity() {
            return Err(OwnerTargetError::EffectStorage(
                crate::resumable::effect_storage::EffectStorageError::Bound,
            ));
        }
        // Capacity is already prepared before the producing callback. No Clone,
        // allocation, callback or drop in this complete same-owner transfer.
        for effect in incoming.effects.drain() {
            page.effects.push_back(OwnerTargetWork::Effect(effect));
        }
        Ok((generated_len, attempted))
    }

    pub(super) fn finish_entered_reduction(
        &mut self,
        (generated_len, attempted): (usize, usize),
    ) -> Result<(), OwnerTargetError> {
        let max = self.config.max_pending_effects;
        let page = self
            .aborted_effects
            .as_mut()
            .ok_or(OwnerTargetError::AbortedEffectInvariant)?;
        if let Some(incoming) = page.incoming.as_mut() {
            if !incoming.observations.is_empty() {
                page.observations
                    .get_or_insert_with(OwnerTargetReducerObservations::default)
                    .try_merge(std::mem::take(&mut incoming.observations))?;
            }
        }
        if generated_len > MAX_INCREMENTAL_EFFECT_BURST {
            return Err(OwnerTargetError::GeneratedEffectBoundExceeded {
                max: MAX_INCREMENTAL_EFFECT_BURST,
                actual: generated_len,
            });
        }
        if attempted > max {
            return Err(OwnerTargetError::PendingEffectCapacityInvariant { max, attempted });
        }
        if let Some(incoming) = page.incoming.as_mut() {
            self.supervisor
                .reclaim_effect_batch(&mut incoming.effects)?;
        }
        page.incoming = None;
        Ok(())
    }

    pub(super) fn run_entered_effects(&mut self) -> Result<(), OwnerTargetError> {
        let actual = self.entered_page()?.effects.len();
        if actual > MAX_INCREMENTAL_EFFECT_BURST {
            return Err(OwnerTargetError::GeneratedEffectBoundExceeded {
                max: MAX_INCREMENTAL_EFFECT_BURST,
                actual,
            });
        }
        self.run_entered_queue()
    }

    /// Success is the sole owning handoff. No callback follows this take.
    pub(super) fn finish_observed_apply(
        &mut self,
    ) -> Result<OwnerTargetObservedApply, OwnerTargetError> {
        let page = self.entered_page()?;
        if page.recovery.is_some()
            || page.source.is_some()
            || page.incoming.is_some()
            || page.current.is_some()
            || page.source_releasing
            || page.open_outcome.is_some()
            || !page.effects.is_empty()
        {
            return Err(OwnerTargetError::AbortedEffectInvariant);
        }
        match self.aborted_effects.take() {
            Some(page) => {
                if self.outstanding_resume.is_none() {
                    self.pending_effects = page.effects;
                }
                Ok(OwnerTargetObservedApply {
                    outputs: page.outputs,
                    observations: page.observations.unwrap_or_default(),
                })
            }
            None => Err(OwnerTargetError::AbortedEffectInvariant),
        }
    }

    pub(super) fn finish_observed_recovery(
        &mut self,
    ) -> Result<OwnerTargetObservedRecoveryPage, OwnerTargetError> {
        let page = self.entered_page()?;
        if page.recovery.is_none()
            || page.source.is_some()
            || page.incoming.is_some()
            || page.current.is_some()
            || page.source_releasing
            || page.open_outcome.is_some()
            || !page.effects.is_empty()
        {
            return Err(OwnerTargetError::AbortedEffectInvariant);
        }
        // Cursor publication is still the caller's later transaction. Here the
        // original empty cursor merely receives this already-owned output Vec.
        match self.aborted_effects.take() {
            Some(AbortedOwnerTargetEffects {
                effects,
                outputs,
                observations,
                recovery: Some(mut publish),
                ..
            }) => {
                if self.outstanding_resume.is_none() {
                    self.pending_effects = effects;
                }
                publish.pending = outputs.into();
                Ok(OwnerTargetObservedRecoveryPage {
                    publish,
                    observations: observations.unwrap_or_default(),
                })
            }
            retained => {
                self.aborted_effects = retained;
                Err(OwnerTargetError::AbortedEffectInvariant)
            }
        }
    }

    pub(super) fn execute_queue_observed_page(
        &mut self,
        effects: VecDeque<OwnerTargetWork>,
        outputs: Vec<OwnerTargetOutput>,
        observations: OwnerTargetReducerObservations,
    ) -> Result<OwnerTargetObservedApply, OwnerTargetError> {
        self.begin_observed_page(effects, outputs, observations, None)?;
        self.run_entered_queue()?;
        self.finish_observed_apply()
    }

    pub(super) fn execute_effects(
        &mut self,
        effects: Vec<SessionEffect>,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        self.execute_effects_observed(
            SessionEffectBatch::from_vec(effects),
            OwnerTargetReducerObservations::default(),
        )
        .map(OwnerTargetObservedApply::into_outputs)
    }

    pub(super) fn execute_queue(
        &mut self,
        effects: VecDeque<OwnerTargetWork>,
    ) -> Result<Vec<OwnerTargetOutput>, OwnerTargetError> {
        self.execute_queue_observed_page(
            effects,
            Vec::new(),
            OwnerTargetReducerObservations::default(),
        )
        .map(OwnerTargetObservedApply::into_outputs)
    }

    /// Transfer only after known settlement, without a callback between this
    /// replacement and the original Flow's owning store. The position remains
    /// nonzero if subsequent Flow-owned notification work unwinds.
    pub(super) fn take_entered_work(
        &mut self,
        flow_id: SessionFlowId,
        operation: TargetIoOperation,
    ) -> Result<OwnerTargetWork, OwnerTargetError> {
        let current = self
            .entered_page()?
            .current
            .as_mut()
            .ok_or(OwnerTargetError::AbortedEffectInvariant)?;
        Ok(std::mem::replace(
            current,
            OwnerTargetWork::TargetIoInvariant { flow_id, operation },
        ))
    }

    pub(super) fn retire_entered_work(&mut self) -> Result<(), OwnerTargetError> {
        let page = self.entered_page()?;
        page.current.take();
        page.open_outcome = None;
        Ok(())
    }

    pub(super) fn run_entered_queue(&mut self) -> Result<(), OwnerTargetError> {
        let page = self.entered_page()?;
        if page.current.is_some() {
            return Err(OwnerTargetError::AbortedEffectInvariant);
        }
        let mut steps = 0usize;
        while steps < self.config.max_effect_steps {
            let page = self.entered_page()?;
            // The old pop is immediately followed by an owning store, before
            // any callback. Suffix append sees exactly the old physical length.
            page.current = page.effects.pop_front();
            let Some(work) = page.current.as_ref() else {
                return Ok(());
            };
            steps = steps
                .checked_add(1)
                .ok_or(OwnerTargetError::EffectStepOverflow)?;
            let reserved = match work {
                OwnerTargetWork::Effect(effect) => incremental_effect_burst(effect),
                _ => 0,
            };
            let pending = page.effects.len();
            if matches!(work, OwnerTargetWork::Effect(_))
                && !pending
                    .checked_add(reserved)
                    .is_some_and(|attempted| attempted <= self.config.max_pending_effects)
            {
                return Err(OwnerTargetError::PendingEffectCapacityInvariant {
                    max: self.config.max_pending_effects,
                    attempted: pending.saturating_add(1).saturating_add(reserved),
                });
            }
            // This page already owns its popped current and untouched suffix.
            // Prepare both possible generated work and output before callbacks.
            self.prepare_entered_effect(reserved, steps == self.config.max_effect_steps)?;
            match self
                .aborted_effects
                .as_ref()
                .and_then(|page| page.current.as_ref())
            {
                Some(OwnerTargetWork::ExpireNextTarget) => {
                    if self.expire_next_target()? {
                        let page = self.entered_page()?;
                        if let Some(current) = page.current.take() {
                            page.effects.push_front(current);
                        }
                        continue;
                    }
                    self.entered_page()?
                        .outputs
                        .push(OwnerTargetOutput::SessionExpired);
                }
                Some(OwnerTargetWork::TargetIoInvariant { .. }) | None => {
                    return Err(OwnerTargetError::AbortedEffectInvariant);
                }
                Some(OwnerTargetWork::Effect(SessionEffect::Transmit(_))) => {
                    let page = self.entered_page()?;
                    if let Some(OwnerTargetWork::Effect(SessionEffect::Transmit(frame))) =
                        page.current.take()
                    {
                        page.outputs.push(OwnerTargetOutput::Transmit(frame));
                    }
                    continue;
                }
                Some(OwnerTargetWork::Effect(SessionEffect::LegActivated { generation })) => {
                    let generation = *generation;
                    self.entered_page()?
                        .outputs
                        .push(OwnerTargetOutput::LegActivated { generation });
                }
                Some(OwnerTargetWork::Effect(SessionEffect::ResumeGraceStarted { generation })) => {
                    let generation = *generation;
                    self.entered_page()?
                        .outputs
                        .push(OwnerTargetOutput::ResumeGraceStarted { generation });
                }
                Some(OwnerTargetWork::Effect(SessionEffect::SessionExpired)) => {
                    let page = self.entered_page()?;
                    page.current = None;
                    page.effects.push_front(OwnerTargetWork::ExpireNextTarget);
                    continue;
                }
                Some(OwnerTargetWork::Effect(SessionEffect::PeerOpenRequested {
                    request,
                    flow,
                    ..
                })) => {
                    let (request, flow) = (*request, *flow);
                    let flow_id = request.flow_id();
                    if flow.flow_id() != flow_id || self.flows.contains_key(&flow_id) {
                        return Err(OwnerTargetError::DuplicateOrMismatchedPeerOpen { flow_id });
                    }
                    let (source_flow, driver) = self
                        .source_factory
                        .open_flow(flow)
                        .map_err(OwnerTargetError::FlowPort)?;
                    self.entered_page()?.source = Some(TargetReadSource {
                        flow: source_flow,
                        driver,
                    });
                    let page = self
                        .aborted_effects
                        .as_ref()
                        .ok_or(OwnerTargetError::AbortedEffectInvariant)?;
                    let Some(OwnerTargetWork::Effect(SessionEffect::PeerOpenRequested {
                        target,
                        ..
                    })) = page.current.as_ref()
                    else {
                        return Err(OwnerTargetError::AbortedEffectInvariant);
                    };
                    let outcome = self
                        .target
                        .open(flow_id, target)
                        .map_err(OwnerTargetError::Target)?;
                    self.entered_page()?.open_outcome = Some(outcome);
                    let (result, target_present, retain_source, terminal) =
                        match self.entered_page()?.open_outcome.as_ref() {
                            Some(
                                TargetOpenCompletion::Opened | TargetOpenCompletion::AlreadyOpen,
                            ) => (OpenResultCode::Opened, true, true, None),
                            Some(TargetOpenCompletion::AlreadyTerminal(kind)) => {
                                (OpenResultCode::Internal, true, false, Some(*kind))
                            }
                            Some(TargetOpenCompletion::Failed(failure)) => {
                                (open_failure_result(*failure), false, false, None)
                            }
                            None => return Err(OwnerTargetError::AbortedEffectInvariant),
                        };
                    let source = if retain_source {
                        self.entered_page()?.source.take()
                    } else {
                        // Source destruction can publish factory progress. Keep
                        // the entered OPEN and original facts through that drop;
                        // unknown disposal is not a live Source or clean zero.
                        self.entered_page()?.source_releasing = true;
                        drop(self.entered_page()?.source.take());
                        self.entered_page()?.source_releasing = false;
                        None
                    };
                    self.flows.insert(
                        flow_id,
                        TargetFlowOwner {
                            target_present,
                            source,
                            pending_sink: None,
                            terminal,
                            join_pending: false,
                            retire_after_join: false,
                            pending_completion: None,
                            service_position: None,
                        },
                    );
                    self.submit_target_completion(
                        flow_id,
                        PendingTargetCompletion::open(flow, request, result, terminal),
                    )?;
                }
                Some(OwnerTargetWork::Effect(SessionEffect::OfferToSink { offer, .. })) => {
                    let flow_id = offer.flow_id();
                    let flow = self
                        .flows
                        .get(&flow_id)
                        .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
                    if flow.pending_sink.is_some() {
                        return Err(OwnerTargetError::DuplicatePendingTargetWrite { flow_id });
                    }
                    let failed = matches!(
                        (&self.service, &flow.pending_completion),
                        (
                            service::OwnerTargetServiceState::Bound { .. },
                            Some(PendingTargetCompletion::Failed { .. })
                        )
                    );
                    if failed {
                        self.park_entered_sink(flow_id)?;
                    } else {
                        self.attempt_target_write()?;
                    }
                }
                Some(OwnerTargetWork::Effect(SessionEffect::HalfCloseSink { completion })) => {
                    let completion = *completion;
                    let flow_id = completion.flow_id();
                    let flow = self
                        .flows
                        .get(&flow_id)
                        .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
                    if flow.pending_sink.is_some() {
                        return Err(OwnerTargetError::HalfCloseBeforePendingWrite { flow_id });
                    }
                    let failed = matches!(
                        (&self.service, &flow.pending_completion),
                        (
                            service::OwnerTargetServiceState::Bound { .. },
                            Some(PendingTargetCompletion::Failed { .. })
                        )
                    );
                    if failed {
                        self.park_entered_sink(flow_id)?;
                    } else {
                        match self
                            .target
                            .half_close_write(flow_id)
                            .map_err(OwnerTargetError::Target)?
                        {
                            TargetHalfCloseCompletion::Closed
                            | TargetHalfCloseCompletion::AlreadyClosed => self
                                .submit_target_completion(
                                    flow_id,
                                    PendingTargetCompletion::HalfClose {
                                        event: SessionEvent::SinkHalfClosed { completion },
                                    },
                                )?,
                            TargetHalfCloseCompletion::Failed(cause) => {
                                self.apply_entered_target_failure(flow_id, cause)?
                            }
                        }
                    }
                }
                Some(OwnerTargetWork::Effect(SessionEffect::PeerReset { flow_id, .. })) => {
                    let flow_id = *flow_id;
                    let failed =
                        matches!(self.service, service::OwnerTargetServiceState::Bound { .. })
                            && self.flows.get(&flow_id).is_some_and(|flow| {
                                matches!(
                                    flow.pending_completion,
                                    Some(PendingTargetCompletion::Failed { .. })
                                )
                            });
                    if !failed {
                        if !self.flows.contains_key(&flow_id) {
                            return Err(OwnerTargetError::UnknownTargetFlow { flow_id });
                        }
                        let terminal = match self
                            .target
                            .reset(flow_id)
                            .map_err(OwnerTargetError::Target)?
                        {
                            TargetResetCompletion::Reset { .. }
                            | TargetResetCompletion::AlreadyReset => TargetTerminalKind::Reset,
                            TargetResetCompletion::AlreadyTerminal(kind) => kind,
                        };
                        if let Some(flow) = self.flows.get_mut(&flow_id) {
                            flow.pending_sink = None;
                            flow.terminal = Some(terminal);
                        }
                    }
                }
                Some(OwnerTargetWork::Effect(SessionEffect::FlowFinished {
                    terminal,
                    reason,
                    ..
                })) => {
                    let (terminal, reason) = (*terminal, *reason);
                    self.finish_target_flow(terminal, reason)?;
                    self.entered_page()?
                        .outputs
                        .push(OwnerTargetOutput::TerminalGraceStarted { terminal });
                }
                Some(OwnerTargetWork::Effect(
                    SessionEffect::ReplayStored { .. } | SessionEffect::ReplayAcknowledged { .. },
                )) => return Err(OwnerTargetError::InternalReceiptEscapedSupervisor),
                Some(OwnerTargetWork::Effect(
                    SessionEffect::LocalFlowOpened { .. } | SessionEffect::LocalOpenResolved { .. },
                )) => return Err(OwnerTargetError::ClientOnlyEffectAtOwner),
            }
            self.retire_entered_work()?;
        }
        let attempted = self.entered_page()?.effects.len();
        if attempted == 0 {
            return Ok(());
        }
        if attempted > self.config.max_pending_effects {
            return Err(OwnerTargetError::PendingEffectCapacityInvariant {
                max: self.config.max_pending_effects,
                attempted,
            });
        }
        let resume = self.issue_resume()?;
        self.entered_page()?
            .outputs
            .push(OwnerTargetOutput::NeedsResume { resume });
        self.pending_effects = std::mem::take(&mut self.entered_page()?.effects);
        Ok(())
    }

    fn park_entered_sink(&mut self, flow_id: SessionFlowId) -> Result<(), OwnerTargetError> {
        let current = self.take_entered_work(flow_id, TargetIoOperation::WriteCompletion)?;
        let sink = match current {
            OwnerTargetWork::Effect(SessionEffect::OfferToSink { offer, segments }) => {
                PendingTargetSink::Write(PendingTargetWrite { offer, segments })
            }
            OwnerTargetWork::Effect(SessionEffect::HalfCloseSink { completion }) => {
                PendingTargetSink::HalfClose(completion)
            }
            other => {
                self.entered_page()?.current = Some(other);
                return Err(OwnerTargetError::AbortedEffectInvariant);
            }
        };
        self.flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?
            .pending_sink = Some(sink);
        Ok(())
    }

    pub(super) fn attempt_target_write(&mut self) -> Result<(), OwnerTargetError> {
        let page = self
            .aborted_effects
            .as_ref()
            .ok_or(OwnerTargetError::AbortedEffectInvariant)?;
        let Some(OwnerTargetWork::Effect(SessionEffect::OfferToSink { offer, segments })) =
            page.current.as_ref()
        else {
            return Err(OwnerTargetError::AbortedEffectInvariant);
        };
        let offer = *offer;
        let flow_id = offer.flow_id();
        let first = segments
            .first()
            .ok_or(OwnerTargetError::EmptyTargetOffer { flow_id })?;
        let exposed = first.len();
        match self
            .target
            .write(flow_id, first.payload())
            .map_err(OwnerTargetError::Target)?
        {
            TargetWriteCompletion::Accepted(bytes) => {
                let accepted = bytes.get();
                let offered = offer.len();
                if accepted > exposed || accepted > offered {
                    return Err(OwnerTargetError::InvalidTargetWriteAcceptance {
                        flow_id,
                        accepted,
                        exposed,
                        offered,
                    });
                }
                let (generated, committed) = self
                    .reduce_owner_command_observed(SessionOwnerCommand::Event(
                        SessionEvent::SinkAccepted {
                            offer,
                            bytes: accepted,
                        },
                    ))
                    .map_err(OwnerTargetError::Supervisor)?;
                let bounds = self.stage_entered_reduction(generated, committed)?;
                self.finish_entered_reduction(bounds)
            }
            TargetWriteCompletion::Zero | TargetWriteCompletion::WouldBlock => {
                let flow = self
                    .flows
                    .get(&flow_id)
                    .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
                if flow.pending_sink.is_some() {
                    return Err(OwnerTargetError::DuplicatePendingTargetWrite { flow_id });
                }
                self.park_entered_sink(flow_id)
            }
            TargetWriteCompletion::Failed(cause) => {
                self.apply_entered_target_failure(flow_id, cause)
            }
        }
    }

    pub(super) fn retain_observations_on_failure<R>(
        &mut self,
        result: Result<R, OwnerTargetError>,
        observations: OwnerTargetReducerObservations,
    ) -> Result<R, OwnerTargetError> {
        let Err(error) = result else {
            return result;
        };
        if observations.is_empty() {
            return Err(error);
        }
        match self.retain_failed_observations(observations) {
            Ok(()) => Err(error),
            Err(observations) => Err(OwnerTargetError::PostCommitObservationRetentionInvariant {
                effects: SessionEffectBatch::empty(),
                error: Box::new(error),
                observations: Box::new(observations),
            }),
        }
    }

    pub(super) fn retain_failed_observations(
        &mut self,
        observations: OwnerTargetReducerObservations,
    ) -> Result<(), OwnerTargetReducerObservations> {
        match self.aborted_effects.as_mut() {
            Some(aborted) if aborted.observations.is_none() => {
                aborted.observations = Some(observations);
                Ok(())
            }
            Some(_) => Err(observations),
            None => {
                self.aborted_effects = Some(AbortedOwnerTargetEffects {
                    effects: VecDeque::new(),
                    outputs: Vec::new(),
                    observations: Some(observations),
                    recovery: None,
                    ..AbortedOwnerTargetEffects::default()
                });
                Ok(())
            }
        }
    }
    pub(super) fn retain_failed_recovery_cursor(
        &mut self,
        recovery: OwnerTargetRecoveryDrain,
        error: OwnerTargetError,
    ) -> OwnerTargetError {
        match self.aborted_effects.as_mut() {
            Some(aborted) if aborted.recovery.is_none() => {
                aborted.recovery = Some(recovery);
                error
            }
            Some(_) => OwnerTargetError::PostCommitRecoveryRetentionInvariant {
                error: Box::new(error),
                recovery: Box::new(recovery),
            },
            None => {
                self.aborted_effects = Some(AbortedOwnerTargetEffects {
                    effects: VecDeque::new(),
                    outputs: Vec::new(),
                    observations: None,
                    recovery: Some(recovery),
                    ..AbortedOwnerTargetEffects::default()
                });
                error
            }
        }
    }
    /// Retains the exact work item and every already-generated output before
    /// surfacing a fatal owner/Target invariant. The executor is permanently
    /// poisoned after this point: no retry can accidentally repeat I/O whose
    /// commit status is no longer trusted.
    pub(super) fn abort_owner_error<R>(
        &mut self,
        current: OwnerTargetWork,
        pending: &mut VecDeque<OwnerTargetWork>,
        outputs: &mut Vec<OwnerTargetOutput>,
        error: OwnerTargetError,
    ) -> Result<R, OwnerTargetError> {
        if self.aborted_effects.is_some() {
            return Err(error);
        }
        self.aborted_effects = Some(AbortedOwnerTargetEffects {
            current: Some(current),
            effects: std::mem::take(pending),
            outputs: std::mem::take(outputs),
            observations: None,
            recovery: None,
            ..AbortedOwnerTargetEffects::default()
        });
        Err(error)
    }
    pub(super) fn abort_target_io<R>(
        &mut self,
        current: OwnerTargetWork,
        pending: &mut VecDeque<OwnerTargetWork>,
        outputs: &mut Vec<OwnerTargetOutput>,
        error: TargetIoError,
    ) -> Result<R, OwnerTargetError> {
        self.abort_owner_error(current, pending, outputs, OwnerTargetError::Target(error))
    }
    pub(super) fn abort_direct_target_io<R>(
        &mut self,
        flow_id: SessionFlowId,
        operation: TargetIoOperation,
        error: TargetIoError,
    ) -> Result<R, OwnerTargetError> {
        self.abort_direct_owner_error(flow_id, operation, OwnerTargetError::Target(error))
    }
    pub(super) fn abort_direct_owner_error<R>(
        &mut self,
        flow_id: SessionFlowId,
        operation: TargetIoOperation,
        error: OwnerTargetError,
    ) -> Result<R, OwnerTargetError> {
        if self.aborted_effects.is_some() {
            return Err(error);
        }
        let mut pending = std::mem::take(&mut self.pending_effects);
        let mut outputs = std::mem::take(&mut self.prepared_outputs);
        self.abort_owner_error(
            OwnerTargetWork::TargetIoInvariant { flow_id, operation },
            &mut pending,
            &mut outputs,
            error,
        )
    }
    pub(super) fn issue_resume(&mut self) -> Result<OwnerTargetResume, OwnerTargetError> {
        if self.outstanding_resume.is_some() {
            return Err(OwnerTargetError::DuplicateEffectResume);
        }
        let id = self
            .next_resume_id
            .ok_or(OwnerTargetError::EffectResumeIdExhausted)?;
        self.next_resume_id = id.checked_add(1);
        self.outstanding_resume = Some(id);
        Ok(OwnerTargetResume {
            owner: Arc::downgrade(&self.identity),
            id,
        })
    }
    pub(crate) fn apply_event_observed(
        &mut self,
        event: SessionEvent,
    ) -> Result<OwnerTargetObservedApply, OwnerTargetError> {
        if let Some(block) = self.event_admission_block(&event) {
            return Err(OwnerTargetError::RejectedEvent {
                block,
                event: Box::new(event),
            });
        }
        if let Err(source) = self.prepare_effect_entry(3) {
            return Err(OwnerTargetError::RejectedEvent {
                block: OwnerTargetInputBlock::EffectStorage(source),
                event: Box::new(event),
            });
        }
        let terminal_retirement = match &event {
            SessionEvent::TerminalGraceExpired { terminal } => {
                Some((terminal.session_id(), terminal.flow_id()))
            }
            _ => None,
        };
        let (effects, mut observations) =
            match self.reduce_owner_command_observed(SessionOwnerCommand::Event(event)) {
                Ok(observed) => observed,
                Err(SessionSupervisorError::RejectedCommand { block, command }) => {
                    let SessionOwnerCommand::Event(event) = *command else {
                        return Err(OwnerTargetError::Supervisor(
                            SessionSupervisorError::RejectedCommand { block, command },
                        ));
                    };
                    return Err(OwnerTargetError::RejectedEvent {
                        block: OwnerTargetInputBlock::ReducerAdmission(block),
                        event: Box::new(event),
                    });
                }
                Err(error) => return Err(OwnerTargetError::Supervisor(error)),
            };
        if let Some((session_id, flow_id)) = terminal_retirement {
            observations.flow_retired = Some(OwnerTargetFlowRetired {
                session_id,
                flow_id,
                generation: self.supervisor.snapshot().session.generation(),
            });
        }
        self.begin_observed_effects(effects, observations)?;
        if let Some((_, flow_id)) = terminal_retirement {
            let page = self.entered_page()?;
            page.current = Some(OwnerTargetWork::TargetIoInvariant {
                flow_id,
                operation: TargetIoOperation::TerminalRetirement,
            });
            self.retire_target_tombstone(flow_id)?;
            if let Some(current) = self.entered_page()?.current.as_mut() {
                *current = OwnerTargetWork::TargetIoInvariant {
                    flow_id,
                    operation: TargetIoOperation::FailureCompletion,
                };
            }
            self.retry_one_pending_target_completion()?;
            self.retire_entered_work()?;
        }
        self.run_entered_effects()?;
        self.finish_observed_apply()
    }

    /// Executes exactly one bounded post-attach recovery page without
    /// mutating its acceptance FIFO. The caller must consume the returned
    /// reducer observations before publishing the cursor.
    pub(crate) fn execute_attached_recovery_observed(
        &mut self,
        recovery: EnqueuedOwnerTargetAttach,
        queue: &LegOutboundQueue,
    ) -> Result<OwnerTargetObservedRecoveryPage, OwnerTargetError> {
        if let Some(block) = self.recovery_owner_block(&recovery.owner) {
            return Err(OwnerTargetError::RejectedAttachedRecovery {
                block,
                recovery: Box::new(recovery),
            });
        }
        if !recovery.recovery.acceptance.queue_is_live() {
            return Err(OwnerTargetError::RejectedAttachedRecovery {
                block: OwnerTargetInputBlock::AcceptanceQueueLost,
                recovery: Box::new(recovery),
            });
        }
        if !recovery.recovery.acceptance.belongs_to_queue(queue) {
            return Err(OwnerTargetError::RejectedAttachedRecovery {
                block: OwnerTargetInputBlock::WrongAcceptanceQueue,
                recovery: Box::new(recovery),
            });
        }
        if let Some(block) = self.recovery_admission_block() {
            return Err(OwnerTargetError::RejectedAttachedRecovery {
                block,
                recovery: Box::new(recovery),
            });
        }
        if let Err(source) = self.prepare_effect_entry(recovery.recovery.effects.len()) {
            return Err(OwnerTargetError::RejectedAttachedRecovery {
                block: OwnerTargetInputBlock::EffectStorage(source),
                recovery: Box::new(recovery),
            });
        }
        let EnqueuedOwnerTargetAttach {
            owner,
            attached,
            recovery:
                EnqueuedOwnerTargetRecovery {
                    acceptance,
                    effects,
                },
        } = recovery;
        let guard = OwnerTargetRecoveryGuard {
            owner,
            attached,
            acceptance,
        };
        let actual = effects.len();
        let work = std::mem::take(&mut self.pending_effects);
        let outputs = std::mem::take(&mut self.prepared_outputs);
        self.begin_observed_page(
            work,
            outputs,
            OwnerTargetReducerObservations::default(),
            Some(guard.into_publish_cursor(Vec::new())),
        )?;
        let (_generated, _attempted) = self.stage_entered_reduction(
            SessionEffectBatch::from_vec(effects),
            OwnerTargetReducerObservations::default(),
        )?;
        if actual > self.max_recovery_effects {
            return Err(OwnerTargetError::GeneratedEffectBoundExceeded {
                max: self.max_recovery_effects,
                actual,
            });
        }
        let page = self
            .aborted_effects
            .as_mut()
            .ok_or(OwnerTargetError::AbortedEffectInvariant)?;
        let incoming = page
            .incoming
            .as_mut()
            .ok_or(OwnerTargetError::AbortedEffectInvariant)?;
        self.supervisor
            .reclaim_effect_batch(&mut incoming.effects)?;
        page.incoming = None;
        self.run_entered_queue()?;
        self.finish_observed_recovery()
    }

    pub(crate) fn retry_target_write_observed(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<OwnerTargetObservedApply, OwnerTargetError> {
        self.require_no_pending_effects()?;
        self.require_no_pending_target_completion(flow_id)?;
        let flow = self
            .flows
            .get_mut(&flow_id)
            .ok_or(OwnerTargetError::UnknownTargetFlow { flow_id })?;
        let pending = match flow.pending_sink.take() {
            Some(PendingTargetSink::Write(pending)) => pending,
            retained => {
                flow.pending_sink = retained;
                return Err(OwnerTargetError::NoPendingTargetWrite { flow_id });
            }
        };
        self.begin_observed_page(
            VecDeque::new(),
            Vec::new(),
            OwnerTargetReducerObservations::default(),
            None,
        )?;
        self.entered_page()?.current = Some(OwnerTargetWork::Effect(SessionEffect::OfferToSink {
            offer: pending.offer,
            segments: pending.segments,
        }));
        self.attempt_target_write()?;
        self.retire_entered_work()?;
        self.run_entered_queue()?;
        self.finish_observed_apply()
    }
}

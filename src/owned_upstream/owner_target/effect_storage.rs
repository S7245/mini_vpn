//! Preparation and transfer of the Executor's original work and output backing.
//! No second pending queue, reducer rule, Target callback or lifecycle owner.
//! Entry prepares before consumption; entered work retains custody on failure.
//! Tests: tests/r7_initial_bootstrap_handoff/effect_storage.rs.

use super::*;
use crate::resumable::effect_storage::{
    EffectStorageError, SessionEffectBatch, exact_vec, growth_peak, prepare_deque, prepare_vec,
};

pub(super) struct OwnerTargetIncomingReduction {
    pub(super) effects: SessionEffectBatch,
    pub(super) observations: OwnerTargetReducerObservations,
}

#[derive(Debug)]
pub(crate) struct OwnerRegisteredRestoreFailure {
    pub(super) source: super::super::standby::OwnerRegisteredAttachRestoreError,
    pub(super) error: Box<SessionSupervisorError>,
}

#[derive(Debug)]
pub(crate) struct OwnerCatchUpReleaseFailure {
    pub(super) error: Box<SessionSupervisorError>,
    pub(super) reservation: Box<AttachAcceptanceReservation>,
}

impl<T: TargetIo> OwnerTargetExecutor<T> {
    pub(super) fn prepare_cold_effect_storage(
        config: OwnerTargetConfig,
    ) -> Result<(VecDeque<OwnerTargetWork>, Vec<OwnerTargetOutput>), EffectStorageError> {
        let work_max = config
            .max_pending_effects
            .checked_add(3)
            .ok_or(EffectStorageError::Layout)?;
        let output_max = config
            .max_effect_steps
            .checked_add(1)
            .ok_or(EffectStorageError::Layout)?;
        growth_peak::<OwnerTargetWork>(0, work_max)?
            .checked_add(growth_peak::<OwnerTargetOutput>(0, output_max)?)
            .ok_or(EffectStorageError::Layout)?;
        Ok((exact_vec(3)?.into(), exact_vec(1)?))
    }

    pub(super) fn prepare_effect_entry(
        &mut self,
        incoming: usize,
    ) -> Result<(), EffectStorageError> {
        let maximum = self
            .config
            .max_pending_effects
            .checked_add(3)
            .ok_or(EffectStorageError::Layout)?;
        if let Some(page) = &mut self.aborted_effects {
            let need = page
                .effects
                .len()
                .checked_add(incoming)
                .ok_or(EffectStorageError::Layout)?;
            prepare_deque(&mut page.effects, need, 3, maximum)?;
        } else {
            let need = self
                .pending_effects
                .len()
                .checked_add(incoming)
                .ok_or(EffectStorageError::Layout)?;
            prepare_deque(&mut self.pending_effects, need, 3, maximum)?;
            let output_max = self
                .config
                .max_effect_steps
                .checked_add(1)
                .ok_or(EffectStorageError::Layout)?;
            prepare_vec(&mut self.prepared_outputs, 1, 1, output_max)?;
        }
        Ok(())
    }

    pub(super) fn prepare_entered_effect(
        &mut self,
        burst: usize,
        final_step: bool,
    ) -> Result<(), OwnerTargetError> {
        self.prepare_effect_entry(burst)?;
        let output_max = self
            .config
            .max_effect_steps
            .checked_add(1)
            .ok_or(EffectStorageError::Layout)?;
        let page = self
            .aborted_effects
            .as_mut()
            .ok_or(OwnerTargetError::AbortedEffectInvariant)?;
        let may_resume = final_step && (burst != 0 || !page.effects.is_empty());
        let need = page
            .outputs
            .len()
            .checked_add(1 + usize::from(may_resume))
            .ok_or(EffectStorageError::Layout)?;
        prepare_vec(&mut page.outputs, need, 1, output_max)?;
        Ok(())
    }
}

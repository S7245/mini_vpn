//! Shared TCP pool admission: one candidate policy and one reservation commit.
//! Legacy callers retry contention; try_decide_once performs one round without
//! waiting for admission mutexes. Neither owns transport I/O, timers or recovery.
//! All fallible locks precede lease counts; the final idle-epoch write is infallible.
//! Atomic counters may internally retry: this is not a wait-free/hard-real-time API.
//! Focused contention/ownership tests: tests/bounded.rs; original policy: ../tests/.

use super::*;
use std::sync::{MutexGuard, TryLockError};

#[derive(Clone, Copy)]
enum AdmissionLockPolicy {
    LegacyBlocking,
    BoundedTry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TcpPoolAdmissionRoundError {
    Contended,
    Poisoned,
    InvalidInventory,
    Reservation(TcpPoolReservationError),
}

impl From<TcpPoolReservationError> for TcpPoolAdmissionRoundError {
    fn from(error: TcpPoolReservationError) -> Self {
        Self::Reservation(error)
    }
}

fn lock_for_admission<T>(
    lock: &StdMutex<T>,
    policy: AdmissionLockPolicy,
) -> Result<Option<MutexGuard<'_, T>>, TcpPoolAdmissionRoundError> {
    match policy {
        AdmissionLockPolicy::LegacyBlocking => Ok(lock.lock().ok()),
        AdmissionLockPolicy::BoundedTry => match lock.try_lock() {
            Ok(guard) => Ok(Some(guard)),
            Err(TryLockError::WouldBlock) => Err(TcpPoolAdmissionRoundError::Contended),
            Err(TryLockError::Poisoned(_)) => Err(TcpPoolAdmissionRoundError::Poisoned),
        },
    }
}

/// One value-only selection, immediately consumed by the reservation commit.
struct SelectedAdmission {
    selected: TcpPoolAdmissionCandidate,
    path_service_tiebreak: bool,
    service_normalized_ordering: bool,
    qualification_override: bool,
    all_degraded_fallback: bool,
    candidates: Vec<TcpPoolAdmissionCandidate>,
}

impl TcpPoolAdmission {
    /// No waiter is registered. The control owner must schedule a later bounded turn.
    pub(super) fn try_decide_once(
        &self,
        observations: &[TcpPoolPathObservation],
        replacement_allowed: &[bool],
        excluded_slots: &[bool],
        qualified_only: bool,
    ) -> Result<TcpPoolAdmissionDecision, TcpPoolAdmissionRoundError> {
        let count = self.active_slots.len();
        if count > MAX_TUIC_TCP_POOL
            || self.preparing_slots.len() != count
            || self.current_generation_activity.len() != count
            || self.qualification_epochs.len() != count
        {
            return Err(TcpPoolAdmissionRoundError::InvalidInventory);
        }
        self.decide_round(
            observations,
            replacement_allowed,
            excluded_slots,
            qualified_only,
            AdmissionLockPolicy::BoundedTry,
        )
    }

    pub(super) fn try_decide_excluding(
        &self,
        observations: &[TcpPoolPathObservation],
        replacement_allowed: &[bool],
        excluded_slots: &[bool],
        qualified_only: bool,
    ) -> Result<TcpPoolAdmissionDecision, TcpPoolReservationError> {
        loop {
            match self.decide_round(
                observations,
                replacement_allowed,
                excluded_slots,
                qualified_only,
                AdmissionLockPolicy::LegacyBlocking,
            ) {
                Err(TcpPoolAdmissionRoundError::Contended) => continue,
                Err(TcpPoolAdmissionRoundError::Reservation(error)) => return Err(error),
                // Legacy locking maps poison to its original None/Unknown behavior.
                Err(
                    TcpPoolAdmissionRoundError::Poisoned
                    | TcpPoolAdmissionRoundError::InvalidInventory,
                ) => return Err(TcpPoolReservationError::Saturated),
                Ok(decision) => return Ok(decision),
            }
        }
    }

    fn decide_round(
        &self,
        observations: &[TcpPoolPathObservation],
        replacement_allowed: &[bool],
        excluded_slots: &[bool],
        qualified_only: bool,
        policy: AdmissionLockPolicy,
    ) -> Result<TcpPoolAdmissionDecision, TcpPoolAdmissionRoundError> {
        if self.active_slots.is_empty() {
            return Err(TcpPoolReservationError::Empty.into());
        }
        debug_assert_eq!(self.active_slots.len(), self.preparing_slots.len());
        debug_assert_eq!(
            self.active_slots.len(),
            self.current_generation_activity.len()
        );
        debug_assert_eq!(self.active_slots.len(), self.qualification_epochs.len());

        let mut preparing = false;
        let mut candidates = Vec::with_capacity(self.active_slots.len());
        for (index, slot_preparing) in self.preparing_slots.iter().enumerate() {
            if excluded_slots.get(index).copied().unwrap_or(false) {
                continue;
            }
            if slot_preparing.load(Ordering::Acquire) {
                preparing = true;
                continue;
            }
            let Some(generation_active) = self.activity_for_admission(index, policy)? else {
                continue;
            };
            let active_before = generation_active.active();
            if active_before == u64::MAX {
                continue;
            }
            let observation = observations.get(index).copied().unwrap_or_default();
            let path_service = observation.path_service();
            let successor_service_readiness = observation.successor_service_readiness();
            let (
                successor_service_current_path_generation,
                successor_service_proved_path_generation,
                successor_service_proved_cwnd_floor,
            ) = observation.successor_service_evidence();
            let qualification =
                self.qualification_for_admission(index, active_before, observation, policy)?;
            if qualified_only
                && qualification.qualification != TcpPoolForwardQualification::Qualified
            {
                continue;
            }
            let identity = match observation {
                TcpPoolPathObservation::Unknown => None,
                TcpPoolPathObservation::Known { identity, .. } => Some(identity),
            };
            candidates.push(TcpPoolAdmissionCandidate {
                index,
                identity,
                active_before,
                path_service,
                qualification: qualification.qualification,
                qualification_anchor: qualification.anchor,
                black_holes_current: qualification.current,
                successor_service_readiness,
                successor_service_current_path_generation,
                successor_service_proved_path_generation,
                successor_service_proved_cwnd_floor,
                admitted: true,
            });
        }

        if candidates.is_empty() {
            return Err((if preparing {
                TcpPoolReservationError::Busy
            } else {
                TcpPoolReservationError::Saturated
            })
            .into());
        }

        let all_busy = candidates
            .iter()
            .all(|candidate| candidate.active_before != 0);
        let all_degraded_fallback = all_busy
            && candidates
                .iter()
                .all(|candidate| candidate.qualification == TcpPoolForwardQualification::Degraded);
        let qualification_override = all_busy
            && !all_degraded_fallback
            && candidates
                .iter()
                .any(|candidate| candidate.qualification == TcpPoolForwardQualification::Degraded);
        if qualification_override {
            for candidate in &mut candidates {
                candidate.admitted =
                    candidate.qualification != TcpPoolForwardQualification::Degraded;
            }
        }
        let service_readiness_override = candidates.iter().any(|candidate| {
            candidate.admitted && candidate.successor_service_readiness.is_stale()
        }) && candidates.iter().any(|candidate| {
            candidate.admitted && !candidate.successor_service_readiness.is_stale()
        });
        if service_readiness_override {
            for candidate in &mut candidates {
                if candidate.successor_service_readiness.is_stale() {
                    candidate.admitted = false;
                }
            }
        }
        let service_normalized_ordering = !all_degraded_fallback
            && candidates
                .iter()
                .filter(|candidate| candidate.admitted)
                .all(|candidate| candidate.active_before != 0 && candidate.path_service.is_known());

        let replacement = (!qualified_only
            && (qualification_override || service_readiness_override))
            .then(|| {
                candidates
                    .iter()
                    .copied()
                    .filter(|candidate| {
                        candidate.index != 0
                            && replacement_allowed
                                .get(candidate.index)
                                .copied()
                                .unwrap_or(false)
                            && ((candidate.active_before != 0
                                && candidate.qualification
                                    == TcpPoolForwardQualification::Degraded)
                                || candidate.successor_service_readiness.is_stale())
                            && candidate.identity.is_some()
                            && candidate.qualification_anchor.is_some()
                            && candidate.black_holes_current.is_some()
                            && !candidate.admitted
                    })
                    .min_by_key(|candidate| (candidate.active_before, candidate.index))
            });
        if let Some(Some(replacement)) = replacement {
            let Some(identity) = replacement.identity else {
                return Err(TcpPoolAdmissionRoundError::Contended);
            };
            let Some(qualification_anchor) = replacement.qualification_anchor else {
                return Err(TcpPoolAdmissionRoundError::Contended);
            };
            let Some(black_holes_current) = replacement.black_holes_current else {
                return Err(TcpPoolAdmissionRoundError::Contended);
            };
            let slot_preparing = self.preparing_slots[replacement.index].clone();
            if slot_preparing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return Err(TcpPoolAdmissionRoundError::Contended);
            }
            return Ok(TcpPoolAdmissionDecision::ReplaceAuxiliary(
                TcpPoolAuxiliaryReplacementPreparation {
                    index: replacement.index,
                    identity,
                    active_before: replacement.active_before,
                    qualification_anchor,
                    black_holes_current,
                    candidates,
                    preparation: TcpPoolSlotPreparation {
                        preparing: slot_preparing,
                        available: self.available.clone(),
                    },
                },
            ));
        }

        let mut choice: Option<TcpPoolAdmissionCandidate> = None;
        let mut path_service_tiebreak = false;
        for candidate in candidates
            .iter()
            .copied()
            .filter(|candidate| candidate.admitted)
        {
            let Some(current) = choice else {
                choice = Some(candidate);
                continue;
            };
            if service_normalized_ordering
                && let Some(ordering) = candidate.path_service.normalized_load_cmp(
                    candidate.active_before,
                    current.path_service,
                    current.active_before,
                )
            {
                match ordering {
                    std::cmp::Ordering::Less => {
                        choice = Some(candidate);
                        path_service_tiebreak = false;
                        continue;
                    }
                    std::cmp::Ordering::Greater => continue,
                    std::cmp::Ordering::Equal => {}
                }
            }
            match candidate.active_before.cmp(&current.active_before) {
                std::cmp::Ordering::Less => {
                    choice = Some(candidate);
                    path_service_tiebreak = false;
                }
                std::cmp::Ordering::Equal if candidate.active_before != 0 => {
                    if candidate
                        .path_service
                        .has_greater_service_than(current.path_service)
                    {
                        choice = Some(candidate);
                        path_service_tiebreak = true;
                    } else if current
                        .path_service
                        .has_greater_service_than(candidate.path_service)
                    {
                        path_service_tiebreak = true;
                    }
                }
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => {}
            }
        }

        let Some(selected) = choice else {
            return Err(TcpPoolReservationError::Saturated.into());
        };
        self.reserve_selected(
            SelectedAdmission {
                selected,
                path_service_tiebreak,
                service_normalized_ordering,
                qualification_override,
                all_degraded_fallback,
                candidates,
            },
            policy,
        )
    }

    fn reserve_selected(
        &self,
        selection: SelectedAdmission,
        policy: AdmissionLockPolicy,
    ) -> Result<TcpPoolAdmissionDecision, TcpPoolAdmissionRoundError> {
        let SelectedAdmission {
            selected,
            path_service_tiebreak,
            service_normalized_ordering,
            qualification_override,
            all_degraded_fallback,
            candidates,
        } = selection;
        let index = selected.index;
        let active_before = selected.active_before;
        let slot_preparing = self.preparing_slots[index].clone();
        if slot_preparing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(TcpPoolAdmissionRoundError::Contended);
        }
        let preparation = TcpPoolSlotPreparation {
            preparing: slot_preparing,
            available: self.available.clone(),
        };
        let Some(generation_active) = self.activity_for_admission(index, policy)? else {
            drop(preparation);
            return Err(TcpPoolAdmissionRoundError::Contended);
        };
        // The idle epoch is the final fallible lock, acquired BEFORE any lease count.
        // No selected guard escapes this synchronous turn.
        let mut epoch = if active_before == 0
            && selected.identity.is_some()
            && selected.black_holes_current.is_some()
        {
            match self.qualification_epochs.get(index) {
                Some(epoch) => lock_for_admission(epoch, policy)?,
                None => None,
            }
        } else {
            None
        };
        if generation_active.try_increment_from(active_before).is_ok() {
            let slot_active = self.active_slots[index].clone();
            let lease = match TcpPoolSlotLease::from_generation_reserved(
                generation_active,
                slot_active,
                self.pool_active_total.clone(),
            ) {
                Ok(lease) => lease,
                Err(error) => {
                    drop(preparation);
                    return Err(error.into());
                }
            };
            if let (Some(epoch), Some(identity), Some(black_hole_anchor)) = (
                epoch.as_mut(),
                selected.identity,
                selected.black_holes_current,
            ) {
                epoch.identity = Some(identity);
                epoch.black_hole_anchor = black_hole_anchor;
            }
            return Ok(TcpPoolAdmissionDecision::ReserveCurrent(
                TcpPoolSlotReservation {
                    index,
                    lease,
                    active_before,
                    path_service: selected.path_service,
                    path_service_tiebreak,
                    service_normalized_ordering,
                    qualification: selected.qualification,
                    qualification_anchor: selected.qualification_anchor,
                    black_holes_current: selected.black_holes_current,
                    qualification_override,
                    all_degraded_fallback,
                    candidates,
                    preparation,
                },
            ));
        }
        drop(preparation);
        Err(TcpPoolAdmissionRoundError::Contended)
    }

    #[cfg(test)]
    pub(super) fn qualification_for(
        &self,
        index: usize,
        active_before: u64,
        observation: TcpPoolPathObservation,
    ) -> TcpPoolQualificationDecision {
        self.qualification_for_admission(
            index,
            active_before,
            observation,
            AdmissionLockPolicy::LegacyBlocking,
        )
        .unwrap_or_else(|_| TcpPoolQualificationDecision::unknown(None, None))
    }

    fn qualification_for_admission(
        &self,
        index: usize,
        active_before: u64,
        observation: TcpPoolPathObservation,
        policy: AdmissionLockPolicy,
    ) -> Result<TcpPoolQualificationDecision, TcpPoolAdmissionRoundError> {
        let TcpPoolPathObservation::Known {
            identity,
            black_holes_detected,
            ..
        } = observation
        else {
            return Ok(TcpPoolQualificationDecision::unknown(None, None));
        };
        let Some(epoch) = self.qualification_epochs.get(index) else {
            return Ok(TcpPoolQualificationDecision::unknown(
                None,
                Some(black_holes_detected),
            ));
        };
        let Some(epoch) = lock_for_admission(epoch, policy)? else {
            return Ok(TcpPoolQualificationDecision::unknown(
                None,
                Some(black_holes_detected),
            ));
        };

        if active_before == 0 {
            return Ok(TcpPoolQualificationDecision {
                qualification: TcpPoolForwardQualification::Qualified,
                anchor: Some(black_holes_detected),
                current: Some(black_holes_detected),
            });
        }
        if epoch.identity != Some(identity) {
            return Ok(TcpPoolQualificationDecision::unknown(
                None,
                Some(black_holes_detected),
            ));
        }

        Ok(match black_holes_detected.cmp(&epoch.black_hole_anchor) {
            std::cmp::Ordering::Less => TcpPoolQualificationDecision::unknown(
                Some(epoch.black_hole_anchor),
                Some(black_holes_detected),
            ),
            std::cmp::Ordering::Equal => TcpPoolQualificationDecision {
                qualification: TcpPoolForwardQualification::Qualified,
                anchor: Some(epoch.black_hole_anchor),
                current: Some(black_holes_detected),
            },
            std::cmp::Ordering::Greater => TcpPoolQualificationDecision {
                qualification: TcpPoolForwardQualification::Degraded,
                anchor: Some(epoch.black_hole_anchor),
                current: Some(black_holes_detected),
            },
        })
    }

    pub(super) fn commit_idle_epoch(
        &self,
        index: usize,
        identity: TcpPoolTransportIdentity,
        black_hole_anchor: u64,
    ) {
        let Some(epoch) = self.qualification_epochs.get(index) else {
            return;
        };
        let Ok(Some(mut epoch)) = lock_for_admission(epoch, AdmissionLockPolicy::LegacyBlocking)
        else {
            return;
        };
        epoch.identity = Some(identity);
        epoch.black_hole_anchor = black_hole_anchor;
    }

    pub(super) fn current_generation_activity(
        &self,
        index: usize,
    ) -> Option<Arc<TcpPoolGenerationActivity>> {
        self.activity_for_admission(index, AdmissionLockPolicy::LegacyBlocking)
            .ok()
            .flatten()
    }

    fn activity_for_admission(
        &self,
        index: usize,
        policy: AdmissionLockPolicy,
    ) -> Result<Option<Arc<TcpPoolGenerationActivity>>, TcpPoolAdmissionRoundError> {
        let Some(activity) = self.current_generation_activity.get(index) else {
            return Ok(None);
        };
        Ok(lock_for_admission(activity, policy)?.map(|activity| activity.clone()))
    }
}

impl TcpPoolSlotLease {
    #[cfg(test)]
    pub(super) fn from_reserved(active: Arc<AtomicU64>) -> Self {
        Self {
            slot_active: active,
            pool_active_total: None,
            generation_active: None,
            release: None,
            drop_release: true,
        }
    }

    #[cfg(test)]
    pub(super) fn reserve_generation(
        generation_active: Arc<TcpPoolGenerationActivity>,
        slot_active: Arc<AtomicU64>,
        pool_active_total: Arc<AtomicU64>,
    ) -> Result<Self, TcpPoolReservationError> {
        generation_active.try_increment()?;
        if slot_active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            generation_active.decrement();
            return Err(TcpPoolReservationError::Saturated);
        }
        if pool_active_total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            slot_active.fetch_sub(1, Ordering::AcqRel);
            generation_active.decrement();
            return Err(TcpPoolReservationError::Saturated);
        }
        Ok(Self {
            slot_active,
            pool_active_total: Some(pool_active_total),
            generation_active: Some(generation_active),
            release: None,
            drop_release: true,
        })
    }

    pub(super) fn from_generation_reserved(
        generation_active: Arc<TcpPoolGenerationActivity>,
        slot_active: Arc<AtomicU64>,
        pool_active_total: Arc<AtomicU64>,
    ) -> Result<Self, TcpPoolReservationError> {
        if slot_active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            generation_active.decrement();
            return Err(TcpPoolReservationError::Saturated);
        }
        if pool_active_total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            slot_active.fetch_sub(1, Ordering::AcqRel);
            generation_active.decrement();
            return Err(TcpPoolReservationError::Saturated);
        }
        Ok(Self {
            slot_active,
            pool_active_total: Some(pool_active_total),
            generation_active: Some(generation_active),
            release: None,
            drop_release: true,
        })
    }

    pub(super) fn try_clone(&self) -> Result<Self, TcpPoolReservationError> {
        if self.release.is_some() || !self.drop_release {
            return Err(TcpPoolReservationError::LeaseNotActive);
        }
        if let Some(active) = &self.generation_active {
            active.try_increment()?;
        }
        if self
            .slot_active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_add(1)
            })
            .is_err()
        {
            if let Some(active) = &self.generation_active {
                active.decrement();
            }
            return Err(TcpPoolReservationError::Saturated);
        }
        if let Some(active) = &self.pool_active_total
            && active
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                    active.checked_add(1)
                })
                .is_err()
        {
            self.slot_active.fetch_sub(1, Ordering::AcqRel);
            if let Some(active) = &self.generation_active {
                active.decrement();
            }
            return Err(TcpPoolReservationError::Saturated);
        }
        Ok(Self {
            slot_active: self.slot_active.clone(),
            pool_active_total: self.pool_active_total.clone(),
            generation_active: self.generation_active.clone(),
            release: None,
            drop_release: true,
        })
    }

    #[cfg(test)]
    pub(super) fn reserve(active: Arc<AtomicU64>) -> (Self, bool) {
        match active.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => (Self::from_reserved(active), true),
            Err(_) => {
                active.fetch_add(1, Ordering::AcqRel);
                (Self::from_reserved(active), false)
            }
        }
    }
}

impl Drop for TcpPoolSlotLease {
    fn drop(&mut self) {
        if self.release.is_some() || !self.drop_release {
            return;
        }
        if let Some(active) = &self.generation_active {
            active.decrement();
        }
        let _ = self
            .slot_active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_sub(1)
            });
        if let Some(active) = &self.pool_active_total {
            let _ = active.fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                active.checked_sub(1)
            });
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CounterRelease {
    Absent,
    Pending,
    Decremented { before: u64, after: u64 },
    Underflow { observed: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LeaseReleaseReport {
    pub(super) generation: CounterRelease,
    pub(super) slot: CounterRelease,
    pub(super) total: CounterRelease,
}

impl LeaseReleaseReport {
    /// Only a full generation/slot/pool lease can certify all three debits.
    pub(super) fn complete(self) -> bool {
        [self.generation, self.slot, self.total]
            .iter()
            .all(|entry| matches!(entry, CounterRelease::Decremented { .. }))
    }
}

fn release_counter(counter: &AtomicU64) -> CounterRelease {
    match counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        value.checked_sub(1)
    }) {
        Ok(before) => CounterRelease::Decremented {
            before,
            after: before - 1,
        },
        Err(observed) => CounterRelease::Underflow { observed },
    }
}

impl TcpPoolSlotLease {
    pub(super) fn retain_until_release(&mut self) {
        self.drop_release = false;
    }

    /// Once begun, even an interrupted release cannot clone, retry or fall back to Drop.
    /// Records are written BEFORE the generation zero wake can call arbitrary waker code.
    pub(super) fn release_once(&mut self) -> LeaseReleaseReport {
        if let Some(report) = self.release {
            return report;
        }
        let pending = LeaseReleaseReport {
            generation: if self.generation_active.is_some() {
                CounterRelease::Pending
            } else {
                CounterRelease::Absent
            },
            slot: CounterRelease::Pending,
            total: if self.pool_active_total.is_some() {
                CounterRelease::Pending
            } else {
                CounterRelease::Absent
            },
        };
        self.release = Some(pending);
        if let Some(report) = self.release.as_mut() {
            if let Some(generation) = &self.generation_active {
                report.generation = release_counter(&generation.active);
                if matches!(
                    report.generation,
                    CounterRelease::Decremented { before: 1, .. }
                ) {
                    generation.zero.notify_one();
                }
            }
            report.slot = release_counter(&self.slot_active);
            if let Some(total) = &self.pool_active_total {
                report.total = release_counter(total);
            }
        }
        self.release.unwrap_or(pending)
    }
}

#[cfg(test)]
mod tests;

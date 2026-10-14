//! Unpublished ready-slot reservation and its exact owning failure exits.
//!
//! Owns the four-class metadata transaction, not a Controller, mailbox, Target
//! or physical session. Only the original index can reserve/reuse a position.
//! A scoped reservation must bind, return to its associated owner, explicitly
//! cancel, or remain in nonzero quarantine before the Root can park. Drop marks
//! the shared first fault and dirty epoch; it never frees, locks or notifies.
//! Entries: ReadyIndex::reserve_session/reenter_associated and consuming exits.
//! Focused lifecycle tests belong in ../tests/cold.rs; real binding lives at
//! the existing Controller/Executor and server-root construction seams.

use super::*;

impl OwnerReadySessionLimits {
    pub(in crate::owned_upstream) fn checked(
        max_flows: usize,
        max_opens: usize,
        control_capacity: usize,
        ordered_capacity: usize,
    ) -> Result<Self, OwnerReadySessionLimitsError> {
        let max_flows =
            NonZeroUsize::new(max_flows).ok_or(OwnerReadySessionLimitsError::ZeroFlows)?;
        let max_opens = NonZeroUsize::new(max_opens)
            .filter(|opens| opens.get() <= max_flows.get())
            .ok_or(OwnerReadySessionLimitsError::InvalidOpens)?;
        let control_capacity = NonZeroUsize::new(control_capacity)
            .ok_or(OwnerReadySessionLimitsError::ZeroControlCapacity)?;
        let ordered_capacity = NonZeroUsize::new(ordered_capacity)
            .ok_or(OwnerReadySessionLimitsError::ZeroOrderedCapacity)?;
        Ok(Self {
            max_flows,
            max_opens,
            control_capacity,
            ordered_capacity,
        })
    }
}

impl ReadyCell {
    fn reserved() -> Self {
        Self {
            phase: ReadyPhase::Reserved,
            revision: 0,
        }
    }

    fn is_unused(&self, phase: ReadyPhase) -> bool {
        self.phase == phase && self.revision == 0
    }
}

impl ReadySlot {
    fn is_unpublished(&self, class_phase: ReadyPhase) -> bool {
        self.local_word == 0
            && self.capacity_mask == 0
            && self.source_cursor == 0
            && self
                .sources
                .iter()
                .all(|cell| cell.is_unused(ReadyPhase::Dormant))
            && self
                .processes
                .iter()
                .all(|cell| cell.is_unused(class_phase))
    }

    fn identified_occupancy(&self) -> Result<Option<ReadyCorrelation>, ReadyFault> {
        match &self.occupancy {
            ReadyOccupancy::Free if self.is_unpublished(ReadyPhase::Dormant) => Ok(None),
            ReadyOccupancy::ColdReserved(correlation)
                if self.is_unpublished(ReadyPhase::Reserved) =>
            {
                Ok(Some(*correlation))
            }
            ReadyOccupancy::AssociatedPreparing(correlation)
            | ReadyOccupancy::BoundInactive(correlation, _)
            | ReadyOccupancy::BoundActive(correlation, _)
                if self
                    .processes
                    .iter()
                    .all(|cell| cell.phase != ReadyPhase::Dormant) =>
            {
                Ok(Some(*correlation))
            }
            _ => Err(ReadyFault::Corrupt),
        }
    }
}

impl ReadyTree {
    fn contains(&self, bit: usize) -> bool {
        let first = &self.levels[0];
        self.words[first.start + bit / WORD_BITS].load(Ordering::Acquire) & (1 << (bit % WORD_BITS))
            != 0
    }
}

impl ReadyIndex {
    pub(in crate::owned_upstream) fn check_prepared_resident(
        &self,
        slot: usize,
        session_id: SessionId,
    ) -> Result<(), ReadyFault> {
        self.check_root()?;
        let envelope = self
            .shared
            .slots
            .get(slot)
            .ok_or_else(|| self.fail_root(ReadyFault::Corrupt))?;
        let state = envelope
            .lock()
            .map_err(|_| self.fail_root(ReadyFault::Poisoned))?;
        if state.key.slot != slot
            || !matches!(&state.occupancy,
            ReadyOccupancy::BoundActive(correlation, _) if correlation.session_id == session_id)
        {
            return Err(self.fail_root(ReadyFault::Corrupt));
        }
        self.check_root()
    }

    /// Cold-only prefix check for sequential whole-root adoption. A pending
    /// build is a recoverable boundary rejection, never implicitly cancelled.
    /// Real resident identity is checked by its separate consuming adoption;
    /// this operation only forbids holes or an unrepresented occupied suffix.
    pub(in crate::owned_upstream) fn check_preparation_boundary(
        &self,
        residents: usize,
    ) -> Result<(), ReadyFault> {
        self.check_root()?;
        if residents > self.shared.slots.len() {
            return Err(self.fail_root(ReadyFault::Corrupt));
        }
        let mut pending = false;
        for (position, envelope) in self.shared.slots.iter().enumerate() {
            let state = envelope
                .lock()
                .map_err(|_| self.fail_root(ReadyFault::Poisoned))?;
            if state.key.slot != position
                || state.key.slot_generation == 0
                || state.key.scheduling_generation == 0
            {
                return Err(self.fail_root(ReadyFault::Corrupt));
            }
            state
                .identified_occupancy()
                .map_err(|fault| self.fail_root(fault))?;
            let publication = envelope.publication.load(Ordering::Acquire);
            if position < residents {
                if !matches!(state.occupancy, ReadyOccupancy::BoundActive(_, _)) {
                    return Err(self.fail_root(ReadyFault::Corrupt));
                }
                if publication & flow::PUBLICATION_ENABLED == 0
                    || publication & flow::PUBLICATION_ACTIVE != 0
                {
                    return Err(self.fail_root(ReadyFault::Corrupt));
                }
            } else {
                match &state.occupancy {
                    ReadyOccupancy::Free => {}
                    ReadyOccupancy::ColdReserved(_)
                    | ReadyOccupancy::AssociatedPreparing(_)
                    | ReadyOccupancy::BoundInactive(_, _) => pending = true,
                    _ => return Err(self.fail_root(ReadyFault::Corrupt)),
                }
                if publication != 0 || self.shared.trees.iter().any(|tree| tree.contains(position))
                {
                    return Err(self.fail_root(ReadyFault::Corrupt));
                }
            }
        }
        self.check_root()?;
        if pending {
            Err(ReadyFault::InvalidResident)
        } else {
            Ok(())
        }
    }

    pub(in crate::owned_upstream) fn reserve_session(
        &mut self,
        session_id: SessionId,
        limits: OwnerReadySessionLimits,
    ) -> Result<OwnerReadySessionReservation<'_>, OwnerReadyReservationError> {
        use OwnerReadyReservationError::{DuplicateSession, Full, Metadata};
        self.check_root().map_err(Metadata)?;
        let mut free = None;
        // Occupancy identity changes only with this exclusive index access.
        // Publishers may change ready work, never these IDs or generations.
        for (index, lock) in self.shared.slots.iter().enumerate() {
            let state = lock
                .lock()
                .map_err(|_| Metadata(self.fail_root(ReadyFault::Poisoned)))?;
            if state.key.slot != index
                || state.key.slot_generation == 0
                || state.key.scheduling_generation == 0
            {
                return Err(Metadata(self.fail_root(ReadyFault::Corrupt)));
            }
            match state
                .identified_occupancy()
                .map_err(|fault| Metadata(self.fail_root(fault)))?
            {
                Some(correlation) if correlation.session_id == session_id => {
                    return Err(DuplicateSession);
                }
                Some(_) => {}
                None => {
                    if self.shared.trees.iter().any(|tree| tree.contains(index)) {
                        return Err(Metadata(self.fail_root(ReadyFault::Corrupt)));
                    }
                    if free.is_none() {
                        free = Some(index);
                    }
                }
            }
        }
        let index = free.ok_or(Full)?;
        let correlation = ReadyCorrelation { session_id, limits };
        let mut custody = {
            let mut state = self.shared.slots[index]
                .lock()
                .map_err(|_| Metadata(self.fail_root(ReadyFault::Poisoned)))?;
            if !matches!(state.occupancy, ReadyOccupancy::Free)
                || !state.is_unpublished(ReadyPhase::Dormant)
            {
                return Err(Metadata(self.fail_root(ReadyFault::Corrupt)));
            }
            let custody = ReservationCustody {
                shared: Arc::clone(&self.shared),
                key: state.key,
                correlation,
                abandon_cold: false,
            };
            // Last fallible operation. No allocation, validation or notification
            // separates this RMW from publication of all four charges and custody.
            self.shared.failure.admit().map_err(Metadata)?;
            state.processes = std::array::from_fn(|_| ReadyCell::reserved());
            state.occupancy = ReadyOccupancy::ColdReserved(correlation);
            custody
        };
        custody.abandon_cold = true;
        Ok(OwnerReadySessionReservation {
            access: self,
            state: UnboundReservation::Cold(custody),
        })
    }

    pub(in crate::owned_upstream) fn reenter_associated(
        &mut self,
        associated: OwnerReadyAssociatedReservation,
    ) -> Result<
        OwnerReadySessionReservation<'_>,
        (OwnerReadyAssociatedReservation, OwnerReadyReservationError),
    > {
        let checked = self.validate_associated(&associated.custody);
        match checked {
            Ok(()) => Ok(OwnerReadySessionReservation {
                access: self,
                state: UnboundReservation::Associated(associated.custody),
            }),
            Err(cause) => Err((associated, cause)),
        }
    }

    fn validate_associated(
        &self,
        custody: &ReservationCustody,
    ) -> Result<(), OwnerReadyReservationError> {
        use OwnerReadyReservationError::{Metadata, NotAssociated, WrongDomain};
        if !Arc::ptr_eq(&self.shared, &custody.shared) {
            return Err(WrongDomain);
        }
        self.check_root().map_err(Metadata)?;
        let lock = self
            .shared
            .slots
            .get(custody.key.slot)
            .ok_or_else(|| Metadata(self.fail_root(ReadyFault::Stale)))?;
        let state = lock
            .lock()
            .map_err(|_| Metadata(self.fail_root(ReadyFault::Poisoned)))?;
        if state.key != custody.key {
            return Err(Metadata(self.fail_root(ReadyFault::Stale)));
        }
        if custody.abandon_cold {
            return Err(Metadata(self.fail_root(ReadyFault::Corrupt)));
        }
        match state.occupancy {
            ReadyOccupancy::AssociatedPreparing(correlation)
                if correlation == custody.correlation =>
            {
                state
                    .identified_occupancy()
                    .map_err(|fault| Metadata(self.fail_root(fault)))?;
                Ok(())
            }
            _ => Err(NotAssociated),
        }
    }
}

impl OwnerReadySessionBinding {
    pub(in crate::owned_upstream) fn compare_declared_session(
        &self,
        actual: SessionId,
    ) -> Result<(), OwnerReadyReservationError> {
        if self.custody.correlation.session_id == actual {
            Ok(())
        } else {
            Err(OwnerReadyReservationError::DeclaredSessionMismatch)
        }
    }

    /// Final metadata edge of whole-built-root adoption. The caller owns the
    /// physical/C eligibility preflight and reserved resident destination.
    /// Both are borrowed synchronously through this operation, not inferred
    /// from the scalar mask. No work or notification is performed here.
    pub(in crate::owned_upstream) fn activate(
        &mut self,
        index: &mut ReadyIndex,
        position: usize,
        tickets: &OwnerReadySessionTickets,
        eligible: usize,
    ) -> Result<OwnerReadyActivatedSession, OwnerReadyReservationError> {
        use OwnerReadyReservationError::{AlreadyActive, Metadata, WrongDomain, WrongPosition};
        let custody = &self.custody;
        if !Arc::ptr_eq(&custody.shared, &index.shared) {
            return Err(WrongDomain);
        }
        index.check_root().map_err(Metadata)?;
        if custody.key.slot != position {
            return Err(WrongPosition);
        }
        let (capacity, output) = tickets
            .prepare_activation_publishers(self)
            .map_err(|fault| custody.shared.cold_fault(fault))?;
        let envelope = index
            .shared
            .slots
            .get(position)
            .ok_or_else(|| custody.shared.cold_fault(ReadyFault::Corrupt))?;
        let mut state = envelope
            .lock()
            .map_err(|_| custody.shared.cold_fault(ReadyFault::Poisoned))?;
        if state.key != custody.key || custody.abandon_cold {
            return Err(custody.shared.cold_fault(ReadyFault::Stale));
        }
        if custody.key.slot_generation == 0 || custody.key.scheduling_generation == 0 {
            return Err(custody.shared.cold_fault(ReadyFault::Corrupt));
        }
        let backing = match &state.occupancy {
            ReadyOccupancy::BoundActive(correlation, _) if *correlation == custody.correlation => {
                if envelope.publication.load(Ordering::Acquire) & flow::PUBLICATION_ENABLED == 0 {
                    return Err(custody.shared.cold_fault(ReadyFault::Corrupt));
                }
                return Err(AlreadyActive);
            }
            ReadyOccupancy::BoundInactive(correlation, backing)
                if *correlation == custody.correlation =>
            {
                backing
            }
            _ => return Err(custody.shared.cold_fault(ReadyFault::Corrupt)),
        };
        let runnable = backing
            .activation_sources(eligible)
            .map_err(|fault| custody.shared.cold_fault(fault))?;
        if !state.is_unpublished(ReadyPhase::Reserved)
            || envelope.publication.load(Ordering::Acquire) != 0
            || index
                .shared
                .trees
                .iter()
                .any(|tree| tree.contains(position))
        {
            return Err(custody.shared.cold_fault(ReadyFault::Corrupt));
        }
        // Prepare every revision before the commit. Inactive due may have
        // changed since construction; runnable was read under this same lock.
        let mut revisions = [0; SOURCE_COUNT];
        for (source, revision) in revisions.iter_mut().enumerate() {
            if runnable & (1 << source) != 0 {
                *revision = state.sources[source]
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| custody.shared.cold_fault(ReadyFault::RevisionExhausted))?;
            }
        }
        let process_revision = if runnable == 0 {
            state.processes[SESSION_CLASS].revision
        } else {
            state.processes[SESSION_CLASS]
                .revision
                .checked_add(1)
                .ok_or_else(|| custody.shared.cold_fault(ReadyFault::RevisionExhausted))?
        };
        // Move the exact inactive payload only under exclusion. The only
        // following fallible operation is admission; its Err restores it.
        let (correlation, mut backing) =
            match std::mem::replace(&mut state.occupancy, ReadyOccupancy::Free) {
                ReadyOccupancy::BoundInactive(correlation, backing) => (correlation, backing),
                original => {
                    state.occupancy = original;
                    return Err(custody.shared.cold_fault(ReadyFault::Corrupt));
                }
            };
        if let Err(fault) = index.shared.failure.admit() {
            state.occupancy = ReadyOccupancy::BoundInactive(correlation, backing);
            return Err(Metadata(fault));
        }
        backing.enable_sources(eligible);
        state.occupancy = ReadyOccupancy::BoundActive(correlation, backing);
        for (source, revision) in revisions.into_iter().enumerate() {
            state.sources[source].revision = revision;
            state.sources[source].phase = if runnable & (1 << source) == 0 {
                ReadyPhase::Idle
            } else {
                ReadyPhase::Published
            };
        }
        state.local_word = runnable;
        state.processes[SESSION_CLASS].revision = process_revision;
        state.processes[SESSION_CLASS].phase = if runnable == 0 {
            ReadyPhase::Idle
        } else {
            ReadyPhase::Published
        };
        envelope
            .publication
            .fetch_or(flow::PUBLICATION_ENABLED, Ordering::AcqRel);
        if runnable != 0 {
            index.shared.trees[SESSION_CLASS].publish(position);
            index.shared.epoch.fetch_or(1, Ordering::Release);
        }
        Ok(OwnerReadyActivatedSession {
            session_id: correlation.session_id,
            capacity,
            output,
        })
    }
}

impl<'r> OwnerReadySessionReservation<'r> {
    pub(in crate::owned_upstream) fn bind_prepared(
        self,
        prepared: OwnerReadyPrepared,
    ) -> Result<OwnerReadyInstalled, (Self, OwnerReadyPrepared, OwnerReadyReservationError)> {
        use OwnerReadyReservationError::{Metadata, WrongDomain};
        // Borrow exclusion from this same domain independently of the owning
        // reservation, so rejection can return it whole without unlocking a
        // successful commit between admission and the metadata move.
        let shared = Arc::clone(&self.access.shared);
        let checked = (|| {
            let custody = match &self.state {
                UnboundReservation::Cold(custody) | UnboundReservation::Associated(custody) => {
                    custody
                }
            };
            if !Arc::ptr_eq(&shared, &custody.shared)
                || !prepared.shared.ptr_eq(&Arc::downgrade(&shared))
            {
                return Err(WrongDomain);
            }
            if prepared.key != custody.key
                || prepared.correlation != custody.correlation
                || prepared.mailbox_capacities
                    != [
                        custody.correlation.limits.control_capacity.get(),
                        custody.correlation.limits.ordered_capacity.get(),
                    ]
            {
                return Err(shared.cold_fault(ReadyFault::Stale));
            }
            let state = shared
                .slots
                .get(custody.key.slot)
                .ok_or_else(|| shared.cold_fault(ReadyFault::Stale))?
                .lock()
                .map_err(|_| shared.cold_fault(ReadyFault::Poisoned))?;
            if state.key != custody.key {
                return Err(shared.cold_fault(ReadyFault::Stale));
            }
            let valid = match (&self.state, &state.occupancy) {
                (UnboundReservation::Cold(_), ReadyOccupancy::ColdReserved(correlation)) => {
                    *correlation == custody.correlation
                        && custody.abandon_cold
                        && state.is_unpublished(ReadyPhase::Reserved)
                        && shared
                            .trees
                            .iter()
                            .all(|tree| !tree.contains(custody.key.slot))
                }
                (
                    UnboundReservation::Associated(_),
                    ReadyOccupancy::AssociatedPreparing(correlation),
                ) => {
                    *correlation == custody.correlation
                        && !custody.abandon_cold
                        && state.local_word == 0
                        && state.capacity_mask == 0
                        && state
                            .sources
                            .iter()
                            .all(|cell| cell.is_unused(ReadyPhase::Dormant))
                        && state.processes[SESSION_CLASS].is_unused(ReadyPhase::Reserved)
                        && !shared.trees[SESSION_CLASS].contains(custody.key.slot)
                        && state
                            .processes
                            .iter()
                            .all(|cell| cell.phase != ReadyPhase::Dormant)
                }
                _ => false,
            };
            if !valid {
                return Err(shared.cold_fault(ReadyFault::Corrupt));
            }
            Ok(state)
        })();
        let mut state = match checked {
            Ok(state) => state,
            Err(cause) => return Err((self, prepared, cause)),
        };
        // Final fallible operation. The caller already owns all real mailbox,
        // Flow backing and progress allocations. No source is published here.
        if let Err(fault) = shared.failure.admit() {
            drop(state);
            return Err((self, prepared, Metadata(fault)));
        }
        state.occupancy = ReadyOccupancy::BoundInactive(prepared.correlation, prepared.metadata);
        let mut custody = match self.state {
            UnboundReservation::Cold(custody) | UnboundReservation::Associated(custody) => custody,
        };
        custody.abandon_cold = false;
        Ok(OwnerReadyInstalled {
            tickets: prepared.tickets,
            session: prepared.session,
            binding: OwnerReadySessionBinding { custody },
        })
    }

    pub(in crate::owned_upstream) fn compare_declared_session(
        &self,
        actual: SessionId,
    ) -> Result<(), OwnerReadyReservationError> {
        let custody = match &self.state {
            UnboundReservation::Cold(custody) | UnboundReservation::Associated(custody) => custody,
        };
        if custody.correlation.session_id == actual {
            Ok(())
        } else {
            Err(OwnerReadyReservationError::DeclaredSessionMismatch)
        }
    }

    pub(in crate::owned_upstream) fn cancel(mut self) -> Result<(), OwnerReadyCancelFailure<'r>> {
        match self.cancel_unattached() {
            Ok(()) => Ok(()),
            Err(cause) => Err(OwnerReadyCancelFailure {
                reservation: self,
                cause,
            }),
        }
    }

    fn cancel_unattached(&mut self) -> Result<(), OwnerReadyReservationError> {
        use OwnerReadyReservationError::{NotUnattached, WrongDomain};
        let custody = match &mut self.state {
            UnboundReservation::Cold(custody) => custody,
            UnboundReservation::Associated(_) => return Err(NotUnattached),
        };
        if !Arc::ptr_eq(&self.access.shared, &custody.shared) {
            return Err(WrongDomain);
        }
        let lock = custody
            .shared
            .slots
            .get(custody.key.slot)
            .ok_or_else(|| custody.shared.cold_fault(ReadyFault::Stale))?;
        let mut state = lock
            .lock()
            .map_err(|_| custody.shared.cold_fault(ReadyFault::Poisoned))?;
        if state.key != custody.key {
            return Err(custody.shared.cold_fault(ReadyFault::Stale));
        }
        if !custody.abandon_cold
            || !matches!(state.occupancy, ReadyOccupancy::ColdReserved(correlation) if correlation == custody.correlation)
            || !state.is_unpublished(ReadyPhase::Reserved)
            || custody
                .shared
                .trees
                .iter()
                .any(|tree| tree.contains(custody.key.slot))
        {
            return Err(custody.shared.cold_fault(ReadyFault::Corrupt));
        }
        let generation = state.key.slot_generation.checked_add(1).ok_or_else(|| {
            custody
                .shared
                .cold_fault(ReadyFault::SlotGenerationExhausted)
        })?;
        // Settlement deliberately does not require an Open gate. The known-empty
        // Cold position can be returned without clearing any earlier first fault.
        custody.abandon_cold = false;
        state.processes = std::array::from_fn(|_| ReadyCell::dormant());
        state.key.slot_generation = generation;
        state.occupancy = ReadyOccupancy::Free;
        Ok(())
    }

    pub(in crate::owned_upstream) fn into_associated(
        self,
    ) -> Result<OwnerReadyAssociatedReservation, Self> {
        match self {
            Self {
                state: UnboundReservation::Associated(custody),
                ..
            } => Ok(OwnerReadyAssociatedReservation { custody }),
            cold => Err(cold),
        }
    }
}

impl<'r> OwnerReadyCancelFailure<'r> {
    pub(in crate::owned_upstream) fn source(&self) -> OwnerReadyReservationError {
        self.cause
    }

    pub(in crate::owned_upstream) fn into_parts(
        self,
    ) -> (OwnerReadySessionReservation<'r>, OwnerReadyReservationError) {
        (self.reservation, self.cause)
    }

    pub(in crate::owned_upstream) fn into_constructor_quarantine(
        self,
    ) -> OwnerReadyConstructorQuarantine {
        let Self {
            reservation: OwnerReadySessionReservation { mut state, .. },
            cause,
        } = self;
        let fault = match cause {
            OwnerReadyReservationError::Metadata(fault) => fault,
            _ => ReadyFault::Corrupt,
        };
        let (custody, cold) = match &mut state {
            UnboundReservation::Cold(custody) => (custody, true),
            UnboundReservation::Associated(custody) => (custody, false),
        };
        custody.shared.failure.fail(fault);
        custody.shared.epoch.fetch_or(1, Ordering::Release);
        if cold {
            custody.abandon_cold = false;
        }
        OwnerReadyConstructorQuarantine { state, cause }
    }
}

impl Drop for ReservationCustody {
    fn drop(&mut self) {
        if self.abandon_cold {
            self.shared
                .failure
                .fail(ReadyFault::ColdReservationAbandoned);
            self.shared.epoch.fetch_or(1, Ordering::Release);
        }
    }
}

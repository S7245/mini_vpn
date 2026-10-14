//! Bound publication protocol and the retained-work entry.
//!
//! Owns admission, deferred notification, finish and controlled Wake impls.
//! Does not own Flow/Source/Target/controller settlement; storage, metadata
//! algorithms and strict receipt validation stay in the parent.
//! Unfinished intervals stay sticky; notification follows physical guard exit.
//! Entries: request_retained_flow_work, begin_publication, turn finish methods.
//! Tests: private tests/retained.rs and owner_ready/tests/flow.rs.

use super::*;

#[cfg(test)]
mod tests;

impl OwnerReadySessionBinding {
    /// A real retained operation or released resource requests its original
    /// Flow. No ticket is manufactured for a source-less completion.
    pub(in crate::owned_upstream) fn request_retained_flow_work(
        &self,
        position: usize,
        expected: (OwnerReadyFlowAssociation, u64, bool),
        category: OwnerReadyCategory,
    ) -> Result<(), ReadyFault> {
        if self.custody.abandon_cold
            || self.custody.key.slot_generation == 0
            || self.custody.key.scheduling_generation == 0
        {
            return Err(self.defer_send_fault(ReadyFault::Corrupt));
        }
        self.custody.shared.publish_bound(
            self.custody.key,
            BoundRequest::Retained(
                self.custody.correlation,
                FlowWakeKey {
                    position,
                    association: expected.0,
                    generation: expected.1,
                },
                expected.2,
                category,
            ),
            false,
        );
        self.custody.shared.failure.check()
    }

    pub(in crate::owned_upstream) fn begin_publication(
        &self,
    ) -> Result<OwnerPublicationTurn, OwnerPublicationAdmissionFailure> {
        let custody = &self.custody;
        let admitted = (|| {
            custody.shared.failure.check()?;
            let envelope = custody
                .shared
                .slots
                .get(custody.key.slot)
                .ok_or(ReadyFault::Corrupt)?;
            let state = envelope.lock().map_err(|_| ReadyFault::Poisoned)?;
            if state.key != custody.key {
                return Err(ReadyFault::Stale);
            }
            if !matches!(&state.occupancy, ReadyOccupancy::BoundActive(c, _) if *c == custody.correlation)
                || custody.abandon_cold
                || custody.key.slot_generation == 0
                || custody.key.scheduling_generation == 0
                || !matches!(
                    state.processes[SESSION_CLASS].phase,
                    ReadyPhase::RunningClean | ReadyPhase::RunningDirty
                )
            {
                return Err(ReadyFault::Corrupt);
            }
            let word = envelope.publication.load(Ordering::Acquire);
            if word & !PUBLICATION_PENDING != PUBLICATION_ENABLED {
                return Err(ReadyFault::Corrupt);
            }
            // Only this single-owner operation sets ACTIVE. Publishers may OR
            // PENDING concurrently, so preserve it rather than doing a store.
            custody.shared.failure.admit()?;
            envelope
                .publication
                .fetch_or(PUBLICATION_ACTIVE, Ordering::AcqRel);
            Ok(())
        })();
        admitted.map_err(|fault| {
            let fault = custody.shared.failure.fail(fault);
            custody.shared.epoch.fetch_or(1, Ordering::Release);
            OwnerPublicationAdmissionFailure {
                fault,
                disposition: OwnerPublicationAdmissionDisposition::NotEntered,
            }
        })?;
        Ok(OwnerPublicationTurn {
            shared: Arc::clone(&custody.shared),
            key: custody.key,
            correlation: custody.correlation,
        })
    }
}

impl OwnerPublicationTurn {
    /// Owning physical quarantine must precede this operation. Metadata must
    /// already describe that same quarantined resident. No successful result,
    /// clear-to-Idle transition or completed work is manufactured here.
    pub(in crate::owned_upstream) fn finish_quarantined(self) -> Result<(), ReadyFault> {
        let envelope = self
            .shared
            .slots
            .get(self.key.slot)
            .ok_or(ReadyFault::Corrupt)?;
        let checked = {
            let state = envelope.lock().map_err(|_| ReadyFault::Poisoned)?;
            if state.key != self.key {
                return Err(ReadyFault::Stale);
            }
            matches!(&state.occupancy, ReadyOccupancy::BoundActive(c, _) if *c == self.correlation)
                && state.processes[SESSION_CLASS].phase == ReadyPhase::Quarantined
                && envelope.publication.load(Ordering::Acquire) & !PUBLICATION_PENDING
                    == PUBLICATION_ENABLED | PUBLICATION_ACTIVE
        };
        if !checked {
            return Err(self.shared.failure.fail(ReadyFault::Corrupt));
        }
        let before = envelope
            .publication
            .fetch_and(PUBLICATION_ENABLED, Ordering::AcqRel);
        if before & PUBLICATION_ENABLED != 0 && before & PUBLICATION_PENDING != 0 {
            self.shared.notify_bound();
        }
        self.shared.failure.check()
    }

    /// Called only after physical guards return and Root stores the completed
    /// value beside its original claim. No business disposition moves here.
    pub(in crate::owned_upstream) fn finish<'a>(
        self,
        completed: &'a SessionCommandVisit,
    ) -> Result<(), OwnerPublicationFinishFailure<'a>> {
        let outcome = (|| {
            let envelope = self
                .shared
                .slots
                .get(self.key.slot)
                .ok_or(ReadyFault::Corrupt)?;
            // A healthy stale key must never end another occupant's interval.
            // Poison cannot be recovered: this admitted generation is retained
            // and cannot retire/reuse while Root holds its unfinished claim.
            let checked = match envelope.lock() {
                Ok(state) => {
                    if state.key != self.key {
                        return Err(ReadyFault::Stale);
                    }
                    if matches!(&state.occupancy, ReadyOccupancy::BoundActive(c, _) if *c == self.correlation)
                        && matches!(
                            state.processes[SESSION_CLASS].phase,
                            ReadyPhase::RunningClean | ReadyPhase::RunningDirty
                        )
                        && envelope.publication.load(Ordering::Acquire) & !PUBLICATION_PENDING
                            == PUBLICATION_ENABLED | PUBLICATION_ACTIVE
                    {
                        Ok(())
                    } else {
                        Err(ReadyFault::Corrupt)
                    }
                }
                Err(poisoned) => {
                    drop(poisoned);
                    Err(ReadyFault::Poisoned)
                }
            };
            if let Err(fault) = checked {
                self.shared.failure.fail(fault);
                self.shared.epoch.fetch_or(1, Ordering::Release);
                envelope
                    .publication
                    .fetch_or(PUBLICATION_PENDING, Ordering::AcqRel);
            }
            // No mutex/actor/source guard survives to this edge. Do not skip
            // completed work merely because another publisher faulted Root.
            let before = envelope
                .publication
                .fetch_and(PUBLICATION_ENABLED, Ordering::AcqRel);
            if before & PUBLICATION_ENABLED != 0 && before & PUBLICATION_PENDING != 0 {
                self.shared.notify_bound();
            }
            self.shared.failure.check()
        })();
        outcome.map_err(|fault| {
            let fault = self.shared.failure.fail(fault);
            self.shared.epoch.fetch_or(1, Ordering::Release);
            OwnerPublicationFinishFailure { fault, completed }
        })
    }
}

// Only a live Bound occupancy uses this protocol. Prototype's existing native
// callback tests retain their original publish_capacity behavior unchanged.
impl ReadyShared {
    fn publish_bound(&self, key: ReadyKey, request: BoundRequest, external: bool) {
        let owning = matches!(request, BoundRequest::Retained(..));
        let Some(envelope) = self.slots.get(key.slot) else {
            if owning {
                self.failure.fail(ReadyFault::Corrupt);
                self.epoch.fetch_or(1, Ordering::Release);
            }
            return;
        };
        let publication = (|| {
            if owning {
                self.failure.check()?;
            }
            let mut state = envelope.lock().map_err(|_| ReadyFault::Poisoned)?;
            if state.key != key {
                return if owning {
                    Err(ReadyFault::Stale)
                } else {
                    Ok(false)
                };
            }
            let capacity_mask = state.capacity_mask;
            let active = matches!(state.occupancy, ReadyOccupancy::BoundActive(..));
            let publication = envelope.publication.load(Ordering::Acquire);
            if active != (publication & PUBLICATION_ENABLED != 0) {
                return Err(ReadyFault::Corrupt);
            }
            if owning
                && (publication & !PUBLICATION_PENDING
                    != if active {
                        PUBLICATION_ENABLED | PUBLICATION_ACTIVE
                    } else {
                        0
                    }
                    || if active {
                        !matches!(
                            state.processes[SESSION_CLASS].phase,
                            ReadyPhase::RunningClean | ReadyPhase::RunningDirty
                        )
                    } else {
                        state.local_word != 0
                            || state.capacity_mask != 0
                            || state.source_cursor != 0
                            || state
                                .sources
                                .iter()
                                .any(|cell| cell.phase != ReadyPhase::Dormant || cell.revision != 0)
                            || state.processes.iter().any(|cell| {
                                cell.phase != ReadyPhase::Reserved || cell.revision != 0
                            })
                    })
            {
                return Err(ReadyFault::Corrupt);
            }
            if owning && !active {
                // An inactive slot cannot already be runnable at Root. Inspect
                // the fixed four hierarchies, not a scan over resident Flows.
                for tree in &self.trees {
                    if tree.select(key.slot)? == Some(key.slot) {
                        return Err(ReadyFault::Corrupt);
                    }
                }
            }
            if state.processes[SESSION_CLASS].phase == ReadyPhase::Quarantined {
                // Late advisory hints do not reopen work at a quarantined owner.
                if owning
                    || !active
                    || state.local_word != 0
                    || state.capacity_mask != 0
                    || state
                        .sources
                        .iter()
                        .chain(state.processes.iter())
                        .any(|cell| {
                            !matches!(cell.phase, ReadyPhase::Dormant | ReadyPhase::Quarantined)
                        })
                {
                    return Err(ReadyFault::Corrupt);
                }
                return Ok(false);
            }
            let (correlation, metadata) = match &mut state.occupancy {
                ReadyOccupancy::BoundInactive(c, metadata)
                | ReadyOccupancy::BoundActive(c, metadata) => (c, metadata),
                _ => {
                    return if owning {
                        Err(ReadyFault::Corrupt)
                    } else {
                        Ok(false)
                    };
                }
            };
            let mut retained = None;
            let sources = match request {
                BoundRequest::Flow(flow, kind) => match metadata.request_flow(flow, kind)? {
                    Some(sources) => sources,
                    None => return Ok(false),
                },
                BoundRequest::Session(kind) => metadata.request_session(kind, capacity_mask)?,
                BoundRequest::Retained(expected, flow, retiring, category) => {
                    if *correlation != expected
                        || metadata.slots.len() != expected.limits.max_flows.get()
                        || (!active && metadata.eligible_sources != 0)
                        || !matches!(
                            category,
                            OwnerReadyCategory::Completion | OwnerReadyCategory::Retirement
                        )
                    {
                        return Err(ReadyFault::Corrupt);
                    }
                    let record = match (metadata.slots.get(flow.position), retiring) {
                        (Some(FlowReadySlot::Live(record)), false)
                        | (Some(FlowReadySlot::Retiring(record)), true) => record,
                        _ => return Err(ReadyFault::Corrupt),
                    };
                    if record.association != flow.association
                        || record.generation != flow.generation
                        || flow.generation == 0
                    {
                        return Err(ReadyFault::Corrupt);
                    }
                    let retirement = retiring || category == OwnerReadyCategory::Retirement;
                    let kind = if retirement { 10 } else { 9 };
                    let mut revisions = record.revisions;
                    revisions[kind] = revisions[kind]
                        .checked_add(1)
                        .ok_or(ReadyFault::RevisionExhausted)?;
                    // This is only derived Flow metadata, never a Source/lease copy.
                    // Keep the original intact until every publication check passes.
                    let next = FlowReadyRecord {
                        association: record.association,
                        generation: record.generation,
                        due: (if retirement && !retiring {
                            0
                        } else {
                            record.due
                        }) | (1 << kind),
                        revisions,
                        credit: record.credit,
                        read_phase: if retirement {
                            ReadPhase::Closed
                        } else {
                            record.read_phase
                        },
                    };
                    let next = if retirement {
                        FlowReadySlot::Retiring(next)
                    } else {
                        FlowReadySlot::Live(next)
                    };
                    retained = Some((flow.position, kind, retirement && !retiring, next));
                    1 << CONTINUATION_SOURCE
                }
            };
            let eligible = if active {
                sources & metadata.eligible_sources
            } else {
                0
            };
            if owning && eligible != 0 {
                // Preflight the original cells; their transition remains publish().
                for cell in state
                    .sources
                    .iter()
                    .enumerate()
                    .filter(|(source, _)| eligible & (1 << source) != 0)
                    .map(|(_, cell)| cell)
                    .chain(std::iter::once(&state.processes[SESSION_CLASS]))
                {
                    if matches!(
                        cell.phase,
                        ReadyPhase::Dormant | ReadyPhase::Reserved | ReadyPhase::Quarantined
                    ) {
                        return Err(ReadyFault::Corrupt);
                    }
                    cell.revision
                        .checked_add(1)
                        .ok_or(ReadyFault::RevisionExhausted)?;
                }
            }
            // Order admission against the shared first-fault RMW before writes.
            // No Source, lease or ReadyCell is copied into the metadata plan.
            if owning {
                self.failure.admit()?;
            }
            if eligible != 0 {
                for source in 0..SOURCE_COUNT {
                    if eligible & (1 << source) != 0 {
                        state.sources[source].publish()?;
                        state.local_word |= 1 << source;
                    }
                }
                state.processes[SESSION_CLASS].publish()?;
            }
            if let Some((position, kind, clear, next)) = retained {
                // Same exclusion: no callback or fallibility after this commit.
                let metadata = match &mut state.occupancy {
                    ReadyOccupancy::BoundInactive(_, metadata)
                    | ReadyOccupancy::BoundActive(_, metadata) => metadata,
                    _ => return Err(ReadyFault::Corrupt),
                };
                if clear {
                    for tree in &metadata.trees {
                        tree.clear(position);
                    }
                }
                metadata.slots[position] = next;
                metadata.trees[kind].publish(position);
            }
            if eligible == 0 {
                return Ok(false);
            }
            self.trees[SESSION_CLASS].publish(key.slot);
            Ok(true)
        })();
        let faulted = match publication {
            Ok(false) => return,
            Ok(true) => false,
            Err(fault) => {
                self.failure.fail(fault);
                true
            }
        };
        self.epoch.fetch_or(1, Ordering::Release);
        let before = envelope
            .publication
            .fetch_or(PUBLICATION_PENDING, Ordering::AcqRel);
        // Faults and hints share the original deferred-notification handshake.
        if before & PUBLICATION_ENABLED != 0 {
            if faulted {
                self.trees[SESSION_CLASS].publish(key.slot);
            }
            if external && before & PUBLICATION_ACTIVE == 0 {
                self.notify_bound();
            }
        }
    }

    fn notify_bound(&self) {
        struct NotificationGuard<'a> {
            shared: &'a ReadyShared,
            returned: bool,
        }
        impl Drop for NotificationGuard<'_> {
            fn drop(&mut self) {
                if !self.returned {
                    self.shared.failure.fail(ReadyFault::NotificationUnwind);
                    self.shared.epoch.fetch_or(1, Ordering::Release);
                }
            }
        }
        let mut guard = NotificationGuard {
            shared: self,
            returned: false,
        };
        self.notify.notify_waiters();
        guard.returned = true;
    }
}

enum BoundRequest {
    Retained(ReadyCorrelation, FlowWakeKey, bool, OwnerReadyCategory),
    Flow(FlowWakeKey, usize),
    Session(SessionWakeKind),
}

impl Wake for FlowWake {
    fn wake(self: Arc<Self>) {
        let shared = self.shared.upgrade();
        let (key, flow, kind, external) = (self.key, self.flow, self.kind, self.external);
        drop(self);
        if let Some(shared) = shared {
            shared.publish_bound(key, BoundRequest::Flow(flow, kind), external);
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(shared) = self.shared.upgrade() {
            shared.publish_bound(
                self.key,
                BoundRequest::Flow(self.flow, self.kind),
                self.external,
            );
        }
    }
}

impl Wake for SessionWake {
    fn wake(self: Arc<Self>) {
        let shared = self.shared.upgrade();
        let (key, kind) = (self.key, self.kind);
        drop(self);
        if let Some(shared) = shared {
            shared.publish_bound(
                key,
                BoundRequest::Session(kind),
                matches!(
                    kind,
                    SessionWakeKind::TargetCommon | SessionWakeKind::Output
                ),
            );
        }
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(shared) = self.shared.upgrade() {
            shared.publish_bound(
                self.key,
                BoundRequest::Session(self.kind),
                matches!(
                    self.kind,
                    SessionWakeKind::TargetCommon | SessionWakeKind::Output
                ),
            );
        }
    }
}

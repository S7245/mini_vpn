//! Flow metadata, immutable controlled publishers and publication intervals.
//!
//! Owns only correlation, due/credit/revision state and fixed hierarchies. The
//! Executor retains every actual source, Target, command and completion. Fresh
//! preparation never polls or publishes; binding consumes it into the original
//! reserved slot. Publisher callbacks preserve generations and never reclaim a
//! Flow. Owner-local callbacks defer notification even on poisoned metadata.
//! Admitted visits defer external Notify until all physical guards have exited;
//! operation unwind leaves ACTIVE sticky, never a synthetic completed value.
//! Entries: prepare_flow_backing and binding metadata operations; publication
//! Implementation lives in private publication. Tests: ../tests/flow.rs.
//! Physical quarantine stays at its real owner.

use super::*;

mod publication;

const FLOW_KINDS: usize = 11;
const CONTINUATION_SOURCE: usize = 4;
pub(super) const PUBLICATION_ACTIVE: u8 = 1;
const PUBLICATION_PENDING: u8 = 2;
// Set only by successful activation commit, under slot exclusion.
// Preserved by finish/quarantine; it is not evidence of health or eligibility.
pub(super) const PUBLICATION_ENABLED: u8 = 4;

#[derive(Clone, Copy, PartialEq, Eq)]
enum CreditPhase {
    Available,
    Blocked,
    RetryDue,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadPhase {
    AwaitingReadiness,
    Hint,
    Suspended,
    Closed,
}

impl ReadPhase {
    fn accepts_work(self) -> bool {
        matches!(self, Self::AwaitingReadiness | Self::Hint)
    }
}

struct FlowReadyRecord {
    association: OwnerReadyFlowAssociation,
    generation: u64,
    due: usize,
    revisions: [u64; FLOW_KINDS],
    credit: CreditPhase,
    read_phase: ReadPhase,
}

enum FlowReadySlot {
    // Exhaustion is sticky; a later admission cannot reset the old generation.
    Free { next_generation: Option<u64> },
    Live(FlowReadyRecord),
    Retiring(FlowReadyRecord),
}

pub(super) struct FlowReadyBacking {
    slots: Vec<FlowReadySlot>,
    trees: [ReadyTree; FLOW_KINDS],
    cursors: [usize; FLOW_KINDS],
    category_cursor: usize,
    session_due: usize,
    session_revisions: [u64; 4],
    direct_due: usize,
    direct_revisions: [u64; SOURCE_COUNT],
    credit_pass_revision: u64,
    credit_pass_completed: u64,
    credit_pass_cursor: usize,
    eligible_sources: usize,
}

pub(super) struct FlowWake {
    shared: Weak<ReadyShared>,
    key: ReadyKey,
    flow: FlowWakeKey,
    kind: usize,
    external: bool,
}

#[derive(Clone, Copy)]
struct FlowWakeKey {
    position: usize,
    association: OwnerReadyFlowAssociation,
    generation: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionWakeKind {
    TargetCommon,
    Credit,
    ControlInbox,
    OrderedInbox,
    Capacity,
    Output,
}

pub(super) struct SessionWake {
    shared: Weak<ReadyShared>,
    key: ReadyKey,
    kind: SessionWakeKind,
}

// Pinned std ArcInner has two atomic counters before the actual payload.
// Check that concrete allocation, not merely the size of an Arc reference.
pub(super) fn arc_layout<T>() -> Result<Layout, ReadyFault> {
    Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<T>())
        .map(|(layout, _)| layout.pad_to_align())
        .map_err(|_| ReadyFault::Layout)
}

fn check_layout(max_flows: usize) -> Result<(), ReadyFault> {
    let words = ReadyTree::checked_word_count(max_flows)?;
    let trees = words.checked_mul(FLOW_KINDS).ok_or(ReadyFault::Layout)?;
    let directories = WORD_BITS
        .checked_mul(FLOW_KINDS)
        .ok_or(ReadyFault::Layout)?;
    let publishers = max_flows.checked_mul(5).ok_or(ReadyFault::Layout)?;
    let pieces = [
        Layout::array::<FlowReadySlot>(max_flows).map_err(|_| ReadyFault::Layout)?,
        Layout::array::<AtomicUsize>(trees).map_err(|_| ReadyFault::Layout)?,
        Layout::array::<std::ops::Range<usize>>(directories).map_err(|_| ReadyFault::Layout)?,
        Layout::array::<OwnerReadyFlowTickets>(max_flows).map_err(|_| ReadyFault::Layout)?,
        Layout::array::<OwnerReadyFlowImport>(max_flows).map_err(|_| ReadyFault::Layout)?,
        Layout::from_size_align(
            arc_layout::<FlowWake>()?
                .size()
                .checked_mul(publishers)
                .ok_or(ReadyFault::Layout)?,
            arc_layout::<FlowWake>()?.align(),
        )
        .map_err(|_| ReadyFault::Layout)?,
        Layout::from_size_align(
            arc_layout::<SessionWake>()?
                .size()
                .checked_mul(6)
                .ok_or(ReadyFault::Layout)?,
            arc_layout::<SessionWake>()?.align(),
        )
        .map_err(|_| ReadyFault::Layout)?,
    ];
    let mut peak = Layout::new::<OwnerReadyPrepared>();
    for piece in pieces {
        peak = peak.extend(piece).map_err(|_| ReadyFault::Layout)?.0;
    }
    Ok(())
}

impl OwnerReadyFlowAssociation {
    pub(in crate::owned_upstream) fn flow_id(self) -> SessionFlowId {
        match self {
            Self::Source(flow) => flow.flow_id(),
            Self::NoSource { flow_id, .. } => flow_id,
        }
    }
}

impl OwnerReadySessionReservation<'_> {
    pub(in crate::owned_upstream) fn prepare_flow_backing(
        &self,
        max_flows: usize,
        imports: &[OwnerReadyFlowImport],
    ) -> Result<OwnerReadyPrepared, OwnerReadyReservationError> {
        use OwnerReadyReservationError::{DeclaredLimitsMismatch, Metadata, WrongDomain};
        let custody = match &self.state {
            UnboundReservation::Cold(custody) | UnboundReservation::Associated(custody) => custody,
        };
        if !Arc::ptr_eq(&custody.shared, &self.access.shared) {
            return Err(WrongDomain);
        }
        self.access.check_root().map_err(Metadata)?;
        if custody.correlation.limits.max_flows.get() != max_flows
            || imports.len() > max_flows
            || imports.iter().filter(|import| import.pending_open).count()
                > custody.correlation.limits.max_opens.get()
        {
            return Err(DeclaredLimitsMismatch);
        }
        check_layout(max_flows).map_err(Metadata)?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(max_flows)
            .map_err(|_| Metadata(ReadyFault::Reserve))?;
        let mut tickets = Vec::new();
        tickets
            .try_reserve_exact(imports.len())
            .map_err(|_| Metadata(ReadyFault::Reserve))?;
        let trees = (|| -> Result<_, ReadyFault> {
            Ok([
                ReadyTree::new(max_flows)?,
                ReadyTree::new(max_flows)?,
                ReadyTree::new(max_flows)?,
                ReadyTree::new(max_flows)?,
                ReadyTree::new(max_flows)?,
                ReadyTree::new(max_flows)?,
                ReadyTree::new(max_flows)?,
                ReadyTree::new(max_flows)?,
                ReadyTree::new(max_flows)?,
                ReadyTree::new(max_flows)?,
                ReadyTree::new(max_flows)?,
            ])
        })()
        .map_err(Metadata)?;
        let mut previous = None;
        for (position, import) in imports.iter().enumerate() {
            let flow_id = import.association.flow_id();
            if previous.is_some_and(|old| old >= flow_id)
                || matches!(import.association, OwnerReadyFlowAssociation::Source(flow)
                    if flow.session_id() != custody.correlation.session_id)
                || (!import.target_present
                    && (import.read_interest || import.write_interest || import.join_interest))
            {
                return Err(Metadata(ReadyFault::Corrupt));
            }
            previous = Some(flow_id);
            let has_source = matches!(import.association, OwnerReadyFlowAssociation::Source(_));
            let requested = [
                has_source,
                has_source,
                false,
                import.read_interest && !import.completion && !import.retiring,
                false,
                import.write_interest,
                false,
                import.join_interest,
                false,
                import.completion,
                import.retiring,
            ];
            let mut due = 0;
            let mut revisions = [0; FLOW_KINDS];
            for (kind, requested) in requested.into_iter().enumerate() {
                if requested {
                    due |= 1 << kind;
                    revisions[kind] = 1;
                    // Retiring still owes any imported source probe, but it
                    // cannot re-enter ordinary DATA service. Its existing
                    // retirement category retains that drain obligation.
                    if !import.retiring || kind >= 2 {
                        trees[kind].publish(position);
                    }
                }
            }
            let record = FlowReadyRecord {
                association: import.association,
                generation: 1,
                due,
                revisions,
                credit: CreditPhase::Available,
                read_phase: if !import.read_interest || import.retiring {
                    ReadPhase::Closed
                } else if import.completion {
                    ReadPhase::Suspended
                } else {
                    ReadPhase::AwaitingReadiness
                },
            };
            slots.push(if import.retiring {
                FlowReadySlot::Retiring(record)
            } else {
                FlowReadySlot::Live(record)
            });
            let publisher = |kind, external| {
                Arc::new(FlowWake {
                    shared: Arc::downgrade(&custody.shared),
                    key: custody.key,
                    flow: FlowWakeKey {
                        position,
                        association: import.association,
                        generation: 1,
                    },
                    kind,
                    external,
                })
            };
            tickets.push(OwnerReadyFlowTickets {
                control: has_source.then(|| publisher(0, false)),
                ordered: has_source.then(|| publisher(1, false)),
                read: import.target_present.then(|| publisher(3, true)),
                write: import.target_present.then(|| publisher(5, true)),
                join: import.target_present.then(|| publisher(7, true)),
            });
        }
        slots.resize_with(max_flows, || FlowReadySlot::Free {
            next_generation: Some(1),
        });
        let session_publisher = |kind| {
            Arc::new(SessionWake {
                shared: Arc::downgrade(&custody.shared),
                key: custody.key,
                kind,
            })
        };
        Ok(OwnerReadyPrepared {
            shared: Arc::downgrade(&custody.shared),
            key: custody.key,
            correlation: custody.correlation,
            metadata: FlowReadyBacking {
                slots,
                trees,
                cursors: [0; FLOW_KINDS],
                category_cursor: 0,
                // Register the real common notifier before a named Target wait.
                session_due: 1 << 3,
                session_revisions: [0, 0, 0, 1],
                direct_due: 0,
                direct_revisions: [0; SOURCE_COUNT],
                credit_pass_revision: 0,
                credit_pass_completed: 0,
                credit_pass_cursor: 0,
                eligible_sources: 0,
            },
            tickets,
            session: OwnerReadySessionTickets {
                target_common: session_publisher(SessionWakeKind::TargetCommon),
                credit: session_publisher(SessionWakeKind::Credit),
                control_inbox: session_publisher(SessionWakeKind::ControlInbox),
                ordered_inbox: session_publisher(SessionWakeKind::OrderedInbox),
                capacity: session_publisher(SessionWakeKind::Capacity),
                output: session_publisher(SessionWakeKind::Output),
            },
            mailbox_capacities: [
                custody.correlation.limits.control_capacity.get(),
                custody.correlation.limits.ordered_capacity.get(),
            ],
        })
    }
}

impl FlowReadyBacking {
    pub(super) fn activation_sources(&self, eligible: usize) -> Result<usize, ReadyFault> {
        let all_sources = (1 << SOURCE_COUNT) - 1;
        if eligible & !all_sources != 0
            || self.direct_due & !all_sources != 0
            || self.session_due & !0b1111 != 0
            || self.eligible_sources != 0
        {
            return Err(ReadyFault::Corrupt);
        }
        let mut requested = self.direct_due;
        for source in 0..2 {
            if !self.trees[source].is_empty() {
                requested |= 1 << source;
            }
        }
        if self.session_due != 0
            || self.credit_pass_revision != self.credit_pass_completed
            || self.trees[2..].iter().any(|tree| !tree.is_empty())
        {
            requested |= 1 << CONTINUATION_SOURCE;
        }
        Ok(requested & eligible)
    }

    pub(super) fn enable_sources(&mut self, eligible: usize) {
        self.eligible_sources = eligible;
    }

    fn request_flow(
        &mut self,
        flow: FlowWakeKey,
        kind: usize,
    ) -> Result<Option<usize>, ReadyFault> {
        let (record, kind) = match self.slots.get_mut(flow.position) {
            Some(FlowReadySlot::Live(record)) => (record, kind),
            // A same-generation late wake may reconsider actual retirement,
            // never reopen DATA/read/send work or imply that references are gone.
            Some(FlowReadySlot::Retiring(record)) => (record, 10),
            _ => return Ok(None),
        };
        if record.association != flow.association || record.generation != flow.generation {
            return Ok(None);
        }
        if (2..=4).contains(&kind) && !record.read_phase.accepts_work() {
            return Ok(None);
        }
        let revision = record.revisions.get_mut(kind).ok_or(ReadyFault::Corrupt)?;
        *revision = revision
            .checked_add(1)
            .ok_or(ReadyFault::RevisionExhausted)?;
        record.due |= 1 << kind;
        self.trees[kind].publish(flow.position);
        Ok(Some(1 << if kind < 2 { kind } else { CONTINUATION_SOURCE }))
    }

    fn request_session(
        &mut self,
        kind: SessionWakeKind,
        capacity_mask: usize,
    ) -> Result<usize, ReadyFault> {
        let source = match kind {
            SessionWakeKind::ControlInbox => 1 << 2,
            SessionWakeKind::OrderedInbox => 1 << 3,
            SessionWakeKind::Capacity => capacity_mask,
            SessionWakeKind::Credit => {
                // Release requests a bounded pass over blocked credit records;
                // it is not the unrelated Controller recovery category.
                self.credit_pass_revision = self
                    .credit_pass_revision
                    .checked_add(1)
                    .ok_or(ReadyFault::RevisionExhausted)?;
                1 << CONTINUATION_SOURCE
            }
            SessionWakeKind::TargetCommon | SessionWakeKind::Output => {
                let category = match kind {
                    SessionWakeKind::Output => 0,
                    _ => 3,
                };
                self.session_revisions[category] = self.session_revisions[category]
                    .checked_add(1)
                    .ok_or(ReadyFault::RevisionExhausted)?;
                self.session_due |= 1 << category;
                1 << CONTINUATION_SOURCE
            }
        };
        for index in 0..SOURCE_COUNT {
            if source & (1 << index) != 0 {
                self.direct_revisions[index] = self.direct_revisions[index]
                    .checked_add(1)
                    .ok_or(ReadyFault::RevisionExhausted)?;
            }
        }
        self.direct_due |= source;
        Ok(source)
    }
}

impl OwnerReadySessionTickets {
    pub(in crate::owned_upstream) fn target_common_waker(&self) -> Waker {
        Waker::from(Arc::clone(&self.target_common))
    }
    pub(in crate::owned_upstream) fn capacity_waker(&self) -> Waker {
        Waker::from(Arc::clone(&self.capacity))
    }

    pub(in crate::owned_upstream) fn inbox_waker(
        &self,
        lane: crate::owned_upstream::session::SessionMailboxLane,
    ) -> Waker {
        use crate::owned_upstream::session::SessionMailboxLane;
        Waker::from(Arc::clone(match lane {
            SessionMailboxLane::Control => &self.control_inbox,
            SessionMailboxLane::OrderedSource => &self.ordered_inbox,
        }))
    }

    pub(super) fn prepare_activation_publishers(
        &self,
        binding: &OwnerReadySessionBinding,
    ) -> Result<(Waker, Waker), ReadyFault> {
        let custody = &binding.custody;
        let expected = Arc::downgrade(&custody.shared);
        for (publisher, kind) in [
            (&self.target_common, SessionWakeKind::TargetCommon),
            (&self.credit, SessionWakeKind::Credit),
            (&self.control_inbox, SessionWakeKind::ControlInbox),
            (&self.ordered_inbox, SessionWakeKind::OrderedInbox),
            (&self.capacity, SessionWakeKind::Capacity),
            (&self.output, SessionWakeKind::Output),
        ] {
            if !Weak::ptr_eq(&publisher.shared, &expected)
                || publisher.key != custody.key
                || publisher.kind != kind
            {
                return Err(ReadyFault::Corrupt);
            }
        }
        Ok((
            Waker::from(Arc::clone(&self.capacity)),
            Waker::from(Arc::clone(&self.output)),
        ))
    }
}

impl OwnerReadyFlowTickets {
    pub(in crate::owned_upstream) fn target_waker(
        &self,
        category: OwnerReadyCategory,
    ) -> Result<Waker, ReadyFault> {
        let publisher = match category {
            OwnerReadyCategory::ReadReady => &self.read,
            OwnerReadyCategory::WriteReady => &self.write,
            OwnerReadyCategory::JoinReady => &self.join,
            _ => return Err(ReadyFault::Corrupt),
        };
        publisher
            .as_ref()
            .map(|wake| Waker::from(Arc::clone(wake)))
            .ok_or(ReadyFault::Corrupt)
    }

    pub(in crate::owned_upstream) fn driver_waker(
        &self,
        lane: SendLane,
    ) -> Result<Waker, ReadyFault> {
        let publisher = match lane {
            SendLane::Control => &self.control,
            SendLane::OrderedSource => &self.ordered,
        };
        publisher
            .as_ref()
            .map(|wake| Waker::from(Arc::clone(wake)))
            .ok_or(ReadyFault::Corrupt)
    }
}

impl OwnerReadySessionBinding {
    pub(in crate::owned_upstream) fn select_continuation(
        &self,
        eligible: usize,
        requested: usize,
    ) -> Result<Option<OwnerReadyContinuationSelection>, ReadyFault> {
        let result = (|| {
            self.custody.shared.failure.check()?;
            if eligible & !0x1fff != 0 || requested & !0xf != 0 {
                return Err(ReadyFault::Corrupt);
            }
            let envelope = self
                .custody
                .shared
                .slots
                .get(self.custody.key.slot)
                .ok_or(ReadyFault::Corrupt)?;
            let mut state = envelope.lock().map_err(|_| ReadyFault::Poisoned)?;
            if state.key != self.custody.key
                || envelope.publication.load(Ordering::Acquire) & !PUBLICATION_PENDING
                    != PUBLICATION_ENABLED | PUBLICATION_ACTIVE
                || !matches!(
                    state.sources[CONTINUATION_SOURCE].phase,
                    ReadyPhase::RunningClean | ReadyPhase::RunningDirty
                )
            {
                return Err(ReadyFault::Corrupt);
            }
            let ReadyOccupancy::BoundActive(c, backing) = &mut state.occupancy else {
                return Err(ReadyFault::Corrupt);
            };
            if *c != self.custody.correlation {
                return Err(ReadyFault::Corrupt);
            }
            for category in 0..4 {
                if requested & (1 << category) != 0 && backing.session_due & (1 << category) == 0 {
                    backing.session_revisions[category] = backing.session_revisions[category]
                        .checked_add(1)
                        .ok_or(ReadyFault::RevisionExhausted)?;
                    backing.session_due |= 1 << category;
                }
            }
            for offset in 0..13 {
                let category = (backing.category_cursor + offset) % 13;
                if eligible & (1 << category) == 0 {
                    continue;
                }
                let candidate = if category < 4 {
                    if backing.session_due & (1 << category) == 0 {
                        continue;
                    }
                    None
                } else {
                    let kind = category - 2;
                    let Some(position) = backing.trees[kind].select(backing.cursors[kind])? else {
                        continue;
                    };
                    let Some(FlowReadySlot::Live(record) | FlowReadySlot::Retiring(record)) =
                        backing.slots.get(position)
                    else {
                        return Err(ReadyFault::Corrupt);
                    };
                    if record.due & (1 << kind) == 0 || record.generation == 0 {
                        return Err(ReadyFault::Corrupt);
                    }
                    let candidate = OwnerReadyDriverCandidate {
                        position,
                        association: record.association,
                        generation: record.generation,
                        revision: record.revisions[kind],
                    };
                    backing.cursors[kind] = if position + 1 == backing.slots.len() {
                        0
                    } else {
                        position + 1
                    };
                    Some(candidate)
                };
                backing.category_cursor = (category + 1) % 13;
                return Ok(Some(OwnerReadyContinuationSelection {
                    domain: Arc::downgrade(&self.custody.shared),
                    key: self.custody.key,
                    category: OwnerReadyCategory::ALL[category],
                    candidate,
                    session_revision: if category < 4 {
                        backing.session_revisions[category]
                    } else {
                        0
                    },
                }));
            }
            Ok(None)
        })();
        result.map_err(|fault| self.defer_send_fault(fault))
    }

    pub(in crate::owned_upstream) fn finish_continuation(
        &self,
        selection: &OwnerReadyContinuationSelection,
        result: OwnerReadyReadResult,
        eligible: usize,
    ) -> Result<usize, ReadyFault> {
        let outcome = (|| {
            if selection.key != self.custody.key
                || !selection
                    .domain
                    .ptr_eq(&Arc::downgrade(&self.custody.shared))
            {
                return Err(ReadyFault::Corrupt);
            }
            let envelope = self
                .custody
                .shared
                .slots
                .get(self.custody.key.slot)
                .ok_or(ReadyFault::Corrupt)?;
            let mut state = envelope.lock().map_err(|_| ReadyFault::Poisoned)?;
            if state.key != selection.key
                || envelope.publication.load(Ordering::Acquire) & !PUBLICATION_PENDING
                    != PUBLICATION_ENABLED | PUBLICATION_ACTIVE
            {
                return Err(ReadyFault::Corrupt);
            }
            let ReadyOccupancy::BoundActive(c, backing) = &mut state.occupancy else {
                return Err(ReadyFault::Corrupt);
            };
            if *c != self.custody.correlation {
                return Err(ReadyFault::Corrupt);
            }
            let category = selection.category as usize;
            if let Some(selected) = &selection.candidate {
                let kind = category.checked_sub(2).ok_or(ReadyFault::Corrupt)?;
                if matches!(result, OwnerReadyReadResult::RetirementReleased)
                    && (selection.category != OwnerReadyCategory::Retirement
                        || !matches!(
                            backing.slots.get(selected.position),
                            Some(FlowReadySlot::Retiring(_))
                        ))
                {
                    return Err(ReadyFault::Corrupt);
                }
                let Some(FlowReadySlot::Live(record) | FlowReadySlot::Retiring(record)) =
                    backing.slots.get_mut(selected.position)
                else {
                    return Err(ReadyFault::Corrupt);
                };
                if record.association != selected.association
                    || record.generation != selected.generation
                {
                    return Err(ReadyFault::Corrupt);
                }
                if record.revisions[kind] == selected.revision {
                    record.due &= !(1 << kind);
                    backing.trees[kind].clear(selected.position);
                }
                let request = match (selection.category, result) {
                    (
                        OwnerReadyCategory::Retirement,
                        OwnerReadyReadResult::Hint | OwnerReadyReadResult::CommandHeld(_),
                    ) => Some(10),
                    _ if (2..=4).contains(&kind) && !record.read_phase.accepts_work() => None,
                    (OwnerReadyCategory::ReadReady, OwnerReadyReadResult::Hint) => {
                        record.read_phase = ReadPhase::Hint;
                        (record.credit != CreditPhase::Blocked).then_some(4)
                    }
                    (OwnerReadyCategory::ReadAttempt, OwnerReadyReadResult::CreditBlocked) => {
                        record.credit = CreditPhase::Blocked;
                        None
                    }
                    (
                        OwnerReadyCategory::ReadAttempt,
                        OwnerReadyReadResult::Data | OwnerReadyReadResult::Pending,
                    ) => {
                        record.read_phase = ReadPhase::AwaitingReadiness;
                        record.credit = CreditPhase::Available;
                        Some(3)
                    }
                    (OwnerReadyCategory::ReadAttempt, OwnerReadyReadResult::Eof) => {
                        record.read_phase = ReadPhase::Closed;
                        for read_kind in 2..=4 {
                            record.due &= !(1 << read_kind);
                            backing.trees[read_kind].clear(selected.position);
                        }
                        None
                    }
                    (OwnerReadyCategory::Credit, OwnerReadyReadResult::Hint) => {
                        record.credit = CreditPhase::Available;
                        (record.read_phase == ReadPhase::Hint).then_some(4)
                    }
                    _ => None,
                };
                if let Some(next) = request {
                    record.revisions[next] = record.revisions[next]
                        .checked_add(1)
                        .ok_or(ReadyFault::RevisionExhausted)?;
                    record.due |= 1 << next;
                    backing.trees[next].publish(selected.position);
                }
                if matches!(result, OwnerReadyReadResult::RetirementReleased) {
                    let next_generation = record.generation.checked_add(1);
                    for tree in &backing.trees {
                        tree.clear(selected.position);
                    }
                    // Late in-flight publishers retain the immutable old key;
                    // uniqueness alone was not used as publication quiescence.
                    backing.slots[selected.position] = FlowReadySlot::Free { next_generation };
                }
            } else if category < 4
                && !matches!(result, OwnerReadyReadResult::Hint)
                && backing.session_revisions[category] == selection.session_revision
            {
                backing.session_due &= !(1 << category);
            }
            let flow_due = (4..13).any(|category| {
                eligible & (1 << category) != 0 && !backing.trees[category - 2].is_empty()
            });
            let command = if let OwnerReadyReadResult::CommandHeld(lane) = result {
                1 << lane.index()
            } else {
                0
            };
            Ok((command
                | if (backing.session_due & eligible) != 0 || flow_due {
                    1 << CONTINUATION_SOURCE
                } else {
                    0
                })
                & backing.eligible_sources)
        })();
        outcome.map_err(|fault| self.defer_send_fault(fault))
    }

    fn active_send_slot(
        &self,
        lane: SendLane,
    ) -> Result<std::sync::MutexGuard<'_, ReadySlot>, ReadyFault> {
        let custody = &self.custody;
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
            || envelope.publication.load(Ordering::Acquire) & !PUBLICATION_PENDING
                != PUBLICATION_ENABLED | PUBLICATION_ACTIVE
            || !matches!(
                state.processes[SESSION_CLASS].phase,
                ReadyPhase::RunningClean | ReadyPhase::RunningDirty
            )
            || !matches!(
                state.sources[lane.index()].phase,
                ReadyPhase::RunningClean | ReadyPhase::RunningDirty
            )
        {
            return Err(ReadyFault::Corrupt);
        }
        Ok(state)
    }

    fn defer_send_fault(&self, fault: ReadyFault) -> ReadyFault {
        let first = self.custody.shared.failure.fail(fault);
        self.custody.shared.epoch.fetch_or(1, Ordering::Release);
        if let Some(envelope) = self.custody.shared.slots.get(self.custody.key.slot) {
            envelope
                .publication
                .fetch_or(PUBLICATION_PENDING, Ordering::AcqRel);
        }
        first
    }

    pub(in crate::owned_upstream) fn select_driver_source(
        &self,
        lane: SendLane,
        vacant: bool,
    ) -> Result<OwnerReadySendSelection, ReadyFault> {
        let result = (|| {
            self.custody.shared.failure.check()?;
            let mut state = self.active_send_slot(lane)?;
            let ReadyOccupancy::BoundActive(_, backing) = &mut state.occupancy else {
                return Err(ReadyFault::Corrupt);
            };
            let kind = lane.index();
            let candidate = if vacant {
                match backing.trees[kind].select(backing.cursors[kind])? {
                    None => None,
                    Some(position) => {
                        let Some(FlowReadySlot::Live(record)) = backing.slots.get(position) else {
                            return Err(ReadyFault::Corrupt);
                        };
                        if record.generation == 0
                            || record.due & (1 << kind) == 0
                            || !matches!(record.association, OwnerReadyFlowAssociation::Source(_))
                        {
                            return Err(ReadyFault::Corrupt);
                        }
                        let selected = OwnerReadyDriverCandidate {
                            position,
                            association: record.association,
                            generation: record.generation,
                            revision: record.revisions[kind],
                        };
                        // Fairness advances before the owning Driver operation,
                        // even when that operation returns Pending or unwinds.
                        backing.cursors[kind] = if position + 1 == backing.slots.len() {
                            0
                        } else {
                            position + 1
                        };
                        Some(selected)
                    }
                }
            } else {
                None
            };
            Ok(OwnerReadySendSelection {
                domain: Arc::downgrade(&self.custody.shared),
                key: self.custody.key,
                lane,
                direct_revision: backing.direct_revisions[kind],
                candidate,
            })
        })();
        result.map_err(|fault| self.defer_send_fault(fault))
    }

    // Called after the actual command disposition is root-held. A later first
    // fault must not suppress bookkeeping for this already-admitted operation.
    pub(in crate::owned_upstream) fn finish_driver_source(
        &self,
        selection: &OwnerReadySendSelection,
        pulled: bool,
        vacant: usize,
    ) -> Result<usize, ReadyFault> {
        let result = (|| {
            if selection.key != self.custody.key
                || !selection
                    .domain
                    .ptr_eq(&Arc::downgrade(&self.custody.shared))
                || vacant & !0b11 != 0
            {
                return Err(ReadyFault::Corrupt);
            }
            let mut state = self.active_send_slot(selection.lane)?;
            let ReadyOccupancy::BoundActive(_, backing) = &mut state.occupancy else {
                return Err(ReadyFault::Corrupt);
            };
            let kind = selection.lane.index();
            let mut credit_due = false;
            if let Some(selected) = &selection.candidate {
                let Some(FlowReadySlot::Live(record)) = backing.slots.get_mut(selected.position)
                else {
                    return Err(ReadyFault::Corrupt);
                };
                if record.association != selected.association
                    || record.generation != selected.generation
                {
                    return Err(ReadyFault::Corrupt);
                }
                if !pulled && record.revisions[kind] == selected.revision {
                    record.due &= !(1 << kind);
                    backing.trees[kind].clear(selected.position);
                }
                // Only a real ordered dequeue releases a source FIFO slot.
                // It requests a later read-credit attempt, never a read here.
                if pulled
                    && selection.lane == SendLane::OrderedSource
                    && record.credit == CreditPhase::Blocked
                    && record.read_phase.accepts_work()
                {
                    record.revisions[2] = record.revisions[2]
                        .checked_add(1)
                        .ok_or(ReadyFault::RevisionExhausted)?;
                    record.credit = CreditPhase::RetryDue;
                    record.due |= 1 << 2;
                    backing.trees[2].publish(selected.position);
                    credit_due = true;
                }
            } else if pulled {
                return Err(ReadyFault::Corrupt);
            }
            if backing.direct_revisions[kind] == selection.direct_revision {
                backing.direct_due &= !(1 << kind);
            }
            let mut demand = backing.direct_due & 0b11;
            for source in 0..2 {
                if !backing.trees[source].is_empty() {
                    demand |= 1 << source;
                }
            }
            Ok(((demand & vacant)
                | if credit_due {
                    1 << CONTINUATION_SOURCE
                } else {
                    0
                })
                & backing.eligible_sources)
        })();
        result.map_err(|fault| self.defer_send_fault(fault))
    }
}

impl OwnerReadySessionBinding {
    /// Runtime receipt preflight is deliberately distinct from cold adoption.
    /// It requires the same active publication interval and actual live claim;
    /// callers receive no lock, backing reference, or reusable admission right.
    pub(in crate::owned_upstream) fn validate_receipt_flow(
        &self,
        capacity: usize,
        position: usize,
        expected: (OwnerReadyFlowAssociation, u64, bool),
    ) -> Result<(), ReadyFault> {
        let custody = &self.custody;
        let result = (|| {
            custody.shared.failure.check()?;
            if capacity != custody.correlation.limits.max_flows.get() {
                return Err(ReadyFault::Corrupt);
            }
            let envelope = custody
                .shared
                .slots
                .get(custody.key.slot)
                .ok_or(ReadyFault::Corrupt)?;
            let state = envelope.lock().map_err(|_| ReadyFault::Poisoned)?;
            if state.key != custody.key {
                return Err(ReadyFault::Stale);
            }
            let ReadyOccupancy::BoundActive(correlation, backing) = &state.occupancy else {
                return Err(ReadyFault::Corrupt);
            };
            if *correlation != custody.correlation
                || backing.slots.len() != capacity
                || envelope.publication.load(Ordering::Acquire) & !PUBLICATION_PENDING
                    != PUBLICATION_ENABLED | PUBLICATION_ACTIVE
                || !matches!(
                    state.processes[SESSION_CLASS].phase,
                    ReadyPhase::RunningClean | ReadyPhase::RunningDirty
                )
            {
                return Err(ReadyFault::Corrupt);
            }
            match (backing.slots.get(position), expected) {
                (Some(FlowReadySlot::Live(record)), (association, generation, false))
                | (Some(FlowReadySlot::Retiring(record)), (association, generation, true))
                    if record.association == association
                        && record.generation == generation
                        && generation != 0 =>
                {
                    Ok(())
                }
                _ => Err(ReadyFault::Corrupt),
            }
        })();
        result.map_err(|fault| {
            let first = custody.shared.failure.fail(fault);
            custody.shared.epoch.fetch_or(1, Ordering::Release);
            // This is an owner-local borrowed operation; notification remains
            // deferred until its original physical transaction has ended.
            if let Some(envelope) = custody.shared.slots.get(custody.key.slot) {
                envelope
                    .publication
                    .fetch_or(PUBLICATION_PENDING, Ordering::AcqRel);
            }
            first
        })
    }

    /// The actual Flow has retained completion work. Close only its derived
    /// read candidates; immutable publishers and all business custody remain.
    pub(in crate::owned_upstream) fn suspend_flow_reads(
        &self,
        capacity: usize,
        position: usize,
        expected: (OwnerReadyFlowAssociation, u64, bool),
    ) -> Result<(), ReadyFault> {
        let result = (|| {
            let custody = &self.custody;
            custody.shared.failure.check()?;
            if capacity != custody.correlation.limits.max_flows.get() {
                return Err(ReadyFault::Corrupt);
            }
            let envelope = custody
                .shared
                .slots
                .get(custody.key.slot)
                .ok_or(ReadyFault::Corrupt)?;
            let mut state = envelope.lock().map_err(|_| ReadyFault::Poisoned)?;
            custody.shared.failure.check()?;
            if state.key != custody.key {
                return Err(ReadyFault::Stale);
            }
            let active = matches!(state.occupancy, ReadyOccupancy::BoundActive(..));
            let publication = envelope.publication.load(Ordering::Acquire) & !PUBLICATION_PENDING;
            if active {
                if publication != PUBLICATION_ENABLED | PUBLICATION_ACTIVE
                    || !matches!(
                        state.processes[SESSION_CLASS].phase,
                        ReadyPhase::RunningClean | ReadyPhase::RunningDirty
                    )
                {
                    return Err(ReadyFault::Corrupt);
                }
            } else {
                // Cold terminal settlement changes only this Flow's derived
                // candidates. No Root work may already exist or be published.
                if publication != 0
                    || state.local_word != 0
                    || state.capacity_mask != 0
                    || state.source_cursor != 0
                    || state
                        .sources
                        .iter()
                        .any(|cell| cell.phase != ReadyPhase::Dormant || cell.revision != 0)
                    || state
                        .processes
                        .iter()
                        .any(|cell| cell.phase != ReadyPhase::Reserved || cell.revision != 0)
                {
                    return Err(ReadyFault::Corrupt);
                }
                for tree in &custody.shared.trees {
                    if tree.select(custody.key.slot)? == Some(custody.key.slot) {
                        return Err(ReadyFault::Corrupt);
                    }
                }
            }
            let (correlation, backing) = match &mut state.occupancy {
                ReadyOccupancy::BoundInactive(correlation, backing)
                | ReadyOccupancy::BoundActive(correlation, backing) => (correlation, backing),
                _ => return Err(ReadyFault::Corrupt),
            };
            if *correlation != custody.correlation || backing.slots.len() != capacity {
                return Err(ReadyFault::Corrupt);
            }
            let record = match (backing.slots.get_mut(position), expected) {
                (Some(FlowReadySlot::Live(record)), (association, generation, false))
                | (Some(FlowReadySlot::Retiring(record)), (association, generation, true))
                    if record.association == association
                        && record.generation == generation
                        && generation != 0 =>
                {
                    record
                }
                _ => return Err(ReadyFault::Corrupt),
            };
            if record.read_phase != ReadPhase::Closed {
                record.read_phase = ReadPhase::Suspended;
            }
            for kind in 2..=4 {
                record.due &= !(1 << kind);
                backing.trees[kind].clear(position);
            }
            Ok(())
        })();
        result.map_err(|fault| self.defer_send_fault(fault))
    }

    /// Cold bidirectional position check against the caller's actual owner
    /// record. This returns no metadata or reusable identity certificate.
    pub(in crate::owned_upstream) fn compare_flow_position(
        &self,
        capacity: usize,
        position: usize,
        expected: Option<(OwnerReadyFlowAssociation, u64, bool)>,
    ) -> Result<(), OwnerReadyReservationError> {
        let custody = &self.custody;
        if capacity != custody.correlation.limits.max_flows.get() {
            return Err(OwnerReadyReservationError::DeclaredLimitsMismatch);
        }
        let slot = custody
            .shared
            .slots
            .get(custody.key.slot)
            .ok_or_else(|| custody.shared.cold_fault(ReadyFault::Corrupt))?;
        let state = slot
            .lock()
            .map_err(|_| custody.shared.cold_fault(ReadyFault::Poisoned))?;
        if state.key != custody.key {
            return Err(custody.shared.cold_fault(ReadyFault::Stale));
        }
        let active = matches!(state.occupancy, ReadyOccupancy::BoundActive(..));
        let enabled = slot.publication.load(Ordering::Acquire) & PUBLICATION_ENABLED != 0;
        if active != enabled {
            return Err(custody.shared.cold_fault(ReadyFault::Corrupt));
        }
        let (correlation, backing) = match &state.occupancy {
            ReadyOccupancy::BoundInactive(correlation, backing) => (correlation, backing),
            ReadyOccupancy::BoundActive(correlation, _) if *correlation == custody.correlation => {
                return Err(OwnerReadyReservationError::AlreadyActive);
            }
            _ => return Err(custody.shared.cold_fault(ReadyFault::Corrupt)),
        };
        if *correlation != custody.correlation || backing.slots.len() != capacity {
            return Err(custody.shared.cold_fault(ReadyFault::Corrupt));
        }
        let valid = match (backing.slots.get(position), expected) {
            (Some(FlowReadySlot::Free { .. }), None) => true,
            (Some(FlowReadySlot::Live(record)), Some((association, generation, false)))
            | (Some(FlowReadySlot::Retiring(record)), Some((association, generation, true))) => {
                record.association == association
                    && record.generation == generation
                    && generation != 0
            }
            _ => false,
        };
        if !valid {
            return Err(custody.shared.cold_fault(ReadyFault::Corrupt));
        }
        custody
            .shared
            .failure
            .check()
            .map_err(OwnerReadyReservationError::Metadata)
    }
}

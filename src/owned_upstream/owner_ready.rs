//! Neutral ready metadata, checked geometry and bounded selection/publication.
//!
//! Owns cells, trees, correlation, the single failure gate and ready operations.
//! Root retains the sole mutable selector, admitted visit and actual park waiter;
//! publishers retain only Weak metadata, never commands, Driver, Target or leases.
//! Publication sets leaf/ancestors/epoch before notifying outside exclusion;
//! claim/finish retain original generation, revision and first-fault ordering.
//! Geometry owns immutable N-dependent capacities, not outer budgets or identity.
//! Cold reservation owns four unpublished class destinations in this same index;
//! cancellation returns metadata only. Bound installation/activation is separate.
//! Entries: ReadyGeometry::checked, SendCursor, ReadyIndex and its operations.
//! Private tests/ owns fixed metadata recipes. Real Root scenarios remain in
//! quinn_leg_composition/owner/server_root/tests/{ready_scheduler,ready_dispatch}.

use crate::resumable::effect_storage::{EffectStorageError, allocate_exact_vec};
use crate::resumable::{LocalFlow, SessionFlowId, SessionId};
use std::num::NonZeroUsize;

mod cold;
mod flow;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerReadyFlowAssociation {
    Source(LocalFlow),
    NoSource {
        flow_id: SessionFlowId,
        phase: OwnerReadyNoSourcePhase,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerReadyNoSourcePhase {
    OpenCompletion,
    TargetTerminal,
    RetainedCompletion,
    PendingFinishEffect,
    Retirement,
}

// Cold import is a non-owning description of the original retained Flow.
// It never supplies a Driver, Target, permit, registration or fabricated lease.
pub(super) struct OwnerReadyFlowImport {
    pub(super) association: OwnerReadyFlowAssociation,
    pub(super) target_present: bool,
    pub(super) read_interest: bool,
    pub(super) write_interest: bool,
    pub(super) join_interest: bool,
    pub(super) completion: bool,
    pub(super) pending_open: bool,
    pub(super) retiring: bool,
}

pub(super) struct OwnerReadyFlowTickets {
    control: Option<Arc<flow::FlowWake>>,
    ordered: Option<Arc<flow::FlowWake>>,
    read: Option<Arc<flow::FlowWake>>,
    write: Option<Arc<flow::FlowWake>>,
    join: Option<Arc<flow::FlowWake>>,
}

pub(super) struct OwnerReadySessionTickets {
    target_common: Arc<flow::SessionWake>,
    credit: Arc<flow::SessionWake>,
    control_inbox: Arc<flow::SessionWake>,
    ordered_inbox: Arc<flow::SessionWake>,
    capacity: Arc<flow::SessionWake>,
    output: Arc<flow::SessionWake>,
}

impl OwnerReadyFlowTickets {
    pub(in crate::owned_upstream) fn retirement_references_are_unique(&self) -> bool {
        [
            &self.control,
            &self.ordered,
            &self.read,
            &self.write,
            &self.join,
        ]
        .into_iter()
        .flatten()
        .all(|ticket| Arc::strong_count(ticket) == 1)
    }
}

// A non-owning selection of one original Driver position. Root retains its
// separate actual ReadyClaim; neither record owns a source or permits.
pub(super) struct OwnerReadyDriverCandidate {
    pub(super) position: usize,
    pub(super) association: OwnerReadyFlowAssociation,
    pub(super) generation: u64,
    revision: u64,
}

pub(super) struct OwnerReadySendSelection {
    domain: Weak<ReadyShared>,
    key: ReadyKey,
    lane: SendLane,
    direct_revision: u64,
    pub(super) candidate: Option<OwnerReadyDriverCandidate>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerReadyCategory {
    Output,
    Effects,
    Recovery,
    TargetCommon,
    Credit,
    ReadReady,
    ReadAttempt,
    WriteReady,
    WriteAttempt,
    JoinReady,
    JoinAttempt,
    Completion,
    Retirement,
}

impl OwnerReadyCategory {
    const ALL: [Self; 13] = [
        Self::Output,
        Self::Effects,
        Self::Recovery,
        Self::TargetCommon,
        Self::Credit,
        Self::ReadReady,
        Self::ReadAttempt,
        Self::WriteReady,
        Self::WriteAttempt,
        Self::JoinReady,
        Self::JoinAttempt,
        Self::Completion,
        Self::Retirement,
    ];
    pub(super) fn mask(self) -> usize {
        1 << self as usize
    }
}

pub(super) struct OwnerReadyContinuationSelection {
    domain: Weak<ReadyShared>,
    key: ReadyKey,
    pub(super) category: OwnerReadyCategory,
    pub(super) candidate: Option<OwnerReadyDriverCandidate>,
    session_revision: u64,
}

#[derive(Clone, Copy)]
pub(super) enum OwnerReadyReadResult {
    Hint,
    Pending,
    Data,
    Eof,
    CreditBlocked,
    FailureStaged,
    CommandHeld(SendLane),
    RetirementReleased,
}

// Unpublished preparation owns the actual metadata and immutable publishers.
// Only the matching original reservation can install it; it is not a proof.
pub(super) struct OwnerReadyPrepared {
    shared: Weak<ReadyShared>,
    key: ReadyKey,
    correlation: ReadyCorrelation,
    metadata: flow::FlowReadyBacking,
    tickets: Vec<OwnerReadyFlowTickets>,
    session: OwnerReadySessionTickets,
    pub(super) mailbox_capacities: [usize; 2],
}

pub(super) struct OwnerReadyInstalled {
    pub(super) tickets: Vec<OwnerReadyFlowTickets>,
    pub(super) session: OwnerReadySessionTickets,
    pub(super) binding: OwnerReadySessionBinding,
}

/// Immutable configuration, never an identity or a registration capability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct OwnerReadySessionLimits {
    max_flows: NonZeroUsize,
    max_opens: NonZeroUsize,
    control_capacity: NonZeroUsize,
    ordered_capacity: NonZeroUsize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerReadySessionLimitsError {
    ZeroFlows,
    InvalidOpens,
    ZeroControlCapacity,
    ZeroOrderedCapacity,
}

pub(super) struct OwnerReadySessionReservation<'r> {
    access: &'r mut ReadyIndex,
    state: UnboundReservation,
}

enum UnboundReservation {
    Cold(ReservationCustody),
    Associated(ReservationCustody),
}

pub(super) struct OwnerReadyAssociatedReservation {
    custody: ReservationCustody,
}

pub(super) struct OwnerReadySessionBinding {
    custody: ReservationCustody,
}

// Non-Clone sequencing right, not an actor/command/claim owner. Dropping it
// leaves the interval sticky; the original physical owner must quarantine.
pub(super) struct OwnerPublicationTurn {
    shared: Arc<ReadyShared>,
    key: ReadyKey,
    correlation: ReadyCorrelation,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum OwnerPublicationAdmissionDisposition {
    NotEntered,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct OwnerPublicationAdmissionFailure {
    pub(super) fault: ReadyFault,
    pub(super) disposition: OwnerPublicationAdmissionDisposition,
}

// Borrow the actual completed value already stored beside Root's sole claim.
// A notifying finish may unwind: it must never take that value out of Root.
pub(super) struct OwnerPublicationFinishFailure<'a> {
    pub(super) fault: ReadyFault,
    pub(super) completed: &'a SessionCommandVisit,
}

// Successful activation returns only the original correlation and controlled
// publishers. It exports neither metadata exclusion nor any business owner.
pub(super) struct OwnerReadyActivatedSession {
    pub(super) session_id: SessionId,
    pub(super) capacity: Waker,
    pub(super) output: Waker,
}

// This is the only Drop-bearing object in the reservation/return family. It
// moves whole; the wrapper never extracts or duplicates its operation right.
struct ReservationCustody {
    shared: Arc<ReadyShared>,
    key: ReadyKey,
    correlation: ReadyCorrelation,
    abandon_cold: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct ReadyCorrelation {
    session_id: SessionId,
    limits: OwnerReadySessionLimits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerReadyReservationError {
    Metadata(ReadyFault),
    Full,
    DuplicateSession,
    WrongDomain,
    NotUnattached,
    NotAssociated,
    DeclaredSessionMismatch,
    DeclaredLimitsMismatch,
    WrongPosition,
    AlreadyActive,
}

pub(super) struct OwnerReadyCancelFailure<'r> {
    reservation: OwnerReadySessionReservation<'r>,
    cause: OwnerReadyReservationError,
}

pub(super) struct OwnerReadyConstructorQuarantine {
    state: UnboundReservation,
    cause: OwnerReadyReservationError,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ReadyGeometryError {
    ProcessCapacityOverflow,
}

#[derive(Debug)]
pub(super) struct ReadyGeometry {
    session_capacity: usize,
    process_cells: usize,
    class_capacities: [usize; 4],
}

impl ReadyGeometry {
    pub(super) fn checked(session_capacity: NonZeroUsize) -> Result<Self, ReadyGeometryError> {
        let session_capacity = session_capacity.get();
        let process_cells = session_capacity
            .checked_mul(4)
            .and_then(|cells| cells.checked_add(1))
            .ok_or(ReadyGeometryError::ProcessCapacityOverflow)?;
        let settlement_cells = session_capacity
            .checked_add(1)
            .ok_or(ReadyGeometryError::ProcessCapacityOverflow)?;
        Ok(Self {
            session_capacity,
            process_cells,
            class_capacities: [
                settlement_cells,
                session_capacity,
                session_capacity,
                session_capacity,
            ],
        })
    }

    pub(super) fn session_capacity(&self) -> usize {
        self.session_capacity
    }

    pub(super) fn process_cells(&self) -> usize {
        self.process_cells
    }

    pub(super) fn class_capacities(&self) -> [usize; 4] {
        self.class_capacities
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerReadySource {
    ControlSend,
    OrderedSend,
    ControlInbox,
    OrderedInbox,
    OwnerContinuation,
}

impl OwnerReadySource {
    pub(super) fn index(self) -> usize {
        match self {
            Self::ControlSend => 0,
            Self::OrderedSend => 1,
            Self::ControlInbox => 2,
            Self::OrderedInbox => 3,
            Self::OwnerContinuation => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SendLane {
    Control,
    OrderedSource,
}

impl SendLane {
    pub(super) fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::OrderedSource => 1,
        }
    }

    fn other(self) -> Self {
        match self {
            Self::Control => Self::OrderedSource,
            Self::OrderedSource => Self::Control,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum OwnerReadyDrive {
    MailboxOwned,
    Full,
    Closed,
    NoRunnable,
    Quarantined,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum OwnerCapacityRearm {
    Rearmed,
    UnrelatedLane,
    Closed,
    NoBlocked,
    Quarantined,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum OwnerReadyUnit {
    SourcePulled,
    SourceDrained,
    SourceRead,
    Registered,
    OutputAdmitted,
    Transfer(OwnerReadyDrive),
    Rearm(OwnerCapacityRearm),
    Waiting,
}

pub(super) struct SessionCommandVisit {
    pub(super) work: OwnerReadyUnit,
    pub(super) runnable: usize,
    // None leaves the actual send owner's two-lane demand unchanged.
    // Only a completed commands operation supplies a replacement summary.
    pub(super) blocked: Option<usize>,
}

pub(super) struct SendCursor {
    next: SendLane,
}

impl SendCursor {
    pub(super) fn new() -> Self {
        Self {
            next: SendLane::Control,
        }
    }

    pub(super) fn select(&mut self, eligible: [bool; 2]) -> Option<SendLane> {
        for lane in [self.next, self.next.other()] {
            if eligible[lane.index()] {
                self.next = lane.other();
                return Some(lane);
            }
        }
        None
    }
}

use std::alloc::Layout;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Wake, Waker};
use tokio::sync::Notify;

const WORD_BITS: usize = usize::BITS as usize;
const SOURCE_COUNT: usize = 5;
const SESSION_CLASS: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReadyFault {
    Layout,
    Reserve,
    InvalidResident,
    Stale,
    Corrupt,
    Poisoned,
    RevisionExhausted,
    EpochExhausted,
    ColdReservationAbandoned,
    SlotGenerationExhausted,
    NotificationUnwind,
}

// One monotonic record: zero is Open, nonzero is the first committed root fault.
// The existing ReadyClaim owns admitted work; this gate owns no second lease.
struct ReadyFailureGate(AtomicU8);

impl ReadyFailureGate {
    fn new() -> Self {
        Self(AtomicU8::new(0))
    }

    fn decode(code: u8) -> ReadyFault {
        match code {
            1 => ReadyFault::Layout,
            2 => ReadyFault::Reserve,
            3 => ReadyFault::InvalidResident,
            4 => ReadyFault::Stale,
            5 => ReadyFault::Corrupt,
            6 => ReadyFault::Poisoned,
            7 => ReadyFault::RevisionExhausted,
            8 => ReadyFault::EpochExhausted,
            9 => ReadyFault::ColdReservationAbandoned,
            10 => ReadyFault::SlotGenerationExhausted,
            11 => ReadyFault::NotificationUnwind,
            _ => ReadyFault::Corrupt,
        }
    }

    fn check(&self) -> Result<(), ReadyFault> {
        match self.0.load(Ordering::Acquire) {
            0 => Ok(()),
            code => Err(Self::decode(code)),
        }
    }

    fn admit(&self) -> Result<(), ReadyFault> {
        // A strong successful Open->Open RMW orders against fail's RMW. Only a
        // unit admitted before failure may finish; there is no load/act gap.
        self.0
            .compare_exchange(0, 0, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(Self::decode)
    }

    fn fail(&self, fault: ReadyFault) -> ReadyFault {
        let code = match fault {
            ReadyFault::Layout => 1,
            ReadyFault::Reserve => 2,
            ReadyFault::InvalidResident => 3,
            ReadyFault::Stale => 4,
            ReadyFault::Corrupt => 5,
            ReadyFault::Poisoned => 6,
            ReadyFault::RevisionExhausted => 7,
            ReadyFault::EpochExhausted => 8,
            ReadyFault::ColdReservationAbandoned => 9,
            ReadyFault::SlotGenerationExhausted => 10,
            ReadyFault::NotificationUnwind => 11,
        };
        match self
            .0
            .compare_exchange(0, code, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => fault,
            Err(first) => Self::decode(first),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct ReadyKey {
    slot: usize,
    slot_generation: u64,
    scheduling_generation: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadyPhase {
    Dormant,
    Reserved,
    Idle,
    Published,
    RunningClean,
    RunningDirty,
    Clearing,
    Quarantined,
}

// Non-Clone cell: activation owns one ready position. Idle is still active;
// a clear never frees its lease. No retirement/reuse exists in this packet.
struct ReadyCell {
    phase: ReadyPhase,
    revision: u64,
}

impl ReadyCell {
    fn dormant() -> Self {
        Self {
            phase: ReadyPhase::Dormant,
            revision: 0,
        }
    }

    fn publish(&mut self) -> Result<(), ReadyFault> {
        if matches!(
            self.phase,
            ReadyPhase::Dormant | ReadyPhase::Reserved | ReadyPhase::Quarantined
        ) {
            return Err(ReadyFault::Corrupt);
        }
        let Some(next) = self.revision.checked_add(1) else {
            self.phase = ReadyPhase::Quarantined;
            return Err(ReadyFault::RevisionExhausted);
        };
        self.revision = next;
        self.phase = match self.phase {
            ReadyPhase::RunningClean | ReadyPhase::RunningDirty | ReadyPhase::Clearing => {
                ReadyPhase::RunningDirty
            }
            _ => ReadyPhase::Published,
        };
        Ok(())
    }
}

enum ReadyOccupancy {
    Free,
    ColdReserved(ReadyCorrelation),
    AssociatedPreparing(ReadyCorrelation),
    BoundInactive(ReadyCorrelation, flow::FlowReadyBacking),
    BoundActive(ReadyCorrelation, flow::FlowReadyBacking),
    #[cfg(test)]
    Prototype,
}

struct ReadySlot {
    key: ReadyKey,
    occupancy: ReadyOccupancy,
    sources: [ReadyCell; SOURCE_COUNT],
    processes: [ReadyCell; 4],
    local_word: usize,
    source_cursor: usize,
    capacity_mask: usize,
}

// The publication interval must remain observable even if the metadata mutex
// is poisoned. Neither this atomic word nor its envelope owns work or a lease.
struct ReadySlotEnvelope {
    metadata: Mutex<ReadySlot>,
    publication: AtomicU8,
}

impl ReadySlotEnvelope {
    fn lock(&self) -> std::sync::LockResult<std::sync::MutexGuard<'_, ReadySlot>> {
        self.metadata.lock()
    }

    #[cfg(test)]
    fn try_lock(&self) -> std::sync::TryLockResult<std::sync::MutexGuard<'_, ReadySlot>> {
        self.metadata.try_lock()
    }

    #[cfg(test)]
    fn is_poisoned(&self) -> bool {
        self.metadata.is_poisoned()
    }
}

// Fixed-height, preallocated hierarchy. Only ReadyIndex clears/selects;
// publishers set under their slot guard through the entire ancestor chain.
struct ReadyTree {
    capacity: usize,
    levels: Vec<std::ops::Range<usize>>,
    words: Vec<AtomicUsize>,
}

impl ReadyTree {
    fn checked_word_count(capacity: usize) -> Result<usize, ReadyFault> {
        let mut width = capacity;
        let mut count = 0usize;
        for _ in 0..WORD_BITS {
            width = width / WORD_BITS + usize::from(width % WORD_BITS != 0);
            count = count.checked_add(width).ok_or(ReadyFault::Layout)?;
            if width <= 1 {
                Layout::array::<AtomicUsize>(count).map_err(|_| ReadyFault::Layout)?;
                return Ok(count);
            }
        }
        Err(ReadyFault::Layout)
    }

    fn new(capacity: usize) -> Result<Self, ReadyFault> {
        let count = Self::checked_word_count(capacity)?;
        let mut words = allocate_exact_vec(count).map_err(|error| match error {
            EffectStorageError::Reserve => ReadyFault::Reserve,
            _ => ReadyFault::Layout,
        })?;
        for _ in 0..count {
            words.push(AtomicUsize::new(0));
        }
        let mut levels = allocate_exact_vec(WORD_BITS).map_err(|error| match error {
            EffectStorageError::Reserve => ReadyFault::Reserve,
            _ => ReadyFault::Layout,
        })?;
        let (mut width, mut offset) = (capacity, 0);
        for _ in 0..WORD_BITS {
            width = width / WORD_BITS + usize::from(width % WORD_BITS != 0);
            levels.push(offset..offset + width);
            offset += width;
            if width <= 1 {
                break;
            }
        }
        Ok(Self {
            capacity,
            levels,
            words,
        })
    }

    fn publish(&self, mut bit: usize) {
        for range in &self.levels {
            self.words[range.start + bit / WORD_BITS]
                .fetch_or(1 << (bit % WORD_BITS), Ordering::Release);
            bit /= WORD_BITS;
        }
    }

    fn clear(&self, bit: usize) {
        let first = &self.levels[0];
        let mut child_word = bit / WORD_BITS;
        self.words[first.start + child_word].fetch_and(!(1 << (bit % WORD_BITS)), Ordering::AcqRel);
        for level in 1..self.levels.len() {
            let parent_word = child_word / WORD_BITS;
            let mask = 1 << (child_word % WORD_BITS);
            let parent = &self.words[self.levels[level].start + parent_word];
            parent.fetch_and(!mask, Ordering::AcqRel);
            if self.words[self.levels[level - 1].start + child_word].load(Ordering::Acquire) != 0 {
                parent.fetch_or(mask, Ordering::Release);
            }
            child_word = parent_word;
        }
    }

    fn first_at(&self, level: usize, start: usize) -> Result<Option<usize>, ReadyFault> {
        let Some(range) = self.levels.get(level) else {
            return Ok(None);
        };
        let word = start / WORD_BITS;
        if word >= range.len() {
            return Ok(None);
        }
        let available = self.words[range.start + word].load(Ordering::Acquire)
            & (usize::MAX << (start % WORD_BITS));
        if available != 0 {
            return Ok(Some(word * WORD_BITS + available.trailing_zeros() as usize));
        }
        let Some(next_word) = self.first_at(level + 1, word + 1)? else {
            return Ok(None);
        };
        let Some(index) = range.clone().nth(next_word) else {
            return Err(ReadyFault::Corrupt);
        };
        let available = self.words[index].load(Ordering::Acquire);
        if available == 0 {
            return Err(ReadyFault::Corrupt);
        }
        Ok(Some(
            next_word * WORD_BITS + available.trailing_zeros() as usize,
        ))
    }

    fn select(&self, cursor: usize) -> Result<Option<usize>, ReadyFault> {
        let found = match self.first_at(0, cursor)? {
            Some(bit) => Some(bit),
            None if cursor != 0 => self.first_at(0, 0)?,
            None => None,
        };
        if found.is_some_and(|bit| bit >= self.capacity) {
            return Err(ReadyFault::Corrupt);
        }
        Ok(found)
    }

    fn is_empty(&self) -> bool {
        self.words
            .last()
            .is_none_or(|word| word.load(Ordering::Acquire) == 0)
    }
}

struct ReadyShared {
    slots: Vec<ReadySlotEnvelope>,
    // All process capacities exist, including the dormant root-only cell.
    trees: [ReadyTree; 4],
    root_settlement: ReadyCell,
    failure: ReadyFailureGate,
    epoch: AtomicU64,
    notify: Arc<Notify>,
}

impl ReadyShared {
    fn cold_fault(&self, detected: ReadyFault) -> OwnerReadyReservationError {
        self.failure.fail(detected);
        OwnerReadyReservationError::Metadata(detected)
    }

    fn signal(&self) {
        self.epoch.fetch_or(1, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn publish_capacity(&self, key: ReadyKey) {
        let Some(slot) = self.slots.get(key.slot) else {
            return;
        };
        {
            let mut state = match slot.lock() {
                Ok(state) => state,
                Err(poisoned) => {
                    drop(poisoned);
                    // Commit before notification; no poisoned metadata is reclaimed.
                    self.failure.fail(ReadyFault::Poisoned);
                    self.trees[SESSION_CLASS].publish(key.slot);
                    self.signal();
                    return;
                }
            };
            if state.key != key {
                return;
            }
            if state.processes[SESSION_CLASS].phase == ReadyPhase::Quarantined {
                return;
            }
            let mask = state.capacity_mask;
            if mask == 0 {
                return;
            }
            for source in 0..2 {
                if mask & (1 << source) != 0 {
                    if let Err(fault) = state.sources[source].publish() {
                        state.sources[source].phase = ReadyPhase::Quarantined;
                        self.failure.fail(fault);
                    }
                    // Even exhaustion remains indexed for owning quarantine.
                    state.local_word |= 1 << source;
                }
            }
            if let Err(fault) = state.processes[SESSION_CLASS].publish() {
                state.processes[SESSION_CLASS].phase = ReadyPhase::Quarantined;
                self.failure.fail(fault);
            }
            self.trees[SESSION_CLASS].publish(key.slot);
        }
        self.signal();
    }
}

struct CapacityWake {
    shared: Weak<ReadyShared>,
    key: ReadyKey,
}

impl Wake for CapacityWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(shared) = self.shared.upgrade() {
            shared.publish_capacity(self.key);
        }
    }
}

pub(super) struct ReadyClaim {
    pub(super) slot: usize,
    pub(super) source: OwnerReadySource,
    key: ReadyKey,
    source_revision: u64,
    process_revision: u64,
}

pub(super) struct ReadyIndex {
    shared: Arc<ReadyShared>,
    cursors: [usize; 4],
}

impl ReadyIndex {
    /// Requested backing only: the enclosing Root already owns our inline
    /// header. No session Flow backing exists at this cold metadata boundary.
    pub(super) fn cold_backing_layout(geometry: &ReadyGeometry) -> Result<Layout, ReadyFault> {
        let n = geometry.session_capacity();
        n.checked_mul(2).ok_or(ReadyFault::Layout)?;
        n.checked_mul(SOURCE_COUNT).ok_or(ReadyFault::Layout)?;
        let mut peak = Layout::array::<ReadySlotEnvelope>(n).map_err(|_| ReadyFault::Layout)?;
        for capacity in geometry.class_capacities() {
            for piece in [
                Layout::array::<AtomicUsize>(ReadyTree::checked_word_count(capacity)?)
                    .map_err(|_| ReadyFault::Layout)?,
                Layout::array::<std::ops::Range<usize>>(WORD_BITS)
                    .map_err(|_| ReadyFault::Layout)?,
            ] {
                peak = peak.extend(piece).map_err(|_| ReadyFault::Layout)?.0;
            }
        }
        for piece in [
            flow::arc_layout::<ReadyShared>()?,
            flow::arc_layout::<Notify>()?,
        ] {
            peak = peak.extend(piece).map_err(|_| ReadyFault::Layout)?.0;
        }
        Ok(peak.pad_to_align())
    }

    #[cfg(test)]
    pub(super) fn new(
        geometry: &ReadyGeometry,
        initial: &[[usize; 2]],
    ) -> Result<Self, ReadyFault> {
        Self::prepare(geometry, initial)
    }

    pub(super) fn cold(geometry: &ReadyGeometry) -> Result<Self, ReadyFault> {
        Self::prepare(geometry, &[])
    }

    fn prepare(geometry: &ReadyGeometry, initial: &[[usize; 2]]) -> Result<Self, ReadyFault> {
        let n = geometry.session_capacity();
        if initial.len() > n
            || initial
                .iter()
                .any(|[owned, blocked]| owned & !3 != 0 || blocked & !owned != 0)
        {
            return Err(ReadyFault::InvalidResident);
        }
        Layout::new::<Self>()
            .extend(Self::cold_backing_layout(geometry)?)
            .map_err(|_| ReadyFault::Layout)?;
        let mut slots = allocate_exact_vec(n).map_err(|error| match error {
            EffectStorageError::Reserve => ReadyFault::Reserve,
            _ => ReadyFault::Layout,
        })?;
        for slot in 0..n {
            let [owned, blocked] = initial.get(slot).copied().unwrap_or([0, 0]);
            let mut sources = std::array::from_fn(|_| ReadyCell::dormant());
            for source in 0..2 {
                if owned & (1 << source) != 0 {
                    sources[source] = ReadyCell {
                        phase: ReadyPhase::Published,
                        revision: 1,
                    };
                }
            }
            let mut processes = std::array::from_fn(|_| ReadyCell::dormant());
            if owned != 0 {
                processes[SESSION_CLASS] = ReadyCell {
                    phase: ReadyPhase::Published,
                    revision: 1,
                };
            }
            slots.push(ReadySlotEnvelope {
                metadata: Mutex::new(ReadySlot {
                    key: ReadyKey {
                        slot,
                        slot_generation: 1,
                        scheduling_generation: 1,
                    },
                    #[cfg(test)]
                    occupancy: if slot < initial.len() {
                        ReadyOccupancy::Prototype
                    } else {
                        ReadyOccupancy::Free
                    },
                    #[cfg(not(test))]
                    occupancy: ReadyOccupancy::Free,
                    sources,
                    processes,
                    local_word: owned,
                    source_cursor: 0,
                    capacity_mask: blocked,
                }),
                publication: AtomicU8::new(0),
            });
        }
        let [a, b, c, d] = geometry.class_capacities();
        let trees = [
            ReadyTree::new(a)?,
            ReadyTree::new(b)?,
            ReadyTree::new(c)?,
            ReadyTree::new(d)?,
        ];
        for (slot, [owned, _]) in initial.iter().enumerate() {
            if *owned != 0 {
                trees[SESSION_CLASS].publish(slot);
            }
        }
        Ok(Self {
            shared: Arc::new(ReadyShared {
                slots,
                trees,
                root_settlement: ReadyCell::dormant(),
                failure: ReadyFailureGate::new(),
                epoch: AtomicU64::new(0),
                notify: Arc::new(Notify::new()),
            }),
            cursors: [0; 4],
        })
    }

    pub(super) fn notification(&self) -> Arc<Notify> {
        Arc::clone(&self.shared.notify)
    }

    pub(super) fn capacity_waker(&self, slot: usize) -> Waker {
        Waker::from(Arc::new(CapacityWake {
            shared: Arc::downgrade(&self.shared),
            key: ReadyKey {
                slot,
                slot_generation: 1,
                scheduling_generation: 1,
            },
        }))
    }

    pub(super) fn select(&mut self, class: usize) -> Result<Option<usize>, ReadyFault> {
        let found = self.shared.trees[class]
            .select(self.cursors[class])
            .map_err(|fault| self.fail_root(fault))?;
        if let Some(slot) = found {
            self.cursors[class] = if slot + 1 == self.shared.trees[class].capacity {
                0
            } else {
                slot + 1
            };
        }
        Ok(found)
    }

    pub(super) fn claim(&mut self, slot: usize) -> Result<ReadyClaim, ReadyFault> {
        let outcome = (|| {
            let mut state = self
                .shared
                .slots
                .get(slot)
                .ok_or(ReadyFault::Corrupt)?
                .lock()
                .map_err(|_| ReadyFault::Poisoned)?;
            if state.processes[SESSION_CLASS].phase != ReadyPhase::Published {
                return Err(ReadyFault::Corrupt);
            }
            let start = state.source_cursor;
            let source = (0..SOURCE_COUNT)
                .map(|offset| (start + offset) % SOURCE_COUNT)
                .find(|source| state.local_word & (1 << source) != 0)
                .ok_or(ReadyFault::Corrupt)?;
            let selected = match source {
                0 => OwnerReadySource::ControlSend,
                1 => OwnerReadySource::OrderedSend,
                2 => OwnerReadySource::ControlInbox,
                3 => OwnerReadySource::OrderedInbox,
                4 => OwnerReadySource::OwnerContinuation,
                _ => return Err(ReadyFault::Corrupt),
            };
            if state.sources[source].phase != ReadyPhase::Published {
                return Err(ReadyFault::Corrupt);
            }
            // All selected metadata is valid. No fallible step or callback follows
            // this admission before the same claim and Running phases are published.
            self.shared.failure.admit()?;
            state.source_cursor = (source + 1) % SOURCE_COUNT;
            let claim = ReadyClaim {
                slot,
                source: selected,
                key: state.key,
                source_revision: state.sources[source].revision,
                process_revision: state.processes[SESSION_CLASS].revision,
            };
            state.sources[source].phase = ReadyPhase::RunningClean;
            state.processes[SESSION_CLASS].phase = ReadyPhase::RunningClean;
            Ok(claim)
        })();
        outcome.map_err(|fault| self.fail_root(fault))
    }

    pub(super) fn finish(
        &mut self,
        claim: &ReadyClaim,
        runnable: usize,
        blocked: Option<usize>,
    ) -> Result<(), ReadyFault> {
        // Another slot's fatal event cannot undo this already-admitted unit.
        let outcome = (|| {
            let mut state = self.shared.slots[claim.slot]
                .lock()
                .map_err(|_| ReadyFault::Poisoned)?;
            if state.key != claim.key {
                return Err(ReadyFault::Stale);
            }
            let source = claim.source.index();
            if matches!(state.sources[source].phase, ReadyPhase::Quarantined)
                || state.processes[SESSION_CLASS].phase == ReadyPhase::Quarantined
            {
                return Err(ReadyFault::RevisionExhausted);
            }
            let dirty = state.sources[source].phase == ReadyPhase::RunningDirty
                || state.sources[source].revision != claim.source_revision;
            state.sources[source].phase = ReadyPhase::Clearing;
            state.local_word &= !(1 << source);
            // Only the owning send operation reports both retained send-cell
            // demands. Inbox/continuation work cannot cancel their registration.
            if let Some(blocked) = blocked {
                if source >= 2 || blocked & !0b11 != 0 {
                    return Err(ReadyFault::Corrupt);
                }
                state.capacity_mask = blocked;
            }
            if dirty || runnable & (1 << source) != 0 {
                state.sources[source].phase = ReadyPhase::Published;
                state.local_word |= 1 << source;
            } else {
                state.sources[source].phase = ReadyPhase::Idle;
            }
            // Publish every other owning source made runnable by the same
            // selected transaction; mailbox capacity still applies two epochs.
            for other in 0..SOURCE_COUNT {
                if other != source && runnable & (1 << other) != 0 {
                    state.sources[other].publish()?;
                    state.local_word |= 1 << other;
                }
            }
            let process_dirty = state.processes[SESSION_CLASS].revision != claim.process_revision
                || state.processes[SESSION_CLASS].phase == ReadyPhase::RunningDirty;
            state.processes[SESSION_CLASS].phase = ReadyPhase::Clearing;
            self.shared.trees[SESSION_CLASS].clear(claim.slot);
            // A process dirty revision by itself is not work after its only source
            // has been serviced; local dirty work remains explicitly in local_word.
            if state.local_word != 0 {
                state.processes[SESSION_CLASS].phase = ReadyPhase::Published;
                self.shared.trees[SESSION_CLASS].publish(claim.slot);
            } else if process_dirty {
                // A same-slot publisher always marks a local bit first. Empty here
                // is corruption unless its selected source retained that dirty bit.
                return Err(ReadyFault::Corrupt);
            } else {
                state.processes[SESSION_CLASS].phase = ReadyPhase::Idle;
            }
            Ok(())
        })();
        outcome.map_err(|fault| self.fail_root(fault))
    }

    pub(super) fn quarantine(&mut self, slot: usize) -> Result<(), ReadyFault> {
        let outcome = (|| {
            let lock = self.shared.slots.get(slot).ok_or(ReadyFault::Corrupt)?;
            let mut state = lock.lock().map_err(|_| ReadyFault::Poisoned)?;
            self.check_root()?;
            // Preflight is complete; only healthy metadata may retire local work.
            for cell in &mut state.sources {
                if cell.phase != ReadyPhase::Dormant {
                    cell.phase = ReadyPhase::Quarantined;
                }
            }
            for cell in &mut state.processes {
                if cell.phase != ReadyPhase::Dormant {
                    cell.phase = ReadyPhase::Quarantined;
                }
            }
            state.capacity_mask = 0;
            state.local_word = 0;
            self.shared.trees[SESSION_CLASS].clear(slot);
            Ok(())
        })();
        outcome.map_err(|fault| self.fail_root(fault))
    }

    pub(super) fn check_root(&self) -> Result<(), ReadyFault> {
        self.shared.failure.check()
    }

    pub(super) fn fail_root(&self, fault: ReadyFault) -> ReadyFault {
        self.shared.failure.fail(fault)
    }

    pub(super) fn park_epoch(&self) -> Result<Option<u64>, ReadyFault> {
        self.check_root()?;
        let seen = self.shared.epoch.load(Ordering::Acquire);
        if seen & 1 == 0 {
            return Ok(Some(seen));
        }
        let next = seen
            .checked_add(1)
            .ok_or_else(|| self.fail_root(ReadyFault::EpochExhausted))?;
        Ok(self
            .shared
            .epoch
            .compare_exchange(seen, next, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| next))
    }

    pub(super) fn park_is_unchanged(&self, epoch: u64) -> bool {
        self.shared.trees.iter().all(ReadyTree::is_empty)
            && self.shared.epoch.load(Ordering::Acquire) == epoch
            && self.check_root().is_ok()
    }
}

#[cfg(test)]
mod tests;

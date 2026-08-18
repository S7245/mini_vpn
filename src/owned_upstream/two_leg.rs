//! Deterministic byte-only transport substrate for the Knife16 two-leg harness.
//!
//! This module deliberately owns no protocol authority.  Its input and output
//! are encoded byte messages, so faults cannot manufacture decoded frames,
//! session events, attach commits, or switch decisions.

use std::collections::{BTreeMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::time::Duration;

/// Monotonic virtual time used by the deterministic harness.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct SimTime(u64);

impl SimTime {
    pub(crate) const ZERO: Self = Self(0);

    pub(crate) const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    pub(crate) const fn as_nanos(self) -> u64 {
        self.0
    }

    pub(crate) fn checked_add(self, duration: Duration) -> Result<Self, SimTimeError> {
        let nanos = u64::try_from(duration.as_nanos()).map_err(|_| SimTimeError::Overflow)?;
        self.0
            .checked_add(nanos)
            .map(Self)
            .ok_or(SimTimeError::Overflow)
    }

    pub(crate) fn checked_duration_since(self, earlier: Self) -> Result<Duration, SimTimeError> {
        self.0
            .checked_sub(earlier.0)
            .map(Duration::from_nanos)
            .ok_or(SimTimeError::TimeWentBackwards)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SimTimeError {
    Overflow,
    TimeWentBackwards,
}

impl fmt::Display for SimTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => f.write_str("simulated time overflow"),
            Self::TimeWentBackwards => f.write_str("simulated time went backwards"),
        }
    }
}

impl Error for SimTimeError {}

/// Stable scheduling phase.  Lower phases are prerequisites for higher ones
/// at the same virtual deadline.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub(crate) enum EventPhase {
    FaultRelease = 0,
    TransportReceive = 1,
    SupervisorCommand = 2,
    IoCompletion = 3,
    TimerExpiry = 4,
    JoinCleanup = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ActorId(u32);

impl ActorId {
    pub(crate) const fn new(id: u32) -> Self {
        Self(id)
    }

    pub(crate) const fn get(self) -> u32 {
        self.0
    }
}

/// The reproducible key of one bounded scheduler action.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct EventKey {
    deadline: SimTime,
    phase: EventPhase,
    actor_id: ActorId,
    sequence: u64,
}

impl EventKey {
    pub(crate) const fn deadline(self) -> SimTime {
        self.deadline
    }

    pub(crate) const fn phase(self) -> EventPhase {
        self.phase
    }

    pub(crate) const fn actor_id(self) -> ActorId {
        self.actor_id
    }

    pub(crate) const fn sequence(self) -> u64 {
        self.sequence
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EventBudget {
    max_events: usize,
    max_owned_bytes: usize,
}

impl EventBudget {
    pub(crate) fn new(max_events: usize, max_owned_bytes: usize) -> Result<Self, ScheduleError> {
        if max_events == 0 {
            return Err(ScheduleError::ZeroEventBudget);
        }
        Ok(Self {
            max_events,
            max_owned_bytes,
        })
    }

    pub(crate) const fn max_events(self) -> usize {
        self.max_events
    }

    pub(crate) const fn max_owned_bytes(self) -> usize {
        self.max_owned_bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EventSpec<T> {
    deadline: SimTime,
    phase: EventPhase,
    actor_id: ActorId,
    owned_bytes: usize,
    trace_tag: u64,
    payload: T,
}

impl<T> EventSpec<T> {
    pub(crate) const fn new(
        deadline: SimTime,
        phase: EventPhase,
        actor_id: ActorId,
        owned_bytes: usize,
        trace_tag: u64,
        payload: T,
    ) -> Self {
        Self {
            deadline,
            phase,
            actor_id,
            owned_bytes,
            trace_tag,
            payload,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScheduledEvent<T> {
    key: EventKey,
    owned_bytes: usize,
    trace_tag: u64,
    payload: T,
}

impl<T> ScheduledEvent<T> {
    pub(crate) const fn key(&self) -> EventKey {
        self.key
    }

    pub(crate) const fn owned_bytes(&self) -> usize {
        self.owned_bytes
    }

    pub(crate) fn into_payload(self) -> T {
        self.payload
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScheduleError {
    ZeroEventBudget,
    TimeWentBackwards,
    EventCountOverflow,
    ByteCountOverflow,
    ByteCountUnderflow,
    EventBudgetExceeded,
    ByteBudgetExceeded,
    SequenceExhausted,
}

impl fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroEventBudget => f.write_str("scheduler event budget must be nonzero"),
            Self::TimeWentBackwards => f.write_str("cannot schedule an event in the past"),
            Self::EventCountOverflow => f.write_str("scheduler event count overflow"),
            Self::ByteCountOverflow => f.write_str("scheduler byte count overflow"),
            Self::ByteCountUnderflow => f.write_str("scheduler byte count underflow"),
            Self::EventBudgetExceeded => f.write_str("scheduler event budget exceeded"),
            Self::ByteBudgetExceeded => f.write_str("scheduler byte budget exceeded"),
            Self::SequenceExhausted => f.write_str("scheduler sequence exhausted"),
        }
    }
}

impl Error for ScheduleError {}

const TRACE_FNV_OFFSET: u64 = 0xcbf29ce484222325;
const TRACE_FNV_PRIME: u64 = 0x100000001b3;

/// A single-threaded virtual scheduler with bounded work and byte ownership.
///
/// Actor selection rotates within one `(deadline, phase)` group.  Therefore
/// an actor that creates more same-deadline work cannot re-enter ahead of a
/// peer that was already ready.
pub(crate) struct DeterministicScheduler<T> {
    budget: EventBudget,
    now: SimTime,
    next_sequence: u64,
    scheduled_events: usize,
    scheduled_bytes: usize,
    pending_bytes: usize,
    events: BTreeMap<EventKey, ScheduledEvent<T>>,
    active_round: Option<SchedulerRound>,
    actor_cursor: Option<((SimTime, EventPhase), ActorId)>,
    trace_hash: u64,
}

/// A micro-round freezes every event that was ready at its deadline when the
/// round began. Same-deadline work scheduled by one of those events is
/// deferred until the next round, including work in an earlier phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SchedulerRound {
    deadline: SimTime,
    sequence_limit: u64,
}

impl<T> DeterministicScheduler<T> {
    pub(crate) fn new(budget: EventBudget) -> Self {
        Self {
            budget,
            now: SimTime::ZERO,
            next_sequence: 0,
            scheduled_events: 0,
            scheduled_bytes: 0,
            pending_bytes: 0,
            events: BTreeMap::new(),
            active_round: None,
            actor_cursor: None,
            trace_hash: TRACE_FNV_OFFSET,
        }
    }

    pub(crate) const fn now(&self) -> SimTime {
        self.now
    }

    pub(crate) fn pending_events(&self) -> usize {
        self.events.len()
    }

    pub(crate) const fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    pub(crate) const fn trace_hash(&self) -> u64 {
        self.trace_hash
    }

    pub(crate) fn peek_deadline(&self) -> Option<SimTime> {
        self.events.keys().next().map(|key| key.deadline)
    }

    pub(crate) fn schedule(&mut self, event: EventSpec<T>) -> Result<EventKey, ScheduleError> {
        let mut keys = self.schedule_batch(vec![event])?;
        keys.pop().ok_or(ScheduleError::EventCountOverflow)
    }

    /// Atomically admits a batch, so a duplicate/reorder action cannot retain
    /// only a prefix when the declared scenario budget is exhausted.
    pub(crate) fn schedule_batch(
        &mut self,
        batch: Vec<EventSpec<T>>,
    ) -> Result<Vec<EventKey>, ScheduleError> {
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        if batch.iter().any(|event| event.deadline < self.now) {
            return Err(ScheduleError::TimeWentBackwards);
        }

        let batch_events = batch.len();
        let batch_bytes = batch.iter().try_fold(0usize, |total, event| {
            total
                .checked_add(event.owned_bytes)
                .ok_or(ScheduleError::ByteCountOverflow)
        })?;
        let new_event_total = self
            .scheduled_events
            .checked_add(batch_events)
            .ok_or(ScheduleError::EventCountOverflow)?;
        let new_byte_total = self
            .scheduled_bytes
            .checked_add(batch_bytes)
            .ok_or(ScheduleError::ByteCountOverflow)?;
        if new_event_total > self.budget.max_events {
            return Err(ScheduleError::EventBudgetExceeded);
        }
        if new_byte_total > self.budget.max_owned_bytes {
            return Err(ScheduleError::ByteBudgetExceeded);
        }
        let batch_sequence =
            u64::try_from(batch_events).map_err(|_| ScheduleError::SequenceExhausted)?;
        self.next_sequence
            .checked_add(batch_sequence)
            .ok_or(ScheduleError::SequenceExhausted)?;

        let mut keys = Vec::with_capacity(batch_events);
        for event in batch {
            let key = EventKey {
                deadline: event.deadline,
                phase: event.phase,
                actor_id: event.actor_id,
                sequence: self.next_sequence,
            };
            self.next_sequence += 1;
            let scheduled = ScheduledEvent {
                key,
                owned_bytes: event.owned_bytes,
                trace_tag: event.trace_tag,
                payload: event.payload,
            };
            let replaced = self.events.insert(key, scheduled);
            debug_assert!(replaced.is_none(), "global sequence must make keys unique");
            keys.push(key);
        }
        self.scheduled_events = new_event_total;
        self.scheduled_bytes = new_byte_total;
        self.pending_bytes += batch_bytes;
        Ok(keys)
    }

    /// Removes pending ownership selected by a deterministic cleanup action.
    ///
    /// Cumulative work counters deliberately remain monotonic: cancellation
    /// releases live bytes, but it does not refund the scenario's total work
    /// budget and thereby cannot create unbounded resubmission capacity.
    fn cancel_where(
        &mut self,
        mut predicate: impl FnMut(&T) -> bool,
    ) -> Result<Vec<ScheduledEvent<T>>, ScheduleError> {
        let keys: Vec<_> = self
            .events
            .iter()
            .filter_map(|(key, event)| predicate(&event.payload).then_some(*key))
            .collect();
        let removed_bytes = keys.iter().try_fold(0usize, |total, key| {
            let owned_bytes = self
                .events
                .get(key)
                .map(|event| event.owned_bytes)
                .ok_or(ScheduleError::EventCountOverflow)?;
            total
                .checked_add(owned_bytes)
                .ok_or(ScheduleError::ByteCountOverflow)
        })?;
        let pending_bytes = self
            .pending_bytes
            .checked_sub(removed_bytes)
            .ok_or(ScheduleError::ByteCountUnderflow)?;

        let mut removed = Vec::with_capacity(keys.len());
        for key in keys {
            let event = self
                .events
                .remove(&key)
                .ok_or(ScheduleError::EventCountOverflow)?;
            removed.push(event);
        }
        self.pending_bytes = pending_bytes;
        self.reconcile_active_round();
        Ok(removed)
    }

    fn matching_ownership(
        &self,
        mut predicate: impl FnMut(&T) -> bool,
    ) -> Result<(usize, usize), ScheduleError> {
        self.events
            .values()
            .filter(|event| predicate(&event.payload))
            .try_fold((0usize, 0usize), |(messages, bytes), event| {
                Ok((
                    messages
                        .checked_add(1)
                        .ok_or(ScheduleError::EventCountOverflow)?,
                    bytes
                        .checked_add(event.owned_bytes)
                        .ok_or(ScheduleError::ByteCountOverflow)?,
                ))
            })
    }

    /// Pops one event while preserving exact pending-byte ownership.
    ///
    /// Selection is previewed before any event, time, cursor, or trace state
    /// changes. A corrupted ownership counter therefore fails closed with the
    /// event still queued instead of manufacturing a false-zero snapshot.
    pub(crate) fn pop_next_checked(&mut self) -> Result<Option<ScheduledEvent<T>>, ScheduleError> {
        let Some((round, selected)) = self.preview_next() else {
            return Ok(None);
        };
        let event = self
            .events
            .get(&selected)
            .ok_or(ScheduleError::EventCountOverflow)?;
        let pending_bytes = self
            .pending_bytes
            .checked_sub(event.owned_bytes)
            .ok_or(ScheduleError::ByteCountUnderflow)?;

        if self.active_round.is_none() {
            self.active_round = Some(round);
            self.actor_cursor = None;
        }
        let event = self
            .events
            .remove(&selected)
            .ok_or(ScheduleError::EventCountOverflow)?;
        self.pending_bytes = pending_bytes;
        self.now = selected.deadline;

        let group = (selected.deadline, selected.phase);
        let group_remains =
            self.events.keys().copied().any(|key| {
                Self::eligible_in_round(key, round) && (key.deadline, key.phase) == group
            });
        if group_remains {
            self.actor_cursor = Some((group, selected.actor_id));
        } else {
            self.actor_cursor = None;
        }
        if !self
            .events
            .keys()
            .copied()
            .any(|key| Self::eligible_in_round(key, round))
        {
            self.active_round = None;
            self.actor_cursor = None;
        }
        self.trace_key(&event);
        Ok(Some(event))
    }

    /// Pops at most one event due at or before `through`.
    pub(crate) fn pop_one_due(
        &mut self,
        through: SimTime,
    ) -> Result<Option<ScheduledEvent<T>>, ScheduleError> {
        if through < self.now {
            return Err(ScheduleError::TimeWentBackwards);
        }
        if self
            .peek_deadline()
            .is_none_or(|deadline| deadline > through)
        {
            return Ok(None);
        }
        self.pop_next_checked()
    }

    fn preview_next(&self) -> Option<(SchedulerRound, EventKey)> {
        let round = match self.active_round {
            Some(round) => round,
            None => SchedulerRound {
                deadline: self.events.keys().next()?.deadline,
                sequence_limit: self.next_sequence,
            },
        };
        let first = self
            .events
            .keys()
            .copied()
            .find(|key| Self::eligible_in_round(*key, round))?;
        let group = (first.deadline, first.phase);
        let cursor = self
            .actor_cursor
            .filter(|(cursor_group, _)| *cursor_group == group)
            .map(|(_, actor)| actor);
        let mut first_in_group = None;
        let mut after_cursor = None;

        for key in self.events.keys().copied() {
            if !Self::eligible_in_round(key, round) || (key.deadline, key.phase) != group {
                if first_in_group.is_some() {
                    break;
                }
                continue;
            }
            first_in_group.get_or_insert(key);
            if cursor.is_some_and(|actor| key.actor_id > actor) {
                after_cursor = Some(key);
                break;
            }
        }
        let selected = after_cursor.or(first_in_group)?;
        Some((round, selected))
    }

    const fn eligible_in_round(key: EventKey, round: SchedulerRound) -> bool {
        key.deadline.0 == round.deadline.0 && key.sequence < round.sequence_limit
    }

    fn reconcile_active_round(&mut self) {
        let Some(round) = self.active_round else {
            self.actor_cursor = None;
            return;
        };
        let round_remains = self
            .events
            .keys()
            .copied()
            .any(|key| Self::eligible_in_round(key, round));
        if !round_remains {
            self.active_round = None;
            self.actor_cursor = None;
            return;
        }
        if let Some((group, _)) = self.actor_cursor {
            let group_remains = self.events.keys().copied().any(|key| {
                Self::eligible_in_round(key, round) && (key.deadline, key.phase) == group
            });
            if !group_remains {
                self.actor_cursor = None;
            }
        }
    }

    pub(crate) fn advance_idle_to(&mut self, deadline: SimTime) -> Result<(), ScheduleError> {
        if deadline < self.now {
            return Err(ScheduleError::TimeWentBackwards);
        }
        if self.peek_deadline().is_some_and(|next| next <= deadline) {
            return Err(ScheduleError::TimeWentBackwards);
        }
        self.now = deadline;
        Ok(())
    }

    fn trace_key(&mut self, event: &ScheduledEvent<T>) {
        for byte in event
            .key
            .deadline
            .as_nanos()
            .to_le_bytes()
            .into_iter()
            .chain([event.key.phase as u8])
            .chain(event.key.actor_id.get().to_le_bytes())
            .chain(event.key.sequence.to_le_bytes())
            .chain(event.trace_tag.to_le_bytes())
        {
            self.trace_hash ^= u64::from(byte);
            self.trace_hash = self.trace_hash.wrapping_mul(TRACE_FNV_PRIME);
        }
    }
}

const RATE_DENOMINATOR: u128 = 8_000_000_000;

/// Explicit directional workload inputs used to derive finite wire work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WireCapacitySpec {
    rate_bits_per_second: u64,
    horizon: Duration,
    max_payload_bytes: usize,
    frame_overhead_bytes: usize,
    forced_tail_fragments: usize,
    replay_copies: u8,
    duplicates_per_send: u8,
    fixed_events: usize,
    fixed_bytes: usize,
}

impl WireCapacitySpec {
    pub(crate) fn new(
        rate_bits_per_second: u64,
        horizon: Duration,
        max_payload_bytes: usize,
        frame_overhead_bytes: usize,
        forced_tail_fragments: usize,
    ) -> Result<Self, CapacityError> {
        if rate_bits_per_second == 0 {
            return Err(CapacityError::ZeroRate);
        }
        if horizon.is_zero() {
            return Err(CapacityError::ZeroHorizon);
        }
        if max_payload_bytes == 0 {
            return Err(CapacityError::ZeroPayloadWidth);
        }
        if forced_tail_fragments == 0 {
            return Err(CapacityError::ZeroTailFragments);
        }
        Ok(Self {
            rate_bits_per_second,
            horizon,
            max_payload_bytes,
            frame_overhead_bytes,
            forced_tail_fragments,
            replay_copies: 0,
            duplicates_per_send: 0,
            fixed_events: 0,
            fixed_bytes: 0,
        })
    }

    /// Declares at most one replay and one injector duplicate for each send.
    pub(crate) fn with_fault_copies(
        mut self,
        replay_copies: u8,
        duplicates_per_send: u8,
    ) -> Result<Self, CapacityError> {
        if replay_copies > 1 {
            return Err(CapacityError::TooManyReplayCopies);
        }
        if duplicates_per_send > 1 {
            return Err(CapacityError::TooManyDuplicateCopies);
        }
        self.replay_copies = replay_copies;
        self.duplicates_per_send = duplicates_per_send;
        Ok(self)
    }

    /// Adds exact non-DATA transport sends (attach, ACK, probe, FIN/reset)
    /// that are not part of the rate-derived application payload. Injector
    /// duplication applies to these sends just as it does to DATA.
    pub(crate) const fn with_fixed_work(mut self, messages: usize, bytes: usize) -> Self {
        self.fixed_events = messages;
        self.fixed_bytes = bytes;
        self
    }

    pub(crate) fn derive(self) -> Result<WireCapacity, CapacityError> {
        let numerator = u128::from(self.rate_bits_per_second)
            .checked_mul(self.horizon.as_nanos())
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let quotient = numerator / RATE_DENOMINATOR;
        let remainder = numerator % RATE_DENOMINATOR;
        let application_bytes = quotient
            .checked_add(u128::from(remainder != 0))
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let application_bytes = u64::try_from(application_bytes)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?;
        let payload_width = u64::try_from(self.max_payload_bytes)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?;
        let forced_tail = u64::try_from(self.forced_tail_fragments)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?
            .min(application_bytes);
        let logical_messages = application_bytes
            .checked_sub(forced_tail)
            .ok_or(CapacityError::ArithmeticOverflow)?
            / payload_width
            + forced_tail;
        let overhead = u64::try_from(self.frame_overhead_bytes)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?;
        let encoded_once_bytes = logical_messages
            .checked_mul(overhead)
            .and_then(|value| value.checked_add(application_bytes))
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let send_factor = u64::from(self.replay_copies) + 1;
        let delivery_factor = u64::from(self.duplicates_per_send) + 1;
        let data_send_messages = logical_messages
            .checked_mul(send_factor)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let data_send_bytes = encoded_once_bytes
            .checked_mul(send_factor)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let fixed_messages = u64::try_from(self.fixed_events)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?;
        let fixed_bytes =
            u64::try_from(self.fixed_bytes).map_err(|_| CapacityError::PlatformCapacityOverflow)?;
        let max_send_messages = data_send_messages
            .checked_add(fixed_messages)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let max_send_bytes = data_send_bytes
            .checked_add(fixed_bytes)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let max_delivery_messages = max_send_messages
            .checked_mul(delivery_factor)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let max_delivery_bytes = max_send_bytes
            .checked_mul(delivery_factor)
            .ok_or(CapacityError::ArithmeticOverflow)?;

        Ok(WireCapacity {
            application_bytes,
            logical_messages,
            encoded_once_bytes,
            delivery_factor,
            max_send_messages,
            max_delivery_messages,
            max_send_bytes,
            max_delivery_bytes,
            max_events: usize::try_from(max_delivery_messages)
                .map_err(|_| CapacityError::PlatformCapacityOverflow)?,
            max_owned_bytes: usize::try_from(max_delivery_bytes)
                .map_err(|_| CapacityError::PlatformCapacityOverflow)?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WireCapacity {
    application_bytes: u64,
    logical_messages: u64,
    encoded_once_bytes: u64,
    delivery_factor: u64,
    max_send_messages: u64,
    max_delivery_messages: u64,
    max_send_bytes: u64,
    max_delivery_bytes: u64,
    max_events: usize,
    max_owned_bytes: usize,
}

impl WireCapacity {
    pub(crate) const fn application_bytes(self) -> u64 {
        self.application_bytes
    }

    pub(crate) const fn logical_messages(self) -> u64 {
        self.logical_messages
    }

    pub(crate) const fn encoded_once_bytes(self) -> u64 {
        self.encoded_once_bytes
    }

    pub(crate) const fn delivery_factor(self) -> u64 {
        self.delivery_factor
    }

    pub(crate) const fn max_send_messages(self) -> u64 {
        self.max_send_messages
    }

    pub(crate) const fn max_delivery_messages(self) -> u64 {
        self.max_delivery_messages
    }

    pub(crate) const fn max_send_bytes(self) -> u64 {
        self.max_send_bytes
    }

    pub(crate) const fn max_delivery_bytes(self) -> u64 {
        self.max_delivery_bytes
    }

    pub(crate) const fn wire_event_budget(self) -> EventBudget {
        EventBudget {
            max_events: self.max_events,
            max_owned_bytes: self.max_owned_bytes,
        }
    }
}

/// Fixed non-DATA actor actions declared by one deterministic scenario.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HarnessFixedWork {
    attach_turns: usize,
    probe_turns: usize,
    timer_turns: usize,
    close_turns: usize,
    reset_turns: usize,
    join_cleanup_turns: usize,
    adapter_turns: usize,
}

impl HarnessFixedWork {
    pub(crate) const fn new(
        attach_turns: usize,
        probe_turns: usize,
        timer_turns: usize,
        close_turns: usize,
        reset_turns: usize,
        join_cleanup_turns: usize,
    ) -> Self {
        Self {
            attach_turns,
            probe_turns,
            timer_turns,
            close_turns,
            reset_turns,
            join_cleanup_turns,
            adapter_turns: 0,
        }
    }

    pub(crate) const fn with_adapter_turns(mut self, adapter_turns: usize) -> Self {
        self.adapter_turns = adapter_turns;
        self
    }

    pub(crate) fn turns(self) -> Result<usize, CapacityError> {
        [
            self.attach_turns,
            self.probe_turns,
            self.timer_turns,
            self.close_turns,
            self.reset_turns,
            self.join_cleanup_turns,
            self.adapter_turns,
        ]
        .into_iter()
        .try_fold(0usize, |total, turns| {
            total
                .checked_add(turns)
                .ok_or(CapacityError::ArithmeticOverflow)
        })
    }
}

/// Complete pure inputs for the one-global-scheduler work bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HarnessWorkSpec {
    wire: WireCapacity,
    directions: usize,
    max_positive_accept_pieces: usize,
    max_zero_would_block: usize,
    max_ack_constructs_per_direction: usize,
    fixed: HarnessFixedWork,
    max_non_wire_owned_bytes: usize,
}

impl HarnessWorkSpec {
    pub(crate) fn new(
        wire: WireCapacity,
        directions: usize,
        max_positive_accept_pieces: usize,
        max_zero_would_block: usize,
        max_ack_constructs_per_direction: usize,
        fixed: HarnessFixedWork,
        max_non_wire_owned_bytes: usize,
    ) -> Result<Self, CapacityError> {
        if directions == 0 {
            return Err(CapacityError::ZeroDirectionCount);
        }
        if max_positive_accept_pieces == 0 {
            return Err(CapacityError::ZeroPositiveAcceptancePieces);
        }
        Ok(Self {
            wire,
            directions,
            max_positive_accept_pieces,
            max_zero_would_block,
            max_ack_constructs_per_direction,
            fixed,
            max_non_wire_owned_bytes,
        })
    }

    pub(crate) fn derive(self) -> Result<HarnessWorkBudget, CapacityError> {
        let logical_messages = usize::try_from(self.wire.logical_messages)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?;
        let wire_send_turns = usize::try_from(self.wire.max_send_messages)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?
            .checked_mul(self.directions)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let wire_delivery_turns = usize::try_from(self.wire.max_delivery_messages)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?
            .checked_mul(self.directions)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let logical_across_directions = logical_messages
            .checked_mul(self.directions)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let positive_accept_turns = logical_across_directions
            .checked_mul(self.max_positive_accept_pieces)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let zero_would_block_turns = logical_across_directions
            .checked_mul(self.max_zero_would_block)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let ack_construct_turns = self
            .max_ack_constructs_per_direction
            .checked_mul(self.directions)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let delivery_factor = usize::try_from(self.wire.delivery_factor)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?;
        let ack_decode_turns = ack_construct_turns
            .checked_mul(delivery_factor)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let fixed_actor_turns = self.fixed.turns()?;
        let categories = HarnessWorkCategories {
            wire_send_turns,
            wire_delivery_turns,
            positive_accept_turns,
            zero_would_block_turns,
            ack_construct_turns,
            ack_decode_turns,
            fixed_actor_turns,
        };
        let max_turns = categories.total()?;

        // The wire totals are cumulative scheduler ownership for outbound
        // admission plus fault-delivered copies. All Target/sink/supervisor
        // action ownership must be declared separately; P/Z alone cannot
        // infer how many backing bytes those actors retain.
        let max_wire_send_owned_bytes = usize::try_from(self.wire.max_send_bytes)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?
            .checked_mul(self.directions)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let max_wire_delivery_owned_bytes = usize::try_from(self.wire.max_delivery_bytes)
            .map_err(|_| CapacityError::PlatformCapacityOverflow)?
            .checked_mul(self.directions)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        let max_owned_bytes = max_wire_send_owned_bytes
            .checked_add(max_wire_delivery_owned_bytes)
            .ok_or(CapacityError::ArithmeticOverflow)?
            .checked_add(self.max_non_wire_owned_bytes)
            .ok_or(CapacityError::ArithmeticOverflow)?;

        Ok(HarnessWorkBudget {
            categories,
            max_turns,
            max_owned_bytes,
            max_wire_send_owned_bytes,
            max_wire_delivery_owned_bytes,
            max_non_wire_owned_bytes: self.max_non_wire_owned_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HarnessWorkCategories {
    wire_send_turns: usize,
    wire_delivery_turns: usize,
    positive_accept_turns: usize,
    zero_would_block_turns: usize,
    ack_construct_turns: usize,
    ack_decode_turns: usize,
    fixed_actor_turns: usize,
}

impl HarnessWorkCategories {
    fn total(self) -> Result<usize, CapacityError> {
        [
            self.wire_send_turns,
            self.wire_delivery_turns,
            self.positive_accept_turns,
            self.zero_would_block_turns,
            self.ack_construct_turns,
            self.ack_decode_turns,
            self.fixed_actor_turns,
        ]
        .into_iter()
        .try_fold(0usize, |total, turns| {
            total
                .checked_add(turns)
                .ok_or(CapacityError::ArithmeticOverflow)
        })
    }

    pub(crate) const fn wire_send_turns(self) -> usize {
        self.wire_send_turns
    }

    pub(crate) const fn wire_delivery_turns(self) -> usize {
        self.wire_delivery_turns
    }

    pub(crate) const fn positive_accept_turns(self) -> usize {
        self.positive_accept_turns
    }

    pub(crate) const fn zero_would_block_turns(self) -> usize {
        self.zero_would_block_turns
    }

    pub(crate) const fn ack_construct_turns(self) -> usize {
        self.ack_construct_turns
    }

    pub(crate) const fn ack_decode_turns(self) -> usize {
        self.ack_decode_turns
    }

    pub(crate) const fn fixed_actor_turns(self) -> usize {
        self.fixed_actor_turns
    }

    const fn turns(self, category: HarnessWorkCategory) -> usize {
        match category {
            HarnessWorkCategory::WireSend => self.wire_send_turns,
            HarnessWorkCategory::WireDelivery => self.wire_delivery_turns,
            HarnessWorkCategory::PositiveAccept => self.positive_accept_turns,
            HarnessWorkCategory::ZeroWouldBlock => self.zero_would_block_turns,
            HarnessWorkCategory::AckConstruct => self.ack_construct_turns,
            HarnessWorkCategory::AckDecode => self.ack_decode_turns,
            HarnessWorkCategory::FixedActor => self.fixed_actor_turns,
        }
    }
}

/// One independently enforced work class in a deterministic harness scenario.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HarnessWorkCategory {
    WireSend,
    WireDelivery,
    PositiveAccept,
    ZeroWouldBlock,
    AckConstruct,
    AckDecode,
    FixedActor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HarnessOwnedByteCategory {
    WireSend,
    WireDelivery,
    NonWire,
}

impl HarnessOwnedByteCategory {
    const ALL: [Self; 3] = [Self::WireSend, Self::WireDelivery, Self::NonWire];
}

impl HarnessWorkCategory {
    const ALL: [Self; 7] = [
        Self::WireSend,
        Self::WireDelivery,
        Self::PositiveAccept,
        Self::ZeroWouldBlock,
        Self::AckConstruct,
        Self::AckDecode,
        Self::FixedActor,
    ];
}

/// Monotonic observed work. Every charge is checked against its own declared
/// category, so unused work in another class cannot hide an overrun.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct HarnessObservedWork {
    categories: HarnessWorkCategories,
    wire_send_owned_bytes: usize,
    wire_delivery_owned_bytes: usize,
    non_wire_owned_bytes: usize,
}

impl HarnessObservedWork {
    pub(crate) fn charged(
        mut self,
        budget: HarnessWorkBudget,
        category: HarnessWorkCategory,
        turns: usize,
    ) -> Result<Self, CapacityError> {
        self.charge(budget, category, turns)?;
        Ok(self)
    }

    pub(crate) fn charge(
        &mut self,
        budget: HarnessWorkBudget,
        category: HarnessWorkCategory,
        turns: usize,
    ) -> Result<(), CapacityError> {
        let next = self
            .categories
            .turns(category)
            .checked_add(turns)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        if next > budget.categories.turns(category) {
            return Err(CapacityError::HarnessWorkCategoryExceeded { category });
        }
        match category {
            HarnessWorkCategory::WireSend => self.categories.wire_send_turns = next,
            HarnessWorkCategory::WireDelivery => self.categories.wire_delivery_turns = next,
            HarnessWorkCategory::PositiveAccept => self.categories.positive_accept_turns = next,
            HarnessWorkCategory::ZeroWouldBlock => {
                self.categories.zero_would_block_turns = next;
            }
            HarnessWorkCategory::AckConstruct => self.categories.ack_construct_turns = next,
            HarnessWorkCategory::AckDecode => self.categories.ack_decode_turns = next,
            HarnessWorkCategory::FixedActor => self.categories.fixed_actor_turns = next,
        }
        Ok(())
    }

    pub(crate) fn total(self) -> Result<usize, CapacityError> {
        self.categories.total()
    }

    pub(crate) fn charged_owned_bytes(
        mut self,
        budget: HarnessWorkBudget,
        category: HarnessOwnedByteCategory,
        bytes: usize,
    ) -> Result<Self, CapacityError> {
        self.charge_owned_bytes(budget, category, bytes)?;
        Ok(self)
    }

    pub(crate) fn charge_owned_bytes(
        &mut self,
        budget: HarnessWorkBudget,
        category: HarnessOwnedByteCategory,
        bytes: usize,
    ) -> Result<(), CapacityError> {
        let observed = match category {
            HarnessOwnedByteCategory::WireSend => self.wire_send_owned_bytes,
            HarnessOwnedByteCategory::WireDelivery => self.wire_delivery_owned_bytes,
            HarnessOwnedByteCategory::NonWire => self.non_wire_owned_bytes,
        };
        let next = observed
            .checked_add(bytes)
            .ok_or(CapacityError::ArithmeticOverflow)?;
        if next > budget.max_owned_bytes_for(category) {
            return Err(CapacityError::HarnessOwnedByteCategoryExceeded { category });
        }
        match category {
            HarnessOwnedByteCategory::WireSend => self.wire_send_owned_bytes = next,
            HarnessOwnedByteCategory::WireDelivery => self.wire_delivery_owned_bytes = next,
            HarnessOwnedByteCategory::NonWire => self.non_wire_owned_bytes = next,
        }
        Ok(())
    }

    pub(crate) const fn categories(self) -> HarnessWorkCategories {
        self.categories
    }

    pub(crate) const fn zero_would_block_turns(self) -> usize {
        self.categories.zero_would_block_turns()
    }

    pub(crate) const fn non_wire_owned_bytes(self) -> usize {
        self.non_wire_owned_bytes
    }

    pub(crate) const fn wire_send_owned_bytes(self) -> usize {
        self.wire_send_owned_bytes
    }

    pub(crate) const fn wire_delivery_owned_bytes(self) -> usize {
        self.wire_delivery_owned_bytes
    }

    pub(crate) fn total_owned_bytes(self) -> Result<usize, CapacityError> {
        self.wire_send_owned_bytes
            .checked_add(self.wire_delivery_owned_bytes)
            .and_then(|bytes| bytes.checked_add(self.non_wire_owned_bytes))
            .ok_or(CapacityError::ArithmeticOverflow)
    }

    const fn owned_bytes(self, category: HarnessOwnedByteCategory) -> usize {
        match category {
            HarnessOwnedByteCategory::WireSend => self.wire_send_owned_bytes,
            HarnessOwnedByteCategory::WireDelivery => self.wire_delivery_owned_bytes,
            HarnessOwnedByteCategory::NonWire => self.non_wire_owned_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HarnessWorkBudget {
    categories: HarnessWorkCategories,
    max_turns: usize,
    max_owned_bytes: usize,
    max_wire_send_owned_bytes: usize,
    max_wire_delivery_owned_bytes: usize,
    max_non_wire_owned_bytes: usize,
}

impl HarnessWorkBudget {
    pub(crate) const fn categories(self) -> HarnessWorkCategories {
        self.categories
    }

    pub(crate) const fn max_turns(self) -> usize {
        self.max_turns
    }

    pub(crate) const fn max_owned_bytes(self) -> usize {
        self.max_owned_bytes
    }

    pub(crate) const fn max_non_wire_owned_bytes(self) -> usize {
        self.max_non_wire_owned_bytes
    }

    pub(crate) const fn max_wire_send_owned_bytes(self) -> usize {
        self.max_wire_send_owned_bytes
    }

    pub(crate) const fn max_wire_delivery_owned_bytes(self) -> usize {
        self.max_wire_delivery_owned_bytes
    }

    const fn max_owned_bytes_for(self, category: HarnessOwnedByteCategory) -> usize {
        match category {
            HarnessOwnedByteCategory::WireSend => self.max_wire_send_owned_bytes,
            HarnessOwnedByteCategory::WireDelivery => self.max_wire_delivery_owned_bytes,
            HarnessOwnedByteCategory::NonWire => self.max_non_wire_owned_bytes,
        }
    }

    pub(crate) const fn event_budget(self) -> EventBudget {
        EventBudget {
            max_events: self.max_turns,
            max_owned_bytes: self.max_owned_bytes,
        }
    }

    pub(crate) fn validate_observed(
        self,
        observed: HarnessObservedWork,
    ) -> Result<(), CapacityError> {
        for category in HarnessWorkCategory::ALL {
            if observed.categories.turns(category) > self.categories.turns(category) {
                return Err(CapacityError::HarnessWorkCategoryExceeded { category });
            }
        }
        if observed.total()? > self.max_turns {
            return Err(CapacityError::HarnessWorkTotalExceeded);
        }
        for category in HarnessOwnedByteCategory::ALL {
            if observed.owned_bytes(category) > self.max_owned_bytes_for(category) {
                return Err(CapacityError::HarnessOwnedByteCategoryExceeded { category });
            }
        }
        if observed.total_owned_bytes()? > self.max_owned_bytes {
            return Err(CapacityError::HarnessOwnedByteTotalExceeded);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CapacityError {
    ZeroRate,
    ZeroHorizon,
    ZeroPayloadWidth,
    ZeroTailFragments,
    TooManyReplayCopies,
    TooManyDuplicateCopies,
    ZeroDirectionCount,
    ZeroPositiveAcceptancePieces,
    HarnessWorkCategoryExceeded { category: HarnessWorkCategory },
    HarnessWorkTotalExceeded,
    HarnessOwnedByteCategoryExceeded { category: HarnessOwnedByteCategory },
    HarnessOwnedByteTotalExceeded,
    ArithmeticOverflow,
    PlatformCapacityOverflow,
}

impl fmt::Display for CapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRate => f.write_str("scenario rate must be nonzero"),
            Self::ZeroHorizon => f.write_str("scenario horizon must be nonzero"),
            Self::ZeroPayloadWidth => f.write_str("scenario payload width must be nonzero"),
            Self::ZeroTailFragments => {
                f.write_str("scenario must reserve at least one tail fragment")
            }
            Self::TooManyReplayCopies => f.write_str("scenario permits at most one replay copy"),
            Self::TooManyDuplicateCopies => {
                f.write_str("fault injector permits at most one duplicate per send")
            }
            Self::ZeroDirectionCount => f.write_str("harness direction count must be nonzero"),
            Self::ZeroPositiveAcceptancePieces => {
                f.write_str("harness positive-acceptance bound must be nonzero")
            }
            Self::HarnessWorkCategoryExceeded { category } => {
                write!(f, "harness work exceeded the declared {category:?} bound")
            }
            Self::HarnessWorkTotalExceeded => {
                f.write_str("harness work exceeded the declared total turn bound")
            }
            Self::HarnessOwnedByteCategoryExceeded { category } => {
                write!(
                    f,
                    "harness owned bytes exceeded the declared {category:?} bound"
                )
            }
            Self::HarnessOwnedByteTotalExceeded => {
                f.write_str("harness owned bytes exceeded the declared total bound")
            }
            Self::ArithmeticOverflow => f.write_str("scenario capacity arithmetic overflow"),
            Self::PlatformCapacityOverflow => {
                f.write_str("scenario capacity exceeds platform limits")
            }
        }
    }
}

impl Error for CapacityError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum LegId {
    A,
    B,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum WireDirection {
    ClientToOwner,
    OwnerToClient,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct WireRoute {
    leg: LegId,
    direction: WireDirection,
}

impl WireRoute {
    pub(crate) const fn new(leg: LegId, direction: WireDirection) -> Self {
        Self { leg, direction }
    }

    pub(crate) const fn leg(self) -> LegId {
        self.leg
    }

    pub(crate) const fn direction(self) -> WireDirection {
        self.direction
    }

    const fn index(self) -> usize {
        let leg = match self.leg {
            LegId::A => 0,
            LegId::B => 1,
        };
        let direction = match self.direction {
            WireDirection::ClientToOwner => 0,
            WireDirection::OwnerToClient => 1,
        };
        leg * 2 + direction
    }

    const fn actor_id(self) -> ActorId {
        ActorId::new(self.index() as u32 + 1)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum WireLane {
    Control,
    Data,
}

impl WireLane {
    const fn index(self) -> usize {
        match self {
            Self::Control => 0,
            Self::Data => 1,
        }
    }
}

/// Faults are applied only to owned encoded byte messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FaultAction {
    Pass,
    Drop,
    Duplicate,
    Delay { release_at: SimTime },
    ReorderAdjacent,
    Hold { token: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WireBounds {
    wire_capacity: WireCapacity,
    max_live_messages: usize,
    max_live_bytes: usize,
    max_blackouts: usize,
    max_held_messages: usize,
}

impl WireBounds {
    pub(crate) fn new(
        wire_capacity: WireCapacity,
        max_live_messages: usize,
        max_live_bytes: usize,
        max_blackouts: usize,
        max_held_messages: usize,
    ) -> Result<Self, WireError> {
        if max_live_messages == 0 || max_live_messages > wire_capacity.max_events {
            return Err(WireError::InvalidLiveMessageBound);
        }
        if max_live_bytes == 0 || max_live_bytes > wire_capacity.max_owned_bytes {
            return Err(WireError::InvalidLiveByteBound);
        }
        Ok(Self {
            wire_capacity,
            max_live_messages,
            max_live_bytes,
            max_blackouts,
            max_held_messages,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Blackout {
    route: WireRoute,
    start: SimTime,
    end: SimTime,
}

impl Blackout {
    const fn contains(self, route: WireRoute, now: SimTime) -> bool {
        self.route.leg as u8 == route.leg as u8
            && self.route.direction as u8 == route.direction as u8
            && now.0 >= self.start.0
            && now.0 < self.end.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WireEnvelope {
    route: WireRoute,
    lane: WireLane,
    bytes: Vec<u8>,
    send_ordinal: u64,
    copy_index: u8,
}

/// One bounded hold token owns either a single physical message or the exact
/// `second, first` pair completed by an adjacent-reorder action.
#[derive(Clone, Debug, PartialEq, Eq)]
enum HeldWire {
    Single(WireEnvelope),
    ReorderedPair {
        second: WireEnvelope,
        first: WireEnvelope,
    },
}

impl HeldWire {
    const fn len(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::ReorderedPair { .. } => 2,
        }
    }

    fn envelopes(&self) -> [Option<&WireEnvelope>; 2] {
        match self {
            Self::Single(envelope) => [Some(envelope), None],
            Self::ReorderedPair { second, first } => [Some(second), Some(first)],
        }
    }

    fn into_envelopes(self) -> Vec<WireEnvelope> {
        match self {
            Self::Single(envelope) => vec![envelope],
            Self::ReorderedPair { second, first } => vec![second, first],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EncodedDelivery {
    route: WireRoute,
    lane: WireLane,
    bytes: Vec<u8>,
    send_ordinal: u64,
    copy_index: u8,
}

impl EncodedDelivery {
    /// Creates one byte message released by a bounded in-memory ingress.
    ///
    /// The constructor is restricted to the `owned_upstream` adapter: session
    /// controllers receive only the completed delivery and cannot mint one.
    pub(super) fn from_memory_ingress(
        route: WireRoute,
        lane: WireLane,
        bytes: Vec<u8>,
        send_ordinal: u64,
        copy_index: u8,
    ) -> Result<Self, WireError> {
        if bytes.is_empty() {
            return Err(WireError::EmptyMessage);
        }
        Ok(Self {
            route,
            lane,
            bytes,
            send_ordinal,
            copy_index,
        })
    }

    pub(crate) const fn route(&self) -> WireRoute {
        self.route
    }

    pub(crate) const fn lane(&self) -> WireLane {
        self.lane
    }

    pub(crate) const fn send_ordinal(&self) -> u64 {
        self.send_ordinal
    }

    pub(crate) const fn copy_index(&self) -> u8 {
        self.copy_index
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireCounters {
    pub(crate) submitted_messages: u64,
    pub(crate) submitted_bytes: u64,
    pub(crate) physical_messages: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) delivered_messages: u64,
    pub(crate) delivered_bytes: u64,
    pub(crate) dropped_messages: u64,
    pub(crate) dropped_bytes: u64,
    pub(crate) blackout_drops: u64,
    pub(crate) duplicate_copies: u64,
    pub(crate) delayed_messages: u64,
    pub(crate) reordered_pairs: u64,
    pub(crate) held_messages: u64,
    pub(crate) cancelled_messages: u64,
    pub(crate) cancelled_bytes: u64,
    pub(crate) capacity_rejections: u64,
    pub(crate) queue_high_water_messages: usize,
    pub(crate) queue_high_water_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireCancelSummary {
    messages: usize,
    bytes: usize,
}

impl WireCancelSummary {
    pub(crate) const fn messages(self) -> usize {
        self.messages
    }

    pub(crate) const fn bytes(self) -> usize {
        self.bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SendOutcome {
    send_ordinal: u64,
    physical_copies: u8,
    dropped: bool,
}

impl SendOutcome {
    pub(crate) const fn send_ordinal(self) -> u64 {
        self.send_ordinal
    }

    pub(crate) const fn physical_copies(self) -> u8 {
        self.physical_copies
    }

    pub(crate) const fn dropped(self) -> bool {
        self.dropped
    }
}

#[derive(Debug)]
struct RouteQueues {
    control: VecDeque<WireEnvelope>,
    data: VecDeque<WireEnvelope>,
    consecutive_control: u8,
}

impl RouteQueues {
    fn new() -> Self {
        Self {
            control: VecDeque::new(),
            data: VecDeque::new(),
            consecutive_control: 0,
        }
    }

    fn push(&mut self, envelope: WireEnvelope) {
        match envelope.lane {
            WireLane::Control => self.control.push_back(envelope),
            WireLane::Data => self.data.push_back(envelope),
        }
    }

    fn pop(&mut self) -> Option<WireEnvelope> {
        const MAX_CONTROL_BURST: u8 = 8;
        if !self.data.is_empty()
            && (self.control.is_empty() || self.consecutive_control >= MAX_CONTROL_BURST)
        {
            self.consecutive_control = 0;
            return self.data.pop_front();
        }
        if let Some(control) = self.control.pop_front() {
            self.consecutive_control = self.consecutive_control.saturating_add(1);
            return Some(control);
        }
        self.consecutive_control = 0;
        self.data.pop_front()
    }

    fn peek(&self) -> Option<&WireEnvelope> {
        const MAX_CONTROL_BURST: u8 = 8;
        if !self.data.is_empty()
            && (self.control.is_empty() || self.consecutive_control >= MAX_CONTROL_BURST)
        {
            return self.data.front();
        }
        self.control.front().or_else(|| self.data.front())
    }

    fn iter_all(&self) -> impl Iterator<Item = &WireEnvelope> {
        self.control.iter().chain(self.data.iter())
    }

    fn drain_all(&mut self) -> impl Iterator<Item = WireEnvelope> + '_ {
        self.consecutive_control = 0;
        self.control.drain(..).chain(self.data.drain(..))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WireError {
    InvalidLiveMessageBound,
    InvalidLiveByteBound,
    InvalidBlackout,
    BlackoutBudgetExceeded,
    EmptyMessage,
    SendMessageBudgetExceeded,
    SendByteBudgetExceeded,
    LiveMessageCapacityExceeded,
    LiveByteCapacityExceeded,
    HoldBudgetExceeded,
    DuplicateHoldToken,
    UnknownHoldToken,
    LiveOwnershipUnderflow,
    CounterOverflow,
    UnsupportedFaultAction,
    Schedule(ScheduleError),
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLiveMessageBound => f.write_str("invalid live wire message bound"),
            Self::InvalidLiveByteBound => f.write_str("invalid live wire byte bound"),
            Self::InvalidBlackout => f.write_str("blackout must have start before end"),
            Self::BlackoutBudgetExceeded => f.write_str("wire blackout budget exceeded"),
            Self::EmptyMessage => f.write_str("encoded wire message must be nonempty"),
            Self::SendMessageBudgetExceeded => f.write_str("scenario send-message budget exceeded"),
            Self::SendByteBudgetExceeded => f.write_str("scenario send-byte budget exceeded"),
            Self::LiveMessageCapacityExceeded => f.write_str("live wire message capacity exceeded"),
            Self::LiveByteCapacityExceeded => f.write_str("live wire byte capacity exceeded"),
            Self::HoldBudgetExceeded => f.write_str("wire hold budget exceeded"),
            Self::DuplicateHoldToken => f.write_str("wire hold token already exists"),
            Self::UnknownHoldToken => f.write_str("wire hold token does not exist"),
            Self::LiveOwnershipUnderflow => f.write_str("wire live ownership underflow"),
            Self::CounterOverflow => f.write_str("wire counter overflow"),
            Self::UnsupportedFaultAction => f.write_str("fault action is not implemented"),
            Self::Schedule(error) => write!(f, "wire scheduling failed: {error}"),
        }
    }
}

impl Error for WireError {}

impl From<ScheduleError> for WireError {
    fn from(value: ScheduleError) -> Self {
        Self::Schedule(value)
    }
}

/// Two legs with direction-local, bounded control and DATA byte queues.
pub(crate) struct TwoLegWire {
    bounds: WireBounds,
    scheduler: DeterministicScheduler<WireEnvelope>,
    routes: [RouteQueues; 4],
    reorder_slots: [Option<WireEnvelope>; 8],
    held: BTreeMap<u64, HeldWire>,
    blackouts: Vec<Blackout>,
    next_send_ordinal: u64,
    sent_messages: u64,
    sent_bytes: u64,
    live_messages: usize,
    live_bytes: usize,
    counters: WireCounters,
}

impl TwoLegWire {
    pub(crate) fn new(bounds: WireBounds) -> Self {
        Self {
            scheduler: DeterministicScheduler::new(bounds.wire_capacity.wire_event_budget()),
            bounds,
            routes: std::array::from_fn(|_| RouteQueues::new()),
            reorder_slots: std::array::from_fn(|_| None),
            held: BTreeMap::new(),
            blackouts: Vec::with_capacity(bounds.max_blackouts),
            next_send_ordinal: 0,
            sent_messages: 0,
            sent_bytes: 0,
            live_messages: 0,
            live_bytes: 0,
            counters: WireCounters::default(),
        }
    }

    pub(crate) fn add_blackout(
        &mut self,
        route: WireRoute,
        start: SimTime,
        end: SimTime,
    ) -> Result<(), WireError> {
        if start >= end {
            return Err(WireError::InvalidBlackout);
        }
        if self.blackouts.len() >= self.bounds.max_blackouts {
            return Err(WireError::BlackoutBudgetExceeded);
        }
        self.blackouts.push(Blackout { route, start, end });
        Ok(())
    }

    pub(crate) fn send(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Vec<u8>,
        action: FaultAction,
    ) -> Result<SendOutcome, WireError> {
        if bytes.is_empty() {
            return Err(WireError::EmptyMessage);
        }
        if now < self.scheduler.now() {
            return Err(WireError::Schedule(ScheduleError::TimeWentBackwards));
        }
        let len = bytes.len();
        let new_sent_messages = self
            .sent_messages
            .checked_add(1)
            .ok_or(WireError::SendMessageBudgetExceeded)?;
        if new_sent_messages > self.bounds.wire_capacity.max_send_messages {
            return self.reject(WireError::SendMessageBudgetExceeded);
        }
        let len_u64 = u64::try_from(len).map_err(|_| WireError::SendByteBudgetExceeded)?;
        let new_sent_bytes = self
            .sent_bytes
            .checked_add(len_u64)
            .ok_or(WireError::SendByteBudgetExceeded)?;
        if new_sent_bytes > self.bounds.wire_capacity.max_send_bytes {
            return self.reject(WireError::SendByteBudgetExceeded);
        }

        let send_ordinal = self.next_send_ordinal;
        let next_ordinal = self
            .next_send_ordinal
            .checked_add(1)
            .ok_or(WireError::SendMessageBudgetExceeded)?;
        let in_blackout = self
            .blackouts
            .iter()
            .any(|blackout| blackout.contains(route, now));
        let reorder_index = route.index() * 2 + lane.index();

        if self.reorder_slots[reorder_index].is_some() {
            return self.send_after_reorder_slot(
                now,
                route,
                lane,
                bytes,
                action,
                in_blackout,
                reorder_index,
                send_ordinal,
                next_ordinal,
                new_sent_messages,
                new_sent_bytes,
                len_u64,
            );
        }

        if !in_blackout && matches!(action, FaultAction::ReorderAdjacent) {
            if self.reorder_slots[reorder_index].is_some() {
                return Err(WireError::UnsupportedFaultAction);
            }
            let new_live_messages = self
                .live_messages
                .checked_add(1)
                .ok_or(WireError::LiveMessageCapacityExceeded)?;
            let new_live_bytes = self
                .live_bytes
                .checked_add(len)
                .ok_or(WireError::LiveByteCapacityExceeded)?;
            if new_live_messages > self.bounds.max_live_messages {
                return self.reject(WireError::LiveMessageCapacityExceeded);
            }
            if new_live_bytes > self.bounds.max_live_bytes {
                return self.reject(WireError::LiveByteCapacityExceeded);
            }
            self.reorder_slots[reorder_index] = Some(WireEnvelope {
                route,
                lane,
                bytes,
                send_ordinal,
                copy_index: 0,
            });
            self.live_messages = new_live_messages;
            self.live_bytes = new_live_bytes;
            self.counters.physical_messages += 1;
            self.counters.physical_bytes += len_u64;
            self.update_high_water();
            self.next_send_ordinal = next_ordinal;
            self.sent_messages = new_sent_messages;
            self.sent_bytes = new_sent_bytes;
            self.counters.submitted_messages += 1;
            self.counters.submitted_bytes += len_u64;
            return Ok(SendOutcome {
                send_ordinal,
                physical_copies: 1,
                dropped: false,
            });
        }

        if !in_blackout && let FaultAction::Hold { token } = action {
            if self.held.contains_key(&token) {
                return Err(WireError::DuplicateHoldToken);
            }
            if self.held_message_count()? >= self.bounds.max_held_messages {
                return self.reject(WireError::HoldBudgetExceeded);
            }
            let new_live_messages = self
                .live_messages
                .checked_add(1)
                .ok_or(WireError::LiveMessageCapacityExceeded)?;
            let new_live_bytes = self
                .live_bytes
                .checked_add(len)
                .ok_or(WireError::LiveByteCapacityExceeded)?;
            if new_live_messages > self.bounds.max_live_messages {
                return self.reject(WireError::LiveMessageCapacityExceeded);
            }
            if new_live_bytes > self.bounds.max_live_bytes {
                return self.reject(WireError::LiveByteCapacityExceeded);
            }
            self.held.insert(
                token,
                HeldWire::Single(WireEnvelope {
                    route,
                    lane,
                    bytes,
                    send_ordinal,
                    copy_index: 0,
                }),
            );
            self.live_messages = new_live_messages;
            self.live_bytes = new_live_bytes;
            self.counters.physical_messages += 1;
            self.counters.physical_bytes += len_u64;
            self.counters.held_messages += 1;
            self.update_high_water();
            self.next_send_ordinal = next_ordinal;
            self.sent_messages = new_sent_messages;
            self.sent_bytes = new_sent_bytes;
            self.counters.submitted_messages += 1;
            self.counters.submitted_bytes += len_u64;
            return Ok(SendOutcome {
                send_ordinal,
                physical_copies: 1,
                dropped: false,
            });
        }

        let (copies, delivery_at, phase) = if in_blackout || matches!(action, FaultAction::Drop) {
            (0, now, EventPhase::TransportReceive)
        } else {
            match action {
                FaultAction::Pass => (1, now, EventPhase::TransportReceive),
                FaultAction::Duplicate => (2, now, EventPhase::TransportReceive),
                FaultAction::Drop => (0, now, EventPhase::TransportReceive),
                FaultAction::Delay { release_at } => {
                    if release_at < now {
                        return Err(WireError::Schedule(ScheduleError::TimeWentBackwards));
                    }
                    (1, release_at, EventPhase::FaultRelease)
                }
                FaultAction::ReorderAdjacent => unreachable!("handled above"),
                FaultAction::Hold { .. } => unreachable!("handled above"),
            }
        };

        if copies > 0 {
            let new_live_messages = self
                .live_messages
                .checked_add(copies)
                .ok_or(WireError::LiveMessageCapacityExceeded)?;
            let copy_bytes = len
                .checked_mul(copies)
                .ok_or(WireError::LiveByteCapacityExceeded)?;
            let new_live_bytes = self
                .live_bytes
                .checked_add(copy_bytes)
                .ok_or(WireError::LiveByteCapacityExceeded)?;
            if new_live_messages > self.bounds.max_live_messages {
                return self.reject(WireError::LiveMessageCapacityExceeded);
            }
            if new_live_bytes > self.bounds.max_live_bytes {
                return self.reject(WireError::LiveByteCapacityExceeded);
            }

            let mut batch = Vec::with_capacity(copies);
            for copy_index in 0..copies {
                let envelope = WireEnvelope {
                    route,
                    lane,
                    bytes: bytes.clone(),
                    send_ordinal,
                    copy_index: copy_index as u8,
                };
                batch.push(EventSpec::new(
                    delivery_at,
                    phase,
                    route.actor_id(),
                    len,
                    wire_trace_tag(route, lane, send_ordinal, copy_index as u8),
                    envelope,
                ));
            }
            self.scheduler.schedule_batch(batch)?;
            self.live_messages = new_live_messages;
            self.live_bytes = new_live_bytes;
            self.counters.physical_messages += copies as u64;
            self.counters.physical_bytes += copy_bytes as u64;
            if copies == 2 {
                self.counters.duplicate_copies += 1;
            }
            if matches!(action, FaultAction::Delay { .. }) {
                self.counters.delayed_messages += 1;
            }
            self.update_high_water();
        } else {
            self.counters.dropped_messages += 1;
            self.counters.dropped_bytes += len_u64;
            if in_blackout {
                self.counters.blackout_drops += 1;
            }
        }

        self.next_send_ordinal = next_ordinal;
        self.sent_messages = new_sent_messages;
        self.sent_bytes = new_sent_bytes;
        self.counters.submitted_messages += 1;
        self.counters.submitted_bytes += len_u64;
        Ok(SendOutcome {
            send_ordinal,
            physical_copies: copies as u8,
            dropped: copies == 0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn send_after_reorder_slot(
        &mut self,
        now: SimTime,
        route: WireRoute,
        lane: WireLane,
        bytes: Vec<u8>,
        action: FaultAction,
        in_blackout: bool,
        reorder_index: usize,
        send_ordinal: u64,
        next_ordinal: u64,
        new_sent_messages: u64,
        new_sent_bytes: u64,
        len_u64: u64,
    ) -> Result<SendOutcome, WireError> {
        let first = self.reorder_slots[reorder_index]
            .as_ref()
            .cloned()
            .ok_or(WireError::UnsupportedFaultAction)?;

        if matches!(action, FaultAction::ReorderAdjacent) {
            return Err(WireError::UnsupportedFaultAction);
        }
        if !in_blackout
            && let FaultAction::Delay { release_at } = action
            && release_at < now
        {
            return Err(WireError::Schedule(ScheduleError::TimeWentBackwards));
        }
        if !in_blackout && let FaultAction::Hold { token } = action {
            if self.held.contains_key(&token) {
                return Err(WireError::DuplicateHoldToken);
            }
            let held_messages = self
                .held_message_count()?
                .checked_add(2)
                .ok_or(WireError::HoldBudgetExceeded)?;
            if held_messages > self.bounds.max_held_messages {
                return self.reject(WireError::HoldBudgetExceeded);
            }
        }

        let copies = if in_blackout || matches!(action, FaultAction::Drop) {
            0usize
        } else {
            match action {
                FaultAction::Pass | FaultAction::Delay { .. } | FaultAction::Hold { .. } => 1,
                FaultAction::Duplicate => 2,
                FaultAction::Drop | FaultAction::ReorderAdjacent => 0,
            }
        };
        let copy_bytes = bytes
            .len()
            .checked_mul(copies)
            .ok_or(WireError::LiveByteCapacityExceeded)?;
        let new_live_messages = self
            .live_messages
            .checked_add(copies)
            .ok_or(WireError::LiveMessageCapacityExceeded)?;
        let new_live_bytes = self
            .live_bytes
            .checked_add(copy_bytes)
            .ok_or(WireError::LiveByteCapacityExceeded)?;
        if new_live_messages > self.bounds.max_live_messages {
            return self.reject(WireError::LiveMessageCapacityExceeded);
        }
        if new_live_bytes > self.bounds.max_live_bytes {
            return self.reject(WireError::LiveByteCapacityExceeded);
        }
        let physical_bytes = len_u64
            .checked_mul(u64::try_from(copies).map_err(|_| WireError::LiveByteCapacityExceeded)?)
            .ok_or(WireError::LiveByteCapacityExceeded)?;

        let completed_pair = !in_blackout
            && matches!(
                action,
                FaultAction::Pass
                    | FaultAction::Duplicate
                    | FaultAction::Delay { .. }
                    | FaultAction::Hold { .. }
            );
        if !in_blackout && let FaultAction::Hold { token } = action {
            let second = WireEnvelope {
                route,
                lane,
                bytes,
                send_ordinal,
                copy_index: 0,
            };
            self.held
                .insert(token, HeldWire::ReorderedPair { second, first });
        } else {
            let (delivery_at, phase) = match action {
                FaultAction::Delay { release_at } if !in_blackout => {
                    (release_at, EventPhase::FaultRelease)
                }
                _ => (now, EventPhase::TransportReceive),
            };
            let mut batch = Vec::with_capacity(copies.saturating_add(1));
            for copy_index in 0..copies {
                let envelope = WireEnvelope {
                    route,
                    lane,
                    bytes: bytes.clone(),
                    send_ordinal,
                    copy_index: u8::try_from(copy_index)
                        .map_err(|_| WireError::UnsupportedFaultAction)?,
                };
                batch.push(EventSpec::new(
                    delivery_at,
                    phase,
                    route.actor_id(),
                    envelope.bytes.len(),
                    wire_trace_tag(route, lane, send_ordinal, envelope.copy_index),
                    envelope,
                ));
            }
            batch.push(EventSpec::new(
                delivery_at,
                phase,
                route.actor_id(),
                first.bytes.len(),
                wire_trace_tag(
                    first.route,
                    first.lane,
                    first.send_ordinal,
                    first.copy_index,
                ),
                first,
            ));
            self.scheduler.schedule_batch(batch)?;
        }

        self.reorder_slots[reorder_index] = None;
        self.live_messages = new_live_messages;
        self.live_bytes = new_live_bytes;
        self.counters.physical_messages += copies as u64;
        self.counters.physical_bytes += physical_bytes;
        if copies == 2 {
            self.counters.duplicate_copies += 1;
        }
        if !in_blackout && matches!(action, FaultAction::Delay { .. }) {
            self.counters.delayed_messages += 1;
        }
        if !in_blackout && matches!(action, FaultAction::Hold { .. }) {
            self.counters.held_messages += 1;
        }
        if completed_pair {
            self.counters.reordered_pairs += 1;
        }
        if copies == 0 {
            self.counters.dropped_messages += 1;
            self.counters.dropped_bytes += len_u64;
            if in_blackout {
                self.counters.blackout_drops += 1;
            }
        }
        self.update_high_water();
        self.next_send_ordinal = next_ordinal;
        self.sent_messages = new_sent_messages;
        self.sent_bytes = new_sent_bytes;
        self.counters.submitted_messages += 1;
        self.counters.submitted_bytes += len_u64;
        Ok(SendOutcome {
            send_ordinal,
            physical_copies: u8::try_from(copies).map_err(|_| WireError::UnsupportedFaultAction)?,
            dropped: copies == 0,
        })
    }

    fn held_message_count(&self) -> Result<usize, WireError> {
        self.held.values().try_fold(0usize, |total, held| {
            total
                .checked_add(held.len())
                .ok_or(WireError::HoldBudgetExceeded)
        })
    }

    pub(crate) fn release_hold(
        &mut self,
        token: u64,
        release_at: SimTime,
    ) -> Result<(), WireError> {
        if release_at < self.scheduler.now() {
            return Err(WireError::Schedule(ScheduleError::TimeWentBackwards));
        }
        let held = self
            .held
            .get(&token)
            .cloned()
            .ok_or(WireError::UnknownHoldToken)?;
        let batch = held
            .into_envelopes()
            .into_iter()
            .map(|envelope| {
                EventSpec::new(
                    release_at,
                    EventPhase::FaultRelease,
                    envelope.route.actor_id(),
                    envelope.bytes.len(),
                    wire_trace_tag(
                        envelope.route,
                        envelope.lane,
                        envelope.send_ordinal,
                        envelope.copy_index,
                    ),
                    envelope,
                )
            })
            .collect();
        self.scheduler.schedule_batch(batch)?;
        self.held.remove(&token);
        Ok(())
    }

    /// Cancels every encoded message currently owned by one transport leg.
    ///
    /// The configured fault script/blackout remains intact so a later leg
    /// incarnation cannot silently escape the scenario. Only live message
    /// ownership is released.
    pub(crate) fn cancel_leg(&mut self, leg: LegId) -> Result<WireCancelSummary, WireError> {
        self.cancel_matching(Some(leg))
    }

    pub(crate) fn cancel_all(&mut self) -> Result<WireCancelSummary, WireError> {
        self.cancel_matching(None)
    }

    fn cancel_matching(&mut self, leg: Option<LegId>) -> Result<WireCancelSummary, WireError> {
        let matches = |route: WireRoute| leg.is_none_or(|expected| route.leg == expected);
        let mut messages = 0usize;
        let mut bytes = 0usize;
        {
            let mut add = |envelope: &WireEnvelope| -> Result<(), WireError> {
                if !matches(envelope.route) {
                    return Ok(());
                }
                messages = messages
                    .checked_add(1)
                    .ok_or(WireError::LiveMessageCapacityExceeded)?;
                bytes = bytes
                    .checked_add(envelope.bytes.len())
                    .ok_or(WireError::LiveByteCapacityExceeded)?;
                Ok(())
            };
            for envelope in self.reorder_slots.iter().flatten() {
                add(envelope)?;
            }
            for held in self.held.values() {
                for envelope in held.envelopes().into_iter().flatten() {
                    add(envelope)?;
                }
            }
            for route in &self.routes {
                for envelope in route.iter_all() {
                    add(envelope)?;
                }
            }
        }
        let (scheduled_messages, scheduled_bytes) = self
            .scheduler
            .matching_ownership(|envelope| matches(envelope.route))?;
        messages = messages
            .checked_add(scheduled_messages)
            .ok_or(WireError::LiveMessageCapacityExceeded)?;
        bytes = bytes
            .checked_add(scheduled_bytes)
            .ok_or(WireError::LiveByteCapacityExceeded)?;

        let new_live_messages = self
            .live_messages
            .checked_sub(messages)
            .ok_or(WireError::LiveOwnershipUnderflow)?;
        let new_live_bytes = self
            .live_bytes
            .checked_sub(bytes)
            .ok_or(WireError::LiveOwnershipUnderflow)?;
        let messages_u64 =
            u64::try_from(messages).map_err(|_| WireError::LiveMessageCapacityExceeded)?;
        let bytes_u64 = u64::try_from(bytes).map_err(|_| WireError::LiveByteCapacityExceeded)?;
        let cancelled_messages = self
            .counters
            .cancelled_messages
            .checked_add(messages_u64)
            .ok_or(WireError::LiveMessageCapacityExceeded)?;
        let cancelled_bytes = self
            .counters
            .cancelled_bytes
            .checked_add(bytes_u64)
            .ok_or(WireError::LiveByteCapacityExceeded)?;

        // Scheduler cancellation performs its own subtraction preflight and
        // mutates only after every fallible check. Everything below this call
        // is an infallible ownership move/drop.
        drop(
            self.scheduler
                .cancel_where(|envelope| matches(envelope.route))?,
        );
        for slot in &mut self.reorder_slots {
            if slot
                .as_ref()
                .is_some_and(|envelope| matches(envelope.route))
            {
                *slot = None;
            }
        }
        self.held.retain(|_, held| {
            !held
                .envelopes()
                .into_iter()
                .flatten()
                .any(|envelope| matches(envelope.route))
        });
        for route in &mut self.routes {
            if route
                .iter_all()
                .next()
                .is_some_and(|envelope| matches(envelope.route))
            {
                for _ in route.drain_all() {}
            }
        }
        self.live_messages = new_live_messages;
        self.live_bytes = new_live_bytes;
        self.counters.cancelled_messages = cancelled_messages;
        self.counters.cancelled_bytes = cancelled_bytes;
        Ok(WireCancelSummary { messages, bytes })
    }

    /// Releases at most one due physical wire message. The global harness
    /// scheduler owns repeated service; this method cannot bulk-drain a leg.
    pub(crate) fn advance_one_due(&mut self, through: SimTime) -> Result<bool, WireError> {
        let Some(event) = self.scheduler.pop_one_due(through)? else {
            return Ok(false);
        };
        let envelope = event.into_payload();
        self.routes[envelope.route.index()].push(envelope);
        Ok(true)
    }

    /// Advances idle wire time only when no wire event is due through the
    /// requested deadline.
    pub(crate) fn advance_idle_to(&mut self, deadline: SimTime) -> Result<(), WireError> {
        self.scheduler.advance_idle_to(deadline).map_err(Into::into)
    }

    /// Receives one ready physical message with checked, transactional live
    /// ownership accounting.
    pub(crate) fn try_recv_next(
        &mut self,
        route: WireRoute,
    ) -> Result<Option<EncodedDelivery>, WireError> {
        let Some(envelope) = self.routes[route.index()].peek() else {
            return Ok(None);
        };
        let len = envelope.bytes.len();
        let live_messages = self
            .live_messages
            .checked_sub(1)
            .ok_or(WireError::LiveOwnershipUnderflow)?;
        let live_bytes = self
            .live_bytes
            .checked_sub(len)
            .ok_or(WireError::LiveOwnershipUnderflow)?;
        let len_u64 = u64::try_from(len).map_err(|_| WireError::CounterOverflow)?;
        let delivered_messages = self
            .counters
            .delivered_messages
            .checked_add(1)
            .ok_or(WireError::CounterOverflow)?;
        let delivered_bytes = self
            .counters
            .delivered_bytes
            .checked_add(len_u64)
            .ok_or(WireError::CounterOverflow)?;

        let envelope = self.routes[route.index()]
            .pop()
            .ok_or(WireError::LiveOwnershipUnderflow)?;
        self.live_messages = live_messages;
        self.live_bytes = live_bytes;
        self.counters.delivered_messages = delivered_messages;
        self.counters.delivered_bytes = delivered_bytes;
        Ok(Some(EncodedDelivery {
            route: envelope.route,
            lane: envelope.lane,
            bytes: envelope.bytes,
            send_ordinal: envelope.send_ordinal,
            copy_index: envelope.copy_index,
        }))
    }

    pub(crate) const fn counters(&self) -> WireCounters {
        self.counters
    }

    pub(crate) const fn live_messages(&self) -> usize {
        self.live_messages
    }

    pub(crate) const fn live_bytes(&self) -> usize {
        self.live_bytes
    }

    pub(crate) const fn trace_hash(&self) -> u64 {
        self.scheduler.trace_hash()
    }

    fn reject<T>(&mut self, error: WireError) -> Result<T, WireError> {
        self.counters.capacity_rejections += 1;
        Err(error)
    }

    fn update_high_water(&mut self) {
        self.counters.queue_high_water_messages = self
            .counters
            .queue_high_water_messages
            .max(self.live_messages);
        self.counters.queue_high_water_bytes =
            self.counters.queue_high_water_bytes.max(self.live_bytes);
    }
}

fn wire_trace_tag(route: WireRoute, lane: WireLane, ordinal: u64, copy_index: u8) -> u64 {
    let route_tag = route.index() as u64;
    let lane_tag = match lane {
        WireLane::Control => 0,
        WireLane::Data => 1,
    };
    ordinal.rotate_left(11) ^ (route_tag << 5) ^ (lane_tag << 4) ^ u64::from(copy_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn release_due_events_through(wire: &mut TwoLegWire, through: SimTime) {
        while wire.advance_one_due(through).unwrap() {}
        wire.advance_idle_to(through).unwrap();
    }

    #[test]
    fn simulated_time_is_checked_and_never_wraps() {
        let start = SimTime::from_nanos(7);
        assert_eq!(
            start.checked_add(Duration::from_nanos(5)).unwrap(),
            SimTime::from_nanos(12)
        );
        assert_eq!(
            SimTime::from_nanos(u64::MAX).checked_add(Duration::from_nanos(1)),
            Err(SimTimeError::Overflow)
        );
        assert_eq!(
            start.checked_duration_since(SimTime::from_nanos(8)),
            Err(SimTimeError::TimeWentBackwards)
        );
    }

    #[test]
    fn scheduler_is_replayable_and_round_robins_same_phase_actors() {
        fn run() -> (Vec<ActorId>, u64, SimTime) {
            let budget = EventBudget::new(8, 64).unwrap();
            let mut scheduler = DeterministicScheduler::new(budget);
            let at = SimTime::from_nanos(10);
            scheduler
                .schedule_batch(vec![
                    EventSpec::new(
                        at,
                        EventPhase::TransportReceive,
                        ActorId::new(1),
                        3,
                        11,
                        "actor-1-first",
                    ),
                    EventSpec::new(
                        at,
                        EventPhase::TransportReceive,
                        ActorId::new(2),
                        3,
                        12,
                        "actor-2",
                    ),
                ])
                .unwrap();

            let first = scheduler.pop_next_checked().unwrap().unwrap();
            assert_eq!(first.key().actor_id(), ActorId::new(1));
            assert_eq!(scheduler.now(), at);

            scheduler
                .schedule(EventSpec::new(
                    at,
                    EventPhase::TransportReceive,
                    ActorId::new(1),
                    3,
                    13,
                    "actor-1-reentered",
                ))
                .unwrap();

            let mut actors = vec![first.key().actor_id()];
            while let Some(event) = scheduler.pop_next_checked().unwrap() {
                actors.push(event.key().actor_id());
            }
            (actors, scheduler.trace_hash(), scheduler.now())
        }

        let first = run();
        let second = run();
        assert_eq!(first, second);
        assert_eq!(
            first.0,
            vec![ActorId::new(1), ActorId::new(2), ActorId::new(1)]
        );
        assert_eq!(first.2, SimTime::from_nanos(10));
        assert_eq!(first.1, 0xdc51_2836_7cc1_cca1);
    }

    #[test]
    fn scheduler_defers_new_same_phase_actor_until_ready_micro_round_finishes() {
        let at = SimTime::from_nanos(10);
        let mut scheduler = DeterministicScheduler::new(EventBudget::new(8, 64).unwrap());
        scheduler
            .schedule_batch(vec![
                EventSpec::new(
                    at,
                    EventPhase::TransportReceive,
                    ActorId::new(1),
                    1,
                    1,
                    "actor-1-ready",
                ),
                EventSpec::new(
                    at,
                    EventPhase::TransportReceive,
                    ActorId::new(3),
                    1,
                    3,
                    "actor-3-ready",
                ),
            ])
            .unwrap();

        assert_eq!(
            scheduler
                .pop_next_checked()
                .unwrap()
                .unwrap()
                .into_payload(),
            "actor-1-ready"
        );
        scheduler
            .schedule(EventSpec::new(
                at,
                EventPhase::TransportReceive,
                ActorId::new(2),
                1,
                2,
                "actor-2-deferred",
            ))
            .unwrap();

        assert_eq!(
            scheduler
                .pop_next_checked()
                .unwrap()
                .unwrap()
                .into_payload(),
            "actor-3-ready"
        );
        assert_eq!(
            scheduler
                .pop_next_checked()
                .unwrap()
                .unwrap()
                .into_payload(),
            "actor-2-deferred"
        );
    }

    #[test]
    fn scheduler_defers_new_lower_phase_until_ready_micro_round_finishes() {
        let at = SimTime::from_nanos(10);
        let mut scheduler = DeterministicScheduler::new(EventBudget::new(8, 64).unwrap());
        scheduler
            .schedule_batch(vec![
                EventSpec::new(
                    at,
                    EventPhase::SupervisorCommand,
                    ActorId::new(1),
                    1,
                    1,
                    "supervisor-ready",
                ),
                EventSpec::new(
                    at,
                    EventPhase::JoinCleanup,
                    ActorId::new(3),
                    1,
                    3,
                    "join-ready",
                ),
            ])
            .unwrap();

        assert_eq!(
            scheduler
                .pop_next_checked()
                .unwrap()
                .unwrap()
                .into_payload(),
            "supervisor-ready"
        );
        scheduler
            .schedule(EventSpec::new(
                at,
                EventPhase::TransportReceive,
                ActorId::new(2),
                1,
                2,
                "transport-deferred",
            ))
            .unwrap();

        assert_eq!(
            scheduler
                .pop_next_checked()
                .unwrap()
                .unwrap()
                .into_payload(),
            "join-ready"
        );
        assert_eq!(
            scheduler
                .pop_next_checked()
                .unwrap()
                .unwrap()
                .into_payload(),
            "transport-deferred"
        );
    }

    #[test]
    fn scheduler_pop_underflow_is_typed_atomic_and_preserves_the_event() {
        let at = SimTime::from_nanos(10);
        let mut scheduler = DeterministicScheduler::new(EventBudget::new(2, 8).unwrap());
        scheduler
            .schedule(EventSpec::new(
                at,
                EventPhase::TransportReceive,
                ActorId::new(1),
                2,
                7,
                "owned",
            ))
            .unwrap();
        scheduler.pending_bytes = 1;
        let trace = scheduler.trace_hash();

        assert_eq!(
            scheduler.pop_next_checked(),
            Err(ScheduleError::ByteCountUnderflow)
        );
        assert_eq!(scheduler.pending_events(), 1);
        assert_eq!(scheduler.pending_bytes(), 1);
        assert_eq!(scheduler.now(), SimTime::ZERO);
        assert_eq!(scheduler.trace_hash(), trace);
        assert_eq!(scheduler.active_round, None);

        scheduler.pending_bytes = 2;
        assert_eq!(
            scheduler
                .pop_next_checked()
                .unwrap()
                .unwrap()
                .into_payload(),
            "owned"
        );
        assert_eq!(scheduler.pending_events(), 0);
        assert_eq!(scheduler.pending_bytes(), 0);
    }

    #[test]
    fn scheduler_due_api_releases_exactly_one_global_event_per_call() {
        let at = SimTime::from_nanos(10);
        let mut scheduler = DeterministicScheduler::new(EventBudget::new(3, 8).unwrap());
        scheduler
            .schedule_batch(vec![
                EventSpec::new(
                    at,
                    EventPhase::TransportReceive,
                    ActorId::new(1),
                    1,
                    1,
                    "first",
                ),
                EventSpec::new(
                    at,
                    EventPhase::TransportReceive,
                    ActorId::new(2),
                    1,
                    2,
                    "second",
                ),
            ])
            .unwrap();

        assert!(
            scheduler
                .pop_one_due(SimTime::from_nanos(9))
                .unwrap()
                .is_none()
        );
        assert_eq!(scheduler.pending_events(), 2);
        assert_eq!(
            scheduler.pop_one_due(at).unwrap().unwrap().into_payload(),
            "first"
        );
        assert_eq!(scheduler.pending_events(), 1);
        assert_eq!(
            scheduler.pop_one_due(at).unwrap().unwrap().into_payload(),
            "second"
        );
        assert!(scheduler.pop_one_due(at).unwrap().is_none());
    }

    #[test]
    fn wire_capacity_derives_application_frames_and_fault_copies() {
        for (rate, bytes, frames, encoded_once, delivered_bytes) in [
            (100_000_000, 6_250_000, 96, 6_253_552, 25_014_208),
            (170_000_000, 10_625_000, 163, 10_631_031, 42_524_124),
            (240_000_000, 15_000_000, 229, 15_008_473, 60_033_892),
        ] {
            let capacity = WireCapacitySpec::new(rate, Duration::from_millis(500), 65_536, 37, 1)
                .unwrap()
                .with_fault_copies(1, 1)
                .unwrap()
                .derive()
                .unwrap();

            assert_eq!(capacity.application_bytes(), bytes);
            assert_eq!(capacity.logical_messages(), frames);
            assert_eq!(capacity.encoded_once_bytes(), encoded_once);
            assert_eq!(capacity.max_send_messages(), frames * 2);
            assert_eq!(capacity.max_delivery_messages(), frames * 4);
            assert_eq!(capacity.max_delivery_bytes(), delivered_bytes);
        }

        assert_eq!(
            WireCapacitySpec::new(1, Duration::ZERO, 1, 0, 1),
            Err(CapacityError::ZeroHorizon)
        );
    }

    #[test]
    fn wire_rate_ceil_does_not_overflow_before_classifying_platform_capacity() {
        // `(2^64 - 1) * (2^64 + 1) == u128::MAX`. The quotient is valid in
        // u128, but cannot fit the protocol's u64 application-byte model.
        // Ceil division must therefore reach the typed platform-capacity
        // rejection instead of overflowing while adding `denominator - 1`.
        let horizon_nanos = u128::from(u64::MAX) + 2;
        let horizon = Duration::new(
            u64::try_from(horizon_nanos / 1_000_000_000).unwrap(),
            u32::try_from(horizon_nanos % 1_000_000_000).unwrap(),
        );
        let spec = WireCapacitySpec::new(u64::MAX, horizon, 1, 0, 1).unwrap();

        assert_eq!(spec.derive(), Err(CapacityError::PlatformCapacityOverflow));
    }

    #[test]
    fn declared_fixed_transport_work_is_admitted_after_the_data_budget() {
        let capacity = WireCapacitySpec::new(8, Duration::from_secs(1), 1, 0, 1)
            .unwrap()
            .with_fixed_work(1, 1)
            .derive()
            .unwrap();
        assert_eq!(capacity.logical_messages(), 1);
        assert_eq!(capacity.max_send_messages(), 2);
        assert_eq!(capacity.max_send_bytes(), 2);

        let bounds = WireBounds::new(capacity, 2, 2, 1, 1).unwrap();
        let mut wire = TwoLegWire::new(bounds);
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1],
            FaultAction::Pass,
        )
        .unwrap();
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Control,
            vec![2],
            FaultAction::Pass,
        )
        .unwrap();
        assert_eq!(
            wire.send(
                SimTime::ZERO,
                route,
                WireLane::Control,
                vec![3],
                FaultAction::Pass,
            ),
            Err(WireError::SendMessageBudgetExceeded)
        );
    }

    #[test]
    fn forced_tail_formula_always_has_nonempty_bounded_tail_fragments() {
        fn assert_feasible(application_bytes: u64, width: u64, requested_tail: u64) {
            let rate = application_bytes.checked_mul(8).unwrap();
            let capacity = WireCapacitySpec::new(
                rate,
                Duration::from_secs(1),
                usize::try_from(width).unwrap(),
                0,
                usize::try_from(requested_tail).unwrap(),
            )
            .unwrap()
            .derive()
            .unwrap();
            let tail = requested_tail.min(application_bytes);
            let full_width_messages = capacity.logical_messages().checked_sub(tail).unwrap();
            let tail_bytes = application_bytes
                .checked_sub(full_width_messages.checked_mul(width).unwrap())
                .unwrap();

            assert!(tail_bytes >= tail);
            assert!(tail_bytes <= tail.checked_mul(width).unwrap());
            assert_eq!(
                capacity.logical_messages(),
                (application_bytes - tail) / width + tail
            );
        }

        // B < F, B == F, the largest tail before another full-width frame,
        // and the first byte after that boundary.
        for bytes in [1, 4, 11, 12] {
            assert_feasible(bytes, 8, 4);
        }

        for (rate, frames, encoded_once) in [
            (100_000_000, 159, 6_255_883),
            (170_000_000, 226, 10_633_362),
            (240_000_000, 292, 15_010_804),
        ] {
            let capacity = WireCapacitySpec::new(rate, Duration::from_millis(500), 65_536, 37, 64)
                .unwrap()
                .derive()
                .unwrap();
            assert_eq!(capacity.logical_messages(), frames);
            assert_eq!(capacity.encoded_once_bytes(), encoded_once);
        }
    }

    fn micro_wire_capacity() -> WireCapacity {
        WireCapacitySpec::new(24, Duration::from_secs(1), 3, 0, 1)
            .unwrap()
            .with_fault_copies(1, 1)
            .unwrap()
            .derive()
            .unwrap()
    }

    fn harness_work_budget(
        positive_pieces: usize,
        zero_would_block: usize,
        fixed: HarnessFixedWork,
    ) -> HarnessWorkBudget {
        HarnessWorkSpec::new(
            micro_wire_capacity(),
            1,
            positive_pieces,
            zero_would_block,
            positive_pieces.checked_add(1).unwrap(),
            fixed,
            5,
        )
        .unwrap()
        .derive()
        .unwrap()
    }

    #[test]
    fn harness_work_budget_separates_wire_p_z_ack_and_fixed_actor_turns() {
        let base = harness_work_budget(1, 0, HarnessFixedWork::default());
        let categories = base.categories();
        assert_eq!(categories.wire_send_turns(), 2);
        assert_eq!(categories.wire_delivery_turns(), 4);
        assert_eq!(categories.positive_accept_turns(), 1);
        assert_eq!(categories.zero_would_block_turns(), 0);
        assert_eq!(categories.ack_construct_turns(), 2);
        assert_eq!(categories.ack_decode_turns(), 4);
        assert_eq!(categories.fixed_actor_turns(), 0);
        assert_eq!(base.max_turns(), 13);
        assert_eq!(base.max_owned_bytes(), 23);
        assert_eq!(base.event_budget().max_events(), 13);

        // One extra positive piece adds one sink/Target completion, one ACK
        // construction, and two fault-delivered ACK decodes for S=1.
        assert_eq!(
            harness_work_budget(2, 0, HarnessFixedWork::default()).max_turns(),
            base.max_turns() + 4
        );
        assert_eq!(
            harness_work_budget(1, 1, HarnessFixedWork::default()).max_turns(),
            base.max_turns() + 1
        );
        assert_eq!(
            harness_work_budget(1, 0, HarnessFixedWork::new(1, 0, 0, 0, 0, 0)).max_turns(),
            base.max_turns() + 1
        );
    }

    #[test]
    fn harness_work_budget_rejects_category_borrowing_before_total_turns_are_exhausted() {
        let tight = harness_work_budget(1, 0, HarnessFixedWork::default());
        let loose = harness_work_budget(1, 1, HarnessFixedWork::new(1, 0, 0, 0, 0, 0));
        let mut observed = HarnessObservedWork::default();

        observed
            .charge(loose, HarnessWorkCategory::WireSend, 1)
            .unwrap();
        assert!(observed.total().unwrap() < tight.max_turns());
        assert_eq!(
            observed.charge(tight, HarnessWorkCategory::ZeroWouldBlock, 1),
            Err(CapacityError::HarnessWorkCategoryExceeded {
                category: HarnessWorkCategory::ZeroWouldBlock,
            })
        );
        assert_eq!(observed.zero_would_block_turns(), 0);

        observed
            .charge(loose, HarnessWorkCategory::FixedActor, 1)
            .unwrap();
        assert!(observed.total().unwrap() < tight.max_turns());
        assert_eq!(
            tight.validate_observed(observed),
            Err(CapacityError::HarnessWorkCategoryExceeded {
                category: HarnessWorkCategory::FixedActor,
            })
        );
    }

    #[test]
    fn harness_work_budget_rejects_owned_byte_borrowing_for_all_three_categories() {
        let budget = harness_work_budget(1, 0, HarnessFixedWork::default());
        for (category, category_limit) in [
            (
                HarnessOwnedByteCategory::WireSend,
                budget.max_wire_send_owned_bytes(),
            ),
            (
                HarnessOwnedByteCategory::WireDelivery,
                budget.max_wire_delivery_owned_bytes(),
            ),
            (
                HarnessOwnedByteCategory::NonWire,
                budget.max_non_wire_owned_bytes(),
            ),
        ] {
            let mut observed = HarnessObservedWork::default();
            let borrowed = category_limit.checked_add(1).unwrap();
            assert!(borrowed < budget.max_owned_bytes());
            assert_eq!(
                observed.charge_owned_bytes(budget, category, borrowed),
                Err(CapacityError::HarnessOwnedByteCategoryExceeded { category })
            );
            assert_eq!(observed.total_owned_bytes(), Ok(0));
        }
    }

    #[test]
    fn one_byte_partial_acceptance_boundary_fails_when_p_is_one_short() {
        let exact = harness_work_budget(3, 0, HarnessFixedWork::default());
        let mut observed = HarnessObservedWork::default();
        assert_eq!(
            observed.charge(exact, HarnessWorkCategory::PositiveAccept, 3),
            Ok(())
        );

        let one_short = harness_work_budget(2, 0, HarnessFixedWork::default());
        let mut observed = HarnessObservedWork::default();
        assert_eq!(
            observed.charge(one_short, HarnessWorkCategory::PositiveAccept, 3),
            Err(CapacityError::HarnessWorkCategoryExceeded {
                category: HarnessWorkCategory::PositiveAccept,
            })
        );
        assert_eq!(observed.total(), Ok(0));
    }

    #[test]
    fn harness_work_inputs_and_arithmetic_fail_closed() {
        let wire = micro_wire_capacity();
        assert_eq!(
            HarnessWorkSpec::new(wire, 0, 1, 0, 2, HarnessFixedWork::default(), 0),
            Err(CapacityError::ZeroDirectionCount)
        );
        assert_eq!(
            HarnessWorkSpec::new(wire, 1, 0, 0, 1, HarnessFixedWork::default(), 0),
            Err(CapacityError::ZeroPositiveAcceptancePieces)
        );
        let overflowing_fixed =
            HarnessFixedWork::new(usize::MAX, 0, 0, 0, 0, 0).with_adapter_turns(1);
        assert_eq!(
            HarnessWorkSpec::new(wire, 1, 1, 0, 2, overflowing_fixed, 0)
                .unwrap()
                .derive(),
            Err(CapacityError::ArithmeticOverflow)
        );
    }

    fn test_wire_bounds(live_messages: usize, live_bytes: usize) -> WireBounds {
        let capacity = WireCapacitySpec::new(80_000, Duration::from_secs(1), 100, 0, 1)
            .unwrap()
            .with_fault_copies(1, 1)
            .unwrap()
            .derive()
            .unwrap();
        WireBounds::new(capacity, live_messages, live_bytes, 4, 4).unwrap()
    }

    #[test]
    fn blackout_is_half_open_and_scoped_to_one_leg_direction() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let unaffected = WireRoute::new(LegId::B, WireDirection::ClientToOwner);
        let mut wire = TwoLegWire::new(test_wire_bounds(64, 16_000));
        wire.add_blackout(route, SimTime::from_nanos(10), SimTime::from_nanos(20))
            .unwrap();

        wire.send(
            SimTime::from_nanos(9),
            route,
            WireLane::Data,
            vec![9],
            FaultAction::Pass,
        )
        .unwrap();
        wire.send(
            SimTime::from_nanos(10),
            route,
            WireLane::Data,
            vec![10],
            FaultAction::Pass,
        )
        .unwrap();
        wire.send(
            SimTime::from_nanos(19),
            route,
            WireLane::Data,
            vec![19],
            FaultAction::Duplicate,
        )
        .unwrap();
        wire.send(
            SimTime::from_nanos(15),
            unaffected,
            WireLane::Data,
            vec![15],
            FaultAction::Pass,
        )
        .unwrap();
        wire.send(
            SimTime::from_nanos(20),
            route,
            WireLane::Data,
            vec![20],
            FaultAction::Pass,
        )
        .unwrap();

        release_due_events_through(&mut wire, SimTime::from_nanos(20));
        let mut route_bytes = Vec::new();
        while let Some(delivery) = wire.try_recv_next(route).unwrap() {
            route_bytes.push(delivery.into_bytes());
        }
        assert_eq!(route_bytes, vec![vec![9], vec![20]]);
        assert_eq!(
            wire.try_recv_next(unaffected)
                .unwrap()
                .unwrap()
                .into_bytes(),
            vec![15]
        );
        assert!(wire.try_recv_next(unaffected).unwrap().is_none());

        let counters = wire.counters();
        assert_eq!(counters.blackout_drops, 2);
        assert_eq!(counters.dropped_messages, 2);
        assert_eq!(counters.duplicate_copies, 0);
        assert_eq!(counters.delivered_messages, 3);
    }

    #[test]
    fn live_capacity_rejects_atomically_and_recovers_after_receive() {
        let route = WireRoute::new(LegId::A, WireDirection::OwnerToClient);
        let now = SimTime::from_nanos(1);
        let mut wire = TwoLegWire::new(test_wire_bounds(1, 4));

        wire.send(
            now,
            route,
            WireLane::Control,
            vec![1, 2, 3, 4],
            FaultAction::Pass,
        )
        .unwrap();
        assert_eq!(
            wire.send(now, route, WireLane::Data, vec![5], FaultAction::Pass,),
            Err(WireError::LiveMessageCapacityExceeded)
        );
        assert_eq!(wire.live_messages(), 1);
        assert_eq!(wire.live_bytes(), 4);

        release_due_events_through(&mut wire, now);
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);

        let outcome = wire
            .send(now, route, WireLane::Data, vec![5], FaultAction::Pass)
            .unwrap();
        assert_eq!(outcome.send_ordinal(), 1);
        release_due_events_through(&mut wire, now);
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![5]
        );
        assert_eq!(wire.counters().capacity_rejections, 1);
    }

    #[test]
    fn drop_duplicate_and_adjacent_reorder_have_exact_physical_counters() {
        let route = WireRoute::new(LegId::B, WireDirection::OwnerToClient);
        let now = SimTime::from_nanos(7);
        let mut wire = TwoLegWire::new(test_wire_bounds(16, 128));

        wire.send(now, route, WireLane::Data, vec![1], FaultAction::Drop)
            .unwrap();
        let duplicate = wire
            .send(now, route, WireLane::Data, vec![2], FaultAction::Duplicate)
            .unwrap();
        assert_eq!(duplicate.physical_copies(), 2);
        wire.send(
            now,
            route,
            WireLane::Data,
            vec![3],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        wire.send(now, route, WireLane::Data, vec![4], FaultAction::Pass)
            .unwrap();

        release_due_events_through(&mut wire, now);
        let mut delivered = Vec::new();
        while let Some(message) = wire.try_recv_next(route).unwrap() {
            let copy_index = message.copy_index();
            delivered.push((message.into_bytes(), copy_index));
        }
        assert_eq!(
            delivered,
            vec![(vec![2], 0), (vec![2], 1), (vec![4], 0), (vec![3], 0)]
        );

        let counters = wire.counters();
        assert_eq!(counters.submitted_messages, 4);
        assert_eq!(counters.physical_messages, 4);
        assert_eq!(counters.dropped_messages, 1);
        assert_eq!(counters.duplicate_copies, 1);
        assert_eq!(counters.reordered_pairs, 1);
        assert_eq!(counters.delivered_messages, 4);
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);
    }

    #[test]
    fn delay_and_hold_release_only_owned_bytes_at_declared_virtual_time() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let mut wire = TwoLegWire::new(test_wire_bounds(8, 64));
        wire.send(
            SimTime::from_nanos(5),
            route,
            WireLane::Control,
            vec![1],
            FaultAction::Delay {
                release_at: SimTime::from_nanos(20),
            },
        )
        .unwrap();
        wire.send(
            SimTime::from_nanos(5),
            route,
            WireLane::Control,
            vec![2],
            FaultAction::Hold { token: 7 },
        )
        .unwrap();

        release_due_events_through(&mut wire, SimTime::from_nanos(19));
        assert!(wire.try_recv_next(route).unwrap().is_none());
        assert_eq!(wire.live_messages(), 2);
        wire.release_hold(7, SimTime::from_nanos(20)).unwrap();
        release_due_events_through(&mut wire, SimTime::from_nanos(20));
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1]
        );
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![2]
        );
        assert!(wire.try_recv_next(route).unwrap().is_none());

        let counters = wire.counters();
        assert_eq!(counters.delayed_messages, 1);
        assert_eq!(counters.held_messages, 1);
        assert_eq!(counters.delivered_messages, 2);
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);
    }

    #[test]
    fn adjacent_reorder_releases_the_first_message_when_the_second_is_dropped() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let mut wire = TwoLegWire::new(test_wire_bounds(8, 64));
        wire.add_blackout(route, SimTime::from_nanos(1), SimTime::from_nanos(2))
            .unwrap();
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        wire.send(
            SimTime::from_nanos(1),
            route,
            WireLane::Data,
            vec![2],
            FaultAction::Pass,
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::from_nanos(1));

        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1]
        );
        assert!(wire.try_recv_next(route).unwrap().is_none());
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);
        assert_eq!(wire.counters().blackout_drops, 1);
    }

    #[test]
    fn explicit_adjacent_drop_also_releases_the_reorder_slot() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let mut wire = TwoLegWire::new(test_wire_bounds(8, 64));
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        wire.send(
            SimTime::from_nanos(1),
            route,
            WireLane::Data,
            vec![2],
            FaultAction::Drop,
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::from_nanos(1));

        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1]
        );
        assert!(wire.try_recv_next(route).unwrap().is_none());
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);
        assert_eq!(wire.counters().dropped_messages, 1);
        assert_eq!(wire.counters().blackout_drops, 0);
    }

    #[test]
    fn adjacent_duplicate_is_atomic_and_releases_the_reorder_slot() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let now = SimTime::from_nanos(1);
        let mut wire = TwoLegWire::new(test_wire_bounds(8, 64));
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        wire.send(now, route, WireLane::Data, vec![2], FaultAction::Duplicate)
            .unwrap();
        release_due_events_through(&mut wire, now);

        let mut delivered = Vec::new();
        while let Some(message) = wire.try_recv_next(route).unwrap() {
            let copy_index = message.copy_index();
            delivered.push((message.into_bytes(), copy_index));
        }
        assert_eq!(delivered, vec![(vec![2], 0), (vec![2], 1), (vec![1], 0)]);
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);
        assert_eq!(wire.counters().duplicate_copies, 1);
        assert_eq!(wire.counters().reordered_pairs, 1);
    }

    #[test]
    fn nested_adjacent_reorder_is_rejected_without_consuming_either_message() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let now = SimTime::from_nanos(1);
        let mut wire = TwoLegWire::new(test_wire_bounds(8, 64));
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        let before = wire.counters();

        assert_eq!(
            wire.send(
                now,
                route,
                WireLane::Data,
                vec![2],
                FaultAction::ReorderAdjacent,
            ),
            Err(WireError::UnsupportedFaultAction)
        );
        assert_eq!(wire.counters(), before);
        assert_eq!(wire.live_messages(), 1);
        assert_eq!(wire.live_bytes(), 1);

        let outcome = wire
            .send(now, route, WireLane::Data, vec![3], FaultAction::Pass)
            .unwrap();
        assert_eq!(outcome.send_ordinal(), 1);
        release_due_events_through(&mut wire, now);
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![3]
        );
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1]
        );
        assert!(wire.try_recv_next(route).unwrap().is_none());
    }

    #[test]
    fn adjacent_duplicate_capacity_failure_preserves_the_first_slot_atomically() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let now = SimTime::from_nanos(1);
        let mut wire = TwoLegWire::new(test_wire_bounds(2, 64));
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        let before = wire.counters();

        assert_eq!(
            wire.send(now, route, WireLane::Data, vec![2], FaultAction::Duplicate),
            Err(WireError::LiveMessageCapacityExceeded)
        );
        let after = wire.counters();
        assert_eq!(after.capacity_rejections, before.capacity_rejections + 1);
        assert_eq!(after.submitted_messages, before.submitted_messages);
        assert_eq!(after.physical_messages, before.physical_messages);
        assert_eq!(wire.live_messages(), 1);

        let outcome = wire
            .send(now, route, WireLane::Data, vec![3], FaultAction::Pass)
            .unwrap();
        assert_eq!(outcome.send_ordinal(), 1);
        release_due_events_through(&mut wire, now);
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![3]
        );
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1]
        );
        assert!(wire.try_recv_next(route).unwrap().is_none());
    }

    #[test]
    fn adjacent_pair_scheduler_budget_failure_preserves_the_first_slot_atomically() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let now = SimTime::from_nanos(1);
        let mut wire = TwoLegWire::new(test_wire_bounds(4, 64));
        wire.scheduler.budget.max_events = 1;
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        let before = wire.counters();

        assert_eq!(
            wire.send(now, route, WireLane::Data, vec![2], FaultAction::Pass),
            Err(WireError::Schedule(ScheduleError::EventBudgetExceeded))
        );
        assert_eq!(wire.counters(), before);
        assert_eq!(wire.live_messages(), 1);
        assert_eq!(wire.scheduler.pending_events(), 0);

        wire.scheduler.budget.max_events = 2;
        let outcome = wire
            .send(now, route, WireLane::Data, vec![3], FaultAction::Pass)
            .unwrap();
        assert_eq!(outcome.send_ordinal(), 1);
        release_due_events_through(&mut wire, now);
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![3]
        );
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1]
        );
    }

    #[test]
    fn adjacent_invalid_delay_and_duplicate_hold_token_preserve_the_first_slot() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let other_lane = WireLane::Control;
        let data_lane = WireLane::Data;
        let now = SimTime::from_nanos(5);
        let mut wire = TwoLegWire::new(test_wire_bounds(8, 64));
        wire.send(
            SimTime::ZERO,
            route,
            other_lane,
            vec![9],
            FaultAction::Hold { token: 7 },
        )
        .unwrap();
        wire.send(
            SimTime::ZERO,
            route,
            data_lane,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        let before = wire.counters();

        assert_eq!(
            wire.send(
                now,
                route,
                data_lane,
                vec![2],
                FaultAction::Delay {
                    release_at: SimTime::from_nanos(4),
                },
            ),
            Err(WireError::Schedule(ScheduleError::TimeWentBackwards))
        );
        assert_eq!(wire.counters(), before);
        assert_eq!(wire.live_messages(), 2);
        assert_eq!(
            wire.send(
                now,
                route,
                data_lane,
                vec![2],
                FaultAction::Hold { token: 7 },
            ),
            Err(WireError::DuplicateHoldToken)
        );
        assert_eq!(wire.counters(), before);
        assert_eq!(wire.live_messages(), 2);

        let outcome = wire
            .send(now, route, data_lane, vec![3], FaultAction::Pass)
            .unwrap();
        assert_eq!(outcome.send_ordinal(), 2);
        release_due_events_through(&mut wire, now);
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![3]
        );
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1]
        );
        wire.release_hold(7, now).unwrap();
        release_due_events_through(&mut wire, now);
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![9]
        );
    }

    #[test]
    fn adjacent_delay_releases_the_exact_pair_at_the_declared_time() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let mut wire = TwoLegWire::new(test_wire_bounds(8, 64));
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        wire.send(
            SimTime::from_nanos(1),
            route,
            WireLane::Data,
            vec![2],
            FaultAction::Delay {
                release_at: SimTime::from_nanos(10),
            },
        )
        .unwrap();

        release_due_events_through(&mut wire, SimTime::from_nanos(9));
        assert!(wire.try_recv_next(route).unwrap().is_none());
        assert_eq!(wire.live_messages(), 2);
        release_due_events_through(&mut wire, SimTime::from_nanos(10));
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![2]
        );
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1]
        );
        assert!(wire.try_recv_next(route).unwrap().is_none());
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);
        assert_eq!(wire.counters().delayed_messages, 1);
        assert_eq!(wire.counters().reordered_pairs, 1);
    }

    #[test]
    fn adjacent_hold_token_owns_and_releases_the_exact_reordered_pair() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let mut wire = TwoLegWire::new(test_wire_bounds(8, 64));
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        wire.send(
            SimTime::from_nanos(1),
            route,
            WireLane::Data,
            vec![2],
            FaultAction::Hold { token: 7 },
        )
        .unwrap();

        release_due_events_through(&mut wire, SimTime::from_nanos(9));
        assert!(wire.try_recv_next(route).unwrap().is_none());
        assert_eq!(wire.live_messages(), 2);
        assert_eq!(wire.live_bytes(), 2);
        wire.release_hold(7, SimTime::from_nanos(10)).unwrap();
        release_due_events_through(&mut wire, SimTime::from_nanos(10));
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![2]
        );
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1]
        );
        assert!(wire.try_recv_next(route).unwrap().is_none());
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);
        assert_eq!(wire.counters().held_messages, 1);
        assert_eq!(wire.counters().reordered_pairs, 1);
    }

    #[test]
    fn cancelling_a_leg_releases_every_queue_kind_and_preserves_the_other_leg() {
        let a_up = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let a_down = WireRoute::new(LegId::A, WireDirection::OwnerToClient);
        let b_up = WireRoute::new(LegId::B, WireDirection::ClientToOwner);
        let mut wire = TwoLegWire::new(test_wire_bounds(16, 128));

        wire.send(
            SimTime::ZERO,
            a_up,
            WireLane::Data,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        wire.send(
            SimTime::ZERO,
            a_up,
            WireLane::Control,
            vec![2],
            FaultAction::Hold { token: 1 },
        )
        .unwrap();
        wire.send(
            SimTime::ZERO,
            a_down,
            WireLane::Data,
            vec![3],
            FaultAction::Delay {
                release_at: SimTime::from_nanos(10),
            },
        )
        .unwrap();
        wire.send(
            SimTime::ZERO,
            a_down,
            WireLane::Control,
            vec![4],
            FaultAction::Pass,
        )
        .unwrap();
        wire.send(
            SimTime::ZERO,
            b_up,
            WireLane::Data,
            vec![5],
            FaultAction::Hold { token: 2 },
        )
        .unwrap();
        release_due_events_through(&mut wire, SimTime::ZERO);
        assert_eq!(wire.live_messages(), 5);

        assert_eq!(
            wire.cancel_leg(LegId::A).unwrap(),
            WireCancelSummary {
                messages: 4,
                bytes: 4,
            }
        );
        assert_eq!(wire.live_messages(), 1);
        assert_eq!(wire.live_bytes(), 1);
        assert!(wire.try_recv_next(a_up).unwrap().is_none());
        assert!(wire.try_recv_next(a_down).unwrap().is_none());
        assert_eq!(wire.counters().cancelled_messages, 4);
        assert_eq!(wire.counters().cancelled_bytes, 4);

        wire.release_hold(2, SimTime::from_nanos(1)).unwrap();
        release_due_events_through(&mut wire, SimTime::from_nanos(1));
        assert_eq!(
            wire.try_recv_next(b_up).unwrap().unwrap().into_bytes(),
            vec![5]
        );
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);
        assert_eq!(wire.cancel_all().unwrap(), WireCancelSummary::default());
    }

    #[test]
    fn wire_receive_underflow_is_typed_atomic_and_preserves_the_message() {
        let route = WireRoute::new(LegId::A, WireDirection::OwnerToClient);
        let mut wire = TwoLegWire::new(test_wire_bounds(4, 64));
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1, 2, 3],
            FaultAction::Pass,
        )
        .unwrap();
        assert!(wire.advance_one_due(SimTime::ZERO).unwrap());
        assert!(!wire.advance_one_due(SimTime::ZERO).unwrap());
        let counters = wire.counters();
        wire.live_bytes = 2;

        assert_eq!(
            wire.try_recv_next(route),
            Err(WireError::LiveOwnershipUnderflow)
        );
        assert_eq!(wire.live_messages(), 1);
        assert_eq!(wire.live_bytes(), 2);
        assert_eq!(wire.counters(), counters);
        assert_eq!(wire.routes[route.index()].iter_all().count(), 1);

        wire.live_bytes = 3;
        assert_eq!(
            wire.try_recv_next(route).unwrap().unwrap().into_bytes(),
            vec![1, 2, 3]
        );
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);
    }

    #[test]
    fn wire_cancel_underflow_is_typed_atomic_across_every_owner_kind() {
        let a_up = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let a_down = WireRoute::new(LegId::A, WireDirection::OwnerToClient);
        let mut wire = TwoLegWire::new(test_wire_bounds(8, 64));
        wire.send(
            SimTime::ZERO,
            a_up,
            WireLane::Data,
            vec![1],
            FaultAction::ReorderAdjacent,
        )
        .unwrap();
        wire.send(
            SimTime::ZERO,
            a_up,
            WireLane::Control,
            vec![2],
            FaultAction::Hold { token: 1 },
        )
        .unwrap();
        wire.send(
            SimTime::ZERO,
            a_down,
            WireLane::Data,
            vec![3],
            FaultAction::Delay {
                release_at: SimTime::from_nanos(10),
            },
        )
        .unwrap();
        wire.send(
            SimTime::ZERO,
            a_down,
            WireLane::Control,
            vec![4],
            FaultAction::Pass,
        )
        .unwrap();
        assert!(wire.advance_one_due(SimTime::ZERO).unwrap());
        let counters = wire.counters();
        wire.live_bytes = 3;

        assert_eq!(
            wire.cancel_leg(LegId::A),
            Err(WireError::LiveOwnershipUnderflow)
        );
        assert_eq!(wire.live_messages(), 4);
        assert_eq!(wire.live_bytes(), 3);
        assert_eq!(wire.counters(), counters);
        assert!(wire.reorder_slots.iter().any(Option::is_some));
        assert_eq!(wire.held_message_count().unwrap(), 1);
        assert_eq!(wire.scheduler.pending_events(), 1);
        assert_eq!(wire.routes[a_down.index()].iter_all().count(), 1);

        wire.live_bytes = 4;
        assert_eq!(
            wire.cancel_leg(LegId::A).unwrap(),
            WireCancelSummary {
                messages: 4,
                bytes: 4,
            }
        );
        assert_eq!(wire.live_messages(), 0);
        assert_eq!(wire.live_bytes(), 0);
    }

    #[test]
    fn scheduler_cancel_underflow_does_not_partially_cancel_wire_ownership() {
        let route = WireRoute::new(LegId::A, WireDirection::ClientToOwner);
        let mut wire = TwoLegWire::new(test_wire_bounds(4, 64));
        wire.send(
            SimTime::ZERO,
            route,
            WireLane::Data,
            vec![1, 2],
            FaultAction::Delay {
                release_at: SimTime::from_nanos(10),
            },
        )
        .unwrap();
        let counters = wire.counters();
        wire.scheduler.pending_bytes = 1;

        assert_eq!(
            wire.cancel_leg(LegId::A),
            Err(WireError::Schedule(ScheduleError::ByteCountUnderflow))
        );
        assert_eq!(wire.live_messages(), 1);
        assert_eq!(wire.live_bytes(), 2);
        assert_eq!(wire.scheduler.pending_events(), 1);
        assert_eq!(wire.scheduler.pending_bytes(), 1);
        assert_eq!(wire.counters(), counters);

        wire.scheduler.pending_bytes = 2;
        assert_eq!(
            wire.cancel_leg(LegId::A).unwrap(),
            WireCancelSummary {
                messages: 1,
                bytes: 2,
            }
        );
    }
}

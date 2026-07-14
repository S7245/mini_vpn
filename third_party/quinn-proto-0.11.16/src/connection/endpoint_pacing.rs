use std::{
    collections::{hash_map::Entry, VecDeque},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    task::Waker,
};

use rustc_hash::FxHashMap;

use crate::{ConnectionHandle, EndpointPacingServiceConfig, Instant};

const NANOS_PER_SECOND: u128 = 1_000_000_000;

#[derive(Clone, Debug)]
pub(crate) struct EndpointPacingService(Arc<EndpointPacingServiceInner>);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct EndpointPacingConnectionKey {
    connection_handle: ConnectionHandle,
    path_generation: u64,
}

impl EndpointPacingConnectionKey {
    pub(crate) const fn new(connection_handle: ConnectionHandle, path_generation: u64) -> Self {
        Self {
            connection_handle,
            path_generation,
        }
    }
}

#[derive(Debug)]
struct EndpointPacingServiceInner {
    config: EndpointPacingServiceConfig,
    state: Mutex<EndpointPacingState>,
}

#[derive(Debug)]
struct EndpointPacingState {
    available_tokens: u64,
    live_reservation_bytes: u64,
    outstanding_bytes: u64,
    stateless_outstanding_bytes: u64,
    stateless_responses_sent: u64,
    stateless_responses_dropped: u64,
    granted_bytes: u64,
    granted_datagrams: u64,
    refunded_bytes: u64,
    refund_events: u64,
    sent_bytes: u64,
    sent_datagrams: u64,
    abandoned_bytes: u64,
    abandoned_datagrams: u64,
    outstanding_bytes_high_water: u64,
    socket_would_block_events: u64,
    socket_would_block_outstanding_high_water: u64,
    endpoint_delay_events: u64,
    max_endpoint_delay_nanos: u64,
    waiter_high_water: usize,
    cancellations: u64,
    migrations: u64,
    detaches: u64,
    fairness_lead_high_water_bytes: u64,
    stale_waker_events: u64,
    refill_remainder: u128,
    last_refill: Option<Instant>,
    connections: FxHashMap<EndpointPacingConnectionKey, ConnectionAccounting>,
    control_waiters: VecDeque<EndpointPacingConnectionKey>,
    control_turn: Option<EndpointPacingConnectionKey>,
    control_yielding_to_bulk: bool,
    bulk_service_before_control_bytes: u64,
    bulk_waiters: VecDeque<EndpointPacingConnectionKey>,
    bulk_turn: Option<EndpointPacingConnectionKey>,
}

#[derive(Debug)]
struct ConnectionAccounting {
    attached: bool,
    live_reservation_bytes: u64,
    outstanding_bytes: u64,
    outstanding_datagrams: u64,
    control_deficit_bytes: u64,
    bulk_deficit_bytes: u64,
    waiter: Option<PacingWaiter>,
    service_counters: ConnectionServiceCounters,
}

#[derive(Clone, Copy, Debug, Default)]
struct ConnectionServiceCounters {
    granted_bulk_bytes: u64,
    granted_control_bytes: u64,
    turn_count: u64,
    wait_count: u64,
    max_service_gap_nanos: u64,
    last_grant_at: Option<Instant>,
}

#[derive(Debug)]
struct PacingWaiter {
    class: PacingTrafficClass,
    path_ready_at: Instant,
    planned_bytes: u64,
    waker: Option<Waker>,
}

impl ConnectionAccounting {
    fn attached() -> Self {
        Self {
            attached: true,
            live_reservation_bytes: 0,
            outstanding_bytes: 0,
            outstanding_datagrams: 0,
            control_deficit_bytes: 0,
            bulk_deficit_bytes: 0,
            waiter: None,
            service_counters: ConnectionServiceCounters::default(),
        }
    }
}

impl EndpointPacingState {
    fn refill(&mut self, config: EndpointPacingServiceConfig, now: Instant) {
        let Some(last_refill) = self.last_refill else {
            self.last_refill = Some(now);
            return;
        };
        let Some(elapsed) = now.checked_duration_since(last_refill) else {
            return;
        };
        if elapsed.is_zero() {
            return;
        }
        self.last_refill = Some(now);

        let occupied = self
            .live_reservation_bytes
            .saturating_add(self.outstanding_bytes);
        let capacity = config.burst_bytes.saturating_sub(occupied);
        let room = capacity.saturating_sub(self.available_tokens);
        if room == 0 {
            self.refill_remainder = 0;
            self.assert_conservation(config.burst_bytes);
            return;
        }

        let numerator = elapsed
            .as_nanos()
            .saturating_mul(u128::from(config.rate_bytes_per_second))
            .saturating_add(self.refill_remainder);
        let minted = (numerator / NANOS_PER_SECOND).min(u128::from(u64::MAX)) as u64;
        let credited = minted.min(room);
        self.available_tokens += credited;
        self.refill_remainder = if credited < room {
            numerator % NANOS_PER_SECOND
        } else {
            0
        };
        self.assert_conservation(config.burst_bytes);
    }

    fn assert_conservation(&self, burst_bytes: u64) {
        debug_assert!(
            u128::from(self.available_tokens)
                + u128::from(self.live_reservation_bytes)
                + u128::from(self.outstanding_bytes)
                <= u128::from(burst_bytes)
        );
    }

    fn record_endpoint_delay(&mut self, deadline: Option<Instant>, now: Instant) {
        self.endpoint_delay_events = self.endpoint_delay_events.saturating_add(1);
        let delay_nanos = deadline
            .and_then(|deadline| deadline.checked_duration_since(now))
            .map_or(0, |delay| delay.as_nanos().min(u128::from(u64::MAX)) as u64);
        self.max_endpoint_delay_nanos = self.max_endpoint_delay_nanos.max(delay_nanos);
    }

    fn update_waiter_high_water(&mut self) {
        self.waiter_high_water = self.waiter_high_water.max(
            self.control_waiters
                .len()
                .saturating_add(self.bulk_waiters.len()),
        );
    }

    fn record_bulk_fairness_lead(&mut self, granted_key: EndpointPacingConnectionKey) {
        let mut minimum = u64::MAX;
        let mut maximum = 0u64;
        let mut participants = 0usize;
        for (key, connection) in &self.connections {
            let continuously_backlogged = *key == granted_key
                || connection
                    .waiter
                    .as_ref()
                    .is_some_and(|waiter| waiter.class == PacingTrafficClass::Bulk);
            if !connection.attached || !continuously_backlogged {
                continue;
            }
            participants += 1;
            minimum = minimum.min(connection.service_counters.granted_bulk_bytes);
            maximum = maximum.max(connection.service_counters.granted_bulk_bytes);
        }
        if participants >= 2 {
            self.fairness_lead_high_water_bytes = self
                .fairness_lead_high_water_bytes
                .max(maximum.saturating_sub(minimum));
        }
    }

    fn token_deadline(
        &self,
        config: EndpointPacingServiceConfig,
        planned_bytes: u64,
        now: Instant,
    ) -> Option<Instant> {
        if planned_bytes <= self.available_tokens {
            return Some(now);
        }
        let occupied = self
            .live_reservation_bytes
            .saturating_add(self.outstanding_bytes);
        let capacity = config.burst_bytes.saturating_sub(occupied);
        if planned_bytes > capacity {
            return None;
        }

        let missing_bytes = planned_bytes - self.available_tokens;
        let required_numerator = u128::from(missing_bytes)
            .saturating_mul(NANOS_PER_SECOND)
            .saturating_sub(self.refill_remainder);
        let delay_nanos = required_numerator.div_ceil(u128::from(config.rate_bytes_per_second));
        let delay_nanos = u64::try_from(delay_nanos).ok()?;
        now.checked_add(crate::Duration::from_nanos(delay_nanos))
    }

    fn combined_deadline(
        &self,
        config: EndpointPacingServiceConfig,
        path_ready_at: Instant,
        planned_bytes: u64,
        now: Instant,
    ) -> Option<Instant> {
        match self.token_deadline(config, planned_bytes, now) {
            Some(endpoint_ready_at) => Some(path_ready_at.max(endpoint_ready_at)),
            None if path_ready_at > now => Some(path_ready_at),
            None => None,
        }
    }

    fn remove_waiter(&mut self, key: EndpointPacingConnectionKey) -> bool {
        let Some(connection) = self.connections.get_mut(&key) else {
            return false;
        };
        let removed = connection.waiter.take().is_some();
        connection.control_deficit_bytes = 0;
        connection.bulk_deficit_bytes = 0;
        self.control_waiters.retain(|queued| *queued != key);
        self.bulk_waiters.retain(|queued| *queued != key);
        if self.control_turn == Some(key) {
            self.control_turn = None;
        }
        if self.bulk_turn == Some(key) {
            self.bulk_turn = None;
        }
        if self.control_waiters.is_empty() {
            self.control_yielding_to_bulk = false;
            self.bulk_service_before_control_bytes = 0;
        }
        removed
    }

    fn register_bulk_waiter(
        &mut self,
        key: EndpointPacingConnectionKey,
        path_ready_at: Instant,
        planned_bytes: u64,
        waker: Option<&Waker>,
    ) {
        if self.connections.get(&key).is_some_and(|connection| {
            connection
                .waiter
                .as_ref()
                .is_some_and(|waiter| waiter.class != PacingTrafficClass::Bulk)
        }) {
            self.remove_waiter(key);
        }
        let connection = self
            .connections
            .get_mut(&key)
            .expect("attached connection was checked before waiter registration");
        if let Some(waiter) = connection.waiter.as_mut() {
            waiter.path_ready_at = path_ready_at;
            waiter.planned_bytes = planned_bytes;
            let replaced = replace_waker(&mut waiter.waker, waker);
            if replaced {
                self.stale_waker_events = self.stale_waker_events.saturating_add(1);
            }
            return;
        }
        connection.waiter = Some(PacingWaiter {
            class: PacingTrafficClass::Bulk,
            path_ready_at,
            planned_bytes,
            waker: waker.cloned(),
        });
        connection.service_counters.wait_count =
            connection.service_counters.wait_count.saturating_add(1);
        self.bulk_waiters.push_back(key);
        self.update_waiter_high_water();
    }

    fn register_control_waiter(
        &mut self,
        key: EndpointPacingConnectionKey,
        path_ready_at: Instant,
        planned_bytes: u64,
        waker: Option<&Waker>,
    ) {
        if self.connections.get(&key).is_some_and(|connection| {
            connection
                .waiter
                .as_ref()
                .is_some_and(|waiter| waiter.class != PacingTrafficClass::Control)
        }) {
            self.remove_waiter(key);
        }
        let connection = self
            .connections
            .get_mut(&key)
            .expect("attached connection was checked before waiter registration");
        if let Some(waiter) = connection.waiter.as_mut() {
            waiter.path_ready_at = path_ready_at;
            waiter.planned_bytes = planned_bytes;
            let replaced = replace_waker(&mut waiter.waker, waker);
            if replaced {
                self.stale_waker_events = self.stale_waker_events.saturating_add(1);
            }
            return;
        }
        connection.waiter = Some(PacingWaiter {
            class: PacingTrafficClass::Control,
            path_ready_at,
            planned_bytes,
            waker: waker.cloned(),
        });
        connection.service_counters.wait_count =
            connection.service_counters.wait_count.saturating_add(1);
        self.control_waiters.push_back(key);
        self.update_waiter_high_water();
    }

    fn clean_bulk_waiters(&mut self) {
        self.bulk_waiters.retain(|key| {
            self.connections.get(key).is_some_and(|connection| {
                connection.attached
                    && connection
                        .waiter
                        .as_ref()
                        .is_some_and(|waiter| waiter.class == PacingTrafficClass::Bulk)
            })
        });
        if let Some(owner) = self.bulk_turn {
            let owner_waiting = self
                .connections
                .get(&owner)
                .is_some_and(|connection| connection.waiter.is_some());
            if !owner_waiting {
                if let Some(connection) = self.connections.get_mut(&owner) {
                    connection.bulk_deficit_bytes = 0;
                }
                self.bulk_turn = None;
            }
        }
    }

    fn clean_control_waiters(&mut self) {
        self.control_waiters.retain(|key| {
            self.connections.get(key).is_some_and(|connection| {
                connection.attached
                    && connection
                        .waiter
                        .as_ref()
                        .is_some_and(|waiter| waiter.class == PacingTrafficClass::Control)
            })
        });
        if let Some(owner) = self.control_turn {
            let owner_waiting = self
                .connections
                .get(&owner)
                .is_some_and(|connection| connection.waiter.is_some());
            if !owner_waiting {
                if let Some(connection) = self.connections.get_mut(&owner) {
                    connection.control_deficit_bytes = 0;
                }
                self.control_turn = None;
            }
        }
        if self.control_waiters.is_empty() {
            self.control_yielding_to_bulk = false;
            self.bulk_service_before_control_bytes = 0;
        }
    }

    fn has_ready_waiter(&self, class: PacingTrafficClass, now: Instant) -> bool {
        let waiters = match class {
            PacingTrafficClass::Control => &self.control_waiters,
            PacingTrafficClass::Bulk => &self.bulk_waiters,
        };
        waiters.iter().any(|key| {
            self.connections.get(key).is_some_and(|connection| {
                connection.attached
                    && connection
                        .waiter
                        .as_ref()
                        .is_some_and(|waiter| waiter.class == class && waiter.path_ready_at <= now)
            })
        })
    }

    fn assign_control_turn(&mut self, key: EndpointPacingConnectionKey, reserve_bytes: u64) {
        self.control_turn = Some(key);
        if let Some(connection) = self.connections.get_mut(&key) {
            connection.service_counters.turn_count =
                connection.service_counters.turn_count.saturating_add(1);
            connection.control_deficit_bytes = connection
                .control_deficit_bytes
                .saturating_add(reserve_bytes);
        }
    }

    fn ensure_ready_control_turn(
        &mut self,
        now: Instant,
        reserve_bytes: u64,
    ) -> Option<EndpointPacingConnectionKey> {
        self.clean_control_waiters();

        if let Some(owner) = self.control_turn {
            let owner_ready = self.connections.get(&owner).is_some_and(|connection| {
                connection
                    .waiter
                    .as_ref()
                    .is_some_and(|waiter| waiter.path_ready_at <= now)
            });
            let ready_peer_exists = self.control_waiters.iter().any(|key| {
                *key != owner
                    && self.connections.get(key).is_some_and(|connection| {
                        connection
                            .waiter
                            .as_ref()
                            .is_some_and(|waiter| waiter.path_ready_at <= now)
                    })
            });
            if owner_ready || !ready_peer_exists {
                return Some(owner);
            }
            if let Some(connection) = self.connections.get_mut(&owner) {
                connection.control_deficit_bytes = 0;
            }
            self.control_turn = None;
        }

        let next = self.control_waiters.iter().copied().find(|key| {
            self.connections.get(key).is_some_and(|connection| {
                connection
                    .waiter
                    .as_ref()
                    .is_some_and(|waiter| waiter.path_ready_at <= now)
            })
        })?;
        self.assign_control_turn(next, reserve_bytes);
        Some(next)
    }

    fn reactivate_ready_control_turn(&mut self, now: Instant, reserve_bytes: u64) {
        self.control_yielding_to_bulk = false;
        self.bulk_service_before_control_bytes = 0;
        let previous = self.control_turn;
        let owner = self.ensure_ready_control_turn(now, reserve_bytes);
        if owner.is_some() && owner == previous {
            if let Some(connection) = owner.and_then(|key| self.connections.get_mut(&key)) {
                connection.control_deficit_bytes = connection
                    .control_deficit_bytes
                    .saturating_add(reserve_bytes);
            }
        }
    }

    fn assign_bulk_turn(&mut self, key: EndpointPacingConnectionKey, quantum_bytes: u64) {
        self.bulk_turn = Some(key);
        if let Some(connection) = self.connections.get_mut(&key) {
            connection.service_counters.turn_count =
                connection.service_counters.turn_count.saturating_add(1);
            connection.bulk_deficit_bytes =
                connection.bulk_deficit_bytes.saturating_add(quantum_bytes);
        }
    }

    fn ensure_ready_bulk_turn(
        &mut self,
        now: Instant,
        quantum_bytes: u64,
    ) -> Option<EndpointPacingConnectionKey> {
        self.clean_bulk_waiters();

        if let Some(owner) = self.bulk_turn {
            let owner_ready = self.connections.get(&owner).is_some_and(|connection| {
                connection
                    .waiter
                    .as_ref()
                    .is_some_and(|waiter| waiter.path_ready_at <= now)
            });
            let ready_peer_exists = self.bulk_waiters.iter().any(|key| {
                *key != owner
                    && self.connections.get(key).is_some_and(|connection| {
                        connection
                            .waiter
                            .as_ref()
                            .is_some_and(|waiter| waiter.path_ready_at <= now)
                    })
            });
            if owner_ready || !ready_peer_exists {
                return Some(owner);
            }
            self.bulk_turn = None;
        }

        let next = self.bulk_waiters.iter().copied().find(|key| {
            self.connections.get(key).is_some_and(|connection| {
                connection
                    .waiter
                    .as_ref()
                    .is_some_and(|waiter| waiter.path_ready_at <= now)
            })
        })?;
        self.assign_bulk_turn(next, quantum_bytes);
        Some(next)
    }

    fn rotate_bulk_turn(
        &mut self,
        owner: EndpointPacingConnectionKey,
        now: Instant,
        quantum_bytes: u64,
    ) -> bool {
        let Some(next) = self.bulk_waiters.iter().copied().find(|key| {
            *key != owner
                && self.connections.get(key).is_some_and(|connection| {
                    connection
                        .waiter
                        .as_ref()
                        .is_some_and(|waiter| waiter.path_ready_at <= now)
                })
        }) else {
            return false;
        };
        self.assign_bulk_turn(next, quantum_bytes);
        true
    }

    fn take_turn_waker(
        &mut self,
        class: PacingTrafficClass,
        exclude: EndpointPacingConnectionKey,
        now: Instant,
    ) -> Option<Waker> {
        let owner = match class {
            PacingTrafficClass::Control => self.control_turn?,
            PacingTrafficClass::Bulk => self.bulk_turn?,
        };
        if owner == exclude {
            return None;
        }
        let waiter = self.connections.get_mut(&owner)?.waiter.as_mut()?;
        if waiter.class != class || waiter.path_ready_at > now {
            return None;
        }
        waiter.waker.take()
    }

    fn take_next_waiter_waker(&mut self, exclude: EndpointPacingConnectionKey) -> Option<Waker> {
        let control_first = !self.control_yielding_to_bulk;
        let key = if control_first {
            self.control_waiters
                .iter()
                .chain(self.bulk_waiters.iter())
                .copied()
                .find(|key| *key != exclude)
        } else {
            self.bulk_waiters
                .iter()
                .chain(self.control_waiters.iter())
                .copied()
                .find(|key| *key != exclude)
        }?;
        self.connections
            .get_mut(&key)?
            .waiter
            .as_mut()?
            .waker
            .take()
    }

    fn take_any_waiter_waker(&mut self) -> Option<Waker> {
        let control_first = !self.control_yielding_to_bulk;
        let key = if control_first {
            self.control_waiters
                .iter()
                .chain(self.bulk_waiters.iter())
                .copied()
                .next()
        } else {
            self.bulk_waiters
                .iter()
                .chain(self.control_waiters.iter())
                .copied()
                .next()
        }?;
        self.connections
            .get_mut(&key)?
            .waiter
            .as_mut()?
            .waker
            .take()
    }
}

fn replace_waker(stored: &mut Option<Waker>, current: Option<&Waker>) -> bool {
    let Some(current) = current else {
        return false;
    };
    if stored
        .as_ref()
        .is_some_and(|existing| existing.will_wake(current))
    {
        return false;
    }
    let replaced = stored.is_some();
    *stored = Some(current.clone());
    replaced
}

impl EndpointPacingService {
    pub(crate) fn new(config: EndpointPacingServiceConfig) -> Self {
        Self(Arc::new(EndpointPacingServiceInner {
            config,
            state: Mutex::new(EndpointPacingState {
                available_tokens: config.burst_bytes,
                live_reservation_bytes: 0,
                outstanding_bytes: 0,
                stateless_outstanding_bytes: 0,
                stateless_responses_sent: 0,
                stateless_responses_dropped: 0,
                granted_bytes: 0,
                granted_datagrams: 0,
                refunded_bytes: 0,
                refund_events: 0,
                sent_bytes: 0,
                sent_datagrams: 0,
                abandoned_bytes: 0,
                abandoned_datagrams: 0,
                outstanding_bytes_high_water: 0,
                socket_would_block_events: 0,
                socket_would_block_outstanding_high_water: 0,
                endpoint_delay_events: 0,
                max_endpoint_delay_nanos: 0,
                waiter_high_water: 0,
                cancellations: 0,
                migrations: 0,
                detaches: 0,
                fairness_lead_high_water_bytes: 0,
                stale_waker_events: 0,
                refill_remainder: 0,
                last_refill: None,
                connections: FxHashMap::default(),
                control_waiters: VecDeque::new(),
                control_turn: None,
                control_yielding_to_bulk: false,
                bulk_service_before_control_bytes: 0,
                bulk_waiters: VecDeque::new(),
                bulk_turn: None,
            }),
        }))
    }

    pub(crate) fn attach_connection(&self, key: EndpointPacingConnectionKey) -> bool {
        let mut state = self.lock_state();
        match state.connections.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(ConnectionAccounting::attached());
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    pub(crate) fn migrate_connection(
        &self,
        old_key: EndpointPacingConnectionKey,
        new_key: EndpointPacingConnectionKey,
    ) -> Result<(), MigrationError> {
        let mut state = self.lock_state();
        if old_key == new_key
            || state.connections.contains_key(&new_key)
            || !state
                .connections
                .get(&old_key)
                .is_some_and(|connection| connection.attached)
        {
            return Err(MigrationError);
        }

        if state.connections.get(&old_key).is_some_and(|connection| {
            connection
                .waiter
                .as_ref()
                .is_some_and(|waiter| waiter.waker.is_some())
        }) {
            state.stale_waker_events = state.stale_waker_events.saturating_add(1);
        }
        state.remove_waiter(old_key);
        let (remove_old, service_counters) = {
            let connection = state
                .connections
                .get_mut(&old_key)
                .expect("validated old pacing connection");
            connection.attached = false;
            let service_counters = connection.service_counters;
            connection.service_counters = ConnectionServiceCounters::default();
            (
                connection.live_reservation_bytes == 0 && connection.outstanding_bytes == 0,
                service_counters,
            )
        };
        if remove_old {
            state.connections.remove(&old_key);
        }
        let mut migrated = ConnectionAccounting::attached();
        migrated.service_counters = service_counters;
        state.connections.insert(new_key, migrated);
        state.migrations = state.migrations.saturating_add(1);
        state.assert_conservation(self.0.config.burst_bytes);
        let wake = state.take_next_waiter_waker(old_key);
        drop(state);
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn reserve(
        &self,
        key: EndpointPacingConnectionKey,
        now: Instant,
        planned_bytes: u64,
    ) -> Option<DatagramReservation> {
        if planned_bytes == 0 {
            return None;
        }

        let mut state = self.lock_state();
        if !state
            .connections
            .get(&key)
            .is_some_and(|connection| connection.attached)
        {
            return None;
        }
        state.refill(self.0.config, now);
        if planned_bytes > state.available_tokens {
            return None;
        }
        state.available_tokens -= planned_bytes;
        state.live_reservation_bytes += planned_bytes;
        let connection = state.connections.get_mut(&key)?;
        connection.live_reservation_bytes += planned_bytes;
        state.assert_conservation(self.0.config.burst_bytes);
        drop(state);

        Some(DatagramReservation {
            service: self.0.clone(),
            key,
            planned_bytes,
            charge: ReservationCharge::None,
            settled: false,
        })
    }

    pub(crate) fn poll_reserve(
        &self,
        key: EndpointPacingConnectionKey,
        class: PacingTrafficClass,
        path_ready_at: Instant,
        planned_bytes: u64,
        waker: Option<&Waker>,
        now: Instant,
    ) -> PollReservation {
        if planned_bytes == 0 {
            return PollReservation::Blocked {
                deadline: None,
                reason: ReservationBlockReason::InvalidRequest,
            };
        }

        let mut state = self.lock_state();
        if !state
            .connections
            .get(&key)
            .is_some_and(|connection| connection.attached)
        {
            return PollReservation::Blocked {
                deadline: None,
                reason: ReservationBlockReason::Detached,
            };
        }
        state.refill(self.0.config, now);

        if class == PacingTrafficClass::Control {
            state.register_control_waiter(key, path_ready_at, planned_bytes, waker);
            if path_ready_at > now {
                state.ensure_ready_control_turn(now, self.0.config.control_reserve_bytes);
                let deadline =
                    state.combined_deadline(self.0.config, path_ready_at, planned_bytes, now);
                return PollReservation::Blocked {
                    deadline,
                    reason: ReservationBlockReason::Path,
                };
            }

            state.clean_bulk_waiters();
            let bulk_ready = state.has_ready_waiter(PacingTrafficClass::Bulk, now);
            if state.control_yielding_to_bulk && bulk_ready {
                return PollReservation::Blocked {
                    deadline: None,
                    reason: ReservationBlockReason::Turn,
                };
            }

            let Some(owner) =
                state.ensure_ready_control_turn(now, self.0.config.control_reserve_bytes)
            else {
                return PollReservation::Blocked {
                    deadline: None,
                    reason: ReservationBlockReason::Turn,
                };
            };
            if owner != key {
                return PollReservation::Blocked {
                    deadline: None,
                    reason: ReservationBlockReason::Turn,
                };
            }

            let deficit = state
                .connections
                .get(&key)
                .map_or(0, |connection| connection.control_deficit_bytes);
            if deficit < planned_bytes {
                if bulk_ready {
                    state.control_yielding_to_bulk = true;
                    state.bulk_service_before_control_bytes =
                        self.0.config.connection_quantum_bytes;
                    return PollReservation::Blocked {
                        deadline: None,
                        reason: ReservationBlockReason::Turn,
                    };
                }

                let missing = planned_bytes - deficit;
                let reserve = self.0.config.control_reserve_bytes;
                let quanta = missing.div_ceil(reserve);
                if let Some(connection) = state.connections.get_mut(&key) {
                    connection.control_deficit_bytes = connection
                        .control_deficit_bytes
                        .saturating_add(quanta.saturating_mul(reserve));
                }
            }

            if planned_bytes > state.available_tokens {
                let deadline =
                    state.combined_deadline(self.0.config, path_ready_at, planned_bytes, now);
                state.record_endpoint_delay(deadline, now);
                return PollReservation::Blocked {
                    deadline,
                    reason: ReservationBlockReason::Tokens,
                };
            }

            state.control_waiters.retain(|queued| *queued != key);
            if let Some(connection) = state.connections.get_mut(&key) {
                connection.waiter = None;
                connection.control_deficit_bytes -= planned_bytes;
            }
            return PollReservation::Granted(
                reserve_locked(
                    &self.0,
                    &mut state,
                    key,
                    planned_bytes,
                    ReservationCharge::Control,
                    now,
                )
                .expect("tokens and attachment were checked before control grant"),
            );
        }

        state.register_bulk_waiter(key, path_ready_at, planned_bytes, waker);
        if path_ready_at > now {
            state.ensure_ready_bulk_turn(now, self.0.config.connection_quantum_bytes);
            let deadline =
                state.combined_deadline(self.0.config, path_ready_at, planned_bytes, now);
            return PollReservation::Blocked {
                deadline,
                reason: ReservationBlockReason::Path,
            };
        }

        state.clean_control_waiters();
        let control_ready = state.has_ready_waiter(PacingTrafficClass::Control, now);
        if control_ready && !state.control_yielding_to_bulk {
            return PollReservation::Blocked {
                deadline: None,
                reason: ReservationBlockReason::Turn,
            };
        }
        if control_ready && state.bulk_service_before_control_bytes < planned_bytes {
            state.reactivate_ready_control_turn(now, self.0.config.control_reserve_bytes);
            return PollReservation::Blocked {
                deadline: None,
                reason: ReservationBlockReason::Turn,
            };
        }
        let serving_control_yield = control_ready && state.control_yielding_to_bulk;

        let Some(owner) = state.ensure_ready_bulk_turn(now, self.0.config.connection_quantum_bytes)
        else {
            return PollReservation::Blocked {
                deadline: None,
                reason: ReservationBlockReason::Turn,
            };
        };
        if owner != key {
            return PollReservation::Blocked {
                deadline: None,
                reason: ReservationBlockReason::Turn,
            };
        }

        let deficit = state
            .connections
            .get(&key)
            .map_or(0, |connection| connection.bulk_deficit_bytes);
        if deficit < planned_bytes {
            if state.rotate_bulk_turn(key, now, self.0.config.connection_quantum_bytes) {
                let wake = state.take_turn_waker(PacingTrafficClass::Bulk, key, now);
                drop(state);
                if let Some(waker) = wake {
                    waker.wake();
                }
                return PollReservation::Blocked {
                    deadline: None,
                    reason: ReservationBlockReason::Turn,
                };
            }

            let missing = planned_bytes - deficit;
            let quantum = self.0.config.connection_quantum_bytes;
            let quanta = missing.div_ceil(quantum);
            if let Some(connection) = state.connections.get_mut(&key) {
                connection.bulk_deficit_bytes = connection
                    .bulk_deficit_bytes
                    .saturating_add(quanta.saturating_mul(quantum));
            }
        }

        if planned_bytes > state.available_tokens {
            let deadline =
                state.combined_deadline(self.0.config, path_ready_at, planned_bytes, now);
            state.record_endpoint_delay(deadline, now);
            return PollReservation::Blocked {
                deadline,
                reason: ReservationBlockReason::Tokens,
            };
        }

        state.bulk_waiters.retain(|queued| *queued != key);
        if let Some(connection) = state.connections.get_mut(&key) {
            connection.waiter = None;
            connection.bulk_deficit_bytes -= planned_bytes;
        }
        if serving_control_yield {
            state.bulk_service_before_control_bytes -= planned_bytes;
        }
        PollReservation::Granted(
            reserve_locked(
                &self.0,
                &mut state,
                key,
                planned_bytes,
                ReservationCharge::Bulk {
                    control_yield: serving_control_yield,
                },
                now,
            )
            .expect("tokens and attachment were checked before bulk grant"),
        )
    }

    pub(crate) fn cancel_waiter(&self, key: EndpointPacingConnectionKey) -> bool {
        let mut state = self.lock_state();
        let removed = state.remove_waiter(key);
        if removed {
            state.cancellations = state.cancellations.saturating_add(1);
        }
        let wake = removed.then(|| state.take_next_waiter_waker(key)).flatten();
        drop(state);
        if let Some(waker) = wake {
            waker.wake();
        }
        removed
    }

    #[cfg(test)]
    pub(crate) fn note_socket_sent(
        &self,
        key: EndpointPacingConnectionKey,
        batch_bytes: u64,
        now: Instant,
    ) -> Result<(), SocketOutcomeError> {
        self.note_socket_outcome(key, batch_bytes, 1, now, true)
    }

    fn note_socket_batch_sent(
        &self,
        key: EndpointPacingConnectionKey,
        batch_bytes: u64,
        batch_datagrams: u64,
        now: Instant,
    ) -> Result<(), SocketOutcomeError> {
        self.note_socket_outcome(key, batch_bytes, batch_datagrams, now, true)
    }

    pub(crate) fn reserve_stateless_response(
        &self,
        now: Instant,
        response_bytes: u64,
    ) -> Option<StatelessResponseReservation> {
        let mut state = self.lock_state();
        state.refill(self.0.config, now);
        if response_bytes == 0 || response_bytes > state.available_tokens {
            state.stateless_responses_dropped = state.stateless_responses_dropped.saturating_add(1);
            return None;
        }
        state.available_tokens -= response_bytes;
        state.outstanding_bytes += response_bytes;
        state.stateless_outstanding_bytes += response_bytes;
        state.outstanding_bytes_high_water = state
            .outstanding_bytes_high_water
            .max(state.outstanding_bytes);
        state.assert_conservation(self.0.config.burst_bytes);
        Some(StatelessResponseReservation {
            service: self.0.clone(),
            response_bytes,
            reserved_at: now,
            settled: false,
        })
    }

    fn note_stateless_response_outcome(
        service: &Arc<EndpointPacingServiceInner>,
        response_bytes: u64,
        now: Instant,
        sent: bool,
    ) -> bool {
        let mut state = service.state.lock().unwrap_or_else(PoisonError::into_inner);
        if response_bytes == 0 || response_bytes > state.stateless_outstanding_bytes {
            return false;
        }
        state.refill(service.config, now);
        state.outstanding_bytes -= response_bytes;
        state.stateless_outstanding_bytes -= response_bytes;
        if sent {
            state.stateless_responses_sent = state.stateless_responses_sent.saturating_add(1);
        } else {
            state.stateless_responses_dropped = state.stateless_responses_dropped.saturating_add(1);
        }
        state.assert_conservation(service.config.burst_bytes);
        let wake = state.take_any_waiter_waker();
        drop(state);
        if let Some(waker) = wake {
            waker.wake();
        }
        true
    }

    #[cfg(test)]
    pub(crate) fn note_socket_abandoned(
        &self,
        key: EndpointPacingConnectionKey,
        batch_bytes: u64,
        now: Instant,
    ) -> Result<(), SocketOutcomeError> {
        self.note_socket_outcome(key, batch_bytes, 1, now, false)
    }

    fn note_socket_batch_abandoned(
        &self,
        key: EndpointPacingConnectionKey,
        batch_bytes: u64,
        batch_datagrams: u64,
        now: Instant,
    ) -> Result<(), SocketOutcomeError> {
        self.note_socket_outcome(key, batch_bytes, batch_datagrams, now, false)
    }

    fn note_socket_would_block(
        &self,
        key: EndpointPacingConnectionKey,
        batch_bytes: u64,
    ) -> Result<(), SocketOutcomeError> {
        let mut state = self.lock_state();
        let valid = batch_bytes != 0
            && state.connections.get(&key).is_some_and(|connection| {
                connection.outstanding_bytes >= batch_bytes && connection.outstanding_datagrams != 0
            });
        if !valid {
            return Err(SocketOutcomeError);
        }
        state.socket_would_block_events = state.socket_would_block_events.saturating_add(1);
        state.socket_would_block_outstanding_high_water = state
            .socket_would_block_outstanding_high_water
            .max(batch_bytes);
        Ok(())
    }

    fn note_socket_outcome(
        &self,
        key: EndpointPacingConnectionKey,
        batch_bytes: u64,
        batch_datagrams: u64,
        now: Instant,
        sent: bool,
    ) -> Result<(), SocketOutcomeError> {
        let mut state = self.lock_state();
        let Some(connection) = state.connections.get(&key) else {
            return Err(SocketOutcomeError);
        };
        if batch_bytes == 0
            || batch_datagrams == 0
            || batch_bytes > connection.outstanding_bytes
            || batch_datagrams > connection.outstanding_datagrams
        {
            return Err(SocketOutcomeError);
        }
        state.refill(self.0.config, now);
        let Some(connection) = state.connections.get_mut(&key) else {
            return Err(SocketOutcomeError);
        };
        connection.outstanding_bytes -= batch_bytes;
        connection.outstanding_datagrams -= batch_datagrams;
        let remove_connection = !connection.attached
            && connection.live_reservation_bytes == 0
            && connection.outstanding_bytes == 0
            && connection.outstanding_datagrams == 0;
        state.outstanding_bytes -= batch_bytes;
        if sent {
            state.sent_bytes = state.sent_bytes.saturating_add(batch_bytes);
            state.sent_datagrams = state.sent_datagrams.saturating_add(batch_datagrams);
        } else {
            state.abandoned_bytes = state.abandoned_bytes.saturating_add(batch_bytes);
            state.abandoned_datagrams = state.abandoned_datagrams.saturating_add(batch_datagrams);
        }
        if remove_connection {
            state.connections.remove(&key);
        }
        state.assert_conservation(self.0.config.burst_bytes);
        let wake = state.take_next_waiter_waker(key);
        drop(state);
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }

    pub(crate) fn detach_connection(
        &self,
        key: EndpointPacingConnectionKey,
        now: Instant,
    ) -> Result<DetachedConnectionAccounting, DetachError> {
        let mut state = self.lock_state();
        let Some(connection) = state.connections.get(&key) else {
            return Err(DetachError);
        };
        if !connection.attached {
            return Err(DetachError);
        }

        state.refill(self.0.config, now);
        if state.connections.get(&key).is_some_and(|connection| {
            connection
                .waiter
                .as_ref()
                .is_some_and(|waiter| waiter.waker.is_some())
        }) {
            state.stale_waker_events = state.stale_waker_events.saturating_add(1);
        }
        state.remove_waiter(key);
        let Some(connection) = state.connections.get_mut(&key) else {
            return Err(DetachError);
        };
        connection.attached = false;
        let result = DetachedConnectionAccounting {
            abandoned_bytes: connection.outstanding_bytes,
            abandoned_datagrams: connection.outstanding_datagrams,
            live_reservation_bytes: connection.live_reservation_bytes,
        };
        connection.outstanding_bytes = 0;
        connection.outstanding_datagrams = 0;
        state.outstanding_bytes -= result.abandoned_bytes;
        state.abandoned_bytes = state.abandoned_bytes.saturating_add(result.abandoned_bytes);
        state.abandoned_datagrams = state
            .abandoned_datagrams
            .saturating_add(result.abandoned_datagrams);
        if result.live_reservation_bytes == 0 {
            state.connections.remove(&key);
        }
        state.detaches = state.detaches.saturating_add(1);
        state.assert_conservation(self.0.config.burst_bytes);
        Ok(result)
    }

    pub(crate) fn snapshot(&self) -> EndpointPacingSnapshot {
        let state = self.lock_state();
        EndpointPacingSnapshot {
            rate_bytes_per_second: self.0.config.rate_bytes_per_second,
            burst_bytes: self.0.config.burst_bytes,
            control_reserve_bytes: self.0.config.control_reserve_bytes,
            connection_quantum_bytes: self.0.config.connection_quantum_bytes,
            available_tokens: state.available_tokens,
            live_reservation_bytes: state.live_reservation_bytes,
            outstanding_bytes: state.outstanding_bytes,
            connection_records: state.connections.len(),
            stateless_responses_sent: state.stateless_responses_sent,
            stateless_responses_dropped: state.stateless_responses_dropped,
            granted_bytes: state.granted_bytes,
            granted_datagrams: state.granted_datagrams,
            refunded_bytes: state.refunded_bytes,
            refund_events: state.refund_events,
            sent_bytes: state.sent_bytes,
            sent_datagrams: state.sent_datagrams,
            abandoned_bytes: state.abandoned_bytes,
            abandoned_datagrams: state.abandoned_datagrams,
            outstanding_bytes_high_water: state.outstanding_bytes_high_water,
            socket_would_block_events: state.socket_would_block_events,
            socket_would_block_outstanding_high_water: state
                .socket_would_block_outstanding_high_water,
            endpoint_delay_events: state.endpoint_delay_events,
            max_endpoint_delay_nanos: state.max_endpoint_delay_nanos,
            control_waiters: state.control_waiters.len(),
            bulk_waiters: state.bulk_waiters.len(),
            waiter_high_water: state.waiter_high_water,
            cancellations: state.cancellations,
            migrations: state.migrations,
            detaches: state.detaches,
            fairness_lead_high_water_bytes: state.fairness_lead_high_water_bytes,
            stale_waker_events: state.stale_waker_events,
        }
    }

    fn connection_snapshot(
        &self,
        key: EndpointPacingConnectionKey,
    ) -> Option<EndpointPacingConnectionSnapshot> {
        let state = self.lock_state();
        let connection = state.connections.get(&key)?;
        Some(EndpointPacingConnectionSnapshot {
            connection_handle: key.connection_handle.0,
            path_generation: key.path_generation,
            attached: connection.attached,
            live_reservation_bytes: connection.live_reservation_bytes,
            outstanding_bytes: connection.outstanding_bytes,
            granted_bulk_bytes: connection.service_counters.granted_bulk_bytes,
            granted_control_bytes: connection.service_counters.granted_control_bytes,
            turn_count: connection.service_counters.turn_count,
            wait_count: connection.service_counters.wait_count,
            max_service_gap_nanos: connection.service_counters.max_service_gap_nanos,
        })
    }

    fn lock_state(&self) -> MutexGuard<'_, EndpointPacingState> {
        self.0.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    pub(crate) fn stable_id(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionEndpointPacing {
    service: EndpointPacingService,
    key: EndpointPacingConnectionKey,
    driver_waker: Option<Waker>,
    pending_socket_batch: Option<PendingSocketBatch>,
    socket_batch_exposed: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingSocketBatch {
    key: EndpointPacingConnectionKey,
    bytes: u64,
    datagrams: u64,
}

impl ConnectionEndpointPacing {
    pub(crate) fn attach(service: EndpointPacingService, key: EndpointPacingConnectionKey) -> Self {
        let attached = service.attach_connection(key);
        debug_assert!(attached, "endpoint pacing connection key must be unique");
        Self {
            service,
            key,
            driver_waker: None,
            pending_socket_batch: None,
            socket_batch_exposed: false,
        }
    }

    pub(crate) fn begin_transmit_poll(&self) -> bool {
        !self.socket_batch_exposed
    }

    pub(crate) fn poll_reserve(
        &mut self,
        class: PacingTrafficClass,
        path_ready_at: Instant,
        planned_bytes: u64,
        now: Instant,
    ) -> PollReservation {
        if self.socket_batch_exposed {
            return PollReservation::Blocked {
                deadline: None,
                reason: ReservationBlockReason::SocketOutstanding,
            };
        }
        self.service.poll_reserve(
            self.key,
            class,
            path_ready_at,
            planned_bytes,
            self.driver_waker.as_ref(),
            now,
        )
    }

    pub(crate) fn set_driver_waker(&mut self, waker: &Waker) {
        let _ = replace_waker(&mut self.driver_waker, Some(waker));
    }

    pub(crate) fn cancel_waiter(&self) -> bool {
        self.service.cancel_waiter(self.key)
    }

    pub(crate) fn settle_datagram(
        &mut self,
        reservation: DatagramReservation,
        actual_bytes: u64,
    ) -> Result<(), ReservationError> {
        if reservation.key != self.key {
            return Err(ReservationError);
        }
        let batch_bytes = match self.pending_socket_batch {
            Some(batch) if batch.key == self.key => batch
                .bytes
                .checked_add(actual_bytes)
                .ok_or(ReservationError)?,
            Some(_) => return Err(ReservationError),
            None => actual_bytes,
        };
        let batch_datagrams = match self.pending_socket_batch {
            Some(batch) if batch.key == self.key => {
                batch.datagrams.checked_add(1).ok_or(ReservationError)?
            }
            Some(_) => return Err(ReservationError),
            None => 1,
        };
        reservation.settle_built(actual_bytes)?;
        self.pending_socket_batch = Some(PendingSocketBatch {
            key: self.key,
            bytes: batch_bytes,
            datagrams: batch_datagrams,
        });
        Ok(())
    }

    pub(crate) fn expose_socket_batch(&mut self) -> bool {
        if self.pending_socket_batch.is_none() || self.socket_batch_exposed {
            return false;
        }
        self.socket_batch_exposed = true;
        true
    }

    pub(crate) fn migrate(&mut self, path_generation: u64) -> Result<(), MigrationError> {
        let new_key = EndpointPacingConnectionKey::new(self.key.connection_handle, path_generation);
        self.service.migrate_connection(self.key, new_key)?;
        self.key = new_key;
        Ok(())
    }

    pub(crate) fn note_socket_sent(&mut self, now: Instant) -> Result<bool, SocketOutcomeError> {
        self.note_socket_outcome(now, true)
    }

    pub(crate) fn note_socket_abandoned(
        &mut self,
        now: Instant,
    ) -> Result<bool, SocketOutcomeError> {
        self.note_socket_outcome(now, false)
    }

    pub(crate) fn note_socket_would_block(&self) -> Result<bool, SocketOutcomeError> {
        let Some(batch) = self.pending_socket_batch else {
            return Ok(false);
        };
        self.service
            .note_socket_would_block(batch.key, batch.bytes)?;
        Ok(true)
    }

    fn note_socket_outcome(
        &mut self,
        now: Instant,
        sent: bool,
    ) -> Result<bool, SocketOutcomeError> {
        let Some(batch) = self.pending_socket_batch.take() else {
            return Ok(false);
        };
        let result = if sent {
            self.service
                .note_socket_batch_sent(batch.key, batch.bytes, batch.datagrams, now)
        } else {
            self.service
                .note_socket_batch_abandoned(batch.key, batch.bytes, batch.datagrams, now)
        };
        if result.is_err() {
            self.pending_socket_batch = Some(batch);
            return Err(SocketOutcomeError);
        }
        self.socket_batch_exposed = false;
        Ok(true)
    }

    pub(crate) fn detach(
        &mut self,
        now: Instant,
    ) -> Result<DetachedConnectionAccounting, DetachError> {
        let abandoned_batch_bytes = self.pending_socket_batch.map_or(0, |batch| batch.bytes);
        let abandoned_batch_datagrams =
            self.pending_socket_batch.map_or(0, |batch| batch.datagrams);
        self.note_socket_abandoned(now).map_err(|_| DetachError)?;
        let mut detached = self.service.detach_connection(self.key, now)?;
        detached.abandoned_bytes = detached
            .abandoned_bytes
            .checked_add(abandoned_batch_bytes)
            .ok_or(DetachError)?;
        detached.abandoned_datagrams = detached
            .abandoned_datagrams
            .checked_add(abandoned_batch_datagrams)
            .ok_or(DetachError)?;
        self.driver_waker = None;
        self.socket_batch_exposed = false;
        Ok(detached)
    }

    pub(crate) fn pending_socket_batch_bytes(&self) -> u64 {
        self.pending_socket_batch.map_or(0, |batch| batch.bytes)
    }

    pub(crate) fn snapshot(&self) -> Option<EndpointPacingConnectionSnapshot> {
        self.service.connection_snapshot(self.key)
    }

    #[cfg(test)]
    pub(crate) fn service_id(&self) -> usize {
        self.service.stable_id()
    }
}

/// A point-in-time view of endpoint pacing conservation state.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointPacingSnapshot {
    /// Configured aggregate sustained wire-byte rate.
    pub rate_bytes_per_second: u64,
    /// Configured aggregate burst capacity.
    pub burst_bytes: u64,
    /// Configured control reserve within the aggregate burst.
    pub control_reserve_bytes: u64,
    /// Configured per-connection bulk service quantum.
    pub connection_quantum_bytes: u64,
    /// Bytes currently available for new reservations.
    pub available_tokens: u64,
    /// Bytes held by reservations whose datagrams have not been finalized.
    pub live_reservation_bytes: u64,
    /// Finalized bytes waiting for a real UDP socket outcome.
    pub outstanding_bytes: u64,
    /// Live or tombstoned connection-accounting records.
    pub connection_records: usize,
    /// Stateless endpoint responses accepted by the UDP socket.
    pub stateless_responses_sent: u64,
    /// Stateless endpoint responses denied by service or abandoned by the UDP socket.
    pub stateless_responses_dropped: u64,
    /// Bytes granted to connection datagram reservations.
    pub granted_bytes: u64,
    /// Connection datagram reservations granted.
    pub granted_datagrams: u64,
    /// Planned bytes returned by short or dropped reservations.
    pub refunded_bytes: u64,
    /// Reservation settlements or drops that returned bytes.
    pub refund_events: u64,
    /// Connection bytes accepted by the UDP socket.
    pub sent_bytes: u64,
    /// Connection datagrams accepted by the UDP socket.
    pub sent_datagrams: u64,
    /// Connection bytes abandoned before UDP socket acceptance.
    pub abandoned_bytes: u64,
    /// Connection datagrams abandoned before UDP socket acceptance.
    pub abandoned_datagrams: u64,
    /// Maximum connection plus stateless outstanding bytes observed.
    pub outstanding_bytes_high_water: u64,
    /// UDP socket `WouldBlock` observations while a charged batch was retained.
    pub socket_would_block_events: u64,
    /// Largest charged batch retained across a UDP socket `WouldBlock`.
    pub socket_would_block_outstanding_high_water: u64,
    /// Token-service denials that installed an endpoint deadline.
    pub endpoint_delay_events: u64,
    /// Maximum computed endpoint token delay in nanoseconds.
    pub max_endpoint_delay_nanos: u64,
    /// Current control-class waiter count.
    pub control_waiters: usize,
    /// Current bulk-class waiter count.
    pub bulk_waiters: usize,
    /// Maximum aggregate waiter count.
    pub waiter_high_water: usize,
    /// Waiters explicitly cancelled after an empty or completed transmit poll.
    pub cancellations: u64,
    /// Successful connection path-generation migrations.
    pub migrations: u64,
    /// Successful connection detach operations.
    pub detaches: u64,
    /// Maximum cumulative bulk grant lead while at least two bulk connections were backlogged.
    pub fairness_lead_high_water_bytes: u64,
    /// Stored waiter wakers replaced or invalidated before they could be used.
    pub stale_waker_events: u64,
}

/// A point-in-time view of one connection's endpoint pacing service history.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointPacingConnectionSnapshot {
    /// Endpoint-local Quinn connection handle.
    pub connection_handle: usize,
    /// Current path generation for migration-safe accounting.
    pub path_generation: u64,
    /// Whether the connection is live rather than an outstanding tombstone.
    pub attached: bool,
    /// Bytes held by live datagram reservations for this connection.
    pub live_reservation_bytes: u64,
    /// Finalized bytes waiting for this connection's UDP socket outcome.
    pub outstanding_bytes: u64,
    /// Cumulative bulk-class bytes granted to this connection.
    pub granted_bulk_bytes: u64,
    /// Cumulative control-class bytes granted to this connection.
    pub granted_control_bytes: u64,
    /// Service turns assigned to this connection.
    pub turn_count: u64,
    /// Times this connection entered a control or bulk waiter queue.
    pub wait_count: u64,
    /// Maximum time between grants for this connection, in nanoseconds.
    pub max_service_gap_nanos: u64,
}

/// A stateless endpoint response charged before its UDP socket attempt.
#[doc(hidden)]
#[derive(Debug)]
pub struct StatelessResponseReservation {
    service: Arc<EndpointPacingServiceInner>,
    response_bytes: u64,
    reserved_at: Instant,
    settled: bool,
}

/// Endpoint pacing denied a stateless response because no immediate byte service was available.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatelessResponseDenied;

impl StatelessResponseReservation {
    /// Report that the UDP socket accepted this response.
    #[doc(hidden)]
    pub fn note_sent(mut self, now: Instant) {
        self.finish(now, true);
    }

    /// Report that the UDP socket rejected or abandoned this response.
    #[doc(hidden)]
    pub fn note_abandoned(mut self, now: Instant) {
        self.finish(now, false);
    }

    fn finish(&mut self, now: Instant, sent: bool) {
        if self.settled {
            return;
        }
        let settled = EndpointPacingService::note_stateless_response_outcome(
            &self.service,
            self.response_bytes,
            now,
            sent,
        );
        debug_assert!(
            settled,
            "stateless response outcome must match its reservation"
        );
        self.settled = true;
    }
}

impl Drop for StatelessResponseReservation {
    fn drop(&mut self) {
        if !self.settled {
            self.finish(self.reserved_at, false);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DetachedConnectionAccounting {
    pub(crate) abandoned_bytes: u64,
    pub(crate) abandoned_datagrams: u64,
    pub(crate) live_reservation_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct DatagramReservation {
    service: Arc<EndpointPacingServiceInner>,
    key: EndpointPacingConnectionKey,
    planned_bytes: u64,
    charge: ReservationCharge,
    settled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReservationCharge {
    #[allow(dead_code)]
    None,
    Control,
    Bulk {
        control_yield: bool,
    },
}

impl DatagramReservation {
    pub(crate) fn settle_built(mut self, actual_bytes: u64) -> Result<(), ReservationError> {
        if actual_bytes > self.planned_bytes {
            return Err(ReservationError);
        }

        let mut state = self
            .service
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Some(connection) = state.connections.get_mut(&self.key) else {
            return Err(ReservationError);
        };
        if !connection.attached || connection.live_reservation_bytes < self.planned_bytes {
            return Err(ReservationError);
        }
        connection.live_reservation_bytes -= self.planned_bytes;
        connection.outstanding_bytes += actual_bytes;
        connection.outstanding_datagrams = connection.outstanding_datagrams.saturating_add(1);
        let refund = self.planned_bytes - actual_bytes;
        match self.charge {
            ReservationCharge::None => {}
            ReservationCharge::Control => {
                connection.control_deficit_bytes =
                    connection.control_deficit_bytes.saturating_add(refund);
            }
            ReservationCharge::Bulk { control_yield } => {
                connection.bulk_deficit_bytes =
                    connection.bulk_deficit_bytes.saturating_add(refund);
                if control_yield {
                    state.bulk_service_before_control_bytes = state
                        .bulk_service_before_control_bytes
                        .saturating_add(refund);
                }
            }
        }
        state.live_reservation_bytes -= self.planned_bytes;
        state.outstanding_bytes += actual_bytes;
        state.available_tokens += self.planned_bytes - actual_bytes;
        state.outstanding_bytes_high_water = state
            .outstanding_bytes_high_water
            .max(state.outstanding_bytes);
        if refund > 0 && !matches!(self.charge, ReservationCharge::None) {
            state.refunded_bytes = state.refunded_bytes.saturating_add(refund);
            state.refund_events = state.refund_events.saturating_add(1);
        }
        state.assert_conservation(self.service.config.burst_bytes);
        self.settled = true;
        Ok(())
    }
}

impl Drop for DatagramReservation {
    fn drop(&mut self) {
        if self.settled {
            return;
        }

        let mut state = self
            .service
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let remove_connection = match state.connections.get_mut(&self.key) {
            Some(connection) if connection.live_reservation_bytes >= self.planned_bytes => {
                connection.live_reservation_bytes -= self.planned_bytes;
                match self.charge {
                    ReservationCharge::None => {}
                    ReservationCharge::Control => {
                        connection.control_deficit_bytes = connection
                            .control_deficit_bytes
                            .saturating_add(self.planned_bytes);
                    }
                    ReservationCharge::Bulk { .. } => {
                        connection.bulk_deficit_bytes = connection
                            .bulk_deficit_bytes
                            .saturating_add(self.planned_bytes);
                    }
                }
                !connection.attached
                    && connection.live_reservation_bytes == 0
                    && connection.outstanding_bytes == 0
            }
            _ => {
                debug_assert!(false, "live reservation must retain its connection record");
                return;
            }
        };
        state.live_reservation_bytes -= self.planned_bytes;
        state.available_tokens += self.planned_bytes;
        if !matches!(self.charge, ReservationCharge::None) {
            state.refunded_bytes = state.refunded_bytes.saturating_add(self.planned_bytes);
            state.refund_events = state.refund_events.saturating_add(1);
        }
        if matches!(
            self.charge,
            ReservationCharge::Bulk {
                control_yield: true
            }
        ) {
            state.bulk_service_before_control_bytes = state
                .bulk_service_before_control_bytes
                .saturating_add(self.planned_bytes);
        }
        if remove_connection {
            state.connections.remove(&self.key);
        }
        state.assert_conservation(self.service.config.burst_bytes);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReservationError;

/// An endpoint pacing socket outcome did not match the pending connection batch.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SocketOutcomeError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DetachError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MigrationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PacingTrafficClass {
    Control,
    Bulk,
}

#[derive(Debug)]
pub(crate) enum PollReservation {
    Granted(DatagramReservation),
    Blocked {
        deadline: Option<Instant>,
        reason: ReservationBlockReason,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReservationBlockReason {
    InvalidRequest,
    Detached,
    Path,
    Tokens,
    Turn,
    SocketOutstanding,
}

fn reserve_locked(
    service: &Arc<EndpointPacingServiceInner>,
    state: &mut EndpointPacingState,
    key: EndpointPacingConnectionKey,
    planned_bytes: u64,
    charge: ReservationCharge,
    now: Instant,
) -> Option<DatagramReservation> {
    if planned_bytes > state.available_tokens
        || !state
            .connections
            .get(&key)
            .is_some_and(|connection| connection.attached)
    {
        return None;
    }
    state.available_tokens -= planned_bytes;
    state.live_reservation_bytes += planned_bytes;
    let bulk_grant = matches!(charge, ReservationCharge::Bulk { .. });
    let connection = state.connections.get_mut(&key)?;
    connection.live_reservation_bytes += planned_bytes;
    match charge {
        ReservationCharge::None => {}
        ReservationCharge::Control => {
            connection.service_counters.granted_control_bytes = connection
                .service_counters
                .granted_control_bytes
                .saturating_add(planned_bytes);
        }
        ReservationCharge::Bulk { .. } => {
            connection.service_counters.granted_bulk_bytes = connection
                .service_counters
                .granted_bulk_bytes
                .saturating_add(planned_bytes);
        }
    }
    if let Some(previous) = connection.service_counters.last_grant_at {
        let gap = now
            .checked_duration_since(previous)
            .map_or(0, |gap| gap.as_nanos().min(u128::from(u64::MAX)) as u64);
        connection.service_counters.max_service_gap_nanos =
            connection.service_counters.max_service_gap_nanos.max(gap);
    }
    connection.service_counters.last_grant_at = Some(now);
    state.granted_bytes = state.granted_bytes.saturating_add(planned_bytes);
    state.granted_datagrams = state.granted_datagrams.saturating_add(1);
    if bulk_grant {
        state.record_bulk_fairness_lead(key);
    }
    state.assert_conservation(service.config.burst_bytes);
    Some(DatagramReservation {
        service: service.clone(),
        key,
        planned_bytes,
        charge,
        settled: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectionHandle, Duration, Instant};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        task::Wake,
    };

    const RATE_BYTES_PER_SECOND: u64 = 30_720_000;
    const BURST_BYTES: u64 = 61_440;
    const DATAGRAM_BYTES: u64 = 1_280;
    const TEST_KEY: EndpointPacingConnectionKey =
        EndpointPacingConnectionKey::new(ConnectionHandle(0), 0);

    #[test]
    fn fixed_candidate_obeys_exact_one_and_ten_millisecond_bounds() {
        let start = Instant::now();
        let service = test_service(EndpointPacingServiceConfig::endpoint_window_v1(), start);

        let mut sent_bytes = drain_available(&service, start);
        for tick in 1..=200 {
            let now = start + Duration::from_micros(tick * 50);
            sent_bytes += drain_available(&service, now);
            if tick == 20 {
                assert_eq!(sent_bytes, 92_160);
            }
        }

        assert_eq!(sent_bytes, 368_640);
    }

    #[test]
    fn reservation_drop_and_short_settlement_preserve_conservation() {
        let now = Instant::now();
        let service = test_service(EndpointPacingServiceConfig::endpoint_window_v1(), now);

        let reservation = service
            .reserve(TEST_KEY, now, DATAGRAM_BYTES)
            .expect("initial burst");
        assert_conservation_snapshot(&service, BURST_BYTES - DATAGRAM_BYTES, DATAGRAM_BYTES, 0, 1);

        drop(reservation);
        assert_conservation_snapshot(&service, BURST_BYTES, 0, 0, 1);

        service
            .reserve(TEST_KEY, now, DATAGRAM_BYTES)
            .expect("refunded reservation")
            .settle_built(1_000)
            .expect("short datagram is valid");
        assert_conservation_snapshot(&service, BURST_BYTES - 1_000, 0, 1_000, 1);

        service
            .note_socket_sent(TEST_KEY, 1_000, now)
            .expect("settled bytes are outstanding");
        assert_conservation_snapshot(&service, BURST_BYTES - 1_000, 0, 0, 1);
    }

    #[test]
    fn deterministic_traces_obey_the_any_window_bound() {
        for datagram_bytes in [1, 17, 1_279, 1_280, 4_096, BURST_BYTES] {
            let start = Instant::now();
            let service = test_service(EndpointPacingServiceConfig::endpoint_window_v1(), start);
            let mut events = Vec::new();
            let mut elapsed_micros = 0u64;
            let mut cumulative = 0u64;
            let mut seed = 0x4d59_5df4_d0f3_3173u64;

            for event_index in 0..=240 {
                if event_index != 0 {
                    seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                    elapsed_micros += 1 + seed % 250;
                }
                let before = cumulative;
                cumulative += drain_available_bytes(
                    &service,
                    start + Duration::from_micros(elapsed_micros),
                    datagram_bytes,
                );
                events.push((elapsed_micros * 1_000, before, cumulative));
            }

            for start_index in 0..events.len() {
                for end_index in start_index..events.len() {
                    let (window_start, before, _) = events[start_index];
                    let (window_end, _, after) = events[end_index];
                    let elapsed_nanos = window_end - window_start;
                    let bound = u128::from(BURST_BYTES)
                        + u128::from(RATE_BYTES_PER_SECOND) * u128::from(elapsed_nanos)
                            / NANOS_PER_SECOND;
                    let window_bytes = after - before;
                    assert!(
                        u128::from(window_bytes) <= bound,
                        "datagram={datagram_bytes} window={window_start}..={window_end} bytes={window_bytes} bound={bound}"
                    );
                }
            }
        }
    }

    #[test]
    fn refill_preserves_rounding_without_minting_early() {
        let start = Instant::now();
        let service = test_service(test_config(3, 2), start);
        assert_eq!(drain_available_bytes(&service, start, 1), 2);

        assert!(service
            .reserve(TEST_KEY, start + Duration::from_nanos(333_333_333), 1)
            .is_none());
        settle_and_send_one(&service, start + Duration::from_nanos(333_333_334));
        assert!(service
            .reserve(TEST_KEY, start + Duration::from_nanos(666_666_666), 1)
            .is_none());
        settle_and_send_one(&service, start + Duration::from_nanos(666_666_667));
    }

    #[test]
    fn time_rollback_cannot_double_mint() {
        let start = Instant::now();
        let service = test_service(test_config(1_000, 10), start);
        assert_eq!(drain_available_bytes(&service, start, 1), 10);
        assert_eq!(
            drain_available_bytes(&service, start + Duration::from_millis(5), 1),
            5
        );
        assert_eq!(
            drain_available_bytes(&service, start + Duration::from_millis(4), 1),
            0
        );
        assert_eq!(
            drain_available_bytes(&service, start + Duration::from_millis(10), 1),
            5
        );
    }

    #[test]
    fn full_bucket_discards_stale_refill_credit() {
        let start = Instant::now();
        let service = test_service(test_config(3, 2), start);
        let saturated_at = start + Duration::from_secs(10);
        assert_eq!(drain_available_bytes(&service, saturated_at, 1), 2);
        assert!(service
            .reserve(
                TEST_KEY,
                saturated_at + Duration::from_nanos(333_333_333),
                1,
            )
            .is_none());
        settle_and_send_one(&service, saturated_at + Duration::from_nanos(333_333_334));
    }

    #[test]
    fn refill_arithmetic_saturates_without_overflow() {
        let start = Instant::now();
        let service = test_service(test_config(u64::MAX, u64::MAX), start);
        service
            .reserve(TEST_KEY, start, u64::MAX)
            .expect("initial full-width burst")
            .settle_built(u64::MAX)
            .expect("full-width settlement");
        service
            .note_socket_sent(TEST_KEY, u64::MAX, start)
            .expect("full-width outstanding batch");

        let thousand_years = Duration::from_secs(1_000 * 365 * 24 * 60 * 60);
        let later = start
            .checked_add(thousand_years)
            .expect("platform Instant represents the test interval");
        assert!(service.reserve(TEST_KEY, later, u64::MAX).is_some());
    }

    #[test]
    fn rejected_oversized_settlement_refunds_the_reservation() {
        let now = Instant::now();
        let service = test_service(EndpointPacingServiceConfig::endpoint_window_v1(), now);
        let result = service
            .reserve(TEST_KEY, now, DATAGRAM_BYTES)
            .expect("initial burst")
            .settle_built(DATAGRAM_BYTES + 1);
        assert_eq!(result, Err(ReservationError));
        assert_conservation_snapshot(&service, BURST_BYTES, 0, 0, 1);
    }

    #[test]
    fn blocked_full_burst_cannot_bank_a_second_burst() {
        let start = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let key = test_key(7, 0);
        assert!(service.attach_connection(key));

        service
            .reserve(key, start, BURST_BYTES)
            .expect("initial full burst")
            .settle_built(BURST_BYTES)
            .expect("full burst settlement");

        let unblocked_at = start + Duration::from_secs(1);
        assert!(service.reserve(key, unblocked_at, 1).is_none());
        assert_conservation_snapshot(&service, 0, 0, BURST_BYTES, 1);

        service
            .note_socket_sent(key, BURST_BYTES, unblocked_at)
            .expect("blocked batch belongs to this connection");
        assert!(service.reserve(key, unblocked_at, 1).is_none());
        assert!(service
            .reserve(key, unblocked_at + Duration::from_millis(2), BURST_BYTES)
            .is_some());
    }

    #[test]
    fn abandonment_releases_only_the_named_connection_batch() {
        let start = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let first = test_key(1, 0);
        let second = test_key(2, 0);
        assert!(service.attach_connection(first));
        assert!(service.attach_connection(second));

        for key in [first, second] {
            service
                .reserve(key, start, BURST_BYTES / 2)
                .expect("each connection gets half the initial burst")
                .settle_built(BURST_BYTES / 2)
                .expect("half-burst settlement");
        }

        let abandoned_at = start + Duration::from_secs(1);
        service
            .note_socket_abandoned(first, BURST_BYTES / 2, abandoned_at)
            .expect("first connection owns this batch");
        assert_conservation_snapshot(&service, 0, 0, BURST_BYTES / 2, 2);
        assert!(service.reserve(first, abandoned_at, 1).is_none());
        assert!(service.reserve(second, abandoned_at, 1).is_none());
    }

    #[test]
    fn detach_cleans_outstanding_and_defers_live_reservation_cleanup() {
        let start = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let old_key = test_key(9, 0);
        assert!(service.attach_connection(old_key));

        let live = service
            .reserve(old_key, start, DATAGRAM_BYTES)
            .expect("initial live reservation");
        let detached = service
            .detach_connection(old_key, start)
            .expect("attached connection");
        assert_eq!(detached.abandoned_bytes, 0);
        assert_eq!(detached.live_reservation_bytes, DATAGRAM_BYTES);
        assert!(service.reserve(old_key, start, 1).is_none());
        assert!(!service.attach_connection(old_key));

        drop(live);
        assert_eq!(service.snapshot().connection_records, 0);
        assert!(service.attach_connection(old_key));

        service
            .reserve(old_key, start, DATAGRAM_BYTES)
            .expect("reattached connection")
            .settle_built(1_000)
            .expect("short datagram");
        let detached_at = start + Duration::from_secs(1);
        let detached = service
            .detach_connection(old_key, detached_at)
            .expect("reattached connection");
        assert_eq!(detached.abandoned_bytes, 1_000);
        assert_eq!(detached.live_reservation_bytes, 0);
        assert_eq!(service.snapshot().connection_records, 0);
        assert_eq!(
            service.note_socket_sent(old_key, 1_000, detached_at),
            Err(SocketOutcomeError)
        );

        let migrated_key = test_key(9, 1);
        assert!(service.attach_connection(migrated_key));
        assert!(service.reserve(migrated_key, detached_at, 1).is_some());
    }

    #[test]
    fn one_bulk_connection_borrows_the_full_endpoint_burst() {
        let now = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let key = test_key(21, 0);
        assert!(service.attach_connection(key));

        let mut granted = 0;
        while let Some(reservation) = try_reserve_bulk(&service, key, now, now, DATAGRAM_BYTES) {
            reservation
                .settle_built(DATAGRAM_BYTES)
                .expect("bulk datagram fits its reservation");
            service
                .note_socket_sent(key, DATAGRAM_BYTES, now)
                .expect("settled bulk datagram is outstanding");
            granted += DATAGRAM_BYTES;
        }

        assert_eq!(granted, BURST_BYTES);
    }

    #[test]
    fn two_continuous_bulk_waiters_stay_within_one_quantum_lead() {
        let start = Instant::now();
        let ready_at = start + Duration::from_millis(1);
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let first = test_key(31, 0);
        let second = test_key(32, 0);
        assert!(service.attach_connection(first));
        assert!(service.attach_connection(second));

        assert!(try_reserve_bulk(&service, first, start, ready_at, DATAGRAM_BYTES).is_none());
        assert!(try_reserve_bulk(&service, second, start, ready_at, DATAGRAM_BYTES).is_none());

        let mut first_granted = 0u64;
        let mut second_granted = 0u64;
        while let Some(reservation) =
            try_reserve_bulk(&service, first, ready_at, ready_at, DATAGRAM_BYTES)
        {
            reservation
                .settle_built(DATAGRAM_BYTES)
                .expect("first-turn datagram fits its reservation");
            service
                .note_socket_sent(first, DATAGRAM_BYTES, ready_at)
                .expect("first-turn datagram is outstanding");
            first_granted += DATAGRAM_BYTES;
        }
        assert_eq!(
            first_granted,
            EndpointPacingServiceConfig::endpoint_window_v1().connection_quantum_bytes()
        );

        while let Some(reservation) =
            try_reserve_bulk(&service, second, ready_at, ready_at, DATAGRAM_BYTES)
        {
            reservation
                .settle_built(DATAGRAM_BYTES)
                .expect("second-turn datagram fits its reservation");
            service
                .note_socket_sent(second, DATAGRAM_BYTES, ready_at)
                .expect("second-turn datagram is outstanding");
            second_granted += DATAGRAM_BYTES;
        }
        assert_eq!(
            second_granted,
            EndpointPacingServiceConfig::endpoint_window_v1().connection_quantum_bytes()
        );

        let mut attempts_without_progress = 0;
        while first_granted + second_granted < BURST_BYTES {
            let mut progressed = false;
            for key in [first, second] {
                if let Some(reservation) =
                    try_reserve_bulk(&service, key, ready_at, ready_at, DATAGRAM_BYTES)
                {
                    reservation
                        .settle_built(DATAGRAM_BYTES)
                        .expect("bulk datagram fits its reservation");
                    service
                        .note_socket_sent(key, DATAGRAM_BYTES, ready_at)
                        .expect("settled bulk datagram is outstanding");
                    if key == first {
                        first_granted += DATAGRAM_BYTES;
                    } else {
                        second_granted += DATAGRAM_BYTES;
                    }
                    progressed = true;

                    let lead = first_granted.abs_diff(second_granted);
                    assert!(
                        lead <= EndpointPacingServiceConfig::endpoint_window_v1()
                            .connection_quantum_bytes()
                            + DATAGRAM_BYTES,
                        "first={first_granted} second={second_granted} lead={lead}"
                    );
                }
            }
            attempts_without_progress = if progressed {
                0
            } else {
                attempts_without_progress + 1
            };
            assert!(attempts_without_progress < 3, "DRR made no progress");
        }
        let fairness = service.snapshot().fairness_lead_high_water_bytes;
        assert!(fairness > 0);
        assert!(
            fairness
                <= EndpointPacingServiceConfig::endpoint_window_v1().connection_quantum_bytes()
                    + DATAGRAM_BYTES,
            "observed fairness lead high water exceeded the formal bound: {fairness}"
        );
    }

    #[test]
    fn cancelling_an_idle_bulk_head_lends_its_turn_to_the_peer() {
        let start = Instant::now();
        let ready_at = start + Duration::from_millis(1);
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let idle = test_key(41, 0);
        let peer = test_key(42, 0);
        assert!(service.attach_connection(idle));
        assert!(service.attach_connection(peer));

        assert!(try_reserve_bulk(&service, idle, start, ready_at, DATAGRAM_BYTES).is_none());
        assert!(try_reserve_bulk(&service, peer, start, ready_at, DATAGRAM_BYTES).is_none());
        assert!(service.cancel_waiter(idle));

        assert!(try_reserve_bulk(&service, peer, start, start, DATAGRAM_BYTES).is_some());
    }

    #[test]
    fn idle_or_cancelled_bulk_connections_retain_no_deficit() {
        let start = Instant::now();
        let later = start + Duration::from_millis(1);
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let first = test_key(51, 0);
        let peer = test_key(52, 0);
        assert!(service.attach_connection(first));
        assert!(service.attach_connection(peer));

        drop(
            try_reserve_bulk(&service, first, start, start, DATAGRAM_BYTES)
                .expect("single active connection starts a turn"),
        );
        assert_ne!(
            service.lock_state().connections[&first].bulk_deficit_bytes,
            0
        );

        assert!(try_reserve_bulk(&service, peer, start, later, DATAGRAM_BYTES).is_none());
        assert_eq!(
            service.lock_state().connections[&first].bulk_deficit_bytes,
            0
        );

        assert!(service.cancel_waiter(peer));
        assert_eq!(
            service.lock_state().connections[&peer].bulk_deficit_bytes,
            0
        );
    }

    #[test]
    fn one_control_connection_borrows_the_full_endpoint_burst() {
        let now = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let key = test_key(61, 0);
        assert!(service.attach_connection(key));

        let mut granted = 0;
        while let Some(reservation) = try_reserve_control(&service, key, now, now, DATAGRAM_BYTES) {
            reservation
                .settle_built(DATAGRAM_BYTES)
                .expect("control datagram fits its reservation");
            service
                .note_socket_sent(key, DATAGRAM_BYTES, now)
                .expect("settled control datagram is outstanding");
            granted += DATAGRAM_BYTES;
        }

        assert_eq!(granted, BURST_BYTES);
    }

    #[test]
    fn continuous_control_and_bulk_rotate_after_their_bounded_quanta() {
        let start = Instant::now();
        let ready_at = start + Duration::from_millis(1);
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let control = test_key(71, 0);
        let bulk = test_key(72, 0);
        assert!(service.attach_connection(control));
        assert!(service.attach_connection(bulk));

        assert!(try_reserve_control(&service, control, start, ready_at, DATAGRAM_BYTES).is_none());
        assert!(try_reserve_bulk(&service, bulk, start, ready_at, DATAGRAM_BYTES).is_none());

        let control_first_turn =
            drain_scheduled_class(&service, control, PacingTrafficClass::Control, ready_at);
        assert_eq!(
            control_first_turn,
            EndpointPacingServiceConfig::endpoint_window_v1().control_reserve_bytes()
        );

        let bulk_first_turn =
            drain_scheduled_class(&service, bulk, PacingTrafficClass::Bulk, ready_at);
        assert_eq!(
            bulk_first_turn,
            EndpointPacingServiceConfig::endpoint_window_v1().connection_quantum_bytes()
        );

        let control_second_turn =
            drain_scheduled_class(&service, control, PacingTrafficClass::Control, ready_at);
        assert_eq!(
            control_second_turn,
            EndpointPacingServiceConfig::endpoint_window_v1().control_reserve_bytes()
        );
    }

    #[test]
    fn token_deadline_is_exact_and_combines_with_the_path_deadline() {
        let start = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let key = test_key(81, 0);
        assert!(service.attach_connection(key));
        assert_eq!(
            drain_scheduled_class(&service, key, PacingTrafficClass::Bulk, start),
            BURST_BYTES
        );

        let endpoint_deadline = blocked_deadline(service.poll_reserve(
            key,
            PacingTrafficClass::Bulk,
            start,
            DATAGRAM_BYTES,
            None,
            start,
        ));
        assert_eq!(
            endpoint_deadline,
            Some(start + Duration::from_nanos(41_667))
        );

        let path_ready_at = start + Duration::from_micros(100);
        let combined_deadline = blocked_deadline(service.poll_reserve(
            key,
            PacingTrafficClass::Bulk,
            path_ready_at,
            DATAGRAM_BYTES,
            None,
            start,
        ));
        assert_eq!(combined_deadline, Some(path_ready_at));
    }

    #[test]
    fn turn_handoff_wakes_only_the_newly_eligible_peer() {
        let start = Instant::now();
        let ready_at = start + Duration::from_millis(1);
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let first = test_key(91, 0);
        let second = test_key(92, 0);
        assert!(service.attach_connection(first));
        assert!(service.attach_connection(second));
        let first_counter = Arc::new(WakeCounter::default());
        let second_counter = Arc::new(WakeCounter::default());
        let first_waker = Waker::from(first_counter.clone());
        let second_waker = Waker::from(second_counter.clone());

        assert!(matches!(
            service.poll_reserve(
                first,
                PacingTrafficClass::Bulk,
                ready_at,
                DATAGRAM_BYTES,
                Some(&first_waker),
                start,
            ),
            PollReservation::Blocked { .. }
        ));
        assert!(matches!(
            service.poll_reserve(
                second,
                PacingTrafficClass::Bulk,
                ready_at,
                DATAGRAM_BYTES,
                Some(&second_waker),
                start,
            ),
            PollReservation::Blocked { .. }
        ));

        for _ in 0..(EndpointPacingServiceConfig::endpoint_window_v1().connection_quantum_bytes()
            / DATAGRAM_BYTES)
        {
            let reservation = match service.poll_reserve(
                first,
                PacingTrafficClass::Bulk,
                ready_at,
                DATAGRAM_BYTES,
                Some(&first_waker),
                ready_at,
            ) {
                PollReservation::Granted(reservation) => reservation,
                PollReservation::Blocked { reason, .. } => {
                    panic!("first turn blocked early: {reason:?}")
                }
            };
            reservation
                .settle_built(DATAGRAM_BYTES)
                .expect("turn datagram fits");
            service
                .note_socket_sent(first, DATAGRAM_BYTES, ready_at)
                .expect("turn datagram is outstanding");
        }

        assert!(matches!(
            service.poll_reserve(
                first,
                PacingTrafficClass::Bulk,
                ready_at,
                DATAGRAM_BYTES,
                Some(&first_waker),
                ready_at,
            ),
            PollReservation::Blocked {
                reason: ReservationBlockReason::Turn,
                ..
            }
        ));
        assert_eq!(first_counter.count.load(Ordering::SeqCst), 0);
        assert_eq!(second_counter.count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn socket_progress_wakes_a_parked_peer_before_control_yields() {
        let start = Instant::now();
        let ready_at = start + Duration::from_secs(1);
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let control = test_key(91, 0);
        let bulk = test_key(92, 0);
        assert!(service.attach_connection(control));
        assert!(service.attach_connection(bulk));

        let control_counter = Arc::new(WakeCounter::default());
        let bulk_counter = Arc::new(WakeCounter::default());
        let control_waker = Waker::from(control_counter.clone());
        let bulk_waker = Waker::from(bulk_counter.clone());
        assert!(matches!(
            service.poll_reserve(
                control,
                PacingTrafficClass::Control,
                ready_at,
                DATAGRAM_BYTES,
                Some(&control_waker),
                start,
            ),
            PollReservation::Blocked {
                reason: ReservationBlockReason::Path,
                ..
            }
        ));
        assert!(matches!(
            service.poll_reserve(
                bulk,
                PacingTrafficClass::Bulk,
                ready_at,
                DATAGRAM_BYTES,
                Some(&bulk_waker),
                start,
            ),
            PollReservation::Blocked {
                reason: ReservationBlockReason::Path,
                ..
            }
        ));

        assert!(matches!(
            service.poll_reserve(
                bulk,
                PacingTrafficClass::Bulk,
                ready_at,
                DATAGRAM_BYTES,
                Some(&bulk_waker),
                ready_at,
            ),
            PollReservation::Blocked {
                reason: ReservationBlockReason::Turn,
                deadline: None,
            }
        ));
        for _ in 0..(EndpointPacingServiceConfig::endpoint_window_v1().control_reserve_bytes()
            / DATAGRAM_BYTES)
        {
            settle_and_send_class(&service, control, PacingTrafficClass::Control, ready_at);
        }
        assert!(
            bulk_counter.count.load(Ordering::SeqCst) > 0,
            "the latest socket outcome must schedule the parked bulk driver"
        );
        let reservation = match service.poll_reserve(
            bulk,
            PacingTrafficClass::Bulk,
            ready_at,
            DATAGRAM_BYTES,
            Some(&bulk_waker),
            ready_at,
        ) {
            PollReservation::Granted(reservation) => reservation,
            PollReservation::Blocked { reason, .. } => {
                panic!("scheduled bulk peer did not make progress: {reason:?}")
            }
        };
        reservation
            .settle_built(DATAGRAM_BYTES)
            .expect("scheduled bulk datagram fits its reservation");
        assert_eq!(control_counter.count.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn cancellation_and_socket_outcome_wake_a_peer_once_without_self_wake() {
        let start = Instant::now();
        let outcome_at = start + Duration::from_secs(1);
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let blocker = test_key(101, 0);
        let cancelled = test_key(102, 0);
        let peer = test_key(103, 0);
        for key in [blocker, cancelled, peer] {
            assert!(service.attach_connection(key));
        }
        service
            .reserve(blocker, start, BURST_BYTES)
            .expect("blocker takes the full burst")
            .settle_built(BURST_BYTES)
            .expect("full burst settlement");

        let cancelled_counter = Arc::new(WakeCounter::default());
        let peer_counter = Arc::new(WakeCounter::default());
        let cancelled_waker = Waker::from(cancelled_counter.clone());
        let peer_waker = Waker::from(peer_counter.clone());
        assert!(matches!(
            service.poll_reserve(
                cancelled,
                PacingTrafficClass::Bulk,
                start,
                DATAGRAM_BYTES,
                Some(&cancelled_waker),
                start,
            ),
            PollReservation::Blocked {
                reason: ReservationBlockReason::Tokens,
                ..
            }
        ));
        for _ in 0..2 {
            assert!(matches!(
                service.poll_reserve(
                    peer,
                    PacingTrafficClass::Bulk,
                    start,
                    DATAGRAM_BYTES,
                    Some(&peer_waker),
                    start,
                ),
                PollReservation::Blocked { .. }
            ));
        }
        assert_eq!(cancelled_counter.count.load(Ordering::SeqCst), 0);
        assert_eq!(peer_counter.count.load(Ordering::SeqCst), 0);

        assert!(service.cancel_waiter(cancelled));
        assert_eq!(cancelled_counter.count.load(Ordering::SeqCst), 0);
        assert_eq!(peer_counter.count.load(Ordering::SeqCst), 1);

        assert!(matches!(
            service.poll_reserve(
                peer,
                PacingTrafficClass::Bulk,
                outcome_at,
                DATAGRAM_BYTES,
                Some(&peer_waker),
                outcome_at,
            ),
            PollReservation::Blocked { deadline: None, .. }
        ));
        service
            .note_socket_sent(blocker, BURST_BYTES, outcome_at)
            .expect("socket accepts the blocked batch");
        assert_eq!(peer_counter.count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn migration_keeps_old_outstanding_charged_until_its_socket_outcome() {
        let start = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let old = test_key(111, 0);
        let new = test_key(111, 1);
        assert!(service.attach_connection(old));
        service
            .reserve(old, start, DATAGRAM_BYTES)
            .expect("old path reserves a datagram")
            .settle_built(1_000)
            .expect("old path settles a short datagram");
        let wake_counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(wake_counter);
        assert!(matches!(
            service.poll_reserve(
                old,
                PacingTrafficClass::Bulk,
                start + Duration::from_secs(1),
                DATAGRAM_BYTES,
                Some(&waker),
                start,
            ),
            PollReservation::Blocked {
                reason: ReservationBlockReason::Path,
                ..
            }
        ));
        let before = service.snapshot();

        service
            .migrate_connection(old, new)
            .expect("new path generation is vacant");
        let migrated = service.snapshot();
        assert_eq!(migrated.available_tokens, before.available_tokens);
        assert_eq!(migrated.outstanding_bytes, 1_000);
        assert_eq!(migrated.connection_records, 2);
        assert_eq!(migrated.stale_waker_events, 1);
        assert!(service.reserve(old, start, 1).is_none());
        assert!(service.reserve(new, start, 1).is_some());

        service
            .note_socket_sent(old, 1_000, start)
            .expect("old path socket batch remains attributable");
        assert_eq!(service.snapshot().connection_records, 1);
        assert_eq!(
            service.note_socket_sent(old, 1_000, start),
            Err(SocketOutcomeError)
        );
    }

    #[test]
    fn connection_adapter_keeps_one_batch_attributed_across_migration_and_detach() {
        let start = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let old = test_key(121, 0);
        let mut connection = ConnectionEndpointPacing::attach(service.clone(), old);

        let reservation =
            match connection.poll_reserve(PacingTrafficClass::Bulk, start, DATAGRAM_BYTES, start) {
                PollReservation::Granted(reservation) => reservation,
                PollReservation::Blocked { reason, .. } => {
                    panic!("initial grant blocked: {reason:?}")
                }
            };
        connection
            .settle_datagram(reservation, 1_000)
            .expect("first datagram settles into the socket batch");
        assert_eq!(connection.pending_socket_batch_bytes(), 1_000);
        assert!(connection.expose_socket_batch());
        assert!(matches!(
            connection.poll_reserve(PacingTrafficClass::Bulk, start, DATAGRAM_BYTES, start,),
            PollReservation::Blocked {
                reason: ReservationBlockReason::SocketOutstanding,
                ..
            }
        ));

        connection
            .migrate(1)
            .expect("new generation attaches without resetting the bucket");
        assert_eq!(service.snapshot().connection_records, 2);
        assert!(connection
            .note_socket_sent(start)
            .expect("old generation socket outcome is attributable"));
        assert_eq!(service.snapshot().connection_records, 1);

        let reservation = match connection.poll_reserve(
            PacingTrafficClass::Control,
            start,
            DATAGRAM_BYTES,
            start,
        ) {
            PollReservation::Granted(reservation) => reservation,
            PollReservation::Blocked { reason, .. } => panic!("new path grant blocked: {reason:?}"),
        };
        connection
            .settle_datagram(reservation, 900)
            .expect("new generation datagram settles");
        assert!(connection.expose_socket_batch());
        let detached = connection.detach(start).expect("live connection detaches");
        assert_eq!(detached.abandoned_bytes, 900);
        assert_eq!(connection.pending_socket_batch_bytes(), 0);
        assert_eq!(service.snapshot().connection_records, 0);
        assert!(!connection
            .note_socket_sent(start)
            .expect("no pending batch is a no-op"));
    }

    #[test]
    fn rejected_migration_keeps_the_old_adapter_attached_and_paced() {
        let start = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let old = test_key(122, 0);
        let occupied = test_key(122, 1);
        let mut connection = ConnectionEndpointPacing::attach(service.clone(), old);
        assert!(service.attach_connection(occupied));

        assert_eq!(connection.migrate(1), Err(MigrationError));
        assert_eq!(
            connection
                .snapshot()
                .expect("failed migration must retain the old adapter")
                .path_generation,
            0
        );
        let reservation =
            match connection.poll_reserve(PacingTrafficClass::Bulk, start, DATAGRAM_BYTES, start) {
                PollReservation::Granted(reservation) => reservation,
                PollReservation::Blocked { reason, .. } => {
                    panic!("old adapter must remain paced after migration rejection: {reason:?}")
                }
            };
        connection
            .settle_datagram(reservation, DATAGRAM_BYTES)
            .expect("old adapter reservation still settles");
        assert!(connection.expose_socket_batch());
        assert!(connection
            .note_socket_sent(start)
            .expect("old adapter socket outcome remains attributable"));
        assert_eq!(service.snapshot().sent_bytes, DATAGRAM_BYTES);
    }

    #[test]
    fn stateless_responses_require_immediate_tokens_and_report_socket_outcomes() {
        let start = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());

        let full_burst = service
            .reserve_stateless_response(start, BURST_BYTES)
            .expect("full initial burst must be immediately available");
        assert_eq!(service.snapshot().outstanding_bytes, BURST_BYTES);
        assert!(service.reserve_stateless_response(start, 1).is_none());
        assert_eq!(service.snapshot().stateless_responses_dropped, 1);

        full_burst.note_sent(start);
        let sent = service.snapshot();
        assert_eq!(sent.outstanding_bytes, 0);
        assert_eq!(sent.stateless_responses_sent, 1);

        let later = start + Duration::from_millis(2);
        let abandoned = service
            .reserve_stateless_response(later, DATAGRAM_BYTES)
            .expect("elapsed time must replenish stateless service");
        abandoned.note_abandoned(later);
        let final_snapshot = service.snapshot();
        assert_eq!(final_snapshot.outstanding_bytes, 0);
        assert_eq!(final_snapshot.stateless_responses_sent, 1);
        assert_eq!(final_snapshot.stateless_responses_dropped, 2);
    }

    #[test]
    fn snapshot_reports_config_service_and_lifecycle_counters() {
        let start = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let key = test_key(122, 0);
        let mut connection = ConnectionEndpointPacing::attach(service.clone(), key);

        let reservation =
            match connection.poll_reserve(PacingTrafficClass::Bulk, start, BURST_BYTES, start) {
                PollReservation::Granted(reservation) => reservation,
                PollReservation::Blocked { reason, .. } => {
                    panic!("initial grant blocked: {reason:?}")
                }
            };
        connection
            .settle_datagram(reservation, 60_000)
            .expect("short full-burst datagram settles");
        assert!(connection.expose_socket_batch());
        assert!(connection.note_socket_sent(start).unwrap());

        assert!(matches!(
            connection.poll_reserve(PacingTrafficClass::Bulk, start, 2_000, start),
            PollReservation::Blocked {
                reason: ReservationBlockReason::Tokens,
                ..
            }
        ));
        assert!(connection.cancel_waiter());
        connection.detach(start).expect("connection detaches");

        let snapshot = service.snapshot();
        assert_eq!(snapshot.rate_bytes_per_second, RATE_BYTES_PER_SECOND);
        assert_eq!(snapshot.burst_bytes, BURST_BYTES);
        assert_eq!(snapshot.control_reserve_bytes, 10_240);
        assert_eq!(snapshot.connection_quantum_bytes, 20_480);
        assert_eq!(snapshot.granted_bytes, BURST_BYTES);
        assert_eq!(snapshot.granted_datagrams, 1);
        assert_eq!(snapshot.refunded_bytes, BURST_BYTES - 60_000);
        assert_eq!(snapshot.refund_events, 1);
        assert_eq!(snapshot.sent_bytes, 60_000);
        assert_eq!(snapshot.sent_datagrams, 1);
        assert_eq!(snapshot.abandoned_bytes, 0);
        assert_eq!(snapshot.abandoned_datagrams, 0);
        assert_eq!(snapshot.outstanding_bytes_high_water, 60_000);
        assert_eq!(snapshot.endpoint_delay_events, 1);
        assert!(snapshot.max_endpoint_delay_nanos > 0);
        assert_eq!(snapshot.waiter_high_water, 1);
        assert_eq!(snapshot.control_waiters, 0);
        assert_eq!(snapshot.bulk_waiters, 0);
        assert_eq!(snapshot.cancellations, 1);
        assert_eq!(snapshot.detaches, 1);
    }

    #[test]
    fn connection_snapshot_preserves_service_counters_across_migration() {
        let start = Instant::now();
        let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
        let key = test_key(123, 0);
        let mut connection = ConnectionEndpointPacing::attach(service, key);

        let bulk = match connection.poll_reserve(PacingTrafficClass::Bulk, start, 1_000, start) {
            PollReservation::Granted(reservation) => reservation,
            PollReservation::Blocked { reason, .. } => panic!("bulk grant blocked: {reason:?}"),
        };
        connection.settle_datagram(bulk, 1_000).unwrap();
        assert!(connection.expose_socket_batch());
        assert!(connection.note_socket_sent(start).unwrap());

        let later = start + Duration::from_millis(1);
        let control = match connection.poll_reserve(PacingTrafficClass::Control, later, 500, later)
        {
            PollReservation::Granted(reservation) => reservation,
            PollReservation::Blocked { reason, .. } => {
                panic!("control grant blocked: {reason:?}")
            }
        };
        connection.settle_datagram(control, 500).unwrap();
        assert!(connection.expose_socket_batch());
        assert!(connection.note_socket_sent(later).unwrap());

        let before = connection.snapshot().expect("attached connection snapshot");
        assert_eq!(before.connection_handle, 123);
        assert_eq!(before.path_generation, 0);
        assert!(before.attached);
        assert_eq!(before.granted_bulk_bytes, 1_000);
        assert_eq!(before.granted_control_bytes, 500);
        assert!(before.turn_count >= 2);
        assert!(before.wait_count >= 2);
        assert!(before.max_service_gap_nanos >= 1_000_000);

        connection.migrate(1).expect("migration succeeds");
        let migrated = connection.snapshot().expect("migrated connection snapshot");
        assert_eq!(migrated.connection_handle, 123);
        assert_eq!(migrated.path_generation, 1);
        assert_eq!(migrated.granted_bulk_bytes, before.granted_bulk_bytes);
        assert_eq!(migrated.granted_control_bytes, before.granted_control_bytes);
        assert_eq!(migrated.turn_count, before.turn_count);
        assert_eq!(migrated.wait_count, before.wait_count);
        assert_eq!(migrated.max_service_gap_nanos, before.max_service_gap_nanos);
    }

    #[derive(Debug, Default)]
    struct WakeCounter {
        count: AtomicUsize,
    }

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn assert_conservation_snapshot(
        service: &EndpointPacingService,
        available_tokens: u64,
        live_reservation_bytes: u64,
        outstanding_bytes: u64,
        connection_records: usize,
    ) {
        let snapshot = service.snapshot();
        assert_eq!(snapshot.available_tokens, available_tokens);
        assert_eq!(snapshot.live_reservation_bytes, live_reservation_bytes);
        assert_eq!(snapshot.outstanding_bytes, outstanding_bytes);
        assert_eq!(snapshot.connection_records, connection_records);
    }

    fn drain_available(service: &EndpointPacingService, now: Instant) -> u64 {
        drain_available_bytes(service, now, DATAGRAM_BYTES)
    }

    fn drain_available_bytes(
        service: &EndpointPacingService,
        now: Instant,
        datagram_bytes: u64,
    ) -> u64 {
        let mut sent = 0;
        while let Some(reservation) = service.reserve(TEST_KEY, now, datagram_bytes) {
            reservation
                .settle_built(datagram_bytes)
                .expect("actual bytes do not exceed the reservation");
            service
                .note_socket_sent(TEST_KEY, datagram_bytes, now)
                .expect("the settled datagram is outstanding");
            sent += datagram_bytes;
        }
        sent
    }

    fn settle_and_send_one(service: &EndpointPacingService, now: Instant) {
        service
            .reserve(TEST_KEY, now, 1)
            .expect("one byte is available")
            .settle_built(1)
            .expect("one-byte settlement");
        service
            .note_socket_sent(TEST_KEY, 1, now)
            .expect("one byte is outstanding");
    }

    fn settle_and_send_class(
        service: &EndpointPacingService,
        key: EndpointPacingConnectionKey,
        class: PacingTrafficClass,
        now: Instant,
    ) {
        let reservation = match service.poll_reserve(key, class, now, DATAGRAM_BYTES, None, now) {
            PollReservation::Granted(reservation) => reservation,
            PollReservation::Blocked { reason, .. } => {
                panic!("class service unexpectedly blocked: {reason:?}")
            }
        };
        reservation
            .settle_built(DATAGRAM_BYTES)
            .expect("class reservation settles exactly");
        service
            .note_socket_sent(key, DATAGRAM_BYTES, now)
            .expect("class datagram socket outcome matches");
    }

    fn try_reserve_bulk(
        service: &EndpointPacingService,
        key: EndpointPacingConnectionKey,
        now: Instant,
        path_ready_at: Instant,
        planned_bytes: u64,
    ) -> Option<DatagramReservation> {
        match service.poll_reserve(
            key,
            PacingTrafficClass::Bulk,
            path_ready_at,
            planned_bytes,
            None,
            now,
        ) {
            PollReservation::Granted(reservation) => Some(reservation),
            PollReservation::Blocked { .. } => None,
        }
    }

    fn try_reserve_control(
        service: &EndpointPacingService,
        key: EndpointPacingConnectionKey,
        now: Instant,
        path_ready_at: Instant,
        planned_bytes: u64,
    ) -> Option<DatagramReservation> {
        match service.poll_reserve(
            key,
            PacingTrafficClass::Control,
            path_ready_at,
            planned_bytes,
            None,
            now,
        ) {
            PollReservation::Granted(reservation) => Some(reservation),
            PollReservation::Blocked { .. } => None,
        }
    }

    fn drain_scheduled_class(
        service: &EndpointPacingService,
        key: EndpointPacingConnectionKey,
        class: PacingTrafficClass,
        now: Instant,
    ) -> u64 {
        let mut granted = 0;
        loop {
            let reservation = match class {
                PacingTrafficClass::Control => {
                    try_reserve_control(service, key, now, now, DATAGRAM_BYTES)
                }
                PacingTrafficClass::Bulk => {
                    try_reserve_bulk(service, key, now, now, DATAGRAM_BYTES)
                }
            };
            let Some(reservation) = reservation else {
                break;
            };
            reservation
                .settle_built(DATAGRAM_BYTES)
                .expect("scheduled datagram fits its reservation");
            service
                .note_socket_sent(key, DATAGRAM_BYTES, now)
                .expect("scheduled datagram is outstanding");
            granted += DATAGRAM_BYTES;
        }
        granted
    }

    fn blocked_deadline(result: PollReservation) -> Option<Instant> {
        match result {
            PollReservation::Granted(_) => panic!("expected reservation to be blocked"),
            PollReservation::Blocked { deadline, .. } => deadline,
        }
    }

    fn test_config(rate_bytes_per_second: u64, burst_bytes: u64) -> EndpointPacingServiceConfig {
        EndpointPacingServiceConfig::new(rate_bytes_per_second, burst_bytes, 0, burst_bytes)
            .expect("nonzero test config")
    }

    fn test_service(config: EndpointPacingServiceConfig, _now: Instant) -> EndpointPacingService {
        let service = EndpointPacingService::new(config);
        assert!(service.attach_connection(TEST_KEY));
        service
    }

    fn test_key(handle: usize, path_generation: u64) -> EndpointPacingConnectionKey {
        EndpointPacingConnectionKey::new(ConnectionHandle(handle), path_generation)
    }
}

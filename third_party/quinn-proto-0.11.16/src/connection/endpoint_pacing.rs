use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
    task::Waker,
};

use rustc_hash::FxHashMap;

use crate::{EndpointPacingServiceConfig, Instant};

const NANOS_PER_SECOND: u128 = 1_000_000_000;

#[derive(Clone, Debug)]
pub(crate) struct EndpointPacingService(Arc<EndpointPacingServiceInner>);

mod authority;
mod identity;
mod inventory;
use inventory::{Records, StateStorage, WaitQueue};
mod prepared;
pub(crate) use identity::EndpointPacingConnectionKey;

#[derive(Debug)]
struct EndpointPacingServiceInner {
    config: EndpointPacingServiceConfig,
    state: Mutex<StateStorage>,
    meter: Option<bytes::BackingMeter>,
    failed: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
struct EndpointPacingState {
    retired_wakers: [Option<Waker>; 2],
    callback_overflow: bool,
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
    connections: Records,
    control_waiters: WaitQueue,
    control_turn: Option<EndpointPacingConnectionKey>,
    control_yielding_to_bulk: bool,
    bulk_service_before_control_bytes: u64,
    bulk_waiters: WaitQueue,
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
        let waiter = connection.waiter.take();
        let removed = waiter.is_some();
        let retired = waiter.and_then(|waiter| waiter.waker);
        connection.control_deficit_bytes = 0;
        connection.bulk_deficit_bytes = 0;
        self.retire_waker(retired);
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
        waker: Option<Waker>,
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
            let (replaced, retired) = replace_waker(&mut waiter.waker, waker);
            self.retire_waker(retired);
            if replaced {
                self.stale_waker_events = self.stale_waker_events.saturating_add(1);
            }
            return;
        }
        connection.waiter = Some(PacingWaiter {
            class: PacingTrafficClass::Bulk,
            path_ready_at,
            planned_bytes,
            waker,
        });
        connection.service_counters.wait_count =
            connection.service_counters.wait_count.saturating_add(1);
        if !self.bulk_waiters.push_back(key) {
            self.callback_overflow = true;
        }
        self.update_waiter_high_water();
    }

    fn register_control_waiter(
        &mut self,
        key: EndpointPacingConnectionKey,
        path_ready_at: Instant,
        planned_bytes: u64,
        waker: Option<Waker>,
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
            let (replaced, retired) = replace_waker(&mut waiter.waker, waker);
            self.retire_waker(retired);
            if replaced {
                self.stale_waker_events = self.stale_waker_events.saturating_add(1);
            }
            return;
        }
        connection.waiter = Some(PacingWaiter {
            class: PacingTrafficClass::Control,
            path_ready_at,
            planned_bytes,
            waker,
        });
        connection.service_counters.wait_count =
            connection.service_counters.wait_count.saturating_add(1);
        if !self.control_waiters.push_back(key) {
            self.callback_overflow = true;
        }
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

fn replace_waker(stored: &mut Option<Waker>, current: Option<Waker>) -> (bool, Option<Waker>) {
    let Some(current) = current else {
        return (false, None);
    };
    if stored.as_ref().is_some_and(|old| old.will_wake(&current)) {
        return (false, Some(current));
    }
    let old = stored.replace(current);
    (old.is_some(), old)
}

impl EndpointPacingService {
    pub(crate) fn new(config: EndpointPacingServiceConfig) -> Self {
        Self::prepare(config, None).expect("unbudgeted pacing state allocation")
    }
    pub(crate) const fn native_layout_bytes() -> usize {
        StateStorage::layout_bytes()
    }
    pub(crate) fn prepare(
        config: EndpointPacingServiceConfig,
        meter: Option<&bytes::BackingMeter>,
    ) -> std::io::Result<Self> {
        let native = meter.is_some();
        let state = EndpointPacingState {
            retired_wakers: [None, None],
            callback_overflow: false,
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
            connections: Records::new(native),
            control_waiters: WaitQueue::new(native),
            control_turn: None,
            control_yielding_to_bulk: false,
            bulk_service_before_control_bytes: 0,
            bulk_waiters: WaitQueue::new(native),
            bulk_turn: None,
        };
        let service = Self(Arc::new(EndpointPacingServiceInner {
            config,
            state: Mutex::new(StateStorage::new(state, meter)?),
            meter: meter.cloned(),
            failed: std::sync::atomic::AtomicBool::new(false),
        }));
        // Some platforms initialize the existing mutex's opaque OS storage on
        // first lock. Do that at cold construction, before publishing the service.
        drop(service.lock_state());
        Ok(service)
    }

    pub(crate) fn attach_connection(&self, key: EndpointPacingConnectionKey) -> bool {
        let mut state = self.lock_state();
        if !state.validate_control() {
            return false;
        }
        state
            .connections
            .insert(key, ConnectionAccounting::attached())
    }

    #[cfg(test)]
    pub(crate) fn migrate_connection(
        &self,
        old_key: EndpointPacingConnectionKey,
        new_key: EndpointPacingConnectionKey,
    ) -> Result<(), MigrationError> {
        self.migrate_deferred(old_key, new_key)?.finish();
        Ok(())
    }

    fn migrate_deferred(
        &self,
        old_key: EndpointPacingConnectionKey,
        new_key: EndpointPacingConnectionKey,
    ) -> Result<authority::MigrationEffects, MigrationError> {
        let mut state = self.lock_state();
        if !state.validate_control() {
            return Err(MigrationError);
        }
        if old_key == new_key
            || state.connections.contains_key(&new_key)
            || !state
                .connections
                .get(&old_key)
                .is_some_and(|connection| connection.attached)
        {
            return Err(MigrationError);
        }

        if !state.connections.has_capacity()
            && state
                .connections
                .get(&old_key)
                .is_some_and(|old| old.live_reservation_bytes != 0 || old.outstanding_bytes != 0)
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
        let effects = authority::MigrationEffects::new(&self.0, &mut state, wake);
        drop(state);
        Ok(effects)
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
        if state.unavailable() {
            return None;
        }
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

        if self.0.unavailable() {
            return PollReservation::Blocked {
                deadline: None,
                reason: ReservationBlockReason::Detached,
            };
        }
        let mut waker = self.0.prepare_waker(waker);
        let mut state = self.lock_state();
        if state.unavailable() {
            return PollReservation::Blocked {
                deadline: None,
                reason: ReservationBlockReason::Detached,
            };
        }
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
            state.register_control_waiter(key, path_ready_at, planned_bytes, waker.take());
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
            let retired = state.connections.get_mut(&key).and_then(|connection| {
                connection.control_deficit_bytes -= planned_bytes;
                connection.waiter.take().and_then(|waiter| waiter.waker)
            });
            state.retire_waker(retired);
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

        state.register_bulk_waiter(key, path_ready_at, planned_bytes, waker.take());
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
                let wake =
                    self.0
                        .hold_waker(state.take_turn_waker(PacingTrafficClass::Bulk, key, now));
                drop(state);
                wake.wake();
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
        let retired = state.connections.get_mut(&key).and_then(|connection| {
            connection.bulk_deficit_bytes -= planned_bytes;
            connection.waiter.take().and_then(|waiter| waiter.waker)
        });
        state.retire_waker(retired);
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
        if !state.validate_control() {
            return false;
        }
        let removed = state.remove_waiter(key);
        if removed {
            state.cancellations = state.cancellations.saturating_add(1);
        }
        let wake = self
            .0
            .hold_waker(removed.then(|| state.take_next_waiter_waker(key)).flatten());
        drop(state);
        wake.wake();
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
        if state.unavailable() {
            return None;
        }
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
        let mut state = service.lock();
        if state.unavailable() {
            return false;
        }
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
        let wake = service.hold_waker(state.take_any_waiter_waker());
        drop(state);
        wake.wake();
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
        if state.unavailable() {
            return Err(SocketOutcomeError);
        }
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
        if state.unavailable() {
            return Err(SocketOutcomeError);
        }
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
        let wake = self.0.hold_waker(state.take_next_waiter_waker(key));
        drop(state);
        wake.wake();
        Ok(())
    }

    pub(crate) fn detach_connection(
        &self,
        key: EndpointPacingConnectionKey,
        now: Instant,
    ) -> Result<DetachedConnectionAccounting, DetachError> {
        let mut state = self.lock_state();
        if !state.validate_control() {
            return Err(DetachError);
        }
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

    fn lock_state(&self) -> authority::Guard<'_> {
        self.0.lock()
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
    #[cfg(test)]
    pub(crate) fn attach(service: EndpointPacingService, key: EndpointPacingConnectionKey) -> Self {
        Self::try_attach(service, key).expect("test pacing attachment")
    }
    pub(crate) fn try_attach(
        service: EndpointPacingService,
        key: EndpointPacingConnectionKey,
    ) -> Option<Self> {
        if !service.attach_connection(key) {
            return None;
        }
        Some(Self {
            service,
            key,
            driver_waker: None,
            pending_socket_batch: None,
            socket_batch_exposed: false,
        })
    }

    pub(crate) fn is_native(&self) -> bool {
        self.service.0.meter.is_some()
    }

    pub(crate) fn begin_transmit_poll(&self) -> bool {
        !self.socket_batch_exposed && !self.service.0.unavailable()
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
        if self.service.0.unavailable()
            || self
                .driver_waker
                .as_ref()
                .is_some_and(|old| old.will_wake(waker))
        {
            return;
        }
        let prepared = self.service.0.opaque(|| waker.clone());
        let old = self.driver_waker.replace(prepared);
        self.service.0.dispose_wakers([old]);
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

    #[cfg(test)]
    pub(crate) fn migrate(&mut self, path_generation: u64) -> Result<(), MigrationError> {
        self.migrate_deferred(path_generation)?.finish();
        Ok(())
    }

    pub(crate) fn migrate_deferred(
        &mut self,
        path_generation: u64,
    ) -> Result<authority::MigrationEffects, MigrationError> {
        let new_key = self.key.with_path(path_generation);
        let effects = self.service.migrate_deferred(self.key, new_key)?;
        self.key = new_key;
        Ok(effects)
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
        self.socket_batch_exposed = false;
        self.service.0.dispose_wakers([self.driver_waker.take()]);
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
        if !settled {
            self.service.quarantine();
        }
        self.settled = true;
    }
}

impl Drop for StatelessResponseReservation {
    fn drop(&mut self) {
        if std::thread::panicking() || self.service.unavailable() {
            self.service.quarantine();
            return;
        }
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

        let mut state = self.service.lock();
        if state.unavailable() {
            return Err(ReservationError);
        }
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

        if std::thread::panicking() || self.service.unavailable() {
            self.service.quarantine();
            return;
        }
        let mut state = self.service.lock();
        if state.unavailable() {
            return;
        }
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
                self.service.quarantine();
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
mod tests;

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
            PacingTrafficClass::Bulk => try_reserve_bulk(service, key, now, now, DATAGRAM_BYTES),
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

mod lifecycle;
mod scheduling;
mod tokens;

mod authority;
mod inventory;
mod migration;

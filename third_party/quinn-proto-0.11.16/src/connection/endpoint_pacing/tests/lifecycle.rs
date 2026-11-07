use super::*;

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


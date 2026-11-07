use super::*;

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


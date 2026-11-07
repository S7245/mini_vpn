use super::*;

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
        service.lock_state().connections.get(&first).unwrap().bulk_deficit_bytes,
        0
    );

    assert!(try_reserve_bulk(&service, peer, start, later, DATAGRAM_BYTES).is_none());
    assert_eq!(
        service.lock_state().connections.get(&first).unwrap().bulk_deficit_bytes,
        0
    );

    assert!(service.cancel_waiter(peer));
    assert_eq!(
        service.lock_state().connections.get(&peer).unwrap().bulk_deficit_bytes,
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


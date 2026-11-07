use super::*;

#[test]
fn datagram_and_stream_full_rejects_the_whole_packet_before_any_publication() {
    recovery(true, PacketShape::StreamAndDatagram);
}

#[test]
fn refused_mixed_datagram_is_lost_but_actual_peer_recovers_stream() {
    recovery(false, PacketShape::StreamAndDatagram);
}
#[test]
fn many_zero_datagrams_and_stream_prepare_all_descriptors_and_events() {
    recovery(true, PacketShape::StreamAndManyDatagrams);
}
#[test]
fn repeated_mixed_datagrams_full_preserves_original_deadline() {
    deadline(PacketShape::StreamAndManyDatagrams);
}

#[test]
fn application_event_full_refuses_successor_before_adoption_and_reserved_close_completes() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    let mut pair = Pair::new(Arc::new(config), server_config());
    let (client, server) = control_records::connect(&mut pair);
    // Four actual DatagramReceived events fill the original handshake event
    // layout. Each empty datagram is dequeued before the next arrives so the
    // queue is empty again (D16: an entry costs at least one window byte, so
    // "readable" notifies only on the empty -> non-empty edge).
    for _ in 0..4 {
        pair.server_datagrams(server)
            .send(Bytes::new(), false)
            .unwrap();
        assert!(!pair.drive_bounded());
        assert_eq!(
            pair.client_datagrams(client).recv().as_deref(),
            Some(b"".as_slice())
        );
    }
    let before = pair
        .client_conn_mut(client)
        .packet_admission_snapshot_for_probe();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert!(matches!(
        pair.client_conn_mut(client).start_successor_service_turn(),
        Err(SuccessorServiceTurnStartError::Admission(
            bytes::MeteredCopyError::BudgetFull
        ))
    ));
    assert_eq!(
        pair.client_conn_mut(client)
            .packet_admission_snapshot_for_probe(),
        before
    );
    assert!(pair
        .client_conn_mut(client)
        .successor_service_turn_stats()
        .is_none());
    assert!(!budget.quarantined());
    drop(pressure);
    for _ in 0..4 {
        assert!(matches!(
            pair.client_conn_mut(client).poll(),
            Some(Event::DatagramReceived)
        ));
    }
    assert!(pair.client_datagrams(client).recv().is_none());
    assert!(pair.client_conn_mut(client).poll().is_none());
    pair.client_conn_mut(client)
        .start_successor_service_turn()
        .unwrap();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let now = pair.time;
    pair.client_conn_mut(client)
        .close(now, 0u32.into(), Bytes::new());
    assert!(matches!(
        pair.client_conn_mut(client).poll(),
        Some(Event::SuccessorServiceTurn {
            outcome: SuccessorServiceTurnOutcome::Failed {
                reason: SuccessorServiceTurnFailure::ConnectionClosed,
                ..
            }
        })
    ));
    assert!(pair
        .client_conn_mut(client)
        .successor_service_turn_stats()
        .is_none());
    assert!(!budget.quarantined());
    drop(pressure);
    control_records::stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

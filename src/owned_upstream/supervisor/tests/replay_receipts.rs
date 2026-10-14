//! Original Supervisor DATA, quarantine and ACK ownership characterizations.
//!
//! Uses the real reducer/factory/receipt path; does not own production policy.
//! Original helpers remain in the parent test Module.

use super::*;

#[test]
fn data_from_unbound_test_factory_is_quarantined_before_reducer_mutation() {
    let leg = committed_leg();
    let model = SessionModel::new(SessionRole::Client, session_config(), leg);
    let mut supervisor = SessionSupervisor::new(model);
    let local_flow = opened_flow(&mut supervisor, leg);
    let raw = ResumableTcpPortFactory::new_unbound_for_test(
        FlowPortConfig::new(8, 2, 2, 2).unwrap(),
        64,
        8,
        8,
    )
    .unwrap();
    let (flow, mut driver) = raw.open_flow(local_flow).unwrap();
    flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .unwrap();
    let before = supervisor.snapshot().session;

    assert!(matches!(
        supervisor.apply_command(SessionOwnerCommand::Driver(
            driver.try_recv_next().unwrap().unwrap(),
        )),
        Err(SessionSupervisorError::UplinkFactoryProvenanceMismatch)
    ));
    let poisoned = supervisor.snapshot();
    assert_eq!(poisoned.session, before);
    assert!(poisoned.poisoned);
    assert_eq!(poisoned.uplink_replay_extents, 0);
    assert_eq!(poisoned.quarantined_uplink_extents, 1);
    assert_eq!(poisoned.quarantined_uplink_owned_bytes, 1);
    assert_eq!(raw.session_uplink_owned_bytes(), 1);
    assert_eq!(raw.session_uplink_owned_segments(), 1);
    assert_eq!(
        supervisor
            .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
            .unwrap_err(),
        TcpPortFactoryMintError::PoisonedSupervisor
    );

    drop(supervisor);
    assert_eq!(raw.session_uplink_owned_bytes(), 0);
    assert_eq!(raw.session_uplink_owned_segments(), 0);
}

#[test]
fn correctly_provenanced_replay_segment_overrun_is_quarantined_and_poisons() {
    let leg = committed_leg();
    let config = SessionConfig::new(
        2,
        2,
        TcpWindowLimits::new(8, 2).unwrap(),
        TcpWindowLimits::new(8, 2).unwrap(),
        ReplayBudgetLimits::new(8, 1).unwrap(),
        ReplayBudgetLimits::new(8, 1).unwrap(),
        ReceiveBudgetLimits::new(16, 4).unwrap(),
        8,
    )
    .unwrap();
    let model = SessionModel::new(SessionRole::Client, config, leg);
    let mut supervisor = SessionSupervisor::new(model);
    let first_factory = supervisor
        .mint_tcp_port_factory(FlowPortConfig::new(8, 2, 2, 2).unwrap())
        .unwrap();
    let second_factory = first_factory
        .fork_with_independent_ledgers_for_test()
        .unwrap();
    let first_flow = opened_flow(&mut supervisor, leg);
    let second_flow = opened_flow(&mut supervisor, leg);
    let (first, mut first_driver) = first_factory.open_flow(first_flow).unwrap();
    let (second, mut second_driver) = second_factory.open_flow(second_flow).unwrap();

    first
        .try_send_uplink_with(1, || Bytes::from_static(b"a"))
        .unwrap();
    supervisor
        .apply_command(SessionOwnerCommand::Driver(
            first_driver.try_recv_next().unwrap().unwrap(),
        ))
        .unwrap();
    second
        .try_send_uplink_with(1, || Bytes::from_static(b"b"))
        .unwrap();

    assert!(matches!(
        supervisor.apply_command(SessionOwnerCommand::Driver(
            second_driver.try_recv_next().unwrap().unwrap(),
        )),
        Err(SessionSupervisorError::ReducerReplayCapacityInvariant {
            source: SessionError::ReplaySegmentBudgetExceeded {
                direction: Direction::ClientToTarget,
                attempted: 2,
                max: 1,
            }
        })
    ));
    let poisoned = supervisor.snapshot();
    assert!(poisoned.poisoned);
    assert_eq!(poisoned.uplink_replay_extents, 1);
    assert_eq!(poisoned.uplink_replay_owned_bytes, 1);
    assert_eq!(poisoned.quarantined_uplink_extents, 1);
    assert_eq!(poisoned.quarantined_uplink_owned_bytes, 1);
    assert_eq!(first_factory.session_uplink_owned_segments(), 1);
    assert_eq!(second_factory.session_uplink_owned_segments(), 1);

    drop(supervisor);
    assert_eq!(first_factory.session_uplink_owned_segments(), 0);
    assert_eq!(second_factory.session_uplink_owned_segments(), 0);
}

#[test]
fn correctly_provenanced_replay_byte_overrun_is_quarantined_and_poisons() {
    let leg = committed_leg();
    let config = SessionConfig::new(
        2,
        2,
        TcpWindowLimits::new(8, 2).unwrap(),
        TcpWindowLimits::new(8, 2).unwrap(),
        ReplayBudgetLimits::new(1, 2).unwrap(),
        ReplayBudgetLimits::new(1, 2).unwrap(),
        ReceiveBudgetLimits::new(16, 4).unwrap(),
        8,
    )
    .unwrap();
    let model = SessionModel::new(SessionRole::Client, config, leg);
    let mut supervisor = SessionSupervisor::new(model);
    let first_factory = supervisor
        .mint_tcp_port_factory(FlowPortConfig::new(1, 2, 2, 2).unwrap())
        .unwrap();
    let second_factory = first_factory
        .fork_with_independent_ledgers_for_test()
        .unwrap();
    let first_flow = opened_flow(&mut supervisor, leg);
    let second_flow = opened_flow(&mut supervisor, leg);
    let (first, mut first_driver) = first_factory.open_flow(first_flow).unwrap();
    let (second, mut second_driver) = second_factory.open_flow(second_flow).unwrap();

    first
        .try_send_uplink_with(1, || Bytes::from_static(b"a"))
        .unwrap();
    supervisor
        .apply_command(SessionOwnerCommand::Driver(
            first_driver.try_recv_next().unwrap().unwrap(),
        ))
        .unwrap();
    second
        .try_send_uplink_with(1, || Bytes::from_static(b"b"))
        .unwrap();

    assert!(matches!(
        supervisor.apply_command(SessionOwnerCommand::Driver(
            second_driver.try_recv_next().unwrap().unwrap(),
        )),
        Err(SessionSupervisorError::ReducerReplayCapacityInvariant {
            source: SessionError::ReplayByteBudgetExceeded {
                direction: Direction::ClientToTarget,
                attempted: 2,
                max: 1,
            }
        })
    ));
    let poisoned = supervisor.snapshot();
    assert!(poisoned.poisoned);
    assert_eq!(poisoned.uplink_replay_owned_bytes, 1);
    assert_eq!(poisoned.quarantined_uplink_owned_bytes, 1);
    assert_eq!(first_factory.session_uplink_owned_bytes(), 1);
    assert_eq!(second_factory.session_uplink_owned_bytes(), 1);

    drop(supervisor);
    assert_eq!(first_factory.session_uplink_owned_bytes(), 0);
    assert_eq!(second_factory.session_uplink_owned_bytes(), 0);
}

#[test]
fn terminal_superseded_queued_data_releases_staging_without_poisoning() {
    let leg = committed_leg();
    let model = SessionModel::new(SessionRole::Client, session_config(), leg);
    let mut supervisor = SessionSupervisor::new(model);
    let factory = port_factory(&mut supervisor);
    let local_flow = opened_flow(&mut supervisor, leg);
    let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
    flow.try_send_uplink_with(1, || Bytes::from_static(b"s"))
        .unwrap();
    let queued = driver.try_recv_next().unwrap().unwrap();
    supervisor
        .apply_event(SessionEvent::LocalReset {
            flow: local_flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();

    assert!(matches!(
        supervisor.apply_command(SessionOwnerCommand::Driver(queued)),
        Err(SessionSupervisorError::Reducer(SessionError::RetiredFlow(
            flow_id
        ))) if flow_id == local_flow.flow_id()
    ));
    let snapshot = supervisor.snapshot();
    assert!(!snapshot.poisoned);
    assert_eq!(snapshot.uplink_replay_extents, 0);
    assert_eq!(snapshot.quarantined_uplink_extents, 0);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
}

#[test]
fn dequeued_uplink_ownership_survives_until_reducer_application_ack() {
    let leg = committed_leg();
    let model = SessionModel::new(SessionRole::Client, session_config(), leg);
    let mut supervisor = SessionSupervisor::new(model);
    let factory = port_factory(&mut supervisor);
    let local_flow = opened_flow(&mut supervisor, leg);
    let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
    let probe = flow.probe();

    flow.try_send_uplink_with(4, || Bytes::from_static(b"data"))
        .unwrap();
    let input = driver.try_recv_next().unwrap().unwrap();
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
    assert_eq!(factory.session_uplink_owned_bytes(), 4);

    let effects = supervisor
        .apply_command(SessionOwnerCommand::Driver(input))
        .unwrap();
    assert!(effects.iter().any(|effect| matches!(
        effect,
        SessionEffect::Transmit(frame)
            if matches!(frame.record(), Record::Data { payload, .. } if payload.as_ref() == b"data")
    )));
    assert_eq!(supervisor.snapshot().uplink_replay_extents, 1);
    assert_eq!(supervisor.snapshot().uplink_replay_owned_bytes, 4);
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
    assert_eq!(factory.session_uplink_owned_bytes(), 4);

    let equal = supervisor
        .apply_event_observed_inner(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Ack {
                    flow_id: local_flow.flow_id(),
                    direction: Direction::ClientToTarget,
                    next_accepted: ByteOffset::new(0),
                    final_accepted: false,
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (visible, application_ack) = equal.into_parts();
    assert!(visible.is_empty());
    assert_eq!(application_ack, None);
    assert_eq!(supervisor.snapshot().uplink_replay_extents, 1);

    let advanced = supervisor
        .apply_event_observed_inner(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Ack {
                    flow_id: local_flow.flow_id(),
                    direction: Direction::ClientToTarget,
                    next_accepted: ByteOffset::new(2),
                    final_accepted: false,
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (visible, application_ack) = advanced.into_parts();
    assert!(
        visible.is_empty(),
        "the opaque reducer receipt must not escape"
    );
    let application_ack = application_ack.unwrap();
    assert_eq!(application_ack.session_id(), local_flow.session_id());
    assert_eq!(application_ack.flow_id(), local_flow.flow_id());
    assert_eq!(application_ack.generation(), leg.generation());
    assert_eq!(application_ack.direction(), Direction::ClientToTarget);
    assert_eq!(application_ack.previous_accepted(), ByteOffset::new(0));
    assert_eq!(application_ack.next_accepted(), ByteOffset::new(2));
    assert_eq!(application_ack.released_bytes(), 2);
    assert!(!application_ack.final_accepted());
    assert_eq!(
        supervisor.snapshot().uplink_replay_extents,
        1,
        "a partial reducer ACK must not release the whole adapter extent"
    );
    assert_eq!(supervisor.snapshot().uplink_replay_owned_bytes, 4);

    let final_advance = supervisor
        .apply_event_observed_inner(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Ack {
                    flow_id: local_flow.flow_id(),
                    direction: Direction::ClientToTarget,
                    next_accepted: ByteOffset::new(4),
                    final_accepted: false,
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (visible, application_ack) = final_advance.into_parts();
    assert!(visible.is_empty());
    let application_ack = application_ack.unwrap();
    assert_eq!(application_ack.previous_accepted(), ByteOffset::new(2));
    assert_eq!(application_ack.next_accepted(), ByteOffset::new(4));
    assert_eq!(application_ack.released_bytes(), 2);
    assert_eq!(supervisor.snapshot().uplink_replay_extents, 0);
    assert_eq!(supervisor.snapshot().uplink_replay_owned_bytes, 0);
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
}

#[test]
fn post_reduce_receipt_mismatch_quarantines_ownership_and_poisons_every_mutator() {
    let leg = committed_leg();
    let model = SessionModel::new(SessionRole::Client, session_config(), leg);
    let mut supervisor = SessionSupervisor::new(model);
    let factory = port_factory(&mut supervisor);
    let duplicate_factory = factory.fork_with_independent_ledgers_for_test().unwrap();
    let local_flow = opened_flow(&mut supervisor, leg);
    let (first, mut first_driver) = factory.open_flow(local_flow).unwrap();
    let (second, mut second_driver) = duplicate_factory.open_flow(local_flow).unwrap();

    first
        .try_send_uplink_with(1, || Bytes::from_static(b"a"))
        .unwrap();
    supervisor
        .apply_command(SessionOwnerCommand::Driver(
            first_driver.try_recv_next().unwrap().unwrap(),
        ))
        .unwrap();

    // A second endpoint for the same capability starts its local source
    // offset at zero. The reducer has already retained this byte at offset
    // one before the adapter can detect the mismatched storage receipt.
    second
        .try_send_uplink_with(1, || Bytes::from_static(b"b"))
        .unwrap();
    assert!(matches!(
        supervisor.apply_command(SessionOwnerCommand::Driver(
            second_driver.try_recv_next().unwrap().unwrap(),
        )),
        Err(SessionSupervisorError::FlowPort(
            FlowPortError::UplinkReplayReceiptMismatch
        ))
    ));

    let poisoned = supervisor.snapshot();
    assert!(poisoned.poisoned);
    assert_eq!(poisoned.uplink_replay_extents, 1);
    assert_eq!(poisoned.uplink_replay_owned_bytes, 1);
    assert_eq!(poisoned.quarantined_uplink_extents, 1);
    assert_eq!(poisoned.quarantined_uplink_owned_bytes, 1);
    assert_eq!(
        poisoned
            .session
            .replay_usage(Direction::ClientToTarget)
            .bytes(),
        2
    );
    assert_eq!(first.probe().snapshot().uplink_owned_segments(), 1);
    assert_eq!(second.probe().snapshot().uplink_owned_segments(), 1);
    assert_eq!(factory.session_uplink_owned_bytes(), 1);
    assert_eq!(factory.session_uplink_owned_segments(), 1);
    assert_eq!(duplicate_factory.session_uplink_owned_bytes(), 1);
    assert_eq!(duplicate_factory.session_uplink_owned_segments(), 1);

    assert!(matches!(
        supervisor.apply_event(SessionEvent::LegLost { leg }),
        Err(SessionSupervisorError::PoisonedOwnershipInvariant)
    ));
    assert!(matches!(
        supervisor.apply_command(SessionOwnerCommand::Event(SessionEvent::LegLost { leg })),
        Err(SessionSupervisorError::PoisonedOwnershipInvariant)
    ));

    let transport = binding(0x71);
    let established = EstablishedLeg::for_authenticated_transport(transport);
    let request = attach_request(3, 0x71);
    let pending = established.begin_attach(request);
    let proof = credentials().prove(&request, &transport).unwrap();
    let committed = authority(2)
        .verify_and_commit(&request, &transport, &proof)
        .unwrap();
    let AttachResponse::Accepted(attached) = pending
        .validate_response(established.bind_received_frame(committed.attach_accepted_frame()))
        .unwrap()
    else {
        panic!("the exact correlated acceptance changed response kind");
    };
    assert!(matches!(
        supervisor.install_attached_leg(&attached),
        Err(SessionSupervisorError::PoisonedOwnershipInvariant)
    ));

    let owner_transport = binding(0x72);
    let owner_leg = EstablishedLeg::for_authenticated_transport(owner_transport);
    let owner_request = attach_request(3, 0x72);
    let owner_proof = credentials()
        .prove(&owner_request, &owner_transport)
        .unwrap();
    assert!(matches!(
        supervisor.accept_owner_attach_for_test(
            &owner_leg,
            owner_leg.bind_received_frame(owner_request.to_attach_frame(owner_proof)),
        ),
        Err(SessionSupervisorError::PoisonedOwnershipInvariant)
    ));

    assert_eq!(supervisor.snapshot(), poisoned);
    drop(supervisor);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
    assert_eq!(duplicate_factory.session_uplink_owned_bytes(), 0);
    assert_eq!(duplicate_factory.session_uplink_owned_segments(), 0);
}

#[test]
fn duplicate_replay_extent_keeps_the_original_ownership_and_quarantines_the_new_one() {
    let leg = committed_leg();
    let model = SessionModel::new(SessionRole::Client, session_config(), leg);
    let mut supervisor = SessionSupervisor::new(model);
    let factory = port_factory(&mut supervisor);
    let local_flow = opened_flow(&mut supervisor, leg);
    let (flow, mut driver) = factory.open_flow(local_flow).unwrap();

    flow.try_send_uplink_with(1, || Bytes::from_static(b"a"))
        .unwrap();
    supervisor
        .apply_command(SessionOwnerCommand::Driver(
            driver.try_recv_next().unwrap().unwrap(),
        ))
        .unwrap();

    // Fault-inject the adapter index only: the ownership itself remains
    // the reducer's first 0..1 extent. The next valid DATA produces 1..2,
    // exercising the duplicate-key branch without constructing permits
    // outside the factory.
    let first_key = ReplayExtentKey {
        flow_id: local_flow.flow_id(),
        direction: Direction::ClientToTarget,
        start: ByteOffset::new(0),
        end: ByteOffset::new(1),
    };
    let duplicate_key = ReplayExtentKey {
        start: ByteOffset::new(1),
        end: ByteOffset::new(2),
        ..first_key
    };
    assert!(supervisor.rekey_uplink_replay_for_test(first_key, duplicate_key));

    flow.try_send_uplink_with(1, || Bytes::from_static(b"b"))
        .unwrap();
    assert!(matches!(
        supervisor.apply_command(SessionOwnerCommand::Driver(
            driver.try_recv_next().unwrap().unwrap(),
        )),
        Err(SessionSupervisorError::DuplicateReplayExtent)
    ));

    let retained = supervisor.uplink_replay.get(&duplicate_key).unwrap();
    assert_eq!(retained.start(), ByteOffset::new(0));
    assert_eq!(retained.end(), ByteOffset::new(1));
    let poisoned = supervisor.snapshot();
    assert!(poisoned.poisoned);
    assert_eq!(poisoned.uplink_replay_extents, 1);
    assert_eq!(poisoned.quarantined_uplink_extents, 1);
    assert_eq!(poisoned.uplink_replay_owned_bytes, 1);
    assert_eq!(poisoned.quarantined_uplink_owned_bytes, 1);
    assert_eq!(factory.session_uplink_owned_bytes(), 2);
    assert_eq!(factory.session_uplink_owned_segments(), 2);

    drop(supervisor);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
}

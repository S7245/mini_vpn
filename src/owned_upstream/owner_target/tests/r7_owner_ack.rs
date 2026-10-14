use super::*;

#[test]
fn owner_ack_observation_gate_is_minted_and_consumed_only_by_owner_target() {
    let owner_target_source = include_str!("../../owner_target.rs");
    let observed_turn_source = include_str!("../observed_turn.rs");
    let supervisor_source = include_str!("../../supervisor.rs");
    let owner_target_production = owner_target_source
        .split_once("#[cfg(test)]\nmod tests")
        .unwrap()
        .0;
    let supervisor_production = supervisor_source
        .split_once("#[cfg(test)]\nmod tests")
        .unwrap()
        .0;

    assert_eq!(
        observed_turn_source
            .matches("OwnerTargetObservedApplyGate::mint()")
            .count(),
        1,
        "only the serialized OwnerTarget event path may mint observation authority"
    );
    assert_eq!(
        observed_turn_source
            .matches(".apply_owner_command_observed(command, &observation_gate)")
            .count(),
        1,
        "only OwnerTarget may consume command observation authority"
    );
    for moved_call in [
        "OwnerTargetObservedApplyGate::mint()",
        ".apply_owner_command_observed(command, &observation_gate)",
    ] {
        assert_eq!(
            owner_target_production.matches(moved_call).count(),
            0,
            "moved observation calls must not also remain in the parent"
        );
    }
    assert!(supervisor_production.contains("_gate: &OwnerTargetObservedApplyGate,"));
    assert!(
        owner_target_production
            .contains("impl OwnerTargetObservedApplyGate {\n    fn mint() -> Self")
    );
    assert!(!owner_target_production.contains("impl OwnerTargetObservedApplyGate {\n    pub"));
}

fn application_ack_event(
    leg: crate::resumable::CommittedLeg,
    flow_id: SessionFlowId,
    next_accepted: u64,
    final_accepted: bool,
) -> SessionEvent {
    SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(next_accepted),
                final_accepted,
            },
        )
        .unwrap(),
    }
}

#[test]
fn owner_target_exports_first_outstanding_replay_after_owned_target_read() {
    let (leg, mut executor) = executor();
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));
    executor
        .target_mut()
        .feed_read(flow_id, Bytes::from_static(b"reply"))
        .unwrap();

    let observed = executor.try_read_target_observed(flow_id).unwrap();
    let (outputs, observations) = observed.into_progress_parts();
    assert!(transmitted(&outputs).iter().any(|record| matches!(
        record,
        Record::Data {
            flow_id: observed_flow,
            direction: Direction::TargetToClient,
            offset,
            payload,
        } if *observed_flow == flow_id
            && *offset == ByteOffset::new(0)
            && payload.as_ref() == b"reply"
    )));

    let outstanding = observations
        .replay_became_outstanding()
        .expect("owned Target DATA must export one post-commit replay fact");
    assert_eq!(
        outstanding.session_id(),
        SessionId::new([0x11; 16]).unwrap()
    );
    assert_eq!(outstanding.generation(), leg.generation());
    assert_eq!(outstanding.flow_id(), flow_id);
    assert_eq!(outstanding.direction(), Direction::TargetToClient);
    assert_eq!(outstanding.start(), ByteOffset::new(0));
    assert_eq!(outstanding.end(), ByteOffset::new(5));
    assert_eq!(executor.snapshot().target_read_owned_bytes, 5);
    assert_eq!(executor.snapshot().target_read_owned_segments, 1);
}

#[test]
fn owner_target_exports_outstanding_only_on_idle_to_nonempty_transition() {
    let (leg, mut executor) = executor();
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));

    executor
        .target_mut()
        .feed_read(flow_id, Bytes::from_static(b"a"))
        .unwrap();
    let (_, first) = executor
        .try_read_target_observed(flow_id)
        .unwrap()
        .into_progress_parts();
    assert_eq!(
        first.replay_became_outstanding().unwrap().start(),
        ByteOffset::new(0)
    );

    executor
        .target_mut()
        .feed_read(flow_id, Bytes::from_static(b"b"))
        .unwrap();
    let (_, still_outstanding) = executor
        .try_read_target_observed(flow_id)
        .unwrap()
        .into_progress_parts();
    assert!(still_outstanding.replay_became_outstanding().is_none());

    let (_, acked) = executor
        .apply_event_observed(application_ack_event(leg, flow_id, 2, false))
        .unwrap()
        .into_progress_parts();
    assert_eq!(acked.application_ack().unwrap().remaining_oldest(), None);

    executor
        .target_mut()
        .feed_read(flow_id, Bytes::from_static(b"c"))
        .unwrap();
    let (_, next) = executor
        .try_read_target_observed(flow_id)
        .unwrap()
        .into_progress_parts();
    let next = next.replay_became_outstanding().unwrap();
    assert_eq!(next.start(), ByteOffset::new(2));
    assert_eq!(next.end(), ByteOffset::new(3));
}

#[test]
fn owner_target_surfaces_only_positive_post_commit_application_ack_progress() {
    let (leg, mut executor) = executor();
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));
    executor
        .target_mut()
        .feed_read(flow_id, Bytes::from_static(b"reply"))
        .unwrap();
    executor.try_read_target(flow_id).unwrap();

    let zero = executor
        .apply_event_observed(application_ack_event(leg, flow_id, 0, false))
        .unwrap();
    let (outputs, observation) = zero.into_parts();
    assert!(outputs.is_empty());
    assert_eq!(observation, None);

    let advanced = executor
        .apply_event_observed(application_ack_event(leg, flow_id, 2, false))
        .unwrap();
    let (outputs, observation) = advanced.into_parts();
    assert!(outputs.is_empty());
    let observation = observation.expect("positive reducer ACK must be observable");
    assert_eq!(
        observation.session_id(),
        SessionId::new([0x11; 16]).unwrap()
    );
    assert_eq!(observation.flow_id(), flow_id);
    assert_eq!(observation.generation(), leg.generation());
    assert_eq!(observation.direction(), Direction::TargetToClient);
    assert_eq!(observation.previous_accepted(), ByteOffset::new(0));
    assert_eq!(observation.next_accepted(), ByteOffset::new(2));
    assert_eq!(observation.released_bytes(), 2);
    assert!(!observation.final_accepted());
    assert_eq!(
        observation.remaining_oldest(),
        Some(ByteOffset::new(2)),
        "partial ACK must use the exact accepted frontier, not the coarse extent start"
    );

    let duplicate = executor
        .apply_event_observed(application_ack_event(leg, flow_id, 2, false))
        .unwrap();
    let (outputs, observation) = duplicate.into_parts();
    assert!(outputs.is_empty());
    assert_eq!(observation, None);

    let before_rejected = executor.snapshot();
    assert!(matches!(
        executor.apply_event_observed(application_ack_event(leg, flow_id, 6, false)),
        Err(OwnerTargetError::Supervisor(
            SessionSupervisorError::Reducer(_)
        ))
    ));
    assert_eq!(executor.snapshot(), before_rejected);

    executor.target_mut().finish_read(flow_id).unwrap();
    let eof = executor.try_read_target(flow_id).unwrap();
    assert!(transmitted(&eof).iter().any(|record| matches!(
        record,
        Record::Close {
            flow_id: observed,
            direction: Direction::TargetToClient,
            final_offset,
        } if *observed == flow_id && *final_offset == ByteOffset::new(5)
    )));

    let final_advance = executor
        .apply_event_observed(application_ack_event(leg, flow_id, 5, true))
        .unwrap();
    let (outputs, observation) = final_advance.into_parts();
    assert!(outputs.is_empty());
    let observation = observation.expect("remaining positive delta must be observable");
    assert_eq!(observation.previous_accepted(), ByteOffset::new(2));
    assert_eq!(observation.next_accepted(), ByteOffset::new(5));
    assert_eq!(observation.released_bytes(), 3);
    assert!(observation.final_accepted());
    assert_eq!(observation.remaining_oldest(), None);
    assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
    assert_eq!(executor.snapshot().target_read_owned_segments, 0);
}

#[test]
fn owner_target_preserves_positive_ack_and_flow_terminal_from_the_same_turn() {
    let (leg, mut executor) = executor();
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));

    peer_frame(
        &mut executor,
        leg,
        Record::Close {
            flow_id,
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(0),
        },
    )
    .unwrap();
    executor
        .target_mut()
        .feed_read(flow_id, Bytes::from_static(b"reply"))
        .unwrap();
    executor.try_read_target(flow_id).unwrap();
    executor.target_mut().finish_read(flow_id).unwrap();
    executor.try_read_target(flow_id).unwrap();

    let observed = executor
        .apply_event_observed(application_ack_event(leg, flow_id, 5, true))
        .unwrap();
    let (outputs, observations) = observed.into_progress_parts();
    assert!(outputs.iter().any(|output| matches!(
        output,
        OwnerTargetOutput::TerminalGraceStarted { terminal }
            if terminal.flow_id() == flow_id
    )));

    let ack = observations
        .application_ack()
        .expect("positive final ACK must survive the terminal effect");
    assert_eq!(ack.previous_accepted(), ByteOffset::new(0));
    assert_eq!(ack.next_accepted(), ByteOffset::new(5));
    assert_eq!(ack.remaining_oldest(), None);
    let terminal = observations
        .flow_terminal()
        .expect("same-turn flow terminal must not overwrite the ACK fact");
    assert_eq!(terminal.session_id(), SessionId::new([0x11; 16]).unwrap());
    assert_eq!(terminal.generation(), leg.generation());
    assert_eq!(terminal.flow_id(), flow_id);
    assert_eq!(terminal.direction(), Direction::TargetToClient);
}

#[test]
fn owner_target_exports_terminal_from_nested_half_close_completion() {
    let (leg, mut executor) = executor();
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));
    executor
        .target_mut()
        .feed_read(flow_id, Bytes::from_static(b"reply"))
        .unwrap();
    executor.try_read_target(flow_id).unwrap();
    executor.target_mut().finish_read(flow_id).unwrap();
    executor.try_read_target(flow_id).unwrap();

    let acked = executor
        .apply_event_observed(application_ack_event(leg, flow_id, 5, true))
        .unwrap();
    let (_, acked) = acked.into_progress_parts();
    assert!(acked.application_ack().is_some());
    assert!(acked.flow_terminal().is_none());

    let observed = executor
        .apply_event_observed(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Close {
                    flow_id,
                    direction: Direction::ClientToTarget,
                    final_offset: ByteOffset::new(0),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (outputs, observations) = observed.into_progress_parts();
    assert!(outputs.iter().any(|output| matches!(
        output,
        OwnerTargetOutput::TerminalGraceStarted { terminal }
            if terminal.flow_id() == flow_id
    )));
    let terminal = observations
        .flow_terminal()
        .expect("nested SinkHalfClosed reducer commit must remain in the same observed turn");
    assert_eq!(terminal.flow_id(), flow_id);
    assert_eq!(terminal.direction(), Direction::TargetToClient);
}

#[test]
fn owner_target_exports_session_expiry_only_after_reducer_commit() {
    let (leg, mut executor) = executor();

    let leg_lost = executor
        .apply_event_observed(SessionEvent::LegLost { leg })
        .unwrap();
    let (_, observations) = leg_lost.into_progress_parts();
    assert!(observations.is_empty());

    let expired = executor
        .apply_event_observed(SessionEvent::ResumeGraceExpired { leg })
        .unwrap();
    let (outputs, observations) = expired.into_progress_parts();
    assert_eq!(outputs, vec![OwnerTargetOutput::SessionExpired]);
    let expiry = observations
        .session_expired()
        .expect("committed session expiry must export one fixed scalar");
    assert_eq!(expiry.session_id(), SessionId::new([0x11; 16]).unwrap());
    assert_eq!(expiry.generation(), leg.generation());
    assert!(observations.replay_became_outstanding().is_none());
    assert!(observations.application_ack().is_none());
    assert!(observations.flow_terminal().is_none());
}

#[test]
fn post_commit_observation_is_retained_when_later_target_cleanup_fails() {
    let (leg, mut executor) = executor();
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));

    executor.target_mut().cancel(flow_id).unwrap();
    let terminal = match executor.target_mut().join(flow_id).unwrap() {
        TargetJoinCompletion::Joined(terminal) => terminal,
        other => panic!("test setup must join the physical Target, got {other:?}"),
    };
    assert_eq!(
        executor
            .target_mut()
            .retire_terminal_tombstone(flow_id)
            .unwrap(),
        TargetTombstoneRetireCompletion::Retired(terminal)
    );

    executor.apply_event(SessionEvent::LegLost { leg }).unwrap();
    assert!(matches!(
        executor.apply_event_observed(SessionEvent::ResumeGraceExpired { leg }),
        Err(OwnerTargetError::Target(_))
    ));
    assert_eq!(executor.snapshot().aborted_observations, 1);
    assert!(executor.snapshot().aborted_effects > 0);

    let retained = executor
        .take_retained_observations_after_failure()
        .expect("the committed session-expiry fact must not be dropped by later I/O failure");
    let expiry = retained.session_expired().unwrap();
    assert_eq!(expiry.session_id(), SessionId::new([0x11; 16]).unwrap());
    assert_eq!(expiry.generation(), leg.generation());
    assert_eq!(executor.snapshot().aborted_observations, 0);
    assert!(matches!(
        executor.apply_event(SessionEvent::ResumeGraceExpired { leg }),
        Err(OwnerTargetError::RejectedEvent {
            block: OwnerTargetInputBlock::AbortedEffectInvariant,
            ..
        })
    ));
}

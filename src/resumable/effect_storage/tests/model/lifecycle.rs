//! Real Session OPEN/terminal storage transactions and ordered recovery.
//! Uses original protocol capabilities; no transport or whole-root zero claim.

use super::*;

fn limited_model(f: usize, z: usize) -> (SessionModel, CommittedLeg) {
    let (_, leg) = model_and_leg(SessionRole::Client);
    let config = SessionConfig::new(
        f,
        z,
        TcpWindowLimits::new(64, 8).unwrap(),
        TcpWindowLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReceiveBudgetLimits::new(64, 8).unwrap(),
        64,
    )
    .unwrap();
    (SessionModel::new(SessionRole::Client, config, leg), leg)
}

#[test]
fn original_f_z_and_checked_node_layout_refuse_before_new_storage() {
    let (mut model, leg) = limited_model(1, 1);
    let open = || SessionEvent::LocalOpen {
        leg,
        target: crate::shared::TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
    };
    let first = model.reduce(open()).unwrap();
    let SessionEffect::LocalFlowOpened { flow: first } = first[0] else {
        panic!("missing OPEN")
    };
    SessionModel::fail_next_flow_node_allocation_for_test();
    assert_eq!(
        model.reduce(open()).unwrap_err(),
        SessionError::FlowCapacityExceeded { max: 1 }
    );
    let finished = model
        .reduce(SessionEvent::LocalReset {
            flow: first,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();
    let Some(SessionEffect::FlowFinished { terminal, .. }) = finished.last() else {
        panic!("missing terminal")
    };
    assert_eq!(
        model.reduce(open()).unwrap_err(),
        SessionError::FlowStorageReserve
    );
    let second = model.reduce(open()).unwrap();
    let SessionEffect::LocalFlowOpened { flow: second } = second[0] else {
        panic!("missing second OPEN")
    };
    let before = (model.snapshot(), model.flow_snapshot(second.flow_id()));
    assert_eq!(
        model
            .reduce(SessionEvent::LocalReset {
                flow: second,
                reason: ResetReason::LocalAbandon
            })
            .unwrap_err(),
        SessionError::TerminalTombstoneCapacityExceeded { max: 1 }
    );
    assert_eq!(
        (model.snapshot(), model.flow_snapshot(second.flow_id())),
        before
    );
    model
        .reduce(SessionEvent::TerminalGraceExpired {
            terminal: *terminal,
        })
        .unwrap();
    model
        .reduce(SessionEvent::LocalReset {
            flow: second,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();

    let (mut huge, huge_leg) = limited_model(usize::MAX, 1);
    let before = huge.snapshot();
    SessionModel::fail_next_flow_node_allocation_for_test();
    assert_eq!(
        huge.reduce(SessionEvent::LocalOpen {
            leg: huge_leg,
            target: crate::shared::TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
        })
        .unwrap_err(),
        SessionError::FlowStorageLayout
    );
    assert_eq!(huge.snapshot(), before);
    let (mut fresh, fresh_leg) = limited_model(1, 1);
    assert_eq!(
        fresh
            .reduce(SessionEvent::LocalOpen {
                leg: fresh_leg,
                target: crate::shared::TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
            })
            .unwrap_err(),
        SessionError::FlowStorageReserve
    );
}

#[test]
fn local_open_node_reserve_precedes_adapter_and_returns_exact_target() {
    use crate::resumable::session::PreparedLocalOpenAdapterRejection;
    let (mut model, leg) = model_and_leg(SessionRole::Client);
    let before = model.snapshot();
    let mut host = String::with_capacity(4096);
    host.push_str("bounded.example");
    let pointer = host.as_ptr();
    let capacity = host.capacity();
    let target = crate::shared::TargetAddr::DomainPort { host, port: 443 };
    SessionModel::fail_next_flow_node_allocation_for_test();
    let failure = model
        .install_prepared_client_local_open(
            leg,
            target,
            |_| -> Result<(), PreparedLocalOpenAdapterRejection<()>> {
                panic!("Adapter must not enter before physical Session storage is prepared");
            },
        )
        .expect_err("new physical node must be prepared before Adapter");
    let (target, error) = failure.into_parts();
    assert_eq!(error, SessionError::FlowStorageReserve);
    let crate::shared::TargetAddr::DomainPort { host, port } = &target else {
        panic!("original target must be returned")
    };
    assert_eq!(
        (*port, host.as_ptr(), host.capacity()),
        (443, pointer, capacity)
    );
    assert_eq!(model.snapshot(), before);

    let rejected = model
        .install_prepared_client_local_open(leg, target, |input| Err::<(), _>(input.reject(17_u8)))
        .unwrap()
        .unwrap_err();
    let (target, input, cause) = rejected.into_parts();
    assert_eq!(cause, 17);
    assert_eq!(input.local_flow().flow_id(), SessionFlowId::new(1).unwrap());
    assert_eq!(model.snapshot(), before);
    let opened = model
        .reduce(SessionEvent::LocalOpen { leg, target })
        .unwrap();
    assert!(matches!(opened[0], SessionEffect::LocalFlowOpened { flow }
        if flow.flow_id() == SessionFlowId::new(1).unwrap()));
    assert_eq!(model.snapshot().flow_count(), 1);
}

#[test]
fn adapter_unwind_preserves_original_vacant_node_for_later_open() {
    use crate::resumable::session::PreparedLocalOpenAdapterRejection;
    let (mut model, leg, flow) = opened_model();
    let finished = model
        .reduce(SessionEvent::LocalReset {
            flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();
    let Some(SessionEffect::FlowFinished { terminal, .. }) = finished.last() else {
        panic!("missing terminal")
    };
    model
        .reduce(SessionEvent::TerminalGraceExpired {
            terminal: *terminal,
        })
        .unwrap();
    let before = model.snapshot();
    let target = || crate::shared::TargetAddr::IpPort(([127, 0, 0, 1], 443).into());
    SessionModel::fail_next_flow_node_allocation_for_test();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        model.install_prepared_client_local_open(
            leg,
            target(),
            |_| -> Result<(), PreparedLocalOpenAdapterRejection<()>> {
                panic!("test-only Adapter unwind")
            },
        )
    }))
    .expect_err("Adapter actually entered");
    assert_eq!(
        unwind.downcast_ref::<&str>(),
        Some(&"test-only Adapter unwind")
    );
    assert_eq!(model.snapshot(), before);
    let rejected = model
        .install_prepared_client_local_open(leg, target(), |input| Err::<(), _>(input.reject(())))
        .unwrap();
    assert!(rejected.is_err());
    assert_eq!(model.snapshot(), before);
    let accepted = model
        .reduce(SessionEvent::LocalOpen {
            leg,
            target: target(),
        })
        .unwrap();
    assert!(
        matches!(accepted[0], SessionEffect::LocalFlowOpened { flow }
        if flow.flow_id() == SessionFlowId::new(2).unwrap())
    );
    assert_eq!(
        model
            .reduce(SessionEvent::LocalOpen {
                leg,
                target: target()
            })
            .unwrap_err(),
        SessionError::FlowStorageReserve
    );
}

#[test]
fn terminal_and_session_publication_precede_test_only_flow_disposal_unwind() {
    for mode in 0..3 {
        let (mut model, leg, flow) = opened_model();
        model
            .reduce(SessionEvent::LocalData {
                flow,
                payload: bytes::Bytes::from_static(b"owned"),
            })
            .unwrap();
        let event = if mode == 2 {
            model.reduce(SessionEvent::LegLost { leg }).unwrap();
            SessionEvent::ResumeGraceExpired { leg }
        } else {
            SessionEvent::LocalReset {
                flow,
                reason: ResetReason::LocalAbandon,
            }
        };
        let mut batch = if mode == 1 {
            SessionEffectBatch::prepared(1).unwrap()
        } else {
            SessionEffectBatch::empty()
        };
        SessionModel::fail_next_flow_disposal_for_test();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            model.reduce_into(event, &mut batch)
        }))
        .expect_err("test-only original FlowState destructor must run");
        assert_eq!(
            unwind.downcast_ref::<&str>(),
            Some(&"test-only FlowState disposal")
        );
        assert_eq!(model.snapshot().flow_count(), 0);
        assert_eq!(
            model
                .snapshot()
                .replay_usage(Direction::ClientToTarget)
                .bytes(),
            0
        );
        assert!(batch.mutation_started());
        let effects: Vec<_> = batch.iter().collect();
        if mode == 2 {
            assert_eq!(
                model.snapshot().phase(),
                crate::resumable::session::SessionPhase::Expired
            );
            assert_eq!(model.snapshot().terminal_tombstones(), 0);
            assert!(matches!(
                effects.as_slice(),
                [SessionEffect::SessionExpired]
            ));
        } else {
            assert_eq!(model.snapshot().terminal_tombstones(), 1);
            assert!(
                matches!(effects.as_slice(), [SessionEffect::Transmit(_), SessionEffect::FlowFinished { flow_id, .. }]
                if *flow_id == flow.flow_id())
            );
        }
    }
}

#[test]
fn peer_open_duplicates_terminal_replay_and_expiry_reuse_do_not_allocate_nodes() {
    let (mut model, leg) = model_and_leg(SessionRole::Owner);
    let open = |raw| SessionEvent::PeerFrame {
        leg,
        frame: Frame::new(
            leg.generation(),
            Record::Open {
                flow_id: SessionFlowId::new(raw).unwrap(),
                target: crate::shared::TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
            },
        ),
    };
    let opened = model.reduce(open(1)).unwrap();
    let [SessionEffect::PeerOpenRequested { request, .. }] = opened.as_slice() else {
        panic!("missing original request")
    };
    SessionModel::fail_next_flow_node_allocation_for_test();
    assert!(model.reduce(open(1)).unwrap().is_empty());
    let terminal = model
        .reduce(SessionEvent::PeerOpenResolved {
            request: *request,
            result: OpenResultCode::TargetRefused,
        })
        .unwrap();
    let Some(SessionEffect::FlowFinished { terminal, .. }) = terminal.last() else {
        panic!("missing original terminal")
    };
    let terminal = *terminal;
    let replay = model.reduce(open(1)).unwrap();
    assert!(matches!(replay.as_slice(), [SessionEffect::Transmit(frame)]
        if matches!(frame.record(), Record::OpenResult { result: OpenResultCode::TargetRefused, .. })));
    let before = model.snapshot();
    assert_eq!(
        model.reduce(open(2)).unwrap_err(),
        SessionError::FlowStorageReserve
    );
    assert_eq!(model.snapshot(), before);
    model
        .reduce(SessionEvent::TerminalGraceExpired { terminal })
        .unwrap();
    assert_eq!(model.snapshot().terminal_tombstones(), 0);
    SessionModel::fail_next_flow_node_allocation_for_test();
    assert!(matches!(
        model.reduce(open(2)).unwrap().as_slice(),
        [SessionEffect::PeerOpenRequested { .. }]
    ));
    assert!(matches!(
        model.reduce(open(1)),
        Err(SessionError::PeerFlowIdNotMonotonic { .. })
    ));
    assert_eq!(
        model.reduce(open(3)).unwrap_err(),
        SessionError::FlowStorageReserve
    );
    assert_eq!(model.snapshot().flow_count(), 1);
}

#[test]
fn recovery_orders_terminal_keys_before_live_keys_not_retirement_order() {
    let (mut model, leg) = model_and_leg(SessionRole::Client);
    let mut flows = Vec::new();
    for port in 443..446 {
        let opened = model
            .reduce(SessionEvent::LocalOpen {
                leg,
                target: crate::shared::TargetAddr::IpPort(([127, 0, 0, 1], port).into()),
            })
            .unwrap();
        let SessionEffect::LocalFlowOpened { flow } = opened[0] else {
            panic!("missing real local OPEN")
        };
        flows.push(flow);
    }
    for flow in [flows[2], flows[0]] {
        model
            .reduce(SessionEvent::LocalReset {
                flow,
                reason: ResetReason::LocalAbandon,
            })
            .unwrap();
    }
    assert_eq!(model.snapshot().flow_count(), 1);
    assert!(model.flow_snapshot(flows[1].flow_id()).is_some());
    model.reduce(SessionEvent::LegLost { leg }).unwrap();
    let (_, next) = model_and_leg_at(SessionRole::Client, 3);
    let effects = model
        .reduce(SessionEvent::ReplacementAttached { leg: next })
        .unwrap();
    assert!(matches!(effects[0], SessionEffect::LegActivated { .. }));
    let records: Vec<_> = effects[1..]
        .iter()
        .map(|effect| match effect {
            SessionEffect::Transmit(frame) => frame.record(),
            _ => panic!("unexpected recovery effect"),
        })
        .collect();
    assert!(matches!(records.as_slice(), [
        Record::Reset { flow_id: first, .. },
        Record::Reset { flow_id: third, .. },
        Record::Open { flow_id: second, .. },
    ] if *first == flows[0].flow_id() && *third == flows[2].flow_id()
        && *second == flows[1].flow_id()));
}

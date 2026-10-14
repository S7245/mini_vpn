//! Natural terminal completion/refusal precommit ownership in the pure Model.
//! Fixtures stay private in the ancestor; no physical/session-root zero claim.

use super::*;

fn reject_then_retry(
    model: &mut SessionModel,
    flow: LocalFlow,
    event: SessionEvent,
) -> Vec<SessionEffect> {
    let before = model.snapshot();
    let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let rejected = model.reduce(event.clone());
    FAIL_NEXT_ALLOCATION.set(false);
    assert!(
        matches!(rejected, Err(SessionError::EffectStorageReserve)),
        "precommit rejection: {rejected:?}"
    );
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
    let effects = model.reduce(event).unwrap();
    assert_eq!(effects.capacity(), 3);
    effects
}

#[test]
fn client_first_open_result_is_not_recorded_on_reserve_rejection() {
    use bytes::Bytes;
    for result in [OpenResultCode::Opened, OpenResultCode::TargetRefused] {
        let (mut model, leg, flow) = opened_model();
        model
            .reduce(SessionEvent::LocalData {
                flow,
                payload: Bytes::from_static(b"abc"),
            })
            .unwrap();
        let response = || SessionEvent::PeerFrame {
            leg,
            frame: Frame::new(
                leg.generation(),
                Record::OpenResult {
                    flow_id: flow.flow_id(),
                    result,
                },
            ),
        };
        let effects = reject_then_retry(&mut model, flow, response());
        assert!(
            matches!(effects.first(), Some(SessionEffect::LocalOpenResolved { result: actual, .. }) if *actual == result)
        );
        assert_eq!(
            effects.len(),
            if result == OpenResultCode::Opened {
                1
            } else {
                2
            }
        );
        if result == OpenResultCode::TargetRefused {
            assert!(matches!(
                effects.last(),
                Some(SessionEffect::FlowFinished { .. })
            ));
            assert_eq!(model.snapshot().flow_count(), 0);
            assert_eq!(
                model
                    .snapshot()
                    .replay_usage(Direction::ClientToTarget)
                    .bytes(),
                0
            );
        }
        FAIL_NEXT_ALLOCATION.set(true);
        let duplicate = model.reduce(response()).unwrap();
        assert!(FAIL_NEXT_ALLOCATION.replace(false));
        assert!(duplicate.is_empty());
        assert_eq!(duplicate.capacity(), 0);
    }
}

#[test]
fn natural_reset_abandon_and_halfclose_failure_retain_exact_precommit_flow() {
    use bytes::Bytes;
    for case in 0..4 {
        let (mut model, leg, flow) = opened_model();
        model
            .reduce(SessionEvent::LocalData {
                flow,
                payload: Bytes::from_static(b"abc"),
            })
            .unwrap();
        let inbound = |record| SessionEvent::PeerFrame {
            leg,
            frame: Frame::new(leg.generation(), record),
        };
        let event = match case {
            0 => SessionEvent::LocalReset {
                flow,
                reason: ResetReason::LocalAbandon,
            },
            1 => inbound(Record::Reset {
                flow_id: flow.flow_id(),
                reason: ResetReason::LocalAbandon,
            }),
            _ => {
                model
                    .reduce(inbound(Record::OpenResult {
                        flow_id: flow.flow_id(),
                        result: OpenResultCode::Opened,
                    }))
                    .unwrap();
                if case == 2 {
                    let offered = model
                        .reduce(inbound(Record::Data {
                            flow_id: flow.flow_id(),
                            direction: Direction::TargetToClient,
                            offset: ByteOffset::new(0),
                            payload: Bytes::from_static(b"xyz"),
                        }))
                        .unwrap();
                    let [SessionEffect::OfferToSink { offer, .. }] = offered.as_slice() else {
                        panic!("missing offer")
                    };
                    SessionEvent::SinkAbandoned { offer: *offer }
                } else {
                    let closed = model
                        .reduce(inbound(Record::Close {
                            flow_id: flow.flow_id(),
                            direction: Direction::TargetToClient,
                            final_offset: ByteOffset::new(0),
                        }))
                        .unwrap();
                    let [
                        SessionEffect::Transmit(_),
                        SessionEffect::HalfCloseSink { completion },
                    ] = closed.as_slice()
                    else {
                        panic!("missing half close")
                    };
                    SessionEvent::SinkHalfCloseFailed {
                        completion: *completion,
                    }
                }
            }
        };
        let effects = reject_then_retry(&mut model, flow, event);
        let [first, SessionEffect::FlowFinished { .. }] = effects.as_slice() else {
            panic!("missing terminal disposition")
        };
        if case == 1 {
            assert!(matches!(first, SessionEffect::PeerReset { .. }));
        } else {
            assert!(
                matches!(first, SessionEffect::Transmit(frame) if matches!(frame.record(), Record::Reset { .. }))
            );
        }
        assert_eq!(model.snapshot().flow_count(), 0);
        assert_eq!(model.snapshot().terminal_tombstones(), 1);
        assert_eq!(model.snapshot().receive_owned_bytes(), 0);
        assert_eq!(
            model
                .snapshot()
                .replay_usage(Direction::ClientToTarget)
                .bytes(),
            0
        );
    }
}

#[test]
fn both_graceful_finish_orders_reserve_before_retiring_the_live_flow() {
    use bytes::Bytes;
    for ack_finishes in [true, false] {
        let (mut model, leg, flow) = opened_model();
        let inbound = |record| SessionEvent::PeerFrame {
            leg,
            frame: Frame::new(leg.generation(), record),
        };
        model
            .reduce(SessionEvent::LocalData {
                flow,
                payload: Bytes::from_static(b"abc"),
            })
            .unwrap();
        model
            .reduce(inbound(Record::OpenResult {
                flow_id: flow.flow_id(),
                result: OpenResultCode::Opened,
            }))
            .unwrap();
        model.reduce(SessionEvent::LocalClose { flow }).unwrap();
        let closed = model
            .reduce(inbound(Record::Close {
                flow_id: flow.flow_id(),
                direction: Direction::TargetToClient,
                final_offset: ByteOffset::new(0),
            }))
            .unwrap();
        let [
            SessionEffect::Transmit(_),
            SessionEffect::HalfCloseSink { completion },
        ] = closed.as_slice()
        else {
            panic!("missing half-close")
        };
        let half_close = SessionEvent::SinkHalfClosed {
            completion: *completion,
        };
        let ack = inbound(Record::Ack {
            flow_id: flow.flow_id(),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(3),
            final_accepted: true,
        });
        let final_event = if ack_finishes {
            model.reduce(half_close).unwrap();
            ack
        } else {
            model.reduce(ack).unwrap();
            half_close
        };
        let before = model.snapshot();
        let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
        FAIL_NEXT_ALLOCATION.set(true);
        let rejected = model.reduce(final_event.clone());
        FAIL_NEXT_ALLOCATION.set(false);
        assert!(matches!(rejected, Err(SessionError::EffectStorageReserve)));
        assert_eq!(model.snapshot(), before);
        assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
        let finished = model.reduce(final_event).unwrap();
        assert_eq!(finished.capacity(), 3);
        let [first, SessionEffect::FlowFinished { terminal, .. }] = finished.as_slice() else {
            panic!("missing final disposition")
        };
        assert!(if ack_finishes {
            matches!(first, SessionEffect::ReplayAcknowledged { .. })
        } else {
            matches!(first, SessionEffect::Transmit(_))
        });
        assert_eq!(model.snapshot().flow_count(), 0);
        assert_eq!(model.snapshot().terminal_tombstones(), 1);
        assert_eq!(
            model
                .snapshot()
                .replay_usage(Direction::ClientToTarget)
                .bytes(),
            0
        );
        FAIL_NEXT_ALLOCATION.set(true);
        let expired = model
            .reduce(SessionEvent::TerminalGraceExpired {
                terminal: *terminal,
            })
            .unwrap();
        assert!(FAIL_NEXT_ALLOCATION.replace(false));
        assert!(expired.is_empty());
        assert_eq!(expired.capacity(), 0);
        assert_eq!(model.snapshot().terminal_tombstones(), 0);
    }
}

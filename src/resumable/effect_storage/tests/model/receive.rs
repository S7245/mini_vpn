//! Real receiver preparation/Offer transaction through the original Model.
//! Owns refusal/retry and partial acceptance; no fake transport or root-zero claim.

use super::*;
use crate::resumable::tcp::{TcpOwnershipError, TcpReceiveWindow};
use bytes::Bytes;

#[test]
fn global_receive_budgets_reject_before_reservation_storage() {
    for range_limit in [false, true] {
        let (mut model, leg) = model_and_leg(SessionRole::Client);
        let mut flows = Vec::new();
        for _ in 0..if range_limit { 5 } else { 3 } {
            let opened = model
                .reduce(SessionEvent::LocalOpen {
                    leg,
                    target: crate::shared::TargetAddr::IpPort("127.0.0.1:443".parse().unwrap()),
                })
                .unwrap();
            let SessionEffect::LocalFlowOpened { flow } = opened[0] else {
                panic!("missing flow")
            };
            flows.push(flow);
        }
        let data = |flow: LocalFlow, offset, payload| SessionEvent::PeerFrame {
            leg,
            frame: Frame::new(
                leg.generation(),
                Record::Data {
                    flow_id: flow.flow_id(),
                    direction: Direction::TargetToClient,
                    offset: ByteOffset::new(offset),
                    payload,
                },
            ),
        };
        for flow in &flows[..flows.len() - 1] {
            if range_limit {
                for i in 0..8 {
                    model
                        .reduce(data(*flow, 2 * i + 1, Bytes::from_static(b"x")))
                        .unwrap();
                }
            } else {
                model
                    .reduce(data(*flow, 0, Bytes::from(vec![b'x'; 64])))
                    .unwrap();
            }
        }
        let last = *flows.last().unwrap();
        let before = (
            model.snapshot(),
            model.flow_snapshot(last.flow_id()).unwrap(),
        );
        TcpReceiveWindow::fail_next_receive_reservation_allocation_for_test();
        let rejected = model.reduce(data(last, 0, Bytes::from_static(b"x")));
        if range_limit {
            assert_eq!(
                rejected.unwrap_err(),
                SessionError::ReceiveRangeBudgetExceeded {
                    attempted: 33,
                    max: 32
                }
            );
        } else {
            assert_eq!(
                rejected.unwrap_err(),
                SessionError::ReceiveByteBudgetExceeded {
                    attempted: 129,
                    max: 128
                }
            );
        }
        assert_eq!(
            (
                model.snapshot(),
                model.flow_snapshot(last.flow_id()).unwrap()
            ),
            before
        );
        // A second genuine Model operation observes that the first rejection
        // did not consume the storage allocator's test-only single-use arm.
        let (mut fresh, fresh_leg, flow) = opened_model();
        let rejected = fresh.reduce(SessionEvent::PeerFrame {
            leg: fresh_leg,
            frame: Frame::new(
                fresh_leg.generation(),
                Record::Data {
                    flow_id: flow.flow_id(),
                    direction: Direction::TargetToClient,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"x"),
                },
            ),
        });
        assert!(matches!(
            rejected,
            Err(SessionError::TcpOwnership {
                source: TcpOwnershipError::ReceiveStorageReserve,
                ..
            })
        ));
    }
}

#[test]
fn offer_overflow_retains_original_ack_prefix_and_full_prepared_segments() {
    let (mut model, leg, flow) = opened_model();
    let inbound = |record| SessionEvent::PeerFrame {
        leg,
        frame: Frame::new(leg.generation(), record),
    };
    model
        .reduce(inbound(Record::OpenResult {
            flow_id: flow.flow_id(),
            result: OpenResultCode::Opened,
        }))
        .unwrap();
    let first = model
        .reduce(inbound(Record::Data {
            flow_id: flow.flow_id(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"abc"),
        }))
        .unwrap();
    let [SessionEffect::OfferToSink { offer, .. }] = first.as_slice() else {
        panic!("missing Offer")
    };
    let mut batch = SessionEffectBatch::prepared(1).unwrap();
    let rejected = model.reduce_into(
        SessionEvent::SinkAccepted {
            offer: *offer,
            bytes: 1,
        },
        &mut batch,
    );
    assert_eq!(rejected, Err(SessionError::EffectStorageOverflow));
    assert!(batch.has_overflow());
    assert!(batch.mutation_started());
    assert_eq!(batch.len(), 2);
    assert!(matches!(batch.get(0), Some(SessionEffect::Transmit(_))));
    assert!(
        matches!(batch.iter().nth(1), Some(SessionEffect::OfferToSink { segments, .. })
        if segments.len() == 1 && segments.capacity() == 1 && segments[0].payload().as_ref() == b"bc")
    );
    assert_eq!(model.snapshot().outstanding_sink_offers(), 1);
}

#[test]
fn receive_reservation_refusal_follows_validation_and_preserves_retry() {
    let (mut model, leg, flow) = opened_model();
    let data = |payload| SessionEvent::PeerFrame {
        leg,
        frame: Frame::new(
            leg.generation(),
            Record::Data {
                flow_id: flow.flow_id(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(0),
                payload,
            },
        ),
    };
    let before = (
        model.snapshot(),
        model.flow_snapshot(flow.flow_id()).unwrap(),
    );
    TcpReceiveWindow::fail_next_receive_reservation_allocation_for_test();
    assert!(model.reduce(data(Bytes::new())).is_err());
    let rejected = model.reduce(data(Bytes::from_static(b"abc")));
    assert!(matches!(
        rejected,
        Err(SessionError::TcpOwnership {
            source: TcpOwnershipError::ReceiveStorageReserve,
            ..
        })
    ));
    assert_eq!(
        (
            model.snapshot(),
            model.flow_snapshot(flow.flow_id()).unwrap()
        ),
        before
    );
    assert!(
        model
            .reduce(data(Bytes::from_static(b"abc")))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        model
            .flow_snapshot(flow.flow_id())
            .unwrap()
            .receive()
            .buffered_bytes(),
        3
    );
}

#[test]
fn backlogged_open_results_prepare_offer_before_either_role_records_success() {
    for role in [SessionRole::Client, SessionRole::Owner] {
        let (mut model, leg, flow_id, event, direction) = if role == SessionRole::Client {
            let (model, leg, flow) = opened_model();
            let event = SessionEvent::PeerFrame {
                leg,
                frame: Frame::new(
                    leg.generation(),
                    Record::OpenResult {
                        flow_id: flow.flow_id(),
                        result: OpenResultCode::Opened,
                    },
                ),
            };
            (model, leg, flow.flow_id(), event, Direction::TargetToClient)
        } else {
            let (mut model, leg) = model_and_leg(role);
            let flow_id = SessionFlowId::new(1).unwrap();
            let opened = model
                .reduce(SessionEvent::PeerFrame {
                    leg,
                    frame: Frame::new(
                        leg.generation(),
                        Record::Open {
                            flow_id,
                            target: crate::shared::TargetAddr::IpPort(
                                "127.0.0.1:443".parse().unwrap(),
                            ),
                        },
                    ),
                })
                .unwrap();
            let [SessionEffect::PeerOpenRequested { request, .. }] = opened.as_slice() else {
                panic!("missing request")
            };
            (
                model,
                leg,
                flow_id,
                SessionEvent::PeerOpenResolved {
                    request: *request,
                    result: OpenResultCode::Opened,
                },
                Direction::ClientToTarget,
            )
        };
        let buffered = model
            .reduce(SessionEvent::PeerFrame {
                leg,
                frame: Frame::new(
                    leg.generation(),
                    Record::Data {
                        flow_id,
                        direction,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"abc"),
                    },
                ),
            })
            .unwrap();
        assert!(buffered.is_empty());
        let before = (model.snapshot(), model.flow_snapshot(flow_id).unwrap());
        TcpReceiveWindow::fail_next_receive_offer_allocation_for_test();
        let rejected = model.reduce(event.clone());
        assert!(matches!(
            rejected,
            Err(SessionError::TcpOwnership {
                source: TcpOwnershipError::ReceiveStorageReserve,
                ..
            })
        ));
        assert_eq!(
            (model.snapshot(), model.flow_snapshot(flow_id).unwrap()),
            before
        );
        let result = model.reduce(event).unwrap();
        assert_eq!(result.len(), 2);
        assert!(
            matches!(result.last(), Some(SessionEffect::OfferToSink { segments, .. })
            if segments.len() == 1 && segments.capacity() == 1 && segments[0].payload().as_ref() == b"abc")
        );
    }
}

struct DisposalPanic;
impl AsRef<[u8]> for DisposalPanic {
    fn as_ref(&self) -> &[u8] {
        b"abc"
    }
}
impl Drop for DisposalPanic {
    fn drop(&mut self) {
        panic!("original receive input owner disposal");
    }
}

#[test]
fn input_disposal_unwind_follows_tcp_global_and_original_batch_publication() {
    let (mut model, leg, flow) = opened_model();
    model
        .reduce(SessionEvent::PeerFrame {
            leg,
            frame: Frame::new(
                leg.generation(),
                Record::OpenResult {
                    flow_id: flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            ),
        })
        .unwrap();
    let mut batch = SessionEffectBatch::empty();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        model
            .reduce_into(
                SessionEvent::PeerFrame {
                    leg,
                    frame: Frame::new(
                        leg.generation(),
                        Record::Data {
                            flow_id: flow.flow_id(),
                            direction: Direction::TargetToClient,
                            offset: ByteOffset::new(0),
                            payload: Bytes::from_owner(DisposalPanic),
                        },
                    ),
                },
                &mut batch,
            )
            .unwrap();
    }));
    assert!(result.is_err());
    assert!(batch.mutation_started());
    let effects = batch.iter().collect::<Vec<_>>();
    let [SessionEffect::OfferToSink { offer, segments }] = effects.as_slice() else {
        panic!("original batch must retain Offer")
    };
    assert_eq!(segments[0].payload().as_ref(), b"abc");
    assert_eq!(
        model
            .flow_snapshot(flow.flow_id())
            .unwrap()
            .receive()
            .buffered_bytes(),
        3
    );
    assert_eq!(model.snapshot().outstanding_sink_offers(), 1);
    // Full acceptance exercises exact global accounting, not only TCP fields.
    model
        .reduce(SessionEvent::SinkAccepted {
            offer: *offer,
            bytes: 3,
        })
        .unwrap();
    assert_eq!(model.snapshot().outstanding_sink_offers(), 0);
}

#[test]
fn receive_offer_refusal_preserves_data_and_partial_acceptance() {
    let (mut model, leg, flow) = opened_model();
    let inbound = |record| SessionEvent::PeerFrame {
        leg,
        frame: Frame::new(leg.generation(), record),
    };
    model
        .reduce(inbound(Record::OpenResult {
            flow_id: flow.flow_id(),
            result: OpenResultCode::Opened,
        }))
        .unwrap();
    let event = inbound(Record::Data {
        flow_id: flow.flow_id(),
        direction: Direction::TargetToClient,
        offset: ByteOffset::new(0),
        payload: Bytes::from_static(b"abcdef"),
    });
    let before = (
        model.snapshot(),
        model.flow_snapshot(flow.flow_id()).unwrap(),
    );
    TcpReceiveWindow::fail_next_receive_offer_allocation_for_test();
    let rejected = model.reduce(event.clone());
    assert!(matches!(
        rejected,
        Err(SessionError::TcpOwnership {
            source: TcpOwnershipError::ReceiveStorageReserve,
            ..
        })
    ));
    assert_eq!(
        (
            model.snapshot(),
            model.flow_snapshot(flow.flow_id()).unwrap()
        ),
        before
    );
    let offered = model.reduce(event).unwrap();
    let [SessionEffect::OfferToSink { offer, .. }] = offered.as_slice() else {
        panic!("missing Offer")
    };
    let event = SessionEvent::SinkAccepted {
        offer: *offer,
        bytes: 1,
    };
    let before = (
        model.snapshot(),
        model.flow_snapshot(flow.flow_id()).unwrap(),
    );
    TcpReceiveWindow::fail_next_receive_offer_allocation_for_test();
    let rejected = model.reduce(event.clone());
    assert!(matches!(
        rejected,
        Err(SessionError::TcpOwnership {
            source: TcpOwnershipError::ReceiveStorageReserve,
            ..
        })
    ));
    assert_eq!(
        (
            model.snapshot(),
            model.flow_snapshot(flow.flow_id()).unwrap()
        ),
        before
    );
    let accepted = model.reduce(event).unwrap();
    assert!(
        matches!(accepted.as_slice(), [SessionEffect::Transmit(_), SessionEffect::OfferToSink { segments, .. }]
        if segments.len() == 1 && segments.capacity() == 1 && segments[0].payload().as_ref() == b"bcdef")
    );
}

#[test]
fn receive_node_refusal_preserves_bridge_then_offers_accepts_and_reuses() {
    let (mut model, leg, flow) = opened_model();
    let inbound = |record| SessionEvent::PeerFrame {
        leg,
        frame: Frame::new(leg.generation(), record),
    };
    model
        .reduce(inbound(Record::OpenResult {
            flow_id: flow.flow_id(),
            result: OpenResultCode::Opened,
        }))
        .unwrap();
    let data = |offset, payload: &'static [u8]| {
        inbound(Record::Data {
            flow_id: flow.flow_id(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(offset),
            payload: Bytes::from_static(payload),
        })
    };
    for (offset, payload) in [(2, b"cd"), (6, b"gh")] {
        assert!(model.reduce(data(offset, payload)).unwrap().is_empty());
    }
    let before = model.snapshot();
    let flow_before = model.flow_snapshot(flow.flow_id()).unwrap();
    TcpReceiveWindow::fail_next_receive_node_allocation_for_test();
    let rejected = model.reduce(data(0, b"abcdefgh"));
    assert!(
        matches!(
            rejected,
            Err(SessionError::TcpOwnership {
                source: TcpOwnershipError::ReceiveStorageReserve,
                ..
            })
        ),
        "node preparation must refuse before bridge publication: {rejected:?}"
    );
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), flow_before);
    let offered = model.reduce(data(0, b"abcdefgh")).unwrap();
    let [SessionEffect::OfferToSink { offer, segments }] = offered.as_slice() else {
        panic!("bridge must publish the complete original contiguous Offer")
    };
    assert_eq!(segments.len(), 4);
    assert_eq!(segments.capacity(), 4);
    assert_eq!(
        segments
            .iter()
            .flat_map(|s| s.payload().iter().copied())
            .collect::<Vec<_>>(),
        b"abcdefgh"
    );
    let next = model
        .reduce(SessionEvent::SinkAccepted {
            offer: *offer,
            bytes: 1,
        })
        .unwrap();
    let [
        SessionEffect::Transmit(_),
        SessionEffect::OfferToSink { offer, segments },
    ] = next.as_slice()
    else {
        panic!("partial accept must publish ACK and original suffix Offer")
    };
    assert_eq!(
        segments
            .iter()
            .flat_map(|s| s.payload().iter().copied())
            .collect::<Vec<_>>(),
        b"bcdefgh"
    );
    model
        .reduce(SessionEvent::SinkAccepted {
            offer: *offer,
            bytes: 7,
        })
        .unwrap();
    TcpReceiveWindow::fail_next_receive_node_allocation_for_test();
    let reused = model.reduce(data(8, b"ij"));
    // Test-only single-use arm is cleared explicitly; reuse must leave it armed.
    assert!(TcpReceiveWindow::take_receive_node_failure_for_test());
    assert!(matches!(
        reused.unwrap().as_slice(),
        [SessionEffect::OfferToSink { .. }]
    ));
}

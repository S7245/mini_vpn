use super::*;
use crate::owned_upstream::TunAction;

#[test]
fn sink_acceptance_exports_exact_frontier_only_after_controller_reducer_commit() {
    let mut controller = initial_controller();
    let factory = controller
        .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
        .unwrap();
    let local_flow = controller
        .open_local(target())
        .unwrap()
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::LocalFlowOpened { flow } => Some(flow),
            _ => None,
        })
        .unwrap();
    let (mut flow, mut driver) = factory.open_flow(local_flow).unwrap();
    let route = controller.current_inbound_route().unwrap();
    let generation = controller.generation();

    assert!(matches!(
        controller.receive_current(
            classified_session_delivery(
                route,
                generation,
                Record::OpenResult {
                    flow_id: local_flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
                1,
            ),
            FeatureSet::new(FEATURES),
        ),
        Ok(ClientInboundDisposition::Applied { .. })
    ));
    let effects = match controller
        .receive_current(
            classified_session_delivery(
                route,
                generation,
                Record::Data {
                    flow_id: local_flow.flow_id(),
                    direction: Direction::TargetToClient,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"abcdef"),
                },
                2,
            ),
            FeatureSet::new(FEATURES),
        )
        .unwrap()
    {
        ClientInboundDisposition::Applied { effects, .. } => effects,
        _ => panic!("current DATA must commit and produce one sink offer"),
    };
    let (offer, segments) = effects
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::OfferToSink { offer, segments } => Some((offer, segments)),
            _ => None,
        })
        .expect("committed DATA must offer its contiguous bytes to the sink");
    driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("the exact sink offer must reach the flow");
    };
    assert!(delivery.complete_write(3).unwrap().is_none());
    let input = driver
        .try_recv_next_for_client_controller()
        .unwrap()
        .expect("positive sink acceptance reserves one typed reducer input");

    let applied = match controller.apply_driver_input_observed(input) {
        ClientControllerObservedDriverApply::Applied(applied) => applied,
        ClientControllerObservedDriverApply::Deferred { .. } => {
            panic!("sink acceptance unexpectedly met reducer pressure")
        }
        ClientControllerObservedDriverApply::Failed(error) => {
            panic!("sink acceptance unexpectedly failed: {error}")
        }
    };
    let (effects, observations) = applied.into_parts();
    let frontier = observations
        .sink_accepted_frontier()
        .expect("positive committed SinkAccepted must export one exact frontier");

    assert_eq!(frontier.session_id(), local_flow.session_id());
    assert_eq!(frontier.flow_id(), local_flow.flow_id());
    assert_eq!(frontier.generation(), generation);
    assert_eq!(frontier.direction(), Direction::TargetToClient);
    assert_eq!(frontier.previous_accepted(), ByteOffset::new(0));
    assert_eq!(frontier.next_accepted(), ByteOffset::new(3));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        SessionEffect::Transmit(frame)
            if matches!(frame.record(), Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted,
                final_accepted: false,
            } if *flow_id == local_flow.flow_id() && *next_accepted == ByteOffset::new(3))
    )));
}

#[test]
fn client_replay_and_positive_ack_export_exact_local_stall_facts() {
    let mut controller = initial_controller();
    let factory = controller
        .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
        .unwrap();
    let local_flow = controller
        .open_local(target())
        .unwrap()
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::LocalFlowOpened { flow } => Some(flow),
            _ => None,
        })
        .unwrap();
    let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
    let route = controller.current_inbound_route().unwrap();
    let generation = controller.generation();

    assert!(matches!(
        controller.receive_current(
            classified_session_delivery(
                route,
                generation,
                Record::OpenResult {
                    flow_id: local_flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
                1,
            ),
            FeatureSet::new(FEATURES),
        ),
        Ok(ClientInboundDisposition::Applied { .. })
    ));

    flow.try_send_uplink_with(3, || Bytes::from_static(b"abc"))
        .unwrap();
    let input = driver
        .try_recv_next_for_client_controller()
        .unwrap()
        .expect("one exact local DATA input");
    let applied = match controller.apply_driver_input_observed(input) {
        ClientControllerObservedDriverApply::Applied(applied) => applied,
        ClientControllerObservedDriverApply::Deferred { .. } => {
            panic!("local DATA unexpectedly met reducer pressure")
        }
        ClientControllerObservedDriverApply::Failed(error) => {
            panic!("local DATA unexpectedly failed: {error}")
        }
    };
    let (effects, observations) = applied.into_parts();
    let outstanding = observations
        .replay_became_outstanding()
        .expect("first committed local replay exports one exact outstanding fact");
    assert_eq!(outstanding.session_id(), local_flow.session_id());
    assert_eq!(outstanding.flow_id(), local_flow.flow_id());
    assert_eq!(outstanding.generation(), generation);
    assert_eq!(outstanding.direction(), Direction::ClientToTarget);
    assert_eq!(outstanding.start(), ByteOffset::new(0));
    assert_eq!(outstanding.end(), ByteOffset::new(3));
    assert!(outstanding.session_became_nonempty());
    assert!(observations.session_has_outstanding_replay());
    assert!(effects.iter().any(|effect| matches!(
        effect,
        SessionEffect::Transmit(frame)
            if matches!(frame.record(), Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset,
                payload,
            } if *flow_id == local_flow.flow_id()
                && *offset == ByteOffset::new(0)
                && payload.as_ref() == b"abc")
    )));

    let acked = controller
        .receive_current(
            classified_session_delivery(
                route,
                generation,
                Record::Ack {
                    flow_id: local_flow.flow_id(),
                    direction: Direction::ClientToTarget,
                    next_accepted: ByteOffset::new(3),
                    final_accepted: false,
                },
                2,
            ),
            FeatureSet::new(FEATURES),
        )
        .unwrap();
    assert!(matches!(
        acked,
        ClientInboundDisposition::Applied {
            observation: Some(ClientInboundObservation::ApplicationAckAdvanced {
                session_id,
                flow_id,
                generation: observed_generation,
                direction: Direction::ClientToTarget,
                previous_accepted,
                next_accepted,
                released_bytes: 3,
                remaining_oldest: None,
                session_has_outstanding_replay: false,
                ..
            }),
            ..
        } if session_id == local_flow.session_id()
            && flow_id == local_flow.flow_id()
            && observed_generation == generation
            && previous_accepted == ByteOffset::new(0)
            && next_accepted == ByteOffset::new(3)
    ));
}

#[test]
fn committed_local_reset_exports_exact_flow_direction_terminal_fact() {
    let mut controller = initial_controller();
    let factory = controller
        .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).unwrap())
        .unwrap();
    let local_flow = controller
        .open_local(target())
        .unwrap()
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::LocalFlowOpened { flow } => Some(flow),
            _ => None,
        })
        .unwrap();
    let (flow, mut driver) = factory.open_flow(local_flow).unwrap();
    let generation = controller.generation();

    flow.try_send_reset(ResetReason::LocalAbandon).unwrap();
    let input = driver
        .try_recv_next_for_client_controller()
        .unwrap()
        .expect("one exact local RESET input");
    let applied = match controller.apply_driver_input_observed(input) {
        ClientControllerObservedDriverApply::Applied(applied) => applied,
        ClientControllerObservedDriverApply::Deferred { .. } => {
            panic!("local RESET unexpectedly met reducer pressure")
        }
        ClientControllerObservedDriverApply::Failed(error) => {
            panic!("local RESET unexpectedly failed: {error}")
        }
    };
    let (effects, observations) = applied.into_parts();
    let terminal = observations
        .flow_direction_terminal()
        .expect("committed FlowFinished must export its exact terminal fact");

    assert_eq!(terminal.session_id(), local_flow.session_id());
    assert_eq!(terminal.flow_id(), local_flow.flow_id());
    assert_eq!(terminal.generation(), generation);
    assert_eq!(terminal.direction(), Direction::ClientToTarget);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        SessionEffect::FlowFinished {
            flow_id,
            terminal,
            ..
        }
            if *flow_id == local_flow.flow_id()
                && terminal.direction() == Direction::ClientToTarget
    )));
}

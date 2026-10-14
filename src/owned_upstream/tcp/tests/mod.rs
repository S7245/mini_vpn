use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
    AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
    Frame, LegGeneration, OpenResultCode, OwnerIdentity, ReceiveBudgetLimits, Record,
    ReplayBudgetLimits, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig, SessionEffect,
    SessionId, SessionModel, SessionRole, TcpWindowLimits, TlsExporterBinding, VersionRange,
};
use crate::shared::TargetAddr;

mod capacity;
mod owner_terminal;
struct Fixture {
    model: SessionModel,
    leg: crate::resumable::CommittedLeg,
    flow: LocalFlow,
}

fn test_leg_for_session(session_byte: u8) -> crate::resumable::CommittedLeg {
    let session_id = SessionId::new([session_byte; 16]).unwrap();
    let owner = OwnerIdentity::new([0x31; 32]).unwrap();
    let principal = DevicePrincipal::new([0x41; 16]).unwrap();
    let alpn = AttachAlpn::new(b"mini-vpn-owned/1").unwrap();
    let binding = AttachTransportBinding::new(
        owner,
        alpn,
        TlsExporterBinding::new([0x51; 32]).unwrap(),
        principal,
    );
    let credentials = AttachCredentials::new(
        DeviceSecret::new([0x61; 32]).unwrap(),
        ResumeSecret::new([0x71; 32]).unwrap(),
    )
    .unwrap();
    let request = AttachRequest::new(
        session_id,
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([0x81; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b11, 0b01).unwrap(),
    );
    let proof = credentials.prove(&request, &binding).unwrap();
    let authority = AttachAuthority::new(
        session_id,
        LegGeneration::new(1).unwrap(),
        credentials,
        AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b11).unwrap(),
    );
    authority
        .verify_and_commit(&request, &binding, &proof)
        .unwrap()
}

fn test_leg() -> crate::resumable::CommittedLeg {
    test_leg_for_session(0x21)
}

fn test_session_config() -> SessionConfig {
    SessionConfig::new(
        4,
        8,
        TcpWindowLimits::new(64, 8).unwrap(),
        TcpWindowLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReceiveBudgetLimits::new(128, 16).unwrap(),
        64,
    )
    .unwrap()
}

fn fixture_for_session(session_byte: u8) -> Fixture {
    let leg = test_leg_for_session(session_byte);
    let config = test_session_config();
    let mut model = SessionModel::new(SessionRole::Client, config, leg);
    let effects = model
        .reduce(SessionEvent::LocalOpen {
            leg,
            target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 443))),
        })
        .unwrap();
    let flow = effects
        .iter()
        .find_map(|effect| match effect {
            SessionEffect::LocalFlowOpened { flow } => Some(*flow),
            _ => None,
        })
        .unwrap();
    Fixture { model, leg, flow }
}

fn fixture() -> Fixture {
    fixture_for_session(0x21)
}

fn owner_fixture() -> Fixture {
    let leg = test_leg();
    let mut model = SessionModel::new(SessionRole::Owner, test_session_config(), leg);
    let flow_id = SessionFlowId::new(1).unwrap();
    let effects = model
        .reduce(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Open {
                    flow_id,
                    target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        443,
                    ))),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (request, flow) = effects
        .iter()
        .find_map(|effect| match effect {
            SessionEffect::PeerOpenRequested { request, flow, .. } => Some((*request, *flow)),
            _ => None,
        })
        .unwrap();
    model
        .reduce(SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::Opened,
        })
        .unwrap();
    Fixture { model, leg, flow }
}

fn port_config(
    bytes: usize,
    data_messages: usize,
    controls: usize,
    downlink: usize,
) -> FlowPortConfig {
    FlowPortConfig::new(bytes, data_messages, controls, downlink).unwrap()
}

fn isolated_pair(
    flow: LocalFlow,
    config: FlowPortConfig,
) -> (ResumableTcpFlow, ResumableTcpDriver) {
    ResumableTcpPortFactory::new_unbound_for_test(config, config.uplink_byte_capacity(), 8, 16)
        .unwrap()
        .open_flow(flow)
        .unwrap()
}

fn open_another_local_flow(fixture: &mut Fixture, port: u16) -> LocalFlow {
    fixture
        .model
        .reduce(SessionEvent::LocalOpen {
            leg: fixture.leg,
            target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                port,
            ))),
        })
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::LocalFlowOpened { flow } => Some(flow),
            _ => None,
        })
        .unwrap()
}

async fn assert_factory_progress_pending(
    factory: &ResumableTcpPortFactory,
    cursor: &TcpFactoryProgressCursor,
) {
    let wait = factory.wait_for_progress(cursor);
    tokio::pin!(wait);
    tokio::select! {
        biased;
        result = &mut wait => panic!("factory progress advanced unexpectedly: {result:?}"),
        () = tokio::task::yield_now() => {}
    }
}

async fn wait_for_factory_progress(
    factory: &ResumableTcpPortFactory,
    cursor: &TcpFactoryProgressCursor,
) -> TcpFactoryProgressCursor {
    tokio::time::timeout(Duration::from_secs(1), factory.wait_for_progress(cursor))
        .await
        .expect("factory progress edge must not busy-wait or hang")
        .expect("factory progress cursor must retain its exact source")
}

fn take_replay_stored(effects: &mut Vec<SessionEffect>) -> ReplayStored {
    let index = effects
        .iter()
        .position(|effect| matches!(effect, SessionEffect::ReplayStored { .. }))
        .expect("LocalData must return replay storage authority");
    match effects.remove(index) {
        SessionEffect::ReplayStored { receipt } => receipt,
        _ => unreachable!("located exact ReplayStored variant"),
    }
}

fn take_replay_acknowledged(effects: &mut Vec<SessionEffect>) -> ReplayAcknowledged {
    let index = effects
        .iter()
        .position(|effect| matches!(effect, SessionEffect::ReplayAcknowledged { .. }))
        .expect("accepted peer ACK must return replay release authority");
    match effects.remove(index) {
        SessionEffect::ReplayAcknowledged { receipt } => receipt,
        _ => unreachable!("located exact ReplayAcknowledged variant"),
    }
}

fn open_peer(fixture: &mut Fixture) {
    let frame = Frame::try_new(
        fixture.leg.generation(),
        Record::OpenResult {
            flow_id: fixture.flow.flow_id(),
            result: OpenResultCode::Opened,
        },
    )
    .unwrap();
    fixture
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: fixture.leg,
            frame,
        })
        .unwrap();
}

fn sink_offer(fixture: &mut Fixture, payload: &[u8]) -> (SinkOffer, Vec<TcpDataSegment>) {
    open_peer(fixture);
    let frame = Frame::try_new(
        fixture.leg.generation(),
        Record::Data {
            flow_id: fixture.flow.flow_id(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::copy_from_slice(payload),
        },
    )
    .unwrap();
    let effects = fixture
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: fixture.leg,
            frame,
        })
        .unwrap();
    effects
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::OfferToSink { offer, segments } => Some((offer, segments)),
            _ => None,
        })
        .unwrap()
}

fn half_close(fixture: &mut Fixture) -> SinkHalfClose {
    open_peer(fixture);
    let frame = Frame::try_new(
        fixture.leg.generation(),
        Record::Close {
            flow_id: fixture.flow.flow_id(),
            direction: Direction::TargetToClient,
            final_offset: ByteOffset::new(0),
        },
    )
    .unwrap();
    fixture
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: fixture.leg,
            frame,
        })
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::HalfCloseSink { completion } => Some(completion),
            _ => None,
        })
        .unwrap()
}

fn owner_half_close(fixture: &mut Fixture) -> SinkHalfClose {
    let frame = Frame::try_new(
        fixture.leg.generation(),
        Record::Close {
            flow_id: fixture.flow.flow_id(),
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(0),
        },
    )
    .unwrap();
    fixture
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: fixture.leg,
            frame,
        })
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::HalfCloseSink { completion } => Some(completion),
            _ => None,
        })
        .unwrap()
}

fn flow_finished(fixture: &mut Fixture) -> SessionEffect {
    fixture
        .model
        .reduce(SessionEvent::LocalReset {
            flow: fixture.flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap()
        .into_iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .unwrap()
}

#[test]
fn replay_segment_factory_exposes_exact_reducer_aligned_capacities() {
    let config = port_config(8, 1, 1, 1);
    let factory = ResumableTcpPortFactory::new_unbound_for_test(config, 32, 3, 11).unwrap();
    assert_eq!(factory.per_flow_uplink_segment_capacity(), 3);
    assert_eq!(factory.session_uplink_segment_capacity(), 11);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
    assert_eq!(
        ResumableTcpPortFactory::new_unbound_for_test(config, 32, 0, 11).unwrap_err(),
        FlowPortConfigError::ZeroUplinkSegmentCapacity
    );
    assert_eq!(
        ResumableTcpPortFactory::new_unbound_for_test(config, 32, 3, 0).unwrap_err(),
        FlowPortConfigError::ZeroSessionUplinkSegmentCapacity
    );
}

#[tokio::test(flavor = "current_thread")]
async fn factory_progress_is_shared_cross_flow_factory_bound_and_cancellation_safe() {
    let mut fixture = fixture();
    let second_local = open_another_local_flow(&mut fixture, 8443);
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 2, 2, 2), 16, 8, 16).unwrap();
    let (first, mut first_driver) = factory.open_flow(fixture.flow).unwrap();
    let (second, mut second_driver) = factory.open_flow(second_local).unwrap();

    let first_cursor = factory.progress_cursor();
    assert_factory_progress_pending(&factory, &first_cursor).await;
    first
        .try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .unwrap();
    let _after_first = wait_for_factory_progress(&factory, &first_cursor).await;
    assert!(matches!(
        first_driver.try_recv_next().unwrap(),
        Some(DriverInput::Data(_))
    ));

    let after_data_release = factory.progress_cursor();
    assert_factory_progress_pending(&factory, &after_data_release).await;
    first.try_send_close().unwrap();
    let after_close = wait_for_factory_progress(&factory, &after_data_release).await;
    assert!(matches!(
        first_driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::LocalClose { .. }))
    ));

    // Waiting on the shared factory cursor observes another flow, and
    // cancelling the first waiter does not consume the later reset edge.
    assert_factory_progress_pending(&factory, &after_close).await;
    second.try_send_reset(ResetReason::LocalAbandon).unwrap();
    let after_second = wait_for_factory_progress(&factory, &after_close).await;
    assert!(matches!(
        second_driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::LocalReset { .. }))
    ));
    assert_factory_progress_pending(&factory, &after_second).await;

    let other_factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 8, 8).unwrap();
    assert_eq!(
        other_factory
            .wait_for_progress(&after_second)
            .await
            .unwrap_err(),
        TcpFactoryProgressWaitError::WrongFactory
    );
}

#[tokio::test(flavor = "current_thread")]
async fn factory_progress_covers_control_capacity_release_terminal_dequeue_and_endpoint_drop() {
    let mut fixture = fixture();
    let second_local = open_another_local_flow(&mut fixture, 8443);
    let third_local = open_another_local_flow(&mut fixture, 9443);
    let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
    let retry_segments = segments.clone();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 2, 2, 1), 24, 8, 24).unwrap();
    let (mut first, mut first_driver) = factory.open_flow(fixture.flow).unwrap();
    let (second, _second_driver) = factory.open_flow(second_local).unwrap();
    let (third, _third_driver) = factory.open_flow(third_local).unwrap();

    first_driver.try_deliver_sink(offer, segments).unwrap();
    let before_failed_completion_release = factory.progress_cursor();
    assert_factory_progress_pending(&factory, &before_failed_completion_release).await;
    assert_eq!(
        first_driver
            .try_deliver_sink(offer, retry_segments)
            .unwrap_err(),
        FlowPortError::DownlinkLaneFull
    );
    let after_failed_completion_release =
        wait_for_factory_progress(&factory, &before_failed_completion_release).await;
    assert_eq!(
        first_driver.probe().snapshot().reserved_completion_slots(),
        1
    );

    assert_factory_progress_pending(&factory, &after_failed_completion_release).await;
    let TunAction::Sink(delivery) = first.try_recv_action().unwrap().unwrap() else {
        panic!("expected sink delivery")
    };
    let after_downlink_dequeue =
        wait_for_factory_progress(&factory, &after_failed_completion_release).await;

    // The local completion both releases its reserved slot and publishes
    // one control fact for the driver without a polling loop.
    assert_factory_progress_pending(&factory, &after_downlink_dequeue).await;
    delivery.abandon();
    let after_completion = wait_for_factory_progress(&factory, &after_downlink_dequeue).await;
    assert_eq!(
        first_driver.probe().snapshot().reserved_completion_slots(),
        0
    );
    assert!(matches!(
        first_driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::SinkAbandoned { .. }))
    ));

    let terminal_effects = fixture
        .model
        .reduce(SessionEvent::LocalReset {
            flow: fixture.flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();
    let terminal = terminal_effects
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .unwrap();
    first_driver.try_deliver_terminal_effect(terminal).unwrap();
    let before_terminal_dequeue = factory.progress_cursor();
    assert_factory_progress_pending(&factory, &before_terminal_dequeue).await;
    let action = first.try_recv_action().unwrap().unwrap();
    assert!(matches!(action, TunAction::Terminal(_)));
    let after_terminal_dequeue =
        wait_for_factory_progress(&factory, &before_terminal_dequeue).await;
    drop(action);

    // Split endpoints retain the same factory identity. Dropping either
    // local half publishes a fresh edge after its channel endpoint closes.
    let (second_uplink, second_downlink) = second.into_split();
    let before_downlink_drop = factory.progress_cursor();
    drop(second_downlink);
    let after_downlink_drop = wait_for_factory_progress(&factory, &before_downlink_drop).await;
    drop(second_uplink);
    let after_uplink_drop = wait_for_factory_progress(&factory, &after_downlink_drop).await;

    assert_factory_progress_pending(&factory, &after_uplink_drop).await;
    drop(third);
    let _after_unsplit_drop = wait_for_factory_progress(&factory, &after_uplink_drop).await;

    // Keep the intermediate cursor live through the terminal branch so a
    // compiler cannot optimize the observed edge into a mere final check.
    drop(after_terminal_dequeue);
    drop(after_completion);
}

#[tokio::test(flavor = "current_thread")]
async fn factory_progress_covers_source_rollback_and_every_uplink_permit_release() {
    let mut fixture = fixture();
    let ack_flow = open_another_local_flow(&mut fixture, 8443);
    let terminal_flow = open_another_local_flow(&mut fixture, 9443);
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 2, 2, 1), 8, 8, 24).unwrap();
    let (drop_endpoint, mut drop_driver) = factory.open_flow(fixture.flow).unwrap();
    let (ack_endpoint, mut ack_driver) = factory.open_flow(ack_flow).unwrap();
    let (terminal_endpoint, mut terminal_driver) = factory.open_flow(terminal_flow).unwrap();

    let reservation = drop_endpoint.try_reserve_uplink_source(4).unwrap();
    let before_reservation_rollback = factory.progress_cursor();
    assert_factory_progress_pending(&factory, &before_reservation_rollback).await;
    drop(reservation);
    let _after_reservation_rollback =
        wait_for_factory_progress(&factory, &before_reservation_rollback).await;
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);

    drop_endpoint
        .try_send_uplink_with(8, || Bytes::from_static(b"dropdata"))
        .unwrap();
    let Some(DriverInput::Data(dropped_data)) = drop_driver.try_recv_next().unwrap() else {
        panic!("expected DATA ownership for the error-drop path")
    };

    // The second flow can reserve its own slot and bytes before failing on
    // the already-full session ledger. Every partial acquisition must be
    // gone before the shared rollback edge is published.
    let before_partial_rollback = factory.progress_cursor();
    assert_factory_progress_pending(&factory, &before_partial_rollback).await;
    assert_eq!(
        ack_endpoint.try_reserve_uplink_source(1).unwrap_err(),
        FlowPortError::UplinkGlobalByteBudgetExhausted {
            requested: 1,
            available: 0,
        }
    );
    let after_partial_rollback =
        wait_for_factory_progress(&factory, &before_partial_rollback).await;
    assert_eq!(ack_endpoint.probe().snapshot().uplink_owned_bytes(), 0);
    assert_eq!(ack_endpoint.probe().snapshot().uplink_owned_segments(), 0);
    assert_eq!(factory.session_uplink_owned_bytes(), 8);

    let before_error_drop = after_partial_rollback;
    assert_factory_progress_pending(&factory, &before_error_drop).await;
    drop(dropped_data);
    let _after_error_drop = wait_for_factory_progress(&factory, &before_error_drop).await;
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);

    ack_endpoint
        .try_send_uplink_with(1, || Bytes::from_static(b"a"))
        .unwrap();
    let Some(DriverInput::Data(ack_data)) = ack_driver.try_recv_next().unwrap() else {
        panic!("expected DATA ownership for the ACK path")
    };
    let (ack_event, mut ack_ownership) = ack_data.into_event_and_ownership();
    let mut ack_effects = fixture.model.reduce(ack_event).unwrap();
    let mut ack_replay = ack_ownership
        .bind_replay(take_replay_stored(&mut ack_effects))
        .unwrap();
    let mut accepted = fixture
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: fixture.leg,
            frame: Frame::try_new(
                fixture.leg.generation(),
                Record::Ack {
                    flow_id: ack_flow.flow_id(),
                    direction: Direction::ClientToTarget,
                    next_accepted: ByteOffset::new(1),
                    final_accepted: false,
                },
            )
            .unwrap(),
        })
        .unwrap();
    let accepted = take_replay_acknowledged(&mut accepted);
    let before_ack_release = factory.progress_cursor();
    assert_factory_progress_pending(&factory, &before_ack_release).await;
    assert!(ack_replay.release_after_reducer_ack(&accepted).unwrap());
    let _after_ack_release = wait_for_factory_progress(&factory, &before_ack_release).await;
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);

    terminal_endpoint
        .try_send_uplink_with(1, || Bytes::from_static(b"t"))
        .unwrap();
    let Some(DriverInput::Data(terminal_data)) = terminal_driver.try_recv_next().unwrap() else {
        panic!("expected DATA ownership for the terminal path")
    };
    let (terminal_event, mut terminal_ownership) = terminal_data.into_event_and_ownership();
    let mut terminal_effects = fixture.model.reduce(terminal_event).unwrap();
    let mut terminal_replay = terminal_ownership
        .bind_replay(take_replay_stored(&mut terminal_effects))
        .unwrap();
    let terminal_effects = fixture
        .model
        .reduce(SessionEvent::LocalReset {
            flow: terminal_flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();
    let terminal = terminal_effects
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .unwrap();
    let before_terminal_release = factory.progress_cursor();
    assert_factory_progress_pending(&factory, &before_terminal_release).await;
    assert!(
        terminal_replay
            .release_after_reducer_terminal(terminal)
            .unwrap()
    );
    let _after_terminal_release =
        wait_for_factory_progress(&factory, &before_terminal_release).await;
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
}

#[test]
fn cloned_factory_rejects_duplicate_local_flow_authority_before_source_admission() {
    let fixture = fixture();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 16, 8, 16).unwrap();
    let duplicate_caller = factory.clone();

    let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
    assert!(matches!(
        duplicate_caller.open_flow(fixture.flow),
        Err(FlowPortError::LocalFlowAuthorityNotMonotonic {
            highest: 1,
            attempted: 1,
        })
    ));
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);

    flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .unwrap();
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Data(data)) if data.payload() == b"x"
    ));
}

#[test]
fn factory_admission_close_is_idempotent_and_shared_by_every_clone() {
    let fixture = fixture();
    let (factory, mut admission) = ResumableTcpPortFactory::new_unbound_with_admission_for_test(
        port_config(8, 1, 1, 1),
        16,
        8,
        16,
    )
    .unwrap();
    let stale_clone = factory.clone();

    assert_eq!(admission.close(), TcpFactoryAdmissionClose::ClosedNow);
    assert_eq!(admission.close(), TcpFactoryAdmissionClose::AlreadyClosed);
    assert!(matches!(
        factory.open_flow(fixture.flow),
        Err(FlowPortError::FactoryAdmissionClosed)
    ));
    assert!(matches!(
        stale_clone.open_flow(fixture.flow),
        Err(FlowPortError::FactoryAdmissionClosed)
    ));
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
}

#[test]
fn flow_linearized_before_factory_close_remains_owned_while_later_open_is_rejected() {
    let mut fixture = fixture();
    let second_flow = open_another_local_flow(&mut fixture, 8443);
    let (factory, mut admission) = ResumableTcpPortFactory::new_unbound_with_admission_for_test(
        port_config(8, 1, 1, 1),
        16,
        8,
        16,
    )
    .unwrap();
    let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();

    assert_eq!(admission.close(), TcpFactoryAdmissionClose::ClosedNow);
    assert!(matches!(
        factory.open_flow(second_flow),
        Err(FlowPortError::FactoryAdmissionClosed)
    ));

    flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .unwrap();
    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("the pre-close flow must remain under its driver owner");
    };
    assert_eq!(data.payload(), b"x");
    drop(data);
    drop(flow);
    drop(driver);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
}

#[test]
fn factory_close_cannot_cross_an_admitted_flow_construction() {
    let fixture = fixture();
    let flow = fixture.flow;
    let (factory, mut admission) = ResumableTcpPortFactory::new_unbound_with_admission_for_test(
        port_config(8, 1, 1, 1),
        16,
        8,
        16,
    )
    .unwrap();
    let opener = factory.clone();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let open_task = std::thread::spawn(move || {
        opener.with_flow_admission(flow, || {
            entered_tx
                .send(())
                .expect("construction publishes admission");
            release_rx
                .recv()
                .expect("test releases admitted construction");
        })
    });

    entered_rx
        .recv()
        .expect("admitted construction reaches the deterministic gate");
    assert!(matches!(
        admission.flow_gate.try_lock(),
        Err(std::sync::TryLockError::WouldBlock),
    ));
    release_tx.send(()).expect("release admitted construction");
    open_task
        .join()
        .expect("admitted construction thread joins")
        .expect("pre-close admission completes");

    assert_eq!(admission.close(), TcpFactoryAdmissionClose::ClosedNow);
    assert!(matches!(
        factory.open_flow(flow),
        Err(FlowPortError::FactoryAdmissionClosed)
    ));
}

#[test]
fn factory_binding_rejects_cross_session_and_cross_direction_capabilities() {
    let session = SessionId::new([0x91; 16]).unwrap();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 16, 8, 16).unwrap();
    let first = LocalFlow::for_adapter_test(
        session,
        SessionFlowId::new(1).unwrap(),
        Direction::ClientToTarget,
        1,
    );
    factory.open_flow(first).unwrap();

    let cross_session = LocalFlow::for_adapter_test(
        SessionId::new([0x92; 16]).unwrap(),
        SessionFlowId::new(2).unwrap(),
        Direction::ClientToTarget,
        2,
    );
    assert!(matches!(
        factory.open_flow(cross_session),
        Err(FlowPortError::CrossSessionFlowFactory)
    ));

    let cross_direction = LocalFlow::for_adapter_test(
        session,
        SessionFlowId::new(2).unwrap(),
        Direction::TargetToClient,
        2,
    );
    assert!(matches!(
        factory.open_flow(cross_direction),
        Err(FlowPortError::FlowFactoryDirectionMismatch {
            expected: Direction::ClientToTarget,
            actual: Direction::TargetToClient,
        })
    ));
}

#[test]
fn factory_accepts_authority_gaps_but_rejects_a_later_stale_capability() {
    let session = SessionId::new([0x93; 16]).unwrap();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 16, 8, 16).unwrap();
    let flow = |flow_id, authority_id| {
        LocalFlow::for_adapter_test(
            session,
            SessionFlowId::new(flow_id).unwrap(),
            Direction::ClientToTarget,
            authority_id,
        )
    };

    factory.open_flow(flow(1, 1)).unwrap();
    factory.open_flow(flow(3, 3)).unwrap();
    assert!(matches!(
        factory.open_flow(flow(2, 2)),
        Err(FlowPortError::LocalFlowAuthorityNotMonotonic {
            highest: 3,
            attempted: 2,
        })
    ));
}

#[test]
fn session_uplink_ledger_backpressures_across_flows_before_extraction() {
    let mut fixture = fixture();
    let second_flow = open_another_local_flow(&mut fixture, 8443);
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(4, 1, 1, 1), 4, 8, 16).unwrap();
    let (first, mut first_driver) = factory.open_flow(fixture.flow).unwrap();
    let (second, _second_driver) = factory.open_flow(second_flow).unwrap();
    let calls = AtomicUsize::new(0);

    first
        .try_send_uplink_with(4, || Bytes::from_static(b"full"))
        .unwrap();
    assert_eq!(factory.session_uplink_owned_bytes(), 4);
    assert_eq!(
        second
            .try_send_uplink_with(1, || {
                calls.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"x")
            })
            .unwrap_err(),
        FlowPortError::UplinkGlobalByteBudgetExhausted {
            requested: 1,
            available: 0,
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let Some(DriverInput::Data(first_data)) = first_driver.try_recv_next().unwrap() else {
        panic!("expected first flow DATA ownership")
    };
    drop(first_data);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    second
        .try_send_uplink_with(1, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"x")
        })
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(factory.session_uplink_owned_bytes(), 1);
}

#[test]
fn replay_storage_receipt_rejects_equal_length_wrong_payload() {
    let mut fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 1, 1));

    // First advance both the port's source extent and the reducer's replay
    // extent in lockstep. The retained replay ownership intentionally
    // stays live while the second equal-sized extent is checked.
    flow.try_send_uplink_with(4, || Bytes::from_static(b"good"))
        .unwrap();
    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("expected admitted DATA")
    };
    let (first_event, mut first_ownership) = data.into_event_and_ownership();
    let mut first_effects = fixture.model.reduce(first_event).unwrap();
    let first_receipt = take_replay_stored(&mut first_effects);
    let first_replay = first_ownership.bind_replay(first_receipt).unwrap();
    assert_eq!(first_replay.start(), ByteOffset::new(0));
    assert_eq!(first_replay.end(), ByteOffset::new(4));

    flow.try_send_uplink_with(4, || Bytes::from_static(b"good"))
        .unwrap();
    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("expected second admitted DATA")
    };
    let (_real_event, mut ownership) = data.into_event_and_ownership();

    // Mint a genuine reducer receipt for the same flow, direction, range,
    // and length but different bytes. Metadata equality alone must never
    // transfer the second port permit to this replay extent.
    let wrong_receipt = fixture
        .model
        .reduce(SessionEvent::LocalData {
            flow: fixture.flow,
            payload: Bytes::from_static(b"evil"),
        })
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::ReplayStored { receipt } => Some(receipt),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        ownership.bind_replay(wrong_receipt).unwrap_err(),
        FlowPortError::UplinkReplayReceiptMismatch
    );
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 8);
    drop(ownership);
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);
    assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 1);
    drop(first_replay);
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
}

#[test]
fn replay_storage_receipt_rejects_wrong_direction() {
    let client = fixture();
    let mut owner = owner_fixture();
    assert_eq!(client.flow.session_id(), owner.flow.session_id());
    assert_eq!(client.flow.flow_id(), owner.flow.flow_id());
    assert_ne!(client.flow.direction(), owner.flow.direction());

    let (flow, mut driver) = isolated_pair(client.flow, port_config(4, 1, 1, 1));
    flow.try_send_uplink_with(4, || Bytes::from_static(b"same"))
        .unwrap();
    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("expected admitted DATA")
    };
    let (_event, mut ownership) = data.into_event_and_ownership();
    let wrong_direction = owner
        .model
        .reduce(SessionEvent::LocalData {
            flow: owner.flow,
            payload: Bytes::from_static(b"same"),
        })
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::ReplayStored { receipt } => Some(receipt),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        ownership.bind_replay(wrong_direction).unwrap_err(),
        FlowPortError::UplinkReplayReceiptMismatch
    );
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);
    drop(ownership);
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
}

#[test]
fn identical_payload_receipt_cannot_bind_a_different_source_extent() {
    let mut fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 1, 1));
    flow.try_send_uplink_with(4, || Bytes::from_static(b"same"))
        .unwrap();
    flow.try_send_uplink_with(4, || Bytes::from_static(b"same"))
        .unwrap();
    let Some(DriverInput::Data(first)) = driver.try_recv_next().unwrap() else {
        panic!("expected first identical DATA")
    };
    let Some(DriverInput::Data(second)) = driver.try_recv_next().unwrap() else {
        panic!("expected second identical DATA")
    };
    let (first_event, first_ownership) = first.into_event_and_ownership();
    let (second_event, mut second_ownership) = second.into_event_and_ownership();
    let mut first_effects = fixture.model.reduce(first_event).unwrap();
    let first_receipt = take_replay_stored(&mut first_effects);
    let mut second_effects = fixture.model.reduce(second_event).unwrap();
    let second_receipt = take_replay_stored(&mut second_effects);

    assert_eq!(
        second_ownership.bind_replay(first_receipt).unwrap_err(),
        FlowPortError::UplinkReplayReceiptMismatch
    );
    let second_replay = second_ownership.bind_replay(second_receipt).unwrap();
    assert_eq!(second_replay.start(), ByteOffset::new(4));
    assert_eq!(second_replay.end(), ByteOffset::new(8));
    drop(first_ownership);
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);
    drop(second_replay);
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
}

#[test]
fn uplink_reserves_message_and_exact_bytes_before_extraction() {
    let mut fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(4, 1, 2, 1));
    let calls = AtomicUsize::new(0);

    flow.try_send_uplink_with(4, || {
        calls.fetch_add(1, Ordering::SeqCst);
        Bytes::from_static(b"data")
    })
    .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);

    let error = flow
        .try_send_uplink_with(1, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"x")
        })
        .unwrap_err();
    assert_eq!(error, FlowPortError::UplinkMessageLaneFull);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let DriverInput::Data(data) = driver.try_recv_next().unwrap().unwrap() else {
        panic!("expected DATA")
    };
    let (event, mut ownership) = data.into_event_and_ownership();
    let error = flow
        .try_send_uplink_with(1, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"x")
        })
        .unwrap_err();
    assert_eq!(
        error,
        FlowPortError::UplinkByteBudgetExhausted {
            requested: 1,
            available: 0,
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut stored_effects = fixture.model.reduce(event).unwrap();
    let stored = take_replay_stored(&mut stored_effects);
    let mut replay = ownership.bind_replay(stored).unwrap();
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);

    let partial_ack = Frame::try_new(
        fixture.leg.generation(),
        Record::Ack {
            flow_id: fixture.flow.flow_id(),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(2),
            final_accepted: false,
        },
    )
    .unwrap();
    let mut partial_effects = fixture
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: fixture.leg,
            frame: partial_ack,
        })
        .unwrap();
    let partial_receipt = take_replay_acknowledged(&mut partial_effects);
    assert!(!replay.release_after_reducer_ack(&partial_receipt).unwrap());
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);

    let full_ack = Frame::try_new(
        fixture.leg.generation(),
        Record::Ack {
            flow_id: fixture.flow.flow_id(),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(4),
            final_accepted: false,
        },
    )
    .unwrap();
    let mut full_effects = fixture
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: fixture.leg,
            frame: full_ack,
        })
        .unwrap();
    let full_receipt = take_replay_acknowledged(&mut full_effects);
    assert!(replay.release_after_reducer_ack(&full_receipt).unwrap());
    assert!(!replay.release_after_reducer_ack(&full_receipt).unwrap());
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
}

#[test]
fn replay_segment_permits_survive_source_dequeue_until_exact_ack() {
    let mut fixture = fixture();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 2, 1), 8, 2, 2).unwrap();
    let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
    let mut retained = Vec::new();

    for payload in [b"a".as_slice(), b"b".as_slice()] {
        flow.try_send_uplink_with(1, || Bytes::copy_from_slice(payload))
            .unwrap();
        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected admitted DATA")
        };
        let (event, mut ownership) = data.into_event_and_ownership();
        let mut effects = fixture.model.reduce(event).unwrap();
        retained.push(
            ownership
                .bind_replay(take_replay_stored(&mut effects))
                .unwrap(),
        );
    }
    assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 2);
    assert_eq!(factory.session_uplink_owned_segments(), 2);

    let extractions = AtomicUsize::new(0);
    assert_eq!(
        flow.try_send_uplink_with(1, || {
            extractions.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"c")
        })
        .unwrap_err(),
        FlowPortError::UplinkSegmentBudgetExhausted { available: 0 }
    );
    assert_eq!(extractions.load(Ordering::SeqCst), 0);

    let acknowledged = Frame::try_new(
        fixture.leg.generation(),
        Record::Ack {
            flow_id: fixture.flow.flow_id(),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(1),
            final_accepted: false,
        },
    )
    .unwrap();
    let mut effects = fixture
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: fixture.leg,
            frame: acknowledged,
        })
        .unwrap();
    assert!(
        retained[0]
            .release_after_reducer_ack(&take_replay_acknowledged(&mut effects))
            .unwrap()
    );
    assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 1);

    flow.try_send_uplink_with(1, || {
        extractions.fetch_add(1, Ordering::SeqCst);
        Bytes::from_static(b"c")
    })
    .unwrap();
    assert_eq!(extractions.load(Ordering::SeqCst), 1);
}

#[test]
fn replay_segment_limits_are_per_flow_and_session_global_before_extraction() {
    let mut fixture = fixture();
    let second_flow = open_another_local_flow(&mut fixture, 8443);
    let third_flow = open_another_local_flow(&mut fixture, 9443);
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 2, 1), 8, 1, 2).unwrap();
    let (first, mut first_driver) = factory.open_flow(fixture.flow).unwrap();
    let (second, mut second_driver) = factory.open_flow(second_flow).unwrap();
    let (third, mut third_driver) = factory.open_flow(third_flow).unwrap();
    let extractions = AtomicUsize::new(0);

    first
        .try_send_uplink_with(1, || {
            extractions.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"a")
        })
        .unwrap();
    let Some(DriverInput::Data(first_data)) = first_driver.try_recv_next().unwrap() else {
        panic!("expected first DATA")
    };
    assert_eq!(
        first
            .try_send_uplink_with(1, || {
                extractions.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"x")
            })
            .unwrap_err(),
        FlowPortError::UplinkSegmentBudgetExhausted { available: 0 }
    );

    second
        .try_send_uplink_with(1, || {
            extractions.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"b")
        })
        .unwrap();
    let Some(DriverInput::Data(second_data)) = second_driver.try_recv_next().unwrap() else {
        panic!("expected second DATA")
    };
    assert_eq!(factory.session_uplink_owned_segments(), 2);
    assert_eq!(
        third
            .try_send_uplink_with(1, || {
                extractions.fetch_add(1, Ordering::SeqCst);
                Bytes::from_static(b"c")
            })
            .unwrap_err(),
        FlowPortError::UplinkGlobalSegmentBudgetExhausted { available: 0 }
    );
    assert_eq!(extractions.load(Ordering::SeqCst), 2);

    drop(first_data);
    assert_eq!(factory.session_uplink_owned_segments(), 1);
    third
        .try_send_uplink_with(1, || {
            extractions.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"c")
        })
        .unwrap();
    assert!(matches!(
        third_driver.try_recv_next().unwrap(),
        Some(DriverInput::Data(_))
    ));
    assert_eq!(extractions.load(Ordering::SeqCst), 3);
    drop(second_data);
}

#[test]
fn source_reservation_commits_eof_through_its_pre_reserved_fifo_slot() {
    let fixture = fixture();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 1, 1).unwrap();
    let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
    let eof_reads = AtomicUsize::new(0);

    let reservation = flow.try_reserve_uplink_source(8).unwrap();
    eof_reads.fetch_add(1, Ordering::SeqCst);
    assert_eq!(
        flow.try_send_close().unwrap_err(),
        FlowPortError::UplinkSourceReservationActive
    );
    reservation.commit_eof().unwrap();

    assert_eq!(eof_reads.load(Ordering::SeqCst), 1);
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
    assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 0);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
    assert_eq!(flow.try_send_close().unwrap_err(), FlowPortError::Closed);
    let post_eof_extractions = AtomicUsize::new(0);
    assert_eq!(
        flow.try_send_uplink_with(1, || {
            post_eof_extractions.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"late")
        })
        .unwrap_err(),
        FlowPortError::Closed
    );
    assert_eq!(post_eof_extractions.load(Ordering::SeqCst), 0);
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::LocalClose { flow: observed }))
            if observed == fixture.flow
    ));
    assert!(driver.try_recv_next().unwrap().is_none());
}

#[test]
fn committed_eof_cannot_overtake_preceding_data_on_a_one_slot_lane() {
    let fixture = fixture();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 2, 2).unwrap();
    let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
    let eof_reads = AtomicUsize::new(0);

    flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .unwrap();
    assert!(matches!(
        flow.try_reserve_uplink_source(1),
        Err(FlowPortError::UplinkMessageLaneFull)
    ));
    assert_eq!(eof_reads.load(Ordering::SeqCst), 0);

    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("preceding DATA must be observed before EOF can reserve its slot")
    };
    let reservation = flow.try_reserve_uplink_source(1).unwrap();
    eof_reads.fetch_add(1, Ordering::SeqCst);
    reservation.commit_eof().unwrap();

    assert_eq!(data.payload(), b"x");
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::LocalClose { flow: observed }))
            if observed == fixture.flow
    ));
    assert_eq!(eof_reads.load(Ordering::SeqCst), 1);
}

#[test]
fn uplink_source_reservation_shrinks_max_budget_to_committed_bytes() {
    let mut fixture = fixture();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 8, 16).unwrap();
    let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();

    let reservation = flow.try_reserve_uplink_source(8).unwrap();
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 8);
    assert_eq!(factory.session_uplink_owned_bytes(), 8);
    reservation.commit(Bytes::from_static(b"abc")).unwrap();
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 3);
    assert_eq!(factory.session_uplink_owned_bytes(), 3);

    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("expected committed source DATA")
    };
    let (event, mut ownership) = data.into_event_and_ownership();
    let mut effects = fixture.model.reduce(event).unwrap();
    let stored = take_replay_stored(&mut effects);
    let replay = ownership.bind_replay(stored).unwrap();
    assert_eq!(replay.start(), ByteOffset::new(0));
    assert_eq!(replay.end(), ByteOffset::new(3));
    drop(replay);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
}

#[test]
fn invalid_source_commit_releases_reservation_without_advancing_offset() {
    let mut fixture = fixture();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 8, 16).unwrap();
    let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();

    assert_eq!(
        flow.try_reserve_uplink_source(8)
            .unwrap()
            .commit(Bytes::new())
            .unwrap_err(),
        FlowPortError::UplinkLengthMismatch {
            reserved: 8,
            actual: 0,
        }
    );
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert!(driver.try_recv_next().unwrap().is_none());

    flow.try_reserve_uplink_source(8)
        .unwrap()
        .commit(Bytes::from_static(b"abc"))
        .unwrap();
    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("expected valid DATA after rejected empty commit")
    };
    let (event, mut ownership) = data.into_event_and_ownership();
    let mut effects = fixture.model.reduce(event).unwrap();
    let stored = take_replay_stored(&mut effects);
    let replay = ownership.bind_replay(stored).unwrap();
    assert_eq!(replay.start(), ByteOffset::new(0));
    assert_eq!(replay.end(), ByteOffset::new(3));
}

#[test]
fn exhausted_source_offset_rejects_before_touching_the_socket_extractor() {
    let fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    flow.state
        .next_uplink_offset
        .store(u64::MAX, Ordering::Release);
    let extractions = AtomicUsize::new(0);

    assert_eq!(
        flow.try_send_uplink_with(1, || {
            extractions.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"x")
        })
        .unwrap_err(),
        FlowPortError::UplinkOffsetExhausted
    );
    assert_eq!(extractions.load(Ordering::SeqCst), 0);
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
    assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 0);
    assert!(driver.try_recv_next().unwrap().is_none());
}

#[test]
fn ordered_close_waits_for_outstanding_source_reservation() {
    let fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 1, 1));

    let reservation = flow.try_reserve_uplink_source(8).unwrap();
    assert_eq!(
        flow.try_send_close().unwrap_err(),
        FlowPortError::UplinkSourceReservationActive
    );
    assert!(driver.try_recv_next().unwrap().is_none());

    drop(reservation);
    flow.try_send_close().unwrap();
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::LocalClose { flow: observed }))
            if observed == fixture.flow
    ));
}

#[test]
fn extractor_length_mismatch_does_not_advance_later_source_offsets() {
    let mut fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 1, 1));

    assert_eq!(
        flow.try_send_uplink_with(8, || Bytes::from_static(b"bad"))
            .unwrap_err(),
        FlowPortError::UplinkLengthMismatch {
            reserved: 8,
            actual: 3,
        }
    );
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);

    let mut replays = Vec::new();
    for payload in [b"abc".as_slice(), b"def".as_slice()] {
        flow.try_send_uplink_with(payload.len(), || Bytes::copy_from_slice(payload))
            .unwrap();
        let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
            panic!("expected valid DATA after mismatch")
        };
        let (event, mut ownership) = data.into_event_and_ownership();
        let mut effects = fixture.model.reduce(event).unwrap();
        let stored = take_replay_stored(&mut effects);
        replays.push(ownership.bind_replay(stored).unwrap());
    }

    assert_eq!(replays[0].start(), ByteOffset::new(0));
    assert_eq!(replays[0].end(), ByteOffset::new(3));
    assert_eq!(replays[1].start(), ByteOffset::new(3));
    assert_eq!(replays[1].end(), ByteOffset::new(6));
}

#[test]
fn source_error_drops_reservation_without_capacity_or_offset_ownership() {
    let mut fixture = fixture();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 8, 16).unwrap();
    let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();

    let reservation = flow.try_reserve_uplink_source(8).unwrap();
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 8);
    assert_eq!(factory.session_uplink_owned_bytes(), 8);
    let source_result = Result::<Bytes, &'static str>::Err("would block");
    match source_result {
        Ok(payload) => reservation.commit(payload).unwrap(),
        Err(_) => drop(reservation),
    }
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert!(driver.try_recv_next().unwrap().is_none());

    flow.try_reserve_uplink_source(8)
        .unwrap()
        .commit(Bytes::from_static(b"abc"))
        .unwrap();
    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("expected valid DATA after source error")
    };
    let (event, mut ownership) = data.into_event_and_ownership();
    let mut effects = fixture.model.reduce(event).unwrap();
    let stored = take_replay_stored(&mut effects);
    let replay = ownership.bind_replay(stored).unwrap();
    assert_eq!(replay.start(), ByteOffset::new(0));
    assert_eq!(replay.end(), ByteOffset::new(3));
}

#[test]
fn source_reservations_share_the_session_global_byte_budget() {
    let mut fixture = fixture();
    let second_flow = open_another_local_flow(&mut fixture, 8443);
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 4, 8, 16).unwrap();
    let (first, _first_driver) = factory.open_flow(fixture.flow).unwrap();
    let (second, mut second_driver) = factory.open_flow(second_flow).unwrap();

    let first_reservation = first.try_reserve_uplink_source(4).unwrap();
    assert_eq!(factory.session_uplink_owned_bytes(), 4);
    let Err(error) = second.try_reserve_uplink_source(1) else {
        panic!("the second flow must not bypass the shared session byte budget")
    };
    assert_eq!(
        error,
        FlowPortError::UplinkGlobalByteBudgetExhausted {
            requested: 1,
            available: 0,
        }
    );

    drop(first_reservation);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    second
        .try_reserve_uplink_source(1)
        .unwrap()
        .commit(Bytes::from_static(b"x"))
        .unwrap();
    assert_eq!(factory.session_uplink_owned_bytes(), 1);
    let Some(DriverInput::Data(data)) = second_driver.try_recv_next().unwrap() else {
        panic!("expected second flow DATA after shared capacity was released")
    };
    drop(data);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
}

#[test]
fn one_source_reservation_blocks_ordered_sources_but_not_urgent_reset() {
    let fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 1, 1));

    let reservation = flow.try_reserve_uplink_source(8).unwrap();
    let Err(error) = flow.try_reserve_uplink_source(1) else {
        panic!("a flow must not hold two ordered-source reservations")
    };
    assert_eq!(error, FlowPortError::UplinkSourceReservationActive);
    flow.try_send_reset(ResetReason::LocalAbandon).unwrap();
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::LocalReset {
            flow: reset_flow,
            reason: ResetReason::LocalAbandon,
        })) if reset_flow == fixture.flow
    ));

    drop(reservation);
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
    flow.try_reserve_uplink_source(1)
        .unwrap()
        .commit(Bytes::from_static(b"x"))
        .unwrap();
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Data(_))
    ));
}

#[test]
fn uplink_queue_normalizes_a_small_slice_before_taking_ownership() {
    let fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let backing = Bytes::from(vec![0x5a; 1024 * 1024]);
    let tiny = backing.slice(512..516);
    let unbounded_pointer = tiny.as_ptr();
    flow.try_send_uplink_with(4, move || tiny).unwrap();
    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("expected normalized DATA")
    };
    assert_eq!(data.payload(), [0x5a; 4]);
    assert_ne!(data.payload().as_ptr(), unbounded_pointer);
}

#[test]
fn exact_vec_extractor_transfers_without_second_copy_and_compacts_excess_capacity() {
    let fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(4, 1, 1, 1));
    let mut exact_pointer = 0usize;
    flow.try_send_uplink_vec_with(4, || {
        let payload = vec![1, 2, 3, 4];
        assert_eq!(payload.capacity(), payload.len());
        exact_pointer = payload.as_ptr() as usize;
        payload
    })
    .unwrap();
    let Some(DriverInput::Data(exact)) = driver.try_recv_next().unwrap() else {
        panic!("expected exact Vec DATA")
    };
    assert_eq!(exact.payload().as_ptr() as usize, exact_pointer);

    let calls = AtomicUsize::new(0);
    assert_eq!(
        flow.try_send_uplink_vec_with(1, || {
            calls.fetch_add(1, Ordering::SeqCst);
            vec![9]
        })
        .unwrap_err(),
        FlowPortError::UplinkByteBudgetExhausted {
            requested: 1,
            available: 0,
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(exact);

    let mut excess_pointer = 0usize;
    flow.try_send_uplink_vec_with(4, || {
        let mut payload = Vec::with_capacity(1024 * 1024);
        payload.extend_from_slice(&[5, 6, 7, 8]);
        excess_pointer = payload.as_ptr() as usize;
        payload
    })
    .unwrap();
    let Some(DriverInput::Data(compacted)) = driver.try_recv_next().unwrap() else {
        panic!("expected compacted Vec DATA")
    };
    assert_eq!(compacted.payload(), [5, 6, 7, 8]);
    assert_ne!(compacted.payload().as_ptr() as usize, excess_pointer);
}

#[test]
fn uplink_replay_budget_releases_only_on_exact_terminal_reducer_effect() {
    let mut fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 2, 1));
    flow.try_send_uplink_with(4, || Bytes::from_static(b"data"))
        .unwrap();
    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("expected DATA")
    };
    let (event, mut ownership) = data.into_event_and_ownership();
    let mut stored_effects = fixture.model.reduce(event).unwrap();
    let stored = take_replay_stored(&mut stored_effects);
    let mut replay = ownership.bind_replay(stored).unwrap();
    let terminal_effects = fixture
        .model
        .reduce(SessionEvent::LocalReset {
            flow: fixture.flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();
    let nonterminal = terminal_effects
        .iter()
        .find(|effect| matches!(effect, SessionEffect::Transmit(_)))
        .unwrap();
    assert_eq!(
        replay
            .release_after_reducer_terminal(nonterminal)
            .unwrap_err(),
        FlowPortError::UplinkTerminalMismatch
    );
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 4);
    assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 1);
    let terminal = terminal_effects
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .unwrap();
    assert!(replay.release_after_reducer_terminal(terminal).unwrap());
    assert_eq!(flow.probe().snapshot().uplink_owned_bytes(), 0);
    assert_eq!(flow.probe().snapshot().uplink_owned_segments(), 0);
}

#[test]
fn local_close_cannot_overtake_preceding_uplink_data() {
    let mut fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 2, 1));
    flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .unwrap();
    flow.try_send_close().unwrap();

    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("DATA queued before local FIN must be reduced first")
    };
    let (data_event, mut ownership) = data.into_event_and_ownership();
    let mut data_effects = fixture.model.reduce(data_event).unwrap();
    let stored = take_replay_stored(&mut data_effects);
    let data_frame = data_effects
        .iter()
        .find_map(|effect| match effect {
            SessionEffect::Transmit(frame) => Some(frame),
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        data_frame.record(),
        Record::Data {
            offset,
            payload,
            ..
        } if *offset == ByteOffset::new(0) && payload.as_ref() == b"x"
    ));
    let _replay = ownership.bind_replay(stored).unwrap();

    let Some(DriverInput::Control(close_event @ SessionEvent::LocalClose { .. })) =
        driver.try_recv_next().unwrap()
    else {
        panic!("local FIN must follow every preceding DATA extent")
    };
    let close_frame = fixture
        .model
        .reduce(close_event)
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::Transmit(frame) => Some(frame),
            _ => None,
        })
        .unwrap();
    assert!(matches!(
        close_frame.record(),
        Record::Close { final_offset, .. } if *final_offset == ByteOffset::new(1)
    ));
}

#[test]
fn local_reset_has_reserved_terminal_capacity_and_urgent_priority() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));

    // Reserving the sink completion occupies the sole ordinary control
    // slot. A queued DATA extent simultaneously occupies the source lane.
    driver.try_deliver_sink(offer, segments).unwrap();
    flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .unwrap();

    flow.try_send_reset(ResetReason::LocalAbandon).unwrap();
    let Some(DriverInput::Control(reset @ SessionEvent::LocalReset { .. })) =
        driver.try_recv_next().unwrap()
    else {
        panic!("the reserved terminal reset must outrank ordinary source/control work")
    };
    assert!(
        fixture
            .model
            .reduce(reset)
            .unwrap()
            .iter()
            .any(|effect| matches!(
                effect,
                SessionEffect::FlowFinished {
                    reason: FlowFinishReason::LocalReset(ResetReason::LocalAbandon),
                    ..
                }
            ))
    );
}

#[test]
fn abandoning_an_uninstalled_flow_emits_exact_local_reset() {
    let fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));

    flow.abandon_before_install().unwrap();

    let Some(DriverInput::Control(SessionEvent::LocalReset {
        flow: reset_flow,
        reason,
    })) = driver.try_recv_next().unwrap()
    else {
        panic!("an uninstalled TUN owner must leave one exact reset for the supervisor")
    };
    assert_eq!(reset_flow, fixture.flow);
    assert_eq!(reason, ResetReason::LocalAbandon);
}

#[test]
fn driver_local_abandon_seal_mints_one_opaque_reset_and_rejects_repeat() {
    let fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));

    let sealed = driver.seal_for_local_abandon();
    assert_eq!(
        format!("{sealed:?}"),
        "TcpDriverLocalAbandonSeal::Sealed([REDACTED])"
    );
    let TcpDriverLocalAbandonSeal::Sealed(input) = sealed else {
        panic!("first seal must return one opaque LocalAbandon input")
    };
    assert!(matches!(
        input.as_inner(),
        DriverInput::Control(SessionEvent::LocalReset {
            flow: reset_flow,
            reason: ResetReason::LocalAbandon,
        }) if *reset_flow == fixture.flow
    ));

    assert!(matches!(
        driver.seal_for_local_abandon(),
        TcpDriverLocalAbandonSeal::AlreadySealed
    ));
    assert_eq!(
        flow.try_send_reset(ResetReason::TargetFailure).unwrap_err(),
        FlowPortError::DriverAdmissionSealed
    );
    assert_eq!(
        flow.try_send_close().unwrap_err(),
        FlowPortError::DriverAdmissionSealed
    );
    let extractions = AtomicUsize::new(0);
    assert_eq!(
        flow.try_send_uplink_with(1, || {
            extractions.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"x")
        })
        .unwrap_err(),
        FlowPortError::DriverAdmissionSealed
    );
    assert_eq!(extractions.load(Ordering::SeqCst), 0);
    assert!(flow.probe().snapshot().is_quiescent());
}

#[test]
fn driver_local_abandon_seal_revokes_outstanding_source_reservation_and_capacity() {
    let mut fixture = fixture();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 1, 1).unwrap();
    let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
    let probe = flow.probe();
    let reservation = flow.try_reserve_uplink_source(8).unwrap();
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 8);
    assert_eq!(probe.snapshot().uplink_owned_segments(), 1);
    assert_eq!(factory.session_uplink_owned_bytes(), 8);
    assert_eq!(factory.session_uplink_owned_segments(), 1);

    let sealed = driver.seal_for_local_abandon();
    assert!(matches!(sealed, TcpDriverLocalAbandonSeal::Sealed(_)));
    assert_eq!(
        reservation.commit(Bytes::from_static(b"late")).unwrap_err(),
        FlowPortError::DriverAdmissionSealed
    );
    assert!(probe.snapshot().is_quiescent());
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
    assert_eq!(
        flow.try_reserve_uplink_source(1).unwrap_err(),
        FlowPortError::DriverAdmissionSealed
    );

    let second = open_another_local_flow(&mut fixture, 8443);
    let (second_flow, mut second_driver) = factory.open_flow(second).unwrap();
    let eof_reservation = second_flow.try_reserve_uplink_source(8).unwrap();
    assert!(matches!(
        second_driver.seal_for_local_abandon(),
        TcpDriverLocalAbandonSeal::Sealed(_)
    ));
    assert_eq!(
        eof_reservation.commit_eof().unwrap_err(),
        FlowPortError::DriverAdmissionSealed
    );
    assert!(second_flow.probe().snapshot().is_quiescent());
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
}

#[test]
fn driver_local_abandon_seal_linearizes_with_an_endpoint_send_in_extractor() {
    let fixture = fixture();
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 1, 1, 1), 8, 1, 1).unwrap();
    let (flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
    let probe = flow.probe();
    let reserved = Arc::new(std::sync::Barrier::new(2));
    let resume = Arc::new(std::sync::Barrier::new(2));

    std::thread::scope(|scope| {
        let reserved_by_sender = Arc::clone(&reserved);
        let resume_sender = Arc::clone(&resume);
        let sender = scope.spawn(move || {
            flow.try_send_uplink_with(8, || {
                reserved_by_sender.wait();
                resume_sender.wait();
                Bytes::from_static(b"latebyte")
            })
        });

        reserved.wait();
        assert_eq!(probe.snapshot().uplink_owned_bytes(), 8);
        assert!(matches!(
            driver.seal_for_local_abandon(),
            TcpDriverLocalAbandonSeal::Sealed(_)
        ));
        resume.wait();
        assert_eq!(
            sender.join().unwrap().unwrap_err(),
            FlowPortError::DriverAdmissionSealed
        );
    });

    assert!(probe.snapshot().is_quiescent());
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
    assert!(matches!(
        driver.seal_for_local_abandon(),
        TcpDriverLocalAbandonSeal::AlreadySealed
    ));
}

#[test]
fn driver_local_abandon_seal_returns_queued_reset_and_drains_ordinary_inputs() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
    let factory =
        ResumableTcpPortFactory::new_unbound_for_test(port_config(8, 2, 2, 1), 8, 2, 2).unwrap();
    let (mut flow, mut driver) = factory.open_flow(fixture.flow).unwrap();
    let probe = flow.probe();

    flow.try_send_uplink_with(4, || Bytes::from_static(b"data"))
        .unwrap();
    flow.try_send_close().unwrap();
    driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected sink delivery")
    };
    delivery.abandon();
    flow.try_send_reset(ResetReason::TargetFailure).unwrap();
    assert_eq!(
        flow.try_send_reset(ResetReason::LocalAbandon).unwrap_err(),
        FlowPortError::TerminalLaneFull
    );

    let TcpDriverLocalAbandonSeal::Sealed(input) = driver.seal_for_local_abandon() else {
        panic!("the reset admitted before seal must retain priority")
    };
    assert!(matches!(
        input.as_inner(),
        DriverInput::Control(SessionEvent::LocalReset {
            flow: reset_flow,
            reason: ResetReason::TargetFailure,
        }) if *reset_flow == fixture.flow
    ));
    assert!(probe.snapshot().is_quiescent());
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
    assert_eq!(driver.try_recv_next().unwrap_err(), FlowPortError::Closed);
    assert!(matches!(
        driver.seal_for_local_abandon(),
        TcpDriverLocalAbandonSeal::AlreadySealed
    ));
}

#[test]
fn driver_local_abandon_seal_distinguishes_a_prior_dequeued_reset() {
    let fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    flow.try_send_reset(ResetReason::TargetFailure).unwrap();
    let deferred = driver
        .try_recv_next_for_client_controller()
        .unwrap()
        .expect("root must own the already-dequeued reset");
    assert!(matches!(
        deferred.as_inner(),
        DriverInput::Control(SessionEvent::LocalReset {
            flow: reset_flow,
            reason: ResetReason::TargetFailure,
        }) if *reset_flow == fixture.flow
    ));
    assert_eq!(
        flow.try_send_reset(ResetReason::LocalAbandon).unwrap_err(),
        FlowPortError::TerminalLaneFull
    );

    let disposition = driver.seal_for_local_abandon();
    assert!(matches!(
        disposition,
        TcpDriverLocalAbandonSeal::PriorResetAlreadyDequeued
    ));
    assert_eq!(
        format!("{disposition:?}"),
        "TcpDriverLocalAbandonSeal::PriorResetAlreadyDequeued"
    );
    assert!(matches!(
        driver.seal_for_local_abandon(),
        TcpDriverLocalAbandonSeal::AlreadySealed
    ));
    assert_eq!(
        flow.try_send_reset(ResetReason::TargetFailure).unwrap_err(),
        FlowPortError::DriverAdmissionSealed
    );
    assert!(flow.probe().snapshot().is_quiescent());
    drop(deferred);
}

#[test]
fn completion_reserved_before_seal_cannot_publish_after_local_abandon() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
    let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let probe = flow.probe();
    driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected sink delivery")
    };
    assert_eq!(probe.snapshot().reserved_completion_slots(), 1);
    assert_eq!(probe.snapshot().pending_downlink_actions(), 1);

    assert!(matches!(
        driver.seal_for_local_abandon(),
        TcpDriverLocalAbandonSeal::Sealed(_)
    ));
    assert!(!delivery.is_live());
    assert_eq!(
        delivery.with_live_first_segment(|_| ()).unwrap_err(),
        FlowPortError::SinkCapabilityRevoked
    );
    assert!(delivery.complete_write(1).unwrap().is_none());
    assert_eq!(probe.snapshot().reserved_completion_slots(), 0);
    assert_eq!(probe.snapshot().pending_downlink_actions(), 0);
    assert_eq!(driver.try_recv_next().unwrap_err(), FlowPortError::Closed);
    assert!(probe.snapshot().is_quiescent());
}

#[test]
fn flow_poll_after_seal_drains_a_queued_downlink_capability_without_mutation() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
    let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let probe = flow.probe();
    driver.try_deliver_sink(offer, segments).unwrap();
    assert_eq!(probe.snapshot().reserved_completion_slots(), 1);
    assert_eq!(probe.snapshot().pending_downlink_actions(), 1);

    assert!(matches!(
        driver.seal_for_local_abandon(),
        TcpDriverLocalAbandonSeal::Sealed(_)
    ));
    assert!(flow.try_recv_action().unwrap().is_none());
    assert!(probe.snapshot().is_quiescent());
    assert_eq!(driver.try_recv_next().unwrap_err(), FlowPortError::Closed);
}

#[test]
fn sealed_driver_still_delivers_reducer_terminal_and_then_reports_retired() {
    let mut fixture = fixture();
    let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let probe = flow.probe();
    let TcpDriverLocalAbandonSeal::Sealed(input) = driver.seal_for_local_abandon() else {
        panic!("first seal must return the reducer input")
    };
    let ClientControllerDriverInput(input) = input;
    let DriverInput::Control(reset) = *input else {
        panic!("local abandon must be a control input")
    };
    let terminal_effects = fixture.model.reduce(reset).unwrap();
    let terminal = terminal_effects
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .expect("LocalAbandon must finish the flow");

    driver.try_deliver_terminal_effect(terminal).unwrap();
    assert!(matches!(
        driver.seal_for_local_abandon(),
        TcpDriverLocalAbandonSeal::TerminalRetired
    ));
    let TunAction::Terminal(terminal) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("terminal lane must remain deliverable after source admission is sealed")
    };
    assert_eq!(terminal.local_flow(), fixture.flow);
    assert_eq!(
        terminal.action(),
        TerminalAction::FlowFinished(FlowFinishReason::LocalReset(ResetReason::LocalAbandon))
    );
    drop(terminal);
    assert!(probe.snapshot().is_quiescent());
}

#[test]
fn reducer_terminal_revokes_a_live_sink_capability_when_endpoint_is_gone() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
    let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let probe = flow.probe();
    driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected one live sink capability")
    };
    assert!(delivery.is_live());

    // Closing the endpoint also closes the independent terminal receiver,
    // but SocketCtx may still own the previously dequeued delivery.
    drop(flow);
    let terminal_effects = fixture
        .model
        .reduce(SessionEvent::LocalReset {
            flow: fixture.flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();
    let terminal = terminal_effects
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .expect("LocalAbandon must finish the flow");

    assert_eq!(
        driver.try_deliver_terminal_effect(terminal).unwrap(),
        TcpTerminalDeliveryDisposition::ReceiverDetached,
    );
    assert!(!delivery.is_live());
    assert_eq!(
        delivery.with_live_first_segment(|_| ()).unwrap_err(),
        FlowPortError::SinkCapabilityRevoked,
    );
    drop(delivery);
    assert!(probe.snapshot().is_quiescent());
    assert!(matches!(
        driver.seal_for_local_abandon(),
        TcpDriverLocalAbandonSeal::TerminalRetired
    ));
}

#[test]
fn bounded_control_burst_cannot_starve_ready_ordered_source() {
    let mut fixture = fixture();
    let (offer, _) = sink_offer(&mut fixture, b"peer-data");
    let (flow, mut driver) =
        isolated_pair(fixture.flow, port_config(1, 1, CONTROL_BURST_LIMIT + 1, 1));
    flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .unwrap();
    let control = driver.completion_tx.upgrade().unwrap();
    for _ in 0..=CONTROL_BURST_LIMIT {
        control
            .try_send(FlowControl::SinkAbandoned { offer })
            .unwrap();
    }

    for _ in 0..CONTROL_BURST_LIMIT {
        assert!(matches!(
            driver.try_recv_next().unwrap(),
            Some(DriverInput::Control(_))
        ));
        control
            .try_send(FlowControl::SinkAbandoned { offer })
            .unwrap();
    }
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Data(_))
    ));
}

#[test]
fn ready_completion_preempts_an_ordered_source_flood() {
    let mut fixture = fixture();
    let (offer, _) = sink_offer(&mut fixture, b"peer-data");
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(4, 4, 1, 1));
    for _ in 0..4 {
        flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
            .unwrap();
    }
    driver
        .completion_tx
        .upgrade()
        .unwrap()
        .try_send(FlowControl::SinkAbandoned { offer })
        .unwrap();
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(_))
    ));
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Data(_))
    ));
}

#[test]
fn sink_completion_has_priority_over_ordered_source_and_leg_loss_is_not_terminal() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
    let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 2, 1));
    flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .unwrap();
    driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected sink delivery")
    };
    delivery.abandon();
    driver.note_leg_lost();

    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::SinkAbandoned { offer: observed }))
            if observed == offer
    ));
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Data(_))
    ));
    assert_eq!(driver.probe().snapshot().observed_leg_losses(), 1);
}

#[test]
fn partial_sink_completion_discards_old_suffix_and_waits_for_new_offer() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"abcdef");
    let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 2, 2));
    driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected sink delivery")
    };
    assert_eq!(delivery.first_segment_slice(), b"abcdef");
    let delivery = delivery.complete_write(0).unwrap().unwrap();
    assert_eq!(driver.probe().snapshot().reserved_completion_slots(), 1);
    assert!(delivery.complete_write(2).unwrap().is_none());

    let Some(DriverInput::Control(event @ SessionEvent::SinkAccepted { bytes: 2, .. })) =
        driver.try_recv_next().unwrap()
    else {
        panic!("expected exact sink acceptance")
    };
    let effects = fixture.model.reduce(event).unwrap();
    let (new_offer, new_segments) = effects
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::OfferToSink { offer, segments } => Some((offer, segments)),
            _ => None,
        })
        .unwrap();
    assert_eq!(new_offer.offset(), ByteOffset::new(2));
    assert_eq!(new_offer.len(), 4);
    assert_eq!(new_segments[0].payload(), &Bytes::from_static(b"cdef"));
    assert_eq!(driver.probe().snapshot().reserved_completion_slots(), 0);
}

#[test]
fn abandon_uses_pre_reserved_control_slot() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"payload");
    let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    driver.try_deliver_sink(offer, segments).unwrap();
    assert_eq!(driver.probe().snapshot().reserved_completion_slots(), 1);
    let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected sink delivery")
    };
    delivery.abandon();
    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::SinkAbandoned { offer: observed }))
            if observed == offer
    ));
}

#[test]
fn cancellation_drop_fails_closed_for_sink_and_half_close() {
    let mut sink_fixture = fixture();
    let (offer, segments) = sink_offer(&mut sink_fixture, b"payload");
    let (mut sink_flow, mut sink_driver) =
        isolated_pair(sink_fixture.flow, port_config(8, 1, 1, 1));
    sink_driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = sink_flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected sink delivery")
    };
    drop(delivery);
    assert!(matches!(
        sink_driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::SinkAbandoned { offer: observed }))
            if observed == offer
    ));

    let mut close_fixture = fixture();
    let capability = half_close(&mut close_fixture);
    let (mut close_flow, mut close_driver) =
        isolated_pair(close_fixture.flow, port_config(8, 1, 1, 1));
    close_driver.try_deliver_half_close(capability).unwrap();
    let TunAction::HalfClose(delivery) = close_flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected half-close delivery")
    };
    drop(delivery);
    assert!(matches!(
        close_driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::SinkHalfCloseFailed { completion }))
            if completion == capability
    ));
}

#[test]
fn downlink_refuses_delivery_when_completion_lane_has_no_slot() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"payload");
    let retry_segments = segments.clone();
    let (mut flow, driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected sink delivery")
    };
    delivery.abandon();
    assert_eq!(
        driver.try_deliver_sink(offer, retry_segments).unwrap_err(),
        FlowPortError::ControlLaneFull
    );
    assert_eq!(driver.probe().snapshot().pending_downlink_actions(), 0);
}

#[test]
fn exact_sink_delivery_returns_same_ownership_across_downlink_pressure_and_retry() {
    let mut sink_fixture = fixture();
    let (offer, segments) = sink_offer(&mut sink_fixture, b"private-payload-marker");
    let mut close_fixture = fixture();
    let blocking_capability = half_close(&mut close_fixture);
    assert_eq!(sink_fixture.flow, close_fixture.flow);
    let original_segments_ptr = segments.as_ptr();
    let original_payload_ptr = segments[0].payload().as_ptr();
    let (mut flow, driver) = isolated_pair(sink_fixture.flow, port_config(8, 1, 2, 1));

    driver.try_deliver_half_close(blocking_capability).unwrap();
    let failure = driver.try_deliver_sink_exact(offer, segments).unwrap_err();
    assert_eq!(failure.kind(), FlowPortError::DownlinkLaneFull);
    assert!(!format!("{failure:?}").contains("private-payload-marker"));
    let (returned_offer, returned_segments) = failure.into_parts();
    assert_eq!(returned_offer, offer);
    assert_eq!(returned_segments.as_ptr(), original_segments_ptr);
    assert_eq!(
        returned_segments[0].payload().as_ptr(),
        original_payload_ptr
    );

    let TunAction::HalfClose(blocking_delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected blocking half-close delivery")
    };
    driver
        .try_deliver_sink_exact(returned_offer, returned_segments)
        .unwrap();
    let TunAction::Sink(retried_delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected retried sink delivery")
    };
    assert_eq!(
        retried_delivery.first_segment_slice().as_ptr(),
        original_payload_ptr
    );
    drop(blocking_delivery);
}

#[test]
fn exact_sink_delivery_returns_same_ownership_across_completion_pressure_and_retry() {
    let mut sink_fixture = fixture();
    let (offer, segments) = sink_offer(&mut sink_fixture, b"completion-pressure-payload");
    let original_segments_ptr = segments.as_ptr();
    let original_payload_ptr = segments[0].payload().as_ptr();
    let (mut flow, mut driver) = isolated_pair(sink_fixture.flow, port_config(8, 1, 1, 2));

    let mut blocking_fixture = fixture();
    let blocking_capability = half_close(&mut blocking_fixture);
    driver.try_deliver_half_close(blocking_capability).unwrap();
    let TunAction::HalfClose(blocking_delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected blocking half-close delivery")
    };
    blocking_delivery.fail();

    let failure = driver.try_deliver_sink_exact(offer, segments).unwrap_err();
    assert_eq!(failure.kind(), FlowPortError::ControlLaneFull);
    let (returned_offer, returned_segments) = failure.into_parts();
    assert_eq!(returned_offer, offer);
    assert_eq!(returned_segments.as_ptr(), original_segments_ptr);
    assert_eq!(
        returned_segments[0].payload().as_ptr(),
        original_payload_ptr
    );

    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::SinkHalfCloseFailed { completion }))
            if completion == blocking_capability
    ));
    driver
        .try_deliver_sink_exact(returned_offer, returned_segments)
        .unwrap();
    let TunAction::Sink(retried_delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected retried sink delivery")
    };
    assert_eq!(
        retried_delivery.first_segment_slice().as_ptr(),
        original_payload_ptr
    );
}

#[test]
fn exact_sink_delivery_validation_failure_returns_original_vector_and_payload_backing() {
    let mut fixture = fixture();
    let (offer, mut segments) = sink_offer(&mut fixture, b"validation-private-payload");
    segments.push(segments[0].clone());
    let original_segments_ptr = segments.as_ptr();
    let original_payload_ptrs = segments
        .iter()
        .map(|segment| segment.payload().as_ptr())
        .collect::<Vec<_>>();
    let (_flow, driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));

    let failure = driver.try_deliver_sink_exact(offer, segments).unwrap_err();
    assert_eq!(failure.kind(), FlowPortError::InvalidSinkCoverage);
    assert!(!format!("{failure:?}").contains("validation-private-payload"));
    let (returned_offer, returned_segments) = failure.into_parts();
    assert_eq!(returned_offer, offer);
    assert_eq!(returned_segments.as_ptr(), original_segments_ptr);
    assert_eq!(
        returned_segments
            .iter()
            .map(|segment| segment.payload().as_ptr())
            .collect::<Vec<_>>(),
        original_payload_ptrs
    );
}

#[test]
fn exact_sink_delivery_closed_and_retired_failures_return_original_ownership() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"closed-private-payload");

    let completion_closed_segments = segments.clone();
    let completion_closed_segments_ptr = completion_closed_segments.as_ptr();
    let completion_closed_payload_ptr = completion_closed_segments[0].payload().as_ptr();
    let (completion_closed_flow, completion_closed_driver) =
        isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let (completion_uplink, _completion_downlink) = completion_closed_flow.into_split();
    drop(completion_uplink);
    let completion_closed_failure = completion_closed_driver
        .try_deliver_sink_exact(offer, completion_closed_segments)
        .unwrap_err();
    assert_eq!(completion_closed_failure.kind(), FlowPortError::Closed);
    let (completion_closed_offer, completion_closed_segments) =
        completion_closed_failure.into_parts();
    assert_eq!(completion_closed_offer, offer);
    assert_eq!(
        completion_closed_segments.as_ptr(),
        completion_closed_segments_ptr
    );
    assert_eq!(
        completion_closed_segments[0].payload().as_ptr(),
        completion_closed_payload_ptr
    );

    let downlink_closed_segments = segments.clone();
    let downlink_closed_segments_ptr = downlink_closed_segments.as_ptr();
    let downlink_closed_payload_ptr = downlink_closed_segments[0].payload().as_ptr();
    let (downlink_closed_flow, downlink_closed_driver) =
        isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let (_downlink_uplink, downlink) = downlink_closed_flow.into_split();
    drop(downlink);
    let downlink_closed_failure = downlink_closed_driver
        .try_deliver_sink_exact(offer, downlink_closed_segments)
        .unwrap_err();
    assert_eq!(downlink_closed_failure.kind(), FlowPortError::Closed);
    let (downlink_closed_offer, downlink_closed_segments) = downlink_closed_failure.into_parts();
    assert_eq!(downlink_closed_offer, offer);
    assert_eq!(
        downlink_closed_segments.as_ptr(),
        downlink_closed_segments_ptr
    );
    assert_eq!(
        downlink_closed_segments[0].payload().as_ptr(),
        downlink_closed_payload_ptr
    );
    assert_eq!(
        downlink_closed_driver
            .probe()
            .snapshot()
            .reserved_completion_slots(),
        0
    );

    let retired_segments_ptr = segments.as_ptr();
    let retired_payload_ptr = segments[0].payload().as_ptr();
    let (_retired_flow, mut retired_driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let terminal = flow_finished(&mut fixture);
    retired_driver
        .try_deliver_terminal_effect(&terminal)
        .unwrap();
    let retired_failure = retired_driver
        .try_deliver_sink_exact(offer, segments)
        .unwrap_err();
    assert_eq!(retired_failure.kind(), FlowPortError::Closed);
    let (retired_offer, retired_segments) = retired_failure.into_parts();
    assert_eq!(retired_offer, offer);
    assert_eq!(retired_segments.as_ptr(), retired_segments_ptr);
    assert_eq!(retired_segments[0].payload().as_ptr(), retired_payload_ptr);
}

#[test]
fn sink_and_half_close_capabilities_cannot_cross_direction() {
    let client = fixture();
    let mut owner = owner_fixture();
    assert_eq!(client.flow.session_id(), owner.flow.session_id());
    assert_eq!(client.flow.flow_id(), owner.flow.flow_id());
    assert_ne!(client.flow.direction(), owner.flow.direction());
    let (_flow, driver) = isolated_pair(client.flow, port_config(8, 1, 2, 2));

    let offered = owner
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: owner.leg,
            frame: Frame::try_new(
                owner.leg.generation(),
                Record::Data {
                    flow_id: owner.flow.flow_id(),
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"x"),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (offer, segments) = offered
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::OfferToSink { offer, segments } => Some((offer, segments)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        driver.try_deliver_sink(offer, segments).unwrap_err(),
        FlowPortError::SinkDirectionMismatch {
            expected: Direction::TargetToClient,
            actual: Direction::ClientToTarget,
        }
    );

    let mut close_owner = owner_fixture();
    let completion = owner_half_close(&mut close_owner);
    assert_eq!(
        driver.try_deliver_half_close(completion).unwrap_err(),
        FlowPortError::SinkDirectionMismatch {
            expected: Direction::TargetToClient,
            actual: Direction::ClientToTarget,
        }
    );
    assert!(driver.probe().snapshot().is_quiescent());
}

#[test]
fn half_close_reports_success_or_failure_only_after_local_completion() {
    let mut success_fixture = fixture();
    let success_capability = half_close(&mut success_fixture);
    let (mut success_flow, mut success_driver) =
        isolated_pair(success_fixture.flow, port_config(8, 1, 1, 1));
    success_driver
        .try_deliver_half_close(success_capability)
        .unwrap();
    assert!(success_driver.try_recv_next().unwrap().is_none());
    let TunAction::HalfClose(delivery) = success_flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected half-close")
    };
    delivery.succeed();
    assert!(matches!(
        success_driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::SinkHalfClosed { completion }))
            if completion == success_capability
    ));

    let mut failure_fixture = fixture();
    let failure_capability = half_close(&mut failure_fixture);
    let (mut failure_flow, mut failure_driver) =
        isolated_pair(failure_fixture.flow, port_config(8, 1, 1, 1));
    failure_driver
        .try_deliver_half_close(failure_capability)
        .unwrap();
    let TunAction::HalfClose(delivery) = failure_flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected half-close")
    };
    delivery.fail();
    assert!(matches!(
        failure_driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::SinkHalfCloseFailed { completion }))
            if completion == failure_capability
    ));
}

#[test]
fn exact_half_close_returns_same_ownership_across_downlink_pressure_and_retry() {
    let mut close_fixture = fixture();
    let capability = half_close(&mut close_fixture);
    let mut sink_fixture = fixture();
    let (blocking_offer, blocking_segments) = sink_offer(&mut sink_fixture, b"blocking-payload");
    assert_eq!(close_fixture.flow, sink_fixture.flow);
    let (mut flow, driver) = isolated_pair(close_fixture.flow, port_config(8, 1, 2, 1));

    driver
        .try_deliver_sink(blocking_offer, blocking_segments)
        .unwrap();
    let failure = driver.try_deliver_half_close_exact(capability).unwrap_err();
    assert_eq!(failure.kind(), FlowPortError::DownlinkLaneFull);
    assert_eq!(
        format!("{failure:?}"),
        "HalfCloseDeliveryFailure([REDACTED])"
    );
    let returned_capability = failure.into_capability();
    assert_eq!(returned_capability, capability);

    let TunAction::Sink(blocking_delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected blocking sink delivery")
    };
    driver
        .try_deliver_half_close_exact(returned_capability)
        .unwrap();
    let TunAction::HalfClose(retried_delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected retried half-close delivery")
    };
    assert_eq!(retried_delivery.capability(), capability);
    drop(blocking_delivery);
}

#[test]
fn exact_half_close_returns_same_ownership_across_completion_pressure_and_retry() {
    let mut close_fixture = fixture();
    let capability = half_close(&mut close_fixture);
    let mut sink_fixture = fixture();
    let (blocking_offer, blocking_segments) = sink_offer(&mut sink_fixture, b"blocking-payload");
    assert_eq!(close_fixture.flow, sink_fixture.flow);
    let (mut flow, mut driver) = isolated_pair(close_fixture.flow, port_config(8, 1, 1, 2));

    driver
        .try_deliver_sink(blocking_offer, blocking_segments)
        .unwrap();
    let TunAction::Sink(blocking_delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected blocking sink delivery")
    };
    blocking_delivery.abandon();

    let failure = driver.try_deliver_half_close_exact(capability).unwrap_err();
    assert_eq!(failure.kind(), FlowPortError::ControlLaneFull);
    let returned_capability = failure.into_capability();
    assert_eq!(returned_capability, capability);

    assert!(matches!(
        driver.try_recv_next().unwrap(),
        Some(DriverInput::Control(SessionEvent::SinkAbandoned { offer }))
            if offer == blocking_offer
    ));
    driver
        .try_deliver_half_close_exact(returned_capability)
        .unwrap();
    let TunAction::HalfClose(retried_delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected retried half-close delivery")
    };
    assert_eq!(retried_delivery.capability(), capability);
}

#[test]
fn exact_half_close_validation_failure_returns_original_capability() {
    let client = fixture();
    let mut owner = owner_fixture();
    let capability = owner_half_close(&mut owner);
    let (_flow, driver) = isolated_pair(client.flow, port_config(8, 1, 1, 1));

    let failure = driver.try_deliver_half_close_exact(capability).unwrap_err();
    assert_eq!(
        failure.kind(),
        FlowPortError::SinkDirectionMismatch {
            expected: Direction::TargetToClient,
            actual: Direction::ClientToTarget,
        }
    );
    assert_eq!(
        format!("{failure:?}"),
        "HalfCloseDeliveryFailure([REDACTED])"
    );
    assert_eq!(failure.into_capability(), capability);
}

#[test]
fn exact_half_close_closed_and_retired_failures_return_original_capability() {
    let mut fixture = fixture();
    let capability = half_close(&mut fixture);

    let (completion_closed_flow, completion_closed_driver) =
        isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let (completion_uplink, _completion_downlink) = completion_closed_flow.into_split();
    drop(completion_uplink);
    let completion_closed_failure = completion_closed_driver
        .try_deliver_half_close_exact(capability)
        .unwrap_err();
    assert_eq!(completion_closed_failure.kind(), FlowPortError::Closed);
    assert_eq!(completion_closed_failure.into_capability(), capability);

    let (downlink_closed_flow, downlink_closed_driver) =
        isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let (_downlink_uplink, downlink) = downlink_closed_flow.into_split();
    drop(downlink);
    let downlink_closed_failure = downlink_closed_driver
        .try_deliver_half_close_exact(capability)
        .unwrap_err();
    assert_eq!(downlink_closed_failure.kind(), FlowPortError::Closed);
    assert_eq!(downlink_closed_failure.into_capability(), capability);
    assert_eq!(
        downlink_closed_driver
            .probe()
            .snapshot()
            .reserved_completion_slots(),
        0
    );

    let (_retired_flow, mut retired_driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let terminal = flow_finished(&mut fixture);
    retired_driver
        .try_deliver_terminal_effect(&terminal)
        .unwrap();
    let retired_failure = retired_driver
        .try_deliver_half_close_exact(capability)
        .unwrap_err();
    assert_eq!(retired_failure.kind(), FlowPortError::Closed);
    assert_eq!(retired_failure.into_capability(), capability);
}

#[test]
fn split_halves_remain_independent_nonaliasing_owners() {
    let fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 2, 2));
    let probe = flow.probe();
    let (uplink, downlink) = flow.into_split();
    assert_eq!(uplink.local_flow(), fixture.flow);
    assert_eq!(downlink.local_flow(), fixture.flow);

    drop(downlink);
    uplink
        .try_send_uplink_with(4, || Bytes::from_static(b"data"))
        .unwrap();
    let Some(DriverInput::Data(data)) = driver.try_recv_next().unwrap() else {
        panic!("expected independently admitted uplink DATA")
    };
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);
    drop(data);
    assert!(probe.snapshot().is_quiescent());
    drop(uplink);
    drop(driver);
    assert!(probe.snapshot().is_quiescent());
}

#[tokio::test(flavor = "current_thread")]
async fn async_driver_waits_for_reserved_completion_after_uplink_half_closes() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"payload");
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 1));
    let (uplink, mut downlink) = flow.into_split();
    driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = downlink.try_recv_action().unwrap().unwrap() else {
        panic!("expected sink delivery")
    };
    drop(uplink);

    let waiting = tokio::spawn(async move { driver.recv_next().await });
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    delivery.abandon();
    assert!(matches!(
        waiting.await.unwrap(),
        Some(DriverInput::Control(SessionEvent::SinkAbandoned { offer: observed }))
            if observed == offer
    ));
}

#[test]
fn reducer_terminal_effects_are_forwarded_as_typed_tun_actions() {
    let mut fixture = fixture();
    let (flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 1, 1, 2));
    let (_uplink, mut downlink) = flow.into_split();
    let constructible_notification = SessionEffect::PeerReset {
        flow_id: fixture.flow.flow_id(),
        reason: ResetReason::TargetFailure,
    };
    assert_eq!(
        driver
            .try_deliver_terminal_effect(&constructible_notification)
            .unwrap_err(),
        FlowPortError::NotTerminalEffect
    );
    let mut other = fixture_for_session(0x99);
    assert_eq!(other.flow.flow_id(), fixture.flow.flow_id());
    let other_effects = other
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: other.leg,
            frame: Frame::try_new(
                other.leg.generation(),
                Record::Reset {
                    flow_id: other.flow.flow_id(),
                    reason: ResetReason::TargetFailure,
                },
            )
            .unwrap(),
        })
        .unwrap();
    let other_terminal = other_effects
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .unwrap();
    assert_eq!(
        driver
            .try_deliver_terminal_effect(other_terminal)
            .unwrap_err(),
        FlowPortError::CrossFlowTerminalEffect
    );
    let mut opposite = owner_fixture();
    assert_eq!(opposite.flow.session_id(), fixture.flow.session_id());
    assert_eq!(opposite.flow.flow_id(), fixture.flow.flow_id());
    assert_ne!(opposite.flow.direction(), fixture.flow.direction());
    let opposite_effects = opposite
        .model
        .reduce(SessionEvent::LocalReset {
            flow: opposite.flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();
    let opposite_terminal = opposite_effects
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .unwrap();
    assert_eq!(
        driver
            .try_deliver_terminal_effect(opposite_terminal)
            .unwrap_err(),
        FlowPortError::CrossFlowTerminalEffect
    );
    let terminal_effects = fixture
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: fixture.leg,
            frame: Frame::try_new(
                fixture.leg.generation(),
                Record::Reset {
                    flow_id: fixture.flow.flow_id(),
                    reason: ResetReason::TargetFailure,
                },
            )
            .unwrap(),
        })
        .unwrap();
    let terminal = terminal_effects
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .unwrap();
    driver.try_deliver_terminal_effect(terminal).unwrap();
    assert_eq!(
        driver.try_deliver_terminal_effect(terminal).unwrap_err(),
        FlowPortError::TerminalAlreadyDelivered
    );
    let TunAction::Terminal(terminal) = downlink.try_recv_action().unwrap().unwrap() else {
        panic!("expected terminal action")
    };
    assert_eq!(terminal.local_flow(), fixture.flow);
    assert_eq!(
        terminal.action(),
        TerminalAction::FlowFinished(FlowFinishReason::PeerReset(ResetReason::TargetFailure))
    );
}

#[test]
fn terminal_authority_drains_queued_source_and_completion_ownership() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"peer-data");
    let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(8, 2, 2, 1));
    let probe = flow.probe();
    flow.try_send_uplink_with(4, || Bytes::from_static(b"late"))
        .unwrap();
    driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(delivery) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected sink delivery")
    };
    delivery.abandon();
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 4);

    let effects = fixture
        .model
        .reduce(SessionEvent::PeerFrame {
            leg: fixture.leg,
            frame: Frame::try_new(
                fixture.leg.generation(),
                Record::Reset {
                    flow_id: fixture.flow.flow_id(),
                    reason: ResetReason::TargetFailure,
                },
            )
            .unwrap(),
        })
        .unwrap();
    let terminal = effects
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .unwrap();
    driver.try_deliver_terminal_effect(terminal).unwrap();
    assert_eq!(probe.snapshot().uplink_owned_bytes(), 0);
    assert_eq!(probe.snapshot().reserved_completion_slots(), 0);
    assert!(!matches!(driver.try_recv_next(), Ok(Some(_))));

    let calls = AtomicUsize::new(0);
    assert_eq!(
        flow.try_send_uplink_with(1, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Bytes::from_static(b"x")
        })
        .unwrap_err(),
        FlowPortError::Closed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let TunAction::Terminal(terminal) = flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected exact terminal delivery")
    };
    assert_eq!(
        terminal.action(),
        TerminalAction::FlowFinished(FlowFinishReason::PeerReset(ResetReason::TargetFailure))
    );
    drop(terminal);
    assert!(probe.snapshot().is_quiescent());
}

#[test]
fn terminal_authority_revokes_already_forwarded_sink_capabilities() {
    let mut sink_fixture = fixture();
    let (offer, segments) = sink_offer(&mut sink_fixture, b"peer-data");
    let (mut sink_flow, mut sink_driver) =
        isolated_pair(sink_fixture.flow, port_config(8, 1, 1, 1));
    let sink_probe = sink_flow.probe();
    sink_driver.try_deliver_sink(offer, segments).unwrap();
    let TunAction::Sink(sink_delivery) = sink_flow.try_recv_action().unwrap().unwrap() else {
        panic!("expected forwarded sink capability")
    };
    assert!(sink_delivery.is_live());
    let sink_mutations = AtomicUsize::new(0);
    sink_delivery
        .with_live_first_segment(|segment| {
            assert_eq!(segment, b"peer-data");
            sink_mutations.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    let sink_terminal = sink_fixture
        .model
        .reduce(SessionEvent::LocalReset {
            flow: sink_fixture.flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();
    let sink_terminal = sink_terminal
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .unwrap();
    sink_driver
        .try_deliver_terminal_effect(sink_terminal)
        .unwrap();
    assert!(!sink_delivery.is_live());
    assert_eq!(
        sink_delivery
            .with_live_first_segment(|_| {
                sink_mutations.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap_err(),
        FlowPortError::SinkCapabilityRevoked
    );
    assert_eq!(sink_mutations.load(Ordering::SeqCst), 1);
    drop(sink_delivery);
    let terminal = sink_flow.try_recv_action().unwrap().unwrap();
    drop(terminal);
    assert!(sink_probe.snapshot().is_quiescent());

    let mut close_fixture = fixture();
    let completion = half_close(&mut close_fixture);
    let (mut close_flow, mut close_driver) =
        isolated_pair(close_fixture.flow, port_config(8, 1, 1, 1));
    let close_probe = close_flow.probe();
    close_driver.try_deliver_half_close(completion).unwrap();
    let TunAction::HalfClose(close_delivery) = close_flow.try_recv_action().unwrap().unwrap()
    else {
        panic!("expected forwarded half-close capability")
    };
    assert!(close_delivery.is_live());
    let close_mutations = AtomicUsize::new(0);
    close_delivery
        .with_live(|| {
            close_mutations.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    let close_terminal = close_fixture
        .model
        .reduce(SessionEvent::LocalReset {
            flow: close_fixture.flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap();
    let close_terminal = close_terminal
        .iter()
        .find(|effect| matches!(effect, SessionEffect::FlowFinished { .. }))
        .unwrap();
    close_driver
        .try_deliver_terminal_effect(close_terminal)
        .unwrap();
    assert!(!close_delivery.is_live());
    assert_eq!(
        close_delivery
            .with_live(|| {
                close_mutations.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap_err(),
        FlowPortError::SinkCapabilityRevoked
    );
    assert_eq!(close_mutations.load(Ordering::SeqCst), 1);
    drop(close_delivery);
    let terminal = close_flow.try_recv_action().unwrap().unwrap();
    drop(terminal);
    assert!(close_probe.snapshot().is_quiescent());
}

#[test]
fn debug_is_redacted_and_endpoint_drop_releases_all_port_ownership() {
    let mut fixture = fixture();
    let (offer, segments) = sink_offer(&mut fixture, b"DOWNLINK_SECRET_SENTINEL");
    let (mut flow, mut driver) = isolated_pair(fixture.flow, port_config(64, 2, 2, 2));
    let probe = flow.probe();
    flow.try_send_uplink_with(22, || Bytes::from_static(b"UPLINK_SECRET_SENTINEL"))
        .unwrap();
    let DriverInput::Data(data) = driver.try_recv_next().unwrap().unwrap() else {
        panic!("expected DATA")
    };
    let debug = format!("{data:?}");
    assert!(!debug.contains("UPLINK_SECRET_SENTINEL"));
    driver.try_deliver_sink(offer, segments).unwrap();
    let action = flow.try_recv_action().unwrap().unwrap();
    let debug = format!("{action:?}");
    assert!(!debug.contains("DOWNLINK_SECRET_SENTINEL"));
    assert!(!probe.snapshot().is_quiescent());

    drop(data);
    drop(action);
    drop(flow);
    drop(driver);
    assert!(probe.snapshot().is_quiescent());
}

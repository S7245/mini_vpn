//! A real received W expires while its observer is absent, before any provider.
//! Drives the real root control and physical reset, without filling F counters.
use super::super::super::rearm::FlowAdmission;
use super::super::super::rearm::physical_flow::tests::admission::{
    Endpoint, NeverOpen, receiving_pair,
};
use super::super::*;
use crate::client_tun::{
    DownlinkBackpressureConfig, HandshakeCompletion, flush_tx_and_release_downlink_permits,
};
use crate::fake_ip::FakeIpPool;
use crate::shared::TargetAddr;
use std::collections::HashMap;
use std::time::Duration;

async fn control_turn(
    dispatch: &OpenDispatch,
    receiver: &mut mpsc::Receiver<HandshakeNotice>,
    server: &mut Endpoint,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
    pool: &mut FakeIpPool,
) {
    dispatch.begin_turn().await;
    let event = std::future::poll_fn(|cx| {
        let mut next = std::pin::pin!(dispatch.recv(receiver));
        Poll::Ready(next.as_mut().poll(cx))
    })
    .await;
    if let Poll::Ready(Some(event)) = event {
        let (global_tx, _global_rx) = mpsc::channel(4);
        let (owned_action_tx, _owned_rx) = mpsc::channel(4);
        HandshakeCompletion::from_loop(
            dispatch,
            event,
            HandshakeCompletion {
                sockets: &mut server.sockets,
                socket_ctxs: contexts,
                global_tx: &global_tx,
                owned_action_tx: &owned_action_tx,
                fake_pool: pool,
                now_secs: 0,
                metrics_handle: &crate::metrics::Metrics::default(),
                downlink_backpressure: DownlinkBackpressureConfig::default(),
                relay_read_hard_pause: false,
            },
        );
    }
    tokio::time::advance(Duration::from_millis(1)).await;
}

#[tokio::test(start_paused = true)]
async fn expired_received_w_keeps_original_payload_until_reset_without_starting_provider() {
    let (mut client, mut server) = receiving_pair().await;
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, mut receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let upstream = Arc::new(NeverOpen::default());
    let handle = server.handle;
    let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
    let mut pool = FakeIpPool::new();
    dispatch.begin_turn().await;
    dispatch.admit_flow(
        upstream.clone(),
        FlowAdmission {
            handle,
            socket: server.sockets.get_mut::<TcpSocket>(handle),
            ctx: contexts.get_mut(&handle).unwrap(),
            fake_pool: &mut pool,
            fake_ip: None,
            target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()),
            now: 0,
        },
    );
    let (id, original_deadline) = {
        let opens = owner.opens();
        let OpenPhase::Flow(flow) = &opens.slots[0].as_ref().unwrap().phase else {
            panic!("actual admission");
        };
        assert_eq!(flow.stage, FlowStage::Waiting);
        assert_eq!(&flow.escrow, b"untouched-rx");
        assert!(flow.w_held && !flow.f_held && flow.attempt.is_none());
        (flow.id, flow.open_deadline)
    };
    assert_eq!(
        original_deadline,
        tokio::time::Instant::now().into_std() + Duration::from_secs(20)
    );
    for _ in 0..31 {
        assert!(owner.take_control());
    }
    assert!(!owner.take_control());
    tokio::time::advance(Duration::from_secs(20)).await;
    let held = std::future::poll_fn(|cx| {
        let mut next = std::pin::pin!(dispatch.recv(&mut receiver));
        Poll::Ready(next.as_mut().poll(cx))
    })
    .await;
    assert!(matches!(held, Poll::Ready(Some(OpenEvent::Yield))));
    {
        let mut opens = owner.opens();
        let flow = opens.flow_mut(id).unwrap();
        assert_eq!(flow.stage, FlowStage::Waiting);
        assert_eq!(flow.open_deadline, original_deadline);
        assert!(flow.rejection.is_none() && flow.stop_deadline.is_none());
        assert!(flow.w_held && !flow.f_held && flow.attempt.is_none());
        assert_eq!(&flow.escrow, b"untouched-rx");
    }
    assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
    for _ in 0..512 {
        control_turn(
            &dispatch,
            &mut receiver,
            &mut server,
            &mut contexts,
            &mut pool,
        )
        .await;
        if server.sockets.get::<TcpSocket>(handle).state() == smoltcp::socket::tcp::State::Closed {
            break;
        }
    }
    assert_eq!(
        upstream.0.load(Ordering::SeqCst),
        0,
        "an expired W must not construct an upstream attempt"
    );
    let stop_deadline = {
        let mut opens = owner.opens();
        let flow = opens.flow_mut(id).unwrap();
        assert_eq!(flow.stage, FlowStage::Rearming);
        assert_eq!(flow.open_deadline, original_deadline);
        assert_eq!(flow.rejection, Some(FlowRejection::Deadline));
        assert!(flow.w_held && !flow.f_held && flow.attempt.is_none());
        assert_eq!(&flow.escrow, b"untouched-rx");
        assert_eq!(
            flow.stop_deadline,
            Some(original_deadline + Duration::from_secs(15))
        );
        flow.stop_deadline
    };
    for _ in 0..256 {
        control_turn(
            &dispatch,
            &mut receiver,
            &mut server,
            &mut contexts,
            &mut pool,
        )
        .await;
    }
    {
        let mut opens = owner.opens();
        let flow = opens.flow_mut(id).unwrap();
        assert_eq!(flow.open_deadline, original_deadline);
        assert_eq!(flow.rejection, Some(FlowRejection::Deadline));
        assert_eq!(flow.stop_deadline, stop_deadline);
        assert_eq!(&flow.escrow, b"untouched-rx");
        let accounting = opens.flow.as_ref().unwrap();
        assert_eq!(
            (
                accounting.w,
                accounting.f,
                accounting.escrow_bytes,
                accounting.released,
                accounting.retry
            ),
            (1, 0, b"untouched-rx".len(), 0, 0)
        );
        assert!(opens.audit_flows());
    }
    assert_eq!(contexts[&handle].conn_epoch, id.epoch);
    assert!(
        server
            .sockets
            .get::<TcpSocket>(handle)
            .remote_endpoint()
            .is_some()
    );
    assert!(
        owner.ledger().first_failure.is_none(),
        "do not consume the terminal cause before receipt"
    );
    server.iface.poll(
        smoltcp::time::Instant::now(),
        &mut server.device,
        &mut server.sockets,
    );
    flush_tx_and_release_downlink_permits(
        &mut server.device,
        &server.sockets,
        &mut contexts,
        "expired-W",
    )
    .await
    .0
    .unwrap();
    client.turn().await;
    assert_eq!(
        client.sockets.get::<TcpSocket>(client.handle).state(),
        smoltcp::socket::tcp::State::Closed
    );
    for _ in 0..512 {
        control_turn(
            &dispatch,
            &mut receiver,
            &mut server,
            &mut contexts,
            &mut pool,
        )
        .await;
        if owner.flow_observation().unwrap().live == 0 {
            break;
        }
    }
    let seen = owner.flow_observation().unwrap();
    assert_eq!(
        (seen.live, seen.w, seen.f, seen.reserved, seen.escrow_bytes),
        (0, 0, 0, 0, 0)
    );
    assert_eq!(
        (seen.released, seen.joined, seen.published, seen.attempts),
        (1, 0, 0, 0)
    );
    assert!(seen.audit && !owner.ledger().quarantined);
    let opens = owner.opens();
    assert_eq!(
        opens.flow.as_ref().unwrap().first_rejection,
        Some(FlowRejection::Deadline)
    );
    assert_eq!(
        opens.flow.as_ref().unwrap().retry,
        0,
        "pre-F timeout cannot mint retry"
    );
    drop(opens);
    assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
    assert_eq!(contexts[&handle].conn_epoch, id.epoch + 1);
    assert_eq!(
        server.sockets.get::<TcpSocket>(handle).state(),
        smoltcp::socket::tcp::State::Listen
    );
    assert_eq!(
        owner
            .ledger()
            .first_failure
            .as_ref()
            .unwrap()
            .facts()
            .io_kind,
        Some(std::io::ErrorKind::TimedOut)
    );
    owner.request_stop();
    for _ in 0..2 {
        assert!(dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
    }
    assert_eq!(owner.ledger().additional_failures, 0);
}

//! Two actual received TCP flows share a real authenticated TUIC ready pool.
//! Root checks, including a newly reserved provider follow-up, spend one budget.
use super::super::super::rearm::FlowAdmission;
use super::super::super::rearm::physical_flow::tests::admission::{Endpoint, receiving_pair};
use super::super::*;
use crate::client_tun::{
    DownlinkBackpressureConfig, HandshakeCompletion, flush_tx_and_release_downlink_permits,
};
use crate::fake_ip::FakeIpPool;
use crate::metrics::Metrics;
use crate::shared::TargetAddr;
use crate::tuic::tcp_fixture::{UdpPair, upstream_endpoint, upstream_ready_connection};
use crate::tuic::{TuicClientConfig, TuicUpstream, encode_authenticate};
use std::collections::HashMap;
use std::time::Duration;

async fn capture<F: Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}

async fn second_received(client: &mut Endpoint, server: &mut Endpoint) -> SocketHandle {
    let socket = || {
        TcpSocket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 4096]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 4096]),
        )
    };
    let mut listener = socket();
    listener.listen(10000).unwrap();
    let second = server.sockets.add(listener);
    let mut app = socket();
    app.connect(
        client.iface.context(),
        (smoltcp::wire::IpAddress::v4(10, 0, 0, 2), 10000),
        20001,
    )
    .unwrap();
    let app = client.sockets.add(app);
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut sent = false;
        loop {
            client.turn().await;
            let socket = client.sockets.get_mut::<TcpSocket>(app);
            if !sent && socket.can_send() {
                assert_eq!(socket.send_slice(b"second").unwrap(), 6);
                sent = true;
            }
            server.turn().await;
            if server.sockets.get::<TcpSocket>(second).can_recv() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    second
}

async fn poll_once(
    dispatch: &OpenDispatch,
    receiver: &mut mpsc::Receiver<HandshakeNotice>,
) -> Poll<Option<OpenEvent>> {
    std::future::poll_fn(|cx| {
        let mut next = std::pin::pin!(dispatch.recv(receiver));
        Poll::Ready(next.as_mut().poll(cx))
    })
    .await
}

fn phases(owner: &SessionControl) -> Vec<Option<TcpOpenPhase>> {
    owner
        .opens()
        .slots
        .iter()
        .flatten()
        .map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                panic!("actual flow");
            };
            flow.attempt.as_ref().map(|attempt| {
                assert_eq!(flow.stage, FlowStage::Opening);
                assert_eq!(attempt.view().connect_written, 0);
                attempt.view().phase
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
enum PreparationCase {
    SharedFollowup,
    FactoryProgress,
    CancelPhase,
}

async fn budget_case(
    upstream: Arc<TuicUpstream>,
    peer: &quinn::Connection,
    budget: usize,
    case: PreparationCase,
) {
    let from_factory = !matches!(case, PreparationCase::SharedFollowup);
    let observed_upstream = upstream.clone();
    let connection_id = {
        let current = upstream_ready_connection(&observed_upstream).unwrap();
        assert_eq!(
            current.bi_stream_open_snapshot(),
            (0, 0, false, false, false)
        );
        current.stable_id()
    };
    let upstream = Arc::new(crate::owned_upstream::LegacyOwnedUpstream::new(upstream));
    let (mut client, mut server) = receiving_pair().await;
    let handles = [
        server.handle,
        second_received(&mut client, &mut server).await,
    ];
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, mut receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let mut contexts: HashMap<_, _> = handles
        .iter()
        .map(|h| (*h, SocketCtx::new(10000)))
        .collect();
    let mut pool = FakeIpPool::new();
    let scenario = capture(async {
        dispatch.begin_turn().await;
        for handle in handles {
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
        }

        if from_factory {
            dispatch.begin_turn().await;
            for _ in budget..32 {
                assert!(owner.take_control());
            }
            let event = poll_once(&dispatch, &mut receiver).await;
            let observed: Vec<_> = owner.opens().slots.iter().flatten().map(|slot| {
                let OpenPhase::Flow(flow) = &slot.phase else { unreachable!() };
                let view = flow.attempt.as_ref().map(|attempt| attempt.view());
                if let Some(view) = view { assert_eq!(view.connect_written, 0); }
                (flow.stage, view.map(|view| view.phase), view.is_some_and(|view| view.lease_held))
            }).collect();
            let first = match budget {
                1 => TcpOpenPhase::Acquiring,
                2 => TcpOpenPhase::Reserved,
                _ => TcpOpenPhase::Opening,
            };
            let second = if budget == 32 {
                (FlowStage::Opening, Some(TcpOpenPhase::Opening), true)
            } else {
                (FlowStage::Waiting, None, false)
            };
            assert_eq!(observed, vec![(FlowStage::Opening, Some(first), budget > 1), second],
                "forward phases must use remaining tokens before another table scan; no-credit Opening stops");
            assert!(matches!(event, Poll::Ready(Some(OpenEvent::Yield))));
            assert_eq!(owner.control_remaining.load(Ordering::Relaxed), 0);
            assert_eq!(owner.opens().cursor, if budget < 4 { 0 } else if budget == 32 { 26 } else { 1 });
            let seen = owner.flow_observation().unwrap();
            assert_eq!((seen.live, seen.w, seen.f, seen.attempts),
                (2, 2, if budget == 32 { 2 } else { 1 }, if budget == 32 { 2 } else { 1 }));
            assert_eq!(seen.escrow_bytes, b"untouched-rx".len() + 6);
            assert_eq!((seen.published, seen.released, seen.joined), (0, 0, 0));
            assert!(seen.audit && !owner.ledger().quarantined);
            if matches!(case, PreparationCase::CancelPhase) {
                assert_eq!(seen.factory_started, 1);
                let current = upstream_ready_connection(&observed_upstream).unwrap();
                assert_eq!(current.stable_id(), connection_id);
                assert_eq!(current.bi_stream_open_snapshot(), (0, 0, false, false, false));
                return; // Stop this actual phase; do not advance it to Opening first.
            }
            if budget < 4 {
                for step in budget + 1..=4 {
                    dispatch.begin_turn().await;
                    for _ in 1..32 { assert!(owner.take_control()); }
                    assert!(matches!(poll_once(&dispatch, &mut receiver).await,
                        Poll::Ready(Some(OpenEvent::Yield))));
                    let expected = if step == 2 { TcpOpenPhase::Reserved } else { TcpOpenPhase::Opening };
                    assert_eq!(phases(&owner), vec![Some(expected), None]);
                    let opens = owner.opens();
                    assert_eq!(opens.cursor, usize::from(step == 4));
                    let OpenPhase::Flow(flow) = &opens.slots[0].as_ref().unwrap().phase else { unreachable!() };
                    assert_eq!(flow.open_steps, step);
                }
                // Actual no-credit Pending releases the cursor to the next flow.
                dispatch.begin_turn().await;
                for _ in 1..32 { assert!(owner.take_control()); }
                let _ = poll_once(&dispatch, &mut receiver).await;
                assert_eq!(phases(&owner), vec![Some(TcpOpenPhase::Opening), Some(TcpOpenPhase::Acquiring)]);
                assert_eq!(owner.opens().cursor, 1);
            }
            return;
        }
        // One factory is actually retained at Acquiring after the turn ends.
        dispatch.begin_turn().await;
        for _ in 1..32 { assert!(owner.take_control()); }
        assert!(matches!(poll_once(&dispatch, &mut receiver).await,
            Poll::Ready(Some(OpenEvent::Yield))));
        assert_eq!(phases(&owner), vec![Some(TcpOpenPhase::Acquiring), None]);
        assert_eq!(owner.opens().cursor, 0);
        dispatch.begin_turn().await;
        for _ in budget..32 {
            assert!(owner.take_control());
        }
        let event = poll_once(&dispatch, &mut receiver).await;
        let expected = if budget == 1 {
            vec![Some(TcpOpenPhase::Reserved), None]
        } else {
            vec![Some(TcpOpenPhase::Opening); 2]
        };
        assert_eq!(
            phases(&owner),
            expected,
            "the same pool preparation must not wait a full scan when another control token exists"
        );
        assert!(matches!(event, Poll::Ready(Some(OpenEvent::Yield))));
        assert_eq!(owner.control_remaining.load(Ordering::Relaxed), 0);
        assert_eq!(owner.opens().cursor, if budget == 1 { 0 } else { 27 });
        let seen = owner.flow_observation().unwrap();
        assert_eq!((seen.live, seen.w, seen.f, seen.attempts),
            (2, 2, if budget == 1 { 1 } else { 2 }, if budget == 1 { 1 } else { 2 }));
        assert_eq!((seen.published, seen.released, seen.joined), (0, 0, 0));
        assert_eq!(seen.escrow_bytes, b"untouched-rx".len() + 6);
        assert!(seen.audit && !owner.ledger().quarantined);
        {
            let opens = owner.opens();
            for (index, slot) in opens.slots.iter().flatten().enumerate() {
                let OpenPhase::Flow(flow) = &slot.phase else {
                    unreachable!()
                };
                assert_eq!(
                    flow.escrow.as_slice(),
                    if index == 0 {
                        b"untouched-rx".as_slice()
                    } else {
                        b"second".as_slice()
                    }
                );
                assert_eq!(
                    flow.attempt.as_ref().is_some_and(|attempt| attempt.view().lease_held),
                    budget == 32 || index == 0
                );
            }
        }
        assert!(peer.close_reason().is_none());
    })
    .await;
    owner.request_stop();
    let (global_tx, _global_rx) = mpsc::channel(4);
    let (owned_action_tx, _owned_rx) = mpsc::channel(4);
    let metrics = Metrics::default();
    let mut first_stop = None;
    let mut dirty = std::collections::HashSet::new();
    let cleanup = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            dispatch.begin_turn().await;
            if let Poll::Ready(Some(event)) = poll_once(&dispatch, &mut receiver).await {
                let check = first_stop.is_none();
                let stop_budget = if budget == 32 {
                    if from_factory { 32 } else { 2 }
                } else {
                    1
                };
                if check {
                    while owner.control_remaining.load(Ordering::Relaxed) > stop_budget - 1 {
                        assert!(owner.take_control());
                    }
                }
                {
                    let mut processing = std::pin::pin!(HandshakeCompletion::from_loop_turn(
                        &dispatch,
                        event,
                        HandshakeCompletion {
                            sockets: &mut server.sockets,
                            socket_ctxs: &mut contexts,
                            global_tx: &global_tx,
                            owned_action_tx: &owned_action_tx,
                            fake_pool: &mut pool,
                            now_secs: 0,
                            metrics_handle: &metrics,
                            downlink_backpressure: DownlinkBackpressureConfig::default(),
                            relay_read_hard_pause: false,
                        },
                        &mut dirty,
                    ));
                    std::future::poll_fn(|cx| {
                        assert!(
                            processing.as_mut().poll(cx).is_ready(),
                            "physical batch must never await another event"
                        );
                        Poll::Ready(())
                    })
                    .await;
                }
                if check {
                    let seen = owner.flow_observation().unwrap();
                    assert_eq!(owner.control_remaining.load(Ordering::Relaxed), 0);
                    assert_eq!(dirty.len(), seen.stopped_producers);
                    first_stop = Some((
                        seen.stopped_producers,
                        seen.live,
                        seen.w,
                        seen.released,
                        seen.escrow_bytes,
                        stop_budget,
                    ));
                }
            }
            server.iface.poll(
                smoltcp::time::Instant::now(),
                &mut server.device,
                &mut server.sockets,
            );
            flush_tx_and_release_downlink_permits(
                &mut server.device,
                &server.sockets,
                &mut contexts,
                "preparation",
            )
            .await
            .0
            .unwrap();
            client.turn().await;
            if dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        cleanup.is_ok(),
        "real root/physical cleanup: {:?}",
        owner.flow_observation()
    );
    let done = owner.flow_observation().unwrap();
    assert_eq!(
        (done.live, done.w, done.f, done.reserved, done.escrow_bytes),
        (0, 0, 0, 0, 0)
    );
    assert_eq!((done.released, done.published, done.joined), (2, 0, 0));
    assert!(done.audit && !owner.ledger().quarantined);
    assert!(client.sockets.iter().all(|(_, socket)| match socket {
        smoltcp::socket::Socket::Tcp(tcp) => tcp.state() == smoltcp::socket::tcp::State::Closed,
        _ => false,
    }));
    let failure = owner.retained_provider_failure().unwrap();
    assert_eq!(failure.1.phase, TcpOpenPhase::Settled);
    assert!(!failure.1.lease_held && !failure.1.retirement_pending);
    assert_eq!(failure.1.connect_written, 0);
    assert!(peer.close_reason().is_none());
    if matches!(case, PreparationCase::CancelPhase) {
        assert_eq!(done.factory_started, 1);
        let opens = owner.opens();
        let source = opens
            .flow
            .as_ref()
            .unwrap()
            .first_provider_failure
            .as_ref()
            .unwrap()
            .failure()
            .unwrap();
        assert!(matches!(source, crate::shared::ClientError::Io(error)
            if error.kind() == std::io::ErrorKind::Interrupted));
        drop(opens);
        let current = upstream_ready_connection(&observed_upstream).unwrap();
        assert_eq!(current.stable_id(), connection_id);
        assert_eq!(
            current.bi_stream_open_snapshot(),
            (0, 0, false, false, false)
        );
        for _ in 0..2 {
            assert!(dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
            assert_eq!(owner.retained_provider_failure().unwrap(), failure);
            let seen = owner.flow_observation().unwrap();
            assert_eq!(
                (seen.factory_started, seen.released, seen.joined),
                (1, 2, 0)
            );
            let ledger = owner.ledger();
            assert_eq!(ledger.additional_failures, 0);
            assert_eq!(
                ledger.first_failure.as_ref().unwrap().facts().io_kind,
                Some(std::io::ErrorKind::Other)
            );
        }
    }
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    let (stopped, live, w, released, escrow, stop_budget) = first_stop.unwrap();
    assert_eq!(
        (stopped, live, w, released, escrow),
        (stop_budget.min(2), 2, 2, 0, b"untouched-rx".len() + 6),
        "one physical turn must use only the remaining shared budget, without releasing custody"
    );
}

async fn real_preparation_case(case: PreparationCase) {
    let pair = UdpPair::new(8, 65536).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 65536, 0, true)));
    let mut config = TuicClientConfig::from_sources(
        Some(&pair.server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap();
    config.tcp_pool = 1;
    let connected = capture(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                TuicUpstream::connect(&config, Arc::new(Metrics::default())),
                async { pair.server.accept().await.unwrap().await.unwrap() }
            )
        })
        .await
        .unwrap()
    })
    .await;
    let (upstream, peer) = match connected {
        Ok((Ok(upstream), peer)) => (Arc::new(upstream), peer),
        failure => {
            pair.close().await;
            match failure {
                Err(panic) => std::panic::resume_unwind(panic),
                _ => panic!("real TUIC setup failed"),
            }
        }
    };
    let endpoint = upstream_endpoint(&upstream);
    let mut auth = None;
    let scenario = capture(async {
        auth = Some(
            tokio::time::timeout(Duration::from_secs(2), peer.accept_uni())
                .await
                .unwrap()
                .unwrap(),
        );
        let observed = auth.as_mut().unwrap().read_to_end(64).await.unwrap();
        let mut token = [0; 32];
        peer.export_keying_material(&mut token, &config.uuid, config.password.as_bytes())
            .unwrap();
        assert_eq!(observed, encode_authenticate(&config.uuid, &token));
        let budgets: &[usize] = match case {
            PreparationCase::SharedFollowup => &[1, 32],
            PreparationCase::FactoryProgress => &[1, 2, 3, 4, 32],
            PreparationCase::CancelPhase => &[1, 2, 3],
        };
        for &budget in budgets {
            budget_case(upstream.clone(), &peer, budget, case).await;
        }
    })
    .await;
    endpoint.close(0u32.into(), b"preparation test cleanup");
    peer.close(0u32.into(), b"preparation test cleanup");
    drop(upstream);
    drop(auth);
    drop(peer);
    let idle = tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle()).await;
    pair.close().await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    idle.unwrap();
}

#[tokio::test]
async fn real_shared_preparation_followup_consumes_budget_and_retires_on_stop() {
    real_preparation_case(PreparationCase::SharedFollowup).await;
}

#[tokio::test]
async fn real_factory_progress_is_bounded_and_parks_on_missing_stream_credit() {
    real_preparation_case(PreparationCase::FactoryProgress).await;
}

#[tokio::test]
async fn real_stop_in_acquiring_reserved_and_opening_never_starts_waiting_factory() {
    real_preparation_case(PreparationCase::CancelPhase).await;
}

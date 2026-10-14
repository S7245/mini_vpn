//! Real F-mode W128 admission under peer-owned QUIC stream-credit pressure.
//! Uses saved session, real TCP packets and authenticated TUIC; no filled slots.
use super::*;
use crate::tuic::tcp_fixture::{UdpPair, upstream_endpoint};
use crate::tuic::{TuicClientConfig, TuicUpstream, encode_authenticate, encode_connect};

// Honor the saved driver's own wakes between bounded app observations. A single
// manual poll followed by a sleep artificially paces the 4224-slot scan.
pub(super) async fn credit_turn<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    include_last: bool,
) {
    apps.turn(include_last).await;
    tokio::select! {
        state = session.run() => panic!("pressure driver ended early: {state:?}"),
        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
    }
}

#[tokio::test(flavor = "current_thread")]
async fn f_w128_credit_pressure_preserves_c129_rx_until_real_publication() {
    let pair = UdpPair::new(8, 1_048_576).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 1_048_576, 0, true)));
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
                _ => panic!("actual TUIC connection setup failed"),
            }
        }
    };
    let endpoint = upstream_endpoint(&upstream);
    let inbound = PacketLink::bounded(2048);
    let outbound = PacketLink::bounded(2048);
    let mut apps = RealApps::new(outbound.clone(), inbound.clone());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("129")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound, outbound),
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let mut auth = None;
    let mut server = None;
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            auth = Some(peer.accept_uni().await.unwrap());
            let observed = auth.as_mut().unwrap().read_to_end(64).await.unwrap();
            let mut token = [0; 32];
            peer.export_keying_material(&mut token, &config.uuid, config.password.as_bytes())
                .unwrap();
            assert_eq!(observed, encode_authenticate(&config.uuid, &token));
        })
        .await
        .unwrap();
        let setup = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                credit_turn(&mut session, &mut apps, false).await;
                let seen = session.control.flow_observation().unwrap();
                if apps.established() == 129 && seen.opening_providers == 128 {
                    break;
                }
            }
        })
        .await;
        assert!(
            setup.is_ok(),
            "actual W128 setup: {:?}",
            session.control.flow_observation()
        );
        assert!(apps.sent[..128].iter().all(|bytes| *bytes == 1));
        let before = session.control.flow_observation().unwrap();
        assert_eq!(
            (before.live, before.w, before.f, before.attempts),
            (128, 128, 128, 128)
        );
        assert_eq!(before.reserved, 128 * crate::client_tun::MAX_UPLINK_BUFFER);
        assert_eq!(
            (before.escrow_bytes, before.published, before.released),
            (128, 0, 0)
        );
        assert!(before.audit);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                credit_turn(&mut session, &mut apps, true).await;
                if apps.sent[128] == 8192
                    && apps.queued(128) == 4096
                    && apps.windows[128] == Some(0)
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
        for _ in 0..40 {
            // Each bounded observation cancels its still-pending run observer.
            credit_turn(&mut session, &mut apps, true).await;
            let held = session.control.flow_observation().unwrap();
            assert_eq!(
                (held.live, held.w, held.f, held.attempts),
                (128, 128, 128, 128)
            );
            assert_eq!(held.opening_providers, 128);
            assert_eq!(held.escrow_bytes, 128);
            assert_eq!((held.published, held.released, held.joined), (0, 0, 0));
            assert_eq!((apps.queued(128), apps.windows[128]), (4096, Some(0)));
            assert!(held.audit && !session.control.ledger().quarantined);
        }
        let peer_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let payload_seen = peer_ready.clone();
        let remote = peer.clone();
        server = Some(tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(10), async {
                let (send, mut recv) = remote.accept_bi().await.unwrap();
                let header =
                    encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                let mut observed = vec![0; header.len() + 1];
                recv.read_exact(&mut observed).await.unwrap();
                assert_eq!(&observed[..header.len()], &header);
                assert!(
                    observed[header.len()] < 128,
                    "C129 cannot jump the first publication"
                );
                payload_seen.store(true, Ordering::Release);
                assert!(
                    send.stopped().await.unwrap().is_some(),
                    "root sends real STOP_SENDING"
                );
                drop(send);
                let mut byte = [0];
                assert!(matches!(
                    recv.read(&mut byte).await,
                    Ok(None) | Err(quinn::ReadError::Reset(_))
                ));
            })
            .await
        }));
        peer.set_max_concurrent_bi_streams(1u32.into());
        let progressed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                credit_turn(&mut session, &mut apps, true).await;
                let seen = session.control.flow_observation().unwrap();
                if seen.published == 1
                    && seen.attempts == 129
                    && seen.opening_providers == 128
                    && apps.queued(128) == 0
                    && peer_ready.load(Ordering::Acquire)
                {
                    break;
                }
            }
        })
        .await;
        assert!(
            progressed.is_ok(),
            "publication/C129 progress: {:?}",
            session.control.flow_observation()
        );
        let after = session.control.flow_observation().unwrap();
        assert_eq!(
            (
                after.live,
                after.w,
                after.f,
                after.published,
                after.released
            ),
            (129, 128, 129, 1, 0)
        );
        assert_eq!(after.reserved, before.reserved);
        assert_eq!(after.escrow_bytes, 127 + 4096);
        assert!(after.audit && peer.close_reason().is_none());
        session.request_stop();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                apps.turn(true).await;
                {
                    let mut observer = Box::pin(session.run());
                    let _ =
                        std::future::poll_fn(|cx| Poll::Ready(observer.as_mut().poll(cx))).await;
                }
                if session.state() == SessionDeviceState::DeviceFailedSettled
                    && server.as_ref().unwrap().is_finished()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let result = server.take().unwrap().await;
        assert!(matches!(result, Ok(Ok(()))), "{result:?}");
        apps.turn(true).await;
        assert!(
            apps.handles
                .iter()
                .all(|handle| apps.sockets.get::<Socket>(*handle).state()
                    == smoltcp::socket::tcp::State::Closed)
        );
        let done = session.control.flow_observation().unwrap();
        assert_eq!(
            (done.live, done.w, done.f, done.reserved, done.escrow_bytes),
            (0, 0, 0, 0, 0)
        );
        assert_eq!((done.published, done.released, done.joined), (1, 129, 3));
        assert!(done.audit && !session.control.ledger().quarantined);
        let failure = session.control.retained_provider_failure().unwrap();
        assert_eq!(failure.2, "I/O error: TUIC open Cancelled");
        assert_eq!(failure.1.phase, crate::upstream::TcpOpenPhase::Settled);
        assert!(!failure.1.lease_held && !failure.1.retirement_pending);
        for _ in 0..2 {
            assert_eq!(
                session.observe_shutdown().await,
                SessionDeviceState::DeviceFailedSettled
            );
            assert_eq!(
                session.control.retained_provider_failure().unwrap(),
                failure
            );
            assert_eq!(session.control.flow_observation().unwrap().released, 129);
        }
        assert!(
            peer.close_reason().is_none(),
            "endpoint teardown is not a settlement receipt"
        );
    })
    .await;
    session.request_stop();
    let state = session.observe_shutdown().await;
    endpoint.close(0u32.into(), b"pressure fixture cleanup");
    peer.close(0u32.into(), b"pressure fixture cleanup");
    if let Some(server) = server {
        let _ = server.await;
    }
    drop(session);
    drop(upstream);
    drop(auth);
    drop(peer);
    let idle = tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle()).await;
    pair.close().await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(state, SessionDeviceState::DeviceFailedSettled);
    idle.unwrap();
}

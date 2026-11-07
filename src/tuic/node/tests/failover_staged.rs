use super::super::*;
use super::running::boot;
use super::startup::{config, finish, guarded, server};
use crate::failover::FailoverUpstream;
use crate::reality_upstream::{RealityClientConfig, RealityUpstream};
use crate::upstream::{OpenedTcpRelay, TcpOpenAttempt, TcpOpenPhase, TcpOpenRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) fn request() -> TcpOpenRequest {
    TcpOpenRequest {
        target: TargetAddr::IpPort("192.0.2.1:443".parse().unwrap()),
        deadline: Instant::now() + Duration::from_secs(20),
    }
}
pub(super) fn reality(server: String, pbk: [u8; 32]) -> Arc<RealityUpstream> {
    Arc::new(RealityUpstream::new(RealityClientConfig {
        server,
        uuid: [0x42; 16],
        pbk,
        short_id: [0; 8],
        sni: "example.com".into(),
    }))
}
pub(super) async fn phase(open: &mut dyn TcpOpenAttempt<OpenedTcpRelay>, goal: TcpOpenPhase) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            std::future::poll_fn(|cx| {
                open.step(cx, Instant::now());
                Poll::Ready(())
            })
            .await;
            if open.view().phase == goal {
                break;
            }
            assert_ne!(
                open.view().phase,
                TcpOpenPhase::Quarantined,
                "{:?}",
                open.failure()
            );
            assert_ne!(
                open.view().phase,
                TcpOpenPhase::Settled,
                "{:?}",
                open.failure()
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn actual_node_failover_staged_tuic_payload_and_retained_aggregate_drain() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let factory = Arc::new(FailoverUpstream::new(
            data.clone(),
            reality("127.0.0.1:9".into(), [0; 32]),
        ));
        // Advisory counter setup; actual publication must reset it exactly once.
        assert!(!factory.state().record_tuic_failure(false, 0));
        assert!(!factory.state().record_tuic_failure(false, 0));
        let mut open = factory
            .clone()
            .staged_tcp_open(request())
            .expect("actual Failover staged factory");
        assert_eq!(open.view().phase, TcpOpenPhase::Unstarted);
        tokio::select! {
            _ = host.run() => panic!("node must remain driven"),
            _ = phase(open.as_mut(), TcpOpenPhase::Ready) => {}
        }
        let OpenedTcpRelay::Generic(mut io) = open.take_carrier(Instant::now()).unwrap() else {
            panic!("Generic");
        };
        assert!(!factory.state().record_tuic_failure(false, 0));
        assert!(!factory.state().record_tuic_failure(false, 0));
        assert!(open.take_carrier(Instant::now()).is_none());
        assert!(
            factory.state().record_tuic_failure(false, 0),
            "second take cannot reset failures"
        );
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                async {
                    io.write_all(b"ping").await.unwrap();
                    io.shutdown().await.unwrap();
                    let mut response = Vec::new();
                    io.read_to_end(&mut response).await.unwrap();
                    assert_eq!(response, b"pong");
                },
                async {
                    let (mut tx, mut rx) = peers[0].accept_bi().await.unwrap();
                    assert_eq!(
                        rx.read_to_end(128).await.unwrap(),
                        [encode_connect(&request().target), b"ping".to_vec()].concat()
                    );
                    tx.write_all(b"pong").await.unwrap();
                    tx.finish().unwrap();
                }
            );
        })
        .await
        .unwrap();
        drop(io);
        let deadline = Instant::now() + Duration::from_secs(5);
        std::future::poll_fn(|cx| {
            open.settle(cx, Instant::now(), deadline);
            Poll::Ready(())
        })
        .await;
        phase(open.as_mut(), TcpOpenPhase::Settled).await;
        assert!(!open.view().lease_held && !open.view().retirement_pending);
        drop(factory);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        assert!(host.resources.state().endpoint_drained);
        assert_eq!(
            open.view().phase,
            TcpOpenPhase::Settled,
            "retained actual aggregate"
        );
    })
    .await;
    finish(host, server, peers, result).await;
}

// Production F repeatedly calls settle after actual carrier roles return.
pub(super) async fn settled(open: &mut dyn TcpOpenAttempt<OpenedTcpRelay>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            std::future::poll_fn(|cx| {
                open.settle(cx, Instant::now(), deadline);
                Poll::Ready(())
            })
            .await;
            if open.view().phase == TcpOpenPhase::Settled {
                break;
            }
            assert_ne!(
                open.view().phase,
                TcpOpenPhase::Quarantined,
                "{:?}",
                open.failure()
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
pub(super) async fn reality_peer() -> (Arc<RealityUpstream>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sk, pk) = crate::reality::auth::generate_ephemeral_keypair();
    let upstream = reality(listener.local_addr().unwrap().to_string(), pk);
    let peer = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        crate::reality_upstream::reality_server_sim(io, sk, [0x42; 16]).await;
    });
    (upstream, peer)
}
async fn reality_payload(open: &mut dyn TcpOpenAttempt<OpenedTcpRelay>) {
    phase(open, TcpOpenPhase::Ready).await;
    let OpenedTcpRelay::Generic(mut io) = open.take_carrier(Instant::now()).unwrap() else {
        panic!("Generic REALITY");
    };
    io.write_all(b"ping").await.unwrap();
    io.shutdown().await.unwrap();
    let mut response = Vec::new();
    io.read_to_end(&mut response).await.unwrap();
    assert_eq!(response, b"pong");
    drop(io);
    settled(open).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_initial_reality_ignores_busy_primary_and_udp_stays_tuic() {
    use crate::owned_upstream::UdpDownlinkEvent;
    use crate::upstream::DatagramUpstream;
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let mut source = data.start_udp_source().unwrap();
        let (backup, peer) = reality_peer().await;
        let factory = Arc::new(FailoverUpstream::new(data.clone(), backup));
        assert!(factory.state().record_blackhole(0));
        let busy = data.tcp_pool_slots[0].state.lock().await;
        let mut open = factory.clone().staged_tcp_open(request()).unwrap();
        tokio::select! {
            result = host.run() => panic!("node ended {result:?}"),
            _ = reality_payload(open.as_mut()) => {}
        }
        drop(busy);
        tokio::time::timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
        let target = request().target;
        let uplink = encode_packet(7, &target, b"pinned-uplink");
        // Releasing a fair Mutex hands it to the original queued UDP pump.
        // Observe its actual lock return before submitting a new packet.
        tokio::time::timeout(Duration::from_secs(2), async {
            while crate::failover::HealthProbe::snapshot(data.as_ref()).is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        factory.send_udp(uplink.clone()).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), peers[0].read_datagram())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            uplink
        );
        peers[0]
            .send_datagram(encode_packet(7, &target, b"typed-downlink").into())
            .unwrap();
        let clock = Instant::now();
        let event = tokio::time::timeout(Duration::from_secs(2), source.recv(&clock))
            .await
            .unwrap()
            .unwrap();
        let UdpDownlinkEvent::Deliver(packet) = event else {
            panic!("typed delivery");
        };
        assert_eq!(packet.payload().as_ref(), b"typed-downlink");
        drop(packet);
        drop(source);
        drop(factory);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        assert!(host.resources.state().endpoint_drained);
        assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn original_node_tokens_drop_and_demand_settle_precede_only_cas_winner_fallback() {
    use crate::failover::{FallbackAdmission, HealthProbe, TcpLeg};
    use tokio::sync::oneshot;
    type DriverOutcome = Result<(), Box<dyn std::any::Any + Send>>;
    struct Release(Arc<recovery::ClearGate>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release(2);
        }
    }
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = Some(NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default())));
    let mut peers = Vec::new();
    // Original host and its unique driver handle stay outside the guarded scenario.
    let mut driver: Option<tokio::task::JoinHandle<(NodeHost, DriverOutcome)>> = None;
    let mut stop = None;
    let gate = Arc::new(recovery::ClearGate::default());
    let result = guarded(async {
        let _release = Release(gate.clone());
        boot(host.as_mut().unwrap(), &server, &cfg, &mut peers).await;
        let data = host.as_mut().unwrap().data().unwrap();
        let owner = host.as_ref().unwrap().resources.clone();
        owner.state().recovery.clear_gate = Some(gate.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (sk, pk) = crate::reality::auth::generate_ephemeral_keypair();
        let factory = Arc::new(FailoverUpstream::new(
            data.clone(),
            reality(listener.local_addr().unwrap().to_string(), pk),
        ));
        peers[0].close(17u32.into(), b"real failed primary before fallback");
        tokio::time::timeout(Duration::from_secs(2), async {
            while !data.is_dead().await {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let mut opens = [
            factory.clone().staged_tcp_open(request()).unwrap(),
            factory.clone().staged_tcp_open(request()).unwrap(),
        ];
        let expired_request = request();
        let original_deadline = expired_request.deadline;
        let mut expired = factory.clone().staged_tcp_open(expired_request).unwrap();
        let (tx, rx) = oneshot::channel();
        stop = Some(tx);
        let mut original = host.take().unwrap();
        driver = Some(tokio::spawn(async move {
            // Catch inside the task while the original host remains outside that
            // observation future. Even observer panic returns the actual owner.
            let outcome = guarded(async {
                tokio::select! {
                    _ = rx => {},
                    result = original.run() => panic!("original node ended {result:?}"),
                }
            })
            .await;
            (original, outcome)
        }));
        tokio::time::timeout(Duration::from_secs(4), async {
            tokio::join!(
                async {
                    while !gate.arrived(1) {
                        for open in &mut opens {
                            open.step(
                                &mut Context::from_waker(std::task::Waker::noop()),
                                Instant::now(),
                            );
                        }
                        expired.step(
                            &mut Context::from_waker(std::task::Waker::noop()),
                            Instant::now(),
                        );
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                },
                async {
                    for _ in 0..2 {
                        server.accept().await.unwrap().refuse();
                    }
                }
            );
        })
        .await
        .unwrap();
        for checkpoint in 1..=2 {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !gate.arrived(checkpoint) {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            {
                let state = owner.state();
                assert!(state.recovery.preparation.is_none() && state.recovery.permit.is_none());
                assert_eq!(state.attempts_started, 3);
                assert_eq!(state.attempts_drained, 2);
                assert!(
                    state
                        .connections
                        .iter()
                        .flatten()
                        .all(|entry| entry.published)
                );
            }
            assert_eq!(
                data.tcp_pool_admission.preparing_slots[0].load(Ordering::Acquire),
                checkpoint == 1
            );
            assert_eq!(
                owner
                    .state()
                    .recovery
                    .primary_permit
                    .load(Ordering::Acquire),
                checkpoint == 1
            );
            for _ in 0..8 {
                for open in &mut opens {
                    open.step(
                        &mut Context::from_waker(std::task::Waker::noop()),
                        Instant::now(),
                    );
                }
                expired.step(
                    &mut Context::from_waker(std::task::Waker::noop()),
                    Instant::now(),
                );
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            assert_eq!(
                factory.state().active_leg(),
                TcpLeg::Tuic,
                "no CAS before actual original clear receipt"
            );
            assert_ne!(
                data.snapshot().unwrap().fallback,
                FallbackAdmission::Allowed
            );
            for open in &opens {
                assert_eq!(open.view().phase, TcpOpenPhase::Acquiring);
                assert!(!open.view().lease_held && !open.view().retirement_pending);
                assert!(
                    open.failure().is_none(),
                    "internal retirement is not outward failure"
                );
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(20), listener.accept())
                    .await
                    .is_err()
            );
            if checkpoint == 2 {
                // The third original request expires while classification waits.
                // Synthetic control time, not a wall-clock recovery SLO.
                assert_eq!(expired.view().phase, TcpOpenPhase::Acquiring);
                expired.step(
                    &mut Context::from_waker(std::task::Waker::noop()),
                    original_deadline,
                );
                assert_eq!(expired.view().phase, TcpOpenPhase::Settled);
                assert!(expired.failure().is_some());
                assert!(!expired.view().lease_held && !expired.view().retirement_pending);
                assert_eq!(factory.state().active_leg(), TcpLeg::Tuic);
            }
            gate.release(checkpoint);
        }
        // The original receipt is complete, but Busy alone must still block CAS.
        let busy = data.tcp_pool_slots[0].state.lock().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while owner.state().recovery.fallback_pending() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(data.snapshot(), None);
        for open in &mut opens {
            open.step(
                &mut Context::from_waker(std::task::Waker::noop()),
                Instant::now(),
            );
            assert_eq!(open.view().phase, TcpOpenPhase::Acquiring);
        }
        assert_eq!(factory.state().active_leg(), TcpLeg::Tuic);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        drop(busy);
        tokio::time::timeout(Duration::from_secs(4), async {
            tokio::join!(
                async {
                    loop {
                        for open in &mut opens {
                            open.step(
                                &mut Context::from_waker(std::task::Waker::noop()),
                                Instant::now(),
                            );
                        }
                        if let Some(winner) = opens
                            .iter()
                            .position(|open| open.view().phase == TcpOpenPhase::Ready)
                        {
                            let loser = 1 - winner;
                            assert_eq!(opens[loser].view().phase, TcpOpenPhase::Settled);
                            assert!(opens[loser].failure().is_some());
                            reality_payload(opens[winner].as_mut()).await;
                            break;
                        }
                        assert!(
                            opens
                                .iter()
                                .all(|open| open.view().phase != TcpOpenPhase::Quarantined)
                        );
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                },
                async {
                    let (io, _) = listener.accept().await.unwrap();
                    crate::reality_upstream::reality_server_sim(io, sk, [0x42; 16]).await;
                }
            );
        })
        .await
        .unwrap();
        assert_eq!(factory.state().active_leg(), TcpLeg::Reality);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err(),
            "CAS loser never opens backup"
        );
        drop(factory);
        drop(data);
        // Retained settled aggregate boxes must not keep the original Node DATA.
        stop.take().unwrap().send(()).unwrap();
        let (original, outcome) = driver.take().unwrap().await.unwrap();
        assert!(outcome.is_ok());
        host = Some(original);
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(15),
                host.as_mut().unwrap().drive_shutdown()
            )
            .await
            .unwrap(),
            NodeShutdown::FailedSettled
        );
        assert!(host.as_ref().unwrap().resources.state().endpoint_drained);
        assert!(
            opens
                .iter()
                .all(|open| open.view().phase == TcpOpenPhase::Settled)
        );
    })
    .await;
    gate.release(2);
    if let Some(stop) = stop.take() {
        let _ = stop.send(());
    }
    let driver_outcome = if let Some(driver) = driver.take() {
        let (original, outcome) = driver.await.unwrap();
        host = Some(original);
        outcome
    } else {
        Ok(())
    };
    finish(host.unwrap(), server, peers, result.and(driver_outcome)).await;
}

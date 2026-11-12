//! Original health task + true Node recovery: observation hooks never drive work.
use super::*;
use crate::reality_upstream::{RealityClientConfig, RealityUpstream};
use device::{device, Gate};
use fixture::{auth, capture, config};
use std::sync::atomic::Ordering;

fn backup() -> RealityUpstream {
    RealityUpstream::new(RealityClientConfig {
        server: "127.0.0.1:9".into(),
        uuid: [0x42; 16],
        pbk: [0; 32],
        short_id: [0; 8],
        sni: "example.com".into(),
    })
}
async fn until<F: Future<Output = Completion>>(
    service: &mut Service<F>,
    budget: Duration,
    ready: impl Fn() -> bool,
) {
    tokio::time::timeout(budget, async {
        tokio::select! {
            biased;
            _=async {while !ready() {tokio::time::sleep(Duration::from_millis(1)).await;}}=>{},
            report=service.run()=>panic!("service ended before witness {report:?}"),
        }
    })
    .await
    .unwrap();
}
async fn boot<F: Future<Output = Completion>>(
    service: &mut Service<F>,
    server: &quinn::Endpoint,
    cfg: &TuicClientConfig,
) -> quinn::Connection {
    let peer=tokio::time::timeout(Duration::from_secs(3),async {
        tokio::select! {
            biased;
            peer=async {let p=server.accept().await.unwrap().await.unwrap();auth(&p,cfg).await;p}=>peer,
            report=service.run()=>panic!("unexpected boot {report:?}"),
        }
    }).await.unwrap();
    peer
}
async fn drain(mut server: quinn::Endpoint, peers: Vec<quinn::Connection>) {
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), drain.drained())
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn stop_keeps_original_real_probe_and_node_driven_until_actual_health_and_device_joins() {
    let server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let gate = Arc::new(Gate::default());
    let mut service = prepare_with(
        device(gate.clone()),
        Configuration::failover(cfg.clone(), backup()),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let mut peers = Vec::new();
    let scenario = capture(async {
        peers.push(boot(&mut service, &server, &cfg).await);
        until(&mut service, Duration::from_secs(2), || {
            service_control_ready(&handle.0)
        })
        .await;
        let observation = service
            .control
            .health_observation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        until(&mut service, Duration::from_secs(2), || {
            observation.ticks.load(Ordering::Acquire) >= 1
        })
        .await;
        let state = service
            .control
            .health_state
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        state.set_leg(crate::failover::TcpLeg::Reality); // expose the original 30s probe schedule
        until(&mut service, Duration::from_secs(29), || {
            observation.ticks.load(Ordering::Acquire) >= 10
        })
        .await;
        assert_eq!(observation.probes_started.load(Ordering::Acquire), 0);
        peers[0].close(17u32.into(), b"primary lost before actual health tick");
        // No server.accept yet: original physical recovery handshake cannot finish.
        until(&mut service, Duration::from_secs(5), || {
            observation.probes_started.load(Ordering::Acquire) == 1
        })
        .await;
        assert_eq!(observation.probes_finished.load(Ordering::Acquire), 0);
        let generation=service.control.udp_generation.lock().unwrap().as_ref().unwrap().clone();
        eprintln!("DIAGNOSTIC before-stop (consumer,epoch,sealed,valid,absent-publication-rejections)={:?}",generation.diagnostic());
        handle.request_stop();
        let deadline = *service.control.deadline.lock().unwrap();
        assert!(deadline.is_some());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), handle.observe_shutdown())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), service.run())
                .await
                .is_err()
        );
        assert_eq!(*service.control.deadline.lock().unwrap(), deadline);
        assert_eq!(observation.probes_finished.load(Ordering::Acquire), 0);
        assert!(!observation.joined.load(Ordering::Acquire));
        assert!(gate.polls.load(Ordering::SeqCst) > 0);
        assert_eq!(gate.joins.load(Ordering::SeqCst), 0);
        assert_eq!(generation.diagnostic().0,0,"session retains actual source until original health join");
        eprintln!("DIAGNOSTIC actual-device-stop (consumer,epoch,sealed,valid,absent-publication-rejections)={:?}",generation.diagnostic());
        // Same saved root must continue the real Node handshake/AUTH after stop.
        peers.push(boot(&mut service, &server, &cfg).await);
        until(&mut service, Duration::from_secs(3), || {
            observation.joined.load(Ordering::Acquire)
        })
        .await;
        assert_eq!(
            (
                observation.probes_started.load(Ordering::Acquire),
                observation.probes_finished.load(Ordering::Acquire),
                observation.probes_ok.load(Ordering::Acquire)
            ),
            (1, 1, 1)
        );
        assert!(!observation.quarantined.load(Ordering::Acquire));
        assert!(
            peers[1].close_reason().is_none(),
            "node cannot clean before actual business join"
        );
        assert_eq!(gate.joins.load(Ordering::SeqCst), 0);
        assert_eq!(gate.drops.load(Ordering::SeqCst), 0);
        gate.open();
    })
    .await;
    handle.request_stop();
    gate.open();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        eprintln!(
            "DIAGNOSTIC original-node-outcome={:?}, generation={:?}",
            report.node,
            service
                .control
                .udp_generation
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .diagnostic()
        );
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        let generation = service
            .control
            .udp_generation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .diagnostic();
        assert_eq!(generation.4, 0, "no publication to an absent consumer");
        assert_eq!(
            generation.1, 2,
            "actual successor published before consumer retirement"
        );
        assert_eq!(
            (
                gate.joins.load(Ordering::SeqCst),
                gate.drops.load(Ordering::SeqCst)
            ),
            (1, 1)
        );
        let done = service.completion.as_ref().unwrap();
        assert!(done._health.as_ref().unwrap().failure().is_none());
        assert!(done.control.driver_complete.load(Ordering::Acquire));
        assert_eq!(handle.observe_shutdown().await, Phase::Complete);
    })
    .await;
    drain(server, peers).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
}
fn service_control_ready(control: &Control) -> bool {
    control.health_observation.lock().unwrap().is_some()
}

#[tokio::test(flavor = "current_thread")]
async fn original_health_task_join_error_stops_business_and_keeps_original_node_in_quarantine() {
    let server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let gate = Arc::new(Gate::default());
    let mut service = prepare_with(
        device(gate.clone()),
        Configuration::failover(cfg.clone(), backup()),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    // Explicit cancellation of the original task, not a natural OS/peer fault.
    service.control.abort_health.store(true, Ordering::Release);
    let handle = service.take_handle().unwrap();
    let mut peers = Vec::new();
    let scenario = capture(async {
        peers.push(boot(&mut service, &server, &cfg).await);
        until(&mut service, Duration::from_secs(2), || {
            service_control_ready(&handle.0)
        })
        .await;
        let observation = service
            .control
            .health_observation
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        until(&mut service, Duration::from_secs(2), || {
            observation.joined.load(Ordering::Acquire) && gate.polls.load(Ordering::SeqCst) > 0
        })
        .await;
        assert!(observation.quarantined.load(Ordering::Acquire));
        assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
        let original_deadline = *service.control.deadline.lock().unwrap();
        assert!(original_deadline.is_some());
        let control = service
            .control
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        let first = {
            let ledger = control.ledger();
            assert_eq!(ledger.additional_failures, 0);
            ledger.first_failure.as_ref().unwrap() as *const RetainedFailure
        };
        assert!(!control.driver_complete.load(Ordering::Acquire));
        assert!(peers[0].close_reason().is_none());
        gate.open();
        until(&mut service, Duration::from_secs(3), || {
            control.driver_complete.load(Ordering::Acquire)
        })
        .await;
        assert_eq!(
            (
                gate.joins.load(Ordering::SeqCst),
                gate.drops.load(Ordering::SeqCst)
            ),
            (1, 1)
        );
        assert!(
            peers[0].close_reason().is_none(),
            "health Q never starts node cleanup"
        );
        assert!(service.completion.is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), service.run())
                .await
                .is_err()
        );
        assert_eq!(*service.control.deadline.lock().unwrap(), original_deadline);
        {
            let ledger = control.ledger();
            assert_eq!(
                ledger.first_failure.as_ref().unwrap() as *const RetainedFailure,
                first
            );
            assert_eq!(ledger.additional_failures, 0);
        }
        assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
    })
    .await;
    // Forced destruction of retained Q is not asynchronous Node settlement.
    drop(service);
    drain(server, peers).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
}

#[tokio::test(start_paused = true)]
async fn failover_cold_rejection_never_constructs_node_or_health_and_keeps_original_device() {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let gate = Arc::new(Gate::default());
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 65536;
    let mut service = prepare_with(
        device(gate.clone()),
        Configuration::failover(config(socket.local_addr().unwrap()), backup()),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(1), service.run())
            .await
            .is_err()
    );
    let first = *service.control.deadline.lock().unwrap();
    assert!(first.is_some());
    assert!(service.control.health_observation.lock().unwrap().is_none());
    assert!(service.control.udp_generation.lock().unwrap().is_none());
    assert_eq!(gate.joins.load(Ordering::SeqCst), 0);
    assert_eq!(handle.observe_shutdown().await, Phase::Incomplete);
    assert_eq!(*service.control.deadline.lock().unwrap(), first);
    assert_eq!(gate.drops.load(Ordering::SeqCst), 0);
    gate.open();
    let report = service.run().await;
    assert_eq!(report.node, None);
    assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
    let done = service.completion.as_ref().unwrap();
    assert!(done._health.is_none());
    assert_eq!(
        (
            gate.joins.load(Ordering::SeqCst),
            gate.drops.load(Ordering::SeqCst)
        ),
        (1, 1)
    );
    assert_eq!(
        done.control
            .ledger()
            .first_failure
            .as_ref()
            .unwrap()
            .facts()
            .class,
        crate::shared::errors::FailureClass::InvalidTarget
    );
    let mut bytes = [0; 128];
    assert_eq!(
        socket.recv_from(&mut bytes).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[tokio::test(flavor = "current_thread")]
async fn late_device_drop_secondary_keeps_actual_session_and_node_in_quarantine() {
    let server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let gate = Arc::new(Gate::default());
    gate.open();
    let mut service = prepare(
        device(gate.clone()),
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let mut peers = Vec::new();
    let scenario = capture(async {
        peers.push(boot(&mut service, &server, &cfg).await);
        until(&mut service, Duration::from_secs(2), || {
            handle.0.session.lock().unwrap().is_some()
        })
        .await;
        let control = service
            .control
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        gate.drop_secondary.store(true, Ordering::SeqCst);
        handle.request_stop();
        until(&mut service, Duration::from_secs(3), || {
            control.ledger().quarantined()
        })
        .await;
        assert_eq!(
            (
                gate.joins.load(Ordering::SeqCst),
                gate.drops.load(Ordering::SeqCst)
            ),
            (1, 1)
        );
        assert!(!control.driver_complete.load(Ordering::Acquire));
        assert!(service.completion.is_none());
        {
            let ledger = control.ledger();
            let facts = ledger.first_failure.as_ref().unwrap().facts();
            assert_eq!(facts.origin.phase, FailurePhase::DeviceDrop);
            assert_eq!(facts.class, crate::shared::errors::FailureClass::Panic);
            assert_eq!(
                (ledger.additional_failures, ledger.unknown_panic_payloads),
                (1, 1)
            );
        }
        assert!(
            peers[0].close_reason().is_none(),
            "late Q blocks node cleanup"
        );
        assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), service.run())
                .await
                .is_err()
        );
        assert!(!control.driver_complete.load(Ordering::Acquire));
    })
    .await;
    drop(service); // forced Q shell destruction, never clean asynchronous shutdown
    drain(server, peers).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
}

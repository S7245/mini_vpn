use super::*;
use device::{device, Gate};
use fixture::{auth, capture, config};
use std::sync::atomic::Ordering;

#[tokio::test(start_paused = true)]
async fn rejected_config_owns_device_and_freezes_first_stop_deadline() {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let gate = Arc::new(Gate::default());
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 65536;
    let mut service = prepare(
        device(gate.clone()),
        config(socket.local_addr().unwrap()),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    assert!(service.take_handle().is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(1), service.run())
            .await
            .is_err()
    );
    assert!(gate.polls.load(Ordering::SeqCst) > 0);
    assert_eq!(gate.joins.load(Ordering::SeqCst), 0);
    let original = *service.control.deadline.lock().unwrap();
    assert!(
        original.is_some(),
        "internal config rejection must start the original stop clock"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(1), handle.observe_shutdown())
            .await
            .is_err()
    );
    assert_eq!(*service.control.deadline.lock().unwrap(), original);
    assert_eq!(handle.observe_shutdown().await, Phase::Incomplete);
    assert_eq!(*service.control.deadline.lock().unwrap(), original);
    assert_eq!(gate.drops.load(Ordering::SeqCst), 0);
    gate.open();
    let report = service.run().await;
    assert_eq!(report.node, None);
    assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
    assert_eq!(gate.joins.load(Ordering::SeqCst), 1);
    assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
    assert_eq!(service.run().await, report);
    assert_eq!(handle.observe_shutdown().await, Phase::Complete);
    let completion = service.completion.as_ref().unwrap();
    let ledger = completion.control.ledger();
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().class,
        crate::shared::errors::FailureClass::InvalidTarget
    );
    assert!(!ledger.quarantined);
    let mut b = [0; 2048];
    assert_eq!(
        socket.recv_from(&mut b).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

async fn boot<F: std::future::Future<Output = Completion>>(
    service: &mut Service<F>,
    server: &quinn::Endpoint,
    cfg: &TuicClientConfig,
) -> quinn::Connection {
    let peer=tokio::time::timeout(Duration::from_secs(3),async {
        tokio::select! {
            peer=async {let p=server.accept().await.unwrap().await.unwrap(); auth(&p,cfg).await; p}=>peer,
            report=service.run()=>panic!("unexpected startup completion {report:?}"),
        }
    }).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), service.run())
            .await
            .is_err()
    );
    peer
}

async fn close_server(mut server: quinn::Endpoint, peer: Option<quinn::Connection>) {
    if let Some(peer) = peer {
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

#[tokio::test]
async fn cancelled_run_and_stop_keep_node_alive_until_actual_device_join() {
    let server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let gate = Arc::new(Gate::default());
    let mut service = prepare(
        device(gate.clone()),
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let mut peer = None;
    let scenario = capture(async {
        peer = Some(boot(&mut service, &server, &cfg).await);
        assert_eq!(*service.control.phase.lock().unwrap(), Phase::Running);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), service.run())
                .await
                .is_err()
        );
        assert_eq!(
            gate.stops.load(Ordering::SeqCst),
            0,
            "run observer cancellation is not stop"
        );
        handle.request_stop();
        let deadline = *service.control.deadline.lock().unwrap();
        for _ in 0..2 {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), service.run())
                    .await
                    .is_err()
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(1), handle.observe_shutdown())
                    .await
                    .is_err()
            );
            assert_eq!(*service.control.deadline.lock().unwrap(), deadline);
            assert_eq!(gate.joins.load(Ordering::SeqCst), 0);
            assert_eq!(gate.drops.load(Ordering::SeqCst), 0);
            assert!(
                peer.as_ref().unwrap().close_reason().is_none(),
                "node must not close before original device join"
            );
        }
        assert!(gate.polls.load(Ordering::SeqCst) > 0);
    })
    .await;
    gate.open();
    handle.request_stop();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!(gate.joins.load(Ordering::SeqCst), 1);
        assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
        assert_eq!(handle.observe_shutdown().await, Phase::Complete);
    })
    .await;
    let server_cleanup = capture(close_server(server, peer)).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = server_cleanup {
        std::panic::resume_unwind(e);
    }
}

#[tokio::test]
async fn original_node_task_failure_stops_business_and_retains_quarantined_node() {
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
    service.control.abort_stats.store(true, Ordering::Release);
    let handle = service.take_handle().unwrap();
    let mut peer = None;
    let scenario=capture(async {
        peer=Some(boot(&mut service,&server,&cfg).await);
        let observed=service.control.clone();
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5),async {
            tokio::select! {
                _=async {while !observed.node_fault_settled.load(Ordering::Acquire) {tokio::time::sleep(Duration::from_millis(1)).await;}}=>{},
                report=service.run()=>panic!("Q node must keep its occupied slot {report:?}"),
            }
        }).await.unwrap();
        assert_eq!(handle.observe_shutdown().await,Phase::Quarantined);
        assert!(service.completion.is_none());
        assert_eq!(gate.joins.load(Ordering::SeqCst),1);assert_eq!(gate.drops.load(Ordering::SeqCst),1);
        assert!(peer.as_ref().unwrap().close_reason().is_some());
        let control=service.control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert!(control.driver_complete.load(Ordering::Acquire));
        assert!(tokio::time::timeout(Duration::from_millis(10),service.run()).await.is_err());
        assert!(service.control.node_fault_settled.load(Ordering::Acquire));
    }).await;
    // Explicit destruction of a physically settled Q diagnostic owner is not a clean verdict.
    drop(service);
    let cleanup = capture(close_server(server, peer)).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
}

#[tokio::test]
async fn cancelled_startup_keeps_original_connecting_until_device_join_then_node_drain() {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let gate = Arc::new(Gate::default());
    let mut service = prepare(
        device(gate.clone()),
        config(socket.local_addr().unwrap()),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let scenario = capture(async {
        for _ in 0..2 {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), service.run())
                    .await
                    .is_err()
            );
        }
        assert_eq!(*service.control.phase.lock().unwrap(), Phase::Starting);
        assert_eq!(gate.stops.load(Ordering::SeqCst), 0);
        drop(handle);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), service.run())
                .await
                .is_err()
        );
        assert!(gate.polls.load(Ordering::SeqCst) > 0);
        assert_eq!(gate.drops.load(Ordering::SeqCst), 0);
    })
    .await;
    gate.open();
    service.control.request_stop();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert_eq!(report.node, Some(NodeShutdown::StartupResourcesSettled));
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        service
            .completion
            .as_ref()
            .unwrap()
            ._node
            .as_ref()
            .unwrap()
            .assert_service_clean_for_test();
        assert_eq!(gate.joins.load(Ordering::SeqCst), 1);
        assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
    })
    .await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
}

#[tokio::test]
async fn actual_one_shot_device_write_error_retains_os_facts_before_node_cleanup() {
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
    let mut peer = None;
    let scenario = capture(async {
        peer = Some(boot(&mut service, &server, &cfg).await);
        gate.write_error.store(true, Ordering::SeqCst);
        let report = tokio::time::timeout(Duration::from_secs(3), service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        let completion = service.completion.as_ref().unwrap();
        completion
            ._node
            .as_ref()
            .unwrap()
            .assert_service_clean_for_test();
        let ledger = completion.control.ledger();
        let failure = ledger.first_failure.as_ref().unwrap().facts();
        assert_eq!(failure.raw_os_code, Some(5));
        assert_eq!(failure.origin.phase, FailurePhase::DeviceWrite);
        assert!(!ledger.quarantined);
        assert_eq!(gate.joins.load(Ordering::SeqCst), 1);
        assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
    })
    .await;
    service.control.request_stop();
    gate.open();
    let cleanup = capture(async {
        tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
    })
    .await;
    let server_cleanup = capture(close_server(server, peer)).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = server_cleanup {
        std::panic::resume_unwind(e);
    }
}

use super::*;
use device::{device, Gate};
use fixture::{auth, capture, config};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn actual_device_read_fatal_retains_original_business_and_node_in_quarantine() {
    let mut server = quinn::Endpoint::server(
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
    let mut peer = None;
    let scenario=capture(async {
        tokio::select! {
            remote=async {let p=server.accept().await.unwrap().await.unwrap();auth(&p,&cfg).await;p}=>peer=Some(remote),
            report=service.run()=>panic!("premature completion {report:?}"),
        }
        assert!(tokio::time::timeout(Duration::from_millis(10),service.run()).await.is_err());
        gate.read_error.store(true,Ordering::SeqCst);
        let observed=service.control.clone();
        tokio::time::timeout(Duration::from_secs(3),async {
            tokio::select! {
                _=async {while *observed.phase.lock().unwrap()!=Phase::Quarantined {tokio::time::sleep(Duration::from_millis(1)).await;}}=>{},
                report=service.run()=>panic!("read fatal must retain actual DATA {report:?}"),
            }
        }).await.unwrap();
        let control=service.control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let ledger=control.ledger();let facts=ledger.first_failure.as_ref().unwrap().facts();
        assert!(ledger.quarantined());assert_eq!(facts.raw_os_code,Some(5));assert_eq!(facts.origin.phase,FailurePhase::DeviceRead);drop(ledger);
        assert!(!control.driver_complete.load(Ordering::Acquire));
        for _ in 0..2 {
            assert_eq!(handle.observe_shutdown().await,Phase::Quarantined);
            assert!(tokio::time::timeout(Duration::from_millis(10),service.run()).await.is_err());
            assert!(service.completion.is_none());
            assert_eq!(gate.joins.load(Ordering::SeqCst),0);assert_eq!(gate.drops.load(Ordering::SeqCst),0);
            assert!(peer.as_ref().unwrap().close_reason().is_none());
        }
    }).await;
    // Explicit forced teardown after all retention assertions; never a client/node
    // async-clean verdict. Only the server endpoint's drain is observed below.
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"forced Q test teardown");
    }
    server.close(0u32.into(), b"forced Q test teardown");
    drop(service);
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    let cleanup = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    assert!(cleanup.unwrap());
    assert_eq!(gate.joins.load(Ordering::SeqCst), 0);
    assert_eq!(
        gate.drops.load(Ordering::SeqCst),
        1,
        "forced shell destruction is not join"
    );
}

#[tokio::test]
async fn settled_failed_open_keeps_original_error_without_holding_node_data() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 0, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = fixture::Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound, outbound),
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let mut peer = None;
    let mut original = None;
    let scenario=capture(async {
        let observed=service.control.clone();
        let mut open_deadline=None;
        tokio::time::timeout(Duration::from_secs(3),async {
            tokio::select! {
                _=async {
                    let p=server.accept().await.unwrap().await.unwrap();auth(&p,&cfg).await;peer=Some(p);
                    loop {
                        apps.turn().await;
                        let session=observed.session.lock().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
                        if let Some(control)=session {
                            if control.flow_observation().unwrap().opening_providers==1 {
                                let contracts=control.flow_contracts();assert_eq!(contracts.len(),1);
                                open_deadline=Some(contracts[0].open_deadline);break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }=>{},
                report=service.run()=>panic!("opening must retain service {report:?}"),
            }
        }).await.unwrap();
        let deadline=open_deadline.unwrap();
        // F's existing request deadline is20s; the existing provider settlement
        // cap is5s. This observer derives its bound from that original request.
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline)+Duration::from_secs(5),async {
            tokio::select! {
                _=async {
                    loop {
                        apps.turn().await;
                        let session=observed.session.lock().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
                        if let Some(control)=session {
                            for contract in control.flow_contracts() {assert_eq!(contract.open_deadline,deadline);}
                            if let Some(failure)=control.retained_provider_failure() {original=Some(failure);break;}
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }=>{},
                report=service.run()=>panic!("flow rejection must not stop service {report:?}"),
            }
        }).await.unwrap();
        assert!(std::time::Instant::now()>=deadline,"natural original deadline precedes any manual stop");
        assert!(!handle.0.stop.stopping.load(Ordering::Acquire));
        let control=service.control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let state=control.flow_observation().unwrap();
        assert_eq!((state.factory_started,state.published,state.released),(1,0,1));
        assert_eq!((state.live,state.w,state.f,state.reserved,state.escrow_bytes),(0,0,0,0,0));
        assert!(state.audit);assert!(original.is_some());
    }).await;
    handle.request_stop();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
        let completion = service.completion.as_ref().unwrap();
        completion
            ._node
            .as_ref()
            .unwrap()
            .assert_service_clean_for_test();
        assert_eq!(
            completion.control.retained_provider_failure(),
            original,
            "same retained provider/error after actual node drain"
        );
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let server_cleanup = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
    assert!(server_cleanup.unwrap());
}

#[tokio::test(start_paused = true)]
async fn unpublished_device_shutdown_panic_keeps_original_q_without_constructing_node() {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let gate = Arc::new(Gate::default());
    gate.shutdown_panic.store(true, Ordering::SeqCst);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.tx_bytes = 65536;
    let mut service = prepare(
        device(gate.clone()),
        config(socket.local_addr().unwrap()),
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
    let control = service
        .control
        .session
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
    assert!(!control.driver_complete.load(Ordering::Acquire));
    let ledger = control.ledger();
    assert!(ledger.quarantined());
    assert_eq!(ledger.additional_failures, 1);
    drop(ledger);
    let polls = gate.polls.load(Ordering::SeqCst);
    assert_eq!(polls, 1);
    gate.shutdown_panic.store(false, Ordering::SeqCst);
    gate.open();
    assert!(
        tokio::time::timeout(Duration::from_millis(1), service.run())
            .await
            .is_err()
    );
    assert_eq!(
        gate.polls.load(Ordering::SeqCst),
        polls,
        "Q never retries unknown physical join"
    );
    assert_eq!(gate.joins.load(Ordering::SeqCst), 0);
    assert_eq!(gate.drops.load(Ordering::SeqCst), 0);
    assert!(service.completion.is_none());
    let mut b = [0; 2048];
    assert_eq!(
        socket.recv_from(&mut b).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    drop(service);
    assert_eq!(
        gate.drops.load(Ordering::SeqCst),
        1,
        "explicit forced shell destruction only"
    );
}

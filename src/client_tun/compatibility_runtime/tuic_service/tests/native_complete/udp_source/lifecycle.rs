use super::*;
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_udp_cold_refusal_precedes_attach_and_monitor() {
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let mut peers = Vec::new();
    let scenario = capture(async {
        let (started, ()) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::join!(host.start(), async {
                for _ in 0..cfg.tcp_pool {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    peers.push(peer);
                }
            })
        })
        .await
        .unwrap();
        assert_eq!(started, NodeStartup::Ready);
        let before = budget.used();
        let filler = meter.reserve_cold_layout(budget.limit() - before).unwrap();
        let error = match host.data() {
            Err(error) => error,
            Ok(_) => panic!("unfunded cold UDP must reject before DATA attach"),
        };
        assert!(
            matches!(error, ClientError::Io(ref io) if io.kind() == std::io::ErrorKind::OutOfMemory)
        );
        assert_eq!(host.udp_cold_observation_for_test(), (false, false, false));
        assert!(!budget.quarantined());
        drop((error, filler));
        assert_eq!(budget.used(), before, "all attempted cold stock rolls back");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::StartupResourcesSettled
        );
    })
    .await;
    for peer in peers {
        peer.close(0u32.into(), b"test complete");
    }
    server.close(0u32.into(), b"test complete");
    server.wait_idle().await;
    server.set_server_config(None);
    drop((host, server));
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_udp_cancelled_observation_keeps_original_task_and_deadline() {
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let mut peers = Vec::new();
    let mut data = None;
    let mut source = None;
    let scenario = capture(async {
        let (started, ()) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::join!(host.start(), async {
                for _ in 0..cfg.tcp_pool {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    peers.push(peer);
                }
            })
        })
        .await
        .unwrap();
        assert_eq!(started, NodeStartup::Ready);
        data = Some(host.data().unwrap());
        source = Some(data.as_ref().unwrap().start_udp_source().unwrap());
        let mut stream = peers[0].open_uni().await.unwrap();
        stream.write_all(b"partial").await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while data.as_ref().unwrap().udp_native_children_for_test() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(source.take());
        // The actual external DATA Arc keeps Node cleanup pending after the real
        // parent/uni stop. Cancel only observation, retain the original executor.
        assert!(
            tokio::time::timeout(Duration::from_millis(5), host.observe_shutdown())
                .await
                .is_err()
        );
        let deadline = host.shutdown_deadline_for_test().unwrap();
        drop(data.take());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        assert_eq!(host.shutdown_deadline_for_test(), Some(deadline));
        drop(stream);
    })
    .await;
    drop((source, data));
    for peer in peers {
        peer.close(0u32.into(), b"test complete");
    }
    server.close(0u32.into(), b"test complete");
    server.wait_idle().await;
    server.set_server_config(None);
    drop((host, server));
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_udp_first_quiet_and_session_deadline_survive_q_and_join() {
    use crate::shared::errors::{FailureClass, QuietFailureSink};
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let app_meter = bytes::BackingMeter::with_budget(&budget);
    let mut session = SessionControl::default();
    session.app_meter = Some(app_meter);
    let owner = Arc::new(session);
    let parent: Arc<dyn QuietFailureSink> = owner.clone();
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    host.bind_udp_parent(Arc::downgrade(&parent)).unwrap();
    let mut peers = Vec::new();
    let mut data = None;
    let mut original_clock = None;
    let scenario = capture(async {
        let (started, ()) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::join!(host.start(), async {
                for _ in 0..cfg.tcp_pool {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    peers.push(peer);
                }
            })
        })
        .await
        .unwrap();
        assert_eq!(started, NodeStartup::Ready);
        data = Some(host.data().unwrap());
        let clock = data.as_ref().unwrap().udp_native_clock_for_test();
        let early = tokio::time::Instant::now() + Duration::from_secs(3);
        owner.ledger().shutdown_deadline = Some(early);
        let first = clock.io(&std::io::Error::from_raw_os_error(5));
        assert_eq!(first, 0);
        assert!(clock.reported(first));
        let second = clock.panic();
        assert_eq!(second, 1);
        assert!(clock.reported(second));
        let (first, additional) = host.udp_first_for_test();
        assert_eq!(first.unwrap().raw_os_code, Some(5));
        assert_eq!(additional, 1);
        assert_eq!(
            owner
                .ledger()
                .first_failure
                .as_ref()
                .unwrap()
                .facts()
                .raw_os_code,
            Some(5)
        );
        assert_eq!(owner.ledger().additional_failures, 1);
        assert_eq!(
            owner.ledger().first_failure.as_ref().unwrap().facts().class,
            FailureClass::Io
        );
        host.request_stop();
        assert_eq!(host.shutdown_deadline_for_test(), Some(early));
        assert_eq!(owner.ledger().shutdown_deadline, Some(early));
        drop(data.take());
        original_clock = Some(clock);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::Quarantined
        );
        assert_eq!(host.shutdown_deadline_for_test(), Some(early));
        assert_eq!(
            owner.ledger().additional_failures,
            1,
            "JOIN must not redeliver original clock ordinals"
        );
        assert!(budget.quarantined());
        assert!(budget.used() > 0);
    })
    .await;
    drop((data, original_clock));
    for peer in peers {
        peer.close(0u32.into(), b"test complete");
    }
    server.close(0u32.into(), b"test complete");
    server.wait_idle().await;
    server.set_server_config(None);
    drop((host, server));
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_lost_unique_udp_owner_quarantines_still_live_source() {
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let mut host = Some(NodeHost::prepare_with_budget(
        cfg.clone(),
        Arc::new(Metrics::default()),
        Some(&budget),
    ));
    let mut peers = Vec::new();
    let mut data = None;
    let mut source = None;
    let mut observed = None;
    let mut original_observer = None;
    let scenario = capture(async {
        let (started, ()) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::join!(host.as_mut().unwrap().start(), async {
                for _ in 0..cfg.tcp_pool {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    peers.push(peer);
                }
            })
        })
        .await
        .unwrap();
        assert_eq!(started, NodeStartup::Ready);
        data = Some(host.as_mut().unwrap().data().unwrap());
        source = Some(data.as_ref().unwrap().start_udp_source().unwrap());
        let control = source.as_ref().unwrap().generation_for_test().unwrap();
        assert!(budget.used() > 0 && !budget.quarantined());
        original_observer = Some(host.as_ref().unwrap().native_observer_for_test());
        drop(host.take()); // original actor/observer Arc is still alive
        observed = Some((
            budget.quarantined(),
            control.frozen(1),
            control.sealed(1),
            control.valid(),
        ));
        assert!(data.is_some() && source.is_some());
        assert!(original_observer.is_some());
    })
    .await;
    // Unknown authority loss cannot be turned into a replacement JOIN observer.
    drop((source, data, host, original_observer));
    for peer in peers {
        peer.close(0u32.into(), b"test complete");
    }
    server.close(0u32.into(), b"test complete");
    server.wait_idle().await;
    server.set_server_config(None);
    drop(server);
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    assert_eq!(
        observed,
        Some((true, true, false, false)),
        "Q/freeze must precede last runtime/source Arc Drop"
    );
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}

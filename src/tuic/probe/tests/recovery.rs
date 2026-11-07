use super::super::*;
use super::lifecycle::{auth, close, config, server};
use crate::tuic::encode_connect;

async fn drive<T>(owner: &mut Probe, event: impl Future<Output = T>) -> T {
    tokio::select! {value=event=>value,result=owner.run()=>panic!("premature completion: {result:?}")}
}
async fn connected(
    owner: &mut Probe,
    server: &quinn::Endpoint,
    config: &TuicClientConfig,
) -> (quinn::Connection, quinn::SendStream, quinn::RecvStream) {
    drive(owner, async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, config).await;
        let (send, mut recv) = peer.accept_bi().await.unwrap();
        let target = TargetAddr::parse("192.0.2.1:443").unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        (peer, send, recv)
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn actual_closed_primary_recovers_new_auth_connect_with_udp_never_started() {
    let server = server();
    let config = config(&server);
    let mut owner = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        1,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    let facts = owner.control.facts.clone();
    facts.hold.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(10), async {
        let old = drive(&mut owner, async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &config).await;
            facts.entered.notified().await;
            peer
        })
        .await;
        let data = facts.data.lock().unwrap().upgrade().unwrap();
        let original = crate::tuic::tcp_fixture::upstream_ready_connection(&data).unwrap();
        old.close(73u32.into(), b"real primary closed before new CONNECT");
        drive(&mut owner, original.closed()).await;
        drop(original); // Must not retain the original physical client connection.
        assert_eq!(data.udp_runtime.generation.epoch(), 1);
        facts.hold.store(false, Ordering::Release);
        facts.release.notify_one();
        let (new, mut send, mut recv) = connected(&mut owner, &server, &config).await;
        assert_eq!(data.udp_runtime.generation.epoch(), 2);
        let (disposition, epoch, _, valid, rejections) = data.udp_runtime.generation.diagnostic();
        assert_eq!((disposition, epoch, valid, rejections), (0, 2, true, 0));
        assert_eq!(data.udp_runtime.child_count.load(Ordering::Acquire), 0);
        assert!(Arc::ptr_eq(
            &data,
            &facts.data.lock().unwrap().upgrade().unwrap()
        ));
        drop(data);
        send.write_all(b"closed-recovered").await.unwrap();
        send.finish().unwrap();
        let peer = async {
            let _ = recv.read_to_end(1024).await;
            drop(recv);
            drop(send);
            new.closed().await;
        };
        let (observed, ()) = tokio::join!(owner.run(), peer);
        assert_eq!(observed.phase, Phase::Complete);
        assert_eq!(observed.sink.unwrap().bytes, 16);
        assert_eq!(observed.node, Some(NodeShutdown::RunningResourcesSettled));
        owner
            .completion
            .as_ref()
            .unwrap()
            ._node
            .assert_service_clean_for_test();
        assert_eq!(owner.run().await.phase, Phase::Complete);
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test(flavor = "current_thread")]
async fn busy_pre_admission_slot_resumes_same_attempt_without_identityless_lease() {
    let server = server();
    let config = config(&server);
    let mut owner = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        1,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    let facts = owner.control.facts.clone();
    facts.hold.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(8), async {
        let peer = drive(&mut owner, async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &config).await;
            facts.entered.notified().await;
            peer
        })
        .await;
        let data = facts.data.lock().unwrap().upgrade().unwrap();
        let slot = data.tcp_pool_slots[0].state.lock().await;
        facts.hold.store(false, Ordering::Release);
        facts.release.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(35), owner.run())
                .await
                .is_err()
        );
        assert!(Arc::ptr_eq(
            &data,
            &facts.data.lock().unwrap().upgrade().unwrap()
        ));
        assert!(peer.close_reason().is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), server.accept())
                .await
                .is_err()
        );
        let original = facts.opening.lock().unwrap().unwrap();
        assert_eq!(
            data.tcp_pool_admission.active_total(),
            0,
            "unreadable physical identity must not commit a lease"
        );
        assert!(!data.tcp_pool_admission.preparing_slots[0].load(Ordering::Acquire));
        assert!(!original.2.lease_held && !original.2.retirement_pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(35), owner.run())
                .await
                .is_err()
        );
        assert_eq!(facts.opening.lock().unwrap().unwrap(), original);
        assert!(!owner.control.stopped.load(Ordering::Acquire));
        assert!(owner.control.deadline.lock().unwrap().is_none());
        drop(slot);
        drop(data);
        let (mut send, mut recv) = drive(&mut owner, peer.accept_bi()).await.unwrap();
        let header = encode_connect(&TargetAddr::parse("192.0.2.1:443").unwrap());
        let mut actual = vec![0; header.len()];
        recv.read_exact(&mut actual).await.unwrap();
        assert_eq!(actual, header);
        send.write_all(b"identity-resumed").await.unwrap();
        send.finish().unwrap();
        let (observed, ()) = tokio::join!(owner.run(), async {
            let _ = recv.read_to_end(1024).await;
            drop(recv);
            drop(send);
            peer.closed().await;
        });
        assert_eq!(observed.phase, Phase::Complete);
        assert_eq!(observed.sink.unwrap().bytes, 16);
        assert_eq!(observed.node, Some(NodeShutdown::RunningResourcesSettled));
        owner
            .completion
            .as_ref()
            .unwrap()
            ._node
            .assert_service_clean_for_test();
        assert!(owner.failure().is_none());
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_stop_and_cancelled_shutdown_observation_join_actual_open_before_node() {
    let server = server();
    let config = config(&server);
    let mut owner = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        30,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    let facts = owner.control.facts.clone();
    let handle = owner.take_handle().unwrap();
    assert!(owner.take_handle().is_none());
    tokio::time::timeout(Duration::from_secs(8), async {
        let (peer, send, recv) = connected(&mut owner, &server, &config).await;
        let data = facts.data.lock().unwrap().upgrade().unwrap();
        assert_eq!(data.tcp_pool_admission.active_total(), 1);
        handle.request_stop();
        let deadline = *owner.control.deadline.lock().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(2), handle.observe_shutdown())
                .await
                .is_err()
        );
        assert_eq!(data.tcp_pool_admission.active_total(), 1);
        drop(data);
        drop(send);
        drop(recv);
        let (observed, ()) = tokio::join!(owner.run(), async {
            peer.closed().await;
        });
        assert_eq!(observed.phase, Phase::Stopped);
        assert_eq!(observed.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!(handle.observe_shutdown().await, Phase::Stopped);
        assert_eq!(*owner.control.deadline.lock().unwrap(), deadline);
        owner
            .completion
            .as_ref()
            .unwrap()
            ._node
            .assert_service_clean_for_test();
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_original_stats_task_failure_stops_business_and_retains_original_node_quarantine() {
    let server = server();
    let config = config(&server);
    let mut owner = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        30,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    let facts = owner.control.facts.clone();
    tokio::time::timeout(Duration::from_secs(8), async {
        let (peer, send, recv) = connected(&mut owner, &server, &config).await;
        facts.stats_abort.lock().unwrap().as_ref().unwrap().abort();
        drop(send);
        drop(recv);
        let (observed, ()) = tokio::join!(owner.run(), async {
            peer.closed().await;
        });
        assert_eq!(observed.phase, Phase::Quarantined);
        assert_eq!(observed.node, Some(NodeShutdown::Quarantined));
        owner
            .completion
            .as_ref()
            .unwrap()
            ._node
            .assert_service_fault_settled_for_test();
        assert_eq!(owner.run().await.phase, Phase::Quarantined);
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test(flavor = "current_thread")]
async fn cold_rejections_and_stop_before_start_create_no_network_work() {
    let server = server();
    let config = config(&server);
    assert!(
        prepare_sink(
            config.clone(),
            "192.0.2.1:443",
            0,
            Arc::new(Metrics::default())
        )
        .is_err()
    );
    assert!(
        prepare_sink(
            config.clone(),
            "192.0.2.1:443",
            601,
            Arc::new(Metrics::default())
        )
        .is_err()
    );
    assert!(
        prepare_sink(
            config.clone(),
            &format!("{}:443", "a".repeat(256)),
            1,
            Arc::new(Metrics::default())
        )
        .is_err()
    );
    let mut owner = prepare_sink(config, "192.0.2.1:443", 1, Arc::new(Metrics::default())).unwrap();
    let handle = owner.take_handle().unwrap();
    handle.request_stop();
    let observed = owner.run().await;
    assert_eq!(observed.phase, Phase::Stopped);
    assert_eq!(observed.node, Some(NodeShutdown::StartupResourcesSettled));
    owner
        .completion
        .as_ref()
        .unwrap()
        ._node
        .assert_service_clean_for_test();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), server.accept())
            .await
            .is_err()
    );
    close(server).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_quic_stream_credit_pressure_and_cancelled_run_resume_same_original_open() {
    let server = quinn::Endpoint::server(
        crate::tuic::tcp_fixture::UdpPair::server_config(8, 65536, 0, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let config = config(&server);
    let mut owner = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        1,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    let facts = owner.control.facts.clone();
    tokio::time::timeout(Duration::from_secs(8), async {
        let peer = drive(&mut owner, async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &config).await;
            peer
        })
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(35), owner.run())
                .await
                .is_err()
        );
        let original = facts.opening.lock().unwrap().unwrap();
        assert_eq!(original.2.phase, crate::upstream::TcpOpenPhase::Opening);
        assert!(original.2.lease_held && !original.2.retirement_pending);
        let data = facts.data.lock().unwrap().upgrade().unwrap();
        assert_eq!(data.tcp_pool_admission.active_total(), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(35), owner.run())
                .await
                .is_err()
        );
        assert_eq!(facts.opening.lock().unwrap().unwrap(), original);
        assert!(!owner.control.stopped.load(Ordering::Acquire));
        assert!(owner.control.deadline.lock().unwrap().is_none());
        assert!(Arc::ptr_eq(
            &data,
            &facts.data.lock().unwrap().upgrade().unwrap()
        ));
        assert!(peer.close_reason().is_none());
        drop(data);
        peer.set_max_concurrent_bi_streams(1u32.into());
        let (mut send, mut recv) = drive(&mut owner, peer.accept_bi()).await.unwrap();
        let target = TargetAddr::parse("192.0.2.1:443").unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        send.write_all(b"credit-resumed").await.unwrap();
        send.finish().unwrap();
        let peer_done = async {
            let _ = recv.read_to_end(1024).await;
            drop(send);
            drop(recv);
            peer.closed().await;
        };
        let (observed, ()) = tokio::join!(owner.run(), peer_done);
        assert_eq!(observed.phase, Phase::Complete);
        assert_eq!(observed.sink.unwrap().bytes, 14);
        assert_eq!(observed.node, Some(NodeShutdown::RunningResourcesSettled));
        owner
            .completion
            .as_ref()
            .unwrap()
            ._node
            .assert_service_clean_for_test();
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test(flavor = "current_thread")]
async fn stop_during_physical_slot_contention_settles_original_open_then_actual_node() {
    let server = server();
    let config = config(&server);
    let mut owner = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        1,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    let facts = owner.control.facts.clone();
    facts.hold.store(true, Ordering::Release);
    let handle = owner.take_handle().unwrap();
    tokio::time::timeout(Duration::from_secs(8), async {
        let peer = drive(&mut owner, async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &config).await;
            facts.entered.notified().await;
            peer
        })
        .await;
        let data = facts.data.lock().unwrap().upgrade().unwrap();
        let slot = data.tcp_pool_slots[0].state.lock().await;
        facts.hold.store(false, Ordering::Release);
        facts.release.notify_one();
        assert!(
            tokio::time::timeout(Duration::from_millis(35), owner.run())
                .await
                .is_err()
        );
        let original = facts.opening.lock().unwrap().unwrap();
        assert_eq!(original.2.phase, crate::upstream::TcpOpenPhase::Acquiring);
        assert!(!original.2.lease_held && !original.2.retirement_pending);
        assert_eq!(data.tcp_pool_admission.active_total(), 0);
        assert!(!data.tcp_pool_admission.preparing_slots[0].load(Ordering::Acquire));
        handle.request_stop();
        let first = *owner.control.deadline.lock().unwrap();
        assert!(first.is_some());
        assert!(
            tokio::time::timeout(Duration::from_millis(2), handle.observe_shutdown())
                .await
                .is_err()
        );
        assert_eq!(*owner.control.deadline.lock().unwrap(), first);
        assert_eq!(facts.opening.lock().unwrap().unwrap(), original);
        drop(slot);
        drop(data);
        let (observed, ()) = tokio::join!(owner.run(), async {
            peer.closed().await;
        });
        assert_eq!(observed.phase, Phase::Stopped);
        assert_eq!(observed.node, Some(NodeShutdown::RunningResourcesSettled));
        assert!(matches!(owner.failure(), Some(ClientError::Io(error))
            if error.kind() == std::io::ErrorKind::Interrupted
                && error.to_string() == "TUIC open Shutdown"));
        let original_error = owner.failure().unwrap() as *const ClientError;
        assert_eq!(owner.run().await, observed);
        assert_eq!(handle.observe_shutdown().await, Phase::Stopped);
        assert_eq!(
            owner.failure().unwrap() as *const ClientError,
            original_error
        );
        assert_eq!(*owner.control.deadline.lock().unwrap(), first);
        owner
            .completion
            .as_ref()
            .unwrap()
            ._node
            .assert_service_clean_for_test();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.accept())
                .await
                .is_err()
        );
    })
    .await
    .unwrap();
    close(server).await;
}

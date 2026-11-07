use super::super::*;
use super::running::boot;
use super::startup::{config, finish, guarded, server};
use crate::failover::HealthProbe;

#[tokio::test(flavor = "current_thread")]
async fn original_primary_health_preserves_auth_rx_busy_and_actual_close() {
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await; // verifies actual AUTH
        let data = host.data().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), data.probe())
                .await
                .unwrap()
        );
        assert!(!data.is_dead().await);
        assert!(data.rx_datagrams().await.is_some_and(|rx| rx > 0));
        let original = data.tcp_pool_slots[0].state.lock().await;
        let id = original.current.identity();
        assert_eq!(data.rx_datagrams().await, None, "busy is not zero RX");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), data.is_dead())
                .await
                .is_err(),
            "old dead predicate waits for the original slot"
        );
        drop(original);
        assert_eq!(
            data.tcp_pool_slots[0].state.lock().await.current.identity(),
            id
        );
        assert!(!data.is_dead().await);
        assert_eq!(host.resources.state().attempts_started, 1);
        assert_eq!(peers.len(), 1);
        peers[0].close(19u32.into(), b"health characterization actual close");
        tokio::time::timeout(Duration::from_secs(2), async {
            while !data.is_dead().await {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert!(data.rx_datagrams().await.is_some_and(|rx| rx > 0));
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        assert!(host.resources.state().endpoint_drained);
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_stats_task_join_failure_denies_fallback_but_preserves_primary_rx() {
    use super::failover_staged::{phase, reality, request};
    use crate::failover::{FailoverUpstream, FallbackAdmission, TcpLeg};
    use crate::upstream::TcpOpenPhase;
    let server = server(8);
    let cfg = config(server.local_addr().unwrap());
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let data = host.data().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let factory = Arc::new(FailoverUpstream::new(
            data.clone(),
            reality(listener.local_addr().unwrap().to_string(), [0; 32]),
        ));
        let mut open = factory.clone().staged_tcp_open(request()).unwrap();
        host.cancel_original_stats_for_service_test(); // actual JoinHandle cancellation
        assert!(
            tokio::time::timeout(Duration::from_secs(3), host.run())
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(
            data.snapshot().unwrap().fallback,
            FallbackAdmission::Quarantined
        );
        assert!(
            data.rx_datagrams().await.is_some(),
            "gate denial is not missing transport facts"
        );
        phase(open.as_mut(), TcpOpenPhase::Settled).await;
        assert_eq!(factory.state().active_leg(), TcpLeg::Tuic);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
        drop(factory);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::Quarantined
        );
        host.assert_service_fault_settled_for_test();
        assert_eq!(open.view().phase, TcpOpenPhase::Settled);
        // Actual injected task cancellation: physical completion plus sticky Q,
        // not a natural OS failure or clean service acceptance.
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_unowned_and_defensive_unpublished_lost_owner_keep_same_rx_facts() {
    use crate::failover::FallbackAdmission;
    use crate::tuic::tcp_fixture::UdpPair;
    let pair = UdpPair::new(8, 65536).await;
    let data = pair.upstream(UdpRelayMode::Native).await;
    let rx = data.rx_datagrams().await.unwrap();
    assert_eq!(
        data.snapshot().unwrap().fallback,
        FallbackAdmission::Unowned
    );
    assert_eq!(data.snapshot().unwrap().rx_datagrams, rx);
    // A valid published Node cannot expose an unpublished/lost adapter. Bind this
    // real legacy peer to a cold owner's Weak solely to cover defensive refusal.
    // Cold owner constructs no endpoint/task; this is not production composition.
    let cold = NodeHost::prepare(
        config(pair.server.local_addr().unwrap()),
        Arc::new(Metrics::default()),
    );
    assert!(!cold.resources.state().endpoint_created);
    data.udp_runtime
        .owner
        .set(Arc::downgrade(&cold.resources))
        .unwrap();
    assert_eq!(
        data.snapshot().unwrap().fallback,
        FallbackAdmission::Unpublished
    );
    assert!(data.rx_datagrams().await.is_some());
    drop(cold);
    assert_eq!(
        data.snapshot().unwrap().fallback,
        FallbackAdmission::OwnerLost
    );
    assert!(data.rx_datagrams().await.is_some());
    drop(data);
    pair.close().await;
}

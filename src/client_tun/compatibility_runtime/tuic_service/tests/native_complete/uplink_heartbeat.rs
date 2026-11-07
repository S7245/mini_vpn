//! Original Node probe and real Heartbeat peer, including local backing pressure.
use super::*;
use crate::tuic::{NodeHost, NodeShutdown, NodeStartup};
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_heartbeat_probe_funds_original_datagram() {
    probe(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_heartbeat_full_keeps_original_probe_failure_without_q() {
    probe(true).await;
}
async fn probe(full: bool) {
    let identity = PublicCa::new();
    let server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let mut peers = Vec::new();
    let mut data = None;
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
        if full {
            let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
            let pressure = bytes::BytesMut::try_zeroed_metered(
                budget.limit() - budget.used() - header,
                &meter,
            )
            .unwrap();
            assert_eq!(
                data.as_ref().unwrap().udp_native_heartbeat_for_test().await,
                0
            );
            assert!(!data.as_ref().unwrap().native_probe_primary_for_test().await);
            assert_eq!(budget.used(), budget.limit());
            assert!(
                !budget.quarantined(),
                "known local probe pressure must retain its original SendFailed classification"
            );
            drop(pressure);
        }
        let (alive, frame) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                data.as_ref().unwrap().native_probe_primary_for_test(),
                peers[0].read_datagram()
            )
        })
        .await
        .unwrap();
        assert_eq!(frame.unwrap().as_ref(), [5, 4]);
        assert!(alive);
        assert!(!budget.quarantined());
    })
    .await;
    drop(data.take());
    let cleanup = capture(async {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
        );
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    })
    .await;
    for peer in peers {
        peer.close(0u32.into(), b"probe complete");
    }
    server.close(0u32.into(), b"probe complete");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle())
        .await
        .unwrap();
    scenario.unwrap_or_else(|p| std::panic::resume_unwind(p));
    cleanup.unwrap_or_else(|p| std::panic::resume_unwind(p));
}

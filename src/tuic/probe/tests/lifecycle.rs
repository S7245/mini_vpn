use super::super::*;
use crate::metrics::Metrics;
use crate::shared::TargetAddr;
use crate::tuic::tcp_fixture::UdpPair;
use crate::tuic::{TuicClientConfig, encode_authenticate, encode_connect};
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
use std::sync::Arc;
use std::time::Duration;

pub(super) fn server() -> quinn::Endpoint {
    quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap()
}
pub(super) fn config(server: &quinn::Endpoint) -> TuicClientConfig {
    let mut config = TuicClientConfig::from_sources(
        Some(&server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap();
    config.tcp_pool = 1;
    config.quic_stats_secs = Some(1);
    config
}
pub(super) async fn auth(peer: &quinn::Connection, config: &TuicClientConfig) {
    let mut recv = peer.accept_uni().await.unwrap();
    let observed = recv.read_to_end(64).await.unwrap();
    let mut token = [0; 32];
    peer.export_keying_material(&mut token, &config.uuid, config.password.as_bytes())
        .unwrap();
    assert_eq!(observed, encode_authenticate(&config.uuid, &token));
}
pub(super) async fn close(mut endpoint: quinn::Endpoint) {
    let drain = endpoint.take_driver_drain_handle().unwrap();
    endpoint.close(0u32.into(), b"test complete");
    drop(endpoint);
    assert!(
        tokio::time::timeout(Duration::from_secs(3), drain.drained())
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn actual_sink_same_production_helper_joins_before_node_cleanup_with_native_projection() {
    let server = server();
    let config = config(&server).with_relay_policy(Arc::new(RelayFamilyPolicy::from_sources(
        RelayFamilyPolicySources {
            d16_byte_owned: Some("1"),
            ..Default::default()
        },
    )));
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let mut probe = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        1,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    let business = async {
        let report = probe.run().await;
        assert_eq!(report.phase, Phase::Complete);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        let sink = report.sink.unwrap();
        assert_eq!(sink.bytes, 19);
        assert!(sink.eof);
        assert!(probe.failure().is_none());
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        send.write_all(b"original-statistics").await.unwrap();
        send.finish().unwrap();
        let _ = recv.read_to_end(1024).await;
        drop(send);
        drop(recv);
        peer.closed().await;
    };
    tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(business, peer);
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test(flavor = "current_thread")]
async fn known_original_node_cleanup_failure_remains_failed_settled_not_success_or_quarantine() {
    let server = server();
    let config = config(&server).with_relay_policy(Arc::new(RelayFamilyPolicy::from_sources(
        RelayFamilyPolicySources {
            d16_byte_owned: Some("1"),
            ..Default::default()
        },
    )));
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let mut probe = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        1,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    probe
        .control
        .facts
        .known_cleanup_failure
        .store(true, std::sync::atomic::Ordering::Release);
    let business = async {
        let report = probe.run().await;
        assert_eq!(report.phase, Phase::Failed);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::FailedSettled));
        let sink = report.sink.unwrap();
        assert_eq!(sink.bytes, 19);
        assert!(sink.eof);
        assert!(probe.failure().is_none());
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        send.write_all(b"original-statistics").await.unwrap();
        send.finish().unwrap();
        let _ = recv.read_to_end(1024).await;
        drop(send);
        drop(recv);
        peer.closed().await;
    };
    tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(business, peer);
    })
    .await
    .unwrap();
    close(server).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_sink_default_no_stats_profile_keeps_same_production_semantics() {
    let server = server();
    let mut config = config(&server).with_relay_policy(Arc::new(RelayFamilyPolicy::from_sources(
        RelayFamilyPolicySources {
            d16_byte_owned: Some("1"),
            ..Default::default()
        },
    )));
    config.quic_stats_secs = None;
    let target = TargetAddr::parse("192.0.2.1:443").unwrap();
    let mut probe = prepare_sink(
        config.clone(),
        "192.0.2.1:443",
        1,
        Arc::new(Metrics::default()),
    )
    .unwrap();
    let business = async {
        let report = probe.run().await;
        assert_eq!(report.phase, Phase::Complete);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        let sink = report.sink.unwrap();
        assert_eq!(sink.bytes, 19);
        assert!(sink.eof);
        assert!(probe.failure().is_none());
    };
    let peer = async {
        let peer = server.accept().await.unwrap().await.unwrap();
        auth(&peer, &config).await;
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        let mut header = vec![0; encode_connect(&target).len()];
        recv.read_exact(&mut header).await.unwrap();
        assert_eq!(header, encode_connect(&target));
        send.write_all(b"original-statistics").await.unwrap();
        send.finish().unwrap();
        let _ = recv.read_to_end(1024).await;
        drop(send);
        drop(recv);
        peer.closed().await;
    };
    tokio::time::timeout(Duration::from_secs(8), async {
        tokio::join!(business, peer);
    })
    .await
    .unwrap();
    close(server).await;
}

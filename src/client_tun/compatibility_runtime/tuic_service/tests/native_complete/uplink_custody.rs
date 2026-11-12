//! Original Node probe and real Heartbeat peer, including local backing pressure.
use super::*;
use crate::tuic::{NodeHost, NodeShutdown, NodeStartup};
use std::future::Future;
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_ready_before_data_stop_preserves_session_deadline() {
    use crate::shared::errors::QuietFailureSink;
    let identity = PublicCa::new();
    let server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let control = control_with_budget(false, Some(budget.clone()));
    let runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    let session = business::control(&runtime, control.app_meter.clone());
    session
        .parent_deadline
        .set(control.deadline.clone())
        .unwrap();
    let sink: Arc<dyn QuietFailureSink> = session.clone();
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    host.bind_udp_parent(Arc::downgrade(&sink)).unwrap();
    let observer = host.native_observer_for_test();
    let mut peers = Vec::new();
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
    let original = tokio::time::Instant::now() + Duration::from_secs(3);
    control.expire_at(original);
    host.request_stop();
    let deadlines = (
        host.shutdown_deadline_for_test(),
        observer.uplink_deadline_for_test(),
    );
    let result = tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
        .await
        .unwrap();
    for peer in peers {
        peer.close(0u32.into(), b"normal early stop");
    }
    server.close(0u32.into(), b"normal early stop");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle())
        .await
        .unwrap();
    assert_eq!(result, NodeShutdown::StartupResourcesSettled);
    assert_eq!(observer.facts(), (None, 0, false));
    assert!(!session.ledger().quarantined() && !budget.quarantined());
    assert_eq!(budget.used(), 0);
    assert_eq!(deadlines, (Some(original), Some(original)));
}

#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_uplink_cold_table_refunds_only_actual_layout_drop() {
    node(false, true, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_uplink_original_stream_single_poll_fin_and_custody_pressure() {
    node(true, true, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_legacy_node_uplink_keeps_single_poll_fin_and_custody_pressure() {
    node(true, false, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_uplink_partial_write_resets_and_keeps_bounded_custody() {
    node(true, true, true).await;
}
async fn node(stream: bool, hard: bool, partial: bool) {
    let identity = PublicCa::new();
    let mut server_config = identity.server_config();
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(1024u32.into());
    transport.stream_receive_window(if partial { 16u32 } else { 65536u32 }.into());
    server_config.transport_config(Arc::new(transport));
    let server = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    if stream {
        cfg.udp_relay_mode = "quic".into();
    }
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let mut host = NodeHost::prepare_with_budget(
        cfg.clone(),
        Arc::new(Metrics::default()),
        hard.then_some(&budget),
    );
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
        if stream {
            let packet = crate::tuic::encode_packet(
                7,
                &crate::shared::TargetAddr::IpPort("127.0.0.1:9000".parse().unwrap()),
                &[0x5a; 128],
            );
            let data = data.as_ref().unwrap();
            let mut send = std::pin::pin!(data.send_udp(packet.clone()));
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(send.as_mut().poll(cx)))
                    .await
                    .is_ready()
            );
            let actual = tokio::time::timeout(Duration::from_secs(2), async {
                peers[0]
                    .accept_uni()
                    .await
                    .unwrap()
                    .read_to_end(65535)
                    .await
            })
            .await
            .unwrap();
            if partial {
                assert!(matches!(
                    actual,
                    Err(quinn::ReadToEndError::Read(quinn::ReadError::Reset(_)))
                ));
                assert_eq!(data.udp_drop_count(), 1);
            } else {
                assert_eq!(actual.unwrap(), packet);
                assert_eq!(data.udp_drop_count(), 0);
            }
            // No yielding: FIN tails occupy all remaining slots until actual ACK retirement.
            for _ in 0..257 {
                data.send_udp(packet.clone()).await;
            }
            assert!(data.udp_drop_count() > 0);
            assert_eq!(data.uplink_occupied_for_test(), 256);
            if hard {
                let before = budget.used();
                let mut retirement = std::pin::pin!(host.retire_live_uplink_for_test());
                assert!(
                    std::future::poll_fn(|cx| std::task::Poll::Ready(retirement.as_mut().poll(cx)))
                        .await
                        .is_pending()
                );
                assert_eq!(budget.used(), before);
                assert!(
                    !budget.quarantined(),
                    "real FIN witnesses pending on ACK are not bad inventory"
                );
                tokio::time::timeout(Duration::from_secs(2), retirement)
                    .await
                    .unwrap();
                assert!(!budget.quarantined());
            }
        } else {
            let before = budget.used();
            let layout = data.as_ref().unwrap().retire_unused_uplink_for_test();
            let retired = before - budget.used();
            assert_eq!(
                retired, layout,
                "actual original 256-slot allocation must stay funded until its physical Drop"
            );
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

#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_ready_before_data_owner_loss_quiets_without_fake_io() {
    let identity = PublicCa::new();
    let server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let observer = host.native_observer_for_test();
    let mut peers = Vec::new();
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
    assert_eq!(host.udp_cold_observation_for_test(), (false, false, false));
    assert!(!budget.quarantined());
    drop(host); // Actual ready value, never data(): original runtime clock is still unbound.
    let (first, additional, quarantined) = observer.facts();
    assert!(quarantined && budget.quarantined());
    assert!(first.is_none());
    assert_eq!(additional, 0);
    drop(observer);
    for peer in peers {
        peer.close(0u32.into(), b"owner lost");
    }
    server.close(0u32.into(), b"owner lost");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle())
        .await
        .unwrap();
    assert!(budget.used() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_uplink_cold_full_after_auth_rolls_back_real_endpoint() {
    let identity = PublicCa::new();
    let server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let observer = host.native_observer_for_test();
    let pressure = crate::tuic::TuicUpstream::hold_uplink_cold_headroom_for_test(&budget);
    let (started, peer) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
        tokio::join!(host.start(), async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &cfg).await;
            peer
        })
    })
    .await
    .unwrap();
    let result = capture(async {
        assert_eq!(started, NodeStartup::Failed);
        let (before, after, layout) = pressure.observed();
        assert!(before > 0 && after > before);
        assert_eq!(budget.limit() - after, layout - 1);
        let (first, additional, q) = observer.facts();
        assert_eq!(first.unwrap().io_kind, Some(std::io::ErrorKind::WouldBlock));
        assert_eq!(additional, 0);
        assert!(!q && !budget.quarantined());
        assert_eq!(host.udp_cold_observation_for_test(), (false, false, false));
    })
    .await;
    drop(pressure); // Actual unrelated App backing, not a logical budget adjustment.
    let cleanup = tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
        .await
        .unwrap();
    peer.close(0u32.into(), b"cold Full complete");
    server.close(0u32.into(), b"cold Full complete");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle())
        .await
        .unwrap();
    result.unwrap_or_else(|p| std::panic::resume_unwind(p));
    assert_eq!(cleanup, NodeShutdown::FailedSettled);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_uplink_interrupted_write_keeps_original_packet_stream_and_box() {
    let identity = PublicCa::new();
    let server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    cfg.udp_relay_mode = "quic".into();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let observer = host.native_observer_for_test();
    let (started, peer) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
        tokio::join!(host.start(), async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &cfg).await;
            peer
        })
    })
    .await
    .unwrap();
    assert_eq!(started, NodeStartup::Ready);
    let data = host.data().unwrap();
    let original = Arc::new(());
    let packet = crate::tuic::encode_packet(
        7,
        &crate::shared::TargetAddr::IpPort("127.0.0.1:9000".parse().unwrap()),
        &[42; 128],
    );
    let length = packet.len();
    let before = data.uplink_payload_bytes_for_test();
    data.panic_next_native_uplink_poll_for_test(original.clone());
    data.send_udp(packet).await;
    let retained = data.uplink_payload_bytes_for_test();
    let same = data.uplink_panic_and_stream_for_test(&original);
    let facts = observer.facts();
    drop((data, host, observer));
    peer.close(0u32.into(), b"actual stream interrupted");
    server.close(0u32.into(), b"actual stream interrupted");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle())
        .await
        .unwrap();
    assert!(same && facts.2 && budget.quarantined());
    assert_eq!(
        facts.0.unwrap().class,
        crate::shared::errors::FailureClass::Panic
    );
    assert_eq!(facts.1, 0);
    assert_eq!(Arc::strong_count(&original), 2);
    assert_eq!(
        retained,
        before + length,
        "unknown original write must keep the actual funded Packet, not only its stream and panic marker"
    );
    assert!(budget.used() > 0);
}

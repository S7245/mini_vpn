//! The original hard-budget Node, real QUIC datagram, generation Source and
//! delivered payload. No substitute channel or synthetic successful port.
use super::*;
use crate::tuic::{NodeHost, NodeShutdown, NodeStartup};

#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_udp_source_preserves_native_payload_budget() {
    delivery(Delivery::Datagram).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_udp_uni_preserves_native_payload_budget() {
    delivery(Delivery::Uni).await;
}
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_udp_fragments_preserve_native_payload_budget() {
    delivery(Delivery::Fragments).await;
}
enum Delivery {
    Datagram,
    Uni,
    Fragments,
}
async fn delivery(mode: Delivery) {
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 * 1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let mut peers = Vec::new();
    let mut source = None;
    let mut data = None;
    let mut observed = None;
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
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let frame = crate::tuic::encode_packet(7, &target, b"original-native-udp");
        match mode {
            Delivery::Datagram => peers[0].send_datagram(frame.into()).unwrap(),
            Delivery::Uni => {
                let mut stream = peers[0].open_uni().await.unwrap();
                stream.write_all(&frame).await.unwrap();
                stream.finish().unwrap();
            }
            Delivery::Fragments => {
                for (id, bytes) in [b"original-".as_slice(), b"native-udp".as_slice()]
                    .into_iter()
                    .enumerate()
                {
                    let mut fragment = vec![5, 2, 0, 7, 0, 1, 2, id as u8];
                    fragment.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
                    fragment.push(0xff);
                    fragment.extend_from_slice(bytes);
                    peers[0].send_datagram(fragment.into()).unwrap();
                }
                assert_eq!(
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        source.as_mut().unwrap().recv(&std::time::Instant::now())
                    )
                    .await
                    .unwrap(),
                    Some(UdpDownlinkEvent::Ignored)
                );
            }
        }
        let event = tokio::time::timeout(
            Duration::from_secs(2),
            source.as_mut().unwrap().recv(&std::time::Instant::now()),
        )
        .await
        .unwrap()
        .unwrap();
        let UdpDownlinkEvent::Deliver(delivered) = event else {
            panic!("one real unfragmented TUIC packet must deliver");
        };
        assert_eq!(delivered.payload().as_ref(), b"original-native-udp");
        observed = Some(
            delivered
                .payload()
                .backing_meter()
                .is_some_and(|actual| actual.same_budget(&meter)),
        );
        eprintln!(
            "actual Native UDP delivered same original budget: {:?}",
            observed
        );
    })
    .await;
    // Always finish the actual source/driver/Node before evaluating the new
    // ownership assertion; an expected RED must not strand a loopback runner.
    drop(source.take());
    drop(data.take());
    let cleanup = capture(async {
        let state = tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
            .await
            .unwrap();
        assert_eq!(state, NodeShutdown::RunningResourcesSettled);
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
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert_eq!(
        budget.used(),
        0,
        "actual original Node cleanup remains required"
    );
    assert!(!budget.quarantined());
    assert_eq!(
        observed,
        Some(true),
        "Native UDP Source lost the original physical backing budget"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_cold_udp_unused_retires_without_quarantine() {
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 * 1024 * 1024).unwrap();
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
        drop(host.data().unwrap()); // Original DATA attached; no UDP start/poll.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::RunningResourcesSettled
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
    assert_eq!(
        budget.used(),
        0,
        "unused original cold UDP must really retire"
    );
    assert!(!budget.quarantined());
}

mod capacity;
mod lifecycle;
mod recovery;

mod proof;

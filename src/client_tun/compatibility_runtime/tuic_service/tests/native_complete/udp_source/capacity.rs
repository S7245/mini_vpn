use super::*;
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_parent_classifies_original_record_capacity() {
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
        let (sender, meter) = data.as_ref().unwrap().native_udp_sender_for_test();
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        let frame = crate::tuic::encode_packet(7, &target, b"record capacity");
        for _ in 0..1024 {
            sender
                .send(
                    1,
                    bytes::Bytes::copy_from_slice_metered(&frame, &meter).unwrap(),
                )
                .await
                .unwrap();
        }
        // Fault injection holds all actual original 257 records, not a fake
        // ReserveError. The next producer is the original real QUIC parent.
        let mut waiters = Vec::new();
        for _ in 0..257 {
            let sender = sender.clone();
            let bytes = bytes::Bytes::copy_from_slice_metered(&frame, &meter).unwrap();
            let mut future = Box::pin(async move { sender.send(1, bytes).await });
            assert!(
                future
                    .as_mut()
                    .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            waiters.push(future);
        }
        source = Some(data.as_ref().unwrap().start_udp_source().unwrap());
        peers[0].send_datagram(frame.into()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while host.udp_first_for_test().0.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (first, additional) = host.udp_first_for_test();
        assert_eq!(
            first.unwrap().io_kind,
            Some(std::io::ErrorKind::OutOfMemory)
        );
        assert_eq!(additional, 0);
        assert!(
            !budget.quarantined(),
            "known record Capacity is not opaque corruption"
        );
        drop((waiters, sender));
        drop(source.take());
        drop(data.take());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::FailedSettled
        );
        assert_eq!(
            host.udp_first_for_test().1,
            0,
            "real JOIN must deduplicate already reported original fact"
        );
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

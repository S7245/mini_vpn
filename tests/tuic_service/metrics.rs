//! Counts actual successful F publication once through the public API and SAME
//! Metrics Arc across a sole successor. Polls, pending Open and stop do not count.
use super::*;
#[path = "pressure_fixture.rs"]
mod wire;

#[tokio::test(flavor = "current_thread")]
async fn public_f_metrics_count_each_publication_once_across_successor_not_pending_open() {
    let mut server = wire::server();
    let cfg = wire::config(&server);
    let counters = Arc::new(Metrics::default());
    let (device, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let mut service = prepare_tuic_service(
        device,
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        counters.clone(),
        NoopSink,
    );
    let control = service.take_handle().unwrap();
    let mut apps = Apps::new(application);
    apps.tcp_payload = wire::PAYLOAD;
    let mut peers: Vec<quinn::Connection> = Vec::with_capacity(2);
    let mut stream = None;
    let mut pending = false;
    assert_eq!(counters.snapshot(0, 0).relays_spawned, 0);
    let scenario = wire::capture(async {
        for generation in 0..2 {
            wire::drive(&mut service, async {
                if generation == 1 {
                    let release = std::sync::atomic::AtomicBool::new(true);
                    wire::retire(&mut stream, &release).await;
                }
                let incoming = server.accept().await.unwrap();
                if generation == 1 {
                    tokio::time::timeout(Duration::from_secs(2), peers[0].closed())
                        .await
                        .unwrap();
                    apps.restart();
                }
                let peer = incoming.await.unwrap();
                wire::auth(&peer, &cfg).await;
                peers.push(peer);
            })
            .await;
            assert_eq!(
                counters.snapshot(0, 0).relays_spawned,
                generation as u64,
                "startup is not publication"
            );
            wire::drive(&mut service, async {
                let remote = async {
                    stream = Some(wire::healthy(&peers[generation]).await);
                };
                let application = async {
                    while apps.tcp_reply != b"first" {
                        apps.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                };
                tokio::join!(remote, application);
            })
            .await;
            assert_eq!(
                counters.snapshot(0, 0).relays_spawned,
                generation as u64 + 1,
                "one actual F publication, original shared counter"
            );
            for _ in 0..4 {
                assert!(
                    tokio::time::timeout(Duration::from_millis(1), service.run())
                        .await
                        .is_err()
                );
                assert_eq!(
                    counters.snapshot(0, 0).relays_spawned,
                    generation as u64 + 1,
                    "repeat polls do not republish/count"
                );
            }
            if generation == 0 {
                assert!(control.request_rebuild());
            }
        }
        let mut socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; 4096]),
            tcp::SocketBuffer::new(vec![0; 4096]),
        );
        socket
            .connect(
                apps.iface.context(),
                (Ipv4Address::from(apps.target), 10000),
                40003,
            )
            .unwrap();
        let extra = apps.sockets.add(socket);
        let mut sent = false;
        wire::drive(&mut service, async {
            loop {
                apps.turn().await;
                let socket = apps.sockets.get_mut::<tcp::Socket>(extra);
                if socket.can_send() && !sent {
                    socket.send_slice(b"pending").unwrap();
                    sent = true;
                }
                if sent && socket.state() == tcp::State::Established && socket.send_queue() == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        pending = true;
        for _ in 0..4 {
            assert!(
                tokio::time::timeout(Duration::from_millis(1), service.run())
                    .await
                    .is_err()
            );
            assert_eq!(
                counters.snapshot(0, 0).relays_spawned,
                2,
                "pending one-credit Open is not successful publication"
            );
        }
        assert!(!control.request_rebuild());
    })
    .await;
    control.request_stop();
    let cleanup = wire::capture(async {
        let release = std::sync::atomic::AtomicBool::new(true);
        let report = tokio::time::timeout(Duration::from_secs(15), async {
            let (report, ()) = tokio::join!(service.run(), wire::retire(&mut stream, &release));
            report
        })
        .await
        .unwrap();
        println!(
            "public metrics final {report:?}; successful_count={}",
            counters.snapshot(0, 0).relays_spawned
        );
        assert!(report.device_settled());
        assert_eq!(
            report.device_failed(),
            pending,
            "known cancelled provider remains FailedSettled"
        );
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
            assert_eq!(counters.snapshot(0, 0).relays_spawned, 2);
        }
        assert_eq!(control.observe_shutdown().await, TuicServicePhase::Complete);
    })
    .await;
    drop(stream);
    drop(service);
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}

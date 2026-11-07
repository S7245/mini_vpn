//! External public caller full-load/recovery matrix; exact original F/W census
//! is separately measured by the private saved-root tracer using this fixture.
//! Public evidence observes actual4096 streams,4225 appTCP, realzeroWindow,
//! healthyTCP+sameUDP, clean retirement and lifetime sole successor.
use super::*;
#[path = "full_fixture.rs"]
mod wire;

#[tokio::test(flavor = "current_thread")]
async fn public_full4096_pressure_then_clean_unique_successor_tcp_same_udp() {
    let server = wire::server();
    let cfg = wire::config(&server);
    let runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    assert_eq!(runtime.tcp_socket_buffer_bytes(), (65535, 65535));
    let (device, application) = LoopbackTunDevice::bounded_pair(16384, 4);
    let mut service = prepare_tuic_service(
        device,
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let control = service.take_handle().unwrap();
    assert!(service.take_handle().is_none());
    let mut app = wire::App::new(application);
    let mut peers = Vec::with_capacity(2);
    let mut current = None;
    let scenario=wire::capture(async {
        wire::drive(&mut service,Duration::from_secs(5),async {
            let peer=server.accept().await.unwrap().await.unwrap();wire::auth(&peer,&cfg).await;
            peers.push(peer.clone());current=Some(wire::Peer::new(peer));
        }).await;
        wire::fill(&mut service,&mut app,current.as_mut().unwrap()).await;
        assert_eq!(current.as_ref().unwrap().count(),4096);
        app.connect(wire::F..wire::TOTAL);
        wire::drive(&mut service,Duration::from_secs(5),async {
            while !app.acked(wire::F..wire::TOTAL){app.turn().await;}
        }).await;
        app.start_c();
        wire::drive(&mut service,Duration::from_secs(5),async {while !app.c_full(){app.turn().await;}}).await;
        wire::health(&mut service,&mut app,current.as_mut().unwrap()).await;
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(1),service.run()).await.is_err());
            assert_eq!(control.phase(),TuicServicePhase::Running);
        }
        assert!(app.c_full());
        assert_eq!(current.as_ref().unwrap().count(),4096);
        assert!(current.as_ref().unwrap().connection.close_reason().is_none());
        assert!(control.request_rebuild());assert!(!control.request_rebuild());
        wire::drive(&mut service,Duration::from_secs(15),async {
            let remote=current.as_mut().unwrap().retire();
            let application=async {while !app.c_closed(){app.turn().await;}};
            tokio::join!(remote,application);
        }).await;
        wire::drive(&mut service,Duration::from_secs(5),async {
            let incoming=server.accept().await.unwrap();
            tokio::time::timeout(Duration::from_secs(2),peers[0].closed()).await.unwrap();
            assert!(app.c_closed());app.restart();
            let peer=incoming.await.unwrap();wire::auth(&peer,&cfg).await;
            peers.push(peer.clone());current=Some(wire::Peer::new(peer));
        }).await;
        wire::drive(&mut service,Duration::from_secs(5),async {
            let remote=current.as_mut().unwrap().accept_to(1);
            let application=async {while !app.acked(0..1){app.turn().await;}};
            tokio::join!(remote,application);
        }).await;
        wire::health(&mut service,&mut app,current.as_mut().unwrap()).await;
        assert_eq!(control.phase(),TuicServicePhase::Running);
        assert!(!control.request_rebuild());
        assert!(tokio::time::timeout(Duration::from_millis(20),server.accept()).await.is_err());
        println!("truepublic4096streams/4225appTCP/realzeroWindow: cleanoldclose→sole successor newTCP30000/sameUDP50000 restored");
    }).await;
    control.request_stop();
    let cleanup = wire::capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), async {
            let remote = async {
                if let Some(peer) = &mut current {
                    peer.retire().await;
                }
            };
            let (report, ()) = tokio::join!(service.run(), remote);
            report
        })
        .await
        .unwrap();
        println!("publicfull final {report:?}");
        assert!(report.device_settled() && !report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
        }
        assert_eq!(control.observe_shutdown().await, TuicServicePhase::Complete);
    })
    .await;
    drop(current);
    drop(service);
    let drained = wire::capture(wire::drain(server, peers)).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drained {
        std::panic::resume_unwind(error);
    }
}

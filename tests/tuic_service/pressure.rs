//! External public caller under the same real130-TCP/UDP pressure load. Precise
//! W128/deadline/error-identity census is separately traced at the private original
//! owner; these tests do not expose or fabricate a public inventory API.
use super::*;
#[path = "pressure_fixture.rs"]
mod wire;

async fn run(rebuild: bool) {
    let mut server = wire::server();
    let config = wire::config(&server);
    let (device, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let mut service = prepare_tuic_service(
        device,
        config.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    assert!(service.take_handle().is_none());
    let mut app = wire::App::new(application);
    let mut peer = None;
    let mut stream = None;
    let scenario=wire::capture(async {
        wire::drive(&mut service,async {
            let connected=server.accept().await.unwrap().await.unwrap();peer=Some(connected);wire::auth(peer.as_ref().unwrap(),&config).await;
            let remote=async {let (s,())=tokio::join!(wire::healthy(peer.as_ref().unwrap()),wire::udp_reply(peer.as_ref().unwrap()));stream=Some(s);};
            let application=async {while app.tcp_reply!=b"first" || app.udp_reply!=b"udp-reply" {app.turn().await;tokio::time::sleep(Duration::from_millis(1)).await;}};
            tokio::join!(remote,application);
        }).await;
        wire::drive(&mut service,async {
            for _ in 0..129 {app.add();while !app.last_acked(){app.turn().await;tokio::time::sleep(Duration::from_millis(1)).await;}}
        }).await;
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(1),service.run()).await.is_err());
            assert_eq!(handle.phase(),TuicServicePhase::Running);
        }
        app.ping();
        wire::drive(&mut service,async {
            let remote=async {tokio::join!(wire::ping(stream.as_mut().unwrap()),wire::udp_reply(peer.as_ref().unwrap()));};
            let application=async {while app.tcp_reply!=b"second" || app.udp_reply!=b"udp-reply" {app.turn().await;tokio::time::sleep(Duration::from_millis(1)).await;}};
            tokio::join!(remote,application);
        }).await;
        assert!(app.last_acked());assert!(peer.as_ref().unwrap().close_reason().is_none());
        assert!(tokio::time::timeout(Duration::from_millis(20),server.accept()).await.is_err(),"no pressure redial");
        assert!(tokio::time::timeout(Duration::from_millis(20),peer.as_ref().unwrap().accept_bi()).await.is_err(),"original one-stream peer credit remains occupied");
        if rebuild {assert!(handle.request_rebuild());} else {handle.request_stop();}
        assert!(!handle.request_rebuild(),"stop/admitted rebuild cannot create another request");
        println!("public pressure trigger rebuild={rebuild}:130 TCP acknowledged, healthy TCP/UDP survive4 cancelled run borrows");
    }).await;
    if scenario.is_err() {
        handle.request_stop();
    }
    let cleanup = wire::capture(async {
        let release = std::sync::atomic::AtomicBool::new(true);
        let report = tokio::time::timeout(Duration::from_secs(15), async {
            let (report, ()) = tokio::join!(service.run(), wire::retire(&mut stream, &release));
            report
        })
        .await
        .unwrap();
        println!("public pressure rebuild={rebuild} final={report:?}");
        assert!(report.device_settled());
        assert!(report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!(report.rebuilds, 0);
        assert_eq!(handle.observe_shutdown().await, TuicServicePhase::Complete);
        assert!(!handle.request_rebuild());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.accept())
                .await
                .is_err(),
            "pending original Open cancellation is known failure: no successor"
        );
    })
    .await;
    drop(stream);
    drop(service);
    let drained = wire::capture(wire::drain(server, peer)).await;
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

#[tokio::test(flavor = "current_thread")]
async fn public_pressure_stop_keeps_healthy_data_and_actually_settles_known_failure() {
    run(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_pressure_rebuild_refuses_successor_after_original_open_cancellation() {
    run(true).await;
}

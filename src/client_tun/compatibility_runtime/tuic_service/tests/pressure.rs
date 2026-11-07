//! Real W128 pressure at the original saved service. Reads the existing private
//! owner inventory; public caller evidence is a separate external test. No fake
//! provider, changed clock/deadline, injected receipt or manufactured clean zero.
use super::*;
use std::sync::atomic::Ordering;
#[path = "../../../../../tests/tuic_service/pressure_fixture.rs"]
mod wire;

#[tokio::test(flavor = "current_thread")]
async fn w128_pressure_keeps_healthy_data_and_original_deadlines_then_refuses_failed_rebuild() {
    let mut server = wire::server();
    let cfg = wire::config(&server);
    let (device, app_device) = LoopbackTunDevice::bounded_pair(64, 4);
    let mut service = prepare(
        device,
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut app = wire::App::new(app_device);
    let mut peer = None;
    let mut stream = None;
    let mut original = None;
    let scenario=wire::capture(async {
        wire::drive(&mut service,async {
            let p=server.accept().await.unwrap().await.unwrap();peer=Some(p);wire::auth(peer.as_ref().unwrap(),&cfg).await;
            let remote=async {let (s,())=tokio::join!(wire::healthy(peer.as_ref().unwrap()),wire::udp_reply(peer.as_ref().unwrap()));stream=Some(s);};
            let application=async {while app.tcp_reply!=b"first" || app.udp_reply!=b"udp-reply" {app.turn().await;tokio::time::sleep(Duration::from_millis(1)).await;}};
            tokio::join!(remote,application);
        }).await;
        let control=saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        wire::drive(&mut service,async {
            for _ in 0..129 {
                app.add();
                while !app.last_acked() {app.turn().await;tokio::time::sleep(Duration::from_millis(1)).await;}
            }
            loop {
                let state=control.flow_observation().unwrap();
                assert!(state.w<=128);assert!(state.factory_started<=129);assert!(state.audit);
                if state.w==128 && state.opening_providers==128 {println!("pressure reached {state:?}");break;}
                app.turn().await;tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await;
        let before=control.flow_observation().unwrap();
        assert_eq!((before.live,before.w,before.f,before.factory_started),(129,128,129,129));
        assert_eq!((before.attempts,before.opening_providers,before.published,before.running),(129,128,1,3));
        assert_eq!(before.reserved,128*256*1024);assert_eq!(before.escrow_bytes,128*wire::PAYLOAD.len());
        let contracts=control.flow_contracts();assert_eq!(contracts.len(),129);
        assert_eq!(contracts.iter().filter(|c| c.w && c.f && c.attempt && !c.published).count(),128);
        app.ping();
        wire::drive(&mut service,async {
            let remote=async {tokio::join!(wire::ping(stream.as_mut().unwrap()),wire::udp_reply(peer.as_ref().unwrap()));};
            let application=async {while app.tcp_reply!=b"second" || app.udp_reply!=b"udp-reply" {app.turn().await;tokio::time::sleep(Duration::from_millis(1)).await;}};
            tokio::join!(remote,application);
        }).await;
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(1),service.run()).await.is_err());
            assert_eq!(control.flow_contracts(),contracts,"all original identities/deadlines retained");
        }
        let after=control.flow_observation().unwrap();
        assert_eq!((after.live,after.w,after.f,after.factory_started),(129,128,129,129));assert!(after.audit);assert!(app.last_acked());
        assert!(!saved.stop.stopping.load(Ordering::Acquire));
        assert!(handle.request_rebuild());assert!(!handle.request_rebuild());
        println!("healthy TCP/UDP survived W128; cancelled borrows preserved129 contracts; rebuild requested");
    }).await;
    if scenario.is_err() {
        handle.request_stop();
    }
    let cleanup = wire::capture(async {
        let control = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        let release = std::sync::atomic::AtomicBool::new(scenario.is_err());
        let report = tokio::time::timeout(Duration::from_secs(15), async {
            let retired = wire::retire(&mut stream, &release);
            let driven = std::future::poll_fn(|cx| {
                let mut run = std::pin::pin!(service.run());
                let result = run.as_mut().poll(cx);
                if let Some(control) = &control {
                    if original.is_none() && control.retained_provider_failure().is_some() {
                        assert!(result.is_pending());
                        assert!(!control.driver_complete.load(Ordering::Acquire));
                        assert!(peer.as_ref().unwrap().close_reason().is_none());
                        assert_eq!(
                            control.retained_provider_io_kind(),
                            Some(std::io::ErrorKind::Interrupted)
                        );
                        original = control.retained_provider_failure();
                        release.store(true, Ordering::Release);
                    }
                }
                result
            });
            let (report, ()) = tokio::join!(driven, retired);
            report
        })
        .await
        .unwrap();
        println!("pressure final {report:?}");
        assert!(report.device_settled());
        assert!(report.device_failed());
        assert_eq!(report.rebuilds, 0);
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!(handle.observe_shutdown().await, Phase::Complete);
        assert!(!handle.request_rebuild());
        if scenario.is_ok() {
            let completion = service.completion.as_ref().unwrap();
            assert!(completion._retired.is_none());
            completion
                ._node
                .as_ref()
                .unwrap()
                .assert_service_clean_for_test();
            let final_state = completion.control.flow_observation().unwrap();
            println!("actual final inventory {final_state:?}");
            assert_eq!(
                (
                    final_state.live,
                    final_state.w,
                    final_state.f,
                    final_state.reserved,
                    final_state.escrow_bytes,
                    final_state.running
                ),
                (0, 0, 0, 0, 0, 0)
            );
            assert_eq!(
                (
                    final_state.factory_started,
                    final_state.published,
                    final_state.released,
                    final_state.joined
                ),
                (129, 1, 129, 3)
            );
            assert!(final_state.audit);
            assert!(original.is_some());
            assert_eq!(completion.control.retained_provider_failure(), original);
            let (_, view, _) = original.as_ref().unwrap();
            assert_eq!(view.phase, crate::upstream::TcpOpenPhase::Settled);
            assert!(!view.lease_held && !view.retirement_pending);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), server.accept())
                    .await
                    .is_err(),
                "no successor after known cancellation failure"
            );
        }
    })
    .await;
    drop(stream);
    drop(service);
    let drain = wire::capture(wire::drain(server, peer)).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drain {
        std::panic::resume_unwind(error);
    }
}

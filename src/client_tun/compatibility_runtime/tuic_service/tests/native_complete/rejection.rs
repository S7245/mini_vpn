//! Actual DNS root QNAME -> fake-IP -> original hard Service provider rejection.
//! No source, policy or queue is replaced; rejected Flow never publishes roles.
use super::*;
use crate::shared::errors::{FailureClass, FailurePhase};

#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_direct_root_dns_rejection_preserves_first_and_budget() {
    rejection(true, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_chunk_root_dns_rejection_preserves_first_and_budget() {
    rejection(false, false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_permit_root_dns_rejection_preserves_first_and_budget() {
    rejection(false, true).await;
}
async fn rejection(direct: bool, permit: bool) {
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_chunk: direct.then_some("1"),
        tuic_native_ordered: (!direct).then_some("1"),
        native_egress_permit: permit.then_some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        if direct {
            TuicNativeOverride::NativeChunkPump
        } else {
            TuicNativeOverride::NativeOrderedPump
        }
    );
    assert_eq!(
        policy.native_engine(),
        if permit {
            NativeTcpRelayEngine::PermitPump
        } else {
            NativeTcpRelayEngine::ChunkPump
        }
    );
    let mut cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let mut runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy)
        .with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(32 << 20), 64)
        .unwrap();
    runtime.tun_mtu = 1500;
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let device_in = inbound.clone();
    let device_out = outbound.clone();
    let mut service = prepare_factory(
        move |meter: Option<bytes::BackingMeter>| async move {
            Ok::<_, ClientError>(LoopbackTunDevice::native_with_factory(
                64,
                1500,
                &meter.unwrap(),
                || Ok((device_in, device_out)),
            )?)
        },
        Configuration {
            tuic: cfg.clone(),
            composition: (),
        },
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().unwrap();
    let app_meter = saved.app_meter.clone().unwrap();
    let mut peer = None;
    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5),async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap(); auth(&remote,&cfg).await; peer=Some(remote);
                    let fake = super::super::rebuild::dns(&inbound,&outbound,"").await;
                    let mut app = Apps::with_target(outbound.clone(),inbound.clone(),fake); app.sent_udp=true;
                    loop {
                        app.turn().await;
                        let owner=saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                        let flow=owner.flow_observation().unwrap();
                        if flow.released == 1 {
                            assert_eq!((flow.factory_started,flow.published,flow.released,flow.joined,flow.live,flow.w,flow.f),(1,0,1,0,0,0,0));
                            assert!(flow.audit);
                            assert!(owner.flow_native_permit_queue().is_none() && owner.flow_native_chunk_queue().is_none());
                            assert!(owner.flow_native_backing_meter().is_none()); break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report=service.run()=>panic!("hard Service ended before actual root DNS rejection: {report:?}"),
            }
        }).await.expect("original five-second root DNS target rejection");
        let owner=saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        {
            let ledger=owner.ledger(); let facts=ledger.first_failure.as_ref().unwrap().facts();
            assert!(!ledger.quarantined()); assert_eq!(ledger.additional_failures,0);
            assert_eq!(facts.origin.phase,FailurePhase::TcpOpen);
            assert_eq!(facts.class,FailureClass::Io);
            assert_eq!(facts.io_kind,Some(std::io::ErrorKind::InvalidInput));
        }
        fixture::first_failure_identity(&owner).unwrap()
    }).await;
    handle.request_stop();
    let deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let report = tokio::time::timeout_at(deadline, service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        assert_eq!(handle.phase(), Phase::Complete);
        let completed = service.completion.as_ref().unwrap();
        completed
            ._node
            .as_ref()
            .unwrap()
            .assert_service_clean_for_test();
        let owner = &completed.control;
        let flow = owner.flow_observation().unwrap();
        assert_eq!(
            (flow.live, flow.w, flow.f, flow.published, flow.joined),
            (0, 0, 0, 0, 0)
        );
        assert!(flow.audit && !owner.ledger().quarantined());
        assert_eq!(*saved.deadline.lock().unwrap(), Some(deadline));
        assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
        assert_eq!(app_meter.snapshot().physical_bytes, 0);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
        fixture::first_failure_identity(owner).unwrap()
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"test complete");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"test complete");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    let first = scenario.unwrap_or_else(|p| std::panic::resume_unwind(p));
    let after = cleanup.unwrap_or_else(|p| std::panic::resume_unwind(p));
    assert_eq!(first, after);
    assert!(drained.unwrap());
    eprintln!(
        "hard Native prepublication DNS rejection: direct={direct} permit={permit} first={first:?} budget0/nonQ deadline={deadline:?}"
    );
}

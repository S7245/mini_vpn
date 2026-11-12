//! Real Service prepublication rejection and saved Root settlement.
//! Owns only the peer/App scenario and passive proof; the original Root owns
//! the provider attempt, first error, F and Node cleanup.

use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{auth, capture, config, first_failure_identity, Apps};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenMode {
    ByteOwned,
    NativeDirect,
    NativeOrderedChunk,
    NativePermit,
}

fn policy_for(mode: OpenMode) -> Arc<RelayFamilyPolicy> {
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        d16_byte_owned: (mode == OpenMode::ByteOwned).then_some("1"),
        tuic_native_chunk: (mode == OpenMode::NativeDirect).then_some("1"),
        tuic_native_ordered: matches!(mode, OpenMode::NativeOrderedChunk | OpenMode::NativePermit)
            .then_some("1"),
        native_egress_permit: (mode == OpenMode::NativePermit).then_some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        match mode {
            OpenMode::ByteOwned => TuicNativeOverride::D16DirectOrdered,
            OpenMode::NativeDirect => TuicNativeOverride::NativeChunkPump,
            OpenMode::NativeOrderedChunk | OpenMode::NativePermit => {
                TuicNativeOverride::NativeOrderedPump
            }
        }
    );
    assert_eq!(
        policy.native_engine(),
        if mode == OpenMode::NativePermit {
            NativeTcpRelayEngine::PermitPump
        } else {
            NativeTcpRelayEngine::ChunkPump
        }
    );
    policy
}

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_failed_open_releases_unpublished_flow_without_allocating_d16_roles() {
    failed_open_case(OpenMode::ByteOwned).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_direct_failed_open_releases_unpublished_flow_before_roles() {
    failed_open_case(OpenMode::NativeDirect).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_failed_open_releases_unpublished_flow_before_roles() {
    failed_open_case(OpenMode::NativeOrderedChunk).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_permit_failed_open_releases_unpublished_flow_before_roles() {
    failed_open_case(OpenMode::NativePermit).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_permit_root_dns_target_rejects_synchronously_before_publication() {
    root_dns_target_case(OpenMode::NativePermit).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_direct_root_dns_target_rejects_synchronously_before_publication() {
    root_dns_target_case(OpenMode::NativeDirect).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_root_dns_target_rejects_synchronously_before_publication() {
    root_dns_target_case(OpenMode::NativeOrderedChunk).await;
}

async fn root_dns_target_case(mode: OpenMode) {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = policy_for(mode);
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut service = prepare(
        LoopbackTunDevice::new(inbound.clone(), outbound.clone()),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peer = None;
    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote);
                    let fake = super::rebuild::dns(&inbound, &outbound, "").await;
                    let mut app = Apps::with_target(outbound.clone(), inbound.clone(), fake);
                    app.sent_udp = true;
                    loop {
                        app.turn().await;
                        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                        let flow = owner.flow_observation().unwrap();
                        if flow.released == 1 {
                            assert_eq!((flow.factory_started, flow.published, flow.released,
                                        flow.joined, flow.live, flow.w, flow.f),
                                       (1, 0, 1, 0, 0, 0, 0));
                            assert!(flow.audit);
                            assert!(owner.flow_native_permit_queue().is_none());
                            assert!(owner.flow_native_backing_meter().is_none());
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("Native Permit Service ended before synchronous rejection: {report:?}"),
            }
        }).await.expect("real DNS root target did not reach original Root rejection");
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        {
            let ledger = owner.ledger();
            assert!(!ledger.quarantined());
            let first = ledger.first_failure.as_ref().expect("original InvalidTarget retained");
            assert_eq!(first.facts().origin.phase, crate::shared::errors::FailurePhase::TcpOpen);
            assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Io);
            assert_eq!(first.facts().io_kind, Some(std::io::ErrorKind::InvalidInput));
        eprintln!("Native synchronous rejection: mode={mode:?} first={:?}", first.facts());
        }
        first_failure_identity(&owner).expect("original InvalidTarget identity")
    }).await;
    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
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
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        first_failure_identity(owner).expect("same original first error after shutdown")
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    let first = scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    let after = cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert_eq!(first, after);
    assert!(drained.unwrap());
}

async fn failed_open_case(mode: OpenMode) {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 0, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = policy_for(mode);
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound, outbound),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peer = None;
    let mut original = None;

    let scenario = capture(async {
        let mut open_deadline = None;
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote);
                    loop {
                        app.turn().await;
                        let owner = saved.session.lock().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
                        if let Some(owner) = owner {
                            if owner.flow_observation().unwrap().opening_providers == 1 {
                                let contracts = owner.flow_contracts();
                                assert_eq!(contracts.len(), 1);
                                open_deadline = Some(contracts[0].open_deadline);
                                assert!(owner.flow_d16_physical_snapshots().is_none());
                                assert!(owner.flow_native_backing_meter().is_none());
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("D16 open completed before rejection: {report:?}"),
            }
        }).await.unwrap();
        let deadline = open_deadline.unwrap();
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline) + Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    loop {
                        app.turn().await;
                        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                        if let Some(failure) = owner.retained_provider_failure() {
                            original = Some(failure);
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("D16 rejection stopped unrelated Service: {report:?}"),
            }
        }).await.unwrap();
        assert!(std::time::Instant::now() >= deadline);
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.factory_started, flow.published, flow.released, flow.joined), (1, 0, 1, 0));
        assert_eq!((flow.live, flow.w, flow.f, flow.reserved, flow.escrow_bytes), (0, 0, 0, 0, 0));
        assert!(flow.audit);
        assert!(owner.flow_d16_physical_snapshots().is_none());
        assert!(owner.flow_d16_backing_meter().is_none());
        assert!(owner.flow_native_backing_meter().is_none());
        let ledger = owner.ledger();
        assert!(!ledger.quarantined());
        let first = ledger.first_failure.as_ref().expect("original failed open retained");
        assert_eq!(first.facts().origin.phase, crate::shared::errors::FailurePhase::TcpOpen);
        assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Io);
        assert!(!first.facts().opaque && !first.facts().source_content_omitted);
        assert!(first.text_bytes().is_empty());
        assert!(original.is_some());
        (first as *const _ as usize, format!("{:?}", first.facts()), first.text_bytes().to_vec())
    }).await;

    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        let completed = service.completion.as_ref().unwrap();
        completed
            ._node
            .as_ref()
            .unwrap()
            .assert_service_clean_for_test();
        let owner = &completed.control;
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.live, flow.w, flow.f, flow.joined), (0, 0, 0, 0));
        assert!(flow.audit);
        assert_eq!(owner.retained_provider_failure(), original);
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        let ledger = owner.ledger();
        assert!(!ledger.quarantined());
        let first = ledger.first_failure.as_ref().unwrap();
        (
            first as *const _ as usize,
            format!("{:?}", first.facts()),
            first.text_bytes().to_vec(),
        )
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    let first = scenario.unwrap_or_else(|error| std::panic::resume_unwind(error));
    let after = cleanup.unwrap_or_else(|error| std::panic::resume_unwind(error));
    assert_eq!(first, after);
    assert!(drained.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn byte_owned_pending_open_cancels_without_publishing_roles() {
    pending_open_case(OpenMode::ByteOwned).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_direct_pending_open_cancels_without_publishing_roles() {
    pending_open_case(OpenMode::NativeDirect).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_pending_open_cancels_without_publishing_roles() {
    pending_open_case(OpenMode::NativeOrderedChunk).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_permit_pending_open_cancels_without_publishing_roles() {
    pending_open_case(OpenMode::NativePermit).await;
}

async fn pending_open_case(mode: OpenMode) {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 0, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = policy_for(mode);
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare(
        LoopbackTunDevice::new(inbound, outbound),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peer = None;
    let setup = capture(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote);
                    loop {
                        app.turn().await;
                        let owner = saved.session.lock().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
                        if let Some(owner) = owner {
                            let flow = owner.flow_observation().unwrap();
                            if flow.opening_providers == 1 {
                                assert_eq!((flow.live, flow.f, flow.published, flow.joined), (1, 1, 0, 0));
                                assert!(flow.audit);
                                assert!(owner.flow_d16_physical_snapshots().is_none());
                                assert!(owner.flow_native_backing_meter().is_none());
                                assert!(owner.ledger().first_failure.is_none());
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("pending D16 open completed before stop: {report:?}"),
            }
        }).await.unwrap();
    }).await;
    if let Err(error) = setup {
        if let Some(peer) = peer.take() {
            peer.close(0u32.into(), b"done");
        }
        server.close(0u32.into(), b"done");
        std::panic::resume_unwind(error);
    }

    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        let owner = &service.completion.as_ref().unwrap().control;
        let flow = owner.flow_observation().unwrap();
        assert_eq!(
            (flow.live, flow.w, flow.f, flow.opening_providers),
            (0, 0, 0, 0)
        );
        assert_eq!((flow.published, flow.released, flow.joined), (0, 1, 0));
        assert!(flow.audit);
        let provider = owner
            .retained_provider_failure()
            .expect("cancelled provider retained");
        assert!(provider.2.contains("Cancelled"));
        let ledger = owner.ledger();
        assert!(!ledger.quarantined());
        let first = ledger
            .first_failure
            .as_ref()
            .expect("cancelled open first failure");
        assert_eq!(
            first.facts().origin.phase,
            crate::shared::errors::FailurePhase::TcpOpen
        );
        assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Io);
        assert_eq!(first.facts().io_kind, Some(std::io::ErrorKind::Other));
        assert!(owner.flow_d16_physical_snapshots().is_none());
        assert!(owner.flow_native_backing_meter().is_none());
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        service
            .completion
            .as_ref()
            .unwrap()
            ._node
            .as_ref()
            .unwrap()
            .assert_service_clean_for_test();
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}

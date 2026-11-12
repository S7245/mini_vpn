//! Owns one real ordered Chunk Service delivery and its original queue receipt.
//! The five roles, FlowMirror, Root, and Node retain all settlement authority.

use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{auth, capture, config, Apps};

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_delivers_exact_bytes_and_joins_original_root() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeOrderedPump
    );
    assert_eq!(policy.native_engine(), NativeTcpRelayEngine::ChunkPump);
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
    let mut stream = None;
    let mut original_queue = None;
    let mut backing = None;
    let mut original_id = None;

    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_open = async {
                        let (send, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        stream = Some((send, recv));
                    };
                    let app_open = async {
                        loop {
                            app.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            let flow = owner.flow_observation().unwrap();
                            if flow.published == 1 && flow.running == 5 {
                                let contracts = owner.flow_contracts();
                                assert_eq!(contracts.len(), 1);
                                original_id = Some(contracts[0].id);
                                original_queue = owner.flow_native_chunk_queue();
                                backing = owner.flow_native_backing_meter();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("ordered Chunk Service ended before publication: {report:?}"),
            }
        }).await.unwrap();
        let queue = original_queue.as_ref().unwrap();
        assert!(backing.is_some() && app.tcp_reply.is_empty());
        let payload: Vec<u8> = (0..8192).map(|index| (index % 251) as u8).collect();
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                result = stream.as_mut().unwrap().0.write_all(&payload) => result.unwrap(),
                report = service.run() => panic!("ordered Chunk Service ended before peer DATA: {report:?}"),
            }
        }).await.unwrap();
        let delivered = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                app.turn().await;
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                if app.tcp_reply.len() >= 8192 {
                    break;
                }
            }
        }).await;
        if delivered.is_err() {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("ordered Chunk delivery deferred: app={} queue={:?} flow={:?} meter={:?}",
                app.tcp_reply.len(), queue.custody_snapshot().await, owner.flow_observation(),
                backing.as_ref().unwrap().snapshot());
        }
        delivered.expect("ordered Chunk App exact delivery before original 5 seconds");
        assert_eq!(app.tcp_reply, payload);
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let contracts = owner.flow_contracts();
        assert_eq!(contracts.len(), 1);
        assert_eq!(Some(contracts[0].id), original_id);
        let (physical, custody) = queue.custody_snapshot().await;
        eprintln!("ordered Chunk App delivery: id={:?} queue={physical:?} emitted={} consumed={} discarded={} source={:?} meter={:?}",
            original_id.unwrap(), custody.emitted, custody.consumed, custody.discarded_bytes,
            owner.flow_native_pump_source_read_bytes(), backing.as_ref().unwrap().snapshot());
        assert!(custody.emitted >= 8192);
        assert_eq!(custody.consumed, 8192);
        assert_eq!(custody.discarded_bytes, 0);
        assert_eq!(owner.flow_native_pump_source_read_bytes(), Some(8192));
        assert!(physical.queued_bytes <= physical.capacity_bytes);
        assert!(owner.ledger().first_failure.is_none());

        handle.request_stop();
        let (mut send, mut recv) = stream.take().unwrap();
        send.finish().unwrap();
        recv.stop(78u32.into()).unwrap();
        drop((send, recv));
    }).await;

    if scenario.is_err() {
        handle.request_stop();
    }
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        if scenario.is_err() {
            fixture::observe_failed_native_stop(service.run(), &handle, original_deadline, None).await;
            return;
        }
        let report = tokio::time::timeout_at(original_deadline, service.run()).await.unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(handle.phase(), Phase::Complete);
        let owner = service.completion.as_ref().unwrap().control.clone();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.joined, flow.released, flow.f), (1, 5, 1, 0));
        assert!(flow.audit && !owner.ledger().quarantined() && owner.ledger().first_failure.is_none());
        let (messages, bytes, transferred, discarded, _) =
            flow.last_data_disposition.expect("original ordered Chunk physical receipt");
        assert!(messages > 0 && bytes >= 8192);
        assert_eq!(bytes, transferred + discarded);
        let (physical, custody) = original_queue.as_ref().unwrap().custody_snapshot().await;
        assert_eq!(physical.queued_bytes, 0);
        assert_eq!(custody.consumed.checked_add(custody.discarded_bytes), Some(custody.emitted));
        assert_eq!(custody.emitted, 8192);
        assert_eq!((custody.consumed, custody.discarded_bytes), (8192, 0));
        let meter = backing.as_ref().unwrap().snapshot();
        assert_eq!((meter.allocations, meter.payload_bytes, meter.physical_bytes), (0, 0, 0));
        eprintln!("ordered Chunk terminal: id={:?} flow={flow:?} queue={physical:?} emitted={} consumed={} discarded={} meter={meter:?}",
            original_id.unwrap(), custody.emitted, custody.consumed, custody.discarded_bytes);
    }).await;
    drop(stream.take());
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(drained.unwrap());
}

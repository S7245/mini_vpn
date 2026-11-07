//! True ordered Chunk Service pressure at its original queue, then exact App delivery.
//! The original FlowMirror and Root retain queue, role, receipt and Node ownership.

use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{auth, capture, config, Apps};

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_original_queue_full_stalls_source_then_recovers() {
    const REPLY_BYTES: usize = 12 * 1024 * 1024;
    let mut server_config = UdpPair::server_config(8, 65536, 100, true);
    let mut cubic = quinn::congestion::CubicConfig::default();
    cubic.initial_window(8 * 1024 * 1024);
    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .congestion_controller_factory(Arc::new(cubic));
    let mut server =
        quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
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
    let mut writer = None;

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
        let capacity = queue.snapshot().await.capacity_bytes;
        assert_eq!(capacity, 4 * 1024 * 1024);
        assert!(backing.is_some() && app.tcp_reply.is_empty());
        let payload: Vec<u8> = (0..REPLY_BYTES).map(|index| (index % 251) as u8).collect();
        let (send, recv) = stream.take().unwrap();
        writer = Some(tokio::spawn(async move {
            let mut send = send;
            send.write_all(&payload).await.unwrap();
            send
        }));

        let pressure = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let physical = queue.snapshot().await;
                assert!(physical.queued_bytes <= capacity);
                if physical.queued_bytes == capacity {
                    break physical;
                }
            }
        }).await;
        if pressure.is_err() {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("Chunk pressure RED: id={original_id:?} queue={:?} source={:?} meter={:?} app={} writer_finished={}",
                queue.custody_snapshot().await, owner.flow_native_pump_source_read_bytes(),
                backing.as_ref().unwrap().snapshot(), app.tcp_reply.len(), writer.as_ref().unwrap().is_finished());
        }
        let full = pressure.expect("original ordered Chunk queue did not reach exact 4 MiB cap");
        assert_eq!(full.high_water_bytes, capacity);
        assert!(backing.as_ref().unwrap().snapshot().payload_bytes > 0);
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let settle_until = tokio::time::Instant::now() + Duration::from_millis(100);
        while tokio::time::Instant::now() < settle_until {
            assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
            assert_eq!(queue.snapshot().await.queued_bytes, capacity);
        }
        let source_before = owner.flow_native_pump_source_read_bytes().unwrap();
        let blocked_until = tokio::time::Instant::now() + Duration::from_millis(200);
        while tokio::time::Instant::now() < blocked_until {
            assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
            assert_eq!(queue.snapshot().await.queued_bytes, capacity);
        }
        let source_after = owner.flow_native_pump_source_read_bytes().unwrap();
        assert_eq!(source_after, source_before, "ordered Chunk source advanced with full queue");
        assert!(source_after < REPLY_BYTES);
        assert!(app.tcp_reply.is_empty());
        eprintln!("Chunk pressure full: id={original_id:?} queue={full:?} source={source_after} meter={:?}",
            backing.as_ref().unwrap().snapshot());

        let recovered = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    loop {
                        app.turn().await;
                        assert!(app.tcp_reply.len() <= REPLY_BYTES);
                        if app.tcp_reply.len() == REPLY_BYTES { break; }
                    }
                } => {},
                report = service.run() => panic!("ordered Chunk Service ended before recovery: {report:?}"),
            }
        }).await;
        if recovered.is_err() {
            eprintln!("Chunk recovery RED: app={} queue={:?} meter={:?} writer_finished={}",
                app.tcp_reply.len(), queue.custody_snapshot().await,
                backing.as_ref().unwrap().snapshot(), writer.as_ref().unwrap().is_finished());
        }
        recovered.expect("original ordered Chunk pressure did not recover in 5 seconds");
        assert!(app.tcp_reply.iter().enumerate().all(|(index, byte)| *byte == (index % 251) as u8));
        assert_eq!(owner.flow_native_pump_source_read_bytes(), Some(REPLY_BYTES));
        let mut send = writer.take().unwrap().await.unwrap();
        send.finish().unwrap();
        let mut recv = recv;
        recv.stop(78u32.into()).unwrap();
        drop((send, recv));
        let (physical, custody) = queue.custody_snapshot().await;
        assert_eq!(physical.queued_bytes, 0);
        assert_eq!((custody.emitted, custody.consumed, custody.discarded_bytes), (REPLY_BYTES, REPLY_BYTES, 0));
        assert!(owner.ledger().first_failure.is_none());
    }).await;

    if scenario.is_err() {
        if let Some(writer) = writer.take() {
            writer.abort();
        }
    }
    handle.request_stop();
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
        assert!(flow.audit && !owner.ledger().quarantined && owner.ledger().first_failure.is_none());
        let (messages, bytes, transferred, discarded, _) = flow.last_data_disposition.unwrap();
        assert!(messages > 0);
        assert_eq!((bytes, transferred, discarded), (REPLY_BYTES, REPLY_BYTES, 0));
        let (physical, custody) = original_queue.as_ref().unwrap().custody_snapshot().await;
        assert_eq!(physical.queued_bytes, 0);
        assert_eq!((custody.emitted, custody.consumed, custody.discarded_bytes), (REPLY_BYTES, REPLY_BYTES, 0));
        let meter = backing.as_ref().unwrap().snapshot();
        assert_eq!((meter.allocations, meter.payload_bytes, meter.physical_bytes), (0, 0, 0));
        eprintln!("Chunk pressure terminal: id={original_id:?} flow={flow:?} queue={physical:?} meter={meter:?}");
    }).await;
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

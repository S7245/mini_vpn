//! Real Service Native Permit queue capacity, resumed App drain and Root settlement.
//! The original queue owns DATA; this test does not install a test-only pump.

use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{auth, capture, config, Apps};

#[tokio::test(flavor = "current_thread")]
async fn native_permit_original_queue_reaches_capacity_then_recovers_to_clean_join() {
    capacity_case(768 * 1024, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_permit_full_queue_blocks_peer_window_then_recovers() {
    capacity_case(10 * 1024 * 1024, true).await;
}

async fn capacity_case(reply_bytes: usize, expect_peer_block: bool) {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        native_egress_permit: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeOrderedPump
    );
    assert_eq!(policy.native_engine(), NativeTcpRelayEngine::PermitPump);
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let expected_capacity = runtime.downlink_backpressure.high_bytes;
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
                                original_queue = owner.flow_native_permit_queue();
                                backing = owner.flow_native_backing_meter();
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("Native Permit Service ended before publication: {report:?}"),
            }
        }).await.unwrap();

        let queue = original_queue.as_ref().expect("original Native Permit queue");
        let meter = backing.as_ref().expect("original Node backing meter");
        assert_eq!(queue.snapshot_now().capacity_bytes, expected_capacity);
        assert_eq!(queue.custodied_release_mode(),
            Some(crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnEgressDrain));
        assert!(app.tcp_reply.is_empty());
        let (send, recv) = stream.take().unwrap();
        writer = Some(tokio::spawn(async move {
            let mut send = send;
            send.write_all(&vec![0x5a; reply_bytes]).await.unwrap();
            send
        }));

        let pressure = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let physical = queue.snapshot_now();
                assert!(physical.owned_bytes() <= physical.capacity_bytes);
                if physical.high_water_bytes == physical.capacity_bytes {
                    assert_eq!(physical.owned_bytes(), physical.capacity_bytes);
                    assert!(meter.snapshot().payload_bytes > 0);
                    assert!(app.tcp_reply.is_empty());
                    break physical;
                }
            }
        }).await.unwrap_or_else(|elapsed| {
            eprintln!("Native Permit capacity: queue={:?} meter={:?} writer_finished={}",
                queue.snapshot_now(), meter.snapshot(), writer.as_ref().unwrap().is_finished());
            panic!("original Permit capacity pressure: {elapsed}");
        });
        assert_eq!(pressure.capacity_bytes, expected_capacity);

        if expect_peer_block {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let settle_until = tokio::time::Instant::now() + Duration::from_millis(100);
            while tokio::time::Instant::now() < settle_until {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                assert_eq!(queue.snapshot_now().owned_bytes(), expected_capacity);
            }
            let source_before = owner.flow_native_pump_source_read_bytes().unwrap();
            let blocked_until = tokio::time::Instant::now() + Duration::from_millis(200);
            while tokio::time::Instant::now() < blocked_until {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                assert_eq!(queue.snapshot_now().owned_bytes(), expected_capacity);
            }
            let source_after = owner.flow_native_pump_source_read_bytes().unwrap();
            assert_eq!(source_after, source_before,
                "original Native producer advanced while Permit queue remained full");
            assert!(source_after < reply_bytes);
            assert!(!writer.as_ref().unwrap().is_finished(),
                "peer completed beyond QUIC receive window while original Permit queue was full");
        }

        let egress_bridge = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap()
            .relay_event_test_bridge().unwrap();
        egress_bridge.arm_egress_probe(queue.clone());
        let pressure_ledger = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let (samples, overflow, ambiguous) = egress_bridge.egress_probe_samples();
                assert!(!overflow && !ambiguous, "pressure cap probe lost its queue identity");
                if let Some(sample) = samples.last() {
                    if sample.ledger.admitted > sample.ledger.refunded
                        && sample.ledger.cap_count > 0
                        && sample.ledger.first_cap_remaining > 0 {
                        break sample.ledger;
                    }
                }
            }
        }).await.expect("original pressure cap was not visible before App drain");
        let released_at_pressure = queue.custodied_released_bytes_for_test()
            .expect("original Permit release ledger");
        assert_eq!(pressure_ledger.refunded, released_at_pressure as u64);
        let pressure_cap_end = pressure_ledger.refunded
            .checked_add(pressure_ledger.first_cap_remaining as u64).unwrap();
        assert!(pressure_cap_end <= pressure_ledger.admitted);
        let mut pressure_cap_refunded = false;

        tokio::time::timeout(Duration::from_secs(if expect_peer_block { 30 } else { 10 }), async {
            tokio::select! {
                _ = async {
                    loop {
                        app.turn().await;
                        if !pressure_cap_refunded {
                            let (samples, overflow, ambiguous) = egress_bridge.egress_probe_samples();
                            assert!(!overflow && !ambiguous,
                                "pressure cap refund observation overflowed before first release");
                            if let Some(sample) = samples.iter().find(|sample|
                                sample.ledger.epoch == pressure_ledger.epoch
                                    && sample.ledger.origin == pressure_ledger.origin
                                    && sample.ledger.refunded > pressure_ledger.refunded) {
                                assert!(!sample.ledger.quarantined);
                                let first_cap_release = (sample.ledger.refunded - pressure_ledger.refunded)
                                    .min(pressure_ledger.first_cap_remaining as u64);
                                assert!(first_cap_release > 0);
                                assert!(queue.custodied_released_bytes_for_test().unwrap() as u64
                                    >= released_at_pressure as u64 + first_cap_release);
                                println!(
                                    "Permit pressure cap epoch={} origin={} range={}..{} refunded={} released_at_pressure={} released_now={}",
                                    pressure_ledger.epoch, pressure_ledger.origin,
                                    pressure_ledger.refunded, pressure_cap_end,
                                    sample.ledger.refunded,
                                    released_at_pressure,
                                    queue.custodied_released_bytes_for_test().unwrap(),
                                );
                                pressure_cap_refunded = true;
                            }
                        }
                        assert!(app.tcp_reply.len() <= reply_bytes);
                        let physical = queue.snapshot_now();
                        assert!(physical.owned_bytes() <= physical.capacity_bytes);
                        if app.tcp_reply.len() == reply_bytes
                            && physical.queued_bytes == 0 && physical.leased_bytes == 0 {
                            assert!(app.tcp_reply.iter().all(|byte| *byte == 0x5a));
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                } => {},
                report = service.run() => panic!("Native Permit Service ended before recovery: {report:?}"),
            }
        }).await.unwrap_or_else(|elapsed| {
            eprintln!("Native Permit recovery: app={} queue={:?} meter={:?} writer_finished={}",
                app.tcp_reply.len(), queue.snapshot_now(), meter.snapshot(), writer.as_ref().unwrap().is_finished());
            panic!("original Permit capacity recovery: {elapsed}");
        });
        assert!(pressure_cap_refunded,
            "original pressure cap had no observed refund before terminal recovery");
        if expect_peer_block {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            assert_eq!(owner.flow_native_pump_source_read_bytes(), Some(reply_bytes));
        }
        let mut send = writer.take().unwrap().await.unwrap();
        send.finish().unwrap();
        let mut recv = recv;
        recv.stop(78u32.into()).unwrap();
        drop((send, recv));
        assert!(saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap().ledger().first_failure.is_none());
    }).await;

    let scenario_failed = scenario.is_err();
    if scenario_failed {
        if let Some(writer) = writer.take() {
            writer.abort();
        }
    }
    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        if scenario_failed {
            fixture::observe_failed_native_stop(
                service.run(),
                &handle,
                original_deadline,
                original_queue.as_ref(),
            )
            .await;
            return;
        }
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        let owner = service.completion.as_ref().unwrap().control.clone();
        let flow = owner.flow_observation().unwrap();
        assert_eq!(
            (flow.live, flow.published, flow.released, flow.joined),
            (0, 1, 1, 5)
        );
        assert_eq!(flow.f, 0);
        assert!(flow.audit);
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined());
        assert!(original_queue.as_ref().unwrap().physical_zero());
        let physical = backing.as_ref().unwrap().snapshot();
        assert_eq!(
            (
                physical.allocations,
                physical.payload_bytes,
                physical.physical_bytes,
                physical.quarantined
            ),
            (0, 0, 0, false)
        );
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(panic) = scenario {
        if !matches!(drained, Ok(true)) {
            eprintln!("Native Permit failure cleanup: server driver did not drain: {drained:?}");
        }
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
    assert!(drained.unwrap());
}

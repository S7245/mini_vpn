//! Two real Native direct TCP flows through one Node Service and original Root.
use super::*;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride};
use fixture::{auth, capture, config, Apps};

#[tokio::test(flavor = "current_thread")]
async fn native_direct_two_simultaneous_service_flows_share_node_meter_and_join() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_chunk: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeChunkPump
    );
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    apps.enable_second_tcp();
    apps.sent_udp = true;
    let mut service = prepare(
        LoopbackTunDevice::new(inbound.clone(), outbound.clone()),
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let service_control = service.control.clone();
    let mut peer = None;
    let mut backing = None;
    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_traffic = async {
                        let mut seen = [false; 2];
                        let mut streams = Vec::new();
                        for _ in 0..2 {
                            let (mut send, mut recv) = remote.accept_bi().await.unwrap();
                            let header = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                            let mut actual = vec![0; header.len()];
                            recv.read_exact(&mut actual).await.unwrap();
                            assert_eq!(actual, header);
                            let mut payload = [0; 11];
                            recv.read_exact(&mut payload).await.unwrap();
                            let index = if &payload == b"service-tcp" { 0 } else if &payload == b"service-two" { 1 } else { panic!("unexpected TCP payload {payload:?}") };
                            assert!(!seen[index], "each app socket owns one stream");
                            seen[index] = true;
                            send.write_all(if index == 0 { b"reply-one" } else { b"reply-two" }).await.unwrap();
                            streams.push((send, recv));
                        }
                        assert_eq!(seen, [true; 2]);
                        streams
                    };
                    let app_traffic = async {
                        while apps.tcp_reply != b"reply-one" || apps.second_reply != b"reply-two" {
                            apps.turn().await;
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    let (streams, ()) = tokio::join!(remote_traffic, app_traffic);
                    let control = service_control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                    let observation = control.flow_observation().unwrap();
                    assert_eq!((observation.factory_started, observation.published, observation.live), (2, 2, 2));
                    assert_eq!((observation.f, observation.running, observation.joined), (2, 8, 0));
                    assert_eq!(control.flow_native_join_counts(), [0, 0]);
                    assert!(observation.audit);
                    let meters = control.flow_native_backing_meters();
                    assert_eq!(meters.len(), 2);
                    assert!(meters[0].same_authority(&meters[1]));
                    assert!(!meters[0].snapshot().quarantined);
                    backing = Some(meters[0].clone());
                    assert!(control.ledger().first_failure.is_none());
                    for (mut send, recv) in streams {
                        send.finish().unwrap();
                        drop((send, recv));
                    }
                    loop {
                        // Role JOIN is not the independent provider witness.
                        let physical = control.flow_observation().unwrap();
                        if physical.joined == 8 && physical.settled_providers == 2 { break; }
                        apps.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    let observation = control.flow_observation().unwrap();
                    assert_eq!((observation.live, observation.f, observation.published, observation.released, observation.joined), (2, 2, 2, 0, 8));
                    assert_eq!((observation.running, observation.settled_providers), (0, 2));
                    assert_eq!(control.flow_native_join_counts(), [4, 4]);
                    assert!(observation.audit);
                } => {},
                report = service.run() => panic!("service ended during simultaneous Native flows: {report:?}"),
            }
        }).await.unwrap_or_else(|elapsed| {
            let control = service_control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("dual path timeout flow={:?} first={:?} second={:?}", control.flow_observation(), apps.tcp_reply, apps.second_reply);
            panic!("dual path timeout: {elapsed}");
        });
    }).await;
    handle.request_stop();
    let original_deadline = service_control.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap();
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        let completion = service.completion.as_ref().unwrap();
        completion
            ._node
            .as_ref()
            .unwrap()
            .assert_service_clean_for_test();
        assert_eq!(
            *service_control.deadline.lock().unwrap(),
            Some(original_deadline)
        );
        let observation = completion.control.flow_observation().unwrap();
        assert_eq!(
            (
                observation.live,
                observation.running,
                observation.published,
                observation.released,
                observation.joined
            ),
            (0, 0, 2, 2, 8)
        );
        assert_eq!(
            (
                observation.w,
                observation.f,
                observation.reserved,
                observation.escrow_bytes
            ),
            (0, 0, 0, 0)
        );
        assert!(observation.audit);
        assert!(completion.control.ledger().first_failure.is_none());
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    scenario.unwrap_or_else(|error| std::panic::resume_unwind(error));
    cleanup.unwrap_or_else(|error| std::panic::resume_unwind(error));
    let physical = backing.expect("same original Node backing").snapshot();
    assert_eq!(
        (
            physical.allocations,
            physical.payload_bytes,
            physical.physical_bytes,
            physical.quarantined
        ),
        (0, 0, 0, false)
    );
    assert!(drained.unwrap());
    assert_eq!(inbound.dropped_packets(), 0);
    assert_eq!(outbound.dropped_packets(), 0);
}

//! Hard Native Direct two-Flow DATA through the actual factory, Node and Root.
use super::*;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride};
use fixture::{Apps, auth, capture, config};

// Preserve the prior DATA headroom after adding two real Native uplink lanes.
// Production layout: 1024 * 48 + 768 + 96 + 136 = 50_152 bytes per lane.
// Fixed, not derived from a peak or auto-raised when a layout grows.
// One measured Session IO cold role: 352B callback stock + 160B clock.
const PRESSURE_BUDGET: usize = (8 << 20) + 2 * 50_152 + 512;

#[tokio::test(flavor = "current_thread")]
async fn hard_native_direct_two_flows_share_original_budget_and_join() {
    direct_case(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_direct_two_roots_backing_full_recovers_only_from_actual_app_ack() {
    direct_case(true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_direct_permit_two_roots_backing_full_recovers_only_from_actual_app_ack() {
    two_flow_case(true, true, false).await;
}
async fn direct_case(pressure: bool) {
    two_flow_case(pressure, false, false).await;
}
pub(super) async fn two_flow_case(pressure: bool, permit: bool, ordered: bool) {
    let roles = if ordered { 5 } else { 4 };
    assert_eq!(egress::deadline::layout_bytes(), 512);
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_chunk: (!ordered).then_some("1"),
        tuic_native_ordered: ordered.then_some("1"),
        native_egress_permit: permit.then_some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        if ordered { TuicNativeOverride::NativeOrderedPump } else { TuicNativeOverride::NativeChunkPump }
    );
    let mut cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let mut runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy)
        .with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(if pressure { PRESSURE_BUDGET } else { 32 << 20 }), 64)
        .unwrap();
    runtime.tun_mtu = 1500;
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    if !pressure { apps.enable_second_tcp(); }
    apps.sent_udp = true;
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
    let service_control = service.control.clone();
    let budget = service_control.backing_budget.clone().unwrap();
    let app_meter = service_control.app_meter.clone().unwrap();
    let mut peer = None;
    let mut backing = None;
    let observed_peak = std::cell::Cell::new(budget.used());
    let sample = |phase: &str| {
        let weak = service_control.native_node_sample.lock().unwrap().as_ref().cloned()
            .expect("original Service must bind its actual Node Weak observer");
        let node = weak.sample().expect("original live Node endpoint must be observable");
        let used = budget.used();
        observed_peak.set(observed_peak.get().max(used));
        eprintln!("A1 baseline phase={phase} used={used} limit={} app={:?} node={:?} slots={} endpoint={:?} connections={:?} observed_peak={}",
            budget.limit(), app_meter.snapshot(), node.backing, node.slots, node.endpoint, node.connections, observed_peak.get());
        node
    };
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(if pressure { 25 } else { 5 }), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let auth_sample = sample("after_AUTH_before_App_turn");
                    assert_eq!(auth_sample.endpoint.budget_rejected_datagrams, 0);
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
                            // Keep actual App socket identity, independent of QUIC accept order.
                            if index == 0 { streams.insert(0, (send, recv)); }
                            else { streams.push((send, recv)); }
                        }
                        assert_eq!(seen, [true; 2]);
                        streams
                    };
                    let mut first_pressure_id = None;
                    let app_traffic = async {
                        while apps.tcp_reply != b"reply-one" || apps.second_reply != b"reply-two" {
                            apps.turn().await;
                            if pressure && first_pressure_id.is_none() && apps.tcp_reply == b"reply-one" {
                                let owner = service_control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                                let contracts = owner.flow_contracts();
                                assert_eq!(contracts.len(), 1, "first App alone binds the first original Flow identity");
                                first_pressure_id = Some(contracts[0].id);
                                apps.enable_second_tcp();
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    let (mut streams, ()) = tokio::join!(remote_traffic, app_traffic);
                    let control = service_control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                    let observation = control.flow_observation().unwrap();
                    assert_eq!((observation.factory_started, observation.published, observation.live), (2, 2, 2));
                    assert_eq!((observation.f, observation.running, observation.joined), (2, 2 * roles, 0));
                    assert_eq!(control.flow_native_join_counts(), [0, 0]);
                    assert!(observation.audit);
                    assert_eq!(control.flow_native_permit_queue().is_some(), permit);
                    let meters = control.flow_native_backing_meters();
                    assert_eq!(meters.len(), 2);
                    assert!(meters[0].same_authority(&meters[1]));
                    assert!(!meters[0].snapshot().quarantined);
                    let node_sample = sample("F2_after_real_DATA");
                    assert_eq!(node_sample.backing, meters[0].snapshot());
                    let live: Vec<_> = node_sample.connections.iter().flatten().filter(|c| c.stats.is_some()).collect();
                    assert_eq!(live.len(), 1);
                    assert!(live[0].published);
                    assert_eq!(live[0].registered_id, live[0].actual_id);
                    assert_eq!(node_sample.endpoint.budget_rejected_datagrams, 0);
                    backing = Some(meters[0].clone());
                    if pressure {
                        let first = first_pressure_id.unwrap();
                        let contracts = control.flow_contracts();
                        assert_eq!(contracts.len(), 2);
                        let second = contracts.iter().find(|c| c.id != first).unwrap().id;
                        assert_ne!(first, second);
                        eprintln!("[N1-A1-binding] actual App40000/stream0 Root={first:?}; App40004/stream1 Root={second:?}");
                        backing_pressure::exchange(&mut streams, &mut apps, &service_control, &outbound).await;
                    }
                    assert!(control.ledger().first_failure.is_none());
                    for (mut send, recv) in streams {
                        send.finish().unwrap();
                        drop((send, recv));
                    }
                    loop {
                        // Role JOIN is not the independent provider witness.
                        let physical = control.flow_observation().unwrap();
                        if physical.joined == (2 * roles) as u64 && physical.settled_providers == 2 { break; }
                        apps.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    let observation = control.flow_observation().unwrap();
                    assert_eq!((observation.live, observation.f, observation.published, observation.released, observation.joined), (2, 2, 2, 0, (2 * roles) as u64));
                    assert_eq!((observation.running, observation.settled_providers), (0, 2));
                    assert_eq!(control.flow_native_join_counts(), [roles, roles]);
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
        if scenario.is_err() {
            eprintln!("[N1-direct-permit-peer-first] reason={:?}", peer.as_ref().and_then(quinn::Connection::close_reason));
            fixture::observe_failed_native_stop(service.run(), &handle, original_deadline, None).await;
            if pressure {
                // Existing absolute deadline stays frozen; this is only the
                // already accepted bounded watchdog for observing final Q.
                let report = tokio::time::timeout_at(original_deadline + Duration::from_secs(5), service.run()).await;
                let owner = service_control.session.lock().unwrap().as_ref().and_then(std::sync::Weak::upgrade);
                eprintln!("[N1-A1-failure-stop] report={report:?} original_Service_deadline={original_deadline:?} actual_deadline={:?} used={} Q={} app={:?} first={:?}",
                    *service_control.deadline.lock().unwrap(), budget.used(), budget.quarantined(),
                    app_meter.snapshot(), owner.as_ref().and_then(|owner| fixture::first_failure_identity(owner)));
                if let Some(owner) = owner {
                    for contract in owner.flow_contracts() {
                        eprintln!("[N1-A1-failure-Flow] contract={contract:?} original_stop={:?} terminal={:?}",
                            owner.flow_original_stop_for_test(contract.id), owner.flow_native_retained_terminal_for_test(contract.id));
                    }
                }
            }
            return;
        }
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
            (0, 0, 2, 2, (2 * roles) as u64)
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
    drop((apps, inbound, outbound));
    assert_eq!(app_meter.snapshot().physical_bytes, 0);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    eprintln!(
        "hard twoFlow permit={permit} ordered={ordered}: original shared meter and {}JOIN, Node/App/budget0nonQ deadline={original_deadline:?}", 2 * roles
    );
}

mod backing_pressure;

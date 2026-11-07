//! Real Device → original adapter → TUIC transport under pre-encoding Full.
use super::*;
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_service_udp_uplink_full_drops_before_unfunded_packet_encoding() {
    let identity = PublicCa::new();
    let server = quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_chunk: Some("1"), ..Default::default()
    }));
    let mut cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap()
        .with_relay_policy(policy).with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(32 << 20), 64).unwrap();
    runtime.tun_mtu = 1500;
    let inbound = PacketLink::bounded(64);
    let feed = inbound.clone();
    let mut service = prepare_factory(move |meter: Option<bytes::BackingMeter>| async move {
        Ok::<_, ClientError>(LoopbackTunDevice::native_with_factory(64, 1500, &meter.unwrap(), || {
            Ok((inbound, PacketLink::bounded(64)))
        })?)
    }, Configuration { tuic: cfg.clone(), composition: () }, runtime, Arc::new(Metrics::default()), NoopSink);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().unwrap();
    let meter = saved.app_meter.clone().unwrap();
    let mut peer = None;
    let mut observed = None;
    let mut leaked = false;
    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote);
                    while handle.phase() != Phase::Running { tokio::task::yield_now().await; }
                } => {},
                report = service.run() => panic!("original Service ended before UDP: {report:?}"),
            }
        }).await.unwrap();
        let probe = crate::owned_upstream::LegacyOwnedUpstream::<crate::tuic::TuicUpstream>::arm_udp_budget_probe(budget.clone(), meter.clone());
        let payload = b"original-device-udp-uplink";
        let builder = etherparse::PacketBuilder::ipv4([10,0,0,1], [192,0,2,10], 64).udp(40001,443);
        let mut frame = Vec::with_capacity(builder.size(payload.len()));
        builder.write(&mut frame, payload).unwrap();
        assert!(feed.push(bytes::BytesMut::from(frame.as_slice())));
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async { while !probe.observed().done { tokio::task::yield_now().await; } } => {},
                report = service.run() => panic!("original Service ended during UDP: {report:?}"),
            }
        }).await.unwrap();
        let observation = probe.observed();
        eprintln!("[N1-real-UDP-uplink-Full] {observation:?} Q={}", budget.quarantined());
        observed = Some(observation);
        if let Ok(Ok(data)) = tokio::time::timeout(Duration::from_millis(100), peer.as_ref().unwrap().read_datagram()).await {
            assert_eq!(crate::tuic::decode_packet_meta(&data).unwrap().data, payload);
            leaked = true;
        }
        // Reuse the real original Device and association after actual pressure Drop.
        assert!(feed.push(bytes::BytesMut::from(frame.as_slice())));
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                data = peer.as_ref().unwrap().read_datagram() => {
                    assert_eq!(crate::tuic::decode_packet_meta(&data.unwrap()).unwrap().data, payload);
                },
                report = service.run() => panic!("original Service ended before UDP retry: {report:?}"),
            }
        }).await.unwrap();
    }).await;
    handle.request_stop();
    let deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let report = tokio::time::timeout_at(deadline, service.run()).await.unwrap();
        assert!(report.device_settled());
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
        assert_eq!(*saved.deadline.lock().unwrap(), Some(deadline));
    }).await;
    drop((service, handle, peer));
    server.close(0u32.into(), b"UDP uplink test complete");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle()).await.unwrap();
    scenario.unwrap_or_else(|p| std::panic::resume_unwind(p));
    cleanup.unwrap_or_else(|p| std::panic::resume_unwind(p));
    let observation = observed.unwrap();
    assert!(observation.full);
    assert_eq!(observation.drops_after - observation.drops_before, 1,
        "the original UDP pressure policy must reject unfunded output before encoding");
    assert!(!leaked, "Full must not publish uncharged TUIC payload to the real peer");
    assert_eq!(observation.encoded_capacity, 0);
}

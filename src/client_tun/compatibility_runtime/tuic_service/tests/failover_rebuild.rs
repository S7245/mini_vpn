//! Actual Failover cold composition, original health/source custody and one
//! controlled successor. No leg setter or replacement health clock is used.
use super::*;
use crate::reality_upstream::{RealityClientConfig, RealityUpstream};
use device::{Gate, GatedDevice};
use fixture::{auth, capture, config, Apps};
use std::sync::atomic::Ordering;

pub(super) async fn retire(streams: &mut Option<(quinn::SendStream, quinn::RecvStream)>) {
    if let Some((mut tx, mut rx)) = streams.take() {
        let code = tx.stopped().await.unwrap().expect("actual STOP");
        assert!(rx.read_to_end(0).await.unwrap().is_empty());
        tx.reset(code).unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn failover_rebuild_joins_original_health_before_unique_successor_tcp_udp() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let backup = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (_, pk) = crate::reality::auth::generate_ephemeral_keypair();
    let reality = RealityUpstream::new(RealityClientConfig {
        server: backup.local_addr().unwrap().to_string(),
        uuid: [0x42; 16],
        pbk: pk,
        short_id: [0; 8],
        sni: "example.com".into(),
    });
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let gate = Arc::new(Gate::default());
    gate.cut_enabled.store(true, Ordering::Release);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let mut service = prepare_with(
        GatedDevice {
            inner: LoopbackTunDevice::new(inbound.clone(), outbound.clone()).with_ingress_pump(4),
            gate: gate.clone(),
        },
        Configuration::failover(cfg.clone(), reality),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peers: Vec<quinn::Connection> = Vec::with_capacity(2);
    let mut streams = None;
    let mut old: Option<Arc<SessionControl>> = None;
    let mut old_health: Option<Arc<crate::failover::HealthObservation>> = None;
    let mut old_generation: Option<Arc<crate::owned_upstream::generation::GenerationControl>> =
        None;
    let mut policy = None;
    let scenario = capture(async {
        for round in 0..2 {
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                tokio::select! {
                    _ = async {
                        let incoming = server.accept().await.unwrap();
                        if round == 1 {
                            // The original second Incoming exists, but no second
                            // handshake/AUTH/DATA can mask late old retirement.
                            let old = old.as_ref().unwrap();
                            let health = old_health.as_ref().unwrap();
                            let source = old_generation.as_ref().unwrap().diagnostic();
                            assert!(health.joined.load(Ordering::Acquire));
                            assert!(!health.quarantined.load(Ordering::Acquire));
                            assert_eq!(source.0, 2);
                            assert_eq!(source.4, 0);
                            assert!(old.business_complete.load(Ordering::Acquire));
                            assert!(!old.driver_complete.load(Ordering::Acquire));
                            let settled = old.flow_observation().unwrap();
                            assert_eq!((settled.live, settled.running, settled.f, settled.w, settled.released, settled.joined), (0, 0, 0, 0, 1, 3));
                            assert!(settled.audit);
                            assert!(peers[0].close_reason().is_some());
                            assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0));
                            eprintln!("B22 BEFORE second handshake: actual oldhealth joined=true, source={source:?}, business settled, Djoin/drop=0/0");
                        }
                        let peer = incoming.await.unwrap();
                        auth(&peer, &cfg).await;
                        peers.push(peer.clone());
                        if round == 1 { apps.restart(); }
                        let tcp = async {
                            let (mut tx, mut rx) = peer.accept_bi().await.unwrap();
                            let target = TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap());
                            let mut want = encode_connect(&target);
                            want.extend_from_slice(b"service-tcp");
                            let mut actual = vec![0; want.len()];
                            rx.read_exact(&mut actual).await.unwrap();
                            assert_eq!(actual, want);
                            tx.write_all(b"tcp-reply").await.unwrap();
                            streams = Some((tx, rx));
                        };
                        let udp = async {
                            let actual = peer.read_datagram().await.unwrap();
                            let assoc = u16::from_be_bytes([actual[2], actual[3]]);
                            let target = TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());
                            assert_eq!(&actual[..], encode_packet(assoc, &target, b"service-udp"));
                            peer.send_datagram(encode_packet(assoc, &target, b"udp-reply").into()).unwrap();
                        };
                        let local = async {
                            while apps.tcp_reply != b"tcp-reply" || apps.udp_reply != b"udp-reply" {
                                apps.turn().await;
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                        };
                        tokio::join!(tcp, udp, local);
                    } => {},
                    report = service.run() => panic!("service ended before DATA {report:?}"),
                }
            }).await.unwrap();
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let health = saved.health_observation.lock().unwrap().as_ref().unwrap().clone();
            let generation = saved.udp_generation.lock().unwrap().as_ref().unwrap().clone();
            let state = saved.health_state.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            assert_eq!(*saved.phase.lock().unwrap(), Phase::Running);
            assert!(!health.joined.load(Ordering::Acquire));
            assert!(!health.quarantined.load(Ordering::Acquire));
            let live = owner.flow_observation().unwrap();
            assert_eq!((live.live, live.f, live.running, live.published), (1, 1, 3, 1));
            if round == 0 {
                old = Some(owner);
                old_health = Some(health);
                old_generation = Some(generation);
                policy = Some(state);
                assert!(saved.deadline.lock().unwrap().is_none());
                let accepted = handle.request_rebuild();
                eprintln!("B22 actual AUTH/CONNECT TCP+typedUDP succeeded; request_rebuild={accepted}; phase={:?}; parent_deadline={:?}; health_joined={}; old_live={}", *saved.phase.lock().unwrap(), *saved.deadline.lock().unwrap(), old_health.as_ref().unwrap().joined.load(Ordering::Acquire), live.live);
                assert!(accepted, "real Failover public controlled rebuild must be accepted");
                crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                    tokio::select! {
                        () = retire(&mut streams) => (),
                        report = service.run() => panic!("ended during retirement {report:?}"),
                    }
                }).await.unwrap();
            } else {
                let previous = old.as_ref().unwrap();
                assert!(!Arc::ptr_eq(previous, &owner));
                assert!(Arc::ptr_eq(policy.as_ref().unwrap(), &state));
                assert!(old_health.as_ref().unwrap().joined.load(Ordering::Acquire));
                assert!(!old_health.as_ref().unwrap().quarantined.load(Ordering::Acquire));
                assert!(!Arc::ptr_eq(old_health.as_ref().unwrap(), &health));
                assert_eq!(old_generation.as_ref().unwrap().diagnostic().0, 2);
                assert!(!Arc::ptr_eq(old_generation.as_ref().unwrap(), &generation));
                assert!(previous.business_complete.load(Ordering::Acquire));
                assert!(!previous.driver_complete.load(Ordering::Acquire));
                let settled = previous.flow_observation().unwrap();
                assert_eq!((settled.live, settled.running, settled.released, settled.joined), (0, 0, 1, 3));
                assert!(settled.audit);
                assert_eq!((gate.cuts.load(Ordering::Acquire), gate.resumed.load(Ordering::Acquire)), (1, 1));
                assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0));
                assert!(peers[0].close_reason().is_some());
                assert!(!handle.request_rebuild(), "one successor budget");
                let clocks = saved.namespace_clocks.lock().unwrap();
                assert!(clocks[0].is_some());
                assert_eq!(clocks[0], clocks[1]);
            }
        }
    }).await;
    // These original owners outlive the scenario. One remote retirement and the
    // same original service run to the parent's first15, even after a RED.
    handle.request_stop();
    gate.open();
    let deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        let (report, ()) = tokio::time::timeout_at(deadline, async {
            tokio::join!(service.run(), retire(&mut streams))
        }).await.unwrap();
        eprintln!("B22 actual cleanup {report:?}; phase={:?}; device_join/drop={}/{}; old_health_join={:?}; old_source={:?}", *saved.phase.lock().unwrap(), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire), old_health.as_ref().map(|h| h.joined.load(Ordering::Acquire)), old_generation.as_ref().map(|g| g.diagnostic()));
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!(report.rebuilds, u8::from(scenario.is_ok()));
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 1));
        assert!(old_health.as_ref().unwrap().joined.load(Ordering::Acquire));
        assert_eq!(old_generation.as_ref().unwrap().diagnostic().0, 2);
        let completion = service.completion.as_ref().unwrap();
        assert!(completion._health.as_ref().unwrap().failure().is_none());
        completion._node.as_ref().unwrap().assert_service_clean_for_test();
        if scenario.is_ok() { completion._retired.as_ref().unwrap().node.assert_service_clean_for_test(); }
        let current = saved.health_observation.lock().unwrap().as_ref().unwrap().clone();
        assert!(current.joined.load(Ordering::Acquire));
        let o = completion.control.flow_observation().unwrap();
        assert_eq!((o.live, o.running, o.f, o.w, o.reserved, o.escrow_bytes), (0, 0, 0, 0, 0, 0));
        assert_eq!((o.released, o.joined), (1, 3));
        assert!(o.audit);
        assert_eq!((inbound.dropped_packets(), outbound.dropped_packets()), (0, 0));
    }).await;
    if cleanup.is_err() {
        eprintln!(
            "B22 secondary cleanup assertion failed; original scenario error retains priority"
        );
    }
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}

//! A real entered health Probe survives cancelled run borrows during clean
//! rebuild. No old-session DATA: closing an active carrier would be real failure.
//! Owns true peers/apps only; the saved production root supplies all receipts.
use super::*;
use crate::reality_upstream::{RealityClientConfig, RealityUpstream};
use device::{Gate, GatedDevice};
use failover_successor::{drive, traffic, Streams};
use fixture::{auth, capture, config, Apps};
use std::sync::atomic::Ordering;

fn deadline(control: &Control) -> Option<tokio::time::Instant> {
    *control
        .deadline
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn owner_deadline(owner: &SessionControl) -> Option<tokio::time::Instant> {
    owner.ledger().shutdown_deadline
}
async fn until<F: Future<Output = Completion>>(
    service: &mut Service<F>,
    budget: Duration,
    ready: impl Fn() -> bool,
) {
    tokio::time::timeout(budget, async {
        tokio::select! {
            biased;
            () = async { while !ready() { tokio::time::sleep(Duration::from_millis(1)).await; } } => (),
            report = service.run() => panic!("inflight service ended before witness {report:?}"),
        }
    }).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn original_inflight_probe_joins_before_clean_unique_successor_after_cancelled_run() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sk, pk) = crate::reality::auth::generate_ephemeral_keypair();
    let configuration = Configuration::failover(
        cfg.clone(),
        RealityUpstream::new(RealityClientConfig {
            server: listener.local_addr().unwrap().to_string(),
            uuid: [0x42; 16],
            pbk: pk,
            short_id: [0; 8],
            sni: "example.com".into(),
        }),
    );
    let original_state = configuration.composition.state.clone();
    let original_backup = configuration.composition.backup.clone();
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let gate = Arc::new(Gate::default());
    gate.cut_enabled.store(true, Ordering::Release);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    apps.tcp_payload = b"ping";
    let original_udp = apps.udp; // Remains unpolled throughout the old session.
    let mut service = prepare_with(
        GatedDevice {
            inner: LoopbackTunDevice::new(inbound.clone(), outbound.clone()).with_ingress_pump(4),
            gate: gate.clone(),
        },
        configuration,
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peers: Vec<quinn::Connection> = Vec::with_capacity(3);
    let mut incoming: Option<quinn::Incoming> = None;
    let mut old: Option<Arc<SessionControl>> = None;
    let mut old_health: Option<Arc<crate::failover::HealthObservation>> = None;
    let mut old_source: Option<Arc<crate::owned_upstream::generation::GenerationControl>> = None;
    let mut original_limit = None;
    let mut current: Option<Arc<SessionControl>> = None;
    let mut streams: Streams = None;
    let mut original_now = 0;
    let scenario = capture(async {
        drive(&mut service, async {
            let peer = server.accept().await.unwrap().await.unwrap(); peers.push(peer);
            auth(&peers[0], &cfg).await;
        }).await;
        until(&mut service, Duration::from_secs(2), || saved.health_observation.lock().unwrap().is_some()).await;
        let health = saved.health_observation.lock().unwrap().as_ref().unwrap().clone();
        old_health = Some(health.clone());
        until(&mut service, Duration::from_secs(2), || health.ticks.load(Ordering::Acquire) >= 1).await;
        let state = saved.health_state.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert!(Arc::ptr_eq(&state, &original_state));
        state.set_leg(crate::failover::TcpLeg::Reality); // Selection injection, not natural hysteresis.
        original_now = state.now_secs();
        until(&mut service, Duration::from_secs(29), || health.ticks.load(Ordering::Acquire) >= 10).await;
        assert_eq!(health.probes_started.load(Ordering::Acquire), 0);
        peers[0].close(17u32.into(), b"old primary lost before health tick, no old DATA");
        until(&mut service, Duration::from_secs(5), || health.probes_started.load(Ordering::Acquire) == 1).await;
        incoming = Some(drive(&mut service, async { server.accept().await.unwrap() }).await);
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let source = saved.udp_generation.lock().unwrap().as_ref().unwrap().clone();
        old = Some(owner.clone()); old_source = Some(source.clone());
        assert!(handle.request_rebuild());
        until(&mut service, Duration::from_secs(2), || handle.phase() == Phase::Rebuilding && owner_deadline(&owner).is_some()).await;
        original_limit = owner_deadline(&owner);
        assert!(original_limit.is_some() && deadline(&saved).is_none());
        // Do NOT poll observe_shutdown here: it requests an external stop.
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
            assert_eq!(handle.phase(), Phase::Rebuilding);
            assert_eq!(owner_deadline(&owner), original_limit);
            assert!(deadline(&saved).is_none() && incoming.is_some());
            assert_eq!((health.probes_started.load(Ordering::Acquire), health.probes_finished.load(Ordering::Acquire)), (1, 0));
            assert!(!health.joined.load(Ordering::Acquire));
            let observed_health = saved.health_observation.lock().unwrap().as_ref().unwrap().clone();
            let observed_source = saved.udp_generation.lock().unwrap().as_ref().unwrap().clone();
            assert!(Arc::ptr_eq(&health, &observed_health) && Arc::ptr_eq(&source, &observed_source));
            assert_eq!(source.diagnostic().0, 0);
            assert!(!owner.business_complete.load(Ordering::Acquire));
            assert!(!owner.driver_complete.load(Ordering::Acquire));
            assert_eq!((gate.cuts.load(Ordering::Acquire), gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0, 0, 0));
            assert!(!handle.request_rebuild());
        }
        drive(&mut service, async {
            assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
        }).await;
        eprintln!("B26 enteredretirement originalProbe started1/finished0/joinfalse source={:?} businessfalse/cutD0 limit={original_limit:?}, parentNone", source.diagnostic());
        // Complete the SAME original recovery Incoming; this is not session 2.
        drive(&mut service, async {
            let peer = incoming.take().unwrap().await.unwrap(); assert!(peers.len() < 3); peers.push(peer);
            auth(&peers[1], &cfg).await;
        }).await;
        until(&mut service, Duration::from_secs(3), || health.joined.load(Ordering::Acquire) && source.diagnostic().0 == 2 && owner.business_complete.load(Ordering::Acquire)).await;
        assert_eq!((health.probes_started.load(Ordering::Acquire), health.probes_finished.load(Ordering::Acquire), health.probes_ok.load(Ordering::Acquire)), (1, 1, 1));
        assert!(!health.quarantined.load(Ordering::Acquire));
        let generation = source.diagnostic(); assert_eq!((generation.0, generation.1, generation.4), (2, 2, 0));
        let settled = owner.flow_observation().unwrap();
        assert_eq!((settled.live, settled.running, settled.f, settled.w, settled.reserved, settled.escrow_bytes, settled.factory_started), (0, 0, 0, 0, 0, 0, 0));
        assert!(settled.audit && owner.ledger().first_failure.is_none());
        let successor = drive(&mut service, async { server.accept().await.unwrap() }).await;
        // Actual old Node gate must precede this sole successor's handshake.
        for peer in &peers {
            tokio::time::timeout(Duration::from_secs(2), peer.closed()).await.unwrap();
        }
        assert_eq!((gate.cuts.load(Ordering::Acquire), gate.resumed.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 0, 0, 0));
        assert_eq!(owner_deadline(&owner), original_limit);
        assert!(deadline(&saved).is_none());
        eprintln!("B26 before sole successor handshake: actual originalhealth1/1/1join source={generation:?}/businessallzero/old2peersclosed/cut1/D0");
        drive(&mut service, async {
            let peer = successor.await.unwrap(); assert!(peers.len() < 3); peers.push(peer);
            auth(&peers[2], &cfg).await;
        }).await;
        traffic(&mut service, &mut apps, &peers[2], &mut streams, Some((&listener, sk))).await;
        assert!(streams.is_none());
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let health = saved.health_observation.lock().unwrap().as_ref().unwrap().clone();
        let state = saved.health_state.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert!(!Arc::ptr_eq(old.as_ref().unwrap(), &owner)); current = Some(owner);
        assert!(!Arc::ptr_eq(old_health.as_ref().unwrap(), &health));
        assert!(!health.joined.load(Ordering::Acquire));
        assert!(Arc::ptr_eq(&original_state, &state));
        assert_eq!(state.active_leg(), crate::failover::TcpLeg::Reality);
        assert!(state.now_secs() >= original_now && Arc::strong_count(&original_backup) >= 2);
        let clocks = *saved.namespace_clocks.lock().unwrap(); assert!(clocks[0].is_some()); assert_eq!(clocks[0], clocks[1]);
        assert_eq!(apps.udp, original_udp);
        assert_eq!((gate.cuts.load(Ordering::Acquire), gate.resumed.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 1, 0, 0));
        assert!(deadline(&saved).is_none());
        assert_eq!(handle.phase(), Phase::Running);
        assert!(!handle.request_rebuild());
        assert_eq!(peers.len(), 3);
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
    }).await;
    handle.request_stop();
    gate.open();
    let final_limit = deadline(&saved).unwrap();
    let mut actual_report = None;
    let cleanup = capture(async {
        let finish_original = async {
            if let Some(value) = incoming.take() {
                let peer = value.await.unwrap();
                assert!(peers.len() < 3);
                peers.push(peer);
                auth(peers.last().unwrap(), &cfg).await;
            }
        };
        // First stop observer is polled only here. Original Probe work, if held
        // after a scenario failure, still finishes inside the same saved root.
        let (report, (), phase) = tokio::time::timeout_at(final_limit, async {
            tokio::join!(service.run(), finish_original, handle.observe_shutdown())
        })
        .await
        .unwrap();
        actual_report = Some(report);
        eprintln!(
            "B26 actualfinal={report:?} phase={phase:?} Djoin/drop={}/{}",
            gate.joins.load(Ordering::Acquire),
            gate.drops.load(Ordering::Acquire)
        );
        assert_eq!(phase, Phase::Complete);
        assert!(report.device_settled() && !report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!(
            (
                gate.joins.load(Ordering::Acquire),
                gate.drops.load(Ordering::Acquire)
            ),
            (1, 1)
        );
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
            let done = service.completion.as_ref().unwrap();
            let retired = done._retired.as_ref().unwrap();
            retired.node.assert_service_clean_for_test();
            done._node.as_ref().unwrap().assert_service_clean_for_test();
            assert!(
                Arc::ptr_eq(&retired.control, old.as_ref().unwrap())
                    && Arc::ptr_eq(&done.control, current.as_ref().unwrap())
            );
            assert!(old_health.as_ref().unwrap().joined.load(Ordering::Acquire));
            assert_eq!(old_source.as_ref().unwrap().diagnostic().0, 2);
            assert_eq!(owner_deadline(old.as_ref().unwrap()), original_limit);
            assert!(done._health.as_ref().unwrap().failure().is_none());
            assert!(saved
                .health_observation
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .joined
                .load(Ordering::Acquire));
            let empty = done.control.flow_observation().unwrap();
            assert_eq!(
                (
                    empty.live,
                    empty.running,
                    empty.f,
                    empty.w,
                    empty.released,
                    empty.joined
                ),
                (0, 0, 0, 0, 1, 3)
            );
            assert!(empty.audit);
        }
        assert_eq!(
            (inbound.dropped_packets(), outbound.dropped_packets()),
            (0, 0)
        );
        assert!(!handle.request_rebuild());
    })
    .await;
    if cleanup.is_err() {
        eprintln!(
            "B26 secondary cleanup failure actual_report={actual_report:?}; original scenario retains priority"
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

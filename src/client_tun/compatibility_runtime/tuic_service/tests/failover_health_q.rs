//! Controlled cancellation of the ORIGINAL entered health actor during actual
//! retirement. Owns test fault capability/peers only; never supplies a join proof.
//! Q retains original Node/error custody after real device settlement.
use super::*;
use crate::reality_upstream::{RealityClientConfig, RealityUpstream};
use device::{device, Gate};
use failover_successor::drive;
use fixture::{auth, capture, config};
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FailureSnapshot {
    address: usize,
    additional: u64,
    kind: Option<std::io::ErrorKind>,
    raw: Option<i32>,
    opaque: bool,
}
fn failure(owner: &SessionControl) -> FailureSnapshot {
    let ledger = owner.ledger();
    let error = ledger.first_failure.as_ref().unwrap();
    let facts = error.facts();
    FailureSnapshot {
        address: error as *const RetainedFailure as usize,
        additional: ledger.additional_failures,
        kind: facts.io_kind,
        raw: facts.raw_os_code,
        opaque: facts.opaque,
    }
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
            report = service.run() => panic!("healthQ service returned instead of retaining Q {report:?}"),
        }
    }).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn original_actor_join_error_during_retirement_keeps_node_q_after_real_device_join() {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let backup = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (_, pk) = crate::reality::auth::generate_ephemeral_keypair();
    let gate = Arc::new(Gate::default());
    gate.cut_enabled.store(true, Ordering::Release);
    let mut service = prepare_with(
        device(gate.clone()),
        Configuration::failover(
            cfg.clone(),
            RealityUpstream::new(RealityClientConfig {
                server: backup.local_addr().unwrap().to_string(),
                uuid: [0x42; 16],
                pbk: pk,
                short_id: [0; 8],
                sni: "example.com".into(),
            }),
        ),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut peers: Vec<quinn::Connection> = Vec::with_capacity(1);
    let mut incoming: Option<quinn::Incoming> = None;
    let mut retained_owner: Option<Arc<SessionControl>> = None;
    let mut retained_health: Option<Arc<crate::failover::HealthObservation>> = None;
    let mut retained_source: Option<Arc<crate::owned_upstream::generation::GenerationControl>> =
        None;
    let mut original_limit = None;
    let scenario = capture(async {
        drive(&mut service, async {
            let peer = server.accept().await.unwrap().await.unwrap(); peers.push(peer);
            auth(&peers[0], &cfg).await;
        }).await;
        until(&mut service, Duration::from_secs(2), || saved.health_observation.lock().unwrap().is_some()).await;
        let health = saved.health_observation.lock().unwrap().as_ref().unwrap().clone(); retained_health = Some(health.clone());
        until(&mut service, Duration::from_secs(2), || health.ticks.load(Ordering::Acquire) >= 1).await;
        let state = saved.health_state.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        state.set_leg(crate::failover::TcpLeg::Reality); // Expose unchanged real30s Probe, selection only.
        until(&mut service, Duration::from_secs(29), || health.ticks.load(Ordering::Acquire) >= 10).await;
        assert_eq!(health.probes_started.load(Ordering::Acquire), 0);
        peers[0].close(17u32.into(), b"closed primary, no old DATA");
        until(&mut service, Duration::from_secs(5), || health.probes_started.load(Ordering::Acquire) == 1).await;
        incoming = Some(drive(&mut service, async { server.accept().await.unwrap() }).await);
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap(); retained_owner = Some(owner.clone());
        let source = saved.udp_generation.lock().unwrap().as_ref().unwrap().clone(); retained_source = Some(source.clone());
        assert!(handle.request_rebuild());
        until(&mut service, Duration::from_secs(2), || handle.phase() == Phase::Rebuilding && owner_deadline(&owner).is_some()).await;
        original_limit = owner_deadline(&owner);
        assert!(original_limit.is_some() && deadline(&saved).is_none());
        assert_eq!((health.probes_started.load(Ordering::Acquire), health.probes_finished.load(Ordering::Acquire)), (1, 0));
        assert!(!health.joined.load(Ordering::Acquire)); assert!(health.failure_for_test().is_none());
        assert_eq!(source.diagnostic().0, 0);
        assert!(!owner.business_complete.load(Ordering::Acquire));
        // Explicit fault only AFTER real retirement entered. Abort is not join.
        let original_task = health.abort_original_for_test().unwrap();
        assert!(health.abort_original_for_test().is_none(), "one fault capability only");
        assert!(!health.joined.load(Ordering::Acquire));
        assert!(health.failure_for_test().is_none());
        until(&mut service, Duration::from_secs(3), || health.joined.load(Ordering::Acquire) && source.diagnostic().0 == 2 && owner.business_complete.load(Ordering::Acquire) && gate.polls.load(Ordering::Acquire) > 0).await;
        assert!(health.quarantined.load(Ordering::Acquire));
        let joined = health.failure_for_test().unwrap();
        assert_eq!(joined.task, original_task); assert!(joined.cancelled && !joined.panicked && joined.original_storage != 0);
        assert_eq!((health.probes_started.load(Ordering::Acquire), health.probes_finished.load(Ordering::Acquire), health.probes_ok.load(Ordering::Acquire)), (1, 0, 0));
        let original_failure = failure(&owner); assert_eq!(original_failure.additional, 0);
        assert!(original_failure.address != 0);
        assert_eq!(original_failure.kind, Some(std::io::ErrorKind::Other));
        assert!(original_failure.opaque && original_failure.raw.is_none());
        let generation = source.diagnostic(); assert_eq!(generation.0, 2);
        let empty = owner.flow_observation().unwrap();
        assert_eq!((empty.live, empty.running, empty.f, empty.w, empty.reserved, empty.escrow_bytes, empty.factory_started), (0, 0, 0, 0, 0, 0, 0)); assert!(empty.audit);
        assert_eq!(deadline(&saved), original_limit);
        assert_eq!((gate.cuts.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0, 0));
        assert!(!owner.driver_complete.load(Ordering::Acquire));
        assert_eq!(handle.phase(), Phase::Quarantined);
        assert!(service.completion.is_none() && incoming.is_some());
        assert!(!handle.request_rebuild());
        eprintln!("B27 realretiring actor={original_task:?} originalJoinErrorSnapshot={joined:?} source={generation:?} firstVALUE={original_failure:?} DgatePending Q/noCut/noCompletion");
        gate.open();
        until(&mut service, Duration::from_secs(3), || owner.driver_complete.load(Ordering::Acquire)).await;
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 1));
        let mut observer = Box::pin(handle.observe_shutdown());
        for _ in 0..4 {
            assert_eq!(observer.as_mut().await, Phase::Quarantined);
            // Completed observer cannot be repolled. Each later observation is
            // separate; the SAME saved driver and its original limit remain.
            observer = Box::pin(handle.observe_shutdown());
            assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
            assert_eq!(deadline(&saved), original_limit);
            assert_eq!(owner_deadline(&owner), original_limit);
            assert_eq!(health.failure_for_test(), Some(joined)); // Stable one-time snapshot only.
            assert_eq!(failure(&owner), original_failure);
            assert_eq!((gate.cuts.load(Ordering::Acquire), gate.resumes.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0, 1, 1));
            assert!(service.completion.is_none() && incoming.is_some());
            assert!(!handle.request_rebuild());
        }
        drive(&mut service, async { assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err()); }).await;
        let clocks = *saved.namespace_clocks.lock().unwrap(); assert!(clocks[0].is_some()); assert!(clocks[1].is_none());
        eprintln!("B27 actualDjoinDrop1/drivercomplete but originalNode+HealthTask/JoinError retainedQ, source={:?}, limits={original_limit:?}, no successor/cut/Completion; Nodephysicalcleanup NOTclaimed", source.diagnostic());
    }).await;
    // The retained original JoinError/Node make clean completion forbidden.
    // Releasing test-held Incoming and destroying Q below is forced, not a join.
    handle.request_stop();
    gate.open();
    drop(incoming);
    drop(service);
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    eprintln!(
        "B27 forcedfinally devicejoin/drop={}/{} retainedhealthjoined={:?} source={:?} originalLimit={original_limit:?}; serverdrainonly/no clientasync-clean",
        gate.joins.load(Ordering::Acquire),
        gate.drops.load(Ordering::Acquire),
        retained_health
            .as_ref()
            .map(|v| v.joined.load(Ordering::Acquire)),
        retained_source.as_ref().map(|v| v.diagnostic())
    );
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
    drop(retained_owner);
}

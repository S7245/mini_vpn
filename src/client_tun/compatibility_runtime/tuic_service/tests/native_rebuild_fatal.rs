//! N1 stage B fatal slice (B4): hard Native controlled rebuild whose successor
//! cannot resume the original cut Device. Uses the shared `GatedDevice` fault
//! wrapper around a real Native loopback Device; owns no production authority.
//! A resume `Err` is a typed successor failure (no DATA, no Q, budget zero);
//! a resume panic is sticky non-zero Q with the original device unjoined.
use super::*;
use device::{Gate, GatedDevice};
use fixture::{auth, capture, config, window, Apps, ServiceProfile};
use native_rebuild::{native_ordered_chunk, serve_generation};
use std::future::Future;
use std::sync::atomic::Ordering;

fn native_gated_service(
    profile: &ServiceProfile,
    gate: Arc<Gate>,
    inbound: PacketLink,
    outbound: PacketLink,
    mut cfg: TuicClientConfig,
    policy: Arc<crate::upstream::RelayFamilyPolicy>,
) -> Service<impl Future<Output = Completion> + use<>> {
    let ServiceProfile::Hard(identity) = profile else { unreachable!() };
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let mut runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy)
        .with_native_limits(fixture::native_hard(32 << 20), 64)
        .unwrap();
    runtime.tun_mtu = 1500;
    prepare_factory(
        move |meter: Option<bytes::BackingMeter>| async move {
            let meter = meter.ok_or_else(|| {
                ClientError::Io(std::io::Error::from(std::io::ErrorKind::InvalidData))
            })?;
            let inner = LoopbackTunDevice::native_with_factory(64, 1500, &meter, || {
                Ok((inbound, outbound))
            })?;
            Ok::<_, ClientError>(GatedDevice { inner, gate })
        },
        Configuration { tuic: cfg, composition: () },
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    )
}

pub(super) struct Rig<F: Future<Output = Completion>> {
    pub(super) server: quinn::Endpoint,
    pub(super) cfg: TuicClientConfig,
    pub(super) service: Service<F>,
    pub(super) handle: Handle,
    pub(super) gate: Arc<Gate>,
    pub(super) apps: Apps,
    pub(super) _profile: ServiceProfile,
}

pub(super) fn rig(configure: impl FnOnce(&Gate)) -> Rig<impl Future<Output = Completion>> {
    rig_with(native_ordered_chunk(), configure)
}

/// Same rig under an explicit Native relay policy (B6: Chunk or Permit engine).
pub(super) fn rig_with(
    policy: Arc<crate::upstream::RelayFamilyPolicy>,
    configure: impl FnOnce(&Gate),
) -> Rig<impl Future<Output = Completion>> {
    let profile = ServiceProfile::new(true);
    let server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let gate = Arc::new(Gate::default());
    gate.cut_enabled.store(true, Ordering::Release);
    gate.release.store(true, Ordering::Release);
    configure(&gate);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let apps = Apps::new(outbound.clone(), inbound.clone());
    let mut service = native_gated_service(&profile, gate.clone(), inbound, outbound, cfg.clone(), policy);
    let handle = service.take_handle().unwrap();
    Rig { server, cfg, service, handle, gate, apps, _profile: profile }
}

/// Real first-generation DATA, then one explicit rebuild request.
pub(super) async fn first_data_then_rebuild<F: Future<Output = Completion>>(
    rig: &mut Rig<F>,
) -> (Arc<SessionControl>, quinn::Connection) {
    let Rig { server, cfg, service, handle, apps, .. } = rig;
    let peer = window(Duration::from_secs(5), async {
        tokio::select! {
            peer = async {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, cfg).await;
                serve_generation(&peer, apps).await;
                peer
            } => peer,
            report = service.run() => panic!("Native Service ended before first DATA: {report:?}"),
        }
    })
    .await
    .expect("first generation DATA within 5s");
    let old = service.control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
    assert!(handle.request_rebuild());
    (old, peer)
}

/// Drive the Service until `done` or completion, accepting (handshake only)
/// the successor connection on the way. Returns the completion report, if any.
/// C5/F3: `done` is re-evaluated on the same 1ms tick as `tick_until` (a held
/// or Quarantined Service wakes nothing), never only at `select!` entry.
pub(super) async fn drive_successor<F: Future<Output = Completion>>(
    rig: &mut Rig<F>,
    peers: &mut Vec<quinn::Connection>,
    done: impl Fn(&Handle) -> bool,
) -> Option<Report> {
    let Rig { server, service, handle, .. } = rig;
    window(Duration::from_secs(5), async {
        let accept = async { server.accept().await.unwrap().await };
        tokio::pin!(accept);
        let mut run = std::pin::pin!(service.run());
        let mut accepted = false;
        while !done(handle) {
            tokio::select! {
                connection = &mut accept, if !accepted => {
                    accepted = true;
                    peers.push(connection.expect("successor handshake completes"));
                }
                report = &mut run => return Some(report),
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
        None
    })
    .await
    .expect("successor disposition within the original 5s window")
}

pub(super) async fn close(mut server: quinn::Endpoint, peers: Vec<quinn::Connection>) {
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    assert!(tokio::time::timeout(Duration::from_secs(10), drain.drained()).await.unwrap());
}

/// B4 resume Err: the successor Node reached AUTH, but the cut Device refuses
/// to resume. Frozen disposition (Generic `rebuild_rejection.rs`): the original
/// IO error is the successor's typed first failure, the successor publishes no
/// DATA, the Device is joined/dropped once, no Q, and the budget returns to 0.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_resume_error_is_typed_without_successor_data_or_q() {
    let mut rig = rig(|gate| gate.resume_error.store(true, Ordering::Release));
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let scenario = capture(async {
        let (old, first) = first_data_then_rebuild(&mut rig).await;
        peers.push(first);
        let report = drive_successor(&mut rig, &mut peers, |_| false).await;
        assert_eq!(peers.len(), 2, "successor Node completed its handshake before the resume");
        (report.expect("resume failure settles on its own"), old)
    })
    .await;
    let cleanup = capture(async {
        let (report, old) = scenario.as_ref().ok().cloned().unzip();
        let report = match report {
            Some(report) => report,
            None => {
                rig.handle.request_stop();
                window(Duration::from_secs(15), rig.service.run()).await.unwrap()
            }
        };
        assert!(report.device_settled() && report.device_failed(), "{report:?}");
        assert_eq!(report.rebuilds, 1);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(rig.handle.phase(), Phase::Complete);
        assert_eq!(rig.gate.cuts.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.resumes.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.resumed.load(Ordering::Acquire), 0);
        assert_eq!(rig.gate.joins.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.drops.load(Ordering::Acquire), 1);
        let completion = rig.service.completion.as_ref().unwrap();
        let current = &completion.control;
        if let Some(old) = &old {
            assert!(!Arc::ptr_eq(current, old), "failure owned by the successor");
            {
                let ledger = old.ledger();
                assert!(ledger.first_failure.is_none() && !ledger.quarantined);
            }
        }
        {
            let ledger = current.ledger();
            let facts = ledger.first_failure.as_ref().expect("typed resume failure").facts();
            assert_eq!(facts.raw_os_code, Some(13), "original resume error retained");
            assert!(!ledger.quarantined);
        }
        assert_eq!(current.flow_observation().map(|f| f.published).unwrap_or(0), 0,
            "successor never published DATA");
        let retired = completion._retired.as_ref().unwrap();
        retired.node.assert_service_clean_for_test();
        completion._node.as_ref().unwrap().assert_service_clean_for_test();
        assert!(!rig.handle.request_rebuild(), "no third generation");
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0, "both generations returned the shared budget");
    })
    .await;
    let drained = capture(close(rig.server, peers)).await;
    scenario.map(drop).unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

/// B4 resume panic: unknown Device state after a cut. The original session
/// takes sticky Q (DeviceShutdown), the shared budget is quarantined non-zero,
/// the Service never completes or rebuilds again, and the original Device is
/// never joined; only forced shell destruction drops it.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_resume_panic_is_sticky_q_without_successor_data() {
    // Physical release stays closed (as in the Generic twin): any shutdown
    // poll after Q stays pending, so a join would prove an unknown join.
    let mut rig = rig(|gate| {
        gate.resume_panic.store(true, Ordering::Release);
        gate.release.store(false, Ordering::Release);
    });
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let scenario = capture(async {
        let (old, first) = first_data_then_rebuild(&mut rig).await;
        peers.push(first);
        let report = drive_successor(&mut rig, &mut peers, |h| h.phase() == Phase::Quarantined).await;
        assert!(report.is_none(), "resume panic never completes: {report:?}");
        {
            let ledger = old.ledger();
            assert!(ledger.quarantined, "original session owns the unknown Device");
            let facts = ledger.first_failure.as_ref().unwrap().facts();
            assert_eq!(facts.origin.phase, crate::shared::errors::FailurePhase::DeviceShutdown);
        }
        assert!(old.business_complete.load(Ordering::Acquire));
        assert!(budget.quarantined() && budget.used() != 0, "sticky non-zero Q");
        assert_eq!(rig.gate.cuts.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.resumes.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.resumed.load(Ordering::Acquire), 0);
        for _ in 0..3 {
            assert_eq!(rig.handle.observe_shutdown().await, Phase::Quarantined);
            assert!(tokio::time::timeout(Duration::from_millis(10), rig.service.run()).await.is_err());
            assert_eq!(rig.gate.joins.load(Ordering::Acquire), 0);
            assert!(!rig.handle.request_rebuild());
        }
        // The driver parked before installing the successor session, so no
        // successor ingress/DATA was ever reachable.
        let current = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert!(Arc::ptr_eq(&current, &old), "successor session never installed");
    })
    .await;
    // Forced shell destruction is explicitly not asynchronous Node/device cleanup.
    let Rig { server, service, gate, _profile, .. } = rig;
    drop(service);
    let drained = capture(close(server, peers)).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
    assert!(budget.quarantined(), "Q stays sticky after forced destruction");
}

/// Same driver polled with a 1ms tick until `done` (a held cut wakes nothing).
pub(super) async fn tick_until<F: Future<Output = Completion>>(
    rig: &mut Rig<F>,
    done: impl Fn(&Handle, &Gate) -> bool,
) {
    let Rig { service, handle, gate, .. } = rig;
    window(Duration::from_secs(5), async {
        let mut run = std::pin::pin!(service.run());
        while !done(handle, gate) {
            tokio::select! {
                report = &mut run => panic!("Service ended before the awaited state: {report:?}"),
                () = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
    })
    .await
    .expect("awaited state within the original window");
}

/// B4b old drain Expired: the real cut is held until 10ms before the original
/// business deadline (paused clock); the real old Node drain (QUIC 3×PTO) then
/// ends past it. Node disposition `Expired` → session TimedOut, no successor
/// Node/DATA/accept, rebuilds 0, device joined once, budget 0, non-Q.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_old_drain_past_deadline_expires_without_successor() {
    let mut rig = rig(|gate| gate.hold_cut.store(true, Ordering::Release));
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let mut paused = false;
    let scenario = capture(async {
        let (old, first) = first_data_then_rebuild(&mut rig).await;
        peers.push(first);
        tick_until(&mut rig, |_, gate| gate.cuts.load(Ordering::Acquire) == 1).await;
        let limit = old.ledger().shutdown_deadline.expect("original business deadline");
        tokio::time::pause();
        paused = true;
        let before = limit.saturating_duration_since(tokio::time::Instant::now());
        tokio::time::advance(before - Duration::from_millis(10)).await;
        rig.gate.release_cut.store(true, Ordering::Release);
        rig.gate.cut_wake.notify_waiters();
        let report = drive_successor(&mut rig, &mut peers, |_| false).await;
        assert!(tokio::time::Instant::now() > limit, "real drain ended past the deadline");
        (report.expect("expired rebuild settles on its own"), old, limit)
    })
    .await;
    if paused {
        tokio::time::resume();
    }
    let cleanup = capture(async {
        let (report, old, limit) = scenario.as_ref().ok().cloned().expect("scenario");
        assert!(report.device_settled() && report.device_failed(), "{report:?}");
        assert_eq!(report.rebuilds, 0, "no successor budget consumed");
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(rig.handle.phase(), Phase::Complete);
        assert_eq!(peers.len(), 1, "no successor connection reached the peer");
        assert_eq!(rig.gate.cuts.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.resumes.load(Ordering::Acquire), 0);
        assert_eq!(rig.gate.joins.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.drops.load(Ordering::Acquire), 1);
        assert_eq!(*saved.deadline.lock().unwrap(), Some(limit), "original limit adopted");
        let completion = rig.service.completion.as_mut().unwrap();
        assert!(completion._retired.is_none(), "old Node never became a Retired shell");
        assert!(Arc::ptr_eq(&completion.control, &old), "no successor session");
        let node = completion._node.as_mut().unwrap();
        assert!(node.controlled_rebuild_disposition().is_expired());
        node.assert_service_clean_for_test();
        {
            let ledger = old.ledger();
            let facts = ledger.first_failure.as_ref().expect("typed expiry").facts();
            assert_eq!(facts.origin.phase, crate::shared::errors::FailurePhase::SessionLoop);
            assert_eq!(facts.io_kind, Some(std::io::ErrorKind::TimedOut));
            assert!(!ledger.quarantined);
        }
        assert!(!rig.handle.request_rebuild(), "no further generation");
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0, "budget returned");
    })
    .await;
    let drained = capture(close(rig.server, peers)).await;
    scenario.map(drop).unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

/// B4b old drain Quarantined: the old Node's real live stats task is aborted
/// right after `begin_controlled_rebuild` (cfg(test) `abort_stats_at_rebuild`),
/// so the real drain joins a cancelled task. Node physical custody is clear,
/// yet the outcome is uncertain: sticky Phase Q, budget Q holding exactly the
/// cut Device's bytes, no resume/join, no successor, no completion.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_old_drain_quarantine_is_sticky_without_successor() {
    let mut rig = rig(|gate| gate.release.store(false, Ordering::Release));
    rig.service.control.abort_stats_at_rebuild.store(true, Ordering::Release);
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let scenario = capture(async {
        let (old, first) = first_data_then_rebuild(&mut rig).await;
        peers.push(first);
        tick_until(&mut rig, |handle, _| handle.phase() == Phase::Quarantined).await;
        assert!(saved.node_fault_settled.load(Ordering::Acquire), "Node custody asserted clear");
        assert!(old.business_complete.load(Ordering::Acquire));
        {
            let ledger = old.ledger();
            let facts = ledger.first_failure.as_ref().expect("Node cancellation retained").facts();
            assert_eq!(facts.class, crate::shared::errors::FailureClass::Cancelled);
        }
        assert!(budget.quarantined(), "uncertain old drain quarantines the shared budget");
        assert_eq!(budget.used(), native_rebuild::DEVICE_HELD_AT_SUCCESSOR, "only the cut Device");
        for _ in 0..3 {
            assert_eq!(rig.handle.observe_shutdown().await, Phase::Quarantined);
            assert!(tokio::time::timeout(Duration::from_millis(10), rig.service.run()).await.is_err());
            assert_eq!(rig.gate.resumes.load(Ordering::Acquire), 0);
            assert_eq!(rig.gate.joins.load(Ordering::Acquire), 0);
            assert!(!rig.handle.request_rebuild());
        }
        assert_eq!(rig.gate.cuts.load(Ordering::Acquire), 1);
        assert!(rig.service.completion.is_none());
        let current = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert!(Arc::ptr_eq(&current, &old), "successor session never installed");
    })
    .await;
    let Rig { server, service, gate, _profile, .. } = rig;
    drop(service);
    let drained = capture(close(server, peers)).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
    assert!(budget.quarantined(), "Q stays sticky after forced destruction");
}

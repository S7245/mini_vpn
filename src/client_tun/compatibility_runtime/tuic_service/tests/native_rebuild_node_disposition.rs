//! N1 C4 (B-review F1/F2): hard Native controlled rebuild whose original Node
//! drain is injected into each non-Allowed disposition through the cfg(test)
//! `NodeHost::C4_*` seam (D102). Owns no production authority: every fault
//! acts on the real Node custody at one drain stage; Service and Node keep
//! their production settlement. Frozen dispositions:
//! - Incomplete (retained DATA Arc): drain never settles, no successor/DATA,
//!   observer `Phase::Incomplete`; only the real holder Drop settles it.
//! - Quarantined (custody contradiction after a settled drain): sticky Phase
//!   and budget Q holding exactly the cut Device, no successor/JOIN.
//! - Failed (known Node failure): typed first, final stop, no successor, non-Q.
//! - Unsettled (result not a controlled Running settlement, custody clear):
//!   final clean stop, no successor, non-Q.
use super::*;
use fixture::capture;
use native_rebuild::DEVICE_HELD_AT_SUCCESSOR;
use native_rebuild_fatal::{close, drive_successor, first_data_then_rebuild, rig, tick_until, Rig};
use std::future::Future;
use std::sync::atomic::Ordering;

fn injected(fault: u8) -> Rig<impl Future<Output = Completion>> {
    let rig = rig(|_| {});
    rig.service.control.rebuild_node_fault.store(fault, Ordering::Release);
    rig
}

fn disposition<F: Future<Output = Completion>>(rig: &mut Rig<F>) -> String {
    let node = rig.service.completion.as_mut().unwrap()._node.as_mut().unwrap();
    format!("{:?}", node.controlled_rebuild_disposition())
}

/// Poll the saved driver for `span` (real clock); it must neither finish nor
/// leave the rebuilding position.
async fn hold<F: Future<Output = Completion>>(rig: &mut Rig<F>, span: Duration) {
    let Rig { service, handle, .. } = rig;
    let mut run = std::pin::pin!(service.run());
    let until = tokio::time::Instant::now() + span;
    while tokio::time::Instant::now() < until {
        tokio::select! {
            report = &mut run => panic!("drain settled while DATA is held: {report:?}"),
            () = tokio::time::sleep(Duration::from_millis(1)) => {}
        }
        let phase = handle.phase();
        assert!(
            !matches!(phase, Phase::Starting | Phase::Running | Phase::Complete | Phase::Quarantined),
            "held drain published {phase:?}"
        );
    }
}

/// Common final-stop shape for a refused successor whose custody is clear.
fn assert_refused<F: Future<Output = Completion>>(
    rig: &mut Rig<F>,
    peers: &[quinn::Connection],
    old: &Arc<SessionControl>,
    report: &Report,
) {
    assert_eq!(report.rebuilds, 0, "no successor budget consumed");
    assert_eq!(rig.handle.phase(), Phase::Complete);
    assert_eq!(peers.len(), 1, "no successor connection reached the peer");
    assert_eq!(rig.gate.cuts.load(Ordering::Acquire), 1);
    assert_eq!(rig.gate.resumes.load(Ordering::Acquire), 0);
    assert_eq!(rig.gate.joins.load(Ordering::Acquire), 1);
    assert_eq!(rig.gate.drops.load(Ordering::Acquire), 1);
    let completion = rig.service.completion.as_ref().unwrap();
    assert!(completion._retired.is_none(), "old Node never became a Retired shell");
    assert!(Arc::ptr_eq(&completion.control, old), "no successor session");
    assert!(!rig.handle.request_rebuild(), "no further generation");
}

/// F1 Incomplete: one real DATA Arc retained across the controlled drain.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_old_drain_with_retained_data_stays_incomplete() {
    let mut rig = injected(NodeHost::C4_HOLD_DATA);
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let mut paused = false;
    let scenario = capture(async {
        let (old, first) = first_data_then_rebuild(&mut rig).await;
        peers.push(first);
        tick_until(&mut rig, |_, gate| gate.cuts.load(Ordering::Acquire) == 1).await;
        hold(&mut rig, Duration::from_millis(500)).await;
        assert!(saved.held_node_data.lock().unwrap().is_some());
        let used = budget.used();
        println!("C4 T1 held used={used} phase={:?}", rig.handle.phase());
        assert!(!budget.quarantined() && used >= DEVICE_HELD_AT_SUCCESSOR, "used {used}");
        assert_eq!(rig.gate.resumes.load(Ordering::Acquire), 0);
        assert_eq!(rig.gate.joins.load(Ordering::Acquire), 0);
        assert_eq!(rig.gate.drops.load(Ordering::Acquire), 0);
        assert!(rig.service.completion.is_none());
        assert!(old.ledger().first_failure.is_none());
        tokio::time::pause();
        paused = true;
        let Rig { service, handle, .. } = &mut rig;
        {
            let mut run = std::pin::pin!(service.run());
            let phase = tokio::select! {
                report = &mut run => panic!("held drain completed: {report:?}"),
                phase = handle.observe_shutdown() => phase,
            };
            assert_eq!(phase, Phase::Incomplete, "observer sees the unsettled drain");
            // D108 (review F5): the CLI verdict reporter runs alongside.
            let mut verdicts = Vec::new();
            tokio::select! {
                report = &mut run => panic!("held drain completed: {report:?}"),
                never = super::super::report_verdicts(handle, |line| verdicts.push(line)) => match never {},
                () = tokio::time::sleep(Duration::from_secs(16)) => {}
            }
            // O-C4b (D105): past the Node's rebuild deadline the Service
            // verdict is Incomplete while it keeps driving the same drain.
            assert_eq!(handle.phase(), Phase::Incomplete, "bounded stalled-drain verdict");
            println!("C4 T1 verdicts={verdicts:?}");
            assert_eq!(verdicts.len(), 1, "Incomplete reported exactly once: {verdicts:?}");
            assert!(verdicts[0].contains("Incomplete"), "{verdicts:?}");
        }
        assert_eq!(rig.gate.joins.load(Ordering::Acquire), 0);
        assert!(!budget.quarantined());
        drop(saved.held_node_data.lock().unwrap().take());
        tokio::time::resume();
        paused = false;
        let report = drive_successor(&mut rig, &mut peers, |_| false).await;
        (report.expect("released drain settles on its own"), old)
    })
    .await;
    if paused {
        tokio::time::resume();
    }
    let cleanup = capture(async {
        let (report, old) = scenario.as_ref().ok().cloned().expect("scenario");
        assert!(report.device_settled() && report.device_failed(), "{report:?}");
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_refused(&mut rig, &peers, &old, &report);
        assert_eq!(disposition(&mut rig), "Expired");
        {
            let ledger = old.ledger();
            let facts = ledger.first_failure.as_ref().expect("typed expiry").facts();
            assert_eq!(facts.origin.phase, crate::shared::errors::FailurePhase::SessionLoop);
            assert_eq!(facts.io_kind, Some(std::io::ErrorKind::TimedOut));
            assert!(!ledger.quarantined);
            // D107 F3: the bounded verdict and the late Expired settlement are
            // one expiry event, recorded once.
            assert_eq!(ledger.additional_failures, 0, "one expiry, one TimedOut record");
        }
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0, "budget returned only after the real holder Drop");
    })
    .await;
    let drained = capture(close(rig.server, peers)).await;
    scenario.map(drop).unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

/// F2(a) Quarantined: settled drain contradicted by one real live task.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_settled_drain_custody_contradiction_is_sticky_q() {
    let mut rig = injected(NodeHost::C4_UNJOINED_TASK);
    rig.gate.release.store(false, Ordering::Release);
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let scenario = capture(async {
        let (old, first) = first_data_then_rebuild(&mut rig).await;
        peers.push(first);
        tick_until(&mut rig, |handle, _| handle.phase() == Phase::Quarantined).await;
        assert!(saved.node_fault_settled.load(Ordering::Acquire), "Node contradiction asserted");
        assert!(old.business_complete.load(Ordering::Acquire));
        {
            let ledger = old.ledger();
            let first = ledger.first_failure.as_ref().map(|f| f.facts().class.clone());
            println!("C4 T2 session first={first:?} q={}", ledger.quarantined);
            assert!(ledger.first_failure.is_none(), "D102 observation: Node-local first only");
        }
        assert!(budget.quarantined(), "contradiction quarantines the shared budget");
        assert_eq!(budget.used(), DEVICE_HELD_AT_SUCCESSOR, "only the cut Device");
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

/// F2(b) Failed / Unsettled: settled custody, successor refused, final stop.
async fn refused(fault: u8, node: crate::tuic::NodeShutdown, failed: bool, expected: &str) {
    let mut rig = injected(fault);
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let scenario = capture(async {
        let (old, first) = first_data_then_rebuild(&mut rig).await;
        peers.push(first);
        let report = drive_successor(&mut rig, &mut peers, |_| false).await;
        (report.expect("refused rebuild settles on its own"), old)
    })
    .await;
    let cleanup = capture(async {
        let (report, old) = scenario.as_ref().ok().cloned().expect("scenario");
        assert!(report.device_settled(), "{report:?}");
        assert_eq!(report.device_failed(), failed, "{report:?}");
        assert_eq!(report.node, Some(node));
        assert_refused(&mut rig, &peers, &old, &report);
        assert_eq!(disposition(&mut rig), expected);
        {
            let ledger = old.ledger();
            assert!(!ledger.quarantined);
            match ledger.first_failure.as_ref() {
                Some(first) => {
                    let facts = first.facts();
                    assert!(failed, "unexpected first {facts:?}");
                    assert_eq!(facts.class, crate::shared::errors::FailureClass::InvalidTarget);
                    assert_eq!(facts.origin.resource, crate::shared::errors::FailureResource::TuicNode);
                }
                None => assert!(!failed, "typed Node first must reach the session"),
            }
        }
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0, "budget returned");
    })
    .await;
    let drained = capture(close(rig.server, peers)).await;
    scenario.map(drop).unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_failed_node_drain_refuses_successor_with_typed_first() {
    refused(NodeHost::C4_KNOWN_FAILURE, crate::tuic::NodeShutdown::FailedSettled, true, "Failed").await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_unsettled_node_result_refuses_successor_cleanly() {
    refused(
        NodeHost::C4_STARTUP_CLASSIFIED,
        crate::tuic::NodeShutdown::StartupResourcesSettled,
        false,
        "Unsettled",
    )
    .await;
}

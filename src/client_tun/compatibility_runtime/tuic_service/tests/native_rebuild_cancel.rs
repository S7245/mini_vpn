//! N1 stage B cancellation slice (B5): explicit stop during a hard Native
//! controlled rebuild, through the original saved driver. Reuses the B4
//! `GatedDevice` rig (`native_rebuild_fatal`); owns no production authority.
//! Stop is not a failure: no typed first failure, no Q, budget returns to 0,
//! the Device is joined/dropped once, and no successor DATA is ever reachable.
use super::*;
use fixture::{capture, window, ServiceProfile};
use native_rebuild_fatal::{close, first_data_then_rebuild, rig, tick_until};
use std::sync::atomic::Ordering;

/// Stop while the real cut is held (Rebuilding, before any Node drain): the
/// cut's stop arm wins, no receipt, no `begin_controlled_rebuild`, the original
/// business limit is kept, rebuilds 0, the original Node settles normally.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_stop_during_held_cut_keeps_original_limit_without_successor() {
    let mut rig = rig(|gate| gate.hold_cut.store(true, Ordering::Release));
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let scenario = capture(async {
        let (old, first) = first_data_then_rebuild(&mut rig).await;
        peers.push(first);
        tick_until(&mut rig, |_, gate| gate.cuts.load(Ordering::Acquire) == 1).await;
        let limit = old.ledger().shutdown_deadline.expect("original business deadline");
        rig.handle.request_stop();
        let report = window(Duration::from_secs(5), rig.service.run())
            .await
            .expect("stop settles within the original window");
        assert!(tokio::time::Instant::now() < limit, "settled before the original limit");
        (report, old, limit)
    })
    .await;
    let cleanup = capture(async {
        let (report, old, limit) = scenario.as_ref().ok().cloned().expect("scenario");
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(report.rebuilds, 0);
        assert_eq!(rig.handle.phase(), Phase::Complete);
        assert_eq!(*saved.deadline.lock().unwrap(), Some(limit), "original limit kept");
        assert_eq!(peers.len(), 1);
        assert!(peers[0].close_reason().is_some(), "original connection really closed");
        assert_eq!(rig.gate.cuts.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.resumes.load(Ordering::Acquire), 0);
        assert_eq!(rig.gate.joins.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.drops.load(Ordering::Acquire), 1);
        let completion = rig.service.completion.as_ref().unwrap();
        assert!(completion._retired.is_none());
        assert!(Arc::ptr_eq(&completion.control, &old), "no successor session");
        completion._node.as_ref().unwrap().assert_service_clean_for_test();
        {
            let ledger = old.ledger();
            assert!(ledger.first_failure.is_none() && !ledger.quarantined);
        }
        assert!(!rig.handle.request_rebuild());
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0);
    })
    .await;
    let drained = capture(close(rig.server, peers)).await;
    scenario.map(drop).unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

/// Where the successor is held when stop arrives (F4).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Hold {
    /// Peer holds the `Incoming` without accepting (mid-handshake).
    Handshake,
    /// Handshake completed, but the peer grants 0 uni streams, so the
    /// successor's TUIC Authenticate blocks in `open_uni` (before AUTH,
    /// compose and resume).
    Auth,
}

/// Stop while the successor is held at `hold`: the old Node is already a
/// Retired shell, the successor Node settles as startup, the cut Device is
/// never resumed, both Nodes are service-clean, no successor session/DATA is
/// installed, and the stop limit is a fresh 15s limit (never the retired
/// generation's business limit, F4).
async fn stop_before_successor_resume(hold: Hold) {
    let mut rig = rig(|_| {});
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let mut held = None;
    let scenario = capture(async {
        let (old, first) = first_data_then_rebuild(&mut rig).await;
        peers.push(first);
        if hold == Hold::Auth {
            rig.server.set_server_config(Some(no_uni_streams(&rig._profile)));
        }
        let incoming = {
            let mut run = std::pin::pin!(rig.service.run());
            window(Duration::from_secs(5), async {
                tokio::select! {
                    incoming = rig.server.accept() => incoming.expect("successor Initial"),
                    report = &mut run => panic!("Service ended before successor: {report:?}"),
                }
            })
            .await
            .expect("successor reaches the peer within the original window")
        };
        match hold {
            Hold::Handshake => held = Some(incoming),
            Hold::Auth => {
                let peer = {
                    let mut run = std::pin::pin!(rig.service.run());
                    let peer = window(Duration::from_secs(5), async {
                        tokio::select! {
                            peer = incoming => peer.expect("successor handshake completes"),
                            report = &mut run => panic!("Service ended in handshake: {report:?}"),
                        }
                    })
                    .await
                    .expect("successor handshake within the original window");
                    // Give the successor ample time to reach AUTH and block.
                    let _ = tokio::time::timeout(Duration::from_millis(300), &mut run).await;
                    peer
                };
                assert_eq!(rig.gate.resumes.load(Ordering::Acquire), 0, "AUTH blocked before resume");
                peers.push(peer);
            }
        }
        assert_eq!(rig.handle.phase(), Phase::Starting);
        assert!(old.business_complete.load(Ordering::Acquire));
        let retired_limit = old.ledger().shutdown_deadline;
        let requested = tokio::time::Instant::now();
        rig.handle.request_stop();
        // The retired limit is distinguishable: inheriting it would fail below.
        assert!(retired_limit.is_some_and(|l| l < requested + Duration::from_secs(15)));
        let limit = saved.deadline.lock().unwrap().expect("stop limit published");
        assert!(limit >= requested + Duration::from_secs(15),
            "fresh stop limit, not the retired one ({retired_limit:?})");
        let report = window(Duration::from_secs(5), rig.service.run())
            .await
            .expect("stop settles within the window");
        (report, old)
    })
    .await;
    let cleanup = capture(async {
        let (report, old) = scenario.as_ref().ok().cloned().expect("scenario");
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::StartupResourcesSettled));
        assert_eq!(report.rebuilds, 1, "successor budget consumed");
        assert_eq!(rig.handle.phase(), Phase::Complete);
        let expected_peers = if hold == Hold::Auth { 2 } else { 1 };
        assert_eq!(peers.len(), expected_peers, "successor handshake completion per hold");
        assert_eq!(rig.gate.cuts.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.resumes.load(Ordering::Acquire), 0, "cut Device never resumed");
        assert_eq!(rig.gate.joins.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.drops.load(Ordering::Acquire), 1);
        let completion = rig.service.completion.as_ref().unwrap();
        let retired = completion._retired.as_ref().expect("old Node retired shell");
        assert!(Arc::ptr_eq(&retired.control, &old));
        retired.node.assert_service_clean_for_test();
        completion._node.as_ref().expect("successor Node").assert_service_clean_for_test();
        assert_eq!(completion.control.flow_observation().map(|f| f.published).unwrap_or(0), 0,
            "no successor DATA");
        for control in [&old, &completion.control] {
            let ledger = control.ledger();
            assert!(ledger.first_failure.is_none() && !ledger.quarantined);
        }
        assert!(!rig.handle.request_rebuild());
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0, "both generations returned the shared budget");
    })
    .await;
    drop(held);
    let drained = capture(close(rig.server, peers)).await;
    scenario.map(drop).unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

/// Same identity/ALPN, but the peer grants no uni stream (AUTH cannot open).
fn no_uni_streams(profile: &ServiceProfile) -> quinn::ServerConfig {
    let mut config = profile.server_config();
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(0u32.into());
    transport.max_concurrent_bidi_streams(100u32.into());
    transport.stream_receive_window(65536u32.into());
    config.transport_config(Arc::new(transport));
    config
}

/// B5(b) + F4: stop while the successor handshake is held.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_stop_during_successor_handshake_joins_both_generations() {
    stop_before_successor_resume(Hold::Handshake).await;
}

/// F4: stop after the successor handshake but before AUTH/compose/resume.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_stop_after_successor_handshake_before_auth_joins_both_generations() {
    stop_before_successor_resume(Hold::Auth).await;
}

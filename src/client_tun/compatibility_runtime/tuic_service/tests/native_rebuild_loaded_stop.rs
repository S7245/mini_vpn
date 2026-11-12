//! N1 stage B shutdown slice (B6): explicit stop of the successor generation
//! while a real Native flow (Chunk or Permit engine) owns DATA, after one
//! controlled rebuild through the original saved driver. Reuses the B4
//! `GatedDevice` rig; owns no production authority. Asserts the clean parent
//! stop disposition: Retired shell and successor Node both service-clean, the
//! cut Device stopped/joined/dropped before the successor Node drains, the
//! successor flow settles with a real receipt, budget 0, non-Q.
use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{auth, capture, window};
use native_rebuild_fatal::{close, first_data_then_rebuild, rig_with, Rig};
use std::sync::atomic::Ordering;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Engine {
    Chunk,
    Permit,
}

impl Engine {
    pub(super) fn policy(self) -> Arc<RelayFamilyPolicy> {
        let permit = self == Self::Permit;
        let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            tuic_native_ordered: Some("1"),
            native_egress_permit: permit.then_some("1"),
            ..Default::default()
        }));
        assert_eq!(policy.tuic_native_override(), TuicNativeOverride::NativeOrderedPump);
        let engine = if permit { NativeTcpRelayEngine::PermitPump } else { NativeTcpRelayEngine::ChunkPump };
        assert_eq!(policy.native_engine(), engine);
        policy
    }

    /// Same payload sizes as the accepted single-generation loaded-stop leaves.
    pub(super) fn payload(self) -> usize {
        match self {
            Self::Chunk => 1024 * 1024,
            Self::Permit => 65536,
        }
    }

    /// This generation's original engine queue owns DATA right now.
    pub(super) async fn loaded(self, owner: &SessionControl) -> bool {
        match self {
            Self::Chunk => {
                let queue = owner.flow_native_chunk_queue().expect("Chunk queue");
                let snapshot = queue.custody_snapshot().await.0;
                assert!(snapshot.queued_bytes <= snapshot.capacity_bytes);
                snapshot.queued_bytes > 0
            }
            Self::Permit => {
                let queue = owner.flow_native_permit_queue().expect("Permit queue");
                let physical = queue.snapshot_now();
                assert!(physical.owned_bytes() <= physical.capacity_bytes);
                physical.queued_bytes + physical.leased_bytes > 0
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_rebuild_successor_chunk_loaded_stop_settles_device_before_node() {
    successor_loaded_stop(Engine::Chunk).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_rebuild_successor_permit_loaded_stop_settles_device_before_node() {
    successor_loaded_stop(Engine::Permit).await;
}

/// Device custody seen at the instant the successor peer observes the
/// successor Node's connection close: (joins, drops, driver_complete).
type CloseWitness = (usize, usize, bool);

async fn successor_loaded_stop(engine: Engine) {
    let mut rig = rig_with(engine.policy(), |_| {});
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let mut backing = None;
    let scenario = capture(async {
        let (old, first) = first_data_then_rebuild(&mut rig).await;
        peers.push(first);
        let Rig { server, cfg, service, handle, gate, apps, .. } = &mut rig;
        // Successor generation: real accept + AUTH, then one real Native flow.
        let (peer, (mut send, recv), owner) = window(Duration::from_secs(5), async {
            tokio::select! {
                successor = async {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, cfg).await;
                    assert!(old.business_complete.load(Ordering::Acquire),
                        "successor AUTH only after original business settlement");
                    apps.restart();
                    let target = TargetAddr::IpPort(std::net::SocketAddr::from((apps.target, 10000)));
                    let remote_open = async {
                        let (send, mut recv) = peer.accept_bi().await.unwrap();
                        let mut want = encode_connect(&target);
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        (send, recv)
                    };
                    let app_open = async {
                        loop {
                            apps.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            if !Arc::ptr_eq(&owner, &old) {
                                let flow = owner.flow_observation().unwrap();
                                if flow.published == 1 && flow.running == 5 {
                                    assert_eq!(owner.flow_contracts().len(), 1);
                                    break owner;
                                }
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    let (stream, owner) = tokio::join!(remote_open, app_open);
                    (peer, stream, owner)
                } => successor,
                report = service.run() => panic!("Service ended before successor publication: {report:?}"),
            }
        })
        .await
        .expect("successor publication within 5s");
        let meter = owner.flow_native_backing_meter().expect("successor Node backing meter");
        backing = Some(meter.clone());
        assert!(apps.tcp_reply.is_empty());
        window(Duration::from_secs(5), async {
            tokio::select! {
                () = async {
                    let payload = vec![0x5a; engine.payload()];
                    let write = send.write_all(&payload);
                    tokio::pin!(write);
                    let mut written = false;
                    loop {
                        tokio::select! {
                            biased;
                            result = &mut write, if !written => { result.unwrap(); written = true; }
                            () = tokio::time::sleep(Duration::from_millis(1)) => {}
                        }
                        if engine.loaded(&owner).await {
                            assert!(meter.snapshot().payload_bytes > 0, "Node backing charged");
                            break;
                        }
                    }
                } => {},
                report = service.run() => panic!("Service ended before successor DATA load: {report:?}"),
            }
        })
        .await
        .expect("successor DATA loaded within 5s");
        assert!(apps.tcp_reply.is_empty(), "App never drained the loaded successor flow");
        // Parent stop on load, then the real peer retires (never App abort/RESET).
        handle.request_stop();
        let limit = saved.deadline.lock().unwrap().expect("stop limit");
        send.finish().unwrap();
        let mut recv = recv;
        recv.stop(78u32.into()).unwrap();
        drop((send, recv));
        let closed = peer.clone();
        peers.push(peer);
        let (report, witness) = window(Duration::from_secs(5), async {
            let mut run = std::pin::pin!(service.run());
            let mut closed = std::pin::pin!(closed.closed());
            let mut witness: Option<CloseWitness> = None;
            loop {
                tokio::select! {
                    _ = &mut closed, if witness.is_none() => {
                        witness = Some((
                            gate.joins.load(Ordering::Acquire),
                            gate.drops.load(Ordering::Acquire),
                            owner.driver_complete.load(Ordering::Acquire),
                        ));
                    }
                    report = &mut run => break (report, witness),
                }
            }
        })
        .await
        .expect("loaded successor stop settles within 5s");
        assert!(tokio::time::Instant::now() < limit, "settled before the stop limit");
        (report, witness, old, owner)
    })
    .await;
    let cleanup = capture(async {
        let (report, witness, old, owner) = scenario.as_ref().ok().cloned().expect("scenario");
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(report.rebuilds, 1);
        assert_eq!(rig.handle.phase(), Phase::Complete);
        // Final stop order (rebuild.rs): Device stop/JOIN/Drop → driver_complete
        // → successor Node drive_shutdown. The successor Node's CONNECTION_CLOSE
        // can only reach the peer after the Device is fully settled.
        assert_eq!(witness, Some((1, 1, true)), "Device settled before successor Node drain");
        assert_eq!(rig.gate.cuts.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.resumes.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.resumed.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.joins.load(Ordering::Acquire), 1);
        assert_eq!(rig.gate.drops.load(Ordering::Acquire), 1);
        let completion = rig.service.completion.as_ref().unwrap();
        let retired = completion._retired.as_ref().expect("Retired shell");
        assert!(Arc::ptr_eq(&retired.control, &old));
        assert_eq!(old.state(), SessionDeviceState::BusinessSettled);
        retired.node.assert_service_clean_for_test();
        completion._node.as_ref().unwrap().assert_service_clean_for_test();
        assert!(Arc::ptr_eq(&completion.control, &owner), "successor owns completion");
        {
            let ledger = old.ledger();
            assert!(ledger.first_failure.is_none() && !ledger.quarantined());
        }
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.live, flow.published, flow.joined, flow.released, flow.f), (0, 1, 5, 1, 0));
        assert!(flow.audit);
        {
            let ledger = owner.ledger();
            assert!(ledger.first_failure.is_none() && !ledger.quarantined());
        }
        let (messages, bytes, transferred, discarded, _) =
            flow.last_data_disposition.expect("successor real receipt");
        assert!(messages > 0 && bytes > 0);
        assert_eq!(transferred.checked_add(discarded), Some(bytes));
        let physical = backing.as_ref().unwrap().snapshot();
        assert_eq!(
            (physical.allocations, physical.payload_bytes, physical.physical_bytes, physical.quarantined),
            (0, 0, 0, false)
        );
        eprintln!("B6 {engine:?}: receipt=({messages},{bytes},{transferred},{discarded}) witness={witness:?}");
        assert!(!rig.handle.request_rebuild(), "no third generation");
    })
    .await;
    let Rig { server, apps, .. } = rig;
    drop(apps);
    let zero = capture(async {
        assert_eq!(saved.app_meter.as_ref().unwrap().snapshot().physical_bytes, 0);
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0, "both generations returned the shared budget");
    })
    .await;
    let drained = capture(close(server, peers)).await;
    scenario.map(drop).unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    zero.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    drained.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
}

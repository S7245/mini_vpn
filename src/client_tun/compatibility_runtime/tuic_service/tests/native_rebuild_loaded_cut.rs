//! N1 stage B loaded-rebuild slice (B7): explicit controlled rebuild while a
//! real first-generation Native flow (Chunk or Permit engine) owns DATA. The
//! original peer retires the stream normally (finish + STOP_SENDING); the
//! saved driver must settle the whole original business (flow joined and
//! released, real receipt, backing zero) before it cuts the Device. The cut is
//! held by the shared `GatedDevice` so the witness is sampled at the exact cut
//! instant. Reuses the B4/B6 rigs; owns no production authority.
use super::*;
use fixture::{auth, capture, window};
use native_rebuild::serve_generation;
use native_rebuild_fatal::{close, rig_with, Rig};
use native_rebuild_loaded_stop::Engine;
use std::sync::atomic::Ordering;

#[tokio::test(flavor = "current_thread")]
async fn hard_native_rebuild_chunk_loaded_original_settles_business_before_cut() {
    loaded_original_rebuild(Engine::Chunk).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_rebuild_permit_loaded_original_settles_business_before_cut() {
    loaded_original_rebuild(Engine::Permit).await;
}

/// Original Node protocol-tier residual at the held cut, measured identical
/// with and without a loaded flow (D43 probe; Node-owned, returned by the
/// Node drain that follows the cut): (allocations, payload, physical, Q).
const IDLE_NODE_AT_CUT: (usize, usize, usize, bool) = (2, 300, 428, false);

/// Original-generation custody at the held cut instant.
#[derive(Debug)]
struct CutWitness {
    business_complete: bool,
    /// (live, published, joined, released, f)
    flow: (u64, u64, u64, u64, u64),
    receipt: Option<(u64, u64, u64, u64)>,
    /// (allocations, payload, physical, quarantined) of the original Node meter.
    backing: (usize, usize, usize, bool),
    used: usize,
}

async fn loaded_original_rebuild(engine: Engine) {
    let mut rig = rig_with(engine.policy(), |gate| gate.hold_cut.store(true, Ordering::Release));
    let saved = rig.service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let scenario = capture(async {
        let Loaded { peer, mut send, recv, old, meter } = load_original(&mut rig, engine).await;
        let Rig { server, cfg, service, handle, gate, apps, .. } = &mut rig;
        // Rebuild on load; the original peer then retires normally (D43).
        assert!(handle.request_rebuild());
        send.finish().unwrap();
        let mut recv = recv;
        recv.stop(78u32.into()).unwrap();
        drop((send, recv));
        peers.push(peer);
        // Drive until the saved driver reaches the (held) cut, then sample.
        let witness = window(Duration::from_secs(5), async {
            tokio::select! {
                () = async {
                    while gate.cuts.load(Ordering::Acquire) == 0 {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("Service ended before the cut: {report:?}"),
            }
            let flow = old.flow_observation().expect("original flow observation");
            let physical = meter.snapshot();
            CutWitness {
                business_complete: old.business_complete.load(Ordering::Acquire),
                flow: (flow.live as u64, flow.published, flow.joined, flow.released, flow.f as u64),
                receipt: flow.last_data_disposition.map(|(m, b, t, d, _)| (m as u64, b as u64, t as u64, d as u64)),
                backing: (physical.allocations, physical.payload_bytes, physical.physical_bytes, physical.quarantined),
                used: budget.used(),
            }
        })
        .await
        .expect("original business settles and reaches the cut within 5s");
        drop(meter);
        // Order witness, asserted at the held cut itself (before any successor).
        eprintln!("B7 {engine:?}: cut witness {witness:?}");
        assert!(witness.business_complete, "cut only after original business settlement");
        assert_eq!(witness.flow, (0, 1, 5, 1, 0), "original flow joined/released at the cut");
        let (messages, bytes, transferred, discarded) = witness.receipt.expect("original real receipt");
        assert!(messages > 0 && bytes > 0);
        assert_eq!(transferred.checked_add(discarded), Some(bytes));
        assert_eq!(witness.backing, IDLE_NODE_AT_CUT, "no flow-owned Node backing at the cut");
        gate.release_cut.store(true, Ordering::Release);
        gate.cut_wake.notify_waiters();
        // Successor: real accept + AUTH, then real TCP and UDP.
        window(Duration::from_secs(5), async {
            tokio::select! {
                () = async {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, cfg).await;
                    let current = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                    assert!(!Arc::ptr_eq(&current, &old), "successor owns a new session");
                    apps.restart();
                    serve_generation(&peer, apps).await;
                    peers.push(peer);
                } => {},
                report = service.run() => panic!("Service ended before successor DATA: {report:?}"),
            }
        })
        .await
        .expect("successor DATA within 5s");
        assert!(!handle.request_rebuild(), "no third generation");
        handle.request_stop();
        let report = window(Duration::from_secs(15), service.run()).await.expect("stop settles");
        (report, old)
    })
    .await;
    let cleanup = capture(async {
        let (report, old) = scenario.as_ref().ok().cloned().expect("scenario");
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(report.rebuilds, 1);
        assert_eq!(rig.handle.phase(), Phase::Complete);
        for (count, want) in [
            (&rig.gate.cuts, 1),
            (&rig.gate.resumes, 1),
            (&rig.gate.resumed, 1),
            (&rig.gate.joins, 1),
            (&rig.gate.drops, 1),
        ] {
            assert_eq!(count.load(Ordering::Acquire), want);
        }
        let completion = rig.service.completion.as_ref().unwrap();
        let retired = completion._retired.as_ref().expect("Retired shell");
        assert!(Arc::ptr_eq(&retired.control, &old));
        assert_eq!(old.state(), SessionDeviceState::BusinessSettled);
        retired.node.assert_service_clean_for_test();
        completion._node.as_ref().unwrap().assert_service_clean_for_test();
        assert!(!Arc::ptr_eq(&completion.control, &old), "successor owns completion");
        {
            let ledger = old.ledger();
            assert!(ledger.first_failure.is_none() && !ledger.quarantined);
        }
        {
            let ledger = completion.control.ledger();
            assert!(ledger.first_failure.is_none() && !ledger.quarantined);
        }
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


/// One loaded original generation (B7, F5): real accept + AUTH, one real
/// Native TCP flow published (running 5) + UDP answered, then the peer writes
/// until the engine queue owns DATA and the Node backing is charged. The App
/// never reads. Returns the live peer stream halves for the caller to retire
/// (B7) or keep open (F5).
pub(super) struct Loaded {
    pub(super) peer: quinn::Connection,
    pub(super) send: quinn::SendStream,
    pub(super) recv: quinn::RecvStream,
    pub(super) old: Arc<SessionControl>,
    pub(super) meter: bytes::BackingMeter,
}

/// First generation only (B7, F5, D62): real accept + AUTH, one real Native
/// TCP flow published (running 5) + UDP answered; no DATA load.
pub(super) async fn open_original<F: std::future::Future<Output = Completion>>(
    rig: &mut Rig<F>,
) -> (quinn::Connection, (quinn::SendStream, quinn::RecvStream), Arc<SessionControl>) {
    let saved = rig.service.control.clone();
    let Rig { server, cfg, service, apps, .. } = rig;
    window(Duration::from_secs(5), async {
        tokio::select! {
            first = async {
                let peer = server.accept().await.unwrap().await.unwrap();
                auth(&peer, cfg).await;
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
                // First-generation UDP answered too (real TCP + UDP).
                let udp_target = TargetAddr::IpPort(std::net::SocketAddr::from((apps.target, 10001)));
                let remote_udp = async {
                    let actual = peer.read_datagram().await.unwrap();
                    let assoc = u16::from_be_bytes([actual[2], actual[3]]);
                    assert_eq!(&actual[..], encode_packet(assoc, &udp_target, b"service-udp"));
                    peer.send_datagram(encode_packet(assoc, &udp_target, b"udp-reply").into()).unwrap();
                };
                let app_open = async {
                    loop {
                        apps.turn().await;
                        let owner = saved.session.lock().unwrap().as_ref().and_then(|w| w.upgrade());
                        if let Some(owner) = owner {
                            let flow = owner.flow_observation();
                            if flow.is_some_and(|f| f.published == 1 && f.running == 5)
                                && apps.udp_reply == b"udp-reply"
                            {
                                assert_eq!(owner.flow_contracts().len(), 1);
                                break owner;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                };
                let (stream, (), owner) = tokio::join!(remote_open, remote_udp, app_open);
                (peer, stream, owner)
            } => first,
            report = service.run() => panic!("Service ended before first publication: {report:?}"),
        }
    })
    .await
    .expect("first-generation publication within 5s")
}

pub(super) async fn load_original<F: std::future::Future<Output = Completion>>(
    rig: &mut Rig<F>,
    engine: Engine,
) -> Loaded {
    let (peer, (mut send, recv), old) = open_original(rig).await;
    let Rig { service, apps, .. } = rig;
    let meter = old.flow_native_backing_meter().expect("original Node backing meter");
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
                    if engine.loaded(&old).await {
                        assert!(meter.snapshot().payload_bytes > 0, "Node backing charged");
                        break;
                    }
                }
            } => {},
            report = service.run() => panic!("Service ended before original DATA load: {report:?}"),
        }
    })
    .await
    .expect("original DATA loaded within 5s");
    assert!(apps.tcp_reply.is_empty(), "App never drained the loaded original flow");
    Loaded { peer, send, recv, old, meter }
}
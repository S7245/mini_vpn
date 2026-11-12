//! Exact full F4096/W128 inventory through the original saved service, NodeHost
//! and typed UDP. Shares the real bounded app/peer fixture with public callers;
//! no old manual-upstream session, fake factory, settlement receipt or clock.
use super::*;
use device::{Gate, GatedDevice};
use std::sync::atomic::Ordering;
#[path = "../../../../../tests/tuic_service/full_fixture.rs"]
mod wire;

#[tokio::test(flavor = "current_thread")]
async fn saved_full_f4096_w128_clean_rebuild_preserves_device_and_restores_tcp_udp() {
    exercise(Case::Legacy).await;
}

#[tokio::test(flavor = "current_thread")]
async fn thin_full_f4096_w128_clean_rebuild_preserves_device_and_restores_tcp_udp() {
    exercise(Case::Thin).await;
}

#[derive(Clone, Copy, Debug)]
enum Case {
    Legacy,
    Thin,
    Continuous,
    Permit,
    Ordered,
}
impl Case {
    fn roles(self) -> usize {
        match self {
            Self::Legacy => 3,
            Self::Thin | Self::Continuous | Self::Permit | Self::Ordered => 4,
        }
    }
}

async fn exercise(case: Case) {
    let roles = case.roles();
    let server = wire::server();
    let mut cfg = wire::config(&server);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    if !matches!(case, Case::Legacy) {
        use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TcpRelayEngine};
        let continuous = matches!(case, Case::Continuous);
        let permit = matches!(case, Case::Permit);
        let ordered = matches!(case, Case::Ordered);
        let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            thin: matches!(case, Case::Thin).then_some("1"),
            continuous: continuous.then_some("1"),
            permit: permit.then_some("1"),
            ordered_egress_permit: ordered.then_some("1"),
            ..Default::default()
        }));
        cfg = cfg.with_relay_policy(policy.clone());
        runtime = runtime.with_relay_policy(policy.clone());
        assert!(
            Arc::ptr_eq(&policy, cfg.relay_policy())
                && Arc::ptr_eq(&policy, runtime.relay_policy())
        );
        assert_eq!(
            policy.generic_engine(),
            if ordered {
                TcpRelayEngine::OrderedEgressPermitPump
            } else if permit {
                TcpRelayEngine::PermitPump
            } else if continuous {
                TcpRelayEngine::ContinuousPump
            } else {
                TcpRelayEngine::ThinStaging
            }
        );
    }
    println!("cleanfullWF case={case:?} originalroles_per_flow={roles}");
    assert_eq!(runtime.tcp_socket_buffer_bytes(), (65535, 65535));
    let (inner, application) = LoopbackTunDevice::bounded_pair(16384, 4);
    let gate = Arc::new(Gate::default());
    let counters = Arc::new(Metrics::default());
    gate.open();
    gate.cut_enabled.store(true, Ordering::Release);
    let mut service = prepare(
        GatedDevice {
            inner,
            gate: gate.clone(),
        },
        cfg.clone(),
        runtime,
        counters.clone(),
        NoopSink,
    );
    let control = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut app = wire::App::new(application);
    let mut peers = Vec::with_capacity(2);
    let mut current = None;
    // This flag belongs to the current physical peer generation. Even a
    // cancelled partial retire may already have reset some original half-pairs.
    let mut retirement_started = false;
    let mut old = None;
    let mut successor_owner = None;
    let mut observation_stage = "before-retirement";
    let mut remote_retire_returned = false;
    let mut app_c_closed = false;
    let scenario = wire::capture(async {
        wire::drive(&mut service, Duration::from_secs(5), async {
            let peer = server.accept().await.unwrap().await.unwrap();
            wire::auth(&peer, &cfg).await;
            peers.push(peer.clone());
            current = Some(wire::Peer::new(peer));
        })
        .await;
        let owner = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        old = Some(owner.clone());
        wire::fill(&mut service, &mut app, current.as_mut().unwrap()).await;
        let full = owner.flow_observation().unwrap();
        assert_eq!(
            (full.live, full.f, full.w, full.attempts, full.running),
            (wire::F, wire::F, 0, wire::F, wire::F * roles)
        );
        assert_eq!(
            (
                full.factory_started,
                full.published,
                full.released,
                full.joined
            ),
            (4096, 4096, 0, 0)
        );
        assert!(full.audit);
        assert_eq!(counters.snapshot(0, 0).relays_spawned, 4096);
        app.connect(wire::F..wire::TOTAL);
        wire::drive(&mut service, Duration::from_secs(5), async {
            while !app.acked(wire::F..wire::TOTAL)
                || owner.flow_observation().unwrap().waiting != wire::W
            {
                app.turn().await;
            }
        })
        .await;
        let waiting: Vec<_> = owner
            .flow_contracts()
            .into_iter()
            .filter(|row| row.waiting)
            .collect();
        assert_eq!(waiting.len(), wire::W);
        assert!(
            waiting
                .iter()
                .all(|row| row.w && !row.f && !row.attempt && !row.published)
        );
        app.start_c();
        wire::drive(&mut service, Duration::from_secs(5), async {
            while !app.c_full() {
                app.turn().await;
            }
        })
        .await;
        let assoc = wire::health(&mut service, &mut app, current.as_mut().unwrap()).await;
        for _ in 0..4 {
            assert!(
                tokio::time::timeout(Duration::from_millis(1), service.run())
                    .await
                    .is_err()
            );
            let after: Vec<_> = owner
                .flow_contracts()
                .into_iter()
                .filter(|row| row.waiting)
                .collect();
            assert_eq!(
                after, waiting,
                "same128 original waiting identities/deadlines after cancelled borrows"
            );
        }
        let pressure = owner.flow_observation().unwrap();
        println!("saved actualfullWF {pressure:?} originalUDPassoc={assoc}");
        assert_eq!(
            (
                pressure.live,
                pressure.f,
                pressure.w,
                pressure.waiting,
                pressure.attempts,
                pressure.running
            ),
            (4224, 4096, 128, 128, 4096, wire::F * roles)
        );
        assert_eq!(
            (pressure.reserved, pressure.escrow_bytes),
            (128 * 256 * 1024, 128)
        );
        assert_eq!(
            (
                pressure.factory_started,
                pressure.published,
                pressure.released,
                pressure.joined,
                pressure.retry
            ),
            (4096, 4096, 0, 0, 0)
        );
        assert!(pressure.audit && app.c_full());
        assert_eq!(
            counters.snapshot(0, 0).relays_spawned,
            4096,
            "Waiting128 and C do not count as publication"
        );
        assert!(
            current
                .as_ref()
                .unwrap()
                .connection
                .close_reason()
                .is_none()
        );
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined());
        assert!(
            waiting
                .iter()
                .all(|r| std::time::Instant::now() < r.open_deadline)
        );
        let requested_at = tokio::time::Instant::now();
        assert!(control.request_rebuild());
        assert!(!control.request_rebuild());
        println!("B31 timing actual request_rebuild bracket start={requested_at:?} returned={:?}", tokio::time::Instant::now());
        retirement_started = true;
        observation_stage = "retire15";
        wire::drive(&mut service, Duration::from_secs(15), async {
            println!("B31 timing inside original retire15 work now={:?}", tokio::time::Instant::now());
            let remote = async {
                current.as_mut().unwrap().retire().await;
                remote_retire_returned = true;
            };
            let application = async {
                while !app.c_closed() {
                    app.turn().await;
                }
                app_c_closed = true;
            };
            tokio::join!(remote, application);
        })
        .await;
        observation_stage = "successorIncoming";
        println!(
            "B31 diagnostic retire15 returned remote={remote_retire_returned} appC={app_c_closed}"
        );
        wire::drive(&mut service, Duration::from_secs(5), async {
            let now = tokio::time::Instant::now();
            let limit = owner.ledger().shutdown_deadline;
            let remaining = limit.map(|limit| limit.saturating_duration_since(now));
            println!("B31 timing inside original successor5 work now={now:?} originalOwnerLimit={limit:?} remaining={remaining:?}");
            let incoming = server.accept().await.unwrap();
            observation_stage = "oldPeerClosed";
            tokio::time::timeout(Duration::from_secs(2), peers[0].closed())
                .await
                .unwrap();
            observation_stage = "oldLedger";
            assert!(owner.business_complete.load(Ordering::Acquire));
            let final_old = owner.flow_observation().unwrap();
            println!("actualfullWF old settled {final_old:?}");
            assert_eq!(
                (
                    final_old.live,
                    final_old.w,
                    final_old.f,
                    final_old.reserved,
                    final_old.escrow_bytes,
                    final_old.running
                ),
                (0, 0, 0, 0, 0, 0)
            );
            assert_eq!(
                (
                    final_old.factory_started,
                    final_old.published,
                    final_old.released,
                    final_old.joined
                ),
                (4096, 4096, 4224, (wire::F * roles) as u64)
            );
            assert!(final_old.audit && owner.ledger().first_failure.is_none());
            assert_eq!(
                (
                    gate.cuts.load(Ordering::Acquire),
                    gate.joins.load(Ordering::Acquire),
                    gate.drops.load(Ordering::Acquire)
                ),
                (1, 0, 0)
            );
            app.restart();
            observation_stage = "successorHandshake";
            let peer = incoming.await.unwrap();
            wire::auth(&peer, &cfg).await;
            peers.push(peer.clone());
            current = Some(wire::Peer::new(peer));
            retirement_started = false;
            observation_stage = "successorInstalled";
        })
        .await;
        wire::drive(&mut service, Duration::from_secs(5), async {
            let remote = current.as_mut().unwrap().accept_to(1);
            let application = async {
                while !app.acked(0..1) {
                    app.turn().await;
                }
            };
            tokio::join!(remote, application);
        })
        .await;
        wire::health(&mut service, &mut app, current.as_mut().unwrap()).await;
        assert_eq!(
            counters.snapshot(0, 0).relays_spawned,
            4097,
            "same counter across sole successor"
        );
        assert_eq!(
            (
                gate.cuts.load(Ordering::Acquire),
                gate.resumes.load(Ordering::Acquire),
                gate.resumed.load(Ordering::Acquire)
            ),
            (1, 1, 1)
        );
        let successor = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(!Arc::ptr_eq(&owner, &successor));
        assert_eq!(successor.flow_observation().unwrap().published, 1);
        if !matches!(case, Case::Legacy) {
            let actual = successor.flow_observation().unwrap();
            println!("Thin fullWF successor running {actual:?}");
            assert_eq!(actual.running, roles);
            let first_none = successor.ledger().first_failure.is_none();
            assert!(first_none && actual.audit);
            successor_owner = Some(successor.clone());
        }
        assert!(!control.request_rebuild());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.accept())
                .await
                .is_err()
        );
    })
    .await;
    if scenario.is_err() {
        let phase = *saved
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let parent = *saved
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stopping = saved.stop.stopping.load(Ordering::Acquire);
        let device = (
            gate.cuts.load(Ordering::Acquire),
            gate.resumes.load(Ordering::Acquire),
            gate.polls.load(Ordering::Acquire),
            gate.joins.load(Ordering::Acquire),
            gate.drops.load(Ordering::Acquire),
        );
        let peer = current
            .as_ref()
            .map(|p| (p.count(), p.connection.close_reason()));
        println!(
            "B31 diagnostic BEFORE parent stop stage={observation_stage} remoteReturned={remote_retire_returned} appC={app_c_closed} phase={phase:?} stopping={stopping} parent={parent:?} cutsResumesDpollJoinDrop={device:?} peer={peer:?}"
        );
        if let Some(owner) = &old {
            let observation = owner.flow_observation();
            let state = owner.state();
            let facts = {
                let ledger = owner.ledger();
                (
                    ledger.shutdown_deadline,
                    ledger.quarantined(),
                    ledger
                        .first_failure
                        .as_ref()
                        .map(|e| format!("{:?}", e.facts())),
                )
            };
            let business = owner.business_complete.load(Ordering::Acquire);
            let driver = owner.driver_complete.load(Ordering::Acquire);
            println!(
                "B31 diagnostic original owner state={state:?} business={business} driver={driver} deadlineQfirst={facts:?} inventory={observation:?}"
            );
        }
    }
    control.request_stop();
    if scenario.is_err() && retirement_started {
        println!(
            "partial peer retirement Unknown; drop remaining original halves, never retire twice"
        );
        drop(current.take());
    }
    let cleanup = wire::capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), async {
            let remote = async {
                if let Some(peer) = &mut current {
                    assert!(
                        !retirement_started,
                        "one retirement per physical generation"
                    );
                    retirement_started = true;
                    peer.retire().await;
                }
            };
            let (report, ()) = tokio::join!(service.run(), remote);
            report
        })
        .await
        .unwrap();
        println!("saved fullWF final {report:?}");
        assert!(report.device_settled() && !report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
            assert_eq!(
                counters.snapshot(0, 0).relays_spawned,
                4097,
                "stop never counts again"
            );
            let done = service.completion.as_ref().unwrap();
            done._retired
                .as_ref()
                .unwrap()
                .node
                .assert_service_clean_for_test();
            done._node.as_ref().unwrap().assert_service_clean_for_test();
            assert_eq!(
                old.as_ref().unwrap().flow_observation().unwrap().released,
                4224
            );
            if !matches!(case, Case::Legacy) {
                let successor = successor_owner.as_ref().unwrap();
                let final_successor = successor.flow_observation().unwrap();
                let first_none = successor.ledger().first_failure.is_none();
                println!("Thin fullWF final successor {final_successor:?} firstNone={first_none}");
                assert_eq!(
                    (
                        final_successor.live,
                        final_successor.w,
                        final_successor.f,
                        final_successor.reserved,
                        final_successor.retained_capacity,
                        final_successor.escrow_bytes,
                        final_successor.running,
                        final_successor.attempts,
                        final_successor.opening_providers,
                        final_successor.waiting,
                        final_successor.stopped_producers,
                        final_successor.settled_providers
                    ),
                    (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
                );
                assert_eq!(
                    (
                        final_successor.factory_started,
                        final_successor.published,
                        final_successor.released,
                        final_successor.joined,
                        final_successor.retained_identity_faults
                    ),
                    (1, 1, 1, roles as u64, 0)
                );
                assert!(first_none && final_successor.audit);
            }
        }
        assert_eq!(
            (
                gate.joins.load(Ordering::Acquire),
                gate.drops.load(Ordering::Acquire)
            ),
            (1, 1)
        );
        assert_eq!(control.observe_shutdown().await, Phase::Complete);
    })
    .await;
    drop(current);
    drop(service);
    let drained = wire::capture(wire::drain(server, peers)).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drained {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_full_f4096_w128_clean_rebuild_preserves_device_and_restores_tcp_udp() {
    exercise(Case::Continuous).await;
}

#[tokio::test(flavor = "current_thread")]
async fn permit_full_f4096_w128_clean_rebuild_preserves_device_and_restores_tcp_udp() {
    exercise(Case::Permit).await;
}

#[tokio::test(flavor = "current_thread")]
async fn ordered_full_f4096_w128_clean_rebuild_preserves_device_and_restores_tcp_udp() {
    exercise(Case::Ordered).await;
}

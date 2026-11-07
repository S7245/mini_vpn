//! Full F/W normal stop through the original saved service. The real peer owns
//! all published halves until one STOP/EOF/RESET retirement; the existing device
//! gate then proves business retirement precedes physical device and Node cleanup.
use super::*;
use device::{Gate, GatedDevice};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
#[path = "../../../../../tests/tuic_service/full_fixture.rs"]
mod wire;

fn deadline(control: &Control) -> Option<tokio::time::Instant> {
    *control
        .deadline
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn owner_limit(owner: &SessionControl) -> Option<tokio::time::Instant> {
    owner.ledger().shutdown_deadline
}

#[tokio::test(flavor = "current_thread")]
async fn full_f4096_w128_cancelled_stop_observer_retires_before_device_and_node_without_successor()
{
    exercise(Case::Legacy).await;
}

#[tokio::test(flavor = "current_thread")]
async fn thin_full_f4096_w128_cancelled_stop_observer_retires_before_device_and_node_without_successor(
) {
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
    println!("fullWF normalstop case={case:?} originalroles_per_flow={roles}");
    assert_eq!(runtime.tcp_socket_buffer_bytes(), (65535, 65535));
    let (inner, application) = LoopbackTunDevice::bounded_pair(16384, 4);
    let gate = Arc::new(Gate::default()); // Only physical poll_shutdown is held.
    let metrics = Arc::new(Metrics::default());
    let mut service = prepare(
        GatedDevice {
            inner,
            gate: gate.clone(),
        },
        cfg.clone(),
        runtime,
        metrics.clone(),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut app = wire::App::new(application);
    let mut peers = Vec::with_capacity(1);
    let mut current = None;
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
        wire::fill(&mut service, &mut app, current.as_mut().unwrap()).await;
        app.connect(wire::F..wire::TOTAL);
        wire::drive(&mut service, Duration::from_secs(5), async {
            while !app.acked(wire::F..wire::TOTAL)
                || owner.flow_observation().unwrap().waiting != wire::W
            {
                app.turn().await;
            }
        })
        .await;
        app.start_c();
        wire::drive(&mut service, Duration::from_secs(5), async {
            while !app.c_full() {
                app.turn().await;
            }
        })
        .await;
        let assoc = wire::health(&mut service, &mut app, current.as_mut().unwrap()).await;
        let full = owner.flow_observation().unwrap();
        assert_eq!(
            (
                full.live,
                full.f,
                full.w,
                full.waiting,
                full.attempts,
                full.running
            ),
            (4224, 4096, 128, 128, 4096, wire::F * roles)
        );
        assert_eq!(
            (
                full.factory_started,
                full.published,
                full.released,
                full.joined,
                full.retry
            ),
            (4096, 4096, 0, 0, 0)
        );
        assert_eq!((full.reserved, full.escrow_bytes), (128 * 256 * 1024, 128));
        assert!(full.audit && app.c_full());
        assert_eq!(metrics.snapshot(0, 0).relays_spawned, 4096);
        let contracts = owner.flow_contracts();
        assert_eq!(contracts.len(), 4224);
        let original: BTreeMap<_, _> = contracts.iter().map(|row| (row.id.index, row)).collect();
        assert_eq!(original.len(), 4224);
        assert_eq!(
            contracts
                .iter()
                .filter(|row| row.published && row.f && row.attempt)
                .count(),
            4096
        );
        assert_eq!(
            contracts
                .iter()
                .filter(|row| row.waiting && row.w && !row.f && !row.attempt)
                .count(),
            128
        );
        assert!(owner.ledger().first_failure.is_none());
        println!("fullWF normalstop initial {full:?} healthy originalUDPassoc={assoc}");

        // This very observer is polled, cancelled by borrow only, then completed.
        let mut observer = Box::pin(handle.observe_shutdown());
        assert!(
            tokio::time::timeout(Duration::from_millis(1), observer.as_mut())
                .await
                .is_err()
        );
        let parent_deadline = deadline(&saved).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), service.run())
                .await
                .is_err()
        );
        let owner_deadline = owner_limit(&owner).unwrap();
        for _ in 0..4 {
            assert!(
                tokio::time::timeout(Duration::from_millis(1), observer.as_mut())
                    .await
                    .is_err()
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(1), service.run())
                    .await
                    .is_err()
            );
            assert_eq!(deadline(&saved), Some(parent_deadline));
            assert_eq!(owner_limit(&owner), Some(owner_deadline));
            assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
            let after = owner.flow_contracts();
            let by_index: BTreeMap<_, _> = after.iter().map(|row| (row.id.index, row)).collect();
            for row in &after {
                let before = original
                    .get(&row.id.index)
                    .expect("no new admission after stop");
                assert_eq!(row.id, before.id);
                assert_eq!(row.open_deadline, before.open_deadline);
                assert_eq!(row.slot_deadline, before.slot_deadline);
            }
            for before in contracts.iter().filter(|row| row.published) {
                let row = by_index
                    .get(&before.id.index)
                    .expect("held original provider stays charged");
                assert!(row.f && row.attempt && row.published);
            }
            assert_eq!(current.as_ref().unwrap().count(), 4096);
            assert!(current
                .as_ref()
                .unwrap()
                .connection
                .close_reason()
                .is_none());
            assert_eq!(
                (
                    gate.polls.load(Ordering::Acquire),
                    gate.joins.load(Ordering::Acquire),
                    gate.drops.load(Ordering::Acquire)
                ),
                (0, 0, 0)
            );
            assert!(!handle.request_rebuild());
        }

        // Call retire exactly once. A partially reset peer cannot safely repeat it.
        tokio::time::timeout_at(parent_deadline, async {
            let retire_and_gate = async {
                let remote = current.as_mut().unwrap().retire();
                let application = async {
                    while !app.c_closed() {
                        app.turn().await;
                    }
                };
                tokio::join!(remote, application);
                while gate.polls.load(Ordering::Acquire) == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::select! { biased;
                report = service.run() => panic!("held physical device completed {report:?}"),
                () = retire_and_gate => (),
            }
        })
        .await
        .unwrap();
        let settled = owner.flow_observation().unwrap();
        println!("fullWF actualbusiness beforephysicalD {settled:?}");
        assert_eq!(
            (
                settled.live,
                settled.f,
                settled.w,
                settled.attempts,
                settled.running,
                settled.reserved,
                settled.escrow_bytes,
                settled.waiting
            ),
            (0, 0, 0, 0, 0, 0, 0, 0)
        );
        assert_eq!(
            (
                settled.factory_started,
                settled.published,
                settled.released,
                settled.joined
            ),
            (4096, 4096, 4224, (wire::F * roles) as u64)
        );
        if !matches!(case, Case::Legacy) {
            assert_eq!(
                (
                    settled.retained_identity_faults,
                    settled.retained_capacity,
                    settled.opening_providers,
                    settled.stopped_producers,
                    settled.settled_providers,
                ),
                (0, 0, 0, 0, 0)
            );
        }
        assert!(settled.audit && owner.flow_contracts().is_empty());
        assert!(owner.business_complete.load(Ordering::Acquire));
        assert!(!owner.driver_complete.load(Ordering::Acquire));
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
        assert_eq!(owner_limit(&owner), Some(owner_deadline));
        assert_eq!(deadline(&saved), Some(parent_deadline));
        assert_eq!(current.as_ref().unwrap().count(), 0);
        assert!(current
            .as_ref()
            .unwrap()
            .connection
            .close_reason()
            .is_none());
        assert!(service.completion.is_none());
        assert!(gate.polls.load(Ordering::Acquire) > 0);
        assert_eq!(
            (
                gate.joins.load(Ordering::Acquire),
                gate.drops.load(Ordering::Acquire)
            ),
            (0, 0)
        );

        gate.open(); // No new timeout: actual D join/Drop and Node use original first15.
        let (report, phase) = tokio::time::timeout_at(parent_deadline, async {
            tokio::join!(service.run(), observer.as_mut())
        })
        .await
        .unwrap();
        println!("fullWF normalstop final {report:?} phase={phase:?}");
        assert_eq!(phase, Phase::Complete);
        assert!(report.device_settled() && !report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!(report.rebuilds, 0);
        let done = service.completion.as_ref().unwrap();
        assert!(done._retired.is_none());
        done._node.as_ref().unwrap().assert_service_clean_for_test();
        assert!(owner.driver_complete.load(Ordering::Acquire));
        if !matches!(case, Case::Legacy) {
            let final_inventory = owner.flow_observation().unwrap();
            println!("Thin fullWF normalstop final inventory {final_inventory:?}");
            assert_eq!(
                (
                    final_inventory.live,
                    final_inventory.w,
                    final_inventory.f,
                    final_inventory.attempts,
                    final_inventory.running,
                    final_inventory.reserved,
                    final_inventory.escrow_bytes,
                    final_inventory.waiting,
                ),
                (0, 0, 0, 0, 0, 0, 0, 0)
            );
            assert_eq!(
                (
                    final_inventory.retained_identity_faults,
                    final_inventory.retained_capacity,
                    final_inventory.opening_providers,
                    final_inventory.stopped_producers,
                    final_inventory.settled_providers,
                ),
                (0, 0, 0, 0, 0)
            );
            assert_eq!(
                (
                    final_inventory.factory_started,
                    final_inventory.published,
                    final_inventory.released,
                    final_inventory.joined,
                ),
                (4096, 4096, 4224, (wire::F * roles) as u64)
            );
            assert!(final_inventory.audit && owner.flow_contracts().is_empty());
        }
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
        assert_eq!(owner_limit(&owner), Some(owner_deadline));
        assert_eq!(deadline(&saved), Some(parent_deadline));
        assert_eq!(
            (
                gate.joins.load(Ordering::Acquire),
                gate.drops.load(Ordering::Acquire)
            ),
            (1, 1)
        );
        assert_eq!(
            (
                gate.cuts.load(Ordering::Acquire),
                gate.resumes.load(Ordering::Acquire),
                gate.resumed.load(Ordering::Acquire)
            ),
            (0, 0, 0)
        );
        assert_eq!(metrics.snapshot(0, 0).relays_spawned, 4096);
        assert_eq!(peers.len(), 1);
        assert!(current
            .as_ref()
            .unwrap()
            .connection
            .close_reason()
            .is_some());
        assert!(!handle.request_rebuild());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.accept())
                .await
                .is_err()
        );
    })
    .await;
    // Original owners survive any scenario unwind. Never retry Peer.retire: a
    // partially reset vector has no progress cursor. Drop remaining original
    // halves, then drive the saved service only to its already established limit.
    let cleanup = if scenario.is_err() {
        drop(current.take());
        gate.open();
        handle.request_stop();
        let deadline = deadline(&saved).unwrap();
        Some(
            wire::capture(async {
                let report = tokio::time::timeout_at(deadline, service.run()).await;
                println!("fullstop secondary cleanup {report:?}");
            })
            .await,
        )
    } else {
        None
    };
    drop(current);
    drop(service);
    let drained = wire::capture(wire::drain(server, peers)).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Some(Err(error)) = cleanup {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drained {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_full_f4096_w128_cancelled_stop_observer_retires_before_device_and_node_without_successor(
) {
    exercise(Case::Continuous).await;
}

#[tokio::test(flavor = "current_thread")]
async fn permit_full_f4096_w128_cancelled_stop_observer_retires_before_device_and_node_without_successor(
) {
    exercise(Case::Permit).await;
}

#[tokio::test(flavor = "current_thread")]
async fn ordered_full_f4096_w128_cancelled_stop_observer_retires_before_device_and_node_without_successor(
) {
    exercise(Case::Ordered).await;
}

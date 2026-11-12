//! Full published/Waiting retirement followed by a real ALPN-denied successor.
//! Owns only test endpoints/app observation; the saved production service owns
//! the original paused device, namespace, retired Node and rejected Node cleanup.
use super::*;
use device::{Gate, GatedDevice};
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
async fn full_f4096_w128_clean_retirement_then_real_alpn_rejection_keeps_original_device() {
    exercise(Case::Legacy).await;
}

#[tokio::test(flavor = "current_thread")]
async fn thin_full_f4096_w128_retirement_then_real_alpn_rejection_keeps_original_device() {
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
    println!("fullWF ALPNrejection case={case:?} originalroles_per_flow={roles}");
    assert_eq!(runtime.tcp_socket_buffer_bytes(), (65535, 65535));
    let (inner, application) = LoopbackTunDevice::bounded_pair(16384, 4);
    let gate = Arc::new(Gate::default());
    gate.cut_enabled.store(true, Ordering::Release);
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
        println!("fullWF before actualsuccessor ALPN refusal {full:?} healthyUDPassoc={assoc}");
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
        assert_eq!((full.reserved, full.escrow_bytes), (128 * 256 * 1024, 128));
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
        assert!(full.audit && app.c_full());
        let contracts = owner.flow_contracts();
        assert_eq!(contracts.len(), 4224);
        assert_eq!(
            contracts
                .iter()
                .filter(|r| r.published && r.f && r.attempt)
                .count(),
            4096
        );
        assert_eq!(
            contracts
                .iter()
                .filter(|r| r.waiting && r.w && !r.f && !r.attempt)
                .count(),
            128
        );
        assert_eq!(metrics.snapshot(0, 0).relays_spawned, 4096);
        assert!(owner.ledger().first_failure.is_none());
        assert!(deadline(&saved).is_none());
        server.set_server_config(Some(server.rejected_alpn()));
        assert!(handle.request_rebuild());
        assert!(!handle.request_rebuild());
        // Do not observe_shutdown here: its stop would preempt the rebuild.
        assert!(
            tokio::time::timeout(Duration::from_millis(1), service.run())
                .await
                .is_err()
        );
        let old_deadline = owner_limit(&owner).unwrap();
        let incoming = tokio::time::timeout_at(old_deadline, async {
            let remote = async {
                let retire = current.as_mut().unwrap().retire(); // Exactly once.
                let application = async {
                    while !app.c_closed() {
                        app.turn().await;
                    }
                };
                tokio::join!(retire, application);
                server.accept().await.unwrap()
            };
            tokio::select! {
                incoming = remote => incoming,
                report = service.run() => panic!("paused device ended early {report:?}"),
            }
        })
        .await
        .unwrap();
        let retired = owner.flow_observation().unwrap();
        println!("fullWF actualold business BEFORE deniedIncomingawait {retired:?}");
        assert_eq!(
            (
                retired.live,
                retired.f,
                retired.w,
                retired.attempts,
                retired.running,
                retired.reserved,
                retired.escrow_bytes,
                retired.waiting
            ),
            (0, 0, 0, 0, 0, 0, 0, 0)
        );
        assert_eq!(
            (
                retired.factory_started,
                retired.published,
                retired.released,
                retired.joined
            ),
            (4096, 4096, 4224, (wire::F * roles) as u64)
        );
        if !matches!(case, Case::Legacy) {
            assert_eq!(
                (
                    retired.retained_identity_faults,
                    retired.retained_capacity,
                    retired.opening_providers,
                    retired.stopped_producers,
                    retired.settled_providers,
                ),
                (0, 0, 0, 0, 0)
            );
        }
        assert!(retired.audit && owner.flow_contracts().is_empty());
        assert!(owner.business_complete.load(Ordering::Acquire));
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined());
        assert_eq!(owner_limit(&owner), Some(old_deadline));
        assert_eq!(current.as_ref().unwrap().count(), 0);
        assert!(current
            .as_ref()
            .unwrap()
            .connection
            .close_reason()
            .is_some());
        assert_eq!(
            (
                gate.cuts.load(Ordering::Acquire),
                gate.resumes.load(Ordering::Acquire),
                gate.joins.load(Ordering::Acquire),
                gate.drops.load(Ordering::Acquire)
            ),
            (1, 0, 0, 0)
        );
        let denied = wire::drive(&mut service, Duration::from_secs(5), async move {
            incoming.await
        })
        .await;
        match denied {
            Err(quinn::ConnectionError::TransportError(error)) => {
                // The real TLS no_application_protocol alert is QUIC CRYPTO_ERROR(120).
                assert_eq!(error.code, quinn::TransportErrorCode::crypto(120));
                println!("fullWF real successor ALPN refusal {error:?}");
            }
            other => panic!("expected actual ALPN transport rejection: {other:?}"),
        }
        wire::drive(&mut service, Duration::from_secs(5), async {
            while gate.polls.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        let rejected_owner = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(!Arc::ptr_eq(&owner, &rejected_owner));
        assert!(rejected_owner.business_complete.load(Ordering::Acquire));
        assert!(!rejected_owner.driver_complete.load(Ordering::Acquire));
        // Startup's configuration cause belongs to NodeHost, not this ledger.
        assert!(rejected_owner.ledger().first_failure.is_none());
        let new_deadline = owner_limit(&rejected_owner).unwrap();
        let parent_deadline = deadline(&saved).unwrap();
        let clocks = *saved.namespace_clocks.lock().unwrap();
        assert!(clocks[0].is_some());
        assert_eq!(clocks[0], clocks[1]);
        let empty = rejected_owner.flow_observation().unwrap();
        assert_eq!(
            (
                empty.live,
                empty.f,
                empty.w,
                empty.factory_started,
                empty.published
            ),
            (0, 0, 0, 0, 0)
        );
        assert!(empty.audit);
        if !matches!(case, Case::Legacy) {
            assert_eq!(
                (
                    empty.attempts,
                    empty.running,
                    empty.reserved,
                    empty.escrow_bytes,
                    empty.waiting,
                    empty.retained_identity_faults,
                    empty.retained_capacity,
                ),
                (0, 0, 0, 0, 0, 0, 0)
            );
            assert_eq!(
                (
                    empty.opening_providers,
                    empty.stopped_producers,
                    empty.settled_providers,
                    empty.released,
                    empty.joined,
                ),
                (0, 0, 0, 0, 0)
            );
            assert!(rejected_owner.flow_contracts().is_empty());
        }
        let mut observer = Box::pin(handle.observe_shutdown());
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
            assert_eq!(owner_limit(&owner), Some(old_deadline));
            assert_eq!(owner_limit(&rejected_owner), Some(new_deadline));
            assert_eq!(deadline(&saved), Some(parent_deadline));
            assert!(
                owner.ledger().first_failure.is_none()
                    && rejected_owner.ledger().first_failure.is_none()
            );
            assert!(service.completion.is_none());
            assert_eq!(
                (
                    gate.cuts.load(Ordering::Acquire),
                    gate.resumes.load(Ordering::Acquire),
                    gate.resumed.load(Ordering::Acquire),
                    gate.joins.load(Ordering::Acquire),
                    gate.drops.load(Ordering::Acquire)
                ),
                (1, 0, 0, 0, 0)
            );
            assert!(!handle.request_rebuild());
            assert_eq!(metrics.snapshot(0, 0).relays_spawned, 4096);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.accept())
                .await
                .is_err()
        );
        gate.open();
        let (report, phase) = tokio::time::timeout_at(parent_deadline, async {
            tokio::join!(service.run(), observer.as_mut())
        })
        .await
        .unwrap();
        println!("fullWF configurationrejected actualfinal {report:?} phase={phase:?}");
        assert_eq!(phase, Phase::Complete);
        assert!(report.device_settled() && !report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::FailedSettled));
        assert_eq!(report.rebuilds, 1); // Budget consumed at old Allowed→Retired, not DATA success.
        let done = service.completion.as_mut().unwrap();
        let old = done._retired.as_ref().unwrap();
        assert!(Arc::ptr_eq(&old.control, &owner));
        old.node.assert_service_clean_for_test();
        assert!(done
            ._node
            .as_mut()
            .unwrap()
            .controlled_rebuild_disposition()
            .configuration_rejected_for_test());
        assert!(done.control.ledger().first_failure.is_none());
        assert!(Arc::ptr_eq(&done.control, &rejected_owner));
        assert!(rejected_owner.driver_complete.load(Ordering::Acquire));
        if !matches!(case, Case::Legacy) {
            let final_inventory = rejected_owner.flow_observation().unwrap();
            println!("Thin fullWF ALPNrejection final inventory {final_inventory:?}");
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
                (0, 0, 0, 0)
            );
            assert!(final_inventory.audit && rejected_owner.flow_contracts().is_empty());
            assert!(!rejected_owner.ledger().quarantined());
        }
        assert_eq!(owner_limit(&owner), Some(old_deadline));
        assert_eq!(owner_limit(&rejected_owner), Some(new_deadline));
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
            (1, 0, 0)
        );
        assert_eq!(metrics.snapshot(0, 0).relays_spawned, 4096);
        assert!(!handle.request_rebuild());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.accept())
                .await
                .is_err()
        );
    })
    .await;
    let cleanup = if scenario.is_err() {
        // Never repeat a partially completed retire. Keep original Service alive.
        drop(current.take());
        gate.open();
        handle.request_stop();
        let deadline = deadline(&saved).unwrap();
        Some(
            wire::capture(async {
                let report = tokio::time::timeout_at(deadline, service.run()).await;
                println!("fullrejection secondary cleanup {report:?}");
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
async fn continuous_full_f4096_w128_retirement_then_real_alpn_rejection_keeps_original_device() {
    exercise(Case::Continuous).await;
}

#[tokio::test(flavor = "current_thread")]
async fn permit_full_f4096_w128_retirement_then_real_alpn_rejection_keeps_original_device() {
    exercise(Case::Permit).await;
}

#[tokio::test(flavor = "current_thread")]
async fn ordered_full_f4096_w128_retirement_then_real_alpn_rejection_keeps_original_device() {
    exercise(Case::Ordered).await;
}

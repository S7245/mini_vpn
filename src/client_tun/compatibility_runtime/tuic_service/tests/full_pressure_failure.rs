//! Full original F/W custody under a controlled actual TunIo read failure.
//! Every original peer half stays held through Q: actor join never substitutes
//! for provider Bi retirement, Node cleanup or actual device shutdown.
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

fn first_error(owner: &SessionControl) -> (usize, String, Vec<u8>) {
    let snapshot = {
        let ledger = owner.ledger();
        ledger.first_failure.as_ref().map(|error| {
            (
                error.facts().raw_os_code,
                error.facts().origin.phase == FailurePhase::DeviceRead,
                error as *const _ as usize,
                format!("{:?}", error.facts()),
                error.text_bytes().to_vec(),
            )
        })
    };
    let (raw_os_code, device_read, address, facts, text) =
        snapshot.expect("original retained DeviceRead failure");
    assert_eq!(raw_os_code, Some(5));
    assert!(device_read);
    (address, facts, text)
}

#[tokio::test(flavor = "current_thread")]
async fn actual_full_f4096_w128_read_fatal_retains_every_published_provider_and_rejects_successor()
{
    exercise(Case::Legacy).await;
}

#[tokio::test(flavor = "current_thread")]
async fn thin_full_f4096_w128_read_fatal_keeps_original_providers_charged() {
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
    println!("fullWF readfatal case={case:?} originalroles_per_flow={roles}");
    assert_eq!(runtime.tcp_socket_buffer_bytes(), (65535, 65535));
    let (inner, application) = LoopbackTunDevice::bounded_pair(16384, 4);
    let gate = Arc::new(Gate::default());
    gate.open();
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
    let scenario = async {
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
        assert_eq!(current.as_ref().unwrap().count(), 4096);
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
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
        println!("actualfullWF beforecontrolledreadfatal {full:?} originalUDPassoc={assoc}");
        gate.read_error.store(true, Ordering::Release);
        app.prepare_health(); // Real original app ingress wakes a pending TunIo read.
        wire::drive(&mut service, Duration::from_secs(5), async {
            while owner.ledger().first_failure.is_none() {
                app.turn().await;
            }
        })
        .await;
        assert!(!gate.read_error.load(Ordering::Acquire));
        let first = first_error(&owner);
        let owner_deadline = owner_limit(&owner).unwrap();
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
        }
        let parent_deadline = deadline(&saved).unwrap();
        let phase = tokio::select! {biased;
            report=service.run()=>panic!("fullfatal cannotcomplete {report:?}"),
            phase=observer.as_mut()=>phase,
        };
        match phase {
            Phase::Incomplete => assert!(tokio::time::Instant::now() >= parent_deadline),
            Phase::Quarantined => assert!(owner.ledger().quarantined),
            other => panic!("fullfatal falselysettled {other:?}"),
        }
        println!(
            "fullfatal originalownerfirst15={owner_deadline:?} parentfirst15={parent_deadline:?} sameobserver={phase:?}"
        );
        // Original observer ended above. This diagnostic cap grants no new
        // business deadline, cleanup allowance or permission to create a successor.
        wire::drive(&mut service, Duration::from_secs(5), async {
            while handle.phase() != Phase::Quarantined {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        for _ in 0..4 {
            assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), service.run())
                    .await
                    .is_err()
            );
            assert_eq!(first_error(&owner), first);
            assert_eq!(owner_limit(&owner), Some(owner_deadline));
            assert_eq!(deadline(&saved), Some(parent_deadline));
            assert!(owner.ledger().quarantined);
            assert!(!owner.business_complete.load(Ordering::Acquire));
            assert!(!owner.driver_complete.load(Ordering::Acquire));
            assert!(service.completion.is_none());
            let after = owner.flow_contracts();
            let by_index: BTreeMap<_, _> = after.iter().map(|row| (row.id.index, row)).collect();
            assert_eq!(after.len(), by_index.len());
            for row in &after {
                let before = original
                    .get(&row.id.index)
                    .expect("no new flow after fatal");
                assert_eq!(row.id, before.id);
                assert_eq!(row.open_deadline, before.open_deadline);
                assert_eq!(row.slot_deadline, before.slot_deadline);
            }
            for before in contracts.iter().filter(|row| row.published) {
                let row = by_index
                    .get(&before.id.index)
                    .expect("EVERY originalpublished provider mustremaincharged");
                assert!(row.f && row.attempt && row.published);
            }
            let missing_waiting = contracts
                .iter()
                .filter(|row| row.waiting && !by_index.contains_key(&row.id.index))
                .count();
            let state = owner.flow_observation().unwrap();
            assert!(state.audit);
            assert_eq!(state.f, 4096);
            assert_eq!(state.attempts, 4096);
            assert!((4096..=4224).contains(&state.live));
            assert!(state.w <= 128);
            assert_eq!(state.live, after.len());
            assert_eq!(state.live + state.released as usize, 4224);
            assert_eq!(state.released as usize, missing_waiting);
            assert_eq!(
                (state.factory_started, state.published, state.retry),
                (4096, 4096, 0)
            );
            assert_eq!(
                state.running as u64 + state.joined,
                (wire::F * roles) as u64
            );
            assert_eq!(metrics.snapshot(0, 0).relays_spawned, 4096);
            assert_eq!(
                (
                    gate.polls.load(Ordering::Acquire),
                    gate.joins.load(Ordering::Acquire),
                    gate.drops.load(Ordering::Acquire)
                ),
                (0, 0, 0)
            );
            assert_eq!(
                (
                    gate.cuts.load(Ordering::Acquire),
                    gate.resumes.load(Ordering::Acquire),
                    gate.resumed.load(Ordering::Acquire)
                ),
                (0, 0, 0)
            );
            assert_eq!(current.as_ref().unwrap().count(), 4096);
            assert!(current
                .as_ref()
                .unwrap()
                .connection
                .close_reason()
                .is_none());
            assert!(!handle.request_rebuild());
            println!(
                "actualfullfatal chargedledger {state:?}; all4096originalpeerhalves held, providerphysicalretirement NOT proved byactorsjoined"
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.accept())
                .await
                .is_err()
        );
    };
    let result = wire::capture(scenario).await;
    // Deliberate forced boundary, only AFTER all original Q observations above.
    // Server drain below is real; it never certifies client/Node async-clean.
    drop(current);
    drop(service);
    let drained = wire::capture(wire::drain(server, peers)).await;
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drained {
        std::panic::resume_unwind(error);
    }
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_full_f4096_w128_read_fatal_keeps_original_providers_charged() {
    exercise(Case::Continuous).await;
}

#[tokio::test(flavor = "current_thread")]
async fn permit_full_f4096_w128_read_fatal_keeps_original_providers_charged() {
    exercise(Case::Permit).await;
}

#[tokio::test(flavor = "current_thread")]
async fn ordered_full_f4096_w128_read_fatal_keeps_original_providers_charged() {
    exercise(Case::Ordered).await;
}

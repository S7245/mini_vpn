//! Running full F/W service: one real remote reset returns one physical slot;
//! only one original Waiting is admitted while useful siblings/typed UDP remain.
//! Original peer owns all other half-pairs. No synthetic release, ACK or provider.
use super::*;
use device::{Gate, GatedDevice};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
#[path = "../../../../../tests/tuic_service/full_fixture.rs"]
mod wire;

fn first_error(owner: &SessionControl) -> Option<(usize, String, Vec<u8>)> {
    let ledger = owner.ledger();
    ledger.first_failure.as_ref().map(|error| {
        (
            error as *const _ as usize,
            format!("{:?}", error.facts()),
            error.text_bytes().to_vec(),
        )
    })
}

#[tokio::test(flavor = "current_thread")]
async fn full_f4096_w128_single_reset_retries_only_original_waiting_and_keeps_healthy_tcp_udp() {
    // Heap scenario: the stack-pinned test future stays small (D94).
    Box::pin(exercise(Case::Legacy)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn thin_full_f4096_w128_reset_keeps_original_error_and_healthy_tcp_udp() {
    // Heap scenario: the stack-pinned test future stays small (D94).
    Box::pin(exercise(Case::Thin)).await;
}

#[derive(Clone, Copy, Debug)]
enum Case {
    Legacy,
    Thin,
    Continuous,
    Permit,
    Ordered,
    OrderedFair,
    LegacyFair,
    ThinFair,
    ContinuousFair,
    PermitFair,
}
impl Case {
    fn roles(self) -> usize {
        match self {
            Self::Legacy | Self::LegacyFair => 3,
            Self::Thin
            | Self::Continuous
            | Self::Permit
            | Self::Ordered
            | Self::OrderedFair
            | Self::ThinFair
            | Self::ContinuousFair
            | Self::PermitFair => 4,
        }
    }
    fn legacy(self) -> bool {
        matches!(self, Self::Legacy | Self::LegacyFair)
    }
    fn fair(self) -> bool {
        matches!(
            self,
            Self::OrderedFair
                | Self::LegacyFair
                | Self::ThinFair
                | Self::ContinuousFair
                | Self::PermitFair
        )
    }
}

fn strict_reset_error(owner: &SessionControl, index: usize, epoch: u64) {
    use crate::shared::errors::{FailureClass, FailurePhase, FailureResource};
    // The guard is released before any assertion, logging or cleanup can panic.
    let actual = {
        let ledger = owner.ledger();
        ledger.first_failure.as_ref().map(|error| {
            let facts = error.facts();
            (
                facts.origin.phase == FailurePhase::TcpOpen,
                facts.origin.resource == FailureResource::TcpOpen,
                facts.origin.resource_id,
                facts.origin.attempt_id,
                facts.class == FailureClass::Io,
                facts.io_kind,
            )
        })
    };
    assert_eq!(
        actual,
        Some((
            true,
            true,
            index as u64,
            Some(epoch),
            true,
            Some(std::io::ErrorKind::ConnectionReset)
        )),
        "actual original reset first failure, not peer stream identity"
    );
}

async fn exercise(case: Case) {
    let roles = case.roles();
    let server = wire::server();
    let mut cfg = wire::config(&server);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    if !matches!(case, Case::Legacy) {
        use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TcpRelayEngine};
        let continuous = matches!(case, Case::Continuous | Case::ContinuousFair);
        let permit = matches!(case, Case::Permit | Case::PermitFair);
        let ordered = matches!(case, Case::Ordered | Case::OrderedFair);
        let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            thin: matches!(case, Case::Thin | Case::ThinFair).then_some("1"),
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
            if case.legacy() {
                TcpRelayEngine::Legacy
            } else if ordered {
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
    println!("fullWF case={case:?} originalroles_per_flow={roles}");
    assert_eq!(runtime.tcp_socket_buffer_bytes(), (65535, 65535));
    let (inner, application) = LoopbackTunDevice::bounded_pair(16384, 4);
    let gate = Arc::new(Gate::default());
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
        }).await;
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        wire::fill(&mut service, &mut app, current.as_mut().unwrap()).await;
        app.connect(wire::F..wire::TOTAL);
        wire::drive(&mut service, Duration::from_secs(5), async {
            while !app.acked(wire::F..wire::TOTAL)
                || owner.flow_observation().unwrap().waiting != wire::W {
                app.turn().await;
            }
        }).await;
        app.start_c();
        wire::drive(&mut service, Duration::from_secs(5), async {
            while !app.c_full() { app.turn().await; }
        }).await;
        let assoc = wire::health(&mut service, &mut app, current.as_mut().unwrap()).await;
        let full = owner.flow_observation().unwrap();
        println!("fullWF before nonhealthyreset {full:?} healthyUDPassoc={assoc}");
        assert_eq!((full.live, full.f, full.w, full.waiting, full.attempts, full.running),
            (4224, 4096, 128, 128, 4096, wire::F * roles));
        assert_eq!((full.factory_started, full.published, full.released, full.joined, full.retry),
            (4096, 4096, 0, 0, 0));
        assert_eq!((full.reserved, full.escrow_bytes), (128 * 256 * 1024, 128));
        assert!(full.audit && app.c_full());
        assert!(first_error(&owner).is_none());
        let contracts = owner.flow_contracts();
        let original: BTreeMap<_, _> = contracts.iter().map(|r| (r.id.index, r)).collect();
        assert_eq!(contracts.len(), 4224);
        assert_eq!(original.len(), 4224);
        assert_eq!(contracts.iter().filter(|r| r.published).count(), 4096);
        assert_eq!(contracts.iter().filter(|r| r.waiting).count(), 128);
        let (reset_stream, accepted) = if case.fair() {
            assert_eq!(crate::tuic::encode_heartbeat(), [0x05, 0x04]);
            let progress = wire::FairProgress::default();
            let mut rounds = [None; wire::FAIR_ROUNDS];
            let mut promoted_at = None;
            let mut accepted_at = None;
            let mut after_promotion = 0;
            let mut observed_first = None;
            let observation_started = tokio::time::Instant::now();
            app.prepare_health();
            let result = wire::drive(&mut service, Duration::from_secs(5), async {
                let remote = current.as_mut().unwrap().fair_reset(&progress, assoc);
                let application = async {
                    let mut completed = 0;
                    loop {
                        app.turn().await;
                        let state = owner.flow_observation().unwrap();
                        let facts = progress.snapshot();
                        assert!(state.audit && state.live <= wire::TOTAL && state.f <= wire::F
                            && state.w <= wire::W && state.running <= wire::F * roles);
                        assert!(state.factory_started <= 4097 && state.published <= 4097 && state.released <= 1);
                        let quarantined = owner.ledger().quarantined;
                        let parent_limit = *saved.deadline.lock().unwrap();
                        let owner_limit = owner.ledger().shutdown_deadline;
                        assert!(!quarantined);
                        assert_eq!(handle.phase(), Phase::Running);
                        assert!(parent_limit.is_none() && owner_limit.is_none());
                        if let Some(first) = first_error(&owner) {
                            if let Some(original) = &observed_first { assert_eq!(&first, original); }
                            else { observed_first = Some(first); }
                        }
                        if facts.accepted.is_some() && accepted_at.is_none() {
                            assert!(completed < wire::FAIR_MAX_ROUNDS);
                            accepted_at = Some(completed);
                        }
                        if facts.accepted.is_some() && state.published == 4097 && state.released == 1
                            && (state.live,state.f,state.w,state.waiting) == (4224,4096,128,128)
                            && promoted_at.is_none() {
                            assert!(completed < wire::FAIR_MAX_ROUNDS);
                            promoted_at = Some(completed);
                        }
                        if app.healthy() {
                            assert!(completed < wire::FAIR_MAX_ROUNDS);
                            if completed < 3 {
                                assert_eq!((state.live,state.f,state.w,state.waiting,state.running),
                                    (4224,4096,128,128,wire::F * roles));
                                assert!(app.c_full() && facts.reset_stream.is_none());
                                assert!(first_error(&owner).is_none());
                            }
                            if promoted_at.is_some() { after_promotion += 1; }
                            rounds[completed % wire::FAIR_ROUNDS] = Some((completed + 1,
                                state.live,state.f,state.w,state.running,state.published,state.released,
                                facts.remote_rounds,facts.accepted));
                            completed += 1;
                            progress.record_app_round(completed, promoted_at.is_some());
                            if facts.finished {
                                assert_eq!(completed, facts.remote_rounds);
                                break;
                            }
                            assert!(completed < wire::FAIR_MAX_ROUNDS,
                                "App cannot prepare a request beyond the bounded workload");
                            app.prepare_health();
                        }
                    }
                };
                let (result, ()) = tokio::join!(remote, application);
                result
            }).await;
            let facts = progress.snapshot();
            assert_eq!(facts.reset_after_request, Some(4));
            assert_eq!(facts.remote_rounds, facts.app_rounds);
            assert!(facts.finished && facts.app_rounds <= wire::FAIR_MAX_ROUNDS
                && facts.post_promotion >= wire::FAIR_ROUNDS);
            assert_ne!(facts.healthy_stream, facts.reset_stream.unwrap());
            assert_eq!(facts.accepted, Some(result.1));
            let joined_root = owner.flow_observation();
            let elapsed = observation_started.elapsed();
            println!("actualfullWF continuousfair diagnostic facts={facts:?} acceptedAtAppRounds={accepted_at:?} promotedAtAppRounds={promoted_at:?} actualCompletedAfterPromotion={after_promotion} boundedRounds={rounds:?} originalFirst={observed_first:?} joinedRoot={joined_root:?} originalSingle5Elapsed={elapsed:?}");
            assert!(promoted_at.is_some() && accepted_at.is_some()
                && after_promotion >= wire::FAIR_ROUNDS);
            assert!(rounds.iter().all(Option::is_some));
            if !case.legacy() { assert!(observed_first.is_some()); }
            assert_eq!(first_error(&owner), observed_first);
            result
        } else {
        let reset_stream = current.as_mut().unwrap().reset_nonhealthy();
        let accepted = wire::drive(&mut service, Duration::from_secs(5), async {
            let remote = current.as_mut().unwrap().accept_one_waiting();
            let application = async {
                loop {
                    app.turn().await;
                    let state = owner.flow_observation().unwrap();
                    assert!(state.audit && state.factory_started <= 4097 && state.published <= 4097);
                    if state.published == 4097 && state.released == 1 && state.waiting == 128 {
                        break;
                    }
                }
            };
            let (accepted, ()) = tokio::join!(remote, application);
            accepted
        }).await;
            (reset_stream, accepted)
        };
        let after = owner.flow_contracts();
        let by_index: BTreeMap<_, _> = after.iter().map(|r| (r.id.index, r)).collect();
        assert_eq!(after.len(), 4224);
        assert_eq!(by_index.len(), 4224);
        let mut missing_published = 0;
        let mut reset_identity = None;
        let mut promoted = 0;
        let mut new_c = 0;
        for before in &contracts {
            let same = by_index.get(&before.id.index).filter(|row| row.id == before.id);
            if before.published {
                if let Some(row) = same {
                    assert!(row.published && row.f && row.attempt);
                } else {
                    missing_published += 1;
                    reset_identity = Some((before.id.index, before.id.epoch));
                }
            } else {
                assert!(before.waiting && before.w && !before.attempt);
                let row = same.expect("every originalWaiting keeps fullIdentity");
                if row.published {
                    promoted += 1;
                    assert!(row.f && row.attempt && !row.waiting);
                } else {
                    assert!(row.waiting && row.w && !row.f && !row.attempt);
                }
            }
            if let Some(row) = same {
                assert_eq!(row.open_deadline, before.open_deadline);
                assert_eq!(row.slot_deadline, before.slot_deadline);
            }
        }
        for row in &after {
            let same = original.get(&row.id.index).is_some_and(|before| before.id == row.id);
            if !same {
                new_c += 1;
                assert!(row.waiting && row.w && !row.f && !row.attempt && !row.published);
            }
        }
        assert_eq!((missing_published, promoted, new_c), (1, 1, 1));
        let resumed = owner.flow_observation().unwrap();
        assert_eq!((resumed.live, resumed.f, resumed.w, resumed.waiting, resumed.attempts,
            resumed.running), (4224, 4096, 128, 128, 4096, wire::F * roles));
        assert_eq!((resumed.factory_started, resumed.published, resumed.released, resumed.joined,
            resumed.retry), (4097, 4097, 1, roles as u64, 0));
        assert!(resumed.audit && !owner.ledger().quarantined);
        assert_eq!(current.as_ref().unwrap().count(), 4096);
        assert_eq!(metrics.snapshot(0, 0).relays_spawned, 4097);
        if !case.legacy() {
            let (index, epoch) = reset_identity.expect("one actual missing Published FlowId");
            strict_reset_error(&owner, index, epoch);
        }
        let first = first_error(&owner);
        if !case.legacy() { assert!(first.is_some(), "Thin actual first reset VALUE retained"); }
        println!("fullWF resetstream={reset_stream} accepted={accepted:?} actualretry {resumed:?} firstVALUE={first:?}");
        let again = wire::health(&mut service, &mut app, current.as_mut().unwrap()).await;
        assert_eq!(again, assoc, "same actual typedUDP association remains useful after reset");
        assert_eq!(first_error(&owner), first);
        assert_eq!(handle.phase(), Phase::Running);
        assert!(saved.deadline.lock().unwrap().is_none());
        assert!(owner.ledger().shutdown_deadline.is_none());
        assert!(!owner.business_complete.load(Ordering::Acquire));
        let still = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert!(Arc::ptr_eq(&still, &owner));
        assert!(current.as_ref().unwrap().connection.close_reason().is_none());
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
        println!("fullWF postreset healthyTCP+sameUDP={again} sameNode/controlRunning, no newIncoming");

        let mut observer = Box::pin(handle.observe_shutdown());
        assert!(tokio::time::timeout(Duration::from_millis(1), observer.as_mut()).await.is_err());
        let parent_deadline = saved.deadline.lock().unwrap().unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(1), service.run()).await.is_err());
        let owner_deadline = owner.ledger().shutdown_deadline.unwrap();
        tokio::time::timeout_at(parent_deadline, async {
            let work = async {
                let remote = current.as_mut().unwrap().retire(); // Once, only surviving4096.
                let application = async {
                    while !app.c_closed() { app.turn().await; }
                };
                tokio::join!(remote, application);
                while gate.polls.load(Ordering::Acquire) == 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            tokio::select! {
                () = work => (),
                report = service.run() => panic!("devicegate bypass {report:?}"),
            }
        }).await.unwrap();
        let settled = owner.flow_observation().unwrap();
        println!("fullWF reset finalbusiness beforeD {settled:?}");
        assert_eq!((settled.live, settled.w, settled.f, settled.reserved, settled.escrow_bytes,
            settled.attempts, settled.running, settled.waiting), (0, 0, 0, 0, 0, 0, 0, 0));
        assert_eq!((settled.factory_started, settled.published, settled.released, settled.joined),
            (4097, 4097, 4225, ((wire::F + 1) * roles) as u64));
        assert!(settled.audit && owner.business_complete.load(Ordering::Acquire));
        assert_eq!(first_error(&owner), first);
        assert_eq!(owner.ledger().shutdown_deadline, Some(owner_deadline));
        assert_eq!(*saved.deadline.lock().unwrap(), Some(parent_deadline));
        assert!(!owner.driver_complete.load(Ordering::Acquire));
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0));
        assert!(current.as_ref().unwrap().connection.close_reason().is_none());
        gate.open();
        let (report, phase) = tokio::time::timeout_at(parent_deadline, async {
            tokio::join!(service.run(), observer.as_mut())
        }).await.unwrap();
        println!("fullWF reset actualfinal {report:?} phase={phase:?} retainedfirst={first:?}");
        assert_eq!(phase, Phase::Complete);
        assert!(report.device_settled());
        assert_eq!(report.device_failed(), first.is_some());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!(report.rebuilds, 0);
        let done = service.completion.as_ref().unwrap();
        done._node.as_ref().unwrap().assert_service_clean_for_test();
        assert!(done._retired.is_none());
        assert_eq!(first_error(&done.control), first);
        assert_eq!(owner.ledger().shutdown_deadline, Some(owner_deadline));
        assert_eq!(*saved.deadline.lock().unwrap(), Some(parent_deadline));
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 1));
        assert_eq!((gate.cuts.load(Ordering::Acquire), gate.resumes.load(Ordering::Acquire)), (0, 0));
        assert_eq!(metrics.snapshot(0, 0).relays_spawned, 4097);
        assert!(!handle.request_rebuild());
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
    }).await;
    let cleanup = if scenario.is_err() {
        drop(current.take()); // No second retire after any partial RESET.
        gate.open();
        handle.request_stop();
        let deadline = saved.deadline.lock().unwrap().unwrap();
        Some(
            wire::capture(async {
                let report = tokio::time::timeout_at(deadline, service.run()).await;
                println!("fullreset secondary cleanup {report:?}");
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
async fn continuous_full_f4096_w128_reset_keeps_original_error_and_healthy_tcp_udp() {
    // Heap scenario: the stack-pinned test future stays small (D94).
    Box::pin(exercise(Case::Continuous)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn permit_full_f4096_w128_reset_keeps_original_error_and_healthy_tcp_udp() {
    // Heap scenario: the stack-pinned test future stays small (D94).
    Box::pin(exercise(Case::Permit)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn ordered_full_f4096_w128_reset_keeps_original_error_and_healthy_tcp_udp() {
    // Heap scenario: the stack-pinned test future stays small (D94).
    Box::pin(exercise(Case::Ordered)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn ordered_full_f4096_w128_continuous_health_overlaps_actual_reset_and_waiting_promotion() {
    // Heap scenario: the stack-pinned test future stays small (D94).
    Box::pin(exercise(Case::OrderedFair)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_full_f4096_w128_continuous_health_overlaps_actual_reset_and_waiting_promotion() {
    // Heap scenario: the stack-pinned test future stays small (D94).
    Box::pin(exercise(Case::LegacyFair)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn thin_full_f4096_w128_continuous_health_overlaps_actual_reset_and_waiting_promotion() {
    // Heap scenario: the stack-pinned test future stays small (D94).
    Box::pin(exercise(Case::ThinFair)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn continuous_full_f4096_w128_continuous_health_overlaps_actual_reset_and_waiting_promotion()
{
    // Heap scenario: the stack-pinned test future stays small (D94).
    Box::pin(exercise(Case::ContinuousFair)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn permit_full_f4096_w128_continuous_health_overlaps_actual_reset_and_waiting_promotion() {
    // Heap scenario: the stack-pinned test future stays small (D94).
    Box::pin(exercise(Case::PermitFair)).await;
}

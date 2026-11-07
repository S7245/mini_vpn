//! Real App abort of a published Native Permit Flow and original peer retirement.
//! The original Root owns five roles, F, queue and Node backing settlement.

use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{Apps, auth, capture, config};

#[tokio::test(flavor = "current_thread")]
async fn native_permit_loaded_app_abort_retires_peer_then_cleans_original_root() {
    local_abort_case(true, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_permit_loaded_app_abort_with_held_peer_keeps_charged_root() {
    local_abort_case(false, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_permit_loaded_app_abort_retires_with_original_receipt() {
    local_abort_case(true, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_permit_loaded_app_abort_keeps_original_held_debt() {
    local_abort_case(false, true).await;
}

async fn local_abort_case(peer_finishes: bool, hard: bool) {
    let profile = fixture::ServiceProfile::new(hard);
    let mut server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        native_egress_permit: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeOrderedPump
    );
    assert_eq!(policy.native_engine(), NativeTcpRelayEngine::PermitPump);
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let mut service = profile.service(inbound, outbound, cfg.clone(), runtime);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    if hard {
        assert!(saved.backing_budget.is_some() && saved.app_meter.is_some());
    }
    let mut peer = None;
    let mut stream = None;
    let mut original_queue = None;
    let mut backing = None;
    let mut original_id = None;
    let mut original_owner = None;

    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_open = async {
                        let (send, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        stream = Some((send, recv));
                    };
                    let app_open = async {
                        loop {
                            app.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            let flow = owner.flow_observation().unwrap();
                            if flow.published == 1 && flow.running == 5 {
                                let contracts = owner.flow_contracts();
                                assert_eq!(contracts.len(), 1);
                                original_id = Some(contracts[0].id);
                                original_owner = Some(owner.clone());
                                original_queue = owner.flow_native_permit_queue();
                                backing = owner.flow_native_backing_meter();
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("Native Permit Service ended before publication: {report:?}"),
            }
        }).await.unwrap();
        let queue = original_queue.as_ref().expect("original Native Permit queue");
        let meter = backing.as_ref().expect("original Node backing meter");
        assert_eq!(queue.custodied_release_mode(),
            Some(crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnEgressDrain));
        let egress_bridge = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap()
            .relay_event_test_bridge().unwrap();
        if peer_finishes {
            egress_bridge.arm_egress_probe(queue.clone());
        }
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    stream.as_mut().unwrap().0.write_all(&[0x5a; 8192]).await.unwrap();
                    loop {
                        let physical = queue.snapshot_now();
                        if physical.queued_bytes + physical.leased_bytes > 0 {
                            assert!(physical.owned_bytes() <= physical.capacity_bytes);
                            assert!(meter.snapshot().payload_bytes > 0);
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                } => {},
                report = service.run() => panic!("Native Permit Service ended before charged DATA: {report:?}"),
            }
        }).await.unwrap();
        assert!(app.tcp_reply.is_empty());

        if peer_finishes {
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                loop {
                    let (samples, overflow, ambiguous) = egress_bridge.egress_probe_samples();
                    assert!(!overflow && !ambiguous, "original Permit cap probe lost identity");
                    if samples.last().is_some_and(|sample| {
                        sample.ledger.cap_count > 0
                            && sample.ledger.admitted > sample.ledger.refunded
                    }) && queue.snapshot_now().leased_bytes > 0 {
                        break;
                    }
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                }
            }).await.expect("original Permit inflight cap not reached before App abort");
        }

        app.sockets.get_mut::<smoltcp::socket::tcp::Socket>(app.tcp).abort();
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                app.turn().await;
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                let flow = owner.flow_observation().unwrap();
                let provider = owner.flow_native_provider_view();
                if flow.joined == 5
                    && provider.is_some_and(|view| view.phase == crate::upstream::TcpOpenPhase::Retiring)
                {
                    assert_eq!((flow.live, flow.published, flow.released, flow.f,
                                flow.running), (1, 1, 0, 1, 0));
                    assert!(flow.audit);
                    assert_eq!(Some(owner.flow_contracts()[0].id), original_id);
                    let provider = provider.unwrap();
                    assert_eq!(provider.phase, crate::upstream::TcpOpenPhase::Retiring);
                    assert!(provider.lease_held && provider.retirement_pending);
                    let physical = queue.snapshot_now();
                    assert!(physical.owned_bytes() <= physical.capacity_bytes);
                    assert!(matches!(physical.closure.terminal_cause(),
                        Some(cause) if cause.direction == "root" && cause.reason == "custody_disposal"));
                    assert!(!owner.ledger().quarantined);
                    break;
                }
            }
        }).await.unwrap_or_else(|elapsed| {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("Native Permit App abort: flow={:?} debug={:?} queue={:?} meter={:?}",
                owner.flow_observation(), owner.flow_debug(), queue.snapshot_now(), meter.snapshot());
            panic!("original Permit abort JOIN/queue: {elapsed}");
        });

        if !peer_finishes {
            handle.request_stop();
            let original_deadline = saved.deadline.lock().unwrap().unwrap();
            let mut observer = Box::pin(handle.observe_shutdown());
            let phase = tokio::time::timeout_at(original_deadline + Duration::from_millis(100), async {
                tokio::select! {
                    phase = observer.as_mut() => phase,
                    report = service.run() => panic!("held Native Permit peer falsely settled: {report:?}"),
                }
            }).await.unwrap();
            assert!(matches!(phase, Phase::Quarantined | Phase::Incomplete));
            if tokio::time::Instant::now() < original_deadline {
                tokio::select! {
                    _ = tokio::time::sleep_until(original_deadline) => {},
                    report = service.run() => panic!("held Native Permit peer completed before deadline: {report:?}"),
                }
            }
            if phase == Phase::Incomplete {
                tokio::time::timeout(Duration::from_secs(1), async {
                    while handle.phase() != Phase::Quarantined {
                        assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                    }
                }).await.unwrap();
            }
            assert_eq!(handle.phase(), Phase::Quarantined);
            assert!(service.completion.is_none());
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let flow = owner.flow_observation().unwrap();
            assert_eq!((flow.live, flow.published, flow.released, flow.f, flow.joined),
                       (1, 1, 0, 1, 5));
            assert!(flow.audit);
            assert!(owner.ledger().quarantined);
            if !hard { assert!(owner.ledger().first_failure.is_none()); }
            assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
            let provider = owner.flow_native_provider_view().unwrap();
            assert_eq!(provider.phase, crate::upstream::TcpOpenPhase::Retiring);
            assert!(provider.lease_held && provider.retirement_pending);
            let physical = queue.snapshot_now();
            assert!(physical.owned_bytes() <= physical.capacity_bytes);
            assert!(physical.queued_bytes + physical.leased_bytes <= physical.owned_bytes());
            if hard {
                assert!(Arc::ptr_eq(original_owner.as_ref().unwrap(), &owner));
                let expected_deadline = owner.ledger().shutdown_deadline;
                assert_eq!(expected_deadline, Some(original_deadline));
                let budget = saved.backing_budget.as_ref().unwrap();
                assert!(meter.snapshot().quarantined && budget.quarantined() && budget.used() > 0);
                let mut first = fixture::hard_q_refusal(&owner, &saved, false);
                for _ in 0..2 {
                    assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
                    assert_eq!(Some(owner.flow_contracts()[0].id), original_id);
                    let current = fixture::hard_q_refusal(&owner, &saved, false);
                    if first.is_some() { assert_eq!(current, first); } else { first = current; }
                    assert_eq!(owner.ledger().shutdown_deadline, expected_deadline);
                    assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                }
            } else { assert!(!meter.snapshot().quarantined); }
            return;
        }

        let (mut send, mut recv) = stream.take().unwrap();
        send.finish().unwrap();
        recv.stop(78u32.into()).unwrap();
        drop((send, recv));
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                app.turn().await;
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                let flow = owner.flow_observation().unwrap();
                if flow.released == 1 || owner.ledger().quarantined {
                    flow.assert_permit_reset_balance(queue);
                    assert_eq!((flow.live, flow.published, flow.released, flow.f, flow.joined),
                        (0, 1, 1, 0, 5));
                    assert!(flow.audit && !owner.ledger().quarantined);
                    assert!(owner.ledger().first_failure.is_none());
                    assert!(queue.physical_zero());
                    assert!(owner.flow_native_provider_view().is_none());
                    if hard { egress_bridge.retire_egress_probe_for_test(); }
                    break;
                }
            }
        }).await.unwrap_or_else(|elapsed| {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("Native Permit peer retirement: flow={:?} debug={:?} queue={:?} meter={:?}",
                owner.flow_observation(), owner.flow_debug(), queue.snapshot_now(), meter.snapshot());
            panic!("original Permit abort peer retirement: {elapsed}");
        });
    }).await;

    let scenario_failed = scenario.is_err();
    if hard && peer_finishes && !scenario_failed {
        drop(original_queue.take());
    }
    if !peer_finishes {
        drop(stream.take());
        if let Some(peer) = peer.take() {
            peer.close(0u32.into(), b"done");
        }
        let drain = server.take_driver_drain_handle().unwrap();
        server.close(0u32.into(), b"done");
        drop(server);
        let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
        if let Err(panic) = scenario {
            std::panic::resume_unwind(panic);
        }
        assert!(drained.unwrap());
        return;
    }
    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        if scenario_failed {
            fixture::observe_failed_native_stop(
                service.run(),
                &handle,
                original_deadline,
                original_queue.as_ref(),
            )
            .await;
            return;
        }
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        let owner = service.completion.as_ref().unwrap().control.clone();
        let flow = owner.flow_observation().unwrap();
        assert_eq!(
            (flow.live, flow.published, flow.released, flow.joined),
            (0, 1, 1, 5)
        );
        assert!(flow.audit);
        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
        if !hard {
            assert!(original_queue.as_ref().unwrap().physical_zero());
        } else {
            assert!(original_queue.is_none());
        }
        let physical = backing.as_ref().unwrap().snapshot();
        assert_eq!(
            (
                physical.allocations,
                physical.payload_bytes,
                physical.physical_bytes,
                physical.quarantined
            ),
            (0, 0, 0, false)
        );
        if hard {
            assert!(Arc::ptr_eq(original_owner.as_ref().unwrap(), &owner));
            assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
            assert_eq!(owner.ledger().shutdown_deadline, Some(original_deadline));
            drop(app);
            assert_eq!(
                saved.app_meter.as_ref().unwrap().snapshot().physical_bytes,
                0
            );
            let budget = saved.backing_budget.as_ref().unwrap();
            assert_eq!(budget.used(), 0);
            assert!(!budget.quarantined());
        }
    })
    .await;
    drop(stream.take());
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
    assert!(drained.unwrap());
}

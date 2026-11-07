//! Real Service ordered Chunk RESET after the original queue has owned DATA.
//! The original Root, five roles, queue, and Node backing decide settlement.

use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TcpOpenPhase,
    TuicNativeOverride,
};
use fixture::{Apps, auth, capture, config};

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_loaded_reset_joins_with_original_first_error() {
    reset_case(false, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_loaded_reset_keeps_original_debt_to_parent_q() {
    reset_case(true, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_ordered_chunk_loaded_reset_preserves_source_first_and_receipt() {
    reset_case(false, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_ordered_chunk_loaded_reset_keeps_source_first_and_ack_debt() {
    reset_case(true, true).await;
}

async fn reset_case(hold_ack: bool, hard: bool) {
    let profile = fixture::ServiceProfile::new(hard);
    let (mut server, fault) = udp_fault::server(profile.server_config());
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeOrderedPump
    );
    assert_eq!(policy.native_engine(), NativeTcpRelayEngine::ChunkPump);
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let outbound_probe = outbound.clone();
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    let mut service = profile.service(inbound, outbound, cfg.clone(), runtime);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    if hard {
        assert!(saved.backing_budget.is_some() && saved.app_meter.is_some());
    }
    let mut peer = None;
    let mut send = None;
    let mut original_queue = None;
    let mut backing = None;
    let mut original_id = None;
    let mut retired_queue = None;

    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_open = async {
                        let (stream, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        send = Some(stream);
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
                                assert!(owner.flow_native_permit_queue().is_none());
                                original_queue = owner.flow_native_chunk_queue();
                                backing = owner.flow_native_backing_meter();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("ordered Chunk Service ended before publication: {report:?}"),
            }
        }).await.unwrap();
        let queue = original_queue.as_ref().expect("original ordered Chunk queue");
        let meter = backing.as_ref().expect("original Node backing meter");
        assert!(app.tcp_reply.is_empty());
        let pending_probe = (hard && !hold_ack).then(|| {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let bridge = owner.relay_event_test_bridge().unwrap();
            let id = original_id.unwrap();
            bridge.arm_pending_probe(id.handle, id.epoch);
            bridge
        });
        let loaded = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let payload = vec![0x5a; 1024 * 1024];
                    let write = send.as_mut().unwrap().write_all(&payload);
                    tokio::pin!(write);
                    let mut written = false;
                    loop {
                        tokio::select! {
                            biased;
                            result = &mut write, if !written => { result.unwrap(); written = true; }
                            _ = tokio::task::yield_now() => {}
                        }
                        let snapshot = if hard { queue.custody_snapshot().await.0 } else { queue.snapshot().await };
                        // RESET may discard bytes merely accepted by Quinn's send
                        // buffer. Observe the real Source's complete fixed prefix
                        // before challenging Main's tail; no larger credit/deadline.
                        let source_loaded = pending_probe.is_none()
                            || queue.custody_snapshot().await.1.emitted >= 1024 * 1024;
                        if snapshot.queued_bytes > 0 && source_loaded {
                            if pending_probe.is_some() {
                                let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                                assert_eq!(owner.flow_contracts()[0].id, original_id.unwrap());
                                assert!(owner.ledger().first_failure.is_none());
                            }
                            assert!(snapshot.queued_bytes <= snapshot.capacity_bytes);
                            let (_, custody) = queue.custody_snapshot().await;
                            assert!(custody.emitted > 0);
                            assert!(meter.snapshot().payload_bytes > 0);
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                } => {},
                report = service.run() => panic!("ordered Chunk Service ended before charged DATA: {report:?}"),
            }
        }).await;
        if loaded.is_err() {
            let (queue_snapshot, custody) = queue.custody_snapshot().await;
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("ordered Chunk load: queue={queue_snapshot:?} emitted={} source={:?} meter={:?} flow={:?}",
                custody.emitted, owner.flow_native_pump_source_read_bytes(), meter.snapshot(), owner.flow_observation());
        }
        loaded.unwrap();
        assert!(app.tcp_reply.is_empty());
        if hard {
            let before = queue.custody_snapshot().await;
            eprintln!("[N1-RESET-before-reset] hold_ack={hold_ack} queued={} emitted={} consumed={} original_flow={:?}", before.0.queued_bytes, before.1.emitted, before.1.consumed, original_id);
        }
        if hold_ack {
            fault.rx_drop().unwrap();
        }
        let mut stream = send.take().unwrap();
        stream.reset(77u32.into()).unwrap();
        drop(stream);
        let join_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut observed_original_joins = false;
        let joined = tokio::time::timeout_at(join_deadline, async {
            loop {
                if pending_probe.is_some() {
                    // Observe the actual saved host after one poll, before the
                    // App can consume another prefix of the required tail.
                    std::future::poll_fn(|cx| {
                        let mut run = std::pin::pin!(service.run());
                        assert!(std::future::Future::poll(run.as_mut(), cx).is_pending());
                        std::task::Poll::Ready(())
                    }).await;
                } else {
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                }
                let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                let flow = owner.flow_observation().unwrap();
                if flow.joined == 5 && owner.ledger().first_failure.is_some() {
                    observed_original_joins = true;
                    assert!(flow.audit);
                    assert_eq!(flow.published, 1);
                    assert_eq!(flow.released, 0);
                    assert_eq!(flow.f, 1);
                    assert!(flow.last_data_disposition.is_none());
                    assert_eq!(owner.flow_contracts()[0].id, original_id.unwrap());
                    // Actual JOIN, field/proxy retirement and provider retirement
                    // are separate facts, all still within the original 5s bound.
                    if hold_ack && (owner.flow_roles_data_disposed_for_test(original_id.unwrap()) != Some(true)
                        || owner.flow_native_provider_view().is_none_or(|view| view.phase != TcpOpenPhase::Retiring)) {
                        continue;
                    }
                    if pending_probe.as_ref().is_none_or(|bridge| {
                        owner.flow_roles_data_disposed_for_test(original_id.unwrap()) == Some(true)
                            && bridge.pending_probe().is_some_and(|(bytes, can_send)| bytes > 0 && !can_send)
                    }) { break; }
                }
                // DATA precedes the original terminal. Drive the real App only
                // until JOIN/settled and actual Main tail have been observed.
                if pending_probe.is_some() {
                    if observed_original_joins { tokio::task::yield_now().await; }
                    else { app.turn_with_tcp_read_limit(128).await; }
                } else { app.turn().await; }
            }
        }).await;
        if joined.is_err() {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let queue = original_queue.as_ref().unwrap().custody_snapshot().await;
            let flow = owner.flow_observation();
            let debug = owner.flow_debug();
            let source = owner.flow_native_pump_source_read_bytes();
            let meter = backing.as_ref().unwrap().snapshot();
            let ledger = owner.ledger();
            eprintln!("ordered Chunk loaded RESET: flow={:?} debug={:?} queue={:?} custody_emitted={} source={:?} meter={:?} first={:?} Q={}",
                flow, debug, queue.0, queue.1.emitted,
                source, meter,
                ledger.first_failure.as_ref().map(|first| first.facts()), ledger.quarantined);
        }
        joined.unwrap();
        assert!(observed_original_joins);
        if hard && !hold_ack {
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                loop {
                    let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                    let id = original_id.unwrap();
                    if owner.flow_roles_data_disposed_for_test(id) == Some(true) {
                        let flow = owner.flow_observation().unwrap();
                        assert_eq!((flow.joined, flow.f, flow.released), (5, 1, 0));
                        // Original local DATA receipts are final; stock may still be held
                        // by Main. This only retires
                        // the external test observer, never the Root's queue.
                        retired_queue = Some(original_queue.as_ref().unwrap().custody_snapshot().await);
                        drop(original_queue.take());
                        break;
                    }
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                }
            }).await.unwrap();
        }
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let ledger = owner.ledger();
        let first = ledger.first_failure.as_ref().unwrap();
        assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Io);
        assert_eq!(first.facts().io_kind, Some(std::io::ErrorKind::Other));
        let id = original_id.unwrap();
        assert_eq!(first.facts().origin, open_origin(id.index, id.epoch,
            crate::shared::errors::FailurePhase::TcpOpen));
        assert!(first.facts().opaque && first.facts().source_content_omitted);
        assert!(first.text_bytes().is_empty());
        (first as *const _ as usize, format!("{:?}", first.facts()), first.text_bytes().to_vec())
    }).await;

    let scenario_failed = scenario.is_err();
    handle.request_stop();
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let original_owner = saved
        .session
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    let original_session_deadline = Some(
        original_owner
            .ledger()
            .shutdown_deadline
            .map_or(original_deadline, |old| old.min(original_deadline)),
    );
    let cleanup = capture(async {
        if scenario_failed {
            fixture::observe_failed_native_stop(service.run(), &handle, original_deadline, None)
                .await;
            return None;
        }
        let owner = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!(flow.joined, 5);
        assert!(flow.audit);
        assert_eq!(flow.released, 0);
        assert_eq!(flow.f, 1);
        assert!(flow.last_data_disposition.is_none());
        assert_eq!(owner.flow_contracts()[0].id, original_id.unwrap());
        if hold_ack {
            let provider = owner.flow_native_provider_view().unwrap();
            eprintln!("ordered RESET original retirement gate: provider={provider:?} roles_settled={:?} flow={:?}", owner.flow_roles_data_disposed_for_test(original_id.unwrap()), owner.flow_debug());
            assert_eq!(provider.phase, TcpOpenPhase::Retiring);
            assert!(provider.lease_held && provider.retirement_pending);
            assert!(fault.snapshot().counts.rx_dropped_datagrams > 0);
        }
        let report = tokio::time::timeout_at(original_deadline, service.run()).await;
        if hold_ack {
            assert!(report.is_err());
            if hard {
                let owner = &original_owner;
                eprintln!("[N1-RESET-IO-before] outbound={} highwater={} census_runs={} control_remaining={}", outbound_probe.packets_for_test().len(), outbound_probe.high_water_packets(), owner.shutdown_census_runs.load(Ordering::Relaxed), owner.control_remaining.load(Ordering::Relaxed));
                eprintln!("Chunk fatal original clocks: now={:?} original_control={:?} actual_control={:?} expected_session={:?} actual_session={:?} flow_stop={:?} census={:?}",
                    tokio::time::Instant::now(), original_deadline, *saved.deadline.lock().unwrap(),
                    original_session_deadline, owner.ledger().shutdown_deadline,
                    owner.flow_original_stop_for_test(original_id.unwrap()), owner.relay_event_test_bridge().unwrap().fatal_census());
                assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                assert_eq!(owner.ledger().shutdown_deadline, original_session_deadline);
            }
            if hard {
                // The original deadline expires authority; it does not promise
                // that the bounded retirement census/UI publishes in this poll.
                // Match the existing fatal observation watchdog, never a new
                // cleanup or clean-settlement deadline.
                let observed_q = tokio::time::timeout_at(original_deadline + Duration::from_secs(5), async {
                    tokio::select! {
                        _ = async {
                            loop {
                                assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                                assert_eq!(original_owner.ledger().shutdown_deadline, original_session_deadline);
                                if handle.phase() == Phase::Quarantined { break; }
                                tokio::time::sleep(Duration::from_millis(1)).await;
                            }
                        } => {},
                        report = service.run() => panic!("original ACK-held custody falsely settled: {report:?}"),
                    }
                }).await;
                if observed_q.is_err() {
                    eprintln!("[N1-RESET-IO-after] outbound={} highwater={} census_runs={} control_remaining={}", outbound_probe.packets_for_test().len(), outbound_probe.high_water_packets(), original_owner.shutdown_census_runs.load(Ordering::Relaxed), original_owner.control_remaining.load(Ordering::Relaxed));
                    let ledger = original_owner.ledger();
                    eprintln!("[N1-RESET-Q-publication] phase={:?} session_q={} session_deadline={:?} business_complete={} first={:?} budget={:?}",
                        handle.phase(), ledger.quarantined, ledger.shutdown_deadline,
                        original_owner.business_complete.load(Ordering::Acquire),
                        ledger.first_failure.as_ref().map(|f| f.facts()),
                        saved.backing_budget.as_ref().map(|b| (b.used(), b.quarantined())));
                    drop(ledger);
                    eprintln!("[N1-RESET-Q-publication] flow={:?} debug={:?} census={:?}", original_owner.flow_observation(), original_owner.flow_debug(), original_owner.relay_event_test_bridge().unwrap().fatal_census());
                }
                observed_q.expect("original bounded census must publish Q without restarting its deadline");
            } else {
            if handle.phase() != Phase::Quarantined {
                let observed = tokio::time::timeout(
                    Duration::from_millis(100),
                    service.run(),
                ).await;
                assert!(observed.is_err());
            }
            }
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let flow = owner.flow_observation().unwrap();
            let provider = owner.flow_native_provider_view().unwrap();
            let queue = original_queue.as_ref().unwrap().custody_snapshot().await;
            let ledger = owner.ledger();
            eprintln!("ordered Chunk fatal held: id={original_id:?} phase={:?} flow={flow:?} provider={provider:?} queue={:?} meter={:?} dropped={}",
                handle.phase(), queue,
                backing.as_ref().unwrap().snapshot(), fault.snapshot().counts.rx_dropped_datagrams);
            assert_eq!(handle.phase(), Phase::Quarantined);
            assert_eq!((flow.joined, flow.released, flow.f), (5, 0, 1));
            assert!(flow.last_data_disposition.is_none());
            assert_eq!(provider.phase, TcpOpenPhase::Retiring);
            assert!(provider.lease_held && provider.retirement_pending);
            assert!(ledger.quarantined);
            let first = ledger.first_failure.as_ref().unwrap();
            if hard {
                assert!(Arc::ptr_eq(&owner, &original_owner));
                assert_eq!(ledger.shutdown_deadline, original_session_deadline);
                assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                let budget = saved.backing_budget.as_ref().unwrap();
                assert!(budget.quarantined() && budget.used() > 0);
            }
            return Some((first as *const _ as usize, format!("{:?}", first.facts()), first.text_bytes().to_vec()));
        }
        let report = report.unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        let owner = service.completion.as_ref().unwrap().control.clone();
        let flow = owner.flow_observation().unwrap();
        assert_eq!(
            (flow.f, flow.released, flow.joined, flow.audit),
            (0, 1, 5, true)
        );
        let (messages, bytes, transferred, discarded, abandoned_downlink) =
            flow.last_data_disposition.expect("original ordered Chunk physical receipt");
        eprintln!("[N1-RESET-final-data] hard={hard} hold_ack={hold_ack} messages={messages} bytes={bytes} transferred={transferred} discarded={discarded} abandoned_downlink={abandoned_downlink} app_received={}", app.tcp_reply.len());
        assert!(messages > 0 && bytes > 0 && abandoned_downlink > 0);
        assert_eq!(bytes, transferred + discarded);
        let (queue, custody) = match retired_queue.take() {
            Some(snapshot) => snapshot,
            None => original_queue.as_ref().unwrap().custody_snapshot().await,
        };
        assert_eq!(queue.queued_bytes, 0);
        assert_eq!(
            custody.consumed.checked_add(custody.discarded_bytes),
            Some(custody.emitted)
        );
        assert!(custody.emitted > 0);
        assert_eq!(custody.discarded_bytes, 0);
        eprintln!("ordered Chunk fatal receipt: id={original_id:?} flow={flow:?} queue={queue:?} emitted={} consumed={} discarded={} meter={:?}",
            custody.emitted, custody.consumed, custody.discarded_bytes,
            backing.as_ref().unwrap().snapshot());
        let ledger = owner.ledger();
        assert!(!ledger.quarantined);
        let first = ledger.first_failure.as_ref().unwrap();
        if hard {
            assert!(Arc::ptr_eq(&owner, &original_owner));
            assert_eq!(ledger.shutdown_deadline, original_session_deadline);
            assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
            drop(original_queue.take());
            drop(app);
            assert_eq!(saved.app_meter.as_ref().unwrap().snapshot().physical_bytes, 0);
            let budget = saved.backing_budget.as_ref().unwrap();
            assert_eq!(budget.used(), 0);
            assert!(!budget.quarantined());
        }
        Some((
            first as *const _ as usize,
            format!("{:?}", first.facts()),
            first.text_bytes().to_vec(),
        ))
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    let first = scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    let final_first = cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    if let Some(ref final_first) = final_first {
        assert_eq!(&first, final_first);
    }
    assert!(drained.unwrap());
    if final_first.is_some() && !hold_ack {
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
    }
}

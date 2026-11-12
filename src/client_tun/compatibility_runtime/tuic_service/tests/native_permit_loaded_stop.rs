//! Real Service Native Permit loaded stop/RESET and original queue custody.
//! The peer and App supply pressure; Root owns the five roles, F, queue and Node.

use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{Apps, auth, capture, config};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ending {
    StopHeld,
    StopClean,
    RemoteReset,
}

#[tokio::test(flavor = "current_thread")]
async fn native_permit_loaded_stop_joins_five_roles_without_refunding_unacked_data() {
    loaded_terminal_case(Ending::StopHeld, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_permit_loaded_remote_reset_retains_first_error_after_receipt() {
    loaded_terminal_case(Ending::RemoteReset, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_permit_loaded_parent_stop_keeps_original_cap_debt() {
    loaded_terminal_case(Ending::StopHeld, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_permit_loaded_remote_reset_keeps_source_first_and_receipt() {
    loaded_terminal_case(Ending::RemoteReset, true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_permit_loaded_parent_stop_retires_original_cap_debt_cleanly() {
    loaded_terminal_case(Ending::StopClean, true).await;
}

async fn loaded_terminal_case(ending: Ending, hard: bool) {
    let remote_reset = ending == Ending::RemoteReset;
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
    let mut held_send = None;
    let mut held_recv = None;
    let mut original_queue = None;
    let mut backing = None;
    let mut held_cap = None;
    let mut egress_bridge = None;
    let mut original_id = None;
    let mut original_owner = None;

    let setup = capture(async {
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
                        held_send = Some(send);
                        if ending == Ending::StopClean { held_recv = Some(recv); }
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
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("Native Permit Service ended before publication: {report:?}"),
            }
        }).await.unwrap();
        let queue = original_queue.as_ref().expect("original Native Permit queue");
        let meter = backing.as_ref().expect("original Node endpoint backing meter");
        let bridge = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap()
            .relay_event_test_bridge().unwrap();
        if !remote_reset {
            bridge.arm_egress_probe(queue.clone());
            egress_bridge = Some(bridge);
        }
        assert!(app.tcp_reply.is_empty());
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    held_send.as_mut().unwrap().write_all(&[0x5a; 65536]).await.unwrap();
                    loop {
                        let physical = queue.snapshot_now();
                        if physical.queued_bytes + physical.leased_bytes > 0 {
                            assert!(physical.owned_bytes() <= physical.capacity_bytes);
                            assert!(meter.snapshot().payload_bytes > 0);
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("Native Permit Service ended before charged DATA: {report:?}"),
            }
        }).await.unwrap();
        assert!(app.tcp_reply.is_empty());
        if !remote_reset {
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                loop {
                    let (samples, overflow, ambiguous) = egress_bridge.as_ref().unwrap().egress_probe_samples();
                    assert!(!overflow && !ambiguous, "original Permit cap probe lost identity");
                    if let Some(sample) = samples.last().copied().filter(|sample| {
                        sample.ledger.cap_count > 0
                            && sample.ledger.admitted > sample.ledger.refunded
                            && sample.ledger.first_cap_remaining > 0
                            && sample.mirror_receipt == Some(false)
                            && queue.snapshot_now().leased_bytes >= sample.ledger.first_cap_remaining
                    }) {
                        held_cap = Some(sample);
                        break;
                    }
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                }
            }).await.expect("original Permit inflight cap not reached before parent stop");
        }
    }).await;

    let fatal_first = capture(async {
        if !remote_reset {
            return None;
        }
        let mut send = held_send.take().expect("original peer send half");
        send.reset(77u32.into()).unwrap();
        drop(send);
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(
                    tokio::time::timeout(Duration::from_millis(10), service.run())
                        .await
                        .is_err()
                );
                let owner = saved
                    .session
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                let flow = owner.flow_observation().unwrap();
                if flow.joined == 5 && owner.ledger().first_failure.is_some() {
                    break;
                }
            }
        })
        .await
        .unwrap_or_else(|elapsed| {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let flow = owner.flow_observation();
            let debug = owner.flow_debug();
            let queue = original_queue.as_ref().unwrap().snapshot_now();
            let meter = backing.as_ref().unwrap().snapshot();
            let ledger = owner.ledger();
            eprintln!("Native Permit loaded RESET wait: flow={flow:?} debug={debug:?} queue={queue:?} meter={meter:?} first={:?} Q={} peer={:?}",
                ledger.first_failure.as_ref().map(|first| first.facts()), ledger.quarantined(),
                peer.as_ref().map(|conn| conn.stats()));
            panic!("loaded Native Permit RESET disposition: {elapsed}");
        });
        let owner = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!(
            (
                flow.live,
                flow.published,
                flow.released,
                flow.joined,
                flow.f
            ),
            (1, 1, 0, 5, 1)
        );
        assert!(flow.audit);
        let queue = original_queue.as_ref().unwrap().snapshot_now();
        assert!(queue.queued_bytes + queue.leased_bytes > 0);
        assert!(backing.as_ref().unwrap().snapshot().payload_bytes > 0);
        let ledger = owner.ledger();
        let first = ledger.first_failure.as_ref().unwrap();
        assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Io);
        assert_eq!(first.facts().io_kind, Some(std::io::ErrorKind::Other));
        let id = original_id.unwrap();
        assert_eq!(first.facts().origin, open_origin(id.index, id.epoch,
            crate::shared::errors::FailurePhase::TcpOpen));
        assert!(Arc::ptr_eq(original_owner.as_ref().unwrap(), &owner));
        assert!(first.facts().opaque && first.facts().source_content_omitted);
        assert!(first.text_bytes().is_empty());
        Some((
            first as *const _ as usize,
            format!("{:?}", first.facts()),
            first.text_bytes().to_vec(),
        ))
    })
    .await;

    if hard && remote_reset && setup.is_ok() && fatal_first.is_ok() {
        // Final disposition is the original Root's full cap/range receipt and
        // real Node/App zero, not a strong external queue observer.
        drop(original_queue.take());
    }
    handle.request_stop();
    let peer_retirement = capture(async {
        if ending == Ending::StopClean && setup.is_ok() && fatal_first.is_ok() {
            // Submit the original parent limit while the same real cap still owes
            // bytes. Only then let the actual peer retire; never App abort/RESET.
            let owner = original_owner.as_ref().unwrap();
            let flow = owner.flow_observation().unwrap();
            assert_eq!(Some(owner.flow_contracts()[0].id), original_id);
            assert_eq!((flow.published, flow.f, flow.released), (1, 1, 0));
            assert!(owner.ledger().first_failure.is_none());
            let queue = original_queue.as_ref().unwrap().snapshot_now();
            let cap = held_cap.unwrap();
            assert!(queue.leased_bytes >= cap.ledger.first_cap_remaining);
            assert!(cap.ledger.cap_count > 0 && cap.ledger.admitted > cap.ledger.refunded);
            held_send.take().unwrap().finish().unwrap();
            held_recv.take().unwrap().stop(78u32.into()).unwrap();
            // These are test observers only. The Root still owns the original
            // capabilities and must return the complete physical receipt below.
            egress_bridge
                .as_ref()
                .unwrap()
                .retire_egress_probe_for_test();
            drop(original_queue.take());
        }
    })
    .await;
    let deadline = capture(async {
        saved
            .deadline
            .lock()
            .unwrap()
            .expect("original parent deadline")
    })
    .await;
    let original_deadline = deadline.as_ref().ok().copied();
    let assertions_ready = setup.is_ok()
        && fatal_first.is_ok()
        && peer_retirement.is_ok()
        && original_deadline.is_some();
    let joined = if assertions_ready {
        capture(async {
            let original_deadline = original_deadline.unwrap();
            if ending == Ending::StopHeld {
                crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                    loop {
                        assert!(
                            tokio::time::timeout(Duration::from_millis(10), service.run())
                                .await
                                .is_err()
                        );
                        let owner = saved
                            .session
                            .lock()
                            .unwrap()
                            .as_ref()
                            .unwrap()
                            .upgrade()
                            .unwrap();
                        let flow = owner.flow_observation().unwrap();
                        if flow.joined == 5 {
                            assert_eq!(
                                (flow.published, flow.released, flow.f, flow.running),
                                (1, 0, 1, 0)
                            );
                            assert_eq!((flow.w, flow.reserved, flow.escrow_bytes), (0, 0, 0));
                            assert!(flow.audit);
                            assert!(owner.ledger().first_failure.is_none());
                            assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                            let queue = original_queue.as_ref().unwrap().snapshot_now();
                            assert!(queue.queued_bytes + queue.leased_bytes > 0);
                            if let Some(cap) = held_cap {
                                assert!(queue.leased_bytes >= cap.ledger.first_cap_remaining);
                            }
                            assert!(backing.as_ref().unwrap().snapshot().payload_bytes > 0);
                            break;
                        }
                    }
                })
                .await
                .unwrap();
            }
        })
        .await
    } else {
        Ok(())
    };
    let expired = if assertions_ready && joined.is_ok() {
        capture(async {
        let original_deadline = original_deadline.unwrap();
        if ending != Ending::StopHeld {
            let report = tokio::time::timeout_at(original_deadline, service.run())
                .await
                .expect("original parent deadline expired before reset custody settled");
            assert_eq!(report.session, if remote_reset { SessionDeviceState::DeviceFailedSettled }
                else { SessionDeviceState::DeviceSettled });
            assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let flow = owner.flow_observation().unwrap();
            assert_eq!((flow.live, flow.published, flow.released, flow.joined, flow.f), (0, 1, 1, 5, 0));
            let (_, total, transferred, discarded, _) = flow.last_data_disposition.unwrap();
            assert!(total > 0);
            assert_eq!(transferred.checked_add(discarded), Some(total));
            if hard { flow.assert_permit_reset_receipt(); }
            else { flow.assert_permit_reset_balance(original_queue.as_ref().unwrap()); }
            assert!(flow.audit && !owner.ledger().quarantined());
            let ledger = owner.ledger();
            if remote_reset {
                let first = ledger.first_failure.as_ref().unwrap();
                assert_eq!(
                    (first as *const _ as usize, format!("{:?}", first.facts()), first.text_bytes().to_vec()),
                    fatal_first.as_ref().unwrap().as_ref().unwrap().clone()
                );
            } else { assert!(ledger.first_failure.is_none()); }
            assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
            if !hard { assert!(original_queue.as_ref().unwrap().physical_zero()); }
            else { assert!(original_queue.is_none()); }
            let physical = backing.as_ref().unwrap().snapshot();
            assert_eq!((physical.allocations, physical.payload_bytes,
                        physical.physical_bytes, physical.quarantined), (0, 0, 0, false));
            assert_eq!(handle.phase(), Phase::Complete);
            if hard {
                assert!(Arc::ptr_eq(original_owner.as_ref().unwrap(), &owner));
                assert_eq!(ledger.shutdown_deadline, Some(original_deadline));
                drop(app);
                assert_eq!(saved.app_meter.as_ref().unwrap().snapshot().physical_bytes, 0);
                let budget = saved.backing_budget.as_ref().unwrap();
                assert_eq!(budget.used(), 0); assert!(!budget.quarantined());
            }
            return;
        }
        let mut observer = Box::pin(handle.observe_shutdown());
        let phase = tokio::time::timeout_at(original_deadline + Duration::from_millis(100), async {
            tokio::select! {
                phase = observer.as_mut() => phase,
                report = service.run() => panic!("unacked Native Permit falsely settled: {report:?}"),
            }
        }).await.unwrap();
        assert!(matches!(phase, Phase::Quarantined | Phase::Incomplete));
        // D152 (A1): hard Q follows the provider deadline, not the parent limit.
        let a1 = hard.then(|| {
            assert_eq!(phase, Phase::Incomplete, "hard Q at the parent limit: no A1 extension");
            fixture::a1_q_deadline(&saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap(), original_deadline)
        });
        if phase == Phase::Incomplete {
            let limit = a1.unwrap_or_else(tokio::time::Instant::now) + Duration::from_secs(1);
            tokio::time::timeout_at(limit, async {
                while handle.phase() != Phase::Quarantined {
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                }
            }).await.unwrap();
        }
        if let Some(provider) = a1 { fixture::assert_a1_extended(&saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap(), provider); }
        if tokio::time::Instant::now() < original_deadline {
            tokio::select! {
                _ = tokio::time::sleep_until(original_deadline) => {},
                report = service.run() => panic!("charged Native Permit falsely settled before deadline: {report:?}"),
            }
        }
        assert!(tokio::time::Instant::now() >= original_deadline);
        assert_eq!(handle.phase(), Phase::Quarantined);
        assert!(service.completion.is_none());
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.released, flow.joined, flow.f), (1, 0, 5, 1));
        assert!(owner.ledger().quarantined());
        if remote_reset {
            let ledger = owner.ledger();
            let first = ledger.first_failure.as_ref().unwrap();
            assert_eq!(
                (first as *const _ as usize, format!("{:?}", first.facts()), first.text_bytes().to_vec()),
                fatal_first.as_ref().unwrap().as_ref().unwrap().clone()
            );
        } else if !hard {
            assert!(owner.ledger().first_failure.is_none());
        }
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        let queue = original_queue.as_ref().unwrap().snapshot_now();
        assert!(queue.queued_bytes + queue.leased_bytes > 0);
        let cap = held_cap.expect("same original inflight cap before shutdown");
        assert_eq!(cap.mirror_receipt, Some(false));
        let (terminal, audit, pending, visits, ambiguous) = egress_bridge.as_ref().unwrap().egress_shutdown_snapshot();
        assert!(visits > 0 && !ambiguous, "Root census missed or duplicated the bound SocketCtx");
        let terminal = terminal.expect("bound SocketCtx at Root shutdown census");
        assert_eq!((terminal.handle, terminal.ledger.epoch, terminal.ledger.origin),
                   (cap.handle, cap.ledger.epoch, cap.ledger.origin));
        assert_eq!(terminal.phase, crate::client_tun::legacy_event_loop::relay_events::EgressProbePhase::ShutdownCensus);
        assert_eq!(terminal.mirror_receipt, Some(false));
        assert_eq!((terminal.ledger.first_cap_ordinal, terminal.ledger.first_cap_remaining,
                    terminal.ledger.refunded),
                   (cap.ledger.first_cap_ordinal, cap.ledger.first_cap_remaining,
                    cap.ledger.refunded), "original inflight cap changed without a physical receipt");
        let audit = audit.expect("original queue audit at Root shutdown census");
        let pending = pending.expect("original pending permits at Root shutdown census");
        eprintln!("Permit shutdown ownership diagnostic: pending={pending:?} audit={audit:?} inflight={:?}", terminal.ledger);
        assert!(pending.same_queue);
        assert_eq!(pending.bytes, pending.permit_bytes);
        assert_eq!(pending.count == 0, pending.bytes == 0);
        assert!(!audit.interrupted);
        assert!(terminal.ledger.cap_count > 0 && terminal.ledger.admitted > terminal.ledger.refunded);
        assert_eq!((audit.queued, audit.leased), (queue.queued_bytes, queue.leased_bytes));
        assert_eq!(audit.emitted, audit.consumed + audit.discarded + audit.queued);
        assert_eq!(audit.consumed, audit.released + audit.leased);
        assert_eq!(audit.released, audit.packet_flush + audit.ack_drain + audit.explicit_other + audit.dropped);
        let admitted = usize::try_from(terminal.ledger.admitted).unwrap();
        let refunded = usize::try_from(terminal.ledger.refunded).unwrap();
        assert_eq!(audit.leased, pending.permit_bytes + admitted - refunded);
        assert_eq!(audit.packet_flush + audit.ack_drain, refunded);
        assert_eq!(audit.explicit_other + audit.dropped,
            audit.consumed - admitted - pending.permit_bytes);
        eprintln!("Permit shutdown terminal: epoch={} origin={} pre_cap_ordinal={} pre_cap_remaining={} pre_refunded={} cap_ordinal={} cap_remaining={} admitted={} refunded={} caps={} visits={} queue_emitted={} consumed={} discarded={} queued={} leased={} released={} packet_flush={} ack={} explicit_other={} drop={} receipt={:?} F={}",
            terminal.ledger.epoch, terminal.ledger.origin, cap.ledger.first_cap_ordinal,
            cap.ledger.first_cap_remaining, cap.ledger.refunded, terminal.ledger.first_cap_ordinal,
            terminal.ledger.first_cap_remaining, terminal.ledger.admitted, terminal.ledger.refunded,
            terminal.ledger.cap_count, visits, audit.emitted, audit.consumed, audit.discarded,
            audit.queued, audit.leased, audit.released, audit.packet_flush, audit.ack_drain,
            audit.explicit_other, audit.dropped, terminal.mirror_receipt, flow.f);
        assert!(backing.as_ref().unwrap().snapshot().payload_bytes > 0);
        if hard {
            assert!(Arc::ptr_eq(original_owner.as_ref().unwrap(), &owner));
            assert_eq!(Some(owner.flow_contracts()[0].id), original_id);
            assert_eq!(owner.ledger().shutdown_deadline, Some(original_deadline));
            let budget = saved.backing_budget.as_ref().unwrap();
            assert!(budget.quarantined() && budget.used() > 0);
            let mut first = fixture::hard_q_refusal(&owner, &saved, true);
            for _ in 0..2 {
                assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
                let current = fixture::hard_q_refusal(&owner, &saved, true);
                if first.is_some() { assert_eq!(current, first); } else { first = current; }
                assert_eq!(owner.ledger().shutdown_deadline, Some(original_deadline));
                assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
            }
        }
    }).await
    } else {
        capture(async {
            if let Some(original_deadline) = original_deadline {
                fixture::observe_failed_native_stop(
                    service.run(),
                    &handle,
                    original_deadline,
                    original_queue.as_ref(),
                )
                .await;
            }
        })
        .await
    };
    drop(held_send.take());
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = match drain {
        Some(drain) => tokio::time::timeout(Duration::from_secs(10), drain.drained())
            .await
            .ok(),
        None => None,
    };
    for outcome in [
        setup,
        fatal_first.map(|_| ()),
        peer_retirement,
        deadline.map(|_| ()),
        joined,
        expired,
    ] {
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
    assert_eq!(drained, Some(true));
}

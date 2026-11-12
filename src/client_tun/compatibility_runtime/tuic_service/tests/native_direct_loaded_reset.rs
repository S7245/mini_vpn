//! Diagnostic of real direct four-role RESET with pending local egress.
//! The original Root decides whether a charged F survives four JOINs.

use super::*;
use crate::upstream::{
    RelayFamilyPolicy, RelayFamilyPolicySources, TcpOpenPhase, TuicNativeOverride,
};
use fixture::{Apps, auth, capture, config};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ending {
    AppDrain,
    ParentStop,
    AckHeld,
}

#[tokio::test(flavor = "current_thread")]
async fn native_direct_loaded_reset_observes_four_join_debt_and_first_error() {
    exercise(Ending::AppDrain, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_direct_loaded_reset_keeps_first_at_parent_deadline_without_app_drain() {
    exercise(Ending::ParentStop, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_direct_loaded_reset_holds_local_retirement_without_return_ack() {
    exercise(Ending::AckHeld, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_direct_loaded_reset_preserves_source_first_through_app_drain() {
    exercise(Ending::AppDrain, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_direct_loaded_reset_preserves_source_first_through_parent_stop() {
    exercise(Ending::ParentStop, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_direct_loaded_reset_keeps_ack_held_debt_and_source_first() {
    exercise(Ending::AckHeld, true).await;
}

async fn exercise(ending: Ending, hard: bool) {
    let profile = fixture::ServiceProfile::new(hard);
    let (mut server, fault) = super::udp_fault::server(profile.server_config());
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_chunk: Some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeChunkPump
    );
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
    let mut send = None;
    let mut recv = None;
    let mut backing = None;
    let mut original_flow_id = None;
    let mut charged_four_join_without_first = false;
    let mut first_before_release = None;

    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_open = async {
                        let (stream, mut receive) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        receive.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        send = Some(stream);
                        recv = Some(receive);
                    };
                    let app_open = async {
                        loop {
                            app.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            let flow = owner.flow_observation().unwrap();
                            if flow.published == 1 && flow.running == 4 {
                                let contracts = owner.flow_contracts();
                                assert_eq!(contracts.len(), 1);
                                original_flow_id = Some(contracts[0].id);
                                backing = owner.flow_native_backing_meter();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("direct Service ended before publication: {report:?}"),
            }
        }).await.unwrap();
        assert!(app.tcp_reply.is_empty());
        assert!(backing.is_some());
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                result = send.as_mut().unwrap().write_all(&[0x5a; 8192]) => { result.unwrap(); },
                report = service.run() => panic!("direct Service ended before write: {report:?}"),
            }
        }).await.unwrap();
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    while app.tcp_reply.len() < 8192 {
                        app.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("direct Service ended before original App prefix: {report:?}"),
            }
        }).await.unwrap();
        assert_eq!(app.tcp_reply, vec![0x5a; 8192]);
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                result = send.as_mut().unwrap().write_all(&[0x5a; 65536]) => { result.unwrap(); },
                report = service.run() => panic!("direct Service ended before second write: {report:?}"),
            }
        }).await.unwrap();
        if hard && ending != Ending::AckHeld {
            // Defer only the original final ACK for observation. Source/stock,
            // provider and SocketCtx still physically retire before this ACK.
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            owner.hold_published_receipt(original_flow_id.unwrap());
        }
        if ending == Ending::AckHeld {
            fault.rx_drop().unwrap();
        }
        let mut stream = send.take().unwrap();
        stream.reset(77u32.into()).unwrap();
        drop(stream);
        let mut receive = recv.take().unwrap();
        if ending != Ending::AckHeld {
            receive.stop(78u32.into()).unwrap();
        }
        drop(receive);

        let observed = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                let flow = owner.flow_observation().unwrap();
                if flow.joined == 4 || flow.released == 1 {
                    let source = owner.flow_native_direct_source_received_bytes();
                    let provider = owner.flow_native_provider_view();
                    let pending_error = owner.flow_native_pending_reader_error_order();
                    let ledger = owner.ledger();
                    eprintln!("direct loaded RESET first gate: flow={flow:?} source={source:?} provider={provider:?} pending_error={pending_error:?} first={:?} Q={} meter={:?}",
                        ledger.first_failure.as_ref().map(|first| first.facts()), ledger.quarantined,
                        backing.as_ref().unwrap().snapshot());
                    assert!(source.is_some_and(|bytes| bytes >= 8192));
                    charged_four_join_without_first |= flow.joined == 4 && flow.f == 1
                        && pending_error.is_some()
                        && ledger.first_failure.is_none();
                    if flow.joined == 4 && flow.f == 1 {
                        if let Some(first) = ledger.first_failure.as_ref() {
                            assert!(pending_error.is_none());
                            let id = original_flow_id.unwrap();
                            assert_eq!(first.facts().origin, open_origin(id.index, id.epoch,
                                crate::shared::errors::FailurePhase::TcpOpen));
                            assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Io);
                            assert_eq!(first.facts().io_kind, Some(std::io::ErrorKind::Other));
                            assert!(first.facts().opaque && first.facts().source_content_omitted);
                            assert!(first.text_bytes().is_empty());
                            first_before_release = Some((
                                first as *const _ as usize,
                                format!("{:?}", first.facts()),
                                first.text_bytes().to_vec(),
                            ));
                        }
                    }
                    // JOIN does not prove the original fields/callbacks or
                    // provider have retired. Observe both within this same 5s.
                    if hard && ending == Ending::AckHeld
                        && (owner.flow_roles_data_disposed_for_test(original_flow_id.unwrap()) != Some(true)
                            || provider.is_none_or(|view| view.phase != TcpOpenPhase::Retiring)) {
                        continue;
                    }
                    if hard && ending != Ending::AckHeld {
                        let Some(receipt) = owner.held_exact_receipt() else { continue; };
                        assert_eq!(receipt.id, original_flow_id.unwrap());
                        assert_eq!(receipt.role_joins, 4);
                        assert!(receipt.row_held && receipt.original_matches && receipt.provider_settled);
                        assert!(receipt.rearming && receipt.producer_stopped && !receipt.quarantined);
                        assert!(first_before_release.is_some());
                        owner.release_held_receipt();
                    }
                    break;
                }
            }
        }).await;
        if observed.is_err() {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let flow = owner.flow_observation();
            let source = owner.flow_native_direct_source_received_bytes();
            let provider = owner.flow_native_provider_view();
            let ledger = owner.ledger();
            eprintln!("direct loaded RESET deferred: flow={:?} source={:?} provider={:?} first={:?} Q={}",
                flow, source, provider, ledger.first_failure.as_ref().map(|first| first.facts()), ledger.quarantined);
        }
        observed.unwrap();
        if ending == Ending::AckHeld {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let flow = owner.flow_observation().unwrap();
            let contracts = owner.flow_contracts();
            assert_eq!(contracts.len(), 1);
            assert_eq!(Some(contracts[0].id), original_flow_id);
            let provider = owner.flow_native_provider_view().unwrap();
            let ledger = owner.ledger();
            eprintln!("direct ACK-held four JOIN: flow={flow:?} provider={:?} first={:?} rx_dropped={}",
                provider,
                ledger.first_failure.as_ref().map(|first| first.facts()),
                fault.snapshot().counts.rx_dropped_datagrams);
            assert_eq!((flow.joined, flow.released, flow.f), (4, 0, 1));
            assert_eq!(flow.last_data_disposition, None);
            assert_eq!(provider.phase, TcpOpenPhase::Retiring);
            assert!(provider.lease_held && provider.retirement_pending);
            assert!(fault.snapshot().counts.rx_dropped_datagrams > 0);
            assert!(first_before_release.is_some());
            let first = ledger.first_failure.as_ref().unwrap().facts();
            assert_eq!(first.origin.phase, crate::shared::errors::FailurePhase::TcpOpen);
            assert_eq!(first.class, crate::shared::errors::FailureClass::Io);
            assert_eq!(first.io_kind, Some(std::io::ErrorKind::Other));
        }
        if ending != Ending::AppDrain {
            return;
        }
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    loop {
                        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                        if owner.flow_observation().unwrap().released == 1 { break; }
                        app.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("direct Service ended before clean release: {report:?}"),
            }
        }).await.unwrap();
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.f, flow.released, flow.joined), (0, 1, 4));
        assert!(flow.audit);
        let ledger = owner.ledger();
        let first = ledger.first_failure.as_ref().unwrap();
        assert_eq!(first.facts().class, crate::shared::errors::FailureClass::Io);
        assert_eq!(first.facts().io_kind, Some(std::io::ErrorKind::Other));
        if let Some(before) = &first_before_release {
            assert_eq!(
                &(first as *const _ as usize, format!("{:?}", first.facts()), first.text_bytes().to_vec()),
                before
            );
        }
    }).await;
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
        if scenario.is_err() {
            fixture::observe_failed_native_stop(service.run(), &handle, original_deadline, None)
                .await;
            return;
        }
        if ending == Ending::AckHeld {
            let mut run = Box::pin(service.run());
            let completed = tokio::time::timeout_at(original_deadline, run.as_mut()).await;
            assert!(completed.is_err() && tokio::time::Instant::now() >= original_deadline);
            // The deadline → Q publication takes several task hops after the
            // timer tick this observer shares (D29): keep driving the original
            // run until the parent phase is published.
            // D152 (A1): Q follows the provider deadline, not the parent limit;
            // the extension has no meter gate, so non-hard rows get it too (D194).
            let a1 = Some(fixture::a1_q_deadline(&original_owner, original_deadline));
            let wait = a1.map_or(Duration::ZERO, |provider| {
                provider.saturating_duration_since(tokio::time::Instant::now())
            });
            // D187 (model T7|T6, as the D170 direct held rows): with D181's
            // derived bound the hard provider may retire inside its one A1
            // extension (T7: clean settle in (parent, P] keeping the reset's
            // first failure) or outlive it (T6: second expiry, sticky Q below).
            let id = original_flow_id.unwrap();
            let mut tail = None;
            let ended = fixture::window(wait + Duration::from_secs(5), std::future::poll_fn(|cx| {
                let result = run.as_mut().poll(cx);
                if original_owner.flow_contracts().first().is_some_and(|c| c.id == id) {
                    tail = original_owner.flow_provider_tail_for_test(id);
                }
                match result {
                    std::task::Poll::Ready(report) => std::task::Poll::Ready(Some(report)),
                    std::task::Poll::Pending if handle.phase() != Phase::Stopping => std::task::Poll::Ready(None),
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            }))
            .await
            .expect("original Service did not publish parent phase after deadline");
            let at = tokio::time::Instant::now();
            drop(run);
            if let Some(report) = ended {
                let provider = a1.expect("A1 provider deadline");
                eprintln!("[D187-direct-ack-ending] T7 at_after_parent={:?} provider_after_parent={:?} report={report:?}",
                    at - original_deadline, provider - original_deadline);
                assert!(at > original_deadline && at <= provider, "T7 outside (parent, P]");
                assert_eq!(tail, Some((Some(provider), true)), "A1 ending without its one extension");
                assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
                assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
                assert_eq!(handle.phase(), Phase::Complete);
                let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                assert!(Arc::ptr_eq(&original_owner, &owner));
                let flow = owner.flow_observation().unwrap();
                assert_eq!((flow.joined, flow.released, flow.f), (4, 1, 0));
                assert!(flow.audit && !owner.ledger().quarantined);
                assert_eq!(fixture::first_failure_identity(&owner), first_before_release);
                assert_eq!(backing.as_ref().unwrap().snapshot().physical_bytes, 0);
                assert_eq!(owner.ledger().shutdown_deadline, original_session_deadline);
                assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                if hard {
                    let budget = saved.backing_budget.as_ref().unwrap();
                    assert_eq!(budget.used(), 0);
                    assert!(!budget.quarantined());
                }
                return;
            }
            if let Some(provider) = a1 { fixture::assert_a1_extended(&original_owner, provider); }
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let flow = owner.flow_observation().unwrap();
            let contracts = owner.flow_contracts();
            assert_eq!(contracts.len(), 1);
            assert_eq!(Some(contracts[0].id), original_flow_id);
            let provider = owner.flow_native_provider_view().unwrap();
            let ledger = owner.ledger();
            eprintln!("direct ACK-held parent: completed={} phase={:?} flow={flow:?} provider={:?} first={:?} Q={} rx_dropped={}",
                completed.is_ok(), handle.phase(), provider,
                ledger.first_failure.as_ref().map(|first| first.facts()),
                ledger.quarantined, fault.snapshot().counts.rx_dropped_datagrams);
            assert!(completed.is_err());
            assert_eq!(handle.phase(), Phase::Quarantined);
            assert_eq!((flow.joined, flow.released, flow.f), (4, 0, 1));
            assert_eq!(flow.last_data_disposition, None);
            assert_eq!(provider.phase, TcpOpenPhase::Retiring);
            assert!(provider.lease_held && provider.retirement_pending);
            assert!(ledger.quarantined);
            drop(ledger);
            assert_eq!(fixture::first_failure_identity(&owner), first_before_release);
            if hard {
                assert!(Arc::ptr_eq(&original_owner, &owner));
                assert_eq!(owner.ledger().shutdown_deadline, original_session_deadline);
                assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                let budget = saved.backing_budget.as_ref().unwrap();
                assert!(budget.used() > 0 && budget.quarantined());
            }
            return;
        }
        if ending == Ending::ParentStop {
            let completed = tokio::time::timeout_at(original_deadline, service.run()).await;
            let owner = saved
                .session
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .unwrap();
            let flow = owner.flow_observation().unwrap();
            eprintln!(
                "direct loaded RESET held parent: completed={} phase={:?} flow={flow:?} meter={:?}",
                completed.is_ok(),
                handle.phase(),
                backing.as_ref().unwrap().snapshot()
            );
            let report =
                completed.expect("original parent stop must return after physical receipt");
            assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
            assert_eq!(
                report.node,
                Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
            );
            assert_eq!(handle.phase(), Phase::Complete);
            assert_eq!((flow.joined, flow.released, flow.f), (4, 1, 0));
            let (messages, bytes, transferred, discarded, _) = flow
                .last_data_disposition
                .expect("original physical receipt");
            assert!(messages > 0 && transferred >= 8192);
            assert_eq!(bytes, transferred + discarded);
            assert_eq!(backing.as_ref().unwrap().snapshot().physical_bytes, 0);
            assert_eq!(
                fixture::first_failure_identity(&owner),
                first_before_release
            );
            if hard {
                assert!(Arc::ptr_eq(&original_owner, &owner));
                assert_eq!(owner.ledger().shutdown_deadline, original_session_deadline);
                assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                drop(app);
                assert_eq!(saved.app_meter.as_ref().unwrap().snapshot().physical_bytes, 0);
                let budget = saved.backing_budget.as_ref().unwrap();
                assert_eq!(budget.used(), 0);
                assert!(!budget.quarantined());
            }
            return;
        }
        let report = tokio::time::timeout_at(original_deadline, service.run())
            .await
            .unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
        assert_eq!(
            report.node,
            Some(crate::tuic::NodeShutdown::RunningResourcesSettled)
        );
        assert_eq!(backing.as_ref().unwrap().snapshot().physical_bytes, 0);
        if hard {
            assert_eq!(fixture::first_failure_identity(&original_owner), first_before_release);
            assert_eq!(original_owner.ledger().shutdown_deadline, original_session_deadline);
            assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
            drop(app);
            assert_eq!(saved.app_meter.as_ref().unwrap().snapshot().physical_bytes, 0);
            let budget = saved.backing_budget.as_ref().unwrap();
            assert_eq!(budget.used(), 0);
            assert!(!budget.quarantined());
        }
    })
    .await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(drained.unwrap());
    assert!(
        !charged_four_join_without_first,
        "four joined direct roles retained F without original IO first"
    );
    assert!(
        first_before_release.is_some(),
        "direct four JOIN/F1 first was not observed"
    );
}

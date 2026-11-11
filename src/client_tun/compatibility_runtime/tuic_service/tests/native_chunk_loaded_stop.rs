//! Real Service ordered Chunk parent stop while the original queue owns DATA.
//! Root owns the five joins, provider retirement, F, and physical settlement.

use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{Apps, auth, capture, config};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Ending {
    AppClean,
    AppHeld,
    StopClean,
    StopHeld,
}

impl Ending {
    fn app_abort(self) -> bool {
        matches!(self, Self::AppClean | Self::AppHeld)
    }

    fn clean(self) -> bool {
        matches!(self, Self::AppClean | Self::StopClean)
    }
}

fn original_reset_packets(link: &PacketLink) -> usize {
    link.packets_for_test()
        .iter()
        .filter_map(|packet| etherparse::PacketHeaders::from_ip_slice(packet).ok())
        .filter_map(|headers| match headers.transport {
            Some(etherparse::TransportHeader::Tcp(tcp)) => Some(tcp),
            _ => None,
        })
        .filter(|tcp| tcp.rst && tcp.source_port == 10000 && tcp.destination_port == 40000)
        .count()
}

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_loaded_stop_retains_unsettled_parent() {
    exercise(Ending::StopHeld, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_loaded_parent_stop_retires_peer_and_releases_root() {
    exercise(Ending::StopClean, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_loaded_app_abort_retires_peer_and_releases_root() {
    exercise(Ending::AppClean, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_ordered_chunk_loaded_app_abort_holds_provider_to_parent_q() {
    exercise(Ending::AppHeld, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_ordered_chunk_loaded_parent_stop_retains_provider_to_q() {
    exercise(Ending::StopHeld, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_ordered_chunk_loaded_parent_stop_retires_with_real_receipt() {
    exercise(Ending::StopClean, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_ordered_chunk_loaded_app_abort_retires_with_real_receipt() {
    exercise(Ending::AppClean, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_ordered_chunk_loaded_app_abort_retains_provider_to_q() {
    exercise(Ending::AppHeld, true).await;
}

async fn exercise(ending: Ending, hard: bool) {
    let profile = fixture::ServiceProfile::new(hard);
    let mut server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
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
    let outbound_tap = outbound.clone();
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
    let mut reset_before_stop = None;

    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let remote_open = async {
                        let (remote_send, mut recv) = remote.accept_bi().await.unwrap();
                        let mut want = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        want.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; want.len()];
                        recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, want);
                        stream = Some((remote_send, recv));
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
        let queue = original_queue.as_ref().unwrap();
        let meter = backing.as_ref().unwrap();
        assert!(app.tcp_reply.is_empty());
        let loaded = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let payload = vec![0x5a; 1024 * 1024];
                    let write = stream.as_mut().unwrap().0.write_all(&payload);
                    tokio::pin!(write);
                    let mut written = false;
                    loop {
                        tokio::select! {
                            biased;
                            result = &mut write, if !written => { result.unwrap(); written = true; }
                            _ = tokio::task::yield_now() => {}
                        }
                        let snapshot = if hard { queue.custody_snapshot().await.0 } else { queue.snapshot().await };
                        if snapshot.queued_bytes > 0 {
                            assert!(snapshot.queued_bytes <= snapshot.capacity_bytes);
                            assert!(meter.snapshot().payload_bytes > 0);
                            break;
                        }
                    }
                } => {},
                report = service.run() => panic!("ordered Chunk Service ended before charged DATA: {report:?}"),
            }
        }).await;
        if loaded.is_err() {
            let (snapshot, custody) = queue.custody_snapshot().await;
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("Chunk stop load: queue={snapshot:?} emitted={} meter={:?} flow={:?}",
                custody.emitted, meter.snapshot(), owner.flow_observation());
        }
        loaded.unwrap();
        assert!(app.tcp_reply.is_empty());
        if !ending.app_abort() {
            reset_before_stop = Some(original_reset_packets(&outbound_tap));
            handle.request_stop();
        }
        if ending.app_abort() {
            app.sockets.get_mut::<smoltcp::socket::tcp::Socket>(app.tcp).abort();
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                loop {
                    app.turn().await;
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                    let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                    let flow = owner.flow_observation().unwrap();
                    if flow.joined == 5 {
                        assert_eq!(owner.flow_contracts()[0].id, original_id.unwrap());
                        assert_eq!((flow.published, flow.released, flow.f), (1, 0, 1));
                        assert!(flow.audit && owner.ledger().first_failure.is_none());
                        break;
                    }
                }
            }).await.unwrap();
        }
        if ending.clean() {
            let (mut send, mut recv) = stream.take().unwrap();
            send.finish().unwrap();
            recv.stop(78u32.into()).unwrap();
            drop((send, recv));
        }
    }).await;

    // The external observer owns real cold inventory. Hard final custody is
    // verified by the original Root receipt and actual Node/App zero, rather
    // than retaining this test clone through Node's aggregate audit.
    if hard && ending.clean() {
        drop(original_queue.take());
    }
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
    let settlement = capture(async {
        if scenario.is_err() {
            fixture::observe_failed_native_stop(service.run(), &handle, original_deadline, None).await;
            return;
        }
        if ending.clean() {
            let report = tokio::time::timeout_at(original_deadline, service.run()).await.unwrap();
            assert_eq!(report.session, SessionDeviceState::DeviceSettled);
            assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
            assert_eq!(handle.phase(), Phase::Complete);
            let owner = service.completion.as_ref().unwrap().control.clone();
            let flow = owner.flow_observation().unwrap();
            assert_eq!((flow.published, flow.joined, flow.released, flow.f), (1, 5, 1, 0));
            assert!(flow.audit && !owner.ledger().quarantined && owner.ledger().first_failure.is_none());
            let (messages, bytes, transferred, discarded, _) = flow.last_data_disposition.unwrap();
            assert!(messages > 0 && bytes > 0);
            assert_eq!(bytes, transferred + discarded);
            if !hard {
            let (queue, custody) = original_queue.as_ref().unwrap().custody_snapshot().await;
            assert_eq!(queue.queued_bytes, 0);
            assert_eq!(custody.consumed.checked_add(custody.discarded_bytes), Some(custody.emitted));
            assert!(custody.emitted > 0);
                eprintln!("Chunk Legacy final custody: queue={queue:?} custody={custody:?}");
            }
            let meter = backing.as_ref().unwrap().snapshot();
            assert_eq!((meter.allocations, meter.payload_bytes, meter.physical_bytes), (0, 0, 0));
            if ending == Ending::StopClean {
                let before = reset_before_stop.unwrap();
                let after = original_reset_packets(&outbound_tap);
                eprintln!("Chunk shutdown wire: id={original_id:?} rst_before={before} rst_after={after}");
                assert!(after > before, "original stop did not flush a same-socket TCP RST");
            }
            eprintln!("Chunk cancel clean: hard={hard} ending={ending:?} id={original_id:?} flow={flow:?} meter={meter:?}");
            assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
            assert_eq!(owner.ledger().shutdown_deadline, original_session_deadline);
            if hard {
                assert!(Arc::ptr_eq(&original_owner, &owner));
                assert!(original_queue.is_none());
                drop(app);
                assert_eq!(saved.app_meter.as_ref().unwrap().snapshot().physical_bytes, 0);
                let budget = saved.backing_budget.as_ref().unwrap();
                assert_eq!(budget.used(), 0); assert!(!budget.quarantined());
            }
            return;
        }
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                let flow = owner.flow_observation().unwrap();
                if flow.joined == 5 {
                    assert!(flow.audit);
                    assert_eq!((flow.published, flow.running), (1, 0));
                    assert_eq!(owner.flow_contracts()[0].id, original_id.unwrap());
                    assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
                    assert!(owner.ledger().first_failure.is_none());
                    break;
                }
            }
        }).await.unwrap();
        let mut observer = Box::pin(handle.observe_shutdown());
        let phase = tokio::time::timeout_at(original_deadline + Duration::from_millis(100), async {
            tokio::select! {
                phase = observer.as_mut() => phase,
                report = service.run() => panic!("held ordered Chunk falsely settled: {report:?}"),
            }
        }).await.unwrap();
        assert!(matches!(phase, Phase::Quarantined | Phase::Incomplete));
        // D152 (A1): hard Q follows the provider deadline, not the parent limit.
        let a1 = hard.then(|| {
            assert_eq!(phase, Phase::Incomplete, "hard Q at the parent limit: no A1 extension");
            fixture::a1_q_deadline(&original_owner, original_deadline)
        });
        if phase == Phase::Incomplete {
            let limit = a1.unwrap_or_else(tokio::time::Instant::now) + Duration::from_secs(1);
            tokio::time::timeout_at(limit, async {
                while handle.phase() != Phase::Quarantined {
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                }
            }).await.unwrap();
        }
        if let Some(provider) = a1 { fixture::assert_a1_extended(&original_owner, provider); }
        if tokio::time::Instant::now() < original_deadline {
            tokio::select! {
                _ = tokio::time::sleep_until(original_deadline) => {},
                report = service.run() => panic!("held ordered Chunk falsely settled before deadline: {report:?}"),
            }
        }
        assert!(tokio::time::Instant::now() >= original_deadline);
        assert_eq!(handle.phase(), Phase::Quarantined);
        assert!(service.completion.is_none());
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!(owner.flow_contracts()[0].id, original_id.unwrap());
        assert_eq!((flow.published, flow.released, flow.joined, flow.f), (1, 0, 5, 1));
        assert!(flow.audit && owner.ledger().quarantined);
        if !hard { assert!(owner.ledger().first_failure.is_none()); }
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        let provider = owner.flow_native_provider_view().unwrap();
        assert_eq!(provider.phase, crate::upstream::TcpOpenPhase::Retiring);
        assert!(provider.lease_held && provider.retirement_pending);
        let (queue, custody) = original_queue.as_ref().unwrap().custody_snapshot().await;
        let meter = backing.as_ref().unwrap().snapshot();
        assert_eq!(queue.queued_bytes, 0);
        assert_eq!(custody.discarded_bytes, custody.frozen_bytes);
        assert_eq!(
            custody.consumed.checked_add(custody.discarded_bytes),
            Some(custody.emitted)
        );
        assert!(custody.emitted > 0);
        assert!(flow.last_data_disposition.is_none());
        eprintln!("Chunk stop expiry: queue={queue:?} emitted={} consumed={} discarded={} meter={meter:?}",
            custody.emitted, custody.consumed, custody.discarded_bytes);
        assert!(meter.physical_bytes > 0);
        if hard {
            assert!(Arc::ptr_eq(&original_owner, &owner));
            assert_eq!(owner.ledger().shutdown_deadline, original_session_deadline);
            let budget = saved.backing_budget.as_ref().unwrap();
            assert!(budget.quarantined() && budget.used() > 0 && meter.quarantined);
            let mut first = fixture::hard_q_refusal(&owner, &saved, !ending.app_abort());
            for _ in 0..2 {
                assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
                assert_eq!(Some(owner.flow_contracts()[0].id), original_id);
                let current = fixture::hard_q_refusal(&owner, &saved, !ending.app_abort());
                if first.is_some() { assert_eq!(current, first); } else { first = current; }
                assert_eq!(owner.ledger().shutdown_deadline, original_session_deadline);
                assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
            }
        } else { assert!(!meter.quarantined); }
    }).await;
    drop(stream.take());
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    settlement.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(drained.unwrap());
}

//! Owns true Service direct App-abort/parent-stop stimulus and same-Flow custody observations.
//! Root retains four-role, provider, physical receipt, deadline, and F authority.

use super::*;
use crate::upstream::{
    RelayFamilyPolicy, RelayFamilyPolicySources, TcpOpenPhase, TuicNativeOverride,
};
use fixture::{Apps, auth, capture, config};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Ending {
    PeerRetires,
    AckHeld,
    ParentStopClean,
    ParentStopHeld,
}

impl Ending {
    fn held(self) -> bool {
        matches!(self, Self::AckHeld | Self::ParentStopHeld)
    }

    fn parent_stop(self) -> bool {
        matches!(self, Self::ParentStopClean | Self::ParentStopHeld)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_direct_loaded_app_abort_retires_peer_and_releases_root() {
    exercise(Ending::PeerRetires, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_direct_loaded_app_abort_holds_unretired_provider_to_parent_q() {
    exercise(Ending::AckHeld, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_direct_loaded_parent_stop_retires_peer_and_releases_root() {
    exercise(Ending::ParentStopClean, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_direct_loaded_parent_stop_holds_unretired_provider_to_parent_q() {
    exercise(Ending::ParentStopHeld, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_direct_loaded_app_abort_retires_peer_and_releases_root() {
    exercise(Ending::PeerRetires, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_direct_loaded_app_abort_holds_provider_to_original_parent_q() {
    exercise(Ending::AckHeld, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_direct_loaded_parent_stop_retires_peer_and_releases_root() {
    exercise(Ending::ParentStopClean, true).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_direct_loaded_parent_stop_holds_provider_to_original_parent_q() {
    exercise(Ending::ParentStopHeld, true).await;
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
    let mut stream = None;
    let mut backing = None;
    let mut original_id = None;
    let mut loaded = None;

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
                            if flow.published == 1 && flow.running == 4 {
                                let contracts = owner.flow_contracts();
                                assert_eq!(contracts.len(), 1);
                                original_id = Some(contracts[0].id);
                                backing = owner.flow_native_backing_meter();
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(remote_open, app_open);
                } => {},
                report = service.run() => panic!("direct cancel Service ended before publication: {report:?}"),
            }
        }).await.unwrap();
        assert!(backing.is_some());
        assert!(app.tcp_reply.is_empty());
        let bridge = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap()
            .relay_event_test_bridge().unwrap();
        let baseline = bridge.snapshot().map_or(0, |sample| sample.generation);
        let payload = vec![0x5a; 1024 * 1024];
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                result = stream.as_mut().unwrap().0.write_all(&payload) => result.unwrap(),
                report = service.run() => panic!("direct cancel Service ended before peer write: {report:?}"),
            }
        }).await.unwrap();
        let observed = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                if let Some(sample) = bridge.snapshot() {
                    let id = original_id.unwrap();
                    if sample.generation > baseline
                        && sample.handle == id.handle
                        && sample.event_epoch == id.epoch
                        && sample.context.is_some_and(|ctx| {
                            ctx.epoch == id.epoch && ctx.pending_downlink_bytes > 0
                        }) {
                        loaded = Some(sample);
                        break;
                    }
                }
            }
        }).await;
        if observed.is_err() {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("direct cancel pre-abort missing charge: sample={:?} flow={:?} meter={:?}",
                bridge.snapshot(), owner.flow_observation(), backing.as_ref().unwrap().snapshot());
        }
        observed.expect("same Flow/epoch original SocketCtx pending DATA before App abort");
        let sample = loaded.unwrap();
        eprintln!("direct cancel pre-abort charge: generation={} identity={:?} pending={} meter={:?}",
            sample.generation, original_id.unwrap(), sample.context.unwrap().pending_downlink_bytes,
            backing.as_ref().unwrap().snapshot());
        assert!(app.tcp_reply.is_empty());

        if ending.held() {
            fault.rx_drop().unwrap();
        }
        if ending.parent_stop() {
            handle.request_stop();
            if ending == Ending::ParentStopClean {
                let (mut send, mut recv) = stream.take().unwrap();
                send.finish().unwrap();
                recv.stop(78u32.into()).unwrap();
                drop((send, recv));
            }
            return;
        }
        app.sockets.get_mut::<smoltcp::socket::tcp::Socket>(app.tcp).abort();
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                app.turn().await;
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                let flow = owner.flow_observation().unwrap();
                if flow.joined == 4 {
                    let contracts = owner.flow_contracts();
                    assert_eq!(contracts.len(), 1);
                    assert_eq!(Some(contracts[0].id), original_id);
                    assert_eq!((flow.published, flow.running, flow.released, flow.f), (1, 0, 0, 1));
                    assert!(flow.audit);
                    assert!(owner.ledger().first_failure.is_none());
                    eprintln!("direct cancel four JOIN: flow={flow:?} provider={:?} meter={:?}",
                        owner.flow_native_provider_view(), backing.as_ref().unwrap().snapshot());
                    break;
                }
            }
        }).await.unwrap();

        if ending == Ending::PeerRetires {
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
                        assert_eq!((flow.joined, flow.released, flow.f), (4, 1, 0));
                        assert!(flow.audit);
                        assert!(owner.ledger().first_failure.is_none() && !owner.ledger().quarantined);
                        let (messages, bytes, transferred, discarded, _) =
                            flow.last_data_disposition.expect("original physical receipt");
                        assert!(messages > 0 && bytes > 0);
                        assert_eq!(bytes, transferred + discarded);
                        assert!(owner.flow_native_provider_view().is_none());
                        break;
                    }
                }
            }).await.unwrap();
        }
    }).await;

    if !ending.parent_stop() {
        handle.request_stop();
    }
    let original_deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        if scenario.is_err() {
            fixture::observe_failed_native_stop(service.run(), &handle, original_deadline, None).await;
            return;
        }
        if ending.held() {
            let held_owner=hard.then(||saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap());
            let mut observed_first=None;
            let completed = if let Some(observing)=held_owner.as_ref() {
                let mut run=Box::pin(service.run());
                tokio::time::timeout_at(original_deadline,std::future::poll_fn(|cx| {
                    let result=run.as_mut().poll(cx);
                    assert_eq!(Some(observing.flow_contracts()[0].id),original_id);
                    let current=fixture::hard_q_refusal(observing,&saved,ending.parent_stop());
                    if observed_first.is_some() { assert_eq!(current,observed_first); }
                    else if current.is_some() { observed_first=current; }
                    result
                })).await
            } else { tokio::time::timeout_at(original_deadline, service.run()).await };
            assert!(completed.is_err());
            assert!(tokio::time::Instant::now() >= original_deadline);
            tokio::time::timeout_at(original_deadline + Duration::from_millis(100), async {
                while handle.phase() != Phase::Quarantined {
                    assert!(
                        tokio::time::timeout(Duration::from_millis(10), service.run())
                            .await
                            .is_err(),
                        "unretired direct provider falsely completed after parent deadline"
                    );
                }
            })
            .await
            .expect("original Service did not publish parent Q after deadline");
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let flow = owner.flow_observation().unwrap();
            let contracts = owner.flow_contracts();
            assert_eq!(contracts.len(), 1);
            assert_eq!(Some(contracts[0].id), original_id);
            let provider = owner.flow_native_provider_view().unwrap();
            let ledger = owner.ledger();
            eprintln!("direct cancel ACK-held parent: completed={} phase={:?} flow={flow:?} provider={provider:?} Q={} rx_dropped={}",
                completed.is_ok(), handle.phase(), ledger.quarantined, fault.snapshot().counts.rx_dropped_datagrams);
            assert_eq!(handle.phase(), Phase::Quarantined);
            assert_eq!((flow.joined, flow.released, flow.f), (4, 0, 1));
            assert_eq!(flow.last_data_disposition, None);
            assert_eq!(provider.phase, TcpOpenPhase::Retiring);
            assert!(provider.lease_held && provider.retirement_pending);
            assert!(ledger.quarantined);
            if !hard { assert!(ledger.first_failure.is_none()); }
            drop(ledger);
            assert!(fault.snapshot().counts.rx_dropped_datagrams > 0);
            assert_eq!(*saved.deadline.lock().unwrap(),Some(original_deadline));
            assert_eq!(owner.ledger().shutdown_deadline,Some(original_deadline));
            if let Some(original)=held_owner.as_ref() {
                assert!(Arc::ptr_eq(original,&owner));
                let budget=saved.backing_budget.as_ref().unwrap();
                assert!(budget.quarantined() && budget.used()>0);
                for _ in 0..2 {
                    let current=fixture::hard_q_refusal(original,&saved,ending.parent_stop());
                    if observed_first.is_some() { assert_eq!(current,observed_first); }
                    else if current.is_some() { observed_first=current; }
                    assert!(tokio::time::timeout(Duration::from_millis(5),service.run()).await.is_err());
                    assert_eq!(*saved.deadline.lock().unwrap(),Some(original_deadline));
                }
                if let Some(first)=observed_first { assert_eq!(fixture::first_failure_identity(original),Some(first)); }
            }
            return;
        }
        let report = tokio::time::timeout_at(original_deadline, service.run()).await.unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(handle.phase(), Phase::Complete);
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.joined, flow.released, flow.f), (4, 1, 0));
        assert!(flow.audit && !owner.ledger().quarantined && owner.ledger().first_failure.is_none());
        if ending == Ending::ParentStopClean {
            let (messages, bytes, transferred, discarded, _) =
                flow.last_data_disposition.expect("original parent-stop physical receipt");
            assert!(messages > 0 && bytes > 0);
            assert_eq!(bytes, transferred + discarded);
        }
        let physical = backing.as_ref().unwrap().snapshot();
        assert_eq!((physical.allocations, physical.payload_bytes, physical.physical_bytes), (0, 0, 0));
        if hard {
            assert_eq!(owner.ledger().shutdown_deadline,Some(original_deadline));
            assert_eq!(*saved.deadline.lock().unwrap(),Some(original_deadline));
            drop(app);
            assert_eq!(saved.app_meter.as_ref().unwrap().snapshot().physical_bytes,0);
            let budget=saved.backing_budget.as_ref().unwrap();
            assert_eq!(budget.used(),0); assert!(!budget.quarantined());
        }
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
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(drained.unwrap());
}

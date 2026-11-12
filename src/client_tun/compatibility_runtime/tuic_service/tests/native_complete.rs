//! Actual Native cold factory, QUIC AUTH/CONNECT, typed DATA, Main and Device.
//! Original Service/Flow/Node retain settlement authority. This functional
//! tracer does not accept global event inventory or the complete A/B/C matrix.

use super::*;
use crate::upstream::{
    NativeTcpRelayEngine, RelayFamilyPolicy, RelayFamilyPolicySources, TuicNativeOverride,
};
use fixture::{Apps, PublicCa, auth, capture, config};

#[tokio::test(flavor = "current_thread")]
async fn native_complete_service_chunk_data_and_original_owners_settle() {
    complete_service(false, ServiceUdp::None).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_complete_service_permit_capture_and_original_owners_settle() {
    complete_service(true, ServiceUdp::None).await;
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ServiceUdp {
    None,
    Datagram,
    Uni,
    Fragments,
}
#[tokio::test(flavor = "current_thread")]
async fn native_complete_service_udp_datagram_reaches_original_device_and_app() {
    complete_service(false, ServiceUdp::Datagram).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_complete_service_udp_uni_reaches_original_device_and_app() {
    complete_service(false, ServiceUdp::Uni).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_complete_service_udp_fragments_reach_original_device_and_app() {
    complete_service(false, ServiceUdp::Fragments).await;
}
async fn complete_service(permit: bool, udp: ServiceUdp) {
    complete_service_ending(permit, udp, terminal::Ending::Normal).await;
}
async fn complete_service_ending(permit: bool, udp: ServiceUdp, ending: terminal::Ending) {
    complete_service_observed(permit, udp, ending, 0).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_complete_source_retirement_runs_outside_original_opens() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 11).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_complete_service_actual_cold_roles_include_retirement_future() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 17).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_complete_service_full_preserves_established_uplink_before_allocation() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 18).await;
}
async fn complete_service_observed(permit: bool, udp: ServiceUdp, ending: terminal::Ending, source_probe: u8) {
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        native_egress_permit: permit.then_some("1"),
        ..Default::default()
    }));
    assert_eq!(
        policy.tuic_native_override(),
        TuicNativeOverride::NativeOrderedPump
    );
    assert_eq!(
        policy.native_engine(),
        if permit {
            NativeTcpRelayEngine::PermitPump
        } else {
            NativeTcpRelayEngine::ChunkPump
        }
    );
    let mut cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let mut runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy)
        .with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(32 * 1024 * 1024), 64)
        .unwrap();
    runtime.tun_mtu = 1500;
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    app.sent_udp = udp == ServiceUdp::None;
    let gate = Arc::new(device::Gate::default());
    gate.open();
    let observed_meter = Arc::new(std::sync::Mutex::new(None));
    let observed = observed_meter.clone();
    let actual = gate.clone();
    let factory = move |meter: Option<bytes::BackingMeter>| async move {
        let meter = meter.expect("original hard Native factory meter");
        *observed.lock().unwrap() = Some(meter.clone());
        Ok::<_, ClientError>(device::GatedDevice {
            inner: LoopbackTunDevice::native_with_factory(64, 1500, &meter, || {
                Ok((inbound, outbound))
            })?,
            gate: actual,
        })
    };
    let mut service = prepare_factory(
        factory,
        Configuration {
            tuic: cfg.clone(),
            composition: (),
        },
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().unwrap();
    let app_meter = saved.app_meter.clone().unwrap();
    let mut peer = None;
    let mut stream = None;
    let mut original_queue = None;
    let mut backing = None;
    let mut original_id = None;
    let mut intentional_q = false;
    let mut source_observer = None;
    let mut stock_bridge = None;

    let mut provider_observer = None;
    let mut timer_observation = None;
    let scenario = capture(async {
        let startup = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    if matches!(source_probe, 22..=28 | 30) {
                        let owner = saved.session.lock().unwrap().as_ref().unwrap().clone();
                        provider_observer = Some(send_inventory::ProviderProbe::install(owner, source_probe));
                    }
                    if source_probe != 0 && source_probe != 15 && !matches!(source_probe, 22..=30) {
                        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                        source_observer = Some(owner.cold_stop_probe(source_probe, budget.clone()));
                    }
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
                            let Some(flow) = owner.flow_observation() else {
                                tokio::task::yield_now().await;
                                continue;
                            };
                            if flow.published == 1 && flow.running == 5 {
                                let contracts = owner.flow_contracts();
                                assert_eq!(contracts.len(), 1);
                                original_id = Some(contracts[0].id);
                                original_queue = Some(if permit {
                                    ObservedQueue::Permit(owner.flow_native_permit_queue().unwrap())
                                } else {
                                    ObservedQueue::Chunk(owner.flow_native_chunk_queue().unwrap())
                                });
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
        }).await;
        if startup.is_err() {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let ledger = { let l = owner.ledger(); (l.quarantined(), l.shutdown_deadline, l.additional_failures) };
            eprintln!("NATIVE-SERVICE-START permit={permit} phase={:?} stream={} sent={} tcp={:?} flow={:?} contracts={:?} first={:?} ledger={:?} budget={} q={} app={:?}",
                handle.phase(), stream.is_some(), app.sent_tcp,
                app.sockets.get::<smoltcp::socket::tcp::Socket>(app.tcp).state(),
                owner.flow_observation(), owner.flow_contracts(), fixture::first_failure_identity(&owner),
                ledger, budget.used(), budget.quarantined(), app_meter.snapshot());
        }
        startup.expect("original five-second publication boundary");
        let queue = original_queue.as_ref().unwrap();
        assert!(backing.is_some() && app.tcp_reply.is_empty());
        assert!(observed_meter.lock().unwrap().as_ref().unwrap().same_authority(&app_meter));
        let node_meter = backing.as_ref().unwrap();
        assert!(!node_meter.same_authority(&app_meter));
        let ticket = budget.try_reserve(1).unwrap();
        assert!(node_meter.cold_layout_matches(Some(&ticket), 1));
        assert!(app_meter.cold_layout_matches(Some(&ticket), 1));
        drop(ticket);
        if matches!(source_probe, 15 | 16 | 31) {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let bridge = owner.relay_event_test_bridge().unwrap();
            let id = original_id.unwrap();
            bridge.arm_stock_alias(id.handle, id.epoch, owner.flow_stock_key_for_test(id).unwrap());
            stock_bridge = Some(bridge);
        }
        let payload: Vec<u8> = (0..8192).map(|index| (index % 251) as u8).collect();
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                result = stream.as_mut().unwrap().0.write_all(&payload) => result.unwrap(),
                report = service.run() => panic!("ordered Chunk Service ended before peer DATA: {report:?}"),
            }
        }).await.unwrap();
        let delivered = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                app.turn().await;
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                if app.tcp_reply.len() >= 8192 {
                    break;
                }
            }
        }).await;
        if delivered.is_err() {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("ordered Chunk delivery deferred: app={} queue={:?} flow={:?} meter={:?}",
                app.tcp_reply.len(), queue.snapshot().await, owner.flow_observation(),
                backing.as_ref().unwrap().snapshot());
        }
        delivered.expect("ordered Chunk App exact delivery before original 5 seconds");
        assert_eq!(app.tcp_reply, payload);
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let contracts = owner.flow_contracts();
        assert_eq!(contracts.len(), 1);
        assert_eq!(Some(contracts[0].id), original_id);
        let custody = queue.snapshot().await;
        eprintln!("ordered Chunk App delivery: id={:?} queue={custody:?} emitted={} consumed={} discarded={} source={:?} meter={:?}",
            original_id.unwrap(), custody.emitted, custody.consumed, custody.discarded,
            owner.flow_native_pump_source_read_bytes(), backing.as_ref().unwrap().snapshot());
        assert_eq!(custody.emitted, 8192);
        assert_eq!(custody.consumed, 8192);
        assert_eq!(custody.discarded, 0);
        assert_eq!(owner.flow_native_pump_source_read_bytes(), Some(8192));
        custody.assert_capacity();
        assert!(owner.ledger().first_failure.is_none());

        assert_eq!(custody.data_held(), 0);
        queue.assert_proofs();
        drop(owner);
        if udp != ServiceUdp::None {
            let wire = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                tokio::select! {
                    wire = peer.as_ref().unwrap().read_datagram() => wire.unwrap(),
                    report = service.run() => panic!("Service ended before real UDP uplink: {report:?}"),
                }
            }).await.unwrap();
            let meta = crate::tuic::decode_packet_meta(&wire).unwrap();
            assert_eq!(meta.data, b"service-udp");
            let association = meta.assoc_id;
            let expected = b"original-service-udp-reply";
            let target = TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());
            let reply = crate::tuic::encode_packet(association, &target, expected);
            match udp {
                ServiceUdp::Datagram => peer.as_ref().unwrap().send_datagram(reply.into()).unwrap(),
                ServiceUdp::Uni => {
                    let mut stream = peer.as_ref().unwrap().open_uni().await.unwrap();
                    stream.write_all(&reply).await.unwrap(); stream.finish().unwrap();
                }
                ServiceUdp::Fragments => {
                    for (id, bytes) in [expected[..9].as_ref(), expected[9..].as_ref()].into_iter().enumerate() {
                        let mut frame = vec![5, 2]; frame.extend_from_slice(&association.to_be_bytes());
                        frame.extend_from_slice(&[0, 1, 2, id as u8]);
                        frame.extend_from_slice(&(bytes.len() as u16).to_be_bytes()); frame.push(0xff); frame.extend_from_slice(bytes);
                        peer.as_ref().unwrap().send_datagram(frame.into()).unwrap();
                    }
                }
                ServiceUdp::None => unreachable!(),
            }
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                while app.udp_reply.len() < expected.len() {
                    app.turn().await;
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                }
            }).await.unwrap();
            assert_eq!(app.udp_reply, expected);
            assert!(saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap().ledger().first_failure.is_none());
        }
        if source_probe == 29 {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let bridge = owner.relay_event_test_bridge().unwrap();
            let before = owner.ledger().shutdown_deadline;
            timer_observation = Some(timer::observe_delayed_main(service.run(), &bridge).await);
            assert_eq!(owner.ledger().shutdown_deadline, before);
        }
        if source_probe == 19 {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                tokio::select! {
                    _ = initial_uplink::reject_and_continue(&mut app, stream.as_mut().unwrap(), &owner) => {},
                    report = service.run() => panic!("one rejected initial Flow stopped the Service: {report:?}"),
                }
            }).await.unwrap();
        }
        if source_probe == 21 {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                tokio::select! {
                    _ = established_uplink::quarantine(&mut app, &owner, &budget) => {},
                    report = service.run() => panic!("unknown uplink Q cannot claim settled: {report:?}"),
                }
            }).await.unwrap();
            intentional_q = true;
            return;
        }
        if source_probe == 20 {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                tokio::select! {
                    _ = established_uplink::reject_and_continue(&mut app, peer.as_ref().unwrap(), stream.as_mut().unwrap(), &owner) => {},
                    report = service.run() => panic!("one rejected published Flow stopped Service: {report:?}"),
                }
            }).await.unwrap();
        }
        if source_probe == 18 {
            let extra: Vec<u8> = (0..4096).map(|i| (i % 239) as u8).collect();
            assert_eq!(app.sockets.get_mut::<smoltcp::socket::tcp::Socket>(app.tcp).send_slice(&extra).unwrap(), extra.len());
            let mut received = vec![0; extra.len()];
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                tokio::select! {
                    result = stream.as_mut().unwrap().1.read_exact(&mut received) => result.unwrap(),
                    _ = async { loop { app.turn().await; tokio::task::yield_now().await; } } => unreachable!(),
                    report = service.run() => panic!("Service ended during actual established uplink: {report:?}"),
                }
            }).await.unwrap();
            assert_eq!(received, extra, "original channel and QUIC writer preserve the full payload after actual pressure release");
        }
        if ending == terminal::Ending::DeviceFatal {
            terminal::fatal(&mut service, &handle, &gate, backing.as_ref().unwrap(), &mut app).await;
            assert!(peer.as_ref().unwrap().close_reason().is_none());
            intentional_q = true;
            return;
        }
        if ending == terminal::Ending::CancelShutdown {
            gate.release.store(false, std::sync::atomic::Ordering::SeqCst);
        }
        // A queue Arc retains actual cold descriptor backing even when empty.
        // Keep its terminal values above, release this observer before Root drain.
        drop(original_queue.take());
        if matches!(source_probe, 23..=28 | 30) {
            *saved.deadline.lock().unwrap() = Some(tokio::time::Instant::now() + Duration::from_secs(3));
        }
        handle.request_stop();
        let (mut send, mut recv) = stream.take().unwrap();
        send.finish().unwrap();
        recv.stop(78u32.into()).unwrap();
        drop((send, recv));
        if matches!(source_probe, 23..=28 | 30) {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let original = saved.deadline.lock().unwrap().unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while !owner.ledger().quarantined() {
                    assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
                }
            }).await.unwrap();
            provider_observer.as_ref().unwrap().assert_fault(&owner, original);
            assert!(budget.quarantined() && budget.used() > 0);
            intentional_q = true;
            return;
        }
        if source_probe == 31 {
            let bridge = stock_bridge.as_ref().unwrap();
            let alias = bridge.take_stock_alias();
            let allocation = alias.allocation_identity().unwrap();
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let original = saved.deadline.lock().unwrap().unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while !budget.quarantined() {
                    assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
                }
            }).await.unwrap();
            let probe = source_observer.as_ref().unwrap();
            assert_eq!(probe.fired(), 1);
            assert_eq!(probe.deadline(), Some(original));
            assert_eq!(owner.ledger().shutdown_deadline, Some(original));
            let flow = owner.flow_observation().unwrap();
            assert_eq!((flow.f, flow.released, flow.joined), (1, 0, 5));
            assert_eq!(alias.allocation_identity(), Some(allocation));
            let charged = budget.used();
            drop(alias);
            assert_eq!(budget.used(), charged);
            assert!(budget.quarantined() && charged > 0 && service.completion.is_none());
            intentional_q = true;
            return;
        }
        if source_probe == 16 {
            let alias = stock_bridge.as_ref().unwrap().take_stock_alias();
            let allocation = alias.allocation_identity().unwrap();
            let original = saved.deadline.lock().unwrap().unwrap();
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
                while !budget.quarantined() {
                    assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
                }
            }).await.unwrap();
            let probe = source_observer.as_ref().unwrap();
            assert_eq!(probe.fired(), 1);
            assert!(probe.stock_wait().is_some_and(|(ptr, size)| ptr != 0 && size > 0));
            assert_eq!((probe.dropped(), probe.writer_drops()), (1, 1));
            assert_eq!(probe.deadline(), Some(original));
            assert_eq!(owner.ledger().shutdown_deadline, Some(original));
            assert_eq!(*saved.deadline.lock().unwrap(), Some(original));
            let flow = owner.flow_observation().unwrap();
            assert_eq!((flow.f, flow.released, flow.joined), (1, 0, 5));
            assert!(owner.ledger().first_failure.is_none());
            assert_eq!(alias.allocation_identity(), Some(allocation));
            assert!(stock_bridge.as_ref().unwrap().stock_receipt().is_some());
            let key = stock_bridge.as_ref().unwrap().stock_key_for_test();
            assert_eq!(key.loans_for_test(), Some((2, 1, alias.len(), false)));
            let retained = budget.used();
            drop(alias);
            assert_eq!(key.loans_for_test(), Some((2, 0, 0, false)), "the original stock remains physically owned by the lost waiting future");
            assert_eq!(budget.used(), retained, "lost turn retains the actual stock and waiting future, even after last loan");
            assert!(budget.quarantined() && retained > 0 && service.completion.is_none());
            intentional_q = true;
            return;
        }
        if source_probe == 15 {
            use std::{future::Future, task::{Context, Poll, Wake, Waker}, sync::atomic::{AtomicUsize, Ordering}};
            struct CountWake(AtomicUsize);
            impl Wake for CountWake { fn wake(self: Arc<Self>) { self.wake_by_ref(); }
                fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); } }
            let bridge = stock_bridge.as_ref().unwrap();
            let alias = bridge.take_stock_alias();
            let allocation = alias.allocation_identity();
            assert!(allocation.is_some() && !alias.is_empty());
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            let id = original_id.unwrap();
            let observation_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            tokio::time::timeout_at(observation_deadline, async {
                loop {
                    assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
                    if bridge.stock_receipt().is_some() && owner.flow_roles_data_disposed_for_test(id) == Some(true) { break; }
                }
            }).await.unwrap();
            let receipt = bridge.stock_receipt().unwrap();
            assert!(receipt.0 > 0 && receipt.1 >= 8192);
            assert_eq!(receipt.1, receipt.2 + receipt.3);
            let flow = owner.flow_observation().unwrap();
            assert_eq!((flow.f, flow.released, flow.joined), (1, 0, 5));
            assert_eq!(owner.flow_roles_settled_for_test(id), Some(false));
            assert!(budget.used() > 0 && !budget.quarantined());
            let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
            let waker = Waker::from(wakes.clone());
            // Poll the same original run with this observer, then cancel it.
            // The original Roles retirement future and its registered proxy stay.
            let key = bridge.stock_key_for_test();
            assert_eq!(key.loans_for_test(), Some((2, 1, alias.len(), false)));
            tokio::time::timeout_at(observation_deadline, async {
                loop {
                    let mut run = Box::pin(service.run());
                    assert!(matches!(run.as_mut().poll(&mut Context::from_waker(&waker)), Poll::Pending));
                    drop(run);
                    if key.retirement_waiter_matches_for_test(&waker) { break; }
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
            let charged = budget.used();
            let before = wakes.0.load(Ordering::SeqCst);
            assert_eq!(alias.allocation_identity(), allocation);
            drop(alias); // No await/poll/timer between the two wake samples.
            assert!(wakes.0.load(Ordering::SeqCst) > before, "actual final view must notify the original retirement waiter");
            assert_eq!(budget.used(), charged, "last view returns pool availability, not global backing");
            assert!(!budget.quarantined());
        }
        if (12..=14).contains(&source_probe) {
            let original = saved.deadline.lock().unwrap().unwrap();
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while !budget.quarantined() {
                    assert!(tokio::time::timeout(Duration::from_millis(5), service.run()).await.is_err());
                }
            }).await.unwrap();
            let probe = source_observer.as_ref().unwrap();
            assert_eq!(probe.fired(), 1);
            assert_eq!(probe.dropped(), 0, "original Source field retained by the actual failed turn");
            assert_eq!(probe.deadline(), Some(original));
            assert_eq!(owner.ledger().shutdown_deadline, Some(original));
            assert_eq!(*saved.deadline.lock().unwrap(), Some(original));
            let flow = owner.flow_observation().unwrap();
            assert_eq!((flow.released, flow.f), (0, 1));
            assert!(budget.used() > 0 && service.completion.is_none());
            intentional_q = true;
            return;
        }
        if ending == terminal::Ending::CancelShutdown {
            terminal::cancelled_shutdown(&mut service, &handle, &gate).await;
        }
    }).await;

    if intentional_q && scenario.is_ok() {
        // Explicit forced teardown only after all sticky-Q assertions. It is
        // never an async Node/Device clean verdict or a substitute JOIN.
        drop(original_queue.take());
        peer.take()
            .unwrap()
            .close(0u32.into(), b"forced Q teardown");
        drop(stream.take());
        drop(service);
        drop(app);
        let drain = server.take_driver_drain_handle().unwrap();
        server.close(0u32.into(), b"forced Q teardown");
        drop(server);
        assert!(
            tokio::time::timeout(Duration::from_secs(10), drain.drained())
                .await
                .unwrap()
        );
        assert!(budget.quarantined() && budget.used() > 0);
        assert_eq!(gate.joins.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(gate.drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        return;
    }
    if scenario.is_err() {
        handle.request_stop();
    }
    drop(original_queue.take());
    let original_deadline = {
        let saved_deadline = saved.deadline.lock().unwrap().unwrap();
        if source_probe == 20 { saved_deadline.min(source_observer.as_ref().unwrap().deadline().unwrap()) } else { saved_deadline }
    };
    let cleanup = capture(async {
        if scenario.is_err() {
            fixture::observe_failed_native_stop(service.run(), &handle, original_deadline, None).await;
            let owner = saved.session.lock().unwrap().as_ref().and_then(|s| s.upgrade());
            eprintln!("NATIVE-SERVICE-STOP permit={permit} deadline={original_deadline:?} current={:?} budget={} q={} app={:?} first={:?} flow={:?}",
                *saved.deadline.lock().unwrap(), budget.used(), budget.quarantined(), app_meter.snapshot(),
                owner.as_ref().and_then(|o| fixture::first_failure_identity(o)), owner.as_ref().and_then(|o| o.flow_observation()));
            return;
        }
        let report = tokio::time::timeout_at(original_deadline, service.run()).await;
        if report.is_err() {
            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            eprintln!("[N1-native-final-timeout] permit={permit} probe={source_probe} used={} Q={} phase={:?} flow={:?} first={:?} stock={:?}",
                budget.used(), budget.quarantined(), handle.phase(), owner.flow_observation(), fixture::first_failure_identity(&owner), owner.flow_stock_inventory_for_test());
            for row in owner.flow_contracts() {
                eprintln!("[N1-native-final-roles] id={:?} actual={:?}", row.id, owner.flow_native_retirement_for_test(row.id));
            }
            if let Some(queue) = owner.flow_native_permit_queue() {
                eprintln!("[N1-native-final-permit] actual={:?} release={:?}", queue.snapshot_now(), queue.permit_release_audit_for_test());
            }
        }
        let report = report.unwrap();
        assert_eq!(report.session, if matches!(source_probe, 19 | 20) { SessionDeviceState::DeviceFailedSettled } else { SessionDeviceState::DeviceSettled });
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(handle.phase(), Phase::Complete);
        let owner = service.completion.as_ref().unwrap().control.clone();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.joined, flow.released, flow.f), (if source_probe == 20 { 2 } else { 1 }, if source_probe == 20 { 10 } else { 5 }, if matches!(source_probe, 19 | 20) { 2 } else { 1 }, 0));
        assert!(flow.audit && !owner.ledger().quarantined());
        if source_probe == 19 {
            owner.assert_initial_uplink_refusal();
        } else if source_probe == 20 { owner.assert_established_uplink_refusal(); } else { assert!(owner.ledger().first_failure.is_none()); }
        let (messages, bytes, transferred, discarded, _) =
            flow.last_data_disposition.expect("original ordered Chunk physical receipt");
        assert!(messages > 0 && bytes >= 8192);
        assert_eq!(bytes, transferred + discarded);
        let meter = backing.as_ref().unwrap().snapshot();
        assert_eq!((meter.allocations, meter.payload_bytes, meter.physical_bytes), (0, 0, 0));
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original_deadline));
        assert_eq!(owner.ledger().shutdown_deadline, Some(original_deadline));
        assert_eq!(gate.joins.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(gate.drops.load(std::sync::atomic::Ordering::SeqCst), 1);
        drop(app); // External peer may still own the final Native ACK/FIN backing.
        let app = app_meter.snapshot();
        assert_eq!((app.allocations, app.payload_bytes, app.physical_bytes), (0, 0, 0));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
        if let Some(probe) = &source_observer {
            assert_eq!((probe.dropped(), probe.writer_drops()), if source_probe == 20 { (2, 2) } else { (1, 1) });
            if source_probe == 17 {
                // Historical component subtotal from the signed 1202 epoch.
                // It deliberately excluded the production retirement future.
                const HISTORICAL_ROLE_COMPONENTS: usize = 68_120 + 352;
                assert!(probe.cold_bill() > HISTORICAL_ROLE_COMPONENTS,
                    "production cold billing must include the actual retirement future");
                eprintln!("[N1-real-Flow-cold] roles={} plus_actual_original_main=65856 plus_clock=160 subtotal={} historical_components=134488 missing_from_old_subtotal={} remaining_endpoint_actor_platform_smoltcp_costs_separate=true",
                    probe.cold_bill(), probe.cold_bill() + 65_856 + 160,
                    probe.cold_bill() - HISTORICAL_ROLE_COMPONENTS);
            }
        }
        eprintln!("Native complete Service terminal: flow={flow:?} node={meter:?} app={app:?} budget={} deadline={original_deadline:?}", budget.used());
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
    if source_probe == 18 { source_observer.as_ref().unwrap().assert_uplink_full_preserved_socket(); }
    if source_probe == 22 { provider_observer.as_ref().unwrap().assert_clean(); }
    if let Some(observed) = timer_observation { timer::assert_coalesced(observed); }
}

// Passive clones are released before the original Root settles; they may keep
// real cold allocations alive and are never used as replacement ownership.
enum ObservedQueue {
    Chunk(crate::tcp_downlink_pump::AsyncByteBoundedFlowQueue),
    Permit(crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue),
}
#[derive(Debug)]
struct QueueSnapshot {
    queued: usize,
    leased: usize,
    reserved: usize,
    capacity: usize,
    emitted: usize,
    consumed: usize,
    discarded: usize,
}
impl QueueSnapshot {
    fn data_held(&self) -> usize { self.queued.checked_add(self.leased).unwrap() }
    fn assert_capacity(&self) { assert!(self.data_held().checked_add(self.reserved).unwrap() <= self.capacity); }
}
impl ObservedQueue {
    async fn snapshot(&self) -> QueueSnapshot {
        match self {
            Self::Chunk(queue) => {
                let (p, c) = queue.custody_snapshot().await;
                QueueSnapshot {
                    queued: p.queued_bytes,
                    leased: 0, reserved: 0,
                    capacity: p.capacity_bytes,
                    emitted: c.emitted,
                    consumed: c.consumed,
                    discarded: c.discarded_bytes,
                }
            }
            Self::Permit(queue) => {
                let p = queue.snapshot_now();
                let c = queue.permit_release_audit_for_test().unwrap();
                queue.assert_stock_credit_for_test(p);
                QueueSnapshot {
                    queued: p.queued_bytes, leased: p.leased_bytes, reserved: p.reserved_bytes,
                    capacity: p.capacity_bytes,
                    emitted: c.emitted,
                    consumed: c.consumed,
                    discarded: c.discarded,
                }
            }
        }
    }
    fn assert_proofs(&self) {
        if let Self::Permit(queue) = self {
            let audit = queue.permit_release_audit_for_test().unwrap();
            assert_eq!(audit.released, 8192);
            assert_eq!(audit.packet_flush + audit.ack_drain, 8192);
            assert_eq!(
                (audit.explicit_other, audit.dropped, audit.interrupted),
                (0, 0, false)
            );
            assert_eq!((audit.queued, audit.leased), (0, 0));
            eprintln!("actual Native capture proof: {audit:?}");
        }
    }
}

mod udp_source;

mod pressure;

mod terminal;

mod rejection;

mod direct;

mod direct_gap;

mod cold;

#[tokio::test(flavor = "current_thread")]
async fn native_complete_source_lost_disposal_retains_real_actor_and_original_deadline() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 12).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_complete_source_wrong_epoch_disposal_retains_real_actor() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 13).await;
}
#[tokio::test(flavor = "current_thread")]
async fn native_complete_source_disposal_q_preserves_original_stop_and_actor() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 14).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_complete_stock_last_alias_notifies_after_main_receipt_and_run_cancel() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 15).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_complete_lost_stock_pending_turn_keeps_original_inventory_and_deadline() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 16).await;
}

mod initial_uplink;

mod established_uplink;

mod uplink_udp;

mod uplink_heartbeat;

mod uplink_custody;

mod uplink_deadline;
mod send_inventory;

mod capacity;
mod leased_stock;

mod timer;

mod policy_admission;

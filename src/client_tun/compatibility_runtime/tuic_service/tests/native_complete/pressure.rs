//! Hard Native Service pressure through original Root/queue/source/Main/Device.
//! Test peer and observers carry no settlement authority and release before stop.
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_chunk_full_queue_stalls_then_recovers_original_budget() {
    pressure(false).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_permit_full_queue_stalls_then_recovers_original_budget() {
    pressure(true).await;
}
async fn pressure(permit: bool) {
    const REPLY: usize = 12 * 1024 * 1024;
    let identity = PublicCa::new();
    let mut remote_config = identity.server_config();
    let mut cubic = quinn::congestion::CubicConfig::default();
    cubic.initial_window(8 * 1024 * 1024);
    Arc::get_mut(&mut remote_config.transport)
        .unwrap()
        .congestion_controller_factory(Arc::new(cubic));
    let mut server =
        quinn::Endpoint::server(remote_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: Some("1"),
        native_egress_permit: permit.then_some("1"),
        ..Default::default()
    }));
    let mut cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone())
        .with_transport_shape(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_large_shape());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let mut runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy)
        .with_native_limits((32 << 20) + crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_reserve_for(&crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_large_shape()), 64)
        .unwrap();
    runtime.tun_mtu = 1500;
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut app = Apps::new(outbound.clone(), inbound.clone());
    app.sent_udp = true;
    let mut service = prepare_factory(
        move |meter: Option<bytes::BackingMeter>| async move {
            let meter = meter.unwrap();
            Ok::<_, ClientError>(LoopbackTunDevice::native_with_factory(
                64,
                1500,
                &meter,
                || Ok((inbound, outbound)),
            )?)
        },
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
    let mut queue = None;
    let mut backing = None;
    let mut identity_seen = None;
    let mut writer = None;
    let scenario = capture(async {
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::select! {
                _ = async {
                    let remote = server.accept().await.unwrap().await.unwrap();
                    auth(&remote, &cfg).await;
                    peer = Some(remote.clone());
                    let opening = async {
                        let (send, mut recv) = remote.accept_bi().await.unwrap();
                        let mut expected = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                        expected.extend_from_slice(b"service-tcp");
                        let mut actual = vec![0; expected.len()]; recv.read_exact(&mut actual).await.unwrap();
                        assert_eq!(actual, expected); stream = Some((send, recv));
                    };
                    let publishing = async {
                        loop {
                            app.turn().await;
                            let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                            if owner.flow_observation().is_some_and(|f| f.published == 1 && f.running == 5) {
                                identity_seen = Some(owner.flow_contracts()[0].id);
                                queue = Some(if permit { ObservedQueue::Permit(owner.flow_native_permit_queue().unwrap()) }
                                    else { ObservedQueue::Chunk(owner.flow_native_chunk_queue().unwrap()) });
                                backing = owner.flow_native_backing_meter(); break;
                            }
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    tokio::join!(opening, publishing);
                } => {},
                report = service.run() => panic!("hard Service ended before publication: {report:?}"),
            }
        }).await.unwrap();
        let original = queue.as_ref().unwrap();
        let (mut send, mut recv) = stream.take().unwrap();
        writer = Some(tokio::spawn(async move {
            let payload: Vec<u8> = (0..REPLY).map(|n| (n % 251) as u8).collect();
            send.write_all(&payload).await.unwrap(); send
        }));
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let pending = owner.relay_event_test_bridge().unwrap();
        let id = identity_seen.unwrap();
        pending.arm_pending_probe(id.handle, id.epoch);
        let full = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            loop {
                assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                let physical = original.snapshot().await;
                physical.assert_capacity();
                let data_full = match original {
                    ObservedQueue::Permit(q) => {
                        let stock = q.snapshot_now();
                        stock.reserved_bytes == 0 && stock.queued_bytes + stock.leased_bytes == stock.capacity_bytes
                    },
                    ObservedQueue::Chunk(_) => physical.queued == physical.capacity,
                };
                if data_full && pending.pending_probe().is_some_and(|(bytes, can_send)| bytes > 0 && !can_send) {
                    if let ObservedQueue::Permit(q) = original {
                        eprintln!("[N1-Permit-full] physical={:?} release={:?}", q.snapshot_now(), q.permit_release_audit_for_test());
                    }
                    break physical;
                }
            }
        }).await;
        if full.is_err() {
            eprintln!("hard Native pressure before full permit={permit} queue={:?} node={:?} app={:?} budget={} q={}", original.snapshot().await,
                backing.as_ref().unwrap().snapshot(), app_meter.snapshot(), budget.used(), budget.quarantined());
        }
        let full = full.expect("original queue did not reach exact capacity");
        assert_eq!(owner.flow_contracts()[0].id, identity_seen.unwrap());
        // Let the already admitted original source turn settle before measuring.
        let until = tokio::time::Instant::now() + Duration::from_millis(100);
        while tokio::time::Instant::now() < until {
            assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
            if let ObservedQueue::Permit(q) = original {
                eprintln!("[N1-Permit-settle] physical={:?} release={:?} source={:?}", q.snapshot_now(), q.permit_release_audit_for_test(), owner.flow_native_pump_source_read_bytes());
            }
            let current = original.snapshot().await;
            current.assert_capacity();
            assert_eq!((current.data_held(), current.reserved), (full.capacity, 0));
        }
        let before = owner.flow_native_pump_source_read_bytes().unwrap();
        let until = tokio::time::Instant::now() + Duration::from_millis(200);
        while tokio::time::Instant::now() < until {
            assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
            let current = original.snapshot().await;
            current.assert_capacity();
            assert_eq!((current.data_held(), current.reserved), (full.capacity, 0));
        }
        assert_eq!(owner.flow_native_pump_source_read_bytes(), Some(before));
        assert!(before < REPLY && app.tcp_reply.is_empty());
        assert!(!budget.quarantined() && backing.as_ref().unwrap().snapshot().payload_bytes > 0);
        eprintln!("hard Native pressure: permit={permit} flow={identity_seen:?} physical={full:?} source={before} budget={}", budget.used());
        // D47 R2: recovery fails on a 5s App-progress stall (cap 12×), not on a
        // fixed completion bound; a recorded first failure fails at once.
        let delivered = std::cell::Cell::new(0u64);
        let recovery = fixture::progress_window(Duration::from_secs(5), &delivered, async {
            tokio::select! {
                _ = async {
                    loop {
                        app.turn().await;
                        assert!(app.tcp_reply.len() <= REPLY);
                        delivered.set(app.tcp_reply.len() as u64);
                        let failed = owner.ledger().first_failure.is_some();
                        assert!(!failed, "recovery recorded a first failure: {:?}", fixture::first_failure_identity(&owner));
                        if app.tcp_reply.len() == REPLY && original.snapshot().await.data_held() == 0 { break; }
                    }
                } => {},
                report = service.run() => panic!("hard Service ended before exact recovery: {report:?}"),
            }
        }).await;
        if recovery.is_err() {
            eprintln!("[N1-custody-recovery-timeout] permit={permit} App_received={} target={REPLY} queue={:?} source={:?} flow={:?} first={:?} used={} Q={}",
                app.tcp_reply.len(), original.snapshot().await, owner.flow_native_pump_source_read_bytes(),
                owner.flow_observation(), fixture::first_failure_identity(&owner), budget.used(), budget.quarantined());
        }
        recovery.unwrap();
        assert!(app.tcp_reply.iter().enumerate().all(|(n,b)| *b == (n % 251) as u8));
        assert_eq!(owner.flow_native_pump_source_read_bytes(), Some(REPLY));
        let physical = original.snapshot().await;
        assert_eq!((physical.emitted, physical.consumed, physical.discarded), (REPLY, REPLY, 0));
        if let ObservedQueue::Permit(q) = original {
            let audit = q.permit_release_audit_for_test().unwrap();
            assert_eq!((audit.released, audit.packet_flush + audit.ack_drain), (REPLY, REPLY));
            assert_eq!((audit.explicit_other, audit.dropped, audit.interrupted), (0,0,false));
        }
        assert!(owner.ledger().first_failure.is_none());
        let mut send = writer.take().unwrap().await.unwrap(); send.finish().unwrap(); recv.stop(78u32.into()).unwrap(); drop((send, recv));
    }).await;
    // A passive queue observer retains its real cold allocation. Release before
    // requiring the original Service's aggregate same-budget zero verdict.
    drop(queue.take());
    handle.request_stop();
    let deadline = saved.deadline.lock().unwrap().unwrap();
    let cleanup = capture(async {
        if scenario.is_err() {
            fixture::observe_failed_native_stop(service.run(), &handle, deadline, None).await; return;
        }
        let report = tokio::time::timeout_at(deadline, service.run()).await.unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        let owner = service.completion.as_ref().unwrap().control.clone();
        let flow = owner.flow_observation().unwrap();
        assert_eq!((flow.published, flow.joined, flow.released, flow.f), (1,5,1,0));
        assert!(flow.audit && owner.ledger().first_failure.is_none() && !owner.ledger().quarantined());
        let (_, bytes, transferred, discarded, _) = flow.last_data_disposition.unwrap();
        assert_eq!((bytes, transferred, discarded), (REPLY,REPLY,0));
        assert_eq!(backing.as_ref().unwrap().snapshot().physical_bytes, 0);
        assert_eq!(*saved.deadline.lock().unwrap(), Some(deadline));
        drop(app);
        assert_eq!(app_meter.snapshot().physical_bytes, 0);
        assert_eq!(budget.used(), 0); assert!(!budget.quarantined());
        eprintln!("hard Native pressure terminal: permit={permit} flow={flow:?} budget=0/nonQ deadline={deadline:?}");
    }).await;
    if let Some(peer) = peer.take() {
        peer.close(0u32.into(), b"test cleanup");
    }
    if let Some(writer) = writer.take() {
        let _ = tokio::time::timeout(Duration::from_secs(2), writer).await;
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"test cleanup");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    scenario.unwrap_or_else(|p| std::panic::resume_unwind(p));
    cleanup.unwrap_or_else(|p| std::panic::resume_unwind(p));
    assert!(drained.unwrap());
}

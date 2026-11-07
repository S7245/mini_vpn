//! Unknown original capabilities stay in the saved driver's selected/lane slots.
//! Real Service/QUIC/TCP/UDP; final forced Drop never certifies asynchronous clean.
use super::*;
use crate::client_tun::legacy_event_loop::RelayEvent;
use crate::tcp_downlink_pump::{AsyncLeasedByteFlowQueue, DownstreamPermitReleaseMode};
use device::{Gate, GatedDevice};
use std::sync::atomic::Ordering;
#[path = "../../../../../tests/tuic_service/pressure_fixture.rs"]
mod wire;

#[tokio::test(flavor = "current_thread")]
async fn foreign_lease_stays_in_original_selected_and_second_lane_during_q() {
    let server = wire::server();
    let cfg = wire::config(&server);
    let (inner, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let gate = Arc::new(Gate::default());
    gate.open();
    gate.cut_enabled.store(true, Ordering::Release);
    let mut service = prepare(
        GatedDevice {
            inner,
            gate: gate.clone(),
        },
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut app = wire::App::new(application);
    let mut peer = None;
    let mut stream = None;
    let foreign = AsyncLeasedByteFlowQueue::new(8).unwrap();
    let scenario = wire::capture(async {
        wire::drive(&mut service, async {
            let p = server.accept().await.unwrap().await.unwrap();
            wire::auth(&p, &cfg).await;
            peer = Some(p);
        }).await;
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        stream = Some(wire::drive(&mut service, async {
            let remote = async { tokio::join!(wire::healthy(peer.as_ref().unwrap()), wire::udp_reply(peer.as_ref().unwrap())) };
            let application = async {
                while app.tcp_reply != b"first" || app.udp_reply != b"udp-reply" {
                    app.turn().await;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            let ((stream, ()), ()) = tokio::join!(remote, application);
            stream
        }).await);
        let contracts = owner.flow_contracts();
        assert_eq!(contracts.len(), 1);
        assert!(contracts[0].published);
        let id = contracts[0].id;
        let bridge = owner.relay_event_test_bridge().unwrap();
        assert_eq!(bridge.queued_len(), 0);
        assert!(owner.ledger().first_failure.is_none());
        foreign.push(bytes::Bytes::from_static(b"12345678")).await.unwrap();
        let (_, first) = foreign.recv_up_to(4).await.unwrap().into_parts();
        let (_, second) = foreign.recv_up_to(4).await.unwrap().into_parts();
        let mut bytes = Vec::with_capacity(19);
        bytes.extend_from_slice(b"bad1");
        let original = (id.epoch, bytes.len(), bytes.as_ptr() as usize, 0);
        let capability = (bytes.capacity(), Some((4, DownstreamPermitReleaseMode::OnAdmission)));
        assert!(bridge.try_send(id.handle, RelayEvent::Data { epoch: id.epoch, bytes: bytes.into(),
            stream_service_window: Default::default(), downstream_permit: Some(first) }).is_accepted());
        assert!(bridge.try_send(id.handle, RelayEvent::Data { epoch: id.epoch, bytes: b"bad2".to_vec().into(),
            stream_service_window: Default::default(), downstream_permit: Some(second) }).is_accepted());
        assert_eq!(foreign.snapshot_now().leased_bytes, 8);
        wire::drive(&mut service, async {
            loop {
                if owner.ledger().first_failure.is_some() || foreign.snapshot_now().leased_bytes != 8 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await;
        assert_eq!(foreign.snapshot_now().leased_bytes, 8, "unknown lease must remain physically charged");
        assert_eq!(bridge.selected_data(), Some(original));
        assert_eq!(bridge.selected_capability(), Some(capability));
        assert!(bridge.queued_len() >= 1);
        let first = {
            let ledger = owner.ledger();
            let error = ledger.first_failure.as_ref().unwrap();
            (error as *const _ as usize, format!("{:?}", error.facts()), ledger.shutdown_deadline.unwrap())
        };
        let mut observer = Box::pin(handle.observe_shutdown());
        let mut phase = None;
        for _ in 0..4 {
            if let Ok(value) = tokio::time::timeout(Duration::from_millis(1), observer.as_mut()).await {
                phase = Some(value);
                break;
            }
            assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
        }
        let phase = match phase {
            Some(value) => value,
            None => tokio::select! {
                value = observer.as_mut() => value,
                report = service.run() => panic!("unknown selected must not complete {report:?}"),
            },
        };
        drop(observer);
        if phase == Phase::Incomplete {
            let parent = *saved.deadline.lock().unwrap();
            assert!(tokio::time::Instant::now() >= parent.unwrap());
            assert_eq!(foreign.snapshot_now().leased_bytes, 8);
            println!("original observer Incomplete ownerlimit={:?} parent={parent:?}", first.2);
            // Independent cold census only, never a new retirement deadline.
            wire::drive(&mut service, async {
                while handle.phase() != Phase::Quarantined {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }).await;
        } else {
            assert_eq!(phase, Phase::Quarantined);
        }
        for _ in 0..4 {
            assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
            assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
            assert_eq!(foreign.snapshot_now().leased_bytes, 8);
            assert_eq!(bridge.selected_data(), Some(original));
            assert_eq!(bridge.selected_capability(), Some(capability));
            assert!(bridge.queued_len() >= 1);
            let current = {
                let ledger = owner.ledger();
                let error = ledger.first_failure.as_ref().unwrap();
                (error as *const _ as usize, format!("{:?}", error.facts()), ledger.shutdown_deadline.unwrap())
            };
            assert_eq!(current, first);
            assert_eq!((gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0,0,0));
            assert!(service.completion.is_none());
            assert!(peer.as_ref().unwrap().close_reason().is_none());
            assert!(!handle.request_rebuild());
        }
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
        println!("original selected ptr/cap/lease and second lane charged Q; first error/deadline retained, no D/Node clean or successor");
    }).await;
    drop(stream);
    drop(service); // Explicit forced boundary, no clean asynchronous claim.
    let drained = wire::capture(wire::drain(server, peer)).await;
    println!("forced finally server drain completed={}", drained.is_ok());
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drained {
        std::panic::resume_unwind(error);
    }
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn permit_foreign_egress_entry_keeps_original_selected_and_lane_during_q() {
    let server = wire::server();
    let policy = Arc::new(crate::upstream::RelayFamilyPolicy::from_sources(
        crate::upstream::RelayFamilyPolicySources {
            permit: Some("1"),
            ..Default::default()
        },
    ));
    let cfg = wire::config(&server).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy.clone());
    assert!(Arc::ptr_eq(cfg.relay_policy(), runtime.relay_policy()));
    let (inner, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let gate = Arc::new(Gate::default());
    gate.open();
    gate.cut_enabled.store(true, Ordering::Release);
    let mut service = prepare(
        GatedDevice {
            inner,
            gate: gate.clone(),
        },
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut app = wire::App::new(application);
    let mut peer = None;
    let mut stream = None;
    let foreign = AsyncLeasedByteFlowQueue::new_with_release_mode(
        8,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let scenario = wire::capture(async {
        wire::drive(&mut service, async {
            let p = server.accept().await.unwrap().await.unwrap();
            wire::auth(&p, &cfg).await;
            peer = Some(p);
        }).await;
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        stream = Some(wire::drive(&mut service, async {
            let remote = async { tokio::join!(wire::healthy(peer.as_ref().unwrap()), wire::udp_reply(peer.as_ref().unwrap())) };
            let application = async {
                while app.tcp_reply != b"first" || app.udp_reply != b"udp-reply" {
                    app.turn().await;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            };
            let ((stream, ()), ()) = tokio::join!(remote, application);
            stream
        }).await);
        let contracts = owner.flow_contracts();
        assert_eq!(contracts.len(), 1);
        assert!(contracts[0].published);
        let id = contracts[0].id;
        let original_queue = owner.test_permit_queue(id).unwrap();
        assert_eq!(original_queue.snapshot_now().leased_bytes, 0);
        let bridge = owner.relay_event_test_bridge().unwrap();
        assert_eq!(bridge.queued_len(), 0);
        assert!(owner.ledger().first_failure.is_none());
        foreign.push(bytes::Bytes::from_static(b"12345678")).await.unwrap();
        let (_, first) = foreign.recv_up_to(4).await.unwrap().into_parts();
        let (_, second) = foreign.recv_up_to(4).await.unwrap().into_parts();
        let mut bytes = Vec::with_capacity(19);
        bytes.extend_from_slice(b"bad1");
        let original = (id.epoch, bytes.len(), bytes.as_ptr() as usize, 0);
        let capability = (bytes.capacity(), Some((4, DownstreamPermitReleaseMode::OnEgressDrain)));
        assert!(bridge.try_send(id.handle, RelayEvent::Data { epoch: id.epoch, bytes: bytes.into(),
            stream_service_window: Default::default(), downstream_permit: Some(first) }).is_accepted());
        assert!(bridge.try_send(id.handle, RelayEvent::Data { epoch: id.epoch, bytes: b"bad2".to_vec().into(),
            stream_service_window: Default::default(), downstream_permit: Some(second) }).is_accepted());
        assert_eq!(foreign.snapshot_now().leased_bytes, 8);
        wire::drive(&mut service, async {
            loop {
                if owner.ledger().first_failure.is_some() || foreign.snapshot_now().leased_bytes != 8 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await;
        assert_eq!(foreign.snapshot_now().leased_bytes, 8, "unknown lease must remain physically charged");
        assert_eq!(bridge.selected_data(), Some(original));
        assert_eq!(bridge.selected_capability(), Some(capability));
        assert!(bridge.queued_len() >= 1);
        let first = {
            let ledger = owner.ledger();
            let error = ledger.first_failure.as_ref().unwrap();
            (error as *const _ as usize, format!("{:?}", error.facts()), ledger.shutdown_deadline.unwrap(),
                matches!(error.facts().class, crate::shared::errors::FailureClass::Io),
                matches!(error.facts().origin.phase, FailurePhase::DeviceRead),
                error.facts().io_kind, error.facts().raw_os_code)
        };
        assert_eq!((first.3, first.4, first.5, first.6), (true, true, Some(std::io::ErrorKind::InvalidData), None));
        // Public observation fixes the same already-owned absolute deadline.
        let mut observer = Box::pin(handle.observe_shutdown());
        let mut phase = None;
        for _ in 0..4 {
            if let Ok(value) = tokio::time::timeout(Duration::from_millis(1), observer.as_mut()).await {
                phase = Some(value);
                break;
            }
            assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
        }
        let phase = match phase {
            Some(value) => value,
            None => tokio::select! {
                value = observer.as_mut() => value,
                report = service.run() => panic!("unknown selected must not complete {report:?}"),
            },
        };
        drop(observer);
        if phase == Phase::Incomplete {
            let parent = *saved.deadline.lock().unwrap();
            assert_eq!(parent, Some(first.2));
            assert!(tokio::time::Instant::now() >= parent.unwrap());
            assert_eq!(foreign.snapshot_now().leased_bytes, 8);
            println!("original observer Incomplete ownerlimit={:?} parent={parent:?}", first.2);
            // Independent cold census only, never a new retirement deadline.
            wire::drive(&mut service, async {
                while handle.phase() != Phase::Quarantined {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }).await;
        } else {
            assert_eq!(phase, Phase::Quarantined);
        }
        let parent = *saved.deadline.lock().unwrap();
        assert_eq!(parent, Some(first.2));
        for _ in 0..4 {
            assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
            assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
            assert_eq!(foreign.snapshot_now().leased_bytes, 8);
            assert_eq!(bridge.selected_data(), Some(original));
            assert_eq!(bridge.selected_capability(), Some(capability));
            assert!(bridge.queued_len() >= 1);
            let current = {
                let ledger = owner.ledger();
                let error = ledger.first_failure.as_ref().unwrap();
                (error as *const _ as usize, format!("{:?}", error.facts()), ledger.shutdown_deadline.unwrap(),
                matches!(error.facts().class, crate::shared::errors::FailureClass::Io),
                matches!(error.facts().origin.phase, FailurePhase::DeviceRead),
                error.facts().io_kind, error.facts().raw_os_code)
            };
            assert_eq!(current, first);
            assert_eq!((gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0,0,0));
            assert!(service.completion.is_none());
            assert!(peer.as_ref().unwrap().close_reason().is_none());
            assert!(!handle.request_rebuild());
        }
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
        println!("Running P-bound foreign OnEgressDrain entry: originalselected lease8+lane retained, typed IO/DeviceRead/InvalidData first15, no D/Node clean or successor");
    }).await;
    drop(stream);
    drop(service); // Explicit forced boundary, no clean asynchronous claim.
    let drained = wire::capture(wire::drain(server, peer)).await;
    println!("forced finally server drain completed={}", drained.is_ok());
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drained {
        std::panic::resume_unwind(error);
    }
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
}

//! Fault the next actual Permit DATA length after real successful TCP/UDP.
//! One Copy fault changes no original capability; Q retains selected + lane.
use super::*;
use crate::client_tun::legacy_event_loop::RelayEvent;
use crate::tcp_downlink_pump::{AsyncLeasedByteFlowQueue, DownstreamPermitReleaseMode};
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
use device::{Gate, GatedDevice};
use std::sync::atomic::Ordering;
#[path = "../../../../../tests/tuic_service/pressure_fixture.rs"]
mod wire;

#[tokio::test(flavor = "current_thread")]
async fn actual_permit_wrong_length_keeps_original_selected_lease_and_foreign_mode_lane_in_q() {
    let server = wire::server();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        permit: Some("1"),
        ..Default::default()
    }));
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
        let queue = owner.test_permit_queue(id).unwrap();
        assert_eq!(queue.snapshot_now().leased_bytes, 0);
        let bridge = owner.relay_event_test_bridge().unwrap();
        assert!(owner.ledger().first_failure.is_none());
        bridge.arm_short_data(id.handle, id.epoch);
        // The real peer, original reader, original FIFO and original dispatcher
        // generate this event. No test-created DATA substitutes its capability.
        stream.as_mut().unwrap().0.write_all(&[88; 128]).await.unwrap();
        wire::drive(&mut service, async {
            while owner.ledger().first_failure.is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await;
        let facts = bridge.selected_fault().unwrap();
        assert!(facts.0 >= 2 && facts.0 <= 128);
        assert_eq!(facts.0 - 1, facts.1);
        assert_eq!(facts.0, facts.4);
        let selected = (id.epoch, facts.1, facts.2, 0);
        let capability = (facts.3, Some((facts.4, DownstreamPermitReleaseMode::OnAdmission)));
        assert_eq!(bridge.selected_data(), Some(selected));
        assert_eq!(bridge.selected_capability(), Some(capability));
        assert!(queue.snapshot_now().leased_bytes >= facts.4);
        // Q has already stopped consumption. These foreign OnEgressDrain caps
        // prove lane custody only; no subsequent entry-mode validation is claimed.
        foreign.push(bytes::Bytes::from_static(b"12345678")).await.unwrap();
        for _ in 0..2 {
            let (_, permit) = foreign.recv_up_to(4).await.unwrap().into_parts();
            assert!(bridge.try_send(id.handle, RelayEvent::Data { epoch: id.epoch,
                bytes: b"mode".to_vec().into(), stream_service_window: Default::default(), downstream_permit: Some(permit) }).is_accepted());
        }
        let first = {
            let ledger = owner.ledger();
            let error = ledger.first_failure.as_ref().unwrap();
            (error as *const _ as usize, format!("{:?}", error.facts()), ledger.shutdown_deadline.unwrap())
        };
        let mut observer = Box::pin(handle.observe_shutdown());
        let mut phase = None;
        for _ in 0..4 {
            if let Ok(value) = tokio::time::timeout(Duration::from_millis(1), observer.as_mut()).await {
                phase = Some(value); break;
            }
            assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
            assert_eq!(bridge.selected_data(), Some(selected));
        }
        let phase = match phase {
            Some(value) => value,
            None => tokio::select! {
                value = observer.as_mut() => value,
                report = service.run() => panic!("unknown actual lease cannot complete {report:?}"),
            },
        };
        drop(observer);
        if phase == Phase::Incomplete {
            let parent = *saved.deadline.lock().unwrap();
            assert!(tokio::time::Instant::now() >= parent.unwrap());
            println!("actual P mismatch originalfirst15 Incomplete owner={:?} parent={parent:?}", first.2);
            wire::drive(&mut service, async {
                while handle.phase() != Phase::Quarantined {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }).await;
        } else { assert_eq!(phase, Phase::Quarantined); }
        let charged = queue.snapshot_now().leased_bytes;
        assert!(charged >= facts.4);
        for _ in 0..4 {
            assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
            assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
            assert_eq!(queue.snapshot_now().leased_bytes, charged);
            assert_eq!(foreign.snapshot_now().leased_bytes, 8);
            assert_eq!(bridge.selected_data(), Some(selected));
            assert_eq!(bridge.selected_capability(), Some(capability));
            assert!(bridge.queued_len() >= 2);
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
        println!("actual selected P len mismatch {facts:?}, originalqueue leased={charged}, foreignlane8; Q no D/Node clean or successor");
    }).await;
    drop(stream);
    drop(service); // Forced test boundary only.
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

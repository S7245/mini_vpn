//! Real queued original Permit DATA is drained by the normal saved stop path.
//! No fake DATA/capability/receipt or new actor: park the service, hold its
//! original lane, release DATA before the real provider final offset, then D.
use super::*;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources};
use device::{Gate, GatedDevice};
use std::sync::atomic::{AtomicBool, Ordering};
#[path = "../../../../../tests/tuic_service/pressure_fixture.rs"]
mod wire;

fn parent_limit(control: &Control) -> Option<tokio::time::Instant> {
    *control
        .deadline
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn owner_limit(owner: &SessionControl) -> Option<tokio::time::Instant> {
    owner.ledger().shutdown_deadline
}
fn first_none(owner: &SessionControl) -> bool {
    owner.ledger().first_failure.is_none()
}

#[tokio::test(flavor = "current_thread")]
async fn queued_permit_normal_stop_releases_actual_main_cap_before_provider_and_device() {
    exercise(RelayFamilyPolicySources {
        permit: Some("1"),
        ..Default::default()
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_ordered_main_cap_four_joins_precede_provider_and_actual_reset() {
    exercise(RelayFamilyPolicySources {
        ordered_egress_permit: Some("1"),
        ..Default::default()
    })
    .await;
}

async fn exercise(sources: RelayFamilyPolicySources<'_>) {
    let server = wire::server();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(sources));
    let cfg = wire::config(&server).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy.clone());
    let (inner, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let gate = Arc::new(Gate::default()); // Hold only actual device shutdown.
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
    let mut owner = None;
    let mut original_queue = None;
    let mut lane_held = false;
    let mut retirement_started = false;
    let mut observer = Box::pin(handle.observe_shutdown());
    let scenario = wire::capture(async {
        wire::drive(&mut service, async {
            let p = server.accept().await.unwrap().await.unwrap();
            wire::auth(&p, &cfg).await;
            peer = Some(p);
        }).await;
        owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade();
        let owner = owner.as_ref().unwrap();
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
        let id = contracts[0].id;
        assert!(contracts[0].published);
        original_queue = owner.test_permit_queue(id);
        let queue = original_queue.as_ref().unwrap();
        let mode = queue.custodied_release_mode().unwrap();
        assert_eq!(mode, if policy.generic_engine() == crate::upstream::TcpRelayEngine::OrderedEgressPermitPump {
            crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnEgressDrain
        } else { crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnAdmission });
        println!("actual original F queue mode={mode:?}; engine={:?}", policy.generic_engine());
        assert_eq!(queue.snapshot_now().leased_bytes, 0);
        let bridge = owner.relay_event_test_bridge().unwrap();
        stream.as_mut().unwrap().0.write_all(&[88; 128]).await.unwrap();
        // Only original background actors run here. The saved main driver is
        // deliberately parked, so no application admission can erase this cap.
        tokio::time::timeout(Duration::from_secs(2), async {
            while bridge.queued_len() == 0
                || owner.test_queued_data(id.handle) != Some(128)
                || queue.snapshot_now().leased_bytes != 128 {
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        owner.hold_test_lane(id.handle);
        lane_held = true;
        handle.request_stop();
        let parent = parent_limit(&saved);
        assert!(parent.is_some());
        wire::drive(&mut service, async {
            loop {
                let state = owner.flow_observation().unwrap();
                assert!(state.audit);
                if state.joined == 4 { break; }
                tokio::task::yield_now().await;
            }
        }).await;
        let original_owner_limit = owner_limit(owner);
        assert!(original_owner_limit.is_some());
        assert_eq!(queue.snapshot_now().leased_bytes, 128);
        assert!(!queue.physical_zero());
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(1), observer.as_mut()).await.is_err());
            assert!(tokio::time::timeout(Duration::from_millis(1), service.run()).await.is_err());
            assert_eq!(queue.snapshot_now().leased_bytes, 128);
            assert!(bridge.queued_len() > 0);
            assert_eq!(parent_limit(&saved), parent);
            assert_eq!(owner_limit(owner), original_owner_limit);
        }
        assert_eq!(gate.polls.load(Ordering::Acquire), 0);
        assert!(first_none(owner));
        owner.release_test_lane();
        lane_held = false;
        wire::drive(&mut service, async {
            while queue.snapshot_now().leased_bytes != 0 || bridge.queued_len() != 0 {
                tokio::task::yield_now().await;
            }
        }).await;
        assert!(queue.physical_zero());
        assert_eq!(gate.polls.load(Ordering::Acquire), 0);
        assert!(!owner.business_complete.load(Ordering::Acquire));
        assert!(peer.as_ref().unwrap().close_reason().is_none());
        assert!(first_none(owner));
        println!("actualqueued P128: four joined, normal draining released original Main lease; original provider still held/D0");
        retirement_started = true;
        wire::drive(&mut service, wire::retire(&mut stream, &AtomicBool::new(true))).await;
        wire::drive(&mut service, async {
            while gate.polls.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        }).await;
        let state = owner.flow_observation().unwrap();
        assert!(state.audit);
        assert_eq!((state.live, state.w, state.f, state.running, state.attempts, state.reserved), (0,0,0,0,0,0));
        assert_eq!((state.published, state.released, state.joined), (1,1,4));
        let (_, total, transferred, discarded, pending) = state.last_data_disposition.unwrap();
        assert_eq!((total, transferred, discarded, pending), (133,5,128,0));
        assert!(owner.business_complete.load(Ordering::Acquire));
        assert!(first_none(owner));
        assert!(queue.physical_zero());
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0,0));
        assert_eq!(parent_limit(&saved), parent);
        assert_eq!(owner_limit(owner), original_owner_limit);
        assert!(!handle.request_rebuild());
        println!("actualDATA disposition133=transfer5+discard128; provider released1/join4/businesszero before heldD, original deadlines unchanged");
    }).await;
    // Always remove only the original test scheduling hold before real cleanup.
    if lane_held {
        owner.as_ref().unwrap().release_test_lane();
    }
    if scenario.is_err() && retirement_started && stream.is_some() {
        eprintln!(
            "partial retirement Unknown: dispose remaining original halves; no second retire or STOP/EOF proof"
        );
        drop(stream.take());
    }
    gate.open();
    handle.request_stop();
    let parent = parent_limit(&saved);
    let mut actual_report = None;
    let cleanup = wire::capture(async {
        let (report, ()) = tokio::time::timeout_at(parent.unwrap(), async {
            tokio::join!(service.run(), async {
                if !retirement_started && stream.is_some() {
                    retirement_started = true;
                    wire::retire(&mut stream, &AtomicBool::new(true)).await;
                }
            })
        })
        .await
        .unwrap();
        actual_report = Some(report);
        println!(
            "actualqueued P stop cleanup={report:?}, phase={:?}",
            handle.phase()
        );
        if scenario.is_ok() {
            assert!(report.device_settled() && !report.device_failed());
            assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
            assert_eq!(report.rebuilds, 0);
            assert_eq!(observer.as_mut().await, Phase::Complete);
        }
    })
    .await;
    println!(
        "actualqueued P finalreport={actual_report:?}, cleanup_completed={}",
        cleanup.is_ok()
    );
    drop(stream);
    drop(service);
    let drained = wire::capture(wire::drain(server, peer)).await;
    println!("actualqueued P serverdrain_completed={}", drained.is_ok());
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drained {
        std::panic::resume_unwind(error);
    }
    assert_eq!(
        (
            gate.joins.load(Ordering::Acquire),
            gate.drops.load(Ordering::Acquire)
        ),
        (1, 1)
    );
}

//! Original Main-lane Permit release panics through its own Notify waker.
//! Owns one test waiter and passive queue clone, no actor or replacement DATA.
//! Actual released zero must retain the original in-flight transaction in Q.
use super::*;
use crate::shared::errors::{FailureClass, FailurePhase};
use crate::tcp_downlink_pump::DownstreamPermitReleaseMode;
use crate::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TcpRelayEngine};
use device::{Gate, GatedDevice};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};
#[path = "../../../../../tests/tuic_service/pressure_fixture.rs"]
mod wire;

struct PanicWake(AtomicUsize);
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert_ne!(
            self.0.fetch_add(1, Ordering::SeqCst),
            0,
            "actual Main permit Notify release panic"
        );
    }
}
fn parent_limit(control: &Control) -> Option<tokio::time::Instant> {
    *control
        .deadline
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn first(owner: &SessionControl) -> (usize, String, Option<tokio::time::Instant>) {
    let ledger = owner.ledger();
    let error = ledger.first_failure.as_ref().unwrap();
    (
        error as *const _ as usize,
        format!("{:?}", error.facts()),
        ledger.shutdown_deadline,
    )
}

#[derive(Clone, Copy, Debug)]
enum Case {
    Permit,
    Ordered,
}
impl Case {
    fn mode(self) -> DownstreamPermitReleaseMode {
        match self {
            Self::Permit => DownstreamPermitReleaseMode::OnAdmission,
            Self::Ordered => DownstreamPermitReleaseMode::OnEgressDrain,
        }
    }
    fn engine(self) -> TcpRelayEngine {
        match self {
            Self::Permit => TcpRelayEngine::PermitPump,
            Self::Ordered => TcpRelayEngine::OrderedEgressPermitPump,
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_ordered_normal_drain_notify_panic_keeps_released_original_cap_and_window_in_q() {
    exercise(Case::Ordered).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_normal_drain_notify_panic_keeps_released_original_cap_and_window_in_q() {
    exercise(Case::Permit).await;
}

async fn exercise(case: Case) {
    let ordered = matches!(case, Case::Ordered);
    let server = wire::server();
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        permit: (!ordered).then_some("1"),
        ordered_egress_permit: ordered.then_some("1"),
        ..Default::default()
    }));
    let cfg = wire::config(&server).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    assert!(Arc::ptr_eq(cfg.relay_policy(), runtime.relay_policy()));
    assert_eq!(cfg.relay_policy().generic_engine(), case.engine());
    let (inner, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let gate = Arc::new(Gate::default());
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
    let mut waiter = None;
    let panic_wake = Arc::new(PanicWake(AtomicUsize::new(0)));
    let mut lane_held = false;
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
        assert_eq!(queue.custodied_release_mode(), Some(case.mode()));
        assert_eq!(queue.snapshot_now().leased_bytes, 0);
        let bridge = owner.relay_event_test_bridge().unwrap();
        assert_eq!(bridge.queued_len(), 0);
        stream.as_mut().unwrap().0.write_all(&[88; 128]).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while bridge.queued_len() != 1 || owner.test_queued_data(id.handle) != Some(128)
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
        let owner_limit = owner.ledger().shutdown_deadline;
        assert!(owner_limit.is_some());
        assert_eq!(queue.snapshot_now().leased_bytes, 128);
        assert!(owner.ledger().first_failure.is_none());
        assert_eq!(panic_wake.0.load(Ordering::Acquire), 0);
        // Arm only AFTER original stop/queue-close and four actual joins. The
        // next wake must come from the actual normal-draining permit release.
        waiter = Some(Box::pin(queue.clone().test_original_capacity_notify()));
        {
            let waker = Waker::from(panic_wake.clone());
            let mut cx = Context::from_waker(&waker);
            assert!(waiter.as_mut().unwrap().as_mut().poll(&mut cx).is_pending());
        }
        assert_eq!(panic_wake.0.load(Ordering::Acquire), 0);
        owner.release_test_lane();
        lane_held = false;
        wire::drive(&mut service, async {
            while owner.ledger().first_failure.is_none() { tokio::task::yield_now().await; }
        }).await;
        assert_eq!(panic_wake.0.load(Ordering::Acquire), 1);
        assert_eq!(queue.snapshot_now().leased_bytes, 0);
        assert!(!queue.physical_zero());
        let original_first = first(owner);
        assert_eq!(original_first.2, owner_limit);
        let typed = {
            let ledger = owner.ledger();
            let facts = ledger.first_failure.as_ref().unwrap().facts();
            (facts.class == FailureClass::Panic, facts.origin.phase == FailurePhase::SessionLoop)
        };
        assert_eq!(typed, (true, true));
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(1), observer.as_mut()).await.is_err());
            assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
            assert_eq!(parent_limit(&saved), parent);
            assert_eq!(first(owner), original_first);
        }
        let phase = tokio::select! {
            value = observer.as_mut() => value,
            report = service.run() => panic!("interrupted actual permit release cannot complete {report:?}"),
        };
        if phase == Phase::Incomplete {
            assert!(tokio::time::Instant::now() >= parent.unwrap());
            wire::drive(&mut service, async {
                while handle.phase() != Phase::Quarantined { tokio::task::yield_now().await; }
            }).await;
        } else { assert_eq!(phase, Phase::Quarantined); }
        let selected = bridge.selected_data().unwrap();
        let capability = bridge.selected_capability().unwrap();
        assert_eq!((selected.0, selected.1, selected.3), (id.epoch, 128, 0));
        assert_ne!(selected.2, 0);
        assert!(capability.0 >= 128);
        assert_eq!(capability.1, Some((0, case.mode())));
        let census = bridge.fatal_census().unwrap();
        assert_eq!(census.1, 1); // Original Mirror event never reached finish.
        assert_eq!(census.0[0].1, 133);
        assert_eq!(census.0[1].1, 5);
        assert_eq!(census.0[2].1, 0);
        println!("actual Notify panic: originallease128->0, selected={selected:?} capability={capability:?} census={census:?}, first={original_first:?}");
        for _ in 0..4 {
            assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
            assert!(tokio::time::timeout(Duration::from_millis(2), service.run()).await.is_err());
            assert_eq!(first(owner), original_first);
            assert_eq!(parent_limit(&saved), parent);
            assert_eq!(bridge.selected_data(), Some(selected));
            assert_eq!(bridge.selected_capability(), Some(capability));
            assert_eq!(bridge.fatal_census(), Some(census));
            assert_eq!(queue.snapshot_now().leased_bytes, 0);
            assert!(!queue.physical_zero());
            let state = owner.flow_observation().unwrap();
            assert!(state.audit);
            assert_eq!((state.live, state.f, state.attempts, state.released, state.joined), (1,1,1,0,4));
            assert!(!owner.business_complete.load(Ordering::Acquire));
            assert!(service.completion.is_none());
            assert_eq!((gate.polls.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0,0,0));
            assert!(peer.as_ref().unwrap().close_reason().is_none());
            assert!(!handle.request_rebuild());
        }
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
        println!("original first15/firstPanic SessionLoop retained; leased0 does not settle original in-flight128/Window/Flow/provider debt; no D/Node clean or successor");
    }).await;
    if lane_held {
        owner.as_ref().unwrap().release_test_lane();
    }
    // The waiter and passive queue survive all async observations. Forced Drop
    // below is the final test boundary, never asynchronous clean settlement.
    drop(waiter);
    drop(stream);
    drop(service);
    let drained = wire::capture(wire::drain(server, peer)).await;
    println!(
        "Notify panic forced finally serverdrain_completed={}",
        drained.is_ok()
    );
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = drained {
        std::panic::resume_unwind(error);
    }
    assert_eq!(gate.joins.load(Ordering::Acquire), 0);
    assert_eq!(gate.drops.load(Ordering::Acquire), 1);
}

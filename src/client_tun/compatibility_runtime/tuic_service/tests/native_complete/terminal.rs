//! Original hard Service fatal and cancelled shutdown observation after real DATA.
//! The saved driver, Session and actual Device remain the only lifecycle owners.
use super::*;
use std::sync::atomic::Ordering;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Ending {
    Normal,
    CancelShutdown,
    DeviceFatal,
}

#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_chunk_device_fatal_keeps_original_root_and_first() {
    complete_service_ending(false, ServiceUdp::Datagram, Ending::DeviceFatal).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_permit_device_fatal_keeps_original_root_and_first() {
    complete_service_ending(true, ServiceUdp::Datagram, Ending::DeviceFatal).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_chunk_cancelled_shutdown_keeps_original_device_and_clock() {
    complete_service_ending(false, ServiceUdp::Datagram, Ending::CancelShutdown).await;
}
#[tokio::test(flavor = "current_thread")]
async fn hard_native_service_permit_cancelled_shutdown_keeps_original_device_and_clock() {
    complete_service_ending(true, ServiceUdp::Datagram, Ending::CancelShutdown).await;
}

pub(super) async fn fatal<F: Future<Output = Completion>>(
    service: &mut Service<F>,
    handle: &Handle,
    gate: &Arc<device::Gate>,
    backing: &bytes::BackingMeter,
    app: &mut Apps,
) {
    let saved = service.control.clone();
    let owner = saved
        .session
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    let flow = owner.flow_observation().unwrap();
    let identity = owner.flow_contracts()[0].id;
    assert_eq!((flow.published, flow.running, flow.f), (1, 5, 1));
    assert!(owner.ledger().first_failure.is_none());
    gate.read_error.store(true, Ordering::SeqCst);
    // The previous inner wait already registered its real RX waker. A real
    // application packet wakes it; the next original wait observes the fault.
    let socket = app.sockets.get_mut::<smoltcp::socket::tcp::Socket>(app.tcp);
    assert_eq!(socket.send_slice(b"fatal-trigger").unwrap(), 13);
    app.turn().await;
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::select! {
            _ = async {
                while owner.ledger().first_failure.is_none() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            } => {},
            report = service.run() => panic!("actual Device fatal falsely completed Service: {report:?}"),
        }
    }).await.expect("original IO5 must reach the shared fatal owner");
    let first = fixture::first_failure_identity(&owner).unwrap();
    {
        let ledger = owner.ledger();
        let facts = ledger.first_failure.as_ref().unwrap().facts();
        assert_eq!(facts.raw_os_code, Some(5));
        assert_eq!(
            facts.origin.phase,
            crate::shared::errors::FailurePhase::DeviceRead
        );
    }
    let original = saved
        .deadline
        .lock()
        .unwrap()
        .expect("fatal must commit original Service stop");
    let original_session = owner
        .ledger()
        .shutdown_deadline
        .expect("fatal must retain original Session deadline");
    assert_eq!(original_session, original);
    assert!(tokio::time::Instant::now() < original);
    // Quiet first/stop/deadline publication may precede the Service phase's
    // next dispatch turn; test the original synchronous control authority.
    assert!(saved.stop.stopping.load(Ordering::Acquire));
    assert_eq!(fixture::first_failure_identity(&owner), Some(first.clone()));
    assert!(backing.snapshot().physical_bytes > 0);
    assert_eq!(owner.flow_observation().unwrap().f, 1);
    let bridge = owner.relay_event_test_bridge().unwrap();
    // Same fatal owner and first deadline as transaction/tests/fatal_settle.
    // The +5s bounds observing its bounded census, not any cleanup authority:
    // the actual original Service/Session deadline is asserted unchanged.
    tokio::time::timeout_at(original + Duration::from_secs(5), async {
        tokio::select! {
            _ = async {
                while handle.phase() != Phase::Quarantined || bridge.fatal_census().is_none() {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            } => {},
            report = service.run() => panic!("fatal original root falsely settled: {report:?}"),
        }
    })
    .await
    .expect("original fatal owner must publish its once-only census/Q");
    let expired = tokio::time::Instant::now() >= original;
    if !expired {
        // The other legitimate fatal exit is actual provider/lane quiescence.
        // It still keeps the invalidated Main receipt and F, never clean zero.
        let physical = owner.flow_observation().unwrap();
        assert_eq!(physical.settled_providers, physical.attempts);
        assert_eq!(physical.stopped_producers, 1);
    }
    let census = bridge.fatal_census().unwrap();
    assert_eq!(census.0[0].1, 8192);
    assert_eq!(census.0[0].1, census.0[1].1 + census.0[2].1);
    assert_eq!(census.1, 0);
    for _ in 0..2 {
        let current = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(Arc::ptr_eq(&current, &owner));
        assert_eq!(current.flow_contracts()[0].id, identity);
        let retained = current.flow_observation().unwrap();
        assert_eq!(
            (
                retained.published,
                retained.released,
                retained.joined,
                retained.f,
                retained.running
            ),
            (1, 0, 5, 1, 0)
        );
        assert_eq!(handle.observe_shutdown().await, Phase::Quarantined);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), service.run())
                .await
                .is_err()
        );
        assert!(service.completion.is_none() && !owner.driver_complete.load(Ordering::Acquire));
        assert_eq!(fixture::first_failure_identity(&owner), Some(first.clone()));
        assert_eq!(owner.flow_contracts()[0].id, identity);
        assert_eq!(
            (
                gate.joins.load(Ordering::SeqCst),
                gate.drops.load(Ordering::SeqCst)
            ),
            (0, 0)
        );
        assert_eq!(*saved.deadline.lock().unwrap(), Some(original));
        assert_eq!(owner.ledger().shutdown_deadline, Some(original_session));
        assert!(backing.snapshot().quarantined && backing.snapshot().physical_bytes > 0);
        let budget = saved.backing_budget.as_ref().unwrap();
        assert!(budget.quarantined() && budget.used() > 0);
    }
    eprintln!(
        "hard Native actual Device fatal retained: id={identity:?} first={first:?} node={:?} deadline={original:?}",
        backing.snapshot()
    );
    let root = owner.root_layout_for_test();
    assert_eq!((root.rows, root.published, root.provider_boxes), (1, 1, 1));
    assert!(root.slot_bytes > 0 && root.row_bytes > 0 && root.provider_outer_bytes > 0);
    let observer = saved.native_node_sample.lock().unwrap().as_ref().unwrap().clone();
    let node = observer.sample().unwrap(); // Opaque stats/drop stay outside the Control lock.
    let budget = saved.backing_budget.as_ref().unwrap();
    eprintln!("[N1-capacity-fatal-retained] root={root:?} node={:?} charged={} app={:?} first={first:?} deadline={original:?} Q={} no_candidate=true",
        node.instances, budget.used(), saved.app_meter.as_ref().unwrap().snapshot(), budget.quarantined());
}

pub(super) async fn cancelled_shutdown<F: Future<Output = Completion>>(
    service: &mut Service<F>,
    handle: &Handle,
    gate: &Arc<device::Gate>,
) {
    let saved = service.control.clone();
    let deadline = saved.deadline.lock().unwrap().unwrap();
    let owner = saved
        .session
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .upgrade()
        .unwrap();
    tokio::time::timeout_at(deadline, async {
        while gate.polls.load(Ordering::SeqCst) == 0 {
            assert!(
                tokio::time::timeout(Duration::from_millis(10), service.run())
                    .await
                    .is_err()
            );
        }
    })
    .await
    .expect("original Root must reach its actual Device shutdown");
    for _ in 0..2 {
        let current = saved
            .session
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .unwrap();
        assert!(Arc::ptr_eq(&current, &owner));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), service.run())
                .await
                .is_err()
        );
        assert!(
            gate.polls.load(Ordering::SeqCst) > 0,
            "actual Device shutdown must be pending"
        );
        assert_eq!(
            (
                gate.joins.load(Ordering::SeqCst),
                gate.drops.load(Ordering::SeqCst)
            ),
            (0, 0)
        );
        assert!(service.completion.is_none() && owner.ledger().first_failure.is_none());
        assert_eq!(*saved.deadline.lock().unwrap(), Some(deadline));
        assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
        let budget = saved.backing_budget.as_ref().unwrap();
        assert!(!budget.quarantined() && budget.used() > 0);
        let observation = handle.observe_shutdown();
        tokio::pin!(observation);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), &mut observation)
                .await
                .is_err()
        );
    }
    eprintln!(
        "hard Native cancelled shutdown: flow={:?} deadline={deadline:?} device_polls={}",
        owner.flow_observation(),
        gate.polls.load(Ordering::SeqCst)
    );
    gate.open(); // original pending Device, not a fresh factory or replacement root
}

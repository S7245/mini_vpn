//! Original saved factory and provided-device refusal. No OS TUN or peer run.
use super::device::{Gate, GatedDevice};
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Waker};

fn base() -> TunRuntimeConfig {
    TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(fixture::native_policy())
}
/// Real App startup cold for one Device slot (O1): what preflight requires
/// above the protocol reserve before any factory runs.
fn app_cold() -> usize {
    let probe = base().with_native_limits(usize::MAX >> 2, 1).unwrap();
    usize::try_from(native_app_cold(probe.native_limits.unwrap(), &probe).unwrap()).unwrap()
}
/// The historical 64KiB app allowance, now on top of the real startup cold
/// (previously it sat below it and relied on runtime cold Full).
fn runtime() -> TunRuntimeConfig {
    base().with_native_limits(fixture::native_hard(65536 + app_cold()), 1).unwrap()
}
/// Hard limit whose app tier is exactly one byte below the startup cold.
fn under_cold() -> TunRuntimeConfig {
    base().with_native_limits(fixture::native_hard(app_cold() - 1), 1).unwrap()
}
fn cfg() -> Configuration<()> {
    Configuration {
        tuic: fixture::config("127.0.0.1:9".parse().unwrap()).with_relay_policy(fixture::native_policy()),
        composition: (),
    }
}

#[tokio::test(start_paused = true)]
async fn source_panic_immediately_inherits_original_parent_stop_deadline() {
    use crate::shared::errors::{FailureOrder, QuietFailureSink};
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let parent = control_with_budget(false, Some(budget.clone()));
    let owner = business::control(&runtime(), parent.app_meter.clone());
    owner.parent_deadline.set(parent.deadline.clone()).unwrap();
    let original = parent.request_stop();
    assert!(owner.ledger().shutdown_deadline.is_none());
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    let sink: Arc<dyn QuietFailureSink> = owner.clone();
    let clock = FailureOrder::default();
    clock.bind(Arc::downgrade(&sink), SessionControl::origin(FailurePhase::TcpOpen), parent.app_meter.clone()).unwrap();
    clock.panic();
    assert_eq!(owner.ledger().shutdown_deadline, Some(original));
    assert_eq!(*parent.deadline.lock().unwrap(), Some(original));
    assert!(owner.ledger().quarantined && budget.quarantined() && budget.used() > 0);
}

#[tokio::test]
async fn invalid_native_budget_never_falls_back_to_unmetered_factory() {
    let mut runtime = runtime();
    // A crate-local corrupted configuration must fail closed too, not convert
    // BackingBudget::new(None) into Legacy mode at the composition root.
    runtime.native_limits.as_mut().unwrap().backing_bytes =
        std::num::NonZeroUsize::new(usize::MAX).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let factory = move |_: Option<bytes::BackingMeter>| async move {
        observed.fetch_add(1, Ordering::SeqCst);
        Err::<LoopbackTunDevice, _>(ClientError::Io(std::io::Error::from_raw_os_error(17)))
    };
    let mut service = prepare_factory(
        factory,
        cfg(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let report = service.run().await;
    assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "invalid Native cannot invoke a Legacy factory"
    );
}

#[tokio::test(start_paused = true)]
async fn cancelled_factory_observer_keeps_one_work_budget_and_original_stop_deadline() {
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));
    let (called, ready) = (calls.clone(), release.clone());
    let factory = move |meter: Option<bytes::BackingMeter>| async move {
        called.fetch_add(1, Ordering::SeqCst);
        let original =
            bytes::Bytes::copy_from_slice_metered(b"cold factory", &meter.unwrap()).unwrap();
        std::future::poll_fn(|_| {
            if ready.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        drop(original);
        Err::<LoopbackTunDevice, _>(ClientError::Io(std::io::Error::from_raw_os_error(17)))
    };
    let mut service = prepare_factory(
        factory,
        cfg(),
        runtime(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let budget = service.control.backing_budget.clone().unwrap();
    for _ in 0..2 {
        let mut work = Box::pin(service.run());
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(budget.used() > 0);
    let limit = handle.0.request_stop();
    tokio::time::advance(Duration::from_secs(3)).await;
    release.store(true, Ordering::SeqCst);
    let report = service.run().await;
    assert!(report.device_failed() && report.device_settled());
    assert_eq!(report.node, None);
    let owner = &service.completion.as_ref().unwrap().control;
    assert_eq!(owner.ledger().shutdown_deadline, Some(limit));
    assert_eq!(*service.control.deadline.lock().unwrap(), Some(limit));
    assert!(owner.ledger().first_failure.is_some());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn provided_started_unmetered_device_refusal_still_joins_original_pump_and_drops_device() {
    let gate = Arc::new(Gate::default());
    gate.open();
    let device = GatedDevice {
        inner: LoopbackTunDevice::new(PacketLink::bounded(8), PacketLink::bounded(8))
            .with_ingress_pump(4),
        gate: gate.clone(),
    };
    let mut service = prepare(
        device,
        cfg().tuic,
        runtime(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let budget = service.control.backing_budget.clone().unwrap();
    let report = tokio::time::timeout(Duration::from_secs(1), service.run())
        .await
        .unwrap();
    assert!(report.device_failed() && report.device_settled());
    assert_eq!(report.node, None);
    assert_eq!(gate.joins.load(Ordering::SeqCst), 1);
    assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
    assert!(gate.stops.load(Ordering::SeqCst) > 0);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn factory_panic_retains_original_payload_and_sticky_nonzero_budget() {
    let original = Arc::new(AtomicUsize::new(7));
    let payload = original.clone();
    let inspect = Arc::new(std::sync::OnceLock::<std::sync::Weak<Control>>::new());
    let before_panic = inspect.clone();
    let mut service = prepare_factory(
        move |meter: Option<bytes::BackingMeter>| async move {
            let control = before_panic.get().unwrap().upgrade().unwrap();
            let owner = control.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            assert_eq!(egress::deadline::inventory_for_test(&owner), Some((1, false, 0, false, false, 0)));
            assert_eq!(control.backing_budget.as_ref().unwrap().used(), egress::deadline::layout_bytes());
            drop((owner, control));
            let _actual =
                bytes::Bytes::copy_from_slice_metered(b"physical", &meter.unwrap()).unwrap();
            fn fail(payload: Arc<AtomicUsize>) -> Result<LoopbackTunDevice, ClientError> {
                std::panic::panic_any(payload)
            }
            fail(payload)
        },
        cfg(),
        runtime(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let control = service.control.clone();
    inspect.set(Arc::downgrade(&control)).unwrap();
    let budget = control.backing_budget.clone().unwrap();
    for _ in 0..2 {
        let mut work = Box::pin(service.run());
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(*control.phase.lock().unwrap(), Phase::Quarantined);
    assert!(service.completion.is_none());
    let slot = control.terminal.poll.lock().unwrap();
    assert!(Arc::ptr_eq(
        slot.as_ref()
            .unwrap()
            .downcast_ref::<Arc<AtomicUsize>>()
            .unwrap(),
        &original
    ));
    drop(slot);
    drop(service);
    assert!(budget.quarantined());
    // Factory unwind actually destroyed the unused cold Session inventory
    // before the saved root marked Q. Bind-panic retains it instead.
    assert!(control.session.lock().unwrap().as_ref().unwrap().upgrade().is_none());
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn cold_bind_panic_keeps_actual_device_custodied_until_forced_saved_disposal() {
    let gate = Arc::new(Gate::default());
    gate.bind_panic.store(true, Ordering::SeqCst);
    let mut service = prepare(
        super::device::device(gate.clone()),
        cfg().tuic,
        runtime(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let budget = service.control.backing_budget.clone().unwrap();
    {
        let mut work = Box::pin(service.run());
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    let before = gate.drops.load(Ordering::SeqCst);
    let phase = *service.control.phase.lock().unwrap();
    assert!(budget.quarantined());
    drop(service);
    assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
    assert_eq!(budget.used(), egress::deadline::layout_bytes());
    assert_eq!(phase, Phase::Quarantined);
    assert_eq!(
        before, 0,
        "unknown bind must retain actual D in the saved root"
    );
}

#[tokio::test]
async fn actual_cold_bind_then_device_drop_panics_keep_two_original_payloads_without_abort() {
    let gate = Arc::new(Gate::default());
    let first = Arc::new(AtomicUsize::new(17));
    let second = Arc::new(AtomicUsize::new(29));
    *gate.bind_payload.lock().unwrap() = Some(first.clone());
    *gate.drop_payload.lock().unwrap() = Some(second.clone());
    gate.bind_panic.store(true, Ordering::SeqCst);
    let mut service = prepare(
        super::device::device(gate.clone()),
        cfg().tuic,
        runtime(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let control = service.control.clone();
    let budget = control.backing_budget.clone().unwrap();
    {
        let mut work = Box::pin(service.run());
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(gate.drops.load(Ordering::SeqCst), 0);
    assert_eq!(gate.joins.load(Ordering::SeqCst), 0);
    assert_eq!(gate.stops.load(Ordering::SeqCst), 0);
    assert!(budget.quarantined());
    assert_eq!(*control.phase.lock().unwrap(), Phase::Quarantined);
    // The original primary must already be retained, not destroyed during catch.
    assert_eq!(Arc::strong_count(&first), 2);
    assert_eq!(
        control.terminal.first_site(),
        super::super::saved::Site::Bind as usize
    );
    {
        let slot = control.terminal.bind.lock().unwrap();
        assert!(Arc::ptr_eq(
            slot.as_ref()
                .unwrap()
                .downcast_ref::<Arc<AtomicUsize>>()
                .unwrap(),
            &first
        ));
    }
    drop(service);
    let slot = control.terminal.disposal.lock().unwrap();
    assert!(Arc::ptr_eq(
        slot.as_ref()
            .unwrap()
            .downcast_ref::<Arc<AtomicUsize>>()
            .unwrap(),
        &second
    ));
    assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&first), 2);
    assert_eq!(budget.used(), egress::deadline::layout_bytes());
}

/// O1: an under-cold hard limit (formerly admitted, then cold Full with the
/// factory still called once) is now a preflight rejection: no budget, no
/// factory call, one typed first failure, no Node.
#[tokio::test]
async fn under_cold_hard_is_rejected_before_the_private_factory() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut service = prepare_factory(
        move |_: Option<bytes::BackingMeter>| async move {
            observed.fetch_add(1, Ordering::SeqCst);
            Err::<LoopbackTunDevice, _>(ClientError::Io(std::io::Error::from_raw_os_error(17)))
        }, cfg(), under_cold(), Arc::new(Metrics::default()), NoopSink,
    );
    assert!(service.control.backing_budget.is_none() && service.control.app_meter.is_none());
    let report = service.run().await;
    assert!(report.device_settled() && report.device_failed() && report.node.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let ledger = service.completion.as_ref().unwrap().control.ledger();
    assert_eq!(ledger.first_failure.as_ref().unwrap().facts().io_kind, None);
    assert_eq!(ledger.additional_failures, 0);
    assert!(!ledger.quarantined);
}

#[tokio::test(start_paused = true)]
async fn actual_parent_tightening_wakes_already_stopping_native_flush() {
    struct Count(AtomicUsize);
    impl std::task::Wake for Count {
        fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
        fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
    }
    let budget = bytes::BackingBudget::new(2 << 20).unwrap();
    let parent = control_with_budget(false, Some(budget.clone()));
    let owner = business::control(&runtime(), parent.app_meter.clone());
    owner.parent_deadline.set(parent.deadline.clone()).unwrap();
    egress::deadline::prepare(&owner).unwrap();
    let peer = PacketLink::bounded(4);
    let mut raw = LoopbackTunDevice::native_with_factory(4, 1500, parent.app_meter.as_ref().unwrap(), || {
        Ok((PacketLink::bounded(4), peer.clone()))
    }).unwrap();
    raw.bind_backing_meter(crate::device::DeviceBinding::Native {
        meter: parent.app_meter.as_ref().unwrap(), slots: 4, mtu: 1500,
    }).unwrap();
    for _ in 0..4 { assert!(peer.push(bytes::BytesMut::from(&b"old"[..]))); }
    raw.inject_ip_packet_with(3, |_| Ok(3)).unwrap();
    let signal = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(signal.clone());
    let mut device = egress::SessionEgress::new(&mut raw, owner.clone());
    let mut flush = Box::pin(device.flush_tx());
    assert!(flush.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    owner.request_stop();
    assert!(flush.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    signal.0.store(0, Ordering::SeqCst);
    let earlier = tokio::time::Instant::now() + Duration::from_secs(2);
    parent.expire_at(earlier);
    parent.stop_session(&owner); // Actual owner transaction, not direct Mutex mutation.
    let wakes = signal.0.load(Ordering::SeqCst);
    if wakes != 0 {
        assert!(flush.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(flush.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        assert!(owner.ledger().quarantined);
        assert_eq!(owner.ledger().shutdown_deadline, Some(earlier));
        assert!(owner.ledger().first_failure.is_none());
    }
    drop(flush);
    drop(device);
    let mut receipt = std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    receipt.settle().await;
    drop((receipt, raw));
    assert!(wakes > 0, "actual tightening must wake the already-stopping IO observer");
}

/// O1: a provided (already started) device under a preflight rejection is not
/// dropped inside the factory wrapper: it is retained, never bound, joined and
/// dropped once by the original drive; no budget is minted, no Node starts.
#[tokio::test]
async fn under_cold_preflight_rejection_still_disposes_actual_provided_device() {
    let gate = Arc::new(Gate::default());
    gate.open();
    let device = GatedDevice {
        inner: LoopbackTunDevice::new(PacketLink::bounded(8), PacketLink::bounded(8)).with_ingress_pump(4),
        gate: gate.clone(),
    };
    let mut service = prepare(device, cfg().tuic, under_cold(), Arc::new(Metrics::default()), NoopSink);
    assert!(service.control.backing_budget.is_none());
    let report = tokio::time::timeout(Duration::from_secs(1), service.run()).await.unwrap();
    assert!(report.device_failed() && report.device_settled() && report.node.is_none());
    assert_eq!((gate.joins.load(Ordering::SeqCst), gate.drops.load(Ordering::SeqCst)), (1, 1));
    let ledger = service.completion.as_ref().unwrap().control.ledger();
    assert_eq!(ledger.first_failure.as_ref().unwrap().facts().io_kind, None);
    assert_eq!(ledger.additional_failures, 0);
    assert!(!ledger.quarantined);
}

#[tokio::test(start_paused = true)]
async fn actual_native_factory_q_refusal_is_not_a_new_source_io() {
    let called = Arc::new(AtomicUsize::new(0));
    let observed = called.clone();
    let mut service = prepare_factory(
        move |meter: Option<bytes::BackingMeter>| async move {
            observed.fetch_add(1, Ordering::SeqCst);
            LoopbackTunDevice::native_with_factory(1, 1500, &meter.unwrap(), || {
                Ok((PacketLink::bounded(1), PacketLink::bounded(1)))
            }).map_err(ClientError::Io)
        }, cfg(), runtime(), Arc::new(Metrics::default()), NoopSink,
    );
    let saved = service.control.clone();
    let original = saved.request_stop();
    saved.app_meter.as_ref().unwrap().quarantine();
    let mut run = Box::pin(service.run());
    assert!(run.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    drop(run);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
    let ledger = owner.ledger();
    assert!(ledger.quarantined);
    assert_eq!(ledger.shutdown_deadline, Some(original));
    assert!(ledger.first_failure.is_none());
    assert_eq!((ledger.additional_failures, ledger.egress_failures), (0, 0));
}

/// A1 item 7 (Service Control cold budget) × A2 preflight (review O1): the
/// actual `native_budget` composition root rejects typed one byte below
/// reserve + the real App startup cold (Service control + Device headroom),
/// and a rejected Service never mints a budget, meter, factory call or Node.
/// Exactly at the boundary the original hard limit is admitted.
#[tokio::test]
async fn a2_preflight_boundary_is_typed_before_any_budget_factory_or_node() {
    let reserve = fixture::native_reserve();
    let limits = |hard| runtime().with_native_limits(hard, 1).unwrap().native_limits.unwrap();
    let cold = usize::try_from(native_app_cold(limits(reserve + 1), &runtime()).unwrap()).unwrap();
    assert!(cold > egress::deadline::layout_bytes() + crate::device::RxPacket::test_protection_cold_bytes());
    let boundary = reserve + cold;
    match native_budget(limits(boundary - 1), &cfg().tuic, &runtime(), true).err() {
        Some(ClientError::InvalidTarget(message)) => {
            assert!(message.starts_with("Native preflight rejected"), "{message}")
        }
        other => panic!("reserve + app cold - 1 must be a typed preflight rejection: {other:?}"),
    }
    let admitted = native_budget(limits(boundary), &cfg().tuic, &runtime(), true).unwrap().0;
    assert_eq!((admitted.limit(), admitted.used()), (boundary, 0));
    drop(admitted);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut service = prepare_factory(
        move |_: Option<bytes::BackingMeter>| async move {
            observed.fetch_add(1, Ordering::SeqCst);
            Err::<LoopbackTunDevice, _>(ClientError::Io(std::io::Error::from_raw_os_error(17)))
        },
        cfg(),
        runtime().with_native_limits(boundary - 1, 1).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    assert!(service.control.backing_budget.is_none() && service.control.app_meter.is_none());
    let report = tokio::time::timeout(Duration::from_secs(1), service.run()).await.unwrap();
    assert_eq!(report.session, SessionDeviceState::DeviceFailedSettled);
    assert_eq!(report.node, None);
    assert_eq!(calls.load(Ordering::SeqCst), 0, "rejected preflight cannot reach the factory");

    // At the boundary the real cold chain fits: Service control is charged,
    // the factory builds an actual Native Device on the same meter, and the
    // first failure is the factory's own error rather than cold pressure.
    let built = Arc::new(AtomicUsize::new(0));
    let observed = built.clone();
    let mtu = runtime().tun_mtu;
    let mut service = prepare_factory(
        move |meter: Option<bytes::BackingMeter>| async move {
            observed.fetch_add(1, Ordering::SeqCst);
            let device = LoopbackTunDevice::native_with_factory(1, mtu, &meter.unwrap(), || {
                Ok((PacketLink::bounded(1), PacketLink::bounded(1)))
            }).map_err(ClientError::Io)?;
            drop(device);
            Err::<LoopbackTunDevice, _>(ClientError::Io(std::io::Error::from_raw_os_error(17)))
        },
        cfg(),
        runtime().with_native_limits(boundary, 1).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let budget = service.control.backing_budget.clone().unwrap();
    let report = tokio::time::timeout(Duration::from_secs(1), service.run()).await.unwrap();
    assert_eq!((report.session, report.node), (SessionDeviceState::DeviceFailedSettled, None));
    assert_eq!(built.load(Ordering::SeqCst), 1);
    let ledger = service.completion.as_ref().unwrap().control.ledger();
    assert_eq!(ledger.first_failure.as_ref().unwrap().facts().io_kind, Some(std::io::ErrorKind::AlreadyExists));
    assert_eq!(ledger.additional_failures, 0);
    assert_eq!((budget.used(), budget.quarantined()), (0, false));
}

/// O3 (D105): an explicit process ceiling gates hard + I_cfg. One byte below
/// the computed total is a typed rejection before any budget, factory or Node;
/// exactly the total is admitted; no ceiling keeps the record-only behaviour.
#[tokio::test]
async fn o3_memory_ceiling_is_typed_before_any_budget_factory_or_node() {
    let base = runtime();
    let memory = native_budget(base.native_limits.unwrap(), &cfg().tuic, &base, true).unwrap().1;
    let ceiled = |ceiling| base.clone().with_native_memory_ceiling(ceiling).unwrap();
    assert!(ceiled(memory).native_limits.unwrap().memory_ceiling.is_some());
    let admitted = ceiled(memory);
    assert_eq!(native_budget(admitted.native_limits.unwrap(), &cfg().tuic, &admitted, true).unwrap().1, memory);
    let rejected = ceiled(memory - 1);
    match native_budget(rejected.native_limits.unwrap(), &cfg().tuic, &rejected, true).err() {
        Some(ClientError::InvalidTarget(message)) => {
            assert!(message.starts_with("Native memory preflight rejected"), "{message}")
        }
        other => panic!("ceiling below hard + instances must be typed: {other:?}"),
    }
    assert!(base.clone().with_native_memory_ceiling(0).is_err());
    assert!(TunRuntimeConfig::from_sources(Some("1")).unwrap().with_native_memory_ceiling(1).is_err());
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut service = prepare_factory(
        move |_: Option<bytes::BackingMeter>| async move {
            observed.fetch_add(1, Ordering::SeqCst);
            Err::<LoopbackTunDevice, _>(ClientError::Io(std::io::Error::from_raw_os_error(17)))
        },
        cfg(),
        rejected,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    assert!(service.control.backing_budget.is_none() && service.control.app_meter.is_none());
    let report = tokio::time::timeout(Duration::from_secs(1), service.run()).await.unwrap();
    assert_eq!((report.session, report.node), (SessionDeviceState::DeviceFailedSettled, None));
    assert_eq!(calls.load(Ordering::SeqCst), 0, "rejected ceiling cannot reach the factory");
}

/// D108 (review F2): SIGHUP → rebuild is selected only for hard Native. The
/// legacy default (no `MINI_VPN_NATIVE_*`) never registers the handler, so its
/// SIGHUP disposition stays the OS default.
#[test]
fn hangup_rebuild_is_selected_only_for_hard_native() {
    let legacy = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    assert!(legacy.native_limits.is_none());
    assert!(!super::super::hangup_rebuild(&legacy), "legacy default keeps SIGHUP default");
    assert!(super::super::hangup_rebuild(&runtime()), "hard Native requests rebuild");
}

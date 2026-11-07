//! Original Session first/stop boundary around Main capture; Egress-only shell.
use super::*;
#[tokio::test(start_paused = true)]
async fn native_empty_main_capture_keeps_actual_device_first_io_and_original_deadline() {
    let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let meter = raw.main_capture_meter().unwrap().clone();
    let owner = Arc::new({
        let mut control = SessionControl::default();
        control.app_meter = Some(meter);
        control
    });
    let deadline = owner.request_stop();
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    raw.request_stop();
    let sockets = smoltcp::iface::SocketSet::new(vec![]);
    let mut contexts = std::collections::HashMap::new();
    let mut device = SessionEgress::new(&mut raw, owner.clone());
    for _ in 0..2 {
        let (result, released) = crate::client_tun::flush_tx_and_release_downlink_permits(
            &mut device,
            &sockets,
            &mut contexts,
            "empty_native_first",
        )
        .await;
        assert!(result.is_err());
        assert_eq!(released.released_inflight_permit_bytes, 0);
        let ledger = owner.ledger();
        assert!(ledger.first_egress_io.is_some());
        assert_eq!(ledger.shutdown_deadline, Some(deadline));
    }
    drop(device);
    assert!(!budget.quarantined(), "Main has no unknown observation");
    let mut receipt = std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    receipt.settle().await;
    drop(receipt);
    drop(raw);
    drop(owner);
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}

#[tokio::test(start_paused = true)]
async fn native_main_capture_rejection_reaches_original_session_stop_and_first() {
    for prior in [false, true] {
        let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
        let meter = raw.main_capture_meter().unwrap().clone();
        let owner = Arc::new({
            let mut control = SessionControl::default();
            control.app_meter = Some(meter);
            control
        });
        let mut device = SessionEgress::new(&mut raw, owner.clone());
        if prior {
            device.request_stop();
            assert!(device.flush_tx().await.is_err());
        }
        let first = owner.ledger().first_egress_io.clone();
        let deadline = owner.ledger().shutdown_deadline;
        let additional = owner.ledger().additional_failures;
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
        let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
        let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
        ));
        let mut ctx = crate::client_tun::SocketCtx::new(443);
        ctx.downlink_pending = vec![1].into(); // Real non-pristine Legacy allocation under Native Root.
        let mut contexts = std::collections::HashMap::from([(handle, ctx)]);
        let result = {
            crate::client_tun::flush_tx_and_release_downlink_permits(
                &mut device,
                &sockets,
                &mut contexts,
                "native_capture_refusal",
            )
            .await
            .0
        };
        let (q, observed, after, failures) = {
            let ledger = owner.ledger();
            (
                ledger.quarantined,
                ledger.first_egress_io.clone(),
                ledger.shutdown_deadline,
                ledger.additional_failures,
            )
        };
        drop(device);
        std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
        drop(contexts);
        drop(raw);
        drop(owner);
        assert!(result.is_err());
        assert!(q && observed.is_some() && after.is_some());
        if let Some(first) = first {
            assert!(Arc::ptr_eq(&first, observed.as_ref().unwrap()));
            assert_eq!(after, deadline);
            assert_eq!(failures, additional + 1); // New capture fault, not replay of old IO.
        }
        assert!(budget.quarantined());
        assert_eq!(budget.used(), super::super::deadline::layout_bytes());
    }
}

#[tokio::test(start_paused = true)]
async fn returned_egress_carrier_is_not_a_second_loop_source_for_its_original_session() {
    let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let meter = raw.main_capture_meter().unwrap().clone();
    let owner = Arc::new({
        let mut control = SessionControl::default();
        control.app_meter = Some(meter);
        control
    });
    raw.request_stop();
    let returned = SessionEgress::new(&mut raw, owner.clone())
        .flush_tx()
        .await
        .unwrap_err();
    let (identity, additional, deadline) = {
        let ledger = owner.ledger();
        (
            ledger.first_failure.as_ref().unwrap() as *const _ as usize,
            ledger.additional_failures,
            ledger.shutdown_deadline.unwrap(),
        )
    };
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    owner.note_loop_error(&returned);
    let same = {
        let ledger = owner.ledger();
        (
            ledger.first_failure.as_ref().unwrap() as *const _ as usize,
            ledger.additional_failures,
            ledger.shutdown_deadline,
        )
    };
    let foreign = SessionControl::default();
    foreign.note_loop_error(&returned);
    assert!(
        foreign.ledger().first_failure.is_some(),
        "foreign marker must not suppress a new owner fact"
    );
    owner.note_loop_error(&std::io::Error::from_raw_os_error(5));
    let after_fresh = owner.ledger().additional_failures;
    crate::client_tun::compatibility_runtime::business::shutdown_device(&mut raw, &owner).await;
    drop((returned, raw, owner, foreign));
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
    assert_eq!(
        same,
        (identity, additional, Some(deadline)),
        "the original returned carrier is already recorded; loop observation must only preserve stop"
    );
    assert_eq!(
        after_fresh,
        additional + 1,
        "fresh actual IO remains a distinct source fact"
    );
}

#[tokio::test(start_paused = true)]
async fn native_main_admission_q_is_not_a_new_io_and_publishes_each_session_quiet() {
    let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let meter = raw.main_capture_meter().unwrap().clone();
    let owner = Arc::new({
        let mut control = SessionControl::default();
        control.app_meter = Some(meter.clone());
        control
    });
    let foreign = Arc::new({
        let mut control = SessionControl::default();
        control.app_meter = Some(bytes::BackingMeter::with_budget(&budget));
        control
    });
    owner.record(RetainedFailure::from_io(
        SessionControl::origin(FailurePhase::TcpOpen),
        io::Error::from_raw_os_error(5),
    ));
    let deadline = Some(owner.request_stop());
    meter.quarantine();
    let sockets = smoltcp::iface::SocketSet::new(vec![]);
    let mut contexts = std::collections::HashMap::new();
    for control in [&owner, &foreign] {
        assert!(
            !control.ledger().quarantined,
            "budget Q is not this Session's quiet"
        );
        for _ in 0..3 {
            let mut device = SessionEgress::new(&mut raw, control.clone());
            let (result, released) = crate::client_tun::flush_tx_and_release_downlink_permits(
                &mut device,
                &sockets,
                &mut contexts,
                "admission_q",
            )
            .await;
            let error = result.unwrap_err();
            assert!(is_handled_refusal(&error, control));
            assert!(!is_recorded_error(&error, control));
            control.note_loop_error(&error);
            let original = original_source(error);
            assert!(matches!(
                original.get_ref().and_then(|e| {
                    e.downcast_ref::<crate::client_tun::downlink_custody::CaptureRefusal>()
                }),
                Some(
                    crate::client_tun::downlink_custody::CaptureRefusal::Admission(
                        bytes::MeteredCopyError::Quarantined
                    )
                )
            ));
            assert_eq!(released.released_inflight_permit_bytes, 0);
            let ledger = control.ledger();
            assert!(ledger.quarantined && ledger.shutdown_deadline.is_some());
            assert_eq!((ledger.additional_failures, ledger.egress_failures), (0, 0));
            assert!(ledger.first_egress_io.is_none());
        }
    }
    assert_eq!(owner.ledger().shutdown_deadline, deadline);
    assert!(foreign.ledger().first_failure.is_none());
    // A distinct observed inventory fault remains reportable after admission Q.
    let error = SessionEgress::new(&mut raw, owner.clone()).main_capture_failure(Some(
        crate::client_tun::downlink_custody::CaptureRefusal::Inventory.into_io(),
    ));
    owner.note_loop_error(&error);
    assert_eq!(owner.ledger().additional_failures, 1);
    std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    drop((error, raw, owner, foreign, meter));
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
}

#[tokio::test(start_paused = true)]
async fn native_device_callback_external_q_returns_zero_new_facts() {
    let (budget, meter, mut raw) = crate::device::VirtualTunDevice::test_native_egress_only_shell();
    let owner = Arc::new(SessionControl::default());
    meter.quarantine();
    for _ in 0..3 {
        let error = SessionEgress::new(&mut raw, owner.clone())
            .flush_tx()
            .await
            .unwrap_err();
        owner.note_loop_error(&error);
        let ledger = owner.ledger();
        assert!(ledger.first_failure.is_none() && ledger.first_egress_io.is_none());
        assert_eq!(ledger.additional_failures, 0);
    }
    std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    drop((raw, owner, meter));
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
}

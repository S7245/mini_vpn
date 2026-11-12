//! Actual Native Loopback write -> SessionEgress first IO, with waiter Drop panic.
//! This is an explicit Egress-only shell, not full Native RX or Service acceptance.
use super::*;
use std::{
    future::Future,
    sync::atomic::{AtomicBool, Ordering},
    task::{RawWaker, RawWakerVTable, Waker},
};
struct DropOnce {
    armed: AtomicBool,
    payload: Arc<()>,
}
unsafe fn clone(data: *const ()) -> RawWaker {
    unsafe { Arc::increment_strong_count(data.cast::<DropOnce>()) };
    RawWaker::new(data, &VTABLE)
}
unsafe fn retire(data: *const ()) {
    let value = unsafe { Arc::from_raw(data.cast::<DropOnce>()) };
    if value.armed.swap(false, Ordering::AcqRel) {
        std::panic::panic_any(value.payload.clone());
    }
}
unsafe fn wake(data: *const ()) {
    unsafe { retire(data) };
}
unsafe fn wake_ref(_: *const ()) {}
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_ref, retire);
#[tokio::test(start_paused = true)]
async fn native_link_write_io_precedes_waiter_drop_and_keeps_deadline() {
    let (budget, raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let (mut raw, probe) = raw.with_partial_flush_probe();
    for sequence in [10, 15] {
        let builder = etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64)
            .tcp(1000, 2000, sequence, 65535);
        let mut bytes = Vec::with_capacity(builder.size(5));
        builder.write(&mut bytes, b"bytes").unwrap();
        raw.inject_ip_packet_with(bytes.len(), |out| {
            out.copy_from_slice(&bytes);
            Ok(bytes.len())
        })
        .unwrap();
    }
    probe.arm();
    let owner = Arc::new({ let mut control = SessionControl::default(); control.app_meter = raw.main_capture_meter().cloned(); control });
    let deadline = owner.request_stop();
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    let payload = Arc::new(());
    let waker = unsafe {
        Waker::from_raw(RawWaker::new(
            Arc::into_raw(Arc::new(DropOnce {
                armed: AtomicBool::new(true),
                payload: payload.clone(),
            }))
            .cast(),
            &VTABLE,
        ))
    };
    let outcome = {
        let mut device = SessionEgress::new(&mut raw, owner.clone());
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let sockets = smoltcp::iface::SocketSet::new(vec![]);
            let mut contexts = std::collections::HashMap::new();
            let mut flush = Box::pin(crate::client_tun::flush_tx_and_release_downlink_permits(
                &mut device,
                &sockets,
                &mut contexts,
                "actual_io_then_main_capture_q",
            ));
            flush.as_mut().poll(&mut Context::from_waker(&waker))
        }))
    };
    drop(waker);
    std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    drop(raw);
    let result = outcome.unwrap_or_else(|_| panic!("later waiter Drop replaced original EIO"));
    let Poll::Ready((Err(error), _)) = result else {
        panic!("expected actual IO5")
    };
    let source = original_source(error);
    assert_eq!(source.raw_os_error(), Some(5));
    let ledger = owner.ledger();
    assert_eq!(
        ledger.egress_failures, 1,
        "Main must not count the already reported original IO again"
    );
    assert!(ledger.quarantined());
    assert!(Arc::ptr_eq(
        &source,
        ledger.first_egress_io.as_ref().unwrap()
    ));
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().io_kind,
        Some(io::Error::from_raw_os_error(5).kind())
    );
    assert_eq!(ledger.shutdown_deadline, Some(deadline));
    assert_eq!(Arc::strong_count(&payload), 2);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), crate::device::egress::CallbackCustody::test_cold_bytes(1)
        + super::super::deadline::layout_bytes());
}

struct DeferredWake {
    entered: std::sync::mpsc::Sender<()>,
    release: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    panic: bool,
    payload: Arc<()>,
}
impl std::task::Wake for DeferredWake {
    fn wake(self: Arc<Self>) {
        self.entered.send(()).unwrap();
        let (lock, condition) = &*self.release;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = condition.wait(released).unwrap();
        }
        drop(released);
        if self.panic {
            std::panic::panic_any(self.payload.clone());
        }
    }
}
#[tokio::test(start_paused = true)]
async fn native_link_shutdown_keeps_live_stock_and_captures_late_original_panic() {
    for panic in [false, true] {
        let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
        let peer = raw.test_outbound_link();
        for _ in 0..4 {
            assert!(peer.push(bytes::BytesMut::from(&b"old"[..])));
        }
        raw.inject_ip_packet_with(3, |_| Ok(3)).unwrap();
        let release = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let payload = Arc::new(());
        let (sent, received) = std::sync::mpsc::channel();
        let waker = Waker::from(Arc::new(DeferredWake {
            entered: sent,
            release: release.clone(),
            panic,
            payload: payload.clone(),
        }));
        let peer_thread;
        {
            let mut flush = Box::pin(raw.flush_tx());
            assert!(
                flush
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            peer_thread = {
                let peer = peer.clone();
                std::thread::spawn(move || {
                    peer.test_pop();
                })
            };
            received
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap();
            // The original wake is still blocked after scheduling this same writer.
            assert!(matches!(
                flush.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(()))
            ));
        }
        drop(waker);
        let owner = Arc::new(SessionControl::default());
        let deadline = owner.request_stop();
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
        {
            let mut shutdown = Box::pin(
                crate::client_tun::compatibility_runtime::business::shutdown_device(
                    &mut raw, &owner,
                ),
            );
            assert!(
                shutdown
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert!(
                budget.used() > 0,
                "actual cold callback State remains owned by terminal observer"
            );
            assert!(
                !budget.quarantined(),
                "ordinary in-flight wake is not itself an unknown failure"
            );
            {
                let (lock, condition) = &*release;
                *lock.lock().unwrap() = true;
                condition.notify_all();
            }
            peer_thread.join().unwrap();
            assert!(matches!(
                shutdown
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready((None, None))
            ));
        }
        let ledger = owner.ledger();
        assert_eq!(ledger.shutdown_deadline, Some(deadline));
        assert!(
            ledger.first_egress_io.is_none(),
            "Panic cannot be recorded as a synthetic original IO"
        );
        if panic {
            assert_eq!(
                ledger.first_failure.as_ref().unwrap().facts().class,
                crate::shared::errors::FailureClass::Panic
            );
            assert_eq!(
                ledger.first_failure.as_ref().unwrap().facts().origin.phase,
                crate::shared::errors::FailurePhase::DeviceWrite
            );
            assert_eq!(Arc::strong_count(&payload), 2);
        } else {
            assert!(ledger.first_failure.is_none());
        }
        assert_eq!(ledger.additional_failures, 0);
        drop(ledger);
        drop(raw);
        assert_eq!(budget.used(), if panic { crate::device::egress::CallbackCustody::test_cold_bytes(1) } else { 0 });
        assert_eq!(budget.quarantined(), panic);
    }
}

struct ImmediatePanic(Arc<()>);
impl std::task::Wake for ImmediatePanic {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(self.0.clone());
    }
}
#[tokio::test(start_paused = true)]
async fn native_link_peer_panic_is_reported_once_without_fake_io() {
    let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let peer = raw.test_outbound_link();
    for _ in 0..4 {
        assert!(peer.push(bytes::BytesMut::from(&b"old"[..])));
    }
    raw.inject_ip_packet_with(3, |_| Ok(3)).unwrap();
    let owner = Arc::new(SessionControl::default());
    let deadline = owner.request_stop();
    let payload = Arc::new(());
    let waker = Waker::from(Arc::new(ImmediatePanic(payload.clone())));
    {
        let mut device = SessionEgress::new(&mut raw, owner.clone());
        {
            let mut flush = Box::pin(device.flush_tx());
            assert!(
                flush
                    .as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            peer.test_pop();
            let Poll::Ready(Err(error)) =
                flush.as_mut().poll(&mut Context::from_waker(Waker::noop()))
            else {
                panic!("expected callback carrier")
            };
            drop(error);
        }
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
        drop(device.flush_tx().await.unwrap_err());
    }
    drop(waker);
    crate::client_tun::compatibility_runtime::business::shutdown_device(&mut raw, &owner).await;
    let ledger = owner.ledger();
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().class,
        crate::shared::errors::FailureClass::Panic
    );
    assert!(ledger.first_egress_io.is_none());
    assert_eq!(
        ledger.additional_failures, 0,
        "repeated carrier/terminal observation is not a new fault"
    );
    assert_eq!(ledger.egress_failures, 2);
    assert_eq!(ledger.shutdown_deadline, Some(deadline));
    drop(ledger);
    drop(raw);
    assert_eq!(Arc::strong_count(&payload), 2);
    assert_eq!(budget.used(), crate::device::egress::CallbackCustody::test_cold_bytes(1));
}

#[tokio::test]
async fn native_link_inject_after_peer_panic_keeps_original_typed_first() {
    let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let peer = raw.test_outbound_link();
    for _ in 0..4 {
        peer.push(bytes::BytesMut::from(&b"old"[..]));
    }
    raw.inject_ip_packet_with(3, |_| Ok(3)).unwrap();
    let waker = Waker::from(Arc::new(ImmediatePanic(Arc::new(()))));
    {
        let mut flush = Box::pin(raw.flush_tx());
        assert!(
            flush
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        peer.test_pop();
    }
    let owner = Arc::new(SessionControl::default());
    {
        let mut device = SessionEgress::new(&mut raw, owner.clone());
        drop(
            device
                .inject_ip_packet_with(3, |_| panic!("fault must precede encoder"))
                .unwrap_err(),
        );
    }
    crate::client_tun::compatibility_runtime::business::shutdown_device(&mut raw, &owner).await;
    drop(raw);
    let ledger = owner.ledger();
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().class,
        crate::shared::errors::FailureClass::Panic
    );
    assert!(ledger.first_egress_io.is_none());
    assert_eq!(ledger.additional_failures, 0);
    assert_eq!(budget.used(), crate::device::egress::CallbackCustody::test_cold_bytes(1));
}

#[tokio::test]
async fn native_link_shutdown_receipt_waits_for_claim_and_stays_failed_on_repeat() {
    let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let peer = raw.test_outbound_link();
    for _ in 0..4 {
        peer.push(bytes::BytesMut::from(&b"old"[..]));
    }
    raw.inject_ip_packet_with(3, |_| Ok(3)).unwrap();
    let waker = Waker::from(Arc::new(ImmediatePanic(Arc::new(()))));
    {
        let mut flush = Box::pin(raw.flush_tx());
        assert!(
            flush
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        peer.test_pop();
    }
    let mut receipt = std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    let before = receipt.state();
    let mut repeated = std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    assert_eq!(
        repeated.state(),
        crate::device::DeviceShutdownState::PendingCallbacks
    );
    {
        let mut wait =
            Box::pin(repeated.settle_callbacks(SessionControl::origin(FailurePhase::DeviceWrite)));
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    // Public settlement must preserve the original unreported cause for Session.
    assert_eq!(
        receipt.settle().await,
        crate::device::DeviceShutdownState::FailedSettled
    );
    assert_eq!(
        receipt.settle().await,
        crate::device::DeviceShutdownState::FailedSettled
    );
    let report = receipt
        .settle_callbacks(SessionControl::origin(FailurePhase::DeviceWrite))
        .await
        .unwrap();
    let after = receipt.state();
    let _ = repeated
        .settle_callbacks(SessionControl::origin(FailurePhase::DeviceWrite))
        .await;
    let repeat = repeated.state();
    drop(receipt);
    drop(repeated);
    drop(raw);
    assert_eq!(
        report.failure.unwrap().facts().class,
        crate::shared::errors::FailureClass::Panic
    );
    assert_eq!(before, crate::device::DeviceShutdownState::PendingCallbacks);
    assert_eq!(after, crate::device::DeviceShutdownState::FailedSettled);
    assert_eq!(repeat, crate::device::DeviceShutdownState::FailedSettled);
    assert_eq!(budget.used(), crate::device::egress::CallbackCustody::test_cold_bytes(1));
}

#[tokio::test]
async fn native_link_stopped_typed_gates_preserve_panic_without_repeat_io() {
    let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let peer = raw.test_outbound_link();
    for _ in 0..4 {
        peer.push(bytes::BytesMut::from(&b"old"[..]));
    }
    raw.inject_ip_packet_with(3, |_| Ok(3)).unwrap();
    let waker = Waker::from(Arc::new(ImmediatePanic(Arc::new(()))));
    {
        let mut flush = Box::pin(raw.flush_tx());
        assert!(
            flush
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        peer.test_pop();
    }
    let mut receipt = std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    let owner = Arc::new(SessionControl::default());
    {
        let mut device = SessionEgress::new(&mut raw, owner.clone());
        drop(
            device
                .inject_ip_packet_with(3, |_| panic!("stopped gate must reject"))
                .unwrap_err(),
        );
        drop(device.flush_tx().await.unwrap_err());
    }
    let report = receipt
        .settle_callbacks(SessionControl::origin(FailurePhase::DeviceWrite))
        .await
        .unwrap();
    assert!(report.failure.is_none());
    assert!(report.source.is_none());
    assert_eq!(report.additional, 0);
    assert_eq!(
        receipt.state(),
        crate::device::DeviceShutdownState::FailedSettled
    );
    assert!(
        receipt.io_kind().is_none(),
        "callback Panic is never a backend IO"
    );
    crate::client_tun::compatibility_runtime::business::shutdown_device(&mut raw, &owner).await;
    let ledger = owner.ledger();
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().class,
        crate::shared::errors::FailureClass::Panic
    );
    assert!(ledger.first_egress_io.is_none());
    assert_eq!(ledger.additional_failures, 0);
    drop(ledger);
    drop(receipt);
    drop(raw);
    assert_eq!(budget.used(), crate::device::egress::CallbackCustody::test_cold_bytes(1));
}

//! Actual Native Loopback IO plus the original Session deadline and fixed role.
use super::super::deadline::{claim, layout_bytes, prepare, release};
use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::Waker,
};

fn owner(raw: &crate::harness::LoopbackTunDevice) -> Arc<SessionControl> {
    Arc::new({
        let mut owner = SessionControl::default();
        owner.app_meter = raw.main_capture_meter().cloned();
        owner
    })
}
struct Tracked<F> {
    work: F,
    dropped: Arc<AtomicUsize>,
}
impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|v| &mut v.work) }.poll(cx)
    }
}
impl<F> Drop for Tracked<F> {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(start_paused = true)]
async fn actual_pending_flush_later_stop_earlier_parent_freezes_without_drop_or_ack() {
    let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let owner = owner(&raw);
    let before = budget.used();
    prepare(&owner).unwrap();
    assert_eq!(budget.used() - before, layout_bytes());
    let mut native = claim(&owner).unwrap().unwrap();
    let peer = raw.test_outbound_link();
    for _ in 0..4 {
        assert!(peer.push(bytes::BytesMut::from(&b"old"[..])));
    }
    raw.inject_ip_packet_with(3, |out| {
        out.copy_from_slice(b"new");
        Ok(3)
    })
    .unwrap();
    let initial_charge = budget.used();
    let dropped = Arc::new(AtomicUsize::new(0));
    let work = Tracked {
        work: raw.flush_tx(),
        dropped: dropped.clone(),
    };
    let old_frame = std::mem::size_of_val(&work);
    let guarded = native.flush(&owner, work);
    let new_frame = std::mem::size_of_val(&guarded);
    println!(
        "[N1-Session-IO-layout] cold={} raw_tracked_flush={} guarded_flush={} delta={} Sleep={} simultaneous_roles=1 timer_max=1",
        layout_bytes(),
        old_frame,
        new_frame,
        new_frame - old_frame,
        std::mem::size_of::<tokio::time::Sleep>()
    );
    let mut guarded = Box::pin(guarded);
    assert!(
        guarded
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(!owner.ledger().quarantined);
    let original = owner.request_stop();
    assert!(
        guarded
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let earlier = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    owner
        .parent_deadline
        .set(Arc::new(std::sync::Mutex::new(Some(earlier))))
        .unwrap();
    assert!(earlier < original);
    // Re-poll the actual same IO with a newly tightened parent, then its Sleep.
    assert!(
        guarded
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    assert!(
        guarded
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(owner.ledger().quarantined && budget.quarantined());
    assert_eq!(owner.ledger().shutdown_deadline, Some(earlier));
    assert!(owner.ledger().first_failure.is_none());
    assert_eq!(budget.used(), initial_charge);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert_eq!(peer.packets_for_test().len(), 4);
    peer.test_pop(); // Actual IO would now complete; frozen must never retry it.
    for _ in 0..3 {
        let mut observer = std::pin::pin!(std::future::poll_fn(|cx| guarded.as_mut().poll(cx)));
        assert!(
            observer
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(peer.packets_for_test().len(), 3);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(guarded);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        raw.queued_tx_bytes(),
        3,
        "selected actual packet stayed in its Device"
    );
    assert_eq!(raw.take_flushed_tcp_packets().len(), 0);
    release(&owner, native);
    std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    drop(raw);
    assert!(budget.used() >= layout_bytes() && budget.quarantined());
}

#[tokio::test(start_paused = true)]
async fn actual_io_ready_at_deadline_wins_and_zero_poll_has_real_drop() {
    for zero_poll in [true, false] {
        let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
        let owner = owner(&raw);
        prepare(&owner).unwrap();
        let mut native = claim(&owner).unwrap().unwrap();
        let peer = raw.test_outbound_link();
        for _ in 0..4 {
            assert!(peer.push(bytes::BytesMut::from(&b"old"[..])));
        }
        raw.inject_ip_packet_with(3, |out| {
            out.copy_from_slice(b"new");
            Ok(3)
        })
        .unwrap();
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut guarded = Box::pin(native.flush(
            &owner,
            Tracked {
                work: raw.flush_tx(),
                dropped: dropped.clone(),
            },
        ));
        if !zero_poll {
            assert!(
                guarded
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            let deadline = owner.request_stop();
            tokio::time::advance(deadline - tokio::time::Instant::now()).await;
            peer.test_pop();
            assert!(matches!(
                guarded
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(Ok(()))
            ));
            assert!(!owner.ledger().quarantined);
            assert_eq!(&peer.packets_for_test().last().unwrap()[..], b"new");
        }
        drop(guarded);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        release(&owner, native);
        let mut receipt = std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
        receipt.settle().await;
        drop((receipt, raw, owner));
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn cold_q_refusal_preserves_original_source_and_is_not_another_io() {
    for prior in [false, true] {
        let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
        let owner = owner(&raw);
        prepare(&owner).unwrap();
        let original = prior.then(|| super::super::failed(&owner, io::Error::from_raw_os_error(9)));
        let first = owner.ledger().first_egress_io.clone();
        let original_deadline = owner.request_stop();
        owner.app_meter.as_ref().unwrap().quarantine();
        let result = SessionEgress::new(&mut raw, owner.clone())
            .flush_tx()
            .await
            .unwrap_err();
        assert!(super::super::is_handled_refusal(&result, &owner));
        let ledger = owner.ledger();
        assert_eq!(ledger.egress_failures, u64::from(prior));
        assert_eq!(ledger.additional_failures, 0);
        assert_eq!(ledger.shutdown_deadline, Some(original_deadline));
        if let Some(first) = first {
            assert!(Arc::ptr_eq(
                &first,
                ledger.first_egress_io.as_ref().unwrap()
            ));
        } else {
            assert!(ledger.first_failure.is_none());
        }
        drop(ledger);
        drop(original);
        std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
        drop(raw);
        assert!(budget.quarantined());
    }
}

#[tokio::test(start_paused = true)]
async fn actual_flush_drop_and_original_callback_drop_keep_two_original_boxes() {
    struct Work(Arc<()>);
    impl Future for Work {
        type Output = io::Result<()>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }
    impl Drop for Work {
        fn drop(&mut self) {
            std::panic::panic_any(self.0.clone());
        }
    }
    struct Callback(Arc<()>);
    impl std::task::Wake for Callback {
        fn wake(self: Arc<Self>) {}
    }
    impl Drop for Callback {
        fn drop(&mut self) {
            std::panic::panic_any(self.0.clone());
        }
    }
    let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let owner = owner(&raw);
    prepare(&owner).unwrap();
    let mut native = claim(&owner).unwrap().unwrap();
    let first = Arc::new(());
    let second = Arc::new(());
    let waker = Waker::from(Arc::new(Callback(second.clone())));
    let mut flush = Box::pin(native.flush(&owner, Work(first.clone())));
    assert!(
        flush
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(flush); // Work's actual Drop fails while Stop owns its safe proxy.
    assert_eq!(Arc::strong_count(&first), 2);
    let original_deadline = owner.ledger().shutdown_deadline;
    release(&owner, native); // Independently dispose the original callback.
    assert_eq!(Arc::strong_count(&second), 2);
    assert_eq!(owner.ledger().shutdown_deadline, original_deadline);
    assert!(owner.ledger().quarantined && budget.quarantined());
    std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    drop((raw, owner));
    assert_eq!(budget.used(), layout_bytes());
}

#[tokio::test(start_paused = true)]
async fn actual_cold_slot_poison_refuses_claim_and_preserves_returned_inventory() {
    for in_use in [false, true] {
        let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
        let owner = owner(&raw);
        prepare(&owner).unwrap();
        let active = in_use.then(|| claim(&owner).unwrap().unwrap());
        let used = budget.used();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _actual = owner.egress_cold.lock().unwrap();
            panic!("actual original cold-slot poison");
        }));
        if let Some(native) = active {
            release(&owner, native);
        } else {
            assert!(matches!(
                claim(&owner),
                Err(bytes::MeteredCopyError::Quarantined)
            ));
        }
        assert!(owner.ledger().quarantined && budget.quarantined());
        assert_eq!(budget.used(), used);
        assert!(!super::super::deadline::retire(&owner));
        assert!(owner.ledger().first_failure.is_none());
        std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
        drop(raw);
        assert!(budget.used() >= layout_bytes());
    }
}

#[tokio::test]
async fn actual_inflight_callback_keeps_returned_stock_without_false_q_until_exit() {
    struct Block {
        entered: std::sync::mpsc::Sender<()>,
        gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    }
    impl std::task::Wake for Block {
        fn wake(self: Arc<Self>) {
            self.entered.send(()).unwrap();
            let (lock, wake) = &*self.gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = wake.wait(open).unwrap();
            }
        }
    }
    let (budget, mut raw) = crate::harness::LoopbackTunDevice::test_native_egress_only_shell();
    let owner = owner(&raw);
    prepare(&owner).unwrap();
    let mut native = claim(&owner).unwrap().unwrap();
    let peer = raw.test_outbound_link();
    for _ in 0..4 {
        assert!(peer.push(bytes::BytesMut::from(&b"old"[..])));
    }
    raw.inject_ip_packet_with(3, |_| Ok(3)).unwrap();
    let (entered, waiting) = std::sync::mpsc::channel();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let waker = Waker::from(Arc::new(Block {
        entered,
        gate: gate.clone(),
    }));
    let mut flush = Box::pin(native.flush(&owner, raw.flush_tx()));
    assert!(
        flush
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let thread = std::thread::spawn(move || {
        peer.test_pop();
    });
    waiting
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    assert!(matches!(
        flush.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    ));
    drop(flush);
    drop(waker);
    let before = budget.used();
    release(&owner, native);
    assert_eq!(budget.used(), before);
    assert!(!super::super::deadline::retire(&owner));
    assert!(!owner.ledger().quarantined && !budget.quarantined());
    assert!(matches!(
        claim(&owner),
        Err(bytes::MeteredCopyError::BudgetFull)
    ));
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    thread.join().unwrap();
    assert!(super::super::deadline::retire(&owner));
    assert_eq!(before - budget.used(), layout_bytes());
    let mut receipt = std::future::poll_fn(|cx| raw.poll_shutdown(cx)).await;
    receipt.settle().await;
    drop((receipt, raw, owner));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

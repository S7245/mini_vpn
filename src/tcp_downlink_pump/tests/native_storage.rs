use super::*;
use crate::tcp_egress::TcpEgressLease;
use bytes::{BackingBudget, BackingMeter, MeteredCopyError};

fn payload(meter: &BackingMeter, value: &[u8]) -> OwnedPayload {
    OwnedPayload::Native((Bytes::copy_from_slice_metered(value, meter).unwrap()).into())
}

#[tokio::test]
async fn native_fifo_full_keeps_pending_and_slices_keep_original_charge() {
    let max_records = 4 << 20; // Original Native C; every live payload is nonempty.
    eprintln!("[N1-fifo-layout-bound] max_nonempty_records={max_records} actual_record={} final_layout={} largest_old_plus_new={} logical_credit_and_output_slab_backing_separate=true bound_not_full_allocation=true",
        std::mem::size_of::<ByteChunk>(), std::alloc::Layout::array::<ByteChunk>(max_records).unwrap().size(),
        std::alloc::Layout::array::<ByteChunk>(max_records).unwrap().size() + std::alloc::Layout::array::<ByteChunk>(max_records / 2).unwrap().size());
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let q = AsyncByteBoundedFlowQueue::new(16).unwrap();
    q.bind_native_meter(Some(meter.clone())).unwrap();
    let (clock, _facts) = super::native_waiters::clock(&meter);
    assert!(q.bind_native_failures(clock));
    let mut pending = Some(payload(&meter, b"abcdefgh"));
    let identity = match pending.as_ref().unwrap() {
        OwnedPayload::Native(b) => b.allocation_identity(),
        _ => unreachable!(),
    };
    let held_budget = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert_eq!(
        q.push_custodied(&mut pending).await,
        Err(ByteCustodyError::Capacity(MeteredCopyError::BudgetFull))
    );
    assert_eq!(pending.as_ref().unwrap().as_ref(), b"abcdefgh");
    assert_eq!(q.inner.state.lock().await.queue.queued_bytes, 0);
    drop(held_budget);
    q.push_custodied(&mut pending).await.unwrap();
    assert!(pending.is_none());
    let before = budget.used();
    let mut held = None;
    q.recv_custodied(3, &mut held).await.unwrap();
    let first = held.take().unwrap();
    assert_eq!(first.bytes.as_ref(), b"abc");
    assert_eq!(first.bytes.allocation_identity(), identity);
    let segment = OwnedPayload::from_segment(&first.bytes, 1..3);
    assert_eq!(budget.used(), before, "no output or segment allocation");
    drop(first);
    let receipt = q.dispose_custodied().unwrap().unwrap();
    assert_eq!(
        (receipt.emitted, receipt.consumed, receipt.discarded_bytes),
        (8, 3, 5)
    );
    drop(q);
    assert!(
        budget.used() > 0,
        "selected segment still owns original backing"
    );
    drop(segment);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn native_leased_fifo_keeps_data_and_original_permit_domains_separate() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let q = AsyncLeasedByteFlowQueue::new_custodied_with_mode(
        16,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    q.bind_native_meter(Some(meter.clone())).unwrap();
    let (clock, _facts) = super::native_waiters::clock(&meter);
    assert!(q.bind_native_failures(clock));
    q.push_custodied(&mut Some(payload(&meter, b"abcdefgh")))
        .await
        .unwrap();
    q.push_custodied(&mut Some(payload(&meter, b"ijkl")))
        .await
        .unwrap();
    let before = budget.used();
    let mut held = None;
    q.recv_custodied(16, &mut held).await.unwrap();
    let mut held = held.unwrap();
    assert_eq!(held.data.bytes.as_ref(), b"abcdefgh");
    assert_eq!(held.permit.remaining_bytes(), 8);
    assert_eq!(budget.used(), before);
    let selected = OwnedPayload::from_segment(&held.data.bytes, 2..5);
    let selected_permit = held.permit.split_to(3).unwrap();
    drop(held);
    let receipt = q.dispose_custodied().unwrap().unwrap();
    assert_eq!(
        (receipt.emitted, receipt.consumed, receipt.discarded_bytes),
        (12, 8, 4)
    );
    assert!(!q.physical_zero(), "original Main permit still owns credit");
    drop(selected_permit);
    assert!(q.physical_zero());
    drop(q);
    assert!(
        budget.used() > 0,
        "physical bytes outlive logical lease release"
    );
    drop(selected);
    assert_eq!(budget.used(), 0);
}

struct PanicWake(std::sync::Arc<usize>);
impl std::task::Wake for PanicWake {
    fn wake(self: std::sync::Arc<Self>) {
        std::panic::panic_any(self.0.clone());
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        std::panic::panic_any(self.0.clone());
    }
}

#[tokio::test]
async fn native_byte_notify_panic_immediately_quarantines_original_budget() {
    use std::{
        future::Future,
        task::{Context, Poll},
    };
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let q = AsyncByteBoundedFlowQueue::new(16).unwrap();
    q.bind_native_meter(Some(meter.clone())).unwrap();
    let (clock, _facts) = super::native_waiters::clock(&meter);
    assert!(q.bind_native_failures(clock));
    let first = std::sync::Arc::new(71usize);
    let waker = std::task::Waker::from(std::sync::Arc::new(PanicWake(first.clone())));
    let mut slot = None;
    let mut receive = Box::pin(q.recv_custodied(8, &mut slot));
    assert!(
        receive
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut pending = Some(payload(&meter, b"abcd"));
    let mut push = Box::pin(q.push_custodied(&mut pending));
    let result = push
        .as_mut()
        .poll(&mut Context::from_waker(std::task::Waker::noop()));
    assert!(matches!(
        result,
        Poll::Ready(Err(ByteCustodyError::Invalid))
    ));
    assert!(q.inner.waiters.get().unwrap().retains_arc(&first));
    assert!(
        budget.quarantined(),
        "no recovery/Root poll may be needed to close admission"
    );
    assert!(meter.reserve_cold_layout(1).is_err());
    drop((push, receive));
    assert!(
        pending.is_none(),
        "the committed prefix remains in its real FIFO"
    );
    assert_eq!(q.inner.state.lock().await.queue.queued_bytes, 4);
    drop((q, slot));
    assert_eq!(
        budget.used(),
        native_wait::Waiters::layout_bytes() + crate::shared::errors::FailureOrder::layout_bytes()
    );
}

#[tokio::test]
async fn native_leased_notify_panic_immediately_quarantines_original_budget() {
    use std::{future::Future, task::Context};
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let q = AsyncLeasedByteFlowQueue::new_custodied(16).unwrap();
    q.bind_native_meter(Some(meter.clone())).unwrap();
    let (clock, _facts) = super::native_waiters::clock(&meter);
    assert!(q.bind_native_failures(clock));
    let first = std::sync::Arc::new(72usize);
    let waker = std::task::Waker::from(std::sync::Arc::new(PanicWake(first.clone())));
    let mut slot = None;
    let mut receive = Box::pin(q.recv_custodied(8, &mut slot));
    assert!(
        receive
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut pending = Some(payload(&meter, b"abcd"));
    let mut push = Box::pin(q.push_custodied(&mut pending));
    let result = push
        .as_mut()
        .poll(&mut Context::from_waker(std::task::Waker::noop()));
    assert!(matches!(
        result,
        std::task::Poll::Ready(Err(ByteCustodyError::Invalid))
    ));
    assert!(q.retained_callback_arc(&first));
    assert!(budget.quarantined());
    assert!(meter.reserve_cold_layout(1).is_err());
    drop((push, receive));
    assert!(pending.is_none());
    assert_eq!(q.snapshot_now().queued_bytes, 4);
    drop((q, slot));
    assert_eq!(
        budget.used(),
        native_wait::Waiters::layout_bytes() + crate::shared::errors::FailureOrder::layout_bytes()
    );
}

#[tokio::test]
async fn last_main_permit_retains_q_observer_after_fifo_layout_is_released() {
    use std::{future::Future, task::Context};
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let q = AsyncLeasedByteFlowQueue::new_custodied_with_mode(
        4,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    q.bind_native_meter(Some(meter.clone())).unwrap();
    let (clock, _facts) = super::native_waiters::clock(&meter);
    assert!(q.bind_native_failures(clock));
    q.push_custodied(&mut Some(payload(&meter, b"abcd")))
        .await
        .unwrap();
    let mut held = None;
    q.recv_custodied(4, &mut held).await.unwrap();
    let LeasedQueueBatch { data, mut permit } = held.unwrap();
    let first = std::sync::Arc::new(73usize);
    let waker = std::task::Waker::from(std::sync::Arc::new(PanicWake(first.clone())));
    let mut waiting = Box::pin(q.wait_read_len(4));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(
        q.dispose_custodied().unwrap().is_none(),
        "live original waiter prevents a clean receipt"
    );
    drop(data);
    assert_eq!(
        budget.used(),
        native_wait::Waiters::retained_role_bytes()
            + crate::shared::errors::FailureOrder::layout_bytes(),
        "DATA/slab gone; original producer callback cold stock still live"
    );
    assert_eq!(permit.release(4), 4);
    assert!(q.retained_callback_arc(&first));
    assert_eq!(permit.remaining_bytes(), 0);
    assert_eq!(
        budget.used(),
        native_wait::Waiters::retained_role_bytes()
            + crate::shared::errors::FailureOrder::layout_bytes(),
        "last permit retains original callback Box and actual cold charge"
    );
    assert!(!q.physical_zero());
    drop(waiting);
    drop((permit, q));
    assert_eq!(
        budget.used(),
        native_wait::Waiters::retained_role_bytes()
            + crate::shared::errors::FailureOrder::layout_bytes()
    );
}

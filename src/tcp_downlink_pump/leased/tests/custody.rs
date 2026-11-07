//! Real original queue, backing, lease and Notify custody. No actor/TUN claim.
use super::*;
use crate::tcp_egress::TcpEgressLease;
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

#[tokio::test]
async fn d16_rejected_read_commit_returns_the_same_original_backing() {
    let budget = D16GlobalByteBudget::new(32).unwrap();
    let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
        16,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    let other = AsyncLeasedByteFlowQueue::new_with_global_budget(
        16,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    other.push(Bytes::from_static(b"held")).await.unwrap();
    let mut other_lease = other.recv_up_to(4).await.unwrap();

    let empty = queue.reserve_read_up_to(8).await.unwrap();
    let (error, returned) = empty.commit_retaining(Bytes::new()).unwrap_err();
    assert_eq!(error, ByteQueuePushError::EmptyChunk);
    assert!(returned.is_empty());

    let oversized = queue.reserve_read_up_to(8).await.unwrap();
    let oversized_bytes = Bytes::from(vec![3; 9]);
    let oversized_backing = oversized_bytes.as_ptr();
    let (error, returned) = oversized.commit_retaining(oversized_bytes).unwrap_err();
    assert_eq!(
        error,
        ByteQueuePushError::ChunkExceedsCapacity {
            len: 9,
            capacity: 8,
        }
    );
    assert_eq!(returned.as_ptr(), oversized_backing);

    let reservation = queue.reserve_read_up_to(8).await.unwrap();
    let bytes = Bytes::from(vec![7; 8]);
    let backing = bytes.as_ptr();
    queue
        .close_for_terminal_and_mark_ready("root", "cancelled")
        .await;
    let (error, returned) = reservation.commit_retaining(bytes).unwrap_err();
    assert_eq!(error, ByteQueuePushError::Closed);
    assert_eq!(returned.as_ptr(), backing);
    assert_eq!(returned.as_ref(), &[7; 8]);
    assert_eq!(queue.snapshot_now().reserved_bytes, 0);
    assert_eq!(budget.snapshot().reserved_bytes, 0);
    assert_eq!(budget.snapshot().owned_bytes, 4);
    assert!(queue.d16_terminal_quota_zero(&budget));
    assert!(!other.d16_terminal_quota_zero(&budget));
    other.close_for_remote_eof_and_mark_ready().await;
    assert_eq!(other_lease.permit_mut().release(4), 4);
    assert_eq!(budget.snapshot().total_bytes(), 0);
}

#[tokio::test]
async fn d16_terminal_quota_requires_exact_budget_and_released_original_permit() {
    let budget = D16GlobalByteBudget::new(16).unwrap();
    let foreign = D16GlobalByteBudget::new(16).unwrap();
    let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
        16,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    let copy = queue.clone();
    assert!(queue.same_queue(&copy));
    assert!(queue.d16_uses_budget(&budget));
    assert!(!queue.d16_uses_budget(&foreign));
    assert!(!queue.d16_terminal_quota_zero(&budget));
    queue.push(Bytes::from_static(b"abcd")).await.unwrap();
    let mut leased = queue.recv_up_to(4).await.unwrap();
    queue.close_for_remote_eof_and_mark_ready().await;
    assert!(!queue.d16_terminal_quota_zero(&budget));
    assert!(!queue.d16_terminal_quota_zero(&foreign));
    assert_eq!(leased.permit.release(4), 4);
    assert!(queue.d16_terminal_quota_zero(&budget));
    assert!(!queue.d16_terminal_quota_zero(&foreign));
}

#[tokio::test]
async fn partial_coalesced_backing_and_original_split_release_are_distinct() {
    let q = AsyncLeasedByteFlowQueue::new_custodied(16).unwrap();
    let mut v = Vec::with_capacity(24);
    v.extend_from_slice(&[1; 8]);
    q.push_custodied(&mut Some(v)).await.unwrap();
    q.push_custodied(&mut Some(vec![2; 8])).await.unwrap();
    let mut held = None;
    assert!(q.recv_custodied(4, &mut held).await.unwrap());
    let mut first = held.take().unwrap();
    assert_eq!(first.data.bytes.as_ref(), &[1; 4]);
    assert!(first.data.allocation >= 4);
    assert!(q.owns_permit(&first.permit, 4));
    let mut segment = first.permit.split_to(2).unwrap();
    assert!(q.owns_permit(&segment, 2));
    assert_eq!(segment.release(2), 2);
    assert_eq!(first.permit.release(2), 2);
    assert!(q.recv_custodied(8, &mut held).await.unwrap());
    let mut second = held.take().unwrap();
    assert_eq!(second.data.bytes.as_ref(), &[1, 1, 1, 1, 2, 2, 2, 2]);
    let receipt = q.dispose_custodied().unwrap().unwrap();
    assert_eq!(
        (
            receipt.sources,
            receipt.emitted,
            receipt.batches,
            receipt.consumed
        ),
        (2, 16, 2, 12)
    );
    assert_eq!(
        (
            receipt.discarded_chunks,
            receipt.discarded_bytes,
            receipt.discarded_allocation
        ),
        (1, 4, 8)
    );
    assert!(receipt.metadata_slots >= 1 && receipt.metadata_bytes > 0);
    assert!(!q.physical_zero()); // Original eight-byte lease is still physically held.
    assert_eq!(second.permit.release(8), 8);
    assert_eq!(q.snapshot_now().leased_bytes, 0);
    assert!(q.physical_zero());
}

#[tokio::test]
async fn full_four_mib_counts_actual_leases_and_preserves_rejected_pending() {
    let q = AsyncLeasedByteFlowQueue::new_custodied(4 * 1024 * 1024).unwrap();
    for _ in 0..64 {
        q.push_custodied(&mut Some(vec![7; 65_536])).await.unwrap();
    }
    let mut held = None;
    q.recv_custodied(128 * 1024, &mut held).await.unwrap();
    let snapshot = q.snapshot_now();
    assert_eq!(snapshot.owned_bytes(), 4 * 1024 * 1024);
    assert_eq!(snapshot.leased_bytes, 128 * 1024);
    let mut pending = Some(vec![9]);
    let identity = pending.as_ref().unwrap().as_ptr();
    assert_eq!(
        q.push_custodied(&mut pending).await,
        Err(ByteCustodyError::Rejected)
    );
    assert_eq!(pending.as_ref().unwrap().as_ptr(), identity);
    assert!(matches!(q.dispose_custodied(), Ok(None))); // bounded 32 original chunks
    assert!(matches!(q.dispose_custodied(), Ok(Some(_))));
    assert!(!q.physical_zero());
    drop(held.take()); // Actual original permit Drop, not a synthetic refund.
    assert!(q.physical_zero());
}

#[tokio::test]
async fn foreign_mode_and_length_rejection_never_refund_original_permits() {
    let q = AsyncLeasedByteFlowQueue::new_custodied(8).unwrap();
    let other = AsyncLeasedByteFlowQueue::new_custodied(8).unwrap();
    other.push_custodied(&mut Some(vec![1; 4])).await.unwrap();
    let mut held = None;
    other.recv_custodied(4, &mut held).await.unwrap();
    let original = held.as_ref().unwrap();
    assert!(!q.owns_permit(&original.permit, 4));
    assert!(!other.owns_permit(&original.permit, 3));
    assert!(!other.owns_permit(&original.permit, 0));
    assert_eq!(other.snapshot_now().leased_bytes, 4);
    let ordered = AsyncLeasedByteFlowQueue::new_with_release_mode(
        8,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    ordered.push(Bytes::from_static(b"abcd")).await.unwrap();
    let leased = ordered.recv_up_to(4).await.unwrap();
    assert!(!ordered.owns_permit(&leased.permit, 4));
    assert_eq!(ordered.snapshot_now().leased_bytes, 4);
    let legacy = AsyncLeasedByteFlowQueue::new(8).unwrap();
    legacy.push(Bytes::from_static(b"abcd")).await.unwrap();
    let legacy_lease = legacy.recv_up_to(4).await.unwrap();
    assert!(!legacy.owns_permit(&legacy_lease.permit, 4));
    assert_eq!(legacy.snapshot_now().leased_bytes, 4);
    drop(legacy_lease);
    assert!(q.same_queue(&q.clone()) && !q.same_queue(&other));
    other.dispose_custodied().unwrap().unwrap();
    assert!(!other.physical_zero());
    drop(held);
    assert!(other.physical_zero());
    drop(leased);
    assert_eq!(ordered.snapshot_now().leased_bytes, 0);
}

struct OncePanic(AtomicBool);
impl Wake for OncePanic {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            !self.0.swap(false, Ordering::SeqCst),
            "actual Notify waker panic"
        );
    }
}

#[tokio::test]
async fn push_notify_panic_keeps_actual_committed_chunk_and_sticky_quarantine() {
    let q = AsyncLeasedByteFlowQueue::new_custodied(8).unwrap();
    let waker = Waker::from(Arc::new(OncePanic(AtomicBool::new(true))));
    let mut cx = Context::from_waker(&waker);
    let mut held = None;
    let mut receiver = pin!(q.recv_custodied(8, &mut held));
    assert!(receiver.as_mut().poll(&mut cx).is_pending());
    let mut pending = Some(vec![3; 4]);
    let mut push = pin!(q.push_custodied(&mut pending));
    let panic =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| push.as_mut().poll(&mut cx)));
    assert!(panic.is_err());
    drop(push);
    assert_eq!(q.snapshot_now().queued_bytes, 4);
    assert!(matches!(
        q.dispose_custodied(),
        Err(ByteCustodyError::Invalid)
    ));
    assert!(!q.physical_zero());
}

#[tokio::test]
async fn actual_release_notify_panic_releases_once_but_retains_quarantine() {
    let q = AsyncLeasedByteFlowQueue::new_custodied(4).unwrap();
    q.push_custodied(&mut Some(vec![1; 4])).await.unwrap();
    let mut held = None;
    q.recv_custodied(4, &mut held).await.unwrap();
    let waker = Waker::from(Arc::new(OncePanic(AtomicBool::new(true))));
    let mut cx = Context::from_waker(&waker);
    let mut waiter = pin!(q.wait_read_len(1));
    assert!(waiter.as_mut().poll(&mut cx).is_pending());
    let permit = &mut held.as_mut().unwrap().permit;
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| permit.release(4))).is_err());
    assert_eq!(permit.remaining_bytes(), 0);
    assert_eq!(permit.release(4), 0);
    assert_eq!(q.snapshot_now().leased_bytes, 0);
    assert!(matches!(
        q.dispose_custodied(),
        Err(ByteCustodyError::Invalid)
    ));
    assert!(!q.physical_zero());
}

#[tokio::test]
async fn cancelled_wait_and_corrupt_stored_count_keep_actual_held_custody() {
    let q = AsyncLeasedByteFlowQueue::new_custodied(8).unwrap();
    let waker = Waker::from(Arc::new(OncePanic(AtomicBool::new(false))));
    let mut cx = Context::from_waker(&waker);
    let mut held = None;
    {
        let mut wait = pin!(q.recv_custodied(8, &mut held));
        assert!(wait.as_mut().poll(&mut cx).is_pending());
    }
    assert!(held.is_none());
    q.push_custodied(&mut Some(vec![8; 4])).await.unwrap();
    q.inner.lock_state().queued_bytes = 8;
    assert_eq!(
        q.recv_custodied(8, &mut held).await,
        Err(ByteCustodyError::Invalid)
    );
    assert_eq!(held.as_ref().unwrap().data.bytes.as_ref(), &[8; 4]);
    assert_eq!(held.as_ref().unwrap().permit.remaining_bytes(), 4);
    assert!(matches!(
        q.dispose_custodied(),
        Err(ByteCustodyError::Invalid)
    ));
    assert!(!q.physical_zero());
}

#[tokio::test]
async fn legacy_mutators_and_unknown_backing_never_produce_known_zero() {
    let q = AsyncLeasedByteFlowQueue::new_custodied(8).unwrap();
    q.push(Bytes::from_static(b"opaque")).await.unwrap();
    assert!(matches!(
        q.dispose_custodied(),
        Err(ByteCustodyError::Invalid)
    ));
    assert_eq!(q.snapshot_now().queued_bytes, 6);
    assert!(!q.physical_zero());
    let q = AsyncLeasedByteFlowQueue::new_custodied(8).unwrap();
    let reservation = q.reserve_read_up_to(4).await.unwrap();
    q.close().await;
    let mut held = None;
    assert_eq!(
        q.recv_custodied(8, &mut held).await,
        Err(ByteCustodyError::Invalid)
    );
    assert_eq!(q.snapshot_now().reserved_bytes, 4);
    drop(reservation);
    assert_eq!(q.snapshot_now().reserved_bytes, 0);
    assert!(!q.physical_zero());
}

#[tokio::test]
async fn metadata_and_final_audit_precede_physical_zero_on_exact_disposal_turn() {
    let q = AsyncLeasedByteFlowQueue::new_custodied(32).unwrap();
    for _ in 0..32 {
        q.push_custodied(&mut Some(vec![1])).await.unwrap();
    }
    assert!(matches!(q.dispose_custodied(), Ok(None)));
    assert_eq!(q.snapshot_now().queued_bytes, 0);
    assert!(!q.physical_zero());
    assert!(matches!(q.dispose_custodied(), Ok(Some(_))));
    assert!(q.physical_zero());
}

#[tokio::test]
async fn stored_zero_with_original_chunk_never_publishes_eof_or_disposes() {
    let q = AsyncLeasedByteFlowQueue::new_custodied(8).unwrap();
    q.push_custodied(&mut Some(vec![2; 4])).await.unwrap();
    q.inner.lock_state().queued_bytes = 0;
    q.close_for_remote_eof_and_mark_ready().await;
    let mut held = None;
    assert_eq!(
        q.recv_custodied(8, &mut held).await,
        Err(ByteCustodyError::Invalid)
    );
    assert!(held.is_none());
    assert_eq!(q.snapshot_now().chunks, 1);
    assert!(matches!(
        q.dispose_custodied(),
        Err(ByteCustodyError::Invalid)
    ));
    assert!(!q.physical_zero());
}

// Await only the original capacity Notify. Legacy queue APIs would invalidate
// the custodied ledger before the Service's actual release under test.
impl AsyncLeasedByteFlowQueue {
    pub(crate) async fn test_original_capacity_notify(self) {
        self.inner.not_full.notified().await;
    }
}

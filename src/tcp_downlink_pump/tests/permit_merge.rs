//! Aggregation moves the original queue capability; it never refunds credit.
use super::*;
use crate::tcp_egress::TcpEgressLease;

#[tokio::test]
async fn same_queue_merge_retains_exact_credit_until_actual_release() {
    let q = AsyncLeasedByteFlowQueue::new_with_release_mode(
        16,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    q.push(Bytes::from_static(b"abcdefghijklmnop"))
        .await
        .unwrap();
    let (data, mut a) = q.recv_up_to(16).await.unwrap().into_parts();
    let mut b = a.split_to(7).unwrap();
    let before = q.snapshot_now();
    assert_eq!(b.transfer_to(&mut a, 7), Some(7));
    assert_eq!(b.remaining_bytes(), 0);
    drop(b);
    assert_eq!(a.remaining_bytes(), 16);
    assert_eq!(
        q.snapshot_now(),
        before,
        "aggregation is neither release nor progress"
    );
    let mut part = a.split_to(5).unwrap();
    assert_eq!(part.release(3), 3);
    assert_eq!(q.snapshot_now().leased_bytes, 13);
    assert_eq!(part.transfer_to(&mut a, 2), Some(2));
    drop(part);
    assert_eq!(a.remaining_bytes(), 13);
    assert_eq!(q.snapshot_now().leased_bytes, 13);
    drop(a);
    assert_eq!(q.snapshot_now().leased_bytes, 0);
    assert_eq!(data.as_ref(), b"abcdefghijklmnop");
}

#[tokio::test]
async fn foreign_merge_returns_original_capability_without_releasing_either_queue() {
    let a = AsyncLeasedByteFlowQueue::new_with_release_mode(
        4,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let b = AsyncLeasedByteFlowQueue::new_with_release_mode(
        4,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    a.push(Bytes::from_static(b"abcd")).await.unwrap();
    b.push(Bytes::from_static(b"efgh")).await.unwrap();
    let (_, mut left) = a.recv_up_to(4).await.unwrap().into_parts();
    let (_, mut right) = b.recv_up_to(4).await.unwrap().into_parts();
    assert_eq!(right.transfer_to(&mut left, 4), None);
    assert_eq!((left.remaining_bytes(), right.remaining_bytes()), (4, 4));
    assert_eq!(
        (a.snapshot_now().leased_bytes, b.snapshot_now().leased_bytes),
        (4, 4)
    );
    drop(left);
    assert_eq!(
        (a.snapshot_now().leased_bytes, b.snapshot_now().leased_bytes),
        (0, 4)
    );
    drop(right);
    assert_eq!(b.snapshot_now().leased_bytes, 0);
}

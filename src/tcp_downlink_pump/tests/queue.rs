use super::*;

#[tokio::test]
async fn custody_partial_backing_and_coalesced_batch_have_distinct_domains() {
    let q = AsyncByteBoundedFlowQueue::new(16).unwrap();
    let mut first = Vec::with_capacity(24);
    first.extend_from_slice(&[1; 8]);
    q.push_custodied(&mut Some(first)).await.unwrap();
    q.push_custodied(&mut Some(vec![2; 8])).await.unwrap();
    let mut held = None;
    assert!(q.recv_custodied(4, &mut held).await.unwrap());
    let batch = held.take().unwrap();
    assert_eq!(batch.bytes.as_ref(), &[1; 4]);
    assert!(batch.allocation >= 4);
    let receipt = q.dispose_custodied().unwrap().unwrap();
    assert_eq!(
        (
            receipt.sources,
            receipt.emitted,
            receipt.batches,
            receipt.consumed
        ),
        (2, 16, 1, 4)
    );
    assert_eq!(
        (
            receipt.discarded_chunks,
            receipt.discarded_bytes,
            receipt.discarded_allocation
        ),
        (2, 12, 32)
    );
    assert!(receipt.metadata_slots >= 2 && receipt.metadata_bytes > 0);

    let q = AsyncByteBoundedFlowQueue::new(16).unwrap();
    q.push_custodied(&mut Some(vec![1; 4])).await.unwrap();
    q.push_custodied(&mut Some(vec![2; 4])).await.unwrap();
    assert!(q.recv_custodied(8, &mut held).await.unwrap());
    let batch = held.take().unwrap();
    assert_eq!(batch.bytes.as_ref(), &[1, 1, 1, 1, 2, 2, 2, 2]);
    assert!(batch.allocation >= 8);
    let receipt = q.dispose_custodied().unwrap().unwrap();
    assert_eq!(
        (
            receipt.sources,
            receipt.batches,
            receipt.emitted,
            receipt.consumed
        ),
        (2, 1, 8, 8)
    );
    let q = AsyncByteBoundedFlowQueue::new(8).unwrap();
    q.push_custodied(&mut Some(vec![3; 4])).await.unwrap();
    q.inner.state.lock().await.queue.queued_bytes = 8;
    let result = q.recv_custodied(8, &mut held).await;
    assert_eq!(result, Err(ByteCustodyError::Invalid));
    assert_eq!(held.unwrap().bytes.len(), 4);
    assert!(matches!(
        q.dispose_custodied(),
        Err(ByteCustodyError::Invalid)
    ));
}

#[tokio::test]
async fn custody_rejection_preserves_original_pending_and_bounded_disposal() {
    let q = AsyncByteBoundedFlowQueue::new(65).unwrap();
    for _ in 0..65 {
        q.push_custodied(&mut Some(vec![7])).await.unwrap();
    }
    let mut pending = Some(vec![9]);
    let identity = pending.as_ref().unwrap().as_ptr();
    assert_eq!(
        q.push_custodied(&mut pending).await,
        Err(ByteCustodyError::Rejected)
    );
    assert_eq!(pending.as_ref().unwrap().as_ptr(), identity);
    q.close().await;
    assert_eq!(
        q.push_custodied(&mut pending).await,
        Err(ByteCustodyError::Rejected)
    );
    assert_eq!(pending.as_ref().unwrap().as_ptr(), identity);
    assert!(q.dispose_custodied().unwrap().is_none());
    assert_eq!(q.snapshot().await.queued_bytes, 33);
    assert!(q.dispose_custodied().unwrap().is_none());
    assert_eq!(q.snapshot().await.queued_bytes, 1);
    let receipt = q.dispose_custodied().unwrap().unwrap();
    assert_eq!(
        (receipt.discarded_chunks, receipt.discarded_bytes),
        (65, 65)
    );
    assert_eq!(q.snapshot().await.chunks, 0);
}

#[tokio::test]
async fn custody_opaque_storage_is_sticky_unknown_and_is_never_disposed() {
    let q = AsyncByteBoundedFlowQueue::new(8).unwrap();
    q.try_push(Bytes::from_static(b"opaque")).await.unwrap();
    assert!(matches!(
        q.dispose_custodied(),
        Err(ByteCustodyError::Invalid)
    ));
    assert!(matches!(
        q.dispose_custodied(),
        Err(ByteCustodyError::Invalid)
    ));
    assert_eq!(q.snapshot().await.queued_bytes, 6);
    assert_eq!(
        q.push_custodied(&mut Some(vec![1])).await,
        Err(ByteCustodyError::Invalid)
    );
}

const ONE_HUNDRED_MBIT_CHUNK_BYTES: usize = 64 * 1024;
const ONE_HUNDRED_MBIT_CADENCE: Duration = Duration::from_millis(5);
const THIRTY_SECONDS_AT_100M_STEPS: usize = 30_000 / 5;

#[test]
fn byte_bounded_flow_queue_enforces_byte_capacity() {
    let mut queue = ByteBoundedFlowQueue::new(128 * 1024).unwrap();

    queue.try_push(Bytes::from(vec![1; 64 * 1024])).unwrap();
    queue.try_push(Bytes::from(vec![2; 64 * 1024])).unwrap();

    assert_eq!(queue.snapshot().queued_bytes, 128 * 1024);
    assert_eq!(
        queue.try_push(Bytes::from(vec![3; 1])),
        Err(ByteQueuePushError::Full {
            len: 1,
            available: 0
        })
    );

    let drained = queue.pop_up_to(96 * 1024).unwrap();
    assert_eq!(drained.len(), 96 * 1024);
    assert_eq!(queue.snapshot().queued_bytes, 32 * 1024);

    queue.try_push(Bytes::from(vec![4; 64 * 1024])).unwrap();
    assert_eq!(queue.snapshot().queued_bytes, 96 * 1024);
}

#[test]
fn continuous_pump_model_sustains_100mbit_for_30s_without_capacity_read_gaps() {
    let mut queue = ByteBoundedFlowQueue::new(4 * 1024 * 1024).unwrap();
    let mut probe = ContinuousPumpProbe::default();
    let start = Instant::now();

    for step in 0..THIRTY_SECONDS_AT_100M_STEPS {
        let now = start + ONE_HUNDRED_MBIT_CADENCE.saturating_mul(step as u32);
        let before = queue.snapshot();
        assert!(
            queue.can_accept(ONE_HUNDRED_MBIT_CHUNK_BYTES),
            "100M equivalent reader should have queue capacity at step {step}: {before:?}"
        );
        probe.note_read_armed(before, ONE_HUNDRED_MBIT_CHUNK_BYTES);
        queue
            .try_push(Bytes::from(vec![0; ONE_HUNDRED_MBIT_CHUNK_BYTES]))
            .unwrap();
        probe.note_remote_read(now, ONE_HUNDRED_MBIT_CHUNK_BYTES, true);

        let drained = queue.pop_up_to(ONE_HUNDRED_MBIT_CHUNK_BYTES).unwrap();
        probe.note_drain(drained.len(), queue.snapshot());
    }

    let stats = probe.stats();
    assert_eq!(
        stats.remote_read_bytes,
        (THIRTY_SECONDS_AT_100M_STEPS * ONE_HUNDRED_MBIT_CHUNK_BYTES) as u64
    );
    assert_eq!(stats.remote_read_bytes, stats.drained_bytes);
    assert_eq!(queue.snapshot().queued_bytes, 0);
    assert_eq!(stats.queue_full_waits, 0);
    assert!(
        stats.max_remote_read_gap_with_capacity <= ONE_HUNDRED_MBIT_CADENCE,
        "capacity read gap must stay at cadence, got {:?}",
        stats.max_remote_read_gap_with_capacity
    );
}

#[test]
fn continuous_pump_absorbs_short_drain_stall_without_remote_read_gap() {
    let mut queue = ByteBoundedFlowQueue::new(4 * 1024 * 1024).unwrap();
    let mut probe = ContinuousPumpProbe::default();
    let start = Instant::now();
    let stall_start = 5_000 / 5;
    let stall_end = stall_start + 200 / 5;

    for step in 0..THIRTY_SECONDS_AT_100M_STEPS {
        let now = start + ONE_HUNDRED_MBIT_CADENCE.saturating_mul(step as u32);
        let before = queue.snapshot();
        assert!(
            queue.can_accept(ONE_HUNDRED_MBIT_CHUNK_BYTES),
            "4MiB queue should absorb a 200ms drain stall at step {step}: {before:?}"
        );
        probe.note_read_armed(before, ONE_HUNDRED_MBIT_CHUNK_BYTES);
        queue
            .try_push(Bytes::from(vec![0; ONE_HUNDRED_MBIT_CHUNK_BYTES]))
            .unwrap();
        probe.note_remote_read(now, ONE_HUNDRED_MBIT_CHUNK_BYTES, true);

        if !(stall_start..stall_end).contains(&step) {
            let drain_limit = if step >= stall_end {
                256 * 1024
            } else {
                ONE_HUNDRED_MBIT_CHUNK_BYTES
            };
            if let Some(drained) = queue.pop_up_to(drain_limit) {
                probe.note_drain(drained.len(), queue.snapshot());
            }
        }
    }

    while let Some(drained) = queue.pop_up_to(256 * 1024) {
        probe.note_drain(drained.len(), queue.snapshot());
    }

    let stats = probe.stats();
    assert_eq!(stats.remote_read_bytes, stats.drained_bytes);
    assert_eq!(queue.snapshot().queued_bytes, 0);
    assert_eq!(stats.queue_full_waits, 0);
    assert!(
        stats.max_queue_bytes >= 2 * 1024 * 1024,
        "stall should exercise real queue buffering, got {}",
        stats.max_queue_bytes
    );
    assert!(
        stats.max_remote_read_gap_with_capacity <= ONE_HUNDRED_MBIT_CADENCE,
        "drain jitter must not create remote read gaps while queue has capacity: {:?}",
        stats.max_remote_read_gap_with_capacity
    );
}

#[test]
fn continuous_pump_pauses_only_at_explicit_queue_full_edge() {
    let mut queue = ByteBoundedFlowQueue::new(128 * 1024).unwrap();
    let mut probe = ContinuousPumpProbe::default();
    let start = Instant::now();

    for step in 0..4 {
        let now = start + ONE_HUNDRED_MBIT_CADENCE.saturating_mul(step);
        let before = queue.snapshot();
        if queue.can_accept(ONE_HUNDRED_MBIT_CHUNK_BYTES) {
            probe.note_read_armed(before, ONE_HUNDRED_MBIT_CHUNK_BYTES);
            queue
                .try_push(Bytes::from(vec![0; ONE_HUNDRED_MBIT_CHUNK_BYTES]))
                .unwrap();
            probe.note_remote_read(now, ONE_HUNDRED_MBIT_CHUNK_BYTES, true);
        } else {
            probe.note_queue_full_wait(before);
        }
    }

    let stats = probe.stats();
    assert_eq!(stats.remote_read_chunks, 2);
    assert_eq!(stats.queue_full_waits, 2);
    assert_eq!(stats.max_queue_bytes, 128 * 1024);
    assert!(
        stats.max_remote_read_gap_with_capacity <= ONE_HUNDRED_MBIT_CADENCE,
        "queue-full pauses must not be counted as unexplained read gaps"
    );
}

#[tokio::test]
async fn async_byte_queue_waits_until_drain_creates_capacity() {
    let queue = AsyncByteBoundedFlowQueue::new(128 * 1024).unwrap();
    queue.push(Bytes::from(vec![1; 64 * 1024])).await.unwrap();
    queue.push(Bytes::from(vec![2; 64 * 1024])).await.unwrap();

    let producer_queue = queue.clone();
    let producer = tokio::spawn(async move {
        producer_queue
            .push(Bytes::from(vec![3; 32 * 1024]))
            .await
            .unwrap();
    });

    tokio::task::yield_now().await;
    assert!(
        !producer.is_finished(),
        "producer must wait when the byte queue is full"
    );

    let drained = queue.pop_up_to(64 * 1024).await.unwrap();
    assert_eq!(drained.len(), 64 * 1024);
    producer.await.unwrap();

    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.queued_bytes, 96 * 1024);
    assert_eq!(snapshot.high_water_bytes, 128 * 1024);
}

#[tokio::test]
async fn async_byte_queue_wait_read_len_tracks_available_capacity() {
    let queue = AsyncByteBoundedFlowQueue::new(128 * 1024).unwrap();
    queue.push(Bytes::from(vec![1; 128 * 1024])).await.unwrap();

    let waiter_queue = queue.clone();
    let waiter = tokio::spawn(async move { waiter_queue.wait_read_len(64 * 1024).await });

    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "remote reader must not arm another read while the byte queue is full"
    );

    let drained = queue.pop_up_to(48 * 1024).await.unwrap();
    assert_eq!(drained.len(), 48 * 1024);
    assert_eq!(waiter.await.unwrap().unwrap(), 48 * 1024);
}

#[tokio::test]
async fn async_byte_queue_recv_waits_for_producer_and_releases_capacity() {
    let queue = AsyncByteBoundedFlowQueue::new(128 * 1024).unwrap();
    let consumer_queue = queue.clone();
    let consumer = tokio::spawn(async move { consumer_queue.recv_up_to(64 * 1024).await });

    tokio::task::yield_now().await;
    assert!(
        !consumer.is_finished(),
        "consumer should wait for producer data"
    );

    queue.push(Bytes::from(vec![4; 96 * 1024])).await.unwrap();
    let consumed = consumer.await.unwrap().unwrap();
    assert_eq!(consumed.len(), 64 * 1024);
    assert_eq!(queue.snapshot().await.queued_bytes, 32 * 1024);

    queue.push(Bytes::from(vec![5; 96 * 1024])).await.unwrap();
    assert_eq!(queue.snapshot().await.queued_bytes, 128 * 1024);
}

#[tokio::test]
async fn async_byte_queue_closes_waiters_without_data_loss() {
    let consumer_queue = AsyncByteBoundedFlowQueue::new(64 * 1024).unwrap();
    consumer_queue
        .push(Bytes::from(vec![1; 16 * 1024]))
        .await
        .unwrap();

    let first = consumer_queue.recv_up_to(64 * 1024).await.unwrap();
    assert_eq!(first.len(), 16 * 1024);

    let waiting_consumer_queue = consumer_queue.clone();
    let consumer = tokio::spawn(async move { waiting_consumer_queue.recv_up_to(64 * 1024).await });

    let reader_queue = AsyncByteBoundedFlowQueue::new(64 * 1024).unwrap();
    reader_queue
        .push(Bytes::from(vec![2; 64 * 1024]))
        .await
        .unwrap();
    let waiting_reader_queue = reader_queue.clone();
    let reader = tokio::spawn(async move { reader_queue.wait_read_len(64 * 1024).await });

    tokio::task::yield_now().await;
    consumer_queue.close().await;
    waiting_reader_queue.close().await;

    assert!(consumer.await.unwrap().is_none());
    assert_eq!(reader.await.unwrap(), Err(AsyncByteQueueWait::Closed));
}

#[tokio::test]
async fn async_byte_queue_close_allows_draining_already_queued_bytes() {
    let queue = AsyncByteBoundedFlowQueue::new(64 * 1024).unwrap();
    queue.push(Bytes::from(vec![1; 48 * 1024])).await.unwrap();

    queue.close().await;

    let drained = queue.recv_up_to(64 * 1024).await.unwrap();
    assert_eq!(drained.len(), 48 * 1024);
    assert_eq!(
        queue.recv_up_to(64 * 1024).await,
        None,
        "closed queue should report EOF only after queued bytes are drained"
    );
}

#[tokio::test]
async fn async_byte_queue_close_releases_full_queue_producer() {
    let queue = AsyncByteBoundedFlowQueue::new(64 * 1024).unwrap();
    queue.push(Bytes::from(vec![1; 64 * 1024])).await.unwrap();

    let producer_queue = queue.clone();
    let producer = tokio::spawn(async move { producer_queue.push(Bytes::from(vec![2; 1])).await });

    tokio::task::yield_now().await;
    assert!(
        !producer.is_finished(),
        "producer should wait while the queue is full"
    );

    queue.close().await;
    assert_eq!(producer.await.unwrap(), Err(ByteQueuePushError::Closed));
    assert_eq!(
        queue.try_push(Bytes::from(vec![3; 1])).await,
        Err(ByteQueuePushError::Closed)
    );
    assert_eq!(queue.snapshot().await.queued_bytes, 64 * 1024);
}

#[tokio::test]
async fn leased_byte_queue_holds_capacity_until_permit_release() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    queue.push(Bytes::from(vec![1; 128 * 1024])).await.unwrap();
    assert_eq!(queue.snapshot().await.local_progress_bytes, 0);

    let mut leased = queue.recv_up_to(64 * 1024).await.unwrap();
    assert_eq!(leased.bytes().len(), 64 * 1024);
    assert_eq!(queue.snapshot().await.queued_bytes, 64 * 1024);
    assert_eq!(queue.snapshot().await.leased_bytes, 64 * 1024);
    assert_eq!(queue.snapshot().await.local_progress_bytes, 64 * 1024);
    assert_eq!(queue.snapshot().await.available_bytes(), 0);

    let producer_queue = queue.clone();
    let producer = tokio::spawn(async move { producer_queue.push(Bytes::from(vec![2; 1])).await });
    tokio::task::yield_now().await;
    assert!(
        !producer.is_finished(),
        "dispatcher pop must not release reader capacity before local admission"
    );

    assert_eq!(leased.permit_mut().release(32 * 1024), 32 * 1024);
    assert_eq!(
        queue.snapshot().await.local_progress_bytes,
        64 * 1024 + 32 * 1024
    );
    tokio::task::yield_now().await;
    assert!(
        producer.is_finished(),
        "partial local admission should release matching reader capacity"
    );
    producer.await.unwrap().unwrap();

    assert_eq!(leased.permit_mut().release(usize::MAX), 32 * 1024);
    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.leased_bytes, 0);
    assert_eq!(snapshot.queued_bytes, 64 * 1024 + 1);
    assert_eq!(snapshot.local_progress_bytes, 128 * 1024);
}

#[tokio::test]
async fn read_reservation_is_counted_before_commit_and_refunded_on_drop() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert_eq!(reservation.max_len(), 64 * 1024);
    assert_eq!(queue.snapshot().await.reserved_bytes, 64 * 1024);
    drop(reservation);
    assert_eq!(queue.snapshot().await.reserved_bytes, 0);
}

#[tokio::test]
async fn read_reservation_commit_refunds_unused_capacity() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert!(reservation.commit(Bytes::from(vec![1; 16 * 1024])).unwrap());
    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.queued_bytes, 16 * 1024);
    assert_eq!(snapshot.available_bytes(), 112 * 1024);
}

#[tokio::test]
async fn read_reservation_rejects_overcommit_and_refunds_capacity() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert_eq!(
        reservation.commit(Bytes::from(vec![1; 64 * 1024 + 1])),
        Err(ByteQueuePushError::ChunkExceedsCapacity {
            len: 64 * 1024 + 1,
            capacity: 64 * 1024,
        })
    );
    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.available_bytes(), 128 * 1024);
}

#[tokio::test]
async fn read_reservation_preserves_owned_byte_cap_across_queue_lifecycle() {
    const CAPACITY: usize = 128 * 1024;

    let queue = AsyncLeasedByteFlowQueue::new(CAPACITY).unwrap();
    let first = queue.reserve_read_up_to(96 * 1024).await.unwrap();
    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.owned_bytes(), 96 * 1024);
    assert!(snapshot.owned_bytes() <= CAPACITY);

    first.commit(Bytes::from(vec![1; 64 * 1024])).unwrap();
    let second = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.queued_bytes, 64 * 1024);
    assert_eq!(snapshot.reserved_bytes, 64 * 1024);
    assert_eq!(snapshot.owned_bytes(), CAPACITY);
    assert!(snapshot.high_water_bytes <= CAPACITY);

    drop(second);
    let mut leased = queue.recv_up_to(48 * 1024).await.unwrap();
    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.queued_bytes, 16 * 1024);
    assert_eq!(snapshot.leased_bytes, 48 * 1024);
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.owned_bytes(), 64 * 1024);
    assert!(snapshot.owned_bytes() <= CAPACITY);

    assert_eq!(leased.permit_mut().release(16 * 1024), 16 * 1024);
    assert_eq!(queue.snapshot().await.owned_bytes(), 48 * 1024);
    drop(leased);

    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.queued_bytes, 16 * 1024);
    assert_eq!(snapshot.leased_bytes, 0);
    assert_eq!(snapshot.owned_bytes(), 16 * 1024);
    assert!(snapshot.high_water_bytes <= CAPACITY);
}

#[tokio::test]
async fn read_reservation_waiter_rearms_after_capacity_release() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let first = queue.reserve_read_up_to(128 * 1024).await.unwrap();
    let waiting_queue = queue.clone();
    let waiter = tokio::spawn(async move { waiting_queue.reserve_read_up_to(64 * 1024).await });

    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(first);

    let second = tokio::time::timeout(Duration::from_millis(200), waiter)
        .await
        .expect("released reservation must wake the next reader")
        .unwrap()
        .unwrap();
    assert_eq!(second.max_len(), 64 * 1024);
}

#[tokio::test]
async fn read_reservation_closed_commit_refunds_exactly_once() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    queue
        .close_for_terminal_and_mark_ready("test", "reservation_commit_after_close")
        .await;

    assert_eq!(
        reservation.commit(Bytes::from(vec![1; 16 * 1024])),
        Err(ByteQueuePushError::Closed)
    );
    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.available_bytes(), 128 * 1024);
}

#[tokio::test]
async fn leased_queue_readiness_coalesces_until_open_empty_is_observed() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let first = queue.reserve_read_up_to(16 * 1024).await.unwrap();
    assert!(first.commit(Bytes::from(vec![1; 16 * 1024])).unwrap());
    let second = queue.reserve_read_up_to(16 * 1024).await.unwrap();
    assert!(!second.commit(Bytes::from(vec![2; 16 * 1024])).unwrap());

    let LeasedQueuePoll::Data(leased) = queue.try_recv_up_to(32 * 1024) else {
        panic!("one readiness event must expose all queued data to the actor");
    };
    assert_eq!(leased.bytes().len(), 32 * 1024);
    drop(leased);
    assert!(matches!(
        queue.try_recv_up_to(32 * 1024),
        LeasedQueuePoll::Empty
    ));

    let third = queue.reserve_read_up_to(16 * 1024).await.unwrap();
    assert!(
        third.commit(Bytes::from(vec![3; 16 * 1024])).unwrap(),
        "observing an open empty queue must rearm one future readiness wake"
    );
}

#[tokio::test]
async fn leased_queue_close_reuses_pending_wake_and_reports_closed_after_data() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let reservation = queue.reserve_read_up_to(16 * 1024).await.unwrap();
    assert!(reservation.commit(Bytes::from(vec![1; 16 * 1024])).unwrap());
    assert!(
        !queue.close_for_remote_eof_and_mark_ready().await,
        "the queued payload wake must also carry the later close state"
    );

    let LeasedQueuePoll::Data(leased) = queue.try_recv_up_to(128 * 1024) else {
        panic!("closed queue must drain its owned payload first");
    };
    drop(leased);
    assert!(matches!(
        queue.try_recv_up_to(128 * 1024),
        LeasedQueuePoll::Closed
    ));
    assert!(!queue.close_for_remote_eof_and_mark_ready().await);
}

#[tokio::test]
async fn leased_queue_closed_waits_for_reservation_refund() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert!(queue.close_for_remote_eof_and_mark_ready().await);
    assert!(matches!(
        queue.try_recv_up_to(128 * 1024),
        LeasedQueuePoll::Empty
    ));

    drop(reservation);
    assert!(matches!(
        queue.try_recv_up_to(128 * 1024),
        LeasedQueuePoll::Closed
    ));
}

#[tokio::test]
async fn leased_queue_terminal_drop_is_exact_and_idempotent() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    let reservation = queue.reserve_read_up_to(32 * 1024).await.unwrap();
    reservation.commit(Bytes::from(vec![1; 32 * 1024])).unwrap();

    assert_eq!(
        queue.close_and_drop_queued_for_terminal("test", "terminal_drop"),
        32 * 1024
    );
    let snapshot = queue.snapshot_now();
    assert!(snapshot.closure.is_closed());
    assert_eq!(
        snapshot.closure.terminal_cause(),
        Some(LeasedByteQueueTerminalCause {
            direction: "test",
            reason: "terminal_drop",
        })
    );
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.leased_bytes, 0);
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(
        queue.close_and_drop_queued_for_terminal("test", "different_reason"),
        0
    );
    assert_eq!(
        queue.snapshot_now().closure.terminal_cause(),
        Some(LeasedByteQueueTerminalCause {
            direction: "test",
            reason: "terminal_drop",
        }),
        "the first terminal cause must remain authoritative"
    );
}

#[tokio::test]
async fn d16_global_budget_counts_reservation_and_owned_bytes_across_flows() {
    let budget = D16GlobalByteBudget::new(128 * 1024).unwrap();
    let first = AsyncLeasedByteFlowQueue::new_with_global_budget(
        128 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    let second = AsyncLeasedByteFlowQueue::new_with_global_budget(
        128 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();

    let first_reservation = first.reserve_read_up_to(96 * 1024).await.unwrap();
    let second_reservation = second.reserve_read_up_to(64 * 1024).await.unwrap();
    assert_eq!(first_reservation.max_len(), 96 * 1024);
    assert_eq!(second_reservation.max_len(), 32 * 1024);
    assert_eq!(budget.snapshot().reserved_bytes, 128 * 1024);
    assert_eq!(budget.snapshot().owned_bytes, 0);

    first_reservation
        .commit(Bytes::from(vec![1; 64 * 1024]))
        .unwrap();
    second_reservation
        .commit(Bytes::from(vec![2; 32 * 1024]))
        .unwrap();
    let snapshot = budget.snapshot();
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.owned_bytes, 96 * 1024);
    assert!(snapshot.total_bytes() <= snapshot.capacity_bytes);

    let mut first_lease = first.recv_up_to(64 * 1024).await.unwrap();
    let mut second_lease = second.recv_up_to(32 * 1024).await.unwrap();
    assert_eq!(first_lease.permit_mut().release(usize::MAX), 64 * 1024);
    assert_eq!(second_lease.permit_mut().release(usize::MAX), 32 * 1024);
    let snapshot = budget.snapshot();
    assert_eq!(snapshot.total_bytes(), 0);
    assert!(snapshot.high_water_bytes <= 128 * 1024);
}

#[tokio::test]
async fn d16_global_budget_waiter_wakes_after_other_flow_releases() {
    let budget = D16GlobalByteBudget::new(128 * 1024).unwrap();
    let first = AsyncLeasedByteFlowQueue::new_with_global_budget(
        128 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    let second = AsyncLeasedByteFlowQueue::new_with_global_budget(
        128 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    let reservation = first.reserve_read_up_to(128 * 1024).await.unwrap();
    reservation
        .commit(Bytes::from(vec![3; 128 * 1024]))
        .unwrap();
    let waiting = second.clone();
    let waiter = tokio::spawn(async move { waiting.reserve_read_up_to(64 * 1024).await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());

    let mut leased = first.recv_up_to(64 * 1024).await.unwrap();
    assert_eq!(leased.permit_mut().release(64 * 1024), 64 * 1024);
    let reservation = tokio::time::timeout(Duration::from_millis(200), waiter)
        .await
        .expect("global release must wake another flow")
        .unwrap()
        .unwrap();
    assert_eq!(reservation.max_len(), 64 * 1024);
    drop(reservation);
    drop(leased);
    assert_eq!(budget.snapshot().total_bytes(), 64 * 1024);
}

#[tokio::test]
async fn d16_global_budget_accounts_direct_push_and_lease_release() {
    let budget = D16GlobalByteBudget::new(64 * 1024).unwrap();
    let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
        64 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    queue.push(Bytes::from(vec![9; 64 * 1024])).await.unwrap();
    assert_eq!(budget.snapshot().owned_bytes, 64 * 1024);

    let leased = queue.recv_up_to(64 * 1024).await.unwrap();
    drop(leased);
    assert_eq!(budget.snapshot().total_bytes(), 0);
}

#[path = "native_storage.rs"]
mod native_storage;

#[path = "permit_merge.rs"]
mod permit_merge;

#[path = "native_waiters.rs"]
pub(in crate::tcp_downlink_pump) mod native_waiters;

// Read-only cold geometry for real caller provenance tests. No new authority.
impl AsyncByteBoundedFlowQueue {
    pub(crate) fn native_waiter_layout_for_test(&self) -> Option<usize> {
        self.inner.waiters.get().map(|_| native_wait::Waiters::layout_bytes())
    }
}

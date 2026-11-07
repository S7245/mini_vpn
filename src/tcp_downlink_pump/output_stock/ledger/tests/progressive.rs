//! Explicitly funded stock: bootstrap is physical progress, not global credit.
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn progressive_cold_stock_uses_real_64k_and_only_actual_last_alias_recovers_full() {
    let budget = BackingBudget::new(1 << 20).unwrap();
    let app = BackingMeter::with_budget(&budget);
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&app);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
        4 << 20,
        &meter,
        clock.clone(),
        512 << 10,
        64 << 10,
    )
    .unwrap();
    assert_eq!(
        (meter.snapshot().allocations, meter.snapshot().payload_bytes),
        (1, 64 << 10)
    );
    assert!(
        budget.used() < 128 << 10,
        "all metadata and actual bootstrap fit without pretending all N13 are funded"
    );
    let data = BackingMeter::with_budget(&budget);
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let unrelated =
        bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &data)
            .unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(64 << 10).unwrap().unwrap();
    space.write(&vec![0x23; 64 << 10]).unwrap();
    let payload = space.finish().unwrap();
    let original = payload.allocation_identity().unwrap();
    let alias = payload.clone();
    let mut pending = Some(OwnedPayload::Native(payload));
    queue.push_stock(&producer, &mut pending).await.unwrap();
    let mut batch = None;
    assert!(queue.recv_custodied(64 << 10, &mut batch).await.unwrap());
    drop(batch);
    assert_eq!(budget.used(), budget.limit());
    assert!(producer.try_prepare(64 << 10).unwrap().is_none());
    let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut context = Context::from_waker(&waker);
    let mut waiting = Box::pin(producer.wait_prepare(64 << 10));
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    drop(alias);
    assert!(
        wakes.0.load(Ordering::SeqCst) > 0,
        "no new input, dequeue, timer or global refund can substitute for this actual last reference"
    );
    let Poll::Ready(Ok(mut space)) = waiting.as_mut().poll(&mut context) else {
        panic!()
    };
    drop(waiting);
    space.write(b"same").unwrap();
    let returned = space.finish().unwrap();
    assert_eq!(returned.allocation_identity(), Some(original));
    assert_eq!(
        budget.used(),
        budget.limit(),
        "local availability is not global refund"
    );
    drop(returned);
    drop(producer);
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop((queue, stock, unrelated, clock));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(facts.0.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn progressive_funds_original_thirteen_large_slots_and_preserves_ready_priority_and_cancel() {
    let budget = BackingBudget::new(8 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
        4 << 20,
        &meter,
        clock,
        512 << 10,
        64 << 10,
    )
    .unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut first = producer.try_prepare(512 << 10).unwrap().unwrap();
    assert_eq!(first.capacity(), 64 << 10);
    first.write(&vec![1; first.capacity()]).unwrap();
    let first = first.finish().unwrap();
    let original = first.allocation_identity();
    let cancelled = producer.try_prepare(512 << 10).unwrap().unwrap();
    assert_eq!(cancelled.capacity(), 512 << 10);
    assert_eq!(meter.snapshot().allocations, 2);
    let charged = budget.used();
    drop(cancelled);
    assert_eq!(
        budget.used(),
        charged,
        "cancelling an operation does not retire its newly funded actual slab"
    );
    let mut second = producer.try_prepare(512 << 10).unwrap().unwrap();
    second.write(&vec![2; second.capacity()]).unwrap();
    let second = second.finish().unwrap();
    drop(first);
    let mut ready = producer.try_prepare(512 << 10).unwrap().unwrap();
    assert_eq!(ready.capacity(), 64 << 10);
    assert_eq!(
        meter.snapshot().allocations,
        2,
        "already funded ready inventory wins over growth"
    );
    ready.write(&vec![3; ready.capacity()]).unwrap();
    let ready = ready.finish().unwrap();
    assert_eq!(ready.allocation_identity(), original);
    let mut held = vec![ready, second];
    while let Some(mut space) = producer.try_prepare(512 << 10).unwrap() {
        space.write(&vec![4; space.capacity()]).unwrap();
        held.push(space.finish().unwrap());
    }
    assert_eq!(held.len(), 14);
    assert_eq!(
        (meter.snapshot().allocations, meter.snapshot().payload_bytes),
        (14, 13 * (512 << 10) + (64 << 10))
    );
    {
        let root = stock.root.as_ref().unwrap();
        let state = root.lock().unwrap();
        assert_eq!((state.funded, state.busy), (14, 14));
        assert_eq!(state.funded_capacity, 13 * (512 << 10) + (64 << 10));
        assert!(root.inventory_matches(&state));
    }
    assert!(!stock.try_retire());
    drop(held);
    drop(producer);
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(facts.0.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn progressive_full_growth_preserves_exact_unfunded_inventory_and_actual_charge() {
    let budget = BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let data = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
        4 << 20,
        &meter,
        clock,
        512 << 10,
        64 << 10,
    )
    .unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(64 << 10).unwrap().unwrap();
    space.write(&vec![1; 64 << 10]).unwrap();
    let held = space.finish().unwrap();
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let unrelated =
        bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &data)
            .unwrap();
    let actual = meter.snapshot();
    for _ in 0..3 {
        assert!(producer.try_prepare(64 << 10).unwrap().is_none());
        assert_eq!(budget.used(), budget.limit());
        let after = meter.snapshot();
        assert_eq!(
            (after.allocations, after.payload_bytes, after.physical_bytes),
            (
                actual.allocations,
                actual.payload_bytes,
                actual.physical_bytes
            )
        );
        let root = stock.root.as_ref().unwrap();
        let state = root.lock().unwrap();
        assert_eq!(
            (state.funded, state.busy, state.funded_capacity),
            (1, 1, 64 << 10)
        );
        assert!(
            state.slabs[1..]
                .iter()
                .all(|slot| matches!(slot, super::super::SlabRecord::Unfunded))
        );
        assert!(root.inventory_matches(&state));
    }
    drop(unrelated);
    drop(held);
    drop(producer);
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(facts.0.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn progressive_cold_and_retirement_reject_damaged_funded_ledgers_with_original_quiet() {
    for fault in 0..4 {
        let budget = BackingBudget::new(1 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
        let (_queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
            4 << 20,
            &meter,
            clock,
            512 << 10,
            64 << 10,
        )
        .unwrap();
        {
            let mut state = stock.root.as_ref().unwrap().lock().unwrap();
            match fault {
                0 => state.funded += 1,
                1 => state.busy += 1,
                2 => state.funded_capacity += 1,
                _ => state.ready |= 1 << 1,
            }
        }
        assert!(!stock.try_retire());
        assert!(budget.quarantined());
        assert!(
            budget.used() > 64 << 10,
            "unknown inventory retains actual bootstrap and cold layout"
        );
        assert!(facts.1.load(Ordering::SeqCst) > 0);
        assert!(
            facts.0.lock().unwrap().is_empty(),
            "inventory failure does not manufacture an IO ordinal"
        );
        drop(stock);
    }
}

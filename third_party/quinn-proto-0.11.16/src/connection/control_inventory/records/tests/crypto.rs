use super::*;
fn crypto_owner(capacity: usize) -> (BackingBudget, Pool) {
    let mut config = EndpointConfig::default();
    config.receive_control_record_limit(1).unwrap();
    config.receive_stream_record_limit(1).unwrap();
    config.receive_crypto_record_limit(capacity).unwrap();
    config.receive_sent_history_limits(1, 1).unwrap();
    let budget = BackingBudget::new(Pool::layout_bytes(&config).unwrap() + 1024).unwrap();
    config.receive_backing_meter(BackingMeter::with_budget(&budget));
    let pool = Pool::prepare(&config).unwrap().unwrap();
    (budget, pool)
}
fn enqueue(queue: &mut CryptoFrames, offset: u64) {
    let mut entry = queue.prepare().unwrap();
    entry.frame.offset = offset;
    queue.push_back(entry);
}
#[test]
fn crypto_fifo_prepend_and_three_classes_conserve_all_nodes() {
    let (budget, pool) = crypto_owner(4);
    let mut pending = CryptoFrames::new(Some(&pool));
    let mut lost = pending.empty_like();
    let control = pool.reserve(0).unwrap();
    let stream = pool.reserve(1).unwrap();
    enqueue(&mut pending, 30);
    enqueue(&mut pending, 40);
    enqueue(&mut lost, 10);
    enqueue(&mut lost, 20);
    assert!(pending.prepare().is_err());
    assert_eq!(crate::tests::allocations_in(|| pending.prepend(lost)), 0);
    for offset in [10, 20, 30, 40] {
        let entry = pending.pop_front().unwrap();
        assert_eq!(entry.frame.offset, offset);
        drop(entry);
    }
    assert_eq!(pool.lock().free, [0, 0, 4]);
    drop(control);
    drop(stream);
    drop(pending);
    drop(pool);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn prepared_split_bound_handles_jumbo_and_cancellation_without_borrowing_classes() {
    let (budget, pool) = crypto_owner(5);
    let mut pending = CryptoFrames::new(Some(&pool));
    enqueue(&mut pending, 0);
    let mut split = pending.prepare_packet(65527).unwrap();
    assert_eq!(split.len(), 4);
    assert_eq!(pool.lock().free, [1, 1, 0]);
    assert!(pending.prepare_packet(1200).is_err());
    for _ in 0..4 {
        drop(split.split_ticket().unwrap());
    }
    assert_eq!(pool.lock().free, [1, 1, 4]);
    drop(split);
    drop(pending);
    drop(pool);
    assert_eq!(budget.used(), 0);
}
#[test]
fn crypto_backing_last_reference_and_dirty_unwind_retain_exact_charge() {
    for dirty in [false, true] {
        let (budget, pool) = crypto_owner(2);
        let mut queue = CryptoFrames::new(Some(&pool));
        let mut entry = queue.prepare().unwrap();
        entry.frame.data =
            bytes::BytesMut::try_copy_from_slice_metered(b"owned-flight", &pool.0.meter)
                .unwrap()
                .freeze();
        let last = entry.frame.data.clone();
        queue.push_back(entry);
        let mut entry = queue.pop_front().unwrap();
        if dirty {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                entry.begin_mutation();
                let _entry = entry;
                panic!("interrupted original CRYPTO owner");
            }));
            assert!(result.is_err());
        } else {
            drop(entry);
        }
        drop(queue);
        drop(pool);
        assert!(budget.used() >= bytes::BytesMut::metered_copy_layout_for_len(last.len()).unwrap());
        drop(last);
        assert_eq!(budget.used(), usize::from(dirty));
        assert_eq!(budget.quarantined(), dirty);
    }
}

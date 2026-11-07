use super::*;
use bytes::BackingBudget;
mod streams;

fn config(count: usize, budget: &BackingBudget) -> EndpointConfig {
    let mut config = EndpointConfig::default();
    config.receive_control_record_limit(count).unwrap();
    config.receive_sent_history_limits(1, 1).unwrap();
    config.receive_backing_meter(BackingMeter::with_budget(budget));
    config
}
fn owner(count: usize) -> (BackingBudget, Pool) {
    let mut cfg = EndpointConfig::default();
    cfg.receive_control_record_limit(count).unwrap();
    cfg.receive_sent_history_limits(1, 1).unwrap();
    let budget = BackingBudget::new(Pool::layout_bytes(&cfg).unwrap()).unwrap();
    let pool = Pool::prepare(&config(count, &budget)).unwrap().unwrap();
    (budget, pool)
}
fn value(id: u32) -> (StreamId, VarInt) {
    (StreamId::from(VarInt::from_u32(id)), VarInt::from_u32(id))
}

#[test]
fn exact_layout_refusal_and_last_owner_release() {
    let mut cfg = EndpointConfig::default();
    cfg.receive_control_record_limit(3).unwrap();
    cfg.receive_sent_history_limits(1, 1).unwrap();
    let bytes = Pool::layout_bytes(&cfg).unwrap();
    let insufficient = BackingBudget::new(bytes - 1).unwrap();
    assert!(Pool::prepare(&config(3, &insufficient)).is_err());
    assert_eq!(insufficient.used(), 0);
    assert!(!insufficient.quarantined());
    let (budget, pool) = owner(3);
    let queue = Queue::<(StreamId, VarInt)>::new(Some(&pool));
    assert_eq!(budget.used(), bytes);
    drop(pool);
    assert_eq!(budget.used(), bytes);
    drop(queue);
    assert_eq!(budget.used(), 0);
}

#[test]
fn shared_reset_stop_capacity_and_prepared_cancellation_preserve_inventory() {
    let (budget, pool) = owner(2);
    let mut resets = Queue::new(Some(&pool));
    let mut stops = Queue::new(Some(&pool));
    let reset = resets.prepare(value(0)).unwrap();
    let stop = stops
        .prepare(frame::StopSending {
            id: value(1).0,
            error_code: 2u32.into(),
        })
        .unwrap();
    assert!(resets.prepare(value(4)).is_err());
    assert!(stops.prepare(stop.value()).is_err());
    drop(reset); // Cancellation before mutation returns only the exact node.
    assert_eq!(resets.cold_stock(), Some((2, 1, 0)));
    resets.push(resets.prepare(value(4)).unwrap());
    stops.push(stop);
    assert_eq!(resets.cold_stock(), Some((2, 0, 2)));
    drop(stops);
    assert_eq!(resets.cold_stock(), Some((2, 1, 1)));
    drop(resets);
    drop(pool);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn pending_packet_loss_transfer_preserves_original_lifo_and_node_inventory() {
    let (budget, pool) = owner(3);
    let mut pending = Queue::new(Some(&pool));
    for id in [0, 4, 8] {
        pending.push(pending.prepare(value(id)).unwrap());
    }
    let mut packet = Queue::default();
    let mut encoded = pending.pop().unwrap();
    assert_eq!(encoded.value(), value(8));
    encoded.begin_mutation();
    packet.push(encoded);
    assert_eq!(pending.cold_stock(), Some((3, 0, 3)));
    pending.append(packet);
    assert_eq!(
        pending.iter().collect::<Vec<_>>(),
        [value(0), value(4), value(8)]
    );
    for id in [8, 4, 0] {
        assert_eq!(pending.pop().unwrap().value(), value(id));
    }
    assert_eq!(pending.cold_stock(), Some((3, 3, 0)));
    drop(pending);
    drop(pool);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn dirty_unwind_is_sticky_q_after_known_storage_destruction() {
    let (budget, pool) = owner(1);
    let queue = Queue::new(Some(&pool));
    let result = std::panic::catch_unwind(|| {
        let mut record = queue.prepare(value(0)).unwrap();
        record.begin_mutation();
        panic!("intentional control record interruption");
    });
    assert!(result.is_err());
    assert_eq!(queue.cold_stock(), Some((1, 1, 0)));
    assert!(budget.quarantined());
    assert!(queue.prepare(value(4)).is_err());
    drop(queue);
    drop(pool);
    assert_eq!(budget.used(), 1);
}

#[test]
fn foreign_empty_queue_cannot_adopt_another_connections_custody() {
    let (left_budget, left) = owner(1);
    let (right_budget, right) = owner(1);
    let mut pending = Queue::new(Some(&left));
    let mut sent = Queue::new(Some(&right));
    sent.push(sent.prepare(value(0)).unwrap());
    assert!(!pending.accepts(&sent));
    pending.append(sent);
    assert!(left_budget.quarantined() && right_budget.quarantined());
    assert!(pending.is_empty());
    assert_eq!(pending.cold_stock(), Some((1, 1, 0)));
    drop(pending);
    drop(left);
    drop(right);
    assert_eq!((left_budget.used(), right_budget.used()), (1, 1));
}

#[test]
fn general_only_descriptors_cover_all_history_slots_and_one_builder_without_nodes() {
    let (budget, pool) = owner(1);
    let reset = Queue::new(Some(&pool));
    let stop = Queue::new(Some(&pool));
    let mut packets = Vec::new();
    // Three spaces, each one ordinary + one control slot, plus sole builder.
    for _ in 0..7 {
        let mut packet = PacketRecords::prepare(&reset, &stop).unwrap();
        packet.get_or_create().handshake_done = true;
        packet.release_if_empty();
        assert!(packet.get().is_some());
        packets.push(packet);
    }
    assert_eq!(reset.cold_stock(), Some((1, 1, 0)));
    assert!(PacketRecords::prepare(&reset, &stop).is_err());
    assert!(!budget.quarantined());
    let mut requeue = packets.pop().unwrap();
    let metadata = requeue.take_metadata().unwrap();
    assert!(metadata.handshake_done && requeue.get().is_none());
    let mut builder = PacketRecords::prepare(&reset, &stop).unwrap();
    builder.release_if_empty();
    assert!(builder.get().is_none());
    assert!(PacketRecords::prepare(&reset, &stop).is_ok());
    drop(metadata);
    drop(requeue);
    drop(packets);
    drop(pool);
    assert!(budget.used() > 0); // Original pending owner still retains layout.
    drop(reset);
    drop(stop);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn actual_allocation_failure_at_every_cold_storage_step_preserves_original_stock() {
    for fail_at in 1..=10 {
        // Node slab, credit index, descriptor table, seven descriptor boxes.
        let budget = BackingBudget::new(1 << 20).unwrap();
        let held = budget.try_reserve(7).unwrap();
        let config = config(1, &budget);
        let mut result = None;
        crate::tests::allocator_failure_in(fail_at, || result = Some(Pool::prepare(&config)));
        assert!(
            result.unwrap().is_err(),
            "allocation {fail_at} must be fallible"
        );
        assert_eq!(budget.used(), 7);
        assert!(!budget.quarantined());
        drop(held);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn packet_preparation_rejects_mixed_authority_in_both_directions() {
    for cold_reset in [false, true] {
        let (budget, pool) = owner(1);
        let reset = Queue::new(cold_reset.then_some(&pool));
        let stop = Queue::new((!cold_reset).then_some(&pool));
        assert!(PacketRecords::prepare(&reset, &stop).is_err());
        assert!(budget.quarantined());
        drop(reset);
        drop(stop);
        drop(pool);
        assert_eq!(budget.used(), 1);
    }
}

mod crypto;

mod retire;

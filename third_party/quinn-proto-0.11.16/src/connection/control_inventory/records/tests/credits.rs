use super::*;
use crate::packet::SpaceId;

fn owner(count: usize) -> (bytes::BackingBudget, Pool) {
    let mut config = EndpointConfig::default();
    config.receive_control_record_limit(count).unwrap();
    config.receive_sent_history_limits(1, 1).unwrap();
    let budget = bytes::BackingBudget::new(Pool::layout_bytes(&config).unwrap()).unwrap();
    config.receive_backing_meter(BackingMeter::with_budget(&budget));
    (budget, Pool::prepare(&config).unwrap().unwrap())
}
fn id(value: u32) -> StreamId {
    StreamId::from(VarInt::from_u32(value * 4))
}
fn insert(set: &mut Credits, value: u32) {
    if let Some(entry) = set.prepare(id(value)).unwrap() {
        set.insert(entry).unwrap();
    }
}
fn indexed(pool: &Pool) -> usize {
    let state = pool.lock();
    let mut indexed = std::collections::BTreeSet::new();
    for (bucket, &head) in state.credit_index.0.iter().enumerate() {
        let mut next = head;
        while next != NONE {
            assert!(indexed.insert(next), "no duplicate reference/cycle");
            let node = &state.nodes[next as usize];
            let Some(Value::Credit(value)) = node.value else {
                panic!("index points at non-credit node");
            };
            assert_eq!(state.credit_index.bucket(value.key, value.id), bucket);
            assert_eq!(
                state.credit_index.find(&state.nodes, value.key, value.id),
                Some(next)
            );
            next = node.prev;
        }
    }
    let physical: std::collections::BTreeSet<_> = state
        .nodes
        .iter()
        .enumerate()
        .filter_map(|(i, node)| matches!(node.value, Some(Value::Credit(_))).then_some(i as u32))
        .collect();
    assert_eq!(indexed, physical, "both directions, including orphan nodes");
    indexed.len()
}

#[test]
fn same_id_different_packets_and_loss_duplicate_return_only_redundant_node() {
    let (budget, pool) = owner(3);
    let mut pending = Credits::new(Some(&pool), CreditKey::pending(SpaceId::Data));
    let mut first = Credits::new(Some(&pool), CreditKey::packet(0, 1));
    let mut second = Credits::new(Some(&pool), CreditKey::packet(1, 1));
    insert(&mut pending, 7);
    first.insert(pending.pop().unwrap()).unwrap();
    insert(&mut pending, 7);
    second.insert(pending.pop().unwrap()).unwrap();
    insert(&mut pending, 7);
    assert_eq!(indexed(&pool), 3);
    assert!(pending.prepare(id(7)).unwrap().is_none()); // Full but already represented.
    pending.append(first);
    pending.append(second);
    assert_eq!(
        (pending.len(), indexed(&pool), pool.lock().free[0]),
        (1, 1, 2)
    );
    drop(pending);
    assert_eq!(indexed(&pool), 0);
    drop(pool);
    assert_eq!(budget.used(), 0);
}

#[test]
fn recycled_descriptor_generation_does_not_alias_detached_old_metadata() {
    let (budget, pool) = owner(2);
    let reset = Queue::new(Some(&pool));
    let stop = Queue::new(Some(&pool));
    let mut old = PacketRecords::prepare(&reset, &stop).unwrap();
    insert(&mut old.get_or_create().max_stream_data, 3);
    let mut detached = old.take_metadata().unwrap(); // Box returns before metadata does.
    let mut new = PacketRecords::prepare(&reset, &stop).unwrap();
    insert(&mut new.get_or_create().max_stream_data, 3);
    assert_eq!(indexed(&pool), 2);
    drop(detached.max_stream_data.pop());
    assert_eq!(indexed(&pool), 1);
    assert!(new
        .get_or_create()
        .max_stream_data
        .prepare(id(3))
        .unwrap()
        .is_none());
    drop((old, detached, new, reset, stop, pool));
    assert_eq!(budget.used(), 0);
}

#[test]
fn fixed_index_collision_chains_match_sets_through_transfers_and_reuse() {
    let (budget, pool) = owner(32);
    let mut sets: [_; 3] =
        std::array::from_fn(|i| Credits::new(Some(&pool), CreditKey::packet(i, 1)));
    let mut model: [_; 3] = std::array::from_fn(|_| std::collections::BTreeSet::new());
    for step in 0..240 {
        let a = step % 3;
        let b = (a + 1) % 3;
        let value = (step % 11) as u32;
        if sets[a].len() < 8 {
            insert(&mut sets[a], value);
            model[a].insert(id(value));
        }
        if let Some(entry) = sets[a].pop() {
            let value = entry.value();
            model[a].remove(&value);
            sets[b].insert(entry).unwrap();
            model[b].insert(value);
        }
        for i in 0..3 {
            assert_eq!(
                sets[i].iter().collect::<std::collections::BTreeSet<_>>(),
                model[i]
            );
        }
        assert_eq!(indexed(&pool), model.iter().map(|s| s.len()).sum::<usize>());
    }
    drop(sets);
    assert_eq!(indexed(&pool), 0);
    drop(pool);
    assert_eq!(budget.used(), 0);
}

#[test]
fn prepared_cancel_dirty_unwind_and_foreign_insert_keep_exact_owner() {
    let (budget, pool) = owner(1);
    let set = Credits::new(Some(&pool), CreditKey::pending(SpaceId::Data));
    drop(set.prepare(id(0)).unwrap());
    assert_eq!((indexed(&pool), pool.lock().free[0]), (0, 1));
    let result = std::panic::catch_unwind(|| {
        let mut entry = set.prepare(id(0)).unwrap().unwrap();
        entry.begin_mutation();
        panic!("intentional read settlement interruption");
    });
    assert!(result.is_err() && budget.quarantined());
    drop((set, pool));
    assert_eq!(budget.used(), 1);
    let (a_budget, a_pool) = owner(1);
    let (b_budget, b_pool) = owner(1);
    let a = Credits::new(Some(&a_pool), CreditKey::pending(SpaceId::Data));
    let mut b = Credits::new(Some(&b_pool), CreditKey::pending(SpaceId::Data));
    assert!(b.insert(a.prepare(id(0)).unwrap().unwrap()).is_err());
    assert_eq!((indexed(&a_pool), indexed(&b_pool)), (0, 0));
    drop((a, b, a_pool, b_pool));
    assert_eq!((a_budget.used(), b_budget.used()), (1, 1));
}

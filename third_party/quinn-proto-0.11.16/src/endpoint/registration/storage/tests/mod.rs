use super::*;

impl Store {
    pub(in crate::endpoint::registration) fn assert_inventory(&self) -> (usize, usize, usize) {
        for (slot, meta) in self.iter() {
            let owner = ConnectionHandle(slot);
            for cid in meta.loc_cids.values().filter(|cid| !cid.is_empty()) {
                assert_eq!(self.cid_owner(cid), Some(&owner));
            }
            if let Some((remote, token)) = meta.reset_token {
                assert_eq!(self.reset_owner(remote, &token), Some(&owner));
            }
        }
        let mut resets = 0;
        match self {
            Self::Legacy { index, .. } => {
                for (cid, owner) in &index.connection_ids {
                    assert!(self
                        .get(owner.0)
                        .unwrap()
                        .loc_cids
                        .values()
                        .any(|value| value == cid));
                }
                for (remote, tokens) in &index.connection_reset_tokens.0 {
                    for (token, owner) in tokens {
                        resets += 1;
                        assert_eq!(
                            self.get(owner.0).unwrap().reset_token,
                            Some((*remote, *token))
                        );
                    }
                }
            }
            Self::Native(f) => {
                for (cid, owner) in f.data.cids.iter() {
                    assert!(self
                        .get(owner.0)
                        .unwrap()
                        .loc_cids
                        .values()
                        .any(|value| value == cid));
                }
                for ((remote, token), owner) in f.data.resets.iter() {
                    resets += 1;
                    assert_eq!(
                        self.get(owner.0).unwrap().reset_token,
                        Some((*remote, *token))
                    );
                }
                assert_eq!(self.len(), f.data.slots.iter().flatten().count());
            }
            Self::Unavailable => {}
        }
        (self.len(), self.cid_count(), resets)
    }
}

impl Store {
    pub(in crate::endpoint::registration) fn known_connections(&self) -> usize {
        if let Self::Legacy { index, connections } = self {
            let len = connections.len();
            debug_assert_eq!(len, index.connection_ids_initial.len());
            debug_assert!(len >= index.connection_reset_tokens.0.len());
            debug_assert!(len >= index.incoming_connection_remotes.len());
            debug_assert!(len >= index.outgoing_connection_remotes.len());
        }
        self.len()
    }
}

#[derive(Default)]
struct Collide;
impl std::hash::Hasher for Collide {
    fn write(&mut self, _: &[u8]) {}
    fn finish(&self) -> u64 {
        7
    }
}

#[test]
fn colliding_wrapped_table_matches_independent_map_through_reuse() {
    let mut table =
        Table::<u64, u64, 8, std::hash::BuildHasherDefault<Collide>>::new(Default::default());
    let mut model = std::collections::BTreeMap::new();
    for key in 0..8 {
        let cell = table.prepare(&key, &[]).unwrap();
        table.commit(cell, key, key + 100);
        model.insert(key, key + 100);
    }
    assert!(table.prepare(&99, &[]).is_none());
    let mut seed = 0x8ebc_6af0_9c88_c6e3u64;
    for step in 0..6000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = (seed >> 17) % 23;
        if step % 3 == 0 {
            assert_eq!(table.remove(&key), model.remove(&key));
        } else if !model.contains_key(&key) {
            if let Some(cell) = table.prepare(&key, &[]) {
                table.commit(cell, key, seed);
                model.insert(key, seed);
            } else {
                assert_eq!(model.len(), 8);
            }
        }
        assert_eq!(table.len, model.len());
        assert_eq!(table.iter().count(), model.len());
        for key in 0..23 {
            assert_eq!(table.get(&key), model.get(&key));
        }
    }
    for key in model.keys().copied().collect::<Vec<_>>() {
        assert_eq!(table.remove(&key), model.remove(&key));
    }
    assert_eq!(table.len, 0);
    let first = table.prepare(&40, &[]).unwrap();
    let second = table.prepare(&41, &[first]).unwrap();
    assert_ne!(first.index, second.index);
    assert_eq!(table.len, 0, "preparation cannot mutate the table");
    table.commit(first, 40, 1);
    table.commit(second, 41, 2);
    assert_eq!(table.get(&40), Some(&1));
    assert_eq!(table.get(&41), Some(&2));
}

#[test]
fn fixed_box_full_and_allocator_refusal_have_exact_known_disposal() {
    let layout = Store::native_layout_bytes();
    let full = bytes::BackingBudget::new(layout - 1).unwrap();
    let full_meter = BackingMeter::with_budget(&full);
    let count =
        crate::tests::allocations_in(|| assert!(!Store::new(true, Some(&full_meter)).ready()));
    assert_eq!(count, 0);
    assert_eq!(full.used(), 0);
    assert!(!full.quarantined());
    let budget = bytes::BackingBudget::new(layout).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    // Warm the actual std random hash seed before injecting the fixed Box allocation.
    let store = Store::new(true, Some(&meter));
    assert!(store.ready());
    assert_eq!(budget.used(), layout);
    let freed = crate::tests::freed_bytes_in(|| drop(store));
    assert_eq!(freed, layout);
    assert_eq!(budget.used(), 0);
    crate::tests::allocator_failure_in(1, || assert!(!Store::new(true, Some(&meter)).ready()));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn independent_count_or_reverse_route_damage_is_rejected_before_any_terminal_ack() {
    for fault in 0..5 {
        let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
        let mut config = EndpointConfig::default();
        config.receive_backing_meter(BackingMeter::with_budget(&budget));
        let mut endpoint = Endpoint::try_new(Arc::new(config), None, true, None).unwrap();
        let Store::Native(f) = &mut endpoint.registrations.store else {
            panic!("Native storage required");
        };
        match fault {
            0 => f.data.len += 1,
            1 => f.data.cids.len += 1,
            2 => f.data.resets.len += 1,
            3 => {
                let key = ConnectionId::new(&[0x94; 8]);
                let cell = f.data.cids.prepare(&key, &[]).unwrap();
                f.data.cids.commit(cell, key, ConnectionHandle(0));
            }
            _ => {
                let key = ("[::1]:4433".parse().unwrap(), ResetToken::from([0x95; 16]));
                let cell = f.data.resets.prepare(&key, &[]).unwrap();
                f.data.resets.commit(cell, key, ConnectionHandle(0));
            }
        }
        assert!(!endpoint.registrations.inventory_valid());
        let outcome = endpoint.handle_event_checked(ConnectionHandle(0), EndpointEvent::drained());
        assert_eq!(outcome.disposition, EndpointEventDisposition::Rejected);
        assert!(!outcome.terminal);
        assert!(budget.quarantined() && budget.used() > 1);
        drop(outcome);
        drop(endpoint);
        assert_eq!(budget.used(), 1);
    }
}

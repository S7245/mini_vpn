use super::*;
use crate::tests::{allocations_in, allocator_failure_in};

#[test]
fn exact_slabs_cancel_and_each_real_allocator_refusal_refunds_known_layouts() {
    let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let exact = Layout::array::<Server>(3).unwrap().size()
        + Layout::array::<Option<Bytes>>(6).unwrap().size();
    assert_eq!(Cold::layout_bytes(3, 2).unwrap(), exact);
    let mut candidate = None;
    let allocations = allocations_in(|| candidate = Some(Cold::prepare(3, 2, &meter).unwrap()));
    assert_eq!(allocations, 2);
    assert_eq!(budget.used(), exact);
    drop(candidate);
    assert_eq!(budget.used(), 0, "unpublished candidate cancellation");
    for failed in 1..=2 {
        allocator_failure_in(failed, || {
            assert_eq!(
                Cold::prepare(3, 2, &meter).unwrap_err(),
                TokenStoreError::Capacity(MeteredCopyError::Allocation)
            );
        });
        assert_eq!(budget.used(), 0);
    }
    assert!(!budget.quarantined());
}

#[test]
fn dirty_mutation_quarantines_same_owner_without_refunding_to_clean_zero() {
    let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut cache = Cold::prepare(3, 2, &meter).unwrap();
    let held = budget.used();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _mutation = Mutation::begin(&meter);
        cache.servers.values[0].count = 1; // interrupted real owner's mutation
        panic!("explicit dirty cache interruption");
    }));
    assert!(panic.is_err());
    assert!(budget.quarantined());
    assert_eq!(budget.used(), held);
    assert_eq!(
        cache.take("localhost"),
        Err(TokenStoreError::Capacity(MeteredCopyError::Quarantined))
    );
    drop(cache);
    assert!(budget.used() > 0);
}

fn assert_inventory(cache: &Cold, budget: &bytes::BackingBudget) {
    let mut linked = std::collections::HashSet::new();
    let mut cursor = cache.oldest;
    let mut previous = None;
    while let Some(index) = cursor {
        assert!(index < cache.servers.values.len());
        assert!(linked.insert(index), "LRU cycle/duplicate");
        let server = &cache.servers.values[index];
        assert!(server.name.is_some());
        assert_eq!(server.previous, previous);
        previous = Some(index);
        cursor = server.next;
    }
    assert_eq!(previous, cache.newest);
    let mut names = std::collections::HashSet::new();
    let mut tokens = 0;
    let mut payload = 0;
    let mut physical = 0;
    for (index, server) in cache.servers.values.iter().enumerate() {
        let segment =
            &cache.tokens.values[index * cache.per_server..(index + 1) * cache.per_server];
        if let Some(name) = &server.name {
            assert!(linked.contains(&index));
            assert!(names.insert(name.as_ref()), "duplicate name authority");
            assert!(server.count > 0 && server.count <= cache.per_server);
            assert!(server.first < cache.per_server);
            payload += name.len();
            physical += BytesMut::metered_copy_layout_for_len(name.len()).unwrap();
            for step in 0..cache.per_server {
                let position = (server.first + step) % cache.per_server;
                assert_eq!(segment[position].is_some(), step < server.count);
            }
        } else {
            assert!(!linked.contains(&index));
            assert_eq!(server.count, 0);
            assert_eq!(server.first, 0);
            assert_eq!((server.previous, server.next), (None, None));
            assert!(segment.iter().all(Option::is_none));
        }
        for token in segment.iter().flatten() {
            assert!(token.backing_meter().unwrap().same_authority(&cache.meter));
            payload += token.len();
            physical += BytesMut::metered_copy_layout_for_len(token.len()).unwrap();
            tokens += 1;
        }
    }
    assert_eq!(names.len(), linked.len());
    assert_eq!(
        tokens,
        cache.servers.values.iter().map(|s| s.count).sum::<usize>()
    );
    assert_eq!(cache.meter.snapshot().payload_bytes, payload);
    assert_eq!(cache.meter.snapshot().physical_bytes, physical);
    let layouts = Layout::array::<Server>(cache.servers.values.len())
        .unwrap()
        .size()
        + Layout::array::<Option<Bytes>>(cache.tokens.values.len())
            .unwrap()
            .size();
    assert_eq!(budget.used(), layouts + physical);
}

#[test]
fn lru_names_fifo_slots_and_independent_byte_sums_agree_after_every_transition() {
    use rand::{RngExt, SeedableRng};
    let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut cache = Cold::prepare(4, 3, &meter).unwrap();
    let mut rng = rand_pcg::Pcg32::seed_from_u64(0x137a_5b09);
    assert_inventory(&cache, &budget);
    for value in 0u64..1500 {
        let name = format!("server-{}", rng.random::<u8>() % 9);
        if rng.random_bool(0.6) {
            let token = BytesMut::try_copy_from_slice_metered(&value.to_be_bytes(), &meter)
                .unwrap()
                .freeze();
            cache.insert(&name, token).unwrap();
        } else {
            drop(cache.take(&name).unwrap());
        }
        assert_inventory(&cache, &budget);
    }
    drop(cache);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

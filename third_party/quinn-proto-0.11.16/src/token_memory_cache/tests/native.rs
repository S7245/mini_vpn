//! Hard cache metadata and exact one-time backing transfer.
use super::*;

#[test]
fn metadata_is_precharged_and_token_backing_moves_once() {
    let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let token = bytes::BytesMut::try_copy_from_slice_metered(b"opaque-token", &meter)
        .unwrap()
        .freeze();
    let ptr = token.as_ptr();
    let payload_charge = budget.used();
    let cache = TokenMemoryCache::default();
    cache.insert("localhost", token);
    assert!(
        budget.used() > payload_charge,
        "cache metadata must be charged before its physical allocations"
    );
    let taken = cache.take("localhost").unwrap();
    assert_eq!(taken.as_ptr(), ptr);
    assert!(cache.take("localhost").is_none());
    drop(taken);
    assert!(
        budget.used() > 0,
        "empty cache still owns fixed physical metadata"
    );
    drop(cache);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

fn token(value: u32, meter: &bytes::BackingMeter) -> Bytes {
    bytes::BytesMut::try_copy_from_slice_metered(&value.to_be_bytes(), meter)
        .unwrap()
        .freeze()
}

#[test]
fn hard_cache_matches_original_lru_fifo_with_eviction_and_empty_slot_reuse() {
    let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let hard = TokenMemoryCache::new(3, 2);
    let legacy = TokenMemoryCache::new(3, 2);
    let mut rng = new_rng();
    for value in 0u32..2000 {
        let name = (rng.random::<u32>() % 9).to_string();
        if rng.random_bool(0.65) {
            legacy.insert(&name, Bytes::copy_from_slice(&value.to_be_bytes()));
            hard.insert_metered(&name, token(value, &meter), &meter)
                .unwrap();
        } else {
            assert_eq!(
                hard.take_metered(&name, &meter).unwrap(),
                legacy.take(&name)
            );
        }
    }
    drop(hard);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn full_name_preparation_preserves_original_entry_and_fifo() {
    let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let cache = TokenMemoryCache::new(1, 2);
    cache
        .insert_metered("old", token(1, &meter), &meter)
        .unwrap();
    cache
        .insert_metered("old", token(2, &meter), &meter)
        .unwrap();
    let incoming = token(3, &meter);
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let exact = budget.used();
    assert_eq!(
        cache.insert_metered("new", incoming.clone(), &meter),
        Err(TokenStoreError::Capacity(
            bytes::MeteredCopyError::BudgetFull
        ))
    );
    assert_eq!(budget.used(), exact);
    assert!(!budget.quarantined());
    assert_eq!(cache.take_metered("new", &meter).unwrap(), None);
    // Same-name operation needs no new name or token queue allocation even at Full.
    cache.insert_metered("old", incoming, &meter).unwrap();
    assert_eq!(
        cache.take_metered("old", &meter).unwrap().unwrap().as_ref(),
        &2u32.to_be_bytes()
    );
    assert_eq!(
        cache.take_metered("old", &meter).unwrap().unwrap().as_ref(),
        &3u32.to_be_bytes()
    );
    assert!(cache.take_metered("old", &meter).unwrap().is_none());
    drop(held);
    drop(cache);
    assert_eq!(budget.used(), 0);
}

#[test]
fn unsupported_foreign_and_existing_legacy_refuse_without_consuming_tokens() {
    let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let other_budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let other = bytes::BackingMeter::with_budget(&other_budget);
    let cache = TokenMemoryCache::new(1, 2);
    cache
        .insert_metered("old", token(1, &meter), &meter)
        .unwrap();
    assert_eq!(
        cache.take_metered("old", &other),
        Err(TokenStoreError::ForeignAuthority)
    );
    assert_eq!(
        cache.insert_metered("new", token(2, &other), &meter),
        Err(TokenStoreError::ForeignAuthority)
    );
    assert_eq!(
        cache.take_metered("old", &meter).unwrap().unwrap().as_ref(),
        &1u32.to_be_bytes()
    );
    let legacy = TokenMemoryCache::default();
    legacy.insert("old", Bytes::from_static(b"legacy"));
    assert_eq!(
        legacy.take_metered("old", &meter),
        Err(TokenStoreError::OccupiedLegacy)
    );
    assert_eq!(legacy.take("old"), Some(Bytes::from_static(b"legacy")));
    for cache in [TokenMemoryCache::new(0, 2), TokenMemoryCache::new(2, 0)] {
        assert_eq!(
            cache.take_metered("old", &meter),
            Err(TokenStoreError::Unsupported)
        );
    }
    drop(cache);
    assert_eq!(budget.used(), 0);
    assert_eq!(other_budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(!other_budget.quarantined());
}

#[test]
fn first_insert_allocation_refusal_keeps_cache_unbound_and_old_source_known() {
    use crate::tests::allocator_failure_in;
    let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    for failed in 1..=4 {
        let cache = TokenMemoryCache::new(3, 2);
        // macOS first lock initializes a platform Mutex with an infallible Box.
        // The fault scope below covers only the four fallible cache allocations.
        assert!(cache.take_metered("old", &meter).unwrap().is_none());
        let original = token(1, &meter);
        let baseline = budget.used();
        let mut result = None;
        allocator_failure_in(failed, || {
            result = Some(cache.insert_metered("old", original.clone(), &meter));
        });
        assert_eq!(
            result.unwrap(),
            Err(TokenStoreError::Capacity(
                bytes::MeteredCopyError::Allocation
            ))
        );

        assert_eq!(budget.used(), baseline);
        assert!(cache.take_metered("old", &meter).unwrap().is_none());
        cache.insert_metered("old", original, &meter).unwrap();
        drop(cache);
        assert_eq!(budget.used(), 0);
    }
    assert!(!budget.quarantined());
}

#[test]
fn empty_legacy_transition_releases_actual_retained_layouts_before_hard_mode() {
    let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let cache = TokenMemoryCache::new(3, 2);
    cache.insert("old", Bytes::from_static(b"legacy"));
    assert_eq!(cache.take("old"), Some(Bytes::from_static(b"legacy")));
    // Logical empty alone leaves HashMap and LruSlab allocations behind.
    let released = crate::tests::freed_bytes_in(|| {
        assert!(cache.take_metered("old", &meter).unwrap().is_none());
    });
    assert!(
        released > 0,
        "hard mode must physically destroy empty legacy layouts"
    );
    cache
        .insert_metered("new", token(1, &meter), &meter)
        .unwrap();
    assert_eq!(
        cache.take_metered("new", &meter).unwrap().unwrap().as_ref(),
        &1u32.to_be_bytes()
    );
    drop(cache);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn real_cache_mutex_poison_rejects_all_later_entry_and_retains_same_authority_q() {
    let budget = bytes::BackingBudget::new(1024 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let cache = TokenMemoryCache::new(3, 2);
    cache
        .insert_metered("old", token(1, &meter), &meter)
        .unwrap();
    let before = budget.used();
    let interruption = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _held_actual_owner = cache.0.lock().unwrap();
        panic!("explicit interruption while owning the actual cache mutex");
    }));
    assert!(interruption.is_err());
    let error = TokenStoreError::Capacity(bytes::MeteredCopyError::Quarantined);
    assert_eq!(cache.take_metered("old", &meter), Err(error));
    assert!(meter.is_quarantined());
    assert!(budget.quarantined());
    assert_eq!(budget.used(), before);
    assert!(cache.take("old").is_none());
    assert_eq!(
        cache.insert_metered("old", Bytes::from_static(b"foreign"), &meter),
        Err(error)
    );
    drop(cache);
    assert!(
        budget.used() > 0,
        "uncertain authority never normalizes to clean zero"
    );
}

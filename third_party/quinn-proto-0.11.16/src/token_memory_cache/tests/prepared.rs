use super::*;
use bytes::{BackingBudget, BackingMeter, BytesMut};
fn token(value: u8, meter: &BackingMeter) -> Bytes {
    BytesMut::try_copy_from_slice_metered(&[value], meter)
        .unwrap()
        .freeze()
}
#[test]
fn cancelled_preparation_keeps_cache_unbound_and_full_keeps_old_fifo() {
    let budget = BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let cache = Arc::new(TokenMemoryCache::new(1, 2));
    let plan = cache.clone().prepare_insert_metered("a", &meter).unwrap();
    assert!(matches!(&*cache.lock().unwrap(), State::Empty { .. }));
    drop(plan);
    assert_eq!(budget.used(), 0);
    cache.insert_metered("a", token(1, &meter), &meter).unwrap();
    let used = budget.used();
    let held = budget.try_reserve(budget.limit() - used).unwrap();
    assert!(matches!(
        cache.clone().prepare_insert_metered("b", &meter),
        Err(TokenStoreError::Capacity(
            bytes::MeteredCopyError::BudgetFull
        ))
    ));
    assert_eq!(budget.used(), budget.limit());
    assert_eq!(
        cache.take_metered("a", &meter).unwrap().unwrap().as_ref(),
        &[1]
    );
    drop((held, cache));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn prepared_tokens_rebase_after_concurrent_initialization_eviction_and_take() {
    let budget = BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let cache = Arc::new(TokenMemoryCache::new(1, 2));
    let mut first = cache.clone().prepare_insert_metered("a", &meter).unwrap();
    let mut second = cache.clone().prepare_insert_metered("b", &meter).unwrap();
    let inputs: Vec<_> = (1..=5).map(|n| token(n, &meter)).collect();
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    second.insert(inputs[0].clone()).unwrap();
    first.insert(inputs[1].clone()).unwrap(); // original candidate rebases on b's slots
    assert!(cache.take_metered("b", &meter).unwrap().is_none());
    assert_eq!(cache.take_metered("a", &meter).unwrap().unwrap(), inputs[1]);
    second.insert(inputs[2].clone()).unwrap();
    second.insert(inputs[3].clone()).unwrap();
    second.insert(inputs[4].clone()).unwrap();
    assert_eq!(cache.take_metered("b", &meter).unwrap().unwrap(), inputs[3]);
    assert_eq!(cache.take_metered("b", &meter).unwrap().unwrap(), inputs[4]);
    assert!(cache.take_metered("b", &meter).unwrap().is_none());
    drop((first, second, held, inputs, cache));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn prepared_existing_name_survives_eviction_without_late_allocation() {
    let budget = BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let cache = Arc::new(TokenMemoryCache::new(1, 2));
    cache.insert_metered("a", token(1, &meter), &meter).unwrap();
    let mut plan = cache.clone().prepare_insert_metered("a", &meter).unwrap();
    cache.insert_metered("b", token(2, &meter), &meter).unwrap();
    let incoming = token(3, &meter);
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    plan.insert(incoming.clone()).unwrap();
    assert_eq!(cache.take_metered("a", &meter).unwrap(), Some(incoming));
    assert!(cache.take_metered("b", &meter).unwrap().is_none());
    drop((plan, held, cache));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

use super::super::*;

#[test]
fn pure_registry_token_drop_does_not_clear_live_carrier_pressure() {
    let pressure = Arc::new(TcpWritePressure::new(Instant::now()));
    let (retained, mut metrics) = RetainedPressure::split(pressure.try_writer(1).unwrap().unwrap());
    metrics.note_pending_at(Instant::now(), 0);
    let episode = metrics.state.episode.load(Ordering::Acquire);
    assert_ne!(episode, 0);
    drop(retained);
    assert_eq!(metrics.state.episode.load(Ordering::Acquire), episode);
    assert_eq!(pressure.writers.lock().unwrap().len(), 1);
    metrics.note_ready();
    assert_eq!(metrics.state.episode.load(Ordering::Acquire), 0);
}

#[test]
fn retained_registry_waits_without_blocking_and_removes_exactly_once() {
    let pressure = Arc::new(TcpWritePressure::new(Instant::now()));
    let original = pressure.try_writer(7).unwrap().unwrap();
    let id = original.state.writer;
    let (mut retained, metrics) = RetainedPressure::split(original);
    assert_eq!(
        retained.try_retire(),
        Ok(false),
        "live carrier owns its original progress state"
    );
    assert!(pressure.writers.lock().unwrap().contains_key(&id));
    let guard = pressure.writers.lock().unwrap();
    assert!(pressure.try_writer(8).unwrap().is_none());
    drop(metrics);
    assert_eq!(retained.try_retire(), Ok(false));
    assert!(guard.contains_key(&id));
    drop(guard);
    assert_eq!(retained.try_retire(), Ok(true));
    assert!(pressure.writers.lock().unwrap().is_empty());
    let later = pressure.try_writer(9).unwrap().unwrap();
    assert_eq!(retained.try_retire(), Ok(true));
    assert_eq!(pressure.writers.lock().unwrap().len(), 1);
    drop(retained);
    drop(later);
    assert!(pressure.writers.lock().unwrap().is_empty());
}

#[test]
fn identity_mismatch_and_poison_stay_failed_without_removing_other_owner() {
    let pressure = Arc::new(TcpWritePressure::new(Instant::now()));
    let first = pressure.try_writer(7).unwrap().unwrap();
    let id = first.state.writer;
    let (mut retained, metrics) = RetainedPressure::split(first);
    let other = pressure.try_writer(8).unwrap().unwrap();
    pressure
        .writers
        .lock()
        .unwrap()
        .insert(id, Arc::downgrade(&other.state));
    assert_eq!(
        retained.try_retire(),
        Err(PressureRetirementFailure::Identity)
    );
    assert_eq!(pressure.writers.lock().unwrap().len(), 2);
    pressure
        .writers
        .lock()
        .unwrap()
        .insert(id, Arc::downgrade(retained.state.as_ref().unwrap()));
    assert_eq!(
        retained.try_retire(),
        Err(PressureRetirementFailure::Identity)
    );
    drop(metrics);
    drop(retained);
    drop(other);

    let first = pressure.try_writer(9).unwrap().unwrap();
    let (mut retained, metrics) = RetainedPressure::split(first);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = pressure.writers.lock().unwrap();
        panic!("poison actual registry");
    }));
    assert_eq!(
        retained.try_retire(),
        Err(PressureRetirementFailure::Poisoned)
    );
    assert!(pressure.try_writer(10).is_err());
    pressure.writers.clear_poison();
    assert_eq!(
        retained.try_retire(),
        Err(PressureRetirementFailure::Poisoned)
    );
    drop(metrics);
    drop(retained);
}

#[test]
fn staged_registration_rejects_overflow_and_collision_without_overwrite() {
    let pressure = Arc::new(TcpWritePressure::new(Instant::now()));
    let first = pressure.try_writer(7).unwrap().unwrap();
    pressure.next_writer.store(0, Ordering::Relaxed);
    assert!(pressure.try_writer(8).is_err());
    let guard = pressure.writers.lock().unwrap();
    assert_eq!(guard.len(), 1);
    assert!(Weak::ptr_eq(
        guard.get(&1).unwrap(),
        &Arc::downgrade(&first.state)
    ));
    drop(guard);
    pressure.next_writer.store(u64::MAX, Ordering::Relaxed);
    assert!(pressure.try_writer(9).is_err());
    assert_eq!(pressure.next_writer.load(Ordering::Relaxed), u64::MAX);
    assert_eq!(pressure.writers.lock().unwrap().len(), 1);
    drop(first);
}

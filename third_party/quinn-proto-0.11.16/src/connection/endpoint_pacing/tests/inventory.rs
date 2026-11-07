//! Actual service slots include detached old generations with independently held leases.
use super::*;

fn native_service() -> (EndpointPacingService, bytes::BackingBudget) {
    let budget = bytes::BackingBudget::new(EndpointPacingService::native_layout_bytes()).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let service = EndpointPacingService::prepare(
        EndpointPacingServiceConfig::endpoint_window_v1(),
        Some(&meter),
    )
    .unwrap();
    (service, budget)
}
fn generation(generation: u64) -> EndpointPacingConnectionKey {
    EndpointPacingConnectionKey::for_registration(ConnectionHandle(0), generation, 0)
}
#[test]
fn native_exact_layout_is_charged_until_actual_last_lease_and_service_release() {
    let bytes = EndpointPacingService::native_layout_bytes();
    let budget = bytes::BackingBudget::new(bytes - 1).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    assert!(EndpointPacingService::prepare(
        EndpointPacingServiceConfig::endpoint_window_v1(),
        Some(&meter)
    )
    .is_err());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    let (service, budget) = native_service();
    assert_eq!(budget.used(), bytes);
    let key = generation(1);
    assert!(service.attach_connection(key));
    let now = Instant::now();
    let lease = service.reserve(key, now, 1).unwrap();
    service.detach_connection(key, now).unwrap();
    drop(service);
    assert_eq!(budget.used(), bytes);
    drop(lease);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[test]
fn nineteen_old_registration_leases_block_new_attachment_without_overwrite_and_then_reuse() {
    let (service, budget) = native_service();
    let now = Instant::now();
    let mut leases = Vec::new();
    for generation_number in 1..=19 {
        let key = generation(generation_number);
        assert!(service.attach_connection(key));
        leases.push(service.reserve(key, now, generation_number).unwrap());
        let detached = service.detach_connection(key, now).unwrap();
        assert_eq!(detached.live_reservation_bytes, generation_number);
    }
    let before = service.snapshot();
    assert!(!service.attach_connection(generation(20)));
    assert_eq!(service.snapshot().connection_records, 19);
    assert_eq!(
        service.snapshot().live_reservation_bytes,
        before.live_reservation_bytes
    );
    assert_eq!(service.snapshot().available_tokens, before.available_tokens);
    assert!(!budget.quarantined());
    drop(leases.pop());
    assert!(service.attach_connection(generation(20)));
    service.detach_connection(generation(20), now).unwrap();
    drop(leases);
    assert_eq!(service.snapshot().connection_records, 0);
    drop(service);
    assert_eq!(budget.used(), 0);
}
#[test]
fn full_migration_keeps_old_attached_key_lease_and_counters_until_capacity_exists() {
    let (service, budget) = native_service();
    let now = Instant::now();
    let mut leases = Vec::new();
    for i in 1..=19 {
        let key = generation(i);
        assert!(service.attach_connection(key));
        leases.push(service.reserve(key, now, 1).unwrap());
        if i != 19 {
            service.detach_connection(key, now).unwrap();
        }
    }
    let old = generation(19);
    let new = old.with_path(1);
    let before = service.snapshot();
    assert!(service.migrate_connection(old, new).is_err());
    assert!(service.lock_state().connections.get(&old).unwrap().attached);
    assert!(!service.lock_state().connections.contains_key(&new));
    assert_eq!(service.snapshot().migrations, before.migrations);
    assert_eq!(service.snapshot().live_reservation_bytes, 19);
    drop(leases.remove(0));
    service.migrate_connection(old, new).unwrap();
    assert!(!service.lock_state().connections.get(&old).unwrap().attached);
    service.detach_connection(new, now).unwrap();
    drop(leases);
    assert_eq!(service.snapshot().connection_records, 0);
    drop(service);
    assert_eq!(budget.used(), 0);
}

#[test]
fn native_full_inventory_queues_grants_old_leases_and_reuse_allocate_nothing() {
    let (service, budget) = native_service();
    let lock_init_allocations = crate::tests::allocations_in(|| drop(service.lock_state()));
    assert_eq!(
        lock_init_allocations, 0,
        "native service mutex must initialize before publication"
    );
    let now = Instant::now();
    let mut counts = [0usize; 4];
    counts[0] = crate::tests::allocations_in(|| {
        for i in 1..=19 {
            let key = generation(i);
            assert!(service.attach_connection(key));
            assert!(matches!(
                service.poll_reserve(
                    key,
                    PacingTrafficClass::Bulk,
                    now + Duration::from_secs(1),
                    1,
                    None,
                    now
                ),
                PollReservation::Blocked { .. }
            ));
        }
    });
    assert_eq!(service.lock_state().bulk_waiters.iter().count(), 19);
    counts[1] = crate::tests::allocations_in(|| {
        for i in 1..=19 {
            assert!(service.cancel_waiter(generation(i)));
        }
    });
    counts[2] = crate::tests::allocations_in(|| {
        for i in 1..=19 {
            let key = generation(i);
            let PollReservation::Granted(lease) =
                service.poll_reserve(key, PacingTrafficClass::Control, now, 1, None, now)
            else {
                panic!("ready control grant");
            };
            service.detach_connection(key, now).unwrap();
            assert!(!service.attach_connection(generation(i + 19)));
            drop(lease);
            assert!(service.attach_connection(generation(i + 19)));
        }
    });
    counts[3] = crate::tests::allocations_in(|| {
        for i in 20..=38 {
            service.detach_connection(generation(i), now).unwrap();
        }
    });
    assert_eq!(counts, [0; 4]);
    assert_eq!(service.snapshot().connection_records, 0);
    drop(service);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

//! Exact mapping/acquisition custody; no time or network dependencies.
use super::super::*;

#[test]
fn checked_acquisition_prepare_and_commit_preserve_legacy_reference() {
    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("lease.test", 0).unwrap();
    pool.acquire(ip, 1);
    let lease = pool.acquire_checked(ip, 2).unwrap();
    assert!(pool.validate_checked_leases([&lease].into_iter()));
    assert_eq!(pool.ip_to_mapping[&ip].refcount, 2);
    drop(pool.prepare_checked_release(&lease, 3).unwrap());
    assert_eq!(pool.ip_to_mapping[&ip].refcount, 2);
    let receipt = pool.prepare_checked_release(&lease, 4).unwrap().commit();
    assert_eq!((receipt.before, receipt.after), (2, 1));
    assert!(pool.validate_checked_leases(std::iter::empty()));
    assert!(pool.prepare_checked_release(&lease, 4).is_err());
    pool.release(ip, 5);
    assert_eq!(pool.sweep(1000, 10), 1);
}

#[test]
fn checked_lease_rejects_wrong_pool_identity_and_duplicate_census() {
    let mut first = FakeIpPool::new();
    let mut other = FakeIpPool::new();
    let ip = first.alloc("lease.test", 0).unwrap();
    assert_eq!(other.alloc("lease.test", 0).unwrap(), ip);
    let lease = first.acquire_checked(ip, 0).unwrap();
    assert!(matches!(
        other.prepare_checked_release(&lease, 0),
        Err(CheckedFakeIpError::WrongPool)
    ));
    assert!(!first.validate_checked_leases([&lease, &lease].into_iter()));
    assert!(!first.validate_checked_leases(std::iter::empty()));
    assert!(!other.validate_checked_leases([&lease].into_iter()));
    assert_eq!(first.ip_to_mapping[&ip].refcount, 1);
}

#[test]
fn checked_lease_rejects_corruption_without_release_or_sweep() {
    for corruption in 0..5 {
        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("lease.test", 0).unwrap();
        let lease = pool.acquire_checked(ip, 0).unwrap();
        match corruption {
            0 => pool.ip_to_mapping.get_mut(&ip).unwrap().refcount = 0,
            1 => pool.ip_to_mapping.get_mut(&ip).unwrap().incarnation += 1,
            2 => {
                pool.checked_leases.remove(&lease.receipt.acquisition);
            }
            3 => {
                pool.domain_to_ip.remove("lease.test");
            }
            _ => pool.ip_to_mapping.get_mut(&ip).unwrap().checked_refs += 1,
        }
        let before = pool.ip_to_mapping[&ip].refcount;
        assert!(pool.prepare_checked_release(&lease, 100).is_err());
        assert_eq!(pool.ip_to_mapping[&ip].refcount, before);
        assert_eq!(pool.sweep(1000, 10), 0);
    }
}

#[test]
fn checked_ids_and_capacity_fail_before_mutation() {
    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("lease.test", 0).unwrap();
    pool.next_acquisition = u64::MAX;
    assert!(matches!(
        pool.acquire_checked(ip, 1),
        Err(CheckedFakeIpError::AcquisitionIdExhausted)
    ));
    assert_eq!(pool.ip_to_mapping[&ip].refcount, 0);
    assert!(pool.checked_leases.is_empty());
    pool.next_acquisition = 0;
    let mut leases = Vec::new();
    for _ in 0..MAX_CHECKED_LEASES {
        leases.push(pool.acquire_checked(ip, 1).unwrap());
    }
    assert!(pool.validate_checked_leases(leases.iter()));
    let next = pool.next_acquisition;
    assert!(matches!(
        pool.acquire_checked(ip, 1),
        Err(CheckedFakeIpError::CapacityReached)
    ));
    assert_eq!(pool.next_acquisition, next);
    for lease in leases {
        pool.prepare_checked_release(&lease, 2).unwrap().commit();
    }
    assert!(pool.validate_checked_leases(std::iter::empty()));
}

#[test]
fn checked_mapping_incarnation_never_wraps_or_reuses_after_sweep() {
    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("old.test", 0).unwrap();
    let old = pool.acquire_checked(ip, 0).unwrap();
    pool.prepare_checked_release(&old, 0).unwrap().commit();
    assert_eq!(pool.sweep(100, 1), 1);
    pool.next = u32::from(ip);
    assert_eq!(pool.alloc("new.test", 100).unwrap(), ip);
    let new = pool.acquire_checked(ip, 100).unwrap();
    assert_ne!(old.receipt.incarnation, new.receipt.incarnation);
    assert!(matches!(
        pool.prepare_checked_release(&old, 100),
        Err(CheckedFakeIpError::WrongIncarnation)
    ));
    assert!(pool.validate_checked_leases([&new].into_iter()));
    pool.next_mapping = u64::MAX;
    let cursor = pool.next;
    assert_eq!(pool.alloc("overflow.test", 100), Err(FakeIpPoolExhausted));
    assert_eq!(pool.next, cursor);
}

#[test]
fn checked_census_rejects_missing_or_reincarnated_mapping() {
    for missing in [false, true] {
        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("lease.test", 0).unwrap();
        let lease = pool.acquire_checked(ip, 0).unwrap();
        if missing {
            pool.ip_to_mapping.remove(&ip);
        } else {
            pool.ip_to_mapping.get_mut(&ip).unwrap().incarnation += 1;
        }
        assert!(!pool.validate_checked_leases([&lease].into_iter()));
        assert!(pool.prepare_checked_release(&lease, 1).is_err());
    }
}

#[test]
fn checked_missing_mapping_is_distinct_from_reverse_map_corruption() {
    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("lease.test", 7).unwrap();
    let absent = std::net::Ipv4Addr::new(198, 18, 0, 99);
    assert_eq!(
        pool.acquire_checked(absent, 8).unwrap_err(),
        CheckedFakeIpError::MissingMapping
    );
    pool.domain_to_ip.remove("lease.test");
    let error = pool.acquire_checked(ip, 8).unwrap_err();
    assert_eq!(
        error,
        CheckedFakeIpError::ReverseMappingMismatch,
        "a broken reverse map is integrity failure, not an expired DNS mapping"
    );
    assert_eq!(pool.ip_to_mapping[&ip].refcount, 0);
    assert_eq!(pool.ip_to_mapping[&ip].checked_refs, 0);
    assert_eq!(pool.ip_to_mapping[&ip].last_used, 7);
    assert_eq!(pool.next_acquisition, 0);
    assert!(pool.checked_leases.is_empty());
}

#[test]
fn checked_integrity_errors_precede_capacity_and_never_mutate() {
    for corruption in 0..5 {
        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("lease.test", 7).unwrap();
        let lease = pool.acquire_checked(ip, 8).unwrap();
        let expected = match corruption {
            0 => {
                pool.ip_to_mapping.get_mut(&ip).unwrap().checked_refs += 1;
                CheckedFakeIpError::CountMismatch
            }
            1 => {
                pool.ip_to_mapping.get_mut(&ip).unwrap().incarnation += 1;
                CheckedFakeIpError::WrongIncarnation
            }
            2 => {
                pool.next_acquisition = 0;
                CheckedFakeIpError::UnknownAcquisition
            }
            3 => {
                pool.ip_to_mapping.get_mut(&ip).unwrap().refcount = u32::MAX;
                CheckedFakeIpError::CountMismatch
            }
            _ => {
                let incarnation = pool.ip_to_mapping[&ip].incarnation;
                for id in 2..=MAX_CHECKED_LEASES as u64 + 1 {
                    pool.checked_leases.insert(id, (ip, incarnation));
                }
                CheckedFakeIpError::CountMismatch
            }
        };
        let mapping = &pool.ip_to_mapping[&ip];
        let before = (
            mapping.refcount,
            mapping.checked_refs,
            mapping.last_used,
            pool.next_acquisition,
        );
        let registry = pool.checked_leases.clone();
        assert_eq!(pool.acquire_checked(ip, 100).unwrap_err(), expected);
        let mapping = &pool.ip_to_mapping[&ip];
        assert_eq!(
            before,
            (
                mapping.refcount,
                mapping.checked_refs,
                mapping.last_used,
                pool.next_acquisition
            )
        );
        assert_eq!(pool.checked_leases, registry);
        assert_eq!(lease.receipt().acquisition, 1);
    }
}

#[test]
fn classified_census_missing_or_duplicate_capability_does_not_poison_its_peer() {
    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("same.test", 0).unwrap();
    let other_ip = pool.alloc("other.test", 0).unwrap();
    let a = pool.acquire_checked(ip, 0).unwrap();
    let b = pool.acquire_checked(ip, 0).unwrap();
    let c = pool.acquire_checked(other_ip, 0).unwrap();
    let registry = pool.checked_leases.clone();

    let missing = pool.audit_checked_leases([&b, &c].into_iter());
    assert!(!missing.complete());
    assert!(!missing.accepts(&a));
    assert!(missing.accepts(&b) && missing.accepts(&c));
    let duplicate = pool.audit_checked_leases([&a, &b, &a, &c].into_iter());
    assert!(!duplicate.complete());
    assert!(!duplicate.accepts(&a));
    assert!(duplicate.accepts(&b) && duplicate.accepts(&c));
    assert_eq!(pool.checked_leases, registry);

    // A registry entry without any observed owner remains physically owned.
    let orphan = pool.audit_checked_leases(std::iter::empty());
    assert!(!orphan.complete());
    assert_eq!(pool.ip_to_mapping[&ip].checked_refs, 2);
    assert_eq!(pool.ip_to_mapping[&ip].refcount, 2);
    assert_eq!(pool.checked_leases, registry);
    assert!(
        pool.audit_checked_leases([&a, &b, &c].into_iter())
            .complete()
    );
}

#[test]
fn classified_census_mapping_count_fault_affects_only_that_ip() {
    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("same.test", 0).unwrap();
    let other_ip = pool.alloc("other.test", 0).unwrap();
    let a = pool.acquire_checked(ip, 0).unwrap();
    let b = pool.acquire_checked(ip, 0).unwrap();
    let c = pool.acquire_checked(other_ip, 0).unwrap();
    pool.release(ip, 0);
    let audit = pool.audit_checked_leases([&a, &b, &c].into_iter());
    assert!(!audit.complete());
    assert!(!audit.accepts(&a) && !audit.accepts(&b));
    assert!(audit.accepts(&c));
    assert_eq!(pool.checked_leases.len(), 3);
    drop(audit);
    pool.acquire(ip, 0);
    assert!(
        pool.audit_checked_leases([&a, &b, &c].into_iter())
            .complete()
    );
}

#[test]
fn classified_census_overflow_is_bounded_and_never_accepts_partial_inventory() {
    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("bound.test", 0).unwrap();
    let lease = pool.acquire_checked(ip, 0).unwrap();
    let audit = pool.audit_checked_leases(std::iter::repeat_n(&lease, MAX_CHECKED_LEASES + 1));
    assert!(!audit.complete() && !audit.accepts(&lease));
    assert!(audit.observed.is_empty());
    assert_eq!(pool.checked_leases.len(), 1);
    drop(audit);
    let tuple = pool.checked_leases[&lease.receipt.acquisition];
    for id in 2..=MAX_CHECKED_LEASES as u64 + 1 {
        pool.checked_leases.insert(id, tuple);
    }
    let audit = pool.audit_checked_leases([&lease].into_iter());
    assert!(!audit.complete() && !audit.accepts(&lease));
    assert!(audit.observed.is_empty());
    assert_eq!(pool.checked_leases.len(), MAX_CHECKED_LEASES + 1);
}

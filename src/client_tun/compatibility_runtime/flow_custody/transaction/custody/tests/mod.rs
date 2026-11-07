use super::*;

#[test]
fn mutable_access_invalidates_whole_replacement_and_exclusive_drop_access() {
    let mut owner = Owner::new(OpenCustody::with_flows());
    let first = owner.lock().unwrap_or_else(PoisonError::into_inner).revision().unwrap();
    {
        let read = owner.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(read.audit_flows());
        assert_eq!(read.revision(), Some(first));
        assert!(matches!(owner.try_lock(), Err(std::sync::TryLockError::WouldBlock)));
    }
    {
        let mut guard = owner.try_lock().unwrap_or_else(|_| panic!("original lock free"));
        guard.live += 1;
        assert!(!guard.audit_flows());
        assert_ne!(guard.revision(), Some(first));
        let changed = guard.revision();
        *guard = OpenCustody::with_flows();
        assert!(guard.audit_flows());
        assert_ne!(guard.revision(), changed, "whole payload replacement cannot reset revision");
    }
    let replaced = owner.lock().unwrap_or_else(PoisonError::into_inner).revision();
    owner.get_mut().unwrap_or_else(PoisonError::into_inner).live += 1;
    let guard = owner.lock().unwrap_or_else(PoisonError::into_inner);
    assert!(!guard.audit_flows());
    assert_ne!(guard.revision(), replaced);
    eprintln!("[N1-open-owner-layout] old={} current={} session={}",
        std::mem::size_of::<Mutex<OpenCustody>>(), std::mem::size_of::<Owner>(),
        std::mem::size_of::<crate::client_tun::compatibility_runtime::SessionControl>());
}

#[test]
fn poison_and_revision_exhaustion_never_issue_reusable_evidence() {
    let owner = Owner::new(OpenCustody::with_flows());
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = owner.lock().unwrap_or_else(PoisonError::into_inner);
        panic!("original lock poison");
    }));
    assert!(failure.is_err());
    let guard = owner.lock().unwrap_or_else(PoisonError::into_inner);
    assert!(guard.audit_flows());
    assert_eq!(guard.revision(), None);
    drop(guard);
    let guard = match owner.try_lock() {
        Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
        _ => panic!("original poison must remain visible"),
    };
    assert_eq!(guard.revision(), None);
    drop(guard);
    let mut owner = Owner::new(OpenCustody::with_flows());
    owner.0.get_mut().unwrap_or_else(PoisonError::into_inner).revision = u64::MAX - 1;
    let mut guard = owner.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(guard.revision(), Some(u64::MAX - 1));
    guard.live += 1;
    assert_eq!(guard.revision(), None);
    guard.live -= 1;
    assert!(guard.audit_flows());
    assert_eq!(guard.revision(), None, "never wrap into an old success");
}

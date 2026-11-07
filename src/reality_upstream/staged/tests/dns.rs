use super::*;
use crate::upstream::TcpOpenCancel;

#[tokio::test]
async fn original_dns_completion_holds_capacity_until_join_cancel_never_connects() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut open = local_attempt(format!(
        "localhost:{}",
        listener.local_addr().unwrap().port()
    ));
    let gate = Arc::new(DnsGate::default());
    let release = GateRelease(gate.clone());
    open.dns_gate = Some(gate);
    let available = super::super::dns_slots().available_permits();
    turn(&mut open);
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Cancelled, deadline);
    settle_at(&mut open, Instant::now(), deadline);
    assert!(matches!(open.state, super::super::State::Dns(_)));
    assert_eq!(super::super::dns_slots().available_permits(), available - 1);
    drop(release);
    dns_finished(&open).await;
    assert_eq!(super::super::dns_slots().available_permits(), available - 1);
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    assert_eq!(super::super::dns_slots().available_permits(), available);
    assert!(open.witness.is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn dns_pressure_rejects_without_job_and_bounds_preserve_address_order() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let slots = super::super::dns_slots().clone();
    let all = slots
        .clone()
        .try_acquire_many_owned(super::super::DNS_JOBS as u32)
        .unwrap();
    let mut open = local_attempt("localhost:443".into());
    turn(&mut open);
    assert!(!matches!(open.state, super::super::State::Dns(_)));
    assert!(
        matches!(open.failure(), Some(crate::shared::ClientError::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock)
    );
    turn(&mut open);
    assert_eq!(open.view().phase, TcpOpenPhase::Settled);
    drop(all);
    assert_eq!(slots.available_permits(), super::super::DNS_JOBS);
    let addresses = [
        "[::1]:123".parse().unwrap(),
        "127.0.0.1:123".parse().unwrap(),
    ];
    let values = super::super::bounded_addresses(addresses.into_iter()).unwrap();
    assert_eq!(values.into_iter().collect::<Vec<_>>(), addresses);
    assert!(super::super::bounded_addresses(std::iter::repeat_n(addresses[0], 33)).is_err());
    assert!(super::super::bounded_addresses(std::iter::empty()).is_err());
}

#[tokio::test]
async fn original_dns_panic_retains_actual_join_error_and_occupied_slot() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let mut open = local_attempt("localhost:443".into());
    let gate = Arc::new(DnsGate::default());
    gate.panic.store(true, std::sync::atomic::Ordering::Release);
    let release = GateRelease(gate.clone());
    open.dns_gate = Some(gate);
    let available = super::super::dns_slots().available_permits();
    turn(&mut open);
    drop(release);
    dns_finished(&open).await;
    turn(&mut open);
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert!(
        matches!(open.failure(), Some(crate::shared::ClientError::Io(e)) if e.get_ref().and_then(|e| e.downcast_ref::<tokio::task::JoinError>()).is_some_and(|e| e.is_panic()))
    );
    assert_eq!(super::super::dns_slots().available_permits(), available - 1);
    settle_at(
        &mut open,
        Instant::now(),
        Instant::now() + Duration::from_secs(50),
    );
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    drop(open); // forced Q destruction; no async-clean claim
    assert_eq!(super::super::dns_slots().available_permits(), available);
}

#[tokio::test]
async fn cancelled_dns_panic_preserves_first_failure_and_original_join_source() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let mut open = local_attempt("localhost:443".into());
    let gate = Arc::new(DnsGate::default());
    gate.panic.store(true, std::sync::atomic::Ordering::Release);
    let release = GateRelease(gate.clone());
    open.dns_gate = Some(gate);
    turn(&mut open);
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Shutdown, deadline);
    let first = open.failure().unwrap().to_string();
    drop(release);
    dns_finished(&open).await;
    settle_at(&mut open, Instant::now(), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert_eq!(open.failure().unwrap().to_string(), first);
    assert!(
        matches!(open.dns_failure.as_ref(), Some(crate::shared::ClientError::Io(e)) if e.get_ref().and_then(|e| e.downcast_ref::<tokio::task::JoinError>()).is_some_and(|e| e.is_panic()))
    );
}

#[tokio::test]
async fn dns_late_pending_then_late_finished_keeps_first_deadline_and_owner() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let mut open = local_attempt("localhost:443".into());
    let gate = Arc::new(DnsGate::default());
    let release = GateRelease(gate.clone());
    open.dns_gate = Some(gate);
    turn(&mut open);
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Deadline, deadline);
    let first = open.failure().unwrap().to_string();
    settle_at(&mut open, deadline, deadline + Duration::from_secs(100));
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert_eq!(open.settle_deadline, Some(deadline));
    drop(release);
    dns_finished(&open).await;
    settle_at(
        &mut open,
        deadline + Duration::from_secs(1),
        deadline + Duration::from_secs(200),
    );
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert_eq!(open.failure().unwrap().to_string(), first);
    assert!(matches!(open.state, super::super::State::Dns(_)));
    assert!(open.view().lease_held && open.view().retirement_pending);
}

#[tokio::test]
async fn late_dns_completion_cannot_clear_original_debt() {
    let _guard = crate::test_support::local_capacity_test_guard().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut open = local_attempt(format!(
        "localhost:{}",
        listener.local_addr().unwrap().port()
    ));
    let gate = Arc::new(DnsGate::default());
    let release = GateRelease(gate.clone());
    open.dns_gate = Some(gate);
    let available = super::super::dns_slots().available_permits();
    turn(&mut open);
    assert!(matches!(open.state, super::super::State::Dns(_)));
    assert_eq!(super::super::dns_slots().available_permits(), available - 1);
    let deadline = Instant::now() + Duration::from_secs(5);
    open.cancel(TcpOpenCancel::Shutdown, deadline);
    drop(release);
    dns_finished(&open).await;
    assert_eq!(super::super::dns_slots().available_permits(), available - 1);
    settle_at(&mut open, deadline + Duration::from_millis(1), deadline);
    assert_eq!(open.view().phase, TcpOpenPhase::Quarantined);
    assert!(open.view().lease_held && open.view().retirement_pending);
    assert!(matches!(open.state, super::super::State::Dns(_)));
    assert_eq!(super::super::dns_slots().available_permits(), available - 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    // Explicit forced teardown of sticky Q; not an async-clean receipt.
    drop(open);
    assert_eq!(super::super::dns_slots().available_permits(), available);
}

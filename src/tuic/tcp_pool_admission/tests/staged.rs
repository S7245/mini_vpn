//! Staged physical-proof admission: actual original mapping/counters/preparation.
//! Compatibility policy remains characterized separately in bounded.rs.
use super::super::*;

fn current(admission: &TcpPoolAdmission, index: usize, black_holes: u64) -> StagedSlotSnapshot {
    StagedSlotSnapshot::Current {
        observation: TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(index + 1, 1),
            24_000,
            Duration::from_millis(20),
            black_holes,
        ),
        activity: admission.current_generation_activity(index).unwrap(),
        replacement_allowed: index != 0,
    }
}
fn assert_zero(admission: &TcpPoolAdmission) {
    assert_eq!(admission.active_total(), 0);
    assert!(
        admission
            .active_slots
            .iter()
            .all(|s| s.load(Ordering::Acquire) == 0)
    );
    assert!(
        admission
            .preparing_slots
            .iter()
            .all(|s| !s.load(Ordering::Acquire))
    );
    assert!(
        admission
            .current_generation_activity
            .iter()
            .all(|s| s.lock().unwrap().active() == 0)
    );
}
#[test]
fn all_sixteen_contended_do_not_touch_auxiliary_locks_or_commit_any_debt() {
    let admission = TcpPoolAdmission::new(MAX_TUIC_TCP_POOL);
    let snapshots = vec![StagedSlotSnapshot::Contended; MAX_TUIC_TCP_POOL];
    let mapping = admission.current_generation_activity[0].lock().unwrap();
    let epoch = admission.qualification_epochs[0].lock().unwrap();
    for _ in 0..3 {
        assert!(matches!(
            admission.try_decide_staged_once(&snapshots, &[], false),
            Err(TcpPoolAdmissionRoundError::Contended)
        ));
        assert_eq!(admission.active_total(), 0);
        assert_eq!(mapping.active(), 0);
        assert!(
            admission
                .preparing_slots
                .iter()
                .all(|s| !s.load(Ordering::Acquire))
        );
    }
    drop((mapping, epoch));
    assert_zero(&admission);
}
#[test]
fn unreadable_slot_zero_cannot_block_known_slot_one_or_steal_its_identity() {
    let admission = TcpPoolAdmission::new(MAX_TUIC_TCP_POOL);
    let mut snapshots = vec![StagedSlotSnapshot::Contended; MAX_TUIC_TCP_POOL];
    snapshots[1] = current(&admission, 1, 0);
    let mapping = admission.current_generation_activity[0].lock().unwrap();
    let epoch = admission.qualification_epochs[0].lock().unwrap();
    let decision = admission
        .try_decide_staged_once(&snapshots, &[], false)
        .unwrap();
    let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = decision else {
        panic!("current")
    };
    assert_eq!(reservation.index, 1);
    assert_eq!(reservation.candidates.len(), 1);
    assert_eq!(
        reservation.candidates[0].identity,
        Some(TcpPoolTransportIdentity::new(2, 1))
    );
    assert!(Arc::ptr_eq(
        reservation.lease.generation_active.as_ref().unwrap(),
        snapshots[1].activity().unwrap()
    ));
    assert_eq!(admission.active_total(), 1);
    drop(reservation);
    // The only known slot was excluded by this original open. Unreadable peers
    // are still potentially eligible; no Saturated/known failure is synthesized.
    assert!(matches!(
        admission.try_decide_staged_once(&snapshots, &[false, true], false),
        Err(TcpPoolAdmissionRoundError::Contended)
    ));
    drop((mapping, epoch));
    assert_zero(&admission);
}
#[test]
fn stale_activity_rejects_current_before_lease_and_returns_its_preparation() {
    let admission = TcpPoolAdmission::new(1);
    let snapshot = current(&admission, 0, 0);
    let old = snapshot.activity().unwrap().clone();
    let next = Arc::new(TcpPoolGenerationActivity::new());
    assert!(admission.replace_current_generation_activity(0, &old, next.clone()));
    assert!(matches!(
        admission.try_decide_staged_once(&[snapshot], &[], false),
        Err(TcpPoolAdmissionRoundError::Contended)
    ));
    assert_eq!(old.active(), 0);
    assert_eq!(next.active(), 0);
    assert_zero(&admission);
    let decision = admission
        .try_decide_staged_once(&[current(&admission, 0, 0)], &[], false)
        .unwrap();
    assert_eq!(next.active(), 1);
    drop(decision);
    assert_zero(&admission);
}
#[test]
fn stale_auxiliary_snapshot_cannot_publish_replacement_preparation() {
    let admission = TcpPoolAdmission::new(2);
    for i in 0..2 {
        admission.commit_idle_epoch(i, TcpPoolTransportIdentity::new(i + 1, 1), 0);
        admission.set_active_for_test(i, 1);
    }
    let snapshots = [current(&admission, 0, 0), current(&admission, 1, 16)];
    let old = snapshots[1].activity().unwrap().clone();
    let next = Arc::new(TcpPoolGenerationActivity::new());
    next.active.store(1, Ordering::Release);
    assert!(admission.replace_current_generation_activity(1, &old, next.clone()));
    assert!(matches!(
        admission.try_decide_staged_once(&snapshots, &[], false),
        Err(TcpPoolAdmissionRoundError::Contended)
    ));
    assert_eq!(admission.active_total(), 2);
    assert!(
        admission
            .preparing_slots
            .iter()
            .all(|s| !s.load(Ordering::Acquire))
    );
    assert_eq!((old.active(), next.active()), (1, 1));
    let snapshots = [current(&admission, 0, 0), current(&admission, 1, 16)];
    let decision = admission
        .try_decide_staged_once(&snapshots, &[], false)
        .unwrap();
    let TcpPoolAdmissionDecision::ReplaceAuxiliary(preparation) = decision else {
        panic!("replacement")
    };
    assert_eq!(preparation.index, 1);
    assert_eq!(preparation.identity, TcpPoolTransportIdentity::new(2, 1));
    assert_eq!(admission.active_total(), 2);
    assert!(admission.preparing_slots[1].load(Ordering::Acquire));
    drop(preparation);
    assert!(
        admission
            .preparing_slots
            .iter()
            .all(|s| !s.load(Ordering::Acquire))
    );
}
#[test]
fn physical_closed_retains_its_identity_and_original_unknown_path_policy() {
    let admission = TcpPoolAdmission::new(1);
    let activity = admission.current_generation_activity(0).unwrap();
    let identity = TcpPoolTransportIdentity::new(19, 7);
    let decision = admission
        .try_decide_staged_once(
            &[StagedSlotSnapshot::Closed {
                identity,
                activity: activity.clone(),
            }],
            &[],
            false,
        )
        .unwrap();
    let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = decision else {
        panic!("closed demand")
    };
    assert_eq!(reservation.candidates[0].identity, Some(identity));
    assert_eq!(
        reservation.qualification,
        TcpPoolForwardQualification::Unknown
    );
    assert!(Arc::ptr_eq(
        reservation.lease.generation_active.as_ref().unwrap(),
        &activity
    ));
    drop(reservation);
    assert_zero(&admission);
}
#[test]
fn malformed_known_or_inventory_cannot_acquire_preparation_or_lease() {
    let admission = TcpPoolAdmission::new(1);
    let invalid = StagedSlotSnapshot::Current {
        observation: TcpPoolPathObservation::Unknown,
        activity: admission.current_generation_activity(0).unwrap(),
        replacement_allowed: true,
    };
    assert!(matches!(
        admission.try_decide_staged_once(&[invalid], &[], false),
        Err(TcpPoolAdmissionRoundError::InvalidInventory)
    ));
    assert!(matches!(
        admission.try_decide_staged_once(&[], &[], false),
        Err(TcpPoolAdmissionRoundError::InvalidInventory)
    ));
    assert_zero(&admission);
    let too_many = TcpPoolAdmission::new(MAX_TUIC_TCP_POOL + 1);
    assert!(matches!(
        too_many.try_decide_staged_once(
            &vec![StagedSlotSnapshot::Contended; MAX_TUIC_TCP_POOL + 1],
            &[],
            false
        ),
        Err(TcpPoolAdmissionRoundError::InvalidInventory)
    ));
    assert_zero(&too_many);
}

#[tokio::test(flavor = "current_thread")]
async fn real_unreadable_slot_cancellation_and_expiry_settle_without_lease_or_connect() {
    use crate::tuic::tcp_fixture::UdpPair;
    use crate::upstream::{ProxyUpstream, TcpOpenCancel, TcpOpenPhase, TcpOpenRequest};
    for reason in 0..4 {
        let pair = UdpPair::new(8, 65536).await;
        let data = Arc::new(pair.upstream(UdpRelayMode::Native).await);
        let slot = data.tcp_pool_slots[0].state.lock().await;
        let now = Instant::now();
        let deadline = now
            + if reason == 3 {
                Duration::from_millis(35)
            } else {
                Duration::from_secs(20)
            };
        let mut attempt = data
            .clone()
            .staged_tcp_open(TcpOpenRequest {
                target: TargetAddr::IpPort("192.0.2.1:443".parse().unwrap()),
                deadline,
            })
            .unwrap();
        let mut cx = Context::from_waker(std::task::Waker::noop());
        for _ in 0..3 {
            attempt.step(&mut cx, now);
        }
        assert_eq!(attempt.view().phase, TcpOpenPhase::Acquiring);
        assert!(!attempt.view().lease_held && !attempt.view().retirement_pending);
        assert!(attempt.failure().is_none());
        assert_zero(&data.tcp_pool_admission);
        let retirement = deadline + Duration::from_secs(5);
        if reason == 3 {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            assert!(Instant::now() >= deadline);
            attempt.step(&mut cx, Instant::now());
        } else {
            attempt.cancel(
                match reason {
                    0 => TcpOpenCancel::Cancelled,
                    1 => TcpOpenCancel::Shutdown,
                    _ => TcpOpenCancel::Deadline,
                },
                retirement,
            );
        }
        attempt.settle(&mut cx, Instant::now(), retirement);
        assert_eq!(attempt.view().phase, TcpOpenPhase::Settled);
        assert!(!attempt.view().lease_held && !attempt.view().retirement_pending);
        assert_zero(&data.tcp_pool_admission);
        drop(attempt);
        drop(slot);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), pair.peer.accept_bi())
                .await
                .is_err()
        );
        drop(data);
        pair.close().await;
    }
}

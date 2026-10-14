use super::super::*;

#[test]
fn bounded_turn_does_not_wait_for_generation_lock() {
    let admission = Arc::new(TcpPoolAdmission::new(1));
    let held = admission.current_generation_activity[0].lock().unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn({
        let admission = admission.clone();
        move || {
            started_tx.send(()).unwrap();
            let result = admission.try_decide_once(&[], &[], &[], false);
            let contended = matches!(result, Err(TcpPoolAdmissionRoundError::Contended));
            drop(result);
            result_tx.send(contended).unwrap();
        }
    });
    started_rx.recv().unwrap();
    let returned_while_locked = result_rx.recv_timeout(Duration::from_secs(1));
    drop(held);
    worker.join().unwrap();
    assert_eq!(
        returned_while_locked.ok(),
        Some(true),
        "one control turn must return Contended while another thread owns the lock"
    );
    assert_eq!(admission.active_total(), 0);
    assert!(!admission.preparing_slots[0].load(Ordering::Acquire));
}

fn observation(index: usize, black_holes: u64) -> TcpPoolPathObservation {
    TcpPoolPathObservation::known(
        TcpPoolTransportIdentity::new(index + 1, 1),
        24_000 * (index as u64 + 1),
        Duration::from_millis(20),
        black_holes,
    )
}

fn idle_choice() -> SelectedAdmission {
    let selected = TcpPoolAdmissionCandidate {
        index: 0,
        identity: Some(TcpPoolTransportIdentity::new(1, 1)),
        active_before: 0,
        path_service: observation(0, 7).path_service(),
        qualification: TcpPoolForwardQualification::Qualified,
        qualification_anchor: Some(7),
        black_holes_current: Some(7),
        successor_service_readiness: TcpPoolSuccessorServiceReadiness::Unknown,
        successor_service_current_path_generation: None,
        successor_service_proved_path_generation: None,
        successor_service_proved_cwnd_floor: None,
        admitted: true,
    };
    SelectedAdmission {
        selected,
        path_service_tiebreak: false,
        service_normalized_ordering: false,
        qualification_override: false,
        all_degraded_fallback: false,
        candidates: vec![selected],
    }
}

fn assert_counts(admission: &TcpPoolAdmission, generation: u64, slot: u64, total: u64) {
    assert_eq!(
        admission.current_generation_activity(0).unwrap().active(),
        generation
    );
    assert_eq!(admission.active_slots[0].load(Ordering::Acquire), slot);
    assert_eq!(admission.active_total(), total);
}

#[test]
fn qualification_contention_cannot_skip_to_another_candidate() {
    let admission = TcpPoolAdmission::new(2);
    let held = admission.qualification_epochs[0].lock().unwrap();
    assert!(matches!(
        admission.try_decide_once(&[observation(0, 0), observation(1, 0)], &[], &[], false),
        Err(TcpPoolAdmissionRoundError::Contended)
    ));
    assert_eq!(admission.active_total(), 0);
    assert!(
        admission
            .preparing_slots
            .iter()
            .all(|slot| !slot.load(Ordering::Acquire))
    );
    drop(held);
    let decision = admission
        .try_decide_once(&[observation(0, 0), observation(1, 0)], &[], &[], false)
        .unwrap();
    let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = decision else {
        panic!("idle current generation expected");
    };
    assert_eq!(reservation.index, 0);
}

#[test]
fn poisoned_locks_fail_explicitly_without_changing_legacy_unknown_behavior() {
    for generation_lock in [true, false] {
        let admission = Arc::new(TcpPoolAdmission::new(2));
        let poisoner = std::thread::spawn({
            let admission = admission.clone();
            move || {
                if generation_lock {
                    let _held = admission.current_generation_activity[0].lock().unwrap();
                    panic!("poison generation mapping");
                } else {
                    let _held = admission.qualification_epochs[0].lock().unwrap();
                    panic!("poison qualification");
                }
            }
        });
        assert!(poisoner.join().is_err());
        assert!(matches!(
            admission.try_decide_once(&[observation(0, 0), observation(1, 0)], &[], &[], false),
            Err(TcpPoolAdmissionRoundError::Poisoned)
        ));
        assert_eq!(admission.active_total(), 0);
        let legacy = admission
            .try_decide_excluding(&[observation(0, 0), observation(1, 0)], &[], &[], false)
            .unwrap();
        let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = legacy else {
            panic!("legacy current reservation expected");
        };
        assert_eq!(reservation.index, usize::from(generation_lock));
        if !generation_lock {
            assert_eq!(
                reservation.qualification,
                TcpPoolForwardQualification::Unknown
            );
        }
        drop(reservation);
        assert_eq!(admission.active_total(), 0);
    }
}

#[test]
fn selected_preparation_cas_and_generation_cas_return_without_retry() {
    let admission = TcpPoolAdmission::new(1);
    let first = admission.try_decide_once(&[], &[], &[], false).unwrap();
    assert_counts(&admission, 1, 1, 1);
    assert!(matches!(
        admission.reserve_selected(idle_choice(), AdmissionLockPolicy::BoundedTry),
        Err(TcpPoolAdmissionRoundError::Contended)
    ));
    assert_counts(&admission, 1, 1, 1);
    assert!(admission.preparing_slots[0].load(Ordering::Acquire));
    drop(first);
    assert_counts(&admission, 0, 0, 0);

    // A sampled zero became active before our generation CAS.
    admission.set_active_for_test(0, 1);
    assert!(matches!(
        admission.reserve_selected(idle_choice(), AdmissionLockPolicy::BoundedTry),
        Err(TcpPoolAdmissionRoundError::Contended)
    ));
    assert_counts(&admission, 1, 1, 1);
    assert!(!admission.preparing_slots[0].load(Ordering::Acquire));
    assert_eq!(
        admission.qualification_epochs[0].lock().unwrap().identity,
        None
    );
}

#[test]
fn selected_lock_contention_returns_preparation_before_any_lease_count() {
    let admission = TcpPoolAdmission::new(1);
    {
        let held = admission.current_generation_activity[0].lock().unwrap();
        assert!(matches!(
            admission.reserve_selected(idle_choice(), AdmissionLockPolicy::BoundedTry),
            Err(TcpPoolAdmissionRoundError::Contended)
        ));
        assert_eq!(held.active(), 0);
        assert!(!admission.preparing_slots[0].load(Ordering::Acquire));
    }
    {
        let held = admission.qualification_epochs[0].lock().unwrap();
        assert!(matches!(
            admission.reserve_selected(idle_choice(), AdmissionLockPolicy::BoundedTry),
            Err(TcpPoolAdmissionRoundError::Contended)
        ));
        assert_counts(&admission, 0, 0, 0);
        assert!(!admission.preparing_slots[0].load(Ordering::Acquire));
        assert_eq!(held.identity, None);
    }
    let selected = admission
        .reserve_selected(idle_choice(), AdmissionLockPolicy::BoundedTry)
        .unwrap();
    let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = selected else {
        panic!("selected idle current expected");
    };
    assert_counts(&admission, 1, 1, 1);
    assert!(Arc::ptr_eq(
        reservation.lease.generation_active.as_ref().unwrap(),
        &admission.current_generation_activity(0).unwrap()
    ));
    let epoch = admission.qualification_epochs[0].lock().unwrap();
    assert_eq!(epoch.identity, Some(TcpPoolTransportIdentity::new(1, 1)));
    assert_eq!(epoch.black_hole_anchor, 7);
    drop(epoch);
    drop(reservation);
    assert_counts(&admission, 0, 0, 0);
    assert!(!admission.preparing_slots[0].load(Ordering::Acquire));
}

#[test]
fn selected_counter_saturation_rolls_back_without_publishing_idle_epoch() {
    for slot_full in [true, false] {
        let admission = TcpPoolAdmission::new(1);
        let counter = if slot_full {
            &admission.active_slots[0]
        } else {
            &admission.pool_active_total
        };
        counter.store(u64::MAX, Ordering::Release);
        assert!(matches!(
            admission.reserve_selected(idle_choice(), AdmissionLockPolicy::BoundedTry),
            Err(TcpPoolAdmissionRoundError::Reservation(
                TcpPoolReservationError::Saturated
            ))
        ));
        assert_counts(
            &admission,
            0,
            if slot_full { u64::MAX } else { 0 },
            if slot_full { 0 } else { u64::MAX },
        );
        assert_eq!(
            admission.qualification_epochs[0].lock().unwrap().identity,
            None
        );
        assert!(!admission.preparing_slots[0].load(Ordering::Acquire));
    }
}

fn decision_view(result: Result<TcpPoolAdmissionDecision, TcpPoolAdmissionRoundError>) -> String {
    match result {
        Ok(TcpPoolAdmissionDecision::ReserveCurrent(r)) => format!(
            "current:{}:{}:{:?}:{:?}:{:?}",
            r.index,
            r.active_before,
            r.path_service,
            (
                r.path_service_tiebreak,
                r.service_normalized_ordering,
                r.qualification,
                r.qualification_anchor,
                r.black_holes_current,
                r.qualification_override,
                r.all_degraded_fallback
            ),
            r.candidates
        ),
        Ok(TcpPoolAdmissionDecision::ReplaceAuxiliary(r)) => format!(
            "replace:{}:{:?}:{}:{}:{}:{:?}",
            r.index,
            r.identity,
            r.active_before,
            r.qualification_anchor,
            r.black_holes_current,
            r.candidates
        ),
        Err(error) => format!("{error:?}"),
    }
}

#[test]
fn bounded_and_legacy_entries_share_the_exact_candidate_policy() {
    let mut cases = 0;
    for left in [0, 1, 3] {
        for right in [0, 1, 3] {
            for unknown_mask in 0..4 {
                for degraded_mask in 0..4 {
                    for replacement_mask in 0..4 {
                        for excluded_mask in 0..4 {
                            for qualified_only in [false, true] {
                                let admission = TcpPoolAdmission::new(2);
                                admission.set_active_for_test(0, left);
                                admission.set_active_for_test(1, right);
                                for i in 0..2 {
                                    admission.commit_idle_epoch(
                                        i,
                                        TcpPoolTransportIdentity::new(i + 1, 1),
                                        0,
                                    );
                                }
                                let observations = (0..2)
                                    .map(|i| {
                                        if unknown_mask & (1 << i) != 0 {
                                            TcpPoolPathObservation::Unknown
                                        } else {
                                            observation(i, u64::from(degraded_mask & (1 << i) != 0))
                                        }
                                    })
                                    .collect::<Vec<_>>();
                                let replacement =
                                    [replacement_mask & 1 != 0, replacement_mask & 2 != 0];
                                let excluded = [excluded_mask & 1 != 0, excluded_mask & 2 != 0];
                                let bounded = decision_view(admission.try_decide_once(
                                    &observations,
                                    &replacement,
                                    &excluded,
                                    qualified_only,
                                ));
                                // Reset idle qualification updates before the second entry.
                                for i in 0..2 {
                                    admission.commit_idle_epoch(
                                        i,
                                        TcpPoolTransportIdentity::new(i + 1, 1),
                                        0,
                                    );
                                }
                                let legacy = decision_view(
                                    admission
                                        .try_decide_excluding(
                                            &observations,
                                            &replacement,
                                            &excluded,
                                            qualified_only,
                                        )
                                        .map_err(TcpPoolAdmissionRoundError::Reservation),
                                );
                                assert_eq!(bounded, legacy);
                                assert_eq!(admission.active_total(), left + right);
                                assert!(
                                    admission
                                        .preparing_slots
                                        .iter()
                                        .all(|p| !p.load(Ordering::Acquire))
                                );
                                cases += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(cases, 4608);
}

#[test]
fn bounded_entry_preserves_empty_busy_saturated_and_rejects_invalid_inventory() {
    let empty = TcpPoolAdmission::new(0);
    assert!(matches!(
        empty.try_decide_once(&[], &[], &[], false),
        Err(TcpPoolAdmissionRoundError::Reservation(
            TcpPoolReservationError::Empty
        ))
    ));
    let admission = TcpPoolAdmission::new(1);
    let first = admission.try_decide_once(&[], &[], &[], false).unwrap();
    assert!(matches!(
        admission.try_decide_once(&[], &[], &[], false),
        Err(TcpPoolAdmissionRoundError::Reservation(
            TcpPoolReservationError::Busy
        ))
    ));
    drop(first);
    admission.set_active_for_test(0, u64::MAX);
    assert!(matches!(
        admission.try_decide_once(&[], &[], &[], false),
        Err(TcpPoolAdmissionRoundError::Reservation(
            TcpPoolReservationError::Saturated
        ))
    ));
    let too_many = TcpPoolAdmission::new(MAX_TUIC_TCP_POOL + 1);
    assert!(matches!(
        too_many.try_decide_once(&[], &[], &[], false),
        Err(TcpPoolAdmissionRoundError::InvalidInventory)
    ));
    let mut inconsistent = TcpPoolAdmission::new(1);
    inconsistent.qualification_epochs.clear();
    assert!(matches!(
        inconsistent.try_decide_once(&[], &[], &[], false),
        Err(TcpPoolAdmissionRoundError::InvalidInventory)
    ));
    assert_eq!(inconsistent.active_total(), 0);
}

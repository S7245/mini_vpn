//! Real metadata custody regressions; these do not stand in for Bound service.

use super::super::*;

#[test]
fn preparation_distinguishes_pending_binding_from_resident_corruption() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let session_id = SessionId::new([0x6f; 16]).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let ready = ReadyIndex::cold(&geometry).unwrap();
    assert_eq!(ready.check_preparation_boundary(0), Ok(()));
    assert_eq!(
        ready.check_preparation_boundary(1),
        Err(ReadyFault::Corrupt)
    );
    assert_eq!(ready.check_root(), Err(ReadyFault::Corrupt));
    assert!(matches!(
        ready.shared.slots[0].lock().unwrap().occupancy,
        ReadyOccupancy::Free
    ));

    for scenario in 0..5 {
        let mut ready = ReadyIndex::cold(&geometry).unwrap();
        let reservation = ready.reserve_session(session_id, limits).unwrap();
        let prepared = reservation.prepare_flow_backing(1, &[]).unwrap();
        let installed = match reservation.bind_prepared(prepared) {
            Ok(installed) => installed,
            Err(_) => panic!("same-domain real binding must succeed"),
        };
        // A live, not-yet-adopted binding is a normal rejection, retaining
        // its actual custody and leaving the gate open for whole adoption.
        assert_eq!(
            ready.check_preparation_boundary(0),
            Err(ReadyFault::InvalidResident)
        );
        assert_eq!(ready.check_root(), Ok(()));
        assert!(Arc::ptr_eq(
            &installed.binding.custody.shared,
            &ready.shared
        ));
        if scenario == 0 {
            assert_eq!(
                ready.check_preparation_boundary(1),
                Err(ReadyFault::Corrupt)
            );
        } else {
            // Metadata fault injection only: no physical activation or
            // business owner is fabricated by this characterization.
            {
                let mut state = ready.shared.slots[0].lock().unwrap();
                let ReadyOccupancy::BoundInactive(correlation, backing) =
                    std::mem::replace(&mut state.occupancy, ReadyOccupancy::Free)
                else {
                    panic!("the actual binding remains inactive");
                };
                state.occupancy = ReadyOccupancy::BoundActive(correlation, backing);
                ready.shared.slots[0]
                    .publication
                    .store(flow::PUBLICATION_ENABLED, Ordering::Release);
            }
            match scenario {
                1 => assert_eq!(
                    ready.check_preparation_boundary(0),
                    Err(ReadyFault::Corrupt)
                ),
                2 => assert_eq!(
                    ready.check_prepared_resident(0, SessionId::new([0x70; 16]).unwrap()),
                    Err(ReadyFault::Corrupt)
                ),
                3 => assert_eq!(
                    ready.check_prepared_resident(1, session_id),
                    Err(ReadyFault::Corrupt)
                ),
                _ => {
                    ready.shared.slots[0].lock().unwrap().key.slot = 1;
                    assert_eq!(
                        ready.check_prepared_resident(0, session_id),
                        Err(ReadyFault::Corrupt)
                    );
                }
            }
        }
        assert_eq!(ready.check_root(), Err(ReadyFault::Corrupt));
        let state = ready.shared.slots[0].lock().unwrap();
        assert!(matches!(
            &state.occupancy,
            ReadyOccupancy::BoundInactive(c, _) | ReadyOccupancy::BoundActive(c, _)
                if c.session_id == session_id && c.limits == limits
        ));
        assert!(
            state
                .processes
                .iter()
                .all(|cell| cell.phase == ReadyPhase::Reserved)
        );
        assert!(Arc::ptr_eq(
            &installed.binding.custody.shared,
            &ready.shared
        ));
    }
}

#[test]
fn cold_cancel_retains_detected_generation_error_beside_original_root_fault() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready = ReadyIndex::cold(&geometry).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let session_id = SessionId::new([0x6d; 16]).unwrap();
    let mut reservation = ready.reserve_session(session_id, limits).unwrap();
    let shared = Arc::clone(&reservation.access.shared);
    let custody = match &mut reservation.state {
        UnboundReservation::Cold(custody) => custody,
        UnboundReservation::Associated(_) => panic!("new cold reservation cannot be associated"),
    };
    // Only the arithmetic boundary is injected, in both halves of the actual
    // correlation. Cancellation still validates the real recorded four charges.
    custody.key.slot_generation = u64::MAX;
    shared.slots[0].lock().unwrap().key = custody.key;
    shared.failure.fail(ReadyFault::RevisionExhausted);

    let failure = reservation.cancel().expect_err("generation cannot wrap");
    assert_eq!(
        failure.source(),
        OwnerReadyReservationError::Metadata(ReadyFault::SlotGenerationExhausted)
    );
    assert_eq!(shared.failure.check(), Err(ReadyFault::RevisionExhausted));
    let quarantine = failure.into_constructor_quarantine();
    let UnboundReservation::Cold(custody) = &quarantine.state else {
        panic!("terminal conversion preserves the original Cold tag");
    };
    assert!(Arc::ptr_eq(&custody.shared, &shared));
    assert!(!custody.abandon_cold);
    assert_eq!(
        quarantine.cause,
        OwnerReadyReservationError::Metadata(ReadyFault::SlotGenerationExhausted)
    );
    assert_eq!(ready.check_root(), Err(ReadyFault::RevisionExhausted));
    {
        let state = shared.slots[0].lock().unwrap();
        assert!(
            matches!(state.occupancy, ReadyOccupancy::ColdReserved(c) if c.session_id == session_id && c.limits == limits)
        );
        assert_eq!(state.key.slot_generation, u64::MAX);
        assert!(
            state
                .processes
                .iter()
                .all(|cell| cell.phase == ReadyPhase::Reserved && cell.revision == 0)
        );
        assert!(
            state
                .sources
                .iter()
                .all(|cell| cell.phase == ReadyPhase::Dormant && cell.revision == 0)
        );
        assert_eq!(state.local_word, 0);
        assert_eq!(state.capacity_mask, 0);
    }
    drop(quarantine);
    assert_eq!(ready.check_root(), Err(ReadyFault::RevisionExhausted));
    assert!(matches!(
        shared.slots[0].lock().unwrap().occupancy,
        ReadyOccupancy::ColdReserved(_)
    ));
}

#[test]
fn terminal_associated_cancel_retains_tag_custody_and_charges_without_index_borrow() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready = ReadyIndex::cold(&geometry).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let session_id = SessionId::new([0x6e; 16]).unwrap();
    let OwnerReadySessionReservation { access, state } =
        ready.reserve_session(session_id, limits).unwrap();
    let UnboundReservation::Cold(mut custody) = state else {
        panic!("the actual new reservation is Cold");
    };
    // Metadata characterization of the existing associated phase, not a P4
    // parent/adoption claim. Change both rights and occupancy before real reentry.
    let shared = Arc::clone(&custody.shared);
    let key = custody.key;
    shared.slots[key.slot].lock().unwrap().occupancy =
        ReadyOccupancy::AssociatedPreparing(custody.correlation);
    custody.abandon_cold = false;
    let reservation = match access.reenter_associated(OwnerReadyAssociatedReservation { custody }) {
        Ok(reservation) => reservation,
        Err(_) => panic!("the same actual associated metadata reenters its index"),
    };
    shared.failure.fail(ReadyFault::RevisionExhausted);
    let failure = reservation
        .cancel()
        .expect_err("Associated is never cancelled as Cold");
    assert_eq!(failure.source(), OwnerReadyReservationError::NotUnattached);
    let quarantine = failure.into_constructor_quarantine();
    let UnboundReservation::Associated(retained) = &quarantine.state else {
        panic!("terminal conversion cannot relabel Associated as Cold");
    };
    assert!(Arc::ptr_eq(&retained.shared, &shared));
    assert!(retained.key == key);
    assert!(retained.correlation == ReadyCorrelation { session_id, limits });
    assert!(!retained.abandon_cold);
    assert_eq!(quarantine.cause, OwnerReadyReservationError::NotUnattached);
    // Moving the original index while quarantine still owns custody proves
    // that neither actual input variant leaves a borrowed failure behind.
    let ready_after = ready;
    assert_eq!(ready_after.check_root(), Err(ReadyFault::RevisionExhausted));
    assert_eq!(shared.epoch.load(Ordering::Acquire) & 1, 1);
    {
        let state = shared.slots[key.slot].lock().unwrap();
        assert!(state.key == key);
        assert!(
            matches!(state.occupancy, ReadyOccupancy::AssociatedPreparing(c)
            if c.session_id == session_id && c.limits == limits)
        );
        assert!(
            state
                .processes
                .iter()
                .all(|cell| cell.phase == ReadyPhase::Reserved && cell.revision == 0)
        );
        assert!(
            state
                .sources
                .iter()
                .all(|cell| cell.phase == ReadyPhase::Dormant && cell.revision == 0)
        );
        assert_eq!(state.local_word, 0);
        assert_eq!(state.capacity_mask, 0);
    }
    drop(quarantine);
    assert_eq!(ready_after.check_root(), Err(ReadyFault::RevisionExhausted));
    assert!(matches!(
        shared.slots[key.slot].lock().unwrap().occupancy,
        ReadyOccupancy::AssociatedPreparing(_)
    ));
}

//! Metadata-only binding/return tests, not a Controller or runtime substitute.
//! Each test retains the same actual index/reservation; no business source,
//! mailbox or successful physical service is synthesized by this fixture.

use super::super::*;
use std::future::Future;
use std::task::{Context, Poll};

struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn admitted_publication_defers_callbacks_until_completed_value_is_retained() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let shared = Arc::clone(&index.shared);
    let count = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut notification = Box::pin(Arc::clone(&shared.notify).notified_owned());
    assert!(notification.as_mut().poll(&mut cx).is_pending());
    let id = SessionId::new([0x77; 16]).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let reservation = index.reserve_session(id, limits).unwrap();
    let prepared = reservation.prepare_flow_backing(1, &[]).unwrap();
    let mut installed = match reservation.bind_prepared(prepared) {
        Ok(installed) => installed,
        Err(_) => panic!("same-domain real metadata binding"),
    };
    assert!(
        installed
            .binding
            .activate(&mut index, 0, &installed.session, (1 << 2) | (1 << 4))
            .is_ok()
    );
    installed.session.control_inbox.wake_by_ref();
    let claim = index.claim(0).unwrap();
    let turn = match installed.binding.begin_publication() {
        Ok(turn) => turn,
        Err(_) => panic!("one active original binding admits one publication interval"),
    };
    installed.session.output.wake_by_ref();
    installed.session.control_inbox.wake_by_ref();
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    assert_ne!(
        shared.slots[0].publication.load(Ordering::Acquire) & flow::PUBLICATION_ACTIVE,
        0
    );
    // This value is a metadata recipe, not evidence of a physical output.
    // Its real owner must retain it beside the same claim before outer Notify.
    let completed = SessionCommandVisit {
        work: OwnerReadyUnit::Waiting,
        runnable: 0,
        blocked: None,
    };
    assert!(turn.finish(&completed).is_ok());
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        shared.slots[0].publication.load(Ordering::Acquire),
        flow::PUBLICATION_ENABLED
    );
    assert_eq!(completed.work, OwnerReadyUnit::Waiting);
    assert_eq!(index.check_root(), Ok(()));
    assert!(shared.slots[0].try_lock().is_ok());
    assert_eq!(claim.source, OwnerReadySource::ControlInbox);
    index
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
    drop(notification);
    let mut next_notification = Box::pin(Arc::clone(&shared.notify).notified_owned());
    assert!(next_notification.as_mut().poll(&mut cx).is_pending());
    installed.session.control_inbox.wake_by_ref();
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    installed.session.output.wake_by_ref();
    assert_eq!(count.0.load(Ordering::SeqCst), 2);
}

fn active_publication_fixture(id: u8) -> (ReadyIndex, OwnerReadyInstalled, ReadyClaim) {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let reservation = index
        .reserve_session(SessionId::new([id; 16]).unwrap(), limits)
        .unwrap();
    let prepared = reservation.prepare_flow_backing(1, &[]).unwrap();
    let mut installed = match reservation.bind_prepared(prepared) {
        Ok(installed) => installed,
        Err(_) => panic!("same-domain real metadata binding"),
    };
    assert!(
        installed
            .binding
            .activate(&mut index, 0, &installed.session, (1 << 2) | (1 << 4))
            .is_ok()
    );
    installed.session.control_inbox.wake_by_ref();
    let claim = index.claim(0).unwrap();
    (index, installed, claim)
}

#[test]
fn inbox_finish_preserves_send_capacity_demand_for_later_release() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let reservation = index
        .reserve_session(SessionId::new([0x7a; 16]).unwrap(), limits)
        .unwrap();
    let prepared = reservation.prepare_flow_backing(1, &[]).unwrap();
    let mut installed = match reservation.bind_prepared(prepared) {
        Ok(installed) => installed,
        Err(_) => panic!("same-domain metadata preparation must bind"),
    };
    installed
        .binding
        .activate(&mut index, 0, &installed.session, 0b1_1111)
        .unwrap();
    installed.session.control_inbox.wake_by_ref();
    let claim = index.claim(0).unwrap();
    assert_eq!(claim.source, OwnerReadySource::ControlInbox);
    // Metadata-only pressure recipe. The actual send owner is still Full;
    // an unrelated inbox/continuation result owns no change to that demand.
    {
        let mut state = index.shared.slots[0].lock().unwrap();
        state.capacity_mask = 0b11;
    }
    index.finish(&claim, 0, None).unwrap();
    assert_eq!(index.shared.slots[0].lock().unwrap().capacity_mask, 0b11);
    installed.session.capacity.wake_by_ref();
    let state = index.shared.slots[0].lock().unwrap();
    assert_eq!(state.local_word & 0b11, 0b11);
}

#[test]
fn deferred_send_preserves_demand_but_actual_send_summary_can_clear_it() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::new(&geometry, &[[1, 0]]).unwrap();
    let claim = index.claim(0).unwrap();
    assert_eq!(claim.source, OwnerReadySource::ControlSend);
    index.shared.slots[0].lock().unwrap().capacity_mask = 0b11;
    // A selected Send excluded by a retained output page did not execute the
    // commands owner. No replacement summary was produced by that visit.
    index.finish(&claim, 1, None).unwrap();
    assert_eq!(index.shared.slots[0].lock().unwrap().capacity_mask, 0b11);
    let resumed = index.claim(0).unwrap();
    index.finish(&resumed, 0, Some(0)).unwrap();
    assert_eq!(index.shared.slots[0].lock().unwrap().capacity_mask, 0);
}

struct PublicationWake {
    shared: Arc<ReadyShared>,
    count: AtomicUsize,
    panic: bool,
}

#[test]
fn quarantined_bound_late_wakes_preserve_the_other_live_session() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(2).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let mut bindings = Vec::new();
    for position in 0..2 {
        let id = SessionId::new([0x81 + position as u8; 16]).unwrap();
        let reservation = index.reserve_session(id, limits).unwrap();
        let prepared = reservation.prepare_flow_backing(1, &[]).unwrap();
        let mut installed = match reservation.bind_prepared(prepared) {
            Ok(installed) => installed,
            Err(_) => panic!("each original metadata reservation must bind"),
        };
        installed
            .binding
            .activate(&mut index, position, &installed.session, 0b1_1111)
            .unwrap();
        bindings.push(installed);
    }
    let original_claim = index.claim(0).unwrap();
    let turn = bindings[0].binding.begin_publication().unwrap();
    index.quarantine(0).unwrap();
    // This exercises the actual metadata boundary, not physical cleanup.
    // Binding and publishers remain owned, as after a real local quarantine.
    bindings[0].session.output.wake_by_ref();
    assert_eq!(index.check_root(), Ok(()));
    assert_ne!(
        index.shared.slots[0].publication.load(Ordering::Acquire) & flow::PUBLICATION_ACTIVE,
        0
    );
    assert!(turn.finish_quarantined().is_ok());
    for _ in 0..3 {
        bindings[0].session.output.wake_by_ref();
        bindings[0].session.target_common_waker().wake_by_ref();
        assert_eq!(index.check_root(), Ok(()));
        assert_eq!(index.select(SESSION_CLASS).unwrap(), Some(1));
        let state = index.shared.slots[0].lock().unwrap();
        assert!(state.key == original_claim.key);
        assert!(matches!(state.occupancy, ReadyOccupancy::BoundActive(..)));
        assert!(matches!(
            state.processes[SESSION_CLASS].phase,
            ReadyPhase::Quarantined
        ));
        assert_eq!(state.local_word, 0);
    }
    let healthy_claim = index.claim(1).unwrap();
    assert_eq!(healthy_claim.source, OwnerReadySource::OwnerContinuation);
    index.finish(&healthy_claim, 0, None).unwrap();
    assert_eq!(index.select(SESSION_CLASS).unwrap(), None);
    assert_eq!(index.check_root(), Ok(()));
    // Ignoring legitimate late hints must not hide an inconsistent quarantine.
    index.shared.slots[0].lock().unwrap().local_word = 1;
    bindings[0].session.output.wake_by_ref();
    assert_eq!(index.check_root(), Err(ReadyFault::Corrupt));
}

impl Wake for PublicationWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Assert at the actual outer callback, not merely after finish returns.
        assert_eq!(
            self.shared.slots[0].publication.load(Ordering::Acquire) & flow::PUBLICATION_ACTIVE,
            0
        );
        assert!(!matches!(
            self.shared.slots[0].try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        self.count.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic, "outer Notify observer unwind");
    }
}

#[test]
fn publication_admission_rejects_poison_stale_mismatch_overlap_and_prior_fault() {
    for case in 0..6 {
        let (index, installed, claim) = active_publication_fixture(0x78);
        let shared = &index.shared;
        let mut first_turn = None;
        let expected = match case {
            0 => {
                assert!(
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let _guard = shared.slots[0].lock().unwrap();
                        panic!("admission retains poison");
                    }))
                    .is_err()
                );
                ReadyFault::Poisoned
            }
            1 => {
                shared.slots[0].lock().unwrap().key.scheduling_generation += 1;
                ReadyFault::Stale
            }
            2 => {
                if let ReadyOccupancy::BoundActive(c, _) =
                    &mut shared.slots[0].lock().unwrap().occupancy
                {
                    c.session_id = SessionId::new([0xff; 16]).unwrap();
                } else {
                    panic!("active original fixture");
                }
                ReadyFault::Corrupt
            }
            3 => {
                shared.slots[0]
                    .publication
                    .fetch_and(!flow::PUBLICATION_ENABLED, Ordering::AcqRel);
                ReadyFault::Corrupt
            }
            4 => {
                first_turn = Some(installed.binding.begin_publication().unwrap());
                ReadyFault::Corrupt
            }
            _ => {
                shared.failure.fail(ReadyFault::RevisionExhausted);
                ReadyFault::RevisionExhausted
            }
        };
        let before = shared.slots[0].publication.load(Ordering::Acquire);
        let failure = installed
            .binding
            .begin_publication()
            .err()
            .expect("no second admission");
        assert_eq!(failure.fault, expected);
        assert_eq!(
            failure.disposition,
            OwnerPublicationAdmissionDisposition::NotEntered
        );
        assert_eq!(index.check_root(), Err(expected));
        assert_eq!(shared.slots[0].publication.load(Ordering::Acquire), before);
        assert_eq!(claim.source, OwnerReadySource::ControlInbox);
        drop(first_turn);
    }
}

#[test]
fn operation_unwind_keeps_publication_sticky_without_notifying_or_completing() {
    let (index, installed, claim) = active_publication_fixture(0x79);
    let shared = &index.shared;
    let count = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut notification = Box::pin(Arc::clone(&shared.notify).notified_owned());
    assert!(notification.as_mut().poll(&mut cx).is_pending());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _turn = installed.binding.begin_publication().unwrap();
            installed.session.output.wake_by_ref();
            panic!("source operation did not return a completion");
        }))
        .is_err()
    );
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    assert_ne!(
        shared.slots[0].publication.load(Ordering::Acquire) & flow::PUBLICATION_ACTIVE,
        0
    );
    assert_eq!(index.check_root(), Ok(()));
    assert_eq!(claim.source, OwnerReadySource::ControlInbox);
    assert_eq!(
        installed.binding.begin_publication().err().unwrap().fault,
        ReadyFault::Corrupt
    );
    // Only the real physical owner may close this interval after quarantine.
    assert_ne!(
        shared.slots[0].publication.load(Ordering::Acquire) & flow::PUBLICATION_ACTIVE,
        0
    );
}

#[test]
fn completed_publication_preserves_first_failure_and_borrowed_value_after_poison() {
    for poison in [false, true] {
        let (index, installed, claim) = active_publication_fixture(0x7a);
        let shared = &index.shared;
        let observer = Arc::new(PublicationWake {
            shared: Arc::clone(shared),
            count: AtomicUsize::new(0),
            panic: false,
        });
        let waker = Waker::from(Arc::clone(&observer));
        let mut cx = Context::from_waker(&waker);
        let mut notification = Box::pin(Arc::clone(&shared.notify).notified_owned());
        assert!(notification.as_mut().poll(&mut cx).is_pending());
        let turn = installed.binding.begin_publication().unwrap();
        if poison {
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _guard = shared.slots[0].lock().unwrap();
                    panic!("actual metadata lock poisoned during admitted operation");
                }))
                .is_err()
            );
            installed.session.control_inbox.wake_by_ref();
        } else {
            shared.slots[0].lock().unwrap().sources[4].revision = u64::MAX;
            installed.session.output.wake_by_ref();
        }
        let expected = if poison {
            ReadyFault::Poisoned
        } else {
            ReadyFault::RevisionExhausted
        };
        assert_eq!(index.check_root(), Err(expected));
        assert_eq!(observer.count.load(Ordering::SeqCst), 0);
        let completed = SessionCommandVisit {
            work: OwnerReadyUnit::Waiting,
            runnable: 0,
            blocked: None,
        };
        let failure = turn
            .finish(&completed)
            .err()
            .expect("completed work cannot erase the fault");
        assert_eq!(failure.fault, expected);
        assert!(std::ptr::eq(failure.completed, &completed));
        assert_eq!(observer.count.load(Ordering::SeqCst), 1);
        assert_eq!(
            shared.slots[0].publication.load(Ordering::Acquire),
            flow::PUBLICATION_ENABLED
        );
        assert_eq!(shared.slots[0].is_poisoned(), poison);
        assert_eq!(claim.source, OwnerReadySource::ControlInbox);
        assert_eq!(index.check_root(), Err(expected));
    }
}

#[test]
fn notification_unwind_keeps_completed_value_and_first_fault_outside_all_guards() {
    for prior_fault in [None, Some(ReadyFault::RevisionExhausted)] {
        let (index, installed, claim) = active_publication_fixture(0x7b);
        let shared = &index.shared;
        let observer = Arc::new(PublicationWake {
            shared: Arc::clone(shared),
            count: AtomicUsize::new(0),
            panic: true,
        });
        let waker = Waker::from(Arc::clone(&observer));
        let mut cx = Context::from_waker(&waker);
        let mut notification = Box::pin(Arc::clone(&shared.notify).notified_owned());
        assert!(notification.as_mut().poll(&mut cx).is_pending());
        let turn = installed.binding.begin_publication().unwrap();
        installed.session.output.wake_by_ref();
        if let Some(fault) = prior_fault {
            shared.failure.fail(fault);
        }
        let completed = SessionCommandVisit {
            work: OwnerReadyUnit::Waiting,
            runnable: 0,
            blocked: None,
        };
        let completed_address = &completed as *const SessionCommandVisit;
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _finish = turn.finish(&completed);
            }))
            .is_err()
        );
        assert!(std::ptr::eq(completed_address, &completed));
        assert_eq!(completed.work, OwnerReadyUnit::Waiting);
        assert_eq!(claim.source, OwnerReadySource::ControlInbox);
        assert_eq!(observer.count.load(Ordering::SeqCst), 1);
        assert_eq!(
            index.check_root(),
            Err(prior_fault.unwrap_or(ReadyFault::NotificationUnwind))
        );
        assert_eq!(
            shared.slots[0].publication.load(Ordering::Acquire),
            flow::PUBLICATION_ENABLED
        );
        assert!(shared.slots[0].try_lock().is_ok());
    }
}

#[test]
fn activation_uses_original_domain_and_current_due_without_notifying() {
    // This is a metadata transaction characterization, not a fabricated
    // physical resident. The real Bound tracer owns the C/actor preflight.
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let mut foreign = ReadyIndex::cold(&geometry).unwrap();
    let shared = Arc::clone(&index.shared);
    let count = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut notification = Box::pin(Arc::clone(&shared.notify).notified_owned());
    assert!(notification.as_mut().poll(&mut cx).is_pending());
    let session_id = SessionId::new([0x75; 16]).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let reservation = index.reserve_session(session_id, limits).unwrap();
    let prepared = reservation.prepare_flow_backing(1, &[]).unwrap();
    let mut installed = match reservation.bind_prepared(prepared) {
        Ok(installed) => installed,
        Err(_) => panic!("same-domain real metadata binding"),
    };
    assert!(matches!(
        installed
            .binding
            .activate(&mut foreign, 0, &installed.session, 1 << 2),
        Err(OwnerReadyReservationError::WrongDomain)
    ));
    assert_eq!(foreign.check_root(), Ok(()));
    assert_eq!(index.check_root(), Ok(()));
    installed.session.control_inbox.wake_by_ref();
    installed.session.ordered_inbox.wake_by_ref();
    assert!(shared.trees.iter().all(ReadyTree::is_empty));
    let activated = match installed
        .binding
        .activate(&mut index, 0, &installed.session, 1 << 2)
    {
        Ok(activated) => activated,
        Err(_) => panic!("the original binding retains its sole activation right"),
    };
    assert_eq!(activated.session_id, session_id);
    assert!(
        activated
            .capacity
            .will_wake(&Waker::from(Arc::clone(&installed.session.capacity)))
    );
    assert!(
        activated
            .output
            .will_wake(&Waker::from(Arc::clone(&installed.session.output)))
    );
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    assert_eq!(index.check_preparation_boundary(1), Ok(()));
    assert_eq!(index.check_prepared_resident(0, session_id), Ok(()));
    assert_eq!(index.select(SESSION_CLASS), Ok(Some(0)));
    let claim = index.claim(0).unwrap();
    assert_eq!(claim.source, OwnerReadySource::ControlInbox);
    index.finish(&claim, 0, None).unwrap();
    assert!(matches!(
        installed
            .binding
            .activate(&mut index, 0, &installed.session, 1 << 2),
        Err(OwnerReadyReservationError::AlreadyActive)
    ));
    assert_eq!(index.check_root(), Ok(()));
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    let state = shared.slots[0].lock().unwrap();
    assert!(
        matches!(state.occupancy, ReadyOccupancy::BoundActive(c, _) if c.session_id == session_id)
    );
    assert!(Arc::ptr_eq(&installed.binding.custody.shared, &shared));
}

#[test]
fn position_preflight_rejects_active_publication_mismatch_as_corruption() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let shared = Arc::clone(&index.shared);
    let session_id = SessionId::new([0x76; 16]).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let reservation = index.reserve_session(session_id, limits).unwrap();
    let prepared = reservation.prepare_flow_backing(1, &[]).unwrap();
    let mut installed = match reservation.bind_prepared(prepared) {
        Ok(installed) => installed,
        Err(_) => panic!("same-domain real metadata binding"),
    };
    assert!(
        installed
            .binding
            .activate(&mut index, 0, &installed.session, 0)
            .is_ok()
    );
    assert_eq!(
        installed.binding.compare_flow_position(1, 0, None),
        Err(OwnerReadyReservationError::AlreadyActive)
    );
    assert_eq!(index.check_root(), Ok(()));
    shared.slots[0]
        .publication
        .fetch_and(!flow::PUBLICATION_ENABLED, Ordering::AcqRel);
    assert_eq!(
        installed.binding.compare_flow_position(1, 0, None),
        Err(OwnerReadyReservationError::Metadata(ReadyFault::Corrupt))
    );
    assert_eq!(index.check_root(), Err(ReadyFault::Corrupt));
    assert!(matches!(
        shared.slots[0].lock().unwrap().occupancy,
        ReadyOccupancy::BoundActive(c, _) if c.session_id == session_id
    ));
    assert!(Arc::ptr_eq(&installed.binding.custody.shared, &shared));
}

#[test]
fn inactive_binding_keeps_owner_inbox_requests_unpublished() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let shared = Arc::clone(&index.shared);
    let count = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut notification = Box::pin(Arc::clone(&shared.notify).notified_owned());
    assert!(matches!(notification.as_mut().poll(&mut cx), Poll::Pending));
    let session_id = SessionId::new([0x71; 16]).unwrap();
    let limits = OwnerReadySessionLimits::checked(2, 1, 3, 4).unwrap();
    let reservation = index.reserve_session(session_id, limits).unwrap();
    let prepared = reservation.prepare_flow_backing(2, &[]).unwrap();
    assert_eq!(prepared.mailbox_capacities, [3, 4]);
    let installed = match reservation.bind_prepared(prepared) {
        Ok(installed) => installed,
        Err(_) => panic!("same-domain unpublished preparation must bind"),
    };
    installed.session.control_inbox.wake_by_ref();
    installed.session.ordered_inbox.wake_by_ref();
    installed.session.target_common.wake_by_ref();
    let state = shared.slots[0].lock().unwrap();
    assert!(
        matches!(state.occupancy, ReadyOccupancy::BoundInactive(c, _) if c.session_id == session_id && c.limits == limits)
    );
    assert!(
        state
            .sources
            .iter()
            .all(|source| source.phase == ReadyPhase::Dormant && source.revision == 0)
    );
    assert!(
        state
            .processes
            .iter()
            .all(|cell| cell.phase == ReadyPhase::Reserved && cell.revision == 0)
    );
    assert_eq!(state.local_word, 0);
    assert_eq!(state.capacity_mask, 0);
    assert!(shared.trees.iter().all(ReadyTree::is_empty));
    assert_eq!(shared.epoch.load(Ordering::Acquire), 0);
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    assert_eq!(index.check_root(), Ok(()));
    assert!(Arc::ptr_eq(&installed.binding.custody.shared, &shared));
    assert!(!installed.binding.custody.abandon_cold);
}

#[test]
fn foreign_preparation_returns_both_original_custodies_before_bind() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut a = ReadyIndex::cold(&geometry).unwrap();
    let mut b = ReadyIndex::cold(&geometry).unwrap();
    let a_shared = Arc::clone(&a.shared);
    let b_shared = Arc::clone(&b.shared);
    let id = SessionId::new([0x72; 16]).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let a_reservation = a.reserve_session(id, limits).unwrap();
    let b_reservation = b.reserve_session(id, limits).unwrap();
    let prepared = a_reservation.prepare_flow_backing(1, &[]).unwrap();
    let original_publisher = Arc::clone(&prepared.session.capacity);
    let (b_reservation, prepared, cause) = match b_reservation.bind_prepared(prepared) {
        Err(failure) => failure,
        Ok(_) => panic!("equal scalar correlation cannot bind a foreign preparation"),
    };
    assert_eq!(cause, OwnerReadyReservationError::WrongDomain);
    assert!(Arc::ptr_eq(&prepared.session.capacity, &original_publisher));
    assert!(matches!(
        a_shared.slots[0].lock().unwrap().occupancy,
        ReadyOccupancy::ColdReserved(_)
    ));
    assert!(matches!(
        b_shared.slots[0].lock().unwrap().occupancy,
        ReadyOccupancy::ColdReserved(_)
    ));
    assert_eq!(a_shared.failure.check(), Ok(()));
    assert_eq!(b_shared.failure.check(), Ok(()));
    assert!(b_reservation.cancel().is_ok());
    let installed = match a_reservation.bind_prepared(prepared) {
        Ok(installed) => installed,
        Err(_) => panic!("the original preparation still belongs to A"),
    };
    assert!(Arc::ptr_eq(
        &installed.session.capacity,
        &original_publisher
    ));
    assert!(Arc::ptr_eq(&installed.binding.custody.shared, &a_shared));
}

#[test]
fn mismatched_flow_limits_leave_the_original_cold_reservation_cancellable() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let id = SessionId::new([0x73; 16]).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let reservation = index.reserve_session(id, limits).unwrap();
    assert!(matches!(
        reservation.prepare_flow_backing(2, &[]),
        Err(OwnerReadyReservationError::DeclaredLimitsMismatch)
    ));
    assert!(reservation.cancel().is_ok());
    assert_eq!(index.check_root(), Ok(()));
    assert!(matches!(
        index.shared.slots[0].lock().unwrap().occupancy,
        ReadyOccupancy::Free
    ));
}

#[test]
fn poisoned_bound_owner_callback_retains_fault_without_notifying_under_actor_context() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let shared = Arc::clone(&index.shared);
    let count = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut notification = Box::pin(Arc::clone(&shared.notify).notified_owned());
    assert!(matches!(notification.as_mut().poll(&mut cx), Poll::Pending));
    let id = SessionId::new([0x74; 16]).unwrap();
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let reservation = index.reserve_session(id, limits).unwrap();
    let prepared = reservation.prepare_flow_backing(1, &[]).unwrap();
    let installed = match reservation.bind_prepared(prepared) {
        Ok(installed) => installed,
        Err(_) => panic!("same-domain preparation must bind"),
    };
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = shared.slots[0].lock().unwrap();
            panic!("retain actual poisoned metadata");
        }))
        .is_err()
    );
    installed.session.control_inbox.wake_by_ref();
    assert_eq!(index.check_root(), Err(ReadyFault::Poisoned));
    assert!(shared.slots[0].is_poisoned());
    assert_ne!(shared.slots[0].publication.load(Ordering::Acquire), 0);
    assert_ne!(shared.epoch.load(Ordering::Acquire), 0);
    assert!(shared.trees.iter().all(ReadyTree::is_empty));
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    assert!(Arc::ptr_eq(&installed.binding.custody.shared, &shared));
}

#[test]
fn retiring_import_keeps_source_correlation_without_reopening_driver_service() {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let id = SessionId::new([0x79; 16]).unwrap();
    // Metadata-only adversarial recipe, not a fabricated running TCP source.
    let local = LocalFlow::for_adapter_test(
        id,
        SessionFlowId::new(1).unwrap(),
        crate::resumable::Direction::TargetToClient,
        1,
    );
    let limits = OwnerReadySessionLimits::checked(1, 1, 1, 1).unwrap();
    let reservation = index.reserve_session(id, limits).unwrap();
    let import = OwnerReadyFlowImport {
        association: OwnerReadyFlowAssociation::Source(local),
        target_present: false,
        read_interest: false,
        write_interest: false,
        join_interest: false,
        completion: false,
        pending_open: false,
        retiring: true,
    };
    let prepared = reservation.prepare_flow_backing(1, &[import]).unwrap();
    assert_eq!(prepared.metadata.activation_sources(0b1_1111), Ok(0b1_0000));
    let mut installed = match reservation.bind_prepared(prepared) {
        Ok(installed) => installed,
        Err(_) => panic!("same original retirement association"),
    };
    assert!(
        installed
            .binding
            .compare_flow_position(
                1,
                0,
                Some((OwnerReadyFlowAssociation::Source(local), 1, true))
            )
            .is_ok()
    );
    let count = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut notification = Box::pin(Arc::clone(&index.shared.notify).notified_owned());
    assert!(notification.as_mut().poll(&mut cx).is_pending());
    installed
        .binding
        .request_retained_flow_work(
            0,
            (OwnerReadyFlowAssociation::Source(local), 1, true),
            OwnerReadyCategory::Retirement,
        )
        .expect("the original inactive owner may retain work without a receipt claim");
    assert_eq!(index.check_root(), Ok(()));
    assert_eq!(index.shared.epoch.load(Ordering::Acquire), 0);
    assert!(index.shared.trees.iter().all(ReadyTree::is_empty));
    assert_eq!(count.0.load(Ordering::SeqCst), 0);
    for lane in [SendLane::Control, SendLane::OrderedSource] {
        installed.tickets[0]
            .driver_waker(lane)
            .unwrap()
            .wake_by_ref();
    }
    installed
        .binding
        .activate(&mut index, 0, &installed.session, 0b1_1111)
        .unwrap();
    let claim = index.claim(0).unwrap();
    assert_eq!(claim.source, OwnerReadySource::OwnerContinuation);
    assert_eq!(index.check_root(), Ok(()));
    let turn = installed
        .binding
        .begin_publication()
        .unwrap_or_else(|_| panic!("active claim"));
    let mask = OwnerReadyCategory::Retirement.mask();
    let selected = installed
        .binding
        .select_continuation(mask, 0)
        .unwrap()
        .unwrap();
    assert_eq!(
        selected.candidate.as_ref().unwrap().revision,
        4,
        "initial due plus one owning request and both late Driver hints survive activation"
    );
    let completed = SessionCommandVisit {
        work: OwnerReadyUnit::Waiting,
        runnable: 0,
        blocked: None,
    };
    assert!(turn.finish(&completed).is_ok());
    index
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
    // The actual retirement obligation remains admitted and nonzero. No
    // synthetic completion, reclaim, Driver drain or whole-root zero is claimed.
}

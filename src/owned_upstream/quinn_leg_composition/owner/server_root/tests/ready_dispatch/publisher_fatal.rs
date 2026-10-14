//! Actual publisher-fatal scenarios retain root and command custody here.
//! Metadata arrangements remain inside the scheduler; no lifecycle finalizer.

mod notify_unwind;
mod park_fatal;
mod source_unknown;

use super::super::super::scheduler::ReadyFault;
use super::*;

fn root_with_full_b(
    a: OwnerCommandPrototype,
    mut b: OwnerCommandPrototype,
) -> (OwnerServerRoot, *const SessionOwnerCommand) {
    assert_eq!(b.drive_one_unit(), OwnerReadyDrive::Full);
    let original_b = full_pointer(&b);
    let Ok(root) =
        OwnerServerRoot::from_commands(ReadyProcessPlan::checked(2, [1; 4]).unwrap(), vec![b, a])
    else {
        panic!("two distinct real command owners");
    };
    (root, original_b)
}

#[test]
fn publisher_revision_exhaustion_closes_admission_before_another_resident_send() {
    let (mut model_a, leg_a) = session(0x29);
    let (mut model_b, leg_b) = session(0x2a);
    let request_a = open(&mut model_a, leg_a, 1);
    let request_b = open(&mut model_b, leg_b, 1);
    let prefill_b = open(&mut model_b, leg_b, 2);
    let (ingress_a, mut inbox_a) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let (ingress_b, mut inbox_b) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    ingress_b
        .try_send_event(SessionEvent::PeerOpenResolved {
            request: prefill_b,
            result: OpenResultCode::PolicyDenied,
        })
        .unwrap();
    let a = OwnerCommandPrototype::from_open_completion(
        ingress_a,
        request_a,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let b = OwnerCommandPrototype::from_open_completion(
        ingress_b,
        request_b,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let (mut root, original_b) = root_with_full_b(a, b);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Waiting }) if session == request_b.session_id()));
    assert_eq!(
        match &root.residents[0].owner {
            OwnerResident::Prototype(prototype) => full_pointer(prototype),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        },
        original_b
    );
    root.ready.arm_b_source_revision_exhaustion_after_waiting();
    let prefill_receipt = receive(&mut inbox_b);
    root.ready.assert_b_source_quarantined_after_dequeue();
    // B's completed actual capacity publication must stop even the next A visit.
    assert_eq!(
        root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Quarantined)
    );
    assert_eq!(
        root.ready.claim(1).err(),
        Some(ReadyFault::RevisionExhausted)
    );
    for _ in 0..8 {
        assert_eq!(
            root.poll_ready_unit(&mut cx),
            Poll::Ready(OwnerReadyStep::Quarantined)
        );
        assert_eq!(
            match &root.residents[0].owner {
                OwnerResident::Prototype(prototype) => full_pointer(prototype),
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            },
            original_b
        );
    }
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
    match &root.residents[1].owner {
        OwnerResident::Prototype(prototype) => prototype.control_ready(),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    for (resident, request) in root.residents.iter().zip([request_b, request_a]) {
        assert!(resident.fault.is_none());
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.command_quarantined(false),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.held_open_completion(request),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.ordered_vacant(),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
    }
    for inbox in [&mut inbox_a, &mut inbox_b] {
        assert!(pin!(inbox.recv_next()).as_mut().poll(&mut cx).is_pending());
    }
    reduce_receipt(
        &mut model_b,
        prefill_receipt,
        prefill_b,
        OpenResultCode::PolicyDenied,
    );
    inbox_a.close();
    inbox_b.close();
    // Root still owns both unresolved commands: this is fail-closed, never zero.
}

#[test]
fn process_revision_exhaustion_is_published_before_root_selection() {
    let (mut model_a, leg_a) = session(0x2b);
    let (mut model_b, leg_b) = session(0x2c);
    let request_a = open(&mut model_a, leg_a, 1);
    let request_b = open(&mut model_b, leg_b, 1);
    let prefill_b = open(&mut model_b, leg_b, 2);
    let (ingress_a, mut inbox_a) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let (ingress_b, mut inbox_b) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    ingress_b
        .try_send_event(SessionEvent::PeerOpenResolved {
            request: prefill_b,
            result: OpenResultCode::PolicyDenied,
        })
        .unwrap();
    let a = OwnerCommandPrototype::from_open_completion(
        ingress_a,
        request_a,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let b = OwnerCommandPrototype::from_open_completion(
        ingress_b,
        request_b,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let (mut root, original_b) = root_with_full_b(a, b);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Waiting }) if session == request_b.session_id()));
    root.ready.arm_b_process_revision_exhaustion_after_waiting();
    assert_eq!(root.ready.check_root(), Ok(()));
    let prefill_receipt = receive(&mut inbox_b);
    root.ready.assert_b_process_quarantined_after_dequeue();
    // No root selection/claim has run since the only actual capacity dequeue.
    assert_eq!(root.ready.check_root(), Err(ReadyFault::RevisionExhausted));
    for _ in 0..8 {
        assert_eq!(
            root.poll_ready_unit(&mut cx),
            Poll::Ready(OwnerReadyStep::Quarantined)
        );
        assert_eq!(
            match &root.residents[0].owner {
                OwnerResident::Prototype(prototype) => full_pointer(prototype),
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            },
            original_b
        );
    }
    assert_eq!(
        root.ready.claim(1).err(),
        Some(ReadyFault::RevisionExhausted)
    );
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
    match &root.residents[1].owner {
        OwnerResident::Prototype(prototype) => prototype.control_ready(),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    for (resident, request) in root.residents.iter().zip([request_b, request_a]) {
        assert!(resident.fault.is_none());
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.command_quarantined(false),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.held_open_completion(request),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.ordered_vacant(),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
    }
    for inbox in [&mut inbox_a, &mut inbox_b] {
        assert!(pin!(inbox.recv_next()).as_mut().poll(&mut cx).is_pending());
    }
    reduce_receipt(
        &mut model_b,
        prefill_receipt,
        prefill_b,
        OpenResultCode::PolicyDenied,
    );
    inbox_a.close();
    inbox_b.close();
    assert_eq!(root.ready.check_root(), Err(ReadyFault::RevisionExhausted));
    // No source/command or process lease is claimed settled by this fixture.
}

#[test]
fn slot_poison_is_published_before_root_selection() {
    let (mut model_a, leg_a) = session(0x2d);
    let (mut model_b, leg_b) = session(0x2e);
    let request_a = open(&mut model_a, leg_a, 1);
    let request_b = open(&mut model_b, leg_b, 1);
    let prefill_b = open(&mut model_b, leg_b, 2);
    let (ingress_a, mut inbox_a) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let (ingress_b, mut inbox_b) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    ingress_b
        .try_send_event(SessionEvent::PeerOpenResolved {
            request: prefill_b,
            result: OpenResultCode::PolicyDenied,
        })
        .unwrap();
    let a = OwnerCommandPrototype::from_open_completion(
        ingress_a,
        request_a,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let b = OwnerCommandPrototype::from_open_completion(
        ingress_b,
        request_b,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let (mut root, original_b) = root_with_full_b(a, b);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Waiting }) if session == request_b.session_id()));
    root.ready.assert_b_slot_unpoisoned_before_guarded_panic();
    assert_eq!(root.ready.check_root(), Ok(()));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        root.ready.poison_b_slot_inside_publication_catch();
    }))
    .expect_err("the mutex fixture must actually unwind");
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"publisher fixture slot poison")
    );
    root.ready.assert_b_slot_poisoned_before_dequeue();
    // Poisoning alone has not run a publisher or changed the root gate.
    assert_eq!(root.ready.check_root(), Ok(()));
    let prefill_receipt = receive(&mut inbox_b);
    // Actual capacity notification, not root selection, publishes Poisoned.
    assert_eq!(root.ready.check_root(), Err(ReadyFault::Poisoned));
    for _ in 0..8 {
        assert_eq!(
            root.poll_ready_unit(&mut cx),
            Poll::Ready(OwnerReadyStep::Quarantined)
        );
        assert_eq!(
            match &root.residents[0].owner {
                OwnerResident::Prototype(prototype) => full_pointer(prototype),
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            },
            original_b
        );
    }
    assert_eq!(root.ready.claim(1).err(), Some(ReadyFault::Poisoned));
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
    match &root.residents[1].owner {
        OwnerResident::Prototype(prototype) => prototype.control_ready(),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    for (resident, request) in root.residents.iter().zip([request_b, request_a]) {
        assert!(resident.fault.is_none());
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.command_quarantined(false),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.held_open_completion(request),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.ordered_vacant(),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
    }
    for inbox in [&mut inbox_a, &mut inbox_b] {
        assert!(pin!(inbox.recv_next()).as_mut().poll(&mut cx).is_pending());
    }
    reduce_receipt(
        &mut model_b,
        prefill_receipt,
        prefill_b,
        OpenResultCode::PolicyDenied,
    );
    inbox_a.close();
    inbox_b.close();
    root.ready.assert_b_slot_still_poisoned_after_close();
    assert_eq!(root.ready.check_root(), Err(ReadyFault::Poisoned));
    // Metadata and commands remain retained; no poison recovery or zero claim.
}

#[test]
fn poisoned_metadata_source_unwind_retains_claim_and_mailbox_disposition() {
    let (mut model_a, leg_a) = session(0x2f);
    let (mut model_b, leg_b) = session(0x30);
    let request_a = open(&mut model_a, leg_a, 1);
    let request_b = open(&mut model_b, leg_b, 1);
    let (ingress_a, mut inbox_a) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let (ingress_b, mut inbox_b) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let a = OwnerCommandPrototype::from_open_completion(
        ingress_a,
        request_a,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let b = OwnerCommandPrototype::from_open_completion(
        ingress_b,
        request_b,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let Ok(mut root) =
        OwnerServerRoot::from_commands(ReadyProcessPlan::checked(2, [1; 4]).unwrap(), vec![a, b])
    else {
        panic!("two distinct real command owners");
    };
    let expected_metadata = root.ready.capture_poisoned_source_metadata();
    let trigger = root.ready.poison_on_first_receive_wake();
    let waker = Waker::from(Arc::clone(&trigger));
    assert!(
        pin!(inbox_a.recv_next())
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut cx = Context::from_waker(Waker::noop());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        root.poll_ready_unit(&mut cx)
    }))
    .expect_err("the actual receiver wake must unwind the send");
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"actual enqueue wake poisons source recovery metadata")
    );
    trigger.assert_called_once();
    let ReadyVisitState::Running(claim, _) = &root.in_flight else {
        panic!("root must keep the admitted claim after the source unwinds");
    };
    let original_claim = std::ptr::from_ref(claim);
    // Round zero observes the unwind before any recovery poll or actual dequeue.
    for round in 0..9 {
        if round == 0 {
            assert_eq!(root.ready.check_root(), Ok(()));
        } else {
            assert_eq!(
                root.poll_ready_unit(&mut cx),
                Poll::Ready(OwnerReadyStep::Quarantined)
            );
            assert_eq!(root.ready.check_root(), Err(ReadyFault::Poisoned));
        }
        let ReadyVisitState::Running(claim, _) = &root.in_flight else {
            panic!("poisoned recovery must not clear the original claim");
        };
        assert_eq!(std::ptr::from_ref(claim), original_claim);
        assert_eq!(claim.slot, 0);
        assert!(matches!(claim.source, OwnerReadySource::ControlSend));
        claim.assert_poisoned_source_metadata(&expected_metadata);
        match &root.residents[0].owner {
            OwnerResident::Prototype(prototype) => prototype.unknown_control(request_a),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &root.residents[1].owner {
            OwnerResident::Prototype(prototype) => prototype.held_open_completion(request_b),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &root.residents[1].owner {
            OwnerResident::Prototype(prototype) => prototype.control_ready(),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        for resident in &root.residents {
            assert!(resident.fault.is_none());
            match &resident.owner {
                OwnerResident::Prototype(prototype) => prototype.command_quarantined(false),
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            }
            match &resident.owner {
                OwnerResident::Prototype(prototype) => prototype.ordered_vacant(),
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            }
        }
        root.ready
            .assert_poisoned_source_readback(&expected_metadata);
    }
    assert_eq!(root.ready.claim(1).err(), Some(ReadyFault::Poisoned));
    trigger.assert_called_once();
    // The actual payload was enqueued once; Unknown is not a second payload owner.
    let receipt = receive(&mut inbox_a);
    reduce_receipt(
        &mut model_a,
        receipt,
        request_a,
        OpenResultCode::TargetRefused,
    );
    for inbox in [&mut inbox_a, &mut inbox_b] {
        assert!(pin!(inbox.recv_next()).as_mut().poll(&mut cx).is_pending());
    }
    match &root.residents[0].owner {
        OwnerResident::Prototype(prototype) => prototype.unknown_control(request_a),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    // Only now retire both cached receiver Wakers, after all causal observations.
    drop(waker);
    assert_eq!(Arc::strong_count(&trigger), 1);
    trigger.assert_called_once();
    drop(trigger);
    inbox_a.close();
    inbox_b.close();
    assert_eq!(root.ready.check_root(), Err(ReadyFault::Poisoned));
    // Running claim + Unknown + B Held remain nonzero, not whole-root settlement.
}

#[test]
fn first_published_poison_survives_later_process_revision_exhaustion() {
    let (mut model_a, leg_a) = session(0x31);
    let (mut model_b, leg_b) = session(0x32);
    let request_a = open(&mut model_a, leg_a, 1);
    let request_b = open(&mut model_b, leg_b, 1);
    let prefill_a = open(&mut model_a, leg_a, 2);
    let prefill_b = open(&mut model_b, leg_b, 2);
    let (ingress_a, mut inbox_a) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let (ingress_b, mut inbox_b) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    for (ingress, request) in [(&ingress_a, prefill_a), (&ingress_b, prefill_b)] {
        ingress
            .try_send_event(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::PolicyDenied,
            })
            .unwrap();
    }
    let mut a = OwnerCommandPrototype::from_open_completion(
        ingress_a,
        request_a,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let b = OwnerCommandPrototype::from_open_completion(
        ingress_b,
        request_b,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    assert_eq!(a.drive_one_unit(), OwnerReadyDrive::Full);
    let original_a = full_pointer(&a);
    let (mut root, original_b) = root_with_full_b(a, b);
    let mut cx = Context::from_waker(Waker::noop());
    for request in [request_b, request_a] {
        assert!(matches!(root.poll_ready_unit(&mut cx),
            Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
                work: OwnerReadyUnit::Waiting }) if session == request.session_id()));
    }
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
    root.ready.arm_a_process_exhaustion_after_both_waiting();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        root.ready.poison_b_before_either_publisher();
    }))
    .expect_err("B's actual slot mutex must unwind");
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"first-cause fixture poisons B before either publisher")
    );
    root.ready.assert_b_slot_poisoned_before_dequeue();
    assert_eq!(root.ready.check_root(), Ok(()));
    let receipt_b = receive(&mut inbox_b);
    assert_eq!(root.ready.check_root(), Err(ReadyFault::Poisoned));
    root.ready.assert_a_still_unpublished_after_b_dequeue();
    // No root selection, claim or close intervenes between these real dequeues.
    let receipt_a = receive(&mut inbox_a);
    root.ready.assert_a_process_quarantined_after_a_dequeue();
    // The second publisher really exhausted; it must not overwrite the first cause.
    assert_eq!(root.ready.check_root(), Err(ReadyFault::Poisoned));
    for _ in 0..8 {
        assert_eq!(
            root.poll_ready_unit(&mut cx),
            Poll::Ready(OwnerReadyStep::Quarantined)
        );
        assert_eq!(
            match &root.residents[0].owner {
                OwnerResident::Prototype(prototype) => full_pointer(prototype),
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            },
            original_b
        );
        assert_eq!(
            match &root.residents[1].owner {
                OwnerResident::Prototype(prototype) => full_pointer(prototype),
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            },
            original_a
        );
    }
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
    for (resident, request) in root.residents.iter().zip([request_b, request_a]) {
        assert!(resident.fault.is_none());
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.command_quarantined(false),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.held_open_completion(request),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.ordered_vacant(),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
    }
    for inbox in [&mut inbox_a, &mut inbox_b] {
        assert!(pin!(inbox.recv_next()).as_mut().poll(&mut cx).is_pending());
    }
    reduce_receipt(
        &mut model_b,
        receipt_b,
        prefill_b,
        OpenResultCode::PolicyDenied,
    );
    reduce_receipt(
        &mut model_a,
        receipt_a,
        prefill_a,
        OpenResultCode::PolicyDenied,
    );
    inbox_a.close();
    inbox_b.close();
    assert_eq!(root.ready.check_root(), Err(ReadyFault::Poisoned));
    // Ordered first-cause retention only, not a concurrent race or settled root.
}

#[test]
fn publisher_fault_during_send_preserves_admitted_completion() {
    let (mut model_a, leg_a) = session(0x33);
    let (mut model_b, leg_b) = session(0x34);
    let request_a = open(&mut model_a, leg_a, 1);
    let request_b = open(&mut model_b, leg_b, 1);
    let prefill_b = open(&mut model_b, leg_b, 2);
    let (ingress_a, mut inbox_a) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let (ingress_b, inbox_b) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    ingress_b
        .try_send_event(SessionEvent::PeerOpenResolved {
            request: prefill_b,
            result: OpenResultCode::PolicyDenied,
        })
        .unwrap();
    let a = OwnerCommandPrototype::from_open_completion(
        ingress_a,
        request_a,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let b = OwnerCommandPrototype::from_open_completion(
        ingress_b,
        request_b,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let (mut root, original_b) = root_with_full_b(a, b);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Waiting }) if session == request_b.session_id()));
    root.ready.arm_b_source_revision_exhaustion_after_waiting();
    let initial_a = root.ready.capture_a_before_admitted_send();
    assert_eq!(root.ready.check_root(), Ok(()));
    let trigger = Arc::new(DequeueDuringSend(Mutex::new(HandoffState {
        inbox: inbox_b,
        receipt: None,
        calls: 0,
    })));
    let trigger_waker = Waker::from(Arc::clone(&trigger));
    assert!(
        pin!(inbox_a.recv_next())
            .as_mut()
            .poll(&mut Context::from_waker(&trigger_waker))
            .is_pending()
    );
    // A is admitted first. Its real enqueue synchronously publishes B's fault.
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned) })
            if session == request_a.session_id()));
    assert_eq!(root.ready.check_root(), Err(ReadyFault::RevisionExhausted));
    {
        let state = trigger.0.lock().unwrap();
        assert_eq!(state.calls, 1);
        assert!(state.receipt.is_some());
    }
    root.ready.assert_b_source_quarantined_after_dequeue();
    match &root.residents[1].owner {
        OwnerResident::Prototype(prototype) => prototype.control_vacant(),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
    root.ready.assert_a_idle_after_admitted_send(&initial_a);
    // Finish must clear A's aggregate while leaving B's fault actually indexed.
    root.ready.assert_only_b_indexed_after_admitted_send();
    for _ in 0..8 {
        assert_eq!(
            root.poll_ready_unit(&mut cx),
            Poll::Ready(OwnerReadyStep::Quarantined)
        );
        assert_eq!(
            match &root.residents[0].owner {
                OwnerResident::Prototype(prototype) => full_pointer(prototype),
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            },
            original_b
        );
    }
    match &root.residents[0].owner {
        OwnerResident::Prototype(prototype) => prototype.held_open_completion(request_b),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    for resident in &root.residents {
        assert!(resident.fault.is_none());
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.command_quarantined(false),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.ordered_vacant(),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
    }
    match &root.residents[1].owner {
        OwnerResident::Prototype(prototype) => prototype.control_vacant(),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
    // Only after the entire admitted-unit proof do either actual receipts leave.
    let prefill_receipt = {
        let mut state = trigger.0.lock().unwrap();
        assert_eq!(state.calls, 1);
        state.receipt.take().unwrap()
    };
    reduce_receipt(
        &mut model_b,
        prefill_receipt,
        prefill_b,
        OpenResultCode::PolicyDenied,
    );
    reduce_receipt(
        &mut model_a,
        receive(&mut inbox_a),
        request_a,
        OpenResultCode::TargetRefused,
    );
    assert!(
        pin!(inbox_a.recv_next())
            .as_mut()
            .poll(&mut cx)
            .is_pending()
    );
    drop(trigger_waker);
    let Ok(trigger) = Arc::try_unwrap(trigger) else {
        panic!("no cached receiver Waker may retain the completed fixture");
    };
    let mut state = trigger.0.into_inner().unwrap();
    assert_eq!(state.calls, 1);
    assert!(state.receipt.is_none());
    assert!(
        pin!(state.inbox.recv_next())
            .as_mut()
            .poll(&mut cx)
            .is_pending()
    );
    inbox_a.close();
    state.inbox.close();
    assert_eq!(root.ready.check_root(), Err(ReadyFault::RevisionExhausted));
    // B's unresolved command/leases remain; this is not whole-root zero.
}

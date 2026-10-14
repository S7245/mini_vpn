//! Real root notification unwind and exact command receipt re-entry.
//! The owning metadata Module supplies the unchanged native Wake recipe.

use super::*;

#[test]
fn committed_publisher_fault_survives_root_notify_unwind() {
    let (mut model_a, leg_a) = session(0x35);
    let (mut model_b, leg_b) = session(0x36);
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
    let mut inbox_cx = Context::from_waker(Waker::noop());
    assert!(matches!(root.poll_ready_unit(&mut inbox_cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Waiting }) if session == request_b.session_id()));
    assert!(matches!(root.poll_ready_unit(&mut inbox_cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned) })
            if session == request_a.session_id()));
    // A's real command stays in its inbox until after the entire unwind proof.
    root.ready.arm_b_source_revision_exhaustion_after_waiting();
    let trigger = root.ready.notify_unwind_wake();
    let waker = Waker::from(Arc::clone(&trigger));
    let mut root_cx = Context::from_waker(&waker);
    assert!(root.poll_ready_unit(&mut root_cx).is_pending());
    assert_eq!(root.ready.check_root(), Ok(()));
    trigger.assert_not_called();
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
    // Real dequeue stores prefetch before capacity publication can unwind.
    let Err(panic) =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| receive(&mut inbox_b)))
    else {
        panic!("the real root Notify must interrupt this receipt return");
    };
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"root Notify wake after committed publisher fault")
    );
    trigger.assert_called_once();
    assert_eq!(root.ready.check_root(), Err(ReadyFault::RevisionExhausted));
    root.ready.assert_committed_notify_fault_metadata();
    for _ in 0..8 {
        // Reuse the original context; do not poll/reset registration separately.
        assert_eq!(
            root.poll_ready_unit(&mut root_cx),
            Poll::Ready(OwnerReadyStep::Quarantined)
        );
        trigger.assert_called_once();
        assert_eq!(
            match &root.residents[0].owner {
                OwnerResident::Prototype(prototype) => full_pointer(prototype),
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            },
            original_b
        );
        assert!(matches!(root.in_flight, ReadyVisitState::Idle));
        match &root.residents[1].owner {
            OwnerResident::Prototype(prototype) => prototype.control_vacant(),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
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
    // Re-entry takes the actual prefetched event once, with no rescue publication.
    let prefill_receipt = receive(&mut inbox_b);
    assert!(
        pin!(inbox_b.recv_next())
            .as_mut()
            .poll(&mut inbox_cx)
            .is_pending()
    );
    trigger.assert_called_once();
    let a_receipt = receive(&mut inbox_a);
    reduce_receipt(
        &mut model_b,
        prefill_receipt,
        prefill_b,
        OpenResultCode::PolicyDenied,
    );
    reduce_receipt(
        &mut model_a,
        a_receipt,
        request_a,
        OpenResultCode::TargetRefused,
    );
    assert!(
        pin!(inbox_a.recv_next())
            .as_mut()
            .poll(&mut inbox_cx)
            .is_pending()
    );
    assert_eq!(
        root.poll_ready_unit(&mut root_cx),
        Poll::Ready(OwnerReadyStep::Quarantined)
    );
    assert_eq!(
        match &root.residents[0].owner {
            OwnerResident::Prototype(prototype) => full_pointer(prototype),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        },
        original_b
    );
    inbox_a.close();
    inbox_b.close();
    trigger.assert_called_once();
    assert_eq!(root.ready.check_root(), Err(ReadyFault::RevisionExhausted));
    root.ready.assert_notify_slot_unpoisoned_after_close();
    // Same living root re-entry only: B ownership and the root remain nonzero.
}

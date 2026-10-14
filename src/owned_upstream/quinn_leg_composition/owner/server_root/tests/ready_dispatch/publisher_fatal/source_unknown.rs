//! Healthy source-unwind isolation through the actual two-resident root.
//! Mailbox payload, claimed borrows and receipt reduction remain in this scope.

use super::*;

#[test]
fn healthy_source_unwind_quarantines_only_its_resident() {
    let (mut model_a, leg_a) = session(0x37);
    let (mut model_b, leg_b) = session(0x38);
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
    let expected_metadata = root.ready.capture_healthy_source_metadata();
    let trigger = root.ready.healthy_source_unwind_wake();
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
    .expect_err("the actual receiver wake must interrupt the admitted send");
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"actual enqueue wake leaves healthy source metadata")
    );
    trigger.assert_called_once();
    // First observe the retained claim and physical disposition without recovery.
    // Only the next ordinary poll may retire this healthy local scheduling claim.
    for recovered in [false, true] {
        if recovered {
            assert_eq!(
                root.poll_ready_unit(&mut cx),
                Poll::Ready(OwnerReadyStep::Quarantined)
            );
            assert!(matches!(root.in_flight, ReadyVisitState::Idle));
        } else {
            let ReadyVisitState::Running(claim, _) = &root.in_flight else {
                panic!("the actual admitted claim must survive source unwind");
            };
            assert_eq!(claim.slot, 0);
            assert!(matches!(claim.source, OwnerReadySource::ControlSend));
            claim.assert_healthy_source_metadata(&expected_metadata);
        }
        assert_eq!(root.ready.check_root(), Ok(()));
        trigger.assert_called_once();
        match &root.residents[0].owner {
            OwnerResident::Prototype(prototype) => prototype.unknown_control(request_a),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &root.residents[1].owner {
            OwnerResident::Prototype(prototype) => prototype.fresh_ready_control(request_b),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        assert!(
            pin!(inbox_b.recv_next())
                .as_mut()
                .poll(&mut cx)
                .is_pending()
        );
        for slot in [0, 1] {
            let local_quarantine = slot == 0 && recovered;
            let resident = &root.residents[slot];
            assert_eq!(
                resident.fault,
                local_quarantine.then_some(ReadyFault::Corrupt)
            );
            match &resident.owner {
                OwnerResident::Prototype(prototype) => {
                    prototype.command_quarantined(local_quarantine)
                }
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            }
            match &resident.owner {
                OwnerResident::Prototype(prototype) => prototype.ordered_vacant(),
                OwnerResident::Current(_) => panic!("expected the original command Prototype"),
            }
            if slot == 0 {
                if recovered {
                    root.ready
                        .assert_a_metadata_after_local_recovery(&expected_metadata);
                } else {
                    root.ready
                        .assert_a_metadata_before_local_recovery(&expected_metadata);
                }
            } else if recovered {
                root.ready
                    .assert_b_metadata_after_a_recovery(&expected_metadata);
            } else {
                root.ready
                    .assert_b_metadata_before_a_recovery(&expected_metadata);
            }
        }
        if recovered {
            root.ready.assert_healthy_tree_after_local_recovery();
        } else {
            root.ready.assert_healthy_tree_before_local_recovery();
        }
    }
    // The isolation proof is the next real B send, not merely an Open marker.
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned) })
            if session == request_b.session_id()));
    assert_eq!(root.ready.check_root(), Ok(()));
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
    match &root.residents[1].owner {
        OwnerResident::Prototype(prototype) => prototype.control_vacant(),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    assert!(root.residents[1].fault.is_none());
    match &root.residents[1].owner {
        OwnerResident::Prototype(prototype) => prototype.command_quarantined(false),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    root.ready
        .assert_b_metadata_after_healthy_send(&expected_metadata);
    trigger.assert_called_once();
    match &root.residents[0].owner {
        OwnerResident::Prototype(prototype) => prototype.unknown_control(request_a),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    assert_eq!(root.residents[0].fault, Some(ReadyFault::Corrupt));
    match &root.residents[0].owner {
        OwnerResident::Prototype(prototype) => prototype.command_quarantined(true),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    // Only now consume the two real commands, each against its own model once.
    let receipt_a = receive(&mut inbox_a);
    let receipt_b = receive(&mut inbox_b);
    reduce_receipt(
        &mut model_a,
        receipt_a,
        request_a,
        OpenResultCode::TargetRefused,
    );
    reduce_receipt(
        &mut model_b,
        receipt_b,
        request_b,
        OpenResultCode::TargetRefused,
    );
    for inbox in [&mut inbox_a, &mut inbox_b] {
        assert!(pin!(inbox.recv_next()).as_mut().poll(&mut cx).is_pending());
    }
    match &root.residents[0].owner {
        OwnerResident::Prototype(prototype) => prototype.unknown_control(request_a),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    assert_eq!(root.residents[0].fault, Some(ReadyFault::Corrupt));
    match &root.residents[0].owner {
        OwnerResident::Prototype(prototype) => prototype.command_quarantined(true),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
    }
    assert_eq!(root.ready.check_root(), Ok(()));
    // The noop empty polls replaced the cached receiver Wakers after the proof.
    drop(waker);
    assert_eq!(Arc::strong_count(&trigger), 1);
    trigger.assert_called_once();
    drop(trigger);
    inbox_a.close();
    inbox_b.close();
    assert_eq!(root.ready.check_root(), Ok(()));
    root.ready.assert_healthy_slots_unpoisoned_after_close();
    // Retired local scheduling claim is not resolution of the retained Unknown.
}

//! Actual cached-Waker replacement, receipt custody and re-park scenario.
//! Dequeue-on-Drop is the real callback window, not a cleanup proof.

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct DequeueOnCachedWakerDrop {
    state: Arc<Mutex<HandoffState>>,
    wakes: Arc<AtomicUsize>,
}

impl Wake for DequeueOnCachedWakerDrop {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for DequeueOnCachedWakerDrop {
    fn drop(&mut self) {
        // Tokio releases its waiter lock before dropping this last cached Arc.
        // The replacement Wake must not try to acquire this fixture mutex.
        let mut state = self.state.lock().unwrap();
        state.calls += 1;
        let receipt = receive(&mut state.inbox);
        state.receipt = Some(receipt);
    }
}

struct CountReplacementWake(AtomicUsize);

impl Wake for CountReplacementWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn fatal_during_cached_waker_replacement_prevents_repark() {
    let (mut model_a, leg_a) = session(0x39);
    let (mut model_b, leg_b) = session(0x3a);
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
    let mut inbox_cx = Context::from_waker(Waker::noop());
    assert!(matches!(root.poll_ready_unit(&mut inbox_cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Waiting }) if session == request_b.session_id()));
    assert!(matches!(root.poll_ready_unit(&mut inbox_cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned) })
            if session == request_a.session_id()));
    root.ready.arm_b_source_revision_exhaustion_after_waiting();
    let handoff = Arc::new(Mutex::new(HandoffState {
        inbox: inbox_b,
        receipt: None,
        calls: 0,
    }));
    let old_wakes = Arc::new(AtomicUsize::new(0));
    let old_waker = Waker::from(Arc::new(DequeueOnCachedWakerDrop {
        state: Arc::clone(&handoff),
        wakes: Arc::clone(&old_wakes),
    }));
    assert!(
        root.poll_ready_unit(&mut Context::from_waker(&old_waker))
            .is_pending()
    );
    drop(old_waker);
    // No external strong reference to the old Waker remains. The real cached
    // registration must keep it alive until this next poll replaces its Waker.
    let before_replacement = {
        let state = handoff.lock().unwrap();
        (state.calls, state.receipt.is_none())
    };
    assert_eq!(before_replacement, (0, true));
    assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
    assert_eq!(root.ready.check_root(), Ok(()));
    let even_epoch = root.ready.capture_even_epoch_before_cached_replacement();
    let replacement = Arc::new(CountReplacementWake(AtomicUsize::new(0)));
    let new_waker = Waker::from(Arc::clone(&replacement));
    let mut root_cx = Context::from_waker(&new_waker);
    // park_epoch has sampled before registration.poll drops the old Waker.
    // That Drop really dequeues B and wakes the replacement before final recheck.
    assert_eq!(
        root.poll_ready_unit(&mut root_cx),
        Poll::Ready(OwnerReadyStep::Rescan)
    );
    let after_replacement = {
        let state = handoff.lock().unwrap();
        (state.calls, state.receipt.is_some())
    };
    assert_eq!(after_replacement, (1, true));
    assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
    assert_eq!(replacement.0.load(Ordering::SeqCst), 1);
    assert_eq!(root.ready.check_root(), Err(ReadyFault::RevisionExhausted));
    let dirty_epoch = root
        .ready
        .assert_fault_during_cached_replacement(&even_epoch);
    // Primitive rejection only: ordinary root entry would reject even earlier.
    // It must not normalize the actual dirty epoch after fatal is committed.
    assert_eq!(root.ready.park_epoch(), Err(ReadyFault::RevisionExhausted));
    root.ready
        .assert_replacement_epoch_stays_dirty(&dirty_epoch);
    for _ in 0..8 {
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
        match &root.residents[1].owner {
            OwnerResident::Prototype(prototype) => prototype.control_vacant(),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        assert!(matches!(root.in_flight, ReadyVisitState::Idle));
        assert_eq!(replacement.0.load(Ordering::SeqCst), 1);
        root.ready
            .assert_replacement_epoch_stays_dirty(&dirty_epoch);
    }
    match &root.residents[0].owner {
        OwnerResident::Prototype(prototype) => prototype.control_capacity_blocked(),
        OwnerResident::Current(_) => panic!("expected the original command Prototype"),
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
    // Take both actual receipts only after the full re-park/fatal custody proof.
    let prefill_receipt = handoff
        .lock()
        .unwrap()
        .receipt
        .take()
        .expect("the sole dequeue receipt");
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
    let Ok(handoff) = Arc::try_unwrap(handoff) else {
        panic!("the old Waker must have released its sole fixture reference");
    };
    let mut handoff = handoff.into_inner().unwrap();
    assert_eq!(handoff.calls, 1);
    assert!(handoff.receipt.is_none());
    for inbox in [&mut inbox_a, &mut handoff.inbox] {
        assert!(
            pin!(inbox.recv_next())
                .as_mut()
                .poll(&mut inbox_cx)
                .is_pending()
        );
    }
    inbox_a.close();
    handoff.inbox.close();
    assert_eq!(old_wakes.load(Ordering::SeqCst), 0);
    assert_eq!(replacement.0.load(Ordering::SeqCst), 1);
    assert_eq!(root.ready.check_root(), Err(ReadyFault::RevisionExhausted));
    assert_eq!(
        match &root.residents[0].owner {
            OwnerResident::Prototype(prototype) => full_pointer(prototype),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        },
        original_b
    );
    // Nonempty aggregate masks the final epoch/gate predicates in this branch;
    // this is not their independent proof, a first-registration or CAS race test.
}

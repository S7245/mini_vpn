//! Actual two-session dispatch and capacity publication during another send.
//! Owns real request/receipt fixtures; no physical session or Target lifecycle.

mod publisher_fatal;

use super::super::*;
type OwnerServerRoot = super::super::OwnerServerRoot<crate::owned_upstream::target::MemoryTarget>;
use crate::owned_upstream::session::SessionMailboxObserverBusy;
use crate::owned_upstream::session::{SessionOwnerCommand, SessionOwnerInbox, SessionOwnerMailbox};
use crate::resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
    AttachTransportBinding, CommittedLeg, DevicePrincipal, DeviceSecret, FeatureOffer, Frame,
    LegGeneration, OpenResultCode, OwnerIdentity, PeerOpenRequest, ReceiveBudgetLimits, Record,
    ReplayBudgetLimits, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig, SessionEffect,
    SessionEvent, SessionFlowId, SessionId, SessionModel, SessionRole, TcpWindowLimits,
    TlsExporterBinding, VersionRange,
};
use crate::shared::TargetAddr;
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

fn session(id: u8) -> (SessionModel, CommittedLeg) {
    let session_id = SessionId::new([id; 16]).unwrap();
    let owner = OwnerIdentity::new([0x31; 32]).unwrap();
    let principal = DevicePrincipal::new([0x41; 16]).unwrap();
    let alpn = AttachAlpn::new(b"mini-vpn-owned/1").unwrap();
    let binding = AttachTransportBinding::new(
        owner,
        alpn,
        TlsExporterBinding::new([0x51; 32]).unwrap(),
        principal,
    );
    let credentials = AttachCredentials::new(
        DeviceSecret::new([0x61; 32]).unwrap(),
        ResumeSecret::new([0x71; 32]).unwrap(),
    )
    .unwrap();
    let request = AttachRequest::new(
        session_id,
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([0x81; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b11, 0b01).unwrap(),
    );
    let proof = credentials.prove(&request, &binding).unwrap();
    let leg = AttachAuthority::new(
        session_id,
        LegGeneration::new(1).unwrap(),
        credentials,
        AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b11).unwrap(),
    )
    .verify_and_commit(&request, &binding, &proof)
    .unwrap();
    let config = SessionConfig::new(
        4,
        8,
        TcpWindowLimits::new(64, 8).unwrap(),
        TcpWindowLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReceiveBudgetLimits::new(128, 16).unwrap(),
        64,
    )
    .unwrap();
    (SessionModel::new(SessionRole::Owner, config, leg), leg)
}

fn open(model: &mut SessionModel, leg: CommittedLeg, flow: u64) -> PeerOpenRequest {
    model
        .reduce(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Open {
                    flow_id: SessionFlowId::new(flow).unwrap(),
                    target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
                },
            )
            .unwrap(),
        })
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::PeerOpenRequested { request, .. } => Some(request),
            _ => None,
        })
        .unwrap()
}

fn receive(inbox: &mut SessionOwnerInbox) -> SessionOwnerCommand {
    let Poll::Ready(Some(command)) = pin!(inbox.recv_next())
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("one actual bounded receive must produce its queued command");
    };
    command
}

fn reduce_receipt(
    model: &mut SessionModel,
    command: SessionOwnerCommand,
    request: PeerOpenRequest,
    expected: OpenResultCode,
) {
    let SessionOwnerCommand::Event(event) = command else {
        panic!("real owner event");
    };
    assert_eq!(
        event,
        SessionEvent::PeerOpenResolved {
            request,
            result: expected
        }
    );
    let effects = model.reduce(event).unwrap();
    assert!(effects.iter().any(|effect| matches!(effect,
        SessionEffect::Transmit(frame) if matches!(frame.record(),
            Record::OpenResult { flow_id, result }
                if *flow_id == request.flow_id() && *result == expected))));
}

fn full_pointer(commands: &OwnerCommandPrototype) -> *const SessionOwnerCommand {
    commands.capture_full_control()
}

struct HandoffState {
    inbox: SessionOwnerInbox,
    receipt: Option<SessionOwnerCommand>,
    calls: usize,
}

struct DequeueDuringSend(Mutex<HandoffState>);

impl Wake for DequeueDuringSend {
    fn wake(self: Arc<Self>) {
        {
            let mut state = self.0.lock().unwrap();
            assert!(
                state.receipt.is_none(),
                "the handoff must occur exactly once"
            );
            state.calls += 1;
            let command = receive(&mut state.inbox);
            state.receipt = Some(command);
        }
    }
}

#[test]
fn ready_session_enqueued_during_cursor_handoff_is_not_lost() {
    let (mut model_a, leg_a) = session(0x22);
    let (mut model_b, leg_b) = session(0x23);
    let request_a = open(&mut model_a, leg_a, 1);
    let request_b = open(&mut model_b, leg_b, 1);
    let prefill_b = open(&mut model_b, leg_b, 2);
    assert_ne!(request_a.session_id(), request_b.session_id());
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
    let mut b = OwnerCommandPrototype::from_open_completion(
        ingress_b,
        request_b,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    assert_eq!(b.drive_one_unit(), OwnerReadyDrive::Full);
    let original_b = full_pointer(&b);
    let plan = ReadyProcessPlan::checked(2, [1; 4]).unwrap();
    let Ok(mut root) = OwnerServerRoot::from_commands(plan, vec![b, a]) else {
        panic!("two actual distinct command residents must be admitted");
    };
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Waiting })
        if session == request_b.session_id()));
    assert_eq!(
        match &root.residents[0].owner {
            OwnerResident::Prototype(prototype) => full_pointer(prototype),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        },
        original_b
    );

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

    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned) })
        if session == request_a.session_id()));
    {
        let state = trigger.0.lock().unwrap();
        assert_eq!(state.calls, 1);
        assert!(state.receipt.is_some());
    }
    // No A dequeue, B dequeue, close, manual rearm or second publication may
    // rescue B between the one causal Wake and its two real dispatch visits.
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Rearm(OwnerCapacityRearm::Rearmed) })
        if session == request_b.session_id()));
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned) })
        if session == request_b.session_id()));
    let (prefill_receipt, final_b) = {
        let mut state = trigger.0.lock().unwrap();
        assert_eq!(state.calls, 1);
        (state.receipt.take().unwrap(), receive(&mut state.inbox))
    };
    reduce_receipt(
        &mut model_b,
        prefill_receipt,
        prefill_b,
        OpenResultCode::PolicyDenied,
    );
    reduce_receipt(
        &mut model_b,
        final_b,
        request_b,
        OpenResultCode::TargetRefused,
    );
    reduce_receipt(
        &mut model_a,
        receive(&mut inbox_a),
        request_a,
        OpenResultCode::TargetRefused,
    );
    assert_eq!(
        root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Rescan)
    );
    assert!(root.poll_ready_unit(&mut cx).is_pending());
    // The proof is complete. Replace A's still-cached Ordered receiver Waker
    // before last-sender destruction can invoke that trigger a second time.
    assert!(
        pin!(inbox_a.recv_next())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(trigger_waker);
    let Ok(trigger) = Arc::try_unwrap(trigger) else {
        panic!("no channel registration may retain the fixture trigger");
    };
    let mut state = trigger.0.into_inner().unwrap();
    assert_eq!(state.calls, 1);
    assert!(state.receipt.is_none());
    inbox_a.close();
    state.inbox.close();
}

#[test]
fn scheduler_claim_corruption_stops_all_residents_without_discarding_commands() {
    let (mut model_a, leg_a) = session(0x24);
    let (mut model_b, leg_b) = session(0x25);
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
        panic!("two real distinct command residents");
    };
    // Deliberately create scheduler Running without a corresponding root visit.
    // No production hook, source mutation or synthetic command is involved.
    let unmatched_claim = root.ready.claim(0).unwrap();
    assert_eq!(unmatched_claim.slot, 0);
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(
        root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Quarantined)
    );
    // A resident-only quarantine would advance the cursor and send B here.
    assert_eq!(
        root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Quarantined)
    );
    assert_eq!(root.ready.check_root(), Err(scheduler::ReadyFault::Corrupt));
    for _ in 0..4 {
        assert_eq!(
            root.poll_ready_unit(&mut cx),
            Poll::Ready(OwnerReadyStep::Quarantined)
        );
    }
    for (resident, request) in root.residents.iter().zip([request_a, request_b]) {
        assert!(resident.fault.is_none());
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.command_quarantined(false),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.fresh_ready_control(request),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
        match &resident.owner {
            OwnerResident::Prototype(prototype) => prototype.ordered_vacant(),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        }
    }
    for inbox in [&mut inbox_a, &mut inbox_b] {
        assert!(pin!(inbox.recv_next()).as_mut().poll(&mut cx).is_pending());
        inbox.close();
    }
    // Fixtures still own unresolved requests/commands; this is not settlement.
}

#[test]
fn unrelated_lane_rearm_registers_remaining_full_before_root_parks() {
    use crate::resumable::{ByteOffset, Direction};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct WakeCount(AtomicUsize);
    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let (mut model, leg) = session(0x26);
    let request = open(&mut model, leg, 1);
    let prefill = open(&mut model, leg, 2);
    let ancillary = open(&mut model, leg, 3);
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    ingress
        .try_send_event(SessionEvent::PeerOpenResolved {
            request: ancillary,
            result: OpenResultCode::Opened,
        })
        .unwrap();
    reduce_receipt(
        &mut model,
        receive(&mut inbox),
        ancillary,
        OpenResultCode::Opened,
    );
    // Reach the real inbox's eighth Control turn using valid reducer receipts.
    // The later observer must not mistake these warm-up epochs for new progress.
    let ack = SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Ack {
                flow_id: ancillary.flow_id(),
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(0),
                final_accepted: false,
            },
        )
        .unwrap(),
    };
    for _ in 0..7 {
        ingress.try_send_event(ack.clone()).unwrap();
        let SessionOwnerCommand::Event(event) = receive(&mut inbox) else {
            panic!("real wire ACK event");
        };
        assert_eq!(event, ack);
        let effects = model.reduce(event).unwrap();
        let [SessionEffect::ReplayAcknowledged { receipt }] = effects.as_slice() else {
            panic!("equal-frontier ACK still produces its exact release receipt");
        };
        assert_eq!(receipt.session_id(), ancillary.session_id());
        assert_eq!(receipt.flow_id(), ancillary.flow_id());
        assert_eq!(receipt.generation(), leg.generation());
        assert_eq!(receipt.direction(), Direction::TargetToClient);
        assert_eq!(receipt.previous_accepted(), ByteOffset::new(0));
        assert_eq!(receipt.next_accepted(), ByteOffset::new(0));
        assert_eq!(receipt.released_bytes(), 0);
        assert!(!receipt.final_accepted());
    }
    let mut commands = OwnerCommandPrototype::from_open_completion(
        ingress.clone(),
        request,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    ingress
        .try_send_event(SessionEvent::PeerOpenResolved {
            request: prefill,
            result: OpenResultCode::PolicyDenied,
        })
        .unwrap();
    let ordered = SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Close {
                flow_id: ancillary.flow_id(),
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(0),
            },
        )
        .unwrap(),
    };
    ingress.try_send_event(ordered.clone()).unwrap();
    assert_eq!(commands.drive_one_unit(), OwnerReadyDrive::Full);
    let original = full_pointer(&commands);
    let Ok(mut root) = OwnerServerRoot::from_commands(
        ReadyProcessPlan::checked(1, [1; 4]).unwrap(),
        vec![commands],
    ) else {
        panic!("one actual Full command resident");
    };
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Waiting }) if session == request.session_id()));
    assert!(root.poll_ready_unit(&mut cx).is_pending());
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);

    let ordered_receipt = receive(&mut inbox);
    assert!(matches!(&ordered_receipt, SessionOwnerCommand::Event(event) if *event == ordered));
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Rearm(OwnerCapacityRearm::UnrelatedLane) })
        if session == request.session_id()));
    assert_eq!(
        match &root.residents[0].owner {
            OwnerResident::Prototype(prototype) => full_pointer(prototype),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        },
        original
    );
    // Control is still full. Root must execute another registration unit,
    // not send again, fabricate Pending or depend on another Ordered wake.
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Waiting }) if session == request.session_id()));
    assert_eq!(
        match &root.residents[0].owner {
            OwnerResident::Prototype(prototype) => full_pointer(prototype),
            OwnerResident::Current(_) => panic!("expected the original command Prototype"),
        },
        original
    );
    assert_eq!(
        root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Rescan)
    );
    assert!(root.poll_ready_unit(&mut cx).is_pending());
    wakes.0.store(0, Ordering::SeqCst);

    // This real Control dequeue is the only new progress before rearm/send.
    // No close, manual publication/rearm, extra enqueue or source polling.
    let prefill_receipt = receive(&mut inbox);
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Rearm(OwnerCapacityRearm::Rearmed) })
        if session == request.session_id()));
    assert!(matches!(root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Visited { session, source: OwnerReadySource::ControlSend,
            work: OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned) })
        if session == request.session_id()));
    reduce_receipt(
        &mut model,
        prefill_receipt,
        prefill,
        OpenResultCode::PolicyDenied,
    );
    reduce_receipt(
        &mut model,
        receive(&mut inbox),
        request,
        OpenResultCode::TargetRefused,
    );

    let SessionOwnerCommand::Event(event) = ordered_receipt else {
        panic!("the retained Ordered receipt must be its actual Close event");
    };
    let mut effects = model.reduce(event).unwrap().into_iter();
    let Some(SessionEffect::Transmit(frame)) = effects.next() else {
        panic!("Close must first acknowledge the current frontier");
    };
    assert_eq!(
        frame.record(),
        &Record::Ack {
            flow_id: ancillary.flow_id(),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(0),
            final_accepted: false,
        }
    );
    let Some(SessionEffect::HalfCloseSink { completion }) = effects.next() else {
        panic!("settle the exact capability minted by this Close");
    };
    assert!(effects.next().is_none());
    let finished = model
        .reduce(SessionEvent::SinkHalfClosed { completion })
        .unwrap();
    let [SessionEffect::Transmit(frame)] = finished.as_slice() else {
        panic!("half-close completion emits exactly its final ACK");
    };
    assert_eq!(
        frame.record(),
        &Record::Ack {
            flow_id: ancillary.flow_id(),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(0),
            final_accepted: true,
        }
    );
    assert_eq!(
        root.poll_ready_unit(&mut cx),
        Poll::Ready(OwnerReadyStep::Rescan)
    );
    assert!(root.poll_ready_unit(&mut cx).is_pending());
    inbox.close();
    // One reducer direction is half-closed; no real Target or whole-root zero.
}

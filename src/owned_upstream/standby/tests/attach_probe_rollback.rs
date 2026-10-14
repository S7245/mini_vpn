use super::tests::{
    FEATURES, active_pair, binding, credentials, deliver_acceptance, deliver_registration, limits,
    release, transport,
};
use super::*;
use crate::owned_upstream::leg::{BoundInbound, LegTerminalTransitionError};
use crate::owned_upstream::leg_io::{
    LegControlQueueErrorKind, LegEndpointRole, LegIo, LegOutboundQueue, MemoryLegTransport,
};
use crate::owned_upstream::owner_target::{
    OwnerTargetConfig, OwnerTargetError, OwnerTargetExecutor,
};
use crate::owned_upstream::supervisor::SessionSupervisor;
use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
use crate::owned_upstream::tcp::FlowPortConfig;
use crate::owned_upstream::two_leg::{LegId, SimTime};
use crate::resumable::{
    AttachNonce, AttachRequest, FeatureOffer, LegControlFrame, LegControlRecord, LegEpochNonce,
    ProbeSequence, SESSION_PROTOCOL_VERSION, VersionRange,
};

#[test]
fn owner_registration_conflict_returns_exact_authenticated_candidate() {
    let (authority, _model, client_active, owner_active) = active_pair();
    let mut client_b = LegIo::for_authenticated_transport(
        LegId::B,
        LegEndpointRole::Client,
        binding(0xc1),
        limits(),
    );
    let mut owner_b = LegIo::for_authenticated_transport(
        LegId::B,
        LegEndpointRole::Owner,
        binding(0xc1),
        limits(),
    );
    let mut client_queue = LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
    let mut owner_queue = LegOutboundQueue::for_leg(owner_b.established_leg(), 4, 4_096).unwrap();
    let mut wire = transport();
    let (_awaiting, installed) = deliver_registration(
        &client_active,
        &owner_active,
        &mut client_b,
        &mut owner_b,
        &mut client_queue,
        &mut wire,
        &authority,
        0x31,
    );
    let mut slot = OwnerStandbySlot::new();
    assert_eq!(
        slot.admit(installed, &mut owner_queue).unwrap(),
        OwnerStandbyAdmission::Installed,
    );

    let (_awaiting_conflict, conflict) = deliver_registration(
        &client_active,
        &owner_active,
        &mut client_b,
        &mut owner_b,
        &mut client_queue,
        &mut wire,
        &authority,
        0x32,
    );
    let queue_before = (owner_queue.len(), owner_queue.owned_bytes());
    let failure = slot.admit(conflict, &mut owner_queue).unwrap_err();

    assert_eq!(failure.kind(), &OwnerStandbyAdmissionErrorKind::Rejected);
    assert!(
        failure.into_retry().is_some(),
        "precommit rejection must return the exact authenticated candidate",
    );
    assert_eq!((owner_queue.len(), owner_queue.owned_bytes()), queue_before);
    assert!(slot.registered_is_live());
}

#[derive(Debug, PartialEq, Eq)]
enum ProbeSnapshot {
    Pending {
        registration: usize,
        probe_sequence: ProbeSequence,
        encoded: Vec<u8>,
    },
    Awaiting {
        registration: usize,
        probe_sequence: ProbeSequence,
        receipt: String,
    },
    Armed {
        registration: usize,
        probe_sequence: ProbeSequence,
        receipt: String,
        deadline: SimTime,
    },
    Fresh {
        registration: usize,
        probe_sequence: ProbeSequence,
        receipt: String,
        acknowledged_at: SimTime,
        fresh_until: SimTime,
    },
}

fn probe_snapshot(slot: &OwnerStandbySlot) -> ProbeSnapshot {
    let registered = slot
        .registered
        .as_ref()
        .expect("rollback must retain the exact registered standby");
    match &slot.probe {
        OwnerStandbyProbeState::Pending(pending) => ProbeSnapshot::Pending {
            registration: Arc::as_ptr(&pending.registration) as usize,
            probe_sequence: pending.probe_sequence,
            encoded: pending
                .frame
                .encode(registered.candidate.contract().features)
                .expect("the authenticated probe remains encodable")
                .to_vec(),
        },
        OwnerStandbyProbeState::Awaiting {
            registration,
            probe_sequence,
            receipt,
        } => ProbeSnapshot::Awaiting {
            registration: Arc::as_ptr(registration) as usize,
            probe_sequence: *probe_sequence,
            receipt: format!("{receipt:?}"),
        },
        OwnerStandbyProbeState::Armed {
            registration,
            probe_sequence,
            receipt,
            deadline,
        } => ProbeSnapshot::Armed {
            registration: Arc::as_ptr(registration) as usize,
            probe_sequence: *probe_sequence,
            receipt: format!("{receipt:?}"),
            deadline: *deadline,
        },
        OwnerStandbyProbeState::Fresh {
            registration,
            probe_sequence,
            receipt,
            acknowledged_at,
            fresh_until,
        } => ProbeSnapshot::Fresh {
            registration: Arc::as_ptr(registration) as usize,
            probe_sequence: *probe_sequence,
            receipt: format!("{receipt:?}"),
            acknowledged_at: *acknowledged_at,
            fresh_until: *fresh_until,
        },
        OwnerStandbyProbeState::Idle => panic!("fixture must own a non-idle physical probe"),
    }
}

struct RegisteredAttachFixture {
    client_b: LegIo,
    owner_b: LegIo,
    owner_queue: LegOutboundQueue,
    wire: MemoryLegTransport,
    slot: OwnerStandbySlot,
    received_attach: crate::owned_upstream::leg::LegBoundFrame,
    executor: OwnerTargetExecutor<MemoryTarget>,
}

fn registered_attach_fixture(owner_queue_frames: usize) -> RegisteredAttachFixture {
    let (authority, model, client_active, owner_active) = active_pair();
    let mut client_b = LegIo::for_authenticated_transport(
        LegId::B,
        LegEndpointRole::Client,
        binding(0xd1),
        limits(),
    );
    let mut owner_b = LegIo::for_authenticated_transport(
        LegId::B,
        LegEndpointRole::Owner,
        binding(0xd1),
        limits(),
    );
    let mut client_queue = LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
    let mut owner_queue =
        LegOutboundQueue::for_leg(owner_b.established_leg(), owner_queue_frames, 4_096).unwrap();
    let mut wire = transport();
    let (awaiting, candidate) = deliver_registration(
        &client_active,
        &owner_active,
        &mut client_b,
        &mut owner_b,
        &mut client_queue,
        &mut wire,
        &authority,
        0x61,
    );
    let mut slot = OwnerStandbySlot::new();
    slot.admit(candidate, &mut owner_queue).unwrap();
    let registered = awaiting
        .validate(deliver_acceptance(
            &mut owner_b,
            &mut client_b,
            &mut owner_queue,
            &mut wire,
        ))
        .unwrap();

    let awaiting_attach = registered
        .begin_exact_next_attach(AttachNonce::new([0x62; 16]).unwrap(), &credentials())
        .unwrap()
        .enqueue(&mut client_queue)
        .unwrap();
    client_queue
        .try_flush(&mut client_b, &mut wire.controller_sender(), SimTime::ZERO)
        .unwrap()
        .unwrap();
    release(&mut wire);
    let received_attach = owner_b
        .receive_delivery(
            wire.try_recv_next(owner_b.inbound_route())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
    assert!(awaiting_attach.request_receipt.was_delivered());

    let supervisor =
        SessionSupervisor::new_owner_with_active(model, authority, &owner_active).unwrap();
    let executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(64, 1_024, 64).unwrap(),
        supervisor,
        MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
        FlowPortConfig::new(64, 8, 8, 8).unwrap(),
    )
    .unwrap();

    RegisteredAttachFixture {
        client_b,
        owner_b,
        owner_queue,
        wire,
        slot,
        received_attach,
        executor,
    }
}

fn make_pending_probe(fixture: &mut RegisteredAttachFixture) {
    let registered = fixture.slot.registered.as_ref().unwrap();
    let contract = registered.candidate.contract();
    let filler = contract.acceptance_frame().unwrap();
    fixture
        .owner_queue
        .push_leg_control(&registered.candidate.seal, filler, contract.features)
        .unwrap();
    let error = fixture
        .slot
        .drive_exact_probe(ProbeSequence::new(7).unwrap(), &mut fixture.owner_queue)
        .unwrap_err();
    assert!(matches!(
        error,
        OwnerStandbyProbeDriveError::Queue(LegControlQueueErrorKind::CapacityExceeded { .. })
    ));
    fixture
        .owner_queue
        .try_flush(
            &mut fixture.owner_b,
            &mut fixture.wire.controller_sender(),
            SimTime::ZERO,
        )
        .unwrap()
        .unwrap();
    release(&mut fixture.wire);
    let _filler = fixture
        .client_b
        .receive_classified_delivery(
            fixture
                .wire
                .try_recv_next(fixture.client_b.inbound_route())
                .unwrap()
                .unwrap(),
            contract.features,
        )
        .unwrap();
    assert!(fixture.owner_queue.is_empty());
}

fn make_awaiting_probe(fixture: &mut RegisteredAttachFixture, sequence: u64) {
    let contract = fixture
        .slot
        .registered
        .as_ref()
        .unwrap()
        .candidate
        .contract();
    fixture
        .slot
        .drive_exact_probe(
            ProbeSequence::new(sequence).unwrap(),
            &mut fixture.owner_queue,
        )
        .unwrap();
    fixture
        .owner_queue
        .try_flush(
            &mut fixture.owner_b,
            &mut fixture.wire.controller_sender(),
            SimTime::ZERO,
        )
        .unwrap()
        .unwrap();
    release(&mut fixture.wire);
    let _probe = fixture
        .client_b
        .receive_classified_delivery(
            fixture
                .wire
                .try_recv_next(fixture.client_b.inbound_route())
                .unwrap()
                .unwrap(),
            contract.features,
        )
        .unwrap();
    assert!(fixture.owner_queue.is_empty());
}

fn make_armed_probe(fixture: &mut RegisteredAttachFixture, sequence: u64) -> SimTime {
    make_awaiting_probe(fixture, sequence);
    let OwnerStandbyProbeArmDrive::Armed { deadline, .. } = fixture
        .slot
        .arm_exact_probe_after_delivery(
            SimTime::from_nanos(100),
            std::time::Duration::from_nanos(50),
        )
        .unwrap()
    else {
        panic!("delivered physical probe must arm exactly once");
    };
    deadline
}

fn make_fresh_probe(fixture: &mut RegisteredAttachFixture, sequence: u64) {
    make_awaiting_probe(fixture, sequence);
    let registered = fixture.slot.registered.as_ref().unwrap();
    let contract = registered.candidate.contract();
    let probe_sequence = ProbeSequence::new(sequence).unwrap();
    let ack = BoundInbound::LegControl(
        fixture
            .owner_b
            .established_leg()
            .bind_received_control_frame(
                LegControlFrame::try_new(
                    contract.generation,
                    LegControlRecord::LegProbeAck {
                        session_id: contract.session_id,
                        leg_epoch_nonce: LegEpochNonce::from(contract.nonce),
                        probe_sequence,
                    },
                    contract.features,
                )
                .unwrap(),
            ),
    );
    fixture
        .slot
        .observe_exact_probe_ack(
            ack,
            SimTime::from_nanos(120),
            std::time::Duration::from_nanos(80),
        )
        .unwrap();
}

fn exact_attach_request(slot: &OwnerStandbySlot, nonce: u8) -> AttachRequest {
    let contract = slot.registered.as_ref().unwrap().candidate.contract();
    AttachRequest::new(
        contract.session_id,
        crate::resumable::LegGeneration::new(contract.generation.get() + 1).unwrap(),
        AttachNonce::new([nonce; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(contract.features.bits(), contract.features.bits()).unwrap(),
    )
}

fn attach_with_wrong_proof(
    fixture: &RegisteredAttachFixture,
) -> crate::owned_upstream::leg::LegBoundFrame {
    let carried = exact_attach_request(&fixture.slot, 0x62);
    let differently_signed = exact_attach_request(&fixture.slot, 0x63);
    let proof = credentials()
        .prove(
            &differently_signed,
            &fixture
                .owner_b
                .established_leg()
                .standby_transport_binding(),
        )
        .unwrap();
    fixture
        .owner_b
        .established_leg()
        .bind_received_frame(carried.to_attach_frame(proof))
}

#[test]
fn pending_probe_is_restored_whole_after_attach_terminal_gate_rejects() {
    let mut fixture = registered_attach_fixture(1);
    make_pending_probe(&mut fixture);
    let before = probe_snapshot(&fixture.slot);
    fixture
        .owner_b
        .established_leg()
        .poison_terminal_transition_for_test();

    assert!(matches!(
        fixture.executor.accept_registered_owner_attach(
            &mut fixture.slot,
            fixture.owner_b.established_leg(),
            fixture.received_attach,
            &mut fixture.owner_queue,
        ),
        Err(OwnerTargetError::RegisteredOwnerAttachTerminalTransition {
            source: LegTerminalTransitionError::Poisoned,
            ..
        })
    ));
    assert_eq!(probe_snapshot(&fixture.slot), before);
}

#[test]
fn awaiting_probe_receipt_is_restored_whole_after_attach_supervisor_rejects() {
    let mut fixture = registered_attach_fixture(2);
    make_awaiting_probe(&mut fixture, 8);
    let before = probe_snapshot(&fixture.slot);
    let received = attach_with_wrong_proof(&fixture);

    assert!(matches!(
        fixture.executor.accept_registered_owner_attach(
            &mut fixture.slot,
            fixture.owner_b.established_leg(),
            received,
            &mut fixture.owner_queue,
        ),
        Err(OwnerTargetError::Supervisor(_))
    ));
    assert_eq!(probe_snapshot(&fixture.slot), before);
}

#[test]
fn armed_probe_receipt_and_deadline_are_restored_after_attach_supervisor_rejects() {
    let mut fixture = registered_attach_fixture(2);
    let deadline = make_armed_probe(&mut fixture, 9);
    assert_eq!(deadline, SimTime::from_nanos(150));
    let before = probe_snapshot(&fixture.slot);
    let received = attach_with_wrong_proof(&fixture);

    assert!(matches!(
        fixture.executor.accept_registered_owner_attach(
            &mut fixture.slot,
            fixture.owner_b.established_leg(),
            received,
            &mut fixture.owner_queue,
        ),
        Err(OwnerTargetError::Supervisor(_))
    ));
    assert_eq!(probe_snapshot(&fixture.slot), before);
}

#[test]
fn fresh_probe_receipt_and_window_are_restored_after_defensive_status_rollback() {
    let mut fixture = registered_attach_fixture(2);
    make_fresh_probe(&mut fixture, 10);
    let before = probe_snapshot(&fixture.slot);

    let gate = fixture
        .slot
        .begin_exact_next_attach(
            fixture.owner_b.established_leg(),
            &fixture.received_attach,
            &mut fixture.owner_queue,
        )
        .unwrap();
    gate.restore(&mut fixture.slot, &mut fixture.owner_queue)
        .expect("a defensive status/resynchronization outcome restores the exact gate");

    assert_eq!(probe_snapshot(&fixture.slot), before);
}

#[test]
fn defensive_restore_error_retains_whole_gate_for_exact_queue_retry() {
    let mut fixture = registered_attach_fixture(2);
    make_armed_probe(&mut fixture, 11);
    let before = probe_snapshot(&fixture.slot);
    let gate = fixture
        .slot
        .begin_exact_next_attach(
            fixture.owner_b.established_leg(),
            &fixture.received_attach,
            &mut fixture.owner_queue,
        )
        .unwrap();
    let wrong_leg =
        crate::owned_upstream::leg::EstablishedLeg::for_authenticated_transport(binding(0xef));
    let mut wrong_queue = LegOutboundQueue::for_leg(&wrong_leg, 2, 4_096).unwrap();

    assert!(gate.restore(&mut fixture.slot, &mut wrong_queue).is_err());
    assert!(fixture.slot.has_retained_attach_restore());
    fixture
        .slot
        .retry_retained_attach_restore(&mut fixture.owner_queue)
        .expect("the fixed retained gate restores only through its exact FIFO");
    assert!(!fixture.slot.has_retained_attach_restore());
    assert_eq!(probe_snapshot(&fixture.slot), before);
}

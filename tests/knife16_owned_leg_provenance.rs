mod shared {
    pub use mini_vpn::shared::*;
}

#[path = "../src/resumable/auth.rs"]
mod auth;
#[path = "../src/resumable/capacity.rs"]
mod capacity;
#[path = "../src/resumable/protocol.rs"]
mod protocol;
#[path = "../src/resumable/session.rs"]
mod session;
#[path = "../src/resumable/tcp.rs"]
mod tcp;

mod resumable {
    pub mod auth {
        pub use crate::auth::*;
    }
    pub mod capacity {
        pub use crate::capacity::*;
    }
    pub mod protocol {
        pub use crate::protocol::*;
    }
    pub mod session {
        pub use crate::session::*;
    }
    pub mod tcp {
        pub use crate::tcp::*;
    }

    pub use auth::*;
    pub use capacity::*;
    pub use protocol::*;
    pub use session::*;
    pub use tcp::*;
}

// `leg.rs` is compiled standalone here to prove its raw capabilities remain
// non-cloneable. Production obtains these gates from the sibling standby
// typestate; the uninhabited shim keeps this provenance crate unable to mint
// any of them while preserving the exact production signatures.
mod standby {
    use crate::leg::{LegSeal, LegTransportEndpoint};
    use crate::resumable::{AttachRequest, AttachTransportBinding};
    use std::sync::Weak;

    pub(super) enum RegisteredPendingAttachAuthority {}

    impl RegisteredPendingAttachAuthority {
        pub(super) fn into_parts(
            self,
        ) -> (
            LegSeal,
            Weak<LegTransportEndpoint>,
            AttachTransportBinding,
            AttachRequest,
        ) {
            match self {}
        }
    }

    pub(super) enum RegisteredAttachResponseGate {}
    pub(super) enum RegisteredCatchUpBeginGate {}
    pub(super) enum RegisteredCatchUpAssemblyGate {}
    pub(super) enum RegisteredCatchUpResponseGate {}
}

#[path = "../src/owned_upstream/leg.rs"]
mod leg;

use bytes::Bytes;
use leg::{
    AttachResponse, AttachedLeg, CaughtUpAttachedLeg, EstablishedLeg, LegBoundFrame,
    LegGenerationStatus, LegProvenanceError, OwnerAttachTransaction, PendingAttach,
    PendingCatchUpAttach,
};
use resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
    AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
    FeatureSet, Frame, LegGeneration, OwnerIdentity, ReceiveBudgetLimits, Record,
    ReplayBudgetLimits, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig, SessionEvent,
    SessionFlowId, SessionId, SessionModel, SessionRole, TcpWindowLimits, TlsExporterBinding,
    VersionRange,
};

// This inference becomes ambiguous and fails compilation if either type ever
// implements Clone.  It keeps the one-response/one-capability contract in the
// type system without adding a test-only dependency.
trait AmbiguousIfClone<Marker> {
    fn marker() {}
}

impl<T: ?Sized> AmbiguousIfClone<()> for T {}
impl<T: Clone> AmbiguousIfClone<u8> for T {}

const _: fn() = || {
    let _ = <PendingAttach as AmbiguousIfClone<_>>::marker;
    let _ = <LegBoundFrame as AmbiguousIfClone<_>>::marker;
    let _ = <AttachedLeg as AmbiguousIfClone<_>>::marker;
    let _ = <OwnerAttachTransaction as AmbiguousIfClone<_>>::marker;
    let _ = <LegGenerationStatus as AmbiguousIfClone<_>>::marker;
    let _ = <PendingCatchUpAttach as AmbiguousIfClone<_>>::marker;
    let _ = <CaughtUpAttachedLeg as AmbiguousIfClone<_>>::marker;
};

fn binding(exporter_byte: u8) -> AttachTransportBinding {
    AttachTransportBinding::new(
        OwnerIdentity::new([0x31; 32]).unwrap(),
        AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
        TlsExporterBinding::new([exporter_byte; 32]).unwrap(),
        DevicePrincipal::new([0x53; 16]).unwrap(),
    )
}

fn request() -> AttachRequest {
    request_for(2, 0x22)
}

fn request_for(generation: u64, nonce_byte: u8) -> AttachRequest {
    AttachRequest::new(
        SessionId::new([0x11; 16]).unwrap(),
        LegGeneration::new(generation).unwrap(),
        AttachNonce::new([nonce_byte; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    )
}

fn credentials() -> AttachCredentials {
    AttachCredentials::new(
        DeviceSecret::new([0x71; 32]).unwrap(),
        ResumeSecret::new([0x72; 32]).unwrap(),
    )
    .unwrap()
}

fn authority(current_generation: u64) -> AttachAuthority {
    AttachAuthority::new(
        request().session_id(),
        LegGeneration::new(current_generation).unwrap(),
        credentials(),
        AttachPolicy::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
            SESSION_PROTOCOL_VERSION,
            0b1111,
        )
        .unwrap(),
    )
}

fn session_config() -> SessionConfig {
    SessionConfig::new(
        4,
        4,
        TcpWindowLimits::new(64, 8).unwrap(),
        TcpWindowLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReceiveBudgetLimits::new(128, 16).unwrap(),
        64,
    )
    .unwrap()
}

fn initial_owner_model() -> SessionModel {
    let authority = authority(1);
    let request = request();
    let transport = binding(0x41);
    let proof = credentials().prove(&request, &transport).unwrap();
    let initial = authority
        .verify_and_commit(&request, &transport, &proof)
        .unwrap();
    SessionModel::new(SessionRole::Owner, session_config(), initial)
}

fn attach(request: AttachRequest, transport: AttachTransportBinding) -> Frame {
    let proof = credentials().prove(&request, &transport).unwrap();
    request.to_attach_frame(proof)
}

fn accepted(request: AttachRequest) -> Frame {
    Frame::try_new(
        request.requested_generation(),
        Record::AttachAccepted {
            session_id: request.session_id(),
            nonce: request.nonce(),
            selected_version: SESSION_PROTOCOL_VERSION,
            features: FeatureSet::new(request.features().offered()),
        },
    )
    .unwrap()
}

fn generation_status(request: AttachRequest, current_generation: u64) -> Frame {
    Frame::try_new(
        LegGeneration::new(current_generation).unwrap(),
        Record::AttachGenerationStatus {
            session_id: request.session_id(),
            requested_generation: request.requested_generation(),
            nonce: request.nonce(),
        },
    )
    .unwrap()
}

#[test]
fn owner_commit_returns_same_leg_attached_capability_and_correlated_acceptance() {
    let authority = authority(2);
    let mut model = initial_owner_model();
    let transport = binding(0x42);
    let established = EstablishedLeg::for_authenticated_transport(transport);
    let request = request_for(3, 0x33);
    let received = established.bind_received_frame(attach(request, transport));

    let OwnerAttachTransaction::Installed {
        attached,
        acceptance,
        recovery,
    } = established
        .transact_owner_attach_for_provenance_test(received, &authority, &mut model)
        .unwrap()
        .unwrap()
    else {
        panic!("exact next generation changed owner attach outcome");
    };

    assert_eq!(attached.generation(), request.requested_generation());
    assert_eq!(attached.nonce(), request.nonce());
    assert_eq!(attached.transport_binding(), transport);
    let SessionEvent::ReplacementAttached { leg } = attached.replacement_attached_event() else {
        panic!("owner must be able to install the attached leg before sending acceptance");
    };
    assert_eq!(leg.generation(), request.requested_generation());
    assert_eq!(acceptance, accepted(request));
    assert_eq!(authority.current_generation(), 3);
    assert_eq!(model.snapshot().generation().get(), 3);
    assert!(matches!(
        recovery.first(),
        Some(resumable::SessionEffect::LegActivated { generation }) if generation.get() == 3
    ));
}

#[test]
fn owner_attach_rejects_a_frame_bound_by_another_identical_leg_before_commit() {
    let authority = authority(2);
    let mut model = initial_owner_model();
    let transport = binding(0x42);
    let leg_a = EstablishedLeg::for_authenticated_transport(transport);
    let leg_b = EstablishedLeg::for_authenticated_transport(transport);
    let request = request_for(3, 0x33);
    let received_on_a = leg_a.bind_received_frame(attach(request, transport));

    assert!(matches!(
        leg_b.transact_owner_attach_for_provenance_test(received_on_a, &authority, &mut model),
        Err(LegProvenanceError::WrongLeg)
    ));
    assert_eq!(authority.current_generation(), 2);
    assert_eq!(model.snapshot().generation().get(), 2);
}

#[test]
fn owner_attach_rejects_data_and_non_attach_records_without_committing() {
    let authority = authority(2);
    let mut model = initial_owner_model();
    let transport = binding(0x42);
    let leg = EstablishedLeg::for_authenticated_transport(transport);

    for record in [
        Record::Data {
            flow_id: SessionFlowId::new(7).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"not-an-attach"),
        },
        Record::Ack {
            flow_id: SessionFlowId::new(8).unwrap(),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(0),
            final_accepted: false,
        },
        Record::AttachAccepted {
            session_id: request().session_id(),
            nonce: request().nonce(),
            selected_version: SESSION_PROTOCOL_VERSION,
            features: FeatureSet::new(0b111),
        },
    ] {
        let received = leg
            .bind_received_frame(Frame::try_new(LegGeneration::new(3).unwrap(), record).unwrap());
        assert!(matches!(
            leg.transact_owner_attach_for_provenance_test(received, &authority, &mut model),
            Err(LegProvenanceError::Rejected)
        ));
        assert_eq!(authority.current_generation(), 2);
        assert_eq!(model.snapshot().generation().get(), 2);
    }
}

#[test]
fn owner_stale_attach_returns_only_correlated_generation_status() {
    let authority = authority(2);
    let mut model = initial_owner_model();
    let transport = binding(0x42);
    let leg = EstablishedLeg::for_authenticated_transport(transport);
    let request = request();
    let received = leg.bind_received_frame(attach(request, transport));

    let OwnerAttachTransaction::Resynchronize { status } = leg
        .transact_owner_attach_for_provenance_test(received, &authority, &mut model)
        .unwrap()
        .unwrap()
    else {
        panic!("stale generation unexpectedly minted an attached capability");
    };

    assert_eq!(status, generation_status(request, 2));
    assert_eq!(authority.current_generation(), 2);
}

#[test]
fn simultaneous_next_generation_attaches_mint_one_capability_and_one_status() {
    let authority = authority(2);
    let mut model_a = initial_owner_model();
    let mut model_b = initial_owner_model();
    let request_a = request_for(3, 0x32);
    let request_b = request_for(3, 0x33);
    let binding_a = binding(0x42);
    let binding_b = binding(0x43);
    let leg_a = EstablishedLeg::for_authenticated_transport(binding_a);
    let leg_b = EstablishedLeg::for_authenticated_transport(binding_b);
    let received_a = leg_a.bind_received_frame(attach(request_a, binding_a));
    let received_b = leg_b.bind_received_frame(attach(request_b, binding_b));

    let (outcome_a, outcome_b) = std::thread::scope(|scope| {
        let attempt_a = scope.spawn(|| {
            leg_a
                .transact_owner_attach_for_provenance_test(received_a, &authority, &mut model_a)
                .unwrap()
                .unwrap()
        });
        let attempt_b = scope.spawn(|| {
            leg_b
                .transact_owner_attach_for_provenance_test(received_b, &authority, &mut model_b)
                .unwrap()
                .unwrap()
        });
        (attempt_a.join().unwrap(), attempt_b.join().unwrap())
    });

    let mut acceptance = None;
    let mut status = None;
    for outcome in [outcome_a, outcome_b] {
        match outcome {
            OwnerAttachTransaction::Installed {
                attached,
                acceptance: frame,
                recovery: _,
            } => {
                assert!(acceptance.replace(frame).is_none());
                assert_eq!(attached.generation(), LegGeneration::new(3).unwrap());
            }
            OwnerAttachTransaction::Resynchronize { status: frame } => {
                assert!(status.replace(frame).is_none());
            }
        }
    }

    let acceptance = acceptance.expect("one concurrent attach must commit");
    let status = status.expect("the concurrent loser must receive status only");
    assert!(
        (acceptance == accepted(request_a) && status == generation_status(request_b, 3))
            || (acceptance == accepted(request_b) && status == generation_status(request_a, 3))
    );
    assert_eq!(authority.current_generation(), 3);
    assert_eq!(
        [
            model_a.snapshot().generation().get(),
            model_b.snapshot().generation().get()
        ]
        .into_iter()
        .filter(|generation| *generation == 3)
        .count(),
        1
    );
}

#[test]
fn owner_attach_outcomes_redact_capability_and_response_state() {
    let transport = binding(0x42);

    let committed_authority = authority(2);
    let mut committed_model = initial_owner_model();
    let committed_leg = EstablishedLeg::for_authenticated_transport(transport);
    let committed_request = request_for(3, 0x33);
    let committed = committed_leg
        .transact_owner_attach_for_provenance_test(
            committed_leg.bind_received_frame(attach(committed_request, transport)),
            &committed_authority,
            &mut committed_model,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        format!("{committed:?}"),
        "OwnerAttachTransaction::Installed([REDACTED])"
    );

    let stale_authority = authority(2);
    let mut stale_model = initial_owner_model();
    let stale_leg = EstablishedLeg::for_authenticated_transport(transport);
    let stale = stale_leg
        .transact_owner_attach_for_provenance_test(
            stale_leg.bind_received_frame(attach(request(), transport)),
            &stale_authority,
            &mut stale_model,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        format!("{stale:?}"),
        "OwnerAttachTransaction::Resynchronize([REDACTED])"
    );
}

#[test]
fn same_authenticated_leg_accepts_its_correlated_response() {
    let leg = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let request = request();
    let pending = leg.begin_attach(request);
    let response = leg.bind_received_frame(accepted(request));

    let AttachResponse::Accepted(attached) = pending.validate_response(response).unwrap() else {
        panic!("correlated ATTACH_ACCEPTED changed response kind");
    };
    assert_eq!(attached.generation(), request.requested_generation());
    assert_eq!(attached.nonce(), request.nonce());
    assert_eq!(attached.transport_binding(), binding(0x42));
    assert_eq!(format!("{attached:?}"), "AttachedLeg([REDACTED])");

    let SessionEvent::ReplacementAttached { leg } = attached.replacement_attached_event() else {
        panic!("attached leg must mint its exact replacement event");
    };
    assert_eq!(leg.generation(), request.requested_generation());
}

#[test]
fn attached_data_plane_rejects_frames_bound_to_another_identical_transport() {
    let leg_a = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let leg_b = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let request = request();
    let AttachResponse::Accepted(attached_a) = leg_a
        .begin_attach(request)
        .validate_response(leg_a.bind_received_frame(accepted(request)))
        .unwrap()
    else {
        panic!("correlated ATTACH_ACCEPTED changed response kind");
    };

    for record in [
        Record::Data {
            flow_id: SessionFlowId::new(7).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"leg-bound-data"),
        },
        Record::Close {
            flow_id: SessionFlowId::new(7).unwrap(),
            direction: Direction::TargetToClient,
            final_offset: ByteOffset::new(14),
        },
        Record::Reset {
            flow_id: SessionFlowId::new(7).unwrap(),
            reason: resumable::ResetReason::TargetFailure,
        },
    ] {
        let frame = Frame::try_new(request.requested_generation(), record).unwrap();
        let encoded = frame.encode().unwrap();
        let from_b = leg_b.bind_received_frame(Frame::decode_owned_exact(encoded.clone()).unwrap());
        assert!(matches!(
            attached_a.accept_frame(from_b),
            Err(LegProvenanceError::WrongLeg)
        ));

        let from_a = leg_a.bind_received_frame(Frame::decode_owned_exact(encoded).unwrap());
        let SessionEvent::PeerFrame { leg, frame } = attached_a.accept_frame(from_a).unwrap()
        else {
            panic!("attached leg must mint only a peer-frame event");
        };
        assert_eq!(leg.generation(), request.requested_generation());
        assert_eq!(frame.leg_generation(), request.requested_generation());
    }
}

#[test]
fn same_authenticated_leg_accepts_its_correlated_generation_status() {
    let leg = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let request = request();
    let pending = leg.begin_attach(request);
    let response = leg.bind_received_frame(generation_status(request, 4));

    let AttachResponse::GenerationStatus(status) = pending.validate_response(response).unwrap()
    else {
        panic!("correlated generation status changed response kind");
    };
    assert_eq!(status.current_generation(), LegGeneration::new(4).unwrap());
    assert_eq!(
        status.requested_generation(),
        request.requested_generation()
    );
    assert_eq!(status.transport_binding(), binding(0x42));
}

#[test]
fn status_and_followup_acceptance_retain_both_exact_leg_seals_for_catch_up() {
    let status_leg = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let followup_leg = EstablishedLeg::for_authenticated_transport(binding(0x43));
    let wrong_followup_leg = EstablishedLeg::for_authenticated_transport(binding(0x43));
    let original = request();
    let pending = status_leg.begin_attach(original);
    let status_response = status_leg.bind_received_frame(generation_status(original, 4));
    let AttachResponse::GenerationStatus(status) =
        pending.validate_response(status_response).unwrap()
    else {
        panic!("correlated generation status changed response kind");
    };

    let pending_catch_up = status
        .begin_catch_up(&followup_leg, AttachNonce::new([0x55; 16]).unwrap())
        .unwrap();
    let followup = pending_catch_up.request();
    assert_eq!(followup.requested_generation().get(), 5);
    let encoded = accepted(followup).encode().unwrap();
    let wrong_response =
        wrong_followup_leg.bind_received_frame(Frame::decode_owned_exact(encoded.clone()).unwrap());
    assert!(matches!(
        pending_catch_up.validate_response(wrong_response),
        Err(LegProvenanceError::WrongLeg)
    ));

    let AttachResponse::GenerationStatus(status) = status_leg
        .begin_attach(original)
        .validate_response(status_leg.bind_received_frame(generation_status(original, 4)))
        .unwrap()
    else {
        panic!("correlated generation status changed response kind");
    };
    let pending_catch_up = status
        .begin_catch_up(&followup_leg, AttachNonce::new([0x56; 16]).unwrap())
        .unwrap();
    let followup = pending_catch_up.request();
    let response = followup_leg.bind_received_frame(accepted(followup));
    let caught_up = pending_catch_up.validate_response(response).unwrap();
    assert_eq!(caught_up.generation().get(), 5);
    assert!(matches!(
        caught_up.replacement_caught_up_event(),
        SessionEvent::ReplacementCaughtUp { .. }
    ));

    let data = Frame::try_new(
        LegGeneration::new(5).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(7).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"catch-up-leg-bound"),
        },
    )
    .unwrap();
    let from_status_leg = status_leg.bind_received_frame(data.clone());
    assert!(matches!(
        caught_up.accept_frame(from_status_leg),
        Err(LegProvenanceError::WrongLeg)
    ));
    let from_followup_leg = followup_leg.bind_received_frame(data);
    assert!(matches!(
        caught_up.accept_frame(from_followup_leg),
        Ok(SessionEvent::PeerFrame { .. })
    ));
}

#[test]
fn byte_identical_responses_from_another_authenticated_leg_are_rejected_first() {
    let leg_a = EstablishedLeg::for_authenticated_transport(binding(0x42));
    // Use the same authenticated facts deliberately: binding equality must
    // not collapse two independent TLS connections into one provenance.
    let leg_b = EstablishedLeg::for_authenticated_transport(binding(0x42));
    let request = request();

    for frame in [accepted(request), generation_status(request, 4)] {
        let encoded = frame.encode().unwrap();
        let decoded = Frame::decode_owned_exact(encoded.clone()).unwrap();
        assert_eq!(decoded.encode().unwrap(), encoded);

        let pending = leg_a.begin_attach(request);
        let response_from_b = leg_b.bind_received_frame(decoded);
        assert!(matches!(
            pending.validate_response(response_from_b),
            Err(LegProvenanceError::WrongLeg)
        ));
    }
}

#[test]
fn typed_leg_values_redact_transport_and_attach_state_from_debug() {
    let leg = EstablishedLeg::for_authenticated_transport(binding(0x42));
    assert_eq!(format!("{leg:?}"), "EstablishedLeg([REDACTED])");

    let request = request();
    let pending = leg.begin_attach(request);
    assert_eq!(format!("{pending:?}"), "PendingAttach([REDACTED])");

    let response = leg.bind_received_frame(accepted(request));
    assert_eq!(format!("{response:?}"), "LegBoundFrame([REDACTED])");
    let outcome = pending.validate_response(response).unwrap();
    assert_eq!(
        format!("{outcome:?}"),
        "AttachResponse::Accepted([REDACTED])"
    );
}

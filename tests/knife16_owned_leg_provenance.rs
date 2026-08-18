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

#[path = "../src/owned_upstream/leg.rs"]
mod leg;

use bytes::Bytes;
use leg::{
    AttachResponse, AttachedLeg, EstablishedLeg, LegBoundFrame, LegProvenanceError, PendingAttach,
};
use resumable::{
    AttachAlpn, AttachNonce, AttachRequest, AttachTransportBinding, ByteOffset, DevicePrincipal,
    Direction, FeatureOffer, FeatureSet, Frame, LegGeneration, OwnerIdentity, Record,
    SESSION_PROTOCOL_VERSION, SessionEvent, SessionFlowId, SessionId, TlsExporterBinding,
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
    AttachRequest::new(
        SessionId::new([0x11; 16]).unwrap(),
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([0x22; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    )
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

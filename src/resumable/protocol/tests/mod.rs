use super::*;

use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

fn generation() -> LegGeneration {
    LegGeneration::new(2).unwrap()
}

fn flow_id() -> SessionFlowId {
    SessionFlowId::new(7).unwrap()
}

fn assert_length_parity(frame: &Frame) {
    assert_eq!(
        frame.encoded_len_checked(),
        frame.encode().map(|encoded| encoded.len())
    );
}

fn assert_error_parity(frame: &Frame, expected: ProtocolError) {
    assert_eq!(frame.encoded_len_checked(), Err(expected.clone()));
    assert_eq!(frame.encode(), Err(expected));
}

#[test]
fn encoded_len_checked_matches_encoding_for_every_session_frame_variant() {
    let session_id = SessionId::new([0x11; 16]).unwrap();
    let nonce = AttachNonce::new([0x22; 16]).unwrap();
    let frames = [
        Frame::try_new(
            generation(),
            Record::Attach {
                session_id,
                nonce,
                min_version: SESSION_PROTOCOL_VERSION,
                max_version: SESSION_PROTOCOL_VERSION,
                offered_features: FeatureSet::new(0b1111),
                required_features: FeatureSet::new(0b0011),
                proof: AttachProof::new([0x33; 32]).unwrap(),
            },
        )
        .unwrap(),
        Frame::try_new(
            generation(),
            Record::AttachAccepted {
                session_id,
                nonce,
                selected_version: SESSION_PROTOCOL_VERSION,
                features: FeatureSet::new(0b1111),
            },
        )
        .unwrap(),
        Frame::try_new(
            generation(),
            Record::AttachGenerationStatus {
                session_id,
                requested_generation: LegGeneration::new(3).unwrap(),
                nonce,
            },
        )
        .unwrap(),
        Frame::try_new(
            generation(),
            Record::Open {
                flow_id: flow_id(),
                target: TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::LOCALHOST,
                    443,
                ))),
            },
        )
        .unwrap(),
        Frame::try_new(
            generation(),
            Record::Data {
                flow_id: flow_id(),
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(9),
                payload: Bytes::from_static(b"payload"),
            },
        )
        .unwrap(),
        Frame::try_new(
            generation(),
            Record::OpenResult {
                flow_id: flow_id(),
                result: OpenResultCode::Opened,
            },
        )
        .unwrap(),
        Frame::try_new(
            generation(),
            Record::Ack {
                flow_id: flow_id(),
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(16),
                final_accepted: true,
            },
        )
        .unwrap(),
        Frame::try_new(
            generation(),
            Record::Close {
                flow_id: flow_id(),
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(16),
            },
        )
        .unwrap(),
        Frame::try_new(
            generation(),
            Record::Reset {
                flow_id: flow_id(),
                reason: ResetReason::TargetFailure,
            },
        )
        .unwrap(),
    ];

    for frame in &frames {
        assert_length_parity(frame);
    }
}

#[test]
fn encoded_len_checked_matches_data_validation_at_wire_boundaries() {
    for payload_len in [1, MAX_DATA_PAYLOAD_BYTES] {
        let frame = Frame::try_new(
            generation(),
            Record::Data {
                flow_id: flow_id(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(0),
                payload: Bytes::from(vec![0x5a; payload_len]),
            },
        )
        .unwrap();
        assert_length_parity(&frame);
    }

    assert_error_parity(
        &Frame::new(
            generation(),
            Record::Data {
                flow_id: flow_id(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(0),
                payload: Bytes::new(),
            },
        ),
        ProtocolError::EmptyData,
    );
    assert_error_parity(
        &Frame::new(
            generation(),
            Record::Data {
                flow_id: flow_id(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(0),
                payload: Bytes::from(vec![0x5a; MAX_DATA_PAYLOAD_BYTES + 1]),
            },
        ),
        ProtocolError::DataTooLarge {
            len: MAX_DATA_PAYLOAD_BYTES + 1,
            max: MAX_DATA_PAYLOAD_BYTES,
        },
    );
    assert_error_parity(
        &Frame::new(
            generation(),
            Record::Data {
                flow_id: flow_id(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(u64::MAX),
                payload: Bytes::from_static(b"x"),
            },
        ),
        ProtocolError::OffsetOverflow,
    );
}

#[test]
fn encoded_len_checked_matches_open_target_validation_and_domain_boundaries() {
    for host_len in [1, MAX_DOMAIN_BYTES] {
        let frame = Frame::try_new(
            generation(),
            Record::Open {
                flow_id: flow_id(),
                target: TargetAddr::DomainPort {
                    host: "a".repeat(host_len),
                    port: 443,
                },
            },
        )
        .unwrap();
        assert_length_parity(&frame);
    }
    let ipv6 = Frame::try_new(
        generation(),
        Record::Open {
            flow_id: flow_id(),
            target: TargetAddr::IpPort(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::LOCALHOST,
                443,
                0,
                0,
            ))),
        },
    )
    .unwrap();
    assert_length_parity(&ipv6);

    for (target, expected) in [
        (
            TargetAddr::DomainPort {
                host: String::new(),
                port: 443,
            },
            ProtocolError::InvalidDomainLength {
                len: 0,
                max: MAX_DOMAIN_BYTES,
            },
        ),
        (
            TargetAddr::DomainPort {
                host: "a".repeat(MAX_DOMAIN_BYTES + 1),
                port: 443,
            },
            ProtocolError::InvalidDomainLength {
                len: MAX_DOMAIN_BYTES + 1,
                max: MAX_DOMAIN_BYTES,
            },
        ),
        (
            TargetAddr::DomainPort {
                host: "a\0b".to_owned(),
                port: 443,
            },
            ProtocolError::InvalidDomainEncoding,
        ),
        (
            TargetAddr::DomainPort {
                host: "a".to_owned(),
                port: 0,
            },
            ProtocolError::ZeroTargetPort,
        ),
        (
            TargetAddr::IpPort(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::LOCALHOST,
                443,
                1,
                0,
            ))),
            ProtocolError::ScopedIpv6Target,
        ),
    ] {
        assert_error_parity(
            &Frame::new(
                generation(),
                Record::Open {
                    flow_id: flow_id(),
                    target,
                },
            ),
            expected,
        );
    }
}

#[test]
fn encoded_len_checked_fails_closed_on_unrepresentable_body_length() {
    assert_eq!(
        checked_encoded_frame_len(RECORD_DATA, usize::MAX),
        Err(ProtocolError::FrameTooLarge)
    );
}

#[test]
fn attach_accepted_capacity_constant_matches_the_codec() {
    let frame = Frame::try_new(
        LegGeneration::new(2).unwrap(),
        Record::AttachAccepted {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            nonce: AttachNonce::new([0x22; 16]).unwrap(),
            selected_version: SESSION_PROTOCOL_VERSION,
            features: FeatureSet::new(0b1111),
        },
    )
    .unwrap();
    assert_eq!(frame.encode().unwrap().len(), ATTACH_ACCEPTED_ENCODED_BYTES);
}

#[test]
fn maximum_encoded_data_frame_capacity_matches_the_codec() {
    let frame = Frame::try_new(
        LegGeneration::new(2).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(7).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from(vec![0x5a; MAX_DATA_PAYLOAD_BYTES]),
        },
    )
    .unwrap();

    assert_eq!(MAX_ENCODED_DATA_FRAME_BYTES, 65_573);
    assert_eq!(frame.encode().unwrap().len(), MAX_ENCODED_DATA_FRAME_BYTES);
}

#[test]
fn standby_register_round_trips_through_the_feature_gated_classifier() {
    let negotiated = FeatureSet::STANDBY_CONTROL_V1;
    let frame = LegControlFrame::try_new(
        LegGeneration::new(2).unwrap(),
        LegControlRecord::StandbyRegister {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            standby_nonce: StandbyNonce::new([0x33; 16]).unwrap(),
            selected_version: SESSION_PROTOCOL_VERSION,
            features: negotiated,
            proof: StandbyProof::new([0x44; 32]).unwrap(),
        },
        negotiated,
    )
    .unwrap();

    let encoded = frame.encode(negotiated).unwrap();
    assert_eq!(encoded.len(), 94);
    assert_eq!(encoded[6], 0x0a);
    assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 82);
    assert_eq!(
        DecodedFrame::decode_exact(&encoded, negotiated).unwrap(),
        DecodedFrame::LegControl(frame)
    );
}

#[test]
fn standby_accepted_round_trips_with_the_exact_registered_contract() {
    let negotiated = FeatureSet::STANDBY_CONTROL_V1;
    let frame = LegControlFrame::try_new(
        LegGeneration::new(2).unwrap(),
        LegControlRecord::StandbyAccepted {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            standby_nonce: StandbyNonce::new([0x33; 16]).unwrap(),
            selected_version: SESSION_PROTOCOL_VERSION,
            features: negotiated,
        },
        negotiated,
    )
    .unwrap();

    let encoded = frame.encode(negotiated).unwrap();
    assert_eq!(encoded.len(), 62);
    assert_eq!(encoded[6], 0x0b);
    assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 50);
    assert_eq!(
        DecodedFrame::decode_exact(&encoded, negotiated).unwrap(),
        DecodedFrame::LegControl(frame)
    );
}

#[test]
fn leg_probe_round_trips_with_nonzero_epoch_and_sequence() {
    let negotiated = FeatureSet::STANDBY_CONTROL_V1;
    let frame = LegControlFrame::try_new(
        LegGeneration::new(2).unwrap(),
        LegControlRecord::LegProbe {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            leg_epoch_nonce: LegEpochNonce::new([0x77; 16]).unwrap(),
            probe_sequence: ProbeSequence::new(9).unwrap(),
        },
        negotiated,
    )
    .unwrap();

    let encoded = frame.encode(negotiated).unwrap();
    assert_eq!(encoded.len(), 60);
    assert_eq!(encoded[6], 0x0c);
    assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 48);
    assert_eq!(
        DecodedFrame::decode_exact(&encoded, negotiated).unwrap(),
        DecodedFrame::LegControl(frame)
    );
}

#[test]
fn leg_probe_ack_round_trips_the_exact_probe_correlation() {
    let negotiated = FeatureSet::STANDBY_CONTROL_V1;
    let frame = LegControlFrame::try_new(
        LegGeneration::new(2).unwrap(),
        LegControlRecord::LegProbeAck {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            leg_epoch_nonce: LegEpochNonce::new([0x77; 16]).unwrap(),
            probe_sequence: ProbeSequence::new(9).unwrap(),
        },
        negotiated,
    )
    .unwrap();

    let encoded = frame.encode(negotiated).unwrap();
    assert_eq!(encoded.len(), 60);
    assert_eq!(encoded[6], 0x0d);
    assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 48);
    assert_eq!(
        DecodedFrame::decode_exact(&encoded, negotiated).unwrap(),
        DecodedFrame::LegControl(frame)
    );
}

#[test]
fn switch_hint_round_trips_the_closed_reverse_stall_evidence() {
    let negotiated = FeatureSet::STANDBY_CONTROL_V1;
    let frame = LegControlFrame::try_new(
        LegGeneration::new(2).unwrap(),
        LegControlRecord::SwitchHint {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            standby_nonce: StandbyNonce::new([0x33; 16]).unwrap(),
            hint_sequence: HintSequence::new(5).unwrap(),
            flow_id: SessionFlowId::new(7).unwrap(),
            direction: Direction::TargetToClient,
            oldest_unacknowledged: ByteOffset::new(1024),
            cause: SwitchHintCause::ReverseApplicationAckStall,
        },
        negotiated,
    )
    .unwrap();

    let encoded = frame.encode(negotiated).unwrap();
    assert_eq!(encoded.len(), 79);
    assert_eq!(encoded[6], 0x0e);
    assert_eq!(u32::from_be_bytes(encoded[8..12].try_into().unwrap()), 67);
    assert_eq!(
        DecodedFrame::decode_exact(&encoded, negotiated).unwrap(),
        DecodedFrame::LegControl(frame)
    );
}

use bytes::Bytes;
use mini_vpn::resumable::{
    AttachNonce, AttachProof, ByteOffset, DecodedFrame, Direction, FRAME_HEADER_BYTES, FeatureSet,
    Frame, HintSequence, LegControlFrame, LegControlRecord, LegEpochNonce, LegGeneration,
    MAX_DATA_PAYLOAD_BYTES, OpenResultCode, ProbeSequence, ProtocolError, Record, ResetReason,
    SESSION_PROTOCOL_VERSION, STANDBY_CONTROL_V1, SessionFlowId, SessionId, StandbyNonce,
    StandbyProof, SwitchHintCause, ValidatedFrameHeader,
};
use mini_vpn::shared::TargetAddr;
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn valid_frame(leg_generation: LegGeneration, record: Record) -> Frame {
    Frame::try_new(leg_generation, record).expect("test record must construct a valid frame")
}

fn valid_leg_control_frame(
    leg_generation: LegGeneration,
    record: LegControlRecord,
) -> LegControlFrame {
    LegControlFrame::try_new(leg_generation, record, STANDBY_CONTROL_V1)
        .expect("test record must construct a valid feature-gated leg-control frame")
}

struct TrackedBacking {
    bytes: Vec<u8>,
    dropped: Arc<AtomicBool>,
}

impl AsRef<[u8]> for TrackedBacking {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for TrackedBacking {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}

#[test]
fn tcp_data_frame_round_trips_through_the_public_protocol_interface() {
    let frame = valid_frame(
        LegGeneration::new(7).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(11).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(4096),
            payload: Bytes::from_static(b"owned bytes"),
        },
    );

    let encoded = frame.encode().unwrap();
    let decoded = Frame::decode_exact(&encoded).unwrap();

    assert_eq!(decoded, frame);
}

#[test]
fn attach_frame_round_trips_without_exposing_its_proof_in_debug_output() {
    let proof = AttachProof::new([0x5a; 32]).unwrap();
    let frame = valid_frame(
        LegGeneration::new(3).unwrap(),
        Record::Attach {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            nonce: AttachNonce::new([0x22; 16]).unwrap(),
            min_version: 1,
            max_version: 1,
            offered_features: FeatureSet::new(0b111),
            required_features: FeatureSet::new(0b001),
            proof,
        },
    );

    assert_eq!(
        Frame::decode_exact(&frame.encode().unwrap()).unwrap(),
        frame
    );
    assert_eq!(format!("{proof:?}"), "AttachProof([REDACTED])");
}

#[test]
fn public_constructor_rejects_invalid_attach_negotiation() {
    let result = Frame::try_new(
        LegGeneration::new(3).unwrap(),
        Record::Attach {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            nonce: AttachNonce::new([0x22; 16]).unwrap(),
            min_version: 0,
            max_version: 1,
            offered_features: FeatureSet::new(0b001),
            required_features: FeatureSet::new(0b010),
            proof: AttachProof::new([0x5a; 32]).unwrap(),
        },
    );

    assert_eq!(
        result,
        Err(ProtocolError::InvalidVersionRange { min: 0, max: 1 })
    );

    let result = Frame::try_new(
        LegGeneration::new(3).unwrap(),
        Record::Attach {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            nonce: AttachNonce::new([0x22; 16]).unwrap(),
            min_version: 1,
            max_version: 1,
            offered_features: FeatureSet::new(0b001),
            required_features: FeatureSet::new(0b010),
            proof: AttachProof::new([0x5a; 32]).unwrap(),
        },
    );
    assert_eq!(
        result,
        Err(ProtocolError::RequiredFeaturesNotOffered {
            offered: 0b001,
            required: 0b010,
        })
    );
}

#[test]
fn public_constructor_rejects_a_zero_selected_version() {
    let result = Frame::try_new(
        LegGeneration::new(3).unwrap(),
        Record::AttachAccepted {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            nonce: AttachNonce::new([0x22; 16]).unwrap(),
            selected_version: 0,
            features: FeatureSet::new(0),
        },
    );

    assert_eq!(
        result,
        Err(ProtocolError::InvalidVersionRange { min: 0, max: 0 })
    );
}

#[test]
fn declared_oversized_frame_is_rejected_from_the_header_without_buffering_its_body() {
    let frame = valid_frame(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"x"),
        },
    );
    let mut header = frame.encode().unwrap()[..FRAME_HEADER_BYTES].to_vec();
    header[8..12].copy_from_slice(&u32::MAX.to_be_bytes());

    assert_eq!(
        ValidatedFrameHeader::decode(&header),
        Err(ProtocolError::FrameTooLarge)
    );
}

#[test]
fn tcp_open_frame_round_trips_a_validated_target() {
    let target = TargetAddr::parse("example.com:443").unwrap();
    let frame = valid_frame(
        LegGeneration::new(4).unwrap(),
        Record::Open {
            flow_id: SessionFlowId::new(19).unwrap(),
            target,
        },
    );

    assert_eq!(
        Frame::decode_exact(&frame.encode().unwrap()).unwrap(),
        frame
    );
}

#[test]
fn public_open_constructor_rebuilds_a_small_domain_from_huge_string_backing() {
    const HOST_BACKING_BYTES: usize = 64 * 1024 * 1024;
    let mut host = String::with_capacity(HOST_BACKING_BYTES);
    host.push('x');
    let original_ptr = host.as_ptr();
    assert!(host.capacity() >= HOST_BACKING_BYTES);

    let frame = Frame::try_new(
        LegGeneration::new(4).unwrap(),
        Record::Open {
            flow_id: SessionFlowId::new(19).unwrap(),
            target: TargetAddr::DomainPort { host, port: 443 },
        },
    )
    .unwrap();

    let Record::Open {
        target: TargetAddr::DomainPort { host, port },
        ..
    } = frame.record()
    else {
        panic!("constructed OPEN changed target kind");
    };
    assert_eq!(host, "x");
    assert_eq!(*port, 443);
    assert_eq!(host.capacity(), host.len());
    assert_ne!(host.as_ptr(), original_ptr);
}

#[test]
fn owned_data_decode_does_not_retain_an_oversized_uncharged_backing() {
    let frame = valid_frame(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(2).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(8),
            payload: Bytes::from_static(b"zero-copy payload"),
        },
    );
    let encoded = frame.encode().unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let prefix_len = 1024 * 1024;
    let mut oversized = vec![0xa5; prefix_len];
    oversized.extend_from_slice(&encoded);
    let owner = Bytes::from_owner(TrackedBacking {
        bytes: oversized,
        dropped: dropped.clone(),
    });
    let exact_frame = owner.slice(prefix_len..);
    drop(owner);

    let decoded = Frame::decode_owned_exact(exact_frame).unwrap();
    let Record::Data { payload, .. } = decoded.record() else {
        panic!("decoded DATA changed record kind");
    };

    assert_eq!(payload, &Bytes::from_static(b"zero-copy payload"));
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn owned_open_decode_releases_transport_backing_and_returns_an_exact_domain() {
    let frame = valid_frame(
        LegGeneration::new(1).unwrap(),
        Record::Open {
            flow_id: SessionFlowId::new(2).unwrap(),
            target: TargetAddr::DomainPort {
                host: "x".to_owned(),
                port: 443,
            },
        },
    );
    let encoded = frame.encode().unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let prefix_len = 1024 * 1024;
    let mut oversized = vec![0xa5; prefix_len];
    oversized.extend_from_slice(&encoded);
    let owner = Bytes::from_owner(TrackedBacking {
        bytes: oversized,
        dropped: dropped.clone(),
    });
    let exact_frame = owner.slice(prefix_len..);
    drop(owner);

    let decoded = Frame::decode_owned_exact(exact_frame).unwrap();
    let Record::Open {
        target: TargetAddr::DomainPort { host, port },
        ..
    } = decoded.record()
    else {
        panic!("decoded OPEN changed target kind");
    };

    assert_eq!(host, "x");
    assert_eq!(*port, 443);
    assert_eq!(host.capacity(), host.len());
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn public_data_constructor_does_not_retain_an_oversized_uncharged_backing() {
    let dropped = Arc::new(AtomicBool::new(false));
    let prefix_len = 1024 * 1024;
    let mut oversized = vec![0xa5; prefix_len];
    oversized.extend_from_slice(b"exact payload");
    let owner = Bytes::from_owner(TrackedBacking {
        bytes: oversized,
        dropped: dropped.clone(),
    });
    let payload = owner.slice(prefix_len..);
    drop(owner);
    assert!(!dropped.load(Ordering::Acquire));

    let frame = Frame::try_new(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(2).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload,
        },
    )
    .unwrap();

    let Record::Data { payload, .. } = frame.record() else {
        panic!("constructed DATA changed record kind");
    };
    assert_eq!(payload, &Bytes::from_static(b"exact payload"));
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn public_data_constructor_rejects_invalid_payload_before_retaining_its_owner() {
    let leg = LegGeneration::new(1).unwrap();
    let flow_id = SessionFlowId::new(2).unwrap();
    let dropped = Arc::new(AtomicBool::new(false));
    let prefix_len = 1024 * 1024;
    let mut oversized = vec![0xa5; prefix_len];
    oversized.push(0x42);
    let owner = Bytes::from_owner(TrackedBacking {
        bytes: oversized,
        dropped: dropped.clone(),
    });
    let payload = owner.slice(prefix_len..);
    drop(owner);

    let overflow = Frame::try_new(
        leg,
        Record::Data {
            flow_id,
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(u64::MAX),
            payload,
        },
    );
    assert_eq!(overflow, Err(ProtocolError::OffsetOverflow));
    assert!(dropped.load(Ordering::Acquire));

    assert_eq!(
        Frame::try_new(
            leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::new(),
            },
        ),
        Err(ProtocolError::EmptyData)
    );

    let too_large = MAX_DATA_PAYLOAD_BYTES + 1;
    assert_eq!(
        Frame::try_new(
            leg,
            Record::Data {
                flow_id,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from(vec![0; too_large]),
            },
        ),
        Err(ProtocolError::DataTooLarge {
            len: too_large,
            max: MAX_DATA_PAYLOAD_BYTES,
        })
    );
}

#[test]
fn streaming_decoder_uses_the_validated_header_to_admit_an_owned_body() {
    let frame = valid_frame(
        LegGeneration::new(2).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(3).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"stream body"),
        },
    );
    let encoded = frame.encode().unwrap();
    let header = ValidatedFrameHeader::decode(&encoded[..FRAME_HEADER_BYTES]).unwrap();
    let body = encoded.slice(FRAME_HEADER_BYTES..);
    let decoded = header.decode_body(body).unwrap();
    let Record::Data { payload, .. } = decoded.record() else {
        panic!("validated DATA body changed record kind");
    };

    assert_eq!(payload, &Bytes::from_static(b"stream body"));
}

#[test]
fn open_rejects_a_domain_that_cannot_be_represented_exactly() {
    let frame = Frame::try_new(
        LegGeneration::new(1).unwrap(),
        Record::Open {
            flow_id: SessionFlowId::new(2).unwrap(),
            target: TargetAddr::DomainPort {
                host: "x".repeat(254),
                port: 443,
            },
        },
    );

    assert_eq!(
        frame,
        Err(ProtocolError::InvalidDomainLength { len: 254, max: 253 })
    );
}

#[test]
fn open_preserves_bounded_utf8_resolver_input_without_normalization() {
    let host = "例子.example.";
    let frame = valid_frame(
        LegGeneration::new(1).unwrap(),
        Record::Open {
            flow_id: SessionFlowId::new(2).unwrap(),
            target: TargetAddr::DomainPort {
                host: host.to_owned(),
                port: 443,
            },
        },
    );

    let decoded = Frame::decode_exact(&frame.encode().unwrap()).unwrap();
    assert_eq!(decoded, frame);

    let nul = Frame::try_new(
        LegGeneration::new(1).unwrap(),
        Record::Open {
            flow_id: SessionFlowId::new(3).unwrap(),
            target: TargetAddr::DomainPort {
                host: "example.com\0ignored".to_owned(),
                port: 443,
            },
        },
    );
    assert_eq!(nul, Err(ProtocolError::InvalidDomainEncoding));
}

#[test]
fn public_constructor_rejects_zero_port_and_scoped_ipv6_targets() {
    let leg = LegGeneration::new(1).unwrap();
    let flow_id = SessionFlowId::new(2).unwrap();

    assert_eq!(
        Frame::try_new(
            leg,
            Record::Open {
                flow_id,
                target: TargetAddr::DomainPort {
                    host: "example.com".to_owned(),
                    port: 0,
                },
            },
        ),
        Err(ProtocolError::ZeroTargetPort)
    );

    let scoped = SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 443, 0, 7));
    assert_eq!(
        Frame::try_new(
            leg,
            Record::Open {
                flow_id,
                target: TargetAddr::IpPort(scoped),
            },
        ),
        Err(ProtocolError::ScopedIpv6Target)
    );
}

#[test]
fn tcp_control_records_round_trip_final_acceptance_and_stable_outcomes() {
    let flow_id = SessionFlowId::new(42).unwrap();
    let records = [
        Record::AttachAccepted {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            nonce: AttachNonce::new([0x22; 16]).unwrap(),
            selected_version: 1,
            features: FeatureSet::new(0b101),
        },
        Record::AttachGenerationStatus {
            session_id: SessionId::new([0x11; 16]).unwrap(),
            requested_generation: LegGeneration::new(8).unwrap(),
            nonce: AttachNonce::new([0x22; 16]).unwrap(),
        },
        Record::OpenResult {
            flow_id,
            result: OpenResultCode::TargetRefused,
        },
        Record::Ack {
            flow_id,
            direction: Direction::TargetToClient,
            next_accepted: ByteOffset::new(8192),
            final_accepted: false,
        },
        Record::Close {
            flow_id,
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(4096),
        },
        Record::Reset {
            flow_id,
            reason: ResetReason::ProtocolViolation,
        },
    ];

    for record in records {
        let frame = valid_frame(LegGeneration::new(9).unwrap(), record);
        assert_eq!(
            Frame::decode_owned_exact(frame.encode().unwrap()).unwrap(),
            frame
        );
    }
}

#[test]
fn frame_debug_redacts_authentication_targets_and_application_payload() {
    let data = valid_frame(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"PRIVATE_PAYLOAD_SENTINEL"),
        },
    );
    let open = valid_frame(
        LegGeneration::new(1).unwrap(),
        Record::Open {
            flow_id: SessionFlowId::new(1).unwrap(),
            target: TargetAddr::DomainPort {
                host: "private-target-sentinel.example".to_owned(),
                port: 443,
            },
        },
    );
    let attach = valid_frame(
        LegGeneration::new(2).unwrap(),
        Record::Attach {
            session_id: SessionId::new([0x53; 16]).unwrap(),
            nonce: AttachNonce::new([0x4e; 16]).unwrap(),
            min_version: 1,
            max_version: 1,
            offered_features: FeatureSet::new(1),
            required_features: FeatureSet::new(1),
            proof: AttachProof::new([0x50; 32]).unwrap(),
        },
    );
    let generation_status = valid_frame(
        LegGeneration::new(2).unwrap(),
        Record::AttachGenerationStatus {
            session_id: SessionId::new([0x53; 16]).unwrap(),
            requested_generation: LegGeneration::new(3).unwrap(),
            nonce: AttachNonce::new([0x4e; 16]).unwrap(),
        },
    );
    let accepted = valid_frame(
        LegGeneration::new(2).unwrap(),
        Record::AttachAccepted {
            session_id: SessionId::new([0x53; 16]).unwrap(),
            nonce: AttachNonce::new([0x4e; 16]).unwrap(),
            selected_version: 1,
            features: FeatureSet::new(1),
        },
    );

    let debug = format!("{data:?} {open:?} {attach:?} {generation_status:?} {accepted:?}");
    assert!(!debug.contains("PRIVATE_PAYLOAD_SENTINEL"));
    assert!(!debug.contains("private-target-sentinel"));
    assert!(!debug.contains("session_id"));
    assert!(!debug.contains("nonce"));
    assert!(!debug.contains("proof"));
    assert!(debug.contains("payload_len: 24"));
    assert_eq!(
        format!("{:?}", SessionId::new([0x53; 16]).unwrap()),
        "SessionId([REDACTED])"
    );
    assert_eq!(
        format!("{:?}", AttachNonce::new([0x4e; 16]).unwrap()),
        "AttachNonce([REDACTED])"
    );
}

#[test]
fn v1_wire_golden_vectors_freeze_every_record_and_target_encoding() {
    let leg = LegGeneration::new(0x0102_0304_0506_0708).unwrap();
    let flow = SessionFlowId::new(9).unwrap();
    let records = [
        (
            "attach",
            Record::Attach {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                nonce: AttachNonce::new([0x22; 16]).unwrap(),
                min_version: 1,
                max_version: 2,
                offered_features: FeatureSet::new(5),
                required_features: FeatureSet::new(1),
                proof: AttachProof::new([0x33; 32]).unwrap(),
            },
        ),
        (
            "attach_accepted",
            Record::AttachAccepted {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                nonce: AttachNonce::new([0x22; 16]).unwrap(),
                selected_version: 1,
                features: FeatureSet::new(5),
            },
        ),
        (
            "attach_generation_status",
            Record::AttachGenerationStatus {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                requested_generation: LegGeneration::new(9).unwrap(),
                nonce: AttachNonce::new([0x22; 16]).unwrap(),
            },
        ),
        (
            "open_domain",
            Record::Open {
                flow_id: flow,
                target: TargetAddr::parse("example.com:443").unwrap(),
            },
        ),
        (
            "open_ipv4",
            Record::Open {
                flow_id: flow,
                target: TargetAddr::parse("192.0.2.1:80").unwrap(),
            },
        ),
        (
            "open_ipv6",
            Record::Open {
                flow_id: flow,
                target: TargetAddr::parse("[2001:db8::1]:8443").unwrap(),
            },
        ),
        (
            "data",
            Record::Data {
                flow_id: flow,
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(16),
                payload: Bytes::from_static(b"abc"),
            },
        ),
        (
            "open_result",
            Record::OpenResult {
                flow_id: flow,
                result: OpenResultCode::TargetRefused,
            },
        ),
        (
            "ack",
            Record::Ack {
                flow_id: flow,
                direction: Direction::ClientToTarget,
                next_accepted: ByteOffset::new(3),
                final_accepted: true,
            },
        ),
        (
            "close",
            Record::Close {
                flow_id: flow,
                direction: Direction::TargetToClient,
                final_offset: ByteOffset::new(3),
            },
        ),
        (
            "reset",
            Record::Reset {
                flow_id: flow,
                reason: ResetReason::ProtocolViolation,
            },
        ),
    ];

    let expected_hex = [
        "4d56504e000101000000005c0102030405060708111111111111111111111111111111112222222222222222222222222222222200010002000000000000000500000000000000013333333333333333333333333333333333333333333333333333333333333333",
        "4d56504e00010700000000320102030405060708111111111111111111111111111111112222222222222222222222222222222200010000000000000005",
        "4d56504e0001090000000030010203040506070811111111111111111111111111111111000000000000000922222222222222222222222222222222",
        "4d56504e000102000000001f010203040506070800000000000000090001bb0b6578616d706c652e636f6d",
        "4d56504e000102000000001701020304050607080000000000000009010050c0000201",
        "4d56504e0001020000000023010203040506070800000000000000090220fb20010db8000000000000000000000001",
        "4d56504e000103000000001c01020304050607080000000000000009010000000000000010616263",
        "4d56504e0001080000000012010203040506070800000000000000090001",
        "4d56504e000104000000001a0102030405060708000000000000000900000000000000000301",
        "4d56504e000105000000001901020304050607080000000000000009010000000000000003",
        "4d56504e0001060000000012010203040506070800000000000000090001",
    ];

    for ((name, record), expected_hex) in records.into_iter().zip(expected_hex) {
        let frame = valid_frame(leg, record);
        let expected = decode_hex(expected_hex);
        assert_eq!(frame.encode().unwrap().as_ref(), expected, "encode {name}");
        assert_eq!(
            Frame::decode_exact(&expected).unwrap(),
            frame,
            "decode {name}"
        );
    }
}

#[test]
fn standby_control_v1_wire_golden_vectors_freeze_all_five_leg_control_records() {
    assert_eq!(STANDBY_CONTROL_V1.bits(), 0x10);
    assert_eq!(FeatureSet::STANDBY_CONTROL_V1, STANDBY_CONTROL_V1);
    let leg = LegGeneration::new(0x0102_0304_0506_0708).unwrap();
    let session_id = SessionId::new([0x11; 16]).unwrap();
    let standby_nonce = StandbyNonce::new([0x22; 16]).unwrap();
    let leg_epoch_nonce = LegEpochNonce::new([0x44; 16]).unwrap();
    let records = [
        (
            "standby_register",
            valid_leg_control_frame(
                leg,
                LegControlRecord::StandbyRegister {
                    session_id,
                    standby_nonce,
                    selected_version: SESSION_PROTOCOL_VERSION,
                    features: STANDBY_CONTROL_V1,
                    proof: StandbyProof::new([0x33; 32]).unwrap(),
                },
            ),
        ),
        (
            "standby_accepted",
            valid_leg_control_frame(
                leg,
                LegControlRecord::StandbyAccepted {
                    session_id,
                    standby_nonce,
                    selected_version: SESSION_PROTOCOL_VERSION,
                    features: STANDBY_CONTROL_V1,
                },
            ),
        ),
        (
            "leg_probe",
            valid_leg_control_frame(
                leg,
                LegControlRecord::LegProbe {
                    session_id,
                    leg_epoch_nonce,
                    probe_sequence: ProbeSequence::new(5).unwrap(),
                },
            ),
        ),
        (
            "leg_probe_ack",
            valid_leg_control_frame(
                leg,
                LegControlRecord::LegProbeAck {
                    session_id,
                    leg_epoch_nonce,
                    probe_sequence: ProbeSequence::new(5).unwrap(),
                },
            ),
        ),
        (
            "switch_hint",
            valid_leg_control_frame(
                leg,
                LegControlRecord::SwitchHint {
                    session_id,
                    standby_nonce,
                    hint_sequence: HintSequence::new(6).unwrap(),
                    flow_id: SessionFlowId::new(9).unwrap(),
                    direction: Direction::TargetToClient,
                    oldest_unacknowledged: ByteOffset::new(16),
                    cause: SwitchHintCause::ReverseApplicationAckStall,
                },
            ),
        ),
    ];
    let expected_hex = [
        "4d56504e00010a000000005201020304050607081111111111111111111111111111111122222222222222222222222222222222000100000000000000103333333333333333333333333333333333333333333333333333333333333333",
        "4d56504e00010b00000000320102030405060708111111111111111111111111111111112222222222222222222222222222222200010000000000000010",
        "4d56504e00010c0000000030010203040506070811111111111111111111111111111111444444444444444444444444444444440000000000000005",
        "4d56504e00010d0000000030010203040506070811111111111111111111111111111111444444444444444444444444444444440000000000000005",
        "4d56504e00010e000000004301020304050607081111111111111111111111111111111122222222222222222222222222222222000000000000000600000000000000090100000000000000100001",
    ];

    for ((name, frame), expected_hex) in records.into_iter().zip(expected_hex) {
        let expected = decode_hex(expected_hex);
        assert_eq!(
            frame.encode(STANDBY_CONTROL_V1).unwrap().as_ref(),
            expected,
            "encode {name}"
        );
        assert_eq!(
            DecodedFrame::decode_exact(&expected, STANDBY_CONTROL_V1).unwrap(),
            DecodedFrame::LegControl(frame),
            "decode {name}"
        );
    }
}

#[test]
fn standby_control_requires_negotiation_and_is_classified_away_from_session_frames() {
    let leg = LegGeneration::new(2).unwrap();
    let record = LegControlRecord::StandbyRegister {
        session_id: SessionId::new([0x11; 16]).unwrap(),
        standby_nonce: StandbyNonce::new([0x22; 16]).unwrap(),
        selected_version: SESSION_PROTOCOL_VERSION,
        features: STANDBY_CONTROL_V1,
        proof: StandbyProof::new([0x33; 32]).unwrap(),
    };
    assert_eq!(
        LegControlFrame::try_new(leg, record.clone(), FeatureSet::new(0)),
        Err(ProtocolError::StandbyControlNotNegotiated { negotiated: 0 })
    );
    let frame = valid_leg_control_frame(leg, record);
    assert_eq!(
        frame.encode(FeatureSet::new(0)),
        Err(ProtocolError::StandbyControlNotNegotiated { negotiated: 0 })
    );
    let encoded = frame.encode(STANDBY_CONTROL_V1).unwrap();
    assert_eq!(
        DecodedFrame::decode_owned_exact(encoded.clone(), STANDBY_CONTROL_V1).unwrap(),
        DecodedFrame::LegControl(frame.clone())
    );
    assert_eq!(
        ValidatedFrameHeader::decode(&encoded),
        Err(ProtocolError::StandbyControlNotNegotiated { negotiated: 0 })
    );
    assert_eq!(
        Frame::decode_exact(&encoded),
        Err(ProtocolError::StandbyControlNotNegotiated { negotiated: 0 })
    );
    assert_eq!(
        DecodedFrame::decode_exact(&encoded, FeatureSet::new(0)),
        Err(ProtocolError::StandbyControlNotNegotiated { negotiated: 0 })
    );

    let header = ValidatedFrameHeader::decode_with_features(&encoded, STANDBY_CONTROL_V1).unwrap();
    let body = Bytes::copy_from_slice(&encoded[FRAME_HEADER_BYTES..]);
    assert_eq!(
        header.decode_body(body.clone()),
        Err(ProtocolError::LegControlRequiresClassification)
    );
    assert!(matches!(
        header.decode_classified_body(body).unwrap(),
        DecodedFrame::LegControl(_)
    ));

    let session = valid_frame(
        leg,
        Record::Reset {
            flow_id: SessionFlowId::new(1).unwrap(),
            reason: ResetReason::Unspecified,
        },
    );
    assert!(matches!(
        DecodedFrame::decode_exact(&session.encode().unwrap(), STANDBY_CONTROL_V1).unwrap(),
        DecodedFrame::Session(decoded) if decoded == session
    ));

    assert_eq!(
        LegControlFrame::try_new(
            leg,
            LegControlRecord::StandbyAccepted {
                session_id: SessionId::new([0x11; 16]).unwrap(),
                standby_nonce: StandbyNonce::new([0x22; 16]).unwrap(),
                selected_version: SESSION_PROTOCOL_VERSION,
                features: FeatureSet::new(0),
            },
            STANDBY_CONTROL_V1,
        ),
        Err(ProtocolError::StandbyControlFeatureMissing { features: 0 })
    );
}

#[test]
fn leg_control_envelope_is_the_only_active_generation_authority() {
    let leg = LegGeneration::new(0x0102_0304_0506_0708).unwrap();
    let session_id = SessionId::new([0x11; 16]).unwrap();
    let standby_nonce = StandbyNonce::new([0x22; 16]).unwrap();
    let frame = valid_leg_control_frame(
        leg,
        LegControlRecord::StandbyAccepted {
            session_id,
            standby_nonce,
            selected_version: SESSION_PROTOCOL_VERSION,
            features: STANDBY_CONTROL_V1,
        },
    );
    let encoded = frame.encode(STANDBY_CONTROL_V1).unwrap();

    assert_eq!(frame.leg_generation(), leg);
    assert_eq!(&encoded[12..20], &leg.get().to_be_bytes());
    assert_eq!(&encoded[20..36], session_id.as_bytes());
    assert_eq!(&encoded[36..52], standby_nonce.as_bytes());
    assert!(matches!(
        frame.record(),
        LegControlRecord::StandbyAccepted {
            session_id: decoded_session,
            standby_nonce: decoded_nonce,
            ..
        } if *decoded_session == session_id && *decoded_nonce == standby_nonce
    ));
}

#[test]
fn malformed_standby_control_records_fail_closed_at_every_parser_boundary() {
    let leg = LegGeneration::new(2).unwrap();
    let session_id = SessionId::new([0x11; 16]).unwrap();
    let standby_nonce = StandbyNonce::new([0x22; 16]).unwrap();
    let leg_epoch_nonce = LegEpochNonce::new([0x44; 16]).unwrap();
    let frames = [
        (
            0x0a,
            82usize,
            valid_leg_control_frame(
                leg,
                LegControlRecord::StandbyRegister {
                    session_id,
                    standby_nonce,
                    selected_version: SESSION_PROTOCOL_VERSION,
                    features: STANDBY_CONTROL_V1,
                    proof: StandbyProof::new([0x33; 32]).unwrap(),
                },
            ),
        ),
        (
            0x0b,
            50,
            valid_leg_control_frame(
                leg,
                LegControlRecord::StandbyAccepted {
                    session_id,
                    standby_nonce,
                    selected_version: SESSION_PROTOCOL_VERSION,
                    features: STANDBY_CONTROL_V1,
                },
            ),
        ),
        (
            0x0c,
            48,
            valid_leg_control_frame(
                leg,
                LegControlRecord::LegProbe {
                    session_id,
                    leg_epoch_nonce,
                    probe_sequence: ProbeSequence::new(5).unwrap(),
                },
            ),
        ),
        (
            0x0d,
            48,
            valid_leg_control_frame(
                leg,
                LegControlRecord::LegProbeAck {
                    session_id,
                    leg_epoch_nonce,
                    probe_sequence: ProbeSequence::new(5).unwrap(),
                },
            ),
        ),
        (
            0x0e,
            67,
            valid_leg_control_frame(
                leg,
                LegControlRecord::SwitchHint {
                    session_id,
                    standby_nonce,
                    hint_sequence: HintSequence::new(6).unwrap(),
                    flow_id: SessionFlowId::new(9).unwrap(),
                    direction: Direction::TargetToClient,
                    oldest_unacknowledged: ByteOffset::new(16),
                    cause: SwitchHintCause::ReverseApplicationAckStall,
                },
            ),
        ),
    ];

    for (record_type, body_len, frame) in frames.clone() {
        let encoded = frame.encode(STANDBY_CONTROL_V1).unwrap();
        for (declared, expected) in [
            (
                body_len - 1,
                ProtocolError::InvalidRecordLength {
                    record_type,
                    len: body_len - 1,
                },
            ),
            (body_len + 1, ProtocolError::FrameTooLarge),
        ] {
            let mut bad = encoded[..FRAME_HEADER_BYTES].to_vec();
            bad[8..12].copy_from_slice(&(declared as u32).to_be_bytes());
            assert_eq!(
                ValidatedFrameHeader::decode_with_features(&bad, STANDBY_CONTROL_V1),
                Err(expected)
            );
        }

        let mut truncated = encoded.to_vec();
        truncated.pop();
        assert!(matches!(
            DecodedFrame::decode_exact(&truncated, STANDBY_CONTROL_V1),
            Err(ProtocolError::Truncated { .. })
        ));
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(matches!(
            DecodedFrame::decode_exact(&trailing, STANDBY_CONTROL_V1),
            Err(ProtocolError::TrailingBytes { .. })
        ));
    }

    let mut unknown = frames[0].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    unknown[6] = 0x0f;
    assert_eq!(
        ValidatedFrameHeader::decode_with_features(&unknown, STANDBY_CONTROL_V1),
        Err(ProtocolError::UnknownRecordType(0x0f))
    );

    let mut register = frames[0].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    register[12..20].fill(0);
    assert_eq!(
        DecodedFrame::decode_exact(&register, STANDBY_CONTROL_V1),
        Err(ProtocolError::ZeroLegGeneration)
    );
    let mut register = frames[0].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    register[20..36].fill(0);
    assert_eq!(
        DecodedFrame::decode_exact(&register, STANDBY_CONTROL_V1),
        Err(ProtocolError::ZeroSessionId)
    );
    let mut register = frames[0].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    register[36..52].fill(0);
    assert_eq!(
        DecodedFrame::decode_exact(&register, STANDBY_CONTROL_V1),
        Err(ProtocolError::ZeroStandbyNonce)
    );
    let mut register = frames[0].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    register[52..54].fill(0);
    assert_eq!(
        DecodedFrame::decode_exact(&register, STANDBY_CONTROL_V1),
        Err(ProtocolError::InvalidVersionRange { min: 0, max: 0 })
    );
    let mut register = frames[0].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    register[54..62].fill(0);
    assert_eq!(
        DecodedFrame::decode_exact(&register, STANDBY_CONTROL_V1),
        Err(ProtocolError::StandbyControlFeatureMissing { features: 0 })
    );
    let mut register = frames[0].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    register[62..94].fill(0);
    assert_eq!(
        DecodedFrame::decode_exact(&register, STANDBY_CONTROL_V1),
        Err(ProtocolError::ZeroStandbyProof)
    );

    let mut probe = frames[2].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    probe[36..52].fill(0);
    assert_eq!(
        DecodedFrame::decode_exact(&probe, STANDBY_CONTROL_V1),
        Err(ProtocolError::ZeroLegEpochNonce)
    );
    let mut probe = frames[2].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    probe[52..60].fill(0);
    assert_eq!(
        DecodedFrame::decode_exact(&probe, STANDBY_CONTROL_V1),
        Err(ProtocolError::ZeroProbeSequence)
    );

    let mut hint = frames[4].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    hint[52..60].fill(0);
    assert_eq!(
        DecodedFrame::decode_exact(&hint, STANDBY_CONTROL_V1),
        Err(ProtocolError::ZeroHintSequence)
    );
    let mut hint = frames[4].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    hint[60..68].fill(0);
    assert_eq!(
        DecodedFrame::decode_exact(&hint, STANDBY_CONTROL_V1),
        Err(ProtocolError::ZeroFlowId)
    );
    let mut hint = frames[4].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    hint[68] = 0;
    assert_eq!(
        DecodedFrame::decode_exact(&hint, STANDBY_CONTROL_V1),
        Err(ProtocolError::InvalidSwitchHintDirection(
            Direction::ClientToTarget
        ))
    );
    let mut hint = frames[4].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    hint[68] = 2;
    assert_eq!(
        DecodedFrame::decode_exact(&hint, STANDBY_CONTROL_V1),
        Err(ProtocolError::InvalidDirection(2))
    );
    let mut hint = frames[4].2.encode(STANDBY_CONTROL_V1).unwrap().to_vec();
    hint[77..79].copy_from_slice(&u16::MAX.to_be_bytes());
    assert_eq!(
        DecodedFrame::decode_exact(&hint, STANDBY_CONTROL_V1),
        Err(ProtocolError::InvalidSwitchHintCause(u16::MAX))
    );
}

#[test]
fn standby_control_nonce_and_sequence_types_reject_zero_and_checked_overflow() {
    assert_eq!(
        StandbyNonce::new([0; 16]),
        Err(ProtocolError::ZeroStandbyNonce)
    );
    assert_eq!(
        LegEpochNonce::new([0; 16]),
        Err(ProtocolError::ZeroLegEpochNonce)
    );
    assert_eq!(ProbeSequence::new(0), Err(ProtocolError::ZeroProbeSequence));
    assert_eq!(HintSequence::new(0), Err(ProtocolError::ZeroHintSequence));
    assert_eq!(
        StandbyProof::new([0; 32]),
        Err(ProtocolError::ZeroStandbyProof)
    );
    assert_eq!(
        ProbeSequence::new(u64::MAX).unwrap().checked_next(),
        Err(ProtocolError::ProbeSequenceOverflow)
    );
    assert_eq!(
        HintSequence::new(u64::MAX).unwrap().checked_next(),
        Err(ProtocolError::HintSequenceOverflow)
    );

    let attach_nonce = AttachNonce::new([0x55; 16]).unwrap();
    let standby_nonce = StandbyNonce::new([0x66; 16]).unwrap();
    assert_eq!(
        LegEpochNonce::from(attach_nonce).as_bytes(),
        attach_nonce.as_bytes()
    );
    assert_eq!(
        LegEpochNonce::from(standby_nonce).as_bytes(),
        standby_nonce.as_bytes()
    );
}

#[test]
fn standby_control_debug_redacts_proof_and_nonce_material() {
    let proof = StandbyProof::new([0x5a; 32]).unwrap();
    let standby_nonce = StandbyNonce::new([0x4e; 16]).unwrap();
    let leg_epoch_nonce = LegEpochNonce::from(standby_nonce);
    let frame = valid_leg_control_frame(
        LegGeneration::new(2).unwrap(),
        LegControlRecord::StandbyRegister {
            session_id: SessionId::new([0x53; 16]).unwrap(),
            standby_nonce,
            selected_version: SESSION_PROTOCOL_VERSION,
            features: STANDBY_CONTROL_V1,
            proof,
        },
    );

    let debug = format!("{frame:?}");
    assert!(debug.contains("[REDACTED]"));
    assert!(!debug.contains("90, 90"));
    assert!(!debug.contains("78, 78"));
    assert!(!debug.contains("session_id"));
    assert!(!debug.contains("standby_nonce"));
    assert!(!debug.contains("proof"));
    assert_eq!(format!("{proof:?}"), "StandbyProof([REDACTED])");
    assert_eq!(format!("{standby_nonce:?}"), "StandbyNonce([REDACTED])");
    assert_eq!(format!("{leg_epoch_nonce:?}"), "LegEpochNonce([REDACTED])");
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}

#[test]
fn malformed_v1_frames_fail_closed_at_every_parser_boundary() {
    let leg = LegGeneration::new(1).unwrap();
    let flow = SessionFlowId::new(1).unwrap();
    let data = valid_frame(
        leg,
        Record::Data {
            flow_id: flow,
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"x"),
        },
    )
    .encode()
    .unwrap();

    let mut bad = data.to_vec();
    bad[0] ^= 0xff;
    assert_eq!(Frame::decode_exact(&bad), Err(ProtocolError::InvalidMagic));

    let mut bad = data.to_vec();
    bad[4..6].copy_from_slice(&2u16.to_be_bytes());
    assert_eq!(
        Frame::decode_exact(&bad),
        Err(ProtocolError::UnsupportedVersion(2))
    );

    let mut bad = data.to_vec();
    bad[6] = 0xff;
    assert_eq!(
        ValidatedFrameHeader::decode(&bad),
        Err(ProtocolError::UnknownRecordType(0xff))
    );

    let mut bad = data.to_vec();
    bad[7] = 1;
    assert_eq!(
        ValidatedFrameHeader::decode(&bad),
        Err(ProtocolError::NonZeroFlags(1))
    );

    for (record_type, body_len, expected) in [
        (
            0x04,
            25,
            ProtocolError::InvalidRecordLength {
                record_type: 0x04,
                len: 25,
            },
        ),
        (0x04, 27, ProtocolError::FrameTooLarge),
        (
            0x02,
            20,
            ProtocolError::InvalidRecordLength {
                record_type: 0x02,
                len: 20,
            },
        ),
        (0x02, 274, ProtocolError::FrameTooLarge),
        (
            0x03,
            25,
            ProtocolError::InvalidRecordLength {
                record_type: 0x03,
                len: 25,
            },
        ),
        (0x03, 65_562, ProtocolError::FrameTooLarge),
        (
            0x07,
            49,
            ProtocolError::InvalidRecordLength {
                record_type: 0x07,
                len: 49,
            },
        ),
        (0x07, 51, ProtocolError::FrameTooLarge),
        (
            0x09,
            47,
            ProtocolError::InvalidRecordLength {
                record_type: 0x09,
                len: 47,
            },
        ),
        (0x09, 49, ProtocolError::FrameTooLarge),
    ] {
        let mut header = data[..FRAME_HEADER_BYTES].to_vec();
        header[6] = record_type;
        header[8..12].copy_from_slice(&(body_len as u32).to_be_bytes());
        assert_eq!(ValidatedFrameHeader::decode(&header), Err(expected));
    }

    let mut truncated = data.to_vec();
    truncated.pop();
    assert!(matches!(
        Frame::decode_exact(&truncated),
        Err(ProtocolError::Truncated { .. })
    ));
    let mut trailing = data.to_vec();
    trailing.push(0);
    assert!(matches!(
        Frame::decode_exact(&trailing),
        Err(ProtocolError::TrailingBytes { .. })
    ));

    let mut bad = data.to_vec();
    bad[12..20].fill(0);
    assert_eq!(
        Frame::decode_exact(&bad),
        Err(ProtocolError::ZeroLegGeneration)
    );
    let mut bad = data.to_vec();
    bad[20..28].fill(0);
    assert_eq!(Frame::decode_exact(&bad), Err(ProtocolError::ZeroFlowId));
    let mut bad = data.to_vec();
    bad[28] = 2;
    assert_eq!(
        Frame::decode_exact(&bad),
        Err(ProtocolError::InvalidDirection(2))
    );
    let mut bad = data.to_vec();
    bad[29..37].copy_from_slice(&u64::MAX.to_be_bytes());
    assert_eq!(
        Frame::decode_exact(&bad),
        Err(ProtocolError::OffsetOverflow)
    );

    let attach = valid_frame(
        leg,
        Record::Attach {
            session_id: SessionId::new([1; 16]).unwrap(),
            nonce: AttachNonce::new([2; 16]).unwrap(),
            min_version: 1,
            max_version: 1,
            offered_features: FeatureSet::new(1),
            required_features: FeatureSet::new(1),
            proof: AttachProof::new([3; 32]).unwrap(),
        },
    )
    .encode()
    .unwrap();
    for (range, error) in [
        (20..36, ProtocolError::ZeroSessionId),
        (36..52, ProtocolError::ZeroAttachNonce),
        (72..104, ProtocolError::ZeroAttachProof),
    ] {
        let mut bad = attach.to_vec();
        bad[range].fill(0);
        assert_eq!(Frame::decode_exact(&bad), Err(error));
    }

    let accepted = valid_frame(
        leg,
        Record::AttachAccepted {
            session_id: SessionId::new([1; 16]).unwrap(),
            nonce: AttachNonce::new([2; 16]).unwrap(),
            selected_version: 1,
            features: FeatureSet::new(1),
        },
    )
    .encode()
    .unwrap();
    for (range, error) in [
        (20..36, ProtocolError::ZeroSessionId),
        (36..52, ProtocolError::ZeroAttachNonce),
    ] {
        let mut bad = accepted.to_vec();
        bad[range].fill(0);
        assert_eq!(Frame::decode_exact(&bad), Err(error));
    }
    let mut bad = accepted.to_vec();
    bad[52..54].fill(0);
    assert_eq!(
        Frame::decode_exact(&bad),
        Err(ProtocolError::InvalidVersionRange { min: 0, max: 0 })
    );

    let generation_status = valid_frame(
        leg,
        Record::AttachGenerationStatus {
            session_id: SessionId::new([1; 16]).unwrap(),
            requested_generation: LegGeneration::new(2).unwrap(),
            nonce: AttachNonce::new([3; 16]).unwrap(),
        },
    )
    .encode()
    .unwrap();
    for (range, error) in [
        (20..36, ProtocolError::ZeroSessionId),
        (36..44, ProtocolError::ZeroLegGeneration),
        (44..60, ProtocolError::ZeroAttachNonce),
    ] {
        let mut bad = generation_status.to_vec();
        bad[range].fill(0);
        assert_eq!(Frame::decode_exact(&bad), Err(error));
    }

    let open = valid_frame(
        leg,
        Record::Open {
            flow_id: flow,
            target: TargetAddr::parse("example.com:443").unwrap(),
        },
    )
    .encode()
    .unwrap();
    let mut bad = open.to_vec();
    bad[29..31].fill(0);
    assert_eq!(
        Frame::decode_exact(&bad),
        Err(ProtocolError::ZeroTargetPort)
    );
    let mut bad = open.to_vec();
    bad[28] = 0xff;
    assert_eq!(
        Frame::decode_exact(&bad),
        Err(ProtocolError::InvalidTargetType(0xff))
    );

    let ack = valid_frame(
        leg,
        Record::Ack {
            flow_id: flow,
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(0),
            final_accepted: false,
        },
    )
    .encode()
    .unwrap();
    let mut bad = ack.to_vec();
    bad[37] = 2;
    assert_eq!(
        Frame::decode_exact(&bad),
        Err(ProtocolError::InvalidBoolean(2))
    );

    for record in [
        Record::OpenResult {
            flow_id: flow,
            result: OpenResultCode::Opened,
        },
        Record::Reset {
            flow_id: flow,
            reason: ResetReason::Unspecified,
        },
    ] {
        let mut bad = valid_frame(leg, record).encode().unwrap().to_vec();
        bad[28..30].copy_from_slice(&u16::MAX.to_be_bytes());
        assert!(matches!(
            Frame::decode_exact(&bad),
            Err(ProtocolError::InvalidOpenResult(u16::MAX))
                | Err(ProtocolError::InvalidResetReason(u16::MAX))
        ));
    }
}

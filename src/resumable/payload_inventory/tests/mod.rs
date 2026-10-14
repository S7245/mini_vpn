//! Value-only diagnostic arithmetic and codec equivalence, not authentication
//! or lifecycle acceptance. Original OwnerTarget fixtures own retry evidence.
use super::*;
use crate::resumable::*;
use bytes::Bytes;

#[test]
fn malformed_retained_frame_cannot_publish_partial_inventory() {
    let frame = Frame::new(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::new(),
        },
    );
    assert!(frame.encode().is_err());
    for _ in 0..2 {
        assert_eq!(
            SessionPayloadInventory::frame(&frame),
            Err(PayloadAuditFailure::InvalidFrame)
        );
    }
    let Record::Data { payload, .. } = frame.record() else {
        unreachable!()
    };
    assert!(payload.is_empty());
}

#[test]
fn every_payload_axis_is_checked_and_nonzero() {
    fn axis(value: &mut SessionPayloadInventory, n: usize) -> &mut usize {
        match n {
            0 => &mut value.events,
            1 => &mut value.session_frames,
            2 => &mut value.session_encoded_bytes,
            3 => &mut value.byte_views,
            4 => &mut value.logical_bytes,
            5 => &mut value.targets,
            6 => &mut value.domain_bytes,
            7 => &mut value.domain_capacity,
            8 => &mut value.capability_values,
            9 => &mut value.effect_batches,
            10 => &mut value.effect_vectors,
            11 => &mut value.effects,
            12 => &mut value.effect_capacity,
            13 => &mut value.effect_array_bytes,
            14 => &mut value.overflow_effects,
            15 => &mut value.mutated_batches,
            16 => &mut value.segment_values,
            17 => &mut value.segment_capacity,
            18 => &mut value.segment_array_bytes,
            19 => &mut value.segment_backing_charges,
            20 => &mut value.receipt_values,
            21 => &mut value.control_frames,
            22 => &mut value.control_encoded_bytes,
            _ => unreachable!(),
        }
    }
    for n in 0..23 {
        let mut one = SessionPayloadInventory::empty();
        *axis(&mut one, n) = 1;
        assert!(!one.is_exact_zero());
        let mut two = one;
        *axis(&mut two, n) = 2;
        assert_eq!(one.checked_add(one), Ok(two));
        let mut full = one;
        *axis(&mut full, n) = usize::MAX;
        assert_eq!(full.checked_add(SessionPayloadInventory::empty()), Ok(full));
        assert_eq!(
            full.checked_add(one),
            Err(PayloadAuditFailure::CountOverflow)
        );
    }
    assert!(SessionPayloadInventory::empty().is_exact_zero());
}

#[test]
fn original_frame_payload_and_domain_capacity_are_read_without_replacement() {
    let frame = Frame::try_new(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"original"),
        },
    )
    .unwrap();
    let Record::Data { payload, .. } = frame.record() else {
        unreachable!()
    };
    let identity = payload.as_ptr();
    let wire = frame.encode().unwrap();
    let expected = SessionPayloadInventory {
        session_frames: 1,
        session_encoded_bytes: wire.len(),
        byte_views: 1,
        logical_bytes: 8,
        ..SessionPayloadInventory::empty()
    };
    assert_eq!(SessionPayloadInventory::frame(&frame), Ok(expected));
    assert_eq!(SessionPayloadInventory::frame(&frame), Ok(expected));
    assert_eq!(payload.as_ptr(), identity);
    assert_eq!(frame.encode().unwrap(), wire);
    let mut host = String::with_capacity(64);
    host.push_str("example.test");
    let identity = host.as_ptr();
    let capacity = host.capacity();
    let target = TargetAddr::DomainPort { host, port: 443 };
    let audit = SessionPayloadInventory::target(&target).unwrap();
    assert_eq!(
        (audit.targets, audit.domain_bytes, audit.domain_capacity),
        (1, 12, capacity)
    );
    let TargetAddr::DomainPort { host, .. } = &target else {
        unreachable!()
    };
    assert_eq!(host.as_ptr(), identity);
}

#[test]
fn batch_overflow_and_empty_mutation_keep_original_backing() {
    let mut batch = SessionEffectBatch::prepared(1).unwrap();
    batch.mark_mutation();
    batch.push(SessionEffect::SessionExpired).unwrap();
    assert_eq!(
        batch.push(SessionEffect::LegActivated {
            generation: LegGeneration::new(1).unwrap(),
        }),
        Err(crate::resumable::effect_storage::EffectStorageError::Overflow)
    );
    let identity = batch.as_ptr();
    let inventory = SessionPayloadInventory::batch(&batch).unwrap();
    assert_eq!(
        (
            inventory.effects,
            inventory.effect_capacity,
            inventory.overflow_effects
        ),
        (2, 1, 1)
    );
    assert_eq!(
        (inventory.effect_batches, inventory.mutated_batches),
        (1, 1)
    );
    assert_eq!(
        inventory.effect_array_bytes,
        std::mem::size_of::<SessionEffect>()
    );
    assert_eq!(SessionPayloadInventory::batch(&batch), Ok(inventory));
    assert_eq!(batch.as_ptr(), identity);
    let empty = SessionEffectBatch::from_vec(Vec::new());
    let inventory = SessionPayloadInventory::batch(&empty).unwrap();
    assert_eq!(
        (
            inventory.effects,
            inventory.effect_capacity,
            inventory.mutated_batches
        ),
        (0, 0, 1)
    );
    assert!(!inventory.is_exact_zero());
}

#[test]
fn all_control_lengths_equal_actual_encoding_without_negotiation_claim() {
    let session_id = SessionId::new([1; 16]).unwrap();
    let standby_nonce = StandbyNonce::new([2; 16]).unwrap();
    let leg_epoch_nonce = LegEpochNonce::new([3; 16]).unwrap();
    let probe_sequence = ProbeSequence::new(1).unwrap();
    let records = [
        LegControlRecord::StandbyRegister {
            session_id,
            standby_nonce,
            selected_version: SESSION_PROTOCOL_VERSION,
            features: STANDBY_CONTROL_V1,
            proof: StandbyProof::new([4; 32]).unwrap(),
        },
        LegControlRecord::StandbyAccepted {
            session_id,
            standby_nonce,
            selected_version: SESSION_PROTOCOL_VERSION,
            features: STANDBY_CONTROL_V1,
        },
        LegControlRecord::LegProbe {
            session_id,
            leg_epoch_nonce,
            probe_sequence,
        },
        LegControlRecord::LegProbeAck {
            session_id,
            leg_epoch_nonce,
            probe_sequence,
        },
        LegControlRecord::SwitchHint {
            session_id,
            standby_nonce,
            hint_sequence: HintSequence::new(1).unwrap(),
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::TargetToClient,
            oldest_unacknowledged: ByteOffset::new(0),
            cause: SwitchHintCause::ReverseApplicationAckStall,
        },
    ];
    for record in records {
        let frame =
            LegControlFrame::try_new(LegGeneration::new(1).unwrap(), record, STANDBY_CONTROL_V1)
                .unwrap();
        let encoded = frame.encode(STANDBY_CONTROL_V1).unwrap();
        assert_eq!(frame.encoded_len_checked(), Ok(encoded.len()));
        let audit = SessionPayloadInventory::leg_control(&frame).unwrap();
        assert_eq!(
            (audit.control_frames, audit.control_encoded_bytes),
            (1, encoded.len())
        );
        assert_eq!((audit.session_frames, audit.session_encoded_bytes), (0, 0));
        assert_eq!(SessionPayloadInventory::leg_control(&frame), Ok(audit));
    }
}

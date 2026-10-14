//! Failed public producer custody; no Supervisor, transport or whole-root claim.
use super::*;
use crate::resumable::auth::*;
use crate::resumable::effect_storage::SessionEffectBatch;
use crate::resumable::protocol::*;

fn model_and_leg(role: SessionRole) -> (SessionModel, CommittedLeg) {
    let session = SessionId::new([21; 16]).unwrap();
    let owner = OwnerIdentity::new([22; 32]).unwrap();
    let principal = DevicePrincipal::new([23; 16]).unwrap();
    let alpn = AttachAlpn::new(b"mini-vpn-owned/1").unwrap();
    let binding = AttachTransportBinding::new(
        owner,
        alpn,
        TlsExporterBinding::new([24; 32]).unwrap(),
        principal,
    );
    let credentials = || {
        AttachCredentials::new(
            DeviceSecret::new([25; 32]).unwrap(),
            ResumeSecret::new([26; 32]).unwrap(),
        )
        .unwrap()
    };
    let authority = AttachAuthority::new(
        session,
        LegGeneration::new(1).unwrap(),
        credentials(),
        AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b1111).unwrap(),
    );
    let request = AttachRequest::new(
        session,
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([27; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b0111, 0b0001).unwrap(),
    );
    let proof = credentials().prove(&request, &binding).unwrap();
    let leg = authority
        .verify_and_commit(&request, &binding, &proof)
        .unwrap();
    let config = SessionConfig::new(
        8,
        32,
        TcpWindowLimits::new(64, 8).unwrap(),
        TcpWindowLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReceiveBudgetLimits::new(128, 32).unwrap(),
        16,
    )
    .unwrap();
    (SessionModel::new(role, config, leg), leg)
}

#[test]
fn overflowing_public_batch_retains_prefix_and_first_effect_and_blocks_mutation() {
    let (mut model, leg) = model_and_leg(SessionRole::Client);
    let target = crate::shared::TargetAddr::IpPort("127.0.0.1:443".parse().unwrap());
    let opened = model
        .reduce(SessionEvent::LocalOpen {
            leg,
            target: target.clone(),
        })
        .unwrap();
    let SessionEffect::LocalFlowOpened { flow } = opened[0] else {
        panic!("local flow")
    };
    let batch = SessionEffectBatch::prepared(1).unwrap();
    let original = batch.as_ptr();
    // Deliberately undersized backing challenges the exact same owned wrapper;
    // normal ordinary capacity is three, not an assertion of a natural >3 burst.
    let error = model
        .reduce_owned(
            SessionEvent::LocalData {
                flow,
                payload: Bytes::from_static(b"retained"),
            },
            batch,
        )
        .unwrap_err();
    assert_eq!(error, SessionError::EffectStorageOverflow);
    let failed = model.failed_effects.as_ref().unwrap();
    assert_eq!(failed.as_ptr(), original);
    assert_eq!(failed.capacity(), 1);
    assert_eq!(failed.len(), 2);
    assert!(failed.has_overflow());
    let mut values = failed.iter();
    assert!(
        matches!(values.next(), Some(SessionEffect::ReplayStored { receipt })
        if receipt.payload().as_ref() == b"retained")
    );
    assert!(matches!(values.next(), Some(SessionEffect::Transmit(frame))
        if matches!(frame.record(), Record::Data { payload, .. } if payload.as_ref() == b"retained")));
    assert!(values.next().is_none());
    drop(values);
    let retained = model.snapshot();
    let flow_retained = model.flow_snapshot(flow.flow_id());
    for event in [
        SessionEvent::LegLost { leg },
        SessionEvent::LocalClose { flow },
        SessionEvent::LocalOpen {
            leg,
            target: target.clone(),
        },
    ] {
        assert_eq!(
            model.reduce(event).unwrap_err(),
            SessionError::EffectStorageOverflow
        );
        assert_eq!(model.snapshot(), retained);
        assert_eq!(model.flow_snapshot(flow.flow_id()), flow_retained);
        assert_eq!(model.failed_effects.as_ref().unwrap().as_ptr(), original);
    }
    let failure = model.preflight_local_open(leg, target, true).err().unwrap();
    assert_eq!(failure.into_parts().1, SessionError::EffectStorageOverflow);
    assert_eq!(
        model.preflight_replacement(leg).err().unwrap(),
        SessionError::EffectStorageOverflow
    );
    assert_eq!(
        model.prepare_owner_replacement(leg).err().unwrap(),
        SessionError::EffectStorageOverflow
    );
    assert_eq!(model.snapshot(), retained);
}

#[test]
fn zero_effect_mutation_is_distinct_from_precommit_rejection_and_cannot_be_reused() {
    let (mut model, leg) = model_and_leg(SessionRole::Client);
    let opened = model
        .reduce(SessionEvent::LocalOpen {
            leg,
            target: crate::shared::TargetAddr::IpPort("127.0.0.1:443".parse().unwrap()),
        })
        .unwrap();
    let SessionEffect::LocalFlowOpened { flow } = opened[0] else {
        panic!("local flow")
    };
    model
        .reduce(SessionEvent::PeerFrame {
            leg,
            frame: Frame::new(
                leg.generation(),
                Record::OpenResult {
                    flow_id: flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            ),
        })
        .unwrap();
    let mut batch = SessionEffectBatch::prepared(3).unwrap();
    let original = batch.as_ptr();
    // A real receive gap commits bytes/ranges without emitting any effect.
    model
        .reduce_into(
            SessionEvent::PeerFrame {
                leg,
                frame: Frame::new(
                    leg.generation(),
                    Record::Data {
                        flow_id: flow.flow_id(),
                        direction: Direction::TargetToClient,
                        offset: ByteOffset::new(4),
                        payload: Bytes::from_static(b"gap"),
                    },
                ),
            },
            &mut batch,
        )
        .unwrap();
    assert!(batch.is_empty() && batch.mutation_started());
    assert_eq!(model.snapshot().receive_owned_bytes(), 3);
    let committed = model.snapshot();
    // The same wrapper cannot silently normalize an already-entered empty
    // batch. This admission check follows an actual mutation, not a fake token.
    assert_eq!(
        model
            .reduce_owned(SessionEvent::LegLost { leg }, batch)
            .unwrap_err(),
        SessionError::EffectStorageOverflow
    );
    let failed = model.failed_effects.as_ref().unwrap();
    assert!(failed.is_empty() && failed.mutation_started());
    assert_eq!(failed.as_ptr(), original);
    assert_eq!(model.snapshot(), committed);
    assert_eq!(
        model.reduce(SessionEvent::LegLost { leg }).unwrap_err(),
        SessionError::EffectStorageOverflow
    );
    assert_eq!(model.snapshot(), committed);
}

#[test]
fn terminal_grace_zero_output_marks_mutation_and_only_completed_transfer_rearms() {
    let (mut model, leg) = model_and_leg(SessionRole::Client);
    let opened = model
        .reduce(SessionEvent::LocalOpen {
            leg,
            target: crate::shared::TargetAddr::IpPort("127.0.0.1:443".parse().unwrap()),
        })
        .unwrap();
    let SessionEffect::LocalFlowOpened { flow } = opened[0] else {
        panic!("local flow")
    };
    let terminal = model
        .reduce(SessionEvent::LocalReset {
            flow,
            reason: ResetReason::LocalAbandon,
        })
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::FlowFinished { terminal, .. } => Some(terminal),
            _ => None,
        })
        .unwrap();
    let mut batch = SessionEffectBatch::prepared(3).unwrap();
    let original = batch.as_ptr();
    model
        .reduce_into(SessionEvent::TerminalGraceExpired { terminal }, &mut batch)
        .unwrap();
    assert!(batch.is_empty() && batch.mutation_started());
    assert_eq!(model.snapshot().terminal_tombstones(), 0);
    batch.rearm_after_transfer().unwrap();
    assert!(!batch.mutation_started());
    assert_eq!(batch.as_ptr(), original);
    model
        .reduce_into(SessionEvent::LegLost { leg }, &mut batch)
        .unwrap();
    assert!(batch.mutation_started());
    assert!(
        batch.rearm_after_transfer().is_err(),
        "untransferred effect cannot return to idle"
    );
}

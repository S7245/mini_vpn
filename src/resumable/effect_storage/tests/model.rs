//! Pure Model precommit effect-storage transactions through real protocol capabilities.
//! Owns fixture and refusal/retry assertions; no transport or whole-root claims.

use super::super::*;

mod lifecycle;
mod receive;
mod terminal;
use crate::resumable::auth::*;
use crate::resumable::protocol::*;
use crate::resumable::session::{
    LocalFlow, ReceiveBudgetLimits, ReplayBudgetLimits, SessionConfig, SessionError, SessionEvent,
    SessionModel, SessionRole,
};
use crate::resumable::tcp::TcpWindowLimits;

fn model_and_leg(role: SessionRole) -> (SessionModel, CommittedLeg) {
    model_and_leg_at(role, 2)
}

fn model_and_leg_at(role: SessionRole, generation: u64) -> (SessionModel, CommittedLeg) {
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
        LegGeneration::new(generation - 1).unwrap(),
        credentials(),
        AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 0b1111).unwrap(),
    );
    let request = AttachRequest::new(
        session,
        LegGeneration::new(generation).unwrap(),
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
fn ordinary_effect_reserve_rejects_before_phase_change_then_retries() {
    let (mut model, leg) = model_and_leg(SessionRole::Client);
    let before = model.snapshot();
    FAIL_NEXT_ALLOCATION.set(true);
    let result = model.reduce(SessionEvent::LegLost { leg });
    FAIL_NEXT_ALLOCATION.set(false);
    let error = result.expect_err("effect preparation must precede Legless publication");
    assert_eq!(error, SessionError::EffectStorageReserve);
    assert_eq!(model.snapshot(), before);
    let effects = model.reduce(SessionEvent::LegLost { leg }).unwrap();
    assert_eq!(effects.capacity(), 3);
    assert!(matches!(
        effects.as_slice(),
        [SessionEffect::ResumeGraceStarted { .. }]
    ));
    FAIL_NEXT_ALLOCATION.set(true);
    let repeated = model.reduce(SessionEvent::LegLost { leg }).unwrap();
    assert!(
        FAIL_NEXT_ALLOCATION.replace(false),
        "no-op must not allocate"
    );
    assert!(repeated.is_empty());
    assert_eq!(repeated.capacity(), 0);
}

fn opened_model() -> (SessionModel, CommittedLeg, LocalFlow) {
    let (mut model, leg) = model_and_leg(SessionRole::Client);
    let opened = model
        .reduce(SessionEvent::LocalOpen {
            leg,
            target: crate::shared::TargetAddr::IpPort("127.0.0.1:443".parse().unwrap()),
        })
        .unwrap();
    let SessionEffect::LocalFlowOpened { flow } = opened[0] else {
        panic!("missing local flow")
    };
    (model, leg, flow)
}

#[test]
fn local_data_effect_reserve_preserves_tcp_and_global_ownership_and_error_order() {
    use crate::resumable::tcp::TcpOwnershipError;
    use bytes::Bytes;
    let (mut model, _, flow) = opened_model();
    let before = model.snapshot();
    let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let invalid = model.reduce(SessionEvent::LocalData {
        flow,
        payload: Bytes::new(),
    });
    let not_allocated = FAIL_NEXT_ALLOCATION.replace(false);
    assert!(matches!(
        invalid,
        Err(SessionError::TcpOwnership {
            source: TcpOwnershipError::EmptyData,
            ..
        })
    ));
    assert!(
        not_allocated,
        "original TCP validation must precede effect reserve"
    );
    FAIL_NEXT_ALLOCATION.set(true);
    let rejected = model.reduce(SessionEvent::LocalData {
        flow,
        payload: Bytes::from_static(b"abc"),
    });
    FAIL_NEXT_ALLOCATION.set(false);
    assert!(
        matches!(rejected, Err(SessionError::EffectStorageReserve)),
        "valid input must reach precommit reserve: {rejected:?}"
    );
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
    let effects = model
        .reduce(SessionEvent::LocalData {
            flow,
            payload: Bytes::from_static(b"abc"),
        })
        .unwrap();
    assert_eq!(effects.capacity(), 3);
    assert!(matches!(
        effects.as_slice(),
        [
            SessionEffect::ReplayStored { .. },
            SessionEffect::Transmit(_)
        ]
    ));
    assert_eq!(
        model
            .flow_snapshot(flow.flow_id())
            .unwrap()
            .send()
            .retained_bytes(),
        3
    );
    // Now the real window is nonempty (one original descriptor). The next
    // append prepares capacity2, then the original effect reserve rejects.
    let before = model.snapshot();
    let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let rejected = model.reduce(SessionEvent::LocalData {
        flow,
        payload: Bytes::from_static(b"def"),
    });
    assert!(matches!(rejected, Err(SessionError::EffectStorageReserve)));
    assert!(!FAIL_NEXT_ALLOCATION.replace(false));
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
    let retry = model
        .reduce(SessionEvent::LocalData {
            flow,
            payload: Bytes::from_static(b"def"),
        })
        .unwrap();
    assert!(matches!(retry.as_slice(),
        [SessionEffect::ReplayStored { .. }, SessionEffect::Transmit(frame)]
        if matches!(frame.record(), Record::Data { offset, payload, .. }
            if *offset == ByteOffset::new(3) && payload.as_ref() == b"def")
    ));
    let committed = model.flow_snapshot(flow.flow_id()).unwrap();
    assert_eq!(committed.send().retained_bytes(), 6);
    assert_eq!(committed.send().segment_count(), 2);
}

#[test]
fn sender_completion_reserves_before_fin_and_ack_publication() {
    use crate::resumable::tcp::TcpOwnershipError;
    use bytes::Bytes;
    let (mut model, leg, flow) = opened_model();
    model
        .reduce(SessionEvent::LocalData {
            flow,
            payload: Bytes::from_static(b"abc"),
        })
        .unwrap();
    let before = model.snapshot();
    let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let closed = model.reduce(SessionEvent::LocalClose { flow });
    FAIL_NEXT_ALLOCATION.set(false);
    assert!(
        matches!(closed, Err(SessionError::EffectStorageReserve)),
        "FIN must reserve first: {closed:?}"
    );
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
    let closed = model.reduce(SessionEvent::LocalClose { flow }).unwrap();
    assert_eq!(closed.capacity(), 3);
    assert!(matches!(closed.as_slice(), [SessionEffect::Transmit(_)]));
    let ack = |offset, final_accepted| SessionEvent::PeerFrame {
        leg,
        frame: Frame::new(
            leg.generation(),
            Record::Ack {
                flow_id: flow.flow_id(),
                direction: Direction::ClientToTarget,
                next_accepted: ByteOffset::new(offset),
                final_accepted,
            },
        ),
    };
    FAIL_NEXT_ALLOCATION.set(true);
    let invalid = model.reduce(ack(4, false));
    assert!(FAIL_NEXT_ALLOCATION.replace(false));
    assert!(matches!(
        invalid,
        Err(SessionError::TcpOwnership {
            source: TcpOwnershipError::AckBeyondSent { .. },
            ..
        })
    ));
    let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let rejected = model.reduce(ack(1, false));
    FAIL_NEXT_ALLOCATION.set(false);
    assert!(
        matches!(rejected, Err(SessionError::EffectStorageReserve)),
        "ACK must reserve first: {rejected:?}"
    );
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
    let acked = model.reduce(ack(1, false)).unwrap();
    assert_eq!(acked.capacity(), 3);
    assert!(matches!(
        acked.as_slice(),
        [SessionEffect::ReplayAcknowledged { .. }]
    ));
    assert_eq!(
        model
            .flow_snapshot(flow.flow_id())
            .unwrap()
            .send()
            .retained_bytes(),
        2
    );
    model.reduce(ack(3, true)).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let repeated = model.reduce(SessionEvent::LocalClose { flow }).unwrap();
    assert!(FAIL_NEXT_ALLOCATION.replace(false));
    assert!(repeated.is_empty());
    assert_eq!(repeated.capacity(), 0);
}

#[test]
fn expiry_effect_reserve_preserves_live_flows_until_session_expiration() {
    use bytes::Bytes;
    let (mut model, leg, flow) = opened_model();
    model
        .reduce(SessionEvent::LocalData {
            flow,
            payload: Bytes::from_static(b"abc"),
        })
        .unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let premature = model.reduce(SessionEvent::ResumeGraceExpired { leg });
    assert!(FAIL_NEXT_ALLOCATION.replace(false));
    assert!(matches!(premature, Err(SessionError::ResumeGraceNotActive)));
    model.reduce(SessionEvent::LegLost { leg }).unwrap();
    let before = model.snapshot();
    let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let expired = model.reduce(SessionEvent::ResumeGraceExpired { leg });
    FAIL_NEXT_ALLOCATION.set(false);
    assert!(
        matches!(expired, Err(SessionError::EffectStorageReserve)),
        "expiry must reserve before clearing: {expired:?}"
    );
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
    let expired = model
        .reduce(SessionEvent::ResumeGraceExpired { leg })
        .unwrap();
    assert_eq!(expired.capacity(), 3);
    assert!(matches!(
        expired.as_slice(),
        [SessionEffect::SessionExpired]
    ));
    assert_eq!(model.snapshot().flow_count(), 0);
    assert_eq!(
        model
            .snapshot()
            .replay_usage(Direction::ClientToTarget)
            .bytes(),
        0
    );
}

#[test]
fn receiver_effect_reserve_preserves_reorder_acceptance_and_fin() {
    use bytes::Bytes;
    let (mut model, leg, flow) = opened_model();
    let inbound = |record| SessionEvent::PeerFrame {
        leg,
        frame: Frame::new(leg.generation(), record),
    };
    model
        .reduce(inbound(Record::OpenResult {
            flow_id: flow.flow_id(),
            result: OpenResultCode::Opened,
        }))
        .unwrap();
    let data = |offset, bytes: &'static [u8]| {
        inbound(Record::Data {
            flow_id: flow.flow_id(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(offset),
            payload: Bytes::from_static(bytes),
        })
    };
    FAIL_NEXT_ALLOCATION.set(true);
    let gap = model.reduce(data(3, b"def")).unwrap();
    assert!(
        FAIL_NEXT_ALLOCATION.replace(false),
        "gap must not allocate an empty effects batch"
    );
    assert!(gap.is_empty());
    assert_eq!(gap.capacity(), 0);
    let before = model.snapshot();
    let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let rejected = model.reduce(data(0, b"abc"));
    FAIL_NEXT_ALLOCATION.set(false);
    assert!(
        matches!(rejected, Err(SessionError::EffectStorageReserve)),
        "contiguous DATA must reserve first: {rejected:?}"
    );
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
    let offered = model.reduce(data(0, b"abc")).unwrap();
    assert_eq!(offered.capacity(), 3);
    let [SessionEffect::OfferToSink { offer, .. }] = offered.as_slice() else {
        panic!("missing sink offer")
    };
    let offer = *offer;
    FAIL_NEXT_ALLOCATION.set(true);
    let duplicate = model.reduce(data(0, b"abc")).unwrap();
    assert!(FAIL_NEXT_ALLOCATION.replace(false));
    assert!(duplicate.is_empty());
    assert_eq!(duplicate.capacity(), 0);
    let before = model.snapshot();
    let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let rejected = model.reduce(SessionEvent::SinkAccepted { offer, bytes: 3 });
    FAIL_NEXT_ALLOCATION.set(false);
    assert!(
        matches!(rejected, Err(SessionError::EffectStorageReserve)),
        "acceptance must reserve first: {rejected:?}"
    );
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
    let accepted = model
        .reduce(SessionEvent::SinkAccepted { offer, bytes: 3 })
        .unwrap();
    assert_eq!(accepted.capacity(), 3);
    let [
        SessionEffect::Transmit(_),
        SessionEffect::OfferToSink { offer, .. },
    ] = accepted.as_slice()
    else {
        panic!("missing ACK/next offer")
    };
    let offer = *offer;
    let close = || {
        inbound(Record::Close {
            flow_id: flow.flow_id(),
            direction: Direction::TargetToClient,
            final_offset: ByteOffset::new(6),
        })
    };
    let before = model.snapshot();
    let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let rejected = model.reduce(close());
    FAIL_NEXT_ALLOCATION.set(false);
    assert!(
        matches!(rejected, Err(SessionError::EffectStorageReserve)),
        "receive FIN must reserve first: {rejected:?}"
    );
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
    model.reduce(close()).unwrap();
    let accepted = model
        .reduce(SessionEvent::SinkAccepted { offer, bytes: 3 })
        .unwrap();
    let [
        SessionEffect::Transmit(_),
        SessionEffect::HalfCloseSink { completion },
    ] = accepted.as_slice()
    else {
        panic!("missing ACK/half close")
    };
    let completion = *completion;
    let before = model.snapshot();
    let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
    FAIL_NEXT_ALLOCATION.set(true);
    let rejected = model.reduce(SessionEvent::SinkHalfClosed { completion });
    FAIL_NEXT_ALLOCATION.set(false);
    assert!(
        matches!(rejected, Err(SessionError::EffectStorageReserve)),
        "half-close completion must reserve first: {rejected:?}"
    );
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
    let completed = model
        .reduce(SessionEvent::SinkHalfClosed { completion })
        .unwrap();
    assert_eq!(completed.capacity(), 3);
    assert!(matches!(completed.as_slice(), [SessionEffect::Transmit(_)]));
}

#[test]
fn legless_accept_reserves_only_for_actual_partial_or_adjacent_offer() {
    use bytes::Bytes;
    for (first, successor, offers_again) in [
        (b"abcdef".as_slice(), None, true),
        (b"abc".as_slice(), Some(3), true),
        (b"abc".as_slice(), Some(6), false),
    ] {
        let (mut model, leg, flow) = opened_model();
        let inbound = |record| SessionEvent::PeerFrame {
            leg,
            frame: Frame::new(leg.generation(), record),
        };
        model
            .reduce(inbound(Record::OpenResult {
                flow_id: flow.flow_id(),
                result: OpenResultCode::Opened,
            }))
            .unwrap();
        let data = |offset, bytes: &'static [u8]| {
            inbound(Record::Data {
                flow_id: flow.flow_id(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(offset),
                payload: Bytes::from_static(bytes),
            })
        };
        let offered = model.reduce(data(0, first)).unwrap();
        let [SessionEffect::OfferToSink { offer, .. }] = offered.as_slice() else {
            panic!("missing sink offer")
        };
        let offer = *offer;
        if let Some(offset) = successor {
            assert!(model.reduce(data(offset, b"def")).unwrap().is_empty());
        }
        model.reduce(SessionEvent::LegLost { leg }).unwrap();
        let before = model.snapshot();
        let tcp_before = model.flow_snapshot(flow.flow_id()).unwrap();
        FAIL_NEXT_ALLOCATION.set(true);
        let result = model.reduce(SessionEvent::SinkAccepted { offer, bytes: 3 });
        let unused = FAIL_NEXT_ALLOCATION.replace(false);
        if offers_again {
            assert!(!unused);
            assert!(matches!(result, Err(SessionError::EffectStorageReserve)));
            assert_eq!(model.snapshot(), before);
            assert_eq!(model.flow_snapshot(flow.flow_id()).unwrap(), tcp_before);
            let accepted = model
                .reduce(SessionEvent::SinkAccepted { offer, bytes: 3 })
                .unwrap();
            assert_eq!(accepted.capacity(), 3);
            assert!(matches!(
                accepted.as_slice(),
                [SessionEffect::OfferToSink { .. }]
            ));
        } else {
            assert!(
                unused,
                "a retained gap does not require an effects allocation"
            );
            let accepted = result.unwrap();
            assert!(accepted.is_empty());
            assert_eq!(accepted.capacity(), 0);
            assert_eq!(
                model
                    .flow_snapshot(flow.flow_id())
                    .unwrap()
                    .receive()
                    .accepted(),
                ByteOffset::new(3)
            );
            assert_eq!(
                model
                    .flow_snapshot(flow.flow_id())
                    .unwrap()
                    .receive()
                    .buffered_bytes(),
                3
            );
        }
    }
}

#[test]
fn owner_open_resolution_and_replay_reserve_before_authority_publication() {
    for result in [OpenResultCode::Opened, OpenResultCode::TargetRefused] {
        let (mut model, leg) = model_and_leg(SessionRole::Owner);
        let open = || SessionEvent::PeerFrame {
            leg,
            frame: Frame::new(
                leg.generation(),
                Record::Open {
                    flow_id: SessionFlowId::new(1).unwrap(),
                    target: crate::shared::TargetAddr::IpPort("127.0.0.1:443".parse().unwrap()),
                },
            ),
        };
        let before = model.snapshot();
        FAIL_NEXT_ALLOCATION.set(true);
        let rejected = model.reduce(open());
        FAIL_NEXT_ALLOCATION.set(false);
        assert!(
            matches!(rejected, Err(SessionError::EffectStorageReserve)),
            "OPEN must reserve first: {rejected:?}"
        );
        assert_eq!(model.snapshot(), before);
        let opened = model.reduce(open()).unwrap();
        assert_eq!(opened.capacity(), 3);
        let [SessionEffect::PeerOpenRequested { request, .. }] = opened.as_slice() else {
            panic!("missing peer request")
        };
        let request = *request;
        let before = model.snapshot();
        FAIL_NEXT_ALLOCATION.set(true);
        let rejected = model.reduce(SessionEvent::PeerOpenResolved { request, result });
        FAIL_NEXT_ALLOCATION.set(false);
        assert!(
            matches!(rejected, Err(SessionError::EffectStorageReserve)),
            "resolution must reserve before publication: {rejected:?}"
        );
        assert_eq!(model.snapshot(), before);
        // Unresolved duplicate emits nothing: this observes that neither a
        // successful nor a failed result was recorded before Reserve rejection.
        FAIL_NEXT_ALLOCATION.set(true);
        let duplicate = model.reduce(open()).unwrap();
        assert!(FAIL_NEXT_ALLOCATION.replace(false));
        assert!(duplicate.is_empty());
        assert_eq!(duplicate.capacity(), 0);
        let resolved = model
            .reduce(SessionEvent::PeerOpenResolved { request, result })
            .unwrap();
        assert_eq!(resolved.capacity(), 3);
        assert!(matches!(resolved.first(), Some(SessionEffect::Transmit(_))));
        assert_eq!(
            resolved.len(),
            if result == OpenResultCode::Opened {
                1
            } else {
                2
            }
        );
        if result != OpenResultCode::Opened {
            assert!(matches!(
                resolved.last(),
                Some(SessionEffect::FlowFinished { .. })
            ));
        }
        let before = model.snapshot();
        FAIL_NEXT_ALLOCATION.set(true);
        let replay = model.reduce(open());
        FAIL_NEXT_ALLOCATION.set(false);
        assert!(
            matches!(replay, Err(SessionError::EffectStorageReserve)),
            "recorded/tombstone replay must reserve: {replay:?}"
        );
        assert_eq!(model.snapshot(), before);
        let replay = model.reduce(open()).unwrap();
        assert_eq!(replay.capacity(), 3);
        let [SessionEffect::Transmit(frame)] = replay.as_slice() else {
            panic!("missing replay")
        };
        assert!(
            matches!(frame.record(), Record::OpenResult { result: recorded, .. } if *recorded == result)
        );
    }
}

#[test]
fn replacement_effect_reserve_precedes_generation_and_adapter_commit() {
    let (mut model, leg, flow) = opened_model();
    let (_, replacement) = model_and_leg_at(SessionRole::Client, 3);
    model
        .reduce(SessionEvent::LocalData {
            flow,
            payload: bytes::Bytes::from_static(b"replay"),
        })
        .unwrap();
    model.reduce(SessionEvent::LocalClose { flow }).unwrap();
    model.reduce(SessionEvent::LegLost { leg }).unwrap();
    let before = model.snapshot();
    let flow_before = model.flow_snapshot(flow.flow_id());
    FAIL_NEXT_ALLOCATION.set(true);
    let result = model.reduce(SessionEvent::ReplacementAttached { leg: replacement });
    FAIL_NEXT_ALLOCATION.set(false);
    assert_eq!(result.unwrap_err(), SessionError::EffectStorageReserve);
    assert_eq!(model.snapshot(), before);
    assert_eq!(model.flow_snapshot(flow.flow_id()), flow_before);

    FAIL_NEXT_ALLOCATION.set(true);
    let result = model.install_prepared_client_replacement(replacement, |_| -> Result<(), ()> {
        panic!("adapter cannot enter on storage rejection")
    });
    FAIL_NEXT_ALLOCATION.set(false);
    assert_eq!(result.unwrap_err(), SessionError::EffectStorageReserve);
    assert_eq!(model.snapshot(), before);

    FAIL_NEXT_ALLOCATION.set(true);
    let invalid = model.reduce(SessionEvent::ReplacementAttached { leg });
    assert!(
        FAIL_NEXT_ALLOCATION.replace(false),
        "original generation check comes first"
    );
    assert!(matches!(
        invalid,
        Err(SessionError::ReplacementGenerationNotNext { .. })
    ));
    assert_eq!(model.snapshot(), before);
    let effects = model
        .reduce(SessionEvent::ReplacementAttached { leg: replacement })
        .unwrap();
    assert_eq!(
        effects.capacity(),
        4,
        "original exact batch, not ordinary3 or dense maximum"
    );
    assert!(matches!(effects.as_slice(), [
        SessionEffect::LegActivated { .. },
        SessionEffect::Transmit(open),
        SessionEffect::Transmit(data),
        SessionEffect::Transmit(close),
    ] if matches!(open.record(), Record::Open { .. })
        && matches!(data.record(), Record::Data { payload, .. } if payload.as_ref() == b"replay")
        && matches!(close.record(), Record::Close { .. })));
    assert_eq!(model.snapshot().generation(), replacement.generation());
    assert_eq!(model.flow_snapshot(flow.flow_id()), flow_before);
}

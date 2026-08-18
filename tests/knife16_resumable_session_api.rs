use mini_vpn::resumable::{
    FlowFinishReason, LocalFlow, PeerOpenRequest, ReceiveBudgetLimits, ReplayBudgetLimits,
    SessionConfig, SessionEffect, SessionEvent, SessionModel, SinkHalfClose, SinkOffer,
    TerminalGrace,
};

// This integration test deliberately lives outside the crate. It prevents the
// adapter-facing reducer from exposing enum fields whose capability types are
// trapped inside a private module.
#[test]
fn session_adapter_surface_is_public_and_exhaustive() {
    fn assert_public<T>() {}
    assert_public::<FlowFinishReason>();
    assert_public::<LocalFlow>();
    assert_public::<PeerOpenRequest>();
    assert_public::<ReceiveBudgetLimits>();
    assert_public::<ReplayBudgetLimits>();
    assert_public::<SessionConfig>();
    assert_public::<SessionModel>();
    assert_public::<SinkHalfClose>();
    assert_public::<SinkOffer>();
    assert_public::<TerminalGrace>();

    fn consume_event(event: SessionEvent) {
        match event {
            SessionEvent::ReplacementAttached { .. }
            | SessionEvent::LegLost { .. }
            | SessionEvent::ResumeGraceExpired { .. }
            | SessionEvent::LocalOpen { .. }
            | SessionEvent::LocalData { .. }
            | SessionEvent::LocalClose { .. }
            | SessionEvent::LocalReset { .. }
            | SessionEvent::PeerOpenResolved { .. }
            | SessionEvent::PeerFrame { .. }
            | SessionEvent::SinkAccepted { .. }
            | SessionEvent::SinkAbandoned { .. }
            | SessionEvent::SinkHalfClosed { .. }
            | SessionEvent::SinkHalfCloseFailed { .. }
            | SessionEvent::TerminalGraceExpired { .. } => {}
        }
    }

    fn consume_effect(effect: SessionEffect) {
        match effect {
            SessionEffect::LegActivated { .. }
            | SessionEffect::ResumeGraceStarted { .. }
            | SessionEffect::SessionExpired
            | SessionEffect::Transmit(_)
            | SessionEffect::LocalFlowOpened { .. }
            | SessionEffect::PeerOpenRequested { .. }
            | SessionEffect::LocalOpenResolved { .. }
            | SessionEffect::OfferToSink { .. }
            | SessionEffect::HalfCloseSink { .. }
            | SessionEffect::PeerReset { .. }
            | SessionEffect::FlowFinished { .. } => {}
        }
    }

    let _: fn(SessionEvent) = consume_event;
    let _: fn(SessionEffect) = consume_effect;
}

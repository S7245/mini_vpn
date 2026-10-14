//! Real owner-reducer requests and mailbox observations for root progress.

mod capacity_registration;
mod control;
mod ordered_source;

use super::super::*;
use crate::owned_upstream::session::{SessionOwnerCommand, SessionOwnerInbox, SessionOwnerMailbox};
use crate::resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
    AttachTransportBinding, CommittedLeg, DevicePrincipal, DeviceSecret, FeatureOffer, Frame,
    LegGeneration, LocalFlow, OpenResultCode, OwnerIdentity, PeerOpenRequest, ReceiveBudgetLimits,
    Record, ReplayBudgetLimits, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig,
    SessionEffect, SessionEvent, SessionFlowId, SessionId, SessionModel, SessionRole,
    TcpWindowLimits, TlsExporterBinding, VersionRange,
};
use crate::shared::TargetAddr;

fn owner_session() -> (SessionModel, CommittedLeg) {
    let session_id = SessionId::new([0x21; 16]).unwrap();
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

fn owner_request() -> (SessionModel, PeerOpenRequest, LocalFlow) {
    let (mut model, leg) = owner_session();
    let effects = model
        .reduce(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Open {
                    flow_id: SessionFlowId::new(1).unwrap(),
                    target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (request, flow) = effects
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::PeerOpenRequested { request, flow, .. } => Some((request, flow)),
            _ => None,
        })
        .unwrap();
    (model, request, flow)
}

use std::time::Duration;

use super::tests::{
    FEATURES, active_pair, binding, deliver_acceptance, deliver_registration, limits, transport,
};
use super::*;
use crate::owned_upstream::leg::BoundInbound;
use crate::owned_upstream::leg_io::{LegEndpointRole, LegIo, LegOutboundQueue};
use crate::owned_upstream::two_leg::{LegId, SimTime};
use crate::resumable::{
    FeatureSet, LegControlFrame, LegControlRecord, LegEpochNonce, ProbeSequence,
};

#[test]
fn owner_probe_freshness_rejects_times_before_the_authenticated_ack() {
    let (authority, _model, client_active, owner_active) = active_pair();
    let mut client_b = LegIo::for_authenticated_transport(
        LegId::B,
        LegEndpointRole::Client,
        binding(0x99),
        limits(),
    );
    let mut owner_b = LegIo::for_authenticated_transport(
        LegId::B,
        LegEndpointRole::Owner,
        binding(0x99),
        limits(),
    );
    let mut client_queue = LegOutboundQueue::for_leg(client_b.established_leg(), 4, 4_096).unwrap();
    let mut owner_queue = LegOutboundQueue::for_leg(owner_b.established_leg(), 4, 4_096).unwrap();
    let mut wire = transport();
    let (awaiting, candidate) = deliver_registration(
        &client_active,
        &owner_active,
        &mut client_b,
        &mut owner_b,
        &mut client_queue,
        &mut wire,
        &authority,
        0x33,
    );
    let contract = candidate.contract();
    let mut slot = OwnerStandbySlot::new();
    assert_eq!(
        slot.admit(candidate, &mut owner_queue).unwrap(),
        OwnerStandbyAdmission::Installed,
    );
    let registered = awaiting
        .validate(deliver_acceptance(
            &mut owner_b,
            &mut client_b,
            &mut owner_queue,
            &mut wire,
        ))
        .unwrap();
    assert!(registered.is_live());
    let hint_binding = slot.take_exact_hint_binding(&owner_queue).unwrap();

    let sequence = ProbeSequence::new(7).unwrap();
    slot.drive_exact_probe(sequence, &mut owner_queue).unwrap();
    let ack = BoundInbound::LegControl(
        owner_b.established_leg().bind_received_control_frame(
            LegControlFrame::try_new(
                contract.generation,
                LegControlRecord::LegProbeAck {
                    session_id: contract.session_id,
                    leg_epoch_nonce: LegEpochNonce::from(contract.nonce),
                    probe_sequence: sequence,
                },
                FeatureSet::new(FEATURES),
            )
            .unwrap(),
        ),
    );
    slot.observe_exact_probe_ack(ack, SimTime::from_nanos(50), Duration::from_nanos(10))
        .unwrap();

    assert!(!slot.probe_is_fresh_at(SimTime::from_nanos(49)));
    assert_eq!(
        slot.validate_exact_hint_binding_at(&hint_binding, SimTime::from_nanos(49)),
        Err(OwnerStandbyHintEvidenceError::ProbeNotFresh),
    );
    assert!(slot.probe_is_fresh_at(SimTime::from_nanos(50)));
    assert!(slot.probe_is_fresh_at(SimTime::from_nanos(60)));
    assert!(!slot.probe_is_fresh_at(SimTime::from_nanos(61)));
}

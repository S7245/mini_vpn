use super::*;
use crate::owned_upstream::FlowPortConfig;
use crate::owned_upstream::leg::EstablishedLeg;
use crate::owned_upstream::leg_io::{LegEndpointRole, LegIo, LegIoLimits};
use crate::owned_upstream::owner_output_drain::{
    OwnerOutputDrain, OwnerOutputDrainDrive, OwnerOutputLifecycleAction, OwnerOutputPageDrained,
};
use crate::owned_upstream::owner_ready::{OwnerReadySessionLimits, ReadyGeometry, ReadyIndex};
use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
use crate::owned_upstream::owner_target::{
    OwnerTargetAttachPublication, OwnerTargetConfig, OwnerTargetExecutor, OwnerTargetOutput,
};
use crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate;
use crate::owned_upstream::supervisor::SessionSupervisor;
use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
use crate::owned_upstream::two_leg::LegId;
use crate::resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy,
    AttachTransportBinding, BitsPerSecond, DevicePrincipal, DeviceSecret, DirectionalRates,
    FeatureOffer, Frame, LegGeneration, OwnerIdentity, Record, ReplayCapacitySpec,
    ReplayStorageGeometry, ReplayStoragePlan, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionEvent,
    SessionFlowId, SessionId, TlsExporterBinding, VersionRange,
};
use crate::shared::TargetAddr;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;
mod transactions;
const TEST_PORT: u16 = 443;
const TTL: Duration = Duration::from_secs(1); // Explicit test-only terminal grace.
fn credentials() -> AttachCredentials {
    AttachCredentials::new(
        DeviceSecret::new([0xde; 32]).unwrap(),
        ResumeSecret::new([0xad; 32]).unwrap(),
    )
    .unwrap()
}
fn binding() -> AttachTransportBinding {
    AttachTransportBinding::new(
        OwnerIdentity::new([0x31; 32]).unwrap(),
        AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
        TlsExporterBinding::new([0x42; 32]).unwrap(),
        DevicePrincipal::new([0x53; 16]).unwrap(),
    )
}
fn committed_leg() -> crate::resumable::CommittedLeg {
    let session_id = SessionId::new([0x11; 16]).unwrap();
    let transport = binding();
    let request = crate::resumable::AttachRequest::new(
        session_id,
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([0x22; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    );
    let proof = credentials().prove(&request, &transport).unwrap();
    AttachAuthority::new(
        session_id,
        LegGeneration::new(1).unwrap(),
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
    .verify_and_commit(&request, &transport, &proof)
    .unwrap()
}
fn replay_storage() -> ReplayStoragePlan {
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(100_000_000).unwrap());
    ReplayCapacitySpec::new(rates, rates, Duration::from_millis(500), Duration::ZERO)
        .unwrap()
        .derive()
        .unwrap()
        .storage_plan(ReplayStorageGeometry::new(62_500, 4).unwrap())
        .unwrap()
}
fn checked_root(
    index: &mut ReadyIndex,
) -> (
    SessionMonotonicClock,
    crate::resumable::CommittedLeg,
    QuinnOwnerControllerOutputRoot<MemoryTarget>,
) {
    let plan = OwnerSessionCapacityPlan::derive(replay_storage(), 8, 128).unwrap();
    let leg = committed_leg();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8).unwrap();
    let ready = index.reserve_session(leg.session_id(), limits).unwrap();
    let required_pending_effects = plan
        .session_config()
        .owner_pending_work_bound()
        .expect("the checked fixture has a representable pending-work bound");
    let supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, leg);
    let target = MemoryTarget::new(
        MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
            .unwrap()
            .with_max_terminal_tombstones(8)
            .unwrap(),
    );
    let executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(1, required_pending_effects, 64).unwrap(),
        supervisor,
        target,
        FlowPortConfig::new(128, 8, 8, 8).unwrap(),
    )
    .unwrap();
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let controller =
        OwnerSessionController::new_checked(executor, clock.clone(), plan, ready, TTL).unwrap();
    let root = match QuinnOwnerControllerOutputRoot::new(controller) {
        Ok(root) => root,
        Err(failure) => {
            let (_controller, error) = failure.into_parts();
            panic!("checked root output allocation failed: {error}");
        }
    };
    (clock, leg, root)
}
fn checked_root_with_pending_recovery(
    index: &mut ReadyIndex,
) -> (
    QuinnOwnerControllerOutputRoot<MemoryTarget>,
    LegIo,
    LegOutboundQueue,
    QuinnLegCompositionGate,
) {
    let plan = OwnerSessionCapacityPlan::derive(replay_storage(), 8, 128).unwrap();
    let required_pending_effects = plan
        .session_config()
        .owner_pending_work_bound()
        .expect("the checked fixture has a representable pending-work bound");
    let initial_leg = committed_leg();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8).unwrap();
    let ready = index
        .reserve_session(initial_leg.session_id(), limits)
        .unwrap();
    let initial_transport = binding();
    let initial_endpoint = EstablishedLeg::for_authenticated_transport(initial_transport);
    let initial_request = crate::resumable::AttachRequest::new(
        SessionId::new([0x11; 16]).unwrap(),
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([0x22; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    );
    let initial_proof = credentials()
        .prove(&initial_request, &initial_transport)
        .unwrap();
    let initial_authority = AttachAuthority::new(
        SessionId::new([0x11; 16]).unwrap(),
        LegGeneration::new(1).unwrap(),
        credentials(),
        AttachPolicy::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
            SESSION_PROTOCOL_VERSION,
            0b1111,
        )
        .unwrap(),
    );
    let mut pending = SessionSupervisor::prepare_owner(plan, initial_authority);
    let mut initial_queue = LegOutboundQueue::for_leg(&initial_endpoint, 4, 8_192).unwrap();
    let bootstrap = pending
        .accept(
            &initial_endpoint,
            initial_endpoint.bind_received_frame(initial_request.to_attach_frame(initial_proof)),
            &mut initial_queue,
        )
        .unwrap()
        .enqueue_acceptance(&mut initial_queue)
        .unwrap();
    let (mut executor, initial_handoff) = OwnerTargetExecutor::from_initial_bootstrap(
        OwnerTargetConfig::new(1, required_pending_effects, 64).unwrap(),
        bootstrap,
        MemoryTarget::new(
            MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
                .unwrap()
                .with_max_terminal_tombstones(8)
                .unwrap(),
        ),
        FlowPortConfig::new(128, 8, 8, 8).unwrap(),
    )
    .unwrap();
    let _ = executor
        .apply_event(SessionEvent::LegLost { leg: initial_leg })
        .expect("the initial no-flow leg loss enters recovery without retained work");
    let replacement_transport = AttachTransportBinding::new(
        OwnerIdentity::new([0x31; 32]).unwrap(),
        AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
        TlsExporterBinding::new([0x43; 32]).unwrap(),
        DevicePrincipal::new([0x53; 16]).unwrap(),
    );
    let replacement_io = LegIo::for_authenticated_transport(
        LegId::B,
        LegEndpointRole::Owner,
        replacement_transport,
        LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
    );
    let replacement_request = crate::resumable::AttachRequest::new(
        SessionId::new([0x11; 16]).unwrap(),
        LegGeneration::new(3).unwrap(),
        AttachNonce::new([0x33; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    );
    let replacement_proof = credentials()
        .prove(&replacement_request, &replacement_transport)
        .unwrap();
    let OwnerTargetAttachPublication::Installed { pending } = executor
        .accept_owner_attach_for_test(
            replacement_io.established_leg(),
            replacement_io
                .established_leg()
                .bind_received_frame(replacement_request.to_attach_frame(replacement_proof)),
        )
        .expect("the exact-next replacement installs one recovery")
    else {
        panic!("the exact-next replacement unexpectedly requested resynchronization");
    };
    let mut acceptance_queue =
        LegOutboundQueue::for_leg(replacement_io.established_leg(), 4, 8_192).unwrap();
    let recovery = pending
        .enqueue_acceptance(&mut acceptance_queue)
        .expect("the replacement acceptance enters its exact bounded FIFO");
    drop((initial_handoff, initial_queue, initial_endpoint));
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let mut owner = OwnerSessionController::new_checked(executor, clock, plan, ready, TTL).unwrap();
    let gate = QuinnLegCompositionGate::mint_for_test();
    let drive = owner
        .begin_attached_recovery(recovery, &gate)
        .expect("the exact enqueued replacement enters owner ownership");
    assert!(drive.needs_execute());
    let root = QuinnOwnerControllerOutputRoot::new(owner)
        .map_err(|failure| failure.into_parts().1)
        .unwrap();
    (root, replacement_io, acceptance_queue, gate)
}
fn open_event(
    leg: crate::resumable::CommittedLeg,
    clock: &SessionMonotonicClock,
) -> OwnerSessionActorEvent {
    let target = TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::LOCALHOST,
        TEST_PORT,
    )));
    OwnerSessionActorEvent::capture_for_test(
        SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Open {
                    flow_id: SessionFlowId::new(1).unwrap(),
                    target,
                },
            )
            .unwrap(),
        },
        clock,
    )
    .unwrap()
}
fn root_with_real_pending_open(
    index: &mut ReadyIndex,
) -> QuinnOwnerControllerOutputRoot<MemoryTarget> {
    let (clock, leg, mut root) = checked_root(index);
    let transaction = root
        .begin_inbound_admission()
        .expect("a cold checked root reserves one ordinary admission transaction");
    let applied = transaction
        .apply_actor_event_and_admit(open_event(leg, &clock))
        .expect("the real OPEN turn is admitted into the exact output root");
    assert!(applied.needs_continue);
    assert_eq!(root.snapshot().pending_continuations, 1);
    root
}
fn drain_real_first_page(root: &mut QuinnOwnerControllerOutputRoot<MemoryTarget>) {
    let mut queue = LegOutboundQueue::new(4, 8_192).unwrap();
    loop {
        match root.begin_service_turn() {
            QuinnOwnerOutputServiceTurn::Drive(transaction) => {
                let driven = transaction.drive(&mut queue).unwrap();
                if let OwnerOutputDrainDrive::PageDrained(page)
                | OwnerOutputDrainDrive::Transmitted {
                    page_drained: Some(page),
                }
                | OwnerOutputDrainDrive::LifecycleInstalled {
                    page_drained: Some(page),
                } = driven
                {
                    assert!(page.needs_continue());
                    break;
                }
            }
            _ => panic!("real first page must drain before continuation"),
        }
    }
    let inventory = root.output_inventory();
    assert_eq!(inventory.phase_markers(), 1);
    assert_eq!(inventory.continuation_markers(), 1);
    assert_eq!(inventory.continuation_transactions(), 0);
    assert!(inventory.phase_drain_consistent());
}
fn drain_real_terminal_page(root: &mut QuinnOwnerControllerOutputRoot<MemoryTarget>) {
    let mut queue = LegOutboundQueue::new(4, 8_192).unwrap();
    loop {
        match root.begin_service_turn() {
            QuinnOwnerOutputServiceTurn::Drive(transaction) => {
                let driven = transaction.drive(&mut queue).unwrap();
                if let OwnerOutputDrainDrive::PageDrained(page)
                | OwnerOutputDrainDrive::Transmitted {
                    page_drained: Some(page),
                }
                | OwnerOutputDrainDrive::LifecycleInstalled {
                    page_drained: Some(page),
                } = driven
                {
                    assert!(!page.needs_continue());
                }
            }
            QuinnOwnerOutputServiceTurn::CompletedBoundary => break,
            _ => panic!("real continuation page must close at one boundary"),
        }
    }
}

use super::*;

mod cleanup;
mod scenarios;

use cleanup::{
    cleanup_composed_registered_fixture, cleanup_registered_fixture,
    settle_raw_local_cancelled_actor_join,
};

use crate::owned_upstream::client_controller::ClientRecoveryTailSettlement;
use crate::owned_upstream::owner_ready::{OwnerReadySessionLimits, ReadyGeometry, ReadyIndex};
use crate::owned_upstream::owner_session_controller::OwnerSessionController;
use crate::owned_upstream::quinn_leg_composition::{
    QuinnCurrentProbeSchedulerAuthority, QuinnOwnerRecoveryInboundDrive,
    QuinnOwnerRecoveryServiceDrive, QuinnOwnerRecoverySessionComposition,
    QuinnOwnerRecoveryTerminalDrive, QuinnOwnerStandbySessionComposition,
    QuinnPhysicalAttachAdmissionErrorKind, QuinnPhysicalAttachAuditError,
    QuinnPhysicalAttachAuthorityKind, QuinnPhysicalAttachShutdownError,
    QuinnPhysicalReplacementCommitError, QuinnStandbyLivenessConfig,
    audit_physical_pending_frame_bytes_for_test,
};
use crate::owned_upstream::reverse_switch::{
    ClientLocalAckStallSource, ClientSwitchArbiter, PhysicalPendingAttachStage, SwitchArbiterError,
    SwitchEpoch,
};
use crate::owned_upstream::standby::{ExactLegProbeResponder, ExactLegProbeResponse};
use crate::resumable::ProtocolError;
use crate::resumable::StandbyNonce;

const PHYSICAL_SESSION: [u8; 16] = [0x93; 16];
const PHYSICAL_STANDBY_NONCE: [u8; 16] = [0x97; 16];

struct OwnedRegisteredFixture {
    listener: QuinnLegActorListener,
    composition: QuinnClientSessionComposition,
    server_a: StartedQuinnLegActor,
    server_b: StartedQuinnLegActor,
    tcp_factory: ResumableTcpPortFactory,
    local_flows: Vec<LocalFlow>,
    peer_policy: AttachPeerPolicy,
    owner_executor: OwnerTargetExecutor<MemoryTarget>,
    owner_capacity_plan: OwnerSessionCapacityPlan,
    owner_active_a: AttachedLeg,
    standby_slot: OwnerStandbySlot,
}

/// The accepted/no-ACK shutdown tracer must retain the complete owner-side B
/// transaction. Unlike `OwnedRegisteredFixture`, this fixture does not expose
/// a raw B actor, Target executor, standby slot, or active recovery handoff to
/// its cleanup path.
struct OwnedComposedRegisteredFixture {
    listener: QuinnLegActorListener,
    composition: QuinnClientSessionComposition,
    server_a: StartedQuinnLegActor,
    owner_b: QuinnOwnerRecoverySessionComposition<MemoryTarget>,
    tcp_factory: ResumableTcpPortFactory,
    local_flows: Vec<LocalFlow>,
    peer_policy: AttachPeerPolicy,
    owner_active_a: AttachedLeg,
    ready_index: ReadyIndex,
}

fn compose_registered_owner_b(fixture: OwnedRegisteredFixture) -> OwnedComposedRegisteredFixture {
    let mut ready_index =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let OwnedRegisteredFixture {
        listener,
        composition,
        server_a,
        server_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_capacity_plan,
        owner_active_a,
        standby_slot,
    } = fixture;
    let owner_clock = server_b.session_clock_for_test();
    let ready_limits =
        OwnerReadySessionLimits::checked(owner_capacity_plan.session_config().max_flows(), 1, 8, 8)
            .unwrap();
    // Declared fixture request input; the installer still checks original source identity.
    let ready = ready_index
        .reserve_session(SessionId::new(PHYSICAL_SESSION).unwrap(), ready_limits)
        .unwrap();
    let controller = OwnerSessionController::new_checked(
        owner_executor,
        owner_clock,
        owner_capacity_plan,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("physical owner controller uses actor B's exact clock and capacity plan");
    let standby = QuinnOwnerStandbySessionComposition::adopt_registered(
        server_b,
        standby_slot,
        QuinnStandbyLivenessConfig::new(Duration::from_millis(30), Duration::from_millis(200))
            .expect("bounded owner B liveness"),
    )
    .expect("physical owner adopts the exact registered B actor and FIFO");
    let owner_b = QuinnOwnerRecoverySessionComposition::adopt(standby, controller)
        .expect("physical owner adopts the controller sharing B's session clock");
    OwnedComposedRegisteredFixture {
        listener,
        composition,
        server_a,
        owner_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_active_a,
        ready_index,
    }
}

fn soft_permit(
    arbiter: &mut ClientSwitchArbiter,
    session_id: SessionId,
    generation: LegGeneration,
) -> crate::owned_upstream::reverse_switch::PendingPhysicalAttachPermit {
    let mut source = ClientLocalAckStallSource::new(session_id, generation);
    source
        .begin_outstanding(SimTime::from_nanos(10), Duration::from_nanos(10))
        .expect("one exact local outstanding episode");
    let trigger = source
        .take_due_trigger(SimTime::from_nanos(20))
        .expect("the local stall deadline is due");
    arbiter
        .on_local_ack_stall(&mut source, trigger)
        .expect("the exact local trigger starts one attach")
        .into_physical_permit()
        .expect("the first trigger owns the unique physical permit")
}

async fn registered_fixture() -> OwnedRegisteredFixture {
    let (fixture, owner_capacity_plan) =
        real_unregistered_standby_fixture_with_target_state_checked(2, 1, true).await;
    let RealUnregisteredStandbyFixture {
        listener,
        mut composition,
        server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy,
        mut owner_executor,
        owner_active_a,
    } = fixture;
    let mut standby_slot = OwnerStandbySlot::new();
    register_owned_real_b(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        SimTime::ZERO,
    )
    .await;
    OwnedRegisteredFixture {
        listener,
        composition,
        server_a,
        server_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_capacity_plan,
        owner_active_a,
        standby_slot,
    }
}

fn admit_soft_physical_attempt(
    fixture: &mut OwnedRegisteredFixture,
    tag: u8,
) -> ClientSwitchArbiter {
    let session_id = SessionId::new(PHYSICAL_SESSION).expect("nonzero session");
    let generation = fixture.composition.snapshot().session.generation();
    let mut arbiter = ClientSwitchArbiter::new(
        session_id,
        generation,
        StandbyNonce::new(PHYSICAL_STANDBY_NONCE).expect("nonzero standby nonce"),
        AttachNonce::new([tag; 16]).expect("nonzero attach nonce"),
        SwitchEpoch::new(u64::from(tag)).expect("nonzero switch epoch"),
    )
    .expect("one exact physical arbiter");
    let permit = soft_permit(&mut arbiter, session_id, generation);
    fixture
        .composition
        .admit_pending_physical_attach(&arbiter, permit)
        .expect("the exact permit enters the physical slot");
    arbiter
}

fn drive_physical_to_awaiting(fixture: &mut OwnedRegisteredFixture, now: SimTime) {
    assert_eq!(
        fixture
            .composition
            .drive_owned_standby_physical_attach(now, &actor_attach_credentials())
            .expect("the physical attach enters B's exact queue"),
        crate::owned_upstream::quinn_leg_composition::QuinnStandbyAttachDrive::AwaitingResponse,
    );
    assert_eq!(
        fixture
            .composition
            .physical_attach_inventory()
            .expect("awaiting physical ownership remains auditable")
            .stage(),
        Some(PhysicalPendingAttachStage::AwaitingAttach),
    );
}

async fn receive_physical_attach_on_owner(
    fixture: &mut OwnedRegisteredFixture,
    now: SimTime,
) -> crate::owned_upstream::leg::LegBoundFrame {
    drive_physical_to_awaiting(fixture, now);
    assert_eq!(
        fixture
            .composition
            .try_flush_registered_standby(now)
            .expect("physical B attach flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(
        TEST_TIMEOUT,
        fixture.server_b.recv_classified(STANDBY_CONTROL_V1),
    )
    .await
    .expect("physical B attach reaches the owner within the local bound")
    .expect("owner B remains live")
    .expect("owner B receives one physical attach");
    let BoundInbound::Session(received) = take_actor_inbound(inbound) else {
        panic!("physical B attach was misclassified as leg control");
    };
    received
}

async fn settle_owner_b_ordered_delivery(
    fixture: &mut OwnedRegisteredFixture,
    now: SimTime,
    stage: &'static str,
) {
    let settlement =
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let progress = fixture.server_b.readiness_epoch();
                match fixture.server_b.try_flush(now).unwrap_or_else(|error| {
                    panic!("{stage}: owner B delivery drain failed: {error:?}")
                }) {
                    QuinnLegActorFlushDisposition::Idle => break,
                    QuinnLegActorFlushDisposition::Submitted => {}
                    QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                        fixture
                            .server_b
                            .wait_for_progress(progress)
                            .await
                            .unwrap_or_else(|error| {
                                panic!("{stage}: owner B delivery progress failed: {error:?}")
                            });
                    }
                }
            }
        })
        .await;
    if settlement.is_err() {
        let client_b = fixture
            .composition
            .registered_standby_actor_diagnostic_for_test();
        let owner_b = fixture.server_b.diagnostic_observation_for_test();
        panic!(
            "{stage}: owner B delivery did not settle within the local bound; client_b={client_b:#?}; owner_b={owner_b:#?}",
        );
    }
}

async fn advance_physical_to_accepted(
    fixture: &mut OwnedRegisteredFixture,
    now: SimTime,
) -> AttachedLeg {
    let received = receive_physical_attach_on_owner(fixture, now).await;
    let OwnerTargetAttachPublication::Installed { pending } = fixture
        .owner_executor
        .accept_registered_owner_attach(
            &mut fixture.standby_slot,
            fixture
                .server_b
                .io
                .as_ref()
                .expect("owner B retains LegIo")
                .established_leg(),
            received,
            fixture
                .server_b
                .queue
                .as_mut()
                .expect("owner B retains its sole FIFO"),
        )
        .expect("owner installs the exact physical B attach")
    else {
        panic!("exact physical B attach unexpectedly resynchronized");
    };
    let recovery = pending
        .enqueue_acceptance(
            fixture
                .server_b
                .queue
                .as_mut()
                .expect("owner B FIFO remains live"),
        )
        .expect("owner queues the exact physical B acceptance");
    let owner_active_b = fixture
        .owner_executor
        .execute_attached_recovery(
            recovery,
            fixture
                .server_b
                .queue
                .as_mut()
                .expect("owner B FIFO remains live"),
        )
        .expect("owner completes bounded physical B recovery")
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetRecoveryOutput::RecoveryCompleted { handoff } => {
                Some(handoff.into_attached_for_test())
            }
            OwnerTargetRecoveryOutput::LegActivated { .. }
            | OwnerTargetRecoveryOutput::ResumeGraceStarted { .. }
            | OwnerTargetRecoveryOutput::TerminalGraceStarted { .. }
            | OwnerTargetRecoveryOutput::SessionExpired => None,
            OwnerTargetRecoveryOutput::NeedsResume { .. } => {
                panic!("bounded physical B recovery unexpectedly needs continuation")
            }
        })
        .expect("owner recovery returns the exact active B authority");
    assert_eq!(
        fixture
            .server_b
            .try_flush(now)
            .expect("owner B acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let accepted_generation = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match fixture.composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::AttachAcceptedPendingCommit { generation } => {
                    break generation;
                }
                outcome => panic!("physical B acceptance did not validate: {outcome:?}"),
            }
        }
    })
    .await
    .expect("client receives physical B acceptance within the local bound");
    assert_eq!(accepted_generation, owner_active_b.generation());
    let inventory = fixture
        .composition
        .physical_attach_inventory()
        .expect("accepted physical ownership remains auditable");
    assert_eq!(
        inventory.stage(),
        Some(PhysicalPendingAttachStage::Accepted)
    );
    assert_eq!(
        inventory.authority_kind(),
        Some(QuinnPhysicalAttachAuthorityKind::Accepted),
        "the exact ATTACH_ACCEPTED response must consume the pending permit into commit authority",
    );
    owner_active_b
}

async fn advance_composed_physical_to_accepted(
    fixture: &mut OwnedComposedRegisteredFixture,
    now: SimTime,
) -> LegGeneration {
    assert_eq!(
        fixture
            .composition
            .drive_owned_standby_physical_attach(now, &actor_attach_credentials())
            .expect("the physical attach enters composition-owned B's exact queue"),
        crate::owned_upstream::quinn_leg_composition::QuinnStandbyAttachDrive::AwaitingResponse,
    );
    assert_eq!(
        fixture
            .composition
            .try_flush_registered_standby(now)
            .expect("physical B attach flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match fixture.owner_b.try_receive_owner_b() {
                QuinnOwnerRecoveryInboundDrive::Idle | QuinnOwnerRecoveryInboundDrive::Blocked => {
                    tokio::task::yield_now().await
                }
                QuinnOwnerRecoveryInboundDrive::RecoveryAccepted { .. } => break,
                outcome => panic!("physical owner did not accept exact B attach: {outcome:?}"),
            }
        }
    })
    .await
    .expect("physical owner accepts B attach within the local bound");
    let owner_generation = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match fixture.owner_b.service_recovery_once() {
                QuinnOwnerRecoveryServiceDrive::Progress {
                    installed_generation: Some(generation),
                    ..
                }
                | QuinnOwnerRecoveryServiceDrive::Installed { generation } => break generation,
                QuinnOwnerRecoveryServiceDrive::Progress {
                    installed_generation: None,
                    ..
                }
                | QuinnOwnerRecoveryServiceDrive::Pressure
                | QuinnOwnerRecoveryServiceDrive::Inert => tokio::task::yield_now().await,
                outcome => panic!("physical owner did not install active B: {outcome:?}"),
            }
        }
    })
    .await
    .expect("physical owner installs active B within the local bound");
    assert_eq!(fixture.owner_b.active_generation(), Some(owner_generation));
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match fixture
                .owner_b
                .try_flush(now)
                .expect("owner B acceptance flush remains live")
            {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::Idle
                | QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                    tokio::task::yield_now().await
                }
            }
        }
    })
    .await
    .expect("physical owner submits B acceptance within the local bound");
    let accepted_generation = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match fixture.composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::AttachAcceptedPendingCommit { generation } => {
                    break generation;
                }
                outcome => panic!("physical B acceptance did not validate: {outcome:?}"),
            }
        }
    })
    .await
    .expect("client receives physical B acceptance within the local bound");
    assert_eq!(accepted_generation, owner_generation);
    let inventory = fixture
        .composition
        .physical_attach_inventory()
        .expect("accepted physical ownership remains auditable");
    assert_eq!(
        inventory.stage(),
        Some(PhysicalPendingAttachStage::Accepted)
    );
    assert_eq!(
        inventory.authority_kind(),
        Some(QuinnPhysicalAttachAuthorityKind::Accepted),
    );
    assert_eq!(
        fixture
            .owner_b
            .try_flush(now)
            .expect("owner B acceptance delivery barrier remains live"),
        QuinnLegActorFlushDisposition::OrderedDeliveryPending,
        "accepted shutdown must begin before owner B publishes DELIVERY",
    );
    owner_generation
}

async fn assert_owner_recovery_waits_for_physical_commit(
    fixture: &mut OwnedRegisteredFixture,
    now: SimTime,
) {
    assert_eq!(
        fixture
            .server_b
            .try_flush(now)
            .expect("owner B acceptance wait remains live"),
        QuinnLegActorFlushDisposition::OrderedDeliveryPending,
        "owner recovery must not cross the acceptance delivery barrier before B is current",
    );
    let progress = fixture.server_b.readiness_epoch();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(25),
            fixture.server_b.wait_for_progress(progress),
        )
        .await
        .is_err(),
        "an uncommitted physical acceptance must not publish RECORD_DELIVERED",
    );
    assert_eq!(
        fixture
            .server_b
            .try_flush(now)
            .expect("owner B acceptance wait remains live after the bounded observation"),
        QuinnLegActorFlushDisposition::OrderedDeliveryPending,
    );
}

struct CommittedOwnerRecoveryObservation {
    leg_activated: bool,
    admission_completed: bool,
    admitted_frames: usize,
    applied_records: usize,
    applied_effects: Vec<SessionEffect>,
}

async fn drive_owner_recovery_through_committed_current(
    fixture: &mut OwnedRegisteredFixture,
    now: SimTime,
) -> CommittedOwnerRecoveryObservation {
    let mut observation = CommittedOwnerRecoveryObservation {
        leg_activated: false,
        admission_completed: false,
        admitted_frames: 0,
        applied_records: 0,
        applied_effects: Vec::new(),
    };
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let owner_drive = fixture
                .server_b
                .try_flush(now)
                .expect("owner B recovery delivery remains live after physical commit");
            match fixture.composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => {}
                QuinnCurrentInboundDrive::PolicyBlocked => {
                    match fixture.composition.drive_recovery_one() {
                        ClientRecoveryDrive::Progress(
                            ClientRecoveryObservation::LegActivated { .. },
                        ) => observation.leg_activated = true,
                        ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted) => {
                            observation.admitted_frames += 1;
                        }
                        ClientRecoveryDrive::AdmissionCompleted { .. } => {
                            observation.admission_completed = true;
                        }
                        outcome => panic!(
                            "client recovery did not make exact bounded progress: {outcome:?}"
                        ),
                    }
                }
                QuinnCurrentInboundDrive::Applied {
                    effects,
                    observation: None,
                } => {
                    observation.applied_records += 1;
                    observation.applied_effects.extend(effects);
                }
                _ => panic!("owner recovery did not enter committed current B"),
            }
            if observation.applied_records == 1
                && observation.admission_completed
                && owner_drive == QuinnLegActorFlushDisposition::Idle
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner recovery crosses current B within the local bound");
    observation
}

async fn advance_physical_to_generation_status(
    fixture: &mut OwnedRegisteredFixture,
    now: SimTime,
    attach_nonce: AttachNonce,
) {
    let received = receive_physical_attach_on_owner(fixture, now).await;
    drop(received);
    let current_generation = fixture.composition.snapshot().session.generation();
    let requested_generation = LegGeneration::new(current_generation.get() + 1)
        .expect("fixture generation has an exact successor");
    let status = Frame::try_new(
        LegGeneration::new(requested_generation.get() + 3)
            .expect("status high-water remains representable"),
        Record::AttachGenerationStatus {
            session_id: SessionId::new(PHYSICAL_SESSION).expect("nonzero session"),
            requested_generation,
            nonce: attach_nonce,
        },
    )
    .expect("the correlated generation status is protocol-valid");
    let leg = fixture
        .server_b
        .io
        .as_ref()
        .expect("owner B retains LegIo")
        .established_leg();
    let queue = fixture
        .server_b
        .queue
        .as_mut()
        .expect("owner B retains its sole FIFO");
    let reservation = queue
        .reserve_attach_acceptance(leg)
        .expect("the received attach reserves the exact empty response FIFO");
    reservation
        .release(queue)
        .expect("the test status path restores the awaiting-response phase");
    let status_seal = leg.standby_seal();
    let _receipt = queue
        .push_authenticated_attach_status(&status_seal, status)
        .expect("the exact status is first on the authenticated B FIFO");
    assert_eq!(
        fixture
            .server_b
            .try_flush(now)
            .expect("owner B status flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match fixture.composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::AttachGenerationStatus => break,
                outcome => panic!("physical B generation status did not validate: {outcome:?}"),
            }
        }
    })
    .await
    .expect("client receives physical B status within the local bound");
    assert_eq!(
        fixture
            .composition
            .physical_attach_inventory()
            .expect("status physical ownership remains auditable")
            .stage(),
        Some(PhysicalPendingAttachStage::GenerationStatus),
    );
    settle_owner_b_ordered_delivery(fixture, now, "generation-status").await;
}

fn settle_physical_composition_and_assert_exact_zero(
    composition: &mut QuinnClientSessionComposition,
    arbiter: &mut ClientSwitchArbiter,
    expected_stage: PhysicalPendingAttachStage,
) {
    let audit = composition
        .settle_pending_physical_attach_for_shutdown(arbiter)
        .expect("the exact retained authority settles synchronously")
        .expect("one pending physical attempt produces one discard audit");
    assert_eq!(audit.physical_stage(), Some(expected_stage));
    assert_eq!(audit.discarded_intents(), 1);
    assert_eq!(audit.discarded_attach_nonces(), 1);
    assert_eq!(audit.discarded_switch_epochs(), 1);
    assert!(arbiter.inventory().is_live_exact_zero());
    assert_eq!(arbiter.inventory().historical_discards(), 1);
    assert!(
        composition
            .physical_attach_inventory()
            .expect("settlement leaves a readable empty physical ledger")
            .is_empty()
    );
}

fn settle_physical_and_assert_exact_zero(
    fixture: &mut OwnedRegisteredFixture,
    arbiter: &mut ClientSwitchArbiter,
    expected_stage: PhysicalPendingAttachStage,
) {
    settle_physical_composition_and_assert_exact_zero(
        &mut fixture.composition,
        arbiter,
        expected_stage,
    );
}

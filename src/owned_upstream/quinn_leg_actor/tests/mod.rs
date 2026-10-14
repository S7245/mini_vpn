use bytes::Bytes;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use super::bootstrap::{BootstrapTaskTerminationGuard, ServerBootstrapHandoff};
use super::{
    ActorOwnerPhase, ActorRole, AttachPeerPolicy, CLOSE_CODE, ListenerRegistry,
    QuinnLegActorAdmission, QuinnLegActorClientConfig as ProductionQuinnLegActorClientConfig,
    QuinnLegActorEnqueueError, QuinnLegActorError, QuinnLegActorFactory,
    QuinnLegActorFlushDisposition, QuinnLegActorInbound, QuinnLegActorLimits,
    QuinnLegActorListener, QuinnLegActorLiveLimits, QuinnLegActorQueueLimits,
    QuinnLegActorServerConfig as ProductionQuinnLegActorServerConfig, QuinnLegActorSnapshot,
    RECORD_HEADER_BYTES, STREAM_PREFACE_BYTES, StartedQuinnLegActor, TestPendingWrite,
    TestWriteKind, binding_confirmation, bootstrap_streams, build_client_endpoint, export_binding,
};
use crate::owned_upstream::client_controller::{
    ClientInboundObservation, ClientRecoveryBeginErrorKind, ClientRecoveryDrive,
    ClientRecoveryObservation, ClientRecoveryStarted,
};
use crate::owned_upstream::leg::{AttachedLeg, BoundInbound, LegTransportTerminalReason};
use crate::owned_upstream::leg_io::{AcceptedInitialAttach, LegIoError, LegIoLimits};
use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
use crate::owned_upstream::owner_target::{
    OwnerTargetAttachPublication, OwnerTargetConfig, OwnerTargetError, OwnerTargetExecutor,
    OwnerTargetOutput, OwnerTargetRecoveryOutput,
};
use crate::owned_upstream::quinn_client_root::{
    QuinnClientRoot, QuinnClientRootOutcome, QuinnClientRootPhase, QuinnClientRootPlan,
    QuinnClientRootStartFailureKind,
};
use crate::owned_upstream::quinn_leg_composition::{
    QuinnClientInitialAttach, QuinnClientInitialAttachBeginErrorKind,
    QuinnClientInitialAttachBeginFailureOwnership, QuinnClientInitialAttachConfig,
    QuinnClientInitialAttachConfigError, QuinnClientInitialAttachDrive,
    QuinnClientInitialAttachDriveErrorKind, QuinnClientInitialAttachDriveFailureOwnership,
    QuinnClientInitialAttachProgress, QuinnClientSessionComposition, QuinnCompositionProgress,
    QuinnCurrentInboundDrive, QuinnCurrentJoinError, QuinnCurrentProbeDeadlineDrive,
    QuinnCurrentTerminalDrive, QuinnCurrentTerminalError, QuinnOwnedStandbyRegistrationDrive,
    QuinnOwnedStandbyRegistrationErrorKind, QuinnRetiredInboundDrive, QuinnStandbyInboundDrive,
    QuinnStandbyJoinDrive, QuinnStandbyJoinStartDisposition, QuinnStandbyLivenessConfig,
    QuinnStandbyProbeArmDrive, QuinnStandbyProbeDrive, QuinnStandbyProbeExpiryDrive,
    QuinnStandbyQueueFailureKind, QuinnTerminalGraceTimer, QuinnTerminalGraceTimerDrive,
};
use crate::owned_upstream::session_clock::SessionMonotonicClock;
use crate::owned_upstream::standby::{
    AcceptedClientRegisteredAttach, ClientRegisteredStandby, ExactAuthenticatedStandby,
    OwnerStandbyAdmission, OwnerStandbySlot, RegisteredAttachResponse,
    RegisteredStandbyProbeAckErrorKind,
};
use crate::owned_upstream::supervisor::{
    OwnerSessionBootstrap, PendingOwnerBootstrap, SessionSupervisor, SessionSupervisorError,
};
use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
use crate::owned_upstream::two_leg::{LegId, SimTime, WireDirection, WireLane, WireRoute};
use crate::owned_upstream::{FlowPortConfig, ResumableTcpPortFactory};
use crate::resumable::{
    AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
    AttachTransportBinding, BitsPerSecond, ByteOffset, DevicePrincipal, DeviceSecret, Direction,
    DirectionalRates, FeatureOffer, FeatureSet, Frame, LegControlFrame, LegControlRecord,
    LegEpochNonce, LegGeneration, LocalFlow, MAX_ENCODED_DATA_FRAME_BYTES, OpenResultCode,
    OwnerIdentity, ProbeSequence, ProtocolError, ReceiveBudgetLimits, Record, RecoveryBudgetSpec,
    RecoveryStageBudgets, ReplayBudgetLimits, ReplayHorizon, ReplayStorageGeometry, ResumeSecret,
    SESSION_PROTOCOL_VERSION, STANDBY_CONTROL_V1, SessionConfig, SessionEffect, SessionError,
    SessionEvent, SessionId, SessionPhase, TcpWindowLimits, VersionRange,
};
use crate::shared::TargetAddr;

mod bootstrap_custody;
mod join_fifo_inventory;
mod ordered_write_cancel;
mod probe_deadline_ownership;
mod r7_actor_capacity;
mod r7_arrival;
mod r7_client_b_control_dispatch;
mod r7_client_recovery_tail_settlement;
mod r7_client_replacement_supersession;
mod r7_initial_acceptance_settlement;
mod r7_owner_b_freshness;
mod r7_owner_physical_recovery;
mod r7_physical_attach_permit;
mod r7_probe_responder;
mod r7_terminal_stamp;

const TEST_TIMEOUT: Duration = Duration::from_secs(3);
const TEST_ENQUEUE_LINEARIZATION_FATAL: &str = "test enqueue linearization fatal";

struct QuinnLegActorClientConfig;

impl QuinnLegActorClientConfig {
    fn new(
        bind_addr: SocketAddr,
        server_addr: SocketAddr,
        server_name: impl Into<String>,
        ca_path: PathBuf,
    ) -> Result<ProductionQuinnLegActorClientConfig, QuinnLegActorError> {
        ProductionQuinnLegActorClientConfig::new(
            bind_addr,
            server_addr,
            server_name,
            ca_path,
            TEST_TIMEOUT,
        )
    }
}

struct QuinnLegActorServerConfig;

impl QuinnLegActorServerConfig {
    fn new(
        bind_addr: SocketAddr,
        cert_path: PathBuf,
        key_path: PathBuf,
        max_live_actors: usize,
    ) -> Result<ProductionQuinnLegActorServerConfig, QuinnLegActorError> {
        ProductionQuinnLegActorServerConfig::new(
            bind_addr,
            cert_path,
            key_path,
            max_live_actors,
            TEST_TIMEOUT,
        )
    }
}

fn ordered_control_for_generation(generation: LegGeneration, sequence: u64) -> LegControlFrame {
    LegControlFrame::try_new(
        generation,
        LegControlRecord::LegProbe {
            session_id: SessionId::new([0x51; 16]).expect("nonzero session id"),
            leg_epoch_nonce: LegEpochNonce::new([0x61; 16]).expect("nonzero leg epoch nonce"),
            probe_sequence: ProbeSequence::new(sequence).expect("nonzero probe sequence"),
        },
        STANDBY_CONTROL_V1,
    )
    .expect("valid ordered leg-control frame")
}

fn ordered_control(sequence: u64) -> LegControlFrame {
    ordered_control_for_generation(
        LegGeneration::new(2).expect("nonzero leg generation"),
        sequence,
    )
}

fn actor_session_config() -> SessionConfig {
    SessionConfig::new(
        4,
        4,
        TcpWindowLimits::new(64, 8).expect("valid client TCP window"),
        TcpWindowLimits::new(64, 8).expect("valid owner TCP window"),
        ReplayBudgetLimits::new(64, 8).expect("valid client replay budget"),
        ReplayBudgetLimits::new(64, 8).expect("valid owner replay budget"),
        ReceiveBudgetLimits::new(128, 16).expect("valid receive budget"),
        64,
    )
    .expect("valid actor session config")
}

fn production_client_root_plan() -> QuinnClientRootPlan {
    production_client_root_plan_with_terminal_policy(4, Duration::from_millis(500))
}

#[derive(Clone, Copy)]
struct PhysicalActorSessionPlans {
    client: QuinnClientRootPlan,
    owner: OwnerSessionCapacityPlan,
}

impl PhysicalActorSessionPlans {
    fn derive() -> Self {
        let client = production_client_root_plan();
        let client_session = client.session_config();
        let owner = OwnerSessionCapacityPlan::derive(
            client.storage(),
            client_session.max_terminal_tombstones(),
            client_session.max_sink_offer_bytes(),
        )
        .expect("the owner session derives from the client's exact replay storage plan");
        assert_eq!(
            client_session,
            owner.session_config(),
            "the symmetric physical fixture installs one role-compatible session geometry",
        );
        Self { client, owner }
    }
}

#[derive(Clone, Copy)]
enum ActorSessionBootstrapPlan {
    LegacyUnproven(SessionConfig),
    PhysicalProven(PhysicalActorSessionPlans),
}

impl ActorSessionBootstrapPlan {
    const fn client_session_config(self) -> SessionConfig {
        match self {
            Self::LegacyUnproven(config) => config,
            Self::PhysicalProven(plans) => plans.client.session_config(),
        }
    }

    fn prepare_owner(self, authority: AttachAuthority) -> PendingOwnerBootstrap {
        match self {
            Self::LegacyUnproven(config) => {
                SessionSupervisor::prepare_owner_unproven_for_test(config, authority)
            }
            Self::PhysicalProven(plans) => SessionSupervisor::prepare_owner(plans.owner, authority),
        }
    }
}

fn production_client_root_plan_with_terminal_policy(
    max_terminal_tombstones: usize,
    terminal_grace: Duration,
) -> QuinnClientRootPlan {
    production_client_root_plan_with_scheduler_policy(max_terminal_tombstones, terminal_grace, 8)
}

fn production_client_root_plan_with_scheduler_policy(
    max_terminal_tombstones: usize,
    terminal_grace: Duration,
    work_budget: usize,
) -> QuinnClientRootPlan {
    let horizon = ReplayHorizon::new(Duration::from_millis(450), Duration::from_millis(50))
        .expect("nonzero exact 500ms replay horizon");
    let recovery = RecoveryBudgetSpec::new(
        horizon,
        RecoveryStageBudgets {
            hard_liveness_after_normal_ack: Duration::from_millis(250),
            reverse_stall_after_normal_ack: Duration::from_millis(100),
            authenticated_hint: Duration::from_millis(150),
            attach_install: Duration::from_millis(100),
            replay_to_first_application_ack: Duration::from_millis(100),
        },
    );
    let rate = BitsPerSecond::new(1_024).expect("nonzero local tracer rate");
    QuinnClientRootPlan::preflight(
        recovery,
        DirectionalRates::symmetric(rate),
        DirectionalRates::symmetric(rate),
        ReplayStorageGeometry::new(8, 4).expect("four exact bounded flow slots"),
        max_terminal_tombstones,
        terminal_grace,
        64,
        FlowPortConfig::new(64, 8, 8, 8).expect("exact checked TCP source bounds"),
        4,
        work_budget,
    )
    .expect("root timing, storage, reducer, mailbox, and scheduler plan is coherent")
}

fn composition_actor_limits() -> QuinnLegActorLimits {
    composition_actor_limits_with_outbound_messages(4)
}

fn composition_actor_limits_with_outbound_messages(
    outbound_messages: usize,
) -> QuinnLegActorLimits {
    QuinnLegActorLimits::new(
        LegIoLimits::new(MAX_ENCODED_DATA_FRAME_BYTES, 16, 1_048_576, 16, 1_048_576)
            .expect("bounded multi-stage LegIo limits"),
        QuinnLegActorQueueLimits::new(4, 262_144).expect("bounded multi-stage actor FIFO"),
        QuinnLegActorLiveLimits::new(outbound_messages, 262_144, 4, 262_144, 4, 4, 72)
            .expect("bounded multi-stage live actor lanes"),
    )
    .expect("valid multi-stage composition actor limits")
}

fn actor_attach_credentials() -> AttachCredentials {
    AttachCredentials::new(
        DeviceSecret::new([0x91; 32]).expect("nonzero device secret"),
        ResumeSecret::new([0x92; 32]).expect("nonzero resume secret"),
    )
    .expect("distinct attach credentials")
}

fn actor_attach_authority(binding: AttachTransportBinding) -> AttachAuthority {
    AttachAuthority::new(
        SessionId::new([0x93; 16]).expect("nonzero actor session id"),
        LegGeneration::new(1).expect("nonzero authority generation"),
        actor_attach_credentials(),
        AttachPolicy::new(
            binding.owner_identity(),
            binding.alpn(),
            binding.device_principal(),
            SESSION_PROTOCOL_VERSION,
            STANDBY_CONTROL_V1.bits(),
        )
        .expect("actor attach policy matches the physical binding"),
    )
}

fn take_actor_inbound(inbound: QuinnLegActorInbound) -> BoundInbound {
    let mut consumed = None;
    inbound.consume_for_test(|inbound| consumed = Some(inbound));
    consumed.expect("actor inbound is consumed exactly once")
}

async fn wait_actor_ordered_delivery(actor: &mut StartedQuinnLegActor) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match actor
                .try_flush(SimTime::ZERO)
                .expect("delivery progress remains live")
            {
                QuinnLegActorFlushDisposition::Idle => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Submitted => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("ordered actor delivery completes within the test bound");
}

async fn wait_actor_delivery_ack_drain(mut snapshot: impl FnMut() -> QuinnLegActorSnapshot) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if snapshot().reserved_delivery_acks() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the actor delivery writer drains its bounded ACK reservation");
}

async fn wait_composition_current_ordered_delivery(
    composition: &mut QuinnClientSessionComposition,
) {
    tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition
                    .try_flush_current(SimTime::ZERO)
                    .expect("composition ordered-delivery progress remains live")
                {
                    crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle => break,
                    crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending
                    | crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted => {
                        tokio::task::yield_now().await;
                    }
                }
            }
        })
        .await
        .expect("composition ordered delivery completes within the test bound");
}

async fn complete_initial_actor_attach(
    client: &mut StartedQuinnLegActor,
    owner: &mut StartedQuinnLegActor,
) -> (AcceptedInitialAttach, OwnerSessionBootstrap) {
    complete_initial_actor_attach_with_config(client, owner, actor_session_config()).await
}

async fn complete_initial_actor_attach_with_config(
    client: &mut StartedQuinnLegActor,
    owner: &mut StartedQuinnLegActor,
    session_config: SessionConfig,
) -> (AcceptedInitialAttach, OwnerSessionBootstrap) {
    complete_initial_actor_attach_with_plan(
        client,
        owner,
        ActorSessionBootstrapPlan::LegacyUnproven(session_config),
    )
    .await
}

async fn complete_initial_actor_attach_with_capacity_plans(
    client: &mut StartedQuinnLegActor,
    owner: &mut StartedQuinnLegActor,
    plans: PhysicalActorSessionPlans,
) -> (AcceptedInitialAttach, OwnerSessionBootstrap) {
    complete_initial_actor_attach_with_plan(
        client,
        owner,
        ActorSessionBootstrapPlan::PhysicalProven(plans),
    )
    .await
}

async fn complete_initial_actor_attach_with_plan(
    client: &mut StartedQuinnLegActor,
    owner: &mut StartedQuinnLegActor,
    session_plan: ActorSessionBootstrapPlan,
) -> (AcceptedInitialAttach, OwnerSessionBootstrap) {
    let client_leg = client.io.as_ref().expect("client actor owns its LegIo");
    let owner_leg = owner.io.as_ref().expect("owner actor owns its LegIo");
    let binding = client_leg.established_leg().standby_transport_binding();
    assert_eq!(
        owner_leg.established_leg().standby_transport_binding(),
        binding,
        "the real loopback actor pair agrees on exporter-derived binding",
    );
    let credentials = actor_attach_credentials();
    let request = AttachRequest::new(
        SessionId::new([0x93; 16]).expect("nonzero actor session id"),
        LegGeneration::new(2).expect("nonzero initial generation"),
        AttachNonce::new([0x94; 16]).expect("nonzero attach nonce"),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION)
            .expect("single supported version"),
        FeatureOffer::new(STANDBY_CONTROL_V1.bits(), STANDBY_CONTROL_V1.bits())
            .expect("standby control feature offer"),
    );
    let proof = credentials
        .prove(&request, &binding)
        .expect("initial actor attach proof");
    let pending = client_leg
        .established_leg()
        .begin_attach(request)
        .bind_initial_frame(request.to_attach_frame(proof))
        .expect("bind exact initial actor frame");
    let awaiting = pending
        .enqueue(client.queue.as_mut().expect("client actor owns its FIFO"))
        .expect("initial actor queue admits request");
    assert_eq!(
        client
            .try_flush(SimTime::ZERO)
            .expect("client initial attach flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );

    let owner_inbound =
        tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("owner initial receive is bounded")
            .expect("owner actor remains live")
            .expect("owner receives initial attach");
    let BoundInbound::Session(received) = take_actor_inbound(owner_inbound) else {
        panic!("initial attach was misclassified as leg control");
    };
    wait_actor_ordered_delivery(client).await;

    let authority = actor_attach_authority(binding);
    let mut pending_owner = session_plan.prepare_owner(authority);
    let publication = pending_owner
        .accept(
            owner
                .io
                .as_ref()
                .expect("owner actor retains its LegIo")
                .established_leg(),
            received,
            owner.queue.as_mut().expect("owner actor owns its FIFO"),
        )
        .expect("owner authenticates initial actor attach");
    let owner_bootstrap = publication
        .enqueue_acceptance(owner.queue.as_mut().expect("owner FIFO remains live"))
        .expect("owner queues initial acceptance");
    assert_eq!(
        owner
            .try_flush(SimTime::ZERO)
            .expect("owner acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let client_inbound =
        tokio::time::timeout(TEST_TIMEOUT, client.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("client initial response is bounded")
            .expect("client actor remains live")
            .expect("client receives initial acceptance");
    let BoundInbound::Session(response) = take_actor_inbound(client_inbound) else {
        panic!("initial acceptance was misclassified as leg control");
    };
    let accepted = awaiting
        .validate_response(
            client.queue.as_ref().expect("client FIFO remains live"),
            response,
        )
        .expect("client validates exact initial acceptance");
    (accepted, owner_bootstrap)
}

#[test]
fn production_initial_attach_capacity_overflow_rejects_before_actor_creation() {
    let overflowing_flow_bound = usize::MAX
        .checked_div(3)
        .and_then(|value| value.checked_add(1))
        .expect("usize can express the first overflowing triple");
    let config = SessionConfig::new(
        overflowing_flow_bound,
        1,
        TcpWindowLimits::new(1, 1).expect("minimal client TCP window"),
        TcpWindowLimits::new(1, 1).expect("minimal owner TCP window"),
        ReplayBudgetLimits::new(1, 1).expect("minimal client replay budget"),
        ReplayBudgetLimits::new(1, 1).expect("minimal owner replay budget"),
        ReceiveBudgetLimits::new(1, 1).expect("minimal receive budget"),
        1,
    )
    .expect("primitive session capacities remain individually valid");

    assert_eq!(
        QuinnClientInitialAttachConfig::preflight(config),
        Err(QuinnClientInitialAttachConfigError::RecoveryEffectBoundOverflow),
        "the production initial API requires a checked witness before it can accept an actor",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn production_initial_attach_rejects_wrong_role_or_leg_before_wire_bytes() {
    let initial_config = QuinnClientInitialAttachConfig::preflight(actor_session_config())
        .expect("initial capacity arithmetic passes before actor creation");
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid shared-listener configuration"),
    )
    .await
    .expect("bind wrong-route listener");
    let request = || {
        AttachRequest::new(
            SessionId::new([0x93; 16]).expect("nonzero actor session id"),
            LegGeneration::new(2).expect("nonzero initial generation"),
            AttachNonce::new([0x94; 16]).expect("nonzero attach nonce"),
            VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION)
                .expect("single supported version"),
            FeatureOffer::new(STANDBY_CONTROL_V1.bits(), STANDBY_CONTROL_V1.bits())
                .expect("standby control feature offer"),
        )
    };
    let credentials = actor_attach_credentials();

    let (mut client_a, owner_a) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let client_a_before = client_a.actor_snapshot();
    let owner_a_request = request();
    let failure = match QuinnClientInitialAttach::begin(
        initial_config,
        owner_a,
        owner_a_request,
        &credentials,
    ) {
        Ok(_) => panic!("the owner-A actor must not impersonate the initial client"),
        Err(failure) => failure,
    };
    assert!(matches!(
        failure.kind(),
        QuinnClientInitialAttachBeginErrorKind::WrongActorRoute
    ));
    let mut owner_a = match failure.into_ownership() {
        QuinnClientInitialAttachBeginFailureOwnership::Request {
            config,
            request,
            actor,
        } => {
            assert_eq!(config, initial_config);
            assert_eq!(request, owner_a_request);
            *actor
        }
        QuinnClientInitialAttachBeginFailureOwnership::Signed { .. }
        | QuinnClientInitialAttachBeginFailureOwnership::Pending { .. } => {
            panic!("wrong actor routing must reject before signing or enqueue")
        }
    };
    let rejected = owner_a.actor_snapshot();
    assert_eq!(rejected.outbound_queue_messages(), 0);
    assert_eq!(rejected.outbound_queue_bytes(), 0);
    assert_eq!(rejected.outbound_owned_messages(), 0);
    assert_eq!(rejected.outbound_owned_bytes(), 0);
    assert_eq!(client_a.actor_snapshot(), client_a_before);
    let (client_joined, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client_a.join(), owner_a.join())
    })
    .await
    .expect("wrong-role pair cleanup is bounded");
    assert_eq!(
        client_joined
            .expect("wrong-role client peer joins cleanly")
            .owned_bytes(),
        0
    );
    assert_eq!(
        owner_joined
            .expect("rejected owner-A joins cleanly")
            .owned_bytes(),
        0
    );

    let (client_b, mut owner_b) =
        connect_real_actor_pair(&mut listener, LegId::B, limits, peer_policy).await;
    let owner_b_before = owner_b.actor_snapshot();
    let client_b_request = request();
    let failure = match QuinnClientInitialAttach::begin(
        initial_config,
        client_b,
        client_b_request,
        &credentials,
    ) {
        Ok(_) => panic!("the client-B actor must not impersonate initial client-A"),
        Err(failure) => failure,
    };
    assert!(matches!(
        failure.kind(),
        QuinnClientInitialAttachBeginErrorKind::WrongActorRoute
    ));
    let mut client_b = match failure.into_ownership() {
        QuinnClientInitialAttachBeginFailureOwnership::Request {
            config,
            request,
            actor,
        } => {
            assert_eq!(config, initial_config);
            assert_eq!(request, client_b_request);
            *actor
        }
        QuinnClientInitialAttachBeginFailureOwnership::Signed { .. }
        | QuinnClientInitialAttachBeginFailureOwnership::Pending { .. } => {
            panic!("wrong leg routing must reject before signing or enqueue")
        }
    };
    let rejected = client_b.actor_snapshot();
    assert_eq!(rejected.outbound_queue_messages(), 0);
    assert_eq!(rejected.outbound_queue_bytes(), 0);
    assert_eq!(rejected.outbound_owned_messages(), 0);
    assert_eq!(rejected.outbound_owned_bytes(), 0);
    assert_eq!(owner_b.actor_snapshot(), owner_b_before);
    let (client_joined, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client_b.join(), owner_b.join())
    })
    .await
    .expect("wrong-leg pair cleanup is bounded");
    assert_eq!(
        client_joined
            .expect("rejected client-B joins cleanly")
            .owned_bytes(),
        0
    );
    assert_eq!(
        owner_joined
            .expect("wrong-leg owner peer joins cleanly")
            .owned_bytes(),
        0
    );

    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("wrong-route listener cleanup is bounded")
        .expect("both rejected actor pairs release the listener slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_initial_attach_does_not_consume_acceptance_before_request_delivery() {
    let initial_config = QuinnClientInitialAttachConfig::preflight(actor_session_config())
        .expect("initial capacity arithmetic passes before actor creation");
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid shared-listener configuration"),
    )
    .await
    .expect("bind ordered-initial-attach listener");
    let (client, mut owner) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let binding = owner
        .io
        .as_ref()
        .expect("test owner retains its private LegIo")
        .established_leg()
        .standby_transport_binding();
    let credentials = actor_attach_credentials();
    let request = AttachRequest::new(
        SessionId::new([0x93; 16]).expect("nonzero actor session id"),
        LegGeneration::new(2).expect("nonzero initial generation"),
        AttachNonce::new([0x94; 16]).expect("nonzero attach nonce"),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION)
            .expect("single supported version"),
        FeatureOffer::new(STANDBY_CONTROL_V1.bits(), STANDBY_CONTROL_V1.bits())
            .expect("standby control feature offer"),
    );
    let delivery_gate = Arc::clone(&owner.delivery_writer_gate);
    delivery_gate.arm();

    let initial = QuinnClientInitialAttach::begin(initial_config, client, request, &credentials)
        .expect("production initial owner signs and queues exact ATTACH");
    let initial = match initial
        .drive_one(SimTime::ZERO)
        .expect("initial request submission remains live")
    {
        QuinnClientInitialAttachDrive::InProgress {
            owner,
            progress: QuinnClientInitialAttachProgress::Submitted,
        } => *owner,
        _ => panic!("the first drive must submit exactly one initial ATTACH"),
    };
    let owner_inbound =
        tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(FeatureSet::default()))
            .await
            .expect("owner initial receive is bounded")
            .expect("owner actor remains live")
            .expect("owner receives the production initial ATTACH");
    let BoundInbound::Session(received) = take_actor_inbound(owner_inbound) else {
        panic!("production initial ATTACH was misclassified as leg control");
    };
    tokio::time::timeout(TEST_TIMEOUT, delivery_gate.wait_until_blocked())
        .await
        .expect("owner holds request DELIVERED before its wire write");

    let authority = actor_attach_authority(binding);
    let mut pending_owner =
        SessionSupervisor::prepare_owner_unproven_for_test(actor_session_config(), authority);
    let publication = pending_owner
        .accept(
            owner
                .io
                .as_ref()
                .expect("test owner retains its LegIo")
                .established_leg(),
            received,
            owner
                .queue
                .as_mut()
                .expect("test owner retains its sole FIFO"),
        )
        .expect("owner authenticates the production initial ATTACH");
    let owner_bootstrap = publication
        .enqueue_acceptance(
            owner
                .queue
                .as_mut()
                .expect("owner FIFO remains live for acceptance"),
        )
        .expect("owner queues the exact initial acceptance");
    assert_eq!(
        owner
            .try_flush(SimTime::ZERO)
            .expect("owner acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );

    let mut initial = initial;
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if initial.actor_snapshot_for_test().queued_inbound_messages() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("acceptance reaches client inbound while request DELIVERED is held");
    initial = match initial
        .drive_one(SimTime::ZERO)
        .expect("ordered request receipt remains retryable")
    {
        QuinnClientInitialAttachDrive::InProgress {
            owner,
            progress: QuinnClientInitialAttachProgress::OrderedDeliveryPending,
        } => *owner,
        QuinnClientInitialAttachDrive::Ready(_) => {
            panic!("acceptance must not be consumed before request DELIVERED")
        }
        QuinnClientInitialAttachDrive::InProgress { progress, .. } => {
            panic!("request must remain at the exact delivery barrier: {progress:?}")
        }
    };
    assert_eq!(
        initial.actor_snapshot_for_test().queued_inbound_messages(),
        1,
        "the sole acceptance remains actor-owned behind ordered request delivery",
    );

    let cursor = initial.progress_cursor();
    delivery_gate.release();
    initial
        .wait_for_progress(cursor)
        .await
        .expect("request DELIVERED wakes the exact initial owner");
    let mut composition = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let cursor = initial.progress_cursor();
            match initial
                .drive_one(SimTime::ZERO)
                .expect("post-DELIVERED initial drive remains live")
            {
                QuinnClientInitialAttachDrive::Ready(composition) => break *composition,
                QuinnClientInitialAttachDrive::InProgress { owner, progress } => {
                    initial = *owner;
                    assert!(matches!(
                        progress,
                        QuinnClientInitialAttachProgress::OrderedDeliveryPending
                            | QuinnClientInitialAttachProgress::AwaitingResponse
                    ));
                    initial
                        .wait_for_progress(cursor)
                        .await
                        .expect("remaining exact initial progress wakes the owner");
                }
            }
        }
    })
    .await
    .expect("request delivery then admits the sole initial acceptance");
    assert_eq!(composition.snapshot().session.phase(), SessionPhase::Active);

    let (client_joined, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), owner.join())
    })
    .await
    .expect("ordered initial composition cleanup is bounded");
    assert_eq!(
        client_joined
            .expect("ordered initial client joins structurally")
            .owned_bytes(),
        0
    );
    assert_eq!(
        owner_joined
            .expect("ordered initial owner joins structurally")
            .owned_bytes(),
        0
    );
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("ordered initial listener cleanup is bounded")
        .expect("ordered initial actor releases the listener slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_initial_attach_rejects_feature_control_before_acceptance() {
    let initial_config = QuinnClientInitialAttachConfig::preflight(actor_session_config())
        .expect("initial capacity arithmetic passes before actor creation");
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid shared-listener configuration"),
    )
    .await
    .expect("bind pre-acceptance feature listener");
    let (client, mut owner) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let credentials = actor_attach_credentials();
    let request = AttachRequest::new(
        SessionId::new([0x93; 16]).expect("nonzero actor session id"),
        LegGeneration::new(2).expect("nonzero initial generation"),
        AttachNonce::new([0x94; 16]).expect("nonzero attach nonce"),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION)
            .expect("single supported version"),
        FeatureOffer::new(STANDBY_CONTROL_V1.bits(), STANDBY_CONTROL_V1.bits())
            .expect("standby control feature offer"),
    );
    let initial = QuinnClientInitialAttach::begin(initial_config, client, request, &credentials)
        .expect("production initial owner signs and queues exact ATTACH");
    let mut initial = match initial
        .drive_one(SimTime::ZERO)
        .expect("initial request submission remains live")
    {
        QuinnClientInitialAttachDrive::InProgress {
            owner,
            progress: QuinnClientInitialAttachProgress::Submitted,
        } => *owner,
        _ => panic!("the first drive must submit exactly one initial ATTACH"),
    };
    let owner_inbound =
        tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(FeatureSet::default()))
            .await
            .expect("owner initial receive is bounded")
            .expect("owner actor remains live")
            .expect("owner receives the production initial ATTACH");
    let BoundInbound::Session(received) = take_actor_inbound(owner_inbound) else {
        panic!("production initial ATTACH was misclassified as leg control");
    };
    drop(received);

    initial = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let cursor = initial.progress_cursor();
            match initial
                .drive_one(SimTime::ZERO)
                .expect("initial request delivery remains live")
            {
                QuinnClientInitialAttachDrive::Ready(_) => {
                    panic!("initial client cannot become active without acceptance")
                }
                QuinnClientInitialAttachDrive::InProgress { owner, progress } => {
                    initial = *owner;
                    if progress == QuinnClientInitialAttachProgress::AwaitingResponse {
                        break initial;
                    }
                    initial
                        .wait_for_progress(cursor)
                        .await
                        .expect("initial request delivery wakes the exact owner");
                }
            }
        }
    })
    .await
    .expect("initial client reaches the pre-acceptance response barrier");

    let cursor = initial.progress_cursor();
    let receipt = owner
        .enqueue_ordered_leg_control(
            ordered_control_for_generation(request.requested_generation(), 73),
            STANDBY_CONTROL_V1,
        )
        .expect("owner queues one feature-gated pre-acceptance record");
    assert_eq!(
        owner
            .try_flush(SimTime::ZERO)
            .expect("pre-acceptance control submission remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    initial
        .wait_for_progress(cursor)
        .await
        .expect("pre-acceptance record wakes the exact initial owner");
    let failure = match initial.drive_one(SimTime::ZERO) {
        Ok(QuinnClientInitialAttachDrive::Ready(_)) => {
            panic!("feature-gated control cannot impersonate ATTACH acceptance")
        }
        Ok(QuinnClientInitialAttachDrive::InProgress { progress, .. }) => {
            panic!("feature-gated control must fail exact decoding, got {progress:?}")
        }
        Err(failure) => failure,
    };
    match failure.kind() {
        QuinnClientInitialAttachDriveErrorKind::Actor(QuinnLegActorError::RootFailed(cause)) => {
            assert!(matches!(
                cause.as_ref(),
                QuinnLegActorError::LegIo(LegIoError::Decode(
                    ProtocolError::StandbyControlNotNegotiated { negotiated: 0 }
                ))
            ))
        }
        other => panic!("unexpected pre-acceptance rejection: {other:?}"),
    }
    assert!(
        !receipt.was_delivered(),
        "a rejected pre-acceptance feature record never gains ordered delivery",
    );
    let mut client = match failure.into_ownership() {
        QuinnClientInitialAttachDriveFailureOwnership::Awaiting(initial) => {
            (*initial).into_cancelled_actor()
        }
        QuinnClientInitialAttachDriveFailureOwnership::Actor { .. }
        | QuinnClientInitialAttachDriveFailureOwnership::Bootstrap(_) => {
            panic!("decode rejection must preserve AwaitingInitialAttach ownership")
        }
    };

    let (client_joined, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), owner.join())
    })
    .await
    .expect("pre-acceptance rejection cleanup is bounded");
    let client_joined = client_joined.expect("rejected initial client joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert!(matches!(
        client_joined.terminal_failure(),
        Some(QuinnLegActorError::LegIo(LegIoError::Decode(
            ProtocolError::StandbyControlNotNegotiated { negotiated: 0 }
        )))
    ));
    let owner_joined = owner_joined.expect("feature-sending owner joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("pre-acceptance feature listener cleanup is bounded")
        .expect("semantic rejection still structurally releases the listener slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_initial_client_attach_hands_exact_ownership_to_root_and_joins_zero() {
    let root_plan = production_client_root_plan();
    let initial_config = QuinnClientInitialAttachConfig::preflight(root_plan.session_config())
        .expect("initial capacity arithmetic passes before actor creation");
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid shared-listener configuration"),
    )
    .await
    .expect("bind initial-attach listener");
    let (client, mut owner) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let binding = owner
        .io
        .as_ref()
        .expect("test owner retains its private LegIo")
        .established_leg()
        .standby_transport_binding();
    let credentials = actor_attach_credentials();
    let request = AttachRequest::new(
        SessionId::new([0x93; 16]).expect("nonzero actor session id"),
        LegGeneration::new(2).expect("nonzero initial generation"),
        AttachNonce::new([0x94; 16]).expect("nonzero attach nonce"),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION)
            .expect("single supported version"),
        FeatureOffer::new(STANDBY_CONTROL_V1.bits(), STANDBY_CONTROL_V1.bits())
            .expect("standby control feature offer"),
    );
    let initial = QuinnClientInitialAttach::begin(initial_config, client, request, &credentials)
        .expect("production initial owner signs and queues exact ATTACH");
    let initial = match initial
        .drive_one(SimTime::ZERO)
        .expect("initial request submission remains live")
    {
        QuinnClientInitialAttachDrive::InProgress {
            owner,
            progress: QuinnClientInitialAttachProgress::Submitted,
        } => *owner,
        _ => panic!("the first drive must submit exactly one initial ATTACH"),
    };

    let owner_inbound =
        tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(FeatureSet::default()))
            .await
            .expect("owner initial receive is bounded")
            .expect("owner actor remains live")
            .expect("owner receives the production initial ATTACH");
    let BoundInbound::Session(received) = take_actor_inbound(owner_inbound) else {
        panic!("production initial ATTACH was misclassified as leg control");
    };
    let authority = actor_attach_authority(binding);
    let mut pending_owner =
        SessionSupervisor::prepare_owner_unproven_for_test(root_plan.session_config(), authority);
    let publication = pending_owner
        .accept(
            owner
                .io
                .as_ref()
                .expect("test owner retains its LegIo")
                .established_leg(),
            received,
            owner
                .queue
                .as_mut()
                .expect("test owner retains its sole FIFO"),
        )
        .expect("owner authenticates the production initial ATTACH");
    let mut initial = initial;
    let mut wait_count = 0_u64;
    let mut drive_count = 1_u64;
    let (parked_initial, response_cursor) = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let cursor = initial.progress_cursor();
            match initial
                .drive_one(SimTime::ZERO)
                .expect("production initial drive remains ownership-preserving")
            {
                QuinnClientInitialAttachDrive::Ready(_) => {
                    panic!("client cannot bootstrap before the owner sends acceptance")
                }
                QuinnClientInitialAttachDrive::InProgress { owner, progress } => {
                    initial = *owner;
                    drive_count = drive_count.saturating_add(1);
                    match progress {
                        QuinnClientInitialAttachProgress::Submitted => {}
                        QuinnClientInitialAttachProgress::OrderedDeliveryPending => {
                            wait_count = wait_count.saturating_add(1);
                            initial
                                .wait_for_progress(cursor)
                                .await
                                .expect("actor progress wakes the initial owner without polling");
                        }
                        QuinnClientInitialAttachProgress::AwaitingResponse => {
                            break (initial, cursor);
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("initial request reaches an exact response wait within the local bound");

    let initial = parked_initial;
    let mut cancelled_response_wait = Box::pin(initial.wait_for_progress(response_cursor));
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(
            cancelled_response_wait.as_mut(),
            context,
        ))
    })
    .await;
    assert!(matches!(first_poll, std::task::Poll::Pending));
    drop(cancelled_response_wait);
    let response_cursor = initial.progress_cursor();
    let (response_progress, owner_bootstrap) =
        tokio::join!(initial.wait_for_progress(response_cursor), async {
            let owner_bootstrap = publication
                .enqueue_acceptance(
                    owner
                        .queue
                        .as_mut()
                        .expect("owner FIFO remains live for acceptance"),
                )
                .expect("owner queues the exact initial acceptance");
            assert_eq!(
                owner
                    .try_flush(SimTime::ZERO)
                    .expect("owner acceptance flush remains live"),
                QuinnLegActorFlushDisposition::Submitted,
            );
            owner_bootstrap
        },);
    response_progress.expect("the exact response wakes the parked initial owner");
    wait_count = wait_count.saturating_add(1);

    let mut initial = initial;
    let composition = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let cursor = initial.progress_cursor();
            match initial
                .drive_one(SimTime::ZERO)
                .expect("response authentication retains the exact actor owner")
            {
                QuinnClientInitialAttachDrive::Ready(composition) => break *composition,
                QuinnClientInitialAttachDrive::InProgress { owner, progress } => {
                    initial = *owner;
                    drive_count = drive_count.saturating_add(1);
                    match progress {
                        QuinnClientInitialAttachProgress::Submitted => {}
                        QuinnClientInitialAttachProgress::OrderedDeliveryPending
                        | QuinnClientInitialAttachProgress::AwaitingResponse => {
                            wait_count = wait_count.saturating_add(1);
                            initial
                                .wait_for_progress(cursor)
                                .await
                                .expect("late initial progress wakes without polling");
                        }
                    }
                }
            }
        }
    })
    .await
    .expect("production initial ATTACH reaches composition within the local bound");
    assert!(
        wait_count > 0,
        "the tracer must cross an actual parked owner wait"
    );
    assert!(
        drive_count <= 8,
        "bounded progress cannot become a busy loop"
    );
    assert_eq!(composition.snapshot().session.phase(), SessionPhase::Active);
    assert_eq!(
        composition.snapshot().session.generation(),
        request.requested_generation()
    );

    let mut root = QuinnClientRoot::start(root_plan, Box::new(composition))
        .expect("the checked composition transfers whole into one production root");
    let mut cancelled_ready = Box::pin(root.wait_ready());
    let first_ready_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(cancelled_ready.as_mut(), context))
    })
    .await;
    assert!(matches!(first_ready_poll, std::task::Poll::Pending));
    drop(cancelled_ready);
    let ready = tokio::time::timeout(TEST_TIMEOUT, root.wait_ready())
        .await
        .expect("root Ready publication is bounded")
        .expect("the actor first-failure barrier publishes root Ready");
    let repeated_ready = root
        .wait_ready()
        .await
        .expect("a repeated Ready wait observes the same published root");
    assert!(ready.same_root(&repeated_ready));
    assert_eq!(ready.phase(), QuinnClientRootPhase::Running);

    let (root_outcome, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join(), owner.join())
    })
    .await
    .expect("root and peer structural cleanup are bounded");
    let joined = match root_outcome {
        QuinnClientRootOutcome::Joined(joined) => *joined,
        other => panic!("production root must mint one clean joined witness: {other:?}"),
    };
    assert!(joined.is_zero());
    assert_eq!(joined.actor_owned_messages(), 0);
    assert_eq!(joined.actor_owned_bytes(), 0);
    assert_eq!(joined.factory_owned_bytes(), 0);
    assert_eq!(joined.factory_owned_segments(), 0);
    assert_eq!(joined.flow_drivers(), 0);
    assert_eq!(joined.pending_effects(), 0);
    assert_eq!(joined.timers(), 0);
    assert_eq!(joined.mailbox_messages(), 0);
    assert_eq!(ready.phase(), QuinnClientRootPhase::Joined);
    let owner_joined = owner_joined.expect("initial owner joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("initial listener cleanup is bounded")
        .expect("initial production owner releases the listener slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_root_presignal_lifecycle_cutoff_skips_queued_inbound() {
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let generation = composition.snapshot().session.generation();
    owner
        .queue
        .as_mut()
        .expect("pre-signal peer retains its sole FIFO")
        .push(
            Frame::try_new(
                generation,
                Record::Reset {
                    flow_id: crate::resumable::SessionFlowId::new(901)
                        .expect("nonzero unknown pre-signal flow id"),
                    reason: crate::resumable::ResetReason::TargetFailure,
                },
            )
            .expect("unknown-flow RESET is valid bounded wire input"),
        )
        .expect("pre-signal peer queues one bounded RESET");
    assert_eq!(
        owner
            .try_flush(SimTime::ZERO)
            .expect("pre-signal RESET flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition.current_actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.inbound_owned_bytes() > 0
                && snapshot.composition_owned_messages() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one invalid semantic record is queued before root startup");

    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("pre-signal fixture transfers into the sole root");
    assert_eq!(root.live_scan_count_for_test(), 0);
    root.begin_shutdown_lifecycle_only_for_test()
        .expect("shutdown lifecycle CAS wins before the root's first poll");
    assert!(
        root.shutdown_signal_is_open_for_test(),
        "the tracer remains inside the lifecycle-before-watch-signal gap",
    );

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if root.live_scan_count_for_test() > 0 || root.shutdown_cleanup_started_for_test() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("root observes the lifecycle cutoff without waiting for the watch signal");
    assert_eq!(
        root.live_scan_count_for_test(),
        0,
        "no command/effect/timer/driver/flush/inbound category may run after lifecycle cutoff",
    );
    assert!(root.shutdown_cleanup_started_for_test());

    let (root_outcome, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join(), owner.join())
    })
    .await
    .expect("pre-signal root and peer cleanup are bounded");
    let QuinnClientRootOutcome::Joined(joined) = root_outcome else {
        panic!("queued post-cutoff input contaminated root shutdown: {root_outcome:?}")
    };
    assert!(joined.is_zero());
    let owner_joined = owner_joined.expect("pre-signal peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("pre-signal listener cleanup is bounded")
        .expect("pre-signal shutdown releases the exact actor slot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_root_running_cutoff_linearizes_before_ready_inbound_turn() {
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let generation = composition.snapshot().session.generation();
    let inbound_publication = composition.arm_current_inbound_publication_block_for_test();
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("running-cutoff fixture transfers into the sole root");
    root.wait_ready()
        .await
        .expect("root publishes Ready before the running cutoff race");
    let live_inbound = root.arm_before_live_inbound_for_test();
    let live_inbound_waiter = tokio::task::spawn_blocking(move || {
        live_inbound.wait_until_blocked();
        live_inbound
    });

    owner
        .queue
        .as_mut()
        .expect("running-cutoff peer retains its sole FIFO")
        .push(
            Frame::try_new(
                generation,
                Record::Reset {
                    flow_id: crate::resumable::SessionFlowId::new(902)
                        .expect("nonzero unknown running-cutoff flow id"),
                    reason: crate::resumable::ResetReason::TargetFailure,
                },
            )
            .expect("unknown-flow running-cutoff RESET is valid wire input"),
        )
        .expect("running-cutoff peer queues one bounded RESET");
    assert_eq!(
        owner
            .try_flush(SimTime::ZERO)
            .expect("running-cutoff RESET flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, inbound_publication.wait_until_blocked())
        .await
        .expect("running-cutoff RESET reserves actor publication");
    inbound_publication.release();
    let live_inbound = tokio::time::timeout(TEST_TIMEOUT, live_inbound_waiter)
        .await
        .expect("root reaches the exact post-check/pre-turn boundary")
        .expect("live inbound barrier waiter remains joinable");
    let scans_before_cutoff = root.live_scan_count_for_test();

    root.begin_shutdown_lifecycle_only_for_test()
        .expect("running lifecycle cutoff wins before inbound turn admission");
    assert!(root.shutdown_signal_is_open_for_test());
    live_inbound.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !root.shutdown_cleanup_started_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("running cutoff enters shutdown without waiting for watch publication");
    assert_eq!(
        root.live_scan_count_for_test(),
        scans_before_cutoff,
        "the lifecycle CAS must exclude the already-selected inbound category turn",
    );

    let (root_outcome, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join(), owner.join())
    })
    .await
    .expect("running-cutoff root and peer cleanup are bounded");
    let QuinnClientRootOutcome::Joined(joined) = root_outcome else {
        panic!("post-cutoff inbound reached session semantics: {root_outcome:?}")
    };
    assert!(joined.is_zero());
    let owner_joined = owner_joined.expect("running-cutoff peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("running-cutoff listener cleanup is bounded")
        .expect("running cutoff releases the exact actor slot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_root_running_cutoff_returns_exact_selected_command_without_commit() {
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("selected-command fixture transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before selected-command cutoff");
    tokio::time::timeout(TEST_TIMEOUT, async {
        while root.live_wait_count_for_test() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("quiescent root reaches its non-polling live wait");

    let selected_command = root.arm_after_selected_command_for_test();
    let selected_waiter = tokio::task::spawn_blocking(move || {
        selected_command.wait_until_blocked();
        selected_command
    });
    let target = TargetAddr::IpPort(
        "127.0.0.1:9443"
            .parse()
            .expect("valid selected-command target"),
    );
    let opener = tokio::spawn({
        let handle = handle.clone();
        let target = target.clone();
        async move { handle.open_tcp(target).await }
    });
    let selected_command = tokio::time::timeout(TEST_TIMEOUT, selected_waiter)
        .await
        .expect("selected command reaches the exact post-dequeue boundary")
        .expect("selected-command barrier waiter remains joinable");

    root.begin_shutdown_lifecycle_only_for_test()
        .expect("running lifecycle cutoff wins before selected command commit");
    assert!(root.shutdown_signal_is_open_for_test());
    selected_command.release();
    let failure = tokio::time::timeout(TEST_TIMEOUT, opener)
        .await
        .expect("selected command receives one bounded cutoff result")
        .expect("selected command task remains joinable")
        .expect_err("selected command cannot commit after lifecycle cutoff");
    assert!(matches!(
        failure.kind(),
        crate::owned_upstream::quinn_client_root::QuinnClientRootOpenFailureKind::RootNotRunning(
            QuinnClientRootPhase::ShuttingDown
        )
    ));
    assert_eq!(failure.into_target(), target);
    assert_eq!(
        owner.actor_snapshot().queued_inbound_messages(),
        0,
        "the rejected command cannot emit a physical OPEN",
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !root.shutdown_cleanup_started_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("selected-command cutoff enters shutdown without the watch publication");

    let (root_outcome, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join(), owner.join())
    })
    .await
    .expect("selected-command root and peer cleanup are bounded");
    let QuinnClientRootOutcome::Joined(joined) = root_outcome else {
        panic!("selected command contaminated clean shutdown: {root_outcome:?}")
    };
    assert!(joined.is_zero());
    let owner_joined = owner_joined.expect("selected-command peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("selected-command listener cleanup is bounded")
        .expect("selected-command cutoff releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_root_open_then_shutdown_drains_reset_timer_and_driver_to_zero() {
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("quiescent composition transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before OPEN");
    let target = TargetAddr::IpPort(
        "127.0.0.1:443"
            .parse()
            .expect("valid loopback shutdown target"),
    );
    let flow = handle
        .open_tcp(target.clone())
        .await
        .expect("root atomically installs one TCP OPEN");
    let flow_id = flow.local_flow().flow_id();

    let inbound = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("root OPEN reaches the peer within the local bound")
        .expect("peer actor remains live for root OPEN")
        .expect("peer receives exactly one root OPEN");
    let BoundInbound::Session(received) = take_actor_inbound(inbound) else {
        panic!("root OPEN was misclassified as leg control")
    };
    assert!(matches!(
        received.into_frame_for_test().record(),
        Record::Open {
            flow_id: received_flow,
            target: received_target,
            ..
        } if *received_flow == flow_id && received_target == &target
    ));

    let mut root_join = Box::pin(root.join());
    let reset_inbound = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::select! {
            biased;
            inbound = owner.recv_classified(STANDBY_CONTROL_V1) => inbound,
            outcome = &mut root_join => {
                panic!("root joined before its RESET reached the live peer: {outcome:?}")
            }
        }
    })
    .await
    .expect("logical root shutdown emits RESET within the local bound")
    .expect("peer stays live until root RESET")
    .expect("logical root shutdown emits one RESET");
    let BoundInbound::Session(received) = take_actor_inbound(reset_inbound) else {
        panic!("root RESET was misclassified as leg control")
    };
    let reset = received.into_frame_for_test();
    assert!(matches!(
        reset.record(),
        Record::Reset {
            flow_id: reset_flow,
            reason: crate::resumable::ResetReason::LocalAbandon,
        } if *reset_flow == flow_id
    ));
    let root_outcome = tokio::time::timeout(TEST_TIMEOUT, root_join)
        .await
        .expect("terminal grace and structural root join are bounded");
    let joined = match root_outcome {
        QuinnClientRootOutcome::Joined(joined) => *joined,
        other => panic!("flow-owning root must drain to one clean witness: {other:?}"),
    };
    assert!(joined.is_zero());
    assert_eq!(handle.phase(), QuinnClientRootPhase::Joined);
    assert_eq!(
        flow.try_send_reset(crate::resumable::ResetReason::TargetFailure)
            .unwrap_err(),
        crate::owned_upstream::FlowPortError::Closed,
    );
    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("root shutdown peer join is bounded")
        .expect("root shutdown peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, flow, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("flow-owning root listener cleanup is bounded")
        .expect("flow-owning root releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_root_retries_exact_fifo_after_fixed_actor_pressure() {
    let plan = production_client_root_plan_with_terminal_policy(4, Duration::from_millis(500));
    let limits = composition_actor_limits_with_outbound_messages(1);
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture_with_plan_and_limits(plan, limits).await;
    // Hold the first post-bootstrap DATA-stream record after the actor has
    // acquired its sole outbound message permit. The second root OPEN is
    // therefore already committed in LegIo's exact FIFO when Flush sees
    // ActorPressure.
    let writer_block = composition.arm_current_data_writer_block_for_test();
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("pressure fixture transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("pressure fixture publishes Ready");
    let first = handle
        .open_tcp(TargetAddr::IpPort(
            "127.0.0.1:1443"
                .parse()
                .expect("valid first pressure target"),
        ))
        .await
        .expect("first OPEN commits before the writer barrier");
    tokio::time::timeout(TEST_TIMEOUT, writer_block.wait_until_blocked())
        .await
        .expect("the sole actor permit is held by the first OPEN");
    let second = handle
        .open_tcp(TargetAddr::IpPort(
            "127.0.0.1:2443"
                .parse()
                .expect("valid second pressure target"),
        ))
        .await
        .expect("second OPEN remains owned by the exact LegIo FIFO");

    // A current-thread timer proves the bounded root remains cooperative
    // while pressure is unchanged. The pre-fix path changed lifecycle to
    // FailedJoined on its first retry instead.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(handle.phase(), QuinnClientRootPhase::Running);
    writer_block.release();

    let expected = [first.local_flow().flow_id(), second.local_flow().flow_id()];
    for expected_flow in expected {
        let inbound = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("pressure release resumes the exact FIFO")
            .expect("pressure peer remains live")
            .expect("pressure peer receives both retained OPENs");
        let BoundInbound::Session(received) = take_actor_inbound(inbound) else {
            panic!("pressure OPEN was misclassified as leg control")
        };
        assert!(matches!(
            received.into_frame_for_test().record(),
            Record::Open { flow_id, .. } if *flow_id == expected_flow
        ));
    }

    let mut root_join = Box::pin(root.join());
    let mut reset_flows = Vec::with_capacity(2);
    for _ in 0..2 {
        let inbound = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::select! {
                biased;
                inbound = owner.recv_classified(STANDBY_CONTROL_V1) => inbound,
                outcome = &mut root_join => {
                    panic!("pressure root joined before both RESET deliveries: {outcome:?}")
                }
            }
        })
        .await
        .expect("pressure RESET delivery remains bounded")
        .expect("pressure peer remains live for RESET")
        .expect("pressure peer receives both RESETs");
        let BoundInbound::Session(received) = take_actor_inbound(inbound) else {
            panic!("pressure RESET was misclassified as leg control")
        };
        let frame = received.into_frame_for_test();
        let Record::Reset { flow_id, .. } = frame.record() else {
            panic!("pressure shutdown emitted a non-RESET record")
        };
        reset_flows.push(*flow_id);
    }
    reset_flows.sort_unstable_by_key(|flow_id| flow_id.get());
    let mut expected_resets = expected;
    expected_resets.sort_unstable_by_key(|flow_id| flow_id.get());
    assert_eq!(reset_flows, expected_resets);

    let outcome = tokio::time::timeout(TEST_TIMEOUT, root_join)
        .await
        .expect("pressure root observes both exact delivery receipts");
    let QuinnClientRootOutcome::Joined(joined) = outcome else {
        panic!("ordinary actor pressure contaminated root shutdown: {outcome:?}")
    };
    assert!(joined.is_zero());
    assert_eq!(handle.phase(), QuinnClientRootPhase::Joined);

    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("pressure peer join is bounded")
        .expect("pressure peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, first, second, writer_block, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("pressure listener cleanup is bounded")
        .expect("pressure root releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_root_shutdown_does_not_wait_the_configured_terminal_grace() {
    let plan =
        production_client_root_plan_with_terminal_policy(4, Duration::from_secs(24 * 60 * 60));
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture_with_plan(plan).await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("long-grace fixture transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before long-grace shutdown");
    let flow = handle
        .open_tcp(TargetAddr::IpPort(
            "127.0.0.1:443"
                .parse()
                .expect("valid long-grace shutdown target"),
        ))
        .await
        .expect("root installs one live flow before long-grace shutdown");
    let flow_id = flow.local_flow().flow_id();

    let open = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("long-grace OPEN reaches the peer")
        .expect("long-grace peer remains live for OPEN")
        .expect("long-grace peer receives one OPEN");
    let BoundInbound::Session(open) = take_actor_inbound(open) else {
        panic!("long-grace OPEN was misclassified as leg control")
    };
    assert!(matches!(
        open.into_frame_for_test().record(),
        Record::Open {
            flow_id: opened_flow,
            ..
        } if *opened_flow == flow_id
    ));

    let mut root_join = Box::pin(root.join());
    let reset = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::select! {
            biased;
            inbound = owner.recv_classified(STANDBY_CONTROL_V1) => inbound,
            outcome = &mut root_join => {
                panic!("root joined before its long-grace RESET reached the peer: {outcome:?}")
            }
        }
    })
    .await
    .expect("long-grace shutdown emits RESET within the local bound")
    .expect("long-grace peer remains live for RESET")
    .expect("long-grace peer receives one RESET");
    let BoundInbound::Session(reset) = take_actor_inbound(reset) else {
        panic!("long-grace RESET was misclassified as leg control")
    };
    assert!(matches!(
        reset.into_frame_for_test().record(),
        Record::Reset {
            flow_id: reset_flow,
            reason: crate::resumable::ResetReason::LocalAbandon,
        } if *reset_flow == flow_id
    ));

    let root_outcome = tokio::time::timeout(TEST_TIMEOUT, root_join)
        .await
        .expect("whole-session shutdown must not wait the 24-hour terminal grace");
    let joined = match root_outcome {
        QuinnClientRootOutcome::Joined(joined) => *joined,
        other => panic!("long-grace root must still mint one clean witness: {other:?}"),
    };
    assert!(joined.is_zero());
    assert_eq!(handle.phase(), QuinnClientRootPhase::Joined);
    assert_eq!(
        flow.try_send_reset(crate::resumable::ResetReason::TargetFailure)
            .unwrap_err(),
        crate::owned_upstream::FlowPortError::Closed,
    );

    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("long-grace peer join is bounded")
        .expect("long-grace peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, flow, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("long-grace listener cleanup is bounded")
        .expect("long-grace root releases the exact actor slot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_root_shutdown_cannot_lose_final_delivery_between_scan_and_park() {
    let plan =
        production_client_root_plan_with_scheduler_policy(4, Duration::from_secs(24 * 60 * 60), 1);
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture_with_plan(plan).await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("lost-wake fixture transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before the lost-wake interleaving");
    let flow = handle
        .open_tcp(TargetAddr::IpPort(
            "127.0.0.1:443"
                .parse()
                .expect("valid lost-wake shutdown target"),
        ))
        .await
        .expect("root installs one flow before the lost-wake interleaving");
    let flow_id = flow.local_flow().flow_id();

    let open = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("lost-wake OPEN reaches the peer")
        .expect("lost-wake peer remains live for OPEN")
        .expect("lost-wake peer receives one OPEN");
    let BoundInbound::Session(open) = take_actor_inbound(open) else {
        panic!("lost-wake OPEN was misclassified as leg control")
    };
    assert!(matches!(
        open.into_frame_for_test().record(),
        Record::Open {
            flow_id: opened_flow,
            ..
        } if *opened_flow == flow_id
    ));

    let slice_boundary = root.arm_shutdown_before_terminal_timer_slice_for_test();
    let slice_waiter = tokio::task::spawn_blocking(move || {
        slice_boundary.wait_until_blocked();
        slice_boundary
    });
    let mut root_join = Box::pin(root.join());
    let slice_boundary = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::select! {
            biased;
            outcome = &mut root_join => {
                panic!("root joined before reaching the budget-one Flush boundary: {outcome:?}")
            }
            barrier = slice_waiter => {
                barrier.expect("budget-one slice barrier waiter remains joinable")
            }
        }
    })
    .await
    .expect("root reaches the exact budget-one Flush/TerminalTimer boundary");

    let reset = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("the retained ordered RESET reaches the peer at the barrier")
        .expect("lost-wake peer remains live for RESET")
        .expect("lost-wake peer receives one RESET");
    let BoundInbound::Session(reset) = take_actor_inbound(reset) else {
        panic!("lost-wake RESET was misclassified as leg control")
    };
    assert!(matches!(
        reset.into_frame_for_test().record(),
        Record::Reset {
            flow_id: reset_flow,
            reason: crate::resumable::ResetReason::LocalAbandon,
        } if *reset_flow == flow_id
    ));
    // Peer classification publishes the sole DELIVERED edge after Flush
    // has already reported idle, but before the next cooperative slice.
    // The complete idle cycle must retain its older cursor across this
    // budget-one boundary.
    slice_boundary.release();

    let outcome = tokio::time::timeout(TEST_TIMEOUT, root_join)
        .await
        .expect("the pre-scan cursor observes the sole final delivery edge");
    let QuinnClientRootOutcome::Joined(joined) = outcome else {
        panic!("the exact lost-wake interleaving contaminated shutdown: {outcome:?}")
    };
    assert!(joined.is_zero());
    assert_eq!(handle.phase(), QuinnClientRootPhase::Joined);

    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("lost-wake peer join is bounded")
        .expect("lost-wake peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, flow, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("lost-wake listener cleanup is bounded")
        .expect("lost-wake root releases the exact actor slot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_root_shutdown_leaves_post_seal_late_inbound_for_actor_cleanup() {
    let plan =
        production_client_root_plan_with_terminal_policy(4, Duration::from_secs(24 * 60 * 60));
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture_with_plan(plan).await;
    let generation = composition.snapshot().session.generation();
    let late_publication = composition.arm_current_inbound_publication_block_for_test();
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("late-inbound fixture transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before late-inbound shutdown");
    let flow = handle
        .open_tcp(TargetAddr::IpPort(
            "127.0.0.1:443"
                .parse()
                .expect("valid late-inbound shutdown target"),
        ))
        .await
        .expect("root installs one flow before late-inbound shutdown");
    let flow_id = flow.local_flow().flow_id();
    let open = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("late-inbound OPEN reaches the peer")
        .expect("late-inbound peer remains live for OPEN")
        .expect("late-inbound peer receives one OPEN");
    assert!(matches!(take_actor_inbound(open), BoundInbound::Session(_)));

    owner
        .queue
        .as_mut()
        .expect("late-inbound peer retains its sole FIFO")
        .push(
            Frame::try_new(
                generation,
                Record::Data {
                    flow_id,
                    direction: Direction::TargetToClient,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"late-after-seal"),
                },
            )
            .expect("late DATA is valid wire input"),
        )
        .expect("late-inbound peer queues one bounded DATA record");
    assert_eq!(
        owner
            .try_flush(SimTime::ZERO)
            .expect("late DATA flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, late_publication.wait_until_blocked())
        .await
        .expect("late DATA reserves publication before shutdown scans it");

    // The first barrier is reached with the ordered local-abandon RESET
    // waiting for peer delivery. Releasing DATA here creates an exact
    // post-seal actor progress edge; the following complete scheduler
    // scan must not feed that retired-flow record back into the reducer.
    let first_park = root.arm_shutdown_pre_park_block_for_test();
    let second_park = root.arm_shutdown_pre_park_block_for_test();
    let first_waiter = tokio::task::spawn_blocking(move || {
        first_park.wait_until_blocked();
        first_park
    });
    let mut root_join = Box::pin(root.join());
    let first_park = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::select! {
            biased;
            outcome = &mut root_join => {
                panic!("root finished before the first late-inbound barrier: {outcome:?}")
            }
            barrier = first_waiter => {
                barrier.expect("first late-inbound barrier waiter remains joinable")
            }
        }
    })
    .await
    .expect("root reaches the first exact post-scan barrier");
    late_publication.release();
    first_park.release();

    let second_waiter = tokio::task::spawn_blocking(move || {
        second_park.wait_until_blocked();
        second_park
    });
    let second_park = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::select! {
            biased;
            outcome = &mut root_join => {
                panic!("late retired-flow inbound contaminated clean shutdown: {outcome:?}")
            }
            control = second_waiter => {
                control.expect("second shutdown barrier waiter remains live")
            }
        }
    })
    .await
    .expect("root ignores late inbound and reaches the next bounded park");

    let reset = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("late-inbound RESET reaches the peer")
        .expect("late-inbound peer remains live for RESET")
        .expect("late-inbound peer receives one RESET");
    let BoundInbound::Session(reset) = take_actor_inbound(reset) else {
        panic!("late-inbound RESET was misclassified as leg control")
    };
    assert!(matches!(
        reset.into_frame_for_test().record(),
        Record::Reset {
            flow_id: reset_flow,
            reason: crate::resumable::ResetReason::LocalAbandon,
        } if *reset_flow == flow_id
    ));
    second_park.release();

    let outcome = tokio::time::timeout(TEST_TIMEOUT, root_join)
        .await
        .expect("late raw inbound is dropped only by bounded actor cleanup");
    let QuinnClientRootOutcome::Joined(joined) = outcome else {
        panic!("late actor-owned inbound changed clean shutdown: {outcome:?}")
    };
    assert!(joined.is_zero());
    assert_eq!(handle.phase(), QuinnClientRootPhase::Joined);

    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("late-inbound peer join is bounded")
        .expect("late-inbound peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, flow, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("late-inbound listener cleanup is bounded")
        .expect("late-inbound root releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_root_silent_peer_hits_absolute_shutdown_deadline_without_busy_loop() {
    let plan = production_client_root_plan_with_terminal_policy(4, Duration::from_millis(20));
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture_with_plan(plan).await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("silent-peer fixture transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before silent-peer shutdown");
    let flow = handle
        .open_tcp(TargetAddr::IpPort(
            "127.0.0.1:443"
                .parse()
                .expect("valid silent-peer shutdown target"),
        ))
        .await
        .expect("root installs one flow before silent-peer shutdown");
    let open = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("silent-peer OPEN reaches the peer")
        .expect("silent peer remains live for OPEN")
        .expect("silent peer receives one OPEN");
    assert!(matches!(take_actor_inbound(open), BoundInbound::Session(_)));

    // Deliberately do not dequeue the ordered RESET. No DELIVERED edge can
    // retire it, so the absolute terminal deadline must select failed
    // structural cancellation rather than a zero-delay scheduler spin.
    let outcome = tokio::time::timeout(TEST_TIMEOUT, root.join())
        .await
        .expect("silent-peer shutdown is bounded by the absolute deadline");
    match outcome {
        QuinnClientRootOutcome::FailedJoined {
            joined: Some(joined),
            first_failure,
        } => {
            assert!(
                joined.is_zero(),
                "silent-peer structural witness: {joined:?}"
            );
            assert!(
                first_failure.contains("RESET delivery exceeded"),
                "the graceful-delivery deadline remains the first sticky cause: {first_failure}",
            );
        }
        other => panic!("silent peer must fail closed after bounded cleanup: {other:?}"),
    }
    assert_eq!(handle.phase(), QuinnClientRootPhase::FailedJoined);
    assert_eq!(
        flow.try_send_close().unwrap_err(),
        crate::owned_upstream::FlowPortError::Closed,
    );

    owner.request_cancel();
    let owner_outcome = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("silent peer structural cleanup is bounded");
    if let Ok(joined) = owner_outcome {
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }
    drop((root, flow, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("silent-peer listener cleanup is bounded")
        .expect("silent-peer failure releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_root_running_reset_deadline_fails_before_late_shutdown() {
    let plan = production_client_root_plan_with_terminal_policy(4, Duration::from_millis(20));
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture_with_plan(plan).await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("delayed-shutdown fixture transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before the running RESET deadline");
    let flow = handle
        .open_tcp(TargetAddr::IpPort(
            "127.0.0.1:443"
                .parse()
                .expect("valid delayed-shutdown target"),
        ))
        .await
        .expect("root installs one flow before delayed shutdown");
    let open = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("delayed-shutdown OPEN reaches the peer")
        .expect("peer remains live for delayed-shutdown OPEN")
        .expect("peer receives delayed-shutdown OPEN");
    assert!(matches!(take_actor_inbound(open), BoundInbound::Session(_)));

    flow.try_send_reset(crate::resumable::ResetReason::TargetFailure)
        .expect("local RESET enters the reserved driver lane");
    // Keep the peer silent. The normal Running scheduler—not root.join—
    // must retain the same terminal token and make the missed RESET
    // delivery deadline sticky before a later shutdown request arrives.
    tokio::time::timeout(TEST_TIMEOUT, async {
        while handle.phase() != QuinnClientRootPhase::FailedJoined {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the Running RESET deadline reaches bounded failed cleanup");

    let outcome = tokio::time::timeout(TEST_TIMEOUT, root.join())
        .await
        .expect("a late join observes the already-sealed root outcome");
    match outcome {
        QuinnClientRootOutcome::FailedJoined {
            joined: Some(joined),
            first_failure,
        } => {
            assert!(joined.is_zero());
            assert!(
                first_failure.contains("terminal RESET delivery exceeded"),
                "the exact Running deadline remains the first cause: {first_failure}",
            );
        }
        other => panic!("the silent Running RESET must fail closed: {other:?}"),
    }

    owner.request_cancel();
    let owner_outcome = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("delayed-shutdown peer cleanup is bounded");
    if let Ok(joined) = owner_outcome {
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }
    drop((root, flow, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("delayed-shutdown listener cleanup is bounded")
        .expect("delayed-shutdown failure releases the exact actor slot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_open_reply_after_commit_emits_exact_open_then_local_abandon() {
    let plan = production_client_root_plan_with_terminal_policy(4, Duration::from_millis(20));
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture_with_plan(plan).await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("cancellation fixture transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before cancellation race");
    let publication = root.arm_open_reply_publication_block_for_test();
    let opener = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .open_tcp(TargetAddr::IpPort(
                    "127.0.0.1:9443"
                        .parse()
                        .expect("valid cancelled-open target"),
                ))
                .await
        }
    });
    tokio::task::yield_now().await;
    publication.wait_until_blocked();
    opener.abort();
    assert!(
        opener
            .await
            .expect_err("OPEN observer is cancelled at the postcommit barrier")
            .is_cancelled()
    );
    publication.release();

    let mut records = Vec::new();
    for _ in 0..2 {
        let inbound = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("cancelled OPEN/RESET reaches the peer")
            .expect("cancelled-open peer remains live")
            .expect("peer receives both cancellation records");
        let BoundInbound::Session(received) = take_actor_inbound(inbound) else {
            panic!("cancelled OPEN record was misclassified as leg control")
        };
        records.push(received.into_frame_for_test());
    }
    let Record::Open {
        flow_id: opened_flow,
        ..
    } = records[0].record()
    else {
        panic!("postcommit cancellation did not preserve OPEN first")
    };
    assert!(matches!(
        records[1].record(),
        Record::Reset {
            flow_id,
            reason: crate::resumable::ResetReason::LocalAbandon,
        } if flow_id == opened_flow
    ));

    let outcome = tokio::time::timeout(TEST_TIMEOUT, root.join())
        .await
        .expect("cancelled OPEN terminal timer and root join are bounded");
    let QuinnClientRootOutcome::Joined(joined) = outcome else {
        panic!("postcommit cancellation contaminated root ownership: {outcome:?}")
    };
    assert!(joined.is_zero());
    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("cancelled-open peer join is bounded")
        .expect("cancelled-open peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("cancelled-open listener cleanup is bounded")
        .expect("cancelled-open root releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_open_reply_before_command_commit_has_no_model_factory_or_wire_effect() {
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("precommit-cancellation fixture transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before precommit cancellation");
    let mut open = Box::pin(
        handle.open_tcp(TargetAddr::IpPort(
            "127.0.0.1:10443"
                .parse()
                .expect("valid precommit-cancellation target"),
        )),
    );
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(open.as_mut(), context))
    })
    .await;
    assert!(matches!(first_poll, std::task::Poll::Pending));
    assert_eq!(
        handle.command_available_capacity_for_test(),
        plan.command_capacity() - 1,
        "the first poll owns one exact queued command",
    );
    drop(open);

    tokio::time::timeout(TEST_TIMEOUT, async {
        while handle.command_available_capacity_for_test() != plan.command_capacity() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("root observes the closed reply before any commit");
    assert_eq!(owner.actor_snapshot().queued_inbound_messages(), 0);

    let (root_outcome, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join(), owner.join())
    })
    .await
    .expect("precommit-cancelled root and peer joins are bounded");
    let QuinnClientRootOutcome::Joined(joined) = root_outcome else {
        panic!("precommit cancellation mutated root ownership: {root_outcome:?}")
    };
    assert!(joined.is_zero());
    let owner_joined = owner_joined.expect("precommit-cancelled peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("precommit-cancelled listener cleanup is bounded")
        .expect("precommit-cancelled root releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_root_shutdown_drains_two_flows_through_one_terminal_slot() {
    let plan = production_client_root_plan_with_terminal_policy(1, Duration::from_millis(20));
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture_with_plan(plan).await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("single-terminal-slot composition transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before two-flow OPEN");
    let mut flows = Vec::new();
    let mut flow_ids = Vec::new();
    for port in [443_u16, 8443_u16] {
        let flow = handle
            .open_tcp(TargetAddr::IpPort(
                format!("127.0.0.1:{port}")
                    .parse()
                    .expect("valid two-flow target"),
            ))
            .await
            .expect("root admits each flow below registry capacity");
        flow_ids.push(flow.local_flow().flow_id());
        flows.push(flow);
        let inbound = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("each two-flow OPEN reaches the peer")
            .expect("two-flow peer remains live")
            .expect("peer receives each two-flow OPEN");
        assert!(matches!(
            take_actor_inbound(inbound),
            BoundInbound::Session(_)
        ));
    }

    let mut root_join = Box::pin(root.join());
    let mut reset_ids = Vec::new();
    for _ in 0..2 {
        let inbound = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::select! {
                biased;
                inbound = owner.recv_classified(STANDBY_CONTROL_V1) => inbound,
                outcome = &mut root_join => {
                    panic!("two-flow root joined before both RESETs: {outcome:?}")
                }
            }
        })
        .await
        .expect("each capacity-serialized RESET is bounded")
        .expect("two-flow peer stays live through RESET")
        .expect("two-flow shutdown emits RESET");
        let BoundInbound::Session(received) = take_actor_inbound(inbound) else {
            panic!("two-flow RESET was misclassified as leg control")
        };
        let frame = received.into_frame_for_test();
        let Record::Reset {
            flow_id,
            reason: crate::resumable::ResetReason::LocalAbandon,
        } = frame.record()
        else {
            panic!("two-flow shutdown emitted a non-RESET frame: {frame:?}")
        };
        reset_ids.push(*flow_id);
    }
    reset_ids.sort_unstable();
    flow_ids.sort_unstable();
    assert_eq!(reset_ids, flow_ids);

    let outcome = tokio::time::timeout(TEST_TIMEOUT, root_join)
        .await
        .expect("one terminal slot serially releases both tombstones");
    let QuinnClientRootOutcome::Joined(joined) = outcome else {
        panic!("two-flow single-slot shutdown did not join cleanly: {outcome:?}")
    };
    assert!(joined.is_zero());
    for flow in &flows {
        assert_eq!(
            flow.try_send_close().unwrap_err(),
            crate::owned_upstream::FlowPortError::Closed,
        );
    }
    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("two-flow peer join is bounded")
        .expect("two-flow peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, flows, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("two-flow listener cleanup is bounded")
        .expect("two-flow root releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn production_root_registry_n_plus_one_rejects_before_commit_and_returns_exact_target() {
    let plan = production_client_root_plan_with_terminal_policy(4, Duration::from_millis(20));
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture_with_plan(plan).await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("four-slot composition transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before registry filling");
    let mut flows = Vec::new();
    for port in 4_430_u16..4_434_u16 {
        flows.push(
            handle
                .open_tcp(TargetAddr::IpPort(
                    format!("127.0.0.1:{port}")
                        .parse()
                        .expect("valid registry target"),
                ))
                .await
                .expect("each of the four checked registry slots admits one flow"),
        );
        let inbound = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("each registry OPEN reaches the peer")
            .expect("registry peer remains live")
            .expect("peer receives each admitted registry OPEN");
        assert!(matches!(
            take_actor_inbound(inbound),
            BoundInbound::Session(_)
        ));
    }

    let mut host = String::with_capacity(512);
    host.push_str("registry-n-plus-one.example");
    let host_ptr = host.as_ptr();
    let host_capacity = host.capacity();
    let failure = handle
        .open_tcp(TargetAddr::DomainPort { host, port: 443 })
        .await
        .expect_err("logical registry N + 1 fails before factory/model/FIFO commit");
    assert!(matches!(
            failure.kind(),
            crate::owned_upstream::quinn_client_root::QuinnClientRootOpenFailureKind::FlowRegistryCapacity {
                max: 4
            }
        ));
    let TargetAddr::DomainPort { host, port } = failure.into_target() else {
        panic!("registry rejection changed target address family")
    };
    assert_eq!(port, 443);
    assert_eq!(host.as_ptr(), host_ptr);
    assert_eq!(host.capacity(), host_capacity);

    let mut root_join = Box::pin(root.join());
    let mut reset_count = 0usize;
    while reset_count < flows.len() {
        let inbound = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::select! {
                biased;
                inbound = owner.recv_classified(STANDBY_CONTROL_V1) => inbound,
                outcome = &mut root_join => {
                    panic!("registry root joined before every RESET: {outcome:?}")
                }
            }
        })
        .await
        .expect("registry RESET drain is bounded")
        .expect("registry peer stays live through RESET")
        .expect("registry shutdown emits RESET");
        let BoundInbound::Session(received) = take_actor_inbound(inbound) else {
            panic!("registry RESET was misclassified as leg control")
        };
        assert!(matches!(
            received.into_frame_for_test().record(),
            Record::Reset {
                reason: crate::resumable::ResetReason::LocalAbandon,
                ..
            }
        ));
        reset_count += 1;
    }
    let outcome = tokio::time::timeout(TEST_TIMEOUT, root_join)
        .await
        .expect("four-slot registry shutdown joins within the local bound");
    let QuinnClientRootOutcome::Joined(joined) = outcome else {
        panic!("registry rejection contaminated clean shutdown: {outcome:?}")
    };
    assert!(joined.is_zero());
    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("registry peer join is bounded")
        .expect("registry peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, flows, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("registry listener cleanup is bounded")
        .expect("registry root releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_root_join_waiter_can_retry_the_same_sealed_structural_join() {
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("join-retry fixture transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before join-waiter cancellation");

    let mut cancelled_join = Box::pin(root.join());
    tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::select! {
            biased;
            outcome = &mut cancelled_join => {
                panic!("first join waiter bypassed physical cleanup: {outcome:?}")
            }
            () = physical_cleanup.wait_until_blocked() => {}
        }
    })
    .await
    .expect("the first waiter reaches the deterministic cleanup await");
    drop(cancelled_join);

    let mut retried_join = Box::pin(root.join());
    let retry_first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(retried_join.as_mut(), context))
    })
    .await;
    assert!(
        matches!(retry_first_poll, std::task::Poll::Pending),
        "cancelling observation cannot detach or duplicate the in-flight root join",
    );
    assert_eq!(handle.phase(), QuinnClientRootPhase::ShuttingDown);

    physical_cleanup.release();
    let outcome = tokio::time::timeout(TEST_TIMEOUT, retried_join)
        .await
        .expect("the retry observes the same sealed structural cleanup");
    let QuinnClientRootOutcome::Joined(joined) = outcome else {
        panic!("cancelled observation contaminated the retained join: {outcome:?}")
    };
    assert!(joined.is_zero());
    assert_eq!(handle.phase(), QuinnClientRootPhase::Joined);

    let repeated = root.join().await;
    assert!(matches!(repeated, QuinnClientRootOutcome::Joined(joined) if joined.is_zero()));
    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("join-retry peer cleanup is bounded")
        .expect("join-retry peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("join-retry listener cleanup is bounded")
        .expect("join-retry root releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_root_join_then_owner_drop_never_publishes_clean_joined() {
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("quiescent composition transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root publishes Ready before cancellation testing");

    let mut cancelled_join = Box::pin(root.join());
    tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::select! {
            biased;
            outcome = &mut cancelled_join => {
                panic!("root join bypassed the physical cleanup gate: {outcome:?}")
            }
            () = physical_cleanup.wait_until_blocked() => {}
        }
    })
    .await
    .expect("root cleanup reaches the deterministic physical barrier");
    drop(cancelled_join);
    drop(root);
    assert_eq!(
        handle.phase(),
        QuinnClientRootPhase::ShuttingDown,
        "drop starts janitor cleanup but cannot claim a joined witness early",
    );

    physical_cleanup.release();
    let terminal_phase = tokio::time::timeout(TEST_TIMEOUT, handle.wait_terminal_phase())
        .await
        .expect("janitor cleanup reaches one terminal lifecycle phase")
        .expect("root lifecycle source remains observable");
    assert_eq!(terminal_phase, QuinnClientRootPhase::FailedJoined);
    assert_ne!(handle.phase(), QuinnClientRootPhase::Joined);

    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("peer owner cleanup remains bounded")
        .expect("peer owner joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop(owner_bootstrap);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("cancelled-root listener cleanup is bounded")
        .expect("janitor releases the exact client actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn root_actor_fatal_is_sticky_but_still_returns_zero_structural_witness() {
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("quiescent composition transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root is running before the peer fatal");

    owner.request_cancel();
    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("peer fatal source closes within the local bound")
        .expect("peer actor joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);

    let outcome = tokio::time::timeout(TEST_TIMEOUT, root.join())
        .await
        .expect("fatal root still completes every structural join");
    match outcome {
        QuinnClientRootOutcome::FailedJoined {
            joined: Some(joined),
            first_failure,
        } => {
            assert!(joined.is_zero());
            assert!(
                first_failure.to_ascii_lowercase().contains("actor"),
                "the exact actor cause remains sticky: {first_failure}",
            );
        }
        other => panic!("fatal root lost its failed zero-witness outcome: {other:?}"),
    }
    assert_eq!(handle.phase(), QuinnClientRootPhase::FailedJoined);
    drop((root, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("fatal-root listener cleanup is bounded")
        .expect("failed root still releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn root_actor_fatal_with_live_flow_still_revokes_driver_and_tombstone_to_zero() {
    let (plan, mut listener, composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let mut root = QuinnClientRoot::start(plan, Box::new(composition))
        .expect("quiescent composition transfers into the sole root");
    let handle = root
        .wait_ready()
        .await
        .expect("root is running before live-flow fatal testing");
    let flow = handle
        .open_tcp(TargetAddr::IpPort(
            "127.0.0.1:8443".parse().expect("valid actor-fatal target"),
        ))
        .await
        .expect("root installs one flow before actor fatal");
    let inbound = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("pre-fatal OPEN reaches the peer")
        .expect("peer remains live for pre-fatal OPEN")
        .expect("peer receives pre-fatal OPEN");
    assert!(matches!(
        take_actor_inbound(inbound),
        BoundInbound::Session(_)
    ));

    owner.request_cancel();
    let owner_joined = tokio::time::timeout(TEST_TIMEOUT, owner.join())
        .await
        .expect("peer fatal source closes within the local bound")
        .expect("peer fatal actor joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);

    let outcome = tokio::time::timeout(TEST_TIMEOUT, root.join())
        .await
        .expect("failed transport still completes local semantic and structural cleanup");
    match outcome {
        QuinnClientRootOutcome::FailedJoined {
            joined: Some(joined),
            first_failure,
        } => {
            assert!(joined.is_zero(), "failed live-flow witness: {joined:?}");
            assert_eq!(
                joined.session_residual_ownership(),
                0,
                "transport-fatal fallback must revoke the flow and consume its terminal tombstone",
            );
            assert!(
                first_failure.to_ascii_lowercase().contains("actor")
                    || first_failure.to_ascii_lowercase().contains("transport"),
                "physical first failure remains sticky: {first_failure}",
            );
        }
        other => panic!("failed live-flow root lost its zero witness: {other:?}"),
    }
    assert_eq!(handle.phase(), QuinnClientRootPhase::FailedJoined);
    assert_eq!(
        flow.try_send_reset(crate::resumable::ResetReason::TargetFailure)
            .unwrap_err(),
        crate::owned_upstream::FlowPortError::Closed,
    );
    drop((root, flow, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("failed live-flow listener cleanup is bounded")
        .expect("failed live-flow root releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn root_rejects_preexisting_probe_authority_before_factory_mint() {
    let (plan, mut listener, mut composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let queued_probe = composition
        .begin_current_probe()
        .expect("the pre-root composition owns one queued probe authority");
    let failure = match QuinnClientRoot::start(plan, Box::new(composition)) {
        Ok(_) => panic!("a root without a same-domain clock cannot adopt a queued probe"),
        Err(failure) => failure,
    };
    assert!(matches!(
        failure.kind(),
        QuinnClientRootStartFailureKind::CompositionNotQuiescent,
    ));
    let (_plan, mut composition) = failure.into_parts();
    drop(queued_probe);

    let (client_joined, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), owner.join())
    })
    .await
    .expect("rejected root ownership cleanup remains bounded");
    let client_joined = client_joined.expect("recovered composition joins structurally");
    let owner_joined = owner_joined.expect("rejected root peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("rejected-root listener cleanup is bounded")
        .expect("pre-mint rejection returns every actor owner");
}

#[tokio::test(flavor = "current_thread")]
async fn root_rejects_preexisting_live_flow_before_factory_mint() {
    let (plan, mut listener, mut composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let effects = composition
        .open_local(TargetAddr::IpPort(
            "127.0.0.1:443".parse().expect("loopback pre-root target"),
        ))
        .expect("the composition still owns current policy")
        .expect("the reducer accepts one pre-root local flow");
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, SessionEffect::LocalFlowOpened { .. }))
    );
    assert_eq!(composition.snapshot().session.flow_count(), 1);

    let composition = Box::new(composition);
    let exact_composition = std::ptr::from_ref(composition.as_ref());
    let failure = match QuinnClientRoot::start(plan, composition) {
        Ok(_) => panic!("root adopted a live flow without its private timer/driver owners"),
        Err(failure) => failure,
    };
    assert!(matches!(
        failure.kind(),
        QuinnClientRootStartFailureKind::CompositionNotQuiescent,
    ));
    let (returned_plan, mut composition) = failure.into_parts();
    assert_eq!(returned_plan, plan);
    assert_eq!(std::ptr::from_ref(composition.as_ref()), exact_composition);
    assert_eq!(composition.snapshot().session.flow_count(), 1);
    drop(effects);

    let (client_outcome, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), owner.join())
    })
    .await
    .expect("rejected live-flow ownership cleanup remains bounded");
    let (client_joined, _semantic_error) = client_outcome.into_parts();
    let client_joined = client_joined.expect("returned composition joins structurally");
    let owner_joined = owner_joined.expect("rejected live-flow peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("live-flow rejection listener cleanup is bounded")
        .expect("pre-mint live-flow rejection releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn root_rejects_queued_current_fifo_before_factory_mint_and_returns_composition() {
    let (plan, mut listener, mut composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let before = composition.snapshot();
    let queued = Frame::try_new(
        before.session.generation(),
        Record::Ack {
            flow_id: crate::resumable::SessionFlowId::new(1).expect("nonzero pre-root ACK flow id"),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(0),
            final_accepted: false,
        },
    )
    .expect("valid pre-root current frame");
    composition
        .enqueue_current_frame(queued.clone())
        .expect("pristine reducer may still have one controller-owned current frame");
    assert_eq!(composition.snapshot(), before);

    let composition = Box::new(composition);
    let exact_composition = std::ptr::from_ref(composition.as_ref());
    let failure = match QuinnClientRoot::start(plan, composition) {
        Ok(_) => panic!("root adopted a controller-owned current FIFO before factory mint"),
        Err(failure) => failure,
    };
    assert!(matches!(
        failure.kind(),
        QuinnClientRootStartFailureKind::CompositionNotQuiescent,
    ));
    let (returned_plan, mut composition) = failure.into_parts();
    assert_eq!(returned_plan, plan);
    assert_eq!(std::ptr::from_ref(composition.as_ref()), exact_composition);
    assert_eq!(composition.snapshot(), before);

    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("returned composition retains its exact current FIFO"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("returned current FIFO reaches the peer within the local bound")
        .expect("returned current actor remains live")
        .expect("returned current FIFO retains the exact queued frame");
    let BoundInbound::Session(received) = take_actor_inbound(inbound) else {
        panic!("queued session frame was misclassified as leg control");
    };
    assert_eq!(received.into_frame_for_test(), queued);

    let mut root = QuinnClientRoot::start(returned_plan, composition)
        .expect("draining the returned FIFO still permits the sole factory mint");
    root.wait_ready()
        .await
        .expect("retried root publishes Ready from the exact returned composition");
    let (root_outcome, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join(), owner.join())
    })
    .await
    .expect("retried queued-root cleanup remains bounded");
    let QuinnClientRootOutcome::Joined(joined) = root_outcome else {
        panic!("pre-mint rejection consumed factory authority: {root_outcome:?}");
    };
    assert!(joined.is_zero());
    let owner_joined = owner_joined.expect("retried root peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((root, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("queued-root listener cleanup is bounded")
        .expect("queued-root rejection returns the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn root_rejects_submitted_current_fifo_until_ordered_delivery_retires() {
    let (plan, mut listener, mut composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let before = composition.snapshot();
    let generation = before.session.generation();
    let control = ordered_control_for_generation(generation, 700);
    let receipt = composition
        .enqueue_current_ordered_leg_control(control.clone(), STANDBY_CONTROL_V1)
        .expect("current controller FIFO admits one ordered pre-root control frame");
    assert_eq!(composition.snapshot(), before);
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("ordered pre-root frame reaches the actor transport"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    assert!(
        !receipt.was_delivered(),
        "the current-thread tracer has not yielded for peer DELIVERED publication",
    );

    let composition = Box::new(composition);
    let exact_composition = std::ptr::from_ref(composition.as_ref());
    let failure = match QuinnClientRoot::start(plan, composition) {
        Ok(_) => panic!("root adopted an ordered current delivery wait before factory mint"),
        Err(failure) => failure,
    };
    assert!(matches!(
        failure.kind(),
        QuinnClientRootStartFailureKind::CompositionNotQuiescent,
    ));
    let (returned_plan, mut composition) = failure.into_parts();
    assert_eq!(returned_plan, plan);
    assert_eq!(std::ptr::from_ref(composition.as_ref()), exact_composition);
    assert_eq!(composition.snapshot(), before);
    assert_eq!(
            composition
                .try_flush_current(SimTime::ZERO)
                .expect("returned composition retains the ordered wait"),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending,
        );

    let inbound = tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("submitted ordered frame reaches the peer within the local bound")
        .expect("ordered current actor remains live")
        .expect("peer receives the submitted ordered frame");
    let BoundInbound::LegControl(received) = take_actor_inbound(inbound) else {
        panic!("ordered control frame was misclassified as session data");
    };
    let (_seal, received) = received.into_parts();
    assert_eq!(received, control);
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !receipt.was_delivered() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("peer DELIVERED retires the exact submitted frame");
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("positive delivery retires the exact queue wait"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle,
    );

    let (client_joined, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), owner.join())
    })
    .await
    .expect("rejected ordered-root cleanup remains bounded");
    let client_joined = client_joined.expect("returned composition joins structurally");
    let owner_joined = owner_joined.expect("returned composition peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("ordered-root listener cleanup is bounded")
        .expect("ordered-root rejection returns the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn current_enqueue_shutdown_rejection_returns_exact_frame_backing() {
    let (_plan, mut listener, mut composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let secret = Bytes::from_static(b"shutdown-exact-frame-secret");
    let frame = Frame::try_new(
        composition.snapshot().session.generation(),
        Record::Data {
            flow_id: crate::resumable::SessionFlowId::new(501)
                .expect("nonzero shutdown ownership flow id"),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: secret,
        },
    )
    .expect("valid shutdown ownership frame");
    let payload_ptr = match frame.record() {
        Record::Data { payload, .. } => payload.as_ptr(),
        _ => unreachable!("constructed DATA frame"),
    };

    let current_join_block = composition.arm_current_join_block_for_test();
    let mut shutdown = Box::pin(composition.join_current_for_shutdown());
    tokio::select! {
        biased;
        () = current_join_block.wait_until_blocked() => {}
        result = &mut shutdown => panic!("shutdown bypassed current join gate: {result:?}"),
    }
    drop(shutdown);

    let failure = composition
        .enqueue_current_frame(frame)
        .expect_err("shutting-down composition must reject current enqueue");
    assert!(matches!(
            failure.kind(),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentEnqueueErrorKind::SessionShuttingDown
        ));
    assert!(!format!("{failure:?}").contains("shutdown-exact-frame-secret"));
    assert!(!failure.to_string().contains("shutdown-exact-frame-secret"));
    let returned = failure.into_frame();
    assert!(matches!(
        returned.record(),
        Record::Data { payload, .. }
            if payload.as_ptr() == payload_ptr
                && payload.as_ref() == b"shutdown-exact-frame-secret"
    ));

    current_join_block.release();
    let (client_joined, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), owner.join())
    })
    .await
    .expect("shutdown ownership cleanup remains bounded");
    let client_joined = client_joined.expect("client actor joins structurally");
    let owner_joined = owner_joined.expect("owner actor joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("shutdown ownership listener cleanup is bounded")
        .expect("shutdown ownership tracer releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn current_enqueue_actor_failure_returns_exact_frame_backing() {
    let (_plan, mut listener, mut composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let generation = composition.snapshot().session.generation();
    owner
        .enqueue_ordered_leg_control(
            ordered_control_for_generation(generation, 801),
            STANDBY_CONTROL_V1,
        )
        .expect("peer queues one deterministic actor-failure record");
    assert_eq!(
        owner
            .try_flush(SimTime::ZERO)
            .expect("actor-failure record flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        while composition
            .current_actor_snapshot()
            .queued_inbound_messages()
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor-failure record reaches the current actor");
    assert!(matches!(
        composition.force_current_wrong_route_after_dequeue_for_test(),
        QuinnCurrentInboundDrive::ActorFailed(QuinnLegActorError::RootFailed(_))
    ));

    let frame = Frame::try_new(
        generation,
        Record::Data {
            flow_id: crate::resumable::SessionFlowId::new(502)
                .expect("nonzero actor-failure ownership flow id"),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"actor-failure-exact-frame-secret"),
        },
    )
    .expect("valid actor-failure ownership frame");
    let payload_ptr = match frame.record() {
        Record::Data { payload, .. } => payload.as_ptr(),
        _ => unreachable!("constructed DATA frame"),
    };
    let failure = composition
        .enqueue_current_frame(frame)
        .expect_err("failed actor must reject current enqueue");
    assert!(matches!(
            failure.kind(),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentEnqueueErrorKind::ActorTransportUnavailable(
                crate::owned_upstream::quinn_leg_composition::QuinnCurrentActorEnqueueUnavailableKind::FirstFailure
            )
        ));
    assert!(!format!("{failure:?}").contains("actor-failure-exact-frame-secret"));
    assert!(
        !failure
            .to_string()
            .contains("actor-failure-exact-frame-secret")
    );
    let returned = failure.into_frame();
    assert!(matches!(
        returned.record(),
        Record::Data { payload, .. }
            if payload.as_ptr() == payload_ptr
                && payload.as_ref() == b"actor-failure-exact-frame-secret"
    ));

    let (outcome, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), owner.join())
    })
    .await
    .expect("actor-failure ownership cleanup remains bounded");
    let (client_joined, first_error) = outcome.into_parts();
    let client_joined = client_joined.expect("failed actor retains its structural witness");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert!(first_error.is_some());
    let owner_joined = owner_joined.expect("actor-failure peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("actor-failure ownership listener cleanup is bounded")
        .expect("actor-failure ownership tracer releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn current_enqueue_capacity_returns_exact_frame_for_one_retry() {
    let (_plan, mut listener, mut composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let generation = composition.snapshot().session.generation();
    for index in 0..4_u64 {
        composition
            .enqueue_current_frame(
                Frame::try_new(
                    generation,
                    Record::Ack {
                        flow_id: crate::resumable::SessionFlowId::new(600 + index)
                            .expect("nonzero capacity filler flow id"),
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(0),
                        final_accepted: false,
                    },
                )
                .expect("valid capacity filler frame"),
            )
            .expect("bounded current FIFO admits each capacity filler");
    }

    let frame = Frame::try_new(
        generation,
        Record::Data {
            flow_id: crate::resumable::SessionFlowId::new(700)
                .expect("nonzero capacity ownership flow id"),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"capacity-exact-frame-secret"),
        },
    )
    .expect("valid capacity ownership frame");
    let payload_ptr = match frame.record() {
        Record::Data { payload, .. } => payload.as_ptr(),
        _ => unreachable!("constructed DATA frame"),
    };
    let failure = composition
        .enqueue_current_frame(frame)
        .expect_err("full current FIFO must return bounded pressure");
    assert!(matches!(
        failure.kind(),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentEnqueueErrorKind::Queue(kind)
            if matches!(
                kind.as_ref(),
                crate::owned_upstream::leg_io::LegOutboundQueueErrorKind::CapacityExceeded {
                    queued_frames: 4,
                    ..
                }
            )
    ));
    assert!(!format!("{failure:?}").contains("capacity-exact-frame-secret"));
    assert!(!failure.to_string().contains("capacity-exact-frame-secret"));
    let returned = failure.into_frame();
    assert!(matches!(
        returned.record(),
        Record::Data { payload, .. }
            if payload.as_ptr() == payload_ptr
                && payload.as_ref() == b"capacity-exact-frame-secret"
    ));

    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("one filler releases bounded queue pressure"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    drop(
        tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("first filler receive remains bounded")
            .expect("capacity peer remains live")
            .expect("first filler reaches the capacity peer"),
    );
    composition
        .enqueue_current_frame(returned)
        .expect("the exact returned frame retries after pressure releases");

    for _ in 0..4 {
        assert_eq!(
            composition
                .try_flush_current(SimTime::ZERO)
                .expect("retried FIFO drain remains live"),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
        );
        drop(
            tokio::time::timeout(TEST_TIMEOUT, owner.recv_classified(STANDBY_CONTROL_V1))
                .await
                .expect("retried FIFO peer receive remains bounded")
                .expect("retried FIFO peer remains live")
                .expect("retried FIFO emits every accepted frame"),
        );
    }
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("drained current FIFO remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle,
    );

    let (client_joined, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), owner.join())
    })
    .await
    .expect("capacity ownership cleanup remains bounded");
    let client_joined = client_joined.expect("capacity client joins structurally");
    let owner_joined = owner_joined.expect("capacity owner joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("capacity ownership listener cleanup is bounded")
        .expect("capacity ownership tracer releases the exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn join_all_rejects_controller_current_fifo_residual_beside_zero_actor_witness() {
    let (_plan, mut listener, mut composition, mut owner, owner_bootstrap) =
        direct_production_root_fixture().await;
    let before = composition.snapshot();
    let queued = Frame::try_new(
        before.session.generation(),
        Record::Ack {
            flow_id: crate::resumable::SessionFlowId::new(2)
                .expect("nonzero shutdown-residual ACK flow id"),
            direction: Direction::ClientToTarget,
            next_accepted: ByteOffset::new(0),
            final_accepted: false,
        },
    )
    .expect("valid future-root residual frame");
    composition
        .enqueue_current_frame(queued)
        .expect("future root may admit one frame before shutdown");
    assert_eq!(composition.snapshot(), before);

    let (outcome, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_all_for_shutdown(), owner.join())
    })
    .await
    .expect("residual shutdown still completes structural joins");
    let (current, first_error) = outcome.into_parts();
    let current = current.expect("actor structural zero witness is preserved");
    assert_eq!(current.owned_messages(), 0);
    assert_eq!(current.owned_bytes(), 0);
    let Some(QuinnCurrentJoinError::CurrentControllerResidual {
        active: true,
        queued_frames: 1,
        owned_bytes,
        ordered_delivery_wait: false,
    }) = first_error
    else {
        panic!("controller FIFO residual did not retain its exact shutdown witness");
    };
    assert!(owned_bytes > 0);
    let owner_joined = owner_joined.expect("residual shutdown peer joins structurally");
    assert_eq!(owner_joined.owned_messages(), 0);
    assert_eq!(owner_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("residual-root listener cleanup is bounded")
        .expect("residual-root cleanup releases the exact actor slot");
}

async fn connect_real_actor_pair(
    listener: &mut QuinnLegActorListener,
    leg: LegId,
    limits: QuinnLegActorLimits,
    peer_policy: AttachPeerPolicy,
) -> (StartedQuinnLegActor, StartedQuinnLegActor) {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid loopback actor configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, leg, limits, peer_policy),
            listener.accept(leg, limits, peer_policy),
        )
    })
    .await
    .expect("real actor-pair bootstrap is bounded");
    (
        client.expect("publish real client actor"),
        server.expect("publish real server actor"),
    )
}

async fn connect_real_actor_pair_with_clocks(
    listener: &mut QuinnLegActorListener,
    leg: LegId,
    limits: QuinnLegActorLimits,
    peer_policy: AttachPeerPolicy,
    client_clock: SessionMonotonicClock,
    server_clock: SessionMonotonicClock,
) -> (StartedQuinnLegActor, StartedQuinnLegActor) {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid loopback actor configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect_with_clock(
                client_config,
                leg,
                limits,
                peer_policy,
                client_clock,
            ),
            listener.accept_with_clock(leg, limits, peer_policy, server_clock),
        )
    })
    .await
    .expect("same-session actor-pair bootstrap is bounded");
    (
        client.expect("publish same-session client actor"),
        server.expect("publish same-session server actor"),
    )
}

async fn direct_production_root_fixture() -> (
    QuinnClientRootPlan,
    QuinnLegActorListener,
    QuinnClientSessionComposition,
    StartedQuinnLegActor,
    OwnerSessionBootstrap,
) {
    let plan = production_client_root_plan();
    direct_production_root_fixture_with_plan(plan).await
}

async fn direct_production_root_fixture_with_plan(
    plan: QuinnClientRootPlan,
) -> (
    QuinnClientRootPlan,
    QuinnLegActorListener,
    QuinnClientSessionComposition,
    StartedQuinnLegActor,
    OwnerSessionBootstrap,
) {
    direct_production_root_fixture_with_plan_and_limits(plan, composition_actor_limits()).await
}

async fn direct_production_root_fixture_with_plan_and_limits(
    plan: QuinnClientRootPlan,
    limits: QuinnLegActorLimits,
) -> (
    QuinnClientRootPlan,
    QuinnLegActorListener,
    QuinnClientSessionComposition,
    StartedQuinnLegActor,
    OwnerSessionBootstrap,
) {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot production-root listener configuration"),
    )
    .await
    .expect("bind production-root listener");
    let (mut client, mut owner) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (accepted, owner_bootstrap) =
        complete_initial_actor_attach_with_config(&mut client, &mut owner, plan.session_config())
            .await;
    wait_actor_ordered_delivery(&mut owner).await;
    let composition =
        QuinnClientSessionComposition::bootstrap_initial(plan.session_config(), accepted, client)
            .expect("direct fixture installs the same exact initial composition");
    (plan, listener, composition, owner, owner_bootstrap)
}

fn drain_owner_target_outputs(
    executor: &mut OwnerTargetExecutor<MemoryTarget>,
    mut outputs: Vec<OwnerTargetOutput>,
) -> Vec<Frame> {
    let mut frames = Vec::new();
    let mut turns = 0usize;
    loop {
        let mut resume = None;
        for output in outputs {
            match output {
                OwnerTargetOutput::Transmit(frame) => frames.push(frame),
                OwnerTargetOutput::NeedsResume { resume: next } => {
                    assert!(
                        resume.replace(next).is_none(),
                        "one owner turn mints one resume"
                    );
                }
                OwnerTargetOutput::LegActivated { .. }
                | OwnerTargetOutput::ResumeGraceStarted { .. }
                | OwnerTargetOutput::TerminalGraceStarted { .. }
                | OwnerTargetOutput::SessionExpired => {}
            }
        }
        let Some(next) = resume else {
            break;
        };
        turns += 1;
        assert!(turns <= 64, "owner Target continuation must make progress");
        outputs = executor
            .resume_pending_effects(next)
            .expect("owner Target continuation remains live");
    }
    frames
}

async fn open_real_target_flows_on_a(
    composition: &mut QuinnClientSessionComposition,
    server_a: &mut StartedQuinnLegActor,
    owner_executor: &mut OwnerTargetExecutor<MemoryTarget>,
    owner_active_a: &AttachedLeg,
    local_flow_count: usize,
) -> Vec<LocalFlow> {
    let mut local_flows = Vec::with_capacity(local_flow_count);
    for index in 0..local_flow_count {
        let port = 443_u16
            .checked_add(u16::try_from(index).expect("bounded flow index"))
            .expect("bounded target port");
        let effects = composition
            .open_local(TargetAddr::IpPort(
                format!("127.0.0.1:{port}")
                    .parse()
                    .expect("loopback target"),
            ))
            .expect("composition retains current actor A")
            .expect("local open is accepted");
        let mut local_flow = None;
        let mut outbound = Vec::new();
        for effect in effects {
            match effect {
                SessionEffect::LocalFlowOpened { flow } => {
                    assert!(local_flow.replace(flow).is_none());
                }
                SessionEffect::Transmit(frame) => outbound.push(frame),
                other => panic!("unexpected local-open effect: {other:?}"),
            }
        }
        let local_flow = local_flow.expect("local open mints one exact flow authority");
        assert_eq!(outbound.len(), 1);
        assert!(matches!(
            outbound[0].record(),
            Record::Open { flow_id, .. } if *flow_id == local_flow.flow_id()
        ));
        composition
            .enqueue_current_frame(outbound.pop().expect("one OPEN frame"))
            .expect("current A FIFO admits the exact OPEN");
        assert_eq!(
            composition
                .try_flush_current(SimTime::ZERO)
                .expect("client A OPEN flush remains live"),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
        );
        let inbound =
            tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
                .await
                .expect("server A OPEN receive is bounded")
                .expect("server A remains live")
                .expect("server A receives the exact OPEN");
        let BoundInbound::Session(received) = take_actor_inbound(inbound) else {
            panic!("client A OPEN was misclassified as leg control");
        };
        let event = owner_active_a
            .accept_frame(received)
            .expect("exact owner A provenance accepts OPEN");
        let outputs = owner_executor
            .apply_event(event)
            .expect("owner reducer admits OPEN and drives Target");
        let mut responses = drain_owner_target_outputs(owner_executor, outputs);
        assert_eq!(responses.len(), 1);
        assert!(matches!(
            responses[0].record(),
            Record::OpenResult { flow_id, result }
                if *flow_id == local_flow.flow_id() && *result == OpenResultCode::Opened
        ));
        server_a
            .queue
            .as_mut()
            .expect("server A retains its sole FIFO")
            .push(responses.pop().expect("one OPEN result"))
            .expect("server A FIFO admits reducer-minted OPEN result");
        assert_eq!(
            server_a
                .try_flush(SimTime::ZERO)
                .expect("server A OPEN-result flush remains live"),
            QuinnLegActorFlushDisposition::Submitted,
        );
        let applied = tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition.try_receive_current() {
                    QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                    QuinnCurrentInboundDrive::Applied {
                        effects,
                        observation,
                    } => break (effects, observation),
                    QuinnCurrentInboundDrive::PolicyBlocked
                    | QuinnCurrentInboundDrive::ProbeAcked { .. }
                    | QuinnCurrentInboundDrive::Rejected
                    | QuinnCurrentInboundDrive::ReducerRejected(_)
                    | QuinnCurrentInboundDrive::SessionShuttingDown
                    | QuinnCurrentInboundDrive::MissingCurrent
                    | QuinnCurrentInboundDrive::CompositionInvariant
                    | QuinnCurrentInboundDrive::ActorFailed(_) => {
                        panic!("client A failed to apply exact OPEN result")
                    }
                }
            }
        })
        .await
        .expect("client A OPEN result is applied within the local bound");
        assert_eq!(applied.1, None);
        assert!(matches!(
            applied.0.as_slice(),
            [SessionEffect::LocalOpenResolved { flow_id, result }]
                if *flow_id == local_flow.flow_id() && *result == OpenResultCode::Opened
        ));
        wait_actor_ordered_delivery(server_a).await;
        tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    match composition
                        .try_flush_current(SimTime::ZERO)
                        .expect("client A OPEN delivery tail remains live")
                    {
                        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle => break,
                        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted
                        | crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            })
            .await
            .expect("client A OPEN delivery completes within the local bound");
        local_flows.push(local_flow);
    }
    let target = owner_executor.target().snapshot();
    assert_eq!(target.open_attempts, local_flow_count as u64);
    assert_eq!(target.opened_flows, local_flow_count as u64);
    assert_eq!(target.duplicate_opens, 0);
    assert_eq!(target.live_flows, local_flow_count);
    local_flows
}

struct RealAcceptedReplacementFixture {
    listener: QuinnLegActorListener,
    composition: QuinnClientSessionComposition,
    server_a: StartedQuinnLegActor,
    client_b: StartedQuinnLegActor,
    server_b: StartedQuinnLegActor,
    accepted_b: AcceptedClientRegisteredAttach,
    tcp_factory: ResumableTcpPortFactory,
    local_flows: Vec<LocalFlow>,
    peer_policy: AttachPeerPolicy,
    owner_executor: OwnerTargetExecutor<MemoryTarget>,
    owner_active_a: AttachedLeg,
    owner_active_b: AttachedLeg,
    standby_slot: OwnerStandbySlot,
}

struct RealRegisteredStandbyFixture {
    listener: QuinnLegActorListener,
    composition: QuinnClientSessionComposition,
    server_a: StartedQuinnLegActor,
    client_b: StartedQuinnLegActor,
    server_b: StartedQuinnLegActor,
    registered_b: ClientRegisteredStandby,
    tcp_factory: ResumableTcpPortFactory,
    local_flows: Vec<LocalFlow>,
    peer_policy: AttachPeerPolicy,
    owner_executor: OwnerTargetExecutor<MemoryTarget>,
    owner_active_a: AttachedLeg,
    standby_slot: OwnerStandbySlot,
}

/// Shared real-loopback A/Target floor with a physically established but
/// not-yet-registered B. R6a keeps using `RealRegisteredStandbyFixture`,
/// while R6b can transfer this whole B actor into the composition before
/// any registration capability exists outside that owner.
struct RealUnregisteredStandbyFixture {
    listener: QuinnLegActorListener,
    composition: QuinnClientSessionComposition,
    server_a: StartedQuinnLegActor,
    client_b: StartedQuinnLegActor,
    server_b: StartedQuinnLegActor,
    tcp_factory: ResumableTcpPortFactory,
    local_flows: Vec<LocalFlow>,
    peer_policy: AttachPeerPolicy,
    owner_executor: OwnerTargetExecutor<MemoryTarget>,
    owner_active_a: AttachedLeg,
}

struct RealRegisteredReplacementFixture {
    listener: QuinnLegActorListener,
    composition: QuinnClientSessionComposition,
    server_a: StartedQuinnLegActor,
    server_b: StartedQuinnLegActor,
    started_b: ClientRecoveryStarted,
    _tcp_factory: ResumableTcpPortFactory,
    local_flows: Vec<LocalFlow>,
    peer_policy: AttachPeerPolicy,
    _owner_executor: OwnerTargetExecutor<MemoryTarget>,
    _owner_active_a: AttachedLeg,
    _standby_slot: OwnerStandbySlot,
}

/// Builds the shared real-loopback A/Target floor and a physically
/// established, unregistered B. No B registration owner is minted here.
async fn real_unregistered_standby_fixture_with_target_state(
    max_live_actors: usize,
    local_flow_count: usize,
    establish_targets: bool,
) -> RealUnregisteredStandbyFixture {
    real_unregistered_standby_fixture_with_target_state_and_plan(
        max_live_actors,
        local_flow_count,
        establish_targets,
        ActorSessionBootstrapPlan::LegacyUnproven(actor_session_config()),
    )
    .await
}

async fn real_unregistered_standby_fixture_with_target_state_checked(
    max_live_actors: usize,
    local_flow_count: usize,
    establish_targets: bool,
) -> (RealUnregisteredStandbyFixture, OwnerSessionCapacityPlan) {
    let plans = PhysicalActorSessionPlans::derive();
    let fixture = real_unregistered_standby_fixture_with_target_state_and_plan(
        max_live_actors,
        local_flow_count,
        establish_targets,
        ActorSessionBootstrapPlan::PhysicalProven(plans),
    )
    .await;
    (fixture, plans.owner)
}

async fn real_unregistered_standby_fixture_with_target_state_and_plan(
    max_live_actors: usize,
    local_flow_count: usize,
    establish_targets: bool,
    session_plan: ActorSessionBootstrapPlan,
) -> RealUnregisteredStandbyFixture {
    assert!(local_flow_count <= 4);
    if establish_targets {
        assert!(local_flow_count > 0);
    }
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            max_live_actors,
        )
        .expect("bounded shared-listener configuration"),
    )
    .await
    .expect("bind shared actor listener");
    let (mut client_a, mut server_a) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (accepted_a, owner_bootstrap_a) =
        complete_initial_actor_attach_with_plan(&mut client_a, &mut server_a, session_plan).await;
    let mut composition = QuinnClientSessionComposition::bootstrap_initial(
        session_plan.client_session_config(),
        accepted_a,
        client_a,
    )
    .expect("composition consumes the whole exact actor A");
    let tcp_factory = composition
        .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).expect("valid TCP source"))
        .expect("running A admits the session's sole TCP source factory")
        .expect("TCP source capacity derives from the pristine session");
    let (owner_supervisor, owner_active_a) = owner_bootstrap_a.into_parts();
    let mut owner_executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(64, 1_024, 64).expect("valid owner target limits"),
        owner_supervisor,
        MemoryTarget::new(
            MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).expect("valid memory target config"),
        ),
        FlowPortConfig::new(64, 8, 8, 8).expect("valid owner flow-port config"),
    )
    .expect("construct owner target executor while its session is pristine");
    let local_flows = if establish_targets {
        open_real_target_flows_on_a(
            &mut composition,
            &mut server_a,
            &mut owner_executor,
            &owner_active_a,
            local_flow_count,
        )
        .await
    } else {
        let mut local_flows = Vec::with_capacity(local_flow_count);
        for index in 0..local_flow_count {
            let port = 443_u16
                .checked_add(u16::try_from(index).expect("bounded flow index"))
                .expect("bounded target port");
            let opened = composition
                .open_local(TargetAddr::IpPort(
                    format!("127.0.0.1:{port}")
                        .parse()
                        .expect("loopback target"),
                ))
                .expect("composition retains current actor A")
                .expect("local open is accepted");
            local_flows.push(
                opened
                    .into_iter()
                    .find_map(|effect| match effect {
                        SessionEffect::LocalFlowOpened { flow } => Some(flow),
                        _ => None,
                    })
                    .expect("unresolved local open mints one exact flow authority"),
            );
        }
        local_flows
    };

    let client_clock = composition.session_clock();
    let server_clock = server_a.session_clock_for_test();
    let (client_b, server_b) = connect_real_actor_pair_with_clocks(
        &mut listener,
        LegId::B,
        limits,
        peer_policy,
        client_clock,
        server_clock,
    )
    .await;

    RealUnregisteredStandbyFixture {
        listener,
        composition,
        server_a,
        client_b,
        server_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
    }
}

/// Builds an exact registered B while A and every existing Target flow
/// remain current. This preserves the compatibility fixture used by R6a;
/// its caller chooses when the ordinary exact-next attach crosses the
/// wire.
async fn real_registered_standby_fixture_with_target_state(
    max_live_actors: usize,
    local_flow_count: usize,
    establish_targets: bool,
) -> RealRegisteredStandbyFixture {
    real_registered_standby_fixture_with_target_state_and_plan(
        max_live_actors,
        local_flow_count,
        establish_targets,
        ActorSessionBootstrapPlan::LegacyUnproven(actor_session_config()),
    )
    .await
}

async fn real_registered_standby_fixture_with_target_state_checked(
    max_live_actors: usize,
    local_flow_count: usize,
    establish_targets: bool,
) -> (RealRegisteredStandbyFixture, OwnerSessionCapacityPlan) {
    let plans = PhysicalActorSessionPlans::derive();
    let fixture = real_registered_standby_fixture_with_target_state_and_plan(
        max_live_actors,
        local_flow_count,
        establish_targets,
        ActorSessionBootstrapPlan::PhysicalProven(plans),
    )
    .await;
    (fixture, plans.owner)
}

async fn real_registered_standby_fixture_with_target_state_and_plan(
    max_live_actors: usize,
    local_flow_count: usize,
    establish_targets: bool,
    session_plan: ActorSessionBootstrapPlan,
) -> RealRegisteredStandbyFixture {
    let RealUnregisteredStandbyFixture {
        listener,
        mut composition,
        server_a,
        mut client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state_and_plan(
        max_live_actors,
        local_flow_count,
        establish_targets,
        session_plan,
    )
    .await;
    let awaiting_registration = composition
        .begin_standby_registration(
            &mut client_b,
            crate::resumable::StandbyNonce::new([0x97; 16]).expect("nonzero standby nonce"),
            &actor_attach_credentials(),
        )
        .expect("current A authorizes registration of exact actor B");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("B registration flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let registration_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B registration receive is bounded")
            .expect("server B remains live")
            .expect("server B receives registration");
    let registration_inbound = take_actor_inbound(registration_inbound);
    wait_actor_ordered_delivery(&mut client_b).await;

    let candidate = owner_executor
        .authenticate_owner_standby(
            &owner_active_a,
            server_b
                .io
                .as_ref()
                .expect("server B owns its LegIo")
                .established_leg(),
            registration_inbound,
        )
        .expect("owner authenticates exact actor B registration");
    let mut standby_slot = OwnerStandbySlot::new();
    assert_eq!(
        standby_slot
            .admit(
                candidate,
                server_b.queue.as_mut().expect("server B owns its FIFO"),
            )
            .expect("owner B standby slot admits candidate"),
        OwnerStandbyAdmission::Installed,
    );
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("B registration acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let registration_acceptance =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B registration acceptance receive is bounded")
            .expect("client B remains live")
            .expect("client B receives registration acceptance");
    let registered_b = awaiting_registration
        .validate(take_actor_inbound(registration_acceptance))
        .expect("client validates exact actor B registration acceptance");

    RealRegisteredStandbyFixture {
        listener,
        composition,
        server_a,
        client_b,
        server_b,
        registered_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
        standby_slot,
    }
}

async fn real_registered_standby_fixture(
    max_live_actors: usize,
    local_flow_count: usize,
) -> RealRegisteredStandbyFixture {
    real_registered_standby_fixture_with_target_state(max_live_actors, local_flow_count, true).await
}

/// Transfers the whole unregistered B actor into the composition before
/// minting any registration owner and completes the real owner-side
/// acceptance without probing it.
async fn register_owned_real_b(
    composition: &mut QuinnClientSessionComposition,
    client_b: StartedQuinnLegActor,
    server_b: &mut StartedQuinnLegActor,
    owner_executor: &mut OwnerTargetExecutor<MemoryTarget>,
    owner_active_a: &AttachedLeg,
    standby_slot: &mut OwnerStandbySlot,
    observed_at: SimTime,
) {
    assert_eq!(
        composition
            .begin_owned_standby_registration(
                client_b,
                crate::resumable::StandbyNonce::new([0x97; 16]).expect("nonzero standby nonce"),
                &actor_attach_credentials(),
                QuinnStandbyLivenessConfig::new(
                    Duration::from_millis(30),
                    Duration::from_millis(200),
                )
                .expect("bounded B liveness policy"),
            )
            .expect("composition consumes the whole unregistered B actor"),
        QuinnOwnedStandbyRegistrationDrive::Queued,
    );
    assert_eq!(
        composition
            .try_flush_registered_standby(observed_at)
            .expect("composition-owned B registration flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let registration_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B registration receive is bounded")
            .expect("server B remains live")
            .expect("server B receives the exact registration");
    let candidate = owner_executor
        .authenticate_owner_standby(
            owner_active_a,
            server_b
                .io
                .as_ref()
                .expect("server B owns its LegIo")
                .established_leg(),
            take_actor_inbound(registration_inbound),
        )
        .expect("owner authenticates the composition-owned actor B registration");
    assert_eq!(
        standby_slot
            .admit(
                candidate,
                server_b.queue.as_mut().expect("server B owns its FIFO"),
            )
            .expect("owner B standby slot admits the candidate"),
        OwnerStandbyAdmission::Installed,
    );
    assert_eq!(
        server_b
            .try_flush(observed_at)
            .expect("B registration acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::RegistrationAccepted => break,
                outcome => {
                    panic!("composition-owned B registration was not accepted: {outcome:?}")
                }
            }
        }
    })
    .await
    .expect("composition validates the real B acceptance within the local bound");
    wait_actor_ordered_delivery(server_b).await;
}

/// Completes composition-owned registration and proves that an exact peer
/// ACK can mint freshness while B's redundant local DELIVERED publication
/// is still physically blocked.
async fn register_owned_and_make_real_b_fresh(
    composition: &mut QuinnClientSessionComposition,
    client_b: StartedQuinnLegActor,
    server_b: &mut StartedQuinnLegActor,
    owner_executor: &mut OwnerTargetExecutor<MemoryTarget>,
    owner_active_a: &AttachedLeg,
    standby_slot: &mut OwnerStandbySlot,
    observed_at: SimTime,
) -> SimTime {
    register_owned_real_b(
        composition,
        client_b,
        server_b,
        owner_executor,
        owner_active_a,
        standby_slot,
        observed_at,
    )
    .await;

    let delayed_delivered = Arc::clone(&server_b.delivery_writer_gate);
    delayed_delivered.arm();
    assert_eq!(
        composition
            .drive_registered_standby_probe(observed_at)
            .expect("registered B builds one exact probe"),
        QuinnStandbyProbeDrive::Queued,
    );
    assert_eq!(
        composition
            .try_flush_registered_standby(observed_at)
            .expect("composition-owned B probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("registered B probe receive is bounded")
        .expect("server B remains live")
        .expect("server B receives the exact probe");
    let BoundInbound::LegControl(bound) = take_actor_inbound(inbound) else {
        panic!("registered B probe was misclassified as session data");
    };
    let (_seal, probe) = bound.into_parts();
    let LegControlRecord::LegProbe {
        session_id,
        leg_epoch_nonce,
        probe_sequence,
    } = probe.record().clone()
    else {
        panic!("registered B liveness emitted a non-probe record");
    };
    let exact_ack = LegControlFrame::try_new(
        probe.leg_generation(),
        LegControlRecord::LegProbeAck {
            session_id,
            leg_epoch_nonce,
            probe_sequence,
        },
        STANDBY_CONTROL_V1,
    )
    .expect("exact registered B probe ACK is valid");
    tokio::time::timeout(TEST_TIMEOUT, delayed_delivered.wait_until_blocked())
        .await
        .expect("B probe DELIVERED writer blocks before publication");
    server_b
        .enqueue_ordered_leg_control(exact_ack, STANDBY_CONTROL_V1)
        .expect("server B queues the exact probe ACK");
    let arrival_lower_bound = composition
        .session_clock()
        .now()
        .expect("shared B clock remains representable before ACK flush");
    let progress_cursor = composition.progress_cursor();
    assert_eq!(
        server_b
            .try_flush(observed_at)
            .expect("server B probe ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let progress = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_actor_progress(progress_cursor),
    )
    .await
    .expect("standby ACK wakes the composition without a polling loop")
    .expect("standby actor progress remains observable");
    assert!(matches!(progress, QuinnCompositionProgress::Standby { .. }));
    let arrival_upper_bound = composition
        .session_clock()
        .now()
        .expect("shared B clock remains representable after ACK publication");
    let QuinnStandbyInboundDrive::ProbeFresh { fresh_until, .. } =
        composition.try_receive_registered_standby()
    else {
        panic!("exact B probe ACK did not mint freshness after its wake");
    };
    assert!(
        fresh_until
            >= arrival_lower_bound
                .checked_add(Duration::from_millis(200))
                .expect("bounded lower B freshness deadline"),
    );
    assert!(
        fresh_until
            <= arrival_upper_bound
                .checked_add(Duration::from_millis(200))
                .expect("bounded upper B freshness deadline"),
        "B freshness must derive from actor arrival, not root scheduling time",
    );
    assert!(
        server_b.actor_snapshot().reserved_delivery_acks() > 0,
        "freshness must commit before the redundant local DELIVERED write",
    );
    delayed_delivered.release();
    wait_actor_ordered_delivery(server_b).await;
    fresh_until
}

#[tokio::test(flavor = "current_thread")]
async fn composition_rejects_registered_b_ack_after_deadline_before_timer_drive() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let session_clock = composition.session_clock();
    let observed_at = session_clock
        .now()
        .expect("the registered-B session clock remains representable");
    register_owned_real_b(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        observed_at,
    )
    .await;

    assert_eq!(
        composition
            .drive_registered_standby_probe(observed_at)
            .expect("registered B queues one exact probe"),
        QuinnStandbyProbeDrive::Queued,
    );
    assert_eq!(
        composition
            .try_flush_registered_standby(observed_at)
            .expect("registered B probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("registered B probe receive is bounded")
        .expect("server B remains live")
        .expect("server B receives the exact armed probe");
    let BoundInbound::LegControl(bound) = take_actor_inbound(inbound) else {
        panic!("registered B probe was misclassified as session data");
    };
    let (_seal, probe) = bound.into_parts();
    let LegControlRecord::LegProbe {
        session_id,
        leg_epoch_nonce,
        probe_sequence,
    } = probe.record().clone()
    else {
        panic!("registered B liveness emitted a non-probe record");
    };
    let (deadline, recovery_key) = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.arm_registered_standby_probe(observed_at) {
                Ok(QuinnStandbyProbeArmDrive::NotDelivered) => {
                    tokio::task::yield_now().await;
                }
                Ok(QuinnStandbyProbeArmDrive::Armed {
                    deadline,
                    recovery_key,
                }) => break (deadline, recovery_key),
                outcome => panic!("registered B probe did not arm exactly once: {outcome:?}"),
            }
        }
    })
    .await
    .expect("registered B probe arms within the local bound");
    let exact_ack = LegControlFrame::try_new(
        probe.leg_generation(),
        LegControlRecord::LegProbeAck {
            session_id,
            leg_epoch_nonce,
            probe_sequence,
        },
        STANDBY_CONTROL_V1,
    )
    .expect("exact registered B probe ACK is valid");
    tokio::time::timeout(TEST_TIMEOUT, async {
        while session_clock
            .now()
            .expect("the registered-B session clock remains representable")
            <= deadline
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the exact B ACK is held until after its actor deadline");
    let late_at = session_clock
        .now()
        .expect("the post-deadline observation remains representable");
    server_b
        .enqueue_ordered_leg_control(exact_ack, STANDBY_CONTROL_V1)
        .expect("server B queues the deliberately late ACK");
    assert_eq!(
        server_b
            .try_flush(observed_at)
            .expect("late B ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let rejected = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => break outcome,
            }
        }
    })
    .await
    .expect("late B ACK is classified within the local bound");
    assert!(matches!(
        rejected,
        QuinnStandbyInboundDrive::RejectedProbe(
            RegisteredStandbyProbeAckErrorKind::DeadlineExpired,
        )
    ));
    assert_eq!(
        composition
            .try_expire_registered_standby_probe(late_at, &recovery_key)
            .expect("late ACK already demoted B without a second timer effect"),
        QuinnStandbyProbeExpiryDrive::Inert,
    );
    assert_eq!(
        composition
            .drive_registered_standby_probe(late_at)
            .expect("the exact registered B owner remains available for a fresh probe"),
        QuinnStandbyProbeDrive::Queued,
    );

    let (client_joined, server_a_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                server_b.join(),
            )
        })
        .await
        .expect("late-ACK tracer actors join within the local bound");
    let client_joined = client_joined.expect("client A/B join cleanly");
    let server_a_joined = server_a_joined.expect("server A joins cleanly");
    let server_b_joined = server_b_joined.expect("server B joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("late-ACK listener cleanup is bounded")
        .expect("late-ACK tracer releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn owned_standby_registration_pressure_retains_actor_and_pending_state_until_shutdown() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let generation = composition.snapshot().session.generation();
    let max_messages = composition_actor_limits().queue().max_messages;
    for _ in 0..max_messages {
        client_b
            .queue
            .as_mut()
            .expect("candidate B owns its sole FIFO")
            .push(
                Frame::try_new(
                    generation,
                    Record::OpenResult {
                        flow_id: local_flows[0].flow_id(),
                        result: OpenResultCode::Opened,
                    },
                )
                .expect("valid bounded pressure filler"),
            )
            .expect("candidate B FIFO admits the exact bounded filler");
    }
    assert_eq!(
        client_b.actor_snapshot().outbound_queue_messages(),
        max_messages
    );
    let session_before = composition.snapshot();
    assert_eq!(
        composition
            .begin_owned_standby_registration(
                client_b,
                crate::resumable::StandbyNonce::new([0xb1; 16])
                    .expect("nonzero pressure standby nonce"),
                &actor_attach_credentials(),
                QuinnStandbyLivenessConfig::new(
                    Duration::from_millis(30),
                    Duration::from_millis(200),
                )
                .expect("bounded pressure standby policy"),
            )
            .expect("pressure retains the entire candidate inside composition"),
        QuinnOwnedStandbyRegistrationDrive::Pressure,
    );
    assert_eq!(composition.snapshot(), session_before);
    assert_eq!(
        composition
            .drive_owned_standby_registration()
            .expect("retry sees the exact retained pending registration"),
        QuinnOwnedStandbyRegistrationDrive::Pressure,
    );

    let (client_joined, server_a_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                server_b.join(),
            )
        })
        .await
        .expect("pressure-owned actors join within the local bound");
    let client_joined = client_joined.expect("pressure-owned client A/B join cleanly");
    let server_a_joined = server_a_joined.expect("pressure server A joins cleanly");
    let server_b_joined = server_b_joined.expect("pressure server B joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("pressure listener cleanup is bounded")
        .expect("pressure tracer releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn owned_standby_registration_invariant_retains_actor_until_failed_join() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut client_b,
        server_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let binding = client_b
        .io
        .as_ref()
        .expect("candidate B owns its LegIo")
        .established_leg()
        .standby_transport_binding();
    let credentials = actor_attach_credentials();
    let request = AttachRequest::new(
        SessionId::new([0x93; 16]).expect("nonzero actor session id"),
        LegGeneration::new(2).expect("nonzero initial generation"),
        AttachNonce::new([0xb2; 16]).expect("nonzero pending attach nonce"),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION)
            .expect("single supported version"),
        FeatureOffer::new(STANDBY_CONTROL_V1.bits(), STANDBY_CONTROL_V1.bits())
            .expect("standby control feature offer"),
    );
    let proof = credentials
        .prove(&request, &binding)
        .expect("pending initial actor attach proof");
    let pending = client_b
        .io
        .as_ref()
        .expect("candidate B still owns its LegIo")
        .established_leg()
        .begin_attach(request)
        .bind_initial_frame(request.to_attach_frame(proof))
        .expect("bind exact pending initial actor frame");
    let awaiting_initial = pending
        .enqueue(client_b.queue.as_mut().expect("candidate B owns its FIFO"))
        .expect("candidate B queues the deliberately pending initial attach");
    let session_before = composition.snapshot();
    assert_eq!(
        composition
            .begin_owned_standby_registration(
                client_b,
                crate::resumable::StandbyNonce::new([0xb3; 16])
                    .expect("nonzero invariant standby nonce"),
                &credentials,
                QuinnStandbyLivenessConfig::new(
                    Duration::from_millis(30),
                    Duration::from_millis(200),
                )
                .expect("bounded invariant standby policy"),
            )
            .expect("post-enqueue invariant remains composition-owned"),
        QuinnOwnedStandbyRegistrationDrive::Failed(QuinnStandbyQueueFailureKind::Invariant),
    );
    drop(awaiting_initial);
    assert_eq!(composition.snapshot(), session_before);
    assert!(matches!(
        composition.drive_owned_standby_registration(),
        Err(QuinnOwnedStandbyRegistrationErrorKind::StandbyFailed),
    ));
    assert_eq!(
        composition
            .begin_failed_standby_join()
            .expect("sticky registration invariant starts exact B cleanup"),
        QuinnStandbyJoinStartDisposition::Started,
    );
    let server_b_task = tokio::spawn(async move {
        let mut server_b = server_b;
        server_b.join().await
    });
    let cleared = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_finish_failed_standby_join()
                .expect("failed registration B cleanup remains structurally owned")
            {
                QuinnStandbyJoinDrive::Pending => tokio::task::yield_now().await,
                QuinnStandbyJoinDrive::Cleared(cleared) => break cleared,
            }
        }
    })
    .await
    .expect("failed registration B clears within the local bound");
    assert_eq!(cleared.owned_messages(), 0);
    assert_eq!(cleared.owned_bytes(), 0);
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b_task)
        .await
        .expect("invariant server B cleanup remains bounded")
        .expect("invariant server B task joins")
        .expect("invariant server B actor joins structurally");
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);

    let (client_a_joined, server_a_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server_a.join())
    })
    .await
    .expect("current A remains independently cleanable after failed B");
    let client_a_joined = client_a_joined.expect("current A joins cleanly");
    let server_a_joined = server_a_joined.expect("server A joins cleanly");
    assert_eq!(client_a_joined.owned_messages(), 0);
    assert_eq!(client_a_joined.owned_bytes(), 0);
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    drop((
        composition,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("invariant listener cleanup is bounded")
        .expect("invariant tracer releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn composition_owned_fresh_b_survives_cancelled_shutdown_until_exact_join() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let session_before = composition.snapshot();
    let _fresh_until = register_owned_and_make_real_b_fresh(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        SimTime::from_nanos(1_000_000),
    )
    .await;
    assert_eq!(composition.snapshot(), session_before);

    let standby_join_block = composition
        .arm_standby_join_block_for_test()
        .expect("composition still owns the uninstalled fresh B actor");
    let server_a_task = tokio::spawn(async move { server_a.join().await });
    let server_b_task = tokio::spawn(async move { server_b.join().await });
    let mut first_shutdown = Box::pin(composition.join_current_for_shutdown());
    tokio::select! {
        biased;
        result = &mut first_shutdown => {
            panic!("shutdown bypassed the composition-owned B join gate: {result:?}")
        }
        () = standby_join_block.wait_until_blocked() => {}
    }
    drop(first_shutdown);
    assert!(matches!(
        tcp_factory.open_flow(local_flows[0]),
        Err(crate::owned_upstream::tcp::FlowPortError::FactoryAdmissionClosed),
    ));
    assert_eq!(
        composition.snapshot(),
        session_before,
        "cancelled cleanup cannot forge loss, recovery, or application ACK state",
    );
    assert!(matches!(
            composition.drive_owned_standby_post_loss_attach(
                SimTime::from_nanos(2_000_000),
                AttachNonce::new([0x98; 16]).expect("nonzero attach nonce"),
                &actor_attach_credentials(),
            ),
            Err(crate::owned_upstream::quinn_leg_composition::QuinnStandbyAttachError::SessionShuttingDown),
        ));
    assert!(matches!(
            composition.commit_owned_standby_post_loss_replacement(),
            Err(crate::owned_upstream::quinn_leg_composition::QuinnOwnedStandbyInstallError::SessionShuttingDown),
        ));
    assert_eq!(composition.snapshot(), session_before);
    standby_join_block.release();

    let client_joined = tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
        .await
        .expect("retrying cancelled shutdown remains bounded")
        .expect("composition joins current A and retained fresh B exactly once");
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a_task)
        .await
        .expect("server A cleanup task is bounded")
        .expect("server A task joins")
        .expect("server A actor joins cleanly");
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b_task)
        .await
        .expect("server B cleanup task is bounded")
        .expect("server B task joins")
        .expect("server B actor joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("cancelled-shutdown retry releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_without_runtime_rejects_before_owned_running_standby_or_current_mutation() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    assert_eq!(
        composition
            .begin_owned_standby_registration(
                client_b,
                crate::resumable::StandbyNonce::new([0xa8; 16])
                    .expect("nonzero no-runtime standby nonce"),
                &actor_attach_credentials(),
                QuinnStandbyLivenessConfig::new(
                    Duration::from_millis(30),
                    Duration::from_millis(200),
                )
                .expect("bounded no-runtime standby policy"),
            )
            .expect("composition owns the whole running standby actor"),
        QuinnOwnedStandbyRegistrationDrive::Queued,
    );
    let supervisor_before = composition.snapshot();
    let current_before = composition.current_actor_snapshot();
    let current_routes_before = composition.current_routes();
    let readiness_before = composition.current_actor_readiness_epoch_for_test();
    assert_eq!(composition.current_joined_ownership_for_test(), None);

    let (returned, runtime_unavailable) = std::thread::spawn(move || {
        let mut composition = composition;
        let mut shutdown = Box::pin(composition.join_current_for_shutdown());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let result = std::future::Future::poll(shutdown.as_mut(), &mut context);
        drop(shutdown);
        (
            composition,
            matches!(
                result,
                std::task::Poll::Ready(Err(QuinnCurrentJoinError::RuntimeUnavailable))
            ),
        )
    })
    .join()
    .expect("polling owned-standby shutdown without a runtime must not panic");
    composition = returned;
    assert!(
        runtime_unavailable,
        "a running owned standby requires a runtime before shutdown can publish or cancel",
    );
    assert_eq!(composition.snapshot(), supervisor_before);
    assert_eq!(composition.current_actor_snapshot(), current_before);
    assert_eq!(composition.current_routes(), current_routes_before);
    assert_eq!(
        composition.current_actor_readiness_epoch_for_test(),
        readiness_before,
    );
    assert_eq!(composition.current_joined_ownership_for_test(), None);
    assert!(
        composition
            .open_local(TargetAddr::IpPort(
                "127.0.0.1:9443".parse().expect("loopback target"),
            ))
            .is_none(),
        "a rejected shutdown still seals future external flow admission",
    );
    assert!(matches!(
        tcp_factory.open_flow(local_flows[0]),
        Err(crate::owned_upstream::tcp::FlowPortError::FactoryAdmissionClosed),
    ));
    assert_eq!(
        composition
            .drive_owned_standby_registration()
            .expect("runtime rejection leaves composition policy open"),
        QuinnOwnedStandbyRegistrationDrive::AlreadyQueued,
        "runtime preflight cannot move phase or consume the owned B typestate",
    );
    let live_current_probe = composition
        .begin_current_probe()
        .expect("runtime rejection cannot close the exact current actor barrier");
    assert_eq!(
        live_current_probe.generation(),
        supervisor_before.session.generation(),
    );
    let standby_join_block = composition
        .arm_standby_join_block_for_test()
        .expect("runtime rejection leaves the exact standby actor Running");
    standby_join_block.release();

    let server_a_task = tokio::spawn(async move { server_a.join().await });
    let server_b_task = tokio::spawn(async move { server_b.join().await });
    let client_joined = tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
        .await
        .expect("runtime-restored A/B shutdown remains bounded")
        .expect("runtime-restored A/B shutdown joins structurally");
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a_task)
        .await
        .expect("no-runtime server A cleanup remains bounded")
        .expect("no-runtime server A task joins")
        .expect("no-runtime server A actor joins structurally");
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b_task)
        .await
        .expect("no-runtime server B cleanup remains bounded")
        .expect("no-runtime server B task joins")
        .expect("no-runtime server B actor joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("no-runtime listener cleanup remains bounded")
        .expect("no-runtime retry releases both exact actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn failed_owned_b_join_is_split_phase_while_current_a_keeps_progressing() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let observed_at = SimTime::from_nanos(1_000_000);
    register_owned_and_make_real_b_fresh(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        observed_at,
    )
    .await;
    let session_before = composition.snapshot();
    let standby_join_block = composition
        .arm_standby_join_block_for_test()
        .expect("composition owns a live fresh B before failure");

    server_b.request_cancel();
    let server_b_task = tokio::spawn(async move { server_b.join().await });
    tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition.try_receive_registered_standby() {
                    QuinnStandbyInboundDrive::Actor(_) => break,
                    QuinnStandbyInboundDrive::Idle => {}
                    QuinnStandbyInboundDrive::StandbyFailed => break,
                    outcome => panic!("peer-close B produced the wrong inbound outcome: {outcome:?}"),
                }
                match composition.try_flush_registered_standby(observed_at) {
                    Err(crate::owned_upstream::quinn_leg_composition::QuinnStandbyProbeError::Actor(_)
                        | crate::owned_upstream::quinn_leg_composition::QuinnStandbyProbeError::StandbyFailed) => break,
                    Ok(QuinnLegActorFlushDisposition::Idle
                        | QuinnLegActorFlushDisposition::Submitted
                        | QuinnLegActorFlushDisposition::OrderedDeliveryPending) => {}
                    Err(error) => panic!("peer-close B produced the wrong flush error: {error:?}"),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("B peer close becomes an exact composition-owned failure");
    assert_eq!(
        composition
            .begin_failed_standby_join()
            .expect("observed B failure starts independent cleanup"),
        QuinnStandbyJoinStartDisposition::Started,
    );
    assert_eq!(
        composition
            .try_finish_failed_standby_join()
            .expect("blocked B join remains owned by the composition"),
        QuinnStandbyJoinDrive::Pending,
    );

    // Exercise both current-A writer and reader while failed B's close tail
    // is physically blocked. No `&mut composition` await may serialize A
    // behind that unrelated cleanup.
    let _queued = composition
        .begin_current_probe()
        .expect("current A remains able to queue control while B is Joining");
    assert_eq!(
        composition
            .try_flush_current(observed_at)
            .expect("current A remains able to flush while B is Joining"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound_probe =
        tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("current A probe receive is bounded")
            .expect("server A remains live")
            .expect("server A receives current probe during B cleanup");
    let BoundInbound::LegControl(inbound_probe) = take_actor_inbound(inbound_probe) else {
        panic!("current probe was misclassified during B cleanup");
    };
    let (_seal, probe) = inbound_probe.into_parts();
    let LegControlRecord::LegProbe {
        session_id,
        leg_epoch_nonce,
        probe_sequence,
    } = probe.record().clone()
    else {
        panic!("current A emitted a non-probe record during B cleanup");
    };
    server_a
        .enqueue_ordered_leg_control(
            LegControlFrame::try_new(
                probe.leg_generation(),
                LegControlRecord::LegProbeAck {
                    session_id,
                    leg_epoch_nonce,
                    probe_sequence,
                },
                STANDBY_CONTROL_V1,
            )
            .expect("matching current A ACK is valid"),
            STANDBY_CONTROL_V1,
        )
        .expect("server A queues current probe ACK");
    assert_eq!(
        server_a
            .try_flush(observed_at)
            .expect("server A ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnCurrentInboundDrive::ProbeAcked { .. } => break,
                _ => panic!("current A ACK was not consumed during B cleanup"),
            }
        }
    })
    .await
    .expect("current A inbound keeps progressing while B join is blocked");
    assert_eq!(composition.snapshot(), session_before);
    assert_eq!(
        composition
            .try_finish_failed_standby_join()
            .expect("B remains pending at its exact join gate"),
        QuinnStandbyJoinDrive::Pending,
    );

    let join_cursor = composition.progress_cursor();
    assert_eq!(
        composition
            .try_finish_failed_standby_join()
            .expect("the final bounded scan still observes blocked B"),
        QuinnStandbyJoinDrive::Pending,
    );
    let mut cancelled_join_wait =
        Box::pin(composition.wait_for_actor_progress(join_cursor.clone()));
    let mut cancelled_context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        std::future::Future::poll(cancelled_join_wait.as_mut(), &mut cancelled_context),
        std::task::Poll::Pending,
    ));
    drop(cancelled_join_wait);
    assert_eq!(
        composition
            .try_finish_failed_standby_join()
            .expect("dropping the pending wait retains the exact B join task"),
        QuinnStandbyJoinDrive::Pending,
    );
    standby_join_block.release();
    assert!(matches!(
        tokio::time::timeout(
            TEST_TIMEOUT,
            composition.wait_for_actor_progress(join_cursor),
        )
        .await
        .expect("the composition wakes on true B JoinHandle readiness")
        .expect("the exact B join completes cleanly"),
        QuinnCompositionProgress::StandbyJoin,
    ));
    let QuinnStandbyJoinDrive::Cleared(cleared) = composition
        .try_finish_failed_standby_join()
        .expect("the joined B witness remains synchronously clearable")
    else {
        panic!("a completed B join cannot return to Pending");
    };
    assert_eq!(cleared.owned_messages(), 0);
    assert_eq!(cleared.owned_bytes(), 0);
    assert_eq!(composition.snapshot(), session_before);
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b_task)
        .await
        .expect("failed server B cleanup is bounded")
        .expect("failed server B task joins")
        .expect("failed server B actor joins structurally");
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);

    // Clearing only B reopens the sole standby slot without disturbing A.
    let (client_b2, mut server_b2) = connect_real_actor_pair_with_clocks(
        &mut listener,
        LegId::B,
        composition_actor_limits(),
        peer_policy,
        composition.session_clock(),
        server_a.session_clock_for_test(),
    )
    .await;
    assert_eq!(
        composition
            .begin_owned_standby_registration(
                client_b2,
                crate::resumable::StandbyNonce::new([0xa7; 16])
                    .expect("nonzero replacement standby nonce"),
                &actor_attach_credentials(),
                QuinnStandbyLivenessConfig::new(
                    Duration::from_millis(30),
                    Duration::from_millis(200),
                )
                .expect("bounded replacement B policy"),
            )
            .expect("B2 admission succeeds after exact B cleanup"),
        QuinnOwnedStandbyRegistrationDrive::Queued,
    );
    wait_actor_ordered_delivery(&mut server_a).await;
    let server_a_task = tokio::spawn(async move { server_a.join().await });
    let server_b2_task = tokio::spawn(async move { server_b2.join().await });
    let client_joined = tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
        .await
        .expect("final A/B2 shutdown is bounded")
        .expect("final A/B2 shutdown joins exactly once");
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a_task)
        .await
        .expect("server A cleanup is bounded")
        .expect("server A task joins")
        .expect("server A joins structurally");
    let server_b2_joined = tokio::time::timeout(TEST_TIMEOUT, server_b2_task)
        .await
        .expect("server B2 cleanup is bounded")
        .expect("server B2 task joins")
        .expect("server B2 joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(server_b2_joined.owned_messages(), 0);
    assert_eq!(server_b2_joined.owned_bytes(), 0);
    drop((
        composition,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown after B2 is bounded")
        .expect("failed B and replacement B2 release both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn permanent_standby_task_loss_does_not_block_current_or_retired_cleanup() {
    let RealRegisteredReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut server_b,
        started_b,
        _tcp_factory: tcp_factory,
        local_flows,
        peer_policy,
        _owner_executor: owner_executor,
        _owner_active_a: owner_active_a,
        _standby_slot: standby_slot,
    } = real_registered_replacement_fixture(3, 1).await;
    assert_eq!(local_flows.len(), 1);
    assert_eq!(
        composition.snapshot().session.generation(),
        started_b.generation
    );
    let retired_route = composition
        .retired_inbound_route()
        .expect("normal replacement retains exact retired A");
    assert!(composition.retired_actor_snapshot().is_some());
    assert_eq!(composition.current_joined_ownership_for_test(), None);

    let (client_c, mut server_c) = connect_real_actor_pair_with_clocks(
        &mut listener,
        LegId::A,
        composition_actor_limits(),
        peer_policy,
        composition.session_clock(),
        server_b.session_clock_for_test(),
    )
    .await;
    assert_eq!(
        composition
            .begin_owned_standby_registration(
                client_c,
                crate::resumable::StandbyNonce::new([0xa9; 16])
                    .expect("nonzero task-loss standby nonce"),
                &actor_attach_credentials(),
                QuinnStandbyLivenessConfig::new(
                    Duration::from_millis(30),
                    Duration::from_millis(200),
                )
                .expect("bounded task-loss standby policy"),
            )
            .expect("current B adopts the whole third actor as standby"),
        QuinnOwnedStandbyRegistrationDrive::Queued,
    );
    let standby_join_block = composition
        .arm_standby_join_block_for_test()
        .expect("composition owns running standby C before its failure");

    server_c.request_cancel();
    let server_c_joined = tokio::time::timeout(TEST_TIMEOUT, server_c.join())
        .await
        .expect("standby C peer cleanup remains bounded")
        .expect("standby C peer joins structurally");
    assert_eq!(server_c_joined.owned_messages(), 0);
    assert_eq!(server_c_joined.owned_bytes(), 0);
    tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition.try_receive_registered_standby() {
                    QuinnStandbyInboundDrive::Actor(_)
                    | QuinnStandbyInboundDrive::StandbyFailed => break,
                    QuinnStandbyInboundDrive::Idle => {}
                    outcome => {
                        panic!("standby C peer-close produced the wrong inbound outcome: {outcome:?}")
                    }
                }
                match composition.try_flush_registered_standby(SimTime::ZERO) {
                    Err(
                        crate::owned_upstream::quinn_leg_composition::QuinnStandbyProbeError::Actor(
                            _,
                        )
                        | crate::owned_upstream::quinn_leg_composition::QuinnStandbyProbeError::StandbyFailed,
                    ) => break,
                    Ok(
                        QuinnLegActorFlushDisposition::Idle
                        | QuinnLegActorFlushDisposition::Submitted
                        | QuinnLegActorFlushDisposition::OrderedDeliveryPending,
                    ) => {}
                    Err(error) => {
                        panic!("standby C peer-close produced the wrong flush error: {error:?}")
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("standby C peer-close becomes one sticky composition failure");
    assert_eq!(
        composition
            .begin_failed_standby_join()
            .expect("failed standby C starts its sole structured join"),
        QuinnStandbyJoinStartDisposition::Started,
    );
    tokio::time::timeout(TEST_TIMEOUT, standby_join_block.wait_until_blocked())
        .await
        .expect("standby C join reaches the deterministic close-tail gate");
    assert!(
        composition.abort_standby_join_task_for_test(),
        "test abort targets only the installed standby Joining task",
    );
    let initial_task_lost_cause = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_finish_failed_standby_join() {
                Ok(QuinnStandbyJoinDrive::Pending) => tokio::task::yield_now().await,
                Ok(QuinnStandbyJoinDrive::Cleared(cleared)) => {
                    panic!("aborted standby task forged a joined witness: {cleared:?}")
                }
                Err(
                    crate::owned_upstream::quinn_leg_composition::QuinnStandbyClearError::JoinTask(
                        cause,
                    ),
                ) => break cause,
                Err(error) => panic!("aborted standby task produced the wrong error: {error:?}"),
            }
        }
    })
    .await
    .expect("aborted standby join deterministically persists TaskLost");
    standby_join_block.release();
    assert_eq!(composition.retired_inbound_route(), Some(retired_route));
    assert!(composition.retired_actor_snapshot().is_some());
    assert_eq!(composition.current_joined_ownership_for_test(), None);

    let server_a_task = tokio::spawn(async move { server_a.join().await });
    let server_b_task = tokio::spawn(async move { server_b.join().await });
    let first_shutdown_error =
        tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
            .await
            .expect("TaskLost shutdown still completes current and retired cleanup")
            .expect_err("permanent standby TaskLost remains a fail-closed shutdown error");
    let QuinnCurrentJoinError::StandbyJoinTask(first_shutdown_cause) = first_shutdown_error else {
        panic!("TaskLost shutdown returned the wrong error: {first_shutdown_error:?}");
    };
    assert!(Arc::ptr_eq(&initial_task_lost_cause, &first_shutdown_cause));
    assert_eq!(
        composition.current_joined_ownership_for_test(),
        Some((0, 0)),
        "standby task loss cannot block current B structural cleanup",
    );
    assert_eq!(
        composition.retired_inbound_route(),
        None,
        "standby task loss cannot block retired A controller cleanup",
    );
    assert_eq!(composition.retired_actor_snapshot(), None);
    assert!(
        composition.retired_owner_is_absent_for_test(),
        "retired A joined witness is consumed after exact controller clear",
    );

    let second_shutdown_error =
        tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
            .await
            .expect("repeated TaskLost shutdown remains synchronously bounded")
            .expect_err("repeated shutdown retains permanent standby failure");
    let QuinnCurrentJoinError::StandbyJoinTask(second_shutdown_cause) = second_shutdown_error
    else {
        panic!("repeated TaskLost shutdown returned the wrong error: {second_shutdown_error:?}");
    };
    assert!(Arc::ptr_eq(&first_shutdown_cause, &second_shutdown_cause));
    assert_eq!(
        composition.current_joined_ownership_for_test(),
        Some((0, 0))
    );
    assert_eq!(composition.retired_inbound_route(), None);
    assert_eq!(composition.retired_actor_snapshot(), None);
    assert!(composition.retired_owner_is_absent_for_test());

    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a_task)
        .await
        .expect("TaskLost server A cleanup remains bounded")
        .expect("TaskLost server A task joins")
        .expect("TaskLost server A actor joins structurally");
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b_task)
        .await
        .expect("TaskLost server B cleanup remains bounded")
        .expect("TaskLost server B task joins")
        .expect("TaskLost server B actor joins structurally");
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        server_c,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("TaskLost listener cleanup remains bounded")
        .expect("current, retired, and failed standby release all listener slots");
}

async fn complete_real_registered_attach(
    registered_b: ClientRegisteredStandby,
    client_b: &mut StartedQuinnLegActor,
    server_b: &mut StartedQuinnLegActor,
    owner_executor: &mut OwnerTargetExecutor<MemoryTarget>,
    standby_slot: &mut OwnerStandbySlot,
) -> (AcceptedClientRegisteredAttach, AttachedLeg) {
    let awaiting_attach_b = registered_b
        .begin_exact_next_attach(
            AttachNonce::new([0x98; 16]).expect("nonzero B attach nonce"),
            &actor_attach_credentials(),
        )
        .expect("registered B derives exact next attach")
        .enqueue(client_b.queue.as_mut().expect("client B owns its FIFO"))
        .expect("client B queues exact next attach");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("B attach flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let attach_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B attach receive is bounded")
            .expect("server B remains live")
            .expect("server B receives exact next attach");
    let BoundInbound::Session(received_attach_b) = take_actor_inbound(attach_inbound) else {
        panic!("registered B attach was misclassified as leg control");
    };
    wait_actor_ordered_delivery(client_b).await;
    let OwnerTargetAttachPublication::Installed { pending } = owner_executor
        .accept_registered_owner_attach(
            standby_slot,
            server_b
                .io
                .as_ref()
                .expect("server B retains its LegIo")
                .established_leg(),
            received_attach_b,
            server_b.queue.as_mut().expect("server B retains its FIFO"),
        )
        .expect("owner installs exact actor B attach")
    else {
        panic!("exact actor B attach unexpectedly resynchronized");
    };
    let recovery = pending
        .enqueue_acceptance(server_b.queue.as_mut().expect("server B FIFO remains live"))
        .expect("owner queues B attach acceptance");
    let recovery_outputs = owner_executor
        .execute_attached_recovery(
            recovery,
            server_b.queue.as_mut().expect("server B FIFO remains live"),
        )
        .expect("owner completes bounded B attach recovery");
    let owner_active_b = recovery_outputs
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
                panic!("bounded fixture recovery unexpectedly needs continuation")
            }
        })
        .expect("owner recovery releases exact active B authority");
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("B attach acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let attach_acceptance =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B attach acceptance receive is bounded")
            .expect("client B remains live")
            .expect("client B receives attach acceptance");
    let accepted_b = match awaiting_attach_b
        .validate_response(take_actor_inbound(attach_acceptance))
        .expect("client validates exact actor B attach response")
    {
        RegisteredAttachResponse::Accepted(accepted) => accepted,
        RegisteredAttachResponse::GenerationStatus(_) => {
            panic!("exact actor B attach unexpectedly returned generation status");
        }
    };
    (accepted_b, owner_active_b)
}

/// Owner half of the ATTACH transaction after B has been adopted. The
/// client actor and every client typestate remain private in composition.
async fn complete_composition_owned_registered_attach(
    composition: &mut QuinnClientSessionComposition,
    server_b: &mut StartedQuinnLegActor,
    owner_executor: &mut OwnerTargetExecutor<MemoryTarget>,
    standby_slot: &mut OwnerStandbySlot,
    now: SimTime,
) -> AttachedLeg {
    assert_eq!(
        composition
            .drive_owned_standby_post_loss_attach(
                now,
                AttachNonce::new([0x98; 16]).expect("nonzero B attach nonce"),
                &actor_attach_credentials(),
            )
            .expect("fresh composition-owned B derives exact next attach"),
        crate::owned_upstream::quinn_leg_composition::QuinnStandbyAttachDrive::AwaitingResponse,
    );
    assert_eq!(
        composition
            .try_flush_registered_standby(now)
            .expect("composition-owned B attach flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let attach_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("composition-owned B attach receive is bounded")
            .expect("server B remains live")
            .expect("server B receives the exact next attach");
    let BoundInbound::Session(received_attach_b) = take_actor_inbound(attach_inbound) else {
        panic!("composition-owned B attach was misclassified as leg control");
    };
    let OwnerTargetAttachPublication::Installed { pending } = owner_executor
        .accept_registered_owner_attach(
            standby_slot,
            server_b
                .io
                .as_ref()
                .expect("server B retains its LegIo")
                .established_leg(),
            received_attach_b,
            server_b.queue.as_mut().expect("server B retains its FIFO"),
        )
        .expect("owner installs exact composition-owned B attach")
    else {
        panic!("composition-owned B attach unexpectedly resynchronized");
    };
    let recovery = pending
        .enqueue_acceptance(server_b.queue.as_mut().expect("server B FIFO remains live"))
        .expect("owner queues composition-owned B acceptance");
    let recovery_outputs = owner_executor
        .execute_attached_recovery(
            recovery,
            server_b.queue.as_mut().expect("server B FIFO remains live"),
        )
        .expect("owner completes bounded B attach recovery");
    let owner_active_b = recovery_outputs
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
                panic!("bounded composition-owned recovery unexpectedly needs continuation")
            }
        })
        .expect("owner recovery releases exact active B authority");
    assert_eq!(
        server_b
            .try_flush(now)
            .expect("composition-owned B acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let accepted_generation = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::AttachAccepted { generation } => break generation,
                outcome => panic!("exact B attach response did not validate: {outcome:?}"),
            }
        }
    })
    .await
    .expect("composition validates B acceptance within the local bound");
    assert_eq!(accepted_generation, owner_active_b.generation());
    owner_active_b
}

/// Builds the exact real registered-B acceptance while A remains current.
/// The caller decides whether B enters the normal transaction.
async fn real_accepted_replacement_fixture(
    max_live_actors: usize,
    local_flow_count: usize,
) -> RealAcceptedReplacementFixture {
    real_accepted_replacement_fixture_with_target_state(max_live_actors, local_flow_count, false)
        .await
}

async fn real_accepted_replacement_fixture_with_target_state(
    max_live_actors: usize,
    local_flow_count: usize,
    establish_targets: bool,
) -> RealAcceptedReplacementFixture {
    let RealRegisteredStandbyFixture {
        listener,
        composition,
        server_a,
        mut client_b,
        mut server_b,
        registered_b,
        tcp_factory,
        local_flows,
        peer_policy,
        mut owner_executor,
        owner_active_a,
        mut standby_slot,
    } = real_registered_standby_fixture_with_target_state(
        max_live_actors,
        local_flow_count,
        establish_targets,
    )
    .await;
    let (accepted_b, owner_active_b) = complete_real_registered_attach(
        registered_b,
        &mut client_b,
        &mut server_b,
        &mut owner_executor,
        &mut standby_slot,
    )
    .await;
    if establish_targets {
        tokio::time::timeout(TEST_TIMEOUT, async {
            while server_b.actor_snapshot().pending_delivery_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("B attach acceptance DELIVERED remains bounded");
        assert!(
            server_b.actor_snapshot().outbound_queue_messages() > 0,
            "live Target recovery state remains queued until B is installed into composition",
        );
    } else {
        wait_actor_ordered_delivery(&mut server_b).await;
    }
    RealAcceptedReplacementFixture {
        listener,
        composition,
        server_a,
        client_b,
        server_b,
        accepted_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
        owner_active_b,
        standby_slot,
    }
}

async fn apply_real_queued_owner_recovery_state(
    composition: &mut QuinnClientSessionComposition,
    server_b: &mut StartedQuinnLegActor,
    expected_records: usize,
) -> (usize, Vec<SessionEffect>) {
    assert!(expected_records > 0);
    let mut applied_records = 0;
    let mut applied_effects = Vec::new();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let _ = server_b
                .try_flush(SimTime::ZERO)
                .expect("queued owner recovery-state flush remains live");
            loop {
                match composition.try_receive_current() {
                    QuinnCurrentInboundDrive::Idle => break,
                    QuinnCurrentInboundDrive::Applied {
                        effects,
                        observation: None,
                    } => {
                        applied_records += 1;
                        assert!(applied_records <= expected_records);
                        applied_effects.extend(effects);
                    }
                    _ => {
                        panic!("queued owner recovery state was not ordinary current-B traffic")
                    }
                }
            }
            let server_snapshot = server_b.actor_snapshot();
            let client_snapshot = composition.current_actor_snapshot();
            if applied_records == expected_records
                && server_snapshot.outbound_queue_messages() == 0
                && server_snapshot.outbound_queue_bytes() == 0
                && server_snapshot.outbound_owned_messages() == 0
                && server_snapshot.outbound_owned_bytes() == 0
                && server_snapshot.pending_delivery_count() == 0
                && client_snapshot.queued_inbound_messages() == 0
                && client_snapshot.inbound_owned_bytes() == 0
                && client_snapshot.waiting_inbound_bytes() == 0
                && client_snapshot.composition_owned_messages() == 0
                && client_snapshot.reserved_delivery_acks() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued owner recovery state applies through current B within the local bound");
    (applied_records, applied_effects)
}

/// Builds the exact real A-current -> A-retired/B-recovering composition
/// seam. `local_flow_count` controls how many replay frames remain in B's
/// bounded recovery plan without replacing any transport with a fake.
async fn real_registered_replacement_fixture(
    max_live_actors: usize,
    local_flow_count: usize,
) -> RealRegisteredReplacementFixture {
    let RealAcceptedReplacementFixture {
        listener,
        mut composition,
        server_a,
        client_b,
        server_b,
        accepted_b,
        tcp_factory,
        local_flows,
        peer_policy,
        owner_executor,
        owner_active_a,
        owner_active_b: _,
        standby_slot,
    } = real_accepted_replacement_fixture(max_live_actors, local_flow_count).await;
    let started_b = composition
        .begin_registered_replacement(accepted_b, client_b)
        .expect("composition atomically replaces A with exact actor B");
    RealRegisteredReplacementFixture {
        listener,
        composition,
        server_a,
        server_b,
        started_b,
        _tcp_factory: tcp_factory,
        local_flows,
        peer_policy,
        _owner_executor: owner_executor,
        _owner_active_a: owner_active_a,
        _standby_slot: standby_slot,
    }
}

async fn assert_real_retired_session_frame_is_inert(
    composition: &mut QuinnClientSessionComposition,
    server_a: &mut StartedQuinnLegActor,
    frame: Frame,
    expected: crate::owned_upstream::leg::RetiredClientInboundKind,
) {
    server_a
        .queue
        .as_mut()
        .expect("server A retains its sole FIFO while retired")
        .push(frame)
        .expect("server A queues one bounded late session frame");
    assert_eq!(
        server_a
            .try_flush(SimTime::ZERO)
            .expect("retired A late session-frame flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition
                .retired_actor_snapshot()
                .expect("retired A remains a running physical actor");
            if snapshot.queued_inbound_messages() == 1
                && snapshot.inbound_owned_bytes() > 0
                && snapshot.composition_owned_messages() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retired A publishes one bounded late session frame");
    match composition.try_receive_retired() {
        QuinnRetiredInboundDrive::Retired(actual) => assert_eq!(actual, expected),
        _ => panic!("retired A did not inertly classify the exact session frame"),
    }
    wait_actor_delivery_ack_drain(|| {
        composition
            .retired_actor_snapshot()
            .expect("retired A remains live while its delivery ACK drains")
    })
    .await;
    wait_actor_ordered_delivery(server_a).await;
    let retired_after = composition
        .retired_actor_snapshot()
        .expect("retired A remains live after inert session-frame discard");
    assert_eq!(retired_after.queued_inbound_messages(), 0);
    assert_eq!(retired_after.inbound_owned_bytes(), 0);
    assert_eq!(retired_after.composition_owned_messages(), 0);
    assert_eq!(retired_after.reserved_delivery_acks(), 0);
}

async fn assert_real_current_session_frame_is_reducer_rejected(
    composition: &mut QuinnClientSessionComposition,
    server_b: &mut StartedQuinnLegActor,
    frame: Frame,
) {
    let expected_current = composition.snapshot().session.generation().get();
    let expected_actual = frame.leg_generation().get();
    server_b
        .queue
        .as_mut()
        .expect("server B retains its sole FIFO while current")
        .push(frame)
        .expect("server B queues one bounded stale-generation session frame");
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("current B stale session-frame flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition.current_actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.inbound_owned_bytes() > 0
                && snapshot.composition_owned_messages() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("current B publishes one bounded stale-generation session frame");
    assert!(matches!(
        composition.try_receive_current(),
        QuinnCurrentInboundDrive::ReducerRejected(SessionSupervisorError::Reducer(
            SessionError::FrameGenerationMismatch { current, actual }
        )) if current == expected_current && actual == expected_actual,
    ));
    wait_actor_delivery_ack_drain(|| composition.current_actor_snapshot()).await;
    wait_actor_ordered_delivery(server_b).await;
    let current_after = composition.current_actor_snapshot();
    assert_eq!(current_after.queued_inbound_messages(), 0);
    assert_eq!(current_after.inbound_owned_bytes(), 0);
    assert_eq!(current_after.composition_owned_messages(), 0);
    assert_eq!(current_after.reserved_delivery_acks(), 0);
}

fn assert_actor_snapshot_stable_except_validated_headers(
    actual: QuinnLegActorSnapshot,
    before: QuinnLegActorSnapshot,
    added_headers: usize,
) {
    assert_eq!(
        actual.validated_data_headers(),
        before.validated_data_headers() + added_headers,
    );
    assert_eq!(
        actual.outbound_queue_messages(),
        before.outbound_queue_messages()
    );
    assert_eq!(actual.outbound_queue_bytes(), before.outbound_queue_bytes());
    assert_eq!(
        actual.queued_inbound_messages(),
        before.queued_inbound_messages()
    );
    assert_eq!(actual.inbound_owned_bytes(), before.inbound_owned_bytes());
    assert_eq!(
        actual.waiting_inbound_bytes(),
        before.waiting_inbound_bytes()
    );
    assert_eq!(
        actual.reserved_delivery_acks(),
        before.reserved_delivery_acks()
    );
    assert_eq!(
        actual.composition_owned_messages(),
        before.composition_owned_messages()
    );
    assert_eq!(
        actual.outbound_owned_messages(),
        before.outbound_owned_messages()
    );
    assert_eq!(actual.outbound_owned_bytes(), before.outbound_owned_bytes());
    assert_eq!(actual.blocked_write(), before.blocked_write());
    assert_eq!(
        actual.pending_delivery_count(),
        before.pending_delivery_count()
    );
    assert_eq!(
        actual.pending_delivery_ordinal(),
        before.pending_delivery_ordinal()
    );
}

async fn assert_real_owner_retired_a_session_frame_is_reducer_rejected(
    composition: &mut QuinnClientSessionComposition,
    server_a: &mut StartedQuinnLegActor,
    owner_active_a: &AttachedLeg,
    owner_executor: &mut OwnerTargetExecutor<MemoryTarget>,
    frame: Frame,
) {
    let session_before = composition.snapshot();
    let owner_before = owner_executor.snapshot();
    let target_before = owner_executor.target().snapshot();
    let expected_current = owner_before.session.session.generation().get();
    let expected_actual = frame.leg_generation().get();
    composition
        .enqueue_current_frame(frame)
        .expect("client A queues one bounded owner-stale session frame");
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("client A owner-stale frame flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("owner A stale frame receive is bounded")
        .expect("owner A actor remains live")
        .expect("owner A actor receives one exact stale frame");
    let BoundInbound::Session(received) = take_actor_inbound(inbound) else {
        panic!("owner A stale session frame was misclassified as leg control");
    };
    let event = owner_active_a
        .accept_frame(received)
        .expect("exact physical A provenance binds the stale owner event");
    assert!(matches!(
        owner_executor.apply_event(event),
        Err(OwnerTargetError::Supervisor(SessionSupervisorError::Reducer(
            SessionError::LegGenerationMismatch { current, actual }
        ))) if current == expected_current && actual == expected_actual,
    ));
    assert_eq!(composition.snapshot(), session_before);
    assert_eq!(owner_executor.snapshot(), owner_before);
    assert_eq!(owner_executor.target().snapshot(), target_before);
    wait_actor_delivery_ack_drain(|| server_a.actor_snapshot()).await;
    wait_composition_current_ordered_delivery(composition).await;
    let owner_a_after = server_a.actor_snapshot();
    assert_eq!(owner_a_after.queued_inbound_messages(), 0);
    assert_eq!(owner_a_after.inbound_owned_bytes(), 0);
    assert_eq!(owner_a_after.composition_owned_messages(), 0);
    assert_eq!(owner_a_after.reserved_delivery_acks(), 0);
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let client_a_after = composition.current_actor_snapshot();
            if client_a_after.outbound_queue_messages() == 0
                && client_a_after.outbound_queue_bytes() == 0
                && client_a_after.outbound_owned_messages() == 0
                && client_a_after.outbound_owned_bytes() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("client A releases stale owner-frame outbound ownership");
}

#[tokio::test(flavor = "current_thread")]
async fn real_retired_a_session_matrix_cannot_mutate_b_replay_or_target() {
    let RealAcceptedReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        accepted_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
        owner_active_b,
        standby_slot: _,
    } = real_accepted_replacement_fixture_with_target_state(2, 1, true).await;
    let generation_a = composition.snapshot().session.generation();
    for frame in [
        Frame::try_new(
            generation_a,
            Record::Data {
                flow_id: local_flows[0].flow_id(),
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"owner-late-a"),
            },
        )
        .expect("owner-stale A DATA remains valid wire input"),
        Frame::try_new(
            generation_a,
            Record::Ack {
                flow_id: local_flows[0].flow_id(),
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(0),
                final_accepted: false,
            },
        )
        .expect("owner-stale A ACK remains valid wire input"),
        Frame::try_new(
            generation_a,
            Record::Close {
                flow_id: local_flows[0].flow_id(),
                direction: Direction::ClientToTarget,
                final_offset: ByteOffset::new(0),
            },
        )
        .expect("owner-stale A CLOSE remains valid wire input"),
        Frame::try_new(
            generation_a,
            Record::Reset {
                flow_id: local_flows[0].flow_id(),
                reason: crate::resumable::ResetReason::TargetFailure,
            },
        )
        .expect("owner-stale A RESET remains valid wire input"),
    ] {
        assert_real_owner_retired_a_session_frame_is_reducer_rejected(
            &mut composition,
            &mut server_a,
            &owner_active_a,
            &mut owner_executor,
            frame,
        )
        .await;
    }
    let (flow, mut driver) = tcp_factory
        .open_flow(local_flows[0])
        .expect("the exact local flow enters the typed source seam");
    flow.try_send_uplink_with(9, || Bytes::from_static(b"late-data"))
        .expect("one bounded replay segment is reserved before replacement");
    let input = driver
        .try_recv_next_for_client_controller()
        .expect("typed source dequeue remains live")
        .expect("one exact DATA input is ready");
    let effects = match composition
        .apply_driver_input(input)
        .expect("current A admits the exact source command")
    {
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Applied(effects) => {
            effects
        }
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Deferred {
            ..
        } => panic!("small exact DATA unexpectedly met reducer pressure"),
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Failed(_) => {
            panic!("small exact DATA unexpectedly failed the reducer")
        }
    };
    assert!(effects.iter().any(|effect| {
            matches!(
                effect,
                SessionEffect::Transmit(frame)
                    if matches!(frame.record(), Record::Data { payload, .. } if payload.as_ref() == b"late-data")
            )
        }));
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 9);

    let delayed_a_delivered = Arc::clone(&server_a.delivery_writer_gate);
    delayed_a_delivered.arm();
    let queued_a = composition
        .begin_current_probe()
        .expect("current A queues one exact ordered probe before replacement");
    assert_eq!(queued_a.generation(), generation_a);
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("current A pre-replacement probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound_probe =
        tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("current A pre-replacement probe receive is bounded")
            .expect("server A remains live")
            .expect("server A receives the exact current probe");
    let BoundInbound::LegControl(inbound_probe) = take_actor_inbound(inbound_probe) else {
        panic!("current A probe was misclassified as session data");
    };
    let (_seal, probe) = inbound_probe.into_parts();
    assert!(matches!(
        probe.record(),
        LegControlRecord::LegProbe { probe_sequence, .. }
            if *probe_sequence == queued_a.probe_sequence()
    ));
    tokio::time::timeout(TEST_TIMEOUT, delayed_a_delivered.wait_until_blocked())
        .await
        .expect("server A withholds the exact physical DELIVERED publication");
    let held_a = composition.current_actor_snapshot();
    assert_eq!(held_a.pending_delivery_count(), 1);
    assert!(held_a.pending_delivery_ordinal().is_some());
    assert_eq!(held_a.outbound_owned_messages(), 1);
    assert!(held_a.outbound_owned_bytes() > 0);
    let queued_a = match composition.arm_current_probe(
        queued_a,
        SimTime::ZERO,
        Duration::from_millis(50),
    ) {
        Err(
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::NotDelivered(
                queued,
            ),
        ) => queued,
        outcome => panic!("withheld A DELIVERED produced the wrong arm result: {outcome:?}"),
    };

    let started_b = composition
        .begin_registered_replacement(accepted_b, client_b)
        .expect("composition atomically installs B and retires A");
    assert_eq!(started_b.generation.get(), generation_a.get() + 1);
    assert_eq!(
        composition.normal_replacement_blocker(),
        Some(ClientRecoveryBeginErrorKind::NotActive),
    );
    let session_before = composition.snapshot();
    let owner_before = owner_executor.snapshot();
    let target_before = owner_executor.target().snapshot();
    let current_before = composition.current_actor_snapshot();

    let retired_held_a = composition
        .retired_actor_snapshot()
        .expect("A retires with its actor-local DELIVERED receipt pending");
    assert_eq!(retired_held_a.pending_delivery_count(), 1);
    assert_eq!(
        retired_held_a.pending_delivery_ordinal(),
        held_a.pending_delivery_ordinal()
    );
    delayed_a_delivered.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let retired_a = composition
                .retired_actor_snapshot()
                .expect("retired A remains running for late DELIVERED");
            if retired_a.pending_delivery_count() == 0
                && retired_a.pending_delivery_ordinal().is_none()
                && retired_a.outbound_owned_messages() == 0
                && retired_a.outbound_owned_bytes() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late A DELIVERED reconciles only A's actor-private receipt");
    assert_eq!(composition.snapshot(), session_before);
    assert_eq!(composition.snapshot().uplink_replay_extents, 1);
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 9);
    assert_eq!(owner_executor.snapshot(), owner_before);
    assert_eq!(owner_executor.target().snapshot(), target_before);
    assert_eq!(composition.current_actor_snapshot(), current_before);
    assert!(matches!(
        composition.arm_current_probe(queued_a, SimTime::ZERO, Duration::from_millis(50),),
        Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::Stale(_)),
    ));
    assert_eq!(composition.snapshot(), session_before);
    assert_eq!(composition.current_actor_snapshot(), current_before);

    for claimed_generation in [generation_a, started_b.generation] {
        let cases = [
            (
                Frame::try_new(
                    claimed_generation,
                    Record::Data {
                        flow_id: local_flows[0].flow_id(),
                        direction: Direction::TargetToClient,
                        offset: ByteOffset::new(0),
                        payload: Bytes::from_static(b"late-from-a"),
                    },
                )
                .expect("late retired-A DATA remains valid wire input"),
                crate::owned_upstream::leg::RetiredClientInboundKind::Data,
            ),
            (
                Frame::try_new(
                    claimed_generation,
                    Record::Ack {
                        flow_id: local_flows[0].flow_id(),
                        direction: Direction::ClientToTarget,
                        next_accepted: ByteOffset::new(9),
                        final_accepted: false,
                    },
                )
                .expect("late retired-A ACK remains valid wire input"),
                crate::owned_upstream::leg::RetiredClientInboundKind::Ack,
            ),
            (
                Frame::try_new(
                    claimed_generation,
                    Record::Close {
                        flow_id: local_flows[0].flow_id(),
                        direction: Direction::TargetToClient,
                        final_offset: ByteOffset::new(0),
                    },
                )
                .expect("late retired-A CLOSE remains valid wire input"),
                crate::owned_upstream::leg::RetiredClientInboundKind::Close,
            ),
            (
                Frame::try_new(
                    claimed_generation,
                    Record::Reset {
                        flow_id: local_flows[0].flow_id(),
                        reason: crate::resumable::ResetReason::TargetFailure,
                    },
                )
                .expect("late retired-A RESET remains valid wire input"),
                crate::owned_upstream::leg::RetiredClientInboundKind::Reset,
            ),
        ];
        for (frame, expected) in cases {
            assert_real_retired_session_frame_is_inert(
                &mut composition,
                &mut server_a,
                frame,
                expected,
            )
            .await;
            assert_eq!(composition.snapshot(), session_before);
            assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 9);
            assert_eq!(
                composition.normal_replacement_blocker(),
                Some(ClientRecoveryBeginErrorKind::NotActive),
            );
            assert_eq!(owner_executor.target().snapshot(), target_before);
            assert_eq!(owner_executor.snapshot(), owner_before);
            assert_eq!(composition.current_actor_snapshot(), current_before);
        }
    }

    let late_probe_ack = LegControlFrame::try_new(
        generation_a,
        LegControlRecord::LegProbeAck {
            session_id: SessionId::new([0x93; 16]).expect("nonzero actor session id"),
            leg_epoch_nonce: LegEpochNonce::new([0xa4; 16]).expect("nonzero stale A epoch"),
            probe_sequence: ProbeSequence::new(1).expect("nonzero stale A probe sequence"),
        },
        STANDBY_CONTROL_V1,
    )
    .expect("late retired-A probe ACK remains valid wire input");
    let late_probe_receipt = server_a
        .enqueue_ordered_leg_control(late_probe_ack, STANDBY_CONTROL_V1)
        .expect("server A queues one bounded late probe ACK");
    assert!(!late_probe_receipt.was_delivered());
    assert_eq!(
        server_a
            .try_flush(SimTime::ZERO)
            .expect("retired A late probe-ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition
                .retired_actor_snapshot()
                .expect("retired A remains live for its late probe ACK");
            if snapshot.queued_inbound_messages() == 1
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retired A publishes one bounded late probe ACK");
    assert!(matches!(
        composition.try_receive_retired(),
        QuinnRetiredInboundDrive::Rejected,
    ));
    assert_eq!(composition.snapshot(), session_before);
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 9);
    assert_eq!(owner_executor.target().snapshot(), target_before);
    assert_eq!(owner_executor.snapshot(), owner_before);
    assert_eq!(composition.current_actor_snapshot(), current_before);
    wait_actor_ordered_delivery(&mut server_a).await;
    assert!(late_probe_receipt.was_delivered());
    let retired_after_probe = composition
        .retired_actor_snapshot()
        .expect("retired A remains live after rejecting its late probe ACK");
    assert_eq!(retired_after_probe.queued_inbound_messages(), 0);
    assert_eq!(retired_after_probe.inbound_owned_bytes(), 0);
    assert_eq!(retired_after_probe.composition_owned_messages(), 0);
    assert_eq!(retired_after_probe.reserved_delivery_acks(), 0);

    let joining_receipt = server_a
        .enqueue_ordered_leg_control(
            ordered_control_for_generation(generation_a, 701),
            STANDBY_CONTROL_V1,
        )
        .expect("server A queues one exact record at the retired-join boundary");
    assert_eq!(
        server_a
            .try_flush(SimTime::ZERO)
            .expect("retired A join-boundary flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let retired_a = composition
                .retired_actor_snapshot()
                .expect("retired A remains Running before join linearizes");
            if retired_a.queued_inbound_messages() == 1
                && retired_a.inbound_owned_bytes() > 0
                && retired_a.composition_owned_messages() == 1
                && retired_a.reserved_delivery_acks() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one retired A record reaches the exact pre-join ownership boundary");
    let session_before_join = composition.snapshot();
    let owner_before_join = owner_executor.snapshot();
    let target_before_join = owner_executor.target().snapshot();
    let current_b_before_join = composition.current_actor_snapshot();
    let current_routes_before_join = composition.current_routes();
    assert_eq!(
        composition
            .begin_retired_join()
            .expect("retired A starts one exact structural join"),
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinStartDisposition::Started,
    );
    assert!(matches!(
        composition.try_receive_retired(),
        QuinnRetiredInboundDrive::NotRunning,
    ));
    assert!(!joining_receipt.was_delivered());
    let server_a_task = tokio::spawn(async move { server_a.join().await });
    let cleared_a = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_finish_retired_join()
                .expect("retired A join-boundary cleanup remains live")
            {
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending => {
                    tokio::task::yield_now().await;
                }
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Cleared(
                    cleared,
                ) => break cleared,
            }
        }
    })
    .await
    .expect("retired A join-boundary cleanup remains bounded");
    assert_eq!(cleared_a.generation(), generation_a);
    assert_eq!(cleared_a.owned_messages(), 0);
    assert_eq!(cleared_a.owned_bytes(), 0);
    assert!(!cleared_a.terminal_failure());
    assert!(!joining_receipt.was_delivered());
    assert!(!joining_receipt.queue_is_live());
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a_task)
        .await
        .expect("server A join-boundary cleanup task remains bounded")
        .expect("server A join-boundary cleanup task joins")
        .expect("server A join-boundary actor joins structurally");
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(composition.snapshot(), session_before_join);
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 9);
    assert_eq!(owner_executor.snapshot(), owner_before_join);
    assert_eq!(owner_executor.target().snapshot(), target_before_join);
    assert_eq!(composition.current_actor_snapshot(), current_b_before_join);
    assert_eq!(composition.current_routes(), current_routes_before_join);
    assert_eq!(composition.retired_inbound_route(), None);

    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
            generation
        }) if generation == started_b.generation
    ));
    let mut owner_frames = Vec::new();
    loop {
        let admission_complete = match composition.drive_recovery_one() {
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted) => false,
            ClientRecoveryDrive::AdmissionCompleted { generation } => {
                assert_eq!(generation, started_b.generation);
                true
            }
            other => panic!("owned B recovery made no exact progress: {other:?}"),
        };
        assert_eq!(
            composition
                .try_flush_current(SimTime::ZERO)
                .expect("owned B replay flush remains live"),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
        );
        let replayed =
            tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
                .await
                .expect("owned B replay receive is bounded")
                .expect("server B remains live")
                .expect("server B receives one exact replay frame");
        let BoundInbound::Session(received) = take_actor_inbound(replayed) else {
            panic!("owned B replay was misclassified as leg control");
        };
        let event = owner_active_b
            .accept_frame(received)
            .expect("exact owner B provenance accepts the replay frame");
        let outputs = owner_executor
            .apply_event(event)
            .expect("owner reducer and live Target accept B replay");
        owner_frames.extend(drain_owner_target_outputs(&mut owner_executor, outputs));
        tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    match composition
                        .try_flush_current(SimTime::ZERO)
                        .expect("owned B replay delivery tail remains live")
                    {
                        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle => break,
                        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted
                        | crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            })
            .await
            .expect("owned B replay delivery completes within the local bound");
        if admission_complete {
            break;
        }
    }
    let session_before_owner_recovery = composition.snapshot();
    let owner_before_owner_recovery = owner_executor.snapshot();
    let target_before_owner_recovery = owner_executor.target().snapshot();
    let (owner_recovery_records, owner_recovery_effects) =
        apply_real_queued_owner_recovery_state(&mut composition, &mut server_b, 1).await;
    assert_eq!(owner_recovery_records, 1);
    assert!(owner_recovery_effects.is_empty());
    assert_eq!(composition.snapshot(), session_before_owner_recovery);
    assert_eq!(owner_executor.snapshot(), owner_before_owner_recovery);
    assert_eq!(
        owner_executor.target().snapshot(),
        target_before_owner_recovery
    );
    let target_after_replay = owner_executor.target().snapshot();
    assert_eq!(target_after_replay.live_flows, 1);
    assert_eq!(
        target_after_replay.write_accepted_bytes,
        target_before.write_accepted_bytes + 9,
    );
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 9);
    assert_eq!(owner_frames.len(), 1);
    assert!(matches!(
        owner_frames[0].record(),
        Record::Ack {
            flow_id,
            direction: Direction::ClientToTarget,
            next_accepted,
            final_accepted: false,
        } if *flow_id == local_flows[0].flow_id() && *next_accepted == ByteOffset::new(9)
    ));

    let session_before_stale_b = composition.snapshot();
    let owner_before_stale_b = owner_executor.snapshot();
    let current_before_stale_b = composition.current_actor_snapshot();
    for (index, frame) in [
        Frame::try_new(
            generation_a,
            Record::Data {
                flow_id: local_flows[0].flow_id(),
                direction: Direction::TargetToClient,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"stale-on-b"),
            },
        )
        .expect("old-generation DATA is valid wire input on B"),
        Frame::try_new(
            generation_a,
            Record::Ack {
                flow_id: local_flows[0].flow_id(),
                direction: Direction::ClientToTarget,
                next_accepted: ByteOffset::new(9),
                final_accepted: false,
            },
        )
        .expect("old-generation application ACK is valid wire input on B"),
        Frame::try_new(
            generation_a,
            Record::Close {
                flow_id: local_flows[0].flow_id(),
                direction: Direction::TargetToClient,
                final_offset: ByteOffset::new(0),
            },
        )
        .expect("old-generation CLOSE is valid wire input on B"),
        Frame::try_new(
            generation_a,
            Record::Reset {
                flow_id: local_flows[0].flow_id(),
                reason: crate::resumable::ResetReason::TargetFailure,
            },
        )
        .expect("old-generation RESET is valid wire input on B"),
    ]
    .into_iter()
    .enumerate()
    {
        assert_real_current_session_frame_is_reducer_rejected(
            &mut composition,
            &mut server_b,
            frame,
        )
        .await;
        assert_eq!(composition.snapshot(), session_before_stale_b);
        assert_eq!(owner_executor.snapshot(), owner_before_stale_b);
        assert_eq!(owner_executor.target().snapshot(), target_after_replay);
        assert_actor_snapshot_stable_except_validated_headers(
            composition.current_actor_snapshot(),
            current_before_stale_b,
            index + 1,
        );
    }

    for frame in owner_frames {
        server_b
            .queue
            .as_mut()
            .expect("server B retains its sole FIFO")
            .push(frame)
            .expect("server B queues the exact Target-backed ACK");
    }
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("server B application-ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let recovered = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnCurrentInboundDrive::Applied {
                    observation: Some(observation),
                    ..
                } => break observation,
                QuinnCurrentInboundDrive::Applied {
                    observation: None, ..
                } => {}
                _ => panic!("current B rejected its exact Target-backed ACK"),
            }
        }
    })
    .await
    .expect("current B applies its first positive application ACK");
    assert!(matches!(
        recovered,
        ClientInboundObservation::ReplacementRecovered {
            flow_id,
            generation,
            previous_accepted,
            next_accepted,
            released_bytes: 9,
            ..
        } if flow_id == local_flows[0].flow_id()
            && generation == started_b.generation
            && previous_accepted == ByteOffset::new(0)
            && next_accepted == ByteOffset::new(9)
    ));
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 0);

    let (client_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server_b.join())
    })
    .await
    .expect("R6c current B cleanup remains bounded");
    let client_joined = client_joined.expect("composition current B joins cleanly");
    let server_b_joined = server_b_joined.expect("server B joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        flow,
        driver,
        tcp_factory,
        local_flows,
        owner_executor,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("R6c listener cleanup is bounded")
        .expect("R6c actor pair releases the shared listener");
}

#[tokio::test(flavor = "current_thread")]
async fn real_retired_a_probe_ack_cannot_consume_armed_current_b_probe() {
    let RealAcceptedReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        accepted_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        owner_executor,
        owner_active_a,
        owner_active_b,
        standby_slot,
    } = real_accepted_replacement_fixture(2, 0).await;
    assert!(local_flows.is_empty());
    let generation_a = composition.snapshot().session.generation();
    let started_b = composition
        .begin_registered_replacement(accepted_b, client_b)
        .expect("composition atomically installs B and retires A");
    assert_eq!(started_b.generation.get(), generation_a.get() + 1);
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
            generation
        }) if generation == started_b.generation
    ));
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::AdmissionCompleted { generation }
            if generation == started_b.generation
    ));

    let before_probe = composition.snapshot();
    let owner_before_probe = owner_executor.snapshot();
    let target_before_probe = owner_executor.target().snapshot();
    let routes_before_probe = (
        composition.current_routes(),
        composition.retired_inbound_route(),
    );
    let queued = composition
        .begin_current_probe()
        .expect("current B queues one exact liveness probe");
    assert_eq!(queued.generation(), started_b.generation);
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("current B probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound_probe =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("current B probe receive is bounded")
            .expect("server B remains live")
            .expect("server B receives the exact current probe");
    let BoundInbound::LegControl(inbound_probe) = take_actor_inbound(inbound_probe) else {
        panic!("current B probe was misclassified as session data");
    };
    let (_seal, probe) = inbound_probe.into_parts();
    let LegControlRecord::LegProbe {
        session_id,
        leg_epoch_nonce,
        probe_sequence,
    } = probe.record().clone()
    else {
        panic!("current B liveness emitted a non-probe control record");
    };
    assert_eq!(probe.leg_generation(), started_b.generation);
    assert_eq!(queued.probe_sequence(), probe_sequence);
    let timeout = Duration::from_millis(50);
    let deadline = SimTime::ZERO
        .checked_add(timeout)
        .expect("bounded current B probe deadline");
    let token = tokio::time::timeout(TEST_TIMEOUT, async {
            let mut queued = queued;
            loop {
                match composition.arm_current_probe(queued, SimTime::ZERO, timeout) {
                    Ok(token) => break token,
                    Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::NotDelivered(returned)) => {
                        queued = returned;
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("current B probe failed to arm: {error:?}"),
                }
            }
        })
        .await
        .expect("current B probe arms only after physical delivery");
    let current_b_armed = composition.current_actor_snapshot();

    let exact_ack = || {
        LegControlFrame::try_new(
            started_b.generation,
            LegControlRecord::LegProbeAck {
                session_id,
                leg_epoch_nonce,
                probe_sequence,
            },
            STANDBY_CONTROL_V1,
        )
        .expect("exact current B probe ACK is valid wire input")
    };
    let late_a_receipt = server_a
        .enqueue_ordered_leg_control(exact_ack(), STANDBY_CONTROL_V1)
        .expect("retired A queues a byte-identical B probe ACK");
    assert!(!late_a_receipt.was_delivered());
    assert_eq!(
        server_a
            .try_flush(SimTime::ZERO)
            .expect("retired A probe-ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let rejected = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_retired() {
                QuinnRetiredInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => break outcome,
            }
        }
    })
    .await
    .expect("retired A probe ACK is classified within the local bound");
    assert!(matches!(rejected, QuinnRetiredInboundDrive::Rejected));
    wait_actor_ordered_delivery(&mut server_a).await;
    assert!(late_a_receipt.was_delivered());
    let retired_after_rejection = composition
        .retired_actor_snapshot()
        .expect("retired A remains live after the inert rejection");
    assert_eq!(retired_after_rejection.queued_inbound_messages(), 0);
    assert_eq!(retired_after_rejection.inbound_owned_bytes(), 0);
    assert_eq!(retired_after_rejection.composition_owned_messages(), 0);
    assert_eq!(retired_after_rejection.reserved_delivery_acks(), 0);
    assert_eq!(composition.snapshot(), before_probe);
    assert_eq!(owner_executor.snapshot(), owner_before_probe);
    assert_eq!(owner_executor.target().snapshot(), target_before_probe);
    assert_eq!(
        (
            composition.current_routes(),
            composition.retired_inbound_route(),
        ),
        routes_before_probe,
    );
    assert_eq!(composition.current_actor_snapshot(), current_b_armed);

    let exact_b_receipt = server_b
        .enqueue_ordered_leg_control(exact_ack(), STANDBY_CONTROL_V1)
        .expect("current B queues the same exact probe ACK");
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("current B probe-ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let acked = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => break outcome,
            }
        }
    })
    .await
    .expect("current B exact ACK is classified within the local bound");
    assert!(matches!(
        acked,
        QuinnCurrentInboundDrive::ProbeAcked {
            generation,
            probe_sequence: observed,
        } if generation == started_b.generation && observed == probe_sequence
    ));
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(exact_b_receipt.was_delivered());
    assert!(matches!(
        composition.try_expire_current_probe(token, deadline),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeExpiry::ProbeAcked {
            deadline: returned,
            ..
        } if returned.instant() == deadline,
    ));
    assert_eq!(composition.snapshot(), before_probe);
    assert_eq!(owner_executor.snapshot(), owner_before_probe);
    assert_eq!(owner_executor.target().snapshot(), target_before_probe);

    let server_a_task = tokio::spawn(async move { server_a.join().await });
    assert_eq!(
        composition
            .begin_retired_join()
            .expect("retired A starts structural cleanup after B ACK"),
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinStartDisposition::Started,
    );
    let cleared_a = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_finish_retired_join()
                .expect("retired A structural cleanup remains live")
            {
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending => {
                    tokio::task::yield_now().await;
                }
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Cleared(
                    cleared,
                ) => break cleared,
            }
        }
    })
    .await
    .expect("retired A structural cleanup remains bounded");
    assert_eq!(cleared_a.generation(), generation_a);
    assert_eq!(cleared_a.owned_messages(), 0);
    assert_eq!(cleared_a.owned_bytes(), 0);
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a_task)
        .await
        .expect("server A cleanup task remains bounded")
        .expect("server A cleanup task joins")
        .expect("server A joins structurally");
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);

    let (client_b_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server_b.join())
    })
    .await
    .expect("current B cleanup remains bounded");
    let client_b_joined = client_b_joined.expect("current B joins structurally");
    let server_b_joined = server_b_joined.expect("server B joins structurally");
    assert_eq!(client_b_joined.owned_messages(), 0);
    assert_eq!(client_b_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        owner_active_b,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("R6c probe listener cleanup is bounded")
        .expect("R6c probe actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn real_late_a_delivered_after_b_commit_completes_only_a_receipt() {
    let RealAcceptedReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        accepted_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        owner_executor,
        owner_active_a,
        owner_active_b,
        standby_slot,
    } = real_accepted_replacement_fixture(2, 0).await;
    assert!(local_flows.is_empty());
    let generation_a = composition.snapshot().session.generation();

    // A's initial ATTACH owns actor ordinal 1. Complete one full probe at
    // ordinal 2, including its semantic ACK, so the next held A probe is
    // ordinal 3. B has independently consumed ordinals 1 and 2 for its
    // registration and ATTACH, making its first current probe ordinal 3
    // as well. Equal wire numbers must still stay actor-private.
    let first_a_queued = composition
        .begin_current_probe()
        .expect("current A queues the ordinal-aligning probe");
    assert_eq!(first_a_queued.generation(), generation_a);
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("ordinal-aligning A probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let first_a_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("ordinal-aligning A probe receive is bounded")
            .expect("server A remains live")
            .expect("server A receives the ordinal-aligning probe");
    let BoundInbound::LegControl(first_a_inbound) = take_actor_inbound(first_a_inbound) else {
        panic!("ordinal-aligning A probe was misclassified as session data");
    };
    let (_seal, first_a_probe) = first_a_inbound.into_parts();
    let LegControlRecord::LegProbe {
        session_id: first_a_session_id,
        leg_epoch_nonce: first_a_epoch_nonce,
        probe_sequence: first_a_sequence,
    } = first_a_probe.record().clone()
    else {
        panic!("ordinal-aligning A liveness emitted a non-probe record");
    };
    assert_eq!(first_a_probe.leg_generation(), generation_a);
    assert_eq!(first_a_queued.probe_sequence(), first_a_sequence);
    let first_a_timeout = Duration::from_millis(50);
    let first_a_deadline = SimTime::ZERO
        .checked_add(first_a_timeout)
        .expect("bounded ordinal-aligning A deadline");
    let first_a_token = tokio::time::timeout(TEST_TIMEOUT, async {
            let mut queued = first_a_queued;
            loop {
                match composition.arm_current_probe(queued, SimTime::ZERO, first_a_timeout) {
                    Ok(token) => break token,
                    Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::NotDelivered(returned)) => {
                        queued = returned;
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("ordinal-aligning A probe failed to arm: {error:?}"),
                }
            }
        })
        .await
        .expect("ordinal-aligning A probe arms after physical delivery");
    let first_a_ack = LegControlFrame::try_new(
        generation_a,
        LegControlRecord::LegProbeAck {
            session_id: first_a_session_id,
            leg_epoch_nonce: first_a_epoch_nonce,
            probe_sequence: first_a_sequence,
        },
        STANDBY_CONTROL_V1,
    )
    .expect("ordinal-aligning A ACK remains valid wire input");
    let first_a_ack_receipt = server_a
        .enqueue_ordered_leg_control(first_a_ack, STANDBY_CONTROL_V1)
        .expect("server A queues the ordinal-aligning ACK");
    assert_eq!(
        server_a
            .try_flush(SimTime::ZERO)
            .expect("ordinal-aligning A ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let first_a_acked = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => break outcome,
            }
        }
    })
    .await
    .expect("ordinal-aligning A ACK is classified within the local bound");
    assert!(matches!(
        first_a_acked,
        QuinnCurrentInboundDrive::ProbeAcked {
            generation,
            probe_sequence,
        } if generation == generation_a && probe_sequence == first_a_sequence
    ));
    wait_actor_ordered_delivery(&mut server_a).await;
    assert!(first_a_ack_receipt.was_delivered());
    assert!(matches!(
        composition.try_expire_current_probe(first_a_token, first_a_deadline),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeExpiry::ProbeAcked {
            deadline: returned,
            ..
        } if returned.instant() == first_a_deadline,
    ));
    let after_first_a = composition.current_actor_snapshot();
    assert_eq!(after_first_a.pending_delivery_count(), 0);
    assert_eq!(after_first_a.pending_delivery_ordinal(), None);
    assert_eq!(after_first_a.outbound_owned_messages(), 0);
    assert_eq!(after_first_a.outbound_owned_bytes(), 0);

    let delayed_a_delivered = Arc::clone(&server_a.delivery_writer_gate);
    delayed_a_delivered.arm();
    let queued_a = composition
        .begin_current_probe()
        .expect("current A queues the second exact ordered probe");
    assert_eq!(queued_a.generation(), generation_a);
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("second current A probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound_probe =
        tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("second current A probe receive is bounded")
            .expect("server A remains live")
            .expect("server A receives the second exact current probe");
    let BoundInbound::LegControl(inbound_probe) = take_actor_inbound(inbound_probe) else {
        panic!("second current A probe was misclassified as session data");
    };
    let (_seal, probe) = inbound_probe.into_parts();
    assert!(matches!(
        probe.record(),
        LegControlRecord::LegProbe { probe_sequence, .. }
            if *probe_sequence == queued_a.probe_sequence()
    ));
    tokio::time::timeout(TEST_TIMEOUT, delayed_a_delivered.wait_until_blocked())
        .await
        .expect("server A holds the exact physical DELIVERED publication");
    let held_a = composition.current_actor_snapshot();
    assert_eq!(held_a.pending_delivery_count(), 1);
    let held_a_ordinal = held_a
        .pending_delivery_ordinal()
        .expect("second A probe retains its exact actor-private ordinal");
    assert_eq!(held_a.outbound_owned_messages(), 1);
    assert!(held_a.outbound_owned_bytes() > 0);
    assert!(!composition.current_probe_writer_completed_for_test());
    let queued_a = match composition.arm_current_probe(
        queued_a,
        SimTime::ZERO,
        Duration::from_millis(50),
    ) {
        Err(
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::NotDelivered(
                queued,
            ),
        ) => queued,
        outcome => panic!("withheld A DELIVERED produced the wrong arm result: {outcome:?}"),
    };

    let started_b = composition
        .begin_registered_replacement(accepted_b, client_b)
        .expect("composition commits B while A DELIVERED remains withheld");
    assert_eq!(started_b.generation.get(), generation_a.get() + 1);
    let retired_held_a = composition
        .retired_actor_snapshot()
        .expect("A becomes retired with its actor-local receipt still pending");
    assert_eq!(retired_held_a.pending_delivery_count(), 1);
    assert_eq!(
        retired_held_a.pending_delivery_ordinal(),
        held_a.pending_delivery_ordinal()
    );
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
            generation
        }) if generation == started_b.generation
    ));
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::AdmissionCompleted { generation }
            if generation == started_b.generation
    ));

    let delayed_b_delivered = Arc::clone(&server_b.delivery_writer_gate);
    delayed_b_delivered.arm();
    let queued_b = composition
        .begin_current_probe()
        .expect("current B queues its first exact ordered probe");
    assert_eq!(queued_b.generation(), started_b.generation);
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("current B probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound_b_probe =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("current B probe receive is bounded")
            .expect("server B remains live")
            .expect("server B receives its first current probe");
    let BoundInbound::LegControl(inbound_b_probe) = take_actor_inbound(inbound_b_probe) else {
        panic!("current B probe was misclassified as session data");
    };
    let (_seal, b_probe) = inbound_b_probe.into_parts();
    let LegControlRecord::LegProbe {
        session_id: b_session_id,
        leg_epoch_nonce: b_epoch_nonce,
        probe_sequence: b_sequence,
    } = b_probe.record().clone()
    else {
        panic!("current B liveness emitted a non-probe record");
    };
    assert_eq!(b_probe.leg_generation(), started_b.generation);
    assert_eq!(queued_b.probe_sequence(), b_sequence);
    tokio::time::timeout(TEST_TIMEOUT, delayed_b_delivered.wait_until_blocked())
        .await
        .expect("server B holds its exact physical DELIVERED publication");
    let held_b = composition.current_actor_snapshot();
    assert_eq!(held_b.pending_delivery_count(), 1);
    let held_b_ordinal = held_b
        .pending_delivery_ordinal()
        .expect("first current B probe retains its actor-private ordinal");
    assert_eq!(
        held_a_ordinal, held_b_ordinal,
        "A and B must expose the deliberate equal-ordinal collision",
    );
    assert_eq!(held_b.outbound_owned_messages(), 1);
    assert!(held_b.outbound_owned_bytes() > 0);
    assert!(!composition.current_probe_writer_completed_for_test());
    let queued_b = match composition.arm_current_probe(
        queued_b,
        SimTime::ZERO,
        Duration::from_millis(50),
    ) {
        Err(
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::NotDelivered(
                queued,
            ),
        ) => queued,
        outcome => panic!("withheld B DELIVERED produced the wrong arm result: {outcome:?}"),
    };

    let session_both_held = composition.snapshot();
    let owner_both_held = owner_executor.snapshot();
    let target_both_held = owner_executor.target().snapshot();
    let routes_both_held = (
        composition.current_routes(),
        composition.retired_inbound_route(),
    );

    delayed_a_delivered.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let retired_a = composition
                .retired_actor_snapshot()
                .expect("retired A remains running until structural join");
            if retired_a.pending_delivery_count() == 0
                && retired_a.pending_delivery_ordinal().is_none()
                && retired_a.outbound_owned_messages() == 0
                && retired_a.outbound_owned_bytes() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late A DELIVERED reconciles only A's actor-private receipt");
    let b_after_a_release = composition.current_actor_snapshot();
    assert_eq!(b_after_a_release, held_b);
    assert_eq!(b_after_a_release.pending_delivery_count(), 1);
    assert_eq!(
        b_after_a_release.pending_delivery_ordinal(),
        Some(held_b_ordinal),
    );
    assert_eq!(b_after_a_release.outbound_owned_messages(), 1);
    assert!(b_after_a_release.outbound_owned_bytes() > 0);
    assert_eq!(composition.snapshot(), session_both_held);
    assert_eq!(owner_executor.snapshot(), owner_both_held);
    assert_eq!(owner_executor.target().snapshot(), target_both_held);
    assert_eq!(
        (
            composition.current_routes(),
            composition.retired_inbound_route(),
        ),
        routes_both_held,
    );
    assert!(matches!(
        composition.arm_current_probe(queued_a, SimTime::ZERO, Duration::from_millis(50),),
        Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::Stale(_)),
    ));
    let queued_b = match composition.arm_current_probe(
        queued_b,
        SimTime::ZERO,
        Duration::from_millis(50),
    ) {
        Err(
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::NotDelivered(
                queued,
            ),
        ) => queued,
        outcome => panic!("stale A token disturbed B's held receipt: {outcome:?}"),
    };
    assert_eq!(composition.current_actor_snapshot(), held_b);
    assert_eq!(composition.snapshot(), session_both_held);
    assert_eq!(owner_executor.snapshot(), owner_both_held);
    assert_eq!(owner_executor.target().snapshot(), target_both_held);
    assert_eq!(
        (
            composition.current_routes(),
            composition.retired_inbound_route(),
        ),
        routes_both_held,
    );

    delayed_b_delivered.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let current_b = composition.current_actor_snapshot();
            if current_b.pending_delivery_count() == 0
                && current_b.pending_delivery_ordinal().is_none()
                && current_b.outbound_owned_messages() == 0
                && current_b.outbound_owned_bytes() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("B DELIVERED reconciles only B's actor-private receipt");
    let retired_after_b_release = composition
        .retired_actor_snapshot()
        .expect("retired A remains live after B's independent delivery");
    assert_eq!(retired_after_b_release.pending_delivery_count(), 0);
    assert_eq!(retired_after_b_release.pending_delivery_ordinal(), None);
    assert_eq!(retired_after_b_release.outbound_owned_messages(), 0);
    assert_eq!(retired_after_b_release.outbound_owned_bytes(), 0);
    assert_eq!(composition.snapshot(), session_both_held);
    assert_eq!(owner_executor.snapshot(), owner_both_held);
    assert_eq!(owner_executor.target().snapshot(), target_both_held);
    assert_eq!(
        (
            composition.current_routes(),
            composition.retired_inbound_route(),
        ),
        routes_both_held,
    );

    let b_timeout = Duration::from_millis(50);
    let b_deadline = SimTime::ZERO
        .checked_add(b_timeout)
        .expect("bounded current B probe deadline");
    let b_token = composition
        .arm_current_probe(queued_b, SimTime::ZERO, b_timeout)
        .expect("B arms only after its own DELIVERED arrives");
    let b_ack = LegControlFrame::try_new(
        started_b.generation,
        LegControlRecord::LegProbeAck {
            session_id: b_session_id,
            leg_epoch_nonce: b_epoch_nonce,
            probe_sequence: b_sequence,
        },
        STANDBY_CONTROL_V1,
    )
    .expect("exact current B ACK remains valid wire input");
    let b_ack_receipt = server_b
        .enqueue_ordered_leg_control(b_ack, STANDBY_CONTROL_V1)
        .expect("server B queues the exact current probe ACK");
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("current B ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let b_acked = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => break outcome,
            }
        }
    })
    .await
    .expect("current B ACK is classified within the local bound");
    assert!(matches!(
        b_acked,
        QuinnCurrentInboundDrive::ProbeAcked {
            generation,
            probe_sequence,
        } if generation == started_b.generation && probe_sequence == b_sequence
    ));
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(b_ack_receipt.was_delivered());
    assert!(matches!(
        composition.try_expire_current_probe(b_token, b_deadline),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeExpiry::ProbeAcked {
            deadline: returned,
            ..
        } if returned.instant() == b_deadline,
    ));
    assert_eq!(composition.snapshot(), session_both_held);
    assert_eq!(owner_executor.snapshot(), owner_both_held);
    assert_eq!(owner_executor.target().snapshot(), target_both_held);
    assert_eq!(
        (
            composition.current_routes(),
            composition.retired_inbound_route(),
        ),
        routes_both_held,
    );
    let current_b_drained = composition.current_actor_snapshot();
    assert_eq!(current_b_drained.pending_delivery_count(), 0);
    assert_eq!(current_b_drained.pending_delivery_ordinal(), None);
    assert_eq!(current_b_drained.queued_inbound_messages(), 0);
    assert_eq!(current_b_drained.inbound_owned_bytes(), 0);
    assert_eq!(current_b_drained.composition_owned_messages(), 0);
    assert_eq!(current_b_drained.reserved_delivery_acks(), 0);
    assert_eq!(current_b_drained.outbound_owned_messages(), 0);
    assert_eq!(current_b_drained.outbound_owned_bytes(), 0);

    let server_a_task = tokio::spawn(async move { server_a.join().await });
    assert_eq!(
        composition
            .begin_retired_join()
            .expect("retired A starts structural cleanup after late DELIVERED"),
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinStartDisposition::Started,
    );
    let cleared_a = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_finish_retired_join()
                .expect("retired A structural cleanup remains live")
            {
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending => {
                    tokio::task::yield_now().await;
                }
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Cleared(
                    cleared,
                ) => break cleared,
            }
        }
    })
    .await
    .expect("retired A structural cleanup remains bounded");
    assert_eq!(cleared_a.generation(), generation_a);
    assert_eq!(cleared_a.owned_messages(), 0);
    assert_eq!(cleared_a.owned_bytes(), 0);
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a_task)
        .await
        .expect("server A cleanup task remains bounded")
        .expect("server A cleanup task joins")
        .expect("server A joins structurally");
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);

    let (client_b_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server_b.join())
    })
    .await
    .expect("late-DELIVERED B cleanup remains bounded");
    let client_b_joined = client_b_joined.expect("current B joins structurally");
    let server_b_joined = server_b_joined.expect("server B joins structurally");
    assert_eq!(client_b_joined.owned_messages(), 0);
    assert_eq!(client_b_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        owner_active_b,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("late-DELIVERED listener cleanup is bounded")
        .expect("late-DELIVERED actors release both listener slots");
}

#[tokio::test(flavor = "current_thread")]
async fn real_terminal_bound_a_prequeued_probe_drains_before_b_ack_recovery() {
    let RealRegisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut client_b,
        mut server_b,
        registered_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
        mut standby_slot,
    } = real_registered_standby_fixture(2, 1).await;
    let (flow, mut driver) = tcp_factory
        .open_flow(local_flows[0])
        .expect("the exact local flow enters the typed source seam");
    flow.try_send_uplink_with(9, || Bytes::from_static(b"post-loss"))
        .expect("one bounded unacknowledged DATA segment is reserved");
    let input = driver
        .try_recv_next_for_client_controller()
        .expect("typed source dequeue remains live")
        .expect("one exact DATA input is ready");
    let source_effects = match composition
        .apply_driver_input(input)
        .expect("running A admits the typed driver command")
    {
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Applied(effects) => {
            effects
        }
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Deferred {
            ..
        } => panic!("small exact DATA unexpectedly met reducer pressure"),
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Failed(_) => {
            panic!("small exact DATA unexpectedly failed the reducer")
        }
    };
    assert!(source_effects.iter().any(|effect| {
            matches!(
                effect,
                SessionEffect::Transmit(frame)
                    if matches!(frame.record(), Record::Data { payload, .. } if payload.as_ref() == b"post-loss")
            )
        }));
    assert_eq!(composition.snapshot().uplink_replay_extents, 1);
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 9);
    let generation_a = composition.snapshot().session.generation();
    let route_a = composition
        .current_routes()
        .expect("A remains current before its exact loss")
        .0;
    let late_a_probe = ordered_control_for_generation(generation_a, 401);
    let late_a_receipt = server_a
        .enqueue_ordered_leg_control(late_a_probe, STANDBY_CONTROL_V1)
        .expect("server A queues one ordered record before its first failure");
    assert!(!late_a_receipt.was_delivered());
    assert_eq!(
        server_a
            .try_flush(SimTime::ZERO)
            .expect("pre-failure late-A probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let queued_late_a = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition.current_actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.inbound_owned_bytes() > 0
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 1
            {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("one ordered late-A record reaches the real actor queue before failure");
    assert_eq!(queued_late_a.queued_inbound_messages(), 1);
    assert_eq!(queued_late_a.composition_owned_messages(), 1);
    assert_eq!(queued_late_a.reserved_delivery_acks(), 1);
    let snapshot_before_loss = composition.snapshot();
    let readiness_before_loss = composition.current_actor_readiness_epoch_for_test();
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();

    server_a.request_cancel();
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a.join())
        .await
        .expect("server A close remains bounded")
        .expect("server A closes structurally");
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    let observed = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(readiness_before_loss),
    )
    .await
    .expect("A close wakes the current actor")
    .expect_err("A close publishes one exact fatal");
    let QuinnLegActorError::RootFailed(observed_cause) = observed else {
        panic!("A close lost its exact first failure: {observed:?}");
    };
    let terminal_reason = observed_cause.terminal_reason();
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending),
    ));
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("A publishes its terminal before physical cleanup");
    let applied = match composition
        .try_drive_current_terminal()
        .expect("A terminal reaches the controller")
    {
        QuinnCurrentTerminalDrive::Applied(applied) => applied,
        outcome => panic!("A loss did not apply exactly once: {outcome:?}"),
    };
    assert_eq!(applied.generation(), generation_a);
    assert_eq!(applied.reason(), terminal_reason);
    assert_eq!(
        applied.effects(),
        &[SessionEffect::ResumeGraceStarted {
            generation: generation_a,
        }],
    );
    assert_eq!(
        composition.snapshot().session.phase(),
        SessionPhase::Legless
    );
    assert_ne!(composition.snapshot(), snapshot_before_loss);
    assert_eq!(composition.current_routes(), None);
    let drained_after_first_failure = composition.current_actor_snapshot();
    assert_eq!(drained_after_first_failure.queued_inbound_messages(), 0);
    assert_eq!(drained_after_first_failure.inbound_owned_bytes(), 0);
    assert_eq!(drained_after_first_failure.composition_owned_messages(), 0);
    assert_eq!(drained_after_first_failure.reserved_delivery_acks(), 0);
    assert!(
        !late_a_receipt.was_delivered(),
        "first-failure drain cannot fabricate physical DELIVERED",
    );

    let target_before_b_attach = owner_executor.target().snapshot();
    let (accepted_b, owner_active_b) = complete_real_registered_attach(
        registered_b,
        &mut client_b,
        &mut server_b,
        &mut owner_executor,
        &mut standby_slot,
    )
    .await;
    assert_eq!(
        owner_executor.target().snapshot(),
        target_before_b_attach,
        "ordinary B attach preserves the already-open Target flow"
    );

    let started_b = composition
        .begin_registered_post_loss_replacement(accepted_b, client_b)
        .expect("registered exact-next B installs after A loss");
    assert_eq!(started_b.generation.get(), generation_a.get() + 1);
    assert_eq!(composition.snapshot().session.phase(), SessionPhase::Active);
    assert_eq!(
        composition.snapshot().session.generation(),
        started_b.generation
    );
    assert!(composition.current_routes().is_some());
    assert_eq!(composition.retired_inbound_route(), Some(route_a));
    assert!(composition.retired_actor_snapshot().is_some());
    assert_eq!(local_flows.len(), 1);

    let session_before_retired_failure = composition.snapshot();
    let owner_before_retired_failure = owner_executor.snapshot();
    let target_before_retired_failure = owner_executor.target().snapshot();
    let current_b_before_retired_failure = composition.current_actor_snapshot();
    let routes_before_retired_failure = (
        composition.current_routes(),
        composition.retired_inbound_route(),
    );
    let retired_failure = composition.try_receive_retired();
    let QuinnRetiredInboundDrive::ActorFailed(QuinnLegActorError::RootFailed(retired_cause)) =
        retired_failure
    else {
        panic!("terminal-bound A was semantically classified after first failure");
    };
    assert!(
        Arc::ptr_eq(&retired_cause, &observed_cause),
        "retired polling must preserve A's exact first-failure identity",
    );
    let retired_after_failure = composition
        .retired_actor_snapshot()
        .expect("terminal-bound A remains structurally owned before join");
    assert_eq!(retired_after_failure.queued_inbound_messages(), 0);
    assert_eq!(retired_after_failure.inbound_owned_bytes(), 0);
    assert_eq!(retired_after_failure.composition_owned_messages(), 0);
    assert_eq!(retired_after_failure.reserved_delivery_acks(), 0);
    assert!(!late_a_receipt.was_delivered());
    assert_eq!(composition.snapshot(), session_before_retired_failure);
    assert_eq!(composition.snapshot().uplink_replay_extents, 1);
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 9);
    assert_eq!(owner_executor.snapshot(), owner_before_retired_failure);
    assert_eq!(
        owner_executor.target().snapshot(),
        target_before_retired_failure,
    );
    assert_eq!(
        composition.current_actor_snapshot(),
        current_b_before_retired_failure,
    );
    assert_eq!(
        (
            composition.current_routes(),
            composition.retired_inbound_route(),
        ),
        routes_before_retired_failure,
    );

    physical_cleanup.release();
    assert_eq!(
        composition
            .begin_retired_join()
            .expect("terminal-bound A begins exact physical cleanup"),
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinStartDisposition::Started,
    );
    let early_clear = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_finish_retired_join() {
                Ok(
                    crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending,
                ) => {
                    tokio::task::yield_now().await;
                }
                Ok(
                    crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Cleared(
                        cleared,
                    ),
                ) => panic!("terminal-bound A cleared before B application ACK: {cleared:?}"),
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("terminal-bound A joins within the local bound");
    assert!(matches!(
            early_clear,
            crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinClearError::Controller(
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredControllerClearErrorKind::ReplacementApplicationAckPending,
            )
        ));
    assert_eq!(
            composition
                .begin_retired_join()
                .expect("joined A remains retained while application ACK is pending"),
            crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinStartDisposition::AlreadyJoined,
        );
    assert_eq!(composition.retired_inbound_route(), Some(route_a));

    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
            generation
        }) if generation == started_b.generation
    ));
    let mut replay_records = Vec::new();
    let mut owner_frames = Vec::new();
    loop {
        let drive = composition.drive_recovery_one();
        let admission_complete = match drive {
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted) => false,
            ClientRecoveryDrive::AdmissionCompleted { generation } => {
                assert_eq!(generation, started_b.generation);
                true
            }
            other => panic!("post-loss B recovery made no exact progress: {other:?}"),
        };
        assert_eq!(
            composition
                .try_flush_current(SimTime::ZERO)
                .expect("B replay flush remains live"),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
        );
        let replayed =
            tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
                .await
                .expect("server B replay receive is bounded")
                .expect("server B remains live")
                .expect("server B receives one exact replay frame");
        let BoundInbound::Session(received) = take_actor_inbound(replayed) else {
            panic!("B replay was misclassified as leg control");
        };
        let event = owner_active_b
            .accept_frame(received)
            .expect("exact owner B provenance accepts the replay frame");
        replay_records.push(match &event {
            SessionEvent::PeerFrame { frame, .. } => match frame.record() {
                Record::Open { .. } => "open",
                Record::Data { payload, .. } if payload.as_ref() == b"post-loss" => "data",
                record => panic!("unexpected B replay record: {record:?}"),
            },
            _ => panic!("owner B replay did not produce a peer-frame event"),
        });
        let outputs = owner_executor
            .apply_event(event)
            .expect("owner reducer and Target accept the replay event");
        owner_frames.extend(drain_owner_target_outputs(&mut owner_executor, outputs));

        tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    match composition
                        .try_flush_current(SimTime::ZERO)
                        .expect("B replay delivery tail remains live")
                    {
                        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle => break,
                        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted
                        | crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            })
            .await
            .expect("B replay delivery tail completes within the local bound");
        if admission_complete {
            break;
        }
    }
    assert_eq!(
        replay_records,
        ["data"],
        "the already-open flow replays DATA without reopening Target"
    );
    assert_eq!(owner_frames.len(), 1);
    assert!(matches!(
        owner_frames[0].record(),
        Record::Ack {
            flow_id,
            direction: Direction::ClientToTarget,
            next_accepted,
            final_accepted: false,
        } if *flow_id == local_flows[0].flow_id() && *next_accepted == ByteOffset::new(9)
    ));
    let target_after_accept = owner_executor.target().snapshot();
    assert_eq!(target_after_accept.open_attempts, 1);
    assert_eq!(target_after_accept.opened_flows, 1);
    assert_eq!(target_after_accept.duplicate_opens, 0);
    assert_eq!(target_after_accept.live_flows, 1);
    assert_eq!(target_after_accept.write_calls, 1);
    assert_eq!(target_after_accept.write_accepted_bytes, 9);

    for frame in owner_frames {
        server_b
            .queue
            .as_mut()
            .expect("server B retains its sole FIFO")
            .push(frame)
            .expect("server B FIFO admits the reducer-minted response");
    }
    let mut recovered = None;
    for _ in 0..2 {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match server_b
                    .try_flush(SimTime::ZERO)
                    .expect("server B response flush remains live")
                {
                    QuinnLegActorFlushDisposition::Submitted => break,
                    QuinnLegActorFlushDisposition::Idle
                    | QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                        tokio::task::yield_now().await;
                    }
                }
            }
        })
        .await
        .expect("one owner response is physically submitted within the local bound");
        let applied = tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition.try_receive_current() {
                    QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                    QuinnCurrentInboundDrive::Applied {
                        effects,
                        observation,
                    } => break (effects, observation),
                    QuinnCurrentInboundDrive::PolicyBlocked => {
                        panic!("client B remained policy-blocked after recovery admission")
                    }
                    QuinnCurrentInboundDrive::ProbeAcked { .. } => {
                        panic!("client B misclassified a session response as a probe ACK")
                    }
                    QuinnCurrentInboundDrive::MissingCurrent => {
                        panic!("client B current authority disappeared")
                    }
                    QuinnCurrentInboundDrive::Rejected => {
                        panic!("client B rejected an exact owner response")
                    }
                    QuinnCurrentInboundDrive::ReducerRejected(_) => {
                        panic!("client reducer rejected an exact owner response")
                    }
                    QuinnCurrentInboundDrive::ActorFailed(_) => {
                        panic!("client B actor failed while receiving owner response")
                    }
                    QuinnCurrentInboundDrive::CompositionInvariant => {
                        panic!("client B composition identity diverged")
                    }
                    QuinnCurrentInboundDrive::SessionShuttingDown => {
                        panic!("client B session began shutting down")
                    }
                }
            }
        })
        .await
        .expect("client B applies one owner response within the local bound");
        if let Some(observation) = applied.1 {
            assert!(recovered.replace(observation).is_none());
        }
    }
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(matches!(
        recovered,
        Some(ClientInboundObservation::ReplacementRecovered {
            arrived_at: _,
            session_id: _,
            flow_id,
            generation,
            direction: Direction::ClientToTarget,
            previous_accepted,
            next_accepted,
            released_bytes: 9,
            final_accepted: false,
            ..
        }) if flow_id == local_flows[0].flow_id()
            && generation == started_b.generation
            && previous_accepted == ByteOffset::new(0)
            && next_accepted == ByteOffset::new(9)
    ));
    assert_eq!(composition.snapshot().uplink_replay_extents, 0);
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 0);

    let snapshot_b_before_clear = composition.snapshot();
    let routes_b_before_clear = composition.current_routes();
    let target_before_clear = owner_executor.target().snapshot();
    let cleared_a = match composition
        .try_finish_retired_join()
        .expect("positive application ACK unlocks exact joined-A clear")
    {
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Cleared(cleared) => {
            cleared
        }
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending => {
            panic!("joined A unexpectedly returned to pending")
        }
    };
    assert_eq!(cleared_a.generation(), generation_a);
    assert_eq!(cleared_a.owned_messages(), 0);
    assert_eq!(cleared_a.owned_bytes(), 0);
    assert!(cleared_a.terminal_failure());
    assert_eq!(composition.retired_inbound_route(), None);
    assert_eq!(composition.snapshot(), snapshot_b_before_clear);
    assert_eq!(composition.current_routes(), routes_b_before_clear);
    assert_eq!(owner_executor.target().snapshot(), target_before_clear);

    let (client_b_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server_b.join())
    })
    .await
    .expect("B and its peer clean up within the local bound");
    let client_b_joined = client_b_joined.expect("current B joins cleanly");
    let server_b_joined = server_b_joined.expect("server B joins cleanly");
    assert_eq!(client_b_joined.owned_messages(), 0);
    assert_eq!(client_b_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        owner_executor,
        owner_active_a,
        owner_active_b,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown remains bounded")
        .expect("post-loss transaction releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_joins_pending_ack_retired_a_and_current_b_without_forging_recovery() {
    let RealRegisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut client_b,
        mut server_b,
        registered_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
        mut standby_slot,
    } = real_registered_standby_fixture(2, 1).await;
    let (flow, mut driver) = tcp_factory
        .open_flow(local_flows[0])
        .expect("the exact local flow enters the typed source seam");
    flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .expect("one pending replay byte is reserved");
    let input = driver
        .try_recv_next_for_client_controller()
        .expect("typed source dequeue remains live")
        .expect("one exact DATA input is ready");
    assert!(matches!(
        composition
            .apply_driver_input(input)
            .expect("running A admits the typed driver input"),
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Applied(_)
    ));
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 1);
    let route_a = composition.current_routes().unwrap().0;
    let readiness_a = composition.current_actor_readiness_epoch_for_test();
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();

    server_a.request_cancel();
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a.join())
        .await
        .expect("server A close is bounded")
        .expect("server A closes structurally");
    assert_eq!(server_a_joined.owned_messages(), 0);
    let _ = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(readiness_a),
    )
    .await
    .expect("A loss wakes the composition")
    .expect_err("A loss publishes one exact fatal");
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending)
    ));
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("A terminal publishes before physical cleanup");
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Applied(_))
    ));

    let (accepted_b, owner_active_b) = complete_real_registered_attach(
        registered_b,
        &mut client_b,
        &mut server_b,
        &mut owner_executor,
        &mut standby_slot,
    )
    .await;
    composition
        .begin_registered_post_loss_replacement(accepted_b, client_b)
        .expect("ordinary B installs after exact A loss");
    let before_runtime_preflight = composition.snapshot();
    let (returned_composition, runtime_unavailable) = std::thread::spawn(move || {
        let mut composition = composition;
        let mut shutdown = Box::pin(composition.join_current_for_shutdown());
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let result = std::future::Future::poll(shutdown.as_mut(), &mut context);
        drop(shutdown);
        let runtime_unavailable = matches!(
            result,
            std::task::Poll::Ready(Err(QuinnCurrentJoinError::RuntimeUnavailable))
        );
        (composition, runtime_unavailable)
    })
    .join()
    .expect("polling shutdown without a Tokio runtime must not panic");
    composition = returned_composition;
    assert!(
        runtime_unavailable,
        "shutdown must reject a missing runtime before phase, actor, or retired ownership mutation",
    );
    assert_eq!(composition.snapshot(), before_runtime_preflight);
    assert_eq!(composition.retired_inbound_route(), Some(route_a));
    let current_join_block = composition.arm_current_join_block_for_test();
    let server_b_task = tokio::spawn(async move {
        let joined = server_b.join().await;
        (server_b, joined)
    });
    let mut cancelled_shutdown = Box::pin(composition.join_current_for_shutdown());
    tokio::select! {
        biased;
        result = &mut cancelled_shutdown => {
            panic!("shutdown bypassed the current B join gate: {result:?}")
        }
        () = current_join_block.wait_until_blocked() => {}
    }
    drop(cancelled_shutdown);
    assert_eq!(composition.retired_inbound_route(), Some(route_a));
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 1);
    current_join_block.release();
    physical_cleanup.release();

    let client_b_joined =
        tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
            .await
            .expect("retrying cancelled shutdown joins B and retained A without a peer ACK")
            .expect("shutdown owns and joins current B plus retired A");
    let (server_b, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, server_b_task)
        .await
        .expect("server B cleanup task is bounded")
        .expect("server B cleanup task joins");
    let server_b_joined = server_b_joined.expect("server B joins structurally");
    assert_eq!(client_b_joined.owned_messages(), 0);
    assert_eq!(client_b_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    assert_eq!(composition.retired_inbound_route(), None);
    assert_eq!(
        composition.snapshot().uplink_replay_owned_bytes,
        1,
        "shutdown cleanup must not forge recovery or release replay as ACKed",
    );

    drop((
        composition,
        flow,
        driver,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        owner_active_b,
        standby_slot,
        server_b,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("shutdown releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn real_post_loss_b_without_data_replay_clears_a_without_waiting_for_ack() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
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
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 0);
    let generation_a = composition.snapshot().session.generation();
    let route_a = composition.current_routes().unwrap().0;
    let readiness = composition.current_actor_readiness_epoch_for_test();
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();

    server_a.request_cancel();
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a.join())
        .await
        .expect("server A close is bounded")
        .expect("server A closes structurally");
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    let observed = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(readiness),
    )
    .await
    .expect("client A loss wakes the composition")
    .expect_err("client A publishes one exact fatal");
    assert!(matches!(observed, QuinnLegActorError::RootFailed(_)));
    let terminal_cursor = composition.progress_cursor();
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending)
    ));
    let terminal_progress = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_actor_progress(terminal_cursor),
    )
    .await
    .expect("the terminal handoff wakes without retry polling")
    .expect("current actor terminal progress remains observable");
    assert!(matches!(
        terminal_progress,
        QuinnCompositionProgress::Current { .. },
    ));
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("A terminal publishes before its physical close tail");
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Applied(_))
    ));
    assert_eq!(
        composition.snapshot().session.phase(),
        SessionPhase::Legless
    );

    let target_before_attach = owner_executor.target().snapshot();
    let owner_active_b = complete_composition_owned_registered_attach(
        &mut composition,
        &mut server_b,
        &mut owner_executor,
        &mut standby_slot,
        SimTime::ZERO,
    )
    .await;
    assert_eq!(owner_executor.target().snapshot(), target_before_attach);
    let stale_a_cursor = composition.progress_cursor();
    let started_b = composition
        .commit_owned_standby_post_loss_replacement()
        .expect("composition-owned ordinary B installs after exact A loss");
    assert_eq!(started_b.generation.get(), generation_a.get() + 1);
    assert_eq!(started_b.generation, owner_active_b.generation());
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 0);
    assert_eq!(composition.retired_inbound_route(), Some(route_a));
    let stale_cursor_error = composition
        .wait_for_actor_progress(stale_a_cursor)
        .await
        .expect_err("an A cursor cannot wait on replacement B even at an equal epoch");
    assert_eq!(
        stale_cursor_error.source(),
        crate::owned_upstream::quinn_leg_composition::QuinnCompositionProgressSource::Current,
    );
    assert!(stale_cursor_error.topology_changed());
    assert!(stale_cursor_error.actor().is_none());

    physical_cleanup.release();
    assert_eq!(
        composition.begin_retired_join().unwrap(),
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinStartDisposition::Started,
    );
    let cleared = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_finish_retired_join()
                .expect("zero-replay A clear cannot require an impossible ACK")
            {
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending => {
                    tokio::task::yield_now().await;
                }
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Cleared(
                    cleared,
                ) => break cleared,
            }
        }
    })
    .await
    .expect("zero-replay retired A clears within the local bound");
    assert_eq!(cleared.generation(), generation_a);
    assert!(cleared.terminal_failure());
    assert_eq!(cleared.owned_messages(), 0);
    assert_eq!(cleared.owned_bytes(), 0);

    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
            generation
        }) if generation == started_b.generation
    ));
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::AdmissionCompleted { generation }
            if generation == started_b.generation
    ));
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match server_b.try_flush(SimTime::ZERO).unwrap() {
                QuinnLegActorFlushDisposition::Submitted => break,
                QuinnLegActorFlushDisposition::Idle
                | QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .expect("owner B recovery response submits within the local bound");
    let applied = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnCurrentInboundDrive::Applied { observation, .. } => break observation,
                _ => panic!("client B rejected owner recovery state"),
            }
        }
    })
    .await
    .expect("client B consumes owner recovery state");
    assert_eq!(applied, None);
    wait_actor_ordered_delivery(&mut server_b).await;

    let (client_b_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server_b.join())
    })
    .await
    .expect("B pair cleans up within the local bound");
    assert_eq!(client_b_joined.unwrap().owned_messages(), 0);
    assert_eq!(server_b_joined.unwrap().owned_messages(), 0);
    drop((
        composition,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        owner_active_b,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("zero-replay transaction releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn candidate_b_fatal_before_post_loss_install_returns_b_and_preserves_a() {
    let RealRegisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut client_b,
        mut server_b,
        registered_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
        mut standby_slot,
    } = real_registered_standby_fixture(2, 1).await;
    let generation_a = composition.snapshot().session.generation();
    let readiness_a = composition.current_actor_readiness_epoch_for_test();
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();
    server_a.request_cancel();
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a.join())
        .await
        .expect("server A close is bounded")
        .expect("server A closes structurally");
    assert_eq!(server_a_joined.owned_messages(), 0);
    let _ = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(readiness_a),
    )
    .await
    .expect("A loss wakes the composition")
    .expect_err("A loss publishes one exact fatal");
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending)
    ));
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("A terminal publishes before physical cleanup");
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Applied(_))
    ));

    let (accepted_b, _owner_active_b) = complete_real_registered_attach(
        registered_b,
        &mut client_b,
        &mut server_b,
        &mut owner_executor,
        &mut standby_slot,
    )
    .await;
    let before_candidate_failure = composition.snapshot();
    assert_eq!(
        before_candidate_failure.session.phase(),
        SessionPhase::Legless
    );
    assert_eq!(before_candidate_failure.session.generation(), generation_a);
    assert_eq!(composition.current_routes(), None);
    assert_eq!(composition.retired_inbound_route(), None);

    let mut readiness_b = client_b.readiness_epoch();
    server_b.request_cancel();
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b.join())
        .await
        .expect("server B close is bounded")
        .expect("server B closes structurally");
    assert_eq!(server_b_joined.owned_messages(), 0);
    let observed_b = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match client_b.wait_for_progress(readiness_b).await {
                Ok(next) => readiness_b = next,
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("candidate B publishes its first failure within the local bound");
    assert!(matches!(observed_b, QuinnLegActorError::RootFailed(_)));

    let failure = composition
        .begin_registered_post_loss_replacement(accepted_b, client_b)
        .expect_err("candidate fatal linearized before install");
    assert_eq!(
            failure.kind(),
            crate::owned_upstream::quinn_leg_composition::QuinnRegisteredReplacementErrorKind::ActorTransportUnavailable,
        );
    let (accepted_b, mut client_b) = failure.into_parts();
    assert_eq!(composition.snapshot(), before_candidate_failure);
    assert_eq!(composition.current_routes(), None);
    assert_eq!(composition.retired_inbound_route(), None);
    let client_b_joined = tokio::time::timeout(TEST_TIMEOUT, client_b.join())
        .await
        .expect("returned failed B joins within the local bound")
        .expect("returned B retains structural join ownership");
    assert_eq!(client_b_joined.owned_messages(), 0);
    assert_eq!(client_b_joined.owned_bytes(), 0);
    assert!(client_b_joined.terminal_failure().is_some());
    drop(accepted_b);

    physical_cleanup.release();
    let client_a_joined =
        tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
            .await
            .expect("A physical cleanup is bounded")
            .expect("A still owns the composition current actor");
    assert_eq!(client_a_joined.owned_messages(), 0);
    assert_eq!(client_a_joined.owned_bytes(), 0);
    assert!(client_a_joined.terminal_failure());
    drop((
        composition,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("fatal candidate returns both listener slots cleanly");
}

#[tokio::test(flavor = "current_thread")]
async fn controller_policy_input_transaction_linearizes_commit_and_exact_first_failure() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xb1; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xb2; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind policy transaction listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;

    let first_failure = Arc::clone(&client.first_failure);
    let policy_gate = Arc::new(super::TestPortLinearizationGate::default());
    let release_policy = policy_gate.arm();
    let blocking_policy_gate = Arc::clone(&policy_gate);
    let committed_input = Box::new([3_u8, 5, 8]);
    let committed_input_ptr = (&*committed_input) as *const [u8; 3] as usize;
    let transaction = tokio::task::spawn_blocking(move || {
        let mut clear_calls = 0;
        let committed = client.transact_controller_policy_with_input_inner(
            committed_input,
            |_runtime, committed_input| {
                clear_calls += 1;
                blocking_policy_gate.pause_if_armed();
                (committed_input, 17_u8)
            },
        );
        (client, committed, clear_calls)
    });
    tokio::time::timeout(TEST_TIMEOUT, policy_gate.wait_until_blocked())
        .await
        .expect("policy closure reaches the deterministic in-lock gate");
    assert!(matches!(
        first_failure.outcome.try_lock(),
        Err(std::sync::TryLockError::WouldBlock),
    ));
    release_policy.release();
    let (mut client, committed, clear_calls) = tokio::time::timeout(TEST_TIMEOUT, transaction)
        .await
        .expect("in-lock policy transaction is bounded")
        .expect("blocking policy transaction task joins");
    let (committed_input, committed_value) =
        committed.expect("clear actor admits one synchronous policy commit");
    assert_eq!(
        (&*committed_input) as *const [u8; 3] as usize,
        committed_input_ptr,
        "commit-first returns the exact input allocation after one commit",
    );
    assert_eq!(committed_value, 17);
    assert_eq!(clear_calls, 1);
    assert_eq!(client.phase, ActorOwnerPhase::Running);

    let exact_cause = first_failure
        .record(QuinnLegActorError::Protocol(
            TEST_ENQUEUE_LINEARIZATION_FATAL,
        ))
        .expect("running actor admits the exact first failure");
    let rejected_input = Box::new([13_u8, 21, 34]);
    let rejected_input_ptr = (&*rejected_input) as *const [u8; 3] as usize;
    let mut rejected_input_calls = 0;
    let returned_input = client
        .transact_controller_policy_with_input_inner(rejected_input, |_runtime, _input| {
            rejected_input_calls += 1;
        })
        .expect_err("fatal-first returns the exact unconsumed policy input");
    assert_eq!(
        (&*returned_input) as *const [u8; 3] as usize,
        rejected_input_ptr,
        "fatal-first must not reconstruct or replace the typed input",
    );
    assert_eq!(rejected_input_calls, 0);

    let mut rejected_calls = 0;
    let rejected = client
        .transact_controller_policy_inner(|_runtime| {
            rejected_calls += 1;
        })
        .expect_err("published first fatal rejects policy before its closure");
    let QuinnLegActorError::RootFailed(returned_cause) = rejected else {
        panic!("policy rejection lost its exact first fatal: {rejected:?}");
    };
    assert!(Arc::ptr_eq(&returned_cause, &exact_cause));
    assert_eq!(rejected_calls, 0);
    assert_eq!(client.phase, ActorOwnerPhase::Closing);

    server.request_cancel();
    let mut closing_calls = 0;
    assert!(matches!(
        server.transact_controller_policy_inner(|_runtime| {
            closing_calls += 1;
        }),
        Err(QuinnLegActorError::Closing),
    ));
    assert_eq!(closing_calls, 0);

    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), server.join())
    })
    .await
    .expect("policy transaction actor cleanup is bounded");
    let client_joined = client_joined.expect("fatal client joins structurally");
    let server_joined = server_joined.expect("locally cancelled server joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert!(client_joined.terminal_failure().is_some());
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    assert!(server_joined.terminal_failure().is_none());

    let mut joined_calls = 0;
    assert!(matches!(
        server.transact_controller_policy_inner(|_runtime| {
            joined_calls += 1;
        }),
        Err(QuinnLegActorError::AlreadyJoined),
    ));
    assert_eq!(joined_calls, 0);
    drop((client_joined, server_joined, client, server));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("policy listener cleanup is bounded")
        .expect("policy transaction actor releases its exact slot");
}

#[tokio::test(flavor = "current_thread")]
async fn controller_parts_input_rejects_mismatched_fifo_without_consuming_input() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xb5; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xb6; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind controller-parts identity listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;

    std::mem::swap(&mut client.queue, &mut server.queue);
    let input = Box::new([13_u8, 21, 34]);
    let input_ptr = (&*input) as *const [u8; 3] as usize;
    let gate =
        crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate::mint_for_test();
    let mut calls = 0;
    let (returned_input, error) = client
        .with_physical_controller_parts_mut_input(&gate, input, |_input, _runtime, _io, _queue| {
            calls += 1;
        })
        .expect_err("a FIFO from another physical actor must fail before the closure");
    assert_eq!(
        (&*returned_input) as *const [u8; 3] as usize,
        input_ptr,
        "identity rejection must return the exact unconsumed input allocation",
    );
    assert!(matches!(
        error,
        QuinnLegActorError::MissingPhysicalIncarnation
    ));
    assert_eq!(calls, 0, "identity rejection cannot execute the closure");

    std::mem::swap(&mut client.queue, &mut server.queue);
    client.request_cancel();
    server.request_cancel();
    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), server.join())
    })
    .await
    .expect("controller-parts identity cleanup is bounded");
    let client_joined = client_joined.expect("restored client actor joins structurally");
    let server_joined = server_joined.expect("restored server actor joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    drop((client_joined, server_joined, returned_input));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("controller-parts identity listener cleanup is bounded")
        .expect("controller-parts identity actors release the exact slot");
}

#[tokio::test(flavor = "current_thread")]
async fn controller_parts_result_failure_is_canonical_before_the_borrow_returns() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xb9; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xba; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind controller-parts result listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;

    let gate =
        crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate::mint_for_test();
    let returned = client
        .with_controller_parts_mut_result(&gate, |_io, _queue| {
            Err::<(), _>(QuinnLegActorError::Protocol(
                "injected composition controller failure",
            ))
        })
        .expect_err("the callback failure must fail the actor borrow transaction");
    let returned_cause = match returned {
        QuinnLegActorError::RootFailed(cause) => cause,
        error => panic!("callback failure was not installed canonically: {error:?}"),
    };
    assert!(matches!(
        returned_cause.as_ref(),
        QuinnLegActorError::Protocol("injected composition controller failure")
    ));
    let installed = client
        .first_failure
        .current()
        .expect("the canonical failure is visible before the borrow method returns");
    assert!(
        Arc::ptr_eq(&returned_cause, &installed),
        "the returned RootFailed cause must be the actor's exact stored first failure",
    );

    server.request_cancel();
    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), server.join())
    })
    .await
    .expect("controller-parts result cleanup is bounded");
    let client_joined = client_joined.expect("failed actor still joins structurally");
    assert!(
        client_joined.terminal_failure().is_some(),
        "the joined witness must retain the committed first failure"
    );
    let server_joined = server_joined.expect("peer actor joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    drop((returned_cause, installed, client_joined, server_joined));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("controller-parts result listener cleanup is bounded")
        .expect("controller-parts result actors release the exact slot");
}

#[tokio::test(flavor = "current_thread")]
async fn controller_parts_transfer_rejects_mismatched_fifo_without_consuming_input() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xb7; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xb8; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind controller-parts transfer identity listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;

    std::mem::swap(&mut client.queue, &mut server.queue);
    let input = Box::new([55_u8, 89, 144]);
    let input_ptr = (&*input) as *const [u8; 3] as usize;
    let gate =
        crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate::mint_for_test();
    let mut calls = 0;
    let (returned_input, error) = client
        .transact_controller_parts(&gate, input, |input, io, queue| {
            calls += 1;
            Err::<(), _>(Box::new((input, io, queue, ())))
        })
        .expect_err("a FIFO from another physical actor must fail before transfer closure");
    assert_eq!(
        (&*returned_input) as *const [u8; 3] as usize,
        input_ptr,
        "identity rejection must return the exact unconsumed input allocation",
    );
    assert!(
        error.is_none(),
        "actor identity preflight, not the transfer closure, must reject the FIFO"
    );
    assert_eq!(calls, 0, "identity rejection cannot execute the closure");

    std::mem::swap(&mut client.queue, &mut server.queue);
    client.request_cancel();
    server.request_cancel();
    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), server.join())
    })
    .await
    .expect("controller-parts transfer identity cleanup is bounded");
    let client_joined = client_joined.expect("restored client actor joins structurally");
    let server_joined = server_joined.expect("restored server actor joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    drop((client_joined, server_joined, returned_input));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("controller-parts transfer identity listener cleanup is bounded")
        .expect("controller-parts transfer identity actors release the exact slot");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_shutdown_marker_suppresses_waiting_and_late_child_failures() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xb3; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xb4; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind shutdown arbitration listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;

    let first_failure = Arc::clone(&client.first_failure);
    let shutdown_gate = Arc::new(super::TestPortLinearizationGate::default());
    let release_shutdown = shutdown_gate.arm();
    let blocking_shutdown_gate = Arc::clone(&shutdown_gate);
    let shutdown = tokio::task::spawn_blocking(move || {
        let committed = client.transact_controller_shutdown_inner(|_runtime| {
            blocking_shutdown_gate.pause_if_armed();
            Some(29_u8)
        });
        (client, committed)
    });
    tokio::time::timeout(TEST_TIMEOUT, shutdown_gate.wait_until_blocked())
        .await
        .expect("shutdown reaches the deterministic in-lock barrier");
    assert!(matches!(
        first_failure.outcome.try_lock(),
        Err(std::sync::TryLockError::WouldBlock),
    ));

    let waiting_failure_owner = Arc::clone(&first_failure);
    let mut waiting_failure = Box::pin(tokio::task::spawn_blocking(move || {
        waiting_failure_owner.record(QuinnLegActorError::PeerClosed)
    }));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut waiting_failure)
            .await
            .is_err(),
        "a child publisher waits behind the shutdown winner's mutex",
    );

    release_shutdown.release();
    let (mut client, committed) = tokio::time::timeout(TEST_TIMEOUT, shutdown)
        .await
        .expect("shutdown transaction is bounded")
        .expect("shutdown owner task joins");
    assert_eq!(
        committed.expect("open actor admits local shutdown"),
        Some(29),
    );
    assert_eq!(client.phase, ActorOwnerPhase::Closing);
    assert!(*client.cancel.borrow());
    assert!(first_failure.local_shutdown_won());
    assert!(first_failure.current().is_none());
    assert!(
        tokio::time::timeout(TEST_TIMEOUT, &mut waiting_failure)
            .await
            .expect("waiting child publisher is released")
            .expect("waiting child publisher task joins")
            .is_none(),
        "shutdown-first suppresses the already-waiting peer terminal",
    );
    assert!(
        first_failure
            .record(QuinnLegActorError::Protocol(
                "late child failure after local shutdown",
            ))
            .is_none(),
        "the same marker suppresses every later child failure class",
    );

    server.request_cancel();
    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), server.join())
    })
    .await
    .expect("shutdown-arbitration actor cleanup is bounded");
    let client_joined = client_joined.expect("shutdown-first client joins structurally");
    let server_joined = server_joined.expect("local peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert!(client_joined.terminal_failure().is_none());
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    assert!(server_joined.terminal_failure().is_none());
    drop((client_joined, server_joined, client, server));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("shutdown arbitration listener cleanup is bounded")
        .expect("shutdown arbitration listener releases its exact slot");
}

#[tokio::test(flavor = "current_thread")]
async fn real_quinn_reporter_failure_closes_handoff_before_controller_callback() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xb5; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xb6; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind terminal-handoff failure listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (accepted, owner_bootstrap) = complete_initial_actor_attach(&mut client, &mut server).await;
    wait_actor_ordered_delivery(&mut server).await;
    client
        .io
        .as_ref()
        .expect("client actor owns its exact LegIo before composition")
        .established_leg()
        .poison_terminal_transition_for_test();
    let mut composition =
        QuinnClientSessionComposition::bootstrap_initial(actor_session_config(), accepted, client)
            .expect("composition consumes the actor with a poisoned reporter transition");
    let snapshot_before = composition.snapshot();
    let routes_before = composition.current_routes();
    let counters_before = composition.current_leg_io_counters_for_test();
    let progress_epoch = composition.current_actor_readiness_epoch_for_test();

    server.request_cancel();
    let server_joined = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("terminal-handoff failure peer cleanup is bounded")
        .expect("terminal-handoff failure peer joins structurally");
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    let progress_error = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(progress_epoch),
    )
    .await
    .expect("peer close wakes the terminal-handoff failure waiter")
    .expect_err("peer close publishes a first fatal before reporter failure");
    assert!(matches!(progress_error, QuinnLegActorError::RootFailed(_)));
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending),
    ));
    assert_eq!(composition.snapshot(), snapshot_before);
    assert_eq!(composition.current_routes(), routes_before);
    assert_eq!(
        composition.current_leg_io_counters_for_test(),
        counters_before,
    );

    let handoff_error = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_drive_current_terminal() {
                Ok(QuinnCurrentTerminalDrive::Pending) => tokio::task::yield_now().await,
                Err(error) => break error,
                Ok(outcome) => {
                    panic!("closed terminal handoff produced a controller outcome: {outcome:?}")
                }
            }
        }
    })
    .await
    .expect("reporter failure closes the terminal handoff within the local bound");
    assert!(matches!(
        handoff_error,
        QuinnCurrentTerminalError::Actor(QuinnLegActorError::TerminalHandoffClosed),
    ));
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Err(QuinnCurrentTerminalError::Actor(
            QuinnLegActorError::TerminalHandoffClosed,
        )),
    ));
    assert_eq!(composition.snapshot(), snapshot_before);
    assert_eq!(composition.current_routes(), routes_before);
    assert_eq!(
        composition.current_leg_io_counters_for_test(),
        counters_before,
        "a closed terminal channel cannot enter the controller callback",
    );

    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("terminal-handoff failure listener cleanup is bounded")
        .expect("joined peer leaves the shared listener clean");
}

#[tokio::test(flavor = "current_thread")]
async fn real_quinn_recovering_b_close_applies_one_loss_and_preserves_retired_a_cleanup() {
    let RealRegisteredReplacementFixture {
        mut listener,
        mut composition,
        mut server_a,
        mut server_b,
        started_b,
        _tcp_factory,
        local_flows,
        peer_policy: _,
        _owner_executor,
        _owner_active_a,
        _standby_slot,
    } = real_registered_replacement_fixture(2, 3).await;
    assert_eq!(local_flows.len(), 3);
    assert_eq!(composition.snapshot().session.phase(), SessionPhase::Active);
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
            generation
        }) if generation == started_b.generation
    ));
    let counters_before_replay = composition
        .current_leg_io_counters_for_test()
        .expect("recovering B exposes its exact LegIo counters");
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted)
    ));
    let counters_with_queued_replay = composition
        .current_leg_io_counters_for_test()
        .expect("recovering B retains its queued replay transport");
    assert_eq!(
        counters_with_queued_replay, counters_before_replay,
        "FrameAdmitted owns one replay in the controller FIFO without claiming a physical submission",
    );
    assert_eq!(
        composition.normal_replacement_blocker(),
        Some(ClientRecoveryBeginErrorKind::NotActive),
        "B remains Recovering rather than completing admission",
    );

    let current_routes_b = composition
        .current_routes()
        .expect("recovering B retains both exact routes");
    let retired_route_a = composition
        .retired_inbound_route()
        .expect("A occupies the sole retired controller slot");
    assert_ne!(current_routes_b.0, retired_route_a);
    let retired_actor_before = composition
        .retired_actor_snapshot()
        .expect("retired A remains a running physical actor");
    let server_a_before = server_a.actor_snapshot();
    let snapshot_before_loss = composition.snapshot();
    let current_before_loss = composition.current_actor_snapshot();
    let progress_epoch = composition.current_actor_readiness_epoch_for_test();
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();

    server_b.request_cancel();
    let server_b_joined = tokio::time::timeout(TEST_TIMEOUT, server_b.join())
        .await
        .expect("real server B close is bounded")
        .expect("server B closes structurally");
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    let terminal_observation = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(progress_epoch),
    )
    .await
    .expect("real B close wakes the current actor waiter")
    .expect_err("real B close publishes one exact first fatal");
    let QuinnLegActorError::RootFailed(observed_cause) = terminal_observation else {
        panic!("real B close lost its exact first fatal: {terminal_observation:?}");
    };
    let expected_terminal_reason = observed_cause.terminal_reason();
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending),
    ));
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("B terminal publishes before its physical close tail");
    assert_eq!(composition.snapshot(), snapshot_before_loss);
    assert_eq!(
        composition
            .retired_actor_snapshot()
            .expect("retired A remains owned before logical B loss"),
        retired_actor_before,
    );

    let applied = match composition
        .try_drive_current_terminal()
        .expect("exact recovering-B terminal transaction remains live")
    {
        QuinnCurrentTerminalDrive::Applied(applied) => applied,
        outcome => panic!("recovering B did not apply one exact loss: {outcome:?}"),
    };
    assert_eq!(applied.generation(), started_b.generation);
    assert_eq!(applied.reason(), expected_terminal_reason);
    assert_eq!(
        applied.effects(),
        &[SessionEffect::ResumeGraceStarted {
            generation: started_b.generation,
        }],
    );
    assert_eq!(
        composition.snapshot().session.phase(),
        SessionPhase::Legless
    );
    assert_ne!(composition.snapshot(), snapshot_before_loss);
    assert_eq!(composition.current_routes(), None);
    assert_eq!(
        composition.current_leg_io_counters_for_test(),
        None,
        "terminal discard releases the installed recovery/FIFO surface",
    );
    let current_after_loss = composition.current_actor_snapshot();
    assert_eq!(current_after_loss.queued_inbound_messages(), 0);
    assert_eq!(current_after_loss.inbound_owned_bytes(), 0);
    assert_eq!(current_after_loss.composition_owned_messages(), 0);
    assert_eq!(current_after_loss.reserved_delivery_acks(), 0);
    assert_eq!(current_after_loss.outbound_queue_messages(), 0);
    assert_eq!(current_after_loss.outbound_queue_bytes(), 0);
    assert_eq!(current_after_loss.outbound_owned_messages(), 0);
    assert_eq!(current_after_loss.outbound_owned_bytes(), 0);
    assert_eq!(
        composition.retired_inbound_route(),
        Some(retired_route_a),
        "B loss cannot replace or clear retired A's controller route",
    );
    assert_eq!(
        composition
            .retired_actor_snapshot()
            .expect("B loss cannot consume retired A's actor"),
        retired_actor_before,
    );
    assert_eq!(server_a.actor_snapshot(), server_a_before);
    assert!(matches!(
        composition
            .try_drive_current_terminal()
            .expect("consumed B terminal remains replay-observable"),
        QuinnCurrentTerminalDrive::AlreadyApplied {
            generation,
            reason,
        } if generation == started_b.generation && reason == applied.reason()
    ));

    assert_eq!(
        composition
            .begin_retired_join()
            .expect("retired A independently starts physical cleanup"),
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinStartDisposition::Started,
    );
    let cleared_a = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_finish_retired_join()
                .expect("retired A cleanup remains live after B loss")
            {
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending => {
                    tokio::task::yield_now().await;
                }
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Cleared(
                    cleared,
                ) => break cleared,
            }
        }
    })
    .await
    .expect("retired A independently reaches joined clear");
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a.join())
        .await
        .expect("server A cleanup is bounded")
        .expect("server A joins structurally");
    assert_eq!(cleared_a.generation().get() + 1, started_b.generation.get());
    assert_eq!(cleared_a.owned_messages(), 0);
    assert_eq!(cleared_a.owned_bytes(), 0);
    assert!(!cleared_a.terminal_failure());
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(composition.retired_inbound_route(), None);
    assert_eq!(composition.retired_actor_snapshot(), None);

    physical_cleanup.release();
    let client_b_joined =
        tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
            .await
            .expect("post-loss B close tail is bounded")
            .expect("consumed-terminal B still mints its structural joined witness");
    assert_eq!(client_b_joined.owned_messages(), 0);
    assert_eq!(client_b_joined.owned_bytes(), 0);
    assert!(client_b_joined.terminal_failure());
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    assert_eq!(
        current_before_loss, current_after_loss,
        "logical recovery discard cannot fabricate actor payload ownership",
    );

    drop((
        composition,
        local_flows,
        _owner_executor,
        _owner_active_a,
        _standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("both exact physical actor slots release cleanly");
}

#[tokio::test(flavor = "current_thread")]
async fn exact_clean_join_of_retired_a_clears_only_a_while_active_b_keeps_exchanging() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            3,
        )
        .expect("three-slot listener configuration"),
    )
    .await
    .expect("bind shared actor listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("client A address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client A configuration");
    let (client_a, server_a) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor A bootstrap is bounded");
    let mut client_a = client_a.expect("publish client actor A");
    let mut server_a = server_a.expect("publish server actor A");
    let (accepted_a, owner_bootstrap_a) =
        complete_initial_actor_attach(&mut client_a, &mut server_a).await;

    let mut composition = QuinnClientSessionComposition::bootstrap_initial(
        actor_session_config(),
        accepted_a,
        client_a,
    )
    .expect("composition consumes the whole exact actor A");
    let opened = composition
        .open_local(TargetAddr::IpPort(
            "127.0.0.1:443".parse().expect("loopback target"),
        ))
        .expect("composition retains a current client leg")
        .expect("local open is accepted");
    let local_flow = opened
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::LocalFlowOpened { flow } => Some(flow),
            _ => None,
        })
        .expect("local open mints one exact flow authority");

    let client_b_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("client B address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client B configuration");
    let (client_b, server_b) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_b_config, LegId::B, limits, peer_policy),
            listener.accept(LegId::B, limits, peer_policy),
        )
    })
    .await
    .expect("actor B bootstrap is bounded");
    let mut client_b = client_b.expect("publish client actor B");
    let mut server_b = server_b.expect("publish server actor B");

    // C deliberately reuses A's logical LegId. Physical-incarnation
    // identity, rather than route/LegId, must still reject it when it is
    // temporarily placed in either composition slot below.
    let client_c_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("client C address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client C configuration");
    let (client_c, server_c) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_c_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor C bootstrap is bounded");
    let mut client_c = client_c.expect("publish client actor C");
    let mut server_c = server_c.expect("publish server actor C");
    let awaiting_registration = composition
        .begin_standby_registration(
            &mut client_b,
            crate::resumable::StandbyNonce::new([0x97; 16]).expect("nonzero standby nonce"),
            &actor_attach_credentials(),
        )
        .expect("current A authorizes registration of exact actor B");

    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("B registration flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let registration_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B registration receive is bounded")
            .expect("server B remains live")
            .expect("server B receives registration");
    let registration_inbound = take_actor_inbound(registration_inbound);
    wait_actor_ordered_delivery(&mut client_b).await;

    let (owner_supervisor, owner_active_a) = owner_bootstrap_a.into_parts();
    let candidate = owner_supervisor
        .authenticate_owner_standby(
            &owner_active_a,
            server_b
                .io
                .as_ref()
                .expect("server B owns its LegIo")
                .established_leg(),
            registration_inbound,
        )
        .expect("owner authenticates exact actor B registration");
    let mut standby_slot = OwnerStandbySlot::new();
    assert_eq!(
        standby_slot
            .admit(
                candidate,
                server_b.queue.as_mut().expect("server B owns its FIFO"),
            )
            .expect("owner B standby slot admits candidate"),
        OwnerStandbyAdmission::Installed,
    );
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("B registration acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let registration_acceptance =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B registration acceptance receive is bounded")
            .expect("client B remains live")
            .expect("client B receives registration acceptance");
    let registered_b = awaiting_registration
        .validate(take_actor_inbound(registration_acceptance))
        .expect("client validates exact actor B registration acceptance");

    let mut owner_executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(64, 1_024, 64).expect("valid owner target limits"),
        owner_supervisor,
        MemoryTarget::new(
            MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).expect("valid memory target config"),
        ),
        FlowPortConfig::new(64, 8, 8, 8).expect("valid owner flow-port config"),
    )
    .expect("construct owner target executor");
    let awaiting_attach_b = registered_b
        .begin_exact_next_attach(
            AttachNonce::new([0x98; 16]).expect("nonzero B attach nonce"),
            &actor_attach_credentials(),
        )
        .expect("registered B derives exact next attach")
        .enqueue(client_b.queue.as_mut().expect("client B owns its FIFO"))
        .expect("client B queues exact next attach");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("B attach flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let attach_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B attach receive is bounded")
            .expect("server B remains live")
            .expect("server B receives exact next attach");
    let BoundInbound::Session(received_attach_b) = take_actor_inbound(attach_inbound) else {
        panic!("registered B attach was misclassified as leg control");
    };
    wait_actor_ordered_delivery(&mut client_b).await;
    let OwnerTargetAttachPublication::Installed { pending } = owner_executor
        .accept_registered_owner_attach(
            &mut standby_slot,
            server_b
                .io
                .as_ref()
                .expect("server B retains its LegIo")
                .established_leg(),
            received_attach_b,
            server_b.queue.as_mut().expect("server B retains its FIFO"),
        )
        .expect("owner installs exact actor B attach")
    else {
        panic!("exact actor B attach unexpectedly resynchronized");
    };
    let recovery = pending
        .enqueue_acceptance(server_b.queue.as_mut().expect("server B FIFO remains live"))
        .expect("owner queues B attach acceptance");
    owner_executor
        .execute_attached_recovery(
            recovery,
            server_b.queue.as_mut().expect("server B FIFO remains live"),
        )
        .expect("owner completes bounded B attach recovery");
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("B attach acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let attach_acceptance =
        tokio::time::timeout(TEST_TIMEOUT, client_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B attach acceptance receive is bounded")
            .expect("client B remains live")
            .expect("client B receives attach acceptance");
    let accepted_b = match awaiting_attach_b
        .validate_response(take_actor_inbound(attach_acceptance))
        .expect("client validates exact actor B attach response")
    {
        RegisteredAttachResponse::Accepted(accepted) => accepted,
        RegisteredAttachResponse::GenerationStatus(_) => {
            panic!("exact actor B attach unexpectedly returned generation status")
        }
    };
    wait_actor_ordered_delivery(&mut server_b).await;

    let started_b = composition
        .begin_registered_replacement(accepted_b, client_b)
        .expect("composition atomically replaces A with exact actor B");
    let before_current_raw_readiness = composition.current_actor_readiness_epoch_for_test();
    server_b
        .queue
        .as_mut()
        .expect("server B retains its FIFO")
        .push(
            Frame::try_new(
                started_b.generation,
                Record::OpenResult {
                    flow_id: local_flow.flow_id(),
                    result: OpenResultCode::Opened,
                },
            )
            .expect("valid B OpenResult"),
        )
        .expect("server B queues the current inbound tracer");
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("server B inbound tracer flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let held_raw = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition.current_actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.inbound_owned_bytes() > 0
                && snapshot.composition_owned_messages() == 1
            {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("client B publishes one bounded raw inbound record");
    assert_ne!(
        composition.current_actor_readiness_epoch_for_test(),
        before_current_raw_readiness,
        "bounded inbound publication advances the current actor readiness epoch",
    );
    assert_eq!(held_raw.reserved_delivery_acks(), 0);
    let before_blocked = composition.snapshot();
    let leg_io_before_blocked = composition
        .current_leg_io_counters_for_test()
        .expect("controller retains current B LegIo counters");
    assert!(matches!(
        composition.try_receive_current(),
        QuinnCurrentInboundDrive::PolicyBlocked
    ));
    assert_eq!(composition.current_actor_snapshot(), held_raw);
    assert_eq!(composition.snapshot(), before_blocked);
    assert_eq!(
        composition.current_leg_io_counters_for_test(),
        Some(leg_io_before_blocked),
    );
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
            generation
        }) if generation == started_b.generation
    ));
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::AdmissionCompleted { generation }
            if generation == started_b.generation
    ));
    assert_eq!(
        composition.current_actor_snapshot(),
        held_raw,
        "bounded recovery admission cannot consume or duplicate blocked raw inbound",
    );
    let applied = composition.try_receive_current();
    let QuinnCurrentInboundDrive::Applied {
        effects,
        observation,
    } = applied
    else {
        panic!("exact B OpenResult was not applied after recovery")
    };
    assert_eq!(observation, None);
    assert!(matches!(
        effects.as_slice(),
        [SessionEffect::LocalOpenResolved { flow_id, result }]
            if *flow_id == local_flow.flow_id() && *result == OpenResultCode::Opened
    ));
    assert!(matches!(
        composition.try_receive_current(),
        QuinnCurrentInboundDrive::Idle
    ));
    let released_raw = composition.current_actor_snapshot();
    assert_eq!(released_raw.queued_inbound_messages(), 0);
    assert_eq!(released_raw.inbound_owned_bytes(), 0);
    assert_eq!(released_raw.composition_owned_messages(), 0);
    let leg_io_after_applied = composition
        .current_leg_io_counters_for_test()
        .expect("controller retains applied B LegIo counters");
    assert_eq!(
        leg_io_after_applied.inbound_messages,
        leg_io_before_blocked.inbound_messages + 1,
    );
    assert_eq!(
        leg_io_after_applied.bound_session_messages,
        leg_io_before_blocked.bound_session_messages + 1,
    );

    // The local-open tracer is also the one replay frame admitted during
    // B recovery. Drive its exact ordered physical delivery before the
    // pre-existing retired-capacity assertions resume.
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("B recovery replay flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let replayed_open =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B recovery replay receive is bounded")
            .expect("server B remains live for recovery replay")
            .expect("server B receives the exact replayed local open");
    let BoundInbound::Session(replayed_open) = take_actor_inbound(replayed_open) else {
        panic!("replayed local open was misclassified as leg control");
    };
    let replayed_open = replayed_open.into_frame_for_test();
    assert!(matches!(
        replayed_open.record(),
        Record::Open { flow_id, .. } if *flow_id == local_flow.flow_id()
    ));
    tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition
                    .try_flush_current(SimTime::ZERO)
                    .expect("B recovery delivery progress remains live")
                {
                    crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle => break,
                    crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted
                    | crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                        tokio::task::yield_now().await;
                    }
                }
            }
        })
        .await
        .expect("B recovery replay delivery completes within the test bound");

    let inbound_control = ordered_control_for_generation(started_b.generation, 100);
    let inbound_control_receipt = server_b
        .enqueue_ordered_leg_control(inbound_control, STANDBY_CONTROL_V1)
        .expect("server B queues one ordered physical inbound probe");
    assert!(!inbound_control_receipt.was_delivered());
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("server B inbound ordered probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let held_ordered = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition.current_actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 1
            {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("client B retains the ordered inbound and its DELIVERED reservation");
    let before_rejected_control = composition.snapshot();
    let counters_before_defensive_block = composition
        .current_leg_io_counters_for_test()
        .expect("controller retains current counters before defensive block");
    assert!(matches!(
        composition.force_current_raw_block_after_dequeue_for_test(),
        QuinnCurrentInboundDrive::PolicyBlocked
    ));
    assert_eq!(composition.current_actor_snapshot(), held_ordered);
    assert_eq!(composition.snapshot(), before_rejected_control);
    assert_eq!(
        composition.current_leg_io_counters_for_test(),
        Some(counters_before_defensive_block),
    );
    assert!(!inbound_control_receipt.was_delivered());
    assert!(matches!(
        composition.try_receive_current(),
        QuinnCurrentInboundDrive::Rejected
    ));
    assert_eq!(composition.snapshot(), before_rejected_control);
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(
        inbound_control_receipt.was_delivered(),
        "physical DELIVERED commits after exact classification even when active policy rejects leg control",
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition.current_actor_snapshot();
            if snapshot.queued_inbound_messages() == 0
                && snapshot.inbound_owned_bytes() == 0
                && snapshot.composition_owned_messages() == 0
                && snapshot.reserved_delivery_acks() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("current inbound permits and DELIVERED reservation release exactly");

    let generation_a = LegGeneration::new(
        started_b
            .generation
            .get()
            .checked_sub(1)
            .expect("B has an immediately preceding A generation"),
    )
    .expect("retired A generation remains nonzero");
    server_a
        .queue
        .as_mut()
        .expect("server A retains its FIFO before retired join")
        .push(
            Frame::try_new(
                generation_a,
                Record::Ack {
                    flow_id: local_flow.flow_id(),
                    direction: Direction::ClientToTarget,
                    next_accepted: ByteOffset::new(0),
                    final_accepted: false,
                },
            )
            .expect("valid exact retired-A late ACK"),
        )
        .expect("server A queues one late retired frame");
    assert_eq!(
        server_a
            .try_flush(SimTime::ZERO)
            .expect("retired A late frame flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition
                .retired_actor_snapshot()
                .expect("retired A is still running");
            if snapshot.queued_inbound_messages() == 1 && snapshot.composition_owned_messages() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retired A publishes one bounded late inbound");
    let before_retired_discard = composition.snapshot();
    assert!(matches!(
        composition.try_receive_retired(),
        QuinnRetiredInboundDrive::Retired(
            crate::owned_upstream::leg::RetiredClientInboundKind::Ack
        )
    ));
    assert_eq!(composition.snapshot(), before_retired_discard);
    assert!(matches!(
        composition.try_receive_retired(),
        QuinnRetiredInboundDrive::Idle
    ));
    let retired_released = composition
        .retired_actor_snapshot()
        .expect("retired A remains running after inert discard");
    assert_eq!(retired_released.queued_inbound_messages(), 0);
    assert_eq!(retired_released.inbound_owned_bytes(), 0);
    assert_eq!(retired_released.composition_owned_messages(), 0);
    assert_eq!(
        composition.normal_replacement_blocker(),
        Some(ClientRecoveryBeginErrorKind::RetiredCapacityExceeded),
        "retired A keeps the exact capacity slot closed before physical join",
    );
    let before_clear = composition.snapshot();
    let current_routes_before = composition
        .current_routes()
        .expect("composition retains active B routes");
    let retired_route_a = composition
        .retired_inbound_route()
        .expect("controller exposes only retired A's inbound route");

    let wrong_actor_control = ordered_control_for_generation(started_b.generation, 200);
    let wrong_actor_receipt = server_c
        .enqueue_ordered_leg_control(wrong_actor_control, STANDBY_CONTROL_V1)
        .expect("unrelated server C queues one ordered raw identity probe");
    assert!(!wrong_actor_receipt.was_delivered());
    assert_eq!(
        server_c
            .try_flush(SimTime::ZERO)
            .expect("unrelated C raw probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let held_wrong_actor = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = client_c.actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 1
            {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unrelated client C retains one raw record and ACK reservation");
    let counters_before_wrong_actor = composition
        .current_leg_io_counters_for_test()
        .expect("controller current B counters remain observable");

    assert!(composition.swap_current_with_actor_for_test(&mut client_c));
    assert!(matches!(
        composition.try_receive_current(),
        QuinnCurrentInboundDrive::CompositionInvariant
    ));
    assert!(matches!(
            composition.try_drive_current_terminal(),
            Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentTerminalError::CompositionInvariant)
        ));
    assert_eq!(composition.snapshot(), before_clear);
    assert_eq!(composition.current_actor_snapshot(), held_wrong_actor);
    assert!(!wrong_actor_receipt.was_delivered());
    assert_eq!(
        composition.current_leg_io_counters_for_test(),
        Some(counters_before_wrong_actor),
    );
    let wrong_current = composition
        .begin_retired_join()
        .expect_err("wrong current actor is rejected before cancellation");
    assert!(matches!(
            wrong_current,
            crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinClearError::CompositionInvariant
        ));
    assert!(composition.swap_current_with_actor_for_test(&mut client_c));
    assert_eq!(client_c.actor_snapshot(), held_wrong_actor);
    assert!(composition.swap_retired_with_actor_for_test(&mut client_c));
    assert!(matches!(
        composition.try_receive_retired(),
        QuinnRetiredInboundDrive::CompositionInvariant
    ));
    assert_eq!(
        composition
            .retired_actor_snapshot()
            .expect("unrelated C occupies only the retired runtime half"),
        held_wrong_actor,
    );
    assert!(!wrong_actor_receipt.was_delivered());
    let wrong_retired = composition
        .begin_retired_join()
        .expect_err("wrong retired actor is rejected before cancellation");
    assert!(matches!(
            wrong_retired,
            crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinClearError::CompositionInvariant
        ));
    let current_before_wrong_shutdown = composition.current_actor_snapshot();
    let wrong_retired_before_shutdown = composition
        .retired_actor_snapshot()
        .expect("unrelated C remains the wrong retired actor");
    let wrong_shutdown = composition
        .join_current_for_shutdown()
        .await
        .expect_err("wrong retired identity must fail before shutdown cancellation");
    assert!(matches!(
        wrong_shutdown,
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentJoinError::CompositionInvariant
    ));
    assert_eq!(
        composition.current_actor_snapshot(),
        current_before_wrong_shutdown
    );
    assert_eq!(
        composition.retired_actor_snapshot(),
        Some(wrong_retired_before_shutdown)
    );
    assert!(!wrong_actor_receipt.was_delivered());
    assert!(composition.swap_retired_with_actor_for_test(&mut client_c));
    assert_eq!(client_c.actor_snapshot(), held_wrong_actor);
    assert_eq!(composition.snapshot(), before_clear);
    assert_eq!(
        composition.normal_replacement_blocker(),
        Some(ClientRecoveryBeginErrorKind::RetiredCapacityExceeded),
    );

    let (client_c_joined, server_c_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client_c.join(), server_c.join())
    })
    .await
    .expect("unrelated actor C cleanup is bounded");
    let client_c_joined = client_c_joined.expect("client C joins cleanly");
    let server_c_joined = server_c_joined.expect("server C joins cleanly");
    assert_eq!(client_c_joined.owned_messages(), 0);
    assert_eq!(client_c_joined.owned_bytes(), 0);
    assert_eq!(server_c_joined.owned_messages(), 0);
    assert_eq!(server_c_joined.owned_bytes(), 0);
    assert!(
        !wrong_actor_receipt.was_delivered(),
        "identity rejection and C cleanup never fabricate DELIVERED",
    );

    let blocked_control = ordered_control_for_generation(started_b.generation, 101);
    let blocked_receipt = composition
        .enqueue_current_ordered_leg_control(blocked_control.clone(), STANDBY_CONTROL_V1)
        .expect("active B queues ordered control before retired join starts");
    assert!(!blocked_receipt.was_delivered());

    let retired_join_pending_control = ordered_control_for_generation(generation_a, 201);
    let retired_join_pending_receipt = server_a
        .enqueue_ordered_leg_control(retired_join_pending_control, STANDBY_CONTROL_V1)
        .expect("server A queues one late ordered record before retired join");
    assert!(!retired_join_pending_receipt.was_delivered());
    assert_eq!(
        server_a
            .try_flush(SimTime::ZERO)
            .expect("late retired-A join probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition
                .retired_actor_snapshot()
                .expect("retired A is running before join starts");
            if snapshot.queued_inbound_messages() == 1
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retired A retains one raw record at the join boundary");

    let join_block = composition
        .arm_retired_join_block_for_test()
        .expect("running retired A exposes one test-only structural join gate");
    assert_eq!(
        composition
            .begin_retired_join()
            .expect("exact retired A starts one structured join task"),
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinStartDisposition::Started,
    );
    tokio::time::timeout(TEST_TIMEOUT, join_block.wait_until_blocked())
        .await
        .expect("retired A reaches the blocked structural join seam");
    assert!(matches!(
        composition
            .try_finish_retired_join()
            .expect("blocked retired join remains observable"),
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending,
    ));
    assert!(matches!(
        composition.try_receive_retired(),
        QuinnRetiredInboundDrive::NotRunning
    ));
    assert!(
        !retired_join_pending_receipt.was_delivered(),
        "retired join drops queued raw ownership without acknowledging it",
    );
    assert_eq!(
            composition
                .begin_retired_join()
                .expect("repeated start observes the retained cleanup task"),
            crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinStartDisposition::AlreadyJoining,
            "repeated start cannot multiply retired cleanup tasks",
        );
    assert_eq!(composition.snapshot(), before_clear);
    assert_eq!(
        composition.normal_replacement_blocker(),
        Some(ClientRecoveryBeginErrorKind::RetiredCapacityExceeded),
        "an in-flight physical join cannot release retired capacity",
    );

    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("B remains driveable while A structural join is blocked"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let blocked_b_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("blocked-join B receive is bounded")
            .expect("server B remains live while A join is blocked")
            .expect("server B receives work while A join is blocked");
    let BoundInbound::LegControl(blocked_b_inbound) = take_actor_inbound(blocked_b_inbound) else {
        panic!("blocked-join B ordered control was misclassified");
    };
    let (_seal, blocked_b_inbound) = blocked_b_inbound.into_parts();
    assert_eq!(blocked_b_inbound, blocked_control);
    tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition
                    .try_flush_current(SimTime::ZERO)
                    .expect("B ordered-delivery progress remains live")
                {
                    crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle => break,
                    crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted
                    | crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                        tokio::task::yield_now().await;
                    }
                }
            }
        })
        .await
        .expect("B ordered delivery completes while A join remains blocked");
    assert!(
        blocked_receipt.was_delivered(),
        "server arrival alone is insufficient; B's exact ordered receipt must complete",
    );

    let joining_b_control = ordered_control_for_generation(started_b.generation, 202);
    let joining_b_receipt = server_b
        .enqueue_ordered_leg_control(joining_b_control, STANDBY_CONTROL_V1)
        .expect("server B queues current inbound while retired A join is blocked");
    assert!(!joining_b_receipt.was_delivered());
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("joining-boundary B inbound flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition.current_actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("active B publishes inbound while retired A join is blocked");
    let before_joining_b_receive = composition.snapshot();
    assert!(matches!(
        composition.try_receive_current(),
        QuinnCurrentInboundDrive::Rejected
    ));
    assert_eq!(composition.snapshot(), before_joining_b_receive);
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(
        joining_b_receipt.was_delivered(),
        "current-only inbound identity remains live while retired runtime is Joining",
    );
    let after_joining_b_receive = composition.current_actor_snapshot();
    assert_eq!(after_joining_b_receive.queued_inbound_messages(), 0);
    assert_eq!(after_joining_b_receive.inbound_owned_bytes(), 0);
    assert_eq!(after_joining_b_receive.composition_owned_messages(), 0);
    assert_eq!(after_joining_b_receive.reserved_delivery_acks(), 0);

    let retained_control = ordered_control_for_generation(started_b.generation, 102);
    let retained_receipt = composition
        .enqueue_current_ordered_leg_control(retained_control.clone(), STANDBY_CONTROL_V1)
        .expect("B queues an exact ordered sentinel before A clear");
    assert!(!retained_receipt.was_delivered());

    let join_cursor = composition.progress_cursor();
    assert!(matches!(
        composition
            .try_finish_retired_join()
            .expect("the final bounded scan still observes blocked retired A"),
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending,
    ));
    join_block.release();
    assert!(matches!(
        tokio::time::timeout(
            TEST_TIMEOUT,
            composition.wait_for_actor_progress(join_cursor),
        )
        .await
        .expect("the composition wakes on true retired-A JoinHandle readiness")
        .expect("the exact retired-A join completes cleanly"),
        QuinnCompositionProgress::RetiredJoin,
    ));
    let crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Cleared(cleared_a) =
        composition
            .try_finish_retired_join()
            .expect("the joined retired-A witness remains synchronously clearable")
    else {
        panic!("a completed retired-A join cannot return to Pending");
    };
    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a.join())
        .await
        .expect("server A cleanup is bounded")
        .expect("server A joins cleanly");
    assert_eq!(
        cleared_a.generation().get() + 1,
        started_b.generation.get(),
        "the joined witness clears only the immediately retired A generation",
    );
    assert_eq!(cleared_a.owned_messages(), 0);
    assert_eq!(cleared_a.owned_bytes(), 0);
    assert!(!cleared_a.terminal_failure());
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert!(!retired_join_pending_receipt.was_delivered());
    assert_eq!(composition.snapshot(), before_clear);
    assert_eq!(composition.current_routes(), Some(current_routes_before));
    assert_ne!(
        current_routes_before.0, retired_route_a,
        "active B and retired A remain distinct physical routes",
    );
    assert_eq!(composition.retired_inbound_route(), None);
    assert_eq!(composition.normal_replacement_blocker(), None);

    let stale_clear = composition
        .try_finish_retired_join()
        .expect_err("a consumed retired-A witness cannot be replayed against active B");
    assert!(matches!(
        stale_clear,
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinClearError::MissingRetired
    ));
    assert_eq!(composition.snapshot(), before_clear);
    assert_eq!(composition.current_routes(), Some(current_routes_before));

    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("the pre-clear B sentinel remains queued after A clear"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let b_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("post-clear B receive is bounded")
            .expect("server B remains live after A clear")
            .expect("server B receives the retained pre-clear sentinel");
    let BoundInbound::LegControl(b_inbound) = take_actor_inbound(b_inbound) else {
        panic!("retained B ordered control was misclassified");
    };
    let (_seal, b_inbound) = b_inbound.into_parts();
    assert_eq!(b_inbound, retained_control);
    tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition
                    .try_flush_current(SimTime::ZERO)
                    .expect("post-clear B delivery progress remains live")
                {
                    crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle => break,
                    crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted
                    | crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                        tokio::task::yield_now().await;
                    }
                }
            }
        })
        .await
        .expect("post-clear B ordered delivery completes before shutdown");
    assert!(
        retained_receipt.was_delivered(),
        "the pre-clear B ordered receipt must complete after exact A clear",
    );

    let pending_shutdown_control = ordered_control_for_generation(started_b.generation, 104);
    let pending_shutdown_receipt = server_b
        .enqueue_ordered_leg_control(pending_shutdown_control, STANDBY_CONTROL_V1)
        .expect("server B queues one ordered inbound before current shutdown");
    assert!(!pending_shutdown_receipt.was_delivered());
    assert_eq!(
        server_b
            .try_flush(SimTime::ZERO)
            .expect("pending-shutdown inbound flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition.current_actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("current B retains one bounded raw record before shutdown");
    assert!(matches!(
        composition.force_current_raw_block_after_dequeue_for_test(),
        QuinnCurrentInboundDrive::PolicyBlocked
    ));

    let current_join_block = composition.arm_current_join_block_for_test();
    let mut cancelled_current_join = Box::pin(composition.join_current_for_shutdown());
    tokio::select! {
        biased;
        () = current_join_block.wait_until_blocked() => {}
        result = &mut cancelled_current_join => {
            panic!("current B join bypassed its structural cancellation gate: {result:?}")
        }
    }
    drop(cancelled_current_join);
    let after_pending_cancel = composition.current_actor_snapshot();
    assert_eq!(after_pending_cancel.queued_inbound_messages(), 0);
    assert_eq!(after_pending_cancel.inbound_owned_bytes(), 0);
    assert_eq!(after_pending_cancel.composition_owned_messages(), 0);
    assert_eq!(after_pending_cancel.reserved_delivery_acks(), 0);
    assert!(
        !pending_shutdown_receipt.was_delivered(),
        "shutdown releases a blocked raw record without fabricating physical DELIVERED",
    );
    let rejected_after_cancelled_shutdown = crate::resumable::Frame::try_new(
        started_b.generation,
        crate::resumable::Record::Ack {
            flow_id: crate::resumable::SessionFlowId::new(3)
                .expect("nonzero shutdown-rejection flow id"),
            direction: crate::resumable::Direction::ClientToTarget,
            next_accepted: crate::resumable::ByteOffset::new(0),
            final_accepted: false,
        },
    )
    .expect("valid shutdown-rejection frame");
    let rejected_after_cancelled_shutdown = composition
        .enqueue_current_frame(rejected_after_cancelled_shutdown)
        .expect_err("cancelled shutdown closes current frame admission");
    assert!(matches!(
            rejected_after_cancelled_shutdown.kind(),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentEnqueueErrorKind::SessionShuttingDown
        ));
    drop(rejected_after_cancelled_shutdown.into_frame());
    assert!(matches!(
            composition.try_flush_current(SimTime::ZERO),
            Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushError::SessionShuttingDown)
        ));
    assert!(matches!(
            composition.enqueue_current_ordered_leg_control(
                ordered_control_for_generation(started_b.generation, 103),
                STANDBY_CONTROL_V1,
            ),
            Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentControlEnqueueError::SessionShuttingDown)
        ));
    let snapshot_after_cancelled_shutdown = composition.snapshot();
    assert!(matches!(
            composition.try_drive_current_terminal(),
            Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentTerminalError::SessionShuttingDown)
        ));
    assert_eq!(composition.snapshot(), snapshot_after_cancelled_shutdown);
    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::NotRecovering
    ));
    assert_eq!(composition.snapshot(), snapshot_after_cancelled_shutdown);
    assert_eq!(
        composition.normal_replacement_blocker(),
        Some(ClientRecoveryBeginErrorKind::NotActive)
    );
    assert!(matches!(
            composition.begin_retired_join(),
            Err(crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinClearError::SessionShuttingDown)
        ));
    assert!(matches!(
            composition.try_finish_retired_join(),
            Err(crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinClearError::SessionShuttingDown)
        ));
    assert!(matches!(
        composition.try_receive_current(),
        QuinnCurrentInboundDrive::SessionShuttingDown
    ));
    assert!(matches!(
        composition.try_receive_retired(),
        QuinnRetiredInboundDrive::SessionShuttingDown
    ));
    assert!(matches!(
            composition.begin_standby_registration(
                &mut server_b,
                crate::resumable::StandbyNonce::new([0xa1; 16])
                    .expect("nonzero shutdown standby nonce"),
                &actor_attach_credentials(),
            ),
            Err(crate::owned_upstream::quinn_leg_composition::QuinnStandbyRegistrationFailure::SessionShuttingDown)
        ));
    let crate::owned_upstream::standby::tests::AcceptedRegisteredClientTransportFixture {
        accepted,
        io: rejected_io,
        queue: rejected_queue,
        reporter: rejected_reporter,
    } = crate::owned_upstream::standby::tests::accepted_registered_client_transport_fixture(1);
    let replacement_failure = composition
        .begin_registered_replacement(accepted, server_b)
        .expect_err("shutdown composition cannot publish another current actor");
    assert_eq!(
            replacement_failure.kind(),
            crate::owned_upstream::quinn_leg_composition::QuinnRegisteredReplacementErrorKind::SessionShuttingDown
        );
    let (rejected_accepted, returned_server_b) = replacement_failure.into_parts();
    server_b = returned_server_b;
    drop((
        rejected_accepted,
        rejected_io,
        rejected_queue,
        rejected_reporter,
    ));
    assert_eq!(composition.snapshot(), snapshot_after_cancelled_shutdown);
    current_join_block.release();

    let (client_b_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server_b.join())
    })
    .await
    .expect("current B composition and peer cleanup are bounded");
    let client_b_joined = client_b_joined.expect("composition joins current B cleanly");
    let server_b_joined = server_b_joined.expect("server B joins cleanly");
    assert_eq!(client_b_joined.owned_messages(), 0);
    assert_eq!(client_b_joined.owned_bytes(), 0);
    assert!(!client_b_joined.terminal_failure());
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    assert!(!pending_shutdown_receipt.was_delivered());
    let rejected_after_joined_shutdown = crate::resumable::Frame::try_new(
        started_b.generation,
        crate::resumable::Record::Ack {
            flow_id: crate::resumable::SessionFlowId::new(4)
                .expect("nonzero joined-rejection flow id"),
            direction: crate::resumable::Direction::ClientToTarget,
            next_accepted: crate::resumable::ByteOffset::new(0),
            final_accepted: false,
        },
    )
    .expect("valid joined-rejection frame");
    let rejected_after_joined_shutdown = composition
        .enqueue_current_frame(rejected_after_joined_shutdown)
        .expect_err("joined shutdown keeps current frame admission closed");
    assert!(matches!(
            rejected_after_joined_shutdown.kind(),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentEnqueueErrorKind::SessionShuttingDown
        ));
    drop(rejected_after_joined_shutdown.into_frame());
    drop(composition);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("the composition does not leak its exact actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn real_quinn_peer_close_applies_one_current_leg_loss_before_physical_cleanup() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xa3; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xa4; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind shared actor listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actual-close actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");
    let (accepted, owner_bootstrap) = complete_initial_actor_attach(&mut client, &mut server).await;
    wait_actor_ordered_delivery(&mut server).await;
    let mut composition =
        QuinnClientSessionComposition::bootstrap_initial(actor_session_config(), accepted, client)
            .expect("composition consumes the exact current actor");
    let generation = composition.snapshot().session.generation();
    let before = composition.snapshot();
    assert_eq!(before.session.phase(), SessionPhase::Active);
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending),
    ));
    assert_eq!(composition.snapshot(), before);

    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();
    let progress_epoch = composition.current_actor_readiness_epoch_for_test();
    server.request_cancel();
    let server_joined = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("peer close owner joins within the local bound")
        .expect("peer close owner joins structurally");
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    let terminal_observation = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(progress_epoch),
    )
    .await
    .expect("real peer close wakes the current actor progress waiter")
    .expect_err("real peer close publishes the actor's first fatal");
    let QuinnLegActorError::RootFailed(observed_cause) = terminal_observation else {
        panic!("real peer close lost its exact first fatal: {terminal_observation:?}");
    };
    let expected_terminal_reason = observed_cause.terminal_reason();
    let fatal_first_shutdown =
        tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
            .await
            .expect("fatal-first shutdown rejection is synchronous")
            .expect_err("fatal-first cannot be reclassified as local shutdown");
    let QuinnCurrentJoinError::Actor(QuinnLegActorError::RootFailed(shutdown_cause)) =
        fatal_first_shutdown
    else {
        panic!("fatal-first shutdown lost the exact actor cause: {fatal_first_shutdown:?}");
    };
    assert!(Arc::ptr_eq(&shutdown_cause, &observed_cause));
    assert_eq!(composition.snapshot(), before);
    assert!(
        composition
            .open_local(TargetAddr::IpPort(
                "127.0.0.1:444"
                    .parse()
                    .expect("post-fatal rejection target"),
            ))
            .is_none(),
        "an observed actor fatal must close controller admission before terminal handoff",
    );
    assert_eq!(composition.snapshot(), before);
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending),
    ));
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("client publishes its exact terminal before physical cleanup");
    assert_eq!(
        composition.snapshot(),
        before,
        "early terminal publication alone cannot mutate the controller",
    );
    assert_eq!(
        composition
            .reject_current_terminal_before_bind_for_test()
            .expect("pre-bind rejection returns the exact actor terminal"),
        Some(()),
    );
    assert_eq!(
        composition.snapshot(),
        before,
        "pre-bind rejection must restore the terminal without controller mutation",
    );

    let applied = match composition
        .try_drive_current_terminal()
        .expect("exact current terminal transaction remains live")
    {
        QuinnCurrentTerminalDrive::Applied(applied) => applied,
        outcome => panic!("actual close did not apply one exact leg loss: {outcome:?}"),
    };
    assert_eq!(applied.generation(), generation);
    assert_eq!(applied.reason(), expected_terminal_reason);
    assert_eq!(
        applied.effects(),
        &[SessionEffect::ResumeGraceStarted { generation }],
    );
    assert_eq!(
        composition.snapshot().session.phase(),
        SessionPhase::Legless,
    );
    assert!(matches!(
        composition
            .try_drive_current_terminal()
            .expect("replayed terminal drive remains observable"),
        QuinnCurrentTerminalDrive::AlreadyApplied {
            generation: replay_generation,
            reason: replay_reason,
        } if replay_generation == generation && replay_reason == applied.reason(),
    ));
    assert_eq!(
        composition.snapshot().session.phase(),
        SessionPhase::Legless,
        "the same exact terminal cannot apply a second LegLost",
    );

    physical_cleanup.release();
    let joined = tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
        .await
        .expect("post-loss physical cleanup remains bounded")
        .expect("post-loss current actor joins structurally");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    assert!(joined.terminal_failure());
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("actual-close composition releases its actor slot");
}

#[tokio::test(flavor = "current_thread")]
async fn real_quinn_composition_wrong_route_is_first_fatal_never_acknowledges_and_joins_zero() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xa5; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xa6; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind shared actor listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("composition error fixture bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");
    let (accepted, owner_bootstrap) = complete_initial_actor_attach(&mut client, &mut server).await;
    wait_actor_ordered_delivery(&mut server).await;
    let mut composition =
        QuinnClientSessionComposition::bootstrap_initial(actor_session_config(), accepted, client)
            .expect("composition consumes the exact client actor");

    let probe =
        ordered_control_for_generation(LegGeneration::new(2).expect("initial generation"), 300);
    let receipt = server
        .enqueue_ordered_leg_control(probe, STANDBY_CONTROL_V1)
        .expect("server queues one real ordered composition error probe");
    assert!(!receipt.was_delivered());
    assert_eq!(
        server
            .try_flush(SimTime::ZERO)
            .expect("composition error probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let held_raw = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = composition.current_actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.inbound_owned_bytes() > 0
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 1
            {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real ordered probe reaches the composed actor queue");
    let supervisor_before = composition.snapshot();
    let counters_before = composition
        .current_leg_io_counters_for_test()
        .expect("controller retains its current LegIo counters");

    let first = composition.force_current_wrong_route_after_dequeue_for_test();
    let QuinnCurrentInboundDrive::ActorFailed(QuinnLegActorError::RootFailed(first_cause)) = first
    else {
        panic!("wrong-route composition transaction did not publish actor failure")
    };
    assert!(matches!(
        first_cause.as_ref(),
        QuinnLegActorError::LegIo(LegIoError::WrongRoute { .. })
    ));
    let second = composition.try_receive_current();
    let QuinnCurrentInboundDrive::ActorFailed(QuinnLegActorError::RootFailed(second_cause)) =
        second
    else {
        panic!("failed composition port did not retain its shared first cause")
    };
    assert!(Arc::ptr_eq(&first_cause, &second_cause));
    assert_eq!(composition.snapshot(), supervisor_before);
    let counters_after = composition
        .current_leg_io_counters_for_test()
        .expect("failed controller retains its current LegIo counters");
    assert_eq!(
        counters_after.inbound_messages,
        counters_before.inbound_messages + 1,
    );
    assert_eq!(
        counters_after.inbound_bytes,
        counters_before.inbound_bytes
            + u64::try_from(held_raw.inbound_owned_bytes()).expect("held raw length fits u64"),
    );
    assert_eq!(
        counters_after.wrong_route_rejections,
        counters_before.wrong_route_rejections + 1,
    );
    assert_eq!(
        counters_after.bound_messages,
        counters_before.bound_messages
    );
    assert_eq!(counters_after.bound_bytes, counters_before.bound_bytes);
    assert_eq!(
        counters_after.bound_session_messages,
        counters_before.bound_session_messages,
    );
    assert_eq!(
        counters_after.bound_leg_control_messages,
        counters_before.bound_leg_control_messages,
    );
    let released = composition.current_actor_snapshot();
    assert_eq!(released.queued_inbound_messages(), 0);
    assert_eq!(released.inbound_owned_bytes(), 0);
    assert_eq!(released.composition_owned_messages(), 0);
    assert_eq!(released.reserved_delivery_acks(), 0);
    assert!(!receipt.was_delivered());

    let generation = supervisor_before.session.generation();
    let applied = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition
                .try_drive_current_terminal()
                .expect("wrong-route terminal transaction remains live")
            {
                QuinnCurrentTerminalDrive::Pending => tokio::task::yield_now().await,
                QuinnCurrentTerminalDrive::Applied(applied) => break applied,
                outcome => {
                    panic!("wrong-route terminal did not apply one exact leg loss: {outcome:?}")
                }
            }
        }
    })
    .await
    .expect("wrong-route terminal handoff is bounded");
    assert_eq!(applied.generation(), generation);
    assert_eq!(applied.reason(), LegTransportTerminalReason::FatalIo);
    assert_eq!(
        applied.effects(),
        &[SessionEffect::ResumeGraceStarted { generation }],
    );
    assert_eq!(
        composition.snapshot().session.phase(),
        SessionPhase::Legless
    );
    assert!(!receipt.was_delivered());

    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server.join())
    })
    .await
    .expect("failed composition and peer join are bounded");
    let client_joined = client_joined.expect("failed current actor joins structurally");
    let server_joined = server_joined.expect("peer actor joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert!(client_joined.terminal_failure());
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    assert!(!receipt.was_delivered());
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("semantic composition fatal does not poison listener cleanup");
}

#[tokio::test]
async fn readiness_wait_registered_before_fatal_cancel_cannot_hang() {
    let readiness = super::Readiness::new();
    let previous = readiness.current();
    let (cancel_signal, cancel) = tokio::sync::watch::channel(false);
    let waiter =
        tokio::spawn(async move { readiness.wait_after_or_cancel(previous, cancel).await });

    tokio::task::yield_now().await;
    cancel_signal
        .send(true)
        .expect("registered waiter retains the cancellation receiver");
    let outcome = tokio::time::timeout(TEST_TIMEOUT, waiter)
        .await
        .expect("fatal cancellation wakes a previously registered readiness waiter")
        .expect("readiness waiter task joins");
    assert_eq!(outcome.expect("readiness lane remains live"), None);
}

#[tokio::test]
async fn readiness_publication_cannot_roll_back_or_lose_a_waiter_when_producers_finish_out_of_order()
 {
    let readiness = super::Readiness::new();
    let first = readiness.reserve_next();
    readiness.publish(first);
    let mut waiter = Box::pin(readiness.wait_after(first));
    tokio::select! {
        biased;
        result = &mut waiter => panic!("readiness advanced before the second producer: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    let second = readiness.reserve_next();
    readiness.publish(second);
    readiness.publish(first);

    assert_eq!(readiness.current(), second);
    assert_eq!(
        *readiness.sender.borrow(),
        second,
        "the published readiness epoch must never roll back behind the atomic epoch",
    );
    let observed = tokio::time::timeout(TEST_TIMEOUT, waiter)
        .await
        .expect("the out-of-order publication cannot erase the second producer wake")
        .expect("the readiness lane remains live");
    assert_eq!(observed, second);
}

#[tokio::test]
async fn readiness_cursor_never_absorbs_an_unpublished_reserved_epoch() {
    let readiness = super::Readiness::new();
    let reserved = readiness.reserve_next();
    let cursor = readiness.current();
    assert_eq!(
        cursor, 0,
        "a reservation is not observable progress until its watch publication commits",
    );

    readiness.publish(reserved);
    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, readiness.wait_after(cursor))
            .await
            .expect("publication after cursor capture cannot be lost")
            .expect("the readiness lane remains live"),
        reserved,
    );
}

#[tokio::test(flavor = "current_thread")]
async fn abort_before_first_root_poll_publishes_failure_and_wakes_owner_progress() {
    let (cancel, _cancel_rx) = tokio::sync::watch::channel(false);
    let first_failure = std::sync::Arc::new(super::ActorFirstFailure::default());
    let readiness = super::Readiness::new();
    let previous = readiness.current();
    let exit_signal = super::ActorRootExitSignal::new(
        cancel.clone(),
        std::sync::Arc::clone(&first_failure),
        readiness.clone(),
    );
    let root = std::future::pending::<
        Result<super::ActorRootOutcome, std::sync::Arc<QuinnLegActorError>>,
    >();
    let mut nursery = tokio::task::JoinSet::new();
    let abort = nursery.spawn(super::actor_root_with_exit_signal(root, exit_signal));
    abort.abort();
    let join_error = match nursery
        .join_next()
        .await
        .expect("the sole pre-poll root remains registered")
    {
        Err(error) => error,
        Ok(_outcome) => panic!("the pre-poll root was not aborted"),
    };
    assert!(join_error.is_cancelled());
    let cause = first_failure
        .current()
        .expect("the captured guard publishes a causal failure before first poll");
    assert!(
        cause
            .to_string()
            .contains(super::ROOT_EXITED_WITHOUT_OUTCOME)
    );
    assert!(*cancel.borrow());
    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, readiness.wait_after(previous))
            .await
            .expect("root abort wakes the composition-owned progress lane")
            .expect("the readiness sender remains owned"),
        previous + 1,
    );
}

#[test]
fn root_result_preserves_structural_error_after_an_earlier_semantic_cause() {
    let (cancel, _cancel_rx) = tokio::sync::watch::channel(false);
    let first_failure = std::sync::Arc::new(super::ActorFirstFailure::default());
    let semantic = first_failure
        .record(QuinnLegActorError::PeerClosed)
        .expect("open actor admits the semantic failure");
    let mut exit_signal = super::ActorRootExitSignal::new(
        cancel,
        std::sync::Arc::clone(&first_failure),
        super::Readiness::new(),
    );
    let structural = std::sync::Arc::new(QuinnLegActorError::TaskJoin(
        "fixed structural discriminator".to_owned(),
    ));

    let returned = exit_signal.publish_failure(std::sync::Arc::clone(&structural));

    assert!(std::sync::Arc::ptr_eq(&returned, &structural));
    assert!(matches!(returned.as_ref(), QuinnLegActorError::TaskJoin(_)));
    let retained_semantic = first_failure
        .current()
        .expect("ports retain the original semantic first cause");
    assert!(std::sync::Arc::ptr_eq(&retained_semantic, &semantic));
}

#[tokio::test(flavor = "current_thread")]
async fn aborted_outer_root_wakes_started_actor_progress_without_join_poll() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x6b; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x6c; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid server configuration"),
    )
    .await
    .expect("bind listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");

    let previous = client.readiness_epoch();
    let mut waiter = Box::pin(client.wait_for_progress(previous));
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(waiter.as_mut(), context))
    })
    .await;
    assert!(matches!(first_poll, std::task::Poll::Pending));
    let root_abort = client.root_abort.clone();
    root_abort.abort();
    let wait_error = tokio::time::timeout(TEST_TIMEOUT, waiter)
        .await
        .expect("outer-root exit wakes progress without join polling")
        .expect_err("unclean root exit fails the progress port");
    assert!(
        wait_error
            .to_string()
            .contains(super::ROOT_EXITED_WITHOUT_OUTCOME),
        "progress must report the root-exit discriminator: {wait_error}",
    );

    let join_error = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("aborted root join is bounded")
        .expect_err("an aborted root cannot mint Joined");
    assert!(matches!(join_error, QuinnLegActorError::TaskJoin(_)));
    let server_joined = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("peer cleanup join is bounded")
        .expect("peer root still completes structural cleanup");
    assert!(matches!(
        server_joined.terminal_reason(),
        LegTransportTerminalReason::PeerClosed
            | LegTransportTerminalReason::Reset
            | LegTransportTerminalReason::LocalCancelled,
    ));
    drop(client);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("joined server actor leaves the listener clean");
}

#[tokio::test(flavor = "current_thread")]
async fn outer_root_exit_between_progress_check_and_subscription_cannot_hang() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x71; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x72; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid server configuration"),
    )
    .await
    .expect("bind listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");

    client.progress_registration_gate.arm();
    let progress_gate = std::sync::Arc::clone(&client.progress_registration_gate);
    let root_abort = client.root_abort.clone();
    let first_failure = std::sync::Arc::clone(&client.first_failure);
    let previous = client.readiness_epoch();
    let mut waiter = Box::pin(client.wait_for_progress(previous));
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(waiter.as_mut(), context))
    })
    .await;
    assert!(matches!(first_poll, std::task::Poll::Pending));
    progress_gate.wait_until_blocked().await;

    root_abort.abort();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while first_failure.current().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("outer-root guard publishes failure without an existing receiver");
    progress_gate.release();
    let wait_error = tokio::time::timeout(TEST_TIMEOUT, waiter)
        .await
        .expect("check/register/recheck waiter is bounded")
        .expect_err("outer-root exit fails the progress waiter");
    assert!(
        wait_error
            .to_string()
            .contains(super::ROOT_EXITED_WITHOUT_OUTCOME),
        "progress must retain the structural-exit discriminator: {wait_error}",
    );

    let join_error = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("aborted root join is bounded")
        .expect_err("an aborted root cannot mint Joined");
    assert!(matches!(join_error, QuinnLegActorError::TaskJoin(_)));
    let server_joined = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("peer cleanup join is bounded")
        .expect("peer root still completes structural cleanup");
    assert!(matches!(
        server_joined.terminal_reason(),
        LegTransportTerminalReason::PeerClosed
            | LegTransportTerminalReason::Reset
            | LegTransportTerminalReason::LocalCancelled,
    ));
    drop(client);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("joined server actor leaves the listener clean");
}

#[tokio::test(flavor = "current_thread")]
async fn child_panic_completes_cleanup_but_cannot_mint_actor_or_listener_joined() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x73; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x74; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid server configuration"),
    )
    .await
    .expect("bind listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");

    server.panic_data_writer_on_next_record_for_test();
    server
        .enqueue_ordered_leg_control(ordered_control(1), STANDBY_CONTROL_V1)
        .expect("queue one record for the panic failpoint");
    assert_eq!(
        server
            .try_flush(SimTime::from_nanos(1))
            .expect("submit the panic-triggering record"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let previous = server.readiness_epoch();
    let wait_error = tokio::time::timeout(TEST_TIMEOUT, server.wait_for_progress(previous))
        .await
        .expect("child panic publishes failure within the bound")
        .expect_err("child panic cannot report ordinary progress");
    let wait_cause = match wait_error {
        QuinnLegActorError::RootFailed(cause) => cause,
        error => panic!("child panic returned the wrong progress error: {error:?}"),
    };
    assert!(matches!(
        wait_cause.as_ref(),
        QuinnLegActorError::TaskJoin(_)
    ));

    let join_error = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("structural child-failure cleanup is bounded")
        .expect_err("a child panic cannot mint Joined");
    let join_cause = match join_error {
        QuinnLegActorError::RootFailed(cause) => cause,
        error => panic!("child panic returned the wrong join error: {error:?}"),
    };
    assert!(std::sync::Arc::ptr_eq(&wait_cause, &join_cause));

    let client_joined = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("peer cleanup join is bounded")
        .expect("peer transport terminal still completes structural cleanup");
    assert!(matches!(
        client_joined.terminal_reason(),
        LegTransportTerminalReason::PeerClosed
            | LegTransportTerminalReason::Reset
            | LegTransportTerminalReason::LocalCancelled,
    ));
    drop(server);
    let shutdown_error = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown verdict is bounded")
        .expect_err("structural server-child failure poisons listener joined");
    assert!(matches!(
        shutdown_error,
        QuinnLegActorError::ListenerShutdownUnclean
    ));
}

#[test]
fn live_limits_reject_capacity_above_tokio_semaphore_max() {
    let above_semaphore_max = tokio::sync::Semaphore::MAX_PERMITS
        .checked_add(1)
        .expect("Tokio semaphore maximum leaves one invalid usize value");
    let error =
        QuinnLegActorLiveLimits::new(1, 1, 1, 1, above_semaphore_max, 1, RECORD_HEADER_BYTES)
            .expect_err("composition capacity above Tokio's maximum must fail configuration");
    assert!(matches!(error, QuinnLegActorError::InvalidConfig));
}

#[test]
fn bootstrap_configuration_rejects_zero_and_overflowing_deadlines() {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    assert!(matches!(
        ProductionQuinnLegActorClientConfig::new(
            "127.0.0.1:0".parse().expect("loopback client address"),
            "127.0.0.1:1".parse().expect("loopback server address"),
            "localhost",
            cert_dir.join("ca-cert.pem"),
            Duration::ZERO,
        ),
        Err(QuinnLegActorError::InvalidConfig),
    ));
    assert!(matches!(
        ProductionQuinnLegActorClientConfig::new(
            "127.0.0.1:0".parse().expect("loopback client address"),
            "127.0.0.1:1".parse().expect("loopback server address"),
            "localhost",
            cert_dir.join("ca-cert.pem"),
            Duration::MAX,
        ),
        Err(QuinnLegActorError::InvalidConfig),
    ));

    assert!(matches!(
        ProductionQuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
            Duration::ZERO,
        ),
        Err(QuinnLegActorError::InvalidConfig),
    ));
    assert!(matches!(
        ProductionQuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
            Duration::MAX,
        ),
        Err(QuinnLegActorError::InvalidConfig),
    ));
}

#[test]
fn explicit_bootstrap_deadline_check_rejects_expired_publication() {
    let expired = tokio::time::Instant::now();
    assert!(matches!(
        super::ensure_bootstrap_deadline(expired),
        Err(QuinnLegActorError::BootstrapTimedOut),
    ));
    let live = tokio::time::Instant::now() + Duration::from_secs(1);
    assert!(super::ensure_bootstrap_deadline(live).is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_client_bootstrap_guard_releases_endpoint_and_driver() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        "127.0.0.1:9"
            .parse()
            .expect("unused loopback server address"),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid pending-client configuration");
    let mut endpoint =
        super::build_client_endpoint(&config, limits).expect("build pending-client endpoint");
    let endpoint_driver_drain = endpoint
        .take_driver_drain_handle()
        .expect("pending bootstrap owns its endpoint driver witness");
    let mut connecting = endpoint
        .connect(config.server_addr, &config.server_name)
        .expect("begin pending client bootstrap");
    let drain = connecting
        .take_drain_handle()
        .expect("pending bootstrap owns its restricted drain witness");
    let mut cleanup = super::ClientBootstrapCleanup::new(
        tokio::runtime::Handle::current(),
        endpoint.clone(),
        endpoint_driver_drain,
    );
    cleanup
        .install_connection_drain(drain)
        .expect("install exact connection drain");
    let cleanup_done = std::sync::Arc::clone(&cleanup.failure_cleanup_done);
    assert_eq!(endpoint.open_connections(), 1);
    drop(cleanup);
    drop(connecting);

    tokio::time::timeout(TEST_TIMEOUT, async {
        while endpoint.open_connections() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled client bootstrap releases its exact endpoint entry");
    assert!(
        endpoint.wait_idle_or_driver_lost().await,
        "client cancellation releases the endpoint connection map",
    );
    assert!(
        !cleanup_done.load(std::sync::atomic::Ordering::Acquire),
        "map idle cannot masquerade as endpoint-driver/socket completion",
    );
    drop(endpoint);
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !cleanup_done.load(std::sync::atomic::Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping the final endpoint handle completes detached cleanup");
}

#[tokio::test(flavor = "current_thread")]
async fn synchronous_client_connect_error_releases_endpoint_driver_and_socket() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let probe =
        std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve one exact IPv4 client address");
    let bind_addr = probe.local_addr().expect("inspect reserved client address");
    drop(probe);
    let config = QuinnLegActorClientConfig::new(
        bind_addr,
        "[::1]:9"
            .parse()
            .expect("IPv6 remote address mismatches the IPv4 endpoint"),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid configuration reaches Quinn's synchronous connect check");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x81; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x82; 16]).expect("nonzero device principal"),
    );

    let error = tokio::time::timeout(
        TEST_TIMEOUT,
        QuinnLegActorFactory::connect(config, LegId::A, limits, peer_policy),
    )
    .await
    .expect("synchronous connect failure cleanup is bounded")
    .err()
    .expect("an IPv4 endpoint rejects an IPv6 remote address synchronously");
    assert!(matches!(error, QuinnLegActorError::Handshake(_)));

    let rebound = std::net::UdpSocket::bind(bind_addr)
        .expect("factory returns only after releasing its endpoint driver and UDP socket");
    drop(rebound);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_client_failure_waiter_leaves_exact_cleanup_running() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        "127.0.0.1:9"
            .parse()
            .expect("unused loopback server address"),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid pending-client configuration");
    let mut endpoint =
        super::build_client_endpoint(&config, limits).expect("build pending-client endpoint");
    let endpoint_driver_drain = endpoint
        .take_driver_drain_handle()
        .expect("pending bootstrap owns its endpoint driver witness");
    let mut connecting = endpoint
        .connect(config.server_addr, &config.server_name)
        .expect("begin pending client bootstrap");
    let drain = connecting
        .take_drain_handle()
        .expect("pending bootstrap owns its restricted drain witness");
    let mut cleanup = super::ClientBootstrapCleanup::new(
        tokio::runtime::Handle::current(),
        endpoint.clone(),
        endpoint_driver_drain,
    );
    cleanup
        .install_connection_drain(drain)
        .expect("install exact connection drain");
    let cleanup_gate = std::sync::Arc::clone(&cleanup.failure_cleanup_gate);
    let cleanup_done = std::sync::Arc::clone(&cleanup.failure_cleanup_done);
    cleanup_gate.arm();

    let mut finish = Box::pin(cleanup.finish_failure());
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(finish.as_mut(), context))
    })
    .await;
    assert!(matches!(first_poll, std::task::Poll::Pending));
    cleanup_gate.wait_until_blocked().await;
    drop(finish);
    drop(connecting);
    cleanup_gate.release();

    tokio::time::timeout(TEST_TIMEOUT, async {
        while endpoint.open_connections() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached failure cleanup releases its exact endpoint entry");
    assert!(
        endpoint.wait_idle_or_driver_lost().await,
        "detached failure cleanup reaches endpoint map idle",
    );
    assert!(
        !cleanup_done.load(std::sync::atomic::Ordering::Acquire),
        "retained endpoint handle keeps exact driver cleanup pending",
    );
    drop(endpoint);
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !cleanup_done.load(std::sync::atomic::Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached failure cleanup consumes the endpoint-driver witness");
    drop(cleanup);
}

#[tokio::test(flavor = "current_thread")]
async fn listener_preloads_tls_material_before_any_admission() {
    let source_cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock follows the Unix epoch")
        .as_nanos();
    let staged_dir = std::env::temp_dir().join(format!(
        "mini-vpn-quinn-listener-certs-{}-{unique}",
        std::process::id(),
    ));
    std::fs::create_dir(&staged_dir).expect("create private staged certificate directory");
    let staged_cert = staged_dir.join("server-cert.pem");
    let staged_key = staged_dir.join("server-key.pem");
    let staged_ca = staged_dir.join("ca-cert.pem");
    std::fs::copy(source_cert_dir.join("server-cert.pem"), &staged_cert)
        .expect("stage server certificate");
    std::fs::copy(source_cert_dir.join("server-key.pem"), &staged_key)
        .expect("stage server private key");
    std::fs::copy(source_cert_dir.join("ca-cert.pem"), &staged_ca)
        .expect("stage client CA certificate");

    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x6d; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x6e; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            staged_cert.clone(),
            staged_key.clone(),
            1,
        )
        .expect("valid staged listener configuration"),
    )
    .await
    .expect("bind listener while TLS files exist");
    let client_configs = (0..2)
        .map(|_| {
            QuinnLegActorClientConfig::new(
                "127.0.0.1:0".parse().expect("loopback client address"),
                listener.local_addr(),
                "localhost",
                staged_ca.clone(),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .expect("preload both client configurations while the CA exists");
    std::fs::remove_file(staged_cert).expect("remove staged certificate after bind");
    std::fs::remove_file(staged_key).expect("remove staged private key after bind");
    std::fs::remove_file(staged_ca).expect("remove staged client CA after configuration");
    std::fs::remove_dir(staged_dir).expect("remove empty staged directory");

    for (attempt, client_config) in client_configs.into_iter().enumerate() {
        let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy,),
                async {
                    listener
                        .admit_bootstrap_for_test(LegId::A, limits, peer_policy)
                        .await?
                        .start()
                        .await
                },
            )
        })
        .await
        .unwrap_or_else(|_| panic!("admission {attempt} is bounded"));
        let mut client = client.unwrap_or_else(|error| {
            panic!("client admission {attempt} uses preloaded TLS: {error}")
        });
        let mut server = server.unwrap_or_else(|error| {
            panic!("server admission {attempt} uses preloaded TLS: {error}")
        });
        client.request_cancel();
        server.request_cancel();
        let (client_join, server_join) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(client.join(), server.join())
        })
        .await
        .unwrap_or_else(|_| panic!("admission {attempt} cleanup is bounded"));
        client_join.unwrap_or_else(|error| panic!("client {attempt} joins: {error}"));
        server_join.unwrap_or_else(|error| panic!("server {attempt} joins: {error}"));
    }

    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("preloaded listener remains clean");
}

#[tokio::test(flavor = "current_thread")]
async fn fully_buffered_expired_bootstrap_cannot_publish_and_allows_retry() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x6f; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x70; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_config = ProductionQuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
        Duration::from_millis(100),
    )
    .expect("valid listener configuration with bounded bootstrap deadline");
    let mut listener = QuinnLegActorListener::bind(server_config)
        .await
        .expect("bind one-slot listener");
    let raw_client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback raw-client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid raw-client configuration");
    let raw_endpoint = super::build_client_endpoint(&raw_client_config, limits)
        .expect("build raw client endpoint");
    let (raw_connection, bootstrap) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            async {
                raw_endpoint
                    .connect(
                        raw_client_config.server_addr,
                        &raw_client_config.server_name,
                    )
                    .expect("begin raw TLS connection")
                    .await
                    .expect("raw peer completes TLS before app bootstrap")
            },
            listener.admit_bootstrap_for_test(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("raw TLS admission is bounded");
    let bootstrap = bootstrap.expect("listener returns owned bootstrap before app authentication");
    let exporter = super::export_binding(&raw_connection, peer_policy)
        .expect("raw peer derives the same exporter binding");
    let (mut confirmation_send, _confirmation_recv) = raw_connection
        .open_bi()
        .await
        .expect("raw peer opens the confirmation stream");
    let confirmation =
        super::confirmation_tag(&exporter, super::ActorRole::Client, LegId::A, peer_policy)
            .expect("raw peer mints the exact client confirmation");
    confirmation_send
        .write_all(&confirmation)
        .await
        .expect("raw peer buffers the exact confirmation");
    confirmation_send
        .finish()
        .expect("raw peer terminates the confirmation stream");
    let mut data_send = raw_connection
        .open_uni()
        .await
        .expect("raw peer opens the data stream");
    data_send
        .write_all(&super::stream_preface(
            super::STREAM_DATA,
            super::ActorRole::Client,
            LegId::A,
        ))
        .await
        .expect("raw peer buffers the data preface");
    let mut delivered_send = raw_connection
        .open_uni()
        .await
        .expect("raw peer opens the delivered stream");
    delivered_send
        .write_all(&super::stream_preface(
            super::STREAM_DELIVERED,
            super::ActorRole::Client,
            LegId::A,
        ))
        .await
        .expect("raw peer buffers the delivered preface");
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        bootstrap.deadline <= tokio::time::Instant::now(),
        "the admission-owned absolute deadline expires even before start is polled",
    );
    let timeout_error = match tokio::time::timeout(Duration::from_millis(50), bootstrap.start())
        .await
        .expect("an all-ready expired bootstrap does not receive a fresh relative timeout")
    {
        Err(error) => error,
        Ok(_actor) => panic!("fully buffered app bootstrap bypassed its absolute deadline"),
    };
    assert!(matches!(
        timeout_error,
        QuinnLegActorError::BootstrapTimedOut
    ));
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        0,
    );
    assert!(
        !listener
            .registry
            .unclean
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert_eq!(
        listener.open_connections_for_test(),
        0,
        "a failed bootstrap retains its slot until the exact QUIC connection is drained",
    );
    raw_connection.close(CLOSE_CODE, b"stalled bootstrap test cleanup");
    raw_endpoint.close(CLOSE_CODE, b"stalled bootstrap test cleanup");
    tokio::time::timeout(TEST_TIMEOUT, raw_endpoint.wait_idle())
        .await
        .expect("raw client endpoint cleanup is bounded");

    let retry_client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0"
            .parse()
            .expect("loopback retry-client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid retry-client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(retry_client_config, LegId::A, limits, peer_policy,),
            async {
                listener
                    .admit_bootstrap_for_test(LegId::A, limits, peer_policy)
                    .await?
                    .start()
                    .await
            },
        )
    })
    .await
    .expect("healthy retry bootstrap is bounded");
    let mut client = client.expect("healthy retry publishes client actor");
    let mut server = server.expect("healthy retry publishes server actor");
    client.request_cancel();
    server.request_cancel();
    let (client_join, server_join) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), server.join())
    })
    .await
    .expect("healthy retry cleanup is bounded");
    client_join.expect("healthy retry client joins");
    server_join.expect("healthy retry server joins");
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("timed-out prepublication bootstrap does not poison listener");
}

#[tokio::test(flavor = "current_thread")]
async fn address_retry_precedes_slot_reservation_and_cross_thread_drop_drains_cleanly() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x71; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x72; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_config = ProductionQuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
        TEST_TIMEOUT,
    )
    .expect("valid one-slot listener configuration");
    let mut listener = QuinnLegActorListener::bind(server_config)
        .await
        .expect("bind one-slot listener");
    let raw_client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback raw-client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid raw-client configuration");
    let raw_endpoint = super::build_client_endpoint(&raw_client_config, limits)
        .expect("build raw client endpoint");
    let raw_connecting = raw_endpoint
        .connect(
            raw_client_config.server_addr,
            &raw_client_config.server_name,
        )
        .expect("begin raw client connection");
    let raw_task = tokio::spawn(raw_connecting);

    let first = tokio::time::timeout(TEST_TIMEOUT, listener.admit(LegId::A, limits, peer_policy))
        .await
        .expect("first unvalidated Incoming is bounded")
        .expect("listener can issue address Retry");
    assert!(matches!(first, QuinnLegActorAdmission::RetrySent));
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        0,
        "an unvalidated source address cannot consume actor capacity",
    );
    assert_eq!(
        listener
            .registry
            .terminating_tasks
            .load(std::sync::atomic::Ordering::Acquire),
        0,
    );

    let admitted =
        tokio::time::timeout(TEST_TIMEOUT, listener.admit(LegId::A, limits, peer_policy))
            .await
            .expect("retried Incoming is bounded")
            .expect("validated Incoming is admitted");
    let bootstrap = match admitted {
        QuinnLegActorAdmission::RetrySent => {
            panic!("the token-bearing retry was not recognized as address validated")
        }
        QuinnLegActorAdmission::Bootstrap(bootstrap) => bootstrap,
    };
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        1,
        "only the validated bootstrap owns the sole actor slot",
    );
    std::thread::spawn(move || drop(bootstrap))
        .join()
        .expect("a non-runtime drop thread returns after registering exact cleanup");

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if listener
                .registry
                .live
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
                && listener
                    .registry
                    .terminating_tasks
                    .load(std::sync::atomic::Ordering::Acquire)
                    == 0
                && listener.open_connections_for_test() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped admitted bootstrap drains before releasing tracked capacity");
    assert!(
        !listener
            .registry
            .unclean
            .load(std::sync::atomic::Ordering::Acquire),
        "intentional prepublication rejection remains a clean listener event",
    );

    match tokio::time::timeout(TEST_TIMEOUT, raw_task)
        .await
        .expect("raw connection task resolves after server abandonment")
        .expect("raw connection task joins")
    {
        Ok(connection) => connection.close(CLOSE_CODE, b"retry test cleanup"),
        Err(_expected_server_abandonment) => {}
    }
    raw_endpoint.close(CLOSE_CODE, b"retry test cleanup");
    tokio::time::timeout(TEST_TIMEOUT, raw_endpoint.wait_idle())
        .await
        .expect("raw retry endpoint cleanup is bounded");
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("Retry and dropped bootstrap leave a clean listener");
}

#[test]
fn live_limits_reject_remaining_primitive_capacities_above_tokio_semaphore_max() {
    let above_semaphore_max = tokio::sync::Semaphore::MAX_PERMITS
        .checked_add(1)
        .expect("Tokio semaphore maximum leaves one invalid usize value");
    let cases = [
        (
            "outbound_messages",
            (above_semaphore_max, 1, 1, 1, 1, 1, RECORD_HEADER_BYTES),
        ),
        (
            "outbound_bytes",
            (1, above_semaphore_max, 1, 1, 1, 1, RECORD_HEADER_BYTES),
        ),
        (
            "inbound_messages with ack_messages coupled to preserve ack >= inbound",
            (
                1,
                1,
                above_semaphore_max,
                1,
                1,
                above_semaphore_max,
                RECORD_HEADER_BYTES,
            ),
        ),
        (
            "inbound_bytes",
            (1, 1, 1, above_semaphore_max, 1, 1, RECORD_HEADER_BYTES),
        ),
        (
            "ack_messages",
            (1, 1, 1, 1, 1, above_semaphore_max, RECORD_HEADER_BYTES),
        ),
        ("ack_bytes", (1, 1, 1, 1, 1, 1, above_semaphore_max)),
    ];

    for (
        case,
        (
            outbound_messages,
            outbound_bytes,
            inbound_messages,
            inbound_bytes,
            composition_messages,
            ack_messages,
            ack_bytes,
        ),
    ) in cases
    {
        let outcome = QuinnLegActorLiveLimits::new(
            outbound_messages,
            outbound_bytes,
            inbound_messages,
            inbound_bytes,
            composition_messages,
            ack_messages,
            ack_bytes,
        );
        assert!(
            matches!(outcome, Err(QuinnLegActorError::InvalidConfig)),
            "{case} above Tokio's semaphore maximum must return exact InvalidConfig; got {outcome:?}",
        );
    }
}

#[test]
fn production_shared_actor_limits_cover_the_maximum_encoded_data_frame() {
    for limits in [
        QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits"),
        composition_actor_limits(),
    ] {
        assert_eq!(limits.max_frame_bytes(), MAX_ENCODED_DATA_FRAME_BYTES);
        assert!(limits.queue().max_bytes >= MAX_ENCODED_DATA_FRAME_BYTES);
        assert!(limits.live().outbound_bytes >= MAX_ENCODED_DATA_FRAME_BYTES);
        assert!(limits.live().inbound_bytes >= MAX_ENCODED_DATA_FRAME_BYTES);
    }
}

#[test]
fn server_config_rejects_peak_termination_guard_counter_overflow() {
    let bind_addr = "127.0.0.1:0".parse().expect("loopback bind address");
    let cert_path = PathBuf::from("unused-capacity-test-cert.pem");
    let key_path = PathBuf::from("unused-capacity-test-key.pem");
    let maximum_without_overflow = usize::MAX / super::PEAK_TERMINATION_GUARDS_PER_ACTOR;
    QuinnLegActorServerConfig::new(
        bind_addr,
        cert_path.clone(),
        key_path.clone(),
        maximum_without_overflow,
    )
    .expect("the bootstrap-plus-actor guard multiplication boundary remains valid");

    let first_overflowing_actor_limit = maximum_without_overflow
        .checked_add(1)
        .expect("the peak guard divisor leaves one overflowing actor count");
    let error = QuinnLegActorServerConfig::new(
        bind_addr,
        cert_path,
        key_path,
        first_overflowing_actor_limit,
    )
    .expect_err("peak termination-guard counter overflow must fail server configuration");
    assert!(matches!(error, QuinnLegActorError::InvalidConfig));
}

#[test]
fn only_transport_end_before_any_record_header_is_benign() {
    let application_closed = || {
        quinn::ReadError::ConnectionLost(quinn::ConnectionError::ApplicationClosed(
            quinn::ApplicationClose {
                error_code: 1_u32.into(),
                reason: bytes::Bytes::new(),
            },
        ))
    };
    let clean_boundary =
        super::classify_record_read_end(super::RecordReadContext::RecordBoundary, 0, None);
    assert!(matches!(clean_boundary, QuinnLegActorError::PeerClosed));
    let reset_boundary = super::classify_record_read_end(
        super::RecordReadContext::RecordBoundary,
        0,
        Some(quinn::ReadError::Reset(1_u32.into())),
    );
    assert!(matches!(reset_boundary, QuinnLegActorError::Reset));
    let application_close_boundary = super::classify_record_read_end(
        super::RecordReadContext::RecordBoundary,
        0,
        Some(application_closed()),
    );
    assert!(matches!(
        application_close_boundary,
        QuinnLegActorError::PeerClosed
    ));

    for (context, filled, error) in [
        (super::RecordReadContext::RecordBoundary, 1, None),
        (super::RecordReadContext::RecordBody, 0, None),
        (super::RecordReadContext::RecordBody, 1, None),
        (
            super::RecordReadContext::RecordBody,
            0,
            Some(quinn::ReadError::Reset(1_u32.into())),
        ),
        (
            super::RecordReadContext::RecordBody,
            0,
            Some(application_closed()),
        ),
        (
            super::RecordReadContext::RecordBoundary,
            1,
            Some(quinn::ReadError::Reset(1_u32.into())),
        ),
    ] {
        let classified = super::classify_record_read_end(context, filled, error);
        assert!(matches!(
            &classified,
            QuinnLegActorError::Protocol("truncated actor record")
        ));
        assert_eq!(
            classified.terminal_reason(),
            LegTransportTerminalReason::FatalIo,
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn real_quinn_partial_header_and_body_reset_are_fatal_truncation() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback server address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("valid raw Quinn server configuration");
    let server_endpoint = quinn::Endpoint::server(
        super::build_server_quinn_config(&server_config, limits)
            .expect("build raw Quinn server transport"),
        server_config.bind_addr,
    )
    .expect("bind raw Quinn server endpoint");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        server_endpoint
            .local_addr()
            .expect("raw server retains its bound address"),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid raw Quinn client configuration");
    let client_endpoint =
        super::build_client_endpoint(&client_config, limits).expect("build raw Quinn client");
    let (client_connection, server_connection) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            async {
                client_endpoint
                    .connect(client_config.server_addr, &client_config.server_name)
                    .expect("begin raw Quinn connection")
                    .await
            },
            async {
                server_endpoint
                    .accept()
                    .await
                    .expect("raw server accepts one Incoming")
                    .await
            },
        )
    })
    .await
    .expect("raw Quinn handshake is bounded");
    let client_connection = client_connection.expect("raw client reaches full 1-RTT");
    let server_connection = server_connection.expect("raw server reaches full 1-RTT");

    for (context, record_bytes, label) in [
        (
            super::RecordReadContext::RecordBoundary,
            RECORD_HEADER_BYTES,
            "header",
        ),
        (super::RecordReadContext::RecordBody, 8, "body"),
    ] {
        let mut send = client_connection
            .open_uni()
            .await
            .unwrap_or_else(|error| panic!("open {label} tracer stream: {error}"));
        send.write_all(&[0xa5, 0x5a, 0x01])
            .await
            .unwrap_or_else(|error| panic!("write partial {label}: {error}"));
        let mut recv = server_connection
            .accept_uni()
            .await
            .unwrap_or_else(|error| panic!("accept {label} tracer stream: {error}"));
        let gate = super::TestChildPublicationGate::default();
        gate.arm();
        let (_cancel_signal, mut cancel) = tokio::sync::watch::channel(false);
        let mut bytes = vec![0_u8; record_bytes];
        let mut read = Box::pin(super::read_record_or_cancel(
            &mut recv,
            &mut bytes,
            context,
            &mut cancel,
            Some(&gate),
        ));
        tokio::select! {
            () = gate.wait_until_blocked() => {}
            result = &mut read => {
                panic!("partial {label} read completed before reset: {result:?}");
            }
        }
        send.reset(CLOSE_CODE)
            .unwrap_or_else(|error| panic!("reset partial {label}: {error}"));
        gate.release();
        let error = tokio::time::timeout(TEST_TIMEOUT, read)
            .await
            .unwrap_or_else(|_| panic!("partial {label} reset classification is bounded"))
            .expect_err("partial record reset must never become a benign transport end");
        assert!(matches!(
            error,
            QuinnLegActorError::Protocol("truncated actor record")
        ));
        assert_eq!(error.terminal_reason(), LegTransportTerminalReason::FatalIo,);
    }

    client_connection.close(CLOSE_CODE, b"partial read tracer complete");
    server_connection.close(CLOSE_CODE, b"partial read tracer complete");
    client_endpoint.close(CLOSE_CODE, b"partial read tracer complete");
    server_endpoint.close(CLOSE_CODE, b"partial read tracer complete");
    tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client_endpoint.wait_idle(), server_endpoint.wait_idle());
    })
    .await
    .expect("raw Quinn tracer endpoint cleanup is bounded");
}

#[test]
fn limits_reject_message_envelope_overflow() {
    let leg_io = LegIoLimits::new(1, 1, 1, 1, 1).expect("minimal valid LegIo limits");
    let queue_messages = usize::MAX
        .checked_sub(5)
        .expect("usize leaves room for the fixed live-message terms");
    let queue = QuinnLegActorQueueLimits::new(queue_messages, 1)
        .expect("nonzero queue capacities are individually valid");
    let live = QuinnLegActorLiveLimits::new(1, 1, 1, 1, 1, 1, RECORD_HEADER_BYTES)
        .expect("minimal primitive-backed live capacities are valid");

    let error = QuinnLegActorLimits::new(leg_io, queue, live)
        .expect_err("Qm + Om + Im + Cm + Am + two partial readers must not overflow");
    assert!(matches!(error, QuinnLegActorError::InvalidConfig));
}

#[test]
fn limits_reject_byte_envelope_overflow() {
    let leg_io = LegIoLimits::new(1, 1, 1, 1, 1).expect("minimal valid LegIo limits");
    let queue_bytes = usize::MAX
        .checked_sub(56)
        .expect("usize leaves all but one of the fixed 57-byte envelope terms");
    let queue = QuinnLegActorQueueLimits::new(1, queue_bytes)
        .expect("nonzero queue capacities are individually valid");
    let live = QuinnLegActorLiveLimits::new(1, 1, 1, 1, 1, 1, RECORD_HEADER_BYTES)
        .expect("minimal primitive-backed live capacities are valid");

    let error = QuinnLegActorLimits::new(leg_io, queue, live)
        .expect_err("Qb + Ob + Ib + Ab + Hd + F + Ha must not overflow");
    assert!(matches!(error, QuinnLegActorError::InvalidConfig));
}

#[test]
fn limits_accept_live_byte_totals_above_u32_when_semaphore_supports_them() {
    let semaphore_max = tokio::sync::Semaphore::MAX_PERMITS;
    if semaphore_max <= u32::MAX as usize {
        return;
    }
    let total_capacity = (u32::MAX as usize)
        .checked_add(1)
        .expect("a platform with wider Semaphore capacity can express u32::MAX + 1");
    assert!(total_capacity <= semaphore_max);

    let cases = [
        ("outbound_bytes", total_capacity, 1, RECORD_HEADER_BYTES),
        ("inbound_bytes", 1, total_capacity, RECORD_HEADER_BYTES),
        ("ack_bytes", 1, 1, total_capacity),
    ];
    let outcomes = cases.map(|(case, outbound_bytes, inbound_bytes, ack_bytes)| {
        let byte_envelope = [
            1,
            outbound_bytes,
            inbound_bytes,
            ack_bytes,
            RECORD_HEADER_BYTES,
            1,
            RECORD_HEADER_BYTES,
        ]
        .into_iter()
        .try_fold(0usize, |total, bytes| total.checked_add(bytes))
        .expect("the table case has a checked byte envelope");
        let leg_io = LegIoLimits::new(1, 1, 1, 1, 1).expect("minimal valid LegIo limits");
        let queue =
            QuinnLegActorQueueLimits::new(1, 1).expect("minimal valid outbound queue limits");
        let live =
            QuinnLegActorLiveLimits::new(1, outbound_bytes, 1, inbound_bytes, 1, 1, ack_bytes)
                .expect("the live total stays within Tokio's semaphore maximum");
        (
            case,
            byte_envelope,
            QuinnLegActorLimits::new(leg_io, queue, live),
        )
    });

    assert!(
        outcomes.iter().all(|(_, _, outcome)| outcome.is_ok()),
        "live byte totals may exceed u32 when every single-frame acquire still fits: {outcomes:#?}",
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ack_message_bound_includes_delivery_writer_in_flight_record() {
    let ordered = ordered_control(16);
    let payload = ordered
        .encode(STANDBY_CONTROL_V1)
        .expect("legal ordered control encoding");
    let frame_bytes = payload.len();
    let frame_bytes_u64 = u64::try_from(frame_bytes).expect("payload bound fits u64");
    let limits = QuinnLegActorLimits::new(
        LegIoLimits::new(
            frame_bytes,
            1,
            frame_bytes_u64,
            2,
            frame_bytes_u64
                .checked_mul(2)
                .expect("two inbound frames fit the lifetime byte budget"),
        )
        .expect("finite two-frame LegIo lifetime bounds"),
        QuinnLegActorQueueLimits::new(1, frame_bytes)
            .expect("sole outbound queue holds one exact frame"),
        QuinnLegActorLiveLimits::new(
            1,
            frame_bytes,
            1,
            frame_bytes,
            1,
            1,
            RECORD_HEADER_BYTES
                .checked_mul(2)
                .expect("two ACK headers fit the byte permit budget"),
        )
        .expect("one ACK message must include the writer in-flight record"),
    )
    .expect("consistent ACK in-flight test limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x79; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x7a; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_bind: SocketAddr = "127.0.0.1:0".parse().expect("loopback peer address");
    let peer_server_config = QuinnLegActorServerConfig::new(
        peer_bind,
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("valid raw peer server config");
    let bootstrap_server = super::build_server_quinn_config(
        &peer_server_config,
        QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits"),
    )
    .expect("build production bootstrap server config");
    let actor_server = super::build_server_quinn_config(&peer_server_config, limits)
        .expect("build production actor server config");
    let peer_endpoint =
        quinn::Endpoint::server(bootstrap_server, peer_bind).expect("bind raw Quinn peer");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        peer_endpoint.local_addr().expect("raw peer local address"),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client config");
    let (client, peer) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            async {
                let incoming = peer_endpoint
                    .accept()
                    .await
                    .expect("raw peer accepts one Incoming");
                let connecting = incoming
                    .accept_with(std::sync::Arc::new(actor_server))
                    .expect("raw peer selects production actor config");
                let connection = connecting.await.expect("raw peer completes full 1-RTT");
                super::validate_alpn(&connection).expect("raw peer validates exact ALPN");
                let exporter = super::export_binding(&connection, peer_policy)
                    .expect("raw peer derives production exporter binding");
                super::binding_confirmation(
                    &connection,
                    super::ActorRole::Owner,
                    LegId::A,
                    peer_policy,
                    &exporter,
                )
                .await
                .expect("raw peer completes production HMAC prelude");
                let streams =
                    super::bootstrap_streams(&connection, super::ActorRole::Owner, LegId::A)
                        .await
                        .expect("raw peer completes fixed production streams");
                (connection, streams)
            },
        )
    })
    .await
    .expect("client and raw peer bootstrap are bounded");
    let mut client = client.expect("publish client actor");
    let (peer_connection, peer_streams) = peer;
    let super::ActorStreams {
        mut data_send,
        mut delivered_send,
        mut data_recv,
        mut delivered_recv,
    } = peer_streams;
    let route = WireRoute::new(LegId::A, WireDirection::OwnerToClient);
    let delivery_writer_gate = std::sync::Arc::clone(&client.delivery_writer_gate);
    delivery_writer_gate.arm();

    let first_header = super::WireHeader::data(route, WireLane::Control, 1, frame_bytes, true);
    data_send
        .write_all(&first_header.encode().expect("encode first ordered header"))
        .await
        .expect("raw Owner writes first ordered header");
    data_send
        .write_all(&payload)
        .await
        .expect("raw Owner writes first complete payload");
    let first_inbound =
        tokio::time::timeout(TEST_TIMEOUT, client.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("first classification is bounded")
            .expect("first ordered record classifies")
            .expect("first ordered record reaches the composition owner");
    tokio::time::timeout(TEST_TIMEOUT, delivery_writer_gate.wait_until_blocked())
        .await
        .expect("delivery writer holds the first ACK after dequeue and before write");
    let first_held = client.actor_snapshot();
    assert_eq!(first_held.reserved_delivery_acks(), 1);
    assert_eq!(first_held.inbound_owned_bytes(), frame_bytes);
    assert_eq!(first_held.composition_owned_messages(), 1);
    drop(first_inbound);

    let second_header = super::WireHeader::data(route, WireLane::Control, 2, frame_bytes, true);
    data_send
        .write_all(
            &second_header
                .encode()
                .expect("encode second ordered header"),
        )
        .await
        .expect("raw Owner writes second ordered header");
    data_send
        .write_all(&payload)
        .await
        .expect("raw Owner writes second complete payload");
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = client.actor_snapshot();
            if snapshot.validated_data_headers() == 2
                && snapshot.inbound_owned_bytes() == frame_bytes
                && snapshot.composition_owned_messages() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second ordered record reaches the ACK-message admission boundary");
    let blocked = client.actor_snapshot();
    assert_eq!(
        blocked.reserved_delivery_acks(),
        1,
        "Am=1 must include the delivery-writer in-flight record",
    );
    assert_eq!(
        blocked.queued_inbound_messages(),
        0,
        "the second ordered record must backpressure before inbound admission",
    );

    delivery_writer_gate.release();
    let mut first_ack = [0_u8; RECORD_HEADER_BYTES];
    tokio::time::timeout(TEST_TIMEOUT, delivered_recv.read_exact(&mut first_ack))
        .await
        .expect("first ACK observation is bounded")
        .expect("raw Owner reads the first real DELIVERED record");
    assert_eq!(
        first_ack,
        super::WireHeader::delivered(first_header)
            .encode()
            .expect("encode expected first DELIVERED record"),
    );
    let second_inbound =
        tokio::time::timeout(TEST_TIMEOUT, client.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("second classification resumes within the bound")
            .expect("second ordered record classifies")
            .expect("second ordered record reaches the composition owner");
    let mut second_ack = [0_u8; RECORD_HEADER_BYTES];
    tokio::time::timeout(TEST_TIMEOUT, delivered_recv.read_exact(&mut second_ack))
        .await
        .expect("second ACK observation is bounded")
        .expect("raw Owner reads the second real DELIVERED record");
    assert_eq!(
        second_ack,
        super::WireHeader::delivered(second_header)
            .encode()
            .expect("encode expected second DELIVERED record"),
    );
    drop(second_inbound);

    client.request_cancel();
    let joined = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("client cleanup join is bounded")
        .expect("client actor joins cleanly after explicit cancellation");
    assert_eq!(joined.owned_messages(), 0);
    assert_eq!(joined.owned_bytes(), 0);
    let _ = data_send.reset(CLOSE_CODE);
    let _ = delivered_send.reset(CLOSE_CODE);
    let _ = data_recv.stop(CLOSE_CODE);
    let _ = delivered_recv.stop(CLOSE_CODE);
    drop((data_send, delivered_send, data_recv, delivered_recv));
    peer_connection.close(CLOSE_CODE, b"raw peer cleanup");
    peer_endpoint.close(CLOSE_CODE, b"raw peer cleanup");
    tokio::time::timeout(TEST_TIMEOUT, peer_endpoint.wait_idle())
        .await
        .expect("raw peer endpoint cleanup is bounded");
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_server_owner_retains_live_slot_until_fixed_tasks_terminate() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x81; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x82; 16]).expect("nonzero device principal"),
    );
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("one-slot listener configuration is valid");
    let mut listener =
        tokio::time::timeout(TEST_TIMEOUT, QuinnLegActorListener::bind(server_config))
            .await
            .expect("listener bind is bounded")
            .expect("bind one-slot listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("real actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let server = server.expect("publish sole server actor");
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
    assert_eq!(
        listener
            .registry
            .terminating_tasks
            .load(std::sync::atomic::Ordering::Acquire),
        super::FIXED_TASK_GUARDS_PER_ACTOR,
    );

    drop(server);
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        1,
        "an unclean owner drop must retain its slot until all fixed tasks terminate",
    );
    let blocked = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("shutdown observes the terminating owner within the bound")
        .expect_err("terminating actor ownership still blocks listener shutdown");
    assert!(matches!(
        blocked,
        QuinnLegActorError::ListenerShutdownBlocked { live: 1 }
    ));
    assert_eq!(listener.phase, super::ListenerPhase::Accepting);
    assert!(matches!(
        listener.registry.try_reserve(),
        Err(QuinnLegActorError::ListenerShutdownUnclean)
    ));

    client.request_cancel();
    let _ = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("client cleanup join is bounded");
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if listener
                .registry
                .live
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
                && listener
                    .registry
                    .terminating_tasks
                    .load(std::sync::atomic::Ordering::Acquire)
                    == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("aborted server tasks release their sole slot within the bound");
    let shutdown_error = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("unclean listener shutdown is bounded")
        .expect_err("owner loss cannot mint a clean listener join");
    assert!(matches!(
        shutdown_error,
        QuinnLegActorError::ListenerShutdownUnclean
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn listener_join_is_minted_once_and_repeat_shutdown_is_rejected() {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("one-slot listener configuration is valid");
    let mut listener =
        tokio::time::timeout(TEST_TIMEOUT, QuinnLegActorListener::bind(server_config))
            .await
            .expect("listener bind is bounded")
            .expect("bind idle listener");

    let _joined = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("first listener shutdown is bounded")
        .expect("idle listener mints its sole Joined witness");
    let repeat = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("repeat shutdown rejection is bounded")
        .expect_err("a listener cannot mint a second Joined witness");
    assert!(matches!(repeat, QuinnLegActorError::ListenerClosed));
}

#[tokio::test(flavor = "current_thread")]
async fn listener_join_releases_the_udp_socket_before_returning() {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("one-slot listener configuration is valid");
    let mut listener =
        tokio::time::timeout(TEST_TIMEOUT, QuinnLegActorListener::bind(server_config))
            .await
            .expect("listener bind is bounded")
            .expect("bind idle listener");
    let released_addr = listener.local_addr();

    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("listener consumes its endpoint-driver completion witness");
    let rebound = std::net::UdpSocket::bind(released_addr)
        .expect("Joined proves the exact listener UDP socket was released");
    drop(rebound);
    assert_eq!(listener.phase, super::ListenerPhase::Joined);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_listener_driver_wait_reuses_the_registered_cleanup_task() {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("one-slot listener configuration is valid");
    let mut listener =
        tokio::time::timeout(TEST_TIMEOUT, QuinnLegActorListener::bind(server_config))
            .await
            .expect("listener bind is bounded")
            .expect("bind idle listener");
    let endpoint_release_gate = std::sync::Arc::clone(&listener.endpoint_release_gate);
    endpoint_release_gate.arm();

    let mut first_shutdown = Box::pin(listener.shutdown());
    tokio::select! {
        () = endpoint_release_gate.wait_until_blocked() => {}
        result = &mut first_shutdown => {
            panic!("driver cleanup must remain registered before Joined, got {result:?}");
        }
    }
    drop(first_shutdown);
    assert_eq!(listener.phase, super::ListenerPhase::Closing);
    assert!(listener.endpoint_release_task.is_some());

    endpoint_release_gate.release();
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("retried driver cleanup wait is bounded")
        .expect("retry consumes the same endpoint-driver completion witness");
    assert_eq!(listener.phase, super::ListenerPhase::Joined);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_listener_shutdown_retries_the_same_closing_transaction() {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("one-slot listener configuration is valid");
    let mut listener =
        tokio::time::timeout(TEST_TIMEOUT, QuinnLegActorListener::bind(server_config))
            .await
            .expect("listener bind is bounded")
            .expect("bind idle listener");
    let shutdown_gate = std::sync::Arc::clone(&listener.shutdown_gate);
    shutdown_gate.arm();

    let mut first_shutdown = Box::pin(listener.shutdown());
    tokio::select! {
        () = shutdown_gate.wait_until_blocked() => {}
        result = &mut first_shutdown => {
            panic!("armed shutdown must pause after close, got {result:?}");
        }
    }
    drop(first_shutdown);
    shutdown_gate.release();

    let _joined = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("retried listener shutdown is bounded")
        .expect("cancelling the first waiter must not consume the closing transaction");
    let repeat = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("post-join shutdown rejection is bounded")
        .expect_err("the retried transaction still mints Joined exactly once");
    assert!(matches!(repeat, QuinnLegActorError::ListenerClosed));
}

#[tokio::test(flavor = "current_thread")]
async fn policy_and_leg_mismatch_fail_before_publication_and_listener_remains_reusable() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let accepted_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x85; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x86; 16]).expect("nonzero device principal"),
    );
    let mismatched_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x87; 32]).expect("different nonzero owner identity"),
        DevicePrincipal::new([0x88; 16]).expect("different nonzero device principal"),
    );
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("one-slot listener configuration is valid");
    let mut listener =
        tokio::time::timeout(TEST_TIMEOUT, QuinnLegActorListener::bind(server_config))
            .await
            .expect("listener bind is bounded")
            .expect("bind one-slot listener");
    let listener_addr = listener.local_addr();

    let policy_client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("policy client address"),
        listener_addr,
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid policy-mismatch client configuration");
    let (policy_client, policy_server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(
                policy_client_config,
                LegId::A,
                limits,
                mismatched_policy,
            ),
            listener.accept(LegId::A, limits, accepted_policy),
        )
    })
    .await
    .expect("policy mismatch rejection is bounded");
    let policy_client_error = match policy_client {
        Err(error) => error,
        Ok(_actor) => panic!("mismatched client policy published an actor"),
    };
    let policy_server_error = match policy_server {
        Err(error) => error,
        Ok(_actor) => panic!("mismatched server policy published an actor"),
    };
    assert!(
        policy_client_error
            .to_string()
            .contains("binding confirmation mismatch")
            || policy_server_error
                .to_string()
                .contains("binding confirmation mismatch"),
        "one exact role must report the binding discriminator: client={policy_client_error}; server={policy_server_error}",
    );
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert_eq!(
        listener
            .registry
            .terminating_tasks
            .load(std::sync::atomic::Ordering::Acquire),
        0,
    );
    assert!(
        !listener
            .registry
            .unclean
            .load(std::sync::atomic::Ordering::Acquire)
    );

    let leg_client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("leg client address"),
        listener_addr,
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid leg-mismatch client configuration");
    let (leg_client, leg_server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(leg_client_config, LegId::A, limits, accepted_policy,),
            listener.accept(LegId::B, limits, accepted_policy),
        )
    })
    .await
    .expect("LegId mismatch rejection is bounded");
    let leg_client_error = match leg_client {
        Err(error) => error,
        Ok(_actor) => panic!("mismatched client LegId published an actor"),
    };
    let leg_server_error = match leg_server {
        Err(error) => error,
        Ok(_actor) => panic!("mismatched server LegId published an actor"),
    };
    assert!(
        leg_client_error
            .to_string()
            .contains("binding confirmation mismatch")
            || leg_server_error
                .to_string()
                .contains("binding confirmation mismatch"),
        "one exact role must report the LegId discriminator: client={leg_client_error}; server={leg_server_error}",
    );
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert!(
        !listener
            .registry
            .unclean
            .load(std::sync::atomic::Ordering::Acquire)
    );

    let valid_client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("valid client address"),
        listener_addr,
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid matching client configuration");
    let (valid_client, valid_server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(valid_client_config, LegId::A, limits, accepted_policy,),
            listener.accept(LegId::A, limits, accepted_policy),
        )
    })
    .await
    .expect("matching retry bootstrap is bounded");
    let mut valid_client = valid_client.expect("matching client actor is published");
    let mut valid_server = valid_server.expect("matching server actor is published");
    valid_client.request_cancel();
    valid_server.request_cancel();
    let (client_join, server_join) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(valid_client.join(), valid_server.join())
    })
    .await
    .expect("matching actor cleanup joins are bounded");
    client_join.expect("matching client joins cleanly");
    server_join.expect("matching server joins cleanly");
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("reused listener shutdown is bounded")
        .expect("only rejected prepublication attempts leave no poison");
}

#[tokio::test(flavor = "current_thread")]
async fn server_actor_a_joins_while_b_exchanges_after_blocked_shared_listener_shutdown() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x89; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x8a; 16]).expect("nonzero device principal"),
    );
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        2,
    )
    .expect("two-slot listener configuration is valid");
    let mut listener =
        tokio::time::timeout(TEST_TIMEOUT, QuinnLegActorListener::bind(server_config))
            .await
            .expect("listener bind is bounded")
            .expect("bind shared listener");
    let listener_addr = listener.local_addr();
    let client_a_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("client A address"),
        listener_addr,
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client A configuration");
    let (client_a, server_a) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_a_config, LegId::A, limits, peer_policy,),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor A bootstrap is bounded");
    let mut client_a = client_a.expect("publish client actor A");
    let mut server_a = server_a.expect("publish server actor A");

    let client_b_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("client B address"),
        listener_addr,
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client B configuration");
    let (client_b, server_b) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_b_config, LegId::B, limits, peer_policy,),
            listener.accept(LegId::B, limits, peer_policy),
        )
    })
    .await
    .expect("actor B bootstrap is bounded");
    let mut client_b = client_b.expect("publish client actor B");
    let mut server_b = server_b.expect("publish server actor B");
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        2
    );

    client_a.request_cancel();
    server_a.request_cancel();
    let (client_a_join, server_a_join) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client_a.join(), server_a.join())
    })
    .await
    .expect("actor A cleanup joins are bounded");
    client_a_join.expect("client actor A joins cleanly");
    server_a_join.expect("server actor A joins cleanly");
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
    assert_eq!(
        listener.open_connections_for_test(),
        1,
        "server A Joined cannot release its slot before A's exact endpoint entry and driver are gone",
    );

    let blocked = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("live-listener shutdown rejection is bounded")
        .expect_err("actor B keeps shared listener shutdown blocked");
    assert!(matches!(
        blocked,
        QuinnLegActorError::ListenerShutdownBlocked { live: 1 }
    ));
    assert_eq!(listener.phase, super::ListenerPhase::Accepting);

    let frame = ordered_control(21);
    let receipt = client_b
        .enqueue_ordered_leg_control(frame.clone(), STANDBY_CONTROL_V1)
        .expect("actor B queue admits one legal ordered record");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("actor B submits after blocked shutdown"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("actor B receive after blocked shutdown is bounded")
        .expect("actor B shared endpoint remains live")
        .expect("actor B delivers the exact ordered value");
    inbound.consume_for_test(|inbound| match inbound {
        BoundInbound::LegControl(bound) => {
            let (_seal, received) = bound.into_parts();
            assert_eq!(received, frame);
        }
        BoundInbound::Session(_) => panic!("actor B leg control was misclassified"),
    });
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !receipt.was_delivered() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor B delivery ACK survives actor A retirement");
    assert_eq!(
        client_b
            .try_flush(SimTime::ZERO)
            .expect("actor B consumes its completed ordered wait"),
        QuinnLegActorFlushDisposition::Idle,
    );

    client_b.request_cancel();
    server_b.request_cancel();
    let (client_b_join, server_b_join) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client_b.join(), server_b.join())
    })
    .await
    .expect("actor B cleanup joins are bounded");
    client_b_join.expect("client actor B joins cleanly");
    server_b_join.expect("server actor B joins cleanly");
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert_eq!(
        listener.open_connections_for_test(),
        0,
        "both server Joined witnesses require both exact Quinn close tails",
    );
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("shared listener final shutdown is bounded")
        .expect("both exact server actors joined before listener idle");
}

#[tokio::test(flavor = "current_thread")]
async fn port_linearization_guard_drop_releases_blocking_waiter() {
    let gate = std::sync::Arc::new(super::TestPortLinearizationGate::default());
    let release_guard = gate.arm();
    let blocking_gate = std::sync::Arc::clone(&gate);
    let blocking_waiter = tokio::task::spawn_blocking(move || blocking_gate.pause_if_armed());
    tokio::time::timeout(TEST_TIMEOUT, gate.wait_until_blocked())
        .await
        .expect("blocking waiter reaches the armed port gate");

    drop(release_guard);
    tokio::time::timeout(TEST_TIMEOUT, blocking_waiter)
        .await
        .expect("dropping the guard releases the blocking waiter")
        .expect("blocking waiter task joins");
}

#[tokio::test(flavor = "current_thread")]
async fn closing_rejects_actor_ports_before_cancel_can_schedule_and_joined_stays_distinct() {
    let queued = ordered_control(7);
    let closing_rejected = ordered_control(8);
    let joined_rejected = ordered_control(9);
    let queued_bytes = queued
        .encode(STANDBY_CONTROL_V1)
        .expect("valid queued control encoding")
        .len();
    let closing_rejected_bytes = closing_rejected
        .encode(STANDBY_CONTROL_V1)
        .expect("valid closing-rejected control encoding");
    let joined_rejected_bytes = joined_rejected
        .encode(STANDBY_CONTROL_V1)
        .expect("valid joined-rejected control encoding");
    let queued_bytes_u64 = u64::try_from(queued_bytes).expect("encoded length fits u64");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x71; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x72; 16]).expect("nonzero device principal"),
    );
    let leg_io_limits = LegIoLimits::new(
        queued_bytes,
        2,
        queued_bytes_u64 * 2,
        2,
        queued_bytes_u64 * 2,
    )
    .expect("finite lifetime limits");
    let limits = QuinnLegActorLimits::new(
        leg_io_limits,
        QuinnLegActorQueueLimits::new(2, queued_bytes * 2).expect("two-frame sole queue"),
        QuinnLegActorLiveLimits::new(1, queued_bytes, 1, queued_bytes, 1, 1, 64)
            .expect("independent actor bounds"),
    )
    .expect("consistent actor limits");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = tokio::time::timeout(
        TEST_TIMEOUT,
        QuinnLegActorListener::bind(
            QuinnLegActorServerConfig::new(
                "127.0.0.1:0".parse().expect("loopback listener address"),
                cert_dir.join("server-cert.pem"),
                cert_dir.join("server-key.pem"),
                2,
            )
            .expect("valid server config"),
        ),
    )
    .await
    .expect("listener bind is bounded")
    .expect("bind listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client config");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");

    let receipt = client
        .enqueue_ordered_leg_control(queued, STANDBY_CONTROL_V1)
        .expect("sole queue admits one unflushed control frame");
    assert!(receipt.queue_is_live());
    let before = client.actor_snapshot();
    assert_eq!(before.outbound_queue_messages(), 1);
    assert_eq!(before.outbound_queue_bytes(), queued_bytes);
    assert_eq!(before.queued_inbound_messages(), 0);
    assert_eq!(before.inbound_owned_bytes(), 0);
    assert_eq!(before.reserved_delivery_acks(), 0);
    assert_eq!(before.composition_owned_messages(), 0);

    client.request_cancel();
    let closing_error =
        match client.enqueue_ordered_leg_control(closing_rejected, STANDBY_CONTROL_V1) {
            Err(error) => error,
            Ok(_receipt) => panic!("Closing admitted a second ordered frame"),
        };
    let returned = match closing_error {
        QuinnLegActorEnqueueError::RootFailed { .. } => {
            panic!("pure Closing was misreported as a root failure")
        }
        QuinnLegActorEnqueueError::Closing { frame } => frame,
        QuinnLegActorEnqueueError::AlreadyJoined { .. } => {
            panic!("Closing was misreported as AlreadyJoined")
        }
        QuinnLegActorEnqueueError::Queue(error) => {
            panic!("Closing reached the live queue: {error:?}")
        }
    };
    assert_eq!(
        returned
            .encode(STANDBY_CONTROL_V1)
            .expect("returned closing frame remains legal"),
        closing_rejected_bytes,
    );
    assert_eq!(client.actor_snapshot(), before);
    assert!(receipt.queue_is_live());
    assert!(!receipt.was_delivered());
    assert!(matches!(
        client.try_flush(SimTime::ZERO),
        Err(QuinnLegActorError::Closing),
    ));
    assert_eq!(client.actor_snapshot(), before);
    assert!(receipt.queue_is_live());
    assert!(!receipt.was_delivered());

    let closing_epoch = client.readiness_epoch();
    let closing_wait_first_poll = {
        let mut closing_wait = Box::pin(client.wait_for_progress(closing_epoch));
        std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(closing_wait.as_mut(), context))
        })
        .await
    };
    assert!(
        matches!(
            closing_wait_first_poll,
            std::task::Poll::Ready(Err(QuinnLegActorError::Closing)),
        ),
        "Closing readiness wait must reject on its first poll",
    );
    let closing_recv_first_poll = {
        let mut closing_recv = Box::pin(client.recv_classified(STANDBY_CONTROL_V1));
        std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(closing_recv.as_mut(), context))
        })
        .await
    };
    assert!(
        matches!(
            closing_recv_first_poll,
            std::task::Poll::Ready(Err(QuinnLegActorError::Closing)),
        ),
        "pure cancellation receive must reject on its first poll",
    );
    assert_eq!(client.actor_snapshot(), before);
    assert!(receipt.queue_is_live());
    assert!(!receipt.was_delivered());

    let server_epoch = server.readiness_epoch();
    let server_terminal =
        tokio::time::timeout(TEST_TIMEOUT, server.wait_for_progress(server_epoch))
            .await
            .expect("one-sided client cancellation wakes the peer progress port")
            .expect_err("peer observes a semantic transport terminal");
    let QuinnLegActorError::RootFailed(server_first_cause) = server_terminal else {
        panic!("peer terminal returned the wrong port error: {server_terminal:?}");
    };
    assert!(matches!(
        server_first_cause.terminal_reason(),
        LegTransportTerminalReason::PeerClosed | LegTransportTerminalReason::Reset,
    ));
    server.request_cancel();
    let joining = ordered_write_cancel::settle_queued_cancel(&mut client, queued_bytes);
    let (client_joined, server_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async { tokio::join!(joining, server.join()) })
            .await
            .expect("both actor roots join within the local bound");
    let (client_joined, actual_terminal) = client_joined;
    let server_joined = server_joined.expect("server root was truly awaited");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(
        actual_terminal.reason(),
        LegTransportTerminalReason::LocalCancelled,
    );
    assert_eq!(
        server_joined.terminal_reason(),
        server_first_cause.terminal_reason(),
    );
    assert!(std::ptr::eq(
        server_joined
            .terminal_failure()
            .expect("peer terminal remains attached to structural Joined"),
        server_first_cause.as_ref(),
    ));
    assert_eq!(client.actor_snapshot().outbound_queue_messages(), 0);
    assert_eq!(client.actor_snapshot().outbound_queue_bytes(), 0);
    assert!(!receipt.queue_is_live());
    let joined_error = match client.enqueue_ordered_leg_control(joined_rejected, STANDBY_CONTROL_V1)
    {
        Err(QuinnLegActorEnqueueError::AlreadyJoined { frame }) => frame,
        Err(error) => panic!("Joined returned the wrong enqueue error: {error:?}"),
        Ok(_receipt) => panic!("Joined admitted an ordered frame"),
    };
    assert_eq!(
        joined_error
            .encode(STANDBY_CONTROL_V1)
            .expect("returned joined frame remains legal"),
        joined_rejected_bytes,
    );
    assert!(matches!(
        client.try_flush(SimTime::ZERO),
        Err(QuinnLegActorError::AlreadyJoined),
    ));
    let joined_epoch = client.readiness_epoch();
    let first_poll = {
        let mut post_join_wait = Box::pin(client.wait_for_progress(joined_epoch));
        std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(post_join_wait.as_mut(), context))
        })
        .await
    };
    assert!(
        matches!(
            first_poll,
            std::task::Poll::Ready(Err(QuinnLegActorError::AlreadyJoined)),
        ),
        "post-join readiness wait must fail closed on its first poll",
    );
    let post_join_recv_first_poll = {
        let mut post_join_recv = Box::pin(client.recv_classified(STANDBY_CONTROL_V1));
        std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(post_join_recv.as_mut(), context))
        })
        .await
    };
    assert!(
        matches!(
            post_join_recv_first_poll,
            std::task::Poll::Ready(Err(QuinnLegActorError::AlreadyJoined)),
        ),
        "post-join receive must fail closed on its first poll",
    );

    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("listener idles after both clean joins");
}

#[tokio::test(flavor = "current_thread")]
async fn first_failure_linearizes_before_enqueue_queue_commit() {
    let frame = ordered_control(15);
    let encoded = frame
        .encode(STANDBY_CONTROL_V1)
        .expect("valid ordered control encoding");
    let frame_bytes = encoded.len();
    let frame_bytes_u64 = u64::try_from(frame_bytes).expect("encoded length fits u64");
    let limits = QuinnLegActorLimits::new(
        LegIoLimits::new(frame_bytes, 1, frame_bytes_u64, 1, frame_bytes_u64)
            .expect("finite one-frame LegIo limits"),
        QuinnLegActorQueueLimits::new(1, frame_bytes).expect("sole queue can hold the exact frame"),
        QuinnLegActorLiveLimits::new(1, frame_bytes, 1, frame_bytes, 1, 1, 64)
            .expect("independent actor ownership bounds"),
    )
    .expect("consistent actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x75; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x76; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = tokio::time::timeout(
        TEST_TIMEOUT,
        QuinnLegActorListener::bind(
            QuinnLegActorServerConfig::new(
                "127.0.0.1:0".parse().expect("loopback listener address"),
                cert_dir.join("server-cert.pem"),
                cert_dir.join("server-key.pem"),
                2,
            )
            .expect("valid server config"),
        ),
    )
    .await
    .expect("listener bind is bounded")
    .expect("bind real Quinn listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client config");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("real actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");
    let before_actor = client.actor_snapshot();
    let before_leg_io = client
        .io
        .as_ref()
        .expect("running actor retains its sole LegIo")
        .counters();
    assert_eq!(before_actor.outbound_queue_messages(), 0);
    assert_eq!(before_actor.outbound_queue_bytes(), 0);

    let port_gate = std::sync::Arc::clone(&client.port_linearization_gate);
    let first_failure = std::sync::Arc::clone(&client.first_failure);
    let port_release = port_gate.arm();
    let enqueue_task = tokio::task::spawn_blocking(move || {
        let result = client.enqueue_ordered_leg_control(frame, STANDBY_CONTROL_V1);
        (client, result)
    });
    tokio::time::timeout(TEST_TIMEOUT, port_gate.wait_until_blocked())
        .await
        .expect("enqueue pauses after its old clear observation and before queue commit");
    let published_cause = first_failure
        .record(QuinnLegActorError::Protocol(
            TEST_ENQUEUE_LINEARIZATION_FATAL,
        ))
        .expect("open actor admits the exact enqueue failure");
    port_release.release();

    let (mut client, enqueue_result) = tokio::time::timeout(TEST_TIMEOUT, enqueue_task)
        .await
        .expect("blocking enqueue owner returns after gate release")
        .expect("blocking enqueue owner task joins");
    let (returned_frame, enqueue_cause) = match enqueue_result {
        Err(QuinnLegActorEnqueueError::RootFailed { frame, cause }) => (frame, cause),
        Err(error) => panic!("first failure returned the wrong enqueue error: {error:?}"),
        Ok(receipt) => panic!(
            "first failure crossed the enqueue commit boundary: receipt={receipt:?}, snapshot={:?}",
            client.actor_snapshot(),
        ),
    };
    assert!(std::sync::Arc::ptr_eq(&enqueue_cause, &published_cause,));
    assert_eq!(
        returned_frame
            .encode(STANDBY_CONTROL_V1)
            .expect("returned frame remains exactly encodable"),
        encoded,
    );
    assert_eq!(client.actor_snapshot(), before_actor);
    assert_eq!(
        client
            .io
            .as_ref()
            .expect("failed actor retains its sole LegIo")
            .counters(),
        before_leg_io,
    );
    assert_eq!(client.phase, super::ActorOwnerPhase::Closing);
    assert!(*client.cancel.borrow());
    assert!(client.inbound.is_none());
    assert!(client.inbound_handoff.is_none());

    server.request_cancel();
    let (client_join, server_join) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), server.join())
    })
    .await
    .expect("both actor cleanup joins are bounded");
    let client_joined = client_join.expect(
            "a recorded transport failure still mints Joined after the root and all children are structurally cleaned",
        );
    assert_eq!(
        client_joined.terminal_reason(),
        LegTransportTerminalReason::FatalIo,
    );
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert!(std::ptr::eq(
        client_joined
            .terminal_failure()
            .expect("fatal semantic outcome retains the exact first cause"),
        published_cause.as_ref(),
    ));
    server_join.expect("peer actor joins cleanly after explicit cancellation");
    drop(client);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("listener idles after both actor owners are cleaned");
}

#[tokio::test(flavor = "current_thread")]
async fn server_root_error_crosses_exact_close_tail_before_join_failure() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x79; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x7a; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid server configuration"),
    )
    .await
    .expect("bind listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");

    server.fail_root_after_children_for_test();
    let server_error = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("server root failure is bounded")
        .expect_err("post-child root failure cannot mint Joined");
    assert!(
        server_error
            .to_string()
            .contains(super::TEST_ROOT_OUTER_ERROR)
    );
    assert_eq!(
        listener.open_connections_for_test(),
        0,
        "join failure is published only after the exact server connection leaves the endpoint map",
    );
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        1,
        "the failed server owner still retains its slot while connection cleanup is already exact",
    );

    let client_joined = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("peer cleanup is bounded")
        .expect("peer observes the server close and completes structural cleanup");
    assert!(matches!(
        client_joined.terminal_reason(),
        LegTransportTerminalReason::PeerClosed
            | LegTransportTerminalReason::Reset
            | LegTransportTerminalReason::LocalCancelled,
    ));
    drop(client);
    drop(server);
    let shutdown_error = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("unclean listener shutdown is bounded")
        .expect_err("a structurally failed server actor poisons its listener");
    assert!(matches!(
        shutdown_error,
        QuinnLegActorError::ListenerShutdownUnclean
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn semantic_first_cause_stays_on_ports_while_structural_join_poisons_listener() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x8b; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x8c; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid server configuration"),
    )
    .await
    .expect("bind listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");

    let semantic = server
        .first_failure
        .record(QuinnLegActorError::PeerClosed)
        .expect("open server actor admits the semantic failure");
    server.fail_root_after_children_for_test();
    let rejected = ordered_control(91);
    match server.enqueue_ordered_leg_control(rejected, STANDBY_CONTROL_V1) {
        Err(QuinnLegActorEnqueueError::RootFailed { cause, .. }) => {
            assert!(std::sync::Arc::ptr_eq(&cause, &semantic));
        }
        result => panic!("semantic first cause did not close the port: {result:?}"),
    }

    let structural = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("structural server cleanup is bounded")
        .expect_err("root epilogue error cannot mint Joined");
    let structural = match structural {
        QuinnLegActorError::RootFailed(cause) => cause,
        error => panic!("server join lost its structural discriminator: {error:?}"),
    };
    assert!(!std::sync::Arc::ptr_eq(&structural, &semantic));
    assert!(
        structural
            .to_string()
            .contains(super::TEST_ROOT_OUTER_ERROR)
    );
    let retained_semantic = server
        .first_failure
        .current()
        .expect("ports retain their semantic first cause");
    assert!(std::sync::Arc::ptr_eq(&retained_semantic, &semantic));

    let _client_outcome = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("peer actor cleanup is bounded");
    drop((client, server));
    let shutdown_error = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect_err("structural server failure poisons listener cleanup");
    assert!(matches!(
        shutdown_error,
        QuinnLegActorError::ListenerShutdownUnclean
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn observed_root_error_is_stable_across_join_retries_and_ports_stay_closed() {
    let queued = ordered_control(9);
    let queued_bytes = queued
        .encode(STANDBY_CONTROL_V1)
        .expect("valid queued control encoding")
        .len();
    let queued_bytes_u64 = u64::try_from(queued_bytes).expect("encoded length fits u64");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x73; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x74; 16]).expect("nonzero device principal"),
    );
    let leg_io_limits = LegIoLimits::new(
        queued_bytes,
        2,
        queued_bytes_u64 * 2,
        2,
        queued_bytes_u64 * 2,
    )
    .expect("finite lifetime limits");
    let limits = QuinnLegActorLimits::new(
        leg_io_limits,
        QuinnLegActorQueueLimits::new(1, queued_bytes).expect("one-frame sole queue"),
        QuinnLegActorLiveLimits::new(1, queued_bytes, 1, queued_bytes, 1, 1, 64)
            .expect("independent actor bounds"),
    )
    .expect("consistent actor limits");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = tokio::time::timeout(
        TEST_TIMEOUT,
        QuinnLegActorListener::bind(
            QuinnLegActorServerConfig::new(
                "127.0.0.1:0".parse().expect("loopback listener address"),
                cert_dir.join("server-cert.pem"),
                cert_dir.join("server-key.pem"),
                2,
            )
            .expect("valid server config"),
        ),
    )
    .await
    .expect("listener bind is bounded")
    .expect("bind listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client config");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");

    let receipt = client
        .enqueue_ordered_leg_control(queued, STANDBY_CONTROL_V1)
        .expect("sole queue admits one unflushed control frame");
    assert!(receipt.queue_is_live());
    client.fail_root_after_children_for_test();
    server.request_cancel();

    let first_error = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("first root failure observation is bounded")
        .expect_err("root outer error cannot mint Joined");
    let first_cause = match &first_error {
        QuinnLegActorError::RootFailed(cause) => std::sync::Arc::clone(cause),
        error => panic!("outer root returned the wrong join error: {error:?}"),
    };
    assert_eq!(client.actor_snapshot().outbound_queue_messages(), 1);
    assert!(
        !receipt.queue_is_live(),
        "the mandatory terminal-reporting epilogue closes receipt liveness without dropping the retained queue",
    );

    let second_error = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("root failure retry is bounded")
        .expect_err("reobserving root failure cannot mint Joined");
    assert_eq!(
        std::mem::discriminant(&first_error),
        std::mem::discriminant(&second_error),
        "join retry must preserve the observed causal failure kind",
    );
    assert_eq!(first_error.to_string(), second_error.to_string());
    let second_cause = match &second_error {
        QuinnLegActorError::RootFailed(cause) => cause,
        error => panic!("outer root retry returned the wrong join error: {error:?}"),
    };
    assert!(std::sync::Arc::ptr_eq(&first_cause, second_cause));
    assert!(
        first_error
            .to_string()
            .contains(super::TEST_ROOT_OUTER_ERROR)
    );
    assert_eq!(client.actor_snapshot().outbound_queue_messages(), 1);
    assert!(!receipt.queue_is_live());
    let closing_rejected = ordered_control(10);
    let closing_rejected_bytes = closing_rejected
        .encode(STANDBY_CONTROL_V1)
        .expect("failed-join rejected frame remains legal");
    let returned = match client.enqueue_ordered_leg_control(closing_rejected, STANDBY_CONTROL_V1) {
        Err(QuinnLegActorEnqueueError::RootFailed { frame, cause }) => {
            assert!(
                std::sync::Arc::ptr_eq(&first_cause, &cause),
                "ports and join retries must retain the exact outer-root cause",
            );
            frame
        }
        Err(error) => panic!("failed join did not preserve root failure: {error:?}"),
        Ok(_receipt) => panic!("failed join reopened actor admission"),
    };
    assert_eq!(
        returned
            .encode(STANDBY_CONTROL_V1)
            .expect("returned failed-join frame remains legal"),
        closing_rejected_bytes,
    );

    let server_joined = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("peer clean join is bounded")
        .expect("peer root joins cleanly");
    assert_eq!(server_joined.owned_messages(), 0);
    drop(client);
    assert!(!receipt.queue_is_live());
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("listener idles after failed client owner is dropped");
}

#[tokio::test(flavor = "current_thread")]
async fn root_drains_real_queued_inbound_after_children_before_join_is_polled() {
    let ordered = ordered_control(12);
    let payload = ordered
        .encode(STANDBY_CONTROL_V1)
        .expect("legal ordered control encoding");
    let frame_bytes = payload.len();
    let frame_bytes_u64 = u64::try_from(frame_bytes).expect("payload bound fits u64");
    let limits = QuinnLegActorLimits::new(
        LegIoLimits::new(frame_bytes, 1, frame_bytes_u64, 1, frame_bytes_u64)
            .expect("finite LegIo lifetime bounds"),
        QuinnLegActorQueueLimits::new(1, frame_bytes).expect("sole queue holds one ordered frame"),
        QuinnLegActorLiveLimits::new(
            1,
            frame_bytes,
            2,
            frame_bytes.checked_mul(3).expect("three-frame byte bound"),
            3,
            3,
            RECORD_HEADER_BYTES
                .checked_mul(3)
                .expect("three ACK-header byte bound"),
        )
        .expect("three ordered records have independent live bounds"),
    )
    .expect("consistent root-drain test limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x77; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x78; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_bind: SocketAddr = "127.0.0.1:0".parse().expect("loopback peer address");
    let peer_server_config = QuinnLegActorServerConfig::new(
        peer_bind,
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("valid raw peer server config");
    let bootstrap_server = super::build_server_quinn_config(
        &peer_server_config,
        QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits"),
    )
    .expect("build production bootstrap server config");
    let actor_server = super::build_server_quinn_config(&peer_server_config, limits)
        .expect("build asymmetric production actor server config");
    let peer_endpoint = quinn::Endpoint::server(bootstrap_server, peer_bind)
        .expect("bind raw Quinn blackhole peer");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        peer_endpoint.local_addr().expect("raw peer local address"),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client config");
    let (client, peer) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            async {
                let incoming = peer_endpoint
                    .accept()
                    .await
                    .expect("raw peer accepts one Incoming");
                let connecting = incoming
                    .accept_with(std::sync::Arc::new(actor_server))
                    .expect("raw peer selects production actor config");
                let connection = connecting.await.expect("raw peer completes full 1-RTT");
                super::validate_alpn(&connection).expect("raw peer validates exact ALPN");
                let exporter = super::export_binding(&connection, peer_policy)
                    .expect("raw peer derives production exporter binding");
                super::binding_confirmation(
                    &connection,
                    super::ActorRole::Owner,
                    LegId::A,
                    peer_policy,
                    &exporter,
                )
                .await
                .expect("raw peer completes production HMAC prelude");
                let streams =
                    super::bootstrap_streams(&connection, super::ActorRole::Owner, LegId::A)
                        .await
                        .expect("raw peer completes fixed production streams");
                (connection, streams)
            },
        )
    })
    .await
    .expect("client and raw peer bootstrap are bounded");
    let mut client = client.expect("publish client actor");
    let (peer_connection, peer_streams) = peer;
    let super::ActorStreams {
        mut data_send,
        mut delivered_send,
        mut data_recv,
        mut delivered_recv,
    } = peer_streams;
    let route = WireRoute::new(LegId::A, WireDirection::OwnerToClient);

    let first_header = super::WireHeader::data(route, WireLane::Control, 1, frame_bytes, true);
    data_send
        .write_all(&first_header.encode().expect("encode first ordered header"))
        .await
        .expect("raw Owner writes first ordered header");
    data_send
        .write_all(&payload)
        .await
        .expect("raw Owner writes first complete payload");
    let first_inbound =
        tokio::time::timeout(TEST_TIMEOUT, client.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("first classification is bounded")
            .expect("first ordered record classifies")
            .expect("first ordered record reaches the composition owner");
    let mut first_ack = [0_u8; RECORD_HEADER_BYTES];
    tokio::time::timeout(TEST_TIMEOUT, delivered_recv.read_exact(&mut first_ack))
        .await
        .expect("first ACK observation is bounded")
        .expect("raw Owner reads the first real DELIVERED record");
    assert_eq!(
        first_ack,
        super::WireHeader::delivered(first_header)
            .encode()
            .expect("encode expected first DELIVERED record"),
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = client.actor_snapshot();
            if snapshot.queued_inbound_messages() == 0
                && snapshot.inbound_owned_bytes() == frame_bytes
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first wrapper alone retains ownership after its real ACK");

    let second_header = super::WireHeader::data(route, WireLane::Control, 2, frame_bytes, true);
    data_send
        .write_all(
            &second_header
                .encode()
                .expect("encode second ordered header"),
        )
        .await
        .expect("raw Owner writes second ordered header");
    data_send
        .write_all(&payload)
        .await
        .expect("raw Owner writes second complete payload");
    let third_header = super::WireHeader::data(route, WireLane::Control, 3, frame_bytes, true);
    data_send
        .write_all(&third_header.encode().expect("encode third ordered header"))
        .await
        .expect("raw Owner writes only the third ordered header");
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = client.actor_snapshot();
            if snapshot.validated_data_headers() == 3
                && snapshot.queued_inbound_messages() == 1
                && snapshot.inbound_owned_bytes() == frame_bytes * 3
                && snapshot.composition_owned_messages() == 3
                && snapshot.reserved_delivery_acks() == 2
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second is queued while third holds every pre-payload permit");

    client.request_cancel();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = client.actor_snapshot();
            if snapshot.queued_inbound_messages() == 0
                && snapshot.inbound_owned_bytes() == frame_bytes
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("root drains the queued second record after the third pre-close permit drops");

    let mut first_join = Box::pin(client.join());
    let first_poll = std::future::poll_fn(|context| {
        std::task::Poll::Ready(std::future::Future::poll(first_join.as_mut(), context))
    })
    .await;
    assert!(matches!(first_poll, std::task::Poll::Pending));
    drop(first_join);
    drop(first_inbound);

    let client_joined = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("retry join observes the root after the first wrapper releases")
        .expect("retry join remains a clean local cancellation");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(
        client_joined.terminal_reason(),
        LegTransportTerminalReason::LocalCancelled,
    );
    let after_join = client.actor_snapshot();
    assert_eq!(after_join.queued_inbound_messages(), 0);
    assert_eq!(after_join.inbound_owned_bytes(), 0);
    assert_eq!(after_join.composition_owned_messages(), 0);
    assert_eq!(after_join.reserved_delivery_acks(), 0);
    assert_eq!(after_join.outbound_owned_messages(), 0);
    assert_eq!(after_join.outbound_owned_bytes(), 0);
    let mut escaped_ack = [0_u8; RECORD_HEADER_BYTES];
    assert!(
        tokio::time::timeout(TEST_TIMEOUT, delivered_recv.read_exact(&mut escaped_ack))
            .await
            .expect("post-cancel ACK observation is bounded")
            .is_err(),
        "neither the queued second nor header-only third record may be acknowledged",
    );
    let _ = data_send.reset(CLOSE_CODE);
    let _ = delivered_send.reset(CLOSE_CODE);
    let _ = data_recv.stop(CLOSE_CODE);
    let _ = delivered_recv.stop(CLOSE_CODE);
    drop((data_send, delivered_send, data_recv, delivered_recv));
    peer_connection.close(CLOSE_CODE, b"raw peer cleanup");
    peer_endpoint.close(CLOSE_CODE, b"raw peer cleanup");
    tokio::time::timeout(TEST_TIMEOUT, peer_endpoint.wait_idle())
        .await
        .expect("raw peer endpoint cleanup is bounded");
}

#[tokio::test(flavor = "current_thread")]
async fn data_writer_cancel_interrupts_an_actually_pending_ordered_quic_write() {
    let ordered = ordered_control(11);
    let ordered_bytes = ordered
        .encode(STANDBY_CONTROL_V1)
        .expect("legal ordered control encoding")
        .len();
    let ordered_bytes_u64 = u64::try_from(ordered_bytes).expect("payload bound fits u64");
    let limits = QuinnLegActorLimits::new(
        LegIoLimits::new(ordered_bytes, 1, ordered_bytes_u64, 1, ordered_bytes_u64)
            .expect("finite LegIo lifetime bounds"),
        QuinnLegActorQueueLimits::new(1, ordered_bytes)
            .expect("sole queue holds one ordered frame"),
        QuinnLegActorLiveLimits::new(1, ordered_bytes, 1, ordered_bytes, 1, 1, 18)
            .expect("one ordered record has independent live bounds"),
    )
    .expect("consistent flow-control test limits");
    assert_eq!(limits.max_frame_bytes(), ordered_bytes);

    const RAW_PEER_FRAME_BYTES: usize = 1;
    let raw_peer_limits = QuinnLegActorLimits::new(
        LegIoLimits::new(RAW_PEER_FRAME_BYTES, 1, 1, 1, 1)
            .expect("finite raw-peer lifetime bounds"),
        QuinnLegActorQueueLimits::new(1, RAW_PEER_FRAME_BYTES)
            .expect("minimal raw-peer queue bound"),
        QuinnLegActorLiveLimits::new(
            1,
            RAW_PEER_FRAME_BYTES,
            1,
            RAW_PEER_FRAME_BYTES,
            1,
            1,
            RECORD_HEADER_BYTES,
        )
        .expect("minimal raw-peer live bounds"),
    )
    .expect("consistent asymmetric raw-peer limits");
    let raw_peer_stream_window = RAW_PEER_FRAME_BYTES
        .checked_add(RECORD_HEADER_BYTES)
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(STREAM_PREFACE_BYTES))
        .expect("raw-peer stream window calculation is bounded");
    assert!(raw_peer_stream_window >= 32, "HMAC prelude must fit");
    assert!(raw_peer_stream_window >= STREAM_PREFACE_BYTES);
    assert!(
        raw_peer_stream_window < RECORD_HEADER_BYTES + ordered_bytes,
        "raw DATA stream window must be smaller than the sole ordered record",
    );

    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x75; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x76; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_bind: SocketAddr = "127.0.0.1:0".parse().expect("loopback peer address");
    let peer_server_config = QuinnLegActorServerConfig::new(
        peer_bind,
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("valid raw peer server config");
    let bootstrap_server = super::build_server_quinn_config(
        &peer_server_config,
        QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits"),
    )
    .expect("build production bootstrap server config");
    let actor_server = super::build_server_quinn_config(&peer_server_config, raw_peer_limits)
        .expect("build asymmetric production actor server config");
    let peer_endpoint = quinn::Endpoint::server(bootstrap_server, peer_bind)
        .expect("bind raw Quinn blackhole peer");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        peer_endpoint.local_addr().expect("raw peer local address"),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client config");
    let (client, peer) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            async {
                let incoming = peer_endpoint
                    .accept()
                    .await
                    .expect("raw peer accepts one Incoming");
                let connecting = incoming
                    .accept_with(std::sync::Arc::new(actor_server))
                    .expect("raw peer selects production actor config");
                let connection = connecting.await.expect("raw peer completes full 1-RTT");
                super::validate_alpn(&connection).expect("raw peer validates exact ALPN");
                let exporter = super::export_binding(&connection, peer_policy)
                    .expect("raw peer derives production exporter binding");
                super::binding_confirmation(
                    &connection,
                    super::ActorRole::Owner,
                    LegId::A,
                    peer_policy,
                    &exporter,
                )
                .await
                .expect("raw peer completes production HMAC prelude");
                let streams =
                    super::bootstrap_streams(&connection, super::ActorRole::Owner, LegId::A)
                        .await
                        .expect("raw peer completes fixed production streams");
                (connection, streams)
            },
        )
    })
    .await
    .expect("client and raw peer bootstrap are bounded");
    let mut client = client.expect("publish client actor");
    let (peer_connection, peer_streams) = peer;

    let receipt = client
        .enqueue_ordered_leg_control(ordered, STANDBY_CONTROL_V1)
        .expect("sole queue admits the only ordered control frame");
    assert!(receipt.queue_is_live());
    assert_eq!(
        client
            .try_flush(SimTime::ZERO)
            .expect("bounded actor admission"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    assert!(!receipt.was_delivered());

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = client.actor_snapshot();
            if snapshot.blocked_write()
                == Some(TestPendingWrite {
                    ordinal: 1,
                    kind: TestWriteKind::DataPayload,
                })
                && snapshot.pending_delivery_count() == 1
                && snapshot.pending_delivery_ordinal() == Some(1)
                && !receipt.was_delivered()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the sole ordered payload write actually polls Pending under real QUIC flow control");
    let client_blocked = client.actor_snapshot();
    assert_eq!(
        client_blocked.blocked_write(),
        Some(TestPendingWrite {
            ordinal: 1,
            kind: TestWriteKind::DataPayload,
        }),
    );
    assert_eq!(client_blocked.pending_delivery_count(), 1);
    assert_eq!(client_blocked.pending_delivery_ordinal(), Some(1));
    assert_eq!(client_blocked.outbound_owned_messages(), 1);
    assert_eq!(client_blocked.outbound_owned_bytes(), ordered_bytes);
    assert!(!receipt.was_delivered());

    let (client_joined, actual_terminal) =
        ordered_write_cancel::cancel_and_settle_blocked_write(&mut client).await;
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(
        actual_terminal.reason(),
        LegTransportTerminalReason::LocalCancelled,
    );
    let after_join = client.actor_snapshot();
    assert_eq!(
        after_join.blocked_write(),
        Some(TestPendingWrite {
            ordinal: 1,
            kind: TestWriteKind::DataPayload,
        }),
        "the read-only probe must retain proof that the real write polled Pending",
    );
    assert_eq!(after_join.pending_delivery_count(), 0);
    assert_eq!(after_join.pending_delivery_ordinal(), None);
    assert_eq!(after_join.outbound_owned_messages(), 0);
    assert_eq!(after_join.outbound_owned_bytes(), 0);
    assert!(!receipt.was_delivered());
    assert!(!receipt.queue_is_live());

    let super::ActorStreams {
        mut data_send,
        mut delivered_send,
        mut data_recv,
        mut delivered_recv,
    } = peer_streams;
    let _ = data_send.reset(CLOSE_CODE);
    let _ = delivered_send.reset(CLOSE_CODE);
    let _ = data_recv.stop(CLOSE_CODE);
    let _ = delivered_recv.stop(CLOSE_CODE);
    drop((data_send, delivered_send, data_recv, delivered_recv));
    peer_connection.close(CLOSE_CODE, b"raw peer cleanup");
    peer_endpoint.close(CLOSE_CODE, b"raw peer cleanup");
    tokio::time::timeout(TEST_TIMEOUT, peer_endpoint.wait_idle())
        .await
        .expect("raw peer endpoint cleanup is bounded");
}

#[tokio::test(flavor = "current_thread")]
async fn child_fatal_is_published_before_a_queued_ordered_record_can_acknowledge() {
    let ordered = ordered_control(13);
    let payload = ordered
        .encode(STANDBY_CONTROL_V1)
        .expect("legal ordered control encoding");
    let payload_bytes = payload.len();
    let payload_bytes_u64 = u64::try_from(payload_bytes).expect("payload bound fits u64");
    let limits = QuinnLegActorLimits::new(
        LegIoLimits::new(payload_bytes, 1, payload_bytes_u64, 1, payload_bytes_u64)
            .expect("finite one-frame lifetime bounds"),
        QuinnLegActorQueueLimits::new(1, payload_bytes).expect("sole queue holds one frame"),
        QuinnLegActorLiveLimits::new(1, payload_bytes, 1, payload_bytes, 1, 1, 64)
            .expect("independent live ownership bounds"),
    )
    .expect("actor and LegIo bounds agree");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x45; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x46; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_bind: SocketAddr = "127.0.0.1:0".parse().expect("loopback peer address");
    let peer_server_config = QuinnLegActorServerConfig::new(
        peer_bind,
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("valid raw peer server config");
    let bootstrap_server = super::build_server_quinn_config(
        &peer_server_config,
        QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits"),
    )
    .expect("build production bootstrap server config");
    let actor_server = super::build_server_quinn_config(&peer_server_config, limits)
        .expect("build production actor server config");
    let peer_endpoint =
        quinn::Endpoint::server(bootstrap_server, peer_bind).expect("bind raw Quinn peer");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        peer_endpoint.local_addr().expect("raw peer local address"),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client config");
    let (client, peer) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            async {
                let incoming = peer_endpoint
                    .accept()
                    .await
                    .expect("raw peer accepts one Incoming");
                let connecting = incoming
                    .accept_with(std::sync::Arc::new(actor_server))
                    .expect("raw peer selects production actor config");
                let connection = connecting.await.expect("raw peer completes full 1-RTT");
                super::validate_alpn(&connection).expect("raw peer validates exact ALPN");
                let exporter = super::export_binding(&connection, peer_policy)
                    .expect("raw peer derives production exporter binding");
                super::binding_confirmation(
                    &connection,
                    super::ActorRole::Owner,
                    LegId::A,
                    peer_policy,
                    &exporter,
                )
                .await
                .expect("raw peer completes production HMAC prelude");
                let streams =
                    super::bootstrap_streams(&connection, super::ActorRole::Owner, LegId::A)
                        .await
                        .expect("raw peer completes fixed production streams");
                (connection, streams)
            },
        )
    })
    .await
    .expect("client and raw peer bootstrap are bounded");
    let mut client = client.expect("publish client actor");
    let (peer_connection, peer_streams) = peer;
    let super::ActorStreams {
        mut data_send,
        mut delivered_send,
        mut data_recv,
        mut delivered_recv,
    } = peer_streams;

    let inbound_route = WireRoute::new(LegId::A, WireDirection::OwnerToClient);
    let data_header =
        super::WireHeader::data(inbound_route, WireLane::Control, 1, payload_bytes, true)
            .encode()
            .expect("encode legal ordered DATA header");
    data_send
        .write_all(&data_header)
        .await
        .expect("raw peer writes legal DATA header");
    data_send
        .write_all(&payload)
        .await
        .expect("raw peer writes legal ordered payload");
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = client.actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.inbound_owned_bytes() == payload_bytes
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordered DATA reaches bounded actor ingress");
    let flush_frame = ordered_control(14);
    let flush_receipt = client
        .enqueue_ordered_leg_control(flush_frame, STANDBY_CONTROL_V1)
        .expect("one outbound ordered frame enters the sole bounded queue");
    assert!(flush_receipt.queue_is_live());
    assert!(!flush_receipt.was_delivered());
    let before_flush = client.actor_snapshot();
    assert_eq!(before_flush.outbound_queue_messages(), 1);
    assert_eq!(before_flush.outbound_queue_bytes(), payload_bytes);
    let before_leg_io = client
        .io
        .as_ref()
        .expect("running actor retains its sole LegIo")
        .counters();

    let flush_gate = std::sync::Arc::clone(&client.port_linearization_gate);
    let child_gate = std::sync::Arc::clone(&client.child_publication_gate);
    let first_failure = std::sync::Arc::clone(&client.first_failure);
    let flush_release = flush_gate.arm();
    child_gate.arm();
    let flush_task = tokio::task::spawn_blocking(move || {
        let result = client.try_flush(SimTime::ZERO);
        (client, result)
    });
    tokio::time::timeout(TEST_TIMEOUT, flush_gate.wait_until_blocked())
        .await
        .expect("flush pauses after its old clear observation and before queue commit");

    let illegal_delivered_header =
        super::WireHeader::data(inbound_route, WireLane::Control, 2, payload_bytes, false)
            .encode()
            .expect("encode DATA header for the wrong fixed stream");
    delivered_send
        .write_all(&illegal_delivered_header)
        .await
        .expect("raw peer writes malformed DELIVERED-stream record");
    tokio::time::timeout(TEST_TIMEOUT, child_gate.wait_until_blocked())
        .await
        .expect("root pauses after child exit and first-cause publication");
    let published_cause = first_failure
        .current()
        .expect("fixed child publishes its exact cause before root observation");
    assert!(matches!(
        published_cause.as_ref(),
        QuinnLegActorError::Protocol("DATA appeared on DELIVERED stream")
    ));
    let first_cause = std::sync::Arc::as_ptr(&published_cause);

    flush_release.release();
    let (mut client, flush_result) = tokio::time::timeout(TEST_TIMEOUT, flush_task)
        .await
        .expect("blocking flush owner returns after gate release")
        .expect("blocking flush owner task joins");
    let flush_cause = match flush_result {
        Err(QuinnLegActorError::RootFailed(cause)) => cause,
        outcome => panic!("child fatal crossed the flush commit boundary: {outcome:?}"),
    };
    assert_eq!(std::sync::Arc::as_ptr(&flush_cause), first_cause);
    assert_eq!(
        client.actor_snapshot(),
        before_flush,
        "suppressed flush must not mutate actor queue or ownership counters",
    );
    assert_eq!(
        client
            .io
            .as_ref()
            .expect("failed actor retains its sole LegIo")
            .counters(),
        before_leg_io,
        "suppressed flush must not mutate LegIo counters",
    );
    assert!(flush_receipt.queue_is_live());
    assert!(!flush_receipt.was_delivered());
    let readiness = client.readiness_epoch();
    let wait_first_poll = {
        let mut wait = Box::pin(client.wait_for_progress(readiness));
        std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(wait.as_mut(), context))
        })
        .await
    };
    let wait_cause = match wait_first_poll {
        std::task::Poll::Ready(Err(QuinnLegActorError::RootFailed(cause))) => cause,
        outcome => panic!("fatal Closing wait did not fail on first poll: {outcome:?}"),
    };
    assert_eq!(std::sync::Arc::as_ptr(&wait_cause), first_cause);
    let recv_first_poll = {
        let mut recv = Box::pin(client.recv_classified(STANDBY_CONTROL_V1));
        std::future::poll_fn(|context| {
            std::task::Poll::Ready(std::future::Future::poll(recv.as_mut(), context))
        })
        .await
    };
    let recv_cause = match recv_first_poll {
        std::task::Poll::Ready(Err(QuinnLegActorError::RootFailed(cause))) => cause,
        outcome => panic!("fatal Closing receive did not fail on first poll: {outcome:?}"),
    };
    assert_eq!(std::sync::Arc::as_ptr(&recv_cause), first_cause);
    assert_eq!(client.actor_snapshot(), before_flush);

    child_gate.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = client.actor_snapshot();
            if snapshot.queued_inbound_messages() == 0
                && snapshot.inbound_owned_bytes() == 0
                && snapshot.composition_owned_messages() == 0
                && snapshot.reserved_delivery_acks() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("root drains the handed-off fatal record without owner join polling");
    let (joined, actual_terminal) =
        ordered_write_cancel::settle_queued_fatal(&mut client, payload_bytes).await;
    assert_eq!(
        actual_terminal.reason(),
        LegTransportTerminalReason::FatalIo,
    );
    assert_eq!(
        std::ptr::from_ref(
            joined
                .terminal_failure()
                .expect("Joined retains the child fatal cause"),
        ),
        first_cause,
    );
    let released = client.actor_snapshot();
    assert_eq!(released.queued_inbound_messages(), 0);
    assert_eq!(released.inbound_owned_bytes(), 0);
    assert_eq!(released.composition_owned_messages(), 0);
    assert_eq!(released.reserved_delivery_acks(), 0);
    assert_eq!(released.outbound_queue_messages(), 0);
    assert_eq!(released.outbound_queue_bytes(), 0);
    assert!(
        !flush_receipt.queue_is_live(),
        "fatal child shutdown closes the endpoint after the flush is suppressed",
    );
    assert!(!flush_receipt.was_delivered());
    let mut escaped_ack = [0_u8; RECORD_HEADER_BYTES];
    assert!(
        tokio::time::timeout(TEST_TIMEOUT, delivered_recv.read_exact(&mut escaped_ack))
            .await
            .expect("raw peer ACK observation is bounded")
            .is_err(),
        "no complete DELIVERED record may escape after the child fatal",
    );

    let _ = data_send.reset(CLOSE_CODE);
    let _ = delivered_send.reset(CLOSE_CODE);
    let _ = data_recv.stop(CLOSE_CODE);
    let _ = delivered_recv.stop(CLOSE_CODE);
    drop((data_send, delivered_send, data_recv, delivered_recv));
    peer_connection.close(CLOSE_CODE, b"raw peer cleanup");
    peer_endpoint.close(CLOSE_CODE, b"raw peer cleanup");
    tokio::time::timeout(TEST_TIMEOUT, peer_endpoint.wait_idle())
        .await
        .expect("raw peer endpoint cleanup is bounded");
    drop(client);
    assert!(!flush_receipt.queue_is_live());
}

#[tokio::test(flavor = "current_thread")]
async fn classified_decode_failure_is_the_first_fatal_cause_and_never_acknowledges() {
    let frame = ordered_control(7);
    let encoded = frame
        .encode(STANDBY_CONTROL_V1)
        .expect("leg probe is legal when standby control is negotiated");
    let encoded_bytes = encoded.len();
    let encoded_bytes_u64 = u64::try_from(encoded_bytes).expect("encoded length fits u64");
    let limits = QuinnLegActorLimits::new(
        LegIoLimits::new(encoded_bytes, 1, encoded_bytes_u64, 1, encoded_bytes_u64)
            .expect("one-frame lifetime budgets are finite"),
        QuinnLegActorQueueLimits::new(1, encoded_bytes).expect("sole queue holds the exact frame"),
        QuinnLegActorLiveLimits::new(1, encoded_bytes, 1, encoded_bytes, 1, 1, 64)
            .expect("live ownership groups are independently bounded"),
    )
    .expect("lifetime and live actor bounds agree");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x43; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x44; 16]).expect("nonzero device principal"),
    );
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0"
            .parse::<SocketAddr>()
            .expect("valid loopback bind address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("valid dedicated-ALPN server configuration");
    let mut listener =
        tokio::time::timeout(TEST_TIMEOUT, QuinnLegActorListener::bind(server_config))
            .await
            .expect("listener bind is bounded")
            .expect("bind real Quinn listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0"
            .parse::<SocketAddr>()
            .expect("valid client loopback bind address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid dedicated-ALPN client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("full 1-RTT connect and accept are bounded");
    let mut client = client.expect("connect binding-agreed client actor");
    let mut server = server.expect("accept binding-agreed server actor");

    let receipt = client
        .enqueue_ordered_leg_control(frame, STANDBY_CONTROL_V1)
        .expect("client queue admits the legal ordered leg probe");
    assert_eq!(
        client.try_flush(SimTime::ZERO).expect("submit leg probe"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = server.actor_snapshot();
            if snapshot.queued_inbound_messages() == 1
                && snapshot.inbound_owned_bytes() == encoded_bytes
                && snapshot.composition_owned_messages() == 1
                && snapshot.reserved_delivery_acks() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordered probe reaches bounded server ingress");
    assert!(!receipt.was_delivered());

    let receive_error =
        tokio::time::timeout(TEST_TIMEOUT, server.recv_classified(FeatureSet::default()))
            .await
            .expect("classification failure is bounded")
            .expect_err("unnegotiated standby control must fail classification");
    let QuinnLegActorError::RootFailed(receive_cause) = &receive_error else {
        panic!("classification must return the shared first cause: {receive_error:?}");
    };
    assert!(matches!(
        receive_cause.as_ref(),
        QuinnLegActorError::LegIo(LegIoError::Decode(
            ProtocolError::StandbyControlNotNegotiated { negotiated: 0 }
        ))
    ));
    let receive_cause = std::sync::Arc::as_ptr(receive_cause);
    assert!(
        *server.cancel.borrow(),
        "the exact first cause must be latched before fatal cancellation is signalled",
    );
    let released = server.actor_snapshot();
    assert_eq!(released.queued_inbound_messages(), 0);
    assert_eq!(released.inbound_owned_bytes(), 0);
    assert_eq!(released.composition_owned_messages(), 0);
    assert_eq!(released.reserved_delivery_acks(), 0);
    let leg_io = server
        .io
        .as_ref()
        .expect("failed actor retains its sole LegIo for causal inspection")
        .counters();
    assert_eq!(leg_io.inbound_messages, 1);
    assert_eq!(leg_io.inbound_bytes, encoded_bytes_u64);
    assert_eq!(leg_io.bound_messages, 0);
    assert_eq!(leg_io.decode_rejections, 1);
    assert!(!receipt.was_delivered());

    let server_joined = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("fatal server join is bounded")
        .expect("classified decode failure still joins after structural cleanup");
    assert_eq!(
        server_joined.terminal_reason(),
        LegTransportTerminalReason::FatalIo,
    );
    assert_eq!(
        std::ptr::from_ref(
            server_joined
                .terminal_failure()
                .expect("Joined retains the classified first cause"),
        ),
        receive_cause,
    );
    let second_join_error = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("server join retry is bounded")
        .expect_err("a structural Joined witness is minted exactly once");
    assert!(matches!(
        second_join_error,
        QuinnLegActorError::AlreadyJoined
    ));
    assert!(!receipt.was_delivered());

    client.request_cancel();
    let _client_join = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("client cleanup join is bounded")
        .expect("peer client completes structural cleanup");
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("semantic fatal does not poison a structurally cleaned listener");
}

#[tokio::test(flavor = "current_thread")]
async fn real_loopback_actor_marks_ordered_delivery_only_after_peer_admission_and_mints_join_only_after_clean_await()
 {
    let first = ordered_control(1);
    let expected_first = first.clone();
    let second = ordered_control(2);
    let first_encoded = first
        .encode(STANDBY_CONTROL_V1)
        .expect("valid first control encoding");
    let second_encoded = second
        .encode(STANDBY_CONTROL_V1)
        .expect("valid second control encoding");
    assert_eq!(first_encoded.len(), second_encoded.len());

    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x41; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x42; 16]).expect("nonzero device principal"),
    );
    let encoded_bytes = first_encoded.len();
    let encoded_bytes_u64 = u64::try_from(encoded_bytes).expect("encoded length fits u64");
    let lifetime_bytes = encoded_bytes_u64
        .checked_mul(2)
        .expect("two-frame lifetime byte budget fits u64");
    let leg_io_limits = LegIoLimits::new(encoded_bytes, 2, lifetime_bytes, 2, lifetime_bytes)
        .expect("two-frame LegIo lifetime budgets are finite");
    let queue_limits = QuinnLegActorQueueLimits::new(1, encoded_bytes)
        .expect("sole queue holds one encoded frame");
    let actor_live_limits =
        QuinnLegActorLiveLimits::new(1, encoded_bytes, 1, encoded_bytes, 1, 1, 64)
            .expect("outbound, ingress, handoff, and ACK ownership fit independently");
    let limits = QuinnLegActorLimits::new(leg_io_limits, queue_limits, actor_live_limits)
        .expect("lifetime and live actor bounds agree");
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0"
            .parse::<SocketAddr>()
            .expect("valid loopback bind address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        2,
    )
    .expect("valid dedicated-ALPN server configuration");
    let mut listener =
        tokio::time::timeout(TEST_TIMEOUT, QuinnLegActorListener::bind(server_config))
            .await
            .expect("listener bind is bounded")
            .expect("bind real Quinn listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0"
            .parse::<SocketAddr>()
            .expect("valid client loopback bind address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid dedicated-ALPN client configuration");

    // Quinn starts the server handshake only after Incoming is accepted,
    // so connect and accept must progress concurrently. Peer pressure is
    // created later by retaining an exact inbound byte permit.
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("full 1-RTT connect and accept are bounded");
    let mut client = client.expect("complete full 1-RTT binding-agreed client actor");
    let mut server = server.expect("accept full 1-RTT binding-agreed server actor");

    let first_receipt = client
        .enqueue_ordered_leg_control(first, STANDBY_CONTROL_V1)
        .expect("sole queue admits the legitimate ordered frame");
    assert_eq!(
        client.try_flush(SimTime::ZERO).expect("submit first frame"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    assert!(!first_receipt.was_delivered());
    assert_eq!(
        client
            .try_flush(SimTime::ZERO)
            .expect("inspect ordered wait"),
        QuinnLegActorFlushDisposition::OrderedDeliveryPending,
        "QUIC submission and actor-channel admission are not peer dequeue",
    );

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = server.actor_snapshot();
            if snapshot.validated_data_headers() == 1
                && snapshot.queued_inbound_messages() == 1
                && snapshot.inbound_owned_bytes() == encoded_bytes
                && snapshot.reserved_delivery_acks() == 1
                && snapshot.composition_owned_messages() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first frame reaches bounded peer ingress with an ACK reservation");
    assert!(
        !first_receipt.was_delivered(),
        "actor-channel admission is not composition delivery",
    );
    let before_delivery = client.readiness_epoch();

    let admitted = tokio::time::timeout(TEST_TIMEOUT, server.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("first inbound admission is bounded")
        .expect("LegIo classifies the exact peer record")
        .expect("peer composition owner receives the exact first record");
    let inbound_counters = server
        .io
        .as_ref()
        .expect("the live actor still owns its sole LegIo")
        .counters();
    assert_eq!(inbound_counters.inbound_messages, 1);
    assert_eq!(inbound_counters.inbound_bytes, encoded_bytes_u64);
    assert_eq!(inbound_counters.bound_messages, 1);
    assert_eq!(inbound_counters.bound_bytes, encoded_bytes_u64);
    assert_eq!(inbound_counters.bound_leg_control_messages, 1);
    assert_eq!(server.inbound_owned_bytes(), encoded_bytes);
    assert_eq!(server.actor_snapshot().composition_owned_messages(), 1);

    tokio::time::timeout(TEST_TIMEOUT, async {
        while !first_receipt.was_delivered() || client.readiness_epoch() == before_delivery {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("peer delivery ACK advances readiness before waiter registration");
    let after_delivery =
        tokio::time::timeout(TEST_TIMEOUT, client.wait_for_progress(before_delivery))
            .await
            .expect("waiter registered after progress cannot lose the wake")
            .expect("live actor reports the observed readiness epoch");
    assert_ne!(after_delivery, before_delivery);
    assert_eq!(
        client
            .try_flush(SimTime::ZERO)
            .expect("consume the completed ordered wait"),
        QuinnLegActorFlushDisposition::Idle,
    );
    assert!(first_receipt.was_delivered());

    // The first admitted value owns the entire configured inbound byte
    // budget. The second ordered frame can reach the stream but cannot be
    // admitted, dequeued, or acknowledged until that exact value releases.
    let second_receipt = client
        .enqueue_ordered_leg_control(second, FeatureSet::STANDBY_CONTROL_V1)
        .expect("sole queue admits the second ordered frame");
    assert_eq!(
        client
            .try_flush(SimTime::ZERO)
            .expect("submit second frame"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    assert_eq!(
        client
            .try_flush(SimTime::ZERO)
            .expect("inspect held second frame"),
        QuinnLegActorFlushDisposition::OrderedDeliveryPending,
    );
    assert!(!second_receipt.was_delivered());

    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let snapshot = server.actor_snapshot();
            if snapshot.validated_data_headers() == 2
                && snapshot.waiting_inbound_bytes() == encoded_bytes
                && snapshot.inbound_owned_bytes() == encoded_bytes
                && snapshot.composition_owned_messages() == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second header reaches the exact held inbound-byte permit");
    tokio::select! {
        biased;
        value = server.recv_classified(STANDBY_CONTROL_V1) => panic!("second record bypassed inbound byte pressure: {value:?}"),
        () = tokio::task::yield_now() => {}
    }

    client.request_cancel();
    server.request_cancel();
    let server_counters = std::sync::Arc::clone(&server.counters);
    let mut client_join = Box::pin(client.join());
    let mut server_join = Box::pin(server.join());
    tokio::select! {
        biased;
        result = &mut server_join => panic!("server minted joined with inbound ownership held: {result:?}"),
        () = tokio::task::yield_now() => {}
    }

    admitted.consume_for_test(|inbound| match inbound {
        BoundInbound::LegControl(bound) => {
            assert_eq!(
                server_counters
                    .inbound_owned_bytes
                    .load(std::sync::atomic::Ordering::Acquire),
                encoded_bytes,
            );
            assert_eq!(
                server_counters
                    .composition_owned_messages
                    .load(std::sync::atomic::Ordering::Acquire),
                1,
            );
            let (_seal, received) = bound.into_parts();
            assert_eq!(received, expected_first);
        }
        BoundInbound::Session(_) => {
            panic!("leg-control bytes were misclassified as session")
        }
    });
    assert_eq!(
        server_counters
            .inbound_owned_bytes
            .load(std::sync::atomic::Ordering::Acquire),
        0,
    );
    assert_eq!(
        server_counters
            .composition_owned_messages
            .load(std::sync::atomic::Ordering::Acquire),
        0,
    );
    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(&mut client_join, &mut server_join)
    })
    .await
    .expect("both retained actor nurseries join within the local bound");
    let client_joined = client_joined.expect("cleanly await the client root and children");
    let server_joined = server_joined.expect("cleanly await the server root and children");
    assert!(!second_receipt.was_delivered());
    assert!(
        !second_receipt.queue_is_live(),
        "clean join drops the exact sole queue that minted the receipt",
    );
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);

    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("shared server endpoint idles only at listener shutdown");
}

#[tokio::test(flavor = "current_thread")]
async fn real_quinn_terminal_grace_exact_deadline_drains_inbound_then_expires_nonfatally() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xd1; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xd2; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind shared actor listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (accepted, owner_bootstrap) = complete_initial_actor_attach(&mut client, &mut server).await;
    wait_actor_ordered_delivery(&mut server).await;
    let mut composition =
        QuinnClientSessionComposition::bootstrap_initial(actor_session_config(), accepted, client)
            .expect("composition consumes exact current A");
    let factory = composition
        .mint_tcp_port_factory(FlowPortConfig::new(64, 8, 8, 8).expect("valid TCP source"))
        .expect("running A admits the sole TCP factory")
        .expect("pristine reducer mints the TCP factory");

    let mut opened = Vec::new();
    for port in [443_u16, 444_u16, 445_u16, 446_u16] {
        let effects = composition
            .open_local(TargetAddr::IpPort(
                format!("127.0.0.1:{port}")
                    .parse()
                    .expect("loopback target"),
            ))
            .expect("composition retains current A")
            .expect("local open is accepted");
        opened.push(
            effects
                .into_iter()
                .find_map(|effect| match effect {
                    SessionEffect::LocalFlowOpened { flow } => Some(flow),
                    _ => None,
                })
                .expect("local open mints exact flow authority"),
        );
    }

    let (terminal_source, mut terminal_driver) = factory
        .open_flow(opened[0])
        .expect("factory installs first flow source");
    terminal_source
        .try_send_reset(crate::resumable::ResetReason::LocalAbandon)
        .expect("first source emits one exact local reset");
    let input = terminal_driver
        .try_recv_next_for_client_controller()
        .expect("driver source remains live")
        .expect("local reset is ready");
    let terminal = match composition
        .apply_driver_input(input)
        .expect("running actor admits the typed reset")
    {
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Applied(effects) => {
            effects
                .into_iter()
                .find_map(|effect| match effect {
                    SessionEffect::FlowFinished { terminal, .. } => Some(terminal),
                    _ => None,
                })
                .expect("local reset emits one terminal grace capability")
        }
        _ => panic!("small local reset unexpectedly failed or met pressure"),
    };
    assert_eq!(composition.snapshot().session.terminal_tombstones(), 1);

    let session_clock = composition.session_clock();
    let deadline = session_clock
        .now()
        .expect("terminal-grace session clock remains representable")
        .checked_add(Duration::from_millis(500))
        .expect("bounded terminal-grace deadline");
    let timer = QuinnTerminalGraceTimer::new(terminal, deadline);
    let duplicate_timer = QuinnTerminalGraceTimer::new(terminal, deadline);
    let timer = match composition.try_receive_current_before_terminal_grace(
        timer,
        SimTime::from_nanos(deadline.as_nanos() - 1),
    ) {
        QuinnTerminalGraceTimerDrive::NotDue(timer) => timer,
        _ => panic!("deadline-1ns changed terminal ownership"),
    };
    assert_eq!(timer.instant(), deadline);

    let generation = composition.snapshot().session.generation();
    let open_result = Frame::try_new(
        generation,
        Record::OpenResult {
            flow_id: opened[1].flow_id(),
            result: OpenResultCode::Opened,
        },
    )
    .expect("second flow OPEN_RESULT is valid");
    let publication_block = composition.arm_current_inbound_publication_block_for_test();
    server
        .queue
        .as_mut()
        .expect("server retains sole FIFO")
        .push(open_result)
        .expect("server queues exact OPEN_RESULT");
    assert_eq!(
        server
            .try_flush(deadline)
            .expect("OPEN_RESULT flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, publication_block.wait_until_blocked())
        .await
        .expect("complete OPEN_RESULT reserves current publication");
    let timer = match composition.try_receive_current_before_terminal_grace(timer, deadline) {
        QuinnTerminalGraceTimerDrive::PublicationPending(timer) => timer,
        _ => panic!("terminal timer crossed an in-flight exact-deadline publication"),
    };
    assert_eq!(timer.instant(), deadline);

    publication_block.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while composition
            .current_actor_snapshot()
            .queued_inbound_messages()
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reserved OPEN_RESULT publishes within the local bound");
    let timer = tokio::time::timeout(TEST_TIMEOUT, async {
        let mut timer = timer;
        loop {
            match composition.try_receive_current_before_terminal_grace(timer, deadline) {
                QuinnTerminalGraceTimerDrive::Receive {
                    outcome: QuinnCurrentInboundDrive::Applied { effects, .. },
                    timer: returned,
                } => {
                    assert!(effects.iter().any(|effect| matches!(
                        effect,
                        SessionEffect::LocalOpenResolved { flow_id, result }
                            if *flow_id == opened[1].flow_id()
                                && *result == OpenResultCode::Opened
                    )));
                    break returned;
                }
                QuinnTerminalGraceTimerDrive::PublicationPending(returned) => {
                    timer = returned;
                    tokio::task::yield_now().await;
                }
                QuinnTerminalGraceTimerDrive::Receive { outcome, .. } => match outcome {
                    QuinnCurrentInboundDrive::Idle => {
                        panic!("exact-deadline receive was idle")
                    }
                    QuinnCurrentInboundDrive::PolicyBlocked => {
                        panic!("exact-deadline receive was policy blocked")
                    }
                    QuinnCurrentInboundDrive::ProbeAcked { .. } => {
                        panic!("OPEN_RESULT was misclassified as probe ACK")
                    }
                    QuinnCurrentInboundDrive::Rejected => {
                        panic!("exact OPEN_RESULT was rejected")
                    }
                    QuinnCurrentInboundDrive::ReducerRejected(error) => {
                        panic!("exact OPEN_RESULT reducer rejected: {error}")
                    }
                    QuinnCurrentInboundDrive::SessionShuttingDown => {
                        panic!("session shut down before exact OPEN_RESULT")
                    }
                    QuinnCurrentInboundDrive::MissingCurrent => {
                        panic!("current disappeared before exact OPEN_RESULT")
                    }
                    QuinnCurrentInboundDrive::CompositionInvariant => {
                        panic!("composition rejected exact OPEN_RESULT")
                    }
                    QuinnCurrentInboundDrive::ActorFailed(error) => {
                        panic!("actor rejected exact OPEN_RESULT: {error}")
                    }
                    QuinnCurrentInboundDrive::Applied { .. } => unreachable!(),
                },
                QuinnTerminalGraceTimerDrive::NotDue(_) => {
                    panic!("exact deadline became not due")
                }
                QuinnTerminalGraceTimerDrive::Expired { .. } => {
                    panic!("timer expired before consuming queued OPEN_RESULT")
                }
                QuinnTerminalGraceTimerDrive::ResetDeliveryDeadlineWon(_) => {
                    panic!("untracked terminal timer acquired RESET delivery state")
                }
                QuinnTerminalGraceTimerDrive::ReducerInvariant { error, .. } => {
                    panic!("terminal reducer failed before inbound: {error}")
                }
                QuinnTerminalGraceTimerDrive::ActorFailure { cause, .. } => {
                    panic!("actor failed before exact OPEN_RESULT: {cause}")
                }
                QuinnTerminalGraceTimerDrive::SessionShuttingDown(_) => {
                    panic!("session shut down before exact OPEN_RESULT")
                }
                QuinnTerminalGraceTimerDrive::CompositionInvariant(_) => {
                    panic!("composition rejected exact OPEN_RESULT")
                }
            }
        }
    })
    .await
    .expect("released publication reaches a stable exact-deadline decision");
    assert_eq!(timer.instant(), deadline);
    assert_eq!(composition.snapshot().session.terminal_tombstones(), 1);

    tokio::time::timeout(TEST_TIMEOUT, async {
        while session_clock
            .now()
            .expect("terminal-grace session clock remains representable")
            <= deadline
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("post-deadline records are deliberately sent after terminal grace");

    let after_cutoff_open_result = Frame::try_new(
        generation,
        Record::OpenResult {
            flow_id: opened[2].flow_id(),
            result: OpenResultCode::Opened,
        },
    )
    .expect("post-cutoff OPEN_RESULT is valid");
    server
        .queue
        .as_mut()
        .expect("server retains sole FIFO")
        .push(after_cutoff_open_result)
        .expect("server queues post-cutoff OPEN_RESULT");
    assert_eq!(
        server
            .try_flush(deadline)
            .expect("post-cutoff OPEN_RESULT flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        while composition
            .current_actor_snapshot()
            .queued_inbound_messages()
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("post-cutoff OPEN_RESULT publishes within the local bound");

    let after_cutoff_in_flight = Frame::try_new(
        generation,
        Record::OpenResult {
            flow_id: opened[3].flow_id(),
            result: OpenResultCode::Opened,
        },
    )
    .expect("second post-cutoff OPEN_RESULT is valid");
    let after_cutoff_publication_block =
        composition.arm_current_inbound_publication_block_for_test();
    server
        .queue
        .as_mut()
        .expect("server retains sole FIFO")
        .push(after_cutoff_in_flight)
        .expect("server queues second post-cutoff OPEN_RESULT");
    assert_eq!(
        server
            .try_flush(deadline)
            .expect("second post-cutoff OPEN_RESULT flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(
        TEST_TIMEOUT,
        after_cutoff_publication_block.wait_until_blocked(),
    )
    .await
    .expect("second post-cutoff OPEN_RESULT reserves publication");

    match composition.try_receive_current_before_terminal_grace(timer, deadline) {
        QuinnTerminalGraceTimerDrive::Expired { effects } => assert!(effects.is_empty()),
        _ => panic!("post-cutoff inbound extended the absolute terminal grace deadline"),
    }
    assert_eq!(composition.snapshot().session.terminal_tombstones(), 0);
    after_cutoff_publication_block.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while composition
            .current_actor_snapshot()
            .queued_inbound_messages()
            != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both post-cutoff records remain actor-owned after timer commit");
    for expected in [opened[2].flow_id(), opened[3].flow_id()] {
        let outcome = tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                let outcome = composition.try_receive_current();
                if !matches!(outcome, QuinnCurrentInboundDrive::Idle) {
                    break outcome;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("post-cutoff publication reaches ordinary FIFO receive");
        match outcome {
            QuinnCurrentInboundDrive::Applied { effects, .. } => {
                assert!(effects.iter().any(|effect| matches!(
                    effect,
                    SessionEffect::LocalOpenResolved { flow_id, result }
                        if *flow_id == expected && *result == OpenResultCode::Opened
                )))
            }
            QuinnCurrentInboundDrive::Idle => {
                panic!("post-cutoff ordinary FIFO receive was idle")
            }
            QuinnCurrentInboundDrive::PolicyBlocked => {
                panic!("post-cutoff ordinary FIFO receive was policy blocked")
            }
            QuinnCurrentInboundDrive::ProbeAcked { .. } => {
                panic!("post-cutoff OPEN_RESULT was misclassified as probe ACK")
            }
            QuinnCurrentInboundDrive::Rejected => {
                panic!("post-cutoff OPEN_RESULT was rejected")
            }
            QuinnCurrentInboundDrive::ReducerRejected(error) => {
                panic!("post-cutoff OPEN_RESULT reducer rejected: {error}")
            }
            QuinnCurrentInboundDrive::SessionShuttingDown => {
                panic!("session shut down before post-cutoff ordinary receive")
            }
            QuinnCurrentInboundDrive::MissingCurrent => {
                panic!("current disappeared before post-cutoff ordinary receive")
            }
            QuinnCurrentInboundDrive::CompositionInvariant => {
                panic!("composition rejected post-cutoff ordinary receive")
            }
            QuinnCurrentInboundDrive::ActorFailed(error) => {
                panic!("actor rejected post-cutoff ordinary receive: {error}")
            }
        }
    }
    let duplicate_timer =
        match composition.try_receive_current_before_terminal_grace(duplicate_timer, deadline) {
            QuinnTerminalGraceTimerDrive::ReducerInvariant { timer, error } => {
                assert!(matches!(
                    error,
                    SessionSupervisorError::Reducer(SessionError::UnknownTerminalGrace)
                ));
                timer
            }
            _ => panic!("duplicate terminal timer was not a reducer invariant"),
        };
    assert_eq!(duplicate_timer.instant(), deadline);
    match composition.try_receive_current_before_terminal_grace(duplicate_timer, deadline) {
        QuinnTerminalGraceTimerDrive::ReducerInvariant { timer, error } => {
            assert_eq!(timer.instant(), deadline);
            assert!(matches!(
                error,
                SessionSupervisorError::Reducer(SessionError::UnknownTerminalGrace)
            ));
        }
        _ => panic!("duplicate terminal invariant did not retain exact timer evidence"),
    }
    assert_eq!(composition.snapshot().session.terminal_tombstones(), 0);
    assert!(matches!(
        composition.try_receive_current(),
        QuinnCurrentInboundDrive::Idle,
    ));

    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server.join())
    })
    .await
    .expect("terminal timer actors join within the local bound");
    let client_joined = client_joined.expect("client actor joins structurally");
    let server_joined = server_joined.expect("server actor joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    drop((
        composition,
        terminal_source,
        terminal_driver,
        factory,
        owner_bootstrap,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("terminal timer tracer releases the listener slot");
}

#[tokio::test(flavor = "current_thread")]
async fn real_quinn_inflight_publication_defers_deadline_and_exact_ack_wins() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xc1; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xc2; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind shared actor listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (accepted, owner_bootstrap) = complete_initial_actor_attach(&mut client, &mut server).await;
    wait_actor_ordered_delivery(&mut server).await;
    let mut composition =
        QuinnClientSessionComposition::bootstrap_initial(actor_session_config(), accepted, client)
            .expect("composition consumes exact current A");
    let before_probe = composition.snapshot();

    let queued = composition
        .begin_current_probe()
        .expect("one exact current probe enters A's ordered FIFO");
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("current probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound_probe =
        tokio::time::timeout(TEST_TIMEOUT, server.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("probe receive is bounded")
            .expect("server A remains live")
            .expect("server A receives the exact probe");
    let BoundInbound::LegControl(inbound_probe) = take_actor_inbound(inbound_probe) else {
        panic!("current probe was misclassified as a session frame");
    };
    let (_probe_seal, probe) = inbound_probe.into_parts();
    let LegControlRecord::LegProbe {
        session_id,
        leg_epoch_nonce,
        probe_sequence,
    } = probe.record().clone()
    else {
        panic!("current liveness emitted a non-probe control record");
    };
    let exact_ack = LegControlFrame::try_new(
        probe.leg_generation(),
        LegControlRecord::LegProbeAck {
            session_id,
            leg_epoch_nonce,
            probe_sequence,
        },
        STANDBY_CONTROL_V1,
    )
    .expect("matching current probe ACK is valid");
    let wrong_ack = LegControlFrame::try_new(
        probe.leg_generation(),
        LegControlRecord::LegProbeAck {
            session_id,
            leg_epoch_nonce,
            probe_sequence: probe_sequence
                .checked_next()
                .expect("test probe sequence can advance once"),
        },
        STANDBY_CONTROL_V1,
    )
    .expect("wrong-sequence current probe ACK is still valid wire input");
    let session_clock = composition.session_clock();
    let arm_at = session_clock
        .now()
        .expect("current-probe session clock remains representable");
    let probe_timeout = Duration::from_secs(2);
    let deadline = arm_at
        .checked_add(probe_timeout)
        .expect("bounded current-probe deadline");
    let token = tokio::time::timeout(TEST_TIMEOUT, async {
            let mut queued = queued;
            loop {
                match composition.arm_current_probe(
                    queued,
                    arm_at,
                    probe_timeout,
                ) {
                    Ok(token) => break token,
                    Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::NotDelivered(returned)) => {
                        queued = returned;
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("current probe deadline failed to arm: {error:?}"),
                }
            }
        })
        .await
        .expect("only the physically submitted probe arms within the local bound");
    assert_eq!(token.instant(), deadline);

    server
        .enqueue_ordered_leg_control(wrong_ack, STANDBY_CONTROL_V1)
        .expect("server A queues one stale ACK");
    assert_eq!(
        server
            .try_flush(SimTime::ZERO)
            .expect("stale probe ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let rejected = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => break outcome,
            }
        }
    })
    .await
    .expect("stale probe ACK is classified within the local bound");
    assert!(matches!(rejected, QuinnCurrentInboundDrive::Rejected));
    assert_eq!(composition.snapshot(), before_probe);
    wait_actor_ordered_delivery(&mut server).await;

    assert!(matches!(
        composition.try_receive_current(),
        QuinnCurrentInboundDrive::Idle,
    ));
    let publication_block = composition.arm_current_inbound_publication_block_for_test();

    server
        .enqueue_ordered_leg_control(exact_ack, STANDBY_CONTROL_V1)
        .expect("server A queues the matching ACK");
    assert_eq!(
        server
            .try_flush(SimTime::ZERO)
            .expect("probe ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, publication_block.wait_until_blocked())
        .await
        .expect("complete exact ACK reserves publication in the old Empty/commit window");
    assert!(
        session_clock
            .now()
            .expect("current-probe session clock remains representable")
            <= deadline,
        "the exact ACK must reserve publication before its absolute deadline",
    );
    let token = match composition.try_receive_current_before_probe_deadline(token, deadline) {
        QuinnCurrentProbeDeadlineDrive::PublicationPending(token) => token,
        _outcome => panic!("due deadline crossed an in-flight current publication"),
    };
    assert_eq!(token.instant(), deadline);

    let progress_cursor = composition.progress_cursor();
    let mut cancelled_wait = Box::pin(composition.wait_for_actor_progress(progress_cursor.clone()));
    let mut cancelled_context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        std::future::Future::poll(cancelled_wait.as_mut(), &mut cancelled_context),
        std::task::Poll::Pending,
    ));
    drop(cancelled_wait);
    assert_eq!(token.instant(), deadline);

    publication_block.release();
    assert!(matches!(
        tokio::time::timeout(
            TEST_TIMEOUT,
            composition.wait_for_actor_progress(progress_cursor),
        )
        .await
        .expect("publishing the reserved ACK wakes the cancelled wait's replacement")
        .expect("current actor readiness remains live"),
        QuinnCompositionProgress::Current { .. },
    ));
    assert_eq!(
        composition
            .current_actor_snapshot()
            .queued_inbound_messages(),
        1,
    );
    assert!(matches!(
        composition.try_receive_current_before_probe_deadline(token, deadline),
        QuinnCurrentProbeDeadlineDrive::ProbeAcked {
            deadline: returned,
            generation,
            probe_sequence: observed,
        } if returned.instant() == deadline
            && generation == probe.leg_generation()
            && observed == probe_sequence
    ));
    assert_eq!(composition.snapshot(), before_probe);
    assert_eq!(composition.snapshot(), before_probe);

    let second = composition
        .begin_current_probe()
        .expect("a completed probe permits one next monotonic probe");
    assert!(second.probe_sequence() > probe_sequence);
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("second current probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let second_probe =
        tokio::time::timeout(TEST_TIMEOUT, server.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("second probe receive is bounded")
            .expect("server A remains live for a second probe")
            .expect("server A receives the second exact probe");
    let BoundInbound::LegControl(second_probe) = take_actor_inbound(second_probe) else {
        panic!("second current probe was misclassified");
    };
    let (_second_seal, second_probe) = second_probe.into_parts();
    let LegControlRecord::LegProbe {
        session_id: second_session,
        leg_epoch_nonce: second_epoch,
        probe_sequence: second_sequence,
    } = second_probe.record().clone()
    else {
        panic!("second current liveness record was not a probe");
    };
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !composition.current_probe_writer_completed_for_test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second probe writer completion becomes observable before arming");
    wait_actor_ordered_delivery(&mut server).await;
    let second_ack = LegControlFrame::try_new(
        second_probe.leg_generation(),
        LegControlRecord::LegProbeAck {
            session_id: second_session,
            leg_epoch_nonce: second_epoch,
            probe_sequence: second_sequence,
        },
        STANDBY_CONTROL_V1,
    )
    .expect("second exact ACK is valid");
    server
        .enqueue_ordered_leg_control(second_ack, STANDBY_CONTROL_V1)
        .expect("server A queues ACK before the client arms a timer");
    assert_eq!(
        server
            .try_flush(SimTime::ZERO)
            .expect("pre-arm ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let prearm_acked = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_current() {
                QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                outcome => break outcome,
            }
        }
    })
    .await
    .expect("pre-arm ACK is classified within the local bound");
    assert!(matches!(
        prearm_acked,
        QuinnCurrentInboundDrive::ProbeAcked {
            generation,
            probe_sequence,
        } if generation == second_probe.leg_generation()
            && probe_sequence == second_sequence
    ));
    assert!(matches!(
        composition.arm_current_probe(second, SimTime::ZERO, Duration::from_millis(40),),
        Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::Stale(_)),
    ));
    assert_eq!(composition.snapshot(), before_probe);
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending),
    ));

    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server.join())
    })
    .await
    .expect("probe tracer actors join within the local bound");
    let client_joined = client_joined.expect("client A joins cleanly");
    let server_joined = server_joined.expect("server A joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("probe tracer releases the listener slot");
}

async fn apply_real_silent_current_probe_loss(
    composition: &mut QuinnClientSessionComposition,
    server_a: &mut StartedQuinnLegActor,
    observed_at: SimTime,
    probe_timeout: Duration,
) -> (LegGeneration, super::QuinnLegActorPostTerminalCleanupBlock) {
    let generation = composition.snapshot().session.generation();
    let readiness_before_expiry = composition.current_actor_readiness_epoch_for_test();
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();
    let queued = composition
        .begin_current_probe()
        .expect("silent current probe enters A's exact ordered FIFO");
    assert_eq!(queued.generation(), generation);
    assert_eq!(
        composition
            .try_flush_current(observed_at)
            .expect("silent current probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound_probe =
        tokio::time::timeout(TEST_TIMEOUT, server_a.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("silent current probe receive is bounded")
            .expect("server A stays connected")
            .expect("server A receives and withholds the exact probe ACK");
    let BoundInbound::LegControl(inbound_probe) = take_actor_inbound(inbound_probe) else {
        panic!("silent current probe was misclassified as session data");
    };
    let (_seal, probe) = inbound_probe.into_parts();
    let LegControlRecord::LegProbe {
        session_id,
        leg_epoch_nonce,
        probe_sequence,
    } = probe.record().clone()
    else {
        panic!("silent current liveness emitted a non-probe record");
    };
    assert_eq!(probe_sequence, queued.probe_sequence());

    let token = tokio::time::timeout(TEST_TIMEOUT, async {
            let mut queued = queued;
            loop {
                match composition.arm_current_probe(queued, observed_at, probe_timeout) {
                    Ok(token) => break token,
                    Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::NotDelivered(returned)) => {
                        queued = returned;
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("silent current probe failed to arm: {error:?}"),
                }
            }
        })
        .await
        .expect("silent current probe arms after exact writer completion");
    let deadline = observed_at
        .checked_add(probe_timeout)
        .expect("bounded silent current deadline");
    let late_at = deadline
        .checked_add(Duration::from_nanos(1))
        .expect("one-nanosecond late boundary remains representable");
    let late_ack = LegControlFrame::try_new(
        probe.leg_generation(),
        LegControlRecord::LegProbeAck {
            session_id,
            leg_epoch_nonce,
            probe_sequence,
        },
        STANDBY_CONTROL_V1,
    )
    .expect("late exact current probe ACK remains valid wire input");
    let session_clock = composition.session_clock();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while session_clock
            .now()
            .expect("session clock remains representable")
            <= deadline
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the exact actor arrival is forced beyond the semantic deadline");
    server_a
        .enqueue_ordered_leg_control(late_ack, STANDBY_CONTROL_V1)
        .expect("server A queues the exact late ACK");
    assert_eq!(
        server_a
            .try_flush(observed_at)
            .expect("late ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        while composition
            .current_actor_snapshot()
            .queued_inbound_messages()
            != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late exact current probe ACK reaches the bounded actor queue");
    assert!(matches!(
        composition.try_receive_current_before_probe_deadline(token, late_at),
        QuinnCurrentProbeDeadlineDrive::Committed {
            generation: observed_generation,
            probe_sequence: observed_sequence,
        } if observed_generation == generation && observed_sequence == probe_sequence
    ));
    let observed = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(readiness_before_expiry),
    )
    .await
    .expect("silent probe expiry wakes exact current A")
    .expect_err("silent probe expiry publishes one semantic first failure");
    let QuinnLegActorError::RootFailed(cause) = observed else {
        panic!("silent current loss lost its exact root cause: {observed:?}");
    };
    assert!(matches!(*cause, QuinnLegActorError::ProbeDeadlineExpired));
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending),
    ));
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("semantic silent terminal publishes before physical close tail");
    let applied = match composition
        .try_drive_current_terminal()
        .expect("semantic silent terminal reaches the controller exactly once")
    {
        QuinnCurrentTerminalDrive::Applied(applied) => applied,
        outcome => panic!("silent current terminal was not applied: {outcome:?}"),
    };
    assert_eq!(applied.generation(), generation);
    assert_eq!(applied.reason(), LegTransportTerminalReason::FatalIo);
    assert_eq!(
        applied.effects(),
        &[SessionEffect::ResumeGraceStarted { generation }],
    );
    assert_eq!(
        composition.snapshot().session.phase(),
        SessionPhase::Legless
    );
    (generation, physical_cleanup)
}

#[tokio::test(flavor = "current_thread")]
async fn composition_owned_silent_a_installs_b_before_local_attach_delivered() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        mut owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let observed_at = composition
        .session_clock()
        .now()
        .expect("shared actor/root clock remains representable");
    register_owned_and_make_real_b_fresh(
        &mut composition,
        client_b,
        &mut server_b,
        &mut owner_executor,
        &owner_active_a,
        &mut standby_slot,
        observed_at,
    )
    .await;
    let (flow, mut driver) = tcp_factory
        .open_flow(local_flows[0])
        .expect("the exact local flow enters the typed source seam");
    flow.try_send_uplink_with(9, || Bytes::from_static(b"post-loss"))
        .expect("one bounded unacknowledged DATA segment is reserved");
    let input = driver
        .try_recv_next_for_client_controller()
        .expect("typed source dequeue remains live")
        .expect("one exact DATA input is ready");
    let source_effects = match composition
        .apply_driver_input(input)
        .expect("running A admits the typed driver command")
    {
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Applied(effects) => {
            effects
        }
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Deferred {
            ..
        } => panic!("small exact DATA unexpectedly met reducer pressure"),
        crate::owned_upstream::client_controller::ClientControllerDriverApply::Failed(_) => {
            panic!("small exact DATA unexpectedly failed the reducer")
        }
    };
    assert!(source_effects.iter().any(|effect| {
            matches!(
                effect,
                SessionEffect::Transmit(frame)
                    if matches!(frame.record(), Record::Data { payload, .. } if payload.as_ref() == b"post-loss")
            )
        }));
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 9);
    let route_a = composition
        .current_routes()
        .expect("A owns the exact current route before silent loss")
        .0;
    let (generation_a, physical_cleanup) = apply_real_silent_current_probe_loss(
        &mut composition,
        &mut server_a,
        observed_at,
        Duration::from_millis(30),
    )
    .await;
    assert_eq!(composition.current_routes(), None);

    let attach_delivery = Arc::clone(&server_b.delivery_writer_gate);
    attach_delivery.arm();
    let target_before_attach = owner_executor.target().snapshot();
    let owner_active_b = complete_composition_owned_registered_attach(
        &mut composition,
        &mut server_b,
        &mut owner_executor,
        &mut standby_slot,
        observed_at,
    )
    .await;
    tokio::time::timeout(TEST_TIMEOUT, attach_delivery.wait_until_blocked())
        .await
        .expect("client B local ATTACH DELIVERED publication remains withheld");
    assert_eq!(owner_executor.target().snapshot(), target_before_attach);
    assert_eq!(
        composition.snapshot().session.phase(),
        SessionPhase::Legless
    );

    let started_b = composition
        .commit_owned_standby_post_loss_replacement()
        .expect("exact authenticated B response causally commits before local DELIVERED");
    assert_eq!(started_b.generation.get(), generation_a.get() + 1);
    assert_eq!(started_b.generation, owner_active_b.generation());
    assert_eq!(composition.snapshot().session.phase(), SessionPhase::Active);
    assert_eq!(
        composition.snapshot().session.generation(),
        started_b.generation
    );
    assert_eq!(composition.retired_inbound_route(), Some(route_a));
    assert!(
        composition
            .current_actor_snapshot()
            .reserved_delivery_acks()
            > 0,
        "successful install must precede B's intentionally withheld local DELIVERED notice",
    );

    attach_delivery.release();
    tokio::time::timeout(TEST_TIMEOUT, async {
        while composition
            .current_actor_snapshot()
            .reserved_delivery_acks()
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late redundant B DELIVERED publication reconciles safely");

    server_a.request_cancel();
    let server_a_task = tokio::spawn(async move { server_a.join().await });
    physical_cleanup.release();
    assert_eq!(
        composition
            .begin_retired_join()
            .expect("silent terminal-bound A begins exact retired cleanup"),
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinStartDisposition::Started,
    );
    let early_clear = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_finish_retired_join() {
                Ok(
                    crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending,
                ) => {
                    tokio::task::yield_now().await;
                }
                Ok(
                    crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Cleared(
                        cleared,
                    ),
                ) => {
                    panic!("silent A cleared before B application ACK: {cleared:?}")
                }
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("silent retired A structurally joins within the local bound");
    assert!(matches!(
            early_clear,
            crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinClearError::Controller(
                crate::owned_upstream::quinn_leg_composition::QuinnRetiredControllerClearErrorKind::ReplacementApplicationAckPending,
            )
        ));

    assert!(matches!(
        composition.drive_recovery_one(),
        ClientRecoveryDrive::Progress(ClientRecoveryObservation::LegActivated {
            generation
        }) if generation == started_b.generation
    ));
    let mut replay_records = Vec::new();
    let mut owner_frames = Vec::new();
    loop {
        let admission_complete = match composition.drive_recovery_one() {
            ClientRecoveryDrive::Progress(ClientRecoveryObservation::FrameAdmitted) => false,
            ClientRecoveryDrive::AdmissionCompleted { generation } => {
                assert_eq!(generation, started_b.generation);
                true
            }
            other => panic!("owned silent B recovery made no exact progress: {other:?}"),
        };
        assert_eq!(
            composition
                .try_flush_current(observed_at)
                .expect("owned B replay flush remains live"),
            crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
        );
        let replayed =
            tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
                .await
                .expect("owned B replay receive is bounded")
                .expect("server B remains live")
                .expect("server B receives one exact replay frame");
        let BoundInbound::Session(received) = take_actor_inbound(replayed) else {
            panic!("owned B replay was misclassified as leg control");
        };
        let event = owner_active_b
            .accept_frame(received)
            .expect("exact owner B provenance accepts the replay frame");
        replay_records.push(match &event {
            SessionEvent::PeerFrame { frame, .. } => match frame.record() {
                Record::Data { payload, .. } if payload.as_ref() == b"post-loss" => "data",
                record => panic!("unexpected owned B replay record: {record:?}"),
            },
            _ => panic!("owned B replay did not produce a peer-frame event"),
        });
        let outputs = owner_executor
            .apply_event(event)
            .expect("owner reducer and existing Target accept the replay event");
        owner_frames.extend(drain_owner_target_outputs(&mut owner_executor, outputs));
        tokio::time::timeout(TEST_TIMEOUT, async {
                loop {
                    match composition
                        .try_flush_current(observed_at)
                        .expect("owned B replay delivery tail remains live")
                    {
                        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Idle => break,
                        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted
                        | crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::OrderedDeliveryPending => {
                            tokio::task::yield_now().await;
                        }
                    }
                }
            })
            .await
            .expect("owned B replay delivery completes within the local bound");
        if admission_complete {
            break;
        }
    }
    assert_eq!(replay_records, ["data"]);
    assert_eq!(owner_frames.len(), 1);
    assert!(matches!(
        owner_frames[0].record(),
        Record::Ack {
            flow_id,
            direction: Direction::ClientToTarget,
            next_accepted,
            final_accepted: false,
        } if *flow_id == local_flows[0].flow_id() && *next_accepted == ByteOffset::new(9)
    ));
    let target_after_replay = owner_executor.target().snapshot();
    assert_eq!(target_after_replay.open_attempts, 1);
    assert_eq!(target_after_replay.opened_flows, 1);
    assert_eq!(target_after_replay.duplicate_opens, 0);
    assert_eq!(target_after_replay.live_flows, 1);
    assert_eq!(target_after_replay.write_calls, 1);
    assert_eq!(target_after_replay.write_accepted_bytes, 9);

    for frame in owner_frames {
        server_b
            .queue
            .as_mut()
            .expect("server B retains its sole FIFO")
            .push(frame)
            .expect("server B FIFO admits the reducer-minted ACK");
    }
    let mut recovered = None;
    for _ in 0..2 {
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match server_b
                    .try_flush(observed_at)
                    .expect("server B response flush remains live")
                {
                    QuinnLegActorFlushDisposition::Submitted => break,
                    QuinnLegActorFlushDisposition::Idle
                    | QuinnLegActorFlushDisposition::OrderedDeliveryPending => {
                        tokio::task::yield_now().await;
                    }
                }
            }
        })
        .await
        .expect("one owner response is submitted within the local bound");
        let applied = tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                match composition.try_receive_current() {
                    QuinnCurrentInboundDrive::Idle => tokio::task::yield_now().await,
                    QuinnCurrentInboundDrive::Applied {
                        effects,
                        observation,
                    } => break (effects, observation),
                    _ => panic!("current B rejected exact owner recovery state"),
                }
            }
        })
        .await
        .expect("current B applies one owner response");
        if let Some(observation) = applied.1 {
            assert!(recovered.replace(observation).is_none());
        }
    }
    wait_actor_ordered_delivery(&mut server_b).await;
    assert!(matches!(
        recovered,
        Some(ClientInboundObservation::ReplacementRecovered {
            flow_id,
            generation,
            direction: Direction::ClientToTarget,
            previous_accepted,
            next_accepted,
            released_bytes: 9,
            final_accepted: false,
            ..
        }) if flow_id == local_flows[0].flow_id()
            && generation == started_b.generation
            && previous_accepted == ByteOffset::new(0)
            && next_accepted == ByteOffset::new(9)
    ));
    assert_eq!(composition.snapshot().uplink_replay_owned_bytes, 0);
    let cleared_a = match composition
        .try_finish_retired_join()
        .expect("positive application ACK unlocks exact silent-A clear")
    {
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Cleared(cleared) => {
            cleared
        }
        crate::owned_upstream::quinn_leg_composition::QuinnRetiredJoinDrive::Pending => {
            panic!("joined silent A unexpectedly returned to pending")
        }
    };
    assert_eq!(cleared_a.generation(), generation_a);
    assert_eq!(cleared_a.owned_messages(), 0);
    assert_eq!(cleared_a.owned_bytes(), 0);
    assert!(cleared_a.terminal_failure());

    let server_a_joined = tokio::time::timeout(TEST_TIMEOUT, server_a_task)
        .await
        .expect("server A cleanup task remains bounded")
        .expect("server A cleanup task joins")
        .expect("server A actor joins structurally");
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    let (client_b_joined, server_b_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server_b.join())
    })
    .await
    .expect("installed B pair cleans up within the local bound");
    let client_b_joined = client_b_joined.expect("current B joins cleanly");
    let server_b_joined = server_b_joined.expect("server B joins cleanly");
    assert_eq!(client_b_joined.owned_messages(), 0);
    assert_eq!(client_b_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        flow,
        driver,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        owner_active_b,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("silent replacement listener cleanup is bounded")
        .expect("silent replacement releases both actor slots");
}

#[tokio::test(flavor = "current_thread")]
async fn real_quinn_silent_current_probe_deadline_mints_one_exact_loss() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xc3; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xc4; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind shared actor listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (accepted, owner_bootstrap) = complete_initial_actor_attach(&mut client, &mut server).await;
    wait_actor_ordered_delivery(&mut server).await;
    let mut composition =
        QuinnClientSessionComposition::bootstrap_initial(actor_session_config(), accepted, client)
            .expect("composition consumes exact current A");
    let generation = composition.snapshot().session.generation();
    let before_probe = composition.snapshot();
    let readiness_before_expiry = composition.current_actor_readiness_epoch_for_test();
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();

    let queued = composition
        .begin_current_probe()
        .expect("one exact current probe enters A's ordered FIFO");
    assert_eq!(queued.generation(), generation);
    assert_eq!(
        composition
            .try_flush_current(SimTime::ZERO)
            .expect("silent current probe flush remains live"),
        crate::owned_upstream::quinn_leg_composition::QuinnCurrentFlushDisposition::Submitted,
    );
    let inbound_probe =
        tokio::time::timeout(TEST_TIMEOUT, server.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("silent probe receive is bounded")
            .expect("server A stays connected")
            .expect("server A receives and deliberately withholds the exact probe ACK");
    let BoundInbound::LegControl(inbound_probe) = take_actor_inbound(inbound_probe) else {
        panic!("silent current probe was misclassified as a session frame");
    };
    let (_probe_seal, probe) = inbound_probe.into_parts();
    let LegControlRecord::LegProbe { probe_sequence, .. } = probe.record() else {
        panic!("silent liveness emitted a non-probe control record");
    };
    let queued_sequence = queued.probe_sequence();
    assert_eq!(*probe_sequence, queued_sequence);

    let deadline = SimTime::from_nanos(30_000_000);
    let token = tokio::time::timeout(TEST_TIMEOUT, async {
            let mut queued = queued;
            loop {
                match composition.arm_current_probe(
                    queued,
                    SimTime::ZERO,
                    Duration::from_millis(30),
                ) {
                    Ok(token) => break token,
                    Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentProbeArmError::NotDelivered(returned)) => {
                        queued = returned;
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("silent probe deadline failed to arm: {error:?}"),
                }
            }
        })
        .await
        .expect("silent probe arms only after writer completion");
    assert_eq!(token.instant(), deadline);

    let token = match composition.try_receive_current_before_probe_deadline(
        token,
        SimTime::from_nanos(deadline.as_nanos() - 1),
    ) {
        QuinnCurrentProbeDeadlineDrive::NotDue(token) => token,
        _outcome => panic!("deadline-1ns changed silent A state"),
    };
    assert_eq!(composition.snapshot(), before_probe);
    assert!(matches!(
        composition.try_receive_current_before_probe_deadline(token, deadline),
        QuinnCurrentProbeDeadlineDrive::Committed {
            generation: observed_generation,
            probe_sequence: observed_sequence,
        } if observed_generation == generation && observed_sequence == queued_sequence
    ));

    let observed = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(readiness_before_expiry),
    )
    .await
    .expect("probe expiry wakes the exact current actor")
    .expect_err("probe expiry publishes a semantic first failure");
    let QuinnLegActorError::RootFailed(cause) = observed else {
        panic!("probe expiry lost its exact root cause: {observed:?}");
    };
    assert!(matches!(*cause, QuinnLegActorError::ProbeDeadlineExpired));
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::Pending),
    ));
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("semantic probe terminal publishes before physical close tail");
    let applied = match composition
        .try_drive_current_terminal()
        .expect("semantic probe terminal reaches the controller once")
    {
        QuinnCurrentTerminalDrive::Applied(applied) => applied,
        outcome => panic!("probe expiry did not apply exactly once: {outcome:?}"),
    };
    assert_eq!(applied.generation(), generation);
    assert_eq!(applied.reason(), LegTransportTerminalReason::FatalIo);
    assert_eq!(
        applied.effects(),
        &[SessionEffect::ResumeGraceStarted { generation }],
    );
    assert_eq!(
        composition.snapshot().session.phase(),
        SessionPhase::Legless
    );
    assert!(matches!(
        composition.try_drive_current_terminal(),
        Ok(QuinnCurrentTerminalDrive::AlreadyApplied {
            generation: observed_generation,
            reason: LegTransportTerminalReason::FatalIo,
        }) if observed_generation == generation
    ));

    physical_cleanup.release();
    let (client_joined, server_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(composition.join_current_for_shutdown(), server.join())
    })
    .await
    .expect("silent-loss actors join within the local bound");
    let client_joined = client_joined.expect("silent-loss client A joins cleanly");
    let server_joined = server_joined.expect("silent-loss server A joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("silent-loss tracer releases the listener slot");
}

#[tokio::test(flavor = "current_thread")]
async fn real_quinn_controller_only_discard_stays_consumed_failed_through_physical_join() {
    let limits = composition_actor_limits();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xb3; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xb4; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("single-slot listener configuration"),
    )
    .await
    .expect("bind shared actor listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("consumed-failure actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let mut server = server.expect("publish server actor");
    let (accepted, owner_bootstrap) = complete_initial_actor_attach(&mut client, &mut server).await;
    wait_actor_ordered_delivery(&mut server).await;
    let mut composition =
        QuinnClientSessionComposition::bootstrap_initial(actor_session_config(), accepted, client)
            .expect("composition consumes the exact current actor");
    let generation = composition.snapshot().session.generation();
    let physical_cleanup = composition.arm_current_post_terminal_cleanup_block_for_test();
    let progress_epoch = composition.current_actor_readiness_epoch_for_test();

    server.request_cancel();
    let server_joined = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("peer owner joins within the local bound")
        .expect("peer owner joins structurally");
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_current_actor_progress(progress_epoch),
    )
    .await
    .expect("peer close wakes the current actor progress waiter")
    .expect_err("peer close publishes the actor's first fatal");
    assert!(
        composition
            .bind_and_discard_current_terminal_controller_only_for_test()
            .expect("pending controller-only drive remains live")
            .is_none(),
        "the first drive requests cancellation but cannot bind before publication",
    );
    tokio::time::timeout(TEST_TIMEOUT, physical_cleanup.wait_until_blocked())
        .await
        .expect("the exact terminal is published before physical cleanup");

    let controller_only = composition
        .bind_and_discard_current_terminal_controller_only_for_test()
        .expect("exact actor terminal binds through the controller-only test transaction")
        .expect("published actor terminal is available");
    assert_eq!(controller_only.generation(), generation);
    assert_eq!(
        controller_only.effects(),
        &[SessionEffect::ResumeGraceStarted { generation }],
    );
    let after_controller_only_discard = composition.snapshot();
    assert_eq!(
        after_controller_only_discard.session.phase(),
        SessionPhase::Legless,
    );

    for attempt in 0..2 {
        assert!(matches!(
                composition.try_drive_current_terminal(),
                Err(crate::owned_upstream::quinn_leg_composition::QuinnCurrentTerminalError::ControllerDiscard(
                    crate::owned_upstream::client_controller::ClientCurrentTerminalDiscardErrorKind::AlreadyDiscarded,
                )),
            ), "consumed discard failure attempt {attempt} was not sticky");
        assert_eq!(composition.snapshot(), after_controller_only_discard);
    }

    physical_cleanup.release();
    let join_error = tokio::time::timeout(TEST_TIMEOUT, composition.join_current_for_shutdown())
        .await
        .expect("physical current join remains bounded")
        .expect_err("physical join cannot erase the consumed discard failure");
    assert!(matches!(
            join_error,
            QuinnCurrentJoinError::CurrentTerminalDiscardFailed(
                crate::owned_upstream::client_controller::ClientCurrentTerminalDiscardErrorKind::AlreadyDiscarded,
            ),
        ));
    assert_eq!(
        composition.current_joined_ownership_for_test(),
        Some((0, 0)),
        "sticky logical failure retains the exact zero-ownership joined witness",
    );
    let repeated_join_error = composition
        .join_current_for_shutdown()
        .await
        .expect_err("repeated join observation retains the same sticky failure");
    assert!(matches!(
            repeated_join_error,
            QuinnCurrentJoinError::CurrentTerminalDiscardFailed(
                crate::owned_upstream::client_controller::ClientCurrentTerminalDiscardErrorKind::AlreadyDiscarded,
            ),
        ));
    assert_eq!(
        composition.current_joined_ownership_for_test(),
        Some((0, 0)),
    );

    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect("consumed-failure composition releases its exact actor slot");
}

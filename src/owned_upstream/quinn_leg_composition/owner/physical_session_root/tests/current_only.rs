use std::path::PathBuf;
use std::time::Duration;

mod active_source;
mod bound_source;
mod terminal_grace;

use super::super::*;
use crate::owned_upstream::FlowPortConfig;
use crate::owned_upstream::leg::BoundInbound;
use crate::owned_upstream::leg_io::{AcceptedInitialAttach, LegIoLimits};
use crate::owned_upstream::owner_ready::{OwnerReadySessionLimits, ReadyGeometry, ReadyIndex};
use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
use crate::owned_upstream::owner_session_controller::OwnerSessionController;
use crate::owned_upstream::owner_target::{
    OwnerTargetConfig, OwnerTargetExecutor, OwnerTargetInitialAcceptanceDelivery,
    OwnerTargetInitialAcceptanceSettlementDisposition,
};
use crate::owned_upstream::quinn_leg_actor::{
    AttachPeerPolicy, QuinnLegActorAdmission, QuinnLegActorClientConfig, QuinnLegActorError,
    QuinnLegActorFactory, QuinnLegActorFlushDisposition, QuinnLegActorInboundPoll,
    QuinnLegActorInboundPreflight, QuinnLegActorJoinBlock, QuinnLegActorLimits,
    QuinnLegActorListener, QuinnLegActorLiveLimits, QuinnLegActorQueueLimits,
    QuinnLegActorServerConfig, StartedQuinnLegActor,
};
use crate::owned_upstream::quinn_leg_composition::{
    QuinnLegCompositionGate, QuinnOwnerInitialAcceptanceDrive, QuinnOwnerInitialSessionComposition,
};
use crate::owned_upstream::supervisor::SessionSupervisor;
use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
use crate::owned_upstream::two_leg::{LegId, SimTime};
use crate::resumable::{
    AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy, AttachRequest,
    AttachTransportBinding, BitsPerSecond, DevicePrincipal, DeviceSecret, DirectionalRates,
    FeatureOffer, LegControlFrame, LegControlRecord, LegEpochNonce, LegGeneration,
    MAX_ENCODED_DATA_FRAME_BYTES, OwnerIdentity, ProbeSequence, ReplayCapacitySpec,
    ReplayStorageGeometry, ResumeSecret, SESSION_PROTOCOL_VERSION, STANDBY_CONTROL_V1, SessionId,
    VersionRange,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(3);

#[test]
fn current_only_direct_join_has_no_root_local_task_or_actor_extraction() {
    let source = std::fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs"),
    )
    .expect("physical-session root source remains readable");
    let legacy_structures = [
        ("root-local runtime spawn", "runtime.spawn("),
        ("root-local JoinHandle", "tokio::task::JoinHandle"),
        ("unknown admission sentinel", "AdmissionUnknown"),
        ("actor-moving replacement", "std::mem::replace"),
        ("outer task result", "OwnerPhysicalCurrentJoinOutcome"),
    ]
    .into_iter()
    .filter_map(|(label, needle)| source.contains(needle).then_some(label))
    .collect::<Vec<_>>();

    assert!(
        legacy_structures.is_empty(),
        "CurrentOnly still has legacy root-task structures: {}",
        legacy_structures.join(", ")
    );
}

fn actor_limits() -> QuinnLegActorLimits {
    QuinnLegActorLimits::new(
        LegIoLimits::new(MAX_ENCODED_DATA_FRAME_BYTES, 16, 1_048_576, 16, 1_048_576)
            .expect("bounded initial-A LegIo"),
        QuinnLegActorQueueLimits::new(4, 262_144).expect("bounded initial-A FIFO"),
        QuinnLegActorLiveLimits::new(4, 262_144, 4, 262_144, 4, 4, 72)
            .expect("bounded initial-A live lanes"),
    )
    .expect("coherent initial-A actor capacity")
}

fn owner_capacity_plan() -> OwnerSessionCapacityPlan {
    let rates =
        DirectionalRates::symmetric(BitsPerSecond::new(1_024).expect("nonzero local tracer rate"));
    let storage = ReplayCapacitySpec::new(rates, rates, Duration::from_millis(500), Duration::ZERO)
        .expect("bounded local replay horizon")
        .derive()
        .expect("representable local replay capacity")
        .storage_plan(ReplayStorageGeometry::new(8, 4).expect("four bounded flow slots"))
        .expect("bounded local replay storage");
    OwnerSessionCapacityPlan::derive(storage, 4, 64)
        .expect("checked owner capacity derives from exact replay storage")
}

fn attach_credentials() -> AttachCredentials {
    AttachCredentials::new(
        DeviceSecret::new([0x91; 32]).expect("nonzero device secret"),
        ResumeSecret::new([0x92; 32]).expect("nonzero resume secret"),
    )
    .expect("distinct attach credentials")
}

fn attach_authority(binding: AttachTransportBinding) -> AttachAuthority {
    AttachAuthority::new(
        SessionId::new([0x93; 16]).expect("nonzero session id"),
        LegGeneration::new(1).expect("nonzero authority generation"),
        attach_credentials(),
        AttachPolicy::new(
            binding.owner_identity(),
            binding.alpn(),
            binding.device_principal(),
            SESSION_PROTOCOL_VERSION,
            STANDBY_CONTROL_V1.bits(),
        )
        .expect("attach policy matches the physical binding"),
    )
}

async fn connect_actor_pair(
    listener: &mut QuinnLegActorListener,
) -> (StartedQuinnLegActor, StartedQuinnLegActor) {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
        TEST_TIMEOUT,
    )
    .expect("valid loopback client configuration");
    let limits = actor_limits();
    let server = async {
        loop {
            match listener
                .admit(LegId::A, limits, peer_policy)
                .await
                .expect("listener admission remains live")
            {
                QuinnLegActorAdmission::RetrySent => {}
                QuinnLegActorAdmission::Bootstrap(bootstrap) => {
                    break bootstrap.start().await.expect("publish exact owner actor");
                }
            }
        }
    };
    let (client, owner) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            server,
        )
    })
    .await
    .expect("real actor-pair bootstrap is bounded");
    (client.expect("publish exact client actor"), owner)
}

async fn wait_ordered_delivery(actor: &mut StartedQuinnLegActor) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match actor
                .try_flush(SimTime::ZERO)
                .expect("ordered-delivery progress remains live")
            {
                QuinnLegActorFlushDisposition::Idle => break,
                QuinnLegActorFlushDisposition::OrderedDeliveryPending
                | QuinnLegActorFlushDisposition::Submitted => tokio::task::yield_now().await,
            }
        }
    })
    .await
    .expect("ordered delivery completes within the local bound");
}

struct PreparedCurrentOnlyOwner {
    actor: StartedQuinnLegActor,
    executor: OwnerTargetExecutor<MemoryTarget>,
    handoff: OwnerTargetInitialHandoff,
    capacity: OwnerSessionCapacityPlan,
}

struct PreparedCurrentOnlyFixture {
    owner: PreparedCurrentOnlyOwner,
    client: StartedQuinnLegActor,
    listener: QuinnLegActorListener,
    accepted_client: AcceptedInitialAttach,
}

struct PreparedActiveSourceFixture {
    owner: PreparedCurrentOnlyOwner,
    client: StartedQuinnLegActor,
    listener: QuinnLegActorListener,
    peer: SessionSupervisor,
    attached: crate::owned_upstream::leg::AttachedLeg,
    local_flow: crate::resumable::LocalFlow,
    payload: bytes::Bytes,
}

struct SettledCurrentOnlyFixture {
    root: OwnerPhysicalSessionRoot<MemoryTarget>,
    client: StartedQuinnLegActor,
    listener: QuinnLegActorListener,
    join_block: QuinnLegActorJoinBlock,
    ready_index: ReadyIndex,
}

async fn prepare_current_only_fixture() -> PreparedCurrentOnlyFixture {
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
            TEST_TIMEOUT,
        )
        .expect("one exact initial-A actor fits"),
    )
    .await
    .expect("bind CurrentOnly listener");
    let (mut client, mut owner) = connect_actor_pair(&mut listener).await;
    let gate = QuinnLegCompositionGate::mint_for_test();

    let (awaiting, binding) = client
        .with_controller_parts_mut(&gate, |io, queue| {
            let binding = io.established_leg().standby_transport_binding();
            let request = AttachRequest::new(
                SessionId::new([0x93; 16]).expect("nonzero session id"),
                LegGeneration::new(2).expect("nonzero initial generation"),
                AttachNonce::new([0x94; 16]).expect("nonzero attach nonce"),
                VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION)
                    .expect("single supported version"),
                FeatureOffer::new(STANDBY_CONTROL_V1.bits(), STANDBY_CONTROL_V1.bits())
                    .expect("standby-control offer"),
            );
            let proof = attach_credentials()
                .prove(&request, &binding)
                .expect("initial attach proof");
            let awaiting = io
                .established_leg()
                .begin_attach(request)
                .bind_initial_frame(request.to_attach_frame(proof))
                .expect("bind exact initial frame")
                .enqueue(queue)
                .expect("initial request enters the exact FIFO");
            (awaiting, binding)
        })
        .expect("client actor retains its exact LegIo/FIFO");
    assert_eq!(
        client
            .try_flush(SimTime::ZERO)
            .expect("initial attach flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );

    let capacity_plan = owner_capacity_plan();
    let mut pending_owner = Some(SessionSupervisor::prepare_owner(
        capacity_plan,
        attach_authority(binding),
    ));
    let owner_bootstrap = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match owner
                .try_transact_controller_inbound_with_parts(
                    &gate,
                    &mut pending_owner,
                    |_pending, _io, _queue| Ok(QuinnLegActorInboundPreflight::Ready(())),
                    |pending, (), envelope, io, queue| {
                        let (delivery, _arrived_at) = envelope.into_parts();
                        let inbound = io
                            .receive_classified_delivery(delivery, STANDBY_CONTROL_V1)
                            .map_err(|_| {
                                QuinnLegActorError::Protocol("initial attach classification failed")
                            })?;
                        let BoundInbound::Session(received) = inbound else {
                            return Err(QuinnLegActorError::Protocol(
                                "initial attach was misclassified as leg control",
                            ));
                        };
                        pending
                            .take()
                            .ok_or(QuinnLegActorError::Protocol(
                                "initial owner bootstrap was already consumed",
                            ))?
                            .accept(io.established_leg(), received, queue)
                            .map_err(|_| {
                                QuinnLegActorError::Protocol("initial owner authentication failed")
                            })?
                            .enqueue_acceptance(queue)
                            .map_err(|_| {
                                QuinnLegActorError::Protocol(
                                    "initial owner acceptance enqueue failed",
                                )
                            })
                    },
                )
                .expect("owner actor initial transaction remains live")
            {
                QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                QuinnLegActorInboundPoll::Blocked => {
                    panic!("initial owner transaction unexpectedly blocked")
                }
                QuinnLegActorInboundPoll::Classified(bootstrap) => break bootstrap,
            }
        }
    })
    .await
    .expect("owner initial attach transaction is bounded");
    wait_ordered_delivery(&mut client).await;
    assert_eq!(
        owner
            .try_flush(SimTime::ZERO)
            .expect("initial acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );

    let mut awaiting = Some(awaiting);
    let accepted_client = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match client
                .try_transact_composition_inbound(
                    &gate,
                    STANDBY_CONTROL_V1,
                    |inbound, _arrived_at, queue| {
                        let BoundInbound::Session(response) = inbound else {
                            return Err(QuinnLegActorError::Protocol(
                                "initial acceptance was misclassified as leg control",
                            ));
                        };
                        awaiting
                            .take()
                            .ok_or(QuinnLegActorError::Protocol(
                                "initial client acceptance was already consumed",
                            ))?
                            .validate_response(queue, response)
                            .map_err(|_| {
                                QuinnLegActorError::Protocol(
                                    "initial client acceptance validation failed",
                                )
                            })
                    },
                )
                .expect("client acceptance transaction remains live")
            {
                QuinnLegActorInboundPoll::Idle => tokio::task::yield_now().await,
                QuinnLegActorInboundPoll::Blocked => {
                    panic!("client acceptance transaction unexpectedly blocked")
                }
                QuinnLegActorInboundPoll::Classified(accepted) => break accepted,
            }
        }
    })
    .await
    .expect("client initial acceptance is bounded");
    wait_ordered_delivery(&mut owner).await;

    let required_pending_effects = capacity_plan
        .session_config()
        .owner_pending_work_bound()
        .expect("owner pending-work bound is representable");
    let (executor, handoff) = OwnerTargetExecutor::from_initial_bootstrap(
        OwnerTargetConfig::new(64, required_pending_effects, 64)
            .expect("bounded owner Target limits"),
        owner_bootstrap,
        MemoryTarget::new(
            MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
                .expect("bounded memory Target")
                .with_max_terminal_tombstones(4)
                .expect("matching terminal-tombstone capacity"),
        ),
        FlowPortConfig::new(64, 8, 8, 8).expect("bounded owner source"),
    )
    .expect("whole initial bootstrap installs without decomposition");
    PreparedCurrentOnlyFixture {
        owner: PreparedCurrentOnlyOwner {
            actor: owner,
            executor,
            handoff,
            capacity: capacity_plan,
        },
        client,
        listener,
        accepted_client,
    }
}

fn finish_current_only(
    prepared: PreparedCurrentOnlyOwner,
    index: &mut ReadyIndex,
) -> (
    OwnerPhysicalSessionRoot<MemoryTarget>,
    QuinnLegActorJoinBlock,
) {
    let PreparedCurrentOnlyOwner {
        actor: owner,
        executor,
        handoff,
        capacity: capacity_plan,
    } = prepared;
    let gate = QuinnLegCompositionGate::mint_for_test();
    let owner_clock = owner.session_clock(&gate);
    let limits =
        OwnerReadySessionLimits::checked(capacity_plan.session_config().max_flows(), 1, 8, 8)
            .unwrap();
    // Same declared request input as prepare_current_only_fixture, validated against the real source.
    let ready = index
        .reserve_session(SessionId::new([0x93; 16]).unwrap(), limits)
        .unwrap();
    let controller = OwnerSessionController::new_checked(
        executor,
        owner_clock,
        capacity_plan,
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("checked controller accepts exact capacity provenance");
    finish_checked_current_only(owner, controller, handoff)
}

fn finish_checked_current_only(
    owner: StartedQuinnLegActor,
    controller: OwnerSessionController<MemoryTarget>,
    handoff: OwnerTargetInitialHandoff,
) -> (
    OwnerPhysicalSessionRoot<MemoryTarget>,
    QuinnLegActorJoinBlock,
) {
    let mut initial =
        QuinnOwnerInitialSessionComposition::adopt_controller(owner, controller, handoff)
            .expect("actor/controller/handoff enter one initial owner root");
    assert!(matches!(
        initial
            .drive_initial_acceptance()
            .expect("delivered acceptance settles exactly"),
        QuinnOwnerInitialAcceptanceDrive::Settled {
            disposition: OwnerTargetInitialAcceptanceSettlementDisposition::SettledNow,
            ..
        }
    ));
    let join_block = initial
        .arm_join_block_for_test()
        .expect("settled initial actor exposes its deterministic join gate");
    let mut accepted = initial
        .try_into_accepted_root_parts()
        .expect("only the settled initial-A Authority crosses the seed seam");
    assert_eq!(accepted.retained_current_actors(), 1);
    assert_eq!(accepted.retained_controllers(), 1);
    assert_eq!(accepted.retained_executors(), 1);
    assert_eq!(accepted.retained_attached_authorities(), 1);
    assert_eq!(accepted.retained_acceptance_receipts(), 0);
    assert_eq!(accepted.retained_physical_actor_identities(), 1);
    assert!(accepted.controller_shares_current_actor_clock());
    assert_eq!(
        accepted
            .current_actor_inventory()
            .expect("settled initial-A remains physically auditable")
            .delivery(),
        OwnerTargetInitialAcceptanceDelivery::Settled,
    );
    let seed = accepted.into_physical_root_seed();

    let root = match OwnerPhysicalSessionRoot::from_initial_seed(seed) {
        Ok(root) => root,
        Err(failure) => {
            let (_seed, block) = failure.into_parts();
            panic!("settled seed must enter CurrentOnly: {block:?}")
        }
    };
    (root, join_block)
}

async fn settled_current_only_fixture() -> SettledCurrentOnlyFixture {
    let mut ready_index =
        ReadyIndex::cold(&ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap())
            .unwrap();
    let PreparedCurrentOnlyFixture {
        owner,
        client,
        listener,
        accepted_client: _accepted_client,
    } = prepare_current_only_fixture().await;
    let (root, join_block) = finish_current_only(owner, &mut ready_index);
    SettledCurrentOnlyFixture {
        root,
        client,
        listener,
        join_block,
        ready_index,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn settled_initial_seed_enters_current_only_root_with_exact_shutdown_authority() {
    let SettledCurrentOnlyFixture {
        ready_index: _ready_index,
        mut root,
        mut client,
        mut listener,
        join_block,
    } = settled_current_only_fixture().await;
    let inventory = root
        .current_only_inventory()
        .expect("CurrentOnly ownership remains exactly auditable");
    assert_eq!(inventory.current_actors(), 1);
    assert_eq!(inventory.retained_handoffs(), 1);
    assert_eq!(inventory.retained_controllers(), 1);
    assert_eq!(inventory.retained_executors(), 1);
    assert_eq!(inventory.retained_attached_authorities(), 1);
    assert_eq!(inventory.retained_acceptance_receipts(), 0);
    assert_eq!(inventory.actor_fifo_messages(), 0);
    assert_eq!(inventory.actor_fifo_bytes(), 0);
    assert!(inventory.controller_output_is_exact_zero());
    assert!(inventory.restart_credit_is_not_installed());
    assert!(!inventory.is_whole_root_exact_zero());

    join_block.release();
    let (root_joined, client_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join_current_for_shutdown(), client.join())
    })
    .await
    .expect("CurrentOnly root and peer join within the local bound");
    let root_joined = root_joined.expect("CurrentOnly root joins its actor structurally");
    assert_eq!(root_joined.actor_owned_messages(), 0);
    assert_eq!(root_joined.actor_owned_bytes(), 0);
    assert_eq!(root_joined.retained_controllers(), 1);
    assert_eq!(root_joined.retained_executors(), 1);
    assert_eq!(root_joined.retained_handoffs(), 1);
    assert_eq!(root_joined.retained_output_roots(), 1);
    assert!(!root_joined.is_whole_root_exact_zero());
    let client_joined = client_joined.expect("initial-A peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("CurrentOnly listener cleanup is bounded")
        .expect("the exact actor slot is released");
}

#[tokio::test(flavor = "current_thread")]
async fn current_only_observer_cancellation_retains_admitted_join_custody_for_reentry() {
    let SettledCurrentOnlyFixture {
        ready_index: _ready_index,
        mut root,
        mut client,
        mut listener,
        join_block,
    } = settled_current_only_fixture().await;

    let mut first_observer = Box::pin(root.join_current_for_shutdown());
    tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::select! {
            biased;
            _ = &mut first_observer => {
                panic!("current join bypassed its deterministic gate")
            }
            () = join_block.wait_until_blocked() => {}
        }
    })
    .await
    .expect("current join reaches its deterministic gate");
    drop(first_observer);

    let retained = root
        .current_only_inventory()
        .expect("observer cancellation retains auditable current join custody");
    assert_eq!(retained.current_actors(), 1);
    assert_eq!(retained.retained_handoffs(), 1);
    assert_eq!(retained.retained_controllers(), 1);
    assert_eq!(retained.retained_executors(), 1);
    assert_eq!(retained.retained_attached_authorities(), 1);
    assert_eq!(retained.retained_acceptance_receipts(), 0);
    assert_eq!(retained.actor_fifo_messages(), 0);
    assert_eq!(retained.actor_fifo_bytes(), 0);
    assert!(retained.controller_output_is_exact_zero());
    assert!(retained.restart_credit_is_not_installed());
    assert!(!retained.is_whole_root_exact_zero());

    let mut second_observer = Box::pin(root.join_current_for_shutdown());
    tokio::select! {
        biased;
        _ = &mut second_observer => {
            panic!("second observer detached from the blocked custody")
        }
        () = tokio::task::yield_now() => {}
    }
    join_block.release();
    let (root_joined, client_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(second_observer, client.join())
    })
    .await
    .expect("the re-entered root and peer joins complete after release");
    let root_joined = root_joined.expect("the second observer consumes current join once");
    assert_eq!(root_joined.actor_owned_messages(), 0);
    assert_eq!(root_joined.actor_owned_bytes(), 0);
    assert_eq!(root_joined.retained_controllers(), 1);
    assert_eq!(root_joined.retained_executors(), 1);
    assert_eq!(root_joined.retained_handoffs(), 1);
    assert_eq!(root_joined.retained_output_roots(), 1);
    assert!(!root_joined.is_whole_root_exact_zero());
    let client_joined = client_joined.expect("initial-A peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("CurrentOnly listener cleanup is bounded")
        .expect("the exact actor slot is released");
}

#[tokio::test(flavor = "current_thread")]
async fn current_only_joining_pre_cancel_unwind_retains_same_actor_for_roll_forward() {
    let SettledCurrentOnlyFixture {
        ready_index: _ready_index,
        mut root,
        mut client,
        mut listener,
        join_block,
    } = settled_current_only_fixture().await;
    let actor_before = match &root {
        OwnerPhysicalSessionRoot::CurrentOnly(current) => std::ptr::from_ref(&current.actor.actor),
    };
    root.arm_joining_pre_cancel_panic_for_test();

    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let OwnerPhysicalSessionRoot::CurrentOnly(current) = &mut root;
        let _ = current.start_join_for_shutdown();
    }));
    assert!(unwind.is_err(), "the single-use admission failpoint fires");

    let actor_after = match &root {
        OwnerPhysicalSessionRoot::CurrentOnly(current) => {
            assert!(matches!(
                &current.actor.phase,
                OwnerPhysicalCurrentActorPhase::Joining { .. }
            ));
            std::ptr::from_ref(&current.actor.actor)
        }
    };
    assert_eq!(
        actor_after, actor_before,
        "unwind leaves the same concrete actor in sticky Joining custody",
    );

    let retained = root
        .current_only_inventory()
        .expect("join-admission unwind retains auditable nonzero custody");
    assert_eq!(retained.current_actors(), 1);
    assert_eq!(retained.retained_handoffs(), 1);
    assert_eq!(retained.retained_controllers(), 1);
    assert_eq!(retained.retained_executors(), 1);
    assert_eq!(retained.retained_attached_authorities(), 1);
    assert_eq!(retained.retained_acceptance_receipts(), 0);
    assert_eq!(retained.actor_fifo_messages(), 0);
    assert_eq!(retained.actor_fifo_bytes(), 0);
    assert!(retained.controller_output_is_exact_zero());
    assert!(retained.restart_credit_is_not_installed());
    assert!(!retained.is_whole_root_exact_zero());

    let mut observer = Box::pin(root.join_current_for_shutdown());
    tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::select! {
            biased;
            _ = &mut observer => {
                panic!("same-actor roll-forward bypassed its deterministic gate")
            }
            () = join_block.wait_until_blocked() => {}
        }
    })
    .await
    .expect("same-actor roll-forward reaches the existing actor join gate");
    join_block.release();
    let (root_joined, client_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(observer, client.join())
    })
    .await
    .expect("same-actor roll-forward and peer join within the local bound");
    let root_joined = root_joined.expect("the retained actor joins without new admission");
    assert_eq!(root_joined.actor_owned_messages(), 0);
    assert_eq!(root_joined.actor_owned_bytes(), 0);
    assert_eq!(root_joined.retained_controllers(), 1);
    assert_eq!(root_joined.retained_executors(), 1);
    assert_eq!(root_joined.retained_handoffs(), 1);
    assert_eq!(root_joined.retained_output_roots(), 1);
    assert!(!root_joined.is_whole_root_exact_zero());
    let client_joined = client_joined.expect("initial-A peer joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("CurrentOnly listener cleanup is bounded")
        .expect("the exact actor slot is released");
}

#[tokio::test(flavor = "current_thread")]
async fn current_only_inner_join_error_is_retained_once_and_observed_stably() {
    let SettledCurrentOnlyFixture {
        ready_index: _ready_index,
        mut root,
        mut client,
        mut listener,
        join_block,
    } = settled_current_only_fixture().await;
    root.arm_joining_pre_cancel_panic_for_test();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let OwnerPhysicalSessionRoot::CurrentOnly(current) = &mut root;
        let _ = current.start_join_for_shutdown();
    }));
    assert!(
        unwind.is_err(),
        "Joining is published before error injection"
    );

    let residual_frame = LegControlFrame::try_new(
        LegGeneration::new(3).expect("nonzero residual generation"),
        LegControlRecord::LegProbe {
            session_id: SessionId::new([0x93; 16]).expect("nonzero session id"),
            leg_epoch_nonce: LegEpochNonce::new([0x98; 16])
                .expect("nonzero residual leg epoch nonce"),
            probe_sequence: ProbeSequence::new(1).expect("nonzero residual probe sequence"),
        },
        STANDBY_CONTROL_V1,
    )
    .expect("valid residual leg-control frame");
    let expected_bytes = residual_frame
        .encode(STANDBY_CONTROL_V1)
        .expect("residual frame remains encodable")
        .len();
    let _receipt = match &mut root {
        OwnerPhysicalSessionRoot::CurrentOnly(current) => current
            .actor
            .actor
            .enqueue_ordered_leg_control(residual_frame, STANDBY_CONTROL_V1)
            .expect("one real residual record enters the actor FIFO"),
    };
    join_block.release();

    let (first_observation, client_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(root.join_current_for_shutdown(), client.join())
    })
    .await
    .expect("real residual join and peer cleanup stay bounded");
    assert!(matches!(
        first_observation,
        Err(OwnerPhysicalSessionRootError::ActorJoinQuarantined)
    ));
    let client_joined = client_joined.expect("the peer actor still joins structurally");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);

    let exact_error_before = match &root {
        OwnerPhysicalSessionRoot::CurrentOnly(current) => match &current.actor.phase {
            OwnerPhysicalCurrentActorPhase::Failed {
                admitted_inventory,
                exact_error,
            } => {
                assert_eq!(admitted_inventory.current_actors(), 1);
                assert!(matches!(
                    exact_error,
                    QuinnLegActorError::ResidualOwnership { messages: 1, bytes }
                        if *bytes == expected_bytes
                ));
                std::ptr::from_ref(exact_error)
            }
            _ => panic!("the complete modeled actor error was not retained in Failed"),
        },
    };
    assert!(matches!(
        root.join_current_for_shutdown().await,
        Err(OwnerPhysicalSessionRootError::ActorJoinQuarantined)
    ));
    let exact_error_after = match &root {
        OwnerPhysicalSessionRoot::CurrentOnly(current) => match &current.actor.phase {
            OwnerPhysicalCurrentActorPhase::Failed { exact_error, .. } => {
                std::ptr::from_ref(exact_error)
            }
            _ => panic!("repeated observation moved the retained exact error"),
        },
    };
    assert_eq!(
        exact_error_after, exact_error_before,
        "repeated observation only borrows the retained exact error",
    );

    drop(root);
    let listener_shutdown = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("failed-actor fixture cleanup is bounded");
    assert!(matches!(
        listener_shutdown,
        Err(QuinnLegActorError::ListenerShutdownUnclean)
    ));
}

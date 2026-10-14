use bytes::Bytes;

mod progress_storage;

use super::*;
use crate::owned_upstream::owner_ready::{OwnerReadySessionLimits, ReadyGeometry, ReadyIndex};
use crate::owned_upstream::owner_target::OwnerTargetConfigError;
use crate::owned_upstream::target::{
    MemoryTargetConfig, MemoryWriteDirective, TargetIo, TargetIoColdInventory, TargetOpenCompletion,
};
use crate::resumable::{
    BitsPerSecond, DirectionalRates, ReplayCapacitySpec, ReplayStorageGeometry, ReplayStoragePlan,
};

fn replay_storage(coalesced_record_payload_bytes: usize) -> ReplayStoragePlan {
    replay_storage_with_horizon(
        coalesced_record_payload_bytes,
        Duration::from_millis(500),
        Duration::ZERO,
    )
}

fn replay_storage_with_horizon(
    coalesced_record_payload_bytes: usize,
    blackout_budget: Duration,
    max_normal_ack_age: Duration,
) -> ReplayStoragePlan {
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(100_000_000).unwrap());
    ReplayCapacitySpec::new(rates, rates, blackout_budget, max_normal_ack_age)
        .unwrap()
        .derive()
        .unwrap()
        .storage_plan(ReplayStorageGeometry::new(coalesced_record_payload_bytes, 4).unwrap())
        .unwrap()
}

fn executor_from_storage(
    storage: ReplayStoragePlan,
) -> (SessionId, OwnerTargetExecutor<MemoryTarget>) {
    executor_from_storage_and_source(storage, FlowPortConfig::new(128, 8, 8, 8).unwrap(), 64)
}

fn executor_from_storage_and_source(
    storage: ReplayStoragePlan,
    source_config: FlowPortConfig,
    max_target_read_bytes: usize,
) -> (SessionId, OwnerTargetExecutor<MemoryTarget>) {
    let leg = committed_leg();
    let session_id = leg.session_id();
    let plan = OwnerSessionCapacityPlan::derive(storage, 8, 128).unwrap();
    let config = plan.session_config();
    let required_pending_effects = config
        .owner_pending_work_bound()
        .expect("the checked fixture storage has a representable pending-work bound");
    let supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, leg);
    let target = MemoryTarget::new(
        MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
            .unwrap()
            .with_max_terminal_tombstones(8)
            .unwrap(),
    );
    let executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, required_pending_effects, max_target_read_bytes).unwrap(),
        supervisor,
        target,
        source_config,
    )
    .unwrap();
    (session_id, executor)
}

fn one_live_two_terminal_executor() -> (
    crate::resumable::CommittedLeg,
    OwnerTargetExecutor<MemoryTarget>,
) {
    let leg = committed_leg();
    let config = SessionConfig::new(
        1,
        2,
        TcpWindowLimits::new(128, 16).unwrap(),
        TcpWindowLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReceiveBudgetLimits::new(256, 32).unwrap(),
        128,
    )
    .unwrap();
    let required_pending_effects = config.owner_pending_work_bound().unwrap();
    let supervisor = SessionSupervisor::new(SessionModel::new(SessionRole::Owner, config, leg));
    let target = MemoryTarget::new(
        MemoryTargetConfig::new(1, 256, 512, 128, 128, 16)
            .unwrap()
            .with_max_terminal_tombstones(2)
            .unwrap(),
    );
    let executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, required_pending_effects, 64).unwrap(),
        supervisor,
        target,
        FlowPortConfig::new(128, 8, 8, 8).unwrap(),
    )
    .unwrap();
    (leg, executor)
}

#[test]
fn memory_target_capacity_and_cold_inventory_are_independently_checked() {
    let config = MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
        .unwrap()
        .with_max_terminal_tombstones(7)
        .unwrap();
    let mut target_io = MemoryTarget::new(config);
    let flow_id = SessionFlowId::new(1).unwrap();

    let capacity = target_io.capacity_witness();
    assert_eq!(capacity.max_flows(), 4);
    assert_eq!(capacity.max_terminal_tombstones(), 7);
    assert_eq!(capacity.max_buffered_bytes(), 512);
    assert_eq!(capacity.max_write_directives(), 64);

    assert_eq!(
        target_io.open(flow_id, &target(443)).unwrap(),
        TargetOpenCompletion::Opened
    );
    target_io
        .feed_read(flow_id, Bytes::from_static(b"owned"))
        .unwrap();
    target_io
        .push_write_directive(flow_id, MemoryWriteDirective::WouldBlock)
        .unwrap();

    let inventory = target_io.cold_inventory().unwrap();
    assert_eq!(inventory.live_flows(), 1);
    assert_eq!(inventory.terminal_tombstones(), 0);
    assert_eq!(inventory.summed_buffered_bytes(), 5);
    assert_eq!(inventory.stored_buffered_bytes(), 5);
    assert_eq!(inventory.summed_write_directives(), 1);
    assert_eq!(inventory.stored_write_directives(), 1);
    assert!(!inventory.is_quiescent());
    assert_eq!(
        inventory,
        TargetIoColdInventory::checked(capacity, 1, 0, 5, 5, 1, 1,).unwrap(),
    );
}

#[test]
fn owner_capacity_rejects_replay_storage_drift_and_returns_every_constructor_authority() {
    let geometry = ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready_index = ReadyIndex::cold(&geometry).unwrap();
    let installed_storage = replay_storage(62_500);
    let drifted_storage = replay_storage(31_250);
    let (session_id, executor) = executor_from_storage(installed_storage);
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let clock_identity = clock.clone();
    let plan = OwnerSessionCapacityPlan::derive(drifted_storage, 8, 128).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8).unwrap();
    let ready = ready_index.reserve_session(session_id, limits).unwrap();

    let failure = match OwnerSessionController::new_checked(
        executor,
        clock,
        plan,
        ready,
        std::time::Duration::from_secs(1),
    ) {
        Ok(_) => panic!("a drifted replay-storage authority must not build the owner controller"),
        Err(failure) => failure,
    };
    assert!(matches!(
        failure.source(),
        OwnerSessionControllerBuildError::Capacity(
            OwnerSessionCapacityError::ReplayStorageExpectationMismatch
        )
    ));

    let (clock, executor, returned_plan, ready, returned_grace, source) = failure.into_parts();
    assert!(clock.same_source(&clock_identity));
    assert_eq!(returned_plan, plan);
    assert_eq!(returned_grace, std::time::Duration::from_secs(1));
    assert!(matches!(
        source,
        OwnerSessionControllerBuildError::Capacity(
            OwnerSessionCapacityError::ReplayStorageExpectationMismatch
        )
    ));

    let controller = OwnerSessionController::new_checked(
        executor,
        clock,
        OwnerSessionCapacityPlan::derive(installed_storage, 8, 128).unwrap(),
        ready,
        std::time::Duration::from_secs(1),
    )
    .expect("the returned authorities build once with their exact installed storage plan");
    let witness: OwnerSessionCapacityWitness = controller.capacity_witness();
    let _output_page: OwnerOutputPageCapacity = controller.output_page_capacity();
    assert_eq!(witness.replay_storage(), installed_storage);
}

#[test]
fn owner_capacity_rejects_equal_config_horizon_collision() {
    let geometry = ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready_index = ReadyIndex::cold(&geometry).unwrap();
    let installed_storage =
        replay_storage_with_horizon(62_500, Duration::from_millis(500), Duration::ZERO);
    let colliding_storage = replay_storage_with_horizon(
        62_500,
        Duration::from_millis(450),
        Duration::from_millis(50),
    );
    assert_ne!(installed_storage, colliding_storage);
    assert_eq!(
        installed_storage.horizon().effective(),
        colliding_storage.horizon().effective()
    );
    assert_eq!(
        SessionConfig::from_storage_plan(SessionRole::Owner, 8, installed_storage, 128).unwrap(),
        SessionConfig::from_storage_plan(SessionRole::Owner, 8, colliding_storage, 128).unwrap(),
        "the collision must not rely on a scalar SessionConfig mismatch"
    );

    let (session_id, executor) = executor_from_storage(installed_storage);
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let plan = OwnerSessionCapacityPlan::derive(colliding_storage, 8, 128).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8).unwrap();
    let ready = ready_index.reserve_session(session_id, limits).unwrap();
    match OwnerSessionController::new_checked(
        executor,
        clock,
        plan,
        ready,
        std::time::Duration::from_secs(1),
    ) {
        Ok(_) => panic!("equal scalar limits must not forge exact replay-plan provenance"),
        Err(failure) => {
            let (_clock, _executor, _plan, ready, _terminal_grace, _source) = failure.into_parts();
            assert!(
                ready.cancel().is_ok(),
                "same Cold remains explicitly cancellable"
            );
        }
    }
}

#[test]
fn owner_target_rejects_target_read_above_single_wire_record_limit_before_factory_mint() {
    let storage = replay_storage(62_500);
    let plan = OwnerSessionCapacityPlan::derive(storage, 8, 128).unwrap();
    let session = plan.session_config();
    let required_pending_effects = session.owner_pending_work_bound().unwrap();
    let read_bound = crate::resumable::MAX_DATA_PAYLOAD_BYTES + 1;
    let supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, committed_leg());
    let target = MemoryTarget::new(
        MemoryTargetConfig::new(4, read_bound, read_bound * 2, read_bound, read_bound, 16)
            .unwrap()
            .with_max_terminal_tombstones(8)
            .unwrap(),
    );

    match OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, required_pending_effects, read_bound).unwrap(),
        supervisor,
        target,
        FlowPortConfig::new(read_bound, 8, 8, 8).unwrap(),
    ) {
        Err(OwnerTargetConfigError::TargetReadExceedsSingleDataPayload {
            requested,
            available,
        }) => {
            assert_eq!(requested, read_bound);
            assert_eq!(available, crate::resumable::MAX_DATA_PAYLOAD_BYTES);
        }
        Err(error) => panic!("wrong fail-closed constructor error: {error}"),
        Ok(_) => panic!(
            "a configured Target read larger than one wire DATA record is unreachable at runtime"
        ),
    }
}

#[test]
fn owner_capacity_witness_retains_exact_source_factory_primitive_layout() {
    let geometry = ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready_a = ReadyIndex::cold(&geometry).unwrap();
    let mut ready_b = ReadyIndex::cold(&geometry).unwrap();
    let storage = replay_storage(62_500);
    let plan = OwnerSessionCapacityPlan::derive(storage, 8, 128).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8).unwrap();
    let (session_a, executor_a) =
        executor_from_storage_and_source(storage, FlowPortConfig::new(128, 2, 3, 4).unwrap(), 64);
    let reservation_a = ready_a.reserve_session(session_a, limits).unwrap();
    let controller_a = OwnerSessionController::new_checked(
        executor_a,
        SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now()),
        plan,
        reservation_a,
        std::time::Duration::from_secs(1),
    )
    .unwrap();
    let capacity_a = controller_a.capacity_witness();
    let (session_b, executor_b) =
        executor_from_storage_and_source(storage, FlowPortConfig::new(128, 3, 2, 4).unwrap(), 64);
    let reservation_b = ready_b.reserve_session(session_b, limits).unwrap();
    let controller_b = OwnerSessionController::new_checked(
        executor_b,
        SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now()),
        plan,
        reservation_b,
        std::time::Duration::from_secs(1),
    )
    .unwrap();
    let capacity_b = controller_b.capacity_witness();

    let source_a = capacity_a.source_factory();
    let source_b = capacity_b.source_factory();
    assert_eq!(
        source_a.per_flow_channel_slots(),
        source_b.per_flow_channel_slots()
    );
    assert_ne!(source_a.flow_port(), source_b.flow_port());
    assert_ne!(capacity_a, capacity_b);

    assert_eq!(source_a.bound_direction(), Some(Direction::TargetToClient));
    assert_eq!(
        source_a.flow_port(),
        FlowPortConfig::new(128, 2, 3, 4).unwrap()
    );
    assert_eq!(
        source_a.session_uplink_byte_capacity(),
        storage.global().target_to_client().max_bytes()
    );
    assert_eq!(
        source_a.per_flow_uplink_segment_capacity(),
        storage.per_flow().target_to_client().max_segments()
    );
    assert_eq!(
        source_a.session_uplink_segment_capacity(),
        storage.global().target_to_client().max_segments()
    );
    assert_eq!(
        capacity_a.source_session_channel_slots(),
        plan.session_config().max_flows() * source_a.per_flow_channel_slots()
    );
}

#[tokio::test(start_paused = true)]
async fn owner_progress_capacity_includes_live_and_terminal_grace_flows() {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = one_live_two_terminal_executor();
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let mut retained_terminal_graces = Vec::new();

    for raw_flow_id in 1..=2 {
        let flow_id = SessionFlowId::new(raw_flow_id).unwrap();
        assert!(
            !controller
                .apply_actor_event(arrived(
                    peer_event(
                        leg,
                        Record::Open {
                            flow_id,
                            target: target(8_000 + raw_flow_id as u16),
                        },
                    ),
                    &clock,
                ))
                .unwrap()
                .needs_continue()
        );
        controller
            .target_mut_for_test()
            .feed_read(flow_id, Bytes::from_static(b"x"))
            .unwrap();
        assert!(
            !controller
                .try_read_target(flow_id)
                .unwrap()
                .needs_continue()
        );
        let terminal = controller
            .apply_actor_event(arrived(
                peer_event(
                    leg,
                    Record::Reset {
                        flow_id,
                        reason: ResetReason::TargetFailure,
                    },
                ),
                &clock,
            ))
            .unwrap()
            .outputs()
            .iter()
            .find_map(|output| match output {
                OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
                _ => None,
            })
            .expect("each terminal flow retains its exact grace authority");
        retained_terminal_graces.push(terminal);
    }

    assert_eq!(retained_terminal_graces.len(), 2);
    assert_eq!(controller.snapshot().progress_entries, 2);

    let live_flow = SessionFlowId::new(3).unwrap();
    assert!(
        !controller
            .apply_actor_event(arrived(
                peer_event(
                    leg,
                    Record::Open {
                        flow_id: live_flow,
                        target: target(8_003),
                    },
                ),
                &clock,
            ))
            .expect("terminal tombstones release the independent live-flow slot")
            .needs_continue()
    );
    controller
        .target_mut_for_test()
        .feed_read(live_flow, Bytes::from_static(b"y"))
        .unwrap();
    assert!(
        !controller
            .try_read_target(live_flow)
            .expect("live plus terminal-grace progress is within the checked controller envelope")
            .needs_continue()
    );

    assert_eq!(controller.snapshot().progress_entries, 3);
    assert!(!controller.snapshot().failed);
}

#[test]
fn physical_owner_composition_has_no_unchecked_controller_construction_path() {
    let controller = include_str!("../../owner_session_controller.rs");
    let construction = include_str!("../construction.rs");
    let terminal_grace = include_str!("../terminal_grace.rs");
    let initial_composition = include_str!("../../quinn_leg_composition/owner_initial.rs");
    let recovery_composition = include_str!("../../quinn_leg_composition/owner.rs");
    let initial_physical = concat!(
        include_str!("../../quinn_leg_actor/tests/r7_initial_acceptance_settlement.rs"),
        include_str!(
            "../../quinn_leg_actor/tests/r7_initial_acceptance_settlement/initial_composition.rs"
        ),
    );
    let attach_physical = concat!(
        include_str!("../../quinn_leg_actor/tests/r7_physical_attach_permit.rs"),
        include_str!("../../quinn_leg_actor/tests/r7_physical_attach_permit/scenarios.rs"),
    );
    let recovery_physical =
        include_str!("../../quinn_leg_actor/tests/r7_owner_physical_recovery.rs");

    assert!(construction.contains("OwnerSessionControllerCore<T, CheckedOwnerControllerCapacity>"));
    assert!(controller.contains("LegacyOwnerSessionController<T>"));
    for forbidden in [
        "capacity: Option<OwnerSessionCapacityWitness>",
        "match self.capacity",
        "into_executor_after_failed_initial_adopt",
    ] {
        for source in [controller, construction, terminal_grace] {
            assert!(
                !source.contains(forbidden),
                "controller source regained the forbidden runtime/bypass seam: {forbidden}",
            );
        }
    }

    for (name, source) in [
        ("initial composition", initial_composition),
        ("recovery composition", recovery_composition),
        ("initial physical tests", initial_physical),
        ("attach physical tests", attach_physical),
        ("recovery physical tests", recovery_physical),
    ] {
        assert!(
            !source.contains("LegacyOwnerSessionController"),
            "{name} regained the legacy controller type",
        );
        assert!(
            !source.contains("OwnerSessionController::new("),
            "{name} regained unchecked controller construction",
        );
    }
    assert!(!initial_composition.contains("OwnerTargetExecutor"));
    assert!(!initial_physical.contains("QuinnOwnerInitialSessionComposition::adopt("));
}

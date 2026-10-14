//! Original progress backing through checked BoundInactive observation settlement.
//! Uses a test-plan Executor, real reducer pages, Ready binding and terminal expiry.
//! Does not claim Active selected reads, physical transport or whole-root zero.

use super::*;
use crate::owned_upstream::owner_output_drain::{
    OwnerOutputDrain, OwnerOutputDrainDrive, OwnerOutputLifecyclePort,
    OwnerTerminalTimerRegistration,
};

fn admit(output: &mut OwnerOutputDrain, drive: OwnerSessionDrive) {
    let continued = drive.needs_continue();
    output
        .admit_outputs(drive.into_outputs(), continued)
        .unwrap();
}

fn drain_lifecycle(
    controller: &mut OwnerSessionController<MemoryTarget>,
    output: &mut OwnerOutputDrain,
    port: &mut OwnerOutputLifecyclePort,
) {
    for _ in 0..16 {
        let page = match output.drive_one(None, port).unwrap() {
            OwnerOutputDrainDrive::PageDrained(page) => page,
            OwnerOutputDrainDrive::LifecycleInstalled { page_drained } => {
                assert!(controller.install_terminal_action(port).unwrap());
                let Some(page) = page_drained else { continue };
                page
            }
            _ => panic!("only original lifecycle pages may drain without a physical FIFO"),
        };
        if !page.needs_continue() {
            return;
        }
        admit(
            output,
            controller.continue_effects_result_for_test().unwrap(),
        );
    }
    panic!("bounded original lifecycle continuation must finish");
}

#[tokio::test(start_paused = true)]
async fn checked_progress_backing_survives_original_replay_reset_and_expiry() {
    let leg = committed_leg();
    let storage = replay_storage(62_500);
    let plan = OwnerSessionCapacityPlan::derive(storage, 8, 128).unwrap();
    let config = plan.session_config();
    let supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, leg);
    let target_io = MemoryTarget::new(
        MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
            .unwrap()
            .with_max_terminal_tombstones(8)
            .unwrap(),
    );
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, config.owner_pending_work_bound().unwrap(), 64).unwrap(),
        supervisor,
        target_io,
        FlowPortConfig::new(128, 8, 8, 8).unwrap(),
    )
    .unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();
    let opened = executor
        .apply_event(peer_event(
            leg,
            Record::Open {
                flow_id,
                target: target(443),
            },
        ))
        .unwrap();
    assert_eq!(opened.len(), 1);
    assert!(matches!(&opened[0], OwnerTargetOutput::Transmit(frame)
        if matches!(frame.record(), Record::OpenResult { flow_id: id, .. } if *id == flow_id)));
    executor
        .target_mut()
        .feed_read(flow_id, Bytes::from_static(b"reply"))
        .unwrap();
    let original_replay = executor.try_read_target_observed(flow_id).unwrap();

    let geometry = ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let limits = OwnerReadySessionLimits::checked(config.max_flows(), 1, 8, 8).unwrap();
    let ready = index.reserve_session(leg.session_id(), limits).unwrap();
    let clock = SessionMonotonicClock::start();
    let mut controller = OwnerSessionController::new_checked(
        executor,
        clock.clone(),
        plan,
        ready,
        Duration::from_millis(50),
    )
    .unwrap();
    let capacity = 2 * (config.max_flows() + config.max_terminal_tombstones());
    let (backing, slots, occupied, free) =
        controller.progress.storage_measurement_for_test().unwrap();
    assert_eq!((slots, occupied, free), (capacity, 0, capacity));
    let now = controller.begin_terminal_grace_turn().unwrap();
    // Keep the original transmit page in the test sink; never synthesize an observation.
    let replay_output = controller
        .finish_observed_page(original_replay, now, OwnerContinuationOrigin::Local)
        .unwrap();
    assert!(!replay_output.needs_continue());
    assert!(replay_output.outputs().iter().any(|output| matches!(output,
        OwnerTargetOutput::Transmit(frame) if matches!(frame.record(), Record::Data { .. }))));
    assert_eq!(
        controller.progress.storage_measurement_for_test().unwrap(),
        (backing, capacity, 1, capacity - 1)
    );
    assert_eq!(
        controller
            .progress
            .direction_snapshot(flow_id, Direction::TargetToClient)
            .unwrap()
            .oldest_unacknowledged(),
        Some(ByteOffset::new(0))
    );
    controller.cold_inventory().unwrap();

    let mut output = OwnerOutputDrain::new(8, 4096).unwrap();
    let mut port = OwnerOutputLifecyclePort::new();
    let drive = controller
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
        .unwrap();
    admit(&mut output, drive);
    drain_lifecycle(&mut controller, &mut output, &mut port);
    assert_eq!(
        controller.progress.storage_measurement_for_test().unwrap(),
        (backing, capacity, 1, capacity - 1)
    );
    assert!(
        controller
            .progress
            .direction_snapshot(flow_id, Direction::TargetToClient)
            .unwrap()
            .terminal()
    );
    assert_eq!(
        controller
            .progress
            .direction_snapshot(flow_id, Direction::ClientToTarget),
        Err(OwnerProgressError::UnknownFlowDirection)
    );
    let mut timer = OwnerTerminalTimerRegistration::cold().unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        std::future::poll_fn(|cx| controller.poll_terminal_timer(&mut timer, cx).unwrap()),
    )
    .await
    .unwrap();
    let drive = controller.expire_original_terminal(&timer).unwrap();
    admit(&mut output, drive);
    controller
        .settle_terminal_expiry_output(&mut timer)
        .unwrap();
    drain_lifecycle(&mut controller, &mut output, &mut port);
    assert_eq!(
        controller.progress.storage_measurement_for_test().unwrap(),
        (backing, capacity, 0, capacity)
    );
    controller.cold_inventory().unwrap();
    assert_eq!(controller.snapshot().progress_entries, 0);
    assert!(!controller.terminal_timer_requested(&timer));
    assert!(
        !replay_output.outputs().is_empty(),
        "the returned original page is still owned"
    );
}

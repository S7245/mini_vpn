//! C-local expiry index lifecycle through original reducer/output/timer ports.
//!
//! Owns terminal ordering/reuse and Inactive HalfClose pressure characterization.
//! It does not model actor FIFO pressure, Root scheduling or whole-root shutdown.
//! Facts/actions come from real preopened MemoryTarget Flows and peer records.
//! The parent supplies only private fixture primitives; no production getters.

use super::*;
use crate::owned_upstream::owner_output_drain::{OwnerOutputDrain, OwnerOutputDrainDrive};

fn flow(value: u64) -> SessionFlowId {
    SessionFlowId::new(value).unwrap()
}

fn four_opened_controller() -> (
    ReadyIndex,
    crate::resumable::CommittedLeg,
    OwnerSessionController<MemoryTarget>,
    SessionMonotonicClock,
) {
    let (plan, leg, mut executor) = terminal_executor(false);
    for id in 1..=4 {
        let mut outputs = executor
            .apply_event(peer_event(
                leg,
                Record::Open {
                    flow_id: flow(id),
                    target: target(443),
                },
            ))
            .unwrap();
        let mut opened = 0;
        let mut completed = false;
        for _ in 0..8 {
            let mut next = None;
            for output in outputs {
                match output {
                    OwnerTargetOutput::NeedsResume { resume } => {
                        assert!(next.replace(resume).is_none());
                    }
                    OwnerTargetOutput::Transmit(frame) => {
                        assert!(matches!(frame.record(), Record::OpenResult { flow_id, .. }
                            if *flow_id == flow(id)));
                        opened += 1;
                    }
                    _ => panic!("prebind OPEN must not produce lifecycle actions"),
                }
            }
            let Some(resume) = next else {
                completed = true;
                break;
            };
            outputs = executor.resume_pending_effects(resume).unwrap();
        }
        assert!(completed);
        assert_eq!(opened, 1);
    }
    let geometry = ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8).unwrap();
    let ready = index.reserve_session(leg.session_id(), limits).unwrap();
    let clock = SessionMonotonicClock::start();
    let controller = OwnerSessionController::new_checked(
        executor,
        clock.clone(),
        plan,
        ready,
        Duration::from_millis(50),
    )
    .unwrap();
    (index, leg, controller, clock)
}

fn admit(output: &mut OwnerOutputDrain, drive: OwnerSessionDrive) {
    let needs_continue = drive.needs_continue();
    output
        .admit_outputs(drive.into_outputs(), needs_continue)
        .unwrap();
}

fn drain_original_pages(
    controller: &mut OwnerSessionController<MemoryTarget>,
    output: &mut OwnerOutputDrain,
    port: &mut OwnerOutputLifecyclePort,
) {
    for _ in 0..16 {
        let page = match output.drive_one(None, port).unwrap() {
            OwnerOutputDrainDrive::PageDrained(page) => page,
            OwnerOutputDrainDrive::LifecycleInstalled { page_drained } => {
                assert!(controller.install_terminal_action(port).unwrap());
                assert!(!port.has_pending());
                let Some(page) = page_drained else { continue };
                page
            }
            _ => panic!("only the original lifecycle page is permitted; no synthetic FIFO"),
        };
        if !page.needs_continue() {
            return;
        }
        let drive = controller.continue_effects_result_for_test().unwrap();
        admit(output, drive);
    }
    panic!("original bounded continuation must finish");
}

fn reset(
    controller: &mut OwnerSessionController<MemoryTarget>,
    output: &mut OwnerOutputDrain,
    port: &mut OwnerOutputLifecyclePort,
    leg: crate::resumable::CommittedLeg,
    clock: &SessionMonotonicClock,
    id: SessionFlowId,
) {
    let drive = controller
        .apply_actor_event(arrived(peer_reset(leg, id), clock))
        .unwrap();
    admit(output, drive);
    drain_original_pages(controller, output, port);
}

// Read-only redundant-index audit: no synthetic facts, direct slot writes or
// test-only calls to insert/remove/commit can make this lifecycle pass.
fn audit(controller: &OwnerSessionController<MemoryTarget>, order: &[SessionFlowId]) -> usize {
    let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
        panic!("the checked controller must own its actual terminal ledger");
    };
    assert_eq!(ledger.slots.len(), 8);
    assert_eq!(ledger.slots.capacity(), 8);
    assert_eq!(ledger.count, order.len());
    assert_eq!(controller.snapshot().retained_observations, order.len());
    assert!(!controller.snapshot().failed);
    assert!(ledger.turn.is_none());

    fn visit(
        ledger: &OwnerTerminalGraceLedger,
        node: Option<usize>,
        seen: &mut [bool],
        keys: &mut Vec<SessionFlowId>,
    ) -> usize {
        let Some(index) = node else { return 0 };
        assert!(!seen[index], "no cycle or shared tree node");
        seen[index] = true;
        let slot = &ledger.slots[index];
        let TerminalSlotState::AwaitingOutput { fact, .. } = &slot.state else {
            panic!("tree cannot refer to a free slot");
        };
        let left = visit(ledger, slot.left, seen, keys);
        keys.push(fact.flow_id());
        let right = visit(ledger, slot.right, seen, keys);
        assert!(left.abs_diff(right) <= 1);
        assert_eq!(slot.height, 1 + left.max(right));
        assert!(matches!(&slot.lifecycle,
            TerminalLifecycle::Installed { slot, expiry: TerminalExpiry::Waiting }
            if slot.inventory().installed_actions() == 1));
        slot.height
    }

    let mut seen = vec![false; ledger.slots.len()];
    let mut keys = Vec::new();
    visit(ledger, ledger.root, &mut seen, &mut keys);
    let mut sorted = order.to_vec();
    sorted.sort();
    assert_eq!(keys, sorted);
    let mut cursor = ledger.head;
    let mut last = None;
    for expected in order {
        let index = cursor.expect("FIFO cannot end before the original terminal sequence");
        let TerminalSlotState::AwaitingOutput { fact, .. } = &ledger.slots[index].state else {
            panic!("birth FIFO cannot refer to a free slot");
        };
        assert_eq!(fact.flow_id(), *expected);
        assert!(seen[index]);
        last = Some(index);
        cursor = ledger.slots[index].next;
    }
    assert!(cursor.is_none());
    assert_eq!(ledger.tail, last);
    cursor = ledger.free;
    let mut free = 0;
    while let Some(index) = cursor {
        assert!(!seen[index], "free list is disjoint and acyclic");
        seen[index] = true;
        let slot = &ledger.slots[index];
        assert!(matches!(slot.state, TerminalSlotState::Vacant));
        assert!(matches!(slot.lifecycle, TerminalLifecycle::Awaiting));
        assert!(slot.left.is_none() && slot.right.is_none());
        assert_eq!(slot.height, 1);
        cursor = slot.next;
        free += 1;
    }
    assert_eq!(free + order.len(), ledger.slots.len());
    assert!(seen.into_iter().all(|visited| visited));
    ledger.slots.as_ptr() as usize
}

#[tokio::test(start_paused = true)]
async fn original_expiry_rebalances_multiple_terminals_and_reuses_the_released_slot() {
    for order in [[1, 2, 3], [3, 2, 1], [3, 1, 2], [1, 3, 2]] {
        let (_index, leg, mut controller, clock) = four_opened_controller();
        let mut output = OwnerOutputDrain::new(8, 4096).unwrap();
        let mut port = OwnerOutputLifecyclePort::new();
        let mut timer = OwnerTerminalTimerRegistration::cold().unwrap();
        let backing = audit(&controller, &[]);
        let mut pending = Vec::new();
        for id in order {
            reset(
                &mut controller,
                &mut output,
                &mut port,
                leg,
                &clock,
                flow(id),
            );
            pending.push(flow(id));
            assert_eq!(audit(&controller, &pending), backing);
        }
        let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
            unreachable!()
        };
        let original_slot = ledger.head.unwrap();
        let root = ledger.root.unwrap();
        let TerminalSlotState::AwaitingOutput { fact, .. } = &ledger.slots[root].state else {
            unreachable!()
        };
        assert_eq!(
            fact.flow_id(),
            flow(2),
            "all four insertion rotations balance the three keys"
        );
        for expired in 0..4 {
            assert!(controller.terminal_timer_requested(&timer));
            tokio::time::timeout(
                Duration::from_secs(1),
                std::future::poll_fn(|cx| controller.poll_terminal_timer(&mut timer, cx).unwrap()),
            )
            .await
            .expect("the original retained timer must reach its real deadline");
            let (terminal, _) = controller.terminal_expiry_head().unwrap();
            assert_eq!(terminal.flow_id(), pending[0]);
            assert!(timer.holds_due(terminal));
            let drive = controller.expire_original_terminal(&timer).unwrap();
            assert_eq!(controller.snapshot().retained_observations, pending.len());
            // The original output page owns the return BEFORE C returns its Z slot.
            admit(&mut output, drive);
            controller
                .settle_terminal_expiry_output(&mut timer)
                .unwrap();
            assert_eq!(timer.inventory(), (1, 0));
            drain_original_pages(&mut controller, &mut output, &mut port);
            pending.remove(0);
            assert_eq!(audit(&controller, &pending), backing);
            if expired == 0 {
                reset(
                    &mut controller,
                    &mut output,
                    &mut port,
                    leg,
                    &clock,
                    flow(4),
                );
                pending.push(flow(4));
                assert_eq!(audit(&controller, &pending), backing);
                let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
                    unreachable!()
                };
                assert_eq!(
                    ledger.tail,
                    Some(original_slot),
                    "reuse the actual returned slot"
                );
            }
        }
        assert!(pending.is_empty());
        assert!(!controller.terminal_timer_requested(&timer));
        assert!(!port.has_pending());
        assert!(matches!(
            output.drive_one(None, &mut port).unwrap(),
            OwnerOutputDrainDrive::Idle
        ));
        // C/Ready/Executor are still real retained authorities, not whole-root zero.
        assert_eq!(timer.inventory(), (1, 0));
    }
}

fn collect_prebind_frames(
    executor: &mut OwnerTargetExecutor<MemoryTarget>,
    mut outputs: Vec<OwnerTargetOutput>,
) -> Vec<Frame> {
    let mut frames = Vec::new();
    for _ in 0..16 {
        let mut next = None;
        for output in outputs {
            match output {
                OwnerTargetOutput::Transmit(frame) => frames.push(frame),
                OwnerTargetOutput::NeedsResume { resume } => {
                    assert!(next.replace(resume).is_none());
                }
                _ => panic!("prebind OPEN/EOF/ACK cannot own terminal lifecycle work"),
            }
        }
        let Some(resume) = next else { return frames };
        outputs = executor.resume_pending_effects(resume).unwrap();
    }
    panic!("the original prebind continuation must finish");
}

fn eof_acknowledged_inactive_controller() -> (
    ReadyIndex,
    crate::resumable::CommittedLeg,
    OwnerSessionController<MemoryTarget>,
    SessionMonotonicClock,
    Vec<Frame>,
) {
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(100_000_000).unwrap());
    let storage = ReplayCapacitySpec::new(rates, rates, Duration::from_millis(500), Duration::ZERO)
        .unwrap()
        .derive()
        .unwrap()
        .storage_plan(ReplayStorageGeometry::new(62_500, 4).unwrap())
        .unwrap();
    let plan = OwnerSessionCapacityPlan::derive(storage, 1, 128).unwrap();
    let leg = committed_leg();
    let supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, leg);
    let target_io = MemoryTarget::new(
        MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
            .unwrap()
            .with_max_terminal_tombstones(1)
            .unwrap(),
    );
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(
            1,
            plan.session_config().owner_pending_work_bound().unwrap(),
            64,
        )
        .unwrap(),
        supervisor,
        target_io,
        FlowPortConfig::new(128, 8, 8, 8).unwrap(),
    )
    .unwrap();
    let mut retained_frames = Vec::new();
    for id in 1..=2 {
        let outputs = executor
            .apply_event(peer_event(
                leg,
                Record::Open {
                    flow_id: flow(id),
                    target: target(443),
                },
            ))
            .unwrap();
        let frames = collect_prebind_frames(&mut executor, outputs);
        assert_eq!(frames.len(), 1);
        assert!(
            matches!(frames[0].record(), Record::OpenResult { flow_id, .. }
            if *flow_id == flow(id))
        );
        retained_frames.extend(frames);
    }
    executor.target_mut().finish_read(flow(2)).unwrap();
    let outputs = executor.try_read_target(flow(2)).unwrap();
    let frames = collect_prebind_frames(&mut executor, outputs);
    assert_eq!(frames.len(), 1);
    assert!(matches!(frames[0].record(), Record::Close {
        flow_id, direction: Direction::TargetToClient, final_offset,
    } if *flow_id == flow(2) && *final_offset == ByteOffset::new(0)));
    retained_frames.extend(frames);
    let outputs = executor
        .apply_event(peer_event(
            leg,
            Record::Ack {
                flow_id: flow(2),
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(0),
                final_accepted: true,
            },
        ))
        .unwrap();
    assert!(collect_prebind_frames(&mut executor, outputs).is_empty());
    assert_eq!(executor.snapshot().session.session.terminal_tombstones(), 0);
    assert_eq!(executor.snapshot().pending_target_completions, 0);
    assert_eq!(executor.target().snapshot().read_eof, 1);
    assert_eq!(executor.target().snapshot().write_half_closes, 0);

    let geometry = ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8).unwrap();
    let ready = index.reserve_session(leg.session_id(), limits).unwrap();
    let clock = SessionMonotonicClock::start();
    let controller = OwnerSessionController::new_checked(
        executor,
        clock.clone(),
        plan,
        ready,
        Duration::from_millis(50),
    )
    .unwrap();
    (index, leg, controller, clock, retained_frames)
}

#[tokio::test(start_paused = true)]
async fn inactive_half_close_pressure_retains_completion_across_original_terminal_expiry() {
    let (mut index, leg, mut controller, clock, mut retained_frames) =
        eof_acknowledged_inactive_controller();
    let mut output = OwnerOutputDrain::new(8, 4096).unwrap();
    let mut port = OwnerOutputLifecyclePort::new();
    let mut timer = OwnerTerminalTimerRegistration::cold().unwrap();
    reset(
        &mut controller,
        &mut output,
        &mut port,
        leg,
        &clock,
        flow(1),
    );
    let (occupying, window) = controller.terminal_expiry_head().unwrap();
    assert_eq!(occupying.flow_id(), flow(1));
    assert_eq!(
        controller
            .executor
            .snapshot()
            .session
            .session
            .terminal_tombstones(),
        1
    );
    assert_eq!(controller.executor.snapshot().pending_target_completions, 0);
    let mut drive = controller
        .apply_actor_event(arrived(
            peer_event(
                leg,
                Record::Close {
                    flow_id: flow(2),
                    direction: Direction::ClientToTarget,
                    final_offset: ByteOffset::new(0),
                },
            ),
            &clock,
        ))
        .unwrap();
    let mut continued = false;
    let mut finished = false;
    let mut nonfinal_ack = 0;
    for _ in 0..16 {
        let needs_continue = drive.needs_continue();
        // C's real returned frames stay owned by this test consumer; these are
        // not actor FIFO acceptance or peer-delivery witnesses.
        for item in drive.into_outputs() {
            let OwnerTargetOutput::Transmit(frame) = item else {
                panic!("pressure cannot publish a terminal lifecycle action");
            };
            assert_eq!(frame.leg_generation(), leg.generation());
            assert!(matches!(frame.record(), Record::Ack {
                flow_id, direction: Direction::ClientToTarget,
                next_accepted, final_accepted: false,
            } if *flow_id == flow(2) && *next_accepted == ByteOffset::new(0)));
            retained_frames.push(frame);
            nonfinal_ack += 1;
        }
        if !needs_continue {
            finished = true;
            break;
        }
        continued = true;
        drive = controller.continue_effects_result_for_test().unwrap();
    }
    assert!(continued && finished);
    assert_eq!(nonfinal_ack, 1);
    assert_eq!(retained_frames.len(), 4);
    let retained = controller.executor.snapshot();
    assert_eq!(retained.session.session.terminal_tombstones(), 1);
    assert_eq!(retained.pending_target_completions, 1);
    // Bound A still owns its original source/Retirement state after Target join.
    assert_eq!(retained.target_flows, 2);
    assert_eq!(retained.target_tombstones, 1);
    assert_eq!(controller.executor.target().snapshot().write_half_closes, 1);
    assert!(!controller.snapshot().failed);
    assert_eq!(controller.snapshot().retained_observations, 1);
    let (same_terminal, same_window) = controller.terminal_expiry_head().unwrap();
    assert_eq!(same_terminal, occupying);
    assert_eq!(same_window.deadline, window.deadline);
    for class in 0..4 {
        assert_eq!(index.select(class).unwrap(), None);
    }

    tokio::time::timeout(
        Duration::from_secs(1),
        std::future::poll_fn(|cx| controller.poll_terminal_timer(&mut timer, cx).unwrap()),
    )
    .await
    .unwrap();
    assert!(timer.holds_due(occupying));
    let drive = controller.expire_original_terminal(&timer).unwrap();
    admit(&mut output, drive);
    controller
        .settle_terminal_expiry_output(&mut timer)
        .unwrap();
    drain_original_pages(&mut controller, &mut output, &mut port);
    let after_expiry = controller.executor.snapshot();
    assert_eq!(after_expiry.session.session.terminal_tombstones(), 0);
    assert_eq!(after_expiry.target_tombstones, 0);
    assert_eq!(after_expiry.pending_target_completions, 1);
    assert_eq!(after_expiry.target_flows, 2);
    assert_eq!(controller.executor.target().snapshot().write_half_closes, 1);
    assert_eq!(controller.snapshot().retained_observations, 0);
    assert!(!controller.snapshot().failed);
    assert_eq!(timer.inventory(), (1, 0));
    assert!(!controller.terminal_timer_requested(&timer));
    for class in 0..4 {
        assert_eq!(index.select(class).unwrap(), None);
    }
    index.check_root().unwrap();
    assert!(output.inventory().unwrap().is_exact_zero());
    assert!(!port.has_pending());
    // Binding was never activated. The completion and actual returned frames
    // remain nonzero; no selected retry, final ACK or whole-root zero is claimed.
    assert_eq!(retained_frames.len(), 4);
    assert_eq!(controller.executor.snapshot(), after_expiry);
}

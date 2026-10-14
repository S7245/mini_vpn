//! Original Executor failure, receipt and read-eligibility transactions.
//!
//! Uses actual authenticated model/Target/TCP ports, one installed pair and
//! the correlated metadata publication interval. No physical handoff is built:
//! this is component evidence, not checked C/actor activation or Root cleanup.
//! Resources remain in their original owners; no synthetic transit is written.

mod cold_open;
mod completion;
mod terminal_read;

use super::*;
use crate::owned_upstream::owner_ready::{OwnerReadySessionLimits, ReadyGeometry, ReadyIndex};
use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
use crate::owned_upstream::owner_target::service::{
    OwnerFlowServicePosition, OwnerFlowServiceSlot, OwnerTargetServiceState,
};
use crate::owned_upstream::owner_target::{
    OwnerTargetConfig, OwnerTargetExecutor, OwnerTargetOutput,
};
use crate::owned_upstream::session::SessionOwnerInbox;
use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
use crate::resumable::{
    BitsPerSecond, DirectionalRates, ReplayCapacitySpec, ReplayStorageGeometry, TerminalGrace,
};
use std::num::NonZeroUsize;
use std::time::Duration;

struct FlowLifecycleFixture {
    executor: OwnerTargetExecutor<MemoryTarget>,
    inbox: SessionOwnerInbox,
    local: LocalFlow,
    terminal: TerminalGrace,
    // The actual domain outlives its Executor, inbox and all registrations.
    ready: ReadyIndex,
}

fn live_record(
    executor: &OwnerTargetExecutor<MemoryTarget>,
    flow_id: SessionFlowId,
) -> &OwnerFlowServicePosition {
    let position = executor.flows[&flow_id].service_position.unwrap();
    let OwnerTargetServiceState::Bound { flows, .. } = &executor.service else {
        panic!("the original binding must remain installed")
    };
    let OwnerFlowServiceSlot::Live(record) = &flows.slots[position] else {
        panic!("the original F slot must remain live")
    };
    record
}

// Caller already owns the selected lane's publication interval. Keep both
// actual source/send visits and assertions; do not claim, finish or poll inbox.
fn send_source_to_mailbox(
    executor: &mut OwnerTargetExecutor<MemoryTarget>,
    lane: SendLane,
) -> SessionCommandVisit {
    assert_eq!(
        executor.visit_ready_send(lane).unwrap().work,
        OwnerReadyUnit::SourcePulled
    );
    let completed = executor.visit_ready_send(lane).unwrap();
    assert_eq!(
        completed.work,
        OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned)
    );
    completed
}

fn peer_record(
    executor: &mut OwnerTargetExecutor<MemoryTarget>,
    leg: CommittedLeg,
    record: Record,
) -> Vec<OwnerTargetOutput> {
    let outputs = executor
        .apply_event(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(leg.generation(), record).unwrap(),
        })
        .unwrap();
    assert!(
        !outputs
            .iter()
            .any(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
    );
    outputs
}

fn lifecycle_fixture() -> FlowLifecycleFixture {
    lifecycle_fixture_with_cold_open(false)
}

fn lifecycle_executor(
    leg: CommittedLeg,
    max_targets: usize,
    effect_steps: usize,
) -> (OwnerSessionCapacityPlan, OwnerTargetExecutor<MemoryTarget>) {
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(1_000_000).unwrap());
    let storage = ReplayCapacitySpec::new(rates, rates, Duration::from_millis(500), Duration::ZERO)
        .unwrap()
        .derive()
        .unwrap()
        .storage_plan(ReplayStorageGeometry::new(128, 4).unwrap())
        .unwrap();
    let plan = OwnerSessionCapacityPlan::derive(storage, 1, 128).unwrap();
    let supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, leg);
    let target = MemoryTarget::new(
        MemoryTargetConfig::new(max_targets, 256, 512, 128, 128, 16)
            .unwrap()
            .with_max_terminal_tombstones(1)
            .unwrap(),
    );
    let executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(
            effect_steps,
            plan.session_config().owner_pending_work_bound().unwrap(),
            64,
        )
        .unwrap(),
        supervisor,
        target,
        FlowPortConfig::new(128, 8, 8, 8).unwrap(),
    )
    .unwrap();
    (plan, executor)
}

fn lifecycle_fixture_with_cold_open(cold_failed_open: bool) -> FlowLifecycleFixture {
    let leg = source_leg();
    let (plan, mut executor) = lifecycle_executor(leg, if cold_failed_open { 1 } else { 4 }, 128);
    let first = SessionFlowId::new(1).unwrap();
    let second = SessionFlowId::new(2).unwrap();
    peer_record(
        &mut executor,
        leg,
        Record::Open {
            flow_id: first,
            target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
        },
    );
    let outputs = peer_record(
        &mut executor,
        leg,
        Record::Reset {
            flow_id: first,
            reason: ResetReason::TargetFailure,
        },
    );
    let terminal = outputs
        .iter()
        .find_map(|output| match output {
            OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
            _ => None,
        })
        .expect("the real RESET occupies the only terminal slot");
    peer_record(
        &mut executor,
        leg,
        Record::Open {
            flow_id: second,
            target: TargetAddr::IpPort(([127, 0, 0, 1], 8443).into()),
        },
    );
    let local = executor.flows[&second]
        .source
        .as_ref()
        .unwrap()
        .flow
        .local_flow();
    if cold_failed_open {
        let failed = SessionFlowId::new(3).unwrap();
        // The actual one-Target limit fails this OPEN; the occupied real Z
        // forces its exact completion to remain before binding installation.
        assert!(
            peer_record(
                &mut executor,
                leg,
                Record::Open {
                    flow_id: failed,
                    target: TargetAddr::IpPort(([127, 0, 0, 1], 9443).into()),
                }
            )
            .is_empty()
        );
        let retained = &executor.flows[&failed];
        assert!(retained.source.is_none() && !retained.target_present);
        assert_eq!(executor.snapshot().pending_target_completions, 1);
    }
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready = ReadyIndex::cold(&geometry).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 2, 2).unwrap();
    let reservation = ready.reserve_session(leg.session_id(), limits).unwrap();
    let (mut executor, inbox) = match executor.install_owner_ready(reservation, plan) {
        Ok(installed) => installed,
        Err(_) => panic!("the same original factory and plan must install"),
    };
    let OwnerTargetServiceState::Bound { binding, flows, .. } = &mut executor.service else {
        panic!("the actual consuming installation must own the binding");
    };
    // Activate the original component metadata; do not claim physical adoption.
    binding
        .activate(&mut ready, 0, &flows.session, 0b1_1111)
        .unwrap();
    FlowLifecycleFixture {
        executor,
        inbox,
        local,
        terminal,
        ready,
    }
}

#[test]
fn staged_reset_waits_for_its_driver_receipt_after_terminal_capacity_release() {
    let FlowLifecycleFixture {
        mut ready,
        mut executor,
        mut inbox,
        local,
        terminal,
    } = lifecycle_fixture();
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    executor
        .stage_bound_target_failure(local.flow_id())
        .unwrap();
    assert_eq!(executor.snapshot().pending_target_completions, 1);
    let outputs = executor
        .apply_event(SessionEvent::TerminalGraceExpired { terminal })
        .unwrap();
    assert!(
        !outputs.iter().any(|output| matches!(output,
        OwnerTargetOutput::Transmit(frame) if matches!(frame.record(), Record::Reset { flow_id, .. }
            if *flow_id == local.flow_id()))),
        "queued RESET is not reducer-returned completion work"
    );
    assert_eq!(executor.snapshot().pending_target_completions, 1);
    let completed = send_source_to_mailbox(&mut executor, SendLane::Control);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(reset)) = inbox.poll_selected(SessionMailboxLane::Control, &mut cx) else {
        panic!("the original queued RESET must reach the same paired inbox");
    };
    assert!(
        matches!(&reset, SessionOwnerCommand::Driver(DriverInput::Control(
        SessionEvent::LocalReset { flow, reason: ResetReason::TargetFailure })) if *flow == local)
    );
    let outputs = executor
        .apply_ready_receipt_observed(reset)
        .unwrap()
        .into_progress_parts()
        .0;
    assert_eq!(
        outputs
            .iter()
            .filter(|output| matches!(output,
        OwnerTargetOutput::Transmit(frame) if matches!(frame.record(), Record::Reset { flow_id, .. }
            if *flow_id == local.flow_id())))
            .count(),
        1
    );
    assert_eq!(executor.snapshot().pending_target_completions, 0);
    assert!(turn.finish(&completed).is_ok());
    ready
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
}

#[test]
fn in_flight_data_settles_without_erasing_the_staged_reset() {
    let FlowLifecycleFixture {
        mut ready,
        mut executor,
        mut inbox,
        local,
        terminal,
    } = lifecycle_fixture();
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    let control = executor.visit_ready_send(SendLane::Control).unwrap();
    assert!(turn.finish(&control).is_ok());
    ready
        .finish(&claim, control.runnable, control.blocked)
        .unwrap();
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    executor
        .target_mut()
        .feed_read(local.flow_id(), Bytes::from_static(b"held"))
        .unwrap();
    assert!(matches!(
        executor.read_ready_target(local.flow_id()).unwrap(),
        crate::owned_upstream::owner_target::read_source::BoundTargetRead::Data
    ));
    let completed = send_source_to_mailbox(&mut executor, SendLane::OrderedSource);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(data)) = inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
    else {
        panic!("actual Target DATA must reach the paired inbox before RESET staging");
    };
    assert!(
        matches!(&data, SessionOwnerCommand::Driver(DriverInput::Data(data))
        if data.flow() == local && data.payload() == b"held")
    );
    executor
        .stage_bound_target_failure(local.flow_id())
        .unwrap();
    let observed = executor
        .apply_ready_receipt_observed(data)
        .expect("staged RESET must not reject already acquired DATA");
    assert!(observed.into_progress_parts().0.iter().any(|output| matches!(output,
        OwnerTargetOutput::Transmit(frame) if matches!(frame.record(), Record::Data { flow_id, payload, .. }
            if *flow_id == local.flow_id() && payload.as_ref() == b"held"))));
    assert_eq!(
        executor.snapshot().pending_target_completions,
        1,
        "DATA settlement is not RESET settlement"
    );
    assert_eq!(executor.snapshot().target_read_owned_bytes, 4);
    assert_eq!(executor.snapshot().target_read_owned_segments, 1);
    let retired = executor
        .apply_event(SessionEvent::TerminalGraceExpired { terminal })
        .unwrap();
    assert!(!retired.iter().any(|output| matches!(output,
        OwnerTargetOutput::Transmit(frame) if matches!(frame.record(), Record::Reset { .. }))));
    assert_eq!(executor.snapshot().pending_target_completions, 1);
    assert_eq!(executor.snapshot().target_read_owned_bytes, 4);
    assert_eq!(executor.snapshot().target_read_owned_segments, 1);
    assert!(turn.finish(&completed).is_ok());
    ready
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
}

#[test]
fn owner_terminal_revokes_sources_without_bulk_draining_existing_commands() {
    use crate::owned_upstream::tcp::FlowPortError;
    let FlowLifecycleFixture {
        mut ready,
        mut executor,
        mut inbox,
        local,
        terminal,
    } = lifecycle_fixture();
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    assert_eq!(
        executor.visit_ready_send(SendLane::Control).unwrap().work,
        OwnerReadyUnit::Waiting
    );
    executor
        .target_mut()
        .feed_read(local.flow_id(), Bytes::from_static(b"held"))
        .unwrap();
    assert!(matches!(
        executor.read_ready_target(local.flow_id()).unwrap(),
        crate::owned_upstream::owner_target::read_source::BoundTargetRead::Data
    ));
    executor
        .stage_bound_target_failure(local.flow_id())
        .unwrap();
    executor
        .apply_event(SessionEvent::TerminalGraceExpired { terminal })
        .unwrap();
    let outputs = peer_record(
        &mut executor,
        source_leg(),
        Record::Reset {
            flow_id: local.flow_id(),
            reason: ResetReason::TargetFailure,
        },
    );
    assert_eq!(
        outputs
            .iter()
            .filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.local_flow() == local))
            .count(),
        1
    );
    assert_eq!(executor.snapshot().target_read_owned_bytes, 4);
    assert_eq!(executor.snapshot().target_read_owned_segments, 1);
    let source = executor.flows[&local.flow_id()].source.as_ref().unwrap();
    assert!(
        matches!(
            source.flow.try_reserve_uplink_source(1),
            Err(FlowPortError::Closed)
        ),
        "actual terminal must revoke new Owner production without dropping already queued DATA"
    );
    send_source_to_mailbox(&mut executor, SendLane::Control);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(reset)) = inbox.poll_selected(SessionMailboxLane::Control, &mut cx) else {
        panic!("revocation must retain the original queued RESET");
    };
    assert!(
        matches!(&reset, SessionOwnerCommand::Driver(DriverInput::Control(
        SessionEvent::LocalReset { flow, reason: ResetReason::TargetFailure })) if *flow == local)
    );
    assert_eq!(live_record(&executor, local.flow_id()).transit, 1);
    assert!(
        executor
            .apply_ready_receipt_observed(reset)
            .unwrap()
            .into_progress_parts()
            .0
            .is_empty()
    );
    assert_eq!(live_record(&executor, local.flow_id()).transit, 0);
    let control = executor.visit_ready_send(SendLane::Control).unwrap();
    assert_eq!(control.work, OwnerReadyUnit::SourceDrained);
    live_record(&executor, local.flow_id())
        .tickets
        .driver_waker(SendLane::Control)
        .unwrap()
        .wake_by_ref();
    let control = executor.visit_ready_send(SendLane::Control).unwrap();
    assert_eq!(control.work, OwnerReadyUnit::Waiting);
    assert!(turn.finish(&control).is_ok());
    ready
        .finish(&claim, control.runnable, control.blocked)
        .unwrap();
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    send_source_to_mailbox(&mut executor, SendLane::OrderedSource);
    let Poll::Ready(Some(data)) = inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
    else {
        panic!("revocation must retain the original ordered DATA and permits");
    };
    assert!(
        matches!(&data, SessionOwnerCommand::Driver(DriverInput::Data(data))
        if data.flow() == local && data.payload() == b"held")
    );
    assert_eq!(live_record(&executor, local.flow_id()).transit, 1);
    assert!(
        executor
            .apply_ready_receipt_observed(data)
            .unwrap()
            .into_progress_parts()
            .0
            .is_empty()
    );
    assert_eq!(live_record(&executor, local.flow_id()).transit, 0);
    assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
    assert_eq!(executor.snapshot().target_read_owned_segments, 0);
    let ordered = executor.visit_ready_send(SendLane::OrderedSource).unwrap();
    assert_eq!(ordered.work, OwnerReadyUnit::SourceDrained);
    live_record(&executor, local.flow_id())
        .tickets
        .driver_waker(SendLane::OrderedSource)
        .unwrap()
        .wake_by_ref();
    let ordered = executor.visit_ready_send(SendLane::OrderedSource).unwrap();
    assert_eq!(ordered.work, OwnerReadyUnit::Waiting);
    let source = executor
        .flows
        .get_mut(&local.flow_id())
        .unwrap()
        .source
        .as_mut()
        .unwrap();
    assert!(matches!(
        source.driver.poll_owner_control(&mut cx),
        Poll::Ready(None)
    ));
    assert!(matches!(
        source.driver.poll_owner_ordered_source(&mut cx),
        Poll::Ready(None)
    ));
    assert!(
        executor.flows.contains_key(&local.flow_id()),
        "physical lane drain is not F/publisher retirement"
    );
    assert!(turn.finish(&ordered).is_ok());
    ready
        .finish(&claim, ordered.runnable, ordered.blocked)
        .unwrap();
}

#[test]
fn in_flight_close_settles_only_itself_and_preserves_the_staged_reset() {
    use crate::owned_upstream::owner_target::PendingTargetCompletion;
    let FlowLifecycleFixture {
        mut ready,
        mut executor,
        mut inbox,
        local,
        terminal,
    } = lifecycle_fixture();
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    let control = executor.visit_ready_send(SendLane::Control).unwrap();
    assert!(turn.finish(&control).is_ok());
    ready
        .finish(&claim, control.runnable, control.blocked)
        .unwrap();
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    executor.target_mut().finish_read(local.flow_id()).unwrap();
    assert!(matches!(
        executor.read_ready_target(local.flow_id()).unwrap(),
        crate::owned_upstream::owner_target::read_source::BoundTargetRead::Eof
    ));
    let completed = send_source_to_mailbox(&mut executor, SendLane::OrderedSource);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(close)) = inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
    else {
        panic!("actual Target EOF must reach the paired inbox as CLOSE");
    };
    assert!(
        matches!(&close, SessionOwnerCommand::Driver(DriverInput::Control(
        SessionEvent::LocalClose { flow })) if *flow == local)
    );
    assert_eq!(live_record(&executor, local.flow_id()).transit, 1);
    executor
        .stage_bound_target_failure(local.flow_id())
        .unwrap();
    let outputs = executor
        .apply_ready_receipt_observed(close)
        .expect("already acquired CLOSE is independent of the staged RESET")
        .into_progress_parts()
        .0;
    assert_eq!(outputs.len(), 1);
    assert!(matches!(&outputs[0], OwnerTargetOutput::Transmit(frame)
        if matches!(frame.record(), Record::Close { flow_id, final_offset, .. }
            if *flow_id == local.flow_id() && *final_offset == ByteOffset::new(0))));
    assert_eq!(live_record(&executor, local.flow_id()).transit, 0);
    assert_eq!(executor.target().snapshot().read_eof, 1);
    assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
    assert_eq!(executor.snapshot().target_read_owned_segments, 0);
    assert!(
        matches!(executor.flows[&local.flow_id()].pending_completion,
        Some(PendingTargetCompletion::StagedReset(flow)) if flow == local)
    );
    let retired = executor
        .apply_event(SessionEvent::TerminalGraceExpired { terminal })
        .unwrap();
    assert!(!retired.iter().any(|output| matches!(output,
        OwnerTargetOutput::Transmit(frame) if matches!(frame.record(), Record::Reset { .. }))));
    assert_eq!(executor.snapshot().pending_target_completions, 1);
    assert!(
        matches!(executor.flows[&local.flow_id()].pending_completion,
        Some(PendingTargetCompletion::StagedReset(flow)) if flow == local)
    );
    assert_eq!(live_record(&executor, local.flow_id()).transit, 0);
    assert!(
        inbox
            .poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
            .is_pending()
    );
    assert!(turn.finish(&completed).is_ok());
    ready
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
}

#[test]
fn staged_failure_suspends_reads_even_after_a_real_target_notification() {
    use crate::owned_upstream::owner_ready::{
        OwnerReadyCategory as Category, OwnerReadyReadResult,
    };
    let FlowLifecycleFixture {
        mut ready,
        mut executor,
        inbox: _inbox,
        local,
        ..
    } = lifecycle_fixture();
    for lane in [SendLane::Control, SendLane::OrderedSource] {
        let claim = ready.claim(0).unwrap();
        let turn = executor.begin_ready_publication().unwrap();
        let completed = executor.visit_ready_send(lane).unwrap();
        assert!(turn.finish(&completed).is_ok());
        ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
    }
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    let mask = Category::ReadReady.mask() | Category::ReadAttempt.mask() | Category::Credit.mask();
    let selected = executor
        .select_ready_continuation(mask, 0)
        .unwrap()
        .unwrap();
    assert_eq!(selected.category, Category::ReadReady);
    assert!(matches!(
        executor.service_ready_target(&selected).unwrap(),
        OwnerReadyReadResult::Pending
    ));
    let runnable = executor
        .finish_ready_continuation(&selected, OwnerReadyReadResult::Pending, mask)
        .unwrap();
    executor
        .stage_bound_target_failure(local.flow_id())
        .unwrap();
    executor
        .target_mut()
        .feed_read(local.flow_id(), Bytes::from_static(b"late"))
        .unwrap();
    assert!(
        executor
            .select_ready_continuation(mask, 0)
            .unwrap()
            .is_none(),
        "retained failure must exclude reads after the actual registered Target wake"
    );
    assert_eq!(executor.snapshot().pending_target_completions, 1);
    assert_eq!(executor.target().snapshot().buffered_bytes, 4);
    let completed = SessionCommandVisit {
        work: OwnerReadyUnit::Registered,
        runnable,
        blocked: None,
    };
    assert!(turn.finish(&completed).is_ok());
    ready
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
}

#[test]
fn reset_first_keeps_the_original_flow_until_late_data_disposition() {
    let FlowLifecycleFixture {
        mut ready,
        mut executor,
        mut inbox,
        local,
        terminal,
    } = lifecycle_fixture();
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    executor
        .target_mut()
        .feed_read(local.flow_id(), Bytes::from_static(b"late"))
        .unwrap();
    assert!(matches!(
        executor.read_ready_target(local.flow_id()).unwrap(),
        crate::owned_upstream::owner_target::read_source::BoundTargetRead::Data
    ));
    executor
        .stage_bound_target_failure(local.flow_id())
        .unwrap();
    executor
        .apply_event(SessionEvent::TerminalGraceExpired { terminal })
        .unwrap();
    let control = send_source_to_mailbox(&mut executor, SendLane::Control);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Some(reset)) = inbox.poll_selected(SessionMailboxLane::Control, &mut cx) else {
        panic!("the actual RESET receipt must arrive before the queued DATA");
    };
    assert!(turn.finish(&control).is_ok());
    ready
        .finish(&claim, control.runnable, control.blocked)
        .unwrap();
    let claim = ready.claim(0).unwrap();
    let turn = executor.begin_ready_publication().unwrap();
    let completed = send_source_to_mailbox(&mut executor, SendLane::OrderedSource);
    let terminal_outputs = executor
        .apply_ready_receipt_observed(reset)
        .unwrap()
        .into_progress_parts()
        .0;
    assert_eq!(
        terminal_outputs
            .iter()
            .filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.local_flow() == local))
            .count(),
        1
    );
    let retained = executor.flows.get(&local.flow_id()).expect(
        "Target join must retain the original Flow while its actual DATA is still in transit",
    );
    assert_eq!(retained.source.as_ref().unwrap().flow.local_flow(), local);
    assert_eq!(retained.source.as_ref().unwrap().driver.local_flow(), local);
    assert_eq!(executor.snapshot().target_read_owned_bytes, 4);
    assert_eq!(executor.snapshot().target_read_owned_segments, 1);
    let Poll::Ready(Some(data)) = inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
    else {
        panic!("the same ordered mailbox must still own its real DATA");
    };
    assert!(
        matches!(&data, SessionOwnerCommand::Driver(DriverInput::Data(data))
        if data.flow() == local && data.payload() == b"late")
    );
    let disposed = executor
        .apply_ready_receipt_observed(data)
        .expect("late DATA is disposed only under the original full-source terminal fact");
    assert!(
        disposed.into_progress_parts().0.is_empty(),
        "late DATA cannot publish another protocol effect"
    );
    assert_eq!(executor.snapshot().target_read_owned_bytes, 0);
    assert_eq!(executor.snapshot().target_read_owned_segments, 0);
    assert!(
        executor.flows.contains_key(&local.flow_id()),
        "source drain and registration retirement remain nonzero"
    );
    let observed_terminal = terminal_outputs
        .iter()
        .find_map(|output| match output {
            OwnerTargetOutput::TerminalGraceStarted { terminal }
                if terminal.local_flow() == local =>
            {
                Some(*terminal)
            }
            _ => None,
        })
        .unwrap();
    executor
        .apply_event(SessionEvent::TerminalGraceExpired {
            terminal: observed_terminal,
        })
        .expect("retained source custody must not exclude its real terminal grace expiry");
    assert!(
        executor.flows.contains_key(&local.flow_id()),
        "retiring Z is not retiring the original F/source"
    );
    assert!(turn.finish(&completed).is_ok());
    ready
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
}

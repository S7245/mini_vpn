//! Bound failure custody through the unchanged FailOnceTarget Adapter.
//! Owns original-Flow failure tracers, not Controller/actor adoption or cleanup.
//! Uses real plan/factory/ready installation; the ready domain outlives its users.

mod completion;
mod sink;

use super::*;
use crate::owned_upstream::owner_ready::{
    OwnerReadyDrive, OwnerReadySessionLimits, OwnerReadyUnit, ReadyGeometry, ReadyIndex, SendLane,
    SessionCommandVisit,
};
use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
use crate::owned_upstream::owner_target::completion::{
    DriverCompletionState, TargetFailureFeedback,
};
use crate::owned_upstream::owner_target::service::{OwnerFlowServiceSlot, OwnerTargetServiceState};
use crate::owned_upstream::session::{SessionMailboxLane, SessionOwnerInbox};
use crate::owned_upstream::target::MemoryWriteDirective;
use crate::resumable::{
    BitsPerSecond, CommittedLeg, DirectionalRates, LocalFlow, ReplayCapacitySpec,
    ReplayStorageGeometry, TerminalGrace,
};
use std::future::Future;
use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Wake, Waker};
use std::time::Duration;

struct BoundFailureFixture {
    executor: OwnerTargetExecutor<FailOnceTarget>,
    inbox: SessionOwnerInbox,
    leg: CommittedLeg,
    local: LocalFlow,
    terminals: Vec<TerminalGrace>,
    ready: ReadyIndex,
}

fn fixture() -> BoundFailureFixture {
    fixture_with_steps(128)
}

fn fixture_with_steps(steps: usize) -> BoundFailureFixture {
    fixture_with_bounds(steps, 0)
}

fn fixture_with_bounds(steps: usize, occupied: u64) -> BoundFailureFixture {
    fixture_with_cold_failure(steps, occupied, false)
}

fn fixture_with_cold_failure(
    steps: usize,
    occupied: u64,
    fail_before_bind: bool,
) -> BoundFailureFixture {
    let leg = committed_leg(2, 0x22, 0x42);
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(1_000_000).unwrap());
    let storage = ReplayCapacitySpec::new(rates, rates, Duration::from_millis(500), Duration::ZERO)
        .unwrap()
        .derive()
        .unwrap()
        .storage_plan(ReplayStorageGeometry::new(128, 4).unwrap())
        .unwrap();
    let plan = OwnerSessionCapacityPlan::derive(storage, 4, 128).unwrap();
    let supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, leg);
    let mut target_io = FailOnceTarget::new();
    target_io.write = Some(TargetFailure::ConnectionLost);
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(
            steps,
            plan.session_config().owner_pending_work_bound().unwrap(),
            64,
        )
        .unwrap(),
        supervisor,
        target_io,
        source_config(),
    )
    .unwrap();
    let mut terminals = Vec::new();
    for id in 1..=occupied {
        let flow_id = SessionFlowId::new(id).unwrap();
        open_flow(&mut executor, leg, flow_id, target(443));
        let outputs = peer_frame(
            &mut executor,
            leg,
            Record::Reset {
                flow_id,
                reason: ResetReason::TargetFailure,
            },
        )
        .unwrap();
        terminals.push(
            outputs
                .into_iter()
                .find_map(|output| match output {
                    OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(terminal),
                    _ => None,
                })
                .expect("the actual predecessor RESET must occupy real Z"),
        );
    }
    let flow = SessionFlowId::new(occupied + 1).unwrap();
    let outputs = open_flow(&mut executor, leg, flow, target(443));
    for output in outputs {
        if let OwnerTargetOutput::NeedsResume { resume } = output {
            let tail = executor.resume_pending_effects(resume).unwrap();
            assert!(
                !tail
                    .iter()
                    .any(|output| matches!(output, OwnerTargetOutput::NeedsResume { .. }))
            );
        }
    }
    let local = executor.flows[&flow]
        .source
        .as_ref()
        .unwrap()
        .flow
        .local_flow();
    if fail_before_bind {
        let outputs = peer_frame(
            &mut executor,
            leg,
            Record::Data {
                flow_id: flow,
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"cold"),
            },
        )
        .unwrap();
        assert!(transmitted(&outputs).is_empty());
        assert_eq!(executor.target().write_calls, 1);
        assert!(matches!(executor.flows[&flow].pending_completion,
            Some(PendingTargetCompletion::Failure { event: SessionEvent::LocalReset {
                flow: original, reason: ResetReason::TargetFailure,
            } }) if original == local));
        let active = SessionFlowId::new(occupied + 2).unwrap();
        open_flow(&mut executor, leg, active, target(443));
    }
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready = ReadyIndex::cold(&geometry).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 2, 2).unwrap();
    let reservation = ready.reserve_session(leg.session_id(), limits).unwrap();
    let (mut executor, inbox) = match executor.install_owner_ready(reservation, plan) {
        Ok(installed) => installed,
        Err(_) => panic!("original plan and factory must install"),
    };
    let OwnerTargetServiceState::Bound { binding, flows, .. } = &mut executor.service else {
        panic!("original Executor must own Bound service");
    };
    binding
        .activate(&mut ready, 0, &flows.session, 0b1_1111)
        .unwrap();
    BoundFailureFixture {
        executor,
        inbox,
        leg,
        local,
        terminals,
        ready,
    }
}

impl BoundFailureFixture {
    // Caller keeps the selected claim/publication; this transfers exactly one RESET.
    fn send_control_to_inbox(&mut self) -> (SessionCommandVisit, SessionOwnerCommand) {
        assert_eq!(
            self.executor
                .visit_ready_send(SendLane::Control)
                .unwrap()
                .work,
            OwnerReadyUnit::SourcePulled
        );
        let visit = self.executor.visit_ready_send(SendLane::Control).unwrap();
        assert_eq!(
            visit.work,
            OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned)
        );
        let Poll::Ready(Some(reset)) = self.inbox.poll_selected(
            SessionMailboxLane::Control,
            &mut Context::from_waker(Waker::noop()),
        ) else {
            panic!("original paired inbox must own the real RESET");
        };
        (visit, reset)
    }

    fn transit(&self) -> usize {
        self.transit_for(self.local.flow_id())
    }

    fn transit_for(&self, flow: SessionFlowId) -> usize {
        let position = self.executor.flows[&flow].service_position.unwrap();
        let OwnerTargetServiceState::Bound { flows, .. } = &self.executor.service else {
            panic!("same Executor must remain Bound");
        };
        let OwnerFlowServiceSlot::Live(record) = &flows.slots[position] else {
            panic!("actual original Flow must still own its F slot");
        };
        record.transit
    }
    fn write_failure(&mut self) {
        let claim = self.ready.claim(0).unwrap();
        let turn = self.executor.begin_ready_publication().unwrap();
        self.stage_write_failure();
        let visit = self.executor.visit_ready_send(SendLane::Control).unwrap();
        assert_eq!(visit.work, OwnerReadyUnit::SourcePulled);
        assert!(turn.finish(&visit).is_ok());
        self.ready
            .finish(&claim, visit.runnable, visit.blocked)
            .unwrap();
    }

    // Caller owns the original selected publication interval.
    fn stage_write_failure(&mut self) {
        let outputs = peer_frame(
            &mut self.executor,
            self.leg,
            Record::Data {
                flow_id: self.local.flow_id(),
                direction: Direction::ClientToTarget,
                offset: ByteOffset::new(0),
                payload: Bytes::from_static(b"held"),
            },
        )
        .unwrap();
        assert_eq!(self.executor.target().write_calls, 1);
        assert!(self.executor.target().write.is_none());
        assert!(
            transmitted(&outputs).is_empty(),
            "failure feedback is not reduced inline"
        );
    }
}

#[test]
fn bound_failure_fixture_reaches_real_target_write_once_without_inline_reset() {
    let mut fixture = fixture();
    fixture.write_failure();
    let owner = &fixture.executor.flows[&fixture.local.flow_id()];
    assert!(owner.pending_completion.is_some());
    assert!(
        owner.pending_sink.is_none(),
        "Failed write is not the independent sink slot"
    );
    assert!(owner.source.is_some() && owner.target_present);
    assert_eq!(fixture.executor.snapshot().aborted_effect_guards, 0);
    assert_eq!(fixture.executor.target().inner.snapshot().live_flows, 1);
}

#[test]
fn bound_failed_write_retains_original_offer_segments_and_exact_cause() {
    let mut fixture = fixture();
    fixture.write_failure();
    let owner = &fixture.executor.flows[&fixture.local.flow_id()];
    let Some(PendingTargetCompletion::Failed { work, cause, .. }) = &owner.pending_completion
    else {
        panic!("original Flow must retain the actual failed work and cause");
    };
    assert_eq!(*cause, TargetFailure::ConnectionLost);
    let OwnerTargetWork::Effect(SessionEffect::OfferToSink { offer, segments }) = work else {
        panic!("failed write must retain its original OfferToSink, not a value marker");
    };
    assert_eq!(offer.session_id(), fixture.local.session_id());
    assert_eq!(offer.flow_id(), fixture.local.flow_id());
    assert_eq!(offer.direction(), Direction::ClientToTarget);
    assert_eq!(offer.offset(), ByteOffset::new(0));
    assert_eq!(offer.len(), 4);
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].offset(), ByteOffset::new(0));
    assert_eq!(segments[0].end_offset(), ByteOffset::new(4));
    assert_eq!(segments[0].payload().as_ref(), b"held");
    assert_eq!(segments[0].backing_bytes(), 4);
    assert!(owner.pending_sink.is_none());
    assert_eq!(fixture.executor.target().write_calls, 1);
}

#[test]
fn failed_work_survives_real_reset_reduction_until_actual_finish_effect() {
    let mut fixture = fixture_with_steps(1);
    let claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    fixture.stage_write_failure();
    let original_offer = match &fixture.executor.flows[&fixture.local.flow_id()].pending_completion
    {
        Some(PendingTargetCompletion::Failed {
            work: OwnerTargetWork::Effect(SessionEffect::OfferToSink { offer, .. }),
            ..
        }) => *offer,
        _ => panic!("actual failed Offer must already belong to the Flow"),
    };
    let (visit, reset) = fixture.send_control_to_inbox();
    let outputs = fixture
        .executor
        .apply_ready_receipt_observed(reset)
        .unwrap()
        .into_progress_parts()
        .0;
    let owner = &fixture.executor.flows[&fixture.local.flow_id()];
    let Some(PendingTargetCompletion::Failed {
        work: OwnerTargetWork::Effect(SessionEffect::OfferToSink { offer, segments }),
        cause,
        feedback: TargetFailureFeedback::Driver(DriverCompletionState::Reduced(local)),
    }) = &owner.pending_completion
    else {
        panic!("reduction must not release the original failed work before FlowFinished");
    };
    assert_eq!(*offer, original_offer);
    assert_eq!(*cause, TargetFailure::ConnectionLost);
    assert_eq!(*local, fixture.local);
    assert_eq!(segments[0].payload().as_ref(), b"held");
    let resume = outputs
        .into_iter()
        .find_map(|output| match output {
            OwnerTargetOutput::NeedsResume { resume } => Some(resume),
            _ => None,
        })
        .expect("one-step budget must leave the real FlowFinished continuation");
    let outputs = fixture.executor.resume_pending_effects(resume).unwrap();
    assert_eq!(outputs.iter().filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.local_flow() == fixture.local)).count(), 1);
    let owner = &fixture.executor.flows[&fixture.local.flow_id()];
    assert!(matches!(
        owner.pending_completion,
        Some(PendingTargetCompletion::TerminalSettling(_))
    ));
    assert!(owner.source.is_some() && !owner.target_present);
    assert_eq!(fixture.executor.target().write_calls, 1);
    assert_eq!(
        fixture.executor.snapshot().target_flows,
        1,
        "joined Target is not retired source/F"
    );
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
}

struct PanicProgressOnce(AtomicBool);
impl Wake for PanicProgressOnce {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self.0.swap(false, Ordering::SeqCst) {
            panic!("actual factory progress wake unwind");
        }
    }
}

#[test]
fn read_failure_is_owned_before_reservation_release_wake_unwinds() {
    let mut fixture = fixture();
    fixture.executor.target_mut().read = Some(TargetFailure::TimedOut);
    let factory = fixture.executor.source_factory.clone();
    let cursor = factory.progress_cursor();
    let mut wait = Box::pin(factory.wait_for_progress(&cursor));
    let signal = Arc::new(PanicProgressOnce(AtomicBool::new(true)));
    let waker = Waker::from(Arc::clone(&signal));
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let _claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            fixture.executor.read_ready_target(fixture.local.flow_id())
        }))
        .is_err()
    );
    assert!(!signal.0.load(Ordering::SeqCst));
    drop(wait);
    let owner = &fixture.executor.flows[&fixture.local.flow_id()];
    assert!(
        matches!(&owner.pending_completion, Some(PendingTargetCompletion::Failed {
        work: OwnerTargetWork::TargetIoInvariant { flow_id, operation: TargetIoOperation::Read },
        cause: TargetFailure::TimedOut, feedback: TargetFailureFeedback::Prepared(local),
    }) if *flow_id == fixture.local.flow_id() && *local == fixture.local)
    );
    assert_eq!(fixture.executor.snapshot().target_read_owned_bytes, 0);
    assert_eq!(fixture.executor.snapshot().target_read_owned_segments, 0);
    assert!(
        fixture
            .executor
            .read_ready_target(fixture.local.flow_id())
            .is_err()
    );
    assert_eq!(fixture.executor.target().read_calls, 1);
    assert!(owner_source_is_retained(&fixture));
    drop(turn); // Unknown publication is not finished as a successful empty visit.
}

fn owner_source_is_retained(fixture: &BoundFailureFixture) -> bool {
    let owner = &fixture.executor.flows[&fixture.local.flow_id()];
    owner.source.is_some() && owner.target_present && owner.pending_completion.is_some()
}

#[test]
fn write_failure_staging_wake_unwind_retains_original_work_without_second_reset() {
    let mut fixture = fixture();
    let factory = fixture.executor.source_factory.clone();
    let cursor = factory.progress_cursor();
    let mut wait = Box::pin(factory.wait_for_progress(&cursor));
    let signal = Arc::new(PanicProgressOnce(AtomicBool::new(true)));
    let waker = Waker::from(Arc::clone(&signal));
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let _claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| fixture.stage_write_failure())).is_err());
    assert!(!signal.0.load(Ordering::SeqCst));
    drop(wait);
    assert!(
        matches!(&fixture.executor.flows[&fixture.local.flow_id()].pending_completion,
        Some(PendingTargetCompletion::Failed { work: OwnerTargetWork::Effect(
            SessionEffect::OfferToSink { offer, segments }), cause: TargetFailure::ConnectionLost,
            feedback: TargetFailureFeedback::StagingUnknown(local),
        }) if offer.flow_id() == fixture.local.flow_id() && segments[0].payload().as_ref() == b"held"
            && *local == fixture.local)
    );
    assert!(matches!(
        fixture
            .executor
            .stage_retained_failure_reset(fixture.local.flow_id()),
        Err(OwnerTargetError::BoundSourceRequiresSelectedService)
    ));
    assert_eq!(fixture.executor.target().write_calls, 1);
    assert_eq!(fixture.transit(), 0);
    assert!(owner_source_is_retained(&fixture));
    drop(turn);
}

#[test]
fn failed_half_close_retains_original_capability_and_exact_cause() {
    let mut fixture = fixture();
    fixture.executor.target_mut().half_close = Some(TargetFailure::ResourceExhausted);
    let claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    let outputs = peer_frame(
        &mut fixture.executor,
        fixture.leg,
        Record::Close {
            flow_id: fixture.local.flow_id(),
            direction: Direction::ClientToTarget,
            final_offset: ByteOffset::new(0),
        },
    )
    .unwrap();
    assert_eq!(outputs.len(), 1);
    assert!(matches!(&outputs[0], OwnerTargetOutput::Transmit(frame)
        if matches!(frame.record(), Record::Ack { flow_id, direction: Direction::ClientToTarget,
            next_accepted, final_accepted: false }
            if *flow_id == fixture.local.flow_id() && *next_accepted == ByteOffset::new(0))));
    let owner = &fixture.executor.flows[&fixture.local.flow_id()];
    assert!(
        matches!(&owner.pending_completion, Some(PendingTargetCompletion::Failed {
        work: OwnerTargetWork::Effect(SessionEffect::HalfCloseSink { completion }),
        cause: TargetFailure::ResourceExhausted, feedback: TargetFailureFeedback::InTransit(local),
    }) if completion.flow_id() == fixture.local.flow_id() && *local == fixture.local)
    );
    assert!(owner.pending_sink.is_none());
    assert_eq!(fixture.executor.target().half_close_calls, 1);
    let visit = fixture
        .executor
        .visit_ready_send(SendLane::Control)
        .unwrap();
    assert_eq!(visit.work, OwnerReadyUnit::SourcePulled);
    assert!(turn.finish(&visit).is_ok());
    fixture
        .ready
        .finish(&claim, visit.runnable, visit.blocked)
        .unwrap();
}

#[test]
fn ambiguous_reset_admission_retains_failed_work_and_exact_source_error() {
    let mut fixture = fixture();
    let flow_id = fixture.local.flow_id();
    let _claim = fixture.ready.claim(0).unwrap();
    let turn = fixture.executor.begin_ready_publication().unwrap();
    fixture.executor.flows[&flow_id]
        .source
        .as_ref()
        .unwrap()
        .flow
        .try_send_reset(ResetReason::LocalAbandon)
        .unwrap();
    let result = peer_frame(
        &mut fixture.executor,
        fixture.leg,
        Record::Data {
            flow_id,
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"held"),
        },
    );
    assert!(matches!(
        result,
        Err(OwnerTargetError::FlowPort(FlowPortError::TerminalLaneFull))
    ));
    assert!(
        matches!(&fixture.executor.flows[&flow_id].pending_completion,
        Some(PendingTargetCompletion::Failed { work: OwnerTargetWork::Effect(
            SessionEffect::OfferToSink { offer, segments }), cause: TargetFailure::ConnectionLost,
            feedback: TargetFailureFeedback::StagingUnknown(local),
        }) if offer.flow_id() == flow_id && segments[0].payload().as_ref() == b"held"
            && *local == fixture.local)
    );
    let interrupted = fixture.executor.aborted_effects.as_ref().unwrap();
    assert!(
        interrupted.effects.iter().all(|work| !matches!(
            work,
            OwnerTargetWork::Effect(SessionEffect::OfferToSink { .. })
        )),
        "original failed work cannot be duplicated in the interruption queue"
    );
    assert_eq!(fixture.executor.target().write_calls, 1);
    assert_eq!(fixture.transit(), 0);
    assert!(owner_source_is_retained(&fixture));
    drop(turn);
}

#[test]
fn terminal_close_rearms_parked_lanes_without_a_synthetic_first_wake() {
    use crate::owned_upstream::owner_ready::{
        OwnerReadyCategory as Category, OwnerReadyReadResult, OwnerReadySource,
    };
    for failed_read in [true, false] {
        let mut fixture = fixture();
        let flow_id = fixture.local.flow_id();
        let lanes = [
            (SendLane::Control, OwnerReadySource::ControlSend),
            (SendLane::OrderedSource, OwnerReadySource::OrderedSend),
        ];
        for (lane, expected) in lanes {
            let claim = fixture.ready.claim(0).unwrap();
            assert_eq!(claim.source, expected, "failed_read={failed_read}");
            let turn = fixture.executor.begin_ready_publication().unwrap();
            let visit = fixture.executor.visit_ready_send(lane).unwrap();
            assert_eq!(visit.work, OwnerReadyUnit::Waiting);
            assert!(turn.finish(&visit).is_ok());
            let ready = &mut fixture.ready;
            ready.finish(&claim, visit.runnable, visit.blocked).unwrap();
        }
        if failed_read {
            fixture.executor.target_mut().read = Some(TargetFailure::TimedOut);
            fixture
                .executor
                .target_mut()
                .inner
                .feed_read(flow_id, Bytes::from_static(b"unread"))
                .unwrap();
        }
        let claim = fixture.ready.claim(0).unwrap();
        assert_eq!(claim.source, OwnerReadySource::OwnerContinuation);
        let turn = fixture.executor.begin_ready_publication().unwrap();
        let mask =
            Category::ReadReady.mask() | Category::ReadAttempt.mask() | Category::Credit.mask();
        let mut runnable = 0;
        for category in [Category::ReadReady, Category::ReadAttempt] {
            if category == Category::ReadAttempt && !failed_read {
                break;
            }
            let executor = &mut fixture.executor;
            let selected = executor
                .select_ready_continuation(mask, 0)
                .unwrap()
                .unwrap();
            assert_eq!(selected.category, category);
            let result = executor.service_ready_target(&selected).unwrap();
            assert!(matches!(
                (failed_read, category, &result),
                (true, Category::ReadReady, OwnerReadyReadResult::Hint)
                    | (
                        true,
                        Category::ReadAttempt,
                        OwnerReadyReadResult::FailureStaged
                    )
                    | (false, Category::ReadReady, OwnerReadyReadResult::Pending)
            ));
            runnable = executor
                .finish_ready_continuation(&selected, result, mask)
                .unwrap();
        }
        let outputs = peer_frame(
            &mut fixture.executor,
            fixture.leg,
            Record::Reset {
                flow_id,
                reason: ResetReason::TargetFailure,
            },
        )
        .unwrap();
        assert_eq!(outputs.iter().filter(|output| matches!(output,
        OwnerTargetOutput::TerminalGraceStarted { terminal } if terminal.local_flow() == fixture.local)).count(), 1);
        let visit = SessionCommandVisit {
            work: if failed_read {
                OwnerReadyUnit::SourceRead
            } else {
                OwnerReadyUnit::Registered
            },
            runnable,
            blocked: None,
        };
        assert!(turn.finish(&visit).is_ok());
        let ready = &mut fixture.ready;
        ready.finish(&claim, visit.runnable, visit.blocked).unwrap();
        for (lane, expected) in lanes {
            assert_eq!(fixture.ready.select(3).unwrap(), Some(0));
            let claim = fixture.ready.claim(0).unwrap();
            assert_eq!(claim.source, expected, "failed_read={failed_read}");
            let turn = fixture.executor.begin_ready_publication().unwrap();
            if lane == SendLane::Control && failed_read {
                let (_, reset) = fixture.send_control_to_inbox();
                assert_eq!(fixture.transit(), 1);
                let (outputs, _) = fixture
                    .executor
                    .apply_ready_receipt_observed(reset)
                    .unwrap()
                    .into_progress_parts();
                assert!(outputs.is_empty());
                assert_eq!(fixture.transit(), 0);
            }
            let visit = fixture.executor.visit_ready_send(lane).unwrap();
            assert_eq!(visit.work, OwnerReadyUnit::SourceDrained);
            assert_eq!(fixture.transit(), 0);
            let read_calls = fixture.executor.target().read_calls;
            assert_eq!(read_calls, usize::from(failed_read));
            assert_eq!(fixture.executor.snapshot().target_read_owned_bytes, 0);
            let owner = &fixture.executor.flows[&flow_id];
            assert!(owner.source.is_some());
            assert!(matches!(
                owner.pending_completion,
                Some(PendingTargetCompletion::TerminalSettling(_))
            ));
            assert!(turn.finish(&visit).is_ok());
            let ready = &mut fixture.ready;
            ready.finish(&claim, visit.runnable, visit.blocked).unwrap();
        }
    }
}

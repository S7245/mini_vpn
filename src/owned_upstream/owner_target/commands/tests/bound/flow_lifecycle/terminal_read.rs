//! Post-terminal notification isolation on the original Bound read path.
//!
//! Owns the two-Flow fixture and late-wake tracer through actual Target,
//! Driver, paired inbox and protocol output. Does not own physical activation,
//! terminal expiry or whole-root cleanup; original terminal duties stay held.

use super::*;
use crate::owned_upstream::owner_ready::{
    OwnerReadyCategory as Category, OwnerReadyReadResult, OwnerReadySource,
};
use crate::owned_upstream::owner_target::PendingTargetCompletion;

struct TerminalReadFixture {
    executor: OwnerTargetExecutor<MemoryTarget>,
    inbox: SessionOwnerInbox,
    locals: [LocalFlow; 2],
    outputs: Vec<OwnerTargetOutput>,
    // Keep the original domain alive after its installed borrowers are dropped.
    ready: ReadyIndex,
}

fn terminal_read_fixture() -> TerminalReadFixture {
    let leg = source_leg();
    let (plan, mut executor) = lifecycle_executor(leg, 4, 128);
    let ids = [
        SessionFlowId::new(1).unwrap(),
        SessionFlowId::new(2).unwrap(),
    ];
    let mut outputs = Vec::new();
    for flow_id in ids {
        outputs.extend(peer_record(
            &mut executor,
            leg,
            Record::Open {
                flow_id,
                target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
            },
        ));
    }
    let locals = ids.map(|id| {
        executor.flows[&id]
            .source
            .as_ref()
            .unwrap()
            .flow
            .local_flow()
    });
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready = ReadyIndex::cold(&geometry).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 2, 2).unwrap();
    let reservation = ready.reserve_session(leg.session_id(), limits).unwrap();
    let (mut executor, inbox) = match executor.install_owner_ready(reservation, plan) {
        Ok(installed) => installed,
        Err(_) => panic!("original two live Flows must install without a terminal fixture"),
    };
    let OwnerTargetServiceState::Bound { binding, flows, .. } = &mut executor.service else {
        panic!("the original consuming installation must hold the binding");
    };
    binding
        .activate(&mut ready, 0, &flows.session, 0b1_1111)
        .unwrap();
    TerminalReadFixture {
        executor,
        inbox,
        locals,
        outputs,
        ready,
    }
}

#[test]
fn post_terminal_old_read_wake_preserves_independent_flow_data() {
    let mut fixture = terminal_read_fixture();
    let [a, b] = fixture.locals;
    let mask = Category::ReadReady.mask() | Category::ReadAttempt.mask() | Category::Credit.mask();
    let b_record = live_record(&fixture.executor, b.flow_id());
    let b_identity = (
        b_record.association,
        b_record.generation,
        fixture.executor.flows[&b.flow_id()].service_position,
    );
    let mut pending = [false; 2];
    let mut old_a_waker = None;
    let mut terminal = None;

    // First register both real empty Target readers. RESET is applied only
    // after A's actual poll has installed the original publisher in Target.
    for _ in 0..64 {
        assert_eq!(fixture.ready.select(3).unwrap(), Some(0));
        let claim = fixture.ready.claim(0).unwrap();
        let turn = fixture.executor.begin_ready_publication().unwrap();
        let completed = match claim.source {
            OwnerReadySource::ControlSend | OwnerReadySource::OrderedSend => {
                let lane = if claim.source == OwnerReadySource::ControlSend {
                    SendLane::Control
                } else {
                    SendLane::OrderedSource
                };
                fixture.executor.visit_ready_send(lane).unwrap()
            }
            OwnerReadySource::OwnerContinuation => {
                let selected = fixture
                    .executor
                    .select_ready_continuation(mask, 0)
                    .unwrap()
                    .expect("original live readers must reach their first readiness poll");
                let candidate = selected.candidate.as_ref().unwrap();
                let index = fixture
                    .locals
                    .iter()
                    .position(|local| local.flow_id() == candidate.association.flow_id())
                    .unwrap();
                assert_eq!(selected.category, Category::ReadReady);
                let result = fixture.executor.service_ready_target(&selected).unwrap();
                assert!(matches!(result, OwnerReadyReadResult::Pending));
                pending[index] = true;
                if index == 0 {
                    old_a_waker = Some(
                        live_record(&fixture.executor, a.flow_id())
                            .tickets
                            .target_waker(Category::ReadReady)
                            .unwrap(),
                    );
                }
                let runnable = fixture
                    .executor
                    .finish_ready_continuation(&selected, result, mask)
                    .unwrap();
                SessionCommandVisit {
                    work: OwnerReadyUnit::SourceRead,
                    runnable,
                    blocked: None,
                }
            }
            _ => panic!("the original paired inbox has not registered a consumer yet"),
        };
        if pending == [true, true] {
            let outputs = peer_record(
                &mut fixture.executor,
                source_leg(),
                Record::Reset {
                    flow_id: a.flow_id(),
                    reason: ResetReason::TargetFailure,
                },
            );
            terminal = Some(
                outputs
                    .iter()
                    .find_map(|output| match output {
                        OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
                        _ => None,
                    })
                    .expect("actual FlowFinished must publish its original terminal"),
            );
            assert_eq!(terminal.unwrap().local_flow(), a);
            assert!(matches!(
                &fixture.executor.flows[&a.flow_id()].pending_completion,
                Some(PendingTargetCompletion::TerminalSettling(_))
            ));
            fixture.outputs.extend(outputs);
        }
        assert!(turn.finish(&completed).is_ok());
        fixture
            .ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
        if terminal.is_some() {
            break;
        }
    }
    let terminal = terminal.expect("finite real selection must reach A's terminal boundary");
    let target_after_terminal = fixture.executor.target().snapshot();
    assert_eq!(target_after_terminal.read_calls, 0);
    assert_eq!(target_after_terminal.reset_flows, 1);

    // This is the old publisher registered by the real Pending poll, invoked
    // after FlowFinished read suspension AND the previous publication ends.
    // RESET's own synchronous notification cannot satisfy this ordering.
    old_a_waker
        .expect("actual A read registration")
        .wake_by_ref();
    // Both original readers were Pending. Before notifying B, prove there is
    // no read candidate at all: B must not hide an erroneously rearmed A.
    let mut isolated = false;
    for _ in 0..64 {
        if fixture.ready.select(3).unwrap().is_none() {
            isolated = true;
            break;
        }
        let claim = fixture.ready.claim(0).unwrap();
        let turn = fixture.executor.begin_ready_publication().unwrap();
        let completed = match claim.source {
            OwnerReadySource::ControlSend => fixture
                .executor
                .visit_ready_send(SendLane::Control)
                .unwrap(),
            OwnerReadySource::OrderedSend => fixture
                .executor
                .visit_ready_send(SendLane::OrderedSource)
                .unwrap(),
            OwnerReadySource::OwnerContinuation => {
                assert!(
                    fixture
                        .executor
                        .select_ready_continuation(mask, 0)
                        .unwrap()
                        .is_none()
                );
                isolated = true;
                SessionCommandVisit {
                    work: OwnerReadyUnit::Registered,
                    runnable: 0,
                    blocked: None,
                }
            }
            _ => panic!("only original terminal drain work may be pending"),
        };
        assert!(turn.finish(&completed).is_ok());
        fixture
            .ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
        if isolated {
            break;
        }
    }
    assert!(
        isolated,
        "late A notification must not retain runnable read work"
    );
    assert_eq!(fixture.executor.target().snapshot(), target_after_terminal);
    fixture
        .executor
        .target_mut()
        .feed_read(b.flow_id(), Bytes::from_static(b"independent"))
        .unwrap();
    let mut data_reads = 0;
    let mut receipts = 0;
    let mut delivered = false;
    for _ in 0..64 {
        assert_eq!(fixture.ready.select(3).unwrap(), Some(0));
        let claim = fixture.ready.claim(0).unwrap();
        let turn = fixture.executor.begin_ready_publication().unwrap();
        let completed = match claim.source {
            OwnerReadySource::ControlSend | OwnerReadySource::OrderedSend => {
                let lane = if claim.source == OwnerReadySource::ControlSend {
                    SendLane::Control
                } else {
                    SendLane::OrderedSource
                };
                let completed = fixture.executor.visit_ready_send(lane).unwrap();
                if completed.work == OwnerReadyUnit::Transfer(OwnerReadyDrive::MailboxOwned) {
                    assert_eq!(lane, SendLane::OrderedSource);
                    assert_eq!(live_record(&fixture.executor, b.flow_id()).transit, 1);
                    let mut cx = Context::from_waker(Waker::noop());
                    let Poll::Ready(Some(command)) = fixture
                        .inbox
                        .poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
                    else {
                        panic!("the paired inbox must own the original B DATA");
                    };
                    assert!(
                        matches!(&command, SessionOwnerCommand::Driver(DriverInput::Data(data))
                        if data.flow() == b && data.payload() == b"independent")
                    );
                    receipts += 1;
                    fixture.outputs.extend(
                        fixture
                            .executor
                            .apply_ready_receipt_observed(command)
                            .unwrap()
                            .into_progress_parts()
                            .0,
                    );
                    delivered = true;
                }
                completed
            }
            OwnerReadySource::OwnerContinuation => {
                if let Some(selected) = fixture.executor.select_ready_continuation(mask, 0).unwrap()
                {
                    let candidate = selected.candidate.as_ref().unwrap();
                    assert_eq!(
                        candidate.association.flow_id(),
                        b.flow_id(),
                        "late A wake must never reopen its read eligibility"
                    );
                    assert_eq!(
                        (
                            candidate.association,
                            candidate.generation,
                            Some(candidate.position)
                        ),
                        b_identity
                    );
                    let result = fixture.executor.service_ready_target(&selected).unwrap();
                    if matches!(result, OwnerReadyReadResult::Data) {
                        data_reads += 1;
                    }
                    assert!(matches!(
                        result,
                        OwnerReadyReadResult::Hint
                            | OwnerReadyReadResult::Data
                            | OwnerReadyReadResult::Pending
                    ));
                    let runnable = fixture
                        .executor
                        .finish_ready_continuation(&selected, result, mask)
                        .unwrap();
                    SessionCommandVisit {
                        work: OwnerReadyUnit::SourceRead,
                        runnable,
                        blocked: None,
                    }
                } else {
                    SessionCommandVisit {
                        work: OwnerReadyUnit::Registered,
                        runnable: 0,
                        blocked: None,
                    }
                }
            }
            _ => panic!("no unrelated source may substitute for actual B delivery"),
        };
        assert!(turn.finish(&completed).is_ok());
        fixture
            .ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
        if delivered {
            break;
        }
    }
    assert!(
        delivered,
        "B must reach actual reduction, not merely a ready bit"
    );
    assert_eq!((data_reads, receipts), (1, 1));
    let data_frames: Vec<_> = fixture
        .outputs
        .iter()
        .filter_map(|output| match output {
            OwnerTargetOutput::Transmit(frame) if matches!(frame.record(), Record::Data { .. }) => {
                Some(frame)
            }
            _ => None,
        })
        .collect();
    let expected = Frame::try_new(
        source_leg().generation(),
        Record::Data {
            flow_id: b.flow_id(),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"independent"),
        },
    )
    .unwrap();
    assert_eq!(data_frames, vec![&expected]);
    let after = fixture.executor.target().snapshot();
    assert_eq!(after.read_calls, target_after_terminal.read_calls + 1);
    assert_eq!(after.read_delivered_bytes, 11);
    assert_eq!(after.reset_flows, target_after_terminal.reset_flows);
    assert_eq!(after.cancelled_flows, target_after_terminal.cancelled_flows);
    assert_eq!(after.buffered_bytes, 0);
    let record = live_record(&fixture.executor, b.flow_id());
    assert_eq!(
        (
            record.association,
            record.generation,
            fixture.executor.flows[&b.flow_id()].service_position
        ),
        b_identity
    );
    assert_eq!(record.transit, 0);
    for local in [a, b] {
        let source = fixture.executor.flows[&local.flow_id()]
            .source
            .as_ref()
            .unwrap();
        assert_eq!(source.flow.local_flow(), local);
        assert_eq!(source.driver.local_flow(), local);
    }
    assert!(fixture.executor.flows[&b.flow_id()].target_present);
    assert!(
        fixture.executor.flows[&b.flow_id()]
            .pending_completion
            .is_none()
    );
    assert!(matches!(
        &fixture.executor.flows[&a.flow_id()].pending_completion,
        Some(PendingTargetCompletion::TerminalSettling(_))
    ));
    let terminals: Vec<_> = fixture
        .outputs
        .iter()
        .filter_map(|output| match output {
            OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
            _ => None,
        })
        .collect();
    assert_eq!(terminals, vec![terminal]);
    let snapshot = fixture.executor.snapshot();
    assert_eq!(snapshot.session.session.terminal_tombstones(), 1);
    assert_eq!(snapshot.target_flows, 2);
    // Snapshot counts needs_feedback(), not all owning TerminalSettling states.
    // Peer RESET needs no feedback, while its Source/terminal settlement remains.
    assert_eq!(snapshot.pending_target_completions, 0);
    assert_eq!(
        fixture
            .executor
            .flows
            .values()
            .filter(|owner| matches!(
                owner.pending_completion,
                Some(PendingTargetCompletion::TerminalSettling(_))
            ))
            .count(),
        1
    );
    // Reduction transfers the same charged payload into Replay; no peer ACK
    // has released B's original factory permits in this scenario.
    assert_eq!(snapshot.target_read_owned_bytes, 11);
    assert_eq!(snapshot.target_read_owned_segments, 1);
    assert_eq!(snapshot.aborted_effect_guards, 0);
    assert_eq!(snapshot.aborted_recovery_guards, 0);
    assert!(!snapshot.needs_resume);
    // Original A settlement/Source/Z and B replay remain owned. No fabricated
    // expiry, terminal cleanup, physical lifecycle or whole-root-zero claim.
}

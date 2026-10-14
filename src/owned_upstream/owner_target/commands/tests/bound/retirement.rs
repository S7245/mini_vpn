//! Genuine cold-Retiring custody through the original Ready installation.
//!
//! Owns real capacity pressure and selected Retirement, not Controller or
//! physical-root lifecycle, session cleanup or whole-session zero.
//! Uses the ancestor's authenticated source fixture without widening its scope.

use super::*;

#[test]
fn two_retiring_eof_waiters_pass_one_real_vacancy_without_another_transfer() {
    use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
    use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
    use crate::owned_upstream::{owner_ready as ready, owner_target as ot};
    use crate::resumable::{
        BitsPerSecond, DirectionalRates, ReplayCapacitySpec, ReplayStorageGeometry,
    };
    use std::{num::NonZeroUsize, time::Duration};
    let leg = source_leg();
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(1_000_000).unwrap());
    let storage = ReplayCapacitySpec::new(rates, rates, Duration::from_millis(500), Duration::ZERO)
        .unwrap()
        .derive()
        .unwrap()
        .storage_plan(ReplayStorageGeometry::new(128, 4).unwrap())
        .unwrap();
    let plan = OwnerSessionCapacityPlan::derive(storage, 3, 128).unwrap();
    let target = MemoryTarget::new(
        MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
            .unwrap()
            .with_max_terminal_tombstones(1)
            .unwrap(),
    );
    let config = ot::OwnerTargetConfig::new(
        128,
        plan.session_config().owner_pending_work_bound().unwrap(),
        64,
    )
    .unwrap();
    let supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, leg);
    let mut executor = ot::OwnerTargetExecutor::new(
        config,
        supervisor,
        target,
        FlowPortConfig::new(128, 8, 8, 8).unwrap(),
    )
    .unwrap();
    let mut terminals = Vec::new();
    for id in 1..=4 {
        let flow_id = SessionFlowId::new(id).unwrap();
        let mut records = vec![Record::Open {
            flow_id,
            target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
        }];
        if id != 4 {
            records.push(Record::Reset {
                flow_id,
                reason: ResetReason::LocalAbandon,
            });
        }
        for record in records {
            let outputs = executor
                .apply_event(SessionEvent::PeerFrame {
                    leg,
                    frame: Frame::try_new(leg.generation(), record).unwrap(),
                })
                .unwrap();
            assert!(
                !outputs
                    .iter()
                    .any(|o| matches!(o, ot::OwnerTargetOutput::NeedsResume { .. }))
            );
            terminals.extend(outputs.into_iter().filter_map(|o| match o {
                ot::OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(terminal),
                _ => None,
            }));
        }
    }
    assert_eq!(terminals.len(), 3);
    let ids = [terminals[1].flow_id(), terminals[2].flow_id()];
    for terminal in &terminals[1..] {
        executor
            .apply_event(SessionEvent::TerminalGraceExpired {
                terminal: *terminal,
            })
            .unwrap();
    }
    let holder = SessionFlowId::new(4).unwrap();
    let holder_local = executor.flows[&holder]
        .source
        .as_ref()
        .unwrap()
        .flow
        .local_flow();
    for data in [b"one!", b"two!"] {
        executor.flows[&holder]
            .source
            .as_ref()
            .unwrap()
            .flow
            .try_send_uplink_with(4, || Bytes::copy_from_slice(data))
            .unwrap();
    }
    let geometry = ready::ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ready::ReadyIndex::cold(&geometry).unwrap();
    let limits =
        ready::OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 3, 2, 1)
            .unwrap();
    let reservation = index.reserve_session(leg.session_id(), limits).unwrap();
    let (mut executor, mut inbox) = match executor.install_owner_ready(reservation, plan) {
        Ok(pair) => pair,
        Err(_) => panic!("real waiting sources install"),
    };
    let positions = ids.map(|id| executor.flows[&id].service_position.unwrap());
    let holder_position = executor.flows[&holder].service_position.unwrap();
    let ot::service::OwnerTargetServiceState::Bound { binding, flows, .. } = &mut executor.service
    else {
        panic!("bound")
    };
    binding
        .activate(&mut index, 0, &flows.session, 0b1_1111)
        .unwrap();
    let mask = ready::OwnerReadyCategory::Retirement.mask();
    let mut retained_receipt = None;
    let mut eof_visits = [0; 2];
    let mut transfer_count = 0;
    for released in [false, true] {
        if released {
            // The original paired inbox is the actual resource consumer in
            // this component test. Retain, do not discard/reduce, the command
            // or its two original charged permits. This is not a C/root test.
            let mut cx = Context::from_waker(Waker::noop());
            let Poll::Ready(Some(command)) =
                inbox.poll_selected(SessionMailboxLane::OrderedSource, &mut cx)
            else {
                panic!("the real full mailbox must own the first DATA")
            };
            retained_receipt = Some(command);
        }
        let mut reached = false;
        for _ in 0..64 {
            assert_eq!(
                index.select(3).unwrap(),
                Some(0),
                "the next real resource event must be selectable"
            );
            let claim = index.claim(0).unwrap();
            let turn = executor.begin_ready_publication().unwrap();
            let completed = match claim.source {
                ready::OwnerReadySource::ControlSend | ready::OwnerReadySource::OrderedSend => {
                    let lane = if claim.source == ready::OwnerReadySource::ControlSend {
                        SendLane::Control
                    } else {
                        SendLane::OrderedSource
                    };
                    let completed = executor.visit_ready_send(lane).unwrap();
                    if completed.work
                        == ready::OwnerReadyUnit::Transfer(ready::OwnerReadyDrive::MailboxOwned)
                    {
                        transfer_count += 1;
                    }
                    completed
                }
                ready::OwnerReadySource::OwnerContinuation => {
                    let selected = executor
                        .select_ready_continuation(mask, 0)
                        .unwrap()
                        .expect("real Retirement candidate");
                    let candidate = selected.candidate.as_ref().unwrap();
                    let original = positions
                        .iter()
                        .position(|position| *position == candidate.position)
                        .unwrap();
                    let result = executor.service_ready_target(&selected).unwrap();
                    if released {
                        assert!(
                            matches!(result, ready::OwnerReadyReadResult::Hint),
                            "each woken original Source really reaches its ordered EOF"
                        );
                        eof_visits[original] += 1;
                    }
                    let runnable = executor
                        .finish_ready_continuation(&selected, result, mask)
                        .unwrap();
                    ready::SessionCommandVisit {
                        work: ready::OwnerReadyUnit::SourceRead,
                        runnable,
                        blocked: None,
                    }
                }
                _ => panic!("this component has not registered an inbox consumer"),
            };
            assert!(turn.finish(&completed).is_ok());
            index
                .finish(&claim, completed.runnable, completed.blocked)
                .unwrap();
            let ot::service::OwnerTargetServiceState::Bound {
                flows, commands, ..
            } = &executor.service
            else {
                panic!("original command and wait owners")
            };
            let waiting = positions.map(|position| match &flows.slots[position] {
                ot::service::OwnerFlowServiceSlot::Retiring(record) => record.wait.is_some(),
                _ => panic!("actual original F remains Retiring"),
            });
            reached = if !released {
                waiting == [true, true]
                    && matches!(
                        commands.cells[1],
                        SendCell::Held {
                            eligibility: SendEligibility::CapacityBlocked,
                            ..
                        }
                    )
            } else {
                waiting == [false, false] && eof_visits.iter().all(|visits| *visits >= 1)
            };
            if reached {
                break;
            }
        }
        assert!(
            reached,
            "the finite original wait chain must complete this phase"
        );
        assert_eq!(transfer_count, if released { 2 } else { 1 });
    }
    assert!(retained_receipt.is_some());
    let ot::service::OwnerTargetServiceState::Bound {
        flows, commands, ..
    } = &executor.service
    else {
        panic!("original bound resources")
    };
    assert!(matches!(commands.cells[1], SendCell::Vacant));
    let ot::service::OwnerFlowServiceSlot::Live(record) = &flows.slots[holder_position] else {
        panic!("independent holder remains live")
    };
    assert_eq!(record.transit, 2);
    assert_eq!(
        record.association,
        ready::OwnerReadyFlowAssociation::Source(holder_local)
    );
    assert_eq!(executor.snapshot().target_read_owned_bytes, 8);
    assert_eq!(executor.snapshot().target_read_owned_segments, 2);
    assert_eq!(executor.snapshot().target_flows, 3);
    assert_eq!(executor.target().snapshot().joined_flows, 1);
}

#[test]
fn cold_retiring_keeps_original_terminal_through_ready_installation() {
    use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
    use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
    use crate::owned_upstream::{owner_ready as ready, owner_target as ot};
    use crate::resumable::{
        BitsPerSecond, DirectionalRates, ReplayCapacitySpec, ReplayStorageGeometry,
    };
    use std::{num::NonZeroUsize, time::Duration};

    let leg = source_leg();
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(1_000_000).unwrap());
    let horizon = Duration::from_millis(500);
    let spec = ReplayCapacitySpec::new(rates, rates, horizon, Duration::ZERO).unwrap();
    let geometry = ReplayStorageGeometry::new(128, 4).unwrap();
    let storage = spec.derive().unwrap().storage_plan(geometry).unwrap();
    let plan = OwnerSessionCapacityPlan::derive(storage, 2, 128).unwrap();
    let config = MemoryTargetConfig::new(2, 256, 512, 128, 128, 16).unwrap();
    let target = MemoryTarget::new(config.with_max_terminal_tombstones(1).unwrap());
    let pending = plan.session_config().owner_pending_work_bound().unwrap();
    let config = ot::OwnerTargetConfig::new(128, pending, 64).unwrap();
    let supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, leg);
    let source = FlowPortConfig::new(128, 8, 8, 8).unwrap();
    let mut executor = ot::OwnerTargetExecutor::new(config, supervisor, target, source).unwrap();
    let mut terminals = Vec::new();
    for id in [1, 2] {
        let flow_id = SessionFlowId::new(id).unwrap();
        for record in [
            Record::Open {
                flow_id,
                target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
            },
            Record::Reset {
                flow_id,
                reason: ResetReason::LocalAbandon,
            },
        ] {
            let outputs = executor
                .apply_event(SessionEvent::PeerFrame {
                    leg,
                    frame: Frame::try_new(leg.generation(), record).unwrap(),
                })
                .unwrap();
            use ot::OwnerTargetOutput::{NeedsResume, TerminalGraceStarted};
            assert!(!outputs.iter().any(|o| matches!(o, NeedsResume { .. })));
            terminals.extend(outputs.into_iter().filter_map(|output| match output {
                TerminalGraceStarted { terminal } => Some(terminal),
                _ => None,
            }));
        }
    }
    let second = terminals[1].flow_id();
    let [first_terminal, second_terminal] = terminals.as_slice() else {
        panic!("two real terminals")
    };
    let expiry = SessionEvent::TerminalGraceExpired {
        terminal: *second_terminal,
    };
    executor.apply_event(expiry).unwrap();
    let owner = &executor.flows[&second];
    assert!(owner.target_present && owner.join_pending && owner.retire_after_join);
    let local = owner.source.as_ref().unwrap().flow.local_flow();
    assert_eq!(local, terminals[1].local_flow());
    let snapshot = executor.target().snapshot();
    assert_eq!((snapshot.joined_flows, snapshot.live_flows), (1, 1));
    let geometry = ready::ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready = ready::ReadyIndex::cold(&geometry).unwrap();
    let limits =
        ready::OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 2, 2, 2)
            .unwrap();
    let reservation = ready.reserve_session(leg.session_id(), limits).unwrap();
    let (mut executor, _inbox) = match executor.install_owner_ready(reservation, plan) {
        Ok(installed) => installed,
        Err(_) => panic!("real cold Retiring must install without recreating a terminal"),
    };
    let position = executor.flows[&second].service_position.unwrap();
    let ot::service::OwnerTargetServiceState::Bound { flows, .. } = &executor.service else {
        panic!("same bound owner")
    };
    let slot = &flows.slots[position];
    let retiring = matches!(slot, ot::service::OwnerFlowServiceSlot::Retiring(_));
    assert!(retiring);
    // Retire the actual first tombstone, then drive the original pending Flow
    // through real selections. Capacity release itself must not perform joins.
    let expiry = SessionEvent::TerminalGraceExpired {
        terminal: *first_terminal,
    };
    executor
        .apply_event(expiry)
        .expect("original terminal survives capacity release");
    assert!(executor.flows[&second].target_present);
    let ot::service::OwnerTargetServiceState::Bound { binding, flows, .. } = &mut executor.service
    else {
        panic!("original binding")
    };
    binding
        .activate(&mut ready, 0, &flows.session, 1 << 4)
        .unwrap();
    for _ in 0..8 {
        let claim = ready.claim(0).unwrap();
        assert_eq!(claim.source, ready::OwnerReadySource::OwnerContinuation);
        let turn = executor.begin_ready_publication().unwrap();
        let mask = ready::OwnerReadyCategory::Retirement.mask();
        let selected = executor
            .select_ready_continuation(mask, 0)
            .unwrap()
            .expect("retained original work");
        let result = executor.service_ready_target(&selected).unwrap();
        let runnable = executor
            .finish_ready_continuation(&selected, result, mask)
            .unwrap();
        let completed = ready::SessionCommandVisit {
            work: ready::OwnerReadyUnit::SourceRead,
            runnable,
            blocked: None,
        };
        assert!(turn.finish(&completed).is_ok());
        ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
        if !executor.flows[&second].target_present {
            break;
        }
    }
    let owner = &executor.flows[&second];
    assert!(!owner.target_present && !owner.join_pending && owner.retire_after_join);
    assert_eq!(owner.source.as_ref().unwrap().flow.local_flow(), local);
    assert_eq!(owner.source.as_ref().unwrap().driver.local_flow(), local);
    use ot::PendingTargetCompletion::TerminalSettling;
    let settling = matches!(owner.pending_completion, Some(TerminalSettling(_)));
    assert!(settling);
    assert_eq!(executor.snapshot().target_flows, 1);
    let snapshot = executor.target().snapshot();
    assert_eq!((snapshot.joined_flows, snapshot.live_flows), (0, 0));
    // Source destruction remains a distinct charged visit; F is not released
    // merely because Target join reached zero. The next visit commits reuse.
    for remaining in [1, 0] {
        let claim = ready.claim(0).unwrap();
        assert_eq!(claim.source, ready::OwnerReadySource::OwnerContinuation);
        let turn = executor.begin_ready_publication().unwrap();
        let mask = ready::OwnerReadyCategory::Retirement.mask();
        let selected = executor
            .select_ready_continuation(mask, 0)
            .unwrap()
            .unwrap();
        let result = executor.service_ready_target(&selected).unwrap();
        let runnable = executor
            .finish_ready_continuation(&selected, result, mask)
            .unwrap();
        let completed = ready::SessionCommandVisit {
            work: ready::OwnerReadyUnit::SourceRead,
            runnable,
            blocked: None,
        };
        assert!(turn.finish(&completed).is_ok());
        ready
            .finish(&claim, completed.runnable, completed.blocked)
            .unwrap();
        assert_eq!(executor.snapshot().target_flows, remaining);
    }
    let ot::service::OwnerTargetServiceState::Bound { flows, .. } = &executor.service else {
        panic!("same binding")
    };
    assert!(matches!(
        flows.slots[position],
        ot::service::OwnerFlowServiceSlot::Free
    ));
}

#[test]
fn selected_cold_retirement_revokes_source_without_global_retry_or_bulk_drain() {
    use crate::owned_upstream::owner_session_capacity::OwnerSessionCapacityPlan;
    use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
    use crate::owned_upstream::{owner_ready as ready, owner_target as ot};
    use crate::resumable::{
        BitsPerSecond, DirectionalRates, ReplayCapacitySpec, ReplayStorageGeometry,
    };
    use std::{num::NonZeroUsize, time::Duration};

    let leg = source_leg();
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(1_000_000).unwrap());
    let horizon = Duration::from_millis(500);
    let spec = ReplayCapacitySpec::new(rates, rates, horizon, Duration::ZERO).unwrap();
    let geometry = ReplayStorageGeometry::new(128, 4).unwrap();
    let storage = spec.derive().unwrap().storage_plan(geometry).unwrap();
    let plan = OwnerSessionCapacityPlan::derive(storage, 2, 128).unwrap();
    let config = MemoryTargetConfig::new(2, 256, 512, 128, 128, 16).unwrap();
    let target = MemoryTarget::new(config.with_max_terminal_tombstones(1).unwrap());
    let pending = plan.session_config().owner_pending_work_bound().unwrap();
    let config = ot::OwnerTargetConfig::new(128, pending, 64).unwrap();
    let supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, leg);
    let source = FlowPortConfig::new(128, 8, 8, 8).unwrap();
    let mut executor = ot::OwnerTargetExecutor::new(config, supervisor, target, source).unwrap();
    let mut terminals = Vec::new();
    for id in [1, 2] {
        let flow_id = SessionFlowId::new(id).unwrap();
        for record in [
            Record::Open {
                flow_id,
                target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
            },
            Record::Reset {
                flow_id,
                reason: ResetReason::LocalAbandon,
            },
        ] {
            let outputs = executor
                .apply_event(SessionEvent::PeerFrame {
                    leg,
                    frame: Frame::try_new(leg.generation(), record).unwrap(),
                })
                .unwrap();
            use ot::OwnerTargetOutput::{NeedsResume, TerminalGraceStarted};
            assert!(!outputs.iter().any(|o| matches!(o, NeedsResume { .. })));
            terminals.extend(outputs.into_iter().filter_map(|output| match output {
                TerminalGraceStarted { terminal } => Some(terminal),
                _ => None,
            }));
        }
    }
    let second = terminals[1].flow_id();
    let [_first_terminal, second_terminal] = terminals.as_slice() else {
        panic!("two real terminals")
    };
    let expiry = SessionEvent::TerminalGraceExpired {
        terminal: *second_terminal,
    };
    executor.apply_event(expiry).unwrap();
    let owner = &executor.flows[&second];
    assert!(owner.target_present && owner.join_pending && owner.retire_after_join);
    let local = owner.source.as_ref().unwrap().flow.local_flow();
    assert_eq!(local, terminals[1].local_flow());
    let snapshot = executor.target().snapshot();
    assert_eq!((snapshot.joined_flows, snapshot.live_flows), (1, 1));
    let geometry = ready::ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut ready = ready::ReadyIndex::cold(&geometry).unwrap();
    let limits =
        ready::OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 2, 2, 2)
            .unwrap();
    let reservation = ready.reserve_session(leg.session_id(), limits).unwrap();
    let (mut executor, _inbox) = match executor.install_owner_ready(reservation, plan) {
        Ok(installed) => installed,
        Err(_) => panic!("real cold Retiring must install without recreating a terminal"),
    };
    let position = executor.flows[&second].service_position.unwrap();
    let ot::service::OwnerTargetServiceState::Bound { flows, .. } = &executor.service else {
        panic!("same bound owner")
    };
    let slot = &flows.slots[position];
    let retiring = matches!(slot, ot::service::OwnerFlowServiceSlot::Retiring(_));
    assert!(retiring);
    // Genuine original Source ownership stays queued across this first unit.
    let source = executor.flows[&second].source.as_ref().unwrap();
    source
        .flow
        .try_send_uplink_with(4, || Bytes::from_static(b"held"))
        .unwrap();
    assert_eq!(executor.snapshot().target_read_owned_bytes, 4);
    let ot::service::OwnerTargetServiceState::Bound { binding, flows, .. } = &mut executor.service
    else {
        panic!("same installed owner")
    };
    let generation = match &flows.slots[position] {
        ot::service::OwnerFlowServiceSlot::Retiring(record) => record.generation,
        _ => panic!("actual grace expiry must import Retiring"),
    };
    // Component activation only: no physical Root adoption is claimed.
    binding
        .activate(&mut ready, 0, &flows.session, 1 << 4)
        .unwrap();
    assert_eq!(ready.select(3).unwrap(), Some(0));
    let claim = ready.claim(0).unwrap();
    assert_eq!(claim.source, ready::OwnerReadySource::OwnerContinuation);
    let turn = executor.begin_ready_publication().unwrap();
    let mask = ready::OwnerReadyCategory::Retirement.mask();
    let selected = executor
        .select_ready_continuation(mask, 0)
        .unwrap()
        .expect("actual imported Retirement is due without a manual wake");
    assert!(matches!(
        selected.category,
        ready::OwnerReadyCategory::Retirement
    ));
    let candidate = selected.candidate.as_ref().unwrap();
    assert_eq!(candidate.position, position);
    assert_eq!(candidate.generation, generation);
    assert_eq!(
        candidate.association,
        ready::OwnerReadyFlowAssociation::Source(local)
    );
    assert_eq!(executor.target().snapshot().joined_flows, 1);
    assert!(executor.flows[&second].target_present);
    // Do not release the first tombstone: global retry cannot do the work.
    let result = executor
        .service_ready_target(&selected)
        .expect("selected Retirement must revoke the same cold Source");
    let owner = &executor.flows[&second];
    let source = owner.source.as_ref().unwrap();
    assert_eq!(source.flow.local_flow(), local);
    assert_eq!(source.driver.local_flow(), local);
    assert!(matches!(
        source.flow.try_reserve_uplink_source(1),
        Err(crate::owned_upstream::tcp::FlowPortError::Closed)
    ));
    assert!(owner.target_present && owner.join_pending && owner.retire_after_join);
    assert!(matches!(
        owner.pending_completion,
        Some(ot::PendingTargetCompletion::TerminalSettling(_))
    ));
    assert_eq!(executor.snapshot().target_flows, 1);
    assert_eq!(executor.snapshot().target_read_owned_bytes, 4);
    assert_eq!(executor.snapshot().target_read_owned_segments, 1);
    assert_eq!(executor.target().snapshot().joined_flows, 1);
    let runnable = executor
        .finish_ready_continuation(&selected, result, mask)
        .unwrap();
    let completed = ready::SessionCommandVisit {
        work: ready::OwnerReadyUnit::Waiting,
        runnable,
        blocked: None,
    };
    assert!(turn.finish(&completed).is_ok());
    ready
        .finish(&claim, completed.runnable, completed.blocked)
        .unwrap();
    assert_eq!(
        ready.select(3).unwrap(),
        Some(0),
        "queued retirement work must remain selectable"
    );
}

//! Checked process/service arithmetic and real cold-constructor input return.
//! Rejection cases cover error precedence, not legal-layout allocator failure.

use super::*;
use std::future::Future;
use std::pin::pin;
use std::task::Waker;

#[test]
fn ready_process_plan_rejects_service_product_overflow_before_allocation() {
    // Every cell capacity is small and the original budget sum still fits.
    // Allocation absence is a constructor-source property, not this observation.
    assert!(matches!(
        ReadyProcessPlan::checked(1, [usize::MAX - 3, 1, 1, 1]),
        Err(ReadyProcessPlanError::ServiceBoundOverflow {
            class: ReadyClass::Settlement,
        })
    ));
}

#[test]
fn ready_process_plan_preserves_four_budgets_and_exact_class_bounds() {
    let plan = ReadyProcessPlan::checked(3, [1, 2, 7, 1]).unwrap();
    assert_eq!(plan.geometry.session_capacity(), 3);
    assert_eq!(plan.budgets, [1, 2, 7, 1]);
    assert_eq!(plan.geometry.process_cells(), 13);
    assert_eq!(plan.geometry.class_capacities(), [4, 3, 3, 3]);
    assert_eq!(plan.turn_budget, 11);
    assert_eq!(plan.turn_bounds, [5, 3, 2, 4]);
    assert_eq!(plan.quantum_bounds, [55, 33, 22, 44]);
}

#[test]
fn ready_process_plan_rejects_zero_sessions_before_budgets() {
    assert_eq!(
        ReadyProcessPlan::checked(0, [0; 4]).unwrap_err(),
        ReadyProcessPlanError::ZeroSessions
    );
}

#[test]
fn ready_process_plan_rejects_each_zero_budget_before_capacity_math() {
    assert_eq!(
        ReadyProcessPlan::checked(1, [0; 4]).unwrap_err(),
        ReadyProcessPlanError::ZeroClassBudget {
            class: ReadyClass::Settlement,
        }
    );
    for (index, class) in ReadyClass::ALL.into_iter().enumerate() {
        let mut budgets = [1; 4];
        budgets[index] = 0;
        assert_eq!(
            ReadyProcessPlan::checked(usize::MAX, budgets).unwrap_err(),
            ReadyProcessPlanError::ZeroClassBudget { class }
        );
    }
}

#[test]
fn ready_process_plan_rejects_capacity_overflow_before_budget_sum() {
    for sessions in [usize::MAX / 4 + 1, usize::MAX] {
        assert_eq!(
            ReadyProcessPlan::checked(sessions, [usize::MAX, 1, 1, 1]).unwrap_err(),
            ReadyProcessPlanError::ProcessCapacityOverflow
        );
    }
}

#[test]
fn ready_process_plan_rejects_original_budget_sum_overflow() {
    assert_eq!(
        ReadyProcessPlan::checked(1, [usize::MAX, 1, 1, 1]).unwrap_err(),
        ReadyProcessPlanError::TotalBudgetOverflow
    );
}

#[test]
fn ready_process_plan_rejects_budget_sum_before_metadata_source_capacity() {
    assert_eq!(
        ReadyProcessPlan::checked(usize::MAX / 4, [usize::MAX, 1, 1, 1]).unwrap_err(),
        ReadyProcessPlanError::TotalBudgetOverflow
    );
}

#[test]
fn ready_process_plan_accepts_near_max_without_clamping_budgets() {
    let half = usize::MAX / 2;
    let budgets = [half - 3, 1, 1, 1];
    let plan = ReadyProcessPlan::checked(1, budgets).unwrap();
    assert_eq!(plan.budgets, budgets);
    assert_eq!(plan.geometry.process_cells(), 5);
    assert_eq!(plan.turn_budget, half);
    assert_eq!(plan.turn_bounds, [2; 4]);
    assert_eq!(plan.quantum_bounds, [usize::MAX - 1; 4]);
}

#[test]
fn ready_process_plan_accepts_exact_max_service_bound_but_not_one_more_budget() {
    let third = usize::MAX / 3;
    let budgets = [1, third - 3, 1, 1];
    let plan = ReadyProcessPlan::checked(1, budgets).unwrap();
    assert_eq!(plan.budgets, budgets);
    assert_eq!(plan.turn_budget, third);
    assert_eq!(plan.turn_bounds, [3, 2, 2, 2]);
    assert_eq!(
        plan.quantum_bounds,
        [usize::MAX, third * 2, third * 2, third * 2]
    );
    assert_eq!(
        ReadyProcessPlan::checked(1, [1, third - 2, 1, 1]).unwrap_err(),
        ReadyProcessPlanError::ServiceBoundOverflow {
            class: ReadyClass::Settlement,
        }
    );
}

#[test]
fn ready_process_plan_checks_service_products_for_each_later_class() {
    for (index, class) in ReadyClass::ALL.into_iter().enumerate().skip(1) {
        let mut budgets = [usize::MAX / 3 - 7, 3, 3, 3];
        budgets[index] = 1;
        assert_eq!(
            ReadyProcessPlan::checked(3, budgets).unwrap_err(),
            ReadyProcessPlanError::ServiceBoundOverflow { class }
        );
    }
}

fn open_for_cold_rejection(
    model: &mut SessionModel,
    leg: CommittedLeg,
    flow: u64,
) -> PeerOpenRequest {
    model
        .reduce(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::Open {
                    flow_id: SessionFlowId::new(flow).unwrap(),
                    target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
                },
            )
            .unwrap(),
        })
        .unwrap()
        .into_iter()
        .find_map(|effect| match effect {
            SessionEffect::PeerOpenRequested { request, .. } => Some(request),
            _ => None,
        })
        .unwrap()
}

fn assert_cold_plan(plan: &ReadyProcessPlan, sessions: usize) {
    assert_eq!(plan.geometry.session_capacity(), sessions);
    assert_eq!(plan.geometry.process_cells(), 4 * sessions + 1);
    assert_eq!(
        plan.geometry.class_capacities(),
        [sessions + 1, sessions, sessions, sessions]
    );
    assert_eq!(plan.budgets, [1; 4]);
    assert_eq!(plan.turn_budget, 4);
    assert_eq!(
        plan.turn_bounds,
        [sessions + 2, sessions + 1, sessions + 1, sessions + 1]
    );
    assert_eq!(plan.quantum_bounds, plan.turn_bounds.map(|turns| turns * 4));
}

fn returned_cold_command(
    commands: &OwnerCommandPrototype,
    request: PeerOpenRequest,
) -> *const SessionOwnerCommand {
    commands.capture_returned_cold_control(request)
}

fn reduce_cold_receipt(
    model: &mut SessionModel,
    inbox: &mut SessionOwnerInbox,
    request: PeerOpenRequest,
    result: OpenResultCode,
) {
    let Poll::Ready(Some(SessionOwnerCommand::Event(event))) = pin!(inbox.recv_next())
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("the original mailbox must deliver its actual completion");
    };
    assert_eq!(event, SessionEvent::PeerOpenResolved { request, result });
    let effects = model.reduce(event).unwrap();
    assert!(effects.iter().any(|effect| matches!(effect,
        SessionEffect::Transmit(frame) if matches!(frame.record(),
            Record::OpenResult { flow_id, result: actual }
                if *flow_id == request.flow_id() && *actual == result))));
}

#[test]
fn cold_root_rejects_duplicate_residents_before_layout_and_returns_exact_inputs() {
    let (mut model, leg) = owner_session();
    let request = open_for_cold_rejection(&mut model, leg, 1);
    let prefill = open_for_cold_rejection(&mut model, leg, 2);
    let fresh_request = open_for_cold_rejection(&mut model, leg, 3);
    assert_eq!(request.session_id(), fresh_request.session_id());
    assert_ne!(request, fresh_request);
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    let (fresh_ingress, mut fresh_inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    ingress
        .try_send_event(SessionEvent::PeerOpenResolved {
            request: prefill,
            result: OpenResultCode::PolicyDenied,
        })
        .unwrap();
    let mut full = OwnerCommandPrototype::from_open_completion(
        ingress.clone(),
        request,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    assert_eq!(full.drive_one_unit(), OwnerReadyDrive::Full);
    let original_full = returned_cold_command(&full, request);
    let fresh = OwnerCommandPrototype::from_open_completion(
        fresh_ingress.clone(),
        fresh_request,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    let mut commands = Vec::with_capacity(4);
    commands.push(fresh);
    commands.push(full);
    let original_vec = (commands.as_ptr(), commands.len(), commands.capacity());
    assert!(matches!(
        ingress.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));
    assert!(matches!(
        fresh_ingress.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));
    let sessions = isize::MAX as usize / std::mem::size_of::<CommandResident>() + 1;
    assert!(std::alloc::Layout::array::<CommandResident>(sessions).is_err());
    let plan = ReadyProcessPlan::checked(sessions, [1; 4]).unwrap();
    assert_cold_plan(&plan, sessions);

    let Err(OwnerServerBuildFailure {
        plan,
        mut commands,
        cause,
    }) = OwnerServerRoot::from_commands(plan, commands)
    else {
        panic!("duplicate real session residents must be rejected before Layout");
    };
    assert_eq!(cause, scheduler::ReadyFault::InvalidResident);
    assert_cold_plan(&plan, sessions);
    assert_eq!(
        (commands.as_ptr(), commands.len(), commands.capacity()),
        original_vec
    );
    assert_eq!(returned_cold_command(&commands[1], request), original_full);
    commands[0].fresh_ready_control(fresh_request);
    commands[0].ordered_vacant();
    commands[0].command_quarantined(false);
    commands[1].control_capacity_blocked();
    assert_eq!(commands[1].drive_one_unit(), OwnerReadyDrive::NoRunnable);
    assert!(matches!(
        ingress.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));
    assert!(matches!(
        fresh_ingress.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));
    assert!(
        pin!(fresh_inbox.recv_next())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );

    reduce_cold_receipt(
        &mut model,
        &mut inbox,
        prefill,
        OpenResultCode::PolicyDenied,
    );
    assert_eq!(
        pin!(commands[1].rearm_full_commands())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(OwnerCapacityRearm::Rearmed)
    );
    assert_eq!(returned_cold_command(&commands[1], request), original_full);
    assert_eq!(commands[1].drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    reduce_cold_receipt(
        &mut model,
        &mut inbox,
        request,
        OpenResultCode::TargetRefused,
    );
    assert_eq!(commands[0].drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    reduce_cold_receipt(
        &mut model,
        &mut fresh_inbox,
        fresh_request,
        OpenResultCode::TargetRefused,
    );
    for command in &mut commands {
        command.control_vacant();
        command.ordered_vacant();
        assert_eq!(command.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    }
    for inbox in [&mut inbox, &mut fresh_inbox] {
        assert!(
            pin!(inbox.recv_next())
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        inbox.close();
    }
    drop(commands);
    assert!(ingress.observe_capacity().is_ok());
    assert!(fresh_ingress.observe_capacity().is_ok());
}

#[test]
fn cold_root_rejects_invalid_layout_before_reserve_and_returns_exact_inputs() {
    let (mut model, leg) = owner_session();
    let request = open_for_cold_rejection(&mut model, leg, 1);
    let prefill = open_for_cold_rejection(&mut model, leg, 2);
    let (ingress, mut inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    ingress
        .try_send_event(SessionEvent::PeerOpenResolved {
            request: prefill,
            result: OpenResultCode::PolicyDenied,
        })
        .unwrap();
    let mut full = OwnerCommandPrototype::from_open_completion(
        ingress.clone(),
        request,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    assert_eq!(full.drive_one_unit(), OwnerReadyDrive::Full);
    let original_full = returned_cold_command(&full, request);
    let mut commands = Vec::with_capacity(3);
    commands.push(full);
    let original_vec = (commands.as_ptr(), commands.len(), commands.capacity());
    assert!(matches!(
        ingress.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));
    // N is only a checked value: the actual input contains one live resident.
    // If a reserve moves ahead of validation, this invalid layout yields
    // Reserve instead of Layout, without requesting a huge valid allocation.
    let sessions = isize::MAX as usize / std::mem::size_of::<CommandResident>() + 1;
    assert!(std::alloc::Layout::array::<CommandResident>(sessions).is_err());
    let plan = ReadyProcessPlan::checked(sessions, [1; 4]).unwrap();
    assert_cold_plan(&plan, sessions);

    let Err(OwnerServerBuildFailure {
        plan,
        mut commands,
        cause,
    }) = OwnerServerRoot::from_commands(plan, commands)
    else {
        panic!("a valid resident with invalid Layout must be returned before reserve");
    };
    assert_eq!(cause, scheduler::ReadyFault::Layout);
    assert_cold_plan(&plan, sessions);
    assert_eq!(
        (commands.as_ptr(), commands.len(), commands.capacity()),
        original_vec
    );
    assert_eq!(returned_cold_command(&commands[0], request), original_full);
    commands[0].control_capacity_blocked();
    assert_eq!(commands[0].drive_one_unit(), OwnerReadyDrive::NoRunnable);
    assert!(matches!(
        ingress.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));

    reduce_cold_receipt(
        &mut model,
        &mut inbox,
        prefill,
        OpenResultCode::PolicyDenied,
    );
    assert_eq!(
        pin!(commands[0].rearm_full_commands())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(OwnerCapacityRearm::Rearmed)
    );
    assert_eq!(returned_cold_command(&commands[0], request), original_full);
    assert_eq!(commands[0].drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    reduce_cold_receipt(
        &mut model,
        &mut inbox,
        request,
        OpenResultCode::TargetRefused,
    );
    commands[0].control_vacant();
    commands[0].ordered_vacant();
    assert_eq!(commands[0].drive_one_unit(), OwnerReadyDrive::NoRunnable);
    assert!(
        pin!(inbox.recv_next())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    inbox.close();
    drop(commands);
    assert!(ingress.observe_capacity().is_ok());
}

#[test]
fn production_cold_rejects_actual_resident_layout_before_reserve_and_returns_plan() {
    // Exercise the production entry, not the test-only from_commands adapter.
    // N has valid process arithmetic but cannot describe a resident allocation.
    let sessions = isize::MAX as usize / std::mem::size_of::<CommandResident>() + 1;
    assert!(std::alloc::Layout::array::<CommandResident>(sessions).is_err());
    let plan = ReadyProcessPlan::checked(sessions, [1; 4]).unwrap();
    assert_cold_plan(&plan, sessions);

    let Err(OwnerServerColdBuildFailure { plan, cause }) = OwnerServerRoot::cold(plan) else {
        panic!("production cold must reject the invalid resident Layout before reserve");
    };
    // Premature reserve would return Reserve instead; no huge valid allocation
    // or legal-layout allocator-failure claim is needed for this rejection.
    assert_eq!(cause, scheduler::ReadyFault::Layout);
    assert_cold_plan(&plan, sessions);
}

#[test]
fn production_cold_rejects_aggregate_before_reserve_then_preserves_real_empty_root() {
    use super::super::super::construction::FAIL_RESIDENT_RESERVE;
    use std::alloc::Layout;
    use std::sync::Arc;

    // Search only layouts: never attempt to allocate a huge valid array.
    let (mut low, mut high) = (0, isize::MAX as usize / size_of::<CommandResident>());
    while low < high {
        let middle = low + (high - low) / 2 + 1;
        let fits = ReadyProcessPlan::checked(middle, [1; 4])
            .is_ok_and(|plan| OwnerServerRoot::cold_layout(&plan).is_ok());
        if fits {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let sessions = low + 1;
    let plan = ReadyProcessPlan::checked(sessions, [1; 4]).unwrap();
    assert!(Layout::array::<CommandResident>(sessions).is_ok());
    assert!(scheduler::ReadyIndex::cold_backing_layout(&plan.geometry).is_ok());
    assert_eq!(
        OwnerServerRoot::cold_layout(&plan),
        Err(scheduler::ReadyFault::Layout)
    );
    assert!(!FAIL_RESIDENT_RESERVE.replace(true));
    let Err(OwnerServerColdBuildFailure { plan, cause }) = OwnerServerRoot::cold(plan) else {
        panic!("aggregate rejection must precede any allocation");
    };
    assert_eq!(cause, scheduler::ReadyFault::Layout);
    assert_cold_plan(&plan, sessions);
    assert!(FAIL_RESIDENT_RESERVE.get());

    let plan = ReadyProcessPlan::checked(3, [1; 4]).unwrap();
    let Err(OwnerServerColdBuildFailure { plan, cause }) = OwnerServerRoot::cold(plan) else {
        panic!("same unconsumed one-shot refusal must return the small original plan");
    };
    assert_eq!(cause, scheduler::ReadyFault::Reserve);
    assert_cold_plan(&plan, 3);
    assert!(!FAIL_RESIDENT_RESERVE.get());
    let preparation = match OwnerServerRoot::cold(plan) {
        Ok(preparation) => preparation,
        Err(_) => panic!("small real cold construction must succeed after one-shot refusal"),
    };
    let resident = preparation.root.residents.as_ptr();
    let notify = Arc::as_ptr(&preparation.root.notification);
    assert!(preparation.root.residents.is_empty());
    assert_eq!(preparation.root.residents.capacity(), 3);
    assert_eq!(preparation.root.ready.check_preparation_boundary(0), Ok(()));
    let root = match preparation.start() {
        Ok(root) => root,
        Err(_) => panic!("the same empty prepared root must start without rebuilding storage"),
    };
    assert_eq!(root.residents.as_ptr(), resident);
    assert_eq!(root.residents.capacity(), 3);
    assert_eq!(Arc::as_ptr(&root.notification), notify);
    assert!(Arc::ptr_eq(&root.notification, &root.ready.notification()));
    assert!(matches!(root.in_flight, ReadyVisitState::Idle));
}

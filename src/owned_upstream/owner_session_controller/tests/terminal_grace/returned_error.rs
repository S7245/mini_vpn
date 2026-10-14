//! Checked C custody of real Target errors and terminal observations.
//!
//! Uses the original parent fixture and RESET path. The combined-error fault
//! and nested-error wrapper are explicit injections, not natural reachability
//! claims. Known return and unknown unwind retain the original nested payload;
//! supports the finite nested-fact audit, not recovery bytes or root zero.

use super::*;

#[test]
fn returned_recovery_error_audits_original_payload_before_retry() {
    use crate::owned_upstream::leg_io::{LegEndpointRole, LegIo, LegIoLimits, LegOutboundQueue};
    use crate::owned_upstream::owner_target::OwnerTargetAttachPublication;
    use crate::owned_upstream::two_leg::LegId;
    use crate::resumable::{SessionModel, SessionRole};
    // This diagnostic uses the existing legacy admission seam only to obtain
    // real returned recovery errors. It is NOT checked-Bound normal recovery,
    // physical cleanup, or full-root acceptance.
    let initial = committed_leg();
    let (plan, _, _) = terminal_executor(false);
    let authority = AttachAuthority::new(
        initial.session_id(),
        initial.generation(),
        credentials(),
        AttachPolicy::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
            SESSION_PROTOCOL_VERSION,
            0b1111,
        )
        .unwrap(),
    );
    let supervisor = SessionSupervisor::new_owner(
        SessionModel::new(SessionRole::Owner, plan.session_config(), initial),
        authority,
    )
    .unwrap();
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(
            128,
            plan.session_config().owner_pending_work_bound().unwrap(),
            64,
        )
        .unwrap(),
        supervisor,
        MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
        FlowPortConfig::new(128, 8, 8, 8).unwrap(),
    )
    .unwrap();
    executor
        .apply_event(SessionEvent::LegLost { leg: initial })
        .unwrap();
    let binding = binding(0x43);
    let io = LegIo::for_authenticated_transport(
        LegId::A,
        LegEndpointRole::Owner,
        binding,
        LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
    );
    let request = crate::resumable::AttachRequest::new(
        initial.session_id(),
        LegGeneration::new(3).unwrap(),
        AttachNonce::new([0x33; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    );
    let proof = credentials().prove(&request, &binding).unwrap();
    let OwnerTargetAttachPublication::Installed { pending } = executor
        .accept_owner_attach_for_test(
            io.established_leg(),
            io.established_leg()
                .bind_received_frame(request.to_attach_frame(proof)),
        )
        .unwrap()
    else {
        panic!("real generation-three acceptance");
    };
    let mut queue = LegOutboundQueue::for_leg(io.established_leg(), 8, 8_192).unwrap();
    let recovery = pending.enqueue_acceptance(&mut queue).unwrap();
    let expected = recovery.cold_inventory().unwrap();
    assert_eq!(expected.owner.unexecuted_recoveries, 1);
    assert_eq!(expected.recovery.attached_authorities, 1);
    assert_eq!(expected.recovery.acceptance_receipts, 1);
    assert!(expected.payload.effects > 0 && expected.payload.effect_array_bytes > 0);
    let clock = SessionMonotonicClock::start();
    let mut owner = LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let (_, _, foreign_executor) = terminal_executor(false);
    let mut foreign = LegacyOwnerSessionController::new_for_test(foreign_executor, clock).unwrap();
    let error = foreign
        .begin_attached_recovery_for_test(recovery)
        .unwrap_err();
    let OwnerSessionControllerErrorKind::WrongRecoveryOwner { recovery } = &error.kind else {
        panic!("foreign C returns the actual recovery");
    };
    let identity = std::ptr::from_ref(recovery.as_ref());
    assert_eq!(error.cold_inventory(), Ok(expected));
    assert_eq!(error.cold_inventory(), Ok(expected));
    assert_eq!(std::ptr::from_ref(recovery.as_ref()), identity);
    assert!(foreign.recovery.is_none() && owner.recovery.is_none());
    owner.phase = OwnerSessionControllerPhase::SessionExpiring;
    let error = error
        .retry_attached_recovery_start_for_test(&mut owner)
        .unwrap_err();
    assert!(matches!(
        error.kind,
        OwnerSessionControllerErrorKind::RejectedRecoveryStart { .. }
    ));
    assert_eq!(error.cold_inventory(), Ok(expected));
    assert!(owner.recovery.is_none());
    owner.phase = OwnerSessionControllerPhase::Active;
    assert!(
        error
            .retry_attached_recovery_start_for_test(&mut owner)
            .unwrap()
            .needs_execute()
    );
    let Some(PendingOwnerAttachedRecovery::Initial(recovery)) = &owner.recovery else {
        panic!("only the original owner now holds the unexecuted recovery");
    };
    assert_eq!(recovery.cold_inventory(), Ok(expected));
    let cold = owner.cold_inventory().unwrap();
    assert_eq!(cold.recovery_payload(), expected);
    assert!(cold.error_payload().unwrap().is_exact_zero());
    assert!(!cold.is_exact_zero());
    // Defensive C-local interrupted storage, not a naturally reachable panic.
    // Move the original once; do not fabricate a second recovery or cursor.
    let original = owner.recovery.take();
    let mut outputs = Vec::with_capacity(3);
    outputs.push(OwnerTargetRecoveryOutput::SessionExpired);
    let capacity = outputs.capacity();
    let backing = outputs.as_ptr();
    owner.aborted_recovery = Some(AbortedOwnerRecoveryTurn {
        pending: original,
        outputs,
        observations: None,
    });
    let cold = owner.cold_inventory().unwrap();
    let payload = cold.recovery_payload();
    assert_eq!(cold.controller_retained_recovery_turns(), 1);
    assert_eq!(payload.recovery.cursors, 0);
    assert_eq!(payload.recovery.attached_authorities, 1);
    assert_eq!(payload.recovery.acceptance_receipts, 1);
    assert_eq!(payload.recovery.output_values, 1);
    assert_eq!(payload.recovery.ready_capacity, capacity);
    assert_eq!(
        payload.recovery.container_bytes,
        std::alloc::Layout::array::<OwnerTargetRecoveryOutput>(capacity)
            .unwrap()
            .size()
    );
    assert_eq!(payload.payload, expected.payload);
    assert_eq!(payload.owner, expected.owner);
    assert_eq!(owner.cold_inventory().unwrap(), cold);
    assert_eq!(
        owner.aborted_recovery.as_ref().unwrap().outputs.as_ptr(),
        backing
    );
    let turn = owner.aborted_recovery.take().unwrap();
    owner.recovery = turn.pending;
    assert_eq!(owner.cold_inventory().unwrap().recovery_payload(), expected);
}

#[tokio::test(start_paused = true)]
async fn empty_aborted_recovery_guard_remains_nonzero_without_invented_payload() {
    let (_index, _, mut controller, _, _, _) =
        bound_terminal_controller(Duration::from_millis(50), false);
    let baseline = controller.cold_inventory().unwrap();
    assert!(baseline.recovery_payload().is_exact_zero());
    // Explicit invalid/interrupted empty slot: not evidence of runtime reachability.
    controller.aborted_recovery = Some(AbortedOwnerRecoveryTurn {
        pending: None,
        outputs: Vec::new(),
        observations: None,
    });
    let cold = controller.cold_inventory().unwrap();
    assert_eq!(cold.controller_retained_recovery_turns(), 1);
    assert!(cold.recovery_payload().is_exact_zero());
    assert!(!cold.is_exact_zero());
    assert_eq!(controller.cold_inventory().unwrap(), cold);
    controller.aborted_recovery = None;
    assert_eq!(controller.cold_inventory().unwrap(), baseline);
}

#[tokio::test(start_paused = true)]
async fn bound_target_and_progress_error_retains_original_terminal_bundle() {
    let (_index, leg, mut controller, flow_id, epoch, clock) =
        bound_terminal_controller(Duration::from_millis(50), false);
    controller.fail_next_continuation_progress_for_test();
    controller.executor.exhaust_effect_resume_ids_for_test();
    tokio::time::advance(Duration::from_millis(5)).await;

    let error = controller
        .apply_actor_event(arrived(peer_reset(leg, flow_id), &clock))
        .unwrap_err();
    assert!(
        matches!(
            &error.kind,
            OwnerSessionControllerErrorKind::TargetAndProgress {
                target: OwnerTargetError::EffectResumeIdExhausted,
                progress: OwnerSessionProgressError::MissingActorArrival,
            }
        ),
        "the caller must own both exact errors: {error:?}"
    );
    assert!(!controller.fail_next_continuation_progress);
    let returned_payload = error.cold_inventory().unwrap();
    assert_eq!(returned_payload.target_error_nodes, 1);
    assert_eq!(returned_payload.observation_facts, 0);
    let page = controller
        .aborted_turn
        .as_ref()
        .expect("the failed settlement retains the original observation bundle in C");
    assert!(
        page.target_error.is_none(),
        "the caller alone owns the error"
    );
    assert!(page.outputs.is_empty());
    assert_eq!(page.retained_continuation_count(), 0);
    assert_eq!(
        page.page_origin,
        OwnerContinuationOrigin::Actor(SimTime::from_nanos(5_000_000))
    );
    let fact = page.observations.flow_terminal().unwrap();
    let original = *fact.terminal();
    assert_eq!(original.session_id(), leg.session_id());
    assert_eq!(original.flow_id(), flow_id);
    assert_eq!(fact.generation(), leg.generation());

    let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
        panic!("checked C keeps its original terminal journal");
    };
    assert_eq!(ledger.count, 0, "no partial terminal publication");
    assert!(ledger.head.is_none() && ledger.tail.is_none() && ledger.root.is_none());
    let turn = ledger.turn.as_ref().unwrap();
    let reserved = turn
        .reserved
        .expect("original preflight reserved one birth slot");
    assert!(matches!(
        ledger.slots[reserved].state,
        TerminalSlotState::Vacant
    ));
    assert_eq!(turn.window.observed_at, SimTime::from_nanos(5_000_000));
    assert_eq!(turn.window.deadline, SimTime::from_nanos(55_000_000));
    assert_eq!(turn.window.absolute, epoch + Duration::from_millis(55));

    let snapshot = controller.snapshot();
    assert!(snapshot.failed);
    assert_eq!(snapshot.retained_observations, 1);
    assert_eq!(snapshot.pending_continuations, 0);
    assert_eq!(snapshot.retained_continuations, 0);
    let cold = controller.cold_inventory().unwrap();
    assert!(!cold.is_exact_zero());
    assert_eq!(cold.retained_observations(), 1);
    assert_eq!(cold.retained_target_errors, 0);
    assert_eq!(cold.actor_origin_witnesses(), 1);
    assert_eq!(cold.retained_outputs(), 0);
    assert_eq!(cold.independently_summed_frame_bytes(), 0);
    assert_eq!(cold.stored_frame_bytes(), 0);
    assert_eq!(cold.target_aborted_effect_guards(), 1);
    assert_eq!(cold.target_aborted_effects(), 1);
    let executor = controller.executor.snapshot();

    for _ in 0..2 {
        tokio::time::advance(Duration::from_millis(25)).await;
        let rejected = controller.continue_effects_result_for_test().unwrap_err();
        assert!(matches!(
            rejected.kind,
            OwnerSessionControllerErrorKind::Failed
        ));
        let retry = peer_reset(leg, flow_id);
        let rejected = controller
            .apply_actor_event(arrived(retry.clone(), &clock))
            .unwrap_err();
        let OwnerSessionControllerErrorKind::InactiveActorEvent { phase, input } = rejected.kind
        else {
            panic!("Failed C must return the untouched retry input");
        };
        assert_eq!(phase, OwnerSessionControllerPhase::Failed);
        assert!(*input.event == retry);
        assert!(input.arrival.same_clock(&clock));
        assert_eq!(controller.snapshot(), snapshot);
        assert_eq!(controller.cold_inventory().unwrap(), cold);
        assert_eq!(controller.executor.snapshot(), executor);

        let page = controller.aborted_turn.as_ref().unwrap();
        let fact = page.observations.flow_terminal().unwrap();
        assert_eq!(fact.terminal(), &original);
        assert_eq!(fact.generation(), leg.generation());
        assert!(page.target_error.is_none() && page.outputs.is_empty());
        assert_eq!(page.retained_continuation_count(), 0);
        assert_eq!(
            page.page_origin,
            OwnerContinuationOrigin::Actor(SimTime::from_nanos(5_000_000))
        );
        let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
            panic!("failed reentry cannot replace the original ledger");
        };
        let turn = ledger.turn.as_ref().unwrap();
        assert_eq!(turn.reserved, Some(reserved));
        assert!(matches!(
            ledger.slots[reserved].state,
            TerminalSlotState::Vacant
        ));
        assert_eq!(ledger.count, 0);
        assert!(ledger.head.is_none() && ledger.tail.is_none() && ledger.root.is_none());
        assert_eq!(turn.window.observed_at, SimTime::from_nanos(5_000_000));
        assert_eq!(turn.window.deadline, SimTime::from_nanos(55_000_000));
        assert_eq!(turn.window.absolute, epoch + Duration::from_millis(55));
        assert!(matches!(
            &error.kind,
            OwnerSessionControllerErrorKind::TargetAndProgress {
                target: OwnerTargetError::EffectResumeIdExhausted,
                progress: OwnerSessionProgressError::MissingActorArrival,
            }
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn bound_nested_observation_error_keeps_exact_payload_on_return_and_unwind() {
    for interrupted in [false, true] {
        let (_index, leg, mut controller, flow_id, epoch, clock) =
            bound_terminal_controller(Duration::from_millis(50), false);
        controller.executor.exhaust_effect_resume_ids_for_test();
        tokio::time::advance(Duration::from_millis(5)).await;
        let input = arrived(peer_reset(leg, flow_id), &clock);

        // Establish the original actor-entry admission before using its private
        // settlement seam. No fabricated window, observation or registry state.
        controller.require_active().unwrap();
        controller.require_no_recovery().unwrap();
        assert_eq!(controller.phase, OwnerSessionControllerPhase::Active);
        assert!(!controller.terminal_grace.has_turn());
        assert!(controller.aborted_turn.is_none() && controller.aborted_recovery.is_none());
        assert!(controller.recovery.is_none() && controller.pending_continuation.is_none());
        assert!(controller.legless_high_water.is_none());
        assert!(controller.terminal_quarantine.is_none() && controller.attach_status.is_none());
        assert!(input.arrival.same_clock(&controller.clock));
        let arrived_at = input.arrival.arrived_at();
        let origin = OwnerContinuationOrigin::Actor(arrived_at);
        let observed_at = controller.begin_terminal_grace_turn().unwrap();
        assert_eq!(observed_at, SimTime::from_nanos(5_000_000));
        assert_eq!(arrived_at, observed_at);
        let OwnerSessionActorEvent { event, arrival } = input;
        let original_error = match controller.executor.apply_event_observed(*event) {
            Err(error) => error,
            Ok(_) => panic!("real RESET must exhaust the original effect resume ID"),
        };
        drop(arrival);
        assert!(matches!(
            original_error,
            OwnerTargetError::EffectResumeIdExhausted
        ));
        let original_observations = controller
            .executor
            .take_retained_observations_after_failure()
            .expect("take the original terminal bundle once, leaving the aborted effect tail");
        assert_eq!(original_observations.fact_count(), 1);
        let fact = original_observations.flow_terminal().unwrap();
        let terminal = *fact.terminal();
        let generation = fact.generation();
        assert_eq!(terminal.session_id(), leg.session_id());
        assert_eq!(terminal.flow_id(), flow_id);
        assert_eq!(generation, leg.generation());

        // Inject only the defensive wrapper, moving both real originals once.
        // This is not a claim that a second observation conflict occurred.
        let error = Box::new(original_error);
        let observations = Box::new(original_observations);
        let error_address = std::ptr::from_ref(error.as_ref());
        let observations_address = std::ptr::from_ref(observations.as_ref());
        let nested = OwnerTargetError::PostCommitObservationRetentionInvariant {
            error,
            observations,
            effects: crate::resumable::effect_storage::SessionEffectBatch::empty(),
        };
        let assert_nested = |nested: &OwnerTargetError| {
            let OwnerTargetError::PostCommitObservationRetentionInvariant {
                error,
                observations,
                effects,
            } = nested
            else {
                panic!("retain the entire original nested error, not a replacement summary");
            };
            assert!(effects.is_empty());
            assert_eq!(effects.capacity(), 0);
            assert!(matches!(
                error.as_ref(),
                OwnerTargetError::EffectResumeIdExhausted
            ));
            assert_eq!(std::ptr::from_ref(error.as_ref()), error_address);
            assert_eq!(
                std::ptr::from_ref(observations.as_ref()),
                observations_address
            );
            assert_eq!(observations.fact_count(), 1);
            let fact = observations.flow_terminal().unwrap();
            assert_eq!(fact.terminal(), &terminal);
            assert_eq!(fact.generation(), generation);
            let payload = nested.cold_inventory().unwrap();
            assert_eq!(payload.target_error_nodes, 2);
            assert_eq!(payload.observation_facts, 1);
        };
        let OwnerTerminalGraceState::Bound(ledger) = &mut controller.terminal_grace else {
            panic!("use the original checked C ledger");
        };
        let reserved = ledger.turn.as_ref().unwrap().reserved.unwrap();
        ledger.panic_before_returned_page_progress = interrupted;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            controller.finish_target_error(nested, observed_at, origin)
        }));
        let caller_error = if interrupted {
            assert!(
                result.is_err(),
                "the existing hook must interrupt error settlement"
            );
            None
        } else {
            Some(result.expect("known settlement must return the exact nested error"))
        };

        let assert_custody = |controller: &OwnerSessionController<MemoryTarget>| {
            assert_eq!(controller.phase, OwnerSessionControllerPhase::Failed);
            if let Some(error) = &caller_error {
                let OwnerSessionControllerErrorKind::Target(nested) = &error.kind else {
                    panic!("known return gives the caller the exact original error");
                };
                assert_nested(nested);
                assert_eq!(error.cold_inventory(), nested.cold_inventory());
                assert!(
                    controller.aborted_turn.is_none(),
                    "no second error owner in C"
                );
            } else {
                let page = controller.aborted_turn.as_ref().unwrap();
                assert_nested(page.target_error.as_ref().unwrap());
                assert!(
                    page.observations.is_empty(),
                    "the fact stays nested in the error"
                );
                assert!(page.outputs.is_empty());
                assert_eq!(page.retained_continuation_count(), 0);
                assert_eq!(page.page_origin, origin);
            }
            let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
                panic!("failed settlement cannot replace the original ledger");
            };
            assert!(!ledger.panic_before_returned_page_progress);
            assert_eq!(ledger.count, 0, "no partial terminal publication");
            assert!(ledger.head.is_none() && ledger.tail.is_none() && ledger.root.is_none());
            let turn = ledger.turn.as_ref().unwrap();
            assert_eq!(turn.reserved, Some(reserved));
            assert!(matches!(
                ledger.slots[reserved].state,
                TerminalSlotState::Vacant
            ));
            assert_eq!(turn.window.observed_at, observed_at);
            assert_eq!(turn.window.deadline, SimTime::from_nanos(55_000_000));
            assert_eq!(turn.window.absolute, epoch + Duration::from_millis(55));
        };
        assert_custody(&controller);
        let snapshot = controller.snapshot();
        assert!(snapshot.failed);
        assert_eq!(snapshot.retained_observations, 0);
        assert_eq!(snapshot.pending_continuations, 0);
        assert_eq!(snapshot.retained_continuations, 0);
        let cold = controller.cold_inventory().unwrap();
        assert!(
            !cold.is_exact_zero(),
            "the original Executor tail remains owned"
        );
        assert_eq!(cold.target_aborted_effect_guards(), 1);
        assert_eq!(cold.target_aborted_effects(), 1);
        // Ordinary owners and the original nested payload are separate domains.
        assert_eq!(cold.retained_target_errors, usize::from(interrupted));
        let payload = cold.retained_error_payload.unwrap();
        assert_eq!(payload.observation_facts, usize::from(interrupted));
        assert_eq!(payload.target_error_nodes, 2 * usize::from(interrupted));
        assert_eq!(cold.retained_observations(), 0);
        assert_eq!(cold.target_retained_ordinary_observations(), 0);
        assert_eq!(cold.actor_origin_witnesses(), usize::from(interrupted));
        assert_eq!(cold.retained_outputs(), 0);
        assert_eq!(cold.independently_summed_frame_bytes(), 0);
        assert_eq!(cold.stored_frame_bytes(), 0);
        let executor = controller.executor.snapshot();

        for _ in 0..2 {
            tokio::time::advance(Duration::from_millis(25)).await;
            let rejected = controller.continue_effects_result_for_test().unwrap_err();
            assert!(matches!(
                rejected.kind,
                OwnerSessionControllerErrorKind::Failed
            ));
            let retry = peer_reset(leg, flow_id);
            let input = arrived(retry.clone(), &clock);
            let retry_arrived_at = input.arrival.arrived_at();
            let rejected = controller.apply_actor_event(input).unwrap_err();
            let OwnerSessionControllerErrorKind::InactiveActorEvent { phase, input } =
                rejected.kind
            else {
                panic!("Failed C returns the untouched retry input");
            };
            assert_eq!(phase, OwnerSessionControllerPhase::Failed);
            assert!(*input.event == retry);
            assert!(input.arrival.same_clock(&clock));
            assert_eq!(input.arrival.arrived_at(), retry_arrived_at);
            assert_eq!(controller.snapshot(), snapshot);
            assert_eq!(controller.cold_inventory().unwrap(), cold);
            assert_eq!(controller.executor.snapshot(), executor);
            assert_custody(&controller);
        }
    }
}

#[test]
fn error_cold_audit_counts_original_effect_storage_and_mutation() {
    use crate::owned_upstream::owner_target::OwnerTargetReducerObservations;
    use crate::resumable::effect_storage::SessionEffectBatch;
    for effects in [
        SessionEffectBatch::prepared(3).unwrap(),
        SessionEffectBatch::from_vec(Vec::new()),
        SessionEffectBatch::from_vec(vec![crate::resumable::SessionEffect::SessionExpired]),
    ] {
        // Deliberate audit values, not a claim that a live producer emitted a
        // conflict. Even an empty original batch retains capacity or mutation.
        let expected = (
            effects.len(),
            effects.capacity(),
            usize::from(effects.mutation_started()),
        );
        let error = OwnerTargetError::PostCommitObservationRetentionInvariant {
            error: Box::new(OwnerTargetError::EffectResumeIdExhausted),
            observations: Box::new(OwnerTargetReducerObservations::default()),
            effects,
        };
        for _ in 0..2 {
            let audit = error.cold_inventory().unwrap();
            assert_eq!((audit.target_error_nodes, audit.observation_facts), (2, 0));
            assert_eq!(audit.payload.effect_batches, 1);
            assert_eq!(
                (
                    audit.payload.effects,
                    audit.payload.effect_capacity,
                    audit.payload.mutated_batches
                ),
                expected
            );
            assert_eq!(
                audit.payload.effect_array_bytes,
                std::alloc::Layout::array::<crate::resumable::SessionEffect>(expected.1)
                    .unwrap()
                    .size()
            );
            assert!(!audit.is_exact_zero());
            assert_eq!(error.cold_inventory(), Ok(audit));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn error_cold_audit_follows_original_conflict_fact_through_nested_chain() {
    use crate::owned_upstream::owner_target::{
        OwnerTargetObservationKind, OwnerTargetReducerObservations,
    };
    use crate::resumable::effect_storage::SessionEffectBatch;
    let (_index, leg, mut controller, flow_id, _epoch, _clock) =
        bound_terminal_controller(Duration::from_millis(50), false);
    controller.executor.exhaust_effect_resume_ids_for_test();
    let original = match controller
        .executor
        .apply_event_observed(peer_reset(leg, flow_id))
    {
        Err(error) => error,
        Ok(_) => panic!("real RESET must exhaust the original effect resume ID"),
    };
    let observations = controller
        .executor
        .take_retained_observations_after_failure()
        .unwrap();
    assert_eq!(observations.fact_count(), 1);
    let terminal = *observations.flow_terminal().unwrap().terminal();
    // Only the defensive conflict/retention wrappers are injected. The fact
    // itself is moved out of the real RESET; its Executor tail remains owned.
    let mut error = OwnerTargetError::PostCommitObservationConflict {
        kind: OwnerTargetObservationKind::FlowTerminal,
        observations: Box::new(observations),
    };
    for depth in 1..=8 {
        let inventory = error.cold_inventory().unwrap();
        assert_eq!(inventory.target_error_nodes, depth);
        assert_eq!(inventory.observation_facts, 1);
        if depth < 8 {
            error = OwnerTargetError::PostCommitObservationRetentionInvariant {
                error: Box::new(error),
                observations: Box::new(OwnerTargetReducerObservations::default()),
                effects: SessionEffectBatch::empty(),
            };
        }
    }
    let mut inner = &error;
    while let OwnerTargetError::PostCommitObservationRetentionInvariant { error, .. } = inner {
        inner = error.as_ref();
    }
    let OwnerTargetError::PostCommitObservationConflict { observations, .. } = inner else {
        panic!("audit must borrow, not replace the original leaf");
    };
    assert_eq!(observations.flow_terminal().unwrap().terminal(), &terminal);
    assert!(matches!(
        original,
        OwnerTargetError::EffectResumeIdExhausted
    ));
    assert_eq!(
        controller
            .cold_inventory()
            .unwrap()
            .target_aborted_effects(),
        1
    );
}

#[test]
fn error_cold_audit_checked_sums_never_saturate_or_deduplicate() {
    use crate::owned_upstream::owner_target::error_inventory::{
        ErrorAuditFailure, ErrorColdInventory,
    };
    let one = ErrorColdInventory {
        target_error_nodes: 1,
        observation_facts: 1,
        ..ErrorColdInventory::empty()
    };
    assert_eq!(
        one.checked_add(one).unwrap(),
        ErrorColdInventory {
            target_error_nodes: 2,
            observation_facts: 2,
            ..ErrorColdInventory::empty()
        }
    );
    for full in [
        ErrorColdInventory {
            target_error_nodes: usize::MAX,
            observation_facts: 0,
            ..ErrorColdInventory::empty()
        },
        ErrorColdInventory {
            target_error_nodes: 0,
            observation_facts: usize::MAX,
            ..ErrorColdInventory::empty()
        },
    ] {
        assert_eq!(full.checked_add(ErrorColdInventory::empty()), Ok(full));
        assert_eq!(full.checked_add(one), Err(ErrorAuditFailure::CountOverflow));
    }
    assert!(ErrorColdInventory::empty().is_exact_zero());
    assert!(!one.is_exact_zero());
}

#[test]
fn recovery_payload_every_axis_participates_in_checked_sum_and_zero() {
    use crate::owned_upstream::owner_target::error_inventory::{
        ErrorAuditFailure, ErrorColdInventory, RecoveryPayloadInventory,
    };
    fn axis(value: &mut RecoveryPayloadInventory, index: usize) -> &mut usize {
        match index {
            0 => &mut value.cursors,
            1 => &mut value.output_values,
            2 => &mut value.pending_capacity,
            3 => &mut value.ready_capacity,
            4 => &mut value.container_bytes,
            5 => &mut value.frames,
            6 => &mut value.frame_encoded_bytes,
            7 => &mut value.resumes,
            8 => &mut value.attached_authorities,
            9 => &mut value.acceptance_receipts,
            _ => panic!("exact current recovery metric axes"),
        }
    }
    for index in 0..10 {
        let mut full = ErrorColdInventory::empty();
        *axis(&mut full.recovery, index) = usize::MAX;
        let mut one = ErrorColdInventory::empty();
        *axis(&mut one.recovery, index) = 1;
        assert!(!full.is_exact_zero());
        assert!(!one.is_exact_zero());
        assert_eq!(full.checked_add(ErrorColdInventory::empty()), Ok(full));
        assert_eq!(full.checked_add(one), Err(ErrorAuditFailure::CountOverflow));
        let mut two = ErrorColdInventory::empty();
        *axis(&mut two.recovery, index) = 2;
        assert_eq!(one.checked_add(one), Ok(two));
    }
}

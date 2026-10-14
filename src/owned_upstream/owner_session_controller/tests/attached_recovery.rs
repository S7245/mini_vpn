use bytes::Bytes;

use super::*;
use crate::owned_upstream::leg::{LegTransportReporter, LegTransportTerminalReason};
use crate::owned_upstream::leg_io::{
    EncodedLegTransport, EncodedLegTransportError, LegEndpointRole, LegIo, LegIoLimits,
    LegOutboundQueue, OrderedDeliveryToken,
};
use crate::owned_upstream::owner_target::{
    EnqueuedOwnerTargetAttach, OwnerTargetAttachPublication, PendingOwnerTargetAttachPublication,
};
use crate::owned_upstream::two_leg::{SimTime, WireLane, WireRoute};

#[derive(Default)]
struct CapturingTransport {
    frames: Vec<Bytes>,
}

impl EncodedLegTransport for CapturingTransport {
    fn send_encoded(
        &mut self,
        _now: SimTime,
        _route: WireRoute,
        _lane: WireLane,
        bytes: Bytes,
    ) -> Result<(), EncodedLegTransportError> {
        self.frames.push(bytes);
        Ok(())
    }

    fn send_attach_ordered_encoded(
        &mut self,
        _now: SimTime,
        _route: WireRoute,
        _lane: WireLane,
        bytes: Bytes,
        completion: OrderedDeliveryToken,
    ) -> Result<(), EncodedLegTransportError> {
        self.frames.push(bytes);
        completion.mark_delivered();
        Ok(())
    }
}

fn queued_recovery(
    open_flows: usize,
    max_effect_steps: usize,
    queue_frames: usize,
) -> (
    LegacyOwnerSessionController<MemoryTarget>,
    LegIo,
    LegOutboundQueue,
    EnqueuedOwnerTargetAttach,
) {
    let (controller, endpoint, reporter, queue, recovery) =
        queued_recovery_with_reporter(open_flows, max_effect_steps, queue_frames);
    drop(reporter);
    (controller, endpoint, queue, recovery)
}

fn queued_recovery_with_reporter(
    open_flows: usize,
    max_effect_steps: usize,
    queue_frames: usize,
) -> (
    LegacyOwnerSessionController<MemoryTarget>,
    LegIo,
    LegTransportReporter,
    LegOutboundQueue,
    EnqueuedOwnerTargetAttach,
) {
    let (controller, endpoint, reporter, pending) =
        pending_recovery_with_reporter(open_flows, max_effect_steps);
    let established = endpoint.established_leg();
    let mut queue = LegOutboundQueue::for_leg(established, queue_frames, 8_192).unwrap();
    let recovery = pending.enqueue_acceptance(&mut queue).unwrap();
    (controller, endpoint, reporter, queue, recovery)
}

fn pending_recovery_with_reporter(
    open_flows: usize,
    max_effect_steps: usize,
) -> (
    LegacyOwnerSessionController<MemoryTarget>,
    LegIo,
    LegTransportReporter,
    PendingOwnerTargetAttachPublication,
) {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let initial = committed_leg();
    let model = SessionModel::new(SessionRole::Owner, session_config(), initial);
    let authority = AttachAuthority::new(
        SessionId::new([0x11; 16]).unwrap(),
        LegGeneration::new(2).unwrap(),
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
    let supervisor = SessionSupervisor::new_owner(model, authority).unwrap();
    let executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(max_effect_steps, 1_024, 64).unwrap(),
        supervisor,
        MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
        FlowPortConfig::new(128, 8, 8, 8).unwrap(),
    )
    .unwrap();
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    for raw_flow_id in 1..=open_flows {
        let flow_id = SessionFlowId::new(raw_flow_id as u64).unwrap();
        let mut page = controller
            .apply_actor_event(arrived(
                peer_event(
                    initial,
                    Record::Open {
                        flow_id,
                        target: target(440 + raw_flow_id as u16),
                    },
                ),
                &clock,
            ))
            .unwrap();
        while page.needs_continue() {
            page = controller.continue_effects_result_for_test().unwrap();
        }
    }
    let _ = controller
        .apply_actor_event(arrived(SessionEvent::LegLost { leg: initial }, &clock))
        .unwrap();

    let transport_binding = binding(0x43);
    let (endpoint, reporter) = LegIo::for_authenticated_transport_with_reporter(
        crate::owned_upstream::two_leg::LegId::A,
        LegEndpointRole::Owner,
        transport_binding,
        LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
    );
    let established = endpoint.established_leg();
    let request = crate::resumable::AttachRequest::new(
        SessionId::new([0x11; 16]).unwrap(),
        LegGeneration::new(3).unwrap(),
        AttachNonce::new([0x33; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    );
    let proof = credentials().prove(&request, &transport_binding).unwrap();
    let OwnerTargetAttachPublication::Installed { pending } = controller
        .executor
        .accept_owner_attach_for_test(
            established,
            established.bind_received_frame(request.to_attach_frame(proof)),
        )
        .unwrap()
    else {
        panic!("exact-next owner replacement must install");
    };
    (controller, endpoint, reporter, pending)
}

fn publish_raw_first_page(
    controller: &mut LegacyOwnerSessionController<MemoryTarget>,
    queue: &mut LegOutboundQueue,
    recovery: EnqueuedOwnerTargetAttach,
) -> Vec<OwnerTargetRecoveryOutput> {
    let page = controller
        .executor
        .execute_attached_recovery_observed(recovery, queue)
        .unwrap();
    let (publish, observations) = page.into_progress_parts();
    assert!(
        observations.is_empty(),
        "the current recovery observation alphabet must be structurally returned as empty"
    );
    controller
        .executor
        .publish_attached_recovery_page(publish, queue)
        .unwrap()
}

fn assert_exact_zero_controller_ownership(snapshot: OwnerSessionControllerSnapshot) {
    assert_eq!(snapshot.retained_outputs, 0);
    assert_eq!(snapshot.retained_observations, 0);
    assert_eq!(snapshot.pending_continuations, 0);
    assert_eq!(snapshot.retained_continuations, 0);
    assert_eq!(snapshot.pending_recoveries, 0);
    assert_eq!(snapshot.pending_attach_statuses, 0);
    assert_eq!(snapshot.retained_recoveries, 0);
    assert_eq!(snapshot.retained_recovery_cursors, 0);
    assert_eq!(snapshot.retained_recovery_outputs, 0);
    assert_eq!(snapshot.retained_recovery_observations, 0);
    assert_eq!(snapshot.retained_recovery_attached, 0);
    assert!(!snapshot.recovery_needs_acceptance);
    assert!(!snapshot.recovery_needs_execute);
    assert!(!snapshot.recovery_needs_publish);
    assert!(!snapshot.recovery_handoff_ready);
    assert!(!snapshot.recovery_needs_terminal_discard);
    assert_eq!(snapshot.recovery_terminal_quarantined, 0);
    assert_eq!(snapshot.pending_legless_high_waters, 0);
    assert!(!snapshot.attach_status_needs_enqueue);
    assert!(!snapshot.attach_status_needs_delivery);
}

fn bind_and_discard_recovery_terminal(
    controller: &mut LegacyOwnerSessionController<MemoryTarget>,
    reporter: LegTransportReporter,
) {
    let bound = controller
        .bind_recovery_terminal_for_test(
            reporter
                .report(LegTransportTerminalReason::FatalIo)
                .unwrap(),
        )
        .expect("the exact recovery transport terminal binds once");
    assert_eq!(bound.reason(), LegTransportTerminalReason::FatalIo);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.pending_recoveries, 1);
    assert!(snapshot.recovery_needs_terminal_discard);
    assert_eq!(snapshot.pending_legless_high_waters, 0);

    let discarded = controller
        .discard_recovery_terminal_for_test()
        .expect("the terminal-bound recovery tail is explicitly consumed");
    assert_eq!(discarded.generation(), bound.generation());
    assert_eq!(discarded.reason(), bound.reason());
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.pending_recoveries, 0);
    assert!(!snapshot.recovery_needs_terminal_discard);
    assert_eq!(snapshot.pending_legless_high_waters, 1);
}

#[tokio::test(start_paused = true)]
async fn every_recovery_stage_binds_and_discards_one_exact_terminal() {
    let (mut acceptance, _endpoint, reporter, pending) = pending_recovery_with_reporter(0, 128);
    acceptance.recovery = Some(PendingOwnerAttachedRecovery::Acceptance(pending));
    bind_and_discard_recovery_terminal(&mut acceptance, reporter);

    let (mut initial, _endpoint, reporter, _queue, recovery) =
        queued_recovery_with_reporter(0, 128, 4);
    let _ = initial.begin_attached_recovery_for_test(recovery).unwrap();
    bind_and_discard_recovery_terminal(&mut initial, reporter);

    let (mut execute, _endpoint, reporter, mut queue, recovery) =
        queued_recovery_with_reporter(1, 1, 4);
    let _ = execute.begin_attached_recovery_for_test(recovery).unwrap();
    let _ = execute.execute_attached_recovery_for_test(&queue).unwrap();
    let execute_drive = execute
        .publish_attached_recovery_for_test(&mut queue)
        .unwrap();
    assert!(execute_drive.needs_execute());
    bind_and_discard_recovery_terminal(&mut execute, reporter);

    let (mut publish, _endpoint, reporter, queue, recovery) =
        queued_recovery_with_reporter(1, 1, 4);
    let _ = publish.begin_attached_recovery_for_test(recovery).unwrap();
    let _ = publish.execute_attached_recovery_for_test(&queue).unwrap();
    assert!(publish.snapshot().recovery_needs_publish);
    bind_and_discard_recovery_terminal(&mut publish, reporter);

    let (mut handoff, _endpoint, reporter, mut queue, recovery) =
        queued_recovery_with_reporter(0, 128, 4);
    let _ = handoff.begin_attached_recovery_for_test(recovery).unwrap();
    let _ = handoff.execute_attached_recovery_for_test(&queue).unwrap();
    let handoff_drive = handoff
        .publish_attached_recovery_for_test(&mut queue)
        .unwrap();
    assert!(handoff_drive.handoff_ready());
    bind_and_discard_recovery_terminal(&mut handoff, reporter);
}

#[tokio::test(start_paused = true)]
async fn wrong_recovery_terminal_restores_the_exact_stage_and_terminal() {
    let (mut controller, _endpoint, reporter, _queue, recovery) =
        queued_recovery_with_reporter(0, 128, 4);
    let _ = controller
        .begin_attached_recovery_for_test(recovery)
        .unwrap();
    let (wrong_endpoint, wrong_reporter) = LegIo::for_authenticated_transport_with_reporter(
        crate::owned_upstream::two_leg::LegId::B,
        LegEndpointRole::Owner,
        binding(0x77),
        LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
    );
    let failure = controller
        .bind_recovery_terminal_for_test(
            wrong_reporter
                .report(LegTransportTerminalReason::PeerClosed)
                .unwrap(),
        )
        .expect_err("another physical endpoint cannot bind the recovery");
    assert_eq!(format!("{failure:?}"), "WrongLegTerminal");
    assert_eq!(
        failure.into_terminal().reason(),
        LegTransportTerminalReason::PeerClosed
    );
    assert!(controller.snapshot().recovery_needs_execute);
    bind_and_discard_recovery_terminal(&mut controller, reporter);
    drop(wrong_endpoint);
}

fn install_completed_handoff(
    controller: &mut LegacyOwnerSessionController<MemoryTarget>,
    endpoint: &LegIo,
) -> (LegGeneration, AttachNonce) {
    let mut active = None;
    let generation = controller
        .install_recovery_handoff_for_test(&mut active, endpoint.established_leg())
        .expect("the controller installs through the exact physical endpoint")
        .expect("one completed recovery is ready for installation");
    let attached = active
        .as_ref()
        .expect("the root-local active slot owns the completed leg");
    assert_eq!(attached.generation(), generation);
    (generation, attached.nonce())
}

#[tokio::test(start_paused = true)]
async fn controller_owns_second_page_and_publish_pressure_never_reexecutes_target() {
    let (mut controller, mut endpoint, mut queue, recovery) = queued_recovery(1, 1, 1);
    let open_attempts = controller.executor.target().snapshot().open_attempts;

    let begin = controller
        .begin_attached_recovery_for_test(recovery)
        .unwrap();
    assert!(begin.needs_execute());
    assert!(!begin.needs_publish());

    let first_execute = controller
        .execute_attached_recovery_for_test(&queue)
        .unwrap();
    assert!(!first_execute.needs_execute());
    assert!(first_execute.needs_publish());
    assert_eq!(queue.len(), 1, "execute/observe cannot mutate the FIFO");

    let first_publish = controller
        .publish_attached_recovery_for_test(&mut queue)
        .unwrap();
    assert!(first_publish.needs_execute());
    assert!(!first_publish.needs_publish());
    assert!(first_publish.outputs().iter().any(|output| matches!(
        output,
        OwnerSessionRecoveryOutput::LegActivated { generation }
            if generation.get() == 3
    )));

    let second_execute = controller
        .execute_attached_recovery_for_test(&queue)
        .unwrap();
    assert!(!second_execute.needs_execute());
    assert!(second_execute.needs_publish());
    assert_eq!(
        controller.executor.target().snapshot().open_attempts,
        open_attempts
    );

    let pressure = controller
        .publish_attached_recovery_for_test(&mut queue)
        .unwrap_err();
    assert_eq!(format!("{pressure:?}"), "RecoveryQueueBlocked");
    assert_eq!(
        controller.executor.target().snapshot().open_attempts,
        open_attempts
    );
    assert!(controller.snapshot().recovery_needs_publish);

    let mut transport = CapturingTransport::default();
    queue
        .try_flush(&mut endpoint, &mut transport, SimTime::ZERO)
        .unwrap()
        .unwrap();
    let completed = controller
        .publish_attached_recovery_for_test(&mut queue)
        .unwrap();
    assert!(!completed.needs_execute());
    assert!(!completed.needs_publish());
    assert!(completed.handoff_ready());
    let (generation, nonce) = install_completed_handoff(&mut controller, &endpoint);
    assert_eq!(generation.get(), 3);
    assert_eq!(nonce, AttachNonce::new([0x33; 16]).unwrap());
    assert_eq!(
        controller.executor.target().snapshot().open_attempts,
        open_attempts
    );
    assert_exact_zero_controller_ownership(controller.snapshot());
}

#[tokio::test(start_paused = true)]
async fn active_recovery_blocks_every_ordinary_controller_entry_without_consuming_input() {
    let (mut controller, _endpoint, _queue, recovery) = queued_recovery(1, 1, 2);
    let flow_id = SessionFlowId::new(1).unwrap();
    let target_before = controller.executor.target().snapshot();
    let _ = controller
        .begin_attached_recovery_for_test(recovery)
        .unwrap();

    assert_eq!(
        format!("{:?}", controller.try_read_target(flow_id).unwrap_err()),
        "RecoveryRequired"
    );
    assert_eq!(
        format!("{:?}", controller.retry_target_write(flow_id).unwrap_err()),
        "RecoveryRequired"
    );
    assert_eq!(
        format!(
            "{:?}",
            controller.continue_effects_result_for_test().unwrap_err()
        ),
        "RecoveryRequired"
    );

    let actor = arrived(
        peer_event(
            committed_leg(),
            Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(0),
                final_accepted: false,
            },
        ),
        &controller.clock,
    );
    let rejected = controller.apply_actor_event(actor).unwrap_err();
    assert_eq!(
        format!("{rejected:?}"),
        "RejectedActorEvent(AttachBarrierPending)"
    );
    let rejected_again = rejected.retry_actor_event(&mut controller).unwrap_err();
    assert_eq!(
        format!("{rejected_again:?}"),
        "RejectedActorEvent(AttachBarrierPending)",
        "retry must preserve the exact actor event while recovery remains active"
    );

    assert_eq!(controller.executor.target().snapshot(), target_before);
    assert!(controller.snapshot().recovery_needs_execute);
}

#[tokio::test(start_paused = true)]
async fn wrong_publish_queue_restores_the_exact_cursor_for_the_right_queue() {
    let (mut controller, endpoint, mut queue, recovery) = queued_recovery(0, 128, 2);
    let wrong_endpoint = LegIo::for_authenticated_transport(
        crate::owned_upstream::two_leg::LegId::B,
        LegEndpointRole::Owner,
        binding(0x44),
        LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
    );
    let mut wrong_queue =
        LegOutboundQueue::for_leg(wrong_endpoint.established_leg(), 2, 8_192).unwrap();

    let _ = controller
        .begin_attached_recovery_for_test(recovery)
        .unwrap();
    let executed = controller
        .execute_attached_recovery_for_test(&queue)
        .unwrap();
    assert!(executed.needs_publish());

    let rejected = controller
        .publish_attached_recovery_for_test(&mut wrong_queue)
        .unwrap_err();
    assert_eq!(
        format!("{rejected:?}"),
        "RejectedRecoveryPublish(WrongAcceptanceQueue)"
    );
    assert!(controller.snapshot().recovery_needs_publish);
    assert_eq!(queue.len(), 1);
    assert_eq!(wrong_queue.len(), 0);

    let completed = controller
        .publish_attached_recovery_for_test(&mut queue)
        .unwrap();
    assert!(completed.handoff_ready());
    assert_eq!(
        install_completed_handoff(&mut controller, &endpoint)
            .0
            .get(),
        3
    );
    assert_exact_zero_controller_ownership(controller.snapshot());
}

#[tokio::test(start_paused = true)]
async fn rejected_start_retries_the_same_recovery_without_exposing_its_capability() {
    let (mut controller, endpoint, mut queue, recovery) = queued_recovery(0, 128, 2);
    controller.phase = OwnerSessionControllerPhase::SessionExpiring;
    let rejected = controller
        .begin_attached_recovery_for_test(recovery)
        .unwrap_err();
    assert_eq!(format!("{rejected:?}"), "RejectedRecoveryStart");

    controller.phase = OwnerSessionControllerPhase::Active;
    let retried = rejected
        .retry_attached_recovery_start_for_test(&mut controller)
        .unwrap();
    assert!(retried.needs_execute());
    let _ = controller
        .execute_attached_recovery_for_test(&queue)
        .unwrap();
    let completed = controller
        .publish_attached_recovery_for_test(&mut queue)
        .unwrap();
    assert!(completed.handoff_ready());
    let (generation, nonce) = install_completed_handoff(&mut controller, &endpoint);
    assert_eq!(generation.get(), 3);
    assert_eq!(nonce, AttachNonce::new([0x33; 16]).unwrap());
    assert_exact_zero_controller_ownership(controller.snapshot());
}

#[tokio::test(start_paused = true)]
async fn wrong_controller_rejects_and_returns_the_exact_recovery_to_its_owner() {
    let (mut owner, endpoint, mut queue, recovery) = queued_recovery(0, 128, 2);
    let foreign_clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (_, foreign_executor) = executor_with_effect_steps(128);
    let mut foreign = LegacyOwnerSessionController::new_for_test(foreign_executor, foreign_clock)
        .expect("foreign controller");

    let rejected = foreign
        .begin_attached_recovery_for_test(recovery)
        .unwrap_err();
    assert_eq!(format!("{rejected:?}"), "WrongRecoveryOwner");
    assert_exact_zero_controller_ownership(foreign.snapshot());

    let retried = rejected
        .retry_attached_recovery_start_for_test(&mut owner)
        .unwrap();
    assert!(retried.needs_execute());
    let _ = owner.execute_attached_recovery_for_test(&queue).unwrap();
    let completed = owner
        .publish_attached_recovery_for_test(&mut queue)
        .unwrap();
    assert!(completed.handoff_ready());
    let (generation, nonce) = install_completed_handoff(&mut owner, &endpoint);
    assert_eq!(generation.get(), 3);
    assert_eq!(nonce, AttachNonce::new([0x33; 16]).unwrap());
    assert_exact_zero_controller_ownership(owner.snapshot());
}

#[tokio::test(start_paused = true)]
async fn initial_and_execute_wrong_queue_rejections_preserve_each_exact_capability() {
    let (mut controller, endpoint, mut queue, recovery) = queued_recovery(1, 1, 4);
    let wrong_endpoint = LegIo::for_authenticated_transport(
        crate::owned_upstream::two_leg::LegId::B,
        LegEndpointRole::Owner,
        binding(0x45),
        LegIoLimits::new(1_024, 8, 8_192, 8, 8_192).unwrap(),
    );
    let wrong_queue =
        LegOutboundQueue::for_leg(wrong_endpoint.established_leg(), 4, 8_192).unwrap();
    let _ = controller
        .begin_attached_recovery_for_test(recovery)
        .unwrap();

    let initial_rejected = controller
        .execute_attached_recovery_for_test(&wrong_queue)
        .unwrap_err();
    assert_eq!(
        format!("{initial_rejected:?}"),
        "RejectedRecoveryExecute(WrongAcceptanceQueue)"
    );
    assert!(controller.snapshot().recovery_needs_execute);
    let _ = controller
        .execute_attached_recovery_for_test(&queue)
        .unwrap();
    let first_publish = controller
        .publish_attached_recovery_for_test(&mut queue)
        .unwrap();
    assert!(first_publish.needs_execute());

    let execute_rejected = controller
        .execute_attached_recovery_for_test(&wrong_queue)
        .unwrap_err();
    assert_eq!(
        format!("{execute_rejected:?}"),
        "RejectedRecoveryExecute(WrongAcceptanceQueue)"
    );
    assert!(controller.snapshot().recovery_needs_execute);
    let _ = controller
        .execute_attached_recovery_for_test(&queue)
        .unwrap();
    let completed = controller
        .publish_attached_recovery_for_test(&mut queue)
        .unwrap();
    assert!(completed.handoff_ready());
    assert_eq!(
        install_completed_handoff(&mut controller, &endpoint)
            .0
            .get(),
        3
    );
    assert_exact_zero_controller_ownership(controller.snapshot());
}

#[tokio::test(start_paused = true)]
async fn ordinary_fatal_target_turn_never_inflates_recovery_ownership_counters() {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = executor_with_effect_steps(1);
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();

    let mut open = controller
        .apply_actor_event(arrived(
            peer_event(
                leg,
                Record::Open {
                    flow_id,
                    target: target(443),
                },
            ),
            &clock,
        ))
        .unwrap();
    while open.needs_continue() {
        open = controller.continue_effects_result_for_test().unwrap();
    }

    let mut data = controller
        .apply_actor_event(arrived(
            peer_event(
                leg,
                Record::Data {
                    flow_id,
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"x"),
                },
            ),
            &clock,
        ))
        .unwrap();
    while data.needs_continue() {
        data = controller.continue_effects_result_for_test().unwrap();
    }

    controller.executor.exhaust_effect_resume_ids_for_test();
    let error = controller
        .apply_actor_event(arrived(
            peer_event(
                leg,
                Record::Close {
                    flow_id,
                    direction: Direction::ClientToTarget,
                    final_offset: ByteOffset::new(1),
                },
            ),
            &clock,
        ))
        .unwrap_err();
    assert_eq!(
        format!("{error:?}"),
        "Target(\"owner Target effect resume ID space is exhausted\")"
    );
    let target = controller.executor.snapshot();
    assert_eq!(target.aborted_outputs, 1);
    assert_eq!(target.aborted_recovery_guards, 0);
    let snapshot = controller.snapshot();
    assert_eq!(snapshot.retained_outputs, 1);
    assert_eq!(snapshot.retained_observations, 0);
    assert_eq!(snapshot.retained_recovery_outputs, 0);
    assert_eq!(snapshot.retained_recovery_observations, 0);
    assert_eq!(snapshot.retained_recoveries, 0);
    assert_eq!(snapshot.retained_recovery_cursors, 0);
    assert_eq!(snapshot.retained_recovery_attached, 0);
}

#[tokio::test(start_paused = true)]
async fn progress_failure_precedes_publish_and_sticky_retains_the_exact_guard() {
    let (mut controller, _endpoint, mut queue, recovery) = queued_recovery(0, 128, 2);
    let _ = controller
        .begin_attached_recovery_for_test(recovery)
        .unwrap();
    controller.fail_next_recovery_progress_for_test();

    let error = controller
        .execute_attached_recovery_for_test(&queue)
        .unwrap_err();
    assert_eq!(
        format!("{error:?}"),
        "PostCommitProgress(MissingActorArrival)"
    );
    assert_eq!(
        queue.len(),
        1,
        "progress failure must happen before any recovery publication"
    );

    let snapshot = controller.snapshot();
    assert!(snapshot.failed);
    assert_eq!(snapshot.pending_recoveries, 0);
    assert_eq!(snapshot.retained_recoveries, 1);
    assert_eq!(snapshot.retained_recovery_cursors, 1);
    assert_eq!(snapshot.retained_recovery_outputs, 1);
    assert_eq!(snapshot.retained_recovery_observations, 0);
    assert_eq!(snapshot.retained_recovery_attached, 1);
    assert_eq!(
        format!(
            "{:?}",
            controller
                .publish_attached_recovery_for_test(&mut queue)
                .unwrap_err()
        ),
        "Failed"
    );
}

#[tokio::test(start_paused = true)]
async fn fatal_execute_sticky_retains_cursor_outputs_and_attached_guard() {
    let (mut controller, _endpoint, queue, recovery) = queued_recovery(1, 1, 2);
    let _ = controller
        .begin_attached_recovery_for_test(recovery)
        .unwrap();
    controller.executor.exhaust_effect_resume_ids_for_test();

    let error = controller
        .execute_attached_recovery_for_test(&queue)
        .unwrap_err();
    assert_eq!(
        format!("{error:?}"),
        "Target(\"owner Target effect resume ID space is exhausted\")"
    );
    assert_eq!(queue.len(), 1, "fatal execute cannot publish the page");

    let target = controller.executor.snapshot();
    assert_eq!(target.aborted_effects, 1);
    assert_eq!(target.aborted_outputs, 1);
    assert_eq!(target.aborted_observations, 0);
    assert_eq!(target.aborted_recovery_guards, 1);

    let snapshot = controller.snapshot();
    assert!(snapshot.failed);
    assert_eq!(snapshot.pending_recoveries, 0);
    assert_eq!(snapshot.retained_recoveries, 1);
    assert_eq!(snapshot.retained_recovery_cursors, 1);
    assert_eq!(snapshot.retained_recovery_outputs, 1);
    assert_eq!(snapshot.retained_recovery_observations, 0);
    assert_eq!(snapshot.retained_recovery_attached, 1);
    assert_eq!(
        format!(
            "{:?}",
            controller.continue_effects_result_for_test().unwrap_err()
        ),
        "Failed"
    );
}

#[tokio::test(start_paused = true)]
async fn snapshot_exactly_sums_controller_and_target_recovery_guards() {
    let (mut controller, _endpoint, queue, recovery) = queued_recovery(1, 1, 2);
    let _ = controller
        .begin_attached_recovery_for_test(recovery)
        .unwrap();
    controller.executor.exhaust_effect_resume_ids_for_test();
    controller
        .execute_attached_recovery_for_test(&queue)
        .expect_err("Target resume exhaustion retains its attached guard");
    assert_eq!(controller.executor.snapshot().aborted_recovery_guards, 1);

    let (_foreign, _foreign_endpoint, _foreign_queue, foreign_recovery) =
        queued_recovery(0, 128, 2);
    controller.aborted_recovery = Some(AbortedOwnerRecoveryTurn {
        pending: Some(PendingOwnerAttachedRecovery::Initial(foreign_recovery)),
        outputs: Vec::new(),
        observations: None,
    });

    assert_eq!(
        controller.snapshot().retained_recoveries,
        2,
        "the sticky inventory must count both exact non-cloneable guards"
    );
}

#[tokio::test(start_paused = true)]
async fn interior_recovery_capability_fails_closed_with_exact_retained_outputs() {
    let (mut controller, _endpoint, mut queue, recovery) = queued_recovery(1, 1, 4);
    let mut outputs = publish_raw_first_page(&mut controller, &mut queue, recovery);
    assert!(matches!(
        outputs.last(),
        Some(OwnerTargetRecoveryOutput::NeedsResume { .. })
    ));
    outputs.push(OwnerTargetRecoveryOutput::SessionExpired);

    let error = controller.finish_recovery_publication(outputs).unwrap_err();
    assert_eq!(format!("{error:?}"), "InvalidRecoveryPage");
    let snapshot = controller.snapshot();
    assert!(snapshot.failed);
    assert_eq!(snapshot.retained_recoveries, 1);
    assert_eq!(snapshot.retained_recovery_outputs, 3);
    assert_eq!(snapshot.retained_recovery_attached, 1);
}

#[tokio::test(start_paused = true)]
async fn multiple_recovery_capabilities_fail_closed_without_dropping_either_authority() {
    let (mut first, _first_endpoint, mut first_queue, first_recovery) = queued_recovery(1, 1, 4);
    let (mut second, _second_endpoint, mut second_queue, second_recovery) =
        queued_recovery(0, 128, 4);
    let mut outputs = publish_raw_first_page(&mut first, &mut first_queue, first_recovery);
    let mut completed = publish_raw_first_page(&mut second, &mut second_queue, second_recovery);
    assert!(matches!(
        outputs.last(),
        Some(OwnerTargetRecoveryOutput::NeedsResume { .. })
    ));
    assert!(matches!(
        completed.last(),
        Some(OwnerTargetRecoveryOutput::RecoveryCompleted { .. })
    ));
    outputs.append(&mut completed);

    let error = first.finish_recovery_publication(outputs).unwrap_err();
    assert_eq!(format!("{error:?}"), "InvalidRecoveryPage");
    let snapshot = first.snapshot();
    assert!(snapshot.failed);
    assert_eq!(snapshot.retained_recoveries, 1);
    assert_eq!(snapshot.retained_recovery_outputs, 4);
    assert_eq!(snapshot.retained_recovery_attached, 2);
}

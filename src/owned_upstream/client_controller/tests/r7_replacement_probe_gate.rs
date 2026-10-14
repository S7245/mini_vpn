use super::*;

struct QueuedProbeFixture {
    controller: ClientSessionController,
    scheduler: ClientCurrentProbeQueued,
    receipt_io: LegIo,
    receipt_queue: LegOutboundQueue,
}

fn queued_probe_fixture(identity_byte: u8) -> QueuedProbeFixture {
    let mut controller = initial_controller();
    let (seal, session_id, generation, epoch_nonce) = {
        let authority = &controller
            .current
            .as_ref()
            .expect("initial current transport exists")
            .authority;
        (
            authority.share_probe_seal(),
            authority.probe_session_id(),
            authority.generation(),
            authority.probe_epoch_nonce(),
        )
    };
    let probe_sequence = ProbeSequence::new(1).expect("nonzero probe sequence");
    let features = FeatureSet::new(FEATURES);
    let receipt_io = LegIo::for_authenticated_transport(
        LegId::B,
        LegEndpointRole::Client,
        binding(identity_byte),
        limits(),
    );
    let receipt_seal = receipt_io.established_leg().standby_seal();
    let mut receipt_queue = LegOutboundQueue::for_leg(receipt_io.established_leg(), 1, 1_024)
        .expect("test receipt queue is bounded");
    let receipt = receipt_queue
        .push_leg_control(
            &receipt_seal,
            LegControlFrame::try_new(
                generation,
                LegControlRecord::LegProbe {
                    session_id,
                    leg_epoch_nonce: epoch_nonce,
                    probe_sequence,
                },
                features,
            )
            .expect("test probe is protocol-valid"),
            features,
        )
        .expect("test receipt queue admits one probe");
    let identity = Arc::new(ClientCurrentProbeIdentity {
        correlation: ClientCurrentProbeCorrelation {
            seal,
            session_id,
            generation,
            epoch_nonce,
            sequence: probe_sequence,
            switch_epoch: controller.current_liveness.switch_epoch,
        },
    });
    let scheduler = ClientCurrentProbeQueued {
        identity: Arc::clone(&identity),
    };
    controller.current_liveness.phase = ClientCurrentProbePhase::Queued { identity, receipt };
    QueuedProbeFixture {
        controller,
        scheduler,
        receipt_io,
        receipt_queue,
    }
}

fn assert_normal_replacement_rejected_without_mutation(
    controller: &mut ClientSessionController,
    expected: ClientRecoveryBeginErrorKind,
) {
    assert_eq!(
        controller.normal_replacement_blocker(),
        Some(expected),
        "an unsettled exact observation owner must close normal replacement admission with an actionable typed blocker",
    );
    let generation_before = controller.generation();
    let snapshot_before = controller.snapshot();
    let inbound_before = controller.current_inbound_route();
    let outbound_before = controller.current_outbound_route();
    let queue_before = (
        controller.current_queued_frames(),
        controller.current_queued_bytes(),
    );
    let supersession_before = controller.supersession_history;

    let AcceptedRegisteredClientTransportFixture {
        accepted,
        io,
        queue,
        reporter: _reporter,
    } = accepted_registered_client_transport_fixture(1);
    let candidate_route = io.outbound_route();
    let candidate_queue = (queue.len(), queue.owned_bytes());
    let failure = controller
        .begin_registered_replacement(accepted, io, queue)
        .expect_err("normal replacement must not orphan an outstanding probe scheduler owner");
    assert_eq!(failure.kind(), expected);
    assert_eq!(
        failure.failure_class(),
        ClientRegisteredRecoveryFailureClass::Retryable,
    );
    let (returned_accepted, returned_io, returned_queue) = failure.into_parts();
    assert_eq!(returned_io.outbound_route(), candidate_route);
    assert_eq!(
        (returned_queue.len(), returned_queue.owned_bytes()),
        candidate_queue,
        "preflight rejection returns B's exact bounded FIFO unchanged",
    );
    drop((returned_accepted, returned_io, returned_queue));

    assert_eq!(controller.generation(), generation_before);
    assert_eq!(controller.snapshot(), snapshot_before);
    assert_eq!(controller.current_inbound_route(), inbound_before);
    assert_eq!(controller.current_outbound_route(), outbound_before);
    assert_eq!(
        (
            controller.current_queued_frames(),
            controller.current_queued_bytes(),
        ),
        queue_before,
    );
    assert_eq!(controller.supersession_history, supersession_before);
    assert!(controller.retired.is_none());
}

#[test]
fn queued_probe_scheduler_blocks_normal_replacement_without_losing_exact_owner() {
    let QueuedProbeFixture {
        mut controller,
        scheduler,
        receipt_io,
        receipt_queue,
    } = queued_probe_fixture(0xf1);
    let identity = Arc::clone(&scheduler.identity);

    assert_normal_replacement_rejected_without_mutation(
        &mut controller,
        ClientRecoveryBeginErrorKind::CurrentProbeSettlementPending,
    );

    assert!(matches!(
        &controller.current_liveness.phase,
        ClientCurrentProbePhase::Queued { identity: live, .. }
            if Arc::ptr_eq(live, &identity)
    ));
    let (returned, error) = controller
        .arm_current_probe(scheduler, SimTime::ZERO, Duration::from_millis(1))
        .expect_err("the undelivered but exact queued owner remains usable after rejection")
        .into_parts();
    assert_eq!(error, ClientCurrentProbeArmError::NotDelivered);
    assert!(Arc::ptr_eq(&returned.identity, &identity));
    drop((returned, receipt_queue, receipt_io));
}

#[test]
fn armed_probe_deadline_blocks_normal_replacement_without_losing_exact_owner() {
    let QueuedProbeFixture {
        mut controller,
        scheduler,
        receipt_io,
        receipt_queue,
    } = queued_probe_fixture(0xf2);
    let identity = scheduler.identity;
    let deadline_at = SimTime::from_nanos(500);
    let deadline = ClientCurrentProbeDeadline {
        identity: Arc::clone(&identity),
        deadline: deadline_at,
    };
    controller.current_liveness.phase = ClientCurrentProbePhase::Armed {
        identity,
        deadline: deadline_at,
    };

    assert_normal_replacement_rejected_without_mutation(
        &mut controller,
        ClientRecoveryBeginErrorKind::CurrentProbeSettlementPending,
    );

    assert_eq!(
        controller.current_probe_deadline_status(&deadline, SimTime::from_nanos(499)),
        ClientCurrentProbeDeadlineStatus::NotDue,
        "the pointer-identical deadline remains authoritative after rejection",
    );
    drop((deadline, receipt_queue, receipt_io));
}

#[test]
fn ack_won_queued_probe_blocks_normal_replacement_until_exact_reconciliation() {
    let QueuedProbeFixture {
        mut controller,
        scheduler,
        receipt_io,
        receipt_queue,
    } = queued_probe_fixture(0xf3);
    let identity = Arc::clone(&scheduler.identity);
    let generation = identity.correlation.generation;
    let probe_sequence = identity.correlation.sequence;
    controller.current_liveness.phase = ClientCurrentProbePhase::AckWonQueued { identity };

    assert_normal_replacement_rejected_without_mutation(
        &mut controller,
        ClientRecoveryBeginErrorKind::CurrentProbeSettlementPending,
    );

    let settlement = controller
        .reconcile_current_probe_ack(
            ClientCurrentProbeSchedulerAuthority::Queued(scheduler),
            generation,
            probe_sequence,
        )
        .expect("the exact queued scheduler remains reconcilable after rejection");
    assert_eq!(
        settlement.phase(),
        ClientCurrentProbeAckSettlementPhase::Queued,
    );
    assert!(matches!(
        controller.current_liveness.phase,
        ClientCurrentProbePhase::Idle
    ));
    drop((receipt_queue, receipt_io));
}

#[test]
fn ack_won_armed_probe_blocks_normal_replacement_until_exact_reconciliation() {
    let QueuedProbeFixture {
        mut controller,
        scheduler,
        receipt_io,
        receipt_queue,
    } = queued_probe_fixture(0xf4);
    let identity = scheduler.identity;
    let generation = identity.correlation.generation;
    let probe_sequence = identity.correlation.sequence;
    let deadline_at = SimTime::from_nanos(500);
    let deadline = ClientCurrentProbeDeadline {
        identity: Arc::clone(&identity),
        deadline: deadline_at,
    };
    controller.current_liveness.phase = ClientCurrentProbePhase::AckWonArmed {
        identity,
        deadline: deadline_at,
    };

    assert_normal_replacement_rejected_without_mutation(
        &mut controller,
        ClientRecoveryBeginErrorKind::CurrentProbeSettlementPending,
    );

    let settlement = controller
        .reconcile_current_probe_ack(
            ClientCurrentProbeSchedulerAuthority::Armed(deadline),
            generation,
            probe_sequence,
        )
        .expect("the exact armed scheduler remains reconcilable after rejection");
    assert_eq!(
        settlement.phase(),
        ClientCurrentProbeAckSettlementPhase::Armed,
    );
    assert!(matches!(
        controller.current_liveness.phase,
        ClientCurrentProbePhase::Idle
    ));
    drop((receipt_queue, receipt_io));
}

#[test]
fn pending_observation_settlement_blocks_normal_replacement_and_retains_scheduler() {
    let QueuedProbeFixture {
        mut controller,
        scheduler,
        receipt_io,
        receipt_queue,
    } = queued_probe_fixture(0xf5);
    let prepared = {
        let authority = &controller
            .current
            .as_ref()
            .expect("initial current transport exists")
            .authority;
        controller
            .prepare_current_observation_settlement(
                authority,
                ClientControllerObservationSettlementSource::JoinedShutdown,
            )
            .expect("the exact queued observation is structurally valid")
    };
    controller.begin_current_observation_settlement(prepared);
    assert!(controller.observation_settlement_pending());
    assert!(controller.current_or_pending_probe_queued_matches(&scheduler));
    assert!(matches!(
        controller.current_liveness.phase,
        ClientCurrentProbePhase::Idle
    ));

    assert_normal_replacement_rejected_without_mutation(
        &mut controller,
        ClientRecoveryBeginErrorKind::ObservationSettlementPending,
    );

    assert!(controller.observation_settlement_pending());
    assert!(controller.current_or_pending_probe_queued_matches(&scheduler));
    let settlement = match controller.settle_pending_current_observation(Some(
        ClientCurrentProbeSchedulerAuthority::Queued(scheduler),
    )) {
        Ok(settlement) => settlement,
        Err(_) => panic!("the exact scheduler was not usable after replacement rejection"),
    };
    assert_eq!(
        settlement.source(),
        ClientControllerObservationSettlementSource::JoinedShutdown,
    );
    assert!(!controller.observation_settlement_pending());
    drop((receipt_queue, receipt_io));
}

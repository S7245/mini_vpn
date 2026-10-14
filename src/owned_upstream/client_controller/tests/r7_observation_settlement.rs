use super::*;

fn armed_ack_won_fixture(
    identity_byte: u8,
) -> (
    ClientSessionController,
    ClientCurrentProbeDeadline,
    ClientCurrentProbeAcked,
) {
    let mut controller = initial_controller();
    let (session_id, generation, epoch_nonce) = {
        let authority = &controller
            .current
            .as_ref()
            .expect("initial current transport exists")
            .authority;
        (
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
    let identity = Arc::new(ClientCurrentProbeIdentity {
        correlation: ClientCurrentProbeCorrelation {
            seal: receipt_io.established_leg().standby_seal(),
            session_id,
            generation,
            epoch_nonce,
            sequence: probe_sequence,
            switch_epoch: controller.current_liveness.switch_epoch,
        },
    });
    let deadline = SimTime::from_nanos(500);
    let token = ClientCurrentProbeDeadline {
        identity: Arc::clone(&identity),
        deadline,
    };
    controller.current_liveness.phase = ClientCurrentProbePhase::Armed { identity, deadline };
    let ack = LegControlFrame::try_new(
        generation,
        LegControlRecord::LegProbeAck {
            session_id,
            leg_epoch_nonce: epoch_nonce,
            probe_sequence,
        },
        features,
    )
    .expect("matching ACK is valid");
    let acked = controller
        .accept_current_probe_ack(
            receipt_io
                .established_leg()
                .bind_received_control_frame(ack),
            deadline,
        )
        .expect("equal-arrival matching ACK wins the absolute deadline");
    (controller, token, acked)
}

#[test]
fn observation_history_overflow_rejects_terminal_binding_with_exact_owners_restored() {
    let (mut controller, reporter) = initial_controller_with_reporter();
    let generation = controller.generation();
    controller.observation_settlement_history.observations = usize::MAX;

    let terminal = reporter
        .report(LegTransportTerminalReason::PeerClosed)
        .expect("the physical reporter mints one exact terminal");
    let failure = controller
        .bind_current_terminal(terminal)
        .expect_err("checked observation history cannot wrap");

    assert_eq!(
        failure.kind(),
        ClientCurrentTerminalBindErrorKind::ObservationHistoryOverflow
    );
    assert_eq!(controller.generation(), generation);
    assert!(controller.current.is_some());
    assert!(matches!(
        controller.phase,
        ClientControllerPhase::Active {
            recovery_tail: None
        }
    ));
    assert_eq!(
        controller.observation_settlement_history.observations,
        usize::MAX
    );
    let ledger = controller.ownership_ledger(0, 0);
    assert!(!ledger.overflowed());
    assert_eq!(ledger.live_current_transport_owners(), 1);
    assert_eq!(ledger.live_active_leg_authorities(), 1);
    assert_eq!(ledger.settled_observations(), usize::MAX);

    controller.observation_settlement_history =
        ClientControllerObservationSettlementHistory::default();
    let returned_terminal = failure.into_terminal();
    let bound = controller
        .bind_current_terminal(returned_terminal)
        .expect("the exact returned terminal remains usable after capacity is restored");
    assert_eq!(bound.generation(), generation);
    assert_eq!(bound.reason(), LegTransportTerminalReason::PeerClosed);
}

#[test]
fn armed_probe_ack_rejects_wrong_scheduler_and_consumes_the_exact_deadline_once() {
    let (mut controller, exact, acked) = armed_ack_won_fixture(0xe1);
    let wrong_identity = Arc::new(ClientCurrentProbeIdentity {
        correlation: ClientCurrentProbeCorrelation {
            seal: exact.identity.correlation.seal.share(),
            session_id: exact.identity.correlation.session_id,
            generation: exact.identity.correlation.generation,
            epoch_nonce: exact.identity.correlation.epoch_nonce,
            sequence: exact.identity.correlation.sequence,
            switch_epoch: exact.identity.correlation.switch_epoch,
        },
    });
    let wrong = ClientCurrentProbeDeadline {
        identity: Arc::clone(&wrong_identity),
        deadline: exact.deadline,
    };

    let failure = controller
        .reconcile_current_probe_ack(
            ClientCurrentProbeSchedulerAuthority::Armed(wrong),
            acked.generation,
            acked.probe_sequence,
        )
        .expect_err("equal values cannot replace pointer-identical scheduler ownership");
    assert_eq!(
        failure.kind(),
        ClientCurrentProbeAckReconcileErrorKind::WrongSchedulerAuthority
    );
    let ClientCurrentProbeSchedulerAuthority::Armed(returned_wrong) = failure.into_scheduler()
    else {
        panic!("wrong armed authority changed variant");
    };
    assert!(Arc::ptr_eq(&returned_wrong.identity, &wrong_identity));
    assert!(matches!(
        controller.current_liveness.phase,
        ClientCurrentProbePhase::AckWonArmed { .. }
    ));

    let settlement = controller
        .reconcile_current_probe_ack(
            ClientCurrentProbeSchedulerAuthority::Armed(exact),
            acked.generation,
            acked.probe_sequence,
        )
        .expect("pointer-identical armed deadline settles exactly once");
    assert_eq!(
        settlement.phase(),
        ClientCurrentProbeAckSettlementPhase::Armed
    );
    assert!(matches!(
        controller.current_liveness.phase,
        ClientCurrentProbePhase::Idle
    ));
    let ledger = controller.ownership_ledger(0, 0);
    assert_eq!(ledger.live_probe_authorities(), 0);
    assert_eq!(ledger.settled_probe_acks(), 1);
    assert_eq!(ledger.settled_queued_probe_acks(), 0);
    assert_eq!(ledger.settled_armed_probe_acks(), 1);
    drop(returned_wrong);
}

#[test]
fn probe_ack_history_overflow_returns_deadline_and_keeps_ack_won_live() {
    let (mut controller, exact, acked) = armed_ack_won_fixture(0xe2);
    controller.probe_ack_settlement_history.settlements = usize::MAX;

    let failure = controller
        .reconcile_current_probe_ack(
            ClientCurrentProbeSchedulerAuthority::Armed(exact),
            acked.generation,
            acked.probe_sequence,
        )
        .expect_err("checked ACK history cannot wrap");
    assert_eq!(
        failure.kind(),
        ClientCurrentProbeAckReconcileErrorKind::HistoryOverflow
    );
    let returned = failure.into_scheduler();
    assert!(matches!(
        controller.current_liveness.phase,
        ClientCurrentProbePhase::AckWonArmed { .. }
    ));
    let live = controller.ownership_ledger(1, 1);
    assert_eq!(live.live_probe_authorities(), 1);
    assert_eq!(live.live_probe_scheduler_authorities(), 1);
    assert_eq!(live.live_scheduler_probe_deadlines(), 1);
    assert_eq!(live.settled_probe_acks(), usize::MAX);

    controller.probe_ack_settlement_history = ClientCurrentProbeAckSettlementHistory::default();
    let settlement = controller
        .reconcile_current_probe_ack(returned, acked.generation, acked.probe_sequence)
        .expect("returned exact deadline remains usable after capacity is restored");
    assert_eq!(
        settlement.phase(),
        ClientCurrentProbeAckSettlementPhase::Armed
    );
    assert_eq!(controller.ownership_ledger(0, 0).settled_probe_acks(), 1);
}

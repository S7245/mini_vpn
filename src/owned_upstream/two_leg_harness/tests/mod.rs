use bytes::Bytes;

use super::*;
use crate::owned_upstream::leg_io::{LegEndpointRole, LegIo, LegIoLimits};
use crate::owned_upstream::two_leg::{
    EventBudget, LegId, SimTime, WireDirection, WireLane, WireRoute,
};
use crate::resumable::{
    AttachAlpn, AttachTransportBinding, ByteOffset, DevicePrincipal, Direction, Frame,
    LegGeneration, OwnerIdentity, Record, SessionFlowId, TlsExporterBinding,
};

mod r7_capacity;

fn test_transport_binding() -> AttachTransportBinding {
    AttachTransportBinding::new(
        OwnerIdentity::new([0x31; 32]).unwrap(),
        AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
        TlsExporterBinding::new([0x51; 32]).unwrap(),
        DevicePrincipal::new([0x53; 16]).unwrap(),
    )
}

struct RejectOnceTransport {
    reject_next: bool,
    accepted: Vec<Bytes>,
}

impl EncodedLegTransport for RejectOnceTransport {
    fn send_encoded(
        &mut self,
        _now: SimTime,
        _route: WireRoute,
        _lane: WireLane,
        bytes: Bytes,
    ) -> Result<(), EncodedLegTransportError> {
        if self.reject_next {
            self.reject_next = false;
            return Err(EncodedLegTransportError::Wire(
                WireError::LiveMessageCapacityExceeded,
            ));
        }
        self.accepted.push(bytes);
        Ok(())
    }
}

#[test]
fn global_scheduler_releases_exactly_one_wire_message_per_turn() {
    let mut harness = ByteHarnessScheduler::new(EventBudget::new(8, 1_024).unwrap());
    let limits = LegIoLimits::new(1_024, 4, 4_096, 4, 4_096).unwrap();
    let binding = test_transport_binding();
    let mut client =
        LegIo::for_authenticated_transport(LegId::A, LegEndpointRole::Client, binding, limits);
    let work_budget = R5ScenarioManifest::baseline().work_budget().unwrap();
    let mut observed_work = HarnessObservedWork::default();
    {
        let mut sender = harness.controller_sender(work_budget, &mut observed_work);

        for offset in [0, 1] {
            client
                .send_frame(
                    &mut sender,
                    SimTime::ZERO,
                    Frame::try_new(
                        LegGeneration::new(1).unwrap(),
                        Record::Data {
                            flow_id: SessionFlowId::new(1).unwrap(),
                            direction: Direction::ClientToTarget,
                            offset: ByteOffset::new(offset),
                            payload: Bytes::from_static(b"x"),
                        },
                    )
                    .unwrap(),
                )
                .unwrap();
        }
    }

    assert_eq!(harness.pending_actions(), 2);
    let HarnessAction::WireDelivery {
        delivery: first,
        completion: first_completion,
    } = harness.pop_next_checked().unwrap().unwrap().into_payload()
    else {
        panic!("wire-only fixture scheduled a non-wire action")
    };
    assert!(first_completion.is_none());
    assert_eq!(
        first.route(),
        WireRoute::new(LegId::A, WireDirection::ClientToOwner)
    );
    assert_eq!(first.lane(), WireLane::Data);
    assert_eq!(harness.pending_actions(), 1);
    let HarnessAction::WireDelivery {
        delivery: second,
        completion: second_completion,
    } = harness.pop_next_checked().unwrap().unwrap().into_payload()
    else {
        panic!("wire-only fixture scheduled a non-wire action")
    };
    assert!(second_completion.is_none());
    assert_eq!(second.route(), first.route());
    assert_eq!(second.lane(), WireLane::Data);
    assert_eq!(harness.pending_actions(), 0);
}

#[test]
fn failed_wire_schedule_does_not_commit_any_observed_work() {
    let mut harness = ByteHarnessScheduler::new(EventBudget::new(1, 1_024).unwrap());
    harness
        .schedule(
            SimTime::ZERO,
            EventPhase::SupervisorCommand,
            CLIENT_SUPERVISOR,
            0,
            0xdead,
            HarnessAction::StartOpen,
        )
        .unwrap();
    let work_budget = R5ScenarioManifest::baseline().work_budget().unwrap();
    let mut observed_work = HarnessObservedWork::default();
    let frame = Frame::try_new(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"transactional"),
        },
    )
    .unwrap();
    let encoded = frame.encode().unwrap();

    let error = harness
        .controller_sender(work_budget, &mut observed_work)
        .send_encoded(
            SimTime::ZERO,
            WireRoute::new(LegId::A, WireDirection::ClientToOwner),
            WireLane::Data,
            encoded,
        )
        .unwrap_err();

    assert_eq!(
        error,
        EncodedLegTransportError::Wire(WireError::Schedule(ScheduleError::EventBudgetExceeded,))
    );
    assert_eq!(observed_work, HarnessObservedWork::default());
    assert_eq!(harness.next_send_ordinal, 0);
    assert_eq!(harness.pending_actions(), 1);
}

#[test]
fn failed_multi_category_wire_charge_does_not_commit_earlier_categories() {
    let work_budget = R5ScenarioManifest::baseline().work_budget().unwrap();
    let mut harness = ByteHarnessScheduler::new(work_budget.event_budget());
    let mut observed_work = HarnessObservedWork::default();
    observed_work
        .charge(
            work_budget,
            HarnessWorkCategory::WireDelivery,
            work_budget.categories().wire_delivery_turns(),
        )
        .unwrap();
    let before = observed_work;
    let frame = Frame::try_new(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"transactional"),
        },
    )
    .unwrap();

    harness
        .controller_sender(work_budget, &mut observed_work)
        .send_encoded(
            SimTime::ZERO,
            WireRoute::new(LegId::A, WireDirection::ClientToOwner),
            WireLane::Data,
            frame.encode().unwrap(),
        )
        .unwrap_err();

    assert_eq!(observed_work, before);
    assert_eq!(
        harness.take_work_error(),
        Some(CapacityError::HarnessWorkCategoryExceeded {
            category: HarnessWorkCategory::WireDelivery,
        })
    );
    assert_eq!(harness.next_send_ordinal, 0);
    assert_eq!(harness.pending_actions(), 0);
}

#[test]
fn rejected_transport_send_retains_exact_bounded_frame_for_retry() {
    let limits = LegIoLimits::new(1_024, 4, 4_096, 4, 4_096).unwrap();
    let mut endpoint = LegIo::for_authenticated_transport(
        LegId::A,
        LegEndpointRole::Client,
        test_transport_binding(),
        limits,
    );
    let frame = Frame::try_new(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"retained"),
        },
    )
    .unwrap();
    let encoded_len = frame.encode().unwrap().len();
    let mut queue = LegOutboundQueue::new(1, encoded_len).unwrap();
    queue.push(frame).unwrap();
    let mut transport = RejectOnceTransport {
        reject_next: true,
        accepted: Vec::new(),
    };

    assert_eq!(
        queue
            .try_flush(&mut endpoint, &mut transport, SimTime::ZERO)
            .unwrap_err(),
        LegIoError::Transport(EncodedLegTransportError::Wire(
            WireError::LiveMessageCapacityExceeded,
        ))
    );
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.owned_bytes(), encoded_len);
    assert_eq!(endpoint.counters().outbound_messages, 0);
    assert!(transport.accepted.is_empty());

    assert!(
        !queue
            .try_flush(&mut endpoint, &mut transport, SimTime::ZERO)
            .unwrap()
            .unwrap()
            .is_ack
    );
    assert_eq!(queue.len(), 0);
    assert_eq!(queue.owned_bytes(), 0);
    assert_eq!(endpoint.counters().outbound_messages, 1);
    assert_eq!(transport.accepted.len(), 1);
    let decoded = Frame::decode_owned_exact(Bytes::from(transport.accepted.remove(0))).unwrap();
    assert!(matches!(
        decoded.record(),
        Record::Data { payload, .. } if payload.as_ref() == b"retained"
    ));
}

#[test]
fn full_outbound_queue_returns_exact_unconsumed_frame_to_producer() {
    let first = Frame::try_new(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"first"),
        },
    )
    .unwrap();
    let second = Frame::try_new(
        LegGeneration::new(1).unwrap(),
        Record::Data {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(5),
            payload: Bytes::from_static(b"secret-returned-frame"),
        },
    )
    .unwrap();
    let second_payload_ptr = match second.record() {
        Record::Data { payload, .. } => payload.as_ptr(),
        _ => unreachable!(),
    };
    let max_bytes = first.encode().unwrap().len() + second.encode().unwrap().len();
    let mut queue = LegOutboundQueue::new(1, max_bytes).unwrap();
    queue.push(first).unwrap();
    let before_len = queue.len();
    let before_bytes = queue.owned_bytes();

    let error = queue.push(second).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::owned_upstream::leg_io::LegOutboundQueueErrorKind::CapacityExceeded {
            queued_frames: 1,
            ..
        }
    ));
    assert!(!format!("{error:?}").contains("secret-returned-frame"));
    let returned = error.into_frame();
    assert_eq!(queue.len(), before_len);
    assert_eq!(queue.owned_bytes(), before_bytes);
    assert!(matches!(
        returned.record(),
        Record::Data {
            flow_id,
            direction: Direction::ClientToTarget,
            offset,
            payload,
        } if *flow_id == SessionFlowId::new(1).unwrap()
            && *offset == ByteOffset::new(5)
            && payload.as_ref() == b"secret-returned-frame"
            && payload.as_ptr() == second_payload_ptr
    ));
}

#[test]
fn attach_action_is_charged_to_global_scheduler_byte_budget() {
    let binding = baseline_binding().unwrap();
    let request = AttachRequest::new(
        SessionId::new([0x16; 16]).unwrap(),
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([0x62; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b0111, 0b0001).unwrap(),
    );
    let proof = baseline_credentials()
        .unwrap()
        .prove(&request, &binding)
        .unwrap();
    let pending = crate::owned_upstream::leg::EstablishedLeg::for_authenticated_transport(binding)
        .begin_attach(request)
        .bind_initial_frame(request.to_attach_frame(proof))
        .unwrap();
    let owned_bytes = pending.encoded_len().unwrap();
    let mut scheduler =
        ByteHarnessScheduler::new(EventBudget::new(1, owned_bytes.saturating_sub(1)).unwrap());

    assert_eq!(
        scheduler
            .schedule(
                SimTime::ZERO,
                EventPhase::SupervisorCommand,
                CLIENT_SUPERVISOR,
                owned_bytes,
                0xdead,
                HarnessAction::StartAttach(pending),
            )
            .unwrap_err(),
        ScheduleError::ByteBudgetExceeded
    );
    assert_eq!(scheduler.pending_actions(), 0);
    assert_eq!(scheduler.pending_bytes(), 0);
}

#[test]
fn production_baseline_uses_one_derived_budget_and_validates_every_work_category() {
    let budget = R5ScenarioManifest::baseline().work_budget().unwrap();
    let report = run_r5_baseline().unwrap();
    let limits = budget.categories();
    let observed = report.observed_work.categories();

    assert_eq!(report.work_budget, budget);
    assert_eq!(report.scheduler_event_budget, budget.event_budget());
    assert_eq!(report.step_limit, budget.max_turns());
    assert_eq!(budget.validate_observed(report.observed_work), Ok(()));
    assert_eq!(budget.max_wire_send_owned_bytes(), 776);
    assert_eq!(budget.max_wire_delivery_owned_bytes(), 776);
    assert_eq!(budget.max_non_wire_owned_bytes(), 6_656);
    assert_eq!(budget.max_owned_bytes(), 8_208);
    assert_eq!(budget.max_turns(), 118);
    assert_eq!(limits.wire_send_turns(), 16);
    assert_eq!(limits.wire_delivery_turns(), 16);
    assert_eq!(limits.positive_accept_turns(), 4);
    assert_eq!(limits.zero_would_block_turns(), 2);
    assert_eq!(limits.ack_construct_turns(), 8);
    assert_eq!(limits.ack_decode_turns(), 8);
    assert_eq!(limits.fixed_actor_turns(), 64);
    assert_eq!(report.observed_work.wire_send_owned_bytes(), 728);
    assert_eq!(report.observed_work.wire_delivery_owned_bytes(), 728);
    assert_eq!(report.observed_work.non_wire_owned_bytes(), 981);
    assert_eq!(report.observed_work.total_owned_bytes(), Ok(2_437));
    assert_eq!(observed.wire_send_turns(), 16);
    assert_eq!(observed.wire_delivery_turns(), 16);
    assert_eq!(observed.positive_accept_turns(), 4);
    assert_eq!(observed.zero_would_block_turns(), 2);
    assert_eq!(observed.ack_construct_turns(), 8);
    assert_eq!(observed.ack_decode_turns(), 8);
    assert_eq!(observed.fixed_actor_turns(), 55);
    assert!(report.steps <= report.step_limit);
}

#[test]
fn production_byte_baseline_crosses_real_attach_wire_target_and_clean_fin() {
    let report = run_r5_baseline().unwrap();

    assert_eq!(report.target_open_attempts, 1);
    assert_eq!(report.target_opened_flows, 1);
    assert_eq!(report.client_to_target, b"client-production-bytes");
    assert_eq!(report.target_to_client, b"owner-production-bytes");
    assert_eq!(report.target_zero_accepts, 1);
    assert_eq!(report.client_zero_accepts, 1);
    assert!(report.target_positive_accepts >= 2);
    assert!(report.client_positive_accepts >= 2);
    assert_eq!(report.target_accepted_bytes, CLIENT_BYTES.len() as u64);
    assert_eq!(report.client_accepted_bytes, OWNER_BYTES.len());
    assert_eq!(report.client_replay_bytes, 0);
    assert_eq!(report.owner_replay_bytes, 0);
    assert_eq!(report.client_port_owned_bytes, 0);
    assert_eq!(report.owner_port_owned_bytes, 0);
    assert_eq!(report.owner_port_owned_segments, 0);
    assert_eq!(report.target_live_flows, 0);
    assert_eq!(report.target_tombstones, 0);
    assert_eq!(report.client_live_flows, 0);
    assert_eq!(report.owner_live_flows, 0);
    assert_eq!(report.client_terminal_tombstones, 0);
    assert_eq!(report.owner_terminal_tombstones, 0);
    assert_eq!(report.owner_target_flows, 0);
    assert_eq!(report.owner_pending_writes, 0);
    assert_eq!(report.owner_pending_joins, 0);
    assert!(report.client_port_quiescent);
    assert!(report.client_ack_frames > 0);
    assert!(report.owner_ack_frames > 0);
    assert_eq!(report.outbound_queued_frames, 0);
    assert_eq!(report.outbound_queued_bytes, 0);
    assert_eq!(report.outbound_retries, 0);
    assert_eq!(report.scheduler_pending_actions, 0);
    assert_eq!(report.scheduler_pending_bytes, 0);
    assert_eq!(report.zero_ledger, R5ZeroLedger::default());
    assert!(report.encoded_messages >= 10);
    assert!(report.bound_messages >= 10);
    assert!(report.steps < 256);
    assert_ne!(report.trace_hash, 0);
}

#[test]
fn production_baseline_retries_retained_attach_after_transport_pressure() {
    let baseline = run_r5_baseline().unwrap();
    let report = R5BaselineHarness::new_with_rejection(Some(0))
        .unwrap()
        .run()
        .unwrap();
    let baseline_turns = baseline.observed_work.categories();
    let retry_turns = report.observed_work.categories();

    assert_eq!(report.outbound_retries, 1);
    assert_eq!(
        retry_turns.fixed_actor_turns(),
        baseline_turns.fixed_actor_turns() + 1
    );
    assert_eq!(
        retry_turns.wire_send_turns(),
        baseline_turns.wire_send_turns()
    );
    assert_eq!(
        retry_turns.wire_delivery_turns(),
        baseline_turns.wire_delivery_turns()
    );
    assert_eq!(
        retry_turns.positive_accept_turns(),
        baseline_turns.positive_accept_turns()
    );
    assert_eq!(
        retry_turns.zero_would_block_turns(),
        baseline_turns.zero_would_block_turns()
    );
    assert_eq!(
        retry_turns.ack_construct_turns(),
        baseline_turns.ack_construct_turns()
    );
    assert_eq!(
        retry_turns.ack_decode_turns(),
        baseline_turns.ack_decode_turns()
    );
    assert_eq!(
        report.observed_work.wire_send_owned_bytes(),
        baseline.observed_work.wire_send_owned_bytes()
    );
    assert_eq!(
        report.observed_work.wire_delivery_owned_bytes(),
        baseline.observed_work.wire_delivery_owned_bytes()
    );
    assert_eq!(
        report.observed_work.non_wire_owned_bytes(),
        baseline.observed_work.non_wire_owned_bytes()
    );
    assert_eq!(
        report.work_budget.validate_observed(report.observed_work),
        Ok(())
    );
    assert_eq!(report.target_open_attempts, 1);
    assert_eq!(report.client_to_target, CLIENT_BYTES);
    assert_eq!(report.target_to_client, OWNER_BYTES);
    assert_eq!(report.outbound_queued_frames, 0);
    assert_eq!(report.outbound_queued_bytes, 0);
    assert_eq!(report.scheduler_pending_actions, 0);
    assert_eq!(report.scheduler_pending_bytes, 0);
}

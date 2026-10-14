use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use bytes::Bytes;

use super::*;
use crate::owned_upstream::FlowPortConfig;
use crate::owned_upstream::owner_target::{
    OwnerTargetConfig, OwnerTargetExecutor, OwnerTargetOutput,
};
use crate::owned_upstream::session_clock::SessionMonotonicClock;
use crate::owned_upstream::supervisor::SessionSupervisor;
use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig, MemoryWriteDirective};
use crate::resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy,
    AttachTransportBinding, ByteOffset, DevicePrincipal, DeviceSecret, Direction, FeatureOffer,
    Frame, LegGeneration, OwnerIdentity, ReceiveBudgetLimits, Record, ReplayBudgetLimits,
    ResetReason, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionConfig, SessionEvent,
    SessionFlowId, SessionId, SessionModel, SessionRole, TcpWindowLimits, TlsExporterBinding,
    VersionRange,
};

#[test]
fn output_page_capacity_is_checked_from_the_target_effect_bound() {
    let (_leg, executor) = executor_with_effect_steps(3);
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let controller = LegacyOwnerSessionController::new_for_test(executor, clock).unwrap();

    let capacity = controller
        .output_page_capacity_for_test()
        .expect("three protocol frames have a representable byte bound");

    assert_eq!(capacity.max_outputs(), 3);
    assert_eq!(
        capacity.max_encoded_bytes(),
        3 * crate::resumable::MAX_ENCODED_DATA_FRAME_BYTES
    );
    assert_eq!(
        OwnerOutputPageCapacity::checked_from_max_outputs(usize::MAX),
        Err(OwnerOutputPageCapacityError::EncodedByteCapacityOverflow {
            max_outputs: usize::MAX,
            max_frame_encoded_bytes: crate::resumable::MAX_ENCODED_DATA_FRAME_BYTES,
        })
    );
}
use crate::shared::TargetAddr;

mod attached_recovery;
mod capacity;

fn credentials() -> AttachCredentials {
    AttachCredentials::new(
        DeviceSecret::new([0xde; 32]).unwrap(),
        ResumeSecret::new([0xad; 32]).unwrap(),
    )
    .unwrap()
}

fn binding(exporter: u8) -> AttachTransportBinding {
    AttachTransportBinding::new(
        OwnerIdentity::new([0x31; 32]).unwrap(),
        AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
        TlsExporterBinding::new([exporter; 32]).unwrap(),
        DevicePrincipal::new([0x53; 16]).unwrap(),
    )
}

fn committed_leg() -> crate::resumable::CommittedLeg {
    let session_id = SessionId::new([0x11; 16]).unwrap();
    let transport = binding(0x42);
    let request = crate::resumable::AttachRequest::new(
        session_id,
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([0x22; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(0b111, 0b001).unwrap(),
    );
    let proof = credentials().prove(&request, &transport).unwrap();
    AttachAuthority::new(
        session_id,
        LegGeneration::new(1).unwrap(),
        credentials(),
        AttachPolicy::new(
            OwnerIdentity::new([0x31; 32]).unwrap(),
            AttachAlpn::new(b"mini-vpn-owned/1").unwrap(),
            DevicePrincipal::new([0x53; 16]).unwrap(),
            SESSION_PROTOCOL_VERSION,
            0b1111,
        )
        .unwrap(),
    )
    .verify_and_commit(&request, &transport, &proof)
    .unwrap()
}

fn session_config() -> SessionConfig {
    SessionConfig::new(
        4,
        8,
        TcpWindowLimits::new(128, 16).unwrap(),
        TcpWindowLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReplayBudgetLimits::new(128, 16).unwrap(),
        ReceiveBudgetLimits::new(256, 32).unwrap(),
        128,
    )
    .unwrap()
}

fn target(port: u16) -> TargetAddr {
    TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
}

fn executor() -> (
    crate::resumable::CommittedLeg,
    OwnerTargetExecutor<MemoryTarget>,
) {
    executor_with_effect_steps(128)
}

fn executor_with_effect_steps(
    max_effect_steps: usize,
) -> (
    crate::resumable::CommittedLeg,
    OwnerTargetExecutor<MemoryTarget>,
) {
    let leg = committed_leg();
    let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
    let supervisor = SessionSupervisor::new(model);
    let target = MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap());
    (
        leg,
        OwnerTargetExecutor::new(
            OwnerTargetConfig::new(max_effect_steps, 1_024, 64).unwrap(),
            supervisor,
            target,
            FlowPortConfig::new(128, 8, 8, 8).unwrap(),
        )
        .unwrap(),
    )
}

fn arrived(event: SessionEvent, clock: &SessionMonotonicClock) -> OwnerSessionActorEvent {
    OwnerSessionActorEvent::capture_for_test(event, clock).unwrap()
}

fn peer_event(leg: crate::resumable::CommittedLeg, record: Record) -> SessionEvent {
    SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(leg.generation(), record).unwrap(),
    }
}

fn transmitted(outputs: &[OwnerTargetOutput]) -> Vec<&Record> {
    outputs
        .iter()
        .filter_map(|output| match output {
            OwnerTargetOutput::Transmit(frame) => Some(frame.record()),
            _ => None,
        })
        .collect()
}

fn controller_with_pending_open(
    port: u16,
) -> (
    SessionMonotonicClock,
    LegacyOwnerSessionController<MemoryTarget>,
) {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = executor_with_effect_steps(1);
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let drive = controller
        .apply_actor_event(arrived(
            peer_event(
                leg,
                Record::Open {
                    flow_id: SessionFlowId::new(1).unwrap(),
                    target: target(port),
                },
            ),
            &clock,
        ))
        .unwrap();
    assert!(drive.needs_continue());
    (clock, controller)
}

fn controller_with_aborted_ack_page() -> LegacyOwnerSessionController<MemoryTarget> {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = executor();
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();

    assert!(
        !controller
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
            .unwrap()
            .needs_continue()
    );
    controller
        .target_mut_for_test()
        .feed_read(flow_id, Bytes::from_static(b"reply"))
        .unwrap();
    assert!(
        !controller
            .try_read_target(flow_id)
            .unwrap()
            .needs_continue()
    );

    controller.fail_next_continuation_progress_for_test();
    let error = controller
        .apply_actor_event(arrived(
            peer_event(
                leg,
                Record::Ack {
                    flow_id,
                    direction: Direction::TargetToClient,
                    next_accepted: ByteOffset::new(5),
                    final_accepted: false,
                },
            ),
            &clock,
        ))
        .unwrap_err();
    assert_eq!(
        format!("{error:?}"),
        "PostCommitProgress(MissingActorArrival)"
    );
    controller
}

#[tokio::test(start_paused = true)]
async fn continuation_outcome_closes_advanced_and_missing_ownership() {
    let (_clock, mut controller) = controller_with_pending_open(443);

    let advanced = match controller.continue_effects_for_test() {
        OwnerSessionContinuationOutcome::Advanced(drive) => drive,
        other => panic!("expected an advanced continuation, got {other:?}"),
    };
    assert!(!advanced.needs_continue());
    assert_eq!(controller.snapshot().pending_continuations, 0);

    let missing = match controller.continue_effects_for_test() {
        OwnerSessionContinuationOutcome::Missing(error) => error,
        other => panic!("expected missing continuation ownership, got {other:?}"),
    };
    assert_eq!(format!("{missing:?}"), "NoPendingContinuation");
}

#[tokio::test(start_paused = true)]
async fn rejected_foreign_resume_reports_retained_exact_ownership() {
    let (_first_clock, mut first) = controller_with_pending_open(443);
    let (_second_clock, mut second) = controller_with_pending_open(8443);
    std::mem::swap(
        &mut first.pending_continuation,
        &mut second.pending_continuation,
    );

    let error = match first.continue_effects_for_test() {
        OwnerSessionContinuationOutcome::TerminalRetained(error) => error,
        other => panic!("foreign resume must be terminal-retained, got {other:?}"),
    };

    assert!(format!("{error:?}").starts_with("RejectedContinuation("));
    assert_eq!(first.snapshot().pending_continuations, 1);
    assert_eq!(first.snapshot().retained_continuations, 0);

    std::mem::swap(
        &mut first.pending_continuation,
        &mut second.pending_continuation,
    );
    for controller in [&mut first, &mut second] {
        let drive = match controller.continue_effects_for_test() {
            OwnerSessionContinuationOutcome::Advanced(drive) => drive,
            other => panic!("restored exact continuation must advance once, got {other:?}"),
        };
        assert!(!drive.needs_continue());
        assert_eq!(controller.snapshot().pending_continuations, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn pre_take_phase_failure_leaves_exact_continuation_pending() {
    let (_clock, mut controller) = controller_with_pending_open(443);
    controller.phase = OwnerSessionControllerPhase::SessionExpired;

    let error = match controller.continue_effects_for_test() {
        OwnerSessionContinuationOutcome::TerminalRetained(error) => error,
        other => panic!("pre-take failure must be terminal-retained, got {other:?}"),
    };

    assert_eq!(format!("{error:?}"), "SessionExpired");
    assert_eq!(controller.snapshot().pending_continuations, 1);
    assert_eq!(controller.snapshot().retained_continuations, 0);
}

#[tokio::test(start_paused = true)]
async fn pre_take_clock_failure_is_terminal_retained_without_losing_the_continuation() {
    let (clock, mut controller) = controller_with_pending_open(443);
    controller.clock = SessionMonotonicClock::from_epoch_for_test(
        tokio::time::Instant::now() + Duration::from_nanos(1),
    );

    let error = match controller.continue_effects_for_test() {
        OwnerSessionContinuationOutcome::TerminalRetained(error) => error,
        other => panic!("clock failure before take must be terminal-retained, got {other:?}"),
    };
    assert_eq!(format!("{error:?}"), "Clock(TimeWentBackwards)");
    assert_eq!(controller.snapshot().pending_continuations, 1);
    assert!(controller.executor.snapshot().needs_resume);

    controller.clock = clock;
    let drive = match controller.continue_effects_for_test() {
        OwnerSessionContinuationOutcome::Advanced(drive) => drive,
        other => panic!("the retained continuation must remain executable, got {other:?}"),
    };
    assert!(!drive.needs_continue());
    assert_eq!(controller.snapshot().pending_continuations, 0);
    assert!(!controller.executor.snapshot().needs_resume);
}

#[tokio::test(start_paused = true)]
async fn post_commit_progress_failure_reports_aborted_and_retains_generated_page() {
    let (_clock, mut controller) = controller_with_pending_open(443);
    controller.fail_next_continuation_progress_for_test();

    let error = match controller.continue_effects_for_test() {
        OwnerSessionContinuationOutcome::Aborted(error) => error,
        other => panic!("post-commit continuation failure must abort, got {other:?}"),
    };

    assert_eq!(
        format!("{error:?}"),
        "PostCommitProgress(MissingActorArrival)"
    );
    let snapshot = controller.snapshot();
    assert!(snapshot.failed);
    assert_eq!(snapshot.pending_continuations, 0);
    assert_eq!(snapshot.retained_outputs, 1);
    assert_eq!(snapshot.retained_continuations, 0);
    assert!(controller.aborted_turn.is_some());
}

#[tokio::test(start_paused = true)]
async fn invalid_page_inventory_classifies_raw_resume_without_false_zero() {
    let (_clock, mut controller) = controller_with_pending_open(443);
    let (_other_clock, mut other) = controller_with_pending_open(8443);
    let PendingOwnerContinuation {
        resume: prefix_resume,
        origin,
    } = controller
        .pending_continuation
        .take()
        .expect("fixture must own one exact continuation");
    let PendingOwnerContinuation {
        resume: tail_resume,
        origin: _,
    } = other
        .pending_continuation
        .take()
        .expect("second fixture must own one exact continuation");
    let outputs = vec![
        OwnerTargetOutput::NeedsResume {
            resume: prefix_resume,
        },
        OwnerTargetOutput::SessionExpired,
        OwnerTargetOutput::NeedsResume {
            resume: tail_resume,
        },
    ];
    let page_ptr = outputs.as_ptr();
    let page_capacity = outputs.capacity();

    controller.quarantine_invalid_continuation_page(
        outputs,
        OwnerTargetReducerObservations::default(),
        origin,
    );

    let snapshot = controller.snapshot();
    assert!(snapshot.failed);
    assert_eq!(snapshot.pending_continuations, 0);
    assert_eq!(snapshot.retained_outputs, 1);
    assert_eq!(snapshot.retained_continuations, 2);
    let aborted = controller.aborted_turn.as_ref().unwrap();
    assert_eq!(aborted.outputs.as_ptr(), page_ptr);
    assert_eq!(aborted.outputs.capacity(), page_capacity);
    assert!(matches!(
        aborted.outputs.as_slice(),
        [
            OwnerTargetOutput::NeedsResume { .. },
            OwnerTargetOutput::SessionExpired,
            OwnerTargetOutput::NeedsResume { .. }
        ]
    ));
    assert!(matches!(
        aborted.page_origin,
        OwnerContinuationOrigin::Actor(_)
    ));
}

#[tokio::test(start_paused = true)]
async fn controller_cold_inventory_independently_audits_bytes_and_arrival_origins() {
    let (_clock, mut controller) = controller_with_pending_open(443);
    let (_other_clock, mut other) = controller_with_pending_open(8443);
    let PendingOwnerContinuation {
        resume: prefix_resume,
        origin,
    } = controller
        .pending_continuation
        .take()
        .expect("fixture must own one exact continuation");
    let tail_resume = other
        .pending_continuation
        .take()
        .expect("second fixture must own one exact continuation")
        .resume;
    let frame = Frame::try_new(
        LegGeneration::new(7).expect("nonzero generation"),
        Record::Data {
            flow_id: SessionFlowId::new(9).expect("nonzero flow"),
            direction: Direction::TargetToClient,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"cold-ledger"),
        },
    )
    .expect("valid retained DATA frame");
    let frame_bytes = frame
        .encoded_len_checked()
        .expect("valid frame has an exact checked wire length");

    controller.quarantine_invalid_continuation_page(
        vec![
            OwnerTargetOutput::NeedsResume {
                resume: prefix_resume,
            },
            OwnerTargetOutput::Transmit(frame),
            OwnerTargetOutput::SessionExpired,
            OwnerTargetOutput::NeedsResume {
                resume: tail_resume,
            },
        ],
        OwnerTargetReducerObservations::default(),
        origin,
    );

    let inventory = controller
        .cold_inventory()
        .expect("the retained malformed page remains independently auditable");
    assert_eq!(inventory.retained_outputs(), 2);
    assert_eq!(inventory.independently_summed_frame_bytes(), frame_bytes);
    assert_eq!(inventory.stored_frame_bytes(), frame_bytes);
    assert_eq!(inventory.pending_continuations(), 0);
    assert_eq!(inventory.retained_continuations(), 2);
    assert_eq!(inventory.malformed_raw_continuations(), 2);
    assert_eq!(inventory.total_continuations(), 2);
    assert_eq!(inventory.actor_origin_witnesses(), 1);
    assert_eq!(inventory.local_origin_witnesses(), 0);
    assert_eq!(inventory.malformed_page_origin_witnesses(), 1);
    assert_eq!(inventory.malformed_page_arrival_witnesses(), 1);
    assert_eq!(inventory.retained_observations(), 0);
    assert_eq!(inventory.retained_application_ack_observations(), 0);
    assert_eq!(inventory.target_aborted_effect_guards(), 0);
    assert_eq!(inventory.target_aborted_effects(), 0);
    assert_eq!(inventory.target_retained_recovery_outputs(), 0);
    assert_eq!(inventory.target_retained_recovery_observations(), 0);
    assert_eq!(inventory.controller_retained_recovery_turns(), 0);
    assert_eq!(inventory.target_retained_recovery_guards(), 0);
}

#[tokio::test(start_paused = true)]
async fn malformed_local_page_counts_one_shared_origin_without_actor_arrival() {
    let (_clock, mut controller) = controller_with_pending_open(443);
    let (_other_clock, mut other) = controller_with_pending_open(8443);
    let prefix_resume = controller
        .pending_continuation
        .take()
        .expect("fixture must own one exact continuation")
        .resume;
    let tail_resume = other
        .pending_continuation
        .take()
        .expect("second fixture must own one exact continuation")
        .resume;

    controller.quarantine_invalid_continuation_page(
        vec![
            OwnerTargetOutput::NeedsResume {
                resume: prefix_resume,
            },
            OwnerTargetOutput::NeedsResume {
                resume: tail_resume,
            },
        ],
        OwnerTargetReducerObservations::default(),
        OwnerContinuationOrigin::Local,
    );

    let inventory = controller
        .cold_inventory()
        .expect("one local page origin must cover both malformed raw resumes");
    assert_eq!(inventory.malformed_raw_continuations(), 2);
    assert_eq!(inventory.total_continuations(), 2);
    assert_eq!(inventory.actor_origin_witnesses(), 0);
    assert_eq!(inventory.local_origin_witnesses(), 1);
    assert_eq!(inventory.malformed_page_origin_witnesses(), 1);
    assert_eq!(inventory.malformed_page_arrival_witnesses(), 0);
}

#[tokio::test(start_paused = true)]
async fn aborted_ack_page_without_tail_retains_its_actor_arrival_origin() {
    let controller = controller_with_aborted_ack_page();

    let inventory = controller
        .cold_inventory()
        .expect("the aborted ACK page must retain auditable actor provenance");
    assert_eq!(inventory.retained_continuations(), 0);
    assert_eq!(inventory.retained_observations(), 1);
    assert_eq!(inventory.retained_application_ack_observations(), 1);
    assert_eq!(inventory.actor_origin_witnesses(), 1);
    assert_eq!(inventory.local_origin_witnesses(), 0);
}

#[tokio::test(start_paused = true)]
async fn retained_ack_with_local_page_origin_fails_closed() {
    let mut controller = controller_with_aborted_ack_page();
    controller
        .aborted_turn
        .as_mut()
        .expect("fixture must retain one aborted ACK page")
        .page_origin = OwnerContinuationOrigin::Local;

    assert_eq!(
        controller.cold_inventory(),
        Err(OwnerSessionControllerColdInventoryError::RetainedApplicationAckWithoutActorOrigin)
    );
}

#[tokio::test(start_paused = true)]
async fn controller_cold_inventory_rejects_stored_output_count_drift() {
    let mut controller = controller_with_aborted_ack_page();
    controller
        .aborted_turn
        .as_mut()
        .expect("fixture must retain one aborted page")
        .stored_output_count = 1;

    assert_eq!(
        controller.cold_inventory(),
        Err(
            OwnerSessionControllerColdInventoryError::StoredOutputCountMismatch {
                stored: 1,
                summed: 0,
            }
        )
    );
}

#[tokio::test(start_paused = true)]
async fn controller_cold_inventory_rejects_stored_frame_byte_drift() {
    let mut controller = controller_with_aborted_ack_page();
    controller
        .aborted_turn
        .as_mut()
        .expect("fixture must retain one aborted page")
        .stored_frame_bytes = StoredOwnerControllerFrameBytes::Exact(1);

    assert_eq!(
        controller.cold_inventory(),
        Err(
            OwnerSessionControllerColdInventoryError::StoredFrameByteMismatch {
                stored: 1,
                summed: 0,
            }
        )
    );
}

#[tokio::test(start_paused = true)]
async fn controller_cold_inventory_exposes_retained_recovery_as_nonzero() {
    let (_leg, executor) = executor();
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let mut controller = LegacyOwnerSessionController::new_for_test(executor, clock).unwrap();
    controller.aborted_recovery = Some(AbortedOwnerRecoveryTurn {
        pending: None,
        outputs: Vec::new(),
        observations: None,
    });

    let inventory = controller
        .cold_inventory()
        .expect("a recovery residual without ACK provenance remains countable");
    assert_eq!(inventory.retained_outputs(), 0);
    assert_eq!(inventory.total_continuations(), 0);
    assert_eq!(inventory.controller_retained_recovery_turns(), 1);
    assert_eq!(inventory.target_retained_recovery_guards(), 0);
}

#[test]
fn controller_frame_byte_sum_fails_closed_without_large_allocations() {
    assert!(matches!(
        AbortedOwnerControllerTurn::sum_checked_frame_lengths([
            Err(ProtocolError::EmptyData),
            Ok(1),
        ]),
        Err(OwnerControllerFrameByteAuditError::InvalidFrame(
            ProtocolError::EmptyData
        ))
    ));
    assert!(matches!(
        AbortedOwnerControllerTurn::sum_checked_frame_lengths([Ok(usize::MAX), Ok(1),]),
        Err(OwnerControllerFrameByteAuditError::SumOverflow)
    ));
}

#[test]
fn controller_frame_byte_audit_errors_map_exhaustively_to_cold_verdicts() {
    assert_eq!(
        OwnerControllerFrameByteAuditError::InvalidFrame(ProtocolError::EmptyData)
            .into_cold_inventory_error(),
        OwnerSessionControllerColdInventoryError::InvalidRetainedFrame(ProtocolError::EmptyData,)
    );
    assert_eq!(
        OwnerControllerFrameByteAuditError::SumOverflow.into_cold_inventory_error(),
        OwnerSessionControllerColdInventoryError::FrameByteSumOverflow,
    );

    assert_eq!(
        StoredOwnerControllerFrameBytes::InvalidFrame(ProtocolError::EmptyData)
            .cold_inventory_result(),
        Err(
            OwnerSessionControllerColdInventoryError::StoredFrameInvalid(ProtocolError::EmptyData,)
        )
    );
    assert_eq!(
        StoredOwnerControllerFrameBytes::SumOverflow.cold_inventory_result(),
        Err(OwnerSessionControllerColdInventoryError::StoredFrameByteSumOverflow)
    );
}

#[tokio::test(start_paused = true)]
async fn fatal_after_resume_consumption_reports_aborted_not_retained() {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = executor_with_effect_steps(1);
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();

    let mut page = controller
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
    while page.needs_continue() {
        page = controller.continue_effects_result_for_test().unwrap();
    }
    controller
        .target_mut_for_test()
        .push_write_directive(flow_id, MemoryWriteDirective::WouldBlock)
        .unwrap();
    page = controller
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
    assert!(!page.needs_continue());
    let close = controller
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
        .unwrap();
    assert!(!close.needs_continue());
    let accepted = controller.retry_target_write(flow_id).unwrap();
    assert!(accepted.needs_continue());
    assert_eq!(controller.snapshot().pending_continuations, 1);

    controller.executor.exhaust_effect_resume_ids_for_test();
    let error = match controller.continue_effects_for_test() {
        OwnerSessionContinuationOutcome::Aborted(error) => error,
        other => panic!("consumed fatal continuation must abort, got {other:?}"),
    };

    assert_eq!(
        format!("{error:?}"),
        "Target(\"owner Target effect resume ID space is exhausted\")"
    );
    assert_eq!(controller.snapshot().pending_continuations, 0);
    assert!(controller.snapshot().failed);
    let target = controller.executor.snapshot();
    assert!(target.aborted_effects > 0 || target.aborted_outputs > 0);
    let inventory = controller
        .cold_inventory()
        .expect("Target fail-closed work remains countable by the controller inventory");
    assert_eq!(inventory.target_aborted_effect_guards(), 1);
    assert!(inventory.target_aborted_effects() > 0);
}

#[tokio::test(start_paused = true)]
async fn controller_consumes_outstanding_partial_ack_and_terminal_with_actor_arrival() {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = executor();
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();

    assert!(
        !controller
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
            .unwrap()
            .needs_continue()
    );
    controller
        .target_mut_for_test()
        .feed_read(flow_id, Bytes::from_static(b"reply"))
        .unwrap();

    tokio::time::advance(Duration::from_nanos(10)).await;
    let outputs = controller.try_read_target(flow_id).unwrap();
    assert!(transmitted(outputs.outputs()).iter().any(|record| matches!(
        record,
        Record::Data {
            flow_id: observed_flow,
            direction: Direction::TargetToClient,
            offset,
            payload,
        } if *observed_flow == flow_id
            && *offset == ByteOffset::new(0)
            && payload.as_ref() == b"reply"
    )));
    let outstanding = controller
        .reverse_progress(flow_id, Direction::TargetToClient)
        .unwrap();
    assert_eq!(
        outstanding.oldest_unacknowledged(),
        Some(ByteOffset::new(0))
    );
    assert_eq!(
        outstanding.outstanding_since(),
        Some(crate::owned_upstream::two_leg::SimTime::from_nanos(10))
    );
    assert_eq!(outstanding.latest_positive_ack(), None);
    assert!(!outstanding.terminal());

    tokio::time::advance(Duration::from_nanos(10)).await;
    let partial_ack = arrived(
        peer_event(
            leg,
            Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(2),
                final_accepted: false,
            },
        ),
        &clock,
    );
    tokio::time::advance(Duration::from_nanos(50)).await;
    assert!(
        controller
            .apply_actor_event(partial_ack)
            .unwrap()
            .outputs()
            .is_empty()
    );
    let partial = controller
        .reverse_progress(flow_id, Direction::TargetToClient)
        .unwrap();
    assert_eq!(partial.oldest_unacknowledged(), Some(ByteOffset::new(2)));
    assert_eq!(
        partial.outstanding_since(),
        Some(crate::owned_upstream::two_leg::SimTime::from_nanos(10))
    );
    assert_eq!(
        partial.latest_positive_ack(),
        Some((
            ByteOffset::new(2),
            crate::owned_upstream::two_leg::SimTime::from_nanos(20),
        ))
    );

    assert!(
        !controller
            .apply_actor_event(arrived(
                peer_event(
                    leg,
                    Record::Close {
                        flow_id,
                        direction: Direction::ClientToTarget,
                        final_offset: ByteOffset::new(0),
                    },
                ),
                &clock,
            ))
            .unwrap()
            .needs_continue()
    );
    controller
        .target_mut_for_test()
        .finish_read(flow_id)
        .unwrap();
    assert!(
        !controller
            .try_read_target(flow_id)
            .unwrap()
            .needs_continue()
    );

    tokio::time::advance(Duration::from_nanos(10)).await;
    let final_ack = arrived(
        peer_event(
            leg,
            Record::Ack {
                flow_id,
                direction: Direction::TargetToClient,
                next_accepted: ByteOffset::new(5),
                final_accepted: true,
            },
        ),
        &clock,
    );
    tokio::time::advance(Duration::from_nanos(50)).await;
    let outputs = controller.apply_actor_event(final_ack).unwrap();
    assert!(outputs.outputs().iter().any(|output| matches!(
        output,
        OwnerTargetOutput::TerminalGraceStarted { terminal }
            if terminal.flow_id() == flow_id
    )));
    let terminal = controller
        .reverse_progress(flow_id, Direction::TargetToClient)
        .unwrap();
    assert_eq!(terminal.oldest_unacknowledged(), None);
    assert_eq!(terminal.outstanding_since(), None);
    assert_eq!(
        terminal.latest_positive_ack(),
        Some((
            ByteOffset::new(5),
            crate::owned_upstream::two_leg::SimTime::from_nanos(80),
        ))
    );
    assert!(terminal.terminal());
}

#[tokio::test(start_paused = true)]
async fn controller_rebases_retained_old_generation_on_current_reducer_ack() {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = executor();
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let flow_id = SessionFlowId::new(2).unwrap();

    assert!(
        !controller
            .apply_actor_event(arrived(
                peer_event(
                    leg,
                    Record::Open {
                        flow_id,
                        target: target(8443),
                    },
                ),
                &clock,
            ))
            .unwrap()
            .needs_continue()
    );
    controller
        .target_mut_for_test()
        .feed_read(flow_id, Bytes::from_static(b"old"))
        .unwrap();
    tokio::time::advance(Duration::from_nanos(10)).await;
    assert!(
        !controller
            .try_read_target(flow_id)
            .unwrap()
            .needs_continue()
    );

    // Model the bounded progress that survives a physically committed
    // replacement. The ACK below remains a real reducer-accepted gen-2 ACK;
    // no observation is fabricated by this test.
    controller.progress.reset_for_session_expiry();
    let old_generation = LegGeneration::new(1).unwrap();
    controller
        .progress
        .register_live(old_generation, flow_id, Direction::TargetToClient)
        .unwrap();
    controller
        .progress
        .observe_outstanding(
            old_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(0),
            crate::owned_upstream::two_leg::SimTime::from_nanos(10),
        )
        .unwrap();

    tokio::time::advance(Duration::from_nanos(10)).await;
    assert!(
        !controller
            .apply_actor_event(arrived(
                peer_event(
                    leg,
                    Record::Ack {
                        flow_id,
                        direction: Direction::TargetToClient,
                        next_accepted: ByteOffset::new(3),
                        final_accepted: false,
                    },
                ),
                &clock,
            ))
            .expect("current committed generation ACK rebases retained progress")
            .needs_continue()
    );

    let progress = controller
        .reverse_progress(flow_id, Direction::TargetToClient)
        .unwrap();
    assert_eq!(progress.oldest_unacknowledged(), None);
    assert_eq!(progress.outstanding_since(), None);
    assert_eq!(
        progress.latest_positive_ack(),
        Some((
            ByteOffset::new(3),
            crate::owned_upstream::two_leg::SimTime::from_nanos(20),
        ))
    );
    assert!(!controller.snapshot().failed);
}

#[tokio::test(start_paused = true)]
async fn controller_retires_progress_only_after_terminal_grace_expiry_under_churn() {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = executor();
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();

    for raw_flow_id in 1..=10 {
        let flow_id = SessionFlowId::new(raw_flow_id).unwrap();
        assert!(
            !controller
                .apply_actor_event(arrived(
                    peer_event(
                        leg,
                        Record::Open {
                            flow_id,
                            target: target(10_000 + raw_flow_id as u16),
                        },
                    ),
                    &clock,
                ))
                .unwrap()
                .needs_continue()
        );
        controller
            .target_mut_for_test()
            .feed_read(flow_id, Bytes::from_static(b"x"))
            .unwrap();
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert!(
            !controller
                .try_read_target(flow_id)
                .unwrap()
                .needs_continue()
        );

        let outputs = controller
            .apply_actor_event(arrived(
                peer_event(
                    leg,
                    Record::Reset {
                        flow_id,
                        reason: ResetReason::TargetFailure,
                    },
                ),
                &clock,
            ))
            .unwrap();
        let terminal = outputs
            .outputs()
            .iter()
            .find_map(|output| match output {
                OwnerTargetOutput::TerminalGraceStarted { terminal } => Some(*terminal),
                _ => None,
            })
            .expect("reducer publishes terminal-grace authority");
        assert!(
            controller.snapshot().progress_entries > 0,
            "directional terminal must not retire flow progress"
        );

        assert!(
            !controller
                .apply_actor_event(arrived(
                    SessionEvent::TerminalGraceExpired { terminal },
                    &clock,
                ))
                .expect("reducer-proven terminal retirement")
                .needs_continue()
        );
        assert_eq!(controller.snapshot().progress_entries, 0);
    }
    assert!(!controller.snapshot().failed);
}

#[tokio::test(start_paused = true)]
async fn controller_is_sticky_failed_after_supervisor_post_commit_receipt_failure() {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = executor();
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let first_flow_id = SessionFlowId::new(1).unwrap();
    let second_flow_id = SessionFlowId::new(2).unwrap();

    for (flow_id, payload) in [
        (first_flow_id, Bytes::from_static(b"a")),
        (second_flow_id, Bytes::from_static(b"b")),
    ] {
        assert!(
            !controller
                .apply_actor_event(arrived(
                    peer_event(
                        leg,
                        Record::Open {
                            flow_id,
                            target: target(20_000 + flow_id.get() as u16),
                        },
                    ),
                    &clock,
                ))
                .unwrap()
                .needs_continue()
        );
        controller
            .target_mut_for_test()
            .feed_read(flow_id, payload)
            .unwrap();
        assert!(
            !controller
                .try_read_target(flow_id)
                .unwrap()
                .needs_continue()
        );
    }

    assert!(
        controller.executor.swap_uplink_replay_ownership_for_test(
            first_flow_id,
            second_flow_id,
            Direction::TargetToClient,
        ),
        "fixture requires one exact retained replay extent per flow"
    );
    let first_error = controller
        .apply_actor_event(arrived(
            peer_event(
                leg,
                Record::Ack {
                    flow_id: first_flow_id,
                    direction: Direction::TargetToClient,
                    next_accepted: ByteOffset::new(1),
                    final_accepted: false,
                },
            ),
            &clock,
        ))
        .unwrap_err();
    assert_eq!(
        format!("{first_error:?}"),
        "Target(\"session supervisor rejected owner Target work: TCP ownership port rejected a reducer receipt: application ACK does not match the retained uplink replay extent\")"
    );

    let after_first_error = controller.snapshot();
    let read_calls_before_retry = controller.target_mut_for_test().snapshot().read_calls;
    let retry = controller.try_read_target(first_flow_id);
    let read_calls_after_retry = controller.target_mut_for_test().snapshot().read_calls;
    let retry_error = retry.as_ref().err().map(|error| format!("{error:?}"));
    assert_eq!(
        (
            after_first_error.failed,
            after_first_error.retained_outputs,
            after_first_error.retained_observations,
            retry_error.as_deref(),
            read_calls_after_retry,
        ),
        (true, 0, 0, Some("Failed"), read_calls_before_retry),
        "post-model receipt poison must fail the controller before any later Target I/O"
    );
}

#[tokio::test(start_paused = true)]
async fn controller_consumes_terminal_observation_from_exact_second_effect_page() {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = executor_with_effect_steps(1);
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();

    let open = controller
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
    assert!(open.needs_continue());
    let open_result = controller.continue_effects_result_for_test().unwrap();
    assert!(!open_result.needs_continue());
    assert!(
        transmitted(open_result.outputs())
            .iter()
            .any(|record| matches!(
                record,
                Record::OpenResult {
                    flow_id: observed,
                    ..
                } if *observed == flow_id
            ))
    );

    controller
        .target_mut_for_test()
        .feed_read(flow_id, Bytes::from_static(b"r"))
        .unwrap();
    let data = controller.try_read_target(flow_id).unwrap();
    assert!(!data.needs_continue());
    controller
        .target_mut_for_test()
        .finish_read(flow_id)
        .unwrap();
    let close = controller.try_read_target(flow_id).unwrap();
    assert!(!close.needs_continue());

    let final_ack = controller
        .apply_actor_event(arrived(
            peer_event(
                leg,
                Record::Ack {
                    flow_id,
                    direction: Direction::TargetToClient,
                    next_accepted: ByteOffset::new(1),
                    final_accepted: true,
                },
            ),
            &clock,
        ))
        .unwrap();
    assert!(!final_ack.needs_continue());
    assert!(
        !controller
            .reverse_progress(flow_id, Direction::TargetToClient)
            .unwrap()
            .terminal(),
        "the opposite direction must remain live before the nested half-close commit"
    );

    controller
        .target_mut_for_test()
        .push_write_directive(flow_id, MemoryWriteDirective::WouldBlock)
        .unwrap();
    let data = controller
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
    assert!(!data.needs_continue());
    let close = controller
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
        .unwrap();
    assert!(!close.needs_continue());

    let accepted = controller.retry_target_write(flow_id).unwrap();
    assert!(accepted.needs_continue());
    assert!(
        transmitted(accepted.outputs())
            .iter()
            .any(|record| matches!(
                record,
                Record::Ack {
                    flow_id: observed,
                    direction: Direction::ClientToTarget,
                    next_accepted,
                    ..
                } if *observed == flow_id && *next_accepted == ByteOffset::new(1)
            ))
    );

    let half_closed = controller.continue_effects_result_for_test().unwrap();
    assert!(half_closed.needs_continue());
    assert!(half_closed.outputs().is_empty());
    assert!(
        controller
            .reverse_progress(flow_id, Direction::TargetToClient)
            .unwrap()
            .terminal(),
        "the resume-page reducer terminal observation must commit before its following outputs become visible"
    );

    let terminal_outputs = controller.continue_effects_result_for_test().unwrap();
    assert!(terminal_outputs.needs_continue());
    let terminal = controller.continue_effects_result_for_test().unwrap();
    assert!(!terminal.needs_continue());
    assert!(terminal.outputs().iter().any(|output| matches!(
        output,
        OwnerTargetOutput::TerminalGraceStarted { terminal }
            if terminal.flow_id() == flow_id
    )));
}

#[tokio::test(start_paused = true)]
async fn controller_finishes_exact_session_expiry_continuation_before_becoming_inert() {
    let clock = SessionMonotonicClock::from_epoch_for_test(tokio::time::Instant::now());
    let (leg, executor) = executor_with_effect_steps(1);
    let mut controller =
        LegacyOwnerSessionController::new_for_test(executor, clock.clone()).unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();

    let open = controller
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
    assert!(open.needs_continue());
    assert!(
        !controller
            .continue_effects_result_for_test()
            .unwrap()
            .needs_continue()
    );

    let lost = controller
        .apply_actor_event(arrived(SessionEvent::LegLost { leg }, &clock))
        .unwrap();
    assert!(!lost.needs_continue());
    assert!(lost.outputs().iter().any(|output| matches!(
        output,
        OwnerTargetOutput::ResumeGraceStarted { generation }
            if *generation == leg.generation()
    )));

    let mut page = controller
        .apply_actor_event(arrived(SessionEvent::ResumeGraceExpired { leg }, &clock))
        .unwrap();
    assert!(page.needs_continue());
    assert_eq!(controller.snapshot().pending_continuations, 1);
    assert!(
        controller.snapshot().session_expired,
        "the reducer terminal fact is visible while exact Target cleanup remains controller-owned"
    );

    let mut continuation_pages = 0usize;
    let mut saw_expired_output = false;
    while page.needs_continue() {
        continuation_pages += 1;
        assert!(continuation_pages <= 3, "session cleanup must stay bounded");
        page = controller.continue_effects_result_for_test().unwrap();
        saw_expired_output |= page
            .outputs()
            .iter()
            .any(|output| matches!(output, OwnerTargetOutput::SessionExpired));
    }

    assert!(saw_expired_output);
    assert_eq!(continuation_pages, 2);
    assert_eq!(controller.snapshot().pending_continuations, 0);
    assert_eq!(controller.snapshot().retained_continuations, 0);
    assert_eq!(controller.executor.snapshot().target_flows, 0);
    assert!(controller.snapshot().session_expired);
    assert!(!controller.snapshot().failed);
    assert_eq!(
        format!(
            "{:?}",
            controller.continue_effects_result_for_test().unwrap_err()
        ),
        "SessionExpired"
    );
}

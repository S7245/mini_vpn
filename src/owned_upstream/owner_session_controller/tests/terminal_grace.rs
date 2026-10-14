//! Original terminal identity, deferred output and same-clock value preflight.
//!
//! The birth tracer uses checked BoundInactive C; the full-fact tracer holds
//! original Executor observations. Both use real reducer/MemoryTarget paths.
//! Clock tests are value-only; none claim timer/expiry/whole-root closure.
//! Checked-policy rejection proves original inputs can still bind once.

#[path = "terminal_grace/expiry.rs"]
mod expiry;

#[path = "terminal_grace/returned_error.rs"]
mod returned_error;

use super::*;
use crate::owned_upstream::FlowPortConfig;
use crate::owned_upstream::owner_ready::{OwnerReadySessionLimits, ReadyGeometry, ReadyIndex};
use crate::owned_upstream::owner_target::OwnerTargetConfig;
use crate::owned_upstream::supervisor::{OwnerDataUnwindPoint, SessionSupervisor};
use crate::owned_upstream::target::{MemoryTarget, MemoryTargetConfig};
use crate::resumable::ByteOffset;
use crate::resumable::{
    AttachAlpn, AttachAuthority, AttachCredentials, AttachNonce, AttachPolicy,
    AttachTransportBinding, BitsPerSecond, DevicePrincipal, DeviceSecret, DirectionalRates,
    FeatureOffer, Frame, OwnerIdentity, Record, ReplayCapacitySpec, ReplayStorageGeometry,
    ResetReason, ResumeSecret, SESSION_PROTOCOL_VERSION, SessionId, TlsExporterBinding,
    VersionRange,
};
use crate::shared::TargetAddr;
use bytes::Bytes;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn deadline_window_uses_one_original_sample_and_fixed_epoch() {
    let epoch = tokio::time::Instant::now();
    let clock = SessionMonotonicClock::start();
    let same = clock.clone();
    tokio::time::advance(Duration::from_millis(5)).await;
    let window = clock.deadline_window(Duration::from_millis(50)).unwrap();
    assert_eq!(window.0, SimTime::from_nanos(5_000_000));
    assert_eq!(window.1, SimTime::from_nanos(55_000_000));
    assert_eq!(window.2, epoch + Duration::from_millis(55));
    assert_eq!(same.deadline_window(Duration::from_millis(50)), Ok(window));
    tokio::time::advance(Duration::from_millis(25)).await;
    assert_eq!(clock.now(), Ok(SimTime::from_nanos(30_000_000)));
    assert_eq!(window.1, SimTime::from_nanos(55_000_000));
    assert_eq!(window.2, epoch + Duration::from_millis(55));
}

#[test]
fn deadline_window_rejects_zero_interval() {
    let clock = SessionMonotonicClock::start();
    assert_eq!(
        clock.deadline_window(Duration::ZERO),
        Err(SessionClockError::ZeroInterval)
    );
}

#[test]
fn deadline_window_rejects_an_unrepresentable_interval() {
    let clock = SessionMonotonicClock::start();
    let interval = Duration::from_nanos(u64::MAX)
        .checked_add(Duration::from_nanos(1))
        .unwrap();
    assert_eq!(
        clock.deadline_window(interval),
        Err(SessionClockError::Overflow)
    );
}

#[tokio::test(start_paused = true)]
async fn deadline_window_rejects_sum_overflow_without_saturating() {
    let clock = SessionMonotonicClock::start();
    tokio::time::advance(Duration::from_nanos(1)).await;
    assert_eq!(clock.now(), Ok(SimTime::from_nanos(1)));
    assert_eq!(
        clock.deadline_window(Duration::from_nanos(u64::MAX)),
        Err(SessionClockError::Overflow)
    );
}

#[tokio::test(start_paused = true)]
async fn deadline_window_keeps_exact_max_when_the_platform_instant_allows_it() {
    let epoch = tokio::time::Instant::now();
    let clock = SessionMonotonicClock::start();
    let interval = Duration::from_nanos(u64::MAX);
    let absolute = epoch.checked_add(interval);
    let expected = absolute
        .map(|deadline| (SimTime::ZERO, SimTime::from_nanos(u64::MAX), deadline))
        .ok_or(SessionClockError::Overflow);
    assert_eq!(clock.deadline_window(interval), expected);
    println!("exact_max_absolute_representable={}", absolute.is_some());
}

#[tokio::test(start_paused = true)]
async fn deadline_window_rejects_a_sample_before_its_own_epoch() {
    let future_epoch = tokio::time::Instant::now()
        .checked_add(Duration::from_nanos(1))
        .unwrap();
    let clock = SessionMonotonicClock::from_epoch_for_test(future_epoch);
    assert_eq!(
        clock.deadline_window(Duration::from_nanos(1)),
        Err(SessionClockError::TimeWentBackwards)
    );
}

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

fn target(port: u16) -> TargetAddr {
    TargetAddr::IpPort(SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)))
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

fn peer_reset(leg: crate::resumable::CommittedLeg, flow_id: SessionFlowId) -> SessionEvent {
    peer_event(
        leg,
        Record::Reset {
            flow_id,
            reason: ResetReason::TargetFailure,
        },
    )
}

fn terminal_executor(
    receipt_unwind: bool,
) -> (
    OwnerSessionCapacityPlan,
    crate::resumable::CommittedLeg,
    OwnerTargetExecutor<MemoryTarget>,
) {
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(100_000_000).unwrap());
    let storage = ReplayCapacitySpec::new(rates, rates, Duration::from_millis(500), Duration::ZERO)
        .unwrap()
        .derive()
        .unwrap()
        .storage_plan(ReplayStorageGeometry::new(62_500, 4).unwrap())
        .unwrap();
    let plan = OwnerSessionCapacityPlan::derive(storage, 8, 128).unwrap();
    let leg = committed_leg();
    let mut supervisor = SessionSupervisor::from_owner_capacity_plan_for_test(plan, leg);
    if receipt_unwind {
        supervisor.arm_owner_data_unwind(OwnerDataUnwindPoint::ReceiptTerminal);
    }
    let target_io = MemoryTarget::new(
        MemoryTargetConfig::new(4, 256, 512, 128, 128, 16)
            .unwrap()
            .with_max_terminal_tombstones(8)
            .unwrap(),
    );
    let executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(
            1,
            plan.session_config().owner_pending_work_bound().unwrap(),
            64,
        )
        .unwrap(),
        supervisor,
        target_io,
        FlowPortConfig::new(128, 8, 8, 8).unwrap(),
    )
    .unwrap();
    (plan, leg, executor)
}

fn opened_terminal_executor(
    receipt_unwind: bool,
) -> (
    OwnerSessionCapacityPlan,
    crate::resumable::CommittedLeg,
    OwnerTargetExecutor<MemoryTarget>,
    SessionFlowId,
) {
    let (plan, leg, mut executor) = terminal_executor(receipt_unwind);
    let flow_id = SessionFlowId::new(1).unwrap();
    let mut outputs = executor
        .apply_event(peer_event(
            leg,
            Record::Open {
                flow_id,
                target: target(443),
            },
        ))
        .expect("real prebind OPEN creates the original Flow");
    let mut opened = 0;
    for _ in 0..4 {
        let mut continuation = None;
        for output in outputs {
            match output {
                OwnerTargetOutput::NeedsResume { resume } => continuation = Some(resume),
                OwnerTargetOutput::Transmit(frame) => {
                    assert!(
                        matches!(frame.record(), Record::OpenResult { flow_id: actual, .. }
                        if *actual == flow_id)
                    );
                    opened += 1;
                }
                _ => panic!("OPEN cannot publish terminal lifecycle work"),
            }
        }
        let Some(resume) = continuation else {
            break;
        };
        outputs = executor
            .resume_pending_effects(resume)
            .expect("the original OPEN continuation finishes before ready binding");
    }
    assert_eq!(opened, 1);

    (plan, leg, executor, flow_id)
}

fn bound_terminal_controller(
    interval: Duration,
    receipt_unwind: bool,
) -> (
    ReadyIndex,
    crate::resumable::CommittedLeg,
    OwnerSessionController<MemoryTarget>,
    SessionFlowId,
    tokio::time::Instant,
    SessionMonotonicClock,
) {
    let (plan, leg, executor, flow_id) = opened_terminal_executor(receipt_unwind);
    let geometry = ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let limits =
        OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8).unwrap();
    let ready = index.reserve_session(leg.session_id(), limits).unwrap();
    let epoch = tokio::time::Instant::now();
    let clock = SessionMonotonicClock::start();
    let controller =
        OwnerSessionController::new_checked(executor, clock.clone(), plan, ready, interval)
            .expect("the original Flow is imported into the actual checked binding");
    (index, leg, controller, flow_id, epoch, clock)
}

#[tokio::test(start_paused = true)]
async fn bound_target_resume_error_preserves_original_terminal_birth_and_effect_tail() {
    let (_index, leg, mut controller, flow_id, epoch, clock) =
        bound_terminal_controller(Duration::from_millis(50), false);
    for _ in 0..2 {
        let refusal = controller.try_read_target(flow_id).unwrap_err();
        assert!(matches!(
            refusal.kind,
            OwnerSessionControllerErrorKind::Target(
                OwnerTargetError::BoundSourceRequiresSelectedService
            )
        ));
        assert!(!controller.snapshot().failed);
        assert!(controller.aborted_turn.is_none());
        let mut cold = controller.cold_inventory().unwrap();
        assert!(cold.is_exact_zero());
        assert_eq!(cold.retained_target_errors, 0);
        cold.retained_target_errors = 1; // Value-only audit discriminator, not an owner.
        assert!(!cold.is_exact_zero());
    }
    controller.executor.exhaust_effect_resume_ids_for_test();
    tokio::time::advance(Duration::from_millis(5)).await;
    let error = controller
        .apply_actor_event(arrived(peer_reset(leg, flow_id), &clock))
        .unwrap_err();
    assert!(matches!(
        error.kind,
        OwnerSessionControllerErrorKind::Target(OwnerTargetError::EffectResumeIdExhausted)
    ));
    let snapshot = controller.snapshot();
    assert!(snapshot.failed);
    assert_eq!(snapshot.retained_observations, 1);
    assert_eq!(snapshot.pending_continuations, 0);
    assert_eq!(snapshot.retained_continuations, 0);
    let cold = controller.cold_inventory().unwrap();
    assert!(!cold.is_exact_zero());
    assert_eq!(cold.target_aborted_effect_guards(), 1);
    assert_eq!(cold.target_aborted_effects(), 1);
    assert_eq!(cold.retained_observations(), 1);
    assert_eq!(cold.retained_target_errors, 0);
    assert!(
        controller.aborted_turn.is_none(),
        "the known error returned to its caller"
    );
    let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
        panic!("checked C retains the actual terminal fact");
    };
    assert_eq!(ledger.count, 1);
    let TerminalSlotState::AwaitingOutput { fact, window } =
        &ledger.slots[ledger.head.unwrap()].state
    else {
        panic!("original birth slot");
    };
    let original = *fact.terminal();
    assert_eq!(original.session_id(), leg.session_id());
    assert_eq!(original.flow_id(), flow_id);
    assert_eq!(fact.generation(), leg.generation());
    assert_eq!(window.observed_at, SimTime::from_nanos(5_000_000));
    assert_eq!(window.deadline, SimTime::from_nanos(55_000_000));
    assert_eq!(window.absolute, epoch + Duration::from_millis(55));
    tokio::time::advance(Duration::from_millis(25)).await;
    assert!(controller.continue_effects_result_for_test().is_err());
    assert_eq!(controller.snapshot().retained_observations, 1);
    assert_eq!(
        controller
            .cold_inventory()
            .unwrap()
            .target_aborted_effects(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn bound_returned_terminal_unwind_retains_original_page_and_refuses_reentry() {
    for (target_error, receipt_unwind) in [(false, false), (true, false), (false, true)] {
        let (_index, leg, mut controller, flow_id, epoch, clock) =
            bound_terminal_controller(Duration::from_millis(50), receipt_unwind);
        if target_error {
            controller.executor.exhaust_effect_resume_ids_for_test();
        }
        let resumes = usize::from(!target_error);
        let OwnerTerminalGraceState::Bound(ledger) = &mut controller.terminal_grace else {
            panic!("checked C owns the journal and original window");
        };
        ledger.panic_before_returned_page_progress = true;
        tokio::time::advance(Duration::from_millis(5)).await;
        let reset = arrived(peer_reset(leg, flow_id), &clock);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                controller.apply_actor_event(reset)
            }))
            .is_err()
        );
        if receipt_unwind {
            let held = controller.executor.snapshot().session;
            assert_eq!(
                (
                    held.retained_receipt_effects,
                    held.retained_receipt_observations
                ),
                (2, 0)
            );
            assert!(held.poisoned && controller.snapshot().failed);
            assert!(
                controller
                    .apply_actor_event(arrived(peer_reset(leg, flow_id), &clock))
                    .is_err()
            );
            assert_eq!(controller.executor.snapshot().session, held);
            continue;
        }
        let page = controller
            .aborted_turn
            .as_ref()
            .expect("the original returned page survives unwind in C, not on the stack");
        let fact = page
            .observations
            .flow_terminal()
            .expect("the original non-Clone fact");
        let original = *fact.terminal();
        assert_eq!(original.session_id(), leg.session_id());
        assert_eq!(original.flow_id(), flow_id);
        assert_eq!(fact.generation(), leg.generation());
        assert!(
            page.outputs.is_empty(),
            "the lifecycle output is still deferred"
        );
        assert_eq!(page.retained_continuation_count(), resumes);
        assert_eq!(
            page.page_origin,
            OwnerContinuationOrigin::Actor(SimTime::from_nanos(5_000_000))
        );
        let snapshot = controller.snapshot();
        assert!(snapshot.failed);
        assert_eq!(snapshot.retained_observations, 1);
        assert_eq!(snapshot.retained_continuations, resumes);
        let cold = controller.cold_inventory().unwrap();
        assert!(!cold.is_exact_zero());
        assert_eq!(cold.retained_observations(), 1);
        assert_eq!(cold.retained_target_errors, usize::from(target_error));
        assert_eq!(
            matches!(
                page.target_error,
                Some(OwnerTargetError::EffectResumeIdExhausted)
            ),
            target_error
        );
        tokio::time::advance(Duration::from_millis(25)).await;
        assert!(controller.continue_effects_result_for_test().is_err());
        assert!(
            controller
                .apply_actor_event(arrived(peer_reset(leg, flow_id), &clock))
                .is_err()
        );
        let page = controller.aborted_turn.as_ref().unwrap();
        assert_eq!(
            matches!(
                page.target_error,
                Some(OwnerTargetError::EffectResumeIdExhausted)
            ),
            target_error
        );
        assert_eq!(
            page.observations.flow_terminal().unwrap().terminal(),
            &original
        );
        assert_eq!(page.retained_continuation_count(), resumes);
        let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
            panic!("checked journal remains bound");
        };
        let window = ledger.turn.as_ref().unwrap().window;
        assert_eq!(window.observed_at, SimTime::from_nanos(5_000_000));
        assert_eq!(window.deadline, SimTime::from_nanos(55_000_000));
        assert_eq!(window.absolute, epoch + Duration::from_millis(55));
        assert_eq!(
            ledger.count, 0,
            "no partial normal publication after interruption"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn bound_ack_page_unwind_retains_actual_frame_bytes_and_refuses_reentry() {
    let (_index, leg, mut controller, flow_id, epoch, clock) =
        bound_terminal_controller(Duration::from_millis(50), false);
    tokio::time::advance(Duration::from_millis(5)).await;
    let first = controller
        .apply_actor_event(arrived(
            peer_event(
                leg,
                Record::Data {
                    flow_id,
                    direction: Direction::ClientToTarget,
                    offset: ByteOffset::new(0),
                    payload: Bytes::from_static(b"hello"),
                },
            ),
            &clock,
        ))
        .unwrap();
    assert!(first.needs_continue());
    assert!(first.outputs().is_empty());
    assert_eq!(
        controller
            .target_mut_for_test()
            .take_written(flow_id, 5)
            .unwrap(),
        Bytes::from_static(b"hello")
    );
    let OwnerTerminalGraceState::Bound(ledger) = &mut controller.terminal_grace else {
        panic!("the original checked Controller must own the return journal");
    };
    ledger.panic_before_returned_page_progress = true;
    tokio::time::advance(Duration::from_millis(25)).await;
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            controller.continue_effects_result_for_test()
        }))
        .is_err()
    );
    let page = controller.aborted_turn.as_ref().unwrap();
    let [OwnerTargetOutput::Transmit(frame)] = page.outputs.as_slice() else {
        panic!("the original one-step continuation must produce exactly its real ACK");
    };
    assert_eq!(frame.leg_generation(), leg.generation());
    assert!(matches!(frame.record(), Record::Ack {
        flow_id: actual, direction: Direction::ClientToTarget,
        next_accepted, final_accepted: false,
    } if *actual == flow_id && *next_accepted == ByteOffset::new(5)));
    let frame_pointer = frame as *const Frame;
    let page_pointer = page.outputs.as_ptr();
    let encoded = frame.encode().unwrap();
    let frame_bytes = frame.encoded_len_checked().unwrap();
    assert!(frame_bytes > 0);
    assert_eq!(encoded.len(), frame_bytes);
    assert_eq!(page.observations.fact_count(), 0);
    assert_eq!(page.retained_continuation_count(), 0);
    assert_eq!(
        page.page_origin,
        OwnerContinuationOrigin::Actor(SimTime::from_nanos(5_000_000))
    );
    for _ in 0..2 {
        let snapshot = controller.snapshot();
        assert!(snapshot.failed);
        assert_eq!(snapshot.retained_outputs, 1);
        assert_eq!(snapshot.retained_observations, 0);
        assert_eq!(snapshot.retained_continuations, 0);
        let inventory = controller.cold_inventory().unwrap();
        assert!(!inventory.is_exact_zero());
        assert_eq!(inventory.retained_outputs(), 1);
        assert_eq!(inventory.independently_summed_frame_bytes(), frame_bytes);
        assert_eq!(inventory.stored_frame_bytes(), frame_bytes);
        assert!(controller.continue_effects_result_for_test().is_err());
        assert!(controller.try_read_target(flow_id).is_err());
        let retained = controller.aborted_turn.as_ref().unwrap();
        assert_eq!(retained.outputs.as_ptr(), page_pointer);
        let [OwnerTargetOutput::Transmit(frame)] = retained.outputs.as_slice() else {
            panic!("reentry must not take or replace the original ACK page");
        };
        assert_eq!(frame as *const Frame, frame_pointer);
        assert_eq!(frame.encode().unwrap(), encoded);
    }
    let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
        panic!("the same original window remains bound");
    };
    let window = ledger.turn.as_ref().unwrap().window;
    assert_eq!(window.observed_at, SimTime::from_nanos(30_000_000));
    assert_eq!(window.deadline, SimTime::from_nanos(80_000_000));
    assert_eq!(window.absolute, epoch + Duration::from_millis(80));
    assert_eq!(ledger.count, 0, "ACK is not a second terminal birth");
}

#[tokio::test(start_paused = true)]
async fn bound_terminal_birth_precedes_its_deferred_lifecycle_output() {
    let (_index, leg, mut controller, flow_id, _epoch, clock) =
        bound_terminal_controller(Duration::from_secs(1), false);
    tokio::time::advance(Duration::from_millis(5)).await;
    let producing_at = clock.now().unwrap();
    let first = controller
        .apply_actor_event(arrived(peer_reset(leg, flow_id), &clock))
        .expect("checked C admits the real peer terminal before executing its output tail");
    assert!(first.needs_continue());
    assert!(first.outputs().is_empty());
    assert!(
        controller
            .reverse_progress(flow_id, Direction::TargetToClient)
            .unwrap()
            .terminal(),
        "the original terminal fact is consumed before FlowFinished output"
    );

    tokio::time::advance(Duration::from_millis(25)).await;
    assert!(clock.now().unwrap() > producing_at);
    let mut terminal = None;
    for _ in 0..8 {
        let next = controller
            .continue_effects_result_for_test()
            .expect("the original retained continuation finishes without a new input");
        let pending = next.needs_continue();
        for output in next.into_outputs() {
            match output {
                OwnerTargetOutput::TerminalGraceStarted { terminal: actual } => {
                    assert!(
                        terminal.replace(actual).is_none(),
                        "one original terminal action"
                    );
                }
                _ => panic!("peer RESET tail must not synthesize a protocol response"),
            }
        }
        if !pending {
            break;
        }
    }
    let terminal = terminal.expect("the delayed page publishes its original lifecycle action");
    assert_eq!(terminal.session_id(), leg.session_id());
    assert_eq!(terminal.flow_id(), flow_id);
    assert!(!controller.snapshot().failed);
}

#[tokio::test(start_paused = true)]
async fn original_terminal_fact_pairs_with_its_exact_delayed_output() {
    let (_plan, leg, mut executor, flow_id) = opened_terminal_executor(false);
    let observed = executor
        .apply_event_observed(peer_reset(leg, flow_id))
        .unwrap();
    let (mut outputs, observations) = observed.into_progress_parts();
    let fact = observations
        .flow_terminal()
        .expect("the original reducer terminal fact");
    let Some(OwnerTargetOutput::NeedsResume { resume }) = outputs.pop() else {
        panic!("the original terminal output must be deferred by the real one-step budget");
    };
    assert!(outputs.is_empty());
    tokio::time::advance(Duration::from_millis(25)).await;
    let mut next_resume = Some(resume);
    let mut terminal = None;
    for _ in 0..8 {
        let Some(resume) = next_resume.take() else {
            break;
        };
        let (outputs, next_observations) = executor
            .resume_pending_effects_observed(resume)
            .unwrap()
            .into_progress_parts();
        assert!(
            next_observations.flow_terminal().is_none(),
            "the original fact is not minted twice"
        );
        for output in outputs {
            match output {
                OwnerTargetOutput::NeedsResume { resume } => {
                    assert!(next_resume.replace(resume).is_none());
                }
                OwnerTargetOutput::TerminalGraceStarted { terminal: actual } => {
                    assert!(terminal.replace(actual).is_none());
                }
                _ => panic!("a peer RESET tail cannot invent a protocol response"),
            }
        }
    }
    assert!(
        next_resume.is_none(),
        "the original bounded continuation completes"
    );
    let terminal = terminal.expect("the original delayed lifecycle action");
    assert_eq!(
        fact.terminal(),
        &terminal,
        "full private LocalFlow/tombstone identity, not scalar reconstruction"
    );
    assert_eq!(fact.session_id(), terminal.session_id());
    assert_eq!(fact.flow_id(), terminal.flow_id());
    assert_eq!(fact.direction(), terminal.direction());
    assert_eq!(fact.generation(), leg.generation());
}

#[tokio::test(start_paused = true)]
async fn bound_terminal_keeps_its_original_producing_window_across_deferred_output() {
    let (_index, leg, mut controller, flow_id, epoch, clock) =
        bound_terminal_controller(Duration::from_millis(50), false);
    tokio::time::advance(Duration::from_millis(5)).await;
    let producing_at = clock.now().unwrap();
    let first = controller
        .apply_actor_event(arrived(peer_reset(leg, flow_id), &clock))
        .expect("checked C admits the real peer terminal before executing its output tail");
    assert!(first.needs_continue());
    assert!(first.outputs().is_empty());
    assert!(
        controller
            .reverse_progress(flow_id, Direction::TargetToClient)
            .unwrap()
            .terminal(),
        "the original terminal fact is consumed before FlowFinished output"
    );

    let original_window = |controller: &OwnerSessionController<MemoryTarget>| {
        let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
            panic!("checked construction cannot be untimed");
        };
        let mut occupied = ledger.slots.iter().filter_map(|slot| match &slot.state {
            TerminalSlotState::AwaitingOutput { fact, window } => Some((fact, window)),
            TerminalSlotState::Vacant => None,
        });
        let (fact, window) = occupied
            .next()
            .expect("C retains the original terminal and window");
        assert!(
            occupied.next().is_none(),
            "one original birth, not one entry per output page"
        );
        assert_eq!(window.observed_at, producing_at);
        assert_eq!(window.deadline, SimTime::from_nanos(55_000_000));
        assert_eq!(window.absolute, epoch + Duration::from_millis(55));
        *fact.terminal()
    };
    let original_terminal = original_window(&controller);
    assert_eq!(controller.snapshot().retained_observations, 1);
    tokio::time::advance(Duration::from_millis(25)).await;
    assert!(clock.now().unwrap() > producing_at);
    let mut terminal = None;
    for _ in 0..8 {
        let next = controller
            .continue_effects_result_for_test()
            .expect("the original retained continuation finishes without a new input");
        let pending = next.needs_continue();
        for output in next.into_outputs() {
            match output {
                OwnerTargetOutput::TerminalGraceStarted { terminal: actual } => {
                    assert!(
                        terminal.replace(actual).is_none(),
                        "one original terminal action"
                    );
                }
                _ => panic!("peer RESET tail must not synthesize a protocol response"),
            }
        }
        if !pending {
            break;
        }
    }
    let terminal = terminal.expect("the delayed page publishes its original lifecycle action");
    assert_eq!(terminal.session_id(), leg.session_id());
    assert_eq!(terminal.flow_id(), flow_id);
    assert_eq!(original_window(&controller), original_terminal);
    assert_eq!(terminal, original_terminal);
    assert_eq!(controller.snapshot().retained_observations, 1);
    assert!(!controller.snapshot().failed);
    assert!(matches!(
        controller.continue_effects_for_test(),
        OwnerSessionContinuationOutcome::Missing(_)
    ));
    assert!(
        !controller.snapshot().failed,
        "a missing continuation never enters a producing turn"
    );
    assert_eq!(original_window(&controller), original_terminal);
}

#[tokio::test(start_paused = true)]
async fn checked_terminal_policy_returns_original_inputs_before_ready_binding() {
    for (interval, expected) in [
        (Duration::ZERO, SessionClockError::ZeroInterval),
        (
            Duration::from_nanos(u64::MAX) + Duration::from_nanos(1),
            SessionClockError::Overflow,
        ),
        (Duration::from_nanos(u64::MAX), SessionClockError::Overflow),
    ] {
        let (plan, leg, executor) = terminal_executor(false);
        let geometry = ReadyGeometry::checked(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
        let mut index = ReadyIndex::cold(&geometry).unwrap();
        let limits =
            OwnerReadySessionLimits::checked(plan.session_config().max_flows(), 1, 8, 8).unwrap();
        let ready = index.reserve_session(leg.session_id(), limits).unwrap();
        let clock = SessionMonotonicClock::start();
        let identity = clock.clone();
        tokio::time::advance(Duration::from_nanos(1)).await;
        let failure =
            match OwnerSessionController::new_checked(executor, clock, plan, ready, interval) {
                Ok(_) => panic!("invalid grace cannot bind the original ready reservation"),
                Err(failure) => failure,
            };
        let (clock, executor, returned_plan, ready, returned_interval, error) =
            failure.into_parts();
        assert!(clock.same_source(&identity));
        assert_eq!(returned_plan, plan);
        assert_eq!(returned_interval, interval);
        assert!(
            matches!(error, OwnerSessionControllerBuildError::TerminalGrace(
            TerminalGraceError::Clock(actual)) if actual == expected)
        );
        let controller = OwnerSessionController::new_checked(
            executor,
            clock,
            returned_plan,
            ready,
            Duration::from_millis(50),
        )
        .expect("the same returned Executor and ready reservation bind exactly once");
        let OwnerTerminalGraceState::Bound(ledger) = &controller.terminal_grace else {
            panic!("checked binding must own timed capacity");
        };
        assert_eq!(ledger.interval, Duration::from_millis(50));
        assert_eq!(ledger.slots.len(), 8);
        assert_eq!(ledger.slots.capacity(), 8);
        assert_eq!(ledger.count, 0);
        assert_eq!(controller.snapshot().retained_observations, 0);
        assert!(!controller.snapshot().failed);
    }
}

use std::time::Duration;

use super::*;
use crate::owned_upstream::leg_io::{
    LegControlEnqueued, LegControlQueueErrorKind, LegOutboundQueue,
};
use crate::owned_upstream::standby::{OwnerStandbyHintEnqueueErrorKind, OwnerStandbySlot};
use crate::resumable::{LegEpochNonce, ProbeSequence};

fn session(byte: u8) -> SessionId {
    SessionId::new([byte; 16]).expect("nonzero session")
}

fn generation(value: u64) -> LegGeneration {
    LegGeneration::new(value).expect("nonzero generation")
}

fn flow(value: u64) -> SessionFlowId {
    SessionFlowId::new(value).expect("nonzero flow")
}

fn standby_nonce(byte: u8) -> StandbyNonce {
    StandbyNonce::new([byte; 16]).expect("nonzero standby nonce")
}

fn attach_nonce(byte: u8) -> AttachNonce {
    AttachNonce::new([byte; 16]).expect("nonzero attach nonce")
}

fn registered_standby(
    session_id: SessionId,
    current_generation: LegGeneration,
    nonce: StandbyNonce,
) -> (RegisteredStandby, StandbyActorSeal) {
    let seal = StandbyActorSeal::mint();
    let mut standby = RegisteredStandby::new(session_id, current_generation, nonce, seal.clone());
    standby
        .observe_probe_ack(
            &seal,
            session_id,
            current_generation,
            nonce,
            SimTime::from_nanos(10),
            Duration::from_nanos(100),
        )
        .expect("matching B probe ACK is accepted");
    (standby, seal)
}

fn switch_hint(
    session_id: SessionId,
    current_generation: LegGeneration,
    nonce: StandbyNonce,
    sequence: u64,
    flow_id: SessionFlowId,
    direction: Direction,
    oldest: u64,
) -> LegControlFrame {
    LegControlFrame::try_new(
        current_generation,
        LegControlRecord::SwitchHint {
            session_id,
            standby_nonce: nonce,
            hint_sequence: HintSequence::new(sequence).expect("nonzero hint sequence"),
            flow_id,
            direction,
            oldest_unacknowledged: ByteOffset::new(oldest),
            cause: SwitchHintCause::ReverseApplicationAckStall,
        },
        FeatureSet::STANDBY_CONTROL_V1,
    )
    .expect("valid switch hint")
}

// Ambiguous inference fails to compile if an exact once-consumed capability
// ever becomes cloneable again.
trait AmbiguousIfClone<Marker> {
    fn marker() {}
}
impl<T: ?Sized> AmbiguousIfClone<()> for T {}
impl<T: Clone> AmbiguousIfClone<u8> for T {}
const _: fn() = || {
    let _ = <PendingOwnerSwitchHint as AmbiguousIfClone<_>>::marker;
    let _ = <StandbyHintArrival as AmbiguousIfClone<_>>::marker;
    let _ = <AuthenticatedSwitchHint as AmbiguousIfClone<_>>::marker;
    let _ = <HardFailureTrigger as AmbiguousIfClone<_>>::marker;
    let _ = <LocalAckStallTrigger as AmbiguousIfClone<_>>::marker;
    let _ = <AdmittedOwnerSwitchHint<()> as AmbiguousIfClone<_>>::marker;
    let _ = <PendingPhysicalAttachPermit as AmbiguousIfClone<_>>::marker;
    let _ = <PhysicalPendingAttachSettlement as AmbiguousIfClone<_>>::marker;
    let _ = <SwitchTriggerOutcome as AmbiguousIfClone<_>>::marker;
    let _ = <CorrelatedAttachAccepted as AmbiguousIfClone<_>>::marker;
    let _ =
        <crate::owned_upstream::standby::OwnerStandbyHintBinding as AmbiguousIfClone<_>>::marker;
    let _ = <OwnerSwitchHintBuildFailure as AmbiguousIfClone<_>>::marker;
    let _ = <ReadySwitchSeed as AmbiguousIfClone<_>>::marker;
};

type PhysicalHintAdmissionFn =
    fn(
        &mut OwnerSwitchHintIssuer,
        &OwnerReverseProgress,
        SimTime,
        &OwnerStandbySlot,
        &mut LegOutboundQueue,
    ) -> Result<AdmittedOwnerSwitchHint<LegControlEnqueued>, OwnerHintAdmissionError>;

#[test]
fn production_capability_and_admission_boundaries_are_unforgeable() {
    let _: fn(
        &crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate,
    ) -> StandbyActorSeal = StandbyActorSeal::mint_for_exact_b;
    let _: PhysicalHintAdmissionFn = OwnerSwitchHintIssuer::try_admit_pending;
    let _: fn(
        crate::owned_upstream::standby::OwnerStandbyHintBinding,
        Duration,
        HintSequence,
    ) -> Result<OwnerSwitchHintIssuer, OwnerSwitchHintBuildFailure> =
        OwnerSwitchHintIssuer::from_exact_binding;

    let production = include_str!("../../reverse_switch.rs");
    assert!(production.contains("#[cfg(test)]\n    pub(crate) fn mint() -> Self"));
    assert!(
        production.contains(
            "#[cfg(test)]\n    pub(crate) fn new(\n        actor_seal: StandbyActorSeal,"
        )
    );
    assert!(production.contains("pub(super) fn mint_for_exact_b(_gate: &QuinnLegCompositionGate)"));
    assert!(!production.contains("fn from_held_b_hint("));
    assert!(!production.contains("fn from_actor_terminal("));
    assert!(production.contains("impl HardFailureTrigger {\n    #[cfg(test)]\n    const fn new("));
    assert!(production.contains("#[derive(Debug)]\npub(crate) struct StandbyHintArrival"));
    assert!(!production.contains("#[derive(Clone, Debug)]\npub(crate) struct StandbyHintArrival"));
    assert!(production.contains("#[derive(Debug)]\npub(crate) struct CorrelatedAttachAccepted"));
    assert!(!production.contains("CorrelatedAttachAccepted::new"));
    assert!(!production.contains("pub(crate) const fn new(\n        session_id: SessionId,\n        generation: LegGeneration,\n        attach_nonce: AttachNonce,"));
    assert!(!production.contains("impl FnOnce(LegControlFrame)"));
    assert!(!production.contains("Err((frame, error))"));
    assert_eq!(production.matches(".enqueue_exact_switch_hint(").count(), 1);
}

#[test]
fn unsupported_feature_build_failure_returns_the_exact_claimed_binding() {
    let session_id = session(0x10);
    let current_generation = generation(9);
    let nonce = standby_nonce(0x20);
    let binding = crate::owned_upstream::standby::OwnerStandbyHintBinding::for_policy_test(
        session_id,
        current_generation,
        nonce,
        FeatureSet::new(0),
    );
    let failure = match OwnerSwitchHintIssuer::from_exact_binding(
        binding,
        Duration::from_nanos(20),
        HintSequence::new(1).expect("nonzero sequence"),
    ) {
        Ok(_) => panic!("unsupported negotiation cannot build an issuer"),
        Err(failure) => failure,
    };
    assert_eq!(failure.kind(), OwnerHintError::FeatureNotNegotiated);
    assert_eq!(failure.binding().contract().session_id(), session_id);
    assert_eq!(
        failure.binding().contract().generation(),
        current_generation
    );
    assert_eq!(failure.binding().contract().standby_nonce(), nonce);

    let failure = match OwnerSwitchHintIssuer::from_exact_binding(
        failure.into_binding(),
        Duration::from_nanos(20),
        HintSequence::new(1).expect("nonzero sequence"),
    ) {
        Ok(_) => panic!("the returned unsupported binding cannot build an issuer"),
        Err(failure) => failure,
    };
    assert_eq!(failure.kind(), OwnerHintError::FeatureNotNegotiated);
    drop(failure.into_binding());
}

#[test]
fn test_only_capability_constructors_preserve_exact_physical_evidence() {
    let session_id = session(0x10);
    let current_generation = generation(9);
    let nonce = standby_nonce(0x20);
    let exact_b = StandbyActorSeal::mint();
    let another_b = StandbyActorSeal::mint();
    assert!(exact_b.same_actor(&exact_b));
    assert!(!exact_b.same_actor(&another_b));

    let frame = switch_hint(
        session_id,
        current_generation,
        nonce,
        1,
        flow(7),
        Direction::TargetToClient,
        11,
    );
    let arrival = StandbyHintArrival::new(exact_b.clone(), frame.clone(), SimTime::from_nanos(31));
    assert!(arrival.actor_seal.same_actor(&exact_b));
    assert_eq!(arrival.frame, frame);
    assert_eq!(arrival.arrived_at, SimTime::from_nanos(31));

    let terminal = HardFailureTrigger::new(session_id, current_generation, SimTime::from_nanos(41));
    assert_eq!(terminal.session_id, session_id);
    assert_eq!(terminal.generation, current_generation);
    assert_eq!(terminal.failed_at, SimTime::from_nanos(41));
}

#[test]
fn owner_refuses_idle_no_replay_wrong_oldest_and_non_reverse_requests() {
    let session_id = session(0x11);
    let current_generation = generation(1);
    let nonce = standby_nonce(0x21);
    let flow_id = flow(7);
    let (standby, _) = registered_standby(session_id, current_generation, nonce);
    let mut issuer = OwnerSwitchHintIssuer::new(
        session_id,
        current_generation,
        FeatureSet::STANDBY_CONTROL_V1,
        standby,
        Duration::from_nanos(20),
        HintSequence::new(1).expect("nonzero sequence"),
    )
    .expect("issuer");
    let mut progress = OwnerReverseProgress::new(2).expect("bounded progress");

    assert_eq!(
        issuer.prepare_hint(
            &progress,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(0),
            SimTime::from_nanos(40),
        ),
        Err(OwnerHintError::Progress(
            OwnerProgressError::UnknownFlowDirection
        ))
    );

    progress
        .register_live(current_generation, flow_id, Direction::TargetToClient)
        .expect("live reverse flow");
    assert_eq!(
        issuer.prepare_hint(
            &progress,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(0),
            SimTime::from_nanos(40),
        ),
        Err(OwnerHintError::NoOutstandingReplay)
    );

    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(10),
        )
        .expect("exact reducer replay");
    assert_eq!(
        issuer.prepare_hint(
            &progress,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(9),
            SimTime::from_nanos(40),
        ),
        Err(OwnerHintError::WrongOldest {
            expected: ByteOffset::new(8),
            claimed: ByteOffset::new(9),
        })
    );
    assert_eq!(
        issuer.prepare_hint(
            &progress,
            flow_id,
            Direction::ClientToTarget,
            ByteOffset::new(8),
            SimTime::from_nanos(40),
        ),
        Err(OwnerHintError::WrongDirection)
    );
    progress
        .mark_terminal(current_generation, flow_id, Direction::TargetToClient)
        .expect("terminal observation");
    assert_eq!(
        issuer.prepare_hint(
            &progress,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(40),
        ),
        Err(OwnerHintError::TerminalFlowDirection)
    );
    assert!(issuer.pending_hint().is_none());
}

#[test]
fn owner_mints_exact_hint_only_when_due_and_pressure_reuses_sequence() {
    let session_id = session(0x12);
    let current_generation = generation(4);
    let nonce = standby_nonce(0x22);
    let flow_id = flow(9);
    let (standby, _) = registered_standby(session_id, current_generation, nonce);
    let mut issuer = OwnerSwitchHintIssuer::new(
        session_id,
        current_generation,
        FeatureSet::STANDBY_CONTROL_V1,
        standby,
        Duration::from_nanos(20),
        HintSequence::new(5).expect("nonzero sequence"),
    )
    .expect("issuer");
    let mut progress = OwnerReverseProgress::new(1).expect("bounded progress");
    progress
        .register_live(current_generation, flow_id, Direction::TargetToClient)
        .expect("live reverse flow");
    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(10),
        )
        .expect("replay outstanding");

    assert_eq!(
        issuer.prepare_hint(
            &progress,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(29),
        ),
        Err(OwnerHintError::AckStallNotDue)
    );
    let prepared = issuer
        .prepare_hint(
            &progress,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(30),
        )
        .expect("deadline is inclusive");
    assert_eq!(
        prepared,
        OwnerHintPreparation::Prepared {
            hint_sequence: HintSequence::new(5).unwrap(),
        }
    );
    let first = issuer.pending_hint().expect("one exact pending hint");
    let first_frame = first.frame().clone();
    assert_eq!(first.hint_sequence().get(), 5);
    assert_eq!(first.frame().leg_generation(), current_generation);
    assert_eq!(
        first.frame().record(),
        &LegControlRecord::SwitchHint {
            session_id,
            standby_nonce: nonce,
            hint_sequence: HintSequence::new(5).expect("nonzero sequence"),
            flow_id,
            direction: Direction::TargetToClient,
            oldest_unacknowledged: ByteOffset::new(8),
            cause: SwitchHintCause::ReverseApplicationAckStall,
        }
    );

    let retry = issuer
        .prepare_hint(
            &progress,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(31),
        )
        .expect("bounded queue pressure retries exact pending hint");
    assert_eq!(
        retry,
        OwnerHintPreparation::Retry {
            hint_sequence: HintSequence::new(5).unwrap(),
        }
    );
    let pressure =
        OwnerStandbyHintEnqueueErrorKind::Queue(LegControlQueueErrorKind::CapacityExceeded {
            queued_frames: 1,
            queued_bytes: 512,
            incoming_bytes: 128,
        });
    assert_eq!(
        issuer.try_admit_pending_for_test(
            &progress,
            SimTime::from_nanos(31),
            TestOwnerHintAdmission::<u8>::Reject(pressure.clone()),
        ),
        Err(OwnerHintAdmissionError::QueueRejected(pressure))
    );
    assert_eq!(
        issuer
            .pending_hint()
            .expect("pressure restores the exact pending capability")
            .frame(),
        &first_frame
    );
    let admitted = issuer
        .try_admit_pending_for_test(
            &progress,
            SimTime::from_nanos(32),
            TestOwnerHintAdmission::Admit(7_u8),
        )
        .expect("exact queue admission");
    assert_eq!(admitted.hint_sequence().get(), 5);
    assert_eq!(admitted.into_receipt(), 7);
    assert_eq!(
        issuer.last_published_sequence().map(HintSequence::get),
        Some(5)
    );
    assert_eq!(
        issuer.prepare_hint(
            &progress,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(32),
        ),
        Err(OwnerHintError::HintAlreadyAdmitted)
    );

    progress
        .observe_positive_ack(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(12),
            SimTime::from_nanos(40),
        )
        .expect("positive reducer ACK advances");
    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(12),
            SimTime::from_nanos(40),
        )
        .expect("reducer publishes new exact oldest");
    assert_eq!(
        issuer.prepare_hint(
            &progress,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(12),
            SimTime::from_nanos(60),
        ),
        Err(OwnerHintError::HintAlreadyAdmitted)
    );
}

#[test]
fn owner_revalidates_pending_hint_before_queue_admission() {
    let session_id = session(0x16);
    let current_generation = generation(5);
    let nonce = standby_nonce(0x26);
    let flow_id = flow(6);
    let (standby, _) = registered_standby(session_id, current_generation, nonce);
    let mut issuer = OwnerSwitchHintIssuer::new(
        session_id,
        current_generation,
        FeatureSet::STANDBY_CONTROL_V1,
        standby,
        Duration::from_nanos(20),
        HintSequence::new(6).unwrap(),
    )
    .expect("issuer");
    let mut progress = OwnerReverseProgress::new(1).expect("bounded progress");
    progress
        .register_live(current_generation, flow_id, Direction::TargetToClient)
        .expect("live reverse flow");
    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(10),
        )
        .expect("outstanding episode");
    issuer
        .prepare_next_due_hint(&progress, SimTime::from_nanos(30))
        .expect("hint becomes pending at its inclusive deadline");
    progress
        .observe_positive_ack(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(9),
            SimTime::from_nanos(31),
        )
        .expect("positive ACK refreshes the exact episode");

    let physical_slot = OwnerStandbySlot::new();
    let mut physical_queue = LegOutboundQueue::new(4, 4_096).expect("bounded physical queue");
    assert!(matches!(
        issuer.try_admit_pending(
            &progress,
            SimTime::from_nanos(32),
            &physical_slot,
            &mut physical_queue,
        ),
        Err(OwnerHintAdmissionError::PolicyInvalidated(
            OwnerHintError::AckStallNotDue
        ))
    ));
    assert_eq!(physical_queue.len(), 0);
    assert!(issuer.pending_hint().is_none());

    issuer
        .prepare_next_due_hint(&progress, SimTime::from_nanos(51))
        .expect("refreshed ACK baseline eventually stalls");
    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(9),
            SimTime::from_nanos(52),
        )
        .expect("the reducer advances the current replay frontier");
    assert!(matches!(
        issuer.try_admit_pending(
            &progress,
            SimTime::from_nanos(52),
            &physical_slot,
            &mut physical_queue,
        ),
        Err(OwnerHintAdmissionError::PolicyInvalidated(
            OwnerHintError::PendingHintNoLongerCurrent
        ))
    ));
    assert_eq!(physical_queue.len(), 0);
    assert!(issuer.pending_hint().is_none());

    issuer
        .prepare_next_due_hint(&progress, SimTime::from_nanos(52))
        .expect("the issuer prepares the current exact replay frontier");
    assert!(matches!(
        issuer.try_admit_pending(
            &progress,
            SimTime::from_nanos(111),
            &physical_slot,
            &mut physical_queue,
        ),
        Err(OwnerHintAdmissionError::PolicyInvalidated(
            OwnerHintError::PhysicalStandby(OwnerStandbyHintEvidenceError::NoRegisteredStandby)
        ))
    ));
    assert_eq!(physical_queue.len(), 0);
    assert!(issuer.pending_hint().is_none());
}

#[test]
fn owner_requires_exact_physical_probe_freshness_before_fifo_admission() {
    let session_id = session(0x17);
    let current_generation = generation(6);
    let nonce = standby_nonce(0x27);
    let flow_id = flow(17);
    let (standby, _) = registered_standby(session_id, current_generation, nonce);
    let mut issuer = OwnerSwitchHintIssuer::new(
        session_id,
        current_generation,
        FeatureSet::STANDBY_CONTROL_V1,
        standby,
        Duration::from_nanos(20),
        HintSequence::new(7).unwrap(),
    )
    .expect("issuer");
    let mut progress = OwnerReverseProgress::new(1).expect("bounded progress");
    progress
        .register_live(current_generation, flow_id, Direction::TargetToClient)
        .expect("live reverse flow");
    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(3),
            SimTime::from_nanos(10),
        )
        .expect("outstanding episode");
    issuer
        .prepare_next_due_hint(&progress, SimTime::from_nanos(30))
        .expect("value policy is due and fresh");

    let physical_slot = OwnerStandbySlot::new();
    let mut physical_queue = LegOutboundQueue::new(4, 4_096).expect("bounded physical queue");
    assert_eq!(
        issuer.prepare_next_due_hint_for_exact_slot(
            &progress,
            &physical_slot,
            SimTime::from_nanos(30),
        ),
        Err(OwnerHintError::PhysicalStandby(
            OwnerStandbyHintEvidenceError::NoRegisteredStandby,
        )),
        "an empty physical slot terminally settles an already prepared frame",
    );
    assert!(issuer.pending_hint().is_none());
    assert_eq!(issuer.last_published_sequence(), None);
    assert_eq!(
        issuer
            .prepare_next_due_hint(&progress, SimTime::from_nanos(30))
            .expect("terminal prepare rejection does not consume the sequence"),
        OwnerHintPreparation::Prepared {
            hint_sequence: HintSequence::new(7).unwrap(),
        },
    );
    assert!(matches!(
        issuer.try_admit_pending(
            &progress,
            SimTime::from_nanos(30),
            &physical_slot,
            &mut physical_queue,
        ),
        Err(OwnerHintAdmissionError::PolicyInvalidated(
            OwnerHintError::PhysicalStandby(OwnerStandbyHintEvidenceError::NoRegisteredStandby)
        ))
    ));
    assert_eq!(physical_queue.len(), 0);
    assert!(issuer.pending_hint().is_none());
    assert_eq!(issuer.last_published_sequence(), None);
}

#[test]
fn owner_prepare_settles_terminal_physical_evidence_but_retains_wrong_binding() {
    let make_pending = || {
        let session_id = session(0x18);
        let current_generation = generation(6);
        let flow_id = flow(18);
        let (standby, _) = registered_standby(session_id, current_generation, standby_nonce(0x28));
        let mut issuer = OwnerSwitchHintIssuer::new(
            session_id,
            current_generation,
            FeatureSet::STANDBY_CONTROL_V1,
            standby,
            Duration::from_nanos(20),
            HintSequence::new(7).unwrap(),
        )
        .expect("issuer");
        let mut progress = OwnerReverseProgress::new(1).expect("bounded progress");
        progress
            .register_live(current_generation, flow_id, Direction::TargetToClient)
            .expect("live reverse flow");
        progress
            .observe_outstanding(
                current_generation,
                flow_id,
                Direction::TargetToClient,
                ByteOffset::new(3),
                SimTime::from_nanos(10),
            )
            .expect("outstanding episode");
        issuer
            .prepare_next_due_hint(&progress, SimTime::from_nanos(30))
            .expect("one exact frame becomes pending");
        (issuer, progress)
    };

    for terminal in [
        OwnerStandbyHintEvidenceError::NoRegisteredStandby,
        OwnerStandbyHintEvidenceError::ProbeNotFresh,
        OwnerStandbyHintEvidenceError::EndpointOrQueueLost,
    ] {
        let (mut issuer, progress) = make_pending();
        assert_eq!(
            issuer.settle_prepare_validation_failure(terminal),
            OwnerHintError::PhysicalStandby(terminal),
        );
        assert!(issuer.pending_hint().is_none());
        assert_eq!(issuer.last_published_sequence(), None);
        assert_eq!(
            issuer
                .prepare_next_due_hint(&progress, SimTime::from_nanos(30))
                .expect("terminal physical evidence does not advance the sequence"),
            OwnerHintPreparation::Prepared {
                hint_sequence: HintSequence::new(7).unwrap(),
            },
        );
    }

    let (mut issuer, progress) = make_pending();
    let exact_pending = issuer
        .pending_hint()
        .expect("one exact pending frame")
        .frame()
        .clone();
    assert_eq!(
        issuer.settle_prepare_validation_failure(
            OwnerStandbyHintEvidenceError::WrongRegistrationBinding,
        ),
        OwnerHintError::PhysicalStandby(OwnerStandbyHintEvidenceError::WrongRegistrationBinding,),
    );
    assert_eq!(
        issuer
            .pending_hint()
            .expect("wrong physical B preserves the original pending frame")
            .frame(),
        &exact_pending,
    );
    assert_eq!(
        issuer
            .prepare_next_due_hint(&progress, SimTime::from_nanos(30))
            .expect("the original physical B can retry the same frame"),
        OwnerHintPreparation::Retry {
            hint_sequence: HintSequence::new(7).unwrap(),
        },
    );
    assert_eq!(issuer.last_published_sequence(), None);
}

#[test]
fn owner_selects_the_earliest_due_reverse_stall_without_caller_flow_or_offset() {
    let session_id = session(0x13);
    let current_generation = generation(4);
    let nonce = standby_nonce(0x23);
    let first_flow = flow(1);
    let earlier_flow = flow(2);
    let (standby, _) = registered_standby(session_id, current_generation, nonce);
    let mut issuer = OwnerSwitchHintIssuer::new(
        session_id,
        current_generation,
        FeatureSet::STANDBY_CONTROL_V1,
        standby,
        Duration::from_nanos(20),
        HintSequence::new(9).expect("nonzero sequence"),
    )
    .expect("issuer");
    let mut progress = OwnerReverseProgress::new(2).expect("bounded progress");
    for flow_id in [first_flow, earlier_flow] {
        progress
            .register_live(current_generation, flow_id, Direction::TargetToClient)
            .expect("live reverse flow");
    }
    progress
        .observe_outstanding(
            current_generation,
            first_flow,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(10),
        )
        .expect("first replay");
    progress
        .observe_outstanding(
            current_generation,
            earlier_flow,
            Direction::TargetToClient,
            ByteOffset::new(20),
            SimTime::from_nanos(5),
        )
        .expect("earlier replay");

    assert_eq!(
        issuer
            .prepare_next_due_hint(&progress, SimTime::from_nanos(24))
            .expect_err("earliest deadline is not yet due"),
        OwnerHintError::AckStallNotDue,
    );
    assert_eq!(
        issuer
            .prepare_next_due_hint(&progress, SimTime::from_nanos(25))
            .expect("the internally selected earliest stall is due"),
        OwnerHintPreparation::Prepared {
            hint_sequence: HintSequence::new(9).unwrap(),
        },
    );
    let pending = issuer
        .pending_hint()
        .expect("issuer retains one exact hint");
    assert!(matches!(
        pending.frame().record(),
        LegControlRecord::SwitchHint {
            flow_id,
            oldest_unacknowledged,
            ..
        } if *flow_id == earlier_flow && *oldest_unacknowledged == ByteOffset::new(20)
    ));

    progress
        .observe_positive_ack(
            current_generation,
            earlier_flow,
            Direction::TargetToClient,
            ByteOffset::new(21),
            SimTime::from_nanos(26),
        )
        .expect("positive ACK invalidates the pressured hint episode");
    assert_eq!(
        issuer
            .prepare_next_due_hint(&progress, SimTime::from_nanos(30))
            .expect("the next exact due direction replaces stale unadmitted work"),
        OwnerHintPreparation::Replaced {
            hint_sequence: HintSequence::new(9).unwrap(),
        },
    );
    let pending = issuer
        .pending_hint()
        .expect("replacement remains uniquely owned");
    assert!(matches!(
        pending.frame().record(),
        LegControlRecord::SwitchHint { flow_id, .. } if *flow_id == first_flow
    ));
}

#[test]
fn owner_due_selection_resumes_with_fixed_work_and_waits_for_the_complete_registry() {
    let session_id = session(0x19);
    let current_generation = generation(4);
    let nonce = standby_nonce(0x29);
    let (standby, _) = registered_standby(session_id, current_generation, nonce);
    let mut issuer = OwnerSwitchHintIssuer::new(
        session_id,
        current_generation,
        FeatureSet::STANDBY_CONTROL_V1,
        standby,
        Duration::from_nanos(20),
        HintSequence::new(9).unwrap(),
    )
    .expect("issuer");
    let entry_count = OWNER_REVERSE_SCAN_BUDGET_PER_TURN * 2 + 1;
    let mut progress = OwnerReverseProgress::new(entry_count).expect("bounded progress");
    for index in 1..=entry_count {
        let flow_id = flow(index as u64);
        progress
            .register_live(current_generation, flow_id, Direction::TargetToClient)
            .unwrap();
        progress
            .observe_outstanding(
                current_generation,
                flow_id,
                Direction::TargetToClient,
                ByteOffset::new(index as u64),
                if index == entry_count {
                    SimTime::from_nanos(10)
                } else {
                    SimTime::from_nanos(90)
                },
            )
            .unwrap();
    }

    assert!(matches!(
        issuer
            .select_next_due_hint(&progress, SimTime::from_nanos(100))
            .unwrap(),
        OwnerHintDueCursor::Scanning,
    ));
    assert!(matches!(
        issuer
            .select_next_due_hint(&progress, SimTime::from_nanos(100))
            .unwrap(),
        OwnerHintDueCursor::Scanning,
    ));
    let OwnerHintDueCursor::Due(selected) = issuer
        .select_next_due_hint(&progress, SimTime::from_nanos(100))
        .unwrap()
    else {
        panic!("only a complete scan may publish the globally earliest due entry");
    };
    assert_eq!(selected.candidate.flow_id, flow(entry_count as u64));
}

#[test]
fn owner_due_scan_does_not_advance_past_a_fallible_exact_entry() {
    let current_generation = generation(4);
    let key = DirectionalFlowKey::new(flow(1), Direction::TargetToClient);
    let mut progress = OwnerReverseProgress::new(1).expect("bounded progress");
    let entry = OwnerDirectionalProgress {
        generation: current_generation,
        outstanding_episode: 1,
        oldest_unacknowledged: Some(ByteOffset::new(1)),
        outstanding_since: None,
        latest_positive_ack: None,
        terminal: false,
    };
    progress.entries.insert(key, entry).unwrap();
    let mut scan = None;

    for _ in 0..2 {
        assert!(matches!(
            progress.scan_reverse_stall(current_generation, Duration::from_nanos(20), &mut scan,),
            Err(OwnerProgressError::MissingOutstandingAdmissionTime),
        ));
        let cursor = scan.as_ref().expect("the same scan remains retryable");
        assert_eq!(cursor.last_key, None);
        assert_eq!(cursor.scanned_entries, 0);
        assert!(cursor.earliest.is_none());
    }
}

#[test]
fn owner_untracked_committed_retirement_is_a_noop_at_revision_exhaustion() {
    let mut progress = OwnerReverseProgress::new(1).expect("bounded progress");
    progress.revision = u64::MAX;

    assert_eq!(
        progress.retire_flow_after_committed_generation(generation(9), flow(1)),
        Ok(0),
    );
    assert_eq!(progress.revision(), u64::MAX);
    assert_eq!(progress.len(), 0);
}

#[test]
fn owner_reuses_sequence_but_not_fingerprint_across_a_new_outstanding_episode() {
    let session_id = session(0x14);
    let current_generation = generation(6);
    let nonce = standby_nonce(0x24);
    let flow_id = flow(4);
    let (standby, _) = registered_standby(session_id, current_generation, nonce);
    let mut issuer = OwnerSwitchHintIssuer::new(
        session_id,
        current_generation,
        FeatureSet::STANDBY_CONTROL_V1,
        standby,
        Duration::from_nanos(20),
        HintSequence::new(3).unwrap(),
    )
    .expect("issuer");
    let mut progress = OwnerReverseProgress::new(1).expect("bounded progress");
    progress
        .register_live(current_generation, flow_id, Direction::TargetToClient)
        .expect("live reverse flow");
    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(10),
        )
        .expect("first episode");
    assert!(matches!(
        issuer.prepare_next_due_hint(&progress, SimTime::from_nanos(30)),
        Ok(OwnerHintPreparation::Prepared { .. })
    ));

    progress
        .clear_outstanding(current_generation, flow_id, Direction::TargetToClient)
        .expect("first episode clears");
    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(10),
        )
        .expect("a distinct episode may have identical scalar time/offset");
    assert_eq!(
        issuer
            .prepare_next_due_hint(&progress, SimTime::from_nanos(30))
            .expect("new episode replaces stale unadmitted work"),
        OwnerHintPreparation::Replaced {
            hint_sequence: HintSequence::new(3).unwrap(),
        },
    );
}

#[test]
fn owner_discards_a_stale_stall_when_the_outstanding_episode_closes() {
    let session_id = session(0x15);
    let current_generation = generation(6);
    let nonce = standby_nonce(0x25);
    let flow_id = flow(5);
    let (standby, _) = registered_standby(session_id, current_generation, nonce);
    let mut issuer = OwnerSwitchHintIssuer::new(
        session_id,
        current_generation,
        FeatureSet::STANDBY_CONTROL_V1,
        standby,
        Duration::from_nanos(20),
        HintSequence::new(4).unwrap(),
    )
    .expect("issuer");
    let mut progress = OwnerReverseProgress::new(1).expect("bounded progress");
    progress
        .register_live(current_generation, flow_id, Direction::TargetToClient)
        .expect("live reverse flow");
    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(10),
        )
        .expect("first episode");
    assert!(matches!(
        issuer.prepare_next_due_hint(&progress, SimTime::from_nanos(30)),
        Ok(OwnerHintPreparation::Prepared { .. })
    ));

    progress
        .clear_outstanding(current_generation, flow_id, Direction::TargetToClient)
        .expect("first episode closes");
    assert_eq!(
        issuer.prepare_next_due_hint(&progress, SimTime::from_nanos(31)),
        Err(OwnerHintError::NoOutstandingReplay)
    );
    assert!(issuer.pending_hint().is_none());

    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(8),
            SimTime::from_nanos(40),
        )
        .expect("second episode has its own admission time");
    assert_eq!(
        issuer.prepare_next_due_hint(&progress, SimTime::from_nanos(59)),
        Err(OwnerHintError::AckStallNotDue)
    );
    assert!(issuer.pending_hint().is_none());
    assert_eq!(
        issuer
            .prepare_next_due_hint(&progress, SimTime::from_nanos(60))
            .expect("only the second episode's deadline can become due"),
        OwnerHintPreparation::Prepared {
            hint_sequence: HintSequence::new(4).unwrap(),
        }
    );
}

#[test]
fn owner_progress_is_checked_and_bounded_to_two_entries_per_max_flow() {
    assert!(matches!(
        OwnerReverseProgress::new(usize::MAX),
        Err(OwnerProgressError::CapacityLimitOverflow)
    ));
    let mut progress = OwnerReverseProgress::new(1).expect("two directions");
    let current_generation = generation(1);
    progress
        .register_live(current_generation, flow(1), Direction::ClientToTarget)
        .expect("first direction");
    progress
        .register_live(current_generation, flow(1), Direction::TargetToClient)
        .expect("second direction");
    assert_eq!(progress.len(), 2);
    assert_eq!(progress.max_entries(), 2);
    assert_eq!(
        progress.register_live(current_generation, flow(2), Direction::TargetToClient),
        Err(OwnerProgressError::CapacityExceeded { max_entries: 2 })
    );
    assert_eq!(
        progress.observe_positive_ack(
            current_generation,
            flow(1),
            Direction::TargetToClient,
            ByteOffset::new(1),
            SimTime::from_nanos(1),
        ),
        Err(OwnerProgressError::NoOutstandingReplay)
    );
    progress
        .observe_outstanding(
            current_generation,
            flow(1),
            Direction::TargetToClient,
            ByteOffset::new(0),
            SimTime::from_nanos(10),
        )
        .expect("outstanding reverse replay");
    progress
        .observe_positive_ack(
            current_generation,
            flow(1),
            Direction::TargetToClient,
            ByteOffset::new(1),
            SimTime::from_nanos(20),
        )
        .expect("positive application ACK");
    assert_eq!(
        progress.observe_outstanding(
            current_generation,
            flow(1),
            Direction::TargetToClient,
            ByteOffset::new(1),
            SimTime::from_nanos(19),
        ),
        Err(OwnerProgressError::Clock(SimTimeError::TimeWentBackwards))
    );
    assert_eq!(progress.retire_flow(current_generation, flow(1)), Ok(2));
    assert_eq!(progress.len(), 0);
}

#[test]
fn positive_ack_must_advance_the_current_outstanding_episode_not_only_old_history() {
    let current_generation = generation(2);
    let flow_id = flow(3);
    let mut progress = OwnerReverseProgress::new(1).expect("bounded progress");
    progress
        .register_live(current_generation, flow_id, Direction::TargetToClient)
        .expect("live reverse flow");
    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(5),
            SimTime::from_nanos(10),
        )
        .expect("first episode");
    progress
        .observe_positive_ack(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(10),
            SimTime::from_nanos(20),
        )
        .expect("first episode advances");
    progress
        .clear_outstanding(current_generation, flow_id, Direction::TargetToClient)
        .expect("first episode closes");
    progress
        .observe_outstanding(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(20),
            SimTime::from_nanos(30),
        )
        .expect("second episode begins at a later replay frontier");

    assert_eq!(
        progress.observe_positive_ack(
            current_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(15),
            SimTime::from_nanos(40),
        ),
        Err(OwnerProgressError::ApplicationAckDidNotAdvance {
            previous: ByteOffset::new(20),
            observed: ByteOffset::new(15),
        })
    );
}

#[test]
fn owner_progress_rebases_exact_direction_without_losing_outstanding() {
    let flow_id = flow(2);
    let previous_generation = generation(7);
    let committed_generation = generation(8);
    let mut progress = OwnerReverseProgress::new(1).expect("bounded progress");
    progress
        .register_live(previous_generation, flow_id, Direction::TargetToClient)
        .expect("live reverse flow");
    progress
        .observe_outstanding(
            previous_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(11),
            SimTime::from_nanos(20),
        )
        .expect("old generation replay is outstanding");

    progress
        .rebase_direction(committed_generation, flow_id, Direction::TargetToClient)
        .expect("committed successor generation rebases the exact direction");
    progress
        .observe_positive_ack(
            committed_generation,
            flow_id,
            Direction::TargetToClient,
            ByteOffset::new(12),
            SimTime::from_nanos(30),
        )
        .expect("new generation application ACK advances retained replay");

    let snapshot = progress
        .direction_snapshot(flow_id, Direction::TargetToClient)
        .expect("rebased progress remains live");
    assert_eq!(snapshot.oldest_unacknowledged(), Some(ByteOffset::new(11)));
    assert_eq!(snapshot.outstanding_since(), Some(SimTime::from_nanos(20)));
    assert_eq!(
        snapshot.latest_positive_ack(),
        Some((ByteOffset::new(12), SimTime::from_nanos(30)))
    );
}

fn validator_fixture() -> (
    ClientHintValidator,
    StandbyActorSeal,
    SessionId,
    LegGeneration,
    StandbyNonce,
) {
    let session_id = session(0x31);
    let current_generation = generation(3);
    let nonce = standby_nonce(0x41);
    let (standby, seal) = registered_standby(session_id, current_generation, nonce);
    let mut validator = ClientHintValidator::new(
        session_id,
        current_generation,
        FeatureSet::STANDBY_CONTROL_V1,
        standby,
        4,
    )
    .expect("client validator");
    validator
        .register_live_flow(
            current_generation,
            flow(5),
            Direction::TargetToClient,
            ByteOffset::new(100),
        )
        .expect("live reverse flow");
    (validator, seal, session_id, current_generation, nonce)
}

fn assert_rejection_is_snapshot_inert(
    validator: &mut ClientHintValidator,
    arrival: StandbyHintArrival,
    expected: ClientHintError,
) {
    let before = validator.snapshot();
    assert_eq!(validator.validate(arrival), Err(expected));
    assert_eq!(validator.snapshot(), before);
}

#[test]
fn client_rejection_matrix_is_typed_and_snapshot_inert() {
    let (mut validator, seal, session_id, current_generation, nonce) = validator_fixture();
    let valid = || {
        switch_hint(
            session_id,
            current_generation,
            nonce,
            10,
            flow(5),
            Direction::TargetToClient,
            80,
        )
    };

    assert_rejection_is_snapshot_inert(
        &mut validator,
        StandbyHintArrival::new(StandbyActorSeal::mint(), valid(), SimTime::from_nanos(50)),
        ClientHintError::WrongStandbyActor,
    );
    assert_rejection_is_snapshot_inert(
        &mut validator,
        StandbyHintArrival::new(
            seal.clone(),
            switch_hint(
                session(0x32),
                current_generation,
                nonce,
                10,
                flow(5),
                Direction::TargetToClient,
                80,
            ),
            SimTime::from_nanos(50),
        ),
        ClientHintError::WrongSession,
    );
    assert_rejection_is_snapshot_inert(
        &mut validator,
        StandbyHintArrival::new(
            seal.clone(),
            switch_hint(
                session_id,
                generation(4),
                nonce,
                10,
                flow(5),
                Direction::TargetToClient,
                80,
            ),
            SimTime::from_nanos(50),
        ),
        ClientHintError::WrongGeneration,
    );
    assert_rejection_is_snapshot_inert(
        &mut validator,
        StandbyHintArrival::new(
            seal.clone(),
            switch_hint(
                session_id,
                current_generation,
                standby_nonce(0x42),
                10,
                flow(5),
                Direction::TargetToClient,
                80,
            ),
            SimTime::from_nanos(50),
        ),
        ClientHintError::WrongStandbyNonce,
    );
    let before_wrong_direction = validator.snapshot();
    assert_eq!(
        LegControlFrame::try_new(
            current_generation,
            LegControlRecord::SwitchHint {
                session_id,
                standby_nonce: nonce,
                hint_sequence: HintSequence::new(10).expect("nonzero sequence"),
                flow_id: flow(5),
                direction: Direction::ClientToTarget,
                oldest_unacknowledged: ByteOffset::new(80),
                cause: SwitchHintCause::ReverseApplicationAckStall,
            },
            FeatureSet::STANDBY_CONTROL_V1,
        ),
        Err(crate::resumable::ProtocolError::InvalidSwitchHintDirection(
            Direction::ClientToTarget,
        ))
    );
    assert_eq!(validator.snapshot(), before_wrong_direction);
    assert_rejection_is_snapshot_inert(
        &mut validator,
        StandbyHintArrival::new(
            seal.clone(),
            switch_hint(
                session_id,
                current_generation,
                nonce,
                10,
                flow(6),
                Direction::TargetToClient,
                80,
            ),
            SimTime::from_nanos(50),
        ),
        ClientHintError::UnknownFlowDirection,
    );
    assert_rejection_is_snapshot_inert(
        &mut validator,
        StandbyHintArrival::new(
            seal.clone(),
            switch_hint(
                session_id,
                current_generation,
                nonce,
                10,
                flow(5),
                Direction::TargetToClient,
                101,
            ),
            SimTime::from_nanos(50),
        ),
        ClientHintError::HintAheadOfAcceptedFrontier {
            accepted: ByteOffset::new(100),
            hinted: ByteOffset::new(101),
        },
    );
    assert_rejection_is_snapshot_inert(
        &mut validator,
        StandbyHintArrival::new(seal.clone(), valid(), SimTime::from_nanos(111)),
        ClientHintError::Standby(StandbyEvidenceError::Stale),
    );

    let probe = LegControlFrame::try_new(
        current_generation,
        LegControlRecord::LegProbe {
            session_id,
            leg_epoch_nonce: LegEpochNonce::new([0x51; 16]).expect("nonzero epoch nonce"),
            probe_sequence: ProbeSequence::new(1).expect("nonzero probe sequence"),
        },
        FeatureSet::STANDBY_CONTROL_V1,
    )
    .expect("valid probe frame");
    assert_rejection_is_snapshot_inert(
        &mut validator,
        StandbyHintArrival::new(seal.clone(), probe, SimTime::from_nanos(50)),
        ClientHintError::WrongRecordKind,
    );

    validator
        .mark_terminal(current_generation, flow(5), Direction::TargetToClient)
        .expect("terminal flow");
    assert_rejection_is_snapshot_inert(
        &mut validator,
        StandbyHintArrival::new(seal, valid(), SimTime::from_nanos(50)),
        ClientHintError::TerminalFlowDirection,
    );
}

#[test]
fn client_accepts_lower_offset_but_rejects_duplicate_and_lower_sequences() {
    let (mut validator, seal, session_id, current_generation, nonce) = validator_fixture();
    let first = validator
        .validate(StandbyHintArrival::new(
            seal.clone(),
            switch_hint(
                session_id,
                current_generation,
                nonce,
                10,
                flow(5),
                Direction::TargetToClient,
                80,
            ),
            SimTime::from_nanos(50),
        ))
        .expect("valid authenticated hint");
    assert_eq!(first.oldest_unacknowledged, ByteOffset::new(80));

    for sequence in [10, 9] {
        let expected = ClientHintError::HintSequenceNotIncreasing {
            last: HintSequence::new(10).expect("nonzero sequence"),
            observed: HintSequence::new(sequence).expect("nonzero sequence"),
        };
        assert_rejection_is_snapshot_inert(
            &mut validator,
            StandbyHintArrival::new(
                seal.clone(),
                switch_hint(
                    session_id,
                    current_generation,
                    nonce,
                    sequence,
                    flow(5),
                    Direction::TargetToClient,
                    80,
                ),
                SimTime::from_nanos(50),
            ),
            expected,
        );
    }

    let lower_than_frontier = validator
        .validate(StandbyHintArrival::new(
            seal,
            switch_hint(
                session_id,
                current_generation,
                nonce,
                11,
                flow(5),
                Direction::TargetToClient,
                40,
            ),
            SimTime::from_nanos(50),
        ))
        .expect("ACK loss permits a hint below the local accepted frontier");
    assert_eq!(
        lower_than_frontier.oldest_unacknowledged,
        ByteOffset::new(40)
    );
}

#[test]
fn client_hint_sequence_commits_only_after_the_single_arbiter_accepts_it() {
    let (mut validator, seal, session_id, current_generation, nonce) = validator_fixture();
    let arrival = || {
        StandbyHintArrival::new(
            seal.clone(),
            switch_hint(
                session_id,
                current_generation,
                nonce,
                10,
                flow(5),
                Direction::TargetToClient,
                80,
            ),
            SimTime::from_nanos(50),
        )
    };
    let mut closed = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        nonce,
        attach_nonce(0x61),
        SwitchEpoch::new(7).unwrap(),
    )
    .expect("closed arbiter");
    assert_eq!(
        closed.close_switch_admission_for_shutdown(),
        SwitchShutdownDisposition::ClosedIdle,
    );
    let before = validator.snapshot();
    assert!(matches!(
        validator.transact_arrival_with_arbiter(arrival(), &mut closed),
        Err(ClientHintArbiterError::Arbiter(
            SwitchArbiterError::SwitchAdmissionClosedForShutdown,
        )),
    ));
    assert_eq!(validator.snapshot(), before);

    let mut open = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        nonce,
        attach_nonce(0x62),
        SwitchEpoch::new(8).unwrap(),
    )
    .expect("open arbiter");
    let outcome = validator
        .transact_arrival_with_arbiter(arrival(), &mut open)
        .expect("the same sequence remains admissible after downstream rejection");
    assert_eq!(outcome.disposition(), SwitchTriggerDisposition::Started);
    assert_eq!(
        validator.snapshot().last_accepted_sequence,
        Some(HintSequence::new(10).unwrap()),
    );
    let permit = exact_started_permit(outcome);
    assert_eq!(
        open.close_switch_admission_for_shutdown(),
        SwitchShutdownDisposition::ClosedPendingSettlement,
    );
    open.settle_shutdown_pending(
        permit.into_shutdown_settlement_for_test(PhysicalPendingAttachStage::AttachPending),
    )
    .expect("the exact permit settles");
    assert!(open.inventory().is_live_exact_zero());
}

#[test]
fn client_terminal_grace_retirement_releases_bounded_hint_capacity() {
    let session_id = session(0x35);
    let current_generation = generation(3);
    let nonce = standby_nonce(0x45);
    let (standby, _) = registered_standby(session_id, current_generation, nonce);
    let mut validator = ClientHintValidator::new(
        session_id,
        current_generation,
        FeatureSet::STANDBY_CONTROL_V1,
        standby,
        1,
    )
    .expect("one flow has two bounded directional entries");
    for direction in [Direction::ClientToTarget, Direction::TargetToClient] {
        validator
            .register_live_flow(current_generation, flow(1), direction, ByteOffset::new(0))
            .expect("first flow direction");
        validator
            .mark_terminal(current_generation, flow(1), direction)
            .expect("direction reaches terminal grace");
    }
    assert_eq!(
        validator
            .retire_flow_after_terminal_grace(current_generation, flow(1))
            .expect("flow-wide terminal grace commits"),
        2,
    );
    assert_eq!(
        validator
            .retire_flow_after_terminal_grace(current_generation, flow(1))
            .expect("an untracked retired flow is an idempotent no-op"),
        0,
    );
    for direction in [Direction::ClientToTarget, Direction::TargetToClient] {
        validator
            .register_live_flow(current_generation, flow(2), direction, ByteOffset::new(0))
            .expect("retirement releases capacity for the next live flow");
    }
}

fn authenticated_hint() -> AuthenticatedSwitchHint {
    let (mut validator, seal, session_id, current_generation, nonce) = validator_fixture();
    validator
        .validate(StandbyHintArrival::new(
            seal,
            switch_hint(
                session_id,
                current_generation,
                nonce,
                1,
                flow(5),
                Direction::TargetToClient,
                80,
            ),
            SimTime::from_nanos(50),
        ))
        .expect("authenticated hint")
}

#[test]
fn local_stall_trigger_is_bound_to_the_current_outstanding_episode() {
    let session_id = session(0x31);
    let current_generation = generation(3);
    let mut source = ClientLocalAckStallSource::new(session_id, current_generation);
    source
        .begin_outstanding(SimTime::from_nanos(10), Duration::from_nanos(20))
        .expect("first exact outstanding episode");
    let stale = source
        .take_due_trigger(SimTime::from_nanos(30))
        .expect("first episode reaches its inclusive deadline");
    assert!(source.clear_outstanding());
    source
        .begin_outstanding(SimTime::from_nanos(40), Duration::from_nanos(20))
        .expect("second exact outstanding episode");

    let mut arbiter = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        standby_nonce(0x41),
        attach_nonce(0x65),
        SwitchEpoch::new(10).unwrap(),
    )
    .expect("arbiter");
    let before = arbiter.snapshot();
    assert!(matches!(
        arbiter.on_local_ack_stall(&mut source, stale),
        Err(SwitchArbiterError::LocalAckStallEvidence(
            ClientLocalAckStallError::StaleOutstandingEpisode
        ))
    ));
    assert_eq!(arbiter.snapshot(), before);

    assert_eq!(
        source.take_due_trigger(SimTime::from_nanos(59)),
        Err(ClientLocalAckStallError::DeadlineNotDue {
            due_at: SimTime::from_nanos(60)
        })
    );
    let current = source
        .take_due_trigger(SimTime::from_nanos(60))
        .expect("second episode reaches its own deadline");
    assert_eq!(
        arbiter
            .on_local_ack_stall(&mut source, current)
            .expect("the current exact episode may start the switch")
            .disposition(),
        SwitchTriggerDisposition::Started
    );
}

#[test]
fn positive_ack_refresh_makes_an_already_selected_local_stall_inert() {
    let session_id = session(0x31);
    let current_generation = generation(3);
    let mut source = ClientLocalAckStallSource::new(session_id, current_generation);
    source
        .begin_outstanding(SimTime::from_nanos(10), Duration::from_nanos(20))
        .expect("outstanding episode");
    let stale = source
        .take_due_trigger(SimTime::from_nanos(30))
        .expect("old baseline becomes due");
    source
        .observe_positive_ack(SimTime::from_nanos(31))
        .expect("positive application ACK refreshes the exact baseline");

    let mut arbiter = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        standby_nonce(0x41),
        attach_nonce(0x66),
        SwitchEpoch::new(11).unwrap(),
    )
    .expect("arbiter");
    let before = arbiter.snapshot();
    assert!(matches!(
        arbiter.on_local_ack_stall(&mut source, stale),
        Err(SwitchArbiterError::LocalAckStallEvidence(
            ClientLocalAckStallError::StaleOutstandingEpisode
        ))
    ));
    assert_eq!(arbiter.snapshot(), before);
    assert_eq!(
        source.take_due_trigger(SimTime::from_nanos(50)),
        Err(ClientLocalAckStallError::DeadlineNotDue {
            due_at: SimTime::from_nanos(51)
        })
    );
}

#[test]
fn shutdown_rejection_still_settles_the_exact_local_stall_trigger() {
    let session_id = session(0x31);
    let current_generation = generation(3);
    let mut source = ClientLocalAckStallSource::new(session_id, current_generation);
    source
        .begin_outstanding(SimTime::from_nanos(10), Duration::from_nanos(20))
        .expect("outstanding episode");
    let trigger = source
        .take_due_trigger(SimTime::from_nanos(30))
        .expect("exact episode reaches its deadline");
    let mut arbiter = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        standby_nonce(0x41),
        attach_nonce(0x66),
        SwitchEpoch::new(11).unwrap(),
    )
    .expect("arbiter");
    assert_eq!(
        arbiter.close_switch_admission_for_shutdown(),
        SwitchShutdownDisposition::ClosedIdle
    );
    let closed = arbiter.snapshot();

    assert!(matches!(
        arbiter.on_local_ack_stall(&mut source, trigger),
        Err(SwitchArbiterError::SwitchAdmissionClosedForShutdown)
    ));
    assert_eq!(arbiter.snapshot(), closed);
    assert!(arbiter.inventory().is_live_exact_zero());
    assert_eq!(
        source.take_due_trigger(SimTime::from_nanos(30)),
        Err(ClientLocalAckStallError::TriggerAlreadyConsumed)
    );
}

#[test]
fn wrong_arbiter_correlation_restores_the_exact_local_stall_for_retry() {
    let source_session = session(0x31);
    let current_generation = generation(3);
    let mut source = ClientLocalAckStallSource::new(source_session, current_generation);
    source
        .begin_outstanding(SimTime::from_nanos(10), Duration::from_nanos(20))
        .expect("outstanding episode");
    let trigger = source
        .take_due_trigger(SimTime::from_nanos(30))
        .expect("exact episode reaches its deadline");
    let mut wrong_arbiter = ClientSwitchArbiter::new(
        session(0x32),
        current_generation,
        standby_nonce(0x41),
        attach_nonce(0x66),
        SwitchEpoch::new(11).unwrap(),
    )
    .expect("wrong-session arbiter");
    let before = wrong_arbiter.snapshot();

    assert!(matches!(
        wrong_arbiter.on_local_ack_stall(&mut source, trigger),
        Err(SwitchArbiterError::WrongSession),
    ));
    assert_eq!(wrong_arbiter.snapshot(), before);

    let retry = source
        .take_due_trigger(SimTime::from_nanos(30))
        .expect("correlation rejection restores the exact source episode");
    let mut exact_arbiter = ClientSwitchArbiter::new(
        source_session,
        current_generation,
        standby_nonce(0x41),
        attach_nonce(0x66),
        SwitchEpoch::new(11).unwrap(),
    )
    .expect("exact arbiter");
    assert_eq!(
        exact_arbiter
            .on_local_ack_stall(&mut source, retry)
            .expect("the exact retry remains admissible")
            .disposition(),
        SwitchTriggerDisposition::Started,
    );
}

#[test]
fn idle_shutdown_consumes_the_one_ready_seed_into_zero_live_audit_one_hundred_times() {
    for repeat in 1_u64..=100 {
        let mut arbiter = ClientSwitchArbiter::new(
            session(0x31),
            generation(3),
            standby_nonce(0x41),
            attach_nonce(0x66),
            SwitchEpoch::new(repeat).expect("repeat epoch is nonzero"),
        )
        .expect("arbiter");
        let before = arbiter.inventory();
        assert_eq!(before.live_intents(), 0);
        assert_eq!(before.live_attach_nonces(), 1);
        assert_eq!(before.live_switch_epochs(), 1);
        assert_eq!(before.pending_physical_settlements(), 0);

        assert_eq!(
            arbiter.close_switch_admission_for_shutdown(),
            SwitchShutdownDisposition::ClosedIdle
        );
        let audit = arbiter
            .discard_audit()
            .expect("idle shutdown retains scalar discard history");
        assert_eq!(audit.discarded_intents(), 0);
        assert_eq!(audit.discarded_attach_nonces(), 1);
        assert_eq!(audit.discarded_switch_epochs(), 1);
        assert_eq!(audit.physical_stage(), None);
        assert!(arbiter.inventory().is_live_exact_zero());
        assert_eq!(arbiter.inventory().historical_discards(), 1);
        assert_eq!(
            arbiter.close_switch_admission_for_shutdown(),
            SwitchShutdownDisposition::AlreadyClosed
        );
        assert_eq!(arbiter.discard_audit(), Some(audit));
    }
}

#[derive(Clone, Copy)]
enum TestTrigger {
    Hard,
    Local,
    Hint,
}

fn apply_test_trigger(
    arbiter: &mut ClientSwitchArbiter,
    trigger: TestTrigger,
    session_id: SessionId,
    current_generation: LegGeneration,
) -> SwitchTriggerOutcome {
    try_apply_test_trigger(arbiter, trigger, session_id, current_generation).expect("test trigger")
}

fn try_apply_test_trigger(
    arbiter: &mut ClientSwitchArbiter,
    trigger: TestTrigger,
    session_id: SessionId,
    current_generation: LegGeneration,
) -> Result<SwitchTriggerOutcome, SwitchArbiterError> {
    match trigger {
        TestTrigger::Hard => arbiter.on_hard_failure(HardFailureTrigger::new(
            session_id,
            current_generation,
            SimTime::from_nanos(60),
        )),
        TestTrigger::Local => {
            let mut source = ClientLocalAckStallSource::new(session_id, current_generation);
            source
                .begin_outstanding(SimTime::from_nanos(40), Duration::from_nanos(20))
                .expect("local outstanding episode");
            let trigger = source
                .take_due_trigger(SimTime::from_nanos(60))
                .expect("local stall deadline is due");
            let result = arbiter.on_local_ack_stall(&mut source, trigger);
            assert!(source.clear_outstanding());
            result
        }
        TestTrigger::Hint => arbiter.on_authenticated_hint(authenticated_hint()),
    }
}

fn exact_started_permit(outcome: SwitchTriggerOutcome) -> PendingPhysicalAttachPermit {
    assert_eq!(outcome.disposition(), SwitchTriggerDisposition::Started);
    outcome
        .into_physical_permit()
        .expect("only the first trigger carries the exact physical permit")
}

#[test]
fn prepared_switch_commit_rolls_back_snapshot_inert_or_finalizes_infallibly() {
    let session_id = session(0x31);
    let current_generation = generation(3);
    let switch_epoch = SwitchEpoch::new(7).expect("nonzero epoch");
    let mut arbiter = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        standby_nonce(0x41),
        attach_nonce(0x61),
        switch_epoch,
    )
    .expect("arbiter");
    let accepted = exact_started_permit(
        arbiter
            .on_hard_failure(HardFailureTrigger::new(
                session_id,
                current_generation,
                SimTime::from_nanos(60),
            ))
            .expect("hard failure starts one exact attempt"),
    )
    .into_accepted_for_test();
    let pending = arbiter.snapshot();

    let prepared = arbiter
        .preflight_commit(&accepted)
        .expect("exact acceptance prepares the pending attempt");
    assert_eq!(prepared.plan().generation(), generation(4));
    assert_eq!(prepared.plan().switch_epoch(), switch_epoch);
    assert!(prepared.plan().is_post_loss());

    drop(prepared);
    assert_eq!(arbiter.snapshot(), pending);
    assert_eq!(arbiter.inventory().pending_physical_settlements(), 1);

    let prepared = arbiter
        .preflight_commit(&accepted)
        .expect("the restored exact acceptance remains valid");
    let expected = prepared.plan();
    let committed = prepared.finalize();
    drop(accepted);
    assert_eq!(committed.generation(), expected.generation());
    assert_eq!(committed.switch_epoch(), expected.switch_epoch());
    assert_eq!(committed.is_post_loss(), expected.is_post_loss());
    assert_eq!(arbiter.snapshot().current_generation, generation(4));
    assert!(arbiter.inventory().is_live_exact_zero());
}

#[test]
fn all_six_trigger_orderings_share_one_nonce_epoch_and_exact_next_commit() {
    let orderings = [
        [TestTrigger::Hard, TestTrigger::Local, TestTrigger::Hint],
        [TestTrigger::Hard, TestTrigger::Hint, TestTrigger::Local],
        [TestTrigger::Local, TestTrigger::Hard, TestTrigger::Hint],
        [TestTrigger::Local, TestTrigger::Hint, TestTrigger::Hard],
        [TestTrigger::Hint, TestTrigger::Hard, TestTrigger::Local],
        [TestTrigger::Hint, TestTrigger::Local, TestTrigger::Hard],
    ];

    for ordering in orderings {
        let session_id = session(0x31);
        let current_generation = generation(3);
        let nonce = standby_nonce(0x41);
        let attach = attach_nonce(0x61);
        let epoch = SwitchEpoch::new(7).expect("nonzero epoch");
        let mut arbiter =
            ClientSwitchArbiter::new(session_id, current_generation, nonce, attach, epoch)
                .expect("arbiter");

        let mut started_permit = None;
        let mut upgraded = 0;
        for trigger in ordering {
            let outcome = apply_test_trigger(&mut arbiter, trigger, session_id, current_generation);
            let disposition = outcome.disposition();
            upgraded += usize::from(disposition == SwitchTriggerDisposition::UpgradedToPostLoss);
            assert_eq!(outcome.intent().attach_nonce(), attach);
            assert_eq!(outcome.intent().standby_nonce(), nonce);
            assert_eq!(outcome.intent().switch_epoch(), epoch);
            assert_eq!(outcome.intent().current_generation(), current_generation);
            assert_eq!(outcome.intent().requested_generation(), generation(4));
            let permit = outcome.into_physical_permit();
            if disposition == SwitchTriggerDisposition::Started {
                assert!(
                    started_permit
                        .replace(permit.expect("started owns permit"))
                        .is_none()
                );
            } else {
                assert!(permit.is_none(), "dedupe/upgrade must not mint a permit");
            }
        }
        let accepted = started_permit
            .take()
            .expect("exactly one started trigger")
            .into_accepted_for_test();
        assert!(upgraded <= 1);
        let pending = arbiter.snapshot();
        let ArbiterState::Pending(intent) = pending.state else {
            panic!("exactly one attach remains pending");
        };
        assert!(intent.is_post_loss());

        let committed = arbiter
            .commit(accepted)
            .expect("correlated exact-next acceptance");
        assert_eq!(committed.generation(), generation(4));
        assert_eq!(committed.switch_epoch(), epoch);
        assert!(committed.is_post_loss());
        assert_eq!(arbiter.snapshot().current_generation, generation(4));
        assert!(arbiter.inventory().is_live_exact_zero());
        assert!(matches!(
            arbiter.on_hard_failure(HardFailureTrigger::new(
                session_id,
                generation(4),
                SimTime::from_nanos(61),
            )),
            Err(SwitchArbiterError::EpochClosed)
        ));
    }
}

#[test]
fn shutdown_closes_switch_admission_in_all_trigger_orderings() {
    #[derive(Clone, Copy)]
    enum Event {
        Trigger(TestTrigger),
        Shutdown,
    }

    let events = [
        Event::Trigger(TestTrigger::Hard),
        Event::Trigger(TestTrigger::Local),
        Event::Trigger(TestTrigger::Hint),
        Event::Shutdown,
    ];
    for first in 0..events.len() {
        for second in 0..events.len() {
            for third in 0..events.len() {
                for fourth in 0..events.len() {
                    let indexes = [first, second, third, fourth];
                    if indexes.iter().enumerate().any(|(index, value)| {
                        indexes[..index].iter().any(|earlier| earlier == value)
                    }) {
                        continue;
                    }

                    let session_id = session(0x31);
                    let current_generation = generation(3);
                    let attach = attach_nonce(0x67);
                    let mut arbiter = ClientSwitchArbiter::new(
                        session_id,
                        current_generation,
                        standby_nonce(0x41),
                        attach,
                        SwitchEpoch::new(12).unwrap(),
                    )
                    .expect("arbiter");
                    let mut shutdown_snapshot = None;
                    let mut trigger_started_before_shutdown = false;
                    let mut hard_seen_before_shutdown = false;
                    let mut exact_permit = None;

                    for event in indexes.map(|index| events[index]) {
                        match event {
                            Event::Shutdown => {
                                let expected = if exact_permit.is_some() {
                                    SwitchShutdownDisposition::ClosedPendingSettlement
                                } else {
                                    SwitchShutdownDisposition::ClosedIdle
                                };
                                assert_eq!(arbiter.close_switch_admission_for_shutdown(), expected);
                                shutdown_snapshot = Some(arbiter.snapshot());
                            }
                            Event::Trigger(trigger) if shutdown_snapshot.is_some() => {
                                assert!(matches!(
                                    try_apply_test_trigger(
                                        &mut arbiter,
                                        trigger,
                                        session_id,
                                        current_generation,
                                    ),
                                    Err(SwitchArbiterError::SwitchAdmissionClosedForShutdown)
                                ));
                                assert_eq!(arbiter.snapshot(), shutdown_snapshot.unwrap());
                            }
                            Event::Trigger(trigger) => {
                                let outcome = apply_test_trigger(
                                    &mut arbiter,
                                    trigger,
                                    session_id,
                                    current_generation,
                                );
                                trigger_started_before_shutdown = true;
                                hard_seen_before_shutdown |= matches!(trigger, TestTrigger::Hard);
                                assert_eq!(outcome.intent().attach_nonce(), attach);
                                let disposition = outcome.disposition();
                                let permit = outcome.into_physical_permit();
                                if disposition == SwitchTriggerDisposition::Started {
                                    assert!(
                                        exact_permit
                                            .replace(permit.expect("started permit"))
                                            .is_none()
                                    );
                                } else {
                                    assert!(permit.is_none());
                                }
                            }
                        }
                    }

                    let closed = arbiter.snapshot();
                    assert!(closed.switch_admission_closed);
                    match closed.state {
                        ArbiterState::Terminal => assert!(!trigger_started_before_shutdown),
                        ArbiterState::ShutdownPending(intent) => {
                            assert!(trigger_started_before_shutdown);
                            assert_eq!(intent.is_post_loss(), hard_seen_before_shutdown);
                        }
                        _ => panic!("shutdown must be terminal or await exact physical settlement"),
                    }
                    if let Some(permit) = exact_permit.take() {
                        assert_eq!(arbiter.inventory().pending_physical_settlements(), 1);
                        let audit = arbiter
                            .settle_shutdown_pending(permit.into_shutdown_settlement_for_test(
                                PhysicalPendingAttachStage::AttachPending,
                            ))
                            .expect("the exact physical permit settles shutdown");
                        assert_eq!(audit.discarded_intents(), 1);
                        assert_eq!(audit.discarded_attach_nonces(), 1);
                        assert_eq!(audit.discarded_switch_epochs(), 1);
                    }
                    assert!(arbiter.inventory().is_live_exact_zero());
                    assert!(matches!(arbiter.snapshot().state, ArbiterState::Terminal));
                    assert_eq!(
                        arbiter.close_switch_admission_for_shutdown(),
                        SwitchShutdownDisposition::AlreadyClosed
                    );
                }
            }
        }
    }
}

#[test]
fn soft_to_hard_reuses_intent_and_trigger_races_repeat_one_hundred_times() {
    let orderings = [
        [TestTrigger::Hard, TestTrigger::Local, TestTrigger::Hint],
        [TestTrigger::Hard, TestTrigger::Hint, TestTrigger::Local],
        [TestTrigger::Local, TestTrigger::Hard, TestTrigger::Hint],
        [TestTrigger::Local, TestTrigger::Hint, TestTrigger::Hard],
        [TestTrigger::Hint, TestTrigger::Hard, TestTrigger::Local],
        [TestTrigger::Hint, TestTrigger::Local, TestTrigger::Hard],
    ];

    for _repeat in 0..100 {
        for ordering in orderings {
            let session_id = session(0x31);
            let current_generation = generation(3);
            let attach = attach_nonce(0x62);
            let epoch = SwitchEpoch::new(8).expect("nonzero epoch");
            let mut arbiter = ClientSwitchArbiter::new(
                session_id,
                current_generation,
                standby_nonce(0x41),
                attach,
                epoch,
            )
            .expect("arbiter");
            let first =
                apply_test_trigger(&mut arbiter, ordering[0], session_id, current_generation);
            let first_nonce = first.intent().attach_nonce();
            let first_epoch = first.intent().switch_epoch();
            let permit = exact_started_permit(first);
            for trigger in [ordering[1], ordering[2]] {
                let outcome =
                    apply_test_trigger(&mut arbiter, trigger, session_id, current_generation);
                assert_eq!(outcome.intent().attach_nonce(), first_nonce);
                assert_eq!(outcome.intent().switch_epoch(), first_epoch);
                assert!(outcome.into_physical_permit().is_none());
            }
            let ArbiterState::Pending(intent) = arbiter.snapshot().state else {
                panic!("one pending transaction");
            };
            assert!(intent.is_post_loss());
            assert_eq!(
                arbiter.close_switch_admission_for_shutdown(),
                SwitchShutdownDisposition::ClosedPendingSettlement
            );
            arbiter
                .settle_shutdown_pending(
                    permit.into_shutdown_settlement_for_test(
                        PhysicalPendingAttachStage::AttachPending,
                    ),
                )
                .expect("repeat settles the exact physical permit");
            assert!(arbiter.inventory().is_live_exact_zero());
        }
    }
}

#[test]
fn soft_started_physical_permit_observes_the_identity_bound_hard_upgrade() {
    let session_id = session(0x31);
    let current_generation = generation(3);
    let mut arbiter = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        standby_nonce(0x41),
        attach_nonce(0x62),
        SwitchEpoch::new(8).unwrap(),
    )
    .expect("arbiter");
    let soft = arbiter
        .on_authenticated_hint(authenticated_hint())
        .expect("authenticated hint starts the one soft attempt");
    let permit = exact_started_permit(soft);
    assert!(!permit.intent().is_post_loss());

    assert_eq!(
        arbiter
            .on_hard_failure(HardFailureTrigger::new(
                session_id,
                current_generation,
                SimTime::from_nanos(61),
            ))
            .expect("hard terminal upgrades the same attempt")
            .disposition(),
        SwitchTriggerDisposition::UpgradedToPostLoss,
    );
    assert!(permit.intent().is_post_loss());
    let ArbiterState::Pending(current) = arbiter.snapshot().state else {
        panic!("the upgraded attempt remains pending");
    };
    assert_eq!(permit.intent(), current);
    let OwnedArbiterState::Pending(pending) = &arbiter.state else {
        panic!("the exact pending identity remains live");
    };
    assert!(pending.identity.post_loss.load(Ordering::Acquire));
    assert!(
        !pending.intent.post_loss,
        "the copy-only base intent must not duplicate the identity's mutable post-loss fact",
    );

    let committed = arbiter
        .commit(permit.into_accepted_for_test())
        .expect("the identity-bound permit still settles the same attempt");
    assert!(committed.is_post_loss());
}

#[test]
fn physical_preflight_rejects_scalar_equal_permit_from_another_arbiter() {
    let session_id = session(0x31);
    let current_generation = generation(3);
    let standby_nonce = standby_nonce(0x41);
    let attach_nonce = attach_nonce(0x62);
    let epoch = SwitchEpoch::new(8).expect("nonzero epoch");
    let mut exact = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        standby_nonce,
        attach_nonce,
        epoch,
    )
    .expect("exact arbiter");
    let mut scalar_equal = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        standby_nonce,
        attach_nonce,
        epoch,
    )
    .expect("scalar-equal arbiter");
    let exact_permit = exact_started_permit(
        exact
            .on_authenticated_hint(authenticated_hint())
            .expect("exact attempt starts"),
    );
    let wrong_permit = exact_started_permit(
        scalar_equal
            .on_authenticated_hint(authenticated_hint())
            .expect("scalar-equal attempt starts"),
    );
    let exact_before = exact.snapshot();

    assert_eq!(
        exact.preflight_physical_permit(&wrong_permit),
        Err(SwitchArbiterError::WrongPhysicalAttachPermit),
    );
    assert_eq!(exact.snapshot(), exact_before);
    assert_eq!(
        exact
            .preflight_physical_permit(&exact_permit)
            .expect("the exact identity is admitted"),
        exact_permit.intent(),
    );

    assert_eq!(
        exact.close_switch_admission_for_shutdown(),
        SwitchShutdownDisposition::ClosedPendingSettlement,
    );
    exact
        .settle_shutdown_pending(
            exact_permit.into_shutdown_settlement_for_test(PhysicalPendingAttachStage::Unbegun),
        )
        .expect("exact permit settles its own arbiter");
    assert_eq!(
        scalar_equal.close_switch_admission_for_shutdown(),
        SwitchShutdownDisposition::ClosedPendingSettlement,
    );
    scalar_equal
        .settle_shutdown_pending(
            wrong_permit.into_shutdown_settlement_for_test(PhysicalPendingAttachStage::Unbegun),
        )
        .expect("wrong-for-exact permit remains usable by its own arbiter");
}

#[test]
fn arbiter_rejections_and_wrong_acceptance_are_snapshot_inert() {
    let session_id = session(0x31);
    let current_generation = generation(3);
    let attach = attach_nonce(0x63);
    let mut arbiter = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        standby_nonce(0x41),
        attach,
        SwitchEpoch::new(9).expect("nonzero epoch"),
    )
    .expect("arbiter");
    let before = arbiter.snapshot();
    assert!(matches!(
        arbiter.on_hard_failure(HardFailureTrigger::new(
            session(0x32),
            current_generation,
            SimTime::from_nanos(1),
        )),
        Err(SwitchArbiterError::WrongSession)
    ));
    assert_eq!(arbiter.snapshot(), before);

    let mut source = ClientLocalAckStallSource::new(session_id, current_generation);
    source
        .begin_outstanding(SimTime::from_nanos(1), Duration::from_nanos(1))
        .expect("local outstanding episode");
    let local_stall = source
        .take_due_trigger(SimTime::from_nanos(2))
        .expect("local stall is due");
    let first = arbiter
        .on_local_ack_stall(&mut source, local_stall)
        .expect("first valid trigger");
    let intent = first.intent();
    let exact_permit = exact_started_permit(first);
    let pending = arbiter.snapshot();
    let mut other = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        standby_nonce(0x41),
        attach,
        SwitchEpoch::new(9).expect("same scalar epoch"),
    )
    .expect("second arbiter with indistinguishable scalar correlation");
    let wrong_accepted = exact_started_permit(
        other
            .on_hard_failure(HardFailureTrigger::new(
                session_id,
                current_generation,
                SimTime::from_nanos(2),
            ))
            .expect("other exact attempt"),
    )
    .into_accepted_for_test();
    let failure = arbiter
        .commit(wrong_accepted)
        .expect_err("scalar-equal but identity-different acceptance is rejected");
    assert_eq!(failure.kind(), SwitchArbiterError::WrongAttachAcceptance);
    assert_eq!(arbiter.snapshot(), pending);
    other
        .commit(failure.into_accepted())
        .expect("rejected acceptance is restored to its exact arbiter");
    arbiter
        .commit(exact_permit.into_accepted_for_test())
        .expect("original exact acceptance remains usable");
    assert!(arbiter.inventory().is_live_exact_zero());
    assert!(other.inventory().is_live_exact_zero());
    assert_eq!(intent.initial_trigger(), SwitchTriggerKind::LocalAckStall);
    assert!(!intent.is_post_loss());
}

#[test]
fn shutdown_after_physical_acceptance_requires_exact_settlement_and_is_repeat_stable() {
    let session_id = session(0x31);
    let current_generation = generation(3);
    let mut arbiter = ClientSwitchArbiter::new(
        session_id,
        current_generation,
        standby_nonce(0x41),
        attach_nonce(0x68),
        SwitchEpoch::new(13).expect("nonzero epoch"),
    )
    .expect("arbiter");
    let accepted = exact_started_permit(
        arbiter
            .on_authenticated_hint(authenticated_hint())
            .expect("authenticated hint starts one attempt"),
    )
    .into_accepted_for_test();
    assert_eq!(
        arbiter.close_switch_admission_for_shutdown(),
        SwitchShutdownDisposition::ClosedPendingSettlement
    );
    let failure = arbiter
        .commit(accepted)
        .expect_err("shutdown cutoff rejects even exact physical acceptance");
    assert_eq!(
        failure.kind(),
        SwitchArbiterError::SwitchAdmissionClosedForShutdown
    );
    assert_eq!(arbiter.inventory().pending_physical_settlements(), 1);
    let audit = arbiter
        .settle_shutdown_pending(
            failure
                .into_accepted()
                .into_shutdown_settlement_for_test(PhysicalPendingAttachStage::Accepted),
        )
        .expect("the retained exact accepted capability settles shutdown");
    assert_eq!(
        audit.physical_stage(),
        Some(PhysicalPendingAttachStage::Accepted)
    );
    assert!(arbiter.inventory().is_live_exact_zero());
    assert_eq!(arbiter.discard_audit(), Some(audit));
    assert_eq!(
        arbiter.close_switch_admission_for_shutdown(),
        SwitchShutdownDisposition::AlreadyClosed
    );
}

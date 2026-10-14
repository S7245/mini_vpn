use super::*;
mod effect_storage;
use crate::owned_upstream::owner_session_controller::OwnerSessionCapacityPlan;
use crate::owned_upstream::target::{TargetReadyInterest, TargetReadyOutcome};
use crate::resumable::{
    BitsPerSecond, DirectionalRates, ReplayCapacitySpec, ReplayStorageGeometry,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

struct DropTrackedTarget {
    inner: MemoryTarget,
    identity: Box<u8>,
    drops: Arc<AtomicUsize>,
}

impl DropTrackedTarget {
    fn new(drops: Arc<AtomicUsize>) -> Self {
        Self {
            inner: MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
            identity: Box::new(0x7a),
            drops,
        }
    }

    fn identity_ptr(&self) -> *const u8 {
        std::ptr::from_ref(self.identity.as_ref())
    }
}

impl Drop for DropTrackedTarget {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl TargetIo for DropTrackedTarget {
    fn capacity_witness(&self) -> crate::owned_upstream::target::TargetIoCapacityWitness {
        self.inner.capacity_witness()
    }

    fn cold_inventory(
        &self,
    ) -> Result<
        crate::owned_upstream::target::TargetIoColdInventory,
        crate::owned_upstream::target::TargetIoColdInventoryError,
    > {
        self.inner.cold_inventory()
    }

    fn open(
        &mut self,
        flow_id: SessionFlowId,
        target: &TargetAddr,
    ) -> Result<TargetOpenCompletion, TargetIoError> {
        self.inner.open(flow_id, target)
    }

    fn write(
        &mut self,
        flow_id: SessionFlowId,
        bytes: &[u8],
    ) -> Result<TargetWriteCompletion, TargetIoError> {
        self.inner.write(flow_id, bytes)
    }

    fn read(
        &mut self,
        flow_id: SessionFlowId,
        max_bytes: usize,
    ) -> Result<TargetReadCompletion, TargetIoError> {
        self.inner.read(flow_id, max_bytes)
    }

    fn half_close_write(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetHalfCloseCompletion, TargetIoError> {
        self.inner.half_close_write(flow_id)
    }

    fn reset(&mut self, flow_id: SessionFlowId) -> Result<TargetResetCompletion, TargetIoError> {
        self.inner.reset(flow_id)
    }

    fn cancel(&mut self, flow_id: SessionFlowId) -> Result<TargetCancelCompletion, TargetIoError> {
        self.inner.cancel(flow_id)
    }

    fn expire_session_flow(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetExpireCompletion, TargetIoError> {
        self.inner.expire_session_flow(flow_id)
    }

    fn poll_readiness(
        &mut self,
        flow_id: SessionFlowId,
        interest: TargetReadyInterest,
        cx: &mut Context<'_>,
    ) -> Poll<Result<TargetReadyOutcome, TargetIoError>> {
        self.inner.poll_readiness(flow_id, interest, cx)
    }

    fn poll_ready_notifications(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TargetIoError>> {
        self.inner.poll_ready_notifications(cx)
    }

    fn readiness(&self, flow_id: SessionFlowId) -> Result<TargetReadiness, TargetIoError> {
        self.inner.readiness(flow_id)
    }

    fn join(&mut self, flow_id: SessionFlowId) -> Result<TargetJoinCompletion, TargetIoError> {
        self.inner.join(flow_id)
    }

    fn retire_terminal_tombstone(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetTombstoneRetireCompletion, TargetIoError> {
        self.inner.retire_terminal_tombstone(flow_id)
    }
}

fn initial_owner_bootstrap(exporter: u8) -> (OwnerSessionBootstrap, LegIo, LegOutboundQueue) {
    let transport = binding(exporter);
    let owner = LegIo::for_authenticated_transport(
        LegId::A,
        LegEndpointRole::Owner,
        transport,
        LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
    );
    let request = standby_attach_request(2, 0x22);
    let proof = credentials().prove(&request, &transport).unwrap();
    let mut pending =
        SessionSupervisor::prepare_owner_unproven_for_test(session_config(), standby_authority(1));
    let mut queue = LegOutboundQueue::for_leg(owner.established_leg(), 4, 4_096).unwrap();
    let bootstrap = pending
        .accept(
            owner.established_leg(),
            owner
                .established_leg()
                .bind_received_frame(request.to_attach_frame(proof)),
            &mut queue,
        )
        .unwrap()
        .enqueue_acceptance(&mut queue)
        .unwrap();
    (bootstrap, owner, queue)
}

fn large_legless_owner_bootstrap() -> (LeglessOwnerSessionBootstrap, OwnerSessionCapacityPlan) {
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(100_000_000).unwrap());
    let storage = ReplayCapacitySpec::new(rates, rates, Duration::from_millis(500), Duration::ZERO)
        .unwrap()
        .derive()
        .unwrap()
        .storage_plan(ReplayStorageGeometry::new(62_500, 4).unwrap())
        .unwrap();
    let plan = OwnerSessionCapacityPlan::derive(storage, 8, 128).unwrap();
    let transport = binding(0x80);
    let (owner, reporter) = EstablishedLeg::for_authenticated_transport_with_reporter(transport);
    let request = standby_attach_request(2, 0x22);
    let proof = credentials().prove(&request, &transport).unwrap();
    let mut pending = SessionSupervisor::prepare_owner(plan, standby_authority(1));
    let mut queue = LegOutboundQueue::for_leg(&owner, 4, 4_096).unwrap();
    let publication = pending
        .accept(
            &owner,
            owner.bind_received_frame(request.to_attach_frame(proof)),
            &mut queue,
        )
        .unwrap();
    let terminal = reporter
        .report(LegTransportTerminalReason::PeerClosed)
        .unwrap();
    (publication.lose_before_acceptance(terminal).unwrap(), plan)
}

#[test]
fn legless_config_failure_returns_exact_high_water_and_target_for_retry() {
    let (bootstrap, plan) = large_legless_owner_bootstrap();
    let pending_effects = plan.session_config().owner_pending_work_bound().unwrap();
    let invalid_read = crate::resumable::MAX_DATA_PAYLOAD_BYTES + 1;
    let invalid = OwnerTargetConfig::new(128, pending_effects, invalid_read).unwrap();
    let source = FlowPortConfig::new(invalid_read, 8, 8, 8).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let target = DropTrackedTarget::new(Arc::clone(&drops));
    let target_identity = target.identity_ptr();

    let failure =
        match OwnerTargetExecutor::new_legless_bootstrap(invalid, bootstrap, target, source) {
            Ok(_) => panic!("an unreachable legless Target read must fail before factory mint"),
            Err(failure) => failure,
        };
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let expected_error = OwnerTargetConfigError::TargetReadExceedsSingleDataPayload {
        requested: invalid_read,
        available: crate::resumable::MAX_DATA_PAYLOAD_BYTES,
    };
    assert_eq!(failure.source(), expected_error);
    let (error, returned_config, bootstrap, target, returned_source) = failure.into_parts();
    assert_eq!(error, expected_error);
    assert_eq!(returned_config, invalid);
    assert_eq!(returned_source, source);
    assert_eq!(bootstrap.snapshot().session.phase(), SessionPhase::Legless);
    assert_eq!(
        bootstrap.high_water_generation(),
        LegGeneration::new(2).unwrap()
    );
    assert_eq!(target.identity_ptr(), target_identity);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    let valid = OwnerTargetConfig::new(
        returned_config.max_effect_steps(),
        returned_config.max_pending_effects(),
        crate::resumable::MAX_DATA_PAYLOAD_BYTES,
    )
    .unwrap();
    let (executor, high_water) =
        OwnerTargetExecutor::new_legless_bootstrap(valid, bootstrap, target, returned_source)
            .unwrap();
    assert_eq!(high_water.generation(), LegGeneration::new(2).unwrap());
    assert_eq!(high_water.reason(), LegTransportTerminalReason::PeerClosed);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop((high_water, executor));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn initial_bootstrap_config_failure_returns_exact_bootstrap_and_target_for_retry() {
    let (bootstrap, _owner, queue) = initial_owner_bootstrap(0x81);
    let invalid = OwnerTargetConfig::new(128, 1, 64).unwrap();
    let valid = OwnerTargetConfig::new(128, 1_024, 64).unwrap();
    let source = source_config();
    let drops = Arc::new(AtomicUsize::new(0));
    let target = DropTrackedTarget::new(Arc::clone(&drops));
    let target_identity = target.identity_ptr();

    let failure = OwnerTargetExecutor::from_initial_bootstrap(invalid, bootstrap, target, source)
        .unwrap_err();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(matches!(
        failure.source(),
        OwnerTargetConfigError::PendingEffectCapacityTooSmall { .. }
    ));
    let (returned_error, returned_config, bootstrap, target, returned_source) =
        failure.into_parts();
    assert!(matches!(
        returned_error,
        OwnerTargetConfigError::PendingEffectCapacityTooSmall { .. }
    ));
    assert_eq!(returned_config, invalid);
    assert_eq!(returned_source, source);
    assert_eq!(target.identity_ptr(), target_identity);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(queue.len(), 1);
    assert_eq!(queue.owned_bytes(), ATTACH_ACCEPTED_ENCODED_BYTES);

    let (executor, handoff) =
        OwnerTargetExecutor::from_initial_bootstrap(valid, bootstrap, target, returned_source)
            .unwrap();
    assert_eq!(
        executor.snapshot().session.session.phase(),
        SessionPhase::Active
    );
    assert_eq!(handoff.generation(), LegGeneration::new(2).unwrap());
    assert_eq!(handoff.retained_acceptance_receipts(), 1);
    assert!(handoff.acceptance_queue_is_live());
    assert!(handoff.acceptance_belongs_to_queue(&queue));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(handoff);
    drop(executor);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

fn bound_initial_owner(
    exporter: u8,
) -> (
    OwnerTargetExecutor<MemoryTarget>,
    OwnerTargetInitialHandoff,
    LegIo,
    LegOutboundQueue,
) {
    let (bootstrap, owner, queue) = initial_owner_bootstrap(exporter);
    let (executor, handoff) = OwnerTargetExecutor::from_initial_bootstrap(
        OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
        bootstrap,
        MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
        source_config(),
    )
    .unwrap();
    (executor, handoff, owner, queue)
}

#[test]
fn same_scalar_wrong_executor_rejects_and_returns_exact_initial_handoff() {
    let (executor_a, handoff_a, _owner_a, queue_a) = bound_initial_owner(0x82);
    let (executor_b, _handoff_b, _owner_b, queue_b) = bound_initial_owner(0x82);
    assert_eq!(handoff_a.generation(), LegGeneration::new(2).unwrap());
    assert_eq!(handoff_a.nonce(), AttachNonce::new([0x22; 16]).unwrap());

    let failure = executor_b
        .validate_initial_handoff_owner(handoff_a)
        .unwrap_err();
    assert_eq!(
        failure.block(),
        OwnerTargetInputBlock::WrongOwnerTargetExecutor
    );
    let handoff_a = failure.into_handoff();
    assert!(handoff_a.acceptance_belongs_to_queue(&queue_a));
    assert!(!handoff_a.acceptance_belongs_to_queue(&queue_b));

    let handoff_a = executor_a
        .validate_initial_handoff_owner(handoff_a)
        .unwrap();
    assert!(handoff_a.acceptance_belongs_to_queue(&queue_a));
}

#[test]
fn initial_acceptance_inventory_tracks_only_its_record_specific_delivery_state() {
    let (executor, handoff, mut owner, mut queue) = bound_initial_owner(0x83);
    let queued = handoff.inventory(&queue).unwrap();
    assert_eq!(queued.attached_authorities(), 1);
    assert_eq!(queued.acceptance_receipts(), 1);
    assert!(queued.executor_identity_live());
    assert!(queued.acceptance_queue_live());
    assert_eq!(
        queued.delivery(),
        OwnerTargetInitialAcceptanceDelivery::AdmittedDeliveryPending
    );

    let mut transport = CapturingTransport::default();
    queue
        .try_flush(&mut owner, &mut transport, SimTime::ZERO)
        .unwrap()
        .unwrap();
    let submitted_or_delivered = handoff.inventory(&queue).unwrap();
    assert_eq!(
        submitted_or_delivered.delivery(),
        OwnerTargetInitialAcceptanceDelivery::DeliveredAwaitingSettlement
    );

    assert!(
        queue
            .try_flush(&mut owner, &mut transport, SimTime::ZERO)
            .unwrap()
            .is_none()
    );
    let retired_aggregate_wait = handoff.inventory(&queue).unwrap();
    assert_eq!(
        retired_aggregate_wait.delivery(),
        OwnerTargetInitialAcceptanceDelivery::DeliveredAwaitingSettlement
    );
    assert_eq!(retired_aggregate_wait.acceptance_receipts(), 1);

    drop(executor);
    let executor_gone = handoff.inventory(&queue).unwrap();
    assert!(!executor_gone.executor_identity_live());
    assert_eq!(executor_gone.attached_authorities(), 1);
    assert_eq!(executor_gone.acceptance_receipts(), 1);

    drop(owner);
    let endpoint_gone = handoff.inventory(&queue).unwrap();
    assert!(!endpoint_gone.acceptance_queue_live());
    assert_eq!(endpoint_gone.attached_authorities(), 1);
    assert_eq!(endpoint_gone.acceptance_receipts(), 1);
}

#[test]
fn initial_handoff_standby_authentication_requires_exact_initial_settlement() {
    let (executor, handoff, _owner, queue) = bound_initial_owner(0x84);
    let mut candidate = owner_standby_leg(0x94);
    let inbound = classified_standby_inbound(&handoff.attached, &mut candidate, 0x35);

    let failure = executor
        .authenticate_owner_standby_from_initial(&handoff, candidate.established_leg(), inbound)
        .unwrap_err();
    let returned = match failure {
        OwnerTargetError::RejectedOwnerStandbyAuthentication { block, inbound } => {
            assert_eq!(
                block,
                OwnerTargetInputBlock::InitialAcceptanceSettlementPending
            );
            *inbound
        }
        error => panic!("unexpected initial standby authentication failure: {error:?}"),
    };

    assert!(matches!(returned, BoundInbound::LegControl(_)));
    assert_eq!(handoff.retained_acceptance_receipts(), 1);
    assert!(handoff.acceptance_belongs_to_queue(&queue));
    assert_eq!(handoff.physical_actor_identities(), 0);
}

#[derive(Default)]
struct HeldInitialAcceptanceTransport {
    completion: Option<OrderedDeliveryToken>,
}

impl EncodedLegTransport for HeldInitialAcceptanceTransport {
    fn send_encoded(
        &mut self,
        _now: SimTime,
        _route: WireRoute,
        _lane: WireLane,
        _bytes: Bytes,
    ) -> Result<(), EncodedLegTransportError> {
        Ok(())
    }

    fn send_attach_ordered_encoded(
        &mut self,
        _now: SimTime,
        _route: WireRoute,
        _lane: WireLane,
        _bytes: Bytes,
        completion: OrderedDeliveryToken,
    ) -> Result<(), EncodedLegTransportError> {
        self.completion = Some(completion);
        Ok(())
    }
}

#[test]
fn initial_retirement_fifo_witness_rejects_phase_frames_and_ordered_wait() {
    let fresh_owner = LegIo::for_authenticated_transport(
        LegId::A,
        LegEndpointRole::Owner,
        binding(0x85),
        LegIoLimits::new(4_096, 8, 32_768, 8, 32_768).unwrap(),
    );
    let fresh_queue = LegOutboundQueue::for_leg(fresh_owner.established_leg(), 4, 4_096).unwrap();
    let fresh = fresh_queue.cold_inventory().unwrap();
    assert_eq!(fresh.queued_frames(), 0);
    assert_eq!(fresh.summed_frame_bytes(), 0);
    assert_eq!(fresh.stored_owned_bytes(), 0);
    assert!(!fresh.ordered_delivery_wait());
    assert!(
        !fresh.is_exact_active_zero(),
        "zero without Active is not current-A"
    );

    let (_executor, _handoff, mut owner, mut queue) = bound_initial_owner(0x86);
    let queued = queue.cold_inventory().unwrap();
    assert!(queued.was_active());
    assert_eq!(queued.queued_frames(), 1);
    assert!(!queued.is_exact_active_zero());

    let mut transport = HeldInitialAcceptanceTransport::default();
    assert!(
        queue
            .try_flush(&mut owner, &mut transport, SimTime::ZERO)
            .unwrap()
            .is_some()
    );
    let waiting = queue.cold_inventory().unwrap();
    assert!(waiting.was_active());
    assert_eq!(waiting.queued_frames(), 0);
    assert_eq!(waiting.summed_frame_bytes(), 0);
    assert_eq!(waiting.stored_owned_bytes(), 0);
    assert!(waiting.ordered_delivery_wait());
    assert!(!waiting.is_exact_active_zero());

    transport
        .completion
        .take()
        .expect("the exact acceptance delivery remains transport-owned")
        .mark_delivered();
    assert!(
        queue
            .try_flush(&mut owner, &mut transport, SimTime::ZERO)
            .unwrap()
            .is_none()
    );
    let exact_zero = queue.cold_inventory().unwrap();
    assert!(exact_zero.is_exact_active_zero());
}

#[test]
fn initial_bootstrap_decomposition_is_statically_gated_by_its_two_owners() {
    let supervisor = include_str!("../../supervisor.rs");
    let owner_target = include_str!("../../owner_target.rs");
    let legacy_harness = include_str!("../../two_leg_harness.rs");
    let supervisor_production = supervisor.split_once("#[cfg(test)]\nmod tests").unwrap().0;
    let owner_target_production = owner_target
        .split_once("#[cfg(test)]\nmod tests")
        .unwrap()
        .0;
    let owner_target_production =
        [owner_target_production, include_str!("../construction.rs")].concat();

    assert!(supervisor_production.contains("_gate: &OwnerTargetInitialBootstrapInstallGate,"));
    assert_eq!(
        owner_target_production
            .matches("OwnerTargetInitialBootstrapInstallGate::mint()")
            .count(),
        1
    );
    assert_eq!(
        owner_target_production
            .matches("bootstrap.into_target_install_parts(&gate)")
            .count(),
        1
    );
    assert!(supervisor_production.contains("_gate: &OwnerTargetLeglessBootstrapInstallGate,"));
    assert_eq!(
        owner_target_production
            .matches("OwnerTargetLeglessBootstrapInstallGate::mint()")
            .count(),
        1
    );
    assert_eq!(
        owner_target_production
            .matches("bootstrap.into_legless_target_install_parts(&gate)")
            .count(),
        1
    );
    assert!(
        !supervisor_production.contains(
            "pub(super) fn into_parts(self) -> (SessionSupervisor, PendingOwnerHighWater)"
        )
    );
    assert!(supervisor_production.contains("#[cfg(test)]\n    pub(super) fn into_legacy_parts("));
    assert!(supervisor_production.contains("_gate: &LegacyOwnerBootstrapDecompositionGate,"));
    assert_eq!(
        legacy_harness
            .matches("LegacyOwnerBootstrapDecompositionGate::mint()")
            .count(),
        1
    );
    assert_eq!(
        legacy_harness
            .matches("bootstrap.into_legacy_parts(&legacy_gate)")
            .count(),
        1
    );
    assert!(!supervisor_production.contains("pub(crate) fn into_legacy_parts(self)"));

    let physical_constructor = owner_target_production
        .split_once("pub(crate) fn from_initial_bootstrap(")
        .unwrap()
        .1
        .split_once("    pub(crate) fn new(")
        .unwrap()
        .0;
    assert!(!physical_constructor.contains("drop("));
}

trait AmbiguousIfClone<Marker> {
    fn marker() {}
}

impl<T: ?Sized> AmbiguousIfClone<()> for T {}
impl<T: Clone> AmbiguousIfClone<u8> for T {}

#[test]
fn initial_owner_handoff_and_preserving_failure_are_noncloneable() {
    let _ = <OwnerTargetInitialHandoff as AmbiguousIfClone<_>>::marker;
    let _ = <OwnerTargetInitialBootstrapFailure<MemoryTarget> as AmbiguousIfClone<_>>::marker;
    let _ = <OwnerTargetLeglessBootstrapFailure<MemoryTarget> as AmbiguousIfClone<_>>::marker;
    let _ = <OwnerTargetInitialHandoffOwnerFailure as AmbiguousIfClone<_>>::marker;
}

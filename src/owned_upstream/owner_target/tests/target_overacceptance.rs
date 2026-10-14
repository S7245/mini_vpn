//! Overaccepting Target Adapter and its exact write-rejection test.
//!
//! Owns the adversarial acceptance report and complete invalid-write tracer.
//! Parent owns shared setup and production Executor, not this private fixture.
//! Preserve the outstanding sink offer and zero actual accepted Target bytes.
//! Entry is the single test below; this Module adds no production Interface.

use super::*;
use crate::owned_upstream::target::{TargetReadyInterest, TargetReadyOutcome};
use std::task::{Context, Poll};

struct OverAcceptingTarget {
    inner: MemoryTarget,
}

impl TargetIo for OverAcceptingTarget {
    fn capacity_witness(&self) -> TargetIoCapacityWitness {
        self.inner.capacity_witness()
    }

    fn cold_inventory(&self) -> Result<TargetIoColdInventory, TargetIoColdInventoryError> {
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
        self.inner.readiness(flow_id)?;
        TargetWriteCompletion::accepted(bytes.len() + 1)
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

#[test]
fn target_cannot_ack_more_bytes_than_the_slice_it_was_given() {
    let leg = committed_leg(2, 0x22, 0x42);
    let model = SessionModel::new(SessionRole::Owner, session_config(), leg);
    let supervisor = SessionSupervisor::new(model);
    let target_io = OverAcceptingTarget {
        inner: MemoryTarget::new(MemoryTargetConfig::new(4, 256, 512, 128, 128, 16).unwrap()),
    };
    let mut executor = OwnerTargetExecutor::new(
        OwnerTargetConfig::new(128, 1_024, 64).unwrap(),
        supervisor,
        target_io,
        source_config(),
    )
    .unwrap();
    let flow_id = SessionFlowId::new(1).unwrap();
    open_flow(&mut executor, leg, flow_id, target(443));

    let result = peer_frame(
        &mut executor,
        leg,
        Record::Data {
            flow_id,
            direction: Direction::ClientToTarget,
            offset: ByteOffset::new(0),
            payload: Bytes::from_static(b"abcdef"),
        },
    );

    assert!(matches!(
        result,
        Err(OwnerTargetError::InvalidTargetWriteAcceptance {
            flow_id: observed,
            accepted: 7,
            exposed: 6,
            offered: 6,
        }) if observed == flow_id
    ));
    assert_eq!(
        executor
            .snapshot()
            .session
            .session
            .outstanding_sink_offers(),
        1
    );
    assert_eq!(executor.target().inner.snapshot().write_accepted_bytes, 0);
}

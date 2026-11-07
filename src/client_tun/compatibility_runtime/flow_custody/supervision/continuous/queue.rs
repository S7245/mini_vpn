//! Static FIFO/held variants for the one existing dispatcher kernel. Owns no
//! actor, admission or independent ledger. Original queue transfers directly
//! into the Outcome's retained variant before any wake can run.
//! Entries: Backend::try_new and recv_custodied; tests: roles/tests/continuous.rs.
use super::*;
use crate::tcp_downlink_pump::{
    AsyncLeasedByteFlowQueue, ByteCustodyError, DownstreamBytePermit, LeasedQueueBatch,
};
use crate::tcp_egress::TcpEgressLease;

pub(in crate::client_tun::compatibility_runtime::flow_custody) enum Backend {
    Bytes(AsyncByteBoundedFlowQueue),
    Leased(AsyncLeasedByteFlowQueue),
}
pub(in crate::client_tun::compatibility_runtime::flow_custody) enum Held {
    Bytes(Option<ByteQueueBatch>),
    Leased(Option<LeasedQueueBatch>),
}
impl Held {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn new(queue: &Backend) -> Self {
        match queue {
            Backend::Bytes(_) => Self::Bytes(None),
            Backend::Leased(_) => Self::Leased(None),
        }
    }
    pub(super) fn as_ref(&self) -> Option<&ByteQueueBatch> {
        match self {
            Self::Bytes(v) => v.as_ref(),
            Self::Leased(v) => v.as_ref().map(|b| &b.data),
        }
    }
    pub(super) fn clear(&mut self) {
        match self {
            Self::Bytes(v) => *v = None,
            Self::Leased(v) => *v = None,
        }
    }
    pub(super) fn remaining_matches(&self, bytes: usize) -> bool {
        match self {
            Self::Bytes(_) => true,
            Self::Leased(Some(v)) => v.permit.remaining_bytes() == bytes,
            Self::Leased(None) => bytes == 0,
        }
    }
    pub(super) fn segment(
        &mut self,
        n: usize,
        remaining: usize,
    ) -> Result<Option<DownstreamBytePermit>, ByteCustodyError> {
        match self {
            Self::Bytes(Some(_)) => Ok(None),
            Self::Leased(Some(v)) if v.permit.remaining_bytes() == remaining && n <= remaining => v
                .permit
                .split_to(n)
                .map(Some)
                .ok_or(ByteCustodyError::Invalid),
            _ => Err(ByteCustodyError::Invalid),
        }
    }
}
impl Backend {
    pub(super) fn bind_failures(&self, clock: roles::FailureClock) -> bool {
        match self { Self::Bytes(q) => q.bind_native_failures(clock), Self::Leased(q) => q.bind_native_failures(clock) }
    }

    pub(super) fn try_new(
        engine: TcpRelayEngine, cfg: DownlinkBackpressureConfig, meter: Option<bytes::BackingMeter>,
    ) -> Result<Self, bytes::MeteredCopyError> {
        use bytes::MeteredCopyError as E;
        let queue = match engine {
            TcpRelayEngine::ContinuousPump => Self::Bytes(AsyncByteBoundedFlowQueue::new(CONTINUOUS_TCP_RELAY_QUEUE_BYTES).map_err(|_| E::TooLarge)?),
            TcpRelayEngine::PermitPump => Self::Leased(AsyncLeasedByteFlowQueue::new_custodied(CONTINUOUS_TCP_RELAY_QUEUE_BYTES).map_err(|_| E::TooLarge)?),
            TcpRelayEngine::OrderedEgressPermitPump => Self::Leased(AsyncLeasedByteFlowQueue::new_custodied_with_mode(
                native_tcp_permit_queue_capacity_bytes(cfg), DownstreamPermitReleaseMode::OnEgressDrain,
            ).map_err(|_| E::TooLarge)?),
            _ => return Err(E::Quarantined),
        };
        match &queue {
            Self::Bytes(q) => q.bind_native_meter_typed(meter)?,
            Self::Leased(q) => q.bind_native_meter_typed(meter)?,
        }
        Ok(queue)
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn bytes_queue(
        &self,
    ) -> Option<&AsyncByteBoundedFlowQueue> {
        match self {
            Self::Bytes(q) => Some(q),
            _ => None,
        }
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn leased_queue(
        &self,
    ) -> Option<&AsyncLeasedByteFlowQueue> {
        match self {
            Self::Leased(q) => Some(q),
            _ => None,
        }
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) async fn wait_read_len(
        &self,
        limit: usize,
    ) -> Result<usize, crate::tcp_downlink_pump::AsyncByteQueueWait> {
        match self {
            Self::Bytes(q) => q.wait_read_len(limit).await,
            Self::Leased(q) => q.wait_read_len(limit).await,
        }
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) async fn push_custodied<
        P: crate::tcp_downlink_pump::CustodiedPayload,
    >(
        &self,
        pending: &mut Option<P>,
    ) -> Result<(), ByteCustodyError> {
        match self {
            Self::Bytes(q) => q.push_custodied(pending).await,
            Self::Leased(q) => q.push_custodied(pending).await,
        }
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) async fn close(&self) {
        match self {
            Self::Bytes(q) => q.close().await,
            Self::Leased(q) => q.close().await,
        }
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) async fn recv_custodied(
        &self,
        limit: usize,
        held: &mut Held,
    ) -> Result<bool, ByteCustodyError> {
        match (self, held) {
            (Self::Bytes(q), Held::Bytes(slot)) => q.recv_custodied(limit, slot).await,
            (Self::Leased(q), Held::Leased(slot)) => q.recv_custodied(limit, slot).await,
            _ => Err(ByteCustodyError::Invalid),
        }
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn finish_stock_callbacks(&self) -> Result<bool, ByteCustodyError> {
        match self {
            Self::Bytes(q) if q.has_output_stock() => q.dispose_custodied().map(|v| v.is_some()),
            Self::Leased(q) if q.has_output_stock() => q.dispose_custodied().map(|v| v.is_some()),
            _ => Err(ByteCustodyError::Invalid),
        }
    }
    pub(super) fn dispose_custodied(&self) -> Result<Option<ByteQueueCustody>, ByteCustodyError> {
        match self {
            Self::Bytes(q) if q.has_output_stock() => q.dispose_data().map(|value| value.map(|data| data.custody())),
            Self::Bytes(q) => q.dispose_custodied(),
            Self::Leased(q) if q.has_output_stock() => q.dispose_data().map(|value| value.map(|data| data.custody())),
            Self::Leased(q) => q.dispose_custodied(),
        }
    }
}

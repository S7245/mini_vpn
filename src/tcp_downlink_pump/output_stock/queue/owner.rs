//! Static access to the original FIFO owner. Owns no second queue, budget or
//! callback authority. Complete preflight and publication stay under that
//! queue's lock; the caller retains its linear read claim across cancellation.
//! Tests: output_stock/ledger/tests/{read_port,pending_read,online_retirement}.
use super::*;
use crate::tcp_downlink_pump::native_wait::Waiters;
use tokio::sync::Notify;

#[derive(Clone)]
pub(super) enum Owner {
    Bytes(Arc<AsyncByteBoundedFlowQueueInner>),
    Leased(crate::tcp_downlink_pump::leased::stock::StockQueue),
}
impl Owner {
    pub(super) fn from_return(value: &QueueReturn) -> Option<Self> {
        match value {
            QueueReturn::Bytes(weak) => weak.upgrade().map(Self::Bytes),
            QueueReturn::Leased(weak) => weak.upgrade().map(Self::Leased),
        }
    }
    pub(super) fn same(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Bytes(a), Self::Bytes(b)) => Arc::ptr_eq(a, b),
            (Self::Leased(a), Self::Leased(b)) => a.same(b), _ => false,
        }
    }
    pub(super) fn read(&self) -> &ReadAdmission {
        match self { Self::Bytes(q) => &q.read, Self::Leased(q) => q.read() }
    }
    pub(super) fn waiters(&self) -> Option<&Waiters> {
        match self { Self::Bytes(q) => q.waiters.get(), Self::Leased(q) => q.waiters() }
    }
    pub(super) fn stock(&self) -> Option<&StockKey> {
        match self { Self::Bytes(q) => q.stock.get(), Self::Leased(q) => q.stock() }
    }
    pub(super) fn not_empty(&self) -> &Notify {
        match self { Self::Bytes(q) => &q.not_empty, Self::Leased(q) => q.not_empty() }
    }
    pub(super) fn not_full(&self) -> &Notify {
        match self { Self::Bytes(q) => &q.not_full, Self::Leased(q) => q.not_full() }
    }
    pub(super) fn begin(&self) {
        match self { Self::Bytes(q) => q.start.begin(), Self::Leased(q) => q.begin() }
    }
    pub(super) fn resize(&self, id: u64, old: usize, next: usize) -> bool {
        match self { Self::Bytes(q) => q.read.resize(id, old, next), Self::Leased(q) => q.resize(id, old, next) }
    }
    pub(super) fn release(&self, id: u64, max: usize, committed: bool) -> bool {
        match self { Self::Bytes(q) => q.read.release(id, max), Self::Leased(q) => q.release(id, max, committed) }
    }
    pub(super) async fn open(&self, receiver: &QuietScope, owner: &QuietScope) -> Result<(), StockWaitError> {
        let queue = match self { Self::Bytes(queue) => queue, Self::Leased(queue) => return queue.open(owner) };
        let state = queue.state.lock().await;
        if state.closed || self.waiters().is_none_or(|w| w.is_closed()) { return Err(StockWaitError::Closed); }
        if state.custody.interrupted || state.custody.frozen {
            receiver.mark_quarantined(); owner.mark_quarantined();
            return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        }
        if self.read().is_active() { return Err(StockWaitError::Pressure); }
        Ok(())
    }
    pub(super) async fn prepare_claim(&self, max: usize, meter: &BackingMeter, receiver: &QuietScope, owner: &QuietScope)
        -> Result<Option<ReadClaim>, StockWaitError> {
        let queue = match self {
            Self::Bytes(queue) => queue,
            Self::Leased(queue) => return queue.prepare(max, meter, owner).map(|grant| grant.map(|(id, max)| ReadClaim {
                queue: self.clone(), id, max, meter: meter.clone(), quiet: owner.clone(),
                armed: true, committed: false, notify_producer: false,
            })),
        };
        let mut state = queue.state.lock().await;
        if state.closed || self.waiters().is_none_or(|w| w.is_closed()) { return Err(StockWaitError::Closed); }
        if state.custody.interrupted || state.custody.frozen {
            receiver.mark_quarantined(); owner.mark_quarantined();
            return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        }
        let available = max.min(state.queue.capacity_bytes.saturating_sub(state.queue.queued_bytes));
        if self.read().is_active() { return Err(StockWaitError::Pressure); }
        if available == 0 { return Ok(None); }
        match state.queue.chunks.prepare(Some(meter.clone())) {
            Ok(()) => {},
            Err(MeteredCopyError::BudgetFull) if !state.queue.chunks.is_empty() => return Ok(None),
            Err(error) => return Err(StockWaitError::Admission(error)),
        }
        let id = self.read().next.fetch_update(Ordering::AcqRel, Ordering::Acquire, |id| id.checked_add(1))
            .map_err(|_| {
                receiver.mark_quarantined(); owner.mark_quarantined();
                StockWaitError::Admission(MeteredCopyError::Quarantined)
            })? + 1;
        let claim = ReadClaim {
            queue: self.clone(), id, max: available, meter: meter.clone(), quiet: owner.clone(),
            armed: true, committed: false, notify_producer: false,
        };
        self.read().reserved.store(available, Ordering::Relaxed);
        self.read().active.store(id, Ordering::Release);
        drop(state);
        Ok(Some(claim))
    }
    pub(super) async fn publish(&self, pending: &mut Option<OwnedPayload>, claim: &mut ReadClaim)
        -> Result<(), ByteCustodyError> {
        let queue = match self {
            Self::Bytes(queue) => queue,
            Self::Leased(queue) => return queue.publish(pending, (claim.id, claim.max), &claim.meter, &claim.quiet, &mut claim.committed),
        };
        let meter = &claim.meter;
        let quiet = &claim.quiet;
        let mut state = queue.state.lock().await;
        let Some(payload) = pending.as_ref() else {
            return Err(ByteCustodyError::Invalid);
        };
        if state.custody.interrupted
            || state.custody.frozen
            || !state.queue.can_accept(payload.len())
            || state.queue.chunks.len() >= state.queue.chunks.capacity()
            || !state
                .queue
                .chunks
                .meter()
                .is_some_and(|m| m.same_authority(meter))
        {
            quiet.mark_quarantined();
            return Err(ByteCustodyError::Invalid);
        }
        let sources = state.custody.sources.checked_add(1);
        let emitted = state.custody.emitted.checked_add(payload.len());
        let (Some(sources), Some(emitted)) = (sources, emitted) else {
            quiet.mark_quarantined();
            return Err(ByteCustodyError::Invalid);
        };
        let mutation =
            MeteredMutation::begin(Some(meter)).map_err(ByteCustodyError::Capacity)?;
        // A close stops new admission; this already granted token keeps its
        // actual tail promise. All fallible checks precede ownership transfer.
        state.custody.interrupted = true;
        let payload = pending.take().ok_or(ByteCustodyError::Invalid)?;
        let allocation = payload.capacity();
        state.queue.queued_bytes += payload.len();
        state.queue.high_water_bytes = state.queue.high_water_bytes.max(state.queue.queued_bytes);
        state.queue.chunks.push_back(ByteChunk {
            bytes: payload.into_custodied_bytes(),
            allocation: Some(allocation),
        });
        state.custody.sources = sources;
        state.custody.emitted = emitted;
        state.custody.interrupted = false;
        claim.committed = true;
        drop(state);
        self.not_empty().notify_one();
        mutation.commit();
        Ok(())
    }
}

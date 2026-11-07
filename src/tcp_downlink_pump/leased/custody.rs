//! Same-lock custody for the original leased FIFO. Owns source/batch/storage and
//! actual lease release accounting; no actor, Flow, Main or Node authority.
//! Entry: `new_custodied`, push/recv/dispose_custodied and physical_zero.
//! Tests: leased/tests/custody.rs. Legacy entry points retain their old policy.
use super::*;
use crate::tcp_downlink_pump::{ByteCustodyError, ByteQueueBatch, MeteredMutation};

pub(crate) struct LeasedQueueBatch {
    pub(crate) data: ByteQueueBatch,
    pub(crate) permit: DownstreamBytePermit,
}
impl AsyncLeasedByteFlowQueue {
    pub(crate) fn bind_native_failures(&self, clock: crate::shared::errors::FailureClock) -> bool {
        self.inner.waiters.get().is_none_or(|w| w.bind(clock))
    }
    pub(crate) fn bind_native_meter(&self, meter: Option<bytes::BackingMeter>) -> Option<()> {
        self.bind_native_meter_typed(meter).ok()
    }
    pub(crate) fn bind_native_meter_typed(&self, meter: Option<bytes::BackingMeter>) -> Result<(), bytes::MeteredCopyError> {
        let _cold = self.inner.start.cold().ok_or(bytes::MeteredCopyError::Quarantined)?;
        let mut state = self.inner.lock_state();
        if let Some(meter) = meter.as_ref().filter(|m| m.budget_limit().is_some()) {
            if self.inner.waiters.get().is_none() {
                self.inner
                    .waiters
                    .set(super::super::native_wait::Waiters::prepare(meter)?)
                    .map_err(|_| bytes::MeteredCopyError::Quarantined)?;
            }
        }
        state.chunks.bind(meter).ok_or(bytes::MeteredCopyError::Quarantined)?;
        self.inner.retain_meter(state.chunks.meter());
        Ok(())
    }
    /// Identity check for the live D16 Root/SocketCtx binding. A value-only
    /// budget snapshot cannot distinguish another budget with equal counters.
    pub(crate) fn d16_uses_budget(&self, budget: &D16GlobalByteBudget) -> bool {
        self.inner
            .global_budget
            .as_ref()
            .is_some_and(|actual| Arc::ptr_eq(&actual.inner, &budget.inner))
    }

    /// D16 quota witness for a stopped single flow. This is deliberately a
    /// queue-local receipt: other flows may still hold the shared budget.
    /// The caller must have joined every actor and disposed its local permits
    /// before accepting this read-only terminal check.
    pub(crate) fn d16_terminal_quota_zero(&self, budget: &D16GlobalByteBudget) -> bool {
        if !self.d16_uses_budget(budget) {
            return false;
        }
        let state = self.inner.lock_state();
        !state.custodied
            && state.release_mode == DownstreamPermitReleaseMode::OnEgressDrain
            && state.closure.is_closed()
            && state.chunks.is_empty()
            && state.queued_bytes == 0
            && state.leased_bytes == 0
            && state.reserved_bytes == 0
    }

    pub(crate) fn new_custodied(capacity: usize) -> Result<Self, ByteQueueConfigError> {
        Self::new_custodied_with_mode(capacity, DownstreamPermitReleaseMode::OnAdmission)
    }
    pub(crate) fn new_custodied_with_mode(
        capacity: usize,
        mode: DownstreamPermitReleaseMode,
    ) -> Result<Self, ByteQueueConfigError> {
        let queue = Self::new_with_release_mode(capacity, mode)?;
        queue.inner.lock_state().custodied = true;
        Ok(queue)
    }
    pub(crate) fn owns_permit(&self, permit: &DownstreamBytePermit, bytes: usize) -> bool {
        if bytes == 0 || !Arc::ptr_eq(&self.inner, &permit.inner) || permit.remaining_bytes != bytes
        {
            return false;
        }
        let state = self.inner.lock_state();
        state.custodied
            && !state.custody.interrupted
            && state.release_mode == permit.release_mode
            && self.inner.global_budget.is_none()
    }
    pub(crate) fn d16_owns_permit(&self, permit: &DownstreamBytePermit, bytes: usize) -> bool {
        if bytes == 0 || !Arc::ptr_eq(&self.inner, &permit.inner) || permit.remaining_bytes != bytes
        {
            return false;
        }
        let state = self.inner.lock_state();
        !state.custodied
            && self.inner.global_budget.is_some()
            && state.release_mode == DownstreamPermitReleaseMode::OnEgressDrain
            && permit.release_mode == state.release_mode
    }
    pub(crate) fn custodied_release_mode(&self) -> Option<DownstreamPermitReleaseMode> {
        let state = self.inner.lock_state();
        (state.custodied && !state.custody.interrupted && self.inner.global_budget.is_none())
            .then_some(state.release_mode)
    }
    pub(crate) fn same_queue(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
    pub(crate) fn physical_zero(&self) -> bool {
        let state = self.inner.lock_state();
        state.custodied
            && !self
                .inner
                .meter
                .get()
                .is_some_and(bytes::BackingMeter::is_quarantined)
            && !state.custody.interrupted
            && state.custody.frozen
            && state.local_disposed
            && state.chunks.capacity() == 0
            && state
                .custody
                .consumed
                .checked_add(state.custody.discarded_bytes)
                == Some(state.custody.emitted)
            && state.custody.discarded_bytes == state.custody.frozen_bytes
            && state.custody.discarded_chunks == state.custody.frozen_chunks
            && state.chunks.is_empty()
            && state.queued_bytes == 0
            && state.reserved_bytes == 0
            && state.leased_bytes == 0
            && state.released == state.custody.consumed
    }
    pub(crate) async fn push_custodied<P: crate::tcp_downlink_pump::payload::CustodiedPayload>(
        &self,
        pending: &mut Option<P>,
    ) -> Result<(), ByteCustodyError> {
        self.inner.start.begin();
        if self.inner.stock.get().is_some() || pending.as_ref().is_some_and(|p| p.has_stock()) {
            return Err(super::super::payload::reject_raw_stock(
                self.inner.waiters.get().map(|w| w.quiet_scope()), pending));
        }
        match self.inner.waiters.get() {
            Some(w) => w
                .run(
                    super::super::native_wait::Role::Producer,
                    self.push_custodied_inner(pending),
                )
                .await
                .map_err(|e| match e {
                    super::super::native_wait::RunError::Closed => ByteCustodyError::Rejected,
                    super::super::native_wait::RunError::Quarantined => ByteCustodyError::Invalid,
                })?,
            None => self.push_custodied_inner(pending).await,
        }
    }
    async fn push_custodied_inner<P: crate::tcp_downlink_pump::payload::CustodiedPayload>(
        &self,
        pending: &mut Option<P>,
    ) -> Result<(), ByteCustodyError> {
        let mut state = self.inner.lock_state();
        if !state.custodied || state.custody.interrupted || state.custody.frozen {
            return Err(ByteCustodyError::Invalid);
        }
        let payload = pending.as_ref().ok_or(ByteCustodyError::Invalid)?;
        if payload.meter().is_some_and(|m| m.budget_limit().is_some())
            && self.inner.waiters.get().is_none()
        {
            return Err(ByteCustodyError::Invalid);
        }
        if payload.is_empty()
            || state.closure.is_closed()
            || payload.len() > state.available_bytes()
        {
            return Err(ByteCustodyError::Rejected);
        }
        let sources = state.custody.sources.checked_add(1);
        let emitted = state.custody.emitted.checked_add(payload.len());
        let (Some(sources), Some(emitted)) = (sources, emitted) else {
            return Err(ByteCustodyError::Invalid);
        };
        state
            .chunks
            .prepare(payload.meter())
            .map_err(ByteCustodyError::Capacity)?;
        self.inner.retain_meter(state.chunks.meter());
        let mutation =
            MeteredMutation::begin(state.chunks.meter()).map_err(ByteCustodyError::Capacity)?;
        state.custody.interrupted = true;
        let payload = pending.take().ok_or(ByteCustodyError::Invalid)?;
        let allocation = payload.capacity();
        state.queued_bytes += payload.len();
        state.chunks.push_back(ByteChunk {
            bytes: payload.into_custodied_bytes(),
            allocation: Some(allocation),
        });
        state.note_high_water();
        state.custody.sources = sources;
        state.custody.emitted = emitted;
        state.custody.interrupted = false;
        drop(state);
        // A consumer may run concurrently with notify. Publish the completed
        // ledger first; only an actual unwind poisons the committed transfer.
        if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.inner.not_empty.notify_one()
        })) {
            self.inner.lock_state().custody.interrupted = true;
            std::panic::resume_unwind(panic);
        }
        mutation.commit();
        Ok(())
    }
    pub(crate) async fn recv_custodied(
        &self,
        limit: usize,
        held: &mut Option<LeasedQueueBatch>,
    ) -> Result<bool, ByteCustodyError> {
        self.inner.start.begin();
        match self.inner.waiters.get() {
            Some(w) => w
                .run(
                    super::super::native_wait::Role::Consumer,
                    self.recv_custodied_inner(limit, held),
                )
                .await
                .map_err(|e| match e {
                    super::super::native_wait::RunError::Closed => ByteCustodyError::Rejected,
                    super::super::native_wait::RunError::Quarantined => ByteCustodyError::Invalid,
                })?,
            None => self.recv_custodied_inner(limit, held).await,
        }
    }
    async fn recv_custodied_inner(
        &self,
        limit: usize,
        held: &mut Option<LeasedQueueBatch>,
    ) -> Result<bool, ByteCustodyError> {
        if held.is_some() || limit == 0 {
            return Err(ByteCustodyError::Invalid);
        }
        loop {
            let empty = self.inner.not_empty.notified();
            let capacity = self.inner.not_full.notified();
            tokio::pin!(empty, capacity);
            empty.as_mut().enable();
            capacity.as_mut().enable();
            {
                let mut state = self.inner.lock_state();
                if !state.custodied || state.custody.interrupted || state.custody.frozen {
                    return Err(ByteCustodyError::Invalid);
                }
                if state.chunks.is_empty() != (state.queued_bytes == 0) {
                    state.custody.interrupted = true;
                    return Err(ByteCustodyError::Invalid);
                }
                if state.queued_bytes != 0 {
                    let n = state.chunks.next_len(limit, state.queued_bytes);
                    let count = state.custody.batches.checked_add(1);
                    let total = state.custody.consumed.checked_add(n);
                    let (Some(count), Some(total)) = (count, total) else {
                        return Err(ByteCustodyError::Invalid);
                    };
                    let mutation = MeteredMutation::begin(state.chunks.meter())
                        .map_err(ByteCustodyError::Capacity)?;
                    let full_descriptors = state.chunks.len() == state.chunks.capacity();
                    let descriptors = state.chunks.len();
                    state.custody.interrupted = true;
                    let (bytes, allocation) =
                        state.pop_owned(limit).ok_or(ByteCustodyError::Invalid)?;
                    *held = Some(LeasedQueueBatch {
                        permit: DownstreamBytePermit {
                            inner: self.inner.clone(),
                            remaining_bytes: bytes.len(),
                            release_mode: state.release_mode,
                        },
                        data: ByteQueueBatch { bytes, allocation },
                    });
                    if held.as_ref().is_none_or(|batch| {
                        batch.data.bytes.len() != n || batch.data.allocation < n
                    }) {
                        return Err(ByteCustodyError::Invalid);
                    }
                    state.custody.batches = count;
                    state.custody.consumed = total;
                    state.custody.interrupted = false;
                    let freed_descriptor = self.inner.stock.get().is_some() && full_descriptors && state.chunks.len() < descriptors;
                    drop(state);
                    // Only FIFO capacity changed; held bytes and the original
                    // downstream permit remain independently charged/leased.
                    if freed_descriptor { self.inner.not_full.notify_waiters(); }
                    mutation.commit();
                    return Ok(true);
                }
                if state.closure.is_closed() && state.reserved_bytes == 0 {
                    return Ok(false);
                }
            }
            tokio::select! { _ = empty => {}, _ = capacity => {} }
        }
    }
    // Called only after all original producers/actors joined. Main leases are
    // deliberately not refunded here; physical_zero checks their actual return.
    pub(crate) fn dispose_custodied(&self) -> Result<Option<ByteQueueCustody>, ByteCustodyError> {
        let _quiet = self.inner.waiters.get().map(|w| w.quiet_scope());
        let Some(data) = self.dispose_data()? else { return Ok(None); };
        if self.inner.waiters.get().is_some_and(|w| !w.finish()) { return Ok(None); }
        Ok(Some(data.custody()))
    }
    pub(crate) fn dispose_data(&self) -> Result<Option<super::super::ByteQueueDataDisposition>, ByteCustodyError> {
        let _quiet = self.inner.waiters.get().map(|w| w.quiet_scope());
        self.inner.start.begin();
        if self.inner.read.is_active() {
            return if self.inner.waiters.get().is_some_and(|w| w.is_quarantined()) {
                Err(ByteCustodyError::Invalid)
            } else { Ok(None) };
        }
        let turn = match self.inner.waiters.get() {
            Some(w) => match w.try_disposal_turn() { Some(turn) => Some(turn), None => return Ok(None) },
            None => None,
        };
        let mut state = self.inner.lock_state();
        if let Some(w) = self.inner.waiters.get() {
            let phase = w.disposal_state();
            if phase != 0 {
                if !state.custody.interrupted || !state.custody.frozen || phase > 2 {
                    w.mark_quarantined(); return Err(ByteCustodyError::Invalid);
                }
                if phase == 1 { return Ok(None); }
                if !w.commit_disposal() { return Err(ByteCustodyError::Invalid); }
                state.custody.interrupted = false;
            }
        }
        if !state.custodied || state.custody.interrupted {
            return Err(ByteCustodyError::Invalid);
        }
        if !state.custody.frozen {
            let actual = state
                .chunks
                .iter()
                .try_fold(0usize, |n, chunk| n.checked_add(chunk.bytes.len()));
            if actual != Some(state.queued_bytes)
                || state.reserved_bytes != 0
                || state.custody.consumed.checked_sub(state.released) != Some(state.leased_bytes)
            {
                state.custody.interrupted = true;
                return Err(ByteCustodyError::Invalid);
            }
            state.closure = LeasedByteQueueClosure::Terminal(LeasedByteQueueTerminalCause {
                direction: "root",
                reason: "custody_disposal",
            });
            state.custody.frozen_bytes = state.queued_bytes;
            state.custody.frozen_chunks = state.chunks.len();
            state.custody.metadata_slots = state.chunks.capacity();
            state.custody.metadata_bytes = state
                .chunks
                .capacity()
                .checked_mul(std::mem::size_of::<ByteChunk>())
                .ok_or(ByteCustodyError::Invalid)?;
            state.custody.frozen = true;
        }
        for _ in 0..32 {
            let Some(chunk) = state.chunks.front() else {
                if state.queued_bytes != 0
                    || state.custody.discarded_bytes != state.custody.frozen_bytes
                    || state.custody.discarded_chunks != state.custody.frozen_chunks
                    || state
                        .custody
                        .consumed
                        .checked_add(state.custody.discarded_bytes)
                        != Some(state.custody.emitted)
                {
                    state.custody.interrupted = true;
                    return Err(ByteCustodyError::Invalid);
                }
                state.custody.interrupted = true;
                state.chunks = Chunks::default();
                state.local_disposed = true;
                state.custody.interrupted = false;
                let receipt = state.custody;
                drop(state);
                drop(turn);
                return Ok(Some(super::super::ByteQueueDataDisposition(receipt)));
            };
            let n = chunk.bytes.len();
            let Some(allocation) = chunk.allocation else {
                state.custody.interrupted = true;
                return Err(ByteCustodyError::Invalid);
            };
            let bytes = state.custody.discarded_bytes.checked_add(n);
            let count = state.custody.discarded_chunks.checked_add(1);
            let storage = state.custody.discarded_allocation.checked_add(allocation);
            let queued = state.queued_bytes.checked_sub(n);
            let (Some(bytes), Some(count), Some(storage), Some(queued)) =
                (bytes, count, storage, queued)
            else {
                state.custody.interrupted = true;
                return Err(ByteCustodyError::Invalid);
            };
            if let Some(w) = self.inner.waiters.get() {
                if !w.begin_disposal() { return Err(ByteCustodyError::Invalid); }
            }
            state.custody.interrupted = true;
            let retired = state.chunks.pop_front();
            state.queued_bytes = queued;
            state.custody.discarded_bytes = bytes;
            state.custody.discarded_chunks = count;
            state.custody.discarded_allocation = storage;
            if let Some(w) = self.inner.waiters.get() {
                drop(state);
                let Some(retired) = retired else { w.mark_quarantined(); return Err(ByteCustodyError::Invalid); };
                if !w.dispose_chunk(retired) { return Err(ByteCustodyError::Invalid); }
                let Some(guard) = Some(self.inner.lock_state()) else { return Ok(None); };
                state = guard;
                if !state.custody.interrupted || !state.custody.frozen || !w.commit_disposal() {
                    w.mark_quarantined(); return Err(ByteCustodyError::Invalid);
                }
            } else { drop(retired); }
            state.custody.interrupted = false;
        }
        Ok(None)
    }
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

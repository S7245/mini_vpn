mod payload;
use bytes::{Bytes, BytesMut};
pub(crate) use payload::{CustodiedBytes, CustodiedPayload, OwnedPayload};
pub(crate) use payload::{MeteredMutation, PreparedPayload};
mod storage;
mod output_stock;
pub(crate) use output_stock::{OutputStock, StockProducer, StockReadOperation, OutputSpace, StockKey, StockWaitError};
use std::sync::Arc;
use std::time::{Duration, Instant};
use storage::Chunks;
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteQueueSnapshot {
    pub queued_bytes: usize,
    pub capacity_bytes: usize,
    pub high_water_bytes: usize,
    pub chunks: usize,
}

impl ByteQueueSnapshot {
    pub fn available_bytes(self) -> usize {
        self.capacity_bytes.saturating_sub(self.queued_bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteQueueConfigError {
    ZeroCapacity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByteQueuePushError {
    EmptyChunk,
    ChunkExceedsCapacity { len: usize, capacity: usize },
    Full { len: usize, available: usize },
    Closed,
    Capacity(bytes::MeteredCopyError),
}

#[derive(Debug)]
struct ByteChunk {
    bytes: CustodiedBytes,
    // Legacy Bytes have opaque backing. Custodied Vec transfers retain its
    // actual allocation through any partial front slice, without another heap.
    allocation: Option<usize>,
}

#[derive(Debug)]
pub struct ByteBoundedFlowQueue {
    chunks: Chunks,
    queued_bytes: usize,
    capacity_bytes: usize,
    high_water_bytes: usize,
}

impl ByteBoundedFlowQueue {
    pub fn new(capacity_bytes: usize) -> Result<Self, ByteQueueConfigError> {
        if capacity_bytes == 0 {
            return Err(ByteQueueConfigError::ZeroCapacity);
        }
        Ok(Self {
            chunks: Chunks::default(),
            queued_bytes: 0,
            capacity_bytes,
            high_water_bytes: 0,
        })
    }

    pub fn snapshot(&self) -> ByteQueueSnapshot {
        ByteQueueSnapshot {
            queued_bytes: self.queued_bytes,
            capacity_bytes: self.capacity_bytes,
            high_water_bytes: self.high_water_bytes,
            chunks: self.chunks.len(),
        }
    }

    pub fn can_accept(&self, len: usize) -> bool {
        len > 0 && len <= self.capacity_bytes.saturating_sub(self.queued_bytes)
    }

    pub fn try_push(&mut self, bytes: Bytes) -> Result<(), ByteQueuePushError> {
        let len = bytes.len();
        if len == 0 {
            return Err(ByteQueuePushError::EmptyChunk);
        }
        if len > self.capacity_bytes {
            return Err(ByteQueuePushError::ChunkExceedsCapacity {
                len,
                capacity: self.capacity_bytes,
            });
        }
        let available = self.capacity_bytes.saturating_sub(self.queued_bytes);
        if len > available {
            return Err(ByteQueuePushError::Full { len, available });
        }
        self.chunks
            .prepare(bytes.backing_meter())
            .map_err(ByteQueuePushError::Capacity)?;
        self.queued_bytes = self.queued_bytes.saturating_add(len);
        self.high_water_bytes = self.high_water_bytes.max(self.queued_bytes);
        self.chunks.push_back(ByteChunk {
            bytes: bytes.into(),
            allocation: None,
        });
        Ok(())
    }

    pub fn pop_up_to(&mut self, limit: usize) -> Option<Bytes> {
        if self.chunks.front().is_some_and(|c| c.bytes.has_stock()) {
            if let Some(chunk) = self.chunks.front() { chunk.bytes.quarantine_stock(); }
            return None;
        }
        self.pop_owned(limit).and_then(|(bytes, _)| bytes.try_into_raw_legacy().ok())
    }

    fn pop_owned(&mut self, limit: usize) -> Option<(CustodiedBytes, usize)> {
        if limit == 0 {
            return None;
        }
        if let Some((bytes, capacity)) = self.chunks.pop_native(limit) {
            self.queued_bytes -= bytes.len();
            return Some((bytes, capacity));
        }
        let mut remaining = limit;
        let mut out = BytesMut::new();
        while remaining > 0 {
            let Some(mut chunk) = self.chunks.pop_front() else {
                break;
            };
            let take = chunk.bytes.len().min(remaining);
            out.extend_from_slice(&chunk.bytes[..take]);
            self.queued_bytes = self.queued_bytes.saturating_sub(take);
            remaining -= take;
            if take < chunk.bytes.len() {
                chunk.bytes = chunk.bytes.slice(take..);
                self.chunks.push_front(chunk);
                break;
            }
        }
        if out.is_empty() {
            None
        } else {
            let allocation = out.capacity();
            Some((out.freeze().into(), allocation))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContinuousPumpStats {
    pub read_armed_with_capacity: u64,
    pub remote_read_chunks: u64,
    pub remote_read_bytes: u64,
    pub drained_bytes: u64,
    pub queue_full_waits: u64,
    pub max_remote_read_gap_with_capacity: Duration,
    pub max_queue_bytes: usize,
}

#[derive(Debug, Default)]
pub struct ContinuousPumpProbe {
    last_read_with_capacity_at: Option<Instant>,
    stats: ContinuousPumpStats,
}

impl ContinuousPumpProbe {
    pub fn note_read_armed(&mut self, queue: ByteQueueSnapshot, read_len: usize) {
        if read_len > 0 && queue.available_bytes() >= read_len {
            self.stats.read_armed_with_capacity =
                self.stats.read_armed_with_capacity.saturating_add(1);
        }
        self.note_queue(queue);
    }

    pub fn note_remote_read(&mut self, now: Instant, bytes: usize, queue_had_capacity: bool) {
        if bytes == 0 {
            return;
        }
        self.stats.remote_read_chunks = self.stats.remote_read_chunks.saturating_add(1);
        self.stats.remote_read_bytes = self.stats.remote_read_bytes.saturating_add(bytes as u64);
        if queue_had_capacity {
            if let Some(last) = self.last_read_with_capacity_at {
                self.stats.max_remote_read_gap_with_capacity = self
                    .stats
                    .max_remote_read_gap_with_capacity
                    .max(now.saturating_duration_since(last));
            }
            self.last_read_with_capacity_at = Some(now);
        }
    }

    pub fn note_drain(&mut self, bytes: usize, queue: ByteQueueSnapshot) {
        self.stats.drained_bytes = self.stats.drained_bytes.saturating_add(bytes as u64);
        self.note_queue(queue);
    }

    pub fn note_queue_full_wait(&mut self, queue: ByteQueueSnapshot) {
        self.stats.queue_full_waits = self.stats.queue_full_waits.saturating_add(1);
        self.note_queue(queue);
    }

    pub fn stats(&self) -> ContinuousPumpStats {
        self.stats
    }

    fn note_queue(&mut self, queue: ByteQueueSnapshot) {
        self.stats.max_queue_bytes = self
            .stats
            .max_queue_bytes
            .max(queue.queued_bytes)
            .max(queue.high_water_bytes);
    }
}

impl Default for ContinuousPumpStats {
    fn default() -> Self {
        Self {
            read_armed_with_capacity: 0,
            remote_read_chunks: 0,
            remote_read_bytes: 0,
            drained_bytes: 0,
            queue_full_waits: 0,
            max_remote_read_gap_with_capacity: Duration::ZERO,
            max_queue_bytes: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncByteQueueWait {
    Closed,
    Quarantined,
}

#[derive(Debug, Clone)]
pub struct AsyncByteBoundedFlowQueue {
    inner: Arc<AsyncByteBoundedFlowQueueInner>,
}

#[derive(Debug)]
struct AsyncByteBoundedFlowQueueInner {
    start: native_wait::Start,
    waiters: std::sync::OnceLock<native_wait::Waiters>,
    state: Mutex<AsyncByteBoundedFlowQueueState>,
    stock: std::sync::OnceLock<output_stock::StockKey>,
    read: output_stock::ReadAdmission,
    not_empty: Notify,
    not_full: Notify,
}

#[derive(Debug)]
struct AsyncByteBoundedFlowQueueState {
    queue: ByteBoundedFlowQueue,
    closed: bool,
    custody: ByteQueueCustody,
}

/// Independent source-chunk and dispatcher-batch domains. No one-to-one
/// relation is assumed: pop coalesces and slices original chunks.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ByteQueueCustody {
    pub(crate) sources: usize,
    pub(crate) emitted: usize,
    pub(crate) batches: usize,
    pub(crate) consumed: usize,
    pub(crate) discarded_chunks: usize,
    pub(crate) discarded_bytes: usize,
    pub(crate) discarded_allocation: usize,
    pub(crate) frozen_bytes: usize,
    pub(crate) frozen_chunks: usize,
    pub(crate) metadata_slots: usize,
    pub(crate) metadata_bytes: usize,
    frozen: bool,
    interrupted: bool,
}

/// Physical DATA/metadata disposal only. Callbacks and external loans remain.
pub(crate) struct ByteQueueDataDisposition(ByteQueueCustody);
impl ByteQueueDataDisposition {
    pub(crate) fn custody(&self) -> ByteQueueCustody { self.0 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ByteCustodyError {
    Rejected,
    Invalid,
    Capacity(bytes::MeteredCopyError),
}

/// The actual coalesced batch and its original BytesMut allocation move into
/// the consumer's retained slot before any capacity waiter can be woken.
pub(crate) struct ByteQueueBatch {
    pub(crate) bytes: CustodiedBytes,
    pub(crate) allocation: usize,
}

impl AsyncByteBoundedFlowQueue {
    pub(crate) fn bind_native_meter(&self, meter: Option<bytes::BackingMeter>) -> Option<()> {
        self.bind_native_meter_typed(meter).ok()
    }
    pub(crate) fn bind_native_meter_typed(&self, meter: Option<bytes::BackingMeter>) -> Result<(), bytes::MeteredCopyError> {
        let _cold = self.inner.start.cold().ok_or(bytes::MeteredCopyError::Quarantined)?;
        self.bind_native_meter_cold(meter)
    }
    fn bind_native_meter_cold(&self, meter: Option<bytes::BackingMeter>) -> Result<(), bytes::MeteredCopyError> {
        let mut state = self.inner.state.try_lock().map_err(|_| bytes::MeteredCopyError::Quarantined)?;
        if let Some(meter) = meter.as_ref().filter(|m| m.budget_limit().is_some()) {
            if self.inner.waiters.get().is_none() {
                self.inner
                    .waiters
                    .set(native_wait::Waiters::prepare(meter)?)
                    .map_err(|_| bytes::MeteredCopyError::Quarantined)?;
            }
        }
        state.queue.chunks.bind(meter).ok_or(bytes::MeteredCopyError::Quarantined)
    }
    pub(crate) fn bind_native_failures(&self, clock: crate::shared::errors::FailureClock) -> bool {
        self.inner.waiters.get().is_none_or(|w| w.bind(clock))
    }
    pub fn new(capacity_bytes: usize) -> Result<Self, ByteQueueConfigError> {
        Ok(Self {
            inner: Arc::new(AsyncByteBoundedFlowQueueInner {
                start: native_wait::Start::default(),
                waiters: std::sync::OnceLock::new(),
                state: Mutex::new(AsyncByteBoundedFlowQueueState {
                    queue: ByteBoundedFlowQueue::new(capacity_bytes)?,
                    closed: false,
                    custody: ByteQueueCustody::default(),
                }),
                stock: std::sync::OnceLock::new(),
                read: output_stock::ReadAdmission::default(),
                not_empty: Notify::new(),
                not_full: Notify::new(),
            }),
        })
    }

    pub async fn snapshot(&self) -> ByteQueueSnapshot {
        self.inner.start.begin();
        if let Some(w) = self.inner.waiters.get() {
            w.reject_raw();
            return std::future::pending().await;
        }
        self.inner.state.lock().await.queue.snapshot()
    }

    #[cfg(test)]
    pub(crate) async fn custody_snapshot(&self) -> (ByteQueueSnapshot, ByteQueueCustody) {
        self.inner.start.begin();
        let state = self
            .inner
            .state
            .try_lock()
            .expect("passive custody inspection must be quiescent");
        (state.queue.snapshot(), state.custody)
    }

    pub async fn wait_read_len(&self, max_read_len: usize) -> Result<usize, AsyncByteQueueWait> {
        self.inner.start.begin();
        if self.inner.stock.get().is_some() {
            if let Some(waiters) = self.inner.waiters.get() { waiters.reject_raw(); }
            return Err(AsyncByteQueueWait::Closed);
        }
        match self.inner.waiters.get() {
            Some(w) => w
                .run(
                    native_wait::Role::Producer,
                    self.wait_read_len_inner(max_read_len),
                )
                .await
                .map_err(|e| match e {
                    native_wait::RunError::Closed => AsyncByteQueueWait::Closed,
                    native_wait::RunError::Quarantined => AsyncByteQueueWait::Quarantined,
                })?,
            None => self.wait_read_len_inner(max_read_len).await,
        }
    }
    async fn wait_read_len_inner(&self, max_read_len: usize) -> Result<usize, AsyncByteQueueWait> {
        if max_read_len == 0 {
            return Ok(0);
        }
        loop {
            let state = self.inner.state.lock().await;
            if self
                .inner
                .waiters
                .get()
                .map_or(state.closed, native_wait::Waiters::is_closed)
            {
                return Err(AsyncByteQueueWait::Closed);
            }
            let available = state.queue.snapshot().available_bytes();
            if available > 0 {
                return Ok(available.min(max_read_len));
            }
            let notified = self.inner.not_full.notified();
            drop(state);
            notified.await;
        }
    }

    pub async fn try_push(&self, bytes: Bytes) -> Result<(), ByteQueuePushError> {
        self.inner.start.begin();
        if let Some(w) = self.inner.waiters.get() {
            w.reject_raw();
            return Err(ByteQueuePushError::Capacity(
                bytes::MeteredCopyError::Quarantined,
            ));
        }
        let mut state = self.inner.state.lock().await;
        if self
            .inner
            .waiters
            .get()
            .map_or(state.closed, native_wait::Waiters::is_closed)
        {
            return Err(ByteQueuePushError::Closed);
        }
        state.queue.try_push(bytes)?;
        drop(state);
        self.inner.not_empty.notify_one();
        Ok(())
    }

    pub async fn push(&self, bytes: Bytes) -> Result<(), ByteQueuePushError> {
        self.inner.start.begin();
        if let Some(w) = self.inner.waiters.get() {
            w.reject_raw();
            return Err(ByteQueuePushError::Capacity(
                bytes::MeteredCopyError::Quarantined,
            ));
        }
        let len = bytes.len();
        if len == 0 {
            return Err(ByteQueuePushError::EmptyChunk);
        }
        loop {
            let mut state = self.inner.state.lock().await;
            if self
                .inner
                .waiters
                .get()
                .map_or(state.closed, native_wait::Waiters::is_closed)
            {
                return Err(ByteQueuePushError::Closed);
            }
            let snapshot = state.queue.snapshot();
            if len > snapshot.capacity_bytes {
                return Err(ByteQueuePushError::ChunkExceedsCapacity {
                    len,
                    capacity: snapshot.capacity_bytes,
                });
            }
            if state.queue.can_accept(len) {
                state.queue.try_push(bytes)?;
                drop(state);
                self.inner.not_empty.notify_one();
                return Ok(());
            }
            let notified = self.inner.not_full.notified();
            drop(state);
            notified.await;
        }
    }

    pub async fn pop_up_to(&self, limit: usize) -> Option<Bytes> {
        self.inner.start.begin();
        if let Some(w) = self.inner.waiters.get() {
            w.reject_raw();
            return std::future::pending().await;
        }
        let mut state = self.inner.state.lock().await;
        let popped = state.queue.pop_up_to(limit);
        let should_notify = popped.is_some();
        drop(state);
        if should_notify {
            self.inner.not_full.notify_waiters();
        }
        popped
    }

    pub async fn recv_up_to(&self, limit: usize) -> Option<Bytes> {
        self.inner.start.begin();
        if let Some(w) = self.inner.waiters.get() {
            w.reject_raw();
            return std::future::pending().await;
        }
        if limit == 0 {
            return None;
        }
        loop {
            let mut state = self.inner.state.lock().await;
            if let Some(bytes) = state.queue.pop_up_to(limit) {
                drop(state);
                self.inner.not_full.notify_waiters();
                return Some(bytes);
            }
            if self
                .inner
                .waiters
                .get()
                .map_or(state.closed, native_wait::Waiters::is_closed)
            {
                return None;
            }
            let notified = self.inner.not_empty.notified();
            drop(state);
            notified.await;
        }
    }

    pub async fn close(&self) {
        self.inner.start.begin();
        if let Some(w) = self.inner.waiters.get() {
            w.close();
            self.inner.not_empty.notify_waiters();
            self.inner.not_full.notify_waiters();
        } else {
            self.close_inner().await;
        }
    }
    async fn close_inner(&self) {
        let mut state = self.inner.state.lock().await;
        state.closed = true;
        drop(state);
        self.inner.not_empty.notify_waiters();
        self.inner.not_full.notify_waiters();
    }

    pub(crate) async fn push_custodied<P: crate::tcp_downlink_pump::payload::CustodiedPayload>(
        &self,
        pending: &mut Option<P>,
    ) -> Result<(), ByteCustodyError> {
        self.inner.start.begin();
        if self.inner.stock.get().is_some() || pending.as_ref().is_some_and(|p| p.has_stock()) {
            return Err(payload::reject_raw_stock(
                self.inner.waiters.get().map(|w| w.quiet_scope()), pending));
        }
        match self.inner.waiters.get() {
            Some(w) => w
                .run(
                    native_wait::Role::Producer,
                    self.push_custodied_inner(pending),
                )
                .await
                .map_err(|e| match e {
                    native_wait::RunError::Closed => ByteCustodyError::Rejected,
                    native_wait::RunError::Quarantined => ByteCustodyError::Invalid,
                })?,
            None => self.push_custodied_inner(pending).await,
        }
    }
    async fn push_custodied_inner<P: crate::tcp_downlink_pump::payload::CustodiedPayload>(
        &self,
        pending: &mut Option<P>,
    ) -> Result<(), ByteCustodyError> {
        let mut state = self.inner.state.lock().await;
        let Some(payload) = pending.as_ref() else {
            return Err(ByteCustodyError::Invalid);
        };
        if payload.meter().is_some_and(|m| m.budget_limit().is_some())
            && self.inner.waiters.get().is_none()
        {
            return Err(ByteCustodyError::Invalid);
        }
        if state.custody.interrupted || state.custody.frozen {
            return Err(ByteCustodyError::Invalid);
        }
        if self
            .inner
            .waiters
            .get()
            .map_or(state.closed, native_wait::Waiters::is_closed)
            || !state.queue.can_accept(payload.len())
        {
            return Err(ByteCustodyError::Rejected);
        }
        let sources = state.custody.sources.checked_add(1);
        let emitted = state.custody.emitted.checked_add(payload.len());
        let (Some(sources), Some(emitted)) = (sources, emitted) else {
            return Err(ByteCustodyError::Invalid);
        };
        state
            .queue
            .chunks
            .prepare(payload.meter())
            .map_err(ByteCustodyError::Capacity)?;
        let mutation = MeteredMutation::begin(state.queue.chunks.meter())
            .map_err(ByteCustodyError::Capacity)?;
        state.custody.interrupted = true;
        let payload = pending.take().ok_or(ByteCustodyError::Invalid)?;
        let allocation = payload.capacity();
        // All fallible validation precedes the actual transfer. This preserves
        // the original FIFO algorithm and uses its existing VecDeque storage.
        state.queue.queued_bytes += payload.len();
        state.queue.high_water_bytes = state.queue.high_water_bytes.max(state.queue.queued_bytes);
        state.queue.chunks.push_back(ByteChunk {
            bytes: payload.into_custodied_bytes(),
            allocation: Some(allocation),
        });
        state.custody.sources = sources;
        state.custody.emitted = emitted;
        state.custody.interrupted = false;
        drop(state);
        self.inner.not_empty.notify_one();
        mutation.commit();
        Ok(())
    }

    pub(crate) async fn recv_custodied(
        &self,
        limit: usize,
        held: &mut Option<ByteQueueBatch>,
    ) -> Result<bool, ByteCustodyError> {
        self.inner.start.begin();
        match self.inner.waiters.get() {
            Some(w) => w
                .run(
                    native_wait::Role::Consumer,
                    self.recv_custodied_inner(limit, held),
                )
                .await
                .map_err(|e| match e {
                    native_wait::RunError::Closed => ByteCustodyError::Rejected,
                    native_wait::RunError::Quarantined => ByteCustodyError::Invalid,
                })?,
            None => self.recv_custodied_inner(limit, held).await,
        }
    }
    async fn recv_custodied_inner(
        &self,
        limit: usize,
        held: &mut Option<ByteQueueBatch>,
    ) -> Result<bool, ByteCustodyError> {
        if held.is_some() || limit == 0 {
            return Err(ByteCustodyError::Invalid);
        }
        loop {
            let mut state = self.inner.state.lock().await;
            if state.custody.interrupted || state.custody.frozen {
                return Err(ByteCustodyError::Invalid);
            }
            if state.queue.queued_bytes != 0 {
                let n = state.queue.chunks.next_len(limit, state.queue.queued_bytes);
                let batches = state.custody.batches.checked_add(1);
                let consumed = state.custody.consumed.checked_add(n);
                let (Some(batches), Some(consumed)) = (batches, consumed) else {
                    return Err(ByteCustodyError::Invalid);
                };
                let mutation = MeteredMutation::begin(state.queue.chunks.meter())
                    .map_err(ByteCustodyError::Capacity)?;
                state.custody.interrupted = true;
                let (bytes, allocation) = state
                    .queue
                    .pop_owned(limit)
                    .ok_or(ByteCustodyError::Invalid)?;
                *held = Some(ByteQueueBatch { bytes, allocation });
                if held.as_ref().is_none_or(|batch| {
                    batch.bytes.len() != n || batch.allocation < batch.bytes.len()
                }) {
                    return Err(ByteCustodyError::Invalid);
                }
                state.custody.batches = batches;
                state.custody.consumed = consumed;
                state.custody.interrupted = false;
                drop(state);
                self.inner.not_full.notify_waiters();
                mutation.commit();
                return Ok(true);
            }
            if self
                .inner
                .waiters
                .get()
                .map_or(state.closed, native_wait::Waiters::is_closed)
            {
                if !self.inner.read.is_active() { return Ok(false); }
                // An already granted read may still deliver its real tail.
                // Its cancellation is lock-free, so register then recheck.
                let notified = self.inner.not_empty.notified();
                drop(state);
                tokio::pin!(notified);
                notified.as_mut().enable();
                if !self.inner.read.is_active() { continue; }
                notified.await;
                continue;
            }
            let notified = self.inner.not_empty.notified();
            drop(state);
            notified.await;
        }
    }

    /// Only after all original actors join and the producer stops. Every turn
    /// touches at most32 original chunks, even for millions of 1-byte records.
    /// None means more work/lock contention; Err is sticky unknown custody.
    pub(crate) fn has_output_stock(&self) -> bool { self.inner.stock.get().is_some() }

    pub(crate) fn dispose_custodied(&self) -> Result<Option<ByteQueueCustody>, ByteCustodyError> {
        let _quiet = self.inner.waiters.get().map(|w| w.quiet_scope());
        let Some(data) = self.dispose_data()? else { return Ok(None); };
        if self.inner.waiters.get().is_some_and(|w| !w.finish()) { return Ok(None); }
        Ok(Some(data.custody()))
    }

    /// Original bounded disposal transaction, before external loans and the
    /// protected Producer retirement waiter have actually exited.
    pub(crate) fn dispose_data(&self) -> Result<Option<ByteQueueDataDisposition>, ByteCustodyError> {
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
        let Ok(mut state) = self.inner.state.try_lock() else {
            return Ok(None);
        };
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
        if state.custody.interrupted {
            return Err(ByteCustodyError::Invalid);
        }
        if !state.custody.frozen {
            if let Some(w) = self.inner.waiters.get() {
                w.close();
            } else {
                state.closed = true;
            }
            state.custody.frozen_bytes = state.queue.queued_bytes;
            state.custody.frozen_chunks = state.queue.chunks.len();
            state.custody.metadata_slots = state.queue.chunks.capacity();
            state.custody.metadata_bytes = state
                .queue
                .chunks
                .capacity()
                .checked_mul(std::mem::size_of::<ByteChunk>())
                .ok_or(ByteCustodyError::Invalid)?;
            state.custody.frozen = true;
        }
        for _ in 0..32 {
            let Some(chunk) = state.queue.chunks.front() else {
                let valid = state.queue.queued_bytes == 0
                    && state.custody.discarded_bytes == state.custody.frozen_bytes
                    && state.custody.discarded_chunks == state.custody.frozen_chunks
                    && state
                        .custody
                        .consumed
                        .checked_add(state.custody.discarded_bytes)
                        == Some(state.custody.emitted);
                if !valid {
                    state.custody.interrupted = true;
                    return Err(ByteCustodyError::Invalid);
                }
                state.queue.chunks = Chunks::default();
                let receipt = state.custody;
                drop(state);
                drop(turn);
                return Ok(Some(ByteQueueDataDisposition(receipt)));
            };
            let n = chunk.bytes.len();
            let Some(allocation) = chunk.allocation else {
                state.custody.interrupted = true;
                return Err(ByteCustodyError::Invalid);
            };
            let chunks = state.custody.discarded_chunks.checked_add(1);
            let bytes = state.custody.discarded_bytes.checked_add(n);
            let storage = state.custody.discarded_allocation.checked_add(allocation);
            let remaining = state.queue.queued_bytes.checked_sub(n);
            let (Some(chunks), Some(bytes), Some(storage), Some(remaining)) =
                (chunks, bytes, storage, remaining)
            else {
                state.custody.interrupted = true;
                return Err(ByteCustodyError::Invalid);
            };
            if let Some(w) = self.inner.waiters.get() {
                if !w.begin_disposal() { return Err(ByteCustodyError::Invalid); }
            }
            state.custody.interrupted = true;
            let retired = state.queue.chunks.pop_front();
            state.queue.queued_bytes = remaining;
            state.custody.discarded_chunks = chunks;
            state.custody.discarded_bytes = bytes;
            state.custody.discarded_allocation = storage;
            if let Some(w) = self.inner.waiters.get() {
                drop(state);
                let Some(retired) = retired else { w.mark_quarantined(); return Err(ByteCustodyError::Invalid); };
                if !w.dispose_chunk(retired) { return Err(ByteCustodyError::Invalid); }
                let Some(guard) = self.inner.state.try_lock().ok() else { return Ok(None); };
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

mod leased;
mod native_wait;
#[cfg(all(test, feature = "harness"))]
pub(crate) use leased::PermitProvedReleaseSource;
#[cfg(test)]
pub(crate) use leased::PermitQueueReleaseAudit;
pub use leased::{
    AsyncLeasedByteFlowQueue, ByteQueueReadReservation, D16_GLOBAL_BYTE_BUDGET_DEFAULT_BYTES,
    D16GlobalByteBudget, D16GlobalByteBudgetSnapshot, DownstreamBytePermit,
    DownstreamPermitReleaseMode, LeasedByteQueueClosure, LeasedByteQueueSnapshot,
    LeasedByteQueueTerminalCause, LeasedBytes, LeasedQueuePoll,
};
pub(crate) use leased::{LeasedQueueBatch, PermitQueueIdentity};

#[cfg(test)]
#[path = "tcp_downlink_pump/tests/queue.rs"]
mod tests;

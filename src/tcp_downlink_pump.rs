use bytes::{Bytes, BytesMut};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
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
}

#[derive(Debug)]
pub struct ByteBoundedFlowQueue {
    chunks: VecDeque<Bytes>,
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
            chunks: VecDeque::new(),
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
        self.queued_bytes = self.queued_bytes.saturating_add(len);
        self.high_water_bytes = self.high_water_bytes.max(self.queued_bytes);
        self.chunks.push_back(bytes);
        Ok(())
    }

    pub fn pop_up_to(&mut self, limit: usize) -> Option<Bytes> {
        if limit == 0 {
            return None;
        }
        let mut remaining = limit;
        let mut out = BytesMut::new();
        while remaining > 0 {
            let Some(mut chunk) = self.chunks.pop_front() else {
                break;
            };
            let take = chunk.len().min(remaining);
            out.extend_from_slice(&chunk[..take]);
            self.queued_bytes = self.queued_bytes.saturating_sub(take);
            remaining -= take;
            if take < chunk.len() {
                chunk = chunk.slice(take..);
                self.chunks.push_front(chunk);
                break;
            }
        }
        if out.is_empty() {
            None
        } else {
            Some(out.freeze())
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
}

#[derive(Debug, Clone)]
pub struct AsyncByteBoundedFlowQueue {
    inner: Arc<AsyncByteBoundedFlowQueueInner>,
}

#[derive(Debug)]
struct AsyncByteBoundedFlowQueueInner {
    state: Mutex<AsyncByteBoundedFlowQueueState>,
    not_empty: Notify,
    not_full: Notify,
}

#[derive(Debug)]
struct AsyncByteBoundedFlowQueueState {
    queue: ByteBoundedFlowQueue,
    closed: bool,
}

impl AsyncByteBoundedFlowQueue {
    pub fn new(capacity_bytes: usize) -> Result<Self, ByteQueueConfigError> {
        Ok(Self {
            inner: Arc::new(AsyncByteBoundedFlowQueueInner {
                state: Mutex::new(AsyncByteBoundedFlowQueueState {
                    queue: ByteBoundedFlowQueue::new(capacity_bytes)?,
                    closed: false,
                }),
                not_empty: Notify::new(),
                not_full: Notify::new(),
            }),
        })
    }

    pub async fn snapshot(&self) -> ByteQueueSnapshot {
        self.inner.state.lock().await.queue.snapshot()
    }

    pub async fn wait_read_len(&self, max_read_len: usize) -> Result<usize, AsyncByteQueueWait> {
        if max_read_len == 0 {
            return Ok(0);
        }
        loop {
            let state = self.inner.state.lock().await;
            if state.closed {
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
        let mut state = self.inner.state.lock().await;
        if state.closed {
            return Err(ByteQueuePushError::Closed);
        }
        state.queue.try_push(bytes)?;
        drop(state);
        self.inner.not_empty.notify_one();
        Ok(())
    }

    pub async fn push(&self, bytes: Bytes) -> Result<(), ByteQueuePushError> {
        let len = bytes.len();
        if len == 0 {
            return Err(ByteQueuePushError::EmptyChunk);
        }
        loop {
            let mut state = self.inner.state.lock().await;
            if state.closed {
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
            if state.closed {
                return None;
            }
            let notified = self.inner.not_empty.notified();
            drop(state);
            notified.await;
        }
    }

    pub async fn close(&self) {
        let mut state = self.inner.state.lock().await;
        state.closed = true;
        drop(state);
        self.inner.not_empty.notify_waiters();
        self.inner.not_full.notify_waiters();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeasedByteQueueSnapshot {
    pub queued_bytes: usize,
    pub leased_bytes: usize,
    pub reserved_bytes: usize,
    pub capacity_bytes: usize,
    pub high_water_bytes: usize,
    pub chunks: usize,
    pub closure: LeasedByteQueueClosure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeasedByteQueueTerminalCause {
    pub direction: &'static str,
    pub reason: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeasedByteQueueClosure {
    Open,
    RemoteEof,
    Terminal(LeasedByteQueueTerminalCause),
}

impl LeasedByteQueueClosure {
    pub fn is_closed(self) -> bool {
        !matches!(self, Self::Open)
    }

    pub fn terminal_cause(self) -> Option<LeasedByteQueueTerminalCause> {
        match self {
            Self::Terminal(cause) => Some(cause),
            Self::Open | Self::RemoteEof => None,
        }
    }
}

pub const D16_GLOBAL_BYTE_BUDGET_DEFAULT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct D16GlobalByteBudgetSnapshot {
    pub reserved_bytes: usize,
    pub owned_bytes: usize,
    pub capacity_bytes: usize,
    pub high_water_bytes: usize,
}

impl D16GlobalByteBudgetSnapshot {
    pub fn total_bytes(self) -> usize {
        self.reserved_bytes.saturating_add(self.owned_bytes)
    }

    pub fn available_bytes(self) -> usize {
        self.capacity_bytes.saturating_sub(self.total_bytes())
    }
}

#[derive(Debug, Clone)]
pub struct D16GlobalByteBudget {
    inner: Arc<D16GlobalByteBudgetInner>,
}

#[derive(Debug)]
struct D16GlobalByteBudgetInner {
    state: StdMutex<D16GlobalByteBudgetState>,
    not_full: Notify,
}

#[derive(Debug)]
struct D16GlobalByteBudgetState {
    reserved_bytes: usize,
    owned_bytes: usize,
    capacity_bytes: usize,
    high_water_bytes: usize,
}

impl D16GlobalByteBudgetState {
    fn snapshot(&self) -> D16GlobalByteBudgetSnapshot {
        D16GlobalByteBudgetSnapshot {
            reserved_bytes: self.reserved_bytes,
            owned_bytes: self.owned_bytes,
            capacity_bytes: self.capacity_bytes,
            high_water_bytes: self.high_water_bytes,
        }
    }

    fn total_bytes(&self) -> usize {
        self.reserved_bytes.saturating_add(self.owned_bytes)
    }

    fn available_bytes(&self) -> usize {
        self.capacity_bytes.saturating_sub(self.total_bytes())
    }

    fn note_high_water(&mut self) {
        self.high_water_bytes = self.high_water_bytes.max(self.total_bytes());
    }
}

impl D16GlobalByteBudget {
    pub fn new(capacity_bytes: usize) -> Result<Self, ByteQueueConfigError> {
        if capacity_bytes == 0 {
            return Err(ByteQueueConfigError::ZeroCapacity);
        }
        Ok(Self::new_nonzero(capacity_bytes))
    }

    fn new_nonzero(capacity_bytes: usize) -> Self {
        Self {
            inner: Arc::new(D16GlobalByteBudgetInner {
                state: StdMutex::new(D16GlobalByteBudgetState {
                    reserved_bytes: 0,
                    owned_bytes: 0,
                    capacity_bytes,
                    high_water_bytes: 0,
                }),
                not_full: Notify::new(),
            }),
        }
    }

    pub fn new_default() -> Self {
        Self::new_nonzero(D16_GLOBAL_BYTE_BUDGET_DEFAULT_BYTES)
    }

    pub fn snapshot(&self) -> D16GlobalByteBudgetSnapshot {
        self.inner.lock_state().snapshot()
    }

    async fn reserve_up_to(&self, requested: usize) -> D16GlobalReadReservation {
        debug_assert!(requested > 0);
        loop {
            let notified = self.inner.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.inner.lock_state();
                let available = state.available_bytes();
                if available > 0 {
                    let reserved_bytes = requested.min(available);
                    state.reserved_bytes = state.reserved_bytes.saturating_add(reserved_bytes);
                    state.note_high_water();
                    return D16GlobalReadReservation {
                        inner: self.inner.clone(),
                        reserved_bytes,
                    };
                }
            }
            notified.await;
        }
    }
}

impl D16GlobalByteBudgetInner {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, D16GlobalByteBudgetState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn release_owned(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut state = self.lock_state();
        state.owned_bytes = state.owned_bytes.saturating_sub(bytes);
        drop(state);
        self.not_full.notify_waiters();
    }

    fn try_acquire_owned(&self, bytes: usize) -> bool {
        if bytes == 0 {
            return true;
        }
        let mut state = self.lock_state();
        if bytes > state.available_bytes() {
            return false;
        }
        state.owned_bytes = state.owned_bytes.saturating_add(bytes);
        state.note_high_water();
        true
    }
}

struct D16GlobalReadReservation {
    inner: Arc<D16GlobalByteBudgetInner>,
    reserved_bytes: usize,
}

impl D16GlobalReadReservation {
    fn max_len(&self) -> usize {
        self.reserved_bytes
    }

    fn retain(&mut self, retained_bytes: usize) {
        let retained_bytes = retained_bytes.min(self.reserved_bytes);
        let released = self.reserved_bytes.saturating_sub(retained_bytes);
        if released == 0 {
            return;
        }
        let mut state = self.inner.lock_state();
        state.reserved_bytes = state.reserved_bytes.saturating_sub(released);
        self.reserved_bytes = retained_bytes;
        drop(state);
        self.inner.not_full.notify_waiters();
    }

    fn commit(&mut self, bytes: usize) {
        debug_assert!(bytes <= self.reserved_bytes);
        let mut state = self.inner.lock_state();
        state.reserved_bytes = state.reserved_bytes.saturating_sub(self.reserved_bytes);
        state.owned_bytes = state.owned_bytes.saturating_add(bytes);
        let released = self.reserved_bytes.saturating_sub(bytes);
        self.reserved_bytes = 0;
        state.note_high_water();
        drop(state);
        if released > 0 {
            self.inner.not_full.notify_waiters();
        }
    }
}

impl Drop for D16GlobalReadReservation {
    fn drop(&mut self) {
        if self.reserved_bytes == 0 {
            return;
        }
        let mut state = self.inner.lock_state();
        state.reserved_bytes = state.reserved_bytes.saturating_sub(self.reserved_bytes);
        self.reserved_bytes = 0;
        drop(state);
        self.inner.not_full.notify_waiters();
    }
}

impl LeasedByteQueueSnapshot {
    pub fn owned_bytes(self) -> usize {
        self.queued_bytes
            .saturating_add(self.leased_bytes)
            .saturating_add(self.reserved_bytes)
    }

    pub fn available_bytes(self) -> usize {
        self.capacity_bytes.saturating_sub(self.owned_bytes())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownstreamPermitReleaseMode {
    OnAdmission,
    OnEgressDrain,
}

#[derive(Debug)]
pub enum LeasedQueuePoll {
    Data(LeasedBytes),
    Empty,
    Closed,
}

#[derive(Debug, Clone)]
pub struct AsyncLeasedByteFlowQueue {
    inner: Arc<AsyncLeasedByteFlowQueueInner>,
}

#[derive(Debug)]
struct AsyncLeasedByteFlowQueueInner {
    state: StdMutex<LeasedByteFlowQueueState>,
    not_empty: Notify,
    not_full: Notify,
    global_budget: Option<D16GlobalByteBudget>,
}

#[derive(Debug)]
struct LeasedByteFlowQueueState {
    chunks: VecDeque<Bytes>,
    queued_bytes: usize,
    leased_bytes: usize,
    reserved_bytes: usize,
    wake_pending: bool,
    capacity_bytes: usize,
    high_water_bytes: usize,
    release_mode: DownstreamPermitReleaseMode,
    closure: LeasedByteQueueClosure,
}

impl LeasedByteFlowQueueState {
    fn new(capacity_bytes: usize, release_mode: DownstreamPermitReleaseMode) -> Self {
        Self {
            chunks: VecDeque::new(),
            queued_bytes: 0,
            leased_bytes: 0,
            reserved_bytes: 0,
            wake_pending: false,
            capacity_bytes,
            high_water_bytes: 0,
            release_mode,
            closure: LeasedByteQueueClosure::Open,
        }
    }

    fn snapshot(&self) -> LeasedByteQueueSnapshot {
        LeasedByteQueueSnapshot {
            queued_bytes: self.queued_bytes,
            leased_bytes: self.leased_bytes,
            reserved_bytes: self.reserved_bytes,
            capacity_bytes: self.capacity_bytes,
            high_water_bytes: self.high_water_bytes,
            chunks: self.chunks.len(),
            closure: self.closure,
        }
    }

    fn available_bytes(&self) -> usize {
        self.capacity_bytes.saturating_sub(self.owned_bytes())
    }

    fn note_high_water(&mut self) {
        self.high_water_bytes = self.high_water_bytes.max(self.owned_bytes());
    }

    fn owned_bytes(&self) -> usize {
        self.queued_bytes
            .saturating_add(self.leased_bytes)
            .saturating_add(self.reserved_bytes)
    }

    fn pop_up_to(&mut self, limit: usize) -> Option<Bytes> {
        if limit == 0 {
            return None;
        }
        let mut remaining = limit;
        let mut out = BytesMut::new();
        while remaining > 0 {
            let Some(mut chunk) = self.chunks.pop_front() else {
                break;
            };
            let take = chunk.len().min(remaining);
            out.extend_from_slice(&chunk[..take]);
            self.queued_bytes = self.queued_bytes.saturating_sub(take);
            self.leased_bytes = self.leased_bytes.saturating_add(take);
            remaining -= take;
            if take < chunk.len() {
                chunk = chunk.slice(take..);
                self.chunks.push_front(chunk);
                break;
            }
        }
        if out.is_empty() {
            None
        } else {
            self.note_high_water();
            Some(out.freeze())
        }
    }
}

impl AsyncLeasedByteFlowQueue {
    pub fn new(capacity_bytes: usize) -> Result<Self, ByteQueueConfigError> {
        Self::new_with_release_mode(capacity_bytes, DownstreamPermitReleaseMode::OnAdmission)
    }

    pub fn new_with_release_mode(
        capacity_bytes: usize,
        release_mode: DownstreamPermitReleaseMode,
    ) -> Result<Self, ByteQueueConfigError> {
        Self::new_inner(capacity_bytes, release_mode, None)
    }

    pub fn new_with_global_budget(
        capacity_bytes: usize,
        release_mode: DownstreamPermitReleaseMode,
        global_budget: D16GlobalByteBudget,
    ) -> Result<Self, ByteQueueConfigError> {
        Self::new_inner(capacity_bytes, release_mode, Some(global_budget))
    }

    fn new_inner(
        capacity_bytes: usize,
        release_mode: DownstreamPermitReleaseMode,
        global_budget: Option<D16GlobalByteBudget>,
    ) -> Result<Self, ByteQueueConfigError> {
        if capacity_bytes == 0 {
            return Err(ByteQueueConfigError::ZeroCapacity);
        }
        Ok(Self {
            inner: Arc::new(AsyncLeasedByteFlowQueueInner {
                state: StdMutex::new(LeasedByteFlowQueueState::new(capacity_bytes, release_mode)),
                not_empty: Notify::new(),
                not_full: Notify::new(),
                global_budget,
            }),
        })
    }

    pub async fn snapshot(&self) -> LeasedByteQueueSnapshot {
        self.snapshot_now()
    }

    pub fn snapshot_now(&self) -> LeasedByteQueueSnapshot {
        self.inner.lock_state().snapshot()
    }

    #[cfg(test)]
    pub(crate) fn blocking_snapshot_for_test(&self) -> LeasedByteQueueSnapshot {
        self.inner.lock_state().snapshot()
    }

    pub async fn reserve_read_up_to(
        &self,
        requested: usize,
    ) -> Result<ByteQueueReadReservation, AsyncByteQueueWait> {
        if requested == 0 {
            return Ok(ByteQueueReadReservation {
                inner: self.inner.clone(),
                reserved_bytes: 0,
                global_reservation: None,
            });
        }
        loop {
            let notified = self.inner.not_full.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let flow_available = {
                let state = self.inner.lock_state();
                if state.closure.is_closed() {
                    return Err(AsyncByteQueueWait::Closed);
                }
                state.available_bytes()
            };
            if flow_available == 0 {
                notified.await;
                continue;
            }
            let mut global_reservation = if let Some(budget) = &self.inner.global_budget {
                let requested = requested.min(flow_available);
                Some(tokio::select! {
                    reservation = budget.reserve_up_to(requested) => reservation,
                    _ = notified.as_mut() => continue,
                })
            } else {
                None
            };
            let mut state = self.inner.lock_state();
            if state.closure.is_closed() {
                return Err(AsyncByteQueueWait::Closed);
            }
            let global_available = global_reservation
                .as_ref()
                .map(D16GlobalReadReservation::max_len)
                .unwrap_or(requested);
            let reserved_bytes = requested.min(state.available_bytes()).min(global_available);
            if reserved_bytes == 0 {
                drop(state);
                drop(global_reservation);
                continue;
            }
            if let Some(global_reservation) = global_reservation.as_mut() {
                global_reservation.retain(reserved_bytes);
            }
            state.reserved_bytes = state.reserved_bytes.saturating_add(reserved_bytes);
            state.note_high_water();
            return Ok(ByteQueueReadReservation {
                inner: self.inner.clone(),
                reserved_bytes,
                global_reservation,
            });
        }
    }

    pub async fn wait_read_len(&self, max_read_len: usize) -> Result<usize, AsyncByteQueueWait> {
        if max_read_len == 0 {
            return Ok(0);
        }
        loop {
            let notified = {
                let state = self.inner.lock_state();
                if state.closure.is_closed() {
                    return Err(AsyncByteQueueWait::Closed);
                }
                let available = state.available_bytes();
                if available > 0 {
                    return Ok(available.min(max_read_len));
                }
                self.inner.not_full.notified()
            };
            notified.await;
        }
    }

    pub async fn push(&self, bytes: Bytes) -> Result<(), ByteQueuePushError> {
        let len = bytes.len();
        if len == 0 {
            return Err(ByteQueuePushError::EmptyChunk);
        }
        if let Some(global_budget) = &self.inner.global_budget {
            let global_capacity = global_budget.snapshot().capacity_bytes;
            let flow_capacity = self.inner.lock_state().capacity_bytes;
            let capacity = flow_capacity.min(global_capacity);
            if len > capacity {
                return Err(ByteQueuePushError::ChunkExceedsCapacity { len, capacity });
            }
            loop {
                let flow_notified = self.inner.not_full.notified();
                tokio::pin!(flow_notified);
                flow_notified.as_mut().enable();
                let global_notified = global_budget.inner.not_full.notified();
                tokio::pin!(global_notified);
                global_notified.as_mut().enable();
                {
                    let mut state = self.inner.lock_state();
                    if state.closure.is_closed() {
                        return Err(ByteQueuePushError::Closed);
                    }
                    if len <= state.available_bytes() && global_budget.inner.try_acquire_owned(len)
                    {
                        state.queued_bytes = state.queued_bytes.saturating_add(len);
                        state.note_high_water();
                        state.chunks.push_back(bytes);
                        drop(state);
                        self.inner.not_empty.notify_one();
                        return Ok(());
                    }
                }
                tokio::select! {
                    _ = flow_notified.as_mut() => {}
                    _ = global_notified.as_mut() => {}
                }
            }
        }
        loop {
            let notified = {
                let mut state = self.inner.lock_state();
                if state.closure.is_closed() {
                    return Err(ByteQueuePushError::Closed);
                }
                if len > state.capacity_bytes {
                    return Err(ByteQueuePushError::ChunkExceedsCapacity {
                        len,
                        capacity: state.capacity_bytes,
                    });
                }
                if len <= state.available_bytes() {
                    state.queued_bytes = state.queued_bytes.saturating_add(len);
                    state.note_high_water();
                    state.chunks.push_back(bytes);
                    drop(state);
                    self.inner.not_empty.notify_one();
                    return Ok(());
                }
                self.inner.not_full.notified()
            };
            notified.await;
        }
    }

    pub fn try_recv_up_to(&self, limit: usize) -> LeasedQueuePoll {
        let mut state = self.inner.lock_state();
        if limit > 0
            && let Some(bytes) = state.pop_up_to(limit)
        {
            let permit = DownstreamBytePermit {
                inner: self.inner.clone(),
                remaining_bytes: bytes.len(),
                release_mode: state.release_mode,
            };
            return LeasedQueuePoll::Data(LeasedBytes { bytes, permit });
        }
        if state.closure.is_closed() {
            if state.queued_bytes == 0 && state.reserved_bytes == 0 {
                state.wake_pending = false;
                return LeasedQueuePoll::Closed;
            }
            return LeasedQueuePoll::Empty;
        }
        if state.queued_bytes == 0 {
            state.wake_pending = false;
        }
        LeasedQueuePoll::Empty
    }

    pub async fn recv_up_to(&self, limit: usize) -> Option<LeasedBytes> {
        if limit == 0 {
            return None;
        }
        loop {
            let notified = {
                let mut state = self.inner.lock_state();
                if let Some(bytes) = state.pop_up_to(limit) {
                    let permit = DownstreamBytePermit {
                        inner: self.inner.clone(),
                        remaining_bytes: bytes.len(),
                        release_mode: state.release_mode,
                    };
                    return Some(LeasedBytes { bytes, permit });
                }
                if state.closure.is_closed() {
                    return None;
                }
                self.inner.not_empty.notified()
            };
            notified.await;
        }
    }

    pub async fn close_for_remote_eof_and_mark_ready(&self) -> bool {
        let mut state = self.inner.lock_state();
        if state.closure.is_closed() {
            return false;
        }
        state.closure = LeasedByteQueueClosure::RemoteEof;
        let should_wake = !state.wake_pending;
        state.wake_pending = true;
        drop(state);
        self.inner.not_empty.notify_waiters();
        self.inner.not_full.notify_waiters();
        should_wake
    }

    pub async fn close_for_terminal_and_mark_ready(
        &self,
        direction: &'static str,
        reason: &'static str,
    ) -> bool {
        let mut state = self.inner.lock_state();
        let was_closed = state.closure.is_closed();
        if !matches!(state.closure, LeasedByteQueueClosure::Terminal(_)) {
            state.closure = LeasedByteQueueClosure::Terminal(LeasedByteQueueTerminalCause {
                direction,
                reason,
            });
        }
        let should_wake = !was_closed && !state.wake_pending;
        state.wake_pending = true;
        drop(state);
        self.inner.not_empty.notify_waiters();
        self.inner.not_full.notify_waiters();
        should_wake
    }

    pub async fn close(&self) {
        let _ = self
            .close_for_terminal_and_mark_ready("internal", "queue_closed")
            .await;
    }

    pub fn close_and_drop_queued_for_terminal(
        &self,
        direction: &'static str,
        reason: &'static str,
    ) -> usize {
        let mut state = self.inner.lock_state();
        if !matches!(state.closure, LeasedByteQueueClosure::Terminal(_)) {
            state.closure = LeasedByteQueueClosure::Terminal(LeasedByteQueueTerminalCause {
                direction,
                reason,
            });
        }
        let dropped = state.queued_bytes;
        state.chunks.clear();
        state.queued_bytes = 0;
        state.wake_pending = false;
        drop(state);
        if let Some(global_budget) = &self.inner.global_budget {
            global_budget.inner.release_owned(dropped);
        }
        self.inner.not_empty.notify_waiters();
        self.inner.not_full.notify_waiters();
        dropped
    }
}

impl AsyncLeasedByteFlowQueueInner {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, LeasedByteFlowQueueState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for AsyncLeasedByteFlowQueueInner {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let remaining_owned = state.queued_bytes.saturating_add(state.leased_bytes);
        if let Some(global_budget) = &self.global_budget {
            global_budget.inner.release_owned(remaining_owned);
        }
    }
}

pub struct ByteQueueReadReservation {
    inner: Arc<AsyncLeasedByteFlowQueueInner>,
    reserved_bytes: usize,
    global_reservation: Option<D16GlobalReadReservation>,
}

impl ByteQueueReadReservation {
    pub fn max_len(&self) -> usize {
        self.reserved_bytes
    }

    pub fn commit(mut self, bytes: Bytes) -> Result<bool, ByteQueuePushError> {
        let len = bytes.len();
        if len == 0 {
            return Err(ByteQueuePushError::EmptyChunk);
        }
        if len > self.reserved_bytes {
            return Err(ByteQueuePushError::ChunkExceedsCapacity {
                len,
                capacity: self.reserved_bytes,
            });
        }

        let mut state = self.inner.lock_state();
        if state.closure.is_closed() {
            return Err(ByteQueuePushError::Closed);
        }
        let unused_bytes = self.reserved_bytes.saturating_sub(len);
        state.reserved_bytes = state.reserved_bytes.saturating_sub(self.reserved_bytes);
        if let Some(global_reservation) = self.global_reservation.as_mut() {
            global_reservation.commit(len);
        }
        self.reserved_bytes = 0;
        let should_wake = state.queued_bytes == 0 && !state.wake_pending;
        state.queued_bytes = state.queued_bytes.saturating_add(len);
        state.chunks.push_back(bytes);
        state.wake_pending = true;
        state.note_high_water();
        drop(state);

        if unused_bytes > 0 {
            self.inner.not_full.notify_waiters();
        }
        self.inner.not_empty.notify_one();
        Ok(should_wake)
    }
}

impl Drop for ByteQueueReadReservation {
    fn drop(&mut self) {
        if self.reserved_bytes == 0 {
            return;
        }
        let mut state = self.inner.lock_state();
        state.reserved_bytes = state.reserved_bytes.saturating_sub(self.reserved_bytes);
        self.reserved_bytes = 0;
        drop(state);
        self.global_reservation.take();
        self.inner.not_full.notify_waiters();
    }
}

pub struct LeasedBytes {
    bytes: Bytes,
    permit: DownstreamBytePermit,
}

impl LeasedBytes {
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn permit_mut(&mut self) -> &mut DownstreamBytePermit {
        &mut self.permit
    }

    pub fn into_parts(self) -> (Bytes, DownstreamBytePermit) {
        (self.bytes, self.permit)
    }
}

impl std::fmt::Debug for LeasedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeasedBytes")
            .field("bytes", &self.bytes.len())
            .field("permit", &self.permit)
            .finish()
    }
}

pub struct DownstreamBytePermit {
    inner: Arc<AsyncLeasedByteFlowQueueInner>,
    remaining_bytes: usize,
    release_mode: DownstreamPermitReleaseMode,
}

impl DownstreamBytePermit {
    pub fn remaining_bytes(&self) -> usize {
        self.remaining_bytes
    }

    pub fn release_mode(&self) -> DownstreamPermitReleaseMode {
        self.release_mode
    }

    pub fn release(&mut self, bytes: usize) -> usize {
        let released = self.remaining_bytes.min(bytes);
        if released == 0 {
            return 0;
        }
        self.remaining_bytes = self.remaining_bytes.saturating_sub(released);
        let mut state = self.inner.lock_state();
        state.leased_bytes = state.leased_bytes.saturating_sub(released);
        drop(state);
        if let Some(global_budget) = &self.inner.global_budget {
            global_budget.inner.release_owned(released);
        }
        self.inner.not_full.notify_waiters();
        released
    }
}

impl crate::tcp_egress::TcpEgressLease for DownstreamBytePermit {
    fn remaining_bytes(&self) -> usize {
        DownstreamBytePermit::remaining_bytes(self)
    }

    fn release(&mut self, bytes: usize) -> usize {
        DownstreamBytePermit::release(self, bytes)
    }

    fn split_to(&mut self, bytes: usize) -> Option<Self> {
        let moved = self.remaining_bytes.min(bytes);
        if moved == 0 {
            return None;
        }
        self.remaining_bytes = self.remaining_bytes.saturating_sub(moved);
        Some(Self {
            inner: self.inner.clone(),
            remaining_bytes: moved,
            release_mode: self.release_mode,
        })
    }
}

impl Drop for DownstreamBytePermit {
    fn drop(&mut self) {
        self.release(usize::MAX);
    }
}

impl std::fmt::Debug for DownstreamBytePermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownstreamBytePermit")
            .field("remaining_bytes", &self.remaining_bytes)
            .field("release_mode", &self.release_mode)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE_HUNDRED_MBIT_CHUNK_BYTES: usize = 64 * 1024;
    const ONE_HUNDRED_MBIT_CADENCE: Duration = Duration::from_millis(5);
    const THIRTY_SECONDS_AT_100M_STEPS: usize = 30_000 / 5;

    #[test]
    fn byte_bounded_flow_queue_enforces_byte_capacity() {
        let mut queue = ByteBoundedFlowQueue::new(128 * 1024).unwrap();

        queue.try_push(Bytes::from(vec![1; 64 * 1024])).unwrap();
        queue.try_push(Bytes::from(vec![2; 64 * 1024])).unwrap();

        assert_eq!(queue.snapshot().queued_bytes, 128 * 1024);
        assert_eq!(
            queue.try_push(Bytes::from(vec![3; 1])),
            Err(ByteQueuePushError::Full {
                len: 1,
                available: 0
            })
        );

        let drained = queue.pop_up_to(96 * 1024).unwrap();
        assert_eq!(drained.len(), 96 * 1024);
        assert_eq!(queue.snapshot().queued_bytes, 32 * 1024);

        queue.try_push(Bytes::from(vec![4; 64 * 1024])).unwrap();
        assert_eq!(queue.snapshot().queued_bytes, 96 * 1024);
    }

    #[test]
    fn continuous_pump_model_sustains_100mbit_for_30s_without_capacity_read_gaps() {
        let mut queue = ByteBoundedFlowQueue::new(4 * 1024 * 1024).unwrap();
        let mut probe = ContinuousPumpProbe::default();
        let start = Instant::now();

        for step in 0..THIRTY_SECONDS_AT_100M_STEPS {
            let now = start + ONE_HUNDRED_MBIT_CADENCE.saturating_mul(step as u32);
            let before = queue.snapshot();
            assert!(
                queue.can_accept(ONE_HUNDRED_MBIT_CHUNK_BYTES),
                "100M equivalent reader should have queue capacity at step {step}: {before:?}"
            );
            probe.note_read_armed(before, ONE_HUNDRED_MBIT_CHUNK_BYTES);
            queue
                .try_push(Bytes::from(vec![0; ONE_HUNDRED_MBIT_CHUNK_BYTES]))
                .unwrap();
            probe.note_remote_read(now, ONE_HUNDRED_MBIT_CHUNK_BYTES, true);

            let drained = queue.pop_up_to(ONE_HUNDRED_MBIT_CHUNK_BYTES).unwrap();
            probe.note_drain(drained.len(), queue.snapshot());
        }

        let stats = probe.stats();
        assert_eq!(
            stats.remote_read_bytes,
            (THIRTY_SECONDS_AT_100M_STEPS * ONE_HUNDRED_MBIT_CHUNK_BYTES) as u64
        );
        assert_eq!(stats.remote_read_bytes, stats.drained_bytes);
        assert_eq!(queue.snapshot().queued_bytes, 0);
        assert_eq!(stats.queue_full_waits, 0);
        assert!(
            stats.max_remote_read_gap_with_capacity <= ONE_HUNDRED_MBIT_CADENCE,
            "capacity read gap must stay at cadence, got {:?}",
            stats.max_remote_read_gap_with_capacity
        );
    }

    #[test]
    fn continuous_pump_absorbs_short_drain_stall_without_remote_read_gap() {
        let mut queue = ByteBoundedFlowQueue::new(4 * 1024 * 1024).unwrap();
        let mut probe = ContinuousPumpProbe::default();
        let start = Instant::now();
        let stall_start = 5_000 / 5;
        let stall_end = stall_start + 200 / 5;

        for step in 0..THIRTY_SECONDS_AT_100M_STEPS {
            let now = start + ONE_HUNDRED_MBIT_CADENCE.saturating_mul(step as u32);
            let before = queue.snapshot();
            assert!(
                queue.can_accept(ONE_HUNDRED_MBIT_CHUNK_BYTES),
                "4MiB queue should absorb a 200ms drain stall at step {step}: {before:?}"
            );
            probe.note_read_armed(before, ONE_HUNDRED_MBIT_CHUNK_BYTES);
            queue
                .try_push(Bytes::from(vec![0; ONE_HUNDRED_MBIT_CHUNK_BYTES]))
                .unwrap();
            probe.note_remote_read(now, ONE_HUNDRED_MBIT_CHUNK_BYTES, true);

            if !(stall_start..stall_end).contains(&step) {
                let drain_limit = if step >= stall_end {
                    256 * 1024
                } else {
                    ONE_HUNDRED_MBIT_CHUNK_BYTES
                };
                if let Some(drained) = queue.pop_up_to(drain_limit) {
                    probe.note_drain(drained.len(), queue.snapshot());
                }
            }
        }

        while let Some(drained) = queue.pop_up_to(256 * 1024) {
            probe.note_drain(drained.len(), queue.snapshot());
        }

        let stats = probe.stats();
        assert_eq!(stats.remote_read_bytes, stats.drained_bytes);
        assert_eq!(queue.snapshot().queued_bytes, 0);
        assert_eq!(stats.queue_full_waits, 0);
        assert!(
            stats.max_queue_bytes >= 2 * 1024 * 1024,
            "stall should exercise real queue buffering, got {}",
            stats.max_queue_bytes
        );
        assert!(
            stats.max_remote_read_gap_with_capacity <= ONE_HUNDRED_MBIT_CADENCE,
            "drain jitter must not create remote read gaps while queue has capacity: {:?}",
            stats.max_remote_read_gap_with_capacity
        );
    }

    #[test]
    fn continuous_pump_pauses_only_at_explicit_queue_full_edge() {
        let mut queue = ByteBoundedFlowQueue::new(128 * 1024).unwrap();
        let mut probe = ContinuousPumpProbe::default();
        let start = Instant::now();

        for step in 0..4 {
            let now = start + ONE_HUNDRED_MBIT_CADENCE.saturating_mul(step);
            let before = queue.snapshot();
            if queue.can_accept(ONE_HUNDRED_MBIT_CHUNK_BYTES) {
                probe.note_read_armed(before, ONE_HUNDRED_MBIT_CHUNK_BYTES);
                queue
                    .try_push(Bytes::from(vec![0; ONE_HUNDRED_MBIT_CHUNK_BYTES]))
                    .unwrap();
                probe.note_remote_read(now, ONE_HUNDRED_MBIT_CHUNK_BYTES, true);
            } else {
                probe.note_queue_full_wait(before);
            }
        }

        let stats = probe.stats();
        assert_eq!(stats.remote_read_chunks, 2);
        assert_eq!(stats.queue_full_waits, 2);
        assert_eq!(stats.max_queue_bytes, 128 * 1024);
        assert!(
            stats.max_remote_read_gap_with_capacity <= ONE_HUNDRED_MBIT_CADENCE,
            "queue-full pauses must not be counted as unexplained read gaps"
        );
    }

    #[tokio::test]
    async fn async_byte_queue_waits_until_drain_creates_capacity() {
        let queue = AsyncByteBoundedFlowQueue::new(128 * 1024).unwrap();
        queue.push(Bytes::from(vec![1; 64 * 1024])).await.unwrap();
        queue.push(Bytes::from(vec![2; 64 * 1024])).await.unwrap();

        let producer_queue = queue.clone();
        let producer = tokio::spawn(async move {
            producer_queue
                .push(Bytes::from(vec![3; 32 * 1024]))
                .await
                .unwrap();
        });

        tokio::task::yield_now().await;
        assert!(
            !producer.is_finished(),
            "producer must wait when the byte queue is full"
        );

        let drained = queue.pop_up_to(64 * 1024).await.unwrap();
        assert_eq!(drained.len(), 64 * 1024);
        producer.await.unwrap();

        let snapshot = queue.snapshot().await;
        assert_eq!(snapshot.queued_bytes, 96 * 1024);
        assert_eq!(snapshot.high_water_bytes, 128 * 1024);
    }

    #[tokio::test]
    async fn async_byte_queue_wait_read_len_tracks_available_capacity() {
        let queue = AsyncByteBoundedFlowQueue::new(128 * 1024).unwrap();
        queue.push(Bytes::from(vec![1; 128 * 1024])).await.unwrap();

        let waiter_queue = queue.clone();
        let waiter = tokio::spawn(async move { waiter_queue.wait_read_len(64 * 1024).await });

        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "remote reader must not arm another read while the byte queue is full"
        );

        let drained = queue.pop_up_to(48 * 1024).await.unwrap();
        assert_eq!(drained.len(), 48 * 1024);
        assert_eq!(waiter.await.unwrap().unwrap(), 48 * 1024);
    }

    #[tokio::test]
    async fn async_byte_queue_recv_waits_for_producer_and_releases_capacity() {
        let queue = AsyncByteBoundedFlowQueue::new(128 * 1024).unwrap();
        let consumer_queue = queue.clone();
        let consumer = tokio::spawn(async move { consumer_queue.recv_up_to(64 * 1024).await });

        tokio::task::yield_now().await;
        assert!(
            !consumer.is_finished(),
            "consumer should wait for producer data"
        );

        queue.push(Bytes::from(vec![4; 96 * 1024])).await.unwrap();
        let consumed = consumer.await.unwrap().unwrap();
        assert_eq!(consumed.len(), 64 * 1024);
        assert_eq!(queue.snapshot().await.queued_bytes, 32 * 1024);

        queue.push(Bytes::from(vec![5; 96 * 1024])).await.unwrap();
        assert_eq!(queue.snapshot().await.queued_bytes, 128 * 1024);
    }

    #[tokio::test]
    async fn async_byte_queue_closes_waiters_without_data_loss() {
        let consumer_queue = AsyncByteBoundedFlowQueue::new(64 * 1024).unwrap();
        consumer_queue
            .push(Bytes::from(vec![1; 16 * 1024]))
            .await
            .unwrap();

        let first = consumer_queue.recv_up_to(64 * 1024).await.unwrap();
        assert_eq!(first.len(), 16 * 1024);

        let waiting_consumer_queue = consumer_queue.clone();
        let consumer =
            tokio::spawn(async move { waiting_consumer_queue.recv_up_to(64 * 1024).await });

        let reader_queue = AsyncByteBoundedFlowQueue::new(64 * 1024).unwrap();
        reader_queue
            .push(Bytes::from(vec![2; 64 * 1024]))
            .await
            .unwrap();
        let waiting_reader_queue = reader_queue.clone();
        let reader = tokio::spawn(async move { reader_queue.wait_read_len(64 * 1024).await });

        tokio::task::yield_now().await;
        consumer_queue.close().await;
        waiting_reader_queue.close().await;

        assert!(consumer.await.unwrap().is_none());
        assert_eq!(reader.await.unwrap(), Err(AsyncByteQueueWait::Closed));
    }

    #[tokio::test]
    async fn async_byte_queue_close_allows_draining_already_queued_bytes() {
        let queue = AsyncByteBoundedFlowQueue::new(64 * 1024).unwrap();
        queue.push(Bytes::from(vec![1; 48 * 1024])).await.unwrap();

        queue.close().await;

        let drained = queue.recv_up_to(64 * 1024).await.unwrap();
        assert_eq!(drained.len(), 48 * 1024);
        assert_eq!(
            queue.recv_up_to(64 * 1024).await,
            None,
            "closed queue should report EOF only after queued bytes are drained"
        );
    }

    #[tokio::test]
    async fn async_byte_queue_close_releases_full_queue_producer() {
        let queue = AsyncByteBoundedFlowQueue::new(64 * 1024).unwrap();
        queue.push(Bytes::from(vec![1; 64 * 1024])).await.unwrap();

        let producer_queue = queue.clone();
        let producer =
            tokio::spawn(async move { producer_queue.push(Bytes::from(vec![2; 1])).await });

        tokio::task::yield_now().await;
        assert!(
            !producer.is_finished(),
            "producer should wait while the queue is full"
        );

        queue.close().await;
        assert_eq!(producer.await.unwrap(), Err(ByteQueuePushError::Closed));
        assert_eq!(
            queue.try_push(Bytes::from(vec![3; 1])).await,
            Err(ByteQueuePushError::Closed)
        );
        assert_eq!(queue.snapshot().await.queued_bytes, 64 * 1024);
    }

    #[tokio::test]
    async fn leased_byte_queue_holds_capacity_until_permit_release() {
        let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
        queue.push(Bytes::from(vec![1; 128 * 1024])).await.unwrap();

        let mut leased = queue.recv_up_to(64 * 1024).await.unwrap();
        assert_eq!(leased.bytes().len(), 64 * 1024);
        assert_eq!(queue.snapshot().await.queued_bytes, 64 * 1024);
        assert_eq!(queue.snapshot().await.leased_bytes, 64 * 1024);
        assert_eq!(queue.snapshot().await.available_bytes(), 0);

        let producer_queue = queue.clone();
        let producer =
            tokio::spawn(async move { producer_queue.push(Bytes::from(vec![2; 1])).await });
        tokio::task::yield_now().await;
        assert!(
            !producer.is_finished(),
            "dispatcher pop must not release reader capacity before local admission"
        );

        assert_eq!(leased.permit_mut().release(32 * 1024), 32 * 1024);
        tokio::task::yield_now().await;
        assert!(
            producer.is_finished(),
            "partial local admission should release matching reader capacity"
        );
        producer.await.unwrap().unwrap();

        assert_eq!(leased.permit_mut().release(usize::MAX), 32 * 1024);
        let snapshot = queue.snapshot().await;
        assert_eq!(snapshot.leased_bytes, 0);
        assert_eq!(snapshot.queued_bytes, 64 * 1024 + 1);
    }

    #[tokio::test]
    async fn read_reservation_is_counted_before_commit_and_refunded_on_drop() {
        let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
        let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
        assert_eq!(reservation.max_len(), 64 * 1024);
        assert_eq!(queue.snapshot().await.reserved_bytes, 64 * 1024);
        drop(reservation);
        assert_eq!(queue.snapshot().await.reserved_bytes, 0);
    }

    #[tokio::test]
    async fn read_reservation_commit_refunds_unused_capacity() {
        let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
        let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
        assert!(reservation.commit(Bytes::from(vec![1; 16 * 1024])).unwrap());
        let snapshot = queue.snapshot().await;
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.queued_bytes, 16 * 1024);
        assert_eq!(snapshot.available_bytes(), 112 * 1024);
    }

    #[tokio::test]
    async fn read_reservation_rejects_overcommit_and_refunds_capacity() {
        let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
        let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
        assert_eq!(
            reservation.commit(Bytes::from(vec![1; 64 * 1024 + 1])),
            Err(ByteQueuePushError::ChunkExceedsCapacity {
                len: 64 * 1024 + 1,
                capacity: 64 * 1024,
            })
        );
        let snapshot = queue.snapshot().await;
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.queued_bytes, 0);
        assert_eq!(snapshot.available_bytes(), 128 * 1024);
    }

    #[tokio::test]
    async fn read_reservation_preserves_owned_byte_cap_across_queue_lifecycle() {
        const CAPACITY: usize = 128 * 1024;

        let queue = AsyncLeasedByteFlowQueue::new(CAPACITY).unwrap();
        let first = queue.reserve_read_up_to(96 * 1024).await.unwrap();
        let snapshot = queue.snapshot().await;
        assert_eq!(snapshot.owned_bytes(), 96 * 1024);
        assert!(snapshot.owned_bytes() <= CAPACITY);

        first.commit(Bytes::from(vec![1; 64 * 1024])).unwrap();
        let second = queue.reserve_read_up_to(64 * 1024).await.unwrap();
        let snapshot = queue.snapshot().await;
        assert_eq!(snapshot.queued_bytes, 64 * 1024);
        assert_eq!(snapshot.reserved_bytes, 64 * 1024);
        assert_eq!(snapshot.owned_bytes(), CAPACITY);
        assert!(snapshot.high_water_bytes <= CAPACITY);

        drop(second);
        let mut leased = queue.recv_up_to(48 * 1024).await.unwrap();
        let snapshot = queue.snapshot().await;
        assert_eq!(snapshot.queued_bytes, 16 * 1024);
        assert_eq!(snapshot.leased_bytes, 48 * 1024);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.owned_bytes(), 64 * 1024);
        assert!(snapshot.owned_bytes() <= CAPACITY);

        assert_eq!(leased.permit_mut().release(16 * 1024), 16 * 1024);
        assert_eq!(queue.snapshot().await.owned_bytes(), 48 * 1024);
        drop(leased);

        let snapshot = queue.snapshot().await;
        assert_eq!(snapshot.queued_bytes, 16 * 1024);
        assert_eq!(snapshot.leased_bytes, 0);
        assert_eq!(snapshot.owned_bytes(), 16 * 1024);
        assert!(snapshot.high_water_bytes <= CAPACITY);
    }

    #[tokio::test]
    async fn read_reservation_waiter_rearms_after_capacity_release() {
        let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
        let first = queue.reserve_read_up_to(128 * 1024).await.unwrap();
        let waiting_queue = queue.clone();
        let waiter = tokio::spawn(async move { waiting_queue.reserve_read_up_to(64 * 1024).await });

        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        drop(first);

        let second = tokio::time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("released reservation must wake the next reader")
            .unwrap()
            .unwrap();
        assert_eq!(second.max_len(), 64 * 1024);
    }

    #[tokio::test]
    async fn read_reservation_closed_commit_refunds_exactly_once() {
        let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
        let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
        queue
            .close_for_terminal_and_mark_ready("test", "reservation_commit_after_close")
            .await;

        assert_eq!(
            reservation.commit(Bytes::from(vec![1; 16 * 1024])),
            Err(ByteQueuePushError::Closed)
        );
        let snapshot = queue.snapshot().await;
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.queued_bytes, 0);
        assert_eq!(snapshot.available_bytes(), 128 * 1024);
    }

    #[tokio::test]
    async fn leased_queue_readiness_coalesces_until_open_empty_is_observed() {
        let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
        let first = queue.reserve_read_up_to(16 * 1024).await.unwrap();
        assert!(first.commit(Bytes::from(vec![1; 16 * 1024])).unwrap());
        let second = queue.reserve_read_up_to(16 * 1024).await.unwrap();
        assert!(!second.commit(Bytes::from(vec![2; 16 * 1024])).unwrap());

        let LeasedQueuePoll::Data(leased) = queue.try_recv_up_to(32 * 1024) else {
            panic!("one readiness event must expose all queued data to the actor");
        };
        assert_eq!(leased.bytes().len(), 32 * 1024);
        drop(leased);
        assert!(matches!(
            queue.try_recv_up_to(32 * 1024),
            LeasedQueuePoll::Empty
        ));

        let third = queue.reserve_read_up_to(16 * 1024).await.unwrap();
        assert!(
            third.commit(Bytes::from(vec![3; 16 * 1024])).unwrap(),
            "observing an open empty queue must rearm one future readiness wake"
        );
    }

    #[tokio::test]
    async fn leased_queue_close_reuses_pending_wake_and_reports_closed_after_data() {
        let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
        let reservation = queue.reserve_read_up_to(16 * 1024).await.unwrap();
        assert!(reservation.commit(Bytes::from(vec![1; 16 * 1024])).unwrap());
        assert!(
            !queue.close_for_remote_eof_and_mark_ready().await,
            "the queued payload wake must also carry the later close state"
        );

        let LeasedQueuePoll::Data(leased) = queue.try_recv_up_to(128 * 1024) else {
            panic!("closed queue must drain its owned payload first");
        };
        drop(leased);
        assert!(matches!(
            queue.try_recv_up_to(128 * 1024),
            LeasedQueuePoll::Closed
        ));
        assert!(!queue.close_for_remote_eof_and_mark_ready().await);
    }

    #[tokio::test]
    async fn leased_queue_closed_waits_for_reservation_refund() {
        let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
        let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
        assert!(queue.close_for_remote_eof_and_mark_ready().await);
        assert!(matches!(
            queue.try_recv_up_to(128 * 1024),
            LeasedQueuePoll::Empty
        ));

        drop(reservation);
        assert!(matches!(
            queue.try_recv_up_to(128 * 1024),
            LeasedQueuePoll::Closed
        ));
    }

    #[tokio::test]
    async fn leased_queue_terminal_drop_is_exact_and_idempotent() {
        let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
        let reservation = queue.reserve_read_up_to(32 * 1024).await.unwrap();
        reservation.commit(Bytes::from(vec![1; 32 * 1024])).unwrap();

        assert_eq!(
            queue.close_and_drop_queued_for_terminal("test", "terminal_drop"),
            32 * 1024
        );
        let snapshot = queue.snapshot_now();
        assert!(snapshot.closure.is_closed());
        assert_eq!(
            snapshot.closure.terminal_cause(),
            Some(LeasedByteQueueTerminalCause {
                direction: "test",
                reason: "terminal_drop",
            })
        );
        assert_eq!(snapshot.queued_bytes, 0);
        assert_eq!(snapshot.leased_bytes, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(
            queue.close_and_drop_queued_for_terminal("test", "different_reason"),
            0
        );
        assert_eq!(
            queue.snapshot_now().closure.terminal_cause(),
            Some(LeasedByteQueueTerminalCause {
                direction: "test",
                reason: "terminal_drop",
            }),
            "the first terminal cause must remain authoritative"
        );
    }

    #[tokio::test]
    async fn d16_global_budget_counts_reservation_and_owned_bytes_across_flows() {
        let budget = D16GlobalByteBudget::new(128 * 1024).unwrap();
        let first = AsyncLeasedByteFlowQueue::new_with_global_budget(
            128 * 1024,
            DownstreamPermitReleaseMode::OnEgressDrain,
            budget.clone(),
        )
        .unwrap();
        let second = AsyncLeasedByteFlowQueue::new_with_global_budget(
            128 * 1024,
            DownstreamPermitReleaseMode::OnEgressDrain,
            budget.clone(),
        )
        .unwrap();

        let first_reservation = first.reserve_read_up_to(96 * 1024).await.unwrap();
        let second_reservation = second.reserve_read_up_to(64 * 1024).await.unwrap();
        assert_eq!(first_reservation.max_len(), 96 * 1024);
        assert_eq!(second_reservation.max_len(), 32 * 1024);
        assert_eq!(budget.snapshot().reserved_bytes, 128 * 1024);
        assert_eq!(budget.snapshot().owned_bytes, 0);

        first_reservation
            .commit(Bytes::from(vec![1; 64 * 1024]))
            .unwrap();
        second_reservation
            .commit(Bytes::from(vec![2; 32 * 1024]))
            .unwrap();
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.owned_bytes, 96 * 1024);
        assert!(snapshot.total_bytes() <= snapshot.capacity_bytes);

        let mut first_lease = first.recv_up_to(64 * 1024).await.unwrap();
        let mut second_lease = second.recv_up_to(32 * 1024).await.unwrap();
        assert_eq!(first_lease.permit_mut().release(usize::MAX), 64 * 1024);
        assert_eq!(second_lease.permit_mut().release(usize::MAX), 32 * 1024);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.total_bytes(), 0);
        assert!(snapshot.high_water_bytes <= 128 * 1024);
    }

    #[tokio::test]
    async fn d16_global_budget_waiter_wakes_after_other_flow_releases() {
        let budget = D16GlobalByteBudget::new(128 * 1024).unwrap();
        let first = AsyncLeasedByteFlowQueue::new_with_global_budget(
            128 * 1024,
            DownstreamPermitReleaseMode::OnEgressDrain,
            budget.clone(),
        )
        .unwrap();
        let second = AsyncLeasedByteFlowQueue::new_with_global_budget(
            128 * 1024,
            DownstreamPermitReleaseMode::OnEgressDrain,
            budget.clone(),
        )
        .unwrap();
        let reservation = first.reserve_read_up_to(128 * 1024).await.unwrap();
        reservation
            .commit(Bytes::from(vec![3; 128 * 1024]))
            .unwrap();
        let waiting = second.clone();
        let waiter = tokio::spawn(async move { waiting.reserve_read_up_to(64 * 1024).await });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        let mut leased = first.recv_up_to(64 * 1024).await.unwrap();
        assert_eq!(leased.permit_mut().release(64 * 1024), 64 * 1024);
        let reservation = tokio::time::timeout(Duration::from_millis(200), waiter)
            .await
            .expect("global release must wake another flow")
            .unwrap()
            .unwrap();
        assert_eq!(reservation.max_len(), 64 * 1024);
        drop(reservation);
        drop(leased);
        assert_eq!(budget.snapshot().total_bytes(), 64 * 1024);
    }

    #[tokio::test]
    async fn d16_global_budget_accounts_direct_push_and_lease_release() {
        let budget = D16GlobalByteBudget::new(64 * 1024).unwrap();
        let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
            64 * 1024,
            DownstreamPermitReleaseMode::OnEgressDrain,
            budget.clone(),
        )
        .unwrap();
        queue.push(Bytes::from(vec![9; 64 * 1024])).await.unwrap();
        assert_eq!(budget.snapshot().owned_bytes, 64 * 1024);

        let leased = queue.recv_up_to(64 * 1024).await.unwrap();
        drop(leased);
        assert_eq!(budget.snapshot().total_bytes(), 0);
    }
}

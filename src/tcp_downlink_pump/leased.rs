//! Leased FIFO capacity, reservations, downstream permits and shared D16 budget.
//! Owns their single physical queue and release accounting; no actor, DATA
//! scheduling, Flow or Node authority. Public API remains at tcp_downlink_pump.
//! Original characterization tests: parent tests/queue.rs.
use super::storage::Chunks;
use super::{
    AsyncByteQueueWait, ByteChunk, ByteQueueConfigError, ByteQueueCustody, ByteQueuePushError,
};
use bytes::{Bytes, BytesMut};
use std::sync::{Arc, Mutex as StdMutex};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeasedByteQueueSnapshot {
    pub queued_bytes: usize,
    pub leased_bytes: usize,
    pub reserved_bytes: usize,
    /// Monotonic evidence that owned payload advanced toward local egress.
    pub local_progress_bytes: u64,
    pub capacity_bytes: usize,
    pub high_water_bytes: usize,
    pub chunks: usize,
    pub closure: LeasedByteQueueClosure,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct PermitQueueReleaseAudit {
    pub(crate) emitted: usize,
    pub(crate) consumed: usize,
    pub(crate) discarded: usize,
    pub(crate) queued: usize,
    pub(crate) leased: usize,
    pub(crate) released: usize,
    pub(crate) packet_flush: usize,
    pub(crate) ack_drain: usize,
    pub(crate) explicit_other: usize,
    pub(crate) dropped: usize,
    pub(crate) interrupted: bool,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum PermitReleaseCause {
    PacketFlush = 0,
    AckDrain = 1,
    ExplicitOther = 2,
    Drop = 3,
}

#[cfg(all(test, feature = "harness"))]
#[derive(Clone, Copy)]
pub(crate) enum PermitProvedReleaseSource {
    PacketFlush,
    AckDrain,
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

    fn commit(&mut self, bytes: usize) -> bool {
        debug_assert!(bytes <= self.reserved_bytes);
        let mut state = self.inner.lock_state();
        state.reserved_bytes = state.reserved_bytes.saturating_sub(self.reserved_bytes);
        state.owned_bytes = state.owned_bytes.saturating_add(bytes);
        let released = self.reserved_bytes.saturating_sub(bytes);
        self.reserved_bytes = 0;
        state.note_high_water();
        drop(state);
        released > 0
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
    start: super::native_wait::Start,
    waiters: std::sync::OnceLock<super::native_wait::Waiters>,
    stock: std::sync::OnceLock<super::output_stock::StockKey>,
    read: super::output_stock::ReadAdmission,
    state: StdMutex<LeasedByteFlowQueueState>,
    meter: std::sync::OnceLock<bytes::BackingMeter>,
    not_empty: Notify,
    not_full: Notify,
    global_budget: Option<D16GlobalByteBudget>,
}

#[derive(Debug)]
struct LeasedByteFlowQueueState {
    chunks: Chunks,
    custody: ByteQueueCustody,
    custodied: bool,
    released: usize,
    #[cfg(test)]
    release_causes: [usize; 4],
    local_disposed: bool,
    queued_bytes: usize,
    leased_bytes: usize,
    reserved_bytes: usize,
    local_progress_bytes: u64,
    wake_pending: bool,
    capacity_bytes: usize,
    high_water_bytes: usize,
    release_mode: DownstreamPermitReleaseMode,
    closure: LeasedByteQueueClosure,
}

impl LeasedByteFlowQueueState {
    fn new(capacity_bytes: usize, release_mode: DownstreamPermitReleaseMode) -> Self {
        Self {
            chunks: Chunks::default(),
            custody: ByteQueueCustody::default(),
            custodied: false,
            released: 0,
            #[cfg(test)]
            release_causes: [0; 4],
            local_disposed: false,
            queued_bytes: 0,
            leased_bytes: 0,
            reserved_bytes: 0,
            local_progress_bytes: 0,
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
            local_progress_bytes: self.local_progress_bytes,
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
        if self.chunks.front().is_some_and(|c| c.bytes.has_stock()) {
            if let Some(chunk) = self.chunks.front() { chunk.bytes.quarantine_stock(); }
            return None;
        }
        if self.custodied {
            self.custody.interrupted = true;
        }
        self.pop_owned(limit).and_then(|(bytes, _)| bytes.try_into_raw_legacy().ok())
    }
    fn pop_owned(&mut self, limit: usize) -> Option<(super::CustodiedBytes, usize)> {
        if limit == 0 {
            return None;
        }
        if let Some((bytes, capacity)) = self.chunks.pop_native(limit) {
            self.queued_bytes -= bytes.len();
            self.leased_bytes += bytes.len();
            self.local_progress_bytes =
                self.local_progress_bytes.saturating_add(bytes.len() as u64);
            self.note_high_water();
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
            self.leased_bytes = self.leased_bytes.saturating_add(take);
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
            self.local_progress_bytes = self.local_progress_bytes.saturating_add(out.len() as u64);
            self.note_high_water();
            let allocation = out.capacity();
            Some((out.freeze().into(), allocation))
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
                start: super::native_wait::Start::default(),
                waiters: std::sync::OnceLock::new(),
                stock: std::sync::OnceLock::new(),
                read: super::output_stock::ReadAdmission::default(),
                state: StdMutex::new(LeasedByteFlowQueueState::new(capacity_bytes, release_mode)),
                meter: std::sync::OnceLock::new(),
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

    #[cfg(test)]
    pub(crate) fn custodied_released_bytes_for_test(&self) -> Option<usize> {
        let state = self.inner.lock_state();
        state.custodied.then_some(state.released)
    }

    #[cfg(test)]
    pub(crate) fn permit_release_audit_for_test(&self) -> Option<PermitQueueReleaseAudit> {
        let state = self.inner.lock_state();
        state.custodied.then_some(PermitQueueReleaseAudit {
            emitted: state.custody.emitted,
            consumed: state.custody.consumed,
            discarded: state.custody.discarded_bytes,
            queued: state.queued_bytes,
            leased: state.leased_bytes,
            released: state.released,
            packet_flush: state.release_causes[PermitReleaseCause::PacketFlush as usize],
            ack_drain: state.release_causes[PermitReleaseCause::AckDrain as usize],
            explicit_other: state.release_causes[PermitReleaseCause::ExplicitOther as usize],
            dropped: state.release_causes[PermitReleaseCause::Drop as usize],
            interrupted: state.custody.interrupted,
        })
    }

    pub async fn reserve_read_up_to(
        &self,
        requested: usize,
    ) -> Result<ByteQueueReadReservation, AsyncByteQueueWait> {
        self.inner.start.begin();
        if let Some(w) = self.inner.waiters.get() {
            w.reject_raw();
            return Err(AsyncByteQueueWait::Quarantined);
        }
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
            if state.custodied {
                state.custody.interrupted = true;
            }
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
        self.inner.start.begin();
        match self.inner.waiters.get() {
            Some(w) => w
                .run(
                    super::native_wait::Role::Producer,
                    self.wait_read_len_inner(max_read_len),
                )
                .await
                .map_err(|e| match e {
                    super::native_wait::RunError::Closed => AsyncByteQueueWait::Closed,
                    super::native_wait::RunError::Quarantined => AsyncByteQueueWait::Quarantined,
                })?,
            None => self.wait_read_len_inner(max_read_len).await,
        }
    }
    async fn wait_read_len_inner(&self, max_read_len: usize) -> Result<usize, AsyncByteQueueWait> {
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
                    if state.custodied {
                        state.custody.interrupted = true;
                    }
                    if state.closure.is_closed() {
                        return Err(ByteQueuePushError::Closed);
                    }
                    state
                        .chunks
                        .prepare(bytes.backing_meter())
                        .map_err(ByteQueuePushError::Capacity)?;
                    self.inner.retain_meter(state.chunks.meter());
                    if len <= state.available_bytes() && global_budget.inner.try_acquire_owned(len)
                    {
                        state.queued_bytes = state.queued_bytes.saturating_add(len);
                        state.note_high_water();
                        state.chunks.push_back(ByteChunk {
                            bytes: bytes.into(),
                            allocation: None,
                        });
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
                if state.custodied {
                    state.custody.interrupted = true;
                }
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
                    state
                        .chunks
                        .prepare(bytes.backing_meter())
                        .map_err(ByteQueuePushError::Capacity)?;
                    self.inner.retain_meter(state.chunks.meter());
                    state.queued_bytes = state.queued_bytes.saturating_add(len);
                    state.note_high_water();
                    state.chunks.push_back(ByteChunk {
                        bytes: bytes.into(),
                        allocation: None,
                    });
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
        self.inner.start.begin();
        if let Some(w) = self.inner.waiters.get() {
            w.reject_raw();
            return LeasedQueuePoll::Empty;
        }
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
        self.inner.start.begin();
        if let Some(w) = self.inner.waiters.get() {
            w.reject_raw();
            return std::future::pending().await;
        }
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
        self.inner.start.begin();
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
        self.inner.start.begin();
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
        self.inner.start.begin();
        if let Some(waiters) = self.inner.waiters.get() { waiters.reject_raw(); return 0; }
        let mut state = self.inner.lock_state();
        if state.custodied {
            state.custody.interrupted = true;
        }
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
    fn retain_meter(&self, meter: Option<&bytes::BackingMeter>) {
        if let Some(m) = meter {
            let original = self.meter.get_or_init(|| m.clone());
            if !original.same_authority(m) {
                original.quarantine();
                m.quarantine();
            }
        }
    }
    fn lock_state(&self) -> std::sync::MutexGuard<'_, LeasedByteFlowQueueState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                if let Some(m) = self.meter.get() {
                    m.quarantine();
                }
                let mut state = poisoned.into_inner();
                if state.custodied {
                    state.custody.interrupted = true;
                }
                state
            }
        }
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

    pub fn commit(self, bytes: Bytes) -> Result<bool, ByteQueuePushError> {
        self.commit_retaining(bytes).map_err(|(error, _)| error)
    }

    /// Root custody retains the exact backing when a close or invalid chunk
    /// rejects the transfer. The reservation still releases its own quota.
    pub(crate) fn commit_retaining(
        self,
        bytes: Bytes,
    ) -> Result<bool, (ByteQueuePushError, Bytes)> {
        self.commit_root_retaining(bytes)
            .map_err(|(error, bytes, reservation)| {
                drop(reservation);
                (error, bytes)
            })
    }

    /// Root receives the original reservation too, so it can retain a rejected
    /// backing before releasing quota or waking any other task.
    pub(crate) fn commit_root_retaining(
        mut self,
        bytes: Bytes,
    ) -> Result<bool, (ByteQueuePushError, Bytes, Self)> {
        let len = bytes.len();
        if len == 0 {
            return Err((ByteQueuePushError::EmptyChunk, bytes, self));
        }
        if len > self.reserved_bytes {
            return Err((
                ByteQueuePushError::ChunkExceedsCapacity {
                    len,
                    capacity: self.reserved_bytes,
                },
                bytes,
                self,
            ));
        }

        let mut state = self.inner.lock_state();
        if state.custodied {
            state.custody.interrupted = true;
        }
        if state.closure.is_closed() {
            drop(state);
            return Err((ByteQueuePushError::Closed, bytes, self));
        }
        if let Err(error) = state.chunks.prepare(bytes.backing_meter()) {
            drop(state);
            return Err((ByteQueuePushError::Capacity(error), bytes, self));
        }
        self.inner.retain_meter(state.chunks.meter());
        let unused_bytes = self.reserved_bytes.saturating_sub(len);
        state.reserved_bytes = state.reserved_bytes.saturating_sub(self.reserved_bytes);
        let global_unused = self
            .global_reservation
            .as_mut()
            .is_some_and(|global_reservation| global_reservation.commit(len));
        self.reserved_bytes = 0;
        let should_wake = state.queued_bytes == 0 && !state.wake_pending;
        state.queued_bytes = state.queued_bytes.saturating_add(len);
        state.chunks.push_back(ByteChunk {
            bytes: bytes.into(),
            allocation: None,
        });
        state.wake_pending = true;
        state.note_high_water();
        drop(state);

        if unused_bytes > 0 {
            self.inner.not_full.notify_waiters();
        }
        if global_unused {
            if let Some(global_reservation) = self.global_reservation.as_ref() {
                global_reservation.inner.not_full.notify_waiters();
            }
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

/// Read-only weak identity: retains no DATA or queue authority.
#[derive(Debug)]
pub(crate) struct PermitQueueIdentity(std::sync::Weak<AsyncLeasedByteFlowQueueInner>);
impl PermitQueueIdentity {
    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        std::sync::Weak::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn from_queue(queue: &AsyncLeasedByteFlowQueue) -> Self {
        Self(Arc::downgrade(&queue.inner))
    }
    pub(crate) fn matches_queue(&self, queue: &AsyncLeasedByteFlowQueue) -> bool {
        self.0.as_ptr() == Arc::as_ptr(&queue.inner)
    }
    pub(crate) fn matches(
        &self,
        permit: &DownstreamBytePermit,
        mode: DownstreamPermitReleaseMode,
    ) -> bool {
        self.0.as_ptr() == Arc::as_ptr(&permit.inner) && permit.release_mode == mode
    }
}

impl DownstreamBytePermit {
    pub(crate) fn native_custody_for(&self, meter: &bytes::BackingMeter) -> bool {
        self.inner
            .meter
            .get()
            .is_some_and(|actual| actual.same_budget(meter))
            && self
                .inner
                .waiters
                .get()
                .is_some_and(|root| root.uses_budget(meter))
    }

    /// Aggregation preserves the original queue's leased total. It is only
    /// valid within one owner's pending or inflight phase, never across phases.
    pub(crate) fn can_merge(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
            && self.release_mode == other.release_mode
            && self.remaining_bytes > 0
            && other.remaining_bytes > 0
            && self
                .remaining_bytes
                .checked_add(other.remaining_bytes)
                .is_some()
    }

    pub(crate) fn transfer_to(&mut self, target: &mut Self, bytes: usize) -> Option<usize> {
        if !Arc::ptr_eq(&self.inner, &target.inner) || self.release_mode != target.release_mode {
            return None;
        }
        let moved = self.remaining_bytes.min(bytes);
        let next = target.remaining_bytes.checked_add(moved)?;
        target.remaining_bytes = next;
        self.remaining_bytes -= moved;
        Some(moved)
    }

    pub(crate) fn queue_identity(&self) -> PermitQueueIdentity {
        PermitQueueIdentity(Arc::downgrade(&self.inner))
    }
    pub fn remaining_bytes(&self) -> usize {
        self.remaining_bytes
    }

    pub fn release_mode(&self) -> DownstreamPermitReleaseMode {
        self.release_mode
    }

    pub fn release(&mut self, bytes: usize) -> usize {
        self.release_inner(
            bytes,
            #[cfg(test)]
            PermitReleaseCause::ExplicitOther,
        )
    }

    #[cfg(all(test, feature = "harness"))]
    pub(crate) fn release_proved_for_test(
        &mut self,
        bytes: usize,
        source: PermitProvedReleaseSource,
    ) -> usize {
        let cause = match source {
            PermitProvedReleaseSource::PacketFlush => PermitReleaseCause::PacketFlush,
            PermitProvedReleaseSource::AckDrain => PermitReleaseCause::AckDrain,
        };
        self.release_inner(bytes, cause)
    }

    fn release_inner(&mut self, bytes: usize, #[cfg(test)] cause: PermitReleaseCause) -> usize {
        let released = self.remaining_bytes.min(bytes);
        if released == 0 {
            return 0;
        }
        let mutation = super::MeteredMutation::dispose(self.inner.meter.get());
        self.remaining_bytes = self.remaining_bytes.saturating_sub(released);
        let mut state = self.inner.lock_state();
        state.leased_bytes = state.leased_bytes.saturating_sub(released);
        if state.custodied {
            #[cfg(test)]
            if let Some(total) = state.release_causes[cause as usize].checked_add(released) {
                state.release_causes[cause as usize] = total;
            } else {
                state.custody.interrupted = true;
            }
            if let Some(total) = state.released.checked_add(released) {
                state.released = total;
                if state.custody.consumed.checked_sub(total) != Some(state.leased_bytes) {
                    state.custody.interrupted = true;
                }
            } else {
                state.custody.interrupted = true;
            }
        }
        state.local_progress_bytes = state.local_progress_bytes.saturating_add(released as u64);
        let custodied = state.custodied;
        if state.custody.interrupted {
            if let Some(m) = self.inner.meter.get() {
                m.quarantine();
            }
        }
        drop(state);
        if let Some(global_budget) = &self.inner.global_budget {
            global_budget.inner.release_owned(released);
        }
        if custodied {
            if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.inner.not_full.notify_waiters()
            })) {
                self.inner.lock_state().custody.interrupted = true;
                std::panic::resume_unwind(panic);
            }
        } else {
            self.inner.not_full.notify_waiters();
        }
        mutation.commit();
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
        self.release_inner(
            usize::MAX,
            #[cfg(test)]
            PermitReleaseCause::Drop,
        );
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

mod custody;
pub(super) mod stock;
pub(crate) use custody::LeasedQueueBatch;

#[derive(Clone)]
pub(super) struct StockReturn(std::sync::Weak<AsyncLeasedByteFlowQueueInner>);
impl StockReturn {
    pub(super) async fn wait_returned(&self, root: &super::output_stock::Root) -> Result<(), super::output_stock::StockWaitError> {
        use super::output_stock::StockWaitError;
        let Some(queue) = self.0.upgrade() else {
            if root.returned()? { return Ok(()); }
            root.reject();
            return Err(StockWaitError::Admission(bytes::MeteredCopyError::Quarantined));
        };
        let Some(waiters) = queue.waiters.get() else {
            root.reject(); return Err(StockWaitError::Admission(bytes::MeteredCopyError::Quarantined));
        };
        match waiters.run(super::native_wait::Role::Producer, async {
            loop {
                let notified = queue.not_full.notified();
                tokio::pin!(notified); notified.as_mut().enable();
                if root.returned()? { return Ok(()); }
                notified.await;
            }
        }).await {
            Ok(result) => result,
            Err(super::native_wait::RunError::Closed) => Err(StockWaitError::Closed),
            Err(super::native_wait::RunError::Quarantined) => Err(StockWaitError::Admission(bytes::MeteredCopyError::Quarantined)),
        }
    }
    pub(super) fn notify(&self) -> bool {
        let Some(queue) = self.0.upgrade() else { return false; };
        queue.not_full.notify_waiters(); true
    }
    pub(super) async fn wait_prepare(
        &self,
        root: Arc<super::output_stock::Root>,
        max: usize,
    ) -> Result<super::output_stock::OutputSpace, super::output_stock::StockWaitError> {
        use super::output_stock::StockWaitError;
        let Some(queue) = self.0.upgrade() else { root.reject(); return Err(StockWaitError::Admission(bytes::MeteredCopyError::Quarantined)); };
        let Some(waiters) = queue.waiters.get() else { root.reject(); return Err(StockWaitError::Admission(bytes::MeteredCopyError::Quarantined)); };
        let result = waiters.run(super::native_wait::Role::Producer, async {
            loop {
                let notified = queue.not_full.notified();
                tokio::pin!(notified); notified.as_mut().enable();
                if queue.lock_state().closure.is_closed() { return Err(StockWaitError::Closed); }
                if let Some(space) = root.reserve(max, true).map_err(StockWaitError::Admission)? { return Ok(space); }
                notified.await;
            }
        }).await;
        match result {
            Ok(result) => result,
            Err(super::native_wait::RunError::Closed) => Err(StockWaitError::Closed),
            Err(super::native_wait::RunError::Quarantined) => Err(StockWaitError::Admission(bytes::MeteredCopyError::Quarantined)),
        }
    }
}

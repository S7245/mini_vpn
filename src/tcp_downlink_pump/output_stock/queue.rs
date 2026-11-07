//! Cold binding and exclusive Native producer port at the original FIFO.
//! Existing waiters own the Producer callback role; no second DATA queue.
use super::notify::QueueReturn;
use super::{OutputSpace, OutputStock, StockKey, StockLayout, StockProducer, StockWaitError};
use crate::tcp_downlink_pump::native_wait::QuietScope;
use crate::tcp_downlink_pump::native_wait::{Role, RunError};
use crate::tcp_downlink_pump::{AsyncByteBoundedFlowQueue, ByteCustodyError, OwnedPayload};
use crate::tcp_downlink_pump::{AsyncByteBoundedFlowQueueInner, ByteChunk, MeteredMutation};
use bytes::{BackingMeter, MeteredCopyError};
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

#[cfg(test)]
mod tests;
mod owner;

impl AsyncByteBoundedFlowQueue {
    pub(crate) fn new_with_output_stock(
        capacity: usize,
        meter: &BackingMeter,
        clock: crate::shared::errors::FailureClock,
        slab: usize,
        window: usize,
    ) -> Result<(Self, OutputStock), MeteredCopyError> {
        let shape = StockLayout::checked(capacity, slab, window)?;
        Self::new_with_stock_shape(capacity, meter, clock, shape)
    }
    pub(crate) fn new_with_progressive_output_stock(
        capacity: usize,
        meter: &BackingMeter,
        clock: crate::shared::errors::FailureClock,
        slab: usize,
        window: usize,
    ) -> Result<(Self, OutputStock), MeteredCopyError> {
        let shape = StockLayout::progressive(capacity, slab, window)?;
        Self::new_with_stock_shape(capacity, meter, clock, shape)
    }
    fn new_with_stock_shape(
        capacity: usize,
        meter: &BackingMeter,
        clock: crate::shared::errors::FailureClock,
        shape: StockLayout,
    ) -> Result<(Self, OutputStock), MeteredCopyError> {
        let queue = Self::new(capacity).map_err(|_| MeteredCopyError::TooLarge)?;
        let _cold = queue
            .inner
            .start
            .cold()
            .ok_or(MeteredCopyError::Quarantined)?;
        queue.bind_native_meter_cold(Some(meter.clone()))?;
        drop(_cold); // Fresh unpublished queue; failed clock quiet must run outside Start.
        if !queue.bind_native_failures(clock) {
            meter.quarantine();
            return Err(MeteredCopyError::Quarantined);
        }
        let stock = queue.prepare_output_stock_cold(meter, shape)?;
        Ok((queue, stock))
    }
    fn prepare_output_stock_cold(
        &self,
        meter: &BackingMeter,
        shape: StockLayout,
    ) -> Result<OutputStock, MeteredCopyError> {
        if self.inner.stock.get().is_some()
            || !self.inner.waiters.get().is_some_and(|w| w.stock_bound())
        {
            meter.quarantine();
            return Err(MeteredCopyError::Quarantined);
        }
        {
            let mut state = self.inner.state.try_lock().map_err(|_| {
                meter.quarantine();
                MeteredCopyError::Quarantined
            })?;
            if state.closed
                || !state
                    .queue
                    .chunks
                    .meter()
                    .is_some_and(|actual| actual.same_authority(meter))
            {
                meter.quarantine();
                return Err(MeteredCopyError::Quarantined);
            }
            state.queue.chunks.prepare(Some(meter.clone()))?;
        }
        let clock = self
            .inner
            .waiters
            .get()
            .and_then(|w| w.bound_clock())
            .ok_or(MeteredCopyError::Quarantined)?;
        let stock = OutputStock::prepare(
            meter,
            shape,
            QueueReturn::Bytes(Arc::downgrade(&self.inner)),
            clock,
        )?;
        let key = stock.key().ok_or(MeteredCopyError::Quarantined)?;
        if self.inner.stock.set(key).is_err() {
            meter.quarantine();
            return Err(MeteredCopyError::Quarantined);
        }
        Ok(stock)
    }
    pub(in crate::tcp_downlink_pump) async fn push_stock(
        &self,
        producer: &StockProducer,
        pending: &mut Option<OwnedPayload>,
    ) -> Result<(), ByteCustodyError> {
        self.inner.start.begin();
        let _quiet = self.inner.waiters.get().map(|w| w.quiet_scope());
        let valid = self.inner.stock.get().is_some_and(|key| {
            key.matches_producer(producer)
                && pending.as_ref().is_some_and(|p| match p {
                    OwnedPayload::Native(p) => key.matches_payload(p),
                    _ => false,
                })
        });
        if !valid {
            return Err(reject_owners(
                _quiet.as_ref(),
                producer.rejection_scope(),
                payload_scope(pending),
            ));
        }
        if self.inner.read.is_active() {
            // Keep the original component port's ordinary logical rejection;
            // this is not a backing allocator failure or an authority failure.
            return Err(ByteCustodyError::Rejected);
        }
        let Some(waiters) = self.inner.waiters.get() else {
            let quiet = producer.rejection_scope();
            quiet.mark_quarantined();
            return Err(ByteCustodyError::Invalid);
        };
        match waiters
            .run(Role::Producer, self.push_custodied_inner(pending))
            .await
        {
            Ok(result) => result,
            Err(RunError::Closed) => Err(ByteCustodyError::Rejected),
            Err(RunError::Quarantined) => Err(ByteCustodyError::Invalid),
        }
    }
}

// Complete cross-owner refusal in this original queue owner. Capturing all
// three scopes precedes every mark; payload then producer publication occurs
// here, while the caller retains the original outer receiver scope until last.
fn payload_scope(pending: &Option<OwnedPayload>) -> Option<QuietScope> {
    pending.as_ref().and_then(|payload| match payload {
        OwnedPayload::Native(bytes) => bytes.quiet_scope(),
        _ => None,
    })
}
fn reject_owners(
    receiver: Option<&QuietScope>,
    producer: QuietScope,
    actual: Option<QuietScope>,
) -> ByteCustodyError {
    if let Some(receiver) = receiver {
        receiver.mark_quarantined();
    }
    producer.mark_quarantined();
    if let Some(actual) = &actual {
        actual.mark_quarantined();
    }
    // Parameters retire actual, producer, then the caller's outer receiver.
    ByteCustodyError::Invalid
}

// This is the original queue's one exclusive read admission, not another DATA
// inventory. Consumers only release space; no other producer can spend the
// descriptor or byte credit while the granted token exists.
#[derive(Debug, Default)]
pub(in crate::tcp_downlink_pump) struct ReadAdmission {
    active: AtomicU64,
    next: AtomicU64,
    reserved: AtomicUsize,
}
impl ReadAdmission {
    pub(in crate::tcp_downlink_pump) fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire) != 0
    }
    pub(in crate::tcp_downlink_pump) fn matches(&self, id: u64, max: usize) -> bool {
        id != 0 && self.active.load(Ordering::Acquire) == id && self.reserved.load(Ordering::Acquire) == max
    }
    pub(in crate::tcp_downlink_pump) fn arm(&self, max: usize) -> Option<u64> {
        if self.is_active() || max == 0 { return None; }
        let id = self.next.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1)).ok()? + 1;
        self.reserved.store(max, Ordering::Relaxed);
        self.active.store(id, Ordering::Release);
        Some(id)
    }
    pub(in crate::tcp_downlink_pump) fn resize(&self, id: u64, old: usize, next: usize) -> bool {
        if next == 0 || next > old || !self.matches(id, old) { return false; }
        self.reserved.store(next, Ordering::Release); true
    }
    pub(in crate::tcp_downlink_pump) fn release(&self, id: u64, max: usize) -> bool {
        if !self.matches(id, max) { return false; }
        self.reserved.store(0, Ordering::Relaxed);
        self.active.compare_exchange(id, 0, Ordering::Release, Ordering::Acquire).is_ok()
    }
}
struct ReadClaim {
    queue: owner::Owner,
    id: u64,
    max: usize,
    meter: BackingMeter,
    quiet: QuietScope,
    armed: bool,
    committed: bool,
    notify_producer: bool,
}
impl ReadClaim {
    fn release(&mut self) -> bool {
        if !self.armed {
            return true;
        }
        if self.meter.is_quarantined() {
            return false;
        }
        if !self.queue.release(self.id, self.max, self.committed) {
            self.quiet.mark_quarantined();
            return false;
        }
        self.armed = false;
        self.queue.not_empty().notify_waiters();
        // An unsuccessful prepare must not wake its own not_full registration
        // and spin while every funded physical block still has a live loan.
        if self.notify_producer {
            self.queue.not_full().notify_waiters();
        }
        true
    }
}
impl Drop for ReadClaim {
    fn drop(&mut self) {
        self.release();
    }
}

pub(crate) struct StockReadOperation {
    space: Option<OutputSpace>,
    claim: ReadClaim,
    key: StockKey,
}
impl StockReadOperation {
    pub(crate) fn validate_unused(&self) -> Result<(), MeteredCopyError> {
        let _quiet = self.claim.quiet.clone();
        if !self.claim.armed || self.claim.queue.read().active.load(Ordering::Acquire) != self.claim.id {
            self.claim.quiet.mark_quarantined();
            return Err(MeteredCopyError::Quarantined);
        }
        let Some(space) = &self.space else {
            self.claim.quiet.mark_quarantined();
            return Err(MeteredCopyError::Quarantined);
        };
        space.validate_meter(Some(&self.claim.meter))
    }
    pub(crate) fn capacity(&self) -> usize {
        self.claim.max
    }
    pub(crate) fn space(&mut self) -> &mut Option<OutputSpace> {
        &mut self.space
    }
    fn valid_pending(&self, pending: &Option<OwnedPayload>) -> bool {
        self.space.is_none()
            && self.claim.armed
            && self.claim.queue.read().active.load(Ordering::Acquire) == self.claim.id
            && self.claim.queue.read().reserved.load(Ordering::Acquire) == self.claim.max
            && pending.as_ref().is_some_and(|payload| match payload {
                OwnedPayload::Native(bytes) => {
                    !bytes.is_empty()
                        && bytes.len() <= self.claim.max
                        && self.key.matches_payload(bytes)
                }
                _ => false,
            })
    }
    pub(crate) async fn publish(
        &mut self,
        pending: &mut Option<OwnedPayload>,
    ) -> Result<(), ByteCustodyError> {
        let queue = self.claim.queue.clone();
        let receiver = queue.waiters().map(|w| w.quiet_scope());
        // Do not let a receiver already in Q hide the actual pending owner.
        if !self.valid_pending(pending) {
            return Err(reject_owners(
                receiver.as_ref(),
                self.claim.quiet.clone(),
                payload_scope(pending),
            ));
        }
        let Some(waiters) = queue.waiters() else {
            return Err(reject_owners(
                receiver.as_ref(),
                self.claim.quiet.clone(),
                payload_scope(pending),
            ));
        };
        let result = match waiters
            .run(Role::Producer, self.publish_inner(pending))
            .await
        {
            Ok(result) => result,
            Err(RunError::Closed) => Err(reject_owners(
                receiver.as_ref(),
                self.claim.quiet.clone(),
                payload_scope(pending),
            )),
            Err(RunError::Quarantined) => Err(ByteCustodyError::Invalid),
        };
        // This qualification is released only after the actual publisher Work
        // has dropped and its original callback role has begun retirement.
        if result.is_ok() && !self.claim.release() {
            return Err(ByteCustodyError::Invalid);
        }
        result
    }
    async fn publish_inner(&mut self, pending: &mut Option<OwnedPayload>) -> Result<(), ByteCustodyError> {
        let queue = self.claim.queue.clone();
        queue.publish(pending, &mut self.claim).await
    }
}
impl Drop for StockReadOperation {
    fn drop(&mut self) {
        // End the real physical operation before releasing admission. On any
        // uncertainty the original id/credit stays occupied in charged Q.
        if let Some(space) = self.space.take() {
            let done = self
                .claim
                .queue.waiters()
                .is_some_and(|w| w.dispose_output_space(space));
            if !done {
                self.claim.quiet.mark_quarantined();
            }
        }
        self.claim.release();
    }
}
impl StockProducer {
    /// Poll the original read with its existing grant. Only after a real Pending
    /// may this same Producer turn retire one idle large backing under pressure.
    /// None means the grant was rebound from existing stock; recompute read len.
    /// Every Pending preserves the original grant. Cancellation owns no new DATA.
    pub(crate) async fn read_or_trim<T>(
        &mut self,
        ready: &mut Option<StockReadOperation>,
        mut read: impl FnMut(&mut std::task::Context<'_>, usize) -> std::task::Poll<T>,
    ) -> Result<Option<T>, (StockWaitError, Option<T>)> {
        use std::{future::{Future, poll_fn}, task::Poll};
        enum Step<T> { Read(T), Trim, Notification }
        let owner = self.rejection_scope();
        let Some(current) = ready.as_ref() else {
            owner.mark_quarantined();
            return Err((StockWaitError::Admission(MeteredCopyError::Quarantined), None));
        };
        let queue = current.claim.queue.clone();
        let receiver = queue.waiters().map(|waiters| waiters.quiet_scope());
        if !current.key.matches_producer(self)
            || !queue.stock().is_some_and(|key| key.matches_producer(self)) {
            reject_owners(receiver.as_ref(), owner, Some(current.claim.quiet.clone()));
            return Err((StockWaitError::Admission(MeteredCopyError::Quarantined), None));
        }
        let Some(space) = current.space.as_ref() else {
            reject_owners(receiver.as_ref(), owner, Some(current.claim.quiet.clone()));
            return Err((StockWaitError::Admission(MeteredCopyError::Quarantined), None));
        };
        if let Err(error) = self.validate_space_owner(space) {
            return Err((StockWaitError::Admission(error), None));
        }
        let Some(waiters) = queue.waiters() else {
            reject_owners(receiver.as_ref(), owner, Some(current.claim.quiet.clone()));
            return Err((StockWaitError::Admission(MeteredCopyError::Quarantined), None));
        };
        // NativeWait owns only the borrowed transaction. Its final Q must not
        // drop an already completed original input/error as a return temporary.
        let mut completed = None;
        let result = waiters.run(Role::Producer, async {
            let decision = loop {
                let notified = queue.not_full().notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let step = poll_fn(|cx| {
                    if waiters.is_closed() { return Poll::Ready(Err(StockWaitError::Closed)); }
                    let Some(current) = ready.as_ref() else {
                        owner.mark_quarantined();
                        return Poll::Ready(Err(StockWaitError::Admission(MeteredCopyError::Quarantined)));
                    };
                    if let Err(error) = current.validate_unused() {
                        return Poll::Ready(Err(StockWaitError::Admission(error)));
                    }
                    if let Poll::Ready(value) = read(cx, current.capacity()) {
                        return Poll::Ready(Ok(Step::Read(value)));
                    }
                    match current.space.as_ref().map(|space| self.idle_trim_candidate(space)) {
                        Some(Ok(true)) => return Poll::Ready(Ok(Step::Trim)),
                        Some(Ok(false)) => {},
                        Some(Err(error)) => return Poll::Ready(Err(error)),
                        None => {
                            owner.mark_quarantined();
                            return Poll::Ready(Err(StockWaitError::Admission(MeteredCopyError::Quarantined)));
                        },
                    }
                    notified.as_mut().poll(cx).map(|()| Ok(Step::Notification))
                }).await?;
                match step { Step::Notification => continue, step => break step }
            };
            if let Step::Read(value) = decision { completed = Some(value); return Ok(()); }
            // The old read has ended its borrow before any grant is changed.
            drop(read);
            let Some(current) = ready.as_mut() else {
                owner.mark_quarantined();
                return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
            };
            current.validate_unused().map_err(StockWaitError::Admission)?;
            let max = current.claim.max;
            drop(current.space.take());
            if !self.try_retire_idle_output()? {
                owner.mark_quarantined();
                return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
            }
            let Some(space) = self.reserve_existing(max).map_err(StockWaitError::Admission)? else {
                owner.mark_quarantined();
                return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
            };
            let next = space.capacity();
            if next == 0 || next > max {
                owner.mark_quarantined();
                return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
            }
            if !queue.resize(current.claim.id, current.claim.max, next) {
                owner.mark_quarantined();
                return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
            }
            current.claim.max = next;
            current.space = Some(space);
            Ok(())
        }).await;
        match result {
            Ok(Ok(())) => Ok(completed),
            Ok(Err(error)) => Err((error, completed)),
            Err(RunError::Closed) => Err((StockWaitError::Closed, completed)),
            Err(RunError::Quarantined) => Err((StockWaitError::Admission(MeteredCopyError::Quarantined), completed)),
        }
    }

    /// Metadata pressure may explicitly retire this Flow's idle large tail.
    /// The original Producer callback owns the entire wait/disposal turn.
    pub(crate) async fn retire_idle_output(&mut self) -> Result<(), StockWaitError> {
        let owner = self.rejection_scope();
        let Some(queue) = owner::Owner::from_return(&self.queue_return()) else {
            owner.mark_quarantined();
            return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        };
        let Some(waiters) = queue.waiters() else {
            owner.mark_quarantined();
            return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        };
        let receiver = waiters.quiet_scope();
        if !queue.stock().is_some_and(|key| key.matches_producer(self)) {
            reject_owners(Some(&receiver), owner, None);
            return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        }
        if self.meter().is_quarantined() {
            return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        }
        // Do not occupy the role needed to publish/cancel an existing grant.
        if queue.read().is_active() { return Err(StockWaitError::Pressure); }
        let result = waiters.run(Role::Producer, async {
            loop {
                let notified = queue.not_full().notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                queue.open(&receiver, &owner).await?;
                if self.try_retire_idle_output()? { return Ok(()); }
                notified.await;
            }
        }).await;
        match result {
            Ok(result) => result,
            Err(RunError::Closed) => Err(StockWaitError::Closed),
            Err(RunError::Quarantined) => Err(StockWaitError::Admission(MeteredCopyError::Quarantined)),
        }
    }

    pub(crate) async fn prepare_read(
        &mut self,
        max: usize,
        ready: &mut Option<StockReadOperation>,
    ) -> Result<(), StockWaitError> {
        let owner = self.rejection_scope();
        let Some(queue) = owner::Owner::from_return(&self.queue_return()) else {
            let actual = ready.as_ref().map(|read| read.claim.quiet.clone());
            reject_owners(None, owner, actual);
            return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        };
        let Some(waiters) = queue.waiters() else {
            let actual = ready.as_ref().map(|read| read.claim.quiet.clone());
            reject_owners(None, owner, actual);
            return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        };
        let receiver = waiters.quiet_scope();
        let belongs = queue.stock()
            .is_some_and(|key| key.matches_producer(self))
            && ready.as_ref().is_none_or(|read| {
                read.claim.queue.same(&queue)
                    && read.key.matches_producer(self)
                    && read.claim.meter.same_authority(self.meter())
                    && read.claim.id != 0
                    && read.claim.id <= queue.read().next.load(Ordering::Acquire)
                    && (!read.claim.armed
                        || (queue.read().active.load(Ordering::Acquire) == read.claim.id
                            && queue.read().reserved.load(Ordering::Acquire) == read.claim.max))
            });
        if !belongs {
            let actual = ready.as_ref().map(|read| read.claim.quiet.clone());
            reject_owners(Some(&receiver), owner, actual);
            return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        }
        if self.meter().is_quarantined() {
            return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
        }
        // Holding Producer while waiting for a predecessor's publish would
        // deadlock. The caller retains that exact grant and must settle it.
        if ready.is_some() || queue.read().is_active() {
            return Err(StockWaitError::Pressure);
        }
        if max == 0 {
            return Err(StockWaitError::Admission(MeteredCopyError::TooLarge));
        }
        queue.begin();
        let result = waiters
            .run(Role::Producer, async {
                loop {
                    let notified = queue.not_full().notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    let Some(mut claim) = queue.prepare_claim(max, self.meter(), &receiver, &owner).await? else {
                        notified.await;
                        continue;
                    };
                    let available = claim.max;
                    match self
                        .reserve_next(available)
                        .map_err(StockWaitError::Admission)?
                    {
                        Some(space) => {
                            let max = space.capacity();
                            if max == 0 || max > claim.max {
                                owner.mark_quarantined();
                                return Err(StockWaitError::Admission(
                                    MeteredCopyError::Quarantined,
                                ));
                            }
                            if !queue.resize(claim.id, claim.max, max) {
                                owner.mark_quarantined();
                                return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
                            }
                            claim.max = max;
                            claim.notify_producer = true;
                            *ready = Some(StockReadOperation {
                                space: Some(space),
                                claim,
                                key: self.stock_key(),
                            });
                            return Ok(());
                        }
                        None => {
                            claim.release();
                            drop(claim);
                            notified.await;
                        }
                    }
                }
            })
            .await;
        // Original ready output stays in the caller's slot even when actual
        // Work/notification retirement discovers Q after producing it.
        match result {
            Ok(result) => result,
            Err(RunError::Closed) => Err(StockWaitError::Closed),
            Err(RunError::Quarantined) => {
                Err(StockWaitError::Admission(MeteredCopyError::Quarantined))
            }
        }
    }
}

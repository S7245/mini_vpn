//! Complete Native reader actor: original read/assembler/pending/error accounting
//! and explicit disposal stay together. FlowRoles owns start/stop/JOIN and final
//! receipts; this module owns no F, epoch, queue or refund authority.
//! Entry: Request/run/Outcome. Tests retain the original native_inventory leaf.
use super::super::super::continuous::Shared;
use super::*;
use crate::client_tun::{
    NATIVE_TCP_REASSEMBLY_MAX_BYTES, NativeTcpChunkAssembler, RelayCloseReason,
    native_tcp_read_service_limit, note_reader_local_finish_if_needed, send_relay_reader_progress,
};
use crate::tcp_downlink_pump::{OwnedPayload, StockProducer, StockReadOperation, StockWaitError};
use crate::upstream::NativeTcpReadHalf;
use std::future::poll_fn;
use tokio::sync::watch;
use crate::shared::actor_callbacks::{SourceCallbacks, Lease, Guard, Custody, Field};
use std::{ops::{Deref, DerefMut}, panic::{AssertUnwindSafe, catch_unwind}};

/// Original Roles owns this cold stock. A Source lease is issued once; its
/// actual guard/proxy retirement, rather than JOIN, permits physical disposal.
pub(super) struct Cold {
    lease: Option<Lease>,
    producers: Option<Guard>,
    callbacks: Option<SourceCallbacks>,
}
impl Cold {
    pub(super) fn prepare(meter: &bytes::BackingMeter, clock: FailureClock) -> Result<Self, bytes::MeteredCopyError> {
        let callbacks = SourceCallbacks::prepare_source(meter, clock)?;
        let lease = callbacks.acquire_source().map_err(|_| {
            meter.quarantine(); bytes::MeteredCopyError::Quarantined
        })?;
        let producers = Some(lease.guard());
        Ok(Self { lease: Some(lease), producers, callbacks: Some(callbacks) })
    }
    pub(super) fn producer_guard(&self) -> Option<Guard> { self.producers.clone() }
    pub(super) fn take_lease(&mut self) -> Option<Lease> { self.lease.take() }
    pub(super) fn unstarted(&self) -> bool { self.lease.is_some() && self.callbacks.is_some() }
    pub(super) fn retire(&mut self) -> bool {
        // Caller has the actual producer-stop and Source DATA-disposal facts.
        if self.lease.is_none() { drop(self.producers.take()); }
        if self.lease.is_some() || self.callbacks.as_ref().is_some_and(|c| !c.quiescent()) {
            #[cfg(test)]
            eprintln!("[N1-Source-retire] lease={} physical={:?}", self.lease.is_some(), self.callbacks.as_ref().and_then(SourceCallbacks::source_inventory_for_test));
            return false;
        }
        drop(self.callbacks.take());
        true
    }
}
/// Static Legacy/Native representation, not another callback authority. Native
/// delegates every actual field destructor to the already signed Custody.
pub(super) struct Opaque<T>(OpaqueInner<T>);
enum OpaqueInner<T> { Legacy(T), Native(Custody<T>) }
impl<T> From<T> for Opaque<T> { fn from(value: T) -> Self { Self(OpaqueInner::Legacy(value)) } }
impl<T> Deref for Opaque<T> {
    type Target = T;
    fn deref(&self) -> &T { match &self.0 { OpaqueInner::Legacy(v) => v, OpaqueInner::Native(v) => v } }
}
impl<T> DerefMut for Opaque<T> {
    fn deref_mut(&mut self) -> &mut T { match &mut self.0 { OpaqueInner::Legacy(v) => v, OpaqueInner::Native(v) => v } }
}
impl<T> Opaque<Option<T>> {
    fn protect(&mut self, guard: &Guard, field: Field) {
        let old = std::mem::replace(&mut self.0, OpaqueInner::Legacy(None));
        self.0 = match old {
            OpaqueInner::Legacy(value) => OpaqueInner::Native(guard.own(value, field)),
            native => { guard.quarantine(); native }
        };
    }
    fn retire(&mut self) { drop(std::mem::replace(self, Self::from(None))); }
}
impl std::future::Future for Opaque<Option<oneshot::Receiver<()>>> {
    type Output = ();
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.as_mut().get_mut().deref_mut().as_mut() {
            Some(receiver) => std::pin::Pin::new(receiver).poll(cx).map(|_| ()),
            None => Poll::Ready(()),
        }
    }
}


pub(super) struct Request {
    pub(super) producer: Option<StockProducer>,
    pub(super) backing_meter: Option<bytes::BackingMeter>,
    pub(super) handle: SocketHandle,
    pub(super) epoch: u64,
    pub(super) reader: Opaque<Option<NativeTcpReadHalf>>,
    pub(super) shared: Arc<Shared>,
    pub(super) credit: Opaque<Option<watch::Receiver<RelayReadCredit>>>,
    pub(super) signals: Opaque<Option<mpsc::Sender<RelayReaderSignal>>>,
    pub(super) local_finish: Arc<std::sync::atomic::AtomicBool>,
    pub(super) stop: Opaque<Option<oneshot::Receiver<()>>>,
    pub(super) service_floor: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DiscardReceipt {
    pub(super) received_bytes: usize,
    pub(super) queued_bytes: usize,
    pub(super) unpublished_bytes: usize,
    pub(super) assembler_backing_bytes: usize,
    pub(super) rejected_bytes: usize,
    pub(super) pending_bytes: usize,
    pub(super) discarded_bytes: usize,
    pub(super) pending_capacity: usize,
}

pub(super) struct Outcome {
    state: State,
    pub(super) started: bool,
    pub(super) panics: [Option<Box<dyn std::any::Any + Send>>; 3],
    pub(super) receipt: Option<DiscardReceipt>,
    source_read_bytes: Option<usize>,
}

struct State {
    read_operation: Option<StockReadOperation>,
    request: Request,
    assembler: NativeTcpChunkAssembler,
    pending: Option<OwnedPayload>,
    rejected: Option<crate::upstream::NativeTcpChunk>,
    received_bytes: usize,
    queued_messages: usize,
    queued_bytes: usize,
    diag: RelayTaskDiag,
    failure: Opaque<Option<std::io::Error>>,
    guard: Option<Guard>,
    error_order: Option<usize>,
    closed_sent: bool,
    in_flight: bool,
    failed: bool,
}

impl Drop for State {
    fn drop(&mut self) {
        if let Some(guard) = &self.guard {
            if self.failed || self.in_flight || guard.failed()
                || self.assembler.buffered_bytes() != 0 || self.pending.is_some() || self.rejected.is_some() {
                guard.quarantine();
                std::mem::forget(std::mem::take(&mut self.assembler));
                std::mem::forget(self.pending.take());
                std::mem::forget(self.rejected.take());
                std::mem::forget(self.read_operation.take());
                std::mem::forget(self.request.producer.take());
            }
        }
    }
}

impl State {
    fn fail_unknown(&mut self) {
        self.failed = true;
        self.request.shared.quarantine();
        if let Some(guard) = &self.guard { guard.quarantine(); }
    }
    fn note_read(&mut self, bytes: usize) -> bool {
        match self.received_bytes.checked_add(bytes) {
            Some(total) => {
                self.received_bytes = total;
                self.diag.note_remote_raw_read(bytes);
                true
            }
            None => {
                self.fail_unknown();
                false
            }
        }
    }

    async fn publish_pending(&mut self, failures: &Option<FailureClock>) -> bool {
        let Some(pending) = self.pending.as_ref() else {
            self.fail_unknown();
            return false;
        };
        let bytes = pending.len();
        let Some(total) = self.queued_bytes.checked_add(bytes) else {
            self.fail_unknown();
            return false;
        };
        let Some(messages) = self.queued_messages.checked_add(1) else {
            self.fail_unknown();
            return false;
        };
        self.in_flight = true;
        let pushed = tokio::select! {
            biased;
            _ = &mut self.request.stop => None,
            result = async {
                match &mut self.read_operation {
                    Some(operation) => operation.publish(&mut self.pending).await,
                    None => self.request.shared.queue.push_custodied(&mut self.pending).await,
                }
            } => Some(result),
        };
        self.in_flight = false;
        match pushed {
            Some(Ok(())) => {}
            Some(Err(crate::tcp_downlink_pump::ByteCustodyError::Capacity(error))) => {
                #[cfg(test)]
                eprintln!(
                    "[N1-A1-capacity] site=queue_publish handle={:?} epoch={} error={error:?}",
                    self.request.handle, self.request.epoch
                );
                self.reject_capacity(error, failures).await;
                return false;
            }
            Some(Err(crate::tcp_downlink_pump::ByteCustodyError::Invalid)) => {
                self.fail_unknown();
                return false;
            }
            Some(Err(crate::tcp_downlink_pump::ByteCustodyError::Rejected)) => {
                self.reject_capacity(bytes::MeteredCopyError::BudgetFull, failures)
                    .await;
                return false;
            }
            None => return false,
        }
        drop(self.read_operation.take());
        self.queued_messages = messages;
        self.queued_bytes = total;
        self.diag.note_remote_read(bytes);
        self.request.signals.as_ref().is_some_and(|signals| send_relay_reader_progress(signals, &self.diag))
    }

    async fn close(&mut self, reason: Option<RelayCloseReason>) {
        // EOF/known IO refusal cancels the unused read grant before FIFO EOF.
        // A cancelled publication or unknown transaction retains its original.
        if self.pending.is_none() && !self.failed && !self.in_flight {
            drop(self.read_operation.take());
        }
        if let Some((direction, reason)) = reason {
            let signal = RelayReaderSignal::Closed {
                direction,
                reason,
                diag: self.diag.clone(),
            };
            self.closed_sent = self.request.shared.finish(signal);
            if !self.closed_sent {
                self.fail_unknown();
            }
        }
        self.request.shared.queue.close().await;
    }

    async fn reject_capacity(
        &mut self,
        error: bytes::MeteredCopyError,
        failures: &Option<FailureClock>,
    ) {
        if error == bytes::MeteredCopyError::Quarantined {
            self.fail_unknown();
        }
        *self.failure = Some(crate::client_tun::native_chunk_assembler::capacity_error(error));
        self.error_order = failures
            .as_ref()
            .zip(self.failure.as_ref())
            .map(|(clock, error)| clock.io(error));
        self.close(Some(("remote_to_local", "remote_read_failed")))
            .await;
    }

    fn drain_ready(&mut self, limit: usize) -> Result<Option<OwnedPayload>, bytes::MeteredCopyError> {
        match &mut self.read_operation {
            Some(operation) => self.assembler.drain_admitted(operation.space(), limit),
            None => self.assembler.drain_owned_up_to(limit),
        }
    }

    async fn io(&mut self, failures: &Option<FailureClock>) {
        let Some(receiver) = self.request.credit.as_ref() else { self.fail_unknown(); return; };
        let mut credit = *receiver.borrow();
        self.diag.note_read_credit(credit);
        loop {
            note_reader_local_finish_if_needed(&self.request.local_finish, &mut self.diag);
            let Some(receiver) = self.request.credit.as_mut() else { self.fail_unknown(); return; };
            crate::client_tun::drain_relay_read_credit_updates(receiver, &mut credit, &mut self.diag);
            let limit = native_tcp_read_service_limit(credit, self.request.service_floor);
            if limit == 0 {
                let changed = tokio::select! {
                    biased;
                    _ = &mut self.request.stop => None,
                    changed = receiver.changed() => Some(changed),
                };
                if changed.is_none_or(|changed| changed.is_err()) {
                    self.close(None).await;
                    return;
                }
                credit = *receiver.borrow_and_update();
                self.diag.note_read_credit(credit);
                continue;
            }
            let read_len = if let Some(producer) = &mut self.request.producer {
                if self.read_operation.is_none() {
                    let prepared = tokio::select! {
                        biased;
                        _ = &mut self.request.stop => None,
                        value = producer.prepare_read(limit, &mut self.read_operation) => Some(value),
                    };
                    match prepared {
                        Some(Ok(())) => {},
                        Some(Err(StockWaitError::Admission(error))) => { self.reject_capacity(error, failures).await; return; },
                        // The one Source is the exclusive producer and owns its
                        // only grant in read_operation. No competing grant exists.
                        Some(Err(StockWaitError::Pressure)) => { self.fail_unknown(); self.close(None).await; return; },
                        Some(Err(StockWaitError::Closed)) | None => { self.close(None).await; return; },
                    }
                }
                let Some(operation) = &self.read_operation else { self.fail_unknown(); return; };
                operation.capacity().min(limit)
            } else {
                let read_len = tokio::select! {
                    biased;
                    _ = &mut self.request.stop => None,
                    result = self.request.shared.queue.wait_read_len(limit) => Some(result),
                };
                let Some(Ok(read_len)) = read_len else { self.close(None).await; return; };
                read_len
            };
            if read_len == 0 {
                continue;
            }
            if let Some(payload) = match self.drain_ready(read_len) {
                Ok(value) => value,
                Err(error) => {
                    #[cfg(test)]
                    eprintln!(
                        "[N1-A1-capacity] site=assembler_before_read handle={:?} epoch={} error={error:?}",
                        self.request.handle, self.request.epoch
                    );
                    self.reject_capacity(error, failures).await;
                    return;
                }
            } {
                self.pending = Some(payload);
                if !self.publish_pending(failures).await {
                    self.close(None).await;
                    return;
                }
                continue;
            }
            let remaining =
                NATIVE_TCP_REASSEMBLY_MAX_BYTES.saturating_sub(self.assembler.buffered_bytes());
            if remaining == 0 {
                self.assembler.note_cap_hit();
                self.assembler
                    .maybe_log_gap(self.request.handle, self.request.epoch, "root_cap");
                self.fail_unknown();
                self.close(Some(("remote_to_local", "native_reassembly_cap")))
                    .await;
                return;
            }
            let max_len = read_len.min(remaining);
            if self.request.producer.is_some() {
                loop {
                    match self.assembler.prepare_next_input() {
                        Ok(()) => break,
                        Err(bytes::MeteredCopyError::BudgetFull) => {
                            // drain_ready returned None: no output was written
                            // or published. Prove this before cancelling the
                            // old grant and acquiring the same Producer role.
                            if self.pending.is_some() || self.rejected.is_some()
                                || self.read_operation.as_ref().is_some_and(|op| op.validate_unused().is_err()) {
                                self.fail_unknown(); self.close(None).await; return;
                            }
                            drop(self.read_operation.take());
                            let Some(producer) = self.request.producer.as_mut() else {
                                self.fail_unknown(); self.close(None).await; return;
                            };
                            let retired = tokio::select! {
                                biased;
                                _ = &mut self.request.stop => None,
                                result = producer.retire_idle_output() => Some(result),
                            };
                            match retired {
                                Some(Ok(())) => {},
                                Some(Err(StockWaitError::Admission(error))) => {
                                    self.reject_capacity(error, failures).await; return;
                                },
                                Some(Err(StockWaitError::Pressure)) => {
                                    self.fail_unknown(); self.close(None).await; return;
                                },
                                Some(Err(StockWaitError::Closed)) | None => { self.close(None).await; return; },
                            }
                        },
                        Err(error) => { self.reject_capacity(error, failures).await; return; },
                    }
                }
                // Metadata is now owned, before reacquiring any output backing.
                if self.read_operation.is_none() { continue; }
            }
            self.diag.note_remote_read_service_tick(max_len);
            let Some(reader) = self.request.reader.as_mut() else {
                self.fail_unknown();
                self.close(None).await;
                return;
            };
            let chunk = tokio::select! {
                biased;
                _ = &mut self.request.stop => None,
                result = async {
                    match &mut self.request.producer {
                        Some(producer) => producer.read_or_trim(&mut self.read_operation,
                            |cx, capacity| reader.poll_read_chunk(cx, max_len.min(capacity))).await,
                        None => Ok(Some(poll_fn(|cx| reader.poll_read_chunk(cx, max_len)).await)),
                    }
                } => Some(result),
            };
            let Some(chunk) = chunk else {
                self.close(None).await;
                return;
            };
            let chunk = match chunk {
                Ok(Some(chunk)) => chunk,
                Ok(None) => continue, // Rebound grant: recompute limit before the next real read.
                Err((error, completed)) => {
                    if let Some(completed) = completed {
                        match completed {
                            Ok(Some(chunk)) => self.rejected = Some(chunk),
                            Err(error) => {
                                // This is the original completed IO, not an
                                // error manufactured from the final Q verdict.
                                self.error_order = failures.as_ref().map(|clock| clock.io(&error));
                                *self.failure = Some(error);
                            },
                            Ok(None) => {},
                        }
                        self.fail_unknown(); self.close(None).await; return;
                    }
                    match error {
                        StockWaitError::Admission(bytes::MeteredCopyError::Quarantined) | StockWaitError::Pressure => {
                            self.fail_unknown(); self.close(None).await; return;
                        },
                        StockWaitError::Admission(error) => { self.reject_capacity(error, failures).await; return; },
                        StockWaitError::Closed => { self.close(None).await; return; },
                    }
                },
            };
            match chunk {
                Ok(Some(chunk)) => {
                    if chunk.bytes.len() > max_len || !self.note_read(chunk.bytes.len()) {
                        self.rejected = Some(chunk);
                        self.fail_unknown();
                        self.close(Some(("root", "native_read_capacity_failed")))
                            .await;
                        return;
                    }
                    if let Err((error, chunk)) = self.assembler.push_chunk(chunk) {
                        self.rejected = Some(chunk);
                        #[cfg(test)]
                        eprintln!(
                            "[N1-A1-capacity] site=assembler_push handle={:?} epoch={} error={error:?}",
                            self.request.handle, self.request.epoch
                        );
                        self.reject_capacity(error, failures).await;
                        return;
                    }
                    if let Some(payload) = match self.drain_ready(read_len) {
                        Ok(value) => value,
                        Err(error) => {
                            #[cfg(test)]
                            eprintln!(
                                "[N1-A1-capacity] site=assembler_after_read handle={:?} epoch={} error={error:?}",
                                self.request.handle, self.request.epoch
                            );
                            self.reject_capacity(error, failures).await;
                            return;
                        }
                    } {
                        self.pending = Some(payload);
                        if !self.publish_pending(failures).await {
                            self.close(None).await;
                            return;
                        }
                    } else {
                        self.assembler.maybe_log_gap(
                            self.request.handle,
                            self.request.epoch,
                            "root_gap",
                        );
                    }
                }
                Ok(None) => {
                    if let Some(payload) = match self.drain_ready(read_len) {
                        Ok(value) => value,
                        Err(error) => {
                            #[cfg(test)]
                            eprintln!(
                                "[N1-A1-capacity] site=assembler_eof handle={:?} epoch={} error={error:?}",
                                self.request.handle, self.request.epoch
                            );
                            self.reject_capacity(error, failures).await;
                            return;
                        }
                    } {
                        self.pending = Some(payload);
                        if !self.publish_pending(failures).await {
                            self.close(None).await;
                            return;
                        }
                    }
                    self.close(Some(("remote_to_local", "remote_eof"))).await;
                    return;
                }
                Err(error) => {
                    *self.failure = Some(error);
                    self.error_order = failures
                        .as_ref()
                        .zip(self.failure.as_ref())
                        .map(|(clock, error)| clock.io(error));
                    self.close(Some(("remote_to_local", "remote_read_failed")))
                        .await;
                    return;
                }
            }
        }
    }
}

impl Outcome {
    pub(super) fn release_ready(&self) -> bool {
        let s = &self.state;
        self.receipt.is_some() && !self.quarantined() && self.panics.iter().all(Option::is_none)
            && s.read_operation.is_none() && s.pending.is_none() && s.rejected.is_none()
            && s.assembler.buffered_bytes() == 0 && s.request.producer.is_none() && s.guard.is_none()
            && matches!(s.request.reader.0, OpaqueInner::Legacy(None))
            && matches!(s.request.credit.0, OpaqueInner::Legacy(None))
            && matches!(s.request.signals.0, OpaqueInner::Legacy(None))
            && matches!(s.request.stop.0, OpaqueInner::Legacy(None))
            && matches!(s.failure.0, OpaqueInner::Legacy(None))
    }

    pub(super) fn has_error(&self) -> bool {
        self.state.failure.is_some()
    }
    pub(super) fn error_order(&self) -> Option<usize> {
        self.state.error_order
    }
    pub(super) fn take_error(&mut self) -> Option<std::io::Error> {
        self.state.failure.take() // Empty field/guard retirement belongs to the lock-free turn.
    }
    pub(super) fn closed_sent(&self) -> bool {
        self.state.closed_sent
    }
    pub(super) fn received_bytes(&self) -> usize {
        self.state.received_bytes
    }
    pub(super) fn source_read_bytes(&self) -> Option<usize> {
        self.source_read_bytes
    }
    pub(super) fn queued(&self) -> PublishedReceipt {
        PublishedReceipt {
            messages: self.state.queued_messages,
            bytes: self.state.queued_bytes,
        }
    }
    pub(super) fn quarantined(&self) -> bool {
        self.state.failed
            || self.state.in_flight
            || self.panics.iter().any(Option::is_some)
            || self
                .state
                .request
                .backing_meter
                .as_ref()
                .is_some_and(bytes::BackingMeter::is_quarantined)
    }
    fn query_source(&mut self) {
        let queried = catch_unwind(AssertUnwindSafe(|| self.state.request.reader.as_ref().and_then(|reader| reader.source_read_bytes())));
        match queried {
            Ok(bytes) => self.source_read_bytes = bytes,
            Err(payload) => {
                if let Some(guard) = &self.state.guard { guard.capture(Field::Source6, payload); }
                else { self.panics[2] = Some(payload); }
                self.state.fail_unknown(); return;
            }
        }
    }
    pub(super) fn discard_local(
        &mut self,
        meter: Option<&bytes::BackingMeter>,
    ) -> Option<DiscardReceipt> {
        if self.quarantined() {
            return None;
        }
        if let Some(receipt) = self.receipt {
            if self.state.failure.is_none() { self.state.failure.retire(); drop(self.state.guard.take()); }
            return Some(receipt);
        }
        let assembler_backing_bytes = self.state.assembler.buffered_bytes();
        let valid_assembler = match (meter, self.state.request.backing_meter.as_ref()) {
            (Some(expected), Some(original)) => {
                expected.same_authority(original)
                    && self.state.assembler.metered_buffered_bytes(expected)
                        == Some(assembler_backing_bytes)
            }
            (None, None) => {
                self.state.assembler.pristine_unmetered()
                    && self.state.pending.is_none()
                    && self.state.rejected.is_none()
                    && self.state.received_bytes == 0
                    && self.state.queued_bytes == 0
            }
            _ => false,
        };
        if !valid_assembler {
            self.state.fail_unknown();
            return None;
        }
        if let Some(payload) = &self.state.pending {
            let valid = match payload {
                OwnedPayload::Native(bytes) => {
                    bytes.allocation_identity().is_some()
                        && meter.is_some_and(|m| {
                            bytes
                                .backing_meter()
                                .as_ref()
                                .is_some_and(|actual| m.same_authority(actual))
                        })
                }
                OwnedPayload::Legacy(_) => false,
            };
            if !valid {
                self.state.fail_unknown();
                return None;
            }
        }
        let pending_bytes = self.state.pending.as_ref().map_or(0, OwnedPayload::len);
        let rejected_bytes = self.state.rejected.as_ref().map_or(0, |c| c.bytes.len());
        if let Some(chunk) = &self.state.rejected {
            if chunk.bytes.allocation_identity().is_none()
                || !meter.is_some_and(|m| {
                    chunk
                        .bytes
                        .backing_meter()
                        .as_ref()
                        .is_some_and(|actual| m.same_authority(actual))
                })
            {
                self.state.fail_unknown();
                return None;
            }
        }
        let unpublished = assembler_backing_bytes
            .checked_add(pending_bytes)?
            .checked_add(rejected_bytes)?;
        let Some(discarded_bytes) = self.state.assembler.discarded_bytes() else {
            self.state.fail_unknown();
            return None;
        };
        if self
            .state
            .queued_bytes
            .checked_add(unpublished)
            .and_then(|total| total.checked_add(discarded_bytes))
            != Some(self.state.received_bytes)
        {
            self.state.fail_unknown();
            return None;
        }
        let receipt = DiscardReceipt {
            received_bytes: self.state.received_bytes,
            queued_bytes: self.state.queued_bytes,
            unpublished_bytes: unpublished,
            assembler_backing_bytes,
            rejected_bytes,
            pending_bytes,
            discarded_bytes,
            pending_capacity: self
                .state
                .pending
                .as_ref()
                .map_or(0, OwnedPayload::capacity),
        };
        let panic_owner = self.state.request.shared.clone();
        self.panics[2] = panic_owner
            .guard(|| {
                drop(std::mem::take(&mut self.state.assembler));
                drop(self.state.pending.take());
                drop(self.state.rejected.take());
                drop(self.state.read_operation.take());
                drop(self.state.request.producer.take());
                self.state.request.reader.retire();
            })
            .err();
        if self.panics[2].is_some() {
            self.state.fail_unknown();
            return None;
        }
        self.state.request.credit.retire();
        self.state.request.signals.retire();
        self.state.request.stop.retire();
        if self.state.guard.as_ref().is_some_and(Guard::failed) {
            self.state.fail_unknown(); return None;
        }
        if self.state.failure.is_none() {
            self.state.failure.retire();
            drop(self.state.guard.take());
        }
        self.receipt = Some(receipt);
        Some(receipt)
    }
}

pub(super) fn run(
    mut request: Request,
    gate: oneshot::Receiver<()>,
    failures: Option<FailureClock>,
    mut lease: Option<Lease>,
) -> impl std::future::Future<Output = Outcome> {
    let guard = lease.as_ref().map(Lease::guard);
    let mut gate = Opaque::from(Some(gate));
    let mut failure = Opaque::from(None);
    if let Some(guard) = &guard {
        request.reader.protect(guard, Field::Source0);
        request.credit.protect(guard, Field::Source1);
        request.signals.protect(guard, Field::Source2);
        request.stop.protect(guard, Field::Source3);
        gate.protect(guard, Field::Source4);
        failure.protect(guard, Field::Source5);
    }
    let assembler = NativeTcpChunkAssembler::with_meter(request.backing_meter.clone());
    let mut state = State {
        request, assembler, pending: None, rejected: None, read_operation: None,
        received_bytes: 0, queued_messages: 0, queued_bytes: 0,
        diag: RelayTaskDiag::default(), failure, guard,
        error_order: None, closed_sent: false, in_flight: false, failed: false,
    };
    async move {
        let mut started = false;
        let mut panics = std::array::from_fn(|_| None);
        let panic_owner = state.request.shared.clone();
        if let Some(lease) = &mut lease {
            let mut body = Some(async {
                started = match gate.as_mut() { Some(gate) => gate.await.is_ok(), None => false };
                gate.retire();
                if started { state.io(&failures).await; } else { state.close(None).await; }
            });
            let complete = match lease.run(&mut body) {
                Ok(work) => work.await.is_ok(),
                Err(_) => false,
            };
            drop(body); // Q refusal preserves the original borrowed future.
            if !complete { state.fail_unknown(); }
        } else {
            started = match gate.as_mut() { Some(gate) => gate.await.is_ok(), None => false };
            gate.retire();
            if started {
                let mut work = Box::pin(state.io(&failures));
                panics[0] = poll_fn(|cx| match panic_owner.guard(|| work.as_mut().poll(cx)) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(())) => Poll::Ready(None),
                    Err(payload) => Poll::Ready(Some(payload)),
                }).await;
                panics[1] = panic_owner.guard(|| drop(work)).err();
            } else { state.close(None).await; }
        }
        drop(lease);
        let mut outcome = Outcome { state, started, panics, receipt: None, source_read_bytes: None };
        if !outcome.quarantined() { outcome.query_source(); }
        outcome
    }
}

#[cfg(test)]
#[path = "../tests/native_inventory.rs"]
mod tests;

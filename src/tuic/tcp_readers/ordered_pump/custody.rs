//! One ordered pump transaction: cold source, producer, returned reader and
//! original task outcome. The source batch remains in its source object. DATA
//! crosses only the original 64-slot channel. Disposition is bounded and occurs
//! only after actual join and return of the original reader.
use super::*;
use crate::upstream::{
    NativePumpControl, NativePumpFailure, NativePumpFailureValue, NativePumpFinished,
    NativePumpReceipt, NativePumpWork, PreparedNativePump, RunningNativePump,
};
use std::any::Any;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::Waker;
use tokio::sync::Notify;

pub(super) enum Item {
    Data(NativeTcpChunk),
    Eof,
    Error,
}
impl Item {
    fn bytes(&self) -> usize {
        match self {
            Self::Data(c) => c.bytes.len(),
            _ => 0,
        }
    }
}
pub(super) struct Returned {
    pub(super) rx: cold::Receiver,
    pub(super) pending: Option<NativeTcpChunk>,
    pub(super) received_items: usize,
    pub(super) received_bytes: usize,
    pub(super) forwarded_bytes: usize,
}
#[derive(Default)]
struct ReturnSlot {
    value: Option<Returned>,
    claimed: bool,
    waiter: Option<Waker>,
}
// One IO and at most one panic at each non-reentrant callback boundary:
// source poll / future drop / sender drop / reader poll / timer drop /
// returned-observer wake / stop notification / terminal disposition / retained IO drop.
#[derive(Default)]
struct Faults {
    io: Option<(usize, NativePumpFailure)>,
    panics: [Option<(usize, NativePumpFailure)>; 9],
    next: usize,
}
pub(super) struct Shared {
    meter: Option<bytes::BackingMeter>,
    stopped: AtomicBool,
    stop: Notify,
    quarantined: AtomicBool,
    returned: Mutex<ReturnSlot>,
    faults: Mutex<Faults>,
    clock: std::sync::OnceLock<crate::shared::errors::FailureClock>,
    error_order: AtomicUsize,
    #[cfg(test)]
    source_read_bytes: AtomicUsize,
}
impl Shared {
    fn new(meter: Option<bytes::BackingMeter>) -> Arc<Self> {
        Arc::new(Self {
            meter,
            stopped: AtomicBool::new(false),
            stop: Notify::new(),
            quarantined: AtomicBool::new(false),
            returned: Mutex::new(ReturnSlot::default()),
            faults: Mutex::new(Faults::default()),
            clock: std::sync::OnceLock::new(),
            error_order: AtomicUsize::new(usize::MAX),
            #[cfg(test)]
            source_read_bytes: AtomicUsize::new(0),
        })
    }
    fn lock<'a, T>(&self, mutex: &'a Mutex<T>) -> std::sync::MutexGuard<'a, T> {
        match mutex.lock() {
            Ok(guard) => guard,
            Err(poison) => {
                self.bad();
                poison.into_inner()
            }
        }
    }
    pub(super) fn bad(&self) {
        self.quarantined.store(true, Ordering::Release);
        if let Some(meter) = &self.meter {
            meter.quarantine();
        }
    }
    pub(super) fn request_stop(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| self.stop.notify_waiters())) {
            self.unwind(payload);
        }
    }
    async fn stopping(&self) {
        loop {
            let notified = self.stop.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.stopped.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
    fn publish(&self, failure: &mut NativePumpFailure)
        -> Option<crate::shared::errors::failure_clock::Publication<'_>> {
        let clock = self.clock.get()?;
        let publication = match failure.value_ref() {
            NativePumpFailureValue::Io(error) => clock.io_deferred(error),
            NativePumpFailureValue::Panic(_) => clock.panic_deferred(),
        };
        failure.order = Some(publication.order);
        let _ = self.error_order.compare_exchange(usize::MAX, publication.order, Ordering::AcqRel, Ordering::Acquire);
        Some(publication)
    }
    fn put_error(&self, error: io::Error) {
        let mut faults = self.lock(&self.faults);
        if faults.io.is_some() {
            self.bad();
            std::mem::forget(error);
            return;
        }
        let sequence = faults.next;
        faults.next = faults.next.saturating_add(1);
        faults.io = Some((sequence, NativePumpFailure::new(NativePumpFailureValue::Io(error), None)));
        let publication = faults.io.as_mut().and_then(|(_, failure)| self.publish(failure));
        drop(faults);
        drop(publication);
    }
    pub(super) fn reader_error(&self) -> io::Error {
        // A notification only. The original IO/Box stays available to its owner.
        io::Error::other("Native pump terminal failure retained by its owner")
    }
    pub(super) fn return_reader(&self, value: Returned) {
        let waiter = {
            let mut slot = self.lock(&self.returned);
            if slot.value.is_some() || slot.claimed {
                self.bad();
                return;
            }
            slot.value = Some(value);
            slot.waiter.take()
        };
        self.request_stop();
        if let Some(waiter) = waiter {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| waiter.wake())) {
                self.unwind(payload);
            }
        }
    }
    fn receive_return(&self, cx: &mut Context<'_>) -> Option<Returned> {
        let prepared = match catch_unwind(AssertUnwindSafe(|| cx.waker().clone())) {
            Ok(waker) => waker,
            Err(payload) => {
                self.unwind(payload);
                return None;
            }
        };
        let (value, retired) = {
            let mut slot = self.lock(&self.returned);
            if let Some(value) = slot.value.take() {
                slot.claimed = true;
                (Some(value), Some(prepared))
            } else if !slot.claimed {
                (None, slot.waiter.replace(prepared))
            } else {
                (None, Some(prepared))
            }
        };
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(retired))) {
            self.unwind(payload);
        }
        value
    }
}
impl NativePumpControl for Shared {
    #[cfg(test)]
    fn source_read_bytes(&self) -> usize {
        self.source_read_bytes.load(Ordering::Acquire)
    }
    fn stop(&self) {
        self.request_stop();
    }
    fn quarantined(&self) -> bool {
        self.quarantined.load(Ordering::Acquire)
            || self
                .meter
                .as_ref()
                .is_some_and(|meter| meter.is_quarantined())
    }
    fn quarantine(&self) {
        self.bad();
    }
    fn unwind(&self, payload: Box<dyn Any + Send>) {
        let mut faults = self.lock(&self.faults);
        let publication = if let Some(index) = faults.panics.iter().position(Option::is_none) {
            let sequence = faults.next;
            faults.next = faults.next.saturating_add(1);
            faults.panics[index] = Some((sequence, NativePumpFailure::new(NativePumpFailureValue::Panic(payload), None)));
            faults.panics[index].as_mut().and_then(|(_, failure)| self.publish(failure))
        } else {
            std::mem::forget(payload);
            None
        };
        drop(faults);
        self.bad();
        drop(publication);
    }
    fn take_failure(&self) -> Option<NativePumpFailure> {
        let mut faults = self.lock(&self.faults);
        let panic = faults
            .panics
            .iter()
            .enumerate()
            .filter_map(|(i, p)| p.as_ref().map(|p| (i, p.0)))
            .min_by_key(|p| p.1);
        match (faults.io.as_ref().map(|p| p.0), panic) {
            (Some(io), Some((index, panic))) if panic < io => {
                faults.panics[index].take().map(|p| p.1)
            }
            (Some(_), _) => faults.io.take().map(|p| p.1),
            (None, Some((index, _))) => faults.panics[index].take().map(|p| p.1),
            _ => None,
        }
    }
    fn error_order(&self) -> Option<usize> {
        let order = self.error_order.load(Ordering::Acquire);
        (order != usize::MAX).then_some(order)
    }
}

struct State<S> {
    source: Option<S>,
    lease: Option<TcpPoolSlotLease>,
    tx: Option<cold::Sender>,
    shared: Arc<Shared>,
    pending: Option<Item>,
    sent_items: usize,
    sent_bytes: usize,
    read_bytes: usize,
    in_flight: bool,
}
impl<S: TuicOrderedNativeChunkSource> State<S> {
    async fn run(&mut self) {
        loop {
            let Some(source) = self.source.as_mut() else {
                self.shared.bad();
                return;
            };
            let result = tokio::select! {
                biased;
                _ = self.shared.stopping() => return,
                result = source.read_ordered_native_chunk(TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES) => result,
            };
            self.pending = Some(match result {
                Ok(Some(chunk)) => Item::Data(chunk),
                Ok(None) => Item::Eof,
                Err(error) => {
                    self.shared.put_error(error);
                    Item::Error
                }
            });
            let Some(pending) = self.pending.as_ref() else {
                self.shared.bad();
                return;
            };
            let bytes = pending.bytes();
            let terminal = !matches!(pending, Item::Data(_));
            let Some(total) = self.read_bytes.checked_add(bytes) else {
                self.shared.bad();
                return;
            };
            self.read_bytes = total;
            #[cfg(test)]
            self.shared
                .source_read_bytes
                .store(total, Ordering::Release);
            let Some(tx) = self.tx.as_ref() else {
                self.shared.bad();
                return;
            };
            let permit = tokio::select! {
                biased;
                _ = self.shared.stopping() => return,
                result = tx.reserve() => match result {
                    Ok(p) => p,
                    Err(_) => {
                        if !self.shared.stopped.load(Ordering::Acquire) { self.shared.bad(); }
                        return;
                    }
                },
            };
            let Some(items) = self.sent_items.checked_add(1) else {
                self.shared.bad();
                return;
            };
            let Some(sent) = self.sent_bytes.checked_add(bytes) else {
                self.shared.bad();
                return;
            };
            let Some(value) = self.pending.take() else {
                self.shared.bad();
                return;
            };
            self.in_flight = true;
            match permit.send(value) {
                Ok(()) => {
                    self.sent_items = items;
                    self.sent_bytes = sent;
                }
                Err(value) => {
                    if !self.shared.stopped.load(Ordering::Acquire) {
                        self.shared.bad();
                    }
                    self.pending = Some(value);
                    self.in_flight = false;
                    return;
                }
            }
            self.in_flight = false;
            if terminal {
                return;
            }
        }
    }
}
struct Prepared<S> {
    state: State<S>,
}
impl<S: TuicOrderedNativeChunkSource> NativePumpWork for Prepared<S> {
    fn commit(
        mut self: Box<Self>,
        clock: Option<crate::shared::errors::FailureClock>,
        gate: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> RunningNativePump {
        if let Some(clock) = clock {
            let _ = self.state.shared.clock.set(clock);
        }
        let shared = self.state.shared.clone();
        let actor_shared = shared.clone();
        let task = tokio::spawn(async move {
            let mut state = self.state;
            let started = match gate {
                Some(gate) => gate.await.is_ok(),
                None => true,
            };
            if started {
                let mut work = Box::pin(state.run());
                let result =
                    poll_fn(
                        |cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
                            Ok(Poll::Pending) => Poll::Pending,
                            Ok(Poll::Ready(())) => Poll::Ready(None),
                            Err(payload) => Poll::Ready(Some(payload)),
                        },
                    )
                    .await;
                // Record the original failure before guarding borrowed-future destruction.
                if let Some(payload) = result {
                    state_error(&actor_shared, payload);
                }
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(work))) {
                    state_error(&actor_shared, payload);
                }
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(state.tx.take()))) {
                state_error(&actor_shared, payload);
            }
            std::mem::ManuallyDrop::new(Box::new(Outcome {
                state,
                returned: None,
                discarded_items: 0,
                discarded_bytes: 0,
                pending_bytes: None,
                done: None,
                disposing: false,
            }) as Box<dyn NativePumpFinished>)
        });
        RunningNativePump::new(task, shared)
    }
}
fn state_error(shared: &Shared, payload: Box<dyn Any + Send>) {
    NativePumpControl::unwind(shared, payload);
}
struct Outcome<S> {
    state: State<S>,
    returned: Option<Returned>,
    discarded_items: usize,
    discarded_bytes: usize,
    pending_bytes: Option<usize>,
    done: Option<NativePumpReceipt>,
    disposing: bool,
}
impl<S: TuicOrderedNativeChunkSource> NativePumpFinished for Outcome<S> {
    fn poll_dispose(&mut self, cx: &mut Context<'_>) -> Poll<NativePumpReceipt> {
        if let Some(done) = self.done {
            return Poll::Ready(done);
        }
        if self.state.shared.quarantined() || self.state.in_flight || self.disposing {
            self.state.shared.bad();
            return Poll::Pending;
        }
        if self.returned.is_none() {
            self.returned = self.state.shared.receive_return(cx);
        }
        let Some(returned) = self.returned.as_mut() else {
            return Poll::Pending;
        };
        self.disposing = true;
        drop(self.state.tx.take());
        if self.pending_bytes.is_none() {
            self.pending_bytes = Some(self.state.pending.as_ref().map_or(0, Item::bytes));
            drop(self.state.pending.take());
        }
        for _ in 0..32 {
            match returned.rx.poll_recv(cx) {
                Poll::Ready(Some(item)) => {
                    let Some(items) = self.discarded_items.checked_add(1) else {
                        self.state.shared.bad();
                        return Poll::Pending;
                    };
                    let Some(bytes) = self.discarded_bytes.checked_add(item.bytes()) else {
                        self.state.shared.bad();
                        return Poll::Pending;
                    };
                    self.discarded_items = items;
                    self.discarded_bytes = bytes;
                    drop(item);
                }
                Poll::Ready(None) => {
                    if self.state.shared.quarantined() {
                        return Poll::Pending;
                    }
                    let pending = returned.pending.as_ref().map_or(0, |c| c.bytes.len());
                    let producer = self.pending_bytes.unwrap_or(0);
                    if returned.received_items.checked_add(self.discarded_items)
                        != Some(self.state.sent_items)
                        || returned.received_bytes.checked_add(self.discarded_bytes)
                            != Some(self.state.sent_bytes)
                        || returned.forwarded_bytes.checked_add(pending)
                            != Some(returned.received_bytes)
                        || self.state.sent_bytes.checked_add(producer)
                            != Some(self.state.read_bytes)
                    {
                        self.state.shared.bad();
                        return Poll::Pending;
                    }
                    let receipt = NativePumpReceipt {
                        source_read_bytes: self.state.read_bytes,
                        sent_items: self.state.sent_items,
                        sent_bytes: self.state.sent_bytes,
                        received_items: returned.received_items,
                        received_bytes: returned.received_bytes,
                        forwarded_bytes: returned.forwarded_bytes,
                        queue_discarded_items: self.discarded_items,
                        queue_discarded_bytes: self.discarded_bytes,
                        reader_discarded_bytes: pending,
                        producer_discarded_bytes: producer,
                    };
                    drop(returned.pending.take());
                    drop(self.state.source.take());
                    drop(self.state.lease.take());
                    self.disposing = false;
                    self.done = Some(receipt);
                    return Poll::Ready(receipt);
                }
                Poll::Pending => {
                    self.disposing = false;
                    return Poll::Pending;
                }
            }
        }
        cx.waker().wake_by_ref();
        self.disposing = false;
        Poll::Pending
    }
}
pub(super) fn prepare<S: TuicOrderedNativeChunkSource>(
    source: S,
    lease: TcpPoolSlotLease,
    tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Option<Connection>,
    channel: (cold::Sender, cold::Receiver),
) -> (TuicNativeOrderedPumpReader, PreparedNativePump) {
    let backing_meter = source.receive_backing_meter();
    let (tx, rx) = channel;
    let shared = Shared::new(backing_meter.clone());
    if source.admission_quarantined() {
        shared.bad();
    }
    let reader = TuicNativeOrderedPumpReader {
        rx: Some(rx),
        backing_meter,
        poll_in_flight: false,
        shared: shared.clone(),
        received_items: 0,
        received_bytes: 0,
        forwarded_bytes: 0,
        tcp_diag,
        transport_conn,
        pending_self_wake: PendingReadWake::default(),
        pending_chunk: None,
        pending_eof: false,
        pending_error: None,
    };
    let work = Prepared {
        state: State {
            source: Some(source),
            lease: Some(lease),
            tx: Some(tx),
            shared,
            pending: None,
            sent_items: 0,
            sent_bytes: 0,
            read_bytes: 0,
            in_flight: false,
        },
    };
    (reader, PreparedNativePump::new(Box::new(work)))
}

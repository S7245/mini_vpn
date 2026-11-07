//! Continuous FIFO, held batch, Main credit and terminal custody. FlowRoles owns
//! the four original actors; this owner retains physical DATA through their joins
//! and guarded disposal. No F/provider/Node authority or legacy detached launcher.
//! Entry `run`; actual role and wake tests are privately routed from roles.rs.
use super::super::receive_window::ReaderWindow;
use super::super::relay_reader::PublishedReceipt;
use super::*;
use crate::tcp_downlink_pump::{ByteQueueBatch, ByteQueueCustody, DownstreamBytePermit};
mod queue;
pub(in crate::client_tun::compatibility_runtime::flow_custody) use queue::Backend;
pub(super) use queue::Held;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use tokio::sync::oneshot;

#[derive(Default)]
struct TerminalSlot {
    signal: Option<RelayReaderSignal>,
    emitted: bool,
    taken: bool,
    interrupted: bool,
}
pub(in crate::client_tun::compatibility_runtime::flow_custody) struct Shared {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) queue: Backend,
    terminal: Mutex<TerminalSlot>,
    failed: AtomicBool,
    meter: Option<bytes::BackingMeter>,
    failures: std::sync::OnceLock<roles::FailureClock>,
}
impl Shared {
    #[cfg(test)]
    pub(super) fn new(engine: TcpRelayEngine) -> Option<Arc<Self>> {
        Self::configured(engine, DownlinkBackpressureConfig::default())
    }
    pub(super) fn configured(
        engine: TcpRelayEngine,
        cfg: DownlinkBackpressureConfig,
    ) -> Option<Arc<Self>> {
        Self::configured_metered(engine, cfg, None)
    }
    pub(super) fn configured_metered(
        engine: TcpRelayEngine,
        cfg: DownlinkBackpressureConfig,
        meter: Option<bytes::BackingMeter>,
    ) -> Option<Arc<Self>> {
        Self::try_configured_metered(engine, cfg, meter).ok()
    }
    pub(super) fn try_configured_metered(
        engine: TcpRelayEngine, cfg: DownlinkBackpressureConfig, meter: Option<bytes::BackingMeter>,
    ) -> Result<Arc<Self>, bytes::MeteredCopyError> {
        Ok(Arc::new(Self {
            queue: Backend::try_new(engine, cfg, meter.clone())?,
            terminal: Mutex::new(TerminalSlot::default()),
            failed: AtomicBool::new(false),
            meter,
            failures: std::sync::OnceLock::new(),
        }))
    }
    pub(super) fn prepare_native(
        engine: TcpRelayEngine, cfg: DownlinkBackpressureConfig,
        meter: Option<bytes::BackingMeter>, clock: roles::FailureClock,
    ) -> Result<(Arc<Self>, Option<crate::tcp_downlink_pump::OutputStock>), bytes::MeteredCopyError> {
        if let Some(meter) = meter.as_ref().filter(|m| m.budget_limit().is_some()) {
            let (queue, stock) = match engine {
                TcpRelayEngine::ContinuousPump => {
                    let (queue, stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
                        CONTINUOUS_TCP_RELAY_QUEUE_BYTES, meter, clock.clone(), 512 << 10, 64 << 10)?;
                    (Backend::Bytes(queue), stock)
                },
                TcpRelayEngine::PermitPump | TcpRelayEngine::OrderedEgressPermitPump => {
                    let (capacity, mode) = if engine == TcpRelayEngine::PermitPump {
                        (CONTINUOUS_TCP_RELAY_QUEUE_BYTES, DownstreamPermitReleaseMode::OnAdmission)
                    } else {
                        (native_tcp_permit_queue_capacity_bytes(cfg), DownstreamPermitReleaseMode::OnEgressDrain)
                    };
                    let (queue, stock) = crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue::new_with_progressive_output_stock(
                        capacity, mode, meter, clock.clone(), 512 << 10, 64 << 10)?;
                    (Backend::Leased(queue), stock)
                },
                _ => return Err(bytes::MeteredCopyError::Quarantined),
            };
            return Ok((Arc::new(Self {
                queue, terminal: Mutex::new(TerminalSlot::default()), failed: AtomicBool::new(false),
                meter: Some(meter.clone()), failures: std::sync::OnceLock::from(clock),
            }), Some(stock)));
        }
        let shared = Self::try_configured_metered(engine, cfg, meter)?;
        if !shared.bind_failures(clock) { return Err(bytes::MeteredCopyError::Quarantined); }
        Ok((shared, None))
    }
    pub(super) fn bind_failures(&self, failures: roles::FailureClock) -> bool {
        self.queue.bind_failures(failures.clone()) && self.failures.set(failures).is_ok()
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn quarantine(&self) {
        if let Some(meter) = &self.meter {
            meter.quarantine();
        }
        self.failed.store(true, Ordering::Release);
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn guard<T>(
        &self,
        f: impl FnOnce() -> T,
    ) -> std::thread::Result<T> {
        let result = catch_unwind(AssertUnwindSafe(f));
        if result.is_err() {
            if let Some(clock) = self.failures.get().filter(|clock| clock.is_bound()) { clock.panic(); }
            self.quarantine();
        }
        result
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn finish(
        &self,
        signal: RelayReaderSignal,
    ) -> bool {
        let Ok(mut slot) = self.terminal.lock() else {
            self.quarantine();
            return false;
        };
        if slot.emitted || slot.interrupted || slot.taken {
            self.quarantine();
            return false;
        }
        slot.signal = Some(signal);
        slot.emitted = true;
        true
    }
    fn take_terminal(&self, held: &mut Option<RelayReaderSignal>) -> bool {
        let Ok(mut slot) = self.terminal.lock() else {
            self.quarantine();
            return false;
        };
        if held.is_some() || slot.interrupted || slot.taken {
            self.quarantine();
            return false;
        }
        slot.interrupted = true;
        *held = slot.signal.take();
        slot.taken = true;
        slot.interrupted = false;
        true
    }
}
pub(super) struct Request {
    pub(super) handle: SocketHandle,
    pub(super) epoch: u64,
    pub(super) shared: Arc<Shared>,
    pub(super) back: Option<crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>>,
    pub(super) signals: Option<mpsc::Sender<RelayReaderSignal>>,
    pub(super) window: ReaderWindow,
    pub(super) stop: Option<oneshot::Receiver<()>>,
    pub(super) gate: oneshot::Receiver<()>,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct DiscardReceipt {
    pub(super) queue: ByteQueueCustody,
    pub(super) held_bytes: usize,
    pub(super) held_allocation: usize,
    pub(super) unpublished: usize,
    pub(super) segment_bytes: usize,
    pub(super) segment_capacity: usize,
    terminals: usize,
}
pub(super) struct Outcome {
    request: Request,
    pub(super) started: bool,
    pub(super) panics: [Option<Box<dyn std::any::Any + Send>>; 6],
    held: Held,
    segment_permit: Option<DownstreamBytePermit>,
    cursor: usize,
    segment: Option<crate::tcp_downlink_pump::OwnedPayload>,
    terminal: Option<RelayReaderSignal>,
    forwarded_terminal: bool,
    emitted_terminal: bool,
    in_flight: bool,
    failed: bool,
    pub(super) reason: Option<(&'static str, &'static str)>,
    pub(super) consumed: PublishedReceipt,
    pub(super) published: PublishedReceipt,
    pub(super) receipt: Option<DiscardReceipt>,
    diag: RelayTaskDiag,
}
impl Outcome {
    pub(super) fn quarantined(&self) -> bool {
        self.failed
            || self.in_flight
            || self.request.shared.failed.load(Ordering::Acquire)
            || self
                .request
                .shared
                .meter
                .as_ref()
                .is_some_and(bytes::BackingMeter::is_quarantined)
            || self.panics.iter().any(Option::is_some)
            || self.request.window.failed()
    }
    pub(super) fn window_read(&self) -> usize {
        self.request.window.read()
    }
    async fn io(&mut self) {
        loop {
            let Some(stop) = self.request.stop.as_mut() else {
                self.failed = true;
                self.request.shared.quarantine();
                return;
            };
            self.in_flight = true;
            let next = tokio::select! {
                biased;
                _ = stop => { self.in_flight = false; return; }
                next = self.request.shared.queue.recv_custodied(
                    super::super::super::super::RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES, &mut self.held) => next,
            };
            self.in_flight = false;
            match next {
                Err(_) => {
                    self.failed = true;
                    self.request.shared.quarantine();
                    return;
                }
                Ok(false) => {
                    if !self.request.shared.take_terminal(&mut self.terminal) {
                        self.failed = true;
                        self.request.shared.quarantine();
                        return;
                    }
                    self.forward_terminal().await;
                    return;
                }
                Ok(true) => {
                    let Some(held) = self.held.as_ref() else {
                        self.failed = true;
                        self.request.shared.quarantine();
                        return;
                    };
                    let count = self.consumed.messages.checked_add(1);
                    let bytes = self.consumed.bytes.checked_add(held.bytes.len());
                    let (Some(messages), Some(bytes)) = (count, bytes) else {
                        self.failed = true;
                        self.request.shared.quarantine();
                        return;
                    };
                    self.consumed = PublishedReceipt { messages, bytes };
                    self.diag.note_remote_read(held.bytes.len());
                    self.cursor = 0;
                    if !self.publish().await {
                        return;
                    }
                    self.held.clear();
                }
            }
        }
    }
    async fn forward_terminal(&mut self) {
        if self.terminal.is_none() {
            return;
        }
        let (Some(stop), Some(signals)) =
            (self.request.stop.as_mut(), self.request.signals.as_ref())
        else {
            self.failed = true;
            self.request.shared.quarantine();
            return;
        };
        let permit = tokio::select! {
            biased;
            _ = stop => return,
            permit = signals.reserve() => permit,
        };
        let Ok(permit) = permit else {
            self.reason = Some(("root", "continuous_signal_closed"));
            return;
        };
        self.in_flight = true;
        let Some(signal) = self.terminal.take() else {
            self.failed = true;
            self.request.shared.quarantine();
            return;
        };
        permit.send(signal);
        self.forwarded_terminal = true;
        self.in_flight = false;
    }
    async fn publish(&mut self) -> bool {
        loop {
            let Some(held) = self.held.as_ref() else {
                self.failed = true;
                self.request.shared.quarantine();
                return false;
            };
            if self.cursor == held.bytes.len() {
                if !self.held.remaining_matches(0) {
                    self.failed = true;
                    self.request.shared.quarantine();
                    return false;
                }
                return true;
            }
            if self.cursor > held.bytes.len() {
                self.failed = true;
                self.request.shared.quarantine();
                return false;
            }
            let Some(remaining) = self.request.window.remaining() else {
                self.failed = true;
                self.request.shared.quarantine();
                return false;
            };
            if self.request.window.stopped() {
                self.reason = Some(("root", "receive_window_failed"));
                return false;
            }
            if remaining == 0 {
                let Some(stop) = self.request.stop.as_mut() else {
                    self.failed = true;
                    self.request.shared.quarantine();
                    return false;
                };
                let changed = tokio::select! {
                    biased;
                    _ = stop => return false,
                    changed = self.request.window.changed() => changed,
                };
                if !changed {
                    self.reason = Some(("root", "continuous_window_closed"));
                    return false;
                }
                continue;
            }
            let held_remaining = held.bytes.len() - self.cursor;
            let n = remaining.min(held_remaining);
            let end = self.cursor + n;
            let messages = self.published.messages.checked_add(1);
            let bytes = self.published.bytes.checked_add(n);
            let (Some(messages), Some(bytes)) = (messages, bytes) else {
                self.failed = true;
                self.request.shared.quarantine();
                return false;
            };
            self.segment = Some(crate::tcp_downlink_pump::OwnedPayload::from_segment(
                &held.bytes,
                self.cursor..end,
            ));
            self.diag.note_remote_batch(1, n);
            let (Some(stop), Some(back)) = (self.request.stop.as_mut(), self.request.back.as_ref())
            else {
                self.failed = true;
                self.request.shared.quarantine();
                return false;
            };
            let started = std::time::Instant::now();
            let permit = tokio::select! {
                biased;
                _ = stop => return false,
                permit = back.reserve() => permit,
            };
            let permit = match permit {
                Ok(permit) => permit,
                Err(error) => {
                    self.reason = Some(("remote_to_local", error.reason()));
                    return false;
                }
            };
            let waited = started.elapsed();
            let threshold = std::time::Duration::from_millis(5);
            self.diag.note_global_rx_wait(waited, threshold);
            super::super::super::super::note_global_rx_channel_occupancy(&mut self.diag, back);
            let service = self.diag.stream_service_window();
            self.in_flight = true;
            self.segment_permit = match self.held.segment(n, held_remaining) {
                Ok(permit) => permit,
                Err(_) => {
                    self.failed = true;
                    self.request.shared.quarantine();
                    return false;
                }
            };
            if !self.request.window.debit(n) {
                self.failed = true;
                self.request.shared.quarantine();
                return false;
            }
            let Some(payload) = self.segment.take() else {
                self.failed = true;
                self.request.shared.quarantine();
                return false;
            };
            permit.send((
                self.request.handle,
                RelayEvent::Data {
                    epoch: self.request.epoch,
                    bytes: payload,
                    stream_service_window: service,
                    downstream_permit: self.segment_permit.take(),
                },
            ));
            self.cursor = end;
            self.published = PublishedReceipt { messages, bytes };
            self.in_flight = false;
            #[cfg(test)]
            self.request.window.observe_published(bytes);
        }
    }
    pub(super) fn dispose(&mut self, cx: &mut Context<'_>) {
        if self.quarantined() || self.receipt.is_some() {
            return;
        }
        self.in_flight = true;
        let queue = match self.request.shared.queue.dispose_custodied() {
            Ok(Some(receipt)) => receipt,
            Ok(None) => {
                self.in_flight = false;
                cx.waker().wake_by_ref();
                return;
            }
            Err(_) => {
                self.failed = true;
                self.request.shared.quarantine();
                return;
            }
        };
        let (held_bytes, held_allocation, unpublished) = match self.held.as_ref() {
            Some(batch) if self.cursor <= batch.bytes.len() => (
                batch.bytes.len(),
                batch.allocation,
                batch.bytes.len() - self.cursor,
            ),
            None => (0, 0, 0),
            _ => {
                self.failed = true;
                self.request.shared.quarantine();
                return;
            }
        };
        if !self.held.remaining_matches(unpublished)
            || self.segment_permit.is_some()
            || queue.batches != self.consumed.messages
            || queue.consumed != self.consumed.bytes
            || self.published.bytes.checked_add(unpublished) != Some(self.consumed.bytes)
            || self.request.window.read() != self.published.bytes
            || self.held.as_ref().is_some_and(|h| {
                self.request.shared.meter.as_ref().is_some_and(|m| {
                    h.bytes.allocation_identity().is_none()
                        || !h
                            .bytes
                            .backing_meter()
                            .as_ref()
                            .is_some_and(|actual| m.same_authority(actual))
                })
            })
            || self.segment.as_ref().is_some_and(|s| {
                self.held.as_ref().is_none_or(|h| {
                    if !h.bytes[self.cursor..].starts_with(s) {
                        return true;
                    }
                    match (&self.request.shared.meter, s) {
                        (Some(m), crate::tcp_downlink_pump::OwnedPayload::Native(bytes)) => {
                            bytes.allocation_identity() != h.bytes.allocation_identity()
                                || bytes.as_ptr() != h.bytes[self.cursor..].as_ptr()
                                || !bytes
                                    .backing_meter()
                                    .as_ref()
                                    .is_some_and(|actual| m.same_authority(actual))
                        }
                        (Some(_), _) => true,
                        (None, _) => false,
                    }
                })
            })
        {
            self.failed = true;
            self.request.shared.quarantine();
            return;
        }
        let Ok(mut slot) = self.request.shared.terminal.lock() else {
            self.failed = true;
            self.request.shared.quarantine();
            return;
        };
        if slot.interrupted {
            self.failed = true;
            self.request.shared.quarantine();
            return;
        }
        slot.interrupted = true;
        self.emitted_terminal = slot.emitted;
        let terminals = usize::from(slot.signal.is_some()) + usize::from(self.terminal.is_some());
        drop(slot.signal.take());
        slot.interrupted = false;
        drop(slot);
        let receipt = DiscardReceipt {
            queue,
            held_bytes,
            held_allocation,
            unpublished,
            segment_bytes: self
                .segment
                .as_ref()
                .map_or(0, crate::tcp_downlink_pump::OwnedPayload::len),
            segment_capacity: self
                .segment
                .as_ref()
                .map_or(0, crate::tcp_downlink_pump::OwnedPayload::capacity),
            terminals,
        };
        self.held.clear();
        self.segment = None;
        self.terminal = None;
        self.receipt = Some(receipt);
        self.in_flight = false;
    }
    pub(super) fn verify(&mut self, emitted: PublishedReceipt, reader_terminal: bool) {
        if self.receipt.is_some() && !self.audit(emitted, reader_terminal) {
            self.failed = true;
            self.request.shared.quarantine();
        }
    }
    pub(super) fn audit(&self, emitted: PublishedReceipt, reader_terminal: bool) -> bool {
        let Some(receipt) = self.receipt else {
            return false;
        };
        !self.quarantined()
            && receipt.queue.sources == emitted.messages
            && receipt.queue.emitted == emitted.bytes
            && self.emitted_terminal == reader_terminal
            && usize::from(self.forwarded_terminal) + receipt.terminals
                == usize::from(reader_terminal)
    }
}
pub(super) async fn run(request: Request) -> Outcome {
    let held = Held::new(&request.shared.queue);
    let mut state = Outcome {
        request,
        started: false,
        panics: [None, None, None, None, None, None],
        held,
        segment_permit: None,
        cursor: 0,
        segment: None,
        terminal: None,
        forwarded_terminal: false,
        emitted_terminal: false,
        in_flight: false,
        failed: false,
        reason: None,
        consumed: PublishedReceipt {
            messages: 0,
            bytes: 0,
        },
        published: PublishedReceipt {
            messages: 0,
            bytes: 0,
        },
        receipt: None,
        diag: RelayTaskDiag::default(),
    };
    let panic_owner = state.request.shared.clone();
    state.started = (&mut state.request.gate).await.is_ok();
    if state.started {
        let mut work = Box::pin(state.io());
        let panic = poll_fn(|cx| match panic_owner.guard(|| work.as_mut().poll(cx)) {
            Ok(Poll::Ready(())) => Poll::Ready(None),
            Ok(Poll::Pending) => Poll::Pending,
            Err(p) => Poll::Ready(Some(p)),
        })
        .await;
        let dropped = panic_owner.guard(|| drop(work)).err();
        state.panics[0] = panic;
        state.panics[1] = dropped;
    }
    let back = state.request.back.take();
    state.panics[2] = panic_owner.guard(|| drop(back)).err();
    let signals = state.request.signals.take();
    state.panics[3] = panic_owner.guard(|| drop(signals)).err();
    let stop = state.request.stop.take();
    state.panics[4] = panic_owner.guard(|| drop(stop)).err();
    let receiver = state.request.window.take_receiver();
    state.panics[5] = panic_owner.guard(|| drop(receiver)).err();
    state
}

// Typed joined outcomes replace the old Thin boolean without another actor or
// registry. FlowRoles still owns the only original dispatcher JoinHandle.
pub(super) enum DispatchOutcome {
    Thin(super::thin::Outcome),
    Continuous(Outcome),
}
impl DispatchOutcome {
    pub(super) fn native_release_ready(&self) -> bool {
        let Self::Continuous(o) = self else { return false; };
        o.receipt.is_some() && !o.quarantined() && o.panics.iter().all(Option::is_none)
            && o.request.back.is_none() && o.request.signals.is_none() && o.request.stop.is_none()
            && o.request.window.receiver_retired() && o.held.as_ref().is_none()
            && o.segment.is_none() && o.segment_permit.is_none() && o.terminal.is_none()
    }

    pub(super) fn dispose(&mut self, cx: &mut Context<'_>) {
        match self {
            Self::Thin(o) => o.dispose(cx),
            Self::Continuous(o) => o.dispose(cx),
        }
    }
    pub(super) fn verify(&mut self, emitted: PublishedReceipt, terminal: bool) {
        match self {
            Self::Thin(o) => o.verify(emitted, terminal),
            Self::Continuous(o) => o.verify(emitted, terminal),
        }
    }
    pub(super) fn audit(&self, emitted: PublishedReceipt, terminal: bool) -> bool {
        match self {
            Self::Thin(o) => o.audit(emitted, terminal),
            Self::Continuous(o) => o.audit(emitted, terminal),
        }
    }
    pub(super) fn published(&self) -> PublishedReceipt {
        match self {
            Self::Thin(o) => o.published,
            Self::Continuous(o) => o.published,
        }
    }
    pub(super) fn reason(&self) -> Option<(&'static str, &'static str)> {
        match self {
            Self::Thin(o) => o.reason,
            Self::Continuous(o) => o.reason,
        }
    }
    pub(super) fn window_read(&self) -> usize {
        match self {
            Self::Thin(o) => o.window_read(),
            Self::Continuous(o) => o.window_read(),
        }
    }
    pub(super) fn quarantined(&self) -> bool {
        match self {
            Self::Thin(o) => o.quarantined(),
            Self::Continuous(o) => o.quarantined(),
        }
    }
    pub(super) fn started(&self) -> bool {
        match self {
            Self::Thin(o) => o.started,
            Self::Continuous(o) => o.started,
        }
    }
}

#[cfg(test)]
#[path = "continuous/tests/mod.rs"]
mod tests;

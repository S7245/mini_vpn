//! Thin's bounded stage and dispatcher transaction. FlowRoles owns every actual
//! task handle; this module owns the original FIFO, held DATA, main-window
//! publication and guarded final disposal. It owns no F/provider/Node authority.
//! Entry `run`; private lifecycle tests are routed from roles.rs.
use super::super::super::super::{
    RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES, StreamServiceWindow, note_global_rx_channel_occupancy,
};
use super::super::receive_window::ReaderWindow;
use super::super::relay_reader::PublishedReceipt;
use super::*;
use crate::tcp_downlink_pump::OwnedPayload;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::{Context, Poll};
use tokio::sync::oneshot;

pub(in crate::client_tun::compatibility_runtime::flow_custody) enum StageItem {
    Data {
        epoch: u64,
        payload: OwnedPayload,
        window: StreamServiceWindow,
    },
    Terminal(RelayReaderSignal),
}
#[derive(Clone, Copy, Debug)]
pub(super) struct DiscardReceipt {
    pub(super) queued: PublishedReceipt,
    pub(super) queued_terminals: usize,
    pub(super) queued_capacity: usize,
    pub(super) held_bytes: usize,
    pub(super) held_capacity: usize,
    pub(super) segment_bytes: usize,
    pub(super) segment_capacity: usize,
    pub(super) unpublished: usize,
    held_terminal: bool,
}
pub(super) struct Request {
    pub(super) handle: SocketHandle,
    pub(super) epoch: u64,
    pub(super) rx: mpsc::Receiver<StageItem>,
    pub(super) back: Option<crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>>,
    pub(super) signals: Option<mpsc::Sender<RelayReaderSignal>>,
    pub(super) window: ReaderWindow,
    pub(super) stop: Option<oneshot::Receiver<()>>,
    pub(super) gate: oneshot::Receiver<()>,
}
pub(super) struct Outcome {
    request: Request,
    pub(super) started: bool,
    pub(super) panics: [Option<Box<dyn std::any::Any + Send>>; 6],
    held: Option<StageItem>,
    cursor: usize,
    segment: Option<OwnedPayload>,
    in_flight: bool,
    failed: bool,
    pub(super) reason: Option<(&'static str, &'static str)>,
    pub(super) consumed: PublishedReceipt,
    pub(super) published: PublishedReceipt,
    pub(super) terminals: usize,
    forwarded_terminal: bool,
    eof: bool,
    closed: bool,
    queued: PublishedReceipt,
    queued_terminals: usize,
    queued_capacity: usize,
    pub(super) receipt: Option<DiscardReceipt>,
    diag: RelayTaskDiag,
}
impl Outcome {
    pub(super) fn quarantined(&self) -> bool {
        self.failed
            || self.in_flight
            || self.panics.iter().any(Option::is_some)
            || self.request.window.failed()
    }
    pub(super) fn window_read(&self) -> usize {
        self.request.window.read()
    }
    fn bad(&mut self) {
        self.failed = true;
    }
    async fn io(&mut self) {
        loop {
            let Some(stop) = self.request.stop.as_mut() else {
                self.bad();
                return;
            };
            self.in_flight = true;
            let item = tokio::select! {
                biased;
                _ = stop => { self.in_flight = false; return; }
                item = self.request.rx.recv() => item,
            };
            self.held = item;
            self.in_flight = false;
            match self.held.as_ref() {
                None => {
                    self.eof = true;
                    return;
                }
                Some(StageItem::Terminal(_)) => {
                    if self.terminals != 0 {
                        self.bad();
                        return;
                    }
                    self.terminals = 1;
                    let (Some(stop), Some(signals)) =
                        (self.request.stop.as_mut(), self.request.signals.as_ref())
                    else {
                        self.bad();
                        return;
                    };
                    let permit = tokio::select! {
                        biased;
                        _ = stop => return,
                        permit = signals.reserve() => permit,
                    };
                    let Ok(permit) = permit else {
                        self.reason = Some(("root", "thin_signal_closed"));
                        return;
                    };
                    self.in_flight = true;
                    let Some(StageItem::Terminal(signal)) = self.held.take() else {
                        self.failed = true;
                        return;
                    };
                    permit.send(signal);
                    self.forwarded_terminal = true;
                    self.in_flight = false;
                }
                Some(StageItem::Data { epoch, payload, .. }) => {
                    if self.terminals != 0
                        || *epoch != self.request.epoch
                        || payload.is_empty()
                        || payload.len() > RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES
                    {
                        self.bad();
                        return;
                    }
                    let Some(messages) = self.consumed.messages.checked_add(1) else {
                        self.bad();
                        return;
                    };
                    let Some(bytes) = self.consumed.bytes.checked_add(payload.len()) else {
                        self.bad();
                        return;
                    };
                    self.consumed = PublishedReceipt { messages, bytes };
                    self.cursor = 0;
                    if !self.publish().await {
                        return;
                    }
                    self.held = None;
                }
            }
        }
    }
    async fn publish(&mut self) -> bool {
        loop {
            let Some(StageItem::Data {
                payload, window, ..
            }) = &self.held
            else {
                self.failed = true;
                return false;
            };
            if self.cursor == payload.len() {
                return true;
            }
            if self.cursor > payload.len() {
                self.failed = true;
                return false;
            }
            let Some(remaining) = self.request.window.remaining() else {
                self.failed = true;
                return false;
            };
            if self.request.window.stopped() {
                self.reason = Some(("root", "receive_window_failed"));
                return false;
            }
            if remaining == 0 {
                let Some(stop) = self.request.stop.as_mut() else {
                    self.failed = true;
                    return false;
                };
                let changed = tokio::select! {
                    biased;
                    _ = stop => return false,
                    changed = self.request.window.changed() => changed,
                };
                if !changed {
                    self.reason = Some(("root", "thin_window_closed"));
                    return false;
                }
                continue;
            }
            let n = remaining.min(payload.len() - self.cursor);
            let Some(end) = self.cursor.checked_add(n) else {
                self.failed = true;
                return false;
            };
            let Some(messages) = self.published.messages.checked_add(1) else {
                self.failed = true;
                return false;
            };
            let Some(bytes) = self.published.bytes.checked_add(n) else {
                self.failed = true;
                return false;
            };
            self.segment = Some(payload.segment(self.cursor..end));
            let mut service = *window;
            let started = std::time::Instant::now();
            let (Some(stop), Some(back)) = (self.request.stop.as_mut(), self.request.back.as_ref())
            else {
                self.failed = true;
                return false;
            };
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
            let threshold = std::time::Duration::from_millis(5);
            let waited = started.elapsed();
            self.diag.note_global_rx_wait(waited, threshold);
            note_global_rx_channel_occupancy(&mut self.diag, back);
            service.note_global_rx_wait(waited, threshold);
            service.note_global_rx_queue(
                self.diag.global_rx_queue_used_max,
                self.diag.global_rx_queue_capacity,
            );
            self.in_flight = true;
            if !self.request.window.debit(n) {
                self.failed = true;
                return false;
            }
            let Some(bytes_vec) = self.segment.take() else {
                self.failed = true;
                return false;
            };
            permit.send((
                self.request.handle,
                RelayEvent::Data {
                    epoch: self.request.epoch,
                    bytes: bytes_vec.into(),
                    stream_service_window: service,
                    downstream_permit: None,
                },
            ));
            self.cursor = end;
            self.published = PublishedReceipt { messages, bytes };
            self.in_flight = false;
            #[cfg(test)]
            self.request.window.observe_published(bytes);
        }
    }
    /// Only FlowRoles calls this after all four joins and producer stop. Each
    /// turn drains at most32 original items; a panic leaves in_flight sticky.
    pub(super) fn dispose(&mut self, cx: &mut Context<'_>) {
        if self.quarantined() || self.receipt.is_some() {
            return;
        }
        self.in_flight = true;
        if !self.closed {
            self.request.rx.close();
            self.closed = true;
        }
        for _ in 0..32 {
            match self.request.rx.poll_recv(cx) {
                Poll::Ready(Some(StageItem::Data { epoch, payload, .. })) => {
                    if self.terminals != 0
                        || self.queued_terminals != 0
                        || epoch != self.request.epoch
                        || payload.is_empty()
                        || payload.len() > RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES
                    {
                        self.failed = true;
                        return;
                    }
                    let Some(messages) = self.queued.messages.checked_add(1) else {
                        self.failed = true;
                        return;
                    };
                    let Some(bytes) = self.queued.bytes.checked_add(payload.len()) else {
                        self.failed = true;
                        return;
                    };
                    let Some(capacity) = self.queued_capacity.checked_add(payload.capacity())
                    else {
                        self.failed = true;
                        return;
                    };
                    self.queued = PublishedReceipt { messages, bytes };
                    self.queued_capacity = capacity;
                    drop(payload);
                }
                Poll::Ready(Some(StageItem::Terminal(_))) => {
                    let Some(n) = self.queued_terminals.checked_add(1) else {
                        self.failed = true;
                        return;
                    };
                    if n > 1 {
                        self.failed = true;
                        return;
                    }
                    self.queued_terminals = n;
                }
                Poll::Ready(None) => {
                    self.eof = true;
                    break;
                }
                Poll::Pending => break,
            }
        }
        if !self.eof {
            self.in_flight = false;
            cx.waker().wake_by_ref();
            return;
        }
        let (held_bytes, held_capacity, unpublished) = match &self.held {
            Some(StageItem::Data { payload, .. }) if self.cursor <= payload.len() => (
                payload.len(),
                payload.capacity(),
                payload.len() - self.cursor,
            ),
            Some(StageItem::Terminal(_)) | None => (0, 0, 0),
            _ => {
                self.failed = true;
                return;
            }
        };
        if Some(self.consumed.bytes) != self.published.bytes.checked_add(unpublished)
            || self.request.window.read() != self.published.bytes
            || self
                .segment
                .as_ref()
                .is_some_and(|segment| match &self.held {
                    Some(StageItem::Data { payload, .. }) => {
                        !payload[self.cursor..].starts_with(segment)
                    }
                    _ => true,
                })
        {
            self.failed = true;
            return;
        }
        let receipt = DiscardReceipt {
            queued: self.queued,
            queued_terminals: self.queued_terminals,
            queued_capacity: self.queued_capacity,
            held_bytes,
            held_capacity,
            segment_bytes: self.segment.as_ref().map_or(0, OwnedPayload::len),
            segment_capacity: self.segment.as_ref().map_or(0, OwnedPayload::capacity),
            unpublished,
            held_terminal: matches!(self.held, Some(StageItem::Terminal(_))),
        };
        self.held = None;
        self.segment = None;
        self.receipt = Some(receipt);
        self.in_flight = false;
    }
    pub(super) fn verify(&mut self, emitted: PublishedReceipt, reader_terminal: bool) {
        if self.receipt.is_some() && !self.audit(emitted, reader_terminal) {
            self.failed = true;
        }
    }
    pub(super) fn audit(&self, emitted: PublishedReceipt, reader_terminal: bool) -> bool {
        let Some(receipt) = self.receipt else {
            return false;
        };
        !self.quarantined()
            && self.consumed.messages.checked_add(receipt.queued.messages) == Some(emitted.messages)
            && self.consumed.bytes.checked_add(receipt.queued.bytes) == Some(emitted.bytes)
            && self.terminals.checked_add(receipt.queued_terminals)
                == Some(usize::from(reader_terminal))
            && self.terminals
                == usize::from(self.forwarded_terminal) + usize::from(receipt.held_terminal)
    }
}

pub(super) async fn run(request: Request) -> Outcome {
    let mut state = Outcome {
        request,
        started: false,
        panics: [None, None, None, None, None, None],
        held: None,
        cursor: 0,
        segment: None,
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
        terminals: 0,
        forwarded_terminal: false,
        eof: false,
        closed: false,
        queued: PublishedReceipt {
            messages: 0,
            bytes: 0,
        },
        queued_terminals: 0,
        queued_capacity: 0,
        receipt: None,
        diag: RelayTaskDiag::default(),
    };
    state.started = (&mut state.request.gate).await.is_ok();
    if state.started {
        let mut work = Box::pin(state.io());
        let panic = poll_fn(
            |cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
                Ok(Poll::Ready(())) => Poll::Ready(None),
                Ok(Poll::Pending) => Poll::Pending,
                Err(p) => Poll::Ready(Some(p)),
            },
        )
        .await;
        let drop_panic = catch_unwind(AssertUnwindSafe(|| drop(work))).err();
        state.panics[0] = panic;
        state.panics[1] = drop_panic;
    }
    // Drop each original wake-capable endpoint independently after outcomes
    // have been installed; FIFO DATA and held buffers remain for the root.
    let back = state.request.back.take();
    state.panics[2] = catch_unwind(AssertUnwindSafe(|| drop(back))).err();
    let signals = state.request.signals.take();
    state.panics[3] = catch_unwind(AssertUnwindSafe(|| drop(signals))).err();
    let stop = state.request.stop.take();
    state.panics[4] = catch_unwind(AssertUnwindSafe(|| drop(stop))).err();
    let receiver = state.request.window.take_receiver();
    state.panics[5] = catch_unwind(AssertUnwindSafe(|| drop(receiver))).err();
    state
}

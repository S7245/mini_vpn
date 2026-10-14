//! Generic main-channel reader custody: credit, batching, publication and close.
//!
//! One owner retains the first read error, local buffers and exact published
//! counters outside the borrowed IO future. Send/wake interruption is quarantined,
//! never retried. Local disposal does not dispose DATA already in the global queue
//! or retire the shared carrier; the root must join all roles and settle those.
//! Legacy run drops its outcome before task completion. Continuous shares the
//! publication kernel. Tests: tests/outcomes.rs and client_tun/tests/relay_reader_batch.rs.

use super::super::super::{
    RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES, RELAY_REMOTE_READ_BURST_MAX_CHUNKS, RelayCloseReason,
    RelayEvent, RelayReadCredit, RelayReaderSignal, RelayStream, RelayTaskDiag, RemoteReadBurst,
    SocketHandle, append_remote_read_to_batch, drain_relay_read_credit_updates,
    note_global_rx_channel_occupancy, note_reader_local_finish_if_needed, poll_ready_remote_read,
    relay_remote_ready_batch_byte_limit, relay_stream_poll_credit_for_local_admission,
    send_relay_reader_progress, tcp_diag_enabled,
};
use super::receive_window::ReaderWindow;
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::Arc;
use std::task::Poll;
use tokio::io::AsyncReadExt;
use tokio::sync::{mpsc, oneshot, watch};

#[cfg(test)]
pub(in crate::client_tun) struct ReadyReadRequest<'a> {
    pub(in crate::client_tun) remote_reader: &'a mut tokio::io::ReadHalf<RelayStream>,
    pub(in crate::client_tun) handle: SocketHandle,
    pub(in crate::client_tun) epoch: u64,
    pub(in crate::client_tun) initial_payload: Vec<u8>,
    pub(in crate::client_tun) buf: &'a mut [u8],
    pub(in crate::client_tun) back_tx: &'a mpsc::Sender<(SocketHandle, RelayEvent)>,
    pub(in crate::client_tun) diag: &'a mut RelayTaskDiag,
    pub(in crate::client_tun) read_credit: RelayReadCredit,
    pub(in crate::client_tun) global_rx_pressure_threshold: std::time::Duration,
}

pub(super) struct ReaderRequest {
    pub(super) handle: SocketHandle,
    pub(super) epoch: u64,
    pub(super) remote_reader: tokio::io::ReadHalf<RelayStream>,
    pub(super) back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
    pub(super) read_credit_rx: watch::Receiver<RelayReadCredit>,
    pub(super) signal_tx: mpsc::Sender<RelayReaderSignal>,
    pub(super) local_finish_seen: Arc<std::sync::atomic::AtomicBool>,
    pub(super) stop_rx: oneshot::Receiver<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ReaderTerminal {
    Interrupted,
    Eof,
    ReadFailed,
    GlobalClosed,
    SignalClosed,
    CreditClosed,
    Stopped,
    AccountingFailed,
}

impl ReaderTerminal {
    fn close_reason(self) -> Option<RelayCloseReason> {
        match self {
            Self::Eof => Some(("remote_to_local", "remote_eof")),
            Self::ReadFailed => Some(("remote_to_local", "remote_read_failed")),
            Self::GlobalClosed => Some(("remote_to_local", "global_rx_closed")),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PublishedReceipt {
    pub(super) messages: usize,
    pub(super) bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Span {
    start: usize,
    end: usize,
}

#[derive(Default)]
struct Publication {
    pending: Option<Vec<u8>>,
    cursor: usize,
    // Only send itself owns this mark. A panic after successful commit keeps the
    // committed cursor/counters and is quarantined by the outer poll guard.
    in_flight: Option<Span>,
    messages: usize,
    bytes: usize,
}

struct Dispatch<'a> {
    handle: SocketHandle,
    epoch: u64,
    back_tx: &'a mpsc::Sender<(SocketHandle, RelayEvent)>,
    diag: &'a mut RelayTaskDiag,
    stop: Option<&'a mut oneshot::Receiver<()>>,
    threshold: std::time::Duration,
}

async fn stopped(stop: &mut Option<&mut oneshot::Receiver<()>>) {
    match stop {
        Some(receiver) => {
            let _ = receiver.await;
        }
        None => std::future::pending::<()>().await,
    }
}

impl Publication {
    async fn send(
        &mut self,
        dispatch: &mut Dispatch<'_>,
        chunks: usize,
    ) -> Result<(), ReaderTerminal> {
        let Some(payload) = self.pending.as_ref() else {
            return Err(ReaderTerminal::AccountingFailed);
        };
        if self.in_flight.is_some() {
            return Err(ReaderTerminal::AccountingFailed);
        }
        let payload_len = payload.len();
        let Some(end) = self.cursor.checked_add(payload_len) else {
            return Err(ReaderTerminal::AccountingFailed);
        };
        let Some(messages) = self.messages.checked_add(1) else {
            return Err(ReaderTerminal::AccountingFailed);
        };
        let Some(bytes) = self.bytes.checked_add(payload_len) else {
            return Err(ReaderTerminal::AccountingFailed);
        };
        dispatch.diag.note_remote_batch(chunks, payload_len);
        note_global_rx_channel_occupancy(dispatch.diag, dispatch.back_tx);
        let wait_started = std::time::Instant::now();
        let permit_result = tokio::select! {
            _ = stopped(&mut dispatch.stop) => return Err(ReaderTerminal::Stopped),
            permit = dispatch.back_tx.reserve() => permit,
        };
        let waited = wait_started.elapsed();
        dispatch
            .diag
            .note_global_rx_wait(waited, dispatch.threshold);
        let Ok(permit) = permit_result else {
            return Err(ReaderTerminal::GlobalClosed);
        };
        note_global_rx_channel_occupancy(dispatch.diag, dispatch.back_tx);
        let stream_service_window = dispatch.diag.stream_service_window();
        self.in_flight = Some(Span {
            start: self.cursor,
            end,
        });
        let Some(payload) = self.pending.take() else {
            return Err(ReaderTerminal::AccountingFailed);
        };
        permit.send((
            dispatch.handle,
            RelayEvent::Data {
                epoch: dispatch.epoch,
                bytes: payload,
                stream_service_window,
                downstream_permit: None,
            },
        ));
        self.cursor = end;
        self.messages = messages;
        self.bytes = bytes;
        self.in_flight = None;
        note_global_rx_channel_occupancy(dispatch.diag, dispatch.back_tx);
        if waited >= dispatch.threshold {
            tcp_diag_log!(
                "🔎 tcp-global-rx-pressure handle={:?} wait_us={} payload_bytes={} pressure_events={}",
                dispatch.handle,
                waited.as_micros(),
                payload_len,
                dispatch.diag.global_rx_pressure_events
            );
        }
        Ok(())
    }
}

#[derive(Default)]
struct ReadyBatch {
    failure_clock: Option<super::supervision::FailureClock>,
    error_order: Option<usize>,
    initial: Vec<u8>,
    payload: Vec<u8>,
    burst: RemoteReadBurst,
    close_after: Option<ReaderTerminal>,
    failure: Option<std::io::Error>,
}

struct BatchRead<'a> {
    remote: &'a mut tokio::io::ReadHalf<RelayStream>,
    handle: SocketHandle,
    buf: &'a mut [u8],
    valid: &'a mut usize,
    reading: &'a mut bool,
    byte_limit: usize,
    window: Option<&'a mut ReaderWindow>,
    diag: &'a mut RelayTaskDiag,
}

impl ReadyBatch {
    fn fail(&mut self, error: std::io::Error) {
        self.failure = Some(error);
        self.error_order = super::supervision::stamp_failure(&self.failure_clock);
    }

    async fn gather(&mut self, mut read: BatchRead<'_>) {
        read.diag.note_remote_batch_limit(read.byte_limit);
        self.payload = Vec::with_capacity(self.initial.len().min(read.byte_limit));
        append_remote_read_to_batch(read.diag, &mut self.burst, &mut self.payload, &self.initial);
        while self.burst.chunks < RELAY_REMOTE_READ_BURST_MAX_CHUNKS
            && self.burst.bytes < read.byte_limit
        {
            let mut read_len = (read.byte_limit - self.burst.bytes).min(read.buf.len());
            if let Some(window) = read.window.as_mut() {
                let Some(remaining) = window.remaining() else {
                    self.close_after = Some(ReaderTerminal::AccountingFailed);
                    return;
                };
                read_len = read_len.min(remaining);
            }
            if read_len == 0 {
                break;
            }
            *read.valid = 0;
            *read.reading = true;
            let result = poll_ready_remote_read(read.remote, &mut read.buf[..read_len]).await;
            *read.reading = false;
            match result {
                None => break,
                Some(Ok(0)) => {
                    self.close_after = Some(ReaderTerminal::Eof);
                    println!("远端服务器关闭了车厢 {:?}", read.handle);
                    break;
                }
                Some(Ok(n)) => {
                    *read.valid = n;
                    if read.window.as_mut().is_some_and(|window| !window.debit(n)) {
                        self.close_after = Some(ReaderTerminal::AccountingFailed);
                        return;
                    }
                    append_remote_read_to_batch(
                        read.diag,
                        &mut self.burst,
                        &mut self.payload,
                        &read.buf[..n],
                    );
                    *read.valid = 0;
                }
                Some(Err(error)) => {
                    self.fail(error);
                    self.close_after = Some(ReaderTerminal::ReadFailed);
                    println!(
                        "读取上游流失败 direction=remote_to_local handle={:?} err={:?}",
                        read.handle, self.failure
                    );
                    break;
                }
            }
        }
    }

    async fn publish(
        &self,
        publication: &mut Publication,
        dispatch: &mut Dispatch<'_>,
    ) -> Result<(), ReaderTerminal> {
        while publication.cursor < self.payload.len() {
            let end = publication
                .cursor
                .saturating_add(RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES)
                .min(self.payload.len());
            let chunks = if publication.cursor == 0 && end == self.payload.len() {
                self.burst.chunks
            } else {
                1
            };
            if publication.pending.is_none() {
                publication.pending = Some(self.payload[publication.cursor..end].to_vec());
            }
            publication.send(dispatch, chunks).await?;
            if publication.cursor < self.payload.len() {
                tokio::task::yield_now().await;
            }
        }
        Ok(())
    }
}

/// Continuous dispatch transfers its original Vec; it does not add a batch copy.
pub(in crate::client_tun) async fn send_remote_payload_batch_to_main(
    handle: SocketHandle,
    epoch: u64,
    payload: Vec<u8>,
    batch_chunks: usize,
    back_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
    diag: &mut RelayTaskDiag,
    global_rx_pressure_threshold: std::time::Duration,
) -> Result<(), RelayCloseReason> {
    let mut publication = Publication {
        pending: Some(payload),
        ..Publication::default()
    };
    publication
        .send(
            &mut Dispatch {
                handle,
                epoch,
                back_tx,
                diag,
                stop: None,
                threshold: global_rx_pressure_threshold,
            },
            batch_chunks,
        )
        .await
        .map_err(|terminal| {
            terminal
                .close_reason()
                .unwrap_or(("remote_to_local", "publication_accounting_failed"))
        })
}

#[cfg(test)]
pub(in crate::client_tun) async fn drain_ready_remote_reads(
    request: ReadyReadRequest<'_>,
) -> Result<(RemoteReadBurst, Option<RelayCloseReason>), RelayCloseReason> {
    let ReadyReadRequest {
        remote_reader,
        handle,
        epoch,
        initial_payload,
        buf,
        back_tx,
        diag,
        read_credit,
        global_rx_pressure_threshold,
    } = request;
    let limit = relay_remote_ready_batch_byte_limit(back_tx)
        .min(read_credit.max_batch_bytes)
        .max(initial_payload.len());
    let mut batch = ReadyBatch {
        initial: initial_payload,
        ..ReadyBatch::default()
    };
    let mut valid = 0;
    let mut reading = false;
    batch
        .gather(BatchRead {
            remote: remote_reader,
            handle,
            buf,
            valid: &mut valid,
            reading: &mut reading,
            byte_limit: limit,
            window: None,
            diag,
        })
        .await;
    let close = batch.close_after.and_then(ReaderTerminal::close_reason);
    if batch.payload.is_empty() {
        return close.map_or(Ok((batch.burst, None)), Err);
    }
    batch
        .publish(
            &mut Publication::default(),
            &mut Dispatch {
                handle,
                epoch,
                back_tx,
                diag,
                stop: None,
                threshold: global_rx_pressure_threshold,
            },
        )
        .await
        .map_err(|terminal| {
            terminal
                .close_reason()
                .unwrap_or(("remote_to_local", "publication_accounting_failed"))
        })?;
    Ok((batch.burst, close))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ReaderDiscardReceipt {
    pub(super) initial_bytes: usize,
    pub(super) batch_bytes: usize,
    pub(super) segment_bytes: usize,
    pub(super) scratch_bytes: usize,
    pub(super) unpublished_bytes: usize,
}

pub(super) struct ReaderRemainder {
    batch: ReadyBatch,
    publication: Publication,
    scratch: Option<Box<[u8; 65_536]>>,
    valid: usize,
    reading: bool,
    quarantined: bool,
    receipt: Option<ReaderDiscardReceipt>,
    window: Option<ReaderWindow>,
}

impl ReaderRemainder {
    pub(super) fn quarantined(&self) -> bool {
        self.quarantined
            || self.reading
            || self.publication.in_flight.is_some()
            || self.window.as_ref().is_some_and(ReaderWindow::failed)
    }

    pub(super) fn read_total(&self) -> Option<usize> {
        self.window.as_ref().map(ReaderWindow::read)
    }

    pub(super) fn published(&self) -> PublishedReceipt {
        PublishedReceipt {
            messages: self.publication.messages,
            bytes: self.publication.bytes,
        }
    }

    /// Called only after the root retains the joined outcome. Physical duplicate
    /// buffers are reported separately from the single logical unpublished tail.
    /// This receipt says nothing about disposal of already-published global DATA.
    pub(super) fn discard_local(&mut self) -> Option<ReaderDiscardReceipt> {
        if self.quarantined() {
            return None;
        }
        if let Some(receipt) = self.receipt {
            return Some(receipt);
        }
        let segment_bytes = self.publication.pending.as_ref().map_or(0, Vec::len);
        let valid = self.publication.cursor <= self.batch.payload.len()
            && self.valid <= self.scratch.as_ref().map_or(0, |buf| buf.len())
            && self.publication.pending.as_ref().is_none_or(|segment| {
                self.batch
                    .payload
                    .get(self.publication.cursor..)
                    .is_some_and(|tail| tail.starts_with(segment))
            });
        if !valid {
            self.quarantined = true;
            return None;
        }
        let receipt = ReaderDiscardReceipt {
            initial_bytes: self.batch.initial.len(),
            batch_bytes: self.batch.payload.len(),
            segment_bytes,
            scratch_bytes: self.valid,
            unpublished_bytes: self.batch.payload.len() - self.publication.cursor,
        };
        if self.window.as_ref().is_some_and(|window| {
            self.publication
                .bytes
                .checked_add(receipt.unpublished_bytes)
                != Some(window.read())
                || self.valid != 0
        }) {
            self.quarantined = true;
            return None;
        }
        self.batch.initial = Vec::new();
        self.batch.payload = Vec::new();
        self.publication.pending = None;
        self.scratch = None;
        self.valid = 0;
        self.receipt = Some(receipt);
        Some(receipt)
    }
}

#[derive(Default)]
pub(super) struct ReaderNotification {
    pub(super) in_flight: bool,
    pub(super) closed_sent: bool,
}

#[must_use]
pub(super) struct ReaderOutcome {
    pub(super) started: bool,
    pub(super) error_order: Option<usize>,
    pub(super) terminal: ReaderTerminal,
    pub(super) failure: Option<std::io::Error>,
    pub(super) panics: [Option<Box<dyn std::any::Any + Send>>; 8],
    pub(super) notification: ReaderNotification,
    pub(super) diag: RelayTaskDiag,
    pub(super) remainder: ReaderRemainder,
}

struct ReaderState {
    request: ReaderRequest,
    remainder: ReaderRemainder,
    terminal: ReaderTerminal,
    notification: ReaderNotification,
    diag: RelayTaskDiag,
}

async fn notify_closed(state: &mut ReaderState, reason: RelayCloseReason) {
    let permit = tokio::select! {
        _ = &mut state.request.stop_rx => {
            state.terminal = ReaderTerminal::Stopped;
            return;
        }
        permit = state.request.signal_tx.reserve() => permit,
    };
    let Ok(permit) = permit else {
        state.terminal = ReaderTerminal::SignalClosed;
        return;
    };
    let signal = RelayReaderSignal::Closed {
        direction: reason.0,
        reason: reason.1,
        diag: state.diag.clone(),
    };
    state.notification.in_flight = true;
    permit.send(signal);
    state.notification.closed_sent = true;
    state.notification.in_flight = false;
}

async fn run_reader_io(state: &mut ReaderState) {
    let mut read_credit = *state.request.read_credit_rx.borrow();
    state.diag.note_read_credit(read_credit);
    loop {
        let request = &mut state.request;
        let remainder = &mut state.remainder;
        note_reader_local_finish_if_needed(&request.local_finish_seen, &mut state.diag);
        drain_relay_read_credit_updates(
            &mut request.read_credit_rx,
            &mut read_credit,
            &mut state.diag,
        );
        let stream_poll_credit = relay_stream_poll_credit_for_local_admission(read_credit);
        let Some(buf) = remainder.scratch.as_mut() else {
            state.terminal = ReaderTerminal::AccountingFailed;
            return;
        };
        let mut read_len = state
            .diag
            .next_remote_read_len(stream_poll_credit, buf.len());
        if let Some(window) = &mut remainder.window {
            let Some(remaining) = window.remaining() else {
                state.terminal = ReaderTerminal::AccountingFailed;
                return;
            };
            read_len = read_len.min(remaining);
            if window.stopped() {
                state.terminal = ReaderTerminal::CreditClosed;
                notify_closed(state, ("root", "receive_window_failed")).await;
                return;
            }
        }
        if read_len == 0 {
            let credit_update = tokio::select! {
                _ = &mut request.stop_rx => {
                    state.terminal = ReaderTerminal::Stopped;
                    return;
                }
                update = request.read_credit_rx.changed() => update.is_ok(),
                update = async {
                    match &mut remainder.window {
                        Some(window) => window.changed().await,
                        None => std::future::pending().await,
                    }
                } => update,
            };
            if !credit_update {
                state.terminal = ReaderTerminal::CreditClosed;
                return;
            }
            read_credit = *request.read_credit_rx.borrow_and_update();
            state.diag.note_read_credit(read_credit);
            if read_credit.paused {
                tcp_diag_log!(
                    "🔎 tcp-relay-read-credit handle={:?} epoch={} paused=true max_batch_bytes=0 updates={} pause_updates={}",
                    request.handle,
                    request.epoch,
                    state.diag.read_credit_updates,
                    state.diag.read_credit_pause_updates
                );
            }
            continue;
        }
        remainder.valid = 0;
        remainder.reading = true;
        let remote_msg = tokio::select! {
            _ = &mut request.stop_rx => {
                remainder.reading = false;
                state.terminal = ReaderTerminal::Stopped;
                return;
            }
            _ = async {
                match &mut remainder.window {
                    Some(window) => window.wait_stopped().await,
                    None => std::future::pending().await,
                }
            } => {
                remainder.reading = false;
                state.terminal = ReaderTerminal::CreditClosed;
                notify_closed(state, ("root", "receive_window_failed")).await;
                return;
            }
            result = request.remote_reader.read(&mut buf[..read_len]) => result,
        };
        remainder.reading = false;
        match remote_msg {
            Ok(0) => {
                remainder.batch.close_after = Some(ReaderTerminal::Eof);
                state.terminal = ReaderTerminal::Eof;
                println!("远端服务器关闭了车厢 {:?}", request.handle);
            }
            Err(error) => {
                remainder.batch.fail(error);
                remainder.batch.close_after = Some(ReaderTerminal::ReadFailed);
                state.terminal = ReaderTerminal::ReadFailed;
                println!(
                    "读取上游流失败 direction=remote_to_local handle={:?} err={:?}",
                    request.handle, remainder.batch.failure
                );
            }
            Ok(n) => {
                remainder.valid = n;
                if remainder
                    .window
                    .as_mut()
                    .is_some_and(|window| !window.debit(n))
                {
                    state.terminal = ReaderTerminal::AccountingFailed;
                    return;
                }
                note_reader_local_finish_if_needed(&request.local_finish_seen, &mut state.diag);
                remainder.batch.initial = buf[..n].to_vec();
                remainder.valid = 0;
                let limit = relay_remote_ready_batch_byte_limit(&request.back_tx)
                    .min(stream_poll_credit.max_batch_bytes)
                    .max(n);
                remainder
                    .batch
                    .gather(BatchRead {
                        remote: &mut request.remote_reader,
                        handle: request.handle,
                        buf: &mut buf[..],
                        valid: &mut remainder.valid,
                        reading: &mut remainder.reading,
                        byte_limit: limit,
                        window: remainder.window.as_mut(),
                        diag: &mut state.diag,
                    })
                    .await;
                let publish = remainder
                    .batch
                    .publish(
                        &mut remainder.publication,
                        &mut Dispatch {
                            handle: request.handle,
                            epoch: request.epoch,
                            back_tx: &request.back_tx,
                            diag: &mut state.diag,
                            stop: Some(&mut request.stop_rx),
                            threshold: std::time::Duration::from_millis(5),
                        },
                    )
                    .await;
                #[cfg(test)]
                if let Some(window) = &remainder.window {
                    window.observe_published(remainder.publication.bytes);
                }
                if let Err(terminal) = publish {
                    state.terminal = terminal;
                    if let Some(reason) = terminal.close_reason() {
                        notify_closed(state, reason).await;
                    }
                    return;
                }
                state.notification.in_flight = true;
                let progress = send_relay_reader_progress(&request.signal_tx, &state.diag);
                state.notification.in_flight = false;
                if !progress {
                    state.terminal = ReaderTerminal::SignalClosed;
                    return;
                }
                if let Some(terminal) = remainder.batch.close_after {
                    state.terminal = terminal;
                } else {
                    let failure_clock = remainder.batch.failure_clock.take();
                    remainder.batch = ReadyBatch {
                        failure_clock,
                        ..ReadyBatch::default()
                    };
                    // Only the cycle resets; exact published totals never do.
                    remainder.publication.cursor = 0;
                    continue;
                }
            }
        }
        if let Some(reason) = state.terminal.close_reason() {
            notify_closed(state, reason).await;
        }
        return;
    }
}

/// Preserve legacy JoinHandle<()> lifetime and panic behavior.
pub(super) async fn run_relay_reader(request: ReaderRequest) {
    let mut outcome = run_custodied_relay_reader(request).await;
    let panic = outcome.panics.iter_mut().find_map(Option::take);
    drop(outcome);
    if let Some(payload) = panic {
        resume_unwind(payload);
    }
}

pub(super) async fn run_custodied_relay_reader(request: ReaderRequest) -> ReaderOutcome {
    run_custodied_relay_reader_gated(request, None, None, None).await
}

pub(super) async fn run_custodied_relay_reader_gated(
    request: ReaderRequest,
    gate: Option<oneshot::Receiver<()>>,
    failure_clock: Option<super::supervision::FailureClock>,
    window: Option<ReaderWindow>,
) -> ReaderOutcome {
    let mut state = ReaderState {
        request,
        remainder: ReaderRemainder {
            batch: ReadyBatch {
                failure_clock,
                ..ReadyBatch::default()
            },
            publication: Publication::default(),
            scratch: Some(Box::new([0; 65_536])),
            valid: 0,
            reading: false,
            quarantined: false,
            receipt: None,
            window,
        },
        terminal: ReaderTerminal::Interrupted,
        notification: ReaderNotification::default(),
        diag: RelayTaskDiag::default(),
    };
    // One role-start allocation, never per-record. Both polling and dropping the
    // borrowed future may invoke channel wakers; keep state alive across both.
    let started = match gate {
        Some(gate) => gate.await.is_ok(),
        None => true,
    };
    let (body_panic, future_drop_panic) = if !started {
        state.terminal = ReaderTerminal::Stopped;
        (None, None)
    } else {
        let mut body = Box::pin(run_reader_io(&mut state));
        let body_panic =
            poll_fn(
                |cx| match catch_unwind(AssertUnwindSafe(|| body.as_mut().poll(cx))) {
                    Ok(Poll::Ready(())) => Poll::Ready(None),
                    Ok(Poll::Pending) => Poll::Pending,
                    Err(payload) => Poll::Ready(Some(payload)),
                },
            )
            .await;
        let future_drop_panic = catch_unwind(AssertUnwindSafe(|| drop(body))).err();
        (body_panic, future_drop_panic)
    };
    let ReaderState {
        request,
        mut remainder,
        terminal,
        notification,
        diag,
    } = state;
    let mut outcome = ReaderOutcome {
        started,
        error_order: remainder.batch.error_order,
        failure: remainder.batch.failure.take(),
        terminal,
        notification,
        diag,
        remainder,
        panics: [
            body_panic,
            future_drop_panic,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    };
    // Drop the real IO and each wake-capable control independently; a destructor
    // failure cannot unwind the already-retained first error or pending bytes.
    let ReaderRequest {
        remote_reader,
        back_tx,
        read_credit_rx,
        signal_tx,
        stop_rx,
        ..
    } = request;
    outcome.panics[2] = catch_unwind(AssertUnwindSafe(|| drop(remote_reader))).err();
    outcome.panics[3] = catch_unwind(AssertUnwindSafe(|| drop(back_tx))).err();
    outcome.panics[4] = catch_unwind(AssertUnwindSafe(|| drop(read_credit_rx))).err();
    outcome.panics[5] = catch_unwind(AssertUnwindSafe(|| drop(signal_tx))).err();
    outcome.panics[6] = catch_unwind(AssertUnwindSafe(|| drop(stop_rx))).err();
    let window_receiver = outcome
        .remainder
        .window
        .as_mut()
        .and_then(ReaderWindow::take_receiver);
    outcome.panics[7] = catch_unwind(AssertUnwindSafe(|| drop(window_receiver))).err();
    outcome.remainder.quarantined = outcome.panics.iter().any(Option::is_some)
        || matches!(
            outcome.terminal,
            ReaderTerminal::Interrupted | ReaderTerminal::AccountingFailed
        )
        || outcome.notification.in_flight;
    outcome
}

#[cfg(test)]
mod tests;

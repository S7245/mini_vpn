//! Uplink writer transaction: coalescing, partial writes, flush and half-close.
//!
//! One worker owns IO and command buffers. The legacy entry drops its result before
//! task completion; the custodied entry transfers buffers/errors into the root's
//! Joined/Draining role slot, never a second registry. IO never enters that outcome.
//! Root must retain the outcome before calling poll_discard, stop its producer and
//! supply bounded control cadence/deadline. Only actual receiver None signs disposal;
//! pending or interrupted close/poll cannot sign clean. Tests: tests/outcomes.rs.

use super::super::super::{
    RELAY_WRITER_STOP_TIMEOUT, RelayCommand, RelayWriterSignal, SocketHandle,
    coalesce_relay_payload,
};
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::task::{Context, Poll};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

#[derive(Debug, PartialEq, Eq)]
pub(super) enum WriterTerminal {
    Interrupted,
    Finished,
    Stopped,
    SignalClosed,
    ChannelClosed,
    IoFailed,
    ShutdownTimedOut,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum WriterOperation {
    Write,
    Flush,
    Shutdown,
}

#[derive(Debug)]
pub(super) struct WriterIoFailure {
    pub(super) operation: WriterOperation,
    pub(super) source: std::io::Error,
}

#[derive(Debug)]
pub(super) struct WriterResult {
    pub(super) terminal: WriterTerminal,
    pub(super) failure: Option<WriterIoFailure>,
    pub(super) shutdown_failure: Option<std::io::Error>,
}

impl WriterResult {
    fn terminal(terminal: WriterTerminal) -> Self {
        Self {
            terminal,
            failure: None,
            shutdown_failure: None,
        }
    }

    fn failed(operation: WriterOperation, source: std::io::Error) -> Self {
        Self {
            terminal: WriterTerminal::IoFailed,
            failure: Some(WriterIoFailure { operation, source }),
            shutdown_failure: None,
        }
    }
}

struct CurrentWrite {
    payload: Vec<u8>,
    written: usize,
    flushed: bool,
    finish_after: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CurrentWriteReceipt {
    pub(super) bytes: usize,
    pub(super) written: usize,
    pub(super) flushed: bool,
    pub(super) finish_after: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct CommandDiscardReceipt {
    pub(super) bytes: usize,
    pub(super) messages: usize,
    pub(super) finishes: usize,
}

impl CommandDiscardReceipt {
    fn with_command(self, command: &RelayCommand) -> Option<Self> {
        let (bytes, finishes) = match command {
            RelayCommand::Data(bytes) => (bytes.len(), 0),
            RelayCommand::Finish => (0, 1),
        };
        Some(Self {
            bytes: self.bytes.checked_add(bytes)?,
            messages: self.messages.checked_add(1)?,
            finishes: self.finishes.checked_add(finishes)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct WriterDiscardReceipt {
    pub(super) current: Option<CurrentWriteReceipt>,
    pub(super) deferred: CommandDiscardReceipt,
    pub(super) queued: CommandDiscardReceipt,
}

pub(super) struct WriterRemainder {
    current: Option<CurrentWrite>,
    deferred: Option<RelayCommand>,
    receiver: Option<mpsc::Receiver<RelayCommand>>,
    unaccounted: Option<RelayCommand>,
    receipt: WriterDiscardReceipt,
    closed: bool,
    complete: bool,
    interrupted: bool,
    quarantined: bool,
}

impl WriterRemainder {
    pub(super) fn quarantined(&self) -> bool {
        self.interrupted || self.quarantined
    }

    /// Borrow only after the root owns this outcome. The latch intentionally stays
    /// set on panic: receiver close/poll can invoke a user-supplied sender waker.
    /// The caller supplies a fixed settlement deadline; this method owns no timer.
    pub(super) fn poll_discard(
        &mut self,
        cx: &mut Context<'_>,
        budget: usize,
    ) -> Poll<WriterDiscardReceipt> {
        if self.quarantined() {
            return Poll::Pending;
        }
        if self.complete {
            return Poll::Ready(self.receipt);
        }
        if budget == 0 {
            return Poll::Pending;
        }
        self.interrupted = true;
        let result = self.discard_turn(cx, budget);
        self.interrupted = false;
        result
    }

    fn discard_turn(&mut self, cx: &mut Context<'_>, budget: usize) -> Poll<WriterDiscardReceipt> {
        let Some(receiver) = self.receiver.as_mut() else {
            self.quarantined = true;
            return Poll::Pending;
        };
        if !self.closed {
            receiver.close();
            self.closed = true;
        }
        if let Some(current) = self.current.as_ref() {
            if current.written > current.payload.len() {
                self.quarantined = true;
                return Poll::Pending;
            }
            self.receipt.current = Some(CurrentWriteReceipt {
                bytes: current.payload.len(),
                written: current.written,
                flushed: current.flushed,
                finish_after: current.finish_after,
            });
            self.current = None;
        }
        if let Some(command) = self.deferred.as_ref() {
            let Some(next) = self.receipt.deferred.with_command(command) else {
                self.quarantined = true;
                return Poll::Pending;
            };
            self.receipt.deferred = next;
            self.deferred = None;
        }
        for _ in 0..budget.min(receiver.max_capacity().saturating_add(1)) {
            match receiver.poll_recv(cx) {
                Poll::Ready(Some(command)) => {
                    let Some(next) = self.receipt.queued.with_command(&command) else {
                        self.unaccounted = Some(command);
                        self.quarantined = true;
                        return Poll::Pending;
                    };
                    self.receipt.queued = next;
                }
                Poll::Ready(None) => {
                    self.receiver = None;
                    self.complete = true;
                    return Poll::Ready(self.receipt);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Pending
    }
}

#[must_use]
pub(super) struct WriterOutcome {
    pub(super) started: bool,
    pub(super) error_order: [Option<usize>; 2],
    pub(super) panics: [Option<Box<dyn std::any::Any + Send>>; 4],
    pub(super) result: WriterResult,
    pub(super) remainder: WriterRemainder,
}

struct WriterState<W> {
    failure_clock: Option<super::supervision::FailureClock>,
    error_order: [Option<usize>; 2],
    writer: W,
    rx: mpsc::Receiver<RelayCommand>,
    current: Option<CurrentWrite>,
    deferred: Option<RelayCommand>,
    result: WriterResult,
}

impl<W> WriterState<W> {
    fn fail(&mut self, operation: WriterOperation, source: std::io::Error) {
        self.result = WriterResult::failed(operation, source);
        self.error_order[0] = super::supervision::stamp_failure(&self.failure_clock);
    }
}

async fn stop_writer<W: AsyncWrite + Unpin>(state: &mut WriterState<W>, terminal: WriterTerminal) {
    state.result = WriterResult::terminal(terminal);
    state.result.shutdown_failure = state.writer.shutdown().await.err();
    if state.result.shutdown_failure.is_some() {
        state.error_order[1] = super::supervision::stamp_failure(&state.failure_clock);
    }
}

async fn shutdown_relay_writer_after_finish<W>(
    handle: SocketHandle,
    state: &mut WriterState<W>,
    signal_tx: &mpsc::Sender<RelayWriterSignal>,
    stop_rx: &mut tokio::sync::oneshot::Receiver<()>,
) where
    W: AsyncWrite + Unpin,
{
    let shutdown_result = tokio::select! {
        _ = stop_rx => {
            stop_writer(state, WriterTerminal::Stopped).await;
            return;
        }
        result = tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, state.writer.shutdown()) => result,
    };
    let signal = match shutdown_result {
        Ok(Ok(_)) => {
            state.result = WriterResult::terminal(WriterTerminal::Finished);
            RelayWriterSignal::WriteHalfClosed {
                reason: "local_finish",
            }
        }
        Ok(Err(error)) => {
            state.fail(WriterOperation::Shutdown, error);
            println!(
                "关闭上游写半边失败 direction=local_to_remote handle={:?} err={:?}",
                handle, state.result.failure
            );
            RelayWriterSignal::Closed {
                direction: "local_to_remote",
                reason: "remote_shutdown_failed",
            }
        }
        Err(_) => {
            state.result = WriterResult::terminal(WriterTerminal::ShutdownTimedOut);
            println!(
                "关闭上游写半边超时 direction=local_to_remote handle={:?} timeout={}s",
                handle,
                RELAY_WRITER_STOP_TIMEOUT.as_secs()
            );
            RelayWriterSignal::Closed {
                direction: "local_to_remote",
                reason: "remote_shutdown_timeout",
            }
        }
    };
    let _ = signal_tx.send(signal).await;
}

async fn send_relay_writer_signal(
    signal_tx: &mpsc::Sender<RelayWriterSignal>,
    stop_rx: &mut tokio::sync::oneshot::Receiver<()>,
    signal: RelayWriterSignal,
) -> Result<(), WriterTerminal> {
    tokio::select! {
        _ = stop_rx => Err(WriterTerminal::Stopped),
        result = signal_tx.send(signal) => result.map_err(|_| WriterTerminal::SignalClosed),
    }
}

async fn write_payload<W: AsyncWrite + Unpin>(
    state: &mut WriterState<W>,
    signal_tx: &mpsc::Sender<RelayWriterSignal>,
    stop_rx: &mut tokio::sync::oneshot::Receiver<()>,
) -> bool {
    if let Err(terminal) =
        send_relay_writer_signal(signal_tx, stop_rx, RelayWriterSignal::WritePending).await
    {
        stop_writer(state, terminal).await;
        return false;
    }
    let Some(current) = state.current.as_mut() else {
        return false;
    };
    while current.written < current.payload.len() {
        let write_started = std::time::Instant::now();
        let result = tokio::select! {
            _ = &mut *stop_rx => {
                stop_writer(state, WriterTerminal::Stopped).await;
                return false;
            }
            result = state.writer.write(&current.payload[current.written..]) => result,
        };
        let write_wait = write_started.elapsed();
        let bytes = match result {
            Ok(0) => {
                state.fail(
                    WriterOperation::Write,
                    std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "upstream relay write returned zero bytes",
                    ),
                );
                return false;
            }
            Ok(bytes) if bytes <= current.payload.len() - current.written => bytes,
            Ok(_) => {
                state.fail(
                    WriterOperation::Write,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "writer overreported bytes",
                    ),
                );
                return false;
            }
            Err(error) => {
                state.fail(WriterOperation::Write, error);
                return false;
            }
        };
        current.written += bytes;
        if let Err(terminal) = send_relay_writer_signal(
            signal_tx,
            stop_rx,
            RelayWriterSignal::Progress { bytes, write_wait },
        )
        .await
        {
            stop_writer(state, terminal).await;
            return false;
        }
    }
    let result = tokio::select! {
        _ = &mut *stop_rx => {
            stop_writer(state, WriterTerminal::Stopped).await;
            return false;
        }
        result = state.writer.flush() => result,
    };
    if let Err(error) = result {
        state.fail(WriterOperation::Flush, error);
        return false;
    }
    current.flushed = true;
    if let Err(terminal) =
        send_relay_writer_signal(signal_tx, stop_rx, RelayWriterSignal::WriteCompleted).await
    {
        stop_writer(state, terminal).await;
        return false;
    }
    true
}

async fn run_writer_io<W: AsyncWrite + Unpin>(
    handle: SocketHandle,
    state: &mut WriterState<W>,
    signal_tx: &mpsc::Sender<RelayWriterSignal>,
    stop_rx: &mut tokio::sync::oneshot::Receiver<()>,
) {
    loop {
        let local_msg = if let Some(cmd) = state.deferred.take() {
            Some(cmd)
        } else {
            tokio::select! {
                _ = &mut *stop_rx => {
                    stop_writer(state, WriterTerminal::Stopped).await;
                    return;
                }
                local_msg = state.rx.recv() => local_msg,
            }
        };
        match local_msg {
            Some(RelayCommand::Data(payload)) => {
                let coalesced = coalesce_relay_payload(payload, &mut state.rx);
                state.deferred = coalesced.deferred;
                state.current = Some(CurrentWrite {
                    payload: coalesced.payload,
                    written: 0,
                    flushed: false,
                    finish_after: coalesced.finish_after,
                });
                if !write_payload(state, signal_tx, stop_rx).await {
                    if let Some(error) = state.result.failure.as_ref() {
                        println!(
                            "写入上游流失败 direction=local_to_remote handle={:?} attempted_bytes={} err={:?}",
                            handle,
                            state.current.as_ref().map_or(0, |c| c.payload.len()),
                            error.source
                        );
                        let _ = signal_tx
                            .send(RelayWriterSignal::Closed {
                                direction: "local_to_remote",
                                reason: "remote_write_failed",
                            })
                            .await;
                        state.result.shutdown_failure = state.writer.shutdown().await.err();
                        if state.result.shutdown_failure.is_some() {
                            state.error_order[1] =
                                super::supervision::stamp_failure(&state.failure_clock);
                        }
                    }
                    return;
                }
                if state.current.as_ref().is_some_and(|c| c.finish_after) {
                    shutdown_relay_writer_after_finish(handle, state, signal_tx, stop_rx).await;
                    return;
                }
                state.current = None;
            }
            Some(RelayCommand::Finish) => {
                shutdown_relay_writer_after_finish(handle, state, signal_tx, stop_rx).await;
                return;
            }
            None => {
                state.result = WriterResult::terminal(WriterTerminal::ChannelClosed);
                println!("本地房间 {:?} 已关闭通道", handle);
                let _ = signal_tx
                    .send(RelayWriterSignal::Closed {
                        direction: "local_to_remote",
                        reason: "local_channel_closed",
                    })
                    .await;
                state.result.shutdown_failure = state.writer.shutdown().await.err();
                if state.result.shutdown_failure.is_some() {
                    state.error_order[1] = super::supervision::stamp_failure(&state.failure_clock);
                }
                return;
            }
        }
    }
}

/// Legacy lifetime boundary: no receiver, payload or error remains in JoinHandle
/// output. Resume the original panic only after dropping the retained outcome.
pub(in crate::client_tun) async fn run_relay_writer<W>(
    handle: SocketHandle,
    writer: W,
    rx: mpsc::Receiver<RelayCommand>,
    signal_tx: mpsc::Sender<RelayWriterSignal>,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut outcome = run_custodied_relay_writer(handle, writer, rx, signal_tx, stop_rx).await;
    let panic = outcome.panics.iter_mut().find_map(Option::take);
    drop(outcome);
    if let Some(payload) = panic {
        resume_unwind(payload);
    }
}

pub(super) async fn run_custodied_relay_writer<W>(
    handle: SocketHandle,
    writer: W,
    rx: mpsc::Receiver<RelayCommand>,
    signal_tx: mpsc::Sender<RelayWriterSignal>,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
) -> WriterOutcome
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    run_custodied_relay_writer_gated(handle, writer, rx, signal_tx, stop_rx, None, None).await
}

pub(super) async fn run_custodied_relay_writer_gated<W>(
    handle: SocketHandle,
    writer: W,
    rx: mpsc::Receiver<RelayCommand>,
    signal_tx: mpsc::Sender<RelayWriterSignal>,
    mut stop_rx: tokio::sync::oneshot::Receiver<()>,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
    failure_clock: Option<super::supervision::FailureClock>,
) -> WriterOutcome
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut state = WriterState {
        failure_clock,
        error_order: [None; 2],
        writer,
        rx,
        current: None,
        deferred: None,
        result: WriterResult::terminal(WriterTerminal::Interrupted),
    };
    // Only a borrowed future is unwound. Original errors and all settled buffer
    // owners remain in state; cancellation/abort still requires root quarantine.
    let started = match gate {
        Some(gate) => gate.await.is_ok(),
        None => true,
    };
    let body_panic = if !started {
        state.result = WriterResult::terminal(WriterTerminal::Stopped);
        None
    } else {
        let mut body = std::pin::pin!(run_writer_io(handle, &mut state, &signal_tx, &mut stop_rx));
        poll_fn(
            |cx| match catch_unwind(AssertUnwindSafe(|| body.as_mut().poll(cx))) {
                Ok(Poll::Ready(())) => Poll::Ready(None),
                Ok(Poll::Pending) => Poll::Pending,
                Err(payload) => Poll::Ready(Some(payload)),
            },
        )
        .await
    };
    let WriterState {
        failure_clock: _,
        error_order,
        writer,
        rx,
        current,
        deferred,
        result,
    } = state;
    let mut outcome = WriterOutcome {
        started,
        error_order,
        result,
        panics: [body_panic, None, None, None],
        remainder: WriterRemainder {
            current,
            deferred,
            receiver: Some(rx),
            unaccounted: None,
            receipt: WriterDiscardReceipt::default(),
            closed: false,
            complete: false,
            interrupted: false,
            quarantined: false,
        },
    };
    // Resource destructors may wake other tasks. Keep the already-owned errors
    // and buffers while catching each destructor independently, with fixed slots.
    outcome.panics[1] = catch_unwind(AssertUnwindSafe(|| drop(writer))).err();
    outcome.panics[2] = catch_unwind(AssertUnwindSafe(|| drop(signal_tx))).err();
    outcome.panics[3] = catch_unwind(AssertUnwindSafe(|| drop(stop_rx))).err();
    outcome.remainder.interrupted = outcome.panics.iter().any(Option::is_some)
        || outcome.result.terminal == WriterTerminal::Interrupted;
    outcome
}

#[cfg(test)]
mod tests;

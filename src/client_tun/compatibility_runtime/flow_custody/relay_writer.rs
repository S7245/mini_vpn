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
use crate::shared::actor_callbacks::{SourceCallbacks, Lease, Guard, Custody, Field};
use std::ops::{Deref, DerefMut};

/// Original Roles cold ownership. A writer has a distinct callback role from
/// the reader; JOIN never substitutes for actual field/proxy retirement.
pub(super) struct Cold {
    lease: Option<Lease>,
    callbacks: Option<SourceCallbacks>,
}
impl Cold {
    pub(super) fn prepare(meter: &bytes::BackingMeter, clock: super::supervision::FailureClock) -> Result<Self, bytes::MeteredCopyError> {
        let callbacks = SourceCallbacks::prepare_source(meter, clock.clone())?;
        let lease = callbacks.acquire_source().map_err(|_| {
            clock.quarantine(); bytes::MeteredCopyError::Quarantined
        })?;
        Ok(Self { lease: Some(lease), callbacks: Some(callbacks) })
    }
    pub(super) fn unstarted(&self) -> bool { self.lease.is_some() && self.callbacks.is_some() }
    pub(super) fn take_lease(&mut self) -> Option<Lease> { self.lease.take() }
    pub(super) fn retire(&mut self) -> bool {
        if self.lease.is_some() || self.callbacks.as_ref().is_some_and(|c| !c.quiescent()) { return false; }
        drop(self.callbacks.take());
        true
    }
}
pub(super) struct WriterControl {
    clock: Option<super::supervision::FailureClock>,
    lease: Option<Lease>,
}
impl From<Option<super::supervision::FailureClock>> for WriterControl {
    fn from(clock: Option<super::supervision::FailureClock>) -> Self { Self { clock, lease: None } }
}
impl WriterControl {
    pub(super) fn native(clock: super::supervision::FailureClock, lease: Option<Lease>) -> Self {
        Self { clock: Some(clock), lease }
    }
}
/// Static field custody delegates each actual destructor to the shared guard.
/// It is not a second panic/notification authority or whole-result catcher.
pub(super) enum OwnedField<T> { Legacy(T), Native(Custody<T>) }
impl<T> OwnedField<T> {
    fn new(value: T, guard: Option<&Guard>, field: Field) -> Self {
        match guard { Some(guard) => Self::Native(guard.own(value, field)), None => Self::Legacy(value) }
    }
}
impl<T> Deref for OwnedField<T> { type Target = T;
    fn deref(&self) -> &T { match self { Self::Legacy(v) => v, Self::Native(v) => v } }
}
impl<T> DerefMut for OwnedField<T> {
    fn deref_mut(&mut self) -> &mut T { match self { Self::Legacy(v) => v, Self::Native(v) => v } }
}
impl<T: std::fmt::Debug> std::fmt::Debug for OwnedField<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { (**self).fmt(f) }
}
impl<T> OwnedField<Option<T>> {
    fn protect(&mut self, guard: &Guard, field: Field) {
        let old = std::mem::replace(self, Self::Legacy(None));
        *self = match old { Self::Legacy(value) => Self::Native(guard.own(value, field)),
            native => { guard.quarantine(); native } };
    }
    fn retire(&mut self) { drop(std::mem::replace(self, Self::Legacy(None))); }
}
impl<W: AsyncWrite + Unpin> AsyncWrite for OwnedField<W> {
    fn poll_write(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut **self).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut **self).poll_flush(cx)
    }
    fn poll_shutdown(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut **self).poll_shutdown(cx)
    }
}

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
    Coalesce,
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
    pub(super) failure: OwnedField<Option<WriterIoFailure>>,
    pub(super) shutdown_failure: OwnedField<Option<std::io::Error>>,
}

impl WriterResult {
    fn terminal(terminal: WriterTerminal) -> Self {
        Self {
            terminal,
            failure: OwnedField::Legacy(None),
            shutdown_failure: OwnedField::Legacy(None),
        }
    }

    fn reset(&mut self, terminal: WriterTerminal) {
        self.terminal = terminal;
        *self.failure = None;
        *self.shutdown_failure = None;
    }
    fn retire_fields(&mut self) -> bool {
        if self.failure.is_some() || self.shutdown_failure.is_some() { return false; }
        self.failure.retire();
        self.shutdown_failure.retire();
        true
    }
}

struct CurrentWrite {
    payload: crate::client_tun::uplink::Payload,
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
            RelayCommand::NativeData(bytes) => (bytes.len(), 0),
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
    lease: Option<Lease>,
    guard: Option<Guard>,
    current: Option<CurrentWrite>,
    deferred: Option<RelayCommand>,
    receiver: Option<crate::client_tun::uplink::Receiver>,
    unaccounted: Option<RelayCommand>,
    receipt: WriterDiscardReceipt,
    closed: bool,
    complete: bool,
    interrupted: bool,
    quarantined: bool,
}

impl Drop for WriterRemainder {
    fn drop(&mut self) {
        if let Some(guard) = &self.guard {
            if !self.complete || self.quarantined() || guard.failed() {
                guard.quarantine();
                std::mem::forget(self.current.take());
                std::mem::forget(self.deferred.take());
                std::mem::forget(self.receiver.take());
                std::mem::forget(self.unaccounted.take());
            }
        }
    }
}

impl WriterRemainder {
    pub(super) fn quarantined(&self) -> bool {
        self.interrupted || self.quarantined || self.guard.as_ref().is_some_and(Guard::failed)
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
        if self.guard.is_some() && self.lease.is_none() {
            if let Some(guard) = &self.guard { guard.quarantine(); }
            self.quarantined = true;
            return Poll::Pending;
        }
        self.interrupted = true;
        if let Some(mut lease) = self.lease.take() {
            // This borrowed turn owns no opaque fields. Its actual Work Drop
            // precedes restoring the same lease; the receiver stores only proxy
            // Wakers even when a previously granted Permit outlives close.
            let result = {
                let mut body = Some(poll_fn(|safe| self.discard_turn(safe, budget)));
                let result = match lease.run(&mut body) {
                    Ok(work) => {
                        let mut work = std::pin::pin!(work);
                        Some(work.as_mut().poll(cx))
                    }
                    Err(_) => None,
                };
                drop(body);
                result
            };
            self.lease = Some(lease);
            match result {
                Some(Poll::Ready(Ok(receipt))) => { self.interrupted = false; Poll::Ready(receipt) }
                Some(Poll::Pending) => { self.interrupted = false; Poll::Pending }
                Some(Poll::Ready(Err(_))) | None => { self.quarantined = true; Poll::Pending }
            }
        } else {
            let result = self.discard_turn(cx, budget);
            self.interrupted = false;
            result
        }
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
    writer: OwnedField<W>,
    remainder: WriterRemainder,
    result: WriterResult,
}

impl<W> WriterState<W> {
    fn fail(&mut self, operation: WriterOperation, source: std::io::Error) {
        self.result.reset(WriterTerminal::IoFailed);
        *self.result.failure = Some(WriterIoFailure { operation, source });
        self.error_order[0] = self
            .failure_clock
            .as_ref()
            .zip(self.result.failure.as_ref())
            .and_then(|(clock, failure)| record_io(clock, &failure.source));
    }
}

// Keep the original typed failure in WriterResult; Q publishes through the
// original clock and never creates a replacement source-IO ordinal.
fn record_io(clock: &super::supervision::FailureClock, error: &std::io::Error) -> Option<usize> {
    if error.get_ref().and_then(|source| source.downcast_ref::<quinn::owned_events::AdmissionError>())
        .is_some_and(|cause| cause.0 == bytes::MeteredCopyError::Quarantined) {
        clock.quarantine(); None
    } else { Some(clock.io(error)) }
}

// A resumed opaque operation must recheck the original Native budget. This
// monomorphized gate adds no allocation or lock to DATA; Legacy remains allowed.
fn poll_io<T>(
    clock: &Option<super::supervision::FailureClock>,
    cx: &mut Context<'_>,
    operation: impl FnOnce(&mut Context<'_>) -> Poll<std::io::Result<T>>,
) -> Poll<Option<std::io::Result<T>>> {
    if clock.as_ref().is_some_and(|clock| !clock.work_allowed()) {
        Poll::Ready(None)
    } else {
        operation(cx).map(Some)
    }
}
async fn poll_shutdown<W: AsyncWrite + Unpin>(
    state: &mut WriterState<W>,
) -> Option<std::io::Result<()>> {
    let result = poll_fn(|cx| {
        poll_io(&state.failure_clock, cx, |cx| {
            std::pin::Pin::new(&mut state.writer).poll_shutdown(cx)
        })
    })
    .await;
    if result.is_none() {
        state.result.terminal = WriterTerminal::Interrupted;
    }
    result
}

async fn stop_writer<W: AsyncWrite + Unpin>(state: &mut WriterState<W>, terminal: WriterTerminal) {
    state.result.reset(terminal);
    let Some(result) = poll_shutdown(state).await else {
        return;
    };
    *state.result.shutdown_failure = result.err();
    if state.result.shutdown_failure.is_some() {
        state.error_order[1] = state
            .failure_clock
            .as_ref()
            .zip(state.result.shutdown_failure.as_ref())
            .and_then(|(clock, error)| record_io(clock, error));
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
        result = tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, poll_shutdown(state)) => result,
    };
    let signal = match shutdown_result {
        Ok(None) => return,
        Ok(Some(Ok(_))) => {
            state.result.reset(WriterTerminal::Finished);
            RelayWriterSignal::WriteHalfClosed {
                reason: "local_finish",
            }
        }
        Ok(Some(Err(error))) => {
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
            state.result.reset(WriterTerminal::ShutdownTimedOut);
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
    let Some(current) = state.remainder.current.as_mut() else {
        return false;
    };
    while current.written < current.payload.len() {
        let write_started = std::time::Instant::now();
        let result = tokio::select! {
            _ = &mut *stop_rx => {
                stop_writer(state, WriterTerminal::Stopped).await;
                return false;
            }
            result = poll_fn(|cx| poll_io(&state.failure_clock, cx,
                |cx| Pin::new(&mut state.writer).poll_write(cx, &current.payload[current.written..]))) => result,
        };
        let Some(result) = result else {
            state.result.reset(WriterTerminal::Interrupted);
            return false;
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
        result = poll_fn(|cx| poll_io(&state.failure_clock, cx,
            |cx| Pin::new(&mut state.writer).poll_flush(cx))) => result,
    };
    let Some(result) = result else {
        state.result.terminal = WriterTerminal::Interrupted;
        return false;
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
        let local_msg = if let Some(cmd) = state.remainder.deferred.take() {
            Some(cmd)
        } else {
            let Some(receiver) = state.remainder.receiver.as_mut() else {
                state.result.terminal = WriterTerminal::Interrupted;
                return;
            };
            tokio::select! {
                _ = &mut *stop_rx => {
                    stop_writer(state, WriterTerminal::Stopped).await;
                    return;
                }
                local_msg = receiver.recv() => local_msg,
            }
        };
        match local_msg {
            Some(command @ (RelayCommand::Data(_) | RelayCommand::NativeData(_))) => {
                let payload = crate::client_tun::uplink::Payload::from_command(command);
                let Some(receiver) = state.remainder.receiver.as_mut() else {
                    state.remainder.unaccounted = Some(payload.into_command());
                    state.result.terminal = WriterTerminal::Interrupted;
                    return;
                };
                let coalesced = coalesce_relay_payload(payload, receiver);
                state.remainder.deferred = coalesced.deferred;
                state.remainder.current = Some(CurrentWrite {
                    payload: coalesced.payload,
                    written: 0,
                    flushed: false,
                    finish_after: coalesced.finish_after,
                });
                let wrote = if let Some(error) = coalesced.failure {
                    if error == bytes::MeteredCopyError::Quarantined {
                        if let Some(clock) = &state.failure_clock { clock.quarantine(); }
                        state.result.terminal = WriterTerminal::Interrupted;
                    } else { state.fail(WriterOperation::Coalesce, crate::client_tun::uplink::admission_error(error)); }
                    false
                } else { write_payload(state, signal_tx, stop_rx).await };
                if !wrote {
                    if let Some(error) = state.result.failure.as_ref() {
                        println!(
                            "写入上游流失败 direction=local_to_remote handle={:?} attempted_bytes={} err={:?}",
                            handle,
                            state.remainder.current.as_ref().map_or(0, |c| c.payload.len()),
                            error.source
                        );
                        let _ = signal_tx
                            .send(RelayWriterSignal::Closed {
                                direction: "local_to_remote",
                                reason: "remote_write_failed",
                            })
                            .await;
                        let Some(result) = poll_shutdown(state).await else {
                            return;
                        };
                        *state.result.shutdown_failure = result.err();
                        if state.result.shutdown_failure.is_some() {
                            state.error_order[1] = state
                                .failure_clock
                                .as_ref()
                                .zip(state.result.shutdown_failure.as_ref())
                                .and_then(|(clock, error)| record_io(clock, error));
                        }
                    }
                    return;
                }
                if state.remainder.current.as_ref().is_some_and(|c| c.finish_after) {
                    shutdown_relay_writer_after_finish(handle, state, signal_tx, stop_rx).await;
                    return;
                }
                state.remainder.current = None;
            }
            Some(RelayCommand::Finish) => {
                shutdown_relay_writer_after_finish(handle, state, signal_tx, stop_rx).await;
                return;
            }
            None => {
                state.result.reset(WriterTerminal::ChannelClosed);
                println!("本地房间 {:?} 已关闭通道", handle);
                let _ = signal_tx
                    .send(RelayWriterSignal::Closed {
                        direction: "local_to_remote",
                        reason: "local_channel_closed",
                    })
                    .await;
                let Some(result) = poll_shutdown(state).await else {
                    return;
                };
                *state.result.shutdown_failure = result.err();
                if state.result.shutdown_failure.is_some() {
                    state.error_order[1] = state
                        .failure_clock
                        .as_ref()
                        .zip(state.result.shutdown_failure.as_ref())
                        .and_then(|(clock, error)| record_io(clock, error));
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
    rx: crate::client_tun::uplink::Receiver,
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
    rx: crate::client_tun::uplink::Receiver,
    signal_tx: mpsc::Sender<RelayWriterSignal>,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
) -> WriterOutcome
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    run_custodied_relay_writer_gated(handle, writer, rx, signal_tx, stop_rx, None, None).await
}

/// Synchronous ownership transfer: all opaque fields enter independent custody
/// before the returned actor can be cancelled, including before its first poll.
pub(super) fn run_custodied_relay_writer_gated<W>(
    handle: SocketHandle,
    writer: W,
    rx: crate::client_tun::uplink::Receiver,
    signal_tx: mpsc::Sender<RelayWriterSignal>,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
    control: impl Into<WriterControl>,
) -> impl Future<Output = WriterOutcome>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    let WriterControl { clock: failure_clock, mut lease } = control.into();
    let guard = lease.as_ref().map(Lease::guard);
    let writer = OwnedField::new(writer, guard.as_ref(), Field::Source0);
    let signal_tx = OwnedField::new(signal_tx, guard.as_ref(), Field::Source1);
    let mut stop_rx = OwnedField::new(stop_rx, guard.as_ref(), Field::Source2);
    let mut gate = OwnedField::new(gate, guard.as_ref(), Field::Source3);
    let mut result = WriterResult::terminal(WriterTerminal::Interrupted);
    if let Some(guard) = &guard {
        result.failure.protect(guard, Field::Source4);
        result.shutdown_failure.protect(guard, Field::Source5);
    }
    let panic_clock = failure_clock.clone();
    let mut state = WriterState {
        failure_clock, error_order: [None; 2], writer, result,
        remainder: WriterRemainder {
            lease: None, guard, current: None, deferred: None, receiver: Some(rx), unaccounted: None,
            receipt: WriterDiscardReceipt::default(), closed: false, complete: false,
            interrupted: false, quarantined: false,
        },
    };
    async move {
        let mut started = false;
        let mut body_panic = None;
        if let Some(lease) = &mut lease {
            let mut body = Some(async {
                started = match gate.as_mut() { Some(gate) => gate.await.is_ok(), None => true };
                gate.retire();
                if started { run_writer_io(handle, &mut state, &signal_tx, &mut stop_rx).await; }
                else { state.result.reset(WriterTerminal::Stopped); }
            });
            let complete = match lease.run(&mut body) {
                Ok(work) => work.await.is_ok(), Err(_) => false,
            };
            drop(body);
            if !complete { state.remainder.interrupted = true; }
        } else {
            // Preserve Legacy's original gate boundary outside its IO catcher.
            started = match gate.as_mut() { Some(gate) => gate.await.is_ok(), None => true };
            gate.retire();
            if started {
                let mut body = std::pin::pin!(run_writer_io(handle, &mut state, &signal_tx, &mut stop_rx));
                body_panic = poll_fn(|cx| match catch_unwind(AssertUnwindSafe(|| body.as_mut().poll(cx))) {
                    Ok(Poll::Ready(())) => Poll::Ready(None), Ok(Poll::Pending) => Poll::Pending,
                    Err(payload) => {
                        if let Some(clock) = panic_clock.as_ref().filter(|clock| clock.is_bound()) { clock.panic(); }
                        Poll::Ready(Some(payload))
                    }
                }).await;
            } else { state.result.reset(WriterTerminal::Stopped); }
        }
        state.remainder.lease = lease; // The same role also owns close-tail notifications.
        let WriterState { failure_clock: _, error_order, writer, remainder, result } = state;
        let mut outcome = WriterOutcome {
            started, error_order, result, remainder, panics: [body_panic, None, None, None],
        };
        // Native Custody captures each original panic in its independent fixed
        // field. Legacy keeps the original four outcome slots and behavior.
        outcome.panics[1] = catch_unwind(AssertUnwindSafe(|| drop(writer))).err();
        if outcome.panics[1].is_some() {
            if let Some(clock) = panic_clock.as_ref().filter(|clock| clock.is_bound()) { clock.panic(); }
        }
        outcome.panics[2] = catch_unwind(AssertUnwindSafe(|| drop(signal_tx))).err();
        if outcome.panics[2].is_some() {
            if let Some(clock) = panic_clock.as_ref().filter(|clock| clock.is_bound()) { clock.panic(); }
        }
        outcome.panics[3] = catch_unwind(AssertUnwindSafe(|| drop(stop_rx))).err();
        if outcome.panics[3].is_some() {
            if let Some(clock) = panic_clock.as_ref().filter(|clock| clock.is_bound()) { clock.panic(); }
        }
        outcome.remainder.interrupted |= outcome.panics.iter().any(Option::is_some)
            || outcome.result.terminal == WriterTerminal::Interrupted
            || outcome.remainder.guard.as_ref().is_some_and(Guard::failed);
        outcome
    }
}
impl WriterOutcome {
    // Cold release proof after each original field and callback has retired.
    pub(super) fn release_ready(&self) -> bool {
        let r = &self.remainder;
        r.complete && !r.quarantined() && r.current.is_none() && r.deferred.is_none()
            && r.receiver.is_none() && r.unaccounted.is_none() && r.lease.is_none() && r.guard.is_none()
            && matches!(self.result.failure, OwnedField::Legacy(None))
            && matches!(self.result.shutdown_failure, OwnedField::Legacy(None))
            && self.panics.iter().all(Option::is_none)
    }
    pub(super) fn retire_fields(&mut self) -> bool {
        if !self.remainder.complete || self.remainder.quarantined()
            || self.result.failure.is_some() || self.result.shutdown_failure.is_some() { return false; }
        drop(self.remainder.lease.take());
        if self.remainder.quarantined() || !self.result.retire_fields() { return false; }
        drop(self.remainder.guard.take());
        true
    }
}

#[cfg(test)]
mod tests;

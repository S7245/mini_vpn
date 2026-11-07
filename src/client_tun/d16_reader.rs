//! Original D16 direct Native reader and queue reservation transaction.
//! Owns the read loop, reservation and readiness events; it does not own the
//! relay writer, physical SocketCtx, global budget, or Root JoinHandle.
//! Entry `run_d16_native_reader`; characterization tests live in client_tun/tests.

use super::compatibility_runtime::{FailureClock, ReaderWindow, stamp_failure};
use super::*;
use crate::tcp_downlink_pump::ByteQueueReadReservation;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::Poll;

mod legacy;
pub(super) use legacy::run_relay_d16;
use legacy::send_d16_data_ready;

/// Joined reader result kept by the Root until the queue and original backing
/// have one physical disposition. A rejected backing must remain in this value.
pub(in crate::client_tun) struct D16ReaderOutcome {
    pub(in crate::client_tun) started: bool,
    pub(in crate::client_tun) reason: RelayCloseReason,
    pub(in crate::client_tun) error: Option<std::io::Error>,
    pub(in crate::client_tun) queue_error: Option<ByteQueuePushError>,
    pub(in crate::client_tun) retained: Option<Bytes>,
    pub(in crate::client_tun) error_order: Option<usize>,
    pub(in crate::client_tun) window: Option<ReaderWindow>,
    pub(in crate::client_tun) body_panic: Option<Box<dyn std::any::Any + Send>>,
    pub(in crate::client_tun) future_drop_panic: Option<Box<dyn std::any::Any + Send>>,
    pub(in crate::client_tun) panic_order: Option<usize>,
    pub(in crate::client_tun) remainder: Option<D16ReaderCall>,
    pub(in crate::client_tun) disposed_read: Option<usize>,
    pub(in crate::client_tun) queued_bytes: usize,
    pub(in crate::client_tun) ready_sent: usize,
    pub(in crate::client_tun) closed_sent: usize,
    pub(in crate::client_tun) counter_failed: bool,
}

/// One Root-issued window and failure clock. Legacy D16 has neither.
pub(in crate::client_tun) struct D16ReaderAccounting {
    clock: FailureClock,
    window: ReaderWindow,
    backing_meter: Option<bytes::BackingMeter>,
}

impl D16ReaderAccounting {
    pub(in crate::client_tun) fn new(
        clock: FailureClock,
        window: ReaderWindow,
        backing_meter: bytes::BackingMeter,
    ) -> Self {
        Self {
            clock,
            window,
            backing_meter: Some(backing_meter),
        }
    }
}

pub(in crate::client_tun) struct D16ReaderCall {
    handle: SocketHandle,
    epoch: u64,
    remote_reader: NativeTcpReadHalf,
    queue: AsyncLeasedByteFlowQueue,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    activity_tx: Option<mpsc::Sender<()>>,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
    reservation: Option<ByteQueueReadReservation>,
    queued_bytes: usize,
    ready_sent: usize,
    closed_sent: usize,
    counter_failed: bool,
}

impl D16ReaderOutcome {
    fn reason(direction: &'static str, reason: &'static str) -> Self {
        Self {
            started: true,
            reason: (direction, reason),
            error: None,
            queue_error: None,
            retained: None,
            error_order: None,
            window: None,
            body_panic: None,
            future_drop_panic: None,
            panic_order: None,
            remainder: None,
            disposed_read: None,
            queued_bytes: 0,
            ready_sent: 0,
            closed_sent: 0,
            counter_failed: false,
        }
    }

    fn ordered(direction: &'static str, reason: &'static str, error_order: Option<usize>) -> Self {
        let mut outcome = Self::reason(direction, reason);
        outcome.error_order = error_order;
        outcome
    }
}

impl D16ReaderOutcome {
    /// Drop the original direct receive half only after the Root has joined
    /// the reader and the physical owner has proved every queued byte. A
    /// panicking destructor remains sticky; its original panic payload stays
    /// with this outcome and clean settlement is forbidden.
    pub(in crate::client_tun) fn dispose_clean(&mut self) -> bool {
        if self.disposed_read.is_some() {
            return true;
        }
        if !self.started
            || self.counter_failed
            || self.error_order.is_some()
            || self.error.is_some()
            || self.queue_error.is_some()
            || self.retained.is_some()
            || self.body_panic.is_some()
            || self.future_drop_panic.is_some()
            || self.remainder.is_none()
            || self.window.as_ref().is_none_or(ReaderWindow::failed)
        {
            return false;
        }
        let read = self.window.as_ref().map_or(0, ReaderWindow::read);
        let remainder = self.remainder.take();
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(remainder)));
        if let Err(payload) = dropped {
            self.body_panic = Some(payload);
            return false;
        }
        let window = self.window.take();
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(window))) {
            self.future_drop_panic = Some(payload);
            return false;
        }
        self.disposed_read = Some(read);
        true
    }
}

/// Root start gate: a failed publication must return the original receive
/// half, reservation authority and window without polling the source.
pub(in crate::client_tun) async fn run_d16_native_reader_gated(
    handle: SocketHandle,
    epoch: u64,
    remote_reader: NativeTcpReadHalf,
    queue: AsyncLeasedByteFlowQueue,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    activity_tx: Option<mpsc::Sender<()>>,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
    accounting: D16ReaderAccounting,
    gate: tokio::sync::oneshot::Receiver<()>,
) -> D16ReaderOutcome {
    if gate.await.is_err() {
        let mut outcome = D16ReaderOutcome::reason("root", "gate_cancelled");
        outcome.started = false;
        outcome.window = Some(accounting.window);
        outcome.remainder = Some(D16ReaderCall {
            handle,
            epoch,
            remote_reader,
            queue,
            read_credit_rx,
            back_tx,
            activity_tx,
            stop_rx,
            reservation: None,
            queued_bytes: 0,
            ready_sent: 0,
            closed_sent: 0,
            counter_failed: false,
        });
        return outcome;
    }
    run_d16_native_reader(
        handle,
        epoch,
        remote_reader,
        queue,
        read_credit_rx,
        back_tx,
        activity_tx,
        stop_rx,
        Some(accounting),
    )
    .await
}

fn take_pending_failure(pending: &mut Option<D16ReaderOutcome>) -> D16ReaderOutcome {
    pending
        .take()
        .unwrap_or_else(|| D16ReaderOutcome::reason("root", "reader_failure_lost"))
}

async fn send_ready_or_stop(
    handle: SocketHandle,
    epoch: u64,
    back_tx: &crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    stop_rx: &mut tokio::sync::oneshot::Receiver<()>,
    ready_sent: &mut usize,
    counter_failed: &mut bool,
) -> Option<bool> {
    let result = tokio::select! {
        biased;
        _ = stop_rx => None,
        sent = send_d16_data_ready(handle, epoch, back_tx) => Some(sent),
    };
    if result == Some(true) {
        if let Some(next) = ready_sent.checked_add(1) {
            *ready_sent = next;
        } else {
            *counter_failed = true;
        }
    }
    result
}

async fn send_closed_or_stop(
    handle: SocketHandle,
    epoch: u64,
    direction: &'static str,
    reason: &'static str,
    back_tx: &crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    stop_rx: &mut tokio::sync::oneshot::Receiver<()>,
    closed_sent: &mut usize,
    counter_failed: &mut bool,
) {
    let event = RelayEvent::Closed(RelayClose {
        epoch,
        direction,
        reason,
    });
    let sent = tokio::select! {
        biased;
        _ = stop_rx => false,
        result = back_tx.send((handle, event)) => result.is_ok(),
    };
    if sent {
        if let Some(next) = closed_sent.checked_add(1) {
            *closed_sent = next;
        } else {
            *counter_failed = true;
        }
    }
}

pub(in crate::client_tun) async fn run_d16_native_reader(
    handle: SocketHandle,
    epoch: u64,
    remote_reader: NativeTcpReadHalf,
    queue: AsyncLeasedByteFlowQueue,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    activity_tx: Option<mpsc::Sender<()>>,
    stop_rx: tokio::sync::oneshot::Receiver<()>,
    mut accounting: Option<D16ReaderAccounting>,
) -> D16ReaderOutcome {
    let mut call = D16ReaderCall {
        handle,
        epoch,
        remote_reader,
        queue,
        read_credit_rx,
        back_tx,
        activity_tx,
        stop_rx,
        reservation: None,
        queued_bytes: 0,
        ready_sent: 0,
        closed_sent: 0,
        counter_failed: false,
    };
    let mut completed = None;
    let mut pending_failure = None;
    let panic_clock = accounting
        .as_ref()
        .map(|accounting| accounting.clock.clone());
    let mut panic_order = None;
    let mut body = Box::pin(run_d16_native_reader_body(
        &mut call,
        accounting.as_mut(),
        &mut pending_failure,
    ));
    let body_panic = std::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| body.as_mut().poll(cx))) {
            Ok(Poll::Ready(outcome)) => {
                completed = Some(outcome);
                Poll::Ready(None)
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                panic_order = stamp_failure(&panic_clock);
                Poll::Ready(Some(payload))
            }
        }
    })
    .await;
    let future_drop_panic = catch_unwind(AssertUnwindSafe(|| drop(body))).err();
    if future_drop_panic.is_some() && panic_order.is_none() {
        panic_order = stamp_failure(&panic_clock);
    }
    let mut outcome = completed
        .or(pending_failure)
        .unwrap_or_else(|| D16ReaderOutcome::reason("root", "reader_panicked"));
    outcome.panic_order = panic_order;
    outcome.body_panic = body_panic;
    outcome.future_drop_panic = future_drop_panic;
    outcome.queued_bytes = call.queued_bytes;
    outcome.ready_sent = call.ready_sent;
    outcome.closed_sent = call.closed_sent;
    outcome.counter_failed = call.counter_failed;
    outcome.window = accounting.map(|accounting| accounting.window);
    if outcome.window.is_some() {
        outcome.remainder = Some(call);
    }
    outcome
}

async fn run_d16_native_reader_body(
    call: &mut D16ReaderCall,
    mut accounting: Option<&mut D16ReaderAccounting>,
    pending_failure: &mut Option<D16ReaderOutcome>,
) -> D16ReaderOutcome {
    let D16ReaderCall {
        handle,
        epoch,
        remote_reader,
        queue,
        read_credit_rx,
        back_tx,
        activity_tx,
        stop_rx,
        reservation: held_reservation,
        queued_bytes,
        ready_sent,
        closed_sent,
        counter_failed,
    } = call;
    let handle = *handle;
    let epoch = *epoch;
    let mut remote_reader = remote_reader;
    let mut stop_rx = stop_rx;
    let failure_clock = accounting
        .as_ref()
        .map(|accounting| accounting.clock.clone());
    let mut read_credit = *read_credit_rx.borrow_and_update();
    'reader: loop {
        if read_credit.paused || read_credit.max_batch_bytes == 0 {
            let update = tokio::select! {
                biased;
                _ = &mut stop_rx => {
                    queue
                        .close_for_terminal_and_mark_ready("local_to_remote", "reader_stopped")
                        .await;
                    return D16ReaderOutcome::reason("local_to_remote", "reader_stopped");
                }
                update = read_credit_rx.changed() => update,
            };
            if update.is_err() {
                let error_order = stamp_failure(&failure_clock);
                *pending_failure = Some(D16ReaderOutcome::ordered(
                    "local_to_remote",
                    "read_credit_closed",
                    error_order,
                ));
                queue
                    .close_for_terminal_and_mark_ready("local_to_remote", "read_credit_closed")
                    .await;
                return take_pending_failure(pending_failure);
            }
            read_credit = *read_credit_rx.borrow_and_update();
            continue;
        }

        let window_limit = if let Some(accounting) = accounting.as_mut() {
            match accounting.window.remaining() {
                None => {
                    let error_order = stamp_failure(&failure_clock);
                    *pending_failure = Some(D16ReaderOutcome::ordered(
                        "root",
                        "receive_window_failed",
                        error_order,
                    ));
                    queue
                        .close_for_terminal_and_mark_ready("root", "receive_window_failed")
                        .await;
                    return take_pending_failure(pending_failure);
                }
                Some(0) if accounting.window.stopped() => {
                    let error_order = stamp_failure(&failure_clock);
                    *pending_failure = Some(D16ReaderOutcome::ordered(
                        "root",
                        "receive_window_stopped",
                        error_order,
                    ));
                    queue
                        .close_for_terminal_and_mark_ready("root", "receive_window_stopped")
                        .await;
                    return take_pending_failure(pending_failure);
                }
                Some(0) => {
                    tokio::select! {
                        biased;
                        _ = &mut stop_rx => {
                            queue.close_for_terminal_and_mark_ready(
                                "local_to_remote", "reader_stopped",
                            ).await;
                            return D16ReaderOutcome::reason("local_to_remote", "reader_stopped");
                        }
                        update = read_credit_rx.changed() => {
                            if update.is_err() {
                                let error_order = stamp_failure(&failure_clock);
                                *pending_failure = Some(D16ReaderOutcome::ordered(
                                    "local_to_remote", "read_credit_closed", error_order,
                                ));
                                queue.close_for_terminal_and_mark_ready(
                                    "local_to_remote", "read_credit_closed",
                                ).await;
                                return take_pending_failure(pending_failure);
                            }
                            read_credit = *read_credit_rx.borrow_and_update();
                        }
                        changed = accounting.window.changed() => {
                            if !changed {
                                let error_order = stamp_failure(&failure_clock);
                                *pending_failure = Some(D16ReaderOutcome::ordered(
                                    "root", "receive_window_closed", error_order,
                                ));
                                queue.close_for_terminal_and_mark_ready(
                                    "root", "receive_window_closed",
                                ).await;
                                return take_pending_failure(pending_failure);
                            }
                        }
                    }
                    continue;
                }
                Some(remaining) => remaining,
            }
        } else {
            usize::MAX
        };

        let reservation = tokio::select! {
            biased;
            _ = &mut stop_rx => {
                queue
                    .close_for_terminal_and_mark_ready("local_to_remote", "reader_stopped")
                    .await;
                return D16ReaderOutcome::reason("local_to_remote", "reader_stopped");
            }
            update = read_credit_rx.changed() => {
                if update.is_err() {
                    let error_order = stamp_failure(&failure_clock);
                    *pending_failure = Some(D16ReaderOutcome::ordered(
                        "local_to_remote", "read_credit_closed", error_order,
                    ));
                    queue
                        .close_for_terminal_and_mark_ready(
                            "local_to_remote",
                            "read_credit_closed",
                        )
                        .await;
                    return take_pending_failure(pending_failure);
                }
                read_credit = *read_credit_rx.borrow_and_update();
                continue;
            }
            reservation = queue.reserve_read_up_to(read_credit.max_batch_bytes.min(window_limit)) => {
                match reservation {
                    Ok(reservation) => reservation,
                    Err(_) => {
                        match queue.snapshot_now().closure {
                            crate::tcp_downlink_pump::LeasedByteQueueClosure::Terminal(cause) => {
                                return D16ReaderOutcome::reason(cause.direction, cause.reason);
                            }
                            crate::tcp_downlink_pump::LeasedByteQueueClosure::RemoteEof => {
                                return D16ReaderOutcome::reason("remote_to_local", "remote_eof");
                            }
                            crate::tcp_downlink_pump::LeasedByteQueueClosure::Open => {}
                        }
                        let mut outcome = D16ReaderOutcome::reason("root", "queue_reservation_failed");
                        outcome.error_order = stamp_failure(&failure_clock);
                        return outcome;
                    }
                }
            }
        };
        let max_len = reservation.max_len();
        *held_reservation = Some(reservation);
        if max_len == 0 {
            drop(held_reservation.take());
            continue;
        }

        let chunk = loop {
            let result = tokio::select! {
                biased;
                _ = &mut stop_rx => {
                    drop(held_reservation.take());
                    queue
                        .close_for_terminal_and_mark_ready("local_to_remote", "reader_stopped")
                        .await;
                    return D16ReaderOutcome::reason("local_to_remote", "reader_stopped");
                }
                update = read_credit_rx.changed() => {
                    if update.is_err() {
                        let error_order = stamp_failure(&failure_clock);
                        *pending_failure = Some(D16ReaderOutcome::ordered(
                            "local_to_remote", "read_credit_closed", error_order,
                        ));
                        drop(held_reservation.take());
                        queue
                            .close_for_terminal_and_mark_ready(
                                "local_to_remote",
                                "read_credit_closed",
                            )
                            .await;
                        return take_pending_failure(pending_failure);
                    }
                    read_credit = *read_credit_rx.borrow_and_update();
                    if read_credit.paused || read_credit.max_batch_bytes == 0 {
                        drop(held_reservation.take());
                        continue 'reader;
                    }
                    continue;
                }
                chunk = poll_native_tcp_chunk(&mut remote_reader, max_len) => chunk,
            };
            break result;
        };
        match chunk {
            Ok(Some(chunk)) => {
                if accounting
                    .as_ref()
                    .and_then(|accounting| accounting.backing_meter.as_ref())
                    .is_some_and(|expected| {
                        !chunk
                            .bytes
                            .backing_meter()
                            .as_ref()
                            .is_some_and(|meter| meter.same_authority(expected))
                    })
                {
                    let error_order = stamp_failure(&failure_clock);
                    if let Some(expected) = accounting
                        .as_ref()
                        .and_then(|accounting| accounting.backing_meter.as_ref())
                    {
                        expected.quarantine();
                    }
                    if let Some(actual) = chunk.bytes.backing_meter() {
                        actual.quarantine();
                    }
                    *pending_failure = Some(D16ReaderOutcome {
                        retained: Some(chunk.bytes),
                        ..D16ReaderOutcome::ordered("root", "d16_backing_mismatch", error_order)
                    });
                    drop(held_reservation.take());
                    queue
                        .close_for_terminal_and_mark_ready("root", "d16_backing_mismatch")
                        .await;
                    return take_pending_failure(pending_failure);
                }
                let Some(next_queued) = queued_bytes.checked_add(chunk.bytes.len()) else {
                    *counter_failed = true;
                    let error_order = stamp_failure(&failure_clock);
                    *pending_failure = Some(D16ReaderOutcome {
                        retained: Some(chunk.bytes),
                        ..D16ReaderOutcome::ordered("root", "d16_queue_count_overflow", error_order)
                    });
                    drop(held_reservation.take());
                    queue
                        .close_for_terminal_and_mark_ready("root", "d16_queue_count_overflow")
                        .await;
                    return take_pending_failure(pending_failure);
                };
                if accounting
                    .as_mut()
                    .is_some_and(|accounting| !accounting.window.debit(chunk.bytes.len()))
                {
                    let error_order = stamp_failure(&failure_clock);
                    *pending_failure = Some(D16ReaderOutcome {
                        retained: Some(chunk.bytes),
                        ..D16ReaderOutcome::ordered(
                            "root",
                            "receive_window_debit_failed",
                            error_order,
                        )
                    });
                    drop(held_reservation.take());
                    queue
                        .close_for_terminal_and_mark_ready("root", "receive_window_debit_failed")
                        .await;
                    return take_pending_failure(pending_failure);
                }
                let Some(reservation) = held_reservation.take() else {
                    let error_order = stamp_failure(&failure_clock);
                    return D16ReaderOutcome {
                        retained: Some(chunk.bytes),
                        ..D16ReaderOutcome::ordered("root", "reader_reservation_lost", error_order)
                    };
                };
                match reservation.commit_root_retaining(chunk.bytes) {
                    Ok(should_wake) => {
                        *queued_bytes = next_queued;
                        if let Some(activity_tx) = &activity_tx {
                            let _ = activity_tx.try_send(());
                        }
                        if should_wake {
                            match send_ready_or_stop(
                                handle,
                                epoch,
                                &back_tx,
                                &mut stop_rx,
                                ready_sent,
                                counter_failed,
                            )
                            .await
                            {
                                None => {
                                    queue
                                        .close_for_terminal_and_mark_ready(
                                            "local_to_remote",
                                            "reader_stopped",
                                        )
                                        .await;
                                    return D16ReaderOutcome::reason(
                                        "local_to_remote",
                                        "reader_stopped",
                                    );
                                }
                                Some(false) => {
                                    let error_order = stamp_failure(&failure_clock);
                                    *pending_failure = Some(D16ReaderOutcome::ordered(
                                        "remote_to_local",
                                        "global_rx_closed",
                                        error_order,
                                    ));
                                    queue
                                        .close_for_terminal_and_mark_ready(
                                            "remote_to_local",
                                            "global_rx_closed",
                                        )
                                        .await;
                                    return take_pending_failure(pending_failure);
                                }
                                Some(true) => {}
                            }
                        }
                    }
                    Err((error, bytes, reservation)) => {
                        let error_order = stamp_failure(&failure_clock);
                        *pending_failure = Some(D16ReaderOutcome {
                            started: true,
                            reason: ("remote_to_local", "d16_queue_commit_failed"),
                            error: None,
                            queue_error: Some(error),
                            retained: Some(bytes),
                            error_order,
                            window: None,
                            body_panic: None,
                            future_drop_panic: None,
                            panic_order: None,
                            remainder: None,
                            disposed_read: None,
                            queued_bytes: 0,
                            ready_sent: 0,
                            closed_sent: 0,
                            counter_failed: false,
                        });
                        drop(reservation);
                        let should_wake = queue
                            .close_for_terminal_and_mark_ready(
                                "remote_to_local",
                                "d16_queue_commit_failed",
                            )
                            .await;
                        if should_wake {
                            let _ = send_ready_or_stop(
                                handle,
                                epoch,
                                &back_tx,
                                &mut stop_rx,
                                ready_sent,
                                counter_failed,
                            )
                            .await;
                        }
                        send_closed_or_stop(
                            handle,
                            epoch,
                            "remote_to_local",
                            "d16_queue_commit_failed",
                            &back_tx,
                            &mut stop_rx,
                            closed_sent,
                            counter_failed,
                        )
                        .await;
                        return take_pending_failure(pending_failure);
                    }
                }
            }
            Ok(None) => {
                drop(held_reservation.take());
                let should_wake = queue.close_for_remote_eof_and_mark_ready().await;
                if should_wake {
                    let _ = send_ready_or_stop(
                        handle,
                        epoch,
                        &back_tx,
                        &mut stop_rx,
                        ready_sent,
                        counter_failed,
                    )
                    .await;
                }
                return D16ReaderOutcome::reason("remote_to_local", "remote_eof");
            }
            Err(error) => {
                let error_order = stamp_failure(&failure_clock);
                *pending_failure = Some(D16ReaderOutcome {
                    started: true,
                    reason: ("remote_to_local", "remote_read_failed"),
                    error: Some(error),
                    queue_error: None,
                    retained: None,
                    error_order,
                    window: None,
                    body_panic: None,
                    future_drop_panic: None,
                    panic_order: None,
                    remainder: None,
                    disposed_read: None,
                    queued_bytes: 0,
                    ready_sent: 0,
                    closed_sent: 0,
                    counter_failed: false,
                });
                drop(held_reservation.take());
                let should_wake = queue
                    .close_for_terminal_and_mark_ready("remote_to_local", "remote_read_failed")
                    .await;
                if should_wake {
                    let _ = send_ready_or_stop(
                        handle,
                        epoch,
                        &back_tx,
                        &mut stop_rx,
                        ready_sent,
                        counter_failed,
                    )
                    .await;
                }
                send_closed_or_stop(
                    handle,
                    epoch,
                    "remote_to_local",
                    "remote_read_failed",
                    &back_tx,
                    &mut stop_rx,
                    closed_sent,
                    counter_failed,
                )
                .await;
                return take_pending_failure(pending_failure);
            }
        }
    }
}

#[cfg(test)]
mod tests;

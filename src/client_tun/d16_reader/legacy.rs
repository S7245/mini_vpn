//! Existing detached D16 compatibility outer and its timer/worker orchestration.
//! Owns only the legacy outer, not the session Root, physical receipt or clean
//! JoinHandle authority. Entry `run_relay_d16`; tests: ../tests/mod.rs.

use super::*;

pub(super) async fn send_d16_data_ready(
    handle: SocketHandle,
    epoch: u64,
    back_tx: &crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
) -> bool {
    back_tx
        .send((handle, RelayEvent::DataReady { epoch }))
        .await
        .is_ok()
}

pub(in crate::client_tun) async fn run_relay_d16(
    handle: SocketHandle,
    epoch: u64,
    native: crate::upstream::NativeTcpRelayStream,
    rx: crate::client_tun::uplink::Receiver,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
    queue: AsyncLeasedByteFlowQueue,
) {
    let Some((remote_reader, remote_writer, mut pump)) =
        crate::client_tun::legacy_native_readers::start(native, handle, epoch, &back_tx).await
    else {
        return;
    };
    let (writer_signal_tx, mut writer_signal_rx) = mpsc::channel(8);
    let (writer_stop_tx, writer_stop_rx) = tokio::sync::oneshot::channel();
    let (reader_stop_tx, reader_stop_rx) = tokio::sync::oneshot::channel();
    let (activity_tx, mut activity_rx) = mpsc::channel(1);
    let mut reader_task = tokio::spawn(run_d16_native_reader(
        handle,
        epoch,
        remote_reader,
        queue.clone(),
        read_credit_rx,
        back_tx.clone(),
        Some(activity_tx),
        reader_stop_rx,
        None,
    ));
    let mut writer_task = tokio::spawn(run_relay_writer(
        handle,
        remote_writer,
        rx,
        writer_signal_tx,
        writer_stop_rx,
    ));
    let mut close_timer = RelayCloseTimer::new();
    let mut reader_done = false;
    // The compatibility outer cannot claim clean after the joined reader
    // returned an original IO error or rejected backing. Root will own this
    // value through final disposition when D16 moves under FlowRoles.
    let mut reader_outcome = None;
    let mut writer_done = false;
    let mut activity_open = true;
    let mut writer_progress_events = 0u64;
    let mut writer_progress_bytes = 0u64;
    let mut writer_wait_max_us = 0u128;
    let mut half_closed_idle_blocked_events = 0u64;
    let mut half_closed_idle_blocked_max_payload_bytes = 0usize;
    let mut half_closed_local_progress_bytes = None;
    let terminal_error = loop {
        tokio::select! {
            activity = activity_rx.recv(), if !reader_done && activity_open => {
                match activity {
                    Some(()) => {
                        close_timer.note_read_progress();
                    }
                    None => activity_open = false,
                }
            }
            signal = writer_signal_rx.recv(), if !writer_done => {
                match signal {
                    Some(RelayWriterSignal::WritePending) => {
                        close_timer.arm_stalled_write();
                    }
                    Some(RelayWriterSignal::Progress { bytes, write_wait }) => {
                        writer_progress_events = writer_progress_events.saturating_add(1);
                        writer_progress_bytes =
                            writer_progress_bytes.saturating_add(bytes as u64);
                        writer_wait_max_us = writer_wait_max_us.max(write_wait.as_micros());
                        close_timer.note_write_progress();
                    }
                    Some(RelayWriterSignal::WriteCompleted) => {
                        close_timer.complete_write();
                    }
                    Some(RelayWriterSignal::WriteHalfClosed { .. }) => {
                        writer_done = true;
                        half_closed_local_progress_bytes = None;
                        close_timer.arm_half_closed();
                        if reader_done {
                            break None;
                        }
                    }
                    Some(RelayWriterSignal::Closed { direction, reason }) => {
                        writer_done = true;
                        if reason == "local_channel_closed" {
                            let queue_snapshot = queue.snapshot_now();
                            if let Some(cause) = queue_snapshot.closure.terminal_cause() {
                                break Some((cause.direction, cause.reason));
                            }
                            if queue_snapshot.closure.is_closed() {
                                if reader_done {
                                    break None;
                                }
                            } else {
                                break Some((direction, reason));
                            }
                        } else {
                            break Some((direction, reason));
                        }
                    }
                    None => {
                        break Some(("local_to_remote", "writer_task_closed"));
                    }
                }
            }
            result = &mut reader_task, if !reader_done => {
                reader_done = true;
                let outcome = match result {
                    Ok(outcome) => outcome,
                    Err(_) => break Some(("remote_to_local", "reader_task_failed")),
                };
                let clean = outcome.reason == ("remote_to_local", "remote_eof")
                    && outcome.error.is_none()
                    && outcome.queue_error.is_none()
                    && outcome.retained.is_none()
                    && outcome.body_panic.is_none()
                    && outcome.future_drop_panic.is_none();
                let reason = outcome.reason;
                reader_outcome = Some(outcome);
                if !clean {
                    break Some(reason);
                }
                if writer_done {
                    break None;
                }
            }
            _ = close_timer.wait(), if close_timer.is_armed() => {
                if close_timer.is_half_closed() {
                    let queue_snapshot = queue.snapshot_now();
                    let payload_owned_bytes = queue_snapshot
                        .queued_bytes
                        .saturating_add(queue_snapshot.leased_bytes);
                    if queue_snapshot.closure.terminal_cause().is_none()
                        && payload_owned_bytes > 0
                    {
                        let previous_local_progress_bytes =
                            half_closed_local_progress_bytes.unwrap_or(0);
                        let first_owned_window = half_closed_local_progress_bytes.is_none();
                        let made_local_progress = queue_snapshot.local_progress_bytes
                            > previous_local_progress_bytes;
                        if first_owned_window || made_local_progress {
                            half_closed_local_progress_bytes =
                                Some(queue_snapshot.local_progress_bytes);
                            half_closed_idle_blocked_events =
                                half_closed_idle_blocked_events.saturating_add(1);
                            half_closed_idle_blocked_max_payload_bytes =
                                half_closed_idle_blocked_max_payload_bytes.max(payload_owned_bytes);
                            tcp_diag_log!(
                                "🔎 tcp-d16-half-closed-idle-blocked handle={:?} epoch={} blocked_events={} payload_owned_bytes={} local_progress_bytes={} previous_local_progress_bytes={} queue_queued={} queue_leased={} queue_reserved={} queue_closed={}",
                                handle,
                                epoch,
                                half_closed_idle_blocked_events,
                                payload_owned_bytes,
                                queue_snapshot.local_progress_bytes,
                                previous_local_progress_bytes,
                                queue_snapshot.queued_bytes,
                                queue_snapshot.leased_bytes,
                                queue_snapshot.reserved_bytes,
                                queue_snapshot.closure.is_closed(),
                            );
                            close_timer.arm_half_closed();
                            continue;
                        }
                    }
                }
                break Some(("timer", close_timer.terminal_reason()));
            }
        }
    };

    pump.stop(RELAY_WRITER_STOP_TIMEOUT);
    if let Some((direction, reason)) = terminal_error {
        let _ = queue
            .close_for_terminal_and_mark_ready(direction, reason)
            .await;
    }
    let _ = reader_stop_tx.send(());
    if terminal_error.is_some() && !reader_done {
        if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut reader_task)
            .await
            .is_err()
        {
            reader_task.abort();
            let _ = (&mut reader_task).await;
        }
        reader_done = true;
    }

    if let Some((direction, reason)) = terminal_error {
        let _ = back_tx
            .send((
                handle,
                RelayEvent::Closed(RelayClose {
                    epoch,
                    direction,
                    reason,
                }),
            ))
            .await;
    }
    let _ = writer_stop_tx.send(());
    if !reader_done
        && tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut reader_task)
            .await
            .is_err()
    {
        reader_task.abort();
        let _ = reader_task.await;
    }
    if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
        let _ = writer_task.await;
    }
    pump.finish().await;
    let queue_snapshot = queue.snapshot_now();
    let (terminal_direction, terminal_reason) =
        terminal_error.unwrap_or(("none", "clean_queue_lifecycle"));
    tcp_diag_log!(
        "🔎 tcp-d16-relay-close handle={:?} epoch={} terminal_direction={} terminal_reason={} writer_progress_events={} writer_progress_bytes={} writer_wait_max_us={} half_closed_idle_blocked_events={} half_closed_idle_blocked_max_payload_bytes={} queue_queued={} queue_leased={} queue_reserved={} queue_closed={}",
        handle,
        epoch,
        terminal_direction,
        terminal_reason,
        writer_progress_events,
        writer_progress_bytes,
        writer_wait_max_us,
        half_closed_idle_blocked_events,
        half_closed_idle_blocked_max_payload_bytes,
        queue_snapshot.queued_bytes,
        queue_snapshot.leased_bytes,
        queue_snapshot.reserved_bytes,
        queue_snapshot.closure.is_closed(),
    );
    if let Some(outcome) = &reader_outcome {
        tcp_diag_log!(
            "🔎 tcp-d16-reader-outcome handle={:?} epoch={} direction={} reason={} io_os_error={:?} queue_error={:?} retained_bytes={}",
            handle,
            epoch,
            outcome.reason.0,
            outcome.reason.1,
            outcome
                .error
                .as_ref()
                .and_then(std::io::Error::raw_os_error),
            outcome.queue_error,
            outcome.retained.as_ref().map_or(0, Bytes::len),
        );
    }
}

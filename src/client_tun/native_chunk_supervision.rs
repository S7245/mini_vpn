//! Original Legacy ChunkPump lifecycle, including its four joined roles and timers.
//! Owns no Service/Session budget or saved Root. Entry: run_relay_native_chunk;
//! characterization remains in client_tun/tests.
use super::*;

pub(super) async fn run_relay_native_chunk(
    handle: SocketHandle,
    epoch: u64,
    native: crate::upstream::NativeTcpRelayStream,
    rx: crate::client_tun::uplink::Receiver,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
    native_read_service_floor: bool,
) {
    let (writer_signal_tx, mut writer_signal_rx) =
        mpsc::channel::<RelayWriterSignal>(RELAY_CHANNEL_CAPACITY);
    let (writer_stop_tx, writer_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (reader_signal_tx, mut reader_signal_rx) =
        mpsc::channel::<RelayReaderSignal>(RELAY_CHANNEL_CAPACITY);
    let (reader_stop_tx, reader_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let local_finish_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_credit_probe_rx = read_credit_rx.clone();
    tcp_diag_log!(
        "🔎 tcp-relay-engine handle={:?} epoch={} engine=native_chunk_pump queue_bytes={} reassembly_cap={} native_read_floor={}",
        handle,
        epoch,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES,
        NATIVE_TCP_REASSEMBLY_MAX_BYTES,
        native_read_service_floor
    );
    let Ok(queue) = AsyncByteBoundedFlowQueue::new(CONTINUOUS_TCP_RELAY_QUEUE_BYTES) else {
        let _ = back_tx
            .send((
                handle,
                RelayEvent::Closed(RelayClose {
                    epoch,
                    direction: "remote_to_local",
                    reason: "native_queue_config_failed",
                }),
            ))
            .await;
        return;
    };
    let Some((remote_reader, remote_writer, mut pump)) =
        crate::client_tun::legacy_native_readers::start(native, handle, epoch, &back_tx).await
    else {
        return;
    };
    let mut dispatcher_task = tokio::spawn(run_continuous_relay_dispatcher(
        handle,
        epoch,
        queue.clone(),
        back_tx.clone(),
    ));
    let mut reader_task = tokio::spawn(legacy_native_readers::run_chunk(
        legacy_native_readers::Request {
            handle,
            epoch,
            remote_reader,
            read_credit_rx,
            signal_tx: reader_signal_tx,
            local_finish_seen: local_finish_seen.clone(),
            stop_rx: reader_stop_rx,
            native_read_service_floor,
        },
        queue,
    ));
    let mut writer_task = tokio::spawn(run_relay_writer(
        handle,
        remote_writer,
        rx,
        writer_signal_tx,
        writer_stop_rx,
    ));
    let mut diag = RelayTaskDiag::default();
    let local_write_pressure_threshold = std::time::Duration::from_millis(5);
    let mut close_timer = RelayCloseTimer::new();
    let mut diag_tick = tokio::time::interval(std::time::Duration::from_secs(RELAY_LIVE_DIAG_SECS));
    diag_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    diag_tick.tick().await;
    let mut remote_read_probe_tick = tokio::time::interval(std::time::Duration::from_millis(
        RELAY_REMOTE_READ_PROBE_INTERVAL_MS,
    ));
    remote_read_probe_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    remote_read_probe_tick.tick().await;
    let mut writer_done = false;
    let mut read_only_after_local_finish = false;
    let mut last_ack_drain_hint_at = None;
    let (close_direction, close_reason) = loop {
        tokio::select! {
            _ = diag_tick.tick(), if tcp_diag_enabled() => {
                tcp_diag_log!(
                    "{}",
                    format_relay_live_diag(
                        handle,
                        epoch,
                        writer_done,
                        read_only_after_local_finish,
                        &diag
                    )
                );
            }
            _ = remote_read_probe_tick.tick(), if should_poll_relay_remote_read_probe(&diag, *read_credit_probe_rx.borrow()) => {
                diag.note_remote_read_probe_tick();
                let now = std::time::Instant::now();
                if should_poll_relay_ack_drain_hint(&diag, writer_done)
                    && let Some(hint) = relay_ack_drain_hint_due(&diag, now, last_ack_drain_hint_at)
                {
                    last_ack_drain_hint_at = Some(now);
                    diag.note_ack_drain_hint_due();
                    match back_tx.try_send((
                        handle,
                        RelayEvent::AckDrainHint {
                            epoch,
                            gap_ms: hint.gap_ms,
                            remote_to_global_rx_bytes: hint.remote_to_global_rx_bytes,
                        },
                    )) {
                        Ok(()) => diag.note_ack_drain_hint_sent(),
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            diag.note_ack_drain_hint_dropped();
                            tcp_diag_log!(
                                "🔎 tcp-relay-ack-drain-hint-drop handle={:?} epoch={} reason=global_rx_full gap_ms={} remote_to_global_rx_bytes={} dropped={}",
                                handle,
                                epoch,
                                hint.gap_ms,
                                hint.remote_to_global_rx_bytes,
                                diag.ack_drain_hint_dropped
                            );
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            diag.note_ack_drain_hint_dropped();
                            tcp_diag_log!(
                                "🔎 tcp-relay-ack-drain-hint-drop handle={:?} epoch={} reason=global_rx_closed gap_ms={} remote_to_global_rx_bytes={} dropped={}",
                                handle,
                                epoch,
                                hint.gap_ms,
                                hint.remote_to_global_rx_bytes,
                                diag.ack_drain_hint_dropped
                            );
                        }
                    }
                }
            }
            signal = writer_signal_rx.recv(), if !writer_done => {
                match signal {
                    Some(RelayWriterSignal::WritePending) => {
                        close_timer.arm_stalled_write();
                    }
                    Some(RelayWriterSignal::Progress { bytes, write_wait }) => {
                        diag.note_uplink_write(bytes);
                        diag.note_local_write_wait(write_wait, local_write_pressure_threshold);
                        if write_wait >= local_write_pressure_threshold {
                            tcp_diag_log!(
                                "🔎 tcp-local-write-pressure handle={:?} wait_us={} payload_bytes={} pressure_events={}",
                                handle,
                                write_wait.as_micros(),
                                bytes,
                                diag.local_write_pressure_events
                            );
                        }
                        close_timer.note_write_progress();
                    }
                    Some(RelayWriterSignal::WriteCompleted) => {
                        close_timer.complete_write();
                    }
                    Some(RelayWriterSignal::WriteHalfClosed { reason }) => {
                        diag.note_local_finish();
                        local_finish_seen.store(true, std::sync::atomic::Ordering::SeqCst);
                        tcp_diag_log!(
                            "🔎 tcp-relay-write-half-closed handle={:?} reason={}",
                            handle,
                            reason
                        );
                        writer_done = true;
                        read_only_after_local_finish = true;
                        close_timer.arm_half_closed();
                    }
                    Some(RelayWriterSignal::Closed { direction, reason }) => {
                        break (direction, reason);
                    }
                    None => {
                        break ("local_to_remote", "writer_task_closed");
                    }
                }
            }
            reader_signal = reader_signal_rx.recv() => {
                match reader_signal {
                    Some(RelayReaderSignal::Progress { diag: reader_diag }) => {
                        diag.copy_reader_fields_from(&reader_diag);
                        close_timer.note_read_progress();
                    }
                    Some(RelayReaderSignal::Closed { direction, reason, diag: reader_diag }) => {
                        diag.copy_reader_fields_from(&reader_diag);
                        break (direction, reason);
                    }
                    None => {
                        break ("remote_to_local", "reader_task_closed");
                    }
                }
            }
            _ = close_timer.wait(), if close_timer.is_armed() => {
                let timeout = close_timer.timeout().unwrap_or_default();
                let reason = close_timer.terminal_reason();
                println!("⏱️ relay {:?} lifecycle guard {}s expired reason={}", handle, timeout.as_secs(), reason);
                break ("timer", reason);
            }
        }
    };
    pump.stop(RELAY_WRITER_STOP_TIMEOUT);
    let _ = reader_stop_tx.send(());
    let _ = writer_stop_tx.send(());
    if reader_task.is_finished() {
        let _ = reader_task.await;
    } else if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut reader_task)
        .await
        .is_err()
    {
        reader_task.abort();
        let _ = reader_task.await;
    }
    if writer_task.is_finished() {
        let _ = writer_task.await;
    } else if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
        let _ = writer_task.await;
    }
    if dispatcher_task.is_finished() {
        let _ = dispatcher_task.await;
    } else if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut dispatcher_task)
        .await
        .is_err()
    {
        dispatcher_task.abort();
        let _ = dispatcher_task.await;
    }
    pump.finish().await;
    tcp_diag_log!(
        "{}",
        format_relay_close_diag(handle, close_direction, close_reason, &diag)
    );
    let _ = back_tx
        .send((
            handle,
            RelayEvent::Closed(RelayClose {
                epoch,
                direction: close_direction,
                reason: close_reason,
            }),
        ))
        .await;
}

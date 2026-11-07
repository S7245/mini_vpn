//! Legacy Native chunk/permit reader transactions, moved from client_tun.
//! Each reader owns its source, assembler, pending output and stop disposition;
//! task supervision, Main and session authority remain at their original owners.
//! Entries: run_chunk/run_permit. Characterization: client_tun/tests/native_permit_owner.rs
//! and native_chunk_assembler/tests. This move preserves the original reader loops.
use super::*;

pub(super) struct Request {
    pub(super) handle: SocketHandle,
    pub(super) epoch: u64,
    pub(super) remote_reader: NativeTcpReadHalf,
    pub(super) read_credit_rx: watch::Receiver<RelayReadCredit>,
    pub(super) signal_tx: mpsc::Sender<RelayReaderSignal>,
    pub(super) local_finish_seen: Arc<std::sync::atomic::AtomicBool>,
    pub(super) stop_rx: tokio::sync::oneshot::Receiver<()>,
    pub(super) native_read_service_floor: bool,
}

pub(super) async fn run_chunk(request: Request, queue: AsyncByteBoundedFlowQueue) {
    let Request {
        handle,
        epoch,
        mut remote_reader,
        mut read_credit_rx,
        signal_tx,
        local_finish_seen,
        mut stop_rx,
        native_read_service_floor,
    } = request;
    let mut diag = RelayTaskDiag::default();
    let mut read_credit = *read_credit_rx.borrow();
    diag.note_read_credit(read_credit);
    let mut assembler = NativeTcpChunkAssembler::with_meter(remote_reader.receive_backing_meter());
    let queue_wait_threshold = std::time::Duration::from_millis(5);
    loop {
        note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
        drain_relay_read_credit_updates(&mut read_credit_rx, &mut read_credit, &mut diag);
        let read_credit_limit =
            native_tcp_read_service_limit(read_credit, native_read_service_floor);
        if read_credit_limit == 0 {
            if read_credit.paused {
                log_native_tcp_read_credit_pause(handle, epoch, "native_chunk_pump", &diag);
            }
            let update = tokio::select! {
                _ = &mut stop_rx => {
                    queue.close().await;
                    return;
                }
                update = read_credit_rx.changed() => update,
            };
            if update.is_err() {
                queue.close().await;
                return;
            }
            read_credit = *read_credit_rx.borrow_and_update();
            diag.note_read_credit(read_credit);
            continue;
        }
        let queue_wait_started = std::time::Instant::now();
        let read_len = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            result = queue.wait_read_len(read_credit_limit) => {
                match result {
                    Ok(read_len) => {
                        let waited = queue_wait_started.elapsed();
                        diag.note_continuous_queue_wait(waited, queue_wait_threshold);
                        if waited >= queue_wait_threshold {
                            tcp_diag_log!(
                                "🔎 tcp-native-queue-wait handle={:?} epoch={} wait_us={} read_len={} wait_events={}",
                                handle,
                                epoch,
                                waited.as_micros(),
                                read_len,
                                diag.continuous_queue_wait_events
                            );
                        }
                        read_len
                    }
                    Err(_) => return,
                }
            }
        };
        if read_len == 0 {
            continue;
        }
        if let Some(payload) = match assembler.drain_ordered_up_to(read_len) {
            Ok(value) => value,
            Err(_) => {
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_read_failed", diag)
                    .await;
                return;
            }
        } {
            match push_native_ordered_payload(handle, epoch, payload, &queue, &signal_tx, &mut diag)
                .await
            {
                Ok(true) => continue,
                Ok(false) => {
                    queue.close().await;
                    return;
                }
                Err((direction, reason)) => {
                    send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                    return;
                }
            }
        }

        let available_reassembly =
            NATIVE_TCP_REASSEMBLY_MAX_BYTES.saturating_sub(assembler.buffered_bytes());
        if available_reassembly == 0 {
            assembler.note_cap_hit();
            assembler.maybe_log_gap(handle, epoch, "cap");
            queue.close().await;
            send_relay_reader_close(&signal_tx, "remote_to_local", "native_reassembly_cap", diag)
                .await;
            return;
        }
        let native_read_len = read_len.min(available_reassembly);
        diag.note_remote_read_service_tick(native_read_len);
        let chunk = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            chunk = poll_native_tcp_chunk(&mut remote_reader, native_read_len) => chunk,
        };
        match chunk {
            Ok(Some(chunk)) => {
                diag.note_remote_raw_read(chunk.bytes.len());
                if chunk.bytes.len() > native_read_len || assembler.push_chunk(chunk).is_err() {
                    queue.close().await;
                    send_relay_reader_close(
                        &signal_tx,
                        "remote_to_local",
                        "remote_read_failed",
                        diag,
                    )
                    .await;
                    return;
                }
                if let Some(payload) = match assembler.drain_ordered_up_to(read_len) {
                    Ok(value) => value,
                    Err(_) => {
                        queue.close().await;
                        send_relay_reader_close(
                            &signal_tx,
                            "remote_to_local",
                            "remote_read_failed",
                            diag,
                        )
                        .await;
                        return;
                    }
                } {
                    match push_native_ordered_payload(
                        handle, epoch, payload, &queue, &signal_tx, &mut diag,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            queue.close().await;
                            return;
                        }
                        Err((direction, reason)) => {
                            send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                            return;
                        }
                    }
                } else {
                    assembler.maybe_log_gap(handle, epoch, "gap");
                }
            }
            Ok(None) => {
                println!("远端服务器关闭了车厢 {:?}", handle);
                if let Some(payload) = match assembler.drain_ordered_up_to(read_len) {
                    Ok(value) => value,
                    Err(_) => {
                        queue.close().await;
                        send_relay_reader_close(
                            &signal_tx,
                            "remote_to_local",
                            "remote_read_failed",
                            diag,
                        )
                        .await;
                        return;
                    }
                } {
                    let _ = push_native_ordered_payload(
                        handle, epoch, payload, &queue, &signal_tx, &mut diag,
                    )
                    .await;
                }
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_eof", diag).await;
                return;
            }
            Err(e) => {
                println!(
                    "读取上游流失败 direction=remote_to_local handle={:?} err={:?}",
                    handle, e
                );
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_read_failed", diag)
                    .await;
                return;
            }
        }
    }
}

pub(super) async fn run_permit(request: Request, queue: AsyncLeasedByteFlowQueue) {
    let Request {
        handle,
        epoch,
        mut remote_reader,
        mut read_credit_rx,
        signal_tx,
        local_finish_seen,
        mut stop_rx,
        native_read_service_floor,
    } = request;
    let mut diag = RelayTaskDiag::default();
    let mut read_credit = *read_credit_rx.borrow();
    diag.note_read_credit(read_credit);
    let mut assembler = NativeTcpChunkAssembler::with_meter(remote_reader.receive_backing_meter());
    let queue_wait_threshold = std::time::Duration::from_millis(5);
    loop {
        note_reader_local_finish_if_needed(&local_finish_seen, &mut diag);
        drain_relay_read_credit_updates(&mut read_credit_rx, &mut read_credit, &mut diag);
        let read_credit_limit =
            native_tcp_read_service_limit(read_credit, native_read_service_floor);
        if read_credit_limit == 0 {
            if read_credit.paused {
                log_native_tcp_read_credit_pause(handle, epoch, "native_permit_pump", &diag);
            }
            let update = tokio::select! {
                _ = &mut stop_rx => {
                    queue.close().await;
                    return;
                }
                update = read_credit_rx.changed() => update,
            };
            if update.is_err() {
                queue.close().await;
                return;
            }
            read_credit = *read_credit_rx.borrow_and_update();
            diag.note_read_credit(read_credit);
            continue;
        }
        let queue_wait_started = std::time::Instant::now();
        let read_len = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            result = queue.wait_read_len(read_credit_limit) => {
                match result {
                    Ok(read_len) => {
                        let waited = queue_wait_started.elapsed();
                        diag.note_continuous_queue_wait(waited, queue_wait_threshold);
                        if waited >= queue_wait_threshold {
                            let snapshot = queue.snapshot().await;
                            tcp_diag_log!(
                                "🔎 tcp-native-permit-queue-wait handle={:?} epoch={} wait_us={} read_len={} queued_bytes={} leased_bytes={} capacity_bytes={} wait_events={}",
                                handle,
                                epoch,
                                waited.as_micros(),
                                read_len,
                                snapshot.queued_bytes,
                                snapshot.leased_bytes,
                                snapshot.capacity_bytes,
                                diag.continuous_queue_wait_events
                            );
                        }
                        read_len
                    }
                    Err(_) => return,
                }
            }
        };
        if read_len == 0 {
            continue;
        }
        if let Some(payload) = match assembler.drain_ordered_up_to(read_len) {
            Ok(value) => value,
            Err(_) => {
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_read_failed", diag)
                    .await;
                return;
            }
        } {
            match push_native_ordered_leased_payload(
                handle, epoch, payload, &queue, &signal_tx, &mut diag,
            )
            .await
            {
                Ok(true) => continue,
                Ok(false) => {
                    queue.close().await;
                    return;
                }
                Err((direction, reason)) => {
                    send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                    return;
                }
            }
        }

        let available_reassembly =
            NATIVE_TCP_REASSEMBLY_MAX_BYTES.saturating_sub(assembler.buffered_bytes());
        if available_reassembly == 0 {
            assembler.note_cap_hit();
            assembler.maybe_log_gap(handle, epoch, "permit_cap");
            queue.close().await;
            send_relay_reader_close(&signal_tx, "remote_to_local", "native_reassembly_cap", diag)
                .await;
            return;
        }
        let native_read_len = read_len.min(available_reassembly);
        diag.note_remote_read_service_tick(native_read_len);
        let chunk = tokio::select! {
            _ = &mut stop_rx => {
                queue.close().await;
                return;
            }
            chunk = poll_native_tcp_chunk(&mut remote_reader, native_read_len) => chunk,
        };
        match chunk {
            Ok(Some(chunk)) => {
                diag.note_remote_raw_read(chunk.bytes.len());
                if chunk.bytes.len() > native_read_len || assembler.push_chunk(chunk).is_err() {
                    queue.close().await;
                    send_relay_reader_close(
                        &signal_tx,
                        "remote_to_local",
                        "remote_read_failed",
                        diag,
                    )
                    .await;
                    return;
                }
                if let Some(payload) = match assembler.drain_ordered_up_to(read_len) {
                    Ok(value) => value,
                    Err(_) => {
                        queue.close().await;
                        send_relay_reader_close(
                            &signal_tx,
                            "remote_to_local",
                            "remote_read_failed",
                            diag,
                        )
                        .await;
                        return;
                    }
                } {
                    match push_native_ordered_leased_payload(
                        handle, epoch, payload, &queue, &signal_tx, &mut diag,
                    )
                    .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            queue.close().await;
                            return;
                        }
                        Err((direction, reason)) => {
                            send_relay_reader_close(&signal_tx, direction, reason, diag).await;
                            return;
                        }
                    }
                } else {
                    assembler.maybe_log_gap(handle, epoch, "permit_gap");
                }
            }
            Ok(None) => {
                println!("远端服务器关闭了车厢 {:?}", handle);
                if let Some(payload) = match assembler.drain_ordered_up_to(read_len) {
                    Ok(value) => value,
                    Err(_) => {
                        queue.close().await;
                        send_relay_reader_close(
                            &signal_tx,
                            "remote_to_local",
                            "remote_read_failed",
                            diag,
                        )
                        .await;
                        return;
                    }
                } {
                    let _ = push_native_ordered_leased_payload(
                        handle, epoch, payload, &queue, &signal_tx, &mut diag,
                    )
                    .await;
                }
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_eof", diag).await;
                return;
            }
            Err(e) => {
                println!(
                    "读取上游流失败 direction=remote_to_local handle={:?} err={:?}",
                    handle, e
                );
                queue.close().await;
                send_relay_reader_close(&signal_tx, "remote_to_local", "remote_read_failed", diag)
                    .await;
                return;
            }
        }
    }
}

#[cfg(test)]
#[path = "legacy_native_readers/tests/mod.rs"]
mod tests;

/// One cold capability gate for all three detached launchers. Original carrier
/// destruction cancels uncommitted work before publishing the explicit refusal.
pub(super) async fn start(
    native: crate::upstream::NativeTcpRelayStream,
    handle: SocketHandle,
    epoch: u64,
    back: &crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
) -> Option<(
    crate::upstream::NativeTcpReadHalf,
    crate::upstream::NativeTcpWriteHalf,
    crate::upstream::LegacyNativePump,
)> {
    match native.try_start_legacy() {
        Ok(parts) => Some(parts),
        Err(native) => {
            drop(native);
            let _ = back
                .send((
                    handle,
                    RelayEvent::Closed(RelayClose {
                        epoch,
                        direction: "remote_to_local",
                        reason: "native_hard_budget_requires_saved_root",
                    }),
                ))
                .await;
            None
        }
    }
}

//! Generic relay outer-supervision mechanics.
//!
//! This module owns the current reader, writer, dispatcher, bounded-queue, close, timeout, and
//! task-settlement mechanics for Generic relay engines. It owns no F ledger, outer JoinHandle
//! custody, raw rearm, OpenEvent, public entry, or DATA-path state. The S0 invariant preserves the
//! moved bodies and call order exactly. Its entries are `GenericLaunchRequest`,
//! `launch_generic_relay`, and the six `run_relay*` functions; tests live in
//! `src/client_tun/tests/mod.rs` and `compatibility_flow_custody.rs`.

use super::super::super::{
    AsyncByteBoundedFlowQueue, AsyncLeasedByteFlowQueue, CONTINUOUS_TCP_RELAY_QUEUE_BYTES,
    DownlinkBackpressureConfig, DownstreamPermitReleaseMode, RELAY_CHANNEL_CAPACITY,
    RELAY_LIVE_DIAG_SECS, RELAY_REMOTE_READ_PROBE_INTERVAL_MS, RELAY_WRITER_STOP_TIMEOUT,
    RelayClose, RelayCloseTimer, RelayCommand, RelayEvent, RelayReadCredit, RelayReaderSignal,
    RelayStream, RelayTaskDiag, RelayWriterSignal, SocketHandle, THIN_TCP_RELAY_STAGING_CAPACITY,
    TcpRelayEngine, format_relay_close_diag, format_relay_live_diag,
    native_tcp_permit_queue_capacity_bytes, relay_ack_drain_hint_due,
    run_continuous_relay_dispatcher, run_permit_relay_dispatcher, run_relay_continuous_reader,
    run_relay_permit_reader, run_relay_thin_reader, run_relay_writer, run_thin_relay_dispatcher,
    send_relay_reader_close, should_poll_relay_ack_drain_hint, should_poll_relay_remote_read_probe,
    tcp_diag_enabled,
};
use super::relay_reader::{ReaderOutput, ReaderRequest, run_relay_reader};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

/// The complete Generic launch inputs move as one context after the root has selected its engine.
/// This module preserves detached legacy task start and owns no task handle or flow state.
pub(in crate::client_tun) struct GenericLaunchRequest {
    pub(in crate::client_tun) engine: TcpRelayEngine,
    pub(in crate::client_tun) handle: SocketHandle,
    pub(in crate::client_tun) epoch: u64,
    pub(in crate::client_tun) stream: RelayStream,
    pub(in crate::client_tun) rx: crate::client_tun::uplink::Receiver,
    pub(in crate::client_tun) back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    pub(in crate::client_tun) read_credit_rx: watch::Receiver<RelayReadCredit>,
    pub(in crate::client_tun) downlink_backpressure: DownlinkBackpressureConfig,
}

/// Cold Native carrier inputs retained by the original session Root until its
/// final synchronous commit. The optional producer task is still prepared.
pub(in crate::client_tun) struct NativeLaunchRequest {
    pub(in crate::client_tun) uplink_observer: Option<crate::client_tun::uplink::Observer>,
    pub(in crate::client_tun) handle: SocketHandle,
    pub(in crate::client_tun) epoch: u64,
    pub(in crate::client_tun) native: crate::upstream::NativeTcpRelayStream,
    pub(in crate::client_tun) engine: crate::upstream::NativeTcpRelayEngine,
    pub(in crate::client_tun) read_floor: bool,
    pub(in crate::client_tun) rx: crate::client_tun::uplink::Receiver,
    pub(in crate::client_tun) back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    pub(in crate::client_tun) read_credit_rx: watch::Receiver<RelayReadCredit>,
    pub(in crate::client_tun) downlink_backpressure: DownlinkBackpressureConfig,
}

/// The direct ByteOwned carrier and its one original D16 queue/budget identity.
/// Root keeps this cold request until the physical F publication commit.
pub(in crate::client_tun) struct D16LaunchRequest {
    pub(in crate::client_tun) handle: SocketHandle,
    pub(in crate::client_tun) epoch: u64,
    pub(in crate::client_tun) native: crate::upstream::NativeTcpRelayStream,
    pub(in crate::client_tun) queue: AsyncLeasedByteFlowQueue,
    pub(in crate::client_tun) budget: crate::tcp_downlink_pump::D16GlobalByteBudget,
    pub(in crate::client_tun) rx: crate::client_tun::uplink::Receiver,
    pub(in crate::client_tun) back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    pub(in crate::client_tun) read_credit_rx: watch::Receiver<RelayReadCredit>,
}

/// Starts the selected Generic relay exactly as the original root dispatch did.
pub(in crate::client_tun) fn launch_generic_relay(request: GenericLaunchRequest) {
    let GenericLaunchRequest {
        engine,
        handle,
        epoch,
        stream,
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure,
    } = request;
    match engine {
        TcpRelayEngine::Legacy => {
            tokio::spawn(run_relay(
                handle,
                epoch,
                stream,
                rx,
                back_tx,
                read_credit_rx,
            ));
        }
        TcpRelayEngine::ThinStaging => {
            tokio::spawn(run_relay_thin(
                handle,
                epoch,
                stream,
                rx,
                back_tx,
                read_credit_rx,
            ));
        }
        TcpRelayEngine::ContinuousPump => {
            tokio::spawn(run_relay_continuous(
                handle,
                epoch,
                stream,
                rx,
                back_tx,
                read_credit_rx,
            ));
        }
        TcpRelayEngine::PermitPump => {
            tokio::spawn(run_relay_permit(
                handle,
                epoch,
                stream,
                rx,
                back_tx,
                read_credit_rx,
            ));
        }
        TcpRelayEngine::OrderedEgressPermitPump => {
            let queue_capacity_bytes =
                native_tcp_permit_queue_capacity_bytes(downlink_backpressure);
            tokio::spawn(run_relay_ordered_egress_permit(
                handle,
                epoch,
                stream,
                rx,
                back_tx,
                read_credit_rx,
                queue_capacity_bytes,
            ));
        }
    }
}

/// 一条 TCP relay 的双向泵。Established 空闲由 socket/transport 生命周期拥有；只有待完成写入和
/// 半关闭收尾启用有界 close timer。适用 TUIC/REALITY 两种 RelayStream。
pub(in crate::client_tun) async fn run_relay(
    handle: SocketHandle,
    epoch: u64,
    stream: RelayStream,
    rx: crate::client_tun::uplink::Receiver,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
) {
    run_relay_with_engine(
        handle,
        epoch,
        stream,
        rx,
        back_tx,
        read_credit_rx,
        TcpRelayEngine::Legacy,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES,
    )
    .await;
}

pub(in crate::client_tun) async fn run_relay_thin(
    handle: SocketHandle,
    epoch: u64,
    stream: RelayStream,
    rx: crate::client_tun::uplink::Receiver,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
) {
    run_relay_with_engine(
        handle,
        epoch,
        stream,
        rx,
        back_tx,
        read_credit_rx,
        TcpRelayEngine::ThinStaging,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES,
    )
    .await;
}

pub(in crate::client_tun) async fn run_relay_continuous(
    handle: SocketHandle,
    epoch: u64,
    stream: RelayStream,
    rx: crate::client_tun::uplink::Receiver,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
) {
    run_relay_with_engine(
        handle,
        epoch,
        stream,
        rx,
        back_tx,
        read_credit_rx,
        TcpRelayEngine::ContinuousPump,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES,
    )
    .await;
}

pub(in crate::client_tun) async fn run_relay_permit(
    handle: SocketHandle,
    epoch: u64,
    stream: RelayStream,
    rx: crate::client_tun::uplink::Receiver,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
) {
    run_relay_with_engine(
        handle,
        epoch,
        stream,
        rx,
        back_tx,
        read_credit_rx,
        TcpRelayEngine::PermitPump,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES,
    )
    .await;
}

pub(in crate::client_tun) async fn run_relay_ordered_egress_permit(
    handle: SocketHandle,
    epoch: u64,
    stream: RelayStream,
    rx: crate::client_tun::uplink::Receiver,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
    queue_capacity_bytes: usize,
) {
    run_relay_with_engine(
        handle,
        epoch,
        stream,
        rx,
        back_tx,
        read_credit_rx,
        TcpRelayEngine::OrderedEgressPermitPump,
        queue_capacity_bytes,
    )
    .await;
}

pub(in crate::client_tun) async fn run_relay_with_engine(
    handle: SocketHandle,
    epoch: u64,
    stream: RelayStream,
    rx: crate::client_tun::uplink::Receiver,
    back_tx: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    read_credit_rx: watch::Receiver<RelayReadCredit>,
    engine: TcpRelayEngine,
    ordered_egress_permit_queue_capacity_bytes: usize,
) {
    let (remote_reader, remote_writer) = tokio::io::split(stream);
    let (writer_signal_tx, writer_signal_rx) =
        mpsc::channel::<RelayWriterSignal>(RELAY_CHANNEL_CAPACITY);
    let (writer_stop_tx, writer_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (reader_signal_tx, reader_signal_rx) =
        mpsc::channel::<RelayReaderSignal>(RELAY_CHANNEL_CAPACITY);
    let (reader_stop_tx, reader_stop_rx) = tokio::sync::oneshot::channel::<()>();
    let local_finish_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_credit_probe_rx = read_credit_rx.clone();
    let mut dispatcher_task = None;
    let mut reader_task = match engine {
        TcpRelayEngine::Legacy => tokio::spawn(run_relay_reader(ReaderRequest {
            handle,
            epoch,
            remote_reader,
            back_tx: ReaderOutput::Main(back_tx.clone()),
            read_credit_rx,
            signal_tx: reader_signal_tx,
            local_finish_seen: local_finish_seen.clone(),
            stop_rx: reader_stop_rx,
        })),
        TcpRelayEngine::ThinStaging => {
            tcp_diag_log!(
                "🔎 tcp-relay-engine handle={:?} epoch={} engine={} staging_capacity={}",
                handle,
                epoch,
                engine.as_str(),
                THIN_TCP_RELAY_STAGING_CAPACITY
            );
            let (thin_tx, thin_rx) = mpsc::channel(THIN_TCP_RELAY_STAGING_CAPACITY);
            dispatcher_task = Some(tokio::spawn(run_thin_relay_dispatcher(
                handle,
                back_tx.clone(),
                thin_rx,
            )));
            tokio::spawn(run_relay_thin_reader(
                handle,
                epoch,
                remote_reader,
                thin_tx,
                read_credit_rx,
                reader_signal_tx,
                local_finish_seen.clone(),
                reader_stop_rx,
            ))
        }
        TcpRelayEngine::ContinuousPump => {
            tcp_diag_log!(
                "🔎 tcp-relay-engine handle={:?} epoch={} engine={} queue_bytes={}",
                handle,
                epoch,
                engine.as_str(),
                CONTINUOUS_TCP_RELAY_QUEUE_BYTES
            );
            match AsyncByteBoundedFlowQueue::new(CONTINUOUS_TCP_RELAY_QUEUE_BYTES) {
                Ok(queue) => {
                    dispatcher_task = Some(tokio::spawn(run_continuous_relay_dispatcher(
                        handle,
                        epoch,
                        queue.clone(),
                        back_tx.clone(),
                    )));
                    tokio::spawn(run_relay_continuous_reader(
                        handle,
                        epoch,
                        remote_reader,
                        queue,
                        read_credit_rx,
                        reader_signal_tx,
                        local_finish_seen.clone(),
                        reader_stop_rx,
                    ))
                }
                Err(_) => tokio::spawn(async move {
                    drop(remote_reader);
                    drop(read_credit_rx);
                    drop(reader_stop_rx);
                    send_relay_reader_close(
                        &reader_signal_tx,
                        "remote_to_local",
                        "continuous_queue_config_failed",
                        RelayTaskDiag::default(),
                    )
                    .await;
                }),
            }
        }
        TcpRelayEngine::PermitPump => {
            tcp_diag_log!(
                "🔎 tcp-relay-engine handle={:?} epoch={} engine={} queue_bytes={}",
                handle,
                epoch,
                engine.as_str(),
                CONTINUOUS_TCP_RELAY_QUEUE_BYTES
            );
            match AsyncLeasedByteFlowQueue::new(CONTINUOUS_TCP_RELAY_QUEUE_BYTES) {
                Ok(queue) => {
                    dispatcher_task = Some(tokio::spawn(run_permit_relay_dispatcher(
                        handle,
                        epoch,
                        queue.clone(),
                        back_tx.clone(),
                    )));
                    tokio::spawn(run_relay_permit_reader(
                        handle,
                        epoch,
                        remote_reader,
                        queue,
                        read_credit_rx,
                        reader_signal_tx,
                        local_finish_seen.clone(),
                        reader_stop_rx,
                        false,
                        engine.as_str(),
                    ))
                }
                Err(_) => tokio::spawn(async move {
                    drop(remote_reader);
                    drop(read_credit_rx);
                    drop(reader_stop_rx);
                    send_relay_reader_close(
                        &reader_signal_tx,
                        "remote_to_local",
                        "permit_queue_config_failed",
                        RelayTaskDiag::default(),
                    )
                    .await;
                }),
            }
        }
        TcpRelayEngine::OrderedEgressPermitPump => {
            let queue_capacity = ordered_egress_permit_queue_capacity_bytes.max(1);
            tcp_diag_log!(
                "🔎 tcp-relay-engine handle={:?} epoch={} engine={} queue_bytes={}",
                handle,
                epoch,
                engine.as_str(),
                queue_capacity
            );
            match AsyncLeasedByteFlowQueue::new_with_release_mode(
                queue_capacity,
                DownstreamPermitReleaseMode::OnEgressDrain,
            ) {
                Ok(queue) => {
                    dispatcher_task = Some(tokio::spawn(run_permit_relay_dispatcher(
                        handle,
                        epoch,
                        queue.clone(),
                        back_tx.clone(),
                    )));
                    tokio::spawn(run_relay_permit_reader(
                        handle,
                        epoch,
                        remote_reader,
                        queue,
                        read_credit_rx,
                        reader_signal_tx,
                        local_finish_seen.clone(),
                        reader_stop_rx,
                        true,
                        engine.as_str(),
                    ))
                }
                Err(_) => tokio::spawn(async move {
                    drop(remote_reader);
                    drop(read_credit_rx);
                    drop(reader_stop_rx);
                    send_relay_reader_close(
                        &reader_signal_tx,
                        "remote_to_local",
                        "ordered_egress_permit_queue_config_failed",
                        RelayTaskDiag::default(),
                    )
                    .await;
                }),
            }
        }
    };
    let mut writer_task = tokio::spawn(run_relay_writer(
        handle,
        remote_writer,
        rx,
        writer_signal_tx,
        writer_stop_rx,
    ));
    let mut monitor = GenericRelayMonitor::new(GenericRelaySignals {
        handle,
        epoch,
        writer_signal_rx,
        reader_signal_rx,
        local_finish_seen,
        read_credit_probe_rx,
    })
    .await;
    let (close_direction, close_reason) = monitor.run(&back_tx).await;
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
    // M2 preserved after splitting the pump: let the write half drive shutdown so protocol-layer
    // pending bytes can flush, but keep the relay close path bounded if shutdown itself wedges.
    if writer_task.is_finished() {
        let _ = writer_task.await;
    } else if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut writer_task)
        .await
        .is_err()
    {
        writer_task.abort();
        let _ = writer_task.await;
    }
    if let Some(mut task) = dispatcher_task {
        if task.is_finished() {
            let _ = task.await;
        } else if tokio::time::timeout(RELAY_WRITER_STOP_TIMEOUT, &mut task)
            .await
            .is_err()
        {
            task.abort();
            let _ = task.await;
        }
    }
    tcp_diag_log!(
        "{}",
        format_relay_close_diag(handle, close_direction, close_reason, &monitor.diag)
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

// Signals and timers remain owned by the original caller through joins and Closed.
// The shared monitor observes lifecycle; it never owns or joins worker handles.
struct GenericRelaySignals {
    handle: SocketHandle,
    epoch: u64,
    writer_signal_rx: mpsc::Receiver<RelayWriterSignal>,
    reader_signal_rx: mpsc::Receiver<RelayReaderSignal>,
    local_finish_seen: Arc<std::sync::atomic::AtomicBool>,
    read_credit_probe_rx: watch::Receiver<RelayReadCredit>,
}

struct GenericRelayMonitor {
    signals: GenericRelaySignals,
    diag: RelayTaskDiag,
    close_timer: RelayCloseTimer,
    diag_tick: tokio::time::Interval,
    remote_read_probe_tick: tokio::time::Interval,
    published_hints: usize,
    hint_in_flight: bool,
}

impl GenericRelayMonitor {
    async fn new(signals: GenericRelaySignals) -> Self {
        let diag = RelayTaskDiag::default();
        let close_timer = RelayCloseTimer::new();
        let mut diag_tick =
            tokio::time::interval(std::time::Duration::from_secs(RELAY_LIVE_DIAG_SECS));
        diag_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        diag_tick.tick().await;
        let mut remote_read_probe_tick = tokio::time::interval(std::time::Duration::from_millis(
            RELAY_REMOTE_READ_PROBE_INTERVAL_MS,
        ));
        remote_read_probe_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        remote_read_probe_tick.tick().await;

        Self {
            signals,
            diag,
            close_timer,
            diag_tick,
            remote_read_probe_tick,
            published_hints: 0,
            hint_in_flight: false,
        }
    }

    async fn run(
        &mut self,
        back_tx: &crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    ) -> (&'static str, &'static str) {
        let Self {
            signals,
            diag,
            close_timer,
            diag_tick,
            remote_read_probe_tick,
            published_hints,
            hint_in_flight,
        } = self;
        let GenericRelaySignals {
            handle,
            epoch,
            writer_signal_rx,
            reader_signal_rx,
            local_finish_seen,
            read_credit_probe_rx,
        } = signals;
        let (handle, epoch) = (*handle, *epoch);
        let local_write_pressure_threshold = std::time::Duration::from_millis(5);
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
                        let Some(next_hints) = published_hints.checked_add(1) else {
                            *hint_in_flight = true;
                            break ("root", "hint_accounting_overflow");
                        };
                        *hint_in_flight = true;
                        match back_tx.try_send((
                            handle,
                            RelayEvent::AckDrainHint {
                                epoch,
                                gap_ms: hint.gap_ms,
                                remote_to_global_rx_bytes: hint.remote_to_global_rx_bytes,
                            },
                        )) {
                            Ok(()) => { *published_hints = next_hints; diag.note_ack_drain_hint_sent(); },
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
                        *hint_in_flight = false;
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
                // L2：双向静默超时 → 主动清理（防泄漏）。有活动的 select 分支会重启 loop → 重建 sleep。
                _ = close_timer.wait(), if close_timer.is_armed() => {
                    let timeout = close_timer.timeout().unwrap_or_default();
                    let reason = close_timer.terminal_reason();
                    println!("⏱️ relay {:?} lifecycle guard {}s expired reason={}", handle, timeout.as_secs(), reason);
                    break ("timer", reason);
                }
            }
        };

        (close_direction, close_reason)
    }
}

pub(super) mod continuous;
mod roles;
pub(super) mod thin;
pub(super) use roles::{FlowRoles, OriginalFailure};
pub(in crate::client_tun) use roles::{FailureClock, stamp_failure};

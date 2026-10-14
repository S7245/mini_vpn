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
use super::relay_reader::{ReaderRequest, run_relay_reader};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

/// The complete Generic launch inputs move as one context after the root has selected its engine.
/// This module preserves detached legacy task start and owns no task handle or flow state.
pub(in crate::client_tun) struct GenericLaunchRequest {
    pub(in crate::client_tun) engine: TcpRelayEngine,
    pub(in crate::client_tun) handle: SocketHandle,
    pub(in crate::client_tun) epoch: u64,
    pub(in crate::client_tun) stream: RelayStream,
    pub(in crate::client_tun) rx: mpsc::Receiver<RelayCommand>,
    pub(in crate::client_tun) back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
    pub(in crate::client_tun) read_credit_rx: watch::Receiver<RelayReadCredit>,
    pub(in crate::client_tun) downlink_backpressure: DownlinkBackpressureConfig,
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
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
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
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
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
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
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
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
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
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
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
    rx: mpsc::Receiver<RelayCommand>,
    back_tx: mpsc::Sender<(SocketHandle, RelayEvent)>,
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
            back_tx: back_tx.clone(),
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
        back_tx: &mpsc::Sender<(SocketHandle, RelayEvent)>,
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

// Root-owned roles for the Generic/Legacy engine. Gates are opened only after the
// original loop acknowledges publication. No detached outer task can certify joins.
use super::relay_reader::{
    PublishedReceipt, ReaderDiscardReceipt, ReaderOutcome, run_custodied_relay_reader_gated,
};
use super::relay_writer::{WriterDiscardReceipt, WriterOutcome, run_custodied_relay_writer_gated};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

enum Role<T> {
    Empty,
    Running(tokio::task::JoinHandle<T>),
    Joined(T),
    Failed(tokio::task::JoinError),
}
impl<T> Role<T> {
    fn poll(&mut self, cx: &mut Context<'_>) {
        if let Self::Running(task) = self {
            match Pin::new(task).poll(cx) {
                Poll::Ready(Ok(value)) => *self = Self::Joined(value),
                Poll::Ready(Err(error)) => *self = Self::Failed(error),
                Poll::Pending => {}
            }
        }
    }
}

struct MonitorOutcome {
    monitor: GenericRelayMonitor,
    reason: (&'static str, &'static str),
    panics: [Option<Box<dyn std::any::Any + Send>>; 2],
    closed: bool,
    drained: [bool; 2],
    discarded: [usize; 2],
    in_flight: bool,
    started: bool,
    back: Option<mpsc::Sender<(SocketHandle, RelayEvent)>>,
}

async fn run_custodied_monitor(
    mut monitor: GenericRelayMonitor,
    back: mpsc::Sender<(SocketHandle, RelayEvent)>,
    mut stop: oneshot::Receiver<()>,
    gate: oneshot::Receiver<()>,
) -> MonitorOutcome {
    let started = gate.await.is_ok();
    let mut work = Box::pin(monitor.run(&back));
    let result = if !started {
        Ok(("root", "gate_cancelled"))
    } else {
        tokio::select! {
            biased;
            _ = &mut stop => Ok(("root", "stop")),
            result = std::future::poll_fn(|cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
                Err(panic) => Poll::Ready(Err(panic)),
            }) => result,
        }
    };
    let drop_panic = catch_unwind(AssertUnwindSafe(|| drop(work))).err();
    let (reason, body_panic) = match result {
        Ok(reason) => (reason, None),
        Err(panic) => (("root", "monitor_panic"), Some(panic)),
    };
    MonitorOutcome {
        monitor,
        reason,
        panics: [body_panic, drop_panic],
        closed: false,
        drained: [false; 2],
        discarded: [0; 2],
        in_flight: false,
        started,
        back: Some(back),
    }
}

impl MonitorOutcome {
    fn dispose(&mut self, cx: &mut Context<'_>) {
        if self.in_flight {
            return;
        }
        self.in_flight = true;
        drop(self.back.take());
        let signals = &mut self.monitor.signals;
        if !self.closed {
            signals.writer_signal_rx.close();
            signals.reader_signal_rx.close();
            self.closed = true;
        }
        for _ in 0..32 {
            match signals.writer_signal_rx.poll_recv(cx) {
                Poll::Ready(Some(_)) => {
                    let Some(count) = self.discarded[0].checked_add(1) else {
                        return;
                    };
                    self.discarded[0] = count;
                }
                Poll::Ready(None) => {
                    self.drained[0] = true;
                    break;
                }
                Poll::Pending => break,
            }
        }
        for _ in 0..32 {
            match signals.reader_signal_rx.poll_recv(cx) {
                Poll::Ready(Some(_)) => {
                    let Some(count) = self.discarded[1].checked_add(1) else {
                        return;
                    };
                    self.discarded[1] = count;
                }
                Poll::Ready(None) => {
                    self.drained[1] = true;
                    break;
                }
                Poll::Pending => break,
            }
        }
        self.in_flight = false;
    }
}

pub(super) struct FlowRoles {
    failures: FailureClock,
    prepared: Option<GenericLaunchRequest>,
    writer: Role<WriterOutcome>,
    reader: Role<ReaderOutcome>,
    monitor: Role<MonitorOutcome>,
    starts: [Option<oneshot::Sender<()>>; 3],
    stops: [Option<oneshot::Sender<()>>; 3],
    pub(super) writer_receipt: Option<WriterDiscardReceipt>,
    pub(super) reader_receipt: Option<ReaderDiscardReceipt>,
    #[cfg(test)]
    publication_probe: Option<Arc<super::receive_window::PublicationProbe>>,
    started: bool,
    stopping: bool,
    preparation_interrupted: bool,
    pub(super) panic: Option<Box<dyn std::any::Any + Send>>,
}
impl FlowRoles {
    pub(super) fn new(request: GenericLaunchRequest) -> Self {
        Self {
            failures: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            prepared: Some(request),
            writer: Role::Empty,
            reader: Role::Empty,
            monitor: Role::Empty,
            starts: [None, None, None],
            stops: [None, None, None],
            writer_receipt: None,
            reader_receipt: None,
            #[cfg(test)]
            publication_probe: None,
            started: false,
            stopping: false,
            preparation_interrupted: false,
            panic: None,
        }
    }

    // Caller stores Self before invoking this preparation step.
    pub(super) fn prepare(&mut self, window: super::receive_window::ReaderWindow) {
        self.preparation_interrupted = true;
        #[cfg(test)]
        {
            self.publication_probe = Some(window.probe());
        }
        let Some(request) = self.prepared.take() else {
            return;
        };
        let GenericLaunchRequest {
            engine,
            handle,
            epoch,
            stream,
            rx,
            back_tx,
            read_credit_rx,
            ..
        } = request;
        if engine != TcpRelayEngine::Legacy {
            return;
        }
        let (reader, writer) = tokio::io::split(stream);
        let (writer_tx, writer_rx) = mpsc::channel(RELAY_CHANNEL_CAPACITY);
        let (reader_tx, reader_rx) = mpsc::channel(RELAY_CHANNEL_CAPACITY);
        let finish = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let signals = GenericRelaySignals {
            handle,
            epoch,
            writer_signal_rx: writer_rx,
            reader_signal_rx: reader_rx,
            local_finish_seen: finish.clone(),
            read_credit_probe_rx: read_credit_rx.clone(),
        };
        let now = tokio::time::Instant::now();
        let diag_period = std::time::Duration::from_secs(RELAY_LIVE_DIAG_SECS);
        let probe_period = std::time::Duration::from_millis(RELAY_REMOTE_READ_PROBE_INTERVAL_MS);
        let mut diag_tick = tokio::time::interval_at(now + diag_period, diag_period);
        diag_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut remote_read_probe_tick = tokio::time::interval_at(now + probe_period, probe_period);
        remote_read_probe_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let monitor = GenericRelayMonitor {
            signals,
            diag: RelayTaskDiag::default(),
            close_timer: RelayCloseTimer::new(),
            diag_tick,
            remote_read_probe_tick,
            published_hints: 0,
            hint_in_flight: false,
        };
        let (start, gate) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        self.starts[0] = Some(start);
        self.stops[0] = Some(stop);
        let writer_failures = Some(self.failures.clone());
        self.writer = Role::Running(tokio::spawn(async move {
            run_custodied_relay_writer_gated(
                handle,
                writer,
                rx,
                writer_tx,
                stop_rx,
                Some(gate),
                writer_failures,
            )
            .await
        }));
        let (start, gate) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        self.starts[1] = Some(start);
        self.stops[1] = Some(stop);
        let reader_request = ReaderRequest {
            handle,
            epoch,
            remote_reader: reader,
            back_tx: back_tx.clone(),
            read_credit_rx,
            signal_tx: reader_tx,
            local_finish_seen: finish,
            stop_rx,
        };
        let reader_failures = Some(self.failures.clone());
        self.reader = Role::Running(tokio::spawn(async move {
            run_custodied_relay_reader_gated(
                reader_request,
                Some(gate),
                reader_failures,
                Some(window),
            )
            .await
        }));
        let (start, gate) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        self.starts[2] = Some(start);
        self.stops[2] = Some(stop);
        self.monitor = Role::Running(tokio::spawn(async move {
            run_custodied_monitor(monitor, back_tx, stop_rx, gate).await
        }));
        self.preparation_interrupted = false;
    }

    pub(super) fn start(&mut self) {
        if self.started {
            return;
        }
        self.preparation_interrupted = true;
        for gate in &mut self.starts {
            if let Some(gate) = gate.take() {
                let _ = gate.send(());
            }
        }
        self.started = true;
        self.preparation_interrupted = false;
    }

    pub(super) fn stop(&mut self) {
        self.stopping = true;
        for stop in &mut self.stops {
            if let Some(stop) = stop.take() {
                let _ = stop.send(());
            }
        }
        if !self.started {
            for gate in &mut self.starts {
                drop(gate.take());
            }
        }
    }

    fn poll_handles(&mut self, cx: &mut Context<'_>) {
        self.writer.poll(cx);
        self.reader.poll(cx);
        self.monitor.poll(cx);
    }

    /// A poisoned provider cannot be entered again; actual role joins still can.
    /// Retain outcomes untouched (no remainder disposal) under this role owner.
    pub(super) fn poll_fatal_joins(&mut self, cx: &mut Context<'_>) {
        if self.panic.is_some() {
            return;
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            self.stop();
            self.poll_handles(cx);
            if let Role::Joined(monitor) = &mut self.monitor {
                if !monitor.closed {
                    monitor.monitor.signals.writer_signal_rx.close();
                    monitor.monitor.signals.reader_signal_rx.close();
                    monitor.closed = true;
                }
            }
        })) {
            self.panic = Some(payload);
        }
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>, producer_stopped: bool) {
        self.poll_handles(cx);
        if self.stopping {
            // Close real notification receivers first: a blocked terminal sender
            // must be able to return before its actual JoinHandle can complete.
            if let Role::Joined(monitor) = &mut self.monitor {
                monitor.dispose(cx);
            }
            if !producer_stopped {
                return;
            }
            if let Role::Joined(writer) = &mut self.writer {
                if let Poll::Ready(receipt) = writer.remainder.poll_discard(cx, 32) {
                    self.writer_receipt = Some(receipt);
                }
            }
            if let Role::Joined(reader) = &mut self.reader {
                self.reader_receipt = reader.remainder.discard_local();
            }
        }
    }

    pub(super) fn closed(&self) -> Option<(&'static str, &'static str)> {
        match &self.monitor {
            Role::Joined(outcome) => Some(outcome.reason),
            Role::Failed(_) => Some(("root", "monitor_join_failed")),
            _ if self.quarantined() => Some(("root", "role_quarantined")),
            _ => None,
        }
    }

    pub(super) fn published(&self) -> Option<PublishedReceipt> {
        match &self.reader {
            Role::Joined(outcome) => Some(outcome.remainder.published()),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn test_queued_data(&self) -> Option<usize> {
        self.publication_probe.as_ref()?.queued_lower_bound()
    }

    #[cfg(test)]
    pub(super) fn test_outstanding_data(&self) -> Option<usize> {
        self.publication_probe.as_ref()?.outstanding_lower_bound()
    }

    pub(super) fn read_total(&self) -> Option<usize> {
        match &self.reader {
            Role::Joined(outcome) => outcome.remainder.read_total(),
            _ => None,
        }
    }

    pub(super) fn published_hints(&self) -> Option<usize> {
        match &self.monitor {
            Role::Joined(outcome) => Some(outcome.monitor.published_hints),
            _ => None,
        }
    }

    pub(super) fn settled(&self) -> bool {
        !self.quarantined()
            && self.writer_receipt.is_some()
            && self.reader_receipt.is_some()
            && matches!(&self.monitor, Role::Joined(outcome) if outcome.drained == [true; 2])
    }

    pub(super) fn quarantined(&self) -> bool {
        self.preparation_interrupted
            || self.failures.load(std::sync::atomic::Ordering::SeqCst) == usize::MAX
            || (self.started
                && (matches!(&self.writer, Role::Joined(outcome) if !outcome.started)
                    || matches!(&self.reader, Role::Joined(outcome) if !outcome.started)
                    || matches!(&self.monitor, Role::Joined(outcome) if !outcome.started)))
            || self.panic.is_some()
            || matches!(self.writer, Role::Empty | Role::Failed(_))
            || matches!(self.reader, Role::Empty | Role::Failed(_))
            || matches!(self.monitor, Role::Empty | Role::Failed(_))
            || matches!(&self.writer, Role::Joined(outcome) if outcome.panics.iter().any(Option::is_some) || outcome.remainder.quarantined())
            || matches!(&self.reader, Role::Joined(outcome) if outcome.panics.iter().any(Option::is_some) || outcome.notification.in_flight || outcome.remainder.quarantined())
            || matches!(&self.monitor, Role::Joined(outcome) if outcome.panics.iter().any(Option::is_some) || outcome.in_flight || outcome.monitor.hint_in_flight)
    }

    pub(super) fn join_count(&self) -> usize {
        usize::from(matches!(self.writer, Role::Joined(_)))
            + usize::from(matches!(self.reader, Role::Joined(_)))
            + usize::from(matches!(self.monitor, Role::Joined(_)))
    }

    pub(super) fn running_count(&self) -> usize {
        usize::from(matches!(self.writer, Role::Running(_)))
            + usize::from(matches!(self.reader, Role::Running(_)))
            + usize::from(matches!(self.monitor, Role::Running(_)))
    }

    pub(super) fn take_error(&mut self) -> Option<std::io::Error> {
        let mut first = None;
        if let Role::Joined(writer) = &self.writer {
            if writer.result.failure.is_some() {
                first = writer.error_order[0].map(|order| (order, 0));
            }
            if writer.result.shutdown_failure.is_some() {
                if let Some(order) = writer.error_order[1] {
                    if first.is_none_or(|(earlier, _)| order < earlier) {
                        first = Some((order, 1));
                    }
                }
            }
        }
        if let Role::Joined(reader) = &self.reader {
            if reader.failure.is_some() {
                if let Some(order) = reader.error_order {
                    if first.is_none_or(|(earlier, _)| order < earlier) {
                        first = Some((order, 2));
                    }
                }
            }
        }
        match first?.1 {
            0 => match &mut self.writer {
                Role::Joined(value) => value.result.failure.take().map(|failure| failure.source),
                _ => None,
            },
            1 => match &mut self.writer {
                Role::Joined(value) => value.result.shutdown_failure.take(),
                _ => None,
            },
            _ => match &mut self.reader {
                Role::Joined(value) => value.failure.take(),
                _ => None,
            },
        }
    }
}

// Only failure paths allocate an ordinal; DATA processing never touches this clock.
pub(super) type FailureClock = Arc<std::sync::atomic::AtomicUsize>;
pub(super) fn stamp_failure(clock: &Option<FailureClock>) -> Option<usize> {
    clock.as_ref().map(|clock| {
        clock
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |value| value.checked_add(1),
            )
            .unwrap_or(usize::MAX)
    })
}

#[cfg(test)]
mod tests;

//! Compatibility composition and persistent control custody of the original DATA loop.
//! The host session owns its saved outer future; the DATA future borrows its device.
//! Control handles and observers never own either future. Device settlement is not flow or TUIC settlement;
//! public startup remains on the legacy path until those owners are composed here.
//! Custody tests live at `tests/compatibility_relay_custody.rs` via the private seam below.

#[cfg(test)]
use super::ListenerCapacityRejection;
use super::{
    CONTINUOUS_TCP_RELAY_QUEUE_BYTES, D4_STALLED_READ_SERVICE_HOLD_MS, ListenerCapacityProfile,
    MetricsSink, NoopSink, THIN_TCP_RELAY_STAGING_CAPACITY, TunRuntimeConfig, UpstreamKind,
    create_tun_device, d4_stalled_read_service_enabled, d5_capacity_backpressure_enabled,
    format_h10d16_startup_line, pressure_tun_rx_drain_budget, run_borrowed_event_loop,
    select_upstream_kind, tun_egress_capacity_bytes, tun_egress_safe_flush_threshold,
    tx_queue_credit_spend_threshold, tx_queue_flush_threshold, tx_queue_pause_threshold,
};
use crate::device::{DeviceShutdownState, TunIo, VirtualTunDevice};
use crate::failover::FailoverUpstream;
use crate::loop_profiler::LoopProfiler;
use crate::metrics::Metrics;
use crate::owned_upstream::{LegacyOwnedUpstream, OwnedUpstream, UdpDownlinkSource};
use crate::reality_upstream::RealityUpstream;
use crate::shared::errors::{FailureOrigin, FailurePhase, FailureResource, RetainedFailure};
use crate::tuic::{TuicClientConfig, TuicUpstream};
use crate::upstream::{DatagramUpstream, ProxyUpstream};
use crate::upstream::{NativeTcpRelayEngine, RelayFamilyPolicy, TcpRelayEngine};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use tokio::sync::Notify;
use tokio::sync::mpsc;

#[cfg(all(test, feature = "harness"))]
use super::legacy_event_loop::RelayEventTestBridge;

mod flow_custody;
#[cfg(test)]
pub(in crate::client_tun) use flow_custody::{ReadyReadRequest, drain_ready_remote_reads};

pub(in crate::client_tun) use flow_custody::{
    FatalPhysicalTurn, FatalStep, FlowAdmission, FlowMirror, GenericLaunchRequest, LocalReset,
    RearmGate, RearmIntent, RemotePayloadRequest, flush_tx_and_release_downlink_permits,
    gate_legacy_rearm, launch_generic_relay, legacy_bare_rearm_from_loop,
    legacy_reasoned_rearm_from_loop, legacy_reasoned_rearm_with_gate,
    legacy_reasoned_rearm_with_snapshot_and_gate, reap_dead_slots_from_loop, run_relay,
    run_relay_ordered_egress_permit, run_relay_thin, run_relay_with_engine, run_relay_writer,
    send_remote_payload_batch_to_main,
};

#[cfg(test)]
pub(in crate::client_tun) use flow_custody::{
    reap_dead_slots, should_defer_owned_terminal_reap, should_reap_slot,
    should_reap_slot_with_snapshot,
};

#[cfg(test)]
#[path = "tests/compatibility_relay_custody.rs"]
mod tests;

pub(in crate::client_tun::compatibility_runtime) use flow_custody::OpenCustody;
pub(super) use flow_custody::{HandshakeNotice, OpenDispatch, OpenEvent};
#[cfg(all(test, feature = "harness"))]
pub(in crate::client_tun::compatibility_runtime) use flow_custody::{
    OpenOutcome, OpenPhase, OpenSlot, open_origin,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionDeviceState {
    Running,
    Stopping,
    Incomplete,
    DeviceSettled,
    DeviceFailedSettled,
}

#[derive(Default)]
struct SessionLedger {
    first_egress_io: Option<Arc<std::io::Error>>,
    egress_failures: u64,
    first_failure: Option<RetainedFailure>,
    additional_failures: u64,
    shutdown_deadline: Option<tokio::time::Instant>,
    quarantined: bool,
}

#[derive(Default)]
struct SessionStop {
    stopping: AtomicBool,
    stop_wake: Notify,
}

impl SessionStop {
    fn request_stop(&self) {
        self.stopping.store(true, Ordering::Release);
        self.stop_wake.notify_waiters();
    }

    async fn stopped(&self) {
        loop {
            let wait = self.stop_wake.notified();
            tokio::pin!(wait);
            wait.as_mut().enable();
            if self.stopping.load(Ordering::Acquire) {
                return;
            }
            wait.await;
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StartupProfileObservation {
    pub(super) profile: ListenerCapacityProfile,
    pub(super) effective_rx_bytes: usize,
    pub(super) effective_tx_bytes: usize,
    pub(super) disposition: StartupProfileDisposition,
    pub(super) listener_owned: ListenerOwnedCounts,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StartupProfileDisposition {
    Accepted,
    Rejected(ListenerCapacityRejection),
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ListenerOwnedCounts {
    pub(super) sockets: usize,
    pub(super) socket_ctxs: usize,
    pub(super) ports: usize,
    pub(super) total: usize,
}

#[derive(Default)]
pub(super) struct SessionControl {
    stop: Arc<SessionStop>,
    ledger: Mutex<SessionLedger>,
    opens: Mutex<OpenCustody>,
    announced: [AtomicU64; 2],
    open_started: AtomicBool,
    control_remaining: AtomicUsize,
    driver_complete: AtomicBool,
    state_changed: Notify,
    #[cfg(test)]
    startup_profile_observation: Mutex<Option<StartupProfileObservation>>,
    #[cfg(test)]
    device_settlement_observed: AtomicBool,
    #[cfg(all(test, feature = "harness"))]
    relay_event_test_bridge: Mutex<Option<std::sync::Weak<RelayEventTestBridge>>>,
}

impl SessionControl {
    #[cfg(test)]
    pub(super) fn record_startup_profile_observation(
        &self,
        observation: StartupProfileObservation,
    ) {
        *self
            .startup_profile_observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(observation);
    }

    #[cfg(test)]
    pub(super) fn startup_profile_observation(&self) -> Option<StartupProfileObservation> {
        *self
            .startup_profile_observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(all(test, feature = "harness"))]
    fn install_relay_event_test_bridge(&self, bridge: std::sync::Weak<RelayEventTestBridge>) {
        *self
            .relay_event_test_bridge
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(bridge);
    }

    #[cfg(all(test, feature = "harness"))]
    pub(in crate::client_tun) fn relay_event_test_bridge(
        &self,
    ) -> Option<Arc<RelayEventTestBridge>> {
        self.relay_event_test_bridge
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|bridge| bridge.upgrade())
    }

    fn ledger(&self) -> std::sync::MutexGuard<'_, SessionLedger> {
        self.ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn origin(phase: FailurePhase) -> FailureOrigin {
        FailureOrigin {
            phase,
            resource: FailureResource::Session,
            resource_id: 0,
            attempt_id: None,
        }
    }

    fn record(&self, failure: RetainedFailure) {
        let mut ledger = self.ledger();
        if ledger.first_failure.is_none() {
            ledger.first_failure = Some(failure);
        } else {
            ledger.additional_failures = ledger.additional_failures.saturating_add(1);
        }
    }

    fn request_stop(&self) {
        self.ledger().shutdown_deadline.get_or_insert_with(|| {
            tokio::time::Instant::now() + std::time::Duration::from_secs(15)
        });
        self.stop.request_stop();
    }

    async fn stopped(&self) {
        self.stop.stopped().await;
    }

    pub(in crate::client_tun) fn note_loop_panic(&self, phase: FailurePhase) {
        self.record(RetainedFailure::opaque_panic(Self::origin(phase)));
        self.request_stop();
    }

    pub(in crate::client_tun) fn note_loop_error(&self, error: &std::io::Error) {
        self.record(RetainedFailure::from_io_ref(
            Self::origin(FailurePhase::DeviceRead),
            error,
        ));
        self.request_stop();
    }

    fn quarantine(&self, phase: FailurePhase, payload: Box<dyn std::any::Any + Send>) {
        self.record(RetainedFailure::from_panic(Self::origin(phase), payload));
        self.ledger().quarantined = true;
    }
}

/// The host's one live-or-quarantined execution slot; it cannot be replaced.
/// Keep this owner alive while driving or observing it. Dropping the entire
/// host is forced termination, not asynchronous shutdown. Private tests use
/// the same driver; wiring this lifetime into public startup remains required.
struct ClientTunSession<F> {
    driver: Pin<Box<F>>,
    control: Arc<SessionControl>,
    last_reported: SessionDeviceState,
    handle_issued: bool,
}

/// One control handle, with no driver, device, queue, or task ownership.
struct ClientTunHandle {
    control: Arc<SessionControl>,
}

impl SessionControl {
    fn state(&self) -> SessionDeviceState {
        let complete = self.driver_complete.load(Ordering::Acquire);
        if !complete && !self.stop.stopping.load(Ordering::Acquire) {
            return SessionDeviceState::Running;
        }
        let ledger = self.ledger();
        if complete {
            if ledger.first_failure.is_some() {
                SessionDeviceState::DeviceFailedSettled
            } else {
                SessionDeviceState::DeviceSettled
            }
        } else if ledger.quarantined {
            SessionDeviceState::Incomplete
        } else {
            SessionDeviceState::Stopping
        }
    }
}

impl ClientTunHandle {
    fn request_stop(&self) {
        self.control.request_stop();
    }

    /// Observation alone cannot poll or release the host's saved driver.
    async fn observe_shutdown(&mut self) -> SessionDeviceState {
        self.request_stop();
        let deadline = self.control.ledger().shutdown_deadline;
        let observed = async {
            loop {
                let changed = self.control.state_changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                match self.control.state() {
                    SessionDeviceState::Running | SessionDeviceState::Stopping => {
                        changed.await;
                    }
                    state => return state,
                }
            }
        };
        match deadline {
            Some(deadline) => tokio::select! {
                biased;
                state = observed => state,
                _ = tokio::time::sleep_until(deadline) => SessionDeviceState::Incomplete,
            },
            None => SessionDeviceState::Incomplete,
        }
    }
}

impl Drop for ClientTunHandle {
    fn drop(&mut self) {
        self.request_stop();
    }
}

impl<F: Future<Output = ()>> ClientTunSession<F> {
    /// A second handle cannot replace the sole control lifetime or Q slot.
    fn take_handle(&mut self) -> Option<ClientTunHandle> {
        if self.handle_issued {
            return None;
        }
        self.handle_issued = true;
        Some(ClientTunHandle {
            control: Arc::clone(&self.control),
        })
    }

    fn request_stop(&self) {
        self.control.request_stop();
    }

    fn state(&self) -> SessionDeviceState {
        self.control.state()
    }

    fn poll_driver(&mut self, cx: &mut std::task::Context<'_>) -> SessionDeviceState {
        if !self.control.driver_complete.load(Ordering::Acquire)
            && self.driver.as_mut().poll(cx).is_ready()
        {
            self.control.driver_complete.store(true, Ordering::Release);
        }
        let state = self.state();
        if self.last_reported != state {
            self.last_reported = state;
            self.control.state_changed.notify_waiters();
        }
        state
    }

    /// The host drives this borrowed future until actual completion. Q and
    /// observation timeout never finish it or surrender the occupied slot.
    async fn drive_until_settled(&mut self) -> SessionDeviceState {
        std::future::poll_fn(|cx| {
            let state = self.poll_driver(cx);
            if self.control.driver_complete.load(Ordering::Acquire) {
                Poll::Ready(state)
            } else {
                Poll::Pending
            }
        })
        .await
    }

    async fn run(&mut self) -> SessionDeviceState {
        std::future::poll_fn(|cx| match self.poll_driver(cx) {
            SessionDeviceState::Running | SessionDeviceState::Stopping => Poll::Pending,
            state => Poll::Ready(state),
        })
        .await
    }

    async fn observe_shutdown(&mut self) -> SessionDeviceState {
        self.request_stop();
        let deadline = self.control.ledger().shutdown_deadline;
        match deadline {
            Some(deadline) => tokio::select! {
                biased;
                state = self.run() => state,
                _ = tokio::time::sleep_until(deadline) => SessionDeviceState::Incomplete,
            },
            None => SessionDeviceState::Incomplete,
        }
    }
}

impl<F> Drop for ClientTunSession<F> {
    fn drop(&mut self) {
        self.control.request_stop();
        if !self.control.driver_complete.load(Ordering::Acquire) {
            self.control.record(RetainedFailure::from_io(
                SessionControl::origin(FailurePhase::SessionLoopDrop),
                std::io::Error::from(std::io::ErrorKind::Interrupted),
            ));
            self.control.ledger().quarantined = true;
            self.control.state_changed.notify_waiters();
        }
    }
}

fn prepare_client_tun_session<D, U, M>(
    device: D,
    upstream: Arc<U>,
    downlink_rx: mpsc::Receiver<Vec<u8>>,
    runtime_config: TunRuntimeConfig,
    metrics_handle: Arc<Metrics>,
    metrics: M,
) -> ClientTunSession<impl Future<Output = ()>>
where
    D: TunIo,
    U: ProxyUpstream + DatagramUpstream + 'static,
    M: MetricsSink,
{
    let control = Arc::new(SessionControl {
        opens: Mutex::new(
            if runtime_config.listener_capacity_profile == ListenerCapacityProfile::F4096 {
                OpenCustody::with_flows()
            } else {
                OpenCustody::default()
            },
        ),
        control_remaining: AtomicUsize::new(32),
        ..SessionControl::default()
    });
    let retained = Arc::clone(&control);
    let mut device = egress::SessionEgress::new(device, Arc::clone(&control));
    let driver = Box::pin(async move {
        {
            let mut inner = Box::pin(run_borrowed_event_loop(
                &mut device,
                Arc::new(LegacyOwnedUpstream::new(upstream)),
                UdpDownlinkSource::legacy_raw(downlink_rx),
                runtime_config,
                metrics_handle,
                metrics,
                Some(Arc::clone(&retained)),
            ));
            let outcome = tokio::select! {
                biased;
                _ = retained.stopped(), if !retained.flow_mode() => None,
                result = std::future::poll_fn(|cx| {
                    match catch_unwind(AssertUnwindSafe(|| inner.as_mut().poll(cx))) {
                        Ok(Poll::Pending) => Poll::Pending,
                        Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
                        Err(payload) => Poll::Ready(Err(payload)),
                    }
                }) => Some(result),
            };
            match outcome {
                Some(Ok(Err(error))) => retained.record(RetainedFailure::from_io(
                    SessionControl::origin(FailurePhase::DeviceRead),
                    error,
                )),
                Some(Err(payload)) => retained.record(RetainedFailure::from_panic(
                    SessionControl::origin(FailurePhase::SessionLoop),
                    payload,
                )),
                _ => {}
            }
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(inner))) {
                retained.record(RetainedFailure::from_panic(
                    SessionControl::origin(FailurePhase::SessionLoopDrop),
                    payload,
                ));
            }
        }
        retained.request_stop();
        let mut stop_failed = false;
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| device.request_stop())) {
            retained.quarantine(FailurePhase::DeviceStop, payload);
            stop_failed = true;
        }
        let device_settlement = async {
            if stop_failed {
                return;
            }
            let shutdown = std::future::poll_fn(|cx| {
                match catch_unwind(AssertUnwindSafe(|| device.poll_shutdown(cx))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(result)) => Poll::Ready(Some(result)),
                    Err(payload) => {
                        retained.quarantine(FailurePhase::DeviceShutdown, payload);
                        Poll::Ready(None)
                    }
                }
            })
            .await;
            if let Some(mut shutdown) = shutdown {
                #[cfg(test)]
                retained.note_device_settled();
                if let Some(failure) = shutdown.take_failure() {
                    retained.record(failure);
                } else if shutdown.state() == DeviceShutdownState::FailedSettled {
                    retained.record(RetainedFailure::from_io(
                        SessionControl::origin(FailurePhase::DeviceShutdown),
                        std::io::Error::from(
                            shutdown.io_kind().unwrap_or(std::io::ErrorKind::Other),
                        ),
                    ));
                }
                let mut ledger = retained.ledger();
                ledger.additional_failures = ledger
                    .additional_failures
                    .saturating_add(shutdown.take_additional_failures());
            }
        };
        tokio::join!(retained.settle_opens(), device_settlement);
        let quarantined = retained.ledger().quarantined;
        if quarantined {
            std::future::pending::<()>().await;
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(device))) {
            retained.record(RetainedFailure::from_panic(
                SessionControl::origin(FailurePhase::DeviceDrop),
                payload,
            ));
        }
    });
    ClientTunSession {
        driver,
        control,
        last_reported: SessionDeviceState::Running,
        handle_issued: false,
    }
}

/// Knife16 branch-by-abstraction entry. The TUN/smoltcp owner consumes only
/// protocol-owned TCP/UDP facts; legacy TUIC/REALITY behavior is supplied by
/// [`LegacyOwnedUpstream`] and [`UdpDownlinkSource::legacy_raw`].
pub(crate) async fn run_owned_event_loop<D, U, M>(
    mut device: D,
    upstream: Arc<U>,
    udp_downlink: UdpDownlinkSource,
    runtime_config: TunRuntimeConfig,
    metrics_handle: Arc<Metrics>,
    metrics: M,
) where
    D: TunIo,
    U: OwnedUpstream + 'static,
    M: MetricsSink,
{
    if let Err(error) = run_borrowed_event_loop(
        &mut device,
        upstream,
        udp_downlink,
        runtime_config,
        metrics_handle,
        metrics,
        None,
    )
    .await
    {
        println!("TUN ingress terminated: {error}");
    }
}

/// 共享主循环：生产（真 utun + 真 TUIC）与并发压测 harness（内存回环 device + mock 上游）共用。
///
/// 中文要点：泛型 over [`TunIo`]（设备接缝）与 [`MetricsSink`]（分段插桩）。所有 smoltcp 装置
/// （sockets / iface / registry / fake DNS / fake_pool / assoc_table）在此内部构造，与生产逐字一致，
/// 使 harness 也忠实地走同一套 SYN inspector / DNS / relay 调度路径。
pub async fn run_event_loop<D, U, M>(
    device: D,
    upstream: Arc<U>,
    tuic_downlink_rx: mpsc::Receiver<Vec<u8>>,
    runtime_config: TunRuntimeConfig,
    // 刀11：进程级数据面计数/gauge 句柄（与计时导向的 `metrics: M` 正交）。run_event_loop 写
    // dns_*/relays_spawned + 30s tick 发布 gauge；同一 Arc 也被 TuicUpstream::start_udp clone（写 UDP 计数）。
    metrics_handle: Arc<Metrics>,
    metrics: M,
) where
    D: TunIo,
    U: ProxyUpstream + DatagramUpstream + 'static,
    M: MetricsSink,
{
    run_owned_event_loop(
        device,
        Arc::new(LegacyOwnedUpstream::new(upstream)),
        UdpDownlinkSource::legacy_raw(tuic_downlink_rx),
        runtime_config,
        metrics_handle,
        metrics,
    )
    .await;
}

/// 刀12：按 `profile_loop` 在 `NoopSink`（默认零开销）与 `LoopProfiler`（🔬 归因）间**单态化二选一**
/// 装进共享 [`run_event_loop`]。分支收口一处，三上游 arm 各一行调用、零重复。
///
/// 中文要点：`profile_loop`（Copy bool）作首参先求值，`runtime_config` 作后参再 move——左到右求值序
/// 保证读字段不与 move 冲突。默认（`false`）分支逐字等价旧 `run_event_loop(.., NoopSink)`，零回归。
async fn run_event_loop_sinked<D, U>(
    profile_loop: bool,
    device: D,
    upstream: Arc<U>,
    downlink_rx: mpsc::Receiver<Vec<u8>>,
    runtime_config: TunRuntimeConfig,
    metrics: Arc<Metrics>,
) where
    D: TunIo,
    U: ProxyUpstream + DatagramUpstream + 'static,
{
    if profile_loop {
        run_event_loop(
            device,
            upstream,
            downlink_rx,
            runtime_config,
            metrics,
            LoopProfiler::new(),
        )
        .await;
    } else {
        run_event_loop(
            device,
            upstream,
            downlink_rx,
            runtime_config,
            metrics,
            NoopSink,
        )
        .await;
    }
}

/// 生产入口：建真 utun + 真 TUIC 上游，然后跑共享的 [`run_event_loop`]。
///
/// 中文要点：knife1 起把主循环抽成 `run_event_loop`（泛型 over [`TunIo`] 设备 + [`super::MetricsSink`]），
/// 生产与并发压测 harness **跑同一份循环代码**。本薄壳只负责构造真依赖；循环逻辑零回归。
pub async fn start_tun_proxy() {
    let relay_policy = Arc::new(RelayFamilyPolicy::from_env());
    let runtime_config =
        match TunRuntimeConfig::from_env_with_relay_policy(Arc::clone(&relay_policy)) {
            Ok(config) => config,
            Err(e) => {
                println!("加载 TUN 运行时配置失败: {e}");
                return;
            }
        };
    println!(
        "🚀 TUN runtime started with pool_size={}, tun_mtu={}, tun_tx_queue_len_estimate={}",
        runtime_config.listener.pool_size, runtime_config.tun_mtu, runtime_config.tun_tx_queue_len
    );
    // 刀11：数据面 📊 快照周期（MINI_VPN_METRICS_SECS 可调；acceptance 设 5 即秒级看指标）。
    println!(
        "📊 数据面可观测性：快照周期 = {}s（MINI_VPN_METRICS_SECS 可调）",
        runtime_config.metrics_secs
    );
    // 刀12：主循环 profiler（默认关；MINI_VPN_PROFILE_LOOP=1 开 → 打 🔬 归因行量化 #4 vs #3）。
    if runtime_config.profile_loop {
        println!(
            "🔬 主循环 profiler 已启用（loop-active/poll/relay 占比，每 {}s；MINI_VPN_PROFILE_LOOP）",
            runtime_config.metrics_secs
        );
    }
    println!(
        "🧯 TCP 下行背压: high={}B low={}B egress_safe_flush={}B egress_capacity={}B（auto 按 MINI_VPN_TUN_MTU/MINI_VPN_TUN_TX_QUEUE_LEN 估算；MINI_VPN_DOWNLINK_BACKPRESSURE_* 可显式覆盖）",
        runtime_config.downlink_backpressure.high_bytes,
        runtime_config.downlink_backpressure.low_bytes,
        tun_egress_safe_flush_threshold(runtime_config.tun_mtu, runtime_config.tun_tx_queue_len),
        tun_egress_capacity_bytes(runtime_config.tun_mtu, runtime_config.tun_tx_queue_len)
    );
    println!(
        "🚰 TCP 下行 flush budget: max={}B（MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES 可调）",
        runtime_config.downlink_flush_max_bytes
    );
    println!(
        "🚦 TCP 下行 egress pacing: immediate={}B/tick tx_queue_flush_high={}B tx_queue_credit_high={}B tx_queue_pause_high={}B（MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES 可调；pending backlog 在本地 egress 压力低于 high 时强制 immediate flush）",
        runtime_config.downlink_egress_immediate_bytes,
        tx_queue_flush_threshold(runtime_config.downlink_backpressure),
        tx_queue_credit_spend_threshold(runtime_config.downlink_backpressure),
        tx_queue_pause_threshold(runtime_config.downlink_backpressure)
    );
    println!(
        "🧱 TCP socket buffers: rx={}B tx={}B receive_window_limit={}（MINI_VPN_TCP_*_BUFFER_BYTES 可调；H10d16 limit 固定由 EndpointWindowV1 10ms bound 派生）",
        runtime_config.tcp_socket_buffers.rx_bytes,
        runtime_config.tcp_socket_buffers.tx_bytes,
        runtime_config
            .tcp_socket_buffers
            .receive_window_limit
            .map_or_else(|| "none".to_string(), |bytes| format!("{bytes}B"))
    );
    let adaptive_tun_rx_pressure_budget =
        pressure_tun_rx_drain_budget(runtime_config.downlink_backpressure, runtime_config.tun_mtu);
    println!(
        "🧺 TUN RX drain budget: {} packets/pass（0=仅 pressure-adaptive；pressure-adaptive={} packets near tx_queue_credit_high；MINI_VPN_TUN_RX_DRAIN_BUDGET>0 显式覆盖）",
        runtime_config.tun_rx_drain_budget, adaptive_tun_rx_pressure_budget
    );
    println!(
        "🧪 TUN RX active-flow ACK/window service: {}ms（0=disabled；MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS 可覆盖）",
        runtime_config.tun_rx_active_flow_timer_ms
    );
    println!(
        "🧪 bounded global_rx receive window: {}（MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW=1 仅用于 Knife14cg A/B）",
        if runtime_config.bounded_global_rx_receive_window {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "🧪 buffered downlink: {} per_flow(low/high/hard)={}/{}/{}B global(low/high/hard)={}/{}/{}B（MINI_VPN_BUFFERED_DOWNLINK=1）",
        if runtime_config.buffered_downlink.enabled {
            "enabled"
        } else {
            "disabled"
        },
        runtime_config.buffered_downlink.per_flow_low_bytes,
        runtime_config.buffered_downlink.per_flow_high_bytes,
        runtime_config.buffered_downlink.per_flow_hard_bytes,
        runtime_config.buffered_downlink.global_low_bytes,
        runtime_config.buffered_downlink.global_high_bytes,
        runtime_config.buffered_downlink.global_hard_bytes,
    );
    println!(
        "🧪 thin TCP relay staging: {} capacity={} messages（MINI_VPN_THIN_TCP_RELAY=1）",
        if runtime_config.relay_policy.generic_engine() == TcpRelayEngine::ThinStaging {
            "enabled"
        } else {
            "disabled"
        },
        THIN_TCP_RELAY_STAGING_CAPACITY
    );
    println!(
        "🧪 continuous TCP relay pump: {} queue={}B（MINI_VPN_CONTINUOUS_TCP_RELAY=1；优先级高于 thin staging）",
        if runtime_config.relay_policy.generic_engine() == TcpRelayEngine::ContinuousPump {
            "enabled"
        } else {
            "disabled"
        },
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES
    );
    println!(
        "🧪 D2.3 permit TCP relay pump: {} queue={}B（MINI_VPN_D2_PERMIT_TCP_RELAY=1；优先级高于 continuous/thin）",
        if runtime_config.relay_policy.generic_engine() == TcpRelayEngine::PermitPump {
            "enabled"
        } else {
            "disabled"
        },
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES
    );
    println!(
        "🧪 D3 TCP/TUN egress actor: {} adaptive_credit={} legacy_credit={} self_wake={}（MINI_VPN_D3_EGRESS_ACTOR=1；MINI_VPN_D3_EGRESS_ACTOR_ADAPTIVE_CREDIT=1 / MINI_VPN_D3_EGRESS_ACTOR_LEGACY_CREDIT=1 / MINI_VPN_D3_EGRESS_ACTOR_SELF_WAKE=1 仅诊断）",
        if runtime_config.d3_egress_actor {
            "enabled"
        } else {
            "disabled"
        },
        runtime_config.d3_egress_actor_adaptive_credit,
        runtime_config.d3_egress_actor_legacy_credit,
        runtime_config.d3_egress_actor_self_wake
    );
    println!(
        "🧪 D4 stalled-read ACK/window service: {} hold={}ms（MINI_VPN_D4_STALLED_READ_SERVICE=1；requires D3 actor）",
        if d4_stalled_read_service_enabled(&runtime_config) {
            "enabled"
        } else {
            "disabled"
        },
        D4_STALLED_READ_SERVICE_HOLD_MS
    );
    println!(
        "🧪 D5 socket-capacity backpressure: {}（MINI_VPN_D5_CAPACITY_BACKPRESSURE=1；requires D3 actor）",
        if d5_capacity_backpressure_enabled(&runtime_config) {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "🧪 D6 native egress permit: {}（MINI_VPN_D6_NATIVE_EGRESS_PERMIT=1；requires native TUIC pump + D3/D5 for acceptance）",
        if runtime_config.relay_policy.native_engine() == NativeTcpRelayEngine::PermitPump {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "{}",
        format_h10d16_startup_line(runtime_config.relay_policy.d16_byte_owned())
    );

    // 1. 初始化 TUN 设备 / 创建操作系统的原生异步虚拟网卡。
    let raw_tun = match create_tun_device(runtime_config.tun_mtu).await {
        Ok(device) => device,
        Err(e) => {
            println!("无法创建 TUN 设备: {e}");
            return;
        }
    };
    let device = if runtime_config.relay_policy.d16_byte_owned() {
        println!(
            "🧺 TUN ingress service: enabled capacity={} packets source=tun_tx_queue_len",
            runtime_config.tun_tx_queue_len
        );
        VirtualTunDevice::new_pumped(
            raw_tun,
            runtime_config.tun_mtu,
            runtime_config.tun_tx_queue_len,
        )
    } else {
        println!("🧺 TUN ingress service: disabled");
        VirtualTunDevice::new(raw_tun, runtime_config.tun_mtu)
    };

    // 刀11：进程级数据面可观测性句柄。**在选上游之前**构造一次 → 同一 `Arc<Metrics>` 既喂
    // run_event_loop（DNS/relay/gauge），又（T6 起）clone 进 TuicUpstream（UDP 下行 drop/背压）。
    let metrics = Arc::new(Metrics::new());

    // 2. 选上游 Transport：MINI_VPN_UPSTREAM=tuic（默认）| reality（VLESS over REALITY over TCP，刀8）。
    //    两分支各自单态化 run_event_loop（device 只在选中的分支被 move，互斥）。
    match select_upstream_kind(std::env::var("MINI_VPN_UPSTREAM").ok().as_deref()) {
        UpstreamKind::Tuic => {
            let cfg = match TuicClientConfig::from_env_with_relay_policy(Arc::clone(&relay_policy))
            {
                Ok(c) => c,
                Err(e) => {
                    println!("加载 TUIC 客户端配置失败（启动中止）: {e}");
                    return;
                }
            };
            let upstream = match TuicUpstream::connect(&cfg, Arc::clone(&metrics)).await {
                Ok(u) => {
                    println!("✅ 已连接 TUIC 出口 {} (sing-box)", cfg.server);
                    Arc::new(u)
                }
                Err(e) => {
                    println!("连接 TUIC 出口失败（启动中止）: {e:?}");
                    return;
                }
            };
            // 3. UDP over TUIC Packet 下行接收端（断线自愈，见 13b/13c）。
            println!("🌊 UDP relay 数据面就绪（TUIC Packet datagram → sing-box）");
            let Some(mut udp_shutdown) = upstream.take_udp_shutdown() else {
                eprintln!("TUIC UDP shutdown custody unavailable; retaining upstream");
                std::future::pending::<()>().await;
                return;
            };
            let tuic_downlink_rx = upstream.start_udp();
            // 4. 进入共享主循环（生产传 NoopSink：零插桩开销）。
            run_event_loop_sinked(
                runtime_config.profile_loop,
                device,
                Arc::clone(&upstream),
                tuic_downlink_rx,
                runtime_config,
                Arc::clone(&metrics),
            )
            .await;
            udp_shutdown.finish().await;
        }
        UpstreamKind::Reality => {
            // REALITY 是 **TCP-only**（force-reality）：UDP no-op（DatagramUpstream 静默丢）+
            // **空 downlink channel**——持有 tx 永不 send → run_event_loop 的下行 select 分支永久 pending
            // （REALITY 无 UDP 下行；分离上游/UDP-over-VLESS/failover 留刀9）。
            let upstream = match RealityUpstream::from_env() {
                Ok(u) => {
                    println!(
                        "✅ 已配置 REALITY 出口（VLESS over REALITY over TCP；TCP-only，UDP no-op）"
                    );
                    Arc::new(u)
                }
                Err(e) => {
                    println!("加载 REALITY 客户端配置失败（启动中止）: {e:?}");
                    return;
                }
            };
            let (_dummy_tx, dummy_rx) = mpsc::channel::<Vec<u8>>(1); // 持 tx 不 drop → 下行分支永挂
            run_event_loop_sinked(
                runtime_config.profile_loop,
                device,
                upstream,
                dummy_rx,
                runtime_config,
                Arc::clone(&metrics),
            )
            .await;
        }
        UpstreamKind::Failover => {
            // 刀9：健康感知 TUIC↔REALITY。两腿都建——TUIC 既承 TCP relay 又是 **UDP 唯一出口**，
            // REALITY 是 TCP-only 备路。`FailoverUpstream::open_tcp` 按健康态选腿；`send_udp` 恒走 tuic。
            let tuic_cfg =
                match TuicClientConfig::from_env_with_relay_policy(Arc::clone(&relay_policy)) {
                    Ok(c) => c,
                    Err(e) => {
                        println!("加载 TUIC 客户端配置失败（failover 启动中止）: {e}");
                        return;
                    }
                };
            let tuic = match TuicUpstream::connect(&tuic_cfg, Arc::clone(&metrics)).await {
                Ok(u) => {
                    println!("✅ 已连接 TUIC 出口 {} (failover 主腿)", tuic_cfg.server);
                    Arc::new(u)
                }
                Err(e) => {
                    println!("连接 TUIC 出口失败（failover 启动中止）: {e:?}");
                    return;
                }
            };
            // UDP 下行接收端来源端 = TUIC（独立于 TCP 选腿；UDP 永久绑 TUIC）。
            println!("🌊 UDP relay 数据面就绪（TUIC Packet datagram → sing-box；UDP 永留 QUIC）");
            let tuic_downlink_rx = tuic.start_udp();
            let reality = match RealityUpstream::from_env() {
                Ok(u) => {
                    println!(
                        "✅ 已配置 REALITY 出口（failover 备腿；VLESS over REALITY over TCP）"
                    );
                    Arc::new(u)
                }
                Err(e) => {
                    println!(
                        "加载 REALITY 客户端配置失败（failover 需两腿都配齐，启动中止）: {e:?}"
                    );
                    return;
                }
            };
            let upstream = Arc::new(FailoverUpstream::new(tuic, reality));
            // 后台健康探针：REALITY 当班时每 30s 探 TUIC，按不对称迟滞（连续 3 + 60s 冷却）切回。
            upstream.spawn_health_probe();
            println!("🔀 failover 就绪：TCP relay 健康感知 TUIC↔REALITY，UDP 恒走 TUIC");
            run_event_loop_sinked(
                runtime_config.profile_loop,
                device,
                upstream,
                tuic_downlink_rx,
                runtime_config,
                Arc::clone(&metrics),
            )
            .await;
        }
    }
}

mod egress;

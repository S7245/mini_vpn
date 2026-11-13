//! Persistent production TUIC service composition. Owns the saved node/session
//! driver, never DATA scheduling or generation policy. Observers borrow work;
//! business settlement precedes node cleanup. Entries are the public `prepare_*_service` factories; tests below.

use super::*;
use crate::failover::{HealthExit, HealthTask};
use crate::shared::ClientError;
use crate::tuic::{NodeHost, NodeShutdown, NodeStartup};

mod notifications;
mod observe;
mod rebuild;
mod saved;

// Cold composition only. These concrete adapters share one saved driver below;
// no enum, callback or additional dispatch is placed on the DATA path.
trait Composition {
    const CONTROLLED_REBUILD: bool;
    type Upstream: ProxyUpstream + DatagramUpstream + 'static;
    fn compose(&self, data: Arc<TuicUpstream>) -> (Arc<Self::Upstream>, Option<HealthTask>);
}
impl Composition for () {
    const CONTROLLED_REBUILD: bool = true;
    type Upstream = TuicUpstream;
    fn compose(&self, data: Arc<TuicUpstream>) -> (Arc<Self::Upstream>, Option<HealthTask>) {
        (data, None)
    }
}
struct FailoverComposition {
    backup: Arc<RealityUpstream>,
    state: Arc<crate::failover::FailoverState>,
}
impl Composition for FailoverComposition {
    const CONTROLLED_REBUILD: bool = true;
    type Upstream = FailoverUpstream<TuicUpstream, RealityUpstream>;
    fn compose(&self, data: Arc<TuicUpstream>) -> (Arc<Self::Upstream>, Option<HealthTask>) {
        let upstream = Arc::new(FailoverUpstream::with_state(
            data,
            self.backup.clone(),
            self.state.clone(),
        ));
        let health = upstream.spawn_health_probe();
        (upstream, Some(health))
    }
}
struct Configuration<C> {
    tuic: TuicClientConfig,
    composition: C,
}
impl Configuration<FailoverComposition> {
    fn failover(tuic: TuicClientConfig, reality: RealityUpstream) -> Self {
        let policy = Arc::new(tuic.relay_policy().generic_tuic_transport());
        Self {
            tuic: tuic.with_relay_policy(policy),
            composition: FailoverComposition {
                backup: Arc::new(reality),
                state: Arc::new(crate::failover::FailoverState::new()),
            },
        }
    }
}

pub(super) enum Startup {
    Tuic(TuicClientConfig),
    Failover(TuicClientConfig, RealityUpstream),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Starting,
    Running,
    Rebuilding,
    Stopping,
    Incomplete,
    Quarantined,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    session: SessionDeviceState,
    pub node: Option<NodeShutdown>,
    /// Consumed successor budget (0 or 1), including a rejected successor startup.
    pub rebuilds: u8,
    pub discarded_packets: u64,
    pub discarded_backing: u64,
}

impl Report {
    /// Both success and known failure require actual original device settlement.
    pub fn device_settled(&self) -> bool {
        matches!(
            self.session,
            SessionDeviceState::DeviceSettled | SessionDeviceState::DeviceFailedSettled
        )
    }

    pub fn device_failed(&self) -> bool {
        self.session == SessionDeviceState::DeviceFailedSettled
    }
}

/// Saved service work. Cancelling a borrowed run never reconstructs its driver.
/// Keep this owner alive through shutdown; dropping it is forced termination.
/// The unique handle requests actions but cannot supply settlement proofs.
pub trait TunService {
    fn take_handle(&mut self) -> Option<Handle>;
    fn run(&mut self) -> impl Future<Output = Report> + '_;
}

struct Control {
    backing_budget: Option<bytes::BackingBudget>,
    app_meter: Option<bytes::BackingMeter>,
    stop: SessionStop,
    rebuild: SessionStop,
    rebuild_supported: bool,
    terminal: saved::Terminal,
    deadline: Arc<Mutex<Option<tokio::time::Instant>>>,
    phase: Mutex<Phase>,
    changed: Notify,
    notifications: notifications::Notifications,
    /// N5-OBS (D241): weak read windows for the CLI `📊 Native` line.
    observed: Mutex<observe::Watch>,
    #[cfg(test)]
    namespace_clocks: Mutex<[Option<std::time::Instant>; 2]>,
    #[cfg(test)]
    session: Mutex<Option<std::sync::Weak<SessionControl>>>,
    #[cfg(test)]
    abort_stats: AtomicBool,
    /// B4b: abort the old Node's live stats task after controlled-rebuild begin.
    #[cfg(test)]
    abort_stats_at_rebuild: AtomicBool,
    #[cfg(test)]
    node_fault_settled: AtomicBool,
    /// C4: one `NodeHost::C4_*` fault around the controlled-rebuild drain.
    #[cfg(test)]
    rebuild_node_fault: std::sync::atomic::AtomicU8,
    #[cfg(test)]
    held_node_data: Mutex<Option<Arc<dyn std::any::Any + Send + Sync>>>,
    #[cfg(test)]
    health_state: Mutex<Option<std::sync::Weak<crate::failover::FailoverState>>>,
    #[cfg(test)]
    health_observation: Mutex<Option<Arc<crate::failover::HealthObservation>>>,
    #[cfg(test)]
    abort_health: AtomicBool,
    #[cfg(test)]
    udp_generation: Mutex<Option<Arc<crate::owned_upstream::generation::GenerationControl>>>,
    #[cfg(test)]
    old_retirement_probe: Mutex<Option<Arc<Mutex<Vec<Option<usize>>>>>>,
    #[cfg(test)]
    native_node_sample: Mutex<Option<crate::tuic::NativeNodeWeakObserver>>,
    #[cfg(all(test, feature = "harness"))]
    saved_facts: Arc<tests::own_unwind::Facts>,
}

impl Control {
    /// No callbacks here: failure must mark hard Q and stop before another wake.
    fn quarantine_quiet(&self) {
        if let Some(meter) = &self.app_meter {
            meter.quarantine();
        }
        self.stop.stopping.store(true, Ordering::Release);
        self.deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(|| {
                tokio::time::Instant::now() + std::time::Duration::from_secs(15)
            });
        *self
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Phase::Quarantined;
    }
    fn publish(&self, phase: Phase) {
        if phase == Phase::Quarantined {
            if let Some(meter) = &self.app_meter {
                meter.quarantine();
            }
        }
        let changed = {
            let mut saved = self
                .phase
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *saved == Phase::Quarantined || *saved == phase {
                false
            } else {
                *saved = phase;
                true
            }
        };
        if changed {
            self.notifications.send(self, notifications::CHANGED);
        }
    }

    fn expire_at(&self, original: tokio::time::Instant) {
        let mut deadline = self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let earliest = deadline.map_or(original, |old| old.min(original));
        let tightened = *deadline != Some(earliest);
        *deadline = Some(earliest);
        drop(deadline);
        let stopped = !self.stop.stopping.swap(true, Ordering::AcqRel);
        let effects = if stopped { notifications::STOP } else { 0 }
            | if tightened { notifications::CHANGED } else { 0 };
        if effects != 0 {
            self.notifications.send(self, effects);
        }
    }

    /// One parent-stop transaction for the current session, in either DATA or
    /// retirement. Copy its original limit before notification; never carry a
    /// retired owner's limit into a clean successor. Observers do not drive it.
    fn stop_session(&self, owner: &SessionControl) {
        let inherited = *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(inherited) = inherited {
            let mut ledger = owner.ledger();
            ledger.shutdown_deadline = Some(
                ledger
                    .shutdown_deadline
                    .map_or(inherited, |old| old.min(inherited)),
            );
        }
        self.expire_at(owner.request_stop());
    }

    fn request_stop(&self) -> tokio::time::Instant {
        let deadline = *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(|| {
                tokio::time::Instant::now() + std::time::Duration::from_secs(15)
            });
        if !self.stop.stopping.swap(true, Ordering::AcqRel) {
            self.notifications.send(self, notifications::STOP);
        }
        deadline
    }
}

struct Completion {
    // Keep the actual diagnostic owners after settlement. Neither owns a DATA Arc.
    _node: Option<NodeHost>,
    _retired: Option<rebuild::Retired>,
    _health: Option<HealthTask>,
    control: Arc<SessionControl>,
    report: Report,
}

struct Service<F> {
    driver: saved::Driver<F>,
    control: Arc<Control>,
    completion: Option<Completion>,
    handle_issued: bool,
}

pub struct Handle(Arc<Control>);

impl Handle {
    pub fn phase(&self) -> Phase {
        *self
            .0
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// One explicit same-node rebuild request. Only the saved production driver
    /// may retire resources; callers cannot supply a settlement result.
    pub fn request_rebuild(&self) -> bool {
        if !self.0.rebuild_supported {
            return false;
        }
        if *self
            .0
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            != Phase::Running
            || self.0.stop.stopping.load(Ordering::Acquire)
            || self.0.rebuild.stopping.swap(true, Ordering::AcqRel)
        {
            return false;
        }
        self.0.notifications.send(&self.0, notifications::REBUILD);
        !self.0.notifications.failed()
    }

    pub fn request_stop(&self) {
        self.0.request_stop();
    }

    /// Observer only: the first published phase that differs from `seen`.
    pub async fn changed_from(&self, seen: Phase) -> Phase {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let phase = self.phase();
            if phase != seen {
                return phase;
            }
            changed.await;
        }
    }

    /// This observer never drives the saved work and never resets its deadline.
    pub async fn observe_shutdown(&self) -> Phase {
        let mut deadline = self.0.request_stop();
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let phase = self.phase();
            if matches!(phase, Phase::Complete | Phase::Quarantined) {
                return phase;
            }
            if let Some(shared) = *self
                .0
                .deadline
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
            {
                deadline = deadline.min(shared);
            }
            // The absolute logical limit may already be due when a changed
            // notification adopts it; registering a rounded timer is insufficient.
            if tokio::time::Instant::now() >= deadline {
                return Phase::Incomplete;
            }
            tokio::select! {
                biased;
                _ = &mut changed => {},
                _ = tokio::time::sleep_until(deadline) => return Phase::Incomplete,
            }
        }
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.request_stop();
    }
}

impl<F: Future<Output = Completion>> Service<F> {
    pub(super) fn take_handle(&mut self) -> Option<Handle> {
        if self.handle_issued {
            return None;
        }
        self.handle_issued = true;
        Some(Handle(self.control.clone()))
    }

    pub(super) async fn run(&mut self) -> Report {
        if let Some(complete) = &self.completion {
            if self
                .control
                .backing_budget
                .as_ref()
                .is_some_and(|budget| budget.quarantined() || budget.used() != 0)
            {
                self.control.publish(Phase::Quarantined);
                std::future::pending::<()>().await;
            }
            return complete.report;
        }
        std::future::poll_fn(|cx| {
            let complete = std::task::ready!(self.driver.poll(cx, &self.control));
            let report = complete.report;
            // Keep actual Node/session/report custody before fallible F disposal.
            self.completion = Some(complete);
            self.driver.dispose(&self.control);
            if self
                .control
                .backing_budget
                .as_ref()
                .is_some_and(|budget| budget.quarantined() || budget.used() != 0)
            {
                self.control.publish(Phase::Quarantined);
                return Poll::Pending;
            }
            self.control.publish(Phase::Complete); // Sticky Q cannot be replaced.
            if self
                .control
                .backing_budget
                .as_ref()
                .is_some_and(bytes::BackingBudget::quarantined)
            {
                return Poll::Pending;
            }
            Poll::Ready(report)
        })
        .await
    }
}

impl<F: Future<Output = Completion>> TunService for Service<F> {
    fn take_handle(&mut self) -> Option<Handle> {
        Service::take_handle(self)
    }

    async fn run(&mut self) -> Report {
        Service::run(self).await
    }
}

impl<F> Drop for Service<F> {
    fn drop(&mut self) {
        if self.completion.is_none() {
            if let Some(meter) = &self.control.app_meter {
                meter.quarantine();
            }
        }
        self.control.request_stop();
        // Forced termination has no async settlement claim.
        if self.completion.is_none() {
            self.control.publish(Phase::Quarantined);
        }
        self.driver.dispose(&self.control);
    }
}

/// One cold selection, also called by the public entry before creating a TUN.
/// The helper repeats it before NodeHost construction so tests cannot bypass it.
pub(super) fn select_runtime(config: &mut TunRuntimeConfig) -> Result<(), ClientError> {
    config.listener_capacity_profile = ListenerCapacityProfile::F4096;
    config
        .listener_capacity_profile
        .startup_max_total(config.tcp_socket_buffers)
        .map(|_| ())
        .map_err(|reason| {
            ClientError::InvalidTarget(format!("TUIC session listener capacity: {reason:?}"))
        })
}

#[cfg(test)]
fn new_control(rebuild_supported: bool) -> Arc<Control> {
    control_with_budget(rebuild_supported, None)
}
fn control_with_budget(
    rebuild_supported: bool,
    backing_budget: Option<bytes::BackingBudget>,
) -> Arc<Control> {
    let app_meter = backing_budget
        .as_ref()
        .map(bytes::BackingMeter::with_budget);
    Arc::new(Control {
        backing_budget,
        app_meter,
        stop: SessionStop::default(),
        rebuild: SessionStop::default(),
        rebuild_supported: rebuild_supported,
        terminal: saved::Terminal::default(),
        deadline: Arc::new(Mutex::new(None)),
        phase: Mutex::new(Phase::Starting),
        changed: Notify::new(),
        notifications: notifications::Notifications::default(),
        observed: Mutex::default(),
        #[cfg(test)]
        namespace_clocks: Mutex::new([None; 2]),
        #[cfg(test)]
        session: Mutex::new(None),
        #[cfg(test)]
        abort_stats: AtomicBool::new(false),
        #[cfg(test)]
        abort_stats_at_rebuild: AtomicBool::new(false),
        #[cfg(test)]
        node_fault_settled: AtomicBool::new(false),
        #[cfg(test)]
        rebuild_node_fault: std::sync::atomic::AtomicU8::new(0),
        #[cfg(test)]
        held_node_data: Mutex::new(None),
        #[cfg(test)]
        health_state: Mutex::new(None),
        #[cfg(test)]
        health_observation: Mutex::new(None),
        #[cfg(test)]
        abort_health: AtomicBool::new(false),
        #[cfg(test)]
        udp_generation: Mutex::new(None),
        #[cfg(test)]
        old_retirement_probe: Mutex::new(None),
        #[cfg(test)]
        native_node_sample: Mutex::new(None),
        #[cfg(all(test, feature = "harness"))]
        saved_facts: Arc::default(),
    })
}

/// Cold plain-TUIC service over a caller-owned TUN. Uses the same production
/// Node/business/device lifecycle as the CLI, without opening an OS interface.
/// Exactly one explicit same-config successor is supported. No automatic retry.
pub fn prepare_tuic_service<D: TunIo, M: MetricsSink>(
    device: D,
    config: TuicClientConfig,
    runtime: TunRuntimeConfig,
    metrics: Arc<Metrics>,
    sink: M,
) -> impl TunService {
    prepare(device, config, runtime, metrics, sink)
}

/// Cold Failover service using the same saved production lifecycle as the CLI.
/// The original backup, selection state and device survive one explicit rebuild;
/// uncertain or incomplete settlement forbids a successor. No automatic retry.
pub fn prepare_failover_service<D: TunIo, M: MetricsSink>(
    device: D,
    tuic: TuicClientConfig,
    reality: RealityUpstream,
    runtime: TunRuntimeConfig,
    metrics: Arc<Metrics>,
    sink: M,
) -> impl TunService {
    prepare_with(
        device,
        Configuration::failover(tuic, reality),
        runtime,
        metrics,
        sink,
    )
}

fn prepare<D: TunIo, M: MetricsSink>(
    device: D,
    config: TuicClientConfig,
    runtime: TunRuntimeConfig,
    metrics: Arc<Metrics>,
    sink: M,
) -> Service<impl Future<Output = Completion>> {
    prepare_with(
        device,
        Configuration {
            tuic: config,
            composition: (),
        },
        runtime,
        metrics,
        sink,
    )
}

fn prepare_with<D: TunIo, M: MetricsSink, C: Composition>(
    device: D,
    config: Configuration<C>,
    runtime: TunRuntimeConfig,
    metrics: Arc<Metrics>,
    sink: M,
) -> Service<impl Future<Output = Completion>> {
    // The device already exists: a preflight rejection must not drop it inside
    // the factory wrapper, so it takes the retained-validation path instead.
    prepare_driver(
        move |_| async move { Ok(device) },
        true,
        config,
        runtime,
        metrics,
        sink,
    )
}

/// The factory and its cancellation custody stay inside the original saved work.
/// Private factories must return Err only before any device/actor exists; provided
/// devices always return Ok and are bound/retired by rebuild::drive.
fn prepare_factory<D: TunIo, M: MetricsSink, C: Composition, F, Fut>(
    factory: F,
    config: Configuration<C>,
    runtime: TunRuntimeConfig,
    metrics: Arc<Metrics>,
    sink: M,
) -> Service<impl Future<Output = Completion>>
where
    F: FnOnce(Option<bytes::BackingMeter>) -> Fut,
    Fut: Future<Output = Result<D, ClientError>>,
{
    prepare_driver(factory, false, config, runtime, metrics, sink)
}

/// `provided`: the factory only hands over an already-existing device. A
/// private factory is never invoked after a preflight rejection (A2.3: no OS
/// device or pump); a provided device is retained, never bound and disposed by
/// the original drive, exactly like a retained validation failure.
fn prepare_driver<D: TunIo, M: MetricsSink, C: Composition, F, Fut>(
    factory: F,
    provided: bool,
    config: Configuration<C>,
    runtime: TunRuntimeConfig,
    metrics: Arc<Metrics>,
    sink: M,
) -> Service<impl Future<Output = Completion>>
where
    F: FnOnce(Option<bytes::BackingMeter>) -> Fut,
    Fut: Future<Output = Result<D, ClientError>>,
{
    let allocation = runtime
        .native_limits
        .map(|limits| native_budget(limits, &config.tuic, &runtime, C::CONTROLLED_REBUILD))
        .transpose();
    let (budget, admission) = match allocation {
        Ok(Some((budget, memory))) => {
            eprintln!("Native memory preflight: hard {}B + configured instances = {memory}B", budget.limit());
            (Some(budget), Ok(()))
        }
        Ok(None) => (None, Ok(())),
        Err(error) => (None, Err(error)),
    };
    let control = control_with_budget(C::CONTROLLED_REBUILD, budget);
    let saved = control.clone();
    let (admission, retained) = match (admission, provided) {
        (Err(error), true) => (Ok(()), Some(error)),
        (admission, _) => (admission, None),
    };
    // Keep a failed Native allocation inside the original saved factory path.
    // It must never turn into an invocation with the Legacy None capability.
    let factory = move |meter| async move {
        admission?;
        factory(meter).await
    };
    let driver = Box::pin(rebuild::drive(
        factory, retained, config, runtime, metrics, sink, saved,
    ));
    #[cfg(all(test, feature = "harness"))]
    let driver = {
        eprintln!("[N1-service-original-frame] bytes={}", std::mem::size_of_val(driver.as_ref().get_ref()));
        tests::own_unwind::wrap(driver, control.saved_facts.clone())
    };
    Service {
        driver: saved::Driver::new(driver),
        control,
        completion: None,
        handle_issued: false,
    }
}

/// A2 preflight (Route 2): one hard budget whose protocol tier covers every
/// concurrent Node generation's honest QUIC bound. Typed rejection before any
/// device, endpoint or pump exists; production windows are never adjusted.
///
/// The second result is the separate total-memory preflight (amendment §5.3,
/// D25): hard + `I_cfg`, the configured unmetered own-code instances. It never
/// shrinks the metered budget; overflow is a typed rejection.
fn native_budget(
    limits: crate::client_tun::runtime_environment::NativeLimits,
    tuic: &TuicClientConfig,
    runtime: &TunRuntimeConfig,
    controlled_rebuild: bool,
) -> Result<(bytes::BackingBudget, u64), ClientError> {
    use crate::quic::honest_bound::{
        instance_bound, memory_bound, node_bound, preflight, protocol_reserve, InstanceFactors,
        MeasuredFactors,
    };
    let hard = limits.backing_bytes.get();
    let generations = if controlled_rebuild { 2 } else { 1 };
    let overflow = || ClientError::InvalidTarget("Native preflight: bound overflow".into());
    let node = node_bound(
        &tuic.transport_shape(),
        &MeasuredFactors::A2_MEASURED,
        tuic.connection_count(),
    )
    .map_err(|_| overflow())?;
    let reserve = protocol_reserve(node, generations).map_err(|_| overflow())?;
    // A2.3: Σ B_node + A1 startup cold ≤ hard, before any OS device or pump.
    let app_cold = native_app_cold(limits, runtime)?;
    preflight(hard as u64, app_cold, &[reserve]).map_err(|rejection| {
        ClientError::InvalidTarget(format!(
            "Native preflight rejected: {rejection:?}; protocol reserve {reserve}B \
             ({generations} generation(s) × 2 × node {node}B) + app startup cold {app_cold}B"
        ))
    })?;
    let instances = instance_bound(&native_instances(runtime)?, &InstanceFactors::D25_MEASURED)
        .map_err(|_| overflow())?;
    let memory = memory_bound(hard as u64, instances.total).map_err(|rejection| {
        ClientError::InvalidTarget(format!("Native memory preflight rejected: {rejection:?}"))
    })?;
    // O3 (D105): an explicit process ceiling turns the record into a gate.
    if let Some(ceiling) = limits.memory_ceiling.filter(|ceiling| memory > ceiling.get()) {
        return Err(ClientError::InvalidTarget(format!(
            "Native memory preflight rejected: hard + instances {memory}B > ceiling {ceiling}B"
        )));
    }
    let reserve = usize::try_from(reserve).map_err(|_| overflow())?;
    let budget = bytes::BackingBudget::with_protocol_reserve(hard, reserve)
        .ok_or_else(|| ClientError::InvalidTarget("invalid Native backing limit".into()))?;
    Ok((budget, memory))
}

/// A1 App-tier startup cold that must fit before any OS device, pump or Node
/// exists (A2.3, review O1): the Service control cold (`egress::deadline`) plus
/// the Device construction headroom for the configured slots and MTU. Both
/// terms are the owners' own reservation layouts; Node-owned cold is part of
/// the protocol-tier bound and per-flow cold is admitted at runtime.
fn native_app_cold(
    limits: crate::client_tun::runtime_environment::NativeLimits,
    runtime: &TunRuntimeConfig,
) -> Result<u64, ClientError> {
    crate::device::cold::PreparedDevice::service_required_bytes(limits.device_slots.get(), runtime.tun_mtu)
        .and_then(|device| device.checked_add(egress::deadline::layout_bytes()))
        .and_then(|cold| u64::try_from(cold).ok())
        .ok_or_else(|| ClientError::InvalidTarget("Native preflight: app cold overflow".into()))
}

/// Configured instance shape of the Service this runtime will start: the
/// session always selects the F4096 listener profile (`select_runtime`).
fn native_instances(
    runtime: &TunRuntimeConfig,
) -> Result<crate::quic::honest_bound::InstanceShape, ClientError> {
    use std::mem::size_of;
    let buffers = runtime.tcp_socket_buffers;
    let listeners = ListenerCapacityProfile::F4096
        .startup_max_total(buffers)
        .map_err(|reason| {
            ClientError::InvalidTarget(format!("TUIC session listener capacity: {reason:?}"))
        })?;
    let (flows, flow_slots, flow_row, flow_slot, flow_index) = super::flow_custody::ROOT_GEOMETRY;
    Ok(crate::quic::honest_bound::InstanceShape {
        listeners,
        socket_rx: buffers.rx_bytes,
        socket_tx: buffers.tx_bytes,
        socket_context_entry: size_of::<(smoltcp::iface::SocketHandle, crate::client_tun::SocketCtx)>(),
        socket_storage_entry: size_of::<smoltcp::iface::SocketStorage<'static>>()
            + size_of::<smoltcp::iface::SocketHandle>(),
        flow_slots,
        flows,
        flow_row,
        flow_slot,
        flow_index,
    })
}

/// CLI startup and external callers share the public saved-service factories.
pub(super) async fn serve<D: TunIo, F, Fut>(
    factory: F,
    config: Startup,
    runtime: TunRuntimeConfig,
    metrics: Arc<Metrics>,
) where
    F: FnOnce(Option<bytes::BackingMeter>) -> Fut,
    Fut: Future<Output = Result<D, ClientError>>,
{
    let hangup = hangup_rebuild(&runtime);
    // N5-OBS (D241): hard Native only, on the existing metrics cadence.
    let observe = hangup.then(|| std::time::Duration::from_secs(runtime.metrics_secs.max(1)));
    match config {
        Startup::Tuic(tuic) => {
            let config = Configuration {
                tuic,
                composition: (),
            };
            if runtime.profile_loop {
                serve_with(prepare_factory(
                    factory,
                    config,
                    runtime,
                    metrics,
                    LoopProfiler::new(),
                ), hangup, observe)
                .await;
            } else {
                serve_with(prepare_factory(factory, config, runtime, metrics, NoopSink), hangup, observe)
                    .await;
            }
        }
        Startup::Failover(tuic, reality) => {
            let config = Configuration::failover(tuic, reality);
            if runtime.profile_loop {
                serve_with(prepare_factory(
                    factory,
                    config,
                    runtime,
                    metrics,
                    LoopProfiler::new(),
                ), hangup, observe)
                .await;
            } else {
                serve_with(prepare_factory(factory, config, runtime, metrics, NoopSink), hangup, observe)
                    .await;
            }
        }
    }
}

/// D108 (review F2): only hard Native gets SIGHUP → rebuild. The legacy default
/// never registers the handler, so its SIGHUP disposition stays the OS default.
fn hangup_rebuild(runtime: &TunRuntimeConfig) -> bool {
    runtime.native_limits.is_some()
}

/// D108 (review F5): surface every published Incomplete/Quarantined verdict.
/// Observer only; it never drives or settles the Service and never returns.
async fn report_verdicts(handle: &Handle, mut sink: impl FnMut(String)) -> std::convert::Infallible {
    let mut seen = Phase::Starting;
    loop {
        seen = handle.changed_from(seen).await;
        if matches!(seen, Phase::Incomplete | Phase::Quarantined) {
            sink(format!(
                "TUIC service phase {seen:?}: old Node drain not settled; still driving it"
            ));
        }
    }
}

/// CLI control: Ctrl-C requests stop; for hard Native, unix SIGHUP requests the
/// one explicit same-node session rebuild (D105/D108). Signals only request; the
/// saved driver decides. Incomplete/Quarantined verdicts are reported (D108).
async fn serve_with<S: TunService>(
    mut service: S,
    hangup_rebuild: bool,
    observe: Option<std::time::Duration>,
) {
    let handle = service.take_handle();
    #[cfg(unix)]
    let mut hangup = hangup_rebuild
        .then(|| tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()))
        .and_then(|signal| {
            signal
                .map_err(|error| eprintln!("TUIC rebuild signal unavailable: {error}"))
                .ok()
        });
    #[cfg(not(unix))]
    let _ = hangup_rebuild;
    let verdicts = async {
        match handle.as_ref() {
            Some(handle) => report_verdicts(handle, |line| eprintln!("{line}")).await,
            None => std::future::pending().await,
        }
    };
    let mut verdicts = std::pin::pin!(verdicts);
    let observer = async {
        match (handle.as_ref(), observe) {
            (Some(handle), Some(period)) => {
                observe::report(handle, period, |line| println!("{line}")).await
            }
            _ => std::future::pending().await,
        }
    };
    let mut observer = std::pin::pin!(observer);
    let rebuild = async {
        #[cfg(unix)]
        if let Some(hangup) = hangup.as_mut() {
            while hangup.recv().await.is_some() {
                let accepted = handle.as_ref().is_some_and(Handle::request_rebuild);
                eprintln!("TUIC rebuild request (SIGHUP): accepted={accepted}");
            }
        }
        std::future::pending::<()>().await
    };
    let report = {
        let mut run = std::pin::pin!(service.run());
        tokio::select! {
            report = &mut run => Some(report),
            never = &mut verdicts => match never {},
            never = &mut observer => match never {},
            () = rebuild => None,
            signal = tokio::signal::ctrl_c() => {
                if let Err(error) = signal { eprintln!("TUIC stop signal failed: {error}"); }
                None
            }
        }
    };
    if let Some(handle) = &handle {
        handle.request_stop();
    }
    let report = match report {
        Some(report) => report,
        None => tokio::select! {
            report = service.run() => report,
            never = &mut verdicts => match never {},
            never = &mut observer => match never {},
        },
    };
    eprintln!("TUIC service settled: {report:?}");
}

#[cfg(all(test, feature = "harness"))]
mod tests;

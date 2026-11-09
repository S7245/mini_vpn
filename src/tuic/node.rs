//! Persistent TUIC node root: saved startup and exact physical cleanup witnesses.
//! Only NodeHost polls work; observation cancellation retains it. This first step
//! publishes ready DATA with exact runtime cleanup. Closed auxiliary recovery is driven by the saved run future.
//! No startup/observer/drain tasks are spawned. Tests live in node/tests.
use super::*;
use crate::shared::errors::{FailureOrigin, FailurePhase, FailureResource, RetainedFailure};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use tokio::task::JoinHandle;

mod admission_failstop;
mod maintenance;
mod rebuild;
pub(in crate::tuic) mod recovery;
mod startup;
mod uplink;
type Saved<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStartup {
    Ready,
    Failed,
    Stopping,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeShutdown {
    StartupResourcesSettled,
    RunningResourcesSettled,
    FailedSettled,
    Incomplete,
    Quarantined,
}

/// Retain this cold owner through explicit cleanup. Drop is forced termination,
/// never evidence of settlement. DATA references must return before settlement.
pub struct NodeHost {
    resources: Arc<StartupResources>,
    startup: Option<Saved<Result<TuicUpstream, ClientError>>>,
    config: TuicClientConfig,
    running: Option<Saved<Result<(), ClientError>>>,
    running_failed: bool,
    ready: Option<Arc<TuicUpstream>>,
    cleanup: Option<Saved<NodeShutdown>>,
    result: Option<NodeShutdown>,
    controlled_rebuild: Option<usize>,
    deadline: Option<tokio::time::Instant>,
}
impl Drop for NodeHost {
    fn drop(&mut self) {
        // Losing this saved root cannot be postponed by diagnostic/actor Arcs.
        // Clone only the original runtime under the ledger, then seal outside it.
        let runtime = self
            .ready
            .as_ref()
            .map(|data| data.udp_runtime.clone())
            .or_else(|| self.resources.state().udp.as_ref().map(|udp| udp.runtime()));
        if let Some(runtime) = runtime {
            runtime.abandon_native_owner();
        }
        let unfinished_native = {
            let state = self.resources.state();
            state.backing_meter.budget_limit().is_some() && state.endpoint_created
                && !state.endpoint_drained
        };
        if unfinished_native { self.resources.uplink_quarantine(); }
    }
}
impl NodeHost {
    pub fn prepare(cfg: TuicClientConfig, metrics: Arc<Metrics>) -> Self {
        Self::prepare_with_budget(cfg, metrics, None)
    }
    pub(crate) fn prepare_with_budget(
        cfg: TuicClientConfig,
        metrics: Arc<Metrics>,
        budget: Option<&bytes::BackingBudget>,
    ) -> Self {
        let count = cfg.tcp_pool.clamp(MIN_TUIC_TCP_POOL, MAX_TUIC_TCP_POOL);
        let resources = Arc::new(StartupResources::with_meter(
            count,
            // A2: the Node's Quinn/TUIC allocations use the protocol tier, which
            // App backpressure can never consume.
            budget
                .map(bytes::BackingMeter::with_protocol_budget)
                .unwrap_or_default(),
        ));
        let config = cfg.clone();
        let owner = Arc::clone(&resources);
        let startup = Box::pin(async move {
            if cfg.quic_stats_secs == Some(0) {
                return Err(ClientError::InvalidTarget(
                    "TUIC stats interval must be positive".into(),
                ));
            }
            tokio::time::timeout(
                Duration::from_secs(90),
                TuicUpstream::connect_in(&cfg, metrics, Some(owner)),
            )
            .await
            .map_err(|_| io_err("tuic startup", "original 90s budget expired"))?
        });
        Self {
            resources,
            startup: Some(startup),
            config,
            running: None,
            running_failed: false,
            ready: None,
            cleanup: None,
            result: None,
            controlled_rebuild: None,
            deadline: None,
        }
    }
    pub(crate) fn bind_udp_parent(
        &self,
        parent: std::sync::Weak<dyn crate::shared::errors::QuietFailureSink>,
    ) -> Result<(), ClientError> {
        let mut state = self.resources.state();
        if state.published || state.udp_parent.is_some() {
            return Err(io_err("UDP parent", "cold binding already frozen"));
        }
        state.udp_parent = Some(parent);
        Ok(())
    }
    pub async fn start(&mut self) -> NodeStartup {
        std::future::poll_fn(|cx| {
            if self.deadline.is_some() {
                return Poll::Ready(NodeStartup::Stopping);
            }
            if self.ready.is_some() {
                return Poll::Ready(NodeStartup::Ready);
            }
            let Some(work) = self.startup.as_mut() else {
                return Poll::Ready(NodeStartup::Failed);
            };
            let result = match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
                Ok(Poll::Pending) => return Poll::Pending,
                Ok(Poll::Ready(result)) => Some(result),
                Err(payload) => {
                    self.resources.panic(payload);
                    None
                }
            };
            let result = match result {
                Some(Err(error)) => {
                    self.resources.record_startup_result(error);
                    None
                }
                other => other,
            };
            self.drop_startup();
            match result {
                Some(Ok(upstream)) if !self.resources.state().quarantined => {
                    self.ready = Some(Arc::new(upstream));
                    Poll::Ready(NodeStartup::Ready)
                }
                _ => Poll::Ready(NodeStartup::Failed),
            }
        })
        .await
    }
    /// Node-owned ready DATA. Runtime maintenance remains explicitly unavailable
    /// until the saved recovery executor is attached; the legacy constructor is
    /// not used to bypass that boundary.
    pub(crate) fn data(&mut self) -> Result<Arc<TuicUpstream>, ClientError> {
        if self.deadline.is_some() {
            return Err(io_err("tuic node", "stopping"));
        }
        let upstream = self
            .ready
            .as_ref()
            .ok_or_else(|| io_err("tuic node", "not ready"))?;
        let (published, closed) = {
            let state = self.resources.state();
            (state.published, state.quarantined || state.stopping)
        };
        if closed || upstream.udp_runtime.node_stopping() {
            return Err(io_err("tuic node", "runtime stopped or quarantined"));
        }
        if published {
            return Ok(Arc::clone(upstream));
        }
        // No other holder exists before first publication. Validate both directions:
        // every real slot has one witness, and every physical entry is a real slot.
        let ids = upstream
            .tcp_pool_slots
            .iter()
            .map(|slot| {
                slot.state
                    .try_lock()
                    .map(|state| state.current.transport.stable_id())
                    .map_err(|_| io_err("tuic node", "unpublished slot busy"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        {
            let state = self.resources.state();
            let entries = state.connections.iter().flatten().collect::<Vec<_>>();
            if state.quarantined
                || ids.len() != entries.len()
                || ids.iter().any(|id| {
                    entries
                        .iter()
                        .filter(|entry| {
                            entry
                                .connection
                                .as_ref()
                                .is_some_and(|conn| conn.stable_id() == *id)
                                && matches!(entry.retirement, Retirement::Ready(_))
                        })
                        .count()
                        != 1
                })
                || entries.iter().any(|entry| {
                    entry.connection.as_ref().is_none_or(|conn| {
                        ids.iter().filter(|id| **id == conn.stable_id()).count() != 1
                    })
                })
            {
                drop(state);
                self.resources.state().quarantined = true;
                self.resources.record(io_err(
                    "tuic node",
                    "physical publication inventory mismatch",
                ));
                return Err(recorded_failure());
            }
        }
        upstream.udp_runtime.prepare_native(&self.resources)?;
        let Some(udp) = upstream.take_udp_shutdown() else {
            self.resources.state().quarantined = true;
            self.resources
                .record(io_err("tuic node", "UDP token unavailable"));
            return Err(recorded_failure());
        };
        if upstream
            .udp_runtime
            .owner
            .set(Arc::downgrade(&self.resources))
            .is_err()
        {
            self.resources.state().quarantined = true;
            self.resources
                .record(io_err("tuic node", "runtime already attached"));
            return Err(recorded_failure());
        }
        let mut state = self.resources.state();
        state.udp = Some(udp);
        for entry in state.connections.iter_mut().flatten() {
            entry.published = true;
        }
        state.published = true;
        Ok(Arc::clone(upstream))
    }
    /// Observe the same node-owned maintenance work. Cancellation retains it.
    pub(crate) async fn run(&mut self) -> Result<(), ClientError> {
        if self.deadline.is_some() || self.running_failed {
            return Err(recorded_failure());
        }
        if self.running.is_none() {
            let data = self.data()?;
            data.start_endpoint_recovery_monitor();
            self.running = Some(
                match data.udp_runtime.native_recovery(
                    self.resources.clone(),
                    data.clone(),
                    self.config.clone(),
                )? {
                    Some(work) => Box::pin(work),
                    None => Box::pin(recovery::run(
                        self.resources.clone(),
                        data,
                        self.config.clone(),
                    )),
                },
            );
        }
        std::future::poll_fn(|cx| {
            let Some(work) = self.running.as_mut() else {
                return Poll::Ready(Err(recorded_failure()));
            };
            match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(result)) => {
                    self.running_failed = true;
                    if let Err(error) = result {
                        self.resources.record_startup_result(error);
                    }
                    self.stop_running_admission();
                    Poll::Ready(Err(recorded_failure()))
                }
                Err(payload) => {
                    self.resources.panic(payload);
                    self.running_failed = true;
                    self.stop_running_admission();
                    Poll::Ready(Err(recorded_failure()))
                }
            }
        })
        .await
    }
    fn stop_running_admission(&self) {
        if let Some(data) = &self.ready {
            data.udp_runtime.stop_node();
            data.tcp_pool_admission.available.notify_waiters();
        }
        self.resources.wake_recovery();
    }
    fn drop_startup(&mut self) {
        let work = self.startup.take();
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(work))) {
            self.resources.panic(payload);
        }
    }
    pub fn request_stop(&mut self) {
        let already_stopping = self.deadline.is_some();
        let proposed = self
            .ready
            .as_ref()
            .filter(|data| data.udp_runtime.native_enabled())
            .map(|data| data.udp_runtime.request_stop())
            .unwrap_or_else(|| self.resources.uplink_stop_deadline()
                .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(15)));
        let mut state = self.resources.state();
        let deadline = state
            .shutdown_deadline
            .into_iter()
            .chain(self.deadline)
            .fold(proposed, std::cmp::min);
        state.shutdown_deadline = Some(deadline);
        drop(state);
        self.deadline = Some(deadline);
        if already_stopping {
            return;
        }
        self.resources.state().stopping = true;
        self.stop_running_admission();
        self.resources.stop_stats();
        self.resources.dispose(self.running.take());
        self.drop_startup();
        let upstream = self.ready.take();
        if let Some(data) = upstream.as_ref() {
            data.udp_runtime.stop_node();
            data.tcp_pool_admission.available.notify_waiters();
        }
        self.resources.close_published();
        let resources = Arc::clone(&self.resources);
        self.cleanup = Some(Box::pin(async move { resources.cleanup(upstream).await }));
    }
    fn poll_cleanup(&mut self, cx: &mut Context<'_>) -> Poll<NodeShutdown> {
        if let Some(result) = self.result {
            return Poll::Ready(result);
        }
        let Some(work) = self.cleanup.as_mut() else {
            return Poll::Pending;
        };
        let result = match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(result)) => result,
            Err(payload) => {
                self.resources.panic(payload);
                NodeShutdown::Quarantined
            }
        };
        // Panic retains the saved future and any physical debt still held by it.
        if result != NodeShutdown::Quarantined {
            let work = self.cleanup.take();
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(work))) {
                self.resources.panic(payload);
                self.result = Some(NodeShutdown::Quarantined);
                return Poll::Ready(NodeShutdown::Quarantined);
            }
        }
        self.result = Some(result);
        Poll::Ready(result)
    }
    pub async fn observe_shutdown(&mut self) -> NodeShutdown {
        self.request_stop();
        let deadline = self.deadline.unwrap_or_else(tokio::time::Instant::now);
        tokio::select! {
            biased;
            result = std::future::poll_fn(|cx| self.poll_cleanup(cx)) => result,
            _ = async { if tokio::time::Instant::now() < deadline { tokio::time::sleep_until(deadline).await; } }
                => NodeShutdown::Incomplete,
        }
    }
    pub async fn drive_shutdown(&mut self) -> NodeShutdown {
        self.request_stop();
        std::future::poll_fn(|cx| self.poll_cleanup(cx)).await
    }
}

/// Original logical inventory survives destruction of transport storage. No
/// Connection/Endpoint is retained here, and no fresh zero counters certify it.
struct TerminalGeneration {
    slot: usize,
    current: bool,
    identity: TcpPoolTransportIdentity,
    physical_drained: bool,
    activity: Arc<TcpPoolGenerationActivity>,
    writes: Arc<TcpWritePressure>,
    reads: Arc<TcpOrderedReadPressure>,
}
struct TerminalPool {
    invalid: bool,
    admission: TcpPoolAdmission,
    generations: Vec<TerminalGeneration>,
    draining: Arc<AtomicBool>,
    predecessor_permit: Option<TcpPoolDrainingPermit>,
    primary_permit: Option<TcpPoolDrainingPermit>,
    primary_draining: Arc<AtomicBool>,
}
impl TerminalPool {
    async fn capture(data: &mut TuicUpstream, owner: &StartupResources) -> Self {
        let mut generations = Vec::with_capacity(data.tcp_pool_slots.len() + 2);
        for slot in &data.tcp_pool_slots {
            let state = slot.state.lock().await;
            for (current, generation) in std::iter::once((true, &state.current))
                .chain(state.draining.as_ref().map(|old| (false, &old.generation)))
            {
                generations.push(TerminalGeneration {
                    slot: slot.index,
                    current,
                    identity: generation.identity(),
                    physical_drained: false,
                    activity: generation.activity.clone(),
                    writes: generation.write_pressure.clone(),
                    reads: generation.read_pressure.clone(),
                });
            }
        }
        let predecessor = owner.state().recovery.predecessor.take();
        let predecessor_permit = predecessor.map(|old| {
            generations.push(old.generation);
            old.permit
        });
        let primary = owner.state().recovery.primary_predecessor.take();
        let primary_permit = primary.map(|old| {
            generations.push(old.generation);
            old.permit
        });
        Self {
            primary_permit,
            primary_draining: owner.state().recovery.primary_permit.clone(),
            predecessor_permit,
            invalid: false,
            admission: std::mem::replace(
                &mut data.tcp_pool_admission,
                TcpPoolAdmission::with_current_generation_activity(Vec::new()),
            ),
            generations,
            draining: data.tcp_pool_draining_predecessor.clone(),
        }
    }
    fn validate(&self, physical: &StartupState) -> Result<(), ClientError> {
        let admission = &self.admission;
        let count = admission.active_slots.len();
        if count == 0
            || count > MAX_TUIC_TCP_POOL
            || admission.preparing_slots.len() != count
            || admission.current_generation_activity.len() != count
            || admission.qualification_epochs.len() != count
            || self.generations.iter().filter(|g| g.current).count() != count
            || self
                .generations
                .iter()
                .filter(|g| !g.physical_drained)
                .count()
                != physical.connections.iter().flatten().count()
            || self
                .generations
                .iter()
                .filter(|g| !g.current && g.slot != 0)
                .count()
                > 1
            || self
                .generations
                .iter()
                .filter(|g| !g.current && g.slot == 0)
                .count()
                > 1
            || self.draining.load(Ordering::Acquire)
                != self.generations.iter().any(|g| !g.current && g.slot != 0)
            || self.primary_draining.load(Ordering::Acquire)
                != self.generations.iter().any(|g| !g.current && g.slot == 0)
        {
            return Err(io_err("tuic terminal", "inventory shape mismatch"));
        }
        for index in 0..count {
            let current = self
                .generations
                .iter()
                .filter(|g| g.current && g.slot == index)
                .collect::<Vec<_>>();
            let activity = admission.current_generation_activity[index]
                .lock()
                .map_err(|_| io_err("tuic terminal", "activity inventory poisoned"))?;
            if current.len() != 1 || !Arc::ptr_eq(&current[0].activity, &activity) {
                return Err(io_err(
                    "tuic terminal",
                    "current activity identity mismatch",
                ));
            }
        }
        for generation in &self.generations {
            if generation.slot >= count
                || physical
                    .connections
                    .iter()
                    .flatten()
                    .filter(|entry| entry.stable_id == Some(generation.identity.stable_id))
                    .count()
                    != usize::from(!generation.physical_drained)
                || self
                    .generations
                    .iter()
                    .filter(|g| g.identity.stable_id == generation.identity.stable_id)
                    .count()
                    != 1
            {
                return Err(io_err("tuic terminal", "connection identity mismatch"));
            }
        }
        Ok(())
    }
    fn settled(&mut self) -> Result<bool, ClientError> {
        let mut ready = true;
        for generation in &self.generations {
            ready &= generation.activity.active() == 0;
            for empty in [
                generation
                    .writes
                    .writers
                    .try_lock()
                    .map(|entries| entries.is_empty())
                    .map_err(|error| matches!(error, std::sync::TryLockError::Poisoned(_))),
                generation
                    .reads
                    .readers
                    .try_lock()
                    .map(|entries| entries.is_empty())
                    .map_err(|error| matches!(error, std::sync::TryLockError::Poisoned(_))),
            ] {
                match empty {
                    Ok(empty) => ready &= empty,
                    Err(false) => ready = false,
                    Err(true) => {
                        return Err(io_err("tuic terminal", "pressure registry poisoned"));
                    }
                }
            }
        }
        if ready {
            self.predecessor_permit.take();
            self.primary_permit.take();
        }
        ready &= !self.draining.load(Ordering::Acquire)
            && !self.primary_draining.load(Ordering::Acquire);
        // These atomics are sequential debits, not an atomic sum snapshot.
        // With admission stopped, only complete zero is a retirement certificate.
        ready &= self.admission.pool_active_total.load(Ordering::Acquire) == 0;
        ready &= self
            .admission
            .active_slots
            .iter()
            .all(|count| count.load(Ordering::Acquire) == 0);
        ready &= self
            .admission
            .preparing_slots
            .iter()
            .all(|slot| !slot.load(Ordering::Acquire));
        Ok(ready)
    }
}

struct Attempt {
    stable_id: Option<usize>,
    published: bool,
    connection: Option<Connection>,
    retirement: Retirement,
}
enum Retirement {
    Ready(quinn::ConnectionDrain),
    Waiting(Saved<bool>),
    Failed,
}
struct StartupState {
    backing_meter: bytes::BackingMeter,
    udp_parent: Option<std::sync::Weak<dyn crate::shared::errors::QuietFailureSink>>,
    shutdown_deadline: Option<tokio::time::Instant>,
    endpoint: Option<Endpoint>,
    endpoint_drain: Option<quinn::EndpointDriverDrain>,
    endpoint_created: bool,
    endpoint_drained: bool,
    connections: Vec<Option<Attempt>>,
    tasks: Vec<recovery::StatsTask>,
    recovery: recovery::State,
    monitor: Option<JoinHandle<()>>,
    monitor_started: bool,
    monitor_joined: bool,
    published: bool,
    stopping: bool,
    config_rejected: bool,
    udp: Option<node_runtime::UdpShutdown>,
    uplink: udp_send::UdpStreamSender,
    uplink_parent: uplink::Parent,
    terminal: Option<TerminalPool>,
    task_limit: usize,
    poison_recorded: bool,
    stats_stop: Option<watch::Sender<bool>>,
    first: Option<RetainedFailure>,
    additional: u64,
    quarantined: bool,
    attempts_started: u64,
    attempts_drained: u64,
    #[cfg(test)]
    retire_poll_pending: Arc<StdMutex<Vec<Option<usize>>>>,
    tasks_started: u64,
    tasks_joined: u64,
}
#[derive(Debug)]
struct RecordedFailure;
impl std::fmt::Display for RecordedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("node failure retained by original owner")
    }
}
impl std::error::Error for RecordedFailure {}
pub(super) fn recorded_failure() -> ClientError {
    ClientError::Io(io::Error::other(RecordedFailure))
}
/// N2 (D143 F12): an observer of a Waiting closed slot fails fast, but nothing
/// was recorded; treated like `RecordedFailure` (never recorded again).
#[derive(Debug)]
struct WaitingFailure;
impl std::fmt::Display for WaitingFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("node slot waiting to re-probe; no failure recorded")
    }
}
impl std::error::Error for WaitingFailure {}
pub(super) fn waiting_failure() -> ClientError {
    ClientError::Io(io::Error::other(WaitingFailure))
}
/// Errors that must never be recorded again as a Node first failure.
fn is_recorded(error: &ClientError) -> bool {
    matches!(error, ClientError::Io(io) if io.get_ref().is_some_and(|source| {
        source.is::<RecordedFailure>() || source.is::<WaitingFailure>()
    }))
}

pub(super) struct StartupResources(
    StdMutex<StartupState>,
    std::sync::OnceLock<std::sync::Weak<crate::shared::errors::FailureOrder>>,
);
fn origin() -> FailureOrigin {
    FailureOrigin {
        phase: FailurePhase::Startup,
        resource: FailureResource::TuicNode,
        resource_id: 0,
        attempt_id: None,
    }
}
impl StartupResources {
    #[cfg(test)]
    fn new(pool: usize) -> Self {
        Self::with_meter(pool, bytes::BackingMeter::new())
    }
    fn with_meter(pool: usize, backing_meter: bytes::BackingMeter) -> Self {
        Self(
            StdMutex::new(StartupState {
                backing_meter,
                udp_parent: None,
                shutdown_deadline: None,
                endpoint: None,
                endpoint_drain: None,
                endpoint_created: false,
                endpoint_drained: false,
                connections: (0..pool + 3).map(|_| None).collect(),
                tasks: Vec::with_capacity(pool + 5),
                recovery: recovery::State::new(pool),
                task_limit: pool + 5,
                monitor: None,
                monitor_started: false,
                monitor_joined: false,
                published: false,
                stopping: false,
                config_rejected: false,
                udp: None,
                uplink: udp_send::UdpStreamSender::Empty,
                uplink_parent: uplink::Parent::Ready,
                terminal: None,
                poison_recorded: false,
                stats_stop: None,
                first: None,
                additional: 0,
                quarantined: false,
                attempts_started: 0,
                attempts_drained: 0,
                #[cfg(test)]
                retire_poll_pending: Arc::new(StdMutex::new(vec![None; pool + 3])),
                tasks_started: 0,
                tasks_joined: 0,
            }),
            std::sync::OnceLock::new(),
        )
    }
    fn state(&self) -> std::sync::MutexGuard<'_, StartupState> {
        self.0.lock().unwrap_or_else(|poison| {
            let mut state = poison.into_inner();
            state.quarantined = true;
            if !state.poison_recorded {
                state.poison_recorded = true;
                if state.first.is_none() {
                    state.first = Some(RetainedFailure::opaque_panic(origin()));
                } else {
                    state.additional = state.additional.saturating_add(1);
                }
            }
            state
        })
    }
    pub(super) fn bind_udp_clock(
        &self,
        clock: &crate::shared::errors::FailureClock,
    ) -> Result<(), ClientError> {
        if !clock.bound_to_meter(&self.state().backing_meter)
            || self.1.set(Arc::downgrade(clock)).is_err()
        {
            self.state().backing_meter.quarantine();
            return Err(io_err(
                "UDP clock",
                "original Node clock already frozen or foreign",
            ));
        }
        Ok(())
    }
    pub(super) fn udp_cold_context(
        &self,
    ) -> (
        bytes::BackingMeter,
        Option<std::sync::Weak<dyn crate::shared::errors::QuietFailureSink>>,
    ) {
        let state = self.state();
        (state.backing_meter.clone(), state.udp_parent.clone())
    }
    pub(in crate::tuic) fn fallback_admission(&self) -> crate::failover::FallbackAdmission {
        use crate::failover::FallbackAdmission as Gate;
        let state = self.state();
        if state.quarantined {
            Gate::Quarantined
        } else if state.stopping {
            Gate::Stopping
        } else if state.config_rejected {
            Gate::ConfigurationRejected
        } else if !state.published {
            Gate::Unpublished
        } else if state
            .connections
            .iter()
            .flatten()
            .any(|entry| !entry.published)
            || state.recovery.fallback_pending()
        {
            Gate::Pending
        } else {
            Gate::Allowed
        }
    }
    fn failure(&self, failure: RetainedFailure) {
        // Upgrade outside the original ledger, publish Node -> Session once,
        // then release the possibly last original clock outside every owner lock.
        if let Some(clock) = self.1.get().and_then(std::sync::Weak::upgrade) {
            let order = clock.retained(&failure);
            if !clock.reported(order) {
                self.failure_local(failure);
            }
        } else {
            self.failure_local(failure);
        }
    }
    fn failure_local(&self, failure: RetainedFailure) {
        let mut state = self.state();
        if state.first.is_none() {
            state.first = Some(failure);
        } else {
            state.additional = state.additional.saturating_add(1);
        }
    }
    fn record(&self, error: ClientError) {
        self.failure(RetainedFailure::from_client_ref(origin(), &error));
        self.dispose(error);
    }
    fn record_startup_result(&self, error: ClientError) {
        let already_recorded = is_recorded(&error);
        let admission_q = matches!(&error, ClientError::Io(io) if io.get_ref()
            .and_then(|source| source.downcast_ref::<quinn::owned_events::AdmissionError>())
            .is_some_and(|cause| cause.0 == bytes::MeteredCopyError::Quarantined));
        if admission_q { self.uplink_quarantine(); }
        else if !already_recorded { self.failure(RetainedFailure::from_client_ref(origin(), &error)); }
        self.dispose(error);
    }
    fn dispose<T>(&self, value: T) {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(value))) {
            self.panic(payload);
        }
    }
    fn panic(&self, payload: Box<dyn std::any::Any + Send>) {
        self.state().quarantined = true;
        self.failure(RetainedFailure::opaque_panic(origin()));
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            self.failure(RetainedFailure::opaque_panic(origin()));
            let _ = catch_unwind(AssertUnwindSafe(|| drop(secondary)));
        }
    }
    fn register_endpoint(&self, endpoint: &mut Endpoint) -> Result<(), ClientError> {
        let drain = endpoint.take_driver_drain_handle();
        let mut state = self.state();
        state.endpoint_created = true;
        state.endpoint = Some(endpoint.clone());
        state.endpoint_drain = drain;
        if state.endpoint_drain.is_none() {
            state.quarantined = true;
            return Err(io_err("tuic startup", "missing endpoint witness"));
        }
        Ok(())
    }
    fn spawn_stats(&self, spawn: impl FnOnce() -> JoinHandle<()>) -> Result<(), ClientError> {
        self.spawn_stats_identified(None, spawn)
    }
    fn spawn_stats_identified(
        &self,
        stable_id: Option<usize>,
        spawn: impl FnOnce() -> JoinHandle<()>,
    ) -> Result<(), ClientError> {
        let mut state = self.state();
        if state.tasks.len() >= state.task_limit || state.quarantined {
            return Err(io_err(
                "tuic startup stats",
                "fixed role capacity unavailable",
            ));
        }
        // Fixed backing precedes spawn; handle publication cannot allocate or await.
        state.tasks.push(recovery::StatsTask {
            stable_id,
            handle: spawn(),
        });
        state.tasks_started += 1;
        Ok(())
    }
    fn stop_stats(&self) {
        if let Some(stop) = &self.state().stats_stop {
            stop.send_replace(true);
        }
    }
    async fn handshake(
        &self,
        endpoint: &Endpoint,
        cfg: &TuicClientConfig,
    ) -> Result<Connection, ClientError> {
        self.handshake_as(endpoint, cfg, false).await
    }
    /// `retryable`: an N2 closed-slot attempt whose clean failure is counted,
    /// not recorded (config rejection, Q and stop are still recorded).
    async fn handshake_as(
        &self,
        endpoint: &Endpoint,
        cfg: &TuicClientConfig,
        retryable: bool,
    ) -> Result<Connection, ClientError> {
        let (index, connecting) = {
            let mut state = self.state();
            let index = state
                .connections
                .iter()
                .position(Option::is_none)
                .filter(|_| !state.quarantined && !state.config_rejected)
                .ok_or_else(|| {
                    io_err(
                        "tuic startup",
                        "candidate capacity or configuration unavailable",
                    )
                })?;
            let mut connecting = endpoint.connect(cfg.server, &cfg.sni).map_err(|error| {
                state.config_rejected |= recovery::configuration_connect_rejection(&error);
                io_err("tuic connect", error)
            })?;
            let drain = connecting.take_drain_handle();
            state.connections[index] = Some(Attempt {
                stable_id: None,
                published: false,
                connection: None,
                retirement: drain.map(Retirement::Ready).unwrap_or(Retirement::Failed),
            });
            state.attempts_started += 1;
            if state.connections[index]
                .as_ref()
                .is_none_or(|attempt| matches!(attempt.retirement, Retirement::Failed))
            {
                state.quarantined = true;
                return Err(io_err("tuic startup", "missing connection witness"));
            }
            (index, connecting)
        };
        let work = async {
            let conn = if cfg.zero_rtt {
                match connecting.into_0rtt() {
                    Err(original) => original
                        .await
                        .map_err(|error| self.handshake_failure(error))?,
                    Ok((conn, accepted)) => {
                        if let Some(entry) = self.state().connections[index].as_mut() {
                            entry.stable_id = Some(conn.stable_id());
                            entry.connection = Some(conn.clone());
                        }
                        let _early_data_accepted = accepted.await;
                        if let Some(error) = conn.close_reason() {
                            return Err(self.handshake_failure(error));
                        }
                        conn
                    }
                }
            } else {
                connecting
                    .await
                    .map_err(|error| self.handshake_failure(error))?
            };
            if let Some(entry) = self.state().connections[index].as_mut() {
                entry.stable_id = Some(conn.stable_id());
                entry.connection = Some(conn.clone());
            }
            TuicUpstream::authenticate(&conn, &cfg.uuid, &cfg.password).await?;
            Ok(conn)
        };
        match tokio::time::timeout(Duration::from_secs(5), work).await {
            Ok(Ok(conn)) => Ok(conn),
            error => {
                let error = match error {
                    Ok(Err(error)) => error,
                    _ => io_err("tuic startup handshake", "original 5s budget expired"),
                };
                let waiting = retryable && !self.native_quarantined() && {
                    let mut state = self.state();
                    let clean = !state.stopping && !state.quarantined && !state.config_rejected;
                    if clean {
                        state.recovery.waiting_failures = state.recovery.waiting_failures.saturating_add(1);
                    }
                    clean
                };
                if waiting {
                    self.dispose(error);
                } else {
                    self.record(error);
                }
                self.retire(index).await?;
                Err(recorded_failure())
            }
        }
    }
    fn native_quarantined(&self) -> bool {
        self.1.get().is_some() && self.state().backing_meter.is_quarantined()
    }
    async fn retire(&self, index: usize) -> Result<(), ClientError> {
        #[cfg(test)]
        let _probe_guard =
            tests::RetireProbeGuard::new(self.state().retire_poll_pending.clone(), index);
        std::future::poll_fn(|cx| {
            let already_native_q = self.native_quarantined();
            let mut state = self.state();
            #[cfg(test)]
            let retire_poll_pending = state.retire_poll_pending.clone();
            let Some(attempt) = state.connections.get_mut(index).and_then(Option::as_mut) else {
                return Poll::Ready(Ok(()));
            };
            if matches!(attempt.retirement, Retirement::Ready(_)) {
                let Retirement::Ready(drain) =
                    std::mem::replace(&mut attempt.retirement, Retirement::Failed)
                else {
                    return Poll::Ready(Err(io_err("tuic candidate", "invalid retirement phase")));
                };
                if attempt.published {
                    if let Some(conn) = &attempt.connection {
                        conn.close(0u32.into(), b"node_stop");
                    }
                } else {
                    drain.abandon();
                }
                attempt.connection = None;
                attempt.retirement = Retirement::Waiting(Box::pin(drain.drained()));
            }
            #[cfg(test)]
            let stable_id = attempt.published.then_some(attempt.stable_id).flatten();
            let outcome = match &mut attempt.retirement {
                Retirement::Waiting(work) => {
                    catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx)))
                }
                _ => return Poll::Ready(Err(recorded_failure())),
            };
            if matches!(outcome, Ok(Poll::Pending)) {
                #[cfg(test)]
                {
                    retire_poll_pending
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)[index] = stable_id;
                }
                return Poll::Pending;
            }
            #[cfg(test)]
            {
                retire_poll_pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)[index] = None;
            }
            let retired = std::mem::replace(&mut attempt.retirement, Retirement::Failed);
            let success = matches!(outcome, Ok(Poll::Ready(true)));
            if success {
                state.connections[index] = None;
                state.attempts_drained += 1;
            } else {
                state.quarantined = true;
            }
            drop(state);
            // Failed/unknown debt is sealed before any unknown destructor runs.
            match outcome {
                Err(payload) => self.panic(payload),
                Ok(Poll::Ready(false)) if !already_native_q => {
                    self.record(io_err("tuic candidate", "physical retirement failed"))
                }
                _ => {}
            }
            self.dispose(retired);
            Poll::Ready(if success {
                Ok(())
            } else {
                Err(recorded_failure())
            })
        })
        .await
    }
    pub(super) fn spawn_monitor(&self, spawn: impl FnOnce() -> JoinHandle<()>) {
        let mut state = self.state();
        if state.stopping {
            return;
        }
        if state.monitor_started || state.quarantined {
            state.quarantined = true;
            drop(state);
            self.record(io_err("tuic monitor", "fixed role unavailable"));
            return;
        }
        state.monitor_started = true;
        state.monitor = Some(spawn());
    }
    fn close_published(&self) {
        for entry in self.state().connections.iter().flatten() {
            if entry.published
                && let Some(conn) = &entry.connection
            {
                #[cfg(test)]
                Self::trace_first_close(conn);
                conn.close(0u32.into(), b"node_stop");
            }
        }
    }
    async fn join_task(&self, task: JoinHandle<()>) {
        if let Err(error) = task.await {
            self.state().quarantined = true;
            match error.try_into_panic() {
                Ok(payload) => self.panic(payload),
                Err(cancelled) => self.failure(RetainedFailure::from_join(origin(), cancelled)),
            }
        }
    }
    async fn cleanup(self: Arc<Self>, mut upstream: Option<Arc<TuicUpstream>>) -> NodeShutdown {
        self.stop_stats();
        #[cfg(test)]
        self.trace_cleanup("monitor.begin", None);
        let monitor = self.state().monitor.take();
        let mut udp = self.state().udp.take();
        if let Some(task) = monitor {
            if let Some(runtime) = udp.as_ref().filter(|udp| udp.native_enabled()) {
                let complete = match runtime.prepare_monitor_join(task) {
                    Ok(work) => work.await,
                    Err(task) => {
                        self.state().monitor = Some(task);
                        false
                    }
                };
                if !complete {
                    self.state().quarantined = true;
                    self.state().udp = udp.take();
                    return NodeShutdown::Quarantined;
                }
            } else {
                self.join_task(task).await;
            }
            self.state().monitor_joined = true;
        }
        #[cfg(test)]
        self.trace_cleanup("monitor.end", None);
        #[cfg(test)]
        self.trace_cleanup("udp.begin", None);
        if let Some(mut udp) = udp.take() {
            let result = udp.drive_shutdown().await;
            let (failure, additional) = udp.take_node_failure();
            if let Some(failure) = failure {
                self.failure(failure);
            }
            let mut state = self.state();
            state.additional = state.additional.saturating_add(additional);
            state.quarantined |= result != node_runtime::UdpShutdownState::Settled;
            state.udp = Some(udp);
        }
        #[cfg(test)]
        self.trace_cleanup("udp.end", None);
        #[cfg(test)]
        self.trace_cleanup("source.begin", None);
        if let Some(data) = &upstream {
            let already_native_q = self.native_quarantined();
            match data.udp_runtime.settle_source().await {
                Ok(true) => {}
                Ok(false) => {
                    self.state().quarantined = true;
                    if !already_native_q {
                        self.record(io_err(
                            "UDP generation",
                            "original source inventory invalid during shutdown",
                        ));
                    }
                }
                Err(_) => {
                    self.state().quarantined = true;
                }
            }
        }
        #[cfg(test)]
        self.trace_cleanup("source.end", None);
        #[cfg(test)]
        self.trace_cleanup("tasks.begin", None);
        loop {
            let task = self.state().tasks.pop();
            let Some(task) = task else {
                break;
            };
            self.join_task(task.handle).await;
            self.state().tasks_joined += 1;
        }
        #[cfg(test)]
        self.trace_cleanup("tasks.end", None);
        // Before DATA publication, these are the initial current connections:
        // capture their original physical inventory before retiring them below.
        let published = self.state().published;
        #[cfg(test)]
        self.trace_cleanup("unpublished.begin", None);
        if published && self.retire_unpublished().await {
            recovery::settle_uncommitted(&self).await;
        }
        #[cfg(test)]
        self.trace_cleanup("unpublished.end", None);
        // Monitor has actually joined: reuse its original control timer role.
        // Arc Drop does not notify. Failed unwrap returns the exact Arc to this
        // saved future; observer cancellation cannot erase or replace it.
        #[cfg(test)]
        self.trace_cleanup("arc.begin", upstream.as_ref().map(Arc::strong_count));
        while let Some(data) = upstream.take() {
            match Arc::try_unwrap(data) {
                Ok(mut data) => {
                    #[cfg(test)]
                    self.trace_cleanup("arc.unwrapped", None);
                    #[cfg(test)]
                    self.trace_cleanup("terminal.capture.begin", None);
                    let mut terminal = TerminalPool::capture(&mut data, &self).await;
                    #[cfg(test)]
                    self.trace_cleanup("terminal.capture.end", None);
                    let validation = terminal.validate(&self.state());
                    terminal.invalid =
                        validation.is_err() || self.state().recovery.release_failure.is_some();
                    self.state().terminal = Some(terminal);
                    if let Err(error) = validation {
                        self.state().quarantined = true;
                        self.record(error);
                    }
                    let tails = match data.udp_stream_sender.get_mut() {
                        Ok(sender) => sender.empty_take(),
                        Err(poison) => {
                            self.state().quarantined = true;
                            self.record(io_err("tuic uplink", "poisoned physical inventory"));
                            poison.into_inner().empty_take()
                        }
                    };
                    self.state().uplink = tails;
                    self.dispose(data);
                }
                Err(data) => {
                    upstream = Some(data);
                    tokio::time::sleep(ENDPOINT_RECOVERY_SAMPLE_INTERVAL).await;
                }
            }
        }
        let count = self.state().connections.len();
        let mut physical_settled = true;
        for index in 0..count {
            #[cfg(test)]
            self.trace_cleanup("connection.begin", Some(index));
            physical_settled &= self.retire(index).await.is_ok();
            #[cfg(test)]
            self.trace_cleanup("connection.end", Some(index));
        }
        if !physical_settled && let Some(terminal) = self.state().terminal.as_mut() {
            terminal.invalid = true;
        }

        #[cfg(test)]
        self.trace_cleanup("terminal.witness.begin", None);
        loop {
            if self
                .state()
                .terminal
                .as_ref()
                .is_some_and(|terminal| terminal.invalid)
            {
                break; // Keep the original contradictory inventory even if counters reach zero.
            }
            let settled = self
                .state()
                .terminal
                .as_mut()
                .map(TerminalPool::settled)
                .unwrap_or(Ok(true));
            match settled {
                Ok(true) => {
                    self.state().terminal = None;
                    break;
                }
                Ok(false) => tokio::time::sleep(ENDPOINT_RECOVERY_SAMPLE_INTERVAL).await,
                Err(error) => {
                    self.state().quarantined = true;
                    self.record(error);
                    break;
                }
            }
        }
        #[cfg(test)]
        self.trace_cleanup("terminal.witness.end", None);
        #[cfg(test)]
        self.trace_cleanup("uplink.begin", None);
        self.retire_uplink().await;
        #[cfg(test)]
        self.trace_cleanup("uplink.end", None);
        #[cfg(test)]
        self.trace_cleanup("endpoint.begin", None);
        let (endpoint, drain) = {
            let mut state = self.state();
            (state.endpoint.take(), state.endpoint_drain.take())
        };
        drop(endpoint);
        if let Some(drain) = drain {
            let already_native_q = self.native_quarantined();
            let success = drain.drained().await;
            let mut state = self.state();
            state.endpoint_drained = success;
            state.quarantined |= !success;
            drop(state);
            if !success && !already_native_q {
                self.record(io_err("tuic endpoint", "driver release witness failed"));
            }
        }
        #[cfg(test)]
        self.trace_cleanup("endpoint.end", None);
        let udp = self.state().udp.take();
        if let Some(udp) = udp {
            let monitored = self.state().monitor_started;
            let retired = udp.retire_native(monitored);
            self.state().quarantined |= !retired;
            self.state().udp = Some(udp);
        }
        let state = self.state();
        let backing = state.backing_meter.snapshot();
        if state.quarantined
            || backing.quarantined
            || backing.allocations != 0
            || backing.payload_bytes != 0
            || backing.physical_bytes != 0
            || state.connections.iter().any(Option::is_some)
            || state.tasks_started != state.tasks_joined
            || (state.monitor_started && !state.monitor_joined)
            || !state.uplink.is_empty()
            || state.terminal.is_some()
            || (state.endpoint_created && !state.endpoint_drained)
        {
            NodeShutdown::Quarantined
        } else if state.first.is_some() {
            NodeShutdown::FailedSettled
        } else if state.published {
            NodeShutdown::RunningResourcesSettled
        } else {
            NodeShutdown::StartupResourcesSettled
        }
    }
}
#[cfg(test)]
pub(crate) use tests::{NativeNodeWeakObserver, WaitingSample};
#[cfg(test)]
mod tests;

impl crate::shared::errors::QuietFailureSink for StartupResources {
    fn record_quiet(&self, failure: RetainedFailure) {
        let panic = failure.facts().class == crate::shared::errors::FailureClass::Panic;
        self.failure_local(failure);
        if panic {
            self.quarantine_quiet();
        }
    }
    fn quarantine_quiet(&self) {
        let mut state = self.state();
        state.quarantined = true;
        state.backing_meter.quarantine();
    }
    fn deadline_quiet(&self) -> Option<tokio::time::Instant> {
        self.state().shutdown_deadline
    }
}

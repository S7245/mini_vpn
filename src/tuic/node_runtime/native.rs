//! Native composition at the original UdpRuntime: one cold lane observer,
//! callback inventory and original Node clock. Epochs/witnesses stay in the
//! original generation; no forwarding task or second refund authority exists.
//! Concrete actor sources are independently guarded before poll/unwind. The
//! Legacy path uses the same protocol body with plain concrete ownership.
//! Entry: UdpRuntime::prepare_native, start, finish_lane; tests live in native/tests.
use super::*;
use crate::owned_upstream::generation::{
    callbacks::{self, Callbacks, Custody, Role},
    native_source,
};
use crate::shared::errors::{FailureClock, FailureOrder, QuietFailureSink};
use bytes::{BackingMeter, MeteredCopyError};
pub(super) use callbacks::{Field, Guard};
use std::ops::{Deref, DerefMut};

pub(super) struct Native {
    pub(super) meter: BackingMeter,
    stock: StdMutex<Option<Stock>>,
}
struct Stock {
    callbacks: Callbacks, // actual roots before their last shared clock reference
    clock: FailureClock,
    prepared: Option<(GenerationSender, UdpDownlinkSource, TaskRoot)>,
    lane: Lane,
    monitor: Monitor,
}
enum Lane {
    Held(native_source::Observer),
    Checking,
    Finished,
}
enum Monitor {
    Available,
    Joining(JoinHandle<()>),
    Complete,
    Failed,
}
impl Native {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Stock>> {
        self.stock.lock().unwrap_or_else(|e| {
            self.meter.quarantine();
            e.into_inner()
        })
    }
    pub(super) fn clock(&self) -> Option<FailureClock> {
        self.lock().as_ref().map(|s| s.clock.clone())
    }
    pub(super) fn acquire(&self, role: Role) -> Result<callbacks::Lease, callbacks::CallbackError> {
        self.lock()
            .as_ref()
            .ok_or(callbacks::CallbackError::Quarantined)?
            .callbacks
            .acquire(role)
    }
    pub(super) fn take_prepared(&self) -> Option<(GenerationSender, UdpDownlinkSource, TaskRoot)> {
        self.lock().as_mut()?.prepared.take()
    }
    fn finish_lane(&self) -> bool {
        let (prepared, lane) = {
            let mut state = self.lock();
            let Some(stock) = state.as_mut() else {
                return !self.meter.is_quarantined();
            };
            (
                stock.prepared.take(),
                std::mem::replace(&mut stock.lane, Lane::Checking),
            )
        };
        if let Some((sender, source, tasks)) = prepared {
            drop(source);
            drop(sender);
            drop(tasks);
        }
        let lane = match lane {
            Lane::Held(observer) => match observer.finish() {
                Ok(()) => Lane::Finished,
                Err(observer) => Lane::Held(observer),
            },
            Lane::Finished => Lane::Finished,
            Lane::Checking => return false, // another exact observer transaction owns it
        };
        let finished = matches!(lane, Lane::Finished);
        if let Some(stock) = self.lock().as_mut() {
            stock.lane = lane;
        }
        finished && !self.meter.is_quarantined()
    }
    fn retire(&self, monitored: bool) -> bool {
        let stock = {
            let mut state = self.lock();
            let Some(stock) = state.as_ref() else {
                return !self.meter.is_quarantined();
            };
            if self.meter.is_quarantined()
                || stock.prepared.is_some()
                || !matches!(stock.lane, Lane::Finished)
                || !matches!(
                    (&stock.monitor, monitored),
                    (Monitor::Available, false) | (Monitor::Complete, true)
                )
                || !stock.callbacks.quiescent()
            {
                return false;
            }
            state.take()
        };
        // No role acquisition can restart after the unique stock was detached.
        // All actual source/callback roots precede the original clock refund.
        drop(stock);
        !self.meter.is_quarantined()
    }
}
pub(super) struct TaskRoot {
    tasks: Box<JoinSet<UdpOutcome>>,
    _stock: Option<bytes::BackingBudgetReservation>,
}
impl TaskRoot {
    fn prepare(meter: &BackingMeter) -> Result<Self, MeteredCopyError> {
        use std::alloc::{Layout, alloc};
        let layout = Layout::new::<JoinSet<UdpOutcome>>();
        let stock = meter.reserve_cold_layout(layout.size())?;
        let ptr = unsafe { alloc(layout).cast::<JoinSet<UdpOutcome>>() };
        if ptr.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        unsafe {
            ptr.write(JoinSet::new());
        }
        Ok(Self {
            tasks: unsafe { Box::from_raw(ptr) },
            _stock: stock,
        })
    }
}
impl Deref for TaskRoot {
    type Target = JoinSet<UdpOutcome>;
    fn deref(&self) -> &Self::Target {
        &self.tasks
    }
}
impl DerefMut for TaskRoot {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tasks
    }
}
struct NativeTasks {
    root: std::mem::ManuallyDrop<TaskRoot>,
    guard: Guard,
}
impl NativeTasks {
    fn bind(tasks: TaskRoot, guard: &Guard) -> Self {
        Self {
            root: std::mem::ManuallyDrop::new(tasks),
            guard: guard.clone(),
        }
    }
}
impl Deref for NativeTasks {
    type Target = JoinSet<UdpOutcome>;
    fn deref(&self) -> &Self::Target {
        &self.root
    }
}
impl DerefMut for NativeTasks {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.root
    }
}
impl Drop for NativeTasks {
    fn drop(&mut self) {
        if !self.root.is_empty() || self.guard.failed() {
            self.guard.quarantine();
            return; // actual stable Box, task outputs, header and allowance stay owned
        }
        self.guard.dispose(
            unsafe { std::mem::ManuallyDrop::take(&mut self.root) },
            Field::Source2,
        );
    }
}
/// Optional static field custody keeps the Legacy code's concrete types and
/// Native actor's independent Drop sites; no virtual dispatch or heap wrapper.
pub(super) enum Owned<T> {
    Plain(T),
    Native(Custody<T>),
}
impl<T> Owned<T> {
    pub(super) fn new(value: T, guard: Option<&Guard>, field: Field) -> Self {
        match guard {
            Some(g) => Self::Native(g.own(value, field)),
            None => Self::Plain(value),
        }
    }
}
impl<T> Deref for Owned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        match self {
            Self::Plain(v) => v,
            Self::Native(v) => v,
        }
    }
}
impl<T> DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut T {
        match self {
            Self::Plain(v) => v,
            Self::Native(v) => v,
        }
    }
}
impl UdpRuntime {
    pub(in crate::tuic) fn prepare_native(
        &self,
        owner: &Arc<node::StartupResources>,
    ) -> Result<(), ClientError> {
        if self.native.get().is_some() {
            return Ok(());
        }
        let (meter, parent) = owner.udp_cold_context();
        if meter.budget_limit().is_none() {
            return Ok(());
        }
        // Nothing may already own a raw callback registration or producer.
        if self.monitor_started.load(Ordering::Acquire) || self.state().started || self.node_owned()
        {
            meter.quarantine();
            return Err(io_err("UDP cold", "runtime already used"));
        }
        let clock = Arc::new(FailureOrder::default());
        let sink: Arc<dyn QuietFailureSink> = owner.clone();
        clock
            .bind_with_parent(
                Arc::downgrade(&sink),
                parent,
                udp_origin(),
                Some(meter.clone()),
            )
            .map_err(cold_error)?;
        drop(sink);
        let callbacks = Callbacks::prepare(&meter, clock.clone()).map_err(cold_error)?;
        let tasks = TaskRoot::prepare(&meter).map_err(cold_error)?;
        let (sender, source, observer) =
            native_source::prepare(self.generation.clone(), &meter, clock.clone(), &callbacks)
                .map_err(cold_error)?;
        owner.bind_udp_clock(&clock)?;
        let prepared = Native {
            meter,
            stock: StdMutex::new(Some(Stock {
                callbacks,
                clock,
                prepared: Some((sender, source, tasks)),
                lane: Lane::Held(observer),
                monitor: Monitor::Available,
            })),
        };
        drop(prepared.stock.lock());
        self.native
            .set(prepared)
            .map_err(|_| io_err("UDP cold", "duplicate assembly"))
    }
    pub(in crate::tuic) async fn wait_generation_sealed(&self, epoch: u64, role: Role) -> bool {
        let Some(native) = self.native.get() else {
            self.generation.wait_sealed(epoch).await;
            return self.generation.valid();
        };
        let mut lease = match native.acquire(role) {
            Ok(lease) => lease,
            Err(error) => {
                self.callback_error(error);
                return false;
            }
        };
        let mut future = Some(self.generation.wait_sealed(epoch));
        let result = match lease.run(&mut future) {
            Ok(work) => work.await.is_ok(),
            Err(_) => false,
        };
        result && self.generation.valid()
    }
    pub(in crate::tuic) fn uplink_meter(&self) -> Option<&BackingMeter> {
        self.native.get().map(|native| &native.meter)
    }
    /// Full/size are local UDP pressure. Unknown admission failure uses the original clock.
    pub(in crate::tuic) fn uplink_error(&self, error: MeteredCopyError) {
        if matches!(error, MeteredCopyError::BudgetFull | MeteredCopyError::TooLarge) {
            return;
        }
        if let Some(clock) = self.native.get().and_then(Native::clock) {
            match error {
                MeteredCopyError::Quarantined => { clock.quarantine(); }
                MeteredCopyError::Allocation => {
                    clock.io(&io::Error::from(quinn::owned_events::AdmissionError(error)));
                }
                MeteredCopyError::BudgetFull | MeteredCopyError::TooLarge => {},
            }
        }
        self.request_stop();
    }
    pub(super) fn callback_error(&self, error: callbacks::CallbackError) {
        if let Some(native) = self.native.get() {
            if error == callbacks::CallbackError::Capacity {
                let error = io::Error::from(io::ErrorKind::OutOfMemory);
                if let Some(clock) = native.clock() {
                    clock.io(&error);
                }
            } else if let Some(clock) = native.clock() {
                clock.quarantine();
            }
        }
        self.request_stop();
    }
    pub(super) async fn native_join(
        &self,
        deadline: Option<tokio::time::Instant>,
    ) -> UdpShutdownState {
        let Some(native) = self.native.get() else {
            return UdpShutdownState::Quarantined;
        };
        let mut lease = match native.acquire(Role::Shutdown) {
            Ok(lease) => lease,
            Err(error) => {
                self.callback_error(error);
                return UdpShutdownState::Quarantined;
            }
        };
        let guard = lease.guard();
        let mut work = Some(async {
            tokio::select! { biased;
                state = std::future::poll_fn(|cx| self.poll_join(cx, Some(&guard))) => state,
                _ = async { if let Some(deadline) = deadline { tokio::time::sleep_until(deadline).await } else { std::future::pending().await } } => UdpShutdownState::Incomplete,
            }
        });
        let result = match lease.run(&mut work) {
            Ok(work) => work.await,
            Err(error) => Err(callbacks::CallbackFailure {
                output: None,
                error,
            }),
        };
        drop(work);
        match result {
            Ok(state) => state,
            Err(failure) => failure
                .output
                .filter(|s| *s == UdpShutdownState::Incomplete)
                .unwrap_or(UdpShutdownState::Quarantined),
        }
    }
    pub(in crate::tuic) fn native_enabled(&self) -> bool {
        self.native.get().is_some()
    }
    pub(super) fn prepare_monitor_join(
        &self,
        task: JoinHandle<()>,
    ) -> Result<impl Future<Output = bool> + '_, JoinHandle<()>> {
        let Some(native) = self.native.get() else {
            return Err(task);
        };
        let mut lease = match native.acquire(Role::Shutdown) {
            Ok(lease) => lease,
            Err(error) => {
                self.callback_error(error);
                return Err(task);
            }
        };
        let guard = lease.guard();
        {
            let mut state = native.lock();
            let Some(stock) = state.as_mut() else {
                return Err(task);
            };
            if !matches!(stock.monitor, Monitor::Available) {
                return Err(task);
            }
            stock.monitor = Monitor::Joining(task); // unique original handle, before first poll
        }
        Ok(async move {
            let mut work = Some(std::future::poll_fn(|cx| {
                let ready = {
                    let mut state = native.lock();
                    let Some(stock) = state.as_mut() else {
                        return Poll::Ready(false);
                    };
                    let Monitor::Joining(handle) = &mut stock.monitor else {
                        return Poll::Ready(false);
                    };
                    // cx contains only this role's safe proxy. No original callback
                    // is invoked or dropped by handle.poll while the stock is locked.
                    let result = match Pin::new(handle).poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => result,
                    };
                    let handle = match std::mem::replace(&mut stock.monitor, Monitor::Failed) {
                        Monitor::Joining(handle) => handle,
                        _ => return Poll::Ready(false),
                    };
                    (handle, result)
                };
                guard.dispose(ready.0, Field::Source0);
                let complete = match ready.1 {
                    Ok(()) => !guard.failed(),
                    Err(error) => {
                        match error.try_into_panic() {
                            Ok(payload) => guard.capture(Field::Join, payload),
                            Err(cancelled) => {
                                if let Some(clock) = native.clock() {
                                    clock.retained(&RetainedFailure::from_join(
                                        udp_origin(),
                                        cancelled,
                                    ));
                                }
                                guard.quarantine();
                            }
                        }
                        false
                    }
                };
                if complete {
                    if let Some(stock) = native.lock().as_mut() {
                        stock.monitor = Monitor::Complete;
                    }
                }
                Poll::Ready(complete)
            }));
            let result = match lease.run(&mut work) {
                Ok(work) => matches!(work.await, Ok(true)),
                Err(_) => false,
            };
            if !result {
                guard.quarantine();
            }
            result
        })
    }
    pub(super) fn finish_lane(&self) -> bool {
        self.native.get().is_none_or(Native::finish_lane)
    }
    pub(in crate::tuic) fn retire_native(&self, monitored: bool) -> bool {
        self.native
            .get()
            .is_none_or(|native| native.retire(monitored))
    }
    pub(super) async fn finish_source_proof(
        &self,
        epoch: u64,
    ) -> Result<bool, callbacks::CallbackError> {
        let Some(native) = self.native.get() else {
            return Ok(false);
        };
        let deadline = self.request_stop();
        let mut lease = match native.acquire(Role::Shutdown) {
            Ok(lease) => lease,
            Err(error) => {
                self.callback_error(error);
                return Err(error);
            }
        };
        let mut work = Some(async {
            loop {
                let changed = self.generation.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.finish_lane() {
                    break;
                }
                if native.meter.is_quarantined() || tokio::time::Instant::now() >= deadline {
                    return false;
                }
                tokio::select! { _ = &mut changed => {}, _ = tokio::time::sleep_until(deadline) => {} }
            }
            self.generation.wait_sealed(epoch).await;
            self.generation.valid()
        });
        match lease.run(&mut work) {
            Ok(work) => match work.await {
                Ok(settled) => Ok(settled),
                Err(failure) => Err(failure.error),
            },
            Err(error) => Err(error),
        }
    }
}
fn cold_error(error: MeteredCopyError) -> ClientError {
    let kind = match error {
        MeteredCopyError::BudgetFull | MeteredCopyError::Allocation => io::ErrorKind::OutOfMemory,
        MeteredCopyError::TooLarge => io::ErrorKind::InvalidInput,
        MeteredCopyError::Quarantined => io::ErrorKind::InvalidData,
    };
    ClientError::Io(io::Error::new(kind, format!("UDP cold: {error:?}")))
}

pub(in crate::tuic) fn recovery(
    owner: Arc<node::StartupResources>,
    data: Arc<TuicUpstream>,
    config: TuicClientConfig,
) -> Result<impl Future<Output = Result<(), ClientError>> + Send + 'static + use<>, ClientError> {
    let native = data
        .udp_runtime
        .native
        .get()
        .ok_or_else(|| io_err("UDP recovery", "cold stock missing"))?;
    let mut lease = native.acquire(Role::Recovery).map_err(|error| {
        data.udp_runtime.callback_error(error);
        node::recorded_failure()
    })?;
    let guard = lease.guard();
    let owner = guard.own(owner, Field::Source0);
    let data = guard.own(data, Field::Source1);
    let config = guard.own(config, Field::Source2);
    Ok(async move {
        let mut original = Some(node::recovery::run_native(&owner, &data, &config, &guard));
        let result = match lease.run(&mut original) {
            Ok(work) => work.await,
            Err(error) => Err(callbacks::CallbackFailure {
                output: None,
                error,
            }),
        };
        drop(original);
        match result {
            Ok(result) => result,
            Err(failure) => {
                // A Q return retains any produced original error until its own
                // independent guarded disposal; never let async auto-Drop it.
                if let Some(result) = failure.output {
                    guard.dispose(result, Field::Source3);
                }
                Err(node::recorded_failure())
            }
        }
    })
}
impl UdpRuntime {
    pub(in crate::tuic) fn native_recovery(
        &self,
        owner: Arc<node::StartupResources>,
        data: Arc<TuicUpstream>,
        config: TuicClientConfig,
    ) -> Result<
        Option<impl Future<Output = Result<(), ClientError>> + Send + 'static + use<>>,
        ClientError,
    > {
        if self.native.get().is_none() {
            return Ok(None);
        }
        recovery(owner, data, config).map(Some)
    }
}

pub(super) fn start(
    upstream: Arc<TuicUpstream>,
    sender: GenerationSender,
    tasks: TaskRoot,
    mut lease: callbacks::Lease,
) -> impl Future<Output = UdpOutcome> + Send {
    // All actual opaque fields are already in custody even if never polled.
    let guard = lease.guard();
    let upstream = guard.own(upstream, Field::Source0);
    let sender = guard.own(DownlinkSender::Typed(sender), Field::Source1);
    let mut children = NativeTasks::bind(tasks, &guard);
    async move {
        let mut outcome = UdpOutcome::default();
        let mut body = Some(udp_body(
            &upstream,
            &sender,
            &mut children,
            &mut outcome,
            Some(&guard),
        ));
        let result = match lease.run(&mut body) {
            Ok(work) => work.await,
            Err(error) => {
                upstream.udp_runtime.callback_error(error);
                Err(callbacks::CallbackFailure {
                    output: None,
                    error,
                })
            }
        };
        drop(body);
        if result.is_err() {
            outcome.quarantined = true;
        }
        upstream.udp_runtime.request_stop();
        // JOIN still polls with this parent's safe role, never a raw task waker.
        let mut joining = Some(async {
            while let Some(child) = children.join_next().await {
                outcome.child(child, &upstream.udp_runtime, Some(&guard));
            }
        });
        let failed = match lease.run(&mut joining) {
            Ok(work) => work.await.is_err(),
            Err(_) => true,
        };
        drop(joining);
        outcome.quarantined |= failed;
        outcome
    }
}

pub(super) fn child(
    upstream: &Arc<TuicUpstream>,
    sender: DownlinkSender,
    recv: quinn::RecvStream,
    epoch: u64,
) -> Result<impl Future<Output = UdpOutcome> + Send + 'static + use<>, callbacks::CallbackError> {
    let native = upstream
        .udp_runtime
        .native
        .get()
        .ok_or(callbacks::CallbackError::Quarantined)?;
    let mut lease = native.acquire(Role::Child)?;
    let guard = lease.guard();
    let runtime = guard.own(upstream.udp_runtime.clone(), Field::Source0);
    let sender = guard.own(sender, Field::Source1);
    let mut recv = guard.own(recv, Field::Source2);
    let mut stop = guard.own(upstream.udp_runtime.stop.subscribe(), Field::Source3);
    let generation = guard.own(upstream.udp_runtime.generation.clone(), Field::Source4);
    let metrics = guard.own(upstream.metrics.clone(), Field::Source5);
    // Before the first read; no hidden read_to_end Vec or growing scratch.
    let scratch = match bytes::BytesMut::try_zeroed_metered(MAX_TUIC_PACKET_BYTES, &native.meter) {
        Ok(scratch) => Some(scratch),
        Err(
            MeteredCopyError::BudgetFull
            | MeteredCopyError::TooLarge
            | MeteredCopyError::Allocation,
        ) => {
            metrics.inc_udp_drops_down();
            None
        }
        Err(MeteredCopyError::Quarantined) => None,
    };
    let mut scratch = guard.own(scratch, Field::Source6);
    #[cfg(test)]
    let fault = runtime.fault.swap(0, Ordering::Relaxed);
    Ok(async move {
        let mut work = Some(async {
            #[cfg(test)]
            if fault == 2 {
                panic!("injected UDP child panic");
            }
            let Some(scratch) = scratch.as_mut() else {
                return;
            };
            let reading = async {
                let mut cursor = 0usize;
                loop {
                    let room = MAX_TUIC_PACKET_BYTES - cursor;
                    let chunk = recv.read_chunk(room.saturating_add(1), true).await.ok()?;
                    let Some(chunk) = chunk else {
                        return (cursor != 0).then(|| scratch.split_to(cursor).freeze());
                    };
                    if chunk.bytes.len() > room {
                        return None;
                    }
                    scratch[cursor..cursor + chunk.bytes.len()].copy_from_slice(&chunk.bytes);
                    cursor += chunk.bytes.len();
                }
            };
            let packet = tokio::select! { biased;
                _ = stopped(&mut stop) => return,
                _ = generation.wait_frozen(epoch) => return,
                packet = tokio::time::timeout(Duration::from_secs(5), reading) => packet.ok().flatten(),
            };
            #[cfg(test)]
            if runtime
                .fault
                .compare_exchange(4, 0, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                panic!("injected UDP child panic after blocked read");
            }
            if let Some(packet) = packet {
                tokio::select! { biased;
                    _ = stopped(&mut stop) => {},
                    _ = generation.wait_frozen(epoch) => {},
                    result = sender.send(epoch, packet) => {
                        if let Err(crate::shared::bounded_channel::ReserveError::Capacity) = result {
                            runtime.callback_error(callbacks::CallbackError::Capacity);
                        }
                    }
                }
            } else {
                metrics.inc_udp_drops_down();
            }
        });
        let failed = match lease.run(&mut work) {
            Ok(work) => work.await.is_err(),
            Err(error) => {
                runtime.callback_error(error);
                true
            }
        };
        drop(work);
        UdpOutcome {
            quarantined: failed || guard.failed(),
            ..UdpOutcome::default()
        }
    })
}

impl Drop for Native {
    fn drop(&mut self) {
        let stock = self
            .stock
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(stock) = stock {
            self.meter.quarantine();
            std::mem::forget(stock); // original cold roots/data/Boxes, never a clean forced Drop
        }
    }
}

#[cfg(test)]
mod tests;

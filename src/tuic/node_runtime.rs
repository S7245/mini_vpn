//! TUIC UDP receiver and endpoint recovery monitor runtime.
//! Owns one saved UDP pump and its bounded children, stop gate and join evidence.
//! Public start_udp is single-use; its caller retains the unique shutdown token.
//! The pump retains children outside its guarded body and joins before completion.
//! Connection startup/drain, stats and monitor settlement are NOT certified here.
//! Their existing implementations remain below; lifecycle tests live in tests/.

use super::*;
use crate::owned_upstream::{
    UdpDownlinkSource,
    generation::{GenerationControl, GenerationSender},
};
use crate::shared::errors::{FailureOrigin, FailurePhase, FailureResource, RetainedFailure};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use tokio::task::{JoinError, JoinHandle, JoinSet};

pub(super) struct UdpRuntime {
    pub(super) monitor_started: AtomicBool,
    pub(super) owner: std::sync::OnceLock<std::sync::Weak<node::StartupResources>>,
    pub(super) generation: Arc<GenerationControl>,
    state: StdMutex<UdpState>,
    stop: watch::Sender<bool>,
    #[cfg(test)]
    pub(super) child_count: AtomicU64,
    #[cfg(test)]
    pub(super) fault: AtomicU64,
    #[cfg(test)]
    blocked_send: AtomicBool,
}

#[derive(Default)]
struct UdpState {
    started: bool,
    typed: bool,
    token_issued: bool,
    task: Option<JoinHandle<UdpOutcome>>,
    outcome: Option<UdpOutcome>,
    deadline: Option<tokio::time::Instant>,
    quarantined: bool,
}

#[derive(Default)]
struct UdpOutcome {
    failure: Option<RetainedFailure>,
    additional: u64,
    quarantined: bool,
    children_started: u64,
    children_joined: u64,
    children_peak: usize,
}

impl UdpState {
    fn publish(&mut self, mut outcome: UdpOutcome) {
        if let Some(mut previous) = self.outcome.take() {
            if let Some(first) = previous.failure.take() {
                if outcome.failure.is_some() {
                    outcome.additional = outcome.additional.saturating_add(1);
                }
                outcome.failure = Some(first);
            }
            outcome.additional = outcome.additional.saturating_add(previous.additional);
            outcome.quarantined |= previous.quarantined;
        }
        self.outcome = Some(outcome);
    }
}

impl UdpOutcome {
    fn record(&mut self, failure: RetainedFailure) {
        self.quarantined = true;
        if self.failure.is_none() {
            self.failure = Some(failure);
        } else {
            self.additional = self.additional.saturating_add(1);
        }
    }

    fn panic(&mut self, payload: Box<dyn std::any::Any + Send>) {
        // Record before destructing unknown user panic data. The child's/pump's
        // enclosing JoinHandle still fails closed if destruction panics again.
        self.record(RetainedFailure::opaque_panic(udp_origin()));
        self.dispose_panic(payload);
    }

    fn dispose_panic(&mut self, payload: Box<dyn std::any::Any + Send>) {
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
            self.record(RetainedFailure::opaque_panic(udp_origin()));
            let _ = catch_unwind(AssertUnwindSafe(|| drop(secondary)));
        }
    }

    fn join_error(&mut self, error: JoinError) {
        match error.try_into_panic() {
            Ok(payload) => self.panic(payload),
            Err(cancelled) => self.record(RetainedFailure::from_join(udp_origin(), cancelled)),
        }
    }

    fn child(&mut self, result: Result<UdpOutcome, JoinError>, runtime: &UdpRuntime) {
        self.children_joined = self.children_joined.saturating_add(1);
        #[cfg(test)]
        runtime.child_count.fetch_sub(1, Ordering::Relaxed);
        match result {
            Ok(mut child) => {
                if let Some(failure) = child.failure.take() {
                    self.record(failure);
                }
                self.additional = self.additional.saturating_add(child.additional);
                self.quarantined |= child.quarantined;
            }
            Err(error) => self.join_error(error),
        }
        if self.quarantined {
            runtime.request_stop();
        }
    }
}

fn udp_origin() -> FailureOrigin {
    FailureOrigin {
        phase: FailurePhase::UdpJoin,
        resource: FailureResource::UdpDriver,
        resource_id: 0,
        attempt_id: None,
    }
}

impl Default for UdpRuntime {
    fn default() -> Self {
        Self {
            monitor_started: AtomicBool::new(false),
            owner: std::sync::OnceLock::new(),
            generation: Arc::new(GenerationControl::default()),
            state: StdMutex::new(UdpState::default()),
            stop: watch::channel(false).0,
            #[cfg(test)]
            child_count: AtomicU64::new(0),
            #[cfg(test)]
            fault: AtomicU64::new(0),
            #[cfg(test)]
            blocked_send: AtomicBool::new(false),
        }
    }
}

impl UdpRuntime {
    pub(super) fn freeze_primary(&self, epoch: u64) -> Result<(), ClientError> {
        let state = self.state();
        if state.started && !state.typed {
            return Err(io_err(
                "UDP generation",
                "raw receiver cannot seal primary recovery",
            ));
        }
        if !self.generation.freeze(epoch) {
            return Err(io_err("UDP generation", "primary epoch mismatch"));
        }
        if !state.started {
            self.generation.seal_unstarted(epoch);
        }
        Ok(())
    }
    pub(super) async fn settle_source(&self) -> bool {
        let typed = self.state().typed;
        if !typed {
            return true;
        }
        let epoch = self.generation.epoch();
        if !self.generation.freeze(epoch) {
            return false;
        }
        self.generation.producer_sealed(epoch); // Caller has joined the original pump.
        self.generation.wait_sealed(epoch).await;
        self.generation.valid()
    }
    pub(super) fn node_owned(&self) -> bool {
        self.owner.get().is_some()
    }
    pub(super) fn node_stopping(&self) -> bool {
        self.node_owned() && *self.stop.borrow()
    }
    pub(super) fn stop_node(&self) {
        self.request_stop();
    }
    fn state(&self) -> std::sync::MutexGuard<'_, UdpState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                state.quarantined = true;
                self.stop.send_replace(true);
                if state.outcome.is_none() {
                    let mut outcome = UdpOutcome::default();
                    outcome.record(RetainedFailure::opaque_panic(udp_origin()));
                    state.outcome = Some(outcome);
                }
                state
            }
        }
    }

    fn request_stop(&self) -> tokio::time::Instant {
        let mut state = self.state();
        let deadline = *state
            .deadline
            .get_or_insert_with(|| tokio::time::Instant::now() + Duration::from_secs(15));
        self.stop.send_replace(true);
        deadline
    }

    fn poll_join(&self, cx: &mut Context<'_>) -> Poll<UdpShutdownState> {
        let mut state = self.state();
        let failed_join = if let Some(task) = state.task.as_mut() {
            let result = match Pin::new(task).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result,
            };
            state.task = None;
            match result {
                Ok(outcome) => {
                    state.publish(outcome);
                    None
                }
                Err(error) => {
                    // Seal unknown debt, preserving any earlier failure. Only the
                    // known cancelled variant may be consumed while locked.
                    state.quarantined = true;
                    let outcome = state.outcome.get_or_insert_with(UdpOutcome::default);
                    if error.is_panic() {
                        outcome.record(RetainedFailure::opaque_panic(udp_origin()));
                        Some(error)
                    } else {
                        outcome.record(RetainedFailure::from_join(udp_origin(), error));
                        None
                    }
                }
            }
        } else {
            None
        };
        drop(state);
        if let Some(error) = failed_join {
            // No lock across conversion/payload Drop: it may reenter request_stop.
            let mut outcome = UdpOutcome::default();
            match error.try_into_panic() {
                Ok(payload) => outcome.dispose_panic(payload),
                Err(cancelled) => {
                    outcome.record(RetainedFailure::from_join(udp_origin(), cancelled))
                }
            }
            self.state().publish(outcome);
        }
        let mut state = self.state();
        if state.started && state.task.is_none() && state.outcome.is_none() {
            state.quarantined = true;
            let mut outcome = UdpOutcome::default();
            outcome.record(RetainedFailure::opaque_panic(udp_origin()));
            state.outcome = Some(outcome);
        }
        Poll::Ready(
            if state.quarantined
                || state.outcome.as_ref().is_some_and(|outcome| {
                    outcome.quarantined || outcome.children_started != outcome.children_joined
                })
            {
                UdpShutdownState::Quarantined
            } else {
                UdpShutdownState::Settled
            },
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UdpShutdownState {
    Settled,
    Incomplete,
    Quarantined,
}

/// Unique observer/control token; all work remains in UdpRuntime across cancellation.
/// Settled certifies only the UDP pump and its children, never the TUIC node.
pub(crate) struct UdpShutdown(Arc<UdpRuntime>);

impl UdpShutdown {
    // The unique node token transfers bounded error facts to the parent ledger.
    pub(super) fn take_node_failure(&mut self) -> (Option<RetainedFailure>, u64) {
        let mut state = self.0.state();
        state.outcome.as_mut().map_or((None, 0), |outcome| {
            (
                outcome.failure.take(),
                std::mem::take(&mut outcome.additional),
            )
        })
    }
    pub(crate) fn request_stop(&self) {
        self.0.request_stop();
    }

    pub(crate) async fn observe_shutdown(&mut self) -> UdpShutdownState {
        let deadline = self.0.request_stop();
        tokio::select! {
            biased;
            state = std::future::poll_fn(|cx| self.0.poll_join(cx)) => state,
            _ = async {
                if tokio::time::Instant::now() < deadline {
                    tokio::time::sleep_until(deadline).await;
                }
            } => UdpShutdownState::Incomplete,
        }
    }

    pub(crate) async fn drive_shutdown(&mut self) -> UdpShutdownState {
        self.request_stop();
        std::future::poll_fn(|cx| self.0.poll_join(cx)).await
    }

    pub(crate) async fn finish(&mut self) {
        let mut state = self.observe_shutdown().await;
        if state == UdpShutdownState::Incomplete {
            eprintln!("TUIC UDP cleanup incomplete; retaining original pump and children");
            state = self.drive_shutdown().await;
        }
        if state == UdpShutdownState::Quarantined {
            eprintln!("TUIC UDP cleanup quarantined; node replacement is prohibited");
            std::future::pending::<()>().await;
        }
    }
}

impl Drop for UdpShutdown {
    fn drop(&mut self) {
        self.request_stop();
    }
}

async fn stopped(stop: &mut watch::Receiver<bool>) {
    let _ = stop.wait_for(|value| *value).await;
}

#[derive(Clone)]
enum DownlinkSender {
    Raw(mpsc::Sender<Vec<u8>>),
    Typed(GenerationSender),
}
impl DownlinkSender {
    fn is_closed(&self) -> bool {
        match self {
            Self::Raw(tx) => tx.is_closed(),
            Self::Typed(tx) => tx.is_closed(),
        }
    }
    async fn closed(&self) {
        match self {
            Self::Raw(tx) => tx.closed().await,
            Self::Typed(tx) => tx.closed().await,
        }
    }
    async fn send(&self, epoch: u64, bytes: Vec<u8>) -> Result<(), ()> {
        match self {
            Self::Raw(tx) => tx.send(bytes).await.map_err(|_| ()),
            Self::Typed(tx) => tx.send(epoch, bytes).await,
        }
    }
}

async fn udp_connection(
    upstream: &TuicUpstream,
    sender: &DownlinkSender,
    stop: &mut watch::Receiver<bool>,
) -> Result<Option<(Connection, u64)>, ClientError> {
    let slot = upstream
        .tcp_pool_slots
        .first()
        .ok_or_else(|| io_err("UDP primary", "missing primary slot"))?;
    let mut state = tokio::select! {
        biased;
        _ = stopped(stop) => return Ok(None),
        _ = sender.closed() => return Ok(None),
        state = slot.state.lock() => state,
    };
    if *stop.borrow() || sender.is_closed() {
        return Ok(None);
    }
    if upstream.udp_runtime.node_owned() && state.current.transport.close_reason().is_some() {
        let epoch = state.current.identity().generation;
        let ticket = upstream.request_aux_recovery(0, state)?;
        // This entry is reached only before acquiring a producer connection or
        // after the preceding iteration joined every original child.
        upstream.udp_runtime.generation.producer_sealed(epoch);
        tokio::select! {
            biased;
            _ = stopped(stop) => return Ok(None),
            _ = sender.closed() => return Ok(None),
            result = upstream.await_aux_recovery(ticket) => result?,
        }
        state = slot.state.lock().await;
    }
    let current = &mut state.current;
    if current.transport.close_reason().is_some() {
        // Once begun, finish the existing bounded attempt. Its connection-drain
        // witness is a separate node prerequisite; a UDP join never certifies it.
        upstream
            .reconnect_locked(0, &mut current.transport, "transport_closed")
            .await?;
        current.note_reconnect_success();
    }
    Ok(Some((
        current.transport.clone(),
        current.identity().generation,
    )))
}

async fn guarded_udp_child(work: impl Future<Output = ()>, runtime: Arc<UdpRuntime>) -> UdpOutcome {
    let mut work = std::pin::pin!(work);
    let mut outcome = UdpOutcome::default();
    if let Err(payload) =
        std::future::poll_fn(
            |cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
                Ok(Poll::Pending) => Poll::Pending,
                Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
                Err(payload) => Poll::Ready(Err(payload)),
            },
        )
        .await
    {
        runtime.request_stop();
        outcome.panic(payload);
    }
    outcome
}

async fn drive_udp(upstream: Arc<TuicUpstream>, sender: DownlinkSender) -> UdpOutcome {
    let mut children = JoinSet::new();
    let mut outcome = UdpOutcome::default();
    let mut body = Box::pin(udp_body(&upstream, &sender, &mut children, &mut outcome));
    let result = std::future::poll_fn(|cx| {
        match catch_unwind(AssertUnwindSafe(|| body.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(())) => Poll::Ready(Ok(())),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await;
    let destruction = catch_unwind(AssertUnwindSafe(|| drop(body)));
    if let Err(payload) = result {
        outcome.panic(payload);
    }
    if let Err(payload) = destruction {
        outcome.panic(payload);
    }
    upstream.udp_runtime.request_stop();
    #[cfg(test)]
    upstream
        .udp_runtime
        .blocked_send
        .store(false, Ordering::Relaxed);
    while let Some(child) = children.join_next().await {
        outcome.child(child, &upstream.udp_runtime);
    }
    outcome
}

enum UdpTurn {
    Continue,
    Disconnected,
    Stop,
}

async fn udp_body(
    me: &Arc<TuicUpstream>,
    downlink_tx: &DownlinkSender,
    children: &mut JoinSet<UdpOutcome>,
    outcome: &mut UdpOutcome,
) {
    let mut stop = me.udp_runtime.stop.subscribe();
    let mut attempt: u32 = 0;
    'driver: loop {
        if *stop.borrow() || downlink_tx.is_closed() {
            return;
        }
        let (conn, epoch) = match udp_connection(me, downlink_tx, &mut stop).await {
            Ok(Some(conn)) => {
                attempt = 0;
                conn
            }
            Ok(None) => return,
            Err(error) => {
                if me.udp_runtime.node_owned() {
                    // A root demand keeps its first failure/budget; never retry-mint it.
                    return;
                }
                println!("TUIC UDP 驱动重连失败: {error:?}");
                tokio::select! {
                    biased;
                    _ = stopped(&mut stop) => return,
                    _ = downlink_tx.closed() => return,
                    _ = tokio::time::sleep(udp_reconnect_backoff(attempt)) => {}
                }
                attempt = attempt.saturating_add(1);
                continue;
            }
        };
        let mut hb = tokio::time::interval(Duration::from_secs(TUIC_HEARTBEAT_SECS));
        hb.tick().await;
        let mut stats = tokio::time::interval(Duration::from_secs(UDP_STATS_LOG_SECS));
        stats.tick().await;
        let mut prev_pressured = false;
        let mut work_stop = stop.clone();
        loop {
            let turn = tokio::select! {
                biased;
                _ = stopped(&mut stop) => UdpTurn::Stop,
                _ = downlink_tx.closed() => UdpTurn::Stop,
                _ = me.udp_runtime.generation.wait_frozen(epoch), if me.udp_runtime.node_owned() => UdpTurn::Disconnected,
                turn = async {
                    tokio::select! {
                child = children.join_next(), if !children.is_empty() => {
                    if let Some(child) = child { outcome.child(child, &me.udp_runtime); }
                    if outcome.quarantined { return UdpTurn::Stop; }
                }
                dg = conn.read_datagram() => {
                    let Ok(bytes) = dg else { return UdpTurn::Disconnected; };
                    #[cfg(test)]
                    me.udp_runtime.blocked_send.store(true, Ordering::Relaxed);
                    let send = downlink_tx.send(epoch, bytes.to_vec());
                    tokio::pin!(send);
                    loop {
                        tokio::select! {
                            biased;
                            _ = stopped(&mut work_stop) => return UdpTurn::Stop,
                            _ = me.udp_runtime.generation.wait_frozen(epoch), if me.udp_runtime.node_owned() => return UdpTurn::Disconnected,
                            _ = conn.closed(), if me.udp_runtime.node_owned() => return UdpTurn::Disconnected,
                            child = children.join_next(), if !children.is_empty() => {
                                if let Some(child) = child { outcome.child(child, &me.udp_runtime); }
                                if outcome.quarantined { return UdpTurn::Stop; }
                            }
                            sent = &mut send => {
                                if sent.is_err() { return UdpTurn::Stop; }
                                break;
                            }
                        }
                    }
                    #[cfg(test)]
                    me.udp_runtime.blocked_send.store(false, Ordering::Relaxed);
                }
                stream = conn.accept_uni() => {
                    let Ok(recv) = stream else { return UdpTurn::Disconnected; };
                    if children.len() >= MAX_CONCURRENT_DOWNLINK_STREAMS {
                        me.metrics.inc_udp_drops_down();
                        return UdpTurn::Continue;
                    }
                    let tx = downlink_tx.clone();
                    let metrics = Arc::clone(&me.metrics);
                    let mut child_stop = work_stop.clone();
                    #[cfg(test)]
                    let fault = me.udp_runtime.fault.swap(0, Ordering::Relaxed);
                    let child_runtime = Arc::clone(&me.udp_runtime);
                    #[cfg(test)]
                    let test_runtime = Arc::clone(&me.udp_runtime);
                    let generation = me.udp_runtime.generation.clone();
                    let owned = me.udp_runtime.node_owned();
                    children.spawn(guarded_udp_child(async move {
                        #[cfg(test)]
                        if fault == 2 { panic!("injected UDP child panic"); }
                        let packet = tokio::select! {
                            biased;
                            _ = stopped(&mut child_stop) => return,
                            _ = generation.wait_frozen(epoch), if owned => return,
                            packet = tokio::time::timeout(Duration::from_secs(5), read_uni_packet(recv)) => packet.ok().flatten(),
                        };
                        #[cfg(test)]
                        if test_runtime.fault.compare_exchange(4, 0, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                            panic!("injected UDP child panic after blocked read");
                        }
                        if let Some(packet) = packet {
                            tokio::select! {
                                biased;
                                _ = stopped(&mut child_stop) => {},
                                _ = generation.wait_frozen(epoch), if owned => {},
                                _ = tx.send(epoch, packet) => {},
                            }
                        } else { metrics.inc_udp_drops_down(); }
                    }, child_runtime));
                    outcome.children_started = outcome.children_started.saturating_add(1);
                    outcome.children_peak = outcome.children_peak.max(children.len());
                    #[cfg(test)]
                    me.udp_runtime.child_count.fetch_add(1, Ordering::Relaxed);
                    #[cfg(test)]
                    if fault == 1 { panic!("injected UDP parent panic with a live child"); }
                }
                _ = stats.tick() => {
                    let fb = me.udp_stream_fallbacks.load(Ordering::Relaxed);
                    let drops = me.udp_drops.load(Ordering::Relaxed);
                    let now = me.clock.elapsed().as_secs();
                    let last = me.last_udp_activity.load(Ordering::Relaxed);
                    let udp_active = should_send_heartbeat(last, now, TUIC_HB_IDLE_WINDOW_SECS);
                    let max_dg = conn.max_datagram_size();
                    let space = conn.datagram_send_buffer_space();
                    let pressured = me.udp_relay_mode == UdpRelayMode::Native
                        && is_datagram_pressured(space, max_dg.unwrap_or(quic::QUIC_INITIAL_MTU as usize));
                    if note_pressure_edge(pressured, &mut prev_pressured) {
                        me.metrics.inc_datagram_pressure_events();
                    }
                    if udp_active || fb > 0 || drops > 0 {
                        let path = conn.stats().path;
                        let line = format_udp_stats(max_dg, fb, drops, path.rtt.as_millis(), path.cwnd,
                            path.lost_packets, path.sent_packets, space);
                        if pressured { println!("{line} ⚠️背压(datagram 缓冲将丢最老)"); }
                        else { println!("{line}"); }
                    }
                }
                _ = hb.tick() => {
                    let now = me.clock.elapsed().as_secs();
                    let last = me.last_udp_activity.load(Ordering::Relaxed);
                    if should_send_heartbeat(last, now, TUIC_HB_IDLE_WINDOW_SECS)
                        && conn.send_datagram(encode_heartbeat().into()).is_err() { return UdpTurn::Disconnected; }
                }
                    }
                    UdpTurn::Continue
                } => turn,
            };
            match turn {
                UdpTurn::Continue => {}
                UdpTurn::Disconnected => break,
                UdpTurn::Stop => return,
            }
        }
        if me.udp_runtime.node_owned() {
            if !me.udp_runtime.generation.frozen(epoch) {
                let state = me.tcp_pool_slots[0].state.lock().await;
                if let Err(error) = me.request_aux_recovery(0, state) {
                    outcome.record(RetainedFailure::from_client(udp_origin(), error));
                    return;
                }
            }
            drop(conn);
            while let Some(child) = children.join_next().await {
                outcome.child(child, &me.udp_runtime);
            }
            if outcome.quarantined {
                return;
            }
            me.udp_runtime.generation.producer_sealed(epoch);
            continue;
        }
        tokio::select! {
            biased;
            _ = stopped(&mut stop) => break 'driver,
            _ = downlink_tx.closed() => break 'driver,
            _ = tokio::time::sleep(udp_reconnect_backoff(attempt)) => {},
        }
        attempt = attempt.saturating_add(1);
    }
}

impl TuicUpstream {
    /// Starts the sole UDP pump. A duplicate start returns a closed receiver.
    pub fn start_udp(self: &Arc<Self>) -> mpsc::Receiver<Vec<u8>> {
        let (sender, receiver) = mpsc::channel(TUIC_DOWNLINK_CAPACITY);
        let mut state = self.udp_runtime.state();
        if state.started
            || *self.udp_runtime.stop.borrow()
            || self
                .udp_runtime
                .generation
                .frozen(self.udp_runtime.generation.epoch())
        {
            eprintln!("TUIC UDP start rejected: already started or stopping");
            return receiver;
        }
        state.started = true;
        let upstream = Arc::clone(self);
        state.task = Some(tokio::spawn(drive_udp(
            upstream,
            DownlinkSender::Raw(sender),
        )));
        drop(state); // Never acquire node ledger while holding UdpState.
        self.start_endpoint_recovery_monitor();
        receiver
    }

    /// Single original typed queue; required for primary generation custody.
    pub(crate) fn start_udp_source(self: &Arc<Self>) -> Result<UdpDownlinkSource, ClientError> {
        if !self.udp_runtime.node_owned() {
            return Err(io_err("UDP source", "requires attached NodeHost"));
        }
        let mut state = self.udp_runtime.state();
        let control = &self.udp_runtime.generation;
        if state.started || *self.udp_runtime.stop.borrow() || control.frozen(control.epoch()) {
            return Err(io_err("UDP source", "already started, frozen or stopped"));
        }
        let (sender, source) =
            crate::owned_upstream::generation::channel(control.clone(), TUIC_DOWNLINK_CAPACITY);
        state.started = true;
        state.typed = true;
        state.task = Some(tokio::spawn(drive_udp(
            self.clone(),
            DownlinkSender::Typed(sender),
        )));
        drop(state);
        self.start_endpoint_recovery_monitor();
        Ok(source)
    }

    pub(crate) fn take_udp_shutdown(&self) -> Option<UdpShutdown> {
        let mut state = self.udp_runtime.state();
        if state.token_issued {
            return None;
        }
        state.token_issued = true;
        Some(UdpShutdown(Arc::clone(&self.udp_runtime)))
    }

    pub(in crate::tuic) fn start_endpoint_recovery_monitor(self: &Arc<Self>) {
        if self
            .udp_runtime
            .monitor_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let weak = Arc::downgrade(self);
        let mut stop = self.udp_runtime.stop.subscribe();
        let spawn = || {
            tokio::spawn(async move {
                let mut state = EndpointRecoveryState::default();
                let mut evidence_observer =
                    tcp_diag_enabled().then(RecoveryEvidenceObserver::default);
                let mut last_tcp_pool_activity = None;
                let mut tick = tokio::time::interval(ENDPOINT_RECOVERY_SAMPLE_INTERVAL);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        biased;
                        _ = stopped(&mut stop) => return,
                        _ = tick.tick() => {}
                    }
                    let Some(upstream) = weak.upgrade() else {
                        return;
                    };
                    if let Some(owner) = upstream
                        .udp_runtime
                        .owner
                        .get()
                        .and_then(std::sync::Weak::upgrade)
                    {
                        owner.wake_recovery();
                    }
                    let Some(input) = upstream.endpoint_recovery_input() else {
                        continue;
                    };
                    if let Some(active_leases) = note_tcp_pool_activity_transition(
                        &mut last_tcp_pool_activity,
                        input.active_tcp,
                    ) {
                        println!("🔎 tuic-tcp-pool-activity active_leases={active_leases}");
                    }
                    let now = Instant::now();
                    if let Some(observer) = evidence_observer.as_mut() {
                        for event in observer.observe(now, &input) {
                            log_recovery_evidence(event);
                        }
                    }
                    if *stop.borrow() {
                        return;
                    }
                    match state.observe(now, &input) {
                        EndpointRecoveryAction::None => {}
                        EndpointRecoveryAction::Recovered {
                            generation,
                            socket_generation,
                            recovery_time,
                            connection_count,
                        } => {
                            println!(
                                "✅ tuic-endpoint-rebind-recovered generation={generation} first_rx_ms={} socket_generation={socket_generation} connections={connection_count}",
                                recovery_time.as_millis(),
                            );
                        }
                        EndpointRecoveryAction::Rebind {
                            generation,
                            trigger,
                            stalled_for,
                            stall_bound,
                            tx_bytes_since_rx,
                            max_rtt,
                        } => {
                            let (
                                write_conn,
                                write_writer,
                                write_stream,
                                write_episode,
                                write_acknowledged_bytes,
                                write_pending_ms,
                            ) = trigger.tcp_write_fields();
                            let (
                                read_conn,
                                read_reader,
                                read_stream,
                                read_episode,
                                read_offset,
                                read_next_offset,
                                read_highest_offset,
                                read_buffered_bytes,
                                read_gap_bytes,
                                read_observations,
                            ) = trigger.tcp_read_fields();
                            match quic::rebind_client_endpoint_udp_socket(
                                &upstream.endpoint,
                                upstream.udp_send_service_policy,
                                upstream.udp_send_service_stats.as_ref(),
                            ) {
                                Ok(rebound) => {
                                    state.note_rebind_succeeded(now, rebound.rebind_generation);
                                    println!(
                                        "🔄 tuic-endpoint-rebind generation={generation} trigger={} write_conn={} write_writer={} write_stream={} write_episode={} write_acknowledged={}B write_pending_ms={} read_conn={read_conn} read_reader={read_reader} read_stream={read_stream} read_episode={read_episode} read_offset={read_offset} read_next_offset={read_next_offset} read_highest_offset={read_highest_offset} read_buffered={read_buffered_bytes}B read_gap={read_gap_bytes}B read_observations={read_observations} old_local={} new_local={} socket_generation={} active_tcp={} udp_active={} live_connections={} stalled_ms={} bound_ms={} tx_since_rx={}B max_rtt_ms={}",
                                        trigger.label(),
                                        write_conn,
                                        write_writer,
                                        write_stream,
                                        write_episode,
                                        write_acknowledged_bytes,
                                        write_pending_ms,
                                        rebound.old_local_addr,
                                        rebound.new_local_addr,
                                        rebound.rebind_generation,
                                        input.active_tcp,
                                        input.udp_active,
                                        input.connections.len(),
                                        stalled_for.as_millis(),
                                        stall_bound.as_millis(),
                                        tx_bytes_since_rx,
                                        max_rtt.as_millis(),
                                    );
                                }
                                Err(error) => {
                                    println!(
                                        "⚠️ tuic-endpoint-rebind-failed generation={generation} trigger={} write_conn={} write_writer={} write_stream={} write_episode={} write_acknowledged={}B write_pending_ms={} read_conn={read_conn} read_reader={read_reader} read_stream={read_stream} read_episode={read_episode} read_offset={read_offset} read_next_offset={read_next_offset} read_highest_offset={read_highest_offset} read_buffered={read_buffered_bytes}B read_gap={read_gap_bytes}B read_observations={read_observations} active_tcp={} udp_active={} live_connections={} stalled_ms={} bound_ms={} tx_since_rx={}B max_rtt_ms={} error={error}",
                                        trigger.label(),
                                        write_conn,
                                        write_writer,
                                        write_stream,
                                        write_episode,
                                        write_acknowledged_bytes,
                                        write_pending_ms,
                                        input.active_tcp,
                                        input.udp_active,
                                        input.connections.len(),
                                        stalled_for.as_millis(),
                                        stall_bound.as_millis(),
                                        tx_bytes_since_rx,
                                        max_rtt.as_millis(),
                                    );
                                }
                            }
                        }
                    }
                }
            })
        };
        if let Some(owner) = self.udp_runtime.owner.get() {
            if let Some(owner) = owner.upgrade() {
                owner.spawn_monitor(spawn);
            }
        } else {
            drop(spawn()); // Temporary legacy path; final constructor migration removes it.
        }
    }

    fn endpoint_recovery_input(&self) -> Option<EndpointRecoveryInput> {
        let mut connections = Vec::with_capacity(self.tcp_pool_slots.len() + 1);
        let sampled_at = Instant::now();
        for slot in &self.tcp_pool_slots {
            let state = slot.state.try_lock().ok()?;
            for generation in std::iter::once(&state.current)
                .chain(state.draining.as_ref().map(|draining| &draining.generation))
            {
                let conn = &generation.transport;
                if conn.close_reason().is_some() {
                    continue;
                }
                let stats = conn.stats();
                let stable_id = conn.stable_id();
                connections.push(EndpointRecoveryConnectionSample {
                    stable_id,
                    tx_bytes: stats.udp_tx.bytes,
                    rx_bytes: stats.udp_rx.bytes,
                    rtt: stats.path.rtt,
                    black_holes_detected: stats.path.black_holes_detected,
                    current_socket_rx_rebind_generation: conn.current_socket_rx_rebind_generation(),
                    tcp_write_pressures: generation.write_pressure.snapshots_at(sampled_at),
                    tcp_ordered_read_progress: generation.read_pressure.snapshots(),
                });
            }
        }
        let active_tcp = self.tcp_pool_admission.active_total();
        let now = self.clock.elapsed().as_secs();
        let last_udp_activity = self.last_udp_activity.load(Ordering::Relaxed);
        let udp_active = should_send_heartbeat(last_udp_activity, now, TUIC_HB_IDLE_WINDOW_SECS);
        Some(EndpointRecoveryInput {
            active_tcp,
            udp_active,
            last_udp_activity_secs: last_udp_activity,
            current_socket_rx_rebind_generation: self
                .endpoint
                .stats()
                .current_socket_rx_rebind_generation,
            connections,
        })
    }
}

/// 读满一条下行 uni-stream（一个完整 TUIC Packet 命令），上限 `MAX_TUIC_PACKET_BYTES`。
/// 中文要点：流过大/被 reset/读错 → None（丢弃该包，UDP 自愈）。空流（finish 无数据）→ None。
async fn read_uni_packet(mut recv: quinn::RecvStream) -> Option<Vec<u8>> {
    match recv.read_to_end(MAX_TUIC_PACKET_BYTES).await {
        Ok(buf) if !buf.is_empty() => Some(buf),
        _ => None,
    }
}

pub(super) fn spawn_quic_stats_logger(
    conn: Connection,
    conn_index: usize,
    interval_secs: u64,
    mut stop_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = conn.closed() => break,
                _ = tick.tick() => {
                    let stable_id = conn.stable_id();
                    if let Some(reason) = conn.close_reason() {
                        println!("📊 TUIC QUIC stats conn={conn_index} id={stable_id} closed={reason:?}");
                        break;
                    }
                    println!(
                        "{}",
                        format_quic_stats_line(conn_index, stable_id, quic_stats_snapshot(&conn))
                    );
                    if let Some(stats) = conn.endpoint_pacing_snapshot() {
                        println!(
                            "{}",
                            format_endpoint_pacing_connection_stats_line(
                                conn_index,
                                stable_id,
                                stats,
                            )
                        );
                    }
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

pub(super) fn spawn_endpoint_pacing_stats_logger(
    endpoint: Endpoint,
    interval_secs: u64,
    mut stop_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let Some(stats) = endpoint.endpoint_pacing_snapshot() else {
                        break;
                    };
                    println!("{}", format_endpoint_pacing_stats_line(stats));
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

pub(super) fn spawn_udp_send_service_stats_logger(
    stats: quic::QuicUdpSendServiceStats,
    interval_secs: u64,
    mut stop_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    println!("{}", format_udp_send_service_stats_line(stats.snapshot()));
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
    })
}

impl Drop for TuicUpstream {
    fn drop(&mut self) {
        if let Some(stop) = &self.quic_stats_stop {
            let _ = stop.send(true);
        }
    }
}

#[cfg(test)]
mod tests;

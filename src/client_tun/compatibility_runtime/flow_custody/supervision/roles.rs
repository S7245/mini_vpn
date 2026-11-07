//! Root-owned Generic role transaction: gated preparation, actual original
//! joins, ordered failure custody and guarded outcome disposal. Owns no W/F
//! admission, transport policy or DATA scheduling. Entry `FlowRoles`; private
//! tests retain their original physical files under supervision/tests.

use super::*;

// Root-owned roles for the Generic/Legacy engine. Gates are opened only after the
// original loop acknowledges publication. No detached outer task can certify joins.
use super::super::relay_reader::{
    PublishedReceipt, ReaderDiscardReceipt, ReaderOutcome, ReaderOutput,
    run_custodied_relay_reader_gated,
};
use super::super::relay_writer::{
    WriterDiscardReceipt, WriterOutcome, run_custodied_relay_writer_gated,
};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

mod byte_owned;
mod native;
mod failure;
mod release;

pub(in crate::client_tun::compatibility_runtime::flow_custody) enum OriginalFailure {
    Io(std::io::Error),
    OpaquePanic,
    ReportedIo(std::io::Error),
    ReportedPanic,
}

enum Role<T> {
    Empty,
    Running(tokio::task::JoinHandle<T>),
    Joined(T),
    Disposing, // Actual JOIN remains true; only the original exclusive turn owns T.
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
    back: Option<crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>>,
}

async fn run_custodied_monitor(
    mut monitor: GenericRelayMonitor,
    back: crate::client_tun::relay_channel::Sender<(SocketHandle, RelayEvent)>,
    mut stop: oneshot::Receiver<()>,
    gate: oneshot::Receiver<()>,
    failures: Option<FailureClock>,
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
    let (reason, body_panic) = match result {
        Ok(reason) => (reason, None),
        Err(panic) => (("root", "monitor_panic"), Some(panic)),
    };
    if body_panic.is_some() {
        if let Some(clock) = failures.as_ref().filter(|clock| clock.is_bound()) { clock.panic(); }
    }
    let drop_panic = catch_unwind(AssertUnwindSafe(|| drop(work))).err();
    if drop_panic.is_some() {
        if let Some(clock) = failures.as_ref().filter(|clock| clock.is_bound()) { clock.panic(); }
    }
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

pub(in crate::client_tun::compatibility_runtime::flow_custody) struct FlowRoles {
    failures: FailureClock,
    failure_disposal: bool,
    prepared: Option<GenericLaunchRequest>,
    native: Option<native::Roles>,
    d16: Option<byte_owned::Roles>,
    writer: Role<WriterOutcome>,
    reader: Role<ReaderOutcome>,
    monitor: Role<MonitorOutcome>,
    dispatcher: Role<super::continuous::DispatchOutcome>,
    engine: TcpRelayEngine,
    permit_queue: Option<crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue>,
    starts: [Option<oneshot::Sender<()>>; 4],
    stops: [Option<oneshot::Sender<()>>; 4],
    pub(in crate::client_tun::compatibility_runtime::flow_custody) writer_receipt:
        Option<WriterDiscardReceipt>,
    pub(in crate::client_tun::compatibility_runtime::flow_custody) reader_receipt:
        Option<ReaderDiscardReceipt>,
    #[cfg(test)]
    publication_probe: Option<Arc<super::super::receive_window::PublicationProbe>>,
    #[cfg(test)]
    continuous_queue: Option<AsyncByteBoundedFlowQueue>,
    started: bool,
    stopping: bool,
    preparation_interrupted: bool,
    pub(in crate::client_tun::compatibility_runtime::flow_custody) panic:
        Option<Box<dyn std::any::Any + Send>>,
}
pub(in crate::client_tun::compatibility_runtime::flow_custody) struct NativePreparation {
    native: native::Preparation,
    failures: FailureClock,
}

impl FlowRoles {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn is_native(&self) -> bool {
        self.native.is_some()
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn is_d16(&self) -> bool {
        self.d16.is_some()
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn new(
        request: GenericLaunchRequest,
    ) -> Self {
        Self {
            failures: Arc::new(crate::shared::errors::FailureOrder::default()),
            failure_disposal: false,
            writer: Role::Empty,
            reader: Role::Empty,
            monitor: Role::Empty,
            dispatcher: Role::Empty,
            engine: request.engine,
            permit_queue: None,
            prepared: Some(request),
            native: None,
            d16: None,
            starts: [None, None, None, None],
            stops: [None, None, None, None],
            writer_receipt: None,
            reader_receipt: None,
            #[cfg(test)]
            publication_probe: None,
            #[cfg(test)]
            continuous_queue: None,
            started: false,
            stopping: false,
            preparation_interrupted: false,
            panic: None,
        }
    }

    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn new_native(
        request: NativeLaunchRequest,
    ) -> Result<Self, NativeLaunchRequest> {
        Self::new_native_with_clock(request, Arc::new(crate::shared::errors::FailureOrder::default()))
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn new_native_with_clock(
        request: NativeLaunchRequest, failures: FailureClock,
    ) -> Result<Self, NativeLaunchRequest> {
        let meter = request.native.reader.receive_backing_meter();
        Self::new_native_probed(request, failures, meter).map_err(|(request, _)| request)
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn new_native_probed(
        request: NativeLaunchRequest, failures: FailureClock,
        meter: Option<bytes::BackingMeter>,
    ) -> Result<Self, (NativeLaunchRequest, bytes::MeteredCopyError)> {
        let prepared = match Self::prepare_native(&request, failures, meter) {
            Ok(prepared) => prepared,
            Err(error) => return Err((request, error)),
        };
        Ok(Self::from_native_preparation(request, prepared))
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn prepare_native(
        request: &NativeLaunchRequest, failures: FailureClock, meter: Option<bytes::BackingMeter>,
    ) -> Result<NativePreparation, bytes::MeteredCopyError> {
        let native = native::Roles::prepare_cold(request, failures.clone(), meter)?;
        Ok(NativePreparation { native, failures })
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn from_native_preparation(
        request: NativeLaunchRequest, prepared: NativePreparation,
    ) -> Self {
        let NativePreparation { native: prepared, failures } = prepared;
        let native = native::Roles::from_preparation(request, failures.clone(), prepared);
        Self {
            failures,
            failure_disposal: false,
            prepared: None,
            native: Some(native),
            d16: None,
            writer: Role::Empty,
            reader: Role::Empty,
            monitor: Role::Empty,
            dispatcher: Role::Empty,
            engine: TcpRelayEngine::Legacy,
            permit_queue: None,
            starts: [None, None, None, None],
            stops: [None, None, None, None],
            writer_receipt: None,
            reader_receipt: None,
            #[cfg(test)]
            publication_probe: None,
            #[cfg(test)]
            continuous_queue: None,
            started: false,
            stopping: false,
            preparation_interrupted: false,
            panic: None,
        }
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn take_unstarted_native(&mut self) -> Option<NativeLaunchRequest> {
        if self.started || self.stopping || self.preparation_interrupted || self.failure_disposal
            || self.prepared.is_some() || self.d16.is_some() || self.panic.is_some()
            || !matches!(self.writer, Role::Empty) || !matches!(self.reader, Role::Empty)
            || !matches!(self.monitor, Role::Empty) || !matches!(self.dispatcher, Role::Empty)
            || self.starts.iter().any(Option::is_some) || self.stops.iter().any(Option::is_some)
            || self.writer_receipt.is_some() || self.reader_receipt.is_some() { return None; }
        self.native.as_mut()?.take_unstarted()
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn new_d16(
        request: D16LaunchRequest,
    ) -> Result<Self, D16LaunchRequest> {
        let failures = Arc::new(crate::shared::errors::FailureOrder::default());
        let d16 = byte_owned::Roles::new(request, failures.clone())?;
        Ok(Self {
            failures,
            failure_disposal: false,
            prepared: None,
            native: None,
            d16: Some(d16),
            writer: Role::Empty,
            reader: Role::Empty,
            monitor: Role::Empty,
            dispatcher: Role::Empty,
            engine: TcpRelayEngine::Legacy,
            permit_queue: None,
            starts: [None, None, None, None],
            stops: [None, None, None, None],
            writer_receipt: None,
            reader_receipt: None,
            #[cfg(test)]
            publication_probe: None,
            #[cfg(test)]
            continuous_queue: None,
            started: false,
            stopping: false,
            preparation_interrupted: false,
            panic: None,
        })
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn commit_d16(
        &mut self,
    ) -> bool {
        self.d16.as_mut().is_some_and(byte_owned::Roles::commit)
    }

    pub(in crate::client_tun) fn d16_queue(
        &self,
    ) -> Option<&crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue> {
        self.d16.as_ref().map(byte_owned::Roles::queue)
    }

    pub(in crate::client_tun) fn d16_budget(
        &self,
    ) -> Option<&crate::tcp_downlink_pump::D16GlobalByteBudget> {
        self.d16.as_ref().map(byte_owned::Roles::budget)
    }

    pub(in crate::client_tun) fn d16_backing_meter(&self) -> Option<&bytes::BackingMeter> {
        self.d16.as_ref().map(byte_owned::Roles::backing_meter)
    }

    pub(in crate::client_tun) fn native_stock_key(&self) -> Option<&crate::tcp_downlink_pump::StockKey> {
        self.native.as_ref().and_then(native::Roles::stock_key)
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn data_disposed(&self) -> bool {
        if self.failure_disposal || self.panic.is_some() { return false; }
        self.native.as_ref().map_or_else(|| self.settled(), native::Roles::data_disposed)
    }

    pub(in crate::client_tun) fn native_backing_meter(&self) -> Option<&bytes::BackingMeter> {
        self.native.as_ref().and_then(native::Roles::backing_meter)
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn native_pump_receipt(
        &self,
    ) -> Option<crate::upstream::NativePumpReceipt> {
        self.native.as_ref().and_then(native::Roles::pump_receipt)
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn native_reader_received_bytes(
        &self,
    ) -> Option<usize> {
        self.native
            .as_ref()
            .and_then(native::Roles::reader_received_bytes)
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn native_direct_source_received_bytes(
        &self,
    ) -> Option<usize> {
        self.native
            .as_ref()
            .and_then(native::Roles::direct_source_received_bytes)
    }

    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn native_pending_reader_error_order(
        &self,
    ) -> Option<usize> {
        self.native
            .as_ref()
            .and_then(native::Roles::pending_reader_error_order)
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn d16_reader_ledger(
        &self,
    ) -> Option<(usize, usize, usize)> {
        self.d16.as_ref().and_then(byte_owned::Roles::reader_ledger)
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn dispose_d16_reader_after_physical_proof(
        &mut self,
    ) -> bool {
        self.d16
            .as_mut()
            .is_some_and(byte_owned::Roles::dispose_reader_after_physical_proof)
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn commit_native(
        &mut self,
    ) -> bool {
        self.native.as_mut().is_some_and(native::Roles::commit)
    }

    // Caller stores Self before invoking this preparation step.
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn prepare(
        &mut self,
        window: super::super::receive_window::ReaderWindow,
    ) {
        if let Some(d16) = &mut self.d16 {
            d16.prepare(window);
            return;
        }
        if let Some(native) = &mut self.native {
            native.prepare(window);
            return;
        }
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
            downlink_backpressure,
        } = request;
        if !matches!(
            engine,
            TcpRelayEngine::Legacy
                | TcpRelayEngine::ThinStaging
                | TcpRelayEngine::ContinuousPump
                | TcpRelayEngine::PermitPump
                | TcpRelayEngine::OrderedEgressPermitPump
        ) {
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
        let (reader_output, reader_window) = if engine == TcpRelayEngine::ThinStaging {
            let (stage_tx, stage_rx) = mpsc::channel(THIN_TCP_RELAY_STAGING_CAPACITY);
            let (start, gate) = oneshot::channel();
            let (stop, stop_rx) = oneshot::channel();
            self.starts[3] = Some(start);
            self.stops[3] = Some(stop);
            let request = super::thin::Request {
                handle,
                epoch,
                rx: stage_rx,
                back: Some(back_tx.clone()),
                signals: Some(reader_tx.clone()),
                window,
                stop: Some(stop_rx),
                gate,
            };
            self.dispatcher = Role::Running(tokio::spawn(async move {
                super::continuous::DispatchOutcome::Thin(super::thin::run(request).await)
            }));
            (ReaderOutput::Stage(stage_tx), None)
        } else if matches!(
            engine,
            TcpRelayEngine::ContinuousPump
                | TcpRelayEngine::PermitPump
                | TcpRelayEngine::OrderedEgressPermitPump
        ) {
            let Some(shared) = super::continuous::Shared::configured(engine, downlink_backpressure)
            else {
                return;
            };
            #[cfg(test)]
            {
                self.continuous_queue = shared.queue.bytes_queue().cloned();
            }
            self.permit_queue = shared.queue.leased_queue().cloned();
            let (start, gate) = oneshot::channel();
            let (stop, stop_rx) = oneshot::channel();
            self.starts[3] = Some(start);
            self.stops[3] = Some(stop);
            let request = super::continuous::Request {
                handle,
                epoch,
                shared: shared.clone(),
                back: Some(back_tx.clone()),
                signals: Some(reader_tx.clone()),
                window,
                stop: Some(stop_rx),
                gate,
            };
            self.dispatcher = Role::Running(tokio::spawn(async move {
                super::continuous::DispatchOutcome::Continuous(
                    super::continuous::run(request).await,
                )
            }));
            (ReaderOutput::Continuous(shared), None)
        } else {
            (ReaderOutput::Main(back_tx.clone()), Some(window))
        };
        let (start, gate) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        self.starts[1] = Some(start);
        self.stops[1] = Some(stop);
        let reader_request = ReaderRequest {
            handle,
            epoch,
            remote_reader: reader,
            back_tx: reader_output,
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
                reader_window,
            )
            .await
        }));
        let (start, gate) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        self.starts[2] = Some(start);
        self.stops[2] = Some(stop);
        self.monitor = Role::Running(tokio::spawn(async move {
            run_custodied_monitor(monitor, back_tx, stop_rx, gate, None).await
        }));
        self.preparation_interrupted = false;
    }

    pub(in crate::client_tun) fn permit_mode(&self) -> Option<DownstreamPermitReleaseMode> {
        if self.d16.is_some() {
            return None;
        }
        if let Some(native) = &self.native {
            return native
                .permit_queue()
                .map(|_| DownstreamPermitReleaseMode::OnEgressDrain);
        }
        match self.engine {
            TcpRelayEngine::PermitPump => Some(DownstreamPermitReleaseMode::OnAdmission),
            TcpRelayEngine::OrderedEgressPermitPump => {
                Some(DownstreamPermitReleaseMode::OnEgressDrain)
            }
            _ => None,
        }
    }

    pub(in crate::client_tun) fn permit_queue(
        &self,
    ) -> Option<&crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue> {
        if self.d16.is_some() {
            return None;
        }
        if let Some(native) = &self.native {
            return native.permit_queue();
        }
        self.permit_queue.as_ref()
    }

    #[cfg(test)]
    pub(in crate::client_tun) fn native_chunk_queue(
        &self,
    ) -> Option<&crate::tcp_downlink_pump::AsyncByteBoundedFlowQueue> {
        self.native.as_ref()?.chunk_queue()
    }

    #[cfg(test)]
    pub(in crate::client_tun) fn native_pump_source_read_bytes(&self) -> Option<usize> {
        self.native.as_ref()?.pump_source_read_bytes()
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn start(&mut self) {
        if let Some(d16) = &mut self.d16 {
            d16.start();
            return;
        }
        if let Some(native) = &mut self.native {
            native.start();
            return;
        }
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

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn stop(&mut self) {
        if self.failure_disposal || self.panic.is_some() { return; }
        if let Some(d16) = &mut self.d16 {
            d16.stop();
            return;
        }
        if let Some(native) = &mut self.native {
            native.stop();
            return;
        }
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
        self.dispatcher.poll(cx);
    }

    /// A poisoned provider cannot be entered again; actual role joins still can.
    /// Retain outcomes untouched (no remainder disposal) under this role owner.
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn poll_fatal_joins(
        &mut self,
        cx: &mut Context<'_>,
    ) {
        if self.failure_disposal || self.panic.is_some() { return; }
        if let Some(d16) = &mut self.d16 {
            d16.poll_fatal_joins(cx);
            return;
        }
        if let Some(native) = &mut self.native {
            native.poll_fatal_joins(cx);
            return;
        }
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

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn poll(
        &mut self,
        cx: &mut Context<'_>,
        producer_stopped: bool,
    ) {
        if self.failure_disposal || self.panic.is_some() { return; }
        if let Some(d16) = &mut self.d16 {
            d16.poll(cx, producer_stopped);
            return;
        }
        if let Some(native) = &mut self.native {
            native.poll(cx, producer_stopped);
            return;
        }
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
            let all_joined = self.join_count()
                == if self.engine != TcpRelayEngine::Legacy {
                    4
                } else {
                    3
                };
            if let Role::Joined(reader) = &mut self.reader {
                if self.engine == TcpRelayEngine::Legacy || all_joined {
                    self.reader_receipt = reader.remainder.discard_local();
                }
            }
            if all_joined {
                if let Role::Joined(dispatcher) = &mut self.dispatcher {
                    dispatcher.dispose(cx);
                    if let Role::Joined(reader) = &self.reader {
                        dispatcher.verify(
                            reader.remainder.published(),
                            reader.notification.closed_sent,
                        );
                    }
                }
            }
        }
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn closed(
        &self,
    ) -> Option<(&'static str, &'static str)> {
        if let Some(d16) = &self.d16 {
            return d16.closed();
        }
        if let Some(native) = &self.native {
            return native.closed();
        }
        if let Role::Joined(dispatcher) = &self.dispatcher {
            if let Some(reason) = dispatcher.reason() {
                return Some(reason);
            }
        }
        match &self.monitor {
            Role::Joined(outcome) => Some(outcome.reason),
            Role::Failed(_) => Some(("root", "monitor_join_failed")),
            _ if self.quarantined() => Some(("root", "role_quarantined")),
            _ => None,
        }
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn published(
        &self,
    ) -> Option<PublishedReceipt> {
        if self.d16.is_some() {
            return Some(PublishedReceipt {
                messages: 0,
                bytes: 0,
            });
        }
        if let Some(native) = &self.native {
            return native.published();
        }
        if self.engine != TcpRelayEngine::Legacy {
            return match &self.dispatcher {
                Role::Joined(outcome) => Some(outcome.published()),
                _ => None,
            };
        }
        match &self.reader {
            Role::Joined(outcome) => Some(outcome.remainder.published()),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn test_queued_data(
        &self,
    ) -> Option<usize> {
        self.publication_probe.as_ref()?.queued_lower_bound()
    }

    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn test_outstanding_data(
        &self,
    ) -> Option<usize> {
        self.publication_probe.as_ref()?.outstanding_lower_bound()
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn read_total(
        &self,
    ) -> Option<usize> {
        if let Some(d16) = &self.d16 {
            return d16.read_total();
        }
        if let Some(native) = &self.native {
            return native.read_total();
        }
        if self.engine != TcpRelayEngine::Legacy {
            return match &self.dispatcher {
                Role::Joined(outcome) => Some(outcome.window_read()),
                _ => None,
            };
        }
        match &self.reader {
            Role::Joined(outcome) => outcome.remainder.read_total(),
            _ => None,
        }
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn published_hints(
        &self,
    ) -> Option<usize> {
        if self.d16.is_some() {
            return Some(0);
        }
        if let Some(native) = &self.native {
            return native.published_hints();
        }
        match &self.monitor {
            Role::Joined(outcome) => Some(outcome.monitor.published_hints),
            _ => None,
        }
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn disposition_in_flight(&self) -> bool { self.failure_disposal }
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn settled(&self) -> bool {
        if self.failure_disposal || self.panic.is_some() { return false; }
        if let Some(d16) = &self.d16 {
            return d16.settled();
        }
        if let Some(native) = &self.native {
            return native.settled();
        }
        !self.quarantined()
            && (self.engine == TcpRelayEngine::Legacy
                || match (&self.reader, &self.dispatcher) {
                    (Role::Joined(reader), Role::Joined(dispatcher)) => {
                        dispatcher.audit(
                            reader.remainder.published(),
                            reader.notification.closed_sent,
                        ) && self.reader_receipt.is_some_and(|receipt| {
                            reader
                                .remainder
                                .published()
                                .bytes
                                .checked_add(receipt.unpublished_bytes)
                                == Some(reader.remainder.remote_total())
                        })
                    }
                    _ => false,
                })
            && self.writer_receipt.is_some()
            && self.reader_receipt.is_some()
            && matches!(&self.monitor, Role::Joined(outcome) if outcome.drained == [true; 2])
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn quarantined(&self) -> bool {
        if self.panic.is_some() { return true; }
        if let Some(d16) = &self.d16 {
            return d16.quarantined();
        }
        if let Some(native) = &self.native {
            return native.quarantined();
        }
        self.preparation_interrupted
            || self.failures.load(std::sync::atomic::Ordering::SeqCst) == usize::MAX
            || (self.started
                && (matches!(&self.writer, Role::Joined(outcome) if !outcome.started)
                    || matches!(&self.reader, Role::Joined(outcome) if !outcome.started)
                    || matches!(&self.monitor, Role::Joined(outcome) if !outcome.started)))
            || (self.engine != TcpRelayEngine::Legacy
                && (matches!(self.dispatcher, Role::Empty | Role::Failed(_))
                    || matches!(&self.dispatcher, Role::Joined(outcome) if outcome.quarantined() || (self.started && !outcome.started()))))
            || self.panic.is_some()
            || matches!(self.writer, Role::Empty | Role::Failed(_))
            || matches!(self.reader, Role::Empty | Role::Failed(_))
            || matches!(self.monitor, Role::Empty | Role::Failed(_))
            || matches!(&self.writer, Role::Joined(outcome) if outcome.panics.iter().any(Option::is_some) || outcome.remainder.quarantined())
            || matches!(&self.reader, Role::Joined(outcome) if outcome.panics.iter().any(Option::is_some) || outcome.notification.in_flight || outcome.remainder.quarantined())
            || matches!(&self.monitor, Role::Joined(outcome) if outcome.panics.iter().any(Option::is_some) || outcome.in_flight || outcome.monitor.hint_in_flight)
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn join_count(&self) -> usize {
        if let Some(d16) = &self.d16 {
            return d16.join_count();
        }
        if let Some(native) = &self.native {
            return native.join_count();
        }
        usize::from(matches!(self.writer, Role::Joined(_)))
            + usize::from(matches!(self.reader, Role::Joined(_)))
            + usize::from(matches!(self.monitor, Role::Joined(_)))
            + usize::from(matches!(self.dispatcher, Role::Joined(_)))
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn all_original_roles_joined(
        &self,
    ) -> bool {
        if let Some(d16) = &self.d16 {
            return d16.all_original_roles_joined();
        }
        if let Some(native) = &self.native {
            return native.all_original_roles_joined();
        }
        self.started
            && matches!(self.writer, Role::Joined(_))
            && matches!(self.reader, Role::Joined(_))
            && matches!(self.monitor, Role::Joined(_))
            && if self.engine == TcpRelayEngine::Legacy {
                matches!(self.dispatcher, Role::Empty)
            } else {
                matches!(self.dispatcher, Role::Joined(_))
            }
    }

    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn native_reader_receipt_present(
        &self,
    ) -> Option<bool> {
        self.native.as_ref().map(native::Roles::reader_receipt_present)
    }

    #[cfg(test)]
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn unwind_native_pump_for_test(
        &self,
        payload: Box<dyn std::any::Any + Send>,
    ) -> Result<(), Box<dyn std::any::Any + Send>> {
        match &self.native {
            Some(native) => native.unwind_pump_for_test(payload),
            None => Err(payload),
        }
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn running_count(
        &self,
    ) -> usize {
        if let Some(d16) = &self.d16 {
            return d16.running_count();
        }
        if let Some(native) = &self.native {
            return native.running_count();
        }
        usize::from(matches!(self.writer, Role::Running(_)))
            + usize::from(matches!(self.reader, Role::Running(_)))
            + usize::from(matches!(self.monitor, Role::Running(_)))
            + usize::from(matches!(self.dispatcher, Role::Running(_)))
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn take_error(
        &mut self,
    ) -> Option<std::io::Error> {
        if let Some(d16) = &mut self.d16 {
            return d16.take_error();
        }
        if self.native.is_some() {
            // Native failures include opaque panic values. Only the Root's
            // typed drain may consume that ordinal and record its first error.
            return None;
        }
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

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn take_original_failure(
        &mut self,
    ) -> Option<OriginalFailure> {
        if let Some(native) = &mut self.native {
            return native.take_failure();
        }
        self.take_error().map(OriginalFailure::Io)
    }
}

// Only failure paths allocate an ordinal; DATA processing never touches this clock.
pub(in crate::client_tun) use crate::shared::errors::FailureClock;
pub(in crate::client_tun) fn stamp_failure(clock: &Option<FailureClock>) -> Option<usize> {
    clock.as_ref().map(|clock| clock.stamp())
}

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/thin.rs"]
mod thin_tests;

#[cfg(test)]
#[path = "roles/tests/continuous.rs"]
mod continuous_tests;

#[cfg(test)]
#[path = "roles/tests/ordered.rs"]
mod ordered_tests;

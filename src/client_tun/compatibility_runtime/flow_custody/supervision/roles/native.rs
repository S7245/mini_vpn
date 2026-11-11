//! Native FlowRoles composition: owns original actor gates, handles, JOIN and
//! receipt settlement. The complete reader lives in private reader; dispatcher,
//! QUIC producer, F and physical SocketCtx retain their original authorities.
//! Entry Roles; tests: transaction/tests and reader/native_inventory.

use super::*;
use crate::upstream::{
    NativePumpFailure, NativePumpFailureValue, NativePumpReceipt, NativeTcpRelayEngine,
    RunningNativePump,
};
mod reader;
mod stock;
#[cfg(test)]
#[path = "tests/native_stock.rs"]
pub(super) mod stock_tests;
mod disposal;
use disposal::Pump;
pub(super) use disposal::Work;
use reader::{DiscardReceipt, Outcome, Request, run};

struct Prepared {
    request: NativeLaunchRequest,
    shared: Arc<super::super::continuous::Shared>,
    window: Option<super::super::super::receive_window::ReaderWindow>,
}

pub(super) struct Preparation {
    stock: Option<stock::Inventory>,
    source: Option<reader::Cold>,
    writer_callbacks: Option<super::super::super::relay_writer::Cold>,
    shared: Arc<super::super::continuous::Shared>,
    backing_meter: Option<bytes::BackingMeter>,
}

/// Four original Native Chunk/Permit actors plus the one optional original
/// ordered producer. All five handles live under the saved FlowTransaction.
pub(super) struct Roles {
    uplink_observer: Option<crate::client_tun::uplink::Observer>,
    stock: Option<stock::Inventory>,
    stock_retiring: bool,
    source: Option<reader::Cold>,
    writer_callbacks: Option<super::super::super::relay_writer::Cold>,
    prepared: Option<Prepared>,
    backing_meter: Option<bytes::BackingMeter>,
    shared: Arc<super::super::continuous::Shared>,
    failures: FailureClock,
    writer: Role<WriterOutcome>,
    reader: Role<Outcome>,
    monitor: Role<MonitorOutcome>,
    dispatcher: Role<super::super::continuous::DispatchOutcome>,
    pump: Pump,
    disposal_cursor: usize,
    pump_start: Option<oneshot::Sender<()>>,
    expected_pump: bool,
    pump_receipt: Option<NativePumpReceipt>,
    pump_failures: Vec<NativePumpFailure>,
    pump_panics: [Option<Box<dyn std::any::Any + Send>>; 9],
    starts: [Option<oneshot::Sender<()>>; 4],
    stops: [Option<oneshot::Sender<()>>; 4],
    writer_receipt: Option<WriterDiscardReceipt>,
    reader_receipt: Option<DiscardReceipt>,
    committed: bool,
    started: bool,
    stopping: bool,
    stop_notified: bool,
    producers: Option<bool>, // None=attached, false=exclusive/uncertain, true=actual Drop.
    retained_producers: Option<disposal::Producers>,
    interrupted: bool,
    failed: bool,
}

impl Roles {
    #[cfg(test)]
    pub(super) fn drop_start_gate_for_test(&mut self, index: usize) {
        assert!(self.committed && !self.started);
        drop(
            self.starts[index]
                .take()
                .expect("actual Native start sender"),
        );
    }

    pub(super) fn prepare_cold(
        request: &NativeLaunchRequest, failures: FailureClock,
        backing_meter: Option<bytes::BackingMeter>,
    ) -> Result<Preparation, bytes::MeteredCopyError> {
        if request.native.has_prepared_pump() && backing_meter.is_none() {
            return Err(bytes::MeteredCopyError::Quarantined);
        }
        // A2 two-tier: the carrier hands over the Node's protocol-tier meter.
        // Every inventory these roles fund is App-side downlink, so it admits
        // at the App ceiling on the same exact authority (D10).
        let backing_meter = backing_meter.map(|meter| meter.app_tier_view());
        let engine = match request.engine {
            NativeTcpRelayEngine::ChunkPump => TcpRelayEngine::ContinuousPump,
            NativeTcpRelayEngine::PermitPump => TcpRelayEngine::OrderedEgressPermitPump,
        };
        let (shared, output) = super::super::continuous::Shared::prepare_native(
            engine, request.downlink_backpressure, backing_meter.clone(), failures.clone(),
        )?;
        let stock = match output {
            Some(output) => Some(stock::Inventory::prepare(output, backing_meter.as_ref().ok_or(bytes::MeteredCopyError::Quarantined)?, failures.clone())?),
            None => None,
        };
        let source = backing_meter.as_ref().filter(|meter| meter.budget_limit().is_some()).map(|meter| reader::Cold::prepare(meter, failures.clone())).transpose()?;
        let writer_callbacks = backing_meter.as_ref().filter(|meter| meter.budget_limit().is_some()).map(|meter| super::super::super::relay_writer::Cold::prepare(meter, failures)).transpose()?;
        Ok(Preparation { stock, source, writer_callbacks, shared, backing_meter })
    }
    pub(super) fn from_preparation(mut request: NativeLaunchRequest, failures: FailureClock, prepared: Preparation) -> Self {
        let Preparation { stock, source, writer_callbacks, shared, backing_meter } = prepared;
        Self {
            uplink_observer: request.uplink_observer.take(),
            stock, stock_retiring: false,
            source, writer_callbacks,
            prepared: Some(Prepared {
                request,
                shared: shared.clone(),
                window: None,
            }),
            backing_meter,
            shared,
            failures,
            writer: Role::Empty,
            reader: Role::Empty,
            monitor: Role::Empty,
            dispatcher: Role::Empty,
            pump: Pump::Empty,
            disposal_cursor: 0,
            pump_start: None,
            expected_pump: false,
            pump_receipt: None,
            pump_failures: Vec::new(),
            pump_panics: std::array::from_fn(|_| None),
            starts: [None, None, None, None],
            stops: [None, None, None, None],
            writer_receipt: None,
            reader_receipt: None,
            committed: false,
            started: false,
            stopping: false,
            stop_notified: false,
            producers: None,
            retained_producers: None,
            interrupted: false,
            failed: false,
        }
    }

    pub(super) fn take_unstarted(&mut self) -> Option<NativeLaunchRequest> {
        if self.writer_callbacks.as_ref().is_some_and(|stock| !stock.unstarted())
            || self.source.as_ref().is_some_and(|source| !source.unstarted())
            || self.committed || self.started || self.interrupted || self.failed
            || !matches!(self.writer, Role::Empty) || !matches!(self.reader, Role::Empty)
            || !matches!(self.monitor, Role::Empty) || !matches!(self.dispatcher, Role::Empty)
            || self.pump.is_some() || self.pump_start.is_some() || self.expected_pump
            || self.pump_receipt.is_some() || !self.pump_failures.is_empty()
            || self.pump_panics.iter().any(Option::is_some)
            || self.starts.iter().any(Option::is_some) || self.stops.iter().any(Option::is_some)
            || self.writer_receipt.is_some() || self.reader_receipt.is_some()
            || self.prepared.as_ref().is_none_or(|prepared| prepared.window.is_some()) { return None; }
        self.prepared.take().map(|mut prepared| { prepared.request.uplink_observer = self.uplink_observer.take(); prepared.request })
    }

    pub(super) fn permit_queue(
        &self,
    ) -> Option<&crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue> {
        self.shared.queue.leased_queue()
    }

    #[cfg(test)]
    pub(super) fn chunk_queue(
        &self,
    ) -> Option<&crate::tcp_downlink_pump::AsyncByteBoundedFlowQueue> {
        self.shared.queue.bytes_queue()
    }

    pub(super) fn backing_meter(&self) -> Option<&bytes::BackingMeter> {
        self.backing_meter.as_ref()
    }

    pub(super) fn pump_receipt(&self) -> Option<NativePumpReceipt> {
        self.pump_receipt
    }
    #[cfg(test)]
    pub(super) fn pump_source_read_bytes(&self) -> Option<usize> {
        self.pump.as_ref().map(RunningNativePump::source_read_bytes)
    }

    pub(super) fn reader_received_bytes(&self) -> Option<usize> {
        match &self.reader {
            Role::Joined(reader) => Some(reader.received_bytes()),
            _ => None,
        }
    }

    pub(super) fn direct_source_received_bytes(&self) -> Option<usize> {
        if self.expected_pump || self.pump.is_some() || self.pump_receipt.is_some() {
            return None;
        }
        match &self.reader {
            Role::Joined(reader) => reader.source_read_bytes(),
            _ => None,
        }
    }

    pub(super) fn prepare(&mut self, window: super::super::super::receive_window::ReaderWindow) {
        self.interrupted = true;
        if let Some(prepared) = self.prepared.as_mut() {
            if prepared.window.is_none() {
                prepared.window = Some(window);
                self.interrupted = false;
            }
        }
    }

    /// Called only after physical queue, SocketCtx and escrow preflights. No
    /// await or fallible admission follows this synchronous registration.
    pub(super) fn commit(&mut self) -> bool {
        self.interrupted = true;
        let Some(mut prepared) = self.prepared.take() else {
            return false;
        };
        let Some(window) = prepared.window.take() else {
            self.prepared = Some(prepared);
            return false;
        };
        let stock_producer = if let Some(stock) = &mut self.stock {
            let Some(producer) = stock.take_producer() else { return false; };
            Some(producer)
        } else { None };
        let source_lease = if let Some(source) = &mut self.source {
            let Some(lease) = source.take_lease() else {
                prepared.window = Some(window);
                self.prepared = Some(prepared);
                return false;
            };
            Some(lease)
        } else { None };
        let writer_lease = if let Some(stock) = &mut self.writer_callbacks {
            let Some(lease) = stock.take_lease() else {
                self.failures.quarantine();
                prepared.window = Some(window);
                self.prepared = Some(prepared);
                return false;
            };
            Some(lease)
        } else { None };
        let NativeLaunchRequest {
            uplink_observer: _,
            handle,
            epoch,
            native,
            engine: _,
            read_floor,
            rx,
            back_tx,
            read_credit_rx,
            downlink_backpressure: _,
        } = prepared.request;
        let (reader, writer, pump) = native.into_parts();
        self.expected_pump = pump.is_some();
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
        self.starts[1] = Some(start);
        self.stops[1] = Some(stop);
        let reader_future = run(
            Request {
                producer: stock_producer,
                backing_meter: self.backing_meter.clone(),
                handle,
                epoch,
                reader: Some(reader).into(),
                shared: prepared.shared.clone(),
                credit: Some(read_credit_rx).into(),
                signals: Some(reader_tx.clone()).into(),
                local_finish: finish,
                stop: Some(stop_rx).into(),
                service_floor: read_floor,
            },
            gate,
            Some(self.failures.clone()), source_lease,
        );

        let (start, gate) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        self.starts[0] = Some(start);
        self.stops[0] = Some(stop);
        self.writer = Role::Running(tokio::spawn(run_custodied_relay_writer_gated(
            handle,
            writer,
            rx,
            writer_tx,
            stop_rx,
            Some(gate),
            super::super::super::relay_writer::WriterControl::native(self.failures.clone(), writer_lease),
        )));

        self.reader = Role::Running(tokio::spawn(reader_future));

        let (start, gate) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        self.starts[3] = Some(start);
        self.stops[3] = Some(stop);
        let dispatcher_back = back_tx.clone();
        self.dispatcher = Role::Running(tokio::spawn(async move {
            super::super::continuous::DispatchOutcome::Continuous(
                super::super::continuous::run(super::super::continuous::Request {
                    handle,
                    epoch,
                    shared: prepared.shared,
                    back: Some(dispatcher_back),
                    signals: Some(reader_tx),
                    window,
                    stop: Some(stop_rx),
                    gate,
                })
                .await,
            )
        }));

        let (start, gate) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        self.starts[2] = Some(start);
        self.stops[2] = Some(stop);
        self.monitor = Role::Running(tokio::spawn(run_custodied_monitor(
            monitor, back_tx, stop_rx, gate, Some(self.failures.clone()),
        )));
        self.pump = pump.map(|pump| {
            let (start, gate) = oneshot::channel();
            self.pump_start = Some(start);
            pump.commit_gated(Some(self.failures.clone()), gate)
        }).into();
        self.committed = true;
        self.interrupted = false;
        true
    }

    pub(super) fn start(&mut self) {
        // Actor-only callers have no Session lock. Production publication uses
        // the same Work through its original exclusive Flow disposition token.
        if let Some(mut work) = self.take_start_disposal() {
            work.run(&mut Context::from_waker(std::task::Waker::noop()));
            work.restore(self);
        }
    }

    pub(super) fn stop(&mut self) { self.stopping = true; }

    fn poll_handles(&mut self, cx: &mut Context<'_>) {
        self.writer.poll(cx);
        self.reader.poll(cx);
        self.monitor.poll(cx);
        self.dispatcher.poll(cx);
        if let Some(pump) = self.pump.as_mut() {
            pump.poll_join(cx);
        }
    }

    fn collect_pump_failures(&mut self) {
        let Some(pump) = self.pump.as_mut() else {
            return;
        };
        // The failure inventory belongs to the joined producer even when the
        // reader's physical discard cannot produce a receipt. Root must see
        // every earlier ordinal before it records any later role error.
        if !pump.joined() {
            return;
        }
        while self.pump_failures.len() < 10 {
            let Some(failure) = pump.take_failure() else {
                break;
            };
            self.pump_failures.push(failure);
        }
        if self.pump_failures.len() == 10 {
            self.failed = true;
        }
    }

    pub(super) fn poll_fatal_joins(&mut self, cx: &mut Context<'_>) {
        self.stop();
        self.poll_handles(cx);
        self.collect_pump_failures();

    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>, producer_stopped: bool) {
        self.poll_handles(cx);
        self.collect_pump_failures();
        let _ = producer_stopped; // Joined disposition is an original lock-free Root turn.
    }

    pub(super) fn closed(&self) -> Option<(&'static str, &'static str)> {
        if let Role::Joined(dispatcher) = &self.dispatcher {
            if let Some(reason) = dispatcher.reason() {
                return Some(reason);
            }
        }
        match &self.monitor {
            Role::Joined(outcome) => Some(outcome.reason),
            Role::Failed(_) => Some(("root", "native_monitor_join_failed")),
            _ if self.quarantined() => Some(("root", "native_role_quarantined")),
            _ => None,
        }
    }

    pub(super) fn published(&self) -> Option<PublishedReceipt> {
        match &self.dispatcher {
            Role::Joined(outcome) => Some(outcome.published()),
            _ => None,
        }
    }

    pub(super) fn read_total(&self) -> Option<usize> {
        match &self.dispatcher {
            Role::Joined(outcome) => Some(outcome.window_read()),
            _ => None,
        }
    }

    pub(super) fn published_hints(&self) -> Option<usize> {
        match &self.monitor {
            Role::Joined(outcome) => Some(outcome.monitor.published_hints),
            _ => None,
        }
    }

    pub(super) fn join_count(&self) -> usize {
        usize::from(matches!(self.writer, Role::Joined(_) | Role::Disposing))
            + usize::from(matches!(self.reader, Role::Joined(_) | Role::Disposing))
            + usize::from(matches!(self.monitor, Role::Joined(_) | Role::Disposing))
            + usize::from(matches!(self.dispatcher, Role::Joined(_) | Role::Disposing))
            + usize::from(self.pump.joined())
    }

    pub(super) fn all_original_roles_joined(&self) -> bool {
        self.committed
            && self.started
            && matches!(self.writer, Role::Joined(_) | Role::Disposing)
            && matches!(self.reader, Role::Joined(_) | Role::Disposing)
            && matches!(self.monitor, Role::Joined(_) | Role::Disposing)
            && matches!(self.dispatcher, Role::Joined(_) | Role::Disposing)
            && match (self.expected_pump, &self.pump) {
                (false, Pump::Empty) => true,
                (true, pump) => pump.joined(),
                _ => false,
            }
    }

    #[cfg(test)]
    pub(super) fn reader_receipt_present(&self) -> bool {
        self.reader_receipt.is_some()
    }

    #[cfg(test)]
    pub(super) fn unwind_pump_for_test(
        &self,
        payload: Box<dyn std::any::Any + Send>,
    ) -> Result<(), Box<dyn std::any::Any + Send>> {
        match self.pump.as_ref() {
            Some(pump) if self.started => {
                pump.unwind_for_test(payload);
                Ok(())
            }
            _ => Err(payload),
        }
    }

    pub(super) fn running_count(&self) -> usize {
        usize::from(matches!(self.writer, Role::Running(_)))
            + usize::from(matches!(self.reader, Role::Running(_)))
            + usize::from(matches!(self.monitor, Role::Running(_)))
            + usize::from(matches!(self.dispatcher, Role::Running(_)))
            + usize::from(self.pump.running())
    }

    pub(super) fn stock_key(&self) -> Option<&crate::tcp_downlink_pump::StockKey> { self.stock.as_ref().map(stock::Inventory::key) }
    pub(super) fn settled(&self) -> bool {
        self.data_disposed() && self.uplink_observer.is_none() && self.writer_callbacks.is_none() && !self.stock_retiring && self.stock.as_ref().is_none_or(stock::Inventory::done)
    }
    pub(super) fn release_ready(&self) -> bool {
        // D172: only a hard (budgeted) source owns producers to dispose; a non-hard
        // flow's producers leave with its mirror, so it must never have taken them.
        let hard = self.backing_meter.as_ref().is_some_and(|meter| meter.budget_limit().is_some());
        self.settled() && self.prepared.is_none() && self.retained_producers.is_none()
            && if hard { self.producers == Some(true) } else { self.producers.is_none() }
            && self.starts.iter().all(Option::is_none) && self.stops.iter().all(Option::is_none)
            && self.pump_start.is_none() && self.pump_failures.is_empty()
            && self.pump_panics.iter().all(Option::is_none)
            && matches!(&self.writer, Role::Joined(o) if o.release_ready())
            && matches!(&self.reader, Role::Joined(o) if o.release_ready())
            && matches!(&self.monitor, Role::Joined(o) if o.release_ready())
            && matches!(&self.dispatcher, Role::Joined(o) if o.native_release_ready())
    }
    pub(super) fn data_disposed(&self) -> bool {
        if self.quarantined()
            || self.source.is_some()
            || !self.committed
            || !self.started
            || !self.stopping
            || self.join_count() != 4 + usize::from(self.expected_pump)
            || self.writer_receipt.is_none()
            || self.reader_receipt.is_none()
            || (self.expected_pump && self.pump_receipt.is_none())
        {
            return false;
        }
        let (Role::Joined(reader), Role::Joined(dispatcher), Role::Joined(monitor)) =
            (&self.reader, &self.dispatcher, &self.monitor)
        else {
            return false;
        };
        if monitor.drained != [true; 2]
            || !dispatcher.audit(reader.queued(), reader.closed_sent())
            || self.reader_receipt.is_none_or(|receipt| {
                receipt
                    .assembler_backing_bytes
                    .checked_add(receipt.pending_bytes)
                    .and_then(|n| n.checked_add(receipt.rejected_bytes))
                    != Some(receipt.unpublished_bytes)
                    || receipt
                        .queued_bytes
                        .checked_add(receipt.unpublished_bytes)
                        .and_then(|total| total.checked_add(receipt.discarded_bytes))
                        != Some(receipt.received_bytes)
            })
        {
            return false;
        }
        self.pump_receipt
            .is_none_or(|receipt| receipt.forwarded_bytes == reader.received_bytes())
    }

    pub(super) fn quarantined(&self) -> bool {
        self.interrupted
            || self.stock.as_ref().is_some_and(stock::Inventory::failed)
            || self.failed
            || self.pump_panics.iter().any(Option::is_some)
            || (self.committed
                && (matches!(self.writer, Role::Empty | Role::Failed(_))
                    || matches!(self.reader, Role::Empty | Role::Failed(_))
                    || matches!(self.monitor, Role::Empty | Role::Failed(_))
                    || matches!(self.dispatcher, Role::Empty | Role::Failed(_))))
            || (self.started
                && (matches!(&self.writer, Role::Joined(outcome) if !outcome.started)
                    || matches!(&self.reader, Role::Joined(outcome) if !outcome.started)
                    || matches!(&self.monitor, Role::Joined(outcome) if !outcome.started)
                    || matches!(&self.dispatcher, Role::Joined(outcome) if !outcome.started())))
            || matches!(&self.writer, Role::Joined(outcome) if outcome.panics.iter().any(Option::is_some) || outcome.remainder.quarantined())
            || matches!(&self.reader, Role::Joined(outcome) if outcome.quarantined())
            || matches!(&self.monitor, Role::Joined(outcome) if outcome.panics.iter().any(Option::is_some) || outcome.in_flight || outcome.monitor.hint_in_flight)
            || matches!(&self.dispatcher, Role::Joined(outcome) if outcome.quarantined())
            || self
                .pump
                .as_ref()
                .is_some_and(RunningNativePump::quarantined)
            || self.failures.load(std::sync::atomic::Ordering::SeqCst) == usize::MAX
    }

    pub(super) fn take_failure(&mut self) -> Option<super::OriginalFailure> {
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
            if reader.has_error() {
                if let Some(order) = reader.error_order() {
                    if first.is_none_or(|(earlier, _)| order < earlier) {
                        first = Some((order, 2));
                    }
                }
            }
        }
        let pump_index = self
            .pump_failures
            .iter()
            .enumerate()
            .filter_map(|(index, failure)| failure.order.map(|order| (index, order)))
            .min_by_key(|(_, order)| *order);
        if let Some((index, order)) = pump_index {
            if first.is_none_or(|(earlier, _)| order < earlier) {
                first = Some((order, 3 + index));
            }
        }
        let (order, index) = first?;
        let failure = match index {
            0 => match &mut self.writer {
                Role::Joined(value) => value
                    .result
                    .failure
                    .take()
                    .map(|failure| super::OriginalFailure::Io(failure.source)),
                _ => None,
            },
            1 => match &mut self.writer {
                Role::Joined(value) => value
                    .result
                    .shutdown_failure
                    .take()
                    .map(super::OriginalFailure::Io),
                _ => None,
            },
            2 => match &mut self.reader {
                Role::Joined(value) => value.take_error().map(super::OriginalFailure::Io),
                _ => None,
            },
            index => match self.pump_failures.swap_remove(index - 3).into_value() {
                NativePumpFailureValue::Io(error) => Some(super::OriginalFailure::Io(error)),
                NativePumpFailureValue::Panic(payload) => {
                    self.failed = true;
                    // Control has at most nine bounded panic slots. Retain
                    // every original payload; replacing a single Option would drop
                    // an earlier Box and could itself unwind in Root poll.
                    if let Some(slot) = self.pump_panics.iter_mut().find(|slot| slot.is_none()) {
                        *slot = Some(payload);
                    } else {
                        std::mem::forget(payload);
                    }
                    Some(super::OriginalFailure::OpaquePanic)
                }
            },
        }?;
        Some(if self.failures.reported(order) {
            match failure {
                super::OriginalFailure::Io(error) => super::OriginalFailure::ReportedIo(error),
                super::OriginalFailure::OpaquePanic => super::OriginalFailure::ReportedPanic,
                already => already,
            }
        } else { failure })
    }

    #[cfg(test)]
    pub(super) fn pending_reader_error_order(&self) -> Option<usize> {
        let Role::Joined(reader) = &self.reader else {
            return None;
        };
        if !reader.has_error() { return None; }
        reader.error_order()
    }
}


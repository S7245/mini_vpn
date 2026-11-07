//! Original D16 direct carrier under the session Root. Owns the gated reader,
//! writer and close monitor JoinHandles, the one D16 queue/budget identity and
//! guarded joined outcomes. It owns no SocketCtx, F publication or Node meter.
//! Entry `Roles`; true endpoint tests live in client_tun/tests/compatibility_f_flow.rs.

use super::*;
use crate::client_tun::RelayCloseReason;
use crate::client_tun::d16_reader::{
    D16ReaderAccounting, D16ReaderOutcome, run_d16_native_reader_gated,
};
use crate::tcp_downlink_pump::{AsyncLeasedByteFlowQueue, D16GlobalByteBudget};
use crate::upstream::NativeTcpRelayStream;
use tokio::sync::{mpsc, watch};

#[cfg(test)]
mod tests;

struct Prepared {
    request: D16LaunchRequest,
    window: Option<super::super::super::receive_window::ReaderWindow>,
}

#[derive(Clone, Copy, Debug)]
struct ReaderDone {
    reason: RelayCloseReason,
    clean: bool,
}

struct Monitor {
    writer_rx: mpsc::Receiver<RelayWriterSignal>,
    activity_rx: mpsc::Receiver<()>,
    reader_done_rx: oneshot::Receiver<ReaderDone>,
    queue: AsyncLeasedByteFlowQueue,
    timer: RelayCloseTimer,
    writer_done: bool,
    reader_done: bool,
    observed_reader: Option<ReaderDone>,
    reader_done_failed: bool,
    activity_open: bool,
    half_closed_local_progress: Option<u64>,
}

struct MonitorOutcome {
    state: Monitor,
    reason: RelayCloseReason,
    started: bool,
    body_panic: Option<Box<dyn std::any::Any + Send>>,
    drop_panic: Option<Box<dyn std::any::Any + Send>>,
    drained: bool,
}

impl Monitor {
    async fn run(&mut self) -> RelayCloseReason {
        loop {
            tokio::select! {
                activity = self.activity_rx.recv(), if self.activity_open && !self.reader_done => {
                    match activity {
                        Some(()) => self.timer.note_read_progress(),
                        None => self.activity_open = false,
                    }
                }
                signal = self.writer_rx.recv(), if !self.writer_done => {
                    match signal {
                        Some(RelayWriterSignal::WritePending) => self.timer.arm_stalled_write(),
                        Some(RelayWriterSignal::Progress { .. }) => self.timer.note_write_progress(),
                        Some(RelayWriterSignal::WriteCompleted) => self.timer.complete_write(),
                        Some(RelayWriterSignal::WriteHalfClosed { .. }) => {
                            self.writer_done = true;
                            self.half_closed_local_progress = None;
                            self.timer.arm_half_closed();
                            if self.reader_done { return ("remote_to_local", "remote_eof"); }
                        }
                        Some(RelayWriterSignal::Closed { direction, reason }) => {
                            self.writer_done = true;
                            if reason == "local_channel_closed" {
                                let closure = self.queue.snapshot_now().closure;
                                if let Some(cause) = closure.terminal_cause() {
                                    return (cause.direction, cause.reason);
                                }
                                if closure == crate::tcp_downlink_pump::LeasedByteQueueClosure::RemoteEof {
                                    if self.reader_done {
                                        return ("remote_to_local", "remote_eof");
                                    }
                                    self.timer.arm_half_closed();
                                    continue;
                                }
                            }
                            return (direction, reason);
                        }
                        None => return ("local_to_remote", "writer_task_closed"),
                    }
                }
                result = &mut self.reader_done_rx, if !self.reader_done => {
                    self.reader_done = true;
                    match result {
                        Ok(done) if done.clean => {
                            self.observed_reader = Some(done);
                            if self.writer_done { return ("remote_to_local", "remote_eof"); }
                        }
                        Ok(done) => {
                            self.observed_reader = Some(done);
                            return done.reason;
                        }
                        Err(_) => {
                            self.reader_done_failed = true;
                            return ("remote_to_local", "reader_task_failed");
                        }
                    }
                }
                _ = self.timer.wait(), if self.timer.is_armed() => {
                    if self.timer.is_half_closed() {
                        let snapshot = self.queue.snapshot_now();
                        let owned = snapshot.queued_bytes.saturating_add(snapshot.leased_bytes);
                        if snapshot.closure.terminal_cause().is_none() && owned > 0 {
                            let previous = self.half_closed_local_progress.unwrap_or(0);
                            if self.half_closed_local_progress.is_none()
                                || snapshot.local_progress_bytes > previous
                            {
                                self.half_closed_local_progress = Some(snapshot.local_progress_bytes);
                                self.timer.arm_half_closed();
                                continue;
                            }
                        }
                    }
                    return ("timer", self.timer.terminal_reason());
                }
            }
        }
    }
}

async fn run_monitor(
    mut monitor: Monitor,
    mut stop: oneshot::Receiver<()>,
    gate: oneshot::Receiver<()>,
) -> MonitorOutcome {
    let started = gate.await.is_ok();
    let mut work = Box::pin(monitor.run());
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
        state: monitor,
        reason,
        started,
        body_panic,
        drop_panic,
        drained: false,
    }
}

impl MonitorOutcome {
    fn dispose(&mut self, cx: &mut Context<'_>) {
        fn drain<T>(rx: &mut mpsc::Receiver<T>, cx: &mut Context<'_>) -> bool {
            rx.close();
            for _ in 0..32 {
                match rx.poll_recv(cx) {
                    Poll::Ready(Some(_)) => continue,
                    Poll::Ready(None) => return true,
                    Poll::Pending => return false,
                }
            }
            false
        }
        let writer_drained = drain(&mut self.state.writer_rx, cx);
        let activity_drained = drain(&mut self.state.activity_rx, cx);
        if self.state.observed_reader.is_none() && !self.state.reader_done_failed {
            match self.state.reader_done_rx.try_recv() {
                Ok(done) => self.state.observed_reader = Some(done),
                Err(oneshot::error::TryRecvError::Closed) => self.state.reader_done_failed = true,
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
        }
        self.drained = writer_drained && activity_drained && self.state.observed_reader.is_some();
    }
}

pub(super) struct Roles {
    prepared: Option<Prepared>,
    queue: AsyncLeasedByteFlowQueue,
    budget: D16GlobalByteBudget,
    backing_meter: bytes::BackingMeter,
    failures: FailureClock,
    writer: Role<WriterOutcome>,
    reader: Role<D16ReaderOutcome>,
    monitor: Role<MonitorOutcome>,
    starts: [Option<oneshot::Sender<()>>; 3],
    stops: [Option<oneshot::Sender<()>>; 3],
    writer_receipt: Option<WriterDiscardReceipt>,
    committed: bool,
    started: bool,
    stopping: bool,
    interrupted: bool,
    failed: bool,
}

impl Roles {
    pub(super) fn new(
        request: D16LaunchRequest,
        failures: FailureClock,
    ) -> Result<Self, D16LaunchRequest> {
        let backing_meter = request.native.reader.receive_backing_meter();
        if request.native.has_prepared_pump() || !request.queue.d16_uses_budget(&request.budget) {
            return Err(request);
        }
        let Some(backing_meter) = backing_meter else {
            return Err(request);
        };
        Ok(Self {
            queue: request.queue.clone(),
            budget: request.budget.clone(),
            backing_meter,
            prepared: Some(Prepared {
                request,
                window: None,
            }),
            failures,
            writer: Role::Empty,
            reader: Role::Empty,
            monitor: Role::Empty,
            starts: [None, None, None],
            stops: [None, None, None],
            writer_receipt: None,
            committed: false,
            started: false,
            stopping: false,
            interrupted: false,
            failed: false,
        })
    }

    pub(super) fn queue(&self) -> &AsyncLeasedByteFlowQueue {
        &self.queue
    }
    pub(super) fn budget(&self) -> &D16GlobalByteBudget {
        &self.budget
    }
    pub(super) fn backing_meter(&self) -> &bytes::BackingMeter {
        &self.backing_meter
    }

    pub(super) fn prepare(&mut self, window: super::super::super::receive_window::ReaderWindow) {
        self.interrupted = true;
        if let Some(prepared) = &mut self.prepared {
            if prepared.window.is_none() {
                prepared.window = Some(window);
                self.interrupted = false;
            }
        }
    }

    pub(super) fn commit(&mut self) -> bool {
        self.interrupted = true;
        let Some(mut prepared) = self.prepared.take() else {
            return false;
        };
        let Some(window) = prepared.window.take() else {
            self.prepared = Some(prepared);
            return false;
        };
        let D16LaunchRequest {
            handle,
            epoch,
            native,
            queue,
            budget: _,
            rx,
            back_tx,
            read_credit_rx,
        } = prepared.request;
        let (reader, writer, pump) = native.into_parts();
        if let Some(pump) = pump {
            prepared.request = D16LaunchRequest {
                handle,
                epoch,
                native: NativeTcpRelayStream::new(reader, writer).with_prepared_pump(pump),
                queue,
                budget: self.budget.clone(),
                rx,
                back_tx,
                read_credit_rx,
            };
            prepared.window = Some(window);
            self.prepared = Some(prepared);
            return false;
        }
        let (writer_tx, writer_rx) = mpsc::channel(RELAY_CHANNEL_CAPACITY);
        let (activity_tx, activity_rx) = mpsc::channel(1);
        let (reader_done_tx, reader_done_rx) = oneshot::channel();
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
            Some(self.failures.clone()),
        )));
        let (start, gate) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        self.starts[1] = Some(start);
        self.stops[1] = Some(stop);
        let accounting =
            D16ReaderAccounting::new(self.failures.clone(), window, self.backing_meter.clone());
        self.reader = Role::Running(tokio::spawn(async move {
            let outcome = run_d16_native_reader_gated(
                handle,
                epoch,
                reader,
                queue,
                read_credit_rx,
                back_tx,
                Some(activity_tx),
                stop_rx,
                accounting,
                gate,
            )
            .await;
            let clean = outcome.started
                && outcome.reason == ("remote_to_local", "remote_eof")
                && outcome.error.is_none()
                && outcome.queue_error.is_none()
                && outcome.retained.is_none()
                && outcome.body_panic.is_none()
                && outcome.future_drop_panic.is_none();
            let _ = reader_done_tx.send(ReaderDone {
                reason: outcome.reason,
                clean,
            });
            outcome
        }));
        let (start, gate) = oneshot::channel();
        let (stop, stop_rx) = oneshot::channel();
        self.starts[2] = Some(start);
        self.stops[2] = Some(stop);
        self.monitor = Role::Running(tokio::spawn(run_monitor(
            Monitor {
                writer_rx,
                activity_rx,
                reader_done_rx,
                queue: self.queue.clone(),
                timer: RelayCloseTimer::new(),
                writer_done: false,
                reader_done: false,
                observed_reader: None,
                reader_done_failed: false,
                activity_open: true,
                half_closed_local_progress: None,
            },
            stop_rx,
            gate,
        )));
        self.committed = true;
        self.interrupted = false;
        true
    }

    pub(super) fn start(&mut self) {
        if self.started || !self.committed {
            return;
        }
        self.interrupted = true;
        let mut failed = false;
        for gate in &mut self.starts {
            failed |= gate.take().is_none_or(|gate| gate.send(()).is_err());
        }
        self.started = true;
        self.failed |= failed;
        if failed {
            self.stop();
        } else {
            self.interrupted = false;
        }
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

    pub(super) fn poll_fatal_joins(&mut self, cx: &mut Context<'_>) {
        self.stop();
        self.poll_handles(cx);
        if let Role::Joined(monitor) = &mut self.monitor {
            monitor.dispose(cx);
        }
    }

    pub(super) fn poll(&mut self, cx: &mut Context<'_>, producer_stopped: bool) {
        self.poll_handles(cx);
        if !self.stopping {
            return;
        }
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
    }

    pub(super) fn closed(&self) -> Option<RelayCloseReason> {
        match &self.monitor {
            Role::Joined(outcome) => Some(outcome.reason),
            Role::Failed(_) => Some(("root", "d16_monitor_join_failed")),
            _ if self.quarantined() => Some(("root", "d16_roles_quarantined")),
            _ => None,
        }
    }

    pub(super) fn read_total(&self) -> Option<usize> {
        match &self.reader {
            Role::Joined(outcome) => outcome
                .disposed_read
                .or_else(|| outcome.window.as_ref().map(|w| w.read())),
            _ => None,
        }
    }

    pub(super) fn reader_ledger(&self) -> Option<(usize, usize, usize)> {
        match &self.reader {
            Role::Joined(outcome) if !outcome.counter_failed => Some((
                outcome.queued_bytes,
                outcome.ready_sent,
                outcome.closed_sent,
            )),
            _ => None,
        }
    }

    pub(super) fn dispose_reader_after_physical_proof(&mut self) -> bool {
        if !self.stopping
            || self.join_count() != 3
            || !self.queue.d16_terminal_quota_zero(&self.budget)
        {
            return false;
        }
        match &mut self.reader {
            Role::Joined(outcome) => outcome.dispose_clean(),
            _ => false,
        }
    }

    pub(super) fn join_count(&self) -> usize {
        usize::from(matches!(self.writer, Role::Joined(_)))
            + usize::from(matches!(self.reader, Role::Joined(_)))
            + usize::from(matches!(self.monitor, Role::Joined(_)))
    }

    pub(super) fn all_original_roles_joined(&self) -> bool {
        self.committed
            && self.started
            && matches!(self.writer, Role::Joined(_))
            && matches!(self.reader, Role::Joined(_))
            && matches!(self.monitor, Role::Joined(_))
    }

    pub(super) fn running_count(&self) -> usize {
        usize::from(matches!(self.writer, Role::Running(_)))
            + usize::from(matches!(self.reader, Role::Running(_)))
            + usize::from(matches!(self.monitor, Role::Running(_)))
    }

    pub(super) fn quarantined(&self) -> bool {
        self.interrupted
            || self.failed
            || (self.committed
                && (matches!(self.writer, Role::Empty | Role::Failed(_))
                    || matches!(self.reader, Role::Empty | Role::Failed(_))
                    || matches!(self.monitor, Role::Empty | Role::Failed(_))))
            || matches!(&self.reader, Role::Joined(o) if !o.started || o.counter_failed || o.error_order.is_some() || o.body_panic.is_some() || o.future_drop_panic.is_some() || o.retained.is_some() || o.queue_error.is_some())
            || matches!(&self.writer, Role::Joined(o) if !o.started || o.panics.iter().any(Option::is_some) || o.remainder.quarantined())
            || matches!(&self.monitor, Role::Joined(o) if !o.started
                || o.state.reader_done_failed
                || o.body_panic.is_some() || o.drop_panic.is_some()
                || (o.reason != ("remote_to_local", "remote_eof") && o.reason != ("root", "stop")))
    }

    pub(super) fn settled(&self) -> bool {
        !self.quarantined()
            && self.started
            && self.stopping
            && self.join_count() == 3
            && self.writer_receipt.is_some()
            && self.queue.d16_terminal_quota_zero(&self.budget)
            && matches!(&self.monitor, Role::Joined(o) if o.drained)
            && matches!(&self.reader, Role::Joined(o) if o.disposed_read.is_some() && o.remainder.is_none() && o.window.is_none())
            && matches!((&self.reader, &self.monitor), (Role::Joined(reader), Role::Joined(monitor))
                if matches!(monitor.state.observed_reader,
                    Some(ReaderDone { reason, clean })
                    if reason == reader.reason
                        && clean == (reader.started
                            && reader.reason == ("remote_to_local", "remote_eof")
                            && reader.error.is_none()
                            && reader.queue_error.is_none()
                            && reader.retained.is_none()
                            && reader.body_panic.is_none()
                            && reader.future_drop_panic.is_none())))
    }

    pub(super) fn take_error(&mut self) -> Option<std::io::Error> {
        let mut order = [None; 3];
        if let Role::Joined(o) = &self.writer {
            order[0] = o.error_order[0].filter(|_| o.result.failure.is_some());
            order[1] = o.error_order[1].filter(|_| o.result.shutdown_failure.is_some());
        }
        if let Role::Joined(o) = &self.reader {
            order[2] = o.error_order.filter(|_| o.error.is_some());
        }
        match next_error(order)? {
            0 => match &mut self.writer {
                Role::Joined(o) => o.result.failure.take().map(|f| f.source),
                _ => None,
            },
            1 => match &mut self.writer {
                Role::Joined(o) => o.result.shutdown_failure.take(),
                _ => None,
            },
            _ => match &mut self.reader {
                Role::Joined(o) => o.error.take(),
                _ => None,
            },
        }
    }
}

fn next_error(order: [Option<usize>; 3]) -> Option<usize> {
    order
        .into_iter()
        .enumerate()
        .filter_map(|(slot, order)| order.map(|order| (order, slot)))
        .min()
        .map(|(_, slot)| slot)
}

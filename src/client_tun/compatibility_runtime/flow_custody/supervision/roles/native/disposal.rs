//! Original Native joined disposition. One bounded actor turn leaves its real
//! JOIN witness in the original Roles; the Flow token owns the actual outcome.
//! No epoch, queue, admission or refund authority is introduced. Monitor may
//! close before Source JOIN. Tests enter through real Service and Root custody.
use super::*;
use std::mem::{ManuallyDrop, replace};
use crate::shared::actor_callbacks::{Guard, Field};

/// The original SocketCtx producer endpoints are held separately through the
/// same Flow turn. A partial Drop never releases its exclusive qualification.
pub(super) struct Producers {
    uplink: ManuallyDrop<Option<crate::client_tun::uplink::Sender>>,
    credit: ManuallyDrop<Option<watch::Sender<RelayReadCredit>>>,
    guard: Option<Guard>,
    done: bool,
}

pub(super) enum Pump { Empty, Live(RunningNativePump), Disposing, Stopping(bool) }
impl From<Option<RunningNativePump>> for Pump {
    fn from(value: Option<RunningNativePump>) -> Self { value.map_or(Self::Empty, Self::Live) }
}
impl Pump {
    pub(super) fn as_ref(&self) -> Option<&RunningNativePump> { if let Self::Live(v) = self { Some(v) } else { None } }
    pub(super) fn as_mut(&mut self) -> Option<&mut RunningNativePump> { if let Self::Live(v) = self { Some(v) } else { None } }
    pub(super) fn is_none(&self) -> bool { matches!(self, Self::Empty) }
    pub(super) fn is_some(&self) -> bool { !self.is_none() }
    pub(super) fn joined(&self) -> bool { matches!(self, Self::Disposing | Self::Stopping(true)) || self.as_ref().is_some_and(RunningNativePump::joined) }
    pub(super) fn running(&self) -> bool { matches!(self, Self::Stopping(false)) || self.as_ref().is_some_and(|p| !p.joined()) }
}
enum Action {
    Stock(stock::Inventory, Arc<super::super::super::continuous::Shared>),
    Producers(Producers),
    Start { gates: [Option<oneshot::Sender<()>>; 4], pump_gate: Option<oneshot::Sender<()>>, expected_pump: bool },
    Stop { stops: [Option<oneshot::Sender<()>>; 4], gates: [Option<oneshot::Sender<()>>; 4], pump_gate: Option<oneshot::Sender<()>>, pump: Option<RunningNativePump>, joined: bool },
    Monitor(MonitorOutcome),
    Writer(WriterOutcome, Option<WriterDiscardReceipt>, Option<super::super::super::super::relay_writer::Cold>, Option<crate::client_tun::uplink::Observer>),
    Source { outcome: Outcome, stock: Option<reader::Cold>, receipt: Option<DiscardReceipt>, expected_pump: bool },
    Dispatcher(super::super::super::continuous::DispatchOutcome, PublishedReceipt, bool),
    Pump(RunningNativePump, Option<NativePumpReceipt>),
}
/// No automatic destructor ever owns an opaque actor: a lost/invalid original
/// turn retains these originals, and the Flow's exclusive qualification stays.
pub(in super::super) struct Work {
    action: ManuallyDrop<Action>,
    meter: Option<bytes::BackingMeter>,
    failed: bool,
    interrupted: bool,
    progressed: bool,
}
impl Work {
    pub(in super::super) fn run(&mut self, cx: &mut Context<'_>) {
        self.interrupted = true;
        match &mut *self.action {
            Action::Stock(stock, shared) => { stock.poll_retire(shared, cx); self.failed |= stock.failed(); self.progressed = stock.done(); },
            Action::Producers(fields) => {
                let Some(guard) = &fields.guard else { self.failed = true; return; };
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(fields.uplink.take()))) {
                    guard.capture(Field::Source7, payload); self.failed = true;
                }
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(fields.credit.take()))) {
                    guard.capture(Field::Notification, payload); self.failed = true;
                }
                fields.done = !self.failed;
                if fields.done { drop(fields.guard.take()); }
                self.progressed = fields.done;
            }
            Action::Start { gates, pump_gate, expected_pump } => {
                for gate in gates { self.failed |= gate.take().is_none_or(|gate| gate.send(()).is_err()); }
                if let Some(gate) = pump_gate.take() { self.failed |= gate.send(()).is_err(); }
                else { self.failed |= *expected_pump; }
                self.progressed = true;
            }
            Action::Stop { stops, gates, pump_gate, pump, .. } => {
                for stop in stops { if let Some(stop) = stop.take() { let _ = stop.send(()); } }
                for gate in gates { drop(gate.take()); }
                drop(pump_gate.take());
                if let Some(pump) = pump { pump.stop(); }
                self.progressed = true;
            }
            Action::Monitor(outcome) => {
                let before = (outcome.closed, outcome.drained, outcome.discarded);
                outcome.dispose(cx);
                self.progressed = before != (outcome.closed, outcome.drained, outcome.discarded);
            },
            Action::Writer(outcome, receipt, stock, observer) => {
                let before = (receipt.is_some(), stock.is_some(), observer.is_some());
                if receipt.is_none() {
                    if let Poll::Ready(value) = outcome.remainder.poll_discard(cx, 32) { *receipt = Some(value); }
                }
                if receipt.is_some() && outcome.retire_fields()
                    && stock.as_mut().is_some_and(|stock| stock.retire()) { drop(stock.take()); }
                if receipt.is_some() && stock.is_none() {
                    if let Some(value) = observer.take() { *observer = value.finish().err(); }
                }
                self.progressed = before != (receipt.is_some(), stock.is_some(), observer.is_some());
            }
            Action::Source { outcome, stock, receipt, expected_pump } => {
                let before = (receipt.is_some(), stock.is_some());
                *receipt = outcome.discard_local(self.meter.as_ref());
                if !*expected_pump && receipt.is_some() && outcome.source_read_bytes() != Some(outcome.received_bytes()) { self.failed = true; }
                if receipt.is_some() && stock.as_mut().is_some_and(reader::Cold::retire) { drop(stock.take()); }
                self.progressed = before != (receipt.is_some(), stock.is_some());
            }
            Action::Dispatcher(outcome, queued, terminal) => { outcome.dispose(cx); outcome.verify(*queued, *terminal); self.progressed = outcome.audit(*queued, *terminal); }
            Action::Pump(pump, receipt) => match pump.poll_settle(cx) {
                Poll::Ready(Ok(value)) => { *receipt = Some(value); self.progressed = true; },
                Poll::Ready(Err(())) => self.failed = true,
                Poll::Pending => {}
            },
        }
        self.interrupted = false;
    }
    pub(in super::super) fn progressed(&self) -> bool { self.progressed && !self.failed && !self.interrupted }
    pub(in super::super) fn failed(&self) -> bool { self.failed }
    pub(in super::super) fn is_stock_retirement(&self) -> bool { matches!(&*self.action, Action::Stock(_, _)) }
    pub(in super::super) fn is_producers(&self) -> bool { matches!(&*self.action, Action::Producers(_)) }
    pub(in super::super) fn producers_done(&self) -> bool {
        matches!(&*self.action, Action::Producers(fields) if fields.done && !self.interrupted)
    }
    pub(in super::super) fn matches(&self, roles: &Roles) -> bool {
        match &*self.action {
            Action::Stock(_, _) => roles.stock_retiring && roles.stock.is_none(),
            Action::Producers(_) => roles.producers == Some(false) && roles.retained_producers.is_none(),
            Action::Start { .. } => roles.interrupted && !roles.started && roles.starts.iter().all(Option::is_none) && roles.pump_start.is_none(),
            Action::Stop { pump, joined, .. } => roles.stopping && !roles.stop_notified && roles.stops.iter().all(Option::is_none)
                && match pump { Some(_) => matches!(roles.pump, Pump::Stopping(actual) if actual == *joined), None => roles.pump.is_none() },
            Action::Monitor(_) => matches!(roles.monitor, Role::Disposing),
            Action::Writer(_, _, _, _) => matches!(roles.writer, Role::Disposing),
            Action::Source { .. } => matches!(roles.reader, Role::Disposing) && roles.source.is_none(),
            Action::Dispatcher(_, _, _) => matches!(roles.dispatcher, Role::Disposing),
            Action::Pump(_, _) => matches!(roles.pump, Pump::Disposing),
        }
    }
    /// Caller has already checked full Flow identity, original clock and the
    /// exclusive disposition witness. Every remaining move is infallible.
    pub(in super::super) fn restore(mut self, roles: &mut Roles) {
        roles.failed |= self.failed;
        match unsafe { ManuallyDrop::take(&mut self.action) } {
            Action::Stock(stock, _) => { roles.stock = Some(stock); roles.stock_retiring = self.interrupted; },
            Action::Producers(fields) => {
                roles.producers = Some(fields.done && !self.interrupted);
                if roles.producers != Some(true) { roles.retained_producers = Some(fields); roles.failed = true; }
            }
            Action::Start { gates, pump_gate, .. } => {
                roles.starts = gates; roles.pump_start = pump_gate;
                roles.started = !self.interrupted; roles.interrupted = self.interrupted || self.failed;
                roles.stopping |= self.failed;
            }
            Action::Stop { stops, gates, pump_gate, pump, .. } => {
                roles.stops = stops;
                if !roles.started { roles.starts = gates; roles.pump_start = pump_gate; }
                roles.pump = pump.into();
                roles.stop_notified = !self.interrupted;
            }
            Action::Monitor(value) => roles.monitor = Role::Joined(value),
            Action::Writer(value, receipt, stock, observer) => { roles.writer = Role::Joined(value); roles.writer_receipt = receipt; roles.writer_callbacks = stock; roles.uplink_observer = observer; }
            Action::Source { outcome, stock, receipt, .. } => { roles.reader = Role::Joined(outcome); roles.source = stock; roles.reader_receipt = receipt; }
            Action::Dispatcher(value, _, _) => roles.dispatcher = Role::Joined(value),
            Action::Pump(value, receipt) => { roles.pump = Pump::Live(value); roles.pump_receipt = receipt; }
        }
    }
}
fn take<T>(role: &mut Role<T>) -> T {
    // This helper is only called immediately after the matching Joined branch.
    match replace(role, Role::Disposing) { Role::Joined(value) => value, _ => unreachable!() }
}
impl Roles {
    pub(in super::super) fn has_hard_source(&self) -> bool { self.source.is_some() || self.producers.is_some() }
    pub(in super::super) fn take_producers(
        &mut self, uplink: &mut Option<crate::client_tun::uplink::Sender>, credit: &mut Option<watch::Sender<RelayReadCredit>>,
    ) -> Option<Work> {
        if self.producers.is_some() || self.retained_producers.is_some() { return None; }
        let guard = self.source.as_ref()?.producer_guard()?;
        self.producers = Some(false);
        self.stop();
        Some(self.work(Action::Producers(Producers {
            uplink: ManuallyDrop::new(uplink.take()), credit: ManuallyDrop::new(credit.take()), guard: Some(guard), done: false,
        })))
    }
    fn work(&self, action: Action) -> Work {
        Work { action: ManuallyDrop::new(action), meter: self.backing_meter.clone(), failed: false, interrupted: false, progressed: false }
    }
    pub(in super::super) fn take_start_disposal(&mut self) -> Option<Work> {
        if self.started || !self.committed || self.interrupted { return None; }
        self.interrupted = true;
        let action = Action::Start { gates: std::mem::take(&mut self.starts), pump_gate: self.pump_start.take(), expected_pump: self.expected_pump };
        Some(self.work(action))
    }
    pub(in super::super) fn take_disposal(&mut self, producer_stopped: bool) -> Option<Work> {
        if !self.stopping { return None; }
        if !self.stop_notified {
            let joined = self.pump.joined();
            let pump = match replace(&mut self.pump, Pump::Stopping(joined)) {
                Pump::Live(pump) => Some(pump),
                Pump::Empty => { self.pump = Pump::Empty; None },
                original => { self.pump = original; return None; }
            };
            let action = Action::Stop {
                stops: std::mem::take(&mut self.stops),
                gates: if self.started { Default::default() } else { std::mem::take(&mut self.starts) },
                pump_gate: if self.started { None } else { self.pump_start.take() }, pump, joined,
            };
            return Some(self.work(action));
        }
        if self.data_disposed() && !self.stock_retiring && self.stock.as_ref().is_some_and(|s| !s.done() && !s.failed()) {
            self.stock_retiring = true;
            let stock = self.stock.take()?;
            return Some(self.work(Action::Stock(stock, self.shared.clone())));
        }
        for _ in 0..5 {
            let site = self.disposal_cursor;
            self.disposal_cursor = (site + 1) % 5;
            let action = match site {
                0 if matches!(&self.monitor, Role::Joined(o) if o.drained != [true; 2] && !o.in_flight) => Action::Monitor(take(&mut self.monitor)),
                1 if producer_stopped && (self.writer_receipt.is_none() || self.writer_callbacks.is_some() || self.uplink_observer.is_some()) && matches!(&self.writer, Role::Joined(o) if !o.remainder.quarantined()) => Action::Writer(take(&mut self.writer), self.writer_receipt, self.writer_callbacks.take(), self.uplink_observer.take()),
                2 if producer_stopped && matches!(&self.reader, Role::Joined(o) if !o.quarantined() && (self.reader_receipt.is_none() || (self.source.is_some() && !o.has_error()))) => Action::Source {
                    outcome: take(&mut self.reader), stock: self.source.take(), receipt: self.reader_receipt, expected_pump: self.expected_pump,
                },
                3 if producer_stopped && matches!(self.writer, Role::Joined(_)) && matches!(self.reader, Role::Joined(_)) && matches!(self.monitor, Role::Joined(_)) && matches!(self.dispatcher, Role::Joined(_)) => {
                    let Role::Joined(reader) = &self.reader else { continue; };
                    let (queued, terminal) = (reader.queued(), reader.closed_sent());
                    let Role::Joined(dispatcher) = &self.dispatcher else { continue; };
                    if dispatcher.quarantined() || dispatcher.audit(queued, terminal) { continue; }
                    Action::Dispatcher(take(&mut self.dispatcher), queued, terminal)
                }
                4 if self.reader_receipt.is_some() && self.pump_receipt.is_none() && self.pump.as_ref().is_some_and(|p| p.joined() && !p.quarantined()) => {
                    let Pump::Live(pump) = replace(&mut self.pump, Pump::Disposing) else { unreachable!() };
                    Action::Pump(pump, self.pump_receipt)
                }
                _ => continue,
            };
            return Some(self.work(action));
        }
        None
    }
}

#[cfg(test)]
#[path = "../tests/native_disposal.rs"]
mod tests;

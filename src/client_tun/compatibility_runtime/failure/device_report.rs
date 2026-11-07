//! Original Session Device failure transaction. One ledger accepts the actual
//! role, first/delta, original IO and same-Core disposal guard. The original
//! final Device turn disposes IO only after Flow/Device retirement; cancellation
//! retains the real source and cold guard in Q. No failure sorter or DATA queue.
//! Tests: business/tests/ingress.rs and egress/tests/native_link.rs.
use super::*;
use crate::device::egress::{CallbackReport, CallbackSourceGuard};

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn accept_device_report(&self, mut report: CallbackReport) -> bool {
        if let Some(source) = &report.source {
            let valid = report.source_guard.as_ref().is_some_and(|guard|
                guard.matches(source, report.source_phase.clone(), self.app_meter.as_ref()));
            if !valid {
                if let Some(guard) = &report.source_guard { guard.quarantine(); }
                self.quarantine_quiet();
                std::mem::forget(report);
                return false;
            }
        }
        let accepted = report.failure.is_some();
        let unknown = report.unknown || report.failure.as_ref().is_some_and(|failure|
            failure.facts().class == crate::shared::errors::FailureClass::Panic);
        {
            let mut ledger = self.ledger();
            if let Some(first) = report.failure.take() {
                if ledger.first_failure.is_none() { ledger.first_failure = Some(first); }
                else { ledger.additional_failures = ledger.additional_failures.saturating_add(1); }
            }
            ledger.additional_failures = ledger.additional_failures.saturating_add(report.additional);
            if let Some(source) = &report.source {
                let index = usize::from(report.source_phase == Some(FailurePhase::DeviceWrite));
                let empty = if index == 0 { ledger.first_ingress_io.is_none() } else { ledger.first_egress_io.is_none() };
                if empty {
                    if index == 0 { ledger.first_ingress_io = Some(source.clone()); }
                    else { ledger.first_egress_io = Some(source.clone()); }
                    ledger.device_source_guards[index] = report.source_guard.take();
                }
            }
        }
        // All unused sources/guards and actual quiet effects are outside every owner lock.
        drop(report);
        if unknown { self.quarantine_quiet(); }
        accepted
    }
    pub(in crate::client_tun::compatibility_runtime) fn record_device_read(&self, error: std::io::Error) {
        if let Some(report) = crate::device::egress::callback_report(&error, Self::origin(FailurePhase::DeviceRead)) {
            self.accept_device_report(report);
            drop(error);
        } else { self.record(RetainedFailure::from_io(Self::origin(FailurePhase::DeviceRead), error)); }
    }
    fn source_disposal(&self) -> SourceTurn<'_> {
        let mut ledger = self.ledger();
        // An occupied turn owns the guards outside this ledger. Its now-empty
        // slots cannot prove absence to a competing finalizer.
        if ledger.device_source_disposal { return SourceTurn::Busy; }
        if ledger.device_source_guards.iter().all(Option::is_none) {
            return SourceTurn::Empty;
        }
        ledger.device_source_disposal = true;
        let guards = std::mem::take(&mut ledger.device_source_guards);
        let sources = [
            guards[0].as_ref().and_then(|_| ledger.first_ingress_io.take()),
            guards[1].as_ref().and_then(|_| ledger.first_egress_io.take()),
        ];
        SourceTurn::Work(SourceDisposal { owner: self, sources, guards, done: false })
    }
    pub(in crate::client_tun::compatibility_runtime) async fn retire_device_sources(&self) {
        let mut work = None;
        loop {
            if work.is_none() {
                match self.source_disposal() {
                    SourceTurn::Empty => return,
                    SourceTurn::Busy => {},
                    SourceTurn::Work(actual) => work = Some(actual),
                }
            }
            let deadline = QuietFailureSink::deadline_quiet(self);
            if deadline.is_some_and(|limit| tokio::time::Instant::now() >= limit) {
                self.quarantine_quiet();
                // Keep the actual work and exclusive turn until the original
                // owner cancels. Its Drop retains the original fields in Q.
                std::future::pending::<()>().await;
            }
            if work.as_mut().is_some_and(SourceDisposal::try_settle) { return; }
            let deadline = deadline.unwrap_or_else(|| self.request_stop());
            // Same bounded physical-control cadence as original Flow retirement.
            // An arbitrary Arc alias has no local last-reference notification.
            let next = tokio::time::Instant::now() + std::time::Duration::from_millis(5);
            tokio::time::sleep_until(next.min(deadline)).await;
        }
    }
}
enum SourceTurn<'a> {
    Empty,
    Busy,
    Work(SourceDisposal<'a>),
}
struct SourceDisposal<'a> {
    owner: &'a SessionControl,
    sources: [Option<Arc<std::io::Error>>; 2],
    guards: [Option<CallbackSourceGuard>; 2],
    done: bool,
}
impl SourceDisposal<'_> {
    fn try_settle(&mut self) -> bool {
        // Each actual Arc is non-last: its validated Core still owns first IO.
        for source in &mut self.sources { drop(source.take()); }
        for index in 0..2 {
            if let Some(guard) = &self.guards[index] {
                if !guard.try_dispose() { return false; }
                self.owner.accept_device_report(guard.claim(SessionControl::origin(FailurePhase::DeviceShutdown)));
            }
            drop(self.guards[index].take());
        }
        self.owner.ledger().device_source_disposal = false;
        self.done = true;
        true
    }
}
impl Drop for SourceDisposal<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.owner.quarantine_quiet();
            for source in &mut self.sources { std::mem::forget(source.take()); }
            for guard in &mut self.guards { std::mem::forget(guard.take()); }
            // The unique turn remains occupied after unknown cancellation.
        }
    }
}

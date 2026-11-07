//! Transfer the original pump's first failure during Service startup, without
//! consuming packets, joining actors or declaring settlement. The original
//! mutex owns first+delta; a notification does not claim either. The Service
//! accepts them before disposing its observer; later delta stays for shutdown.
//! Native returned errors retain this same PumpControl until its first/delta
//! claim. Tests: cold_factory, native_cold_start, failure/tests/device_source.
use super::*;

/// Notification that the original ingress owner has unclaimed failure facts.
/// Dropping this value leaves those facts in that owner. This is neither a
/// shutdown receipt nor evidence that an actor was joined.
pub struct DeviceIngressFailure {
    control: Arc<PumpControl>,
}
impl DeviceIngressFailure {
    pub(crate) fn claim(&self) -> Option<callback::Report> {
        let mut slot = self.control.lock_failures();
        if let PumpFailureSlot::Callback(kind) = slot.first {
            slot.first = PumpFailureSlot::CallbackTransferred(kind);
            drop(slot);
            let mut report = self.control.native_ingress.as_ref()?.claim();
            report.additional = report.additional.saturating_add(self.control.additional_failures.swap(0, Ordering::Relaxed));
            return Some(report);
        }
        let kind = match &slot.first {
            PumpFailureSlot::Retained(failure) => {
                failure.facts().io_kind.unwrap_or(ErrorKind::Other)
            }
            _ => return None,
        };
        let PumpFailureSlot::Retained(failure) =
            std::mem::replace(&mut slot.first, PumpFailureSlot::Transferred(kind))
        else {
            return None;
        };
        let additional = self.control.additional_failures.swap(0, Ordering::Relaxed);
        let unknown = failure.facts().class == crate::shared::errors::FailureClass::Panic;
        Some(callback::Report { failure: Some(failure), source: None, additional,
            source_phase: None, unknown, source_guard: None })
    }
}
impl TunIngressPump {
    pub(crate) fn ingress_failure_observer(&self) -> DeviceIngressFailure {
        DeviceIngressFailure {
            control: self.control.clone(),
        }
    }
}
impl DeviceIngressFailure {
    pub(crate) async fn wait(&self) -> Option<DeviceIngressFailure> {
        loop {
            let notified = self.control.failure_wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if matches!(
                self.control.lock_failures().first,
                PumpFailureSlot::Retained(_) | PumpFailureSlot::Callback(_)
            ) {
                return Some(DeviceIngressFailure {
                    control: self.control.clone(),
                });
            }
            notified.await;
        }
    }
}
impl DeviceShutdown {
    pub(crate) fn mark_previous_failure_transfer(&mut self, previous: bool) {
        self.failure_transferred |= previous;
    }
    pub(crate) fn failure_was_transferred(&self) -> bool {
        self.failure_transferred
    }
    pub(crate) fn transfers_failure(&self) -> bool {
        self.failure_transferred || self.failure.is_some()
    }
}

/// Same original Pump ledger, including native poll/Drop panic sites. No new
/// failure sorter and no fabricated IO facts after an earlier observer claim.
struct PumpCarrier(Arc<PumpControl>);
impl std::fmt::Debug for PumpCarrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("Native ingress owner failure") }
}
impl std::fmt::Display for PumpCarrier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { std::fmt::Debug::fmt(self, f) }
}
impl std::error::Error for PumpCarrier {}
pub(super) fn error(control: &Arc<PumpControl>) -> std::io::Error {
    std::io::Error::new(control.failure_kind(), PumpCarrier(control.clone()))
}
pub(super) fn report(error: &std::io::Error) -> Option<callback::Report> {
    let control = &error.get_ref()?.downcast_ref::<PumpCarrier>()?.0;
    Some(DeviceIngressFailure { control: control.clone() }.claim().unwrap_or_else(|| callback::Report {
        failure: None, source: None, source_phase: None, source_guard: None,
        additional: control.additional_failures.swap(0, Ordering::Relaxed),
        unknown: control.native_quarantined(),
    }))
}

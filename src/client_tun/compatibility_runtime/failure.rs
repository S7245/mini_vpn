//! Original Session failure publication and forced Native root-loss custody.
//! Normal JOIN/physical receipts remain in flow_custody. An unsettled original
//! registry is retained under the same charged Q, never destroyed as a clean
//! shutdown. No new actor, queue, notification or DATA authority is introduced.
use super::*;
use crate::shared::errors::QuietFailureSink;
mod device_report;

impl QuietFailureSink for SessionControl {
    fn quarantine_quiet(&self) {
        SessionControl::quarantine_quiet(self);
    }
    fn deadline_quiet(&self) -> Option<tokio::time::Instant> {
        let mut poisoned = false;
        let parent = self
            .parent_deadline
            .get()
            .and_then(|deadline| match deadline.lock() {
                Ok(guard) => *guard,
                Err(error) => {
                    poisoned = true;
                    *error.into_inner()
                }
            });
        let local = match self.ledger.lock() {
            Ok(ledger) => ledger.shutdown_deadline,
            Err(error) => {
                poisoned = true;
                error.into_inner().shutdown_deadline
            }
        };
        if poisoned {
            self.quarantine_quiet();
        }
        parent.into_iter().chain(local).min()
    }
    fn record_quiet(&self, failure: RetainedFailure) {
        let panic = failure.facts().class == crate::shared::errors::FailureClass::Panic;
        self.record(failure);
        if panic {
            self.quarantine_quiet();
        }
    }
}
impl Drop for SessionControl {
    fn drop(&mut self) {
        let Some(meter) = self
            .app_meter
            .as_ref()
            .filter(|meter| meter.budget_limit().is_some())
        else {
            return;
        };
        let source_pending = self.ledger.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner)
            .device_source_guards.iter().any(Option::is_some);
        if source_pending {
            self.quarantine_quiet();
            let ledger = self.ledger.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::forget(ledger.first_ingress_io.take());
            std::mem::forget(ledger.first_egress_io.take());
            for guard in &mut ledger.device_source_guards { std::mem::forget(guard.take()); }
        }
        let uncertain = match self.opens.get_mut() {
            Ok(opens) => opens.live != 0 || opens.slots.iter().any(Option::is_some),
            Err(_) => true,
        };
        if !uncertain {
            return;
        }
        // DATA admission observes the same budget Q before any original slot
        // is detached. No destructor, notification or synthetic JOIN runs here.
        meter.quarantine();
        self.quarantine_session_owned();
        let ledger = self
            .ledger
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::forget(ledger.first_egress_io.take());
        let opens = self
            .opens
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Preserve the actual Vec, capacity and every owned value. Its original
        // F/W limit still bounds custody; no receipt or refund is emitted.
        opens.retain_forced_native_custody();
    }
}

#[cfg(test)]
mod tests;

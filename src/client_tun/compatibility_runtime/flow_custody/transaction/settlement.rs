//! Session-owned open settlement and complete bounded worker drain.
//! Owns the original stop audit, worker wake/scan and retained outcome disposal;
//! Flow Root keeps W/F and receipt authority in its parent Module.
//! Entry: `SessionControl::settle_opens`; tests: transaction/tests and Service.
use super::*;

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn opens(
        &self,
    ) -> custody::Guard<'_> {
        self.opens
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(in crate::client_tun::compatibility_runtime) fn take_control(&self) -> bool {
        self.control_remaining
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }

    pub(in crate::client_tun::compatibility_runtime) fn record_open(
        &self,
        mut outcome: OpenOutcome,
    ) {
        if let Some(failure) = outcome.failure.take() {
            self.record(failure);
        }
        let mut ledger = self.ledger();
        ledger.additional_failures = ledger
            .additional_failures
            .saturating_add(outcome.additional);
    }

    pub(in crate::client_tun::compatibility_runtime) async fn settle_opens(&self) {
        if self.flow_mode() {
            let opens = self.opens();
            if opens.live != 0
                || !opens.audit_flows()
                || opens
                    .flow
                    .as_ref()
                    .is_some_and(|a| a.identity_fault.is_some())
            {
                self.ledger().quarantined = true;
            }
            return;
        }
        {
            let opens = self.opens();
            let mut actual = 0;
            let mut workers = [0u64; 2];
            for (index, slot) in opens.slots.iter().enumerate() {
                actual += usize::from(slot.is_some());
                if matches!(
                    slot.as_ref().map(|slot| &slot.phase),
                    Some(OpenPhase::Worker(_))
                ) {
                    workers[index / 64] |= 1u64 << (index % 64);
                }
            }
            let orphan = self
                .announced
                .iter()
                .zip(workers)
                .any(|(announced, workers)| announced.load(Ordering::Acquire) & !workers != 0);
            if actual != opens.live || orphan {
                self.ledger().quarantined = true;
            }
            if actual == 0 {
                return;
            }
        }
        let mut cursor = 0;
        let mut pending = false;
        let signal = Arc::new(OpenJoinWake::default());
        let child_waker = std::task::Waker::from(Arc::clone(&signal));
        std::future::poll_fn(|cx| {
            signal.register(cx.waker());
            if cursor == 0 {
                // Earlier notifications are covered by this complete bounded scan.
                signal.retry.store(false, Ordering::Release);
            }
            let mut child_cx = std::task::Context::from_waker(&child_waker);
            let mut opens = self.opens();
            for index in cursor..cursor + 32 {
                let Some(slot) = opens.slots.get_mut(index) else {
                    continue;
                };
                if matches!(slot.phase, OpenPhase::Worker(_)) {
                    let Some(outcome) = poll_open_slot(slot, index, &mut child_cx) else {
                        pending = true;
                        continue;
                    };
                    slot.phase = OpenPhase::Retained(outcome);
                    self.announced[index / 64]
                        .fetch_and(!(1u64 << (index % 64)), Ordering::Release);
                }
                if matches!(&slot.phase, OpenPhase::Retained(outcome) if outcome.opened.is_some()) {
                    self.ledger().quarantined = true;
                    continue;
                }
                let outcome = opens.slots.take(index).and_then(|slot| match slot.phase {
                    OpenPhase::Retained(outcome) | OpenPhase::AwaitingDisposition(outcome) => {
                        Some(outcome)
                    }
                    _ => None,
                });
                opens.live = opens.live.saturating_sub(1);
                if let Some(outcome) = outcome {
                    self.record_open(outcome);
                }
            }
            cursor = (cursor + 32) % 128;
            if cursor == 0 {
                if !pending {
                    return Poll::Ready(());
                }
                pending = false;
                if signal.retry.load(Ordering::Acquire) {
                    cx.waker().wake_by_ref();
                }
            } else {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        })
        .await;
    }
}

impl super::OpenCustody {
    pub(in crate::client_tun::compatibility_runtime) fn retain_forced_native_custody(&mut self) {
        std::mem::forget(std::mem::take(&mut self.slots));
        // Accounting owns the original retained provider attempt too. Its
        // opaque destructor must not run alongside quarantined role payloads.
        std::mem::forget(self.flow.take());
    }
}

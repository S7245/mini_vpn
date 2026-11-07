//! Original Control's notification effects, separate from its phase/deadline locks.
//! One atomic runner coalesces concurrent/reentrant stop/change/rebuild requests.
//! The first failed callback permanently seals this effect owner and is retained
//! in Control's fixed terminal slot. No DATA coordination or resource refunds.
//! Tests: tests/cold_notifications.rs.
use super::*;

pub(super) const CHANGED: usize = 1;
pub(super) const STOP: usize = 2;
pub(super) const REBUILD: usize = 4;
const ACTIVE: usize = 8;
const FAILED: usize = 16;
const EFFECTS: usize = CHANGED | STOP | REBUILD;

#[derive(Default)]
pub(super) struct Notifications {
    state: AtomicUsize,
    #[cfg(all(test, feature = "harness"))]
    pub(super) after_quarantine: Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
}
impl Notifications {
    pub(super) fn failed(&self) -> bool {
        self.state.load(Ordering::Acquire) & FAILED != 0
    }
    pub(super) fn send(&self, control: &Control, effects: usize) {
        let previous = self.state.fetch_or(effects | ACTIVE, Ordering::AcqRel);
        if previous & (ACTIVE | FAILED) != 0 {
            return;
        }
        loop {
            // Only this runner clears effects. A concurrent/reentrant caller
            // leaves its bits for the next iteration instead of running a callback.
            let pending = self.state.fetch_and(ACTIVE, Ordering::AcqRel) & EFFECTS;
            for effect in [STOP, CHANGED, REBUILD] {
                if pending & effect == 0 {
                    continue;
                }
                let result = catch_unwind(AssertUnwindSafe(|| match effect {
                    STOP => control.stop.stop_wake.notify_waiters(),
                    CHANGED => control.changed.notify_waiters(),
                    REBUILD => control.rebuild.stop_wake.notify_waiters(),
                    _ => (),
                }));
                if let Err(payload) = result {
                    // Unique runner plus permanent FAILED makes this a once-only
                    // payload site, including concurrent and callback reentrancy.
                    // Retain before publishing Q: an observer may then dispose
                    // the saved future on another thread. Its later panic must
                    // not win first capture over this original notification.
                    control.terminal.retain(saved::Site::Notification, payload);
                    self.state.store(FAILED, Ordering::Release);
                    control.quarantine_quiet();
                    #[cfg(all(test, feature = "harness"))]
                    self.pause_after_quarantine_for_test();
                    return;
                }
            }
            if self
                .state
                .compare_exchange(ACTIVE, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }
}

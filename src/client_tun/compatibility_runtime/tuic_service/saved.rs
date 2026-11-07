//! Cold saved-work custody for the one Service root, not DATA or a new actor.
//! Owns the original future's Runnable/Returned/Poisoned/Empty slot. Service owns
//! the actual Completion and commits it before disposal. Control retains the fixed
//! once-only opaque panic sites; their bytes remain Unknown and charged Q, even
//! when the final handle is destroyed. Tests: tests/own_unwind.rs.
use super::*;
use std::{any::Any, mem::ManuallyDrop};

type Payload = ManuallyDrop<Box<dyn Any + Send>>;
#[derive(Clone, Copy)]
#[repr(usize)]
pub(super) enum Site {
    Poll = 1,
    Bind,
    Notification,
    Disposal,
}
#[derive(Default)]
pub(super) struct Terminal {
    pub(super) poll: Mutex<Option<Payload>>,
    pub(super) bind: Mutex<Option<Payload>>,
    pub(super) notification: Mutex<Option<Payload>>,
    pub(super) disposal: Mutex<Option<Payload>>,
    first: AtomicUsize,
}
impl Terminal {
    /// Each site has one linear transition: failed notify seals its runner,
    /// bind runs once, poll poisons its slot, disposal takes that slot to Empty.
    pub(super) fn retain(&self, site: Site, payload: Box<dyn Any + Send>) {
        let slot = match site {
            Site::Poll => &self.poll,
            Site::Bind => &self.bind,
            Site::Notification => &self.notification,
            Site::Disposal => &self.disposal,
        };
        *slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ManuallyDrop::new(payload));
        // Publishing the site is the capture linearization point. Acquire readers
        // can now find its already retained original Box, never an empty marker.
        let _ = self
            .first
            .compare_exchange(0, site as usize, Ordering::AcqRel, Ordering::Acquire);
    }
    #[cfg(test)]
    pub(super) fn first_site(&self) -> usize {
        self.first.load(Ordering::Acquire)
    }
}

pub(super) struct Driver<F> {
    slot: Slot<F>,
}
enum Slot<F> {
    Runnable(Pin<Box<F>>),
    Returned(Pin<Box<F>>),
    Poisoned(Pin<Box<F>>),
    Empty,
}
pub(super) fn quarantine(control: &Control) {
    // Mark the shared hard authority before invoking any stop/phase wake callback.
    if let Some(meter) = &control.app_meter {
        meter.quarantine();
    }
    control.request_stop();
    control.publish(Phase::Quarantined);
}
impl<F> Driver<F> {
    pub(super) fn new(work: Pin<Box<F>>) -> Self {
        Self {
            slot: Slot::Runnable(work),
        }
    }
    // Service invokes this only after installing original Completion or while
    // being forcibly destroyed. Empty prevents Ready/forced double disposal.
    pub(super) fn dispose(&mut self, control: &Control) {
        let work = match std::mem::replace(&mut self.slot, Slot::Empty) {
            Slot::Runnable(work) | Slot::Returned(work) | Slot::Poisoned(work) => work,
            Slot::Empty => return,
        };
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(work))) {
            // Exactly one disposal transition: no second write or payload Drop.
            control.terminal.retain(Site::Disposal, payload);
            quarantine(control);
        }
    }
}
impl<F: Future<Output = Completion>> Driver<F> {
    pub(super) fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        control: &Control,
    ) -> Poll<Completion> {
        if control.notifications.failed() {
            return Poll::Pending;
        }
        let result = match &mut self.slot {
            Slot::Runnable(work) => catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))),
            Slot::Poisoned(_) => return Poll::Pending,
            Slot::Returned(_) | Slot::Empty => {
                quarantine(control);
                return Poll::Pending;
            }
        };
        match result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(done)) => {
                self.slot = match std::mem::replace(&mut self.slot, Slot::Empty) {
                    Slot::Runnable(work) => Slot::Returned(work),
                    original => original,
                };
                Poll::Ready(done)
            }
            Err(payload) => {
                // One Runnable→Poisoned transition; preserve the exact original
                // payload and future before publishing stop/Q. Never poll it again.
                control.terminal.retain(Site::Poll, payload);
                self.slot = match std::mem::replace(&mut self.slot, Slot::Empty) {
                    Slot::Runnable(work) => Slot::Poisoned(work),
                    original => original,
                };
                quarantine(control);
                Poll::Pending
            }
        }
    }
}

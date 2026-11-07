//! Original Session's single cold IO role and absolute-deadline observer.
//! Egress borrows it exclusively; Session retires its actual callback inventory
//! before completion. Flush, stop waiter and timer have separate Drop custody.
//! No task, queue, fresh deadline, ACK or Device-release authority lives here.
//! Entry: prepare/claim/release/retire and Native::flush. Tests: ../tests/deadline.
use super::*;
use crate::shared::{
    actor_callbacks::{Field, Lease, SourceCallbacks},
    errors::{FailureOrder, QuietFailureSink},
};
use bytes::MeteredCopyError;
use std::{
    future::{Future, poll_fn},
    pin::Pin,
};

#[derive(Default)]
pub(in crate::client_tun::compatibility_runtime) enum ColdSlot {
    #[default]
    Empty,
    Preparing,
    Ready(Stock),
    InUse,
    Returned(Stock),
    Retired,
    Refused(MeteredCopyError),
}
pub(in crate::client_tun::compatibility_runtime) struct Stock {
    callbacks: SourceCallbacks,
}
pub(super) struct Native {
    lease: Lease,
    stock: Stock,
}
fn lock(owner: &SessionControl) -> Result<std::sync::MutexGuard<'_, ColdSlot>, MeteredCopyError> {
    match owner.egress_cold.lock() {
        Ok(slot) => Ok(slot),
        Err(poison) => {
            drop(poison.into_inner());
            owner.quarantine_quiet();
            Err(MeteredCopyError::Quarantined)
        }
    }
}
// Only cold preparation and the unique borrowed Native can return a value.
// Even poison must keep that actual stock; it never authorizes clean progress.
fn put(owner: &SessionControl, value: ColdSlot) -> Result<(), MeteredCopyError> {
    let (mut slot, poisoned) = match owner.egress_cold.lock() {
        Ok(slot) => (slot, false),
        Err(poison) => (poison.into_inner(), true),
    };
    if !matches!(*slot, ColdSlot::Preparing | ColdSlot::InUse) {
        drop(slot);
        owner.quarantine_quiet();
        // A sole construction/return token cannot encounter another inventory
        // through this API. Unknown authority retains both physical owners.
        std::mem::forget(value);
        return Err(MeteredCopyError::Quarantined);
    }
    *slot = value;
    drop(slot);
    if poisoned {
        owner.quarantine_quiet();
        Err(MeteredCopyError::Quarantined)
    } else {
        Ok(())
    }
}
pub(in crate::client_tun::compatibility_runtime) fn layout_bytes() -> usize {
    SourceCallbacks::layout_bytes() + FailureOrder::layout_bytes()
}
fn hard(owner: &SessionControl) -> bool {
    owner
        .app_meter
        .as_ref()
        .is_some_and(|m| m.budget_limit().is_some())
}
pub(in crate::client_tun::compatibility_runtime) fn admission_io(
    error: MeteredCopyError,
) -> io::Error {
    quinn::owned_events::AdmissionError(error).into()
}
pub(in crate::client_tun::compatibility_runtime) fn prepare(
    owner: &Arc<SessionControl>,
) -> Result<(), MeteredCopyError> {
    if !hard(owner) {
        return Ok(());
    }
    {
        let mut slot = lock(owner)?;
        match &*slot {
            ColdSlot::Ready(_) => return Ok(()),
            ColdSlot::Refused(error) => return Err(*error),
            ColdSlot::Empty => *slot = ColdSlot::Preparing,
            _ => return Err(MeteredCopyError::BudgetFull),
        }
    }
    let result = (|| {
        let meter = owner
            .app_meter
            .as_ref()
            .ok_or(MeteredCopyError::Quarantined)?;
        let clock = Arc::new(FailureOrder::default());
        let sink: Arc<dyn QuietFailureSink> = owner.clone();
        clock.bind(
            Arc::downgrade(&sink),
            SessionControl::origin(FailurePhase::DeviceWrite),
            Some(meter.clone()),
        )?;
        Ok(Stock {
            callbacks: SourceCallbacks::prepare_source(meter, clock)?,
        })
    })();
    let outcome = result.as_ref().map(|_| ()).map_err(|e| *e);
    put(
        owner,
        match result {
            Ok(stock) => ColdSlot::Ready(stock),
            Err(error) => ColdSlot::Refused(error),
        },
    )?;
    if outcome == Err(MeteredCopyError::Quarantined) {
        owner.quarantine_quiet();
    }
    outcome
}
pub(super) fn claim(owner: &Arc<SessionControl>) -> Result<Option<Native>, MeteredCopyError> {
    if !hard(owner) {
        return Ok(None);
    }
    // Service prepares before its factory; standalone composition uses this
    // same cold entry before constructing any actual IO future.
    prepare(owner)?;
    let stock = {
        let mut slot = lock(owner)?;
        match std::mem::replace(&mut *slot, ColdSlot::InUse) {
            ColdSlot::Ready(stock) => stock,
            original => {
                *slot = original;
                return Err(MeteredCopyError::BudgetFull);
            }
        }
    };
    match stock.callbacks.acquire_source() {
        Ok(lease) => Ok(Some(Native { lease, stock })),
        Err(_) => {
            let _ = put(owner, ColdSlot::Returned(stock));
            owner.quarantine_quiet();
            Err(MeteredCopyError::Quarantined)
        }
    }
}
pub(super) fn release(owner: &SessionControl, native: Native) {
    let Native { lease, stock } = native;
    if owner.egress_cold.is_poisoned() {
        owner.quarantine_quiet();
    }
    // Original Waker and its possible last Arc retire outside all owner locks.
    drop(lease);
    let _ = put(owner, ColdSlot::Returned(stock));
    retire(owner);
}
pub(in crate::client_tun::compatibility_runtime) fn retire(owner: &SessionControl) -> bool {
    let stock = {
        let Ok(mut slot) = lock(owner) else {
            return false;
        };
        match &*slot {
            ColdSlot::Empty | ColdSlot::Retired | ColdSlot::Refused(_) => return true,
            ColdSlot::Returned(stock) | ColdSlot::Ready(stock) if stock.callbacks.quiescent() => {}
            _ => return false,
        }
        match std::mem::replace(&mut *slot, ColdSlot::Retired) {
            ColdSlot::Returned(stock) | ColdSlot::Ready(stock) => stock,
            original => {
                *slot = original;
                return false;
            }
        }
    };
    drop(stock);
    true
}
impl Native {
    pub(super) fn flush<'a, F: Future<Output = io::Result<()>> + 'a>(
        &'a mut self,
        owner: &'a Arc<SessionControl>,
        work: F,
    ) -> impl Future<Output = io::Result<()>> + 'a {
        // All original fields enter individual custody synchronously, before
        // the returned future can be cancelled without ever being polled.
        let lease = &mut self.lease;
        let guard = lease.guard();
        let work = guard.own(work, Field::Source0);
        let stopped = guard.own(owner.stop.stop_wake.notified(), Field::Source1);
        let timer = guard.own(None::<tokio::time::Sleep>, Field::Source2);
        async move {
            let mut work = std::pin::pin!(work);
            let mut stopped = std::pin::pin!(stopped);
            let mut timer = std::pin::pin!(timer);
            let mut frozen = false;
            poll_fn(|cx| {
                if frozen {
                    return Poll::Pending;
                }
                let mut body = Some(poll_fn(|safe| {
                    // Structural pin: each Custody drops its own field in place.
                    let io = unsafe { work.as_mut().map_unchecked_mut(|w| &mut **w) }.poll(safe);
                    if let Poll::Ready(result) = io {
                        // Commit the actual source before any waiter/work Drop.
                        return Poll::Ready(result.map_err(|error| super::failed(owner, error)));
                    }
                    if unsafe { stopped.as_mut().map_unchecked_mut(|w| &mut **w) }
                        .poll(safe)
                        .is_ready()
                    {
                        // Keep the one original notification subscription after
                        // stop: parent/local limits may still become earlier.
                        stopped
                            .as_mut()
                            .set(guard.own(owner.stop.stop_wake.notified(), Field::Source1));
                        let _ =
                            unsafe { stopped.as_mut().map_unchecked_mut(|w| &mut **w) }.poll(safe);
                    }
                    if owner
                        .stop
                        .stopping
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        if let Some(deadline) = owner.deadline_quiet() {
                            let timer = unsafe { timer.as_mut().get_unchecked_mut() };
                            if timer.is_none() {
                                **timer = Some(tokio::time::sleep_until(deadline));
                            }
                            if let Some(sleep) = timer.as_mut() {
                                let mut sleep = unsafe { Pin::new_unchecked(sleep) };
                                if deadline < sleep.deadline() {
                                    sleep.as_mut().reset(deadline);
                                }
                                if sleep.as_mut().poll(safe).is_ready() {
                                    frozen = true;
                                    owner.quarantine_quiet();
                                }
                            }
                        }
                    }
                    Poll::Pending
                }));
                let outcome = match lease.run(&mut body) {
                    Ok(turn) => {
                        let mut turn = std::pin::pin!(turn);
                        Some(turn.as_mut().poll(cx))
                    }
                    Err(_) => None,
                };
                drop(body);
                match outcome {
                    Some(Poll::Ready(Ok(result))) => Poll::Ready(result),
                    Some(Poll::Ready(Err(failure))) => {
                        frozen = true;
                        owner.quarantine_quiet();
                        failure.output.map_or(Poll::Pending, Poll::Ready)
                    }
                    Some(Poll::Pending) => Poll::Pending,
                    None => {
                        frozen = true;
                        owner.quarantine_quiet();
                        Poll::Pending
                    }
                }
            })
            .await
        }
    }
}

#[cfg(test)]
pub(in crate::client_tun::compatibility_runtime) fn inventory_for_test(
    owner: &SessionControl,
) -> Option<(usize, bool, usize, bool, bool, usize)> {
    match &*lock(owner).ok()? {
        ColdSlot::Ready(stock) | ColdSlot::Returned(stock) => {
            stock.callbacks.source_inventory_for_test()
        }
        _ => None,
    }
}

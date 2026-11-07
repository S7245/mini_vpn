//! Same-Root fanout custody: original registration, lane, ticket and value.
//! All <=19 lanes are pinned before Root mutation. Publication only writes fixed
//! slots. Wakes, ticket cancellation and displaced socket disposal occur after
//! Root unlock. DATA and peer acknowledgement remain with their original owners.
//! Tests: construction/tests/fanout.rs and Proto owned_events/tests/control.rs.
use super::*;
use proto::owned_events::{
    ControlPermit, ControlWake, HeldControl, PublishedControl, ReserveError,
};

struct Target {
    handle: ConnectionHandle,
    key: Option<proto::RegistrationKey>,
    permit: ControlPermit<ConnectionEvent>,
    event: Option<ConnectionEvent>,
}
pub(super) struct Fanout {
    targets: [Option<Target>; BUDGETED_CONNECTION_LIMIT],
    wakes: [Option<ControlWake>; BUDGETED_CONNECTION_LIMIT],
    pub(super) candidate: Option<Arc<dyn AsyncUdpSocket>>,
    pub(super) sockets: [Option<Arc<dyn AsyncUdpSocket>>; 2],
    pub(super) meter: Option<BackingMeter>,
    pub(super) driver: Option<Waker>,
}
/// Covers opaque callbacks while Root is still locked, before poison recovery.
pub(super) struct RootPanic<'a>(pub(super) &'a Option<BackingMeter>);
impl Drop for RootPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(meter) = self.0 {
                meter.quarantine();
            }
        }
    }
}
impl Fanout {
    pub(super) fn new() -> Self {
        Self {
            targets: std::array::from_fn(|_| None),
            wakes: std::array::from_fn(|_| None),
            candidate: None,
            sockets: [None, None],
            meter: None,
            driver: None,
        }
    }
    pub(super) fn prepare(
        &mut self,
        state: &State,
        kind: ControlKind,
        mut event: impl FnMut() -> ConnectionEvent,
    ) -> Result<(), ReserveError> {
        if state.recv_state.connections.catalog.len() > BUDGETED_CONNECTION_LIMIT {
            return Err(ReserveError::Closed);
        }
        for (target, entry) in self
            .targets
            .iter_mut()
            .zip(state.recv_state.connections.catalog.entries())
        {
            let ForwardControlPermit::Budgeted(permit) = entry.sender.reserve_control(kind)? else {
                return Err(ReserveError::Closed);
            };
            *target = Some(Target {
                handle: entry.handle,
                key: entry.key,
                permit,
                event: Some(event()),
            });
        }
        Ok(())
    }
    pub(super) fn apply(
        &mut self,
        state: &mut State,
        kind: ControlKind,
        commit_root: impl FnOnce(
            &mut State,
            Option<Arc<dyn AsyncUdpSocket>>,
        ) -> [Option<Arc<dyn AsyncUdpSocket>>; 2],
    ) -> Result<(), ReserveError> {
        if !self.inventory_valid(state, kind) {
            if let Some(meter) = &state.recv_state.backing_meter {
                meter.quarantine();
            }
            return Err(ReserveError::Closed);
        }
        let mut held: [Option<(HeldControl<'_, ConnectionEvent>, ConnectionEvent)>;
            BUDGETED_CONNECTION_LIMIT] = std::array::from_fn(|_| None);
        for (slot, target) in held.iter_mut().zip(self.targets.iter_mut()) {
            if let Some(target) = target {
                let event = target.event.take().ok_or(ReserveError::Closed)?;
                *slot = Some((target.permit.hold()?, event));
            }
        }
        let mut published: [Option<PublishedControl<'_, ConnectionEvent>>;
            BUDGETED_CONNECTION_LIMIT] = std::array::from_fn(|_| None);
        self.sockets = commit_root(state, self.candidate.take());
        for (slot, held) in published.iter_mut().zip(held.iter_mut()) {
            if let Some((guard, event)) = held.take() {
                *slot = Some(guard.commit(event));
            }
        }
        for (wake, slot) in self.wakes.iter_mut().zip(published) {
            *wake = slot.and_then(PublishedControl::unlock);
        }
        Ok(())
    }
    fn inventory_valid(&self, state: &State, kind: ControlKind) -> bool {
        let catalog = &state.recv_state.connections.catalog;
        if (kind == ControlKind::Rebind) != self.candidate.is_some()
            || !state.registration_inventory_valid()
            || self.targets.iter().flatten().count() != catalog.len()
        {
            return false;
        }
        // Equal independent counts plus unique actual lane/key membership closes
        // both directions: no live Catalog entry can be omitted from the batch.
        self.targets.iter().enumerate().all(|(index, target)| {
            let Some(target) = target else { return true };
            let Some(entry) = catalog
                .entries()
                .find(|entry| entry.handle == target.handle && entry.key == target.key)
            else {
                return false;
            };
            let ForwardSender::Budgeted(sender) = &entry.sender else {
                return false;
            };
            let event_matches = matches!(
                (&target.event, kind),
                (Some(ConnectionEvent::Rebind(_)), ControlKind::Rebind)
                    | (Some(ConnectionEvent::Close { .. }), ControlKind::Close)
            );
            let socket_matches = match (&target.event, &self.candidate) {
                (Some(ConnectionEvent::Rebind(socket)), Some(candidate)) => {
                    Arc::ptr_eq(socket, candidate)
                }
                (Some(ConnectionEvent::Close { .. }), None) => true,
                _ => false,
            };
            event_matches
                && socket_matches
                && target.permit.belongs_to(sender, kind)
                && !self.targets[..index]
                    .iter()
                    .flatten()
                    .any(|other| target.permit.same_lane(&other.permit))
        })
    }
    pub(super) fn finish(mut self) {
        for wake in &mut self.wakes {
            if let Some(wake) = wake.take() {
                wake.wake();
            }
        }
        // Drop below disposes original sockets outside Root, including on panic.
    }
}
impl Drop for Fanout {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        if unwinding {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
        }
        let mut first = None;
        for target in self.targets.iter_mut().flatten() {
            let event = target.event.take();
            if let Err(payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(event)))
            {
                if let Some(meter) = &self.meter {
                    meter.quarantine();
                }
                if !unwinding && first.is_none() {
                    first = Some(payload);
                } else {
                    mem::forget(payload);
                }
            }
        }
        for socket in self
            .sockets
            .iter_mut()
            .chain(std::iter::once(&mut self.candidate))
        {
            let socket = socket.take();
            if let Err(payload) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(socket)))
            {
                if let Some(meter) = &self.meter {
                    meter.quarantine();
                }
                if !unwinding && first.is_none() {
                    first = Some(payload);
                } else {
                    mem::forget(payload);
                }
            }
        }
        if let Some(driver) = self.driver.take() {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if unwinding || first.is_some() {
                    drop(driver);
                } else {
                    driver.wake();
                }
            }));
            if let Err(payload) = result {
                if let Some(meter) = &self.meter {
                    meter.quarantine();
                }
                if !unwinding && first.is_none() {
                    first = Some(payload);
                } else {
                    mem::forget(payload);
                }
            }
        }
        if let Some(payload) = first {
            std::panic::resume_unwind(payload);
        }
    }
}

#[cfg(test)]
mod tests;

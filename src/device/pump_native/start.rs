//! The original Device's unpublished Native source/pump transaction. It retains
//! Source and the actual prepared lane until bind validates every Device owner;
//! the sole final commit starts the original PumpGuard. No extra actor or budget.
//! Cancellation disposes the real unstarted Source and lane with original first
//! failure custody. Tests: device/tests/cold_factory.rs and tests/pump_native.rs.
use super::*;

pub(crate) enum Startup<S> {
    Empty,
    Prepared(Preparation<S>),
    Running(TunIngressPump),
}
pub(crate) struct Preparation<S> {
    lane: Option<Prepared>,
    source: Option<ManuallyDrop<S>>,
    runtime: tokio::runtime::Handle,
    control: Arc<PumpControl>,
    committing: bool,
}
impl<S> Preparation<S> {
    fn dispose_source(&mut self) {
        if self.committing {
            self.control.quarantine_quiet();
            self.control.request_stop();
        }
        if let Some(source) = self.source.take() {
            if let Err(payload) =
                catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(source))))
            {
                self.control
                    .capture(pump_control::Site::SourceDrop, payload);
            }
        }
        if let Some(lane) = self.lane.take() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(lane))) {
                self.control.capture(pump_control::Site::WorkDrop, payload);
            }
        }
    }
}
impl<S> Drop for Preparation<S> {
    fn drop(&mut self) {
        self.dispose_source();
    }
}
impl<S> Startup<S> {
    pub(crate) fn new(lane: Prepared, source: S, runtime: tokio::runtime::Handle) -> Self {
        let control = lane.control.clone();
        Self::Prepared(Preparation {
            lane: Some(lane),
            source: Some(ManuallyDrop::new(source)),
            runtime,
            control,
            committing: false,
        })
    }
    pub(crate) fn pending(&self) -> bool {
        matches!(self, Self::Prepared(_))
    }
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
    pub(crate) fn as_ref(&self) -> Option<&TunIngressPump> {
        if let Self::Running(pump) = self {
            Some(pump)
        } else {
            None
        }
    }
    pub(crate) fn as_mut(&mut self) -> Option<&mut TunIngressPump> {
        if let Self::Running(pump) = self {
            Some(pump)
        } else {
            None
        }
    }
    pub(crate) fn is_native(&self) -> bool {
        match self {
            Self::Prepared(_) => true,
            Self::Running(pump) => pump.control.native_meter.is_some() || pump.receiver.is_native(),
            Self::Empty => false,
        }
    }
    pub(crate) fn poll_shutdown(
        &mut self,
        cx: &mut Context<'_>,
        previous: Option<ErrorKind>,
    ) -> Poll<DeviceShutdown> {
        match self {
            Self::Running(pump) => pump.poll_shutdown(cx),
            Self::Prepared(pending) => {
                if pending.committing {
                    pending.control.quarantine_quiet();
                    pending.control.request_stop();
                    return Poll::Pending;
                }
                pending.dispose_source();
                let mut result = DeviceShutdown::from_status(previous);
                if pending.control.failed.load(Ordering::Acquire) {
                    result.failure_kind = Some(pending.control.failure_kind());
                    result.failure = pending.control.take_failure();
                }
                result.additional_failures = pending
                    .control
                    .additional_failures
                    .swap(0, Ordering::Relaxed);
                Poll::Ready(result)
            }
            Self::Empty => Poll::Ready(DeviceShutdown::from_status(previous)),
        }
    }
    pub(crate) fn validate(
        &self,
        meter: &bytes::BackingMeter,
        capacity: usize,
    ) -> std::io::Result<()> {
        match self {
            Self::Prepared(pending) => {
                let lane = pending.lane.as_ref().ok_or_else(device_closed)?;
                let current = tokio::runtime::Handle::try_current()
                    .map_err(|_| std::io::Error::from(ErrorKind::Unsupported))?;
                if current.id() != pending.runtime.id() {
                    return Err(ErrorKind::InvalidInput.into());
                }
                if pending.source.is_none() || pending.committing {
                    return Err(device_closed());
                }
                if !pending
                    .control
                    .native_meter
                    .as_ref()
                    .is_some_and(|m| m.same_authority(meter))
                    || lane.capacity != capacity
                {
                    return Err(ErrorKind::InvalidInput.into());
                }
                if !Arc::ptr_eq(&lane.control, &pending.control)
                    || !pending
                        .control
                        .native_lane
                        .as_ref()
                        .is_some_and(|id| lane.sender.matches_identity(id))
                {
                    pending.control.quarantine_quiet();
                    return Err(device_closed());
                }
                lane.receiver.validate_ordinary(
                    meter,
                    capacity,
                    pending
                        .control
                        .native_lane
                        .as_ref()
                        .ok_or_else(device_closed)?,
                    true,
                )
            }
            Self::Running(pump) => pump.bind_native(meter, capacity),
            Self::Empty => Err(ErrorKind::Unsupported.into()),
        }
    }
}
impl<S: Source> Startup<S> {
    /// All fallible checks belong to validate and the original Device bind.
    /// The original pending slot stays in D during the opaque spawn call.
    pub(crate) fn commit(&mut self) -> std::io::Result<()> {
        let Self::Prepared(pending) = self else {
            return Ok(());
        };
        let (Some(lane), Some(source)) = (pending.lane.take(), pending.source.take()) else {
            pending.control.quarantine_quiet();
            return Err(device_closed());
        };
        pending.committing = true;
        let pump = lane.spawn_on(ManuallyDrop::into_inner(source), &pending.runtime);
        pending.committing = false;
        *self = Self::Running(pump);
        Ok(())
    }
}
impl Receiver {
    fn is_native(&self) -> bool {
        matches!(self.inner, Some(Kind::Native(_)))
    }
}
impl TunIngressPump {
    fn bind_native(&self, meter: &bytes::BackingMeter, capacity: usize) -> std::io::Result<()> {
        if !self
            .control
            .native_meter
            .as_ref()
            .is_some_and(|m| m.same_authority(meter))
            || self.capacity_packets != capacity
        {
            return Err(ErrorKind::InvalidInput.into());
        }
        let Some(Kind::Native(receiver)) = &self.receiver.inner else {
            return Err(ErrorKind::Unsupported.into());
        };
        receiver.validate_ordinary(
            meter,
            capacity,
            self.control
                .native_lane
                .as_ref()
                .ok_or_else(device_closed)?,
            false,
        )
    }
}

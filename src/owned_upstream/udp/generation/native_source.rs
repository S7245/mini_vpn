//! Native half of the original generation Source. Owns the one receiver and
//! fixed fragment inventory, never producer/epoch authority. Final retirement
//! marks destructing; only the lane's actual context retirement marks absent.
//! Entry: prepare/Source::recv; production callers are Node's cold UDP assembly.
//! Lifecycle tracers live in native_source/tests.
use super::callbacks::{Callbacks, Field, Guard, Lease, Role};
use super::native_fragments::Fragments;
use super::*;
use crate::shared::{bounded_channel as lane, errors::FailureClock};
use bytes::{BackingMeter, MeteredCopyError};

pub(crate) struct Frame {
    epoch: u64,
    bytes: Bytes,
}
impl lane::Value for Frame {
    type Disposition = Arc<GenerationControl>;
    fn lane_bytes(&self) -> usize {
        self.bytes.len()
    }
    fn valid_for(&self, meter: &BackingMeter) -> bool {
        self.bytes
            .backing_meter()
            .is_some_and(|m| m.same_authority(meter))
    }
    fn dispose(self, fault: &lane::Fault<'_>, control: &Self::Disposition) -> Option<Self> {
        control.discarded_messages.fetch_add(1, Ordering::Relaxed);
        control
            .discarded_bytes
            .fetch_add(self.bytes.len() as u64, Ordering::Relaxed);
        fault.dispose(self.bytes);
        None
    }
    fn dispose_context(
        control: Self::Disposition,
        fault: &lane::Fault<'_>,
    ) -> Option<Self::Disposition> {
        // The Core has already proved sender/old credit/callback/ring quiescence.
        // Notify uses only role proxies; keep the original Arc if it fails.
        if fault.failed() {
            return Some(control);
        }
        if control.consumer_disposition.load(Ordering::Acquire) != 1 {
            return Some(control);
        }
        control.consumer_disposition.store(2, Ordering::Release);
        fault.dispose_context_head(ControlNotification(std::mem::ManuallyDrop::new(
            control.clone(),
        )));
        if fault.failed() {
            control.consumer_disposition.store(1, Ordering::Release);
            return Some(control);
        }
        fault.dispose_context_tail(control);
        None
    }
}
struct ControlNotification(std::mem::ManuallyDrop<Arc<GenerationControl>>);
impl Drop for ControlNotification {
    fn drop(&mut self) {
        self.0.changed.notify_waiters();
        unsafe {
            std::mem::ManuallyDrop::drop(&mut self.0);
        }
    }
}
#[derive(Clone)]
pub(crate) struct Sender(lane::Sender<Frame>);
impl Sender {
    pub(super) async fn send(&self, epoch: u64, bytes: Bytes) -> Result<(), lane::ReserveError> {
        self.0.send(Frame { epoch, bytes }).await.map_err(|error| {
            let lane::SendError(frame, reason) = error;
            drop(frame); // Bytes is the admitted concrete backing; no opaque user Drop.
            reason
        })
    }
    pub(super) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}
pub(crate) type Observer = lane::Observer<Frame>;
pub(crate) struct Source {
    root: std::mem::ManuallyDrop<Box<State>>,
    stock: std::mem::ManuallyDrop<Option<bytes::BackingBudgetReservation>>,
}
struct State {
    receiver: std::mem::ManuallyDrop<Option<lane::Receiver<Frame>>>,
    fragments: std::mem::ManuallyDrop<Option<Fragments>>,
    control: std::mem::ManuallyDrop<Option<Arc<GenerationControl>>>,
    meter: BackingMeter,
    clock: FailureClock,
    lease: std::mem::ManuallyDrop<Option<Lease>>,
    guard: std::mem::ManuallyDrop<Option<Guard>>,
    held: std::mem::ManuallyDrop<Option<UdpDownlinkEvent>>,
    control_drop: Option<std::mem::ManuallyDrop<Box<dyn std::any::Any + Send>>>,
    retired: bool,
    #[cfg(test)]
    quarantine_ready: bool,
}
pub(crate) fn prepare(
    control: Arc<GenerationControl>,
    meter: &BackingMeter,
    clock: FailureClock,
    callbacks: &Callbacks,
) -> Result<(GenerationSender, UdpDownlinkSource, Observer), MeteredCopyError> {
    use std::{
        alloc::{Layout, alloc, dealloc},
        mem::ManuallyDrop,
    };
    let layout = Layout::new::<State>();
    let stock = meter.reserve_cold_layout(layout.size())?;
    let pointer = unsafe { alloc(layout).cast::<State>() };
    if pointer.is_null() {
        return Err(MeteredCopyError::Allocation);
    }
    struct Allocation(*mut State);
    impl Drop for Allocation {
        fn drop(&mut self) {
            unsafe {
                dealloc(self.0.cast(), Layout::new::<State>());
            }
        }
    }
    let allocation = Allocation(pointer);
    let fragments = Fragments::prepare(meter)?;
    let lease = callbacks
        .acquire(Role::Source)
        .map_err(|error| match error {
            callbacks::CallbackError::Capacity => MeteredCopyError::BudgetFull,
            callbacks::CallbackError::Quarantined => MeteredCopyError::Quarantined,
        })?;
    let guard = lease.guard();
    let limits = lane::NativeCapacity {
        messages: 1024,
        waiters: 257,
    };
    let mut context = Some(control.clone());
    let (tx, rx, observer) = lane::native_with(limits, meter, clock.clone(), &mut context)?;
    control.bind_native_meter(meter)?;
    unsafe {
        pointer.write(State {
            receiver: ManuallyDrop::new(Some(rx)),
            fragments: ManuallyDrop::new(Some(fragments)),
            control: ManuallyDrop::new(Some(control.clone())),
            meter: meter.clone(),
            clock,
            lease: ManuallyDrop::new(Some(lease)),
            guard: ManuallyDrop::new(Some(guard)),
            held: ManuallyDrop::new(None),
            control_drop: None,
            retired: false,
            #[cfg(test)]
            quarantine_ready: false,
        });
    }
    std::mem::forget(allocation);
    let source = Source {
        root: ManuallyDrop::new(unsafe { Box::from_raw(pointer) }),
        stock: ManuallyDrop::new(stock),
    };
    Ok((
        GenerationSender {
            sender: None,
            native: Some(Sender(tx)),
            control,
        },
        UdpDownlinkSource {
            inner: UdpDownlinkSourceInner::Native(source),
        },
        observer,
    ))
}
impl Source {
    #[cfg(test)]
    pub(in crate::owned_upstream::udp) fn control(&self) -> Arc<GenerationControl> {
        self.root.control.as_ref().unwrap().clone()
    }
    #[cfg(test)]
    pub(in crate::owned_upstream::udp) fn pending(&self) -> usize {
        self.root.receiver.as_ref().map_or(0, |rx| rx.len())
    }
    pub(in crate::owned_upstream::udp) async fn recv(
        &mut self,
        clock: &Instant,
    ) -> Option<UdpDownlinkEvent> {
        let root = &mut **self.root;
        if root.retired || root.meter.is_quarantined() {
            return None;
        }
        let State {
            receiver,
            fragments,
            control,
            meter,
            lease,
            clock: failure_clock,
            ..
        } = root;
        #[cfg(test)]
        let quarantine_ready = root.quarantine_ready;
        let original_control = control.as_ref()?;
        let mut original = Some(async {
            let output = Self::receive(
                receiver,
                fragments,
                original_control,
                meter,
                clock,
                failure_clock,
            )
            .await;
            #[cfg(test)]
            if quarantine_ready && matches!(&output, Some(UdpDownlinkEvent::Deliver(_))) {
                meter.quarantine();
            }
            output
        });
        let result = match lease.as_mut()?.run(&mut original) {
            Ok(work) => work.await,
            Err(error) => Err(callbacks::CallbackFailure {
                output: None,
                error,
            }),
        };
        drop(original);
        match result {
            Ok(None) => {
                self.retire();
                None
            }
            Ok(output) => output,
            Err(failure) => {
                *self.root.held = failure.output.flatten();
                self.root
                    .control
                    .as_ref()?
                    .invalid
                    .store(true, Ordering::Release);
                self.root.meter.quarantine();
                None // terminal observation; original output/state stay in this Box
            }
        }
    }
    async fn receive(
        receiver: &mut Option<lane::Receiver<Frame>>,
        fragments: &mut Option<Fragments>,
        control: &GenerationControl,
        meter: &BackingMeter,
        clock: &Instant,
        failure_clock: &FailureClock,
    ) -> Option<UdpDownlinkEvent> {
        loop {
            let rx = receiver.as_mut()?;
            let notified = control.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let epoch = control.epoch();
            if meter.is_quarantined() {
                control.invalid.store(true, Ordering::Release);
                return None;
            }
            if control.frozen(epoch) {
                if control.producer.load(Ordering::Acquire) == epoch
                    && control.consumer.load(Ordering::Acquire) != epoch
                {
                    Self::discard(rx, control);
                    let clean = fragments.as_mut().is_some_and(Fragments::retire_generation);
                    // Original real producer JOIN excludes publications here.
                    if !clean || !unsafe { rx.empty_under_producer_seal() } {
                        meter.quarantine();
                        control.invalid.store(true, Ordering::Release);
                        return Some(UdpDownlinkEvent::Ignored);
                    }
                    control.consumer_sealed(epoch);
                    return Some(UdpDownlinkEvent::Ignored);
                }
                if rx.producers_gone() {
                    return None;
                }
                notified.await;
                continue;
            }
            let frame = tokio::select! { biased;
                _ = &mut notified => continue,
                frame = rx.recv() => frame,
            };
            let Some(frame) = frame else {
                return None;
            };
            if frame.epoch != epoch || control.frozen(epoch) {
                control.discarded_messages.fetch_add(1, Ordering::Relaxed);
                control
                    .discarded_bytes
                    .fetch_add(frame.bytes.len() as u64, Ordering::Relaxed);
                return Some(UdpDownlinkEvent::Ignored);
            }
            return Some(
                match fragments
                    .as_mut()?
                    .accept(frame.bytes, clock.elapsed().as_secs())
                {
                    Ok(Some((association, payload))) => {
                        match LocalAssociationId::new(association) {
                            Some(id) => UdpDownlinkEvent::Deliver(UdpDownlink::new(id, payload)),
                            None => UdpDownlinkEvent::Ignored,
                        }
                    }
                    Ok(None) => UdpDownlinkEvent::Ignored,
                    Err(rejected) => {
                        match rejected.error {
                            MeteredCopyError::BudgetFull
                            | MeteredCopyError::Allocation
                            | MeteredCopyError::TooLarge => {
                                control.capacity_drops.fetch_add(1, Ordering::Relaxed);
                            }
                            MeteredCopyError::Quarantined => {
                                failure_clock
                                    .io(&std::io::Error::from(std::io::ErrorKind::InvalidData));
                                control.invalid.store(true, Ordering::Release);
                                meter.quarantine();
                            }
                        }
                        drop(rejected.frame);
                        UdpDownlinkEvent::Ignored
                    }
                },
            );
        }
    }
    fn discard(rx: &mut lane::Receiver<Frame>, control: &GenerationControl) {
        while let Ok(frame) = rx.try_recv() {
            control.discarded_messages.fetch_add(1, Ordering::Relaxed);
            control
                .discarded_bytes
                .fetch_add(frame.bytes.len() as u64, Ordering::Relaxed);
            drop(frame);
        }
    }
    pub(in crate::owned_upstream::udp) fn sweep(&mut self, now: u64, ttl: u64) {
        if !self
            .root
            .fragments
            .as_mut()
            .is_none_or(|f| f.sweep(now, ttl))
        {
            if let Some(control) = self.root.control.as_ref() {
                control.invalid.store(true, Ordering::Release);
            }
            self.root.meter.quarantine();
        }
    }
    fn retire(&mut self) -> bool {
        let root = &mut **self.root;
        if root.retired {
            return !root.meter.is_quarantined();
        }
        let Some(control) = root.control.as_ref() else {
            root.meter.quarantine();
            return false;
        };
        if root.meter.is_quarantined() {
            control.invalid.store(true, Ordering::Release);
            return false;
        }
        let publication = control.publication.lock().unwrap_or_else(|e| {
            root.meter.quarantine();
            control.invalid.store(true, Ordering::Release);
            e.into_inner()
        });
        control.consumer_disposition.store(1, Ordering::Release);
        drop(publication);
        let Some(guard) = root.guard.as_ref() else {
            root.meter.quarantine();
            return false;
        };
        if let Some(rx) = root.receiver.as_mut() {
            rx.close();
            Self::discard(rx, control);
        }
        if !root
            .fragments
            .as_mut()
            .is_none_or(Fragments::retire_generation)
        {
            control.invalid.store(true, Ordering::Release);
            root.meter.quarantine();
            return false;
        }
        guard.dispose(root.fragments.take(), Field::Source1);
        guard.dispose(root.receiver.take(), Field::Source0);
        guard.notify(&control.changed);
        // Retirement of the original waiter precedes role/guard release.
        drop(root.lease.take());
        if guard.failed() {
            return false;
        }
        drop(root.guard.take());
        root.retired = true;
        true
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        if !self.retire() {
            return;
        } // preserve the actual Box, outputs and charge under Q
        let control = self.root.control.take();
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(control)))
        {
            self.root.control_drop = Some(std::mem::ManuallyDrop::new(payload));
            self.root.clock.panic();
            self.root.meter.quarantine();
            return;
        }
        unsafe {
            std::mem::ManuallyDrop::drop(&mut self.root);
            std::mem::ManuallyDrop::drop(&mut self.stock);
        }
    }
}

#[cfg(test)]
mod tests;

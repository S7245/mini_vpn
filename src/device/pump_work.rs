//! Original pump source and borrowed-read destruction boundaries. Owns no task,
//! queue, budget, deadline or settlement authority. Poll failure is caught before
//! the async body can unwind its source; each borrowed future is destroyed once
//! in place before the source. No allocation per read. Tests: tests/pump_panics.rs.
use super::*;
use std::mem::ManuallyDrop;

pub(super) struct Source<S> {
    inner: ManuallyDrop<S>,
    control: Arc<PumpControl>,
}
impl<S> Source<S> {
    pub(super) fn new(source: S, control: Arc<PumpControl>) -> Self {
        Self {
            inner: ManuallyDrop::new(source),
            control,
        }
    }
}
impl<S: IngressPacketSource> Source<S> {
    pub(super) fn read_packet<'a>(
        &'a mut self,
        packet: &'a mut PumpPacket,
    ) -> Read<'a, impl Future<Output = std::io::Result<SourceRead>> + Send + 'a> {
        let source = &mut *self.inner;
        let inner = match catch_unwind(AssertUnwindSafe(move || {
            let source = source;
            let packet = packet;
            source.read_packet(packet)
        })) {
            Ok(future) => Some(ManuallyDrop::new(future)),
            Err(payload) => {
                self.control
                    .capture(pump_control::Site::ReadCreate, payload);
                None
            }
        };
        Read {
            inner,
            control: &self.control,
            finished: false,
        }
    }
}
impl<S> Drop for Source<S> {
    fn drop(&mut self) {
        // The source is never separately pinned and all its borrowed reads have
        // already been destroyed. ManuallyDrop prevents a second automatic Drop.
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut self.inner);
        })) {
            self.control
                .capture(pump_control::Site::SourceDrop, payload);
        }
    }
}

pub(super) struct Read<'a, F> {
    inner: Option<ManuallyDrop<F>>,
    control: &'a PumpControl,
    finished: bool,
}
impl<F: Future> Future for Read<'_, F> {
    type Output = Option<F::Output>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // The inner future stays at this pinned address until controlled Drop.
        let this = unsafe { self.get_unchecked_mut() };
        if this.finished {
            return Poll::Pending;
        }
        let Some(inner) = this.inner.as_mut() else {
            this.finished = true;
            return Poll::Ready(None);
        };
        let result = catch_unwind(AssertUnwindSafe(|| unsafe {
            Pin::new_unchecked(&mut **inner).poll(cx)
        }));
        match result {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => {
                this.finished = true;
                Poll::Ready(Some(value))
            }
            Err(payload) => {
                this.finished = true;
                this.control.capture(pump_control::Site::ReadPoll, payload);
                Poll::Ready(None)
            }
        }
    }
}
impl<F> Drop for Read<'_, F> {
    fn drop(&mut self) {
        // In-place Drop preserves !Unpin; catch before source destruction, also
        // when select cancels a Pending read or the original task is aborted.
        let Some(inner) = self.inner.as_mut() else {
            return;
        };
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(inner);
        })) {
            self.control
                .capture(pump_control::Site::ReadFutureDrop, payload);
        }
    }
}

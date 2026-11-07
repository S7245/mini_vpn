//! Native representation of the original ingress actor: one cold SPSC lane,
//! one producer packet, and explicit Reading/Reserving/Paused states. Source and
//! packet remain fields during poll unwind; PumpGuard records first before Drop.
//! No new JOIN, deadline, refund or session authority. Tests: tests/pump_native.rs.
use super::*;
use quinn::owned_events as lane;
use std::mem::ManuallyDrop;
use tokio::sync::futures::OwnedNotified;
mod start;
pub(crate) use start::Startup;

pub(crate) trait Source: Send + 'static {
    fn poll_packet(
        self: Pin<&mut Self>,
        packet: &mut PumpPacket,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<SourceRead>>;
    fn cancel_read(self: Pin<&mut Self>) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) struct Prepared {
    sender: lane::Sender<PumpPacket>,
    receiver: lane::Receiver<PumpPacket>,
    control: Arc<PumpControl>,
    capacity: usize,
}
impl Prepared {
    /// Cold bytes `with_ingress(capacity)` reserves: the SPSC lane slots and
    /// the native `PumpControl`.
    pub(super) fn cold_bytes(capacity: usize) -> Option<usize> {
        let lane = lane::slot_layout::<PumpPacket>(capacity).ok()?.size();
        lane.checked_add(PumpControl::native_layout_bytes())
    }
    pub(crate) fn new(capacity: usize, meter: &bytes::BackingMeter) -> std::io::Result<Self> {
        Self::with_ingress(capacity, meter, None)
    }
    pub(crate) fn with_ingress(capacity: usize, meter: &bytes::BackingMeter, ingress: Option<rx_packet::Access>) -> std::io::Result<Self> {
        if capacity == 0 || meter.budget_limit().is_none() {
            return Err(ErrorKind::InvalidInput.into());
        }
        let (sender, receiver) = lane::channel(capacity, meter)?;
        let mut control = PumpControl::native(meter)?;
        control.native_ingress = ingress;
        control.native_lane = Some(sender.channel_identity());
        receiver.validate_ordinary(
            meter,
            capacity,
            control.native_lane.as_ref().ok_or_else(device_closed)?,
            true,
        )?;
        Ok(Self {
            sender,
            receiver,
            control: Arc::new(control),
            capacity,
        })
    }
    pub(crate) fn spawn<S: Source>(self, source: S) -> TunIngressPump {
        self.spawn_on(source, &tokio::runtime::Handle::current())
    }
    fn spawn_on<S: Source>(self, source: S, runtime: &tokio::runtime::Handle) -> TunIngressPump {
        let control = self.control;
        let inner = Actor {
            source: ManuallyDrop::new(source),
            packet: None,
            sender: self.sender,
            stop: ManuallyDrop::new(control.stop_wake.clone().notified_owned()),
            pause: ManuallyDrop::new(control.pause.changed.clone().notified_owned()),
            control: control.clone(),
            phase: Phase::Reading,
            full_recorded: false,
            capacity: self.capacity,
        };
        let task = runtime.spawn(PumpGuard {
            inner: Some(Box::pin(inner)),
            control: control.clone(),
            settled: false,
        });
        TunIngressPump {
            receiver: Receiver {
                inner: Some(Kind::Native(self.receiver)),
                control: control.clone(),
            },
            selected: None,
            task: PumpTask::Running(task),
            control,
            capacity_packets: self.capacity,
            error_delivered: false,
            cut_supported: true,
        }
    }
    pub(crate) fn spawn_reader<R: AsyncRead + Unpin + Send + 'static>(
        self,
        reader: R,
        mtu: usize,
    ) -> TunIngressPump {
        self.spawn(AsyncReadPacketSource { reader, mtu })
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> Source for AsyncReadPacketSource<R> {
    fn poll_packet(
        self: Pin<&mut Self>,
        packet: &mut PumpPacket,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<SourceRead>> {
        let this = self.get_mut();
        let Some(buffer) = packet.prepare_native(rx_buffer_capacity_for_mtu(this.mtu))? else { return Poll::Pending };
        let mut read = tokio::io::ReadBuf::new(buffer);
        std::task::ready!(Pin::new(&mut this.reader).poll_read(cx, &mut read))?;
        let len = read.filled().len();
        packet.finish_native(len)?;
        Poll::Ready(Ok(if len == 0 {
            SourceRead::Eof
        } else {
            SourceRead::Packet
        }))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Reading,
    Reserving,
    Paused,
}
struct Actor<S> {
    source: ManuallyDrop<S>,
    packet: Option<PumpPacket>,
    sender: lane::Sender<PumpPacket>,
    stop: ManuallyDrop<OwnedNotified>,
    pause: ManuallyDrop<OwnedNotified>,
    control: Arc<PumpControl>,
    phase: Phase,
    full_recorded: bool,
    capacity: usize,
}
impl<S: Source> Future for Actor<S> {
    type Output = PumpExit;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<PumpExit> {
        // Source and notification fields are structurally pinned. No field is
        // moved after first poll; resets use Pin::set and Drop runs in place.
        let this = unsafe { self.get_unchecked_mut() };
        let _ = unsafe { Pin::new_unchecked(&mut *this.stop) }.poll(cx);
        let mut pause = unsafe { Pin::new_unchecked(&mut *this.pause) };
        if pause.as_mut().poll(cx).is_ready() {
            pause.set(this.control.pause.changed.clone().notified_owned());
            if pause.poll(cx).is_ready() {
                cx.waker().wake_by_ref();
            }
        }
        for _ in 0..32 {
            if this.control.native_quarantined() || this.control.failed.load(Ordering::Acquire) {
                this.control.record_callback_failure();
                return Poll::Ready(PumpExit::Failed(PumpFailureId));
            }
            if this.control.stop.load(Ordering::Acquire) {
                return Poll::Ready(PumpExit::StopRequested);
            }
            if this.control.pause.requested.load(Ordering::Acquire) {
                if this.phase != Phase::Paused {
                    if let Err(error) =
                        unsafe { Pin::new_unchecked(&mut *this.source) }.cancel_read()
                    {
                        this.control.record_io(error);
                        return Poll::Ready(PumpExit::Failed(PumpFailureId));
                    }
                    if this.control.native_quarantined()
                        || this.control.failed.load(Ordering::Acquire)
                        || this.control.stop.load(Ordering::Acquire)
                    {
                        return Poll::Ready(this.control.exit(PumpExit::StopRequested));
                    }
                    if let Some(access) = &this.control.native_ingress { access.retire_wait(); }
                    if let Some(packet) = this.packet.take() {
                        this.control.pause.discard(packet);
                    }
                    this.phase = Phase::Paused;
                    this.control.pause.paused.store(true, Ordering::Release);
                    this.control.notify(pump_control::PAUSE);
                }
                if this.control.native_quarantined()
                    || this.control.failed.load(Ordering::Acquire)
                    || this.control.stop.load(Ordering::Acquire)
                {
                    return Poll::Ready(this.control.exit(PumpExit::StopRequested));
                }
                return Poll::Pending;
            }
            if this.phase == Phase::Paused {
                this.control.pause.paused.store(false, Ordering::Release);
                this.phase = Phase::Reading;
            }
            if this.phase == Phase::Reading {
                if this.sender.poll_closed(cx).is_ready() {
                    return Poll::Ready(this.control.exit(PumpExit::ConsumerClosed));
                }
                let packet = this
                    .packet
                    .get_or_insert_with(|| PumpPacket::new(this.control.clone()));
                let controlled = this.control.native_ingress.as_ref().map(|a| a.register(cx)).transpose();
                let result = match controlled {
                    Err(error) => Err(error),
                    Ok(waker) => {
                        let mut source_cx = Context::from_waker(waker.as_ref().unwrap_or(cx.waker()));
                        std::task::ready!(unsafe { Pin::new_unchecked(&mut *this.source) }.poll_packet(packet, &mut source_cx))
                    }
                };
                if let Some(access) = &this.control.native_ingress { access.retire_wait(); }
                match result {
                    Ok(SourceRead::Packet) => {}
                    Ok(SourceRead::Eof) => {
                        this.control.record_io(ErrorKind::UnexpectedEof.into());
                        return Poll::Ready(PumpExit::Failed(PumpFailureId));
                    }
                    Err(error) if error.kind() == ErrorKind::Interrupted => {
                        if !this.control.dispose_interrupted(error) {
                            return Poll::Ready(PumpExit::Failed(PumpFailureId));
                        }
                        this.packet.take();
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    Err(error) => {
                        this.control
                            .stats
                            .read_errors
                            .fetch_add(1, Ordering::Relaxed);
                        this.control.record_io(error);
                        return Poll::Ready(PumpExit::Failed(PumpFailureId));
                    }
                }
                this.phase = Phase::Reserving;
                this.full_recorded = false;
                this.control.stats.queue_high_water.fetch_max(
                    (this.sender.ordinary_len() + 1).min(this.capacity) as u64,
                    Ordering::Relaxed,
                );
            }
            let permit = match this.sender.poll_reserve(cx) {
                Poll::Pending => {
                    if !this.full_recorded {
                        this.full_recorded = true;
                        this.control
                            .stats
                            .full_waits
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Err(_)) => {
                    return Poll::Ready(this.control.exit(PumpExit::ConsumerClosed));
                }
                Poll::Ready(Ok(permit)) => permit,
            };
            // Callback effects during reserve may have stopped/quarantined us.
            // No packet is taken until all original publication gates pass.
            if this.control.native_quarantined()
                || this.control.failed.load(Ordering::Acquire)
                || this.control.stop.load(Ordering::Acquire)
            {
                return Poll::Ready(this.control.exit(PumpExit::StopRequested));
            }
            if this.control.pause.requested.load(Ordering::Acquire) {
                drop(permit);
                continue;
            }
            if !this.packet.as_ref().is_some_and(PumpPacket::native_valid) {
                this.control.quarantine_quiet();
                return Poll::Ready(PumpExit::Failed(PumpFailureId));
            }
            let Some(mut packet) = this.packet.take() else {
                return Poll::Ready(PumpExit::Failed(PumpFailureId));
            };
            let len = packet.bytes.len();
            packet.enqueue();
            if let Err(packet) = permit.send(packet) {
                this.packet = Some(packet);
                return Poll::Ready(this.control.exit(PumpExit::ConsumerClosed));
            }
            this.control.stats.packets.fetch_add(1, Ordering::Relaxed);
            this.control
                .stats
                .bytes
                .fetch_add(len as u64, Ordering::Relaxed);
            this.phase = Phase::Reading;
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl<S> Drop for Actor<S> {
    fn drop(&mut self) {
        if let Some(access) = &self.control.native_ingress { access.retire_wait(); }
        if std::thread::panicking() {
            self.control.quarantine_quiet();
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut self.source);
        })) {
            self.control
                .capture(pump_control::Site::SourceDrop, payload);
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut self.stop);
        })) { self.control.capture(pump_control::Site::StopDrop, payload); }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut self.pause);
        })) { self.control.capture(pump_control::Site::PauseDrop, payload); }
    }
}

enum Kind {
    Legacy(mpsc::Receiver<PumpPacket>),
    Native(lane::Receiver<PumpPacket>),
}
pub(super) struct Receiver {
    inner: Option<Kind>,
    control: Arc<PumpControl>,
}
impl Receiver {
    pub(super) fn legacy(receiver: mpsc::Receiver<PumpPacket>, control: Arc<PumpControl>) -> Self {
        Self {
            inner: Some(Kind::Legacy(receiver)),
            control,
        }
    }
    pub(super) fn len(&self) -> usize {
        match &self.inner {
            Some(Kind::Legacy(r)) => r.len(),
            Some(Kind::Native(r)) => r.len(),
            None => 0,
        }
    }
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(super) fn close(&mut self) {
        if self.control.native_quarantined() {
            return;
        }
        let result = catch_unwind(AssertUnwindSafe(|| match &mut self.inner {
            Some(Kind::Legacy(r)) => r.close(),
            Some(Kind::Native(r)) => r.close(),
            None => (),
        }));
        if let Err(payload) = result {
            self.control.capture(pump_control::Site::Receive, payload);
        }
    }
    pub(super) fn try_recv(&mut self) -> Result<PumpPacket, mpsc::error::TryRecvError> {
        use mpsc::error::TryRecvError as Error;
        if self.control.native_quarantined() {
            return Err(Error::Disconnected);
        }
        match catch_unwind(AssertUnwindSafe(|| match &mut self.inner {
            Some(Kind::Legacy(r)) => r.try_recv(),
            Some(Kind::Native(r)) => r.try_recv().map_err(|e| match e {
                lane::TryRecvError::Empty => Error::Empty,
                lane::TryRecvError::Closed => Error::Disconnected,
            }),
            None => Err(Error::Disconnected),
        })) {
            Ok(result) => result,
            Err(payload) => {
                self.control.capture(pump_control::Site::Receive, payload);
                Err(Error::Disconnected)
            }
        }
    }
    pub(super) async fn recv(&mut self) -> Option<PumpPacket> {
        std::future::poll_fn(|cx| {
            if self.control.native_quarantined() {
                return Poll::Ready(None);
            }
            match catch_unwind(AssertUnwindSafe(|| match &mut self.inner {
                Some(Kind::Legacy(r)) => r.poll_recv(cx),
                Some(Kind::Native(r)) => r.poll_recv(cx),
                None => Poll::Ready(None),
            })) {
                Ok(result) => result,
                Err(payload) => {
                    self.control.capture(pump_control::Site::Receive, payload);
                    Poll::Ready(None)
                }
            }
        })
        .await
    }
    pub(super) fn drain(&mut self) {
        match &mut self.inner {
            Some(Kind::Legacy(r)) => while r.try_recv().is_ok() {},
            Some(Kind::Native(r)) => {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| r.close_and_drain(drop))) {
                    self.control
                        .capture(pump_control::Site::ReceiverDrain, payload);
                }
                self.dispose();
            }
            None => (),
        }
    }
    fn dispose(&mut self) {
        if let Some(inner) = self.inner.take() {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(inner))) {
                self.control
                    .capture(pump_control::Site::ReceiverDrop, payload);
            }
        }
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.dispose();
    }
}

impl PumpPacket {
    pub(crate) fn quarantine_native(&self) {
        self.control.quarantine_quiet();
    }
    pub(crate) fn finish_native(&mut self, length: usize) -> std::io::Result<()> {
        if !self.native_valid() || length > self.bytes.len() {
            self.control.quarantine_quiet();
            return Err(device_closed());
        }
        self.bytes.truncate(length);
        Ok(())
    }
    pub(crate) fn prepare_native(&mut self, length: usize) -> std::io::Result<Option<&mut [u8]>> {
        if length == 0 || length > u16::MAX as usize + 4 {
            return Err(ErrorKind::InvalidInput.into());
        }
        let meter = self
            .control
            .native_meter
            .as_ref()
            .ok_or_else(device_closed)?;
        meter.admit_precharged().map_err(lane::AdmissionError)?;
        if self.adopted {
            if !self.native_valid() || self.bytes.len() != length {
                self.control.quarantine_quiet();
                return Err(device_closed());
            }
        } else {
            let bytes = match &self.control.native_ingress {
                Some(access) => match access.acquire(length)? { Some(bytes) => bytes, None => return Ok(None) },
                None => BytesMut::try_zeroed_metered(length, meter).map_err(lane::AdmissionError)?.into(),
            };
            self.adopt(bytes);
        }
        Ok(Some(&mut self.bytes))
    }
    pub(super) fn native_valid(&self) -> bool {
        self.adopted
            && self.control.native_meter.as_ref().is_some_and(|m| {
                !m.is_quarantined()
                    && self.bytes.precharged_backing_matches(m)
                    && self.backing == self.bytes.capacity() as u64
            })
    }
}

#[cfg(test)]
mod tests;

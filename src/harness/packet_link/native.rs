//! Whole-packet Native peer IO at the original PacketLink authority. Queue space
//! is checked before copying; publication is final under the queue lock. Device
//! retains selected backing until exact write completion. Waiters and callbacks
//! are outside that lock. No Device budget, ACK or stop authority is created.
use super::*;
use bytes::BackingMeter;
use std::{future::Future, mem::ManuallyDrop, pin::Pin};
use tokio::{io::AsyncWrite, sync::futures::OwnedNotified};

#[derive(Default)]
pub(in crate::harness) struct Report {
    pub(in crate::harness) tcp_packets: u64,
    pub(in crate::harness) payload: bool,
    #[cfg(test)]
    pub(in crate::harness) partial: Option<(
        Arc<crate::harness::loopback::PartialFlushProbe>,
        TunFlushedTcpPacket,
        usize,
    )>,
}
struct Unwind(BackingMeter);
impl Drop for Unwind {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.quarantine();
        }
    }
}
impl PacketLink {
    pub(in crate::harness) fn read_native(&self, slot: &mut PumpPacket) -> Poll<std::io::Result<bool>> {
        let mut queue = match self.queue.lock() {
            Ok(queue) => queue,
            Err(_) => {
                slot.quarantine_native();
                return Poll::Ready(Err(device_closed()));
            }
        };
        let Some(packet) = queue.front() else {
            return Poll::Ready(Ok(false));
        };
        let length = packet.len();
        let Some(buffer) = slot.prepare_native(length.max(1))? else { return Poll::Pending };
        buffer[..length].copy_from_slice(packet);
        slot.finish_native(length)?;
        let original = queue.pop_front();
        drop(queue);
        drop(original);
        self.space.notify_waiters();
        Poll::Ready(Ok(true))
    }
    pub(in crate::harness) fn discard(
        &self,
        totals: &mut (u64, u64),
        meter: Option<&BackingMeter>,
    ) {
        let _unwind = meter.cloned().map(Unwind);
        let mut queue = self.queue.lock().unwrap_or_else(|poison| {
            if let Some(meter) = meter {
                meter.quarantine();
            }
            poison.into_inner()
        });
        crate::device::cut::discard_packets(queue.drain(..), totals);
        drop(queue);
        self.space.notify_waiters();
    }
}
struct Writer<'a> {
    link: &'a PacketLink,
    meter: BackingMeter,
    custody: crate::device::egress::CallbackCustody,
    wait: ManuallyDrop<OwnedNotified>,
    wait_live: bool,
    retired: bool,
    report: &'a mut Report,
    #[cfg(test)]
    fail_next: bool,
}
impl Writer<'_> {
    // Mark dead before running opaque waiter Drop: a panic must never repeat it.
    fn retire_wait(&mut self) {
        if self.retired {
            return;
        }
        self.retired = true;
        if self.wait_live {
            self.wait_live = false;
            unsafe { ManuallyDrop::drop(&mut self.wait) };
        }
        self.custody.retire();
    }
}
impl Drop for Writer<'_> {
    fn drop(&mut self) {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.retire_wait()));
        if let Err(payload) = result {
            if std::thread::panicking() {
                self.custody.retain_secondary(payload);
            } else {
                self.meter.quarantine();
                std::panic::resume_unwind(payload);
            }
        }
    }
}
impl AsyncWrite for Writer<'_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = unsafe { self.get_unchecked_mut() };
        #[cfg(test)]
        if this.fail_next {
            return Poll::Ready(Err(std::io::Error::from_raw_os_error(5)));
        }
        this.custody.admit()?;
        if bytes.is_empty() {
            return Poll::Ready(Err(std::io::ErrorKind::InvalidInput.into()));
        }
        loop {
            let controlled = this.custody.register(cx.waker())?;
            let mut controlled_cx = Context::from_waker(&controlled);
            // Register before checking actual capacity. The waiter lives at this
            // pinned address across Pending; no clone/drop/wake under queue lock.
            let notified = unsafe { Pin::new_unchecked(&mut *this.wait) }
                .poll(&mut controlled_cx)
                .is_ready();
            this.custody.admit()?;
            let mut queue = match this.link.queue.lock() {
                Ok(queue) => queue,
                Err(_) => {
                    this.meter.quarantine();
                    return Poll::Ready(Err(device_closed()));
                }
            };
            if this
                .link
                .capacity_packets
                .is_some_and(|limit| queue.len() < limit)
            {
                this.custody.admit()?;
                let copy = BytesMut::from(bytes); // External peer's copy, like kernel TUN storage.
                queue.push_back(copy);
                #[cfg(test)]
                let published_index = queue.len() - 1;
                this.link
                    .high_water_packets
                    .fetch_max(queue.len() as u64, Ordering::Relaxed);
                drop(queue);
                if let Some(summary) = summarize_flushed_ip_tcp_packet(bytes) {
                    this.report.tcp_packets = this.report.tcp_packets.saturating_add(1);
                    this.report.payload |= summary.payload_bytes > 0;
                    #[cfg(test)]
                    if let Some((probe, expected, remaining)) = &this.report.partial {
                        if *expected == summary {
                            probe.native_handoff(summary, *remaining, published_index);
                            this.fail_next = true;
                        }
                    }
                }
                this.link.notify.notify_one();
                return Poll::Ready(Ok(bytes.len()));
            }
            drop(queue);
            if this.link.capacity_packets.is_none() {
                return Poll::Ready(Err(std::io::ErrorKind::Unsupported.into()));
            }
            if !notified {
                return Poll::Pending;
            }
            this.wait_live = false;
            unsafe { ManuallyDrop::drop(&mut this.wait) };
            this.wait = ManuallyDrop::new(this.link.space.clone().notified_owned());
            this.wait_live = true;
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
pub(in crate::harness) async fn flush(
    egress: &mut crate::device::egress::Egress,
    link: &PacketLink,
    report: &mut Report,
) -> std::io::Result<()> {
    let meter = egress.native_meter().ok_or_else(device_closed)?.clone();
    let _unwind = Unwind(meter.clone());
    let custody = egress.callback_custody().ok_or_else(device_closed)?;
    custody.admit()?;
    {
        let writer = Writer {
            link,
            meter: meter.clone(),
            custody: custody.clone(),
            wait: ManuallyDrop::new(link.space.clone().notified_owned()),
            wait_live: true,
            retired: false,
            report,
            #[cfg(test)]
            fail_next: false,
        };
        tokio::pin!(writer);
        let result = egress.flush(&mut writer).await;
        custody.note_write(result);
        // Keep original result alive before retiring any original caller Waker.
        let cleanup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            unsafe { writer.as_mut().get_unchecked_mut() }.retire_wait();
        }));
        if let Err(payload) = cleanup {
            custody.retain_secondary(payload);
        }
    };
    custody.finish()
}

#[cfg(test)]
mod tests;

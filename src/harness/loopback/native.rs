//! Native read capability for the original external PacketLink peer. The peer
//! queue remains the harness endpoint; Device owns only its preadmitted copy.
//! No packet is removed before backing admission and copy commit. No session,
//! budget or JOIN authority here. Tests: native/tests.rs.
use super::*;
use crate::device::pump_native::Source;
use std::{future::Future, pin::Pin};
use tokio::sync::futures::OwnedNotified;

pub(crate) struct PacketSource {
    link: PacketLink,
    wait: OwnedNotified,
}
impl PacketSource {
    pub(crate) fn new(link: PacketLink) -> Self {
        let wait = link.notify.clone().notified_owned();
        Self { link, wait }
    }
}
impl Source for PacketSource {
    fn poll_packet(
        self: Pin<&mut Self>,
        slot: &mut PumpPacket,
        cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<SourceRead>> {
        // Only wait is structurally pinned; the link and its original queue stay
        // in this actor field. No per-packet future or heap allocation is added.
        let this = unsafe { self.get_unchecked_mut() };
        for _ in 0..2 {
            if std::task::ready!(this.link.read_native(slot))? {
                return Poll::Ready(Ok(SourceRead::Packet));
            }
            let mut wait = unsafe { Pin::new_unchecked(&mut this.wait) };
            if wait.as_mut().poll(cx).is_pending() {
                return Poll::Pending;
            }
            wait.set(this.link.notify.clone().notified_owned());
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn cancel_read(self: Pin<&mut Self>) -> std::io::Result<()> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.wait) }
            .set(this.link.notify.clone().notified_owned());
        Ok(())
    }
}

impl LoopbackTunDevice {
    /// Prepare every Native Device owner before creating the external peer.
    /// The returned Device is frozen until its original Service bind commits.
    pub fn native_with_factory(
        slots: usize,
        mtu: usize,
        meter: &bytes::BackingMeter,
        source: impl FnOnce() -> std::io::Result<(PacketLink, PacketLink)>,
    ) -> std::io::Result<Self> {
        let prepared = crate::device::cold::PreparedDevice::new(slots, mtu, 0, true, meter)?;
        let (inbound, outbound) = source()?;
        if inbound.capacity_packets != Some(slots) || outbound.capacity_packets != Some(slots) {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let source = PacketSource::new(inbound.clone());
        let parts = prepared.into_parts();
        let pump = parts.pump.ok_or(std::io::ErrorKind::InvalidInput)?;
        let mut device = Self::new(inbound, outbound);
        device.rx = parts.rx;
        device.egress = parts.egress;
        device.mtu = mtu;
        device.ingress_pump = crate::device::pump_native::Startup::new(pump, source, parts.runtime);
        Ok(device)
    }

    pub(super) fn bind_native(
        &mut self,
        binding: crate::device::DeviceBinding<'_>,
    ) -> std::io::Result<()> {
        use crate::device::DeviceBinding;
        match binding {
            DeviceBinding::Legacy => {
                if self.rx.is_native()
                    || self.egress.native_meter().is_some()
                    || self.ingress_pump.is_native()
                {
                    return Err(std::io::ErrorKind::Unsupported.into());
                }
                Ok(())
            }
            DeviceBinding::Native { meter, slots, mtu } => {
                crate::device::cold::validate_request(slots, mtu, 0, meter)?;
                if self.stopping || self.cut.active() {
                    return Err(self.unavailable());
                }
                if self.mtu != mtu
                    || self.inbound.as_ref().and_then(|l| l.capacity_packets) != Some(slots)
                    || self.outbound.as_ref().and_then(|l| l.capacity_packets) != Some(slots)
                {
                    return Err(std::io::ErrorKind::InvalidInput.into());
                }
                self.rx.bind(meter, slots)?;
                self.egress.bind(meter, slots, mtu, 0)?;
                self.ingress_pump.validate(meter, slots)?;
                // No fallible admission or generic Q recheck after this spawn.
                self.ingress_pump.commit()
            }
        }
    }
}

#[cfg(test)]
mod tests;

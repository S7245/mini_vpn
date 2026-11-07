//! Device-owned current packet, direct-read preparation and classified FIFO.
//! Native fixes the real descriptor allocation before IO, verifies actual Shared
//! backing, and never treats an empty slot as escaped-payload release. No pump,
//! session or ACK authority. Entries: prepare/finish/install/stage/take/discard.
//! Deterministic ownership tracers live in tests/mod.rs.
use super::*;
use bytes::{BackingBudgetReservation, BackingMeter};
use std::alloc::Layout;

struct Cold {
    meter: BackingMeter,
    slots: usize,
    layout: BackingBudgetReservation,
}
#[derive(Default)]
pub(crate) struct RxStock {
    current: Option<RxPacket>,
    staged: VecDeque<RxPacket>,
    prepared: Option<RxPacket>,
    protected: Option<rx_packet::Stock>,
    cold: Option<Cold>, // Actual buffers/descriptor storage drop before allowance.
}
impl RxStock {
    /// Cold descriptor bytes `native(slots)` reserves.
    pub(super) fn layout_bytes(slots: usize) -> Option<usize> {
        Layout::array::<RxPacket>(slots).ok().map(|layout| layout.size())
    }
    pub(crate) fn is_native(&self) -> bool {
        self.cold.is_some()
    }
    pub(crate) fn bind(&self, meter: &BackingMeter, slots: usize) -> std::io::Result<()> {
        let cold = self
            .cold
            .as_ref()
            .ok_or_else(|| std::io::Error::from(ErrorKind::Unsupported))?;
        self.admit()?;
        if self
            .staged
            .iter()
            .any(|p| !p.precharged_backing_matches(&cold.meter))
        {
            self.quarantine();
            return Err(device_closed());
        }
        if !cold.meter.same_authority(meter) || cold.slots != slots {
            return Err(ErrorKind::InvalidInput.into());
        }
        if let Some(stock) = &self.protected { stock.bind(meter)?; }
        Ok(())
    }
    pub(crate) fn native(slots: usize, meter: &BackingMeter) -> std::io::Result<Self> {
        if slots == 0 {
            return Err(ErrorKind::InvalidInput.into());
        }
        let bytes = Self::layout_bytes(slots)
            .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
        let reservation = meter
            .reserve_cold_layout(bytes)
            .map_err(quinn::owned_events::AdmissionError)?
            .ok_or_else(|| std::io::Error::from(ErrorKind::Unsupported))?;
        let mut stock = Self::default();
        stock.cold = Some(Cold {
            meter: meter.clone(),
            slots,
            layout: reservation,
        });
        stock.staged = VecDeque::from(exact_vec(slots)?);
        Ok(stock)
    }
    fn quarantine(&self) {
        if let Some(cold) = &self.cold {
            cold.meter.quarantine();
        }
    }
    pub(crate) fn admit(&self) -> std::io::Result<()> {
        let valid = self.cold.as_ref().is_none_or(|cold| {
            cold.meter.admit_precharged().is_ok()
                && self.protected.as_ref().is_none_or(|stock| stock.admit_layout(&cold.meter))
                && self.staged.capacity() == cold.slots
                && self.staged.len() <= cold.slots
                && !(self.current.is_some() && self.prepared.is_some())
                && self
                    .current
                    .iter()
                    .chain(self.prepared.iter())
                    .all(|packet| packet.precharged_backing_matches(&cold.meter))
                && Layout::array::<RxPacket>(cold.slots)
                    .ok()
                    .is_some_and(|layout| {
                        cold.meter
                            .cold_layout_matches(Some(&cold.layout), layout.size())
                    })
        });
        if valid {
            Ok(())
        } else {
            self.quarantine();
            Err(self.access().and_then(|access| access.failure()).unwrap_or_else(device_closed))
        }
    }
    fn validate(&self, packet: &RxPacket, length: usize, prefix: usize) -> std::io::Result<()> {
        self.admit()?;
        if self.current.is_some() {
            return Err(ErrorKind::WouldBlock.into());
        }
        if self
            .cold
            .as_ref()
            .is_some_and(|c| !packet.precharged_backing_matches(&c.meter))
        {
            self.quarantine();
            return Err(self.access().and_then(|access| access.failure()).unwrap_or_else(device_closed));
        }
        if !matches!(prefix, 0 | 4) || length > packet.len() || length < prefix {
            return Err(ErrorKind::InvalidData.into());
        }
        Ok(())
    }
    fn publish(&mut self, mut packet: RxPacket, length: usize, prefix: usize) {
        packet.truncate(length);
        self.current = Some(if prefix == 0 {
            packet
        } else {
            { packet.strip_prefix(prefix); packet }
        });
    }
    /// Borrow the selected original packet: any refusal leaves it with its caller.
    pub(crate) fn install(
        &mut self,
        packet: &mut Option<RxPacket>,
        prefix: usize,
    ) -> std::io::Result<()> {
        self.admit()?;
        if self.prepared.is_some() {
            return Err(ErrorKind::WouldBlock.into());
        }
        let source = packet
            .as_ref()
            .ok_or_else(|| std::io::Error::from(ErrorKind::InvalidInput))?;
        self.validate(source, source.len(), prefix)?;
        let length = source.len();
        if let Some(packet) = packet.take() {
            self.publish(packet, length, prefix);
        }
        Ok(())
    }
    pub(crate) fn protect(&mut self, callbacks: callback::Custody) -> std::io::Result<()> {
        let cold = self.cold.as_ref().ok_or_else(device_closed)?;
        if self.protected.is_some() || self.current.is_some() || self.prepared.is_some() || !self.staged.is_empty() {
            return Err(ErrorKind::InvalidInput.into());
        }
        self.protected = Some(rx_packet::Stock::new(&cold.meter, callbacks)?);
        Ok(())
    }
    pub(crate) fn access(&self) -> Option<rx_packet::Access> {
        self.protected.as_ref().map(rx_packet::Stock::access)
    }
    pub(crate) fn take_stock(&mut self) -> Option<rx_packet::Stock> { self.protected.take() }
    /// Preparation remains in this physical owner across cancellation of a read.
    pub(crate) fn try_prepare(&mut self, length: usize) -> std::io::Result<Option<&mut [u8]>> {
        self.admit()?;
        if self.current.is_some() { return Err(ErrorKind::WouldBlock.into()); }
        if length == 0 || length > u16::MAX as usize + 4 { return Err(ErrorKind::InvalidInput.into()); }
        if let Some(packet) = &self.prepared {
            if packet.len() != length { self.quarantine(); return Err(ErrorKind::InvalidData.into()); }
        } else {
            self.prepared = match (&self.protected, &self.cold) {
                (Some(stock), _) => stock.access().acquire(length)?,
                (_, Some(cold)) => Some(BytesMut::try_zeroed_metered(length, &cold.meter)
                    .map_err(quinn::owned_events::AdmissionError)?.into()),
                _ => Some(BytesMut::zeroed(length).into()),
            };
        }
        Ok(self.prepared.as_deref_mut())
    }
    pub(crate) fn prepare(&mut self, length: usize) -> std::io::Result<&mut [u8]> {
        self.try_prepare(length)?.ok_or_else(|| ErrorKind::WouldBlock.into())
    }
    pub(crate) async fn read<R: AsyncRead + Unpin>(&mut self, reader: &mut R, length: usize) -> std::io::Result<usize> {
        struct Wait(Option<rx_packet::Access>);
        impl Drop for Wait { fn drop(&mut self) { if let Some(access) = &self.0 { access.retire_wait(); } } }
        let wait = Wait(self.access());
        std::future::poll_fn(|cx| {
            let controlled = match &wait.0 { Some(a) => Some(a.register(cx)?), None => None };
            let cx = &mut Context::from_waker(controlled.as_ref().unwrap_or(cx.waker()));
            let Some(bytes) = self.try_prepare(length)? else { return Poll::Pending };
            let mut buffer = tokio::io::ReadBuf::new(bytes);
            match std::task::ready!(Pin::new(&mut *reader).poll_read(cx, &mut buffer)) {
                Ok(()) => Poll::Ready(Ok(buffer.filled().len())),
                Err(error) => {
                    if let Some(access) = &wait.0 {
                        access.note_io(error); // Original IO first, before waiter retirement.
                        Poll::Ready(Err(access.failure().unwrap_or_else(device_closed)))
                    } else { Poll::Ready(Err(error)) }
                }
            }
        }).await
    }
    pub(crate) fn finish_read(&mut self, length: usize, prefix: usize) -> std::io::Result<()> {
        self.validate(
            self.prepared.as_ref().ok_or_else(device_closed)?,
            length,
            prefix,
        )?;
        if let Some(packet) = self.prepared.take() {
            self.publish(packet, length, prefix);
        }
        Ok(())
    }
    pub(crate) fn has_current(&self) -> bool {
        self.current.is_some()
    }
    pub(crate) fn staged_empty(&self) -> bool {
        self.staged.is_empty()
    }
    pub(crate) fn peek(&self) -> Option<&[u8]> {
        self.admit().ok()?;
        self.current.as_deref()
    }
    pub(crate) fn take(&mut self) -> Option<BytesMut> {
        self.admit().ok()?;
        if self.current.as_ref()?.protected() { return None; }
        self.current.take()?.into_raw().ok()
    }
    pub(crate) fn take_packet(&mut self) -> Option<RxPacket> {
        self.admit().ok()?;
        self.current.take()
    }
    pub(crate) fn take_staged(&mut self) -> Option<RxPacket> {
        self.admit().ok()?;
        if self.cold.as_ref().is_some_and(|cold| {
            self.staged
                .front()
                .is_some_and(|packet| !packet.precharged_backing_matches(&cold.meter))
        }) {
            self.quarantine();
            return None;
        }
        self.staged.pop_front()
    }
    pub(crate) fn stage(&mut self) -> bool {
        if self.admit().is_err()
            || self
                .cold
                .as_ref()
                .is_some_and(|c| self.staged.len() == c.slots)
        {
            return false;
        }
        let Some(packet) = self.current.as_ref() else {
            return false;
        };
        if self
            .cold
            .as_ref()
            .is_some_and(|c| !packet.precharged_backing_matches(&c.meter))
        {
            self.quarantine();
            return false;
        }
        if let Some(packet) = self.current.take() {
            self.staged.push_back(packet);
        }
        true
    }
    /// Known cut/shutdown disposition does not claim anything about escaped Bytes.
    pub(crate) fn discard(&mut self, totals: &mut (u64, u64)) {
        // Cold disposal audits every still-owned packet before destroying evidence.
        // DATA selection only checks the selected front/current/prepared slots.
        if self.admit().is_err()
            || self.cold.as_ref().is_some_and(|cold| {
                self.staged
                    .iter()
                    .any(|packet| !packet.precharged_backing_matches(&cold.meter))
            })
        {
            self.quarantine();
        }
        for packet in self.prepared.take().into_iter().chain(self.current.take()).chain(self.staged.drain(..)) {
            totals.0 = totals.0.saturating_add(1);
            totals.1 = totals.1.saturating_add(packet.capacity() as u64);
            drop(packet);
        }
    }
}
impl Drop for RxStock {
    fn drop(&mut self) {
        if self.current.is_some()
            || self.prepared.is_some()
            || !self.staged.is_empty()
            || std::thread::panicking()
        {
            self.quarantine();
        }
    }
}
#[cfg(test)]
mod tests;

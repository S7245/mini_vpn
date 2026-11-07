//! Original Device ingress protection: one cold allocation, one exclusive typed
//! loan, no additional packet queue or refund authority. A packet lends slices
//! only; its actual Drop returns the complete allocation to this owner. Inventory
//! retirement waits for that return before dropping storage. Callback custody is
//! the RX role of the original Device ledger. Tests: device/tests/rx_pressure.rs.
use super::{callback, device_closed};
use bytes::{AllocationIdentity, BackingBudgetReservation, BackingMeter, Bytes, BytesMut, MeteredCopyError};
use std::{alloc::{Layout, alloc}, io, ops::{Deref, DerefMut}, sync::{Arc, Mutex}, task::{Context, Poll}};
const CAPACITY: usize = u16::MAX as usize + 4;
fn control_bytes() -> usize { Layout::new::<State>().size() + Layout::new::<Core>().size() }
pub(super) fn cold_bytes() -> usize {
    control_bytes() + BytesMut::metered_copy_layout_for_len(CAPACITY).unwrap_or(usize::MAX)
}

struct State {
    idle: Option<BytesMut>,
    loan: bool,
    retired: bool,
    closing: bool,
    uncertain: Option<Bytes>,
    identity: AllocationIdentity,
}
struct Core {
    state: Mutex<Box<State>>,
    callbacks: callback::Custody,
    meter: BackingMeter,
    _layout: BackingBudgetReservation,
}
impl Core {
    fn lock(&self) -> std::sync::MutexGuard<'_, Box<State>> {
        self.state.lock().unwrap_or_else(|e| { self.meter.quarantine(); e.into_inner() })
    }
    fn layout_valid(&self) -> bool {
        self.meter.cold_layout_matches(Some(&self._layout), control_bytes())
            && self.callbacks.rx_authority(&self.meter)
    }
    fn recover_idle(&self, state: &mut State) -> bool {
        if state.loan || state.uncertain.is_some() || !self.layout_valid()
            || !state.idle.as_ref().is_some_and(|b| self.valid(b, state.identity)) { return false; }
        let Some(bytes) = state.idle.take() else { return false };
        let bytes = bytes.freeze();
        if bytes.allocation_identity() != Some(state.identity) {
            state.uncertain = Some(bytes); return false;
        }
        match bytes.try_into_mut() {
            Ok(bytes) => { state.idle = Some(bytes); true }
            Err(bytes) => { state.uncertain = Some(bytes); false }
        }
    }
    fn valid(&self, bytes: &BytesMut, identity: AllocationIdentity) -> bool {
        bytes.as_ptr() as usize == identity.base && bytes.capacity() == identity.capacity
            && bytes.len() == CAPACITY && bytes.precharged_backing_matches(&self.meter)
    }
}
#[derive(Clone)]
pub(crate) struct Access(Arc<Core>);
pub(crate) struct Stock(Access);
/// An ingress packet is exclusive and never exposes a clonable backing handle.
/// Legacy raw take is available only for an ordinary dynamically funded packet.
#[derive(Default)]
pub struct RxPacket {
    bytes: BytesMut,
    start: usize,
    end: usize,
    loan: Option<Access>,
}
impl From<BytesMut> for RxPacket {
    fn from(bytes: BytesMut) -> Self {
        let end = bytes.len();
        Self { bytes, start: 0, end, loan: None }
    }
}
impl Deref for RxPacket {
    type Target = [u8];
    fn deref(&self) -> &[u8] { &self.bytes[self.start..self.end] }
}
impl DerefMut for RxPacket {
    fn deref_mut(&mut self) -> &mut [u8] { &mut self.bytes[self.start..self.end] }
}
impl std::fmt::Debug for RxPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.deref().fmt(f) }
}
impl PartialEq<BytesMut> for RxPacket {
    fn eq(&self, other: &BytesMut) -> bool { self.deref() == other.as_ref() }
}
impl PartialEq<RxPacket> for BytesMut {
    fn eq(&self, other: &RxPacket) -> bool { self.as_ref() == other.deref() }
}
impl RxPacket {
    pub(crate) fn capacity(&self) -> usize { self.bytes.capacity() }
    pub(crate) fn protected(&self) -> bool { self.loan.is_some() }
    pub(crate) fn into_raw(mut self) -> Result<BytesMut, Self> {
        if self.protected() { return Err(self); }
        self.bytes.truncate(self.end);
        let bytes = if self.start == 0 { std::mem::take(&mut self.bytes) }
            else { self.bytes.split_off(self.start) };
        Ok(bytes)
    }
    pub(crate) fn truncate(&mut self, length: usize) { self.end = self.start + length.min(self.len()); }
    pub(crate) fn strip_prefix(&mut self, prefix: usize) { self.start += prefix; }
    pub(crate) fn precharged_backing_matches(&self, meter: &BackingMeter) -> bool {
        self.start <= self.end && self.end <= self.bytes.len()
            && self.bytes.precharged_backing_matches(meter)
            && self.loan.as_ref().is_none_or(|a| {
                let state = a.0.lock();
                a.0.meter.same_authority(meter) && state.loan && !state.retired
                    && a.0.valid(&self.bytes, state.identity)
            })
    }
}
impl Drop for RxPacket {
    fn drop(&mut self) {
        let Some(access) = self.loan.take() else { return };
        if std::thread::panicking() { access.0.meter.quarantine(); }
        {
            let mut state = access.0.lock();
            if !state.loan || state.retired || state.idle.is_some()
                || !access.0.valid(&self.bytes, state.identity) {
                access.0.meter.quarantine();
                // Preserve the original allocation on an impossible ownership state.
                std::mem::forget(std::mem::take(&mut self.bytes));
                return;
            }
            state.idle = Some(std::mem::take(&mut self.bytes));
            state.loan = false;
            if !access.0.recover_idle(&mut state) { access.0.meter.quarantine(); }
        }
        access.0.callbacks.notify();
    }
}
impl Stock {
    pub(crate) fn new(meter: &BackingMeter, callbacks: callback::Custody) -> io::Result<Self> {
        if !callbacks.matches_rx(meter) { return Err(io::ErrorKind::InvalidInput.into()); }
        let layout = Layout::new::<State>();
        let allowance = meter.reserve_cold_layout(control_bytes())
            .map_err(quinn::owned_events::AdmissionError)?.ok_or(io::ErrorKind::Unsupported)?;
        let bytes = BytesMut::try_zeroed_metered(CAPACITY, meter)
            .map_err(quinn::owned_events::AdmissionError)?.freeze();
        let identity = bytes.allocation_identity().ok_or_else(device_closed)?;
        let bytes = bytes.try_into_mut().map_err(|_| device_closed())?;
        let pointer = unsafe { alloc(layout).cast::<State>() };
        if pointer.is_null() { return Err(io::ErrorKind::OutOfMemory.into()); }
        unsafe { pointer.write(State { idle: Some(bytes), loan: false, retired: false, closing: false, uncertain: None, identity }); }
        Ok(Self(Access(Arc::new(Core {
            state: Mutex::new(unsafe { Box::from_raw(pointer) }), callbacks,
            meter: meter.clone(), _layout: allowance,
        }))))
    }
    pub(crate) fn access(&self) -> Access { self.0.clone() }
    pub(crate) fn begin_retirement(&self) {
        self.0.0.lock().closing = true;
        self.0.0.callbacks.begin_retirement();
    }
    pub(crate) fn admit_layout(&self, meter: &BackingMeter) -> bool {
        self.0.0.meter.same_authority(meter) && self.0.0.layout_valid()
    }
    pub(crate) fn bind(&self, meter: &BackingMeter) -> io::Result<()> {
        if !self.0.0.meter.same_authority(meter) { return Err(io::ErrorKind::InvalidInput.into()); }
        let mut state = self.0.0.lock();
        let valid = !state.retired && !state.closing && state.uncertain.is_none()
            && self.0.0.layout_valid()
            && if state.loan { state.idle.is_none() } else { self.0.0.recover_idle(&mut state) };
        if !valid { self.0.0.meter.quarantine(); return Err(device_closed()); }
        Ok(())
    }
    pub(crate) fn poll_retire(&self, cx: &mut Context<'_>) -> Poll<()> {
        let loan = self.0.0.lock().loan;
        if loan {
            // Registration failure under Q does not manufacture a release. The
            // existing owner's absolute deadline still bounds this Pending.
            let _ = self.0.0.callbacks.register_retirement(cx.waker());
        }
        let packet = {
            let mut state = self.0.0.lock();
            if state.retired { return Poll::Ready(()); }
            if state.loan { return Poll::Pending; }
            if !self.0.0.recover_idle(&mut state) {
                self.0.0.meter.quarantine();
                return Poll::Pending;
            }
            state.retired = true;
            state.idle.take()
        };
        drop(packet);
        self.0.retire_wait();
        Poll::Ready(())
    }

}
impl Drop for Stock {
    fn drop(&mut self) {
        self.0.retire_wait();
        let mut state = self.0.0.lock();
        state.closing = true;
        let valid = self.0.0.layout_valid() && if state.retired {
            !state.loan && state.idle.is_none() && state.uncertain.is_none()
        } else { self.0.0.recover_idle(&mut state) };
        if !valid || std::thread::panicking() {
            self.0.0.meter.quarantine();
            // Keep the original physical record, allowance and allocation. An
            // uncertain owner loss is bounded custody, never a clean retirement.
            std::mem::forget(self.0.clone());
        } else if !state.retired {
            state.retired = true;
            let packet = state.idle.take();
            drop(state);
            drop(packet);
        }
    }
}
impl Access {
    pub(crate) fn claim(&self) -> callback::Report {
        self.0.callbacks.claim(super::PumpControl::origin(crate::shared::errors::FailurePhase::DeviceRead))
    }
    pub(crate) fn note_io(&self, error: io::Error) { self.0.callbacks.note_write(Err(error)); }
    pub(crate) fn failure(&self) -> Option<io::Error> {
        self.0.callbacks.finish().err()
    }

    pub(crate) fn register(&self, cx: &Context<'_>) -> io::Result<std::task::Waker> {
        self.0.callbacks.register(cx.waker())
    }
    pub(crate) fn retire_wait(&self) { self.0.callbacks.retire(); }
    pub(crate) fn acquire(&self, length: usize) -> io::Result<Option<RxPacket>> {
        if length == 0 || length > CAPACITY { return Err(io::ErrorKind::InvalidInput.into()); }
        self.0.callbacks.admit()?;
        // Owner retirement and both admission branches share this commit lock.
        let mut state = self.0.lock();
        if state.retired || state.closing { return Err(device_closed()); }
        if !self.0.layout_valid() || state.uncertain.is_some()
            || state.loan == state.idle.is_some()
            || state.idle.as_ref().is_some_and(|b| !self.0.valid(b, state.identity)) {
            self.0.meter.quarantine(); return Err(device_closed());
        }
        match BytesMut::try_zeroed_metered(length, &self.0.meter) {
            Ok(bytes) => return Ok(Some(bytes.into())),
            Err(MeteredCopyError::BudgetFull) => (),
            Err(error) => return Err(quinn::owned_events::AdmissionError(error).into()),
        }
        if state.loan { return Ok(None); }
        if !self.0.recover_idle(&mut state) {
            self.0.meter.quarantine(); return Err(device_closed());
        }
        let mut bytes = state.idle.take().ok_or_else(device_closed)?;
        bytes.fill(0);
        state.loan = true;
        Ok(Some(RxPacket { bytes, start: 0, end: length, loan: Some(self.clone()) }))
    }
}

#[cfg(test)]
mod tests;

//! A single precharged response allocation, borrowed only for synchronous receive.
//! It owns physical scratch, not pacing service or UDP completion. Native callers
//! cannot supply an uncharged Vec; rejection precedes RNG, signer and reset clock.
//! Entry: ResponseScratch / ResponseBuffer; tests: endpoint/response/tests.
use super::*;
use bytes::{BackingBudgetReservation, BackingMeter};
use std::{
    alloc::{alloc, Layout},
    io,
};

/// Prepaid scratch for one serial Native endpoint response.
#[doc(hidden)]
pub struct ResponseScratch {
    buffer: Vec<u8>,
    _reservation: BackingBudgetReservation,
    meter: BackingMeter,
    max_datagram: usize,
}
impl ResponseScratch {
    /// Exact backing bound for Native reset responses to a configured receive slot.
    pub fn layout_bytes(max_datagram: usize) -> io::Result<usize> {
        if max_datagram < 43 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "response receive bound",
            ));
        }
        let capacity = max_datagram - 2;
        Layout::array::<u8>(capacity)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "response layout"))?;
        Ok(capacity)
    }
    /// Allocate before Endpoint driver publication, using the original hard authority.
    pub fn prepare(max_datagram: usize, meter: &BackingMeter) -> io::Result<Self> {
        let capacity = Self::layout_bytes(max_datagram)?;
        let reservation = meter
            .reserve_cold_layout(capacity)
            .map_err(|error| io::Error::other(format!("response admission {error:?}")))?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "response requires hard authority",
                )
            })?;
        let layout = Layout::array::<u8>(capacity)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "response layout"))?;
        let raw = unsafe { alloc(layout) };
        if raw.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "response allocation",
            ));
        }
        // SAFETY: exact u8 capacity, zero initialized length; Vec owns its one allocation.
        let buffer = unsafe { Vec::from_raw_parts(raw, 0, capacity) };
        Ok(Self {
            buffer,
            _reservation: reservation,
            meter: meter.clone(),
            max_datagram,
        })
    }
    /// Bytes of the last response; consume synchronously before the next receive.
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer
    }
}
impl Drop for ResponseScratch {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.meter.quarantine();
        }
    }
}

/// Receive response authority. Legacy callers retain their original Vec API.
#[doc(hidden)]
pub enum ResponseBuffer<'a> {
    Legacy(&'a mut Vec<u8>),
    Native(&'a mut ResponseScratch),
}
impl<'a> From<&'a mut Vec<u8>> for ResponseBuffer<'a> {
    fn from(buffer: &'a mut Vec<u8>) -> Self {
        Self::Legacy(buffer)
    }
}
impl<'a> From<&'a mut ResponseScratch> for ResponseBuffer<'a> {
    fn from(buffer: &'a mut ResponseScratch) -> Self {
        Self::Native(buffer)
    }
}
impl ResponseBuffer<'_> {
    pub(super) fn validate(&self, endpoint: &Endpoint, datagram: usize) -> bool {
        match self {
            Self::Legacy(_) => true,
            Self::Native(scratch) => {
                endpoint.registrations.native()
                    && endpoint
                        .config
                        .receive_backing_meter
                        .as_ref()
                        .is_some_and(|meter| meter.same_authority(&scratch.meter))
                    && !scratch.meter.is_quarantined()
                    && datagram <= scratch.max_datagram
            }
        }
    }
    pub(super) fn legacy(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            Self::Legacy(buffer) => Some(buffer),
            Self::Native(_) => None,
        }
    }
    pub(super) fn reset(
        &mut self,
        endpoint: &Endpoint,
        datagram: usize,
        max_output: usize,
    ) -> Option<&mut Vec<u8>> {
        if !self.validate(endpoint, datagram) {
            return None;
        }
        match self {
            Self::Legacy(buffer) if !endpoint.registrations.native() => Some(buffer),
            Self::Native(scratch) if max_output <= scratch.buffer.capacity() => {
                scratch.buffer.clear();
                Some(&mut scratch.buffer)
            }
            _ => None,
        }
    }
}
impl Drop for ResponseBuffer<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Self::Native(scratch) = self {
                scratch.meter.quarantine();
            }
        }
    }
}

#[cfg(test)]
mod tests;

//! Whole-packet receive demand for the original DatagramState. The read-only
//! FIFO walk combines old lengths and preceding frames, so eviction, zero-byte
//! messages and application notifications have the original wire semantics.
//! Holds only an unpublished descriptor layout; payloads stay in the packet.
//! Tests use real encrypted packets in tests/admission/packet_pressure/datagrams.rs.
use super::*;
use crate::frame::{self, Frame};

pub(in crate::connection) struct Checked {
    owner: usize,
    shape: (usize, usize, usize),
    peak: usize,
    meter: Option<BackingMeter>,
    pub(in crate::connection) events: usize,
}
pub(in crate::connection) struct Prepared {
    checked: Checked,
    replacement: Option<storage::Queue>,
}
pub(super) struct Reception {
    evict: usize,
    total: usize,
    notify: bool,
}
impl DatagramState {
    fn packet_shape(&self) -> (usize, usize, usize) {
        (
            self.incoming.len(),
            self.incoming.capacity(),
            self.recv_buffered,
        )
    }
    pub(in crate::connection) fn check_packet(
        &self,
        payload: &Bytes,
        window: &Option<usize>,
    ) -> Result<Checked, TransportError> {
        let mut result = Checked {
            owner: self as *const Self as usize,
            shape: self.packet_shape(),
            peak: self.incoming.len(),
            meter: self.meter.clone(),
            events: 0,
        };
        // All protocol/authority failures precede any capacity refusal.
        for item in frame::Iter::new(payload.clone())? {
            if let Frame::Datagram(data) = item? {
                self.check_received(&data, window)?;
            }
        }
        let limit = window.unwrap_or(0);
        let mut prior = self.incoming.iter().map(|d| window_charge(d.data.len())).chain(
            frame::Iter::new(payload.clone())?.filter_map(|f| match f {
                Ok(Frame::Datagram(data)) => Some(window_charge(data.data.len())),
                _ => None,
            }),
        );
        let (mut count, mut retained) = (self.incoming.len(), self.recv_buffered);
        for item in frame::Iter::new(payload.clone())? {
            let Frame::Datagram(data) = item? else {
                continue;
            };
            result.events += usize::from(retained == 0);
            let charge = window_charge(data.data.len());
            let mut total = retained
                .checked_add(charge)
                .ok_or_else(|| self.packet_error())?;
            while total > limit {
                let old = prior.next().ok_or_else(|| self.packet_error())?;
                count = count.checked_sub(1).ok_or_else(|| self.packet_error())?;
                retained = retained
                    .checked_sub(old)
                    .ok_or_else(|| self.packet_error())?;
                total = retained
                    .checked_add(charge)
                    .ok_or_else(|| self.packet_error())?;
            }
            count = count.checked_add(1).ok_or_else(|| self.packet_error())?;
            retained = total;
            result.peak = result.peak.max(count);
        }
        Ok(result)
    }
    pub(in crate::connection) fn prepare_packet(
        &self,
        checked: Checked,
    ) -> Result<Prepared, MeteredCopyError> {
        if checked.owner != self as *const Self as usize || checked.shape != self.packet_shape() {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
            if let Some(meter) = &checked.meter {
                meter.quarantine();
            }
            return Err(MeteredCopyError::Quarantined);
        }
        let replacement = self.incoming.prepare(checked.peak, self.meter.as_ref())?;
        Ok(Prepared {
            checked,
            replacement,
        })
    }
    pub(in crate::connection) fn install_packet(
        &mut self,
        prepared: Prepared,
    ) -> Result<(), TransportError> {
        if prepared.checked.owner != self as *const Self as usize
            || prepared.checked.shape != self.packet_shape()
        {
            if let Some(meter) = &prepared.checked.meter {
                meter.quarantine();
            }
            return Err(self.packet_error());
        }
        self.incoming.install(prepared.replacement);
        Ok(())
    }
    pub(in crate::connection) fn receive_prepared(
        &mut self,
        datagram: Datagram,
        window: &Option<usize>,
    ) -> Result<bool, TransportError> {
        let window = self.check_received(&datagram, window)?;
        let plan = self.reception(datagram.data.len(), window)?;
        let needed = self.incoming.len() - plan.evict + 1;
        if !self.incoming.funded(needed, self.meter.as_ref()) {
            return Err(self.packet_error());
        }
        Ok(self.commit_reception(datagram, plan, None))
    }
    pub(super) fn check_received(
        &self,
        datagram: &Datagram,
        window: &Option<usize>,
    ) -> Result<usize, TransportError> {
        let window = window
            .ok_or_else(|| TransportError::PROTOCOL_VIOLATION("unexpected DATAGRAM frame"))?;
        if window_charge(datagram.data.len()) > window {
            return Err(TransportError::PROTOCOL_VIOLATION("oversized datagram"));
        }
        self.admit(&datagram.data)
            .map_err(|_| TransportError::INTERNAL_ERROR("datagram backing authority refused"))?;
        Ok(window)
    }
    pub(super) fn reception(
        &self,
        bytes: usize,
        window: usize,
    ) -> Result<Reception, TransportError> {
        let mut evict = 0;
        let mut total = self
            .recv_buffered
            .checked_add(window_charge(bytes))
            .ok_or_else(|| self.packet_error())?;
        for old in self.incoming.iter() {
            if total <= window {
                break;
            }
            total = total
                .checked_sub(window_charge(old.data.len()))
                .ok_or_else(|| self.packet_error())?;
            evict += 1;
        }
        if total > window {
            return Err(self.packet_error());
        }
        Ok(Reception {
            evict,
            total,
            notify: self.recv_buffered == 0,
        })
    }
    pub(super) fn reception_needed(&self, plan: &Reception) -> usize {
        self.incoming.len() - plan.evict + 1
    }
    pub(super) fn commit_reception(
        &mut self,
        datagram: Datagram,
        plan: Reception,
        replacement: Option<storage::Queue>,
    ) -> bool {
        self.incoming.append(replacement, plan.evict, datagram);
        self.recv_buffered = plan.total;
        plan.notify
    }
    fn packet_error(&self) -> TransportError {
        if let Some(meter) = &self.meter {
            meter.quarantine();
        }
        TransportError::INTERNAL_ERROR("datagram packet inventory mismatch")
    }
}

#[cfg(test)]
#[path = "packet/tests.rs"]
mod tests;

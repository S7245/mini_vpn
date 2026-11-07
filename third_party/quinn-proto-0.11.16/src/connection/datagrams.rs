use std::{collections::VecDeque, mem::ManuallyDrop};

use bytes::Bytes;
use thiserror::Error;
use tracing::trace;

use super::Connection;
use bytes::{BackingMeter, MeteredCopyError};
mod storage;
pub(in crate::connection) mod packet;
use crate::{
    frame::{Datagram, FrameStruct},
    TransportError,
};

/// API to control datagram traffic
pub struct Datagrams<'a> {
    pub(super) conn: &'a mut Connection,
}

impl Datagrams<'_> {
    /// Queue an unreliable, unordered datagram for immediate transmission
    ///
    /// If `drop` is true, previously queued datagrams which are still unsent may be discarded to
    /// make space for this datagram, in order of oldest to newest. If `drop` is false, and there
    /// isn't enough space due to previously queued datagrams, this function will return
    /// `SendDatagramError::Blocked`. `Event::DatagramsUnblocked` will be emitted once datagrams
    /// have been sent.
    ///
    /// Returns `Err` iff a `len`-byte datagram cannot currently be sent.
    pub fn send(&mut self, data: Bytes, drop: bool) -> Result<(), SendDatagramError> {
        if self.conn.config.datagram_receive_buffer_size.is_none() {
            return Err(SendDatagramError::Disabled);
        }
        let max = self
            .max_size()
            .ok_or(SendDatagramError::UnsupportedByPeer)?;
        if data.len() > max {
            return Err(SendDatagramError::TooLarge);
        }
        let inventory = &mut self.conn.datagrams;
        if let Err(error) = inventory.admit(&data) {
            return Err(SendDatagramError::Admission { data, error });
        }
        let mut evict = 0;
        let mut retained = inventory.outgoing_total;
        if drop {
            // Keep the original policy: evict only while the OLD byte total is
            // over its limit. A newly appended packet may exceed that limit.
            for old in inventory.outgoing.iter() {
                if retained <= self.conn.config.datagram_send_buffer_size { break; }
                retained -= old.data.len(); evict += 1;
            }
        } else if retained.checked_add(data.len()).is_none_or(|n| n > self.conn.config.datagram_send_buffer_size) {
            // Preserve an actual application-event slot until the original
            // sender becomes unblocked; ordinary notifications cannot consume it.
            let held = usize::from(self.conn.successor_service_turn.is_some());
            if let Err(error) = self.conn.events.reserve(1, held) {
                return Err(SendDatagramError::Admission { data, error });
            }
            inventory.send_blocked = true;
            return Err(SendDatagramError::Blocked(data));
        }
        let needed = inventory.outgoing.len() - evict + 1;
        let replacement = match inventory.outgoing.prepare(needed, inventory.meter.as_ref()) {
            Ok(value) => value,
            Err(error) => return Err(SendDatagramError::Admission { data, error }),
        };
        let Some(total) = retained.checked_add(data.len()) else {
            return Err(SendDatagramError::Admission { data, error: MeteredCopyError::TooLarge });
        };
        inventory.outgoing.append(replacement, evict, Datagram { data });
        inventory.outgoing_total = total;
        Ok(())
    }

    /// Compute the maximum size of datagrams that may passed to `send_datagram`
    ///
    /// Returns `None` if datagrams are unsupported by the peer or disabled locally.
    ///
    /// This may change over the lifetime of a connection according to variation in the path MTU
    /// estimate. The peer can also enforce an arbitrarily small fixed limit, but if the peer's
    /// limit is large this is guaranteed to be a little over a kilobyte at minimum.
    ///
    /// Not necessarily the maximum size of received datagrams.
    pub fn max_size(&self) -> Option<usize> {
        // We use the conservative overhead bound for any packet number, reducing the budget by at
        // most 3 bytes, so that PN size fluctuations don't cause users sending maximum-size
        // datagrams to suffer avoidable packet loss.
        let max_size = self.conn.path.current_mtu() as usize
            - self.conn.predict_1rtt_overhead(None)
            - Datagram::SIZE_BOUND;
        let limit = self
            .conn
            .peer_params
            .max_datagram_frame_size?
            .into_inner()
            .saturating_sub(Datagram::SIZE_BOUND as u64);
        Some(limit.min(max_size as u64) as usize)
    }

    /// Receive an unreliable, unordered datagram
    pub fn recv(&mut self) -> Option<Bytes> {
        self.conn.datagrams.recv()
    }

    /// Bytes available in the outgoing datagram buffer
    ///
    /// When greater than zero, [`send`](Self::send)ing a datagram of at most this size is
    /// guaranteed not to cause older datagrams to be dropped.
    pub fn send_buffer_space(&self) -> usize {
        self.conn
            .config
            .datagram_send_buffer_size
            .saturating_sub(self.conn.datagrams.outgoing_total)
    }
}

/// Receive-window cost of one queued datagram. Upstream counts payload bytes
/// only, so zero-length DATAGRAM frames were free and an unread queue grew
/// without bound in count (A2 measurement, D16). At least one byte each bounds
/// the queue to `window` entries; non-empty datagrams keep upstream semantics.
pub(super) fn window_charge(len: usize) -> usize {
    len.max(1)
}

#[derive(Default)]
pub(super) struct DatagramState {
    /// Number of bytes of datagrams that have been received by the local transport but not
    /// delivered to the application
    pub(super) recv_buffered: usize,
    pub(super) incoming: ManuallyDrop<storage::Queue>,
    pub(super) outgoing: ManuallyDrop<storage::Queue>,
    pub(super) outgoing_total: usize,
    pub(super) send_blocked: bool,
    meter: Option<BackingMeter>,
}

impl DatagramState {
    pub(super) fn new(meter: Option<BackingMeter>) -> Self {
        Self { meter: meter.filter(|m| m.budget_limit().is_some()), incoming: ManuallyDrop::new(storage::Queue::default()), outgoing: ManuallyDrop::new(storage::Queue::default()), recv_buffered: 0, outgoing_total: 0, send_blocked: false }
    }
    fn admit(&self, data: &Bytes) -> Result<(), MeteredCopyError> {
        let Some(meter) = &self.meter else { return Ok(()); };
        meter.admit_precharged()?;
        match data.backing_meter() {
            Some(actual) if actual.same_budget(meter) => return Ok(()),
            None if data.is_empty() && data.allocation_identity().is_none() => return Ok(()),
            _ => {}
        }
        meter.quarantine();
        if let Some(actual) = data.backing_meter() { actual.quarantine(); }
        Err(MeteredCopyError::Quarantined)
    }

    pub(super) fn received(
        &mut self,
        datagram: Datagram,
        window: &Option<usize>,
    ) -> Result<bool, TransportError> {
        let window = self.check_received(&datagram, window)?;
        let plan = self.reception(datagram.data.len(), window)?;
        let needed = self.reception_needed(&plan);
        let replacement = match self.incoming.prepare(needed, self.meter.as_ref()) {
            Ok(value) => value,
            Err(MeteredCopyError::BudgetFull | MeteredCopyError::TooLarge) => return Ok(false),
            Err(_) => {
                if let Some(meter) = &self.meter { meter.quarantine(); }
                return Err(TransportError::INTERNAL_ERROR("datagram descriptor admission refused"));
            }
        };
        Ok(self.commit_reception(datagram, plan, replacement))
    }

    /// Discard outgoing datagrams with a payload larger than `max_payload` bytes
    ///
    /// Used to ensure that reductions in MTU don't get us stuck in a state where we have a datagram
    /// queued but can't send it.
    pub(super) fn drop_oversized(&mut self, max_payload: usize) {
        self.outgoing.retain(|datagram| {
            let result = datagram.data.len() < max_payload;
            if !result {
                trace!(
                    "dropping {} byte datagram violating {} byte limit",
                    datagram.data.len(),
                    max_payload
                );
                self.outgoing_total -= datagram.data.len();
            }
            result
        });
    }

    /// Attempt to write a datagram frame into `buf`, consuming it from `self.outgoing`
    ///
    /// Returns whether a frame was written. At most `max_size` bytes will be written, including
    /// framing.
    pub(super) fn write(&mut self, buf: &mut Vec<u8>, max_size: usize) -> bool {
        let datagram = match self.outgoing.pop_front() {
            Some(x) => x,
            None => return false,
        };

        if buf.len() + datagram.size(true) > max_size {
            // Future work: we could be more clever about cramming small datagrams into
            // mostly-full packets when a larger one is queued first
            self.outgoing.push_front(datagram);
            return false;
        }

        trace!(len = datagram.data.len(), "DATAGRAM");

        self.outgoing_total -= datagram.data.len();
        datagram.encode(true, buf);
        true
    }

    pub(super) fn recv(&mut self) -> Option<Bytes> {
        let x = self.incoming.pop_front()?.data;
        self.recv_buffered -= window_charge(x.len());
        Some(x)
    }
}

/// Errors that can arise when sending a datagram
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SendDatagramError {
    /// The peer does not support receiving datagram frames
    #[error("datagrams not supported by peer")]
    UnsupportedByPeer,
    /// Datagram support is disabled locally
    #[error("datagram support disabled")]
    Disabled,
    /// The datagram is larger than the connection can currently accommodate
    ///
    /// Indicates that the path MTU minus overhead or the limit advertised by the peer has been
    /// exceeded.
    #[error("datagram too large")]
    TooLarge,
    /// Send would block
    #[error("datagram send blocked")]
    Blocked(Bytes),
    /// Original payload and exact Native physical admission refusal.
    #[error("datagram physical admission refused: {error:?}")]
    Admission { data: Bytes, error: MeteredCopyError },
}

impl Drop for DatagramState {
    fn drop(&mut self) {
        if let Some(meter) = &self.meter {
            let incoming = self.incoming.iter().try_fold(0usize, |n, d| n.checked_add(window_charge(d.data.len())));
            let outgoing = self.outgoing.iter().try_fold(0usize, |n, d| n.checked_add(d.data.len()));
            if std::thread::panicking() || incoming != Some(self.recv_buffered)
                || outgoing != Some(self.outgoing_total)
                || !self.incoming.layout_matches(meter) || !self.outgoing.layout_matches(meter)
            { meter.quarantine(); return; }
        }
        // Native payloads are authenticated precharged Bytes; after the whole
        // physical inventory check, these two original containers are known.
        unsafe {
            ManuallyDrop::drop(&mut self.incoming);
            ManuallyDrop::drop(&mut self.outgoing);
        }
    }
}

#[cfg(test)]
mod tests;

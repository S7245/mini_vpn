//! Allocation-free projection over original SentHistory selection bits. Only
//! scalar RTT/ACK-frequency observations are copied; original records stay put.
//! All spaces share the projected path so earlier coalesced ACKs affect DATA
//! loss exactly once. Drop restores marks; the original on_ack_received remains
//! the committing executor. Tests: replay/tests.rs differential and actual cid_loss wire.
use super::*;
use crate::connection::{
    ack_frequency::AckFrequencyState,
    paths::RttEstimator,
    sent_history::{packet_is_lost, Projection},
    spaces::{Dedup, PacketNumberFilter, PacketSpace},
};
use crate::{packet::SpaceId, Duration, TIMER_GRANULARITY};
struct Space<'a> {
    history: Projection<'a>,
    dedup: &'a Dedup,
    largest: Option<u64>,
    largest_sent: Instant,
    next: u64,
}
impl<'a> Space<'a> {
    fn new(s: &'a mut PacketSpace) -> Result<Self, TransportError> {
        Ok(Self {
            history: s.sent_packets.project_recovery()?,
            dedup: &s.dedup,
            largest: s.largest_acked_packet,
            largest_sent: s.largest_acked_packet_sent,
            next: s.next_packet_number,
        })
    }
}
pub(super) struct Replay<'a> {
    spaces: [Space<'a>; 3],
    filter: &'a PacketNumberFilter,
    frequency: AckFrequencyState,
    rtt: RttEstimator,
    now: Instant,
    time_threshold: f32,
    packet_threshold: u64,
    ack_delay_exponent: u64,
    probe: Option<u64>,
}
impl<'a> Replay<'a> {
    pub(super) fn new(conn: &'a mut Connection, now: Instant) -> Result<Self, TransportError> {
        let [initial, handshake, data] = conn.spaces.each_mut();
        Ok(Self {
            spaces: [
                Space::new(initial)?,
                Space::new(handshake)?,
                Space::new(data)?,
            ],
            filter: &conn.packet_number_filter,
            frequency: conn.ack_frequency.clone(),
            rtt: conn.path.rtt,
            now,
            time_threshold: conn.config.time_threshold,
            packet_threshold: conn.config.packet_threshold as u64,
            ack_delay_exponent: conn.peer_params.ack_delay_exponent.0,
            probe: conn.path.mtud.in_flight_mtu_probe(),
        })
    }
    pub(super) fn early(
        &mut self,
        space: SpaceId,
        number: u64,
        payload: &Bytes,
    ) -> Result<(), TransportError> {
        if self.spaces[space as usize].dedup.contains(number) {
            return Ok(());
        }
        for frame in frame::Iter::new(payload.clone())? {
            if let frame::Frame::Ack(ack) = frame? {
                self.ack(space, ack)?;
            }
        }
        Ok(())
    }
    /// Return exact existing retirement nodes requeued by this ACK's loss pass.
    pub(super) fn ack(&mut self, space: SpaceId, ack: frame::Ack) -> Result<usize, TransportError> {
        let s = &mut self.spaces[space as usize];
        if ack.largest >= s.next {
            return Err(TransportError::PROTOCOL_VIOLATION("unsent packet acked"));
        }
        let new_largest = s.largest.is_none_or(|n| ack.largest > n);
        if new_largest {
            s.largest = Some(ack.largest);
            if let Some(info) = s.history.get(ack.largest) {
                s.largest_sent = info.time_sent;
            }
        }
        let mut any = false;
        let mut eliciting = false;
        for range in ack.iter() {
            self.filter.check_ack(space, range.clone())?;
            s.history.mark_where(range, |pn, info| {
                any = true;
                eliciting |= info.ack_eliciting;
                self.frequency.on_acked(pn);
                if space == SpaceId::Data && self.probe == Some(pn) {
                    self.probe = None;
                }
                true
            });
        }
        // The original ACK path observes largest even for an empty selection,
        // but does not update RTT or invoke loss in that case.
        if !any {
            return Ok(0);
        }
        if new_largest && eliciting {
            let delay = if space == SpaceId::Data {
                self.frequency
                    .peer_max_ack_delay
                    .min(Duration::from_micros(ack.delay << self.ack_delay_exponent))
            } else {
                Duration::ZERO
            };
            self.rtt
                .update(delay, self.now.saturating_duration_since(s.largest_sent));
        }
        let delay = self
            .rtt
            .conservative()
            .mul_f32(self.time_threshold)
            .max(TIMER_GRANULARITY);
        let largest = s
            .largest
            .ok_or_else(|| TransportError::INTERNAL_ERROR("missing projected ACK"))?;
        let mut retired = Some(0usize);
        s.history.mark_where(..largest, |pn, info| {
            if !packet_is_lost(
                self.now,
                info.time_sent,
                pn,
                largest,
                delay,
                self.packet_threshold,
            ) {
                return false;
            }
            if space == SpaceId::Data && self.probe == Some(pn) {
                // MTU probe records are removed without requeueing control.
                self.probe = None;
            } else {
                retired = retired.and_then(|n| {
                    n.checked_add(info.retransmits.get().map_or(0, |r| r.retire_cids.len()))
                });
            }
            true
        });
        retired.ok_or_else(|| TransportError::INTERNAL_ERROR("projected retirement overflow"))
    }
}

#[cfg(test)]
mod tests;

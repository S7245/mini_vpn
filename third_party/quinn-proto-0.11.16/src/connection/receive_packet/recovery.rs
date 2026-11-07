//! Read-only ACK/loss resource preparation for the original Connection.
//! ACK intervals are merged once, then matched against relevant SentHistory.
//! Loss includes remaining records below the largest ACK, excluding records
//! covered by the first effective ACK (removed before any loss detection).
//! this covers original RTT/time-threshold decisions without duplicating them.
//! Actual range/event layouts precede commit. Cancellation drops only temporary
//! stock. Original on_ack_received/detect_lost_packets remain the state machine.
use super::stock::Stock;
use super::*;
use crate::connection::send_buffer::PacketCapacity;
use bytes::{BackingMeter, MeteredCopyError};
use std::ops::RangeInclusive;

pub(super) enum PrepareError {
    Capacity(MeteredCopyError),
    Protocol(TransportError),
}
impl From<MeteredCopyError> for PrepareError {
    fn from(e: MeteredCopyError) -> Self {
        Self::Capacity(e)
    }
}
impl From<TransportError> for PrepareError {
    fn from(e: TransportError) -> Self {
        Self::Protocol(e)
    }
}
struct AckRange {
    range: RangeInclusive<u64>,
    first_effective: bool,
}
fn covered(ranges: &[Option<AckRange>], number: u64) -> bool {
    let i = ranges.partition_point(|r| r.as_ref().is_some_and(|r| *r.range.end() < number));
    ranges
        .get(i)
        .and_then(Option::as_ref)
        .is_some_and(|r| r.range.contains(&number))
}
struct Demand {
    id: StreamId,
    acks: usize,
    losses: usize,
    prepared: Option<PacketCapacity>,
}
pub(super) struct Recovery {
    demands: Stock<Demand>,
    pub(super) events: usize,
    pub(super) pending: usize,
    pub(super) retirements: usize,
}
impl Recovery {
    pub(super) fn check(conn: &Connection, space_id: SpaceId, payload: &Bytes)
        -> Result<(Option<u64>, Option<usize>), PrepareError> {
        let space = &conn.spaces[space_id];
        let mut largest = space.largest_acked_packet;
        let mut first_effective = None;
        let mut ack_index = 0usize;
        // Complete protocol validation precedes any capacity refusal.
        for frame in frame::Iter::new(payload.clone())? {
            if let Frame::Ack(ack) = frame.map_err(TransportError::from)? {
                if ack.largest >= space.next_packet_number {
                    return Err(TransportError::PROTOCOL_VIOLATION("unsent packet acked").into());
                }
                largest = Some(largest.map_or(ack.largest, |n| n.max(ack.largest)));
                for range in ack.iter() {
                    conn.packet_number_filter
                        .check_ack(space_id, range.clone())?;
                    if first_effective.is_none() && space.sent_packets.range(range).next().is_some()
                    {
                        first_effective = Some(ack_index);
                    }
                }
                ack_index += 1;
            }
        }
        Ok((largest, first_effective))
    }

    pub(super) fn prepare(
        conn: &Connection,
        space_id: SpaceId,
        payload: &Bytes,
        meter: &BackingMeter,
    ) -> Result<Self, PrepareError> {
        let space = &conn.spaces[space_id];
        let (largest, first_effective) = Self::check(conn, space_id, payload)?;
        let mut result = Self {
            demands: Stock::default(),
            events: 0,
            pending: 0,
            retirements: 0,
        };
        if first_effective.is_none() {
            return Ok(result);
        }
        let largest = largest.ok_or_else(|| {
            TransportError::INTERNAL_ERROR("ACK preparation has no largest packet")
        })?;
        // Qualify original custody before even the temporary range workspace
        // can report Full. Invalid/foreign state must not become a refusal.
        for (&number, info) in space.sent_packets.range(..=largest) {
            if !info.stream_frames.authority_ready() {
                return Err(MeteredCopyError::Quarantined.into());
            }
            if number < largest {
                space.admit_requeue(&info.retransmits)?;
            }
            for frame in info.stream_frames.iter() {
                conn.streams.check_packet_send(&frame, meter)?;
            }
        }
        let mut ranges: Stock<AckRange> = Stock::default();
        let mut ack_index = 0usize;
        for frame in frame::Iter::new(payload.clone())? {
            if let Frame::Ack(ack) = frame.map_err(TransportError::from)? {
                for range in ack.iter() {
                    ranges.push(
                        AckRange {
                            range,
                            first_effective: Some(ack_index) == first_effective,
                        },
                        meter,
                    )?;
                }
                ack_index += 1;
            }
        }
        ranges
            .slice_mut()
            .sort_unstable_by_key(|r| r.as_ref().map(|r| (!r.first_effective, *r.range.start())));
        let mut previous: Option<usize> = None;
        for index in 0..ranges.slice().len() {
            let Some(current) = ranges.slice_mut()[index].take() else {
                continue;
            };
            if let Some(prior) = previous {
                if let Some(old) = &mut ranges.slice_mut()[prior] {
                    if old.first_effective == current.first_effective
                        && *current.range.start() <= old.range.end().saturating_add(1)
                    {
                        old.range =
                            *old.range.start()..=(*old.range.end()).max(*current.range.end());
                        continue;
                    }
                }
            }
            ranges.slice_mut()[index] = Some(current);
            previous = Some(index);
        }
        ranges.compact();
        let split = ranges
            .slice()
            .partition_point(|r| r.as_ref().is_some_and(|r| r.first_effective));
        let (first, later) = ranges.slice().split_at(split);
        for (&number, info) in space.sent_packets.range(..=largest) {
            let first_acked = covered(first, number);
            let acks = usize::from(first_acked || covered(later, number));
            let losses = usize::from(number < largest && !first_acked);
            if acks != 0 {
                result.retirements = result.retirements.checked_add(info.retransmits.reset_iter().count())
                    .and_then(|n| n.checked_add(info.stream_frames.iter().count()))
                    .ok_or(MeteredCopyError::TooLarge)?;
            }
            for frame in info.stream_frames.iter() {
                result.events = result
                    .events
                    .checked_add(acks)
                    .ok_or(MeteredCopyError::TooLarge)?;
                result.pending = result
                    .pending
                    .checked_add(losses)
                    .ok_or(MeteredCopyError::TooLarge)?;
                result.demands.push(
                    Demand {
                        id: frame.id,
                        acks,
                        losses,
                        prepared: None,
                    },
                    meter,
                )?;
            }
        }
        result
            .demands
            .slice_mut()
            .sort_unstable_by_key(|d| d.as_ref().map(|d| d.id));
        let mut previous: Option<usize> = None;
        for index in 0..result.demands.slice().len() {
            let Some(current) = result.demands.slice_mut()[index].take() else {
                continue;
            };
            if let Some(prior) = previous {
                if let Some(old) = &mut result.demands.slice_mut()[prior] {
                    if old.id == current.id {
                        old.acks = old
                            .acks
                            .checked_add(current.acks)
                            .ok_or(MeteredCopyError::TooLarge)?;
                        old.losses = old
                            .losses
                            .checked_add(current.losses)
                            .ok_or(MeteredCopyError::TooLarge)?;
                        continue;
                    }
                }
            }
            result.demands.slice_mut()[index] = Some(current);
            previous = Some(index);
        }
        result.demands.compact();
        for demand in result.demands.slice_mut().iter_mut().flatten() {
            demand.prepared =
                conn.streams
                    .prepare_packet_send(demand.id, demand.acks, demand.losses)?;
        }
        Ok(result)
    }
    pub(super) fn install(
        mut self,
        streams: &mut streams::StreamsState,
    ) -> Result<(), MeteredCopyError> {
        for demand in self.demands.slice_mut().iter_mut().flatten() {
            if let Some(prepared) = demand.prepared.take() {
                streams.install_packet_send(demand.id, prepared)?;
            }
        }
        Ok(())
    }
}

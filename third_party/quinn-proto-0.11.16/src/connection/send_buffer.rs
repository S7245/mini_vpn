use std::{collections::VecDeque, ops::Range};

use bytes::{Buf, Bytes, BackingMeter, MeteredCopyError};
mod native;
#[cfg(test)]
#[path = "send_buffer/tests/native.rs"]
mod native_tests;


use crate::{range_set::RangeSet, VarInt};

/// Buffer of outgoing retransmittable stream data
#[derive(Default, Debug)]
pub(super) struct SendBuffer {
    /// Data queued by the application but not yet acknowledged. May or may not have been sent.
    unacked_segments: VecDeque<Bytes>,
    /// Total size of `unacked_segments`
    unacked_len: usize,
    /// The first offset that hasn't been written by the application, i.e. the offset past the end of `unacked`
    offset: u64,
    /// The first offset that hasn't been sent
    ///
    /// Always lies in (offset - unacked.len())..offset
    unsent: u64,
    /// Acknowledged ranges which couldn't be discarded yet as they don't include the earliest
    /// offset in `unacked`
    // TODO: Recover storage from these by compacting (#700)
    acks: RangeSet,
    /// Previously transmitted ranges deemed lost
    retransmits: RangeSet,
    native: Option<native::Native>,
}

impl SendBuffer {
    pub(in crate::connection) fn check_packet_range(&self, range: &Range<u64>, meter: &BackingMeter) -> Result<(), MeteredCopyError> {
        if range.end > self.offset || range.start > range.end {
            meter.quarantine(); return Err(MeteredCopyError::Quarantined);
        }
        match &self.native {
            Some(native) if native.meter.same_authority(meter) => native.stamp().map(|_| ()),
            None if self.closed_inventory_matches(meter) => Ok(()),
            _ => {
                meter.quarantine();
                if let Some(native) = &self.native { native.meter.quarantine(); }
                Err(MeteredCopyError::Quarantined)
            }
        }
    }
    pub(in crate::connection) fn prepare_packet_capacity(&self, acks: usize, losses: usize) -> Result<PacketCapacity, MeteredCopyError> {
        self.native.as_ref().map(|n| n.prepare_packet_capacity(acks, losses))
            .transpose().map(PacketCapacity)
    }
    pub(in crate::connection) fn install_packet_capacity(&mut self, prepared: PacketCapacity) -> Result<(), MeteredCopyError> {
        match (&mut self.native, prepared.0) {
            (Some(native), Some(plan)) => native.install_packet_capacity(plan),
            (None, None) => Ok(()),
            (Some(native), None) => { native.meter.quarantine(); Err(MeteredCopyError::Quarantined) }
            (None, Some(_)) => Err(MeteredCopyError::Quarantined),
        }
    }

    pub(in crate::connection) fn retained_metadata_bytes(&self, meter: &BackingMeter) -> Option<usize> {
        self.closed_inventory_matches(meter).then_some(())?;
        self.native.as_ref().map_or(Some(0), |native| native.metadata_bytes())
    }

    pub(super) fn closed_inventory_matches(&self, meter: &BackingMeter) -> bool {
        match &self.native {
            Some(native) => native.meter.same_authority(meter) && native.inventory_matches(self.unacked_len),
            None => self.offset == 0 && self.unacked_len == 0 && self.unsent == 0
                && self.unacked_segments.capacity() == 0 && self.acks.is_empty() && self.retransmits.is_empty(),
        }
    }

    #[cfg(test)]
    pub(super) fn encoding_cursor_for_probe(&self) -> u64 {
        self.unsent
    }

    /// Construct an empty buffer at the initial offset
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn prepare_write(&mut self, chunks: usize, meter: Option<&BackingMeter>) -> Result<PreparedWrite, MeteredCopyError> {
        if let Some(meter) = meter {
            meter.admit_precharged()?;
            if self.native.is_none() {
                if self.offset != 0 || !self.unacked_segments.is_empty() || self.unacked_segments.capacity() != 0 {
                    meter.quarantine(); return Err(MeteredCopyError::Quarantined);
                }
                self.native = Some(native::Native::new(meter.clone()));
            }
            let Some(native) = self.native.as_ref() else { meter.quarantine(); return Err(MeteredCopyError::Quarantined); };
            if !native.meter.same_authority(meter) {
                native.meter.quarantine(); meter.quarantine(); return Err(MeteredCopyError::Quarantined);
            }
            let stamp = native.stamp()?;
            return Ok(PreparedWrite { replacement: native.prepare(chunks)?, owner: Some(meter.clone()), stamp: Some(stamp) });
        }
        if let Some(native) = &self.native { native.meter.quarantine(); return Err(MeteredCopyError::Quarantined); }
        Ok(PreparedWrite { replacement: None, owner: None, stamp: None })
    }
    pub(super) fn install_write(&mut self, plan: PreparedWrite) -> Result<(), MeteredCopyError> {
        match (&mut self.native, plan.owner, plan.stamp) {
            (Some(native), Some(owner), Some(stamp)) => native.install(plan.replacement, stamp, &owner),
            (None, None, None) => Ok(()),
            (native, owner, _) => {
                if let Some(native) = native { native.meter.quarantine(); }
                if let Some(owner) = owner { owner.quarantine(); }
                Err(MeteredCopyError::Quarantined)
            }
        }
    }
    pub(super) fn prepare_ack(&self, range: Range<u64>) -> Result<PreparedRange, MeteredCopyError> {
        match &self.native {
            Some(native) => native.prepare_ack(range, self.offset, self.unacked_len).map(PreparedRange::Native),
            None => Ok(PreparedRange::Legacy(range)),
        }
    }
    pub(super) fn apply_ack(&mut self, plan: PreparedRange) -> Result<(), MeteredCopyError> {
        match (&mut self.native, plan) {
            (None, PreparedRange::Legacy(range)) => { self.ack(range); Ok(()) }
            (Some(native), PreparedRange::Native(plan)) => native.ack(plan, self.offset, &mut self.unacked_len),
            (native, plan) => {
                if let Some(native) = native { native.meter.quarantine(); }
                if let PreparedRange::Native(plan) = plan { plan.owner.quarantine(); }
                Err(MeteredCopyError::Quarantined)
            }
        }
    }
    pub(super) fn ack_covers(&self, range: Range<u64>) -> bool {
        let base = self.offset - self.unacked_len as u64;
        match &self.native {
            Some(native) => native.ack_covers(range, base, self.offset),
            None => covers(range, base, self.offset, self.acks.iter()),
        }
    }
    pub(super) fn prepare_retransmit(&self, range: Range<u64>) -> Result<PreparedRange, MeteredCopyError> {
        match &self.native {
            Some(native) => native.prepare_retransmit(range).map(PreparedRange::Native),
            None => Ok(PreparedRange::Legacy(range)),
        }
    }
    pub(super) fn apply_retransmit(&mut self, plan: PreparedRange) -> Result<(), MeteredCopyError> {
        match (&mut self.native, plan) {
            (None, PreparedRange::Legacy(range)) => { self.retransmit(range); Ok(()) }
            (Some(native), PreparedRange::Native(plan)) => native.retransmit(plan),
            (native, plan) => {
                if let Some(native) = native { native.meter.quarantine(); }
                if let PreparedRange::Native(plan) = plan { plan.owner.quarantine(); }
                Err(MeteredCopyError::Quarantined)
            }
        }
    }

    /// Append application data to the end of the stream
    pub(super) fn write(&mut self, data: Bytes) {
        self.unacked_len += data.len();
        self.offset += data.len() as u64;
        if let Some(native) = &mut self.native { native.push(data); }
        else { self.unacked_segments.push_back(data); }
    }

    /// Discard a range of acknowledged stream data
    pub(super) fn ack(&mut self, mut range: Range<u64>) {
        // Clamp the range to data which is still tracked
        let base_offset = self.offset - self.unacked_len as u64;
        range.start = base_offset.max(range.start);
        range.end = base_offset.max(range.end);

        self.acks.insert(range);

        while self.acks.min() == Some(self.offset - self.unacked_len as u64) {
            let prefix = self.acks.pop_min().unwrap();
            let mut to_advance = (prefix.end - prefix.start) as usize;

            self.unacked_len -= to_advance;
            while to_advance > 0 {
                let front = self
                    .unacked_segments
                    .front_mut()
                    .expect("Expected buffered data");

                if front.len() <= to_advance {
                    to_advance -= front.len();
                    self.unacked_segments.pop_front();

                    if self.unacked_segments.len() * 4 < self.unacked_segments.capacity() {
                        self.unacked_segments.shrink_to_fit();
                    }
                } else {
                    front.advance(to_advance);
                    to_advance = 0;
                }
            }
        }
    }

    /// Compute the next range to transmit on this stream and update state to account for that
    /// transmission.
    ///
    /// `max_len` here includes the space which is available to transmit the
    /// offset and length of the data to send. The caller has to guarantee that
    /// there is at least enough space available to write maximum-sized metadata
    /// (8 byte offset + 8 byte length).
    ///
    /// The method returns a tuple:
    /// - The first return value indicates the range of data to send
    /// - The second return value indicates whether the length needs to be encoded
    ///   in the STREAM frames metadata (`true`), or whether it can be omitted
    ///   since the selected range will fill the whole packet.
    pub(super) fn poll_transmit(&mut self, mut max_len: usize) -> (Range<u64>, bool) {
        debug_assert!(max_len >= 8 + 8);
        let mut encode_length = false;

        if let Some(range) = match &mut self.native {
            Some(native) => native.pop_retransmit(), None => self.retransmits.pop_min(),
        } {
            // Retransmit sent data

            // When the offset is known, we know how many bytes are required to encode it.
            // Offset 0 requires no space
            if range.start != 0 {
                max_len -= VarInt::size(unsafe { VarInt::from_u64_unchecked(range.start) });
            }
            if range.end - range.start < max_len as u64 {
                encode_length = true;
                max_len -= 8;
            }

            let end = range.end.min((max_len as u64).saturating_add(range.start));
            if end != range.end {
                if let Some(native) = &mut self.native { native.put_suffix(end..range.end); }
                else { self.retransmits.insert(end..range.end); }
            }
            return (range.start..end, encode_length);
        }

        // Transmit new data

        // When the offset is known, we know how many bytes are required to encode it.
        // Offset 0 requires no space
        if self.unsent != 0 {
            max_len -= VarInt::size(unsafe { VarInt::from_u64_unchecked(self.unsent) });
        }
        if self.offset - self.unsent < max_len as u64 {
            encode_length = true;
            max_len -= 8;
        }

        let end = self
            .offset
            .min((max_len as u64).saturating_add(self.unsent));
        let result = self.unsent..end;
        self.unsent = end;
        (result, encode_length)
    }

    /// Returns data which is associated with a range
    ///
    /// This function can return a subset of the range, if the data is stored
    /// in noncontiguous fashion in the send buffer. In this case callers
    /// should call the function again with an incremented start offset to
    /// retrieve more data.
    pub(super) fn get(&self, offsets: Range<u64>) -> &[u8] {
        let base_offset = self.offset - self.unacked_len as u64;
        if let Some(native) = &self.native { return native.get(offsets, base_offset); }

        let mut segment_offset = base_offset;
        for segment in self.unacked_segments.iter() {
            if offsets.start >= segment_offset
                && offsets.start < segment_offset + segment.len() as u64
            {
                let start = (offsets.start - segment_offset) as usize;
                let end = (offsets.end - segment_offset) as usize;

                return &segment[start..end.min(segment.len())];
            }
            segment_offset += segment.len() as u64;
        }

        &[]
    }

    /// Queue a range of sent but unacknowledged data to be retransmitted
    pub(super) fn retransmit(&mut self, range: Range<u64>) {
        debug_assert!(range.end <= self.unsent, "unsent data can't be lost");
        self.retransmits.insert(range);
    }

    pub(super) fn retransmit_all_for_0rtt(&mut self) {
        debug_assert_eq!(self.offset, self.unacked_len as u64);
        if let Some(native) = &mut self.native { native.changed(); }
        self.unsent = 0;
    }

    /// First stream offset unwritten by the application, i.e. the offset that the next write will
    /// begin at
    pub(super) fn offset(&self) -> u64 {
        self.offset
    }

    /// First stream offset that has not been cumulatively acknowledged.
    pub(super) fn first_unacked_offset(&self) -> u64 {
        self.offset - self.unacked_len as u64
    }

    /// Whether all sent data has been acknowledged
    pub(super) fn is_fully_acked(&self) -> bool {
        self.unacked_len == 0
    }

    /// Whether there's data to send
    ///
    /// There may be sent unacknowledged data even when this is false.
    pub(super) fn has_unsent_data(&self) -> bool {
        self.unsent != self.offset || self.native.as_ref().map_or_else(
            || !self.retransmits.is_empty(), native::Native::has_retransmits)
    }

    /// Compute the amount of data that hasn't been acknowledged
    pub(super) fn unacked(&self) -> u64 {
        self.unacked_len as u64 - self.native.as_ref().map_or_else(
            || self.acks.iter().map(|x| x.end - x.start).sum::<u64>(), native::Native::acknowledged)
    }
}

pub(super) struct PreparedWrite { replacement: Option<native::Segments>, owner: Option<BackingMeter>, stamp: Option<native::Stamp> }
pub(super) enum PreparedRange { Legacy(Range<u64>), Native(native::Ack) }
fn covers(range: Range<u64>, base: u64, offset: u64, old: impl Iterator<Item = Range<u64>>) -> bool {
    if base == offset { return true; }
    if range.start > base || range.end < base { return false; }
    let mut through = range.end;
    for old in old {
        if old.start > through { break; }
        through = through.max(old.end);
    }
    through >= offset
}
impl Drop for SendBuffer {
    fn drop(&mut self) {
        if let Some(native) = &self.native {
            if !native.inventory_matches(self.unacked_len) { native.meter.quarantine(); }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fragment_with_length() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world!";
        buf.write(MSG.into());
        // 0 byte offset => 19 bytes left => 13 byte data isn't enough
        // with 8 bytes reserved for length 11 payload bytes will fit
        assert_eq!(buf.poll_transmit(19), (0..11, true));
        assert_eq!(
            buf.poll_transmit(MSG.len() + 16 - 11),
            (11..MSG.len() as u64, true)
        );
        assert_eq!(
            buf.poll_transmit(58),
            (MSG.len() as u64..MSG.len() as u64, true)
        );
    }

    #[test]
    fn fragment_without_length() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world with some extra data!";
        buf.write(MSG.into());
        // 0 byte offset => 19 bytes left => can be filled by 34 bytes payload
        assert_eq!(buf.poll_transmit(19), (0..19, false));
        assert_eq!(
            buf.poll_transmit(MSG.len() - 19 + 1),
            (19..MSG.len() as u64, false)
        );
        assert_eq!(
            buf.poll_transmit(58),
            (MSG.len() as u64..MSG.len() as u64, true)
        );
    }

    #[test]
    fn reserves_encoded_offset() {
        let mut buf = SendBuffer::new();

        // Pretend we have more than 1 GB of data in the buffer
        let chunk: Bytes = Bytes::from_static(&[0; 1024 * 1024]);
        for _ in 0..1025 {
            buf.write(chunk.clone());
        }

        const SIZE1: u64 = 64;
        const SIZE2: u64 = 16 * 1024;
        const SIZE3: u64 = 1024 * 1024 * 1024;

        // Offset 0 requires no space
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        buf.retransmit(0..16);
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        let mut transmitted = 16u64;

        // Offset 16 requires 1 byte
        assert_eq!(
            buf.poll_transmit((SIZE1 - transmitted + 1) as usize),
            (transmitted..SIZE1, false)
        );
        buf.retransmit(transmitted..SIZE1);
        assert_eq!(
            buf.poll_transmit((SIZE1 - transmitted + 1) as usize),
            (transmitted..SIZE1, false)
        );
        transmitted = SIZE1;

        // Offset 64 requires 2 bytes
        assert_eq!(
            buf.poll_transmit((SIZE2 - transmitted + 2) as usize),
            (transmitted..SIZE2, false)
        );
        buf.retransmit(transmitted..SIZE2);
        assert_eq!(
            buf.poll_transmit((SIZE2 - transmitted + 2) as usize),
            (transmitted..SIZE2, false)
        );
        transmitted = SIZE2;

        // Offset 16384 requires requires 4 bytes
        assert_eq!(
            buf.poll_transmit((SIZE3 - transmitted + 4) as usize),
            (transmitted..SIZE3, false)
        );
        buf.retransmit(transmitted..SIZE3);
        assert_eq!(
            buf.poll_transmit((SIZE3 - transmitted + 4) as usize),
            (transmitted..SIZE3, false)
        );
        transmitted = SIZE3;

        // Offset 1GB requires 8 bytes
        assert_eq!(
            buf.poll_transmit(chunk.len() + 8),
            (transmitted..transmitted + chunk.len() as u64, false)
        );
        buf.retransmit(transmitted..transmitted + chunk.len() as u64);
        assert_eq!(
            buf.poll_transmit(chunk.len() + 8),
            (transmitted..transmitted + chunk.len() as u64, false)
        );
    }

    #[test]
    fn multiple_segments() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world!";
        const MSG_LEN: u64 = MSG.len() as u64;

        const SEG1: &[u8] = b"He";
        buf.write(SEG1.into());
        const SEG2: &[u8] = b"llo,";
        buf.write(SEG2.into());
        const SEG3: &[u8] = b" w";
        buf.write(SEG3.into());
        const SEG4: &[u8] = b"o";
        buf.write(SEG4.into());
        const SEG5: &[u8] = b"rld!";
        buf.write(SEG5.into());

        assert_eq!(aggregate_unacked(&buf), MSG);

        assert_eq!(buf.poll_transmit(16), (0..8, true));
        assert_eq!(buf.get(0..5), SEG1);
        assert_eq!(buf.get(2..8), SEG2);
        assert_eq!(buf.get(6..8), SEG3);

        assert_eq!(buf.poll_transmit(16), (8..MSG_LEN, true));
        assert_eq!(buf.get(8..MSG_LEN), SEG4);
        assert_eq!(buf.get(9..MSG_LEN), SEG5);

        assert_eq!(buf.poll_transmit(42), (MSG_LEN..MSG_LEN, true));

        // Now drain the segments
        buf.ack(0..1);
        assert_eq!(aggregate_unacked(&buf), &MSG[1..]);
        buf.ack(0..3);
        assert_eq!(aggregate_unacked(&buf), &MSG[3..]);
        buf.ack(3..5);
        assert_eq!(aggregate_unacked(&buf), &MSG[5..]);
        buf.ack(7..9);
        assert_eq!(aggregate_unacked(&buf), &MSG[5..]);
        buf.ack(4..7);
        assert_eq!(aggregate_unacked(&buf), &MSG[9..]);
        buf.ack(0..MSG_LEN);
        assert_eq!(aggregate_unacked(&buf), &[] as &[u8]);
    }

    #[test]
    fn retransmit() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world with extra data!";
        buf.write(MSG.into());
        // Transmit two frames
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        assert_eq!(buf.poll_transmit(16), (16..23, true));
        // Lose the first, but not the second
        buf.retransmit(0..16);
        // Ensure we only retransmit the lost frame, then continue sending fresh data
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        assert_eq!(buf.poll_transmit(16), (23..MSG.len() as u64, true));
        // Lose the second frame
        buf.retransmit(16..23);
        assert_eq!(buf.poll_transmit(16), (16..23, true));
    }

    #[test]
    fn ack() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world!";
        buf.write(MSG.into());
        assert_eq!(buf.poll_transmit(16), (0..8, true));
        buf.ack(0..8);
        assert_eq!(aggregate_unacked(&buf), &MSG[8..]);
    }

    #[test]
    fn reordered_ack() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world with extra data!";
        buf.write(MSG.into());
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        assert_eq!(buf.poll_transmit(16), (16..23, true));
        buf.ack(16..23);
        assert_eq!(aggregate_unacked(&buf), MSG);
        buf.ack(0..16);
        assert_eq!(aggregate_unacked(&buf), &MSG[23..]);
        assert!(buf.acks.is_empty());
    }

    fn aggregate_unacked(buf: &SendBuffer) -> Vec<u8> {
        let mut result = Vec::new();
        for segment in buf.unacked_segments.iter() {
            result.extend_from_slice(&segment[..]);
        }
        result
    }
}

pub(in crate::connection) struct PacketCapacity(Option<native::PacketCapacity>);

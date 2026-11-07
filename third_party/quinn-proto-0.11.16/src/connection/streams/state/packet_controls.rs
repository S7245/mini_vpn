//! Packet-local flow-control inventory in the original StreamsState. Counts
//! events in wire order and holds actual previously absent Send instances.
//! Does not apply protocol effects: the original handlers perform the commit.
//! Tests use real encrypted control/STREAM packets and private boundary tracers.
use super::*;
use crate::connection::storage::stock::Stock;
use bytes::{BackingMeter, Bytes, MeteredCopyError};

struct Demand {
    id: StreamId,
    max_data: u64,
    end: u64,
    ready: bool,
    blocked: bool,
    stopped: bool,
    fresh: Option<Box<Send>>,
}
pub(in crate::connection) struct PacketControls {
    demands: Stock<Demand>,
    pub(in crate::connection) events: usize,
    pub(in crate::connection) blocked: usize,
}
impl StreamsState {
    pub(in crate::connection) fn check_packet_flow_control(
        &self,
        frame: &frame::Frame,
    ) -> Result<(), TransportError> {
        match frame {
            frame::Frame::MaxStreams { count, .. } if *count > MAX_STREAM_COUNT => {
                return Err(TransportError::FRAME_ENCODING_ERROR(
                    "unrepresentable stream limit",
                ));
            }
            frame::Frame::StopSending(frame::StopSending { id, .. }) => {
                if id.initiator() != self.side && id.dir() == Dir::Uni {
                    return Err(TransportError::STREAM_STATE_ERROR("STOP_SENDING on recv-only stream"));
                }
                if id.initiator() == self.side && self.is_local_unopened(*id) {
                    return Err(TransportError::STREAM_STATE_ERROR("STOP_SENDING on unopened stream"));
                }
            }
            frame::Frame::MaxStreamData { id, .. } => {
                if id.initiator() != self.side && id.dir() == Dir::Uni {
                    return Err(TransportError::STREAM_STATE_ERROR(
                        "MAX_STREAM_DATA on recv-only stream",
                    ));
                }
                if !self.send.contains_key(id)
                    && id.initiator() == self.side
                    && self.is_local_unopened(*id)
                {
                    return Err(TransportError::STREAM_STATE_ERROR(
                        "MAX_STREAM_DATA on unopened stream",
                    ));
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub(in crate::connection) fn prepare_packet_controls(
        &self,
        payload: &Bytes,
        meter: &BackingMeter,
    ) -> Result<PacketControls, MeteredCopyError> {
        let mut result = PacketControls {
            demands: Stock::default(),
            events: 0,
            blocked: 0,
        };
        let mut max_streams = self.max;
        let mut max_data = self.max_data;
        let mut ack_seen = false;
        for frame in frame::Iter::new(payload.clone()).map_err(|_| MeteredCopyError::Quarantined)? {
            match frame.map_err(|_| MeteredCopyError::Quarantined)? {
                frame::Frame::Ack(_) => ack_seen = true,
                frame::Frame::MaxData(offset) => max_data = max_data.max(offset.into()),
                frame::Frame::MaxStreams { dir, count } => {
                    if count > max_streams[dir as usize] {
                        max_streams[dir as usize] = count;
                        result.events += 1;
                    }
                }
                frame::Frame::StopSending(frame::StopSending { id, .. }) => {
                    let Some(index) = result.demand(self, id, meter)? else { continue };
                    let demand = result.demands.slice_mut()[index].as_mut().ok_or(MeteredCopyError::Quarantined)?;
                    if !demand.stopped { result.events += 1; demand.stopped = true; }
                }
                frame::Frame::MaxStreamData { id, offset } => {
                    let Some(index) = result.demand(self, id, meter)? else { continue };
                    let demand = result.demands.slice_mut()[index]
                        .as_mut()
                        .ok_or(MeteredCopyError::Quarantined)?;
                    if !demand.ready || offset <= demand.max_data {
                        continue;
                    }
                    if demand.end == demand.max_data {
                        // ACK may free send-window credit before this frame. Count both
                        // possible destinations only for an actually blocked stream;
                        // ordinary credit updates allocate no event or blocked stock.
                        let connection_credit = max_data > self.data_sent;
                        let window_credit = self.send_window > self.unacked_data;
                        if connection_credit && (window_credit || ack_seen) {
                            result.events += 1;
                        }
                        if (!connection_credit || !window_credit) && !demand.blocked {
                            result.blocked += 1;
                            demand.blocked = true;
                        }
                    }
                    demand.max_data = offset;
                }
                _ => {}
            }
        }
        Ok(result)
    }
}
impl PacketControls {
    fn demand(&mut self, streams: &StreamsState, id: StreamId, meter: &BackingMeter) -> Result<Option<usize>, MeteredCopyError> {
        let Some(existing) = streams.send.get(&id) else { return Ok(None) };
        if let Some(index) = self.demands.slice().iter().position(|d| d.as_ref().is_some_and(|d| d.id == id)) {
            return Ok(Some(index));
        }
        let fresh = if existing.is_none() { Some(Send::try_new(streams.max_send_data(id))?) } else { None };
        let send = existing.as_deref().or(fresh.as_deref()).ok_or(MeteredCopyError::Quarantined)?;
        let demand = Demand { id, max_data: send.max_data, end: send.pending.offset(),
            ready: send.state == SendState::Ready, blocked: send.connection_blocked,
            stopped: send.stop_reason.is_some(), fresh };
        let index = self.demands.slice().len();
        self.demands.push(demand, meter)?;
        Ok(Some(index))
    }
    pub(in crate::connection) fn install(
        mut self,
        streams: &mut StreamsState,
    ) -> Result<(), MeteredCopyError> {
        // Qualify every original empty slot before installing any new instance.
        if self
            .demands
            .slice()
            .iter()
            .flatten()
            .any(|d| d.fresh.is_some() && !matches!(streams.send.get(&d.id), Some(None)))
        {
            if let Some(meter) = streams.native_meter() {
                meter.quarantine();
            }
            return Err(MeteredCopyError::Quarantined);
        }
        for demand in self.demands.slice_mut().iter_mut().flatten() {
            if let Some(send) = demand.fresh.take() {
                let slot = streams
                    .send
                    .get_mut(&demand.id)
                    .ok_or(MeteredCopyError::Quarantined)?;
                *slot = Some(send);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;

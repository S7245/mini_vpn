//! STREAM and send-recovery preparations belong to the original StreamsState.
//! Connection retains exclusive custody for this synchronous packet turn. ACK
//! may retire Send state. RESET/stopped FIN retires and reuses the original Recv;
//! table and free-cache capacity precedes either wire-order transition.
//! All layouts and operation queues are prepared before protocol publication.
//! Tests exercise real encrypted packets in tests/admission/packet_pressure.rs.
use super::*;
use crate::connection::{
    assembler::{insertion::PacketInsertion, InsertError},
    send_buffer::PacketCapacity,
};
use bytes::{BackingMeter, Bytes, MeteredCopyError};

pub(in crate::connection) struct PacketReceive {
    id: StreamId,
    end: u64,
    fin: bool,
    credit: u64,
    insertion: PacketInsertion,
}

impl StreamsState {
    pub(in crate::connection) fn check_packet_send(
        &self,
        frame: &frame::StreamMeta,
        meter: &BackingMeter,
    ) -> Result<(), MeteredCopyError> {
        if let Some(stream) = self.send.get(&frame.id).and_then(Option::as_ref) {
            stream.pending.check_packet_range(&frame.offsets, meter)?;
        }
        Ok(())
    }
    pub(in crate::connection) fn prepare_packet_send(
        &self,
        id: StreamId,
        acks: usize,
        losses: usize,
    ) -> Result<Option<PacketCapacity>, MeteredCopyError> {
        self.send
            .get(&id)
            .and_then(Option::as_ref)
            .map(|stream| stream.pending.prepare_packet_capacity(acks, losses))
            .transpose()
    }
    pub(in crate::connection) fn install_packet_send(
        &mut self,
        id: StreamId,
        prepared: PacketCapacity,
    ) -> Result<(), MeteredCopyError> {
        let Some(stream) = self.send.get_mut(&id).and_then(Option::as_mut) else {
            if let Some(meter) = self.native_meter() {
                meter.quarantine();
            }
            return Err(MeteredCopyError::Quarantined);
        };
        stream.pending.install_packet_capacity(prepared)
    }
    pub(in crate::connection) fn commit_packet_stream(
        &mut self,
        prepared: PacketReceive,
    ) -> Result<(), TransportError> {
        let Some(Some(StreamRecv::Open(recv))) = self.recv.get_mut(&prepared.id) else {
            if let Some(meter) = self.native_meter() {
                meter.quarantine();
            }
            return Err(TransportError::INTERNAL_ERROR(
                "prepared receive instance changed",
            ));
        };
        prepared
            .insertion
            .commit(&mut recv.assembler)
            .map_err(receive_error)?;
        recv.commit_received_metadata(prepared.end, prepared.fin);
        self.data_recvd = self.data_recvd.saturating_add(prepared.credit);
        self.on_stream_frame(true, prepared.id);
        Ok(())
    }
}

impl PacketReceive {
    fn prepare(
        recv: &mut Recv,
        frame: frame::Stream,
        payload_len: usize,
        received: u64,
        max_data: u64,
        meter: &BackingMeter,
    ) -> Result<Option<Self>, TransportError> {
        let (end, credit) = recv.validate_ingest(
            frame.offset,
            frame.data.len(),
            frame.fin,
            received,
            max_data,
            Some(meter),
        )?;
        recv.assembler
            .check_input_authority(&frame.data, Some(meter))
            .map_err(receive_error)?;
        let insertion = recv
            .assembler
            .prepare_insert(frame.offset, frame.data, payload_len, Some(meter))
            .and_then(|prepared| prepared.prepare_defragmentation());
        let insertion = match insertion {
            Ok(prepared) => prepared.for_packet(),
            Err(InsertError::Capacity(MeteredCopyError::BudgetFull)) => return Ok(None),
            Err(error) => return Err(receive_error(error)),
        };
        Ok(Some(PacketReceive {
            id: frame.id,
            end,
            credit,
            fin: frame.fin,
            insertion,
        }))
    }
}

fn receive_error(error: InsertError) -> TransportError {
    match error {
        InsertError::TooManyChunks => {
            TransportError::INTERNAL_ERROR("too many gaps in stream buffer")
        }
        InsertError::Capacity(_) => {
            TransportError::INTERNAL_ERROR("receive inventory admission refused")
        }
        InsertError::AuthorityMismatch => {
            TransportError::INTERNAL_ERROR("receive backing authority mismatch")
        }
    }
}

use crate::connection::{assembler::batch::BatchInsertion, storage::stock::Stock};

mod retirement;

enum ReceivePlan {
    Control,
    Single(Option<PacketReceive>),
    Batch(BatchInsertion),
}
struct StreamPlan {
    id: StreamId,
    receive: ReceivePlan,
    receiver: Option<Receiver>,
}
pub(in crate::connection) struct PacketReceives {
    plans: Stock<StreamPlan>,
    retirement: Option<retirement::Prepared>,
}

impl StreamsState {
    /// Validate every frame before any budget-dependent work. Replaying only
    /// earlier frames of the same packet keeps FIN and credit validation in the
    /// original Recv's value-only protocol validator, without cloning inventories.
    pub(in crate::connection) fn check_packet_streams(
        &self,
        payload: &Bytes,
        meter: &BackingMeter,
    ) -> Result<usize, TransportError> {
        let mut received = self.data_recvd;
        let mut max_data = self.local_max_data;
        let mut shrink = self.receive_window_shrink_debt;
        let mut events = 0usize;
        let mut next_remote = self.next_remote;
        for (index, frame) in frame::Iter::new(payload.clone())?.enumerate() {
            let frame = frame?;
            let id = match &frame {
                frame::Frame::MaxStreamData { id, .. }
                | frame::Frame::StopSending(frame::StopSending { id, .. }) => {
                    if id.initiator() != self.side {
                        next_remote[id.dir() as usize] = next_remote[id.dir() as usize].max(id.index() + 1);
                    }
                    continue;
                }
                frame::Frame::Stream(s) => s.id,
                frame::Frame::ResetStream(s) => s.id,
                _ => continue,
            };
            self.validate_receive_id(id)?;
            let initial = Recv::initial(self.stream_receive_window);
            let recv = match self.recv.get(&id) {
                Some(None) => &initial,
                Some(Some(StreamRecv::Open(r) | StreamRecv::Free(r))) => r,
                None => &initial,
            };
            if let frame::Frame::Stream(s) = &frame {
                recv.assembler.check_input_authority(&s.data, Some(meter)).map_err(receive_error)?;
            }
            let mut progress = recv.packet_progress();
            progress.closed = !self.recv.contains_key(&id);
            for previous in frame::Iter::new(payload.clone())?.take(index) {
                let previous = previous?;
                if receive_id(&previous) == Some(id) {
                    progress.apply(&previous, 0, u64::MAX)?;
                }
            }
            if !progress.closed && (matches!(frame, frame::Frame::ResetStream(_)) || progress.stopped
                || matches!(self.recv.get(&id), Some(Some(StreamRecv::Free(_))))) {
                recv.check_inventory(Some(meter))?;
            }
            let (credit, refund, notify, opened) = progress.apply(&frame, received, max_data)?;
            received = received.saturating_add(credit);
            max_data = max_data.saturating_add(refund.saturating_sub(shrink));
            shrink = shrink.saturating_sub(refund);
            if opened {
                let next = &mut next_remote[id.dir() as usize];
                if id.initiator() == self.side || id.index() < *next {
                    events += usize::from(notify);
                } else {
                    *next = id.index() + 1;
                }
            }
        }
        Ok(events)
    }
    #[cfg(test)]
    pub(in crate::connection) fn prepare_packet_streams(
        &mut self, payload: &Bytes, meter: &BackingMeter,
    ) -> Result<Option<PacketReceives>, TransportError> {
        self.prepare_packet_streams_with_retirements(payload, meter, 0)
    }
    pub(in crate::connection) fn prepare_packet_streams_with_retirements(
        &mut self,
        payload: &Bytes,
        meter: &BackingMeter,
        ack_retirements: usize,
    ) -> Result<Option<PacketReceives>, TransportError> {
        let mut plans = Stock::<StreamPlan>::default();
        let mut retirements = ack_retirements;
        for frame in frame::Iter::new(payload.clone())? {
            let frame = frame?;
            let Some(id) = receive_id(&frame) else { continue; };
            if plans.slice().iter().flatten().any(|p| p.id == id) { continue; }
            let mut receiver = self.recv.contains_key(&id).then(|| Receiver::prepare(self, id)).transpose()?;
            let receive = if let Some(receiver) = &mut receiver {
                let slot = self.recv.get_mut(&id).ok_or_else(Receiver::error)?;
                let recv = receiver.target(slot)?;
                let reset = !recv.is_receiving() || recv.stopped;
                // Do not collect DATA: the iterator below holds at most a frame
                // at a time. RESET cuts off all subsequent assembler operations.
                let eligible = || {
                    let mut terminal = reset;
                    frame::Iter::new(payload.clone()).into_iter().flatten().filter_map(move |f| match f {
                        Ok(frame::Frame::ResetStream(s)) if s.id == id => { terminal = true; None }
                        Ok(frame::Frame::Stream(s)) if s.id == id && !terminal => Some(s),
                        _ => None,
                    })
                };
                if recv.stopped && frame::Iter::new(payload.clone())?.any(|f| matches!(&f,
                    Ok(frame::Frame::ResetStream(s)) if s.id == id)
                    || matches!(&f, Ok(frame::Frame::Stream(s)) if s.id == id && s.fin)) {
                    retirements += 1;
                }
                match eligible().count() {
                    0 => ReceivePlan::Control,
                    1 => {
                        let stream = eligible().next().ok_or_else(Receiver::error)?;
                        // Wire-order protocol validation already includes credits
                        // returned by earlier RESET/stopped frames in this packet.
                        let Some(plan) = PacketReceive::prepare(recv, stream, payload.len(), 0, u64::MAX, meter)? else { return Ok(None); };
                        ReceivePlan::Single(Some(plan))
                    }
                    _ => match BatchInsertion::prepare(&recv.assembler, eligible().map(|s| (s.offset, s.data, payload.len())), meter) {
                        Ok(batch) => ReceivePlan::Batch(batch),
                        Err(InsertError::Capacity(MeteredCopyError::BudgetFull)) => return Ok(None),
                        Err(error) => return Err(receive_error(error)),
                    },
                }
            } else { ReceivePlan::Control };
            match plans.push(StreamPlan { id, receive, receiver }, meter) {
                Ok(()) => {}
                Err(MeteredCopyError::BudgetFull) => return Ok(None),
                Err(error) => return Err(receive_error(InsertError::Capacity(error))),
            }
        }
        let retirement = retirement::Prepared::prepare(self, retirements)
            .map_err(|error| receive_error(InsertError::Capacity(error)))?;
        Ok(Some(PacketReceives { plans, retirement }))
    }
}
fn receive_id(frame: &frame::Frame) -> Option<StreamId> {
    match frame {
        frame::Frame::Stream(s) => Some(s.id),
        frame::Frame::ResetStream(s) => Some(s.id),
        _ => None,
    }
}

impl PacketReceives {
    pub(in crate::connection) fn install(&mut self, streams: &mut StreamsState) -> Result<(), TransportError> {
        if let Some(prepared) = self.retirement.take() { prepared.install(streams)?; }
        Ok(())
    }
    pub(in crate::connection) fn commit_reset(&mut self, streams: &mut StreamsState, frame: frame::ResetStream)
        -> Result<ShouldTransmit, TransportError>
    {
        self.install(streams)?;
        if let Some(slot) = streams.recv.get_mut(&frame.id) {
            let plan = self.plans.slice_mut().iter_mut().flatten().find(|p| p.id == frame.id).ok_or_else(Receiver::error)?;
            plan.receiver.as_mut().ok_or_else(Receiver::error)?.install(slot)?;
        }
        streams.received_reset(frame)
    }
    pub(in crate::connection) fn commit_frame(
        &mut self,
        streams: &mut StreamsState,
        frame: frame::Stream,
    ) -> Result<ShouldTransmit, TransportError> {
        self.install(streams)?;
        let active = streams.recv.get(&frame.id).is_some_and(|s| match s {
            None => true,
            Some(StreamRecv::Free(r) | StreamRecv::Open(r)) => r.is_receiving() && !r.stopped,
        });
        if !active {
            // No assembler insertion survives RESET/stopped/closed state.
            // Capacity for any original retirement is already installed.
            let meter = streams.native_meter().cloned();
            return streams.received_checked(frame, 0, meter.as_ref());
        }
        let plan = self
            .plans
            .slice_mut()
            .iter_mut()
            .flatten()
            .find(|p| p.id == frame.id)
            .ok_or_else(|| TransportError::INTERNAL_ERROR("packet receive plan missing"))?;
        let slot = streams
            .recv
            .get_mut(&frame.id)
            .ok_or_else(|| TransportError::INTERNAL_ERROR("prepared receive slot missing"))?;
        plan.receiver.as_mut().ok_or_else(Receiver::error)?.install(slot)?;
        match &mut plan.receive {
            ReceivePlan::Control => Err(Receiver::error()),
            ReceivePlan::Single(prepared) => {
                let prepared = prepared.take().ok_or_else(|| {
                    TransportError::INTERNAL_ERROR("packet receive consumed twice")
                })?;
                streams.commit_packet_stream(prepared).map(|()| ShouldTransmit(false))
            }
            ReceivePlan::Batch(batch) => {
                let Some(Some(StreamRecv::Open(recv))) = streams.recv.get_mut(&frame.id) else {
                    return Err(TransportError::INTERNAL_ERROR(
                        "prepared receive instance changed",
                    ));
                };
                let (end, credit) = recv.validate_ingest(
                    frame.offset,
                    frame.data.len(),
                    frame.fin,
                    streams.data_recvd,
                    streams.local_max_data,
                    None,
                )?;
                batch
                    .commit_next(&mut recv.assembler)
                    .map_err(receive_error)?;
                recv.commit_received_metadata(end, frame.fin);
                streams.data_recvd += credit;
                streams.on_stream_frame(true, frame.id);
                Ok(ShouldTransmit(false))
            }
        }
    }
}

/// Custody of one original stream instance. Existing cached Boxes stay in place
/// through preparation; a new Box stays unpublished at its stable address.
enum Receiver {
    Existing { pointer: usize, free: bool },
    New(Box<Recv>),
    Installed(usize),
}
impl Receiver {
    fn prepare(streams: &StreamsState, id: StreamId) -> Result<Self, TransportError> {
        match streams.recv.get(&id) {
            Some(None) => Recv::try_new(streams.stream_receive_window)
                .map(Self::New)
                .map_err(|error| receive_error(InsertError::Capacity(error))),
            Some(Some(StreamRecv::Free(recv))) => Ok(Self::Existing {
                pointer: recv.as_ref() as *const Recv as usize,
                free: true,
            }),
            Some(Some(StreamRecv::Open(recv))) => Ok(Self::Existing {
                pointer: recv.as_ref() as *const Recv as usize,
                free: false,
            }),
            None => Err(Self::error()),
        }
    }
    fn target<'a>(
        &'a mut self,
        slot: &'a mut Option<StreamRecv>,
    ) -> Result<&'a mut Recv, TransportError> {
        match self {
            Self::New(recv) if slot.is_none() => Ok(recv),
            Self::Existing { pointer, free } => match slot {
                Some(StreamRecv::Free(recv))
                    if *free && recv.as_ref() as *const Recv as usize == *pointer =>
                {
                    Ok(recv)
                }
                Some(StreamRecv::Open(recv))
                    if !*free && recv.as_ref() as *const Recv as usize == *pointer =>
                {
                    Ok(recv)
                }
                _ => Err(Self::error()),
            },
            _ => Err(Self::error()),
        }
    }
    fn install(&mut self, slot: &mut Option<StreamRecv>) -> Result<(), TransportError> {
        if let Self::Installed(pointer) = self {
            return match slot {
                Some(StreamRecv::Open(recv))
                    if recv.as_ref() as *const Recv as usize == *pointer =>
                {
                    Ok(())
                }
                _ => Err(Self::error()),
            };
        }
        let pointer = self.target(slot)? as *mut Recv as usize;
        match std::mem::replace(self, Self::Installed(pointer)) {
            Self::New(recv) => *slot = Some(StreamRecv::Open(recv)),
            Self::Existing { free: true, .. } => {
                let Some(StreamRecv::Free(recv)) = slot.take() else {
                    return Err(Self::error());
                };
                *slot = Some(StreamRecv::Open(recv));
            }
            Self::Existing { free: false, .. } => {}
            Self::Installed(_) => return Err(Self::error()),
        }
        Ok(())
    }
    fn error() -> TransportError {
        TransportError::INTERNAL_ERROR("prepared receive instance changed")
    }
}

#[cfg(test)]
#[path = "packet_receive/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "packet_receive/lifecycle_tests.rs"]
mod lifecycle_tests;

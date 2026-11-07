use super::*;

impl StreamsState {
    pub(in crate::connection) fn actual_layouts_for_probe(&self) -> Vec<(Option<bytes::AllocationIdentity>, usize)> {
        let Some(meter) = self.native_meter() else { return Vec::new(); };
        assert!(self.operation_layouts_match(meter));
        let mut layouts = vec![
            (None,
             Layout::array::<PendingStream>(self.pending.streams.capacity()).unwrap().size()),
            (None,
             Layout::array::<StreamEvent>(self.events.capacity()).unwrap().size()),
            (None,
             Layout::array::<StreamId>(self.connection_blocked.capacity()).unwrap().size()),
        ];
        for stream in self.send.values().flatten() {
            layouts.extend(stream.pending.actual_layouts_for_probe());
        }
        layouts
    }
}


impl crate::connection::Connection {
    pub(crate) fn damaged_cached_recv_for_probe(&mut self, id: StreamId) -> bytes::BackingBudgetReservation {
        let meter = self.streams.native_meter().unwrap().clone();
        let slot = self.streams.recv.get_mut(&id).unwrap();
        let recv = get_or_insert_recv(self.streams.stream_receive_window)(slot);
        let payload = bytes::BytesMut::try_copy_from_slice_metered(b"x", &meter).unwrap().freeze();
        for offset in 0..130 {
            recv.ingest_checked(frame::Stream { id, offset, fin: false, data: payload.clone() },
                1, 0, u64::MAX, Some(&meter)).unwrap();
        }
        let original = slot.take().unwrap();
        // Execute the actual cache transition on the original Box, then retain
        // it under the assigned stream slot exactly as insert() does.
        *slot = Some(original.free(self.streams.stream_receive_window).unwrap());
        let Some(StreamRecv::Free(recv)) = slot else { panic!("actual cached Recv") };
        recv.assembler.take_page_allowance_for_probe()
    }
    pub(crate) fn orphan_sparse_recv_for_probe(&mut self, id: StreamId) {
        let meter = self.streams.native_meter().unwrap().clone();
        let slot = self.streams.recv.get_mut(&id).unwrap();
        let recv = get_or_insert_recv(self.streams.stream_receive_window)(slot);
        let payload = bytes::BytesMut::try_copy_from_slice_metered(b"x", &meter).unwrap().freeze();
        recv.ingest_checked(frame::Stream { id, offset: 10, fin: false, data: payload },
            1, 0, u64::MAX, Some(&meter)).unwrap();
        recv.assembler.ensure_ordering(false).unwrap();
        recv.assembler.lose_root_with_sparse_stock_for_probe();
    }
    pub(crate) fn foreign_stopped_recv_for_probe(&mut self, id: StreamId, meter: &bytes::BackingMeter) {
        let mut recv = Recv::new(self.streams.stream_receive_window);
        let payload = bytes::BytesMut::try_copy_from_slice_metered(b"x", meter).unwrap().freeze();
        recv.ingest_checked(frame::Stream { id, offset: 0, fin: false, data: payload },
            1, 0, u64::MAX, Some(meter)).unwrap();
        recv.stop().unwrap();
        self.streams.recv.insert(id, Some(StreamRecv::Open(recv)));
    }
    pub(crate) fn damaged_tail_recv_for_probe(&mut self, id: StreamId) -> bytes::Bytes {
        let meter = self.streams.native_meter().unwrap().clone();
        let slot = self.streams.recv.get_mut(&id).unwrap();
        let recv = get_or_insert_recv(self.streams.stream_receive_window)(slot);
        let payload = bytes::BytesMut::try_copy_from_slice_metered(b"x", &meter).unwrap().freeze();
        for offset in 1..131 {
            recv.ingest_checked(frame::Stream { id, offset, fin: false, data: payload.clone() },
                1, 0, u64::MAX, Some(&meter)).unwrap();
        }
        recv.assembler.take_actual_tail_for_probe()
    }
    pub(crate) fn recv_settlement_snapshot_for_probe(&self, id: StreamId) -> (usize, bool, u64, u64, usize, usize) {
        let (recv, free) = match self.streams.recv.get(&id).unwrap().as_ref().unwrap() {
            StreamRecv::Free(recv) => (recv, true),
            StreamRecv::Open(recv) => (recv, false),
        };
        (recv.as_ref() as *const Recv as usize, free, self.streams.local_max_data,
            self.streams.data_recvd, self.streams.free_recv.len(),
            self.spaces[crate::packet::SpaceId::Data].pending.stop_sending.len())
    }
}


#[test]
fn actual_receive_layout_cached_and_open_instance_are_the_same_box() {
    eprintln!("receive-layout Recv={} StreamRecv={} Recv-times-F4096={} Recv-times-two-F4096={} ",
        std::mem::size_of::<Recv>(), std::mem::size_of::<StreamRecv>(),
        std::mem::size_of::<Recv>() * 4096, std::mem::size_of::<Recv>() * 8192);
    let recv = Recv::new(65536);
    let pointer = recv.as_ref() as *const Recv;
    let cached = StreamRecv::Open(recv).free(65536).unwrap();
    assert_eq!(cached.into_inner().as_ref() as *const Recv, pointer);
}

impl crate::Connection {
    pub(crate) fn packet_receive_instances_snapshot_for_probe(&self) -> String {
        let mut slots: Vec<_> = self.streams.recv.iter().map(|(id, recv)| {
            let value = recv.as_ref().map(|recv| match recv {
                StreamRecv::Free(value) => (false, value.as_ref() as *const Recv as usize),
                StreamRecv::Open(value) => (true, value.as_ref() as *const Recv as usize),
            });
            (*id, value)
        }).collect();
        slots.sort_unstable_by_key(|(id, _)| *id);
        format!("{:?}", (slots, self.streams.recv.capacity(), self.streams.free_recv.len(),
            self.streams.opened, self.streams.next_remote, self.streams.next_reported_remote,
            &self.streams.events, self.streams.events.capacity(), self.streams.data_recvd))
    }
}

impl crate::Connection {
    pub(crate) fn receive_slot_for_probe(&self, id: StreamId) -> Option<(u8, usize)> {
        self.streams.recv.get(&id).map(|slot| match slot {
            None => (0, 0),
            Some(StreamRecv::Free(recv)) => (1, recv.as_ref() as *const Recv as usize),
            Some(StreamRecv::Open(recv)) => (2, recv.as_ref() as *const Recv as usize),
        })
    }
}

impl crate::Connection {
    pub(crate) fn stream_event_slots_for_probe(&self) -> (usize, usize) {
        (self.streams.events.len(), self.streams.events.capacity())
    }
}

impl crate::Connection {
    pub(crate) fn packet_send_controls_snapshot_for_probe(&self) -> String {
        let state = &self.streams;
        let mut slots: Vec<_> = state.send.iter().map(|(id, slot)| (*id, slot.as_ref().map(|send|
            (send.as_ref() as *const Send as usize, send.max_data, send.state,
             send.connection_blocked, send.pending.offset(), send.stop_reason)))).collect();
        slots.sort_unstable_by_key(|(id, _)| *id);
        format!("{:?}", (slots, state.send.capacity(), state.max, state.max_data,
            state.data_sent, state.unacked_data, &state.connection_blocked,
            state.connection_blocked.capacity()))
    }
}

//! Differential packet receive lifecycle against the original stream handler.
use super::{tests::state, *};
fn id(n: u64) -> StreamId { StreamId::new(Side::Server, Dir::Uni, n) }
fn stream(out: &mut Vec<u8>, n: u64, offset: u64, fin: bool) {
    frame::StreamMeta { id: id(n), offsets: offset..offset + 1, fin }.encode(true, out);
    out.push(b'x');
}
fn reset(out: &mut Vec<u8>, n: u64, offset: u32) {
    frame::ResetStream { id: id(n), error_code: 42u32.into(), final_offset: offset.into() }.encode(out);
}
fn wire(out: &[u8], meter: &BackingMeter) -> Bytes {
    bytes::BytesMut::try_copy_from_slice_metered(out, meter).unwrap().freeze()
}
fn original(s: &mut StreamsState, data: &Bytes, meter: &BackingMeter) {
    for f in frame::Iter::new(data.clone()).unwrap() {
        match f.unwrap() {
            frame::Frame::Stream(f) => { s.received_checked(f, data.len(), Some(meter)).unwrap(); }
            frame::Frame::ResetStream(f) => { s.received_reset(f).unwrap(); }
            _ => unreachable!(),
        }
    }
}
fn stop(s: &mut StreamsState, n: u64) {
    let r = s.recv.get_mut(&id(n)).unwrap().as_mut().unwrap().as_open_recv_mut().unwrap();
    let (credits, _) = r.stop().unwrap();
    s.add_read_credits(credits);
}
fn pointer(s: &StreamsState, n: u64) -> usize {
    match s.recv.get(&id(n)).unwrap().as_ref().unwrap() {
        StreamRecv::Free(r) | StreamRecv::Open(r) => r.as_ref() as *const Recv as usize,
    }
}
fn equivalent(a: &StreamsState, b: &StreamsState) {
    assert_eq!((a.data_recvd, a.local_max_data, a.receive_window_shrink_debt, a.next_remote, a.opened,
        a.max_remote, a.allocated_remote_count, a.free_recv.len()),
        (b.data_recvd, b.local_max_data, b.receive_window_shrink_debt, b.next_remote, b.opened,
        b.max_remote, b.allocated_remote_count, b.free_recv.len()));
    assert_eq!(format!("{:?}", a.events), format!("{:?}", b.events));
    assert_eq!(a.recv.len(), b.recv.len());
    for (id, ar) in &a.recv {
        let br = b.recv.get(id).unwrap();
        match (ar, br) {
            (None, None) => {}
            (Some(StreamRecv::Open(a)), Some(StreamRecv::Open(b))) |
            (Some(StreamRecv::Free(a)), Some(StreamRecv::Free(b))) => {
                assert_eq!((a.end, a.stopped, a.reset_code(), a.is_receiving(), a.assembler.progress()),
                    (b.end, b.stopped, b.reset_code(), b.is_receiving(), b.assembler.progress()));
            }
            _ => panic!("receiver disposition differs for {id}"),
        }
    }
}
#[test]
fn reset_stopped_fin_closed_and_duplicate_match_original_at_global_full() {
    for stopped_fin in [false, true] {
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let mut target = state(&meter);
        let mut baseline = state(&meter);
        let mut init = Vec::new();
        stream(&mut init, 0, 0, false);
        let init = wire(&init, &meter);
        for s in [&mut target, &mut baseline] { original(s, &init, &meter); stop(s, 0); }
        let old = pointer(&target, 0);
        let next = target.max_remote[Dir::Uni as usize];
        let mut data = Vec::new();
        if stopped_fin { stream(&mut data, 0, 1, true); } else { reset(&mut data, 0, 2); }
        // Closed frames must be ignored even when their final sizes disagree.
        stream(&mut data, 0, 100, true);
        reset(&mut data, 0, 0);
        // A fresh receiver has two accepted STREAMs then RESET; later STREAM
        // is ignored, while duplicate RESET still validates its final size.
        stream(&mut data, 1, 0, false);
        stream(&mut data, 1, 2, false);
        reset(&mut data, 1, 4);
        stream(&mut data, 1, 100, true);
        reset(&mut data, 1, 4);
        stream(&mut data, 2, 0, true);
        let data = wire(&data, &meter);
        let events = target.check_packet_streams(&data, &meter).unwrap();
        let before = budget.used();
        let ops = target.prepare_operations(events, 0, 0).unwrap();
        let plans = target.prepare_packet_streams(&data, &meter).unwrap().unwrap();
        drop((plans, ops));
        assert_eq!(budget.used(), before);
        assert_eq!(pointer(&target, 0), old);
        equivalent(&target, &baseline);
        let full = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        assert!(target.prepare_packet_streams(&data, &meter).unwrap().is_none());
        equivalent(&target, &baseline);
        assert_eq!(pointer(&target, 0), old);
        drop(full);
        assert_eq!(budget.used(), before);
        original(&mut baseline, &data, &meter);
        let ops = target.prepare_operations(events, 0, 0).unwrap();
        let mut plans = target.prepare_packet_streams(&data, &meter).unwrap().unwrap();
        let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        plans.install(&mut target).unwrap();
        target.commit_operations(ops).unwrap();
        let capacities = (target.recv.capacity(), target.send.capacity(), target.free_recv.capacity());
        for f in frame::Iter::new(data.clone()).unwrap() {
            match f.unwrap() {
                frame::Frame::Stream(f) => { plans.commit_frame(&mut target, f).unwrap(); }
                frame::Frame::ResetStream(f) => { plans.commit_reset(&mut target, f).unwrap(); }
                _ => unreachable!(),
            }
        }
        equivalent(&target, &baseline);
        assert_eq!(pointer(&target, next), old, "original retired Box reused for next grant");
        assert_eq!(capacities, (target.recv.capacity(), target.send.capacity(), target.free_recv.capacity()));
        drop((pressure, plans));
        // The next real grant uses the original cached receiver, including its
        // retained heap. Its subsequent packet also commits at global Full.
        let mut next_wire = Vec::new(); stream(&mut next_wire, next, 0, true);
        let next_wire = wire(&next_wire, &meter);
        let events = target.check_packet_streams(&next_wire, &meter).unwrap();
        let ops = target.prepare_operations(events, 0, 0).unwrap();
        let mut plans = target.prepare_packet_streams(&next_wire, &meter).unwrap().unwrap();
        original(&mut baseline, &next_wire, &meter);
        let full = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        target.commit_operations(ops).unwrap();
        let frame::Frame::Stream(f) = frame::Iter::new(next_wire.clone()).unwrap().next().unwrap().unwrap() else { unreachable!() };
        plans.commit_frame(&mut target, f).unwrap();
        assert_eq!(pointer(&target, next), old);
        equivalent(&target, &baseline);
        drop((full, plans, next_wire, target, baseline, init, data));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}
#[test]
fn reset_credit_and_shrink_debt_are_used_by_later_stream_validation() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut target = state(&meter);
    let mut baseline = state(&meter);
    for s in [&mut target, &mut baseline] { s.local_max_data = 2; s.sent_max_data = 0u32.into(); s.receive_window_shrink_debt = 1; }
    let mut out = Vec::new();
    reset(&mut out, 0, 2);
    stream(&mut out, 1, 0, false);
    let data = wire(&out, &meter);
    let events = target.check_packet_streams(&data, &meter).unwrap();
    let operations = target.prepare_operations(events, 0, 0).unwrap();
    let mut plans = target.prepare_packet_streams(&data, &meter).unwrap().unwrap();
    original(&mut baseline, &data, &meter);
    target.commit_operations(operations).unwrap();
    for f in frame::Iter::new(data.clone()).unwrap() {
        match f.unwrap() {
            frame::Frame::ResetStream(f) => { plans.commit_reset(&mut target, f).unwrap(); }
            frame::Frame::Stream(f) => { plans.commit_frame(&mut target, f).unwrap(); }
            _ => unreachable!(),
        }
    }
    equivalent(&target, &baseline);
    let mut bad = Vec::new(); reset(&mut bad, 0, 3);
    assert!(target.check_packet_streams(&wire(&bad, &meter), &meter).is_err());
    drop((plans, data, target, baseline));
    assert_eq!(budget.used(), 0);
}

use super::*;

pub(super) fn state(meter: &BackingMeter) -> StreamsState {
    StreamsState::new(
        Side::Client,
        128u32.into(),
        128u32.into(),
        1 << 20,
        (1u32 << 20).into(),
        (1u32 << 20).into(),
        Some(meter.clone()),
    )
}
fn payload(inputs: &[(u64, u64, bool)], meter: &BackingMeter) -> Bytes {
    let mut encoded = Vec::new();
    for &(index, offset, fin) in inputs {
        frame::StreamMeta {
            id: StreamId::new(Side::Server, Dir::Uni, index),
            offsets: offset..offset + 1,
            fin,
        }
        .encode(true, &mut encoded);
        encoded.push(b'a' + offset as u8);
    }
    bytes::BytesMut::try_copy_from_slice_metered(&encoded, meter)
        .unwrap()
        .freeze()
}
fn frames(data: &Bytes) -> impl Iterator<Item = frame::Stream> {
    frame::Iter::new(data.clone())
        .unwrap()
        .map(|frame| match frame.unwrap() {
            frame::Frame::Stream(stream) => stream,
            _ => unreachable!(),
        })
}

#[test]
fn first_instances_cancel_then_commit_many_frames_at_full_with_original_event_order() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut inputs: Vec<_> = (0..12).rev().map(|id| (id, 0, false)).collect();
    inputs.extend([(11, 1, true), (0, 1, true)]);
    let data = payload(&inputs, &meter);
    let mut target = state(&meter);
    let baseline = budget.used();
    let events = target.check_packet_streams(&data, &meter).unwrap();
    assert_eq!(
        events, 13,
        "first high stream opens lower ids; all subsequent frames are readable"
    );
    let operations = target.prepare_operations(events, 0, 0).unwrap();
    let plans = target
        .prepare_packet_streams(&data, &meter)
        .unwrap()
        .unwrap();
    assert!(budget.used() > baseline);
    assert!(target.recv.values().all(Option::is_none));
    assert_eq!(target.next_remote, [0, 0]);
    drop((plans, operations));
    assert_eq!(budget.used(), baseline);
    assert!(target.recv.values().all(Option::is_none));

    let mut original = state(&meter);
    for frame in frames(&data) {
        original
            .received_checked(frame, data.len(), Some(&meter))
            .unwrap();
    }
    let operations = target.prepare_operations(events, 0, 0).unwrap();
    let mut plans = target
        .prepare_packet_streams(&data, &meter)
        .unwrap()
        .unwrap();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    target.commit_operations(operations).unwrap();
    for frame in frames(&data) {
        plans.commit_frame(&mut target, frame).unwrap();
    }
    assert_eq!(target.data_recvd, original.data_recvd);
    assert_eq!(target.next_remote, original.next_remote);
    assert_eq!(target.opened, original.opened);
    assert_eq!(
        format!("{:?}", target.events),
        format!("{:?}", original.events)
    );
    assert_eq!(target.events.len(), 13);
    drop(pressure);
    for index in 0..12 {
        let id = StreamId::new(Side::Server, Dir::Uni, index);
        let a = target
            .recv
            .get_mut(&id)
            .unwrap()
            .as_mut()
            .unwrap()
            .as_open_recv_mut()
            .unwrap();
        let b = original
            .recv
            .get_mut(&id)
            .unwrap()
            .as_mut()
            .unwrap()
            .as_open_recv_mut()
            .unwrap();
        assert_eq!(a.assembler.progress(), b.assembler.progress());
        loop {
            match (
                a.assembler.read_checked(1, true).unwrap(),
                b.assembler.read_checked(1, true).unwrap(),
            ) {
                (Some(a), Some(b)) => assert_eq!((a.offset, a.bytes), (b.offset, b.bytes)),
                (None, None) => break,
                _ => panic!("first instance changed original stream delivery"),
            }
        }
    }
    drop((plans, target, original, data));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn later_new_instance_full_drops_all_preparations_without_materializing_earlier_slot() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let one = payload(&[(0, 0, false)], &meter);
    let two = payload(&[(0, 0, false), (1, 0, false)], &meter);
    let mut target = state(&meter);
    let baseline = budget.used();
    let first = target
        .prepare_packet_streams(&one, &meter)
        .unwrap()
        .unwrap();
    let one_cost = budget.used() - baseline;
    assert!(one_cost > 0);
    drop(first);
    assert_eq!(budget.used(), baseline);
    let pressure = budget
        .try_reserve(budget.limit() - budget.used() - one_cost)
        .unwrap();
    let pressured = budget.used();
    assert!(target
        .prepare_packet_streams(&two, &meter)
        .unwrap()
        .is_none());
    assert_eq!(budget.used(), pressured);
    assert!(target.recv.values().all(Option::is_none));
    assert_eq!(target.next_remote, [0, 0]);
    assert_eq!(target.data_recvd, 0);
    assert!(target.events.is_empty());
    drop((pressure, target, one, two));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

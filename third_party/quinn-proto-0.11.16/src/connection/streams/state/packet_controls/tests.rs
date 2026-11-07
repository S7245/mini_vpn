use super::*;

fn state(meter: &BackingMeter) -> StreamsState {
    StreamsState::new(
        Side::Client,
        16u32.into(),
        16u32.into(),
        1024,
        1024u32.into(),
        1024u32.into(),
        Some(meter.clone()),
    )
}
fn credit(data: &mut Vec<u8>, index: u64, limit: u64) {
    data.write(frame::FrameType::MAX_STREAM_DATA);
    data.write(StreamId::new(Side::Server, Dir::Bi, index));
    data.write_var(limit);
}
fn apply(state: &mut StreamsState, data: &Bytes, meter: &BackingMeter) {
    for frame in frame::Iter::new(data.clone()).unwrap().map(Result::unwrap) {
        match frame {
            frame::Frame::MaxData(n) => state.received_max_data(n),
            frame::Frame::StopSending(frame::StopSending { id, error_code }) => state.received_stop_sending(id, error_code).unwrap(),
            frame::Frame::MaxStreamData { id, offset } => {
                state.received_max_stream_data(id, offset).unwrap()
            }
            frame::Frame::MaxStreams { dir, count } => {
                state.received_max_streams(dir, count).unwrap()
            }
            frame::Frame::Stream(frame) => {
                state
                    .received_checked(frame, data.len(), Some(meter))
                    .unwrap();
            }
            _ => unreachable!(),
        }
    }
}
fn snapshot(state: &StreamsState) -> String {
    let mut send: Vec<_> = state
        .send
        .iter()
        .map(|(id, value)| {
            (
                *id,
                value
                    .as_ref()
                    .map(|s| (s.max_data, s.state, s.connection_blocked, s.stop_reason)),
            )
        })
        .collect();
    send.sort_unstable_by_key(|(id, _)| *id);
    format!(
        "{:?}",
        (
            send,
            state.max,
            state.max_data,
            state.next_remote,
            state.opened,
            &state.events,
            &state.connection_blocked
        )
    )
}

#[test]
fn twelve_new_senders_credit_order_and_repeated_grants_cancel_then_match_original_at_full() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut encoded = Vec::new();
    for index in 0..12 {
        if index == 6 {
            encoded.write(frame::FrameType::MAX_DATA);
            encoded.write_var(64);
        }
        credit(&mut encoded, index, 1);
    }
    for index in 0..12 {
        credit(&mut encoded, index, 2);
    }
    for count in [1, 2, 2, 1, 3] {
        encoded.write(frame::FrameType::MAX_STREAMS_BIDI);
        encoded.write_var(count);
    }
    let data = bytes::BytesMut::try_copy_from_slice_metered(&encoded, &meter)
        .unwrap()
        .freeze();
    let mut target = state(&meter);
    let initial = snapshot(&target);
    let baseline = budget.used();
    let controls = target.prepare_packet_controls(&data, &meter).unwrap();
    assert_eq!((controls.events, controls.blocked), (9, 6));
    let operations = target
        .prepare_operations(controls.events, 0, controls.blocked)
        .unwrap();
    assert!(
        budget.used() > baseline,
        "real Stock spill plus event/blocked layouts"
    );
    assert!(target.send.values().all(Option::is_none));
    drop((controls, operations));
    assert_eq!(snapshot(&target), initial);
    assert_eq!(budget.used(), baseline);

    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert!(matches!(
        target.prepare_packet_controls(&data, &meter),
        Err(MeteredCopyError::BudgetFull)
    ));
    assert_eq!(
        snapshot(&target),
        initial,
        "late Stock Full installs none of the first senders"
    );
    assert_eq!(budget.used(), budget.limit());
    drop(pressure);

    let mut original = state(&meter);
    apply(&mut original, &data, &meter);
    let controls = target.prepare_packet_controls(&data, &meter).unwrap();
    let operations = target
        .prepare_operations(controls.events, 0, controls.blocked)
        .unwrap();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    controls.install(&mut target).unwrap();
    target.commit_operations(operations).unwrap();
    apply(&mut target, &data, &meter);
    assert_eq!(snapshot(&target), snapshot(&original));
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    while let Some(event) = target.poll() {
        actual.push(event);
    }
    while let Some(event) = original.poll() {
        expected.push(event);
    }
    assert_eq!(actual, expected);
    assert_eq!(
        actual.len(),
        16,
        "Opened, 12 Writable, 3 Available in original order"
    );
    drop(pressure);
    drop((target, original, data));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn credit_before_first_stream_prepares_readable_in_original_wire_order() {
    for credit_first in [false, true] {
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let mut encoded = Vec::new();
        if credit_first {
            credit(&mut encoded, 0, 1);
        }
        let id = StreamId::new(Side::Server, Dir::Bi, 0);
        frame::StreamMeta {
            id,
            offsets: 0..1,
            fin: false,
        }
        .encode(true, &mut encoded);
        encoded.push(b'x');
        if !credit_first {
            credit(&mut encoded, 0, 1);
        }
        let data = bytes::BytesMut::try_copy_from_slice_metered(&encoded, &meter)
            .unwrap()
            .freeze();
        let mut target = state(&meter);
        let mut original = state(&meter);
        apply(&mut original, &data, &meter);
        let controls = target.prepare_packet_controls(&data, &meter).unwrap();
        let receive_events = target.check_packet_streams(&data, &meter).unwrap();
        assert_eq!(receive_events, usize::from(credit_first));
        let operations = target
            .prepare_operations(controls.events + receive_events, 0, controls.blocked)
            .unwrap();
        let mut receive = target
            .prepare_packet_streams(&data, &meter)
            .unwrap()
            .unwrap();
        let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        controls.install(&mut target).unwrap();
        target.commit_operations(operations).unwrap();
        for frame in frame::Iter::new(data.clone()).unwrap().map(Result::unwrap) {
            match frame {
                frame::Frame::Stream(frame) => { receive.commit_frame(&mut target, frame).unwrap(); },
                frame::Frame::MaxStreamData { id, offset } => {
                    target.received_max_stream_data(id, offset).unwrap()
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(snapshot(&target), snapshot(&original));
        drop(pressure);
        drop((receive, target, original, data));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn twelve_new_senders_repeated_stop_and_credit_share_prepared_instances_and_events() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut encoded = Vec::new();
    for index in 0..12 {
        for _ in 0..2 {
            encoded.write(frame::FrameType::STOP_SENDING);
            encoded.write(StreamId::new(Side::Server, Dir::Bi, index));
            encoded.write_var(42);
        }
        credit(&mut encoded, index, 1);
    }
    let data = bytes::BytesMut::try_copy_from_slice_metered(&encoded, &meter).unwrap().freeze();
    let mut target = state(&meter);
    let before = snapshot(&target); let used = budget.used();
    let cancelled = target.prepare_packet_controls(&data, &meter).unwrap();
    assert_eq!(cancelled.events, 12);
    drop(cancelled); assert_eq!(snapshot(&target), before); assert_eq!(budget.used(), used);
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    assert!(matches!(target.prepare_packet_controls(&data, &meter), Err(MeteredCopyError::BudgetFull)));
    assert_eq!(snapshot(&target), before); drop(pressure);
    let mut original = state(&meter); apply(&mut original, &data, &meter);
    let controls = target.prepare_packet_controls(&data, &meter).unwrap();
    let operations = target.prepare_operations(controls.events, 0, controls.blocked).unwrap();
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    controls.install(&mut target).unwrap(); target.commit_operations(operations).unwrap();
    apply(&mut target, &data, &meter);
    assert_eq!(snapshot(&target), snapshot(&original));
    drop((pressure, target, original, data)); assert_eq!(budget.used(), 0); assert!(!budget.quarantined());
}

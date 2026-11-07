use super::*;
use crate::connection::application_events;

fn metered(data: &[u8], meter: &BackingMeter) -> Bytes {
    if data.is_empty() {
        return Bytes::new();
    }
    bytes::BytesMut::try_copy_from_slice_metered(data, meter)
        .unwrap()
        .freeze()
}
fn payload(items: &[&[u8]], meter: &BackingMeter) -> Bytes {
    let mut encoded = Vec::new();
    for data in items {
        Datagram {
            data: Bytes::copy_from_slice(data),
        }
        .encode(true, &mut encoded);
    }
    metered(&encoded, meter)
}
fn contents(state: &DatagramState) -> Vec<Vec<u8>> {
    state.incoming.iter().map(|d| d.data.to_vec()).collect()
}

#[test]
fn packet_eviction_cancel_and_full_commit_match_original_fifo_and_notifications() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    // D16: the empty-only case uses a window that fits its 16 one-byte charges.
    for (window, empty_only) in [(16, true), (8, false)] {
        let mut original = DatagramState::new(Some(meter.clone()));
        let mut target = DatagramState::new(Some(meter.clone()));
        if !empty_only {
            for bytes in [b"12".as_slice(), b"", b"3456"] {
                for state in [&mut original, &mut target] {
                    state
                        .received(
                            Datagram {
                                data: metered(bytes, &meter),
                            },
                            &Some(window),
                        )
                        .unwrap();
                }
            }
        }
        let wire = if empty_only {
            payload(&[b"".as_slice(); 16], &meter)
        } else {
            payload(&[b"ABCDEFG", b"", b"xy", b"", b"12345678", b""], &meter)
        };
        let mut expected_notices = 0;
        for frame in frame::Iter::new(wire.clone()).unwrap() {
            let Frame::Datagram(data) = frame.unwrap() else {
                unreachable!()
            };
            expected_notices += usize::from(original.received(data, &Some(window)).unwrap());
        }
        let before = contents(&target);
        let charge = budget.used();
        let checked = target.check_packet(&wire, &Some(window)).unwrap();
        assert_eq!(checked.events, expected_notices);
        drop(target.prepare_packet(checked).unwrap());
        assert_eq!(budget.used(), charge);
        assert_eq!(contents(&target), before);
        let checked = target.check_packet(&wire, &Some(window)).unwrap();
        let mut events = application_events::Queue::new(Some(meter.clone()));
        let event_plan = events.prepare(checked.events, 0).unwrap();
        let plan = target.prepare_packet(checked).unwrap();
        let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        events.commit(event_plan).unwrap();
        target.install_packet(plan).unwrap();
        for frame in frame::Iter::new(wire.clone()).unwrap() {
            let Frame::Datagram(data) = frame.unwrap() else {
                unreachable!()
            };
            if target.receive_prepared(data, &Some(window)).unwrap() {
                events
                    .push_reserved(application_events::QueuedEvent::DatagramReceived, 0)
                    .unwrap();
            }
        }
        let mut notices = 0;
        while let Some(event) = events.pop_front() {
            assert!(matches!(event, crate::Event::DatagramReceived));
            notices += 1;
        }
        assert_eq!(notices, expected_notices);
        assert_eq!(contents(&target), contents(&original));
        assert_eq!(target.recv_buffered, original.recv_buffered);
        drop((pressure, wire, events, target, original));
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn later_illegal_datagram_is_validated_before_full_without_eviction() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut target = DatagramState::new(Some(meter.clone()));
    target
        .received(
            Datagram {
                data: metered(b"original", &meter),
            },
            &Some(8),
        )
        .unwrap();
    let wire = payload(&[b"new", b"oversized"], &meter);
    let before = contents(&target);
    let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    let error = match target.check_packet(&wire, &Some(8)) {
        Err(error) => error,
        Ok(_) => panic!("invalid frame accepted"),
    };
    assert_eq!(error.code, crate::TransportErrorCode::PROTOCOL_VIOLATION);
    assert_eq!(contents(&target), before);
    assert_eq!(target.recv_buffered, 8);
    drop((pressure, wire, target));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

//! Original sender backing: real append/ACK, preparation and exact rollback.
//! No transport, effect policy, receive or whole-session peak claim.
use super::super::*;

#[test]
fn one_segment_sender_retains_one_descriptor_not_geometric_slack() {
    let mut sender = TcpSendWindow::new(TcpWindowLimits::new(32, 1).unwrap());
    sender
        .append(ByteOffset::new(0), Bytes::from_static(b"abc"))
        .unwrap();
    assert_eq!(sender.segments.len(), 1);
    assert_eq!(sender.segments.capacity(), 1);
    assert_eq!(sender.retained_bytes(), 3);
}

#[derive(Debug, PartialEq, Eq)]
struct OriginalWindow {
    snapshot: TcpSendSnapshot,
    capacity: usize,
    slices: [(usize, usize); 2],
    payloads: Vec<(ByteOffset, usize, Vec<u8>)>,
}

fn original(sender: &TcpSendWindow) -> OriginalWindow {
    let (front, back) = sender.segments.as_slices();
    OriginalWindow {
        snapshot: sender.snapshot(),
        capacity: sender.segments.capacity(),
        slices: [
            (front.as_ptr().addr(), front.len()),
            (back.as_ptr().addr(), back.len()),
        ],
        payloads: sender
            .segments
            .iter()
            .map(|segment| {
                (
                    segment.offset,
                    segment.payload.bytes.as_ptr().addr(),
                    segment.payload.bytes.to_vec(),
                )
            })
            .collect(),
    }
}

fn append_byte(sender: &mut TcpSendWindow) {
    sender
        .append(sender.next_sent(), Bytes::from_static(b"x"))
        .unwrap();
}

#[test]
fn wrapped_growth_preserves_payloads_and_ack_retains_reusable_backing() {
    let mut sender = TcpSendWindow::new(TcpWindowLimits::new(32, 5).unwrap());
    for capacity in [1, 2, 4, 4] {
        append_byte(&mut sender);
        assert_eq!(sender.segments.capacity(), capacity);
    }
    sender.acknowledge(ByteOffset::new(2), false).unwrap();
    append_byte(&mut sender);
    append_byte(&mut sender);
    assert!(
        !sender.segments.as_slices().1.is_empty(),
        "real wrapped original"
    );
    let before = original(&sender);
    let prepared = sender
        .prepare_append(ByteOffset::new(6), Bytes::from_static(b"x"))
        .unwrap();
    let PreparedSendDescriptors::Replace(replacement) = &prepared.descriptors else {
        panic!("full wrapped capacity4 must prepare capacity5");
    };
    assert_eq!(original(prepared.window), before);
    assert_eq!(replacement.capacity(), 5);
    assert!(replacement.is_empty());
    assert_eq!(
        growth_peak::<TcpDataSegment>(before.capacity, replacement.capacity()),
        Ok(9 * size_of::<TcpDataSegment>())
    );
    let replacement_pointer = replacement.as_ptr().addr();
    prepared.commit();
    assert_eq!(sender.segments.capacity(), 5);
    assert_eq!(
        sender.segments.as_slices().0.as_ptr().addr(),
        replacement_pointer
    );
    assert_eq!(&original(&sender).payloads[..4], before.payloads.as_slice());
    assert_eq!(sender.retained_bytes(), 5);
    sender.acknowledge(ByteOffset::new(7), false).unwrap();
    assert_eq!(sender.segments.capacity(), 5);
    assert_eq!(sender.retained_bytes(), 0);
    FAIL_NEXT_DESCRIPTOR_ALLOCATION.set(true);
    append_byte(&mut sender);
    assert!(
        FAIL_NEXT_DESCRIPTOR_ALLOCATION.replace(false),
        "retained backing reused"
    );
    assert_eq!(sender.segments.capacity(), 5);
}

#[test]
fn prepared_growth_drop_and_effect_refusal_leave_original_window_exact() {
    use crate::resumable::effect_storage::{
        SessionEffectBatch, fail_next_effect_allocation_for_test,
    };
    let mut sender = TcpSendWindow::new(TcpWindowLimits::new(32, 5).unwrap());
    append_byte(&mut sender);
    let before = original(&sender);
    for refuse_effect in [false, true] {
        let prepared = sender
            .prepare_append(ByteOffset::new(1), Bytes::from_static(b"ab"))
            .unwrap();
        let PreparedSendDescriptors::Replace(replacement) = &prepared.descriptors else {
            panic!("nonempty capacity1 requires replacement");
        };
        assert_eq!(replacement.capacity(), 2);
        assert_eq!(original(prepared.window), before);
        if refuse_effect {
            fail_next_effect_allocation_for_test();
            assert_eq!(
                SessionEffectBatch::empty().prepare_ordinary(),
                Err(EffectStorageError::Reserve)
            );
        }
        drop(prepared);
        assert_eq!(original(&sender), before);
    }
    sender
        .append(ByteOffset::new(1), Bytes::from_static(b"ab"))
        .unwrap();
    assert_eq!(sender.segments.capacity(), 2);
    assert_eq!(original(&sender).payloads[0], before.payloads[0]);
    assert_eq!(sender.next_sent(), ByteOffset::new(3));
    assert_eq!(sender.retained_bytes(), 3);
}

#[test]
fn descriptor_refusal_preserves_original_and_validation_error_priority() {
    let mut sender = TcpSendWindow::new(TcpWindowLimits::new(32, 5).unwrap());
    append_byte(&mut sender);
    let before = original(&sender);
    FAIL_NEXT_DESCRIPTOR_ALLOCATION.set(true);
    assert!(matches!(
        sender.prepare_append(ByteOffset::new(1), Bytes::new()),
        Err(TcpOwnershipError::EmptyData)
    ));
    assert!(matches!(
        sender.prepare_append(ByteOffset::new(2), Bytes::from_static(b"x")),
        Err(TcpOwnershipError::UnexpectedSendOffset { .. })
    ));
    // Defensive accounting-overflow fixture; no claim of a natural corrupt window.
    let backing = sender.retained_backing_bytes;
    sender.retained_backing_bytes = usize::MAX;
    assert!(matches!(
        sender.prepare_append(ByteOffset::new(1), Bytes::from_static(b"x")),
        Err(TcpOwnershipError::InvariantViolation(
            "sender retained backing byte accounting overflow"
        ))
    ));
    sender.retained_backing_bytes = backing;
    assert!(matches!(
        sender.prepare_append(ByteOffset::new(1), Bytes::from_static(b"x")),
        Err(TcpOwnershipError::DescriptorReserve)
    ));
    assert!(!FAIL_NEXT_DESCRIPTOR_ALLOCATION.replace(false));
    assert_eq!(original(&sender), before);
    append_byte(&mut sender);
    assert_eq!(sender.segments.capacity(), 2);
    assert_eq!(original(&sender).payloads[0], before.payloads[0]);
}

#[test]
fn descriptor_layout_and_internal_bound_errors_are_typed_before_allocation() {
    let empty = VecDeque::new();
    FAIL_NEXT_DESCRIPTOR_ALLOCATION.set(true);
    assert!(matches!(
        PreparedSendDescriptors::prepare(&empty, usize::MAX, usize::MAX),
        Err(TcpOwnershipError::DescriptorLayout)
    ));
    assert!(FAIL_NEXT_DESCRIPTOR_ALLOCATION.replace(false));
    assert!(matches!(
        PreparedSendDescriptors::prepare(&empty, 2, 1),
        Err(TcpOwnershipError::InvariantViolation(_))
    ));
}

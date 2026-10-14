use super::super::*;
use std::cell::Cell;
use std::rc::Rc;

#[test]
fn shared_backing_keeps_effect_fault_policy_and_layout_zero_zst_priority() {
    FAIL_NEXT_ALLOCATION.set(true);
    assert!(matches!(
        exact_vec::<u64>(usize::MAX),
        Err(EffectStorageError::Layout)
    ));
    assert_eq!(exact_vec::<u8>(0).unwrap().capacity(), 0);
    assert_eq!(exact_vec::<()>(usize::MAX).unwrap().capacity(), usize::MAX);
    let mut unrelated = allocate_exact_vec::<u64>(3).unwrap();
    assert_eq!(unrelated.capacity(), 3);
    unrelated.extend([1, 2, 3]);
    assert_eq!(unrelated.as_slice(), [1, 2, 3]);
    assert!(matches!(
        exact_vec::<u8>(1),
        Err(EffectStorageError::Reserve)
    ));
    assert!(!FAIL_NEXT_ALLOCATION.replace(false));
    assert_eq!(exact_vec::<u8>(1).unwrap().capacity(), 1);
}

#[test]
fn exact_backing_obeys_alignment_zero_and_zst_layouts() {
    #[repr(align(128))]
    struct Aligned(u8);
    let mut values = exact_vec::<Aligned>(3).unwrap();
    assert_eq!(values.capacity(), 3);
    assert_eq!(values.as_ptr().addr() % 128, 0);
    values.push(Aligned(7));
    assert_eq!(values[0].0, 7);
    assert_eq!(exact_vec::<u64>(0).unwrap().capacity(), 0);
    assert_eq!(exact_vec::<()>(usize::MAX).unwrap().capacity(), usize::MAX);
    assert_eq!(growth_peak::<()>(usize::MAX, usize::MAX), Ok(0));
    assert!(matches!(
        exact_vec::<u64>(usize::MAX),
        Err(EffectStorageError::Layout)
    ));
}

#[test]
fn clipped_levels_reuse_backing_and_check_machine_edges() {
    for maximum in 3..100 {
        for need in 1..=maximum {
            let mut expected = 3;
            while expected < need {
                expected = (expected * 2).min(maximum);
            }
            assert_eq!(next_capacity(0, need, 3, maximum), Ok(expected));
            assert_eq!(next_capacity(expected, need, 3, maximum), Ok(expected));
        }
    }
    assert_eq!(next_capacity(0, usize::MAX, 3, usize::MAX), Ok(usize::MAX));
    assert_eq!(next_capacity(0, 1, 0, 3), Err(EffectStorageError::Bound));
    assert_eq!(next_capacity(0, 4, 3, 3), Err(EffectStorageError::Bound));
    assert_eq!(next_capacity(4, 1, 3, 3), Err(EffectStorageError::Bound));
    assert_eq!(growth_peak::<u64>(3, 6), Ok(72));
}

struct Tracked {
    id: usize,
    drops: Rc<Cell<usize>>,
}
impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn wrapped_original_queue_moves_once_without_clone_or_element_drop() {
    let drops = Rc::new(Cell::new(0));
    let mut values: VecDeque<_> = exact_vec(3).unwrap().into();
    for id in 0..3 {
        values.push_back(Tracked {
            id,
            drops: drops.clone(),
        });
    }
    let first = values.pop_front().unwrap();
    values.push_back(first);
    prepare_deque(&mut values, 4, 3, 10).unwrap();
    assert_eq!(values.capacity(), 6);
    assert_eq!(
        values.iter().map(|item| item.id).collect::<Vec<_>>(),
        [1, 2, 0]
    );
    assert_eq!(drops.get(), 0);
    let pointer = values.as_slices().0.as_ptr();
    prepare_deque(&mut values, 3, 3, 10).unwrap();
    assert_eq!(values.as_slices().0.as_ptr(), pointer);
    prepare_deque(&mut values, 7, 3, 10).unwrap();
    assert_eq!(values.capacity(), 10);
    assert_eq!(drops.get(), 0);
    drop(values);
    assert_eq!(drops.get(), 3);
}

#[test]
fn failed_growth_retains_exact_original_items_and_backing() {
    let drops = Rc::new(Cell::new(0));
    let mut values = exact_vec(1).unwrap();
    values.push(Tracked {
        id: 42,
        drops: drops.clone(),
    });
    let pointer = values.as_ptr();
    FAIL_NEXT_ALLOCATION.set(true);
    assert_eq!(
        prepare_vec(&mut values, 2, 1, 3),
        Err(EffectStorageError::Reserve)
    );
    assert_eq!(values.as_ptr(), pointer);
    assert_eq!(values.capacity(), 1);
    assert_eq!(values[0].id, 42);
    assert_eq!(drops.get(), 0);
    prepare_vec(&mut values, 2, 1, 3).unwrap();
    assert_eq!(values.capacity(), 2);
    assert_eq!(drops.get(), 0);
    drop(values);
    assert_eq!(drops.get(), 1);
}

#[test]
fn zst_and_live_length_cannot_bypass_logical_bound() {
    let mut zeroes = vec![(); 3];
    assert_eq!(
        prepare_vec(&mut zeroes, 2, 1, 4),
        Err(EffectStorageError::Bound)
    );
    assert_eq!(
        prepare_vec(&mut zeroes, 5, 1, 4),
        Err(EffectStorageError::Bound)
    );
    assert_eq!(prepare_vec(&mut zeroes, 3, 1, 4), Ok(()));
    let mut queue: VecDeque<_> = zeroes.into();
    assert_eq!(
        prepare_deque(&mut queue, 3, 0, 4),
        Err(EffectStorageError::Bound)
    );
    assert_eq!(prepare_deque(&mut queue, 3, 1, 4), Ok(()));
}

#[test]
fn failed_batch_keeps_original_prefix_and_first_overflow() {
    let mut batch = SessionEffectBatch::prepared(3).unwrap();
    for _ in 0..3 {
        batch.push(SessionEffect::SessionExpired).unwrap();
    }
    assert_eq!(
        batch.push(SessionEffect::SessionExpired),
        Err(EffectStorageError::Overflow)
    );
    assert_eq!(batch.prepare_ordinary(), Err(EffectStorageError::Overflow));
    assert_eq!(batch.len(), 4);
    assert_eq!(batch.iter().count(), 4);
    let (prefix, overflow) = batch.into_parts();
    assert_eq!(prefix.len(), 3);
    assert!(matches!(overflow, Some(SessionEffect::SessionExpired)));
}

#[test]
fn prepared_tcp_append_survives_effect_reserve_failure_without_mutation() {
    use crate::resumable::ByteOffset;
    use crate::resumable::tcp::{TcpSendWindow, TcpWindowLimits};
    use bytes::Bytes;
    let mut sender = TcpSendWindow::new(TcpWindowLimits::new(32, 4).unwrap());
    let before = sender.snapshot();
    let prepared = sender
        .prepare_append(ByteOffset::new(0), Bytes::from_static(b"abc"))
        .unwrap();
    let mut effects = SessionEffectBatch::empty();
    FAIL_NEXT_ALLOCATION.set(true);
    assert_eq!(effects.prepare_ordinary(), Err(EffectStorageError::Reserve));
    drop(prepared);
    assert_eq!(sender.snapshot(), before);
    let prepared = sender
        .prepare_append(ByteOffset::new(0), Bytes::from_static(b"abc"))
        .unwrap();
    effects.prepare_ordinary().unwrap();
    let applied = prepared.commit();
    assert_eq!(applied.retained_bytes_added(), 3);
    assert_eq!(sender.next_sent(), ByteOffset::new(3));
    assert_eq!(
        sender.replay_view().next().unwrap().payload(),
        &Bytes::from_static(b"abc")
    );
}

#[test]
fn prepared_tcp_ack_keeps_full_prefix_and_partial_suffix_until_commit() {
    use crate::resumable::ByteOffset;
    use crate::resumable::tcp::{TcpSendWindow, TcpWindowLimits};
    use bytes::Bytes;
    let mut sender = TcpSendWindow::new(TcpWindowLimits::new(32, 4).unwrap());
    sender
        .append(ByteOffset::new(0), Bytes::from_static(b"ab"))
        .unwrap();
    sender
        .append(ByteOffset::new(2), Bytes::from_static(b"cdefgh"))
        .unwrap();
    let before = sender.snapshot();
    let before_payload = sender.replay_view().last().unwrap().payload().clone();
    let prepared = sender
        .prepare_acknowledge(ByteOffset::new(7), false)
        .unwrap();
    let mut effects = SessionEffectBatch::empty();
    FAIL_NEXT_ALLOCATION.set(true);
    assert_eq!(effects.prepare_ordinary(), Err(EffectStorageError::Reserve));
    drop(prepared);
    assert_eq!(sender.snapshot(), before);
    assert_eq!(
        sender.replay_view().last().unwrap().payload().as_ptr(),
        before_payload.as_ptr()
    );
    let prepared = sender
        .prepare_acknowledge(ByteOffset::new(7), false)
        .unwrap();
    effects.prepare_ordinary().unwrap();
    let applied = prepared.commit();
    assert_eq!(applied.retained_bytes_released(), 7);
    assert_eq!(applied.segments_released(), 1);
    assert_eq!(
        sender.replay_view().next().unwrap().payload(),
        &Bytes::from_static(b"h")
    );
}

#[test]
fn prepared_tcp_close_publishes_only_after_effect_preparation() {
    use crate::resumable::ByteOffset;
    use crate::resumable::tcp::{TcpSendWindow, TcpWindowLimits};
    let mut sender = TcpSendWindow::new(TcpWindowLimits::new(32, 4).unwrap());
    let before = sender.snapshot();
    let prepared = sender.prepare_close(ByteOffset::new(0)).unwrap();
    let mut effects = SessionEffectBatch::empty();
    FAIL_NEXT_ALLOCATION.set(true);
    assert_eq!(effects.prepare_ordinary(), Err(EffectStorageError::Reserve));
    drop(prepared);
    assert_eq!(sender.snapshot(), before);
    let prepared = sender.prepare_close(ByteOffset::new(0)).unwrap();
    effects.prepare_ordinary().unwrap();
    assert!(prepared.commit().newly_recorded());
    assert!(
        !sender
            .prepare_close(ByteOffset::new(0))
            .unwrap()
            .commit()
            .newly_recorded()
    );
}

#[test]
fn prepared_tcp_receive_retains_original_window_on_effect_reserve_failure() {
    use crate::resumable::ByteOffset;
    use crate::resumable::tcp::{TcpReceiveWindow, TcpWindowLimits};
    use bytes::Bytes;
    let mut receiver = TcpReceiveWindow::new(TcpWindowLimits::new(32, 4).unwrap());
    let before = receiver.snapshot();
    let input = Bytes::from_static(b"abc");
    let reservation = receiver
        .preview_receive(ByteOffset::new(0), &input)
        .unwrap();
    let prepared = receiver
        .prepare_receive_commit(reservation, ByteOffset::new(0), input.clone())
        .unwrap();
    let mut effects = SessionEffectBatch::empty();
    FAIL_NEXT_ALLOCATION.set(true);
    assert_eq!(effects.prepare_ordinary(), Err(EffectStorageError::Reserve));
    drop(prepared);
    assert_eq!(receiver.snapshot(), before);
    let reservation = receiver
        .preview_receive(ByteOffset::new(0), &input)
        .unwrap();
    let prepared = receiver
        .prepare_receive_commit(reservation, ByteOffset::new(0), input)
        .unwrap();
    effects.prepare_ordinary().unwrap();
    assert_eq!(prepared.commit().finish().buffered_bytes_added(), 3);
    assert_eq!(receiver.accepted(), ByteOffset::new(0));
    assert_eq!(
        receiver.peek_contiguous(3)[0].payload(),
        &Bytes::from_static(b"abc")
    );
}

#[test]
fn prepared_tcp_accept_keeps_ack_and_original_ranges_until_commit() {
    use crate::resumable::ByteOffset;
    use crate::resumable::tcp::{TcpReceiveWindow, TcpWindowLimits};
    use bytes::Bytes;
    let mut receiver = TcpReceiveWindow::new(TcpWindowLimits::new(32, 4).unwrap());
    receiver
        .receive(ByteOffset::new(0), Bytes::from_static(b"ab"))
        .unwrap();
    receiver
        .receive(ByteOffset::new(2), Bytes::from_static(b"cdefgh"))
        .unwrap();
    let before = receiver.snapshot();
    let before_payload = receiver.peek_contiguous(8)[1].payload().clone();
    let prepared = receiver.prepare_accept(7).unwrap();
    let mut effects = SessionEffectBatch::empty();
    FAIL_NEXT_ALLOCATION.set(true);
    assert_eq!(effects.prepare_ordinary(), Err(EffectStorageError::Reserve));
    drop(prepared);
    assert_eq!(receiver.snapshot(), before);
    assert_eq!(
        receiver.peek_contiguous(8)[1].payload().as_ptr(),
        before_payload.as_ptr()
    );
    let prepared = receiver.prepare_accept(7).unwrap();
    effects.prepare_ordinary().unwrap();
    let applied = prepared.commit();
    assert_eq!(applied.accepted_bytes(), 7);
    assert_eq!(applied.ranges_released(), 1);
    assert_eq!(receiver.accepted(), ByteOffset::new(7));
    assert_eq!(
        receiver.peek_contiguous(8)[0].payload(),
        &Bytes::from_static(b"h")
    );
}

#[test]
fn prepared_tcp_receive_close_keeps_epoch_and_final_offset_until_commit() {
    use crate::resumable::ByteOffset;
    use crate::resumable::tcp::{TcpReceiveWindow, TcpWindowLimits};
    use bytes::Bytes;
    let mut receiver = TcpReceiveWindow::new(TcpWindowLimits::new(32, 4).unwrap());
    receiver
        .receive(ByteOffset::new(0), Bytes::from_static(b"abc"))
        .unwrap();
    let before = receiver.snapshot();
    let reservation = receiver
        .preview_receive(ByteOffset::new(0), &Bytes::from_static(b"abc"))
        .unwrap();
    let prepared = receiver.prepare_receive_close(ByteOffset::new(3)).unwrap();
    let mut effects = SessionEffectBatch::empty();
    FAIL_NEXT_ALLOCATION.set(true);
    assert_eq!(effects.prepare_ordinary(), Err(EffectStorageError::Reserve));
    drop(prepared);
    assert_eq!(receiver.snapshot(), before);
    assert_eq!(
        receiver
            .commit_receive(reservation, ByteOffset::new(0), Bytes::from_static(b"abc"))
            .unwrap()
            .buffered_bytes_added(),
        0
    );
    let prepared = receiver.prepare_receive_close(ByteOffset::new(3)).unwrap();
    effects.prepare_ordinary().unwrap();
    let applied = prepared.commit();
    assert!(applied.newly_recorded());
    assert!(!applied.half_close_ready());
    assert_eq!(receiver.accepted(), ByteOffset::new(0));
    assert!(
        !receiver
            .prepare_receive_close(ByteOffset::new(3))
            .unwrap()
            .commit()
            .newly_recorded()
    );
}

struct PanickingBytesOwner(std::sync::Arc<std::sync::atomic::AtomicUsize>);

impl AsRef<[u8]> for PanickingBytesOwner {
    fn as_ref(&self) -> &[u8] {
        b"abc"
    }
}

impl Drop for PanickingBytesOwner {
    fn drop(&mut self) {
        if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            panic!("original input Bytes owner disposal");
        }
    }
}

#[test]
fn original_bytes_drop_unwind_after_append_keeps_normalized_window_owned() {
    use crate::resumable::ByteOffset;
    use crate::resumable::tcp::{TcpSendWindow, TcpWindowLimits};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let drops = Arc::new(AtomicUsize::new(0));
    let mut sender = TcpSendWindow::new(TcpWindowLimits::new(32, 4).unwrap());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sender
            .append(
                ByteOffset::new(0),
                bytes::Bytes::from_owner(PanickingBytesOwner(drops.clone())),
            )
            .unwrap();
    }));
    assert!(result.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(sender.snapshot().retained_bytes(), 3);
    assert_eq!(sender.next_sent(), ByteOffset::new(3));
    assert_eq!(
        sender.replay_view().next().unwrap().payload().as_ref(),
        b"abc"
    );
}

#[test]
fn original_bytes_drop_unwind_after_receive_keeps_normalized_window_owned() {
    use crate::resumable::ByteOffset;
    use crate::resumable::tcp::{TcpReceiveWindow, TcpWindowLimits};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let drops = Arc::new(AtomicUsize::new(0));
    let mut receiver = TcpReceiveWindow::new(TcpWindowLimits::new(32, 4).unwrap());
    // Equal bytes, distinct owners: the reservation must not accidentally
    // keep the original commit input alive on behalf of the prepared operation.
    let reservation = receiver
        .preview_receive(ByteOffset::new(0), &bytes::Bytes::from_static(b"abc"))
        .unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        receiver
            .commit_receive(
                reservation,
                ByteOffset::new(0),
                bytes::Bytes::from_owner(PanickingBytesOwner(drops.clone())),
            )
            .unwrap();
    }));
    assert!(result.is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(receiver.snapshot().buffered_bytes(), 3);
    assert_eq!(receiver.accepted(), ByteOffset::new(0));
    assert_eq!(receiver.peek_contiguous(3)[0].payload().as_ref(), b"abc");
}

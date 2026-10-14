//! Actual cold backing shapes; no Flow, Target or whole-process memory claim.

use super::super::*;

#[test]
fn cold_ready_does_not_consume_the_effect_storage_fault_domain() {
    use crate::resumable::effect_storage::{exact_vec, fail_next_effect_allocation_for_test};

    fail_next_effect_allocation_for_test();
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(2).unwrap()).unwrap();
    let ready = ReadyIndex::cold(&geometry).unwrap();
    assert_eq!(ready.shared.slots.capacity(), 2);
    assert_eq!(exact_vec::<u8>(1), Err(EffectStorageError::Reserve));
    assert_eq!(exact_vec::<u8>(1).unwrap().capacity(), 1);
}

#[test]
fn cold_ready_preserves_invalid_initial_before_layout_rejection() {
    let n = isize::MAX as usize / size_of::<ReadySlotEnvelope>() + 1;
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(n).unwrap()).unwrap();
    assert_eq!(
        ReadyIndex::cold_backing_layout(&geometry),
        Err(ReadyFault::Layout)
    );
    // Invalid input must return without entering allocation even when layout
    // is also invalid. No huge valid allocation is attempted by this call.
    assert_eq!(
        ReadyIndex::new(&geometry, &[[4, 0]]).err(),
        Some(ReadyFault::InvalidResident)
    );
}

#[test]
fn cold_ready_backing_matches_actual_shapes_and_exact_capacities() {
    for n in [
        1,
        2,
        WORD_BITS - 1,
        WORD_BITS,
        WORD_BITS + 1,
        WORD_BITS * WORD_BITS + 1,
    ] {
        let geometry = ReadyGeometry::checked(NonZeroUsize::new(n).unwrap()).unwrap();
        let ready = ReadyIndex::cold(&geometry).unwrap();
        assert_eq!(ready.shared.slots.len(), n);
        assert_eq!(ready.shared.slots.capacity(), n);
        let mut expected = Layout::array::<ReadySlotEnvelope>(n).unwrap();
        for (tree, capacity) in ready.shared.trees.iter().zip(geometry.class_capacities()) {
            let mut width = capacity;
            let (mut count, mut levels) = (0, 0);
            loop {
                width = width.div_ceil(WORD_BITS);
                count += width;
                levels += 1;
                if width <= 1 {
                    break;
                }
            }
            assert_eq!(tree.capacity, capacity);
            assert_eq!(tree.words.len(), count);
            assert_eq!(tree.words.capacity(), count);
            assert_eq!(tree.levels.len(), levels);
            assert_eq!(tree.levels.capacity(), WORD_BITS);
            assert!(
                tree.words
                    .iter()
                    .all(|word| word.load(Ordering::Relaxed) == 0)
            );
            for piece in [
                Layout::array::<AtomicUsize>(tree.words.capacity()).unwrap(),
                Layout::array::<std::ops::Range<usize>>(tree.levels.capacity()).unwrap(),
            ] {
                expected = expected.extend(piece).unwrap().0;
            }
        }
        // Independently use the pinned std ArcInner layout, not an Arc pointer
        // or a second Notify backing for its eventual cloned handle.
        for payload in [Layout::new::<ReadyShared>(), Layout::new::<Notify>()] {
            let allocation = Layout::new::<[AtomicUsize; 2]>()
                .extend(payload)
                .unwrap()
                .0
                .pad_to_align();
            expected = expected.extend(allocation).unwrap().0;
        }
        assert_eq!(
            ReadyIndex::cold_backing_layout(&geometry).unwrap(),
            expected.pad_to_align()
        );
        for (slot, envelope) in ready.shared.slots.iter().enumerate() {
            let state = envelope.lock().unwrap();
            assert_eq!(state.key.slot, slot);
            assert!(matches!(state.occupancy, ReadyOccupancy::Free));
            assert_eq!(
                (state.local_word, state.source_cursor, state.capacity_mask),
                (0, 0, 0)
            );
            assert!(
                state
                    .sources
                    .iter()
                    .chain(state.processes.iter())
                    .all(|cell| cell.phase == ReadyPhase::Dormant && cell.revision == 0)
            );
            assert_eq!(envelope.publication.load(Ordering::Relaxed), 0);
        }
        assert_eq!(ready.check_preparation_boundary(0), Ok(()));
    }
}

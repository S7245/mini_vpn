//! Native fixed-slot ownership, ordering, revision and corruption characterization.
//! Calls original progress operations; direct slot mutation is only adversarial audit input.

use super::super::*;

fn flow(id: u64) -> SessionFlowId {
    SessionFlowId::new(id).unwrap()
}
fn generation(id: u64) -> LegGeneration {
    LegGeneration::new(id).unwrap()
}

#[test]
fn zero_full_reuse_and_expiry_keep_the_original_backing() {
    let mut zero = OwnerReverseProgress::new(0).unwrap();
    zero.cold_audit().unwrap();
    assert_eq!(
        zero.register_live(generation(1), flow(1), Direction::TargetToClient),
        Err(OwnerProgressError::CapacityExceeded { max_entries: 0 })
    );
    zero.reset_for_session_expiry();
    assert_eq!(zero.storage_measurement_for_test().unwrap().1, 0);

    let mut progress = OwnerReverseProgress::new(1).unwrap();
    let original = progress.storage_measurement_for_test().unwrap();
    for direction in [Direction::ClientToTarget, Direction::TargetToClient] {
        progress
            .register_live(generation(1), flow(u64::MAX), direction)
            .unwrap();
    }
    assert_eq!(
        progress.storage_measurement_for_test().unwrap(),
        (original.0, 2, 2, 0)
    );
    assert_eq!(
        progress.register_live(generation(1), flow(1), Direction::TargetToClient),
        Err(OwnerProgressError::CapacityExceeded { max_entries: 2 })
    );
    let revision = progress.revision;
    progress
        .register_live(generation(1), flow(u64::MAX), Direction::TargetToClient)
        .unwrap();
    assert_eq!(progress.revision, revision);
    assert_eq!(progress.retire_flow(generation(1), flow(u64::MAX)), Ok(2));
    assert_eq!(progress.storage_measurement_for_test().unwrap(), original);
    progress
        .register_live(generation(2), flow(17), Direction::TargetToClient)
        .unwrap();
    progress.reset_for_session_expiry();
    assert_eq!(progress.revision, 0);
    assert_eq!(progress.storage_measurement_for_test().unwrap(), original);
}

#[test]
fn concrete_layout_rejection_is_distinct_from_pure_count_overflow() {
    assert_eq!(
        OwnerProgressCapacity::checked(usize::MAX, 1),
        Err(OwnerProgressCapacityError::TrackedFlowCapacityOverflow)
    );
    let count = OwnerProgressCapacity::checked(usize::MAX / 2, 0).unwrap();
    assert_eq!(count.max_entries(), usize::MAX - 1);
    assert!(matches!(
        OwnerReverseProgress::from_capacity(count),
        Err(OwnerProgressError::StorageLayout)
    ));
    assert!(matches!(
        OwnerReverseProgress::new(usize::MAX),
        Err(OwnerProgressError::CapacityLimitOverflow)
    ));
}

#[test]
fn cold_progress_checks_the_combined_original_header_and_slot_layout() {
    for count in [0, 2, 24, 128] {
        let slots = Layout::array::<ProgressSlot>(count).unwrap();
        let header = Layout::new::<OwnerReverseProgress>();
        let (unrounded, offset) = header.extend(slots).unwrap();
        let combined = unrounded.pad_to_align();
        let (actual_slots, actual_combined) = OwnerProgressStorage::cold_layout(count).unwrap();
        assert_eq!(actual_slots, slots);
        assert_eq!(actual_combined, combined);
        assert_eq!(offset % slots.align(), 0);
        assert!(offset >= header.size() && offset - header.size() < slots.align());
        assert_eq!(unrounded.size(), offset + slots.size());
        assert_eq!(combined.align(), header.align().max(slots.align()));
        assert_eq!(combined.size() % combined.align(), 0);
        assert!(combined.size() - unrounded.size() < combined.align());
        let progress = OwnerReverseProgress::from_capacity(
            OwnerProgressCapacity::checked(count / 2, 0).unwrap(),
        )
        .unwrap();
        assert_eq!(Layout::for_value(&*progress.entries.slots), actual_slots);
        progress.cold_audit().unwrap();
    }
    // Audit the real near-limit geometry without attempting a huge allocation.
    // Directional counts are even; type alignment may make the combined-only
    // rejection interval empty on a platform. That is not a natural C failure.
    let maximum = (isize::MAX as usize / std::mem::size_of::<ProgressSlot>()) & !1;
    let mut combined_only = 0;
    for count in [maximum - 2, maximum, maximum + 2] {
        assert_eq!(
            OwnerProgressCapacity::checked(count / 2, 0)
                .unwrap()
                .max_entries(),
            count
        );
        let slots = Layout::array::<ProgressSlot>(count);
        let slots_valid = slots.is_ok();
        let expected = slots.and_then(|slots| {
            Layout::new::<OwnerReverseProgress>()
                .extend(slots)
                .map(|(layout, _)| (slots, layout.pad_to_align()))
        });
        if slots_valid && expected.is_err() {
            combined_only += 1;
        }
        assert_eq!(
            OwnerProgressStorage::cold_layout(count),
            expected.map_err(|_| OwnerProgressError::StorageLayout)
        );
    }
    eprintln!(
        "actual progress Layout: header={}, slot={}, combined-only near-limit counts={combined_only}",
        std::mem::size_of::<OwnerReverseProgress>(),
        std::mem::size_of::<ProgressSlot>()
    );
}

#[test]
fn all_directions_preflight_before_retirement_and_revision_priority_is_preserved() {
    let mut progress = OwnerReverseProgress::new(2).unwrap();
    progress
        .register_live(generation(1), flow(1), Direction::ClientToTarget)
        .unwrap();
    progress
        .register_live(generation(3), flow(1), Direction::TargetToClient)
        .unwrap();
    let original = progress.storage_measurement_for_test().unwrap();
    let revision = progress.revision;
    assert_eq!(
        progress.retire_flow(generation(1), flow(1)),
        Err(OwnerProgressError::GenerationMismatch)
    );
    assert_eq!(
        progress.retire_flow_after_committed_generation(generation(2), flow(1)),
        Err(OwnerProgressError::GenerationDidNotAdvance {
            previous: generation(3),
            observed: generation(2)
        })
    );
    assert_eq!(progress.storage_measurement_for_test().unwrap(), original);
    assert_eq!(progress.revision, revision);
    progress.revision = u64::MAX;
    assert_eq!(
        progress.retire_flow(generation(3), flow(2)),
        Err(OwnerProgressError::RevisionExhausted)
    );
    assert_eq!(
        progress.retire_flow_after_committed_generation(generation(3), flow(2)),
        Ok(0)
    );
    assert_eq!(
        progress.retire_flow_after_committed_generation(generation(3), flow(1)),
        Err(OwnerProgressError::RevisionExhausted)
    );
    assert_eq!(progress.storage_measurement_for_test().unwrap(), original);
    progress.revision = revision;
    assert_eq!(
        progress.retire_flow_after_committed_generation(generation(3), flow(1)),
        Ok(2)
    );
    progress.cold_audit().unwrap();
}

fn assert_order(
    progress: &OwnerReverseProgress,
    expected: &std::collections::BTreeSet<DirectionalFlowKey>,
) {
    progress.cold_audit().unwrap();
    let mut cursor = progress.entries.after(None);
    for key in expected {
        let index = cursor.unwrap();
        assert_eq!(progress.entries.node(index).key, *key);
        assert_eq!(
            progress.entries.after(Some(*key)),
            progress.entries.successor(index)
        );
        cursor = progress.entries.successor(index);
    }
    assert!(cursor.is_none());
    assert_eq!(progress.len(), expected.len());
}

#[test]
fn sparse_rotations_and_two_child_retirement_match_ordered_reference() {
    for stride in [1, 127, 73] {
        let mut progress = OwnerReverseProgress::new(64).unwrap();
        let backing = progress.storage_measurement_for_test().unwrap().0;
        let mut expected = std::collections::BTreeSet::new();
        for round in 0..3 {
            for raw in 0..128 {
                let id = flow(1 + ((raw * stride) % 128) * 1_000_003 + round * 1_000_000_000);
                progress
                    .register_live(generation(1), id, Direction::TargetToClient)
                    .unwrap();
                expected.insert(DirectionalFlowKey::new(id, Direction::TargetToClient));
                assert_order(&progress, &expected);
            }
            for raw in 0..128 {
                let id = flow(1 + ((raw * 37) % 128) * 1_000_003 + round * 1_000_000_000);
                assert_eq!(progress.retire_flow(generation(1), id), Ok(1));
                expected.remove(&DirectionalFlowKey::new(id, Direction::TargetToClient));
                assert_order(&progress, &expected);
                assert_eq!(progress.storage_measurement_for_test().unwrap().0, backing);
            }
        }
    }
}

#[test]
fn bounded_scans_restart_after_reuse_and_keep_global_deadline_order() {
    let mut progress = OwnerReverseProgress::new(48).unwrap();
    for raw in 1..=96 {
        let id = flow(raw * 1_000_003);
        progress
            .register_live(generation(1), id, Direction::TargetToClient)
            .unwrap();
        progress
            .observe_outstanding(
                generation(1),
                id,
                Direction::TargetToClient,
                ByteOffset::new(0),
                SimTime::from_nanos(97 - raw),
            )
            .unwrap();
    }
    for _ in 0..8 {
        let mut scan = None;
        for completed in [32, 64] {
            assert!(matches!(
                progress
                    .scan_reverse_stall(generation(1), Duration::from_nanos(1), &mut scan)
                    .unwrap(),
                OwnerReverseStallScan::Scanning
            ));
            assert_eq!(scan.as_ref().unwrap().scanned_entries, completed);
        }
        let OwnerReverseStallScan::Complete(Some(candidate)) = progress
            .scan_reverse_stall(generation(1), Duration::from_nanos(1), &mut scan)
            .unwrap()
        else {
            panic!("complete scan")
        };
        assert_eq!(candidate.flow_id, flow(96 * 1_000_003));
        assert!(scan.is_none());
    }
    let mut scan = None;
    progress
        .scan_reverse_stall(generation(1), Duration::from_nanos(1), &mut scan)
        .unwrap();
    progress
        .retire_flow(generation(1), flow(1_000_003))
        .unwrap();
    progress
        .register_live(generation(1), flow(u64::MAX), Direction::TargetToClient)
        .unwrap();
    progress
        .scan_reverse_stall(generation(1), Duration::from_nanos(1), &mut scan)
        .unwrap();
    assert_eq!(scan.as_ref().unwrap().scanned_entries, 32);
    assert_eq!(scan.as_ref().unwrap().revision, progress.revision);
    progress.cold_audit().unwrap();
}

#[test]
fn damaged_partitions_fail_closed_with_independent_bounds() {
    let mut progress = OwnerReverseProgress::new(4).unwrap();
    for id in [2, 1, 3] {
        progress
            .register_live(generation(1), flow(id), Direction::TargetToClient)
            .unwrap();
    }
    let original = progress.entries.slots.clone();
    let root = progress.entries.root.unwrap();
    let free = progress.entries.free.unwrap();
    let saved_root = *progress.entries.node(root);
    for damage in 0..10 {
        progress.entries.slots.clone_from(&original);
        progress.entries.count = 3;
        progress.entries.free = Some(free);
        match damage {
            0 => progress.entries.node_mut(root).height = usize::MAX,
            1 => progress.entries.node_mut(root).left = Some(root),
            2 => progress.entries.node_mut(root).right = saved_root.left,
            3 => progress.entries.node_mut(root).left = Some(usize::MAX),
            4 => progress.entries.node_mut(saved_root.left.unwrap()).parent = None,
            5 => progress.entries.slots[free] = ProgressSlot::Vacant { next: Some(free) },
            6 => progress.entries.free = Some(root),
            7 => progress.entries.node_mut(root).left = None,
            8 => progress.entries.count = usize::MAX,
            9 => progress.entries.free = None,
            _ => unreachable!(),
        }
        assert_eq!(
            progress.cold_audit(),
            Err(OwnerProgressError::StorageCorrupt),
            "damage {damage}"
        );
    }
    progress.entries.slots = original;
    progress.entries.count = 3;
    progress.entries.free = Some(free);
    progress.cold_audit().unwrap();
    progress.capacity.max_entries -= 1;
    assert_eq!(
        progress.cold_audit(),
        Err(OwnerProgressError::StorageCorrupt)
    );
}

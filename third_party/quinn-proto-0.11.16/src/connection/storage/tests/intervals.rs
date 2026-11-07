use super::*;
use crate::range_set::RangeSet;
use std::collections::HashSet;

impl Intervals {
    pub(in crate::connection) fn actual_layout_for_probe(&self) -> (usize, usize) {
        (self.nodes.values.as_ptr() as usize,
         std::alloc::Layout::array::<Node>(self.nodes.values.capacity()).unwrap().size())
    }
}

#[test]
fn actual_avl_pop_suffix_reuses_full_inventory_and_preserves_independent_sum() {
    let budget = bytes::BackingBudget::new(4096).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut tree = Intervals::default();
    let mut original = RangeSet::new();
    for range in [0..8, 16..24, 32..40, 48..56] {
        let prepared = tree.prepare(range.clone(), Some(&meter)).unwrap();
        tree.install(prepared);
        tree.insert(range.clone());
        original.insert(range);
    }
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    assert_eq!(budget.used(), budget.limit());
    let rejected = tree.prepare(64..72, Some(&meter));
    assert!(matches!(rejected, Err(bytes::MeteredCopyError::BudgetFull)));
    assert_eq!(tree.bytes(), 32);
    assert!(tree.inventory_matches(&meter));
    assert_eq!(tree.pop_min(), original.pop_min());
    assert_eq!(crate::tests::allocations_in(|| tree.insert(3..8)), 0);
    original.insert(3..8);
    assert_eq!(tree.bytes(), original.iter().map(|r| r.end - r.start).sum::<u64>());
    assert_eq!(tree.min(), original.min());
    assert_eq!(tree.overlaps(0..u64::MAX).collect::<Vec<_>>(), original.iter().collect::<Vec<_>>());
    assert!(tree.inventory_matches(&meter));
    while let Some(expected) = original.pop_min() {
        assert_eq!(tree.pop_min(), Some(expected));
        assert!(tree.inventory_matches(&meter));
    }
    assert_eq!(tree.bytes(), 0);
    assert_eq!(budget.used(), budget.limit(), "free slots remain physical inventory");
    drop(pressure);
    drop(tree);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

fn validate(tree: &Intervals) {
    fn walk(
        tree: &Intervals,
        id: usize,
        seen: &mut HashSet<usize>,
        ranges: &mut Vec<Range<u64>>,
    ) -> i16 {
        if id == NONE {
            return 0;
        }
        assert!(seen.insert(id));
        let n = &tree.nodes.values[id];
        let left = walk(tree, n.left, seen, ranges);
        ranges.push(n.range.clone());
        let right = walk(tree, n.right, seen, ranges);
        assert!((left - right).abs() <= 1);
        assert_eq!(n.height as i16, 1 + left.max(right));
        1 + left.max(right)
    }
    let mut seen = HashSet::new();
    let mut ranges = Vec::new();
    walk(tree, tree.root, &mut seen, &mut ranges);
    assert!(ranges.windows(2).all(|r| r[0].end < r[1].start));
    let mut free = tree.free;
    while free != NONE {
        assert!(seen.insert(free));
        assert_eq!(tree.nodes.values[free].height, 0);
        free = tree.nodes.values[free].left;
    }
    assert_eq!(seen.len(), tree.nodes.values.len());
}

#[test]
fn slab_avl_matches_original_replace_and_inventory_after_mixed_ranges() {
    let mut tree = Intervals::default();
    let mut original = RangeSet::new();
    let mut seed = 0x87ff1425u64;
    for round in 0..6000 {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        let start = if round < 1024 { round * 3 } else { seed % 4096 };
        let end = start + 1 + (seed >> 32) % 7;
        let range = start..end;
        let actual: Vec<_> = tree.overlaps(range.clone()).collect();
        let expected: Vec<_> = original.replace(range.clone()).collect();
        assert_eq!(actual, expected);
        let growth = tree.prepare(range.clone(), None).unwrap();
        tree.install(growth);
        assert_eq!(crate::tests::allocations_in(|| tree.insert(range)), 0);
        assert_eq!(
            tree.overlaps(0..u64::MAX).collect::<Vec<_>>(),
            original.iter().collect::<Vec<_>>()
        );
        validate(&tree);
    }
}

#[test]
fn sparse_peer_offsets_have_log_height_and_exact_preallocation_peak() {
    use bytes::{BackingBudget, BackingMeter};
    let budget = BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut tree = Intervals::default();
    let start = std::time::Instant::now();
    for i in 0..32768 {
        let range = i * 3..i * 3 + 1;
        let before = budget.used();
        let growth = tree.prepare(range.clone(), Some(&meter)).unwrap();
        let old_layout = std::alloc::Layout::array::<Node>(tree.nodes.values.capacity())
            .unwrap()
            .size();
        assert_eq!(before, old_layout);
        if let Some(new) = &growth {
            assert_eq!(
                budget.used(),
                before
                    + std::alloc::Layout::array::<Node>(new.values.capacity())
                        .unwrap()
                        .size()
            );
        }
        tree.install(growth);
        tree.insert(range);
    }
    validate(&tree);
    let height = tree.height(tree.root);
    assert!(height <= 2 * 16);
    eprintln!(
        "actual sparse 32768-node AVL height={height}, build={:?}, bytes={}",
        start.elapsed(),
        budget.used()
    );
    let growth = tree.prepare(0..100000, Some(&meter)).unwrap();
    assert!(growth.is_none());
    assert_eq!(crate::tests::allocations_in(|| tree.insert(0..100000)), 0);
    validate(&tree);
    assert_eq!(tree.overlaps(0..100000).count(), 1);
    drop(tree);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

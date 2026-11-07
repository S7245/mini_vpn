use super::*;
use std::collections::{BTreeMap, HashSet};

#[test]
fn next_unknown_offset_keeps_monotonic_metadata_across_duplicates_and_reused_slots() {
    let budget = bytes::BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut tree = Inventory::default();
    for key in 0..4 {
        let next = tree.prepare_next(Some(&meter)).unwrap();
        tree.install(next);
        tree.insert(key, Bytes::from_static(b"x"));
    }
    let before = budget.used();
    let next = tree.prepare_next(Some(&meter)).unwrap();
    assert_eq!(budget.used(), before + Layout::array::<Node>(8).unwrap().size());
    tree.install(next);
    let charged = budget.used();
    assert_eq!(charged, Layout::array::<Node>(8).unwrap().size());
    for _ in 0..100 {
        assert!(tree.prepare_next(Some(&meter)).unwrap().is_none());
        tree.insert(3, Bytes::from_static(b"duplicate"));
        assert_eq!(budget.used(), charged);
    }
    tree.remove_first();
    assert!(tree.prepare_next(Some(&meter)).unwrap().is_none());
    tree.insert(9, Bytes::from_static(b"reuse"));
    assert!(tree.valid_inventory(Some(&meter)));
    assert_eq!(budget.used(), charged);
    drop(tree);
    assert_eq!(budget.used(), 0);
}

fn audit(tree: &Inventory, expected: &BTreeMap<u64, Bytes>) {
    fn walk(
        tree: &Inventory,
        id: usize,
        seen: &mut HashSet<usize>,
        entries: &mut BTreeMap<u64, Bytes>,
    ) -> u8 {
        if id == NONE {
            return 0;
        }
        assert!(seen.insert(id));
        let n = &tree.slab.values[id];
        assert!(n.height != 0);
        if n.left != NONE {
            assert!(tree.slab.values[n.left].offset < n.offset);
        }
        if n.right != NONE {
            assert!(tree.slab.values[n.right].offset > n.offset);
        }
        let left = walk(tree, n.left, seen, entries);
        let right = walk(tree, n.right, seen, entries);
        assert!(left.abs_diff(right) <= 1);
        assert_eq!(n.height, left.max(right) + 1);
        assert!(entries.insert(n.offset, n.bytes.clone()).is_none());
        n.height
    }
    let (mut seen, mut entries) = (HashSet::new(), BTreeMap::new());
    walk(tree, tree.root, &mut seen, &mut entries);
    assert_eq!(&entries, expected);
    assert_eq!(tree.len(), expected.len());
    let mut free = tree.free;
    while free != NONE {
        assert!(seen.insert(free));
        let n = &tree.slab.values[free];
        assert_eq!(n.height, 0);
        assert!(n.bytes.is_empty());
        free = n.left;
    }
    assert_eq!(seen.len(), tree.slab.values.len());
}

#[test]
fn avl_sparse_replacement_reuse_and_wrap_keys_match_btree() {
    let budget = bytes::BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (mut tree, mut expected, mut seed) = (Inventory::default(), BTreeMap::new(), 19u64);
    for i in 0..20_000 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        if i % 4 == 0 {
            assert_eq!(tree.remove_first(), expected.pop_first());
        } else {
            let key = if i % 67 == 0 {
                u64::MAX
            } else {
                (seed >> 15) % 6000
            };
            let value = Bytes::copy_from_slice(&seed.to_le_bytes());
            let prepared = tree.prepare(key, Some(&meter)).unwrap();
            tree.install(prepared);
            assert_eq!(tree.insert(key, value.clone()), expected.insert(key, value));
        }
        if i % 113 == 0 {
            audit(&tree, &expected);
            assert!(tree.valid_inventory(Some(&meter)));
        }
    }
    audit(&tree, &expected);
    while !expected.is_empty() {
        assert_eq!(tree.remove_first(), expected.pop_first());
    }
    audit(&tree, &expected);
    assert!(budget.used() > 0, "empty inventory retains its actual slab");
    drop(tree);
    assert_eq!(budget.used(), 0);
}

#[test]
fn growth_requires_old_plus_new_exact_layout_and_refusal_keeps_all_slots() {
    let max_records = crate::client_tun::NATIVE_TCP_REASSEMBLY_MAX_BYTES;
    eprintln!("[N1-assembler-layout-bound] max_nonempty_records={max_records} actual_node={} final_layout={} largest_old_plus_new={} backing_bytes_and_shared_input_headers_separate=true bound_not_full_allocation=true",
        Layout::new::<Node>().size(), Layout::array::<Node>(max_records).unwrap().size(),
        Layout::array::<Node>(max_records).unwrap().size() + Layout::array::<Node>(max_records / 2).unwrap().size());
    let old = Layout::array::<Node>(4).unwrap().size();
    let new = Layout::array::<Node>(8).unwrap().size();
    let budget = bytes::BackingBudget::new(old + new).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut tree = Inventory::default();
    for i in 0..4 {
        let next = tree.prepare(i, Some(&meter)).unwrap();
        tree.install(next);
        tree.insert(i, Bytes::from_static(b"x"));
    }
    assert_eq!(budget.used(), old);
    let held = budget.try_reserve(1).unwrap();
    assert!(matches!(
        tree.prepare(4, Some(&meter)),
        Err(MeteredCopyError::BudgetFull)
    ));
    assert_eq!(tree.len(), 4);
    assert_eq!(tree.slab.values.capacity(), 4);
    drop(held);
    let prepared = tree.prepare(4, Some(&meter)).unwrap();
    assert_eq!(budget.used(), old + new);
    tree.install(prepared);
    assert_eq!(budget.used(), new);
    tree.insert(4, Bytes::from_static(b"y"));
    drop(tree);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn cold_inventory_rejects_orphans_cycles_layout_and_free_backing() {
    for fault in 0..10 {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let mut tree = Inventory::default();
        for key in [2, 1, 3] {
            let slab = tree.prepare(key, Some(&meter)).unwrap();
            tree.install(slab);
            tree.insert(key, Bytes::from_static(b"x"));
        }
        assert!(tree.valid_inventory(Some(&meter)));
        match fault {
            0 => tree.len -= 1,
            1 => tree.root = NONE,
            2 => tree.slab.values[tree.root].left = tree.root,
            3 => tree.slab.allocated_bytes += 1,
            4 => {
                tree.slab._allowance.take();
            }
            5 => {
                tree.remove_first();
                tree.slab.values[tree.free].bytes =
                    Bytes::copy_from_slice_metered(b"orphan", &meter).unwrap();
                tree.slab.values[tree.free].bytes.truncate(0);
                assert!(
                    tree.slab.values[tree.free]
                        .bytes
                        .allocation_identity()
                        .is_some()
                );
            }
            6 => {
                tree.remove_first();
                tree.slab.values[tree.free].left = tree.free;
            }
            7 => {
                while tree.remove_first().is_some() {}
                tree.free = NONE;
            }
            8 => {
                tree.slab._allowance = meter.reserve_cold_layout(1).unwrap();
            }
            9 => {
                let foreign = bytes::BackingBudget::new(65536).unwrap();
                tree.slab._allowance =
                    Some(foreign.try_reserve(tree.slab.allocated_bytes).unwrap());
            }
            _ => unreachable!(),
        }
        assert!(!tree.valid_inventory(Some(&meter)), "fault {fault}");
        drop(tree);
        assert_eq!(budget.used(), 0);
    }
}

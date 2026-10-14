//! Original node addresses, AVL invariants, preparation rollback and warm reuse.
use super::super::*;
use crate::resumable::tcp::{ByteOffset, TcpWindowLimits};
use bytes::Bytes;
use std::collections::BTreeSet;

#[test]
fn clear_recycles_each_live_node_in_one_visit() {
    let mut window = TcpReceiveWindow::new(TcpWindowLimits::new(512, 256).unwrap());
    for i in 0..256 {
        window
            .receive(ByteOffset::new(i), Bytes::from_static(b"x"))
            .unwrap();
    }
    let original = live_addresses(&window.ranges);
    TREE_VISITS.set(0);
    window.abandon();
    assert_eq!(
        TREE_VISITS.get(),
        256,
        "linear owning traversal, not repeated balanced removal"
    );
    let mut free = window.ranges.free.head.as_ref();
    let mut addresses = BTreeSet::new();
    while let Some(node) = free {
        addresses.insert(node.as_ptr() as usize);
        free = node[0].left.as_ref();
    }
    assert_eq!(addresses, original);
    assert_eq!(window.ranges.free.len, 256);
}

fn audit(
    link: &Link,
    low: Option<u64>,
    high: Option<u64>,
    pointers: &mut BTreeSet<usize>,
) -> (u8, usize) {
    let Some(node) = link else {
        return (0, 0);
    };
    assert!(
        pointers.insert(node.as_ptr() as usize),
        "one concrete node, one owner"
    );
    assert!(low.is_none_or(|k| node[0].key > k));
    assert!(high.is_none_or(|k| node[0].key < k));
    let (l, ln) = audit(&node[0].left, low, Some(node[0].key), pointers);
    let (r, rn) = audit(&node[0].right, Some(node[0].key), high, pointers);
    assert!(l.abs_diff(r) <= 1);
    assert_eq!(node[0].height, 1 + l.max(r));
    (node[0].height, ln + rn + 1)
}

fn live_addresses(ranges: &ReceiveRanges) -> BTreeSet<usize> {
    let mut addresses = BTreeSet::new();
    let (height, count) = audit(&ranges.root, None, None, &mut addresses);
    assert_eq!(count, ranges.len);
    assert!((height as usize) < MAX_HEIGHT);
    addresses
}

#[test]
fn actual_window_rotations_prefix_removal_and_reuse_keep_bounded_original_nodes() {
    for mode in 0..4 {
        let mut window = TcpReceiveWindow::new(TcpWindowLimits::new(2048, 256).unwrap());
        for i in 0..256u64 {
            let key = match mode {
                0 => i,
                1 => 255 - i,
                2 => (i * 73) % 256,
                _ => {
                    if i % 2 == 0 {
                        i / 2
                    } else {
                        255 - i / 2
                    }
                }
            };
            let previous = live_addresses(&window.ranges);
            window
                .receive(ByteOffset::new(key), Bytes::from_static(b"x"))
                .unwrap();
            let current = live_addresses(&window.ranges);
            assert!(
                previous.is_subset(&current),
                "rotation must not relocate nodes"
            );
            assert_eq!(
                window
                    .ranges
                    .iter()
                    .map(|(k, _)| *k)
                    .collect::<Vec<_>>()
                    .len(),
                (i + 1) as usize
            );
        }
        let original = live_addresses(&window.ranges);
        assert_eq!(
            window.ranges.prepare(1, 256).err(),
            Some(TcpOwnershipError::SegmentCapacityExceeded {
                attempted: 257,
                max: 256
            })
        );
        for i in 0..256usize {
            window.accept(1).unwrap();
            live_addresses(&window.ranges);
            assert_eq!(window.ranges.len + window.ranges.free.len, 256);
            assert_eq!(window.ranges.free.len, i + 1);
        }
        for i in 0..256u64 {
            FAIL_NODE.set(Some(0));
            window
                .receive(ByteOffset::new(256 + i), Bytes::from_static(b"y"))
                .unwrap();
            assert_eq!(FAIL_NODE.take(), Some(0));
        }
        assert_eq!(live_addresses(&window.ranges), original);
        window.abandon();
        assert_eq!(window.ranges.len, 0);
        assert_eq!(window.ranges.free.len, 256);
    }
}

#[test]
fn partial_rekey_and_prepared_drop_preserve_exact_payload_and_free_chain() {
    let mut window = TcpReceiveWindow::new(TcpWindowLimits::new(64, 8).unwrap());
    window
        .receive(ByteOffset::new(0), Bytes::from_static(b"abcdefgh"))
        .unwrap();
    let node = live_addresses(&window.ranges);
    let payload = window.ranges.get(&0).unwrap().bytes.as_ptr();
    window.accept(1).unwrap();
    assert_eq!(live_addresses(&window.ranges), node);
    assert_eq!(
        window.ranges.get(&1).unwrap().bytes.as_ptr(),
        payload.wrapping_add(1)
    );
    window.accept(7).unwrap();
    let free = window.ranges.free.head.as_ref().unwrap().as_ptr();
    let prepared = window.ranges.prepare(3, 8).unwrap();
    assert_eq!(prepared.len, 2);
    assert_eq!(window.ranges.free.head.as_ref().unwrap().as_ptr(), free);
    assert_eq!(window.ranges.free.len, 1);
    drop(prepared);
    assert_eq!(window.ranges.free.head.as_ref().unwrap().as_ptr(), free);
    assert_eq!(window.ranges.free.len, 1);
    let resident = LIVE_NODES.get();
    FAIL_NODE.set(Some(1));
    assert_eq!(
        window.ranges.prepare(3, 8).err(),
        Some(TcpOwnershipError::ReceiveStorageReserve)
    );
    assert_eq!(
        LIVE_NODES.get(),
        resident,
        "a partially allocated new chain is reclaimed"
    );
    assert_eq!(window.ranges.free.head.as_ref().unwrap().as_ptr(), free);
    assert_eq!(window.ranges.free.len, 1);
    assert_eq!(
        window.ranges.prepare(usize::MAX, 8).err(),
        Some(TcpOwnershipError::SegmentCapacityExceeded {
            attempted: usize::MAX,
            max: 8
        })
    );
}

#[test]
fn prepared_chain_drop_is_iterative_and_layout_overflow_precedes_allocation() {
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(|| {
            let ranges = ReceiveRanges::new();
            let prepared = ranges.prepare(8192, 8192).unwrap();
            assert_eq!(LIVE_NODES.get(), 8192);
            drop(prepared);
            assert_eq!(LIVE_NODES.get(), 0);
            assert_eq!(
                ranges.prepare(usize::MAX, usize::MAX).err(),
                Some(TcpOwnershipError::ReceiveStorageLayout)
            );
            assert_eq!(LIVE_NODES.get(), 0);
        })
        .unwrap()
        .join()
        .unwrap();
}

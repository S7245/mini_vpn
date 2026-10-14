//! Native Target node admission, same-node transfer and retained vacancy reuse.
//! Tests use actual TargetIo operations; logical zero is not backing release.

use super::*;

#[test]
fn node_reserve_preserves_open_then_original_join_and_retire_reuse_backing() {
    let config = MemoryTargetConfig::new(2, 8, 16, 8, 8, 1)
        .unwrap()
        .with_max_terminal_tombstones(1)
        .unwrap();
    let mut io = MemoryTarget::new(config);
    let address = target(443);
    let [a, b, c, d] = [1, 2, 3, 4].map(flow);
    assert_eq!(io.open(a, &address), Ok(TargetOpenCompletion::Opened));
    io.write(a, b"old").unwrap();
    let pointer = io.storage.get(&a).unwrap().written.as_slices().0.as_ptr();
    let before = io.cold_inventory().unwrap();
    let mut snapshot = io.snapshot();
    MemoryTarget::fail_next_node_allocation_for_test();
    assert_eq!(io.open(b, &address), Err(TargetIoError::StorageReserve));
    snapshot.open_attempts += 1;
    assert_eq!(io.snapshot(), snapshot);
    assert_eq!(io.cold_inventory().unwrap(), before);
    assert_eq!(
        io.storage.get(&a).unwrap().written.as_slices().0.as_ptr(),
        pointer
    );
    assert_eq!(io.open(b, &address), Ok(TargetOpenCompletion::Opened));
    io.cancel(a).unwrap();
    assert_eq!(
        io.join(a),
        Ok(TargetJoinCompletion::Joined(TargetTerminalKind::Cancelled))
    );
    io.cancel(b).unwrap();
    assert_eq!(
        io.join(b),
        Ok(TargetJoinCompletion::PendingTerminalCapacity { max: 1 })
    );
    assert_eq!(
        io.retire_terminal_tombstone(a),
        Ok(TargetTombstoneRetireCompletion::Retired(
            TargetTerminalKind::Cancelled
        ))
    );
    assert_eq!(
        io.join(b),
        Ok(TargetJoinCompletion::Joined(TargetTerminalKind::Cancelled))
    );
    MemoryTarget::fail_next_node_allocation_for_test();
    assert_eq!(io.open(c, &address), Ok(TargetOpenCompletion::Opened));
    let before = io.cold_inventory().unwrap();
    assert_eq!(io.open(d, &address), Err(TargetIoError::StorageReserve));
    assert_eq!(io.cold_inventory().unwrap(), before);
    assert_eq!(io.highest_flow_id, Some(c));
    assert_eq!(io.open(d, &address), Ok(TargetOpenCompletion::Opened));
    io.retire_terminal_tombstone(b).unwrap();
    for id in [c, d] {
        io.cancel(id).unwrap();
        assert_eq!(
            io.join(id),
            Ok(TargetJoinCompletion::Joined(TargetTerminalKind::Cancelled))
        );
        io.retire_terminal_tombstone(id).unwrap();
    }
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

fn node(root: &TargetNodeLink, id: SessionFlowId) -> &TargetNode {
    let mut at = root;
    loop {
        let current = &at.as_ref().expect("test key must exist")[0];
        match id.cmp(&current.key) {
            std::cmp::Ordering::Equal => return current,
            std::cmp::Ordering::Less => at = &current.left,
            std::cmp::Ordering::Greater => at = &current.right,
        }
    }
}

#[test]
fn rotations_and_two_child_join_keep_original_boxes_and_address_backing() {
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(127, 8, 16, 8, 8, 1).unwrap());
    let mut original = std::collections::BTreeMap::new();
    for value in 1..=127 {
        let id = flow(value);
        io.open(id, &target(443)).unwrap();
        let ptr = node(&io.storage.live, id) as *const TargetNode;
        let TargetAddr::DomainPort { host, .. } = &io.storage.get(&id).unwrap().target else {
            panic!("domain")
        };
        original.insert(id, (ptr, host.as_ptr()));
        io.cold_inventory().unwrap();
    }
    let mut two_child_removals = 0;
    while let Some(root) = &io.storage.live {
        let id = root[0].key;
        if root[0].left.is_some() && root[0].right.is_some() {
            two_child_removals += 1;
        }
        io.cancel(id).unwrap();
        io.join(id).unwrap();
        let expected = original[&id];
        assert_eq!(
            node(&io.storage.joined, id) as *const TargetNode,
            expected.0
        );
        let TargetAddr::DomainPort { host, .. } = &io.storage.get_joined(&id).unwrap().target
        else {
            panic!("domain")
        };
        assert_eq!(host.as_ptr(), expected.1);
        for (&key, &(ptr, _)) in &original {
            let root = if io.storage.get(&key).is_some() {
                &io.storage.live
            } else {
                &io.storage.joined
            };
            assert_eq!(node(root, key) as *const TargetNode, ptr);
        }
        io.cold_inventory().unwrap();
    }
    assert!(two_child_removals > 20);
    for (&id, &(ptr, _)) in &original {
        io.retire_terminal_tombstone(id).unwrap();
        assert_eq!(
            &io.storage.free.as_ref().unwrap()[0] as *const TargetNode,
            ptr
        );
        io.cold_inventory().unwrap();
    }
    assert_eq!(io.storage.free_len, 127);
    assert!(io.cold_inventory().unwrap().is_quiescent());
    for value in 128..=254 {
        let ptr = &io.storage.free.as_ref().unwrap()[0] as *const TargetNode;
        io.open(flow(value), &target(80)).unwrap();
        assert_eq!(
            node(&io.storage.live, flow(value)) as *const TargetNode,
            ptr
        );
        io.cold_inventory().unwrap();
    }
    assert_eq!((io.storage.live_len, io.storage.free_len), (127, 0));
}

#[test]
fn node_layout_checks_exact_f_plus_z_before_configuration_publication() {
    let base = MemoryTargetConfig::new(1, 8, 8, 8, 8, 1).unwrap();
    assert_eq!(
        base.with_max_terminal_tombstones(usize::MAX),
        Err(TargetIoConfigError::StorageLayoutOverflow)
    );
    let maximum = (isize::MAX as usize) / std::mem::size_of::<TargetNode>();
    assert_eq!(
        base.with_max_terminal_tombstones(maximum),
        Err(TargetIoConfigError::StorageLayoutOverflow)
    );
    assert_eq!(
        base.with_max_terminal_tombstones(0),
        Err(TargetIoConfigError::ZeroCapacity {
            field: "max_terminal_tombstones"
        })
    );
    assert_eq!(base.max_terminal_tombstones(), 1);
    assert!(MemoryTargetStorage::check_layout(1, maximum - 1).is_ok());
    assert_eq!(
        base.with_max_terminal_tombstones(maximum - 1),
        Err(TargetIoConfigError::StorageLayoutOverflow)
    );
    let valid = base.with_max_terminal_tombstones(2).unwrap();
    assert_eq!(valid.max_terminal_tombstones(), 2);
    // Node-only geometry fits; complete configuration also pays other backing.
    assert!(std::alloc::Layout::array::<TargetNode>(maximum).is_ok());
    assert!(std::alloc::Layout::array::<TargetNode>(maximum + 1).is_err());
}

#[test]
fn address_refusal_drops_only_new_candidate_and_never_extracts_vacancy() {
    let config = MemoryTargetConfig::new(2, 8, 16, 8, 8, 1)
        .unwrap()
        .with_max_terminal_tombstones(1)
        .unwrap();
    let mut io = MemoryTarget::new(config);
    let address = target(443);
    let [a, b, c, d] = [1, 2, 3, 4].map(flow);
    io.open(a, &address).unwrap();
    let a_ptr = node(&io.storage.live, a) as *const TargetNode;
    let before = io.cold_inventory().unwrap();
    MemoryTarget::fail_next_node_allocation_for_test();
    memory_io::fail_next_address_allocation_for_test();
    assert_eq!(io.open(b, &address), Err(TargetIoError::StorageReserve)); // Node first.
    assert_eq!(io.open(b, &address), Err(TargetIoError::StorageReserve)); // New node, address refuses.
    assert_eq!(io.cold_inventory().unwrap(), before);
    assert_eq!(io.highest_flow_id, Some(a));
    assert_eq!(io.storage.free_len, 0);
    assert_eq!(node(&io.storage.live, a) as *const TargetNode, a_ptr);
    io.open(b, &address).unwrap();
    io.cancel(a).unwrap();
    io.join(a).unwrap();
    io.retire_terminal_tombstone(a).unwrap();
    io.cancel(b).unwrap();
    io.join(b).unwrap();
    let free_ptr = &io.storage.free.as_ref().unwrap()[0] as *const TargetNode;
    assert_eq!(free_ptr, a_ptr);
    let before = io.cold_inventory().unwrap();
    MemoryTarget::fail_next_node_allocation_for_test();
    memory_io::fail_next_address_allocation_for_test();
    assert_eq!(io.open(c, &address), Err(TargetIoError::StorageReserve));
    assert_eq!(io.cold_inventory().unwrap(), before);
    assert_eq!(io.storage.free_len, 1);
    assert_eq!(
        &io.storage.free.as_ref().unwrap()[0] as *const TargetNode,
        free_ptr
    );
    assert_eq!(io.highest_flow_id, Some(b));
    assert_eq!(io.open(c, &address), Ok(TargetOpenCompletion::Opened));
    assert_eq!(node(&io.storage.live, c) as *const TargetNode, free_ptr);
    assert_eq!(io.open(d, &address), Err(TargetIoError::StorageReserve)); // NODE was not consumed.
    assert_eq!(io.open(d, &address), Ok(TargetOpenCompletion::Opened));
    io.retire_terminal_tombstone(b).unwrap();
    for id in [c, d] {
        io.cancel(id).unwrap();
        io.join(id).unwrap();
        io.retire_terminal_tombstone(id).unwrap();
    }
    assert_eq!(io.storage.free_len, 3);
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

struct DropAtJoin(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl std::task::Wake for DropAtJoin {
    fn wake(self: std::sync::Arc<Self>) {}
}
impl Drop for DropAtJoin {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        panic!("controlled final cached join Waker Drop");
    }
}

#[test]
fn join_waker_drop_unwind_keeps_same_node_joined_and_readiness_removed() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let config = MemoryTargetConfig::new(2, 8, 16, 8, 8, 1)
        .unwrap()
        .with_max_terminal_tombstones(1)
        .unwrap();
    let mut io = MemoryTarget::new(config);
    let [a, b] = [1, 2].map(flow);
    for id in [a, b] {
        io.open(id, &target(443)).unwrap();
        io.cancel(id).unwrap();
    }
    io.join(a).unwrap();
    let ptr = node(&io.storage.live, b) as *const TargetNode;
    let dropped = Arc::new(AtomicUsize::new(0));
    {
        let waker = std::task::Waker::from(Arc::new(DropAtJoin(dropped.clone())));
        assert_eq!(
            io.poll_readiness(
                b,
                TargetReadyInterest::Join,
                &mut Context::from_waker(&waker)
            ),
            Poll::Pending
        );
    }
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    io.retire_terminal_tombstone(a).unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| io.join(b))).is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(io.storage.get(&b).is_none());
    assert_eq!(node(&io.storage.joined, b) as *const TargetNode, ptr);
    assert_eq!(
        io.join(b),
        Ok(TargetJoinCompletion::AlreadyJoined(
            TargetTerminalKind::Cancelled
        ))
    );
    let cold = io.cold_inventory().unwrap();
    assert_eq!((cold.live_flows, cold.terminal_tombstones), (0, 1));
    // Real reentry must use both released registration slots.
    for id in [flow(3), flow(4)] {
        io.open(id, &target(80)).unwrap();
    }
    io.cold_inventory().unwrap();
}

struct PanicRetire(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl std::task::Wake for PanicRetire {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        panic!("controlled terminal capacity wake");
    }
}

#[test]
fn retire_notification_unwind_publishes_vacancy_before_real_reentry() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let config = MemoryTargetConfig::new(1, 8, 8, 8, 8, 1).unwrap();
    let mut io = MemoryTarget::new(config);
    let a = flow(1);
    io.open(a, &target(443)).unwrap();
    let ptr = node(&io.storage.live, a) as *const TargetNode;
    io.cancel(a).unwrap();
    io.join(a).unwrap();
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = std::task::Waker::from(Arc::new(PanicRetire(wakes.clone())));
    assert_eq!(
        io.poll_ready_notifications(&mut Context::from_waker(&waker)),
        Poll::Pending
    );
    assert!(catch_unwind(AssertUnwindSafe(|| io.retire_terminal_tombstone(a))).is_err());
    assert_eq!(wakes.load(Ordering::SeqCst), 1);
    assert!(io.storage.get_joined(&a).is_none());
    assert_eq!(io.storage.free_len, 1);
    assert_eq!(
        &io.storage.free.as_ref().unwrap()[0] as *const TargetNode,
        ptr
    );
    assert!(io.cold_inventory().unwrap().is_quiescent());
    assert_eq!(
        io.retire_terminal_tombstone(a),
        Ok(TargetTombstoneRetireCompletion::Absent)
    );
    assert_eq!(
        io.poll_ready_notifications(&mut Context::from_waker(std::task::Waker::noop())),
        Poll::Ready(Ok(()))
    );
    assert_eq!(
        io.open(flow(2), &target(80)),
        Ok(TargetOpenCompletion::Opened)
    );
    assert_eq!(node(&io.storage.live, flow(2)) as *const TargetNode, ptr);
    io.cold_inventory().unwrap();
}

#[test]
fn native_cold_audit_rejects_defensive_shape_phase_count_and_overlap_corruption() {
    // Deliberate private-state corruption proves defensive rejection only;
    // none of these states are claimed reachable through TargetIo.
    for case in 0..9 {
        let mut io = MemoryTarget::new(MemoryTargetConfig::new(8, 8, 16, 8, 8, 1).unwrap());
        for value in 1..=7 {
            io.open(flow(value), &target(443)).unwrap();
        }
        io.cancel(flow(2)).unwrap();
        io.join(flow(2)).unwrap();
        io.retire_terminal_tombstone(flow(2)).unwrap();
        io.cancel(flow(4)).unwrap();
        io.join(flow(4)).unwrap();
        io.cold_inventory().unwrap();
        match case {
            0 => io.storage.live_len += 1,
            1 => io.storage.live.as_mut().unwrap()[0].height += 1,
            2 => {
                let root = &mut io.storage.live.as_mut().unwrap()[0];
                root.left.as_mut().unwrap()[0].key = root.key;
            }
            3 => io.storage.live.as_mut().unwrap()[0].phase = TargetNodePhase::Vacant,
            4 => {
                io.storage.joined.as_mut().unwrap()[0].key =
                    io.storage.live.as_ref().unwrap()[0].key
            }
            5 => io.storage.free_len += 1,
            6 => io.storage.free.as_mut().unwrap()[0].height = 2,
            7 => {
                io.storage.free.as_mut().unwrap()[0].phase =
                    TargetNodePhase::Joined(JoinedTargetFlow {
                        target: target(80),
                        terminal: TargetTerminalKind::Reset,
                    })
            }
            8 => {
                let extra = Box::new([TargetNode {
                    key: flow(99),
                    phase: TargetNodePhase::Vacant,
                    left: None,
                    right: None,
                    height: 1,
                }]);
                io.storage.free.as_mut().unwrap()[0].right = Some(extra);
            }
            _ => unreachable!(),
        }
        assert!(
            matches!(
                io.cold_inventory(),
                Err(TargetIoColdInventoryError::StoredMismatch {
                    domain: "Target native nodes",
                    ..
                })
            ),
            "defensive mutation {case}"
        );
    }
}

#[test]
fn real_retired_node_chain_drops_on_small_stack_without_recursive_release() {
    let count = 8192;
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(count, 8, 8, 8, 8, 1).unwrap());
    for value in 1..=count as u64 {
        io.open(flow(value), &target(443)).unwrap();
    }
    for value in 1..=count as u64 {
        let id = flow(value);
        io.cancel(id).unwrap();
        io.join(id).unwrap();
        io.retire_terminal_tombstone(id).unwrap();
    }
    assert!(io.cold_inventory().unwrap().is_quiescent());
    assert_eq!(
        (
            io.storage.live_len,
            io.storage.joined_len,
            io.storage.free_len
        ),
        (0, 0, count)
    );
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(move || drop(io))
        .expect("small-stack cleanup thread")
        .join()
        .expect("complete original Target release");
}

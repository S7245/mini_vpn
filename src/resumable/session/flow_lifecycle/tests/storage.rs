//! Native ordered-node ownership, phase partitions and allocation rollback.
//! Model/protocol lifecycle evidence lives in effect_storage/tests/model/lifecycle.

use super::super::*;

fn config(n: usize) -> SessionConfig {
    SessionConfig::new(
        n,
        n,
        TcpWindowLimits::new(64, 8).unwrap(),
        TcpWindowLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReplayBudgetLimits::new(64, 8).unwrap(),
        ReceiveBudgetLimits::new(64, 8).unwrap(),
        64,
    )
    .unwrap()
}
fn flow(config: SessionConfig) -> FlowState {
    FlowState::new(
        TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
        FlowOrigin::Local,
        config,
    )
}
fn terminal(id: SessionFlowId) -> TerminalTombstone {
    TerminalTombstone {
        terminal: TerminalGrace {
            local_flow: LocalFlow {
                session_id: SessionId::new([11; 16]).unwrap(),
                flow_id: id,
                direction: Direction::ClientToTarget,
                authority_id: id.get(),
            },
            tombstone_id: id.get(),
        },
        target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
        replay_control: None,
    }
}
fn audit(
    root: &Link,
    terminal: bool,
    low: Option<SessionFlowId>,
    high: Option<SessionFlowId>,
) -> (u8, usize) {
    let Some(node) = root else { return (0, 0) };
    assert!(low.is_none_or(|low| low < node[0].key));
    assert!(high.is_none_or(|high| node[0].key < high));
    assert!(matches!(
        (&node[0].value, terminal),
        (Value::Live(_), false) | (Value::Terminal(_), true)
    ));
    let (lh, ln) = audit(&node[0].left, terminal, low, Some(node[0].key));
    let (rh, rn) = audit(&node[0].right, terminal, Some(node[0].key), high);
    assert!(lh.abs_diff(rh) <= 1);
    assert_eq!(node[0].height, 1 + lh.max(rh));
    (node[0].height, ln + rn + 1)
}

#[test]
fn original_vacancies_and_live_tree_drop_on_a_bounded_stack() {
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(|| {
            for keep_live in [false, true] {
                let config = config(8192);
                let mut storage = FlowStorage::default();
                for raw in 1..=8192 {
                    let id = SessionFlowId::new(raw).unwrap();
                    let prepared = storage.prepare(id, config).unwrap();
                    storage.insert_prepared(id, flow(config), prepared);
                }
                assert_eq!(audit(&storage.live, false, None, None).1, 8192);
                if !keep_live {
                    for raw in 1..=8192 {
                        let id = SessionFlowId::new(raw).unwrap();
                        drop(storage.retire(id, terminal(id)));
                        drop(storage.expire(id));
                    }
                    assert_eq!(
                        (storage.len(), storage.terminal_len(), storage.free.len),
                        (0, 0, 8192)
                    );
                }
                drop(storage);
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn rotations_retire_the_original_two_child_node_and_reuse_its_allocation() {
    let config = config(256);
    for order in 0..3 {
        let mut storage = FlowStorage::default();
        let mut pointers = std::collections::BTreeMap::new();
        for i in 0..256 {
            let raw = match order {
                0 => i + 1,
                1 => 256 - i,
                _ => ((i * 73) % 256) + 1,
            };
            let id = SessionFlowId::new(raw).unwrap();
            let prepared = storage.prepare(id, config).unwrap();
            pointers.insert(id, &prepared.new.as_ref().unwrap()[0] as *const Node);
            storage.insert_prepared(id, flow(config), prepared);
            assert_eq!(audit(&storage.live, false, None, None).1, storage.len());
        }
        assert_eq!(
            storage.iter().map(|(k, _)| k.get()).collect::<Vec<_>>(),
            (1..=256).collect::<Vec<_>>()
        );
        let root = &storage.live.as_ref().unwrap()[0];
        assert!(root.left.is_some() && root.right.is_some());
        let mut retired = Vec::new();
        while let Some(root) = storage.live.as_ref() {
            let id = root[0].key;
            let original = storage.retire(id, terminal(id));
            assert_eq!(
                original.target,
                TargetAddr::IpPort(([127, 0, 0, 1], 443).into())
            );
            retired.push(id);
            assert_eq!(
                find(&storage.terminal, id).unwrap() as *const Node,
                pointers[&id]
            );
            assert_eq!(audit(&storage.live, false, None, None).1, storage.len());
            assert_eq!(
                audit(&storage.terminal, true, None, None).1,
                storage.terminal_len()
            );
        }
        for (i, id) in retired.into_iter().enumerate() {
            assert_eq!(storage.expire(id).terminal.flow_id(), id);
            let original = &storage.free.head.as_ref().unwrap()[0] as *const Node;
            assert_eq!(original, pointers[&id]);
            let new_id = SessionFlowId::new(1000 + i as u64).unwrap();
            FAIL_NODE.set(true);
            let prepared = storage.prepare(new_id, config).unwrap();
            assert!(prepared.new.is_none());
            drop(prepared);
            assert_eq!(
                &storage.free.head.as_ref().unwrap()[0] as *const Node,
                original
            );
            let prepared = storage.prepare(new_id, config).unwrap();
            storage.insert_prepared(new_id, flow(config), prepared);
            assert_eq!(
                find(&storage.live, new_id).unwrap() as *const Node,
                original
            );
            assert!(FAIL_NODE.replace(false));
            assert_eq!(audit(&storage.live, false, None, None).1, storage.len());
            assert_eq!(
                audit(&storage.terminal, true, None, None).1,
                storage.terminal_len()
            );
        }
        assert_eq!(
            (storage.len(), storage.terminal_len(), storage.free.len),
            (256, 0, 0)
        );
    }
}

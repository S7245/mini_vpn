//! Concrete original-node layout, allocation rollback, ordering and Drop.

#[test]
fn real_session_expiry_recycles_every_original_node_and_retains_backing() {
    let Fixture {
        mut supervisor,
        leg,
        factory,
        flow,
        mut driver,
    } = fixture();
    for _ in 0..128 {
        let input = DriverInput::Data(data(&flow, &mut driver));
        supervisor
            .apply_command(SessionOwnerCommand::Driver(input))
            .unwrap();
    }
    let before = supervisor.uplink_storage_cold_inventory().unwrap();
    let addresses = Nodes::new(&supervisor.uplink_replay.replay)
        .map(|n| n as *const Node as usize)
        .collect::<std::collections::BTreeSet<_>>();
    supervisor.uplink_replay.fail_node = true;
    supervisor.uplink_replay.fail_scratch = true;
    supervisor
        .apply_event(SessionEvent::LegLost { leg })
        .unwrap();
    let effects = supervisor
        .apply_event(SessionEvent::ResumeGraceExpired { leg })
        .unwrap();
    assert!(matches!(
        effects.as_slice(),
        [SessionEffect::SessionExpired]
    ));
    let after = supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!(
        (
            after.replay_nodes,
            after.quarantine_nodes,
            after.vacant_nodes
        ),
        (0, 0, 128)
    );
    assert_eq!(
        (after.node_bytes, after.candidate_bytes),
        (before.node_bytes, before.candidate_bytes)
    );
    assert_eq!(after.candidate_len, 0);
    assert!(after.uplink.is_exact_zero());
    assert_eq!(
        supervisor
            .uplink_replay
            .vacant
            .nodes()
            .map(|n| n as *const Node as usize)
            .collect::<std::collections::BTreeSet<_>>(),
        addresses
    );
    assert!(supervisor.uplink_replay.fail_node && supervisor.uplink_replay.fail_scratch);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
}

#[test]
fn partial_ack_keeps_permits_and_prepared_scratch_without_allocation() {
    let Fixture {
        mut supervisor,
        leg,
        factory,
        flow,
        mut driver,
    } = fixture();
    flow.try_send_uplink_with(4, || Bytes::from_static(b"four"))
        .unwrap();
    supervisor
        .apply_command(SessionOwnerCommand::Driver(
            driver.try_recv_next().unwrap().unwrap(),
        ))
        .unwrap();
    let before = supervisor.uplink_storage_cold_inventory().unwrap();
    let pointer = supervisor.uplink_replay.candidates.as_ptr();
    supervisor.uplink_replay.fail_node = true;
    supervisor.uplink_replay.fail_scratch = true;
    let event = |offset| SessionEvent::PeerFrame {
        leg,
        frame: Frame::try_new(
            leg.generation(),
            Record::Ack {
                flow_id: flow.local_flow().flow_id(),
                direction: Direction::ClientToTarget,
                next_accepted: ByteOffset::new(offset),
                final_accepted: false,
            },
        )
        .unwrap(),
    };
    supervisor.apply_event(event(2)).unwrap();
    assert_eq!(supervisor.uplink_storage_cold_inventory().unwrap(), before);
    assert_eq!(supervisor.uplink_replay.candidates.as_ptr(), pointer);
    assert_eq!(factory.session_uplink_owned_bytes(), 4);
    supervisor.apply_event(event(4)).unwrap();
    let after = supervisor.uplink_storage_cold_inventory().unwrap();
    assert_eq!(
        (after.replay_nodes, after.vacant_nodes, after.candidate_len),
        (0, 1, 0)
    );
    assert_eq!(
        (after.node_bytes, after.candidate_bytes),
        (before.node_bytes, before.candidate_bytes)
    );
    assert!(after.uplink.is_exact_zero());
    assert_eq!(supervisor.uplink_replay.candidates.as_ptr(), pointer);
    assert!(supervisor.uplink_replay.fail_node && supervisor.uplink_replay.fail_scratch);
    assert_eq!(factory.session_uplink_owned_bytes(), 0);
}

#[test]
fn long_vacant_chain_drop_uses_bounded_stack() {
    std::thread::Builder::new()
        .stack_size(64 * 1024)
        .spawn(|| {
            let mut chain = Chain::default();
            for _ in 0..20_000 {
                chain.push(Box::new([Node {
                    value: Value::Vacant,
                    left: None,
                    right: None,
                    height: 1,
                }]));
            }
            assert_eq!(chain.len, 20_000);
            drop(chain);
        })
        .unwrap()
        .join()
        .unwrap();
}

fn audit_nodes(
    root: &Link,
    low: Option<ReplayExtentKey>,
    high: Option<ReplayExtentKey>,
) -> (u8, usize) {
    let Some(node) = root else {
        return (0, 0);
    };
    let key = node[0].key();
    assert!(low.is_none_or(|k| k < key) && high.is_none_or(|k| key < k));
    let Value::Replay { ownership, .. } = &node[0].value else {
        panic!("actual replay node");
    };
    assert_eq!(ReplayExtentKey::of(ownership), key);
    let (lh, ln) = audit_nodes(&node[0].left, low, Some(key));
    let (rh, rn) = audit_nodes(&node[0].right, Some(key), high);
    assert!(lh.abs_diff(rh) <= 1);
    assert_eq!(node[0].height, 1 + lh.max(rh));
    (node[0].height, ln + rn + 1)
}
#[test]
fn original_nodes_preserve_addresses_and_order_through_adversarial_removals() {
    let Fixture {
        mut supervisor,
        factory,
        flow,
        mut driver,
        ..
    } = fixture();
    for _ in 0..128 {
        let input = DriverInput::Data(data(&flow, &mut driver));
        supervisor
            .apply_command(SessionOwnerCommand::Driver(input))
            .unwrap();
    }
    let keys = supervisor.uplink_replay.keys().collect::<Vec<_>>();
    let addresses = Nodes::new(&supervisor.uplink_replay.replay)
        .map(|node| (node.key(), node as *const Node as usize))
        .collect::<std::collections::BTreeMap<_, _>>();
    for round in 0..3 {
        let mut held = Vec::new();
        for i in 0..128 {
            let key = keys[(i * 73 + round * 19) % 128];
            let node = take_node(&mut supervisor.uplink_replay.replay, key).unwrap();
            supervisor.uplink_replay.replay_len -= 1;
            assert_eq!(&node[0] as *const Node as usize, addresses[&key]);
            assert!(node[0].left.is_none() && node[0].right.is_none());
            assert_eq!(
                audit_nodes(&supervisor.uplink_replay.replay, None, None).1,
                127 - i
            );
            held.push(node); // Exact removed owners, no release or replacement.
            assert_eq!(factory.session_uplink_owned_segments(), 128);
        }
        for node in held.into_iter().rev() {
            supervisor.uplink_replay.publish(node);
            assert_eq!(
                audit_nodes(&supervisor.uplink_replay.replay, None, None).1,
                supervisor.uplink_replay.len()
            );
        }
        assert_eq!(supervisor.uplink_replay.keys().collect::<Vec<_>>(), keys);
        assert_eq!(
            supervisor
                .uplink_storage_cold_inventory()
                .unwrap()
                .uplink
                .permit_bundles,
            128
        );
    }
    drop(supervisor);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
}

#[test]
fn corrupt_cached_peak_fails_layout_before_allocation_and_returns_exact_input() {
    let Fixture {
        mut supervisor,
        leg,
        factory,
        flow,
        mut driver,
    } = fixture();
    let input = data(&flow, &mut driver);
    let pointer = input.payload().as_ptr();
    let original = input.cold_inventory();
    // Impossible accounting fault, NOT a reachable valid large session. The
    // real source input still owns its original permits; no huge allocation.
    supervisor.model = SessionModel::new(SessionRole::Client, config(usize::MAX - 1), leg);
    supervisor.uplink_replay.replay_len = isize::MAX as usize / std::mem::size_of::<Node>() + 1;
    supervisor.uplink_replay.fail_node = true;
    let before = supervisor.snapshot();
    let Err(SessionSupervisorError::UplinkStorageLayout { input }) =
        supervisor.apply_owner_data_observed(input)
    else {
        panic!("checked actual Node Layout");
    };
    let DriverInput::Data(data) = &input else {
        panic!("same Driver DATA");
    };
    assert_eq!(data.payload().as_ptr(), pointer);
    assert_eq!(data.cold_inventory(), original);
    assert_eq!(supervisor.snapshot(), before);
    assert!(
        supervisor.uplink_replay.fail_node,
        "Layout precedes even the allocation hook"
    );
    assert!(
        supervisor.uplink_storage_cold_inventory().is_err(),
        "cold audit rejects the fake cached count"
    );
    drop(supervisor);
    assert_eq!(factory.session_uplink_owned_segments(), 1);
    drop(input);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
}

use super::super::*;
use crate::owned_upstream::{
    FlowPortConfig, ResumableTcpDriver, ResumableTcpFlow, ResumableTcpPortFactory,
};
use crate::resumable::*;
use crate::shared::TargetAddr;
use bytes::Bytes;

fn config(segments: usize) -> SessionConfig {
    SessionConfig::new(
        4,
        4,
        TcpWindowLimits::new(4096, 512).unwrap(),
        TcpWindowLimits::new(4096, 512).unwrap(),
        ReplayBudgetLimits::new(4096, segments).unwrap(),
        ReplayBudgetLimits::new(4096, segments).unwrap(),
        ReceiveBudgetLimits::new(8192, 1024).unwrap(),
        1024,
    )
    .unwrap()
}
struct Fixture {
    supervisor: SessionSupervisor,
    leg: CommittedLeg,
    factory: ResumableTcpPortFactory,
    flow: ResumableTcpFlow,
    driver: ResumableTcpDriver,
}
fn fixture() -> Fixture {
    let session = SessionId::new([7; 16]).unwrap();
    let owner = OwnerIdentity::new([8; 32]).unwrap();
    let alpn = AttachAlpn::new(b"mini-vpn-owned/1").unwrap();
    let principal = DevicePrincipal::new([9; 16]).unwrap();
    let credentials = AttachCredentials::new(
        DeviceSecret::new([10; 32]).unwrap(),
        ResumeSecret::new([11; 32]).unwrap(),
    )
    .unwrap();
    let binding = AttachTransportBinding::new(
        owner,
        alpn,
        TlsExporterBinding::new([12; 32]).unwrap(),
        principal,
    );
    let request = AttachRequest::new(
        session,
        LegGeneration::new(2).unwrap(),
        AttachNonce::new([13; 16]).unwrap(),
        VersionRange::new(SESSION_PROTOCOL_VERSION, SESSION_PROTOCOL_VERSION).unwrap(),
        FeatureOffer::new(1, 1).unwrap(),
    );
    let proof = credentials.prove(&request, &binding).unwrap();
    let leg = AttachAuthority::new(
        session,
        LegGeneration::new(1).unwrap(),
        credentials,
        AttachPolicy::new(owner, alpn, principal, SESSION_PROTOCOL_VERSION, 1).unwrap(),
    )
    .verify_and_commit(&request, &binding, &proof)
    .unwrap();
    let mut supervisor =
        SessionSupervisor::new(SessionModel::new(SessionRole::Client, config(512), leg));
    let factory = supervisor
        .mint_tcp_port_factory(FlowPortConfig::new(4096, 512, 512, 512).unwrap())
        .unwrap();
    let effects = supervisor
        .apply_event(SessionEvent::LocalOpen {
            leg,
            target: TargetAddr::IpPort(([127, 0, 0, 1], 443).into()),
        })
        .unwrap();
    let local = effects
        .iter()
        .find_map(|e| match e {
            SessionEffect::LocalFlowOpened { flow } => Some(*flow),
            _ => None,
        })
        .unwrap();
    supervisor
        .apply_event(SessionEvent::PeerFrame {
            leg,
            frame: Frame::try_new(
                leg.generation(),
                Record::OpenResult {
                    flow_id: local.flow_id(),
                    result: OpenResultCode::Opened,
                },
            )
            .unwrap(),
        })
        .unwrap();
    let (flow, driver) = factory.open_flow(local).unwrap();
    Fixture {
        supervisor,
        leg,
        factory,
        flow,
        driver,
    }
}
fn data(flow: &ResumableTcpFlow, driver: &mut ResumableTcpDriver) -> UplinkData {
    flow.try_send_uplink_with(1, || Bytes::from_static(b"x"))
        .unwrap();
    let DriverInput::Data(data) = driver.try_recv_next().unwrap().unwrap() else {
        panic!("real DATA");
    };
    data
}

#[test]
fn missing_node_reserve_returns_unsplit_driver_input_and_allows_exact_retry() {
    let Fixture {
        mut supervisor,
        factory,
        flow,
        mut driver,
        ..
    } = fixture();
    let data = data(&flow, &mut driver);
    let pointer = data.payload().as_ptr();
    let inventory = data.cold_inventory();
    let before = supervisor.snapshot();
    let storage = supervisor.uplink_storage_cold_inventory().unwrap();
    supervisor.uplink_replay.fail_node = true;
    let Err(SessionSupervisorError::UplinkStorageReserve { input }) =
        supervisor.apply_owner_data_observed(data)
    else {
        panic!("node Reserve carrying error");
    };
    let DriverInput::Data(data) = &input else {
        panic!("returned DATA");
    };
    assert_eq!(data.payload().as_ptr(), pointer);
    assert_eq!(data.cold_inventory(), inventory);
    assert_eq!(supervisor.snapshot(), before);
    assert_eq!(supervisor.uplink_storage_cold_inventory().unwrap(), storage);
    assert_eq!(factory.session_uplink_owned_segments(), 1);
    supervisor
        .apply_command(SessionOwnerCommand::Driver(input))
        .unwrap();
    assert_eq!(supervisor.snapshot().uplink_replay_extents, 1);
    drop(supervisor);
    assert_eq!(factory.session_uplink_owned_segments(), 0);
}

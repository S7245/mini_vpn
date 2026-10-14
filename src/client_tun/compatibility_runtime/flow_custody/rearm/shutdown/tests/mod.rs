//! Actual physical ownership, budget and fail-closed shutdown classification.
use super::super::super::transaction::OpenCustody;
use super::super::physical_flow::FlowAdmission;
use super::super::physical_flow::tests::admission::{NeverOpen, receiving_pair};
use super::*;
use crate::client_tun::compatibility_runtime::SessionControl;
use std::sync::atomic::Ordering;

fn dispatch() -> (Arc<SessionControl>, OpenDispatch) {
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _rx) = OpenDispatch::channel(4, Some(owner.clone()));
    (owner, dispatch)
}

#[tokio::test]
async fn physical_scan_shares_control_budget_wraps_and_leaves_unused_epochs_unchanged() {
    let (owner, dispatch) = dispatch();
    let mut sockets = SocketSet::new(vec![]);
    let mut contexts = HashMap::new();
    for port in 10000..10018 {
        let mut socket = TcpSocket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 64]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 64]),
        );
        socket.listen(port).unwrap();
        contexts.insert(sockets.add(socket), SocketCtx::new(port));
    }
    let mut pool = FakeIpPool::new();
    let mut cursor = 0;
    owner.request_stop();
    dispatch.begin_turn().await;
    for _ in 0..29 {
        assert!(owner.take_control());
    }
    dispatch.drain_shutdown_physical(&mut sockets, &mut contexts, &mut pool, &mut cursor, 0);
    assert_eq!(
        (cursor, owner.control_remaining.load(Ordering::Relaxed)),
        (3, 0)
    );
    dispatch.drain_shutdown_physical(&mut sockets, &mut contexts, &mut pool, &mut cursor, 0);
    assert_eq!(cursor, 3);
    dispatch.begin_turn().await;
    dispatch.drain_shutdown_physical(&mut sockets, &mut contexts, &mut pool, &mut cursor, 0);
    assert_eq!(
        (cursor, owner.control_remaining.load(Ordering::Relaxed)),
        (11, 24)
    );
    dispatch.begin_turn().await;
    dispatch.drain_shutdown_physical(&mut sockets, &mut contexts, &mut pool, &mut cursor, 0);
    assert_eq!(
        (cursor, owner.control_remaining.load(Ordering::Relaxed)),
        (0, 25)
    );
    assert!(
        contexts
            .values()
            .all(|ctx| ctx.conn_epoch == 0 && ctx.local_reset.is_none())
    );
    assert!(dispatch.flow_shutdown_complete(&sockets, &mut contexts, &pool));
    assert!(!owner.ledger().quarantined);
    // A stopped default Legacy owner still has legal physical relay resources.
    *owner.opens() = OpenCustody::default();
    contexts.values_mut().next().unwrap().uplink_tx = Some(mpsc::channel(1).0);
    assert!(!dispatch.flow_shutdown_complete(&sockets, &mut contexts, &pool));
    assert!(
        !owner.ledger().quarantined,
        "F census cannot quarantine Legacy"
    );
    assert!(contexts.values().any(|ctx| ctx.uplink_tx.is_some()));
}

#[tokio::test]
async fn unknown_no_flow_identity_or_resources_are_retained_without_reset() {
    for case in 0..9 {
        let (_client, mut server) = receiving_pair().await;
        let (owner, dispatch) = dispatch();
        let mut contexts = HashMap::from([(server.handle, SocketCtx::new(10000))]);
        let mut pool = FakeIpPool::new();
        let mut removed = None;
        {
            let ctx = contexts.get_mut(&server.handle).unwrap();
            match case {
                0 => {
                    removed = contexts.remove(&server.handle);
                }
                1 => ctx.local_port = 0,
                2 => ctx.local_port = 10001,
                3 => ctx.uplink_tx = Some(mpsc::channel(1).0),
                4 => ctx.uplink_buffer.extend_from_slice(b"owned"),
                5 => ctx.d16_egress_phase = EgressPhase::DrainOnly,
                6 => ctx.owned_terminal_wait_since_secs = Some(1),
                7 => ctx.state = SocketState::Closing,
                8 => ctx.fake_ip = Some(Ipv4Addr::new(198, 18, 0, 2)),
                _ => unreachable!(),
            }
        }
        let socket = server.sockets.get::<TcpSocket>(server.handle);
        let before = (
            socket.state(),
            socket.local_endpoint(),
            socket.remote_endpoint(),
            socket.recv_queue(),
        );
        owner.request_stop();
        let deadline = owner.ledger().shutdown_deadline;
        for _ in 0..3 {
            dispatch.begin_turn().await;
            let mut cursor = 0;
            dispatch.drain_shutdown_physical(
                &mut server.sockets,
                &mut contexts,
                &mut pool,
                &mut cursor,
                0,
            );
            assert!(
                !dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool),
                "case {case}"
            );
            assert!(owner.ledger().quarantined);
            assert_eq!(owner.ledger().shutdown_deadline, deadline);
            let socket = server.sockets.get::<TcpSocket>(server.handle);
            assert_eq!(
                (
                    socket.state(),
                    socket.local_endpoint(),
                    socket.remote_endpoint(),
                    socket.recv_queue()
                ),
                before
            );
            if let Some(ctx) = contexts.get(&server.handle).or(removed.as_ref()) {
                assert_eq!(ctx.conn_epoch, 0);
                assert!(ctx.local_reset.is_none());
                match case {
                    3 => assert!(ctx.uplink_tx.is_some()),
                    4 => assert_eq!(ctx.uplink_buffer, b"owned"),
                    5 => assert_eq!(ctx.d16_egress_phase, EgressPhase::DrainOnly),
                    6 => assert_eq!(ctx.owned_terminal_wait_since_secs, Some(1)),
                    8 => assert!(ctx.fake_ip.is_some()),
                    _ => {}
                }
            }
        }
    }
}

#[tokio::test]
async fn root_without_mirror_cannot_be_reclassified_as_bare_socket() {
    let (_client, mut server) = receiving_pair().await;
    let (owner, dispatch) = dispatch();
    let mut pool = FakeIpPool::new();
    let mut contexts = HashMap::from([(server.handle, SocketCtx::new(10000))]);
    dispatch.begin_turn().await;
    dispatch.admit_flow(
        Arc::new(NeverOpen::default()),
        FlowAdmission {
            handle: server.handle,
            socket: server.sockets.get_mut::<TcpSocket>(server.handle),
            ctx: contexts.get_mut(&server.handle).unwrap(),
            fake_pool: &mut pool,
            fake_ip: None,
            target: TargetAddr::DomainPort {
                host: "example.com".into(),
                port: 10000,
            },
            now: 0,
        },
    );
    let before = owner.flow_observation().unwrap();
    assert_eq!((before.live, before.w, before.f), (1, 1, 0));
    owner.request_stop();
    dispatch.begin_turn().await;
    let mut cursor = 0;
    dispatch.drain_shutdown_physical(
        &mut server.sockets,
        &mut contexts,
        &mut pool,
        &mut cursor,
        0,
    );
    let after = owner.flow_observation().unwrap();
    assert_eq!(
        (after.w, after.reserved, after.escrow_bytes, after.released),
        (
            before.w,
            before.reserved,
            before.escrow_bytes,
            before.released
        )
    );
    assert_eq!(owner.control_remaining.load(Ordering::Relaxed), 31);
    assert!(!owner.ledger().quarantined);
    let ctx = contexts.get_mut(&server.handle).unwrap();
    let mirror = ctx.flow_mirror.take().unwrap(); // Retain the contradictory physical capability.
    ctx.state = SocketState::Listening;
    let epoch = ctx.conn_epoch;
    let tuple = server
        .sockets
        .get::<TcpSocket>(server.handle)
        .remote_endpoint();
    cursor = 0;
    dispatch.drain_shutdown_physical(
        &mut server.sockets,
        &mut contexts,
        &mut pool,
        &mut cursor,
        0,
    );
    assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
    assert!(owner.ledger().quarantined);
    assert_eq!(
        server
            .sockets
            .get::<TcpSocket>(server.handle)
            .remote_endpoint(),
        tuple
    );
    assert_eq!(contexts[&server.handle].conn_epoch, epoch);
    assert!(contexts[&server.handle].local_reset.is_none());
    assert!(mirror.matches_root(&owner.opens()));
    assert_eq!(owner.flow_observation().unwrap().released, 0);
}

#[tokio::test]
async fn orphan_context_fails_reverse_physical_census() {
    let (_client, mut server) = receiving_pair().await;
    let (owner, dispatch) = dispatch();
    let mut contexts = HashMap::from([(server.handle, SocketCtx::new(10000))]);
    let socket = server.sockets.remove(server.handle);
    owner.request_stop();
    assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &FakeIpPool::new()));
    assert!(owner.ledger().quarantined);
    assert_eq!(contexts[&server.handle].conn_epoch, 0);
    assert!(matches!(socket, smoltcp::socket::Socket::Tcp(_)));
}

#[tokio::test(start_paused = true)]
async fn delayed_no_flow_reset_keeps_first_root_shutdown_deadline() {
    let (_client, mut server) = receiving_pair().await;
    let (owner, dispatch) = dispatch();
    let mut contexts = HashMap::from([(server.handle, SocketCtx::new(10000))]);
    let mut pool = FakeIpPool::new();
    owner.request_stop();
    let deadline = owner.ledger().shutdown_deadline.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    dispatch.begin_turn().await;
    let mut cursor = 0;
    dispatch.drain_shutdown_physical(
        &mut server.sockets,
        &mut contexts,
        &mut pool,
        &mut cursor,
        0,
    );
    assert!(contexts[&server.handle].local_reset.is_some());
    assert!(
        !contexts[&server.handle]
            .local_reset
            .as_ref()
            .unwrap()
            .quarantined()
    );
    let socket = server.sockets.get::<TcpSocket>(server.handle);
    assert_eq!(socket.state(), TcpState::Closed);
    let tuple = socket.remote_endpoint();
    assert!(tuple.is_some(), "actual RST remains physical work");
    assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
    tokio::time::advance(std::time::Duration::from_secs(13)).await;
    assert_eq!(tokio::time::Instant::now(), deadline);
    cursor = 0;
    dispatch.begin_turn().await;
    dispatch.drain_shutdown_physical(
        &mut server.sockets,
        &mut contexts,
        &mut pool,
        &mut cursor,
        0,
    );
    assert!(
        contexts[&server.handle]
            .local_reset
            .as_ref()
            .unwrap()
            .quarantined()
    );
    assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
    assert!(owner.ledger().quarantined);
    assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
    assert_eq!(contexts[&server.handle].conn_epoch, 0);
    assert_eq!(
        server
            .sockets
            .get::<TcpSocket>(server.handle)
            .remote_endpoint(),
        tuple
    );
}

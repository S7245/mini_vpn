//! Real pre-receive admission with the retained allocator at its identity bounds.
use super::super::super::rearm::FlowAdmission;
use super::super::super::rearm::physical_flow::tests::admission::{NeverOpen, receiving_pair};
use super::super::*;
use crate::fake_ip::FakeIpPool;
use crate::shared::TargetAddr;
use std::collections::HashMap;

fn setup() -> (Arc<SessionControl>, OpenDispatch, Arc<NeverOpen>) {
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    (owner, dispatch, Arc::new(NeverOpen::default()))
}

#[tokio::test]
async fn identity_exhaustion_retains_fault_before_recv_without_inventing_flow() {
    for (epoch, generation_exhausted) in [(u64::MAX, false), (u64::MAX - 1, false), (0, true)] {
        let (_client, mut server) = receiving_pair().await;
        let (owner, dispatch, upstream) = setup();
        let handle = server.handle;
        let mut ctx = SocketCtx::new(10000);
        ctx.conn_epoch = epoch;
        if generation_exhausted {
            owner.opens().flow.as_mut().unwrap().generation = u64::MAX;
        }
        let mut contexts = HashMap::from([(handle, ctx)]);
        let mut pool = FakeIpPool::new();
        for _ in 0..4 {
            dispatch.begin_turn().await;
            dispatch.admit_flow(
                upstream.clone(),
                FlowAdmission {
                    handle,
                    socket: server.sockets.get_mut::<TcpSocket>(handle),
                    ctx: contexts.get_mut(&handle).unwrap(),
                    fake_pool: &mut pool,
                    fake_ip: None,
                    target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()),
                    now: 0,
                },
            );
            assert!(
                owner.ledger().quarantined,
                "permanent identity exhaustion is not temporary pressure"
            );
            let opens = owner.opens();
            assert!(opens.audit_flows() && opens.slots.iter().all(Option::is_none));
            let accounting = opens.flow.as_ref().unwrap();
            let expected = if generation_exhausted {
                AdmissionIdentityFault::GenerationExhausted {
                    handle,
                    generation: u64::MAX,
                }
            } else {
                AdmissionIdentityFault::EpochExhausted { handle, epoch }
            };
            assert_eq!(accounting.identity_fault, Some(expected));
            assert_eq!(
                (opens.live, accounting.w, accounting.f, accounting.bytes),
                (0, 0, 0, 0)
            );
            assert!(contexts[&handle].flow_mirror.is_none());
            assert_eq!(
                server.sockets.get::<TcpSocket>(handle).recv_queue(),
                b"untouched-rx".len()
            );
            assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
            let ledger = owner.ledger();
            let failure = ledger.first_failure.as_ref().unwrap().facts();
            assert_eq!(failure.io_kind, Some(std::io::ErrorKind::InvalidData));
            assert_eq!(failure.origin.resource, FailureResource::Session);
            assert_eq!(failure.origin.phase, FailurePhase::TcpOpen);
            assert_eq!(ledger.additional_failures, 0);
        }
        assert_eq!(
            owner.flow_observation().unwrap().retained_identity_faults,
            1
        );
        owner.request_stop();
        owner.settle_opens().await;
        assert_eq!(
            owner.flow_observation().unwrap().retained_identity_faults,
            1
        );
        assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
        assert_eq!(
            server.sockets.get::<TcpSocket>(handle).recv_queue(),
            b"untouched-rx".len()
        );
    }
}

#[tokio::test]
async fn last_generation_admits_once_and_next_exhaustion_preserves_existing_row() {
    let (_first_client, mut first) = receiving_pair().await;
    let (_second_client, mut second) = receiving_pair().await;
    let (owner, dispatch, upstream) = setup();
    owner.opens().flow.as_mut().unwrap().generation = u64::MAX - 1;
    let first_handle = first.handle;
    // Each socket arrived over its own real packet link; move the second intact
    // socket into this test's sole physical set only to provide a distinct handle.
    let smoltcp::socket::Socket::Tcp(second_socket) = second.sockets.remove(second.handle) else {
        panic!("real TCP fixture");
    };
    let second_handle = first.sockets.add(second_socket);
    let mut contexts = HashMap::from([
        (first_handle, SocketCtx::new(10000)),
        (second_handle, SocketCtx::new(10000)),
    ]);
    contexts.get_mut(&first_handle).unwrap().conn_epoch = u64::MAX - 2;
    let mut pool = FakeIpPool::new();
    dispatch.begin_turn().await;
    dispatch.admit_flow(
        upstream.clone(),
        FlowAdmission {
            handle: first_handle,
            socket: first.sockets.get_mut::<TcpSocket>(first_handle),
            ctx: contexts.get_mut(&first_handle).unwrap(),
            fake_pool: &mut pool,
            fake_ip: None,
            target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()),
            now: 0,
        },
    );
    let id = {
        let opens = owner.opens();
        let OpenPhase::Flow(flow) = &opens.slots.iter().flatten().next().unwrap().phase else {
            panic!("actual admitted row");
        };
        flow.id
    };
    assert_eq!((id.epoch, id.generation), (u64::MAX - 1, u64::MAX));
    assert_eq!(
        id.epoch.checked_add(1),
        Some(u64::MAX),
        "reserve room for the real physical receipt"
    );
    assert!(!owner.ledger().quarantined);
    for _ in 0..4 {
        dispatch.begin_turn().await;
        dispatch.admit_flow(
            upstream.clone(),
            FlowAdmission {
                handle: second_handle,
                socket: first.sockets.get_mut::<TcpSocket>(second_handle),
                ctx: contexts.get_mut(&second_handle).unwrap(),
                fake_pool: &mut pool,
                fake_ip: None,
                target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()),
                now: 0,
            },
        );
        let opens = owner.opens();
        assert!(opens.audit_flows());
        let OpenPhase::Flow(flow) = &opens.slots[id.index].as_ref().unwrap().phase else {
            panic!("actual admitted flow");
        };
        assert_eq!(flow.id, id);
        assert_eq!(&flow.escrow, b"untouched-rx");
        assert_eq!(flow.stage, FlowStage::Waiting);
        let a = opens.flow.as_ref().unwrap();
        assert_eq!(
            a.identity_fault,
            Some(AdmissionIdentityFault::GenerationExhausted {
                handle: second_handle,
                generation: u64::MAX
            })
        );
        assert_eq!(
            (opens.live, a.w, a.f, a.bytes),
            (1, 1, 0, MAX_UPLINK_BUFFER)
        );
        assert!(contexts[&second_handle].flow_mirror.is_none());
        assert_eq!(
            first.sockets.get::<TcpSocket>(second_handle).recv_queue(),
            b"untouched-rx".len()
        );
        assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
        assert!(owner.ledger().quarantined);
        assert_eq!(owner.ledger().additional_failures, 0);
    }
    owner.request_stop();
    owner.settle_opens().await;
    assert!(!dispatch.flow_shutdown_complete(&first.sockets, &mut contexts, &pool));
}

#[tokio::test]
async fn ordinary_w_capacity_wait_keeps_identity_and_rx_without_integrity_fault() {
    let (_client, mut server) = receiving_pair().await;
    let (owner, dispatch, upstream) = setup();
    let mut reserved = Vec::new();
    // This is the root's real pre-receive reservation API, not fabricated slots
    // or successful provider publication. Full saved-loop F4096 pressure is later.
    for _ in 0..OPEN_LIMIT {
        let handle = server.sockets.add(crate::client_tun::build_listener_socket(
            &crate::client_tun::ListenerSpec { local_port: 10001 },
        ));
        dispatch.begin_turn().await;
        reserved.push(
            dispatch
                .reserve_flow(
                    upstream.clone(),
                    TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()),
                    handle,
                    0,
                )
                .unwrap(),
        );
    }
    let before_generation = owner.opens().flow.as_ref().unwrap().generation;
    let handle = server.handle;
    let mut ctx = SocketCtx::new(10000);
    let mut pool = FakeIpPool::new();
    for _ in 0..4 {
        dispatch.begin_turn().await;
        dispatch.admit_flow(
            upstream.clone(),
            FlowAdmission {
                handle,
                socket: server.sockets.get_mut::<TcpSocket>(handle),
                ctx: &mut ctx,
                fake_pool: &mut pool,
                fake_ip: None,
                target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()),
                now: 0,
            },
        );
        let opens = owner.opens();
        assert!(opens.audit_flows());
        let a = opens.flow.as_ref().unwrap();
        assert_eq!(
            (opens.live, a.w, a.f, a.bytes),
            (OPEN_LIMIT, OPEN_LIMIT, 0, OPEN_LIMIT * MAX_UPLINK_BUFFER)
        );
        assert_eq!(a.generation, before_generation);
        assert!(a.identity_fault.is_none());
        assert_eq!(
            server.sockets.get::<TcpSocket>(handle).recv_queue(),
            b"untouched-rx".len()
        );
        let ledger = owner.ledger();
        assert!(!ledger.quarantined && ledger.first_failure.is_none());
        assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
    }
    assert_eq!(reserved.len(), OPEN_LIMIT);
    // These actual root reservations remain nonzero, with no worker/provider to
    // detach; this fixture does not claim a clean physical shutdown receipt.
}

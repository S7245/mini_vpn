//! Real smoltcp receive ownership at the synchronous F admission boundary.
//! This injects a post-resolve missing-mapping contradiction, not a saved-loop
//! corruption oracle. No provider success, task, or directly filled flow slot.
use super::super::*;
use crate::client_tun::compatibility_runtime::SessionControl;
use crate::client_tun::compatibility_runtime::flow_custody::transaction::{OpenCustody, OpenPhase};
use crate::device::TunIo;
use crate::harness::{LoopbackTunDevice, PacketLink};
use crate::owned_upstream::{OpenedOwnedTcp, OwnedUpstream, UdpUplink};
use crate::upstream::{StagedTcpOpen, TcpOpenRequest, TcpOpenUnavailable};
use smoltcp::iface::{Config, Interface};
use smoltcp::wire::{HardwareAddress, IpCidr};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub(in crate::client_tun::compatibility_runtime::flow_custody) struct NeverOpen(
    pub(in crate::client_tun::compatibility_runtime::flow_custody) AtomicUsize,
);

#[async_trait::async_trait]
impl OwnedUpstream for NeverOpen {
    fn staged_tcp_open(self: Arc<Self>, _: TcpOpenRequest) -> StagedTcpOpen<OpenedOwnedTcp> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(TcpOpenUnavailable::Unsupported)
    }
    async fn open_tcp_flow(&self, _: &TargetAddr) -> Result<OpenedOwnedTcp, ClientError> {
        panic!("pre-receive rejection cannot use the opaque factory");
    }
    async fn send_udp(&self, _: UdpUplink<'_>) {
        panic!("this fixture has no UDP");
    }
}

pub(in crate::client_tun::compatibility_runtime::flow_custody) struct Endpoint {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) device: LoopbackTunDevice,
    pub(in crate::client_tun::compatibility_runtime::flow_custody) iface: Interface,
    pub(in crate::client_tun::compatibility_runtime::flow_custody) sockets: SocketSet<'static>,
    pub(in crate::client_tun::compatibility_runtime::flow_custody) handle: SocketHandle,
}

impl Endpoint {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn new(
        inbound: PacketLink,
        outbound: PacketLink,
        listener: bool,
    ) -> Self {
        let mut device = LoopbackTunDevice::new(inbound, outbound);
        let mut iface = Interface::new(
            Config::new(HardwareAddress::Ip),
            &mut device,
            smoltcp::time::Instant::now(),
        );
        let local = smoltcp::wire::Ipv4Address::new(10, 0, 0, if listener { 2 } else { 1 });
        iface.update_ip_addrs(|ips| ips.push(IpCidr::new(local.into(), 24)).unwrap());
        let mut socket = TcpSocket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 4096]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 4096]),
        );
        if listener {
            socket.listen(10000).unwrap();
        } else {
            socket
                .connect(
                    iface.context(),
                    (smoltcp::wire::IpAddress::v4(10, 0, 0, 2), 10000),
                    20000,
                )
                .unwrap();
        }
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(socket);
        Self {
            device,
            iface,
            sockets,
            handle,
        }
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) async fn turn(&mut self) {
        while self.device.try_recv_rx().unwrap() {
            assert!(self.device.stage_rx_for_iface());
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        self.device.flush_tx().await.unwrap();
    }
}

pub(in crate::client_tun::compatibility_runtime::flow_custody) async fn receiving_pair()
-> (Endpoint, Endpoint) {
    let a = PacketLink::bounded(16);
    let b = PacketLink::bounded(16);
    let mut client = Endpoint::new(a.clone(), b.clone(), false);
    let mut server = Endpoint::new(b, a, true);
    let payload = b"untouched-rx";
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let mut sent = false;
        loop {
            client.turn().await;
            if !sent && client.sockets.get::<TcpSocket>(client.handle).can_send() {
                assert_eq!(
                    client
                        .sockets
                        .get_mut::<TcpSocket>(client.handle)
                        .send_slice(payload)
                        .unwrap(),
                    payload.len()
                );
                sent = true;
            }
            server.turn().await;
            if server.sockets.get::<TcpSocket>(server.handle).can_recv() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    (client, server)
}

#[tokio::test]
async fn failed_checked_admission_preserves_rx_and_nonzero_custody_without_retry() {
    let (_client, mut server) = receiving_pair().await;
    let payload = b"untouched-rx";

    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, mut receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let upstream = Arc::new(NeverOpen::default());
    let mut pool = FakeIpPool::new();
    let ip = Ipv4Addr::new(198, 18, 0, 2);
    let mut contexts = HashMap::from([(server.handle, SocketCtx::new(10000))]);
    for _ in 0..4 {
        dispatch.begin_turn().await;
        dispatch.admit_flow(
            upstream.clone(),
            FlowAdmission {
                handle: server.handle,
                socket: server.sockets.get_mut::<TcpSocket>(server.handle),
                ctx: contexts.get_mut(&server.handle).unwrap(),
                fake_pool: &mut pool,
                fake_ip: Some(ip),
                target: TargetAddr::DomainPort {
                    host: "lost.test".into(),
                    port: 10000,
                },
                now: 0,
            },
        );
        let snapshot = owner.flow_observation().unwrap();
        assert_eq!(
            (snapshot.live, snapshot.w, snapshot.f),
            (1, 1, 0),
            "an acquire contradiction cannot erase its reservation and retry the same RX"
        );
        assert_eq!(snapshot.reserved, MAX_UPLINK_BUFFER);
        assert_eq!(
            (
                snapshot.retained_capacity,
                snapshot.published,
                snapshot.released
            ),
            (0, 0, 0)
        );
        assert!(snapshot.audit);
        assert_eq!(
            server.sockets.get::<TcpSocket>(server.handle).recv_queue(),
            payload.len()
        );
        assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
        assert!(owner.ledger().quarantined);
        let ctx = &contexts[&server.handle];
        assert!(ctx.fake_ip.is_none(), "no lease was acquired");
        assert_eq!(ctx.conn_epoch, 1);
        let mirror = ctx.flow_mirror.as_ref().unwrap();
        assert!(mirror.quarantined && mirror.lease.is_none());
        let opens = owner.opens();
        let OpenPhase::Flow(flow) = &opens.slots[mirror.id.index].as_ref().unwrap().phase else {
            panic!("actual F row");
        };
        assert_eq!(flow.stage, FlowStage::Quarantined);
        assert_eq!(
            flow.fake_ip_failure,
            Some((ip, crate::fake_ip::CheckedFakeIpError::MissingMapping))
        );
        assert!(flow.attempt.is_none() && flow.roles.is_none() && flow.escrow.is_empty());
    }

    // Drive the real control action and original physical gateway after stop.
    owner.request_stop();
    let event = std::future::poll_fn(|cx| {
        let mut recv = Box::pin(dispatch.recv(&mut receiver));
        std::future::Future::poll(recv.as_mut(), cx)
    })
    .await
    .unwrap();
    let (global_tx, _global_rx) = mpsc::channel(4);
    let (owned_action_tx, _owned_rx) = mpsc::channel(4);
    crate::client_tun::HandshakeCompletion::from_loop(
        &dispatch,
        event,
        crate::client_tun::HandshakeCompletion {
            sockets: &mut server.sockets,
            socket_ctxs: &mut contexts,
            global_tx: &global_tx,
            owned_action_tx: &owned_action_tx,
            fake_pool: &mut pool,
            now_secs: 0,
            metrics_handle: &crate::metrics::Metrics::default(),
            downlink_backpressure: DownlinkBackpressureConfig::default(),
            relay_read_hard_pause: false,
        },
    );
    legacy_bare_rearm_from_loop(
        &dispatch,
        server.handle,
        server.sockets.get_mut::<TcpSocket>(server.handle),
        contexts.get_mut(&server.handle).unwrap(),
        &mut pool,
        0,
    );
    assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
    owner.settle_opens().await;
    let after = owner.flow_observation().unwrap();
    assert_eq!((after.live, after.w, after.f, after.released), (1, 1, 0, 0));
    assert_eq!(after.stopped_producers, 1);
    assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
    let socket = server.sockets.get_mut::<TcpSocket>(server.handle);
    let mut bytes = vec![0; payload.len()];
    assert_eq!(socket.peek_slice(&mut bytes).unwrap(), payload.len());
    assert_eq!(&bytes, payload);
    let ledger = owner.ledger();
    assert!(ledger.quarantined);
    assert_eq!(
        ledger.additional_failures, 0,
        "repeated observation does not duplicate the source error"
    );
    let facts = ledger.first_failure.as_ref().unwrap().facts();
    assert_eq!(facts.io_kind, Some(std::io::ErrorKind::InvalidData));
    assert_eq!(facts.origin.attempt_id, Some(1));
    // Deliberately unresolved quarantine, not a clean settlement claim. This
    // bounded fixture owns no tasks/provider resources to detach when dropped.
}

#[tokio::test]
async fn flow_and_local_reset_conflict_retains_both_physical_owners() {
    let (_client, mut server) = receiving_pair().await;
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let upstream = Arc::new(NeverOpen::default());
    let handle = server.handle;
    let mut pool = FakeIpPool::new();
    let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
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
    let id = contexts[&handle].flow_mirror.as_ref().unwrap().id;
    // Inject two conflicting authorities only after the actual admission made
    // its real row and consumed RX. This is a corruption test, not normal entry.
    assert!(!LocalReset::prepare(
        contexts.get_mut(&handle).unwrap(),
        server.sockets.get_mut::<TcpSocket>(handle),
        None
    ));
    let (global_tx, _global_rx) = mpsc::channel(4);
    let (owned_action_tx, _owned_rx) = mpsc::channel(4);
    for _ in 0..2 {
        let mut completion = crate::client_tun::HandshakeCompletion {
            sockets: &mut server.sockets,
            socket_ctxs: &mut contexts,
            global_tx: &global_tx,
            owned_action_tx: &owned_action_tx,
            fake_pool: &mut pool,
            now_secs: 0,
            metrics_handle: &crate::metrics::Metrics::default(),
            downlink_backpressure: DownlinkBackpressureConfig::default(),
            relay_read_hard_pause: false,
        };
        assert!(!dispatch.service_flow(id, &mut completion));
        let opens = owner.opens();
        let OpenPhase::Flow(flow) = &opens.slots[id.index].as_ref().unwrap().phase else {
            panic!("actual admitted row");
        };
        assert_eq!(flow.stage, FlowStage::Quarantined);
        assert_eq!(&flow.escrow, b"untouched-rx");
        assert!(flow.attempt.is_none() && flow.roles.is_none());
        assert!(contexts[&handle].local_reset.is_some());
        assert_eq!(contexts[&handle].flow_mirror.as_ref().unwrap().id, id);
        assert_eq!(contexts[&handle].conn_epoch, id.epoch);
        assert_eq!(
            server.sockets.get::<TcpSocket>(handle).state(),
            TcpState::Closed
        );
        assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
        assert!(owner.ledger().quarantined);
    }
    owner.request_stop();
    assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
    let after = owner.flow_observation().unwrap();
    assert_eq!((after.live, after.w, after.f, after.released), (1, 1, 0, 0));
}

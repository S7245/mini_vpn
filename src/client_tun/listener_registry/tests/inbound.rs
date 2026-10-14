//! Actual TCP tuples drive conservative dirty selection; contexts never fake state.
use super::super::*;
use crate::device::TunIo;
use crate::harness::{LoopbackTunDevice, PacketLink};
use smoltcp::iface::{Config, Interface};
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, Ipv4Address, Ipv4Packet, TcpPacket};
use std::time::Duration;

mod targeted;

struct Endpoint {
    device: LoopbackTunDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
}

impl Endpoint {
    fn new(inbound: PacketLink, outbound: PacketLink, address: u8) -> Self {
        let mut device = LoopbackTunDevice::new(inbound, outbound);
        let mut iface = Interface::new(
            Config::new(HardwareAddress::Ip),
            &mut device,
            smoltcp::time::Instant::now(),
        );
        iface.update_ip_addrs(|ips| {
            ips.push(IpCidr::new(Ipv4Address::new(10, 0, 0, address).into(), 24))
                .unwrap()
        });
        Self {
            device,
            iface,
            sockets: SocketSet::new(vec![]),
        }
    }

    async fn poll(&mut self) {
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        self.device.flush_tx().await.unwrap();
    }

    async fn turn(&mut self) {
        while self.device.try_recv_rx().unwrap() {
            assert!(self.device.stage_rx_for_iface());
        }
        self.poll().await;
    }
}

struct Pair {
    client: Endpoint,
    server: Endpoint,
    registry: ListenerRegistry,
    contexts: HashMap<SocketHandle, SocketCtx>,
    dirty: HashSet<SocketHandle>,
    clients: [SocketHandle; 2],
    last: Option<Vec<u8>>,
    last_fin: Option<Vec<u8>>,
}

impl Pair {
    fn new() -> Self {
        let uplink = PacketLink::bounded(64);
        let downlink = PacketLink::bounded(64);
        let mut client = Endpoint::new(downlink.clone(), uplink.clone(), 1);
        let mut server = Endpoint::new(uplink, downlink, 2);
        let mut registry = ListenerRegistry::new(4);
        let mut contexts = HashMap::new();
        registry
            .ensure_port(10000, &mut server.sockets, &mut contexts)
            .unwrap();
        let clients = std::array::from_fn(|index| {
            let mut socket = TcpSocket::new(
                TcpSocketBuffer::new(vec![0; 4096]),
                TcpSocketBuffer::new(vec![0; 4096]),
            );
            configure_local_tcp_socket(&mut socket);
            socket
                .connect(
                    client.iface.context(),
                    (IpAddress::v4(10, 0, 0, 2), 10000),
                    20000 + index as u16,
                )
                .unwrap();
            client.sockets.add(socket)
        });
        Self {
            client,
            server,
            registry,
            contexts,
            dirty: HashSet::new(),
            clients,
            last: None,
            last_fin: None,
        }
    }

    fn stage_server(&mut self) -> usize {
        let mut count = 0;
        while self.server.device.try_recv_rx().unwrap() {
            let packet = self.server.device.rx_peek().unwrap();
            assert!(packet.len() <= 1500);
            let ip = Ipv4Packet::new_checked(packet).unwrap();
            let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
            if tcp.fin() {
                self.last_fin = Some(packet.to_vec());
            }
            self.last = Some(packet.to_vec());
            self.registry.prepare_inbound_tcp(
                Some(packet),
                &mut self.server.sockets,
                &mut self.contexts,
                &mut self.dirty,
            );
            assert!(self.server.device.stage_rx_for_iface());
            count += 1;
        }
        count
    }

    async fn round(&mut self) {
        self.client.turn().await;
        self.stage_server();
        self.server.poll().await;
    }

    async fn until(&mut self, done: impl Fn(&Self) -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while !done(self) {
                self.round().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    fn server_handle(&self, port: u16) -> SocketHandle {
        self.registry
            .handles_for_port(10000)
            .iter()
            .copied()
            .find(|h| {
                self.server
                    .sockets
                    .get::<TcpSocket>(*h)
                    .remote_endpoint()
                    .is_some_and(|endpoint| endpoint.port == port)
            })
            .unwrap()
    }

    fn state(&self, handle: SocketHandle) -> TcpState {
        self.server.sockets.get::<TcpSocket>(handle).state()
    }

    fn listeners(&self) -> HashSet<SocketHandle> {
        self.registry
            .handles_for_port(10000)
            .iter()
            .copied()
            .filter(|h| self.state(*h) == TcpState::Listen)
            .collect()
    }

    async fn connected() -> Self {
        let mut pair = Self::new();
        pair.client.turn().await;
        assert_eq!(
            pair.stage_server(),
            2,
            "both real SYNs stage before iface.poll"
        );
        assert_eq!(pair.dirty, pair.listeners());
        assert_eq!(
            pair.dirty.len(),
            4,
            "do not preselect just one listening socket"
        );
        pair.server.poll().await;
        pair.until(|p| {
            p.registry
                .handles_for_port(10000)
                .iter()
                .filter(|h| p.state(**h) == TcpState::Established)
                .count()
                == 2
        })
        .await;
        assert_eq!(pair.listeners().len(), 2);
        // No synthetic SocketCtx transitions: matching must use physical sockets.
        assert!(
            pair.contexts
                .values()
                .all(|ctx| ctx.state == super::super::super::SocketState::Listening)
        );
        pair
    }

    fn inspect_only(&mut self, packet: &[u8]) {
        self.registry.prepare_inbound_tcp(
            Some(packet),
            &mut self.server.sockets,
            &mut self.contexts,
            &mut self.dirty,
        );
    }
}

#[tokio::test]
async fn actual_tcp_packet_marks_only_its_tuple_and_listeners_preserving_prior_dirty() {
    let mut pair = Pair::connected().await;
    let first = pair.server_handle(20000);
    let second = pair.server_handle(20001);
    pair.dirty.clear();
    pair.client
        .sockets
        .get_mut::<TcpSocket>(pair.clients[0])
        .send_slice(b"first")
        .unwrap();
    pair.until(|p| p.server.sockets.get::<TcpSocket>(first).recv_queue() == 5)
        .await;
    let mut expected = pair.listeners();
    expected.insert(first);
    assert_eq!(
        pair.dirty, expected,
        "an unrelated established tuple must not be re-marked"
    );
    assert!(!pair.dirty.contains(&second));
    let payload = pair.last.clone().unwrap();

    pair.dirty.insert(second);
    pair.inspect_only(&payload);
    expected.insert(second);
    assert_eq!(pair.dirty, expected, "existing work remains scheduled");

    // Fully known but different IP or source port cannot select either established tuple.
    for offset in [12, 16, 20] {
        let mut unknown = payload.clone();
        unknown[offset] ^= 1;
        pair.dirty.clear();
        pair.inspect_only(&unknown);
        assert_eq!(pair.dirty, pair.listeners());
    }
}

#[tokio::test]
async fn actual_fin_time_wait_retransmission_and_reset_keep_the_physical_target_dirty() {
    let mut pair = Pair::connected().await;
    let first = pair.server_handle(20000);
    let second = pair.server_handle(20001);
    pair.server.sockets.get_mut::<TcpSocket>(first).close();
    pair.until(|p| p.client.sockets.get::<TcpSocket>(p.clients[0]).state() == TcpState::CloseWait)
        .await;
    pair.client
        .sockets
        .get_mut::<TcpSocket>(pair.clients[0])
        .close();
    pair.until(|p| p.state(first) == TcpState::TimeWait).await;
    let fin = pair.last_fin.clone().unwrap();
    pair.dirty.clear();
    pair.inspect_only(&fin);
    let mut expected = pair.listeners();
    expected.insert(first);
    assert_eq!(
        pair.dirty, expected,
        "TimeWait is a real tuple despite !is_active"
    );

    pair.dirty.clear();
    pair.client
        .sockets
        .get_mut::<TcpSocket>(pair.clients[1])
        .abort();
    pair.until(|p| p.state(second) == TcpState::Closed).await;
    let mut expected = pair.listeners();
    expected.insert(second);
    assert_eq!(
        pair.dirty, expected,
        "actual RST target stays dirty after physical close"
    );
}

#[tokio::test]
async fn parsed_port_without_safe_ipv4_tuple_keeps_original_fallback_and_bad_input_is_inert() {
    let mut pair = Pair::connected().await;
    let first = pair.server_handle(20000);
    pair.client
        .sockets
        .get_mut::<TcpSocket>(pair.clients[0])
        .send_slice(b"fragment-probe")
        .unwrap();
    pair.until(|p| p.server.sockets.get::<TcpSocket>(first).recv_queue() > 0)
        .await;
    let mut fragment = pair.last.clone().unwrap();
    fragment[6] |= 0x20;
    let builder = etherparse::PacketBuilder::ipv6([1; 16], [2; 16], 64)
        .tcp(20000, 10000, 1, 4096)
        .ack(1);
    let mut ipv6 = Vec::with_capacity(builder.size(1));
    builder.write(&mut ipv6, b"x").unwrap();
    let all = pair
        .registry
        .handles_for_port(10000)
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    for packet in [fragment, ipv6] {
        pair.dirty.clear();
        pair.inspect_only(&packet);
        assert_eq!(
            pair.dirty, all,
            "fallback does not change protocol/fragment support"
        );
    }
    pair.dirty.clear();
    pair.dirty.insert(first);
    pair.inspect_only(&[0; 4]);
    assert_eq!(pair.dirty, HashSet::from([first]));
}

#[tokio::test]
async fn dirty_pressure_counts_real_queues_pending_and_missing_sockets() {
    use crate::client_tun::{DownlinkPressureStats, downlink_pressure_stats};
    let mut pair = Pair::connected().await;
    let first = pair.server_handle(20000);
    let second = pair.server_handle(20001);
    for (handle, bytes) in [(first, 5), (second, 7)] {
        assert_eq!(
            pair.server
                .sockets
                .get_mut::<TcpSocket>(handle)
                .send_slice(&vec![0x5a; bytes])
                .unwrap(),
            bytes
        );
    }
    let idle = *pair.listeners().iter().next().unwrap();
    let udp = pair.server.sockets.add(smoltcp::socket::udp::Socket::new(
        smoltcp::socket::udp::PacketBuffer::new(vec![], vec![]),
        smoltcp::socket::udp::PacketBuffer::new(vec![], vec![]),
    ));
    pair.contexts.insert(udp, SocketCtx::new(10000));
    for (handle, bytes) in [(first, 11), (second, 13), (idle, 17), (udp, 19)] {
        pair.contexts.get_mut(&handle).unwrap().downlink_pending = vec![0x41; bytes];
    }
    for (dirty, expected) in [
        (vec![first, second, idle, udp], (19, 60, 7, 12)),
        (vec![first], (11, 11, 5, 5)),
        (vec![idle], (17, 17, 0, 0)),
        (vec![udp], (19, 19, 0, 0)),
        (vec![], (0, 0, 0, 0)),
    ] {
        let dirty = dirty.into_iter().collect();
        assert_eq!(
            downlink_pressure_stats(&dirty, &pair.contexts, &pair.server.sockets),
            DownlinkPressureStats::new(expected.0, expected.1, expected.2, expected.3)
        );
    }
    pair.contexts.remove(&second);
    let dirty = [first, second, idle, udp].into_iter().collect();
    assert_eq!(
        downlink_pressure_stats(&dirty, &pair.contexts, &pair.server.sockets),
        DownlinkPressureStats::new(19, 47, 5, 5)
    );
    // A removed handle may retain pending context; it never contributes socket TX.
    pair.server.sockets.remove(first);
    assert_eq!(
        downlink_pressure_stats(&dirty, &pair.contexts, &pair.server.sockets),
        DownlinkPressureStats::new(19, 47, 0, 0)
    );
}

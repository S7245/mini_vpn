//! Real staged input proves conservative coverage before any lifecycle work.
use super::*;
mod capacity;
use crate::device::DeviceShutdown;
use crate::harness::{LoopbackTunDevice, PacketLink};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};
use smoltcp::phy::Device;
use smoltcp::wire::{HardwareAddress, IpCidr, Ipv4Address};
use std::task::{Context, Poll};
struct Uncalled;
#[async_trait::async_trait]
impl ProxyUpstream for Uncalled {
    async fn open_tcp(&self, _: &TargetAddr) -> Result<RelayStream, ClientError> {
        panic!("TCP ingress must not open upstream")
    }
}
#[async_trait::async_trait]
impl DatagramUpstream for Uncalled {
    async fn send_udp(&self, _: Vec<u8>) {
        panic!("TCP must not enter UDP")
    }
}
struct Staging(LoopbackTunDevice, bool);
impl Device for Staging {
    type RxToken<'a> = <LoopbackTunDevice as Device>::RxToken<'a>;
    type TxToken<'a> = <LoopbackTunDevice as Device>::TxToken<'a>;
    fn receive(
        &mut self,
        at: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.0.receive(at)
    }
    fn transmit(&mut self, at: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.0.transmit(at)
    }
    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.0.capabilities()
    }
}
impl TunIo for Staging {
    fn request_stop(&mut self) {
        self.0.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.0.poll_shutdown(cx)
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        self.0.wait_for_rx().await
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        self.0.try_recv_rx()
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.0.rx_peek()
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        self.0.rx_take()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        !self.1 && self.0.stage_rx_for_iface()
    }
    fn can_stage_single_iface_rx(&self) -> bool {
        self.0.can_stage_single_iface_rx()
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        self.0.flush_tx().await
    }
    fn inject_ip_packet(&mut self, packet: &[u8]) {
        self.0.inject_ip_packet(packet);
    }
}

#[tokio::test]
async fn actual_older_staged_frame_and_failed_stage_force_full_without_consuming_rx() {
    let up = PacketLink::bounded(16);
    let down = PacketLink::bounded(16);
    let mut client = LoopbackTunDevice::new(down.clone(), up.clone());
    let mut local = Interface::new(
        smoltcp::iface::Config::new(HardwareAddress::Ip),
        &mut client,
        smoltcp::time::Instant::now(),
    );
    local.update_ip_addrs(|ips| {
        ips.push(IpCidr::new(Ipv4Address::new(10, 0, 0, 1).into(), 24))
            .unwrap()
    });
    let mut applications = SocketSet::new(vec![]);
    for port in 20000..20003 {
        let mut s = listener_registry::build_listener_socket(&listener_registry::ListenerSpec {
            local_port: 9999,
        });
        s.abort();
        s.connect(
            local.context(),
            (Ipv4Address::new(10, 0, 0, 2), 10000),
            port,
        )
        .unwrap();
        applications.add(s);
    }
    local.poll(
        smoltcp::time::Instant::now(),
        &mut client,
        &mut applications,
    );
    client.flush_tx().await.unwrap();
    let mut device = Staging(LoopbackTunDevice::new(up.clone(), down.clone()), false);
    let mut iface = Interface::new(
        smoltcp::iface::Config::new(HardwareAddress::Ip),
        &mut device,
        smoltcp::time::Instant::now(),
    );
    iface.update_ip_addrs(|ips| {
        ips.push(IpCidr::new(Ipv4Address::new(10, 0, 0, 2).into(), 24))
            .unwrap()
    });
    let mut registry = ListenerRegistry::new(2);
    let mut sockets = SocketSet::new(vec![]);
    let mut contexts = HashMap::new();
    let mut dirty = HashSet::new();
    let mut assoc = AssocTable::new();
    let mut fake = FakeIpPool::new();
    let metrics = Metrics::default();
    let upstream = Arc::new(LegacyOwnedUpstream::new(Arc::new(Uncalled)));
    for index in 0..3 {
        assert!(device.try_recv_rx().unwrap());
        let original = device.rx_peek().unwrap().to_vec();
        if index == 2 {
            assert!(
                device.can_stage_single_iface_rx(),
                "failure starts with no old staged input"
            );
            assert_eq!(
                registry.prepare_inbound_tcp(
                    Some(&original),
                    &mut sockets,
                    &mut contexts,
                    &mut dirty
                ),
                InboundTcpCoverage::CompleteTcp
            );
        }
        device.1 = index == 2;
        let (kind, coverage) = prepare_ready_tun_rx_packet(
            &mut device,
            &mut assoc,
            &mut fake,
            &upstream,
            0,
            &metrics,
            &mut registry,
            &mut sockets,
            &mut contexts,
            &mut dirty,
            false,
        )
        .await;
        assert_eq!(kind, TunRxPacketKind::Tcp);
        assert_eq!(
            coverage,
            if index == 0 {
                InboundTcpCoverage::CompleteTcp
            } else {
                InboundTcpCoverage::FullFallback
            }
        );
        if index == 0 {
            continue;
        } // Retain real old SYN while preparing another tuple.
        iface.poll_with_egress_targets(
            smoltcp::time::Instant::now(),
            &mut device,
            &mut sockets,
            (coverage == InboundTcpCoverage::CompleteTcp).then_some(dirty.iter().copied()),
        );
        device.flush_tx().await.unwrap();
        assert_eq!(
            registry
                .all_handles()
                .filter(|&h| sockets.get::<TcpSocket>(h).state() == TcpState::SynReceived)
                .count(),
            2
        );
        if index == 2 {
            assert_eq!(device.rx_peek().unwrap(), original);
        }
    }
    device.1 = false;
    let (_, coverage) = prepare_ready_tun_rx_packet(
        &mut device,
        &mut assoc,
        &mut fake,
        &upstream,
        0,
        &metrics,
        &mut registry,
        &mut sockets,
        &mut contexts,
        &mut dirty,
        false,
    )
    .await;
    assert_eq!(coverage, InboundTcpCoverage::CompleteTcp);
    iface.poll_with_egress_targets(
        smoltcp::time::Instant::now(),
        &mut device,
        &mut sockets,
        Some(dirty.iter().copied()),
    );
    device.flush_tx().await.unwrap();
    assert_eq!(
        registry
            .all_handles()
            .filter(|&h| sockets.get::<TcpSocket>(h).state() == TcpState::SynReceived)
            .count(),
        3
    );
    let mut replies = HashSet::new();
    while client.try_recv_rx().unwrap() {
        let bytes = client.rx_take().unwrap();
        let ip = smoltcp::wire::Ipv4Packet::new_checked(&*bytes).unwrap();
        let tcp = smoltcp::wire::TcpPacket::new_checked(ip.payload()).unwrap();
        assert!(tcp.syn() && tcp.ack());
        assert!(replies.insert(tcp.dst_port()));
    }
    assert_eq!(replies, HashSet::from([20000, 20001, 20002]));
    assert_eq!((up.dropped_packets(), down.dropped_packets()), (0, 0));
}

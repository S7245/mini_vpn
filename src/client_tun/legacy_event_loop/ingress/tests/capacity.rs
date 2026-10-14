//! Actual F preparation/poll/common-flush/effect handoff, with no tuple pre-scan.
use super::*;
use smoltcp::wire::{Ipv4Packet, TcpPacket};
use std::time::Duration;

#[tokio::test]
async fn f_capacity_only_preparation_leaves_dirty_to_actual_tcp_effects() {
    let up = PacketLink::bounded(32);
    let down = PacketLink::bounded(32);
    let mut client = LoopbackTunDevice::new(down.clone(), up.clone());
    let mut app_iface = Interface::new(
        smoltcp::iface::Config::new(HardwareAddress::Ip),
        &mut client,
        smoltcp::time::Instant::now(),
    );
    app_iface.update_ip_addrs(|ips| {
        ips.push(IpCidr::new(Ipv4Address::new(10, 0, 0, 1).into(), 24))
            .unwrap();
    });
    let mut applications = SocketSet::new(vec![]);
    let mut app = TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 1024]),
        TcpSocketBuffer::new(vec![0; 1024]),
    );
    app.connect(
        app_iface.context(),
        (Ipv4Address::new(10, 0, 0, 2), 10000),
        20000,
    )
    .unwrap();
    let app = applications.add(app);
    let mut device = Staging(LoopbackTunDevice::new(up.clone(), down.clone()), false);
    let mut iface = Interface::new(
        smoltcp::iface::Config::new(HardwareAddress::Ip),
        &mut device,
        smoltcp::time::Instant::now(),
    );
    iface.update_ip_addrs(|ips| {
        ips.push(IpCidr::new(Ipv4Address::new(10, 0, 0, 2).into(), 24))
            .unwrap();
    });
    let mut registry = ListenerRegistry::new(2);
    let mut sockets = SocketSet::new(vec![]);
    let mut contexts = HashMap::new();
    let mut dirty = HashSet::new();
    let mut assoc = AssocTable::new();
    let mut fake = FakeIpPool::new();
    let metrics = Metrics::default();
    let upstream = Arc::new(LegacyOwnedUpstream::new(Arc::new(Uncalled)));
    let (mut flushes, mut failures) = (0, 0);
    let (mut sent, mut saw_ack, mut saw_data) = (false, false, false);
    let server = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            while client.try_recv_rx().unwrap() {
                assert!(client.stage_rx_for_iface());
            }
            app_iface.poll(
                smoltcp::time::Instant::now(),
                &mut client,
                &mut applications,
            );
            client.flush_tx().await.unwrap();
            if !sent && applications.get::<TcpSocket>(app).state() == TcpState::Established {
                assert_eq!(
                    applications
                        .get_mut::<TcpSocket>(app)
                        .send_slice(b"abc")
                        .unwrap(),
                    3
                );
                sent = true;
            }
            while device.try_recv_rx().unwrap() {
                let ip = Ipv4Packet::new_checked(device.rx_peek().unwrap()).unwrap();
                let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
                saw_ack |= tcp.ack() && !tcp.syn() && tcp.payload().is_empty();
                saw_data |= !tcp.payload().is_empty();
                assert!(dirty.is_empty());
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
                    true,
                )
                .await;
                assert_eq!(coverage, InboundTcpCoverage::CompleteTcp);
                assert!(
                    dirty.is_empty(),
                    "F preparation cannot guess the TCP recipient"
                );
                poll_staged_tun_rx(
                    &mut device,
                    &mut sockets,
                    &mut contexts,
                    &mut iface,
                    &mut NoopSink,
                    &mut flushes,
                    &mut failures,
                    "capacity_tracer",
                    Some(&dirty),
                )
                .await;
                assert!(
                    dirty.is_empty(),
                    "Interface retains effect until explicit handoff"
                );
                super::super::super::ingress_effects::collect(
                    &mut iface, &sockets, &mut dirty, true,
                );
                assert_eq!(dirty.len(), 1);
                let handle = *dirty.iter().next().unwrap();
                let socket = sockets.get::<TcpSocket>(handle);
                assert_eq!(socket.remote_endpoint().unwrap().port, 20000);
                if socket.recv_queue() == 3 {
                    return handle;
                }
                dirty.clear();
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(saw_ack && saw_data);
    let mut bytes = [0; 3];
    assert_eq!(
        sockets
            .get_mut::<TcpSocket>(server)
            .recv_slice(&mut bytes)
            .unwrap(),
        3
    );
    assert_eq!(&bytes, b"abc");
    assert_eq!(failures, 0);
    assert!(flushes >= 3);
    assert_eq!((up.dropped_packets(), down.dropped_packets()), (0, 0));
}

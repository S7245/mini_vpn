//! Real single-packet coverage, TCP window/close work and conservative fallback.
use super::*;

async fn targeted_round(pair: &mut Pair) {
    pair.client.turn().await;
    while pair.server.device.try_recv_rx().unwrap() {
        assert!(pair.server.device.can_stage_single_iface_rx());
        let packet = pair.server.device.rx_peek().unwrap();
        let ip = Ipv4Packet::new_checked(packet).unwrap();
        if TcpPacket::new_checked(ip.payload()).unwrap().fin() {
            pair.last_fin = Some(packet.to_vec());
        }
        pair.last = Some(packet.to_vec());
        let coverage = pair.registry.prepare_inbound_tcp(
            Some(packet),
            &mut pair.server.sockets,
            &mut pair.contexts,
            &mut pair.dirty,
        );
        assert_eq!(coverage, InboundTcpCoverage::CompleteTcp);
        assert!(pair.server.device.stage_rx_for_iface());
        assert!(!pair.server.device.can_stage_single_iface_rx());
        pair.server.iface.poll_with_egress_targets(
            smoltcp::time::Instant::now(),
            &mut pair.server.device,
            &mut pair.server.sockets,
            Some(pair.dirty.iter().copied()),
        );
        assert!(pair.server.device.can_stage_single_iface_rx());
        pair.server.device.flush_tx().await.unwrap();
        pair.dirty.clear();
    }
}
async fn until_targeted(pair: &mut Pair, done: impl Fn(&Pair) -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !done(pair) {
            targeted_round(pair).await;
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn same_port_and_source_port_cross_destination_ip_keep_exact_real_tuple() {
    let mut pair = Pair::new();
    pair.server.iface.update_ip_addrs(|ips| {
        ips.push(IpCidr::new(Ipv4Address::new(10, 0, 0, 3).into(), 24))
            .unwrap();
    });
    pair.client.sockets.remove(pair.clients[1]);
    let mut second = TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 4096]),
        TcpSocketBuffer::new(vec![0; 4096]),
    );
    configure_local_tcp_socket(&mut second);
    second
        .connect(
            pair.client.iface.context(),
            (IpAddress::v4(10, 0, 0, 3), 10000),
            20000,
        )
        .unwrap();
    pair.clients[1] = pair.client.sockets.add(second);
    until_targeted(&mut pair, |p| {
        p.registry
            .all_handles()
            .filter(|&h| p.state(h) == TcpState::Established)
            .count()
            == 2
    })
    .await;
    let server_handles: [SocketHandle; 2] = std::array::from_fn(|i| {
        pair.registry
            .all_handles()
            .find(|&h| {
                let s = pair.server.sockets.get::<TcpSocket>(h);
                s.state() == TcpState::Established
                    && s.local_endpoint().unwrap().addr == IpAddress::v4(10, 0, 0, 2 + i as u8)
            })
            .unwrap()
    });
    for i in 0..2 {
        pair.client
            .sockets
            .get_mut::<TcpSocket>(pair.clients[i])
            .send_slice(&[b'a' + i as u8])
            .unwrap();
        until_targeted(&mut pair, |p| {
            p.server
                .sockets
                .get::<TcpSocket>(server_handles[i])
                .recv_queue()
                == 1
        })
        .await;
        let packet = pair.last.clone().unwrap();
        pair.dirty.clear();
        assert_eq!(
            pair.registry.prepare_inbound_tcp(
                Some(&packet),
                &mut pair.server.sockets,
                &mut pair.contexts,
                &mut pair.dirty
            ),
            InboundTcpCoverage::CompleteTcp
        );
        assert!(pair.dirty.contains(&server_handles[i]));
        assert!(!pair.dirty.contains(&server_handles[1 - i]));
    }
}

#[tokio::test]
async fn actual_window_ack_fin_timewait_and_old_dirty_reset_finish_egress() {
    let mut pair = Pair::connected().await;
    let a = pair.server_handle(20000);
    let b = pair.server_handle(20001);
    pair.dirty.clear();
    assert_eq!(
        pair.server
            .sockets
            .get_mut::<TcpSocket>(a)
            .send_slice(&[7; 4096])
            .unwrap(),
        4096
    );
    pair.server.poll().await;
    until_targeted(&mut pair, |p| {
        p.client.sockets.get::<TcpSocket>(p.clients[0]).recv_queue() == 4096
            && p.server.sockets.get::<TcpSocket>(a).send_queue() == 0
    })
    .await;
    assert_eq!(
        pair.server
            .sockets
            .get_mut::<TcpSocket>(a)
            .send_slice(b"tail")
            .unwrap(),
        4
    );
    pair.server.poll().await;
    assert_eq!(pair.server.sockets.get::<TcpSocket>(a).send_queue(), 4);
    assert_eq!(
        pair.client
            .sockets
            .get_mut::<TcpSocket>(pair.clients[0])
            .recv_slice(&mut [0; 4096])
            .unwrap(),
        4096
    );
    until_targeted(&mut pair, |p| {
        p.client.sockets.get::<TcpSocket>(p.clients[0]).recv_queue() == 4
    })
    .await;
    // Old dirty work belongs to B; incoming A must not erase B's queued RST.
    pair.server.sockets.get_mut::<TcpSocket>(b).abort();
    pair.dirty.insert(b);
    pair.client
        .sockets
        .get_mut::<TcpSocket>(pair.clients[0])
        .send_slice(b"trigger")
        .unwrap();
    until_targeted(&mut pair, |p| {
        p.client.sockets.get::<TcpSocket>(p.clients[1]).state() == TcpState::Closed
    })
    .await;
    assert_eq!(
        pair.server.sockets.get::<TcpSocket>(b).remote_endpoint(),
        None,
        "RST actually dispatched before tuple retirement"
    );
    pair.server.sockets.get_mut::<TcpSocket>(a).close();
    pair.server.poll().await;
    until_targeted(&mut pair, |p| {
        p.client.sockets.get::<TcpSocket>(p.clients[0]).state() == TcpState::CloseWait
    })
    .await;
    pair.client
        .sockets
        .get_mut::<TcpSocket>(pair.clients[0])
        .close();
    until_targeted(&mut pair, |p| p.state(a) == TcpState::TimeWait).await;
    let fin = pair.last_fin.clone().unwrap();
    pair.dirty.clear();
    assert_eq!(
        pair.registry.prepare_inbound_tcp(
            Some(&fin),
            &mut pair.server.sockets,
            &mut pair.contexts,
            &mut pair.dirty
        ),
        InboundTcpCoverage::CompleteTcp
    );
    assert!(pair.dirty.contains(&a));
}

#[tokio::test]
async fn fallback_is_explicit_even_when_old_dirty_is_nonempty() {
    let mut pair = Pair::connected().await;
    let a = pair.server_handle(20000);
    pair.client
        .sockets
        .get_mut::<TcpSocket>(pair.clients[0])
        .send_slice(b"source")
        .unwrap();
    until_targeted(&mut pair, |p| {
        p.server.sockets.get::<TcpSocket>(a).recv_queue() > 0
    })
    .await;
    let mut fragment = pair.last.clone().unwrap();
    fragment[6] |= 0x20;
    let builder = etherparse::PacketBuilder::ipv6([1; 16], [2; 16], 64)
        .tcp(20000, 10000, 1, 4096)
        .ack(1);
    let mut ipv6 = Vec::new();
    builder.write(&mut ipv6, b"x").unwrap();
    let mut unknown = pair.last.clone().unwrap();
    unknown[22..24].copy_from_slice(&12345u16.to_be_bytes());
    let builder = etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64).udp(1234, 1235);
    let mut udp = Vec::new();
    builder.write(&mut udp, b"x").unwrap();
    for packet in [
        None,
        Some(vec![0; 4]),
        Some(fragment),
        Some(ipv6),
        Some(unknown),
        Some(udp),
    ] {
        pair.dirty = HashSet::from([a]);
        assert_eq!(
            pair.registry.prepare_inbound_tcp(
                packet.as_deref(),
                &mut pair.server.sockets,
                &mut pair.contexts,
                &mut pair.dirty
            ),
            InboundTcpCoverage::FullFallback
        );
        assert!(pair.dirty.contains(&a));
    }
}

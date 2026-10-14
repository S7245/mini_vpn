//! Actual TCP SYN dispatch, finite device TX and timer/fallback scheduling.
use super::*;
use crate::iface::SocketHandle;
use crate::socket::tcp;

struct Wire {
    packets: std::vec::Vec<std::vec::Vec<u8>>,
    budget: usize,
}
struct Tx<'a>(&'a mut std::vec::Vec<std::vec::Vec<u8>>);
struct Rx;
impl RxToken for Rx {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, _f: F) -> R {
        unreachable!()
    }
}
impl TxToken for Tx<'_> {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        let mut packet = vec![0; len];
        let result = f(&mut packet);
        self.0.push(packet);
        result
    }
}
impl Device for Wire {
    type RxToken<'a> = Rx;
    type TxToken<'a> = Tx<'a>;
    fn receive(&mut self, _: Instant) -> Option<(Rx, Tx<'_>)> {
        None
    }
    fn transmit(&mut self, _: Instant) -> Option<Tx<'_>> {
        if self.budget == 0 {
            return None;
        }
        self.budget -= 1;
        Some(Tx(&mut self.packets))
    }
    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip;
        caps.max_transmission_unit = 1500;
        caps
    }
}
fn setup_wire() -> (Interface, SocketSet<'static>, Wire, [SocketHandle; 3]) {
    let mut wire = Wire {
        packets: vec![],
        budget: 64,
    };
    let mut iface = Interface::new(Config::new(HardwareAddress::Ip), &mut wire, Instant::ZERO);
    iface.update_ip_addrs(|ips| {
        ips.push(IpCidr::new(IpAddress::v4(127, 0, 0, 1), 8))
            .unwrap()
    });
    let mut sockets = SocketSet::new(vec![]);
    let handles = core::array::from_fn(|i| {
        let mut socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; 1024]),
            tcp::SocketBuffer::new(vec![0; 1024]),
        );
        socket
            .connect(
                iface.context(),
                (IpAddress::v4(127, 0, 0, 2), 443),
                20000 + i as u16,
            )
            .unwrap();
        sockets.add(socket)
    });
    (iface, sockets, wire, handles)
}
fn ports(wire: &Wire) -> std::vec::Vec<u16> {
    wire.packets
        .iter()
        .map(|p| {
            let ip = Ipv4Packet::new_checked(p.as_slice()).unwrap();
            assert!(ip.verify_checksum());
            let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
            assert!(tcp.syn());
            assert!(tcp.verify_checksum(&ip.src_addr().into(), &ip.dst_addr().into()));
            tcp.src_port()
        })
        .collect()
}

#[test]
fn targeted_finite_tx_keeps_unvisited_sockets_for_the_regular_full_poll() {
    let (mut iface, mut sockets, mut wire, handles) = setup_wire();
    wire.budget = 1;
    let order = [handles[1], handles[0]];
    let targeted = iface.supports_egress_targets();
    assert!(iface.poll_with_egress_targets(
        Instant::ZERO,
        &mut wire,
        &mut sockets,
        Some(order.into_iter())
    ));
    assert_eq!(
        ports(&wire),
        if targeted { vec![20001] } else { vec![20000] }
    );
    // Exhausted traversal does not consume another socket's pending SYN.
    wire.budget = 64;
    assert!(iface.poll(Instant::from_millis(5), &mut wire, &mut sockets));
    let mut emitted = ports(&wire);
    emitted.sort();
    assert_eq!(emitted, vec![20000, 20001, 20002]);
    // Repeated targeted polls cannot replace the full timer obligation.
    for ms in (10..2000).step_by(5) {
        wire.budget = 64;
        iface.poll_with_egress_targets(
            Instant::from_millis(ms),
            &mut wire,
            &mut sockets,
            Some([handles[0]].into_iter()),
        );
        iface.poll(Instant::from_millis(ms), &mut wire, &mut sockets);
    }
    let emitted = ports(&wire);
    for port in 20000..20003 {
        assert!(
            emitted.iter().filter(|&&p| p == port).count() >= 2,
            "all original TCP retransmission timers progress"
        );
    }
}

#[test]
fn missing_empty_stale_and_non_tcp_targets_fall_back_before_dispatch() {
    for case in 0..4 {
        let (mut iface, mut sockets, mut wire, handles) = setup_wire();
        let mut targets = vec![handles[1]];
        match case {
            0 => {}
            1 => targets.clear(),
            2 => {
                let removed = sockets.add(tcp::Socket::new(
                    tcp::SocketBuffer::new(vec![0; 16]),
                    tcp::SocketBuffer::new(vec![0; 16]),
                ));
                sockets.remove(removed);
                targets.push(removed);
            }
            3 => {
                use crate::socket::udp;
                let extra = sockets.add(udp::Socket::new(
                    udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 1], vec![0; 16]),
                    udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 1], vec![0; 16]),
                ));
                targets.push(extra);
            }
            _ => unreachable!(),
        }
        iface.poll_with_egress_targets(
            Instant::ZERO,
            &mut wire,
            &mut sockets,
            (case != 0).then_some(targets.into_iter()),
        );
        assert_eq!(
            ports(&wire),
            vec![20000, 20001, 20002],
            "case {case}: full traversal starts at the original first slot"
        );
    }
}

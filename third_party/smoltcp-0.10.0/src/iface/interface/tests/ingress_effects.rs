//! Real checksummed input, finite TX and bounded notification custody.
use super::*;
use crate::iface::SocketHandle;
use crate::socket::tcp;
use std::collections::VecDeque;

struct Wire {
    incoming: VecDeque<std::vec::Vec<u8>>,
    outgoing: std::vec::Vec<std::vec::Vec<u8>>,
    budget: usize,
    panic_rx: bool,
}
struct Rx(std::vec::Vec<u8>, bool);
struct Tx<'a>(&'a mut std::vec::Vec<std::vec::Vec<u8>>);
impl RxToken for Rx {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(mut self, f: F) -> R {
        let result = f(&mut self.0);
        assert!(!self.1, "retained consume interruption");
        result
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
        if self.budget == 0 {
            return None;
        }
        let packet = self.incoming.pop_front()?;
        self.budget -= 1;
        Some((
            Rx(packet, core::mem::replace(&mut self.panic_rx, false)),
            Tx(&mut self.outgoing),
        ))
    }
    fn transmit(&mut self, _: Instant) -> Option<Tx<'_>> {
        if self.budget == 0 {
            return None;
        }
        self.budget -= 1;
        Some(Tx(&mut self.outgoing))
    }
    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ip;
        caps.max_transmission_unit = 1500;
        caps
    }
}
fn syn(port: u16) -> std::vec::Vec<u8> {
    let mut bytes = vec![0; 40];
    let mut ip = Ipv4Packet::new_unchecked(&mut bytes);
    ip.set_version(4);
    ip.set_header_len(20);
    ip.set_total_len(40);
    ip.set_hop_limit(64);
    ip.set_next_header(IpProtocol::Tcp);
    ip.set_src_addr(Ipv4Address::new(127, 0, 0, 2));
    ip.set_dst_addr(Ipv4Address::new(127, 0, 0, 1));
    ip.fill_checksum();
    let src = ip.src_addr().into();
    let dst = ip.dst_addr().into();
    let mut tcp = TcpPacket::new_unchecked(ip.payload_mut());
    tcp.set_src_port(20000);
    tcp.set_dst_port(port);
    tcp.set_header_len(20);
    tcp.set_seq_number(TcpSeqNumber(10));
    tcp.set_syn(true);
    tcp.set_window_len(1024);
    tcp.fill_checksum(&src, &dst);
    bytes
}
fn fixture() -> (Interface, SocketSet<'static>, Wire, [SocketHandle; 2]) {
    let mut wire = Wire {
        incoming: VecDeque::new(),
        outgoing: vec![],
        budget: 8,
        panic_rx: false,
    };
    let mut iface = Interface::new(Config::new(HardwareAddress::Ip), &mut wire, Instant::ZERO);
    iface.update_ip_addrs(|ips| {
        ips.push(IpCidr::new(IpAddress::v4(127, 0, 0, 1), 8))
            .unwrap();
    });
    let mut sockets = SocketSet::new(vec![]);
    let handles = core::array::from_fn(|i| {
        let mut socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; 64]),
            tcp::SocketBuffer::new(vec![0; 64]),
        );
        socket.listen(443 + i as u16).unwrap();
        sockets.add(socket)
    });
    (iface, sockets, wire, handles)
}

#[test]
fn deferred_rx_effect_survives_full_polls_until_taken_once() {
    let (mut iface, mut sockets, mut wire, handles) = fixture();
    wire.incoming.push_back(syn(443));
    wire.budget = 0;
    iface.poll(Instant::ZERO, &mut wire, &mut sockets);
    assert_eq!(wire.incoming.len(), 1);
    assert_eq!(iface.take_ingress_effects(&sockets), IngressEffects::None);
    wire.budget = 8;
    iface.poll(Instant::from_millis(1), &mut wire, &mut sockets);
    assert!(wire.incoming.is_empty());
    assert_eq!(
        sockets.get::<tcp::Socket>(handles[0]).state(),
        tcp::State::SynReceived
    );
    assert_eq!(wire.outgoing.len(), 1);
    // No consumer yet: another poll cannot clear the previous notification.
    iface.poll(Instant::from_millis(2), &mut wire, &mut sockets);
    assert_eq!(
        iface.take_ingress_effects(&sockets),
        if iface.supports_egress_targets() {
            IngressEffects::Tcp(handles[0])
        } else {
            IngressEffects::FullFallback
        }
    );
    assert_eq!(iface.take_ingress_effects(&sockets), IngressEffects::None);
}

#[test]
fn multiple_actual_inputs_force_full_coverage_without_losing_either_socket() {
    let (mut iface, mut sockets, mut wire, handles) = fixture();
    let mut third = tcp::Socket::new(
        tcp::SocketBuffer::new(vec![0; 64]),
        tcp::SocketBuffer::new(vec![0; 64]),
    );
    third
        .connect(iface.context(), (IpAddress::v4(127, 0, 0, 2), 999), 20001)
        .unwrap();
    let third = sockets.add(third);
    wire.incoming.extend([syn(443), syn(444)]);
    iface.poll_with_egress_targets(
        Instant::ZERO,
        &mut wire,
        &mut sockets,
        Some([handles[0]].into_iter()),
    );
    assert_eq!(
        iface.take_ingress_effects(&sockets),
        IngressEffects::FullFallback
    );
    assert_eq!(wire.outgoing.len(), 3);
    assert_eq!(
        sockets.get::<tcp::Socket>(third).state(),
        tcp::State::SynSent
    );
    assert!(wire.outgoing.iter().any(|bytes| {
        let ip = Ipv4Packet::new_checked(bytes.as_slice()).unwrap();
        let tcp = TcpPacket::new_checked(ip.payload()).unwrap();
        tcp.src_port() == 20001 && tcp.syn() && !tcp.ack()
    }));
    for handle in handles {
        assert_eq!(
            sockets.get::<tcp::Socket>(handle).state(),
            tcp::State::SynReceived
        );
    }
    assert_eq!(iface.take_ingress_effects(&sockets), IngressEffects::None);
}

#[test]
fn bad_checksum_never_names_a_tcp_socket_and_keeps_prior_effect() {
    let (mut iface, mut sockets, mut wire, handles) = fixture();
    wire.incoming.push_back(syn(443));
    iface.poll(Instant::ZERO, &mut wire, &mut sockets);
    let mut bad = syn(444);
    bad[36] ^= 1;
    wire.incoming.push_back(bad);
    iface.poll(Instant::from_millis(1), &mut wire, &mut sockets);
    assert_eq!(
        sockets.get::<tcp::Socket>(handles[1]).state(),
        tcp::State::Listen
    );
    assert_eq!(wire.outgoing.len(), 1);
    assert_eq!(
        iface.take_ingress_effects(&sockets),
        IngressEffects::FullFallback
    );
}

fn ack(server_seq: TcpSeqNumber, window: u16) -> std::vec::Vec<u8> {
    let mut bytes = syn(443);
    let mut ip = Ipv4Packet::new_unchecked(&mut bytes);
    let src = ip.src_addr().into();
    let dst = ip.dst_addr().into();
    let mut tcp = TcpPacket::new_unchecked(ip.payload_mut());
    tcp.set_syn(false);
    tcp.set_ack(true);
    tcp.set_seq_number(TcpSeqNumber(11));
    tcp.set_ack_number(server_seq + 1);
    tcp.set_window_len(window);
    tcp.fill_checksum(&src, &dst);
    bytes
}

fn window_update(empty_targets: bool) {
    let (mut iface, mut sockets, mut wire, handles) = fixture();
    wire.incoming.push_back(syn(443));
    iface.poll(Instant::ZERO, &mut wire, &mut sockets);
    let ip = Ipv4Packet::new_checked(wire.outgoing[0].as_slice()).unwrap();
    let server_seq = TcpPacket::new_checked(ip.payload()).unwrap().seq_number();
    iface.take_ingress_effects(&sockets);
    wire.incoming.push_back(ack(server_seq, 0));
    iface.poll(Instant::from_millis(1), &mut wire, &mut sockets);
    iface.take_ingress_effects(&sockets);
    assert_eq!(
        sockets.get::<tcp::Socket>(handles[0]).state(),
        tcp::State::Established
    );
    sockets
        .get_mut::<tcp::Socket>(handles[0])
        .send_slice(b"abc")
        .unwrap();
    wire.outgoing.clear();
    iface.poll(Instant::from_millis(2), &mut wire, &mut sockets);
    assert!(
        wire.outgoing.is_empty(),
        "real peer zero window retains data"
    );
    let mut unrelated = tcp::Socket::new(
        tcp::SocketBuffer::new(vec![0; 64]),
        tcp::SocketBuffer::new(vec![0; 64]),
    );
    unrelated
        .connect(iface.context(), (IpAddress::v4(127, 0, 0, 2), 999), 20001)
        .unwrap();
    sockets.add(unrelated);
    wire.incoming.push_back(ack(server_seq, 1024));
    iface.poll_with_egress_targets(
        Instant::from_millis(3),
        &mut wire,
        &mut sockets,
        Some((!empty_targets).then_some(handles[1]).into_iter()),
    );
    assert!(
        wire.outgoing.iter().any(|bytes| {
            let ip = Ipv4Packet::new_checked(bytes.as_slice()).unwrap();
            TcpPacket::new_checked(ip.payload()).unwrap().payload() == b"abc"
        }),
        "actual accepted ACK must dispatch queued DATA outside old targets"
    );
    if iface.supports_egress_targets() {
        assert!(
            !wire.outgoing.iter().any(|bytes| {
                let ip = Ipv4Packet::new_checked(bytes.as_slice()).unwrap();
                TcpPacket::new_checked(ip.payload()).unwrap().src_port() == 20001
            }),
            "actual TCP effect must not scan unrelated egress, even with empty old targets"
        );
    }
}

#[test]
fn actual_window_update_adds_data_egress_outside_the_old_targets() {
    window_update(false);
}

#[test]
fn actual_window_update_with_empty_old_targets_avoids_unrelated_egress() {
    window_update(true);
}

#[test]
fn removed_saved_handle_becomes_full_fallback() {
    let (mut iface, mut sockets, mut wire, handles) = fixture();
    wire.incoming.push_back(syn(443));
    iface.poll(Instant::ZERO, &mut wire, &mut sockets);
    sockets.remove(handles[0]);
    assert_eq!(
        iface.take_ingress_effects(&sockets),
        IngressEffects::FullFallback
    );
}

#[test]
fn interrupted_consumption_remains_visible_across_a_later_poll() {
    for poll_again in [false, true] {
        let (mut iface, mut sockets, mut wire, handles) = fixture();
        wire.incoming.push_back(syn(443));
        wire.panic_rx = true;
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            iface.poll(Instant::ZERO, &mut wire, &mut sockets)
        }))
        .is_err());
        assert_eq!(
            sockets.get::<tcp::Socket>(handles[0]).state(),
            tcp::State::SynReceived
        );
        if poll_again {
            wire.incoming.push_back(syn(444));
            iface.poll(Instant::from_millis(1), &mut wire, &mut sockets);
        }
        assert_eq!(
            iface.take_ingress_effects(&sockets),
            IngressEffects::FullFallback
        );
        assert_eq!(iface.take_ingress_effects(&sockets), IngressEffects::None);
    }
}

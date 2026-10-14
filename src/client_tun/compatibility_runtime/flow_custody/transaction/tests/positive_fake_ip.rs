//! Real DNS to DomainPort TCP forwarding with overlapping checked fake-IP leases.
//! One saved root owns every provider/role; the app and peer own actual sockets.
//! This positive tracer observes minted release receipts, never injects an ACK.
use super::super::*;
use crate::client_tun::compatibility_runtime::{
    ClientTunSession, SessionDeviceState, prepare_client_tun_session,
};
use crate::client_tun::{NoopSink, TunRuntimeConfig};
use crate::device::TunIo;
use crate::fake_ip::FakeIpLeaseReceipt;
use crate::harness::{LoopbackTunDevice, PacketLink};
use crate::metrics::Metrics;
use crate::shared::TargetAddr;
use crate::tuic::tcp_fixture::{UdpPair, upstream_endpoint, upstream_ready_connection};
use crate::tuic::{TuicClientConfig, TuicUpstream, encode_authenticate, encode_connect};
use smoltcp::iface::{Config, Interface, SocketSet};
use smoltcp::socket::{tcp, udp};
use smoltcp::wire::{HardwareAddress, IpAddress, IpCidr, Ipv4Packet, TcpPacket};
use std::net::Ipv4Addr;
use std::time::Duration;

const DOMAIN: &str = "positive.test";
const QUESTION: &[u8] = b"\x08positive\x04test\x00\x00\x01\x00\x01";
type Halves = (quinn::SendStream, quinn::RecvStream);
type Acquisition = (FlowIdentity, FakeIpLeaseReceipt);

async fn capture<F: Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = std::pin::pin!(work);
    std::future::poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}

struct AppFlow {
    handle: SocketHandle,
    tag: u8,
    sent: bool,
    received: Vec<u8>,
    fin: bool,
}

struct Apps {
    device: LoopbackTunDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    dns: SocketHandle,
    flows: Vec<AppFlow>,
    resets: usize,
}

impl Apps {
    fn new(inbound: PacketLink, outbound: PacketLink) -> Self {
        let mut device = LoopbackTunDevice::new(inbound, outbound);
        let mut iface = Interface::new(
            Config::new(HardwareAddress::Ip),
            &mut device,
            smoltcp::time::Instant::now(),
        );
        let local = smoltcp::wire::Ipv4Address::new(10, 0, 0, 1);
        iface.update_ip_addrs(|ips| ips.push(IpCidr::new(local.into(), 24)).unwrap());
        iface.routes_mut().add_default_ipv4_route(local).unwrap();
        let mut sockets = SocketSet::new(vec![]);
        let buffer = || udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0; 2048]);
        let mut dns = udp::Socket::new(buffer(), buffer());
        dns.bind(20053).unwrap();
        let dns = sockets.add(dns);
        Self {
            device,
            iface,
            sockets,
            dns,
            flows: Vec::new(),
            resets: 0,
        }
    }

    fn connect(&mut self, ip: Ipv4Addr, tag: u8) {
        assert!(self.flows.len() < 4);
        let mut socket = TcpSocket::new(
            tcp::SocketBuffer::new(vec![0; 4096]),
            tcp::SocketBuffer::new(vec![0; 4096]),
        );
        socket
            .connect(
                self.iface.context(),
                (IpAddress::from(ip), 10000),
                40000 + u16::from(tag),
            )
            .unwrap();
        let handle = self.sockets.add(socket);
        self.flows.push(AppFlow {
            handle,
            tag,
            sent: false,
            received: Vec::new(),
            fin: false,
        });
    }

    async fn turn(&mut self) {
        while self.device.try_recv_rx().unwrap() {
            let ip = Ipv4Packet::new_checked(self.device.rx_peek().unwrap()).unwrap();
            if ip.next_header() == smoltcp::wire::IpProtocol::Tcp {
                self.resets += usize::from(TcpPacket::new_checked(ip.payload()).unwrap().rst());
            }
            assert!(self.device.stage_rx_for_iface());
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        for flow in &mut self.flows {
            let socket = self.sockets.get_mut::<TcpSocket>(flow.handle);
            if !flow.sent && socket.can_send() {
                assert_eq!(socket.send_slice(&[flow.tag; 8]).unwrap(), 8);
                flow.sent = true;
            }
            if socket.can_recv() {
                socket
                    .recv(|bytes| {
                        assert!(flow.received.len() + bytes.len() <= 16);
                        flow.received.extend_from_slice(bytes);
                        (bytes.len(), ())
                    })
                    .unwrap();
            }
            if socket.state() == tcp::State::CloseWait {
                flow.fin = true;
                socket.close();
            }
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        self.device.flush_tx().await.unwrap();
    }

    fn query(&mut self, id: u16) {
        let mut query = Vec::from(id.to_be_bytes());
        query.extend_from_slice(b"\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00");
        query.extend_from_slice(QUESTION);
        self.sockets
            .get_mut::<udp::Socket>(self.dns)
            .send_slice(&query, (IpAddress::v4(8, 8, 8, 8), 53))
            .unwrap();
    }

    fn dns_answer(&mut self, id: u16) -> Ipv4Addr {
        let (bytes, meta) = self
            .sockets
            .get_mut::<udp::Socket>(self.dns)
            .recv()
            .unwrap();
        assert_eq!(
            (meta.endpoint.addr, meta.endpoint.port),
            (IpAddress::v4(8, 8, 8, 8), 53)
        );
        assert_eq!(&bytes[..2], &id.to_be_bytes());
        assert_eq!(&bytes[2..12], b"\x81\x00\x00\x01\x00\x01\x00\x00\x00\x00");
        assert_eq!(&bytes[12..12 + QUESTION.len()], QUESTION);
        let answer = &bytes[12 + QUESTION.len()..];
        assert_eq!(answer.len(), 16);
        assert_eq!(&answer[..6], b"\xc0\x0c\x00\x01\x00\x01");
        assert_eq!(&answer[10..12], b"\x00\x04");
        let ip = Ipv4Addr::new(answer[12], answer[13], answer[14], answer[15]);
        assert!(matches!(ip.octets(), [198, 18..=19, _, _]));
        ip
    }
}

async fn turn<F: Future<Output = ()>>(session: &mut ClientTunSession<F>, apps: &mut Apps) {
    apps.turn().await;
    tokio::select! {
        state = session.run() => panic!("saved entry ended early: {state:?}"),
        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
    }
}

async fn until<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut Apps,
    mut ready: impl FnMut(&Apps, &SessionControl) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            turn(session, apps).await;
            if ready(apps, &session.control) {
                break;
            }
        }
    })
    .await
    .expect("actual app/root transition");
}

async fn remote<F: Future<Output = ()>, G: Future>(
    session: &mut ClientTunSession<F>,
    apps: &mut Apps,
    work: G,
) -> G::Output {
    let mut work = std::pin::pin!(work);
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            apps.turn().await;
            tokio::select! {
                value = &mut work => return value,
                state = session.run() => panic!("saved entry ended early: {state:?}"),
                _ = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
    })
    .await
    .expect("actual TUIC peer transition")
}

fn acquisition(owner: &SessionControl, existing: &[Acquisition]) -> Acquisition {
    let opens = owner.opens();
    let mut candidates = opens.slots.iter().flatten().filter_map(|slot| {
        let OpenPhase::Flow(flow) = &slot.phase else {
            return None;
        };
        if existing.iter().any(|known| known.0 == flow.id) {
            return None;
        }
        assert_eq!(flow.stage, FlowStage::Published);
        assert!(flow.fake_ip_failure.is_none());
        assert_eq!(
            flow.target,
            TargetAddr::DomainPort {
                host: DOMAIN.to_owned(),
                port: 10000
            }
        );
        Some((flow.id, flow.fake_ip.expect("real DNS mapping acquired")))
    });
    let found = candidates.next().unwrap();
    assert!(candidates.next().is_none());
    found
}

fn same_live(owner: &SessionControl, expected: Acquisition) {
    let opens = owner.opens();
    let slot = opens.slots[expected.0.index].as_ref().unwrap();
    let OpenPhase::Flow(flow) = &slot.phase else {
        panic!("original flow must remain");
    };
    assert_eq!((flow.id, flow.fake_ip), (expected.0, Some(expected.1)));
    assert_eq!(flow.stage, FlowStage::Published);
    assert!(!owner.ledger().quarantined);
}

fn release(
    owner: &SessionControl,
    acquired: Acquisition,
    before: u32,
    released: u64,
    active: usize,
) {
    let mut expected = acquired.1;
    expected.before = before;
    expected.after = before - 1;
    let seen = owner.flow_observation().unwrap();
    assert_eq!(
        seen.last_fake_release,
        Some((acquired.0, expected, (1, active)))
    );
    assert_eq!((seen.released, seen.joined), (released, released * 3));
    assert!(seen.audit && !owner.ledger().quarantined);
    let opens = owner.opens();
    assert!(
        opens
            .slots
            .iter()
            .flatten()
            .all(|slot| !matches!(&slot.phase, OpenPhase::Flow(flow) if flow.id == acquired.0))
    );
}

async fn open<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut Apps,
    peer: &quinn::Connection,
    ip: Ipv4Addr,
    tag: u8,
) -> Halves {
    apps.connect(ip, tag);
    let halves = remote(session, apps, async {
        let (mut send, mut recv) = peer.accept_bi().await.unwrap();
        assert_eq!(recv.id().index(), u64::from(tag));
        let mut expected = encode_connect(&TargetAddr::DomainPort {
            host: DOMAIN.to_owned(),
            port: 10000,
        });
        expected.extend_from_slice(&[tag; 8]);
        let mut got = vec![0; expected.len()];
        recv.read_exact(&mut got).await.unwrap();
        assert_eq!(
            got, expected,
            "DomainPort CONNECT and initial bytes exactly once"
        );
        send.write_all(&[tag + 100; 8]).await.unwrap();
        (send, recv)
    })
    .await;
    until(session, apps, |apps, owner| {
        apps.flows[usize::from(tag)].received == [tag + 100; 8]
            && owner.flow_observation().unwrap().published == u64::from(tag) + 1
    })
    .await;
    halves
}

async fn fin<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut Apps,
    halves: Halves,
    tag: usize,
    released: u64,
) {
    let (mut send, mut recv) = halves;
    send.finish().unwrap();
    let eof = remote(session, apps, recv.read_to_end(1)).await.unwrap();
    assert!(eof.is_empty(), "no duplicate uplink after initial bytes");
    drop((send, recv));
    until(session, apps, |apps, owner| {
        owner.flow_observation().unwrap().released == released
            && apps
                .sockets
                .get::<TcpSocket>(apps.flows[tag].handle)
                .state()
                == tcp::State::Closed
    })
    .await;
    assert!(apps.flows[tag].fin);
    assert_eq!(apps.resets, 0, "normal FIN cannot gain a late RST");
}

#[tokio::test]
async fn saved_dns_domain_connect_releases_exact_shared_fake_ip_counts() {
    let pair = UdpPair::new(8, 1_048_576).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 1_048_576, 8, true)));
    let mut config = TuicClientConfig::from_sources(
        Some(&pair.server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap();
    config.tcp_pool = 1;
    let setup = capture(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                TuicUpstream::connect(&config, Arc::new(Metrics::default())),
                async {
                    let peer = pair.server.accept().await.unwrap().await.unwrap();
                    let mut auth = peer.accept_uni().await.unwrap();
                    let bytes = auth.read_to_end(64).await.unwrap();
                    let mut token = [0; 32];
                    peer.export_keying_material(
                        &mut token,
                        &config.uuid,
                        config.password.as_bytes(),
                    )
                    .unwrap();
                    assert_eq!(bytes, encode_authenticate(&config.uuid, &token));
                    (peer, auth)
                }
            )
        })
        .await
        .unwrap()
    })
    .await;
    let (upstream, peer, auth) = match setup {
        Ok((Ok(upstream), (peer, auth))) => (Arc::new(upstream), peer, auth),
        failure => {
            pair.close().await;
            match failure {
                Err(panic) => std::panic::resume_unwind(panic),
                _ => panic!("actual TUIC setup failed"),
            }
        }
    };
    let endpoint = upstream_endpoint(&upstream);
    let connection_id = upstream_ready_connection(&upstream).unwrap().stable_id();
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let mut session = prepare_client_tun_session(
        LoopbackTunDevice::new(inbound.clone(), outbound.clone()),
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let mut held: [Option<Halves>; 4] = [None, None, None, None];
    let scenario = capture(async {
        apps.query(31);
        until(&mut session, &mut apps, |apps, _| {
            apps.sockets.get::<udp::Socket>(apps.dns).can_recv()
        })
        .await;
        let ip = apps.dns_answer(31);
        assert_eq!(
            session.control.flow_observation().unwrap().factory_started,
            0
        );

        held[0] = Some(open(&mut session, &mut apps, &peer, ip, 0).await);
        let a = acquisition(&session.control, &[]);
        assert_eq!((a.1.ip, a.1.before, a.1.after), (ip, 0, 1));
        held[1] = Some(open(&mut session, &mut apps, &peer, ip, 1).await);
        let b = acquisition(&session.control, &[a]);
        assert_eq!(
            (b.1.ip, b.1.incarnation, b.1.before, b.1.after),
            (ip, a.1.incarnation, 1, 2)
        );
        assert!(b.1.acquisition > a.1.acquisition);
        same_live(&session.control, a);
        same_live(&session.control, b);

        fin(&mut session, &mut apps, held[0].take().unwrap(), 0, 1).await;
        release(&session.control, a, 2, 1, 1);
        same_live(&session.control, b);
        let handle = apps.flows[1].handle;
        assert_eq!(
            apps.sockets
                .get_mut::<TcpSocket>(handle)
                .send_slice(b"more")
                .unwrap(),
            4
        );
        let (send, recv) = held[1].as_mut().unwrap();
        remote(&mut session, &mut apps, async {
            let mut bytes = [0; 4];
            recv.read_exact(&mut bytes).await.unwrap();
            assert_eq!(&bytes, b"more");
            send.write_all(b"live").await.unwrap();
        })
        .await;
        until(&mut session, &mut apps, |apps, _| {
            apps.flows[1].received.len() == 12
        })
        .await;
        assert_eq!(&apps.flows[1].received[8..], b"live");
        assert_eq!(
            apps.sockets.get::<TcpSocket>(handle).state(),
            tcp::State::Established
        );
        same_live(&session.control, b);
        apps.query(32);
        until(&mut session, &mut apps, |apps, _| {
            apps.sockets.get::<udp::Socket>(apps.dns).can_recv()
        })
        .await;
        assert_eq!(apps.dns_answer(32), ip);

        held[2] = Some(open(&mut session, &mut apps, &peer, ip, 2).await);
        let c = acquisition(&session.control, &[b]);
        assert_eq!(
            (c.1.ip, c.1.incarnation, c.1.before, c.1.after),
            (ip, a.1.incarnation, 1, 2)
        );
        assert!(c.1.acquisition > b.1.acquisition);
        fin(&mut session, &mut apps, held[1].take().unwrap(), 1, 2).await;
        release(&session.control, b, 2, 2, 1);
        same_live(&session.control, c);
        fin(&mut session, &mut apps, held[2].take().unwrap(), 2, 3).await;
        release(&session.control, c, 1, 3, 0);

        held[3] = Some(open(&mut session, &mut apps, &peer, ip, 3).await);
        let d = acquisition(&session.control, &[]);
        assert_eq!(
            (d.1.ip, d.1.incarnation, d.1.before, d.1.after),
            (ip, a.1.incarnation, 0, 1)
        );
        assert!(d.1.acquisition > c.1.acquisition);
        assert_eq!(
            apps.resets, 0,
            "all completed FINs stay reset-free before stop"
        );
        session.request_stop();
        let (send, mut recv) = held[3].take().unwrap();
        let retirement = async {
            let mut byte = [0];
            let (eof, stopped) = tokio::join!(recv.read(&mut byte), send.stopped());
            assert!(matches!(eof, Ok(None) | Err(quinn::ReadError::Reset(_))));
            assert!(stopped.unwrap().is_some());
            drop((send, recv));
        };
        let mut retirement = std::pin::pin!(retirement);
        let mut retired = false;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                apps.turn().await;
                let state = session.state();
                tokio::select! {
                    value = session.run(), if state != SessionDeviceState::DeviceSettled => {
                        assert_eq!(value, SessionDeviceState::DeviceSettled);
                    },
                    () = &mut retirement, if !retired => retired = true,
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                }
                if retired && session.state() == SessionDeviceState::DeviceSettled {
                    break;
                }
            }
        })
        .await
        .expect("original root and peer stop");
        apps.turn().await;
        release(&session.control, d, 1, 4, 0);
        assert_eq!(
            apps.sockets.get::<TcpSocket>(apps.flows[3].handle).state(),
            tcp::State::Closed
        );
        let done = session.control.flow_observation().unwrap();
        assert_eq!(
            (
                done.factory_started,
                done.published,
                done.released,
                done.joined
            ),
            (4, 4, 4, 12)
        );
        assert_eq!(
            (
                done.live,
                done.w,
                done.f,
                done.reserved,
                done.escrow_bytes,
                done.running
            ),
            (0, 0, 0, 0, 0, 0)
        );
        assert!(session.control.ledger().first_failure.is_none());
        assert_eq!(inbound.dropped_packets() + outbound.dropped_packets(), 0);
        for _ in 0..2 {
            assert_eq!(
                session.observe_shutdown().await,
                SessionDeviceState::DeviceSettled
            );
            release(&session.control, d, 1, 4, 0);
        }
        let current = upstream_ready_connection(&upstream).unwrap();
        assert_eq!(current.stable_id(), connection_id);
        let (next, _, handshaking, closed, failed) = current.bi_stream_open_snapshot();
        assert_eq!(
            (next, handshaking, closed, failed),
            (4, false, false, false)
        );
        assert!(peer.close_reason().is_none());
    })
    .await;
    drop(held);
    session.request_stop();
    let state = tokio::time::timeout(Duration::from_secs(15), session.observe_shutdown()).await;
    endpoint.close(0u32.into(), b"fake-IP fixture cleanup");
    peer.close(0u32.into(), b"fake-IP fixture cleanup");
    drop(session);
    drop(upstream);
    drop(auth);
    drop(peer);
    let idle = tokio::time::timeout(Duration::from_secs(2), endpoint.wait_idle()).await;
    let pair_cleanup = capture(pair.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(state.unwrap(), SessionDeviceState::DeviceSettled);
    idle.unwrap();
    if let Err(panic) = pair_cleanup {
        std::panic::resume_unwind(panic);
    }
}

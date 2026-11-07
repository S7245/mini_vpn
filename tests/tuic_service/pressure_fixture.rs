//! Real bounded application/QUIC fixture shared by private inventory and public
//! contract tests. Owns no service authority or success proof. One healthy TCP,
//! at most129 waiting TCP and one UDP; each TCP512/512, reply64, packet64/pump4.
use super::*;
#[path = "observe_window.rs"]
mod observe_window;
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::{tcp, udp};
use smoltcp::wire::{HardwareAddress, IpCidr, Ipv4Address};
use std::future::Future;
use std::task::Poll;

pub(super) const PAYLOAD: &[u8] = b"payload";

pub(super) fn server() -> quinn::Endpoint {
    let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(base.join("server-cert.pem")).unwrap(),
    ))
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        std::fs::File::open(base.join("server-key.pem")).unwrap(),
    ))
    .unwrap()
    .unwrap();
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams(1u32.into());
    config.transport_config(Arc::new(transport));
    quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap()
}

pub(super) fn config(server: &quinn::Endpoint) -> TuicClientConfig {
    let mut config = TuicClientConfig::from_sources(
        Some(&server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap();
    config.tcp_pool = 1;
    config
}

pub(super) async fn auth(peer: &quinn::Connection, cfg: &TuicClientConfig) {
    let mut auth = peer.accept_uni().await.unwrap();
    let actual = auth.read_to_end(64).await.unwrap();
    let mut token = [0; 32];
    peer.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes())
        .unwrap();
    assert_eq!(actual, encode_authenticate(&cfg.uuid, &token));
}

pub(super) async fn capture<F: Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = Box::pin(work);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(error) => Poll::Ready(Err(error)),
        }
    })
    .await
}

pub(super) async fn drive<S: TunService, F: Future>(service: &mut S, work: F) -> F::Output {
    observe_window::window(Duration::from_secs(5), async {
        tokio::select! {
            value = work => value,
            report = service.run() => panic!("pressure service ended early {report:?}"),
        }
    })
    .await
    .expect("pressure observation expired")
}

pub(super) struct App {
    device: LoopbackTunDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    tcp: Vec<(SocketHandle, bool)>,
    udp: SocketHandle,
    udp_sent: bool,
    pub(super) tcp_reply: Vec<u8>,
    pub(super) udp_reply: Vec<u8>,
}

impl App {
    pub(super) fn new(mut device: LoopbackTunDevice) -> Self {
        let mut iface = Interface::new(
            Config::new(HardwareAddress::Ip),
            &mut device,
            smoltcp::time::Instant::now(),
        );
        let local = Ipv4Address::new(10, 0, 0, 1);
        iface.update_ip_addrs(|a| a.push(IpCidr::new(local.into(), 24)).unwrap());
        iface.routes_mut().add_default_ipv4_route(local).unwrap();
        let mut sockets = SocketSet::new(vec![]);
        let mut udp = udp::Socket::new(
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0; 4096]),
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0; 4096]),
        );
        udp.bind(30000).unwrap();
        let udp = sockets.add(udp);
        let mut app = Self {
            device,
            iface,
            sockets,
            tcp: Vec::with_capacity(130),
            udp,
            udp_sent: false,
            tcp_reply: Vec::with_capacity(64),
            udp_reply: Vec::with_capacity(64),
        };
        app.add();
        app
    }

    pub(super) fn add(&mut self) {
        assert!(self.tcp.len() < 130);
        let mut socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; 512]),
            tcp::SocketBuffer::new(vec![0; 512]),
        );
        socket
            .connect(
                self.iface.context(),
                (Ipv4Address::new(192, 0, 2, 10), 10000),
                20000 + self.tcp.len() as u16,
            )
            .unwrap();
        self.tcp.push((self.sockets.add(socket), false));
    }

    pub(super) fn last_acked(&self) -> bool {
        let (handle, sent) = self.tcp.last().unwrap();
        let socket = self.sockets.get::<tcp::Socket>(*handle);
        *sent && socket.state() == tcp::State::Established && socket.send_queue() == 0
    }

    pub(super) fn ping(&mut self) {
        self.sockets
            .get_mut::<tcp::Socket>(self.tcp[0].0)
            .send_slice(b"again")
            .unwrap();
        self.tcp_reply.clear();
        self.udp_reply.clear();
        self.udp_sent = false;
    }

    pub(super) async fn turn(&mut self) {
        for _ in 0..64 {
            if !self.device.try_recv_rx().unwrap() {
                break;
            }
            assert!(self.device.stage_rx_for_iface());
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        for (index, (handle, sent)) in self.tcp.iter_mut().enumerate() {
            let socket = self.sockets.get_mut::<tcp::Socket>(*handle);
            if socket.can_send() && !*sent {
                assert_eq!(socket.send_slice(PAYLOAD).unwrap(), PAYLOAD.len());
                *sent = true;
            }
            if index == 0 {
                while socket.can_recv() {
                    let mut buffer = [0; 64];
                    let n = socket.recv_slice(&mut buffer).unwrap();
                    assert!(self.tcp_reply.len() + n <= 64);
                    self.tcp_reply.extend_from_slice(&buffer[..n]);
                }
            }
        }
        let udp = self.sockets.get_mut::<udp::Socket>(self.udp);
        if !self.udp_sent {
            udp.send_slice(b"udp", (Ipv4Address::new(192, 0, 2, 10), 10001))
                .unwrap();
            self.udp_sent = true;
        }
        while udp.can_recv() {
            let (bytes, _) = udp.recv().unwrap();
            assert!(self.udp_reply.len() + bytes.len() <= 64);
            self.udp_reply.extend_from_slice(bytes);
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        self.device.flush_tx().await.unwrap();
    }
}

pub(super) async fn udp_reply(peer: &quinn::Connection) {
    let packet = peer.read_datagram().await.unwrap();
    assert!(packet.len() >= 4);
    let id = u16::from_be_bytes([packet[2], packet[3]]);
    let target = TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());
    assert_eq!(&packet[..], encode_packet(id, &target, b"udp"));
    peer.send_datagram(encode_packet(id, &target, b"udp-reply").into())
        .unwrap();
}

pub(super) async fn healthy(peer: &quinn::Connection) -> (quinn::SendStream, quinn::RecvStream) {
    let (mut tx, mut rx) = peer.accept_bi().await.unwrap();
    let mut expected = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
    expected.extend_from_slice(PAYLOAD);
    let mut actual = vec![0; expected.len()];
    rx.read_exact(&mut actual).await.unwrap();
    assert_eq!(actual, expected);
    tx.write_all(b"first").await.unwrap();
    (tx, rx)
}

pub(super) async fn ping(stream: &mut (quinn::SendStream, quinn::RecvStream)) {
    let mut actual = [0; 5];
    stream.1.read_exact(&mut actual).await.unwrap();
    assert_eq!(&actual, b"again");
    stream.0.write_all(b"second").await.unwrap();
}

pub(super) async fn retire(
    stream: &mut Option<(quinn::SendStream, quinn::RecvStream)>,
    release: &std::sync::atomic::AtomicBool,
) {
    if let Some((tx, rx)) = stream {
        let code = tx.stopped().await.unwrap().expect("original STOP_SENDING");
        assert!(rx.read_to_end(0).await.unwrap().is_empty());
        // Hold the actual final offset, never a synthetic service receipt.
        while !release.load(std::sync::atomic::Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tx.reset(code).unwrap();
    }
    drop(stream.take());
}

pub(super) async fn drain(mut server: quinn::Endpoint, peer: Option<quinn::Connection>) {
    if let Some(peer) = peer {
        peer.close(0u32.into(), b"test done");
    }
    server.close(0u32.into(), b"test done");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), drain.drained())
            .await
            .unwrap()
    );
}

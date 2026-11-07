//! Bounded real application/peer fixture for the saved-service full F/W matrix.
//! Owns 4225 app TCP sockets, one UDP and at most4096 actual peer half-pairs.
//! No service authority, clocks, receipts, joins or ledger values are fabricated.
//! The old manual-session full fixtures remain unchanged characterizers.
use super::*;
#[path = "observe_window.rs"]
mod observe_window;
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::{tcp, udp};
use smoltcp::wire::{HardwareAddress, IpCidr, Ipv4Address};
use std::future::Future;
use std::task::Poll;

pub(super) const F: usize = 4096;
pub(super) const W: usize = 128;
pub(super) const TOTAL: usize = F + W;
const C_BYTES: usize = 65535 + 4096;

pub(super) async fn capture<T: Future>(work: T) -> std::thread::Result<T::Output> {
    let mut work = Box::pin(work);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    })
    .await
}

pub(super) async fn drive<S: TunService, T: Future>(
    service: &mut S,
    limit: Duration,
    work: T,
) -> T::Output {
    observe_window::window(limit, async {
        tokio::select! {
            value = work => value,
            report = service.run() => panic!("full service ended early {report:?}"),
        }
    })
    .await
    .expect("original full-matrix observation expired")
}

/// Real server endpoint plus its test identity (K2, D91). The private key stays
/// in memory; only the public CA is written, into a per-server temp dir that is
/// removed on Drop, for the unchanged production `ca_path` loader. Mirrors
/// `tuic_service/tests/fixture/native.rs::PublicCa`; certs/dev is not read.
pub(super) struct Server {
    endpoint: quinn::Endpoint,
    // One heap cell keeps the stack-pinned test future at Endpoint + 1 pointer.
    tls: Box<(rcgen::CertifiedKey<rcgen::KeyPair>, CaDir)>,
}
struct CaDir(std::path::PathBuf);
impl Drop for CaDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.0.join("ca.pem"));
        let _ = std::fs::remove_dir(&self.0);
    }
}
impl std::ops::Deref for Server {
    type Target = quinn::Endpoint;
    fn deref(&self) -> &quinn::Endpoint {
        &self.endpoint
    }
}
impl std::borrow::Borrow<quinn::Endpoint> for Server {
    fn borrow(&self) -> &quinn::Endpoint {
        &self.endpoint
    }
}
impl std::borrow::BorrowMut<quinn::Endpoint> for Server {
    fn borrow_mut(&mut self) -> &mut quinn::Endpoint {
        &mut self.endpoint
    }
}
fn tls(identity: &rcgen::CertifiedKey<rcgen::KeyPair>, alpn: &[u8]) -> rustls::ServerConfig {
    let cert = rustls::pki_types::CertificateDer::from(identity.cert.der().to_vec());
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
        identity.signing_key.serialize_der(),
    ));
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .unwrap();
    tls.alpn_protocols = vec![alpn.to_vec()];
    tls
}
impl Server {
    /// Same identity, foreign ALPN, default transport: the real handshake refuses
    /// (replaces the certs/dev based `rebuild_rejection::rejected_alpn` here).
    pub(super) fn rejected_alpn(&self) -> quinn::ServerConfig {
        quinn::ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(tls(
                &self.tls.0,
                b"n1-controlled-refusal",
            ))
            .unwrap(),
        ))
    }
}

pub(super) fn server() -> Server {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    // `NEXT` is per including module (#[path]); module_path! plus an atomic,
    // bounded create_dir retry keeps names unique across modules (D96).
    let tag = module_path!().replace("::", "_");
    let mut attempt = 0;
    let path = loop {
        let path = std::env::temp_dir().join(format!(
            "n1-full-service-{}-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => break path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && attempt < 64 => {
                attempt += 1
            }
            Err(error) => panic!("full fixture CA dir {path:?}: {error}"),
        }
    };
    let ca_dir = CaDir(path);
    std::fs::write(ca_dir.0.join("ca.pem"), identity.cert.pem()).unwrap();
    let mut cfg = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls(&identity, b"h3")).unwrap(),
    ));
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_bidi_streams((TOTAL as u32).into());
    cfg.transport_config(Arc::new(transport));
    let endpoint = quinn::Endpoint::server(cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
    Server {
        endpoint,
        tls: Box::new((identity, ca_dir)),
    }
}

pub(super) fn config(server: &Server) -> TuicClientConfig {
    let ca = server.tls.1 .0.join("ca.pem");
    let mut cfg = TuicClientConfig::from_sources(
        Some(&server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some(ca.to_str().unwrap()),
        Some("h3"),
    )
    .unwrap();
    cfg.tcp_pool = 1;
    cfg
}

pub(super) async fn auth(peer: &quinn::Connection, cfg: &TuicClientConfig) {
    let mut auth = peer.accept_uni().await.unwrap();
    let bytes = auth.read_to_end(64).await.unwrap();
    let mut token = [0; 32];
    peer.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes())
        .unwrap();
    assert_eq!(bytes, encode_authenticate(&cfg.uuid, &token));
}

fn target(port: u16) -> TargetAddr {
    TargetAddr::IpPort(([192, 0, 2, 10], port).into())
}

struct Row {
    handle: SocketHandle,
    sent: usize,
    target: usize,
    window: Option<u16>,
}

pub(super) struct App {
    device: LoopbackTunDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    rows: Vec<Row>,
    udp: SocketHandle,
    base: u16,
    udp_sent: bool,
    tcp_reply: Vec<u8>,
    udp_reply: Vec<u8>,
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
        udp.bind(50000).unwrap();
        let udp = sockets.add(udp);
        let rows = (0..=TOTAL)
            .map(|_| Row {
                handle: sockets.add(tcp::Socket::new(
                    tcp::SocketBuffer::new(vec![0; 4096]),
                    tcp::SocketBuffer::new(vec![0; 8192]),
                )),
                sent: 0,
                target: 1,
                window: None,
            })
            .collect();
        Self {
            device,
            iface,
            sockets,
            rows,
            udp,
            base: 20000,
            udp_sent: true,
            tcp_reply: Vec::with_capacity(64),
            udp_reply: Vec::with_capacity(64),
        }
    }
    pub(super) fn connect(&mut self, range: std::ops::Range<usize>) {
        assert!(range.end <= self.rows.len());
        for index in range {
            self.sockets
                .get_mut::<tcp::Socket>(self.rows[index].handle)
                .connect(
                    self.iface.context(),
                    (Ipv4Address::new(192, 0, 2, 10), 10000),
                    self.base + index as u16,
                )
                .unwrap();
        }
    }
    pub(super) fn acked(&self, range: std::ops::Range<usize>) -> bool {
        range.into_iter().all(|i| {
            let r = &self.rows[i];
            let s = self.sockets.get::<tcp::Socket>(r.handle);
            r.sent == r.target && s.state() == tcp::State::Established && s.send_queue() == 0
        })
    }
    pub(super) fn start_c(&mut self) {
        self.rows[TOTAL].target = C_BYTES;
        self.connect(TOTAL..TOTAL + 1);
    }
    pub(super) fn c_full(&self) -> bool {
        let row = &self.rows[TOTAL];
        let socket = self.sockets.get::<tcp::Socket>(row.handle);
        row.sent == C_BYTES
            && socket.send_queue() == 4096
            && row.window == Some(0)
            && socket.state() == tcp::State::Established
    }
    pub(super) fn c_closed(&self) -> bool {
        self.sockets
            .get::<tcp::Socket>(self.rows[TOTAL].handle)
            .state()
            == tcp::State::Closed
    }
    pub(super) fn prepare_health(&mut self) {
        self.rows[0].target += 32;
        self.tcp_reply.clear();
        self.udp_reply.clear();
        self.udp_sent = false;
    }
    pub(super) fn healthy(&self) -> bool {
        self.tcp_reply == b"full-healthy" && self.udp_reply == b"full-udp-reply"
    }
    pub(super) fn restart(&mut self) {
        assert!(self.c_closed());
        for row in self.rows.drain(..) {
            self.sockets.remove(row.handle);
        }
        self.rows.push(Row {
            handle: self.sockets.add(tcp::Socket::new(
                tcp::SocketBuffer::new(vec![0; 4096]),
                tcp::SocketBuffer::new(vec![0; 8192]),
            )),
            sent: 0,
            target: 1,
            window: None,
        });
        self.base = 30000;
        self.udp_sent = true;
        self.tcp_reply.clear();
        self.udp_reply.clear();
        assert_eq!(
            self.sockets.get::<udp::Socket>(self.udp).endpoint().port,
            50000
        );
        self.connect(0..1);
    }
    pub(super) async fn turn(&mut self) {
        for _ in 0..64 {
            if !self.device.try_recv_rx().unwrap() {
                break;
            }
            if let Some(bytes) = self.device.rx_peek() {
                let ip = smoltcp::wire::Ipv4Packet::new_checked(bytes).unwrap();
                if ip.next_header() == smoltcp::wire::IpProtocol::Tcp {
                    let tcp = smoltcp::wire::TcpPacket::new_checked(ip.payload()).unwrap();
                    if let Some(i) = tcp
                        .dst_port()
                        .checked_sub(self.base)
                        .map(usize::from)
                        .filter(|i| *i < self.rows.len())
                    {
                        self.rows[i].window = Some(tcp.window_len());
                    }
                }
            }
            assert!(self.device.stage_rx_for_iface());
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        let mut scratch = [0; 8192];
        for (i, row) in self.rows.iter_mut().enumerate() {
            let socket = self.sockets.get_mut::<tcp::Socket>(row.handle);
            if socket.can_send() && row.sent < row.target {
                scratch.fill((i % 251) as u8);
                let n = (row.target - row.sent).min(scratch.len());
                row.sent += socket.send_slice(&scratch[..n]).unwrap();
            }
            if i == 0 {
                while socket.can_recv() {
                    let n = socket.recv_slice(&mut scratch).unwrap();
                    assert!(self.tcp_reply.len() + n <= 64);
                    self.tcp_reply.extend_from_slice(&scratch[..n]);
                }
            }
        }
        let udp = self.sockets.get_mut::<udp::Socket>(self.udp);
        if !self.udp_sent {
            udp.send_slice(b"full-udp", (Ipv4Address::new(192, 0, 2, 10), 10001))
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
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

pub(super) struct Peer {
    pub(super) connection: quinn::Connection,
    streams: Vec<(quinn::SendStream, quinn::RecvStream)>,
    healthy: Option<usize>,
}
impl Peer {
    pub(super) fn new(connection: quinn::Connection) -> Self {
        Self {
            connection,
            streams: Vec::with_capacity(F),
            healthy: None,
        }
    }
    pub(super) async fn accept_to(&mut self, end: usize) {
        assert!(end <= F);
        let header = encode_connect(&target(10000));
        while self.streams.len() < end {
            let (tx, mut rx) = self.connection.accept_bi().await.unwrap();
            assert_eq!(rx.id().index(), self.streams.len() as u64);
            let mut actual = vec![0; header.len() + 1];
            rx.read_exact(&mut actual).await.unwrap();
            assert_eq!(&actual[..header.len()], &header);
            if self.streams.len() < 128 && actual[header.len()] == 0 {
                assert!(self.healthy.replace(self.streams.len()).is_none());
            }
            self.streams.push((tx, rx));
        }
    }
    pub(super) async fn health(&mut self) -> u16 {
        health_pair(
            &self.connection,
            &mut self.streams[self.healthy.unwrap()],
            || {},
            None,
        )
        .await
    }
    pub(super) async fn retire(&mut self) {
        // Only one fixture future, no per-stream tasks. Every final offset is
        // returned on actual STOP/EOF while the original saved driver runs.
        for (index, (tx, rx)) in self.streams.iter_mut().enumerate() {
            if index % 256 == 0 { eprintln!("[full-peer-retire] index={index} stage=wait-STOP"); }
            let code = tx.stopped().await.unwrap().expect("actual STOP");
            if index % 256 == 0 { eprintln!("[full-peer-retire] index={index} stage=wait-EOF"); }
            assert!(rx.read_to_end(0).await.unwrap().is_empty());
            tx.reset(code).unwrap();
            if (index + 1) % 32 == 0 {
                tokio::task::yield_now().await;
            }
        }
        self.streams.clear();
        self.healthy = None;
    }
    pub(super) fn count(&self) -> usize {
        self.streams.len()
    }
}

pub(super) async fn fill<S: TunService>(service: &mut S, app: &mut App, peer: &mut Peer) {
    for end in (128..=F).step_by(128) {
        app.connect(end - 128..end);
        drive(service, Duration::from_secs(20), async {
            let remote = peer.accept_to(end);
            let application = async {
                while !app.acked(end - 128..end) {
                    app.turn().await;
                }
            };
            tokio::join!(remote, application);
        })
        .await;
        println!("saved fullF wave={end} actualpeer={}", peer.count());
    }
}
pub(super) async fn health<S: TunService>(service: &mut S, app: &mut App, peer: &mut Peer) -> u16 {
    app.prepare_health();
    drive(service, Duration::from_secs(5), async {
        let remote = peer.health();
        let application = async {
            while !app.healthy() {
                app.turn().await;
            }
        };
        let (assoc, ()) = tokio::join!(remote, application);
        assoc
    })
    .await
}

pub(super) async fn drain<E: std::borrow::BorrowMut<quinn::Endpoint>>(
    mut server: E,
    peers: Vec<quinn::Connection>,
) {
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    server.borrow_mut().close(0u32.into(), b"done");
    let drain = server.borrow_mut().take_driver_drain_handle().unwrap();
    drop(server);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), drain.drained())
            .await
            .unwrap()
    );
}

// One running-service reset/retry tracer. Original methods above stay unchanged;
// this same peer still owns at most F half-pairs and never reindexes healthy.
impl Peer {
    pub(super) fn reset_nonhealthy(&mut self) -> u64 {
        assert_eq!(self.streams.len(), F);
        assert_ne!(self.healthy, Some(F - 1));
        let stream = self.streams.pop().unwrap();
        reset_pair(stream)
    }

    pub(super) async fn accept_one_waiting(&mut self) -> (u64, u8) {
        assert_eq!(self.streams.len(), F - 1);
        let (stream, accepted) = accept_waiting(&self.connection).await;
        self.streams.push(stream);
        assert_eq!(self.streams.len(), F);
        accepted
    }
}

// Complete original wire transactions shared by the bounded overlap tracer.
type Pair = (quinn::SendStream, quinn::RecvStream);

async fn health_pair(
    connection: &quinn::Connection,
    stream: &mut Pair,
    after_request: impl FnOnce(),
    mut heartbeat_count: Option<&mut usize>,
) -> u16 {
    let (tx, rx) = stream;
    let mut actual = [255; 32];
    rx.read_exact(&mut actual).await.unwrap();
    assert_eq!(actual, [0; 32]);
    after_request();
    tx.write_all(b"full-healthy").await.unwrap();
    let mut local_heartbeats = 0;
    let packet = loop {
        let packet = connection.read_datagram().await.unwrap();
        if packet.as_ref() == [0x05, 0x04] {
            let fair = heartbeat_count.is_some();
            let count = heartbeat_count
                .as_deref_mut()
                .unwrap_or(&mut local_heartbeats);
            assert!(*count < 4, "bounded control heartbeat budget exhausted");
            *count += 1;
            if fair {
                println!(
                    "fullWF Fair actual heartbeat bytes={packet:?} len={} total={count}",
                    packet.len()
                );
            }
            continue;
        }
        break packet;
    };
    assert!(
        packet.len() >= 4,
        "unexpected datagram len={} prefix={:?}",
        packet.len(),
        &packet[..packet.len().min(16)]
    );
    let assoc = u16::from_be_bytes([packet[2], packet[3]]);
    assert_eq!(
        &packet[..],
        encode_packet(assoc, &target(10001), b"full-udp")
    );
    connection
        .send_datagram(encode_packet(assoc, &target(10001), b"full-udp-reply").into())
        .unwrap();
    assoc
}

fn reset_pair(stream: Pair) -> u64 {
    let (mut tx, mut rx) = stream;
    let id = rx.id().index();
    assert_eq!(id, (F - 1) as u64);
    assert_eq!(tx.id(), rx.id());
    tx.reset(77u32.into()).unwrap();
    rx.stop(78u32.into()).unwrap();
    drop((tx, rx));
    id
}

async fn accept_waiting(connection: &quinn::Connection) -> (Pair, (u64, u8)) {
    let (tx, mut rx) = connection.accept_bi().await.unwrap();
    let id = rx.id().index();
    assert_eq!(id, F as u64); // Cumulative accept4097, held4095 before append.
    let header = encode_connect(&target(10000));
    let mut actual = vec![0; header.len() + 1];
    rx.read_exact(&mut actual).await.unwrap();
    assert_eq!(&actual[..header.len()], &header);
    let marker = actual[header.len()];
    assert!(
        (80..208).contains(&marker),
        "original Waiting payload, never C"
    );
    ((tx, rx), (id, marker))
}

/// Bounded value-only wire/app facts; never grants production flow authority.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct FairFacts {
    pub(super) remote_rounds: usize,
    pub(super) reset_stream: Option<u64>,
    pub(super) reset_after_request: Option<usize>,
    pub(super) accepted: Option<(u64, u8)>,
    pub(super) healthy_stream: u64,
    pub(super) app_rounds: usize,
    pub(super) post_promotion: usize,
    pub(super) finished: bool,
    pub(super) control_heartbeats: usize,
}
#[derive(Default)]
pub(super) struct FairProgress(std::cell::Cell<FairFacts>);
impl FairProgress {
    pub(super) fn snapshot(&self) -> FairFacts {
        self.0.get()
    }
    pub(super) fn record_app_round(&self, round: usize, promoted: bool) {
        let mut facts = self.0.get();
        assert_eq!(round, facts.app_rounds + 1);
        assert!(round <= FAIR_MAX_ROUNDS);
        facts.app_rounds = round;
        facts.post_promotion += usize::from(promoted);
        self.0.set(facts);
    }
}
pub(super) const FAIR_ROUNDS: usize = 16;
pub(super) const FAIR_MAX_ROUNDS: usize = 1024;

impl Peer {
    pub(super) async fn fair_reset(
        &mut self,
        progress: &FairProgress,
        assoc: u16,
    ) -> (u64, (u64, u8)) {
        assert_eq!(self.streams.len(), F);
        assert_ne!(self.healthy, Some(F - 1));
        // This is an original owned pair, not a released provider/Flow slot.
        // Until reset, 4095 Vec pairs + this local pair still hold all4096.
        let mut retiring = Some(self.streams.pop().unwrap());
        let healthy = &mut self.streams[self.healthy.unwrap()];
        progress.0.set(FairFacts {
            healthy_stream: healthy.1.id().index(),
            ..Default::default()
        });
        let connection = &self.connection;
        let receive_waiting = async {
            let accepted = accept_waiting(connection).await;
            let mut facts = progress.0.get();
            assert!(
                facts.reset_stream.is_some(),
                "no extra F before actual reset"
            );
            facts.accepted = Some(accepted.1);
            progress.0.set(facts);
            accepted
        };
        let exchange = async {
            let mut control_heartbeats = 0;
            for round in 1..=FAIR_MAX_ROUNDS {
                let actual_assoc = health_pair(
                    connection,
                    healthy,
                    || {
                        if round == 4 {
                            // Request has actually been read and verified. Reset
                            // immediately, then the original helper writes its reply.
                            let id = reset_pair(retiring.take().unwrap());
                            let mut facts = progress.0.get();
                            assert!(facts.reset_stream.is_none());
                            facts.reset_stream = Some(id);
                            facts.reset_after_request = Some(round);
                            progress.0.set(facts);
                        }
                    },
                    Some(&mut control_heartbeats),
                )
                .await;
                assert_eq!(
                    actual_assoc, assoc,
                    "same original UDP association each round"
                );
                let mut facts = progress.0.get();
                facts.remote_rounds = round;
                facts.control_heartbeats = control_heartbeats;
                // App's counter records only actual TCP+UDP receipt after its
                // observed original full-root gate. Finish after this response;
                // App sees finished only while consuming that final response.
                facts.finished = facts.accepted.is_some() && facts.post_promotion >= FAIR_ROUNDS;
                progress.0.set(facts);
                if facts.finished {
                    break;
                }
            }
            assert!(
                progress.snapshot().finished,
                "bounded fair workload exhausted before actual progress: {:?}",
                progress.snapshot()
            );
        };
        // Co-poll accept throughout the exchange; appending afterwards only
        // avoids reallocating the original Vec while healthy is still borrowed.
        let ((stream, accepted), ()) = tokio::join!(receive_waiting, exchange);
        assert!(retiring.is_none());
        self.streams.push(stream);
        assert_eq!(self.streams.len(), F);
        (progress.0.get().reset_stream.unwrap(), accepted)
    }
}

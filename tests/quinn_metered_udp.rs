//! A real local UDP receive drives the original Quinn endpoint split and charge.

#[path = "quinn_metered_udp/lifecycle.rs"]
mod lifecycle;

#[path = "quinn_metered_udp/construction.rs"]
mod construction;

use bytes::{BackingBudget, BackingMeter};
use std::{
    future::Future,
    io::{self, IoSliceMut},
    net::{SocketAddr, UdpSocket},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

#[derive(Debug)]
struct InlineFirstPollRuntime(Arc<dyn quinn::Runtime>);

impl quinn::Runtime for InlineFirstPollRuntime {
    fn new_timer(&self, at: std::time::Instant) -> Pin<Box<dyn quinn::AsyncTimer>> {
        self.0.new_timer(at)
    }

    fn spawn(&self, mut future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        let mut cx = Context::from_waker(std::task::Waker::noop());
        if future.as_mut().poll(&mut cx).is_pending() {
            self.0.spawn(future);
        }
    }

    fn wrap_udp_socket(&self, socket: UdpSocket) -> io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        self.0.wrap_udp_socket(socket)
    }
}

#[derive(Debug)]
struct CountingSocket {
    inner: Arc<dyn quinn::AsyncUdpSocket>,
    received: Arc<AtomicUsize>,
    max_receive_segments_override: Option<usize>,
}

impl quinn::AsyncUdpSocket for CountingSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn quinn::UdpPoller>> {
        self.inner.clone().create_io_poller()
    }

    fn try_send(&self, transmit: &quinn::udp::Transmit) -> io::Result<()> {
        self.inner.try_send(transmit)
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let result = self.inner.poll_recv(cx, bufs, meta);
        if let Poll::Ready(Ok(n)) = &result {
            self.received.fetch_add(*n, Ordering::AcqRel);
        }
        result
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.max_receive_segments_override
            .unwrap_or_else(|| self.inner.max_receive_segments())
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

fn local_server(
    cert: rustls::pki_types::CertificateDer<'static>,
    key: rustls::pki_types::PrivateKeyDer<'static>,
) -> quinn::Endpoint {
    let mut tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .expect("server tls");
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).expect("server quic tls"),
    ));
    quinn::Endpoint::server(config, "127.0.0.1:0".parse().expect("loopback"))
        .expect("server endpoint")
}

fn local_client_config(cert: rustls::pki_types::CertificateDer<'static>) -> quinn::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).expect("test root");
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("client quic tls"),
    ))
}

#[tokio::test]
async fn connection_event_layout_rejection_leaves_proto_unregistered() {
    let runtime = quinn::default_runtime().unwrap();
    let budget = BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let socket = runtime
        .wrap_udp_socket(UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let mut endpoint = quinn::Endpoint::new_with_metered_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        socket,
        runtime,
        meter,
    )
    .unwrap();
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    endpoint.set_default_client_config(local_client_config(
        rustls::pki_types::CertificateDer::from(identity.cert.der().to_vec()),
    ));
    let baseline = budget.used();
    let held = budget.try_reserve(budget.limit() - baseline).unwrap();
    assert!(matches!(
        endpoint.connect("127.0.0.1:4433".parse().unwrap(), "localhost"),
        Err(quinn::ConnectError::EventCapacityUnavailable)
    ));
    assert_eq!(endpoint.open_connections(), 0);
    assert_eq!(budget.used(), budget.limit());
    assert!(!budget.quarantined());
    drop(held);
    assert_eq!(budget.used(), baseline);
    let drain = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), drain.drained())
            .await
            .unwrap()
    );
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn budgeted_endpoint_rejects_insufficient_receive_layout_before_creation() {
    let runtime = quinn::default_runtime().expect("Tokio runtime");
    let socket = UdpSocket::bind("127.0.0.1:0").expect("receive socket");
    let budget = BackingBudget::new(1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let result = quinn::Endpoint::new_with_metered_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        runtime.wrap_udp_socket(socket).expect("wrapped socket"),
        runtime,
        meter,
    );
    assert!(
        result.is_err(),
        "an impossible legal GRO layout must fail cold"
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn budgeted_rebind_rejects_larger_receive_segments_without_replacing_socket() {
    let runtime = quinn::default_runtime().expect("Tokio runtime");
    let old_socket = UdpSocket::bind("127.0.0.1:0").expect("old socket");
    let old_addr = old_socket.local_addr().expect("old address");
    let budget = BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut endpoint = quinn::Endpoint::new_with_metered_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        runtime
            .wrap_udp_socket(old_socket)
            .expect("wrapped old socket"),
        runtime.clone(),
        meter.clone(),
    )
    .expect("endpoint");
    let baseline = budget.used();
    let drain = endpoint.take_driver_drain_handle().expect("driver witness");
    let new_socket = UdpSocket::bind("127.0.0.1:0").expect("new socket");
    let oversized = Arc::new(CountingSocket {
        inner: runtime
            .wrap_udp_socket(new_socket)
            .expect("wrapped new socket"),
        received: Arc::new(AtomicUsize::new(0)),
        max_receive_segments_override: Some(usize::MAX),
    });
    assert!(endpoint.rebind_abstract(oversized).is_err());
    assert_eq!(endpoint.stats().socket_rebinds, 0);
    assert_eq!(budget.used(), baseline);
    let sender = UdpSocket::bind("127.0.0.1:0").expect("sender");
    sender
        .send_to(&[0xff; 64], old_addr)
        .expect("old socket datagram");
    tokio::time::timeout(Duration::from_secs(2), async {
        while meter.snapshot().registered_total == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("old socket was displaced by rejected rebind");
    drop(endpoint);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), drain.drained())
            .await
            .expect("driver drain deadline")
    );
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn original_endpoint_receive_registers_and_releases_one_backing() {
    let runtime = quinn::default_runtime().expect("Tokio runtime");
    let socket = UdpSocket::bind("127.0.0.1:0").expect("receive socket");
    let addr = socket.local_addr().expect("receive address");
    let meter = BackingMeter::new();
    let mut endpoint = quinn::Endpoint::new_with_metered_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        runtime.wrap_udp_socket(socket).expect("wrapped socket"),
        runtime,
        meter.clone(),
    )
    .expect("endpoint");
    let drain = endpoint
        .take_driver_drain_handle()
        .expect("original driver");
    let sender = UdpSocket::bind("127.0.0.1:0").expect("sender");
    sender.send_to(&[0xff; 64], addr).expect("local datagram");
    tokio::time::timeout(Duration::from_secs(2), async {
        while meter.snapshot().registered_total == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original receive did not charge");
    assert_eq!(meter.snapshot().registered_total, 1);
    drop(endpoint);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), drain.drained())
            .await
            .expect("driver did not join")
    );
    let settled = meter.snapshot();
    assert_eq!((settled.allocations, settled.payload_bytes), (0, 0));
    assert!(!settled.quarantined);
}

#[tokio::test]
async fn original_endpoint_receive_precharges_budgeted_datagram_before_copy() {
    let runtime = quinn::default_runtime().expect("Tokio runtime");
    let socket = UdpSocket::bind("127.0.0.1:0").expect("receive socket");
    let addr = socket.local_addr().expect("receive address");
    let budget = BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut endpoint = quinn::Endpoint::new_with_metered_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        runtime.wrap_udp_socket(socket).expect("wrapped socket"),
        runtime,
        meter.clone(),
    )
    .expect("endpoint");
    let receive_buffer_charge = budget.used();
    assert!(receive_buffer_charge > 0);
    let drain = endpoint
        .take_driver_drain_handle()
        .expect("original driver");
    let sender = UdpSocket::bind("127.0.0.1:0").expect("sender");
    sender.send_to(&[0xff; 64], addr).expect("local datagram");
    tokio::time::timeout(Duration::from_secs(2), async {
        while meter.snapshot().registered_total == 0 && !meter.snapshot().quarantined {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original receive did not settle");
    let observed = meter.snapshot();
    let charged = budget.used();
    let quarantined = budget.quarantined();
    drop(endpoint);
    let drained = tokio::time::timeout(Duration::from_secs(2), drain.drained()).await;
    assert_eq!(observed.registered_total, 1);
    assert_eq!(charged, receive_buffer_charge + observed.physical_bytes);
    assert!(!quarantined);
    assert!(drained.expect("driver did not join"));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn budget_full_counts_unadmitted_packet_and_keeps_driver_for_later_receive() {
    let runtime = quinn::default_runtime().expect("Tokio runtime");
    let socket = UdpSocket::bind("127.0.0.1:0").expect("receive socket");
    let addr = socket.local_addr().expect("receive address");
    let budget = BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let received = Arc::new(AtomicUsize::new(0));
    let wrapped = Arc::new(CountingSocket {
        inner: runtime.wrap_udp_socket(socket).expect("wrapped socket"),
        received: received.clone(),
        max_receive_segments_override: None,
    });
    let mut endpoint = quinn::Endpoint::new_with_metered_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        wrapped,
        runtime,
        meter.clone(),
    )
    .expect("endpoint");
    let held = budget
        .try_reserve(budget.limit() - budget.used())
        .expect("hold remaining budget after cold receive buffer");
    let drain = endpoint.take_driver_drain_handle().expect("driver witness");
    let sender = UdpSocket::bind("127.0.0.1:0").expect("sender");
    sender.send_to(&[0xff; 64], addr).expect("local datagram");
    tokio::time::timeout(Duration::from_secs(2), async {
        while received.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("socket did not receive original datagram");
    assert_eq!(meter.snapshot().registered_total, 0);
    tokio::time::timeout(Duration::from_secs(2), async {
        while endpoint.stats().budget_rejected_datagrams == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("budget Full was not counted");
    let stats = endpoint.stats();
    assert_eq!(stats.budget_rejected_datagrams, 1);
    assert_eq!(stats.receive_backing_rejected_datagrams, 1);
    assert_eq!(stats.owned_event_rejected_datagrams, 0);
    assert_eq!(stats.budget_rejected_datagrams, stats.receive_backing_rejected_datagrams
        .saturating_add(stats.owned_event_rejected_datagrams));
    drop(held);
    sender.send_to(&[0xee; 64], addr).expect("later datagram");
    tokio::time::timeout(Duration::from_secs(2), async {
        while meter.snapshot().registered_total == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("driver did not process later datagram after budget release");
    let stats = endpoint.stats();
    assert_eq!(stats.budget_rejected_datagrams, 1);
    assert_eq!(stats.receive_backing_rejected_datagrams, 1);
    assert_eq!(stats.owned_event_rejected_datagrams, 0);
    assert_eq!(stats.budget_rejected_datagrams, stats.receive_backing_rejected_datagrams
        .saturating_add(stats.owned_event_rejected_datagrams));
    drop(endpoint);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), drain.drained())
            .await
            .expect("driver did not join")
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn real_quic_two_streams_share_meter_until_last_backing_reference() {
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("temporary test identity");
    let cert = rustls::pki_types::CertificateDer::from(identity.cert.der().to_vec());
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
        identity.signing_key.serialize_der(),
    ));
    let server = local_server(cert.clone(), key);
    let runtime = quinn::default_runtime().expect("Tokio runtime");
    let socket = UdpSocket::bind("127.0.0.1:0").expect("receive socket");
    let meter = BackingMeter::new();
    let mut client = quinn::Endpoint::new_with_metered_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        runtime.wrap_udp_socket(socket).expect("wrapped socket"),
        runtime,
        meter.clone(),
    )
    .expect("client endpoint");
    client.set_default_client_config(local_client_config(cert));
    let drain = client.take_driver_drain_handle().expect("original driver");
    let server_addr = server.local_addr().expect("server address");
    let (client_conn, server_conn) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            async {
                client
                    .connect(server_addr, "localhost")
                    .expect("connect")
                    .await
                    .expect("client handshake")
            },
            async {
                server
                    .accept()
                    .await
                    .expect("incoming")
                    .await
                    .expect("server handshake")
            },
        )
    })
    .await
    .expect("handshake deadline");
    let mut first_tx = server_conn.open_uni().await.expect("first server stream");
    let mut second_tx = server_conn.open_uni().await.expect("second server stream");
    first_tx
        .write_all(b"first-flow")
        .await
        .expect("first write");
    second_tx
        .write_all(b"second-flow")
        .await
        .expect("second write");
    first_tx.finish().expect("first finish");
    second_tx.finish().expect("second finish");
    let mut first_rx = tokio::time::timeout(Duration::from_secs(5), client_conn.accept_uni())
        .await
        .expect("first accept deadline")
        .expect("first client stream");
    let mut second_rx = tokio::time::timeout(Duration::from_secs(5), client_conn.accept_uni())
        .await
        .expect("second accept deadline")
        .expect("second client stream");
    let first = first_rx
        .read_chunk(64, true)
        .await
        .expect("first read")
        .expect("first chunk");
    let second = second_rx
        .read_chunk(64, true)
        .await
        .expect("second read")
        .expect("second chunk");
    assert_eq!(&first.bytes[..], b"first-flow");
    assert_eq!(&second.bytes[..], b"second-flow");
    assert!(first
        .bytes
        .backing_meter()
        .is_some_and(|source| source.same_authority(&meter)));
    assert!(second
        .bytes
        .backing_meter()
        .is_some_and(|source| source.same_authority(&meter)));
    assert!(meter.snapshot().registered_total > 1);
    drop(first);
    drop(first_rx);
    drop(first_tx);
    drop(second_rx);
    drop(second_tx);
    drop(client_conn);
    drop(server_conn);
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    drop(client);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), drain.drained())
            .await
            .expect("client driver deadline")
    );
    drop(server);
    let held = meter.snapshot();
    assert_eq!(
        held.allocations, 1,
        "the second Flow owns the one surviving backing"
    );
    assert!(held.payload_bytes > 0);
    assert!(held.physical_bytes > 0);
    assert!(!held.quarantined);
    drop(second);
    let settled = meter.snapshot();
    assert_eq!(
        (
            settled.allocations,
            settled.payload_bytes,
            settled.physical_bytes
        ),
        (0, 0, 0)
    );
    assert!(!settled.quarantined);
}

#[tokio::test]
async fn real_quic_second_stream_recovers_after_counted_budget_full_loss() {
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("temporary test identity");
    let cert = rustls::pki_types::CertificateDer::from(identity.cert.der().to_vec());
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(rustls::pki_types::PrivatePkcs8KeyDer::from(
        identity.signing_key.serialize_der(),
    ));
    let server = local_server(cert.clone(), key);
    let runtime: Arc<dyn quinn::Runtime> = Arc::new(InlineFirstPollRuntime(
        quinn::default_runtime().expect("Tokio runtime"),
    ));
    let socket = UdpSocket::bind("127.0.0.1:0").expect("receive socket");
    let probe = UdpSocket::bind("127.0.0.1:0").unwrap();
    let (endpoint_bytes, connection_bytes) =
        lifecycle::measure_unpublished_cold_layout(runtime.clone(), probe.local_addr().unwrap())
            .await;
    drop(probe);
    let budget = BackingBudget::new(endpoint_bytes + connection_bytes + 64 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut client = quinn::Endpoint::new_with_metered_abstract_socket(
        quinn::EndpointConfig::default(),
        None,
        runtime.wrap_udp_socket(socket).expect("wrapped socket"),
        runtime,
        meter.clone(),
    )
    .expect("client endpoint");
    client.set_default_client_config(local_client_config(cert));
    let drain = client.take_driver_drain_handle().expect("driver witness");
    let server_addr = server.local_addr().expect("server address");
    let (client_conn, server_conn) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            async {
                client
                    .connect(server_addr, "localhost")
                    .unwrap()
                    .await
                    .unwrap()
            },
            async { server.accept().await.unwrap().await.unwrap() },
        )
    })
    .await
    .expect("handshake deadline");

    let mut first_tx = server_conn.open_uni().await.expect("first Flow");
    first_tx
        .write_all(b"held-first-flow")
        .await
        .expect("first write");
    first_tx.finish().expect("first finish");
    let mut first_rx = tokio::time::timeout(Duration::from_secs(5), client_conn.accept_uni())
        .await
        .expect("first accept deadline")
        .expect("first receive");
    let first = first_rx.read_chunk(64, true).await.unwrap().unwrap();
    assert_eq!(&first.bytes[..], b"held-first-flow");

    let mut held = Vec::new();
    let mut free = budget.limit() - budget.used();
    for _ in 0..32 {
        if free == 0 {
            break;
        }
        if let Some(reservation) = budget.try_reserve(free) {
            held.push(reservation);
            free = budget.limit() - budget.used();
        } else {
            tokio::task::yield_now().await;
            free = budget.limit() - budget.used();
        }
    }
    assert_eq!(budget.used(), budget.limit(), "test must force actual Full");

    let mut second_tx = server_conn.open_uni().await.expect("second Flow");
    second_tx
        .write_all(&[0x5a; 4096])
        .await
        .expect("second write");
    second_tx.finish().expect("second finish");
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.stats().budget_rejected_datagrams == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second Flow never hit real receive Full");
    let stats = client.stats();
    assert!(stats.receive_backing_rejected_datagrams > 0);
    assert_eq!(stats.owned_event_rejected_datagrams, 0);
    assert_eq!(stats.budget_rejected_datagrams, stats.receive_backing_rejected_datagrams
        .saturating_add(stats.owned_event_rejected_datagrams));
    drop(held);
    let mut second_rx = tokio::time::timeout(Duration::from_secs(5), client_conn.accept_uni())
        .await
        .expect("second accept deadline")
        .expect("second receive");
    let delivered = tokio::time::timeout(Duration::from_secs(5), second_rx.read_to_end(8192))
        .await
        .expect("second delivery deadline")
        .expect("second delivery");
    assert_eq!(delivered, [0x5a; 4096]);
    assert_eq!(&first.bytes[..], b"held-first-flow");
    assert!(!budget.quarantined());

    drop(first);
    drop(first_rx);
    drop(first_tx);
    drop(second_rx);
    drop(second_tx);
    drop(client_conn);
    drop(server_conn);
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    drop(client);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), drain.drained())
            .await
            .expect("driver drain deadline")
    );
    drop(server);
    assert_eq!(budget.used(), 0);
}

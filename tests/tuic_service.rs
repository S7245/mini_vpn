//! External callers use the real exported saved service/control, no private seam.
#![cfg(feature = "harness")]
use mini_vpn::client_tun::{
    NoopSink, TuicServicePhase, TunRuntimeConfig, TunService, prepare_tuic_service,
};
use mini_vpn::device::TunIo;
use mini_vpn::harness::LoopbackTunDevice;
use mini_vpn::metrics::Metrics;
use mini_vpn::shared::TargetAddr;
use mini_vpn::tuic::{
    NodeShutdown, TuicClientConfig, encode_authenticate, encode_connect, encode_packet,
};
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::{tcp, udp};
use smoltcp::wire::{HardwareAddress, IpCidr, Ipv4Address};
use std::future::Future;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

async fn capture<F: Future>(work: F) -> std::thread::Result<F::Output> {
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

fn server() -> quinn::Endpoint {
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
    let config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls).unwrap(),
    ));
    quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap()
}

struct Apps {
    device: LoopbackTunDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    target: std::net::Ipv4Addr,
    tcp: SocketHandle,
    udp: SocketHandle,
    sent_tcp: bool,
    tcp_payload: &'static [u8],
    sent_udp: bool,
    tcp_reply: Vec<u8>,
    udp_reply: Vec<u8>,
}

impl Apps {
    fn restart(&mut self) {
        let socket = self.sockets.get_mut::<tcp::Socket>(self.tcp);
        socket.abort();
        socket
            .connect(
                self.iface.context(),
                (Ipv4Address::from(self.target), 10000),
                40002,
            )
            .unwrap();
        self.sent_tcp = false;
        self.sent_udp = false;
        self.tcp_reply.clear();
        self.udp_reply.clear();
    }

    fn new(mut device: LoopbackTunDevice) -> Self {
        let target = std::net::Ipv4Addr::new(192, 0, 2, 10);
        let mut iface = Interface::new(
            Config::new(HardwareAddress::Ip),
            &mut device,
            smoltcp::time::Instant::now(),
        );
        let local = Ipv4Address::new(10, 0, 0, 1);
        iface.update_ip_addrs(|a| a.push(IpCidr::new(local.into(), 24)).unwrap());
        iface.routes_mut().add_default_ipv4_route(local).unwrap();
        let mut sockets = SocketSet::new(vec![]);
        let mut tcp = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; 4096]),
            tcp::SocketBuffer::new(vec![0; 4096]),
        );
        tcp.connect(iface.context(), (Ipv4Address::from(target), 10000), 40000)
            .unwrap();
        let tcp = sockets.add(tcp);
        let mut udp = udp::Socket::new(
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0; 4096]),
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0; 4096]),
        );
        udp.bind(40001).unwrap();
        let udp = sockets.add(udp);
        Self {
            device,
            iface,
            sockets,
            target,
            tcp,
            udp,
            sent_tcp: false,
            tcp_payload: b"service-tcp",
            sent_udp: false,
            tcp_reply: vec![],
            udp_reply: vec![],
        }
    }

    async fn turn(&mut self) {
        while self.device.try_recv_rx().unwrap() {
            assert!(self.device.stage_rx_for_iface());
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        let tcp = self.sockets.get_mut::<tcp::Socket>(self.tcp);
        if tcp.can_send() && !self.sent_tcp {
            tcp.send_slice(self.tcp_payload).unwrap();
            self.sent_tcp = true;
        }
        while tcp.can_recv() {
            let mut b = [0; 128];
            let n = tcp.recv_slice(&mut b).unwrap();
            self.tcp_reply.extend_from_slice(&b[..n]);
        }
        let udp = self.sockets.get_mut::<udp::Socket>(self.udp);
        if !self.sent_udp {
            udp.send_slice(b"service-udp", (Ipv4Address::from(self.target), 10001))
                .unwrap();
            self.sent_udp = true;
        }
        while udp.can_recv() {
            let (b, _) = udp.recv().unwrap();
            self.udp_reply.extend_from_slice(b);
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        self.device.flush_tx().await.unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn public_control_rebuilds_once_and_cancelled_run_keeps_original_connecting() {
    let mut server = server();
    let mut cfg = TuicClientConfig::from_sources(
        Some(&server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap();
    cfg.tcp_pool = 1;
    let (device, app_device) = LoopbackTunDevice::bounded_pair(64, 4);
    let mut service = prepare_tuic_service(
        device,
        cfg.clone(),
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    assert!(service.take_handle().is_none());
    assert!(!handle.request_rebuild());
    let mut peers: Vec<quinn::Connection> = Vec::with_capacity(2);
    let mut apps = Apps::new(app_device);
    let scenario = capture(async {
        for round in 0..2 {
            let incoming = tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    incoming = server.accept() => incoming.unwrap(),
                    report = service.run() => panic!("premature service completion {report:?}"),
                }
            }).await.unwrap();
            if round == 1 {
                // New physical Connecting exists only after the old root gate.
                tokio::time::timeout(Duration::from_secs(2), peers[0].closed()).await.unwrap();
                for _ in 0..4 {
                    assert!(tokio::time::timeout(Duration::from_millis(10), service.run()).await.is_err());
                    assert_eq!(handle.phase(), TuicServicePhase::Starting);
                    assert!(!handle.request_rebuild());
                }
                assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err(), "cancelled run must not redial");
                apps.restart();
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    _ = async {
                        let peer = incoming.await.unwrap();
                        let mut auth = peer.accept_uni().await.unwrap();
                        let actual = auth.read_to_end(64).await.unwrap();
                        let mut token = [0;32];
                        peer.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes()).unwrap();
                        assert_eq!(actual, encode_authenticate(&cfg.uuid, &token));
                        let tcp = async {
                            let (mut tx,mut rx) = peer.accept_bi().await.unwrap();
                            let mut expected = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                            expected.extend_from_slice(b"service-tcp");
                            let mut bytes = vec![0;expected.len()]; rx.read_exact(&mut bytes).await.unwrap(); assert_eq!(bytes,expected);
                            tx.write_all(b"tcp-reply").await.unwrap(); tx.finish().unwrap(); (tx,rx)
                        };
                        let udp = async {
                            let actual = peer.read_datagram().await.unwrap();
                            let assoc = u16::from_be_bytes([actual[2],actual[3]]);
                            assert_eq!(actual.as_ref(), encode_packet(assoc,&TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap()),b"service-udp"));
                            peer.send_datagram(encode_packet(assoc,&TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap()),b"udp-reply").into()).unwrap();
                        };
                        let app = async { while apps.tcp_reply!=b"tcp-reply" || apps.udp_reply!=b"udp-reply" { apps.turn().await;tokio::time::sleep(Duration::from_millis(1)).await; } };
                        let (streams,(),()) = tokio::join!(tcp,udp,app);drop(streams);peers.push(peer);
                    } => {},
                    report = service.run() => panic!("DATA failed {report:?}"),
                }
            }).await.unwrap();
            assert_eq!(handle.phase(), TuicServicePhase::Running);
            assert_eq!(handle.request_rebuild(), round==0);
        }
    }).await;
    handle.request_stop();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), service.run())
            .await
            .unwrap();
        assert!(report.device_settled());
        assert!(!report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
        }
        assert_eq!(handle.observe_shutdown().await, TuicServicePhase::Complete);
    })
    .await;
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(e) = scenario {
        std::panic::resume_unwind(e);
    }
    if let Err(e) = cleanup {
        std::panic::resume_unwind(e);
    }
    assert!(drained.unwrap());
}

#[path = "tuic_service/pool.rs"]
mod pool;

#[path = "tuic_service/pressure.rs"]
mod pressure;

#[path = "tuic_service/downlink.rs"]
mod downlink;

#[path = "tuic_service/full.rs"]
mod full;

#[path = "tuic_service/metrics.rs"]
mod metrics;

#[path = "tuic_service/failover.rs"]
mod failover;

#[path = "tuic_service/family_thin.rs"]
mod family_thin;

//! Public pool matrix owns only the application/true QUIC peer fixture. The
//! original service owns all session/device lifecycle; no private Node seam or
//! test clock is used. Each generation keeps earlier TCP streams active while
//! opening each stale auxiliary slot, then retires through the public Control.
use super::*;
#[path = "observe_window.rs"]
mod observe_window;
use smoltcp::phy::{Device, TxToken};
use std::net::Ipv4Addr;

pub(super) async fn drive<S: TunService, F: Future>(service: &mut S, work: F) -> F::Output {
    observe_window::window(Duration::from_secs(5), async {
        tokio::select! {
            value = work => value,
            report = service.run() => panic!("public matrix ended early {report:?}"),
        }
    })
    .await
    .expect("public matrix observation expired")
}

pub(super) async fn authenticate(peer: &quinn::Connection, cfg: &TuicClientConfig) {
    let mut auth = peer.accept_uni().await.unwrap();
    let actual = auth.read_to_end(64).await.unwrap();
    let mut token = [0; 32];
    peer.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes())
        .unwrap();
    assert_eq!(actual, encode_authenticate(&cfg.uuid, &token));
}

pub(super) async fn dns(device: &mut LoopbackTunDevice, name: &str) -> Ipv4Addr {
    let mut query = vec![0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    for label in name.split('.') {
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.extend_from_slice(&[0, 0, 1, 0, 1]);
    let mut packet = Vec::new();
    etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [8, 8, 8, 8], 64)
        .udp(50000, 53)
        .write(&mut packet, &query)
        .unwrap();
    device
        .transmit(smoltcp::time::Instant::now())
        .unwrap()
        .consume(packet.len(), |b| b.copy_from_slice(&packet));
    device.flush_tx().await.unwrap();
    loop {
        if device.try_recv_rx().unwrap() {
            let reply = device.rx_take().unwrap();
            let headers = etherparse::PacketHeaders::from_ip_slice(&reply).unwrap();
            assert!(
                matches!(headers.transport, Some(etherparse::TransportHeader::Udp(ref udp)) if udp.source_port==53 && udp.destination_port==50000)
            );
            let payload = headers.payload;
            assert!(payload.len() >= 12);
            assert_eq!(&payload[..2], &[0, 1]);
            return Ipv4Addr::new(
                payload[payload.len() - 4],
                payload[payload.len() - 3],
                payload[payload.len() - 2],
                payload[payload.len() - 1],
            );
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

impl Apps {
    pub(super) fn dial(&mut self, source: u16) {
        let tcp = self.sockets.get_mut::<tcp::Socket>(self.tcp);
        tcp.abort();
        tcp.connect(
            self.iface.context(),
            (Ipv4Address::from(self.target), 10000),
            source,
        )
        .unwrap();
        self.sent_tcp = false;
        self.sent_udp = false;
        self.tcp_reply.clear();
        self.udp_reply.clear();
    }

    pub(super) fn auxiliary(&mut self, source: u16) -> SocketHandle {
        let mut socket = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; 4096]),
            tcp::SocketBuffer::new(vec![0; 4096]),
        );
        socket
            .connect(
                self.iface.context(),
                (Ipv4Address::from(self.target), 10000),
                source,
            )
            .unwrap();
        self.sockets.add(socket)
    }

    pub(super) async fn auxiliary_turn(
        &mut self,
        handle: SocketHandle,
        sent: &mut bool,
        reply: &mut Vec<u8>,
    ) {
        self.turn().await;
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        if socket.can_send() && !*sent {
            socket.send_slice(b"aux-tcp").unwrap();
            *sent = true;
        }
        while socket.can_recv() {
            let mut buffer = [0; 128];
            let n = socket.recv_slice(&mut buffer).unwrap();
            assert!(reply.len() + n <= 128);
            reply.extend_from_slice(&buffer[..n]);
        }
    }
}

fn target(port: u16) -> TargetAddr {
    TargetAddr::DomainPort {
        host: "persist.example".into(),
        port,
    }
}

pub(super) async fn tcp_exchange(
    peer: &quinn::Connection,
    payload: &[u8],
    reply: &[u8],
) -> (quinn::SendStream, quinn::RecvStream) {
    let (mut tx, mut rx) = peer.accept_bi().await.unwrap();
    let mut expected = encode_connect(&target(10000));
    expected.extend_from_slice(payload);
    let mut actual = vec![0; expected.len()];
    rx.read_exact(&mut actual).await.unwrap();
    assert_eq!(actual, expected);
    tx.write_all(reply).await.unwrap();
    // Keep this real business stream active until the service retires it.
    (tx, rx)
}

async fn udp_exchange(peer: &quinn::Connection, ip: Ipv4Addr, reply: &[u8]) -> u16 {
    let packet = peer.read_datagram().await.unwrap();
    assert!(packet.len() >= 4);
    let assoc = u16::from_be_bytes([packet[2], packet[3]]);
    assert_eq!(
        &packet[..],
        encode_packet(assoc, &target(10001), b"service-udp")
    );
    peer.send_datagram(encode_packet(assoc, &TargetAddr::IpPort((ip, 10001).into()), reply).into())
        .unwrap();
    assoc
}

pub(super) async fn retire_peers(streams: &mut Vec<(quinn::SendStream, quinn::RecvStream)>) {
    for (tx, rx) in streams.iter_mut() {
        let code = tx
            .stopped()
            .await
            .unwrap()
            .expect("actual original STOP_SENDING");
        // The service still needs this peer's final offset, so it cannot close
        // the original Node before we observe the original writer's actual EOF.
        assert!(rx.read_to_end(0).await.unwrap().is_empty());
        tx.reset(code).unwrap();
        println!(
            "peer stream={} actual STOP/EOF then RESET final offset",
            tx.id()
        );
    }
    streams.clear();
}

#[derive(Clone, Copy)]
enum Family {
    Legacy,
    Thin,
    Continuous,
    Permit,
    Ordered,
}

async fn matrix(pool: usize, family: Family) {
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
    cfg.tcp_pool = pool;
    let (device, app_device) = LoopbackTunDevice::bounded_pair(64, 4);
    let mut runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap();
    if matches!(
        family,
        Family::Thin | Family::Continuous | Family::Permit | Family::Ordered
    ) {
        use mini_vpn::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TcpRelayEngine};
        let thin = matches!(family, Family::Thin);
        let permit = matches!(family, Family::Permit);
        let ordered = matches!(family, Family::Ordered);
        let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            thin: thin.then_some("1"),
            continuous: (!thin && !permit && !ordered).then_some("1"),
            permit: permit.then_some("1"),
            ordered_egress_permit: ordered.then_some("1"),
            ..Default::default()
        }));
        assert_eq!(
            policy.generic_engine(),
            if ordered {
                TcpRelayEngine::OrderedEgressPermitPump
            } else if permit {
                TcpRelayEngine::PermitPump
            } else if thin {
                TcpRelayEngine::ThinStaging
            } else {
                TcpRelayEngine::ContinuousPump
            }
        );
        cfg = cfg.with_relay_policy(policy.clone());
        runtime = runtime.with_relay_policy(policy.clone());
        assert!(
            Arc::ptr_eq(&policy, cfg.relay_policy())
                && Arc::ptr_eq(&policy, runtime.relay_policy())
        );
    }
    let mut service = prepare_tuic_service(
        device,
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let control = service.take_handle().unwrap();
    assert!(service.take_handle().is_none());
    let mut app_device = Some(app_device);
    let mut apps: Option<Apps> = None;
    let mut peers: Vec<quinn::Connection> = Vec::with_capacity(2 * pool);
    let mut streams = Vec::with_capacity(pool);
    let mut auxiliary = Vec::with_capacity(pool - 1);
    let mut heartbeats = [0; 2];
    let mut tcp_rounds = [0; 2];
    let mut udp_rounds = [0; 2];
    let mut fake = None;
    let scenario = capture(async {
        for generation in 0..2 {
            for slot in 0..pool {
                drive(&mut service, async {
                    if generation == 1 && slot == 0 {
                        retire_peers(&mut streams).await;
                    }
                    let incoming = server.accept().await.unwrap();
                    if generation == 1 && slot == 0 {
                        // The actual old endpoint must have closed every old
                        // connection before any successor handshake is accepted.
                        for peer in &peers[..pool] {
                            tokio::time::timeout(Duration::from_secs(2), peer.closed())
                                .await
                                .unwrap();
                        }
                        let apps = apps.as_mut().unwrap();
                        for handle in auxiliary.drain(..) {
                            apps.sockets.remove(handle);
                        }
                        apps.dial(42000);
                    }
                    let peer = incoming.await.unwrap();
                    peers.push(peer);
                    authenticate(peers.last().unwrap(), &cfg).await;
                }).await;
            }
            let current = &peers[generation * pool..(generation + 1) * pool];
            assert!(current.iter().all(|peer| peer.close_reason().is_none()));
            if generation == 0 {
                let device = app_device.as_mut().unwrap();
                let first = drive(&mut service, dns(device, "first.example")).await;
                let persistent = drive(&mut service, dns(device, "persist.example")).await;
                assert_ne!(first, persistent);
                fake = Some(persistent);
                let mut value = Apps::new(app_device.take().unwrap());
                value.target = persistent;
                value.dial(40000);
                apps = Some(value);
            }
            let apps = apps.as_mut().unwrap();
            let ip = fake.unwrap();
            let (stream, assoc) = drive(&mut service, async {
                let remote = async {
                    tokio::join!(
                        tcp_exchange(&current[0], b"service-tcp", b"tcp-reply"),
                        udp_exchange(&current[0], ip, b"udp-reply")
                    )
                };
                let application = async {
                    while apps.tcp_reply != b"tcp-reply" || apps.udp_reply != b"udp-reply" {
                        apps.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                };
                let (result, ()) = tokio::join!(remote, application);
                result
            }).await;
            streams.push(stream);
            tcp_rounds[generation] += 1;
            udp_rounds[generation] += 1;
            // Original elapsed-seconds idle condition (10s), real time and same
            // live primary TCP. No pause/advance/production timeout change.
            tokio::select! {
                _ = async {
                    let until = tokio::time::Instant::now() + Duration::from_secs(10);
                    while tokio::time::Instant::now() < until {
                        apps.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                } => {},
                report = service.run() => panic!("idle primary ended {report:?}"),
            }
            for slot in 1..pool {
                let handle = apps.auxiliary(41000 + (generation * pool + slot) as u16);
                auxiliary.push(handle);
                let mut sent = false;
                let mut reply = Vec::with_capacity(128);
                let stream = drive(&mut service, async {
                    let remote = async {
                        let heartbeat = current[slot].read_datagram().await.unwrap();
                        assert_eq!(&heartbeat[..], mini_vpn::tuic::encode_heartbeat());
                        heartbeats[generation] += 1;
                        current[slot].send_datagram(bytes::Bytes::from_static(b"alive")).unwrap();
                        tcp_exchange(&current[slot], b"aux-tcp", b"aux-reply").await
                    };
                    let application = async {
                        while reply != b"aux-reply" {
                            apps.auxiliary_turn(handle, &mut sent, &mut reply).await;
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    };
                    let (stream, ()) = tokio::join!(remote, application);
                    stream
                }).await;
                assert!(current[slot].close_reason().is_none());
                streams.push(stream);
                tcp_rounds[generation] += 1;
            }
            apps.sent_udp = false;
            apps.udp_reply.clear();
            let repeated_assoc = drive(&mut service, async {
                let application = async {
                    while apps.udp_reply != b"udp-after-probe" {
                        apps.turn().await;
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                };
                let (assoc, ()) = tokio::join!(udp_exchange(&current[0], ip, b"udp-after-probe"), application);
                assoc
            }).await;
            assert_eq!(assoc, repeated_assoc, "same original UDP flow within generation");
            udp_rounds[generation] += 1;
            assert_eq!(control.phase(), TuicServicePhase::Running);
            assert!(current.iter().all(|peer| peer.close_reason().is_none()));
            assert_eq!(control.request_rebuild(), generation == 0);
            assert!(!control.request_rebuild(), "no concurrent/third rebuild");
            println!("pool={pool} generation={generation} successful_aux_probes={} tcp={} udp={} fake={ip}", heartbeats[generation], tcp_rounds[generation], udp_rounds[generation]);
        }
        assert_eq!(heartbeats, [pool - 1; 2]);
        assert_eq!(tcp_rounds, [pool; 2]);
        assert_eq!(udp_rounds, [2; 2]);
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err(), "no replacement/redial/third Node");
    }).await;
    control.request_stop();
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), async {
            let (report, ()) = tokio::join!(service.run(), retire_peers(&mut streams));
            report
        })
        .await
        .unwrap();
        println!("pool={pool} final={report:?} phase={:?}", control.phase());
        assert!(report.device_settled());
        assert!(!report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
        }
        assert_eq!(control.observe_shutdown().await, TuicServicePhase::Complete);
    })
    .await;
    drop(streams);
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn public_pool_two_probe_then_unique_successor_tcp_udp() {
    matrix(2, Family::Legacy).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_pool_three_probes_then_unique_successor_tcp_udp() {
    matrix(3, Family::Legacy).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_continuous_pool_two_probe_then_unique_successor_tcp_udp() {
    matrix(2, Family::Continuous).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_continuous_pool_three_probes_then_unique_successor_tcp_udp() {
    matrix(3, Family::Continuous).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_permit_pool_two_probe_then_unique_successor_tcp_udp() {
    matrix(2, Family::Permit).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_permit_pool_three_probes_then_unique_successor_tcp_udp() {
    matrix(3, Family::Permit).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_ordered_pool_two_probe_then_unique_successor_tcp_udp() {
    matrix(2, Family::Ordered).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_ordered_pool_three_probes_then_unique_successor_tcp_udp() {
    matrix(3, Family::Ordered).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_thin_pool_two_probe_then_unique_successor_tcp_udp() {
    matrix(2, Family::Thin).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_thin_pool_three_probes_then_unique_successor_tcp_udp() {
    matrix(3, Family::Thin).await;
}

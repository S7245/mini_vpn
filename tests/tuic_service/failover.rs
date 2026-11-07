//! External Failover API composes the same CLI lifecycle. These real peers own
//! only their original streams; no private Node/health/clock receipt is injected.
use super::*;
use mini_vpn::client_tun::prepare_failover_service;
use mini_vpn::reality_upstream::{RealityClientConfig, RealityUpstream};
use pool::{authenticate, dns, drive, retire_peers, tcp_exchange};
use std::net::Ipv4Addr;

async fn udp_reply(peer: &quinn::Connection, ip: Ipv4Addr, reply: &[u8]) -> u16 {
    let limit = tokio::time::Instant::now() + Duration::from_secs(2);
    let packet = {
        let mut skipped = 0;
        loop {
            let packet = tokio::time::timeout_at(limit, peer.read_datagram())
                .await
                .unwrap()
                .unwrap();
            if &packet[..] == mini_vpn::tuic::encode_heartbeat() {
                assert!(
                    skipped < 4,
                    "only four exact control heartbeats may precede DATA"
                );
                skipped += 1;
                continue;
            }
            break packet;
        }
    };
    assert!(packet.len() >= 4);
    let assoc = u16::from_be_bytes([packet[2], packet[3]]);
    let target = TargetAddr::DomainPort {
        host: "persist.example".into(),
        port: 10001,
    };
    assert_eq!(&packet[..], encode_packet(assoc, &target, b"service-udp"));
    peer.send_datagram(encode_packet(assoc, &TargetAddr::IpPort((ip, 10001).into()), reply).into())
        .unwrap();
    assoc
}

struct Generation {
    peers: [quinn::Connection; 2],
    streams: Vec<(quinn::SendStream, quinn::RecvStream)>,
    retirement_started: bool,
    auxiliary: Option<SocketHandle>,
}
impl Generation {
    async fn retire(&mut self) {
        assert!(
            !self.retirement_started,
            "never retry a partially reset original peer"
        );
        self.retirement_started = true;
        retire_peers(&mut self.streams).await;
    }
}

async fn accept_generation<S: TunService>(
    service: &mut S,
    server: &quinn::Endpoint,
    cfg: &TuicClientConfig,
    retired: &[Generation],
    all_peers: &mut Vec<quinn::Connection>,
) -> Generation {
    let mut peers = Vec::with_capacity(2);
    for slot in 0..2 {
        drive(service, async {
            let incoming = server.accept().await.unwrap();
            if slot == 0 {
                for old in retired {
                    assert!(old.retirement_started && old.streams.is_empty());
                    for peer in &old.peers {
                        tokio::time::timeout(Duration::from_secs(2), peer.closed())
                            .await
                            .unwrap();
                    }
                }
            }
            let peer = incoming.await.unwrap();
            assert!(all_peers.len() < 4);
            all_peers.push(peer.clone());
            authenticate(&peer, cfg).await;
            peers.push(peer);
        })
        .await;
    }
    Generation {
        peers: [peers.remove(0), peers.remove(0)],
        streams: Vec::with_capacity(2),
        retirement_started: false,
        auxiliary: None,
    }
}

async fn traffic<S: TunService>(
    service: &mut S,
    apps: &mut Apps,
    generation: &mut Generation,
    ip: Ipv4Addr,
    source: u16,
) {
    let primary = &generation.peers[0];
    let streams = &mut generation.streams;
    let assoc = drive(service, async {
        let remote = async {
            let tcp = async {
                streams.push(tcp_exchange(primary, b"service-tcp", b"tcp-reply").await);
            };
            tokio::join!(tcp, udp_reply(primary, ip, b"udp-reply")).1
        };
        let local = async {
            while apps.tcp_reply != b"tcp-reply" || apps.udp_reply != b"udp-reply" {
                apps.turn().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let (result, ()) = tokio::join!(remote, local);
        result
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(5), service.run())
            .await
            .is_err()
    );
    tokio::select! {
        () = async {
            let limit = tokio::time::Instant::now() + Duration::from_secs(10);
            while tokio::time::Instant::now() < limit {
                apps.turn().await; tokio::time::sleep(Duration::from_millis(1)).await;
            }
        } => (),
        report = service.run() => panic!("primary ended during original aux idle threshold {report:?}"),
    }
    let handle = apps.auxiliary(source);
    generation.auxiliary = Some(handle);
    let mut sent = false;
    let mut reply = Vec::with_capacity(128);
    drive(service, async {
        let remote = async {
            let heartbeat = generation.peers[1].read_datagram().await.unwrap();
            assert_eq!(&heartbeat[..], mini_vpn::tuic::encode_heartbeat());
            generation.peers[1]
                .send_datagram(bytes::Bytes::from_static(b"alive"))
                .unwrap();
            generation
                .streams
                .push(tcp_exchange(&generation.peers[1], b"aux-tcp", b"aux-reply").await);
        };
        let local = async {
            while reply != b"aux-reply" {
                apps.auxiliary_turn(handle, &mut sent, &mut reply).await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let (stream, ()) = tokio::join!(remote, local);
        stream
    })
    .await;
    // Useful traffic on the SAME primary stream and UDP socket after the actual
    // auxiliary Probe, while the auxiliary business stream remains live.
    apps.sockets
        .get_mut::<tcp::Socket>(apps.tcp)
        .send_slice(b"primary-after-probe")
        .unwrap();
    apps.sent_udp = false;
    apps.udp_reply.clear();
    let repeated = drive(service, async {
        let remote = async {
            let (tx, rx) = &mut generation.streams[0];
            let mut actual = [0; 19];
            rx.read_exact(&mut actual).await.unwrap();
            assert_eq!(&actual, b"primary-after-probe");
            tx.write_all(b"primary-reply").await.unwrap();
            udp_reply(&generation.peers[0], ip, b"udp-after-probe").await
        };
        let local = async {
            while apps.tcp_reply != b"tcp-replyprimary-reply"
                || apps.udp_reply != b"udp-after-probe"
            {
                apps.turn().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let (assoc, ()) = tokio::join!(remote, local);
        assoc
    })
    .await;
    assert_eq!(assoc, repeated, "same original UDP flow within generation");
    assert_eq!(generation.streams.len(), 2);
    assert!(generation.peers.iter().all(|p| p.close_reason().is_none()));
}

#[tokio::test(flavor = "current_thread")]
async fn public_failover_pool_two_probe_then_unique_successor_tcp_udp() {
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
    cfg.tcp_pool = 2;
    let backup = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (_, pk) = mini_vpn::reality::auth::generate_ephemeral_keypair();
    let reality = RealityUpstream::new(RealityClientConfig {
        server: backup.local_addr().unwrap().to_string(),
        uuid: [0x42; 16],
        pbk: pk,
        short_id: [0; 8],
        sni: "example.com".into(),
    });
    let (device, app_device) = LoopbackTunDevice::bounded_pair(64, 4);
    let mut service = prepare_failover_service(
        device,
        cfg.clone(),
        reality,
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let control = service.take_handle().unwrap();
    assert!(service.take_handle().is_none());
    let mut app_device = Some(app_device);
    let mut apps: Option<Apps> = None;
    let mut generations: Vec<Generation> = Vec::with_capacity(2);
    let mut all_peers = Vec::with_capacity(4);
    let mut fake = None;
    let scenario = capture(async {
        for round in 0..2 {
            let generation = accept_generation(&mut service, &server, &cfg, &generations, &mut all_peers).await;
            generations.push(generation);
            if round == 0 {
                let device = app_device.as_mut().unwrap();
                let first = drive(&mut service, dns(device, "first.example")).await;
                let persistent = drive(&mut service, dns(device, "persist.example")).await;
                assert_ne!(first, persistent); fake = Some(persistent);
                let mut value = Apps::new(app_device.take().unwrap()); value.target = persistent; value.dial(40000);
                apps = Some(value);
            } else {
                let apps = apps.as_mut().unwrap();
                for old in &mut generations[..round] {
                    if let Some(handle) = old.auxiliary.take() { apps.sockets.remove(handle); }
                }
                apps.dial(42000);
            }
            traffic(&mut service, apps.as_mut().unwrap(), &mut generations[round], fake.unwrap(), 41000 + round as u16).await;
            assert_eq!(control.phase(), TuicServicePhase::Running);
            assert_eq!(control.request_rebuild(), round == 0);
            assert!(!control.request_rebuild());
            if round == 0 { drive(&mut service, generations[0].retire()).await; }
            println!("externalFailover pool2 generation={round} primary+aux actualProbe TCP/sameUDP fake={fake:?}");
        }
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
    }).await;
    control.request_stop();
    // A failed retirement borrow may have returned only some final offsets.
    // Dispose the remaining original halves once before driving actual cleanup;
    // the original scenario error still owns the verdict, never an EOF claim.
    for generation in &mut generations {
        if generation.retirement_started && !generation.streams.is_empty() {
            eprintln!(
                "partial original peer retirement: remaining halves disposed, STOP/EOF proof incomplete"
            );
            generation.streams.clear();
        }
    }
    let cleanup = capture(async {
        let report = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(service.run(), async {
                for generation in &mut generations {
                    if !generation.retirement_started {
                        generation.retire().await;
                    }
                }
            })
            .0
        })
        .await
        .unwrap();
        println!(
            "externalFailover actualfinal={report:?} phase={:?}",
            control.phase()
        );
        assert!(report.device_settled() && !report.device_failed());
        assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
        }
        assert_eq!(control.observe_shutdown().await, TuicServicePhase::Complete);
    })
    .await;
    if cleanup.is_err() {
        eprintln!("externalFailover secondary cleanup failure; scenario error retains priority");
    }
    drop(generations);
    for peer in all_peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
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

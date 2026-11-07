//! Real public Thin path: original peer halves are retained before fallible
//! payload reads. Refusal is a RED, never acceptance of this required family.
use super::*;
use mini_vpn::upstream::{RelayFamilyPolicy, RelayFamilyPolicySources, TcpRelayEngine};
use pool::{authenticate, dns, drive, retire_peers};

async fn udp(peer: &quinn::Connection, ip: std::net::Ipv4Addr) {
    let until = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut skipped = 0;
    let packet = loop {
        let value = tokio::time::timeout_at(until, peer.read_datagram())
            .await
            .unwrap()
            .unwrap();
        if &value[..] == mini_vpn::tuic::encode_heartbeat() {
            assert!(skipped < 4);
            skipped += 1;
            continue;
        }
        break value;
    };
    assert!(packet.len() >= 4);
    let assoc = u16::from_be_bytes([packet[2], packet[3]]);
    let target = TargetAddr::DomainPort {
        host: "persist.example".into(),
        port: 10001,
    };
    assert_eq!(&packet[..], encode_packet(assoc, &target, b"service-udp"));
    peer.send_datagram(
        encode_packet(assoc, &TargetAddr::IpPort((ip, 10001).into()), b"thin-udp").into(),
    )
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn public_thin_tcp_udp_survive_clean_unique_successor() {
    exercise(Family::Thin).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_continuous_tcp_udp_survive_clean_unique_successor() {
    exercise(Family::Continuous).await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_permit_tcp_udp_survive_clean_unique_successor() {
    exercise(Family::Permit).await;
}

#[derive(Clone, Copy, Debug)]
enum Family {
    Thin,
    Continuous,
    Permit,
    Ordered,
}

async fn exercise(family: Family) {
    let mut server = server();
    let (sources, engine) = match family {
        Family::Thin => (
            RelayFamilyPolicySources {
                thin: Some("1"),
                ..Default::default()
            },
            TcpRelayEngine::ThinStaging,
        ),
        Family::Continuous => (
            RelayFamilyPolicySources {
                continuous: Some("1"),
                ..Default::default()
            },
            TcpRelayEngine::ContinuousPump,
        ),
        Family::Permit => (
            RelayFamilyPolicySources {
                permit: Some("1"),
                ..Default::default()
            },
            TcpRelayEngine::PermitPump,
        ),
        Family::Ordered => (
            RelayFamilyPolicySources {
                ordered_egress_permit: Some("1"),
                ..Default::default()
            },
            TcpRelayEngine::OrderedEgressPermitPump,
        ),
    };
    let policy = Arc::new(RelayFamilyPolicy::from_sources(sources));
    assert_eq!(policy.generic_engine(), engine);
    let mut cfg = TuicClientConfig::from_sources(
        Some(&server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap()
    .with_relay_policy(policy.clone());
    cfg.tcp_pool = 1;
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy.clone());
    assert!(
        Arc::ptr_eq(&policy, cfg.relay_policy()) && Arc::ptr_eq(&policy, runtime.relay_policy())
    );
    let counters = Arc::new(Metrics::default());
    let (device, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let mut service =
        prepare_tuic_service(device, cfg.clone(), runtime, counters.clone(), NoopSink);
    let handle = service.take_handle().unwrap();
    let mut peers: Vec<quinn::Connection> = Vec::with_capacity(2);
    let mut streams: Vec<(quinn::SendStream, quinn::RecvStream)> = Vec::with_capacity(1);
    let mut retirement_started = false;
    let mut application = Some(application);
    let mut apps: Option<Apps> = None;
    let mut fake = None;
    let scenario = capture(async {
        for round in 0..2 {
            drive(&mut service, async {
                let incoming = server.accept().await.unwrap();
                if round == 1 {
                    assert!(retirement_started && streams.is_empty());
                    tokio::time::timeout(Duration::from_secs(2), peers[0].closed()).await.unwrap();
                }
                let peer = incoming.await.unwrap(); assert!(peers.len() < 2); peers.push(peer);
                authenticate(&peers[round], &cfg).await;
            }).await;
            if round == 0 {
                let device = application.as_mut().unwrap();
                let first = drive(&mut service, dns(device, "first.example")).await;
                let persistent = drive(&mut service, dns(device, "persist.example")).await;
                assert_ne!(first, persistent); fake = Some(persistent);
                let mut value = Apps::new(application.take().unwrap()); value.target = persistent; value.dial(40000);
                apps = Some(value);
            } else {
                apps.as_mut().unwrap().dial(40002);
            }
            retirement_started = false;
            let apps = apps.as_mut().unwrap();
            drive(&mut service, async {
                let tcp = async {
                    let pair = peers[round].accept_bi().await.unwrap();
                    assert!(streams.is_empty()); streams.push(pair); // BEFORE any fallible read.
                    let (tx, rx) = &mut streams[0];
                    let mut expected = encode_connect(&TargetAddr::DomainPort { host: "persist.example".into(), port: 10000 });
                    expected.extend_from_slice(b"service-tcp");
                    let mut actual = vec![0; expected.len()];
                    let received = rx.read_exact(&mut actual).await;
                    eprintln!("publicThin round={round} actualCONNECTpayload read={received:?} actual={actual:?} metrics={:?}", counters.snapshot(0, 0));
                    received.unwrap(); assert_eq!(actual, expected);
                    tx.write_all(b"thin-reply").await.unwrap();
                };
                let remote_udp = udp(&peers[round], fake.unwrap());
                let local = async {
                    while apps.tcp_reply != b"thin-reply" || apps.udp_reply != b"thin-udp" {
                        apps.turn().await; tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                };
                tokio::join!(tcp, remote_udp, local);
            }).await;
            assert_eq!(handle.phase(), TuicServicePhase::Running);
            assert_eq!(counters.snapshot(0, 0).relays_spawned, (round + 1) as u64);
            assert_eq!(handle.request_rebuild(), round == 0); assert!(!handle.request_rebuild());
            if round == 0 {
                retirement_started = true;
                drive(&mut service, retire_peers(&mut streams)).await;
            }
            println!("publicThin actualgeneration={round} TCP+sameUDP/nonfirstfake={fake:?}");
        }
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
    }).await;
    handle.request_stop();
    if scenario.is_err() {
        // Dispose only remaining original peer halves, then measure real cleanup.
        // This failed path supplies no normal STOP/EOF or family-success proof.
        eprintln!(
            "publicThin originalscenariofailed: dispose remaining {} originalhalves pairs; retirement_started={retirement_started}, no STOP/EOF claim",
            streams.len()
        );
        streams.clear();
    }
    let mut actual_report = None;
    let cleanup = capture(async {
        let (report, ()) = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(service.run(), async {
                if !retirement_started && !streams.is_empty() {
                    retirement_started = true;
                    retire_peers(&mut streams).await;
                }
            })
        })
        .await
        .unwrap();
        actual_report = Some(report);
        println!(
            "publicThin actualcleanup={report:?} phase={:?} metrics={:?}",
            handle.phase(),
            counters.snapshot(0, 0)
        );
        if scenario.is_ok() {
            assert!(report.device_settled() && !report.device_failed());
            assert_eq!(report.node, Some(NodeShutdown::RunningResourcesSettled));
            assert_eq!(report.rebuilds, 1);
            assert_eq!(handle.observe_shutdown().await, TuicServicePhase::Complete);
        }
    })
    .await;
    if cleanup.is_err() {
        eprintln!(
            "publicThin secondarycleanupfailure actual_report={actual_report:?}; originalscenario retainspriority"
        );
    }
    drop(streams);
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    drop(service);
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    if matches!(
        family,
        Family::Continuous | Family::Permit | Family::Ordered
    ) {
        eprintln!(
            "public{family:?} actual returned cleanup report={actual_report:?} serverdrain={drained:?}; originalscenario retainspriority, later drain assertion not yet executed"
        );
    }
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn public_ordered_egress_tcp_udp_survive_clean_unique_successor() {
    exercise(Family::Ordered).await;
}

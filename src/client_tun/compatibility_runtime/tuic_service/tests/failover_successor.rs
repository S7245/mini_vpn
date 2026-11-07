//! Two real successor dispositions at the existing Failover root. Owns only
//! peers/apps and fault observations; the original driver owns settlement.
//! REALITY leg selection is explicit, not evidence of natural hysteresis.
use super::*;
use crate::reality_upstream::{RealityClientConfig, RealityUpstream};
use device::{Gate, GatedDevice};
use fixture::{auth, capture, config, Apps};
use std::sync::atomic::Ordering;

pub(super) type Streams = Option<(quinn::SendStream, quinn::RecvStream)>;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Reality,
    Alpn,
}

fn parent_deadline(control: &Control) -> Option<tokio::time::Instant> {
    *control
        .deadline
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
fn owner_deadline(control: &SessionControl) -> Option<tokio::time::Instant> {
    control.ledger().shutdown_deadline
}

pub(super) async fn drive<F: Future<Output = Completion>, T: Future>(
    service: &mut Service<F>,
    work: T,
) -> T::Output {
    crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
        tokio::select! {
            value = work => value,
            report = service.run() => panic!("successor matrix ended early {report:?}"),
        }
    })
    .await
    .unwrap()
}

fn dial(apps: &mut Apps, port: u16) {
    let socket = apps
        .sockets
        .get_mut::<smoltcp::socket::tcp::Socket>(apps.tcp);
    socket.abort();
    socket
        .connect(
            apps.iface.context(),
            (smoltcp::wire::Ipv4Address::from(apps.target), 10000),
            port,
        )
        .unwrap();
    apps.sent_tcp = false;
    apps.sent_udp = false;
    apps.tcp_reply.clear();
    apps.udp_reply.clear();
}

async fn udp(peer: &quinn::Connection) -> u16 {
    let limit = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut skipped = 0;
    let packet = loop {
        let value = tokio::time::timeout_at(limit, peer.read_datagram())
            .await
            .unwrap()
            .unwrap();
        if &value[..] == crate::tuic::encode_heartbeat() {
            assert!(skipped < 4);
            skipped += 1;
            continue;
        }
        break value;
    };
    assert!(packet.len() >= 4);
    let assoc = u16::from_be_bytes([packet[2], packet[3]]);
    let target = TargetAddr::IpPort("192.0.2.10:10001".parse().unwrap());
    assert_eq!(&packet[..], encode_packet(assoc, &target, b"service-udp"));
    peer.send_datagram(encode_packet(assoc, &target, b"udp-reply").into())
        .unwrap();
    assoc
}

pub(super) async fn traffic<F: Future<Output = Completion>>(
    service: &mut Service<F>,
    apps: &mut Apps,
    peer: &quinn::Connection,
    streams: &mut Streams,
    reality: Option<(&tokio::net::TcpListener, [u8; 32])>,
) {
    let reply: &[u8] = if reality.is_some() {
        b"pong"
    } else {
        b"tcp-reply"
    };
    drive(service, async {
        let remote = async {
            let tcp = async {
                if let Some((listener, sk)) = reality {
                    let (io, _) = listener.accept().await.unwrap();
                    // Complete real handshake/VLESS/ping/pong, then natural
                    // server socket Drop. No held REALITY carrier claim.
                    crate::reality_upstream::reality_server_sim(io, sk, [0x42; 16]).await;
                } else {
                    let (mut tx, mut rx) = peer.accept_bi().await.unwrap();
                    let mut want =
                        encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
                    want.extend_from_slice(b"service-tcp");
                    let mut actual = vec![0; want.len()];
                    rx.read_exact(&mut actual).await.unwrap();
                    assert_eq!(actual, want);
                    tx.write_all(b"tcp-reply").await.unwrap();
                    *streams = Some((tx, rx));
                }
            };
            tokio::join!(tcp, udp(peer));
        };
        let local = async {
            while apps.tcp_reply != reply || apps.udp_reply != b"udp-reply" {
                apps.turn().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::join!(remote, local);
    })
    .await;
}

async fn run(mode: Mode) {
    let mut server = quinn::Endpoint::server(
        UdpPair::server_config(8, 65536, 100, true),
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let cfg = config(server.local_addr().unwrap());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (sk, pk) = crate::reality::auth::generate_ephemeral_keypair();
    let configuration = Configuration::failover(
        cfg.clone(),
        RealityUpstream::new(RealityClientConfig {
            server: listener.local_addr().unwrap().to_string(),
            uuid: [0x42; 16],
            pbk: pk,
            short_id: [0; 8],
            sni: "example.com".into(),
        }),
    );
    // The SAME cold configuration is moved once into the saved production root.
    let original_state = configuration.composition.state.clone();
    let original_backup = configuration.composition.backup.clone();
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let gate = Arc::new(Gate::default());
    gate.cut_enabled.store(true, Ordering::Release);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let original_udp = apps.udp;
    let mut service = prepare_with(
        GatedDevice {
            inner: LoopbackTunDevice::new(inbound.clone(), outbound.clone()).with_ingress_pump(4),
            gate: gate.clone(),
        },
        configuration,
        TunRuntimeConfig::from_sources(Some("1")).unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let mut observer = Box::pin(handle.observe_shutdown()); // Unpolled until final stop.
    let mut peers = Vec::with_capacity(2);
    let mut streams = None;
    let mut old: Option<Arc<SessionControl>> = None;
    let mut old_health: Option<Arc<crate::failover::HealthObservation>> = None;
    let mut old_source: Option<Arc<crate::owned_upstream::generation::GenerationControl>> = None;
    let mut old_limit = None;
    let mut new_limit = None;
    let mut parent_limit = None;
    let mut current: Option<Arc<SessionControl>> = None;
    let mut original_now = 0;
    let scenario = capture(async {
        drive(&mut service, async {
            let peer = server.accept().await.unwrap().await.unwrap(); peers.push(peer);
            auth(&peers[0], &cfg).await;
        }).await;
        traffic(&mut service, &mut apps, &peers[0], &mut streams, None).await;
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        let health = saved.health_observation.lock().unwrap().as_ref().unwrap().clone();
        let source = saved.udp_generation.lock().unwrap().as_ref().unwrap().clone();
        let state = saved.health_state.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert!(Arc::ptr_eq(&state, &original_state));
        assert!(!health.joined.load(Ordering::Acquire));
        if mode == Mode::Reality {
            // Real bidirectional FIN/EOF closes the TUIC stream before opening
            // another app TCP tuple; aborting an active tuple is not this oracle.
            streams.as_mut().unwrap().0.finish().unwrap();
            apps.sockets.get_mut::<smoltcp::socket::tcp::Socket>(apps.tcp).close();
            drive(&mut service, async {
                let remote = async { assert!(streams.as_mut().unwrap().1.read_to_end(0).await.unwrap().is_empty()); };
                let local = async {
                    while owner.flow_observation().unwrap().live != 0 {
                        apps.turn().await; tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                };
                tokio::join!(remote, local);
            }).await;
            streams.take();
            state.set_leg(crate::failover::TcpLeg::Reality); // Selection injection only.
            original_now = state.now_secs();
            apps.tcp_payload = b"ping"; apps.restart();
            traffic(&mut service, &mut apps, &peers[0], &mut streams, Some((&listener, sk))).await;
        } else {
            server.set_server_config(Some(rebuild_rejection::rejected_alpn()));
        }
        assert!(Arc::strong_count(&original_backup) >= 2); // Caller + original saved composition.
        assert_eq!(apps.udp, original_udp);
        old = Some(owner.clone()); old_health = Some(health.clone()); old_source = Some(source.clone());
        assert_eq!(handle.phase(), Phase::Running);
        assert!(handle.request_rebuild()); assert!(!handle.request_rebuild());
        drive(&mut service, failover_rebuild::retire(&mut streams)).await;
        let incoming = drive(&mut service, async { server.accept().await.unwrap() }).await;
        // Before actually completing the sole successor's original Incoming.
        assert!(health.joined.load(Ordering::Acquire)); assert!(!health.quarantined.load(Ordering::Acquire));
        let generation = source.diagnostic(); assert_eq!((generation.0, generation.4), (2, 0));
        assert!(owner.business_complete.load(Ordering::Acquire)); assert!(!owner.driver_complete.load(Ordering::Acquire));
        let settled = owner.flow_observation().unwrap();
        let published = if mode == Mode::Reality { 2 } else { 1 };
        assert_eq!((settled.live, settled.running, settled.f, settled.w, settled.released, settled.joined), (0, 0, 0, 0, published, published * 3));
        assert!(settled.audit && owner.ledger().first_failure.is_none());
        old_limit = owner_deadline(&owner);
        assert!(old_limit.is_some());
        tokio::time::timeout(Duration::from_secs(2), peers[0].closed()).await.unwrap();
        assert_eq!((gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (0, 0));
        eprintln!("B25 before successor handshake oldhealth actualjoin/source={generation:?}/businesszero/oldpeerclosed D0");
        if mode == Mode::Reality {
            let peer = drive(&mut service, async { incoming.await.unwrap() }).await;
            peers.push(peer); drive(&mut service, auth(&peers[1], &cfg)).await;
            dial(&mut apps, 40004);
            traffic(&mut service, &mut apps, &peers[1], &mut streams, Some((&listener, sk))).await;
            let state = saved.health_state.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            assert!(Arc::ptr_eq(&state, &original_state));
            assert_eq!(state.active_leg(), crate::failover::TcpLeg::Reality);
            assert!(state.now_secs() >= original_now);
            assert!(Arc::strong_count(&original_backup) >= 2);
            assert_eq!(apps.udp, original_udp);
            assert_eq!((gate.cuts.load(Ordering::Acquire), gate.resumed.load(Ordering::Acquire)), (1, 1));
            assert!(parent_deadline(&saved).is_none());
        } else {
            let denied = drive(&mut service, async { incoming.await }).await;
            match denied {
                Err(quinn::ConnectionError::TransportError(error)) => {
                    assert_eq!(error.code, quinn::TransportErrorCode::crypto(120));
                    eprintln!("B25 actual successor ALPN {error:?}");
                }
                other => panic!("expected real ALPN refusal {other:?}"),
            }
            drive(&mut service, async {
                while gate.polls.load(Ordering::Acquire) == 0 { tokio::time::sleep(Duration::from_millis(1)).await; }
            }).await;
            assert!(saved.health_observation.lock().unwrap().is_none());
        }
        let owner = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
        assert!(!Arc::ptr_eq(old.as_ref().unwrap(), &owner)); current = Some(owner.clone());
        let clocks = *saved.namespace_clocks.lock().unwrap(); assert!(clocks[0].is_some()); assert_eq!(clocks[0], clocks[1]);
        if mode == Mode::Alpn {
            assert!(owner.business_complete.load(Ordering::Acquire));
            assert!(owner.ledger().first_failure.is_none());
            let empty = owner.flow_observation().unwrap();
            assert_eq!((empty.live, empty.f, empty.w, empty.published, empty.factory_started), (0, 0, 0, 0, 0)); assert!(empty.audit);
            new_limit = owner_deadline(&owner); parent_limit = parent_deadline(&saved);
            assert!(new_limit.is_some() && parent_limit.is_some());
            for _ in 0..4 {
                assert!(tokio::time::timeout(Duration::from_millis(1), observer.as_mut()).await.is_err());
                assert!(tokio::time::timeout(Duration::from_millis(1), service.run()).await.is_err());
                assert_eq!(owner_deadline(old.as_ref().unwrap()), old_limit);
                assert_eq!(owner_deadline(&owner), new_limit);
                assert_eq!(parent_deadline(&saved), parent_limit);
                assert_eq!((gate.cuts.load(Ordering::Acquire), gate.resumes.load(Ordering::Acquire), gate.joins.load(Ordering::Acquire), gate.drops.load(Ordering::Acquire)), (1, 0, 0, 0));
                assert!(service.completion.is_none());
            }
        }
        assert!(!handle.request_rebuild());
        assert!(tokio::time::timeout(Duration::from_millis(20), server.accept()).await.is_err());
    }).await;
    // A cancelled original retirement has already taken/dropped its halves.
    // Remaining unretired halves are owned here and retired at most once below.
    handle.request_stop();
    gate.open();
    let deadline = parent_deadline(&saved).unwrap();
    let mut actual_report = None;
    let cleanup = capture(async {
        let (report, (), phase) = tokio::time::timeout_at(deadline, async {
            tokio::join!(
                service.run(),
                failover_rebuild::retire(&mut streams),
                observer.as_mut()
            )
        })
        .await
        .unwrap();
        actual_report = Some(report);
        eprintln!(
            "B25 final modeReality={} {report:?} phase={phase:?} Djoin/drop={}/{}",
            mode == Mode::Reality,
            gate.joins.load(Ordering::Acquire),
            gate.drops.load(Ordering::Acquire)
        );
        assert_eq!(phase, Phase::Complete);
        assert!(report.device_settled() && !report.device_failed());
        assert_eq!(
            report.node,
            Some(if mode == Mode::Reality {
                NodeShutdown::RunningResourcesSettled
            } else {
                NodeShutdown::FailedSettled
            })
        );
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
        }
        assert_eq!(
            (
                gate.joins.load(Ordering::Acquire),
                gate.drops.load(Ordering::Acquire)
            ),
            (1, 1)
        );
        let done = service.completion.as_mut().unwrap();
        if scenario.is_ok() {
            let retired = done._retired.as_ref().unwrap();
            retired.node.assert_service_clean_for_test();
            assert!(Arc::ptr_eq(&retired.control, old.as_ref().unwrap()));
            assert!(old_health.as_ref().unwrap().joined.load(Ordering::Acquire));
            assert!(!old_health
                .as_ref()
                .unwrap()
                .quarantined
                .load(Ordering::Acquire));
            assert_eq!(old_source.as_ref().unwrap().diagnostic().0, 2);
            assert_eq!(owner_deadline(old.as_ref().unwrap()), old_limit);
            assert!(Arc::ptr_eq(&done.control, current.as_ref().unwrap()));
            if mode == Mode::Reality {
                done._node.as_ref().unwrap().assert_service_clean_for_test();
                assert!(done._health.as_ref().unwrap().failure().is_none());
                assert!(saved
                    .health_observation
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .joined
                    .load(Ordering::Acquire));
                assert_eq!(
                    original_state.active_leg(),
                    crate::failover::TcpLeg::Reality
                );
                assert!(original_state.now_secs() >= original_now);
            } else {
                assert!(done._health.is_none());
                assert!(done
                    ._node
                    .as_mut()
                    .unwrap()
                    .controlled_rebuild_disposition()
                    .configuration_rejected_for_test());
                assert!(done.control.ledger().first_failure.is_none());
                assert_eq!(owner_deadline(&done.control), new_limit);
                assert_eq!(parent_deadline(&saved), parent_limit);
            }
        }
        assert_eq!(
            (inbound.dropped_packets(), outbound.dropped_packets()),
            (0, 0)
        );
        assert!(!handle.request_rebuild());
    })
    .await;
    if cleanup.is_err() {
        eprintln!(
            "B25 secondary finally failure; actual_report={actual_report:?}; original scenario retains priority"
        );
    }
    for peer in peers {
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

#[tokio::test(flavor = "current_thread")]
async fn reality_tcp_and_tuic_udp_keep_original_state_across_unique_successor() {
    run(Mode::Reality).await;
}

#[tokio::test(flavor = "current_thread")]
async fn failover_real_alpn_refusal_after_old_health_join_never_resumes_or_retries() {
    run(Mode::Alpn).await;
}

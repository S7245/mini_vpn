//! F4096 and W128 from real TCP packets and authenticated TUIC streams.
//! One bounded peer owns actual halves; only the saved root may settle/retry.
use super::super::flow_custody::f_flow::reset::{GatedResetDevice, ResetFlushGate};
use super::f_pressure::credit_turn;
use super::*;
use crate::tuic::tcp_fixture::{UdpPair, upstream_endpoint, upstream_ready_connection};
use crate::tuic::{TuicClientConfig, TuicUpstream, encode_authenticate, encode_connect};

#[path = "compatibility_f_full/profile.rs"]
mod profile;

#[derive(Debug, Default)]
struct Sample {
    count: usize,
    total: Duration,
    max: Duration,
}
impl Sample {
    fn add(&mut self, elapsed: Duration) {
        self.count += 1;
        self.total += elapsed;
        self.max = self.max.max(elapsed);
    }
}

#[derive(Debug, Default)]
pub(super) struct FillTiming {
    app: Sample,
    driver_poll: Sample,
    driver_window: Sample,
    observation: Sample,
    scan: ScanTrace,
}

#[derive(Debug, Default)]
struct ScanTrace {
    advanced: usize,
    max_delta: usize,
    watch: Option<usize>,
    revisits: usize,
}

impl ScanTrace {
    fn note(&mut self, before: usize, after: usize) {
        let delta = (after + 4224 - before) % 4224;
        self.advanced += delta;
        self.max_delta = self.max_delta.max(delta);
        if self
            .watch
            .is_some_and(|index| (index + 4224 - before) % 4224 < delta)
        {
            self.revisits += 1;
        }
    }
}

pub(super) async fn measured_turn<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    timing: &mut FillTiming,
) {
    let started = std::time::Instant::now();
    apps.turn(true).await;
    timing.app.add(started.elapsed());
    let window = std::time::Instant::now();
    let control = session.control.clone();
    let mut run = std::pin::pin!(session.run());
    tokio::select! {
        state = std::future::poll_fn(|cx| {
            let started = std::time::Instant::now();
            let before = control.opens().cursor;
            let state = run.as_mut().poll(cx);
            let after = control.opens().cursor;
            timing.scan.note(before, after);
            timing.driver_poll.add(started.elapsed());
            state
        }) => panic!("full pressure driver ended early: {state:?}"),
        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
    }
    timing.driver_window.add(window.elapsed());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FullCase {
    AckRetry,
    WaitingDeadline,
    SynBurst,
}

enum PeerCommand {
    Health(u8, tokio::sync::oneshot::Sender<()>),
    ResetOne(tokio::sync::oneshot::Sender<()>),
    Drain,
}

async fn hold_peer(
    peer: quinn::Connection,
    mut commands: mpsc::Receiver<PeerCommand>,
    accepted: Arc<AtomicUsize>,
) -> usize {
    let header = encode_connect(&TargetAddr::IpPort("192.0.2.10:10000".parse().unwrap()));
    let mut held = Vec::with_capacity(4096);
    let mut reset = false;
    let mut healthy = None;
    loop {
        tokio::select! {
            stream = peer.accept_bi() => {
                let Ok((send, mut recv)) = stream else { return 0; };
                let mut observed = vec![0; header.len() + 1];
                recv.read_exact(&mut observed).await.unwrap();
                assert_eq!(&observed[..header.len()], &header);
                let count = accepted.load(Ordering::Acquire);
                assert_eq!(recv.id().index(), count as u64, "actual stream IDs must be contiguous");
                assert!(count < 4097 && held.len() < 4096);
                if count == 4096 {
                    assert!(reset);
                    assert!((80..208).contains(&observed[header.len()]),
                        "only a prior Waiting payload may follow the physical ACK");
                }
                if count < 128 && observed[header.len()] == 0 {
                    assert!(healthy.replace(held.len()).is_none());
                }
                held.push((send, recv));
                accepted.store(count + 1, Ordering::Release);
            }
            command = commands.recv() => match command {
                Some(PeerCommand::Health(marker, done)) => {
                    assert!(!reset);
                    let (send, recv) = &mut held[healthy.expect("app0 from first real payload wave")];
                    let mut uplink = [255; 32];
                    recv.read_exact(&mut uplink).await.unwrap();
                    assert_eq!(uplink, [0; 32]);
                    send.write_all(&[marker; 16]).await.unwrap();
                    done.send(()).unwrap();
                }
                Some(PeerCommand::ResetOne(done)) => {
                    assert!(!reset && held.len() == 4096);
                    assert_eq!(accepted.load(Ordering::Acquire), 4096);
                    let (mut send, mut recv) = held.swap_remove(0);
                    send.reset(77u32.into()).unwrap();
                    recv.stop(78u32.into()).unwrap();
                    drop((send, recv));
                    reset = true;
                    done.send(()).unwrap();
                }
                Some(PeerCommand::Drain) => {
                    let mut retired = usize::from(reset);
                    for (send, mut recv) in held {
                        assert!(send.stopped().await.unwrap().is_some());
                        drop(send);
                        let mut byte = [0];
                        assert!(matches!(recv.read(&mut byte).await,
                            Ok(None) | Err(quinn::ReadError::Reset(_))));
                        retired += 1;
                    }
                    return retired;
                }
                None => return 0,
            }
        }
    }
}

async fn healthy_exchange<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    commands: &mpsc::Sender<PeerCommand>,
    marker: u8,
) {
    assert_eq!(apps.sockets.get::<Socket>(apps.handles[0]).recv_queue(), 0);
    apps.targets[0] += 32;
    let (done, mut completed) = tokio::sync::oneshot::channel();
    commands
        .send(PeerCommand::Health(marker, done))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            credit_turn(session, apps, true).await;
            if apps.sockets.get::<Socket>(apps.handles[0]).recv_queue() == 16 {
                break;
            }
        }
        completed.try_recv().unwrap();
        let socket = apps.sockets.get_mut::<Socket>(apps.handles[0]);
        let mut downlink = [0; 16];
        assert_eq!(socket.recv_slice(&mut downlink).unwrap(), 16);
        assert_eq!(downlink, [marker; 16]);
        assert_eq!(socket.state(), smoltcp::socket::tcp::State::Established);
    })
    .await
    .expect("actual app0 bidirectional bytes");
}

async fn full_f_case(case: FullCase) {
    let pair = UdpPair::new(8, 1_048_576).await;
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 1_048_576, 4224, true)));
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
    let connected = capture(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                TuicUpstream::connect(&config, Arc::new(Metrics::default())),
                async { pair.server.accept().await.unwrap().await.unwrap() }
            )
        })
        .await
        .unwrap()
    })
    .await;
    let (upstream, peer) = match connected {
        Ok((Ok(upstream), peer)) => (Arc::new(upstream), peer),
        failure => {
            pair.close().await;
            match failure {
                Err(panic) => std::panic::resume_unwind(panic),
                _ => panic!("actual TUIC connection setup failed"),
            }
        }
    };
    let endpoint = upstream_endpoint(&upstream);
    let inbound = PacketLink::bounded(16_384);
    let outbound = PacketLink::bounded(16_384);
    let mut apps = RealApps::unconnected(outbound.clone(), inbound.clone(), 4225);
    apps.targets.fill(0);
    let links = (inbound.clone(), outbound.clone());
    let gate = Arc::new(if case == FullCase::SynBurst {
        ResetFlushGate::profiled()
    } else {
        ResetFlushGate::default()
    });
    let (_downlink, downlink_rx) = mpsc::channel(1);
    let mut runtime = TunRuntimeConfig::from_sources(Some("2")).unwrap();
    runtime.tcp_socket_buffers.rx_bytes = 4096;
    runtime.tcp_socket_buffers.tx_bytes = 4096;
    runtime.select_f4096_for_test();
    let profile = profile::Profile::default();
    let mut session = prepare_client_tun_session(
        GatedResetDevice {
            inner: LoopbackTunDevice::new(inbound, outbound),
            gate: gate.clone(),
        },
        upstream.clone(),
        downlink_rx,
        runtime,
        Arc::new(Metrics::default()),
        profile.clone(),
    );
    let accepted = Arc::new(AtomicUsize::new(0));
    let (commands, peer_commands) = mpsc::channel(2);
    let mut server = Some(tokio::spawn(hold_peer(
        peer.clone(),
        peer_commands,
        accepted.clone(),
    )));
    let mut auth = None;
    let scenario = capture(async {
        let (initial_id, initial_budget) = {
            let connection = upstream_ready_connection(&upstream).unwrap();
            (connection.stable_id(), connection.bi_stream_open_snapshot())
        };
        assert_eq!(initial_budget, (0, 4224, false, false, false));
        tokio::time::timeout(Duration::from_secs(2), async {
            auth = Some(peer.accept_uni().await.unwrap());
            let observed = auth.as_mut().unwrap().read_to_end(64).await.unwrap();
            let mut token = [0; 32];
            peer.export_keying_material(&mut token, &config.uuid, config.password.as_bytes())
                .unwrap();
            assert_eq!(observed, encode_authenticate(&config.uuid, &token));
        })
        .await
        .unwrap();
        if case == FullCase::SynBurst {
            // All real SYNs precede the saved host's first poll; no app DATA yet.
            assert_eq!((links.0.high_water_packets(), links.1.high_water_packets()), (0, 0));
            assert_eq!((links.0.dropped_packets(), links.1.dropped_packets()), (0, 0));
            apps.connect_range(0..4225);
            apps.turn(true).await;
            assert!(apps.handles.iter().all(|h| apps.sockets.get::<Socket>(*h).state()
                == smoltcp::socket::tcp::State::SynSent));
            assert!(apps.sent.iter().all(|&n| n == 0));
            assert!((0..4225).all(|i| apps.queued(i) == 0));
            assert_eq!(links.0.high_water_packets(), 4225);
            assert_eq!(links.1.high_water_packets(), 0);
            assert_eq!((links.0.dropped_packets(), links.1.dropped_packets()), (0, 0));
            let before = session.control.flow_observation().unwrap();
            assert_eq!((before.live, before.w, before.f, before.attempts, before.factory_started, before.published), (0, 0, 0, 0, 0, 0));
            profile.boundary("before-host-poll");
            gate.ingress_boundary("before-host-poll");
            let mut timing = FillTiming::default();
            let mut progress = (0, 0);
            let started = std::time::Instant::now();
            tokio::time::timeout(Duration::from_secs(20), async {
                while apps.established() != 4225 {
                    measured_turn(&mut session, &mut apps, &mut timing).await;
                    let now = (apps.established() / 512, links.0.dropped_packets());
                    if now != progress {
                        eprintln!("SYN progress elapsed={:?} established={} highwater=({},{}) drops=({},{}) timing={timing:?}", started.elapsed(), apps.established(), links.0.high_water_packets(), links.1.high_water_packets(), links.0.dropped_packets(), links.1.dropped_packets());
                        progress = now;
                    }
                }
            }).await.expect("all4225 SYNs must establish before payload admission");
            eprintln!("SYN final elapsed={:?} highwater=({},{}) drops=({},{}) timing={timing:?}", started.elapsed(), links.0.high_water_packets(), links.1.high_water_packets(), links.0.dropped_packets(), links.1.dropped_packets());
            profile.boundary("all-app-established");
            gate.ingress_boundary("all-app-established");
            session.control.opens().pressure.request_tcp_states();
            assert!(gate.single_rx_proofs.load(Ordering::SeqCst) >= 4225, "actual reset adapter must forward single-packet coverage");
            assert!(apps.sent.iter().all(|&n| n == 0));
            assert!(apps.targets.iter().all(|&n| n == 0));
            assert_eq!((links.0.dropped_packets(), links.1.dropped_packets()), (0, 0));
            let seen = session.control.flow_observation().unwrap();
            assert_eq!((seen.live, seen.w, seen.f, seen.attempts, seen.factory_started, seen.published), (0, 0, 0, 0, 0, 0));
            assert!(seen.audit);
        }
        for end in (128..=4096).step_by(128) {
            if case != FullCase::SynBurst { apps.connect_range(end - 128..end); }
            apps.targets[end - 128..end].fill(1);
            let wave_start = tokio::time::Instant::now();
            let mut timing = FillTiming::default();
            let wave = tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    measured_turn(&mut session, &mut apps, &mut timing).await;
                    let observation = std::time::Instant::now();
                    let published = session.control.published_flow_count().unwrap();
                    timing.observation.add(observation.elapsed());
                    if published == end as u64 && accepted.load(Ordering::Acquire) == end {
                        break;
                    }
                }
            })
            .await;
            eprintln!(
                "wave {end}: elapsed={:.3} timing={timing:?}",
                wave_start.elapsed().as_secs_f64()
            );
            if case == FullCase::SynBurst && end == 128 {
                profile.boundary("first-128-payload");
                gate.ingress_boundary("first-128-payload");
                eprintln!("F host captured={:?}", session.control.opens().pressure.tcp_states());
            }
            assert!(
                wave.is_ok(),
                "wave {end}, peer={}, flow={:?}",
                accepted.load(Ordering::Acquire),
                session.control.flow_observation()
            );
            let seen = session.control.flow_observation().unwrap();
            assert_eq!(session.control.published_flow_count(), Some(seen.published));
            assert_eq!(
                (seen.live, seen.f, seen.attempts, seen.running),
                (end, end, end, end * 3)
            );
            assert_eq!(
                (seen.w, seen.waiting, seen.escrow_bytes, seen.reserved),
                (0, 0, 0, 0)
            );
            assert_eq!((seen.joined, seen.released, seen.retry), (0, 0, 0));
            assert!(seen.audit && !session.control.ledger().quarantined);
            assert_eq!(
                (links.0.dropped_packets(), links.1.dropped_packets()),
                (0, 0)
            );
            eprintln!(
                "real F fill {end}: seconds={:.3} established={} highwater={:?}",
                wave_start.elapsed().as_secs_f64(),
                apps.established(),
                (links.0.high_water_packets(), links.1.high_water_packets())
            );
        }
        let published_slots: std::collections::HashSet<_> = session.control.opens()
            .slots.iter().flatten().map(|s| (s.handle, s.epoch)).collect();
        assert_eq!(published_slots.len(), 4096);
        let admitted_after = tokio::time::Instant::now();
        if case != FullCase::SynBurst { apps.connect_range(4096..4224); }
        apps.targets[4096..4224].fill(1);
        let waiting = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                credit_turn(&mut session, &mut apps, true).await;
                if session.control.flow_observation().unwrap().waiting == 128 {
                    break;
                }
            }
        })
        .await;
        assert!(
            waiting.is_ok(),
            "actual Waiting rows: {:?}",
            session.control.flow_observation()
        );
        let frozen_waiting: Vec<_> = session.control.opens().slots.iter().flatten()
            .filter(|s| !published_slots.contains(&(s.handle, s.epoch)))
            .map(|s| (s.handle, s.epoch, s.deadline)).collect();
        assert_eq!(frozen_waiting.len(), 128);
        let frozen_at = tokio::time::Instant::now();
        assert!(frozen_waiting.iter().all(|s| s.2 >= admitted_after + Duration::from_secs(20)
            && s.2 <= frozen_at + Duration::from_secs(20)));
        if case != FullCase::SynBurst { apps.connect_range(4224..4225); }
        apps.targets[4224] = 8192;
        let pressure = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                credit_turn(&mut session, &mut apps, true).await;
                if apps.sent[4224] == 8192
                    && apps.queued(4224) == 4096
                    && apps.windows[4224] == Some(0)
                {
                    break;
                }
            }
        })
        .await;
        assert!(
            pressure.is_ok(),
            "C4225 pressure: sent={} queue={} window={:?} peer_finished={} flow={:?}",
            apps.sent[4224],
            apps.queued(4224),
            apps.windows[4224],
            server.as_ref().unwrap().is_finished(),
            session.control.flow_observation()
        );
        assert_eq!(apps.established(), 4225);
        for _ in 0..8 {
            credit_turn(&mut session, &mut apps, true).await;
            let seen = session.control.flow_observation().unwrap();
            assert_eq!(
                (seen.live, seen.f, seen.w, seen.waiting),
                (4224, 4096, 128, 128)
            );
            assert_eq!(
                (seen.attempts, seen.running, seen.escrow_bytes),
                (4096, 12288, 128)
            );
            assert_eq!(seen.reserved, 128 * crate::client_tun::MAX_UPLINK_BUFFER);
            assert_eq!(
                (seen.published, seen.released, seen.joined, seen.retry),
                (4096, 0, 0, 0)
            );
            assert_eq!((apps.queued(4224), apps.windows[4224]), (4096, Some(0)));
            assert_eq!(accepted.load(Ordering::Acquire), 4096);
            assert!(seen.audit && !session.control.ledger().quarantined);
        }
        let total_published = match case {
            FullCase::AckRetry | FullCase::SynBurst => {
                assert!(!gate.reset_seen.load(Ordering::SeqCst));
                gate.blocked.store(true, Ordering::SeqCst);
                let (reset, reset_done) = tokio::sync::oneshot::channel();
                commands.send(PeerCommand::ResetOne(reset)).await.unwrap();
                reset_done.await.unwrap();
                let blocked = tokio::time::timeout(Duration::from_secs(5), async {
                    while !gate.waiting.load(Ordering::SeqCst) {
                        credit_turn(&mut session, &mut apps, true).await;
                    }
                })
                .await;
                assert!(
                    blocked.is_ok(),
                    "actual RST flush gate: {:?}",
                    session.control.flow_observation()
                );
                assert!(gate.reset_seen.load(Ordering::SeqCst));
                for _ in 0..8 {
                    credit_turn(&mut session, &mut apps, true).await;
                    let seen = session.control.flow_observation().unwrap();
                    assert_eq!(
                        (seen.live, seen.f, seen.w, seen.waiting),
                        (4224, 4096, 128, 128)
                    );
                    assert_eq!((seen.attempts, seen.running, seen.joined), (4096, 12285, 3));
                    assert_eq!(
                        (seen.settled_providers, seen.released, seen.retry),
                        (1, 0, 0)
                    );
                    assert_eq!(seen.escrow_bytes, 128);
                    assert_eq!(accepted.load(Ordering::Acquire), 4096);
                    assert_eq!((apps.queued(4224), apps.windows[4224]), (4096, Some(0)));
                    assert!(seen.audit && !session.control.ledger().quarantined);
                }
                gate.release();
                let mut retry_timing = FillTiming::default();
                let retry_start = tokio::time::Instant::now();
                let mut cursor = session.control.opens().cursor;
                let mut scan = (0usize, 0usize, cursor, cursor);
                let (mut released_at, mut restored_at) = (None, None);
                let retried = tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        measured_turn(&mut session, &mut apps, &mut retry_timing).await;
                        let observed = std::time::Instant::now();
                        let seen = session.control.flow_observation().unwrap();
                        let next = session.control.opens().cursor;
                        retry_timing.observation.add(observed.elapsed());
                        scan.0 += 1;
                        scan.1 += usize::from(next < cursor);
                        scan.2 = scan.2.min(next);
                        scan.3 = scan.3.max(next);
                        cursor = next;
                        if seen.released == 1 {
                            released_at.get_or_insert(retry_start.elapsed());
                            if seen.f == 4096 {
                                if restored_at.is_none() {
                                    restored_at = Some(retry_start.elapsed());
                                    let rows = session.control.flow_debug();
                                    let opens = session.control.opens();
                                    retry_timing.scan.watch = opens.slots.iter().enumerate()
                                        .filter(|(_, slot)| slot.is_some())
                                        .zip(rows.iter())
                                        .find_map(|((index, _), row)| row.starts_with("stage=Opening ").then_some(index));
                                }
                            }
                        }
                        if seen.published == 4097
                            && seen.waiting == 128
                            && apps.queued(4224) == 0
                            && accepted.load(Ordering::Acquire) == 4097
                        {
                            break;
                        }
                    }
                })
                .await;
                assert!(
                    retried.is_ok(),
                    "ACK-only retry: {:?}, quarantine={}, scan(samples/wraps/min/max)={scan:?}, cursor={cursor}, budget={}, release={released_at:?}, restored={restored_at:?}, timing={retry_timing:?}, peer_closed={:?}, peer_stats={:?}, initial_id={initial_id}, current={:?}, transitional={:?}",
                    session.control.flow_observation(),
                    session.control.ledger().quarantined,
                    session.control.control_remaining.load(Ordering::Relaxed),
                    peer.close_reason(),
                    peer.stats(),
                    upstream_ready_connection(&upstream)
                        .map(|connection| (connection.stable_id(), connection.bi_stream_open_snapshot())),
                    session
                        .control
                        .flow_debug()
                        .into_iter()
                        .filter(|row| {
                            !row.starts_with("stage=Published ") && !row.starts_with("stage=Waiting ")
                        })
                        .collect::<Vec<_>>()
                );
                let seen = session.control.flow_observation().unwrap();
                assert_eq!(
                    (seen.live, seen.f, seen.w, seen.waiting),
                    (4224, 4096, 128, 128)
                );
                assert_eq!((seen.attempts, seen.running, seen.joined), (4096, 12288, 3));
                assert_eq!((seen.published, seen.released, seen.retry), (4097, 1, 0));
                assert_eq!(seen.escrow_bytes, 127 + 4096);
                assert_eq!(seen.reserved, 128 * crate::client_tun::MAX_UPLINK_BUFFER);
                assert!(seen.audit && !session.control.ledger().quarantined);
                eprintln!("F_RETRY_PASSED elapsed={:?} release={released_at:?} restored={restored_at:?} timing={retry_timing:?} current={:?}",
                    retry_start.elapsed(), upstream_ready_connection(&upstream)
                        .map(|connection| (connection.stable_id(), connection.bi_stream_open_snapshot())));
                4097
            }
            FullCase::WaitingDeadline => {
                healthy_exchange(&mut session, &mut apps, &commands, 31).await;
                let earliest = frozen_waiting.iter().map(|s| s.2).min().unwrap();
                let latest = frozen_waiting.iter().map(|s| s.2).max().unwrap();
                assert!(tokio::time::Instant::now() < earliest);
                assert!(!gate.reset_seen.load(Ordering::SeqCst));
                gate.blocked.store(true, Ordering::SeqCst);
                let blocked = tokio::time::timeout_at(latest + Duration::from_secs(5), async {
                    while !gate.waiting.load(Ordering::SeqCst) {
                        credit_turn(&mut session, &mut apps, true).await;
                        if gate.reset_seen.load(Ordering::SeqCst) {
                            assert!(tokio::time::Instant::now() >= earliest);
                        }
                    }
                }).await;
                assert!(blocked.is_ok(), "natural deadline RST: {:?}", session.control.flow_observation());
                assert!(gate.reset_seen.load(Ordering::SeqCst));
                // The gate blocks the whole flush. Only custody, not DATA progress,
                // is observable here; real healthy bytes are tested before/after.
                for _ in 0..8 {
                    credit_turn(&mut session, &mut apps, true).await;
                    let seen = session.control.flow_observation().unwrap();
                    assert_eq!((seen.live, seen.f, seen.w), (4224, 4096, 128));
                    assert_eq!((seen.escrow_bytes, seen.reserved), (128, 128 * crate::client_tun::MAX_UPLINK_BUFFER));
                    assert_eq!((seen.factory_started, seen.published, seen.released, seen.joined), (4096, 4096, 0, 0));
                    assert_eq!((seen.attempts, seen.running, seen.retry), (4096, 12288, 0));
                    assert!(seen.audit && !session.control.ledger().quarantined);
                    assert!(session.control.ledger().first_failure.is_none());
                    assert_eq!(accepted.load(Ordering::Acquire), 4096);
                    assert_eq!(apps.sockets.get::<Socket>(apps.handles[0]).state(), smoltcp::socket::tcp::State::Established);
                    let opens = session.control.opens();
                    assert!(frozen_waiting.iter().all(|id| opens.slots.iter().flatten()
                        .any(|s| (s.handle, s.epoch, s.deadline) == *id)));
                }
                gate.release();
                let expired = tokio::time::timeout_at(latest + Duration::from_secs(5), async {
                    loop {
                        credit_turn(&mut session, &mut apps, true).await;
                        let seen = session.control.flow_observation().unwrap();
                        if seen.released == 128 && seen.waiting == 1 && apps.queued(4224) == 0 {
                            break;
                        }
                    }
                }).await;
                assert!(expired.is_ok(), "full Waiting expiry: {:?}", session.control.flow_observation());
                {
                    let opens = session.control.opens();
                    assert!(frozen_waiting.iter().all(|id| opens.slots.iter().flatten()
                        .all(|s| (s.handle, s.epoch) != (id.0, id.1))));
                }
                healthy_exchange(&mut session, &mut apps, &commands, 47).await;
                assert!(apps.handles[4096..4224].iter().all(|h|
                    apps.sockets.get::<Socket>(*h).state() == smoltcp::socket::tcp::State::Closed));
                assert!(apps.handles[..4096].iter().chain(&apps.handles[4224..]).all(|h|
                    apps.sockets.get::<Socket>(*h).state() == smoltcp::socket::tcp::State::Established));
                assert_eq!(session.control.ledger().first_failure.as_ref().unwrap().facts().io_kind,
                    Some(std::io::ErrorKind::TimedOut));
                let seen = session.control.flow_observation().unwrap();
                assert_eq!((seen.live, seen.f, seen.w, seen.waiting), (4097, 4096, 1, 1));
                assert_eq!((seen.factory_started, seen.published, seen.released, seen.joined), (4096, 4096, 128, 0));
                assert_eq!((seen.attempts, seen.running, seen.retry), (4096, 12288, 0));
                assert_eq!((seen.escrow_bytes, seen.reserved), (4096, crate::client_tun::MAX_UPLINK_BUFFER));
                assert!(seen.audit && !session.control.ledger().quarantined);
                assert_eq!(accepted.load(Ordering::Acquire), 4096);
                let connection = upstream_ready_connection(&upstream).unwrap();
                assert_eq!(connection.stable_id(), initial_id);
                assert_eq!(connection.bi_stream_open_snapshot(), (4096, 4224, false, false, false));
                eprintln!("F_DEADLINE_PASSED elapsed={:?} flow={seen:?}", admitted_after.elapsed());
                4096
            }
        };
        let failure = {
            let ledger = session.control.ledger();
            let failure = ledger.first_failure.as_ref().unwrap();
            (
                failure as *const _ as usize,
                format!("{:?}", failure.facts()),
            )
        };
        session.request_stop();
        commands.send(PeerCommand::Drain).await.unwrap();
        let mut stop_timing = FillTiming::default();
        let stopped = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let app_start = std::time::Instant::now();
                apps.turn(true).await;
                stop_timing.app.add(app_start.elapsed());
                if session.state() != SessionDeviceState::DeviceFailedSettled {
                    let window = std::time::Instant::now();
                    let control = session.control.clone();
                    let mut run = std::pin::pin!(session.run());
                    tokio::select! {
                        _ = std::future::poll_fn(|cx| {
                            let started = std::time::Instant::now();
                            let before = control.opens().cursor;
                            let result = run.as_mut().poll(cx);
                            stop_timing.scan.note(before, control.opens().cursor);
                            stop_timing.driver_poll.add(started.elapsed());
                            result
                        }) => {},
                        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
                    }
                    stop_timing.driver_window.add(window.elapsed());
                }
                if session.state() == SessionDeviceState::DeviceFailedSettled
                    && server.as_ref().unwrap().is_finished()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        eprintln!("F_STOP_RESULT complete={} timing={stop_timing:?}", stopped.is_ok());
        assert!(
            stopped.is_ok(),
            "full stop: state={:?}, flow={:?}, timing={stop_timing:?}",
            session.state(),
            session.control.flow_observation()
        );
        assert_eq!(server.take().unwrap().await.unwrap(), total_published);
        apps.turn(true).await;
        assert!(
            apps.handles
                .iter()
                .all(|h| apps.sockets.get::<Socket>(*h).state()
                    == smoltcp::socket::tcp::State::Closed)
        );
        let done = session.control.flow_observation().unwrap();
        assert_eq!(
            (done.live, done.w, done.f, done.escrow_bytes, done.reserved),
            (0, 0, 0, 0, 0)
        );
        assert_eq!(
            (done.published, done.released, done.joined),
            (total_published as u64, 4225, total_published as u64 * 3)
        );
        assert_eq!(done.factory_started, total_published as u64);
        assert!(done.audit && !session.control.ledger().quarantined);
        for _ in 0..2 {
            assert_eq!(
                session.observe_shutdown().await,
                SessionDeviceState::DeviceFailedSettled
            );
            let ledger = session.control.ledger();
            let retained = ledger.first_failure.as_ref().unwrap();
            assert_eq!(
                (
                    retained as *const _ as usize,
                    format!("{:?}", retained.facts())
                ),
                failure
            );
        }
        assert!(
            peer.close_reason().is_none(),
            "shared carrier cleanup cannot prove settlement"
        );
    })
    .await;
    gate.release();
    session.request_stop();
    let _ = commands.try_send(PeerCommand::Drain);
    let state = tokio::time::timeout(Duration::from_secs(20), session.observe_shutdown()).await;
    let cleanup_rtt = upstream_ready_connection(&upstream).map(|conn| conn.rtt());
    let close_started = std::time::Instant::now();
    eprintln!(
        "F_ENDPOINT_CLOSE case={case:?} client_rtt={cleanup_rtt:?} peer_rtt={:?} connections={}",
        peer.rtt(),
        endpoint.open_connections()
    );
    endpoint.close(0u32.into(), b"full pressure fixture cleanup");
    peer.close(0u32.into(), b"full pressure fixture cleanup");
    pair.client
        .close(0u32.into(), b"full pressure fixture cleanup");
    pair.server
        .close(0u32.into(), b"full pressure fixture cleanup");
    if let Some(server) = server {
        let _ = server.await;
    }
    drop(session);
    drop(upstream);
    drop(auth);
    drop(peer);
    // Endpoint draining follows QUIC PTO, not the flow's20/5/15s acceptance.
    // This separate fixture budget waits for all three actual endpoints.
    let idle = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            endpoint.wait_idle(),
            pair.client.wait_idle(),
            pair.server.wait_idle()
        );
    })
    .await;
    let remaining = (
        endpoint.open_connections(),
        pair.client.open_connections(),
        pair.server.open_connections(),
    );
    eprintln!(
        "F_ENDPOINT_IDLE case={case:?} complete={} elapsed={:?} connections={remaining:?}",
        idle.is_ok(),
        close_started.elapsed(),
    );
    let pair_cleanup = capture(pair.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(state.unwrap(), SessionDeviceState::DeviceFailedSettled);
    idle.expect("full-pressure fixture endpoints must drain within their cleanup budget");
    assert_eq!(remaining, (0, 0, 0));
    if let Err(panic) = pair_cleanup {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn real_f4096_full_waits_for_physical_ack_before_one_waiting_retry() {
    full_f_case(FullCase::AckRetry).await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_f4096_waiting_deadline_retains_until_ack_and_preserves_healthy_tcp() {
    full_f_case(FullCase::WaitingDeadline).await;
}

#[tokio::test(flavor = "current_thread")]
async fn real_simultaneous_4225_syns_preserve_full_f_capacity_and_physical_ack() {
    full_f_case(FullCase::SynBurst).await;
}

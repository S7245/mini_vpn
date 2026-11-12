//! N2 frozen acceptance A1/A2/A6/A7 (D126, amended by D127): a real hard Native
//! Service talks to a real quinn peer through `RttRelay` (one-way 5ms); a
//! full-path `Tap::outage` black-holes every client socket. `service.run()` is
//! polled for the whole scenario and is never cancelled before stop. Round-0
//! flows settle with the path up first (D128: an unclosed half-closed flow hits
//! the pre-existing 15s close-tail Q, unrelated to N2). Owns no production
//! authority; shared rig is reused by `native_waiting_steady`.
use super::*;
use fixture::{Apps, ServiceProfile, auth, config, window};
use native_rebuild::{native_ordered_chunk, serve_generation};
use native_rebuild_high_rtt::{RttRelay, Tap};
use smoltcp::socket::tcp;
use std::time::Instant;

/// Everything a scenario touches except the Service, so `service.run()` can be
/// polled beside the scenario with disjoint borrows.
pub(super) struct Rig {
    pub(super) server: quinn::Endpoint,
    pub(super) relay: RttRelay,
    pub(super) cfg: TuicClientConfig,
    pub(super) apps: Apps,
    pub(super) handle: Handle,
    pub(super) saved: Arc<Control>,
    pub(super) budget: bytes::BackingBudget,
    /// Device installs for the driver, `[0]` main receive path (b2), `[1]`
    /// drains (N4 M7 RX per Service poll).
    pub(super) rx_installed: Arc<[std::sync::atomic::AtomicU64; 2]>,
    /// TUN links `[App→Service, Service→App]` (N4 P1 drop census).
    pub(super) links: [PacketLink; 2],
    /// Owns the in-memory CA/trust fixture the Service startup reads.
    _profile: ServiceProfile,
}

pub(super) async fn rig(one_way: Duration) -> (Service<impl Future<Output = Completion>>, Rig) {
    rig_pool(one_way, 1).await
}

/// `rig` with an explicit TCP pool size (F-N2b pool=2 variants, D138).
pub(super) async fn rig_pool(
    one_way: Duration,
    tcp_pool: usize,
) -> (Service<impl Future<Output = Completion>>, Rig) {
    rig_links(one_way, tcp_pool, 64).await
}

/// `rig_pool` with an explicit TUN link capacity in packets (N4 M7 flood).
pub(super) async fn rig_links(
    one_way: Duration,
    tcp_pool: usize,
    link: usize,
) -> (Service<impl Future<Output = Completion>>, Rig) {
    let profile = ServiceProfile::new(true);
    let server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let relay = RttRelay::start(server.local_addr().unwrap(), one_way, Tap::default()).await;
    let policy = native_ordered_chunk();
    let mut cfg = config(relay.addr).with_relay_policy(policy.clone());
    cfg.tcp_pool = tcp_pool;
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let (inbound, outbound) = (PacketLink::bounded(link), PacketLink::bounded(link));
    let apps = Apps::new(outbound.clone(), inbound.clone());
    let (link_in, link_out) = (inbound.clone(), outbound.clone());
    let rx_installed = Arc::new([0, 0].map(std::sync::atomic::AtomicU64::new));
    let mut service = profile.service_slots(
        inbound,
        outbound,
        cfg.clone(),
        runtime,
        link,
        rx_installed.clone(),
    );
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved
        .backing_budget
        .clone()
        .expect("hard Native shared budget");
    (
        service,
        Rig {
            server,
            relay,
            cfg,
            apps,
            handle,
            saved,
            budget,
            rx_installed,
            links: [link_in, link_out],
            _profile: profile,
        },
    )
}

pub(super) fn session(saved: &Control) -> Option<Arc<SessionControl>> {
    saved
        .session
        .lock()
        .unwrap()
        .as_ref()
        .and_then(std::sync::Weak::upgrade)
}

pub(super) fn node(saved: &Control) -> crate::tuic::NativeNodeWeakObserver {
    saved
        .native_node_sample
        .lock()
        .unwrap()
        .clone()
        .expect("Native Node observer")
}

/// (session Q, session first failure present, live flows, opening, waiting).
pub(super) fn ledger(saved: &Control) -> (bool, bool, usize, usize, usize) {
    let control = session(saved).expect("live session");
    let flows = control
        .flow_observation()
        .map_or((0, 0, 0), |f| (f.live, f.opening_providers, f.waiting));
    let ledger = control.ledger();
    (
        ledger.quarantined(),
        ledger.first_failure.is_some(),
        flows.0,
        flows.1,
        flows.2,
    )
}

pub(super) async fn accept(server: &quinn::Endpoint, cfg: &TuicClientConfig) -> quinn::Connection {
    loop {
        let incoming = server.accept().await.unwrap();
        if let Ok(peer) = incoming.await {
            auth(&peer, cfg).await;
            return peer;
        }
    }
}

/// The app closes its round TCP and the flow settles while the path is up.
pub(super) async fn settle_flows(apps: &mut Apps, saved: &Control) {
    apps.sockets.get_mut::<tcp::Socket>(apps.tcp).close();
    let limit = Instant::now() + Duration::from_secs(5);
    while ledger(saved).2 != 0 {
        assert!(
            Instant::now() < limit,
            "round flow did not settle with the path up"
        );
        apps.turn().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Keep the app stack polled (the outage only affects the relay path).
pub(super) async fn poll_until(apps: &mut Apps, at: Instant) {
    while Instant::now() < at {
        apps.turn().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Final stop: Phase Complete within 5s, budget 0, no Q, no device failure.
pub(super) fn assert_clean_stop(
    report: Report,
    elapsed: Duration,
    rig_budget: &bytes::BackingBudget,
    handle: &Handle,
    open_failures: bool,
) {
    assert!(elapsed <= Duration::from_secs(5), "stop took {elapsed:?}");
    assert_eq!(handle.phase(), Phase::Complete);
    assert_eq!((rig_budget.used(), rig_budget.quarantined()), (0, false));
    assert_eq!(
        report.node,
        Some(crate::tuic::NodeShutdown::RunningResourcesSettled),
        "{report:?}"
    );
    assert!(
        report.device_settled() && (open_failures || !report.device_failed()),
        "{report:?}"
    );
    assert_eq!(report.rebuilds, 0);
}

/// A6 storm: 64 app TCP opens (each sends a payload, so the Node really opens)
/// spread over `[detect+start_after, from+end)`. Opens start only after the
/// Node detected the loss (D129): before QUIC idle death the black-holed path
/// is indistinguishable from slow-alive (A3) and an open is a live relay.
pub(super) struct Storm {
    start_after: Duration,
    end: Duration,
    /// F-N4d: detection = the Node's own connection close (client side), not
    /// the peer's, so opens really land in the original recovery attempt.
    aligned: bool,
    /// (socket, opened at, payload sent, settled after).
    sockets: Vec<(
        smoltcp::iface::SocketHandle,
        Instant,
        bool,
        Option<Duration>,
    )>,
}

impl Storm {
    pub(super) fn new(start_after: Duration, end: Duration) -> Self {
        Self {
            start_after,
            end,
            aligned: false,
            sockets: Vec::with_capacity(64),
        }
    }
    pub(super) fn aligned(mut self) -> Self {
        self.aligned = true;
        self
    }
    pub(super) fn step(&mut self, apps: &mut Apps, from: Instant, detected: Option<Instant>) {
        if let Some(detected) = detected.filter(|_| self.sockets.len() < 64) {
            let start = detected + self.start_after;
            let gap = (from + self.end).saturating_duration_since(start) / 64;
            let due = start + gap * u32::try_from(self.sockets.len()).unwrap();
            if Instant::now() >= due {
                let mut socket = tcp::Socket::new(
                    tcp::SocketBuffer::new(vec![0; 1024]),
                    tcp::SocketBuffer::new(vec![0; 1024]),
                );
                let port = 41000 + u16::try_from(self.sockets.len()).unwrap();
                let target = (smoltcp::wire::Ipv4Address::from(apps.target), 10002);
                socket.connect(apps.iface.context(), target, port).unwrap();
                self.sockets
                    .push((apps.sockets.add(socket), Instant::now(), false, None));
                apps.sent_udp = false; // One more UDP datagram per open, inside the outage.
            }
        }
        for (handle, opened, sent, settled) in &mut self.sockets {
            let socket = apps.sockets.get_mut::<tcp::Socket>(*handle);
            if !*sent && socket.can_send() {
                *sent = socket.send_slice(b"storm").is_ok();
            }
            let open = matches!(
                socket.state(),
                tcp::State::SynSent | tcp::State::SynReceived | tcp::State::Established
            );
            if settled.is_none() && (!open || socket.can_recv()) {
                *settled = Some(opened.elapsed());
                socket.close(); // App hygiene: a settled storm socket is closed.
            }
        }
    }
    pub(super) fn report(&self, apps: &Apps) {
        let line: Vec<_> = self
            .sockets
            .iter()
            .map(|(handle, _, _, settled)| {
                (
                    apps.sockets.get::<tcp::Socket>(*handle).state(),
                    settled.map(|d| d.as_millis()),
                )
            })
            .collect();
        println!("N2A storm {line:?}");
    }
    pub(super) fn worst(&self) -> Option<Duration> {
        if self.sockets.len() != 64 {
            return None;
        }
        self.sockets
            .iter()
            .map(|(_, _, _, settled)| *settled)
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .max()
    }
}

/// Peer side for storm streams on the recovered connection: answer each.
pub(super) async fn answer_storm(peer: &quinn::Connection) {
    while let Ok((mut tx, mut rx)) = peer.accept_bi().await {
        let mut buf = [0u8; 64];
        let _ = rx.read(&mut buf).await;
        let _ = tx.write_all(b"storm-reply").await;
        let _ = tx.finish();
    }
}

struct Outcome {
    /// Peers stay open until after stop (closing one would mint a demand).
    _peers: Vec<quinn::Connection>,
    recovered: Duration,
    dcids: usize,
    worst_open: Option<Duration>,
    sample: Option<crate::tuic::WaitingSample>,
}

/// One outage of `outage`, then a new TCP+UDP round; returns the measurements
/// and leaves stop to the caller's run-loop `select`.
async fn outage_round(rig: &mut Rig, outage: Duration, mut storm: Option<Storm>) -> Outcome {
    let first = window(Duration::from_secs(5), async {
        let peer = accept(&rig.server, &rig.cfg).await;
        serve_generation(&peer, &mut rig.apps).await;
        peer
    })
    .await
    .expect("round 0 DATA");
    settle_flows(&mut rig.apps, &rig.saved).await;
    let control = session(&rig.saved).map(|c| Arc::as_ptr(&c) as usize);
    let from = Instant::now();
    let until = from + outage;
    rig.relay.tap.lock().unwrap().outage(from, until);
    let lift = until + Duration::from_millis(500);
    let mut detected = None;
    let mut peers = vec![first.clone()];
    let new_peer = std::cell::RefCell::new(None);
    let peer_side = async {
        let peer = accept(&rig.server, &rig.cfg).await;
        *new_peer.borrow_mut() = Some(peer.clone());
        answer_storm(&peer).await;
        std::future::pending::<()>().await
    };
    let app_side = async {
        loop {
            let now = Instant::now();
            let storm_done = storm.as_ref().is_none_or(|s| s.worst().is_some());
            if now >= lift && (storm_done || now >= lift + Duration::from_secs(6)) {
                break;
            }
            let aligned = storm.as_ref().is_some_and(|s| s.aligned);
            if detected.is_none()
                && (if aligned {
                    node(&rig.saved)
                        .sample()
                        .is_some_and(|s| s.connections.iter().flatten().any(|c| c.closed))
                } else {
                    first.close_reason().is_some()
                })
            {
                detected = Some(now);
                println!("N2A detected={:?}", now - from);
            }
            if let Some(storm) = storm.as_mut() {
                storm.step(&mut rig.apps, from, detected);
            }
            rig.apps.turn().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    tokio::select! {
        () = app_side => {},
        () = peer_side => unreachable!(),
    }
    rig.apps.restart();
    if first.close_reason().is_none() {
        serve_generation(&first, &mut rig.apps).await;
    } else {
        let peer = match new_peer.into_inner() {
            Some(peer) => peer,
            None => accept(&rig.server, &rig.cfg).await,
        };
        serve_generation(&peer, &mut rig.apps).await;
        peers.push(peer);
    }
    let recovered = until.elapsed();
    settle_flows(&mut rig.apps, &rig.saved).await;
    assert_eq!(
        session(&rig.saved).map(|c| Arc::as_ptr(&c) as usize),
        control,
        "same SessionControl"
    );
    let (q, failed, _, opening, waiting) = ledger(&rig.saved);
    let first = session(&rig.saved)
        .and_then(|c| fixture::first_failure_identity(&c))
        .map(|f| (f.1, String::from_utf8_lossy(&f.2).into_owned()));
    if let Some(storm) = &storm {
        storm.report(&rig.apps);
    }
    // A storm's failed opens retain the existing typed TcpOpen first failure
    // (N1 per-open rule, the session keeps running); otherwise none at all.
    let typed_open = first
        .as_ref()
        .is_some_and(|f| f.0.contains("phase: TcpOpen, resource: TcpOpen"));
    assert!(
        !q && (!failed || (storm.is_some() && typed_open)),
        "session Q={q} first_failure={first:?}"
    );
    let sample = node(&rig.saved).waiting();
    let dcids = rig.relay.tap.lock().unwrap().new_dcids(from, until);
    println!(
        "N2A outage={outage:?} recovered_after_lift={recovered:?} dcids={dcids} opening={opening} waiting={waiting} sample={sample:?}"
    );
    if storm.is_some() {
        assert_eq!((opening, waiting), (0, 0), "open inventory after recovery");
    }
    Outcome {
        _peers: peers,
        recovered,
        dcids,
        worst_open: storm.as_ref().and_then(Storm::worst),
        sample,
    }
}

/// Drive `script` beside the never-cancelled `service.run()`. The script stores
/// its result, requests stop, then keeps the app stack polled until run ends.
pub(super) async fn drive(
    service: &mut Service<impl Future<Output = Completion>>,
    limit: Duration,
    script: impl Future<Output = std::convert::Infallible>,
) -> Report {
    window(limit, async {
        tokio::select! {
            _ = script => unreachable!(),
            report = service.run() => report,
        }
    })
    .await
    .expect("scenario or stop exceeded its window")
}

/// Request stop now and keep the app stack alive while the Service settles.
pub(super) async fn stop_polling(
    rig: &mut Rig,
    stopped: &std::cell::Cell<Option<Instant>>,
) -> std::convert::Infallible {
    stopped.set(Some(Instant::now()));
    rig.handle.request_stop();
    loop {
        rig.apps.turn().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn run_outage(outage: Duration, storm: Option<Storm>) -> Outcome {
    let (mut service, mut rig) = rig(Duration::from_millis(5)).await;
    let (mut outcome, stopped) = (None, std::cell::Cell::new(None));
    let storm_run = storm.is_some();
    let report = drive(&mut service, outage + Duration::from_secs(60), async {
        outcome = Some(outage_round(&mut rig, outage, storm).await);
        stop_polling(&mut rig, &stopped).await
    })
    .await;
    let outcome = outcome.expect("scenario finished before stop");
    assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        storm_run,
    );
    rig.server.close(0u32.into(), b"done");
    outcome
}

/// A2 bound: 2 original attempts + re-probes whose start fits the outage, with
/// the earliest schedule (idle ≥15s, demand ≥10.5s, then wait+5s per step).
pub(super) fn a2_bound(outage: Duration) -> usize {
    let (mut at, mut steps) = (25.0f64, 0usize);
    for wait in [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0] {
        at += wait;
        if at >= outage.as_secs_f64() {
            break;
        }
        steps += 1;
        at += 5.0;
    }
    2 + steps
}

#[tokio::test(flavor = "current_thread")]
async fn n2_a1_outage_8s_keeps_connection_and_recovers_within_2s() {
    let outcome = run_outage(Duration::from_secs(8), None).await;
    assert_eq!(outcome.dcids, 0, "no new connection below QUIC idle");
    assert!(
        outcome.recovered <= Duration::from_secs(2),
        "{:?}",
        outcome.recovered
    );
    assert_eq!(outcome.sample.map(|s| s.waits), Some(0));
}

#[tokio::test(flavor = "current_thread")]
async fn n2_a2_outage_25s_waiting_recovers_same_session() {
    let outage = Duration::from_secs(25);
    let outcome = run_outage(outage, None).await;
    assert!(
        outcome.dcids <= a2_bound(outage),
        "dcids={} bound={}",
        outcome.dcids,
        a2_bound(outage)
    );
    assert!(
        outcome.recovered <= Duration::from_secs(36),
        "{:?}",
        outcome.recovered
    );
    let sample = outcome.sample.unwrap();
    assert!(!sample.first_failure && !sample.quarantined, "{sample:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn n2_a2_outage_60s_backoff_bounds_attempts_and_recovers() {
    let outage = Duration::from_secs(60);
    let outcome = run_outage(outage, None).await;
    // Frozen literal (≤8) and the frozen formula at 5s/attempt (=6, D129).
    assert!(
        outcome.dcids <= 8 && outcome.dcids <= a2_bound(outage),
        "dcids={} bound={}",
        outcome.dcids,
        a2_bound(outage)
    );
    assert!(
        outcome.recovered <= Duration::from_secs(36),
        "{:?}",
        outcome.recovered
    );
    let sample = outcome.sample.unwrap();
    assert!(
        !sample.first_failure && !sample.quarantined && sample.reprobes >= 1,
        "{sample:?}"
    );
}

/// A6 (frozen): 64 opens + UDP across a 25s outage.
#[tokio::test(flavor = "current_thread")]
async fn n2_a6_storm_25s_opens_settle_and_do_not_multiply_attempts() {
    let outage = Duration::from_secs(25);
    let outcome = run_outage(
        outage,
        Some(Storm::new(
            Duration::from_millis(500),
            Duration::from_secs(24),
        )),
    )
    .await;
    assert!(outcome.dcids <= a2_bound(outage), "dcids={}", outcome.dcids);
    let worst = outcome.worst_open.expect("every open settled");
    assert!(
        worst <= Duration::from_secs(6),
        "slowest open settled after {worst:?}"
    );
    assert!(outcome.recovered <= Duration::from_secs(36));
}

/// F-N4d (D154): the storm starts at the Node's own loss detection, so every
/// open is issued during the ORIGINAL closed-slot attempt (2×5s handshakes).
/// Each open must still settle ≤6s from its own start (D126 open bound).
#[tokio::test(flavor = "current_thread")]
async fn n2_a6_storm_aligned_original_attempt_opens_settle_within_6s() {
    // Client idle detection lands at ~15–20s (keep-alive resets idle); 40s keeps
    // both original handshakes inside the outage, so the attempt fails on its own.
    let outage = Duration::from_secs(40);
    let storm = Storm::new(Duration::from_millis(100), Duration::from_secs(29)).aligned();
    let outcome = run_outage(outage, Some(storm)).await;
    assert!(outcome.dcids <= a2_bound(outage), "dcids={}", outcome.dcids);
    let worst = outcome.worst_open.expect("every open settled");
    assert!(
        worst <= Duration::from_secs(6),
        "slowest open settled after {worst:?}"
    );
    assert!(outcome.recovered <= Duration::from_secs(36));
}

/// A6 Waiting variant (D129): the storm lands in the Waiting/re-probe phase.
#[tokio::test(flavor = "current_thread")]
async fn n2_a6_storm_during_waiting_60s_does_not_multiply_attempts() {
    let outage = Duration::from_secs(60);
    let outcome = run_outage(
        outage,
        Some(Storm::new(Duration::from_secs(12), Duration::from_secs(58))),
    )
    .await;
    assert!(
        outcome.dcids <= 8 && outcome.dcids <= a2_bound(outage),
        "dcids={} bound={}",
        outcome.dcids,
        a2_bound(outage)
    );
    let worst = outcome.worst_open.expect("every open settled");
    assert!(
        worst <= Duration::from_secs(6),
        "slowest open settled after {worst:?}"
    );
}

/// A7: stop while parked in Waiting (between re-probes of a 60s outage).
#[tokio::test(flavor = "current_thread")]
async fn n2_a7_stop_during_waiting_completes_within_5s() {
    let (mut service, mut rig) = rig(Duration::from_millis(5)).await;
    let (mut sample, stopped) = (None, std::cell::Cell::new(None));
    let report = drive(&mut service, Duration::from_secs(90), async {
        let peer = window(Duration::from_secs(5), async {
            let peer = accept(&rig.server, &rig.cfg).await;
            serve_generation(&peer, &mut rig.apps).await;
            peer
        })
        .await
        .expect("round 0 DATA");
        settle_flows(&mut rig.apps, &rig.saved).await;
        let from = Instant::now();
        rig.relay
            .tap
            .lock()
            .unwrap()
            .outage(from, from + Duration::from_secs(60));
        poll_until(&mut rig.apps, from + Duration::from_secs(26)).await;
        let observer = node(&rig.saved);
        let limit = from + Duration::from_secs(55);
        let parked = loop {
            let sample = observer.waiting().expect("Node alive");
            if sample.parked == 1 && sample.reprobes >= 3 {
                break sample;
            }
            assert!(
                Instant::now() < limit,
                "never parked in Waiting: {sample:?}"
            );
            poll_until(&mut rig.apps, Instant::now() + Duration::from_millis(50)).await;
        };
        println!("N2A7 stop at {:?} sample={parked:?}", from.elapsed());
        sample = Some((parked, peer));
        stop_polling(&mut rig, &stopped).await
    })
    .await;
    let (sample, _peer) = sample.expect("parked before stop");
    assert!(!sample.quarantined && !sample.first_failure, "{sample:?}");
    assert_clean_stop(
        report,
        stopped.get().unwrap().elapsed(),
        &rig.budget,
        &rig.handle,
        false,
    );
    rig.server.close(0u32.into(), b"done");
}

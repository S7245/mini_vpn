//! N1 stage C1: high-RTT serial controlled rebuild (F7/D44 B8 decision).
//! Owns no production authority. A test-only loopback UDP relay injects a
//! fixed one-way delay `d = r/2` (FIFO, no loss) between the real Native
//! client Node and a real quinn peer, then the original saved driver performs
//! one explicit rebuild exactly as B1. The tracer measures the serial time
//! `T = C + D' + H` and checks it against the falsifiable prediction derived
//! from the fork's actual timers (`connection/mod.rs::set_close_timer` =
//! 3·PTO, `pto = srtt + max(4·rttvar, 1ms) + max_ack_delay(25ms)`):
//! - C  = rebuild request → client CONNECTION_CLOSE (server close − d),
//!   `∈ [r, r + 25ms] + slack`: settling the live old Native stream costs
//!   one peer round trip (measured; "C is local" was falsified, D52);
//! - D' = client close → successor's first Initial at the relay, bounded by
//!   `[3(r + 1ms + 25ms), 3(3r + 25ms)] + local slack` (rttvar ∈ [0, r/2]);
//! - H  = successor Initial → TCP+UDP replies at the App, `≥ 2.5r` (server
//!   accept 1.5r, then one TCP round trip; UDP overlaps).
//!
//! Serial B8 bound: `(r + 25ms) + (9r + 75ms) + 2.5r ≤ 5s` ⇒ safe for every
//! rttvar when `r ≲ 390ms`; best case (rttvar→0) breaks at `r ≈ 750ms`.
use super::native_rebuild::{native_ordered_chunk, serve_generation};
use super::*;
use fixture::{auth, capture, config, window, Apps, ServiceProfile};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Instant;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// Bounded per-direction delay line (tail drop at capacity, counted).
const RELAY_QUEUE: usize = 4096;
/// Bounded relay datagram log (C3 relay-side timestamps); overflow counted.
const RELAY_LOG: usize = 8192;
const LOCAL_SLACK: Duration = Duration::from_millis(300);
const MAX_ACK_DELAY: Duration = Duration::from_millis(25);
const GRANULARITY: Duration = Duration::from_millis(1);

/// Relay direction: `Up` = Node → peer, `Down` = peer → Node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Dir {
    Up,
    Down,
}

/// One datagram observed at the relay ingress (before the delay line).
#[derive(Clone, Copy, Debug)]
pub(super) struct Seen {
    pub(super) generation: usize,
    pub(super) dir: Dir,
    pub(super) ordinal: u64,
    pub(super) at: Instant,
    pub(super) len: usize,
    pub(super) injected: bool,
    /// N2: first 8 DCID bytes of a QUIC v1 long-header Initial, else None.
    pub(super) initial: Option<u64>,
}

/// QUIC v1 Initial (long header, type 0): first ≤8 DCID bytes as an id.
fn initial_dcid(bytes: &[u8]) -> Option<u64> {
    let (&first, rest) = bytes.split_first()?;
    if first & 0xb0 != 0x80 || rest.len() < 5 {
        return None;
    }
    let len = usize::from(rest[4]).min(8);
    let dcid = rest.get(5..5 + len)?;
    Some(dcid.iter().fold(0u64, |acc, b| acc << 8 | u64::from(*b)))
}

/// Deterministic loss plan + bounded log, shared by both relay directions.
/// A planned drop `(generation, dir, ordinal)` discards exactly that
/// datagram (ordinal counted per generation and direction from 0); a
/// blackhole `(generation, dir, from)` discards every later datagram.
#[derive(Default)]
pub(super) struct Tap {
    counts: HashMap<(usize, Dir), u64>,
    plan: Vec<(usize, Dir, u64)>,
    hits: usize,
    blackhole: Option<(usize, Dir, Instant)>,
    /// N2: discard every datagram of every generation in `[from, until)`.
    outage: Option<(Instant, Instant)>,
    /// N2: deterministic loss, every Nth datagram per (generation, dir).
    loss_every: Option<u64>,
    /// N2: every Up QUIC Initial (time, DCID id), independent of `log`; ≤1024.
    pub(super) initials: Vec<(Instant, u64)>,
    pub(super) log: Vec<Seen>,
    pub(super) log_overflow: u64,
    pub(super) injected: u64,
}

impl Tap {
    /// Returns true when the datagram must be dropped by the plan.
    fn observe(&mut self, generation: usize, dir: Dir, bytes: &[u8]) -> bool {
        let len = bytes.len();
        let count = self.counts.entry((generation, dir)).or_insert(0);
        let ordinal = *count;
        *count += 1;
        let at = Instant::now();
        let planned = self.plan.contains(&(generation, dir, ordinal));
        self.hits += usize::from(planned);
        let holed = self.blackhole.is_some_and(|(g, d, from)| g == generation && d == dir && at >= from);
        let out = self.outage.is_some_and(|(from, until)| at >= from && at < until);
        let lossy = self.loss_every.is_some_and(|n| ordinal % n == n - 1);
        let injected = planned || holed || out || lossy;
        self.injected += u64::from(injected);
        let initial = initial_dcid(bytes);
        if let Some(id) = initial.filter(|_| dir == Dir::Up && self.initials.len() < 1024) {
            self.initials.push((at, id));
        }
        let seen = Seen { generation, dir, ordinal, at, len, injected, initial };
        if self.log.len() < RELAY_LOG {
            self.log.push(seen);
        } else {
            self.log_overflow += 1;
        }
        injected
    }
    /// Plan a drop `offset` datagrams after those already observed.
    pub(super) fn drop_next(&mut self, generation: usize, dir: Dir, offset: u64) {
        let base = self.counts.get(&(generation, dir)).copied().unwrap_or(0);
        self.plan.push((generation, dir, base + offset));
    }
    /// Discard every `generation`/`dir` datagram observed at or after `from`.
    pub(super) fn blackhole(&mut self, generation: usize, dir: Dir, from: Instant) {
        self.blackhole = Some((generation, dir, from));
    }
    /// N2: full-path outage (both directions, every client socket).
    pub(super) fn outage(&mut self, from: Instant, until: Instant) {
        self.outage = Some((from, until));
    }
    /// N2: drop every `n`th datagram of each (generation, dir), both ways.
    pub(super) fn loss_every(&mut self, n: u64) {
        self.loss_every = Some(n.max(1));
    }
    /// N2: distinct Up Initial DCIDs first seen in `[from, until)`.
    pub(super) fn new_dcids(&self, from: Instant, until: Instant) -> usize {
        let before: std::collections::BTreeSet<u64> =
            self.initials.iter().filter(|(at, _)| *at < from).map(|(_, id)| *id).collect();
        let ids: std::collections::BTreeSet<u64> = self.initials.iter()
            .filter(|(at, id)| *at >= from && *at < until && !before.contains(id))
            .map(|(_, id)| *id).collect();
        ids.len()
    }
    /// Every planned ordinal was observed (and dropped) exactly once.
    pub(super) fn plan_exhausted(&self) -> bool {
        self.hits == self.plan.len()
    }
}

pub(super) struct RttRelay {
    pub(super) addr: SocketAddr,
    /// First datagram time per client socket (one per Node generation).
    pub(super) first_seen: Arc<Mutex<Vec<(SocketAddr, Instant)>>>,
    pub(super) tap: Arc<Mutex<Tap>>,
    pub(super) dropped: Arc<std::sync::atomic::AtomicU64>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

type Delayed = (Instant, Vec<u8>, SocketAddr);

fn delay_line(socket: Arc<UdpSocket>) -> (mpsc::Sender<Delayed>, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel::<Delayed>(RELAY_QUEUE);
    let task = tokio::spawn(async move {
        while let Some((due, bytes, to)) = rx.recv().await {
            tokio::time::sleep_until(due.into()).await;
            let _ = socket.send_to(&bytes, to).await;
        }
    });
    (tx, task)
}

impl RttRelay {
    pub(super) async fn start(server: SocketAddr, one_way: Duration, tap: Tap) -> Self {
        let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr = front.local_addr().unwrap();
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let tap = Arc::new(Mutex::new(tap));
        let dropped = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (to_client, client_line) = delay_line(front.clone());
        let (seen, lost, taps) = (first_seen.clone(), dropped.clone(), tap.clone());
        let pump = tokio::spawn(async move {
            let mut upstreams: HashMap<SocketAddr, (usize, mpsc::Sender<Delayed>)> = HashMap::new();
            let mut children = Vec::new();
            let mut buf = vec![0u8; 65536];
            loop {
                let Ok((n, client)) = front.recv_from(&mut buf).await else { break };
                let now = Instant::now();
                let (generation, line) = match upstreams.get(&client) {
                    Some((generation, line)) => (*generation, line.clone()),
                    None => {
                        let generation = {
                            let mut seen = seen.lock().unwrap();
                            seen.push((client, now));
                            seen.len() - 1
                        };
                        let up = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
                        let (line, sender) = delay_line(up.clone());
                        let back = to_client.clone();
                        let (lost, taps) = (lost.clone(), taps.clone());
                        children.push(sender);
                        children.push(tokio::spawn(async move {
                            let mut buf = vec![0u8; 65536];
                            while let Ok((n, _)) = up.recv_from(&mut buf).await {
                                if taps.lock().unwrap().observe(generation, Dir::Down, &buf[..n]) {
                                    continue;
                                }
                                let item = (Instant::now() + one_way, buf[..n].to_vec(), client);
                                if back.try_send(item).is_err() {
                                    lost.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }));
                        upstreams.insert(client, (generation, line.clone()));
                        (generation, line)
                    }
                };
                if taps.lock().unwrap().observe(generation, Dir::Up, &buf[..n]) {
                    continue;
                }
                if line.try_send((now + one_way, buf[..n].to_vec(), server)).is_err() {
                    lost.fetch_add(1, Ordering::Relaxed);
                }
            }
            for child in children {
                child.abort();
            }
        });
        Self { addr, first_seen, tap, dropped, tasks: vec![client_line, pump] }
    }
}

impl Drop for RttRelay {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

/// C3 deterministic loss for one serial rebuild (none for C1).
#[derive(Clone, Copy, Default)]
pub(super) struct Loss {
    /// Drops planned before the run: `(generation, dir, ordinal)`.
    pub(super) fixed: &'static [(usize, Dir, u64)],
    /// Blackhole the original generation's Up direction from `request + r`
    /// (after the one-round-trip stream settlement, D52): its
    /// CONNECTION_CLOSE and every re-sent close are lost, so the first
    /// dropped datagram's relay time replaces the server close observation.
    pub(super) old_close_lost: bool,
}

#[derive(Debug)]
pub(super) struct SerialTiming {
    pub(super) rtt: Duration,
    pub(super) c: Duration,
    pub(super) d_prime: Duration,
    pub(super) h: Duration,
    pub(super) total: Duration,
    pub(super) server_srtt: Duration,
    pub(super) injected: u64,
}

impl SerialTiming {
    pub(super) fn drain_bounds(&self) -> (Duration, Duration) {
        let r = self.rtt;
        (3 * (r + GRANULARITY + MAX_ACK_DELAY), 3 * (3 * r + MAX_ACK_DELAY) + LOCAL_SLACK)
    }
    fn assert_prediction(&self) {
        let (low, high) = self.drain_bounds();
        // 2ms tolerance for relay/timer quantisation on the lower edge.
        assert!(self.d_prime + Duration::from_millis(2) >= low && self.d_prime <= high,
            "D' outside 3·PTO prediction [{low:?}, {high:?}]: {self:?}");
        let h_low = self.rtt * 5 / 2;
        assert!(self.h + Duration::from_millis(2) >= h_low && self.h <= self.rtt * 7 / 2 + LOCAL_SLACK,
            "H outside handshake+AUTH+TCP prediction [{h_low:?}, 3.5r+slack]: {self:?}");
        // First run falsified "C is local" (C ≈ r + 49ms at r=200/400ms):
        // the old generation still owns a live Native TCP stream whose
        // terminal settlement costs one peer round trip (+ ACK delay).
        assert!(self.c + Duration::from_millis(2) >= self.rtt
            && self.c <= self.rtt + MAX_ACK_DELAY + LOCAL_SLACK,
            "loaded settle C outside [r, r+25ms+slack]: {self:?}");
        let upper = self.rtt + MAX_ACK_DELAY + high + self.rtt * 7 / 2 + 2 * LOCAL_SLACK;
        assert!(self.total <= upper, "T above C+D'+H prediction {upper:?}: {self:?}");
    }
}

/// One explicit rebuild of an idle hard Native Service behind `rtt`.
pub(super) async fn serial_rebuild_behind(
    rtt: Duration,
    data_limit: Duration,
    loss: Loss,
) -> (SerialTiming, Vec<Seen>) {
    let one_way = rtt / 2;
    let profile = ServiceProfile::new(true);
    let mut server =
        quinn::Endpoint::server(profile.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let relay = RttRelay::start(server.local_addr().unwrap(), one_way, tap_for(loss)).await;
    let policy = native_ordered_chunk();
    let cfg = config(relay.addr).with_relay_policy(policy.clone());
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let mut service = profile.service(inbound, outbound, cfg.clone(), runtime);
    let handle = service.take_handle().unwrap();
    let saved = service.control.clone();
    let budget = saved.backing_budget.clone().expect("hard Native shared budget");
    let mut peers = Vec::new();
    let mut old_closed = None;
    let mut requested = None;
    let mut timing = None;
    let scenario = capture(async {
        for round in 0..2 {
            let limit = if round == 0 { Duration::from_secs(5) } else { data_limit };
            window(limit, async {
                tokio::select! {
                    _ = async {
                        let peer = server.accept().await.unwrap().await.unwrap();
                        auth(&peer, &cfg).await;
                        if round == 1 {
                            apps.restart();
                        }
                        serve_generation(&peer, &mut apps).await;
                        peers.push(peer);
                    } => {},
                    report = service.run() => panic!("Native Service ended in round {round}: {report:?}"),
                }
            })
            .await
            .unwrap_or_else(|_| panic!("round {round} DATA within {limit:?}"));
            let done = Instant::now();
            let current = saved.session.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
            assert!(current.ledger().first_failure.is_none());
            assert!(!budget.quarantined());
            if round == 0 {
                let old = peers[0].clone();
                old_closed = Some(tokio::spawn(async move {
                    old.closed().await;
                    Instant::now()
                }));
                requested = Some(Instant::now());
                if loss.old_close_lost {
                    let from = requested.unwrap() + rtt;
                    relay.tap.lock().unwrap().blackhole(0, Dir::Up, from);
                }
                assert!(handle.request_rebuild(), "first explicit rebuild accepted");
            } else {
                assert!(!handle.request_rebuild(), "no third generation");
                let seen = relay.first_seen.lock().unwrap().clone();
                assert_eq!(seen.len(), 2, "exactly two client generations: {seen:?}");
                let closed = old_closed.take().unwrap();
                let client_close = match loss.old_close_lost {
                    // Peer never saw any close: use the relay ingress time of
                    // the first dropped datagram (client send + loopback).
                    true => {
                        assert!(!closed.is_finished(), "peer must not observe a lost close");
                        relay_close(&relay.tap.lock().unwrap())
                    }
                    false => {
                        assert!(closed.is_finished(), "old connection closed before successor DATA");
                        closed.await.unwrap() - one_way
                    }
                };
                let req = requested.unwrap();
                timing = Some(SerialTiming {
                    rtt,
                    c: client_close.saturating_duration_since(req),
                    d_prime: seen[1].1.saturating_duration_since(client_close),
                    h: done.saturating_duration_since(seen[1].1),
                    total: done - req,
                    server_srtt: peers[0].rtt(),
                    injected: relay.tap.lock().unwrap().injected,
                });
                // Serial order (I2): successor Initial only after old close.
                assert!(seen[1].1 >= client_close, "serial C+D+H: {seen:?} close {client_close:?}");
            }
        }
    })
    .await;
    handle.request_stop();
    let cleanup = capture(async {
        let report = window(Duration::from_secs(15), service.run()).await.unwrap();
        assert_eq!(report.session, SessionDeviceState::DeviceSettled);
        assert_eq!(report.node, Some(crate::tuic::NodeShutdown::RunningResourcesSettled));
        assert_eq!(handle.phase(), Phase::Complete);
        if scenario.is_ok() {
            assert_eq!(report.rebuilds, 1);
        }
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0, "both generations returned the shared budget");
    })
    .await;
    for peer in peers {
        peer.close(0u32.into(), b"done");
    }
    let drain = server.take_driver_drain_handle().unwrap();
    server.close(0u32.into(), b"done");
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    let dropped = relay.dropped.load(Ordering::Relaxed);
    let (log, exhausted, injected, overflow) = {
        let tap = relay.tap.lock().unwrap();
        (tap.log.clone(), tap.plan_exhausted(), tap.injected, tap.log_overflow)
    };
    drop(relay);
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    assert!(drained.unwrap());
    assert_eq!(dropped, 0, "relay delay line never tail-dropped");
    assert_eq!(overflow, 0, "relay log stayed within {RELAY_LOG}");
    assert!(exhausted, "every planned drop happened exactly once");
    let expected = loss.fixed.len() as u64;
    if loss.old_close_lost {
        assert!(injected > expected, "the old close was actually dropped");
    } else {
        assert_eq!(injected, expected, "only the planned drops");
    }
    let timing = timing.unwrap();
    eprintln!("C serial rebuild timing: {timing:?}");
    (timing, log)
}

fn tap_for(loss: Loss) -> Tap {
    let mut tap = Tap::default();
    for &(generation, dir, ordinal) in loss.fixed {
        tap.drop_next(generation, dir, ordinal);
    }
    tap
}

fn relay_close(tap: &Tap) -> Instant {
    let mut dropped = tap.log.iter().filter(|s| s.injected && s.generation == 0 && s.dir == Dir::Up);
    dropped.next().expect("old close blackholed").at
}

/// C1 gate point: at r = 200ms the serial path meets the original 5s
/// successor-DATA limit and every component matches the timer prediction.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_serial_rebuild_at_200ms_rtt_meets_original_5s_and_formula() {
    let (timing, _) =
        serial_rebuild_behind(Duration::from_millis(200), Duration::from_secs(5), Loss::default()).await;
    timing.assert_prediction();
    assert!(timing.total <= Duration::from_secs(5), "serial B8 bound at 200ms: {timing:?}");
}

/// C1 falsifier near the predicted worst-case crossover (r = 400ms): the
/// components must still follow the formula; the total is recorded, not
/// required to meet 5s (that is the B8 decision input).
#[tokio::test(flavor = "current_thread")]
async fn hard_native_serial_rebuild_at_400ms_rtt_follows_formula() {
    let (timing, _) =
        serial_rebuild_behind(Duration::from_millis(400), Duration::from_secs(15), Loss::default()).await;
    timing.assert_prediction();
}

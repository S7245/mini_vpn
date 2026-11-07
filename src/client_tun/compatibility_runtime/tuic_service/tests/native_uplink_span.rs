//! N1 stage C6 (D79/D80): offline rate×RTT measurement of the uplink
//! sent-history ring, testing C-I8 as a **necessary-only** condition (no
//! throughput claim). One real hard Native generation (Chunk engine) sends an
//! App TCP bulk upload through LoopbackTunDevice → Service → Native → Quinn
//! fork → `RttRelay` (`d = r/2`, FIFO, no loss) → a real quinn peer that counts
//! and discards. The fork's process-global `sent-history-observe` counters
//! (enabled by `harness`) give the Data-space cold ring peak `used`, the count
//! of ordinary refusals, and the peak cwnd. Frozen predictions (D79, before
//! any run): see `docs/current/n1-c6-proposal-20261107.md`.
//!
//! Experimental control (D80, fixed before any run): the shared fixture peer
//! advertises a 64KiB stream window, which alone would cap the uplink at
//! 64KiB/RTT; this leaf's peer advertises 64MiB stream/256MiB connection
//! windows so that only client-side limits (ring, cwnd, CPU) remain. The App
//! bulk socket has a 1MiB send buffer for the same reason.
//!
//! Counters are process-wide: each test is `#[ignore]` and must run alone in
//! its own process (`--ignored --exact --test-threads=1`, gate group
//! `uplink-span`), never inside the parallel joint suite. Owns no production
//! authority.
use super::native_rebuild::native_ordered_chunk;
use super::native_rebuild_high_rtt::{RttRelay, Tap};
use super::*;
use fixture::{auth, capture, config, native_shape, window, Apps, ServiceProfile};
use quinn::sent_history_observe as observe;
use smoltcp::socket::tcp;
use smoltcp::wire::Ipv4Address;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

/// C-I8 per-packet byte budget: installed max UDP payload (D86 Q1 re-freeze;
/// D79's 1230 assumed a 1200-byte path, falsified by D84).
const P: f64 = 1472.0;
/// D79 cwnd check kept verbatim (cwnd must exceed the ring's 1230-byte span).
const P_CWND: f64 = 1230.0;
const ORDINARY: u64 = 1024;
/// Frozen peak interval: `[960, ordinary + control headroom ≤ 1100]`.
const PEAK: std::ops::RangeInclusive<u64> = 960..=1100;
const BULK_PORT: u16 = 40010;
static ZERO: [u8; 16384] = [0; 16384];

struct Span {
    snapshot: observe::Snapshot,
    /// Steady-state goodput (bytes/s) over the second half of the bulk window.
    goodput: f64,
    total: u64,
    relay_dropped: u64,
}

/// C-I8 ring bound `ordinary·P/RTT` in bytes/s (D86, no ACK-delay term).
fn ring_bound(rtt: Duration) -> f64 {
    ORDINARY as f64 * P / rtt.as_secs_f64()
}

/// D83: client send window 4MiB so the ring (≈1.26MB) binds before it.
fn c6_shape() -> crate::quic::honest_bound::TransportShape {
    crate::quic::honest_bound::TransportShape { send_window: 4 << 20, ..native_shape() }
}

fn peer_config(profile: &ServiceProfile) -> quinn::ServerConfig {
    let mut config = profile.server_config();
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(8u32.into());
    transport.max_concurrent_bidi_streams(100u32.into());
    transport.stream_receive_window((64u32 << 20).into());
    transport.receive_window((256u32 << 20).into());
    config.transport_config(Arc::new(transport));
    config
}

/// Accept every bidi stream, count and discard its bytes; keep send halves
/// open so no remote FIN half-closes the flows under test.
fn serve(server: quinn::Endpoint, cfg: TuicClientConfig, received: Arc<AtomicU64>,
    peers: Arc<Mutex<Vec<quinn::Connection>>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Some(incoming) = server.accept().await else { return };
        let peer = incoming.await.unwrap();
        peers.lock().unwrap().push(peer.clone());
        auth(&peer, &cfg).await;
        while let Ok((send, mut recv)) = peer.accept_bi().await {
            let received = received.clone();
            tokio::spawn(async move {
                let _send = send;
                while let Ok(Some(chunk)) = recv.read_chunk(usize::MAX, true).await {
                    received.fetch_add(chunk.bytes.len() as u64, Ordering::Relaxed);
                }
            });
        }
    })
}

async fn uplink_span(rtt: Duration, bulk: Duration) -> Span {
    observe::reset();
    let profile = ServiceProfile::new(true);
    let server = quinn::Endpoint::server(peer_config(&profile), "127.0.0.1:0".parse().unwrap()).unwrap();
    let relay = RttRelay::start(server.local_addr().unwrap(), rtt / 2, Tap::default()).await;
    let policy = native_ordered_chunk();
    let cfg = config(relay.addr).with_relay_policy(policy.clone()).with_transport_shape(c6_shape());
    let runtime = TunRuntimeConfig::from_sources(Some("1")).unwrap().with_relay_policy(policy);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let mut apps = Apps::new(outbound.clone(), inbound.clone());
    let mut socket = tcp::Socket::new(tcp::SocketBuffer::new(vec![0; 4096]), tcp::SocketBuffer::new(vec![0; 1 << 20]));
    socket.connect(apps.iface.context(), (Ipv4Address::from(apps.target), 10000), BULK_PORT).unwrap();
    let bulk_socket = apps.sockets.add(socket);
    let mut service = profile.service(inbound, outbound, cfg.clone(), runtime);
    let handle = service.take_handle().unwrap();
    let budget = service.control.backing_budget.clone().expect("hard Native shared budget");
    let received = Arc::new(AtomicU64::new(0));
    let peers = Arc::new(Mutex::new(Vec::new()));
    let peer_task = serve(server.clone(), cfg, received.clone(), peers.clone());
    let mut samples = Vec::new();
    let scenario = capture(async {
        let started = tokio::time::Instant::now();
        let deadline = started + bulk;
        let mut next_sample = started;
        while tokio::time::Instant::now() < deadline {
            apps.turn().await;
            let socket = apps.sockets.get_mut::<tcp::Socket>(bulk_socket);
            while socket.can_send() {
                if socket.send_slice(&ZERO).unwrap() == 0 {
                    break;
                }
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(1), service.run()).await.is_err(),
                "Service ended during the bulk upload"
            );
            let now = tokio::time::Instant::now();
            if now >= next_sample {
                samples.push((now - started, received.load(Ordering::Relaxed)));
                next_sample += Duration::from_millis(250);
            }
        }
        samples.push((started.elapsed(), received.load(Ordering::Relaxed)));
    })
    .await;
    let snapshot = observe::snapshot();
    let half = bulk / 2;
    let (t0, b0) = samples.iter().copied().find(|(at, _)| *at >= half).expect("mid-window sample");
    let (t1, b1) = *samples.last().unwrap();
    let goodput = (b1 - b0) as f64 / (t1 - t0).as_secs_f64();
    let relay_dropped = relay.dropped.load(Ordering::Relaxed);
    let span = Span { snapshot, goodput, total: b1, relay_dropped };
    eprintln!(
        "C6 uplink r={rtt:?} bulk={bulk:?}: {:?} goodput={:.0}B/s ({:.2}Mbit/s) bound={:.0}B/s total={} relay_dropped={} samples={:?}",
        span.snapshot, span.goodput, span.goodput * 8e-6, ring_bound(rtt), span.total, span.relay_dropped,
        samples.iter().step_by(4).collect::<Vec<_>>()
    );
    let cleanup = capture(async {
        handle.request_stop();
        let report = window(Duration::from_secs(30), service.run()).await.unwrap();
        assert!(report.device_settled() && !report.device_failed(), "{report:?}");
        assert!(!budget.quarantined());
        assert_eq!(budget.used(), 0);
    })
    .await;
    peer_task.abort();
    let peers = std::mem::take(&mut *peers.lock().unwrap());
    native_rebuild_fatal::close(server, peers).await;
    drop(relay);
    scenario.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    span
}

/// Frozen ring-bound predictions shared by r=400ms and r=800ms.
fn assert_ring_bound(span: &Span, rtt: Duration) {
    let s = span.snapshot;
    assert_eq!((s.rings, s.ordinary), (1, ORDINARY), "one observed Data ring: {s:?}");
    assert_eq!(span.relay_dropped, 0, "relay queue never tail-dropped");
    assert!(PEAK.contains(&s.peak_used), "peak_used outside {PEAK:?}: {s:?}");
    assert!(s.ordinary_blocked > 0, "ring never refused ordinary data: {s:?}");
    assert!(s.peak_cwnd as f64 > ORDINARY as f64 * P_CWND, "cwnd bound before the ring: {s:?}");
    assert_eq!(s.peak_ordinary, ORDINARY, "ordinary admission fills exactly the ordinary quota: {s:?}");
    assert!(span.goodput <= ring_bound(rtt), "goodput {:.0} above C-I8 {:.0}", span.goodput, ring_bound(rtt));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "C6 process-global counters: serial uplink-span group only (--ignored --exact)"]
async fn uplink_sent_history_span_binds_at_400ms_rtt() {
    let rtt = Duration::from_millis(400);
    let span = uplink_span(rtt, Duration::from_secs(15)).await;
    assert_ring_bound(&span, rtt);
    assert!(span.goodput >= 0.7 * ring_bound(rtt), "goodput {:.0} below 70% of C-I8", span.goodput);
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "C6 process-global counters: serial uplink-span group only (--ignored --exact)"]
async fn uplink_sent_history_span_binds_at_800ms_rtt() {
    let rtt = Duration::from_millis(800);
    let span = uplink_span(rtt, Duration::from_secs(25)).await;
    assert_ring_bound(&span, rtt);
}

/// r=50ms: record-only (D86; D84 showed bursts reach the ring here).
#[tokio::test(flavor = "current_thread")]
#[ignore = "C6 process-global counters: serial uplink-span group only (--ignored --exact)"]
async fn uplink_sent_history_span_recorded_at_50ms_rtt() {
    let span = uplink_span(Duration::from_millis(50), Duration::from_secs(8)).await;
    assert_eq!(span.snapshot.rings, 1, "{:?}", span.snapshot);
    assert!(span.snapshot.peak_ordinary <= ORDINARY, "{:?}", span.snapshot);
}

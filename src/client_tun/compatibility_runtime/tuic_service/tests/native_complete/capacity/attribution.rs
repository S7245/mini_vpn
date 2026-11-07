//! §5.3 Node-only allocator attribution for the F4096 capacity harness (D22).
//! Owns: the peer's own OS thread + tokio runtime (marked Foreign, so the
//! quinn server endpoint/connection drivers, timers and UDP IO never count as
//! Node) and the verdict line. Does not own the Service, App or peer logic.
//! Peer/App futures polled on the test thread are wrapped with
//! `counting_alloc::foreign`; everything unwrapped (Service, Node, runtime,
//! observers) is conservatively Node. The bound is asserted only in the
//! dedicated alone run (`A3_NODE_ATTRIBUTION=1`, `--exact --test-threads=1`),
//! since parallel tests would pollute the process-wide Node counter.
use super::*;
use crate::quic::honest_bound::tests::counting_alloc as alloc;
use std::{pin::Pin, time::Instant};

#[derive(Debug)]
struct PeerRuntime(tokio::runtime::Handle);
impl quinn::Runtime for PeerRuntime {
    fn new_timer(&self, at: Instant) -> Pin<Box<dyn quinn::AsyncTimer>> {
        let _context = self.0.enter();
        quinn::Runtime::new_timer(&quinn::TokioRuntime, at)
    }
    fn spawn(&self, future: Pin<Box<dyn std::future::Future<Output = ()> + Send>>) {
        self.0.spawn(future);
    }
    fn wrap_udp_socket(&self, socket: std::net::UdpSocket) -> std::io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        let _context = self.0.enter();
        quinn::Runtime::wrap_udp_socket(&quinn::TokioRuntime, socket)
    }
}

/// The peer's dedicated Foreign thread; stopped and joined on drop.
pub(super) struct PeerThread {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    join: Option<std::thread::JoinHandle<()>>,
}
impl Drop for PeerThread {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() { let _ = stop.send(()); }
        if let Some(join) = self.join.take() {
            join.join().expect("peer thread");
        }
    }
}

/// Real quinn server endpoint whose every driver runs on a Foreign thread.
pub(super) fn foreign_server(config: quinn::ServerConfig) -> (quinn::Endpoint, PeerThread) {
    let (handle_tx, handle_rx) = std::sync::mpsc::channel();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let join = std::thread::spawn(move || {
        alloc::mark_thread_foreign();
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        handle_tx.send(runtime.handle().clone()).unwrap();
        runtime.block_on(async { drop(stopped.await) });
    });
    let handle = handle_rx.recv().unwrap();
    let server = alloc::foreign_sync(|| {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        quinn::Endpoint::new(quinn::EndpointConfig::default(), Some(config), socket, Arc::new(PeerRuntime(handle)))
            .unwrap()
    });
    (server, PeerThread { stop: Some(stop), join: Some(join) })
}

/// `wire::health` with only the peer/App half Foreign; the Service stays Node.
pub(super) async fn health(service: &mut impl TunService, app: &mut wire::App, peer: &mut wire::Peer) {
    alloc::foreign_sync(|| app.prepare_health());
    wire::drive(service, Duration::from_secs(5), alloc::foreign(async {
        let remote = peer.health();
        let application = async { while !app.healthy() { app.turn().await; } };
        tokio::join!(remote, application)
    })).await;
}

/// Records (and in the alone run asserts) the attributed Node peak against
/// the amended §5.3 bound (D25): `Σ B_node + A1 + I_cfg` and `hard + I_cfg`,
/// where `hard + I_cfg` is the production `native_budget` memory preflight.
pub(super) fn verdict(
    permit: bool, (node_base, total_base): (usize, usize), budget_peak: usize, app_tier: usize,
    (tuic, runtime): (&TuicClientConfig, &TunRuntimeConfig),
) {
    use crate::client_tun::compatibility_runtime::tuic_service::{native_budget, native_instances};
    use crate::quic::honest_bound::{instance_bound, InstanceFactors};
    let reserve = fixture::native_reserve();
    let hard = app_tier + reserve;
    let (budget, memory) = native_budget(runtime.native_limits.unwrap(), tuic, runtime, true).unwrap();
    assert_eq!(budget.limit(), hard);
    let instances = instance_bound(&native_instances(runtime).unwrap(), &InstanceFactors::D25_MEASURED).unwrap();
    assert_eq!(memory, (hard as u64) + instances.total);
    let formula = (app_tier + reserve / 2) as u64 + instances.total;
    let node = alloc::node_peak().saturating_sub(node_base) as u64;
    let total = alloc::peak().saturating_sub(total_base);
    let alone = std::env::var_os("A3_NODE_ATTRIBUTION").is_some();
    eprintln!("[A3-F4096-node-attributed] permit={permit} node_alloc_peak_delta={node} process_alloc_peak_delta={total} budget_sampled_peak={budget_peak} hard={hard} I_cfg={instances:?} amended_formula={formula} memory_preflight={memory} node_le_formula={} node_le_memory={} margin={} alone_run={alone}",
        node <= formula, node <= memory, memory.saturating_sub(node));
    if alone {
        assert!(node <= formula && node <= memory, "§5.3 gap: node {node} formula {formula} memory {memory}");
    }
}

/// Node-domain census baseline; arms `A3_TRACE_SIZES` backtraces (diagnostic).
pub(super) fn census_base() -> Vec<(usize, usize)> {
    if let Some(sizes) = std::env::var_os("A3_TRACE_SIZES") {
        let sizes: Vec<usize> = sizes.to_string_lossy().split(',').filter_map(|s| s.parse().ok()).collect();
        alloc::arm_size_trace(&sizes, 2 * sizes.len());
    }
    alloc::node_census()
}

/// Prints the Node-domain live-object growth since `base`, largest first.
pub(super) fn census_delta(base: &[(usize, usize)], phase: &str) {
    let base: std::collections::HashMap<_, _> = base.iter().copied().collect();
    let mut rows: Vec<(usize, isize)> = alloc::node_census().into_iter()
        .map(|(size, n)| (size, n as isize - *base.get(&size).unwrap_or(&0) as isize))
        .filter(|(_, d)| *d != 0).collect();
    rows.sort_by_key(|(size, d)| std::cmp::Reverse(*size as isize * *d));
    let total: isize = rows.iter().map(|(s, d)| *s as isize * d).sum();
    eprintln!("[A3-census] phase={phase} delta_bytes={total} classes={}", rows.len());
    for (size, d) in rows.iter().take(60) {
        eprintln!("[A3-census-row] phase={phase} size={size} count={d} bytes={}", *size as isize * d);
    }
}

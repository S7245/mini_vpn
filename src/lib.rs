pub mod client_tun;
pub mod device;
pub mod dns;
pub mod dns_block;
pub mod failover;
pub mod fake_ip;
#[cfg(feature = "harness")]
pub mod harness;
pub mod loop_profiler;
pub mod metrics;
// Task 3 deliberately lands the internal branch-by-abstraction surface before
// Task 4/6 provide the real two-leg transport and owner. Keep that dormant
// half of the facade private without turning its staged API into warning noise.
#[allow(dead_code, unused_imports)]
pub(crate) mod owned_upstream;
pub mod quic;
pub(crate) mod quic_udp_send_service;
pub mod reality;
pub mod reality_upstream;
pub mod resumable;
pub mod shared;
pub mod tcp_downlink_pump;
pub mod tcp_egress;
pub mod tcp_stream_service;
pub mod tuic;
pub mod udp_relay;
pub mod upstream;

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::OnceLock;
    use tokio::sync::{Mutex, MutexGuard};

    /// Real localhost capacity discriminators share one host scheduler and UDP
    /// stack. Serialize only those tests so the full suite cannot turn test
    /// runner contention into a false throughput regression.
    pub(crate) async fn local_capacity_test_guard() -> MutexGuard<'static, ()> {
        static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
        GUARD.get_or_init(|| Mutex::new(())).lock().await
    }
}

//! A2 Route 2 transport shape of one TUIC Node.
//!
//! Owns: the buffer-bounding QUIC `TransportShape` a Node installs (production
//! shape derived from `mtu_policy`, or a test-injected smaller one) and the
//! Node's concurrent connection count. Does not own the bound formula or the
//! Service preflight (`crate::quic::honest_bound`, `tuic_service::native_budget`).
//! Invariant: the shape returned here is exactly the one `startup.rs` installs.
//! Tests: `quic/honest_bound/tests` and the Service fixtures.
use super::{TuicClientConfig, MAX_TUIC_TCP_POOL, MIN_TUIC_TCP_POOL};
use crate::quic::honest_bound::TransportShape;

impl TuicClientConfig {
    /// The QUIC transport shape this Node installs and the A2 preflight bounds.
    pub fn transport_shape(&self) -> TransportShape {
        self.transport_shape.unwrap_or_else(|| {
            TransportShape::tuic(crate::quic::parse_mtu_policy(Some(&self.mtu_policy)).0)
        })
    }

    /// QUIC connections one Node may hold concurrently (its TCP pool slots).
    pub fn connection_count(&self) -> u32 {
        // Clamped to MAX_TUIC_TCP_POOL (16), so the cast cannot truncate.
        self.tcp_pool.clamp(MIN_TUIC_TCP_POOL, MAX_TUIC_TCP_POOL) as u32
    }

    /// Test-only injection of a smaller shape; production windows are unchanged.
    #[cfg(test)]
    pub(crate) fn with_transport_shape(mut self, shape: TransportShape) -> Self {
        self.transport_shape = Some(shape);
        self
    }
}

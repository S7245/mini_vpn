//! A2 honest per-Node memory bound for the third-party QUIC stack (Route 2).
//!
//! Owns: the configuration shape read from the same constants as
//! `quic_transport_config`, the checked bound formula, and the typed Service
//! preflight verdict. Does not own: measured amplification factors (supplied
//! by the A2 measurement harness, never estimated here), budget metering, or
//! the Quinn-internal Full failstop.
//!
//! Invariants: all arithmetic is checked; overflow is a typed error, never a
//! wrapped or saturated bound. Entry: `TransportShape::tuic`, `node_bound`,
//! `preflight`. Tests: `honest_bound/tests/mod.rs`.

use super::{
    quic_receive_window_bytes, quic_stream_receive_window_bytes, MtuPolicy,
    QUIC_MAX_CONCURRENT_BIDI_STREAMS, QUIC_MAX_CONCURRENT_UNI_STREAMS, QUIC_SEND_WINDOW_BYTES,
};

/// Quinn defaults the TUIC transport leaves untouched (quinn-proto 0.11.16
/// `TransportConfig::default`). The shape tracer pins them to the real config.
const QUINN_DATAGRAM_RX_DEFAULT: usize = 1_250_000;
const QUINN_DATAGRAM_TX_DEFAULT: usize = 1024 * 1024;
const QUINN_CRYPTO_BUFFER_DEFAULT: usize = 16 * 1024;
/// Initial, Handshake and Data packet spaces each own one crypto buffer.
const CRYPTO_SPACES: u64 = 3;

/// The configuration terms of one QUIC connection that bound its buffers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransportShape {
    pub bidi_streams: u32,
    pub uni_streams: u32,
    pub stream_window: u32,
    pub connection_window: u32,
    pub send_window: u64,
    pub datagram_rx: usize,
    pub datagram_tx: usize,
    pub crypto_buffer: usize,
}

impl TransportShape {
    /// Shape of the TUIC/Native Node transport built by `quic_transport_config`.
    pub fn tuic(mtu: MtuPolicy) -> Self {
        Self {
            bidi_streams: QUIC_MAX_CONCURRENT_BIDI_STREAMS,
            uni_streams: QUIC_MAX_CONCURRENT_UNI_STREAMS,
            stream_window: quic_stream_receive_window_bytes(mtu),
            connection_window: quic_receive_window_bytes(mtu),
            send_window: QUIC_SEND_WINDOW_BYTES,
            datagram_rx: QUINN_DATAGRAM_RX_DEFAULT,
            datagram_tx: QUINN_DATAGRAM_TX_DEFAULT,
            crypto_buffer: QUINN_CRYPTO_BUFFER_DEFAULT,
        }
    }
}

/// A rational factor applied with ceiling rounding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ratio {
    num: u32,
    den: u32,
}

impl Ratio {
    pub const ONE: Self = Self { num: 1, den: 1 };
    pub fn new(num: u32, den: u32) -> Option<Self> {
        (den != 0).then_some(Self { num, den })
    }
    fn apply(self, value: u64) -> Option<u64> {
        let scaled = u128::from(value).checked_mul(u128::from(self.num))?;
        u64::try_from(scaled.div_ceil(u128::from(self.den))).ok()
    }
}

/// Amplification factors and constants observed by the A2 measurement harness
/// (observed maximum times an explicit safety factor).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MeasuredFactors {
    pub rx: Ratio,
    pub tx: Ratio,
    pub datagram_rx: Ratio,
    pub per_connection: u64,
    pub per_endpoint: u64,
}

impl MeasuredFactors {
    /// Unit factors and zero constants: a lower bound, so a preflight using it
    /// is necessary-only until the A2 measurement supplies observed factors.
    /// This is not an estimate of amplification.
    pub const UNMEASURED_UNIT: Self = Self {
        rx: Ratio::ONE,
        tx: Ratio::ONE,
        datagram_rx: Ratio::ONE,
        per_connection: 0,
        per_endpoint: 0,
    };
}

impl MeasuredFactors {
    /// A2 step 3 measurement (2026-11-05, D17): observed maximum over the
    /// production and Service-test shapes of each honest term × 5/4 safety.
    /// Counting allocator on the real Node endpoint, adversary in a child
    /// process (`honest_bound/tests/amplification.rs`). Observed: rx 1.322
    /// (honest/reorder/duplicate/gap/rebind fills), tx 1.063 (coalesced
    /// writes), datagram rx 2.55 (64KiB window; 1.16 at 1.25MB), c_conn
    /// 1,786,098B, c_endpoint 47,330B. Adversarial layouts (1-byte frames
    /// over all streams, zero-length datagrams, 64B uplink writes) stay
    /// constant-bounded and metered but are outside this honest bound (Route 2:
    /// protocol Full there is that connection's failstop).
    pub const A2_MEASURED: Self = Self {
        rx: Ratio { num: 5, den: 3 },          // 1.322 × 1.25 = 1.652
        tx: Ratio { num: 4, den: 3 },          // 1.063 × 1.25 = 1.329
        datagram_rx: Ratio { num: 13, den: 4 }, // 2.55 × 1.25 = 3.19
        per_connection: 2_232_623,             // ceil(1,786,098 × 1.25)
        per_endpoint: 59_163,                  // ceil(47,330 × 1.25)
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundError {
    Overflow,
}

/// Honest upper bound of one Node with `connections` QUIC connections.
pub fn node_bound(
    shape: &TransportShape,
    factors: &MeasuredFactors,
    connections: u32,
) -> Result<u64, BoundError> {
    connection_bound(shape, factors)
        .and_then(|conn| conn.checked_mul(u64::from(connections)))
        .and_then(|sum| sum.checked_add(factors.per_endpoint))
        .ok_or(BoundError::Overflow)
}

fn connection_bound(shape: &TransportShape, factors: &MeasuredFactors) -> Option<u64> {
    // Locally opened streams (TUIC TCP Connect) also receive and are not
    // limited by the peer-initiated stream counts, so only the connection
    // window bounds received stream data.
    let receive = u64::from(shape.connection_window);
    let crypto = u64::try_from(shape.crypto_buffer)
        .ok()?
        .checked_mul(CRYPTO_SPACES)?;
    [
        factors.rx.apply(receive)?,
        factors.tx.apply(shape.send_window)?,
        factors.datagram_rx.apply(u64::try_from(shape.datagram_rx).ok()?)?,
        u64::try_from(shape.datagram_tx).ok()?,
        crypto,
        factors.per_connection,
    ]
    .into_iter()
    .try_fold(0u64, u64::checked_add)
}

/// Protocol-tier reserve for `generations` concurrent Node generations.
///
/// Soundness (honest load): App-tier admission and App reads of Quinn data
/// keep non-Quinn usage `N ≤ limit - P + B` (a read gated on a stale `used`
/// can move at most one fresh window `B`), and Quinn usage `Q ≤ B`, where
/// `B = generations × node`. Hence `P = 2B` makes honest Quinn Full
/// unreachable regardless of reader concurrency.
pub fn protocol_reserve(node: u64, generations: u32) -> Result<u64, BoundError> {
    node.checked_mul(u64::from(generations))
        .and_then(|b| b.checked_mul(2))
        .ok_or(BoundError::Overflow)
}

/// Typed Service preflight rejection: the configuration cannot be admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreflightRejection {
    OverBudget { required: u64, hard: u64 },
    Overflow,
}

/// `app_cold + Σ nodes ≤ hard`; returns the required bytes on admission.
pub fn preflight(hard: u64, app_cold: u64, nodes: &[u64]) -> Result<u64, PreflightRejection> {
    let required = nodes
        .iter()
        .try_fold(app_cold, |sum, node| sum.checked_add(*node))
        .ok_or(PreflightRejection::Overflow)?;
    if required > hard {
        return Err(PreflightRejection::OverBudget { required, hard });
    }
    Ok(required)
}

/// Configured counts and unit sizes of the Service's unmetered own-code
/// instances (amendment §5.3 `I_cfg`, D25). Built by the Service from its
/// real runtime configuration and `size_of` of the real types; never from
/// observed counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstanceShape {
    /// Listener socket cap of the selected capacity profile.
    pub listeners: usize,
    pub socket_rx: usize,
    pub socket_tx: usize,
    /// One `(SocketHandle, SocketCtx)` entry of the per-socket context map.
    pub socket_context_entry: usize,
    /// One smoltcp `SocketStorage` plus one registry `SocketHandle`.
    pub socket_storage_entry: usize,
    /// Root slots (F + W) and published Flows (F).
    pub flow_slots: usize,
    pub flows: usize,
    /// One boxed Root Flow row and one Root slot.
    pub flow_row: usize,
    pub flow_slot: usize,
    /// Fixed Root slot-occupancy index bytes (D66; one bit per slot).
    pub flow_index: usize,
}

/// Measured unit costs of the instance terms that have no `size_of` form.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstanceFactors {
    /// Unmetered per-Flow residual: task cells, role/actor futures, staged
    /// open and quinn per-stream state, pump custody (census in D25).
    pub per_flow: u64,
    /// Unmetered fixed Service/Node cost before the first Flow.
    pub per_service: u64,
}

impl InstanceFactors {
    /// D25 measurement (2026-11-05, F4096/W128/Cfull alone runs, tagged Node
    /// domain): observed maximum × 5/4. Census: every grown Node class is an
    /// integer multiple of F/F+W/sockets/5F tasks (configured counts only).
    /// See `D25_OBSERVED_*` in `tests/instances.rs`.
    pub const D25_MEASURED: Self = Self {
        per_flow: PER_FLOW_MEASURED,
        per_service: PER_SERVICE_MEASURED,
    };
}
// Observed per-Flow residual 57,675B (permit; native 57,363B): Node peak −
// charged peak − fixed − geometry at 4226 sockets/4224 rows, over F=4096.
const PER_FLOW_MEASURED: u64 = 72_094; // ceil(57,675 × 1.25)
// Observed fixed 5,559,702B (startup Node domain − charged).
const PER_SERVICE_MEASURED: u64 = 6_949_628; // ceil(5,559,702 × 1.25)

/// `I_cfg` by term; `total` is their checked sum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstanceBound {
    pub socket_payload: u64,
    pub socket_tables: u64,
    pub flow_rows: u64,
    pub flow_residual: u64,
    pub service: u64,
    pub total: u64,
}

/// Swiss-table allocation for `entries` live entries grown by insertion:
/// power-of-two buckets at 7/8 load, one control byte each plus one group.
fn hash_table_bytes(entries: u64, entry: u64) -> Option<u64> {
    const GROUP: u64 = 16;
    let buckets = if entries < 8 {
        8
    } else {
        entries.checked_mul(8)?.div_ceil(7).checked_next_power_of_two()?
    };
    let data = buckets.checked_mul(entry)?.checked_next_multiple_of(GROUP)?;
    data.checked_add(buckets)?.checked_add(GROUP)
}

/// Configured upper bound of the unmetered own-code instances (`I_cfg`).
pub fn instance_bound(
    shape: &InstanceShape,
    factors: &InstanceFactors,
) -> Result<InstanceBound, BoundError> {
    let n = |v: usize| u64::try_from(v).ok();
    let bound = (|| {
        let listeners = n(shape.listeners)?;
        let socket_payload = listeners.checked_mul(n(shape.socket_rx)?.checked_add(n(shape.socket_tx)?)?)?;
        let storage = listeners.checked_next_power_of_two()?.checked_mul(n(shape.socket_storage_entry)?)?;
        let socket_tables = hash_table_bytes(listeners, n(shape.socket_context_entry)?)?.checked_add(storage)?;
        let flow_rows = n(shape.flow_slots)?
            .checked_mul(n(shape.flow_row)?.checked_add(n(shape.flow_slot)?)?)?
            .checked_add(n(shape.flow_index)?)?;
        let flow_residual = n(shape.flows)?.checked_mul(factors.per_flow)?;
        let total = [socket_payload, socket_tables, flow_rows, flow_residual, factors.per_service]
            .into_iter()
            .try_fold(0u64, u64::checked_add)?;
        Some(InstanceBound { socket_payload, socket_tables, flow_rows, flow_residual, service: factors.per_service, total })
    })();
    bound.ok_or(BoundError::Overflow)
}

/// Separate total-memory preflight: metered hard budget plus `I_cfg`.
pub fn memory_bound(hard: u64, instances: u64) -> Result<u64, PreflightRejection> {
    hard.checked_add(instances).ok_or(PreflightRejection::Overflow)
}

#[cfg(test)]
pub(crate) mod tests;

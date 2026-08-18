//! Pure, transport-independent replay-capacity derivation.
//!
//! Rates are application bits per second. The global limit is derived from an
//! aggregate directional rate, not by multiplying a per-flow limit by the
//! number of flows. [`ReplayStoragePlan`] is the checked bridge from those
//! `u64` arithmetic limits to platform-sized byte/segment limits. Its record
//! width and flow count are injected: this module characterizes a logical
//! coalescing policy without selecting a production chunk size. Physical TCP
//! ownership bounds remain safe for any legal DATA allocation width, not only
//! the selected coalescing width.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

use super::protocol::MAX_DATA_PAYLOAD_BYTES;
use super::tcp::RETAINED_SLICE_COMPACT_RATIO;

const BITS_PER_BYTE_TIMES_NANOS_PER_SECOND: u128 = 8_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BitsPerSecond(NonZeroU64);

impl BitsPerSecond {
    pub fn new(value: u64) -> Result<Self, ReplayCapacityError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(ReplayCapacityError::ZeroRate)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectionalRates {
    client_to_target: BitsPerSecond,
    target_to_client: BitsPerSecond,
}

impl DirectionalRates {
    pub const fn new(client_to_target: BitsPerSecond, target_to_client: BitsPerSecond) -> Self {
        Self {
            client_to_target,
            target_to_client,
        }
    }

    pub const fn symmetric(rate: BitsPerSecond) -> Self {
        Self::new(rate, rate)
    }

    pub const fn client_to_target(self) -> BitsPerSecond {
        self.client_to_target
    }

    pub const fn target_to_client(self) -> BitsPerSecond {
        self.target_to_client
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayHorizon {
    blackout_budget: Duration,
    max_normal_ack_age: Duration,
    effective: Duration,
}

impl ReplayHorizon {
    pub fn new(
        blackout_budget: Duration,
        max_normal_ack_age: Duration,
    ) -> Result<Self, ReplayCapacityError> {
        let effective = blackout_budget
            .checked_add(max_normal_ack_age)
            .ok_or(ReplayCapacityError::HorizonOverflow)?;
        if effective.is_zero() {
            return Err(ReplayCapacityError::ZeroHorizon);
        }
        Ok(Self {
            blackout_budget,
            max_normal_ack_age,
            effective,
        })
    }

    pub const fn blackout_budget(self) -> Duration {
        self.blackout_budget
    }

    pub const fn max_normal_ack_age(self) -> Duration {
        self.max_normal_ack_age
    }

    pub const fn effective(self) -> Duration {
        self.effective
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectionalByteLimits {
    client_to_target_bytes: u64,
    target_to_client_bytes: u64,
}

impl DirectionalByteLimits {
    pub const fn client_to_target_bytes(self) -> u64 {
        self.client_to_target_bytes
    }

    pub const fn target_to_client_bytes(self) -> u64 {
        self.target_to_client_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayCapacitySpec {
    per_flow: DirectionalRates,
    aggregate: DirectionalRates,
    horizon: ReplayHorizon,
}

impl ReplayCapacitySpec {
    pub fn new(
        per_flow: DirectionalRates,
        aggregate: DirectionalRates,
        blackout_budget: Duration,
        max_normal_ack_age: Duration,
    ) -> Result<Self, ReplayCapacityError> {
        if per_flow.client_to_target().get() > aggregate.client_to_target().get()
            || per_flow.target_to_client().get() > aggregate.target_to_client().get()
        {
            return Err(ReplayCapacityError::PerFlowRateExceedsAggregate {
                per_flow,
                aggregate,
            });
        }
        Ok(Self {
            per_flow,
            aggregate,
            horizon: ReplayHorizon::new(blackout_budget, max_normal_ack_age)?,
        })
    }

    pub const fn per_flow_rates(self) -> DirectionalRates {
        self.per_flow
    }

    pub const fn aggregate_rates(self) -> DirectionalRates {
        self.aggregate
    }

    pub const fn horizon(self) -> ReplayHorizon {
        self.horizon
    }

    pub fn derive(self) -> Result<ReplayCapacityPlan, ReplayCapacityError> {
        let per_flow = derive_directional(self.per_flow, self.horizon.effective())?;
        let global = derive_directional(self.aggregate, self.horizon.effective())?;
        let per_flow_full_duplex_bytes = per_flow
            .client_to_target_bytes
            .checked_add(per_flow.target_to_client_bytes)
            .ok_or(ReplayCapacityError::PerFlowFullDuplexCapacityExceedsU64)?;
        let global_full_duplex_bytes = global
            .client_to_target_bytes
            .checked_add(global.target_to_client_bytes)
            .ok_or(ReplayCapacityError::GlobalFullDuplexCapacityExceedsU64)?;
        Ok(ReplayCapacityPlan {
            horizon: self.horizon,
            per_flow,
            global,
            per_flow_full_duplex_bytes,
            global_full_duplex_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayCapacityPlan {
    horizon: ReplayHorizon,
    per_flow: DirectionalByteLimits,
    global: DirectionalByteLimits,
    per_flow_full_duplex_bytes: u64,
    global_full_duplex_bytes: u64,
}

impl ReplayCapacityPlan {
    pub const fn horizon(self) -> ReplayHorizon {
        self.horizon
    }

    pub const fn per_flow(self) -> DirectionalByteLimits {
        self.per_flow
    }

    pub const fn global(self) -> DirectionalByteLimits {
        self.global
    }

    pub const fn per_flow_full_duplex_bytes(self) -> u64 {
        self.per_flow_full_duplex_bytes
    }

    pub const fn global_full_duplex_bytes(self) -> u64 {
        self.global_full_duplex_bytes
    }

    /// Converts arithmetic byte capacities into checked storage limits.
    ///
    /// The caller must preserve [`ReplayStorageGeometry`]'s coalescing
    /// contract: each live flow has at most one underfilled retained record;
    /// every other retained record has exactly the configured payload width.
    /// A partially acknowledged/accepted replay head counts as that one
    /// underfilled record. This makes the global record allowance additive in
    /// `max_flows`; it is never derived as `per_flow * max_flows`.
    pub fn storage_plan(
        self,
        geometry: ReplayStorageGeometry,
    ) -> Result<ReplayStoragePlan, ReplayCapacityError> {
        self.storage_plan_with_usize_max(geometry, usize::MAX as u128)
    }

    fn storage_plan_with_usize_max(
        self,
        geometry: ReplayStorageGeometry,
        usize_max: u128,
    ) -> Result<ReplayStoragePlan, ReplayCapacityError> {
        let per_flow =
            derive_directional_storage(self.per_flow, geometry, 1, usize_max, "per-flow")?;
        let global = derive_directional_storage(
            self.global,
            geometry,
            geometry.max_flows,
            usize_max,
            "global",
        )?;
        Ok(ReplayStoragePlan {
            geometry,
            per_flow,
            global,
        })
    }
}

/// Injected logical admission geometry for one storage-plan derivation.
///
/// `coalesced_record_payload_bytes` is a policy input, not a tuned constant.
/// It may not exceed the protocol's [`MAX_DATA_PAYLOAD_BYTES`]. The producer
/// must retain at most one underfilled record per live flow; malicious or
/// non-coalesced traffic that exhausts the resulting record/range limit fails
/// closed rather than silently increasing memory. This width controls logical
/// segment admission/coalescing only: legal ownership may still arrive in any
/// allocation up to `min(MAX_DATA_PAYLOAD_BYTES, directional max_bytes)`, so
/// persistent-backing and compaction bounds use that larger physical width.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayStorageGeometry {
    coalesced_record_payload_bytes: usize,
    max_flows: usize,
}

impl ReplayStorageGeometry {
    pub fn new(
        coalesced_record_payload_bytes: usize,
        max_flows: usize,
    ) -> Result<Self, ReplayCapacityError> {
        if coalesced_record_payload_bytes == 0 {
            return Err(ReplayCapacityError::ZeroCoalescedRecordPayload);
        }
        if coalesced_record_payload_bytes > MAX_DATA_PAYLOAD_BYTES {
            return Err(ReplayCapacityError::CoalescedRecordPayloadTooLarge {
                actual: coalesced_record_payload_bytes,
                max: MAX_DATA_PAYLOAD_BYTES,
            });
        }
        if max_flows == 0 {
            return Err(ReplayCapacityError::ZeroStorageFlowCapacity);
        }
        Ok(Self {
            coalesced_record_payload_bytes,
            max_flows,
        })
    }

    pub const fn coalesced_record_payload_bytes(self) -> usize {
        self.coalesced_record_payload_bytes
    }

    pub const fn max_flows(self) -> usize {
        self.max_flows
    }
}

/// Copy/allocation upper bounds for filling and then draining one full replay
/// window without replenishment.
///
/// These are not lifetime counters: an unbounded application stream can fill
/// and drain many windows. At the TCP ownership layer, new ownership copies
/// every byte exactly once. Replay/peek views are zero-copy and therefore
/// absent. Geometric retained-suffix compaction contributes the separately
/// bounded drain cost. Protocol decoding and wire serialization each add
/// their own copy outside these TCP-layer-local bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayCopyAllocationBounds {
    ownership_copy_bytes_per_window_fill: usize,
    ownership_allocations_per_window_fill: usize,
    compaction_copy_bytes_per_window_drain: usize,
    compaction_allocations_per_window_drain: usize,
}

impl ReplayCopyAllocationBounds {
    pub const fn ownership_copy_bytes_per_window_fill(self) -> usize {
        self.ownership_copy_bytes_per_window_fill
    }

    pub const fn ownership_allocations_per_window_fill(self) -> usize {
        self.ownership_allocations_per_window_fill
    }

    pub const fn compaction_copy_bytes_per_window_drain(self) -> usize {
        self.compaction_copy_bytes_per_window_drain
    }

    pub const fn compaction_allocations_per_window_drain(self) -> usize {
        self.compaction_allocations_per_window_drain
    }
}

/// Platform-sized limits for one replay direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayStorageLimit {
    max_bytes: usize,
    max_segments: usize,
    tail_segment_allowance: usize,
    max_persistent_backing_bytes: usize,
    additive_tail_backing_bytes: usize,
    copy_allocation: ReplayCopyAllocationBounds,
}

impl ReplayStorageLimit {
    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    pub const fn max_segments(self) -> usize {
        self.max_segments
    }

    /// Number of live-flow tails added to full-record capacity.
    pub const fn tail_segment_allowance(self) -> usize {
        self.tail_segment_allowance
    }

    /// Maximum live backing under TCP's strict `< 4x` compaction invariant.
    pub const fn max_persistent_backing_bytes(self) -> usize {
        self.max_persistent_backing_bytes
    }

    /// Flow-additive backing above the logical byte limit.
    pub const fn additive_tail_backing_bytes(self) -> usize {
        self.additive_tail_backing_bytes
    }

    pub const fn copy_allocation(self) -> ReplayCopyAllocationBounds {
        self.copy_allocation
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DirectionalReplayStorageLimits {
    client_to_target: ReplayStorageLimit,
    target_to_client: ReplayStorageLimit,
}

impl DirectionalReplayStorageLimits {
    pub const fn client_to_target(self) -> ReplayStorageLimit {
        self.client_to_target
    }

    pub const fn target_to_client(self) -> ReplayStorageLimit {
        self.target_to_client
    }
}

/// Checked byte, segment, backing, and copy/allocation characterization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplayStoragePlan {
    geometry: ReplayStorageGeometry,
    per_flow: DirectionalReplayStorageLimits,
    global: DirectionalReplayStorageLimits,
}

impl ReplayStoragePlan {
    pub const fn geometry(self) -> ReplayStorageGeometry {
        self.geometry
    }

    pub const fn per_flow(self) -> DirectionalReplayStorageLimits {
        self.per_flow
    }

    pub const fn global(self) -> DirectionalReplayStorageLimits {
        self.global
    }
}

fn derive_directional_storage(
    bytes: DirectionalByteLimits,
    geometry: ReplayStorageGeometry,
    flow_slots: usize,
    usize_max: u128,
    scope: &'static str,
) -> Result<DirectionalReplayStorageLimits, ReplayCapacityError> {
    Ok(DirectionalReplayStorageLimits {
        client_to_target: derive_storage_limit(
            bytes.client_to_target_bytes,
            geometry,
            flow_slots,
            usize_max,
            match scope {
                "per-flow" => "per-flow client-to-target storage",
                _ => "global client-to-target storage",
            },
        )?,
        target_to_client: derive_storage_limit(
            bytes.target_to_client_bytes,
            geometry,
            flow_slots,
            usize_max,
            match scope {
                "per-flow" => "per-flow target-to-client storage",
                _ => "global target-to-client storage",
            },
        )?,
    })
}

fn derive_storage_limit(
    max_bytes: u64,
    geometry: ReplayStorageGeometry,
    flow_slots: usize,
    usize_max: u128,
    field: &'static str,
) -> Result<ReplayStorageLimit, ReplayCapacityError> {
    let max_bytes = u128::from(max_bytes);
    let record_bytes = geometry.coalesced_record_payload_bytes as u128;
    let flow_slots = flow_slots as u128;

    // With at most one underfilled retained record per live flow, allocate one
    // tail slot for each possible non-empty flow and fill the rest with exact
    // coalesced records. This is additive in flow count, not a product of the
    // per-flow plan.
    let tail_segment_allowance = flow_slots.min(max_bytes);
    let full_segments = max_bytes
        .checked_sub(tail_segment_allowance)
        .ok_or(ReplayCapacityError::StorageArithmeticOverflow { field })?
        / record_bytes;
    let max_segments = full_segments
        .checked_add(tail_segment_allowance)
        .ok_or(ReplayCapacityError::StorageArithmeticOverflow { field })?;

    // The injected coalescing width only bounds logical segment admission.
    // A legal DATA record (or direct TCP ownership API caller) may allocate up
    // to the protocol maximum, bounded by this direction's logical capacity.
    // Use that physical width for backing and compaction bounds.
    let max_owned_segment_bytes = (MAX_DATA_PAYLOAD_BYTES as u128).min(max_bytes);

    // Only one retained prefix allocation per live flow can carry slice
    // overhead. The geometric policy compacts when live <= backing / ratio,
    // so the largest per-flow overhead occurs one byte above that threshold.
    let per_tail_backing = max_owned_segment_bytes
        .checked_sub(max_owned_segment_bytes / RETAINED_SLICE_COMPACT_RATIO as u128)
        .and_then(|value| value.checked_sub(1))
        .ok_or(ReplayCapacityError::StorageArithmeticOverflow { field })?;
    let raw_additive_tail_backing = tail_segment_allowance
        .checked_mul(per_tail_backing)
        .ok_or(ReplayCapacityError::StorageArithmeticOverflow { field })?;
    let strict_ratio_overhead = max_bytes
        .checked_mul((RETAINED_SLICE_COMPACT_RATIO - 1) as u128)
        .and_then(|value| value.checked_sub(1))
        .ok_or(ReplayCapacityError::StorageArithmeticOverflow { field })?;
    let additive_tail_backing_bytes = raw_additive_tail_backing.min(strict_ratio_overhead);
    let max_persistent_backing_bytes = max_bytes
        .checked_add(additive_tail_backing_bytes)
        .ok_or(ReplayCapacityError::StorageArithmeticOverflow { field })?;

    let compaction_copy_bytes =
        geometric_compaction_copy_bound(max_bytes, max_owned_segment_bytes)?;
    let compaction_allocations =
        geometric_compaction_allocation_bound(max_bytes, max_owned_segment_bytes, max_segments)?;

    let max_bytes = storage_usize(max_bytes, usize_max, field)?;
    let max_segments = storage_usize(max_segments, usize_max, field)?;
    let tail_segment_allowance = storage_usize(tail_segment_allowance, usize_max, field)?;
    let additive_tail_backing_bytes = storage_usize(additive_tail_backing_bytes, usize_max, field)?;
    let max_persistent_backing_bytes =
        storage_usize(max_persistent_backing_bytes, usize_max, field)?;
    let compaction_copy_bytes = storage_usize(compaction_copy_bytes, usize_max, field)?;
    let compaction_allocations = storage_usize(compaction_allocations, usize_max, field)?;

    Ok(ReplayStorageLimit {
        max_bytes,
        max_segments,
        tail_segment_allowance,
        max_persistent_backing_bytes,
        additive_tail_backing_bytes,
        copy_allocation: ReplayCopyAllocationBounds {
            ownership_copy_bytes_per_window_fill: max_bytes,
            ownership_allocations_per_window_fill: max_segments,
            compaction_copy_bytes_per_window_drain: compaction_copy_bytes,
            compaction_allocations_per_window_drain: compaction_allocations,
        },
    })
}

fn geometric_compaction_copy_bound(
    max_bytes: u128,
    max_segment_bytes: u128,
) -> Result<u128, ReplayCapacityError> {
    let mut divisor = RETAINED_SLICE_COMPACT_RATIO as u128;
    let mut total = 0u128;
    while divisor <= max_segment_bytes {
        total = total.checked_add(max_bytes / divisor).ok_or(
            ReplayCapacityError::StorageArithmeticOverflow {
                field: "geometric compaction copy bound",
            },
        )?;
        divisor = divisor
            .checked_mul(RETAINED_SLICE_COMPACT_RATIO as u128)
            .ok_or(ReplayCapacityError::StorageArithmeticOverflow {
                field: "geometric compaction copy divisor",
            })?;
    }
    Ok(total)
}

fn geometric_compaction_allocation_bound(
    max_bytes: u128,
    max_segment_bytes: u128,
    max_segments: u128,
) -> Result<u128, ReplayCapacityError> {
    let mut divisor = RETAINED_SLICE_COMPACT_RATIO as u128;
    let mut total = 0u128;
    while divisor <= max_segment_bytes {
        total = total
            .checked_add(max_segments.min(max_bytes / divisor))
            .ok_or(ReplayCapacityError::StorageArithmeticOverflow {
                field: "geometric compaction allocation bound",
            })?;
        divisor = divisor
            .checked_mul(RETAINED_SLICE_COMPACT_RATIO as u128)
            .ok_or(ReplayCapacityError::StorageArithmeticOverflow {
                field: "geometric compaction allocation divisor",
            })?;
    }
    Ok(total)
}

fn storage_usize(
    value: u128,
    usize_max: u128,
    field: &'static str,
) -> Result<usize, ReplayCapacityError> {
    let effective_max = usize_max.min(usize::MAX as u128);
    if value > effective_max {
        return Err(ReplayCapacityError::StorageValueExceedsUsize {
            field,
            value,
            usize_max: effective_max,
        });
    }
    usize::try_from(value).map_err(|_| ReplayCapacityError::StorageValueExceedsUsize {
        field,
        value,
        usize_max: effective_max,
    })
}

fn derive_directional(
    rates: DirectionalRates,
    horizon: Duration,
) -> Result<DirectionalByteLimits, ReplayCapacityError> {
    Ok(DirectionalByteLimits {
        client_to_target_bytes: retained_bytes(rates.client_to_target(), horizon)?,
        target_to_client_bytes: retained_bytes(rates.target_to_client(), horizon)?,
    })
}

fn retained_bytes(rate: BitsPerSecond, horizon: Duration) -> Result<u64, ReplayCapacityError> {
    let numerator = u128::from(rate.get())
        .checked_mul(horizon.as_nanos())
        .ok_or(ReplayCapacityError::RateHorizonProductOverflow)?;
    let quotient = numerator / BITS_PER_BYTE_TIMES_NANOS_PER_SECOND;
    let rounded = quotient
        .checked_add(u128::from(
            numerator % BITS_PER_BYTE_TIMES_NANOS_PER_SECOND != 0,
        ))
        .ok_or(ReplayCapacityError::RateHorizonProductOverflow)?;
    u64::try_from(rounded).map_err(|_| ReplayCapacityError::CapacityExceedsU64)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayCapacityError {
    ZeroRate,
    ZeroHorizon,
    ZeroCoalescedRecordPayload,
    CoalescedRecordPayloadTooLarge {
        actual: usize,
        max: usize,
    },
    ZeroStorageFlowCapacity,
    HorizonOverflow,
    RateHorizonProductOverflow,
    CapacityExceedsU64,
    PerFlowFullDuplexCapacityExceedsU64,
    GlobalFullDuplexCapacityExceedsU64,
    PerFlowRateExceedsAggregate {
        per_flow: DirectionalRates,
        aggregate: DirectionalRates,
    },
    StorageArithmeticOverflow {
        field: &'static str,
    },
    StorageValueExceedsUsize {
        field: &'static str,
        value: u128,
        usize_max: u128,
    },
}

impl fmt::Display for ReplayCapacityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRate => formatter.write_str("replay rate must be positive"),
            Self::ZeroHorizon => formatter.write_str("effective replay horizon must be positive"),
            Self::ZeroCoalescedRecordPayload => {
                formatter.write_str("coalesced replay record payload must be positive")
            }
            Self::CoalescedRecordPayloadTooLarge { actual, max } => write!(
                formatter,
                "coalesced replay record payload {actual} exceeds protocol maximum {max}"
            ),
            Self::ZeroStorageFlowCapacity => {
                formatter.write_str("replay storage flow capacity must be positive")
            }
            Self::HorizonOverflow => {
                formatter.write_str("replay blackout and ACK-age horizons overflow Duration")
            }
            Self::RateHorizonProductOverflow => {
                formatter.write_str("replay rate and horizon product exceeds u128")
            }
            Self::CapacityExceedsU64 => {
                formatter.write_str("derived replay capacity exceeds u64 bytes")
            }
            Self::PerFlowFullDuplexCapacityExceedsU64 => {
                formatter.write_str("per-flow full-duplex replay capacity exceeds u64 bytes")
            }
            Self::GlobalFullDuplexCapacityExceedsU64 => {
                formatter.write_str("global full-duplex replay capacity exceeds u64 bytes")
            }
            Self::PerFlowRateExceedsAggregate { .. } => {
                formatter.write_str("per-flow replay rate exceeds aggregate replay rate")
            }
            Self::StorageArithmeticOverflow { field } => {
                write!(formatter, "replay storage arithmetic overflow for {field}")
            }
            Self::StorageValueExceedsUsize {
                field,
                value,
                usize_max,
            } => write!(
                formatter,
                "replay storage {field} value {value} exceeds platform usize maximum {usize_max}"
            ),
        }
    }
}

impl Error for ReplayCapacityError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resumable::protocol::ByteOffset;
    use crate::resumable::tcp::{TcpSendWindow, TcpWindowLimits};
    use bytes::Bytes;
    use std::time::Duration;

    fn symmetric_rates(mbit_per_second: u64) -> DirectionalRates {
        let rate = BitsPerSecond::new(mbit_per_second * 1_000_000).unwrap();
        DirectionalRates::symmetric(rate)
    }

    #[test]
    fn capacity_uses_decimal_mbit_and_exact_500ms_horizon() {
        let spec = ReplayCapacitySpec::new(
            symmetric_rates(100),
            symmetric_rates(100),
            Duration::from_millis(500),
            Duration::ZERO,
        )
        .unwrap();

        let plan = spec.derive().unwrap();

        assert_eq!(plan.per_flow().client_to_target_bytes(), 6_250_000);
        assert_eq!(plan.per_flow().target_to_client_bytes(), 6_250_000);
        assert_eq!(plan.per_flow_full_duplex_bytes(), 12_500_000);
        assert_eq!(plan.global(), plan.per_flow());
        assert_eq!(plan.global_full_duplex_bytes(), 12_500_000);
    }

    #[test]
    fn capacity_matches_the_100_170_and_240_mbit_characterization() {
        for (mbit, per_direction, full_duplex) in [
            (100, 6_250_000, 12_500_000),
            (170, 10_625_000, 21_250_000),
            (240, 15_000_000, 30_000_000),
        ] {
            let plan = ReplayCapacitySpec::new(
                symmetric_rates(mbit),
                symmetric_rates(mbit),
                Duration::from_millis(500),
                Duration::ZERO,
            )
            .unwrap()
            .derive()
            .unwrap();

            assert_eq!(plan.per_flow().client_to_target_bytes(), per_direction);
            assert_eq!(plan.per_flow().target_to_client_bytes(), per_direction);
            assert_eq!(plan.per_flow_full_duplex_bytes(), full_duplex);
            assert_eq!(plan.global_full_duplex_bytes(), full_duplex);
        }
    }

    #[test]
    fn storage_plan_matches_100_170_and_240_mbit_record_geometry() {
        let geometry = ReplayStorageGeometry::new(MAX_DATA_PAYLOAD_BYTES, 1).unwrap();

        for (mbit, max_bytes, max_segments, max_backing, compaction_copy) in [
            (100, 6_250_000, 96, 6_299_151, 2_083_299),
            (170, 10_625_000, 163, 10_674_151, 3_541_608),
            (240, 15_000_000, 229, 15_049_151, 4_999_921),
        ] {
            let capacity = ReplayCapacitySpec::new(
                symmetric_rates(mbit),
                symmetric_rates(mbit),
                Duration::from_millis(500),
                Duration::ZERO,
            )
            .unwrap()
            .derive()
            .unwrap();
            let storage = capacity.storage_plan(geometry).unwrap();
            let per_flow = storage.per_flow().client_to_target();

            assert_eq!(per_flow.max_bytes(), max_bytes);
            assert_eq!(per_flow.max_segments(), max_segments);
            assert_eq!(per_flow.tail_segment_allowance(), 1);
            assert_eq!(per_flow.additive_tail_backing_bytes(), 49_151);
            assert_eq!(per_flow.max_persistent_backing_bytes(), max_backing);
            assert_eq!(
                per_flow
                    .copy_allocation()
                    .compaction_copy_bytes_per_window_drain(),
                compaction_copy
            );
            assert_eq!(storage.per_flow(), storage.global());
        }
    }

    #[test]
    fn global_segments_and_backing_use_additive_flow_tails_not_per_flow_product() {
        let capacity = ReplayCapacitySpec::new(
            symmetric_rates(100),
            symmetric_rates(240),
            Duration::from_millis(500),
            Duration::ZERO,
        )
        .unwrap()
        .derive()
        .unwrap();
        let storage = capacity
            .storage_plan(ReplayStorageGeometry::new(MAX_DATA_PAYLOAD_BYTES, 4).unwrap())
            .unwrap();
        let per_flow = storage.per_flow().client_to_target();
        let global = storage.global().client_to_target();

        assert_eq!(per_flow.max_bytes(), 6_250_000);
        assert_eq!(per_flow.max_segments(), 96);
        assert_eq!(global.max_bytes(), 15_000_000);
        assert_eq!(global.tail_segment_allowance(), 4);
        assert_eq!(global.max_segments(), 232);
        assert_ne!(global.max_segments(), per_flow.max_segments() * 4);
        assert_eq!(global.additive_tail_backing_bytes(), 196_604);
        assert_eq!(global.max_persistent_backing_bytes(), 15_196_604);
        assert!(global.max_persistent_backing_bytes() < 4 * global.max_bytes());
    }

    #[test]
    fn storage_plan_documents_one_copy_and_geometric_compaction_cost() {
        let per_flow = DirectionalRates::symmetric(BitsPerSecond::new(128).unwrap());
        let aggregate = DirectionalRates::symmetric(BitsPerSecond::new(256).unwrap());
        let capacity =
            ReplayCapacitySpec::new(per_flow, aggregate, Duration::from_secs(1), Duration::ZERO)
                .unwrap()
                .derive()
                .unwrap();
        let storage = capacity
            .storage_plan(ReplayStorageGeometry::new(16, 2).unwrap())
            .unwrap();

        let per_flow = storage.per_flow().client_to_target();
        assert_eq!(per_flow.max_bytes(), 16);
        assert_eq!(per_flow.max_segments(), 1);
        assert_eq!(per_flow.additive_tail_backing_bytes(), 11);
        assert_eq!(per_flow.max_persistent_backing_bytes(), 27);
        assert_eq!(
            per_flow
                .copy_allocation()
                .ownership_copy_bytes_per_window_fill(),
            16
        );
        assert_eq!(
            per_flow
                .copy_allocation()
                .ownership_allocations_per_window_fill(),
            1
        );
        assert_eq!(
            per_flow
                .copy_allocation()
                .compaction_copy_bytes_per_window_drain(),
            5
        );
        assert_eq!(
            per_flow
                .copy_allocation()
                .compaction_allocations_per_window_drain(),
            2
        );

        let global = storage.global().client_to_target();
        assert_eq!(global.max_bytes(), 32);
        assert_eq!(global.max_segments(), 3);
        assert_eq!(global.tail_segment_allowance(), 2);
        assert_eq!(global.additive_tail_backing_bytes(), 46);
        assert_eq!(global.max_persistent_backing_bytes(), 78);
        assert_eq!(
            global
                .copy_allocation()
                .ownership_copy_bytes_per_window_fill(),
            32
        );
        assert_eq!(
            global
                .copy_allocation()
                .ownership_allocations_per_window_fill(),
            3
        );
        assert_eq!(
            global
                .copy_allocation()
                .compaction_copy_bytes_per_window_drain(),
            10
        );
        assert_eq!(
            global
                .copy_allocation()
                .compaction_allocations_per_window_drain(),
            5
        );
    }

    #[test]
    fn physical_bounds_use_the_largest_legal_ownership_not_coalescer_width() {
        let rates = DirectionalRates::symmetric(BitsPerSecond::new(512).unwrap());
        let capacity =
            ReplayCapacitySpec::new(rates, rates, Duration::from_secs(1), Duration::ZERO)
                .unwrap()
                .derive()
                .unwrap();
        let storage = capacity
            .storage_plan(ReplayStorageGeometry::new(16, 1).unwrap())
            .unwrap();
        let limit = storage.per_flow().client_to_target();

        // A legal 64-byte TCP ownership allocation may drain by 47 bytes,
        // retain its 64-byte backing with 17 live bytes, and then refill the
        // released 47-byte logical capacity. Persistent backing is 64 + 47.
        assert_eq!(limit.max_bytes(), 64);
        assert_eq!(limit.max_segments(), 4);
        assert_eq!(limit.additive_tail_backing_bytes(), 47);
        assert_eq!(limit.max_persistent_backing_bytes(), 111);

        let mut ownership = TcpSendWindow::new(
            TcpWindowLimits::new(limit.max_bytes(), limit.max_segments()).unwrap(),
        );
        ownership
            .append(ByteOffset::new(0), Bytes::from(vec![0x5a; 64]))
            .unwrap();
        let drain = ownership.acknowledge(ByteOffset::new(47), false).unwrap();
        assert_eq!(drain.compaction_bytes_copied(), 0);
        ownership
            .append(ByteOffset::new(64), Bytes::from(vec![0xa5; 47]))
            .unwrap();
        let actual_backing = ownership.snapshot().retained_backing_bytes();
        assert_eq!(actual_backing, 111);
        assert!(actual_backing <= limit.max_persistent_backing_bytes());

        // Repeated geometric compaction of one legal 64-byte allocation copies
        // at most 16 + 4 + 1 bytes. The 16-byte coalescer width must not erase
        // the final legal-allocation level.
        let actual_compaction_copy = [48, 60, 63, 64]
            .into_iter()
            .map(|accepted| {
                ownership
                    .acknowledge(ByteOffset::new(accepted), false)
                    .unwrap()
                    .compaction_bytes_copied()
            })
            .sum::<usize>();
        assert_eq!(actual_compaction_copy, 21);
        assert!(
            actual_compaction_copy
                <= limit
                    .copy_allocation()
                    .compaction_copy_bytes_per_window_drain()
        );
        assert!(
            limit
                .copy_allocation()
                .compaction_allocations_per_window_drain()
                >= 3
        );
    }

    #[test]
    fn storage_geometry_rejects_zero_oversized_and_zero_flow_inputs() {
        assert_eq!(
            ReplayStorageGeometry::new(0, 1),
            Err(ReplayCapacityError::ZeroCoalescedRecordPayload)
        );
        assert_eq!(
            ReplayStorageGeometry::new(MAX_DATA_PAYLOAD_BYTES + 1, 1),
            Err(ReplayCapacityError::CoalescedRecordPayloadTooLarge {
                actual: MAX_DATA_PAYLOAD_BYTES + 1,
                max: MAX_DATA_PAYLOAD_BYTES,
            })
        );
        assert_eq!(
            ReplayStorageGeometry::new(MAX_DATA_PAYLOAD_BYTES, 0),
            Err(ReplayCapacityError::ZeroStorageFlowCapacity)
        );
    }

    #[test]
    fn simulated_32_bit_target_rejects_payload_and_derived_backing_overflow() {
        let payload_overflow = ReplayCapacitySpec::new(
            symmetric_rates(40_000),
            symmetric_rates(40_000),
            Duration::from_secs(1),
            Duration::ZERO,
        )
        .unwrap()
        .derive()
        .unwrap();
        assert_eq!(
            payload_overflow.storage_plan_with_usize_max(
                ReplayStorageGeometry::new(MAX_DATA_PAYLOAD_BYTES, 1).unwrap(),
                u32::MAX as u128,
            ),
            Err(ReplayCapacityError::StorageValueExceedsUsize {
                field: "per-flow client-to-target storage",
                value: 5_000_000_000,
                usize_max: u32::MAX as u128,
            })
        );

        let backing_overflow = ReplayCapacitySpec::new(
            symmetric_rates(32_000),
            symmetric_rates(32_000),
            Duration::from_secs(1),
            Duration::ZERO,
        )
        .unwrap()
        .derive()
        .unwrap();
        assert_eq!(
            backing_overflow.storage_plan_with_usize_max(
                ReplayStorageGeometry::new(MAX_DATA_PAYLOAD_BYTES, 65_536).unwrap(),
                u32::MAX as u128,
            ),
            Err(ReplayCapacityError::StorageValueExceedsUsize {
                field: "global client-to-target storage",
                value: 7_221_159_936,
                usize_max: u32::MAX as u128,
            })
        );
    }

    #[test]
    fn effective_horizon_adds_blackout_and_normal_application_ack_age() {
        let plan = ReplayCapacitySpec::new(
            symmetric_rates(240),
            symmetric_rates(240),
            Duration::from_millis(500),
            Duration::from_millis(100),
        )
        .unwrap()
        .derive()
        .unwrap();

        assert_eq!(plan.horizon().blackout_budget(), Duration::from_millis(500));
        assert_eq!(
            plan.horizon().max_normal_ack_age(),
            Duration::from_millis(100)
        );
        assert_eq!(plan.horizon().effective(), Duration::from_millis(600));
        assert_eq!(plan.per_flow().client_to_target_bytes(), 18_000_000);
    }

    #[test]
    fn fractional_byte_capacity_rounds_up_without_floating_point() {
        let one_bit_per_second = BitsPerSecond::new(1).unwrap();
        let rates = DirectionalRates::symmetric(one_bit_per_second);
        let plan = ReplayCapacitySpec::new(rates, rates, Duration::from_nanos(1), Duration::ZERO)
            .unwrap()
            .derive()
            .unwrap();

        assert_eq!(plan.per_flow().client_to_target_bytes(), 1);
        assert_eq!(plan.per_flow().target_to_client_bytes(), 1);
        assert_eq!(plan.per_flow_full_duplex_bytes(), 2);
    }

    #[test]
    fn asymmetric_per_flow_and_global_limits_are_derived_independently() {
        let per_flow = DirectionalRates::new(
            BitsPerSecond::new(100_000_000).unwrap(),
            BitsPerSecond::new(170_000_000).unwrap(),
        );
        let aggregate = DirectionalRates::new(
            BitsPerSecond::new(240_000_000).unwrap(),
            BitsPerSecond::new(240_000_000).unwrap(),
        );
        let spec = ReplayCapacitySpec::new(
            per_flow,
            aggregate,
            Duration::from_millis(500),
            Duration::ZERO,
        )
        .unwrap();

        assert_eq!(spec.per_flow_rates(), per_flow);
        assert_eq!(spec.aggregate_rates(), aggregate);
        let plan = spec.derive().unwrap();
        assert_eq!(plan.per_flow().client_to_target_bytes(), 6_250_000);
        assert_eq!(plan.per_flow().target_to_client_bytes(), 10_625_000);
        assert_eq!(plan.per_flow_full_duplex_bytes(), 16_875_000);
        assert_eq!(plan.global().client_to_target_bytes(), 15_000_000);
        assert_eq!(plan.global().target_to_client_bytes(), 15_000_000);
        assert_eq!(plan.global_full_duplex_bytes(), 30_000_000);
    }

    #[test]
    fn per_flow_rate_cannot_exceed_aggregate_in_either_direction() {
        let low = BitsPerSecond::new(100_000_000).unwrap();
        let high = BitsPerSecond::new(170_000_000).unwrap();
        for per_flow in [
            DirectionalRates::new(high, low),
            DirectionalRates::new(low, high),
        ] {
            let aggregate = DirectionalRates::symmetric(low);
            assert_eq!(
                ReplayCapacitySpec::new(
                    per_flow,
                    aggregate,
                    Duration::from_millis(500),
                    Duration::ZERO,
                ),
                Err(ReplayCapacityError::PerFlowRateExceedsAggregate {
                    per_flow,
                    aggregate,
                })
            );
        }
    }

    #[test]
    fn zero_rate_and_zero_effective_horizon_are_rejected() {
        assert_eq!(BitsPerSecond::new(0), Err(ReplayCapacityError::ZeroRate));
        assert_eq!(
            ReplayHorizon::new(Duration::ZERO, Duration::ZERO),
            Err(ReplayCapacityError::ZeroHorizon)
        );
    }

    #[test]
    fn effective_horizon_addition_overflow_is_rejected() {
        assert_eq!(
            ReplayHorizon::new(Duration::MAX, Duration::from_nanos(1)),
            Err(ReplayCapacityError::HorizonOverflow)
        );
    }

    #[test]
    fn rate_times_horizon_u128_overflow_is_rejected() {
        let rates = DirectionalRates::symmetric(BitsPerSecond::new(u64::MAX).unwrap());
        let spec = ReplayCapacitySpec::new(rates, rates, Duration::MAX, Duration::ZERO).unwrap();

        assert_eq!(
            spec.derive(),
            Err(ReplayCapacityError::RateHorizonProductOverflow)
        );
    }

    #[test]
    fn derived_directional_capacity_above_u64_is_rejected() {
        let rates = DirectionalRates::symmetric(BitsPerSecond::new(u64::MAX).unwrap());
        let spec =
            ReplayCapacitySpec::new(rates, rates, Duration::from_secs(9), Duration::ZERO).unwrap();

        assert_eq!(spec.derive(), Err(ReplayCapacityError::CapacityExceedsU64));
    }

    #[test]
    fn full_duplex_capacity_sum_overflow_is_rejected() {
        let rates = DirectionalRates::symmetric(BitsPerSecond::new(u64::MAX).unwrap());
        let spec =
            ReplayCapacitySpec::new(rates, rates, Duration::from_secs(4), Duration::ZERO).unwrap();

        assert_eq!(
            spec.derive(),
            Err(ReplayCapacityError::PerFlowFullDuplexCapacityExceedsU64)
        );
    }

    #[test]
    fn global_full_duplex_capacity_sum_overflow_is_rejected() {
        let per_flow = DirectionalRates::symmetric(BitsPerSecond::new(1).unwrap());
        let aggregate = DirectionalRates::symmetric(BitsPerSecond::new(u64::MAX).unwrap());
        let spec =
            ReplayCapacitySpec::new(per_flow, aggregate, Duration::from_secs(4), Duration::ZERO)
                .unwrap();

        assert_eq!(
            spec.derive(),
            Err(ReplayCapacityError::GlobalFullDuplexCapacityExceedsU64)
        );
    }
}

//! D25 tracer: the §5.3 configured-instance term `I_cfg` (checked arithmetic,
//! geometry pinned, measured factors = observed maximum × 5/4) must cover the
//! observed unmetered Node heap of the F4096/W128/Cfull alone runs.
use super::*;

/// Observed in the tagged-domain alone runs (D25, raw n1-a3attr-gate-20261105
/// -attribution): unmetered = Node-domain peak − charged peak − fixed − the
/// geometry terms at the observed 4226 sockets / 4224 rows, per F=4096.
const D25_OBSERVED_PER_FLOW: u64 = 57_675; // permit; native 57,363
const D25_OBSERVED_PER_SERVICE: u64 = 5_559_702; // startup Node − charged
/// Largest observed unmetered Node heap (permit: peak 1,719,804,971 −
/// charged 791,117,913).
const D25_OBSERVED_UNMETERED: u64 = 928_687_058;

/// F4096 production geometry as `native_instances` builds it (test build
/// sizes: context entry 8632, storage+handle 440, row 12608, slot 1280).
fn f4096() -> InstanceShape {
    InstanceShape {
        listeners: 4352,
        socket_rx: 65_535,
        socket_tx: 65_535,
        socket_context_entry: 8632,
        socket_storage_entry: 440,
        flow_slots: 4224,
        flows: 4096,
        flow_row: 12_608,
        flow_slot: 1280,
        flow_index: 528,
    }
}

#[test]
fn measured_factors_are_observed_max_with_safety() {
    let factors = InstanceFactors::D25_MEASURED;
    assert_eq!(factors.per_flow, (D25_OBSERVED_PER_FLOW * 5).div_ceil(4));
    assert_eq!(factors.per_service, (D25_OBSERVED_PER_SERVICE * 5).div_ceil(4));
}

#[test]
fn f4096_instance_bound_covers_observed_unmetered_heap() {
    let bound = instance_bound(&f4096(), &InstanceFactors::D25_MEASURED).unwrap();
    // Geometry terms are exact configuration arithmetic, not observations.
    assert_eq!(bound.socket_payload, 4352 * 131_070);
    assert_eq!(bound.socket_tables, (8192 * 8632 + 8192 + 16) + 8192 * 440);
    assert_eq!(bound.flow_rows, 4224 * (12_608 + 1280) + 528);
    assert_eq!(bound.flow_residual, 4096 * InstanceFactors::D25_MEASURED.per_flow);
    assert_eq!(bound.total, bound.socket_payload + bound.socket_tables + bound.flow_rows
        + bound.flow_residual + bound.service);
    // Geometry alone does not cover what was observed: the residual is needed.
    let geometry = bound.socket_payload + bound.socket_tables + bound.flow_rows;
    assert!(geometry < D25_OBSERVED_UNMETERED);
    assert!(bound.total >= D25_OBSERVED_UNMETERED, "{bound:?}");
}

#[test]
fn instance_and_memory_bounds_reject_overflow_typed() {
    let mut shape = f4096();
    shape.listeners = usize::MAX;
    assert_eq!(instance_bound(&shape, &InstanceFactors::D25_MEASURED), Err(BoundError::Overflow));
    let mut shape = f4096();
    shape.flows = usize::MAX;
    assert_eq!(instance_bound(&shape, &InstanceFactors::D25_MEASURED), Err(BoundError::Overflow));
    assert_eq!(memory_bound(u64::MAX, 1), Err(PreflightRejection::Overflow));
    assert_eq!(memory_bound(816_603_436, 7), Ok(816_603_443));
}

#[test]
fn small_tables_use_minimum_bucket_group() {
    let mut shape = f4096();
    (shape.listeners, shape.flows, shape.flow_slots) = (1, 0, 0);
    let bound = instance_bound(&shape, &InstanceFactors { per_flow: 0, per_service: 0 }).unwrap();
    assert_eq!(bound.socket_tables, (8 * 8632 + 8 + 16) + 440);
}

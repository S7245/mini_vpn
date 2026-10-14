//! Private compatibility custody facade.
//!
//! transaction owns the one W/F slot table; supervision owns direct Generic roles;
//! rearm owns the original-loop physical transaction. Reader/writer children retain
//! actual IO outcomes. This facade adds no ledger, task, queue or authority.
//! Tests live in the implementation children and client_tun/tests/compatibility_*.

mod rearm;
mod receive_window;
mod relay_reader;
mod relay_writer;
mod supervision;
mod transaction;

pub(in crate::client_tun::compatibility_runtime) use transaction::OpenCustody;
pub(in crate::client_tun) use transaction::{HandshakeNotice, OpenDispatch, OpenEvent};
#[cfg(all(test, feature = "harness"))]
pub(in crate::client_tun::compatibility_runtime) use transaction::{
    OpenOutcome, OpenPhase, OpenSlot, open_origin,
};

pub(in crate::client_tun) use rearm::{
    FatalPhysicalTurn, FatalStep, FlowAdmission, FlowMirror, LocalReset, RearmGate, RearmIntent,
    RemotePayloadRequest, flush_tx_and_release_downlink_permits, gate_legacy_rearm,
    legacy_bare_rearm_from_loop, legacy_reasoned_rearm_from_loop, legacy_reasoned_rearm_with_gate,
    legacy_reasoned_rearm_with_snapshot_and_gate, reap_dead_slots_from_loop,
};

#[cfg(test)]
pub(in crate::client_tun) use rearm::{
    reap_dead_slots, should_defer_owned_terminal_reap, should_reap_slot,
    should_reap_slot_with_snapshot,
};

pub(in crate::client_tun) use supervision::{
    GenericLaunchRequest, launch_generic_relay, run_relay, run_relay_ordered_egress_permit,
    run_relay_thin, run_relay_with_engine,
};

pub(in crate::client_tun) use relay_writer::run_relay_writer;

pub(in crate::client_tun) use relay_reader::send_remote_payload_batch_to_main;
#[cfg(test)]
pub(in crate::client_tun) use relay_reader::{ReadyReadRequest, drain_ready_remote_reads};

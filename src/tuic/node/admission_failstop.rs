//! D12 Node-owner failstop for Quinn-internal receive Full (A2 Route 2).
//! Owns: the per-connection policy "a live connection whose quinn-proto
//! `udp_rx.admission_refused` is non-zero is closed with a typed reason".
//! Does not own: Proto's exact whole-packet refusal (fork, unchanged), the
//! endpoint's pre-Proto counted drop (unattributable, stays a drop), recovery
//! or rebuild (the existing live_conn/aux recovery path observes the close).
//! Invariants: Proto only counts a refusal on BudgetFull, and every connection
//! starts at zero, so no baseline is needed; the counter is sticky, so a slot
//! skipped under lock contention is re-evaluated on the next monitor tick.
//! Never Node-fatal, never Q: one connection is lost, siblings stay live.
//! Entry: `TuicUpstream::failstop_admission_refused`, called by the endpoint
//! recovery monitor. Tests: node/tests/admission_failstop.rs.
use crate::tuic::*;

/// Typed application close reason seen by the peer for this failstop.
pub(in crate::tuic) const NODE_ADMISSION_FAILSTOP_REASON: &[u8] =
    b"node_protocol_admission_failstop";

impl TuicUpstream {
    /// Close every live current/draining connection with a Proto admission
    /// refusal. Returns the number closed this pass. Bounded: one stats read
    /// per live generation, try_lock only, no await.
    pub(in crate::tuic) fn failstop_admission_refused(&self) -> usize {
        let mut closed = 0usize;
        for (index, slot) in self.tcp_pool_slots.iter().enumerate() {
            let Ok(state) = slot.state.try_lock() else {
                continue;
            };
            for conn in std::iter::once(&state.current.transport).chain(
                state
                    .draining
                    .as_ref()
                    .map(|draining| &draining.generation.transport),
            ) {
                if conn.close_reason().is_some() {
                    continue;
                }
                let refused = conn.stats().udp_rx.admission_refused;
                if refused == 0 {
                    continue;
                }
                println!(
                    "⛔ tuic-node-admission-failstop conn={index} id={} admission_refused={refused}",
                    conn.stable_id()
                );
                conn.close(VarInt::from_u32(0), NODE_ADMISSION_FAILSTOP_REASON);
                closed = closed.saturating_add(1);
            }
        }
        closed
    }
}

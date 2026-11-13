//! N5-OBS (D241) read-only flow custody census for the CLI metrics tick.
//!
//! Owns: nothing but the value-only [`FlowCensus`]. Does not own: stages,
//! Q markers, close-tail latches or receipts; it never mutates the slot table.
//! One bounded pass over the fixed slot table under the already-taken opens
//! lock, called only from the periodic control-path observer (never per
//! packet). Close-tail A1 counts are latched rows still live at the sample
//! (`aborted` / `extended`), not cumulative grants.
//! Entry: [`OpenCustody::census`]. Tests: `transaction/tests/census.rs`.
use super::*;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::client_tun::compatibility_runtime) struct FlowCensus {
    pub(in crate::client_tun::compatibility_runtime) live: usize,
    pub(in crate::client_tun::compatibility_runtime) closing: usize,
    pub(in crate::client_tun::compatibility_runtime) rearming: usize,
    pub(in crate::client_tun::compatibility_runtime) quarantined: usize,
    /// Registered Flow-Q rows (root counter).
    pub(in crate::client_tun::compatibility_runtime) flow_q_rows: usize,
    pub(in crate::client_tun::compatibility_runtime) tail_aborted: usize,
    pub(in crate::client_tun::compatibility_runtime) tail_extended: usize,
    pub(in crate::client_tun::compatibility_runtime) published: u64,
    pub(in crate::client_tun::compatibility_runtime) released: u64,
}

impl OpenCustody {
    pub(in crate::client_tun::compatibility_runtime) fn census(&self) -> FlowCensus {
        let mut out = FlowCensus {
            live: self.live,
            flow_q_rows: self.flow_q_rows(),
            ..FlowCensus::default()
        };
        if let Some(accounting) = &self.flow {
            (out.published, out.released) = (accounting.published, accounting.released);
        }
        for slot in self.slots.iter().flatten() {
            let OpenPhase::Flow(flow) = &slot.phase else {
                continue;
            };
            match flow.stage {
                FlowStage::Closing => out.closing += 1,
                FlowStage::Rearming => out.rearming += 1,
                FlowStage::Quarantined => out.quarantined += 1,
                _ => {}
            }
            out.tail_aborted += usize::from(flow.tail.aborted);
            out.tail_extended += usize::from(flow.tail.extended);
        }
        out
    }
}

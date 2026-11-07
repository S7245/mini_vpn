//! Test-only witness for the original Permit reset's cap disposal.
//! Owns no cap, receipt, actor or production path. The physical FlowMirror calls
//! these two snapshots around its real reset; Root keeps the resulting Copy
//! witness only after the real receipt commits. Tested by Native Permit Service.
use super::*;
use crate::client_tun::downlink_custody::EgressLedgerSnapshot;

#[derive(Clone, Copy, Debug)]
pub(in crate::client_tun::compatibility_runtime) struct PermitResetObservation {
    pub pre: EgressLedgerSnapshot,
    pub post: EgressLedgerSnapshot,
    pub released_before: usize,
    pub released_after: usize,
    pub pending: usize,
    pub discarded_inflight: usize,
}

#[derive(Clone, Copy)]
pub(in crate::client_tun::compatibility_runtime::flow_custody) struct Pre {
    ledger: EgressLedgerSnapshot,
    released: usize,
    pending: usize,
}

pub(in crate::client_tun::compatibility_runtime::flow_custody) fn capture_pre(
    ledger: &crate::client_tun::downlink_custody::EgressPermitLedger,
    pending: usize,
    mirror: &FlowMirror,
) -> Option<Pre> {
    let queue = mirror.bound_egress_queue()?;
    Some(Pre {
        ledger: ledger.snapshot_for_queue(queue)?,
        released: queue.custodied_released_bytes_for_test()?,
        pending,
    })
}

pub(in crate::client_tun::compatibility_runtime::flow_custody) fn capture_post(
    ledger: &crate::client_tun::downlink_custody::EgressPermitLedger,
    mirror: &FlowMirror,
    pre: Option<Pre>,
    discarded_inflight: usize,
) -> Option<PermitResetObservation> {
    let pre = pre?;
    let queue = mirror.bound_egress_queue()?;
    Some(PermitResetObservation {
        pre: pre.ledger,
        post: ledger.snapshot_for_queue(queue)?,
        released_before: pre.released,
        released_after: queue.custodied_released_bytes_for_test()?,
        pending: pre.pending,
        discarded_inflight,
    })
}

impl FlowPhysicalReceipt {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn permit_reset_observation(
        self,
    ) -> Option<PermitResetObservation> {
        self.permit_reset
    }
}

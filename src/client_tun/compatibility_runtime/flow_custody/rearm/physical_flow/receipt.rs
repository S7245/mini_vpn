//! Bidirectional physical receipt validation before the Root F release.
//! Owns the one FlowPhysicalReceipt-to-FlowTransaction comparison; it grants
//! no release authority and performs no DATA work. Entry: `matches_flow`;
//! tests: physical_flow/tests and saved Root receipt gates.
use super::*;

impl FlowPhysicalReceipt {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn matches_flow(
        self,
        flow: &super::super::super::transaction::FlowTransaction,
    ) -> bool {
        let published = flow.roles.as_ref().and_then(FlowRoles::published);
        let (messages, bytes) = published.map_or((0, 0), |value| (value.messages, value.bytes));
        let fake_matches = match (flow.fake_ip, self.fake_release) {
            (None, None) => true,
            (Some(acquired), Some(released)) => {
                acquired.ip == released.ip
                    && acquired.incarnation == released.incarnation
                    && acquired.acquisition == released.acquisition
                    && released.after.checked_add(1) == Some(released.before)
            }
            _ => false,
        };
        let d16_matches = match (self.d16, flow.roles.as_ref()) {
            (None, Some(roles)) => !roles.is_d16(),
            (None, None) => true,
            (Some(receipt), Some(roles)) if roles.is_d16() => {
                roles.d16_reader_ledger()
                    == Some((
                        receipt.queued_bytes,
                        receipt.ready_events,
                        receipt.closed_events,
                    ))
                    && roles.read_total() == Some(receipt.read_bytes)
                    && receipt.read_bytes == receipt.queued_bytes
                    && receipt
                        .transferred_bytes
                        .checked_add(receipt.discarded_queue_bytes)
                        == Some(receipt.queued_bytes)
                    && roles
                        .d16_queue()
                        .zip(roles.d16_budget())
                        .is_some_and(|(queue, budget)| queue.d16_terminal_quota_zero(budget))
            }
            _ => false,
        };
        self.id == flow.id
            && flow.id.epoch.checked_add(1) == Some(self.next_epoch)
            && self.data_messages == messages
            && self.data_bytes == bytes
            && self.transferred.0.checked_add(self.discarded.0) == Some(self.data_messages)
            && self.transferred.1.checked_add(self.discarded.1) == Some(self.data_bytes)
            && self.discarded_escrow == flow.escrow_bytes
            && flow.escrow_bytes == flow.escrow.len()
            && self.discarded_uplink == 0
            && self
                .discarded_downlink
                .checked_add(self.discarded_inflight)
                .is_some_and(|bytes| {
                    self.transferred
                        .1
                        .checked_add(self.d16.map_or(0, |receipt| receipt.transferred_bytes))
                        .is_some_and(|transferred| bytes <= transferred)
                })
            && flow
                .roles
                .as_ref()
                .and_then(FlowRoles::permit_queue)
                .is_none_or(crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue::physical_zero)
            && fake_matches
            && d16_matches
            && self.native_matches_flow(flow)
            && self
                .discarded_downlink
                .checked_add(self.discarded_socket_rx)
                .and_then(|n| n.checked_add(self.discarded_socket_tx))
                .is_some()
    }
}

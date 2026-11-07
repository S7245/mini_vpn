//! Cold Native backing and producer receipt binding at the original physical F.
//! Owns no queue or transport allocation: Root Roles owns the producer receipt,
//! Quinn owns the backing meter, and the SocketCtx mirror checks both at rearm.
//! Entries `bind_native_backing`, `native_backing_matches`, `native_matches_flow`;
//! true endpoint tests live in client_tun/tests/compatibility_f_flow.rs.

use super::*;

impl FlowMirror {
    // The selected event remains in the original lane until this entire
    // physical admission returns. No bytes, permits or window are taken here.
    pub(in crate::client_tun) fn begin_data_event(
        &mut self,
        epoch: u64,
        payload: &crate::tcp_downlink_pump::OwnedPayload,
        permit: &Option<DownstreamBytePermit>,
    ) -> std::io::Result<bool> {
        let backing_matches = match (&self.native_backing, payload) {
            (Some(expected), crate::tcp_downlink_pump::OwnedPayload::Native(bytes)) => {
                crate::tcp_downlink_pump::StockKey::accepts_payload(self.native_stock.as_ref(), bytes)
                    && bytes.allocation_identity().is_some()
                    && bytes
                        .backing_meter()
                        .is_some_and(|actual| actual.same_authority(expected))
                    && expected.admit_precharged().is_ok()
            }
            (None, crate::tcp_downlink_pump::OwnedPayload::Legacy(_)) => true,
            _ => false,
        };
        if !backing_matches || !self.validate_data_permit(epoch, payload.len(), permit) {
            self.quarantined = true;
            if let Some(meter) = &self.native_backing {
                meter.quarantine();
            }
            crate::tcp_downlink_pump::StockKey::reject_payload(self.native_stock.as_ref(),
                match payload { crate::tcp_downlink_pump::OwnedPayload::Native(bytes) => Some(bytes), _ => None });
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unproven flow DATA capability",
            ));
        }
        Ok(self.begin_event(epoch, Some(payload.len())))
    }

    pub(super) fn bind_native_backing(&mut self, roles: &FlowRoles) -> bool {
        if !roles.is_native() || self.native_backing.is_some() || self.d16_queue.is_some() {
            self.quarantined = true;
            return false;
        }
        let Some(meter) = roles.native_backing_meter() else {
            self.quarantined = true;
            return false;
        };
        self.native_stock = roles.native_stock_key().cloned();
        self.native_backing = Some(meter.clone());
        true
    }

    pub(super) fn native_backing_matches(&self, roles: Option<&FlowRoles>) -> bool {
        match (&self.native_backing, roles) {
            (Some(binding), Some(roles)) if roles.is_native() => {
                roles.native_backing_meter().is_some_and(|meter| {
                    meter.same_authority(binding) && !meter.snapshot().quarantined
                        && match (&self.native_stock, roles.native_stock_key()) {
                            (Some(a), Some(b)) => a.same_stock(b), (None, None) => true, _ => false,
                        }
                })
            }
            (None, Some(roles)) => !roles.is_native(),
            (None, None) => true,
            _ => false,
        }
    }
}

impl FlowPhysicalReceipt {
    pub(super) fn native_matches_flow(
        self,
        flow: &super::super::super::transaction::FlowTransaction,
    ) -> bool {
        match (
            self.native_pump,
            self.native_direct_received,
            flow.roles.as_ref(),
        ) {
            (Some(receipt), None, Some(roles)) if roles.is_native() => {
                roles.native_pump_receipt() == Some(receipt)
                    && receipt
                        .received_items
                        .checked_add(receipt.queue_discarded_items)
                        == Some(receipt.sent_items)
                    && receipt
                        .received_bytes
                        .checked_add(receipt.queue_discarded_bytes)
                        == Some(receipt.sent_bytes)
                    && receipt
                        .forwarded_bytes
                        .checked_add(receipt.reader_discarded_bytes)
                        == Some(receipt.received_bytes)
                    && receipt
                        .sent_bytes
                        .checked_add(receipt.producer_discarded_bytes)
                        == Some(receipt.source_read_bytes)
                    && roles.native_reader_received_bytes() == Some(receipt.forwarded_bytes)
            }
            (None, Some(received), Some(roles)) if roles.is_native() => {
                roles.native_direct_source_received_bytes() == Some(received)
                    && roles.native_reader_received_bytes() == Some(received)
                    && self.data_bytes <= received
            }
            (None, None, Some(roles)) => !roles.is_native(),
            (None, None, None) => true,
            _ => false,
        }
    }
}

pub(super) fn refuse_clock(
    owner: &SessionControl, flow: &mut super::super::super::transaction::FlowTransaction,
    error: bytes::MeteredCopyError,
) {
    owner.record(RetainedFailure::from_io(
        super::super::super::transaction::open_origin(flow.id.index, flow.id.epoch, FailurePhase::TcpOpen),
        std::io::Error::from(quinn::owned_events::AdmissionError(error)),
    ));
    flow.close(("native_failure_clock", "cold_capacity_refused"));
}

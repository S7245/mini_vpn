//! Borrows sealed incoming payload at its existing Leg authority.
//! Does not expose the frame, clone a seal or validate/mint transport ownership.
//! Entry cold_payload returns only a Session summary; error/retry tests live in
//! OwnerTarget's initial-bootstrap effect_storage diagnostic leaves.
use super::{BoundInbound, LegBoundFrame};
use crate::resumable::payload_inventory::{PayloadAuditFailure, SessionPayloadInventory};

impl LegBoundFrame {
    pub(in super::super) fn cold_payload(
        &self,
    ) -> Result<SessionPayloadInventory, PayloadAuditFailure> {
        SessionPayloadInventory::frame(&self.frame)
    }
}
impl BoundInbound {
    pub(in super::super) fn cold_payload(
        &self,
    ) -> Result<SessionPayloadInventory, PayloadAuditFailure> {
        match self {
            Self::Session(frame) => frame.cold_payload(),
            Self::LegControl(frame) => SessionPayloadInventory::leg_control(&frame.frame),
        }
    }
}

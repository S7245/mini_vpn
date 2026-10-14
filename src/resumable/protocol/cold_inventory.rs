//! Allocation-free structural wire-length audit beside the protocol encoder.
//! Reuses its validator/constants; does not validate a negotiated transport or
//! mint a capability. Tests: resumable/payload_inventory/tests/mod.rs.
use super::*;

impl LegControlFrame {
    pub(crate) fn encoded_len_checked(&self) -> Result<usize, ProtocolError> {
        validate_leg_control_record(&self.record)?;
        let body = match &self.record {
            LegControlRecord::StandbyRegister { .. } => STANDBY_REGISTER_FIXED_BODY_BYTES,
            LegControlRecord::StandbyAccepted { .. } => STANDBY_ACCEPTED_FIXED_BODY_BYTES,
            LegControlRecord::LegProbe { .. } => LEG_PROBE_FIXED_BODY_BYTES,
            LegControlRecord::LegProbeAck { .. } => LEG_PROBE_ACK_FIXED_BODY_BYTES,
            LegControlRecord::SwitchHint { .. } => SWITCH_HINT_FIXED_BODY_BYTES,
        };
        FRAME_HEADER_BYTES
            .checked_add(body)
            .ok_or(ProtocolError::FrameTooLarge)
    }
}

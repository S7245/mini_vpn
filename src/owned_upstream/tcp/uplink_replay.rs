//! Exact replay-permit receipt validation and release at the original TCP owner.
//!
//! Holds no second lifecycle authority; receipts and permits remain their original
//! types. Supervisor owns reduction/journaling; tests remain in tcp/tests.

use super::*;

impl UplinkReplayOwnership {
    /// Session expiry releases while the original Supervisor still borrows every
    /// replay node; a permit-wake unwind must not remove the current ownership.
    pub(in crate::owned_upstream) fn release_after_reducer_session_expiry(
        &mut self,
        effect: &SessionEffect,
    ) -> Result<bool, FlowPortError> {
        if !matches!(effect, SessionEffect::SessionExpired) {
            return Err(FlowPortError::UplinkTerminalMismatch);
        }
        Ok(self.permits.take().is_some())
    }

    pub const fn flow_id(&self) -> SessionFlowId {
        self.flow.flow_id()
    }
    pub const fn direction(&self) -> Direction {
        self.direction
    }
    pub const fn start(&self) -> ByteOffset {
        self.start
    }
    pub const fn end(&self) -> ByteOffset {
        self.end
    }
    pub const fn bytes(&self) -> usize {
        self.bytes
    }
    pub(in crate::owned_upstream) fn matches_factory_origin(
        &self,
        expected: &TcpFactoryOrigin,
    ) -> bool {
        self.origin
            .as_ref()
            .is_some_and(|actual| actual.matches(expected))
    }
    pub fn is_released(&self) -> bool {
        self.permits.is_none()
    }

    /// Releases the exact byte permit only when the reducer-accepted ACK
    /// covers this complete DATA extent. Partial coverage returns `false` and
    /// deliberately retains the whole permit.
    pub fn release_after_reducer_ack(
        &mut self,
        acknowledged: &ReplayAcknowledged,
    ) -> Result<bool, FlowPortError> {
        if acknowledged.local_flow() != self.flow || acknowledged.direction() != self.direction {
            return Err(FlowPortError::UplinkAckMismatch);
        }
        if acknowledged.next_accepted() < self.end {
            return Ok(false);
        }
        let released = self.permits.take().is_some();
        Ok(released)
    }

    /// Explicit cleanup after the reducer has emitted the exact terminal
    /// authority for this flow. A peer-reset notification alone is not enough;
    /// `FlowFinished` is the ownership-release point.
    pub fn release_after_reducer_terminal(
        &mut self,
        effect: &SessionEffect,
    ) -> Result<bool, FlowPortError> {
        let SessionEffect::FlowFinished {
            flow_id, terminal, ..
        } = effect
        else {
            return Err(FlowPortError::UplinkTerminalMismatch);
        };
        if *flow_id != self.flow.flow_id() || terminal.local_flow() != self.flow {
            return Err(FlowPortError::UplinkTerminalMismatch);
        }
        let released = self.permits.take().is_some();
        Ok(released)
    }
}

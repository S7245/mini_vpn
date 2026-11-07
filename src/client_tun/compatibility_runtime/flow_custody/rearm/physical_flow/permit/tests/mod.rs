//! Controlled original mirror identity/queue binding for actual wire closure.
//! This is not a Root, role or Service acceptance fixture.
use super::*;
impl FlowMirror {
    pub(in crate::client_tun) fn test_wire_bound_queue(
        handle: SocketHandle,
        epoch: u64,
        queue: AsyncLeasedByteFlowQueue,
    ) -> Self {
        let mut mirror = super::super::tests::flow_mirror();
        mirror.id.handle = handle;
        mirror.id.epoch = epoch;
        mirror.permit_queue = Some(Binding {
            queue,
            mode: DownstreamPermitReleaseMode::OnEgressDrain,
        });
        mirror
    }
}

impl FlowMirror {
    pub(in crate::client_tun) fn test_wire_admission_queue(
        handle: SocketHandle,
        epoch: u64,
        queue: AsyncLeasedByteFlowQueue,
    ) -> Self {
        let mut mirror = Self::test_wire_bound_queue(handle, epoch, queue);
        mirror.permit_queue.as_mut().unwrap().mode = DownstreamPermitReleaseMode::OnAdmission;
        mirror
    }
}

impl FlowMirror {
    pub(in crate::client_tun) fn test_change_capture_identity(&mut self, index: bool) {
        if index {
            self.id.index += 1;
        } else {
            self.id.generation += 1;
        }
    }
}

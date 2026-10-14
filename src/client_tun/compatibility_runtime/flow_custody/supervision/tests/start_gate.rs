//! Real start-sender fault and passive joined-outcome inspection for root tests.
//! No synthesized outcome, publication, receipt, task or production interface.
use super::*;

impl FlowRoles {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn drop_start_gate_for_test(
        &mut self,
        index: usize,
    ) {
        assert!(!self.started && !self.preparation_interrupted);
        drop(
            self.starts[index]
                .take()
                .expect("actual prepared start sender"),
        );
    }

    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn joined_starts_for_test(
        &self,
    ) -> Option<[bool; 3]> {
        match (&self.writer, &self.reader, &self.monitor) {
            (Role::Joined(writer), Role::Joined(reader), Role::Joined(monitor)) => {
                Some([writer.started, reader.started, monitor.started])
            }
            _ => None,
        }
    }
}

//! Caller-fault probes into the actual prepared Root batch, never alternate lanes.
use super::*;
impl Fanout {
    pub(in crate::endpoint) fn omit_first_target_for_probe(&mut self) {
        self.targets[0] = None;
    }
    pub(in crate::endpoint) fn replace_first_kind_for_probe(&mut self, event: ConnectionEvent) {
        self.targets[0].as_mut().unwrap().event = Some(event);
    }
}

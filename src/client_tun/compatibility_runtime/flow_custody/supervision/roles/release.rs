//! Cold final release precondition of the original joined roles. No actor,
//! queue or failure authority is created here. Native checks actual retired
//! fields in addition to the original data/receipt proofs; Legacy keeps its
//! existing settlement contract. Tests exercise the original Service release.
use super::*;
impl FlowRoles {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn release_ready(&self) -> bool {
        self.settled() && self.native.as_ref().is_none_or(|native| {
            self.prepared.is_none() && self.d16.is_none()
                && self.starts.iter().all(Option::is_none) && self.stops.iter().all(Option::is_none)
                && matches!(self.writer, Role::Empty) && matches!(self.reader, Role::Empty)
                && matches!(self.monitor, Role::Empty) && matches!(self.dispatcher, Role::Empty)
                && native.release_ready()
        })
    }
}

impl MonitorOutcome {
    pub(super) fn release_ready(&self) -> bool {
        self.closed && self.drained == [true; 2] && self.back.is_none()
            && !self.in_flight && !self.monitor.hint_in_flight
            && self.panics.iter().all(Option::is_none)
            && self.monitor.signals.writer_signal_rx.is_closed() && self.monitor.signals.writer_signal_rx.is_empty()
            && self.monitor.signals.reader_signal_rx.is_closed() && self.monitor.signals.reader_signal_rx.is_empty()
    }
}

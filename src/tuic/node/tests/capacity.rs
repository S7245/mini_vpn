//! Passive assertions on the original startup ledger after known cold refusal.
use super::super::*;
impl NodeHost {
    pub(crate) fn assert_cold_full_refusal_for_test(&self) -> RetainedFailure {
        let state = self.resources.state();
        assert!(!state.endpoint_created && state.endpoint.is_none() && state.endpoint_drain.is_none());
        assert!(state.connections.iter().all(Option::is_none));
        assert_eq!((state.attempts_started, state.tasks_started, state.tasks_joined), (0, 0, 0));
        assert!(!state.published && !state.monitor_started && state.udp.is_none());
        assert!(!state.quarantined);
        assert_eq!(state.backing_meter.snapshot().physical_bytes, 0);
        let first = state.first.as_ref().unwrap();
        assert_eq!(first.facts().class, crate::shared::errors::FailureClass::InvalidTarget);
        assert_eq!(first.text_bytes(), b"quic client endpoint: receive buffer admission BudgetFull");
        first.clone()
    }
}

/// Actual Node-owned tables and lifecycle fields, not an aggregate Tokio count.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NodeInstances {
    pub connection_len: usize,
    pub connection_capacity: usize,
    pub connection_bytes: usize,
    pub connection_records: usize,
    pub stats_len: usize,
    pub stats_capacity: usize,
    pub stats_bytes: usize,
    pub stats_limit: usize,
    pub stats_started: u64,
    pub stats_joined: u64,
    pub monitor: (bool, bool, bool),
    pub endpoint: (bool, bool, bool, bool),
    pub udp_owner: bool,
}
impl NodeInstances {
    pub(super) fn sample(state: &StartupState) -> Self {
        Self {
            connection_len: state.connections.len(),
            connection_capacity: state.connections.capacity(),
            connection_bytes: std::alloc::Layout::array::<Option<Attempt>>(state.connections.capacity()).unwrap().size(),
            connection_records: state.connections.iter().flatten().count(),
            stats_len: state.tasks.len(), stats_capacity: state.tasks.capacity(),
            stats_bytes: std::alloc::Layout::array::<recovery::StatsTask>(state.tasks.capacity()).unwrap().size(),
            stats_limit: state.task_limit, stats_started: state.tasks_started, stats_joined: state.tasks_joined,
            monitor: (state.monitor.is_some(), state.monitor_started, state.monitor_joined),
            endpoint: (state.endpoint.is_some(), state.endpoint_drain.is_some(), state.endpoint_created, state.endpoint_drained),
            udp_owner: state.udp.is_some(),
        }
    }
}

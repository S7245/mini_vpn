//! Physical fault/inspection seam for the production service composition tracer.
//! Only cancels an existing task; it cannot mint or clear lifecycle evidence.
use super::super::*;

impl NodeHost {
    pub(crate) fn assert_service_clean_for_test(&self) {
        let state = self.resources.state();
        assert!(!state.quarantined);
        assert!(state.first.is_none());
        assert_eq!(state.tasks_started, state.tasks_joined);
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert_eq!(state.monitor_started, state.monitor_joined);
        assert!(state.connections.iter().all(Option::is_none));
        assert!(state.terminal.is_none() && state.uplink.is_empty());
        assert!(!state.endpoint_created || state.endpoint_drained);
    }

    pub(crate) fn cancel_original_stats_for_service_test(&mut self) {
        let state = self.resources.state();
        let task = state
            .tasks
            .iter()
            .find(|task| !task.handle.is_finished())
            .expect("Ready node must own the original live stats task");
        task.handle.abort();
        drop(state);
        self.resources.wake_recovery();
    }

    pub(crate) fn assert_service_fault_settled_for_test(&self) {
        let state = self.resources.state();
        assert!(state.quarantined);
        assert_eq!(
            state.first.as_ref().unwrap().facts().class,
            crate::shared::errors::FailureClass::Cancelled
        );
        assert!(state.tasks_started > 0);
        assert_eq!(state.tasks_started, state.tasks_joined);
        assert!(state.monitor_started && state.monitor_joined);
        assert!(state.endpoint_created && state.endpoint_drained);
        assert_eq!(state.attempts_started, state.attempts_drained);
        assert!(state.connections.iter().all(Option::is_none));
        assert!(state.terminal.is_none());
        assert!(state.uplink.is_empty());
    }
}

impl NodeHost {
    pub(crate) fn record_known_failure_for_probe_test(&mut self) {
        self.resources.record(ClientError::InvalidTarget(
            "probe injected known cleanup failure".into(),
        ));
    }
}

impl NodeHost {
    pub(crate) fn probe_stats_abort_for_test(&self) -> Option<tokio::task::AbortHandle> {
        self.resources
            .state()
            .tasks
            .iter()
            .find(|task| !task.handle.is_finished())
            .map(|task| task.handle.abort_handle())
    }
}

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

/// C4 Node-disposition faults for the Service controlled-rebuild driver.
/// Each acts at one drain stage on real custody; none mints settlement
/// evidence except `C4_STARTUP_CLASSIFIED`, which relabels an already-settled
/// cached result to reach the defensive `Unsettled` branch.
impl NodeHost {
    /// Before begin: retain one real DATA Arc so cleanup cannot unwrap it.
    pub(crate) const C4_HOLD_DATA: u8 = 1;
    /// After begin, before drain: one known typed Node failure.
    pub(crate) const C4_KNOWN_FAILURE: u8 = 2;
    /// After a settled drain: one real live task left in Node custody.
    pub(crate) const C4_UNJOINED_TASK: u8 = 3;
    /// After a settled drain: cached Running result relabelled Startup.
    pub(crate) const C4_STARTUP_CLASSIFIED: u8 = 4;

    pub(crate) fn c4_hold_data_for_service_test(
        &self,
    ) -> Arc<dyn std::any::Any + Send + Sync> {
        self.ready.clone().expect("published Node owns DATA")
    }

    pub(crate) fn c4_after_drain_for_service_test(&mut self, fault: u8) {
        if fault != Self::C4_UNJOINED_TASK && fault != Self::C4_STARTUP_CLASSIFIED {
            return;
        }
        assert_eq!(self.result, Some(NodeShutdown::RunningResourcesSettled));
        if fault == Self::C4_STARTUP_CLASSIFIED {
            self.result = Some(NodeShutdown::StartupResourcesSettled);
            return;
        }
        let mut state = self.resources.state();
        assert!(state.tasks.is_empty() && state.tasks_started == state.tasks_joined);
        state.tasks.push(recovery::StatsTask {
            stable_id: None,
            handle: tokio::spawn(std::future::pending::<()>()),
        });
        state.tasks_started += 1;
    }

    pub(crate) fn assert_c4_custody_contradiction_for_test(&self) {
        assert_eq!(self.result, Some(NodeShutdown::Quarantined));
        let state = self.resources.state();
        assert!(state.quarantined);
        let first = state.first.as_ref().expect("contradiction retains a typed first");
        assert_eq!(first.facts().io_kind, Some(std::io::ErrorKind::Other));
        assert_eq!(state.tasks.len(), 1);
        assert!(!state.tasks[0].handle.is_finished(), "real task still in custody");
        assert_eq!(state.tasks_started, state.tasks_joined + 1);
    }
}

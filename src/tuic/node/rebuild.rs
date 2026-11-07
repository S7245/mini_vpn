//! Explicit whole-session boundary, not individual Closed-connection policy.
//! The original Node owns the Endpoint close and actual cleanup proof. Only a
//! settled controlled operation can permit a sole service successor; no retry,
//! DATA, task or timer lives here. Tests: client_tun/tuic_service/tests/rebuild.
use super::*;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Disposition {
    Allowed,
    NotControlled,
    ConfigurationRejected,
    Quarantined,
    Unsettled,
    Failed,
    Expired,
}
impl Disposition {
    #[cfg(test)]
    pub(crate) fn configuration_rejected_for_test(&self) -> bool {
        *self == Self::ConfigurationRejected
    }

    pub(crate) fn is_expired(&self) -> bool {
        *self == Self::Expired
    }
    pub(crate) fn is_quarantined(&self) -> bool {
        *self == Self::Quarantined
    }
    pub(crate) fn is_allowed(&self) -> bool {
        *self == Self::Allowed
    }
}
impl NodeHost {
    /// Called only after actual business/DATA return. All rejection precedes
    /// the original Endpoint close. The original cleanup future remains saved.
    pub(crate) fn begin_controlled_rebuild(&mut self) -> Result<(), ClientError> {
        if self.deadline.is_some() || self.controlled_rebuild.is_some() {
            return Err(io_err("session rebuild", "node already stopping"));
        }
        let data = self
            .ready
            .as_ref()
            .ok_or_else(|| io_err("session rebuild", "unpublished node"))?;
        let primary = data
            .tcp_pool_slots
            .first()
            .ok_or_else(|| io_err("session rebuild", "no primary"))?;
        let slot = primary
            .state
            .try_lock()
            .map_err(|_| io_err("session rebuild", "primary contended"))?;
        let identity = slot.current.transport.stable_id();
        if slot.current.transport.close_reason().is_some()
            || data.tcp_pool_admission.active_total() != 0
        {
            return Err(io_err("session rebuild", "DATA leases remain"));
        }
        let endpoint = {
            let state = self.resources.state();
            if state.endpoint_drain.is_none()
                || !state.endpoint_created
                || state.endpoint_drained
                || state.endpoint.is_none()
                || !state.published
                || state.stopping
                || state.quarantined
                || state.config_rejected
                || state.first.is_some()
                || state.recovery.has_rebuild_debt()
                || state.connections.iter().flatten().any(|a| !a.published)
                || state
                    .connections
                    .iter()
                    .flatten()
                    .filter(|a| a.stable_id == Some(identity) && a.published)
                    .count()
                    != 1
            {
                return Err(io_err("session rebuild", "original node not eligible"));
            }
            state
                .endpoint
                .as_ref()
                .cloned()
                .ok_or_else(|| io_err("session rebuild", "original endpoint absent"))?
        };
        drop(slot);
        // Seal admission/UDP before the real close, preserving original stop
        // ordering and the original Node first15 rather than manufacturing failure.
        self.request_stop();
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
            endpoint.close(0u32.into(), b"controlled_session_rebuild")
        })) {
            self.resources.panic(payload);
        }
        self.resources.dispose(endpoint);
        if self.resources.state().quarantined {
            return Err(recorded_failure());
        }
        self.controlled_rebuild = Some(identity);
        Ok(())
    }

    pub(crate) fn controlled_rebuild_deadline(&self) -> Option<tokio::time::Instant> {
        self.deadline
    }

    pub(crate) fn controlled_rebuild_disposition(&mut self) -> Disposition {
        let mut state = self.resources.state();
        if state.quarantined {
            return Disposition::Quarantined;
        }
        let terminal = matches!(
            self.result,
            Some(
                NodeShutdown::RunningResourcesSettled
                    | NodeShutdown::StartupResourcesSettled
                    | NodeShutdown::FailedSettled
            )
        );
        let physical_clear = self.ready.is_none()
            && self.cleanup.is_none()
            && state.connections.iter().all(Option::is_none)
            && state.attempts_started == state.attempts_drained
            && state.tasks.is_empty()
            && state.tasks_started == state.tasks_joined
            && state.monitor.is_none()
            && state.monitor_started == state.monitor_joined
            && state.endpoint.is_none()
            && state.endpoint_drain.is_none()
            && (!state.endpoint_created || state.endpoint_drained)
            && state.terminal.is_none()
            && state.uplink.is_empty()
            && !state.recovery.has_rebuild_debt()
            && (!state.published
                || state
                    .udp
                    .as_ref()
                    .is_some_and(|udp| udp.settled_for_rebuild()));
        if terminal && !physical_clear {
            state.quarantined = true;
            self.result = Some(NodeShutdown::Quarantined);
            if state.first.is_none() {
                state.first = Some(RetainedFailure::from_io(
                    origin(),
                    std::io::Error::other(
                        "settled Node inventory contradicted original physical custody",
                    ),
                ));
            } else {
                state.additional = state.additional.saturating_add(1);
            }
            return Disposition::Quarantined;
        }
        if state.config_rejected {
            return Disposition::ConfigurationRejected;
        }
        if self.controlled_rebuild.is_none() {
            return Disposition::NotControlled;
        }
        if !terminal {
            return Disposition::Unsettled;
        }
        if state.first.is_some() {
            return Disposition::Failed;
        }
        if self.result != Some(NodeShutdown::RunningResourcesSettled)
            || !state.published
            || !state.stopping
        {
            return Disposition::Unsettled;
        }
        if self
            .deadline
            .is_none_or(|limit| tokio::time::Instant::now() >= limit)
        {
            return Disposition::Expired;
        }
        Disposition::Allowed
    }
}

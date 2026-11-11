mod admission_failstop;
mod controlled_rebuild;
mod failover_cancel;
mod failover_health;
mod failover_rejection;
mod failover_staged;
mod healthy_recovery;
mod primary_recovery;
mod probe_recovery;
mod recovery;
mod running;
mod service_faults;
mod staged_maintenance;
mod staged_slow;
mod startup;
mod waiting_join;
mod waiting_release;
mod waiting_terminal;

impl super::NodeHost {
    /// Install the original shared meter only before the saved startup's first poll.
    pub(crate) fn cold_meter_for_test(&mut self, meter: bytes::BackingMeter) {
        let mut state = self.resources.state();
        assert!(!state.endpoint_created && state.attempts_started == 0);
        state.backing_meter = meter;
    }

    /// Passive per-slot stable IDs for published drains whose last poll
    /// returned Pending. The original retirement future clears its slot.
    pub(crate) fn retirement_probe_for_test(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Vec<Option<usize>>>> {
        self.resources.state().retire_poll_pending.clone()
    }
}

pub(super) struct RetireProbeGuard {
    slots: std::sync::Arc<std::sync::Mutex<Vec<Option<usize>>>>,
    index: usize,
}

impl RetireProbeGuard {
    pub(super) fn new(
        slots: std::sync::Arc<std::sync::Mutex<Vec<Option<usize>>>>,
        index: usize,
    ) -> Self {
        Self { slots, index }
    }
}

impl Drop for RetireProbeGuard {
    fn drop(&mut self) {
        if let Some(slot) = self
            .slots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(self.index)
        {
            *slot = None;
        }
    }
}

impl super::NodeHost {
    pub(crate) fn udp_cold_observation_for_test(&self) -> (bool, bool, bool) {
        let state = self.resources.state();
        (state.published, state.monitor_started, state.udp.is_some())
    }
    pub(crate) fn shutdown_deadline_for_test(&self) -> Option<tokio::time::Instant> {
        self.deadline
    }
    pub(crate) fn udp_first_for_test(&self) -> (Option<crate::shared::errors::FailureFacts>, u64) {
        let state = self.resources.state();
        (
            state.first.as_ref().map(|f| f.facts().clone()),
            state.additional,
        )
    }
}

impl super::StartupResources {
    pub(in crate::tuic) fn udp_owner_for_test(meter: bytes::BackingMeter) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self::with_meter(1, meter))
    }
}

pub(crate) struct NativeNodeObserver(std::sync::Arc<super::StartupResources>);
impl NativeNodeObserver {
    pub(crate) fn udp_joined(&self) -> bool {
        let state = self.0.state();
        state.monitor_joined
            && state
                .udp
                .as_ref()
                .is_some_and(|udp| udp.settled_for_rebuild())
    }
    pub(crate) fn facts(&self) -> (Option<crate::shared::errors::FailureFacts>, u64, bool) {
        let state = self.0.state();
        (
            state.first.as_ref().map(|first| first.facts().clone()),
            state.additional,
            state.quarantined,
        )
    }
}
impl super::NodeHost {
    pub(crate) fn native_observer_for_test(&self) -> NativeNodeObserver {
        NativeNodeObserver(self.resources.clone())
    }
}

/// Bounded Native C<=19 read window; owns no startup, DATA or drain authority.
#[derive(Clone)]
pub(crate) struct NativeNodeWeakObserver(std::sync::Weak<super::StartupResources>);
#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeConnectionSample {
    pub registered_id: Option<usize>,
    pub actual_id: Option<usize>,
    pub published: bool,
    pub stats: Option<quinn::ConnectionStats>,
    /// The Node's own connection is terminally closed (client-side detection).
    pub closed: bool,
}
#[derive(Clone, Copy, Debug)]
pub(crate) struct NativeNodeSample {
    pub endpoint: quinn::EndpointStats,
    pub backing: bytes::BackingMeterSnapshot,
    pub instances: capacity::NodeInstances,
    pub slots: usize,
    pub connections: [Option<NativeConnectionSample>; 19],
}
impl NativeNodeWeakObserver {
    pub(crate) fn sample(&self) -> Option<NativeNodeSample> {
        let original = self.0.upgrade()?;
        let prepared = {
            let state = original.state();
            if state.connections.len() > 19 {
                None
            } else {
                state.endpoint.as_ref().map(|endpoint| {
                    let handles: [Option<(Option<usize>, bool, Option<quinn::Connection>)>; 19] =
                        std::array::from_fn(|index| {
                            state
                                .connections
                                .get(index)
                                .and_then(Option::as_ref)
                                .map(|entry| {
                                    (entry.stable_id, entry.published, entry.connection.clone())
                                })
                        });
                    (
                        endpoint.clone(),
                        state.backing_meter.clone(),
                        state.connections.len(),
                        handles,
                        capacity::NodeInstances::sample(&state),
                    )
                })
            }
        };
        // stats can wake on a poisoned Quinn lock; neither stats nor last-handle
        // Drop may run under the original Node state lock or a Control lock.
        let sample = prepared.map(|(endpoint, meter, slots, handles, instances)| {
            let endpoint_stats = endpoint.stats();
            let connections = std::array::from_fn(|index| {
                handles[index]
                    .as_ref()
                    .map(|(registered, published, connection)| {
                        let actual = connection.as_ref().map(quinn::Connection::stable_id);
                        if let (Some(registered), Some(actual)) = (*registered, actual) {
                            assert_eq!(registered, actual);
                        }
                        NativeConnectionSample {
                            registered_id: *registered,
                            actual_id: actual,
                            published: *published,
                            stats: connection.as_ref().map(quinn::Connection::stats),
                            closed: connection
                                .as_ref()
                                .is_some_and(|c| c.close_reason().is_some()),
                        }
                    })
            });
            let backing = meter.snapshot();
            drop(handles);
            drop(endpoint);
            drop(meter);
            NativeNodeSample {
                endpoint: endpoint_stats,
                backing,
                instances,
                slots,
                connections,
            }
        });
        drop(original);
        sample
    }
}
/// N2 Waiting evidence: Waiting entries, re-mints, counted (unrecorded)
/// handshake failures, held retry rows, Node first failure and Q.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WaitingSample {
    pub waits: u64,
    pub reprobes: u64,
    pub waiting_failures: u64,
    pub retries_held: usize,
    pub parked: usize,
    pub first_failure: bool,
    pub quarantined: bool,
}
impl NativeNodeWeakObserver {
    pub(crate) fn waiting(&self) -> Option<WaitingSample> {
        let original = self.0.upgrade()?;
        let sample = {
            let state = original.state();
            WaitingSample {
                waits: state.recovery.waits,
                reprobes: state.recovery.reprobes,
                waiting_failures: state.recovery.waiting_failures,
                retries_held: state.recovery.retries_held().0,
                parked: state.recovery.retries_held().1,
                first_failure: state.first.is_some(),
                quarantined: state.quarantined,
            }
        };
        drop(original);
        Some(sample)
    }
}
impl super::NodeHost {
    pub(crate) fn native_weak_observer_for_test(&self) -> NativeNodeWeakObserver {
        NativeNodeWeakObserver(std::sync::Arc::downgrade(&self.resources))
    }
}

impl super::StartupResources {
    pub(in crate::tuic) fn uplink_state_for_test(&self) -> (bool, Option<tokio::time::Instant>, u64) {
        let state = self.state();
        (state.quarantined, state.shutdown_deadline, state.additional)
    }
}

mod uplink;

impl super::NodeHost {
    pub(crate) async fn retire_live_uplink_for_test(&self) {
        let data = self.ready.as_ref().unwrap();
        let table = data.udp_stream_sender.lock().unwrap().empty_take();
        assert!(self.resources.state().uplink.is_empty());
        self.resources.state().uplink = table;
        self.resources.retire_uplink().await;
    }
}

impl super::NodeHost {
    pub(crate) fn uplink_deadline_for_test(&self, deadline: tokio::time::Instant) {
        let mut state = self.resources.state();
        state.shutdown_deadline = Some(state.shutdown_deadline.map_or(deadline, |old| old.min(deadline)));
    }
}
impl NativeNodeObserver {
    pub(crate) fn uplink_inventory_for_test(&self) -> Option<(usize, usize, bool, usize)> {
        self.0.state().uplink.cold_inventory_for_test()
    }
    pub(crate) fn uplink_deadline_for_test(&self) -> Option<tokio::time::Instant> {
        self.0.state().shutdown_deadline
    }
}

mod capacity;

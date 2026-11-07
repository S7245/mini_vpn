//! Original Node uplink disposal and unbound parent notification transaction.
//! Owns one fixed parent effect qualification, original retained Arc/Box, and
//! the existing Node retirement slot. No new clock, task, queue or refund root.
//! Native actual pending witnesses retry under the original saved deadline;
//! Legacy retains its original terminal branch. Tests: node/tests/uplink.rs.
use super::*;
use crate::shared::errors::QuietFailureSink;
use std::mem::ManuallyDrop;

pub(super) enum Parent {
    Ready,
    Running {
        quarantine: bool,
    },
    Failed {
        _parent: ManuallyDrop<Option<Arc<dyn QuietFailureSink>>>,
        _fault: ManuallyDrop<Box<dyn std::any::Any + Send>>,
    },
}
impl StartupResources {
    fn tighten_uplink(&self, deadline: Option<tokio::time::Instant>, quarantine: bool) {
        let mut state = self.state();
        if quarantine {
            state.quarantined = true;
            state.stopping = true;
            let proposed = tokio::time::Instant::now() + Duration::from_secs(15);
            state.shutdown_deadline = Some(
                state
                    .shutdown_deadline
                    .into_iter()
                    .chain(deadline)
                    .fold(proposed, std::cmp::min),
            );
        } else {
            state.shutdown_deadline = state.shutdown_deadline.into_iter().chain(deadline).min();
        }
    }
    /// Complete original Node/Session effect; all opaque sites are outside the ledger.
    pub(in crate::tuic) fn uplink_quarantine(&self) {
        self.uplink_parent(true);
    }
    pub(super) fn uplink_stop_deadline(&self) -> Option<tokio::time::Instant> {
        self.uplink_parent(false);
        self.state().shutdown_deadline
    }
    fn uplink_parent(&self, quarantine: bool) {
        let (meter, weak) = self.udp_cold_context();
        if meter.budget_limit().is_none() {
            return;
        }
        if quarantine {
            meter.quarantine();
        }
        self.tighten_uplink(None, quarantine);
        if let Some(clock) = self.1.get().and_then(std::sync::Weak::upgrade) {
            self.tighten_uplink(clock.parent_deadline_for_stop(), quarantine);
            if quarantine {
                clock.quarantine();
            }
            return;
        }
        {
            let mut state = self.state();
            match &mut state.uplink_parent {
                Parent::Ready => state.uplink_parent = Parent::Running { quarantine },
                Parent::Running {
                    quarantine: pending,
                } => {
                    *pending |= quarantine;
                    return;
                }
                Parent::Failed { .. } => return,
            }
        }
        // At most one read-only turn followed by one Q turn. Q arriving while an
        // opaque read or last Arc Drop runs is held by this same qualification.
        let mut publishing = quarantine;
        loop {
            let mut parent = ManuallyDrop::new(weak.as_ref().and_then(|parent| parent.upgrade()));
            if weak.is_some() && parent.is_none() {
                meter.quarantine();
                self.tighten_uplink(None, true);
            }
            let effect = catch_unwind(AssertUnwindSafe(|| {
                if let Some(parent) = parent.as_ref() {
                    self.tighten_uplink(parent.deadline_quiet(), false);
                    if publishing {
                        parent.quarantine_quiet();
                        self.tighten_uplink(parent.deadline_quiet(), true);
                    }
                }
            }));
            let result = match effect {
                Err(fault) => Err(fault),
                // Actual last Arc Drop is an independent opaque site.
                Ok(()) => {
                    let value = parent.take();
                    catch_unwind(AssertUnwindSafe(|| drop(value)))
                }
            };
            if let Err(fault) = result {
                meter.quarantine();
                self.tighten_uplink(None, true);
                self.state().uplink_parent = Parent::Failed {
                    _parent: parent,
                    _fault: ManuallyDrop::new(fault),
                };
                self.failure_local(RetainedFailure::opaque_panic(origin()));
                return;
            }
            let mut state = self.state();
            if !publishing && matches!(state.uplink_parent, Parent::Running { quarantine: true }) {
                publishing = true;
                drop(state);
                continue;
            }
            state.uplink_parent = Parent::Ready;
            return;
        }
    }
    pub(in crate::tuic) fn uplink_panic(&self) {
        if let Some(clock) = self.1.get().and_then(std::sync::Weak::upgrade) {
            clock.panic();
        } else {
            self.failure_local(RetainedFailure::opaque_panic(origin()));
        }
    }
    pub(super) fn uplink_admission(&self, error: bytes::MeteredCopyError) -> ClientError {
        if error == bytes::MeteredCopyError::Quarantined {
            self.uplink_quarantine();
            recorded_failure()
        } else {
            ClientError::Io(std::io::Error::from(quinn::owned_events::AdmissionError(
                error,
            )))
        }
    }
    pub(super) async fn retire_uplink(&self) {
        if self.state().backing_meter.budget_limit().is_none() {
            // Original Legacy semantics, including its existing poison/failure route.
            let mut state = self.state();
            if state.uplink.settle_legacy() {
                state.uplink = udp_send::UdpStreamSender::Empty;
            } else {
                state.quarantined = true;
            }
            return;
        }
        loop {
            let turn = {
                let mut state = self.state();
                state.uplink.begin(|next, key| {
                    let mut state = self.state();
                    let poisoned = self.0.is_poisoned();
                    match next {
                        Some(next) => state.uplink.restore(next, key, poisoned),
                        None => state.uplink.authorize_disposal(key, poisoned),
                    }
                })
            };
            let result = if let Some(turn) = turn {
                turn.retire()
            } else if self.state().uplink.is_empty() {
                udp_send::Retirement::Complete
            } else {
                udp_send::Retirement::Lost
            };
            match result {
                udp_send::Retirement::Complete => return,
                udp_send::Retirement::Lost => {
                    self.uplink_quarantine();
                    return;
                }
                udp_send::Retirement::Pending => {
                    let parent = self.uplink_stop_deadline();
                    let now = tokio::time::Instant::now();
                    let deadline = {
                        let mut state = self.state();
                        let deadline = state
                            .shutdown_deadline
                            .into_iter()
                            .chain(parent)
                            .fold(now + Duration::from_secs(15), std::cmp::min);
                        state.shutdown_deadline = Some(deadline);
                        deadline
                    };
                    if now >= deadline {
                        self.uplink_quarantine();
                        return;
                    }
                    tokio::time::sleep_until(deadline.min(now + ENDPOINT_RECOVERY_SAMPLE_INTERVAL))
                        .await;
                }
            }
        }
    }
}

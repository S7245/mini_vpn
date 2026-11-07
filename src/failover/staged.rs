//! Saved Failover control transaction. Owns at most two original child attempts,
//! only one of which may remain unsettled. No DATA wrapper, task or queue lives
//! here. Entry: the staged trait; real-node tests: tuic/node/tests/failover_staged.
//! Internal TUIC retirement is not an outward failure before policy classification.
//! The caller must return/join actual carrier users before requesting settlement.

use super::{FailoverState, FailoverUpstream, FallbackAdmission, HealthProbe, TcpLeg};
use crate::shared::ClientError;
use crate::upstream::{
    OpenedTcpRelay, ProxyUpstream, TcpOpenAttempt, TcpOpenCancel, TcpOpenPhase, TcpOpenRequest,
    TcpOpenUnavailable, TcpOpenView,
};
use std::io;
use std::sync::Arc;
use std::task::Context;
use std::time::{Duration, Instant};

const SETTLEMENT: Duration = Duration::from_secs(5);
type Child = Box<dyn TcpOpenAttempt<OpenedTcpRelay>>;

pub(super) fn create<T, R>(
    factory: Arc<FailoverUpstream<T, R>>,
    request: TcpOpenRequest,
) -> Result<Attempt<T, R>, TcpOpenUnavailable>
where
    T: ProxyUpstream + HealthProbe + 'static,
    R: ProxyUpstream + 'static,
{
    let leg = factory.state.active_leg();
    let child_request = TcpOpenRequest {
        target: request.target.clone(),
        deadline: request.deadline,
    };
    // Child factories are cold; unsupported/invalid targets never trigger retry.
    let child = match leg {
        TcpLeg::Tuic => factory.tuic.clone().staged_tcp_open(child_request)?,
        TcpLeg::Reality => factory.reality.clone().staged_tcp_open(child_request)?,
    };
    Ok(Attempt {
        state: factory.state.clone(),
        factory: Some(factory),
        request,
        child: Some(child),
        prior: None,
        leg,
        classification: false,
        published: false,
        cancelled: false,
        settled: false,
        settle_deadline: None,
        failure: None,
        interrupted: false,
        quarantined: false,
    })
}

pub(super) struct Attempt<T, R> {
    factory: Option<Arc<FailoverUpstream<T, R>>>,
    state: Arc<FailoverState>,
    request: TcpOpenRequest,
    child: Option<Child>,
    // Retain the first original diagnostic authority, never its DATA resources.
    prior: Option<Child>,
    leg: TcpLeg,
    classification: bool,
    published: bool,
    cancelled: bool,
    settled: bool,
    settle_deadline: Option<Instant>,
    failure: Option<ClientError>,
    interrupted: bool,
    quarantined: bool,
}

impl<T, R> Attempt<T, R>
where
    T: ProxyUpstream + HealthProbe + 'static,
    R: ProxyUpstream + 'static,
{
    fn begin(&mut self) -> bool {
        if self.interrupted || self.quarantined {
            self.quarantine("interrupted Failover ownership transition");
            return false;
        }
        // Never move/drop an original owner or poll fallible work without this.
        self.interrupted = true;
        true
    }

    fn retain(&mut self, error: ClientError) {
        if self.failure.is_none() {
            self.failure = Some(error);
        }
    }

    fn quarantine(&mut self, message: &'static str) {
        self.retain(ClientError::Io(io::Error::other(message)));
        self.quarantined = true;
    }

    fn tighten(&mut self, deadline: Instant) -> Instant {
        let deadline = self
            .settle_deadline
            .map_or(deadline, |old| old.min(deadline));
        self.settle_deadline = Some(deadline);
        deadline
    }

    fn note_cancel(&mut self, reason: TcpOpenCancel, deadline: Instant) {
        if self.settled {
            return;
        }
        self.cancelled = true;
        self.classification = false;
        let deadline = self.tighten(deadline);
        self.retain(ClientError::Io(io::Error::new(
            if reason == TcpOpenCancel::Deadline {
                io::ErrorKind::TimedOut
            } else {
                io::ErrorKind::Interrupted
            },
            format!("Failover open {reason:?}"),
        )));
        if let Some(child) = self.child.as_mut() {
            child.cancel(reason, deadline);
        }
    }

    fn expire(&mut self, now: Instant) {
        if !self.published && !self.settled && !self.cancelled && now >= self.request.deadline {
            self.note_cancel(TcpOpenCancel::Deadline, now + SETTLEMENT);
        }
    }

    fn finish(&mut self) {
        // Diagnostic boxes remain; original provider contracts have returned DATA.
        // Drop factory before publishing terminal zero. A destructor panic is Q.
        self.factory = None;
        self.classification = false;
        self.settled = true;
    }

    fn observe(&mut self, now: Instant) {
        let Some(child) = self.child.as_ref() else {
            self.quarantine("Failover child owner missing");
            return;
        };
        let view = child.view();
        if view.phase == TcpOpenPhase::Quarantined {
            self.quarantine("original Failover child quarantined");
        } else if view.phase == TcpOpenPhase::Settled {
            if view.lease_held || view.retirement_pending {
                self.quarantine("Failover child settlement has outstanding resources");
            } else if self.leg == TcpLeg::Tuic && !self.cancelled && !self.published {
                self.classification = true;
                self.classify(now);
            } else {
                self.finish();
            }
        }
    }

    fn classify(&mut self, now: Instant) {
        if !self.classification || self.cancelled || now >= self.request.deadline {
            return;
        }
        let Some(factory) = self.factory.as_ref() else {
            self.quarantine("Failover classification factory missing");
            return;
        };
        let Some(health) = factory.tuic.snapshot() else {
            return; // Original slot busy: retain the saved attempt under original20s.
        };
        if health.fallback == FallbackAdmission::Pending {
            return; // Original Node tokens/receipt still belong to its saved work.
        }
        if health.fallback != FallbackAdmission::Allowed
            || !self
                .state
                .record_tuic_failure(health.dead, self.state.now_secs())
        {
            self.finish();
            return;
        }
        // Only the policy CAS winner constructs the second cold child. The first
        // was observed Settled/double-zero; keep its original diagnostic box.
        if self.prior.is_some() {
            self.quarantine("Failover attempted more than one fallback");
            return;
        }
        self.prior = self.child.take();
        let next = factory.reality.clone().staged_tcp_open(TcpOpenRequest {
            target: self.request.target.clone(),
            deadline: self.request.deadline,
        });
        self.classification = false;
        self.leg = TcpLeg::Reality;
        match next {
            Ok(child) => self.child = Some(child),
            Err(reason) => {
                self.retain(ClientError::Io(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("Failover REALITY factory {reason:?}"),
                )));
                self.finish();
            }
        }
    }
}

impl<T, R> TcpOpenAttempt<OpenedTcpRelay> for Attempt<T, R>
where
    T: ProxyUpstream + HealthProbe + 'static,
    R: ProxyUpstream + 'static,
{
    fn step(&mut self, cx: &mut Context<'_>, now: Instant) {
        if !self.begin() {
            return;
        }
        if !self.settled {
            self.expire(now);
            if self.classification {
                self.classify(now);
            } else if let Some(child) = self.child.as_mut() {
                if let Some(deadline) = self.settle_deadline {
                    // Once external settlement begins, every continuation drives
                    // that original work, including step-based observers.
                    child.settle(cx, now, deadline);
                } else {
                    child.step(cx, now);
                }
                self.observe(now);
            } else {
                self.quarantine("Failover live child missing");
            }
        }
        self.interrupted = false;
    }

    fn view(&self) -> TcpOpenView {
        let mut view = self.child.as_ref().map_or(
            TcpOpenView {
                phase: TcpOpenPhase::Settled,
                lease_held: false,
                retirement_pending: false,
                connect_bytes: 0,
                connect_written: 0,
            },
            |child| child.view(),
        );
        if self.interrupted || self.quarantined {
            view.phase = TcpOpenPhase::Quarantined;
            view.lease_held = true;
            view.retirement_pending = true;
        } else if self.settled {
            view.phase = TcpOpenPhase::Settled;
        } else if self.classification
            || (self.leg == TcpLeg::Tuic
                && !self.cancelled
                && !self.published
                && matches!(view.phase, TcpOpenPhase::Retiring | TcpOpenPhase::Settled))
        {
            view.phase = TcpOpenPhase::Acquiring;
        }
        view
    }

    fn cancel(&mut self, reason: TcpOpenCancel, deadline: Instant) {
        if !self.begin() {
            return;
        }
        self.note_cancel(reason, deadline);
        self.interrupted = false;
    }

    fn take_carrier(&mut self, now: Instant) -> Option<OpenedTcpRelay> {
        if !self.begin() {
            return None;
        }
        self.expire(now);
        let mut result = None;
        if !self.cancelled && !self.settled && !self.published && !self.classification {
            if let Some(child) = self.child.as_mut() {
                if child.view().phase == TcpOpenPhase::Ready {
                    result = child.take_carrier(now);
                }
            }
            if result.is_some() {
                self.factory = None;
                if self.leg == TcpLeg::Tuic {
                    self.state.record_tuic_success();
                }
                self.published = true;
            }
        }
        self.interrupted = false;
        result
    }

    fn settle(&mut self, cx: &mut Context<'_>, now: Instant, deadline: Instant) {
        if !self.begin() {
            return;
        }
        if !self.settled {
            let deadline = self.tighten(deadline);
            if !self.published {
                self.note_cancel(TcpOpenCancel::Cancelled, deadline);
            }
            if let Some(child) = self.child.as_mut() {
                child.settle(cx, now, deadline);
                self.observe(now);
            } else {
                self.quarantine("Failover settlement child missing");
            }
        }
        self.interrupted = false;
    }

    fn failure(&self) -> Option<&ClientError> {
        if !self.quarantined
            && !self.interrupted
            && !self.cancelled
            && !self.settled
            && self.leg == TcpLeg::Tuic
            && !self.published
        {
            return None;
        }
        // During fallback the REALITY result is outward; prior remains diagnostic.
        self.child
            .as_ref()
            .and_then(|child| child.failure())
            .or(self.failure.as_ref())
    }
}

#[cfg(test)]
mod tests;

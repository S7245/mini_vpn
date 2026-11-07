//! One original staged attempt and actual carrier. Opening/retirement only; no
//! Node recovery or DATA policy. All fallible calls borrow storage outside catch.
//! Partial carrier Drop never restores a value: original lease/witness stay Q.
//! Entry new/acquire/finish; real fault tracers live in ../tests/faults.rs.
use super::*;
use crate::upstream::{
    OpenedTcpRelay, ProxyUpstream, RelayStream, TcpOpenAttempt, TcpOpenCancel, TcpOpenPhase,
    TcpOpenRequest, TcpOpenView,
};

type Attempt = Box<dyn TcpOpenAttempt<OpenedTcpRelay>>;
enum Provider {
    Active(Attempt),
    RetainedDiagnostic {
        attempt: Attempt,
        receipt: TcpOpenView,
    },
    Released(TcpOpenView),
    PartialDrop,
}
impl Provider {
    fn attempt(&self) -> Option<&Attempt> {
        match self {
            Self::Active(a) | Self::RetainedDiagnostic { attempt: a, .. } => Some(a),
            _ => None,
        }
    }
    fn attempt_mut(&mut self) -> Option<&mut Attempt> {
        match self {
            Self::Active(a) | Self::RetainedDiagnostic { attempt: a, .. } => Some(a),
            _ => None,
        }
    }
}
pub(super) struct Open {
    provider: Provider,
    carrier: Option<OpenedTcpRelay>,
    request_deadline: std::time::Instant,
    retirement_deadline: Option<std::time::Instant>,
    ticker: tokio::time::Interval,
    quarantined: AtomicBool,
    panic: Mutex<Option<ManuallyDrop<Box<dyn std::any::Any + Send>>>>,
    control: Arc<Control>,
}
impl Open {
    pub(super) fn new(
        data: Arc<TuicUpstream>,
        target: TargetAddr,
        control: Arc<Control>,
    ) -> Result<Self, ClientError> {
        let request_deadline = std::time::Instant::now() + Duration::from_secs(20);
        let attempt = data
            .staged_tcp_open(TcpOpenRequest {
                target,
                deadline: request_deadline,
            })
            .map_err(|reason| {
                ClientError::InvalidTarget(format!("probe staged open unavailable: {reason:?}"))
            })?;
        #[cfg(test)]
        let attempt = super::tests::faults::wrap_attempt(attempt, &control);
        let mut ticker = tokio::time::interval(Duration::from_millis(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Ok(Self {
            provider: Provider::Active(attempt),
            carrier: None,
            request_deadline,
            retirement_deadline: None,
            ticker,
            quarantined: AtomicBool::new(false),
            panic: Mutex::new(None),
            control,
        })
    }
    fn mark_quarantined(&self) {
        self.quarantined.store(true, Ordering::Release);
        self.control.publish(Phase::Quarantined);
        #[cfg(test)]
        if let Some(attempt) = self.provider.attempt() {
            if let Ok(view) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| attempt.view()))
            {
                *self.control.facts.after.lock().unwrap() = Some(view);
            }
        }
    }
    pub(super) fn quarantine(&self, payload: Box<dyn std::any::Any + Send>) {
        // Original failure/charged Q precedes any unknown disposal. Unknown
        // payload destruction is never attempted, even at forced owner Drop.
        self.mark_quarantined();
        let mut first = self
            .panic
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if first.is_none() {
            *first = Some(ManuallyDrop::new(payload));
        } else {
            std::mem::forget(payload);
        }
    }
    fn guard<T>(
        &mut self,
        op: impl FnOnce(&mut dyn TcpOpenAttempt<OpenedTcpRelay>) -> T,
    ) -> Option<T> {
        if self.quarantined.load(Ordering::Acquire) {
            return None;
        }
        let Some(attempt) = self.provider.attempt_mut() else {
            self.mark_quarantined();
            return None;
        };
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| op(attempt.as_mut()))) {
            Ok(value) => Some(value),
            Err(payload) => {
                self.quarantine(payload);
                None
            }
        }
    }
    fn deadline(&mut self) -> std::time::Instant {
        *self
            .retirement_deadline
            .get_or_insert_with(|| std::time::Instant::now() + Duration::from_secs(5))
    }
    pub(super) async fn acquire(&mut self, control: &Control) -> bool {
        std::future::poll_fn(|cx| {
            let now = std::time::Instant::now();
            if control.stopped.load(Ordering::Acquire) || now >= self.request_deadline {
                let reason = if now >= self.request_deadline {
                    TcpOpenCancel::Deadline
                } else {
                    TcpOpenCancel::Shutdown
                };
                self.control.begin_retirement();
                let deadline = self.deadline();
                self.guard(|a| a.cancel(reason, deadline));
                return Poll::Ready(false);
            }
            self.guard(|a| a.step(cx, now));
            let Some(view) = self.guard(|a| a.view()) else {
                return Poll::Ready(false);
            };
            #[cfg(test)]
            if let Some(attempt) = self.provider.attempt() {
                *control.facts.opening.lock().unwrap() = Some((
                    attempt.as_ref() as *const dyn TcpOpenAttempt<OpenedTcpRelay> as *const ()
                        as usize,
                    self.request_deadline,
                    view,
                ));
            }
            if view.phase == TcpOpenPhase::Quarantined {
                self.mark_quarantined();
                return Poll::Ready(false);
            }
            if self.failure().is_some()
                || self.quarantined.load(Ordering::Acquire)
                || view.phase == TcpOpenPhase::Settled
            {
                return Poll::Ready(false);
            }
            if view.phase == TcpOpenPhase::Ready {
                if let Some(Some(carrier)) = self.guard(|a| a.take_carrier(now)) {
                    let generic = matches!(carrier, OpenedTcpRelay::Generic(_));
                    #[cfg(test)]
                    let carrier = super::tests::faults::wrap(
                        carrier,
                        control,
                        self.provider.attempt().unwrap().view(),
                    );
                    self.carrier = Some(carrier);
                    if !generic {
                        self.mark_quarantined();
                    }
                    return Poll::Ready(generic);
                }
            }
            // Ready/None may be real registration pressure. Same attempt and
            // original deadline remain; this is never a legacy-open fallback.
            if self.ticker.poll_tick(cx).is_ready() {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        })
        .await
    }
    pub(super) fn stream(&mut self) -> Option<&mut RelayStream> {
        match self.carrier.as_mut() {
            Some(OpenedTcpRelay::Generic(stream)) => Some(stream),
            _ => None,
        }
    }
    pub(super) async fn finish(&mut self) {
        self.control.begin_retirement();
        if self.quarantined.load(Ordering::Acquire) {
            return;
        }
        // This Open/attempt is OUTSIDE the actual carrier destructor catch.
        if let Err(payload) = guard_drop(self.carrier.take()) {
            self.quarantine(payload);
            return;
        }
        let deadline = self.deadline();
        std::future::poll_fn(|cx| {
            self.guard(|a| a.settle(cx, std::time::Instant::now(), deadline));
            let Some(view) = self.guard(|a| a.view()) else {
                return Poll::Ready(());
            };
            match view.phase {
                TcpOpenPhase::Settled if !view.lease_held && !view.retirement_pending => {
                    let failure = self.failure().is_some();
                    if self.quarantined.load(Ordering::Acquire) {
                        return Poll::Ready(());
                    }
                    let original = std::mem::replace(&mut self.provider, Provider::PartialDrop);
                    match original {
                        Provider::Active(attempt) => {
                            if failure {
                                self.provider = Provider::RetainedDiagnostic {
                                    attempt,
                                    receipt: view,
                                };
                            } else {
                                match guard_drop(attempt) {
                                    Ok(()) => self.provider = Provider::Released(view),
                                    Err(payload) => self.quarantine(payload),
                                }
                            }
                        }
                        other => {
                            self.provider = other;
                            self.mark_quarantined();
                        }
                    }
                    Poll::Ready(())
                }
                TcpOpenPhase::Quarantined | TcpOpenPhase::Settled => {
                    self.mark_quarantined();
                    Poll::Ready(())
                }
                _ => {
                    if self.ticker.poll_tick(cx).is_ready() {
                        cx.waker().wake_by_ref();
                    }
                    Poll::Pending
                }
            }
        })
        .await
    }
    pub(super) fn settled(&self) -> bool {
        if self.quarantined.load(Ordering::Acquire) || self.carrier.is_some() {
            return false;
        }
        let valid = |v: TcpOpenView| {
            v.phase == TcpOpenPhase::Settled && !v.lease_held && !v.retirement_pending
        };
        match &self.provider {
            Provider::Released(receipt) => valid(*receipt),
            Provider::RetainedDiagnostic { attempt, receipt } => {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| attempt.view())) {
                    Ok(actual) => valid(*receipt) && actual == *receipt && self.failure().is_some(),
                    Err(payload) => {
                        self.quarantine(payload);
                        false
                    }
                }
            }
            Provider::Active(_) | Provider::PartialDrop => false,
        }
    }
    pub(super) fn failure(&self) -> Option<&ClientError> {
        if self.quarantined.load(Ordering::Acquire) {
            return None;
        }
        if matches!(self.provider, Provider::Released(_)) {
            return None;
        }
        let Some(attempt) = self.provider.attempt() else {
            self.mark_quarantined();
            return None;
        };
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| attempt.failure())) {
            Ok(error) => error,
            Err(payload) => {
                self.quarantine(payload);
                None
            }
        }
    }
}
impl Drop for Open {
    fn drop(&mut self) {
        let provider = std::mem::replace(&mut self.provider, Provider::PartialDrop);
        if self.quarantined.load(Ordering::Acquire) {
            // Forced Q retains remaining original storage; never async-clean.
            std::mem::forget(provider);
            std::mem::forget(self.carrier.take());
        } else {
            if let Err(payload) = guard_drop(self.carrier.take()) {
                self.quarantine(payload);
            }
            if self.quarantined.load(Ordering::Acquire) {
                std::mem::forget(provider);
            } else if let Err(payload) = guard_drop(provider) {
                self.quarantine(payload);
            }
        }
    }
}

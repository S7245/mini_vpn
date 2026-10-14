//! Endpoint-session monotonic clock authority for resumable leg policy.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;

use super::two_leg::SimTime;

/// One fixed monotonic epoch shared by every physical leg and policy owner at
/// one resumable endpoint. Cloning shares identity as well as the epoch; a
/// numerically equal epoch minted elsewhere is not the same clock authority.
#[derive(Clone)]
pub(crate) struct SessionMonotonicClock {
    inner: Arc<SessionClockInner>,
}

struct SessionClockInner {
    epoch: tokio::time::Instant,
}

impl SessionMonotonicClock {
    pub(crate) fn start() -> Self {
        Self::from_epoch(tokio::time::Instant::now())
    }

    fn from_epoch(epoch: tokio::time::Instant) -> Self {
        Self {
            inner: Arc::new(SessionClockInner { epoch }),
        }
    }

    #[cfg(test)]
    pub(super) fn from_epoch_for_test(epoch: tokio::time::Instant) -> Self {
        Self::from_epoch(epoch)
    }

    pub(crate) fn now(&self) -> Result<SimTime, SessionClockError> {
        self.observe(tokio::time::Instant::now())
    }

    /// Prepares value-only admission/deadline timestamps from one actual sample.
    /// The caller retains this original window; this does not register a timer
    /// or carry terminal ownership. Tests live in Controller terminal_grace.
    pub(crate) fn deadline_window(
        &self,
        interval: Duration,
    ) -> Result<(SimTime, SimTime, tokio::time::Instant), SessionClockError> {
        if interval.is_zero() {
            return Err(SessionClockError::ZeroInterval);
        }
        let nanos = u64::try_from(interval.as_nanos()).map_err(|_| SessionClockError::Overflow)?;
        let sampled = tokio::time::Instant::now();
        let observed = self.observe(sampled)?;
        let deadline = observed
            .as_nanos()
            .checked_add(nanos)
            .ok_or(SessionClockError::Overflow)?;
        let absolute = sampled
            .checked_add(interval)
            .ok_or(SessionClockError::Overflow)?;
        Ok((observed, SimTime::from_nanos(deadline), absolute))
    }

    pub(crate) fn same_source(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    fn observe(&self, observed: tokio::time::Instant) -> Result<SimTime, SessionClockError> {
        let elapsed = observed
            .checked_duration_since(self.inner.epoch)
            .ok_or(SessionClockError::TimeWentBackwards)?;
        self.observe_elapsed(elapsed)
    }

    fn observe_elapsed(&self, elapsed: Duration) -> Result<SimTime, SessionClockError> {
        let nanos = u64::try_from(elapsed.as_nanos()).map_err(|_| SessionClockError::Overflow)?;
        Ok(SimTime::from_nanos(nanos))
    }
}

impl fmt::Debug for SessionMonotonicClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionMonotonicClock([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum SessionClockError {
    #[error("session deadline interval must be positive")]
    ZeroInterval,
    #[error("session monotonic clock moved before its fixed epoch")]
    TimeWentBackwards,
    #[error("session monotonic timestamp exceeds the exact u64-nanosecond domain")]
    Overflow,
}

#[cfg(test)]
mod tests;

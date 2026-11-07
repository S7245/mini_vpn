//! Saved Failover health actor: one original task, timer and in-flight observation.
//! Stop closes new turns; an entered probe finishes inside that original task.
//! Owns no recovery policy or DATA scheduling. Entry `spawn`; tests below.
//! A failed join is sticky quarantine, never permission to clean a Node.

use super::*;
use std::sync::atomic::AtomicBool;
use tokio::sync::Notify;
use tokio::task::{JoinError, JoinHandle};

#[cfg(test)]
#[derive(Default)]
pub(crate) struct HealthObservation {
    pub(crate) ticks: std::sync::atomic::AtomicUsize,
    pub(crate) probes_started: std::sync::atomic::AtomicUsize,
    pub(crate) probes_finished: std::sync::atomic::AtomicUsize,
    pub(crate) probes_ok: std::sync::atomic::AtomicUsize,
    pub(crate) joined: AtomicBool,
    pub(crate) quarantined: AtomicBool,
    abort: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
    failure: std::sync::Mutex<Option<HealthFailureFacts>>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HealthFailureFacts {
    pub(crate) task: tokio::task::Id,
    pub(crate) cancelled: bool,
    pub(crate) panicked: bool,
    pub(crate) original_storage: usize,
}

#[cfg(test)]
impl HealthObservation {
    /// Explicit fault injection only. Taking this capability does not consume
    /// the original JoinHandle or publish any join/settlement observation.
    pub(crate) fn abort_original_for_test(&self) -> Option<tokio::task::Id> {
        let original = self
            .abort
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        original.map(|handle| {
            let id = handle.id();
            handle.abort();
            id
        })
    }

    /// Copy of facts borrowed once from the installed original JoinError.
    /// Repeated reads prove this snapshot is stable, not a fresh error borrow.
    pub(crate) fn failure_for_test(&self) -> Option<HealthFailureFacts> {
        *self
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthExit {
    Stopped,
    Quarantined,
}

#[derive(Default)]
struct JoinReceipt {
    joined: AtomicBool,
    changed: Notify,
}

/// Read-only receipt of consuming the original JoinHandle, not of probe success.
/// Only HealthTask::join can release it. Unknown join still quarantines the root.
#[derive(Clone)]
pub(crate) struct HealthJoinObservation(Arc<JoinReceipt>);
impl HealthJoinObservation {
    pub(crate) async fn joined(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.0.joined.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

#[derive(Default)]
struct Stop {
    requested: AtomicBool,
    wake: Notify,
}
impl Stop {
    async fn observed(&self) {
        loop {
            let wait = self.wake.notified();
            tokio::pin!(wait);
            wait.as_mut().enable();
            if self.requested.load(Ordering::Acquire) {
                return;
            }
            wait.await;
        }
    }
}

/// Keep this original task until `join` supplies an actual receipt. Dropping a
/// join observation retains work; destroying the owner only requests stop and
/// supplies no asynchronous settlement proof. There is no abort capability.
#[must_use = "save and join the original health task before Node cleanup"]
pub struct HealthTask {
    task: JoinHandle<()>,
    stop: Arc<Stop>,
    exit: Option<HealthExit>,
    failure: Option<JoinError>,
    receipt: HealthJoinObservation,
    #[cfg(test)]
    state: std::sync::Weak<FailoverState>,
    #[cfg(test)]
    observation: Arc<HealthObservation>,
}
impl HealthTask {
    pub(crate) fn join_observation(&self) -> HealthJoinObservation {
        self.receipt.clone()
    }
    #[cfg(test)]
    pub(crate) fn observation_for_test(&self) -> Arc<HealthObservation> {
        self.observation.clone()
    }
    #[cfg(test)]
    pub(crate) fn cancel_original_for_service_test(&self) {
        self.task.abort();
    }
    #[cfg(test)]
    pub(crate) fn state_for_test(&self) -> std::sync::Weak<FailoverState> {
        self.state.clone()
    }
    pub fn request_stop(&self) {
        self.stop.requested.store(true, Ordering::Release);
        self.stop.wake.notify_waiters();
    }
    pub fn failure(&self) -> Option<&JoinError> {
        self.failure.as_ref()
    }
    pub async fn join(&mut self) -> HealthExit {
        if let Some(exit) = self.exit {
            return exit;
        }
        let exit = match (&mut self.task).await {
            // The actor's sole normal exit is an observed stop, not a flag
            // sampled later by this observer. Late stop cannot launder failure.
            Ok(()) => HealthExit::Stopped,
            Err(error) => {
                self.failure = Some(error);
                HealthExit::Quarantined
            }
        };
        self.exit = Some(exit);
        self.receipt.0.joined.store(true, Ordering::Release);
        self.receipt.0.changed.notify_waiters();
        #[cfg(test)]
        {
            // Borrow only after the original error has reached its final field.
            // Publish these Copy facts before the original joined Release below.
            let failure = self.failure.as_ref().map(|error| HealthFailureFacts {
                task: error.id(),
                cancelled: error.is_cancelled(),
                panicked: error.is_panic(),
                original_storage: error as *const JoinError as usize,
            });
            *self
                .observation
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = failure;
            self.observation
                .quarantined
                .store(exit == HealthExit::Quarantined, Ordering::Release);
            self.observation.joined.store(true, Ordering::Release);
        }
        exit
    }
}
impl Drop for HealthTask {
    fn drop(&mut self) {
        self.request_stop();
    }
}

pub(super) fn spawn<T: HealthProbe + 'static, R: Send + Sync + 'static>(
    upstream: &Arc<FailoverUpstream<T, R>>,
) -> HealthTask {
    let stop = Arc::new(Stop::default());
    let actor_stop = stop.clone();
    let state = upstream.state.clone();
    let tuic = upstream.tuic.clone();
    #[cfg(test)]
    let state_observation = Arc::downgrade(&state);
    #[cfg(test)]
    let observation = Arc::new(HealthObservation::default());
    #[cfg(test)]
    let actor_observation = observation.clone();
    let task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(HEALTH_TICK_SECS));
        let mut detector = BlackholeDetector::new();
        let mut secs_since_up_probe = 0u64;
        loop {
            tokio::select! {
                biased;
                _ = actor_stop.observed() => return,
                _ = tick.tick() => {},
            }
            if actor_stop.requested.load(Ordering::Acquire) {
                return;
            }
            #[cfg(test)]
            actor_observation.ticks.fetch_add(1, Ordering::Release);
            match state.active_leg() {
                TcpLeg::Tuic => {
                    secs_since_up_probe = 0;
                    // rx_datagrams 非阻塞：None=锁被占（重连中）→ 跳过本次观察（停滞计时靠 now 累积、不重置）。
                    if let Some(rx) = tuic.rx_datagrams().await
                        && detector.observe(rx, state.now_secs())
                        && state.record_blackhole(state.now_secs())
                    {
                        println!(
                            "🔀 failover：TUIC 黑洞（{BLACKHOLE_RX_STALE_SECS}s 无收包，连 keepalive ACK 都没有）→ 切到 REALITY 备路",
                        );
                        detector.reset();
                    }
                }
                TcpLeg::Reality => {
                    // 每 REALITY tick 复位（幂等）：保证切回 TUIC 时 detector 从干净状态开始计（非仅切换瞬间）。
                    detector.reset();
                    secs_since_up_probe += HEALTH_TICK_SECS;
                    if secs_since_up_probe >= PROBE_INTERVAL_SECS {
                        secs_since_up_probe = 0;
                        #[cfg(test)]
                        actor_observation
                            .probes_started
                            .fetch_add(1, Ordering::Release);
                        let ok = tuic.probe().await;
                        #[cfg(test)]
                        {
                            actor_observation
                                .probes_finished
                                .fetch_add(1, Ordering::Release);
                            actor_observation
                                .probes_ok
                                .fetch_add(usize::from(ok), Ordering::Release);
                        }
                        if state.record_probe(ok, state.now_secs()) {
                            println!("🔀 failover：TUIC 探针连续成功 + 冷却已过 → 切回 TUIC 主腿");
                        }
                    }
                }
            }
        }
    });
    #[cfg(test)]
    {
        *observation
            .abort
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(task.abort_handle());
    }
    HealthTask {
        task,
        stop,
        exit: None,
        failure: None,
        receipt: HealthJoinObservation(Arc::new(JoinReceipt::default())),
        #[cfg(test)]
        state: state_observation,
        #[cfg(test)]
        observation,
    }
}

#[cfg(test)]
mod tests;

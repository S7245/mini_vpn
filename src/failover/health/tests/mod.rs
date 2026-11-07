//! Control-custody tracer only; real Node/probe coverage belongs to service tests.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Probe {
    entered: AtomicUsize,
    completed: AtomicUsize,
    release: tokio::sync::Notify,
    panic: AtomicUsize,
}
#[async_trait::async_trait]
impl HealthProbe for Probe {
    async fn probe(&self) -> bool {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        assert_eq!(self.panic.load(Ordering::SeqCst), 0, "original probe panic");
        self.completed.fetch_add(1, Ordering::SeqCst);
        true
    }
    async fn is_dead(&self) -> bool {
        false
    }
    async fn rx_datagrams(&self) -> Option<u64> {
        Some(1)
    }
}

#[tokio::test(start_paused = true)]
async fn stop_joins_the_original_inflight_probe_before_returning_data() {
    let probe = Arc::new(Probe::default());
    let upstream = Arc::new(FailoverUpstream::new(probe.clone(), Arc::new(())));
    upstream.state.set_leg(TcpLeg::Reality);
    let mut task = upstream.spawn_health_probe();
    // Immediate tick plus nine original 3s ticks, preserving old 30s schedule.
    for _ in 0..10 {
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
    }
    tokio::task::yield_now().await;
    assert_eq!(probe.entered.load(Ordering::SeqCst), 1);
    drop(upstream);
    assert_eq!(Arc::strong_count(&probe), 2, "original task owns DATA");
    let receipt = task.join_observation();
    task.request_stop();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(15), task.join())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(1), receipt.joined())
            .await
            .is_err()
    );
    assert_eq!(
        probe.completed.load(Ordering::SeqCst),
        0,
        "stop never cancels original probe"
    );
    assert_eq!(Arc::strong_count(&probe), 2);
    probe.release.notify_one();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), task.join())
            .await
            .unwrap(),
        HealthExit::Stopped
    );
    tokio::time::timeout(std::time::Duration::from_millis(1), receipt.joined())
        .await
        .unwrap();
    assert_eq!(probe.completed.load(Ordering::SeqCst), 1);
    assert_eq!(
        Arc::strong_count(&probe),
        1,
        "actual JoinHandle returns DATA"
    );
    assert_eq!(
        task.join().await,
        HealthExit::Stopped,
        "cached receipt, never repoll completed handle"
    );
    tokio::time::advance(std::time::Duration::from_secs(90)).await;
    assert_eq!(probe.entered.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn stopped_before_first_turn_never_admits_observation_or_probe() {
    let probe = Arc::new(Probe::default());
    let upstream = Arc::new(FailoverUpstream::new(probe.clone(), Arc::new(())));
    upstream.state.set_leg(TcpLeg::Reality);
    let mut task = upstream.spawn_health_probe();
    task.request_stop();
    assert_eq!(task.join().await, HealthExit::Stopped);
    assert_eq!(task.observation.ticks.load(Ordering::Acquire), 0);
    assert_eq!(probe.entered.load(Ordering::SeqCst), 0);
    assert!(task.failure().is_none());
}

#[tokio::test(start_paused = true)]
async fn original_probe_panic_keeps_same_join_error_after_late_stop() {
    let probe = Arc::new(Probe::default());
    probe.panic.store(1, Ordering::SeqCst);
    probe.release.notify_one();
    let upstream = Arc::new(FailoverUpstream::new(probe.clone(), Arc::new(())));
    upstream.state.set_leg(TcpLeg::Reality);
    let mut task = upstream.spawn_health_probe();
    for _ in 0..10 {
        tokio::task::yield_now().await;
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
    }
    assert_eq!(task.join().await, HealthExit::Quarantined);
    let original = task.failure().unwrap() as *const JoinError;
    assert!(task.failure().unwrap().is_panic());
    task.request_stop();
    assert_eq!(task.join().await, HealthExit::Quarantined);
    assert_eq!(task.failure().unwrap() as *const JoinError, original);
    assert!(task.observation.quarantined.load(Ordering::Acquire));
}

//! Actual Control notifications and saved-factory destruction, no transport run.
use super::*;
use std::sync::{
    Weak,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Context, Wake, Waker};

struct PhaseProbe {
    owner: Weak<Control>,
    unlocked: AtomicBool,
}
impl Wake for PhaseProbe {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.unlocked.store(
            self.owner.upgrade().unwrap().phase.try_lock().is_ok(),
            Ordering::SeqCst,
        );
    }
}
#[tokio::test]
async fn actual_phase_wake_observes_original_mutex_unlocked() {
    let control = new_control(true);
    let probe = Arc::new(PhaseProbe {
        owner: Arc::downgrade(&control),
        unlocked: AtomicBool::new(false),
    });
    let waker = Waker::from(probe.clone());
    let mut wait = Box::pin(control.changed.notified());
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    control.publish(Phase::Running);
    assert!(
        probe.unlocked.load(Ordering::SeqCst),
        "phase callback must not hold phase lock"
    );
}

struct WakeBomb {
    payload: Arc<AtomicUsize>,
    fired: AtomicBool,
}
impl Wake for WakeBomb {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if !self.fired.swap(true, Ordering::SeqCst) {
            std::panic::panic_any(self.payload.clone());
        }
    }
}
struct DropBomb {
    payload: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Drop for DropBomb {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(self.payload.clone());
    }
}
#[tokio::test]
async fn actual_control_wake_then_saved_factory_drop_preserves_both_panics_without_abort() {
    let first = Arc::new(AtomicUsize::new(17));
    let second = Arc::new(AtomicUsize::new(29));
    let drops = Arc::new(AtomicUsize::new(0));
    let bomb = DropBomb {
        payload: second.clone(),
        drops: drops.clone(),
    };
    let mut service = prepare_factory(
        move |meter: Option<bytes::BackingMeter>| async move {
            let _guard = bomb;
            let _storage =
                bytes::Bytes::copy_from_slice_metered(b"actual factory storage", &meter.unwrap())
                    .unwrap();
            std::future::pending::<Result<LoopbackTunDevice, ClientError>>().await
        },
        Configuration {
            tuic: fixture::config("127.0.0.1:9".parse().unwrap()),
            composition: (),
        },
        TunRuntimeConfig::from_sources(Some("1"))
            .unwrap()
            .with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(65536), 1)
            .unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let control = service.control.clone();
    let budget = control.backing_budget.clone().unwrap();
    {
        let mut run = Box::pin(service.run());
        assert!(
            run.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    let limit = control.request_stop();
    let bomb = Arc::new(WakeBomb {
        payload: first.clone(),
        fired: AtomicBool::new(false),
    });
    let waker = Waker::from(bomb.clone());
    let mut wait = Box::pin(control.changed.notified());
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(service)));
    drop((wait, waker, bomb));
    assert!(result.is_ok(), "wake must not bypass actual saved disposal");
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        *control
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        Phase::Quarantined
    );
    assert_eq!(*control.deadline.lock().unwrap(), Some(limit));
    assert_eq!(Arc::strong_count(&first), 2);
    assert_eq!(
        control.terminal.first_site(),
        super::super::saved::Site::Notification as usize
    );
    {
        let slot = control.terminal.notification.lock().unwrap();
        assert!(Arc::ptr_eq(
            slot.as_ref()
                .unwrap()
                .downcast_ref::<Arc<AtomicUsize>>()
                .unwrap(),
            &first
        ));
    }
    let last = control.terminal.disposal.lock().unwrap();
    assert!(Arc::ptr_eq(
        last.as_ref()
            .unwrap()
            .downcast_ref::<Arc<AtomicUsize>>()
            .unwrap(),
        &second
    ));
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
}

struct Reentrant {
    owner: Weak<Control>,
    calls: AtomicUsize,
}
impl Wake for Reentrant {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let owner = self.owner.upgrade().unwrap();
        owner.request_stop();
        owner.publish(Phase::Stopping);
    }
}
#[tokio::test]
async fn actual_control_callback_can_reenter_phase_and_stop_without_lock_or_recursive_wake() {
    let control = new_control(true);
    let probe = Arc::new(Reentrant {
        owner: Arc::downgrade(&control),
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(probe.clone());
    let mut wait = Box::pin(control.changed.notified());
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    control.publish(Phase::Running);
    assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
    assert_eq!(*control.phase.lock().unwrap(), Phase::Stopping);
    assert!(control.stop.stopping.load(Ordering::Acquire));
    assert!(!control.notifications.failed());
}

struct PausedWake {
    entered: std::sync::mpsc::SyncSender<()>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}
impl Wake for PausedWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.entered.send(()).unwrap();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
    }
}
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn concurrent_phase_change_wakes_waiter_registered_during_prior_notification() {
    let control = new_control(true);
    let (entered_tx, entered) = std::sync::mpsc::sync_channel(1);
    let (release, release_rx) = std::sync::mpsc::sync_channel(1);
    let waker = Waker::from(Arc::new(PausedWake {
        entered: entered_tx,
        release: Mutex::new(release_rx),
    }));
    let mut first = Box::pin(control.changed.notified());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let owner = control.clone();
    let worker = std::thread::spawn(move || owner.publish(Phase::Running));
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    let counted = Arc::new(CountWake(AtomicUsize::new(0)));
    let next_waker = Waker::from(counted.clone());
    let mut next = Box::pin(control.changed.notified());
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&next_waker))
            .is_pending()
    );
    control.publish(Phase::Stopping);
    release.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(counted.0.load(Ordering::SeqCst), 1);
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&next_waker))
            .is_ready()
    );
    assert_eq!(*control.phase.lock().unwrap(), Phase::Stopping);
    assert!(!control.notifications.failed());
}

#[tokio::test]
async fn observing_notification_quarantine_cannot_let_later_disposal_steal_first_capture() {
    let first = Arc::new(AtomicUsize::new(17));
    let second = Arc::new(AtomicUsize::new(29));
    let drops = Arc::new(AtomicUsize::new(0));
    let bomb = DropBomb {
        payload: second.clone(),
        drops: drops.clone(),
    };
    let mut service = prepare_factory(
        move |_: Option<bytes::BackingMeter>| async move {
            let _guard = bomb;
            std::future::pending::<Result<LoopbackTunDevice, ClientError>>().await
        },
        Configuration {
            tuic: fixture::config("127.0.0.1:9".parse().unwrap()),
            composition: (),
        },
        TunRuntimeConfig::from_sources(Some("1"))
            .unwrap()
            .with_native_limits(crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::native_hard(65536), 1)
            .unwrap(),
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let control = service.control.clone();
    let budget = control.backing_budget.clone().unwrap();
    {
        let mut run = Box::pin(service.run());
        assert!(
            run.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    let waker = Waker::from(Arc::new(WakeBomb {
        payload: first.clone(),
        fired: AtomicBool::new(false),
    }));
    let mut wait = Box::pin(control.changed.notified());
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let (entered_tx, entered) = std::sync::mpsc::sync_channel(1);
    let (resume, resume_rx) = std::sync::mpsc::sync_channel(1);
    *control.notifications.after_quarantine.lock().unwrap() = Some((entered_tx, resume_rx));
    let owner = control.clone();
    let worker = std::thread::spawn(move || owner.publish(Phase::Running));
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    let phase = *control.phase.lock().unwrap();
    drop(service);
    resume.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(phase, Phase::Quarantined);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
    assert_eq!(
        control.terminal.first_site(),
        super::super::saved::Site::Notification as usize
    );
    let slot = control.terminal.notification.lock().unwrap();
    assert!(Arc::ptr_eq(
        slot.as_ref()
            .unwrap()
            .downcast_ref::<Arc<AtomicUsize>>()
            .unwrap(),
        &first
    ));
    let slot = control.terminal.disposal.lock().unwrap();
    assert!(Arc::ptr_eq(
        slot.as_ref()
            .unwrap()
            .downcast_ref::<Arc<AtomicUsize>>()
            .unwrap(),
        &second
    ));
}

impl super::super::notifications::Notifications {
    pub(in crate::client_tun::compatibility_runtime::tuic_service) fn pause_after_quarantine_for_test(
        &self,
    ) {
        if let Some((entered, resume)) = self.after_quarantine.lock().unwrap().take() {
            entered.send(()).unwrap();
            resume.recv_timeout(Duration::from_secs(2)).unwrap();
        }
    }
}

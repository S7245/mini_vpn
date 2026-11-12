//! Original saved driver -> owning finisher -> borrowed callback tail. Native
//! Egress-only shell: no full Service, RX or factory acceptance is claimed.
use super::*;
use device::{Gate, GatedDevice};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};

struct Blocked {
    entered: std::sync::mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
}
impl Wake for Blocked {
    fn wake(self: Arc<Self>) {
        self.entered.send(()).unwrap();
        let (lock, condition) = &*self.release;
        let mut ready = lock.lock().unwrap();
        while !*ready {
            ready = condition.wait(ready).unwrap();
        }
    }
}
struct SchedulePanic(Arc<AtomicUsize>);
impl Wake for SchedulePanic {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(self.0.clone());
    }
}

#[tokio::test(start_paused = true)]
async fn native_link_tail_self_wake_then_actual_device_drop_retains_both_without_abort() {
    const CHILD: &str = "N1_CALLBACK_SHUTDOWN_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "client_tun::compatibility_runtime::tuic_service::tests::shutdown_callback::native_link_tail_self_wake_then_actual_device_drop_retains_both_without_abort",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .status()
            .unwrap();
        assert!(
            status.success(),
            "borrowed tail/actual Device Drop child: {status}"
        );
        return;
    }
    let (budget, mut raw) = LoopbackTunDevice::test_native_egress_only_shell();
    let peer = raw.test_outbound_link();
    for _ in 0..4 {
        peer.push(bytes::BytesMut::from(&b"old"[..]));
    }
    raw.inject_ip_packet_with(3, |_| Ok(3)).unwrap();
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let (sent, received) = std::sync::mpsc::channel();
    let original = Waker::from(Arc::new(Blocked {
        entered: sent,
        release: release.clone(),
    }));
    let runner;
    {
        let mut flush = Box::pin(raw.flush_tx());
        assert!(
            flush
                .as_mut()
                .poll(&mut Context::from_waker(&original))
                .is_pending()
        );
        runner = std::thread::spawn(move || {
            peer.test_pop();
        });
        received.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(
            flush.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(()))
        ));
    }
    drop(original);
    let gate = Arc::new(Gate::default());
    gate.release.store(true, Ordering::SeqCst);
    let first = Arc::new(AtomicUsize::new(17));
    let second = Arc::new(AtomicUsize::new(29));
    *gate.drop_payload.lock().unwrap() = Some(second.clone());
    let device = GatedDevice {
        inner: raw,
        gate: gate.clone(),
    };
    let saved = control_with_budget(false, Some(budget.clone()));
    let owner = Arc::new({ let mut control = SessionControl::default(); control.app_meter = saved.app_meter.clone(); control });
    let deadline = owner.request_stop();
    tokio::time::advance(Duration::from_secs(3)).await;
    let retained = owner.clone();
    let mut driver = saved::Driver::new(Box::pin(async move {
        business::finish_device(device, None, retained).await;
        panic!("unknown callback tail cannot complete the original driver");
    }));
    let scheduler = Waker::from(Arc::new(SchedulePanic(first.clone())));
    assert!(
        driver
            .poll(&mut Context::from_waker(&scheduler), &saved)
            .is_pending()
    );
    drop(scheduler);
    assert_eq!(gate.drops.load(Ordering::SeqCst), 0);
    assert!(
        saved.terminal.poll.lock().unwrap().is_none(),
        "outer catch is too late"
    );
    assert_eq!(
        Arc::strong_count(&first),
        2,
        "original first Box retained before Q"
    );
    {
        let ledger = owner.ledger();
        let facts = ledger.first_failure.as_ref().unwrap().facts();
        assert_eq!(facts.class, crate::shared::errors::FailureClass::Panic);
        assert_eq!(facts.origin.phase, FailurePhase::DeviceShutdown);
        assert!(ledger.first_egress_io.is_none());
        assert_eq!(ledger.shutdown_deadline, Some(deadline));
        assert_eq!(ledger.additional_failures, 0);
        assert!(ledger.quarantined());
    }
    assert!(budget.quarantined());
    {
        let (lock, condition) = &*release;
        *lock.lock().unwrap() = true;
        condition.notify_all();
    }
    runner.join().unwrap();
    driver.dispose(&saved);
    assert_eq!(gate.drops.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&first), 2);
    assert!(Arc::ptr_eq(
        saved
            .terminal
            .disposal
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .downcast_ref::<Arc<AtomicUsize>>()
            .unwrap(),
        &second,
    ));
    assert_eq!(budget.used(), 1);
}

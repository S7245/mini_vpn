use super::*;
use std::task::{RawWaker, RawWakerVTable, Waker};

struct CloneQuarantine(BackingMeter);
unsafe fn clone_q(data: *const ()) -> RawWaker {
    let value = unsafe { &*data.cast::<CloneQuarantine>() };
    value.0.quarantine();
    unsafe { Arc::increment_strong_count(data.cast::<CloneQuarantine>()) };
    RawWaker::new(data, &Q_VTABLE)
}
unsafe fn drop_q(data: *const ()) {
    drop(unsafe { Arc::from_raw(data.cast::<CloneQuarantine>()) });
}
unsafe fn wake_q(data: *const ()) {
    unsafe { drop_q(data) };
}
unsafe fn wake_ref_q(_: *const ()) {}
static Q_VTABLE: RawWakerVTable = RawWakerVTable::new(clone_q, wake_q, wake_ref_q, drop_q);

#[tokio::test]
async fn callback_quarantine_prevents_actual_peer_publication() {
    for full in [false, true] {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let mut owner = crate::device::egress::Egress::native(1, 1500, 0, &meter).unwrap();
        owner
            .inject(3, 0, |out| {
                out.copy_from_slice(b"one");
                Ok(3)
            })
            .unwrap();
        let link = PacketLink::bounded(1);
        if full {
            assert!(link.push(BytesMut::from(&b"occupied"[..])));
        }
        let mut report = Report::default();
        let waker = unsafe {
            Waker::from_raw(RawWaker::new(
                Arc::into_raw(Arc::new(CloneQuarantine(meter))).cast(),
                &Q_VTABLE,
            ))
        };
        let mut cx = Context::from_waker(&waker);
        let outcome = {
            let work = flush(&mut owner, &link, &mut report);
            tokio::pin!(work);
            work.as_mut().poll(&mut cx)
        };
        let accepted = link.queue.lock().unwrap().len();
        let retained = owner.owned_bytes();
        owner.discard(&mut (0, 0));
        drop(owner);
        assert!(matches!(outcome, Poll::Ready(Err(_))));
        assert_eq!(
            accepted,
            usize::from(full),
            "Q during waiter clone must precede peer publication"
        );
        assert_eq!(retained, 3);
        assert!(
            budget.used() > 1,
            "the returned typed failure still owns actual cold State"
        );
        drop(outcome);
        assert_eq!(budget.used(), 1);
        assert!(budget.quarantined());
    }
}

struct CountWake {
    count: AtomicU64,
    link: PacketLink,
}
impl std::task::Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.link.queue.try_lock().is_ok(),
            "callback under peer queue lock"
        );
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}
#[tokio::test]
async fn all_actual_space_release_paths_wake_the_same_selected_writer() {
    for release in 0..3 {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let mut owner = crate::device::egress::Egress::native(1, 1500, 0, &meter).unwrap();
        owner
            .inject(3, 0, |out| {
                out.copy_from_slice(b"one");
                Ok(3)
            })
            .unwrap();
        let link = PacketLink::bounded(1);
        assert!(link.push(BytesMut::from(&b"old"[..])));
        let wake = Arc::new(CountWake {
            count: AtomicU64::new(0),
            link: link.clone(),
        });
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        let mut report = Report::default();
        {
            let work = flush(&mut owner, &link, &mut report);
            tokio::pin!(work);
            assert!(work.as_mut().poll(&mut cx).is_pending());
            match release {
                0 => {
                    assert_eq!(link.pop().unwrap().as_ref(), b"old");
                }
                1 => {
                    let mut pump = crate::device::pump_native::Prepared::new(1, &meter)
                        .unwrap()
                        .spawn(crate::harness::loopback::native::PacketSource::new(
                            link.clone(),
                        ));
                    let received = tokio::time::timeout(Duration::from_secs(1), pump.recv())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                    assert_eq!(received.as_ref(), b"old");
                    drop(received);
                    std::future::poll_fn(|cx| pump.poll_shutdown(cx)).await;
                }
                _ => {
                    let mut peer = LoopbackTunDevice::new(link.clone(), PacketLink::bounded(1));
                    let cut = peer.cut_ingress().await.unwrap();
                    assert_eq!(cut.discarded_packets, 1);
                    std::future::poll_fn(|cx| peer.poll_shutdown(cx)).await;
                }
            }
            assert_eq!(wake.count.load(Ordering::Relaxed), 1);
            assert!(matches!(work.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
        }
        assert_eq!(link.pop().unwrap().as_ref(), b"one");
        assert!(link.pop().is_none());
        drop(owner);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[tokio::test]
async fn poisoned_peer_retains_selected_and_sticky_charge() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut owner = crate::device::egress::Egress::native(1, 1500, 0, &meter).unwrap();
    owner.inject(3, 0, |_| Ok(3)).unwrap();
    let link = PacketLink::bounded(1);
    let _ = std::panic::catch_unwind(|| {
        let _lock = link.queue.lock().unwrap();
        panic!("peer poison");
    });
    let mut report = Report::default();
    assert!(flush(&mut owner, &link, &mut report).await.is_err());
    assert_eq!(owner.owned_bytes(), 3);
    assert!(owner.summaries.is_empty());
    assert_eq!(link.queue.lock().unwrap_err().get_ref().len(), 0);
    owner.discard(&mut (0, 0));
    drop(owner);
    assert_eq!(budget.used(), 1);
}

struct Payload {
    name: &'static str,
    alive: Arc<()>,
}
struct DropPanic {
    armed: Arc<AtomicBool>,
    alive: Arc<()>,
}
unsafe fn clone_drop(data: *const ()) -> RawWaker {
    unsafe { Arc::increment_strong_count(data.cast::<DropPanic>()) };
    RawWaker::new(data, &DROP_VTABLE)
}
unsafe fn drop_panic(data: *const ()) {
    let data = unsafe { Arc::from_raw(data.cast::<DropPanic>()) };
    if data.armed.swap(false, Ordering::AcqRel) {
        std::panic::panic_any(Payload {
            name: "secondary-waiter-drop",
            alive: data.alive.clone(),
        });
    }
}
unsafe fn wake_drop(data: *const ()) {
    unsafe { drop_panic(data) };
}
static DROP_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_drop, wake_drop, wake_ref_q, drop_panic);
struct PrimaryWake {
    armed: Arc<AtomicBool>,
    alive: Arc<()>,
}
impl std::task::Wake for PrimaryWake {
    fn wake(self: Arc<Self>) {
        self.armed.store(true, Ordering::Release);
        std::panic::panic_any(Payload {
            name: "primary-peer-wake",
            alive: self.alive.clone(),
        });
    }
}
#[test]
fn actual_peer_wake_and_waiter_drop_keep_first_and_secondary_box() {
    const CHILD: &str = "N1_LINK_PANIC_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "harness::packet_link::native::tests::actual_peer_wake_and_waiter_drop_keep_first_and_secondary_box", "--nocapture"])
            .env(CHILD, "1").output().unwrap();
        println!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "actual double panic child: {}",
            output.status
        );
        return;
    }
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut owner = crate::device::egress::Egress::native(1, 1500, 0, &meter).unwrap();
    owner.inject(3, 0, |_| Ok(3)).unwrap();
    let link = PacketLink::bounded(1);
    let armed = Arc::new(AtomicBool::new(false));
    let first = Arc::new(());
    let second = Arc::new(());
    let peer_waker = Waker::from(Arc::new(PrimaryWake {
        armed: armed.clone(),
        alive: first.clone(),
    }));
    // Keep the pinned peer waiter live through the actual publication callback.
    let mut peer_wait = Box::pin(link.notify.clone().notified_owned());
    assert!(
        peer_wait
            .as_mut()
            .poll(&mut Context::from_waker(&peer_waker))
            .is_pending()
    );
    drop(peer_waker);
    let writer_waker = unsafe {
        Waker::from_raw(RawWaker::new(
            Arc::into_raw(Arc::new(DropPanic {
                armed,
                alive: second.clone(),
            }))
            .cast(),
            &DROP_VTABLE,
        ))
    };
    let mut report = Report::default();
    let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut work = Box::pin(flush(&mut owner, &link, &mut report));
        let _ = work.as_mut().poll(&mut Context::from_waker(&writer_waker));
    }))
    .unwrap_err();
    let primary = error
        .downcast::<Payload>()
        .unwrap_or_else(|_| panic!("lost primary payload"));
    assert_eq!(primary.name, "primary-peer-wake");
    assert!(Arc::ptr_eq(&primary.alive, &first));
    drop(primary);
    drop(writer_waker);
    drop(peer_wait);
    assert_eq!(link.queue.lock().unwrap().len(), 1);
    assert_eq!(owner.owned_bytes(), 3);
    assert!(owner.summaries.is_empty());
    assert!(budget.quarantined());
    assert_eq!(Arc::strong_count(&second), 2);
    owner.discard(&mut (0, 0));
    drop(owner);
    assert_eq!(budget.used(), 1);
    assert_eq!(
        Arc::strong_count(&second),
        2,
        "opaque secondary remains retained after owner destruction"
    );
}

struct SpacePanic;
impl std::task::Wake for SpacePanic {
    fn wake(self: Arc<Self>) {
        panic!("actual-space-wake");
    }
}
#[tokio::test]
async fn actual_space_wake_panic_quarantines_original_selected_owner() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut owner = crate::device::egress::Egress::native(1, 1500, 0, &meter).unwrap();
    owner.inject(3, 0, |_| Ok(3)).unwrap();
    let link = PacketLink::bounded(1);
    assert!(link.push(BytesMut::from(&b"old"[..])));
    let mut report = Report::default();
    {
        let work = flush(&mut owner, &link, &mut report);
        tokio::pin!(work);
        let waker = Waker::from(Arc::new(SpacePanic));
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        assert!(
            std::panic::catch_unwind(|| {
                link.pop();
            })
            .is_ok()
        );
        let Poll::Ready(Err(error)) = work.as_mut().poll(&mut Context::from_waker(&waker)) else {
            panic!("callback first must return through original writer");
        };
        assert!(error.to_string().contains("Wake"));
    }
    let retained = owner.owned_bytes();
    let quarantined = budget.quarantined();
    owner.discard(&mut (0, 0));
    drop(owner);
    assert!(
        quarantined,
        "peer-driven wake interruption must reach the original Egress authority"
    );
    assert_eq!(retained, 3);
    assert_eq!(budget.used(), 1);
}

struct WakeDropBomb {
    first: Arc<()>,
    second: Arc<()>,
}
impl std::task::Wake for WakeDropBomb {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(Payload {
            name: "space-first",
            alive: self.first.clone(),
        });
    }
}
impl Drop for WakeDropBomb {
    fn drop(&mut self) {
        std::panic::panic_any(Payload {
            name: "space-second",
            alive: self.second.clone(),
        });
    }
}
#[test]
fn native_link_last_waker_wake_and_drop_keep_original_first() {
    const CHILD: &str = "N1_LINK_LAST_WAKE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "harness::packet_link::native::tests::native_link_last_waker_wake_and_drop_keep_original_first", "--nocapture"])
            .env(CHILD, "1").output().unwrap();
        println!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.status.success(),
            "last original waker child: {}",
            output.status
        );
        return;
    }
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut owner = crate::device::egress::Egress::native(1, 1500, 0, &meter).unwrap();
    owner.inject(3, 0, |_| Ok(3)).unwrap();
    let link = PacketLink::bounded(1);
    link.push(BytesMut::from(&b"old"[..]));
    let first = Arc::new(());
    let second = Arc::new(());
    let mut report = Report::default();
    {
        let mut work = Box::pin(flush(&mut owner, &link, &mut report));
        let waker = Waker::from(Arc::new(WakeDropBomb {
            first: first.clone(),
            second: second.clone(),
        }));
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        drop(waker);
        assert!(
            std::panic::catch_unwind(|| {
                link.pop();
            })
            .is_ok()
        );
        let Poll::Ready(Err(error)) = work.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("expected retained original callback failure");
        };
        assert!(error.to_string().contains("Wake"));
        assert_eq!(Arc::strong_count(&first), 2, "original first Box retained");
    }
    assert!(budget.quarantined());
    assert_eq!(owner.owned_bytes(), 3);
    assert_eq!(Arc::strong_count(&second), 2);
    owner.discard(&mut (0, 0));
    drop(owner);
    assert_eq!(budget.used(), 1);
    assert_eq!(Arc::strong_count(&second), 2);
}

struct BlockingWake {
    active: Arc<std::sync::atomic::AtomicUsize>,
    peak: Arc<std::sync::atomic::AtomicUsize>,
    entered: std::sync::mpsc::Sender<&'static str>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
}
impl std::task::Wake for BlockingWake {
    fn wake(self: Arc<Self>) {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        self.entered.send("entered").unwrap();
        let (lock, condition) = &*self.release;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = condition.wait(released).unwrap();
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}
#[test]
fn native_link_peer_releases_keep_one_callback_in_flight() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut owner = crate::device::egress::Egress::native(2, 1500, 0, &meter).unwrap();
    for _ in 0..2 {
        owner.inject(3, 0, |_| Ok(3)).unwrap();
    }
    let link = PacketLink::bounded(1);
    link.push(BytesMut::from(&b"old"[..]));
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let (sent, received) = std::sync::mpsc::channel();
    let a = Waker::from(Arc::new(BlockingWake {
        active: active.clone(),
        peak: peak.clone(),
        entered: sent.clone(),
        release: release.clone(),
    }));
    let b = Waker::from(Arc::new(BlockingWake {
        active: active.clone(),
        peak: peak.clone(),
        entered: sent.clone(),
        release: release.clone(),
    }));
    let mut report = Report::default();
    {
        let mut work = Box::pin(flush(&mut owner, &link, &mut report));
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(&a))
                .is_pending()
        );
        let first = {
            let link = link.clone();
            std::thread::spawn(move || {
                link.pop();
            })
        };
        assert_eq!(
            received.recv_timeout(Duration::from_secs(2)).unwrap(),
            "entered"
        );
        // The same writer is polled while its preceding wake is still executing.
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(&b))
                .is_pending()
        );
        let second = {
            let link = link.clone();
            std::thread::spawn(move || {
                link.pop();
                sent.send("returned").unwrap();
            })
        };
        let second_event = received.recv_timeout(Duration::from_secs(2)).unwrap();
        {
            let (lock, condition) = &*release;
            *lock.lock().unwrap() = true;
            condition.notify_all();
        }
        first.join().unwrap();
        second.join().unwrap();
        assert!(matches!(
            work.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(()))
        ));
        assert!(matches!(second_event, "entered" | "returned"));
    }
    drop(owner);
    assert_eq!(
        peak.load(Ordering::SeqCst),
        1,
        "one writer does not itself bound in-flight original callbacks"
    );
    assert_eq!(budget.used(), 0);
}

impl PacketLink {
    pub(crate) fn test_pop(&self) -> Option<BytesMut> {
        self.pop()
    }
}

impl PacketLink {
    pub(crate) fn packet_count_for_test(&self) -> usize {
        self.queue.lock().unwrap().len()
    }
}

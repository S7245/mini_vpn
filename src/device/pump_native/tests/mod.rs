//! Actual original actor notification fields, isolated in a child for abort evidence.
use super::*;
use std::{any::Any, sync::atomic::AtomicUsize, task::{RawWaker, RawWakerVTable, Waker}};
struct Data {
    clones: AtomicUsize,
    drops: AtomicUsize,
    payloads: [Arc<()>; 2],
}
struct Handle { data: Arc<Data>, bomb: Option<usize> }
unsafe fn clone_waker(raw: *const ()) -> RawWaker {
    let handle = unsafe { &*raw.cast::<Handle>() };
    let id = handle.data.clones.fetch_add(1, Ordering::SeqCst);
    RawWaker::new(Box::into_raw(Box::new(Handle {
        data: handle.data.clone(), bomb: (id < 2).then_some(id),
    })).cast(), &VTABLE)
}
unsafe fn drop_waker(raw: *const ()) {
    let handle = unsafe { Box::from_raw(raw.cast_mut().cast::<Handle>()) };
    if let Some(id) = handle.bomb {
        handle.data.drops.fetch_add(1, Ordering::SeqCst);
        let payload: Box<dyn Any + Send> = Box::new(handle.data.payloads[id].clone());
        std::panic::resume_unwind(payload);
    }
}
unsafe fn wake(raw: *const ()) { unsafe { drop_waker(raw) }; }
unsafe fn wake_ref(_: *const ()) {}
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake, wake_ref, drop_waker);

#[test]
fn actual_actor_stop_and_pause_drop_keep_two_original_boxes_without_abort() {
    const CHILD: &str = "N1_RX_ACTOR_NOTIFICATION_DROP_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "device::pump_native::tests::actual_actor_stop_and_pause_drop_keep_two_original_boxes_without_abort", "--nocapture"])
            .env(CHILD, "1").output().unwrap();
        eprintln!("actual notification Drop child status={}\n{}\n{}", output.status,
            String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success(), "actual STOP and PAUSE Drop must be independently custodied");
        return;
    }
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let (tx, rx) = callback::Custody::pair(&meter).unwrap();
        let stock = rx_packet::Stock::new(&meter, rx).unwrap();
        let prepared = Prepared::with_ingress(1, &meter, Some(stock.access())).unwrap();
        let control = prepared.control;
        let (reader, peer) = tokio::io::duplex(2048);
        let actor = Actor {
            source: ManuallyDrop::new(AsyncReadPacketSource { reader, mtu: 1200 }),
            packet: None, sender: prepared.sender,
            stop: ManuallyDrop::new(control.stop_wake.clone().notified_owned()),
            pause: ManuallyDrop::new(control.pause.changed.clone().notified_owned()),
            control: control.clone(), phase: Phase::Reading, full_recorded: false, capacity: 1,
        };
        let data = Arc::new(Data { clones: AtomicUsize::new(0), drops: AtomicUsize::new(0), payloads: [Arc::new(()), Arc::new(())] });
        let waker = unsafe { Waker::from_raw(RawWaker::new(Box::into_raw(Box::new(Handle { data: data.clone(), bomb: None })).cast(), &VTABLE)) };
        let mut work = Box::pin(PumpGuard { inner: Some(Box::pin(actor)), control: control.clone(), settled: false });
        assert!(work.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        assert!(data.clones.load(Ordering::SeqCst) >= 2);
        drop(work);
        assert_eq!(data.drops.load(Ordering::SeqCst), 2);
        for payload in &data.payloads { assert_eq!(Arc::strong_count(payload), 2); }
        assert!(budget.quarantined());
        drop((waker, data, prepared.receiver, peer, control, stock, tx));
        assert!(budget.used() > 0);
    });
}

impl Prepared {
    pub(crate) fn test_control_bytes() -> usize { std::mem::size_of::<PumpControl>() }
}

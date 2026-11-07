use super::*;
use std::future::Future;
struct Retired(Arc<AtomicUsize>);
impl Wake for Retired {
    fn wake(self: Arc<Self>) {}
}
impl Drop for Retired {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn actual_native_recv_future_cancellation_retires_original_registration() {
    use crate::shared::bounded_channel::{Receiver as LaneReceiver, Rx};
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _facts) = clock(&meter);
    let (tx, rx, observer) = prepare::<Item>(2, 2, &meter, clock).unwrap();
    let mut rx = LaneReceiver(Rx::Native(rx));
    let retired = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(Retired(retired.clone())));
    let mut recv = Box::pin(rx.recv());
    assert!(
        recv.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    drop(waker);
    drop(recv);
    assert_eq!(
        retired.load(Ordering::SeqCst),
        1,
        "cancelled future must release its original waiter while receiver stays alive"
    );
    assert!(observer.0.core.callbacks().waiter.is_none());
    let drops = Arc::new(AtomicUsize::new(0));
    tx.try_reserve().unwrap().send(Item {
        bytes: 7,
        drops: drops.clone(),
    });
    assert_eq!(rx.try_recv().unwrap().bytes, 7);
    drop((tx, rx));
    assert!(observer.finish().is_ok());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(budget.used(), 0);
}

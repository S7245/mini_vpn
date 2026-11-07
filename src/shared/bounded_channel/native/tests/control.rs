use super::*;
struct WakeCount(Arc<AtomicUsize>);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn close_admission_keeps_old_unit_and_notifies_eof_with_sender_alive() {
    for publish in [false, true] {
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let (clock, _facts) = clock(&meter);
        let (tx, mut rx, observer) = prepare::<Item>(1, 2, &meter, clock).unwrap();
        let old = tx.try_reserve().unwrap();
        rx.close();
        assert!(tx.is_closed());
        assert!(matches!(tx.try_reserve(), Err(ReserveError::Closed)));
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(WakeCount(wakes.clone())));
        assert!(rx.poll_recv(&mut Context::from_waker(&waker)).is_pending());
        let drops = Arc::new(AtomicUsize::new(0));
        if publish {
            old.send(Item {
                bytes: 7,
                drops: drops.clone(),
            });
        } else {
            drop(old);
        }
        assert_eq!(
            wakes.load(Ordering::SeqCst),
            1,
            "last old Unit must wake close EOF, with or without publication"
        );
        if publish {
            assert_eq!(rx.try_recv().unwrap().bytes, 7);
        }
        assert!(matches!(
            rx.poll_recv(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(None)
        ));
        // Test owns all original producers and receiver; no operation can run.
        assert!(unsafe { rx.empty_under_producer_seal() });
        assert!(
            !observer.settled(),
            "control empty is not final physical retirement"
        );
        assert_eq!(drops.load(Ordering::SeqCst), usize::from(publish));
        drop((tx, rx));
        assert!(observer.finish().is_ok());
        assert_eq!(budget.used(), 0);
    }
}
struct Counted(usize);
struct Disposal {
    messages: Arc<AtomicUsize>,
    bytes: Arc<AtomicUsize>,
}
impl Value for Counted {
    type Disposition = Disposal;
    fn dispose_context(context: Disposal, fault: &Fault<'_>) -> Option<Disposal> {
        let Disposal { messages, bytes } = context;
        fault.dispose_context_head(messages);
        fault.dispose_context_tail(bytes);
        None
    }
    fn lane_bytes(&self) -> usize {
        self.0
    }
    fn valid_for(&self, _: &BackingMeter) -> bool {
        true
    }
    fn dispose(self, fault: &Fault<'_>, context: &Disposal) -> Option<Self> {
        let n = self.0;
        fault.dispose(self);
        if !fault.failed() {
            context.messages.fetch_add(1, Ordering::SeqCst);
            context.bytes.fetch_add(n, Ordering::SeqCst);
        }
        None
    }
}
#[test]
fn actual_single_disposition_context_counts_closed_tail_and_cold_record_layout() {
    use crate::shared::bounded_channel::{NativeCapacity, native_layout_with, native_with};
    let limits = NativeCapacity {
        messages: 2,
        waiters: 257,
    };
    let size = native_layout_with::<Counted>(limits).unwrap() + FailureOrder::layout_bytes();
    for limit in [size - 1, size] {
        let budget = bytes::BackingBudget::new(limit).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let (clock, _facts) = clock(&meter);
        let messages = Arc::new(AtomicUsize::new(0));
        let bytes = Arc::new(AtomicUsize::new(0));
        let mut context = Some(Disposal {
            messages: messages.clone(),
            bytes: bytes.clone(),
        });
        let prepared = native_with(limits, &meter, clock, &mut context);
        if limit < size {
            assert!(prepared.is_err());
            assert!(context.is_some());
        } else {
            let (tx, mut rx, observer) = prepared.unwrap();
            assert!(context.is_none());
            assert_eq!(budget.used(), size);
            let mut first_future = Box::pin(tx.reserve());
            use std::future::Future;
            let Poll::Ready(Ok(first)) = first_future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            else {
                panic!("first credit");
            };
            first.send(Counted(3));
            drop(first_future);
            let mut late_future = Box::pin(tx.reserve());
            let Poll::Ready(Ok(late)) = late_future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
            else {
                panic!("late credit");
            };
            rx.close();
            assert_eq!(
                messages.load(Ordering::SeqCst),
                0,
                "close does not auto-discard the live receiver"
            );
            drop(rx);
            late.send(Counted(4));
            drop(late_future);
            drop(tx);
            assert_eq!(
                (
                    messages.load(Ordering::SeqCst),
                    bytes.load(Ordering::SeqCst)
                ),
                (2, 7)
            );
            assert!(observer.finish().is_ok());
        }
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

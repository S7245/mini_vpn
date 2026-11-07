use super::*;
struct Count(AtomicUsize);
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn try_receive_preserves_registered_waiter_and_closure_wakes_original_producer() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = channel::<u64>(1, &meter).unwrap();
    let consumer = Arc::new(Count(AtomicUsize::new(0)));
    let consumer_waker = Waker::from(consumer.clone());
    assert!(receiver
        .poll_recv(&mut Context::from_waker(&consumer_waker))
        .is_pending());
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    sender.try_reserve().unwrap().send(7).unwrap();
    assert_eq!(consumer.0.load(Ordering::SeqCst), 1);
    assert_eq!(sender.ordinary_len(), 1);
    let producer = Arc::new(Count(AtomicUsize::new(0)));
    let producer_waker = Waker::from(producer.clone());
    assert!(sender
        .poll_closed(&mut Context::from_waker(&producer_waker))
        .is_pending());
    assert_eq!(receiver.try_recv(), Ok(7));
    assert_eq!(producer.0.load(Ordering::SeqCst), 1);
    assert!(sender
        .poll_closed(&mut Context::from_waker(&producer_waker))
        .is_pending());
    receiver.close();
    assert_eq!(producer.0.load(Ordering::SeqCst), 2);
    assert!(sender
        .poll_closed(&mut Context::from_waker(&producer_waker))
        .is_ready());
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    drop((sender, receiver));
    assert_eq!(budget.used(), 0);
}
#[test]
fn quarantined_try_receive_keeps_physical_prefix_for_cold_disposal() {
    let budget = BackingBudget::new(4096).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (sender, mut receiver) = channel::<u64>(2, &meter).unwrap();
    sender.try_reserve().unwrap().send(11).unwrap();
    meter.quarantine();
    assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    assert_eq!(receiver.len(), 1);
    assert_eq!(sender.ordinary_len(), 1);
    let mut disposed = None;
    receiver.close_and_drain(|value| disposed = Some(value));
    assert_eq!(disposed, Some(11));
    assert_eq!(receiver.len(), 0);
    drop((sender, receiver));
    assert_eq!(budget.used(), 1);
}

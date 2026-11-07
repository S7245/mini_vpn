//! Real FIFO credit/descriptor/output admission and granted close-tail behavior.
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn native_read_operation_prepares_every_resource_and_honors_original_close_tail() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) =
        AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(4, &meter, clock, 8, 4)
            .unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut ready = None;
    producer.prepare_read(4, &mut ready).await.unwrap();
    let mut read = ready.take().unwrap();
    assert_eq!(read.capacity(), 4);
    read.space().as_mut().unwrap().write(b"tail").unwrap();
    let mut pending = Some(OwnedPayload::Native(
        read.space().take().unwrap().finish().unwrap(),
    ));
    queue.close().await;
    assert!(
        queue.dispose_custodied().unwrap().is_none(),
        "a real granted read still owns its tail"
    );
    let mut held = None;
    let mut receiving = Box::pin(queue.recv_custodied(4, &mut held));
    assert!(
        receiving
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    read.publish(&mut pending).await.unwrap();
    assert!(pending.is_none());
    assert!(receiving.await.unwrap());
    assert_eq!(held.as_ref().unwrap().bytes.as_ref(), b"tail");
    drop((read, held, producer));
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(facts.0.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn read_admission_typed_pressure_and_zero_poll_cancel_release_space_before_regrant() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) =
        AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(16, &meter, clock, 8, 4)
            .unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut first = None;
    producer.prepare_read(4, &mut first).await.unwrap();
    let charged = budget.used();
    let mut second = None;
    assert!(matches!(
        producer.prepare_read(4, &mut second).await,
        Err(crate::tcp_downlink_pump::output_stock::StockWaitError::Pressure)
    ));
    assert!(second.is_none());
    assert!(queue.inner.read.is_active());
    assert!(!budget.quarantined());
    drop(async move {
        drop(first);
    });
    assert!(
        stock
            .root
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .operation
            .is_none()
    );
    assert!(!queue.inner.read.is_active());
    producer.prepare_read(4, &mut second).await.unwrap();
    assert_eq!(second.as_ref().unwrap().capacity(), 4);
    drop(second);
    assert!(!queue.inner.read.is_active());
    assert_eq!(budget.used(), charged);
    drop(producer);
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(facts.0.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn full_fifo_descriptor_waits_before_output_or_read_and_actual_pop_recovers_without_refund() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let data = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) =
        AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(128, &meter, clock, 128, 64)
            .unwrap();
    let mut producer = stock.take_producer().unwrap();
    for byte in 0..4 {
        let mut ready = None;
        producer.prepare_read(1, &mut ready).await.unwrap();
        let mut read = ready.unwrap();
        read.space().as_mut().unwrap().write(&[byte]).unwrap();
        let mut pending = Some(OwnedPayload::Native(
            read.space().take().unwrap().finish().unwrap(),
        ));
        read.publish(&mut pending).await.unwrap();
    }
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let unrelated =
        bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &data)
            .unwrap();
    {
        let state = queue.inner.state.lock().await;
        assert_eq!(
            (state.queue.chunks.len(), state.queue.chunks.capacity()),
            (4, 4)
        );
    }
    let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut ready = None;
    let mut waiting = Box::pin(producer.prepare_read(1, &mut ready));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    assert!(!queue.inner.read.is_active());
    assert!(
        stock
            .root
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .operation
            .is_none()
    );
    let mut held = None;
    assert!(queue.recv_custodied(1, &mut held).await.unwrap());
    assert_eq!(budget.used(), budget.limit());
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(matches!(
        waiting.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Ok(()))
    ));
    drop(waiting);
    assert_eq!(ready.as_ref().unwrap().capacity(), 1);
    drop(ready);
    assert_eq!(budget.used(), budget.limit());
    drop((held, unrelated, producer));
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(facts.0.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn occupied_ready_is_typed_pressure_and_new_close_is_distinct_and_preserves_original() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) =
        AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(16, &meter, clock, 8, 4)
            .unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut ready = None;
    producer.prepare_read(4, &mut ready).await.unwrap();
    assert!(matches!(
        producer.prepare_read(4, &mut ready).await,
        Err(crate::tcp_downlink_pump::output_stock::StockWaitError::Pressure)
    ));
    assert_eq!(ready.as_ref().unwrap().capacity(), 4);
    assert!(!budget.quarantined());
    drop(ready.take());
    queue.close().await;
    assert!(matches!(
        producer.prepare_read(4, &mut ready).await,
        Err(crate::tcp_downlink_pump::output_stock::StockWaitError::Closed)
    ));
    assert!(ready.is_none());
    drop(producer);
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(facts.0.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn active_read_pressure_never_hides_foreign_producer_and_payload_authority() {
    let budget_a = BackingBudget::new(65536).unwrap();
    let meter_a = BackingMeter::with_budget(&budget_a);
    let (clock_a, facts_a) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter_a);
    let (queue_a, mut stock_a) =
        AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(16, &meter_a, clock_a, 8, 4)
            .unwrap();
    let mut producer_a = stock_a.take_producer().unwrap();
    let mut ready_a = None;
    producer_a.prepare_read(4, &mut ready_a).await.unwrap();
    let budget_b = BackingBudget::new(65536).unwrap();
    let meter_b = BackingMeter::with_budget(&budget_b);
    let (clock_b, facts_b) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter_b);
    let (queue_b, mut stock_b) =
        AsyncByteBoundedFlowQueue::new_with_output_stock(16, &meter_b, clock_b, 8, 4).unwrap();
    let mut producer_b = stock_b.take_producer().unwrap();
    let mut space = producer_b.try_prepare(4).unwrap().unwrap();
    space.write(b"same").unwrap();
    let bytes = space.finish().unwrap();
    let original = bytes.allocation_identity();
    let mut pending = Some(OwnedPayload::Native(bytes));
    let result = queue_a.push_stock(&producer_b, &mut pending).await;
    let retained = match pending.as_ref().unwrap() {
        OwnedPayload::Native(bytes) => bytes.allocation_identity(),
        _ => None,
    };
    let quiet = (
        facts_a.1.load(Ordering::SeqCst),
        facts_b.1.load(Ordering::SeqCst),
    );
    let q = (budget_a.quarantined(), budget_b.quarantined());
    // No failing assertion may manufacture an unrelated owner-loss Q verdict.
    drop((
        ready_a, pending, producer_a, producer_b, stock_a, stock_b, queue_a, queue_b,
    ));
    assert_eq!(retained, original);
    assert_eq!(
        result,
        Err(crate::tcp_downlink_pump::ByteCustodyError::Invalid),
        "authority rejection must precede legitimate pressure"
    );
    assert_eq!(q, (true, true));
    assert!(quiet.0 > 0 && quiet.1 > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn next_read_attempt_cannot_occupy_the_role_needed_to_publish_the_granted_tail() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) =
        AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(16, &meter, clock, 8, 4)
            .unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut first = None;
    producer.prepare_read(4, &mut first).await.unwrap();
    let mut first = first.unwrap();
    first.space().as_mut().unwrap().write(b"tail").unwrap();
    let mut pending = Some(OwnedPayload::Native(
        first.space().take().unwrap().finish().unwrap(),
    ));
    let mut next = None;
    let mut waiting = Box::pin(producer.prepare_read(4, &mut next));
    let pending_wait = match waiting
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Pending => true,
        Poll::Ready(Err(crate::tcp_downlink_pump::output_stock::StockWaitError::Pressure)) => false,
        other => panic!("only legitimate wait or typed pressure is allowed: {other:?}"),
    };
    let result = first.publish(&mut pending).await;
    let q = budget.quarantined();
    let published = pending.is_none();
    drop(waiting);
    drop((first, next, pending, producer));
    // Retain the result before any deliberate assertion/unwind can mark Q.
    if !q {
        let mut held = None;
        assert!(queue.recv_custodied(4, &mut held).await.unwrap());
        assert_eq!(held.unwrap().bytes.as_ref(), b"tail");
        queue.close().await;
        assert!(queue.dispose_custodied().unwrap().is_some());
        assert!(stock.try_retire());
    }
    drop((queue, stock));
    assert_eq!(
        result,
        Ok(()),
        "a successor must not hold Producer while awaiting its predecessor's publication"
    );
    assert!(!q);
    assert!(published);
    assert!(facts.0.lock().unwrap().is_empty());
    let _ = pending_wait;
}

#[tokio::test(flavor = "current_thread")]
async fn foreign_ready_is_not_pressure_and_keeps_original_ticket_and_both_quiet() {
    let budget_a = BackingBudget::new(65536).unwrap();
    let meter_a = BackingMeter::with_budget(&budget_a);
    let (clock_a, facts_a) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter_a);
    let (queue_a, mut stock_a) =
        AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(16, &meter_a, clock_a, 8, 4)
            .unwrap();
    let mut producer_a = stock_a.take_producer().unwrap();
    let budget_b = BackingBudget::new(65536).unwrap();
    let meter_b = BackingMeter::with_budget(&budget_b);
    let (clock_b, facts_b) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter_b);
    let (queue_b, mut stock_b) =
        AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(16, &meter_b, clock_b, 8, 4)
            .unwrap();
    let mut producer_b = stock_b.take_producer().unwrap();
    let mut ready = None;
    producer_b.prepare_read(4, &mut ready).await.unwrap();
    assert!(matches!(
        producer_a.prepare_read(4, &mut ready).await,
        Err(
            crate::tcp_downlink_pump::output_stock::StockWaitError::Admission(
                bytes::MeteredCopyError::Quarantined
            )
        )
    ));
    assert_eq!(ready.as_ref().unwrap().capacity(), 4);
    assert!(
        stock_b
            .root
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .operation
            .is_some()
    );
    assert!(!queue_a.inner.read.is_active());
    assert!(queue_b.inner.read.is_active());
    assert!(budget_a.quarantined() && budget_b.quarantined());
    assert!(facts_a.1.load(Ordering::SeqCst) > 0 && facts_b.1.load(Ordering::SeqCst) > 0);
    drop((
        ready, producer_a, producer_b, stock_a, stock_b, queue_a, queue_b,
    ));
    assert!(budget_a.used() > 0 && budget_b.used() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn already_q_receiver_still_marks_actual_foreign_pending_before_any_quiet() {
    let budget = BackingBudget::new(128 << 10).unwrap();
    let meter_a = BackingMeter::with_budget(&budget);
    let meter_b = BackingMeter::with_budget(&budget);
    let (clock_a, facts_a) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter_a);
    let (clock_b, facts_b) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter_b);
    let (queue_a, mut stock_a) =
        AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(16, &meter_a, clock_a, 8, 4)
            .unwrap();
    let (queue_b, mut stock_b) =
        AsyncByteBoundedFlowQueue::new_with_output_stock(16, &meter_b, clock_b, 8, 4).unwrap();
    let mut producer_a = stock_a.take_producer().unwrap();
    let mut producer_b = stock_b.take_producer().unwrap();
    let mut ready = None;
    producer_a.prepare_read(4, &mut ready).await.unwrap();
    let mut read = ready.unwrap();
    read.space().as_mut().unwrap().write(b"owna").unwrap();
    let own = read.space().take().unwrap().finish().unwrap();
    let mut foreign = producer_b.try_prepare(4).unwrap().unwrap();
    foreign.write(b"badb").unwrap();
    let foreign = foreign.finish().unwrap();
    let identity = foreign.allocation_identity();
    let mut pending = Some(OwnedPayload::Native(foreign));
    meter_a.quarantine(); // Independent test fault; no quiet was published yet.
    assert_eq!(facts_b.1.load(Ordering::SeqCst), 0);
    assert_eq!(
        read.publish(&mut pending).await,
        Err(crate::tcp_downlink_pump::ByteCustodyError::Invalid)
    );
    assert_eq!(pending.as_ref().unwrap().as_ref(), b"badb");
    assert_eq!(
        match pending.as_ref().unwrap() {
            OwnedPayload::Native(bytes) => bytes.allocation_identity(),
            _ => None,
        },
        identity
    );
    assert_eq!(own.as_ref(), b"owna");
    assert!(
        facts_a.1.load(Ordering::SeqCst) > 0 && facts_b.1.load(Ordering::SeqCst) > 0,
        "shared budget Q never substitutes for each actual owner's quiet publication"
    );
    assert!(facts_a.0.lock().unwrap().is_empty() && facts_b.0.lock().unwrap().is_empty());
    drop((
        read, own, pending, producer_a, producer_b, stock_a, stock_b, queue_a, queue_b,
    ));
    assert!(budget.used() > 0);
}

struct RetireFault {
    armed: std::sync::atomic::AtomicBool,
    original: Arc<usize>,
}
unsafe fn fault_clone(data: *const ()) -> std::task::RawWaker {
    unsafe {
        Arc::increment_strong_count(data.cast::<RetireFault>());
    }
    std::task::RawWaker::new(data, &FAULT_VTABLE)
}
unsafe fn fault_wake(data: *const ()) {
    drop(unsafe { Arc::from_raw(data.cast::<RetireFault>()) });
}
unsafe fn fault_wake_ref(_: *const ()) {}
unsafe fn fault_drop(data: *const ()) {
    let fault = unsafe { Arc::from_raw(data.cast::<RetireFault>()) };
    let fire = fault.armed.swap(false, Ordering::SeqCst);
    let original = fault.original.clone();
    drop(fault);
    if fire {
        std::panic::panic_any(original);
    }
}
static FAULT_VTABLE: std::task::RawWakerVTable =
    std::task::RawWakerVTable::new(fault_clone, fault_wake, fault_wake_ref, fault_drop);

#[tokio::test(flavor = "current_thread")]
async fn q_after_prepared_output_keeps_original_ready_and_qualification_until_disposition() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) =
        AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(16, &meter, clock, 8, 4)
            .unwrap();
    let mut producer = stock.take_producer().unwrap();
    let original = Arc::new(723usize);
    let data = Arc::into_raw(Arc::new(RetireFault {
        armed: std::sync::atomic::AtomicBool::new(true),
        original: original.clone(),
    }));
    let waker = unsafe { Waker::from_raw(std::task::RawWaker::new(data.cast(), &FAULT_VTABLE)) };
    let mut ready = None;
    let mut preparing = Box::pin(producer.prepare_read(4, &mut ready));
    assert!(matches!(
        preparing.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Err(
            crate::tcp_downlink_pump::output_stock::StockWaitError::Admission(
                bytes::MeteredCopyError::Quarantined
            )
        ))
    ));
    drop(preparing);
    assert_eq!(
        ready.as_ref().unwrap().capacity(),
        4,
        "the completed output remains in its original caller-owned slot"
    );
    assert!(
        stock
            .root
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .operation
            .is_some()
    );
    assert!(queue.inner.read.is_active());
    assert!(budget.quarantined());
    assert!(
        queue.inner.waiters.get().unwrap().retains_arc(&original),
        "actual original panic payload retained by the fixed callback site"
    );
    assert_eq!(facts.0.lock().unwrap().len(), 1);
    drop(waker);
    drop(ready);
    assert!(
        queue.inner.read.is_active(),
        "Q cannot normalize the interrupted qualification to reusable zero"
    );
    assert!(matches!(
        queue.dispose_custodied(),
        Err(crate::tcp_downlink_pump::ByteCustodyError::Invalid)
    ));
    drop((producer, stock, queue));
    assert!(budget.used() > 0);
}

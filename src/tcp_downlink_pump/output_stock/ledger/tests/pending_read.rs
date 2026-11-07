//! Original Producer, read grant and actual backing retirement while input waits.
use super::*;
use crate::tcp_downlink_pump::{CustodiedBytes, StockReadOperation};
use super::super::{OutputStock, StockProducer, StockWaitError};

#[tokio::test(flavor = "current_thread")]
async fn unused_active_bootstrap_is_existing_rebind_stock_when_only_last_ready_bit_is_set() {
    let (budget, meter, queue, mut stock, mut producer, first, last, mut grant) = granted().await;
    let mut old = grant.take().unwrap();
    old.space().as_mut().unwrap().write(b"done").unwrap();
    let mut pending = Some(OwnedPayload::Native(old.space().take().unwrap().finish().unwrap()));
    old.publish(&mut pending).await.unwrap();
    drop(old);
    let mut held = None;
    assert!(queue.recv_custodied(4, &mut held).await.unwrap());
    drop((held, first, last));
    producer.prepare_read(8, &mut grant).await.unwrap();
    {
        let state = stock.root.as_ref().unwrap().lock().unwrap();
        assert_eq!((state.funded, state.active, state.ready), (2, Some(0), 2));
        assert_eq!(state.operation.unwrap().written, 0);
    }
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    let charged = budget.used();
    let mut waiting = Box::pin(producer.read_or_trim(&mut grant, |_, _| Poll::<()>::Pending));
    let observed = waiting.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    drop(waiting);
    assert!(matches!(observed, Poll::Ready(Ok(None))), "actual unused bootstrap can keep the original read admitted");
    assert_eq!(charged - budget.used(), bytes::BytesMut::metered_copy_layout_for_len(8).unwrap());
    assert_eq!(stock.root.as_ref().unwrap().lock().unwrap().funded, 1);
    grant.as_ref().unwrap().validate_unused().unwrap();
    drop((grant, pressure, producer));
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn completed_read_value_survives_final_callback_quarantine() {
    let (budget, meter, queue, stock, mut producer, first, last, mut grant) = granted().await;
    let mut input = Some(bytes::BytesMut::try_copy_from_slice_metered(b"wire", &meter).unwrap().freeze());
    let physical = meter.snapshot().physical_bytes;
    let result = producer.read_or_trim(&mut grant, |_, _| {
        meter.quarantine();
        Poll::Ready(input.take().unwrap())
    }).await;
    assert!(result.is_err());
    let Err((StockWaitError::Admission(bytes::MeteredCopyError::Quarantined), Some(original))) = result else {
        panic!("final Q must return the completed original output");
    };
    assert_eq!(original.as_ref(), b"wire");
    assert_eq!(meter.snapshot().physical_bytes, physical, "final Q must preserve the actual already-read output");
    drop((original, grant, first, last, producer, stock, queue));
    assert!(budget.quarantined() && budget.used() > 0);
}

async fn granted() -> (BackingBudget, BackingMeter, AsyncByteBoundedFlowQueue,
    OutputStock, StockProducer, CustodiedBytes, CustodiedBytes, Option<StockReadOperation>) {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, _) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
        16, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut first = producer.try_prepare(4).unwrap().unwrap();
    first.write(b"boot").unwrap();
    let first = first.finish().unwrap();
    let mut last = producer.try_prepare(4).unwrap().unwrap();
    last.write(b"tail").unwrap();
    let last = last.finish().unwrap();
    let mut grant = None;
    producer.prepare_read(4, &mut grant).await.unwrap();
    (budget, meter, queue, stock, producer, first, last, grant)
}

#[tokio::test(flavor = "current_thread")]
async fn pending_read_cannot_refund_alias_or_regrow_and_only_actual_return_enables_trim() {
    let (budget, meter, queue, mut stock, mut producer, first, last, mut grant) = granted().await;
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    let charged = budget.used();
    let wakes = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let polls = std::cell::Cell::new(0);
    let mut waiting = Box::pin(producer.read_or_trim(&mut grant, |_, max| {
        assert_eq!(max, 4); polls.set(polls.get() + 1); Poll::<()>::Pending
    }));
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    drop(last);
    assert_eq!(budget.used(), charged, "loan return is availability only");
    assert!(waiting.as_mut().poll(&mut cx).is_pending(), "bootstrap alias forbids retire/re-grow loop");
    assert_eq!(budget.used(), charged);
    let before_wake = wakes.0.load(Ordering::SeqCst);
    drop(first);
    assert!(wakes.0.load(Ordering::SeqCst) > before_wake);
    assert_eq!(budget.used(), charged);
    assert!(matches!(waiting.as_mut().poll(&mut cx), Poll::Ready(Ok(None))));
    drop(waiting);
    assert_eq!(charged - budget.used(), bytes::BytesMut::metered_copy_layout_for_len(8).unwrap());
    grant.as_ref().unwrap().validate_unused().unwrap();
    assert_eq!(grant.as_ref().unwrap().capacity(), 4);
    let after = budget.used();
    let mut waiting = Box::pin(producer.read_or_trim(&mut grant, |_, _| Poll::<()>::Pending));
    assert!(waiting.as_mut().poll(&mut cx).is_pending());
    assert_eq!(budget.used(), after, "no input progress means no new block funding");
    drop(waiting);
    drop((grant, pressure, producer));
    queue.close().await;
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn pending_read_keeps_original_on_cancel_and_does_not_trim_without_pressure_or_pending() {
    let (budget, _, queue, mut stock, mut producer, first, last, mut grant) = granted().await;
    drop((first, last));
    let charged = budget.used();
    drop(producer.read_or_trim(&mut grant, |_, _| Poll::<()>::Pending));
    let mut waiting = Box::pin(producer.read_or_trim(&mut grant, |_, _| Poll::<()>::Pending));
    assert!(waiting.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    drop(waiting);
    assert_eq!(budget.used(), charged, "ordinary network Pending must not churn stock");
    grant.as_ref().unwrap().validate_unused().unwrap();
    assert_eq!(producer.read_or_trim(&mut grant, |_, max| Poll::Ready(max)).await.unwrap(), Some(4));
    assert_eq!(budget.used(), charged);
    queue.close().await;
    assert!(matches!(producer.read_or_trim(&mut grant, |_, _| Poll::<()>::Pending).await, Err((StockWaitError::Closed, None))));
    grant.as_ref().unwrap().validate_unused().unwrap();
    drop((grant, producer));
    assert!(queue.dispose_custodied().unwrap().is_some());
    assert!(stock.try_retire());
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

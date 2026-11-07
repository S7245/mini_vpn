//! Single opaque Bytes destructor fault, keeping the real loan in MD custody.
use super::*;
use crate::tcp_downlink_pump::AsyncByteBoundedFlowQueue;
use std::{any::Any, panic::{AssertUnwindSafe, catch_unwind}, sync::{Arc, atomic::Ordering}};
struct Fault(Arc<usize>);
impl AsRef<[u8]> for Fault { fn as_ref(&self) -> &[u8] { b"fail" } }
impl Drop for Fault { fn drop(&mut self) { std::panic::panic_any(self.0.clone()); } }

#[test]
fn first_opaque_bytes_drop_publishes_original_quiet_even_though_loan_never_drops() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (_queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"cold").unwrap();
    let mut actual = ManuallyDrop::new(space.finish().unwrap());
    // A private adversarial representation, never a production data path: keep
    // the actual cold Bytes alive, while one injected opaque header unwinds.
    let backing = unsafe { ManuallyDrop::take(&mut actual.bytes) };
    let loan = unsafe { ManuallyDrop::take(&mut actual.loan) }.unwrap();
    let original = Arc::new(81usize);
    let fault = CustodiedBytes::from_stock(Bytes::from_owner(Fault(original.clone())), loan);
    let panic: Box<dyn Any + Send> = catch_unwind(AssertUnwindSafe(|| drop(fault))).unwrap_err();
    assert!(Arc::ptr_eq(panic.downcast_ref::<Arc<usize>>().unwrap(), &original));
    assert!(budget.quarantined());
    assert!(facts.1.load(Ordering::SeqCst) > 0, "MD loan cannot be the only quiet publication path");
    assert!(facts.0.lock().unwrap().is_empty(), "outer caller retains the original Box, no invented IO here");
    drop((producer, backing));
    assert!(!stock.try_retire());
    assert!(budget.used() > 0);
}

#[test]
fn zero_loan_counters_cannot_retire_a_real_untracked_backing_alias() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&meter);
    let (_queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &meter, clock, 8, 4).unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut space = producer.try_prepare(4).unwrap().unwrap();
    space.write(b"live").unwrap();
    let mut actual = ManuallyDrop::new(space.finish().unwrap());
    let backing = unsafe { ManuallyDrop::take(&mut actual.bytes) };
    let loan = unsafe { ManuallyDrop::take(&mut actual.loan) };
    drop(producer);
    drop(loan); // Adversarial private representation: physical Bytes still live.
    assert!(!stock.try_retire(), "actual Shared uniqueness, not zero counters, decides retirement");
    assert_eq!(backing.as_ref(), b"live");
    assert!(budget.quarantined());
    assert!(facts.1.load(Ordering::SeqCst) > 0);
    assert!(budget.used() > 0);
    drop(backing);
}

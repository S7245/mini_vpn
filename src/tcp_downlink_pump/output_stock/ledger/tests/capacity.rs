//! Actual cold allocations and refusal, not an F4096 runtime acceptance.
use super::super::{Core, Root, SlabRecord};
use super::*;
use crate::tcp_downlink_pump::{ByteChunk, native_wait};
use std::alloc::Layout;

#[test]
fn measured_eager_stock_cost_and_actual_refusal_bound_target_expansion() {
    const QUEUE: usize = 4 << 20;
    const SLAB: usize = 512 << 10;
    const WINDOW: usize = 64 << 10;
    // Original FLOW_LIMIT in flow_custody/transaction.rs; this is projection,
    // not 4096 synthetic actors or a new concurrency/budget contract.
    const TARGET_F: usize = 4096;
    let budget = BackingBudget::new(32 << 20).unwrap();
    let app_meter = BackingMeter::with_budget(&budget);
    let (clock, facts) = crate::tcp_downlink_pump::tests::native_waiters::clock(&app_meter);
    let initial = budget.used();
    let mut actual = Vec::new();
    let mut bills = Vec::new();
    loop {
        let meter = BackingMeter::with_budget(&budget);
        let before = budget.used();
        match AsyncByteBoundedFlowQueue::new_with_output_stock(
            QUEUE,
            &meter,
            clock.clone(),
            SLAB,
            WINDOW,
        ) {
            Ok((queue, stock)) => {
                let root = stock.root.as_ref().unwrap();
                let state = root.lock().unwrap();
                let n = state.slabs.len();
                let descriptors = queue
                    .inner
                    .state
                    .try_lock()
                    .unwrap()
                    .queue
                    .chunks
                    .capacity();
                assert_eq!(n, 13);
                assert_eq!(meter.snapshot().allocations, n);
                let physical = meter.snapshot().physical_bytes;
                let layouts = Layout::array::<SlabRecord>(n).unwrap().size()
                    + Layout::new::<Core>().size()
                    + native_wait::Waiters::layout_bytes()
                    + Layout::array::<ByteChunk>(descriptors).unwrap().size();
                let bill = budget.used() - before;
                assert_eq!(
                    bill,
                    physical + layouts,
                    "independent budget delta includes each actual charged owner"
                );
                assert_eq!(meter.snapshot().payload_bytes, n * SLAB);
                eprintln!(
                    "[N1-stock-capacity] instance={} backing_count={n} payload={} actual_shared_layouts={physical} slab_records={} core={} callbacks={} fifo_records={} bill={bill} fixed_root_value={} fixed_byte_queue_value={} opaque_arc_headers_separately_instance_bounded=true",
                    actual.len(),
                    n * SLAB,
                    Layout::array::<SlabRecord>(n).unwrap().size(),
                    Layout::new::<Core>().size(),
                    native_wait::Waiters::layout_bytes(),
                    Layout::array::<ByteChunk>(descriptors).unwrap().size(),
                    std::mem::size_of::<Root>(),
                    std::mem::size_of::<crate::tcp_downlink_pump::AsyncByteBoundedFlowQueueInner>()
                );
                drop(state);
                bills.push(bill);
                actual.push((queue, stock, meter));
            }
            Err(error) => {
                assert_eq!(error, bytes::MeteredCopyError::BudgetFull);
                assert_eq!(
                    budget.used(),
                    before,
                    "failed real partial construction rolls back all unpublished allocations"
                );
                assert!(!budget.quarantined());
                eprintln!(
                    "[N1-stock-capacity] actual_cold_accepted={} next_actual_cold_refusal={error:?} used={} limit={} original_clock_bill={initial} target_f={TARGET_F} target_output_payload_lower={} target_charged_output_layouts={} main_pending_payload_separate={} service_node_device_input_reassembly_task_private_headers_not_in_this_total=true",
                    actual.len(),
                    budget.used(),
                    budget.limit(),
                    13 * SLAB * TARGET_F,
                    bills[0] * TARGET_F,
                    WINDOW * TARGET_F
                );
                break;
            }
        }
    }
    assert_eq!(
        actual.len(),
        4,
        "even with no Service/Node/Device bill, current eager stock admits only four under this explicit input"
    );
    assert!(bills.iter().all(|bill| *bill == bills[0]));
    for (queue, mut stock, meter) in actual {
        assert!(queue.dispose_custodied().unwrap().is_some());
        assert!(stock.try_retire());
        assert_eq!(meter.snapshot().allocations, 0);
        drop(queue);
    }
    assert_eq!(budget.used(), initial);
    assert!(facts.0.lock().unwrap().is_empty());
    drop(clock);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

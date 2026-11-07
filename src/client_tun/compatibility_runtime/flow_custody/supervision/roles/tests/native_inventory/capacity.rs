//! Actual approved cold inventories. This projects the original F4096 bound;
//! it does not pretend to run 4096 actors or include Quinn/smoltcp in the bill.
use super::*;
use crate::client_tun::downlink_custody::{self, PendingBytes, PermitStock, EgressPermitLedger};
use crate::shared::errors::*;
use crate::tcp_downlink_pump::{AsyncByteBoundedFlowQueue, DownstreamBytePermit};

#[derive(Default)]
struct Facts(std::sync::Mutex<Vec<RetainedFailure>>);
impl QuietFailureSink for Facts {
    fn record_quiet(&self, failure: RetainedFailure) { self.0.lock().unwrap().push(failure); }
}

#[test]
fn progressive_source_and_main_real_cold_cost_preserves_full_rollback_and_target_geometry() {
    const C: usize = 4 << 20;
    const S: usize = 512 << 10;
    const W: usize = 64 << 10;
    const F: usize = 4096;
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let facts = Arc::new(Facts::default());
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let mut actual = Vec::new();
    let mut bills = Vec::new();
    loop {
        let before = budget.used();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let attempt = (|| {
            let clock = Arc::new(FailureOrder::default());
            clock.bind(Arc::downgrade(&sink), FailureOrigin {
                phase: FailurePhase::TcpOpen, resource: FailureResource::TcpOpen,
                resource_id: u64::try_from(actual.len()).unwrap(), attempt_id: Some(1),
            }, Some(meter.clone()))?;
            let source = Cold::prepare(&meter, clock.clone())?;
            let (mut pending, mut caps, mut ledger) =
                (PendingBytes::default(), PermitStock::default(), EgressPermitLedger::default());
            let main_before = budget.used();
            downlink_custody::storage::prepare(&mut pending, &mut caps, &mut ledger, &meter)?;
            let main_bill = budget.used() - main_before;
            assert_eq!(main_bill, W + 2 * std::mem::size_of::<DownstreamBytePermit>()
                + downlink_custody::storage::capture_layout_bytes());
            let stock_before = budget.used();
            let (queue, stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
                C, &meter, clock.clone(), S, W)?;
            let stock_bill = budget.used() - stock_before;
            let bill = budget.used() - before;
            assert_eq!(bill, main_bill + stock_bill + FailureOrder::layout_bytes()
                + SourceCallbacks::layout_bytes());
            assert_eq!((meter.snapshot().allocations, meter.snapshot().payload_bytes), (1, W));
            Ok::<_, bytes::MeteredCopyError>((queue, stock, source, clock, pending, caps, ledger,
                (bill, main_bill, stock_bill)))
        })();
        match attempt {
            Ok(bundle) => { bills.push(bundle.7); actual.push(bundle); }
            Err(error) => {
                assert_eq!(error, bytes::MeteredCopyError::BudgetFull);
                assert_eq!(budget.used(), before, "unpublished partial cold inventory really dropped");
                assert!(!budget.quarantined());
                break;
            }
        }
    }
    assert!(bills.iter().all(|bill| *bill == bills[0]));
    let (bill, main, stock) = bills[0];
    assert_eq!(actual.len(), budget.limit() / bill);
    assert!(bill * F > budget.limit(), "32MiB never claims F4096 feasibility");
    eprintln!("[N1-progressive-capacity] real_accepted={} next=BudgetFull per_flow={bill} main={main} stock={stock} source_callbacks={} flow_clock={} f={F} cold_f={} open_w=128 old_retained_plus_candidate_requires_additional_actual_node_inventory=true per_flow_max_output_payload={} service_node_device_smoltcp_input_assembler_task_headers_separate=true",
        actual.len(), SourceCallbacks::layout_bytes(), FailureOrder::layout_bytes(), bill * F,
        W + 13 * S);
    for (queue, mut stock, source, clock, pending, caps, ledger, _) in actual {
        assert!(stock.try_retire());
        assert!(queue.dispose_custodied().unwrap().is_some());
        drop((queue, stock, source, clock, pending, caps, ledger));
    }
    assert!(facts.0.lock().unwrap().is_empty());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn progressive_original_flow_max_funded_stock_and_last_loan_cost_are_actual() {
    use crate::tcp_downlink_pump::OwnedPayload;
    const S: usize = 512 << 10;
    const W: usize = 64 << 10;
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let facts = Arc::new(Facts::default());
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let clock = Arc::new(FailureOrder::default());
    clock.bind(Arc::downgrade(&sink), FailureOrigin {
        phase: FailurePhase::TcpOpen, resource: FailureResource::TcpOpen,
        resource_id: 0, attempt_id: Some(1),
    }, Some(meter.clone())).unwrap();
    let source = Cold::prepare(&meter, clock.clone()).unwrap();
    let (mut pending, mut caps, mut ledger) =
        (PendingBytes::default(), PermitStock::default(), EgressPermitLedger::default());
    downlink_custody::storage::prepare(&mut pending, &mut caps, &mut ledger, &meter).unwrap();
    let (queue, mut stock) = AsyncByteBoundedFlowQueue::new_with_progressive_output_stock(
        4 << 20, &meter, clock.clone(), S, W).unwrap();
    let cold = budget.used();
    let mut producer = stock.take_producer().unwrap();
    let mut actual = Vec::new();
    let zeros = [0u8; 8192];
    for index in 0..14 {
        let before = budget.used();
        let mut operation = None;
        producer.prepare_read(S, &mut operation).await.unwrap();
        let mut operation = operation.unwrap();
        assert_eq!(operation.capacity(), if index == 0 { W } else { S });
        if index != 0 {
            assert_eq!(budget.used() - before, bytes::BytesMut::metered_copy_layout_for_len(S).unwrap());
        }
        for _ in 0..operation.capacity() / zeros.len() {
            operation.space().as_mut().unwrap().write(&zeros).unwrap();
        }
        let mut output = Some(OwnedPayload::Native(operation.space().take().unwrap().finish().unwrap()));
        operation.publish(&mut output).await.unwrap();
        drop(operation);
        let mut held = None;
        assert!(queue.recv_custodied(S, &mut held).await.unwrap());
        actual.push(held.unwrap());
    }
    let identities: std::collections::HashSet<_> = actual.iter()
        .map(|batch| { let id = batch.bytes.allocation_identity().unwrap(); (id.base, id.capacity) }).collect();
    assert_eq!(identities.len(), 14, "every funded allocation is physically distinct");
    assert_eq!((meter.snapshot().allocations, meter.snapshot().payload_bytes), (14, W + 13 * S));
    let funded = budget.used();
    assert_eq!(funded, cold + 13 * bytes::BytesMut::metered_copy_layout_for_len(S).unwrap());
    eprintln!("[N1-progressive-funded] cold={cold} actual_funded={funded} f4096_fully_funded={} real_backings=14 payload={} fifo_growth_not_included_because_each_real_batch_consumed=true", funded * 4096, W + 13 * S);
    drop(producer);
    queue.close().await;
    let backing = meter.snapshot().physical_bytes;
    let data = queue.dispose_data().unwrap().unwrap().custody();
    let after_data = budget.used();
    assert_eq!(funded - after_data, data.metadata_bytes, "DATA disposition actually retires FIFO descriptor storage");
    assert_eq!((data.metadata_slots, data.consumed, data.emitted), (4, W + 13 * S, W + 13 * S));
    assert_eq!(meter.snapshot().physical_bytes, backing);
    assert!(!stock.try_retire());
    drop(actual);
    assert_eq!(budget.used(), after_data, "last physical loan still does not retire the owning inventory");
    assert_eq!(meter.snapshot().physical_bytes, backing);
    stock.wait_returned().await.unwrap();
    assert!(stock.try_retire());
    assert!(queue.dispose_custodied().unwrap().is_some());
    drop((queue, stock, source, clock, pending, caps, ledger));
    assert!(facts.0.lock().unwrap().is_empty());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn actual_f4096_cold_inventory_refuses_next_and_recovers_only_after_physical_drop() {
    use crate::client_tun::compatibility_runtime::flow_custody::{relay_writer, supervision::continuous::Shared};
    use crate::client_tun::uplink::RelayCommand;
    use crate::shared::bounded_channel::{native_with, NativeCapacity};
    const F: usize = 4096;
    // Frozen actual real-Service measurement, including stock retirement work,
    // both callback owners, Main, Flow clock and the original 1024-slot lane.
    // The original Root now retains its clock through callback seal: the
    // exact Waiters allocation grew from 864 to 880 bytes. Service quota stays fixed.
    const BILL: usize = 185_544 + (880 - 864);
    let budget = bytes::BackingBudget::new(F * BILL).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let facts = Arc::new(Facts::default());
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let prepare = || {
        let clock = Arc::new(FailureOrder::default());
        clock.bind(Arc::downgrade(&sink), FailureOrigin {
            phase: FailurePhase::TcpOpen, resource: FailureResource::TcpOpen,
            resource_id: 0, attempt_id: Some(1),
        }, Some(meter.clone()))?;
        let source = Cold::prepare(&meter, clock.clone())?;
        let writer = relay_writer::Cold::prepare(&meter, clock.clone())?;
        let (mut pending, mut caps, mut ledger) =
            (PendingBytes::default(), PermitStock::default(), EgressPermitLedger::default());
        downlink_custody::storage::prepare(&mut pending, &mut caps, &mut ledger, &meter)?;
        let (shared, output) = Shared::prepare_native(TcpRelayEngine::ContinuousPump,
            DownlinkBackpressureConfig::default(), Some(meter.clone()), clock.clone())?;
        let inventory = stock::Inventory::prepare(output.unwrap(), &meter, clock.clone())?;
        let (tx, rx, observer) = native_with::<RelayCommand>(
            NativeCapacity { messages: 1024, waiters: 1 }, &meter, clock.clone(), &mut Some(()))?;
        Ok::<_, bytes::MeteredCopyError>((tx, rx, observer, inventory, shared,
            source, writer, pending, caps, ledger, clock))
    };
    let mut bundles = Vec::with_capacity(F);
    let mut main_allocations = std::collections::HashSet::new();
    for index in 0..F {
        let before = budget.used();
        let bundle = prepare().unwrap();
        assert_eq!(budget.used() - before, BILL, "same production inventory at index {index}");
        assert_eq!(bundle.0.capacity(), 1024);
        assert!(main_allocations.insert(bundle.7.as_ptr() as usize));
        bundles.push(bundle);
    }
    assert_eq!(budget.used(), budget.limit());
    assert_eq!(meter.snapshot().allocations, F);
    assert_eq!(meter.snapshot().payload_bytes, F * (64 << 10));
    assert!(matches!(prepare(), Err(bytes::MeteredCopyError::BudgetFull)));
    assert_eq!(budget.used(), F * BILL);
    assert!(!budget.quarantined());
    let dispose = |(tx, rx, observer, inventory, shared, source, writer, pending, caps, ledger, clock): (_, _, crate::client_tun::uplink::Observer, _, _, _, _, _, _, _, _)| {
        drop((tx, rx));
        assert!(observer.finish().is_ok());
        drop((inventory, shared, source, writer, pending, caps, ledger, clock));
    };
    // Removing a test owner is insufficient; actual inventory and callbacks are
    // disposed before a replacement can obtain precisely this same cold bill.
    dispose(bundles.pop().unwrap());
    assert_eq!(budget.used(), (F - 1) * BILL);
    bundles.push(prepare().unwrap());
    assert_eq!(budget.used(), F * BILL);
    eprintln!("[N1-F4096-actual-cold] instances={F} per_instance={BILL} actual_charged={} distinct_main_allocations={} output_backings={} uplink_slots={} next=BudgetFull replacement_after_real_drop=true service_node_smoltcp_platform_tasks_and_dynamic_data_separate=true",
        budget.used(), main_allocations.len(), meter.snapshot().allocations, F * 1024);
    for bundle in bundles { dispose(bundle); }
    assert_eq!(budget.used(), 0);
    assert_eq!(meter.snapshot().allocations, 0);
    assert!(!budget.quarantined());
    assert!(facts.0.lock().unwrap().is_empty());
}

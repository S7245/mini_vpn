//! Real selected-event admission method; borrowed payload identity is the fault.
//! This unit boundary does not claim Service/Device or network acceptance.
use super::*;
use crate::tcp_downlink_pump::OwnedPayload;

#[test]
fn typed_main_rejects_raw_and_foreign_backing_before_event_publication() {
    for mode in 0..3 {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let node = bytes::BackingMeter::with_budget(&budget);
        let foreign = bytes::BackingMeter::with_budget(&budget);
        let app = bytes::BackingMeter::with_budget(&budget);
        let app_charge = app.reserve_cold_layout(17).unwrap().unwrap();
        let mut mirror = flow_mirror();
        mirror.native_backing = Some(node.clone());
        let payload = match mode {
            0 => OwnedPayload::Legacy(vec![7; 3]),
            1 => OwnedPayload::Native((bytes::Bytes::copy_from_slice_metered(&[7; 3], &foreign).unwrap()).into()),
            _ => OwnedPayload::Native((bytes::Bytes::from_static(&[7; 3])).into()),
        };
        let result = mirror.begin_data_event(7, &payload, &None);
        let actual = (
            result.is_err(),
            mirror.data_messages,
            mirror.data_bytes,
            mirror.event_in_flight,
            mirror.quarantined,
            node.is_quarantined(),
            app.is_quarantined(),
        );
        assert_eq!(payload.as_ref(), &[7; 3]);
        drop((payload, mirror, app_charge));
        assert_eq!(actual, (true, 0, 0, false, true, true, true));
        assert_eq!(budget.used(), 1);
    }
}

#[test]
fn typed_main_accepts_original_node_payload_with_distinct_app_meter() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let node = bytes::BackingMeter::with_budget(&budget);
    let app = bytes::BackingMeter::with_budget(&budget);
    assert!(!node.same_authority(&app));
    let app_charge = app.reserve_cold_layout(17).unwrap().unwrap();
    let mut mirror = flow_mirror();
    mirror.native_backing = Some(node.clone());
    let payload =
        OwnedPayload::Native((bytes::Bytes::copy_from_slice_metered(&[7; 3], &node).unwrap()).into());
    assert!(mirror.begin_data_event(7, &payload, &None).unwrap());
    assert_eq!((mirror.data_messages, mirror.data_bytes), (1, 3));
    mirror.discard_data(3);
    mirror.finish_event();
    assert!(!mirror.quarantined);
    drop((payload, mirror, app_charge));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn typed_main_foreign_stock_preserves_selected_and_quiets_both_original_owners() {
    foreign_stock_case(true, false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn typed_main_no_stock_rejects_same_meter_foreign_loan() {
    foreign_stock_case(false, true).await;
}

#[tokio::test(flavor = "current_thread")]
async fn typed_main_no_stock_quiets_foreign_budget_loan() {
    foreign_stock_case(false, false).await;
}

async fn foreign_stock_case(receiver_stock: bool, same_meter: bool) {
    use crate::shared::errors::{FailureOrder, FailureOrigin, FailurePhase, FailureResource, QuietFailureSink, RetainedFailure};
    use crate::tcp_downlink_pump::{AsyncByteBoundedFlowQueue, OutputSpace};
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    struct Facts(AtomicUsize);
    impl QuietFailureSink for Facts {
        fn record_quiet(&self, _: RetainedFailure) { panic!("no replacement IO"); }
        fn quarantine_quiet(&self) { self.0.fetch_add(1, Ordering::SeqCst); }
    }
    fn original(meter: &bytes::BackingMeter) -> (Arc<FailureOrder>, Arc<Facts>) {
        let facts = Arc::new(Facts(AtomicUsize::new(0)));
        let sink: Arc<dyn QuietFailureSink> = facts.clone();
        let clock = Arc::new(FailureOrder::default());
        clock.bind(Arc::downgrade(&sink), FailureOrigin {
            phase: FailurePhase::TcpOpen, resource: FailureResource::TcpOpen,
            resource_id: 0, attempt_id: Some(1),
        }, Some(meter.clone())).unwrap();
        (clock, facts)
    }
    let a = bytes::BackingBudget::new(65536).unwrap();
    let b = if same_meter { a.clone() } else { bytes::BackingBudget::new(65536).unwrap() };
    let ma = bytes::BackingMeter::with_budget(&a);
    let mb = if same_meter { ma.clone() } else { bytes::BackingMeter::with_budget(&b) };
    let (ca, fa) = original(&ma);
    let (cb, fb) = original(&mb);
    let (_qa, sa) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &ma, ca, 8, 4).unwrap();
    let (_qb, mut sb) = AsyncByteBoundedFlowQueue::new_with_output_stock(4, &mb, cb, 8, 4).unwrap();
    let mut producer = sb.take_producer().unwrap();
    let mut operation = None;
    producer.prepare_read(4, &mut operation).await.unwrap();
    let operation = operation.as_mut().unwrap();
    operation.space().as_mut().unwrap().write(b"tail").unwrap();
    let selected = Some(OwnedPayload::Native(OutputSpace::finish_retained(operation.space()).unwrap()));
    let pointer = selected.as_ref().unwrap().as_ref().as_ptr();
    let mut mirror = flow_mirror();
    mirror.native_backing = Some(ma);
    mirror.native_stock = if receiver_stock { sa.key() } else { None };
    assert!(mirror.begin_data_event(7, selected.as_ref().unwrap(), &None).is_err());
    assert_eq!(selected.as_ref().unwrap().as_ref().as_ptr(), pointer);
    assert_eq!(selected.as_ref().unwrap().as_ref(), b"tail");
    assert!(a.quarantined() && b.quarantined());
    assert!(fb.0.load(Ordering::SeqCst) > 0);
    if receiver_stock { assert!(fa.0.load(Ordering::SeqCst) > 0); }
    assert_eq!((mirror.data_messages, mirror.data_bytes, mirror.event_in_flight), (0, 0, false));
    assert!(a.used() > 0 && b.used() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn typed_main_same_meter_foreign_stock_preserves_both_original_owners() {
    foreign_stock_case(true, true).await;
}

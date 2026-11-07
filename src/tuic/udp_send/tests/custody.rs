use super::super::*;
use crate::tuic::node::StartupResources;
use std::sync::Arc;
fn owner() -> (bytes::BackingBudget, Arc<StartupResources>) {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let owner = StartupResources::udp_owner_for_test(BackingMeter::with_budget(&budget));
    (budget, owner)
}
#[tokio::test]
async fn actual_uplink_restore_qualification_loss_quiets_both_original_owners() {
    let (a, first) = owner();
    let (b, second) = owner();
    let sender = Mutex::new(UdpStreamSender::prepare(Some(&first)).unwrap());
    let turn = sender
        .lock()
        .unwrap()
        .begin(|next, key| return_sender(&sender, next, key))
        .unwrap();
    let replacement = UdpStreamSender::prepare(Some(&second)).unwrap();
    *sender.lock().unwrap() = replacement;
    let before = (a.used(), b.used());
    turn.finish();
    assert!(a.quarantined() && b.quarantined());
    assert_eq!((a.used(), b.used()), before);
    assert!(
        first.uplink_state_for_test().0,
        "returned physical owner must publish quiet"
    );
    assert!(
        second.uplink_state_for_test().0,
        "occupied slot owner must publish its own quiet"
    );
}
#[tokio::test]
async fn actual_uplink_restore_poison_keeps_table_and_quiet() {
    let (budget, root) = owner();
    let sender = Mutex::new(UdpStreamSender::prepare(Some(&root)).unwrap());
    let turn = sender
        .lock()
        .unwrap()
        .begin(|next, key| return_sender(&sender, next, key))
        .unwrap();
    let before = budget.used();
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            let _held = sender.lock().unwrap();
            panic!("actual busy slot interrupted");
        }))
        .is_err()
    );
    turn.finish();
    assert!(sender.is_poisoned());
    assert_eq!(budget.used(), before);
    assert!(
        budget.quarantined(),
        "poison cannot silently restore a clean qualification"
    );
    assert!(root.uplink_state_for_test().0);
}

#[test]
fn actual_unused_cold_table_checks_original_allowance_before_drop() {
    let (budget, root) = owner();
    let UdpStreamSender::Ready(mut stock) = UdpStreamSender::prepare(Some(&root)).unwrap() else {
        panic!()
    };
    let before = budget.used();
    let original = ManuallyDrop::new(stock.table.allowance.take());
    drop(stock);
    assert_eq!(budget.used(), before);
    assert!(
        budget.quarantined(),
        "empty logical table does not prove a valid physical reservation"
    );
    std::mem::forget(original);
}

#[tokio::test]
async fn actual_uplink_foreign_return_keeps_original_qualification_and_both_values() {
    let (a, first) = owner();
    let (b, second) = owner();
    let sender = Mutex::new(UdpStreamSender::prepare(Some(&first)).unwrap());
    let turn = sender
        .lock()
        .unwrap()
        .begin(|next, key| return_sender(&sender, next, key))
        .unwrap();
    let key = turn.key;
    let foreign = UdpStreamSender::prepare(Some(&second)).unwrap();
    let foreign_key = foreign.key().unwrap();
    assert_ne!(key, foreign_key);
    let before = (a.used(), b.used());
    assert!(!return_sender(&sender, Some(foreign), foreign_key).publish());
    assert!(matches!(&*sender.lock().unwrap(), UdpStreamSender::Broken(q) if q.key == key));
    assert!(!turn.finish());
    assert!(matches!(&*sender.lock().unwrap(), UdpStreamSender::Lost(stock) if stock.key() == key));
    assert_eq!((a.used(), b.used()), before);
    assert!(first.uplink_state_for_test().0 && second.uplink_state_for_test().0);
}
#[tokio::test]
async fn actual_uplink_cold_full_busy_cancel_and_known_retirement() {
    let layout = std::mem::size_of::<Table>();
    let small = bytes::BackingBudget::new(layout - 1).unwrap();
    let root = StartupResources::udp_owner_for_test(BackingMeter::with_budget(&small));
    assert!(matches!(
        UdpStreamSender::prepare(Some(&root)),
        Err(MeteredCopyError::TooLarge)
    ));
    assert_eq!(small.used(), 0);
    assert!(!small.quarantined());
    let full = bytes::BackingBudget::new(layout * 2).unwrap();
    let meter = BackingMeter::with_budget(&full);
    let full_root = StartupResources::udp_owner_for_test(meter.clone());
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let pressure = bytes::BytesMut::try_zeroed_metered(layout + 1 - header, &meter).unwrap();
    assert!(matches!(
        UdpStreamSender::prepare(Some(&full_root)),
        Err(MeteredCopyError::BudgetFull)
    ));
    assert_eq!(full.used(), layout + 1);
    assert!(!full.quarantined());
    drop(pressure);
    assert_eq!(full.used(), 0);
    let (budget, root) = owner();
    let sender = Mutex::new(UdpStreamSender::prepare(Some(&root)).unwrap());
    assert_eq!(budget.used(), layout);
    let turn = sender
        .lock()
        .unwrap()
        .begin(|next, key| return_sender(&sender, next, key))
        .unwrap();
    assert!(
        sender
            .lock()
            .unwrap()
            .begin(|next, key| return_sender(&sender, next, key))
            .is_none()
    );
    assert_eq!(budget.used(), layout);
    assert!(!budget.quarantined());
    assert!(turn.finish());
    let turn = sender
        .lock()
        .unwrap()
        .begin(|next, key| return_sender(&sender, next, key))
        .unwrap();
    assert_eq!(turn.retire(), Retirement::Complete);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    *sender.lock().unwrap() = UdpStreamSender::prepare(Some(&root)).unwrap();
    let turn = sender
        .lock()
        .unwrap()
        .begin(|next, key| return_sender(&sender, next, key))
        .unwrap();
    let key = turn.key;
    drop(turn); // Actual qualification cancellation keeps the whole original Box/token.
    assert!(matches!(&*sender.lock().unwrap(), UdpStreamSender::Lost(stock) if stock.key() == key));
    assert!(budget.quarantined() && root.uplink_state_for_test().0);
    drop(sender);
    assert_eq!(budget.used(), layout);
    eprintln!("N1_UPLINK_LAYOUT actual_table={layout}");
}
#[tokio::test]
async fn actual_uplink_first_panic_box_remains_in_original_table() {
    let (budget, root) = owner();
    let UdpStreamSender::Ready(mut stock) = UdpStreamSender::prepare(Some(&root)).unwrap() else {
        panic!()
    };
    let before = budget.used();
    let original = Arc::new(());
    assert!(
        stock
            .attempt(|_| std::panic::panic_any(original.clone()))
            .is_err()
    );
    assert!(Arc::ptr_eq(
        stock
            .table
            .panic
            .as_ref()
            .unwrap()
            .downcast_ref::<Arc<()>>()
            .unwrap(),
        &original
    ));
    assert!(root.uplink_state_for_test().0);
    drop(stock);
    assert_eq!(Arc::strong_count(&original), 2);
    assert_eq!(budget.used(), before);
}

#[tokio::test]
async fn actual_uplink_retirement_rejects_broken_qualification_before_physical_drop() {
    let (a, first) = owner();
    let (b, second) = owner();
    let sender = Mutex::new(UdpStreamSender::prepare(Some(&first)).unwrap());
    let turn = sender
        .lock()
        .unwrap()
        .begin(|next, key| return_sender(&sender, next, key))
        .unwrap();
    let key = turn.key;
    let foreign = UdpStreamSender::prepare(Some(&second)).unwrap();
    let foreign_key = foreign.key().unwrap();
    let before = (a.used(), b.used());
    assert!(!return_sender(&sender, Some(foreign), foreign_key).publish());
    assert_eq!(turn.retire(), Retirement::Lost);
    assert_eq!(
        (a.used(), b.used()),
        before,
        "qualification validation must precede actual Box destruction and refund"
    );
    assert!(matches!(&*sender.lock().unwrap(), UdpStreamSender::Lost(stock) if stock.key() == key));
}

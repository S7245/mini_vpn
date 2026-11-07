use super::*;

#[tokio::test]
async fn actual_empty_alias_cannot_become_retired_inventory() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (tx, rx) = callback::Custody::pair(&meter).unwrap();
    let stock = Stock::new(&meter, rx).unwrap();
    let alias = stock.0.0.lock().idle.as_mut().unwrap().split_off(CAPACITY);
    assert!(alias.is_empty());
    assert!(alias.precharged_backing_matches(&meter));
    stock.begin_retirement();
    let result = stock.poll_retire(&mut Context::from_waker(std::task::Waker::noop()));
    let refused = result.is_pending();
    let q = budget.quarantined();
    drop((stock, tx, alias));
    assert!(refused && q, "actual Shared alias must retain original stock in Q, not report retired");
    assert!(budget.used() > 1, "uncertain actual inventory retains its real cold charge");
}

#[test]
fn missing_physical_idle_cannot_be_normal_owner_destruction() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (tx, rx) = callback::Custody::pair(&meter).unwrap();
    let stock = Stock::new(&meter, rx).unwrap();
    let original = stock.0.0.lock().idle.take().unwrap();
    drop(stock);
    let quarantined = budget.quarantined();
    drop((original, tx));
    assert!(quarantined && budget.used() > 1, "missing original allocation cannot normalize cold inventory to clean");
}

#[tokio::test]
async fn known_io_keeps_retirement_notification_and_original_role_origin() {
    use std::{future::Future, sync::atomic::{AtomicUsize, Ordering}, task::{Wake, Waker}};
    struct Count(AtomicUsize);
    impl Wake for Count { fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); } }
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (tx, rx) = callback::Custody::pair(&meter).unwrap();
    let stock = Stock::new(&meter, rx).unwrap();
    let access = stock.access();
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let held = BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    let packet = access.acquire(40).unwrap().unwrap();
    tx.note_write(Err(io::Error::from_raw_os_error(5)));
    let original = callback::original_io(&tx.finish().unwrap_err()).unwrap();
    let mut tail = callback::Tail::with_stock(tx, Some(stock));
    assert_eq!(access.acquire(40).unwrap_err().raw_os_error(), None);
    let wake = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut work = Box::pin(tail.settle());
    assert!(work.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    let charged = budget.used();
    drop(packet);
    assert_eq!(budget.used(), charged);
    assert_eq!(wake.0.load(Ordering::SeqCst), 1, "ordinary IO does not suppress actual loan return");
    let claim = tokio::time::timeout(std::time::Duration::from_secs(1), work).await.unwrap();
    let report = claim.report(super::super::PumpControl::origin(crate::shared::errors::FailurePhase::DeviceRead));
    assert_eq!(report.failure.as_ref().unwrap().facts().origin.phase, crate::shared::errors::FailurePhase::DeviceWrite);
    assert_eq!(report.failure.as_ref().unwrap().facts().raw_os_code, Some(5));
    assert!(Arc::ptr_eq(report.source.as_ref().unwrap(), &original));
    assert_eq!(report.additional, 0);
    drop((report, original, access, tail, held));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn q_with_actual_loan_is_pending_and_unknown_owner_loss_retains_stock() {
    use std::{future::Future, task::Waker};
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (tx, rx) = callback::Custody::pair(&meter).unwrap();
    let stock = Stock::new(&meter, rx).unwrap();
    let access = stock.access();
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let held = BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    let packet = access.acquire(40).unwrap().unwrap();
    let mut tail = callback::Tail::with_stock(tx, Some(stock));
    meter.quarantine();
    {
        let mut work = Box::pin(tail.settle());
        assert!(work.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    }
    drop(tail); // Unknown shutdown owner loss; JOIN/role cancellation cannot retire this stock.
    drop(packet);
    drop((access, held));
    assert!(budget.quarantined());
    assert!(budget.used() >= cold_bytes(), "real protected allocation and its record remain retained");
}

#[test]
fn protected_cold_rejects_wrong_role_owner_and_retains_bad_layout() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let foreign = BackingMeter::with_budget(&budget);
    let (tx, rx) = callback::Custody::pair(&meter).unwrap();
    assert!(Stock::new(&meter, tx.clone()).is_err());
    assert!(Stock::new(&foreign, rx.clone()).is_err());
    assert!(!budget.quarantined());
    let mut stock = Stock::new(&meter, rx).unwrap();
    assert!(stock.bind(&foreign).is_err());
    assert!(!budget.quarantined());
    let core = Arc::get_mut(&mut stock.0.0).unwrap();
    let original = std::mem::replace(&mut core._layout, meter.reserve_cold_layout(1).unwrap().unwrap());
    assert!(stock.bind(&meter).is_err());
    drop((stock, original, tx));
    assert!(budget.quarantined());
    assert!(budget.used() > CAPACITY);
}

impl RxPacket {
    pub(crate) fn test_protection_cold_bytes() -> usize { cold_bytes() }
}

impl Access {
    pub(crate) fn test_inventory_probe(&self) -> impl Fn() -> Option<bytes::AllocationIdentity> + Send + Sync + 'static + use<> {
        let weak = Arc::downgrade(&self.0);
        move || {
            let core = weak.upgrade()?;
            let state = core.lock();
            let bytes = state.idle.as_ref()?;
            (core.layout_valid() && !state.loan && !state.retired && core.valid(bytes, state.identity))
                .then_some(state.identity)
        }
    }
}

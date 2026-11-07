use super::*;
use std::task::{Wake, Waker};
fn prepared() -> (
    UdpRuntime,
    Arc<node::StartupResources>,
    bytes::BackingBudget,
) {
    let budget = bytes::BackingBudget::new(16 << 20).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let owner = node::StartupResources::udp_owner_for_test(meter);
    let runtime = UdpRuntime::default();
    runtime.prepare_native(&owner).unwrap();
    (runtime, owner, budget)
}
#[tokio::test]
async fn actual_monitor_join_error_keeps_original_payload_and_first_io() {
    struct Bomb(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for Bomb {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("original monitor payload Drop");
        }
    }
    let (runtime, _owner, budget) = prepared();
    let clock = runtime.native.get().unwrap().clock().unwrap();
    clock.io(&io::Error::from_raw_os_error(5));
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let original = drops.clone();
    let task = tokio::spawn(async move {
        std::panic::panic_any(Bomb(original));
    });
    assert!(
        !runtime
            .prepare_monitor_join(task)
            .unwrap_or_else(|_| panic!("original role must admit"))
            .await
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let state = runtime.native.get().unwrap().lock();
    let stock = state.as_ref().unwrap();
    assert!(matches!(stock.monitor, Monitor::Failed));
    drop(state);
    assert!(clock.reported(0));
    assert!(clock.reported(1));
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}
struct Hostile;
impl Wake for Hostile {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(11u8);
    }
}
impl Drop for Hostile {
    fn drop(&mut self) {
        std::panic::panic_any(22u8);
    }
}
#[tokio::test]
async fn actual_monitor_join_original_wake_and_drop_are_both_guarded() {
    let (runtime, _owner, budget) = prepared();
    let (send, receive) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let _ = receive.await;
    });
    let original = Waker::from(Arc::new(Hostile));
    let mut join = Box::pin(
        runtime
            .prepare_monitor_join(task)
            .unwrap_or_else(|_| panic!("original role must admit")),
    );
    assert!(
        join.as_mut()
            .poll(&mut Context::from_waker(&original))
            .is_pending()
    );
    drop(original);
    send.send(()).unwrap();
    tokio::task::yield_now().await;
    assert_eq!(
        join.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(false)
    );
    drop(join);
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}
#[tokio::test]
async fn completed_monitor_and_unstarted_udp_really_retire_cold_stock() {
    let (runtime, _owner, budget) = prepared();
    eprintln!("[N1-Node-UDP-cold-capacity] original_runtime_complete_cold={} message_slots=1024 producer_records=257 callback_roles=261 task_root_included=true quinn_endpoint_connections_and_os_not_included=true", budget.used());
    let task = tokio::spawn(async {});
    assert!(
        runtime
            .prepare_monitor_join(task)
            .unwrap_or_else(|_| panic!("original role must admit"))
            .await
    );
    assert!(runtime.finish_lane());
    assert!(runtime.retire_native(true));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn original_uplink_admission_keeps_typed_pressure_first_ordinal_and_earliest_deadline() {
    let (runtime, owner, budget) = prepared();
    assert!(runtime.owner.set(Arc::downgrade(&owner)).is_ok());
    let clock = runtime.native.get().unwrap().clock().unwrap();
    runtime.uplink_error(MeteredCopyError::BudgetFull);
    runtime.uplink_error(MeteredCopyError::TooLarge);
    assert!(!runtime.node_stopping());
    assert!(!clock.reported(0));
    assert!(!budget.quarantined());
    let original = tokio::time::Instant::now() + Duration::from_secs(2);
    runtime.state().deadline = Some(original);
    clock.io(&io::Error::from_raw_os_error(5));
    runtime.uplink_error(MeteredCopyError::Allocation);
    assert!(clock.reported(0) && clock.reported(1));
    assert!(!clock.reported(2));
    assert!(runtime.node_stopping());
    assert_eq!(runtime.state().deadline, Some(original));
    runtime.uplink_error(MeteredCopyError::Quarantined);
    assert!(!clock.reported(2), "Q must not fabricate another IO ordinal");
    assert_eq!(runtime.state().deadline, Some(original));
    assert!(budget.quarantined() && budget.used() > 0);
}

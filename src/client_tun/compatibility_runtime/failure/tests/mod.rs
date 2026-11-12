//! Original Session receives a late Node-owner destructor failure before JOIN.
use super::*;
use crate::shared::errors::{FailureOrder, FailureResource};
struct OriginalNode;
struct NodeDropPayload;
impl QuietFailureSink for OriginalNode {
    fn record_quiet(&self, _: RetainedFailure) {}
}
impl Drop for OriginalNode {
    fn drop(&mut self) {
        std::panic::panic_any(NodeDropPayload);
    }
}
#[test]
fn node_owner_drop_reaches_original_session_first_stop_and_deadline() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut original = SessionControl::default();
    original.app_meter = Some(meter.clone());
    let owner = Arc::new(original);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
    owner.ledger().shutdown_deadline = Some(deadline);
    let node: Arc<dyn QuietFailureSink> = Arc::new(OriginalNode);
    let parent: Arc<dyn QuietFailureSink> = owner.clone();
    let clock = FailureOrder::default();
    clock
        .bind_with_parent(
            Arc::downgrade(&node),
            Some(Arc::downgrade(&parent)),
            crate::shared::errors::FailureOrigin {
                phase: FailurePhase::UdpJoin,
                resource: FailureResource::UdpDriver,
                resource_id: 11,
                attempt_id: None,
            },
            Some(meter),
        )
        .unwrap();
    let publication = clock.io_deferred(&std::io::Error::from_raw_os_error(5));
    assert_eq!(publication.order, 0);
    drop(node);
    drop(publication); // Original Node final destructor, Session remains alive.
    assert!(clock.owner_drop_payload_is::<NodeDropPayload>());
    assert!(budget.quarantined());
    assert!(clock.reported(0));
    let ledger = owner.ledger();
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().raw_os_code,
        Some(5)
    );
    assert_eq!(
        ledger.additional_failures, 1,
        "live Session must receive the actual Node Drop failure"
    );
    assert_eq!(ledger.shutdown_deadline, Some(deadline));
    assert!(ledger.quarantined());
    drop(ledger);
    assert!(owner.stop.stopping.load(Ordering::Acquire));
    assert!(
        !clock.reported(1),
        "Session-only Drop failure is not Node delivery"
    );
    drop((clock, parent, owner));
    assert!(budget.used() > 0);
}

struct QuietNode;
impl QuietFailureSink for QuietNode {
    fn record_quiet(&self, _: RetainedFailure) {}
}
#[test]
fn parent_deadline_reads_original_session_and_parent_minimum_without_extending() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut original = SessionControl::default();
    original.app_meter = Some(meter.clone());
    let owner = Arc::new(original);
    let now = tokio::time::Instant::now();
    let root_deadline = Arc::new(Mutex::new(Some(now + std::time::Duration::from_secs(3))));
    owner.parent_deadline.set(root_deadline.clone()).unwrap();
    owner.ledger().shutdown_deadline = Some(now + std::time::Duration::from_secs(5));
    let node: Arc<dyn QuietFailureSink> = Arc::new(QuietNode);
    let parent: Arc<dyn QuietFailureSink> = owner.clone();
    let clock = FailureOrder::default();
    clock
        .bind_with_parent(
            Arc::downgrade(&node),
            Some(Arc::downgrade(&parent)),
            SessionControl::origin(FailurePhase::UdpJoin),
            Some(meter),
        )
        .unwrap();
    assert_eq!(
        clock.parent_deadline().unwrap(),
        *root_deadline.lock().unwrap()
    );
    let earlier = now + std::time::Duration::from_secs(1);
    owner.ledger().shutdown_deadline = Some(earlier);
    *root_deadline.lock().unwrap() = Some(now + std::time::Duration::from_secs(15));
    assert_eq!(clock.parent_deadline().unwrap(), Some(earlier));
    assert_eq!(owner.ledger().shutdown_deadline, Some(earlier));
    assert!(!owner.stop.stopping.load(Ordering::Acquire));
    assert!(owner.ledger().first_failure.is_none());
    assert!(!budget.quarantined());
    drop((owner, parent));
    assert!(
        clock.parent_deadline().is_err(),
        "lost parent is not a fresh no-deadline state"
    );
    assert!(budget.quarantined());
    drop((clock, node));
    assert!(budget.used() > 0);
}

mod device_source;

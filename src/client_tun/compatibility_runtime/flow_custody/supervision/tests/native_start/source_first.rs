//! Real Native role failure publication before the Root polls its JoinHandles.
use super::*;
use crate::client_tun::compatibility_runtime::SessionControl;
use crate::client_tun::compatibility_runtime::flow_custody::transaction::FlowIdentity;

#[derive(Debug)]
struct OriginalIo(
    Arc<AtomicUsize>,
    std::sync::Weak<SessionControl>,
    Arc<AtomicUsize>,
    Option<Arc<AtomicUsize>>,
);
impl std::fmt::Display for OriginalIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("source A")
    }
}
impl std::error::Error for OriginalIo {}
impl Drop for OriginalIo {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        if let Some(owner) = self.1.upgrade() {
            self.2.store(
                usize::from(owner.opens.try_lock().is_err()),
                Ordering::SeqCst,
            );
        }
        if let Some(counter) = self.3.take() {
            std::panic::panic_any(RetainedBox(counter));
        }
    }
}
struct Source {
    meter: bytes::BackingMeter,
    error: Option<std::io::Error>,
    quarantine_error: bool,
}
impl NativeTcpReader for Source {
    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        Some(self.meter.clone())
    }
    fn source_read_bytes(&self) -> Option<usize> {
        Some(0)
    }
    fn poll_read_chunk(
        &mut self,
        _: &mut Context<'_>,
        _: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        match self.error.take() {
            Some(error) => {
                if self.quarantine_error { self.meter.quarantine(); }
                Poll::Ready(Err(error))
            },
            None => Poll::Pending,
        }
    }
}

#[tokio::test]
async fn source_io_is_in_original_session_before_join_and_unknown_callback() {
    for mode in 0..4 {
        original_disposal(mode).await;
    }
}
#[tokio::test(flavor = "current_thread")]
async fn source_completed_original_io_in_q_is_reported_once_before_join() {
    original_disposal(4).await;
}
async fn original_disposal(mode: u8) {
    let secondary_drops = Arc::new(AtomicUsize::new(0));
    let budget = crate::client_tun::compatibility_runtime::flow_custody::supervision::roles::native::stock_tests::budget_with_old_headroom(65536);
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = Arc::new({
        let mut control = SessionControl::default();
        control.app_meter = Some(meter.clone());
        control
    });
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_locked = Arc::new(AtomicUsize::new(0));
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    use crate::client_tun::compatibility_runtime::flow_custody::transaction::{
        OpenCustody, OpenDispatch,
    };
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _notices) = OpenDispatch::channel(4, Some(owner.clone()));
    dispatch.begin_turn().await;
    let id = dispatch
        .reserve_flow(
            Arc::new(super::forced_root::UnusedUpstream),
            crate::shared::TargetAddr::IpPort("127.0.0.1:9".parse().unwrap()),
            handle,
            7,
        )
        .unwrap();
    let (_tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, _back) = crate::client_tun::relay_channel::channel(2);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let clock = FlowRoles::prepare_failure_owner(&owner, id).unwrap();
    let mut roles = FlowRoles::new_native_with_clock(
        NativeLaunchRequest {
            uplink_observer: None,
            handle,
            epoch: id.epoch,
            native: NativeTcpRelayStream::new(
                Box::new(Source {
                    meter: meter.clone(),
                    quarantine_error: mode == 4,
                    error: Some(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        OriginalIo(
                            drops.clone(),
                            Arc::downgrade(&owner),
                            drop_locked.clone(),
                            (mode == 3).then(|| secondary_drops.clone()),
                        ),
                    )),
                }),
                Box::new(Stream {
                    probe: Arc::new(Probe::default()),
                    fail: false,
                }),
            ),
            engine: NativeTcpRelayEngine::ChunkPump,
            read_floor: false,
            rx,
            back_tx,
            read_credit_rx,
            downlink_backpressure: DownlinkBackpressureConfig::default(),
        },
        clock,
    )
    .ok()
    .unwrap();
    let mut window =
        crate::client_tun::compatibility_runtime::flow_custody::receive_window::ReceiveWindow::new(
        );
    roles.prepare(window.take_reader().unwrap());
    assert!(roles.commit_native());
    roles.start();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while if mode == 4 { !budget.quarantined() } else { roles.failures.load(Ordering::SeqCst) == 0 } {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        roles.join_count(),
        0,
        "Root has not joined any original task"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "original IO stays in source role"
    );
    {
        let ledger = owner.ledger();
        let first = ledger
            .first_failure
            .as_ref()
            .expect("source facts must precede callback");
        assert_eq!(
            first.facts().io_kind,
            Some(std::io::ErrorKind::ConnectionReset)
        );
        assert_eq!(first.facts().origin.resource_id, id.index as u64);
        assert_eq!(ledger.additional_failures, 0);
        if mode == 4 { assert!(ledger.quarantined() && ledger.shutdown_deadline.is_some()); }
        else { assert!(ledger.shutdown_deadline.is_none(), "ordinary flow IO does not stop Session"); }
    }
    if mode == 4 {
        let deadline = owner.ledger().shutdown_deadline;
        assert_eq!(roles.failures.load(Ordering::SeqCst), 1);
        roles.stop();
        until(&mut roles, true, FlowRoles::all_original_roles_joined).await;
        assert_eq!(roles.join_count(), 4);
        assert_eq!(roles.native_pending_reader_error_order(), Some(0));
        let Some(super::super::roles::OriginalFailure::ReportedIo(error)) = roles.take_original_failure() else {
            panic!("actual Source IO must retain its already reported ordinal through JOIN");
        };
        let original = error.get_ref().unwrap().downcast_ref::<OriginalIo>().unwrap();
        assert!(Arc::ptr_eq(&original.0, &drops));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(owner.ledger().additional_failures, 0);
        assert_eq!(owner.ledger().shutdown_deadline, deadline);
        drop(error);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(drop_locked.load(Ordering::SeqCst), 0);
        assert!(budget.quarantined() && budget.used() > 0);
        return;
    }
    let original_panic = if mode == 1 {
        let deadline = owner.request_stop();
        let payload = std::panic::catch_unwind(|| std::panic::panic_any("callback B")).unwrap_err();
        roles.failures.panic();
        assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
        assert_eq!(owner.ledger().additional_failures, 1);
        assert!(budget.quarantined() && budget.used() > 0);
        Some(payload)
    } else {
        None
    };
    roles.stop();
    until(&mut roles, true, FlowRoles::all_original_roles_joined).await;
    assert_eq!(roles.join_count(), 4);
    if mode != 1 {
        until(&mut roles, true, |roles| roles.native_reader_receipt_present() == Some(true)).await;
        assert!(!roles.settled(), "original IO still owns its Source field custody");
    }
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "JOIN alone does not dispose original IO"
    );
    let mut opens = owner.opens();
    opens.flow_mut(id).unwrap().roles = Some(roles);
    let disposal = opens
        .flow_mut(id)
        .unwrap()
        .roles
        .as_mut()
        .unwrap()
        .take_original_disposal(&owner, id)
        .unwrap();
    assert!(
        !opens
            .flow_mut(id)
            .unwrap()
            .roles
            .as_ref()
            .unwrap()
            .settled()
    );
    drop(opens);
    if mode == 2 {
        drop(disposal);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "interruption retains original IO"
        );
        assert!(budget.quarantined() && budget.used() > 0);
        assert!(
            !owner
                .opens()
                .flow_mut(id)
                .unwrap()
                .roles
                .as_ref()
                .unwrap()
                .settled()
        );
        assert_eq!(owner.ledger().additional_failures, 0);
        return;
    }
    disposal.dispose(&owner, &mut Context::from_waker(std::task::Waker::noop()));
    assert_eq!(
        drop_locked.load(Ordering::SeqCst),
        0,
        "original source IO destructor must run outside opens"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let additional = u64::from(mode == 1 || mode == 3);
    assert_eq!(
        owner.ledger().additional_failures,
        additional,
        "JOIN does not recount source A"
    );
    if mode == 3 {
        assert!(
            owner
                .opens()
                .flow_mut(id)
                .unwrap()
                .roles
                .as_ref()
                .unwrap()
                .panic
                .as_ref()
                .unwrap()
                .is::<RetainedBox>()
        );
        assert_eq!(secondary_drops.load(Ordering::SeqCst), 0);
        assert!(budget.quarantined() && budget.used() > 0);
    } else if mode == 0 {
        assert!(!budget.quarantined());
    }

    assert_eq!(
        owner
            .ledger()
            .first_failure
            .as_ref()
            .unwrap()
            .facts()
            .io_kind,
        Some(std::io::ErrorKind::ConnectionReset)
    );
    assert!(
        owner
            .opens()
            .flow_mut(id)
            .unwrap()
            .roles
            .as_mut()
            .unwrap()
            .take_original_disposal(&owner, id)
            .is_none()
    );
    assert_eq!(owner.ledger().additional_failures, additional);
    drop(original_panic);
}

struct WakeAndDrop;
impl std::task::Wake for WakeAndDrop {
    fn wake(self: Arc<Self>) {
        panic!("original callback wake");
    }
    fn wake_by_ref(self: &Arc<Self>) {
        panic!("original callback wake");
    }
}
impl Drop for WakeAndDrop {
    fn drop(&mut self) {
        panic!("original callback Drop");
    }
}

#[tokio::test(start_paused = true)]
async fn cross_flow_capture_order_precedes_two_original_callback_failures() {
    use crate::shared::errors::{FailureOrder, QuietFailureSink};
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = Arc::new({
        let mut control = SessionControl::default();
        control.app_meter = Some(meter.clone());
        control
    });
    let sink: Arc<dyn QuietFailureSink> = owner.clone();
    let origin = |index| crate::shared::errors::FailureOrigin {
        phase: crate::shared::errors::FailurePhase::TcpOpen,
        resource: crate::shared::errors::FailureResource::TcpOpen,
        resource_id: index,
        attempt_id: Some(7),
    };
    let flow2 = FailureOrder::default();
    let flow1 = FailureOrder::default();
    assert!(
        flow2
            .bind(Arc::downgrade(&sink), origin(2), Some(meter.clone()))
            .is_ok()
    );
    assert!(
        flow1
            .bind(Arc::downgrade(&sink), origin(1), Some(meter.clone()))
            .is_ok()
    );
    let first = std::io::Error::from_raw_os_error(5);
    let second = std::io::Error::from_raw_os_error(54);
    let a = flow2.io(&first);
    let c = flow1.io(&second);
    assert!(flow2.reported(a) && flow1.reported(c));
    assert!(owner.ledger().shutdown_deadline.is_none());
    assert!(!owner.stop.stopping.load(Ordering::Acquire));
    let deadline = owner.request_stop();
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    let waker = std::task::Waker::from(Arc::new(WakeAndDrop));
    let wake =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| waker.wake_by_ref())).unwrap_err();
    flow1.panic();
    let drop_panic =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(waker))).unwrap_err();
    flow1.panic();
    let ledger = owner.ledger();
    let facts = ledger.first_failure.as_ref().unwrap().facts();
    assert_eq!(
        facts.origin.resource_id, 2,
        "capture order, never Flow-index scan order"
    );
    assert_eq!(facts.raw_os_code, Some(5));
    assert_eq!(ledger.additional_failures, 3);
    assert_eq!(ledger.shutdown_deadline, Some(deadline));
    assert!(ledger.quarantined() && budget.quarantined() && budget.used() > 0);
    assert!(wake.downcast_ref::<&str>().is_some());
    assert!(drop_panic.downcast_ref::<&str>().is_some());
}

#[test]
fn lost_original_owner_keeps_failure_unreported_and_budget_quarantined() {
    use crate::shared::errors::{FailureOrder, QuietFailureSink};
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = Arc::new({
        let mut control = SessionControl::default();
        control.app_meter = Some(meter.clone());
        control
    });
    let sink: Arc<dyn QuietFailureSink> = owner.clone();
    let clock = FailureOrder::default();
    let origin = crate::shared::errors::FailureOrigin {
        phase: crate::shared::errors::FailurePhase::TcpOpen,
        resource: crate::shared::errors::FailureResource::TcpOpen,
        resource_id: 2,
        attempt_id: Some(7),
    };
    assert!(
        clock
            .bind(Arc::downgrade(&sink), origin, Some(meter))
            .is_ok()
    );
    drop(sink);
    drop(owner);
    let order = clock.io(&std::io::Error::from_raw_os_error(5));
    assert!(!clock.reported(order));
    assert_eq!(clock.load(Ordering::SeqCst), usize::MAX);
    assert!(budget.quarantined() && budget.used() > 0);
}

#[derive(Debug)]
struct RetainedBox(Arc<AtomicUsize>);
impl Drop for RetainedBox {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct OriginalOwnerDrop(Arc<AtomicUsize>);
impl std::fmt::Display for OriginalOwnerDrop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original owner Drop")
    }
}
impl std::error::Error for OriginalOwnerDrop {}
impl Drop for OriginalOwnerDrop {
    fn drop(&mut self) {
        std::panic::panic_any(RetainedBox(self.0.clone()));
    }
}
#[tokio::test]
async fn last_session_arc_drop_keeps_both_original_boxes_and_charged_q() {
    use crate::shared::errors::{FailureOrder, QuietFailureSink};
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = Arc::new({
        let mut control = SessionControl::default();
        control.app_meter = Some(meter.clone());
        control
    });
    let primary_drops = Arc::new(AtomicUsize::new(0));
    let secondary_drops = Arc::new(AtomicUsize::new(0));
    owner.ledger().first_egress_io = Some(Arc::new(std::io::Error::other(OriginalOwnerDrop(
        secondary_drops.clone(),
    ))));
    let clock = Arc::new(FailureOrder::default());
    let sink: Arc<dyn QuietFailureSink> = owner.clone();
    clock
        .bind(
            Arc::downgrade(&sink),
            SessionControl::origin(crate::shared::errors::FailurePhase::TcpOpen),
            Some(meter),
        )
        .unwrap();
    let primary =
        std::panic::catch_unwind(|| std::panic::panic_any(RetainedBox(primary_drops.clone())))
            .unwrap_err();
    let observer = clock.clone();
    let publication = observer.panic_deferred();
    drop(clock);
    drop(sink);
    drop(owner);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(publication))).is_ok());
    assert!(observer.owner_drop_payload_is::<RetainedBox>());
    assert!(primary.is::<RetainedBox>());
    assert_eq!(primary_drops.load(Ordering::SeqCst), 0);
    assert_eq!(secondary_drops.load(Ordering::SeqCst), 0);
    drop(observer);
    assert_eq!(budget.used(), 1);
    assert!(budget.quarantined());
    assert_eq!(secondary_drops.load(Ordering::SeqCst), 0);
    drop(primary);
    assert_eq!(primary_drops.load(Ordering::SeqCst), 1);
}

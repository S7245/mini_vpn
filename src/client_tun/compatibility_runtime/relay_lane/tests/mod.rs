use super::*;
#[test]
fn actual_driver_constructor_refuses_hard_cold_budget_without_tokio_fallback() {
    let budget = bytes::BackingBudget::new(1).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut owner = SessionControl::default();
    owner.app_meter = Some(meter);
    let owner = Arc::new(owner);
    let result = owner.prepare_relay_lane(1024, true);
    assert!(
        result.is_err(),
        "hard driver constructor must refuse before any Flow/actor, never return a Tokio lane"
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn exact_driver_layout_rejects_one_short_and_original_retirement_releases_every_allocation() {
    let size =
        relay_channel::native_layout_bytes::<Event>(1024).unwrap() + FailureOrder::layout_bytes();
    eprintln!("[N1-App-cold-capacity] slots=1024 actual_layout_and_clock={size} service_control_platform_headers_separate=true");
    for limit in [size - 1, size] {
        let budget = bytes::BackingBudget::new(limit).unwrap();
        let mut owner = SessionControl::default();
        owner.app_meter = Some(bytes::BackingMeter::with_budget(&budget));
        let owner = Arc::new(owner);
        let result = owner.prepare_relay_lane(1024, true);
        if limit < size {
            assert!(result.is_err());
        } else {
            let (tx, rx) = result.unwrap();
            assert_eq!(budget.used(), size);
            drop(rx);
            drop(tx);
            tokio::time::timeout(std::time::Duration::from_secs(1), owner.settle_relay_lane())
                .await
                .unwrap();
        }
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[tokio::test(start_paused = true)]
async fn actual_typed_rejection_keeps_original_first_deadline_and_physical_quarantine() {
    use crate::{client_tun::StreamServiceWindow, tcp_downlink_pump::OwnedPayload};
    for foreign in [false, true] {
        let budget = bytes::BackingBudget::new(1 << 22).unwrap();
        let foreign_budget = bytes::BackingBudget::new(4096).unwrap();
        let foreign_meter = bytes::BackingMeter::with_budget(&foreign_budget);
        let mut owner = SessionControl::default();
        owner.app_meter = Some(bytes::BackingMeter::with_budget(&budget));
        let owner = Arc::new(owner);
        let (tx, mut rx) = owner.prepare_relay_lane(2, true).unwrap();
        owner.record(RetainedFailure::from_io(
            SessionControl::origin(FailurePhase::TcpOpen),
            std::io::Error::from_raw_os_error(5),
        ));
        let original = owner.request_stop();
        tokio::time::advance(std::time::Duration::from_secs(3)).await;
        let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
        let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 1]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 1]),
        ));
        let payload = if foreign {
            OwnedPayload::Native((bytes::Bytes::copy_from_slice_metered(b"abc", &foreign_meter).unwrap()).into())
        } else {
            OwnedPayload::from(vec![1, 2, 3])
        };
        tx.reserve().await.unwrap().send((
            handle,
            RelayEvent::Data {
                epoch: 1,
                bytes: payload,
                stream_service_window: StreamServiceWindow::default(),
                downstream_permit: None,
            },
        ));
        assert!(budget.quarantined());
        assert!(
            rx.try_recv().is_err(),
            "invalid original value never escapes Q custody"
        );
        let ledger = owner.ledger();
        assert_eq!(
            ledger.first_failure.as_ref().unwrap().facts().raw_os_code,
            Some(5)
        );
        assert_eq!(ledger.shutdown_deadline, Some(original));
        assert!(ledger.quarantined);
        drop(ledger);
        drop((tx, rx, owner));
        assert!(budget.used() > 1);
        if foreign {
            assert!(
                foreign_budget.used() > 0,
                "original foreign backing retained with actual event"
            );
        }
    }
}

impl SessionControl {
    /// Safety additionally requires the concrete test's closed publisher set:
    /// no external Sender thread/callback, and no await or Service poll here.
    pub(in crate::client_tun::compatibility_runtime) unsafe fn inspect_relay_lane_for_test(
        &self,
        inspect: impl Fn(&Event, bool) -> usize,
    ) -> Option<((usize, usize), (usize, usize), (usize, usize))> {
        assert_eq!(
            tokio::runtime::Handle::current().runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread
        );
        let slot = self.relay_lane.lock().unwrap();
        unsafe { slot.as_ref()?.inspect_values_for_test(inspect) }
    }
}

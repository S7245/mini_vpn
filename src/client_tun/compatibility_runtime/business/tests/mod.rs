//! The borrowed tail's original pin/drop boundary, including cancellation.
use super::*;
use std::task::{Context, Waker};

struct FaultTail {
    poll: Option<Arc<usize>>,
    disposal: Arc<usize>,
}
impl Future for FaultTail {
    type Output = Option<crate::device::egress::CallbackReport>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(payload) = self.poll.take() {
            std::panic::panic_any(payload);
        }
        Poll::Pending
    }
}
impl Drop for FaultTail {
    fn drop(&mut self) {
        std::panic::panic_any(self.disposal.clone());
    }
}

#[tokio::test(start_paused = true)]
async fn callback_shutdown_poll_and_drop_keep_original_boxes_and_deadline() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let owner = { let mut control = SessionControl::default(); control.app_meter = Some(bytes::BackingMeter::with_budget(&budget)); control };
    let deadline = owner.request_stop();
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    let first = Arc::new(17usize);
    let second = Arc::new(29usize);
    let work = CallbackWork::new(
        FaultTail {
            poll: Some(first.clone()),
            disposal: second.clone(),
        },
        &owner,
    );
    let mut work = std::pin::pin!(work);
    let Poll::Ready((None, custody)) = work.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("both original faults must be caught before D can unwind");
    };
    assert!(Arc::ptr_eq(
        custody
            .poll
            .as_ref()
            .unwrap()
            ._payload
            .downcast_ref::<Arc<usize>>()
            .unwrap(),
        &first
    ));
    assert!(Arc::ptr_eq(
        custody
            .disposal
            .as_ref()
            .unwrap()
            ._payload
            .downcast_ref::<Arc<usize>>()
            .unwrap(),
        &second
    ));
    assert!(
        work.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let ledger = owner.ledger();
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().origin.phase,
        FailurePhase::DeviceShutdown
    );
    assert_eq!(ledger.additional_failures, 1);
    assert_eq!(ledger.unknown_panic_payloads, 2);
    assert_eq!(ledger.shutdown_deadline, Some(deadline));
    assert!(ledger.first_egress_io.is_none());
    assert!(ledger.quarantined);
    assert_eq!(budget.used(), 1);
}

#[tokio::test(start_paused = true)]
async fn callback_shutdown_cancellation_disposes_in_place_without_escaping() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let owner = { let mut control = SessionControl::default(); control.app_meter = Some(bytes::BackingMeter::with_budget(&budget)); control };
    let deadline = owner.request_stop();
    let original = Arc::new(31usize);
    {
        let work = CallbackWork::new(
            FaultTail {
                poll: None,
                disposal: original.clone(),
            },
            &owner,
        );
        let mut work = std::pin::pin!(work);
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(
        Arc::strong_count(&original),
        2,
        "cancelled work retains its actual original Box"
    );
    let ledger = owner.ledger();
    assert_eq!(
        ledger.first_failure.as_ref().unwrap().facts().class,
        crate::shared::errors::FailureClass::Panic
    );
    assert_eq!(ledger.additional_failures, 0);
    assert_eq!(ledger.unknown_panic_payloads, 1);
    assert_eq!(ledger.shutdown_deadline, Some(deadline));
    assert!(ledger.quarantined);
    assert_eq!(budget.used(), 1);
}

#[cfg(feature = "harness")]
mod ingress;

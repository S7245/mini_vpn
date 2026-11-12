//! Original pump IO is accepted before opaque observer Drop; real shutdown must
//! neither overwrite the first facts nor count a second synthetic IO.
use super::*;
use crate::{
    device::{DeviceBinding, DeviceIngressFailure},
    harness::{LoopbackTunDevice, PacketLink},
};

struct ReadyThenDrop {
    result: Option<DeviceIngressFailure>,
    payload: Arc<usize>,
    poll_panic: Option<Arc<usize>>,
}
impl Future for ReadyThenDrop {
    type Output = Option<DeviceIngressFailure>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(payload) = self.poll_panic.take() {
            std::panic::panic_any(payload);
        }
        Poll::Ready(self.result.take())
    }
}
impl Drop for ReadyThenDrop {
    fn drop(&mut self) {
        std::panic::panic_any(self.payload.clone());
    }
}

#[tokio::test(start_paused = true)]
async fn cold_startup_original_io_precedes_ready_observer_drop_panic_and_is_not_recounted() {
    let budget = bytes::BackingBudget::new(65536 + crate::device::RxPacket::test_protection_cold_bytes()).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = { let mut control = SessionControl::default(); control.app_meter = Some(meter.clone()); control };
    let deadline = owner.request_stop();
    tokio::time::advance(std::time::Duration::from_secs(3)).await;
    let inbound = PacketLink::bounded(1);
    inbound.push(bytes::BytesMut::zeroed(65540));
    let mut device = LoopbackTunDevice::native_with_factory(1, 1200, &meter, || {
        Ok((inbound, PacketLink::bounded(1)))
    })
    .unwrap();
    device
        .bind_backing_meter(DeviceBinding::Native {
            meter: &meter,
            slots: 1,
            mtu: 1200,
        })
        .unwrap();
    let observer = device.ingress_failure_observer().unwrap();
    let result = observer.wait().await;
    let original = Arc::new(41usize);
    let (accepted, custody) = CallbackWork::new(
        ReadyThenDrop {
            result,
            payload: original.clone(),
            poll_panic: None,
        },
        &owner,
    )
    .with_ingress(&observer)
    .await;
    assert_eq!(accepted, Some(true));
    assert!(custody.poll.is_none());
    assert!(Arc::ptr_eq(
        custody
            .disposal
            .as_ref()
            .unwrap()
            ._payload
            .downcast_ref::<Arc<usize>>()
            .unwrap(),
        &original
    ));
    shutdown_device(&mut device, &owner).await;
    shutdown_device(&mut device, &owner).await;
    let ledger = owner.ledger();
    let first = ledger.first_failure.as_ref().unwrap().facts();
    assert_eq!(
        first.origin.resource,
        crate::shared::errors::FailureResource::TunPump
    );
    assert_eq!(first.origin.phase, FailurePhase::DeviceRead);
    assert_eq!(first.io_kind, Some(std::io::ErrorKind::InvalidInput));
    assert_eq!(ledger.additional_failures, 1);
    assert_eq!(ledger.unknown_panic_payloads, 1);
    assert_eq!(ledger.shutdown_deadline, Some(deadline));
    assert!(ledger.first_egress_io.is_none());
    assert!(ledger.quarantined());
    drop(ledger);
    drop(device);
    drop(observer);
    assert_eq!(budget.used(), crate::device::egress::CallbackCustody::test_cold_bytes(2));
    assert!(budget.quarantined());
}

#[tokio::test]
async fn cold_startup_poll_and_drop_panic_before_ticket_return_keep_original_source_first() {
    let budget = bytes::BackingBudget::new(65536 + crate::device::RxPacket::test_protection_cold_bytes()).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let owner = { let mut control = SessionControl::default(); control.app_meter = Some(meter.clone()); control };
    let inbound = PacketLink::bounded(1);
    inbound.push(bytes::BytesMut::zeroed(65540));
    let mut device = LoopbackTunDevice::native_with_factory(1, 1200, &meter, || {
        Ok((inbound, PacketLink::bounded(1)))
    })
    .unwrap();
    device
        .bind_backing_meter(DeviceBinding::Native {
            meter: &meter,
            slots: 1,
            mtu: 1200,
        })
        .unwrap();
    let observer = device.ingress_failure_observer().unwrap();
    let result = observer.wait().await;
    let poll = Arc::new(43usize);
    let disposal = Arc::new(47usize);
    let (accepted, custody) = CallbackWork::new(
        ReadyThenDrop {
            result,
            payload: disposal.clone(),
            poll_panic: Some(poll.clone()),
        },
        &owner,
    )
    .with_ingress(&observer)
    .await;
    assert_eq!(accepted, None);
    assert!(Arc::ptr_eq(
        custody
            .poll
            .as_ref()
            .unwrap()
            ._payload
            .downcast_ref::<Arc<usize>>()
            .unwrap(),
        &poll
    ));
    assert!(Arc::ptr_eq(
        custody
            .disposal
            .as_ref()
            .unwrap()
            ._payload
            .downcast_ref::<Arc<usize>>()
            .unwrap(),
        &disposal
    ));
    shutdown_device(&mut device, &owner).await;
    let ledger = owner.ledger();
    let first = ledger.first_failure.as_ref().unwrap().facts();
    assert_eq!(
        first.origin.resource,
        crate::shared::errors::FailureResource::TunPump
    );
    assert_eq!(first.io_kind, Some(std::io::ErrorKind::InvalidInput));
    assert_eq!(ledger.additional_failures, 2);
    assert_eq!(ledger.unknown_panic_payloads, 2);
    drop(ledger);
    assert!(observer.claim().is_none());
    drop(device);
    drop(observer);
    assert_eq!(budget.used(), crate::device::egress::CallbackCustody::test_cold_bytes(2));
    assert!(budget.quarantined());
}

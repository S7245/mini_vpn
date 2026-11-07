//! Real original pump pause/receipt retained past the original business deadline.
//! No Node/transport acceptance claim; this locks the same production cut helper.
use super::*;
use crate::device::{DeviceCutReceipt, DeviceShutdown};
use crate::harness::{LoopbackTunDevice, PacketLink};
use smoltcp::phy::{Device, DeviceCapabilities};
use std::task::Context;

struct DelayedCut {
    inner: LoopbackTunDevice,
    receipt: Option<DeviceCutReceipt>,
    reject_at: Option<tokio::time::Instant>,
    drops: Arc<AtomicUsize>,
    panic_after_ready: bool,
}
impl Drop for DelayedCut {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Release);
    }
}
impl Device for DelayedCut {
    type RxToken<'a> = <LoopbackTunDevice as Device>::RxToken<'a>;
    type TxToken<'a> = <LoopbackTunDevice as Device>::TxToken<'a>;
    fn receive(
        &mut self,
        t: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.inner.receive(t)
    }
    fn transmit(&mut self, t: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.inner.transmit(t)
    }
    fn capabilities(&self) -> DeviceCapabilities {
        self.inner.capabilities()
    }
}
struct DropAfterReady<F> {
    inner: Pin<Box<F>>,
    ready: bool,
    panic_after_ready: bool,
}
impl<F: Future> Future for DropAfterReady<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = this.inner.as_mut().poll(cx);
        this.ready = result.is_ready();
        result
    }
}
impl<F> Drop for DropAfterReady<F> {
    fn drop(&mut self) {
        if self.ready && self.panic_after_ready {
            std::panic::panic_any(CutFutureDropPayload);
        }
    }
}
struct CutFutureDropPayload;
impl Drop for CutFutureDropPayload {
    fn drop(&mut self) {
        panic!("original cut future secondary destructor");
    }
}
impl TunIo for DelayedCut {
    fn cut_ingress(&mut self) -> impl Future<Output = std::io::Result<DeviceCutReceipt>> {
        let panic_after_ready = self.panic_after_ready;
        let inner = Box::pin(async move {
            self.receipt = Some(self.inner.cut_ingress().await?);
            if let Some(at) = self.reject_at {
                tokio::time::sleep_until(at).await;
                return Err(std::io::Error::from_raw_os_error(13));
            }
            std::future::pending().await
        });
        DropAfterReady {
            inner,
            ready: false,
            panic_after_ready,
        }
    }
    fn request_stop(&mut self) {
        self.inner.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.inner.poll_shutdown(cx)
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        self.inner.wait_for_rx().await
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        self.inner.try_recv_rx()
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.inner.rx_peek()
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        self.inner.rx_take()
    }
    fn rx_take_packet(&mut self) -> Option<crate::device::RxPacket> {
        self.inner.rx_take_packet()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        self.inner.stage_rx_for_iface()
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        self.inner.flush_tx().await
    }
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        self.inner.inject_ip_packet(pkt);
    }
}

#[tokio::test(start_paused = true)]
async fn original_cut_deadline_retains_timeout_and_real_paused_device_until_final_join() {
    let saved = super::super::new_control(true);
    let owner = Arc::new(SessionControl::default());
    owner.request_stop();
    let original = owner.ledger().shutdown_deadline.unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut device = DelayedCut {
        inner: LoopbackTunDevice::new(PacketLink::bounded(4), PacketLink::bounded(4))
            .with_ingress_pump(1),
        receipt: None,
        reject_at: None,
        drops: drops.clone(),
        panic_after_ready: false,
    };
    assert!(
        cut_device(&mut device, &owner, &saved, Some(original))
            .await
            .is_none()
    );
    assert!(
        device.receipt.is_some(),
        "real original pump reached paused/drained receipt before withheld return"
    );
    assert_eq!(*saved.deadline.lock().unwrap(), Some(original));
    assert_eq!(
        owner
            .ledger()
            .first_failure
            .as_ref()
            .unwrap()
            .facts()
            .io_kind,
        Some(std::io::ErrorKind::TimedOut)
    );
    assert!(!owner.device_settlement_observed.load(Ordering::Acquire));
    assert_eq!(drops.load(Ordering::Acquire), 0);
    business::finish_device(device, None, owner.clone()).await;
    assert!(owner.device_settlement_observed.load(Ordering::Acquire));
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert!(!owner.ledger().quarantined);
}

#[tokio::test(start_paused = true)]
async fn delayed_known_cut_rejection_adopts_original_deadline_and_keeps_original_error() {
    let saved = super::super::new_control(true);
    let owner = Arc::new(SessionControl::default());
    owner.request_stop();
    let first = owner.ledger().shutdown_deadline.unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut device = DelayedCut {
        inner: LoopbackTunDevice::new(PacketLink::bounded(4), PacketLink::bounded(4))
            .with_ingress_pump(1),
        receipt: None,
        reject_at: Some(first - std::time::Duration::from_secs(1)),
        drops: drops.clone(),
        panic_after_ready: false,
    };
    assert!(
        cut_device(&mut device, &owner, &saved, Some(first))
            .await
            .is_none()
    );
    assert!(
        device.receipt.is_some(),
        "actual original pump is paused with its unique receipt"
    );
    assert_eq!(*saved.deadline.lock().unwrap(), Some(first));
    assert_eq!(saved.request_stop(), first);
    assert_eq!(owner.ledger().additional_failures, 0);
    assert_eq!(owner.ledger().shutdown_deadline, Some(first));
    assert_eq!(
        owner
            .ledger()
            .first_failure
            .as_ref()
            .unwrap()
            .facts()
            .raw_os_code,
        Some(13)
    );
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert!(!owner.device_settlement_observed.load(Ordering::Acquire));
    business::finish_device(device, None, owner.clone()).await;
    assert!(owner.device_settlement_observed.load(Ordering::Acquire));
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert!(!owner.ledger().quarantined);
}

#[tokio::test(start_paused = true)]
async fn original_cut_io_error_precedes_unknown_future_drop_and_secondary_q() {
    let saved = super::super::new_control(true);
    let owner = Arc::new(SessionControl::default());
    owner.request_stop();
    let first = owner.ledger().shutdown_deadline.unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut device = DelayedCut {
        inner: LoopbackTunDevice::new(PacketLink::bounded(4), PacketLink::bounded(4))
            .with_ingress_pump(1),
        receipt: None,
        reject_at: Some(tokio::time::Instant::now()),
        drops: drops.clone(),
        panic_after_ready: true,
    };
    // This timeout explicitly forces only the poisoned helper future's teardown.
    // It cannot certify clean service/device settlement or clear the original Q.
    let observed = tokio::time::timeout(
        std::time::Duration::from_millis(10),
        cut_device(&mut device, &owner, &saved, Some(first)),
    )
    .await;
    let facts = {
        let ledger = owner.ledger();
        (
            ledger.first_failure.as_ref().unwrap().facts().raw_os_code,
            ledger.additional_failures,
            ledger.unknown_panic_payloads,
            ledger.quarantined,
        )
    };
    let receipt_retained = device.receipt.is_some();
    let before_drop = drops.load(Ordering::Acquire);
    // Test-finally consumes the actual original pump join, without normalizing Q.
    device.inner.request_stop();
    let joined = std::future::poll_fn(|cx| device.inner.poll_shutdown(cx)).await;
    drop(device);
    assert!(observed.is_err());
    assert!(receipt_retained);
    assert_eq!(before_drop, 0);
    assert_eq!(
        facts.0,
        Some(13),
        "original returned I/O cause must precede unknown future destruction"
    );
    assert_eq!((facts.1, facts.2, facts.3), (2, 1, true));
    assert_eq!(joined.state(), crate::device::DeviceShutdownState::Settled);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert!(owner.ledger().quarantined);
    assert!(!owner.device_settlement_observed.load(Ordering::Acquire));
}

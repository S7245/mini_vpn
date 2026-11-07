use super::*;
mod egress;

#[tokio::test]
async fn cut_retires_original_pump_queues_and_resumes_same_actor() {
    let inbound = PacketLink::bounded(4);
    let outbound = PacketLink::bounded(4);
    let mut device = LoopbackTunDevice::new(inbound.clone(), outbound).with_ingress_pump(1);
    inbound.push(BytesMut::from(&b"old-queue"[..]));
    inbound.push(BytesMut::from(&b"old-producer"[..]));
    tokio::time::timeout(Duration::from_secs(1), async {
        while device.tun_ingress_pump_snapshot().full_waits == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut receipt = device
        .cut_ingress()
        .await
        .expect("actual paused/drained cut");
    assert!(receipt.discarded_packets >= 1);
    assert!(receipt.discarded_backing >= 9);
    assert_eq!(device.tun_ingress_pump_snapshot().closed, 0);
    inbound.push(BytesMut::from(&b"while-paused"[..]));
    device.resume_ingress(&mut receipt).unwrap();
    inbound.push(BytesMut::from(&b"new"[..]));
    tokio::time::timeout(Duration::from_secs(1), device.wait_for_rx())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(device.rx_take().unwrap().as_ref(), b"new");
    assert_eq!(device.tun_ingress_pump_snapshot().closed, 0);
    device.request_stop();
    std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
}

#[tokio::test]
async fn cut_zero_backing_messages_are_counted_and_old_io_stays_blocked() {
    let inbound = PacketLink::bounded(4);
    let mut device =
        LoopbackTunDevice::new(inbound.clone(), PacketLink::bounded(4)).with_ingress_pump(1);
    inbound.push(BytesMut::new());
    inbound.push(BytesMut::new());
    tokio::time::timeout(Duration::from_secs(1), async {
        while device.tun_ingress_pump_snapshot().full_waits == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut receipt = device.cut_ingress().await.unwrap();
    assert_eq!(receipt.discarded_packets, 2);
    assert_eq!(receipt.discarded_backing, 0);
    assert!(device.try_recv_rx().is_err());
    assert!(Device::transmit(&mut device, SmolInstant::now()).is_none());
    assert!(device.cut_ingress().await.is_err());
    device.resume_ingress(&mut receipt).unwrap();
    device.request_stop();
    std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
}

#[tokio::test]
async fn cut_wrong_receipt_is_retained_and_same_receipt_resumes_exactly_once() {
    let mut a = LoopbackTunDevice::new(PacketLink::bounded(4), PacketLink::bounded(4));
    let mut b = LoopbackTunDevice::new(PacketLink::bounded(4), PacketLink::bounded(4));
    let mut ar = a.cut_ingress().await.unwrap();
    let mut br = b.cut_ingress().await.unwrap();
    assert!(b.resume_ingress(&mut ar).is_err());
    assert!(b.cut.active());
    a.resume_ingress(&mut ar).unwrap();
    assert!(a.resume_ingress(&mut ar).is_err());
    b.resume_ingress(&mut br).unwrap();
    a.request_stop();
    std::future::poll_fn(|cx| a.poll_shutdown(cx)).await;
    b.request_stop();
    std::future::poll_fn(|cx| b.poll_shutdown(cx)).await;
}

#[tokio::test]
async fn cut_stopped_pump_cannot_consume_original_receipt() {
    let mut device =
        LoopbackTunDevice::new(PacketLink::bounded(4), PacketLink::bounded(4)).with_ingress_pump(1);
    let mut receipt = device.cut_ingress().await.unwrap();
    device.ingress_pump.as_ref().unwrap().request_stop();
    assert!(device.resume_ingress(&mut receipt).is_err());
    assert!(device.cut.active());
    device.request_stop();
    std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
}

#[tokio::test]
async fn cut_device_cancellation_retains_pending_sequence_and_original_actor() {
    use std::future::Future;
    let inbound = PacketLink::bounded(4);
    let mut device =
        LoopbackTunDevice::new(inbound.clone(), PacketLink::bounded(4)).with_ingress_pump(1);
    let actor = device
        .ingress_pump
        .as_ref()
        .unwrap()
        .original_actor_id()
        .unwrap();
    inbound.push(BytesMut::from(&b"old"[..]));
    tokio::time::timeout(Duration::from_secs(1), async {
        while device.tun_ingress_pump_snapshot().packets == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(device.try_recv_rx().unwrap());
    {
        let cut = device.cut_ingress();
        tokio::pin!(cut);
        std::future::poll_fn(|cx| {
            assert!(cut.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert!(device.cut.active());
    assert_eq!(device.cut.sequence_for_test(), 0);
    assert!(device.rx.has_current());
    assert!(device.try_recv_rx().is_err());
    let mut receipt = tokio::time::timeout(Duration::from_secs(1), device.cut_ingress())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(device.cut.sequence_for_test(), 1);
    assert!(!device.rx.has_current());
    assert_eq!(
        device.ingress_pump.as_ref().unwrap().original_actor_id(),
        Some(actor)
    );
    assert_eq!(receipt.discarded_packets, 1);
    device.resume_ingress(&mut receipt).unwrap();
    assert!(device.resume_ingress(&mut receipt).is_err());
    assert_eq!(device.cut.sequence_for_test(), 1);
    device.request_stop();
    std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
}

impl LoopbackTunDevice {
    pub(crate) fn test_rx_inventory_probe(&self) -> impl Fn() -> Option<bytes::AllocationIdentity> + Send + Sync + 'static + use<> {
        self.rx.access().unwrap().test_inventory_probe()
    }
}

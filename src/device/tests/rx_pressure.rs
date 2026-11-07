//! Real Device/pump ingress admission after the shared budget is already Full.
//! The peer queue keeps the actual packet until original admission succeeds.
use super::*;
use crate::harness::{LoopbackTunDevice, PacketLink};
use smoltcp::phy::{Device, RxToken};

#[tokio::test(flavor = "current_thread")]
async fn native_rx_unstaged_ack_progresses_at_general_backing_full() {
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let app = bytes::BackingMeter::with_budget(&budget);
    let data = bytes::BackingMeter::with_budget(&budget);
    let inbound = PacketLink::bounded(64);
    let mut device = LoopbackTunDevice::native_with_factory(64, 1500, &app, || {
        Ok((inbound.clone(), PacketLink::bounded(64)))
    }).unwrap();
    device.bind_backing_meter(DeviceBinding::Native { meter: &app, slots: 64, mtu: 1500 }).unwrap();
    let builder = etherparse::PacketBuilder::ipv4([10,0,0,1], [10,0,0,2], 64)
        .tcp(1000, 2000, 1, 65535).ack(42);
    let mut frame = Vec::with_capacity(builder.size(0));
    builder.write(&mut frame, &[]).unwrap();
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let held = BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &data).unwrap();
    assert_eq!(budget.used(), budget.limit());
    assert!(device.rx_peek().is_none(), "RX is not already staged before Full");
    inbound.push(BytesMut::from(frame.as_slice()));
    let read = tokio::time::timeout(Duration::from_secs(2), device.wait_for_rx()).await;
    let read_kind = read.as_ref().ok().and_then(|result| result.as_ref().err().map(std::io::Error::kind));
    let mut consumed = None;
    if matches!(read, Ok(Ok(()))) {
        assert!(device.stage_rx_for_iface());
        if let Some((rx, tx)) = Device::receive(&mut device, smoltcp::time::Instant::from_millis(0)) {
            consumed = Some(rx.consume(|bytes| bytes.to_vec()));
            drop(tx);
        }
    }
    eprintln!("[N1-RX-full] read_kind={read_kind:?} timeout={} consumed={} used={} Q={} pump={:?}",
        read.is_err(), consumed.is_some(), budget.used(), budget.quarantined(), device.tun_ingress_pump_snapshot());
    drop(held);
    {
        let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        let settled = receipt.settle().await;
        assert!(matches!(settled, DeviceShutdownState::Settled | DeviceShutdownState::FailedSettled));
    }
    drop(device);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert_eq!(consumed.as_deref(), Some(frame.as_slice()), "original unstaged RX ACK must progress without any general DATA refund");
}

#[tokio::test(flavor = "current_thread")]
async fn native_rx_busy_cancel_reuse_and_shutdown_wait_for_actual_typed_packet() {
    use std::{future::Future, task::{Context, Wake, Waker}};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Count(AtomicUsize);
    impl Wake for Count { fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); } }
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let inbound = PacketLink::bounded(2);
    let mut device = LoopbackTunDevice::native_with_factory(2, 1500, &meter, || {
        Ok((inbound.clone(), PacketLink::bounded(2)))
    }).unwrap();
    let cold = budget.used();
    eprintln!("[N1-RX-cold] slots2 mtu1500={cold}");
    device.bind_backing_meter(DeviceBinding::Native { meter: &meter, slots: 2, mtu: 1500 }).unwrap();
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let held = BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    inbound.push(BytesMut::from(&b"first"[..]));
    device.wait_for_rx().await.unwrap();
    let pointer = device.rx_peek().unwrap().as_ptr();
    assert!(device.rx_take().is_none(), "raw escape cannot strip a protected loan");
    assert_eq!(device.rx_peek().unwrap().as_ptr(), pointer);
    let first = device.rx_take_packet().unwrap();
    // A whole legal IPv4 packet remains accepted even though the configured MTU is smaller.
    let builder = etherparse::PacketBuilder::ipv4([10,0,0,1], [10,0,0,2], 64).udp(1, 2);
    let payload = vec![0x5a; 65507];
    let mut frame = Vec::with_capacity(builder.size(payload.len()));
    builder.write(&mut frame, &payload).unwrap();
    assert_eq!(frame.len(), 65535);
    inbound.push(BytesMut::from(frame.as_slice()));
    for _ in 0..4 { tokio::task::yield_now().await; }
    {
        let mut read = Box::pin(device.wait_for_rx());
        assert!(read.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    }
    assert!(!device.try_recv_rx().unwrap());
    assert_eq!(device.tun_ingress_pump_snapshot().read_errors, 0);
    assert!(!budget.quarantined());
    let charged = budget.used();
    drop(first);
    assert_eq!(budget.used(), charged, "return is availability only");
    tokio::time::timeout(Duration::from_secs(2), device.wait_for_rx()).await.unwrap().unwrap();
    let second = device.rx_take_packet().unwrap();
    assert_eq!(second.as_ptr(), pointer);
    assert_eq!(&second[..], frame.as_slice());
    let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
    let wake = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut settle = Box::pin(receipt.settle());
    assert!(settle.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    let charged = budget.used();
    drop(second);
    assert_eq!(budget.used(), charged, "last packet Drop does not retire the owner inventory");
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);
    assert_eq!(tokio::time::timeout(Duration::from_secs(2), settle).await.unwrap(), DeviceShutdownState::Settled);
    drop((receipt, device, held));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn rx_retired_owner_refuses_dynamic_and_protected_admission() {
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (tx, rx) = super::super::callback::Custody::pair(&meter).unwrap();
    let stock = super::super::rx_packet::Stock::new(&meter, rx).unwrap();
    let access = stock.access();
    drop(stock);
    assert!(access.acquire(40).is_err());
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let fill = BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    assert!(access.acquire(40).is_err());
    drop((fill, access, tx));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn direct_protected_read_keeps_mtu_length_pi_and_preparation_on_cancel() {
    use std::{future::Future, pin::Pin, task::{Context, Poll, Waker}};
    use tokio::io::{AsyncRead, AsyncWriteExt, ReadBuf};
    struct Reader { inner: tokio::io::DuplexStream, largest: usize }
    impl AsyncRead for Reader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            self.largest = self.largest.max(buffer.remaining());
            Pin::new(&mut self.inner).poll_read(cx, buffer)
        }
    }
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (tx, rx) = super::super::callback::Custody::pair(&meter).unwrap();
    let mut stock = super::super::rx::RxStock::native(1, &meter).unwrap();
    stock.protect(rx).unwrap();
    let (mut peer, inner) = tokio::io::duplex(2048);
    let mut reader = Reader { inner, largest: 0 };
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let held = BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &meter).unwrap();
    {
        let mut read = Box::pin(stock.read(&mut reader, 1204));
        assert!(read.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    }
    let pointer = stock.prepare(1204).unwrap().as_ptr();
    assert_eq!(budget.used(), budget.limit());
    peer.write_all(&[0, 0, 0, 2, b'o', b'k']).await.unwrap();
    let n = stock.read(&mut reader, 1204).await.unwrap();
    assert_eq!(reader.largest, 1204, "Direct retains MTU+PI read request, not protection capacity");
    stock.finish_read(n, 4).unwrap();
    let packet = stock.take_packet().unwrap();
    assert_eq!(&packet[..], b"ok");
    assert_eq!(packet.as_ptr(), unsafe { pointer.add(4) });
    assert!(stock.take().is_none());
    drop(packet);
    assert_eq!(budget.used(), budget.limit());
    let mut tail = super::super::callback::Tail::with_stock(tx, stock.take_stock());
    drop(tail.settle().await);
    drop((stock, tail, held));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn direct_original_io_enters_rx_core_before_return_and_terminal_claim() {
    struct Eio;
    impl tokio::io::AsyncRead for Eio {
        fn poll_read(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>, _: &mut tokio::io::ReadBuf<'_>) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::from_raw_os_error(5)))
        }
    }
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (tx, rx) = super::super::callback::Custody::pair(&meter).unwrap();
    let mut stock = super::super::rx::RxStock::native(1, &meter).unwrap();
    stock.protect(rx).unwrap();
    let error = stock.read(&mut Eio, 1500).await.unwrap_err();
    let original = crate::device::egress::callback_original_io(&error);
    assert!(original.is_some(), "actual Direct backend IO must reach original RX custody before return");
    assert_eq!(original.as_ref().unwrap().raw_os_error(), Some(5));
    let origin = super::super::PumpControl::origin(crate::shared::errors::FailurePhase::DeviceRead);
    let report = crate::device::egress::callback_report(&error, origin.clone()).unwrap();
    assert_eq!(report.failure.as_ref().unwrap().facts().raw_os_code, Some(5));
    stock.discard(&mut (0, 0));
    let mut tail = super::super::callback::Tail::with_stock(tx, stock.take_stock());
    assert_eq!(tail.settle().await.report(origin).additional, 0);
    drop((error, original, report, tail, stock));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

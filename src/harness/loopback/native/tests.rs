use super::*;
use crate::device::pump_native::Prepared;
use bytes::{BackingBudget, BackingMeter};

#[tokio::test]
async fn full_native_copy_keeps_actual_peer_front_and_first_io() {
    let layout = quinn::owned_events::slot_layout::<PumpPacket>(1)
        .unwrap()
        .size();
    let budget = BackingBudget::new(layout + Prepared::test_control_bytes()).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let link = PacketLink::bounded(2);
    assert!(link.push(BytesMut::from(&b"unchanged"[..])));
    let pointer = link.queue.lock().unwrap().front().unwrap().as_ptr();
    let mut pump = Prepared::new(1, &meter)
        .unwrap()
        .spawn(PacketSource::new(link.clone()));
    let error = tokio::time::timeout(Duration::from_secs(1), pump.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(
        link.queue.lock().unwrap().front().unwrap().as_ptr(),
        pointer
    );
    assert_eq!(link.queue.lock().unwrap().len(), 1);
    assert_eq!(pump.snapshot().packets, 0);
    let result = std::future::poll_fn(|cx| pump.poll_shutdown(cx)).await;
    assert_eq!(result.io_kind(), Some(std::io::ErrorKind::WouldBlock));
    drop(pump);
    assert_eq!(budget.used(), Prepared::test_control_bytes(), "returned original error still owns its Control");
    drop(error);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn actual_peer_wait_cut_resume_and_zero_packet_keep_native_backing() {
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let link = PacketLink::bounded(4);
    let mut pump = Prepared::new(1, &meter)
        .unwrap()
        .spawn(PacketSource::new(link.clone()));
    // Let the original source register its real empty-peer waiter.
    tokio::task::yield_now().await;
    assert!(link.push(BytesMut::new()));
    let packet = tokio::time::timeout(Duration::from_secs(1), pump.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(packet.is_empty());
    assert!(packet.precharged_backing_matches(&meter));
    drop(packet);
    let cut = tokio::time::timeout(Duration::from_secs(1), pump.cut())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cut, (0, 0));
    assert!(link.push(BytesMut::from(&b"new"[..])));
    pump.validate_resume().unwrap();
    pump.resume();
    let packet = tokio::time::timeout(Duration::from_secs(1), pump.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(&packet[..], b"new");
    assert!(packet.precharged_backing_matches(&meter));
    let alias = packet.into_raw().unwrap().freeze();
    assert_eq!(
        std::future::poll_fn(|cx| pump.poll_shutdown(cx))
            .await
            .state(),
        crate::device::DeviceShutdownState::Settled
    );
    drop(pump);
    assert_eq!(
        budget.used(),
        BytesMut::metered_copy_layout_for_len(3).unwrap()
    );
    drop(alias);
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn repeated_native_bind_keeps_original_actor_and_legacy_cannot_upgrade() {
    use crate::device::DeviceBinding;
    let budget = BackingBudget::new(65536 + crate::device::RxPacket::test_protection_cold_bytes()).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut legacy = LoopbackTunDevice::new(PacketLink::bounded(1), PacketLink::bounded(1));
    assert!(
        legacy
            .bind_backing_meter(DeviceBinding::Native {
                meter: &meter,
                slots: 1,
                mtu: 1500
            })
            .is_err()
    );
    let mut device = LoopbackTunDevice::native_with_factory(1, 1200, &meter, || {
        Ok((PacketLink::bounded(1), PacketLink::bounded(1)))
    })
    .unwrap();
    let binding = DeviceBinding::Native {
        meter: &meter,
        slots: 1,
        mtu: 1200,
    };
    device.bind_backing_meter(binding).unwrap();
    let actor = device.ingress_pump.as_ref().unwrap().original_actor_id();
    assert!(actor.is_some());
    device.bind_backing_meter(binding).unwrap();
    assert_eq!(
        device.ingress_pump.as_ref().unwrap().original_actor_id(),
        actor
    );
    assert!(device.bind_backing_meter(DeviceBinding::Legacy).is_err());
    let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
    assert_eq!(
        receipt.settle().await,
        crate::device::DeviceShutdownState::Settled
    );
    drop(receipt);
    drop(device);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

//! Actual pre-source bill and legal packet sizes; one protected TX and one protected RX packet.
use super::*;
use crate::harness::{LoopbackTunDevice, PacketLink};

#[tokio::test(flavor = "current_thread")]
async fn actual_native_device_cold_cost_and_full_ipv4_packet_remain_distinct_from_mtu() {
    let budget = bytes::BackingBudget::new(2 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let inbound = PacketLink::bounded(64);
    let outbound = PacketLink::bounded(64);
    let before = budget.used();
    let mut factory_bill = None;
    let mut device = LoopbackTunDevice::native_with_factory(64, 1500, &meter, || {
        factory_bill = Some((budget.used() - before, meter.snapshot()));
        Ok((inbound.clone(), outbound.clone()))
    })
    .unwrap();
    let (cold_bill, cold_backing) = factory_bill.unwrap();
    assert_eq!(
        cold_backing.allocations, 2,
        "pumped preflight releases its probe but retains the protected TX and RX packets"
    );
    eprintln!(
        "[N1-device-capacity] slots=64 mtu=1500 prefix=0 before_source_bill={cold_bill} before_source_registered={cold_backing:?} persistent_tx_and_rx_packet_credit=true fixed_device_value={} fixed_pump_value={} fixed_packet_value={} private_runtime_arc_headers_separately_instance_bounded=true",
        std::mem::size_of::<LoopbackTunDevice>(),
        std::mem::size_of::<TunIngressPump>(),
        std::mem::size_of::<PumpPacket>()
    );
    device
        .bind_backing_meter(DeviceBinding::Native {
            meter: &meter,
            slots: 64,
            mtu: 1500,
        })
        .unwrap();
    let builder = etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64).udp(1000, 2000);
    let payload = vec![0x42; u16::MAX as usize - 28];
    let mut frame = Vec::with_capacity(builder.size(payload.len()));
    builder.write(&mut frame, &payload).unwrap();
    assert_eq!(frame.len(), u16::MAX as usize);
    assert!(etherparse::SlicedPacket::from_ip(&frame).is_ok());
    inbound.push(BytesMut::from(frame.as_slice()));
    tokio::time::timeout(Duration::from_secs(2), device.wait_for_rx())
        .await
        .unwrap()
        .unwrap();
    let received = device.rx_take().unwrap();
    assert_eq!(received.as_ref(), frame.as_slice());
    assert!(received.precharged_backing_matches(&meter));
    eprintln!(
        "[N1-device-capacity] after_actual_max_rx used={} registered={:?} legal_ipv4_length={} mtu_is_not_max_injection_length=true",
        budget.used(),
        meter.snapshot(),
        received.len()
    );
    device
        .inject_ip_packet_with(frame.len(), |out| {
            out.copy_from_slice(&frame);
            Ok(frame.len())
        })
        .unwrap();
    eprintln!(
        "[N1-device-capacity] with_actual_rx_and_tx used={} registered={:?}",
        budget.used(),
        meter.snapshot()
    );
    device.flush_tx().await.unwrap();
    assert_eq!(outbound.test_pop().unwrap().as_ref(), frame.as_slice());
    drop(received);
    {
        let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        assert_eq!(receipt.settle().await, DeviceShutdownState::Settled);
    }
    drop(device);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    // Prospective costs are explicit geometry, not already allocated credit.
    // Existing lifecycle: pump C+1, selected1, Rx current/prepared1, staged C,
    // one synchronous consume and one escaped handler = 2*C+5 positions.
    let positions = 2 * 64 + 5;
    eprintln!(
        "[N1-device-capacity] prospective_positions={positions} mtu_only_payload={} max_legal_payload={} not_an_accepted_pool_count=true",
        positions * 1500,
        positions * (u16::MAX as usize)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn native_control_shaped_ingress_with_free_slots_progresses_at_general_backing_full() {
    use smoltcp::phy::{Device, RxToken};
    use smoltcp::time::Instant as SmolInstant;
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let app = bytes::BackingMeter::with_budget(&budget);
    let data = bytes::BackingMeter::with_budget(&budget);
    let inbound = PacketLink::bounded(64);
    let mut device = LoopbackTunDevice::native_with_factory(64, 1500, &app, || {
        Ok((inbound.clone(), PacketLink::bounded(64)))
    })
    .unwrap();
    device
        .bind_backing_meter(DeviceBinding::Native {
            meter: &app,
            slots: 64,
            mtu: 1500,
        })
        .unwrap();
    let builder = etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64)
        .tcp(1000, 2000, 1, 65535)
        .ack(42);
    let mut frame = Vec::with_capacity(builder.size(0));
    builder.write(&mut frame, &[]).unwrap();
    inbound.push(BytesMut::from(frame.as_slice()));
    tokio::time::timeout(Duration::from_secs(2), device.wait_for_rx())
        .await
        .unwrap()
        .unwrap();
    assert!(device.stage_rx_for_iface());
    // A deterministic component pressure injection using a real backing, not a
    // reservation marker. This is not the two-Flow QUIC/ACK acceptance workload.
    let remaining = budget.limit() - budget.used();
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let held_data = BytesMut::try_zeroed_metered(remaining - header, &data).unwrap();
    assert_eq!(budget.used(), budget.limit());
    assert_eq!(device.queued_tx_bytes(), 0, "all logical TX slots are free");
    let full_at_admission = budget.used() == budget.limit();
    let consumed = match Device::receive(&mut device, SmolInstant::from_millis(0)) {
        Some((rx, tx)) => {
            let bytes = rx.consume(|bytes| bytes.to_vec());
            drop(tx);
            Some(bytes)
        }
        None => None,
    };
    eprintln!(
        "[N1-device-control-full] actual_general_budget_full={} consumed={} tx_queued={} app={:?} data={:?}",
        full_at_admission,
        consumed.is_some(),
        device.queued_tx_bytes(),
        app.snapshot(),
        data.snapshot()
    );
    // Clean the actual actor and all held storage before asserting the RED; a
    // deliberate assertion must not create an unrelated owner-drop Q verdict.
    drop(held_data);
    if consumed.is_none() {
        let after_release = match Device::receive(&mut device, SmolInstant::from_millis(1)) {
            Some((rx, tx)) => {
                let bytes = rx.consume(|bytes| bytes.to_vec());
                drop(tx);
                Some(bytes)
            }
            None => None,
        };
        eprintln!(
            "[N1-device-control-full] same_staged_frame_after_actual_data_drop={}",
            after_release.as_deref() == Some(frame.as_slice())
        );
        assert_eq!(
            after_release.as_deref(),
            Some(frame.as_slice()),
            "actual backing refund must release this pre-existing ingress bottleneck"
        );
    }
    {
        let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        assert_eq!(receipt.settle().await, DeviceShutdownState::Settled);
    }
    drop(device);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert_eq!(
        consumed.as_deref(),
        Some(frame.as_slice()),
        "free TX slots must have real control backing before exposing ingress to smoltcp"
    );
}

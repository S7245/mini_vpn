//! Real Native Device assembly, PacketLink input/output and original pump JOIN.
//! No OS device, full Service, Main capture or global queue acceptance claim.
//! Keep the prior 64KiB fixture allowance plus the approved RX actual cold bill
//! (65539B Shared allocation + its exact State/Core). Exact one-byte-short
//! construction pressure below remains measured, not enlarged to pass.
use super::*;
use crate::harness::{LoopbackTunDevice, PacketLink};
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn cold_factory_without_runtime_rejects_before_source_creation() {
    let budget = bytes::BackingBudget::new(65536 + super::super::rx_packet::cold_bytes()).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let calls = AtomicUsize::new(0);
    let result = LoopbackTunDevice::native_with_factory(1, 1200, &meter, || {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok((PacketLink::bounded(1), PacketLink::bounded(1)))
    });
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn cold_factory_full_precedes_source_and_actual_chain_refunds_only_last_alias() {
    let probe_budget = bytes::BackingBudget::new(65536 + super::super::rx_packet::cold_bytes()).unwrap();
    let probe_meter = bytes::BackingMeter::with_budget(&probe_budget);
    let cold = super::super::cold::PreparedDevice::new(1, 1200, 0, true, &probe_meter).unwrap();
    // Independently hold the actual one-copy allocation after all cold owners.
    let copy = BytesMut::try_zeroed_metered(1200, &probe_meter).unwrap();
    let required = probe_budget.used();
    drop(copy);
    drop(cold);
    assert_eq!(probe_budget.used(), 0);
    let small = bytes::BackingBudget::new(required - 1).unwrap();
    let small_meter = bytes::BackingMeter::with_budget(&small);
    let calls = AtomicUsize::new(0);
    let refused = LoopbackTunDevice::native_with_factory(1, 1200, &small_meter, || {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok((PacketLink::bounded(1), PacketLink::bounded(1)))
    });
    assert!(matches!(refused, Err(ref e) if e.kind() == ErrorKind::WouldBlock));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(small.used(), 0);
    assert!(!small.quarantined());

    let budget = bytes::BackingBudget::new(required).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let inbound = PacketLink::bounded(1);
    let outbound = PacketLink::bounded(1);
    let mut device = LoopbackTunDevice::native_with_factory(1, 1200, &meter, || {
        calls.fetch_add(1, Ordering::SeqCst);
        Ok((inbound.clone(), outbound.clone()))
    })
    .unwrap();
    device
        .bind_backing_meter(DeviceBinding::Native {
            meter: &meter,
            slots: 1,
            mtu: 1200,
        })
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    inbound.push(BytesMut::from(&b"real native input"[..]));
    tokio::time::timeout(std::time::Duration::from_secs(2), device.wait_for_rx())
        .await
        .unwrap()
        .unwrap();
    let escaped = device.rx_take().unwrap().freeze();
    assert_eq!(escaped.as_ref(), b"real native input");
    let alias = escaped.clone();
    device
        .inject_ip_packet_with(3, |out| {
            out.copy_from_slice(b"out");
            Ok(3)
        })
        .unwrap();
    device.flush_tx().await.unwrap();
    assert_eq!(outbound.test_pop().unwrap().as_ref(), b"out");
    {
        let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        assert_eq!(receipt.settle().await, DeviceShutdownState::Settled);
    }
    assert!(!device.tun_ingress_pump_snapshot().enabled);
    drop(device);
    assert!(
        budget.used() > 0,
        "actual escaped backing still belongs to original budget"
    );
    let remaining = budget.used();
    drop(escaped);
    assert_eq!(budget.used(), remaining);
    drop(alias);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn cold_factory_rejects_retired_runtime_before_start_commit() {
    let budget = bytes::BackingBudget::new(65536 + super::super::rx_packet::cold_bytes()).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let inbound = PacketLink::bounded(1);
    inbound.push(BytesMut::from(&b"not consumed"[..]));
    let runtime_a = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut device = runtime_a.block_on(async {
        LoopbackTunDevice::native_with_factory(1, 1200, &meter, || {
            Ok((inbound.clone(), PacketLink::bounded(1)))
        })
        .unwrap()
    });
    drop(runtime_a);
    let runtime_b = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime_b.block_on(async {
        let result = device.bind_backing_meter(DeviceBinding::Native {
            meter: &meter,
            slots: 1,
            mtu: 1200,
        });
        // Clean up before asserting, so the RED does not add an unrelated Drop Q.
        let started = device.tun_ingress_pump_snapshot().enabled;
        let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        receipt.settle().await;
        assert!(
            matches!(result, Err(ref e) if e.kind() == ErrorKind::InvalidInput),
            "retired runtime accepted: {result:?}, started={started}"
        );
        assert!(!started);
    });
    assert_eq!(inbound.test_pop().unwrap().as_ref(), b"not consumed");
    drop(device);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn cold_factory_freezes_entries_and_rejects_mixed_authority_without_starting() {
    let budget = bytes::BackingBudget::new(65536 + super::super::rx_packet::cold_bytes()).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let other = bytes::BackingMeter::with_budget(&budget);
    let inbound = PacketLink::bounded(1);
    inbound.push(BytesMut::from(&b"unpublished"[..]));
    let mut device = LoopbackTunDevice::native_with_factory(1, 1200, &meter, || {
        Ok((inbound.clone(), PacketLink::bounded(1)))
    })
    .unwrap();
    assert!(!device.tun_ingress_pump_snapshot().enabled);
    assert!(device.try_recv_rx().is_err());
    assert!(device.wait_for_rx().await.is_err());
    assert!(device.rx_take().is_none());
    assert!(!device.can_stage_single_iface_rx());
    assert!(
        device
            .transmit(smoltcp::time::Instant::from_millis(0))
            .is_none()
    );
    let encode = AtomicUsize::new(0);
    assert!(
        device
            .inject_ip_packet_with(1, |_| {
                encode.fetch_add(1, Ordering::SeqCst);
                Ok(1)
            })
            .is_err()
    );
    assert_eq!(encode.load(Ordering::SeqCst), 0);
    assert!(device.cut_ingress().await.is_err());
    for binding in [
        DeviceBinding::Legacy,
        DeviceBinding::Native {
            meter: &other,
            slots: 1,
            mtu: 1200,
        },
        DeviceBinding::Native {
            meter: &meter,
            slots: 2,
            mtu: 1200,
        },
        DeviceBinding::Native {
            meter: &meter,
            slots: 1,
            mtu: 1500,
        },
    ] {
        assert!(device.bind_backing_meter(binding).is_err());
        assert!(!device.tun_ingress_pump_snapshot().enabled);
        assert!(!budget.quarantined());
    }
    let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
    assert_eq!(receipt.settle().await, DeviceShutdownState::Settled);
    drop(receipt);
    assert_eq!(inbound.test_pop().unwrap().as_ref(), b"unpublished");
    drop(device);
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn cold_factory_known_source_refusal_and_pending_drop_release_real_inventory() {
    let budget = bytes::BackingBudget::new(65536 + super::super::rx_packet::cold_bytes()).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let result = LoopbackTunDevice::native_with_factory(1, 1200, &meter, || {
        assert!(budget.used() > 0);
        Err(std::io::Error::from_raw_os_error(17))
    });
    assert_eq!(result.err().unwrap().raw_os_error(), Some(17));
    assert_eq!(budget.used(), 0);
    let device = LoopbackTunDevice::native_with_factory(1, 1200, &meter, || {
        Ok((PacketLink::bounded(1), PacketLink::bounded(1)))
    })
    .unwrap();
    assert!(budget.used() > 0);
    drop(device);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn virtual_cold_inventory_precedes_os_factory_and_known_refusal_is_clean() {
    for pumped in [false, true] {
        let budget = bytes::BackingBudget::new(65536 + super::super::rx_packet::cold_bytes()).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let calls = AtomicUsize::new(0);
        let result = VirtualTunDevice::native_with_factory(1, 1200, pumped, &meter, || {
            calls.fetch_add(1, Ordering::SeqCst);
            assert!(budget.used() > 0);
            Err(std::io::Error::from_raw_os_error(17))
        });
        assert_eq!(result.err().unwrap().raw_os_error(), Some(17));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
        let tiny = bytes::BackingBudget::new(1).unwrap();
        let result = VirtualTunDevice::native_with_factory(
            1,
            1200,
            pumped,
            &bytes::BackingMeter::with_budget(&tiny),
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(std::io::Error::from_raw_os_error(17))
            },
        );
        assert_eq!(result.err().unwrap().kind(), ErrorKind::WouldBlock);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(tiny.used(), 0);
        assert!(!tiny.quarantined());
    }
}

struct UnstartedSource {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    panic: Option<Arc<AtomicUsize>>,
}
impl super::super::pump_native::Source for UnstartedSource {
    fn poll_packet(
        self: Pin<&mut Self>,
        _: &mut PumpPacket,
        _: &mut Context<'_>,
    ) -> Poll<std::io::Result<SourceRead>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}
impl Drop for UnstartedSource {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(payload) = self.panic.take() {
            std::panic::panic_any(payload);
        }
    }
}

#[tokio::test]
async fn unstarted_source_cancellation_retains_original_drop_failure_and_nonzero_q() {
    use super::super::pump_native::{Prepared, Startup};
    let budget = bytes::BackingBudget::new(65536 + super::super::rx_packet::cold_bytes()).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let original = Arc::new(AtomicUsize::new(19));
    let mut pending = Startup::new(
        Prepared::new(1, &meter).unwrap(),
        UnstartedSource {
            polls: polls.clone(),
            drops: drops.clone(),
            panic: Some(original.clone()),
        },
        tokio::runtime::Handle::current(),
    );
    pending.validate(&meter, 1).unwrap();
    let mut receipt = std::future::poll_fn(|cx| pending.poll_shutdown(cx, None)).await;
    assert_eq!(receipt.state(), DeviceShutdownState::FailedSettled);
    assert_eq!(
        receipt.take_failure().unwrap().facts().class,
        crate::shared::errors::FailureClass::Panic
    );
    assert_eq!(receipt.take_additional_failures(), 0);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        Arc::strong_count(&original),
        2,
        "original opaque Box remains retained"
    );
    assert!(budget.quarantined());
    drop(pending);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn cold_startup_unclaimed_notification_drop_keeps_original_failure() {
    let budget = bytes::BackingBudget::new(65536 + super::super::rx_packet::cold_bytes()).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let inbound = PacketLink::bounded(1);
    inbound.push(BytesMut::zeroed(65540));
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
    let notification = observer.wait().await.unwrap();
    drop(notification); // Caller cancelled before the original owner accepted it.
    let retained = observer.claim().is_some();
    drop(observer);
    let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
    receipt.settle().await;
    drop(receipt);
    drop(device);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(
        retained,
        "notification disposal stole the original first failure before acceptance"
    );
}

/// O1 (A2.3): the Service preflight's Device term is exact, not an estimate.
/// For each mode the composed `required_bytes` admits at exactly that budget,
/// refuses one byte below with typed pressure and no residue, and the retained
/// cold equals it minus the released pumped-copy headroom.
#[tokio::test]
async fn prepared_device_required_bytes_is_the_exact_construction_boundary() {
    use super::super::cold::PreparedDevice;
    for (slots, mtu, prefix, pumped) in [(1, 1200, 0, true), (16, 1500, 4, true), (4, 9000, 0, false), (1, 576, 4, false)] {
        let required = PreparedDevice::required_bytes(slots, mtu, prefix, pumped).unwrap();
        let copy = BytesMut::metered_copy_layout_for_len(mtu + prefix).unwrap();
        let short = bytes::BackingBudget::new(required - 1).unwrap();
        let refused = PreparedDevice::new(slots, mtu, prefix, pumped, &bytes::BackingMeter::with_budget(&short));
        assert!(matches!(refused, Err(ref e) if e.kind() == ErrorKind::WouldBlock), "{slots}/{mtu}/{prefix}/{pumped}");
        assert_eq!((short.used(), short.quarantined()), (0, false));
        let exact = bytes::BackingBudget::new(required).unwrap();
        let cold = PreparedDevice::new(slots, mtu, prefix, pumped, &bytes::BackingMeter::with_budget(&exact)).unwrap();
        assert_eq!(exact.used(), if pumped { required - copy } else { required }, "{slots}/{mtu}/{prefix}/{pumped}");
        drop(cold);
        assert_eq!((exact.used(), exact.quarantined()), (0, false));
    }
}

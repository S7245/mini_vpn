//! Freeze the original Legacy token/flush/summary behavior before owner movement.
use super::*;

fn packet(payload: &[u8], sequence: u32) -> Vec<u8> {
    let builder = etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [10, 0, 0, 2], 64)
        .tcp(1000, 2000, sequence, 65535);
    let mut bytes = Vec::with_capacity(builder.size(payload.len()));
    builder.write(&mut bytes, payload).unwrap();
    bytes
}
#[tokio::test]
async fn legacy_token_injection_fifo_and_summary_characterization() {
    for capacity in [1, 4] {
        let inbound = PacketLink::bounded(4);
        let outbound = PacketLink::bounded(capacity);
        let mut device = LoopbackTunDevice::new(inbound, outbound.clone());
        let first = packet(b"first", 10);
        let second = packet(b"second", 15);
        Device::transmit(&mut device, SmolInstant::from_millis(0))
            .unwrap()
            .consume(first.len(), |out| out.copy_from_slice(&first));
        device.inject_ip_packet(&second);
        assert_eq!(device.queued_tx_bytes(), first.len() + second.len());
        device.flush_tx().await.unwrap();
        assert_eq!(device.queued_tx_bytes(), 0);
        assert_eq!(&outbound.pop().unwrap()[..], first);
        if capacity == 1 {
            // Existing Legacy semantics are deliberately characterized, not
            // certified as Native selected-write/backpressure acceptance.
            assert!(outbound.pop().is_none());
            assert_eq!(outbound.dropped_packets(), 1);
        } else {
            assert_eq!(&outbound.pop().unwrap()[..], second);
            assert_eq!(outbound.dropped_packets(), 0);
        }
        let mut summaries = device.take_flushed_tcp_packets();
        let a = summaries.next().unwrap();
        let b = summaries.next().unwrap();
        assert_eq!((a.payload_start_seq, a.payload_bytes), (10, 5));
        assert_eq!((b.payload_start_seq, b.payload_bytes), (15, 6));
        assert!(summaries.next().is_none());
        summaries.commit();
        assert_eq!(
            std::future::poll_fn(|cx| device.poll_shutdown(cx))
                .await
                .state(),
            crate::device::DeviceShutdownState::Settled
        );
    }
}

// Egress-only seam; full Native Device factory is intentionally tested separately.
#[tokio::test]
async fn native_full_peer_preserves_selected_until_actual_space() {
    use bytes::{BackingBudget, BackingMeter};
    let budget = BackingBudget::new(65536).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let outbound = PacketLink::bounded(1);
    assert!(outbound.push(BytesMut::from(&b"occupied"[..])));
    let mut device = LoopbackTunDevice::new(PacketLink::bounded(1), outbound.clone());
    device.egress = crate::device::egress::Egress::native(2, 1500, 0, &meter).unwrap();
    let bytes = packet(b"native", 19);
    device
        .egress
        .token(1500, 0)
        .unwrap()
        .consume(bytes.len(), |out| out.copy_from_slice(&bytes));
    let pointer = device.egress.queue.front().unwrap().as_ptr();
    let charged = budget.used();
    let pending = {
        let flush = device.flush_tx();
        tokio::pin!(flush);
        std::future::poll_fn(|cx| Poll::Ready(flush.as_mut().poll(cx).is_pending())).await
    };
    // Dispose through the actual Device even when reproducing the old bad path.
    let retained = device.egress.owned_bytes();
    let no_summary = device.egress.summaries.is_empty();
    let unchanged_charge = budget.used() == charged;
    let selected_pointer = device.egress.queue.front().map(|packet| packet.as_ptr());
    assert_eq!(&outbound.pop().unwrap()[..], b"occupied");
    if pending {
        device.flush_tx().await.unwrap();
    }
    let delivered = outbound.pop();
    let summary = {
        let mut batch = device.take_flushed_tcp_packets();
        let summary = batch.next();
        batch.commit();
        summary
    };
    {
        let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        receipt.settle().await;
    }
    drop(device);
    assert!(pending, "full real peer must suspend the selected write");
    assert_eq!(retained, bytes.len());
    assert!(no_summary && unchanged_charge);
    // Before selection or after selection, it is the original backing, never a retry copy.
    if let Some(actual) = selected_pointer {
        assert_eq!(actual, pointer);
    }
    assert_eq!(delivered.unwrap().as_ref(), bytes.as_slice());
    assert_eq!(summary.unwrap().payload_start_seq, 19);
    assert_eq!(outbound.dropped_packets(), 0);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn native_typed_injection_and_token_use_same_admitted_slots() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let outbound = PacketLink::bounded(1);
    let mut device = LoopbackTunDevice::new(PacketLink::bounded(1), outbound.clone());
    device.egress = crate::device::egress::Egress::native(1, 1500, 0, &meter).unwrap();
    let bytes = packet(b"typed", 50);
    device
        .inject_ip_packet_with(bytes.len(), |out| {
            out.copy_from_slice(&bytes);
            Ok(bytes.len())
        })
        .unwrap();
    let denied = Device::transmit(&mut device, SmolInstant::ZERO).is_none();
    let typed = device
        .egress
        .queue
        .front()
        .unwrap()
        .precharged_backing_matches(&meter);
    device.egress.discard(&mut (0, 0));
    {
        let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        receipt.settle().await;
    }
    drop(device);
    assert!(
        denied && typed,
        "actual typed injection and Device token must share Native inventory"
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn native_partial_probe_preserves_first_summary_and_selected_tail() {
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let outbound = PacketLink::bounded(4);
    let (mut device, probe) =
        LoopbackTunDevice::new(PacketLink::bounded(1), outbound.clone()).with_partial_flush_probe();
    device.egress = crate::device::egress::Egress::native(2, 1500, 0, &meter).unwrap();
    let first = packet(b"first", 10);
    let second = packet(b"tail", 15);
    for bytes in [&first, &second] {
        device
            .inject_ip_packet_with(bytes.len(), |out| {
                out.copy_from_slice(bytes);
                Ok(bytes.len())
            })
            .unwrap();
    }
    probe.arm();
    let error = device.flush_tx().await.unwrap_err();
    assert_eq!(
        crate::device::egress::callback_original_io(&error)
            .unwrap()
            .raw_os_error(),
        Some(5)
    );
    drop(error);
    assert_eq!(outbound.pop().unwrap().as_ref(), first);
    assert!(outbound.pop().is_none());
    assert_eq!(device.queued_tx_bytes(), second.len());
    let mut summaries = device.take_flushed_tcp_packets();
    let first_summary = summaries.next().unwrap();
    assert_eq!(first_summary.payload_start_seq, 10);
    assert!(summaries.next().is_none());
    summaries.commit();
    assert_eq!(probe.witness(), Some((first_summary, 1)));
    assert_eq!(probe.native_cut_index(), Some(0));
    assert!(device.flush_tx().await.is_err());
    assert!(outbound.pop().is_none());
    {
        let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        receipt.settle().await;
    }
    drop(device);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined()); // Known pre-write EIO; Main unknown handling is a separate owner.
}

impl LoopbackTunDevice {
    pub(crate) fn test_native_egress_only_shell() -> (bytes::BackingBudget, Self) {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut device = Self::new(PacketLink::bounded(4), PacketLink::bounded(4));
        device.egress = crate::device::egress::Egress::native(2, 1500, 0, &meter).unwrap();
        (budget, device)
    }
}

impl LoopbackTunDevice {
    pub(crate) fn test_outbound_link(&self) -> PacketLink {
        self.outbound.as_ref().unwrap().clone()
    }
}

impl LoopbackTunDevice {
    /// Bind the same original probe to the actual Native factory-created D.
    /// This test-only setter neither constructs a replacement D nor owns DATA.
    pub(crate) fn with_test_partial_flush_probe(
        mut self,
        probe: std::sync::Arc<PartialFlushProbe>,
    ) -> Self {
        assert!(self.partial_flush.is_none());
        self.partial_flush = Some(probe);
        self
    }
    /// N4 M7: count packets installed for the driver into `counter`:
    /// `[0]` by `wait_for_rx` (main receive path), `[1]` by `try_recv_rx`.
    pub(crate) fn with_test_rx_counter(
        mut self,
        counter: std::sync::Arc<[std::sync::atomic::AtomicU64; 2]>,
    ) -> Self {
        assert!(self.rx_installed.is_none());
        self.rx_installed = Some(counter);
        self
    }
    pub(in crate::harness) fn note_rx_installed(&self, path: usize) {
        if let Some(counter) = &self.rx_installed {
            counter[path].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_protected_tx_actual_cut_resume_and_shutdown_keep_stock_charged() {
    use crate::device::{DeviceBinding, DeviceShutdownState};
    let budget = bytes::BackingBudget::new(128 << 10).unwrap();
    let app = bytes::BackingMeter::with_budget(&budget);
    let data = bytes::BackingMeter::with_budget(&budget);
    let outbound = PacketLink::bounded(2);
    let mut device = LoopbackTunDevice::native_with_factory(2, 1500, &app, || {
        Ok((PacketLink::bounded(2), outbound.clone()))
    })
    .unwrap();
    device
        .bind_backing_meter(DeviceBinding::Native {
            meter: &app,
            slots: 2,
            mtu: 1500,
        })
        .unwrap();
    let header = BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    let held =
        BytesMut::try_zeroed_metered(budget.limit() - budget.used() - header, &data).unwrap();
    let first = packet(b"discard", 10);
    Device::transmit(&mut device, SmolInstant::from_millis(0))
        .unwrap()
        .consume(first.len(), |out| out.copy_from_slice(&first));
    let original = device.egress.packets_for_probe().next().unwrap().as_ptr();
    assert_eq!(budget.used(), budget.limit());
    let mut receipt = device.cut_ingress().await.unwrap();
    assert_eq!(
        (receipt.discarded_packets, receipt.discarded_backing),
        (1, 1500)
    );
    assert_eq!(
        budget.used(),
        budget.limit(),
        "actual cut retains the physical idle TX stock"
    );
    assert!(device.egress.packets_for_probe().next().is_none());
    assert!(Device::transmit(&mut device, SmolInstant::from_millis(1)).is_none());
    device.resume_ingress(&mut receipt).unwrap();
    let second = packet(b"actual write", 17);
    Device::transmit(&mut device, SmolInstant::from_millis(2))
        .unwrap()
        .consume(second.len(), |out| out.copy_from_slice(&second));
    assert_eq!(
        device.egress.packets_for_probe().next().unwrap().as_ptr(),
        original
    );
    device.flush_tx().await.unwrap();
    assert_eq!(&outbound.pop().unwrap()[..], second);
    let mut proofs = device.take_flushed_tcp_packets();
    assert_eq!(proofs.next().unwrap().payload_bytes, 12);
    proofs.commit();
    assert_eq!(budget.used(), budget.limit());
    {
        let mut receipt = std::future::poll_fn(|cx| device.poll_shutdown(cx)).await;
        assert_eq!(receipt.settle().await, DeviceShutdownState::Settled);
    }
    assert_eq!(
        app.snapshot().allocations,
        0,
        "original shutdown physically drops the Egress inventory"
    );
    drop(device);
    drop(held);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

use super::*;
#[tokio::test]
async fn native_written_future_sequence_quarantines_at_actual_proof_boundary() {
    use crate::client_tun::downlink_custody::{begin_flush_batch, finish_flush_batch};
    for future_sequence in [false, true] {
        let mut flow = Flow::new();
        flow.admit(b"abcd").await;
        let actual = flow.dispatch_payload();
        let captured = begin_flush_batch(&flow.sockets, &mut flow.ctxs, None).unwrap();
        assert_eq!(captured.len(), 1);
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let prefix = if cfg!(target_os = "macos") { 4 } else { 0 };
        let mut device = crate::device::egress::Egress::native(1, 1200, prefix, &meter).unwrap();
        let sequence = actual
            .payload_start_seq
            .wrapping_add(if future_sequence { 10 } else { 0 });
        let builder =
            etherparse::PacketBuilder::ipv4(actual.src_addr.octets(), actual.dst_addr.octets(), 64)
                .tcp(actual.src_port, actual.dst_port, sequence, 4096);
        device
            .inject(builder.size(4), prefix, |out| {
                let length = out.len();
                builder
                    .write(&mut &mut out[..], b"abcd")
                    .map_err(std::io::Error::other)?;
                Ok(length)
            })
            .unwrap();
        let wire = device.test_write_to_peer().await;
        let observed = crate::device::summarize_flushed_ip_tcp_packet(&wire[prefix..]).unwrap();
        assert_eq!(observed.payload_start_seq, sequence);
        let released = finish_flush_batch(
            device.summaries(),
            &flow.sockets,
            &mut flow.ctxs,
            captured,
            true,
        );
        assert_eq!(
            released.released_inflight_permit_bytes,
            if future_sequence { 0 } else { 4 }
        );
        assert_eq!(
            flow.queue.snapshot_now().leased_bytes,
            if future_sequence { 4 } else { 0 }
        );
        assert_eq!(
            budget.quarantined(),
            future_sequence,
            "Q must be synchronous with original proof failure"
        );
        drop(device);
        assert_eq!(budget.used(), usize::from(future_sequence));
    }
}

#[tokio::test]
async fn empty_native_summary_batch_keeps_unknown_end_batch_quarantined() {
    use crate::client_tun::downlink_custody::{begin_flush_batch, finish_flush_batch};
    let mut flow = Flow::new();
    flow.admit(b"abcd").await;
    flow.poll_queued();
    assert_eq!(flow.device.tx.len(), 1);
    let packet = flow.device.tx.pop().unwrap();
    let captured = begin_flush_batch(&flow.sockets, &mut flow.ctxs, None).unwrap();
    let budget = bytes::BackingBudget::new(65536).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let prefix = if cfg!(target_os = "macos") { 4 } else { 0 };
    let mut device = crate::device::egress::Egress::native(1, 1200, prefix, &meter).unwrap();
    device
        .inject(packet.len(), prefix, |out| {
            out.copy_from_slice(&packet);
            Ok(packet.len())
        })
        .unwrap();
    assert_eq!(
        device.test_write_to_closed_peer().await.kind(),
        std::io::ErrorKind::BrokenPipe
    );
    assert!(
        !budget.quarantined(),
        "plain IO alone has a known original packet"
    );
    assert!(device.owned_bytes() > 0);
    let summary = device.summaries();
    assert!(summary.is_empty());
    let release = finish_flush_batch(summary, &flow.sockets, &mut flow.ctxs, captured, false);
    assert_eq!(release.released_inflight_permit_bytes, 0);
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 4);
    assert!(
        budget.quarantined(),
        "original Main end_batch is explicitly unknown, so its App meter must already be Q"
    );
    drop(device);
    assert_eq!(budget.used(), 1);
}

//! Actual admitted and dispatched bytes; injected summaries model observation
//! overlap, reordering and fragmentation, without manufacturing socket progress.
use super::*;

fn piece(packet: TunFlushedTcpPacket, offset: usize, length: usize) -> TunFlushedTcpPacket {
    assert!(offset + length <= packet.payload_bytes);
    TunFlushedTcpPacket {
        payload_start_seq: packet.payload_start_seq.wrapping_add(offset as u32),
        payload_bytes: length,
        ..packet
    }
}

#[tokio::test]
async fn overlapping_proofs_wait_for_holes_and_refund_only_the_union() {
    let mut f = Flow::new();
    f.admit(b"abcdefghijkl").await;
    let packet = f.dispatch_payload();
    assert_eq!(f.observe(piece(packet, 6, 4)), 0);
    assert_eq!(f.observe(piece(packet, 3, 5)), 0);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 12);
    assert_eq!(f.observe(piece(packet, 0, 4)), 10);
    assert_eq!(f.observe(piece(packet, 8, 4)), 2);
    assert_eq!(f.observe(packet), 0);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 0);
    assert!(f.ctxs[&f.handle].downlink_inflight_permits.is_empty());
}

#[tokio::test]
async fn ack_after_flushed_a_cannot_refund_unflushed_b() {
    let mut f = Flow::new();
    f.admit(b"AAAA").await;
    let a = f.dispatch_payload();
    assert_eq!(f.observe(a), 4);
    f.admit(b"BBBB").await;
    assert_eq!(f.acknowledge(a.payload_start_seq.wrapping_add(4)), 0);
    assert_eq!(f.ctxs[&f.handle].downlink_inflight_permit_bytes(), 4);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
    assert!(!f.ctxs[&f.handle].downlink_inflight_permits.quarantined());
}

#[tokio::test]
async fn thirty_two_gap_slots_close_without_guessing_a_prefix() {
    let mut f = Flow::new();
    f.admit(&[7; 66]).await;
    let p = f.dispatch_payload();
    for i in 0..32 {
        assert_eq!(f.observe(piece(p, i * 2 + 1, 1)), 0);
    }
    assert!(!f.ctxs[&f.handle].downlink_inflight_permits.quarantined());
    assert_eq!(f.queue.snapshot_now().leased_bytes, 66);
    assert_eq!(f.observe(piece(p, 0, 64)), 64);
    assert_eq!(f.observe(piece(p, 64, 2)), 2);
    assert!(f.ctxs[&f.handle].downlink_inflight_permits.is_empty());
}

#[tokio::test]
async fn thirty_third_gap_is_retained_as_sticky_charged_quarantine() {
    let mut f = Flow::new();
    f.admit(&[7; 68]).await;
    let p = f.dispatch_payload();
    for i in 0..33 {
        assert_eq!(f.observe(piece(p, i * 2 + 1, 1)), 0);
    }
    assert_eq!(f.queue.snapshot_now().leased_bytes, 68);
    let c = f.ctxs.get_mut(&f.handle).unwrap();
    assert!(c.downlink_inflight_permits.quarantined());
    c.clear_downlink_pending();
    assert_eq!(c.downlink_inflight_permit_bytes(), 68);
    assert!(!c.downlink_inflight_permits.is_empty());
}

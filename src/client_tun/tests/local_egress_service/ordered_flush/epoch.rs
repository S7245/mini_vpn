//! Real same-tuple handshakes and gateway epochs; old captures are never copied
//! or manufactured. Old summary replay is explicitly an observation fault.
use super::*;
use crate::client_tun::compatibility_runtime::LocalReset;

fn tuple(f: &Flow) -> (IpEndpoint, IpEndpoint) {
    let socket = f.sockets.get::<TcpSocket>(f.handle);
    (
        socket.local_endpoint().unwrap(),
        socket.remote_endpoint().unwrap(),
    )
}

fn rearm(f: &mut Flow) {
    let (dispatch, _) = OpenDispatch::channel(1, None);
    legacy_bare_rearm_from_loop(
        &dispatch,
        f.handle,
        f.sockets.get_mut::<TcpSocket>(f.handle),
        f.ctxs.get_mut(&f.handle).unwrap(),
        &mut FakeIpPool::new(),
        0,
    );
}

async fn reset_and_reconnect(f: &mut Flow) {
    let epoch = f.ctxs[&f.handle].conn_epoch;
    let before = tuple(f);
    assert!(f.ctxs[&f.handle].downlink_inflight_permits.is_empty());
    assert!(!LocalReset::prepare_wire_test(
        f.ctxs.get_mut(&f.handle).unwrap(),
        f.sockets.get_mut::<TcpSocket>(f.handle)
    ));
    let deadline = f.ctxs[&f.handle]
        .local_reset
        .as_ref()
        .unwrap()
        .wire_deadline_test();
    f.poll_queued();
    f.flush_original().await.0.unwrap();
    assert!(f.reset_delivered());
    assert_eq!(
        f.ctxs[&f.handle]
            .local_reset
            .as_ref()
            .unwrap()
            .wire_deadline_test(),
        deadline
    );
    rearm(f);
    assert_eq!(f.ctxs[&f.handle].conn_epoch, epoch + 1);
    assert_eq!(f.queue.snapshot_now().owned_bytes(), 0);
    f.reconnect();
    assert_eq!(tuple(f), before);
}

#[tokio::test]
async fn original_pending_batch_blocks_rearm_then_clean_same_tuple_successor_refunds_once() {
    let mut f = Flow::new();
    let original_tuple = tuple(&f);
    f.admit(b"AAAA").await;
    f.poll_queued();
    let capture = f.capture_pending_flush();
    assert_eq!(capture.len(), 1);
    let old_epoch = f.ctxs[&f.handle].conn_epoch;
    rearm(&mut f);
    assert_eq!(f.ctxs[&f.handle].conn_epoch, old_epoch);
    assert_eq!(tuple(&f), original_tuple);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
    assert!(!f.ctxs[&f.handle].downlink_inflight_permits.is_empty());
    let release = f.finish_pending_flush(capture).await;
    assert_eq!(release.released_inflight_permit_bytes, 4);
    let a = f.last_delivered_payload();
    assert!(f.ctxs[&f.handle].downlink_inflight_permits.is_empty());
    let old_queue = f.queue.clone();
    reset_and_reconnect(&mut f).await;
    f.admit(b"BBBB").await;
    assert_eq!(old_queue.snapshot_now().owned_bytes(), 0);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
    f.poll_queued();
    let release = f.flush_original().await.1;
    assert_eq!(release.released_inflight_permit_bytes, 4);
    let b = f.last_delivered_payload();
    assert_ne!(a.payload_start_seq, b.payload_start_seq);
    assert_eq!(f.acknowledge(b.payload_start_seq.wrapping_add(4)), 0);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 0);
    assert!(f.ctxs[&f.handle].downlink_inflight_permits.is_empty());
    println!(
        "actual same tuple epoch={old_epoch}->{} oldA={} newB={} oldqueue0/newqueue0",
        f.ctxs[&f.handle].conn_epoch, a.payload_start_seq, b.payload_start_seq
    );
}

#[tokio::test]
async fn old_seq_observation_cannot_spend_new_epoch_original_cap() {
    let mut f = Flow::new();
    f.admit(b"AAAA").await;
    f.poll_queued();
    assert_eq!(f.flush_original().await.1.released_inflight_permit_bytes, 4);
    let a = f.last_delivered_payload();
    reset_and_reconnect(&mut f).await;
    f.admit(b"BBBB").await;
    let epoch = f.ctxs[&f.handle].conn_epoch;
    let (head, len) = f
        .sockets
        .get::<TcpSocket>(f.handle)
        .send_payload_range()
        .unwrap();
    assert_eq!(len, 4);
    assert_ne!(head, a.payload_start_seq);
    let behind = head
        .wrapping_add(len as u32)
        .wrapping_sub(a.payload_start_seq);
    assert_eq!(
        f.observe(a),
        0,
        "old DATA summary is not a new-epoch payload proof"
    );
    assert_eq!(f.ctxs[&f.handle].downlink_inflight_permit_bytes(), 4);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
    // The frozen half-space rule treats future/ambiguous observations as Q.
    // Do not suppress this error just to obtain a clean second generation.
    let ambiguous = behind >= (1u32 << 31) || behind < 4;
    assert_eq!(
        f.ctxs[&f.handle].downlink_inflight_permits.quarantined(),
        ambiguous
    );
    if ambiguous {
        rearm(&mut f);
        assert_eq!(f.ctxs[&f.handle].conn_epoch, epoch);
        assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
    } else {
        f.poll_queued();
        assert_eq!(f.flush_original().await.1.released_inflight_permit_bytes, 4);
        assert_eq!(f.queue.snapshot_now().leased_bytes, 0);
    }
    println!(
        "actual oldseq replay after same tuple epoch={epoch}: behind={behind} chargedQ={ambiguous}"
    );
}

#[tokio::test]
async fn old_seq_observation_future_keeps_actual_new_epoch_cap_in_sticky_q() {
    // Legal initial Interface configuration selects a real RNG sequence. Neither
    // socket/ledger seq nor the old actual packet summary is modified here.
    let mut f = Flow::with_seed(2);
    f.admit(b"AAAA").await;
    f.poll_queued();
    assert_eq!(f.flush_original().await.1.released_inflight_permit_bytes, 4);
    let old_a = f.last_delivered_payload();
    let old_queue = f.queue.clone();
    reset_and_reconnect(&mut f).await;
    assert_eq!(old_queue.snapshot_now().owned_bytes(), 0);
    f.admit(b"BBBB").await;
    let epoch = f.ctxs[&f.handle].conn_epoch;
    let original_tuple = tuple(&f);
    let (head, len) = f
        .sockets
        .get::<TcpSocket>(f.handle)
        .send_payload_range()
        .unwrap();
    assert_eq!(len, 4);
    let behind = head
        .wrapping_add(len as u32)
        .wrapping_sub(old_a.payload_start_seq);
    assert!(
        behind >= (1u32 << 31),
        "actual old summary must reach frozen future/ambiguous branch: {behind}"
    );
    // Observation fault only: replay the exact old delivered value, never an
    // invented/copyable nonCopy capture or a claim of a real retransmission.
    assert_eq!(f.observe(old_a), 0);
    assert!(f.ctxs[&f.handle].downlink_inflight_permits.quarantined());
    assert_eq!(f.ctxs[&f.handle].downlink_inflight_permit_bytes(), 4);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
    rearm(&mut f);
    assert_eq!(f.ctxs[&f.handle].conn_epoch, epoch);
    assert_eq!(tuple(&f), original_tuple);
    assert!(f.ctxs[&f.handle].local_reset.is_none());
    f.poll_queued();
    let (result, release) = f.flush_original().await;
    result.unwrap();
    let actual_b = f.last_delivered_payload();
    assert_eq!(
        (actual_b.payload_start_seq, actual_b.payload_bytes),
        (head, 4)
    );
    assert_eq!(release.released_inflight_permit_bytes, 0);
    assert!(f.ctxs[&f.handle].downlink_inflight_permits.quarantined());
    assert!(!f.ctxs[&f.handle].downlink_inflight_permits.is_empty());
    assert_eq!(f.ctxs[&f.handle].downlink_inflight_permit_bytes(), 4);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
    rearm(&mut f);
    assert_eq!(f.ctxs[&f.handle].conn_epoch, epoch);
    assert_eq!(tuple(&f), original_tuple);
    println!(
        "actual seed2 sameTuple epoch={epoch} oldAseq={} newBseq={head} behind={behind}; old observation refund0/new actual deliveredB refund0; original cap4/stickyQ blocks rearm",
        old_a.payload_start_seq
    );
}

#[tokio::test]
async fn actual_empty_no_flow_rearm_remains_pristine_for_native_main() {
    use crate::client_tun::downlink_custody::capture_poll_egress;
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut f = Flow::new();
    drop(capture_poll_egress(&f.sockets, &mut f.ctxs, Some(&meter)).unwrap());
    let epoch = f.ctxs[&f.handle].conn_epoch;
    assert!(!LocalReset::prepare_wire_test(f.ctxs.get_mut(&f.handle).unwrap(),
        f.sockets.get_mut::<TcpSocket>(f.handle)));
    let deadline = f.ctxs[&f.handle].local_reset.as_ref().unwrap().wire_deadline_test();
    f.poll_queued();
    f.flush_original().await.0.unwrap();
    assert!(f.reset_delivered());
    assert_eq!(f.ctxs[&f.handle].local_reset.as_ref().unwrap().wire_deadline_test(), deadline);
    rearm(&mut f);
    assert_eq!(f.ctxs[&f.handle].conn_epoch, epoch + 1);
    assert_eq!(f.sockets.get::<TcpSocket>(f.handle).state(), TcpState::Listen);
    assert_eq!(f.queue.snapshot_now().owned_bytes(), 0);
    eprintln!("[N1-empty-rearm-ledger] {:?}", f.ctxs[&f.handle].downlink_inflight_permits);
    let batch = capture_poll_egress(&f.sockets, &mut f.ctxs, Some(&meter));
    let accepted = batch.is_ok();
    drop((batch, f, meter));
    assert!(accepted, "actual empty no-F reset must not invent capture debt");
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

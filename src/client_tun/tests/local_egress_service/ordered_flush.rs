//! Actual Interface handshake, original send_slice helper and original caps.
//! Duplicate summary injection is an observation fault, not live retransmission.
use super::*;
use crate::client_tun::downlink_custody::release_downlink_inflight_permits_for_flushed_tcp_packets;
mod epoch;
mod ranges;
mod terminal;
mod wire;
mod native_storage;
use wire::Flow;

#[tokio::test]
async fn duplicate_flushed_packet_cannot_release_a_later_original_permit() {
    let mut flow = Flow::new();
    flow.admit(b"AAAA").await;
    let a = flow.dispatch_payload();
    assert_eq!(a.payload_bytes, 4);
    assert_eq!(flow.observe(a), 4);
    assert_eq!(flow.queue.snapshot().await.leased_bytes, 0);
    flow.admit(b"BBBB").await;
    assert_eq!(flow.queue.snapshot().await.leased_bytes, 4);
    // B is accepted by the real socket but has never been dispatched/flushed.
    let duplicate = flow.observe(a);
    let inflight = flow.ctxs[&flow.handle].downlink_inflight_permit_bytes();
    let leased = flow.queue.snapshot().await.leased_bytes;
    println!(
        "actual EstablishedSocket duplicateA release={duplicate} B inflight={inflight} qleased={leased}; B never dispatched"
    );
    assert_eq!(duplicate, 0, "duplicate A cannot release later B");
    assert_eq!((inflight, leased), (4, 4));
}

// Original D6 matching-flow oracle, now using an actual Established socket.
pub(in crate::client_tun::tests) async fn matching_flow_only() {
    static PAYLOAD: [u8; 64 * 1024] = [1; 64 * 1024];
    let mut flow = Flow::with_limits(64 * 1024, 1240);
    flow.admit_prefix(&PAYLOAD, 16 * 1024).await;
    assert_eq!(
        flow.ctxs[&flow.handle].downlink_inflight_permit_bytes(),
        16 * 1024
    );
    let packets = flow.dispatch_packets();
    let first = packets[0];
    assert_eq!(first.payload_bytes, 1200);
    assert_eq!(flow.observe(first), 1200);
    assert_eq!(
        flow.ctxs[&flow.handle].downlink_inflight_permit_bytes(),
        16 * 1024 - 1200
    );
    assert_eq!(flow.queue.snapshot().await.leased_bytes, 64 * 1024 - 1200);
    let wrong = TunFlushedTcpPacket {
        dst_addr: Ipv4Addr::new(10, 0, 0, 99),
        ..first
    };
    let released = release_downlink_inflight_permits_for_flushed_tcp_packets(
        vec![wrong],
        &flow.sockets,
        &mut flow.ctxs,
    );
    assert_eq!(released.flushed_tcp_payload_bytes, 1200);
    assert_eq!(
        released.released_inflight_permit_bytes, 0,
        "nonmatching flow cannot refund"
    );
}

#[tokio::test]
async fn known_local_reset_flush_settles_original_cap_then_relistens() {
    use crate::client_tun::compatibility_runtime::LocalReset;
    let mut flow = Flow::new();
    flow.admit(b"AAAA").await;
    let epoch = flow.ctxs[&flow.handle].conn_epoch;
    assert!(!LocalReset::prepare_wire_test(
        flow.ctxs.get_mut(&flow.handle).unwrap(),
        flow.sockets.get_mut::<TcpSocket>(flow.handle)
    ));
    let deadline = flow.ctxs[&flow.handle]
        .local_reset
        .as_ref()
        .unwrap()
        .wire_deadline_test();
    flow.poll_queued();
    assert_eq!(
        flow.sockets.get::<TcpSocket>(flow.handle).state(),
        TcpState::Closed
    );
    assert!(
        flow.sockets
            .get::<TcpSocket>(flow.handle)
            .remote_endpoint()
            .is_none()
    );
    let (result, release) = flow.flush_original().await;
    result.unwrap();
    assert!(
        flow.reset_delivered(),
        "actual original RST bytes reached local endpoint"
    );
    assert_eq!(
        release.released_inflight_permit_bytes, 0,
        "None is not payload ACK"
    );
    let ctx = &flow.ctxs[&flow.handle];
    assert_eq!(
        ctx.local_reset.as_ref().unwrap().wire_deadline_test(),
        deadline
    );
    println!(
        "actual RST delivered/tuple None; cap={} qleased={} ledgerQ={}",
        ctx.downlink_inflight_permit_bytes(),
        flow.queue.snapshot().await.leased_bytes,
        ctx.downlink_inflight_permits.quarantined()
    );
    assert!(
        !ctx.downlink_inflight_permits.quarantined(),
        "known actual reset must not become unknown tuple Q"
    );
    let (dispatch, _) = OpenDispatch::channel(1, None);
    legacy_bare_rearm_from_loop(
        &dispatch,
        flow.handle,
        flow.sockets.get_mut::<TcpSocket>(flow.handle),
        flow.ctxs.get_mut(&flow.handle).unwrap(),
        &mut FakeIpPool::new(),
        0,
    );
    assert_eq!(
        flow.sockets.get::<TcpSocket>(flow.handle).state(),
        TcpState::Listen
    );
    assert_eq!(flow.ctxs[&flow.handle].conn_epoch, epoch + 1);
    assert!(flow.ctxs[&flow.handle].downlink_inflight_permits.is_empty());
    assert_eq!(flow.queue.snapshot().await.leased_bytes, 0);
}

//! Original production cold qualification versus controlled source corruption.
//! The fixture uses real socket/capabilities, not a complete Service/Root claim.
use super::*;
use crate::client_tun::downlink_custody::*;

fn qualify(flow: &mut Flow, meter: &bytes::BackingMeter) {
    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    let mirror = FlowMirror::test_wire_bound_queue(flow.handle, ctx.conn_epoch, flow.queue.clone());
    let identity = mirror.capture_identity();
    ctx.flow_mirror = Some(mirror);
    storage::prepare_for_flow(
        &mut ctx.downlink_pending,
        &mut ctx.downlink_pending_permits,
        &mut ctx.downlink_inflight_permits,
        meter,
        identity,
    )
    .unwrap();
    flow.native_capture(meter);
}

#[tokio::test]
async fn native_capture_cold_qualification_rejects_source_missing_before_begin() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    qualify(&mut flow, &meter);
    flow.admit(b"cold").await;
    let original = flow.ctxs.get_mut(&flow.handle).unwrap().flow_mirror.take();
    let batch = begin_flush_batch(&flow.sockets, &mut flow.ctxs, Some(&meter));
    assert!(batch.is_err());
    assert!(budget.quarantined());
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 4);
    assert!(!flow.ctxs[&flow.handle].test_native_capture().1);
    flow.ctxs.get_mut(&flow.handle).unwrap().flow_mirror = original;
    drop(flow);
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn native_capture_source_profile_and_complete_flow_identity_stay_frozen() {
    for mutation in 0..3 {
        let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut flow = Flow::new();
        qualify(&mut flow, &meter);
        flow.admit(b"profile").await;
        flow.poll_queued();
        let batch = flow.capture_pending_flush();
        let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
        if mutation == 0 {
            ctx.d16_downlink_queue = Some(flow.queue.clone());
        } else {
            ctx.flow_mirror
                .as_mut()
                .unwrap()
                .test_change_capture_identity(mutation == 1);
        }
        let release = flow.finish_pending_flush(batch).await;
        assert_eq!(
            release.released_inflight_permit_bytes, 0,
            "source mutation={mutation}"
        );
        assert_eq!(flow.queue.snapshot_now().leased_bytes, 7);
        assert!(flow.ctxs[&flow.handle].test_native_capture().1);
        assert!(budget.quarantined());
        drop(flow);
        assert_eq!(budget.used(), 1);
    }
}

#[tokio::test]
async fn native_capture_cold_requalification_preserves_stock_and_requires_settled_ledger() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    qualify(&mut flow, &meter);
    let pointer = flow.ctxs[&flow.handle].test_native_capture().0;
    let cold = budget.used();
    flow.admit(b"live").await;
    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    let mut next = ctx.flow_mirror.as_ref().unwrap().capture_identity();
    next.3 += 1;
    assert!(
        storage::prepare_for_flow(
            &mut ctx.downlink_pending,
            &mut ctx.downlink_pending_permits,
            &mut ctx.downlink_inflight_permits,
            &meter,
            next
        )
        .is_err()
    );
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 4);
    flow.poll_queued();
    flow.flush_original().await.0.unwrap();
    // Storage-only requalification contract. Actual Root Ready/rearm authority
    // remains the production caller's obligation, not established by this probe.
    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    storage::prepare_for_flow(
        &mut ctx.downlink_pending,
        &mut ctx.downlink_pending_permits,
        &mut ctx.downlink_inflight_permits,
        &meter,
        next,
    )
    .unwrap();
    assert_eq!(ctx.test_native_capture(), (pointer, false));
    assert_eq!(budget.used(), cold);
    drop(flow);
    assert!(!budget.quarantined());
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn native_capture_settled_context_waits_for_next_root_without_old_epoch_debt() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    qualify(&mut flow, &meter);
    flow.admit(b"settled").await;
    flow.poll_queued();
    flow.flush_original().await.0.unwrap();
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 0);
    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    assert!(ctx.downlink_inflight_permits.is_empty());
    ctx.conn_epoch += 1;
    ctx.flow_mirror = None;
    // No new DATA admission or replacement authority is inferred here.
    let (result, release) = flow.flush_original().await;
    result.unwrap();
    assert_eq!(release.released_inflight_permit_bytes, 0);
    assert!(!budget.quarantined());
    drop(flow);
    assert_eq!(budget.used(), 0);
}

#[test]
fn checked_physical_socket_lookup_matches_real_storage_and_type() {
    let mut flow = Flow::new();
    let actual = flow.sockets.get::<TcpSocket>(flow.handle) as *const TcpSocket;
    assert_eq!(flow.sockets.try_get::<TcpSocket>(flow.handle).unwrap() as *const _, actual);
    assert!(flow.sockets.try_get::<smoltcp::socket::udp::Socket>(flow.handle).is_none());
    let empty = SocketSet::new(Vec::new());
    assert!(empty.try_get::<TcpSocket>(flow.handle).is_none());
    let removed = flow.sockets.remove(flow.handle);
    assert!(flow.sockets.try_get::<TcpSocket>(flow.handle).is_none());
    assert!(!flow.sockets.iter().any(|(handle, _)| handle == flow.handle));
    drop(removed);
}

#[tokio::test]
async fn native_capture_missing_or_wrong_socket_at_each_phase_retains_original_debt() {
    for phase in 0..4 {
        for wrong_type in [false, true] {
            let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
            let meter = bytes::BackingMeter::with_budget(&budget);
            let mut flow = Flow::new();
            prepare(&mut flow, &meter);
            flow.admit(b"debt").await;
            let mut batch = if phase == 0 { None } else {
                Some(capture_poll_egress(&flow.sockets, &mut flow.ctxs, Some(&meter)).unwrap())
            };
            if phase >= 2 {
                assert!(prepare_poll_egress_flush(batch.as_mut().unwrap(), &flow.sockets, &mut flow.ctxs));
            }
            if phase == 3 {
                assert_eq!(finish_poll_egress_flush(Vec::new(), &flow.sockets, &mut flow.ctxs,
                    batch.as_mut().unwrap(), true).released_inflight_permit_bytes, 0);
            }
            let original = flow.ctxs[&flow.handle].test_native_capture();
            let socket = flow.sockets.remove(flow.handle);
            if wrong_type {
                use smoltcp::socket::udp::{PacketBuffer, PacketMetadata, Socket};
                let rx = PacketBuffer::new(vec![PacketMetadata::EMPTY], vec![0; 64]);
                let tx = PacketBuffer::new(vec![PacketMetadata::EMPTY], vec![0; 64]);
                assert_eq!(flow.sockets.add(Socket::new(rx, tx)), flow.handle);
            }
            match phase {
                0 => assert!(capture_poll_egress(&flow.sockets, &mut flow.ctxs, Some(&meter)).is_err()),
                1 => assert!(!prepare_poll_egress_flush(batch.as_mut().unwrap(), &flow.sockets, &mut flow.ctxs)),
                2 => {
                    let release = finish_poll_egress_flush(Vec::new(), &flow.sockets, &mut flow.ctxs,
                        batch.as_mut().unwrap(), true);
                    assert_eq!(release.released_inflight_permit_bytes, 0);
                    assert!(matches!(release.refusal, Some(CaptureRefusal::Inventory)));
                }
                3 => assert_eq!(release_poll_egress_ack(batch.as_mut().unwrap(), &flow.sockets, &mut flow.ctxs), 0),
                _ => unreachable!(),
            }
            assert_eq!(flow.ctxs[&flow.handle].test_native_capture(), original);
            assert_eq!(flow.queue.snapshot_now().leased_bytes, 4);
            assert!(budget.quarantined());
            drop((batch, socket, flow, meter));
            assert_eq!(budget.used(), 1);
        }
    }
}

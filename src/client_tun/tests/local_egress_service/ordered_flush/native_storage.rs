//! Real smoltcp admission and original flush proof with fixed Main storage.
//! This does not accept the still-unmetered test Device as a Native Device.
use super::*;
use crate::client_tun::compatibility_runtime::FlowMirror;
use crate::client_tun::downlink_custody::storage;

#[tokio::test]
async fn native_fixed_main_aggregates_caps_but_preserves_actual_packet_proofs() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    {
        let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
        storage::prepare(
            &mut ctx.downlink_pending,
            &mut ctx.downlink_pending_permits,
            &mut ctx.downlink_inflight_permits,
            &meter,
        )
        .unwrap();
    }
    flow.native_capture(&meter);
    let charged = budget.used();
    assert_eq!(
        charged,
        65536 + 2 * std::mem::size_of::<DownstreamBytePermit>() + storage::capture_layout_bytes()
    );
    flow.admit_prefix(b"AAAA", 2).await;
    flow.admit_prefix(b"BBBB", 2).await;
    let state = flow.ctxs[&flow.handle].test_ordered_custody();
    assert_eq!(
        (state.pending.0, state.pending_caps.0, state.inflight_caps.0),
        (4, 1, 1)
    );
    assert_eq!(state.inflight_bytes, 4);
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 8);
    flow.poll_queued();
    let (result, release) = flow.flush_original().await;
    result.unwrap();
    assert_eq!(release.released_inflight_permit_bytes, 4);
    let first_packet = flow.last_delivered_payload();
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 4);
    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    assert_eq!(ctx.downlink_pending.as_slice(), b"BBBB");
    let outcome = flush_downlink(
        flow.handle,
        flow.sockets.get_mut::<TcpSocket>(flow.handle),
        ctx,
        4,
        DownlinkBackpressureConfig {
            high_bytes: 128 * 1024,
            low_bytes: 32 * 1024,
        },
        1500,
        &mut DownlinkEgressDropDebt::default(),
    );
    assert_eq!(outcome.accepted_bytes, 4);
    assert_eq!(
        flow.observe(first_packet),
        0,
        "a prior packet cannot prove the new cap"
    );
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 4);
    flow.poll_queued();
    let (result, release) = flow.flush_original().await;
    result.unwrap();
    assert_eq!(release.released_inflight_permit_bytes, 4);
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 0);
    let state = flow.ctxs[&flow.handle].test_ordered_custody();
    assert_eq!(
        (state.pending.0, state.pending_caps.0, state.inflight_caps.0),
        (0, 0, 0)
    );
    assert!(!state.quarantined);
    assert_eq!(
        budget.used(),
        charged,
        "empty retained allocations stay charged"
    );
    drop(flow);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn native_capture_cancel_preserves_original_custody_and_immediately_charges_q() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    storage::prepare(
        &mut ctx.downlink_pending,
        &mut ctx.downlink_pending_permits,
        &mut ctx.downlink_inflight_permits,
        &meter,
    )
    .unwrap();
    flow.native_capture(&meter);
    flow.admit(b"capture").await;
    flow.poll_queued();
    flow.hold_flush();
    let before = flow.ctxs[&flow.handle].test_ordered_custody();
    {
        let mut work = std::pin::pin!(flow.flush_original());
        assert!(
            work.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    let after = flow.ctxs[&flow.handle].test_ordered_custody();
    assert_eq!(after.epoch, before.epoch);
    assert_eq!(after.inflight_bytes, before.inflight_bytes);
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 7);
    let quarantined = budget.quarantined();
    drop(flow);
    assert!(
        quarantined,
        "losing the active capture handle must synchronously quarantine App backing"
    );
    assert_eq!(budget.used(), 1);
}

fn prepare(flow: &mut Flow, meter: &bytes::BackingMeter) {
    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    storage::prepare(
        &mut ctx.downlink_pending,
        &mut ctx.downlink_pending_permits,
        &mut ctx.downlink_inflight_permits,
        meter,
    )
    .unwrap();
    flow.native_capture(meter);
}

#[tokio::test]
async fn native_capture_reverse_inventory_detects_forgotten_entire_chain_before_drop() {
    use crate::client_tun::downlink_custody::*;
    let budget = bytes::BackingBudget::new(256 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    prepare(&mut flow, &meter);
    flow.admit(b"original").await;
    let mut batch = capture_poll_egress(&flow.sockets, &mut flow.ctxs, Some(&meter)).unwrap();
    let original = flow.ctxs[&flow.handle].test_native_capture();
    assert!(original.1);
    batch.test_forget_native_index();
    let accepted = prepare_poll_egress_flush(&mut batch, &flow.sockets, &mut flow.ctxs);
    let immediate = budget.quarantined();
    let same = flow.ctxs[&flow.handle].test_native_capture() == original;
    let leased = flow.queue.snapshot_now().leased_bytes;
    drop(batch);
    drop(flow);
    assert!(!accepted && immediate && same);
    assert_eq!(leased, 8);
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn native_capture_foreign_cap_queue_rejected_before_any_record_publication() {
    use crate::client_tun::downlink_custody::*;
    let budget = bytes::BackingBudget::new(256 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (mut a, mut b) = (Flow::new(), Flow::new());
    prepare(&mut a, &meter);
    prepare(&mut b, &meter);
    a.admit(b"AAAA").await;
    b.admit(b"BBBB").await;
    a.ctxs
        .get_mut(&a.handle)
        .unwrap()
        .test_swap_inflight_stocks(b.ctxs.get_mut(&b.handle).unwrap());
    let captured = begin_flush_batch(&a.sockets, &mut a.ctxs, Some(&meter));
    let refused = captured.is_err();
    let active = a.ctxs[&a.handle].test_native_capture().1;
    let leased = (
        a.queue.snapshot_now().leased_bytes,
        b.queue.snapshot_now().leased_bytes,
    );
    drop(captured);
    drop(a);
    drop(b);
    assert!(refused && !active);
    assert_eq!(leased, (4, 4));
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn native_capture_empty_known_io_has_no_unknown_capture_debt() {
    let budget = bytes::BackingBudget::new(256 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    prepare(&mut flow, &meter);
    flow.fail_flush();
    let (error, release) = flow.flush_original().await;
    assert_eq!(error.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
    assert_eq!(release.released_inflight_permit_bytes, 0);
    let q = budget.quarantined();
    drop(flow);
    assert!(!q);
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn native_capture_stale_socket_preflight_cannot_publish_a_prefix_without_q() {
    use crate::client_tun::downlink_custody::*;
    let budget = bytes::BackingBudget::new(256 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let (mut a, mut b) = (Flow::new(), Flow::new());
    prepare(&mut a, &meter);
    prepare(&mut b, &meter);
    a.admit(b"AAAA").await;
    b.admit(b"BBBB").await;
    let smoltcp::socket::Socket::Tcp(socket) = b.sockets.remove(b.handle) else {
        panic!("TCP fixture");
    };
    let second = a.sockets.add(socket);
    a.ctxs.insert(second, b.ctxs.remove(&b.handle).unwrap());
    let stale = *a.ctxs.keys().last().unwrap();
    a.sockets.remove(stale);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        capture_poll_egress(&a.sockets, &mut a.ctxs, Some(&meter))
    }));
    let returned_refusal = matches!(&result, Ok(Err(_)));
    let immediate = budget.quarantined();
    let any_active = a.ctxs.values().any(|c| c.test_native_capture().1);
    drop(result);
    drop(a);
    drop(b);
    assert!(returned_refusal && immediate && !any_active);
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn native_capture_actual_poll_packet_then_ack_share_one_refund_and_reuse_record() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    prepare(&mut flow, &meter);
    let original = flow.ctxs[&flow.handle].test_native_capture();
    let cold = budget.used();
    flow.admit(b"first").await;
    let (result, release) = flow.poll_original().await;
    result.unwrap();
    assert_eq!(release.released_inflight_permit_bytes, 5);
    assert_eq!(flow.ctxs[&flow.handle].test_native_capture(), original);
    let first = flow.last_delivered_payload();
    flow.enqueue_ack(
        first
            .payload_start_seq
            .wrapping_add(first.payload_bytes as u32),
    );
    let (result, release) = flow.poll_original().await;
    result.unwrap();
    assert_eq!(release.released_inflight_permit_bytes, 0);
    flow.admit(b"second").await;
    let second = flow.dispatch_payload(); // Actual delivery, summary withheld.
    flow.enqueue_ack(
        second
            .payload_start_seq
            .wrapping_add(second.payload_bytes as u32),
    );
    let (result, release) = flow.poll_original().await;
    result.unwrap();
    assert_eq!(release.released_inflight_permit_bytes, 6);
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 0);
    assert_eq!(flow.ctxs[&flow.handle].test_native_capture(), original);
    assert_eq!(budget.used(), cold);
    drop(flow);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn native_capture_epoch_change_and_post_capture_foreign_caps_keep_original_debt() {
    for foreign in [false, true] {
        let budget = bytes::BackingBudget::new(256 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let (mut a, mut b) = (Flow::new(), Flow::new());
        prepare(&mut a, &meter);
        prepare(&mut b, &meter);
        a.admit(b"AAAA").await;
        b.admit(b"BBBB").await;
        a.poll_queued();
        let batch = a.capture_pending_flush();
        let original = a.ctxs[&a.handle].test_native_capture();
        if foreign {
            a.ctxs
                .get_mut(&a.handle)
                .unwrap()
                .test_swap_inflight_stocks(b.ctxs.get_mut(&b.handle).unwrap());
        } else {
            a.ctxs.get_mut(&a.handle).unwrap().conn_epoch += 1;
        }
        let release = a.finish_pending_flush(batch).await;
        let leased = (
            a.queue.snapshot_now().leased_bytes,
            b.queue.snapshot_now().leased_bytes,
        );
        let kept = a.ctxs[&a.handle].test_native_capture();
        let immediate = budget.quarantined();
        drop(a);
        drop(b);
        assert_eq!(release.released_inflight_permit_bytes, 0);
        assert_eq!(leased, (4, 4));
        assert_eq!(kept, original);
        assert!(immediate);
        assert_eq!(budget.used(), 1);
    }
}

#[tokio::test]
async fn native_capture_write_panic_preserves_original_payload_and_marks_q_before_flow_drop() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    prepare(&mut flow, &meter);
    flow.admit(b"panic").await;
    flow.poll_queued();
    let payload = Arc::new(());
    flow.panic_next_flush(payload.clone());
    let original = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut work = std::pin::pin!(flow.flush_original());
        work.as_mut().poll(&mut Context::from_waker(Waker::noop()))
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        original.downcast_ref::<Arc<()>>().unwrap(),
        &payload
    ));
    assert!(budget.quarantined());
    assert!(flow.ctxs[&flow.handle].test_native_capture().1);
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 5);
    drop(flow);
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn native_capture_explicit_mode_rejects_legacy_or_foreign_without_prefix() {
    use crate::client_tun::downlink_custody::*;
    for foreign in [false, true] {
        let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let other = bytes::BackingMeter::with_budget(&budget);
        let mut flow = Flow::new();
        prepare(&mut flow, &meter);
        flow.admit(b"mode").await;
        let mode = if foreign { Some(&other) } else { None };
        let batch = begin_flush_batch(&flow.sockets, &mut flow.ctxs, mode);
        assert!(batch.is_err());
        assert!(!flow.ctxs[&flow.handle].test_native_capture().1);
        assert_eq!(flow.queue.snapshot_now().leased_bytes, 4);
        drop(flow);
        assert!(budget.quarantined());
        assert_eq!(budget.used(), 1);
    }
}

#[tokio::test]
async fn native_capture_dead_retained_without_observation_preserves_known_io_semantics() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    prepare(&mut flow, &meter);
    flow.admit(b"retained").await;
    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    ctx.flow_mirror = Some(FlowMirror::test_wire_bound_queue(
        flow.handle,
        ctx.conn_epoch,
        flow.queue.clone(),
    ));
    flow.sockets.get_mut::<TcpSocket>(flow.handle).abort();
    // Real smoltcp closed dispatch clears the endpoint pair. No fake tuple or
    // capability mutation; controlled mirror binding is explicitly not Root.
    flow.poll_queued();
    assert!(
        flow.sockets
            .get::<TcpSocket>(flow.handle)
            .local_endpoint()
            .is_none()
    );
    flow.fail_flush();
    let (result, release) = flow.flush_original().await;
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
    let kept = flow.queue.snapshot_now().leased_bytes;
    let active = flow.ctxs[&flow.handle].test_native_capture().1;
    let immediate = budget.quarantined();
    drop(flow);
    assert_eq!(release.released_inflight_permit_bytes, 0);
    assert_eq!(kept, 8);
    assert!(!active && !immediate);
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn native_capture_post_io_self_consistent_foreign_queue_cannot_replace_original_source() {
    use crate::client_tun::downlink_custody::*;
    for ack in [false, true] {
        let budget = bytes::BackingBudget::new(256 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let (mut a, mut b) = (Flow::new(), Flow::new());
        prepare(&mut a, &meter);
        prepare(&mut b, &meter);
        a.admit(b"AAAA").await;
        b.admit(b"BBBB").await;
        a.ctxs.get_mut(&a.handle).unwrap().d16_downlink_queue = Some(a.queue.clone());
        let swap = |a: &mut Flow, b: &mut Flow| {
            a.ctxs
                .get_mut(&a.handle)
                .unwrap()
                .downlink_inflight_permits
                .test_swap_complete_inflight(
                    &mut b.ctxs.get_mut(&b.handle).unwrap().downlink_inflight_permits,
                );
        };
        let released = if ack {
            let packet = a.dispatch_payload();
            let mut batch = capture_poll_egress(&a.sockets, &mut a.ctxs, Some(&meter)).unwrap();
            a.enqueue_ack(packet.payload_start_seq.wrapping_add(4));
            a.poll_queued();
            assert!(prepare_poll_egress_flush(
                &mut batch,
                &a.sockets,
                &mut a.ctxs
            ));
            assert_eq!(
                finish_poll_egress_flush(Vec::new(), &a.sockets, &mut a.ctxs, &mut batch, true)
                    .released_inflight_permit_bytes,
                0
            );
            swap(&mut a, &mut b);
            release_poll_egress_ack(&mut batch, &a.sockets, &mut a.ctxs)
        } else {
            a.poll_queued();
            let batch = a.capture_pending_flush();
            swap(&mut a, &mut b);
            a.finish_pending_flush(batch)
                .await
                .released_inflight_permit_bytes
        };
        let leased = (
            a.queue.snapshot_now().leased_bytes,
            b.queue.snapshot_now().leased_bytes,
        );
        let kept = a.ctxs[&a.handle].test_native_capture().1;
        let immediate = budget.quarantined();
        drop(a);
        drop(b);
        assert_eq!(released, 0, "ack={ack}");
        assert_eq!(leased, (4, 4));
        assert!(kept && immediate);
        assert_eq!(budget.used(), 1);
    }
}

#[tokio::test]
async fn native_capture_chunk_pending_without_egress_cap_is_not_an_ack_debt() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    prepare(&mut flow, &meter);
    let input = bytes::Bytes::copy_from_slice_metered(b"chunk", &meter).unwrap();
    assert!(
        flow.ctxs
            .get_mut(&flow.handle)
            .unwrap()
            .append_downlink_pending_with_permit(&input, &mut None)
    );
    let expected = budget.used();
    let (result, release) = flow.poll_original().await;
    let kept = flow.ctxs[&flow.handle].downlink_pending.as_slice() == b"chunk";
    let q = budget.quarantined();
    let charged = budget.used();
    drop(input);
    drop(flow);
    result.unwrap();
    assert_eq!(release.released_inflight_permit_bytes, 0);
    assert!(kept && !q);
    assert_eq!(charged, expected);
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn native_capture_false_delay_marker_cannot_hide_actual_egress_capabilities() {
    use crate::client_tun::downlink_custody::*;
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    prepare(&mut flow, &meter);
    flow.admit_prefix(b"AAAABBBB", 4).await;
    flow.ctxs
        .get_mut(&flow.handle)
        .unwrap()
        .downlink_delay_permit_release_until_egress = false;
    let captured = begin_flush_batch(&flow.sockets, &mut flow.ctxs, Some(&meter));
    assert!(captured.is_err());
    assert_eq!(flow.queue.snapshot_now().leased_bytes, 8);
    assert_eq!(flow.ctxs[&flow.handle].downlink_pending.as_slice(), b"BBBB");
    assert!(!flow.ctxs[&flow.handle].test_native_capture().1);
    assert!(budget.quarantined());
    drop(flow);
    assert_eq!(budget.used(), 1);
}

#[tokio::test]
async fn native_capture_on_admission_pending_stays_in_original_non_ack_owner() {
    let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut flow = Flow::new();
    prepare(&mut flow, &meter);
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        64,
        DownstreamPermitReleaseMode::OnAdmission,
    )
    .unwrap();
    queue
        .push(bytes::Bytes::copy_from_slice_metered(b"admit", &meter).unwrap())
        .await
        .unwrap();
    let (input, permit) = queue.recv_up_to(5).await.unwrap().into_parts();
    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    ctx.flow_mirror = Some(FlowMirror::test_wire_admission_queue(
        flow.handle,
        ctx.conn_epoch,
        queue.clone(),
    ));
    assert!(ctx.append_downlink_pending_with_permit(&input, &mut Some(permit)));
    let (result, release) = flow.poll_original().await;
    result.unwrap();
    assert_eq!(release.released_inflight_permit_bytes, 0);
    assert_eq!(queue.snapshot_now().leased_bytes, 5);
    let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
    let outcome = flush_downlink(
        flow.handle,
        flow.sockets.get_mut::<TcpSocket>(flow.handle),
        ctx,
        5,
        DownlinkBackpressureConfig::default(),
        1500,
        &mut DownlinkEgressDropDebt::default(),
    );
    assert_eq!(outcome.accepted_bytes, 5);
    assert_eq!(queue.snapshot_now().leased_bytes, 0);
    drop(input);
    drop(flow);
    drop(queue);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[tokio::test]
async fn native_capture_lost_original_mirror_binding_cannot_pass_as_absent_source() {
    for mismatch in 0..3 {
        let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut flow = Flow::new();
        prepare(&mut flow, &meter);
        flow.admit(b"bound").await;
        flow.poll_queued();
        let wrong_handle = flow.sockets.add(TcpSocket::new(
            TcpSocketBuffer::new(vec![0; 8]),
            TcpSocketBuffer::new(vec![0; 8]),
        ));
        let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
        ctx.flow_mirror = Some(FlowMirror::test_wire_bound_queue(
            flow.handle,
            ctx.conn_epoch,
            flow.queue.clone(),
        ));
        let batch = flow.capture_pending_flush();
        let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
        ctx.flow_mirror = Some(match mismatch {
            0 => FlowMirror::test_wire_admission_queue(
                flow.handle,
                ctx.conn_epoch,
                flow.queue.clone(),
            ),
            1 => FlowMirror::test_wire_bound_queue(
                flow.handle,
                ctx.conn_epoch + 1,
                flow.queue.clone(),
            ),
            _ => {
                FlowMirror::test_wire_bound_queue(wrong_handle, ctx.conn_epoch, flow.queue.clone())
            }
        });
        let release = flow.finish_pending_flush(batch).await;
        let held = flow.queue.snapshot_now().leased_bytes;
        let q = budget.quarantined();
        drop(flow);
        assert_eq!(
            release.released_inflight_permit_bytes, 0,
            "mirror mismatch={mismatch}"
        );
        assert_eq!(held, 5);
        assert!(q);
        assert_eq!(budget.used(), 1);
    }
}

#[tokio::test]
async fn native_capture_removing_original_mirror_cannot_erase_source_obligation() {
    use crate::client_tun::downlink_custody::*;
    let mut outcomes = Vec::new();
    for ack in [false, true] {
        let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut flow = Flow::new();
        prepare(&mut flow, &meter);
        flow.admit(b"owner").await;
        let ctx = flow.ctxs.get_mut(&flow.handle).unwrap();
        ctx.d16_downlink_queue = Some(flow.queue.clone());
        ctx.flow_mirror = Some(FlowMirror::test_wire_bound_queue(
            flow.handle,
            ctx.conn_epoch,
            flow.queue.clone(),
        ));
        let original;
        let released = if ack {
            let packet = flow.dispatch_payload();
            let mut batch =
                capture_poll_egress(&flow.sockets, &mut flow.ctxs, Some(&meter)).unwrap();
            flow.enqueue_ack(packet.payload_start_seq.wrapping_add(5));
            flow.poll_queued();
            assert!(prepare_poll_egress_flush(
                &mut batch,
                &flow.sockets,
                &mut flow.ctxs
            ));
            finish_poll_egress_flush(Vec::new(), &flow.sockets, &mut flow.ctxs, &mut batch, true);
            original = flow.ctxs.get_mut(&flow.handle).unwrap().flow_mirror.take();
            release_poll_egress_ack(&mut batch, &flow.sockets, &mut flow.ctxs)
        } else {
            flow.poll_queued();
            let batch = flow.capture_pending_flush();
            original = flow.ctxs.get_mut(&flow.handle).unwrap().flow_mirror.take();
            flow.finish_pending_flush(batch)
                .await
                .released_inflight_permit_bytes
        };
        let held = flow.queue.snapshot_now().leased_bytes;
        let q = budget.quarantined();
        let active = flow.ctxs[&flow.handle].test_native_capture().1;
        flow.ctxs.get_mut(&flow.handle).unwrap().flow_mirror = original; // Cleanup only, after observations.
        drop(flow);
        eprintln!(
            "mirror removed after capture: ack={ack} refund={released} held={held} active={active} q={q} final={}",
            budget.used()
        );
        outcomes.push((released, held, active, q, budget.used()));
    }
    assert_eq!(outcomes, vec![(0, 5, true, true, 1); 2]);
}

mod qualification;

#[tokio::test]
async fn native_capture_post_begin_q_preserves_typed_refusal_and_actual_cap_debt() {
    use crate::client_tun::downlink_custody::*;
    for at in 0..3 {
        let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut flow = Flow::new();
        prepare(&mut flow, &meter);
        flow.admit(b"AAAA").await;
        let mut batch = capture_poll_egress(&flow.sockets, &mut flow.ctxs, Some(&meter)).unwrap();
        if at == 0 {
            meter.quarantine();
        }
        let prepared = prepare_poll_egress_flush(&mut batch, &flow.sockets, &mut flow.ctxs);
        if at == 0 {
            assert!(!prepared);
        } else {
            assert!(prepared);
            if at == 1 {
                meter.quarantine();
            }
            let released = finish_poll_egress_flush(
                Vec::new(),
                &flow.sockets,
                &mut flow.ctxs,
                &mut batch,
                true,
            );
            assert_eq!(released.released_inflight_permit_bytes, 0);
            if at == 2 {
                meter.quarantine();
            }
            assert_eq!(
                release_poll_egress_ack(&mut batch, &flow.sockets, &mut flow.ctxs),
                0
            );
        }
        assert_eq!(
            poll_capture_refusal(&batch),
            Some(CaptureRefusal::Admission(
                bytes::MeteredCopyError::Quarantined
            ))
        );
        assert!(flow.ctxs[&flow.handle].test_native_capture().1);
        assert_eq!(flow.queue.snapshot_now().leased_bytes, 4);
        drop((batch, flow, meter));
        assert!(budget.quarantined());
        assert_eq!(budget.used(), 1);
    }
}

#[tokio::test]
async fn native_capture_q_between_gate_and_inventory_keeps_exact_cause() {
    use crate::client_tun::downlink_custody::*;
    for corrupt in [false, true] {
        let budget = bytes::BackingBudget::new(128 * 1024).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut flow = Flow::new();
        prepare(&mut flow, &meter);
        flow.admit(b"AAAA").await;
        let mut batch = capture_poll_egress(&flow.sockets, &mut flow.ctxs, Some(&meter)).unwrap();
        if corrupt {
            flow.ctxs.get_mut(&flow.handle).unwrap().conn_epoch += 1;
        }
        arm_q_after_capture_admission();
        assert!(!prepare_poll_egress_flush(
            &mut batch,
            &flow.sockets,
            &mut flow.ctxs
        ));
        assert_eq!(
            poll_capture_refusal(&batch),
            Some(if corrupt {
                CaptureRefusal::Inventory
            } else {
                CaptureRefusal::Admission(bytes::MeteredCopyError::Quarantined)
            })
        );
        assert_eq!(flow.queue.snapshot_now().leased_bytes, 4);
        assert!(flow.ctxs[&flow.handle].test_native_capture().1);
        drop((batch, flow, meter));
        assert!(budget.quarantined());
        assert_eq!(budget.used(), 1);
    }
}

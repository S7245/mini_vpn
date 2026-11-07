//! Original queue/permit/Notify and common-flush cancellation boundaries.
use super::*;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Wake, Waker};

#[tokio::test]
async fn queued_only_terminal_closes_original_queue_once() {
    let mut f = Flow::new();
    f.queue.push(Bytes::from_static(b"queued")).await.unwrap();
    let ctx = f.ctxs.get_mut(&f.handle).unwrap();
    ctx.d16_downlink_queue = Some(f.queue.clone());
    assert_eq!(
        drop_d16_owned_for_terminal(ctx, "test", "known_terminal"),
        6
    );
    assert!(f.queue.snapshot_now().closure.is_closed());
    assert_eq!(f.queue.snapshot_now().owned_bytes(), 0);
    assert!(ctx.downlink_inflight_permits.is_empty());
    assert_eq!(
        drop_d16_owned_for_terminal(ctx, "test", "known_terminal"),
        0
    );
}

#[tokio::test]
async fn foreign_source_is_rejected_before_either_queue_is_destroyed() {
    let mut f = Flow::new();
    f.admit(b"AAAA").await;
    let other = AsyncLeasedByteFlowQueue::new_with_release_mode(
        8,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    other.push(Bytes::from_static(b"BBBB")).await.unwrap();
    let ctx = f.ctxs.get_mut(&f.handle).unwrap();
    ctx.d16_downlink_queue = Some(other.clone());
    assert_eq!(drop_d16_owned_for_terminal(ctx, "test", "foreign"), 0);
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 4);
    assert!(ctx.downlink_inflight_permits.quarantined());
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
    assert_eq!(other.snapshot_now().queued_bytes, 4);
    assert!(!other.snapshot_now().closure.is_closed());
}

#[tokio::test]
async fn cancelling_actual_flush_observer_cannot_clear_inflight_or_rearm() {
    let mut f = Flow::new();
    f.admit(b"AAAA").await;
    f.poll_queued();
    f.hold_flush();
    {
        let mut future = std::pin::pin!(f.flush_original());
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    let epoch = f.ctxs[&f.handle].conn_epoch;
    let ctx = f.ctxs.get_mut(&f.handle).unwrap();
    assert!(ctx.downlink_inflight_permits.quarantined());
    ctx.clear_downlink_pending();
    let (dispatch, _) = OpenDispatch::channel(1, None);
    legacy_bare_rearm_from_loop(
        &dispatch,
        f.handle,
        f.sockets.get_mut::<TcpSocket>(f.handle),
        ctx,
        &mut FakeIpPool::new(),
        0,
    );
    assert_eq!(ctx.conn_epoch, epoch);
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 4);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
}

#[tokio::test]
async fn reset_io_failure_keeps_original_cap_and_original_deadline() {
    use crate::client_tun::compatibility_runtime::LocalReset;
    let mut f = Flow::new();
    f.admit(b"AAAA").await;
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
    f.fail_flush();
    let (result, release) = f.flush_original().await;
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
    assert_eq!(release.released_inflight_permit_bytes, 0);
    let ctx = &f.ctxs[&f.handle];
    let reset = ctx.local_reset.as_ref().unwrap();
    assert!(reset.quarantined());
    assert_eq!(reset.wire_deadline_test(), deadline);
    assert!(ctx.downlink_inflight_permits.quarantined());
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 4);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
}

struct PanicWake(AtomicBool);
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            !self.0.swap(false, Ordering::SeqCst),
            "actual original Notify panic"
        );
    }
}

#[tokio::test]
async fn notify_panic_after_actual_refund_cannot_normalize_zero_fifo() {
    let mut f = Flow::new();
    f.admit(b"AAAA").await;
    let packet = f.dispatch_payload();
    let free = f.queue.snapshot_now().available_bytes();
    f.queue.push(Bytes::from(vec![2; free])).await.unwrap();
    let queue = f.queue.clone();
    let waker = Waker::from(Arc::new(PanicWake(AtomicBool::new(true))));
    let mut waiter = std::pin::pin!(queue.reserve_read_up_to(1));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f.observe(packet))).is_err());
    assert_eq!(queue.snapshot_now().leased_bytes, 0);
    let ctx = f.ctxs.get_mut(&f.handle).unwrap();
    assert!(ctx.downlink_inflight_permits.quarantined());
    assert!(!ctx.downlink_inflight_permits.is_empty());
    ctx.clear_downlink_pending();
    assert!(!ctx.downlink_inflight_permits.is_empty());
}

#[tokio::test]
async fn bare_gateway_foreign_source_preserves_both_queues_and_epoch() {
    let mut f = Flow::new();
    f.admit(b"AAAA").await;
    let other = AsyncLeasedByteFlowQueue::new_with_release_mode(
        8,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    other.push(Bytes::from_static(b"BBBB")).await.unwrap();
    let ctx = f.ctxs.get_mut(&f.handle).unwrap();
    let epoch = ctx.conn_epoch;
    ctx.d16_downlink_queue = Some(other.clone());
    let (dispatch, _) = OpenDispatch::channel(1, None);
    legacy_bare_rearm_from_loop(
        &dispatch,
        f.handle,
        f.sockets.get_mut::<TcpSocket>(f.handle),
        ctx,
        &mut FakeIpPool::new(),
        0,
    );
    println!(
        "bare actual epoch={} original={epoch} cap={} A={} B={} Bclosed={} ledgerQ={}",
        ctx.conn_epoch,
        ctx.downlink_inflight_permit_bytes(),
        f.queue.snapshot_now().leased_bytes,
        other.snapshot_now().queued_bytes,
        other.snapshot_now().closure.is_closed(),
        ctx.downlink_inflight_permits.quarantined()
    );
    assert_eq!(
        ctx.conn_epoch, epoch,
        "foreign source must stop before actual abort/drop/relisten"
    );
    assert_eq!(
        f.sockets.get::<TcpSocket>(f.handle).state(),
        TcpState::Established
    );
    assert!(ctx.d16_downlink_queue.is_some());
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 4);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
    assert_eq!(other.snapshot_now().queued_bytes, 4);
    assert!(!other.snapshot_now().closure.is_closed());
    assert!(ctx.downlink_inflight_permits.quarantined());
}

#[tokio::test]
async fn partial_actual_flush_refunds_only_delivered_prefix_and_keeps_unknown_tail() {
    let mut f = Flow::with_limits(4096, 1064);
    f.admit(&[9; 2048]).await;
    f.poll_queued();
    f.partial_flush_after(1);
    let (result, release) = f.flush_original().await;
    assert_eq!(
        result.unwrap_err().to_string(),
        "controlled local partial flush"
    );
    assert_eq!(release.flushed_tcp_payload_bytes, 1024);
    assert_eq!(release.released_inflight_permit_bytes, 1024);
    let ctx = f.ctxs.get_mut(&f.handle).unwrap();
    assert!(ctx.downlink_inflight_permits.quarantined());
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 1024);
    assert_eq!(f.queue.snapshot_now().leased_bytes, 1024);
    ctx.clear_downlink_pending();
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 1024);
}

#[tokio::test(start_paused = true)]
async fn reset_deadline_expiring_during_empty_summary_flush_cannot_clean() {
    use crate::client_tun::compatibility_runtime::LocalReset;
    let mut f = Flow::new();
    f.admit(b"AAAA").await;
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
    f.delay_flush(std::time::Duration::from_secs(16));
    let (result, release) = f.flush_original().await;
    result.unwrap();
    assert!(f.reset_delivered());
    assert_eq!(release.flushed_tcp_payload_bytes, 0);
    let ctx = &f.ctxs[&f.handle];
    assert_eq!(
        ctx.local_reset.as_ref().unwrap().wire_deadline_test(),
        deadline
    );
    assert!(tokio::time::Instant::now() >= deadline);
    assert!(ctx.downlink_inflight_permits.quarantined());
    assert_eq!(f.queue.snapshot_now().leased_bytes, 4);
}

#[tokio::test]
async fn queued_close_notify_panic_preserves_cap_and_busy_before_clear() {
    let mut f = Flow::new();
    f.admit(b"AAAA").await;
    let free = f.queue.snapshot_now().available_bytes();
    f.queue.push(Bytes::from(vec![2; free])).await.unwrap();
    let q = f.queue.clone();
    let waker = Waker::from(Arc::new(PanicWake(AtomicBool::new(true))));
    let mut waiter = std::pin::pin!(q.reserve_read_up_to(1));
    assert!(
        waiter
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let ctx = f.ctxs.get_mut(&f.handle).unwrap();
    ctx.d16_downlink_queue = Some(q.clone());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || drop_d16_owned_for_terminal(ctx, "test", "queued_notify")
        ))
        .is_err()
    );
    assert_eq!(q.snapshot_now().queued_bytes, 0);
    assert!(q.snapshot_now().closure.is_closed());
    assert_eq!(q.snapshot_now().leased_bytes, 4);
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 4);
    assert!(ctx.downlink_inflight_permits.quarantined());
    ctx.clear_downlink_pending();
    assert_eq!(q.snapshot_now().leased_bytes, 4);
    assert_eq!(ctx.downlink_diag.permit_terminal_drop_events, 0);
}

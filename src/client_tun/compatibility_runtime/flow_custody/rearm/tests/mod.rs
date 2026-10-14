//! Locks pending-open reaper ordering through the production ReapPlan.
use super::*;

fn close_wait() -> SocketCloseSnapshot {
    SocketCloseSnapshot {
        tcp_state: TcpState::CloseWait,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: false,
        send_capacity: 4096,
        send_queue: 0,
        recv_queue: 0,
    }
}

#[test]
fn resumable_pending_fin_survives_sweep_until_open_completion() {
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::HandshakePending;
    ctx.pending_open_mode = Some(TcpOwnershipMode::Resumable);
    ctx.conn_epoch = 7;
    assert!(!ctx.has_live_uplink());
    assert!(ctx.uplink_buffer.is_empty());

    // This is the first sweep while the async open is still in flight.
    assert!(
        !should_reap_slot_with_snapshot(&mut ctx, close_wait(), 0),
        "a FIN is not a failed open: preserve the incarnation for LocalClose"
    );
    assert_eq!(ctx.state, SocketState::HandshakePending);
    assert_eq!(ctx.pending_open_mode, Some(TcpOwnershipMode::Resumable));
    assert_eq!(ctx.conn_epoch, 7);
    assert!(ctx.owned_terminal_wait_since_secs.is_none());

    // Once the peer resets, a pending provider cannot keep a dead socket alive.
    let closed = SocketCloseSnapshot {
        tcp_state: TcpState::Closed,
        active: false,
        can_send: false,
        may_send: false,
        ..close_wait()
    };
    assert!(should_reap_slot_with_snapshot(&mut ctx, closed, 0));
}

#[test]
fn pending_fin_exception_requires_both_resumable_mode_and_pending_state() {
    for (state, mode) in [
        (
            SocketState::HandshakePending,
            Some(TcpOwnershipMode::LegacyRelay),
        ),
        (SocketState::HandshakePending, None),
        (
            SocketState::OpeningRemote,
            Some(TcpOwnershipMode::Resumable),
        ),
        (SocketState::Relaying, Some(TcpOwnershipMode::Resumable)),
    ] {
        let mut ctx = SocketCtx::new(443);
        ctx.state = state;
        ctx.pending_open_mode = mode;
        assert!(
            should_reap_slot_with_snapshot(&mut ctx, close_wait(), 0),
            "unrelated lifecycle must preserve its prior reaper behavior: {state:?} {mode:?}"
        );
    }
}

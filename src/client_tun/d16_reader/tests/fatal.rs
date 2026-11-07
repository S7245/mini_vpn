//! Original D16 reader fatal paths retain source order and charged queue custody.
use super::*;

#[tokio::test]
async fn closed_global_receiver_stamps_before_queue_terminal() {
    let (queue, budget) = queue();
    let clock = std::sync::Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, back_rx) = crate::client_tun::relay_channel::channel(1);
    drop(back_rx);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    gate_tx.send(()).unwrap();
    let reader = GatedReader {
        gate: gate_rx,
        chunk: Some(NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from(vec![6; 8]),
        }),
    };
    let outcome = run_d16_native_reader(
        handle(),
        7,
        Box::new(reader),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    )
    .await;
    assert_eq!(outcome.reason, ("remote_to_local", "global_rx_closed"));
    assert_eq!(outcome.error_order, Some(0));
    assert_eq!(queue.snapshot_now().queued_bytes, 8);
    let mut leased = queue.recv_up_to(8).await.unwrap();
    assert_eq!(leased.permit_mut().release(8), 8);
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn lost_read_credit_stamps_original_reader_terminal() {
    let (queue, budget) = queue();
    let clock = std::sync::Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: true,
        max_batch_bytes: 0,
    });
    drop(credit);
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let outcome = run_d16_native_reader(
        handle(),
        7,
        Box::new(ErrorReader),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    )
    .await;
    assert_eq!(outcome.reason, ("local_to_remote", "read_credit_closed"));
    assert_eq!(outcome.error_order, Some(0));
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn ready_stop_wins_over_simultaneous_credit_close() {
    for _ in 0..32 {
        let (queue, budget) = queue();
        let clock = std::sync::Arc::new(crate::shared::errors::FailureOrder::default());
        let (_issuer, accounting) = paired_accounting(clock);
        let (credit, credit_rx) = watch::channel(RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        });
        let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        drop(credit);
        stop_tx.send(()).unwrap();
        let outcome = run_d16_native_reader(
            handle(),
            7,
            Box::new(ErrorReader),
            queue.clone(),
            credit_rx,
            back_tx,
            None,
            stop_rx,
            Some(accounting),
        )
        .await;
        assert_eq!(outcome.reason, ("local_to_remote", "reader_stopped"));
        assert_eq!(outcome.error_order, None);
        assert!(queue.d16_terminal_quota_zero(&budget));
    }
}

#[tokio::test]
async fn preclosed_queue_returns_original_terminal_cause() {
    let (queue, budget) = queue();
    queue.close_and_drop_queued_for_terminal("local_to_remote", "local_socket_terminal");
    let clock = std::sync::Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let outcome = run_d16_native_reader(
        handle(),
        7,
        Box::new(ErrorReader),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    )
    .await;
    assert_eq!(outcome.reason, ("local_to_remote", "local_socket_terminal"));
    assert_eq!(outcome.error_order, None);
    assert!(queue.d16_terminal_quota_zero(&budget));
}

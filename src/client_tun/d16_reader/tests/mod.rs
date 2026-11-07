//! Exact D16 reader error and rejected backing custody at the original queue.
use super::*;
use crate::client_tun::compatibility_runtime::ReceiveWindow;
use smoltcp::iface::SocketSet;
use smoltcp::socket::tcp::{Socket, SocketBuffer};
use std::future::Future;
use std::pin::Pin;

mod backing;
mod fatal;
mod panic;
mod window;

struct ErrorReader;
impl crate::upstream::NativeTcpReader for ErrorReader {
    fn poll_read_chunk(
        &mut self,
        _: &mut std::task::Context<'_>,
        _: usize,
    ) -> std::task::Poll<std::io::Result<Option<NativeTcpChunk>>> {
        std::task::Poll::Ready(Err(std::io::Error::from_raw_os_error(54)))
    }
}

struct GatedReader {
    gate: tokio::sync::oneshot::Receiver<()>,
    chunk: Option<NativeTcpChunk>,
}
impl crate::upstream::NativeTcpReader for GatedReader {
    fn poll_read_chunk(
        &mut self,
        cx: &mut std::task::Context<'_>,
        _: usize,
    ) -> std::task::Poll<std::io::Result<Option<NativeTcpChunk>>> {
        match Pin::new(&mut self.gate).poll(cx) {
            std::task::Poll::Ready(Ok(())) => std::task::Poll::Ready(Ok(self.chunk.take())),
            std::task::Poll::Ready(Err(_)) => {
                std::task::Poll::Ready(Err(std::io::Error::other("gate lost")))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

fn handle() -> SocketHandle {
    let mut sockets = SocketSet::new(Vec::new());
    sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ))
}

fn queue() -> (AsyncLeasedByteFlowQueue, D16GlobalByteBudget) {
    let budget = D16GlobalByteBudget::new(16).unwrap();
    let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
        16,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    (queue, budget)
}

fn paired_accounting(clock: FailureClock) -> (ReceiveWindow, D16ReaderAccounting) {
    let mut issuer = ReceiveWindow::new();
    let reader = issuer.take_reader().unwrap();
    (
        issuer,
        D16ReaderAccounting {
            clock,
            window: reader,
            backing_meter: None,
        },
    )
}

#[tokio::test]
async fn original_read_io_is_returned_after_actual_queue_close() {
    let (queue, budget) = queue();
    let (credit, read_credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(4);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let outcome = run_d16_native_reader(
        handle(),
        7,
        Box::new(ErrorReader),
        queue.clone(),
        read_credit_rx,
        back_tx,
        None,
        stop_rx,
        None,
    )
    .await;
    assert_eq!(outcome.error.unwrap().raw_os_error(), Some(54));
    assert!(outcome.retained.is_none());
    assert_eq!(outcome.reason, ("remote_to_local", "remote_read_failed"));
    assert_eq!(budget.snapshot().total_bytes(), 0);
    drop(credit);
}

#[tokio::test]
async fn original_reader_failures_stamp_at_source() {
    let clock = std::sync::Arc::new(crate::shared::errors::FailureOrder::default());
    let run = |clock: FailureClock| async move {
        let (_issuer, accounting) = paired_accounting(clock);
        let (queue, budget) = queue();
        let (_credit, credit_rx) = watch::channel(RelayReadCredit {
            paused: false,
            max_batch_bytes: 8,
        });
        let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(4);
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
        assert!(queue.d16_terminal_quota_zero(&budget));
        outcome
    };
    let first = run(clock.clone()).await;
    let second = run(clock.clone()).await;
    assert_eq!(first.error_order, Some(0));
    assert_eq!(second.error_order, Some(1));
    assert_eq!(first.error.unwrap().raw_os_error(), Some(54));
    assert_eq!(second.error.unwrap().raw_os_error(), Some(54));
}

#[tokio::test]
async fn closed_commit_returns_original_backing_to_reader_outcome() {
    let (queue, budget) = queue();
    let (credit, read_credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(4);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    let bytes = Bytes::from(vec![9; 8]);
    let backing = bytes.as_ptr();
    let reader = GatedReader {
        gate: gate_rx,
        chunk: Some(NativeTcpChunk { offset: 0, bytes }),
    };
    let task = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(reader),
        queue.clone(),
        read_credit_rx,
        back_tx,
        None,
        stop_rx,
        None,
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while budget.snapshot().reserved_bytes != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    queue
        .close_for_terminal_and_mark_ready("root", "cancelled")
        .await;
    gate_tx.send(()).unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        outcome.reason,
        ("remote_to_local", "d16_queue_commit_failed")
    );
    assert_eq!(outcome.queue_error, Some(ByteQueuePushError::Closed));
    assert_eq!(outcome.retained.as_ref().unwrap().as_ptr(), backing);
    assert_eq!(outcome.retained.as_ref().unwrap().as_ref(), &[9; 8]);
    assert_eq!(budget.snapshot().total_bytes(), 0);
    drop(credit);
}

#[tokio::test]
async fn full_readiness_channel_cannot_prevent_root_reader_stop() {
    let (queue, budget) = queue();
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
    back_tx
        .try_send((handle(), RelayEvent::DataReady { epoch: 99 }))
        .unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel();
    gate_tx.send(()).unwrap();
    let reader = GatedReader {
        gate: gate_rx,
        chunk: Some(NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from(vec![5; 8]),
        }),
    };
    let mut task = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(reader),
        queue.clone(),
        read_credit_rx,
        back_tx,
        None,
        stop_rx,
        None,
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while queue.snapshot_now().queued_bytes != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop_tx.send(()).unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_millis(100), &mut task).await;
    if result.is_err() {
        task.abort();
    }
    let outcome = result
        .expect("Root stop must join blocked D16 reader")
        .unwrap();
    assert_eq!(outcome.reason, ("local_to_remote", "reader_stopped"));
    assert_eq!(queue.snapshot_now().queued_bytes, 8);
    let mut leased = queue.recv_up_to(8).await.unwrap();
    assert_eq!(leased.permit_mut().release(8), 8);
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn legacy_outer_does_not_report_clean_after_original_reader_io() {
    let (queue, budget) = queue();
    let (tx, rx) = crate::client_tun::uplink::channel(4);
    let (back_tx, mut back_rx) = crate::client_tun::relay_channel::channel(8);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 8,
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        super::super::run_relay_d16(
            handle(),
            7,
            crate::upstream::NativeTcpRelayStream::new(
                Box::new(ErrorReader),
                Box::new(tokio::io::sink()),
            ),
            rx,
            back_tx,
            read_credit_rx,
            queue.clone(),
        ),
    )
    .await
    .unwrap();
    let mut closed = 0;
    while let Ok((_, event)) = back_rx.try_recv() {
        if let RelayEvent::Closed(close) = event {
            assert_eq!(
                (close.direction, close.reason),
                ("remote_to_local", "remote_read_failed")
            );
            closed += 1;
        }
    }
    assert_eq!(
        closed, 2,
        "reader and outer must both report the IO terminal"
    );
    assert!(queue.d16_terminal_quota_zero(&budget));
    drop(tx);
}

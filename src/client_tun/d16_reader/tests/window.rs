//! Original D16 reader consumes only the physical issuer's live credit.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct OneWindowChunk {
    first_limit: std::sync::Arc<AtomicUsize>,
    polls: std::sync::Arc<AtomicUsize>,
}

struct TwoWindowChunks {
    polls: std::sync::Arc<AtomicUsize>,
}

struct OverdrawReader {
    bytes: Option<Bytes>,
}

impl crate::upstream::NativeTcpReader for OverdrawReader {
    fn poll_read_chunk(
        &mut self,
        _: &mut std::task::Context<'_>,
        max_len: usize,
    ) -> std::task::Poll<std::io::Result<Option<NativeTcpChunk>>> {
        assert_eq!(max_len, 65_536);
        std::task::Poll::Ready(Ok(self
            .bytes
            .take()
            .map(|bytes| NativeTcpChunk { offset: 0, bytes })))
    }
}

impl crate::upstream::NativeTcpReader for TwoWindowChunks {
    fn poll_read_chunk(
        &mut self,
        _: &mut std::task::Context<'_>,
        max_len: usize,
    ) -> std::task::Poll<std::io::Result<Option<NativeTcpChunk>>> {
        let call = self.polls.fetch_add(1, Ordering::SeqCst);
        let chunk = match call {
            0 => Some(NativeTcpChunk {
                offset: 0,
                bytes: Bytes::from(vec![7; 65_536]),
            }),
            1 => {
                assert!(max_len >= 4);
                Some(NativeTcpChunk {
                    offset: 65_536,
                    bytes: Bytes::from_static(b"next"),
                })
            }
            _ => None,
        };
        std::task::Poll::Ready(Ok(chunk))
    }
}

impl crate::upstream::NativeTcpReader for OneWindowChunk {
    fn poll_read_chunk(
        &mut self,
        _: &mut std::task::Context<'_>,
        max_len: usize,
    ) -> std::task::Poll<std::io::Result<Option<NativeTcpChunk>>> {
        if self.polls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first_limit.store(max_len, Ordering::SeqCst);
            std::task::Poll::Ready(Ok(Some(NativeTcpChunk {
                offset: 0,
                bytes: Bytes::from(vec![7; 65_536]),
            })))
        } else {
            std::task::Poll::Pending
        }
    }
}

#[tokio::test]
async fn original_reader_stops_at_issued_window_before_second_poll() {
    let budget = D16GlobalByteBudget::new(131_072).unwrap();
    let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
        131_072,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    let clock = std::sync::Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 131_072,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(4);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let first_limit = std::sync::Arc::new(AtomicUsize::new(0));
    let polls = std::sync::Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(OneWindowChunk {
            first_limit: first_limit.clone(),
            polls: polls.clone(),
        }),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while queue.snapshot_now().queued_bytes != 65_536 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    stop_tx.send(()).unwrap();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.reason, ("local_to_remote", "reader_stopped"));
    assert_eq!(
        outcome.window.as_ref().map(ReaderWindow::read),
        Some(65_536)
    );
    assert!(!outcome.window.as_ref().unwrap().failed());
    assert_eq!(first_limit.load(Ordering::SeqCst), 65_536);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    let mut leased = queue.recv_up_to(65_536).await.unwrap();
    assert_eq!(leased.permit_mut().release(65_536), 65_536);
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn physical_issuer_refill_wakes_original_reader_without_extra_read() {
    let budget = D16GlobalByteBudget::new(131_072).unwrap();
    let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
        131_072,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    let clock = std::sync::Arc::new(crate::shared::errors::FailureOrder::default());
    let (mut issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 131_072,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(4);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let polls = std::sync::Arc::new(AtomicUsize::new(0));
    let task = tokio::spawn(run_d16_native_reader(
        handle(),
        7,
        Box::new(TwoWindowChunks {
            polls: polls.clone(),
        }),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while queue.snapshot_now().queued_bytes != 65_536 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    let mut first = queue.recv_up_to(65_536).await.unwrap();
    assert_eq!(first.permit_mut().release(65_536), 65_536);
    drop(first);
    issuer.admit(65_536, 0, 65_536);
    issuer.audit_storage(65_536, 0, 0);
    issuer.publish();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outcome.reason, ("remote_to_local", "remote_eof"));
    assert_eq!(
        outcome.window.as_ref().map(ReaderWindow::read),
        Some(65_540)
    );
    assert_eq!(polls.load(Ordering::SeqCst), 3);
    let mut final_chunk = queue.recv_up_to(4).await.unwrap();
    assert_eq!(final_chunk.bytes().as_ref(), b"next");
    assert_eq!(final_chunk.permit_mut().release(4), 4);
    assert!(queue.d16_terminal_quota_zero(&budget));
}

#[tokio::test]
async fn overdraw_keeps_original_backing_and_failed_window_in_joined_outcome() {
    let budget = D16GlobalByteBudget::new(131_072).unwrap();
    let queue = AsyncLeasedByteFlowQueue::new_with_global_budget(
        131_072,
        DownstreamPermitReleaseMode::OnEgressDrain,
        budget.clone(),
    )
    .unwrap();
    let clock = std::sync::Arc::new(crate::shared::errors::FailureOrder::default());
    let (_issuer, accounting) = paired_accounting(clock);
    let (_credit, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 131_072,
    });
    let (back_tx, _back_rx) = crate::client_tun::relay_channel::channel(1);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let bytes = Bytes::from(vec![3; 65_537]);
    let backing = bytes.as_ptr();
    let outcome = run_d16_native_reader(
        handle(),
        7,
        Box::new(OverdrawReader { bytes: Some(bytes) }),
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
        Some(accounting),
    )
    .await;
    assert_eq!(outcome.reason, ("root", "receive_window_debit_failed"));
    assert_eq!(outcome.error_order, Some(0));
    assert_eq!(outcome.retained.as_ref().unwrap().as_ptr(), backing);
    assert_eq!(outcome.window.as_ref().map(ReaderWindow::read), Some(0));
    assert!(outcome.window.as_ref().unwrap().failed());
    assert!(queue.d16_terminal_quota_zero(&budget));
}

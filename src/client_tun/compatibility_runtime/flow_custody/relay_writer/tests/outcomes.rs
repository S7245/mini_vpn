use super::*;
use crate::client_tun::RELAY_WRITER_COALESCE_MAX_BYTES;
use smoltcp::iface::SocketSet;
use smoltcp::socket::tcp::{Socket, SocketBuffer};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

#[derive(Default)]
struct Probe {
    dropped: AtomicBool,
    written: AtomicUsize,
    write_polled: AtomicBool,
    shutdowns: AtomicUsize,
}

#[derive(Default)]
struct Writer {
    probe: Arc<Probe>,
    fail_after: Option<usize>,
    flush_error: bool,
    shutdown_error: bool,
    pending_write: bool,
    pending_shutdown: bool,
    zero_write: bool,
    panic_shutdown: bool,
    panic_drop: bool,
    overreport: bool,
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.probe.dropped.store(true, Ordering::SeqCst);
        if self.panic_drop {
            panic!("writer drop panic");
        }
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.probe.write_polled.store(true, Ordering::SeqCst);
        if self.overreport {
            return Poll::Ready(Ok(bytes.len() + 1));
        }
        if self.pending_write {
            return Poll::Pending;
        }
        if self.zero_write {
            return Poll::Ready(Ok(0));
        }
        let written = self.probe.written.load(Ordering::SeqCst);
        let n = match self.fail_after {
            Some(limit) if written == limit => {
                return Poll::Ready(Err(std::io::Error::from_raw_os_error(32)));
            }
            Some(limit) => bytes.len().min(limit - written),
            None => bytes.len(),
        };
        self.probe.written.fetch_add(n, Ordering::SeqCst);
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(if self.flush_error {
            Err(std::io::Error::from_raw_os_error(28))
        } else {
            Ok(())
        })
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.probe.shutdowns.fetch_add(1, Ordering::SeqCst);
        if self.panic_shutdown {
            panic!("writer shutdown panic");
        }
        if self.pending_shutdown {
            return Poll::Pending;
        }
        Poll::Ready(if self.shutdown_error {
            Err(std::io::Error::from_raw_os_error(5))
        } else {
            Ok(())
        })
    }
}

fn handle() -> SocketHandle {
    let mut sockets = SocketSet::new(Vec::new());
    sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ))
}

fn discard(outcome: &mut WriterOutcome, budget: usize) -> Poll<WriterDiscardReceipt> {
    outcome
        .remainder
        .poll_discard(&mut Context::from_waker(Waker::noop()), budget)
}

async fn finish(commands: Vec<RelayCommand>, writer: Writer) -> WriterOutcome {
    let (tx, rx) = mpsc::channel(8);
    for command in commands {
        tx.try_send(command).unwrap();
    }
    drop(tx);
    let (signals, _signals_rx) = mpsc::channel(32);
    let (_stop, stop_rx) = tokio::sync::oneshot::channel();
    run_custodied_relay_writer(handle(), writer, rx, signals, stop_rx).await
}

#[tokio::test]
async fn partial_write_retains_raw_errors_and_every_current_deferred_queued_byte() {
    let cap = RELAY_WRITER_COALESCE_MAX_BYTES;
    let probe = Arc::new(Probe::default());
    let mut outcome = finish(
        vec![
            RelayCommand::Data(vec![1; cap - 1]),
            RelayCommand::Data(vec![2; 2]),
            RelayCommand::Data(vec![3; 3]),
            RelayCommand::Finish,
        ],
        Writer {
            probe: probe.clone(),
            fail_after: Some(3),
            shutdown_error: true,
            ..Writer::default()
        },
    )
    .await;
    assert!(
        probe.dropped.load(Ordering::SeqCst),
        "IO must not enter the outcome"
    );
    let error = outcome.result.failure.as_ref().unwrap();
    assert_eq!(error.operation, WriterOperation::Write);
    assert_eq!(error.source.raw_os_error(), Some(32));
    assert_eq!(
        outcome
            .result
            .shutdown_failure
            .as_ref()
            .unwrap()
            .raw_os_error(),
        Some(5)
    );
    assert_eq!(
        outcome.remainder.current.as_ref().unwrap().payload.len(),
        cap - 1
    );
    assert!(matches!(discard(&mut outcome, 1), Poll::Pending));
    assert_eq!(
        outcome.remainder.receipt.queued.messages, 1,
        "one poll quota"
    );
    let Poll::Ready(receipt) = discard(&mut outcome, 2) else {
        panic!("drain")
    };
    let current = receipt.current.unwrap();
    assert_eq!(
        (current.bytes, current.written, current.flushed),
        (cap - 1, 3, false)
    );
    assert_eq!((receipt.deferred.messages, receipt.deferred.bytes), (1, 2));
    assert_eq!(
        (
            receipt.queued.messages,
            receipt.queued.bytes,
            receipt.queued.finishes
        ),
        (2, 3, 1)
    );
    assert_eq!(
        discard(&mut outcome, 1),
        Poll::Ready(receipt),
        "idempotent receipt"
    );
}

#[tokio::test]
async fn flush_error_keeps_accepted_but_unflushed_payload_and_shutdown_source() {
    let mut outcome = finish(
        vec![RelayCommand::Data(vec![7; 4])],
        Writer {
            probe: Arc::default(),
            flush_error: true,
            shutdown_error: true,
            ..Writer::default()
        },
    )
    .await;
    let error = outcome.result.failure.as_ref().unwrap();
    assert_eq!(error.operation, WriterOperation::Flush);
    assert_eq!(error.source.raw_os_error(), Some(28));
    assert_eq!(
        outcome
            .result
            .shutdown_failure
            .as_ref()
            .unwrap()
            .raw_os_error(),
        Some(5)
    );
    let Poll::Ready(receipt) = discard(&mut outcome, 1) else {
        panic!("drain")
    };
    assert_eq!(
        receipt.current.unwrap(),
        CurrentWriteReceipt {
            bytes: 4,
            written: 4,
            flushed: false,
            finish_after: false,
        }
    );
}

#[tokio::test]
async fn successful_finish_retains_coalesced_current_and_post_finish_queue() {
    let mut outcome = finish(
        vec![
            RelayCommand::Data(vec![1; 2]),
            RelayCommand::Data(vec![2; 3]),
            RelayCommand::Finish,
            RelayCommand::Data(vec![3; 4]),
        ],
        Writer::default(),
    )
    .await;
    assert_eq!(outcome.result.terminal, WriterTerminal::Finished);
    assert!(outcome.result.failure.is_none());
    let Poll::Ready(receipt) = discard(&mut outcome, 2) else {
        panic!("drain")
    };
    assert_eq!(
        receipt.current.unwrap(),
        CurrentWriteReceipt {
            bytes: 5,
            written: 5,
            flushed: true,
            finish_after: true,
        }
    );
    assert_eq!((receipt.queued.bytes, receipt.queued.messages), (4, 1));
}

#[tokio::test]
async fn outstanding_permit_stays_pending_until_actual_late_delivery_is_disposed() {
    let (tx, rx) = mpsc::channel(2);
    let permit = tx.clone().reserve_owned().await.unwrap();
    tx.try_send(RelayCommand::Finish).unwrap();
    let (signals, _signals_rx) = mpsc::channel(8);
    let (_stop, stop_rx) = tokio::sync::oneshot::channel();
    let mut outcome =
        run_custodied_relay_writer(handle(), Writer::default(), rx, signals, stop_rx).await;
    assert_eq!(
        discard(&mut outcome, 4),
        Poll::Pending,
        "close cannot sign an outstanding permit"
    );
    assert!(tx.is_closed());
    assert!(
        outcome.remainder.receiver.is_some(),
        "root still holds actual receiver"
    );
    permit.send(RelayCommand::Data(vec![9; 7]));
    let Poll::Ready(receipt) = discard(&mut outcome, 2) else {
        panic!("actual None")
    };
    assert_eq!((receipt.queued.messages, receipt.queued.bytes), (1, 7));
    assert!(outcome.remainder.receiver.is_none());
}

#[tokio::test]
async fn stop_during_actual_pending_write_preserves_current_and_shutdown_error() {
    let probe = Arc::new(Probe::default());
    let (tx, rx) = mpsc::channel(2);
    tx.try_send(RelayCommand::Data(vec![1; 9])).unwrap();
    let (signals, _signals_rx) = mpsc::channel(8);
    let (stop, stop_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run_custodied_relay_writer(
        handle(),
        Writer {
            probe: probe.clone(),
            pending_write: true,
            shutdown_error: true,
            ..Writer::default()
        },
        rx,
        signals,
        stop_rx,
    ));
    while !probe.write_polled.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    stop.send(()).unwrap();
    let mut outcome = task.await.unwrap();
    assert_eq!(outcome.result.terminal, WriterTerminal::Stopped);
    assert_eq!(
        outcome
            .result
            .shutdown_failure
            .as_ref()
            .unwrap()
            .raw_os_error(),
        Some(5)
    );
    let Poll::Ready(receipt) = discard(&mut outcome, 1) else {
        panic!("drain")
    };
    assert_eq!(
        (
            receipt.current.unwrap().bytes,
            receipt.current.unwrap().written
        ),
        (9, 0)
    );
}

#[tokio::test]
async fn zero_write_and_finish_shutdown_error_preserve_distinct_sources() {
    let mut zero = finish(
        vec![RelayCommand::Data(vec![1])],
        Writer {
            probe: Arc::default(),
            zero_write: true,
            ..Writer::default()
        },
    )
    .await;
    assert_eq!(
        zero.result.failure.as_ref().unwrap().source.kind(),
        std::io::ErrorKind::WriteZero
    );
    assert!(discard(&mut zero, 1).is_ready());
    let mut shutdown = finish(
        vec![RelayCommand::Finish],
        Writer {
            probe: Arc::default(),
            shutdown_error: true,
            ..Writer::default()
        },
    )
    .await;
    assert_eq!(
        shutdown.result.failure.as_ref().unwrap().operation,
        WriterOperation::Shutdown
    );
    assert_eq!(
        shutdown
            .result
            .failure
            .as_ref()
            .unwrap()
            .source
            .raw_os_error(),
        Some(5)
    );
    assert!(discard(&mut shutdown, 1).is_ready());
}

#[tokio::test(start_paused = true)]
async fn finish_shutdown_timeout_is_retained() {
    let mut outcome = finish(
        vec![RelayCommand::Finish],
        Writer {
            probe: Arc::default(),
            pending_shutdown: true,
            ..Writer::default()
        },
    )
    .await;
    assert_eq!(outcome.result.terminal, WriterTerminal::ShutdownTimedOut);
    assert!(outcome.result.failure.is_none());
    assert!(discard(&mut outcome, 1).is_ready());
}

#[tokio::test]
async fn legacy_io_and_receiver_drop_before_joinhandle_is_consumed() {
    let probe = Arc::new(Probe::default());
    let (tx, rx) = mpsc::channel(4);
    tx.try_send(RelayCommand::Finish).unwrap();
    tx.try_send(RelayCommand::Data(vec![1; 4])).unwrap();
    let (signals, _signals_rx) = mpsc::channel(8);
    let (_stop, stop_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run_relay_writer(
        handle(),
        Writer {
            probe: probe.clone(),
            ..Writer::default()
        },
        rx,
        signals,
        stop_rx,
    ));
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    assert!(probe.dropped.load(Ordering::SeqCst));
    assert!(tx.is_closed(), "legacy JoinHandle output cannot retain Rx");
    task.await.unwrap();
}

#[tokio::test]
async fn source_error_and_entities_survive_cleanup_shutdown_panic() {
    let probe = Arc::new(Probe::default());
    let mut outcome = finish(
        vec![RelayCommand::Data(vec![1; 5])],
        Writer {
            probe: probe.clone(),
            fail_after: Some(2),
            panic_shutdown: true,
            ..Writer::default()
        },
    )
    .await;
    assert_eq!(
        outcome
            .result
            .failure
            .as_ref()
            .unwrap()
            .source
            .raw_os_error(),
        Some(32)
    );
    assert_eq!(
        outcome.panics[0].as_ref().unwrap().downcast_ref::<&str>(),
        Some(&"writer shutdown panic")
    );
    assert!(probe.dropped.load(Ordering::SeqCst));
    assert_eq!(outcome.remainder.current.as_ref().unwrap().payload.len(), 5);
    assert_eq!(outcome.remainder.current.as_ref().unwrap().written, 2);
    assert!(outcome.remainder.receiver.is_some());
    assert!(outcome.remainder.quarantined());
    assert_eq!(
        discard(&mut outcome, 8),
        Poll::Pending,
        "interruption cannot sign clean"
    );
}

#[tokio::test]
async fn cooperative_pending_keeps_receiver_until_next_actual_control_poll() {
    tokio::spawn(async {
        let mut outcome = finish(
            vec![RelayCommand::Finish, RelayCommand::Data(vec![1; 3])],
            Writer::default(),
        )
        .await;
        while tokio::task::coop::has_budget_remaining() {
            tokio::task::coop::consume_budget().await;
        }
        assert_eq!(discard(&mut outcome, 8), Poll::Pending);
        assert!(outcome.remainder.receiver.is_some());
        assert_eq!(outcome.remainder.receipt.queued.messages, 0);
        assert!(!outcome.remainder.quarantined());
        tokio::task::yield_now().await;
        let Poll::Ready(receipt) = discard(&mut outcome, 8) else {
            panic!("next turn")
        };
        assert_eq!((receipt.queued.bytes, receipt.queued.messages), (3, 1));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn receiver_close_waker_panic_keeps_source_and_sticky_entity_custody() {
    use std::future::Future;
    struct PanicWake;
    impl std::task::Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("sender wake panic");
        }
    }
    let (tx, rx) = mpsc::channel(2);
    tx.try_send(RelayCommand::Data(vec![1; 3])).unwrap();
    let (signals, _signals_rx) = mpsc::channel(8);
    let (_stop, stop_rx) = tokio::sync::oneshot::channel();
    let mut outcome = run_custodied_relay_writer(
        handle(),
        Writer {
            probe: Arc::default(),
            fail_after: Some(0),
            ..Writer::default()
        },
        rx,
        signals,
        stop_rx,
    )
    .await;
    tx.try_send(RelayCommand::Data(vec![2; 4])).unwrap();
    let mut waiting = std::pin::pin!(tx.reserve_many(2));
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let observed =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| discard(&mut outcome, 8)));
    assert!(observed.is_err());
    assert_eq!(
        outcome
            .result
            .failure
            .as_ref()
            .unwrap()
            .source
            .raw_os_error(),
        Some(32)
    );
    assert!(outcome.remainder.quarantined());
    assert_eq!(outcome.remainder.current.as_ref().unwrap().payload.len(), 3);
    assert!(outcome.remainder.receiver.is_some());
    assert_eq!(discard(&mut outcome, 8), Poll::Pending);
}

#[tokio::test]
async fn receipt_overflow_retains_the_unaccounted_command_and_never_signs_clean() {
    let mut outcome = finish(
        vec![RelayCommand::Finish, RelayCommand::Data(vec![4; 2])],
        Writer::default(),
    )
    .await;
    outcome.remainder.receipt.queued.bytes = usize::MAX;
    assert_eq!(discard(&mut outcome, 8), Poll::Pending);
    assert!(outcome.remainder.quarantined());
    assert!(
        matches!(&outcome.remainder.unaccounted, Some(RelayCommand::Data(bytes)) if bytes.len() == 2)
    );
    assert!(outcome.remainder.receiver.is_some());
    assert_eq!(outcome.remainder.receipt.queued.bytes, usize::MAX);
    assert_eq!(discard(&mut outcome, 8), Poll::Pending);
}

#[tokio::test]
async fn io_drop_panic_keeps_prior_errno_and_root_owned_buffers() {
    let probe = Arc::new(Probe::default());
    let mut outcome = finish(
        vec![RelayCommand::Data(vec![1; 3])],
        Writer {
            probe: probe.clone(),
            fail_after: Some(0),
            panic_drop: true,
            ..Writer::default()
        },
    )
    .await;
    assert!(probe.dropped.load(Ordering::SeqCst));
    assert_eq!(
        outcome
            .result
            .failure
            .as_ref()
            .unwrap()
            .source
            .raw_os_error(),
        Some(32)
    );
    assert_eq!(
        outcome.panics[1].as_ref().unwrap().downcast_ref::<&str>(),
        Some(&"writer drop panic")
    );
    assert_eq!(outcome.remainder.current.as_ref().unwrap().payload.len(), 3);
    assert!(outcome.remainder.receiver.is_some());
    assert!(outcome.remainder.quarantined());
    assert_eq!(discard(&mut outcome, 8), Poll::Pending);
}

#[tokio::test]
async fn legacy_panic_resumes_after_receiver_and_io_are_dropped() {
    let probe = Arc::new(Probe::default());
    let (tx, rx) = mpsc::channel(2);
    tx.try_send(RelayCommand::Data(vec![1; 2])).unwrap();
    let (signals, _signals_rx) = mpsc::channel(8);
    let (_stop, stop_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run_relay_writer(
        handle(),
        Writer {
            probe: probe.clone(),
            fail_after: Some(1),
            panic_shutdown: true,
            ..Writer::default()
        },
        rx,
        signals,
        stop_rx,
    ));
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    assert!(probe.dropped.load(Ordering::SeqCst));
    assert!(tx.is_closed());
    assert!(task.await.unwrap_err().is_panic());
}

#[tokio::test]
async fn overreported_write_is_rejected_without_corrupting_prefix_accounting() {
    let mut outcome = finish(
        vec![RelayCommand::Data(vec![1; 3])],
        Writer {
            probe: Arc::default(),
            overreport: true,
            ..Writer::default()
        },
    )
    .await;
    assert_eq!(
        outcome.result.failure.as_ref().unwrap().source.kind(),
        std::io::ErrorKind::InvalidData
    );
    let Poll::Ready(receipt) = discard(&mut outcome, 1) else {
        panic!("drain")
    };
    assert_eq!(
        (
            receipt.current.unwrap().bytes,
            receipt.current.unwrap().written
        ),
        (3, 0)
    );
}

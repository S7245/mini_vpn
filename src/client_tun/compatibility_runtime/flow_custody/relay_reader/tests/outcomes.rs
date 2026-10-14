use super::*;
use smoltcp::iface::SocketSet;
use smoltcp::socket::tcp::{Socket, SocketBuffer};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::oneshot;

enum Read {
    Data(Vec<u8>),
    Error(i32),
    Eof,
    Pending,
    Panic,
}

#[derive(Default)]
struct Probe {
    reads: AtomicUsize,
    dropped: AtomicBool,
}

struct Stream {
    reads: VecDeque<Read>,
    probe: Arc<Probe>,
    panic_drop: bool,
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.probe.dropped.store(true, Ordering::SeqCst);
        assert!(!self.panic_drop, "reader IO drop panic");
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        this.probe.reads.fetch_add(1, Ordering::SeqCst);
        match this.reads.pop_front().unwrap_or(Read::Pending) {
            Read::Data(bytes) => {
                let n = bytes.len().min(buf.remaining());
                buf.put_slice(&bytes[..n]);
                if n < bytes.len() {
                    this.reads.push_front(Read::Data(bytes[n..].to_vec()));
                }
                Poll::Ready(Ok(()))
            }
            Read::Error(code) => Poll::Ready(Err(std::io::Error::from_raw_os_error(code))),
            Read::Eof => Poll::Ready(Ok(())),
            Read::Pending => Poll::Pending,
            Read::Panic => {
                buf.put_slice(&[9]);
                panic!("read filled buffer then panicked");
            }
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Fixture {
    request: ReaderRequest,
    back: mpsc::Receiver<(SocketHandle, RelayEvent)>,
    signals: mpsc::Receiver<RelayReaderSignal>,
    stop: oneshot::Sender<()>,
    credit: watch::Sender<RelayReadCredit>,
    probe: Arc<Probe>,
}

fn fixture(reads: Vec<Read>, capacity: usize, panic_drop: bool) -> Fixture {
    let (fixture, writer) = fixture_with_writer(reads, capacity, panic_drop);
    drop(writer);
    fixture
}

fn fixture_with_writer(
    reads: Vec<Read>,
    capacity: usize,
    panic_drop: bool,
) -> (Fixture, tokio::io::WriteHalf<RelayStream>) {
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    let probe = Arc::new(Probe::default());
    let stream: RelayStream = Box::new(Stream {
        reads: reads.into(),
        probe: probe.clone(),
        panic_drop,
    });
    let (remote_reader, writer) = tokio::io::split(stream);
    let (back_tx, back) = mpsc::channel(capacity);
    let (signal_tx, signals) = mpsc::channel(8);
    let (credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let (stop, stop_rx) = oneshot::channel();
    (
        Fixture {
            request: ReaderRequest {
                handle,
                epoch: 7,
                remote_reader,
                back_tx,
                read_credit_rx,
                signal_tx,
                local_finish_seen: Arc::new(AtomicBool::new(false)),
                stop_rx,
            },
            back,
            signals,
            stop,
            credit,
            probe,
        },
        writer,
    )
}

fn data(event: (SocketHandle, RelayEvent)) -> Vec<u8> {
    match event.1 {
        RelayEvent::Data {
            epoch: 7, bytes, ..
        } => bytes,
        _ => panic!("expected actual reader DATA"),
    }
}

#[tokio::test]
async fn read_error_survives_successful_data_publication() {
    let mut f = fixture(
        vec![Read::Data(vec![1, 2]), Read::Data(vec![3]), Read::Error(54)],
        8,
        false,
    );
    let mut outcome = run_custodied_relay_reader(f.request).await;
    assert_eq!(data(f.back.recv().await.unwrap()), vec![1, 2, 3]);
    assert_eq!(outcome.failure.as_ref().unwrap().raw_os_error(), Some(54));
    assert_eq!(outcome.terminal, ReaderTerminal::ReadFailed);
    assert_eq!(outcome.remainder.publication.bytes, 3);
    assert_eq!(outcome.remainder.publication.messages, 1);
    assert!(matches!(
        f.signals.recv().await,
        Some(RelayReaderSignal::Progress { .. })
    ));
    assert!(matches!(
        f.signals.recv().await,
        Some(RelayReaderSignal::Closed {
            reason: "remote_read_failed",
            ..
        })
    ));
    assert!(f.probe.dropped.load(Ordering::SeqCst));
    let receipt = outcome.remainder.discard_local().unwrap();
    assert_eq!(receipt.unpublished_bytes, 0);
    assert_eq!(outcome.remainder.discard_local(), Some(receipt));
}

#[tokio::test]
async fn read_error_and_pending_data_survive_global_channel_failure() {
    let f = fixture(vec![Read::Data(vec![4, 5, 6]), Read::Error(54)], 8, false);
    drop(f.back);
    let mut outcome = run_custodied_relay_reader(f.request).await;
    assert_eq!(outcome.failure.as_ref().unwrap().raw_os_error(), Some(54));
    assert_eq!(outcome.terminal, ReaderTerminal::GlobalClosed);
    assert_eq!(outcome.remainder.batch.payload, vec![4, 5, 6]);
    assert_eq!(
        outcome.remainder.publication.pending.as_deref(),
        Some(&[4, 5, 6][..])
    );
    assert_eq!(outcome.remainder.publication.bytes, 0);
    assert_eq!(
        outcome.remainder.discard_local().unwrap().unpublished_bytes,
        3
    );
}

#[tokio::test]
async fn pressure_stop_retains_only_unpublished_logical_tail_and_all_local_copies() {
    let mut f = fixture(
        vec![Read::Data(vec![3; 200_000]), Read::Error(54)],
        1,
        false,
    );
    let mut task = Box::pin(run_custodied_relay_reader(f.request));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(task.as_mut().poll(&mut cx).is_pending());
    assert_eq!(f.back.len(), 1);
    // Resume past the segmentation yield into the actual second reserve wait.
    assert!(task.as_mut().poll(&mut cx).is_pending());
    f.stop.send(()).unwrap();
    let mut outcome = task.await;
    assert_eq!(outcome.terminal, ReaderTerminal::Stopped);
    assert_eq!(outcome.failure.as_ref().unwrap().raw_os_error(), Some(54));
    assert_eq!(outcome.remainder.publication.cursor, 131_072);
    assert_eq!(
        outcome.remainder.published(),
        PublishedReceipt {
            messages: 1,
            bytes: 131_072
        }
    );
    assert_eq!(
        outcome
            .remainder
            .publication
            .pending
            .as_ref()
            .unwrap()
            .len(),
        68_928
    );
    // Reader completion has NOT disposed the actual first global message.
    assert_eq!(data(f.back.recv().await.unwrap()), vec![3; 131_072]);
    assert!(f.back.recv().await.is_none());
    let receipt = outcome.remainder.discard_local().unwrap();
    assert_eq!(receipt.unpublished_bytes, 68_928);
    assert_eq!(receipt.batch_bytes, 200_000);
    assert_eq!(receipt.segment_bytes, 68_928);
    assert_eq!(outcome.remainder.discard_local(), Some(receipt));
}

#[tokio::test]
async fn closed_notification_pressure_is_stoppable_without_losing_read_error() {
    let f = fixture(vec![Read::Error(54)], 8, false);
    for _ in 0..8 {
        f.request
            .signal_tx
            .try_send(RelayReaderSignal::Progress {
                diag: RelayTaskDiag::default(),
            })
            .unwrap();
    }
    let mut task = Box::pin(run_custodied_relay_reader(f.request));
    assert!(
        task.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    f.stop.send(()).unwrap();
    let mut outcome = task.await;
    assert_eq!(outcome.failure.as_ref().unwrap().raw_os_error(), Some(54));
    assert_eq!(outcome.terminal, ReaderTerminal::Stopped);
    assert!(!outcome.notification.closed_sent);
    assert!(!outcome.notification.in_flight);
    assert_eq!(f.signals.len(), 8);
    assert_eq!(
        outcome.remainder.discard_local().unwrap().unpublished_bytes,
        0
    );
}

struct PanicWake(AtomicBool);
impl std::task::Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            !self.0.swap(false, Ordering::SeqCst),
            "publication wake panic"
        );
    }
}

fn panic_waker() -> Waker {
    Waker::from(Arc::new(PanicWake(AtomicBool::new(true))))
}

#[tokio::test]
async fn data_send_wake_panic_retains_unknown_span_and_never_certifies_disposal() {
    let mut f = fixture(vec![Read::Data(vec![1, 2, 3]), Read::Error(54)], 8, false);
    let waker = panic_waker();
    assert!(
        f.back
            .poll_recv(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut outcome = run_custodied_relay_reader(f.request).await;
    assert_eq!(outcome.failure.as_ref().unwrap().raw_os_error(), Some(54));
    assert!(outcome.panics[0].is_some());
    assert_eq!(
        outcome.remainder.publication.in_flight,
        Some(Span { start: 0, end: 3 })
    );
    assert_eq!(
        outcome.remainder.published(),
        PublishedReceipt {
            messages: 0,
            bytes: 0
        }
    );
    assert!(outcome.remainder.publication.pending.is_none());
    assert_eq!(data(f.back.recv().await.unwrap()), vec![1, 2, 3]);
    // Tokio Permit::send unwinds before mem::forget: its Drop returns a permit
    // already represented by queued DATA. Even consuming that DATA cannot prove
    // a healthy channel. Retain the secondary debug assertion as real evidence.
    let terminal_poll = catch_unwind(AssertUnwindSafe(|| {
        f.back.poll_recv(&mut Context::from_waker(Waker::noop()))
    }));
    if cfg!(debug_assertions) {
        assert!(terminal_poll.is_err());
    } else {
        assert!(matches!(terminal_poll, Ok(Poll::Ready(None))));
    }
    assert!(outcome.remainder.discard_local().is_none());
    assert!(outcome.remainder.discard_local().is_none());
}

#[tokio::test]
async fn notification_panic_after_data_commit_does_not_rewind_publication() {
    let mut f = fixture(vec![Read::Data(vec![5, 6]), Read::Error(54)], 8, false);
    let waker = panic_waker();
    assert!(
        f.signals
            .poll_recv(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut outcome = run_custodied_relay_reader(f.request).await;
    assert_eq!(outcome.failure.as_ref().unwrap().raw_os_error(), Some(54));
    assert!(outcome.panics[0].is_some());
    assert!(outcome.notification.in_flight);
    assert_eq!(
        outcome.remainder.published(),
        PublishedReceipt {
            messages: 1,
            bytes: 2
        }
    );
    assert_eq!(outcome.remainder.publication.cursor, 2);
    assert_eq!(outcome.remainder.publication.in_flight, None);
    assert_eq!(data(f.back.recv().await.unwrap()), vec![5, 6]);
    assert!(f.back.recv().await.is_none());
    assert!(matches!(
        f.signals.recv().await,
        Some(RelayReaderSignal::Progress { .. })
    ));
    assert!(outcome.remainder.discard_local().is_none());
}

#[tokio::test]
async fn read_panic_keeps_scratch_and_unknown_read_debt() {
    let f = fixture(vec![Read::Panic], 8, false);
    let mut outcome = run_custodied_relay_reader(f.request).await;
    assert!(outcome.panics[0].is_some());
    assert!(outcome.remainder.reading);
    assert_eq!(outcome.remainder.scratch.as_ref().unwrap()[0], 9);
    // read() did not return its local ReadBuf length. Zero is not a zero-read receipt.
    assert_eq!(outcome.remainder.valid, 0);
    assert!(outcome.remainder.quarantined());
    assert!(outcome.remainder.discard_local().is_none());
    assert!(f.probe.dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn io_drop_panic_keeps_original_error_and_pending_batch() {
    let f = fixture(vec![Read::Data(vec![7; 5]), Read::Error(54)], 8, true);
    drop(f.back);
    let mut outcome = run_custodied_relay_reader(f.request).await;
    assert_eq!(outcome.failure.as_ref().unwrap().raw_os_error(), Some(54));
    assert!(outcome.panics[2].is_some());
    assert_eq!(outcome.remainder.batch.payload, vec![7; 5]);
    assert_eq!(
        outcome.remainder.publication.pending.as_deref(),
        Some(&[7; 5][..])
    );
    assert!(outcome.remainder.discard_local().is_none());
    assert!(f.probe.dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn exact_publication_totals_survive_multiple_successful_batches() {
    let mut f = fixture(
        vec![
            Read::Data(vec![1, 2]),
            Read::Pending,
            Read::Data(vec![3, 4, 5]),
            Read::Eof,
        ],
        8,
        false,
    );
    let mut outcome = run_custodied_relay_reader(f.request).await;
    assert_eq!(outcome.terminal, ReaderTerminal::Eof);
    assert!(outcome.failure.is_none());
    assert!(outcome.notification.closed_sent);
    assert_eq!(data(f.back.recv().await.unwrap()), vec![1, 2]);
    assert_eq!(data(f.back.recv().await.unwrap()), vec![3, 4, 5]);
    assert_eq!(
        outcome.remainder.published(),
        PublishedReceipt {
            messages: 2,
            bytes: 5
        }
    );
    assert_eq!(outcome.remainder.publication.cursor, 3);
    assert_eq!(
        outcome.remainder.discard_local().unwrap().unpublished_bytes,
        0
    );
}

#[tokio::test]
async fn stopped_credit_wait_performs_no_remote_read() {
    let f = fixture(vec![Read::Data(vec![1])], 8, false);
    f.credit
        .send(RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        })
        .unwrap();
    let mut task = Box::pin(run_custodied_relay_reader(f.request));
    assert!(
        task.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert_eq!(f.probe.reads.load(Ordering::SeqCst), 0);
    f.stop.send(()).unwrap();
    let mut outcome = task.await;
    assert_eq!(outcome.terminal, ReaderTerminal::Stopped);
    assert_eq!(
        outcome.remainder.discard_local().unwrap().unpublished_bytes,
        0
    );
}

#[tokio::test]
async fn publication_counter_overflow_preserves_pending_vec_without_sending() {
    let mut f = fixture(vec![], 8, false);
    for (messages, bytes) in [(usize::MAX, 0), (0, usize::MAX)] {
        let mut publication = Publication {
            pending: Some(vec![1]),
            messages,
            bytes,
            ..Publication::default()
        };
        let mut diag = RelayTaskDiag::default();
        assert_eq!(
            publication
                .send(
                    &mut Dispatch {
                        handle: f.request.handle,
                        epoch: 7,
                        back_tx: &f.request.back_tx,
                        diag: &mut diag,
                        stop: None,
                        threshold: std::time::Duration::from_millis(5),
                    },
                    1
                )
                .await,
            Err(ReaderTerminal::AccountingFailed)
        );
        assert_eq!(publication.pending, Some(vec![1]));
        assert_eq!(publication.in_flight, None);
        assert_eq!(publication.cursor, 0);
        assert_eq!((publication.messages, publication.bytes), (messages, bytes));
        assert!(f.back.try_recv().is_err());
    }
}

#[tokio::test]
async fn legacy_drops_io_and_controls_before_output_is_observed_and_resumes_panic() {
    for panic_drop in [false, true] {
        let f = fixture(vec![Read::Error(54)], 8, panic_drop);
        let task = tokio::spawn(run_relay_reader(f.request));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !task.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(f.probe.dropped.load(Ordering::SeqCst));
        assert!(f.back.is_closed());
        assert!(f.signals.is_closed());
        assert_eq!(f.credit.receiver_count(), 0);
        if panic_drop {
            assert!(task.await.unwrap_err().is_panic());
        } else {
            task.await.unwrap();
        }
    }
}

#[tokio::test]
async fn closed_send_wake_panic_keeps_first_error_and_unknown_notification() {
    let mut f = fixture(vec![Read::Error(54)], 8, false);
    let waker = panic_waker();
    assert!(
        f.signals
            .poll_recv(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let mut outcome = run_custodied_relay_reader(f.request).await;
    assert_eq!(outcome.failure.as_ref().unwrap().raw_os_error(), Some(54));
    assert!(outcome.panics[0].is_some());
    assert!(outcome.notification.in_flight);
    assert!(!outcome.notification.closed_sent);
    assert!(matches!(
        f.signals.recv().await,
        Some(RelayReaderSignal::Closed {
            reason: "remote_read_failed",
            ..
        })
    ));
    assert!(outcome.remainder.discard_local().is_none());
}

#[tokio::test]
async fn reader_join_does_not_retire_a_carrier_with_a_live_writer_half() {
    let (f, writer) = fixture_with_writer(vec![Read::Eof], 8, false);
    let mut outcome = run_custodied_relay_reader(f.request).await;
    assert_eq!(outcome.terminal, ReaderTerminal::Eof);
    assert!(outcome.panics.iter().all(Option::is_none));
    assert!(outcome.remainder.discard_local().is_some());
    assert!(!f.probe.dropped.load(Ordering::SeqCst));
    drop(writer);
    assert!(f.probe.dropped.load(Ordering::SeqCst));
}

mod window;

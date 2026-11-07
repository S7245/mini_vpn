//! Real Quinn tests for the same-lock prefix read. No fabricated chunks,
//! provider or UR state: the existing endpoint fixture and real peer own IO.
use crate::tuic::tcp_fixture::UdpPair;
use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

struct Pipe {
    recv: quinn::RecvStream,
    send: quinn::SendStream,
    _client_send: quinn::SendStream,
    _peer_recv: quinn::RecvStream,
}
impl Pipe {
    async fn open(pair: &UdpPair) -> Self {
        let (mut client_send, recv) = pair.connection.open_bi().await.unwrap();
        client_send.write_all(b"q").await.unwrap();
        let (send, mut peer_recv) = pair.peer.accept_bi().await.unwrap();
        let mut marker = [0];
        peer_recv.read_exact(&mut marker).await.unwrap();
        assert_eq!(marker, *b"q");
        Self {
            recv,
            send,
            _client_send: client_send,
            _peer_recv: peer_recv,
        }
    }
}
async fn at(
    recv: &mut quinn::RecvStream,
    offset: u64,
    max: usize,
) -> Result<Option<quinn::Chunk>, quinn::ReadError> {
    poll_fn(|cx| recv.poll_read_chunk_at(cx, offset, max)).await
}
async fn captured<F: Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = Box::pin(work);
    poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(error) => Poll::Ready(Err(error)),
        }
    })
    .await
}
fn conclude(
    result: std::thread::Result<Result<(), tokio::time::error::Elapsed>>,
    cleanup: std::thread::Result<()>,
) {
    match result {
        Err(error) => std::panic::resume_unwind(error),
        Ok(Err(error)) => panic!("original single test5 expired: {error}"),
        Ok(Ok(())) => (),
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
}

#[derive(Default)]
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_quinn_offset_gate_retains_bytes_then_partial_prefix_zero_and_eof() {
    let limit = tokio::time::Instant::now() + Duration::from_secs(5);
    let pair = tokio::time::timeout_at(limit, UdpPair::new(8, 65536))
        .await
        .unwrap();
    let result = captured(tokio::time::timeout_at(limit, async {
        let mut pipe = Pipe::open(&pair).await;
        pipe.send.write_all(b"abcdef").await.unwrap();
        pipe.send.finish().unwrap();
        // A local open reserves an unmaterialized receive slot. Enter the
        // original read transaction before observing it; expected7 cannot
        // consume any byte from this actual six-byte stream.
        let mut initialize = Box::pin(at(&mut pipe.recv, 7, 2));
        assert!(
            initialize
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(initialize);
        let progress = pipe.recv.progress_handle();
        loop {
            if progress.sample().unwrap().buffered_bytes == 6 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let before = progress.sample().unwrap();
        assert_eq!(before.read_offset, 0);
        let mut mismatched = Box::pin(at(&mut pipe.recv, 1, 2));
        assert!(
            mismatched
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(mismatched);
        assert_eq!(
            progress.sample().unwrap(),
            before,
            "no consumption or read credit on mismatch"
        );
        let zero = at(&mut pipe.recv, 0, 0).await.unwrap().unwrap();
        assert_eq!(zero.offset, 0);
        assert!(zero.bytes.is_empty());
        assert_eq!(
            progress.sample().unwrap(),
            before,
            "zero length consumes nothing"
        );
        let mut output = [0; 6];
        let mut offset = 0;
        while offset < output.len() {
            let part = at(&mut pipe.recv, offset as u64, 2).await.unwrap().unwrap();
            assert_eq!(part.offset, offset as u64);
            assert!((1..=2).contains(&part.bytes.len()));
            let end = offset + part.bytes.len();
            output[offset..end].copy_from_slice(&part.bytes);
            offset = end;
        }
        assert_eq!(&output, b"abcdef");
        assert!(at(&mut pipe.recv, 6, 2).await.unwrap().is_none());
        assert_eq!(pipe.send.stopped().await.unwrap(), None);
        println!(
            "B71 actualQuinn mismatch/zero no credit; partial<=2 exactabcdef then real FIN/ACK"
        );
    }))
    .await;
    let cleanup = captured(pair.close()).await;
    conclude(result, cleanup);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_quinn_pending_borrow_cancel_preserves_data_and_original_waker() {
    let limit = tokio::time::Instant::now() + Duration::from_secs(5);
    let pair = tokio::time::timeout_at(limit, UdpPair::new(8, 65536))
        .await
        .unwrap();
    let result = captured(tokio::time::timeout_at(limit, async {
        let mut pipe = Pipe::open(&pair).await;
        let progress = pipe.recv.progress_handle();
        let counter = Arc::new(CountWake::default()); let waker = Waker::from(counter.clone());
        let mut borrowed = Box::pin(at(&mut pipe.recv, 0, 3));
        assert!(borrowed.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        let before = progress.sample().unwrap();
        assert_eq!(before.buffered_bytes, 0);
        assert_eq!(before.read_offset, 0);
        assert_eq!(before.next_received_offset, None);
        drop(borrowed);
        assert_eq!(progress.sample().unwrap(), before);
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
        pipe.send.write_all(b"new").await.unwrap(); pipe.send.finish().unwrap();
        loop {
            if counter.0.load(Ordering::SeqCst) > 0 && progress.sample().unwrap().buffered_bytes == 3 { break; }
            tokio::task::yield_now().await;
        }
        let mut out = [0; 3]; let mut offset = 0;
        while offset < out.len() {
            let part = at(&mut pipe.recv, offset as u64, out.len()-offset).await.unwrap().unwrap();
            assert_eq!(part.offset, offset as u64); assert!(!part.bytes.is_empty());
            let end = offset + part.bytes.len(); out[offset..end].copy_from_slice(&part.bytes); offset = end;
        }
        assert_eq!(&out, b"new"); assert!(at(&mut pipe.recv, 3, 1).await.unwrap().is_none());
        assert_eq!(pipe.send.stopped().await.unwrap(), None);
        println!("B71 actualQuinn original registered wakecount{} after real DATA; dropped Pending borrow lost0bytes",counter.0.load(Ordering::SeqCst));
    })).await;
    let cleanup = captured(pair.close()).await;
    conclude(result, cleanup);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_quinn_prefix_reset_and_connection_error_are_not_eof() {
    let limit = tokio::time::Instant::now() + Duration::from_secs(5);
    let pair = tokio::time::timeout_at(limit, UdpPair::new(8, 65536))
        .await
        .unwrap();
    let result = captured(tokio::time::timeout_at(limit, async {
        let mut reset = Pipe::open(&pair).await;
        let mut borrowed = Box::pin(at(&mut reset.recv, 0, 4));
        assert!(borrowed.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending()); drop(borrowed);
        reset.send.reset(77u32.into()).unwrap();
        let error = at(&mut reset.recv, 9, 4).await.unwrap_err();
        assert!(matches!(error,quinn::ReadError::Reset(code) if code==77u32.into()));
        let mut lost = Pipe::open(&pair).await;
        let mut borrowed = Box::pin(at(&mut lost.recv, 0, 4));
        assert!(borrowed.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending()); drop(borrowed);
        pair.peer.close(41u32.into(), b"original prefix IO");
        let closed = pair.connection.closed().await;
        assert!(matches!(closed,quinn::ConnectionError::ApplicationClosed(ref c) if c.error_code==41u32.into() && c.reason.as_ref()==b"original prefix IO"));
        let error = at(&mut lost.recv, 9, 4).await.unwrap_err();
        assert!(matches!(error,quinn::ReadError::ConnectionLost(quinn::ConnectionError::ApplicationClosed(ref c)) if c.error_code==41u32.into() && c.reason.as_ref()==b"original prefix IO"));
        println!("B71 actualQuinn RESET77/ConnectionLost(ApplicationClosed41,original reason) survive wrong expected offset, no EOF");
    })).await;
    let cleanup = captured(pair.close()).await;
    conclude(result, cleanup);
}

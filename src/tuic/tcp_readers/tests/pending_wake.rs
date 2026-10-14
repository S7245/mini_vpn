use super::super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Wake, Waker};

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn lease() -> TcpPoolSlotLease {
    let (lease, reserved) = TcpPoolSlotLease::reserve(Arc::new(AtomicU64::new(0)));
    assert!(reserved);
    lease
}

#[tokio::test(start_paused = true)]
async fn dropping_reader_cancels_its_pending_wake() {
    let (_peer, inner) = tokio::io::duplex(16);
    let mut reader = TrackedRelayStream::new(inner, lease(), None);
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    let cx = &mut Context::from_waker(&waker);
    reader.arm_pending_self_wake(cx, tokio::time::Instant::now().into_std());
    tokio::task::yield_now().await;
    drop(reader);
    tokio::time::advance(Duration::from_millis(4)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        counter.0.load(Ordering::SeqCst),
        0,
        "reader Drop must deregister its timer"
    );
}

fn counter() -> (Arc<WakeCount>, Waker) {
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    (counter, waker)
}

async fn tick() {
    tokio::time::advance(Duration::from_millis(4)).await;
    tokio::task::yield_now().await;
}

#[tokio::test(start_paused = true)]
async fn timer_refreshes_waker_fires_once_and_reuses_the_same_slot() {
    let mut timer = PendingReadWake::default();
    let (old, old_waker) = counter();
    let (new, new_waker) = counter();
    let now = tokio::time::Instant::now().into_std();
    timer.arm_and_poll(&mut Context::from_waker(&old_waker), now);
    assert_eq!(timer.deadline(), Some(now + Duration::from_millis(2)));
    let address = timer.slot.as_ref().unwrap().as_ref().get_ref() as *const _;
    for _ in 0..32 {
        assert!(!timer.poll_fired(&mut Context::from_waker(&old_waker)));
    }
    assert_eq!(old.0.load(Ordering::SeqCst), 0, "pending polls do not spin");
    assert!(!timer.poll_fired(&mut Context::from_waker(&new_waker)));
    tick().await;
    assert_eq!(
        old.0.load(Ordering::SeqCst),
        0,
        "superseded waker is not retained"
    );
    assert_eq!(new.0.load(Ordering::SeqCst), 1);
    assert!(timer.poll_fired(&mut Context::from_waker(&new_waker)));
    assert!(!timer.poll_fired(&mut Context::from_waker(&new_waker)));
    assert_eq!(timer.deadline(), None);
    for _ in 0..32 {
        timer.arm_and_poll(
            &mut Context::from_waker(&old_waker),
            tokio::time::Instant::now().into_std(),
        );
        assert_eq!(
            timer.slot.as_ref().unwrap().as_ref().get_ref() as *const _,
            address
        );
        timer.cancel();
    }
    tick().await;
    assert_eq!(
        old.0.load(Ordering::SeqCst),
        0,
        "canceled episodes cannot wake later"
    );
    assert_eq!(new.0.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn an_already_elapsed_arm_schedules_one_observation() {
    let mut timer = PendingReadWake::default();
    let (count, waker) = counter();
    let mut cx = Context::from_waker(&waker);
    timer.arm_and_poll(
        &mut cx,
        tokio::time::Instant::now().into_std() - Duration::from_secs(1),
    );
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert!(timer.poll_fired(&mut cx));
    assert!(!timer.poll_fired(&mut cx));
    tick().await;
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
}

enum ReadOutcome {
    Data,
    Empty,
    Eof,
    Error,
    TooLong,
    WrongOffset,
}

impl ReadOutcome {
    fn chunk(&self) -> io::Result<Option<NativeTcpChunk>> {
        match self {
            Self::Eof => Ok(None),
            Self::Error => Err(io::Error::other("reader terminal")),
            _ => Ok(Some(NativeTcpChunk {
                offset: u64::from(matches!(self, Self::WrongOffset)),
                bytes: match self {
                    Self::Empty => bytes::Bytes::new(),
                    Self::TooLong => bytes::Bytes::from_static(b"xx"),
                    _ => bytes::Bytes::from_static(b"x"),
                },
            })),
        }
    }
}

struct ScriptedRead(ReadOutcome);

impl AsyncRead for ScriptedRead {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        match self.0 {
            ReadOutcome::Data => {
                buf.put_slice(b"x");
                Poll::Ready(Ok(()))
            }
            ReadOutcome::Error => Poll::Ready(Err(io::Error::other("reader terminal"))),
            _ => Poll::Ready(Ok(())),
        }
    }
}

impl DirectOrderedNativeChunkRecv for ScriptedRead {
    fn poll_read_ordered_native_chunk(
        &mut self,
        _: &mut Context<'_>,
        _: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
        Poll::Ready(self.0.chunk())
    }
}

#[tokio::test(start_paused = true)]
async fn generic_data_eof_error_cancel_but_zero_capacity_does_not() {
    for outcome in [ReadOutcome::Data, ReadOutcome::Eof, ReadOutcome::Error] {
        let mut reader = TrackedRelayStream::new(ScriptedRead(outcome), lease(), None);
        let (count, waker) = counter();
        let mut cx = Context::from_waker(&waker);
        reader.arm_pending_self_wake(&mut cx, tokio::time::Instant::now().into_std());
        let mut empty = [];
        assert!(
            Pin::new(&mut reader)
                .poll_read(&mut cx, &mut ReadBuf::new(&mut empty))
                .is_ready()
        );
        assert!(
            reader.pending_self_wake.deadline().is_some(),
            "no-capacity read is not EOF"
        );
        let mut bytes = [0; 1];
        assert!(
            Pin::new(&mut reader)
                .poll_read(&mut cx, &mut ReadBuf::new(&mut bytes))
                .is_ready()
        );
        assert!(reader.pending_self_wake.deadline().is_none());
        tick().await;
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn direct_native_all_terminal_paths_cancel_but_empty_chunk_does_not() {
    for outcome in [
        ReadOutcome::Data,
        ReadOutcome::Eof,
        ReadOutcome::Error,
        ReadOutcome::TooLong,
        ReadOutcome::WrongOffset,
        ReadOutcome::Empty,
    ] {
        let empty = matches!(outcome, ReadOutcome::Empty);
        let mut reader =
            TuicNativeOrderedReader::from_recv(ScriptedRead(outcome), lease(), None, None, None);
        let (count, waker) = counter();
        let mut cx = Context::from_waker(&waker);
        reader.arm_pending_self_wake(&mut cx, tokio::time::Instant::now().into_std());
        assert!(reader.poll_read_chunk(&mut cx, 1).is_ready());
        assert_eq!(reader.pending_self_wake.deadline().is_some(), empty);
        drop(reader);
        tick().await;
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
    }
}

struct ChunkSequence(std::collections::VecDeque<io::Result<Option<NativeTcpChunk>>>);

#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for ChunkSequence {
    async fn read_ordered_native_chunk(&mut self, _: usize) -> io::Result<Option<NativeTcpChunk>> {
        match self.0.pop_front() {
            Some(result) => result,
            None => std::future::pending().await,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn pump_native_immediate_and_buffered_terminals_cancel_owned_wake() {
    for buffered in [false, true] {
        for outcome in [ReadOutcome::Eof, ReadOutcome::Error] {
            let mut items = std::collections::VecDeque::new();
            if buffered {
                items.push_back(ReadOutcome::Data.chunk());
            }
            items.push_back(outcome.chunk());
            let expected = items.len();
            let mut reader = TuicNativeOrderedPumpReader::spawn_from_source(
                ChunkSequence(items),
                lease(),
                None,
                None,
            );
            for _ in 0..8 {
                if reader.rx.len() == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                reader.rx.len(),
                expected,
                "real pump must publish actual source outcomes"
            );
            let (count, waker) = counter();
            let mut cx = Context::from_waker(&waker);
            if buffered {
                reader.arm_pending_self_wake(&mut cx, tokio::time::Instant::now().into_std());
                assert!(reader.poll_read_chunk(&mut cx, 8).is_ready());
                assert!(
                    reader.pending_self_wake.deadline().is_none(),
                    "data progress cancels"
                );
                assert!(reader.pending_eof || reader.pending_error.is_some());
            }
            reader.arm_pending_self_wake(&mut cx, tokio::time::Instant::now().into_std());
            assert!(reader.poll_read_chunk(&mut cx, 8).is_ready());
            assert!(reader.pending_self_wake.deadline().is_none());
            (&mut reader.task)
                .await
                .expect("terminal source ends the actual pump");
            drop(reader);
            tick().await;
            assert_eq!(count.0.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn real_unordered_reader_arms_from_transport_then_clears_data_eof_and_reset() {
    let (server, client, addr) = crate::tuic::d16_quinn_test_endpoints();
    let (conn, peer) = tokio::join!(
        async { client.connect(addr, "localhost").unwrap().await.unwrap() },
        async { server.accept().await.unwrap().await.unwrap() },
    );
    for kind in 0..3 {
        let (mut send, recv) = conn.open_bi().await.unwrap();
        send.write_all(b"open").await.unwrap();
        let (mut peer_send, mut peer_recv) = peer.accept_bi().await.unwrap();
        let mut request = [0; 4];
        peer_recv.read_exact(&mut request).await.unwrap();
        assert_eq!(request, *b"open");
        let target = crate::shared::TargetAddr::parse("127.0.0.1:1").unwrap();
        let meta = crate::tuic::TuicTcpStreamDiagMeta::new(
            &target,
            0,
            conn.stable_id(),
            recv.id().index(),
        );
        let mut reader = TuicNativeTcpReader::new(
            recv,
            lease(),
            Some(TuicTcpStreamDiag::new(meta, Instant::now())),
            conn.clone(),
        );
        let (_, waker) = counter();
        assert!(
            reader
                .poll_read_chunk(&mut Context::from_waker(&waker), 16)
                .is_pending()
        );
        assert!(
            reader.pending_self_wake.deadline().is_some(),
            "real RX evidence arms the production pending branch"
        );
        match kind {
            0 => {
                peer_send.write_all(b"x").await.unwrap();
            }
            1 => {
                peer_send.finish().unwrap();
            }
            _ => {
                peer_send.reset(quinn::VarInt::from_u32(1)).unwrap();
            }
        }
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 16)),
        )
        .await
        .expect("local stream makes progress");
        match kind {
            0 => assert_eq!(
                result.unwrap().unwrap().bytes,
                bytes::Bytes::from_static(b"x")
            ),
            1 => assert!(result.unwrap().is_none()),
            _ => assert!(result.is_err()),
        }
        assert!(reader.pending_self_wake.deadline().is_none());
        drop(reader);
        assert!(
            conn.close_reason().is_none(),
            "stream terminal does not close shared connection"
        );
    }
    conn.close(quinn::VarInt::from_u32(0), b"done");
    peer.close(quinn::VarInt::from_u32(0), b"done");
    drop((conn, peer));
    server.wait_idle().await;
    client.wait_idle().await;
}

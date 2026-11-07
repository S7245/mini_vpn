mod cold;
mod lifecycle;
mod physical;
mod physical_reader;
mod source;

use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Wake, Waker};

struct SequenceOrderedSource {
    calls: Arc<AtomicU64>,
    chunks: Vec<NativeTcpChunk>,
    meter: Option<bytes::BackingMeter>,
}

#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for SequenceOrderedSource {
    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        self.meter.clone()
    }

    async fn read_ordered_native_chunk(
        &mut self,
        _max_len: usize,
    ) -> io::Result<Option<NativeTcpChunk>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) as usize;
        if call < self.chunks.len() {
            Ok(Some(self.chunks[call].clone()))
        } else {
            Ok(None)
        }
    }
}

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

fn counter() -> (Arc<WakeCount>, Waker) {
    let counter = Arc::new(WakeCount::default());
    let waker = Waker::from(counter.clone());
    (counter, waker)
}

async fn tick() {
    tokio::time::advance(Duration::from_millis(4)).await;
    tokio::task::yield_now().await;
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
            let (mut reader, prepared) = TuicNativeOrderedPumpReader::prepare_from_source(
                ChunkSequence(items),
                lease(),
                None,
                None,
            );
            let mut pump = prepared.commit(None);
            for _ in 0..8 {
                if reader.rx.as_ref().unwrap().len() == expected {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                reader.rx.as_ref().unwrap().len(),
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
            drop(reader);
            pump.settle()
                .await
                .expect("terminal source actually joins and disposes");
            tick().await;
            assert_eq!(count.0.load(Ordering::SeqCst), 0);
        }
    }
}

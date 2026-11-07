//! Actual Source actor retaining its original joint grant across a gap.
use super::*;
use std::{future::Future, task::{Context, Poll}};
use crate::upstream::{NativeTcpChunk, NativeTcpReader};

struct GapReader {
    chunk: Option<NativeTcpChunk>,
    credit: watch::Sender<RelayReadCredit>,
    reads: Arc<std::sync::Mutex<Vec<usize>>>,
}
impl NativeTcpReader for GapReader {
    fn poll_read_chunk(&mut self, _: &mut Context<'_>, max: usize)
        -> Poll<std::io::Result<Option<NativeTcpChunk>>>
    {
        self.reads.lock().unwrap().push(max);
        if let Some(chunk) = self.chunk.take() {
            self.credit.send(RelayReadCredit { paused: false, max_batch_bytes: 2 }).unwrap();
            Poll::Ready(Ok(Some(chunk)))
        } else { Poll::Pending }
    }
    fn source_read_bytes(&self) -> Option<usize> { Some(if self.chunk.is_none() { 4 } else { 0 }) }
}

#[tokio::test(flavor = "current_thread")]
async fn source_retained_gap_grant_obeys_current_read_credit() {
    use crate::shared::errors::*;
    let budget = bytes::BackingBudget::new(1 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let facts = Arc::new(cancellation::Facts::default());
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let clock = Arc::new(FailureOrder::default());
    clock.bind(Arc::downgrade(&sink), FailureOrigin { phase: FailurePhase::TcpOpen,
        resource: FailureResource::TcpOpen, resource_id: 0, attempt_id: Some(1) }, Some(meter.clone())).unwrap();
    let (shared, stock) = Shared::prepare_native(TcpRelayEngine::ContinuousPump,
        DownlinkBackpressureConfig::default(), Some(meter.clone()), clock.clone()).unwrap();
    let mut stock = stock.unwrap();
    let producer = stock.take_producer().unwrap();
    let mut cold = Cold::prepare(&meter, clock.clone()).unwrap();
    let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
    let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8])));
    let (credit_tx, credit) = watch::channel(RelayReadCredit { paused: false, max_batch_bytes: 8 });
    let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
    let reader = GapReader { chunk: Some(NativeTcpChunk { offset: 4,
        bytes: bytes::Bytes::copy_from_slice_metered(b"tail", &meter).unwrap() }),
        credit: credit_tx, reads: reads.clone() };
    let (signals, _signals) = mpsc::channel(1);
    let (stop_tx, stop) = oneshot::channel();
    let (start, gate) = oneshot::channel();
    let mut future = Box::pin(run(Request { producer: Some(producer), backing_meter: Some(meter.clone()),
        handle, epoch: 7, reader: Some(Box::new(reader) as NativeTcpReadHalf).into(), shared: shared.clone(),
        credit: Some(credit).into(), signals: Some(signals).into(),
        local_finish: Arc::new(std::sync::atomic::AtomicBool::new(false)), stop: Some(stop).into(), service_floor: false,
    }, gate, Some(clock.clone()), cold.take_lease()));
    start.send(()).unwrap();
    assert!(future.as_mut().poll(&mut Context::from_waker(std::task::Waker::noop())).is_pending());
    let observed = reads.lock().unwrap().clone();
    stop_tx.send(()).unwrap();
    let mut outcome = future.await;
    assert!(!outcome.quarantined() && !outcome.has_error());
    assert_eq!(outcome.source_read_bytes(), Some(4));
    assert_eq!(outcome.discard_local(Some(&meter)).unwrap().unpublished_bytes, 4);
    assert!(cold.retire());
    let queue = shared.queue.bytes_queue().unwrap();
    assert!(queue.dispose_data().unwrap().is_some());
    stock.wait_returned().await.unwrap();
    assert!(stock.try_retire());
    assert!(queue.dispose_custodied().unwrap().is_some());
    drop((outcome, stock, cold, shared, clock));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert_eq!(observed, [8, 2], "the original grant remains owned, but new reads obey current credit");
}

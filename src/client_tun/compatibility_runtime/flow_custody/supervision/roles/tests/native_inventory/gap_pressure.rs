//! Deterministic pressure at the original Source/stock/assembler boundary.
//! The reader supplies real metered gap chunks; the actor and all inventories
//! are production components. True Quinn coverage remains the original Full test.
use super::*;
use crate::shared::errors::*;
use crate::upstream::{NativeTcpChunk, NativeTcpReader};
use std::{future::Future, task::{Context, Poll}};

struct Gaps {
    chunks: std::collections::VecDeque<NativeTcpChunk>,
    read: usize,
    budget: bytes::BackingBudget,
    meter: bytes::BackingMeter,
    pressure: Arc<std::sync::Mutex<Option<bytes::BytesMut>>>,
}
impl NativeTcpReader for Gaps {
    fn poll_read_chunk(&mut self, _: &mut Context<'_>, _: usize)
        -> Poll<std::io::Result<Option<NativeTcpChunk>>>
    {
        let Some(chunk) = self.chunks.pop_front() else { return Poll::Pending; };
        self.read += 1;
        if self.read == 4 {
            let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
            *self.pressure.lock().unwrap() = Some(bytes::BytesMut::try_zeroed_metered(
                self.budget.limit() - self.budget.used() - header - 423, &self.meter).unwrap());
            assert_eq!(self.budget.limit() - self.budget.used(), 423);
        }
        if self.read == 5 {
            assert!(self.budget.limit() - self.budget.used() < 423, "metadata was actually funded before fifth input");
        }
        Poll::Ready(Ok(Some(chunk)))
    }
    fn source_read_bytes(&self) -> Option<usize> { Some(self.read) }
}

#[tokio::test(flavor = "current_thread")]
async fn original_source_gap_growth_retires_idle_output_before_taking_next_input() {
    use crate::tcp_downlink_pump::OwnedPayload;
    let budget = bytes::BackingBudget::new(2 << 20).unwrap();
    let meter = bytes::BackingMeter::with_budget(&budget);
    let facts = Arc::new(cancellation::Facts::default());
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let clock = Arc::new(FailureOrder::default());
    clock.bind(Arc::downgrade(&sink), FailureOrigin { phase: FailurePhase::TcpOpen,
        resource: FailureResource::TcpOpen, resource_id: 0, attempt_id: Some(1) }, Some(meter.clone())).unwrap();
    let (shared, stock) = Shared::prepare_native(TcpRelayEngine::ContinuousPump,
        DownlinkBackpressureConfig::default(), Some(meter.clone()), clock.clone()).unwrap();
    let mut stock = stock.unwrap();
    let mut producer = stock.take_producer().unwrap();
    let mut operation = None;
    producer.prepare_read(64 << 10, &mut operation).await.unwrap();
    let mut operation = operation.unwrap();
    operation.space().as_mut().unwrap().write(&vec![1; 64 << 10]).unwrap();
    let mut payload = Some(OwnedPayload::Native(operation.space().take().unwrap().finish().unwrap()));
    operation.publish(&mut payload).await.unwrap();
    drop(operation);
    let queue = shared.queue.bytes_queue().unwrap();
    let mut bootstrap = None;
    assert!(queue.recv_custodied(64 << 10, &mut bootstrap).await.unwrap());
    let mut operation = None;
    producer.prepare_read(512 << 10, &mut operation).await.unwrap();
    drop(operation); // Cancellation retains the actual idle large allocation.
    assert_eq!(meter.snapshot().allocations, 2);
    let mut cold = Cold::prepare(&meter, clock.clone()).unwrap();
    let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
    let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8])));
    let (_credit_tx, credit) = watch::channel(RelayReadCredit { paused: false, max_batch_bytes: 8 });
    let pressure = Arc::new(std::sync::Mutex::new(None));
    let reader = Gaps { chunks: (1..=5).map(|i| NativeTcpChunk { offset: i * 2,
        bytes: bytes::Bytes::copy_from_slice_metered(b"x", &meter).unwrap() }).collect(),
        read: 0, budget: budget.clone(), meter: meter.clone(), pressure: pressure.clone() };
    let (signals, _signals) = mpsc::channel(1);
    let (stop_tx, stop) = oneshot::channel();
    let (start, gate) = oneshot::channel();
    let mut future = Box::pin(run(Request { producer: Some(producer), backing_meter: Some(meter.clone()),
        handle, epoch: 7, reader: Some(Box::new(reader) as NativeTcpReadHalf).into(), shared: shared.clone(),
        credit: Some(credit).into(), signals: Some(signals).into(),
        local_finish: Arc::new(std::sync::atomic::AtomicBool::new(false)), stop: Some(stop).into(), service_floor: false,
    }, gate, Some(clock.clone()), cold.take_lease()));
    start.send(()).unwrap();
    let polled = future.as_mut().poll(&mut Context::from_waker(std::task::Waker::noop()));
    let mut outcome = match polled {
        Poll::Ready(outcome) => { drop(future); outcome },
        Poll::Pending => { stop_tx.send(()).unwrap(); future.await }
    };
    assert!(!outcome.has_error(), "idle same-Flow output must fund gap metadata before the next input");
    assert_eq!(outcome.source_read_bytes(), Some(5));
    assert_eq!(outcome.discard_local(Some(&meter)).unwrap().unpublished_bytes, 5);
    assert!(cold.retire());
    assert!(queue.dispose_data().unwrap().is_some());
    drop(bootstrap);
    stock.wait_returned().await.unwrap();
    assert!(stock.try_retire());
    assert!(queue.dispose_custodied().unwrap().is_some());
    drop((outcome, stock, cold, shared, clock, pressure));
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

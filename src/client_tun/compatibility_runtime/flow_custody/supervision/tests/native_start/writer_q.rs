//! Original borrowed write is Pending when its Native Root disappears. The
//! resumed poll must retain the same payload and never call the writer again.
use super::*;
use crate::client_tun::compatibility_runtime::SessionControl;
use crate::client_tun::compatibility_runtime::flow_custody::transaction::{OpenPhase, OpenSlot};
use crate::shared::errors::{FailureOrder, QuietFailureSink};
struct Writer {
    flush: bool,
    calls: Arc<AtomicUsize>,
    bytes: Arc<AtomicUsize>,
}
impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if !self.flush && self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Poll::Pending;
        }
        self.bytes.fetch_add(bytes.len(), Ordering::SeqCst);
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.flush && self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn pending_native_write_cannot_publish_data_after_original_root_q() {
    for flush in [false, true] {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let owner = Arc::new({
            let mut control = SessionControl::default();
            control.app_meter = Some(meter.clone());
            control
        });
        let sink: Arc<dyn QuietFailureSink> = owner.clone();
        let clock = Arc::new(FailureOrder::default());
        clock
            .bind(
                Arc::downgrade(&sink),
                SessionControl::origin(crate::shared::errors::FailurePhase::TcpOpen),
                Some(meter),
            )
            .unwrap();
        let mut sockets = SocketSet::new(Vec::new());
        let handle = sockets.add(Socket::new(
            SocketBuffer::new(vec![0; 8]),
            SocketBuffer::new(vec![0; 8]),
        ));
        assert!(matches!(owner.opens().slots.put(0, OpenSlot {
            handle,
            epoch: 7,
            deadline: tokio::time::Instant::now(),
            phase: OpenPhase::Reserved,
        }), Ok(None)));
        let (tx, rx) = crate::client_tun::uplink::channel(2);
        let (signals, _received) = mpsc::channel(2);
        let (_stop, stop) = tokio::sync::oneshot::channel();
        tx.send(RelayCommand::Data(b"original".to_vec()))
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let bytes = Arc::new(AtomicUsize::new(0));
        let mut work = std::pin::pin!(crate::client_tun::compatibility_runtime::flow_custody::relay_writer::run_custodied_relay_writer_gated(
        handle, Writer { flush, calls: calls.clone(), bytes: bytes.clone() }, rx, signals, stop, None, Some(clock),
    ));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(work.as_mut().poll(&mut cx).is_pending());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(sink);
        drop(owner);
        assert!(budget.quarantined() && budget.used() > 0);
        let Poll::Ready(outcome) = work.as_mut().poll(&mut cx) else {
            panic!("original writer must return interrupted custody")
        };
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(bytes.load(Ordering::SeqCst), if flush { 8 } else { 0 });
        assert!(
            outcome.result.failure.is_none(),
            "Q does not invent a source IO"
        );
        assert_eq!(outcome.remainder.custody_for_test().current.unwrap().0, 8);
        assert_eq!(
            outcome.remainder.custody_for_test().current.unwrap().3,
            if flush { 8 } else { 0 }
        );
        assert!(
            !outcome.remainder.custody_for_test().flags[1],
            "no physical completion receipt"
        );
    }
}

struct ShutdownWriter(Arc<AtomicUsize>);
impl AsyncWrite for ShutdownWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        panic!("shutdown-only tracer")
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
#[tokio::test]
async fn pending_native_shutdown_cannot_repoll_opaque_writer_after_root_q() {
    for mode in 0..3 {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let owner = Arc::new({
            let mut control = SessionControl::default();
            control.app_meter = Some(meter.clone());
            control
        });
        let sink: Arc<dyn QuietFailureSink> = owner.clone();
        let clock = Arc::new(FailureOrder::default());
        clock
            .bind(
                Arc::downgrade(&sink),
                SessionControl::origin(crate::shared::errors::FailurePhase::TcpOpen),
                Some(meter),
            )
            .unwrap();
        let mut sockets = SocketSet::new(Vec::new());
        let handle = sockets.add(Socket::new(
            SocketBuffer::new(vec![0; 8]),
            SocketBuffer::new(vec![0; 8]),
        ));
        assert!(matches!(owner.opens().slots.put(0, OpenSlot {
            handle,
            epoch: 7,
            deadline: tokio::time::Instant::now(),
            phase: OpenPhase::Reserved,
        }), Ok(None)));
        let (tx, rx) = crate::client_tun::uplink::channel(2);
        let (signals, _received) = mpsc::channel(2);
        let (stop_tx, stop) = tokio::sync::oneshot::channel();
        match mode {
            0 => tx.send(RelayCommand::Finish).await.unwrap(),
            1 => drop(tx),
            _ => stop_tx.send(()).unwrap(),
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let mut work = std::pin::pin!(crate::client_tun::compatibility_runtime::flow_custody::relay_writer::run_custodied_relay_writer_gated(
            handle, ShutdownWriter(calls.clone()), rx, signals, stop, None, Some(clock),
        ));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(work.as_mut().poll(&mut cx).is_pending(), "mode {mode}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(sink);
        drop(owner);
        let Poll::Ready(outcome) = work.as_mut().poll(&mut cx) else {
            panic!("mode {mode} must retain interrupted custody")
        };
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "mode {mode}: Q must stop actual shutdown poll too"
        );
        assert!(outcome.result.failure.is_none() && outcome.result.shutdown_failure.is_none());
        assert!(budget.quarantined() && budget.used() > 0);
    }
}

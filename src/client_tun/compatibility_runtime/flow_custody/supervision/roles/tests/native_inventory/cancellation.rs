//! Original Native reader future, cancelled before its first poll. The real
//! Tokio signal sender holds an independently hostile receiver wake; an outer
//! catch cannot protect two raw fields from unwinding through each other.
use super::*;
use std::{panic::{AssertUnwindSafe, catch_unwind}, task::{Context, Poll, Wake, Waker}};

struct ReaderBomb(Arc<u8>);
impl crate::upstream::NativeTcpReader for ReaderBomb {
    fn poll_read_chunk(&mut self, _: &mut Context<'_>, _: usize) -> Poll<std::io::Result<Option<crate::upstream::NativeTcpChunk>>> {
        panic!("zero poll must never read transport")
    }
}
impl Drop for ReaderBomb {
    fn drop(&mut self) { std::panic::panic_any(self.0.clone()); }
}
struct SignalBomb(Arc<u8>);
impl Wake for SignalBomb {
    fn wake(self: Arc<Self>) { std::panic::panic_any(self.0.clone()); }
    fn wake_by_ref(self: &Arc<Self>) { std::panic::panic_any(self.0.clone()); }
}
#[derive(Default)]
pub(super) struct Facts(std::sync::Mutex<Vec<crate::shared::errors::RetainedFailure>>);
impl crate::shared::errors::QuietFailureSink for Facts {
    fn record_quiet(&self, failure: crate::shared::errors::RetainedFailure) { self.0.lock().unwrap().push(failure); }
}

#[test]
fn native_source_zero_poll_cancel_contains_independent_reader_and_signal_failures() {
    const CHILD: &str = "MINI_VPN_SOURCE_ZERO_POLL_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let name = concat!(module_path!(), "::native_source_zero_poll_cancel_contains_independent_reader_and_signal_failures");
        let name = name.strip_prefix("mini_vpn::").unwrap_or(name);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"]).env(CHILD, "1").output().unwrap();
        eprintln!("original Source zero-poll child status={:?}\n{}\n{}", output.status,
            String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success(), "original reader and signal must be independently contained before first poll");
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"), "child must run the real test");
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        use crate::shared::errors::*;
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let facts = Arc::new(Facts::default());
        let sink: Arc<dyn QuietFailureSink> = facts.clone();
        let clock = Arc::new(FailureOrder::default());
        clock.bind(Arc::downgrade(&sink), FailureOrigin { phase: FailurePhase::TcpOpen,
            resource: FailureResource::Session, resource_id: 0, attempt_id: None }, Some(meter.clone())).unwrap();
        let (shared, stock) = Shared::prepare_native(TcpRelayEngine::ContinuousPump,
            DownlinkBackpressureConfig::default(), Some(meter.clone()), clock.clone()).unwrap();
        let mut stock = stock.unwrap();
        let producer = stock.take_producer().unwrap();
        let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
        let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8])));
        let (_credit_tx, credit) = watch::channel(RelayReadCredit::default());
        let (signals, mut receiver) = mpsc::channel(1);
        let a = Arc::new(11u8);
        let b = Arc::new(22u8);
        let waker = Waker::from(Arc::new(SignalBomb(b.clone())));
        assert!(receiver.poll_recv(&mut Context::from_waker(&waker)).is_pending());
        let (_stop, stop) = oneshot::channel();
        let (_start, gate) = oneshot::channel();
        let mut source = Cold::prepare(&meter, clock.clone()).unwrap();
        let lease = source.take_lease();
        let future = run(Request {
            producer: Some(producer),
            backing_meter: Some(meter), handle, epoch: 7, reader: Some(Box::new(ReaderBomb(a.clone())) as NativeTcpReadHalf).into(),
            shared, credit: Some(credit).into(), signals: Some(signals).into(), local_finish: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stop: Some(stop).into(), service_floor: false,
        }, gate, Some(clock), lease);
        let result = catch_unwind(AssertUnwindSafe(|| drop(future)));
        assert!(result.is_ok(), "no original opaque panic may escape Source cancellation");
        assert!(budget.quarantined() && budget.used() > 0);
        assert_eq!(facts.0.lock().unwrap().len(), 2, "two actual fields, each reported exactly once");
        let stock = source.callbacks.as_ref().unwrap();
        assert!(stock.source_payload_matches(Field::Source0, &a));
        assert!(stock.source_payload_matches(Field::Source2, &b));
    });
}

//! Zero-poll cancellation of the actual owning writer future. Independent
//! original field failures run in a child so the pre-fix SIGABRT is preserved.
use super::*;
use std::{pin::Pin, sync::Arc, task::{Wake, Waker}};
use crate::shared::errors::{FailureOrder, FailureOrigin, FailurePhase, FailureResource, QuietFailureSink, RetainedFailure};

struct WriterBomb(Arc<u8>);
impl AsyncWrite for WriterBomb {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<std::io::Result<usize>> { panic!("zero poll must not write") }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> { panic!("zero poll must not flush") }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> { panic!("zero poll must not shut down") }
}
impl Drop for WriterBomb { fn drop(&mut self) { std::panic::panic_any(self.0.clone()); } }
struct SignalBomb(Arc<u8>);
impl Wake for SignalBomb {
    fn wake(self: Arc<Self>) { std::panic::panic_any(self.0.clone()); }
    fn wake_by_ref(self: &Arc<Self>) { std::panic::panic_any(self.0.clone()); }
}
#[derive(Default)]
struct Facts(std::sync::Mutex<Vec<RetainedFailure>>);
impl QuietFailureSink for Facts {
    fn record_quiet(&self, failure: RetainedFailure) { self.0.lock().unwrap().push(failure); }
}

#[test]
fn native_writer_zero_poll_keeps_independent_original_field_panics() {
    const CHILD: &str = "MINI_VPN_WRITER_ZERO_POLL_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let name = concat!(module_path!(), "::native_writer_zero_poll_keeps_independent_original_field_panics");
        let name = name.strip_prefix("mini_vpn::").unwrap_or(name);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name, "--nocapture"]).env(CHILD, "1").output().unwrap();
        eprintln!("original Writer zero-poll child status={:?}\n{}\n{}", output.status,
            String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success(), "writer and signal require independent custody before first poll");
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let facts = Arc::new(Facts::default());
        let sink: Arc<dyn QuietFailureSink> = facts.clone();
        let clock = Arc::new(FailureOrder::default());
        clock.bind(Arc::downgrade(&sink), FailureOrigin { phase: FailurePhase::TcpOpen,
            resource: FailureResource::TcpOpen, resource_id: 0, attempt_id: Some(7) }, Some(meter.clone())).unwrap();
        let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
        let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8])));
        let (_tx, rx) = crate::client_tun::uplink::channel(2);
        let (signals, mut received) = mpsc::channel(1);
        let writer_panic = Arc::new(11u8);
        let signal_panic = Arc::new(22u8);
        let waker = Waker::from(Arc::new(SignalBomb(signal_panic.clone())));
        assert!(received.poll_recv(&mut Context::from_waker(&waker)).is_pending());
        let (_stop, stop) = tokio::sync::oneshot::channel();
        let (_start, gate) = tokio::sync::oneshot::channel();
        let mut cold = Cold::prepare(&meter, clock.clone()).unwrap();
        let control = WriterControl::native(clock, cold.take_lease());
        let future = run_custodied_relay_writer_gated(handle, WriterBomb(writer_panic.clone()), rx,
            signals, stop, Some(gate), control);
        assert!(catch_unwind(AssertUnwindSafe(|| drop(future))).is_ok());
        assert!(budget.quarantined() && budget.used() > 0);
        assert_eq!(facts.0.lock().unwrap().len(), 2);
        let stock = cold.callbacks.as_ref().unwrap();
        assert!(stock.source_payload_matches(Field::Source0, &writer_panic));
        assert!(stock.source_payload_matches(Field::Source1, &signal_panic));
        assert!(!cold.retire());
    });
}

struct Environment {
    budget: bytes::BackingBudget,
    meter: bytes::BackingMeter,
    clock: Arc<FailureOrder>,
    facts: Arc<Facts>,
    _sockets: smoltcp::iface::SocketSet<'static>,
    handle: SocketHandle,
}
impl Environment {
    fn new() -> Self {
        let budget = bytes::BackingBudget::new(1 << 20).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let facts = Arc::new(Facts::default());
        let sink: Arc<dyn QuietFailureSink> = facts.clone();
        let clock = Arc::new(FailureOrder::default());
        clock.bind(Arc::downgrade(&sink), FailureOrigin { phase: FailurePhase::TcpOpen,
            resource: FailureResource::TcpOpen, resource_id: 0, attempt_id: Some(7) }, Some(meter.clone())).unwrap();
        let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
        let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8])));
        Self { budget, meter, clock, facts, _sockets: sockets, handle }
    }
    fn lane(&self) -> (crate::client_tun::uplink::Sender, crate::client_tun::uplink::Receiver, crate::client_tun::uplink::Observer) {
        crate::shared::bounded_channel::native_with(crate::shared::bounded_channel::NativeCapacity { messages: 1024, waiters: 1 },
            &self.meter, self.clock.clone(), &mut Some(())).unwrap()
    }
    fn payload(&self, bytes: &[u8]) -> bytes::Bytes {
        bytes::BytesMut::try_copy_from_slice_metered(bytes, &self.meter).unwrap().freeze()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_writer_old_permit_tail_precedes_callback_retirement() {
    let env = Environment::new();
    let mut cold = Cold::prepare(&env.meter, env.clock.clone()).unwrap();
    let (tx, rx, observer) = env.lane();
    let cold_cost = env.budget.used();
    let old = tx.try_reserve().unwrap();
    let (signals, _received) = mpsc::channel(2);
    let (stop, stop_rx) = tokio::sync::oneshot::channel();
    stop.send(()).unwrap();
    let mut outcome = run_custodied_relay_writer_gated(env.handle, tokio::io::sink(), rx,
        signals, stop_rx, None, WriterControl::native(env.clock.clone(), cold.take_lease())).await;
    assert_eq!(outcome.result.terminal, WriterTerminal::Stopped);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(outcome.remainder.poll_discard(&mut cx, 32).is_pending());
    assert!(!outcome.retire_fields() && !cold.retire());
    assert_eq!(env.budget.used(), cold_cost);
    let payload = env.payload(b"actual-tail");
    let last = payload.clone();
    old.send(RelayCommand::NativeData(payload));
    drop(tx);
    let Poll::Ready(receipt) = outcome.remainder.poll_discard(&mut cx, 32) else { panic!("actual old Permit tail must drain") };
    assert_eq!((receipt.queued.messages, receipt.queued.bytes), (1, 11));
    assert!(outcome.retire_fields() && cold.retire());
    assert_eq!(env.meter.snapshot().allocations, 1, "queue discard is not last backing ownership");
    assert!(env.budget.used() > 0 && !env.budget.quarantined());
    drop(last);
    drop((outcome, cold));
    assert!(observer.finish().is_ok());
    assert_eq!(env.budget.used(), FailureOrder::layout_bytes());
    drop(env.clock);
    assert_eq!(env.budget.used(), 0);
    assert!(env.facts.0.lock().unwrap().is_empty());
}

#[derive(Debug)]
struct IoBomb(Arc<u8>);
impl std::fmt::Display for IoBomb { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("original IO") } }
impl std::error::Error for IoBomb {}
impl Drop for IoBomb { fn drop(&mut self) { std::panic::panic_any(self.0.clone()); } }
struct TwoErrors { write: Arc<u8>, shutdown: Arc<u8> }
impl AsyncWrite for TwoErrors {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, IoBomb(self.write.clone()))))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> { panic!("write rejected before flush") }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, IoBomb(self.shutdown.clone()))))
    }
}

#[test]
fn native_writer_original_write_and_shutdown_sources_drop_independently() {
    const CHILD: &str = "MINI_VPN_WRITER_IO_DROP_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let name = concat!(module_path!(), "::native_writer_original_write_and_shutdown_sources_drop_independently");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", name.strip_prefix("mini_vpn::").unwrap_or(name), "--nocapture"])
            .env(CHILD, "1").output().unwrap();
        eprintln!("original Writer IO sources child status={:?}\n{}\n{}", output.status,
            String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        let env = Environment::new();
        let mut cold = Cold::prepare(&env.meter, env.clock.clone()).unwrap();
        let (tx, rx, _observer) = env.lane();
        let (signals, _received) = mpsc::channel(4);
        let (_stop, stop_rx) = tokio::sync::oneshot::channel();
        let write = Arc::new(31u8);
        let shutdown = Arc::new(32u8);
        let payload = env.payload(b"retain-original-current");
        let identity = payload.allocation_identity().unwrap();
        tx.try_send(RelayCommand::NativeData(payload)).unwrap();
        let outcome = run_custodied_relay_writer_gated(env.handle,
            TwoErrors { write: write.clone(), shutdown: shutdown.clone() }, rx, signals, stop_rx, None,
            WriterControl::native(env.clock.clone(), cold.take_lease())).await;
        assert_eq!(outcome.result.failure.as_ref().unwrap().source.kind(), std::io::ErrorKind::BrokenPipe);
        assert_eq!(outcome.result.shutdown_failure.as_ref().unwrap().kind(), std::io::ErrorKind::ConnectionReset);
        assert_eq!(outcome.error_order, [Some(0), Some(1)]);
        let crate::client_tun::uplink::Payload::Native(bytes) = &outcome.remainder.current.as_ref().unwrap().payload else { panic!("actual typed DATA") };
        assert_eq!(bytes.allocation_identity(), Some(identity));
        let before = env.budget.used();
        assert!(catch_unwind(AssertUnwindSafe(|| drop(outcome))).is_ok());
        assert!(env.budget.quarantined() && env.budget.used() >= before);
        let callbacks = cold.callbacks.as_ref().unwrap();
        assert!(callbacks.source_payload_matches(Field::Source4, &write));
        assert!(callbacks.source_payload_matches(Field::Source5, &shutdown));
        assert_eq!(env.facts.0.lock().unwrap().len(), 4, "two original IO reports then two independent Drop reports");
        assert!(!cold.retire());
    });
}

#[tokio::test(flavor = "current_thread")]
async fn native_writer_old_permit_notifies_only_custodied_root_waker() {
    let env = Environment::new();
    let mut cold = Cold::prepare(&env.meter, env.clock.clone()).unwrap();
    let (tx, rx, _observer) = env.lane();
    let old = tx.try_reserve().unwrap();
    let (signals, _received) = mpsc::channel(2);
    let (stop, stop_rx) = tokio::sync::oneshot::channel();
    stop.send(()).unwrap();
    let mut outcome = run_custodied_relay_writer_gated(env.handle, tokio::io::sink(), rx,
        signals, stop_rx, None, WriterControl::native(env.clock.clone(), cold.take_lease())).await;
    let original = Arc::new(41u8);
    let waker = Waker::from(Arc::new(SignalBomb(original.clone())));
    assert!(outcome.remainder.poll_discard(&mut Context::from_waker(&waker), 32).is_pending());
    let payload = env.payload(b"late-published-original");
    let sent = catch_unwind(AssertUnwindSafe(|| old.send(RelayCommand::NativeData(payload))));
    assert!(sent.is_ok(), "an original late Permit cannot invoke an unguarded Root wake");
    assert!(env.budget.quarantined() && env.budget.used() > 0);
    assert_eq!(env.facts.0.lock().unwrap().len(), 1);
    assert!(cold.callbacks.as_ref().unwrap().source_wake_payload_matches(&original));
    assert!(!outcome.retire_fields() && !cold.retire());
    drop(tx);
}

struct PendingWrite {
    entered: Option<tokio::sync::oneshot::Sender<usize>>,
    callback: Arc<std::sync::Mutex<Option<Waker>>>,
    drops: Arc<std::sync::atomic::AtomicUsize>,
}
impl AsyncWrite for PendingWrite {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<std::io::Result<usize>> {
        *self.callback.lock().unwrap() = Some(cx.waker().clone());
        if let Some(entered) = self.entered.take() { entered.send(bytes.as_ptr() as usize).unwrap(); }
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> { panic!("original write still Pending") }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> { panic!("abort does not issue new IO") }
}
impl Drop for PendingWrite {
    fn drop(&mut self) { self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst); }
}
#[tokio::test(flavor = "current_thread")]
async fn native_writer_actual_task_abort_keeps_original_current_and_callback_stock() {
    let env = Environment::new();
    let mut cold = Cold::prepare(&env.meter, env.clock.clone()).unwrap();
    let (tx, rx, _observer) = env.lane();
    let (signals, _received) = mpsc::channel(2);
    let (_stop, stop_rx) = tokio::sync::oneshot::channel();
    let (entered, started) = tokio::sync::oneshot::channel();
    let callback = Arc::new(std::sync::Mutex::new(None));
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let payload = env.payload(b"original-current-pending-actual-write");
    let ptr = payload.as_ptr() as usize;
    tx.try_send(RelayCommand::NativeData(payload)).unwrap();
    let charged = env.budget.used();
    let task = tokio::spawn(run_custodied_relay_writer_gated(env.handle,
        PendingWrite { entered: Some(entered), callback: callback.clone(), drops: drops.clone() }, rx,
        signals, stop_rx, None, WriterControl::native(env.clock.clone(), cold.take_lease())));
    assert_eq!(started.await.unwrap(), ptr);
    task.abort();
    assert!(task.await.err().unwrap().is_cancelled(), "actual task JOIN is a separate fact");
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(env.budget.quarantined() && env.budget.used() >= charged);
    assert_eq!(env.meter.snapshot().allocations, 1, "cancel cannot refund the retained original DATA");
    assert!(!cold.retire());
    drop(callback.lock().unwrap().take());
    assert!(!cold.retire(), "proxy Drop does not turn an unknown writer outcome clean");
    assert!(env.facts.0.lock().unwrap().is_empty(), "cancel Q cannot invent a source IO");
    drop(tx);
}

#[tokio::test(flavor = "current_thread")]
async fn native_writer_merge_allocation_refusal_keeps_originals_and_one_source() {
    let env = Environment::new();
    let mut cold = Cold::prepare(&env.meter, env.clock.clone()).unwrap();
    let (tx, rx, observer) = env.lane();
    let a = env.payload(b"first-original");
    let b = env.payload(b"second-original");
    let (pa, pb) = (a.as_ptr(), b.as_ptr());
    tx.try_reserve().unwrap().send(RelayCommand::NativeData(a));
    tx.try_reserve().unwrap().send(RelayCommand::NativeData(b));
    let charged = env.budget.used();
    let (signals, _received) = mpsc::channel(4);
    let (_stop, stop_rx) = tokio::sync::oneshot::channel();
    crate::client_tun::uplink::tests::refuse_next_merge();
    let mut outcome = run_custodied_relay_writer_gated(env.handle, tokio::io::sink(), rx,
        signals, stop_rx, None, WriterControl::native(env.clock.clone(), cold.take_lease())).await;
    assert_eq!(outcome.result.terminal, WriterTerminal::IoFailed);
    let failure = outcome.result.failure.as_ref().unwrap();
    assert_eq!(failure.operation, WriterOperation::Coalesce);
    assert_eq!(crate::client_tun::uplink::admission_cause(&failure.source), Some(bytes::MeteredCopyError::Allocation));
    assert_eq!(outcome.error_order, [Some(0), None]);
    assert_eq!(env.facts.0.lock().unwrap().len(), 1);
    assert_eq!(outcome.remainder.current.as_ref().unwrap().payload.as_ptr(), pa);
    let Some(RelayCommand::NativeData(second)) = outcome.remainder.deferred.as_ref() else { panic!("original deferred"); };
    assert_eq!(second.as_ptr(), pb);
    assert_eq!(env.budget.used(), charged);
    assert!(!env.budget.quarantined());
    drop(tx);
    let Poll::Ready(receipt) = outcome.remainder.poll_discard(&mut Context::from_waker(Waker::noop()), 32) else { panic!("actual EOF disposition"); };
    assert_eq!(receipt.current.unwrap().written, 0);
    assert_eq!((receipt.deferred.messages, receipt.deferred.bytes), (1, 15));
    assert!(!outcome.retire_fields() && !cold.retire(), "original IO still belongs to outcome");
    drop(outcome.result.failure.take()); // Original explicit source disposition.
    assert!(outcome.retire_fields() && cold.retire());
    drop((outcome, cold));
    assert!(observer.finish().is_ok());
    drop(env.clock);
    assert_eq!(env.budget.used(), 0);
    assert_eq!(env.facts.0.lock().unwrap().len(), 1);
}

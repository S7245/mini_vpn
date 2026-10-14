use super::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};

#[derive(Debug)]
struct SourceError(Arc<()>);

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original CONNECT source")
    }
}

impl std::error::Error for SourceError {}

enum WriteStep {
    Bytes(usize),
    Pending,
    Zero,
    Error,
    Overreport,
}

#[derive(Clone, Copy)]
enum ArmFailure {
    None,
    Read,
    Set,
}

struct ProbeState {
    priority: i32,
    arm_failure: ArmFailure,
    steps: VecDeque<WriteStep>,
    bytes: Vec<u8>,
    calls: usize,
    atomic_calls: usize,
    dropped: bool,
    source: Arc<()>,
}

struct Writer(Arc<StdMutex<ProbeState>>);

impl Drop for Writer {
    fn drop(&mut self) {
        let mut state = self.0.lock().unwrap();
        assert!(!state.dropped, "the writer has exactly one owner");
        state.dropped = true;
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.0.lock().unwrap();
        state.calls += 1;
        match state
            .steps
            .pop_front()
            .unwrap_or(WriteStep::Bytes(bytes.len()))
        {
            WriteStep::Bytes(limit) => {
                let n = bytes.len().min(limit);
                state.bytes.extend_from_slice(&bytes[..n]);
                Poll::Ready(Ok(n))
            }
            WriteStep::Pending => Poll::Pending,
            WriteStep::Zero => Poll::Ready(Ok(0)),
            WriteStep::Error => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                SourceError(state.source.clone()),
            ))),
            WriteStep::Overreport => Poll::Ready(Ok(bytes.len() + 1)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl TuicTcpStartupPriorityWrite for Writer {
    fn current_priority(&mut self) -> io::Result<i32> {
        let state = self.0.lock().unwrap();
        if matches!(state.arm_failure, ArmFailure::Read) {
            return Err(io::Error::other(SourceError(state.source.clone())));
        }
        Ok(state.priority)
    }

    fn set_stream_priority(&mut self, priority: i32) -> io::Result<()> {
        let mut state = self.0.lock().unwrap();
        state.priority = priority;
        if matches!(state.arm_failure, ArmFailure::Set) {
            return Err(io::Error::other(SourceError(state.source.clone())));
        }
        Ok(())
    }

    fn poll_write_then_set_priority(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
        priority: i32,
    ) -> Poll<io::Result<usize>> {
        self.0.lock().unwrap().atomic_calls += 1;
        let result = self.as_mut().poll_write(cx, bytes);
        if matches!(result, Poll::Ready(Ok(n)) if n > 0) {
            self.0.lock().unwrap().priority = priority;
        }
        result
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

fn fixture(steps: Vec<WriteStep>) -> (Writer, Arc<StdMutex<ProbeState>>, Arc<WakeCount>, Waker) {
    let state = Arc::new(StdMutex::new(ProbeState {
        priority: 7,
        arm_failure: ArmFailure::None,
        steps: steps.into(),
        bytes: Vec::new(),
        calls: 0,
        atomic_calls: 0,
        dropped: false,
        source: Arc::new(()),
    }));
    let wake = Arc::new(WakeCount::default());
    let waker = Waker::from(wake.clone());
    (Writer(state.clone()), state, wake, waker)
}

fn assert_source(error: &io::Error, state: &Arc<StdMutex<ProbeState>>) {
    let source = error
        .get_ref()
        .unwrap()
        .downcast_ref::<SourceError>()
        .unwrap();
    assert!(Arc::ptr_eq(&source.0, &state.lock().unwrap().source));
}

#[test]
fn partial_pending_and_observer_drop_preserve_connect_and_startup_priority() {
    let (writer, state, wake, waker) = fixture(vec![
        WriteStep::Bytes(2),
        WriteStep::Pending,
        WriteStep::Bytes(3),
        WriteStep::Bytes(2),
    ]);
    let mut attempt = ConnectWrite::new(writer, b"connect".to_vec());
    let mut cx = Context::from_waker(&waker);
    assert_eq!(
        state.lock().unwrap().priority,
        7,
        "constructing performs no IO"
    );
    {
        let mut observer = std::pin::pin!(std::future::poll_fn(|cx| attempt.poll(cx)));
        assert!(std::future::Future::poll(observer.as_mut(), &mut cx).is_pending());
    }
    assert_eq!(attempt.written, 2);
    assert!(!state.lock().unwrap().dropped);
    assert_eq!(
        wake.0.load(Ordering::SeqCst),
        1,
        "successful partial write reschedules"
    );
    assert!(attempt.poll(&mut cx).is_pending());
    assert_eq!(
        wake.0.load(Ordering::SeqCst),
        1,
        "transport Pending does not spin"
    );
    assert!(attempt.poll(&mut cx).is_pending());
    assert!(attempt.poll(&mut cx).is_ready());
    assert!(
        attempt.poll(&mut cx).is_ready(),
        "terminal observation is idempotent"
    );
    {
        let state = state.lock().unwrap();
        assert_eq!(state.calls, 4);
        assert_eq!(state.bytes, b"connect");
        assert_eq!(state.priority, 8);
        assert_eq!(state.atomic_calls, 0, "CONNECT is not business payload");
    }
    let mut ready = attempt
        .into_result()
        .unwrap_or_else(|_| panic!("complete CONNECT"));
    assert!(ready.first_payload_pending);
    assert!(matches!(
        Pin::new(&mut ready).poll_write(&mut cx, b"payload"),
        Poll::Ready(Ok(7))
    ));
    let state_guard = state.lock().unwrap();
    assert_eq!(state_guard.bytes, b"connectpayload");
    assert_eq!(state_guard.priority, 7);
    assert_eq!(state_guard.atomic_calls, 1);
    assert!(!state_guard.dropped);
    drop(state_guard);
    drop(ready);
    assert!(state.lock().unwrap().dropped);
}

#[test]
fn write_failure_is_sticky_and_returns_the_original_error_and_writer() {
    let (writer, state, _, waker) = fixture(vec![
        WriteStep::Bytes(2),
        WriteStep::Error,
        WriteStep::Bytes(9),
    ]);
    let mut attempt = ConnectWrite::new(writer, b"connect".to_vec());
    let mut cx = Context::from_waker(&waker);
    assert!(attempt.poll(&mut cx).is_pending());
    assert!(attempt.poll(&mut cx).is_ready());
    assert!(attempt.poll(&mut cx).is_ready());
    assert_eq!(
        state.lock().unwrap().calls,
        2,
        "failure prevents further writes"
    );
    let failure = match attempt.into_result() {
        Err(failure) => failure,
        Ok(_) => panic!("write error must not publish"),
    };
    assert_eq!(failure.written, 2);
    assert_eq!(failure.error.kind(), io::ErrorKind::ConnectionReset);
    assert_source(&failure.error, &state);
    assert!(Arc::ptr_eq(&failure.inner.0, &state));
    assert!(!state.lock().unwrap().dropped);
    drop(failure);
    assert!(state.lock().unwrap().dropped);
}

#[test]
fn zero_or_invalid_write_preserves_resource_and_bounded_offset() {
    for (step, kind) in [
        (WriteStep::Zero, io::ErrorKind::WriteZero),
        (WriteStep::Overreport, io::ErrorKind::InvalidData),
    ] {
        let (writer, state, _, waker) = fixture(vec![step]);
        let mut attempt = ConnectWrite::new(writer, b"connect".to_vec());
        assert!(attempt.poll(&mut Context::from_waker(&waker)).is_ready());
        let failure = match attempt.into_result() {
            Err(failure) => failure,
            Ok(_) => panic!("invalid write must not publish"),
        };
        assert_eq!(failure.error.kind(), kind);
        assert_eq!(failure.written, 0);
        assert!(!state.lock().unwrap().dropped);
        drop(failure);
        assert!(state.lock().unwrap().dropped);
    }
}

#[test]
fn all_priority_failures_retain_raw_writer_before_connect_bytes() {
    for (arm_failure, priority) in [
        (ArmFailure::Read, 7),
        (ArmFailure::Set, 7),
        (ArmFailure::None, i32::MAX),
    ] {
        let (writer, state, _, waker) = fixture(vec![]);
        {
            let mut state = state.lock().unwrap();
            state.arm_failure = arm_failure;
            state.priority = priority;
        }
        let mut attempt = ConnectWrite::new(writer, b"connect".to_vec());
        let mut cx = Context::from_waker(&waker);
        assert!(attempt.poll(&mut cx).is_ready());
        assert!(attempt.poll(&mut cx).is_ready());
        let failure = match attempt.into_result() {
            Err(failure) => failure,
            Ok(_) => panic!("arming error must not publish"),
        };
        assert_eq!(failure.written, 0);
        assert_eq!(state.lock().unwrap().calls, 0);
        if priority != i32::MAX {
            assert_source(&failure.error, &state);
        }
        assert!(Arc::ptr_eq(&failure.inner.0, &state));
        assert!(!state.lock().unwrap().dropped);
        drop(failure);
        assert!(state.lock().unwrap().dropped);
    }
}

#[test]
fn unfinished_extraction_returns_ownership_without_a_success() {
    for poll_once in [false, true] {
        let (writer, state, _, waker) = fixture(vec![WriteStep::Bytes(2)]);
        let mut attempt = ConnectWrite::new(writer, b"connect".to_vec());
        if poll_once {
            assert!(attempt.poll(&mut Context::from_waker(&waker)).is_pending());
        }
        let failure = match attempt.into_result() {
            Err(failure) => failure,
            Ok(_) => panic!("unfinished CONNECT must not publish"),
        };
        assert_eq!(failure.error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(failure.written, if poll_once { 2 } else { 0 });
        assert!(!state.lock().unwrap().dropped);
        drop(failure);
        assert!(state.lock().unwrap().dropped);
    }
}

#[test]
fn each_poll_admits_at_most_one_partial_write() {
    let (writer, state, wake, waker) = fixture((0..64).map(|_| WriteStep::Bytes(1)).collect());
    let mut attempt = ConnectWrite::new(writer, vec![1; 64]);
    let mut cx = Context::from_waker(&waker);
    for n in 1..=64 {
        assert_eq!(attempt.poll(&mut cx).is_ready(), n == 64);
        assert_eq!(state.lock().unwrap().calls, n);
        assert_eq!(attempt.written, n);
    }
    assert_eq!(wake.0.load(Ordering::SeqCst), 63);
    assert!(!state.lock().unwrap().dropped);
    drop(attempt);
    assert!(state.lock().unwrap().dropped);
}

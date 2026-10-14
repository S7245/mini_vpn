//! Actual MemoryTarget interest/commit/release traces, not a readiness stub.
//! The complete Bound tracer separately proves Root/C/Actor composition.

use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn finite_directive_attempt_then_real_shared_release_rearms_waiting_write() {
    let mut io = MemoryTarget::new(MemoryTargetConfig::new(2, 4, 4, 4, 4, 2).unwrap());
    let (a, b) = (flow(1), flow(2));
    io.open(a, &target(443)).unwrap();
    io.open(b, &target(443)).unwrap();
    assert_eq!(io.write(a, b"full").unwrap().accepted_bytes(), 4);
    io.push_write_directive(b, MemoryWriteDirective::WouldBlock)
        .unwrap();
    let writes = Arc::new(CountWake(AtomicUsize::new(0)));
    let write_waker = Waker::from(Arc::clone(&writes));
    let mut write_cx = Context::from_waker(&write_waker);
    let common = Arc::new(CountWake(AtomicUsize::new(0)));
    let common_waker = Waker::from(Arc::clone(&common));
    let mut common_cx = Context::from_waker(&common_waker);
    assert_eq!(io.poll_ready_notifications(&mut common_cx), Poll::Pending);
    assert!(!io.readiness(b).unwrap().writable);
    assert_eq!(
        io.poll_readiness(b, TargetReadyInterest::Write, &mut write_cx),
        Poll::Ready(Ok(TargetReadyOutcome::Attempt))
    );
    assert_eq!(io.write(b, b"x"), Ok(TargetWriteCompletion::WouldBlock));
    assert_eq!(
        io.poll_readiness(b, TargetReadyInterest::Write, &mut write_cx),
        Poll::Pending
    );
    let before = writes.0.load(Ordering::SeqCst);
    assert_eq!(io.take_written(a, 1).unwrap().as_ref(), b"f");
    assert_eq!(common.0.load(Ordering::SeqCst), 1);
    assert_eq!(writes.0.load(Ordering::SeqCst), before);
    assert_eq!(
        io.poll_ready_notifications(&mut common_cx),
        Poll::Ready(Ok(()))
    );
    assert_eq!(writes.0.load(Ordering::SeqCst), before + 1);
    assert_eq!(
        io.poll_readiness(b, TargetReadyInterest::Write, &mut write_cx),
        Poll::Ready(Ok(TargetReadyOutcome::Attempt))
    );
    assert_eq!(io.write(b, b"x").unwrap().accepted_bytes(), 1);
    for id in [a, b] {
        io.reset(id).unwrap();
        assert!(matches!(io.join(id), Ok(TargetJoinCompletion::Joined(_))));
        io.retire_terminal_tombstone(id).unwrap();
    }
    assert!(io.cold_inventory().unwrap().is_quiescent());
}

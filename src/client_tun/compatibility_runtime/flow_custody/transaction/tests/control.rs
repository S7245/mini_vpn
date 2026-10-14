//! The real F scanner consumes the original shared turn budget and parks after
//! a complete empty scan. No synthetic flow rows or capacity counters are used.
use super::super::*;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Wake, Waker};

#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn flow_scan_consumes_remaining_shared_budget_across_polls() {
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, mut receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let wake = Arc::new(Wakes::default());
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    dispatch.begin_turn().await;
    for _ in 0..5 {
        assert!(owner.take_control());
    }
    for _ in 0..3 {
        let mut next = std::pin::pin!(dispatch.recv(&mut receiver));
        assert!(matches!(
            next.as_mut().poll(&mut cx),
            Poll::Ready(Some(OpenEvent::Yield))
        ));
        assert_eq!(owner.opens().cursor, 27);
        assert_eq!(owner.control_remaining.load(Ordering::Relaxed), 0);
    }
    assert_eq!(wake.0.load(Ordering::SeqCst), 0);
    dispatch.begin_turn().await;
    {
        let mut next = std::pin::pin!(dispatch.recv(&mut receiver));
        assert!(next.as_mut().poll(&mut cx).is_pending());
    }
    assert_eq!(owner.opens().cursor, 59);
    assert_eq!(owner.control_remaining.load(Ordering::Relaxed), 0);
    assert_eq!(wake.0.load(Ordering::SeqCst), 1);
    let mut next = std::pin::pin!(dispatch.recv(&mut receiver));
    assert!(matches!(
        next.as_mut().poll(&mut cx),
        Poll::Ready(Some(OpenEvent::Yield))
    ));
    assert_eq!(owner.opens().cursor, 59);
}

#[tokio::test]
async fn complete_flow_scan_parks_without_self_waking() {
    for offset in [0, 27] {
        let owner = Arc::new(SessionControl::default());
        *owner.opens() = OpenCustody::with_flows();
        let count = owner.opens().slots.len();
        assert_eq!(count, 4224);
        let (dispatch, mut receiver) = OpenDispatch::channel(4, Some(owner.clone()));
        let wake = Arc::new(Wakes::default());
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        if offset != 0 {
            dispatch.begin_turn().await;
            for _ in 0..5 {
                assert!(owner.take_control());
            }
            let mut next = std::pin::pin!(dispatch.recv(&mut receiver));
            assert!(matches!(
                next.as_mut().poll(&mut cx),
                Poll::Ready(Some(OpenEvent::Yield))
            ));
            assert_eq!(owner.opens().cursor, offset);
        }
        for end in (32..=count).step_by(32) {
            dispatch.begin_turn().await;
            let before = wake.0.load(Ordering::SeqCst);
            {
                let mut next = std::pin::pin!(dispatch.recv(&mut receiver));
                assert!(next.as_mut().poll(&mut cx).is_pending());
            }
            assert_eq!(owner.opens().cursor, (end + offset) % count);
            assert_eq!(owner.control_remaining.load(Ordering::Relaxed), 0);
            assert_eq!(
                wake.0.load(Ordering::SeqCst) - before,
                usize::from(end != count)
            );
            if end != count {
                let mut next = std::pin::pin!(dispatch.recv(&mut receiver));
                assert!(matches!(
                    next.as_mut().poll(&mut cx),
                    Poll::Ready(Some(OpenEvent::Yield))
                ));
                assert_eq!(owner.opens().cursor, end + offset);
            }
        }
    }
}

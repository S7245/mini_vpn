//! The real F scanner over the real empty F+W table (D62 R-a/R-b): empty rows
//! spend no shared control, and passing the table end parks with Pending and no
//! self-wake. No synthetic flow rows or capacity counters are used; occupied-row
//! budget spending is covered by tests/preparation.rs with two real flows.
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
async fn empty_flow_scan_spends_no_shared_budget_across_polls() {
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
        assert!(next.as_mut().poll(&mut cx).is_pending());
        assert_eq!(owner.opens().cursor, 0);
        assert_eq!(owner.control_remaining.load(Ordering::Relaxed), 27);
    }
    // Exhausted control is never consulted when no row is occupied.
    for _ in 0..27 {
        assert!(owner.take_control());
    }
    let mut next = std::pin::pin!(dispatch.recv(&mut receiver));
    assert!(next.as_mut().poll(&mut cx).is_pending());
    assert_eq!(owner.opens().cursor, 0);
    assert_eq!(owner.control_remaining.load(Ordering::Relaxed), 0);
    assert_eq!(wake.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn complete_flow_scan_parks_without_self_waking() {
    for spent in [0, 5] {
        let owner = Arc::new(SessionControl::default());
        *owner.opens() = OpenCustody::with_flows();
        assert_eq!(owner.opens().slots.len(), 4224);
        let (dispatch, mut receiver) = OpenDispatch::channel(4, Some(owner.clone()));
        let wake = Arc::new(Wakes::default());
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        for _ in 0..3 {
            dispatch.begin_turn().await;
            for _ in 0..spent {
                assert!(owner.take_control());
            }
            {
                let mut next = std::pin::pin!(dispatch.recv(&mut receiver));
                assert!(next.as_mut().poll(&mut cx).is_pending());
            }
            assert_eq!(owner.opens().cursor, 0);
            assert_eq!(owner.control_remaining.load(Ordering::Relaxed), 32 - spent);
            assert_eq!(wake.0.load(Ordering::SeqCst), 0);
        }
    }
}

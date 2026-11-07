//! Actual original Native Main timer after a bounded absence of Service polls.
//! Observe scheduled tick identities; the shared scenario still proves exact
//! DATA, original peer retirement, 15-second stop, five JOINs and budget zero.
use super::*;
use crate::client_tun::legacy_event_loop::relay_events::RelayEventTestBridge;
use std::{future::{Future, poll_fn}, task::Poll};

#[tokio::test(flavor = "current_thread")]
async fn native_complete_original_main_coalesces_stale_timer_ticks() {
    complete_service_observed(false, ServiceUdp::None, terminal::Ending::Normal, 29).await;
}

pub(super) async fn observe_delayed_main(
    run: impl Future,
    bridge: &RelayEventTestBridge,
) -> (usize, usize) {
    tokio::pin!(run);
    // The actual saved Main remains in its original Service. QUIC's original
    // tasks may run, but no substitute Main or standalone Interval is polled.
    tokio::time::sleep(Duration::from_millis(60)).await;
    bridge.arm_timer_probe(tokio::time::Instant::now());
    let mut polls = 0;
    poll_fn(|cx| {
        assert!(run.as_mut().poll(cx).is_pending());
        polls += 1;
        if polls == 128 { Poll::Ready(()) }
        else { cx.waker().wake_by_ref(); Poll::Pending }
    }).await;
    bridge.take_timer_probe()
}

pub(super) fn assert_coalesced((total, stale): (usize, usize)) {
    eprintln!("[N1-actual-main-timer] total={total} stale_before_resume={stale} polls=128 pause_ms=60 original_clean_stop_already_verified=true");
    assert!(total >= 1, "must observe the actual original timer edge");
    assert_eq!(stale, 1, "one original overdue poll suffices; missed ticks must not replay");
}

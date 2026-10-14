use std::time::Duration;

use super::{SessionClockError, SessionMonotonicClock};
use crate::owned_upstream::two_leg::SimTime;

#[test]
fn one_fixed_epoch_is_shared_and_converts_before_equal_after_exactly() {
    let epoch = tokio::time::Instant::now();
    let clock = SessionMonotonicClock::from_epoch_for_test(epoch);
    let same = clock.clone();
    let other = SessionMonotonicClock::from_epoch_for_test(epoch);

    assert!(clock.same_source(&same));
    assert!(!clock.same_source(&other));
    assert_eq!(clock.observe(epoch), Ok(SimTime::ZERO));
    assert_eq!(
        clock.observe(epoch + Duration::from_nanos(1)),
        Ok(SimTime::from_nanos(1))
    );
    assert_eq!(
        clock.observe(epoch + Duration::from_nanos(2)),
        Ok(SimTime::from_nanos(2))
    );
}

#[test]
fn backwards_and_overflow_fail_closed_without_fabricating_time() {
    let epoch = tokio::time::Instant::now();
    let clock = SessionMonotonicClock::from_epoch_for_test(epoch);
    let before = epoch
        .checked_sub(Duration::from_nanos(1))
        .expect("the test instant has predecessor space");

    assert_eq!(
        clock.observe(before),
        Err(SessionClockError::TimeWentBackwards)
    );
    assert_eq!(
        clock.observe_elapsed(Duration::from_secs(u64::MAX)),
        Err(SessionClockError::Overflow)
    );
}

//! N1 stage C3: high-RTT serial controlled rebuild under deterministic loss.
//! Owns no production authority. Reuses the C1 test-only `RttRelay` (r=200ms)
//! with its `Tap` drop plan (D55): a lost successor handshake datagram must
//! cost exactly the fresh connection's PTO (quinn initial_rtt 333ms ⇒
//! `PTO_i = 333ms + 4·166.5ms = 999ms`, ×2 backoff per repeat) on top of the
//! C1 handshake component, while a lost original CONNECTION_CLOSE must not
//! change the client's local 3·PTO drain (C1 `D'` range).
use super::*;
use native_rebuild_high_rtt::{serial_rebuild_behind, Dir, Loss, SerialTiming};

const RTT: Duration = Duration::from_millis(200);
const PTO_INITIAL: Duration = Duration::from_millis(999);
const SLACK: Duration = Duration::from_millis(300);
const TOL: Duration = Duration::from_millis(2);

/// C1 C/D' ranges unchanged; H shifted by `extra` handshake recovery time.
fn assert_shifted(timing: &SerialTiming, extra: Duration) {
    let r = timing.rtt;
    let (low, high) = timing.drain_bounds();
    assert!(timing.d_prime + TOL >= low && timing.d_prime <= high,
        "D' outside 3·PTO [{low:?}, {high:?}]: {timing:?}");
    assert!(timing.c + TOL >= r && timing.c <= r + Duration::from_millis(25) + SLACK,
        "C outside [r, r+25ms+slack]: {timing:?}");
    let (h_low, h_high) = (r * 5 / 2 + extra, r * 7 / 2 + extra + SLACK);
    assert!(timing.h + TOL >= h_low && timing.h <= h_high,
        "H outside [{h_low:?}, {h_high:?}]: {timing:?}");
}

/// P1: the successor's first Initial is lost ⇒ +PTO_i, still ≤ 5s.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_serial_rebuild_recovers_lost_successor_initial_within_5s() {
    let loss = Loss { fixed: &[(1, Dir::Up, 0)], ..Loss::default() };
    let (timing, _) = serial_rebuild_behind(RTT, Duration::from_secs(5), loss).await;
    assert_eq!(timing.injected, 1);
    assert_shifted(&timing, PTO_INITIAL);
    assert!(timing.total <= Duration::from_secs(5), "one handshake loss at 200ms: {timing:?}");
}

/// P2: the peer's first handshake flight datagram is lost ⇒ +PTO_i.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_serial_rebuild_recovers_lost_peer_first_flight_within_5s() {
    let loss = Loss { fixed: &[(1, Dir::Down, 0)], ..Loss::default() };
    let (timing, _) = serial_rebuild_behind(RTT, Duration::from_secs(5), loss).await;
    assert_eq!(timing.injected, 1);
    assert_shifted(&timing, PTO_INITIAL);
    assert!(timing.total <= Duration::from_secs(5), "one handshake loss at 200ms: {timing:?}");
}

/// P3: the Initial and both first-PTO probes are lost ⇒ +PTO_i + 2·PTO_i.
/// The total is recorded against 5s (B8 input), not required.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_serial_rebuild_backs_off_after_repeated_initial_loss() {
    let loss = Loss { fixed: &[(1, Dir::Up, 0), (1, Dir::Up, 1), (1, Dir::Up, 2)], ..Loss::default() };
    let (timing, _) = serial_rebuild_behind(RTT, Duration::from_secs(15), loss).await;
    assert_eq!(timing.injected, 3);
    assert_shifted(&timing, 3 * PTO_INITIAL);
    eprintln!("C3 P3 total {:?} (5s met: {})", timing.total, timing.total <= Duration::from_secs(5));
}

/// P4: every copy of the original CONNECTION_CLOSE is lost ⇒ the client's
/// local 3·PTO drain is unchanged and the successor still meets 5s.
#[tokio::test(flavor = "current_thread")]
async fn hard_native_serial_rebuild_ignores_lost_original_close() {
    let loss = Loss { old_close_lost: true, ..Loss::default() };
    let (timing, _) = serial_rebuild_behind(RTT, Duration::from_secs(5), loss).await;
    assert!(timing.injected >= 1);
    assert_shifted(&timing, Duration::ZERO);
    assert!(timing.total <= Duration::from_secs(5), "close loss at 200ms: {timing:?}");
}

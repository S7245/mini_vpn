//! C6 (D79/D80) read-only sent-history observation, compiled only under the
//! off-by-default `sent-history-observe` feature (mini_vpn `harness`).
//!
//! - Owns: process-global Relaxed atomics recording the Data-space cold ring
//!   peak `used` (PN span incl. tombstones and outstanding permits), the count
//!   of `ordinary_ready() == false` admissions, and the peak congestion window
//!   seen at a Data-space reservation.
//! - Does not own: any admission, credit, refund, or quarantine decision. No
//!   value read here feeds back into `History`/`Credits`.
//! - Invariants: only rings marked by [`data`] (the Data space built by
//!   `History::prepare`) record; Initial/Handshake rings never do. Recording is
//!   one Relaxed RMW per event: no lock, no allocation.
//! - Entry: `prepare` → [`data`]; `ordinary_ready` → [`ready`]; `reserve` →
//!   [`used`]; `Connection::reserve_sent_packet` → [`cwnd`]. Public API:
//!   [`reset`], [`snapshot`]. Values are process-wide, so a consumer runs alone
//!   in its own process (mini_vpn serial `uplink-span` gate group).
//! - Tests: `sent_history/tests/observe.rs`; end to end
//!   `tuic_service/tests/native_uplink_span.rs`.
use super::{Credits, History, Mode, PacketClass};
use std::sync::{
    atomic::{AtomicU64, Ordering::Relaxed},
    Arc,
};

static PEAK_USED: AtomicU64 = AtomicU64::new(0);
static PEAK_ORDINARY: AtomicU64 = AtomicU64::new(0);
static BLOCKED: AtomicU64 = AtomicU64::new(0);
static PEAK_CWND: AtomicU64 = AtomicU64::new(0);
static RINGS: AtomicU64 = AtomicU64::new(0);
static ORDINARY: AtomicU64 = AtomicU64::new(0);
static CAPACITY: AtomicU64 = AtomicU64::new(0);

/// Process-wide observation since the last [`reset`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Snapshot {
    /// Peak Data-space cold ring `used` after a successful reservation.
    pub peak_used: u64,
    /// Peak `used` after an Ordinary-class reservation (≤ ordinary by
    /// admission); `peak_used - peak_ordinary` is Control headroom in use.
    pub peak_ordinary: u64,
    /// `ordinary_ready()` evaluations that refused new ordinary data.
    pub ordinary_blocked: u64,
    /// Peak congestion window (bytes) at a Data-space reservation.
    pub peak_cwnd: u64,
    /// Data-space cold rings created (one per hard connection).
    pub rings: u64,
    /// Ordinary / total capacity of the last created Data ring.
    pub ordinary: u64,
    pub capacity: u64,
}

/// Clear every counter (call before the observed run, in its own process).
pub fn reset() {
    for counter in [&PEAK_USED, &PEAK_ORDINARY, &BLOCKED, &PEAK_CWND, &RINGS, &ORDINARY, &CAPACITY] {
        counter.store(0, Relaxed);
    }
}

pub fn snapshot() -> Snapshot {
    Snapshot {
        peak_used: PEAK_USED.load(Relaxed),
        peak_ordinary: PEAK_ORDINARY.load(Relaxed),
        ordinary_blocked: BLOCKED.load(Relaxed),
        peak_cwnd: PEAK_CWND.load(Relaxed),
        rings: RINGS.load(Relaxed),
        ordinary: ORDINARY.load(Relaxed),
        capacity: CAPACITY.load(Relaxed),
    }
}

/// Mark a freshly built (uniquely owned) Data-space cold ring as observed.
pub(super) fn data(mut history: History) -> History {
    if let Mode::Cold(ring) = &mut history.mode {
        if let Some(owner) = Arc::get_mut(&mut ring.owner) {
            owner.observed = true;
            RINGS.fetch_add(1, Relaxed);
            ORDINARY.store(owner.ordinary as u64, Relaxed);
            CAPACITY.store(owner.capacity as u64, Relaxed);
        }
    }
    history
}

pub(super) fn ready(owner: &Credits, ready: bool) {
    if owner.observed && !ready {
        BLOCKED.fetch_add(1, Relaxed);
    }
}

pub(super) fn used(owner: &Credits, used: usize, class: PacketClass) {
    if owner.observed {
        PEAK_USED.fetch_max(used as u64, Relaxed);
        if class == PacketClass::Ordinary {
            PEAK_ORDINARY.fetch_max(used as u64, Relaxed);
        }
    }
}

pub(super) fn cwnd(history: &History, window: u64) {
    if let Mode::Cold(ring) = &history.mode {
        if ring.owner.observed {
            PEAK_CWND.fetch_max(window, Relaxed);
        }
    }
}

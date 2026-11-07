//! C6 observation unit tests (feature `sent-history-observe`). Counters are
//! process-global and other tests may build observed rings concurrently, so
//! these assertions only use monotone lower bounds and an unobserved control.
use super::super::observe;
use super::*;

#[test]
fn observed_ring_records_peak_used_and_ordinary_refusals_only_when_marked() {
    let budget = BackingBudget::new(64 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    // Unobserved control: a non-Data ring never records.
    let plain = History::cold(&meter, 2, 1).unwrap();
    let before = observe::snapshot();
    let held: Vec<_> = (0..2).map(|_| plain.reserve(PacketClass::Ordinary).unwrap()).collect();
    assert!(!plain.ordinary_ready());
    assert!(matches!(&plain.mode, Mode::Cold(ring) if !ring.owner.observed));
    drop(held);
    let history = observe::data(History::cold(&meter, 3, 1).unwrap());
    let marked = observe::snapshot();
    assert!(marked.rings > before.rings);
    let held: Vec<_> = (0..3).map(|_| history.reserve(PacketClass::Ordinary).unwrap()).collect();
    assert!(!history.ordinary_ready());
    let control = history.reserve(PacketClass::Control).unwrap();
    observe::cwnd(&history, 123_456);
    let after = observe::snapshot();
    assert!(after.peak_used >= 4, "{after:?}");
    assert!(after.peak_ordinary >= 3, "{after:?}");
    assert!(after.ordinary_blocked > marked.ordinary_blocked, "{after:?}");
    assert!(after.peak_cwnd >= 123_456, "{after:?}");
    drop((held, control));
    assert!(history.ordinary_ready());
}

//! Test-only synchronous Full injection at the real pre-encoding adapter call.
use super::*;
use std::sync::Mutex;
thread_local! { static ACTIVE: std::cell::RefCell<Option<Arc<Probe>>> = const { std::cell::RefCell::new(None) }; }
thread_local! { static ARMED: std::cell::RefCell<Option<Arc<Probe>>> = const { std::cell::RefCell::new(None) }; }
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Observation {
    pub(crate) done: bool,
    pub(crate) full: bool,
    pub(crate) encoded_capacity: usize,
    pub(crate) before: usize,
    pub(crate) after: usize,
    pub(crate) drops_before: u64,
    pub(crate) drops_after: u64,
}
pub(crate) struct Probe {
    budget: bytes::BackingBudget,
    meter: bytes::BackingMeter,
    observed: Mutex<Observation>,
}
impl Probe { pub(crate) fn observed(&self) -> Observation { *self.observed.lock().unwrap() } }
impl<U> LegacyOwnedUpstream<U> {
    pub(crate) fn arm_udp_budget_probe(budget: bytes::BackingBudget, meter: bytes::BackingMeter) -> Arc<Probe> {
        let probe = Arc::new(Probe { budget, meter, observed: Mutex::new(Observation::default()) });
        ARMED.with(|slot| { assert!(slot.borrow().is_none()); *slot.borrow_mut() = Some(probe.clone()); });
        probe
    }
}
pub(super) struct Turn { probe: Arc<Probe>, pressure: Option<bytes::BytesMut> }
pub(super) fn enter(drops: u64) -> Option<Turn> {
    let probe = ARMED.with(|slot| slot.borrow_mut().take())?;
    let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
    // Whole-budget Full: the TUIC encoder charges the Node's protocol tier,
    // so the pressure must fill through a protocol-tier meter (A2 Route 2).
    let filler = bytes::BackingMeter::with_protocol_budget(&probe.budget);
    assert!(probe.meter.same_budget(&filler));
    let pressure = bytes::BytesMut::try_zeroed_metered(probe.budget.limit() - probe.budget.used() - header, &filler).unwrap();
    *probe.observed.lock().unwrap() = Observation { full: probe.budget.used() == probe.budget.limit(), before: probe.budget.used(), drops_before: drops, ..Default::default() };
    ACTIVE.with(|slot| *slot.borrow_mut() = Some(probe.clone()));
    Some(Turn { probe, pressure: Some(pressure) })
}
impl Turn {
    pub(super) fn complete(mut self, drops: u64) {
        let after = self.probe.budget.used();
        drop(self.pressure.take());
        let mut state = self.probe.observed.lock().unwrap();
        state.after = after; state.drops_after = drops; state.done = true;
    }
}

impl Drop for Turn {
    fn drop(&mut self) { ACTIVE.with(|slot| { slot.borrow_mut().take(); }); }
}
impl LegacyOwnedUpstream<crate::tuic::TuicUpstream> {
    pub(crate) fn observe_udp_encoding(capacity: usize) {
        ACTIVE.with(|slot| {
            if let Some(probe) = slot.borrow().as_ref() {
                probe.observed.lock().unwrap().encoded_capacity += capacity;
            }
        });
    }
}

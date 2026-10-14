//! Test-only wall spans at existing callbacks; owns no scheduling or socket state.
//! Boundaries retain open marks; spans may include time while the fixture is idle.
//! SynBurst in the parent is the diagnostic entry; this never enables D16 scans.
use super::{MetricsSink, Sample};
use crate::loop_profiler::LoopProfiler;
use std::{cell::RefCell, rc::Rc, time::Instant};

#[derive(Default, Debug)]
struct Span {
    entered: usize,
    left: usize,
    mark: Option<Instant>,
    elapsed: Sample,
}
impl Span {
    fn enter(&mut self) {
        self.entered += 1;
        self.mark = Some(Instant::now());
    }
    fn leave(&mut self) {
        self.left += 1;
        if let Some(mark) = self.mark.take() {
            self.elapsed.add(mark.elapsed());
        }
    }
    fn reset(&mut self) {
        self.entered = 0;
        self.left = 0;
        self.elapsed = Sample::default();
    }
}
#[derive(Default)]
struct State {
    profiler: LoopProfiler,
    poll: Span,
    relay: Span,
    park: Span,
    listener_calls: usize,
    listener_total: usize,
    listener_max: usize,
    reports: usize,
}
#[derive(Clone, Default)]
pub(super) struct Profile(Rc<RefCell<State>>);
impl Profile {
    pub(super) fn boundary(&self, name: &str) {
        let mut s = self.0.borrow_mut();
        let period = s.profiler.take_period();
        eprintln!(
            "F spans {name}: {period:?} poll={:?} relay={:?} park={:?} listeners=({},{},{}) reports={}",
            s.poll, s.relay, s.park, s.listener_calls, s.listener_total, s.listener_max, s.reports
        );
        s.poll.reset();
        s.relay.reset();
        s.park.reset();
        s.listener_calls = 0;
        s.listener_total = 0;
        s.listener_max = 0;
        s.reports = 0;
    }
}
impl MetricsSink for Profile {
    fn enter_poll(&mut self) {
        let mut s = self.0.borrow_mut();
        s.profiler.enter_poll();
        s.poll.enter();
    }
    fn leave_poll(&mut self) {
        let mut s = self.0.borrow_mut();
        s.profiler.leave_poll();
        s.poll.leave();
    }
    fn enter_relay(&mut self) {
        let mut s = self.0.borrow_mut();
        s.profiler.enter_relay();
        s.relay.enter();
    }
    fn leave_relay(&mut self) {
        let mut s = self.0.borrow_mut();
        s.profiler.leave_relay();
        s.relay.leave();
    }
    fn loop_park_begin(&mut self) {
        let mut s = self.0.borrow_mut();
        s.profiler.loop_park_begin();
        s.park.enter();
    }
    fn loop_park_end(&mut self) {
        let mut s = self.0.borrow_mut();
        s.profiler.loop_park_end();
        s.park.leave();
    }
    fn note_listeners(&mut self, n: usize) {
        let mut s = self.0.borrow_mut();
        s.listener_calls += 1;
        s.listener_total += n;
        s.listener_max = s.listener_max.max(n);
    }
    fn report(&mut self) {
        self.0.borrow_mut().reports += 1;
    }
}

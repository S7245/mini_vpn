//! Actual RelayEvent disposition remains tested beside its private owner.
use super::*;
use crate::shared::errors::*;
use bytes::BackingMeter;
use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
};
#[derive(Default)]
struct Facts(Mutex<Vec<RetainedFailure>>);
impl QuietFailureSink for Facts {
    fn record_quiet(&self, f: RetainedFailure) {
        self.0.lock().unwrap().push(f);
    }
}
fn clock(meter: &BackingMeter) -> (FailureClock, Arc<Facts>) {
    let facts = Arc::new(Facts::default());
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let clock = Arc::new(FailureOrder::default());
    clock
        .bind(
            Arc::downgrade(&sink),
            FailureOrigin {
                phase: FailurePhase::SessionLoop,
                resource: FailureResource::Session,
                resource_id: 4,
                attempt_id: None,
            },
            Some(meter.clone()),
        )
        .unwrap();
    (clock, facts)
}
struct Twice;
impl Wake for Twice {
    fn wake(self: Arc<Self>) {
        panic!("original wake A");
    }
    fn wake_by_ref(self: &Arc<Self>) {
        panic!("original wake A");
    }
}
impl Drop for Twice {
    fn drop(&mut self) {
        panic!("original drop B");
    }
}
mod permit_drop;
mod permit_identity;

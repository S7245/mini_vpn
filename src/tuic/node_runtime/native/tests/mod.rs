use super::*;
use crate::shared::errors::*;
struct Facts(StdMutex<Vec<RetainedFailure>>);
impl QuietFailureSink for Facts {
    fn record_quiet(&self, failure: RetainedFailure) {
        self.0.lock().unwrap().push(failure);
    }
}
fn clock(meter: &BackingMeter) -> (FailureClock, Arc<Facts>) {
    let facts = Arc::new(Facts(StdMutex::new(Vec::new())));
    let sink: Arc<dyn QuietFailureSink> = facts.clone();
    let clock = Arc::new(FailureOrder::default());
    clock
        .bind(Arc::downgrade(&sink), udp_origin(), Some(meter.clone()))
        .unwrap();
    (clock, facts)
}
mod monitor;
mod tasks;

impl TuicUpstream {
    pub(crate) fn native_udp_sender_for_test(&self) -> (GenerationSender, BackingMeter) {
        let native = self.udp_runtime.native.get().unwrap();
        let state = native.lock();
        let stock = state.as_ref().unwrap();
        (
            stock.prepared.as_ref().unwrap().0.clone(),
            native.meter.clone(),
        )
    }
}

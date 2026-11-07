//! Root reader ownership tests for the shared Native reassembly seam.
use super::*;
mod owned;
thread_local! {
    static Q_BEFORE_FINISH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(super) fn before_output_finish(meter: Option<&BackingMeter>) {
    if Q_BEFORE_FINISH.with(|flag| flag.replace(false)) { meter.unwrap().quarantine(); }
}

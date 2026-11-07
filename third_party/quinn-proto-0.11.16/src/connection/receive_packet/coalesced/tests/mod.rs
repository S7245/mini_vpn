//! Exact original Incoming slab size used by the wire cancellation tracer.
use super::*;
impl Connection {
    pub(crate) fn coalesced_stock_bytes_for_probe() -> usize {
        std::alloc::Layout::array::<Option<Incoming>>(16)
            .unwrap()
            .size()
    }
}

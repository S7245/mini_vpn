use super::*;
mod custody;
mod stock;

impl super::AsyncLeasedByteFlowQueue {
    pub(crate) fn retained_callback_arc<T: std::any::Any>(
        &self,
        expected: &std::sync::Arc<T>,
    ) -> bool {
        self.inner
            .waiters
            .get()
            .is_some_and(|w| w.retains_arc(expected))
    }
}

#[test]
fn fixed_native_queue_head_layout_is_observable() {
    eprintln!(
        "Native fixed Leased queue head: {} bytes, at most one per Flow; separate instance-head bound",
        std::mem::size_of::<AsyncLeasedByteFlowQueueInner>()
    );
}

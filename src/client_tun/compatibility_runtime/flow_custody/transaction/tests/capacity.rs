//! Cold observation of actual Root allocations; no marker mints release proof.
use super::super::*;

#[derive(Clone, Copy, Debug)]
pub(in crate::client_tun::compatibility_runtime) struct RootLayout {
    pub slot_len: usize,
    pub slot_capacity: usize,
    pub slot_bytes: usize,
    pub rows: usize,
    pub waiting: usize,
    pub published: usize,
    pub row_bytes: usize,
    pub provider_boxes: usize,
    pub provider_outer_bytes: usize,
}
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn root_layout_for_test(&self) -> RootLayout {
        let opens = self.opens();
        assert!(opens.audit_flows());
        let mut result = RootLayout {
            slot_len: opens.slots.len(), slot_capacity: opens.slots.capacity(),
            slot_bytes: std::alloc::Layout::array::<Option<OpenSlot>>(opens.slots.capacity()).unwrap().size(),
            rows: 0, waiting: 0, published: 0, row_bytes: 0, provider_boxes: 0, provider_outer_bytes: 0,
        };
        let mut allocations = std::collections::HashSet::new();
        for slot in opens.slots.iter().flatten() {
            let OpenPhase::Flow(flow) = &slot.phase else { panic!("actual F4096 Root Flow row"); };
            assert!(allocations.insert(flow.as_ref() as *const FlowTransaction as usize));
            result.rows += 1;
            result.waiting += usize::from(flow.w_held);
            result.published += usize::from(flow.published);
            result.row_bytes += std::alloc::Layout::for_value(flow.as_ref()).size();
            if let Some(provider) = &flow.attempt {
                result.provider_boxes += 1;
                result.provider_outer_bytes += std::alloc::Layout::for_value(provider.as_ref()).size();
            }
        }
        assert_eq!(result.rows, opens.live);
        result
    }
}

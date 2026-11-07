//! Actual pending retirement future allocation; no completion marker.
use super::*;
impl Inventory {
    pub(in crate::client_tun::compatibility_runtime::flow_custody::supervision::roles) fn pending_allocation_for_test(&self) -> Option<(usize, usize)> {
        if !self.issued || self.done || self.failed || self.returned.is_some() || self.allowance.is_none() { return None; }
        let waiting = self.waiting.as_ref()?;
        Some((&**waiting as *const _ as *const () as usize, std::mem::size_of_val(&**waiting)))
    }
}

use super::*;

mod outcomes;
mod native_cancel;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::client_tun::compatibility_runtime::flow_custody) struct CustodyView {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) current:
        Option<(usize, usize, usize, usize, bool)>,
    pub(in crate::client_tun::compatibility_runtime::flow_custody) bytes: Option<[u8; 12]>,
    pub(in crate::client_tun::compatibility_runtime::flow_custody) deferred:
        Option<(usize, usize, usize)>,
    pub(in crate::client_tun::compatibility_runtime::flow_custody) receiver:
        Option<(usize, usize, bool)>,
    pub(in crate::client_tun::compatibility_runtime::flow_custody) flags: [bool; 4],
    pub(in crate::client_tun::compatibility_runtime::flow_custody) unaccounted: bool,
    pub(in crate::client_tun::compatibility_runtime::flow_custody) receipt: WriterDiscardReceipt,
}
impl WriterRemainder {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn custody_for_test(
        &self,
    ) -> CustodyView {
        CustodyView {
            current: self.current.as_ref().map(|c| {
                (
                    c.payload.len(),
                    c.payload.as_ptr() as usize,
                    c.payload.capacity(),
                    c.written,
                    c.flushed,
                )
            }),
            bytes: self
                .current
                .as_ref()
                .and_then(|c| c.payload[..].try_into().ok()),
            deferred: self.deferred.as_ref().and_then(|c| match c {
                RelayCommand::Data(b) => Some((b.len(), b.as_ptr() as usize, b.capacity())),
                RelayCommand::NativeData(b) => Some((b.len(), b.as_ptr() as usize, b.allocation_identity().unwrap().capacity)),
                RelayCommand::Finish => None,
            }),
            receiver: self
                .receiver
                .as_ref()
                .map(|rx| (rx.max_capacity(), rx.len(), rx.is_closed())),
            flags: [
                self.closed,
                self.complete,
                self.interrupted,
                self.quarantined,
            ],
            unaccounted: self.unaccounted.is_some(),
            receipt: self.receipt,
        }
    }
}

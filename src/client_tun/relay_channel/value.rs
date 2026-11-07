//! Static event metadata and original field-by-field terminal disposition.
//! This is sealed to the actual relay event in production; tests may supply
//! their own tracked values. It adds no per-DATA dynamic callback or allocation.
use super::{Fault, Value};
use crate::client_tun::{RelayEvent, SocketHandle};
use std::mem::ManuallyDrop;
impl Value for (SocketHandle, RelayEvent) {
    type Disposition = ();
    fn dispose_context(_: (), _: &Fault<'_>) -> Option<()> {
        None
    }
    fn lane_bytes(&self) -> usize {
        match &self.1 {
            RelayEvent::Data { bytes, .. } => bytes.len(),
            _ => 0,
        }
    }
    fn valid_for(&self, meter: &bytes::BackingMeter) -> bool {
        match &self.1 {
            RelayEvent::Data {
                bytes: crate::tcp_downlink_pump::OwnedPayload::Native(bytes),
                downstream_permit,
                ..
            } => {
                !bytes.is_empty()
                    && bytes
                        .backing_meter()
                        .is_some_and(|actual| actual.same_budget(meter))
                    && downstream_permit.as_ref().is_none_or(|p| {
                        p.remaining_bytes() == bytes.len() && p.native_custody_for(meter)
                    })
            }
            RelayEvent::Data { .. } => false,
            _ => true,
        }
    }
    fn dispose(self, fault: &Fault<'_>, _: &Self::Disposition) -> Option<Self> {
        let (handle, event) = self;
        match event {
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window,
                downstream_permit,
            } => {
                let mut bytes = ManuallyDrop::new(bytes);
                fault.dispose(downstream_permit);
                if fault.failed() {
                    return Some((
                        handle,
                        RelayEvent::Data {
                            epoch,
                            bytes: unsafe { ManuallyDrop::take(&mut bytes) },
                            stream_service_window,
                            downstream_permit: None,
                        },
                    ));
                }
                fault.dispose(ManuallyDrop::into_inner(bytes));
                None
            }
            _ => None,
        }
    }
}

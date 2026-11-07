//! Passive test observations of the original channel, without new ownership.
use super::*;
mod close_probe;

impl<T: Value> Receiver<T> {
    pub(crate) fn len(&self) -> usize {
        match &self.0 {
            Rx::Legacy(rx) => rx.len(),
            Rx::Native(rx) => rx.len(),
        }
    }
    pub(crate) fn is_closed(&self) -> bool {
        match &self.0 {
            Rx::Legacy(rx) => rx.is_closed(),
            Rx::Native(rx) => rx.is_closed(),
        }
    }
}

mod credit;
mod ring;

impl<T: Value> Sender<T> {
    pub(crate) fn same_channel(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Tx::Legacy(a), Tx::Legacy(b)) => a.same_channel(b),
            (Tx::Native(a), Tx::Native(b)) => std::ptr::eq(a.context().0, b.context().0),
            _ => false,
        }
    }
    pub(crate) fn legacy_sender_for_test(&self) -> Option<&tokio::sync::mpsc::Sender<T>> {
        match &self.0 { Tx::Legacy(tx) => Some(tx), Tx::Native(_) => None }
    }
}

//! Harness-only passive relay-event bridge types.
//!
//! Owns test-only weak-sender access and overwrite-only snapshots; no production
//! sender, task, timer, SocketCtx, session/flow ledger, or physical authority.
//! Entries: `RelayEventTestKind::from_event` and `RelayEventTestBridge::{install_sender,
//! try_send, snapshot, observe}`, through the parent's restricted re-export.
//! Tests: `client_tun::compatibility_runtime::tests::open_custody::events` and
//! `client_tun::compatibility_runtime::tests::flow_custody::rearm_custody`.

use super::{
    DownlinkCreditController, RelayEvent, RelayGapAckDrainState, SocketHandle, SocketState,
};

#[cfg(all(test, feature = "harness"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) enum RelayEventTestKind {
    Data,
    DataReady,
    AckDrainHint,
    Closed,
}

#[cfg(all(test, feature = "harness"))]
impl RelayEventTestKind {
    pub(super) fn from_event(event: &RelayEvent) -> (Self, u64) {
        match event {
            RelayEvent::Data { epoch, .. } => (Self::Data, *epoch),
            RelayEvent::DataReady { epoch } => (Self::DataReady, *epoch),
            RelayEvent::AckDrainHint { epoch, .. } => (Self::AckDrainHint, *epoch),
            RelayEvent::Closed(close) => (Self::Closed, close.epoch),
        }
    }
}

#[cfg(all(test, feature = "harness"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) struct RelayEventTestContext {
    pub(in crate::client_tun) epoch: u64,
    pub(in crate::client_tun) state: SocketState,
    pub(in crate::client_tun) has_uplink_tx: bool,
    pub(in crate::client_tun) pending_downlink_bytes: usize,
    pub(in crate::client_tun) read_credit: DownlinkCreditController,
}

#[cfg(all(test, feature = "harness"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) struct RelayEventTestSnapshot {
    pub(in crate::client_tun) generation: u64,
    pub(in crate::client_tun) handle: SocketHandle,
    pub(in crate::client_tun) kind: RelayEventTestKind,
    pub(in crate::client_tun) event_epoch: u64,
    pub(in crate::client_tun) rejected: bool,
    pub(in crate::client_tun) context: Option<RelayEventTestContext>,
    pub(in crate::client_tun) dirty: bool,
    pub(in crate::client_tun) d4_deadline: Option<std::time::Instant>,
    pub(in crate::client_tun) relay_gap_ack_drain: RelayGapAckDrainState,
}

#[cfg(all(test, feature = "harness"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) enum RelayEventTestSendResult {
    Accepted,
    NotInstalled,
    Closed,
    Full,
}

#[cfg(all(test, feature = "harness"))]
impl RelayEventTestSendResult {
    pub(in crate::client_tun) fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted)
    }
}

/// Harness-only passive access to the existing bounded relay-event channel.
/// It owns only a weak sender and one overwrite-only value snapshot; the
/// event-loop-local OpenDispatch is its sole strong owner.
#[cfg(all(test, feature = "harness"))]
#[derive(Default)]
pub(in crate::client_tun) struct RelayEventTestBridge {
    sender: std::sync::Mutex<Option<tokio::sync::mpsc::WeakSender<(SocketHandle, RelayEvent)>>>,
    snapshot: std::sync::Mutex<Option<RelayEventTestSnapshot>>,
    generation: std::sync::atomic::AtomicU64,
    selected_data: std::sync::Mutex<Option<(u64, usize, usize, usize)>>,
    fatal_census: std::sync::Mutex<Option<([(usize, usize); 3], usize)>>,
}

#[cfg(all(test, feature = "harness"))]
impl RelayEventTestBridge {
    pub(in crate::client_tun) fn install_sender(
        &self,
        sender: tokio::sync::mpsc::WeakSender<(SocketHandle, RelayEvent)>,
    ) {
        *self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sender);
    }

    pub(in crate::client_tun) fn queued_len(&self) -> usize {
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|sender| sender.upgrade())
            .map_or(0, |sender| sender.max_capacity() - sender.capacity())
    }

    pub(in crate::client_tun) fn receiver_is_open(&self) -> bool {
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .and_then(|sender| sender.upgrade())
            .is_some_and(|sender| !sender.is_closed())
    }

    pub(in crate::client_tun) fn fatal_census(&self) -> Option<([(usize, usize); 3], usize)> {
        *self
            .fatal_census
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(in crate::client_tun) fn observe_fatal_census(
        &self,
        census: [(usize, usize); 3],
        in_flight: usize,
    ) {
        *self
            .fatal_census
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some((census, in_flight));
    }

    pub(in crate::client_tun) fn selected_data(&self) -> Option<(u64, usize, usize, usize)> {
        *self
            .selected_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(in crate::client_tun) fn observe_selected(
        &self,
        selected: &Option<(SocketHandle, RelayEvent)>,
    ) {
        let mut value = self
            .selected_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next = selected.as_ref().and_then(|(_, event)| match event {
            RelayEvent::Data { epoch, bytes, .. } => {
                Some((*epoch, bytes.len(), bytes.as_ptr() as usize, 0))
            }
            _ => None,
        });
        if next.map(|v| (v.0, v.1, v.2)) != value.map(|v| (v.0, v.1, v.2)) {
            *value = next;
        }
    }

    pub(in crate::client_tun) fn observe_payload_transfer(&self, payload: &[u8]) {
        let mut value = self
            .selected_data
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(value) = value.as_mut() {
            if value.1 == payload.len() && value.2 == payload.as_ptr() as usize {
                value.3 = payload.len();
            }
        }
    }

    pub(in crate::client_tun) fn try_send(
        &self,
        handle: SocketHandle,
        event: RelayEvent,
    ) -> RelayEventTestSendResult {
        let sender = self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sender) = sender else {
            return RelayEventTestSendResult::NotInstalled;
        };
        let Some(sender) = sender.upgrade() else {
            return RelayEventTestSendResult::Closed;
        };
        match sender.try_send((handle, event)) {
            Ok(()) => RelayEventTestSendResult::Accepted,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => RelayEventTestSendResult::Full,
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                RelayEventTestSendResult::Closed
            }
        }
    }

    pub(in crate::client_tun) fn snapshot(&self) -> Option<RelayEventTestSnapshot> {
        *self
            .snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(in crate::client_tun) fn observe(&self, mut snapshot: RelayEventTestSnapshot) {
        snapshot.generation = self
            .generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .saturating_add(1);
        *self
            .snapshot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(snapshot);
    }
}

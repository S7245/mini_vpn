//! Owns the Connection's ordinary event inventory and independent Drained slot.
//! Cold allocation precedes Endpoint connection/CID registration. Datagram
//! operations reserve their complete burst before protocol mutation; producers
//! consume that reservation. This does not own CID maps or lifetime records.
//!
//! Ordinary-event bound for the current production sources: retirement emits at
//! most one event per RETIRE_CONNECTION_ID frame (at least two encoded bytes).
//! NEW_CONNECTION_ID can reset the token at most twice, with more than two
//! encoded bytes. These frame effects fit in the payload byte count. Migration
//! adds at most one token event per processed packet, chargeable to its nonempty
//! wire header. The single handshake transition adds at most ResetToken plus
//! NeedIdentifiers. Thus complete wire length L plus eight covers these sources
//! conservatively, including coalesced packets. Timer/local-address events have
//! separate pre-reservations; Drained has its own slot. New ordinary emitters
//! must preserve this bound or change admission before they are wired.

use crate::{
    owned_events::{self, BatchPermit, Receiver, Sender},
    shared::EndpointEventInner,
    EndpointConfig,
};
use bytes::BackingMeter;
use std::{
    collections::VecDeque,
    io,
    task::{Context, Poll, Waker},
};

#[cfg(test)]
mod tests;

const DATAGRAM_ALLOWANCE: usize = 8;
const CONTROL_RESERVE: usize = 8;

pub(crate) enum EventInventory {
    Legacy(VecDeque<EndpointEventInner>),
    Cold {
        sender: Sender<EndpointEventInner>,
        receiver: Receiver<EndpointEventInner>,
        batch: Option<BatchPermit<EndpointEventInner>>,
        meter: BackingMeter,
    },
}

pub(super) struct OperationGuard {
    meter: Option<BackingMeter>,
    armed: bool,
}

impl OperationGuard {
    pub(super) fn finish(mut self) {
        self.armed = false;
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        if self.armed {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
        }
    }
}

impl EventInventory {
    fn capacity(config: &EndpointConfig) -> io::Result<usize> {
        config
            .receive_datagram_limit
            .checked_add(DATAGRAM_ALLOWANCE)
            .and_then(|n| n.checked_add(CONTROL_RESERVE))
            .ok_or_else(|| io::Error::other("event layout overflow"))
    }
    pub(super) fn layout_bytes(config: &EndpointConfig) -> io::Result<usize> {
        let count = Self::capacity(config)?
            .checked_add(1)
            .ok_or_else(|| io::Error::other("event terminal layout overflow"))?;
        Ok(owned_events::slot_layout::<EndpointEventInner>(count)
            .map_err(io::Error::other)?
            .size())
    }
    pub(crate) fn prepare(config: &EndpointConfig) -> io::Result<Self> {
        let Some(meter) = config
            .receive_backing_meter
            .as_ref()
            .filter(|meter| meter.budget_limit().is_some())
        else {
            return Ok(Self::Legacy(VecDeque::new()));
        };
        // The packet operation may consume at most L+8 ordinary credits. The
        // extra eight slots remain available to bounded non-packet control.
        let capacity = Self::capacity(config)?;
        let (sender, receiver) = owned_events::terminal_channel(capacity, meter)?;
        Ok(Self::Cold {
            sender,
            receiver,
            batch: None,
            meter: meter.clone(),
        })
    }

    pub(super) fn begin(&mut self, datagram_len: Option<usize>) -> bool {
        self.begin_count(datagram_len.map_or(0, |len| len.saturating_add(DATAGRAM_ALLOWANCE)))
    }

    pub(super) fn begin_control(&mut self, count: usize) -> bool {
        self.begin_count(count)
    }

    fn begin_count(&mut self, count: usize) -> bool {
        match self {
            Self::Legacy(_) => true,
            Self::Cold {
                sender,
                batch,
                meter,
                ..
            } => {
                if batch.is_some() || meter.snapshot().quarantined {
                    return false;
                }
                if count == 0 {
                    return true;
                }
                match sender.try_reserve_batch(count) {
                    Ok(credit) => {
                        *batch = Some(credit);
                        true
                    }
                    Err(_) => false,
                }
            }
        }
    }

    pub(super) fn guard(&self) -> OperationGuard {
        OperationGuard {
            meter: match self {
                Self::Cold { meter, .. } => Some(meter.clone()),
                Self::Legacy(_) => None,
            },
            armed: true,
        }
    }

    pub(super) fn finish(&mut self) {
        if let Self::Cold { batch, .. } = self {
            *batch = None;
        }
    }

    pub(super) fn quarantine(&self) {
        if let Self::Cold { meter, .. } = self {
            meter.quarantine();
        }
    }

    pub(super) fn quarantined(&self) -> bool {
        matches!(self, Self::Cold { meter, .. } if meter.snapshot().quarantined)
    }

    pub(super) fn push_back(&mut self, event: EndpointEventInner) {
        match self {
            Self::Legacy(queue) => queue.push_back(event),
            Self::Cold {
                sender,
                batch,
                meter,
                ..
            } => {
                let result = if matches!(event, EndpointEventInner::Drained) {
                    sender.send_terminal(event)
                } else if let Some(credit) = batch {
                    credit.send(event)
                } else {
                    // Non-datagram controls use the independently retained
                    // headroom; failure is an unproved control transaction.
                    match sender.try_reserve() {
                        Ok(credit) => credit.send(event),
                        Err(_) => Err(event),
                    }
                };
                if result.is_err() {
                    meter.quarantine();
                }
            }
        }
    }

    pub(super) fn pop_front(&mut self) -> Option<EndpointEventInner> {
        match self {
            Self::Legacy(queue) => queue.pop_front(),
            Self::Cold { receiver, .. } => {
                match receiver.poll_recv(&mut Context::from_waker(Waker::noop())) {
                    Poll::Ready(event) => event,
                    Poll::Pending => None,
                }
            }
        }
    }
}

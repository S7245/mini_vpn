//! Owns one connection's cold physical inventories before CID/TLS mutation.
//! Partial preparation drops exact known layouts; no live inventory is refunded
//! by a marker or snapshot. The Endpoint owns registration/publication, while
//! Connection/last references own lifetime disposal. This is not a whole-Node
//! budget: non-CID metadata, registry and controllable CRYPTO output remain open.
//! Entry: ColdResources::{layout_bytes,prepare}. Tests: tests/admission.
use super::{cid_frames::Pool, event_inventory::EventInventory};
use crate::{Duration, EndpointConfig};
use bytes::BackingMeter;
use std::io;

pub(crate) struct ColdResources {
    pub(crate) registration_key: Option<crate::RegistrationKey>,
    pub(crate) native_registration: bool,
    pub(crate) events: EventInventory,
    pub(crate) cids: Option<Pool>,
    pub(super) records: Option<super::control_inventory::records::Pool>,
    pub(super) packets: [super::sent_history::History; 3],
    pub(super) ack_ranges: [crate::range_set::ArrayRangeSet; 3],
}
impl ColdResources {
    /// Planning cost for the hard profile, using the actual owner layouts.
    /// Legacy callers do not allocate these cold inventories.
    pub(crate) fn layout_bytes(
        config: &EndpointConfig,
        lifetime: Option<Duration>,
    ) -> io::Result<usize> {
        let events = EventInventory::layout_bytes(config)?;
        let cids = if lifetime.is_none() {
            Pool::layout().map_err(io::Error::other)?.size()
        } else {
            0
        };
        let packets = super::sent_history::History::layout_bytes(config)?;
        let records = super::control_inventory::records::Pool::layout_bytes(config)?;
        let ranges =
            crate::range_set::ArrayRangeSet::cold_layout(super::spaces::ACK_RANGE_CAPACITY)?
                .size()
                .checked_mul(3)
                .ok_or_else(|| io::Error::other("ACK range layout overflow"))?;
        events
            .checked_add(cids)
            .and_then(|n| n.checked_add(records))
            .and_then(|n| n.checked_add(ranges))
            .and_then(|n| n.checked_add(packets))
            .ok_or_else(|| io::Error::other("connection cold layout overflow"))
    }
    pub(crate) fn prepare(config: &EndpointConfig, lifetime: Option<Duration>) -> io::Result<Self> {
        if let Some(limit) = config
            .receive_backing_meter
            .as_ref()
            .and_then(BackingMeter::budget_limit)
        {
            if Self::layout_bytes(config, lifetime)? > limit {
                return Err(io::Error::other(
                    "connection cold layout exceeds hard limit",
                ));
            }
        }
        let events = EventInventory::prepare(config)?;
        let cids = match config.receive_backing_meter.as_ref() {
            Some(meter) if meter.budget_limit().is_some() && lifetime.is_none() => {
                Some(Pool::new(meter)?)
            }
            _ => None,
        };
        let packets = super::sent_history::History::prepare(config)?;
        let records = super::control_inventory::records::Pool::prepare(config)?;
        let ack_ranges = match config
            .receive_backing_meter
            .as_ref()
            .filter(|m| m.budget_limit().is_some())
        {
            Some(meter) => {
                let capacity = super::spaces::ACK_RANGE_CAPACITY;
                [
                    crate::range_set::ArrayRangeSet::cold(meter, capacity)?,
                    crate::range_set::ArrayRangeSet::cold(meter, capacity)?,
                    crate::range_set::ArrayRangeSet::cold(meter, capacity)?,
                ]
            }
            None => std::array::from_fn(|_| Default::default()),
        };
        Ok(Self {
            registration_key: None,
            native_registration: false,
            events,
            cids,
            packets,
            records,
            ack_ranges,
        })
    }
}

impl super::Connection {
    /// Registration identity used by the owning Endpoint's cold control ledger.
    #[doc(hidden)]
    pub fn registration_key(&self) -> Option<crate::RegistrationKey> {
        self.registration_key
    }

    pub(crate) fn install_endpoint_pacing(
        &mut self,
        pacing: Option<super::endpoint_pacing::ConnectionEndpointPacing>,
    ) {
        self.endpoint_pacing = pacing;
    }
    pub(crate) fn track_prepared_registration(
        &mut self,
        disposal: crate::endpoint::ConnectionDisposal,
    ) {
        self.prepared_disposal = Some(disposal);
    }
}

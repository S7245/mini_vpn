//! Cold borrowed DATA custody at the original TCP owner; no resource getter.
//! Counts actual permits and retained payload views without releasing a guard.
//! The DATA and staging Bytes may alias: these separate logical lengths are not
//! additive unique heap bytes. Entry: UplinkData::cold_inventory. Real Driver
//! rejection/retry tests live under OwnerTarget's effect_storage/faults.rs.

use super::{UplinkData, UplinkOwnership, UplinkPermits};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::owned_upstream) struct UplinkColdInventory {
    pub(in crate::owned_upstream) data_values: usize,
    pub(in crate::owned_upstream) payload_bytes: usize,
    pub(in crate::owned_upstream) ownership_values: usize,
    pub(in crate::owned_upstream) stored_bytes: usize,
    pub(in crate::owned_upstream) staging_payload_views: usize,
    pub(in crate::owned_upstream) staging_bytes: usize,
    pub(in crate::owned_upstream) permit_bundles: usize,
    pub(in crate::owned_upstream) flow_byte_permits: usize,
    pub(in crate::owned_upstream) session_byte_permits: usize,
    pub(in crate::owned_upstream) flow_segment_permits: usize,
    pub(in crate::owned_upstream) session_segment_permits: usize,
    pub(in crate::owned_upstream) factory_origins: usize,
    pub(in crate::owned_upstream) progress_guards: usize,
}

impl UplinkColdInventory {
    pub(in crate::owned_upstream) const fn empty() -> Self {
        Self {
            data_values: 0,
            payload_bytes: 0,
            ownership_values: 0,
            stored_bytes: 0,
            staging_payload_views: 0,
            staging_bytes: 0,
            permit_bundles: 0,
            flow_byte_permits: 0,
            session_byte_permits: 0,
            flow_segment_permits: 0,
            session_segment_permits: 0,
            factory_origins: 0,
            progress_guards: 0,
        }
    }
    pub(in crate::owned_upstream) const fn is_exact_zero(self) -> bool {
        self.data_values == 0
            && self.payload_bytes == 0
            && self.ownership_values == 0
            && self.stored_bytes == 0
            && self.staging_payload_views == 0
            && self.staging_bytes == 0
            && self.permit_bundles == 0
            && self.flow_byte_permits == 0
            && self.session_byte_permits == 0
            && self.flow_segment_permits == 0
            && self.session_segment_permits == 0
            && self.factory_origins == 0
            && self.progress_guards == 0
    }
    pub(in crate::owned_upstream) fn checked_add(self, other: Self) -> Option<Self> {
        Some(Self {
            data_values: self.data_values.checked_add(other.data_values)?,
            payload_bytes: self.payload_bytes.checked_add(other.payload_bytes)?,
            ownership_values: self.ownership_values.checked_add(other.ownership_values)?,
            stored_bytes: self.stored_bytes.checked_add(other.stored_bytes)?,
            staging_payload_views: self
                .staging_payload_views
                .checked_add(other.staging_payload_views)?,
            staging_bytes: self.staging_bytes.checked_add(other.staging_bytes)?,
            permit_bundles: self.permit_bundles.checked_add(other.permit_bundles)?,
            flow_byte_permits: self
                .flow_byte_permits
                .checked_add(other.flow_byte_permits)?,
            session_byte_permits: self
                .session_byte_permits
                .checked_add(other.session_byte_permits)?,
            flow_segment_permits: self
                .flow_segment_permits
                .checked_add(other.flow_segment_permits)?,
            session_segment_permits: self
                .session_segment_permits
                .checked_add(other.session_segment_permits)?,
            factory_origins: self.factory_origins.checked_add(other.factory_origins)?,
            progress_guards: self.progress_guards.checked_add(other.progress_guards)?,
        })
    }
}

impl UplinkData {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> UplinkColdInventory {
        let UplinkData {
            flow: _,
            payload,
            ownership,
        } = self;
        let mut inventory = ownership.cold_inventory();
        inventory.data_values = 1;
        inventory.payload_bytes = payload.len();
        inventory
    }
}
impl UplinkOwnership {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> UplinkColdInventory {
        let UplinkOwnership {
            flow: _,
            bytes,
            expected_start: _,
            expected_end: _,
            payload: staging,
            permits,
            origin,
        } = self;
        UplinkColdInventory {
            ownership_values: 1,
            stored_bytes: *bytes,
            staging_payload_views: usize::from(staging.is_some()),
            staging_bytes: staging.as_ref().map_or(0, |value| value.len()),
            factory_origins: usize::from(origin.is_some()),
            ..permit_inventory(permits)
        }
    }
}
impl super::UplinkReplayOwnership {
    pub(in crate::owned_upstream) fn cold_inventory(&self) -> UplinkColdInventory {
        let super::UplinkReplayOwnership {
            flow: _,
            direction: _,
            start: _,
            end: _,
            bytes,
            permits,
            origin,
        } = self;
        UplinkColdInventory {
            ownership_values: 1,
            stored_bytes: *bytes,
            factory_origins: usize::from(origin.is_some()),
            ..permit_inventory(permits)
        }
    }
}
fn permit_inventory(permits: &Option<UplinkPermits>) -> UplinkColdInventory {
    let mut inventory = UplinkColdInventory::empty();
    if let Some(UplinkPermits {
        _flow,
        _session,
        _flow_segment,
        _session_segment,
        _progress_on_drop: _,
    }) = permits
    {
        inventory.permit_bundles = 1;
        inventory.flow_byte_permits = _flow.num_permits();
        inventory.session_byte_permits = _session.num_permits();
        inventory.flow_segment_permits = _flow_segment.num_permits();
        inventory.session_segment_permits = _session_segment.num_permits();
        inventory.progress_guards = 1;
    }
    inventory
}

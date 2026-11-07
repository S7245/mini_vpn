//! SocketCtx's physical pending bytes and original capability descriptor stores.
//! Native prepares exact fixed allocations before carrier extraction; Legacy
//! keeps its original public queue semantics. DATA never grows Native storage.
//! This owns no receive window, ACK proof, Flow epoch or replacement authority.
//! Entry: prepare/append/consume/transfer; tests: storage/tests/mod.rs.
use super::*;
use crate::tcp_egress::{
    TcpEgressLease, TcpEgressPendingAppend, TcpEgressPendingConsume, TcpEgressPendingTransfer,
};
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::{
    alloc::{Layout, alloc},
    ops::Deref,
};

pub(in crate::client_tun) const PENDING_BYTES: usize = 65_536;

#[derive(Debug)]
pub(super) struct Cold {
    allowance: BackingBudgetReservation,
    pub(super) meter: BackingMeter,
}
impl Cold {
    pub(super) fn valid(&self, bytes: usize) -> bool {
        self.meter.admit_precharged().is_ok() && self.matches_layout(bytes)
    }
    pub(super) fn matches_layout(&self, bytes: usize) -> bool {
        self.meter.cold_layout_matches(Some(&self.allowance), bytes)
    }
}
pub(super) fn exact<T>(
    count: usize,
    meter: &BackingMeter,
) -> Result<(Vec<T>, Cold), MeteredCopyError> {
    let layout = Layout::array::<T>(count).map_err(|_| MeteredCopyError::TooLarge)?;
    let allowance = meter
        .reserve_cold_layout(layout.size())?
        .ok_or(MeteredCopyError::Quarantined)?;
    let ptr = unsafe { alloc(layout).cast::<T>() };
    if ptr.is_null() {
        return Err(MeteredCopyError::Allocation);
    }
    Ok((
        unsafe { Vec::from_raw_parts(ptr, 0, count) },
        Cold {
            allowance,
            meter: meter.clone(),
        },
    ))
}

#[derive(Debug, Default)]
pub(in crate::client_tun) struct PendingBytes {
    values: Vec<u8>, // Physical allocation drops before its allowance.
    cold: Option<Cold>,
}
impl From<Vec<u8>> for PendingBytes {
    fn from(values: Vec<u8>) -> Self {
        Self { values, cold: None }
    }
}
impl Deref for PendingBytes {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl PendingBytes {
    pub(in crate::client_tun) fn clear(&mut self) {
        self.values.clear();
    }
    pub(super) fn available(&self) -> usize {
        if self.native() {
            if self.can_append(0) {
                PENDING_BYTES - self.len()
            } else {
                0
            }
        } else {
            usize::MAX
        }
    }
    pub(super) fn quarantine(&self) {
        if let Some(cold) = &self.cold {
            cold.meter.quarantine();
        }
    }
    pub(super) fn native(&self) -> bool {
        self.cold.is_some()
    }
    fn can_append(&self, count: usize) -> bool {
        self.cold.as_ref().is_none_or(|cold| {
            self.values.capacity() == PENDING_BYTES
                && cold.valid(PENDING_BYTES)
                && self
                    .len()
                    .checked_add(count)
                    .is_some_and(|n| n <= PENDING_BYTES)
        })
    }
}

#[derive(Debug, Default)]
pub(in crate::client_tun) struct PermitStock {
    values: VecDeque<DownstreamBytePermit>,
    cold: Option<Cold>,
}
impl Deref for PermitStock {
    type Target = VecDeque<DownstreamBytePermit>;
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl PermitStock {
    pub(super) fn native(&self) -> bool {
        self.cold.is_some()
    }
    pub(super) fn front_mut(&mut self) -> Option<&mut DownstreamBytePermit> {
        self.values.front_mut()
    }
    pub(super) fn pop_front(&mut self) -> Option<DownstreamBytePermit> {
        self.values.pop_front()
    }
    pub(super) fn reserve(&mut self, pieces: usize) -> bool {
        match &self.cold {
            Some(cold) => {
                self.values.capacity() == 1
                    && self.len() <= 1
                    && cold.valid(Layout::new::<DownstreamBytePermit>().size())
            }
            None => self.values.try_reserve(pieces).is_ok(),
        }
    }
    pub(super) fn can_accept(&self, permit: Option<&DownstreamBytePermit>) -> bool {
        let Some(cold) = &self.cold else {
            return true;
        };
        self.values.capacity() == 1
            && self.len() <= 1
            && cold.valid(Layout::new::<DownstreamBytePermit>().size())
            && permit.is_none_or(|incoming| {
                incoming.remaining_bytes() > 0
                    && self.front().is_none_or(|front| front.can_merge(incoming))
            })
    }
    pub(super) fn insert(&mut self, permit: &mut Option<DownstreamBytePermit>) -> bool {
        if !self.can_accept(permit.as_ref()) {
            return false;
        }
        if self.native() {
            if let (Some(front), Some(incoming)) = (self.values.front_mut(), permit.as_mut()) {
                let n = incoming.remaining_bytes();
                if incoming.transfer_to(front, n) != Some(n) {
                    return false;
                }
                drop(permit.take());
                return true;
            }
        }
        if let Some(permit) = permit.take() {
            self.values.push_back(permit);
        }
        true
    }
}

/// All four allocations are prepared while the original attempt still owns
/// its carrier. No partial Native state is installed on known refusal.
fn prepare_for_owner(
    pending: &mut PendingBytes,
    caps: &mut PermitStock,
    ledger: &mut EgressPermitLedger,
    meter: &BackingMeter,
    owner: capture::Owner,
) -> Result<(), MeteredCopyError> {
    let inflight = &ledger.permits;
    if pending.native() {
        return if pending.can_append(0)
            && caps.can_accept(None)
            && inflight.can_accept(None)
            && ledger.capture.idle_for(meter)
            && [&pending.cold, &caps.cold, &inflight.cold]
                .iter()
                .all(|c| c.as_ref().is_some_and(|c| c.meter.same_authority(meter)))
        {
            if ledger.capture.owner() == Some(owner) {
                Ok(())
            } else if pending.is_empty() && caps.is_empty() && ledger.requalify_capture(owner) {
                Ok(())
            } else {
                Err(MeteredCopyError::Quarantined)
            }
        } else {
            Err(MeteredCopyError::Quarantined)
        };
    }
    if pending.capacity() != 0
        || caps.capacity() != 0
        || inflight.capacity() != 0
        || ledger.capture.native()
    {
        meter.quarantine();
        return Err(MeteredCopyError::Quarantined);
    }
    let (values, cold) = exact(PENDING_BYTES, meter)?;
    let next_pending = PendingBytes {
        values,
        cold: Some(cold),
    };
    let (values, cold) = exact::<DownstreamBytePermit>(1, meter)?;
    let next_caps = PermitStock {
        values: VecDeque::from(values),
        cold: Some(cold),
    };
    let (values, cold) = exact::<DownstreamBytePermit>(1, meter)?;
    let next_inflight = PermitStock {
        values: VecDeque::from(values),
        cold: Some(cold),
    };
    let capture = capture::Slot::prepare(meter, owner)?;
    *pending = next_pending;
    *caps = next_caps;
    ledger.permits = next_inflight;
    ledger.capture = capture;
    Ok(())
}

pub(in crate::client_tun) fn preflight_append(
    pending: &PendingBytes,
    caps: &PermitStock,
    bytes: usize,
    permit: Option<&DownstreamBytePermit>,
) -> bool {
    pending.native() == caps.native()
        && pending.can_append(bytes)
        && caps.can_accept(permit)
        && (!pending.native() || permit.is_none_or(|p| p.remaining_bytes() == bytes))
}

pub(in crate::client_tun) fn append(
    pending: &mut PendingBytes,
    caps: &mut PermitStock,
    payload: &[u8],
    permit: &mut Option<DownstreamBytePermit>,
) -> Option<TcpEgressPendingAppend> {
    if !preflight_append(pending, caps, payload.len(), permit.as_ref()) {
        return None;
    }
    if !pending.native() {
        return Some(
            TcpEgressPendingQueueView::new(&mut pending.values, &mut caps.values)
                .append(payload, permit.take()),
        );
    }
    let retained = permit
        .as_ref()
        .map_or(0, DownstreamBytePermit::remaining_bytes);
    if !caps.insert(permit) {
        return None;
    }
    pending.values.extend_from_slice(payload);
    Some(TcpEgressPendingAppend {
        appended_bytes: payload.len(),
        released_excess_bytes: 0,
        retained_lease_bytes: retained,
        pending_bytes_after: pending.len(),
        leased_bytes_after: sum(caps),
    })
}

fn sum(caps: &PermitStock) -> usize {
    caps.iter().map(DownstreamBytePermit::remaining_bytes).sum()
}

pub(in crate::client_tun) fn consume(
    pending: &mut PendingBytes,
    caps: &mut PermitStock,
    bytes: usize,
) -> TcpEgressPendingConsume {
    if !pending.native() {
        return TcpEgressPendingQueueView::new(&mut pending.values, &mut caps.values)
            .consume_prefix(bytes);
    }
    let consumed = bytes.min(pending.len());
    let released = caps.front_mut().map_or(0, |p| p.release(consumed));
    if caps.front().is_some_and(|p| p.remaining_bytes() == 0) {
        caps.pop_front();
    }
    pending.values.drain(..consumed);
    TcpEgressPendingConsume {
        consumed_bytes: consumed,
        released_lease_bytes: released,
        pending_bytes_after: pending.len(),
        leased_bytes_after: sum(caps),
    }
}

pub(in crate::client_tun) fn transfer(
    pending: &mut PendingBytes,
    caps: &mut PermitStock,
    inflight: &mut PermitStock,
    bytes: usize,
) -> Option<TcpEgressPendingTransfer> {
    if !pending.native() {
        return Some(
            TcpEgressPendingQueueView::new(&mut pending.values, &mut caps.values)
                .transfer_prefix_to(bytes, &mut inflight.values),
        );
    }
    let transferred = bytes.min(pending.len());
    // The maximum transfer was checked before socket publication. Keep both
    // original capabilities in their owners while updating their byte split.
    let leased = match (caps.front_mut(), inflight.front_mut()) {
        (Some(source), Some(target)) => source.transfer_to(target, transferred)?,
        (Some(source), None) => {
            let moved = source.split_to(transferred);
            let n = moved
                .as_ref()
                .map_or(0, DownstreamBytePermit::remaining_bytes);
            if let Some(moved) = moved {
                inflight.values.push_back(moved);
            }
            n
        }
        (None, _) => 0,
    };
    if caps.front().is_some_and(|p| p.remaining_bytes() == 0) {
        caps.pop_front();
    }
    if leased != transferred {
        return None;
    }
    pending.values.drain(..transferred);
    Some(TcpEgressPendingTransfer {
        transferred_bytes: transferred,
        transferred_lease_bytes: leased,
        pending_bytes_after: pending.len(),
        pending_leased_bytes_after: sum(caps),
        inflight_leased_bytes_after: sum(inflight),
    })
}

pub(in crate::client_tun) fn clear(
    pending: &mut PendingBytes,
    caps: &mut PermitStock,
) -> TcpEgressPendingConsume {
    if !pending.native() {
        return TcpEgressPendingQueueView::new(&mut pending.values, &mut caps.values).clear();
    }
    consume(pending, caps, pending.len())
}

#[cfg(test)]
mod tests;

// Cold batch preflight: duplicated physical mode and allowance identity agree.
pub(super) fn capture_inventory(ctx: &SocketCtx, meter: &BackingMeter) -> bool {
    let p = &ctx.downlink_pending;
    let c = &ctx.downlink_pending_permits;
    let i = &ctx.downlink_inflight_permits.permits;
    p.values.capacity() == PENDING_BYTES
        && p.len() <= PENDING_BYTES
        && c.values.capacity() == 1
        && c.len() <= 1
        && i.values.capacity() == 1
        && i.len() <= 1
        && [
            (&p.cold, PENDING_BYTES),
            (&c.cold, Layout::new::<DownstreamBytePermit>().size()),
            (&i.cold, Layout::new::<DownstreamBytePermit>().size()),
        ]
        .iter()
        .all(|(cold, size)| {
            cold.as_ref()
                .is_some_and(|cold| cold.meter.same_authority(meter) && cold.matches_layout(*size))
        })
}
pub(super) fn any_native(ctx: &SocketCtx) -> bool {
    ctx.downlink_pending.native()
        || ctx.downlink_pending_permits.native()
        || ctx.downlink_inflight_permits.permits.native()
        || ctx.downlink_inflight_permits.capture.native()
}
pub(in crate::client_tun) fn pristine(ctx: &SocketCtx) -> bool {
    !any_native(ctx)
        && ctx.downlink_pending.capacity() == 0
        && ctx.downlink_pending_permits.capacity() == 0
        && ctx.downlink_inflight_permits.capture_pristine()
}

#[cfg(test)]
pub(in crate::client_tun) fn capture_layout_bytes() -> usize {
    capture::Slot::layout_bytes()
}

/// Only the original production Flow Ready gate issues this cold qualification.
pub(in crate::client_tun) fn prepare_for_flow(
    pending: &mut PendingBytes,
    caps: &mut PermitStock,
    ledger: &mut EgressPermitLedger,
    meter: &BackingMeter,
    identity: (usize, SocketHandle, u64, u64),
) -> Result<(), MeteredCopyError> {
    prepare_for_owner(pending, caps, ledger, meter, capture::Owner::Flow(identity))
}
#[cfg(test)]
pub(in crate::client_tun) use tests::prepare;

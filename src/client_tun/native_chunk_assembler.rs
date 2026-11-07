//! Original Native TCP offset reassembly and bounded gap storage.
//! Owns ordered chunks and diagnostics; no reader task, queue, F, or session
//! authority. Entry: `NativeTcpChunkAssembler`; characterization lives in
//! `client_tun/tests/mod.rs`.

use super::*;
use crate::tcp_downlink_pump::{CustodiedPayload, MeteredMutation, OwnedPayload, PreparedPayload};
use bytes::{BackingMeter, MeteredCopyError};
mod storage;

const NATIVE_TCP_GAP_LOG_MS: u128 = 1_000;

#[derive(Debug, Default)]
pub(super) struct NativeTcpChunkAssembler {
    next_offset: u64,
    chunks: storage::Inventory,
    meter: Option<BackingMeter>,
    buffered_bytes: usize,
    max_buffered_bytes: usize,
    raw_chunks: u64,
    raw_bytes: u64,
    out_of_order_chunks: u64,
    max_gap_bytes: u64,
    cap_hits: u64,
    last_gap_log_at: Option<std::time::Instant>,
    discarded_bytes: usize,
    accounting_failed: bool,
}

impl NativeTcpChunkAssembler {
    pub(super) fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    pub(super) fn discarded_bytes(&self) -> Option<usize> {
        (!self.accounting_failed).then_some(self.discarded_bytes)
    }

    /// Cold terminal check before the reader releases buffered source slices.
    /// The Node meter owns unique allocation charges; this owner sums the
    /// retained logical views without adding per-chunk audit state.
    pub(super) fn metered_buffered_bytes(&self, expected: &BackingMeter) -> Option<usize> {
        if self.accounting_failed
            || expected.is_quarantined()
            || !self
                .meter
                .as_ref()
                .is_some_and(|actual| expected.same_authority(actual))
            || !self.chunks.valid_inventory(self.meter.as_ref())
        {
            expected.quarantine();
            if let Some(actual) = &self.meter {
                actual.quarantine();
            }
            return None;
        }
        let mut total = 0usize;
        for chunk in self.chunks.values() {
            let actual = chunk.backing_meter();
            if chunk.is_empty()
                || chunk.allocation_identity().is_none()
                || !actual
                    .as_ref()
                    .is_some_and(|actual| expected.same_authority(actual))
            {
                expected.quarantine();
                if let Some(actual) = actual {
                    actual.quarantine();
                }
                return None;
            }
            let Some(next) = total.checked_add(chunk.len()) else {
                expected.quarantine();
                return None;
            };
            total = next;
        }
        if total != self.buffered_bytes {
            expected.quarantine();
            return None;
        }
        Some(total)
    }

    pub(super) fn pristine_unmetered(&self) -> bool {
        self.meter.is_none()
            && !self.chunks.has_storage()
            && self.chunks.valid_inventory(None)
            && self.next_offset == 0
            && self.buffered_bytes == 0
            && self.raw_chunks == 0
            && self.raw_bytes == 0
            && self.discarded_bytes == 0
            && !self.accounting_failed
    }

    fn discard(&mut self, bytes: usize) {
        if let Some(total) = self.discarded_bytes.checked_add(bytes) {
            self.discarded_bytes = total;
        } else {
            self.accounting_failed = true;
        }
    }

    pub(super) fn with_meter(meter: Option<BackingMeter>) -> Self {
        Self {
            meter,
            ..Self::default()
        }
    }

    /// Reserve one unknown next offset before taking input. Capacity remains
    /// owned by this assembler, including after duplicate/redundant input.
    pub(super) fn prepare_next_input(&mut self) -> Result<(), MeteredCopyError> {
        if let Some(meter) = &self.meter { meter.admit_precharged()?; }
        let new = self.chunks.prepare_next(self.meter.as_ref())?;
        if new.is_some() {
            let mutation = MeteredMutation::begin(self.meter.as_ref())?;
            self.chunks.install(new);
            mutation.commit();
        }
        Ok(())
    }

    pub(super) fn push_chunk(
        &mut self,
        chunk: NativeTcpChunk,
    ) -> Result<(), (MeteredCopyError, NativeTcpChunk)> {
        let actual = chunk.bytes.backing_meter();
        if self.meter.is_none() && !self.chunks.has_storage() {
            self.meter = actual.clone();
        }
        if let Some(expected) = &self.meter {
            if !actual.as_ref().is_some_and(|m| expected.same_authority(m))
                || expected.is_quarantined()
            {
                expected.quarantine();
                if let Some(actual) = actual {
                    actual.quarantine();
                }
                return Err((MeteredCopyError::Quarantined, chunk));
            }
        } else if let Some(actual) = actual {
            actual.quarantine();
            return Err((MeteredCopyError::Quarantined, chunk));
        }
        if chunk.bytes.is_empty() {
            return Ok(());
        }
        let original_len = chunk.bytes.len();
        let end = chunk.offset.saturating_add(original_len as u64);
        if end <= self.next_offset {
            self.discard(original_len);
            return Ok(());
        }
        let offset = chunk.offset.max(self.next_offset);
        let trim = (offset - chunk.offset) as usize;
        let bytes_len = original_len - trim;
        let redundant = self
            .chunks
            .get(&offset)
            .is_some_and(|old| old.len() >= bytes_len);
        let new = if redundant {
            None
        } else {
            match self.chunks.prepare(offset, self.meter.as_ref()) {
                Ok(new) => new,
                Err(error) => return Err((error, chunk)),
            }
        };
        let mutation = match MeteredMutation::begin(self.meter.as_ref()) {
            Ok(m) => m,
            Err(error) => return Err((error, chunk)),
        };
        // Preparation is complete. No allocation or opaque callback below.
        self.chunks.install(new);
        self.discard(trim);
        let gap = offset.saturating_sub(self.next_offset);
        self.raw_chunks = self.raw_chunks.saturating_add(1);
        self.raw_bytes = self.raw_bytes.saturating_add(bytes_len as u64);
        if gap > 0 {
            self.out_of_order_chunks = self.out_of_order_chunks.saturating_add(1);
            self.max_gap_bytes = self.max_gap_bytes.max(gap);
        }
        if redundant {
            self.discard(bytes_len);
            mutation.commit();
            return Ok(());
        }
        let bytes = chunk.bytes.slice(trim..);
        if let Some(old) = self.chunks.insert(offset, bytes) {
            self.buffered_bytes -= old.len();
            self.discard(old.len());
        }
        self.buffered_bytes += bytes_len;
        self.max_buffered_bytes = self.max_buffered_bytes.max(self.buffered_bytes);
        mutation.commit();
        Ok(())
    }

    pub(super) fn drain_ordered_up_to(
        &mut self,
        limit: usize,
    ) -> Result<Option<Bytes>, MeteredCopyError> {
        self.drain_owned_up_to(limit)?.map(|payload| {
            payload.try_into_legacy_bytes().map_err(|payload| {
                if let Some(meter) = payload.meter() { meter.quarantine(); }
                MeteredCopyError::Quarantined
            })
        }).transpose()
    }

    pub(super) fn drain_owned_up_to(
        &mut self,
        limit: usize,
    ) -> Result<Option<OwnedPayload>, MeteredCopyError> {
        self.drain_output(limit, None)
    }

    /// The original reader keeps this already-admitted operation until FIFO
    /// publication. No output allocation or tree consumption precedes it.
    pub(super) fn drain_admitted(
        &mut self,
        space: &mut Option<crate::tcp_downlink_pump::OutputSpace>,
        limit: usize,
    ) -> Result<Option<OwnedPayload>, MeteredCopyError> {
        let Some(prepared) = space.as_ref() else {
            if let Some(meter) = &self.meter { meter.quarantine(); }
            return Err(MeteredCopyError::Quarantined);
        };
        prepared.validate_meter(self.meter.as_ref())?;
        self.drain_output(prepared.capacity().min(limit), Some(space))
    }

    fn drain_output(
        &mut self, limit: usize,
        mut space: Option<&mut Option<crate::tcp_downlink_pump::OutputSpace>>,
    ) -> Result<Option<OwnedPayload>, MeteredCopyError> {
        if let Some(meter) = &self.meter {
            meter.admit_precharged()?;
        }
        if limit == 0 {
            return Ok(None);
        }
        // Read-only plan. A capacity refusal leaves the entire original inventory.
        let (mut next, mut output_len, mut search) = (self.next_offset, 0usize, 0u64);
        while let Some((offset, bytes)) = self.chunks.at_or_after(search) {
            if offset > next || output_len == limit {
                break;
            }
            let trim = next.saturating_sub(offset).min(bytes.len() as u64) as usize;
            let n = (bytes.len() - trim).min(limit - output_len);
            output_len += n;
            next = next.saturating_add(n as u64);
            let Some(after) = offset.checked_add(1) else {
                break;
            };
            search = after;
        }
        if output_len == 0 {
            let mutation = MeteredMutation::begin(self.meter.as_ref())?;
            while let Some((offset, bytes)) = self.chunks.first() {
                if offset.saturating_add(bytes.len() as u64) > self.next_offset {
                    break;
                }
                let len = bytes.len();
                drop(self.chunks.remove_first());
                self.buffered_bytes -= len;
                self.discard(len);
            }
            mutation.commit();
            return Ok(None);
        }
        let mut prepared = if space.is_none() {
            Some(PreparedPayload::new(output_len, self.meter.as_ref())?)
        } else { None };
        let (mut next, mut written, mut search) = (self.next_offset, 0usize, 0u64);
        while written < output_len {
            let Some((offset, bytes)) = self.chunks.at_or_after(search) else {
                break;
            };
            let trim = next.saturating_sub(offset).min(bytes.len() as u64) as usize;
            let n = (bytes.len() - trim).min(output_len - written);
            if let Some(prepared) = &mut prepared {
                prepared.as_mut()[written..written + n].copy_from_slice(&bytes[trim..trim + n]);
            } else {
                space.as_mut().and_then(|slot| slot.as_mut())
                    .ok_or(MeteredCopyError::Quarantined)?.write(&bytes[trim..trim + n])?;
            }
            written += n;
            next = next.saturating_add(n as u64);
            let Some(after) = offset.checked_add(1) else {
                break;
            };
            search = after;
        }
        // Stock finalization can fail. Keep every original input/offset until
        // the actual typed view exists; only then commit the planned prefix.
        let mutation = MeteredMutation::begin(self.meter.as_ref())?;
        #[cfg(test)]
        tests::before_output_finish(self.meter.as_ref());
        let output = match prepared {
            Some(prepared) => prepared.finish(),
            None => OwnedPayload::Native(crate::tcp_downlink_pump::OutputSpace::finish_retained(
                space.ok_or(MeteredCopyError::Quarantined)?)?),
        };
        // Final infallible commit, including accounting for any displaced remainder.
        let mut remaining = output_len;
        while remaining != 0 {
            let Some((offset, mut bytes)) = self.chunks.remove_first() else {
                break;
            };
            self.buffered_bytes -= bytes.len();
            let trim = self
                .next_offset
                .saturating_sub(offset)
                .min(bytes.len() as u64) as usize;
            self.discard(trim);
            bytes = bytes.slice(trim..);
            let n = bytes.len().min(remaining);
            self.next_offset = self.next_offset.saturating_add(n as u64);
            remaining -= n;
            if n < bytes.len() {
                let rest = bytes.slice(n..);
                self.buffered_bytes += rest.len();
                if let Some(old) = self.chunks.insert(self.next_offset, rest) {
                    self.buffered_bytes -= old.len();
                    self.discard(old.len());
                }
            }
        }
        mutation.commit();
        Ok(Some(output))
    }

    pub(super) fn note_cap_hit(&mut self) {
        self.cap_hits = self.cap_hits.saturating_add(1);
    }

    pub(super) fn maybe_log_gap(&mut self, handle: SocketHandle, epoch: u64, reason: &'static str) {
        let Some((chunk_offset, chunk)) = self.chunks.first() else {
            return;
        };
        if chunk_offset <= self.next_offset {
            return;
        }
        let now = std::time::Instant::now();
        if self
            .last_gap_log_at
            .map(|last| now.saturating_duration_since(last).as_millis() < NATIVE_TCP_GAP_LOG_MS)
            .unwrap_or(false)
        {
            return;
        }
        self.last_gap_log_at = Some(now);
        tcp_diag_log!(
            "🔎 tcp-native-chunk-gap handle={:?} epoch={} reason={} next_offset={} first_chunk_offset={} first_chunk_bytes={} gap_bytes={} max_gap_bytes={} buffered={}B buffered_chunks={} max_buffered={}B raw_chunks={} raw_bytes={} out_of_order_chunks={} cap_hits={} cap={}B",
            handle,
            epoch,
            reason,
            self.next_offset,
            chunk_offset,
            chunk.len(),
            chunk_offset.saturating_sub(self.next_offset),
            self.max_gap_bytes,
            self.buffered_bytes,
            self.chunks.len(),
            self.max_buffered_bytes,
            self.raw_chunks,
            self.raw_bytes,
            self.out_of_order_chunks,
            self.cap_hits,
            NATIVE_TCP_REASSEMBLY_MAX_BYTES
        );
    }
}

#[cfg(test)]
#[path = "native_chunk_assembler/tests/mod.rs"]
mod tests;

#[derive(Debug)]
struct CapacityError(MeteredCopyError);
impl std::fmt::Display for CapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Native assembly admission: {:?}", self.0)
    }
}
impl std::error::Error for CapacityError {}
pub(super) fn capacity_error(error: MeteredCopyError) -> std::io::Error {
    use std::io::ErrorKind;
    let kind = match error {
        MeteredCopyError::BudgetFull | MeteredCopyError::TooLarge => ErrorKind::WouldBlock,
        MeteredCopyError::Allocation => ErrorKind::OutOfMemory,
        MeteredCopyError::Quarantined => ErrorKind::InvalidData,
    };
    std::io::Error::new(kind, CapacityError(error))
}

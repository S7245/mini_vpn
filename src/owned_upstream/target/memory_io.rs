//! MemoryTarget allocation preparation and external feed/drain mutations.
//!
//! Owns original construction and inherent feed/drain mutations on one Target.
//! The private flow_io child owns TargetIo, using these original preparations.
//! Nominal storage/configuration stay parent; no second lifecycle owner.
//! Tests exercise the real TargetIo and feed/drain entries in parent tests.

mod flow_io;

use super::*;
use crate::resumable::effect_storage::{
    EffectStorageError, allocate_exact_vec, growth_peak, next_capacity,
};

#[cfg(test)]
std::thread_local! {
    static FAIL_OUTPUT_ALLOCATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_BUFFER_ALLOCATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_ADDRESS_ALLOCATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn fail_next_output_allocation_for_test() {
    assert!(!FAIL_OUTPUT_ALLOCATION.replace(true));
}

#[cfg(test)]
pub(super) fn fail_next_buffer_allocation_for_test() {
    assert!(!FAIL_BUFFER_ALLOCATION.replace(true));
}

#[cfg(test)]
pub(super) fn fail_next_address_allocation_for_test() {
    assert!(!FAIL_ADDRESS_ALLOCATION.replace(true));
}

fn prepare_address(target: &TargetAddr) -> Result<TargetAddr, TargetIoError> {
    match target {
        TargetAddr::IpPort(address) => Ok(TargetAddr::IpPort(*address)),
        TargetAddr::DomainPort { host, port } => {
            // The borrowed caller still owns its original allocation. Check
            // that overlap without imposing the protocol's bound on raw I/O.
            growth_peak::<u8>(host.capacity(), host.len())
                .map_err(|_| TargetIoError::StorageLayout)?;
            #[cfg(test)]
            if !host.is_empty() && FAIL_ADDRESS_ALLOCATION.replace(false) {
                return Err(TargetIoError::StorageReserve);
            }
            let mut bytes = allocate_exact_vec(host.len()).map_err(|error| match error {
                EffectStorageError::Reserve => TargetIoError::StorageReserve,
                _ => TargetIoError::StorageLayout,
            })?;
            for byte in host.as_bytes() {
                bytes.push(*byte);
            }
            // Source String is valid UTF-8; checked conversion reuses this Vec.
            // All fallible work stays before original OPEN identity publication.
            let host = String::from_utf8(bytes).map_err(|_| TargetIoError::StorageLayout)?;
            Ok(TargetAddr::DomainPort { host, port: *port })
        }
    }
}

fn prepare_buffer<T: Copy>(
    original: &VecDeque<T>,
    need: usize,
    maximum: usize,
) -> Result<Option<VecDeque<T>>, TargetIoError> {
    fn cause(error: EffectStorageError) -> TargetIoError {
        match error {
            EffectStorageError::Reserve => TargetIoError::StorageReserve,
            EffectStorageError::Layout
            | EffectStorageError::Bound
            | EffectStorageError::Overflow => TargetIoError::StorageLayout,
        }
    }
    if need < original.len() {
        return Err(TargetIoError::StorageLayout);
    }
    let capacity =
        next_capacity(original.capacity(), need, maximum.min(8), maximum).map_err(cause)?;
    if capacity == original.capacity() {
        return Ok(None);
    }
    growth_peak::<T>(original.capacity(), capacity).map_err(cause)?;
    #[cfg(test)]
    if FAIL_BUFFER_ALLOCATION.replace(false) {
        return Err(TargetIoError::StorageReserve);
    }
    let mut prepared = allocate_exact_vec(capacity).map_err(cause)?;
    // Copy cannot invoke user Clone/Drop. Wrapped FIFO order is preserved;
    // each push is below the exact capacity prepared for the final need.
    for value in original.iter().copied() {
        prepared.push(value);
    }
    // Same-T Vec -> VecDeque preserves the allocation, without reallocation.
    Ok(Some(prepared.into()))
}

fn prepare_output(original: &VecDeque<u8>, count: usize) -> Result<Bytes, TargetIoError> {
    fn cause(error: EffectStorageError) -> TargetIoError {
        match error {
            EffectStorageError::Reserve => TargetIoError::StorageReserve,
            EffectStorageError::Layout
            | EffectStorageError::Bound
            | EffectStorageError::Overflow => TargetIoError::StorageLayout,
        }
    }
    growth_peak::<u8>(original.capacity(), count).map_err(cause)?;
    #[cfg(test)]
    if count != 0 && FAIL_OUTPUT_ALLOCATION.replace(false) {
        return Err(TargetIoError::StorageReserve);
    }
    let mut prepared = allocate_exact_vec(count).map_err(cause)?;
    // Callers have frozen count <= original.len() under the exclusive borrow.
    // Single-item push cannot grow this exact capacity; no source bytes move yet.
    for byte in original.iter().take(count) {
        prepared.push(*byte);
    }
    // len == capacity: pinned Bytes owns this allocation, without Shared backing.
    // Even this conversion is before any source/accounting/notification commit.
    Ok(Bytes::from(prepared))
}

impl MemoryTarget {
    pub(crate) fn new(config: MemoryTargetConfig) -> Self {
        Self {
            config,
            ready: MemoryReadiness::new(config.max_flows),
            storage: MemoryTargetStorage::default(),
            owned_address_capacity: 0,
            snapshot: MemoryTargetSnapshot::default(),
            buffered_bytes: 0,
            buffer_high_water: 0,
            queued_write_directives: 0,
            directive_high_water: 0,
            highest_flow_id: None,
        }
    }

    pub(crate) fn snapshot(&self) -> MemoryTargetSnapshot {
        let mut snapshot = self.snapshot;
        snapshot.live_flows = self.storage.len();
        snapshot.joined_flows = self.storage.joined_len;
        snapshot.buffered_bytes = self.buffered_bytes;
        snapshot.buffer_high_water = self.buffer_high_water;
        snapshot.queued_write_directives = self.queued_write_directives;
        snapshot.directive_high_water = self.directive_high_water;
        snapshot.highest_flow_id = self.highest_flow_id;
        snapshot
    }

    pub(crate) fn push_write_directive(
        &mut self,
        flow_id: SessionFlowId,
        directive: MemoryWriteDirective,
    ) -> Result<(), TargetIoError> {
        let flow = self
            .storage
            .get_mut(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        flow.require_output_idle(flow_id)?;
        if let Some(kind) = flow.terminal {
            return Err(TargetIoError::FlowTerminal { flow_id, kind });
        }
        if flow.write_directives.len() >= self.config.max_write_directives_per_flow {
            return Err(TargetIoError::WriteDirectiveLimit {
                flow_id,
                max: self.config.max_write_directives_per_flow,
            });
        }
        let queued_write_directives =
            self.queued_write_directives
                .checked_add(1)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "queued_write_directives",
                })?;
        let ready = self
            .ready
            .prepare(flow_id, flow, ReadyChange::WriteDirective(true))?;
        let need = flow
            .write_directives
            .len()
            .checked_add(1)
            .ok_or(TargetIoError::StorageLayout)?;
        let prepared = prepare_buffer(
            &flow.write_directives,
            need,
            self.config.max_write_directives_per_flow,
        )?;
        if let Some(prepared) = prepared {
            flow.write_directives = prepared;
        }
        flow.write_directives.push_back(directive);
        self.queued_write_directives = queued_write_directives;
        self.directive_high_water = self.directive_high_water.max(self.queued_write_directives);
        self.ready.publish(ready);
        Ok(())
    }

    fn terminalize(
        &mut self,
        flow_id: SessionFlowId,
        kind: TargetTerminalKind,
    ) -> Result<TerminalizeOutcome, TargetIoError> {
        let flow = self
            .storage
            .get(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        if let Some(existing) = flow.terminal {
            return Ok(TerminalizeOutcome::Existing(existing));
        }
        let released = flow.released_buffers()?;
        let released_total = released
            .read_bytes
            .checked_add(released.written_bytes)
            .ok_or(TargetIoError::CounterOverflow {
                counter: "released_buffered_bytes",
            })?;
        let released_directives = flow.write_directives.len();
        let buffered_bytes = self.buffered_bytes.checked_sub(released_total).ok_or(
            TargetIoError::CounterOverflow {
                counter: "buffered_bytes",
            },
        )?;
        let queued_write_directives = self
            .queued_write_directives
            .checked_sub(released_directives)
            .ok_or(TargetIoError::CounterOverflow {
                counter: "queued_write_directives",
            })?;
        let ready = self.ready.prepare(flow_id, flow, ReadyChange::Terminal)?;
        let flow = self
            .storage
            .get_mut(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        flow.readable.clear();
        flow.written.clear();
        flow.write_directives.clear();
        flow.output = MemoryTargetOutput::Idle;
        flow.terminal = Some(kind);
        self.buffered_bytes = buffered_bytes;
        self.queued_write_directives = queued_write_directives;
        Ok(TerminalizeOutcome::Changed(released, ready))
    }

    pub(crate) fn feed_read(
        &mut self,
        flow_id: SessionFlowId,
        bytes: Bytes,
    ) -> Result<TargetReadFeedCompletion, TargetIoError> {
        if bytes.is_empty() {
            return Err(TargetIoError::EmptyReadFeed);
        }
        let flow = self
            .storage
            .get_mut(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        flow.require_output_idle(flow_id)?;
        if let Some(kind) = flow.terminal {
            return Err(TargetIoError::FlowTerminal { flow_id, kind });
        }
        if flow.read_eof_queued {
            return Err(TargetIoError::ReadEofAlreadyQueued { flow_id });
        }
        let flow_available = self
            .config
            .per_flow_buffer_bytes
            .saturating_sub(flow.buffered_bytes());
        let global_available = self
            .config
            .global_buffer_bytes
            .saturating_sub(self.buffered_bytes);
        let available = flow_available.min(global_available);
        if bytes.len() > available {
            return Err(TargetIoError::BufferCapacityExceeded {
                flow_id,
                requested: bytes.len(),
                available,
            });
        }
        let len = bytes.len();
        let buffered_bytes =
            self.buffered_bytes
                .checked_add(len)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "buffered_bytes",
                })?;
        let ready = self.ready.prepare(flow_id, flow, ReadyChange::Read)?;
        let need = flow
            .readable
            .len()
            .checked_add(len)
            .ok_or(TargetIoError::StorageLayout)?;
        let prepared = prepare_buffer(
            &flow.readable,
            need,
            self.config
                .per_flow_buffer_bytes
                .min(self.config.global_buffer_bytes),
        )?;
        if let Some(prepared) = prepared {
            flow.readable = prepared;
        }
        for byte in bytes.iter().copied() {
            flow.readable.push_back(byte);
        }
        self.buffered_bytes = buffered_bytes;
        self.buffer_high_water = self.buffer_high_water.max(self.buffered_bytes);
        self.ready.publish(ready);
        // An owned Bytes may call user Drop; run it only after the copied data,
        // accounting and registration/publication state are committed.
        drop(bytes);
        Ok(TargetReadFeedCompletion::Queued { bytes: len })
    }

    pub(crate) fn finish_read(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetReadEofCompletion, TargetIoError> {
        let flow = self
            .storage
            .get_mut(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        flow.require_output_idle(flow_id)?;
        if let Some(kind) = flow.terminal {
            return Err(TargetIoError::FlowTerminal { flow_id, kind });
        }
        if flow.read_eof_queued {
            return Ok(TargetReadEofCompletion::AlreadyQueued);
        }
        let ready = self.ready.prepare(flow_id, flow, ReadyChange::Read)?;
        flow.read_eof_queued = true;
        self.ready.publish(ready);
        Ok(TargetReadEofCompletion::Queued)
    }

    /// Drains bytes already accepted by the memory Target application.
    pub(crate) fn take_written(
        &mut self,
        flow_id: SessionFlowId,
        max_bytes: usize,
    ) -> Result<Bytes, TargetIoError> {
        if max_bytes == 0 {
            return Err(TargetIoError::ZeroDrainLimit);
        }
        let flow = self
            .storage
            .get_mut(&flow_id)
            .ok_or(TargetIoError::UnknownFlow { flow_id })?;
        flow.require_output_idle(flow_id)?;
        let count = max_bytes.min(flow.written.len());
        let buffered_bytes =
            self.buffered_bytes
                .checked_sub(count)
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "buffered_bytes",
                })?;
        let ready = self
            .ready
            .prepare(flow_id, flow, ReadyChange::BytesReleased(count))?;
        let bytes = prepare_output(&flow.written, count)?;
        flow.output = MemoryTargetOutput::Publishing {
            direction: MemoryTargetOutputDirection::Written,
            bytes,
        };
        drop(flow.written.drain(..count));
        self.ready.publish(ready);
        self.buffered_bytes = buffered_bytes;
        Ok(flow.output.finish())
    }
}

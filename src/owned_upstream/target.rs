//! Transport-independent Target socket boundary for one session owner.
//!
//! The sole session supervisor consumes these typed I/O completions and is
//! responsible for translating them into reducer events. Implementations of
//! this module never mint session, ACK, sink, or terminal authority.

use std::collections::VecDeque;
use std::fmt;
use std::num::NonZeroUsize;
use std::task::{Context, Poll};

use crate::resumable::SessionFlowId;
use crate::shared::TargetAddr;
use bytes::Bytes;

mod memory_io;
mod memory_ready;
use memory_ready::{MemoryReadiness, ReadyChange};

/// Closed configuration for one session's Target adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MemoryTargetConfig {
    max_flows: usize,
    max_terminal_tombstones: usize,
    per_flow_buffer_bytes: usize,
    global_buffer_bytes: usize,
    max_read_chunk: usize,
    max_write_chunk: usize,
    max_write_directives_per_flow: usize,
    max_write_directives: usize,
    requested_storage_peak: std::alloc::Layout,
}

impl MemoryTargetConfig {
    pub(crate) fn new(
        max_flows: usize,
        per_flow_buffer_bytes: usize,
        global_buffer_bytes: usize,
        max_read_chunk: usize,
        max_write_chunk: usize,
        max_write_directives_per_flow: usize,
    ) -> Result<Self, TargetIoConfigError> {
        let values = [
            ("max_flows", max_flows),
            ("per_flow_buffer_bytes", per_flow_buffer_bytes),
            ("global_buffer_bytes", global_buffer_bytes),
            ("max_read_chunk", max_read_chunk),
            ("max_write_chunk", max_write_chunk),
            (
                "max_write_directives_per_flow",
                max_write_directives_per_flow,
            ),
        ];
        for (field, value) in values {
            if value == 0 {
                return Err(TargetIoConfigError::ZeroCapacity { field });
            }
        }
        let max_write_directives = max_flows
            .checked_mul(max_write_directives_per_flow)
            .ok_or(TargetIoConfigError::DirectiveCapacityOverflow)?;
        let mut config = Self {
            max_flows,
            // Preserve the original constructor while keeping the retained
            // terminal history explicitly finite. Callers whose reducer uses
            // a different tombstone bound must override this with the checked
            // builder below.
            max_terminal_tombstones: max_flows,
            per_flow_buffer_bytes,
            global_buffer_bytes,
            max_read_chunk,
            max_write_chunk,
            max_write_directives_per_flow,
            max_write_directives,
            requested_storage_peak: std::alloc::Layout::new::<MemoryTarget>(),
        };
        config.requested_storage_peak = MemoryTargetStorage::checked_peak(&config)?;
        Ok(config)
    }

    pub(crate) fn with_max_terminal_tombstones(
        mut self,
        max_terminal_tombstones: usize,
    ) -> Result<Self, TargetIoConfigError> {
        if max_terminal_tombstones == 0 {
            return Err(TargetIoConfigError::ZeroCapacity {
                field: "max_terminal_tombstones",
            });
        }
        self.max_terminal_tombstones = max_terminal_tombstones;
        self.requested_storage_peak = MemoryTargetStorage::checked_peak(&self)?;
        Ok(self)
    }

    pub(crate) const fn max_flows(self) -> usize {
        self.max_flows
    }

    pub(crate) const fn max_terminal_tombstones(self) -> usize {
        self.max_terminal_tombstones
    }

    pub(crate) const fn capacity_witness(self) -> TargetIoCapacityWitness {
        TargetIoCapacityWitness {
            max_flows: self.max_flows,
            max_terminal_tombstones: self.max_terminal_tombstones,
            max_buffered_bytes: self.global_buffer_bytes,
            max_write_directives: self.max_write_directives,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TargetIoConfigError {
    ZeroCapacity { field: &'static str },
    DirectiveCapacityOverflow,
    ReadinessLayoutOverflow,
    StorageLayoutOverflow,
}

impl fmt::Display for TargetIoConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity { field } => write!(formatter, "{field} must be non-zero"),
            Self::DirectiveCapacityOverflow => {
                formatter.write_str("Target write-directive capacity overflow")
            }
            Self::StorageLayoutOverflow => {
                formatter.write_str("Target node storage layout overflow")
            }
            Self::ReadinessLayoutOverflow => {
                formatter.write_str("Target readiness layout overflow")
            }
        }
    }
}

impl std::error::Error for TargetIoConfigError {}

/// Immutable storage/count envelope declared by one concrete Target adapter.
/// A session root retains this value and compares it with the installed
/// reducer/controller plan before accepting physical ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TargetIoCapacityWitness {
    max_flows: usize,
    max_terminal_tombstones: usize,
    max_buffered_bytes: usize,
    max_write_directives: usize,
}

impl TargetIoCapacityWitness {
    pub(crate) const fn max_flows(self) -> usize {
        self.max_flows
    }

    pub(crate) const fn max_terminal_tombstones(self) -> usize {
        self.max_terminal_tombstones
    }

    pub(crate) const fn max_buffered_bytes(self) -> usize {
        self.max_buffered_bytes
    }

    pub(crate) const fn max_write_directives(self) -> usize {
        self.max_write_directives
    }
}

/// Independently scanned live Target ownership. `summed_*` values come from
/// the concrete per-flow containers; `stored_*` values are adapter counters
/// used only as mirrors and are never added to canonical live bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TargetIoColdInventory {
    live_flows: usize,
    terminal_tombstones: usize,
    summed_buffered_bytes: usize,
    stored_buffered_bytes: usize,
    summed_write_directives: usize,
    stored_write_directives: usize,
}

impl TargetIoColdInventory {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn checked(
        capacity: TargetIoCapacityWitness,
        live_flows: usize,
        terminal_tombstones: usize,
        summed_buffered_bytes: usize,
        stored_buffered_bytes: usize,
        summed_write_directives: usize,
        stored_write_directives: usize,
    ) -> Result<Self, TargetIoColdInventoryError> {
        for (domain, actual, max) in [
            ("live_flows", live_flows, capacity.max_flows),
            (
                "terminal_tombstones",
                terminal_tombstones,
                capacity.max_terminal_tombstones,
            ),
            (
                "buffered_bytes",
                stored_buffered_bytes,
                capacity.max_buffered_bytes,
            ),
            (
                "write_directives",
                stored_write_directives,
                capacity.max_write_directives,
            ),
        ] {
            if actual > max {
                return Err(TargetIoColdInventoryError::CapacityExceeded {
                    domain,
                    actual,
                    max,
                });
            }
        }
        if summed_buffered_bytes != stored_buffered_bytes {
            return Err(TargetIoColdInventoryError::StoredMismatch {
                domain: "buffered_bytes",
                summed: summed_buffered_bytes,
                stored: stored_buffered_bytes,
            });
        }
        if summed_write_directives != stored_write_directives {
            return Err(TargetIoColdInventoryError::StoredMismatch {
                domain: "write_directives",
                summed: summed_write_directives,
                stored: stored_write_directives,
            });
        }
        Ok(Self {
            live_flows,
            terminal_tombstones,
            summed_buffered_bytes,
            stored_buffered_bytes,
            summed_write_directives,
            stored_write_directives,
        })
    }

    pub(crate) const fn live_flows(self) -> usize {
        self.live_flows
    }

    pub(crate) const fn terminal_tombstones(self) -> usize {
        self.terminal_tombstones
    }

    pub(crate) const fn summed_buffered_bytes(self) -> usize {
        self.summed_buffered_bytes
    }

    pub(crate) const fn stored_buffered_bytes(self) -> usize {
        self.stored_buffered_bytes
    }

    pub(crate) const fn summed_write_directives(self) -> usize {
        self.summed_write_directives
    }

    pub(crate) const fn stored_write_directives(self) -> usize {
        self.stored_write_directives
    }

    pub(crate) const fn is_quiescent(self) -> bool {
        self.live_flows == 0
            && self.terminal_tombstones == 0
            && self.summed_buffered_bytes == 0
            && self.stored_buffered_bytes == 0
            && self.summed_write_directives == 0
            && self.stored_write_directives == 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetIoColdInventoryError {
    CounterOverflow {
        domain: &'static str,
    },
    CapacityExceeded {
        domain: &'static str,
        actual: usize,
        max: usize,
    },
    StoredMismatch {
        domain: &'static str,
        summed: usize,
        stored: usize,
    },
}

impl fmt::Display for TargetIoColdInventoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CounterOverflow { domain } => {
                write!(
                    formatter,
                    "Target cold-inventory {domain} counter overflowed"
                )
            }
            Self::CapacityExceeded {
                domain,
                actual,
                max,
            } => write!(
                formatter,
                "Target cold-inventory {domain} exceeds capacity ({actual} > {max})",
            ),
            Self::StoredMismatch {
                domain,
                summed,
                stored,
            } => write!(
                formatter,
                "Target cold-inventory {domain} mismatch (summed {summed}, stored {stored})",
            ),
        }
    }
}

impl std::error::Error for TargetIoColdInventoryError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetOpenCompletion {
    Opened,
    AlreadyOpen,
    AlreadyTerminal(TargetTerminalKind),
    Failed(TargetOpenFailure),
}

/// Expected socket-open failures. These are flow-local protocol outcomes, not
/// adapter invariant failures and therefore must not terminate the owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetOpenFailure {
    Refused,
    Unreachable,
    TimedOut,
    ResourceExhausted,
    PolicyDenied,
    Internal,
}

/// Expected failures after a Target socket was opened. All variants collapse
/// to the wire-level `TargetFailure` reset while retaining a closed adapter
/// boundary for future metrics and platform-specific implementations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetFailure {
    ConnectionLost,
    TimedOut,
    ResourceExhausted,
    Internal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetWriteCompletion {
    Accepted(NonZeroUsize),
    Zero,
    WouldBlock,
    Failed(TargetFailure),
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) enum TargetReadCompletion {
    Data(Bytes),
    Eof,
    WouldBlock,
    Failed(TargetFailure),
}

impl fmt::Debug for TargetReadCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data(bytes) => formatter
                .debug_struct("Data")
                .field("bytes", &bytes.len())
                .finish(),
            Self::Eof => formatter.write_str("Eof"),
            Self::WouldBlock => formatter.write_str("WouldBlock"),
            Self::Failed(failure) => formatter.debug_tuple("Failed").field(failure).finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetReadFeedCompletion {
    Queued { bytes: usize },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetReadEofCompletion {
    Queued,
    AlreadyQueued,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetHalfCloseCompletion {
    Closed,
    AlreadyClosed,
    Failed(TargetFailure),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetResetCompletion {
    Reset {
        released_read_bytes: usize,
        released_written_bytes: usize,
    },
    AlreadyReset,
    AlreadyTerminal(TargetTerminalKind),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetCancelCompletion {
    Cancelled {
        released_read_bytes: usize,
        released_written_bytes: usize,
    },
    AlreadyCancelled,
    AlreadyTerminal(TargetTerminalKind),
}

/// Session-expiry-only completion. Unlike ordinary idempotent cancellation,
/// expiry must release adapter-owned buffers even when the socket had already
/// reached a graceful terminal state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TargetExpireCompletion {
    pub(crate) terminal: TargetTerminalKind,
    pub(crate) released_read_bytes: usize,
    pub(crate) released_written_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetJoinCompletion {
    Joined(TargetTerminalKind),
    AlreadyJoined(TargetTerminalKind),
    PendingOwnedState { bytes: usize, directives: usize },
    PendingTerminalCapacity { max: usize },
}

/// Exact result of retiring one reducer-authorized terminal tombstone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetTombstoneRetireCompletion {
    Retired(TargetTerminalKind),
    Absent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TargetReadiness {
    pub(crate) readable: bool,
    pub(crate) writable: bool,
    pub(crate) joinable: bool,
    pub(crate) terminal: Option<TargetTerminalKind>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetReadyInterest {
    Read,
    Write,
    Join,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetReadyOutcome {
    Attempt,
    Failed(TargetFailure),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TargetTerminalKind {
    Graceful,
    Reset,
    Cancelled,
}

impl TargetWriteCompletion {
    pub(crate) fn accepted(bytes: usize) -> Result<Self, TargetIoError> {
        NonZeroUsize::new(bytes)
            .map(Self::Accepted)
            .ok_or(TargetIoError::ZeroAcceptedWrite)
    }

    pub(crate) fn accepted_bytes(self) -> usize {
        match self {
            Self::Accepted(bytes) => bytes.get(),
            Self::Zero | Self::WouldBlock | Self::Failed(_) => 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemoryWriteDirective {
    WouldBlock,
    Zero,
    AcceptAtMost(NonZeroUsize),
}

impl MemoryWriteDirective {
    pub(crate) fn accept_at_most(bytes: usize) -> Result<Self, TargetIoConfigError> {
        NonZeroUsize::new(bytes)
            .map(Self::AcceptAtMost)
            .ok_or(TargetIoConfigError::ZeroCapacity {
                field: "write_directive_bytes",
            })
    }
}

/// Framework-independent Target socket port used by the session supervisor.
///
/// Operational socket outcomes and bounded backpressure are represented by
/// typed completion variants. An [`Err`](TargetIoError) therefore reports an
/// adapter invariant violation and must not consume bytes, change terminal or
/// tombstone ownership, or otherwise commit the requested operation. An
/// executor can retain the exact in-flight capability and fail closed without
/// guessing whether retrying the I/O would duplicate a side effect.
pub(crate) trait TargetIo {
    fn capacity_witness(&self) -> TargetIoCapacityWitness;

    fn cold_inventory(&self) -> Result<TargetIoColdInventory, TargetIoColdInventoryError>;

    fn open(
        &mut self,
        flow_id: SessionFlowId,
        target: &TargetAddr,
    ) -> Result<TargetOpenCompletion, TargetIoError>;

    fn write(
        &mut self,
        flow_id: SessionFlowId,
        bytes: &[u8],
    ) -> Result<TargetWriteCompletion, TargetIoError>;

    fn read(
        &mut self,
        flow_id: SessionFlowId,
        max_bytes: usize,
    ) -> Result<TargetReadCompletion, TargetIoError>;

    fn half_close_write(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetHalfCloseCompletion, TargetIoError>;

    fn reset(&mut self, flow_id: SessionFlowId) -> Result<TargetResetCompletion, TargetIoError>;

    fn cancel(&mut self, flow_id: SessionFlowId) -> Result<TargetCancelCompletion, TargetIoError>;

    fn expire_session_flow(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetExpireCompletion, TargetIoError>;

    fn readiness(&self, flow_id: SessionFlowId) -> Result<TargetReadiness, TargetIoError>;

    fn poll_readiness(
        &mut self,
        flow_id: SessionFlowId,
        interest: TargetReadyInterest,
        cx: &mut Context<'_>,
    ) -> Poll<Result<TargetReadyOutcome, TargetIoError>>;

    fn poll_ready_notifications(&mut self, cx: &mut Context<'_>)
    -> Poll<Result<(), TargetIoError>>;

    /// Releases the live Target handle into a bounded terminal tombstone.
    /// The tombstone preserves duplicate/late OPEN identity until the reducer
    /// authorizes its exact retirement.
    fn join(&mut self, flow_id: SessionFlowId) -> Result<TargetJoinCompletion, TargetIoError>;

    /// Retires only the named terminal tombstone. `Absent` is a typed no-op
    /// for a reducer flow whose Target OPEN never acquired a handle; stale ID
    /// rejection must remain intact independently of tombstone storage.
    fn retire_terminal_tombstone(
        &mut self,
        flow_id: SessionFlowId,
    ) -> Result<TargetTombstoneRetireCompletion, TargetIoError>;
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TargetIoError {
    ReadinessInvariant(&'static str),
    StorageLayout,
    StorageReserve,
    OutputPublicationUnknown {
        flow_id: SessionFlowId,
    },
    StaleFlowId {
        flow_id: SessionFlowId,
        highest_seen: SessionFlowId,
    },
    ConflictingTarget {
        flow_id: SessionFlowId,
    },
    UnknownFlow {
        flow_id: SessionFlowId,
    },
    EmptyWrite,
    EmptyReadFeed,
    ZeroDrainLimit,
    ZeroReadLimit,
    ZeroAcceptedWrite,
    WriteDirectiveLimit {
        flow_id: SessionFlowId,
        max: usize,
    },
    CounterOverflow {
        counter: &'static str,
    },
    BufferCapacityExceeded {
        flow_id: SessionFlowId,
        requested: usize,
        available: usize,
    },
    ReadEofAlreadyQueued {
        flow_id: SessionFlowId,
    },
    ReadAfterEof {
        flow_id: SessionFlowId,
    },
    WriteAfterHalfClose {
        flow_id: SessionFlowId,
    },
    FlowTerminal {
        flow_id: SessionFlowId,
        kind: TargetTerminalKind,
    },
    FlowJoined {
        flow_id: SessionFlowId,
        kind: TargetTerminalKind,
    },
    JoinBeforeTerminal {
        flow_id: SessionFlowId,
    },
}

impl fmt::Display for TargetIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageLayout => formatter.write_str("Target storage layout overflow"),
            Self::StorageReserve => formatter.write_str("Target storage allocation refused"),
            Self::OutputPublicationUnknown { flow_id } => {
                write!(
                    formatter,
                    "Target flow {} retains an interrupted output",
                    flow_id.get()
                )
            }
            Self::ReadinessInvariant(cause) => {
                write!(formatter, "Target readiness invariant: {cause}")
            }
            Self::StaleFlowId {
                flow_id,
                highest_seen,
            } => write!(
                formatter,
                "Target flow {} is stale; highest observed flow is {}",
                flow_id.get(),
                highest_seen.get()
            ),
            Self::ConflictingTarget { flow_id } => write!(
                formatter,
                "flow {} is already bound to a different Target",
                flow_id.get()
            ),
            Self::UnknownFlow { flow_id } => {
                write!(formatter, "unknown Target flow {}", flow_id.get())
            }
            Self::EmptyWrite => formatter.write_str("empty Target write is invalid"),
            Self::EmptyReadFeed => formatter.write_str("empty Target read feed is invalid"),
            Self::ZeroDrainLimit => formatter.write_str("Target drain limit must be non-zero"),
            Self::ZeroReadLimit => formatter.write_str("Target read limit must be non-zero"),
            Self::ZeroAcceptedWrite => {
                formatter.write_str("a positive Target write completion cannot be zero")
            }
            Self::WriteDirectiveLimit { flow_id, max } => write!(
                formatter,
                "Target flow {} write-directive limit reached ({max})",
                flow_id.get()
            ),
            Self::CounterOverflow { counter } => {
                write!(formatter, "Target counter overflow: {counter}")
            }
            Self::BufferCapacityExceeded {
                flow_id,
                requested,
                available,
            } => write!(
                formatter,
                "Target flow {} buffer requires {requested} bytes but {available} are available",
                flow_id.get()
            ),
            Self::ReadEofAlreadyQueued { flow_id } => write!(
                formatter,
                "Target flow {} cannot queue bytes after EOF",
                flow_id.get()
            ),
            Self::ReadAfterEof { flow_id } => write!(
                formatter,
                "Target flow {} EOF was already delivered",
                flow_id.get()
            ),
            Self::WriteAfterHalfClose { flow_id } => write!(
                formatter,
                "Target flow {} write side is half-closed",
                flow_id.get()
            ),
            Self::FlowTerminal { flow_id, kind } => write!(
                formatter,
                "Target flow {} is terminal ({kind:?})",
                flow_id.get()
            ),
            Self::FlowJoined { flow_id, kind } => write!(
                formatter,
                "Target flow {} is already joined ({kind:?})",
                flow_id.get()
            ),
            Self::JoinBeforeTerminal { flow_id } => write!(
                formatter,
                "Target flow {} cannot join before terminal completion",
                flow_id.get()
            ),
        }
    }
}

impl std::error::Error for TargetIoError {}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MemoryTargetSnapshot {
    pub(crate) open_attempts: u64,
    pub(crate) opened_flows: u64,
    pub(crate) duplicate_opens: u64,
    pub(crate) conflicting_opens: u64,
    pub(crate) live_flows: usize,
    pub(crate) write_calls: u64,
    pub(crate) write_accepted_bytes: u64,
    pub(crate) write_zero: u64,
    pub(crate) write_would_block: u64,
    pub(crate) buffered_bytes: usize,
    pub(crate) buffer_high_water: usize,
    pub(crate) queued_write_directives: usize,
    pub(crate) directive_high_water: usize,
    pub(crate) read_calls: u64,
    pub(crate) read_delivered_bytes: u64,
    pub(crate) read_would_block: u64,
    pub(crate) read_eof: u64,
    pub(crate) write_half_closes: u64,
    pub(crate) graceful_flows: u64,
    pub(crate) reset_flows: u64,
    pub(crate) cancelled_flows: u64,
    pub(crate) joined_flows: usize,
    pub(crate) highest_flow_id: Option<SessionFlowId>,
}

struct MemoryTargetFlow {
    ready_position: usize,
    target: TargetAddr,
    written: VecDeque<u8>,
    readable: VecDeque<u8>,
    write_directives: VecDeque<MemoryWriteDirective>,
    output: MemoryTargetOutput,
    read_eof_queued: bool,
    read_eof_delivered: bool,
    write_half_closed: bool,
    terminal: Option<TargetTerminalKind>,
}

impl MemoryTargetFlow {
    fn buffered_bytes(&self) -> usize {
        self.written
            .len()
            .saturating_add(self.readable.len())
            .saturating_add(self.output.len())
    }

    fn require_output_idle(&self, flow_id: SessionFlowId) -> Result<(), TargetIoError> {
        match self.output {
            MemoryTargetOutput::Idle => Ok(()),
            MemoryTargetOutput::Publishing { .. } => {
                Err(TargetIoError::OutputPublicationUnknown { flow_id })
            }
        }
    }

    fn released_buffers(&self) -> Result<ReleasedTargetBuffers, TargetIoError> {
        let mut read_bytes = self.readable.len();
        let mut written_bytes = self.written.len();
        if let MemoryTargetOutput::Publishing { direction, bytes } = &self.output {
            let count = match direction {
                MemoryTargetOutputDirection::Read => &mut read_bytes,
                MemoryTargetOutputDirection::Written => &mut written_bytes,
            };
            *count = count
                .checked_add(bytes.len())
                .ok_or(TargetIoError::CounterOverflow {
                    counter: "released_buffered_bytes",
                })?;
        }
        Ok(ReleasedTargetBuffers {
            read_bytes,
            written_bytes,
        })
    }
}

#[derive(Clone, Copy)]
enum MemoryTargetOutputDirection {
    Read,
    Written,
}

// Concrete copied output stays in its original flow across controlled Waker
// calls. It is charged once until infallible return or explicit terminal discard.
enum MemoryTargetOutput {
    Idle,
    Publishing {
        direction: MemoryTargetOutputDirection,
        bytes: Bytes,
    },
}

impl MemoryTargetOutput {
    fn len(&self) -> usize {
        match self {
            Self::Idle => 0,
            Self::Publishing { bytes, .. } => bytes.len(),
        }
    }

    fn finish(&mut self) -> Bytes {
        // The exclusive flow borrow published this exact state immediately
        // before notification; no caller can replace it during the callback.
        match std::mem::replace(self, Self::Idle) {
            Self::Publishing { bytes, .. } => bytes,
            Self::Idle => unreachable!("the original flow owns its prepared output"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReleasedTargetBuffers {
    read_bytes: usize,
    written_bytes: usize,
}

enum TerminalizeOutcome {
    Changed(ReleasedTargetBuffers, memory_ready::ReadyMutation),
    Existing(TargetTerminalKind),
}

struct JoinedTargetFlow {
    target: TargetAddr,
    terminal: TargetTerminalKind,
}

// One actual Layout owns each phase; join never allocates a second node.
type TargetNodeLink = Option<Box<[TargetNode; 1]>>;
enum TargetNodePhase {
    Vacant,
    Live(MemoryTargetFlow),
    Joined(JoinedTargetFlow),
}
struct TargetNode {
    key: SessionFlowId,
    phase: TargetNodePhase,
    left: TargetNodeLink,
    right: TargetNodeLink,
    height: u8,
}
#[derive(Default)]
struct MemoryTargetStorage {
    live: TargetNodeLink,
    joined: TargetNodeLink,
    free: TargetNodeLink,
    live_len: usize,
    joined_len: usize,
    free_len: usize,
}

/// Bounded deterministic Target adapter used by Task 4's virtual scheduler.
pub(crate) struct MemoryTarget {
    config: MemoryTargetConfig,
    ready: MemoryReadiness,
    storage: MemoryTargetStorage,
    owned_address_capacity: usize,
    snapshot: MemoryTargetSnapshot,
    buffered_bytes: usize,
    buffer_high_water: usize,
    queued_write_directives: usize,
    directive_high_water: usize,
    highest_flow_id: Option<SessionFlowId>,
}

fn bump(counter: &mut u64, name: &'static str) -> Result<(), TargetIoError> {
    *counter = counter
        .checked_add(1)
        .ok_or(TargetIoError::CounterOverflow { counter: name })?;
    Ok(())
}

fn checked_add_usize_to_u64(
    counter: u64,
    value: usize,
    name: &'static str,
) -> Result<u64, TargetIoError> {
    let value =
        u64::try_from(value).map_err(|_| TargetIoError::CounterOverflow { counter: name })?;
    counter
        .checked_add(value)
        .ok_or(TargetIoError::CounterOverflow { counter: name })
}

impl fmt::Debug for MemoryTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryTarget")
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    mod address_storage;
    mod node_storage;
    mod owner_readiness;
    mod retained_backing;
    mod storage_peak;

    use super::*;

    fn flow(value: u64) -> SessionFlowId {
        SessionFlowId::new(value).expect("test flow id must be non-zero")
    }

    fn target(port: u16) -> TargetAddr {
        TargetAddr::DomainPort {
            host: "private.test.invalid".to_owned(),
            port,
        }
    }

    fn config() -> MemoryTargetConfig {
        MemoryTargetConfig::new(4, 32, 96, 8, 8, 4)
            .expect("test Target configuration must be bounded")
    }

    #[test]
    fn invariant_errors_never_commit_open_write_read_or_terminal_ownership() {
        let mut io = MemoryTarget::new(config());
        let flow = flow(1);

        io.snapshot.opened_flows = u64::MAX;
        assert_eq!(
            io.open(flow, &target(443)),
            Err(TargetIoError::CounterOverflow {
                counter: "opened_flows"
            })
        );
        assert!(io.storage.get(&flow).is_none());
        assert_eq!(io.highest_flow_id, None);

        io.snapshot.opened_flows = 0;
        io.open(flow, &target(443)).unwrap();
        io.push_write_directive(flow, MemoryWriteDirective::accept_at_most(2).unwrap())
            .unwrap();
        io.snapshot.write_accepted_bytes = u64::MAX;
        assert_eq!(
            io.write(flow, b"abc"),
            Err(TargetIoError::CounterOverflow {
                counter: "write_accepted_bytes"
            })
        );
        let retained = io.storage.get(&flow).unwrap();
        assert!(retained.written.is_empty());
        assert_eq!(retained.write_directives.len(), 1);
        assert_eq!(io.buffered_bytes, 0);
        assert_eq!(io.queued_write_directives, 1);

        io.feed_read(flow, Bytes::from_static(b"xyz")).unwrap();
        io.snapshot.read_delivered_bytes = u64::MAX;
        assert_eq!(
            io.read(flow, 3),
            Err(TargetIoError::CounterOverflow {
                counter: "read_delivered_bytes"
            })
        );
        let retained = io.storage.get(&flow).unwrap();
        assert_eq!(
            retained.readable.iter().copied().collect::<Vec<_>>(),
            b"xyz"
        );
        assert_eq!(io.buffered_bytes, 3);

        io.snapshot.write_half_closes = u64::MAX;
        assert_eq!(
            io.half_close_write(flow),
            Err(TargetIoError::CounterOverflow {
                counter: "write_half_closes"
            })
        );
        assert!(!io.storage.get(&flow).unwrap().write_half_closed);

        io.snapshot.reset_flows = u64::MAX;
        assert_eq!(
            io.reset(flow),
            Err(TargetIoError::CounterOverflow {
                counter: "reset_flows"
            })
        );
        let retained = io.storage.get(&flow).unwrap();
        assert_eq!(retained.terminal, None);
        assert_eq!(retained.readable.len(), 3);
        assert_eq!(retained.write_directives.len(), 1);
        assert_eq!(io.buffered_bytes, 3);
        assert_eq!(io.queued_write_directives, 1);
    }

    #[test]
    fn invariant_error_before_graceful_transition_preserves_eof_and_directives() {
        let mut io = MemoryTarget::new(config());
        let flow = flow(1);
        io.open(flow, &target(443)).unwrap();
        io.finish_read(flow).unwrap();

        io.snapshot.read_eof = u64::MAX;
        assert_eq!(
            io.read(flow, 1),
            Err(TargetIoError::CounterOverflow {
                counter: "read_eof"
            })
        );
        assert!(!io.storage.get(&flow).unwrap().read_eof_delivered);

        io.snapshot.read_eof = 0;
        assert_eq!(io.read(flow, 1), Ok(TargetReadCompletion::Eof));
        io.push_write_directive(flow, MemoryWriteDirective::Zero)
            .unwrap();
        io.snapshot.graceful_flows = u64::MAX;
        assert_eq!(
            io.half_close_write(flow),
            Err(TargetIoError::CounterOverflow {
                counter: "graceful_flows"
            })
        );
        let retained = io.storage.get(&flow).unwrap();
        assert!(!retained.write_half_closed);
        assert_eq!(retained.terminal, None);
        assert_eq!(retained.write_directives.len(), 1);
        assert_eq!(io.queued_write_directives, 1);
    }

    #[test]
    fn duplicate_open_is_idempotent_and_conflicting_target_is_rejected() {
        let mut io = MemoryTarget::new(config());
        let flow = flow(1);
        let first_target = target(443);

        assert_eq!(
            io.open(flow, &first_target),
            Ok(TargetOpenCompletion::Opened)
        );
        assert_eq!(
            io.open(flow, &first_target),
            Ok(TargetOpenCompletion::AlreadyOpen)
        );
        assert_eq!(
            io.open(flow, &target(8443)),
            Err(TargetIoError::ConflictingTarget { flow_id: flow })
        );

        let snapshot = io.snapshot();
        assert_eq!(snapshot.open_attempts, 3);
        assert_eq!(snapshot.opened_flows, 1);
        assert_eq!(snapshot.duplicate_opens, 1);
        assert_eq!(snapshot.conflicting_opens, 1);
        assert_eq!(snapshot.live_flows, 1);

        let debug = format!("{io:?}");
        assert!(!debug.contains("private.test.invalid"));
        assert!(!debug.contains("443"));
    }

    #[test]
    fn writes_are_partial_zero_would_block_and_capacity_bounded() {
        let config = MemoryTargetConfig::new(2, 8, 8, 8, 8, 4).unwrap();
        let mut io = MemoryTarget::new(config);
        let flow = flow(1);
        io.open(flow, &target(443)).unwrap();
        io.push_write_directive(flow, MemoryWriteDirective::WouldBlock)
            .unwrap();
        io.push_write_directive(flow, MemoryWriteDirective::Zero)
            .unwrap();
        io.push_write_directive(flow, MemoryWriteDirective::accept_at_most(3).unwrap())
            .unwrap();

        assert_eq!(
            io.write(flow, b"abcdefgh"),
            Ok(TargetWriteCompletion::WouldBlock)
        );
        assert_eq!(io.write(flow, b"abcdefgh"), Ok(TargetWriteCompletion::Zero));
        assert_eq!(
            io.write(flow, b"abcdefgh"),
            Ok(TargetWriteCompletion::accepted(3).unwrap())
        );
        assert_eq!(
            io.write(flow, b"defgh"),
            Ok(TargetWriteCompletion::accepted(5).unwrap())
        );
        assert_eq!(io.write(flow, b"i"), Ok(TargetWriteCompletion::WouldBlock));

        let snapshot = io.snapshot();
        assert_eq!(snapshot.write_calls, 5);
        assert_eq!(snapshot.write_accepted_bytes, 8);
        assert_eq!(snapshot.write_zero, 1);
        assert_eq!(snapshot.write_would_block, 2);
        assert_eq!(snapshot.buffered_bytes, 8);
        assert_eq!(snapshot.buffer_high_water, 8);

        assert_eq!(&io.take_written(flow, 3).unwrap()[..], b"abc");
        assert_eq!(
            io.write(flow, b"ijk"),
            Ok(TargetWriteCompletion::accepted(3).unwrap())
        );
        assert_eq!(&io.take_written(flow, 8).unwrap()[..], b"defghijk");
        assert_eq!(io.snapshot().buffered_bytes, 0);
    }

    #[test]
    fn reads_drain_before_eof_and_graceful_half_close_is_ordered_once() {
        let mut io = MemoryTarget::new(config());
        let flow = flow(1);
        io.open(flow, &target(443)).unwrap();
        assert_eq!(
            io.feed_read(flow, Bytes::from_static(b"abcdefghij")),
            Ok(TargetReadFeedCompletion::Queued { bytes: 10 })
        );
        assert_eq!(io.finish_read(flow), Ok(TargetReadEofCompletion::Queued));
        assert_eq!(
            io.half_close_write(flow),
            Ok(TargetHalfCloseCompletion::Closed)
        );
        assert_eq!(
            io.half_close_write(flow),
            Ok(TargetHalfCloseCompletion::AlreadyClosed)
        );
        assert_eq!(
            io.write(flow, b"late"),
            Err(TargetIoError::WriteAfterHalfClose { flow_id: flow })
        );

        assert_eq!(
            io.read(flow, 6),
            Ok(TargetReadCompletion::Data(Bytes::from_static(b"abcdef")))
        );
        assert_eq!(
            io.read(flow, 10),
            Ok(TargetReadCompletion::Data(Bytes::from_static(b"ghij")))
        );
        assert_eq!(io.read(flow, 1), Ok(TargetReadCompletion::Eof));
        assert_eq!(
            io.read(flow, 1),
            Err(TargetIoError::ReadAfterEof { flow_id: flow })
        );

        let snapshot = io.snapshot();
        assert_eq!(snapshot.read_delivered_bytes, 10);
        assert_eq!(snapshot.read_eof, 1);
        assert_eq!(snapshot.write_half_closes, 1);
        assert_eq!(snapshot.graceful_flows, 1);
        assert_eq!(snapshot.buffered_bytes, 0);
    }

    #[test]
    fn reset_and_cancel_are_flow_local_and_release_exact_buffers() {
        let mut io = MemoryTarget::new(config());
        let reset_flow = flow(1);
        let live_flow = flow(2);
        io.open(reset_flow, &target(443)).unwrap();
        io.open(live_flow, &target(8443)).unwrap();
        io.feed_read(reset_flow, Bytes::from_static(b"abc"))
            .unwrap();
        io.feed_read(live_flow, Bytes::from_static(b"xyz")).unwrap();
        assert_eq!(io.write(reset_flow, b"12").unwrap().accepted_bytes(), 2);
        assert_eq!(io.write(live_flow, b"34").unwrap().accepted_bytes(), 2);

        assert_eq!(
            io.reset(reset_flow),
            Ok(TargetResetCompletion::Reset {
                released_read_bytes: 3,
                released_written_bytes: 2,
            })
        );
        assert_eq!(
            io.reset(reset_flow),
            Ok(TargetResetCompletion::AlreadyReset)
        );
        assert_eq!(
            io.read(reset_flow, 1),
            Err(TargetIoError::FlowTerminal {
                flow_id: reset_flow,
                kind: TargetTerminalKind::Reset,
            })
        );

        assert_eq!(
            io.read(live_flow, 8),
            Ok(TargetReadCompletion::Data(Bytes::from_static(b"xyz")))
        );
        assert_eq!(&io.take_written(live_flow, 8).unwrap()[..], b"34");
        assert_eq!(
            io.cancel(live_flow),
            Ok(TargetCancelCompletion::Cancelled {
                released_read_bytes: 0,
                released_written_bytes: 0,
            })
        );

        let snapshot = io.snapshot();
        assert_eq!(snapshot.reset_flows, 1);
        assert_eq!(snapshot.cancelled_flows, 1);
        assert_eq!(snapshot.buffered_bytes, 0);
        assert_eq!(snapshot.live_flows, 2);
    }

    #[test]
    fn deterministic_readiness_and_join_release_all_live_ownership() {
        let mut io = MemoryTarget::new(config());
        let flow = flow(1);
        let target = target(443);
        io.open(flow, &target).unwrap();
        assert_eq!(
            io.readiness(flow),
            Ok(TargetReadiness {
                readable: false,
                writable: true,
                joinable: false,
                terminal: None,
            })
        );

        io.push_write_directive(flow, MemoryWriteDirective::WouldBlock)
            .unwrap();
        assert!(!io.readiness(flow).unwrap().writable);
        assert_eq!(io.write(flow, b"ok"), Ok(TargetWriteCompletion::WouldBlock));
        assert!(io.readiness(flow).unwrap().writable);
        assert_eq!(io.write(flow, b"ok").unwrap().accepted_bytes(), 2);
        assert_eq!(&io.take_written(flow, 8).unwrap()[..], b"ok");
        io.push_write_directive(flow, MemoryWriteDirective::Zero)
            .unwrap();

        io.feed_read(flow, Bytes::from_static(b"reply")).unwrap();
        io.finish_read(flow).unwrap();
        assert!(io.readiness(flow).unwrap().readable);
        io.half_close_write(flow).unwrap();
        assert_eq!(
            io.read(flow, 8),
            Ok(TargetReadCompletion::Data(Bytes::from_static(b"reply")))
        );
        assert!(io.readiness(flow).unwrap().readable);
        assert_eq!(io.read(flow, 8), Ok(TargetReadCompletion::Eof));
        assert_eq!(
            io.readiness(flow),
            Ok(TargetReadiness {
                readable: false,
                writable: false,
                joinable: true,
                terminal: Some(TargetTerminalKind::Graceful),
            })
        );

        assert_eq!(
            io.join(flow),
            Ok(TargetJoinCompletion::Joined(TargetTerminalKind::Graceful))
        );
        assert_eq!(
            io.join(flow),
            Ok(TargetJoinCompletion::AlreadyJoined(
                TargetTerminalKind::Graceful
            ))
        );
        assert_eq!(
            io.open(flow, &target),
            Ok(TargetOpenCompletion::AlreadyTerminal(
                TargetTerminalKind::Graceful
            ))
        );

        let snapshot = io.snapshot();
        assert_eq!(snapshot.live_flows, 0);
        assert_eq!(snapshot.joined_flows, 1);
        assert_eq!(snapshot.buffered_bytes, 0);
        assert_eq!(snapshot.queued_write_directives, 0);
    }

    #[test]
    fn live_flow_capacity_is_reusable_while_terminal_tombstones_stay_bounded() {
        let config = MemoryTargetConfig::new(1, 32, 96, 8, 8, 4)
            .unwrap()
            .with_max_terminal_tombstones(1)
            .unwrap();
        assert_eq!(config.max_flows(), 1);
        assert_eq!(config.max_terminal_tombstones(), 1);
        assert!(matches!(
            config.with_max_terminal_tombstones(0),
            Err(TargetIoConfigError::ZeroCapacity {
                field: "max_terminal_tombstones"
            })
        ));
        let mut io = MemoryTarget::new(config);
        let first = flow(1);
        let second = flow(2);

        io.open(first, &target(443)).unwrap();
        io.reset(first).unwrap();
        assert_eq!(
            io.join(first),
            Ok(TargetJoinCompletion::Joined(TargetTerminalKind::Reset))
        );
        assert_eq!(io.snapshot().live_flows, 0);
        assert_eq!(io.snapshot().joined_flows, 1);

        // A joined tombstone is not a live socket handle, so the one live
        // slot remains reusable before terminal grace expires.
        assert_eq!(
            io.open(second, &target(8443)),
            Ok(TargetOpenCompletion::Opened)
        );
        io.reset(second).unwrap();
        assert_eq!(
            io.join(second),
            Ok(TargetJoinCompletion::PendingTerminalCapacity { max: 1 })
        );
        assert_eq!(io.snapshot().live_flows, 1);
        assert_eq!(io.snapshot().joined_flows, 1);

        assert_eq!(
            io.retire_terminal_tombstone(first),
            Ok(TargetTombstoneRetireCompletion::Retired(
                TargetTerminalKind::Reset
            ))
        );
        assert_eq!(
            io.join(second),
            Ok(TargetJoinCompletion::Joined(TargetTerminalKind::Reset))
        );
        assert_eq!(
            io.retire_terminal_tombstone(second),
            Ok(TargetTombstoneRetireCompletion::Retired(
                TargetTerminalKind::Reset
            ))
        );
        assert_eq!(io.snapshot().live_flows, 0);
        assert_eq!(io.snapshot().joined_flows, 0);

        // Retiring the exact tombstone releases memory, not the monotonic ID
        // fence: stale delivery can never resurrect a Target socket.
        assert_eq!(
            io.open(first, &target(443)),
            Err(TargetIoError::StaleFlowId {
                flow_id: first,
                highest_seen: second,
            })
        );
        assert_eq!(
            io.retire_terminal_tombstone(flow(99)),
            Ok(TargetTombstoneRetireCompletion::Absent)
        );
        assert_eq!(io.snapshot().live_flows, 0);
        assert_eq!(io.snapshot().joined_flows, 0);
    }

    #[test]
    fn capacity_rejected_open_still_consumes_its_monotonic_flow_id() {
        let config = MemoryTargetConfig::new(1, 32, 96, 8, 8, 4)
            .unwrap()
            .with_max_terminal_tombstones(1)
            .unwrap();
        let mut io = MemoryTarget::new(config);
        let first = flow(1);
        let rejected = flow(2);
        io.open(first, &target(443)).unwrap();
        assert_eq!(
            io.open(rejected, &target(8443)),
            Ok(TargetOpenCompletion::Failed(
                TargetOpenFailure::ResourceExhausted
            ))
        );

        io.reset(first).unwrap();
        io.join(first).unwrap();
        io.retire_terminal_tombstone(first).unwrap();
        assert_eq!(
            io.open(rejected, &target(8443)),
            Err(TargetIoError::StaleFlowId {
                flow_id: rejected,
                highest_seen: rejected,
            })
        );
        assert_eq!(io.snapshot().live_flows, 0);
        assert_eq!(io.snapshot().joined_flows, 0);
        assert_eq!(io.snapshot().highest_flow_id, Some(rejected));
    }
}

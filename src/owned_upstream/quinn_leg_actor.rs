//! Fixed Quinn transport actor for one authenticated resumable leg.
//!
//! This module is the production-shared boundary that owns endpoint
//! construction, TLS binding derivation, bounded encoded I/O, cooperative
//! cancellation, and exact structured join for one actor incarnation.

mod bootstrap;
mod output;
mod readiness;

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Condvar;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use hmac::{Hmac, Mac};
use quinn::crypto::rustls::{HandshakeData, QuicClientConfig, QuicServerConfig};
use quinn::{Connection, Endpoint, RecvStream, SendStream, TransportConfig, VarInt};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use sha2::Sha256;
use thiserror::Error;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinSet;

use crate::owned_upstream::leg::{
    BoundInbound, ExactLegTerminal, LegTransportReporter, LegTransportTerminalReason,
};
use crate::owned_upstream::leg_io::{
    EncodedLegSubmission, EncodedLegTransport, EncodedLegTransportError, LegControlEnqueued,
    LegControlQueueError, LegEndpointRole, LegIo, LegIoError, LegIoLimits, LegOutboundQueue,
    LegOutboundShutdownWaitRetirement, OrderedDeliveryToken, PhysicalLegRuntimeIncarnation,
};
use crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate;
use crate::owned_upstream::session_clock::{SessionClockError, SessionMonotonicClock};
use crate::owned_upstream::two_leg::{
    EncodedDelivery, LegId, SimTime, WireDirection, WireLane, WireRoute,
};
use crate::resumable::{
    AttachAlpn, AttachTransportBinding, DevicePrincipal, FeatureSet, LegControlFrame,
    MAX_ENCODED_DATA_FRAME_BYTES, OwnerIdentity, TlsExporterBinding,
};

const ACTOR_ALPN: &[u8] = b"mvpn-resumable-leg-v1";
const EXPORTER_LABEL: &[u8] = b"EXPORTER-mini-vpn-resumable-leg-v1";
const EXPORTER_CONTEXT_PREFIX: &[u8] = b"mini-vpn/r6/quinn-leg/attach-binding/v1\0";
const CONFIRM_PREFIX: &[u8] = b"mini-vpn/r6/quinn-leg/binding-confirm/v1\0";
const STREAM_MAGIC: &[u8; 4] = b"MVL1";
const FIXED_TASK_GUARDS_PER_ACTOR: usize = 5;
const PEAK_TERMINATION_GUARDS_PER_ACTOR: usize = FIXED_TASK_GUARDS_PER_ACTOR + 1;
const STREAM_PREFACE_BYTES: usize = 8;
const RECORD_HEADER_BYTES: usize = 18;
const RECORD_VERSION: u8 = 1;
const RECORD_DATA: u8 = 1;
const RECORD_DELIVERED: u8 = 2;
const RECORD_ORDERED: u8 = 1;
const STREAM_DATA: u8 = 1;
const STREAM_DELIVERED: u8 = 2;
const TERMINAL_STAMP_HANDLES_PER_ACTOR: usize = 2;
const CLOSE_CODE: VarInt = VarInt::from_u32(0);
const ROOT_EXITED_WITHOUT_OUTCOME: &str =
    "fixed actor root exited without publishing a completed structural outcome";
#[cfg(test)]
const TEST_ROOT_OUTER_ERROR: &str = "fixed actor root outer error failpoint";
type HmacSha256 = Hmac<Sha256>;

/// Unforgeable production construction authority whose sole mint remains in
/// this fixed Quinn factory module.
pub(super) struct QuinnLegActorTransportGate(());

impl QuinnLegActorTransportGate {
    const fn mint_for_fixed_factory() -> Self {
        Self(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AttachPeerPolicy {
    owner_identity: OwnerIdentity,
    device_principal: DevicePrincipal,
}

impl AttachPeerPolicy {
    pub(crate) const fn new(
        owner_identity: OwnerIdentity,
        device_principal: DevicePrincipal,
    ) -> Self {
        Self {
            owner_identity,
            device_principal,
        }
    }
}

#[derive(Clone)]
pub(crate) struct QuinnLegActorClientConfig {
    bind_addr: SocketAddr,
    server_addr: SocketAddr,
    server_name: String,
    client_crypto: Arc<dyn quinn::crypto::ClientConfig>,
    bootstrap_timeout: Duration,
}

impl std::fmt::Debug for QuinnLegActorClientConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuinnLegActorClientConfig")
            .field("bind_addr", &self.bind_addr)
            .field("server_addr", &self.server_addr)
            .field("server_name", &self.server_name)
            .field("client_crypto", &"[PRELOADED]")
            .field("bootstrap_timeout", &self.bootstrap_timeout)
            .finish()
    }
}

impl QuinnLegActorClientConfig {
    pub(crate) fn new(
        bind_addr: SocketAddr,
        server_addr: SocketAddr,
        server_name: impl Into<String>,
        ca_path: PathBuf,
        bootstrap_timeout: Duration,
    ) -> Result<Self, QuinnLegActorError> {
        let server_name = server_name.into();
        if server_name.is_empty() {
            return Err(QuinnLegActorError::InvalidConfig);
        }
        validate_bootstrap_timeout(bootstrap_timeout)?;
        Ok(Self {
            bind_addr,
            server_addr,
            server_name,
            client_crypto: build_client_crypto(&ca_path)?,
            bootstrap_timeout,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct QuinnLegActorServerConfig {
    bind_addr: SocketAddr,
    cert_path: PathBuf,
    key_path: PathBuf,
    max_live_actors: usize,
    bootstrap_timeout: Duration,
}

impl QuinnLegActorServerConfig {
    pub(crate) fn new(
        bind_addr: SocketAddr,
        cert_path: PathBuf,
        key_path: PathBuf,
        max_live_actors: usize,
        bootstrap_timeout: Duration,
    ) -> Result<Self, QuinnLegActorError> {
        if max_live_actors == 0
            || max_live_actors
                .checked_mul(PEAK_TERMINATION_GUARDS_PER_ACTOR)
                .is_none()
        {
            return Err(QuinnLegActorError::InvalidConfig);
        }
        validate_bootstrap_timeout(bootstrap_timeout)?;
        Ok(Self {
            bind_addr,
            cert_path,
            key_path,
            max_live_actors,
            bootstrap_timeout,
        })
    }
}

fn validate_bootstrap_timeout(bootstrap_timeout: Duration) -> Result<(), QuinnLegActorError> {
    if bootstrap_timeout.is_zero()
        || std::time::Instant::now()
            .checked_add(bootstrap_timeout)
            .is_none()
    {
        return Err(QuinnLegActorError::InvalidConfig);
    }
    Ok(())
}

fn ensure_bootstrap_deadline(deadline: tokio::time::Instant) -> Result<(), QuinnLegActorError> {
    if tokio::time::Instant::now() >= deadline {
        return Err(QuinnLegActorError::BootstrapTimedOut);
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnLegActorQueueLimits {
    max_messages: usize,
    max_bytes: usize,
}

impl QuinnLegActorQueueLimits {
    pub(crate) fn new(max_messages: usize, max_bytes: usize) -> Result<Self, QuinnLegActorError> {
        if max_messages == 0 || max_bytes == 0 {
            return Err(QuinnLegActorError::InvalidConfig);
        }
        Ok(Self {
            max_messages,
            max_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnLegActorLiveLimits {
    outbound_messages: usize,
    outbound_bytes: usize,
    inbound_messages: usize,
    inbound_bytes: usize,
    composition_messages: usize,
    ack_messages: usize,
    ack_bytes: usize,
}

impl QuinnLegActorLiveLimits {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        outbound_messages: usize,
        outbound_bytes: usize,
        inbound_messages: usize,
        inbound_bytes: usize,
        composition_messages: usize,
        ack_messages: usize,
        ack_bytes: usize,
    ) -> Result<Self, QuinnLegActorError> {
        let primitive_capacities = [
            outbound_messages,
            outbound_bytes,
            inbound_messages,
            inbound_bytes,
            composition_messages,
            ack_messages,
            ack_bytes,
        ];
        if primitive_capacities.contains(&0)
            || ack_messages < inbound_messages
            || primitive_capacities
                .iter()
                .any(|capacity| *capacity > Semaphore::MAX_PERMITS)
        {
            return Err(QuinnLegActorError::InvalidConfig);
        }
        Ok(Self {
            outbound_messages,
            outbound_bytes,
            inbound_messages,
            inbound_bytes,
            composition_messages,
            ack_messages,
            ack_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnLegActorLimits {
    capacity: QuinnLegActorCapacityWitness,
}

/// Immutable construction-time capacity of one exact physical actor.
///
/// The envelope values bound actor-owned storage domains; they are not a live
/// byte inventory and must not be added to runtime payload counters. Retaining
/// the primitive limits makes equality an exact configuration comparison,
/// rather than allowing different queue layouts with the same total to match.
/// The two terminal-handle metadata allocations are already included in
/// `max_owned_bytes`; their count is exposed only for cross-root handle
/// conservation and must never be converted into bytes and added again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuinnLegActorCapacityWitness {
    leg_io: LegIoLimits,
    queue: QuinnLegActorQueueLimits,
    live: QuinnLegActorLiveLimits,
    max_owned_messages: usize,
    max_owned_bytes: usize,
    terminal_stamp_handles: usize,
}

impl QuinnLegActorCapacityWitness {
    pub(crate) const fn max_owned_messages(self) -> usize {
        self.max_owned_messages
    }

    pub(crate) const fn max_owned_bytes(self) -> usize {
        self.max_owned_bytes
    }

    pub(crate) const fn terminal_stamp_handles(self) -> usize {
        self.terminal_stamp_handles
    }
}

fn checked_capacity_sum<const N: usize>(capacities: [usize; N]) -> Option<usize> {
    capacities
        .into_iter()
        .try_fold(0usize, |total, capacity| total.checked_add(capacity))
}

fn checked_actor_timestamp_bytes(inbound_messages: usize) -> Option<usize> {
    // The channel owns at most `inbound_messages` arrival values. Four fixed
    // words cover the controller pending slot, its synchronous callback
    // envelope, the producer reservation, and the atomic in-flight witness.
    // The terminal instant itself has one canonical Option slot inside
    // ActorFirstOutcome; two fixed Arc handles cover root/handoff and
    // composition/join overlap without copying that instant.
    let inbound_bytes = inbound_messages
        .checked_add(4)?
        .checked_mul(std::mem::size_of::<SimTime>())?;
    let terminal_slot_bytes = std::mem::size_of::<Option<SimTime>>();
    let terminal_handle_bytes = TERMINAL_STAMP_HANDLES_PER_ACTOR
        .checked_mul(std::mem::size_of::<ActorTerminalStampWitness>())?;
    checked_capacity_sum([inbound_bytes, terminal_slot_bytes, terminal_handle_bytes])
}

impl QuinnLegActorLimits {
    fn bootstrap_default() -> Result<Self, QuinnLegActorError> {
        let frame_bytes = MAX_ENCODED_DATA_FRAME_BYTES;
        let frame_bytes_u64 =
            u64::try_from(frame_bytes).map_err(|_| QuinnLegActorError::InvalidConfig)?;
        let leg_io = LegIoLimits::new(frame_bytes, 1, frame_bytes_u64, 1, frame_bytes_u64)
            .map_err(|_| QuinnLegActorError::InvalidConfig)?;
        let queue = QuinnLegActorQueueLimits::new(1, frame_bytes)?;
        let live = QuinnLegActorLiveLimits::new(1, frame_bytes, 1, frame_bytes, 1, 1, 18)?;
        Self::new(leg_io, queue, live)
    }

    pub(crate) fn new(
        leg_io: LegIoLimits,
        queue: QuinnLegActorQueueLimits,
        live: QuinnLegActorLiveLimits,
    ) -> Result<Self, QuinnLegActorError> {
        let frame_bytes = usize::try_from(leg_io.max_frame_bytes())
            .map_err(|_| QuinnLegActorError::InvalidConfig)?;
        let minimum_ack_bytes = live
            .ack_messages
            .checked_mul(RECORD_HEADER_BYTES)
            .ok_or(QuinnLegActorError::InvalidConfig)?;
        let timestamp_bytes = checked_actor_timestamp_bytes(live.inbound_messages)
            .ok_or(QuinnLegActorError::InvalidConfig)?;
        let message_envelope = checked_capacity_sum([
            queue.max_messages,
            live.outbound_messages,
            live.inbound_messages,
            live.composition_messages,
            live.ack_messages,
            2,
        ])
        .ok_or(QuinnLegActorError::InvalidConfig)?;
        let byte_envelope = checked_capacity_sum([
            queue.max_bytes,
            live.outbound_bytes,
            live.inbound_bytes,
            live.ack_bytes,
            RECORD_HEADER_BYTES,
            frame_bytes,
            RECORD_HEADER_BYTES,
            timestamp_bytes,
        ])
        .ok_or(QuinnLegActorError::InvalidConfig)?;
        if frame_bytes > queue.max_bytes
            || frame_bytes > live.outbound_bytes
            || frame_bytes > live.inbound_bytes
            || live.ack_bytes < minimum_ack_bytes
            || frame_bytes > u32::MAX as usize
        {
            return Err(QuinnLegActorError::InvalidConfig);
        }
        Ok(Self {
            capacity: QuinnLegActorCapacityWitness {
                leg_io,
                queue,
                live,
                max_owned_messages: message_envelope,
                max_owned_bytes: byte_envelope,
                terminal_stamp_handles: TERMINAL_STAMP_HANDLES_PER_ACTOR,
            },
        })
    }

    pub(crate) const fn capacity_witness(self) -> QuinnLegActorCapacityWitness {
        self.capacity
    }

    const fn leg_io(self) -> LegIoLimits {
        self.capacity.leg_io
    }

    const fn queue(self) -> QuinnLegActorQueueLimits {
        self.capacity.queue
    }

    const fn live(self) -> QuinnLegActorLiveLimits {
        self.capacity.live
    }

    fn max_frame_bytes(self) -> usize {
        usize::try_from(self.leg_io().max_frame_bytes())
            .expect("checked Quinn actor frame bound fits usize")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuinnLegActorFlushDisposition {
    Idle,
    Submitted,
    OrderedDeliveryPending,
}

/// Composition-gated shutdown observation of the sole actor FIFO. Only the
/// `RetiredDelivered` variant changes ownership, and it can retire no more
/// than one already-delivered zero-byte wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QuinnLegActorShutdownWaitDrive {
    Clean,
    RetiredDelivered,
    Pending,
    Residual {
        queued_frames: usize,
        queued_bytes: usize,
        ordered_delivery_wait: bool,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct QuinnLegActorSnapshot {
    outbound_queue_messages: usize,
    outbound_queue_bytes: usize,
    validated_data_headers: usize,
    queued_inbound_messages: usize,
    inbound_owned_bytes: usize,
    waiting_inbound_bytes: usize,
    reserved_delivery_acks: usize,
    ack_owned_bytes: usize,
    composition_owned_messages: usize,
    outbound_owned_messages: usize,
    outbound_owned_bytes: usize,
    #[cfg(test)]
    blocked_write: Option<TestPendingWrite>,
    #[cfg(test)]
    pending_delivery_count: usize,
    #[cfg(test)]
    pending_delivery_ordinal: Option<u64>,
}

/// Test-only copy of the cold FIFO audit fields used to localize structural
/// join failures. This observation cannot clear, transfer, or settle queue
/// ownership and is not a production shutdown witness.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct QuinnLegActorOutboundQueueColdObservation {
    phase: &'static str,
    queued_frames: usize,
    summed_frame_bytes: usize,
    stored_owned_bytes: usize,
    ordered_delivery_wait: bool,
}

/// Test-only, read-only localization of the three actor-owned ACK stages.
/// Capturing it cannot drive the FIFO, consume a delivery record, or settle
/// any live ownership.
#[cfg(test)]
#[derive(Debug)]
pub(super) struct QuinnLegActorDiagnosticObservation {
    snapshot: QuinnLegActorSnapshot,
    outbound_queue: Option<QuinnLegActorOutboundQueueColdObservation>,
    observed_error: Option<QuinnLegActorError>,
}

impl QuinnLegActorSnapshot {
    pub(crate) const fn outbound_queue_messages(self) -> usize {
        self.outbound_queue_messages
    }
    pub(crate) const fn outbound_queue_bytes(self) -> usize {
        self.outbound_queue_bytes
    }
    pub(crate) const fn validated_data_headers(self) -> usize {
        self.validated_data_headers
    }
    pub(crate) const fn queued_inbound_messages(self) -> usize {
        self.queued_inbound_messages
    }
    pub(crate) const fn inbound_owned_bytes(self) -> usize {
        self.inbound_owned_bytes
    }
    pub(crate) const fn waiting_inbound_bytes(self) -> usize {
        self.waiting_inbound_bytes
    }
    pub(crate) const fn reserved_delivery_acks(self) -> usize {
        self.reserved_delivery_acks
    }
    pub(crate) const fn ack_owned_bytes(self) -> usize {
        self.ack_owned_bytes
    }
    pub(crate) const fn composition_owned_messages(self) -> usize {
        self.composition_owned_messages
    }
    pub(crate) const fn outbound_owned_messages(self) -> usize {
        self.outbound_owned_messages
    }
    pub(crate) const fn outbound_owned_bytes(self) -> usize {
        self.outbound_owned_bytes
    }

    #[cfg(test)]
    const fn blocked_write(self) -> Option<TestPendingWrite> {
        self.blocked_write
    }

    #[cfg(test)]
    const fn pending_delivery_count(self) -> usize {
        self.pending_delivery_count
    }

    #[cfg(test)]
    const fn pending_delivery_ordinal(self) -> Option<u64> {
        self.pending_delivery_ordinal
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TestWriteKind {
    DataHeader,
    DataPayload,
    DeliveredHeader,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TestPendingWrite {
    ordinal: u64,
    kind: TestWriteKind,
}

#[cfg(test)]
#[derive(Default)]
struct TestWritePollProbe {
    last_pending: Mutex<Option<TestPendingWrite>>,
}

#[cfg(test)]
impl TestWritePollProbe {
    fn record_pending(&self, pending: TestPendingWrite) {
        *self
            .last_pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(pending);
    }

    fn last_pending(&self) -> Option<TestPendingWrite> {
        *self
            .last_pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
#[derive(Default)]
struct TestChildPublicationGate {
    armed: AtomicBool,
    blocked: AtomicBool,
    released: AtomicBool,
    blocked_notify: Notify,
    release_notify: Notify,
}

#[cfg(test)]
impl TestChildPublicationGate {
    fn arm(&self) {
        self.released.store(false, Ordering::Release);
        self.blocked.store(false, Ordering::Release);
        self.armed.store(true, Ordering::Release);
    }

    async fn pause_if_armed(&self) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        self.blocked.store(true, Ordering::Release);
        self.blocked_notify.notify_waiters();
        while !self.released.load(Ordering::Acquire) {
            let notified = self.release_notify.notified();
            if self.released.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    async fn wait_until_blocked(&self) {
        while !self.blocked.load(Ordering::Acquire) {
            let notified = self.blocked_notify.notified();
            if self.blocked.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_waiters();
    }
}

#[cfg(test)]
#[must_use = "the join block must be released before actor cleanup can complete"]
pub(super) struct QuinnLegActorJoinBlock {
    gate: Arc<TestChildPublicationGate>,
}

#[cfg(test)]
impl QuinnLegActorJoinBlock {
    pub(super) async fn wait_until_blocked(&self) {
        self.gate.wait_until_blocked().await;
    }

    pub(super) fn release(&self) {
        self.gate.release();
    }
}

#[cfg(test)]
#[must_use = "the data-writer block must be released before actor progress can resume"]
pub(super) struct QuinnLegActorDataWriterBlock {
    gate: Arc<TestChildPublicationGate>,
}

#[cfg(test)]
impl QuinnLegActorDataWriterBlock {
    pub(super) async fn wait_until_blocked(&self) {
        self.gate.wait_until_blocked().await;
    }

    pub(super) fn release(&self) {
        self.gate.release();
    }
}

#[cfg(test)]
impl Drop for QuinnLegActorDataWriterBlock {
    fn drop(&mut self) {
        // A failed assertion must not strand the fixed actor child at the
        // deterministic pressure seam and hang structured cleanup.
        self.gate.release();
    }
}

#[cfg(test)]
#[must_use = "the post-terminal cleanup block must be released before actor cleanup can complete"]
pub(super) struct QuinnLegActorPostTerminalCleanupBlock {
    gate: Arc<TestChildPublicationGate>,
}

#[cfg(test)]
impl QuinnLegActorPostTerminalCleanupBlock {
    pub(super) async fn wait_until_blocked(&self) {
        self.gate.wait_until_blocked().await;
    }

    pub(super) fn release(&self) {
        self.gate.release();
    }
}

#[cfg(test)]
#[must_use = "the inbound publication block must be released before the record can enter the actor queue"]
pub(super) struct QuinnLegActorInboundPublicationBlock {
    gate: Arc<TestChildPublicationGate>,
}

#[cfg(test)]
impl QuinnLegActorInboundPublicationBlock {
    pub(super) async fn wait_until_blocked(&self) {
        self.gate.wait_until_blocked().await;
    }

    pub(super) fn release(&self) {
        self.gate.release();
    }
}

#[cfg(test)]
#[derive(Default)]
struct TestPortLinearizationGate {
    armed: AtomicBool,
    blocked: AtomicBool,
    released: AtomicBool,
    blocked_notify: Notify,
    release_lock: Mutex<()>,
    release_notify: Condvar,
}

#[cfg(test)]
#[must_use = "the armed port gate guard must be held until the blocking port is released"]
struct TestPortLinearizationReleaseGuard {
    gate: Arc<TestPortLinearizationGate>,
    released: bool,
}

#[cfg(test)]
impl TestPortLinearizationReleaseGuard {
    fn release(mut self) {
        self.gate.release_waiter();
        self.released = true;
    }
}

#[cfg(test)]
impl Drop for TestPortLinearizationReleaseGuard {
    fn drop(&mut self) {
        if !self.released {
            self.gate.release_waiter();
            self.released = true;
        }
    }
}

#[cfg(test)]
impl TestPortLinearizationGate {
    fn arm(self: &Arc<Self>) -> TestPortLinearizationReleaseGuard {
        let _guard = self
            .release_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.released.store(false, Ordering::Release);
        self.blocked.store(false, Ordering::Release);
        self.armed.store(true, Ordering::Release);
        TestPortLinearizationReleaseGuard {
            gate: Arc::clone(self),
            released: false,
        }
    }

    fn pause_if_armed(&self) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        let mut guard = self
            .release_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.blocked.store(true, Ordering::Release);
        self.blocked_notify.notify_waiters();
        while !self.released.load(Ordering::Acquire) {
            guard = self
                .release_notify
                .wait(guard)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    async fn wait_until_blocked(&self) {
        while !self.blocked.load(Ordering::Acquire) {
            let notified = self.blocked_notify.notified();
            if self.blocked.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    }

    fn release_waiter(&self) {
        let _guard = self
            .release_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.released.store(true, Ordering::Release);
        self.release_notify.notify_all();
    }
}

/// Unforgeable capability retained by the one started actor and accepted only
/// by LegIo's fixed-actor ingress boundary. Its mint and field stay private to
/// this module.
pub(crate) struct QuinnLegActorInboundGate(());

impl QuinnLegActorInboundGate {
    const fn mint_for_started_actor() -> Self {
        Self(())
    }
}

mod inbound_ownership {
    use super::{
        BoundInbound, QuinnLegActorInboundGate, QuinnLegCompositionGate, SimTime, TrackedPermit,
    };

    #[must_use = "dropping the typed inbound is the only final discard that releases Ib/Cm ownership"]
    pub(crate) struct QuinnLegActorInbound {
        inbound: BoundInbound,
        arrived_at: SimTime,
        _byte_ownership: TrackedPermit,
        _composition_ownership: TrackedPermit,
    }

    impl QuinnLegActorInbound {
        pub(super) fn mint(
            _gate: &QuinnLegActorInboundGate,
            inbound: BoundInbound,
            arrived_at: SimTime,
            byte_ownership: TrackedPermit,
            composition_ownership: TrackedPermit,
        ) -> Self {
            Self {
                inbound,
                arrived_at,
                _byte_ownership: byte_ownership,
                _composition_ownership: composition_ownership,
            }
        }

        pub(super) fn consume_for_composition<T>(
            self,
            _gate: &QuinnLegCompositionGate,
            consume: impl FnOnce(BoundInbound, SimTime) -> T,
        ) -> T {
            let Self {
                inbound,
                arrived_at,
                _byte_ownership,
                _composition_ownership,
            } = self;
            let value = consume(inbound, arrived_at);
            drop((_byte_ownership, _composition_ownership));
            value
        }

        #[cfg(test)]
        pub(super) fn consume_for_test(self, consume: impl FnOnce(BoundInbound)) {
            let Self {
                inbound,
                arrived_at: _,
                _byte_ownership,
                _composition_ownership,
            } = self;
            consume(inbound);
            drop((_byte_ownership, _composition_ownership));
        }
    }

    impl std::fmt::Debug for QuinnLegActorInbound {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("QuinnLegActorInbound")
                .field("inbound", &self.inbound)
                .finish()
        }
    }
}

pub(crate) use inbound_ownership::QuinnLegActorInbound;

#[derive(Debug)]
pub(crate) struct QuinnLegActorJoined {
    actor_incarnation: PhysicalLegRuntimeIncarnation,
    owned_messages: usize,
    owned_bytes: usize,
    terminal: Option<ExactLegTerminal>,
    terminal_failure: Option<Arc<QuinnLegActorError>>,
    terminal_stamp: Option<ActorTerminalStampWitness>,
}

impl QuinnLegActorJoined {
    pub(crate) const fn owned_messages(&self) -> usize {
        self.owned_messages
    }
    pub(crate) const fn owned_bytes(&self) -> usize {
        self.owned_bytes
    }

    pub(crate) const fn owned_terminals(&self) -> usize {
        if self.terminal.is_some() { 1 } else { 0 }
    }

    pub(crate) fn terminal_failure(&self) -> Option<&QuinnLegActorError> {
        self.terminal_failure.as_deref()
    }

    pub(crate) fn terminal_failure_observed_at(&self) -> Option<SimTime> {
        self.terminal_stamp
            .as_ref()
            .and_then(ActorTerminalStampWitness::observed_at)
    }

    pub(super) const fn terminal_stamp(&self) -> Option<&ActorTerminalStampWitness> {
        self.terminal_stamp.as_ref()
    }

    pub(super) fn take_terminal_stamp(&mut self) -> Option<ActorTerminalStampWitness> {
        self.terminal_stamp.take()
    }

    pub(super) fn restore_terminal_stamp(&mut self, stamp: ActorTerminalStampWitness) -> bool {
        if self.terminal_stamp.is_some() {
            return false;
        }
        self.terminal_stamp = Some(stamp);
        true
    }

    pub(super) fn physical_incarnation(
        &self,
        _gate: &QuinnLegCompositionGate,
    ) -> &PhysicalLegRuntimeIncarnation {
        &self.actor_incarnation
    }

    pub(super) fn transact_terminal<T, E>(
        &mut self,
        _gate: &QuinnLegCompositionGate,
        transaction: impl FnOnce(
            &PhysicalLegRuntimeIncarnation,
            Option<ExactLegTerminal>,
        ) -> Result<T, (Option<ExactLegTerminal>, E)>,
    ) -> Result<T, E> {
        let terminal = self.terminal.take();
        match transaction(&self.actor_incarnation, terminal) {
            Ok(value) => Ok(value),
            Err((terminal, error)) => {
                self.terminal = terminal;
                Err(error)
            }
        }
    }

    #[cfg(test)]
    fn terminal_reason(&self) -> LegTransportTerminalReason {
        self.terminal.as_ref().map_or(
            LegTransportTerminalReason::FatalIo,
            ExactLegTerminal::reason,
        )
    }
}

#[derive(Debug)]
pub(crate) struct QuinnLegActorListenerJoined {
    _private: (),
}

struct ActorRootOutcome {
    failure: Option<Arc<QuinnLegActorError>>,
    terminal_stamp: ActorTerminalStampWitness,
}

struct ActorFirstFailure {
    outcome: Mutex<ActorFirstOutcome>,
    clock: SessionMonotonicClock,
}

/// Non-forgeable linear reference to the actor's one canonical first-failure
/// time. The `Option<SimTime>` payload remains stored exactly once inside
/// `ActorFirstOutcome`; this handle cannot be cloned by downstream owners.
pub(super) struct ActorTerminalStampWitness(Arc<ActorFirstFailure>);

/// The sole mint transaction creates the two handles required by the live
/// terminal handoff/root-outcome overlap. Neither handle is cloneable, so no
/// later layer can widen the checked fixed-handle capacity bound.
struct ActorTerminalStampPair {
    handoff: ActorTerminalStampWitness,
    root_outcome: ActorTerminalStampWitness,
}

impl ActorTerminalStampPair {
    fn mint(first_failure: &Arc<ActorFirstFailure>) -> Self {
        Self {
            handoff: ActorTerminalStampWitness(Arc::clone(first_failure)),
            root_outcome: ActorTerminalStampWitness(Arc::clone(first_failure)),
        }
    }
}

impl ActorTerminalStampWitness {
    pub(super) fn observed_at(&self) -> Option<SimTime> {
        self.0.failure_observed_at()
    }

    pub(super) fn same_source(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    fn belongs_to(&self, first_failure: &Arc<ActorFirstFailure>) -> bool {
        Arc::ptr_eq(&self.0, first_failure)
    }

    #[cfg(test)]
    fn source_id_for_test(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }
}

impl std::fmt::Debug for ActorTerminalStampWitness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ActorTerminalStampWitness([REDACTED])")
    }
}

#[derive(Default)]
enum ActorFirstOutcome {
    #[default]
    Open,
    LocalShutdownWon,
    Failed(Arc<QuinnLegActorError>, Option<SimTime>),
}

enum ActorFirstBarrier {
    LocalShutdownWon,
    Failed(Arc<QuinnLegActorError>),
}

impl ActorFirstBarrier {
    fn into_actor_error(self) -> QuinnLegActorError {
        match self {
            Self::LocalShutdownWon => QuinnLegActorError::Closing,
            Self::Failed(cause) => QuinnLegActorError::RootFailed(cause),
        }
    }
}

impl Default for ActorFirstFailure {
    fn default() -> Self {
        Self::new(SessionMonotonicClock::start())
    }
}

impl ActorFirstFailure {
    fn new(clock: SessionMonotonicClock) -> Self {
        Self {
            outcome: Mutex::new(ActorFirstOutcome::Open),
            clock,
        }
    }

    fn stamped_failure(
        &self,
        error: Arc<QuinnLegActorError>,
    ) -> (Arc<QuinnLegActorError>, Option<SimTime>) {
        match self.clock.now() {
            Ok(observed_at) => (error, Some(observed_at)),
            Err(clock_error) => (Arc::new(QuinnLegActorError::Clock(clock_error)), None),
        }
    }

    fn install_locked(
        &self,
        outcome: &mut ActorFirstOutcome,
        error: Arc<QuinnLegActorError>,
    ) -> Arc<QuinnLegActorError> {
        let (error, observed_at) = self.stamped_failure(error);
        *outcome = ActorFirstOutcome::Failed(Arc::clone(&error), observed_at);
        error
    }

    /// Installs a failure whose exact instant is already carried by a
    /// composition-owned absolute deadline. This is deliberately narrower
    /// than `install_locked`: transport and protocol failures still capture
    /// the actor clock at the first-failure transition, while a committed
    /// probe deadline must retain its original instant across root delay.
    fn install_locked_at(
        &self,
        outcome: &mut ActorFirstOutcome,
        error: Arc<QuinnLegActorError>,
        observed_at: SimTime,
    ) -> Arc<QuinnLegActorError> {
        *outcome = ActorFirstOutcome::Failed(Arc::clone(&error), Some(observed_at));
        error
    }

    fn record(&self, error: QuinnLegActorError) -> Option<Arc<QuinnLegActorError>> {
        self.record_shared(Arc::new(error))
    }

    fn record_shared(&self, error: Arc<QuinnLegActorError>) -> Option<Arc<QuinnLegActorError>> {
        let mut outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*outcome {
            ActorFirstOutcome::Failed(cause, _) => return Some(Arc::clone(cause)),
            ActorFirstOutcome::LocalShutdownWon => return None,
            ActorFirstOutcome::Open => {}
        }
        Some(self.install_locked(&mut outcome, error))
    }

    fn current(&self) -> Option<Arc<QuinnLegActorError>> {
        let outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*outcome {
            ActorFirstOutcome::Failed(cause, _) => Some(Arc::clone(cause)),
            ActorFirstOutcome::Open | ActorFirstOutcome::LocalShutdownWon => None,
        }
    }

    fn failure_observed_at(&self) -> Option<SimTime> {
        let outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*outcome {
            ActorFirstOutcome::Failed(_, observed_at) => *observed_at,
            ActorFirstOutcome::Open | ActorFirstOutcome::LocalShutdownWon => None,
        }
    }

    fn mark_local_shutdown_if_open(&self) {
        let mut outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if matches!(*outcome, ActorFirstOutcome::Open) {
            *outcome = ActorFirstOutcome::LocalShutdownWon;
        }
    }

    #[cfg(test)]
    fn local_shutdown_won(&self) -> bool {
        matches!(
            *self
                .outcome
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            ActorFirstOutcome::LocalShutdownWon
        )
    }

    /// Linearizes one synchronous port operation against first-failure
    /// publication without making an operation-local error the actor's fatal
    /// cause. The nested result preserves that distinction for the caller.
    fn run_if_clear<T, E>(
        &self,
        operation: impl FnOnce() -> Result<T, E>,
    ) -> Result<Result<T, E>, ActorFirstBarrier> {
        let outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*outcome {
            ActorFirstOutcome::Failed(cause, _) => {
                return Err(ActorFirstBarrier::Failed(Arc::clone(cause)));
            }
            ActorFirstOutcome::LocalShutdownWon => {
                return Err(ActorFirstBarrier::LocalShutdownWon);
            }
            ActorFirstOutcome::Open => {}
        }
        Ok(operation())
    }

    /// Value-only form for a synchronous composition operation. The failure
    /// mutex remains held through the complete ownership borrow/transfer, so
    /// a child fatal cannot publish between the clear check and controller or
    /// encoded-transport commit.
    fn run_value_if_clear<T>(&self, operation: impl FnOnce() -> T) -> Result<T, ActorFirstBarrier> {
        let outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*outcome {
            ActorFirstOutcome::Failed(cause, _) => {
                return Err(ActorFirstBarrier::Failed(Arc::clone(cause)));
            }
            ActorFirstOutcome::LocalShutdownWon => {
                return Err(ActorFirstBarrier::LocalShutdownWon);
            }
            ActorFirstOutcome::Open => {}
        }
        Ok(operation())
    }

    /// Input-carrying form of `run_if_clear`. If a failure already won, the
    /// exact unconsumed input is returned with that same cause. An operation
    /// error remains nested and is not installed as the actor's first cause.
    fn run_with_input_if_clear<I, T, E>(
        &self,
        input: I,
        operation: impl FnOnce(I) -> Result<T, E>,
    ) -> Result<Result<T, E>, (I, ActorFirstBarrier)> {
        let outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*outcome {
            ActorFirstOutcome::Failed(cause, _) => {
                return Err((input, ActorFirstBarrier::Failed(Arc::clone(cause))));
            }
            ActorFirstOutcome::LocalShutdownWon => {
                return Err((input, ActorFirstBarrier::LocalShutdownWon));
            }
            ActorFirstOutcome::Open => {}
        }
        Ok(operation(input))
    }

    /// Linearizes one synchronous actor-side commit against actor-side and
    /// cooperatively returned fixed-child failures. Panic/JoinError fallback
    /// publication remains root-owned and is not covered by this guarantee.
    /// An already-recorded failure suppresses the commit; a commit failure
    /// becomes the exact first cause before this lock is released.
    fn commit<T>(
        &self,
        operation: impl FnOnce() -> Result<T, QuinnLegActorError>,
    ) -> Result<T, ActorFirstBarrier> {
        let mut outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*outcome {
            ActorFirstOutcome::Failed(cause, _) => {
                return Err(ActorFirstBarrier::Failed(Arc::clone(cause)));
            }
            ActorFirstOutcome::LocalShutdownWon => {
                return Err(ActorFirstBarrier::LocalShutdownWon);
            }
            ActorFirstOutcome::Open => {}
        }
        match operation() {
            Ok(value) => Ok(value),
            Err(error) => {
                let error = self.install_locked(&mut outcome, Arc::new(error));
                Err(ActorFirstBarrier::Failed(error))
            }
        }
    }

    /// Atomically validates one exact controller-owned deadline and, only
    /// after that validation commits, installs its semantic failure as this
    /// actor's first cause. A prior transport failure returns the unchanged
    /// input; a rejected/stale deadline does not mutate the actor cause.
    #[cfg(test)]
    fn commit_failure_with_input<I, T>(
        &self,
        input: I,
        failure: QuinnLegActorError,
        operation: impl FnOnce(I) -> Result<T, I>,
    ) -> Result<Result<T, I>, (I, ActorFirstBarrier)> {
        let mut outcome = self
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*outcome {
            ActorFirstOutcome::Failed(cause, _) => {
                return Err((input, ActorFirstBarrier::Failed(Arc::clone(cause))));
            }
            ActorFirstOutcome::LocalShutdownWon => {
                return Err((input, ActorFirstBarrier::LocalShutdownWon));
            }
            ActorFirstOutcome::Open => {}
        }
        match operation(input) {
            Ok(value) => {
                self.install_locked(&mut outcome, Arc::new(failure));
                Ok(Ok(value))
            }
            Err(input) => Ok(Err(input)),
        }
    }
}

struct ActorRootExitSignal {
    cancel: watch::Sender<bool>,
    first_failure: Arc<ActorFirstFailure>,
    readiness: Readiness,
    armed: bool,
}

impl ActorRootExitSignal {
    fn new(
        cancel: watch::Sender<bool>,
        first_failure: Arc<ActorFirstFailure>,
        readiness: Readiness,
    ) -> Self {
        Self {
            cancel,
            first_failure,
            readiness,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn publish_failure(&mut self, error: Arc<QuinnLegActorError>) -> Arc<QuinnLegActorError> {
        let _recorded = self.first_failure.record_shared(Arc::clone(&error));
        self.cancel.send_replace(true);
        self.readiness.advance();
        self.disarm();
        error
    }
}

impl Drop for ActorRootExitSignal {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let _recorded = self
            .first_failure
            .record(QuinnLegActorError::Protocol(ROOT_EXITED_WITHOUT_OUTCOME));
        self.cancel.send_replace(true);
        self.readiness.advance();
    }
}

enum ObservedRootOutcome {
    Completed(ActorRootOutcome),
    ActorError(Arc<QuinnLegActorError>),
    JoinError(tokio::task::JoinError),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActorOwnerPhase {
    Running,
    Closing,
    Joined,
}

pub(super) enum QuinnLegActorEnqueueError {
    RootFailed {
        frame: LegControlFrame,
        cause: Arc<QuinnLegActorError>,
    },
    Closing {
        frame: LegControlFrame,
    },
    AlreadyJoined {
        frame: LegControlFrame,
    },
    Queue(LegControlQueueError),
}

/// Composition-only decision for one raw actor record. A blocked controller
/// must return the exact encoded value so the actor can restore its existing
/// ownership without reordering or allocating another queue entry.
pub(super) enum QuinnLegActorInboundDecision<T> {
    Classified(T),
    Blocked(QuinnLegActorEncodedInbound),
}

/// Immutable first half of a physical-owner inbound transaction. Only this
/// phase may defer an actor record; mutable LegIo/FIFO authority is not
/// available until `Ready` has irrevocably selected commit.
pub(super) enum QuinnLegActorInboundPreflight<P> {
    Ready(P),
    Blocked,
}

/// Result of the client-B namespace dispatcher while the actor still owns the
/// exact raw record reservation. Ordinary protocol work is committed under
/// the first-failure lock; physical control can instead move into one opaque
/// held value without releasing its arrival, ordinal, byte, or delivery-ACK
/// ownership.
pub(super) enum QuinnLegActorCompositionInboundDecision<T, H> {
    Classified(T),
    Hold(H),
}

pub(super) enum QuinnLegActorCompositionInboundPoll<T, H> {
    Idle,
    Classified(T),
    Held(QuinnLegActorHeldInbound<H>),
}

/// One complete actor DATA value plus the immutable endpoint-session arrival
/// captured before publication. Only the actor can mint this envelope; a
/// blocked controller must return it whole so retry cannot recapture time.
pub(super) struct QuinnLegActorEncodedInbound {
    delivery: EncodedDelivery,
    arrived_at: SimTime,
}

/// Non-cloneable physical inbound retained across a control-side retry. The
/// private record carries the original tracked byte permit, publication
/// reservation, queued marker, and ordered-delivery ACK until `try_commit`
/// succeeds. Dropping this value is therefore an unclean discard, never a
/// clean ownership witness.
pub(super) struct QuinnLegActorHeldInbound<T> {
    record: ControllerInboundRecord,
    value: T,
    arrived_at: SimTime,
    publication_epoch: u64,
    send_ordinal: u64,
    encoded_bytes: usize,
}

/// One exact actor-held inbound whose shutdown-only discard committed.  The
/// actor record, tracked permits, queued marker, and unsent reserved DELIVERY
/// were consumed exactly once; only the already-decoded typed value remains
/// for the composition to discard before publishing its copy-only audit.
pub(super) struct QuinnLegActorShutdownDiscarded<T> {
    value: T,
}

impl<T> QuinnLegActorShutdownDiscarded<T> {
    pub(super) fn into_value(self) -> T {
        self.value
    }
}

impl<T> QuinnLegActorHeldInbound<T> {
    pub(super) const fn arrived_at(&self) -> SimTime {
        self.arrived_at
    }

    pub(super) const fn publication_epoch(&self) -> u64 {
        self.publication_epoch
    }

    pub(super) const fn send_ordinal(&self) -> u64 {
        self.send_ordinal
    }

    pub(super) const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    /// Copy-only capacity carried by this one raw held record. These counters
    /// expose no record, channel permit, DELIVERY capability, or typed value.
    pub(super) const fn held_records(&self) -> usize {
        1
    }

    pub(super) const fn queued_inbound_markers(&self) -> usize {
        1
    }

    pub(super) const fn composition_message_permits(&self) -> usize {
        1
    }

    pub(super) fn reserved_delivery_acks(&self) -> usize {
        usize::from(self.record.reserved_delivery.is_some())
    }

    pub(super) fn reserved_delivery_ack_bytes(&self) -> usize {
        self.reserved_delivery_acks() * RECORD_HEADER_BYTES
    }

    pub(super) fn value(&self) -> &T {
        &self.value
    }

    /// Commits the original actor record only when the exact transformed
    /// control value reaches its next ownership seam. A rejection rebuilds
    /// this same held value with every raw permit and timestamp unchanged.
    pub(super) fn try_commit<U, E>(
        self,
        operation: impl FnOnce(T) -> Result<U, (T, E)>,
    ) -> Result<U, (Self, E)> {
        let Self {
            record,
            value,
            arrived_at,
            publication_epoch,
            send_ordinal,
            encoded_bytes,
        } = self;
        match operation(value) {
            Ok(committed) => {
                record.commit_classified();
                Ok(committed)
            }
            Err((value, error)) => Err((
                Self {
                    record,
                    value,
                    arrived_at,
                    publication_epoch,
                    send_ordinal,
                    encoded_bytes,
                },
                error,
            )),
        }
    }

    pub(super) fn commit(self) -> T {
        let Self { record, value, .. } = self;
        record.commit_classified();
        value
    }

    /// Shutdown-only counterpart to semantic commit. A validated discard
    /// releases the exact actor record without sending its reserved DELIVERY
    /// ACK: lifecycle cutoff means no downstream consumer accepted the value.
    /// Validation failure reconstructs the original held owner unchanged.
    pub(super) fn try_shutdown_discard<U, E>(
        self,
        _gate: &QuinnLegCompositionGate,
        operation: impl FnOnce(T) -> Result<U, (T, E)>,
    ) -> Result<QuinnLegActorShutdownDiscarded<U>, (Self, E)> {
        let Self {
            record,
            value,
            arrived_at,
            publication_epoch,
            send_ordinal,
            encoded_bytes,
        } = self;
        match operation(value) {
            Ok(value) => {
                record.commit_shutdown_discard();
                Ok(QuinnLegActorShutdownDiscarded { value })
            }
            Err((value, error)) => Err((
                Self {
                    record,
                    value,
                    arrived_at,
                    publication_epoch,
                    send_ordinal,
                    encoded_bytes,
                },
                error,
            )),
        }
    }
}

/// Opaque endpoint-clock evidence copied only at the synchronous actor /
/// composition boundary. The controller can verify clock identity and use the
/// immutable actor timestamp without accepting a caller-selected `SimTime`.
pub(super) struct QuinnOwnerActorArrival {
    clock: SessionMonotonicClock,
    arrived_at: SimTime,
}

impl QuinnOwnerActorArrival {
    #[allow(
        dead_code,
        reason = "the owner Quinn composition slice will call this gated constructor"
    )]
    pub(super) fn from_actor_capture(
        clock: &SessionMonotonicClock,
        arrived_at: SimTime,
        _gate: &QuinnLegCompositionGate,
    ) -> Self {
        Self {
            clock: clock.clone(),
            arrived_at,
        }
    }

    #[cfg(test)]
    pub(super) fn capture_for_test(
        clock: &SessionMonotonicClock,
    ) -> Result<Self, SessionClockError> {
        Ok(Self {
            clock: clock.clone(),
            arrived_at: clock.now()?,
        })
    }

    pub(super) fn same_clock(&self, clock: &SessionMonotonicClock) -> bool {
        self.clock.same_source(clock)
    }

    pub(super) const fn arrived_at(&self) -> SimTime {
        self.arrived_at
    }
}

impl std::fmt::Debug for QuinnOwnerActorArrival {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("QuinnOwnerActorArrival([REDACTED])")
    }
}

impl QuinnLegActorEncodedInbound {
    fn mint(delivery: EncodedDelivery, arrived_at: SimTime) -> Self {
        Self {
            delivery,
            arrived_at,
        }
    }

    pub(super) const fn arrived_at(&self) -> SimTime {
        self.arrived_at
    }

    pub(super) fn into_parts(self) -> (EncodedDelivery, SimTime) {
        (self.delivery, self.arrived_at)
    }

    pub(super) fn restore_delivery(delivery: EncodedDelivery, arrived_at: SimTime) -> Self {
        Self {
            delivery,
            arrived_at,
        }
    }
}

pub(super) enum QuinnLegActorInboundPoll<T> {
    Idle,
    Blocked,
    Classified(T),
}

/// One bounded receive-before-timer arbitration result. Every path except a
/// successful semantic commit returns the exact non-cloneable timer input.
/// A controller-local invariant remains distinct from actor failure: the
/// outer session root owns sticky-failure policy and structured cleanup.
pub(super) enum QuinnLegActorInboundTimerPoll<I, T, D, E> {
    Inbound {
        poll: QuinnLegActorInboundPoll<T>,
        timer: I,
    },
    PublicationPending(I),
    TimerCommitted(D),
    TimerRejected(I),
    TimerInvariant {
        timer: I,
        error: E,
    },
    Suppressed {
        timer: I,
        cause: Arc<QuinnLegActorError>,
    },
    LocalShutdown(I),
    Failed {
        timer: I,
        cause: Arc<QuinnLegActorError>,
    },
}

pub(super) type QuinnLegActorInboundDeadlinePoll<I, T, D> =
    QuinnLegActorInboundTimerPoll<I, T, D, std::convert::Infallible>;

/// Composition-owned semantic timers persist the first due arbitration's
/// inbound publication high-water. Later actor records must not be able to
/// extend an already-due absolute deadline indefinitely.
pub(super) trait QuinnLegActorSemanticTimer {
    fn capture_inbound_publication_cutoff(&mut self, high_water: u64) -> u64;
}

/// Absolute policy deadline against the actor-captured arrival timestamp.
/// Root observation still decides when to run arbitration; this comparison
/// decides whether the already-complete record may beat that deadline.
pub(super) trait QuinnLegActorArrivalDeadline {
    fn arrival_deadline(&self) -> SimTime;
}

fn arrival_beats_or_meets_deadline(arrived_at: SimTime, deadline: SimTime) -> bool {
    arrived_at <= deadline
}

enum QuinnLegActorTimerCommitMode {
    Fatal(QuinnLegActorError),
    Semantic,
}

pub(super) enum QuinnLegActorTimerDecision<I, D, E> {
    Committed(D),
    Rejected(I),
    Invariant { timer: I, error: E },
}

#[cfg(test)]
pub(super) enum QuinnLegActorProbeDeadlineCommit<T, I> {
    Committed(T),
    Rejected(I),
    Suppressed {
        input: I,
        cause: Arc<QuinnLegActorError>,
    },
}

#[derive(Debug)]
pub(super) enum QuinnLegActorReportedTerminalPoll<T, E> {
    Pending,
    Applied(T),
    Rejected(E),
    AlreadyConsumed,
}

enum ActorReportedTerminalState {
    Awaiting(oneshot::Receiver<QuinnLegActorReportedTerminal>),
    Ready(QuinnLegActorReportedTerminal),
    Consumed,
    ChannelClosed,
}

/// The exact transport terminal and the immutable first-failure timestamp are
/// one actor-owned handoff. Rejection must return this envelope whole; policy
/// must never reconstruct the hard-terminal time after cleanup or join delay.
pub(super) struct QuinnLegActorReportedTerminal {
    terminal: ExactLegTerminal,
    terminal_stamp: ActorTerminalStampWitness,
}

impl QuinnLegActorReportedTerminal {
    fn mint(terminal: ExactLegTerminal, terminal_stamp: ActorTerminalStampWitness) -> Self {
        Self {
            terminal,
            terminal_stamp,
        }
    }

    pub(super) const fn reason(&self) -> LegTransportTerminalReason {
        self.terminal.reason()
    }

    pub(super) fn failure_observed_at(&self) -> Option<SimTime> {
        self.terminal_stamp.observed_at()
    }

    pub(super) fn into_parts(self) -> (ExactLegTerminal, ActorTerminalStampWitness) {
        (self.terminal, self.terminal_stamp)
    }

    pub(super) fn restore(
        terminal: ExactLegTerminal,
        terminal_stamp: ActorTerminalStampWitness,
    ) -> Self {
        Self {
            terminal,
            terminal_stamp,
        }
    }
}

impl ActorReportedTerminalState {
    fn refresh(&mut self) -> Result<(), QuinnLegActorError> {
        let state = std::mem::replace(self, Self::ChannelClosed);
        match state {
            Self::Awaiting(mut receiver) => match receiver.try_recv() {
                Ok(terminal) => {
                    *self = Self::Ready(terminal);
                    Ok(())
                }
                Err(oneshot::error::TryRecvError::Empty) => {
                    *self = Self::Awaiting(receiver);
                    Ok(())
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    *self = Self::ChannelClosed;
                    Err(QuinnLegActorError::TerminalHandoffClosed)
                }
            },
            state => {
                *self = state;
                if matches!(self, Self::ChannelClosed) {
                    Err(QuinnLegActorError::TerminalHandoffClosed)
                } else {
                    Ok(())
                }
            }
        }
    }

    fn take_for_join(
        &mut self,
    ) -> Result<Option<QuinnLegActorReportedTerminal>, QuinnLegActorError> {
        self.refresh()?;
        match std::mem::replace(self, Self::Consumed) {
            Self::Ready(terminal) => Ok(Some(terminal)),
            Self::Consumed => Ok(None),
            Self::Awaiting(receiver) => {
                *self = Self::Awaiting(receiver);
                Err(QuinnLegActorError::TerminalHandoffIncomplete)
            }
            Self::ChannelClosed => {
                *self = Self::ChannelClosed;
                Err(QuinnLegActorError::TerminalHandoffClosed)
            }
        }
    }
}

impl std::fmt::Debug for QuinnLegActorEnqueueError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RootFailed { frame, cause } => {
                let _redacted_frame = frame;
                formatter
                    .debug_struct("QuinnLegActorEnqueueError::RootFailed")
                    .field("frame", &"[REDACTED]")
                    .field("cause", cause)
                    .finish()
            }
            Self::Closing { frame } => {
                let _redacted_frame = frame;
                formatter
                    .debug_struct("QuinnLegActorEnqueueError::Closing")
                    .field("frame", &"[REDACTED]")
                    .finish()
            }
            Self::AlreadyJoined { frame } => {
                let _redacted_frame = frame;
                formatter
                    .debug_struct("QuinnLegActorEnqueueError::AlreadyJoined")
                    .field("frame", &"[REDACTED]")
                    .finish()
            }
            Self::Queue(error) => formatter.debug_tuple("Queue").field(error).finish(),
        }
    }
}

/// Opaque identity plus epoch for the composition owner's check-then-park
/// protocol. Keeping the readiness source in the cursor prevents an actor
/// replacement with the same numeric epoch from impersonating old progress.
#[derive(Clone)]
pub(super) struct QuinnLegActorOwnerProgressCursor {
    readiness: Readiness,
    epoch: u64,
}

pub(crate) struct StartedQuinnLegActor {
    session_clock: SessionMonotonicClock,
    capacity: QuinnLegActorCapacityWitness,
    actor_incarnation: Option<PhysicalLegRuntimeIncarnation>,
    io: Option<LegIo>,
    queue: Option<LegOutboundQueue>,
    controller_parts_transferred: bool,
    transport: Option<ActorEncodedTransport>,
    inbound: Option<mpsc::Receiver<InboundRecord>>,
    inbound_handoff: Option<oneshot::Sender<mpsc::Receiver<InboundRecord>>>,
    pending_controller_inbound: Option<ControllerInboundRecord>,
    /// Complete inbound records reserve one monotonic publication epoch before
    /// competing with timers at `first_failure`. Epoch/high-water evidence
    /// closes the queue-Empty versus in-flight-publication race; the immutable
    /// actor-captured arrival time, not the epoch, decides deadline eligibility.
    inbound_publications: Arc<InboundPublicationTracker>,
    inbound_gate: QuinnLegActorInboundGate,
    cancel: watch::Sender<bool>,
    first_failure: Arc<ActorFirstFailure>,
    reported_terminal: ActorReportedTerminalState,
    readiness: Readiness,
    counters: Arc<ActorCounters>,
    #[cfg(test)]
    pending_deliveries: Arc<Mutex<BTreeMap<u64, PendingDelivery>>>,
    #[cfg(test)]
    write_poll_probe: Arc<TestWritePollProbe>,
    #[cfg(test)]
    child_publication_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)]
    post_terminal_cleanup_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)]
    inbound_publication_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)]
    progress_registration_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)]
    delivery_writer_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)]
    data_writer_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)]
    panic_data_writer: Arc<AtomicBool>,
    #[cfg(test)]
    port_linearization_gate: Arc<TestPortLinearizationGate>,
    nursery: JoinSet<Result<ActorRootOutcome, Arc<QuinnLegActorError>>>,
    #[cfg(test)]
    root_abort: tokio::task::AbortHandle,
    observed_root: Option<ObservedRootOutcome>,
    #[cfg(test)]
    root_outer_error: Arc<AtomicBool>,
    server_registration: Option<ServerActorRegistration>,
    phase: ActorOwnerPhase,
}

impl StartedQuinnLegActor {
    pub(super) fn session_clock(&self, _gate: &QuinnLegCompositionGate) -> SessionMonotonicClock {
        self.session_clock.clone()
    }

    pub(super) fn capacity_witness(
        &self,
        _gate: &QuinnLegCompositionGate,
    ) -> QuinnLegActorCapacityWitness {
        self.capacity
    }

    #[cfg(test)]
    fn session_clock_for_test(&self) -> SessionMonotonicClock {
        self.session_clock.clone()
    }

    pub(super) fn physical_incarnation(
        &self,
        _gate: &QuinnLegCompositionGate,
    ) -> Option<&PhysicalLegRuntimeIncarnation> {
        self.actor_incarnation.as_ref()
    }

    #[cfg(test)]
    pub(super) fn arm_join_block_for_test(&self) -> QuinnLegActorJoinBlock {
        self.child_publication_gate.arm();
        QuinnLegActorJoinBlock {
            gate: Arc::clone(&self.child_publication_gate),
        }
    }

    #[cfg(test)]
    pub(super) fn arm_data_writer_block_for_test(&self) -> QuinnLegActorDataWriterBlock {
        self.data_writer_gate.arm();
        QuinnLegActorDataWriterBlock {
            gate: Arc::clone(&self.data_writer_gate),
        }
    }

    #[cfg(test)]
    pub(super) fn arm_post_terminal_cleanup_block_for_test(
        &self,
    ) -> QuinnLegActorPostTerminalCleanupBlock {
        self.post_terminal_cleanup_gate.arm();
        QuinnLegActorPostTerminalCleanupBlock {
            gate: Arc::clone(&self.post_terminal_cleanup_gate),
        }
    }

    #[cfg(test)]
    pub(super) fn arm_inbound_publication_block_for_test(
        &self,
    ) -> QuinnLegActorInboundPublicationBlock {
        self.inbound_publication_gate.arm();
        QuinnLegActorInboundPublicationBlock {
            gate: Arc::clone(&self.inbound_publication_gate),
        }
    }

    /// Freezes one semantic timer's current inbound publication high-water.
    /// The composition calls this on the first due observation before any
    /// policy early-return, so later traffic cannot repeatedly postpone the
    /// same absolute timer.
    pub(super) fn capture_semantic_timer_inbound_cutoff<I>(&self, timer: &mut I)
    where
        I: QuinnLegActorSemanticTimer,
    {
        let high_water = self
            .inbound_publications
            .capture_high_water(&self.readiness);
        let _cutoff = timer.capture_inbound_publication_cutoff(high_water);
    }

    /// Moves the root-reported exact terminal through one composition-owned
    /// synchronous transaction. A rejected transaction must return the same
    /// terminal so this actor can restore the sole handoff slot.
    pub(super) fn try_transact_reported_terminal<T, E>(
        &mut self,
        _gate: &QuinnLegCompositionGate,
        transaction: impl FnOnce(
            &PhysicalLegRuntimeIncarnation,
            QuinnLegActorReportedTerminal,
        ) -> Result<T, (QuinnLegActorReportedTerminal, E)>,
    ) -> Result<QuinnLegActorReportedTerminalPoll<T, E>, QuinnLegActorError> {
        self.reported_terminal.refresh()?;
        if matches!(
            self.reported_terminal,
            ActorReportedTerminalState::Awaiting(_)
        ) {
            let fatal_observed = self.first_failure.current().is_some();
            if fatal_observed {
                self.request_cancel();
            }
            return Ok(QuinnLegActorReportedTerminalPoll::Pending);
        }
        if matches!(self.reported_terminal, ActorReportedTerminalState::Consumed) {
            return Ok(QuinnLegActorReportedTerminalPoll::AlreadyConsumed);
        }
        let runtime = self
            .actor_incarnation
            .as_ref()
            .ok_or(QuinnLegActorError::MissingPhysicalIncarnation)?;
        let state = std::mem::replace(
            &mut self.reported_terminal,
            ActorReportedTerminalState::Consumed,
        );
        let ActorReportedTerminalState::Ready(terminal) = state else {
            self.reported_terminal = state;
            return Err(QuinnLegActorError::TerminalHandoffClosed);
        };
        match transaction(runtime, terminal) {
            Ok(value) => Ok(QuinnLegActorReportedTerminalPoll::Applied(value)),
            Err((terminal, error)) => {
                self.reported_terminal = ActorReportedTerminalState::Ready(terminal);
                Ok(QuinnLegActorReportedTerminalPoll::Rejected(error))
            }
        }
    }

    /// Post-fatal terminal transaction for a closed physical composition.
    /// Unlike normal controller access this deliberately remains available
    /// after first failure, but exposes parts only after revalidating the
    /// immutable runtime -> LegIo -> sole FIFO identity chain. Rejection
    /// restores the complete reported-terminal envelope.
    pub(super) fn try_transact_reported_terminal_with_parts<T, E>(
        &mut self,
        gate: &QuinnLegCompositionGate,
        transaction: impl FnOnce(
            &PhysicalLegRuntimeIncarnation,
            &LegIo,
            &mut LegOutboundQueue,
            QuinnLegActorReportedTerminal,
        ) -> Result<T, (QuinnLegActorReportedTerminal, E)>,
    ) -> Result<QuinnLegActorReportedTerminalPoll<T, E>, QuinnLegActorError> {
        self.reported_terminal.refresh()?;
        if matches!(
            self.reported_terminal,
            ActorReportedTerminalState::Awaiting(_)
        ) {
            let fatal_observed = self.first_failure.current().is_some();
            if fatal_observed {
                self.request_cancel();
            }
            return Ok(QuinnLegActorReportedTerminalPoll::Pending);
        }
        if matches!(self.reported_terminal, ActorReportedTerminalState::Consumed) {
            return Ok(QuinnLegActorReportedTerminalPoll::AlreadyConsumed);
        }
        let runtime = self
            .actor_incarnation
            .as_ref()
            .ok_or(QuinnLegActorError::MissingPhysicalIncarnation)?;
        let io = self.io.as_ref().ok_or(QuinnLegActorError::AlreadyJoined)?;
        let queue = self
            .queue
            .as_mut()
            .ok_or(QuinnLegActorError::AlreadyJoined)?;
        if !io.belongs_to_physical_actor(runtime, gate)
            || !queue.belongs_to_established_leg(io.established_leg())
        {
            return Err(QuinnLegActorError::MissingPhysicalIncarnation);
        }
        let state = std::mem::replace(
            &mut self.reported_terminal,
            ActorReportedTerminalState::Consumed,
        );
        let ActorReportedTerminalState::Ready(terminal) = state else {
            self.reported_terminal = state;
            return Err(QuinnLegActorError::TerminalHandoffClosed);
        };
        match transaction(runtime, io, queue, terminal) {
            Ok(value) => Ok(QuinnLegActorReportedTerminalPoll::Applied(value)),
            Err((terminal, error)) => {
                self.reported_terminal = ActorReportedTerminalState::Ready(terminal);
                Ok(QuinnLegActorReportedTerminalPoll::Rejected(error))
            }
        }
    }

    /// Linearizes one composition-owned controller policy commit against the
    /// actor's exact first fatal. No policy mutation may begin after fatal
    /// publication, while a fatal producer cannot publish midway through the
    /// synchronous commit.
    pub(super) fn transact_controller_policy<T>(
        &mut self,
        _gate: &QuinnLegCompositionGate,
        transaction: impl FnOnce(&PhysicalLegRuntimeIncarnation) -> T,
    ) -> Result<T, QuinnLegActorError> {
        self.transact_controller_policy_inner(transaction)
    }

    /// Commits the composition's irreversible local-shutdown barrier and the
    /// actor's cooperative Closing publication at one first-failure
    /// linearization point. A rejected composition transaction leaves the
    /// actor Running; a successful one closes actor admission before a child
    /// can publish a later transport terminal.
    pub(super) fn transact_controller_shutdown<T>(
        &mut self,
        _gate: &QuinnLegCompositionGate,
        transaction: impl FnOnce(&PhysicalLegRuntimeIncarnation) -> Option<T>,
    ) -> Result<Option<T>, QuinnLegActorError> {
        self.transact_controller_shutdown_inner(transaction)
    }

    /// Lets one exact, controller-owned probe deadline compete at the same
    /// first-failure linearization point as peer close/reset/fatal I/O. Only a
    /// successful controller transaction publishes the semantic failure; the
    /// actor is cooperatively cancelled after the lock is released.
    #[cfg(test)]
    pub(super) fn commit_current_probe_deadline<I, T>(
        &mut self,
        _gate: &QuinnLegCompositionGate,
        input: I,
        transaction: impl FnOnce(&PhysicalLegRuntimeIncarnation, I) -> Result<T, I>,
    ) -> QuinnLegActorProbeDeadlineCommit<T, I> {
        let first_failure = Arc::clone(&self.first_failure);
        let phase = self.phase;
        let runtime = self.actor_incarnation.as_ref();
        let committed = first_failure.commit_failure_with_input(
            input,
            QuinnLegActorError::ProbeDeadlineExpired,
            |input| {
                if phase != ActorOwnerPhase::Running {
                    return Err(input);
                }
                let Some(runtime) = runtime else {
                    return Err(input);
                };
                transaction(runtime, input)
            },
        );
        match committed {
            Ok(Ok(value)) => {
                self.request_cancel();
                QuinnLegActorProbeDeadlineCommit::Committed(value)
            }
            Ok(Err(input)) => QuinnLegActorProbeDeadlineCommit::Rejected(input),
            Err((input, ActorFirstBarrier::Failed(cause))) => {
                self.request_cancel();
                QuinnLegActorProbeDeadlineCommit::Suppressed { input, cause }
            }
            Err((input, ActorFirstBarrier::LocalShutdownWon)) => {
                QuinnLegActorProbeDeadlineCommit::Rejected(input)
            }
        }
    }

    /// Input-preserving policy transaction for typed local source facts. If
    /// actor failure or close wins before the controller commit starts, the
    /// exact unconsumed input is returned instead of being dropped.
    pub(super) fn transact_controller_policy_with_input<I, T>(
        &mut self,
        _gate: &QuinnLegCompositionGate,
        input: I,
        transaction: impl FnOnce(&PhysicalLegRuntimeIncarnation, I) -> T,
    ) -> Result<T, I> {
        self.transact_controller_policy_with_input_inner(input, transaction)
    }

    /// Detailed input-preserving form used when a caller must distinguish an
    /// actor failure/local close from its own synchronous policy rejection.
    pub(super) fn transact_controller_policy_with_input_result<I, T>(
        &mut self,
        _gate: &QuinnLegCompositionGate,
        input: I,
        transaction: impl FnOnce(&PhysicalLegRuntimeIncarnation, I) -> T,
    ) -> Result<T, (I, QuinnLegActorError)> {
        self.transact_controller_policy_with_input_result_inner(input, transaction)
    }

    fn transact_controller_policy_with_input_inner<I, T>(
        &mut self,
        input: I,
        transaction: impl FnOnce(&PhysicalLegRuntimeIncarnation, I) -> T,
    ) -> Result<T, I> {
        self.transact_controller_policy_with_input_result_inner(input, transaction)
            .map_err(|(input, _error)| input)
    }

    fn transact_controller_policy_with_input_result_inner<I, T>(
        &mut self,
        input: I,
        transaction: impl FnOnce(&PhysicalLegRuntimeIncarnation, I) -> T,
    ) -> Result<T, (I, QuinnLegActorError)> {
        if let Some(error) = self.observe_mutating_port_error() {
            return Err((input, error));
        }
        let first_failure = Arc::clone(&self.first_failure);
        match first_failure.run_with_input_if_clear(input, |input| match self.phase {
            ActorOwnerPhase::Running => {
                let Some(runtime) = self.actor_incarnation.as_ref() else {
                    return Err((input, QuinnLegActorError::MissingPhysicalIncarnation));
                };
                Ok(transaction(runtime, input))
            }
            ActorOwnerPhase::Closing => Err((input, QuinnLegActorError::Closing)),
            ActorOwnerPhase::Joined => Err((input, QuinnLegActorError::AlreadyJoined)),
        }) {
            Ok(result) => result,
            Err((input, barrier)) => {
                self.request_cancel();
                Err((input, barrier.into_actor_error()))
            }
        }
    }

    fn transact_controller_policy_inner<T>(
        &mut self,
        transaction: impl FnOnce(&PhysicalLegRuntimeIncarnation) -> T,
    ) -> Result<T, QuinnLegActorError> {
        if let Some(error) = self.observe_mutating_port_error() {
            return Err(error);
        }
        let first_failure = Arc::clone(&self.first_failure);
        match first_failure.run_if_clear(|| match self.phase {
            ActorOwnerPhase::Running => {
                let runtime = self
                    .actor_incarnation
                    .as_ref()
                    .ok_or(QuinnLegActorError::MissingPhysicalIncarnation)?;
                Ok(transaction(runtime))
            }
            ActorOwnerPhase::Closing => Err(QuinnLegActorError::Closing),
            ActorOwnerPhase::Joined => Err(QuinnLegActorError::AlreadyJoined),
        }) {
            Ok(result) => result,
            Err(barrier) => {
                self.request_cancel();
                Err(barrier.into_actor_error())
            }
        }
    }

    fn transact_controller_shutdown_inner<T>(
        &mut self,
        transaction: impl FnOnce(&PhysicalLegRuntimeIncarnation) -> Option<T>,
    ) -> Result<Option<T>, QuinnLegActorError> {
        let first_failure = Arc::clone(&self.first_failure);
        let mut outcome = first_failure
            .outcome
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &*outcome {
            ActorFirstOutcome::Failed(cause, _) => {
                let cause = Arc::clone(cause);
                drop(outcome);
                self.request_cancel();
                return Err(QuinnLegActorError::RootFailed(cause));
            }
            ActorFirstOutcome::LocalShutdownWon => {
                return Err(QuinnLegActorError::Closing);
            }
            ActorFirstOutcome::Open => {}
        }
        let value = match self.phase {
            ActorOwnerPhase::Running => {
                let runtime = self
                    .actor_incarnation
                    .as_ref()
                    .ok_or(QuinnLegActorError::MissingPhysicalIncarnation)?;
                transaction(runtime)
            }
            ActorOwnerPhase::Closing => return Err(QuinnLegActorError::Closing),
            ActorOwnerPhase::Joined => return Err(QuinnLegActorError::AlreadyJoined),
        };
        if value.is_some() {
            *outcome = ActorFirstOutcome::LocalShutdownWon;
            // The marker and actor Closing/cancel publication are one
            // critical section. Every later publisher observes the marker.
            self.request_cancel_after_arbitration();
        }
        Ok(value)
    }

    /// Runs one composition-owned transfer of this actor's exact policy-side
    /// endpoint and sole FIFO. A rejected controller transaction must return
    /// both pieces; they are restored before the error escapes.
    pub(super) fn transact_controller_parts<I, T, E>(
        &mut self,
        gate: &QuinnLegCompositionGate,
        input: I,
        transaction: impl FnOnce(
            I,
            LegIo,
            LegOutboundQueue,
        ) -> Result<T, Box<(I, LegIo, LegOutboundQueue, E)>>,
    ) -> Result<T, (I, Option<E>)> {
        let first_failure = Arc::clone(&self.first_failure);
        match first_failure.run_with_input_if_clear(input, |input| {
            if self.phase != ActorOwnerPhase::Running {
                return Err((input, None));
            }
            let Some(runtime) = self.actor_incarnation.as_ref() else {
                return Err((input, None));
            };
            let (Some(io), Some(queue)) = (self.io.as_ref(), self.queue.as_ref()) else {
                return Err((input, None));
            };
            if !io.belongs_to_physical_actor(runtime, gate)
                || !queue.belongs_to_established_leg(io.established_leg())
            {
                return Err((input, None));
            }
            let Some(io) = self.io.take() else {
                return Err((input, None));
            };
            let Some(queue) = self.queue.take() else {
                self.io = Some(io);
                return Err((input, None));
            };
            match transaction(input, io, queue) {
                Ok(value) => Ok(value),
                Err(rollback) => {
                    let (input, io, queue, error) = *rollback;
                    self.io = Some(io);
                    self.queue = Some(queue);
                    Err((input, Some(error)))
                }
            }
        }) {
            Ok(Ok(value)) => {
                self.controller_parts_transferred = true;
                Ok(value)
            }
            Ok(Err(error)) => Err(error),
            Err((input, _cause)) => {
                self.request_cancel();
                Err((input, None))
            }
        }
    }

    /// Lets only the composition root operate on this actor's still-owned
    /// exact policy port without extracting either endpoint or FIFO.
    pub(super) fn with_controller_parts_mut<T>(
        &mut self,
        gate: &QuinnLegCompositionGate,
        operation: impl FnOnce(&LegIo, &mut LegOutboundQueue) -> T,
    ) -> Option<T> {
        let first_failure = Arc::clone(&self.first_failure);
        match first_failure.run_value_if_clear(|| {
            if self.phase != ActorOwnerPhase::Running {
                return None;
            }
            let runtime = self.actor_incarnation.as_ref()?;
            let io = self.io.as_ref()?;
            let queue = self.queue.as_mut()?;
            if !io.belongs_to_physical_actor(runtime, gate)
                || !queue.belongs_to_established_leg(io.established_leg())
            {
                return None;
            }
            Some(operation(io, queue))
        }) {
            Ok(result) => result,
            Err(_cause) => {
                self.request_cancel();
                None
            }
        }
    }

    /// Result-carrying counterpart for a composition-owned controller turn.
    /// The physical endpoint/FIFO borrow and the callback's failure result are
    /// covered by one first-failure critical section. A callback error becomes
    /// the actor's canonical cause before either the borrow or that lock can
    /// escape to the composition caller.
    pub(super) fn with_controller_parts_mut_result<T>(
        &mut self,
        gate: &QuinnLegCompositionGate,
        operation: impl FnOnce(&LegIo, &mut LegOutboundQueue) -> Result<T, QuinnLegActorError>,
    ) -> Result<T, QuinnLegActorError> {
        if let Some(error) = self.observe_mutating_port_error() {
            return Err(error);
        }
        let first_failure = Arc::clone(&self.first_failure);
        let committed = first_failure.commit(|| {
            if self.phase != ActorOwnerPhase::Running {
                return Err(QuinnLegActorError::Closing);
            }
            let runtime = self
                .actor_incarnation
                .as_ref()
                .ok_or(QuinnLegActorError::MissingPhysicalIncarnation)?;
            let io = self.io.as_ref().ok_or(QuinnLegActorError::AlreadyJoined)?;
            let queue = self
                .queue
                .as_mut()
                .ok_or(QuinnLegActorError::AlreadyJoined)?;
            if !io.belongs_to_physical_actor(runtime, gate)
                || !queue.belongs_to_established_leg(io.established_leg())
            {
                return Err(QuinnLegActorError::MissingPhysicalIncarnation);
            }
            operation(io, queue)
        });
        match committed {
            Ok(value) => Ok(value),
            Err(barrier) => {
                self.request_cancel();
                Err(barrier.into_actor_error())
            }
        }
    }

    /// Borrow-only cold inventory used before a physical root commits local
    /// cancellation. A non-zero result leaves the actor Running so the same
    /// FIFO can still be flushed or explicitly settled before join starts.
    pub(super) fn shutdown_outbound_inventory(
        &self,
        gate: &QuinnLegCompositionGate,
    ) -> Result<(usize, usize), QuinnLegActorError> {
        if self.phase == ActorOwnerPhase::Joined {
            return Err(QuinnLegActorError::AlreadyJoined);
        }
        let runtime = self
            .actor_incarnation
            .as_ref()
            .ok_or(QuinnLegActorError::MissingPhysicalIncarnation)?;
        if self.controller_parts_transferred {
            return if self.io.is_none() && self.queue.is_none() {
                Ok((0, 0))
            } else {
                Err(QuinnLegActorError::Protocol(
                    "actor controller-parts transfer state is inconsistent",
                ))
            };
        }
        let io = self.io.as_ref().ok_or(QuinnLegActorError::AlreadyJoined)?;
        let queue = self
            .queue
            .as_ref()
            .ok_or(QuinnLegActorError::AlreadyJoined)?;
        if !io.belongs_to_physical_actor(runtime, gate)
            || !queue.belongs_to_established_leg(io.established_leg())
        {
            return Err(QuinnLegActorError::MissingPhysicalIncarnation);
        }
        let inventory = queue.cold_inventory().ok_or(QuinnLegActorError::Protocol(
            "actor outbound FIFO cold inventory is inconsistent",
        ))?;
        let messages = inventory
            .queued_frames()
            .checked_add(usize::from(inventory.ordered_delivery_wait()))
            .ok_or(QuinnLegActorError::Protocol(
                "actor outbound FIFO message inventory overflowed",
            ))?;
        Ok((messages, inventory.summed_frame_bytes()))
    }

    /// Composition-only shutdown transition for an already-delivered ordered
    /// wait. This remains usable after a fatal has put the actor in `Closing`,
    /// because it neither submits nor changes the actor's first-cause/phase.
    /// Pending delivery and every queued-frame residual are snapshot-inert.
    pub(super) fn try_retire_delivered_wait_for_shutdown(
        &mut self,
        gate: &QuinnLegCompositionGate,
    ) -> Result<QuinnLegActorShutdownWaitDrive, QuinnLegActorError> {
        if self.phase == ActorOwnerPhase::Joined {
            return Err(QuinnLegActorError::AlreadyJoined);
        }
        let runtime = self
            .actor_incarnation
            .as_ref()
            .ok_or(QuinnLegActorError::MissingPhysicalIncarnation)?;
        if self.controller_parts_transferred {
            return if self.io.is_none() && self.queue.is_none() {
                Ok(QuinnLegActorShutdownWaitDrive::Clean)
            } else {
                Err(QuinnLegActorError::Protocol(
                    "actor controller-parts transfer state is inconsistent",
                ))
            };
        }
        let io = self.io.as_ref().ok_or(QuinnLegActorError::AlreadyJoined)?;
        let queue = self
            .queue
            .as_mut()
            .ok_or(QuinnLegActorError::AlreadyJoined)?;
        if !io.belongs_to_physical_actor(runtime, gate)
            || !queue.belongs_to_established_leg(io.established_leg())
        {
            return Err(QuinnLegActorError::MissingPhysicalIncarnation);
        }
        let retired = queue
            .try_retire_delivered_wait_for_shutdown(io.established_leg())
            .ok_or(QuinnLegActorError::Protocol(
                "actor shutdown wait retirement inventory is inconsistent",
            ))?;
        Ok(match retired {
            LegOutboundShutdownWaitRetirement::Clean => QuinnLegActorShutdownWaitDrive::Clean,
            LegOutboundShutdownWaitRetirement::RetiredDelivered => {
                QuinnLegActorShutdownWaitDrive::RetiredDelivered
            }
            LegOutboundShutdownWaitRetirement::Pending => QuinnLegActorShutdownWaitDrive::Pending,
            LegOutboundShutdownWaitRetirement::Residual {
                queued_frames,
                queued_bytes,
                ordered_delivery_wait,
            } => QuinnLegActorShutdownWaitDrive::Residual {
                queued_frames,
                queued_bytes,
                ordered_delivery_wait,
            },
        })
    }

    /// Failure/close before endpoint/FIFO borrow returns the original input and cause.
    pub(super) fn with_controller_parts_mut_input<I, T>(
        &mut self,
        gate: &QuinnLegCompositionGate,
        input: I,
        operation: impl FnOnce(I, &LegIo, &mut LegOutboundQueue) -> T,
    ) -> Result<T, (I, QuinnLegActorError)> {
        self.with_physical_controller_parts_mut_input(gate, input, |input, _runtime, io, queue| {
            operation(input, io, queue)
        })
    }

    /// Borrows original runtime/LegIo/FIFO under one first-failure transaction;
    /// a non-cloneable handoff cannot cross a split lifecycle check.
    pub(super) fn with_physical_controller_parts_mut_input<I, T>(
        &mut self,
        gate: &QuinnLegCompositionGate,
        input: I,
        operation: impl FnOnce(I, &PhysicalLegRuntimeIncarnation, &LegIo, &mut LegOutboundQueue) -> T,
    ) -> Result<T, (I, QuinnLegActorError)> {
        let first_failure = Arc::clone(&self.first_failure);
        match first_failure.run_with_input_if_clear(input, |input| {
            if self.phase != ActorOwnerPhase::Running {
                return Err((input, QuinnLegActorError::Closing));
            }
            let Some(runtime) = self.actor_incarnation.as_ref() else {
                return Err((input, QuinnLegActorError::MissingPhysicalIncarnation));
            };
            let (Some(io), Some(queue)) = (self.io.as_ref(), self.queue.as_mut()) else {
                return Err((input, QuinnLegActorError::AlreadyJoined));
            };
            if !io.belongs_to_physical_actor(runtime, gate)
                || !queue.belongs_to_established_leg(io.established_leg())
            {
                return Err((input, QuinnLegActorError::MissingPhysicalIncarnation));
            }
            Ok(operation(input, runtime, io, queue))
        }) {
            Ok(result) => result,
            Err((input, barrier)) => {
                self.request_cancel();
                Err((input, barrier.into_actor_error()))
            }
        }
    }

    /// Borrows only the fixed actor sender behind the composition gate. The
    /// controller retains sole `LegIo/FIFO` ownership and therefore remains
    /// the only source of encoded submissions.
    pub(super) fn with_encoded_transport_mut<T>(
        &mut self,
        _gate: &QuinnLegCompositionGate,
        operation: impl FnOnce(&mut dyn EncodedLegTransport) -> T,
    ) -> Option<T> {
        let first_failure = Arc::clone(&self.first_failure);
        match first_failure.run_value_if_clear(|| {
            if self.phase != ActorOwnerPhase::Running || self.actor_incarnation.is_none() {
                return None;
            }
            Some(operation(self.transport.as_mut()?))
        }) {
            Ok(result) => result,
            Err(_cause) => {
                self.request_cancel();
                None
            }
        }
    }

    #[cfg(test)]
    fn fail_root_after_children_for_test(&self) {
        self.root_outer_error.store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn panic_data_writer_on_next_record_for_test(&self) {
        self.panic_data_writer.store(true, Ordering::Release);
    }

    #[allow(
        clippy::result_large_err,
        reason = "enqueue rejection retains the exact typed control frame"
    )]
    pub(super) fn enqueue_ordered_leg_control(
        &mut self,
        frame: LegControlFrame,
        features: FeatureSet,
    ) -> Result<LegControlEnqueued, QuinnLegActorEnqueueError> {
        if self.phase == ActorOwnerPhase::Joined {
            return Err(QuinnLegActorEnqueueError::AlreadyJoined { frame });
        }
        if let Some(cause) = self.first_failure.current() {
            self.request_cancel();
            return Err(QuinnLegActorEnqueueError::RootFailed { frame, cause });
        }
        if let Some(error) = self.observed_root_port_error() {
            self.request_cancel();
            let cause = match error {
                QuinnLegActorError::RootFailed(cause) => cause,
                error => Arc::new(error),
            };
            return Err(QuinnLegActorEnqueueError::RootFailed { frame, cause });
        }
        if self.phase == ActorOwnerPhase::Closing {
            return Err(QuinnLegActorEnqueueError::Closing { frame });
        }
        #[cfg(test)]
        self.port_linearization_gate.pause_if_armed();
        let first_failure = Arc::clone(&self.first_failure);
        let enqueue_result = {
            let (Some(io), Some(queue)) = (self.io.as_ref(), self.queue.as_mut()) else {
                return Err(QuinnLegActorEnqueueError::AlreadyJoined { frame });
            };
            let seal = io.established_leg().standby_seal();
            first_failure.run_with_input_if_clear(frame, |frame| {
                queue.push_leg_control(&seal, frame, features)
            })
        };
        match enqueue_result {
            Err((frame, ActorFirstBarrier::Failed(cause))) => {
                self.request_cancel();
                Err(QuinnLegActorEnqueueError::RootFailed { frame, cause })
            }
            Err((frame, ActorFirstBarrier::LocalShutdownWon)) => {
                Err(QuinnLegActorEnqueueError::Closing { frame })
            }
            Ok(Ok(receipt)) => Ok(receipt),
            Ok(Err(error)) => Err(QuinnLegActorEnqueueError::Queue(error)),
        }
    }

    pub(crate) fn actor_snapshot(&self) -> QuinnLegActorSnapshot {
        #[cfg(test)]
        let (pending_delivery_count, pending_delivery_ordinal) = {
            let pending = self
                .pending_deliveries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                pending.len(),
                pending.first_key_value().map(|(ordinal, _)| *ordinal),
            )
        };
        QuinnLegActorSnapshot {
            outbound_queue_messages: self.queue.as_ref().map_or(0, LegOutboundQueue::len),
            outbound_queue_bytes: self.queue.as_ref().map_or(0, LegOutboundQueue::owned_bytes),
            validated_data_headers: self.counters.validated_data_headers.load(Ordering::Acquire),
            queued_inbound_messages: self
                .counters
                .queued_inbound_messages
                .load(Ordering::Acquire),
            inbound_owned_bytes: self.counters.inbound_owned_bytes.load(Ordering::Acquire),
            waiting_inbound_bytes: self.counters.waiting_inbound_bytes.load(Ordering::Acquire),
            reserved_delivery_acks: self.counters.reserved_delivery_acks.load(Ordering::Acquire),
            ack_owned_bytes: self.counters.ack_owned_bytes.load(Ordering::Acquire),
            composition_owned_messages: self
                .counters
                .composition_owned_messages
                .load(Ordering::Acquire),
            outbound_owned_messages: self
                .counters
                .outbound_owned_messages
                .load(Ordering::Acquire),
            outbound_owned_bytes: self.counters.outbound_owned_bytes.load(Ordering::Acquire),
            #[cfg(test)]
            blocked_write: self.write_poll_probe.last_pending(),
            #[cfg(test)]
            pending_delivery_count,
            #[cfg(test)]
            pending_delivery_ordinal,
        }
    }

    #[cfg(test)]
    pub(super) fn outbound_queue_cold_observation_for_test(
        &self,
    ) -> Option<QuinnLegActorOutboundQueueColdObservation> {
        let inventory = self.queue.as_ref()?.cold_inventory()?;
        Some(QuinnLegActorOutboundQueueColdObservation {
            phase: inventory.phase_label_for_test(),
            queued_frames: inventory.queued_frames(),
            summed_frame_bytes: inventory.summed_frame_bytes(),
            stored_owned_bytes: inventory.stored_owned_bytes(),
            ordered_delivery_wait: inventory.ordered_delivery_wait(),
        })
    }

    #[cfg(test)]
    pub(super) fn diagnostic_observation_for_test(&self) -> QuinnLegActorDiagnosticObservation {
        QuinnLegActorDiagnosticObservation {
            snapshot: self.actor_snapshot(),
            outbound_queue: self.outbound_queue_cold_observation_for_test(),
            observed_error: self.observe_port_error(),
        }
    }

    pub(crate) fn inbound_owned_bytes(&self) -> usize {
        self.counters.inbound_owned_bytes.load(Ordering::Acquire)
    }

    pub(crate) fn readiness_epoch(&self) -> u64 {
        self.readiness.current()
    }

    pub(super) fn owner_progress_cursor(&self) -> QuinnLegActorOwnerProgressCursor {
        QuinnLegActorOwnerProgressCursor {
            readiness: self.readiness.clone(),
            epoch: self.readiness.current(),
        }
    }

    pub(super) fn matches_owner_progress_cursor(
        &self,
        cursor: &QuinnLegActorOwnerProgressCursor,
    ) -> bool {
        self.readiness.same_source(&cursor.readiness)
    }

    fn observe_port_error(&self) -> Option<QuinnLegActorError> {
        if self.phase == ActorOwnerPhase::Joined {
            return Some(QuinnLegActorError::AlreadyJoined);
        }
        if let Some(cause) = self.first_failure.current() {
            return Some(QuinnLegActorError::RootFailed(cause));
        }
        if let Some(error) = self.observed_root_port_error() {
            return Some(error);
        }
        if self.phase == ActorOwnerPhase::Closing {
            Some(QuinnLegActorError::Closing)
        } else {
            None
        }
    }

    fn observe_mutating_port_error(&mut self) -> Option<QuinnLegActorError> {
        if self.phase == ActorOwnerPhase::Joined {
            return Some(QuinnLegActorError::AlreadyJoined);
        }
        if let Some(cause) = self.first_failure.current() {
            self.request_cancel();
            return Some(QuinnLegActorError::RootFailed(cause));
        }
        if let Some(error) = self.observed_root_port_error() {
            self.request_cancel();
            return Some(error);
        }
        if self.phase == ActorOwnerPhase::Closing {
            Some(QuinnLegActorError::Closing)
        } else {
            None
        }
    }

    fn observed_root_port_error(&self) -> Option<QuinnLegActorError> {
        match self.observed_root.as_ref() {
            Some(ObservedRootOutcome::ActorError(error)) => {
                Some(QuinnLegActorError::RootFailed(Arc::clone(error)))
            }
            Some(ObservedRootOutcome::JoinError(error)) => {
                Some(QuinnLegActorError::TaskJoin(error.to_string()))
            }
            Some(ObservedRootOutcome::Completed(_)) | None => None,
        }
    }

    pub(crate) async fn wait_for_progress(&self, previous: u64) -> Result<u64, QuinnLegActorError> {
        if let Some(error) = self.observe_port_error() {
            return Err(error);
        }
        #[cfg(test)]
        self.progress_registration_gate.pause_if_armed().await;
        let cancel = self.cancel.subscribe();
        if let Some(error) = self.observe_port_error() {
            return Err(error);
        }
        match self
            .readiness
            .wait_after_or_cancel(previous, cancel)
            .await?
        {
            Some(current) => Ok(current),
            None => Err(self
                .observe_port_error()
                .unwrap_or(QuinnLegActorError::Closing)),
        }
    }

    /// Composition-owned wait that remains pending across first-failure and
    /// Closing until the actor root publishes terminal/lifecycle progress.
    /// Unlike the public port wait, this cannot spin on an already-observed
    /// fatal while the structured child cleanup tail is still running.
    pub(super) async fn wait_for_owner_progress(
        &self,
        cursor: QuinnLegActorOwnerProgressCursor,
    ) -> Result<u64, QuinnLegActorError> {
        debug_assert!(self.matches_owner_progress_cursor(&cursor));
        self.readiness.wait_after(cursor.epoch).await
    }

    fn fatal_or_cancelled_receive(
        &mut self,
    ) -> Result<Option<QuinnLegActorInbound>, QuinnLegActorError> {
        match self.observe_mutating_port_error() {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }

    /// Consumes at most one complete raw Quinn record through a synchronous
    /// composition-owned controller transaction. No raw record, byte permit,
    /// or delivery reservation escapes this call. A defensive policy block
    /// restores the exact record in the actor before the first-failure lock is
    /// released.
    pub(super) fn try_transact_controller_inbound<T>(
        &mut self,
        _composition_gate: &QuinnLegCompositionGate,
        operation: impl FnOnce(
            QuinnLegActorEncodedInbound,
        ) -> Result<QuinnLegActorInboundDecision<T>, QuinnLegActorError>,
    ) -> Result<QuinnLegActorInboundPoll<T>, QuinnLegActorError> {
        if let Some(error) = self.observe_mutating_port_error() {
            return Err(error);
        }
        let first_failure = Arc::clone(&self.first_failure);
        let inbound = &mut self.inbound;
        let pending = &mut self.pending_controller_inbound;
        let inbound_gate = &self.inbound_gate;
        let committed = first_failure.commit(|| {
            let mut record = if let Some(record) = pending.take() {
                record
            } else {
                let receiver = inbound.as_mut().ok_or(QuinnLegActorError::AlreadyJoined)?;
                match receiver.try_recv() {
                    Ok(record) => ControllerInboundRecord::from_actor_record(record, inbound_gate)?,
                    Err(mpsc::error::TryRecvError::Empty) => {
                        return Ok(QuinnLegActorInboundPoll::Idle);
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        return Err(QuinnLegActorError::Protocol(
                            "actor inbound receiver closed before root outcome",
                        ));
                    }
                }
            };
            let envelope = record.take_envelope()?;
            match operation(envelope)? {
                QuinnLegActorInboundDecision::Classified(value) => {
                    record.commit_classified();
                    Ok(QuinnLegActorInboundPoll::Classified(value))
                }
                QuinnLegActorInboundDecision::Blocked(envelope) => {
                    record.restore_envelope(envelope)?;
                    if pending.replace(record).is_some() {
                        return Err(QuinnLegActorError::Protocol(
                            "controller inbound pending slot already occupied",
                        ));
                    }
                    Ok(QuinnLegActorInboundPoll::Blocked)
                }
            }
        });
        match committed {
            Ok(outcome) => Ok(outcome),
            Err(barrier) => {
                self.request_cancel();
                Err(barrier.into_actor_error())
            }
        }
    }

    /// Two-phase physical-owner transaction. The immutable preflight is the
    /// only phase allowed to return `Blocked`; in that branch the raw record
    /// never leaves `ControllerInboundRecord`. Mutable LegIo/FIFO authority is
    /// exposed only after `Ready`, and that commit must either classify the
    /// record or fail fatally. A fatal error consumes the record, installs the
    /// actor's first cause under this same lock, and starts structured cancel.
    pub(super) fn try_transact_controller_inbound_with_parts<C, P, T>(
        &mut self,
        composition_gate: &QuinnLegCompositionGate,
        context: &mut C,
        preflight: impl FnOnce(
            &C,
            &LegIo,
            &LegOutboundQueue,
        ) -> Result<QuinnLegActorInboundPreflight<P>, QuinnLegActorError>,
        commit: impl FnOnce(
            &mut C,
            P,
            QuinnLegActorEncodedInbound,
            &mut LegIo,
            &mut LegOutboundQueue,
        ) -> Result<T, QuinnLegActorError>,
    ) -> Result<QuinnLegActorInboundPoll<T>, QuinnLegActorError> {
        if let Some(error) = self.observe_mutating_port_error() {
            return Err(error);
        }
        let first_failure = Arc::clone(&self.first_failure);
        let inbound = &mut self.inbound;
        let pending = &mut self.pending_controller_inbound;
        let inbound_gate = &self.inbound_gate;
        let phase = self.phase;
        let runtime = self.actor_incarnation.as_ref();
        let io = &mut self.io;
        let queue = &mut self.queue;
        let committed = first_failure.commit(|| {
            if phase != ActorOwnerPhase::Running {
                return Err(QuinnLegActorError::Closing);
            }
            let runtime = runtime.ok_or(QuinnLegActorError::MissingPhysicalIncarnation)?;
            let io = io.as_mut().ok_or(QuinnLegActorError::AlreadyJoined)?;
            let queue = queue.as_mut().ok_or(QuinnLegActorError::AlreadyJoined)?;
            if !io.belongs_to_physical_actor(runtime, composition_gate)
                || !queue.belongs_to_established_leg(io.established_leg())
            {
                return Err(QuinnLegActorError::MissingPhysicalIncarnation);
            }
            let mut record = if let Some(record) = pending.take() {
                record
            } else {
                let receiver = inbound.as_mut().ok_or(QuinnLegActorError::AlreadyJoined)?;
                match receiver.try_recv() {
                    Ok(record) => ControllerInboundRecord::from_actor_record(record, inbound_gate)?,
                    Err(mpsc::error::TryRecvError::Empty) => {
                        return Ok(QuinnLegActorInboundPoll::Idle);
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        return Err(QuinnLegActorError::Protocol(
                            "actor inbound receiver closed before root outcome",
                        ));
                    }
                }
            };
            match preflight(&*context, io, queue)? {
                QuinnLegActorInboundPreflight::Ready(prepared) => {
                    let envelope = record.take_envelope()?;
                    let value = commit(context, prepared, envelope, io, queue)?;
                    record.commit_classified();
                    Ok(QuinnLegActorInboundPoll::Classified(value))
                }
                QuinnLegActorInboundPreflight::Blocked => {
                    if pending.replace(record).is_some() {
                        return Err(QuinnLegActorError::Protocol(
                            "controller inbound pending slot already occupied",
                        ));
                    }
                    Ok(QuinnLegActorInboundPoll::Blocked)
                }
            }
        });
        match committed {
            Ok(outcome) => Ok(outcome),
            Err(barrier) => {
                self.request_cancel();
                Err(barrier.into_actor_error())
            }
        }
    }

    /// Fatal probe specialization of the generic receive-before-timer gate.
    /// Its successful timer commit preserves the frozen R6b behavior: install
    /// `ProbeDeadlineExpired` as the exact actor cause and cancel the actor.
    pub(super) fn try_transact_inbound_before_probe_deadline<C, I, T, D>(
        &mut self,
        _composition_gate: &QuinnLegCompositionGate,
        deadline: I,
        context: &mut C,
        inbound_operation: impl FnOnce(
            &mut C,
            &PhysicalLegRuntimeIncarnation,
            QuinnLegActorEncodedInbound,
            I,
        ) -> Result<
            (QuinnLegActorInboundDecision<T>, I),
            (I, QuinnLegActorError),
        >,
        deadline_operation: impl FnOnce(&mut C, &PhysicalLegRuntimeIncarnation, I) -> Result<D, I>,
    ) -> QuinnLegActorInboundDeadlinePoll<I, T, D>
    where
        I: QuinnLegActorArrivalDeadline,
    {
        self.try_transact_inbound_before_timer(
            deadline,
            context,
            QuinnLegActorTimerCommitMode::Fatal(QuinnLegActorError::ProbeDeadlineExpired),
            |_, _, _| None,
            inbound_operation,
            |context, runtime, deadline| match deadline_operation(context, runtime, deadline) {
                Ok(value) => QuinnLegActorTimerDecision::Committed(value),
                Err(deadline) => QuinnLegActorTimerDecision::Rejected(deadline),
            },
        )
    }

    /// Non-fatal semantic-timer specialization. A successful commit consumes
    /// only the exact timer capability; it does not mutate actor first-failure
    /// state and does not request actor cancellation.
    pub(super) fn try_transact_inbound_before_semantic_timer<C, I, T, D, E>(
        &mut self,
        _composition_gate: &QuinnLegCompositionGate,
        timer: I,
        context: &mut C,
        inbound_operation: impl FnOnce(
            &mut C,
            &PhysicalLegRuntimeIncarnation,
            QuinnLegActorEncodedInbound,
            I,
        ) -> Result<
            (QuinnLegActorInboundDecision<T>, I),
            (I, QuinnLegActorError),
        >,
        timer_operation: impl FnOnce(
            &mut C,
            &PhysicalLegRuntimeIncarnation,
            I,
        ) -> QuinnLegActorTimerDecision<I, D, E>,
    ) -> QuinnLegActorInboundTimerPoll<I, T, D, E>
    where
        I: QuinnLegActorSemanticTimer + QuinnLegActorArrivalDeadline,
    {
        self.try_transact_inbound_before_timer(
            timer,
            context,
            QuinnLegActorTimerCommitMode::Semantic,
            |timer, readiness, publications| {
                let high_water = publications.capture_high_water(readiness);
                Some(timer.capture_inbound_publication_cutoff(high_water))
            },
            inbound_operation,
            timer_operation,
        )
    }

    /// Consumes at most one already-published current inbound before a timer.
    /// A complete producer reserves its monotonic publication epoch before
    /// entering the same `first_failure` gate. Fatal probe timers retain their
    /// original unbounded arbitration. Semantic timers admit only records whose
    /// immutable actor-captured arrival is at or before the absolute deadline.
    /// A qualifying in-flight publication returns the exact timer; records that
    /// arrived later remain actor-owned and cannot extend the deadline. Epochs
    /// are publication-ordering witnesses, not arrival timestamps or policy
    /// eligibility.
    fn try_transact_inbound_before_timer<C, I, T, D, E>(
        &mut self,
        timer: I,
        context: &mut C,
        mode: QuinnLegActorTimerCommitMode,
        cutoff_operation: impl FnOnce(&mut I, &Readiness, &InboundPublicationTracker) -> Option<u64>,
        inbound_operation: impl FnOnce(
            &mut C,
            &PhysicalLegRuntimeIncarnation,
            QuinnLegActorEncodedInbound,
            I,
        ) -> Result<
            (QuinnLegActorInboundDecision<T>, I),
            (I, QuinnLegActorError),
        >,
        timer_operation: impl FnOnce(
            &mut C,
            &PhysicalLegRuntimeIncarnation,
            I,
        ) -> QuinnLegActorTimerDecision<I, D, E>,
    ) -> QuinnLegActorInboundTimerPoll<I, T, D, E>
    where
        I: QuinnLegActorArrivalDeadline,
    {
        let phase = self.phase;
        let runtime = self.actor_incarnation.as_ref();
        let first_failure = Arc::clone(&self.first_failure);
        let inbound = &mut self.inbound;
        let pending = &mut self.pending_controller_inbound;
        let inbound_gate = &self.inbound_gate;
        let publications = Arc::clone(&self.inbound_publications);
        let readiness = self.readiness.clone();
        let mut fatal_timer_committed = false;

        let arbitrated = 'arbitration: {
            let mut first = first_failure
                .outcome
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match &*first {
                ActorFirstOutcome::Failed(cause, _) => {
                    break 'arbitration QuinnLegActorInboundTimerPoll::Suppressed {
                        timer,
                        cause: Arc::clone(cause),
                    };
                }
                ActorFirstOutcome::LocalShutdownWon => {
                    break 'arbitration QuinnLegActorInboundTimerPoll::LocalShutdown(timer);
                }
                ActorFirstOutcome::Open => {}
            }
            if phase != ActorOwnerPhase::Running {
                break 'arbitration QuinnLegActorInboundTimerPoll::LocalShutdown(timer);
            }
            let Some(runtime) = runtime else {
                break 'arbitration QuinnLegActorInboundTimerPoll::TimerRejected(timer);
            };
            let mut timer = timer;
            let _inbound_cutoff = cutoff_operation(&mut timer, &readiness, &publications);

            let mut record = if let Some(record) = pending.take() {
                Some(record)
            } else {
                let Some(receiver) = inbound.as_mut() else {
                    break 'arbitration QuinnLegActorInboundTimerPoll::LocalShutdown(timer);
                };
                match receiver.try_recv() {
                    Ok(record) => {
                        match ControllerInboundRecord::from_actor_record(record, inbound_gate) {
                            Ok(record) => Some(record),
                            Err(error) => {
                                let cause =
                                    first_failure.install_locked(&mut first, Arc::new(error));
                                break 'arbitration QuinnLegActorInboundTimerPoll::Failed {
                                    timer,
                                    cause,
                                };
                            }
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => None,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        let cause = first_failure.install_locked(
                            &mut first,
                            Arc::new(QuinnLegActorError::Protocol(
                                "actor inbound receiver closed before root outcome",
                            )),
                        );
                        break 'arbitration QuinnLegActorInboundTimerPoll::Failed { timer, cause };
                    }
                }
            };

            let record_is_after_deadline = record.as_ref().is_some_and(|record| {
                !arrival_beats_or_meets_deadline(record.arrived_at(), timer.arrival_deadline())
            });
            if record_is_after_deadline {
                let Some(record_to_restore) = record.take() else {
                    let cause = first_failure.install_locked(
                        &mut first,
                        Arc::new(QuinnLegActorError::Protocol(
                            "post-cutoff actor record disappeared before restoration",
                        )),
                    );
                    break 'arbitration QuinnLegActorInboundTimerPoll::Failed { timer, cause };
                };
                if pending.replace(record_to_restore).is_some() {
                    let cause = first_failure.install_locked(
                        &mut first,
                        Arc::new(QuinnLegActorError::Protocol(
                            "controller inbound pending slot already occupied",
                        )),
                    );
                    break 'arbitration QuinnLegActorInboundTimerPoll::Failed { timer, cause };
                }
            }

            let Some(mut record) = record.take() else {
                let arrival_deadline = timer.arrival_deadline();
                let decision = match publications.transact_unless_arrived_by(
                    arrival_deadline,
                    timer,
                    |timer| timer_operation(context, runtime, timer),
                ) {
                    Ok(decision) => decision,
                    Err(timer) => {
                        break 'arbitration QuinnLegActorInboundTimerPoll::PublicationPending(
                            timer,
                        );
                    }
                };
                match decision {
                    QuinnLegActorTimerDecision::Committed(value) => {
                        if let QuinnLegActorTimerCommitMode::Fatal(error) = mode {
                            first_failure.install_locked_at(
                                &mut first,
                                Arc::new(error),
                                arrival_deadline,
                            );
                            fatal_timer_committed = true;
                        }
                        break 'arbitration QuinnLegActorInboundTimerPoll::TimerCommitted(value);
                    }
                    QuinnLegActorTimerDecision::Rejected(timer) => {
                        break 'arbitration QuinnLegActorInboundTimerPoll::TimerRejected(timer);
                    }
                    QuinnLegActorTimerDecision::Invariant { timer, error } => {
                        break 'arbitration QuinnLegActorInboundTimerPoll::TimerInvariant {
                            timer,
                            error,
                        };
                    }
                }
            };

            let envelope = match record.take_envelope() {
                Ok(envelope) => envelope,
                Err(error) => {
                    let cause = first_failure.install_locked(&mut first, Arc::new(error));
                    break 'arbitration QuinnLegActorInboundTimerPoll::Failed { timer, cause };
                }
            };
            match inbound_operation(context, runtime, envelope, timer) {
                Ok((decision, timer)) => match decision {
                    QuinnLegActorInboundDecision::Classified(value) => {
                        record.commit_classified();
                        QuinnLegActorInboundTimerPoll::Inbound {
                            poll: QuinnLegActorInboundPoll::Classified(value),
                            timer,
                        }
                    }
                    QuinnLegActorInboundDecision::Blocked(envelope) => {
                        if let Err(error) = record.restore_envelope(envelope) {
                            let cause = first_failure.install_locked(&mut first, Arc::new(error));
                            break 'arbitration QuinnLegActorInboundTimerPoll::Failed {
                                timer,
                                cause,
                            };
                        }
                        if pending.replace(record).is_some() {
                            let cause = first_failure.install_locked(
                                &mut first,
                                Arc::new(QuinnLegActorError::Protocol(
                                    "controller inbound pending slot already occupied",
                                )),
                            );
                            break 'arbitration QuinnLegActorInboundTimerPoll::Failed {
                                timer,
                                cause,
                            };
                        }
                        QuinnLegActorInboundTimerPoll::Inbound {
                            poll: QuinnLegActorInboundPoll::Blocked,
                            timer,
                        }
                    }
                },
                Err((timer, error)) => {
                    let cause = first_failure.install_locked(&mut first, Arc::new(error));
                    QuinnLegActorInboundTimerPoll::Failed { timer, cause }
                }
            }
        };

        if fatal_timer_committed
            || matches!(
                &arbitrated,
                QuinnLegActorInboundTimerPoll::Suppressed { .. }
                    | QuinnLegActorInboundTimerPoll::Failed { .. }
            )
        {
            self.request_cancel_after_arbitration();
        }
        arbitrated
    }

    /// Consumes and classifies at most one complete record while the actor's
    /// first-failure lock is still held, then lets the composition commit its
    /// exact protocol typestate before that lock is released.  Unlike the
    /// async convenience receiver, no decoded record or tracked ownership can
    /// escape this transaction and an idle actor never blocks unrelated
    /// session progress.
    pub(super) fn try_transact_composition_inbound<T>(
        &mut self,
        _composition_gate: &QuinnLegCompositionGate,
        installed_features: FeatureSet,
        operation: impl FnOnce(
            BoundInbound,
            SimTime,
            &LegOutboundQueue,
        ) -> Result<T, QuinnLegActorError>,
    ) -> Result<QuinnLegActorInboundPoll<T>, QuinnLegActorError> {
        if let Some(error) = self.observe_mutating_port_error() {
            return Err(error);
        }
        let first_failure = Arc::clone(&self.first_failure);
        let inbound = &mut self.inbound;
        let pending = &mut self.pending_controller_inbound;
        let inbound_gate = &self.inbound_gate;
        let io = self.io.as_mut().ok_or(QuinnLegActorError::AlreadyJoined)?;
        let queue = self
            .queue
            .as_ref()
            .ok_or(QuinnLegActorError::AlreadyJoined)?;
        let committed = first_failure.commit(|| {
            let mut record = if let Some(record) = pending.take() {
                record
            } else {
                let receiver = inbound.as_mut().ok_or(QuinnLegActorError::AlreadyJoined)?;
                match receiver.try_recv() {
                    Ok(record) => ControllerInboundRecord::from_actor_record(record, inbound_gate)?,
                    Err(mpsc::error::TryRecvError::Empty) => {
                        return Ok(QuinnLegActorInboundPoll::Idle);
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        return Err(QuinnLegActorError::Protocol(
                            "actor inbound receiver closed before root outcome",
                        ));
                    }
                }
            };
            let envelope = record.take_envelope()?;
            let (encoded, arrived_at) = envelope.into_parts();
            let classified = io
                .receive_classified_delivery(encoded, installed_features)
                .map_err(QuinnLegActorError::LegIo)?;
            let value = operation(classified, arrived_at, queue)?;
            record.commit_classified();
            Ok(QuinnLegActorInboundPoll::Classified(value))
        });
        match committed {
            Ok(outcome) => Ok(outcome),
            Err(barrier) => {
                self.request_cancel();
                Err(barrier.into_actor_error())
            }
        }
    }

    /// Client-B counterpart that may retain one already-classified physical
    /// control value without committing the actor record which carried it.
    /// The callback runs under the same first-failure lock as decode and must
    /// select `Hold` before any session typestate preflight. This preserves
    /// the exact raw arrival/ordinal/byte reservation across ACK FIFO pressure
    /// and lets a later physical hint validator consume provenance directly.
    pub(super) fn try_classify_or_hold_composition_inbound<T, H>(
        &mut self,
        _composition_gate: &QuinnLegCompositionGate,
        installed_features: FeatureSet,
        operation: impl FnOnce(
            BoundInbound,
            SimTime,
            &LegOutboundQueue,
        ) -> Result<
            QuinnLegActorCompositionInboundDecision<T, H>,
            QuinnLegActorError,
        >,
    ) -> Result<QuinnLegActorCompositionInboundPoll<T, H>, QuinnLegActorError> {
        if let Some(error) = self.observe_mutating_port_error() {
            return Err(error);
        }
        let first_failure = Arc::clone(&self.first_failure);
        let inbound = &mut self.inbound;
        let pending = &mut self.pending_controller_inbound;
        let inbound_gate = &self.inbound_gate;
        let io = self.io.as_mut().ok_or(QuinnLegActorError::AlreadyJoined)?;
        let queue = self
            .queue
            .as_ref()
            .ok_or(QuinnLegActorError::AlreadyJoined)?;
        let committed = first_failure.commit(|| {
            let mut record = if let Some(record) = pending.take() {
                record
            } else {
                let receiver = inbound.as_mut().ok_or(QuinnLegActorError::AlreadyJoined)?;
                match receiver.try_recv() {
                    Ok(record) => ControllerInboundRecord::from_actor_record(record, inbound_gate)?,
                    Err(mpsc::error::TryRecvError::Empty) => {
                        return Ok(QuinnLegActorCompositionInboundPoll::Idle);
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        return Err(QuinnLegActorError::Protocol(
                            "actor inbound receiver closed before root outcome",
                        ));
                    }
                }
            };
            let envelope = record.take_envelope()?;
            let publication_epoch = record.publication_epoch();
            let (encoded, arrived_at) = envelope.into_parts();
            let send_ordinal = encoded.send_ordinal();
            let encoded_bytes = encoded.bytes().len();
            let classified = io
                .receive_classified_delivery(encoded, installed_features)
                .map_err(QuinnLegActorError::LegIo)?;
            match operation(classified, arrived_at, queue)? {
                QuinnLegActorCompositionInboundDecision::Classified(value) => {
                    record.commit_classified();
                    Ok(QuinnLegActorCompositionInboundPoll::Classified(value))
                }
                QuinnLegActorCompositionInboundDecision::Hold(value) => Ok(
                    QuinnLegActorCompositionInboundPoll::Held(QuinnLegActorHeldInbound {
                        record,
                        value,
                        arrived_at,
                        publication_epoch,
                        send_ordinal,
                        encoded_bytes,
                    }),
                ),
            }
        });
        match committed {
            Ok(outcome) => Ok(outcome),
            Err(barrier) => {
                self.request_cancel();
                Err(barrier.into_actor_error())
            }
        }
    }

    pub(crate) async fn recv_classified(
        &mut self,
        installed_features: FeatureSet,
    ) -> Result<Option<QuinnLegActorInbound>, QuinnLegActorError> {
        let first_failure = Arc::clone(&self.first_failure);
        let mut cancel = self.cancel.subscribe();
        if self.phase != ActorOwnerPhase::Running
            || first_failure.current().is_some()
            || *cancel.borrow()
        {
            return self.fatal_or_cancelled_receive();
        }
        let record = tokio::select! {
            biased;
            () = wait_cancel(&mut cancel) => {
                return self.fatal_or_cancelled_receive();
            }
            record = self.inbound.as_mut().ok_or(QuinnLegActorError::AlreadyJoined)?.recv() => {
                let Some(record) = record else {
                    return self.fatal_or_cancelled_receive();
                };
                record
            },
        };
        if *cancel.borrow() {
            drop(record);
            return match self.observe_mutating_port_error() {
                Some(error) => Err(error),
                None => Ok(None),
            };
        }
        let InboundRecord {
            header,
            bytes,
            publication_epoch: _,
            arrived_at,
            byte_ownership,
            composition_ownership,
            queued_marker,
            delivery,
        } = record;
        drop(queued_marker);
        let io = self.io.as_mut().ok_or(QuinnLegActorError::AlreadyJoined)?;
        let committed = first_failure.commit(|| {
            let inbound = io
                .receive_quinn_actor_delivery(
                    &self.inbound_gate,
                    header.route,
                    header.lane,
                    bytes,
                    installed_features,
                )
                .map_err(QuinnLegActorError::LegIo)?;
            if let Some(delivery) = delivery {
                delivery.commit();
            }
            Ok(QuinnLegActorInbound::mint(
                &self.inbound_gate,
                inbound,
                arrived_at,
                byte_ownership,
                composition_ownership,
            ))
        });
        match committed {
            Ok(inbound) => Ok(Some(inbound)),
            Err(barrier) => {
                self.request_cancel();
                Err(barrier.into_actor_error())
            }
        }
    }

    pub(crate) fn request_cancel(&mut self) {
        if self.phase != ActorOwnerPhase::Running {
            return;
        }
        self.first_failure.mark_local_shutdown_if_open();
        self.request_cancel_after_arbitration();
    }

    /// Records the canonical controller fault before cancel, preserving its hard
    /// terminal time; structural cleanup cannot rewrite it to LocalShutdown.
    pub(super) fn request_composition_failure(&mut self) {
        if self.phase != ActorOwnerPhase::Running {
            return;
        }
        let _first = self.first_failure.record(QuinnLegActorError::Protocol(
            "owner session composition failed",
        ));
        self.request_cancel_after_arbitration();
    }

    fn request_cancel_after_arbitration(&mut self) {
        if self.phase != ActorOwnerPhase::Running {
            return;
        }
        self.phase = ActorOwnerPhase::Closing;
        if let Err(error) = self.readiness.advance_output() {
            let _first = self.first_failure.record(error);
        }
        drop(self.pending_controller_inbound.take());
        self.cancel.send_replace(true);
        if let Some(mut inbound) = self.inbound.take() {
            inbound.close();
            if let Some(handoff) = self.inbound_handoff.take() {
                if let Err(inbound) = handoff.send(inbound) {
                    drop(inbound);
                }
            } else {
                drop(inbound);
            }
        }
    }

    pub(crate) async fn join(&mut self) -> Result<QuinnLegActorJoined, QuinnLegActorError> {
        if self.phase == ActorOwnerPhase::Joined {
            return Err(QuinnLegActorError::AlreadyJoined);
        }
        self.request_cancel();
        if self.observed_root.is_none() {
            let joined = self
                .nursery
                .join_next()
                .await
                .ok_or(QuinnLegActorError::MissingRoot)?;
            self.observed_root = Some(joined.into());
        }
        if !self.nursery.is_empty() {
            return Err(QuinnLegActorError::UnexpectedRootCount);
        }
        match self.observed_root.as_ref() {
            Some(ObservedRootOutcome::Completed(_)) => {}
            Some(ObservedRootOutcome::ActorError(error)) => {
                return Err(QuinnLegActorError::RootFailed(Arc::clone(error)));
            }
            Some(ObservedRootOutcome::JoinError(error)) => {
                return Err(QuinnLegActorError::TaskJoin(error.to_string()));
            }
            None => return Err(QuinnLegActorError::MissingRoot),
        };
        let (queue_owned_messages, queue_owned_bytes) = match self.queue.as_ref() {
            Some(queue) if self.io.is_some() && !self.controller_parts_transferred => {
                let queue_inventory =
                    queue.cold_inventory().ok_or(QuinnLegActorError::Protocol(
                        "actor outbound FIFO cold inventory is inconsistent",
                    ))?;
                let messages = queue_inventory
                    .queued_frames()
                    .checked_add(usize::from(queue_inventory.ordered_delivery_wait()))
                    .ok_or(QuinnLegActorError::Protocol(
                        "actor outbound FIFO message inventory overflowed",
                    ))?;
                (messages, queue_inventory.summed_frame_bytes())
            }
            None if self.io.is_none() && self.controller_parts_transferred => (0, 0),
            _ => {
                return Err(QuinnLegActorError::Protocol(
                    "actor controller-parts transfer state is inconsistent",
                ));
            }
        };
        let owned_messages = self
            .counters
            .owned_messages()
            .checked_add(queue_owned_messages)
            .ok_or(QuinnLegActorError::Protocol(
                "actor joined message inventory overflowed",
            ))?;
        let owned_bytes = self
            .counters
            .owned_bytes()
            .checked_add(queue_owned_bytes)
            .ok_or(QuinnLegActorError::Protocol(
                "actor joined byte inventory overflowed",
            ))?;
        if owned_messages != 0 || owned_bytes != 0 {
            return Err(QuinnLegActorError::ResidualOwnership {
                messages: owned_messages,
                bytes: owned_bytes,
            });
        }
        let Some(actor_incarnation) = self.actor_incarnation.take() else {
            return Err(QuinnLegActorError::MissingPhysicalIncarnation);
        };
        let observed = self
            .observed_root
            .take()
            .ok_or(QuinnLegActorError::MissingRoot)?;
        let outcome = match observed {
            ObservedRootOutcome::Completed(outcome) => outcome,
            ObservedRootOutcome::ActorError(error) => {
                let returned = Arc::clone(&error);
                self.observed_root = Some(ObservedRootOutcome::ActorError(error));
                self.actor_incarnation = Some(actor_incarnation);
                return Err(QuinnLegActorError::RootFailed(returned));
            }
            ObservedRootOutcome::JoinError(error) => {
                self.actor_incarnation = Some(actor_incarnation);
                let retained = self
                    .observed_root
                    .insert(ObservedRootOutcome::JoinError(error));
                return Err(retained.output_error());
            }
        };
        let reported_terminal = match self.reported_terminal.take_for_join() {
            Ok(terminal) => terminal,
            Err(error) => {
                self.observed_root = Some(ObservedRootOutcome::Completed(outcome));
                self.actor_incarnation = Some(actor_incarnation);
                return Err(error);
            }
        };
        let terminal = match reported_terminal {
            Some(reported) => {
                let (terminal, reported_stamp) = reported.into_parts();
                if !reported_stamp.same_source(&outcome.terminal_stamp)
                    || !reported_stamp.belongs_to(&self.first_failure)
                {
                    self.reported_terminal = ActorReportedTerminalState::Ready(
                        QuinnLegActorReportedTerminal::restore(terminal, reported_stamp),
                    );
                    self.observed_root = Some(ObservedRootOutcome::Completed(outcome));
                    self.actor_incarnation = Some(actor_incarnation);
                    return Err(QuinnLegActorError::Protocol(
                        "actor terminal stamp source changed between handoff and root outcome",
                    ));
                }
                Some(terminal)
            }
            None => None,
        };
        drop(self.queue.take());
        drop(self.transport.take());
        drop(self.inbound.take());
        drop(self.inbound_handoff.take());
        drop(self.io.take());
        if let Some(registration) = self.server_registration.as_mut() {
            registration.mark_clean();
        }
        self.phase = ActorOwnerPhase::Joined;
        Ok(QuinnLegActorJoined {
            actor_incarnation,
            owned_messages,
            owned_bytes,
            terminal,
            terminal_failure: outcome.failure,
            terminal_stamp: Some(outcome.terminal_stamp),
        })
    }
}

impl Drop for StartedQuinnLegActor {
    fn drop(&mut self) {
        if self.phase != ActorOwnerPhase::Joined {
            self.request_cancel();
            self.nursery.abort_all();
        }
    }
}

pub(crate) struct QuinnLegActorFactory;

struct ClientBootstrapCleanup {
    runtime: tokio::runtime::Handle,
    endpoint: Option<Endpoint>,
    drain: Option<quinn::ConnectionDrain>,
    endpoint_driver_drain: Option<quinn::EndpointDriverDrain>,
    #[cfg(test)]
    failure_cleanup_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)]
    failure_cleanup_done: Arc<AtomicBool>,
}

impl ClientBootstrapCleanup {
    fn new(
        runtime: tokio::runtime::Handle,
        endpoint: Endpoint,
        endpoint_driver_drain: quinn::EndpointDriverDrain,
    ) -> Self {
        Self {
            runtime,
            endpoint: Some(endpoint),
            drain: None,
            endpoint_driver_drain: Some(endpoint_driver_drain),
            #[cfg(test)]
            failure_cleanup_gate: Arc::new(TestChildPublicationGate::default()),
            #[cfg(test)]
            failure_cleanup_done: Arc::new(AtomicBool::new(false)),
        }
    }

    fn install_connection_drain(
        &mut self,
        drain: quinn::ConnectionDrain,
    ) -> Result<(), QuinnLegActorError> {
        if self.drain.is_some() {
            return Err(QuinnLegActorError::BootstrapConsumed);
        }
        self.drain = Some(drain);
        Ok(())
    }

    fn endpoint_for_publication(&self) -> Result<Endpoint, QuinnLegActorError> {
        self.endpoint
            .as_ref()
            .cloned()
            .ok_or(QuinnLegActorError::BootstrapConsumed)
    }

    fn take_failure_ownership(
        &mut self,
    ) -> Result<
        (
            Endpoint,
            Option<quinn::ConnectionDrain>,
            quinn::EndpointDriverDrain,
        ),
        QuinnLegActorError,
    > {
        match (
            self.endpoint.take(),
            self.drain.take(),
            self.endpoint_driver_drain.take(),
        ) {
            (Some(endpoint), drain, Some(endpoint_driver_drain)) => {
                Ok((endpoint, drain, endpoint_driver_drain))
            }
            (endpoint, drain, endpoint_driver_drain) => {
                self.endpoint = endpoint;
                self.drain = drain;
                self.endpoint_driver_drain = endpoint_driver_drain;
                Err(QuinnLegActorError::BootstrapConsumed)
            }
        }
    }

    fn disarm(&mut self) -> Result<(), QuinnLegActorError> {
        if self.drain.is_some() || self.endpoint_driver_drain.is_some() {
            return Err(QuinnLegActorError::BootstrapConsumed);
        }
        drop(self.endpoint.take());
        Ok(())
    }

    async fn finish_failure(&mut self) -> Result<(), QuinnLegActorError> {
        let (endpoint, drain, endpoint_driver_drain) = self.take_failure_ownership()?;
        #[cfg(test)]
        let failure_cleanup_gate = Arc::clone(&self.failure_cleanup_gate);
        #[cfg(test)]
        let failure_cleanup_done = Arc::clone(&self.failure_cleanup_done);
        // Register exact cleanup on the construction runtime before the first
        // await. Dropping the caller's waiter detaches this task but cannot
        // discard its endpoint-and-driver release authority.
        let cleanup = self.runtime.spawn(async move {
            if let Some(drain) = drain.as_ref() {
                drain.abandon();
            }
            endpoint.close(CLOSE_CODE, b"actor bootstrap failed");
            #[cfg(test)]
            failure_cleanup_gate.pause_if_armed().await;
            let result = release_client_endpoint_tail(endpoint, drain, endpoint_driver_drain).await;
            #[cfg(test)]
            failure_cleanup_done.store(true, Ordering::Release);
            result
        });
        let result = cleanup.await;
        result.map_err(|error| QuinnLegActorError::TaskJoin(error.to_string()))?
    }
}

impl Drop for ClientBootstrapCleanup {
    fn drop(&mut self) {
        let Ok((endpoint, drain, endpoint_driver_drain)) = self.take_failure_ownership() else {
            return;
        };
        #[cfg(test)]
        let failure_cleanup_done = Arc::clone(&self.failure_cleanup_done);
        if let Some(drain) = drain.as_ref() {
            drain.abandon();
        }
        endpoint.close(CLOSE_CODE, b"actor bootstrap cancelled");
        let cleanup = self.runtime.spawn(async move {
            let _cleanup_verdict =
                release_client_endpoint_tail(endpoint, drain, endpoint_driver_drain).await;
            #[cfg(test)]
            failure_cleanup_done.store(true, Ordering::Release);
        });
        drop(cleanup);
    }
}

async fn release_client_endpoint_tail(
    endpoint: Endpoint,
    drain: Option<quinn::ConnectionDrain>,
    endpoint_driver_drain: quinn::EndpointDriverDrain,
) -> Result<(), QuinnLegActorError> {
    let connection_released = match drain {
        Some(drain) => drain.drained().await,
        None => true,
    };
    let endpoint_idle = endpoint.wait_idle_or_driver_lost().await;
    drop(endpoint);
    let endpoint_driver_released = endpoint_driver_drain.drained().await;
    if !connection_released {
        return Err(QuinnLegActorError::EndpointReleaseLost);
    }
    if !endpoint_idle || !endpoint_driver_released {
        return Err(QuinnLegActorError::EndpointDriverLost);
    }
    Ok(())
}

impl QuinnLegActorFactory {
    pub(crate) async fn connect(
        config: QuinnLegActorClientConfig,
        leg: LegId,
        limits: QuinnLegActorLimits,
        peer_policy: AttachPeerPolicy,
    ) -> Result<StartedQuinnLegActor, QuinnLegActorError> {
        Self::connect_with_clock(
            config,
            leg,
            limits,
            peer_policy,
            SessionMonotonicClock::start(),
        )
        .await
    }

    pub(crate) async fn connect_with_clock(
        config: QuinnLegActorClientConfig,
        leg: LegId,
        limits: QuinnLegActorLimits,
        peer_policy: AttachPeerPolicy,
        session_clock: SessionMonotonicClock,
    ) -> Result<StartedQuinnLegActor, QuinnLegActorError> {
        let deadline = tokio::time::Instant::now()
            .checked_add(config.bootstrap_timeout)
            .ok_or(QuinnLegActorError::InvalidConfig)?;
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| QuinnLegActorError::InvalidConfig)?;
        let mut endpoint = build_client_endpoint(&config, limits)?;
        let endpoint_driver_drain = endpoint
            .take_driver_drain_handle()
            .ok_or(QuinnLegActorError::EndpointDriverLost)?;
        // Own the endpoint/socket completion witness before `connect` can
        // fail synchronously and before any await can be cancelled.
        let mut cleanup = ClientBootstrapCleanup::new(runtime, endpoint, endpoint_driver_drain);
        let mut connecting = match cleanup
            .endpoint
            .as_ref()
            .ok_or(QuinnLegActorError::BootstrapConsumed)?
            .connect(config.server_addr, &config.server_name)
        {
            Ok(connecting) => connecting,
            Err(error) => {
                cleanup.finish_failure().await?;
                return Err(QuinnLegActorError::Handshake(error.to_string()));
            }
        };
        let Some(drain) = connecting.take_drain_handle() else {
            drop(connecting);
            cleanup.finish_failure().await?;
            return Err(QuinnLegActorError::EndpointReleaseLost);
        };
        cleanup.install_connection_drain(drain)?;
        let result = tokio::time::timeout_at(deadline, async {
            ensure_bootstrap_deadline(deadline)?;
            let connection = connecting
                .await
                .map_err(|error| QuinnLegActorError::Handshake(error.to_string()))?;
            validate_alpn(&connection)?;
            let exporter = export_binding(&connection, peer_policy)?;
            binding_confirmation(&connection, ActorRole::Client, leg, peer_policy, &exporter)
                .await?;
            let streams = bootstrap_streams(&connection, ActorRole::Client, leg).await?;
            let binding = attach_binding(peer_policy, exporter)?;
            let endpoint = cleanup.endpoint_for_publication()?;
            start_actor(
                connection,
                ActorBootstrapEndpointOwnership::Client(endpoint),
                &mut cleanup.drain,
                &mut cleanup.endpoint_driver_drain,
                ActorRole::Client,
                leg,
                limits,
                binding,
                streams,
                session_clock,
                deadline,
                #[cfg(test)]
                false,
                None,
            )
        })
        .await
        .map_err(|_| QuinnLegActorError::BootstrapTimedOut)
        .and_then(std::convert::identity);
        if result.is_err() {
            cleanup.finish_failure().await?;
        } else {
            cleanup.disarm()?;
        }
        result
    }
}

#[must_use = "an admitted bootstrap retains one bounded listener slot until it is started or dropped"]
pub(crate) struct QuinnLegActorBootstrap {
    connecting: Option<quinn::Connecting>,
    reservation: Option<AcceptReservation>,
    leg: LegId,
    limits: QuinnLegActorLimits,
    peer_policy: AttachPeerPolicy,
    deadline: tokio::time::Instant,
    runtime: tokio::runtime::Handle,
    #[cfg(test)]
    fail_start_before_publication: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ListenerPhase {
    Accepting,
    Closing,
    Joined,
}

pub(crate) enum QuinnLegActorAdmission {
    RetrySent,
    Bootstrap(Box<QuinnLegActorBootstrap>),
}

#[derive(Clone)]
enum ListenerEndpointReleaseVerdict {
    Clean,
    DriverLost,
    TaskJoin(Arc<str>),
}

impl ListenerEndpointReleaseVerdict {
    fn result(&self) -> Result<(), QuinnLegActorError> {
        match self {
            Self::Clean => Ok(()),
            Self::DriverLost => Err(QuinnLegActorError::EndpointDriverLost),
            Self::TaskJoin(error) => Err(QuinnLegActorError::TaskJoin(error.to_string())),
        }
    }
}

pub(crate) struct QuinnLegActorListener {
    endpoint: Option<Endpoint>,
    endpoint_driver_drain: Option<quinn::EndpointDriverDrain>,
    endpoint_release_task: Option<tokio::task::JoinHandle<bool>>,
    endpoint_release_verdict: Option<ListenerEndpointReleaseVerdict>,
    local_addr: SocketAddr,
    server_crypto: Arc<QuicServerConfig>,
    max_live_actors: usize,
    bootstrap_timeout: Duration,
    registry: Arc<ListenerRegistry>,
    runtime: tokio::runtime::Handle,
    phase: ListenerPhase,
    #[cfg(test)]
    shutdown_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)]
    endpoint_release_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)]
    fail_start_before_publication: bool,
}

impl QuinnLegActorListener {
    pub(crate) async fn bind(
        config: QuinnLegActorServerConfig,
    ) -> Result<Self, QuinnLegActorError> {
        let bootstrap_limits = QuinnLegActorLimits::bootstrap_default()?;
        let server_crypto = build_server_crypto(&config)?;
        let server_config = build_server_quinn_config_from_crypto(
            Arc::clone(&server_crypto),
            bootstrap_limits,
            config.max_live_actors,
        )?;
        let mut endpoint = Endpoint::server(server_config, config.bind_addr)
            .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
        let local_addr = endpoint
            .local_addr()
            .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
        let endpoint_driver_drain = endpoint
            .take_driver_drain_handle()
            .ok_or(QuinnLegActorError::EndpointDriverLost)?;
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| QuinnLegActorError::InvalidConfig)?;
        let registry = Arc::new(ListenerRegistry::new(config.max_live_actors));
        Ok(Self {
            endpoint: Some(endpoint),
            endpoint_driver_drain: Some(endpoint_driver_drain),
            endpoint_release_task: None,
            endpoint_release_verdict: None,
            local_addr,
            server_crypto,
            max_live_actors: config.max_live_actors,
            bootstrap_timeout: config.bootstrap_timeout,
            registry,
            runtime,
            phase: ListenerPhase::Accepting,
            #[cfg(test)]
            shutdown_gate: Arc::new(TestChildPublicationGate::default()),
            #[cfg(test)]
            endpoint_release_gate: Arc::new(TestChildPublicationGate::default()),
            #[cfg(test)]
            fail_start_before_publication: false,
        })
    }

    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[cfg(test)]
    fn open_connections_for_test(&self) -> usize {
        self.endpoint.as_ref().map_or(0, Endpoint::open_connections)
    }

    pub(crate) async fn admit(
        &mut self,
        leg: LegId,
        limits: QuinnLegActorLimits,
        peer_policy: AttachPeerPolicy,
    ) -> Result<QuinnLegActorAdmission, QuinnLegActorError> {
        if self.phase != ListenerPhase::Accepting {
            return Err(QuinnLegActorError::ListenerClosed);
        }
        let incoming = self
            .endpoint
            .as_ref()
            .ok_or(QuinnLegActorError::ListenerClosed)?
            .accept()
            .await
            .ok_or(QuinnLegActorError::ListenerClosed)?;
        if !incoming.remote_address_validated() {
            incoming
                .retry()
                .map_err(|_| QuinnLegActorError::Protocol("QUIC address retry was unavailable"))?;
            return Ok(QuinnLegActorAdmission::RetrySent);
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(self.bootstrap_timeout)
            .ok_or(QuinnLegActorError::InvalidConfig)?;
        let reservation = self.registry.try_reserve()?;
        let server_config = build_server_quinn_config_from_crypto(
            Arc::clone(&self.server_crypto),
            limits,
            self.max_live_actors,
        )?;
        let connecting = incoming
            .accept_with(Arc::new(server_config))
            .map_err(|error| QuinnLegActorError::Handshake(error.to_string()))?;
        Ok(QuinnLegActorAdmission::Bootstrap(Box::new(
            QuinnLegActorBootstrap {
                connecting: Some(connecting),
                reservation: Some(reservation),
                leg,
                limits,
                peer_policy,
                deadline,
                runtime: self.runtime.clone(),
                #[cfg(test)]
                fail_start_before_publication: self.fail_start_before_publication,
            },
        )))
    }

    #[cfg(test)]
    async fn admit_bootstrap_for_test(
        &mut self,
        leg: LegId,
        limits: QuinnLegActorLimits,
        peer_policy: AttachPeerPolicy,
    ) -> Result<QuinnLegActorBootstrap, QuinnLegActorError> {
        loop {
            match self.admit(leg, limits, peer_policy).await? {
                QuinnLegActorAdmission::RetrySent => {}
                QuinnLegActorAdmission::Bootstrap(bootstrap) => return Ok(*bootstrap),
            }
        }
    }

    #[cfg(test)]
    async fn accept(
        &mut self,
        leg: LegId,
        limits: QuinnLegActorLimits,
        peer_policy: AttachPeerPolicy,
    ) -> Result<StartedQuinnLegActor, QuinnLegActorError> {
        self.admit_bootstrap_for_test(leg, limits, peer_policy)
            .await?
            .start()
            .await
    }

    #[cfg(test)]
    async fn accept_with_clock(
        &mut self,
        leg: LegId,
        limits: QuinnLegActorLimits,
        peer_policy: AttachPeerPolicy,
        session_clock: SessionMonotonicClock,
    ) -> Result<StartedQuinnLegActor, QuinnLegActorError> {
        self.admit_bootstrap_for_test(leg, limits, peer_policy)
            .await?
            .start_with_clock(session_clock)
            .await
    }

    pub(crate) async fn shutdown(
        &mut self,
    ) -> Result<QuinnLegActorListenerJoined, QuinnLegActorError> {
        match self.phase {
            ListenerPhase::Joined => return Err(QuinnLegActorError::ListenerClosed),
            ListenerPhase::Accepting => {
                let live = self.registry.live.load(Ordering::Acquire);
                if live != 0 {
                    return Err(QuinnLegActorError::ListenerShutdownBlocked { live });
                }
                // Closing is a retryable transaction. Publish the phase before
                // the first await so dropping one shutdown future cannot
                // consume the listener's sole Joined witness.
                self.phase = ListenerPhase::Closing;
                self.endpoint
                    .as_ref()
                    .ok_or(QuinnLegActorError::EndpointDriverLost)?
                    .close(CLOSE_CODE, b"listener shutdown");
                #[cfg(test)]
                self.shutdown_gate.pause_if_armed().await;
            }
            ListenerPhase::Closing => {}
        }
        self.registry.wait_for_task_termination().await;
        if self.endpoint_release_verdict.is_none() {
            if self.endpoint_release_task.is_none() {
                let endpoint_idle = self
                    .endpoint
                    .as_ref()
                    .ok_or(QuinnLegActorError::EndpointDriverLost)?
                    .wait_idle_or_driver_lost()
                    .await;
                let (endpoint, endpoint_driver_drain) =
                    match (self.endpoint.take(), self.endpoint_driver_drain.take()) {
                        (Some(endpoint), Some(endpoint_driver_drain)) => {
                            (endpoint, endpoint_driver_drain)
                        }
                        (endpoint, endpoint_driver_drain) => {
                            self.endpoint = endpoint;
                            self.endpoint_driver_drain = endpoint_driver_drain;
                            return Err(QuinnLegActorError::EndpointDriverLost);
                        }
                    };
                #[cfg(test)]
                let endpoint_release_gate = Arc::clone(&self.endpoint_release_gate);
                self.endpoint_release_task = Some(self.runtime.spawn(async move {
                    drop(endpoint);
                    #[cfg(test)]
                    endpoint_release_gate.pause_if_armed().await;
                    let endpoint_driver_released = endpoint_driver_drain.drained().await;
                    endpoint_idle && endpoint_driver_released
                }));
            }
            let release_result = self
                .endpoint_release_task
                .as_mut()
                .ok_or(QuinnLegActorError::EndpointDriverLost)?
                .await;
            self.endpoint_release_task = None;
            self.endpoint_release_verdict = Some(match release_result {
                Ok(true) => ListenerEndpointReleaseVerdict::Clean,
                Ok(false) => ListenerEndpointReleaseVerdict::DriverLost,
                Err(error) => ListenerEndpointReleaseVerdict::TaskJoin(error.to_string().into()),
            });
        }
        self.endpoint_release_verdict
            .as_ref()
            .ok_or(QuinnLegActorError::EndpointDriverLost)?
            .result()?;
        if self.registry.unclean.load(Ordering::Acquire) {
            return Err(QuinnLegActorError::ListenerShutdownUnclean);
        }
        self.phase = ListenerPhase::Joined;
        Ok(QuinnLegActorListenerJoined { _private: () })
    }
}

impl Drop for QuinnLegActorListener {
    fn drop(&mut self) {
        if self.phase != ListenerPhase::Joined
            && let Some(endpoint) = self.endpoint.as_ref()
        {
            endpoint.close(CLOSE_CODE, b"listener owner dropped");
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActorRole {
    Client,
    Owner,
}

impl ActorRole {
    const fn code(self) -> u8 {
        match self {
            Self::Client => 1,
            Self::Owner => 2,
        }
    }

    const fn peer(self) -> Self {
        match self {
            Self::Client => Self::Owner,
            Self::Owner => Self::Client,
        }
    }

    const fn endpoint_role(self) -> LegEndpointRole {
        match self {
            Self::Client => LegEndpointRole::Client,
            Self::Owner => LegEndpointRole::Owner,
        }
    }

    const fn outbound_direction(self) -> WireDirection {
        match self {
            Self::Client => WireDirection::ClientToOwner,
            Self::Owner => WireDirection::OwnerToClient,
        }
    }

    const fn inbound_direction(self) -> WireDirection {
        self.peer().outbound_direction()
    }
}

const fn leg_code(leg: LegId) -> u8 {
    match leg {
        LegId::A => 1,
        LegId::B => 2,
    }
}

const fn direction_code(direction: WireDirection) -> u8 {
    match direction {
        WireDirection::ClientToOwner => 1,
        WireDirection::OwnerToClient => 2,
    }
}

const fn lane_code(lane: WireLane) -> u8 {
    match lane {
        WireLane::Control => 1,
        WireLane::Data => 2,
    }
}

fn decode_leg(value: u8) -> Option<LegId> {
    match value {
        1 => Some(LegId::A),
        2 => Some(LegId::B),
        _ => None,
    }
}

fn decode_direction(value: u8) -> Option<WireDirection> {
    match value {
        1 => Some(WireDirection::ClientToOwner),
        2 => Some(WireDirection::OwnerToClient),
        _ => None,
    }
}

fn decode_lane(value: u8) -> Option<WireLane> {
    match value {
        1 => Some(WireLane::Control),
        2 => Some(WireLane::Data),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
struct WireHeader {
    kind: u8,
    ordered: bool,
    route: WireRoute,
    lane: WireLane,
    ordinal: u64,
    payload_len: usize,
}

impl WireHeader {
    fn data(
        route: WireRoute,
        lane: WireLane,
        ordinal: u64,
        payload_len: usize,
        ordered: bool,
    ) -> Self {
        Self {
            kind: RECORD_DATA,
            ordered,
            route,
            lane,
            ordinal,
            payload_len,
        }
    }

    fn delivered(original: Self) -> Self {
        Self {
            kind: RECORD_DELIVERED,
            ordered: false,
            route: original.route,
            lane: original.lane,
            ordinal: original.ordinal,
            payload_len: 0,
        }
    }

    fn encode(self) -> Result<[u8; RECORD_HEADER_BYTES], QuinnLegActorError> {
        if self.ordinal == 0 {
            return Err(QuinnLegActorError::Protocol("zero wire ordinal"));
        }
        let payload_len = u32::try_from(self.payload_len)
            .map_err(|_| QuinnLegActorError::Protocol("wire payload length overflow"))?;
        let mut encoded = [0_u8; RECORD_HEADER_BYTES];
        encoded[0] = RECORD_VERSION;
        encoded[1] = self.kind;
        encoded[2] = u8::from(self.ordered);
        encoded[3] = leg_code(self.route.leg());
        encoded[4] = direction_code(self.route.direction());
        encoded[5] = lane_code(self.lane);
        encoded[6..14].copy_from_slice(&self.ordinal.to_be_bytes());
        encoded[14..18].copy_from_slice(&payload_len.to_be_bytes());
        Ok(encoded)
    }

    fn decode(encoded: [u8; RECORD_HEADER_BYTES]) -> Result<Self, QuinnLegActorError> {
        if encoded[0] != RECORD_VERSION || encoded[2] & !RECORD_ORDERED != 0 {
            return Err(QuinnLegActorError::Protocol(
                "invalid record version or flags",
            ));
        }
        let leg =
            decode_leg(encoded[3]).ok_or(QuinnLegActorError::Protocol("invalid record leg"))?;
        let direction = decode_direction(encoded[4])
            .ok_or(QuinnLegActorError::Protocol("invalid record direction"))?;
        let lane =
            decode_lane(encoded[5]).ok_or(QuinnLegActorError::Protocol("invalid record lane"))?;
        let ordinal = u64::from_be_bytes(
            encoded[6..14]
                .try_into()
                .map_err(|_| QuinnLegActorError::Protocol("invalid record ordinal"))?,
        );
        if ordinal == 0 {
            return Err(QuinnLegActorError::Protocol("zero record ordinal"));
        }
        let payload_len = u32::from_be_bytes(
            encoded[14..18]
                .try_into()
                .map_err(|_| QuinnLegActorError::Protocol("invalid record length"))?,
        ) as usize;
        let ordered = encoded[2] == RECORD_ORDERED;
        match encoded[1] {
            RECORD_DATA if payload_len > 0 => {}
            RECORD_DELIVERED if !ordered && payload_len == 0 => {}
            RECORD_DATA => return Err(QuinnLegActorError::Protocol("empty DATA record")),
            RECORD_DELIVERED => {
                return Err(QuinnLegActorError::Protocol("invalid DELIVERED record"));
            }
            _ => return Err(QuinnLegActorError::Protocol("unknown record kind")),
        }
        Ok(Self {
            kind: encoded[1],
            ordered,
            route: WireRoute::new(leg, direction),
            lane,
            ordinal,
            payload_len,
        })
    }
}

#[derive(Clone)]
struct Readiness {
    shared: Arc<SharedReadiness>,
    sender: watch::Sender<u64>,
}
struct SharedReadiness {
    epoch: AtomicU64,
    output: Mutex<Option<std::task::Waker>>,
}

/// Exact publication boundary for the actor's single fixed data-reader child.
/// The small reservation lock makes `reserve_next` plus in-flight publication
/// atomic with a semantic timer's high-water capture. Record transfer and
/// decode remain outside this cold deadline-arbitration lock.
#[derive(Default)]
struct InboundPublicationTracker {
    reservation: Mutex<()>,
    in_flight_epoch: AtomicU64,
    in_flight_arrived_at: AtomicU64,
}

impl InboundPublicationTracker {
    fn reserve_arrival(
        self: &Arc<Self>,
        readiness: Readiness,
        clock: &SessionMonotonicClock,
    ) -> Result<InboundPublicationReservation, SessionClockError> {
        let _reservation = self
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert_eq!(self.in_flight_epoch.load(Ordering::Acquire), 0);
        let arrived_at = clock.now()?;
        let epoch = readiness.reserve_next();
        self.in_flight_arrived_at
            .store(arrived_at.as_nanos(), Ordering::Release);
        self.in_flight_epoch.store(epoch, Ordering::Release);
        Ok(InboundPublicationReservation {
            readiness,
            epoch,
            arrived_at,
            tracker: Arc::clone(self),
        })
    }

    fn capture_high_water(&self, readiness: &Readiness) -> u64 {
        let _reservation = self
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        readiness.reserved()
    }

    fn has_in_flight(&self) -> bool {
        self.in_flight_epoch.load(Ordering::Acquire) != 0
    }

    fn in_flight_at_or_before(&self, cutoff: u64) -> bool {
        let epoch = self.in_flight_epoch.load(Ordering::Acquire);
        epoch != 0 && epoch <= cutoff
    }

    /// Linearizes a semantic timer commit against a complete actor-read
    /// admission. Any in-flight record admitted at or before the absolute
    /// deadline suppresses the operation regardless of whether its publication
    /// epoch was reserved before or after the timer's frozen high-water.
    fn transact_unless_arrived_by<I, T>(
        &self,
        deadline: SimTime,
        input: I,
        operation: impl FnOnce(I) -> T,
    ) -> Result<T, I> {
        let _reservation = self
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let epoch = self.in_flight_epoch.load(Ordering::Acquire);
        if epoch != 0 {
            let arrived_at = SimTime::from_nanos(self.in_flight_arrived_at.load(Ordering::Acquire));
            if arrival_beats_or_meets_deadline(arrived_at, deadline) {
                return Err(input);
            }
        }
        Ok(operation(input))
    }

    fn release(&self, readiness: &Readiness, epoch: u64) {
        let _reservation = self
            .reservation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        readiness.publish(epoch);
        let previous = self.in_flight_epoch.swap(0, Ordering::AcqRel);
        debug_assert_eq!(previous, epoch);
        self.in_flight_arrived_at.store(0, Ordering::Release);
    }
}

/// RAII publication reservation for one fully read inbound record. The
/// readiness epoch is published before its exact in-flight epoch is released
/// on every exit, including test cancellation/panic unwinding, so an owner
/// that deferred expiry can never park forever.
struct InboundPublicationReservation {
    readiness: Readiness,
    epoch: u64,
    arrived_at: SimTime,
    tracker: Arc<InboundPublicationTracker>,
}

impl InboundPublicationReservation {
    const fn epoch(&self) -> u64 {
        self.epoch
    }

    const fn arrived_at(&self) -> SimTime {
        self.arrived_at
    }
}

impl Drop for InboundPublicationReservation {
    fn drop(&mut self) {
        self.tracker.release(&self.readiness, self.epoch);
    }
}

#[derive(Default)]
struct ActorCounters {
    validated_data_headers: AtomicUsize,
    queued_inbound_messages: AtomicUsize,
    inbound_owned_bytes: AtomicUsize,
    waiting_inbound_bytes: AtomicUsize,
    reserved_delivery_acks: AtomicUsize,
    composition_owned_messages: AtomicUsize,
    outbound_owned_messages: AtomicUsize,
    outbound_owned_bytes: AtomicUsize,
    ack_owned_bytes: AtomicUsize,
    zero: Notify,
}

impl ActorCounters {
    fn owned_messages(&self) -> usize {
        self.queued_inbound_messages.load(Ordering::Acquire)
            + self.composition_owned_messages.load(Ordering::Acquire)
            + self.outbound_owned_messages.load(Ordering::Acquire)
            + self.reserved_delivery_acks.load(Ordering::Acquire)
    }

    fn owned_bytes(&self) -> usize {
        self.inbound_owned_bytes.load(Ordering::Acquire)
            + self.outbound_owned_bytes.load(Ordering::Acquire)
            + self.ack_owned_bytes.load(Ordering::Acquire)
    }

    async fn wait_zero(&self) {
        loop {
            let notified = self.zero.notified();
            if self.owned_messages() == 0 && self.owned_bytes() == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct TrackedPermit {
    _permit: OwnedSemaphorePermit,
    counter: Arc<ActorCounters>,
    field: CounterField,
    units: usize,
}

#[derive(Clone, Copy)]
enum CounterField {
    InboundBytes,
    CompositionMessages,
    OutboundMessages,
    OutboundBytes,
    AckMessages,
    AckBytes,
}

impl TrackedPermit {
    fn new(
        permit: OwnedSemaphorePermit,
        counter: Arc<ActorCounters>,
        field: CounterField,
        units: usize,
    ) -> Self {
        field.counter(&counter).fetch_add(units, Ordering::AcqRel);
        Self {
            _permit: permit,
            counter,
            field,
            units,
        }
    }
}

impl CounterField {
    fn counter(self, counters: &ActorCounters) -> &AtomicUsize {
        match self {
            Self::InboundBytes => &counters.inbound_owned_bytes,
            Self::CompositionMessages => &counters.composition_owned_messages,
            Self::OutboundMessages => &counters.outbound_owned_messages,
            Self::OutboundBytes => &counters.outbound_owned_bytes,
            Self::AckMessages => &counters.reserved_delivery_acks,
            Self::AckBytes => &counters.ack_owned_bytes,
        }
    }
}

impl Drop for TrackedPermit {
    fn drop(&mut self) {
        let previous = self
            .field
            .counter(&self.counter)
            .fetch_sub(self.units, Ordering::AcqRel);
        debug_assert!(previous >= self.units);
        self.counter.zero.notify_waiters();
    }
}

struct CountMarker {
    counter: Arc<ActorCounters>,
    field: MarkerField,
}

#[derive(Clone, Copy)]
enum MarkerField {
    QueuedInbound,
}

impl CountMarker {
    fn new(counter: Arc<ActorCounters>, field: MarkerField) -> Self {
        field.counter(&counter).fetch_add(1, Ordering::AcqRel);
        Self { counter, field }
    }
}

impl MarkerField {
    fn counter(self, counters: &ActorCounters) -> &AtomicUsize {
        match self {
            Self::QueuedInbound => &counters.queued_inbound_messages,
        }
    }
}

impl Drop for CountMarker {
    fn drop(&mut self) {
        let previous = self
            .field
            .counter(&self.counter)
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous >= 1);
        self.counter.zero.notify_waiters();
    }
}

struct OutboundRecord {
    header: WireHeader,
    bytes: Bytes,
    completion: Option<OrderedDeliveryToken>,
    message_ownership: TrackedPermit,
    byte_ownership: TrackedPermit,
}

struct PendingDelivery {
    route: WireRoute,
    lane: WireLane,
    completion: OrderedDeliveryToken,
    _message_ownership: TrackedPermit,
    _byte_ownership: TrackedPermit,
}

struct ReservedDelivery {
    permit: mpsc::OwnedPermit<DeliveryRecord>,
    message_ownership: TrackedPermit,
    byte_ownership: TrackedPermit,
    header: WireHeader,
}

impl ReservedDelivery {
    fn commit(self) {
        let Self {
            permit,
            message_ownership,
            byte_ownership,
            header,
        } = self;
        let _sender = permit.send(DeliveryRecord {
            header,
            _message_ownership: message_ownership,
            _byte_ownership: byte_ownership,
        });
    }
}

struct InboundRecord {
    header: WireHeader,
    bytes: Bytes,
    publication_epoch: u64,
    arrived_at: SimTime,
    byte_ownership: TrackedPermit,
    composition_ownership: TrackedPermit,
    queued_marker: CountMarker,
    delivery: Option<ReservedDelivery>,
}

/// Non-cloneable raw record while controller policy owns the corresponding
/// `LegIo`. The queued marker and all permits stay here across a defensive
/// blocked retry; only the encoded delivery is temporarily moved into the
/// synchronous controller callback.
struct ControllerInboundRecord {
    encoded: Option<EncodedDelivery>,
    publication_epoch: u64,
    arrived_at: SimTime,
    byte_ownership: TrackedPermit,
    composition_ownership: TrackedPermit,
    queued_marker: CountMarker,
    reserved_delivery: Option<ReservedDelivery>,
}

impl ControllerInboundRecord {
    fn from_actor_record(
        record: InboundRecord,
        gate: &QuinnLegActorInboundGate,
    ) -> Result<Self, QuinnLegActorError> {
        let InboundRecord {
            header,
            bytes,
            publication_epoch,
            arrived_at,
            byte_ownership,
            composition_ownership,
            queued_marker,
            delivery,
        } = record;
        let encoded = EncodedDelivery::from_quinn_actor_ingress(
            gate,
            header.route,
            header.lane,
            bytes,
            header.ordinal,
        )
        .map_err(|_| QuinnLegActorError::Protocol("invalid actor DATA payload"))?;
        Ok(Self {
            encoded: Some(encoded),
            publication_epoch,
            arrived_at,
            byte_ownership,
            composition_ownership,
            queued_marker,
            reserved_delivery: delivery,
        })
    }

    fn take_envelope(&mut self) -> Result<QuinnLegActorEncodedInbound, QuinnLegActorError> {
        let delivery = self.encoded.take().ok_or(QuinnLegActorError::Protocol(
            "controller inbound delivery already consumed",
        ))?;
        Ok(QuinnLegActorEncodedInbound::mint(delivery, self.arrived_at))
    }

    const fn publication_epoch(&self) -> u64 {
        self.publication_epoch
    }

    const fn arrived_at(&self) -> SimTime {
        self.arrived_at
    }

    fn restore_envelope(
        &mut self,
        envelope: QuinnLegActorEncodedInbound,
    ) -> Result<(), QuinnLegActorError> {
        let (delivery, arrived_at) = envelope.into_parts();
        if arrived_at != self.arrived_at {
            return Err(QuinnLegActorError::Protocol(
                "controller inbound arrival changed across blocked retry",
            ));
        }
        if self.encoded.replace(delivery).is_some() {
            return Err(QuinnLegActorError::Protocol(
                "controller inbound delivery restored twice",
            ));
        }
        Ok(())
    }

    fn commit_classified(self) {
        let Self {
            encoded,
            publication_epoch: _,
            arrived_at: _,
            byte_ownership,
            composition_ownership,
            queued_marker,
            reserved_delivery,
        } = self;
        debug_assert!(encoded.is_none());
        if let Some(delivery) = reserved_delivery {
            delivery.commit();
        }
        drop((
            encoded,
            byte_ownership,
            composition_ownership,
            queued_marker,
        ));
    }

    fn commit_shutdown_discard(self) {
        let Self {
            encoded,
            publication_epoch: _,
            arrived_at: _,
            byte_ownership,
            composition_ownership,
            queued_marker,
            reserved_delivery,
        } = self;
        debug_assert!(encoded.is_none());
        // Shutdown did not semantically accept this record. Releasing the
        // reserved sender permit and its counters is explicit discard, not a
        // peer-visible DELIVERED acknowledgement.
        drop((
            encoded,
            byte_ownership,
            composition_ownership,
            queued_marker,
            reserved_delivery,
        ));
    }
}

struct DeliveryRecord {
    header: WireHeader,
    _message_ownership: TrackedPermit,
    _byte_ownership: TrackedPermit,
}

struct ActorEncodedTransport {
    outbound: mpsc::Sender<OutboundRecord>,
    message_capacity: Arc<Semaphore>,
    byte_capacity: Arc<Semaphore>,
    counters: Arc<ActorCounters>,
    next_ordinal: u64,
    expected_route: WireRoute,
}

struct ListenerRegistry {
    max_live: usize,
    live: AtomicUsize,
    terminating_tasks: AtomicUsize,
    unclean: AtomicBool,
    admission_gate: Mutex<()>,
    task_termination: Notify,
}

impl ListenerRegistry {
    fn new(max_live: usize) -> Self {
        Self {
            max_live,
            live: AtomicUsize::new(0),
            terminating_tasks: AtomicUsize::new(0),
            unclean: AtomicBool::new(false),
            admission_gate: Mutex::new(()),
            task_termination: Notify::new(),
        }
    }

    fn try_reserve(self: &Arc<Self>) -> Result<AcceptReservation, QuinnLegActorError> {
        let _admission = self
            .admission_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.unclean.load(Ordering::Acquire) {
            return Err(QuinnLegActorError::ListenerShutdownUnclean);
        }
        self.live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                (live < self.max_live).then_some(live + 1)
            })
            .map_err(|_| QuinnLegActorError::ListenerRegistryFull)?;
        Ok(AcceptReservation {
            slot: Arc::new(ActorSlotLease {
                registry: Arc::clone(self),
            }),
        })
    }

    fn mark_unclean(&self) {
        let _admission = self
            .admission_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.unclean.store(true, Ordering::Release);
    }

    async fn wait_for_task_termination(&self) {
        loop {
            let notified = self.task_termination.notified();
            if self.terminating_tasks.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

struct ActorSlotLease {
    registry: Arc<ListenerRegistry>,
}

impl Drop for ActorSlotLease {
    fn drop(&mut self) {
        let previous = self.registry.live.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous >= 1);
    }
}

struct AcceptReservation {
    slot: Arc<ActorSlotLease>,
}

impl AcceptReservation {
    fn publish(self) -> ServerActorRegistration {
        ServerActorRegistration {
            slot: Some(self.slot),
            active: true,
        }
    }
}

struct ServerActorRegistration {
    slot: Option<Arc<ActorSlotLease>>,
    active: bool,
}

impl ServerActorRegistration {
    fn task_guard(&self) -> Option<TaskTerminationGuard> {
        let slot = Arc::clone(self.slot.as_ref()?);
        slot.registry
            .terminating_tasks
            .fetch_add(1, Ordering::AcqRel);
        Some(TaskTerminationGuard { slot })
    }

    fn mark_clean(&mut self) {
        if self.active {
            self.active = false;
            drop(self.slot.take());
        }
    }
}

impl Drop for ServerActorRegistration {
    fn drop(&mut self) {
        if self.active {
            self.active = false;
            if let Some(slot) = self.slot.as_ref() {
                slot.registry.mark_unclean();
            }
        }
        drop(self.slot.take());
    }
}

struct TaskTerminationGuard {
    slot: Arc<ActorSlotLease>,
}

impl Drop for TaskTerminationGuard {
    fn drop(&mut self) {
        let previous = self
            .slot
            .registry
            .terminating_tasks
            .fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous >= 1);
        self.slot.registry.task_termination.notify_waiters();
    }
}

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, QuinnLegActorError> {
    let file = File::open(path).map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, QuinnLegActorError> {
    let file = File::open(path).map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?
        .ok_or(QuinnLegActorError::Protocol("missing server private key"))
}

fn actor_transport_config(
    limits: QuinnLegActorLimits,
    role: ActorRole,
) -> Result<Arc<TransportConfig>, QuinnLegActorError> {
    let frame =
        u64::try_from(limits.max_frame_bytes()).map_err(|_| QuinnLegActorError::InvalidConfig)?;
    let data_record = frame
        .checked_add(RECORD_HEADER_BYTES as u64)
        .ok_or(QuinnLegActorError::InvalidConfig)?;
    let stream_window = data_record
        .checked_mul(2)
        .and_then(|value| value.checked_add(STREAM_PREFACE_BYTES as u64))
        .ok_or(QuinnLegActorError::InvalidConfig)?;
    let connection_window = stream_window
        .checked_mul(4)
        .and_then(|value| value.checked_add(128))
        .ok_or(QuinnLegActorError::InvalidConfig)?;
    let stream_window =
        VarInt::from_u64(stream_window).map_err(|_| QuinnLegActorError::InvalidConfig)?;
    let connection_window =
        VarInt::from_u64(connection_window).map_err(|_| QuinnLegActorError::InvalidConfig)?;
    let mut transport = TransportConfig::default();
    transport
        .max_concurrent_bidi_streams(VarInt::from_u32(match role {
            ActorRole::Client => 0,
            ActorRole::Owner => 1,
        }))
        .max_concurrent_uni_streams(VarInt::from_u32(2))
        .stream_receive_window(stream_window)
        .receive_window(connection_window)
        .send_window(connection_window.into_inner())
        .datagram_receive_buffer_size(None)
        .datagram_send_buffer_size(0);
    Ok(Arc::new(transport))
}

fn build_client_endpoint(
    config: &QuinnLegActorClientConfig,
    limits: QuinnLegActorLimits,
) -> Result<Endpoint, QuinnLegActorError> {
    let mut client_config = quinn::ClientConfig::new(Arc::clone(&config.client_crypto));
    client_config.transport_config(actor_transport_config(limits, ActorRole::Client)?);
    let mut endpoint = Endpoint::client(config.bind_addr)
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

fn build_client_crypto(
    path: &Path,
) -> Result<Arc<dyn quinn::crypto::ClientConfig>, QuinnLegActorError> {
    let mut roots = rustls::RootCertStore::empty();
    for certificate in load_certificates(path)? {
        roots
            .add(certificate)
            .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    }
    let mut crypto =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_no_client_auth();
    crypto.alpn_protocols = vec![ACTOR_ALPN.to_vec()];
    crypto.enable_early_data = false;
    let quic_crypto = QuicClientConfig::try_from(crypto)
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    Ok(Arc::new(quic_crypto))
}

#[cfg(test)]
fn build_server_quinn_config(
    config: &QuinnLegActorServerConfig,
    limits: QuinnLegActorLimits,
) -> Result<quinn::ServerConfig, QuinnLegActorError> {
    build_server_quinn_config_from_crypto(
        build_server_crypto(config)?,
        limits,
        config.max_live_actors,
    )
}

fn build_server_crypto(
    config: &QuinnLegActorServerConfig,
) -> Result<Arc<QuicServerConfig>, QuinnLegActorError> {
    let certificates = load_certificates(&config.cert_path)?;
    let private_key = load_private_key(&config.key_path)?;
    let mut crypto =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    crypto.alpn_protocols = vec![ACTOR_ALPN.to_vec()];
    crypto.max_early_data_size = 0;
    let quic_crypto = QuicServerConfig::try_from(crypto)
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    Ok(Arc::new(quic_crypto))
}

fn build_server_quinn_config_from_crypto(
    server_crypto: Arc<QuicServerConfig>,
    limits: QuinnLegActorLimits,
    max_incoming: usize,
) -> Result<quinn::ServerConfig, QuinnLegActorError> {
    let mut server_config = quinn::ServerConfig::with_crypto(server_crypto);
    server_config.max_incoming(max_incoming);
    server_config.transport_config(actor_transport_config(limits, ActorRole::Owner)?);
    Ok(server_config)
}

fn validate_alpn(connection: &Connection) -> Result<(), QuinnLegActorError> {
    let handshake = connection
        .handshake_data()
        .ok_or(QuinnLegActorError::Protocol(
            "missing rustls handshake data",
        ))?
        .downcast::<HandshakeData>()
        .map_err(|_| QuinnLegActorError::Protocol("non-rustls handshake data"))?;
    if handshake.protocol.as_deref() != Some(ACTOR_ALPN) {
        return Err(QuinnLegActorError::Protocol("unexpected actor ALPN"));
    }
    Ok(())
}

fn export_binding(
    connection: &Connection,
    policy: AttachPeerPolicy,
) -> Result<[u8; 32], QuinnLegActorError> {
    let mut context = Vec::with_capacity(
        EXPORTER_CONTEXT_PREFIX.len()
            + policy.owner_identity.as_bytes().len()
            + policy.device_principal.as_bytes().len(),
    );
    context.extend_from_slice(EXPORTER_CONTEXT_PREFIX);
    context.extend_from_slice(policy.owner_identity.as_bytes());
    context.extend_from_slice(policy.device_principal.as_bytes());
    let mut exporter = [0_u8; 32];
    connection
        .export_keying_material(&mut exporter, EXPORTER_LABEL, &context)
        .map_err(|error| QuinnLegActorError::Handshake(format!("{error:?}")))?;
    Ok(exporter)
}

fn attach_binding(
    policy: AttachPeerPolicy,
    exporter: [u8; 32],
) -> Result<AttachTransportBinding, QuinnLegActorError> {
    let exporter = TlsExporterBinding::new(exporter)
        .map_err(|_| QuinnLegActorError::Protocol("invalid TLS exporter binding"))?;
    let alpn = AttachAlpn::new(ACTOR_ALPN)
        .map_err(|_| QuinnLegActorError::Protocol("invalid fixed actor ALPN"))?;
    Ok(AttachTransportBinding::new(
        policy.owner_identity,
        alpn,
        exporter,
        policy.device_principal,
    ))
}

fn confirmation_tag(
    exporter: &[u8; 32],
    sender: ActorRole,
    leg: LegId,
    policy: AttachPeerPolicy,
) -> Result<[u8; 32], QuinnLegActorError> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(exporter)
        .map_err(|_| QuinnLegActorError::Protocol("invalid exporter HMAC key"))?;
    mac.update(CONFIRM_PREFIX);
    mac.update(&[sender.code(), leg_code(leg)]);
    mac.update(policy.owner_identity.as_bytes());
    mac.update(policy.device_principal.as_bytes());
    mac.update(ACTOR_ALPN);
    Ok(mac.finalize().into_bytes().into())
}

fn verify_confirmation(
    exporter: &[u8; 32],
    sender: ActorRole,
    leg: LegId,
    policy: AttachPeerPolicy,
    received: &[u8; 32],
) -> Result<(), QuinnLegActorError> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(exporter)
        .map_err(|_| QuinnLegActorError::Protocol("invalid exporter HMAC key"))?;
    mac.update(CONFIRM_PREFIX);
    mac.update(&[sender.code(), leg_code(leg)]);
    mac.update(policy.owner_identity.as_bytes());
    mac.update(policy.device_principal.as_bytes());
    mac.update(ACTOR_ALPN);
    mac.verify_slice(received)
        .map_err(|_| QuinnLegActorError::Protocol("binding confirmation mismatch"))
}

async fn binding_confirmation(
    connection: &Connection,
    role: ActorRole,
    leg: LegId,
    policy: AttachPeerPolicy,
    exporter: &[u8; 32],
) -> Result<(), QuinnLegActorError> {
    let (mut send, mut recv) = match role {
        ActorRole::Client => connection
            .open_bi()
            .await
            .map_err(|error| QuinnLegActorError::Io(error.to_string()))?,
        ActorRole::Owner => {
            let streams = connection
                .accept_bi()
                .await
                .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
            connection.set_max_concurrent_bi_streams(VarInt::from_u32(0));
            streams
        }
    };
    let local_tag = confirmation_tag(exporter, role, leg, policy)?;
    send.write_all(&local_tag)
        .await
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    send.finish()
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    let mut peer_tag = [0_u8; 32];
    recv.read_exact(&mut peer_tag)
        .await
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    let tail = recv
        .read_to_end(0)
        .await
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    if !tail.is_empty() {
        return Err(QuinnLegActorError::Protocol(
            "binding confirmation trailing bytes",
        ));
    }
    verify_confirmation(exporter, role.peer(), leg, policy, &peer_tag)
}

struct ActorStreams {
    data_send: SendStream,
    delivered_send: SendStream,
    data_recv: RecvStream,
    delivered_recv: RecvStream,
}

fn stream_preface(kind: u8, role: ActorRole, leg: LegId) -> [u8; STREAM_PREFACE_BYTES] {
    let mut preface = [0_u8; STREAM_PREFACE_BYTES];
    preface[..4].copy_from_slice(STREAM_MAGIC);
    preface[4] = 1;
    preface[5] = kind;
    preface[6] = role.code();
    preface[7] = leg_code(leg);
    preface
}

async fn bootstrap_streams(
    connection: &Connection,
    role: ActorRole,
    leg: LegId,
) -> Result<ActorStreams, QuinnLegActorError> {
    let mut data_send = connection
        .open_uni()
        .await
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    data_send
        .write_all(&stream_preface(STREAM_DATA, role, leg))
        .await
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    let mut delivered_send = connection
        .open_uni()
        .await
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    delivered_send
        .write_all(&stream_preface(STREAM_DELIVERED, role, leg))
        .await
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;

    let first = connection
        .accept_uni()
        .await
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    let second = connection
        .accept_uni()
        .await
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    let (first_kind, first) = verify_stream_preface(first, role.peer(), leg).await?;
    let (second_kind, second) = verify_stream_preface(second, role.peer(), leg).await?;
    let (data_recv, delivered_recv) = match (first_kind, second_kind) {
        (STREAM_DATA, STREAM_DELIVERED) => (first, second),
        (STREAM_DELIVERED, STREAM_DATA) => (second, first),
        _ => return Err(QuinnLegActorError::Protocol("duplicate actor stream kind")),
    };
    connection.set_max_concurrent_uni_streams(VarInt::from_u32(0));
    Ok(ActorStreams {
        data_send,
        delivered_send,
        data_recv,
        delivered_recv,
    })
}

async fn verify_stream_preface(
    mut stream: RecvStream,
    expected_role: ActorRole,
    expected_leg: LegId,
) -> Result<(u8, RecvStream), QuinnLegActorError> {
    let mut preface = [0_u8; STREAM_PREFACE_BYTES];
    stream
        .read_exact(&mut preface)
        .await
        .map_err(|error| QuinnLegActorError::Io(error.to_string()))?;
    if &preface[..4] != STREAM_MAGIC
        || preface[4] != 1
        || !matches!(preface[5], STREAM_DATA | STREAM_DELIVERED)
        || preface[6] != expected_role.code()
        || preface[7] != leg_code(expected_leg)
    {
        return Err(QuinnLegActorError::Protocol("invalid actor stream preface"));
    }
    Ok((preface[5], stream))
}

enum ActorBootstrapEndpointOwnership {
    Client(Endpoint),
    Server,
}

enum ActorEndpointOwnership {
    Client {
        endpoint: Endpoint,
        endpoint_driver_drain: quinn::EndpointDriverDrain,
    },
    Server,
}

#[allow(clippy::too_many_arguments)]
fn start_actor(
    connection: Connection,
    endpoint: ActorBootstrapEndpointOwnership,
    connection_drain: &mut Option<quinn::ConnectionDrain>,
    endpoint_driver_drain: &mut Option<quinn::EndpointDriverDrain>,
    role: ActorRole,
    leg: LegId,
    limits: QuinnLegActorLimits,
    binding: AttachTransportBinding,
    streams: ActorStreams,
    session_clock: SessionMonotonicClock,
    publication_deadline: tokio::time::Instant,
    #[cfg(test)] fail_start_before_publication: bool,
    server_reservation: Option<AcceptReservation>,
) -> Result<StartedQuinnLegActor, QuinnLegActorError> {
    #[cfg(test)]
    if fail_start_before_publication {
        return Err(QuinnLegActorError::InvalidConfig);
    }
    let capacity = limits.capacity_witness();
    let gate = QuinnLegActorTransportGate::mint_for_fixed_factory();
    let (actor_incarnation, policy_incarnation) =
        PhysicalLegRuntimeIncarnation::mint_for_quinn_actor(&gate);
    let (io, reporter) = LegIo::for_quinn_leg_actor(
        gate,
        policy_incarnation,
        leg,
        role.endpoint_role(),
        binding,
        limits.leg_io(),
    );
    let queue = LegOutboundQueue::for_leg(
        io.established_leg(),
        limits.queue().max_messages,
        limits.queue().max_bytes,
    )
    .map_err(|_| QuinnLegActorError::InvalidConfig)?;
    let counters = Arc::new(ActorCounters::default());
    let readiness = Readiness::new();
    let inbound_publications = Arc::new(InboundPublicationTracker::default());
    let (outbound_tx, outbound_rx) = mpsc::channel(limits.live().outbound_messages);
    let (inbound_tx, inbound_rx) = mpsc::channel(limits.live().inbound_messages);
    let (inbound_handoff, inbound_handoff_rx) = oneshot::channel();
    let (reported_terminal_tx, reported_terminal_rx) = oneshot::channel();
    let (delivery_tx, delivery_rx) = mpsc::channel(limits.live().ack_messages);
    let outbound_messages = Arc::new(Semaphore::new(limits.live().outbound_messages));
    let outbound_bytes = Arc::new(Semaphore::new(limits.live().outbound_bytes));
    let inbound_bytes = Arc::new(Semaphore::new(limits.live().inbound_bytes));
    let composition_messages = Arc::new(Semaphore::new(limits.live().composition_messages));
    let ack_messages = Arc::new(Semaphore::new(limits.live().ack_messages));
    let ack_bytes = Arc::new(Semaphore::new(limits.live().ack_bytes));
    let pending = Arc::new(Mutex::new(BTreeMap::new()));
    #[cfg(test)]
    let write_poll_probe = Arc::new(TestWritePollProbe::default());
    #[cfg(test)]
    let child_publication_gate = Arc::new(TestChildPublicationGate::default());
    #[cfg(test)]
    let post_terminal_cleanup_gate = Arc::new(TestChildPublicationGate::default());
    #[cfg(test)]
    let inbound_publication_gate = Arc::new(TestChildPublicationGate::default());
    #[cfg(test)]
    let progress_registration_gate = Arc::new(TestChildPublicationGate::default());
    #[cfg(test)]
    let delivery_writer_gate = Arc::new(TestChildPublicationGate::default());
    #[cfg(test)]
    let data_writer_gate = Arc::new(TestChildPublicationGate::default());
    #[cfg(test)]
    let panic_data_writer = Arc::new(AtomicBool::new(false));
    #[cfg(test)]
    let port_linearization_gate = Arc::new(TestPortLinearizationGate::default());
    let (cancel, cancel_rx) = watch::channel(false);
    let first_failure = Arc::new(ActorFirstFailure::new(session_clock.clone()));
    let terminal_stamps = ActorTerminalStampPair::mint(&first_failure);
    #[cfg(test)]
    let root_outer_error = Arc::new(AtomicBool::new(false));
    // Tokio's timeout polls its inner future first. Recheck at the exact
    // publication seam so a fully buffered handshake cannot mint an actor
    // after the absolute bootstrap deadline.
    ensure_bootstrap_deadline(publication_deadline)?;
    // All fallible prepublication work is complete. Moving the exact drain
    // witness past this seam is the actor publication transaction: failure
    // retains it in bootstrap cleanup; success leaves no await or Result edge
    // before the root owns it.
    let (connection_drain, endpoint) = match endpoint {
        ActorBootstrapEndpointOwnership::Client(endpoint) => {
            match (connection_drain.take(), endpoint_driver_drain.take()) {
                (Some(connection_drain), Some(endpoint_driver_drain)) => (
                    connection_drain,
                    ActorEndpointOwnership::Client {
                        endpoint,
                        endpoint_driver_drain,
                    },
                ),
                (connection, endpoint_driver) => {
                    *connection_drain = connection;
                    *endpoint_driver_drain = endpoint_driver;
                    return Err(QuinnLegActorError::BootstrapConsumed);
                }
            }
        }
        ActorBootstrapEndpointOwnership::Server => (
            connection_drain
                .take()
                .ok_or(QuinnLegActorError::BootstrapConsumed)?,
            ActorEndpointOwnership::Server,
        ),
    };
    let server_registration = server_reservation.map(AcceptReservation::publish);

    let root_guard = server_registration
        .as_ref()
        .and_then(ServerActorRegistration::task_guard);
    let child_guards: [Option<TaskTerminationGuard>; FIXED_TASK_GUARDS_PER_ACTOR - 1] =
        std::array::from_fn(|_| {
            server_registration
                .as_ref()
                .and_then(ServerActorRegistration::task_guard)
        });
    let mut nursery = JoinSet::new();
    // Construct the exit guard before the root future is spawned. Tokio may
    // abort a task before its first poll; keeping the guard in the future's
    // captured arguments still publishes failure and wakes every port.
    let root_exit_signal = ActorRootExitSignal::new(
        cancel.clone(),
        Arc::clone(&first_failure),
        readiness.clone(),
    );
    let root_abort = nursery.spawn(actor_root_with_exit_signal(
        actor_root(
            connection,
            endpoint,
            connection_drain,
            role,
            leg,
            limits.max_frame_bytes(),
            streams,
            outbound_rx,
            inbound_tx,
            inbound_handoff_rx,
            delivery_tx,
            delivery_rx,
            inbound_bytes,
            composition_messages,
            ack_messages,
            ack_bytes,
            Arc::clone(&pending),
            Arc::clone(&counters),
            readiness.clone(),
            Arc::clone(&inbound_publications),
            session_clock.clone(),
            #[cfg(test)]
            Arc::clone(&write_poll_probe),
            #[cfg(test)]
            Arc::clone(&child_publication_gate),
            #[cfg(test)]
            Arc::clone(&post_terminal_cleanup_gate),
            #[cfg(test)]
            Arc::clone(&inbound_publication_gate),
            #[cfg(test)]
            Arc::clone(&data_writer_gate),
            #[cfg(test)]
            Arc::clone(&panic_data_writer),
            #[cfg(test)]
            Arc::clone(&delivery_writer_gate),
            cancel.clone(),
            cancel_rx,
            Arc::clone(&first_failure),
            terminal_stamps,
            #[cfg(test)]
            Arc::clone(&root_outer_error),
            reporter,
            reported_terminal_tx,
            root_guard,
            child_guards,
        ),
        root_exit_signal,
    ));
    #[cfg(not(test))]
    drop(root_abort);

    Ok(StartedQuinnLegActor {
        session_clock,
        capacity,
        actor_incarnation: Some(actor_incarnation),
        io: Some(io),
        queue: Some(queue),
        controller_parts_transferred: false,
        transport: Some(ActorEncodedTransport {
            outbound: outbound_tx,
            message_capacity: outbound_messages,
            byte_capacity: outbound_bytes,
            counters: Arc::clone(&counters),
            next_ordinal: 1,
            expected_route: WireRoute::new(leg, role.outbound_direction()),
        }),
        inbound: Some(inbound_rx),
        inbound_handoff: Some(inbound_handoff),
        pending_controller_inbound: None,
        inbound_publications,
        inbound_gate: QuinnLegActorInboundGate::mint_for_started_actor(),
        cancel,
        first_failure,
        reported_terminal: ActorReportedTerminalState::Awaiting(reported_terminal_rx),
        readiness,
        counters,
        #[cfg(test)]
        pending_deliveries: pending,
        #[cfg(test)]
        write_poll_probe,
        #[cfg(test)]
        child_publication_gate,
        #[cfg(test)]
        post_terminal_cleanup_gate,
        #[cfg(test)]
        inbound_publication_gate,
        #[cfg(test)]
        progress_registration_gate,
        #[cfg(test)]
        delivery_writer_gate,
        #[cfg(test)]
        data_writer_gate,
        #[cfg(test)]
        panic_data_writer,
        #[cfg(test)]
        port_linearization_gate,
        nursery,
        #[cfg(test)]
        root_abort,
        observed_root: None,
        #[cfg(test)]
        root_outer_error,
        server_registration,
        phase: ActorOwnerPhase::Running,
    })
}

async fn actor_root_with_exit_signal<F>(
    root: F,
    mut exit_signal: ActorRootExitSignal,
) -> Result<ActorRootOutcome, Arc<QuinnLegActorError>>
where
    F: std::future::Future<Output = Result<ActorRootOutcome, Arc<QuinnLegActorError>>>,
{
    match root.await {
        Ok(outcome) => {
            exit_signal.disarm();
            Ok(outcome)
        }
        Err(error) => Err(exit_signal.publish_failure(error)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn actor_root(
    connection: Connection,
    endpoint: ActorEndpointOwnership,
    connection_drain: quinn::ConnectionDrain,
    role: ActorRole,
    leg: LegId,
    max_frame_bytes: usize,
    streams: ActorStreams,
    outbound_rx: mpsc::Receiver<OutboundRecord>,
    inbound_tx: mpsc::Sender<InboundRecord>,
    inbound_handoff: oneshot::Receiver<mpsc::Receiver<InboundRecord>>,
    delivery_tx: mpsc::Sender<DeliveryRecord>,
    delivery_rx: mpsc::Receiver<DeliveryRecord>,
    inbound_bytes: Arc<Semaphore>,
    composition_messages: Arc<Semaphore>,
    ack_messages: Arc<Semaphore>,
    ack_bytes: Arc<Semaphore>,
    pending: Arc<Mutex<BTreeMap<u64, PendingDelivery>>>,
    counters: Arc<ActorCounters>,
    readiness: Readiness,
    inbound_publications: Arc<InboundPublicationTracker>,
    session_clock: SessionMonotonicClock,
    #[cfg(test)] write_poll_probe: Arc<TestWritePollProbe>,
    #[cfg(test)] child_publication_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)] post_terminal_cleanup_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)] inbound_publication_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)] data_writer_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)] panic_data_writer: Arc<AtomicBool>,
    #[cfg(test)] delivery_writer_gate: Arc<TestChildPublicationGate>,
    cancel: watch::Sender<bool>,
    mut cancel_rx: watch::Receiver<bool>,
    first_failure: Arc<ActorFirstFailure>,
    terminal_stamps: ActorTerminalStampPair,
    #[cfg(test)] root_outer_error: Arc<AtomicBool>,
    reporter: LegTransportReporter,
    reported_terminal: oneshot::Sender<QuinnLegActorReportedTerminal>,
    _root_guard: Option<TaskTerminationGuard>,
    child_guards: [Option<TaskTerminationGuard>; FIXED_TASK_GUARDS_PER_ACTOR - 1],
) -> Result<ActorRootOutcome, Arc<QuinnLegActorError>> {
    let ActorTerminalStampPair {
        handoff: terminal_handoff_stamp,
        root_outcome: terminal_root_stamp,
    } = terminal_stamps;
    let ActorStreams {
        data_send,
        delivered_send,
        data_recv,
        delivered_recv,
    } = streams;
    let [
        data_rx_guard,
        data_tx_guard,
        delivered_rx_guard,
        delivered_tx_guard,
    ] = child_guards;
    let mut children = JoinSet::new();
    children.spawn(data_reader_task(
        data_recv,
        role,
        leg,
        max_frame_bytes,
        inbound_tx,
        delivery_tx,
        inbound_bytes,
        composition_messages,
        ack_messages,
        ack_bytes,
        Arc::clone(&counters),
        readiness.clone(),
        inbound_publications,
        session_clock,
        #[cfg(test)]
        inbound_publication_gate,
        cancel_rx.clone(),
        Arc::clone(&first_failure),
        cancel.clone(),
        data_rx_guard,
    ));
    children.spawn(data_writer_task(
        data_send,
        outbound_rx,
        Arc::clone(&pending),
        readiness.clone(),
        #[cfg(test)]
        Arc::clone(&write_poll_probe),
        #[cfg(test)]
        data_writer_gate,
        #[cfg(test)]
        panic_data_writer,
        cancel_rx.clone(),
        Arc::clone(&first_failure),
        cancel.clone(),
        data_tx_guard,
    ));
    children.spawn(delivery_reader_task(
        delivered_recv,
        Arc::clone(&pending),
        readiness.clone(),
        cancel_rx.clone(),
        Arc::clone(&first_failure),
        cancel.clone(),
        delivered_rx_guard,
    ));
    children.spawn(delivery_writer_task(
        delivered_send,
        delivery_rx,
        readiness.clone(),
        #[cfg(test)]
        write_poll_probe,
        #[cfg(test)]
        delivery_writer_gate,
        cancel_rx.clone(),
        Arc::clone(&first_failure),
        cancel.clone(),
        delivered_tx_guard,
    ));

    let mut fatal = first_failure.current();
    let mut structural_child_join_error: Option<Arc<QuinnLegActorError>> = None;
    let mut cancelling = *cancel_rx.borrow();
    while !children.is_empty() {
        if cancelling {
            let joined = children.join_next().await;
            #[cfg(test)]
            child_publication_gate.pause_if_armed().await;
            match joined {
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(cause))) => {
                    fatal.get_or_insert(cause);
                }
                Some(Err(error)) => {
                    let structural = Arc::new(QuinnLegActorError::TaskJoin(error.to_string()));
                    let cause = first_failure.record_shared(Arc::clone(&structural));
                    structural_child_join_error.get_or_insert(structural);
                    if let Some(cause) = cause {
                        fatal.get_or_insert(cause);
                    }
                }
                None => break,
            }
            readiness.advance_output().map_err(Arc::new)?;
            continue;
        }
        tokio::select! {
            biased;
            changed = cancel_rx.changed() => {
                cancelling = changed.is_err() || *cancel_rx.borrow();
                if cancelling && fatal.is_none() {
                    fatal = first_failure.current();
                }
            }
            joined = children.join_next() => {
                #[cfg(test)]
                child_publication_gate.pause_if_armed().await;
                let cause = match joined {
                    Some(Ok(Ok(()))) if *cancel_rx.borrow() => None,
                    Some(Ok(Ok(()))) => first_failure.record(
                        QuinnLegActorError::Protocol("actor child exited before cancellation"),
                    ),
                    Some(Ok(Err(cause))) => Some(cause),
                    Some(Err(error)) => {
                        let structural = Arc::new(QuinnLegActorError::TaskJoin(error.to_string()));
                        let cause = first_failure.record_shared(Arc::clone(&structural));
                        structural_child_join_error.get_or_insert(structural);
                        cause
                    }
                    None => first_failure.record(
                        QuinnLegActorError::Protocol("actor child set became empty before cancellation"),
                    ),
                };
                cancelling = true;
                if let Some(cause) = cause {
                    fatal = Some(cause);
                    cancel.send_replace(true);
                }
                readiness.advance_output().map_err(Arc::new)?;
            }
        }
    }
    let mut root_epilogue_error = None;
    match inbound_handoff.await {
        Ok(mut inbound) => {
            while let Some(record) = inbound.recv().await {
                drop(record);
            }
            drop(inbound);
        }
        Err(_) => {
            root_epilogue_error.get_or_insert_with(|| {
                Arc::new(QuinnLegActorError::Protocol(
                    "actor inbound receiver handoff closed before cancellation",
                ))
            });
        }
    }
    #[cfg(test)]
    if root_outer_error.load(Ordering::Acquire) {
        root_epilogue_error
            .get_or_insert_with(|| Arc::new(QuinnLegActorError::Protocol(TEST_ROOT_OUTER_ERROR)));
    }
    match pending.lock() {
        Ok(mut pending) => pending.clear(),
        Err(poisoned) => {
            poisoned.into_inner().clear();
            root_epilogue_error.get_or_insert_with(|| {
                Arc::new(QuinnLegActorError::Protocol(
                    "pending-delivery lock poisoned",
                ))
            });
        }
    }
    counters.zero.notify_waiters();
    counters.wait_zero().await;
    if fatal.is_none() {
        fatal = first_failure.current();
    }
    let terminal_reason = fatal.as_deref().map_or(
        LegTransportTerminalReason::LocalCancelled,
        QuinnLegActorError::terminal_reason,
    );
    let terminal_published = match reporter.report(terminal_reason) {
        Ok(terminal) => match reported_terminal.send(QuinnLegActorReportedTerminal::mint(
            terminal,
            terminal_handoff_stamp,
        )) {
            Ok(()) => true,
            Err(_reported_terminal) => {
                root_epilogue_error
                    .get_or_insert_with(|| Arc::new(QuinnLegActorError::TerminalHandoffClosed));
                false
            }
        },
        Err(error) => {
            drop(reported_terminal);
            root_epilogue_error
                .get_or_insert_with(|| Arc::new(QuinnLegActorError::Io(error.to_string())));
            false
        }
    };
    // Wake the composition after the exact terminal handoff attempt. A first
    // failure may already make ordinary actor-port waits return immediately;
    // this distinct monotonic publication lets the sole owner wait without a
    // retry loop until terminal state can actually be consumed.
    readiness.advance();
    #[cfg(test)]
    if terminal_published {
        post_terminal_cleanup_gate.pause_if_armed().await;
    }
    connection.close(CLOSE_CODE, b"actor cancelled");
    drop(connection);
    let connection_released = connection_drain.drained().await;
    let endpoint_driver_clean = match endpoint {
        ActorEndpointOwnership::Client {
            endpoint,
            endpoint_driver_drain,
        } => {
            endpoint.close(CLOSE_CODE, b"actor cancelled");
            let endpoint_idle = endpoint.wait_idle_or_driver_lost().await;
            drop(endpoint);
            let endpoint_driver_released = endpoint_driver_drain.drained().await;
            endpoint_idle && endpoint_driver_released
        }
        ActorEndpointOwnership::Server => true,
    };
    if !connection_released {
        return Err(Arc::new(QuinnLegActorError::EndpointReleaseLost));
    }
    if !endpoint_driver_clean {
        return Err(Arc::new(QuinnLegActorError::EndpointDriverLost));
    }
    if let Some(error) = structural_child_join_error {
        return Err(error);
    }
    if let Some(error) = root_epilogue_error {
        return Err(error);
    }
    if !terminal_published {
        return Err(Arc::new(QuinnLegActorError::Protocol(
            "actor terminal reporter failed without a root error",
        )));
    }
    Ok(ActorRootOutcome {
        failure: fatal,
        terminal_stamp: terminal_root_stamp,
    })
}

async fn wait_cancel(cancel: &mut watch::Receiver<bool>) {
    if *cancel.borrow() {
        return;
    }
    while cancel.changed().await.is_ok() {
        if *cancel.borrow() {
            return;
        }
    }
}

async fn read_record_or_cancel(
    stream: &mut RecvStream,
    bytes: &mut [u8],
    context: RecordReadContext,
    cancel: &mut watch::Receiver<bool>,
    #[cfg(test)] progress_gate: Option<&TestChildPublicationGate>,
) -> Result<bool, QuinnLegActorError> {
    let mut filled = 0;
    while filled < bytes.len() {
        let result = tokio::select! {
            biased;
            () = wait_cancel(cancel) => return Ok(false),
            result = stream.read(&mut bytes[filled..]) => result,
        };
        match result {
            Ok(Some(read)) if read > 0 && read <= bytes.len() - filled => {
                filled += read;
                #[cfg(test)]
                if let Some(progress_gate) = progress_gate {
                    progress_gate.pause_if_armed().await;
                }
            }
            Ok(Some(_)) => {
                return Err(QuinnLegActorError::Protocol(
                    "invalid actor record read progress",
                ));
            }
            Ok(None) => return Err(classify_record_read_end(context, filled, None)),
            Err(error) => {
                return Err(classify_record_read_end(context, filled, Some(error)));
            }
        }
    }
    Ok(true)
}

async fn acquire_one_or_cancel(
    semaphore: Arc<Semaphore>,
    cancel: &mut watch::Receiver<bool>,
) -> Result<Option<OwnedSemaphorePermit>, QuinnLegActorError> {
    tokio::select! {
        biased;
        () = wait_cancel(cancel) => Ok(None),
        result = semaphore.acquire_owned() => result
            .map(Some)
            .map_err(|_| QuinnLegActorError::Protocol("actor semaphore closed")),
    }
}

async fn acquire_many_or_cancel(
    semaphore: Arc<Semaphore>,
    units: u32,
    cancel: &mut watch::Receiver<bool>,
) -> Result<Option<OwnedSemaphorePermit>, QuinnLegActorError> {
    tokio::select! {
        biased;
        () = wait_cancel(cancel) => Ok(None),
        result = semaphore.acquire_many_owned(units) => result
            .map(Some)
            .map_err(|_| QuinnLegActorError::Protocol("actor semaphore closed")),
    }
}

/// Publish before caller-owned resources drop; panic uses the root fallback.
fn finish_fixed_child(
    result: Result<(), QuinnLegActorError>,
    readiness: &Readiness,
    first_failure: &ActorFirstFailure,
    cancel_signal: &watch::Sender<bool>,
) -> Result<(), Arc<QuinnLegActorError>> {
    let error = match result {
        Ok(()) => QuinnLegActorError::Protocol("actor child exited before cancellation"),
        Err(error) => error,
    };
    let result = match first_failure.record(error) {
        Some(cause) => {
            cancel_signal.send_replace(true);
            Err(cause)
        }
        None => Ok(()),
    };
    readiness.advance_output().map_err(Arc::new)?;
    result
}

#[allow(clippy::too_many_arguments)]
async fn data_reader_task(
    mut stream: RecvStream,
    role: ActorRole,
    leg: LegId,
    max_frame_bytes: usize,
    inbound: mpsc::Sender<InboundRecord>,
    delivery: mpsc::Sender<DeliveryRecord>,
    inbound_bytes: Arc<Semaphore>,
    composition_messages: Arc<Semaphore>,
    ack_messages: Arc<Semaphore>,
    ack_bytes: Arc<Semaphore>,
    counters: Arc<ActorCounters>,
    readiness: Readiness,
    inbound_publications: Arc<InboundPublicationTracker>,
    session_clock: SessionMonotonicClock,
    #[cfg(test)] inbound_publication_gate: Arc<TestChildPublicationGate>,
    mut cancel: watch::Receiver<bool>,
    first_failure: Arc<ActorFirstFailure>,
    cancel_signal: watch::Sender<bool>,
    guard: Option<TaskTerminationGuard>,
) -> Result<(), Arc<QuinnLegActorError>> {
    let result = async {
        let expected_route = WireRoute::new(leg, role.inbound_direction());
        loop {
        let mut encoded_header = [0_u8; RECORD_HEADER_BYTES];
        if !read_record_or_cancel(
            &mut stream,
            &mut encoded_header,
            RecordReadContext::RecordBoundary,
            &mut cancel,
            #[cfg(test)]
            None,
        )
        .await?
        {
            return Ok(());
        }
        let header = WireHeader::decode(encoded_header)?;
        if header.kind != RECORD_DATA
            || header.route != expected_route
            || header.payload_len > max_frame_bytes
        {
            return Err(QuinnLegActorError::Protocol("invalid inbound DATA header"));
        }
        counters
            .validated_data_headers
            .fetch_add(1, Ordering::AcqRel);
        counters
            .waiting_inbound_bytes
            .store(header.payload_len, Ordering::Release);
        let byte_units = u32::try_from(header.payload_len)
            .map_err(|_| QuinnLegActorError::Protocol("inbound DATA length overflow"))?;
        let Some(byte_permit) =
            acquire_many_or_cancel(Arc::clone(&inbound_bytes), byte_units, &mut cancel).await?
        else {
            counters.waiting_inbound_bytes.store(0, Ordering::Release);
            return Ok(());
        };
        counters.waiting_inbound_bytes.store(0, Ordering::Release);
        let byte_ownership = TrackedPermit::new(
            byte_permit,
            Arc::clone(&counters),
            CounterField::InboundBytes,
            header.payload_len,
        );
        let Some(composition_permit) =
            acquire_one_or_cancel(Arc::clone(&composition_messages), &mut cancel).await?
        else {
            return Ok(());
        };
        let composition_ownership = TrackedPermit::new(
            composition_permit,
            Arc::clone(&counters),
            CounterField::CompositionMessages,
            1,
        );
        let inbound_reserve = inbound.clone().reserve_owned();
        let inbound_permit = tokio::select! {
            biased;
            () = wait_cancel(&mut cancel) => return Ok(()),
            result = inbound_reserve => result
                .map_err(|_| QuinnLegActorError::Protocol("actor inbound receiver closed"))?,
        };
        let reserved_delivery = if header.ordered {
            let Some(ack_message_permit) =
                acquire_one_or_cancel(Arc::clone(&ack_messages), &mut cancel).await?
            else {
                return Ok(());
            };
            let ack_message_ownership = TrackedPermit::new(
                ack_message_permit,
                Arc::clone(&counters),
                CounterField::AckMessages,
                1,
            );
            let ack_units = u32::try_from(RECORD_HEADER_BYTES)
                .map_err(|_| QuinnLegActorError::InvalidConfig)?;
            let Some(ack_byte_permit) =
                acquire_many_or_cancel(Arc::clone(&ack_bytes), ack_units, &mut cancel).await?
            else {
                return Ok(());
            };
            let delivery_reserve = delivery.clone().reserve_owned();
            let delivery_permit = tokio::select! {
                biased;
                () = wait_cancel(&mut cancel) => return Ok(()),
                result = delivery_reserve => result
                    .map_err(|_| QuinnLegActorError::Protocol("actor delivery receiver closed"))?,
            };
            Some(ReservedDelivery {
                permit: delivery_permit,
                message_ownership: ack_message_ownership,
                byte_ownership: TrackedPermit::new(
                    ack_byte_permit,
                    Arc::clone(&counters),
                    CounterField::AckBytes,
                    RECORD_HEADER_BYTES,
                ),
                header: WireHeader::delivered(header),
            })
        } else {
            None
        };
        let mut payload = vec![0_u8; header.payload_len];
        if !read_record_or_cancel(
            &mut stream,
            &mut payload,
            RecordReadContext::RecordBody,
            &mut cancel,
            #[cfg(test)]
            None,
        )
        .await?
        {
            return Ok(());
        }
        let publication = inbound_publications
            .reserve_arrival(readiness.clone(), &session_clock)
            .map_err(QuinnLegActorError::Clock)?;
        let record = InboundRecord {
            header,
            bytes: Bytes::from(payload),
            publication_epoch: publication.epoch(),
            arrived_at: publication.arrived_at(),
            byte_ownership,
            composition_ownership,
            queued_marker: CountMarker::new(Arc::clone(&counters), MarkerField::QueuedInbound),
            delivery: reserved_delivery,
        };
        #[cfg(test)]
        inbound_publication_gate.pause_if_armed().await;
        let queued = first_failure
            .run_value_if_clear(move || inbound_permit.send(record))
            .is_ok();
        // Publish after either enqueue or first-failure suppression. This is
        // the wake paired with an Empty arbitration that observed the
        // reservation and returned its exact deadline token.
        drop(publication);
        if !queued {
            return Ok(());
        }
        }
    }
    .await;
    let result = finish_fixed_child(result, &readiness, &first_failure, &cancel_signal);
    drop((stream, inbound, delivery));
    drop((
        inbound_bytes,
        composition_messages,
        ack_messages,
        ack_bytes,
        counters,
        readiness,
        inbound_publications,
        session_clock,
    ));
    #[cfg(test)]
    drop(inbound_publication_gate);
    drop((cancel, first_failure, cancel_signal, guard));
    result
}

#[allow(
    clippy::too_many_arguments,
    reason = "fixed child retains each independently bounded resource through failure publication"
)]
async fn data_writer_task(
    mut stream: SendStream,
    mut outbound: mpsc::Receiver<OutboundRecord>,
    pending: Arc<Mutex<BTreeMap<u64, PendingDelivery>>>,
    readiness: Readiness,
    #[cfg(test)] write_poll_probe: Arc<TestWritePollProbe>,
    #[cfg(test)] data_writer_gate: Arc<TestChildPublicationGate>,
    #[cfg(test)] panic_on_next_record: Arc<AtomicBool>,
    mut cancel: watch::Receiver<bool>,
    first_failure: Arc<ActorFirstFailure>,
    cancel_signal: watch::Sender<bool>,
    guard: Option<TaskTerminationGuard>,
) -> Result<(), Arc<QuinnLegActorError>> {
    let result = async {
        loop {
        let record = tokio::select! {
            biased;
            () = wait_cancel(&mut cancel) => break,
            record = outbound.recv() => match record {
                Some(record) => record,
                None => return Err(QuinnLegActorError::Protocol("actor outbound sender closed")),
            },
        };
        #[cfg(test)]
        assert!(
            !panic_on_next_record.swap(false, Ordering::AcqRel),
            "fixed actor data-writer panic failpoint",
        );
        let OutboundRecord {
            header,
            bytes,
            completion,
            message_ownership,
            byte_ownership,
        } = record;
        #[cfg(test)]
        data_writer_gate.pause_if_armed().await;
        let header_bytes = header.encode()?;
        if let Some(completion) = completion {
            let displaced = pending
                .lock()
                .map_err(|_| QuinnLegActorError::Protocol("pending-delivery lock poisoned"))?
                .insert(
                    header.ordinal,
                    PendingDelivery {
                        route: header.route,
                        lane: header.lane,
                        completion,
                        _message_ownership: message_ownership,
                        _byte_ownership: byte_ownership,
                    },
                );
            if displaced.is_some() {
                return Err(QuinnLegActorError::Protocol("duplicate outbound ordinal"));
            }
            match write_all_or_cancel(
                &mut stream,
                &header_bytes,
                &mut cancel,
                #[cfg(test)]
                &write_poll_probe,
                #[cfg(test)]
                TestPendingWrite {
                    ordinal: header.ordinal,
                    kind: TestWriteKind::DataHeader,
                },
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => {
                    remove_pending_after_write_exit(&pending, header.ordinal)?;
                    return Ok(());
                }
                Err(error) => {
                    remove_pending_after_write_exit(&pending, header.ordinal)?;
                    return Err(error);
                }
            }
            match write_all_or_cancel(
                &mut stream,
                &bytes,
                &mut cancel,
                #[cfg(test)]
                &write_poll_probe,
                #[cfg(test)]
                TestPendingWrite {
                    ordinal: header.ordinal,
                    kind: TestWriteKind::DataPayload,
                },
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => {
                    remove_pending_after_write_exit(&pending, header.ordinal)?;
                    return Ok(());
                }
                Err(error) => {
                    remove_pending_after_write_exit(&pending, header.ordinal)?;
                    return Err(error);
                }
            }
        } else {
            if !write_all_or_cancel(
                &mut stream,
                &header_bytes,
                &mut cancel,
                #[cfg(test)]
                &write_poll_probe,
                #[cfg(test)]
                TestPendingWrite {
                    ordinal: header.ordinal,
                    kind: TestWriteKind::DataHeader,
                },
            )
            .await?
            {
                return Ok(());
            }
            if !write_all_or_cancel(
                &mut stream,
                &bytes,
                &mut cancel,
                #[cfg(test)]
                &write_poll_probe,
                #[cfg(test)]
                TestPendingWrite {
                    ordinal: header.ordinal,
                    kind: TestWriteKind::DataPayload,
                },
            )
            .await?
            {
                return Ok(());
            }
            drop((message_ownership, byte_ownership));
            readiness.advance_output()?;
        }
        }
        let _ = stream.finish();
        Ok(())
    }
    .await;
    let result = finish_fixed_child(result, &readiness, &first_failure, &cancel_signal);
    drop((stream, outbound, pending, readiness));
    #[cfg(test)]
    drop((write_poll_probe, data_writer_gate, panic_on_next_record));
    drop((cancel, first_failure, cancel_signal, guard));
    result
}

fn remove_pending_after_write_exit(
    pending: &Mutex<BTreeMap<u64, PendingDelivery>>,
    ordinal: u64,
) -> Result<(), QuinnLegActorError> {
    pending
        .lock()
        .map_err(|_| QuinnLegActorError::Protocol("pending-delivery lock poisoned"))?
        .remove(&ordinal);
    Ok(())
}

async fn write_all_or_cancel(
    stream: &mut SendStream,
    bytes: &[u8],
    cancel: &mut watch::Receiver<bool>,
    #[cfg(test)] write_poll_probe: &TestWritePollProbe,
    #[cfg(test)] pending_write: TestPendingWrite,
) -> Result<bool, QuinnLegActorError> {
    tokio::select! {
        biased;
        () = wait_cancel(cancel) => {
            let _ = stream.reset(CLOSE_CODE);
            Ok(false)
        }
        result = async {
            #[cfg(test)]
            {
                write_all_with_pending_probe(
                    stream,
                    bytes,
                    write_poll_probe,
                    pending_write,
                )
                .await
            }
            #[cfg(not(test))]
            {
                stream.write_all(bytes).await
            }
        } => {
            result.map_err(classify_write_error)?;
            Ok(true)
        }
    }
}

#[cfg(test)]
async fn write_all_with_pending_probe(
    stream: &mut SendStream,
    bytes: &[u8],
    write_poll_probe: &TestWritePollProbe,
    pending_write: TestPendingWrite,
) -> Result<(), quinn::WriteError> {
    let write = stream.write_all(bytes);
    let mut write = std::pin::pin!(write);
    std::future::poll_fn(|context| {
        let polled = std::future::Future::poll(write.as_mut(), context);
        if polled.is_pending() {
            write_poll_probe.record_pending(pending_write);
        }
        polled
    })
    .await
}

async fn delivery_reader_task(
    mut stream: RecvStream,
    pending: Arc<Mutex<BTreeMap<u64, PendingDelivery>>>,
    readiness: Readiness,
    mut cancel: watch::Receiver<bool>,
    first_failure: Arc<ActorFirstFailure>,
    cancel_signal: watch::Sender<bool>,
    guard: Option<TaskTerminationGuard>,
) -> Result<(), Arc<QuinnLegActorError>> {
    let result = async {
        loop {
            let mut encoded_header = [0_u8; RECORD_HEADER_BYTES];
            if !read_record_or_cancel(
                &mut stream,
                &mut encoded_header,
                RecordReadContext::RecordBoundary,
                &mut cancel,
                #[cfg(test)]
                None,
            )
            .await?
            {
                pending
                    .lock()
                    .map_err(|_| QuinnLegActorError::Protocol("pending-delivery lock poisoned"))?
                    .clear();
                return Ok(());
            }
            let header = WireHeader::decode(encoded_header)?;
            if header.kind != RECORD_DELIVERED {
                return Err(QuinnLegActorError::Protocol(
                    "DATA appeared on DELIVERED stream",
                ));
            }
            let completed = pending
                .lock()
                .map_err(|_| QuinnLegActorError::Protocol("pending-delivery lock poisoned"))?
                .remove(&header.ordinal)
                .ok_or(QuinnLegActorError::Protocol(
                    "unknown or duplicate DELIVERED ordinal",
                ))?;
            if completed.route != header.route || completed.lane != header.lane {
                return Err(QuinnLegActorError::Protocol(
                    "DELIVERED route or lane mismatch",
                ));
            }
            let PendingDelivery {
                completion,
                _message_ownership,
                _byte_ownership,
                ..
            } = completed;
            completion.mark_delivered();
            drop((_message_ownership, _byte_ownership));
            readiness.advance_output()?;
        }
    }
    .await;
    let result = finish_fixed_child(result, &readiness, &first_failure, &cancel_signal);
    drop((stream, pending, readiness));
    drop((cancel, first_failure, cancel_signal, guard));
    result
}

#[allow(
    clippy::too_many_arguments,
    reason = "test builds add a write probe to the fixed production child resources"
)]
async fn delivery_writer_task(
    mut stream: SendStream,
    mut delivery: mpsc::Receiver<DeliveryRecord>,
    readiness: Readiness,
    #[cfg(test)] write_poll_probe: Arc<TestWritePollProbe>,
    #[cfg(test)] delivery_writer_gate: Arc<TestChildPublicationGate>,
    mut cancel: watch::Receiver<bool>,
    first_failure: Arc<ActorFirstFailure>,
    cancel_signal: watch::Sender<bool>,
    guard: Option<TaskTerminationGuard>,
) -> Result<(), Arc<QuinnLegActorError>> {
    let result = async {
        loop {
        let record = tokio::select! {
            biased;
            () = wait_cancel(&mut cancel) => break,
            record = delivery.recv() => match record {
                Some(record) => record,
                None => return Err(QuinnLegActorError::Protocol("actor delivery sender closed")),
            },
        };
        #[cfg(test)]
        delivery_writer_gate.pause_if_armed().await;
        let encoded = record.header.encode()?;
        if !write_all_or_cancel(
            &mut stream,
            &encoded,
            &mut cancel,
            #[cfg(test)]
            &write_poll_probe,
            #[cfg(test)]
            TestPendingWrite {
                ordinal: record.header.ordinal,
                kind: TestWriteKind::DeliveredHeader,
            },
        )
        .await?
        {
            return Ok(());
        }
            drop(record);
        }
        let _ = stream.finish();
        Ok(())
    }
    .await;
    let result = finish_fixed_child(result, &readiness, &first_failure, &cancel_signal);
    drop((stream, delivery, readiness));
    #[cfg(test)]
    drop((write_poll_probe, delivery_writer_gate));
    drop((cancel, first_failure, cancel_signal, guard));
    result
}

#[derive(Debug, Error)]
pub(crate) enum QuinnLegActorError {
    #[error("invalid fixed Quinn leg actor configuration")]
    InvalidConfig,
    #[error("fixed Quinn leg actor I/O failed: {0}")]
    Io(String),
    #[error("fixed Quinn leg actor peer closed the transport")]
    PeerClosed,
    #[error("fixed Quinn leg actor transport was reset by the peer")]
    Reset,
    #[error("fixed Quinn leg actor handshake failed: {0}")]
    Handshake(String),
    #[error("fixed Quinn leg actor bootstrap exceeded its configured deadline")]
    BootstrapTimedOut,
    #[error("fixed Quinn leg actor exact liveness probe deadline expired")]
    ProbeDeadlineExpired,
    #[error("fixed Quinn leg actor bootstrap ownership was already consumed")]
    BootstrapConsumed,
    #[error("fixed Quinn endpoint release acknowledgement was lost")]
    EndpointReleaseLost,
    #[error("fixed Quinn endpoint driver exited before reaching idle")]
    EndpointDriverLost,
    #[error("fixed Quinn leg actor protocol violation: {0}")]
    Protocol(&'static str),
    #[error("fixed Quinn leg actor session clock failed: {0}")]
    Clock(#[from] SessionClockError),
    #[error("LegIo operation failed: {0}")]
    LegIo(#[from] LegIoError),
    #[error("fixed Quinn leg actor was already joined")]
    AlreadyJoined,
    #[error("fixed Quinn leg actor is closing")]
    Closing,
    #[error("fixed Quinn leg actor nursery lost its root")]
    MissingRoot,
    #[error("fixed Quinn leg actor lost its physical incarnation authority")]
    MissingPhysicalIncarnation,
    #[error("fixed Quinn leg actor nursery contained more than one root")]
    UnexpectedRootCount,
    #[error("fixed Quinn leg actor root task failed to join: {0}")]
    TaskJoin(String),
    #[error("fixed Quinn leg actor root failed: {0}")]
    RootFailed(Arc<QuinnLegActorError>),
    #[error("fixed Quinn leg actor retains {messages} messages and {bytes} bytes")]
    ResidualOwnership { messages: usize, bytes: usize },
    #[error("fixed Quinn listener actor registry is full")]
    ListenerRegistryFull,
    #[error("fixed Quinn listener is closed")]
    ListenerClosed,
    #[error("fixed Quinn listener shutdown is blocked by {live} live actors")]
    ListenerShutdownBlocked { live: usize },
    #[error("fixed Quinn listener observed an unclean actor termination")]
    ListenerShutdownUnclean,
    #[error("fixed Quinn actor readiness channel closed")]
    ReadinessClosed,
    #[error("fixed Quinn actor terminal handoff channel closed before publication")]
    TerminalHandoffClosed,
    #[error("fixed Quinn actor root completed before terminal handoff publication")]
    TerminalHandoffIncomplete,
}

impl QuinnLegActorError {
    const fn terminal_reason(&self) -> LegTransportTerminalReason {
        match self {
            Self::PeerClosed => LegTransportTerminalReason::PeerClosed,
            Self::Reset => LegTransportTerminalReason::Reset,
            _ => LegTransportTerminalReason::FatalIo,
        }
    }

    const fn is_transport_terminal(&self) -> bool {
        matches!(self, Self::PeerClosed | Self::Reset)
    }
}

fn classify_connection_error(error: quinn::ConnectionError) -> QuinnLegActorError {
    match error {
        quinn::ConnectionError::ApplicationClosed(_) => QuinnLegActorError::PeerClosed,
        quinn::ConnectionError::Reset => QuinnLegActorError::Reset,
        error => QuinnLegActorError::Io(error.to_string()),
    }
}

#[derive(Clone, Copy)]
enum RecordReadContext {
    RecordBoundary,
    RecordBody,
}

fn classify_record_read_end(
    context: RecordReadContext,
    filled: usize,
    error: Option<quinn::ReadError>,
) -> QuinnLegActorError {
    if filled != 0 || matches!(context, RecordReadContext::RecordBody) {
        return QuinnLegActorError::Protocol("truncated actor record");
    }
    match error {
        None => QuinnLegActorError::PeerClosed,
        Some(quinn::ReadError::Reset(_)) => QuinnLegActorError::Reset,
        Some(quinn::ReadError::ConnectionLost(error)) => classify_connection_error(error),
        Some(error) => QuinnLegActorError::Io(error.to_string()),
    }
}

fn classify_write_error(error: quinn::WriteError) -> QuinnLegActorError {
    match error {
        quinn::WriteError::Stopped(_) => QuinnLegActorError::Reset,
        quinn::WriteError::ConnectionLost(error) => classify_connection_error(error),
        error => QuinnLegActorError::Io(error.to_string()),
    }
}

#[cfg(test)]
mod tests;

//! The original per-poll ACK and actual-flush proof transaction. Captures
//! retain physical socket epoch/reset identity across IO; the original ledger
//! alone proves/refunds capabilities. No flow, DATA or retry authority here.
//! Entry points remain re-exported by downlink_custody; tests are here and in
//! client_tun/tests/local_egress_service/ordered_flush (real wire).
use super::*;
use bytes::BackingMeter;
use std::cell::Cell;
mod native;
#[cfg(test)]
pub(in crate::client_tun) use native::tests::arm_q_after_capture_admission;
pub(super) use native::{Owner, Slot};

/// Admission is refused before observing or changing inventory; Inventory is a
/// new failed physical proof. The real caller must keep these facts distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) enum CaptureRefusal {
    Admission(bytes::MeteredCopyError),
    IncompleteIo,
    Inventory,
}
impl std::fmt::Display for CaptureRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Admission(error) => write!(f, "Main capture admission: {error:?}"),
            Self::IncompleteIo => f.write_str("Main capture retains incomplete original IO"),
            Self::Inventory => f.write_str("Main capture custody is uncertain"),
        }
    }
}
impl std::error::Error for CaptureRefusal {}
impl CaptureRefusal {
    pub(in crate::client_tun) fn into_io(self) -> std::io::Error {
        std::io::Error::other(self)
    }
}

#[derive(Clone, Copy, Debug)]
pub(in crate::client_tun) struct AckCapture {
    handle: SocketHandle,
    epoch: u64,
    binding: Option<Binding>,
    range: Option<(u32, usize)>,
}
pub(in crate::client_tun) struct PollEgressCapture {
    ack: AckCapture,
    flush: Option<FlushCapture>,
    flushed_payload: Cell<bool>,
}

/// The one capture allocation for a physical poll/flush/ACK turn. The old
/// post-poll flush scan is replaced by this pre-poll candidate scan.
fn capture_poll_egress_legacy(
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
) -> Option<Vec<PollEgressCapture>> {
    let mut captures = Vec::new();
    for (handle, ctx) in contexts.iter() {
        if ctx.downlink_inflight_permit_bytes() == 0 || ctx.downlink_inflight_permits.quarantined()
        {
            continue;
        }
        if captures.try_reserve(1).is_err() {
            for ctx in contexts.values_mut() {
                if ctx.downlink_inflight_permit_bytes() > 0
                    && !ctx.downlink_inflight_permits.quarantined()
                {
                    ctx.downlink_inflight_permits
                        .fail("poll capture capacity unavailable");
                }
            }
            return None;
        }
        let socket = sockets.get::<TcpSocket>(*handle);
        captures.push(PollEgressCapture {
            ack: AckCapture {
                handle: *handle,
                epoch: ctx.conn_epoch,
                binding: Binding::read(ctx.conn_epoch, socket),
                range: socket.send_payload_range(),
            },
            flush: None,
            flushed_payload: Cell::new(false),
        });
    }
    Some(captures)
}
pub(in crate::client_tun) fn capture_dirty_send_queues(
    dirty: &HashSet<SocketHandle>,
    sockets: &SocketSet<'static>,
    contexts: &HashMap<SocketHandle, SocketCtx>,
) -> Vec<AckCapture> {
    dirty
        .iter()
        .filter_map(|handle| {
            let ctx = contexts.get(handle)?;
            if ctx.downlink_inflight_permits.quarantined() {
                return None;
            }
            let socket = sockets.get::<TcpSocket>(*handle);
            Some(AckCapture {
                handle: *handle,
                epoch: ctx.conn_epoch,
                binding: Binding::read(ctx.conn_epoch, socket),
                range: socket.send_payload_range(),
            })
        })
        .collect()
}
pub(in crate::client_tun) fn release_dirty_downlink_inflight_permits_for_send_queue_drain(
    before: &[AckCapture],
    sockets: &SocketSet<'static>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
) -> usize {
    release_captured_inflight_for_send_queue_drain(before.iter(), sockets, contexts)
}

fn release_captured_inflight_for_send_queue_drain<'a>(
    before: impl IntoIterator<Item = &'a AckCapture>,
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
) -> usize {
    let mut released = 0usize;
    for captured in before {
        let Some(ctx) = contexts.get_mut(&captured.handle) else {
            continue;
        };
        if ctx.downlink_inflight_permit_bytes() == 0 || ctx.downlink_inflight_permits.quarantined()
        {
            continue;
        }
        let socket = sockets.get::<TcpSocket>(captured.handle);
        if captured.epoch == ctx.conn_epoch
            && captured.binding.is_some()
            && Binding::read(ctx.conn_epoch, socket).is_none()
            && retains_dead_ordered_inflight(captured.handle, ctx, socket)
        {
            continue;
        }
        if captured.epoch != ctx.conn_epoch
            || captured.binding != Binding::read(ctx.conn_epoch, socket)
        {
            ctx.downlink_inflight_permits
                .fail("ACK capture crossed physical epoch");
            continue;
        }
        if let (Some((head, len)), Some((after, remaining))) =
            (captured.range, socket.send_payload_range())
        {
            let growth = after
                .wrapping_add(remaining as u32)
                .wrapping_sub(head.wrapping_add(len as u32));
            let advance = after.wrapping_sub(head);
            if growth >= (1u32 << 31)
                || advance >= (1u32 << 31)
                || u64::from(advance) > len as u64 + u64::from(growth)
            {
                ctx.downlink_inflight_permits
                    .fail("ambiguous ACK capture progression");
                continue;
            }
            if ctx
                .downlink_inflight_permits
                .prove_ack(ctx.conn_epoch, socket)
            {
                released += ctx.downlink_inflight_permits.refund_proved(
                    #[cfg(all(test, feature = "harness"))]
                    ledger::EgressRefundSource::AckDrain,
                );
            }
        }
    }
    released
}

fn release_poll_egress_ack_legacy(
    captures: &[PollEgressCapture],
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
) -> usize {
    release_captured_inflight_for_send_queue_drain(
        captures
            .iter()
            .filter(|capture| !capture.flushed_payload.get())
            .map(|capture| &capture.ack),
        sockets,
        contexts,
    )
}

#[cfg(all(test, feature = "harness"))]
fn test_poll_lost_binding_legacy(
    captures: &[PollEgressCapture],
    handle: SocketHandle,
    epoch: u64,
    sockets: &SocketSet<'_>,
) -> bool {
    captures.iter().any(|capture| {
        capture.ack.handle == handle
            && capture.ack.epoch == epoch
            && capture.ack.binding.is_some()
            && !capture.flushed_payload.get()
            && Binding::read(epoch, sockets.get::<TcpSocket>(handle)).is_none()
    })
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(in crate::client_tun) struct TunFlushPermitRelease {
    pub(in crate::client_tun) flushed_tcp_payload_bytes: usize,
    pub(in crate::client_tun) released_inflight_permit_bytes: usize,
    pub(in crate::client_tun) refusal: Option<CaptureRefusal>,
}

fn endpoint_ipv4(endpoint: IpEndpoint) -> Option<(Ipv4Addr, u16)> {
    let std::net::IpAddr::V4(addr) = std::net::IpAddr::from(endpoint.addr) else {
        return None;
    };
    Some((addr, endpoint.port))
}

fn flushed_tcp_packet_matches_endpoint_pair(
    packet: TunFlushedTcpPacket,
    local: Option<IpEndpoint>,
    remote: Option<IpEndpoint>,
) -> bool {
    let Some((local_addr, local_port)) = local.and_then(endpoint_ipv4) else {
        return false;
    };
    let Some((remote_addr, remote_port)) = remote.and_then(endpoint_ipv4) else {
        return false;
    };
    packet.src_addr == local_addr
        && packet.src_port == local_port
        && packet.dst_addr == remote_addr
        && packet.dst_port == remote_port
}

#[derive(Debug)]
enum CaptureDisposition {
    Active,
    Reset(ResetProof),
}
#[derive(Debug)]
pub(in crate::client_tun) struct FlushCapture {
    handle: SocketHandle,
    binding: Binding,
    disposition: CaptureDisposition,
}
fn retains_dead_ordered_inflight(
    handle: SocketHandle,
    ctx: &mut SocketCtx,
    socket: &TcpSocket<'_>,
) -> bool {
    if ctx.local_reset.is_some()
        || socket.is_active()
        || socket.can_send()
        || Binding::read(ctx.conn_epoch, socket).is_some()
        || ctx.downlink_inflight_permits.quarantined()
    {
        return false;
    }
    let Some(mirror) = ctx.flow_mirror.as_ref() else {
        return false;
    };
    if !mirror.live_dead_socket_binding(handle, ctx.conn_epoch) {
        return false;
    }
    let bound = mirror
        .d16_queue()
        .zip(ctx.d16_downlink_queue.as_ref())
        .is_some_and(|(bound, actual)| {
            bound.same_queue(actual) && ctx.downlink_inflight_permits.validate_source(Some(bound))
        })
        || mirror
            .bound_egress_queue()
            .is_some_and(|queue| ctx.downlink_inflight_permits.validate_source(Some(queue)));
    bound
        && ctx
            .downlink_inflight_permits
            .validate_inflight(ctx.conn_epoch)
}
impl FlushCapture {
    fn matches(&self, ctx: &SocketCtx, socket: &TcpSocket<'_>) -> bool {
        ctx.conn_epoch == self.binding.epoch
            && match &self.disposition {
                CaptureDisposition::Active => ctx
                    .downlink_inflight_permits
                    .matches_socket(ctx.conn_epoch, socket),
                CaptureDisposition::Reset(proof) => proof.matches(ctx, socket),
            }
    }
}
// Same original per-batch inflight scan, now before the cancellable actual IO.
fn begin_flush_batch_legacy(
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
) -> Vec<FlushCapture> {
    contexts
        .iter_mut()
        .filter_map(|(handle, ctx)| begin_flush_for(*handle, sockets, ctx))
        .collect()
}
fn begin_flush_for(
    handle: SocketHandle,
    sockets: &SocketSet<'_>,
    ctx: &mut SocketCtx,
) -> Option<FlushCapture> {
    if ctx.downlink_inflight_permit_bytes() == 0 || ctx.downlink_inflight_permits.quarantined() {
        return None;
    }
    let socket = sockets.get::<TcpSocket>(handle);
    // No dead socket endpoint pair can witness a flush or ACK. Its original
    // bound capability stays charged until Root settlement.
    if retains_dead_ordered_inflight(handle, ctx, socket) {
        return None;
    }
    let (binding, disposition) = match ResetProof::capture(ctx, socket) {
        Ok(Some(proof)) => {
            let (epoch, local, remote) = proof.identity();
            let binding = ctx.downlink_inflight_permits.begin_bound_batch(Binding {
                epoch,
                local,
                remote,
            })?;
            (binding, CaptureDisposition::Reset(proof))
        }
        Ok(None) => (
            ctx.downlink_inflight_permits
                .begin_batch(ctx.conn_epoch, socket)?,
            CaptureDisposition::Active,
        ),
        Err(()) => {
            ctx.downlink_inflight_permits
                .fail("invalid/conflicting reset flush identity");
            return None;
        }
    };
    Some(FlushCapture {
        handle,
        binding,
        disposition,
    })
}

fn prepare_poll_egress_flush_legacy(
    captures: &mut [PollEgressCapture],
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
) {
    for capture in captures {
        if let Some(ctx) = contexts.get_mut(&capture.ack.handle) {
            capture.flush = begin_flush_for(capture.ack.handle, sockets, ctx);
        }
    }
}
fn finish_flush_batch_legacy<'p>(
    packets: impl Into<crate::device::FlushedTcpBatch<'p>>,
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
    captured: Vec<FlushCapture>,
    complete: bool,
) -> TunFlushPermitRelease {
    finish_flush_batch_with(
        packets.into(),
        sockets,
        contexts,
        &captured,
        complete,
        Some,
        |_| {},
    )
}

fn finish_poll_egress_flush_legacy<'p>(
    packets: impl Into<crate::device::FlushedTcpBatch<'p>>,
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
    captured: &[PollEgressCapture],
    complete: bool,
) -> TunFlushPermitRelease {
    finish_flush_batch_with(
        packets.into(),
        sockets,
        contexts,
        captured,
        complete,
        |capture| capture.flush.as_ref(),
        |capture| capture.flushed_payload.set(true),
    )
}

fn finish_flush_batch_with<'a, T>(
    mut packets: crate::device::FlushedTcpBatch<'_>,
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
    captured: &'a [T],
    complete: bool,
    get: impl Fn(&'a T) -> Option<&'a FlushCapture>,
    mark_payload: impl Fn(&T),
) -> TunFlushPermitRelease {
    let mut release = TunFlushPermitRelease::default();
    while let Some(packet) = packets.next() {
        release.flushed_tcp_payload_bytes = release
            .flushed_tcp_payload_bytes
            .saturating_add(packet.payload_bytes);
        let mut chosen: Option<(&T, &FlushCapture)> = None;
        let mut ambiguous = false;
        for entry in captured {
            let Some(candidate) = get(entry) else {
                continue;
            };
            if !flushed_tcp_packet_matches_endpoint_pair(
                packet,
                Some(candidate.binding.local),
                Some(candidate.binding.remote),
            ) {
                continue;
            }
            if packet.payload_bytes > 0 {
                mark_payload(entry);
            }
            if let Some((_, first)) = chosen {
                if !ambiguous {
                    if let Some(ctx) = contexts.get_mut(&first.handle) {
                        ctx.downlink_inflight_permits
                            .fail("ambiguous flush endpoint pair");
                    }
                    ambiguous = true;
                }
                if let Some(ctx) = contexts.get_mut(&candidate.handle) {
                    ctx.downlink_inflight_permits
                        .fail("ambiguous flush endpoint pair");
                }
            } else {
                chosen = Some((entry, candidate));
            }
        }
        if ambiguous {
            packets.quarantine();
            continue;
        }
        let Some((_, capture)) = chosen else {
            continue;
        };
        let Some(ctx) = contexts.get_mut(&capture.handle) else {
            continue;
        };
        if !capture.matches(ctx, sockets.get::<TcpSocket>(capture.handle)) {
            packets.quarantine();
            ctx.downlink_inflight_permits
                .fail("actual flush crossed physical epoch");
            continue;
        }
        if ctx
            .downlink_inflight_permits
            .prove_packet(packet.payload_start_seq, packet.payload_bytes)
        {
            release.released_inflight_permit_bytes += ctx.downlink_inflight_permits.refund_proved(
                #[cfg(all(test, feature = "harness"))]
                ledger::EgressRefundSource::PacketFlush,
            );
        }
        if ctx.downlink_inflight_permits.fault().is_some() {
            packets.quarantine();
        }
    }
    for capture in captured.iter().filter_map(get) {
        if let Some(ctx) = contexts.get_mut(&capture.handle) {
            let same = capture.matches(ctx, sockets.get::<TcpSocket>(capture.handle));
            let _ = ctx
                .downlink_inflight_permits
                .end_batch(capture.binding, complete && same);
            if ctx.downlink_inflight_permits.fault().is_some() {
                packets.quarantine();
            }
        }
    }
    packets.commit();
    release
}
#[cfg(test)]
pub(in crate::client_tun) fn release_downlink_inflight_permits_for_flushed_tcp_packets(
    packets: Vec<TunFlushedTcpPacket>,
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
) -> TunFlushPermitRelease {
    let captured = begin_flush_batch_legacy(sockets, contexts);
    finish_flush_batch_legacy(packets, sockets, contexts, captured, true)
}

#[cfg(test)]
mod tests;

pub(in crate::client_tun) enum PollBatch {
    Legacy(Vec<PollEgressCapture>),
    Native(native::Batch),
}
pub(in crate::client_tun) enum FlushBatch {
    Legacy(Vec<FlushCapture>),
    Native(native::Batch),
}
impl FlushBatch {
    #[cfg(test)]
    pub(in crate::client_tun) fn len(&self) -> usize {
        match self {
            Self::Legacy(v) => v.len(),
            Self::Native(b) => b.len(),
        }
    }
}
fn legacy_allowed(contexts: &HashMap<SocketHandle, SocketCtx>) -> bool {
    let valid = !contexts.values().any(storage::any_native);
    if !valid {
        for ctx in contexts.values() {
            ctx.downlink_pending.quarantine();
            ctx.downlink_inflight_permits.capture.quarantine();
        }
    }
    valid
}
pub(in crate::client_tun) fn capture_poll_egress(
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
    meter: Option<&BackingMeter>,
) -> Result<PollBatch, CaptureRefusal> {
    match meter {
        Some(m) => native::Batch::begin(m, sockets, contexts).map(PollBatch::Native),
        None if legacy_allowed(contexts) => capture_poll_egress_legacy(sockets, contexts)
            .map(PollBatch::Legacy)
            .ok_or(CaptureRefusal::Inventory),
        None => Err(CaptureRefusal::Inventory),
    }
}
pub(in crate::client_tun) fn prepare_poll_egress_flush(
    captured: &mut PollBatch,
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
) -> bool {
    match captured {
        PollBatch::Legacy(v) => {
            prepare_poll_egress_flush_legacy(v, sockets, contexts);
            true
        }
        PollBatch::Native(b) => b.prepare_flush(sockets, contexts),
    }
}
pub(in crate::client_tun) fn finish_poll_egress_flush<'p>(
    packets: impl Into<crate::device::FlushedTcpBatch<'p>>,
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
    captured: &mut PollBatch,
    complete: bool,
) -> TunFlushPermitRelease {
    match captured {
        PollBatch::Legacy(v) => {
            finish_poll_egress_flush_legacy(packets, sockets, contexts, v, complete)
        }
        PollBatch::Native(b) => b.finish(packets.into(), sockets, contexts, complete),
    }
}
pub(in crate::client_tun) fn poll_capture_refusal(captured: &PollBatch) -> Option<CaptureRefusal> {
    match captured {
        PollBatch::Native(b) => b.refusal(),
        PollBatch::Legacy(_) => None,
    }
}
pub(in crate::client_tun) fn release_poll_egress_ack(
    captured: &mut PollBatch,
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
) -> usize {
    match captured {
        PollBatch::Legacy(v) => release_poll_egress_ack_legacy(v, sockets, contexts),
        PollBatch::Native(b) => b.ack(sockets, contexts),
    }
}
pub(in crate::client_tun) fn begin_flush_batch(
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
    meter: Option<&BackingMeter>,
) -> Result<FlushBatch, CaptureRefusal> {
    match meter {
        Some(m) => {
            let mut b = native::Batch::begin(m, sockets, contexts)?;
            if !b.prepare_flush(sockets, contexts) {
                return Err(b.refusal().unwrap_or(CaptureRefusal::Inventory));
            }
            Ok(FlushBatch::Native(b))
        }
        None if legacy_allowed(contexts) => Ok(FlushBatch::Legacy(begin_flush_batch_legacy(
            sockets, contexts,
        ))),
        None => Err(CaptureRefusal::Inventory),
    }
}
pub(in crate::client_tun) fn finish_flush_batch<'p>(
    packets: impl Into<crate::device::FlushedTcpBatch<'p>>,
    sockets: &SocketSet<'_>,
    contexts: &mut HashMap<SocketHandle, SocketCtx>,
    captured: FlushBatch,
    complete: bool,
) -> TunFlushPermitRelease {
    match captured {
        FlushBatch::Legacy(v) => finish_flush_batch_legacy(packets, sockets, contexts, v, complete),
        FlushBatch::Native(mut b) => {
            let mut release = b.finish(packets.into(), sockets, contexts, complete);
            b.settle(contexts);
            release.refusal = b.refusal();
            release
        }
    }
}
#[cfg(all(test, feature = "harness"))]
pub(in crate::client_tun) fn test_poll_lost_binding(
    captured: &PollBatch,
    handle: SocketHandle,
    epoch: u64,
    sockets: &SocketSet<'_>,
    contexts: &HashMap<SocketHandle, SocketCtx>,
) -> bool {
    match captured {
        PollBatch::Legacy(v) => test_poll_lost_binding_legacy(v, handle, epoch, sockets),
        PollBatch::Native(b) => b.lost_binding(handle, epoch, sockets, contexts),
    }
}

//! Owns the complete original ordered Native source, pump and reader transaction.
//! Source selection and session publication remain with TCP open and FlowRoles.
//! Prepared construction starts no task. The session or original consumer owns
//! the original task through actual join and guarded terminal disposition.
//! Private owned-wake characterization lives in ordered_pump/tests/.
use super::*;
use crate::upstream::{NativePumpControl, PreparedNativePump};
mod cold;
mod custody;

#[async_trait::async_trait]
pub(in crate::tuic) trait TuicOrderedNativeChunkSource: Send + 'static {
    fn admission_quarantined(&self) -> bool {
        false
    }

    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        None
    }

    async fn read_ordered_native_chunk(
        &mut self,
        max_len: usize,
    ) -> io::Result<Option<NativeTcpChunk>>;
}

struct QuinnOrderedNativeChunkSource {
    recv: quinn::RecvStream,
    backing_meter: Option<bytes::BackingMeter>,
    next_offset: u64,
    chunks: cold::SourceSlots,
    pending_error: Option<io::Error>,
    pending_eof: bool,
    #[cfg(test)]
    copied_batches: usize,
}

pub(in crate::tuic) struct NativeOrderedSourcePreflight {
    backing_meter: Option<bytes::BackingMeter>,
    chunks: cold::SourceSlots,
    channel: (cold::Sender, cold::Receiver),
}

pub(in crate::tuic) fn preflight_native_ordered_source(
    recv: &quinn::RecvStream,
    expected: Option<&bytes::BackingMeter>,
) -> io::Result<NativeOrderedSourcePreflight> {
    let backing_meter = recv.receive_backing_meter();
    if let Some(expected) = expected {
        if !backing_meter
            .as_ref()
            .is_some_and(|actual| expected.same_authority(actual))
        {
            expected.quarantine();
            if let Some(actual) = &backing_meter {
                actual.quarantine();
            }
            return Err(io::Error::other(
                "Native ordered source backing authority mismatch",
            ));
        }
    }
    let chunks = cold::SourceSlots::prepare(backing_meter.as_ref())?;
    let channel = cold::channel(backing_meter.as_ref())?;
    Ok(NativeOrderedSourcePreflight {
        backing_meter,
        chunks,
        channel,
    })
}

impl QuinnOrderedNativeChunkSource {
    #[cfg(test)]
    fn new(recv: quinn::RecvStream, expected: Option<&bytes::BackingMeter>) -> io::Result<Self> {
        let preflight = preflight_native_ordered_source(&recv, expected)?;
        Ok(Self::preflighted(recv, preflight).0)
    }

    fn preflighted(
        recv: quinn::RecvStream,
        preflight: NativeOrderedSourcePreflight,
    ) -> (Self, (cold::Sender, cold::Receiver)) {
        #[cfg(test)]
        super::tests::native_prepared::created(recv.id().index());
        let NativeOrderedSourcePreflight {
            backing_meter: admitted,
            chunks,
            channel,
        } = preflight;
        let backing_meter = recv.receive_backing_meter();
        let valid = match (&admitted, &backing_meter) {
            (Some(a), Some(b)) => a.same_authority(b),
            (None, None) => true,
            _ => false,
        };
        let pending_error = if valid {
            None
        } else {
            if let Some(meter) = &admitted {
                meter.quarantine();
            }
            if let Some(meter) = &backing_meter {
                meter.quarantine();
            }
            Some(io::Error::other(
                "Native ordered preflight authority changed",
            ))
        };
        (
            Self {
                recv,
                backing_meter,
                next_offset: 0,
                chunks,
                pending_error,
                pending_eof: false,
                #[cfg(test)]
                copied_batches: 0,
            },
            channel,
        )
    }
}

pub(in crate::tuic) fn combine_ordered_native_chunks(
    next_offset: &mut u64,
    chunks: &[bytes::Bytes],
    meter: Option<&bytes::BackingMeter>,
) -> io::Result<Option<NativeTcpChunk>> {
    let total_len = chunks
        .iter()
        .try_fold(0usize, |total, chunk| total.checked_add(chunk.len()))
        .ok_or_else(|| io::Error::other("Native ordered batch byte count overflow"))?;
    if total_len == 0 {
        return Ok(None);
    }
    for chunk in chunks {
        let actual = chunk.backing_meter();
        let valid = match (meter, actual.as_ref()) {
            (Some(expected), Some(actual)) => expected.same_authority(actual),
            (None, None) => true,
            _ => false,
        };
        if !valid {
            if let Some(expected) = meter {
                expected.quarantine();
            }
            if let Some(actual) = actual {
                actual.quarantine();
            }
            return Err(io::Error::other(
                "Native ordered backing authority mismatch",
            ));
        }
    }
    let offset = *next_offset;
    let advanced = offset
        .checked_add(
            u64::try_from(total_len)
                .map_err(|_| io::Error::other("Native ordered offset overflow"))?,
        )
        .ok_or_else(|| io::Error::other("Native ordered offset overflow"))?;
    let bytes = if chunks.len() == 1 {
        chunks[0].clone()
    } else if let Some(meter) = meter {
        bytes::Bytes::copy_slices_metered(chunks, meter)
            .ok_or_else(|| io::Error::other("Native ordered copied backing charge failed"))?
    } else {
        let mut combined = bytes::BytesMut::with_capacity(total_len);
        for chunk in chunks {
            combined.extend_from_slice(chunk);
        }
        combined.freeze()
    };
    *next_offset = advanced;
    Ok(Some(NativeTcpChunk { offset, bytes }))
}

#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for QuinnOrderedNativeChunkSource {
    fn admission_quarantined(&self) -> bool {
        // The physical slots retain the admitted authority even when the actual
        // receive half is foreign or unmetered. This closes the reader before spawn.
        self.chunks.quarantined()
            || self
                .backing_meter
                .as_ref()
                .is_some_and(bytes::BackingMeter::is_quarantined)
    }

    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        self.backing_meter.clone()
    }

    async fn read_ordered_native_chunk(
        &mut self,
        max_len: usize,
    ) -> io::Result<Option<NativeTcpChunk>> {
        #[cfg(test)]
        super::tests::native_prepared::polled(self.recv.id().index());
        if max_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Native ordered zero byte reservation",
            ));
        }
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }
        if self.pending_eof {
            return Ok(None);
        }
        let mut remaining = max_len.min(TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES);
        let mut n = 0;
        // Hard Source already has an admitted downstream output owner. Hand
        // off an original view; a second intermediate copy cannot be funded
        // after consuming Quinn under Full. Soft/Legacy batching stays intact.
        let parts = if self.backing_meter.as_ref().is_some_and(|m| m.budget_limit().is_some()) { 1 } else { self.chunks.len() };
        while n < parts {
            // The first read may wait. Later reads only collect already-ready
            // chunks, retaining the original prompt partial-batch handoff.
            // Quinn read_chunk is cancel-safe and honors max_length; unlike
            // read_chunks it does not call chunks.next(usize::MAX).
            let read = if n == 0 {
                Some(self.recv.read_chunk(remaining, true).await)
            } else {
                let mut next = std::pin::pin!(self.recv.read_chunk(remaining, true));
                match std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(std::future::Future::poll(next.as_mut(), cx))
                })
                .await
                {
                    std::task::Poll::Ready(result) => Some(result),
                    std::task::Poll::Pending => None,
                }
            };
            match read {
                Some(Ok(Some(chunk))) => {
                    let Some(left) = remaining.checked_sub(chunk.bytes.len()) else {
                        return Err(io::Error::other(
                            "Native ordered source exceeded byte reservation",
                        ));
                    };
                    remaining = left;
                    self.chunks[n] = chunk.bytes;
                    n += 1;
                    if remaining == 0 {
                        break;
                    }
                }
                Some(Ok(None)) if n > 0 => {
                    self.pending_eof = true;
                    break;
                }
                None if n > 0 => break,
                Some(Ok(None)) | None => return Ok(None),
                Some(Err(error)) if n > 0 => {
                    self.pending_error = Some(io::Error::other(format!(
                        "tuic native ordered read: {error}"
                    )));
                    break;
                }
                Some(Err(error)) => {
                    return Err(io::Error::other(format!(
                        "tuic native ordered read: {error}"
                    )));
                }
            }
        }
        #[cfg(test)]
        super::tests::native_prepared::read(
            self.recv.id().index(),
            self.chunks[..n].iter().map(bytes::Bytes::len).sum(),
        );
        #[cfg(test)]
        if n > 1 {
            self.copied_batches += 1;
        }
        let result = match combine_ordered_native_chunks(
            &mut self.next_offset,
            &self.chunks[..n],
            self.backing_meter.as_ref(),
        ) {
            Ok(result) => result,
            Err(error) => return Err(self.pending_error.take().unwrap_or(error)),
        };
        // Keep the original batch outside its borrowed future. Normal
        // completion releases its inputs at the original handoff point.
        for chunk in &mut self.chunks[..n] {
            *chunk = bytes::Bytes::new();
        }
        Ok(result)
    }
}

pub(in crate::tuic) struct TuicNativeOrderedPumpReader {
    rx: Option<cold::Receiver>,
    backing_meter: Option<bytes::BackingMeter>,
    poll_in_flight: bool,
    shared: std::sync::Arc<custody::Shared>,
    received_items: usize,
    received_bytes: usize,
    forwarded_bytes: usize,
    tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Option<Connection>,
    pending_self_wake: PendingReadWake,
    pending_chunk: Option<NativeTcpChunk>,
    pending_eof: bool,
    pending_error: Option<io::Error>,
}

impl TuicNativeOrderedPumpReader {
    pub(in crate::tuic) fn prepare_preflighted(
        recv: quinn::RecvStream,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Connection,
        preflight: NativeOrderedSourcePreflight,
    ) -> (Self, PreparedNativePump) {
        let (source, channel) = QuinnOrderedNativeChunkSource::preflighted(recv, preflight);
        custody::prepare(source, lease, tcp_diag, Some(transport_conn), channel)
    }
    pub(in crate::tuic) fn prepare(
        recv: quinn::RecvStream,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Connection,
        expected_meter: Option<&bytes::BackingMeter>,
    ) -> io::Result<(Self, PreparedNativePump)> {
        let preflight = preflight_native_ordered_source(&recv, expected_meter)?;
        Ok(Self::prepare_preflighted(
            recv,
            lease,
            tcp_diag,
            transport_conn,
            preflight,
        ))
    }
    #[cfg(test)]
    pub(in crate::tuic) fn prepare_from_source<S: TuicOrderedNativeChunkSource>(
        source: S,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Option<Connection>,
    ) -> (Self, PreparedNativePump) {
        let channel = cold::channel(source.receive_backing_meter().as_ref()).unwrap();
        custody::prepare(source, lease, tcp_diag, transport_conn, channel)
    }
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Option<NativeTcpChunk>>>> {
        let Some(rx) = self.rx.as_mut() else {
            self.shared.bad();
            return Poll::Ready(None);
        };
        match rx.poll_recv(cx) {
            Poll::Ready(Some(item)) => {
                let bytes = match &item {
                    custody::Item::Data(c) => c.bytes.len(),
                    _ => 0,
                };
                let (Some(items), Some(total)) = (
                    self.received_items.checked_add(1),
                    self.received_bytes.checked_add(bytes),
                ) else {
                    self.shared.bad();
                    return Poll::Ready(Some(Err(io::Error::other(
                        "Native pump receive accounting exhausted",
                    ))));
                };
                self.received_items = items;
                self.received_bytes = total;
                Poll::Ready(Some(match item {
                    custody::Item::Data(chunk) => Ok(Some(chunk)),
                    custody::Item::Eof => Ok(None),
                    custody::Item::Error => Err(self.shared.reader_error()),
                }))
            }
            Poll::Ready(None) if self.shared.quarantined() => {
                Poll::Ready(Some(Err(self.shared.reader_error())))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn arm_pending_self_wake(&mut self, cx: &mut Context<'_>, now: Instant) {
        self.pending_self_wake.arm_and_poll(cx, now);
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_self_wake_armed();
        }
    }

    fn note_pending(&mut self, cx: &mut Context<'_>, now: Instant) {
        let transport = self
            .transport_conn
            .as_ref()
            .map(sample_tuic_stream_transport);
        let pending_cause =
            if let (Some(diag), Some(transport)) = (self.tcp_diag.as_ref(), transport) {
                classify_tuic_stream_pending_cause(Some(TuicStreamTransportPending {
                    sample: transport,
                    since_last_read: transport_delta(
                        diag.last_read_transport_sample.unwrap_or_default(),
                        transport,
                    ),
                    since_last_pending: transport_delta(
                        diag.last_pending_transport_sample.unwrap_or_default(),
                        transport,
                    ),
                }))
            } else {
                TuicTcpStreamPendingCause::NoTransportSample
            };
        if should_arm_tuic_stream_pending_self_wake(
            pending_cause,
            self.pending_self_wake.deadline(),
            now,
        ) {
            self.arm_pending_self_wake(cx, now);
        }
        if let Some(diag) = self.tcp_diag.as_mut()
            && let Some(event) = diag.note_pending_at(now, transport)
        {
            let meta = diag.meta.clone();
            println!(
                "{}",
                format_tuic_tcp_stream_pending_line(
                    &meta,
                    event.pending_gap_ms,
                    event.pending_polls,
                    event.polls,
                    event.max_poll_gap_ms,
                    event.rx_bytes,
                    event.reads,
                    event.pending_cause,
                    event.transport,
                    event.self_wake_armed,
                    event.self_wake_fired,
                )
            );
        }
    }

    fn note_chunk_read(&mut self, bytes: usize, now: Instant) {
        self.pending_self_wake.cancel();
        let transport = self
            .transport_conn
            .as_ref()
            .map(sample_tuic_stream_transport);
        if let Some(diag) = self.tcp_diag.as_mut() {
            let event = diag.note_read_at(bytes, now, transport);
            let meta = diag.meta.clone();
            if let Some(first_rx_ms) = event.first_rx_ms {
                println!(
                    "{}",
                    format_tuic_tcp_stream_first_rx_line(
                        &meta,
                        first_rx_ms,
                        event.read_bytes,
                        event.reads,
                    )
                );
            }
            if let Some(gap_ms) = event.gap_ms
                && gap_ms >= TUIC_TCP_STREAM_READ_GAP_LOG_MS
            {
                println!(
                    "{}",
                    format_tuic_tcp_stream_read_gap_line(
                        &meta,
                        gap_ms,
                        event.read_bytes,
                        event.reads,
                        event.rx_bytes,
                    )
                );
            }
        }
    }
}

impl TuicNativeOrderedPumpReader {
    fn poll_inner(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
        let now = Instant::now();
        if self.pending_self_wake.poll_fired(cx) {
            if let Some(diag) = self.tcp_diag.as_mut() {
                diag.note_self_wake_fired();
            }
        }
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_poll_at(now);
        }

        if let Some(err) = self.pending_error.take() {
            self.pending_self_wake.cancel();
            return Poll::Ready(Err(err));
        }
        if self.pending_eof {
            self.pending_self_wake.cancel();
            self.pending_eof = false;
            return Poll::Ready(Ok(None));
        }

        let max_len = max_len.max(1);
        let first = if let Some(chunk) = self.pending_chunk.take() {
            chunk
        } else {
            match self.poll_recv(cx) {
                Poll::Ready(Some(Ok(Some(chunk)))) => chunk,
                Poll::Ready(Some(Ok(None))) | Poll::Ready(None) => {
                    self.pending_self_wake.cancel();
                    return Poll::Ready(Ok(None));
                }
                Poll::Ready(Some(Err(err))) => {
                    self.pending_self_wake.cancel();
                    return Poll::Ready(Err(err));
                }
                Poll::Pending => {
                    self.note_pending(cx, now);
                    return Poll::Pending;
                }
            }
        };

        let hard = self.backing_meter.as_ref().is_some_and(|m| m.budget_limit().is_some());
        let offset = first.offset;
        let mut next_offset = offset;
        // A bounded stack list keeps the original backing for one part and
        // lets a copied batch use the existing one-allocation metered layout.
        let mut parts: [bytes::Bytes; TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS] =
            std::array::from_fn(|_| bytes::Bytes::new());
        let mut parts_len = 0;
        let mut combined_len = 0;
        let mut inspected = 0;
        let mut yielded_on_limit = false;
        let mut chunk = first;

        loop {
            inspected += 1;
            if !chunk.bytes.is_empty() {
                let actual = chunk.bytes.backing_meter();
                let valid = match (self.backing_meter.as_ref(), actual.as_ref()) {
                    (Some(expected), Some(actual)) => expected.same_authority(actual),
                    (None, None) => true,
                    _ => false,
                };
                if !valid {
                    if let Some(expected) = &self.backing_meter {
                        expected.quarantine();
                    }
                    if let Some(actual) = actual {
                        actual.quarantine();
                    }
                    self.shared.bad();
                    return Poll::Ready(Err(io::Error::other(
                        "Native reader chunk backing authority mismatch",
                    )));
                }
            }
            if !chunk.bytes.is_empty() && chunk.offset == next_offset {
                let remaining = max_len.saturating_sub(combined_len);
                let take = remaining.min(chunk.bytes.len());
                if take < chunk.bytes.len() {
                    parts[parts_len] = chunk.bytes.slice(..take);
                    self.pending_chunk = Some(NativeTcpChunk {
                        offset: chunk.offset.saturating_add(take as u64),
                        bytes: chunk.bytes.slice(take..),
                    });
                    parts_len += 1;
                    combined_len += take;
                    break;
                }
                parts[parts_len] = chunk.bytes;
                parts_len += 1;
                combined_len += take;
                next_offset = next_offset.saturating_add(take as u64);
            } else if !chunk.bytes.is_empty() {
                self.pending_chunk = Some(chunk);
                break;
            }

            // Preserve the original backing at the mailbox handoff as well.
            // Empty inputs still obey the same bounded inspection turn below.
            if combined_len >= max_len || parts_len == parts.len() || (hard && parts_len != 0) {
                break;
            }
            if inspected == TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS {
                yielded_on_limit = true;
                break;
            }

            match self.poll_recv(cx) {
                Poll::Ready(Some(Ok(Some(next)))) => {
                    chunk = next;
                }
                Poll::Ready(Some(Ok(None))) | Poll::Ready(None) => {
                    self.pending_eof = true;
                    break;
                }
                Poll::Ready(Some(Err(err))) => {
                    self.pending_error = Some(err);
                    break;
                }
                Poll::Pending => break,
            }
        }

        if combined_len == 0 {
            if yielded_on_limit {
                cx.waker().wake_by_ref();
            } else {
                self.note_pending(cx, now);
            }
            return Poll::Pending;
        }
        let bytes = if let Some(meter) = &self.backing_meter {
            if parts_len == 1 {
                std::mem::replace(&mut parts[0], bytes::Bytes::new())
            } else {
                let Some(copied) = bytes::Bytes::copy_slices_metered(&parts[..parts_len], meter)
                else {
                    self.shared.bad();
                    return Poll::Ready(Err(io::Error::other(
                        "Native reader copied backing charge failed",
                    )));
                };
                copied
            }
        } else {
            let mut combined = bytes::BytesMut::with_capacity(combined_len);
            for part in &parts[..parts_len] {
                combined.extend_from_slice(part);
            }
            combined.freeze()
        };
        let Some(total) = self.forwarded_bytes.checked_add(bytes.len()) else {
            self.shared.bad();
            return Poll::Ready(Err(io::Error::other(
                "Native pump forwarding accounting exhausted",
            )));
        };
        self.note_chunk_read(bytes.len(), now);
        self.forwarded_bytes = total;
        Poll::Ready(Ok(Some(NativeTcpChunk { offset, bytes })))
    }
}

impl NativeTcpReader for TuicNativeOrderedPumpReader {
    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        self.backing_meter.clone()
    }

    fn poll_read_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
        if self.poll_in_flight || self.shared.quarantined() {
            self.shared.bad();
            return Poll::Ready(Err(self.shared.reader_error()));
        }
        self.poll_in_flight = true;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.poll_inner(cx, max_len)
        })) {
            Ok(value) => {
                self.poll_in_flight = false;
                value
            }
            Err(payload) => {
                // The original Box precedes any destructor/wake. The complete
                // interrupted poll remains charged Q; it can never yield a receipt.
                self.shared.unwind(payload);
                Poll::Ready(Err(self.shared.reader_error()))
            }
        }
    }
}

impl Drop for TuicNativeOrderedPumpReader {
    fn drop(&mut self) {
        if self.poll_in_flight {
            self.shared.bad();
        }
        if let Some(rx) = self.rx.take() {
            self.shared.return_reader(custody::Returned {
                rx,
                pending: self.pending_chunk.take(),
                received_items: self.received_items,
                received_bytes: self.received_bytes,
                forwarded_bytes: self.forwarded_bytes,
            });
        }
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.pending_self_wake.cancel();
            drop(self.pending_error.take());
            if let Some(diag) = &self.tcp_diag {
                let snapshot = diag.close_snapshot();
                println!(
                    "{}",
                    format_tuic_tcp_stream_close_line(&diag.meta, &snapshot)
                );
            }
        })) {
            self.shared.unwind(payload);
        }
    }
}

#[cfg(test)]
mod tests;

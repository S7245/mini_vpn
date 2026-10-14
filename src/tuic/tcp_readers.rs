//! Native and tracked TUIC TCP readers and their existing stream adapters.
//! Owns reader progress, diagnostic state, pool leases, and the ordered-pump task.
//! Connection selection and lifecycle authority stay in the parent module.
//! Construction uses the native reader/writer constructors and `TrackedRelayStream`;
//! characterization tests remain in the parent's private `tests` module.
//! Pending wake timers belong to each reader; the ordered-pump DATA task is unchanged.
//! Timer ownership and terminal cancellation tests live in tcp_readers/tests/.

use super::{
    TUIC_TCP_DIRECT_ORDERED_READ_MAX_BYTES, TUIC_TCP_NATIVE_ORDERED_PUMP_CHANNEL_CHUNKS,
    TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS, TUIC_TCP_STREAM_PENDING_SELF_WAKE_MS,
    TUIC_TCP_STREAM_READ_GAP_LOG_MS, TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES,
    TcpOrderedReadPressureReader, TcpPoolSlotLease, TcpWritePressureAdapter,
    TcpWritePressureWriter, TuicStreamTransportPending, TuicTcpStartupWriter, TuicTcpStreamDiag,
    TuicTcpStreamPendingCause, classify_tuic_stream_pending_cause,
    format_tuic_tcp_stream_close_line, format_tuic_tcp_stream_first_rx_line,
    format_tuic_tcp_stream_pending_line, format_tuic_tcp_stream_read_gap_line,
    sample_tuic_stream_transport, should_arm_tuic_stream_pending_self_wake, transport_delta,
};
use crate::upstream::{NativeTcpChunk, NativeTcpReader};
use quinn::Connection;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc;

/// A lazily allocated, pinned timer slot owned by one reader. Clearing drops Sleep in place,
/// deregistering it while keeping the allocation for the next pending episode. No task or
/// separately copied deadline survives the reader; Tokio's TimerEntry is stored inline.
#[derive(Default)]
struct PendingReadWake {
    slot: Option<Pin<Box<Option<tokio::time::Sleep>>>>,
}

impl PendingReadWake {
    fn deadline(&self) -> Option<Instant> {
        self.slot
            .as_ref()
            .and_then(|slot| slot.as_ref().get_ref().as_ref())
            .map(|sleep| sleep.deadline().into_std())
    }

    fn arm_and_poll(&mut self, cx: &mut Context<'_>, now: Instant) {
        let deadline = now + Duration::from_millis(TUIC_TCP_STREAM_PENDING_SELF_WAKE_MS);
        let slot = self.slot.get_or_insert_with(|| Box::pin(None));
        slot.as_mut().set(Some(tokio::time::sleep_until(
            tokio::time::Instant::from_std(deadline),
        )));
        if let Some(sleep) = slot.as_mut().as_pin_mut()
            && sleep.poll(cx).is_ready()
        {
            // The caller's sampled deadline can already have elapsed; schedule its observation.
            cx.waker().wake_by_ref();
        }
    }

    fn poll_fired(&mut self, cx: &mut Context<'_>) -> bool {
        let fired = self
            .slot
            .as_mut()
            .and_then(|slot| slot.as_mut().as_pin_mut())
            .is_some_and(|sleep| sleep.poll(cx).is_ready());
        if fired {
            self.cancel();
        }
        fired
    }

    fn cancel(&mut self) {
        if let Some(slot) = self.slot.as_mut() {
            slot.as_mut().set(None);
        }
    }
}

pub(super) trait DirectOrderedNativeChunkRecv: Unpin + Send {
    fn poll_read_ordered_native_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>>;
}

pub(super) struct QuinnDirectOrderedNativeChunkRecv {
    recv: quinn::RecvStream,
}

impl QuinnDirectOrderedNativeChunkRecv {
    fn new(recv: quinn::RecvStream) -> Self {
        Self { recv }
    }
}

impl DirectOrderedNativeChunkRecv for QuinnDirectOrderedNativeChunkRecv {
    fn poll_read_ordered_native_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
        let poll = {
            // Quinn documents `read_chunk` as cancellation-safe. Polling one
            // short-lived future here keeps RecvStream ownership local and
            // introduces no payload task or message-count channel.
            let fut = self.recv.read_chunk(max_len, true);
            tokio::pin!(fut);
            fut.poll(cx)
        };
        match poll {
            Poll::Ready(Ok(Some(chunk))) => Poll::Ready(Ok(Some(NativeTcpChunk {
                offset: chunk.offset,
                bytes: chunk.bytes,
            }))),
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(None)),
            Poll::Ready(Err(err)) => Poll::Ready(Err(io::Error::other(format!(
                "tuic direct ordered read: {err}"
            )))),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub(super) struct TuicNativeOrderedReader<R = QuinnDirectOrderedNativeChunkRecv> {
    recv: R,
    tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Option<Connection>,
    next_offset: u64,
    pending_self_wake: PendingReadWake,
    _read_pressure: Option<TcpOrderedReadPressureReader>,
    _lease: TcpPoolSlotLease,
}

impl TuicNativeOrderedReader<QuinnDirectOrderedNativeChunkRecv> {
    pub(super) fn new(
        recv: quinn::RecvStream,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Connection,
        read_pressure: Option<TcpOrderedReadPressureReader>,
    ) -> Self {
        Self::from_recv(
            QuinnDirectOrderedNativeChunkRecv::new(recv),
            lease,
            tcp_diag,
            Some(transport_conn),
            read_pressure,
        )
    }
}

impl<R> TuicNativeOrderedReader<R> {
    pub(super) fn from_recv(
        recv: R,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Option<Connection>,
        read_pressure: Option<TcpOrderedReadPressureReader>,
    ) -> Self {
        Self {
            recv,
            tcp_diag,
            transport_conn,
            next_offset: 0,
            pending_self_wake: PendingReadWake::default(),
            _read_pressure: read_pressure,
            _lease: lease,
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

impl<R> NativeTcpReader for TuicNativeOrderedReader<R>
where
    R: DirectOrderedNativeChunkRecv,
{
    fn poll_read_chunk(
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

        let read_len = max_len.max(1).min(TUIC_TCP_DIRECT_ORDERED_READ_MAX_BYTES);
        let chunk = match self.recv.poll_read_ordered_native_chunk(cx, read_len) {
            Poll::Ready(Ok(chunk)) => chunk,
            Poll::Ready(Err(err)) => {
                self.pending_self_wake.cancel();
                return Poll::Ready(Err(err));
            }
            Poll::Pending => {
                self.note_pending(cx, now);
                return Poll::Pending;
            }
        };
        let Some(chunk) = chunk else {
            self.pending_self_wake.cancel();
            return Poll::Ready(Ok(None));
        };
        if chunk.bytes.len() > read_len {
            self.pending_self_wake.cancel();
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tuic direct ordered chunk exceeds max_len: bytes={} max_len={read_len}",
                    chunk.bytes.len()
                ),
            )));
        }
        if chunk.offset != self.next_offset {
            self.pending_self_wake.cancel();
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "tuic direct ordered offset discontinuity: expected={} actual={}",
                    self.next_offset, chunk.offset
                ),
            )));
        }

        self.next_offset = self.next_offset.saturating_add(chunk.bytes.len() as u64);
        if !chunk.bytes.is_empty() {
            self.note_chunk_read(chunk.bytes.len(), now);
        }
        Poll::Ready(Ok(Some(chunk)))
    }
}

impl<R> Drop for TuicNativeOrderedReader<R> {
    fn drop(&mut self) {
        if let Some(diag) = &self.tcp_diag {
            let snapshot = diag.close_snapshot();
            println!(
                "{}",
                format_tuic_tcp_stream_close_line(&diag.meta, &snapshot)
            );
        }
    }
}

#[async_trait::async_trait]
pub(super) trait TuicOrderedNativeChunkSource: Send + 'static {
    async fn read_ordered_native_chunk(
        &mut self,
        max_len: usize,
    ) -> io::Result<Option<NativeTcpChunk>>;
}

struct QuinnOrderedNativeChunkSource {
    recv: quinn::RecvStream,
    next_offset: u64,
}

impl QuinnOrderedNativeChunkSource {
    fn new(recv: quinn::RecvStream) -> Self {
        Self {
            recv,
            next_offset: 0,
        }
    }
}

pub(super) fn combine_ordered_native_chunks(
    next_offset: &mut u64,
    chunks: &[bytes::Bytes],
) -> Option<NativeTcpChunk> {
    let total_len = chunks.iter().map(bytes::Bytes::len).sum::<usize>();
    if total_len == 0 {
        return None;
    }
    let offset = *next_offset;
    *next_offset = next_offset.saturating_add(total_len as u64);
    if chunks.len() == 1 {
        return Some(NativeTcpChunk {
            offset,
            bytes: chunks[0].clone(),
        });
    }
    let mut combined = bytes::BytesMut::with_capacity(total_len);
    for chunk in chunks {
        combined.extend_from_slice(chunk);
    }
    Some(NativeTcpChunk {
        offset,
        bytes: combined.freeze(),
    })
}

#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for QuinnOrderedNativeChunkSource {
    async fn read_ordered_native_chunk(
        &mut self,
        _max_len: usize,
    ) -> io::Result<Option<NativeTcpChunk>> {
        let mut chunks = vec![bytes::Bytes::new(); TUIC_TCP_NATIVE_ORDERED_PUMP_READ_CHUNKS];
        match self.recv.read_chunks(&mut chunks).await {
            Ok(Some(n)) => Ok(combine_ordered_native_chunks(
                &mut self.next_offset,
                &chunks[..n],
            )),
            Ok(None) => Ok(None),
            Err(err) => Err(io::Error::other(format!("tuic native ordered read: {err}"))),
        }
    }
}

pub(super) struct TuicNativeOrderedPumpReader {
    rx: mpsc::Receiver<io::Result<Option<NativeTcpChunk>>>,
    task: tokio::task::JoinHandle<()>,
    tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Option<Connection>,
    pending_self_wake: PendingReadWake,
    pending_chunk: Option<NativeTcpChunk>,
    pending_eof: bool,
    pending_error: Option<io::Error>,
    _lease: TcpPoolSlotLease,
}

impl TuicNativeOrderedPumpReader {
    pub(super) fn spawn(
        recv: quinn::RecvStream,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Connection,
    ) -> Self {
        Self::spawn_from_source(
            QuinnOrderedNativeChunkSource::new(recv),
            lease,
            tcp_diag,
            Some(transport_conn),
        )
    }

    pub(super) fn spawn_from_source<S>(
        mut source: S,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Option<Connection>,
    ) -> Self
    where
        S: TuicOrderedNativeChunkSource,
    {
        let (tx, rx) = mpsc::channel::<io::Result<Option<NativeTcpChunk>>>(
            TUIC_TCP_NATIVE_ORDERED_PUMP_CHANNEL_CHUNKS,
        );
        let task = tokio::spawn(async move {
            loop {
                let result = source
                    .read_ordered_native_chunk(TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES)
                    .await;
                let terminal = !matches!(result, Ok(Some(_)));
                if tx.send(result).await.is_err() || terminal {
                    break;
                }
            }
        });
        Self {
            rx,
            task,
            tcp_diag,
            transport_conn,
            pending_self_wake: PendingReadWake::default(),
            pending_chunk: None,
            pending_eof: false,
            pending_error: None,
            _lease: lease,
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

impl NativeTcpReader for TuicNativeOrderedPumpReader {
    fn poll_read_chunk(
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
            match self.rx.poll_recv(cx) {
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

        let offset = first.offset;
        let mut next_offset = offset;
        let mut combined = bytes::BytesMut::with_capacity(max_len.min(first.bytes.len().max(1)));
        let mut chunk = first;

        loop {
            if !chunk.bytes.is_empty() && chunk.offset == next_offset {
                let remaining = max_len.saturating_sub(combined.len());
                let take = remaining.min(chunk.bytes.len());
                combined.extend_from_slice(&chunk.bytes[..take]);
                next_offset = next_offset.saturating_add(take as u64);
                if take < chunk.bytes.len() {
                    self.pending_chunk = Some(NativeTcpChunk {
                        offset: chunk.offset.saturating_add(take as u64),
                        bytes: chunk.bytes.slice(take..),
                    });
                    break;
                }
            } else if !chunk.bytes.is_empty() {
                self.pending_chunk = Some(chunk);
                break;
            }

            if combined.len() >= max_len {
                break;
            }

            match self.rx.poll_recv(cx) {
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

        if combined.is_empty() {
            self.note_pending(cx, now);
            return Poll::Pending;
        }
        let bytes = combined.freeze();
        self.note_chunk_read(bytes.len(), now);
        Poll::Ready(Ok(Some(NativeTcpChunk { offset, bytes })))
    }
}

impl Drop for TuicNativeOrderedPumpReader {
    fn drop(&mut self) {
        self.task.abort();
        if let Some(diag) = &self.tcp_diag {
            let snapshot = diag.close_snapshot();
            println!(
                "{}",
                format_tuic_tcp_stream_close_line(&diag.meta, &snapshot)
            );
        }
    }
}

pub(super) struct TuicNativeTcpReader {
    recv: quinn::RecvStream,
    tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Connection,
    pending_self_wake: PendingReadWake,
    _lease: TcpPoolSlotLease,
}

impl TuicNativeTcpReader {
    pub(super) fn new(
        recv: quinn::RecvStream,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Connection,
    ) -> Self {
        Self {
            recv,
            tcp_diag,
            transport_conn,
            pending_self_wake: PendingReadWake::default(),
            _lease: lease,
        }
    }

    fn read_error(err: quinn::ReadError) -> io::Error {
        io::Error::other(format!("tuic native unordered read: {err}"))
    }

    fn arm_pending_self_wake(&mut self, cx: &mut Context<'_>, now: Instant) {
        self.pending_self_wake.arm_and_poll(cx, now);
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_self_wake_armed();
        }
    }

    fn note_pending(&mut self, cx: &mut Context<'_>, now: Instant) {
        let transport = sample_tuic_stream_transport(&self.transport_conn);
        let pending_cause = if let Some(diag) = self.tcp_diag.as_ref() {
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
            && let Some(event) = diag.note_pending_at(now, Some(transport))
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
        let transport = sample_tuic_stream_transport(&self.transport_conn);
        if let Some(diag) = self.tcp_diag.as_mut() {
            let event = diag.note_read_at(bytes, now, Some(transport));
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

impl NativeTcpReader for TuicNativeTcpReader {
    fn poll_read_chunk(
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
        let read_len = max_len.max(1).min(TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES);
        let poll = {
            let fut = self.recv.read_chunk(read_len, false);
            tokio::pin!(fut);
            fut.poll(cx)
        };
        let chunk = match poll {
            Poll::Ready(Ok(chunk)) => chunk,
            Poll::Ready(Err(err)) => {
                self.pending_self_wake.cancel();
                return Poll::Ready(Err(Self::read_error(err)));
            }
            Poll::Pending => {
                self.note_pending(cx, now);
                return Poll::Pending;
            }
        };
        match chunk {
            Some(chunk) => {
                let bytes_len = chunk.bytes.len();
                if bytes_len > 0 {
                    self.note_chunk_read(bytes_len, now);
                }
                Poll::Ready(Ok(Some(NativeTcpChunk {
                    offset: chunk.offset,
                    bytes: chunk.bytes,
                })))
            }
            None => {
                self.pending_self_wake.cancel();
                Poll::Ready(Ok(None))
            }
        }
    }
}

impl Drop for TuicNativeTcpReader {
    fn drop(&mut self) {
        if let Some(diag) = &self.tcp_diag {
            let snapshot = diag.close_snapshot();
            println!(
                "{}",
                format_tuic_tcp_stream_close_line(&diag.meta, &snapshot)
            );
        }
    }
}

pub(super) struct TuicNativeTcpWriter {
    send: TcpWritePressureAdapter<TuicTcpStartupWriter<quinn::SendStream>>,
    _lease: TcpPoolSlotLease,
}

impl TuicNativeTcpWriter {
    pub(super) fn new(
        send: TuicTcpStartupWriter<quinn::SendStream>,
        lease: TcpPoolSlotLease,
        pressure: TcpWritePressureWriter,
        progress: quinn::SendStreamProgress,
    ) -> Self {
        Self {
            send: TcpWritePressureAdapter::new_with_progress(send, pressure, progress),
            _lease: lease,
        }
    }
}

impl AsyncWrite for TuicNativeTcpWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.send).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.send).poll_shutdown(cx)
    }
}

pub(super) struct TrackedRelayStream<S> {
    inner: S,
    pub(super) tcp_diag: Option<TuicTcpStreamDiag>,
    transport_conn: Option<Connection>,
    pending_self_wake: PendingReadWake,
    _lease: Option<TcpPoolSlotLease>,
}

impl<S> TrackedRelayStream<S> {
    #[cfg(test)]
    pub(super) fn new(
        inner: S,
        lease: TcpPoolSlotLease,
        tcp_diag: Option<TuicTcpStreamDiag>,
    ) -> Self {
        Self {
            inner,
            tcp_diag,
            transport_conn: None,
            pending_self_wake: PendingReadWake::default(),
            _lease: Some(lease),
        }
    }

    pub(super) fn new_with_transport(
        inner: S,
        lease: Option<TcpPoolSlotLease>,
        tcp_diag: Option<TuicTcpStreamDiag>,
        transport_conn: Connection,
    ) -> Self {
        Self {
            inner,
            tcp_diag,
            transport_conn: Some(transport_conn),
            pending_self_wake: PendingReadWake::default(),
            _lease: lease,
        }
    }

    fn arm_pending_self_wake(&mut self, cx: &mut Context<'_>, now: Instant) {
        self.pending_self_wake.arm_and_poll(cx, now);
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_self_wake_armed();
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TrackedRelayStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before_len = buf.filled().len();
        let had_capacity = buf.remaining() != 0;
        let now = Instant::now();
        if self.pending_self_wake.poll_fired(cx) {
            if let Some(diag) = self.tcp_diag.as_mut() {
                diag.note_self_wake_fired();
            }
        }
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_poll_at(now);
        }
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        match &poll {
            Poll::Ready(Ok(())) => {
                let read_bytes = buf.filled().len().saturating_sub(before_len);
                if read_bytes > 0 || had_capacity {
                    self.pending_self_wake.cancel();
                }
                let transport = self
                    .transport_conn
                    .as_ref()
                    .map(sample_tuic_stream_transport);
                if read_bytes > 0
                    && let Some(diag) = self.tcp_diag.as_mut()
                {
                    let event = diag.note_read_at(read_bytes, now, transport);
                    let meta = diag.meta.clone();
                    if let Some(first_rx_ms) = event.first_rx_ms {
                        println!(
                            "{}",
                            format_tuic_tcp_stream_first_rx_line(
                                &meta,
                                first_rx_ms,
                                event.read_bytes,
                                event.reads
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
                                event.rx_bytes
                            )
                        );
                    }
                }
            }
            Poll::Pending => {
                let transport = self
                    .transport_conn
                    .as_ref()
                    .map(sample_tuic_stream_transport);
                let pending_cause =
                    if let (Some(diag), Some(sample)) = (self.tcp_diag.as_ref(), transport) {
                        classify_tuic_stream_pending_cause(Some(TuicStreamTransportPending {
                            sample,
                            since_last_read: transport_delta(
                                diag.last_read_transport_sample.unwrap_or_default(),
                                sample,
                            ),
                            since_last_pending: transport_delta(
                                diag.last_pending_transport_sample.unwrap_or_default(),
                                sample,
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
            Poll::Ready(Err(_)) => self.pending_self_wake.cancel(),
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TrackedRelayStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<S> Drop for TrackedRelayStream<S> {
    fn drop(&mut self) {
        if let Some(diag) = &self.tcp_diag {
            let snapshot = diag.close_snapshot();
            println!(
                "{}",
                format_tuic_tcp_stream_close_line(&diag.meta, &snapshot)
            );
        }
    }
}

#[cfg(test)]
mod tests;

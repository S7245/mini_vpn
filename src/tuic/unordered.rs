//! Owns the original unordered QUIC receive assembler and its relay IO.
//! Reassembles into the original ReadBuf with the original 4MiB/64KiB bounds;
//! owns no provider, roles, lease, timer or Node settlement authority.
//! Entry: TuicChunkRelayStream::new. Characterizations and passive inventory
//! access live in tests; production fields remain private to this owner.
use super::{
    TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES, TUIC_TCP_UNORDERED_REASSEMBLY_MAX_BYTES,
    TUIC_TCP_UNORDERED_STAGING_LOG_MS, TuicTcpStartupWriter, TuicTcpStreamDiagMeta,
    format_tuic_tcp_unordered_staging_line,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Debug, Default)]
struct OrderedQuicChunkAssembler {
    next_offset: u64,
    chunks: BTreeMap<u64, bytes::Bytes>,
    buffered_bytes: usize,
    max_buffered_bytes: usize,
}

impl OrderedQuicChunkAssembler {
    fn next_offset(&self) -> u64 {
        self.next_offset
    }

    fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    fn buffered_chunks(&self) -> usize {
        self.chunks.len()
    }

    fn max_buffered_bytes(&self) -> usize {
        self.max_buffered_bytes
    }

    fn push_chunk(&mut self, mut offset: u64, mut bytes: bytes::Bytes) {
        if bytes.is_empty() {
            return;
        }
        let end = offset.saturating_add(bytes.len() as u64);
        if end <= self.next_offset {
            return;
        }
        if offset < self.next_offset {
            let trim = (self.next_offset - offset) as usize;
            bytes = bytes.slice(trim..);
            offset = self.next_offset;
        }
        if let Some(old) = self.chunks.get(&offset)
            && old.len() >= bytes.len()
        {
            return;
        }
        if let Some(old) = self.chunks.insert(offset, bytes.clone()) {
            self.buffered_bytes = self.buffered_bytes.saturating_sub(old.len());
        }
        self.buffered_bytes = self.buffered_bytes.saturating_add(bytes.len());
        self.max_buffered_bytes = self.max_buffered_bytes.max(self.buffered_bytes);
    }

    fn drain_into(&mut self, buf: &mut ReadBuf<'_>) -> usize {
        let before = buf.filled().len();
        while buf.remaining() > 0 {
            let Some((&offset, _)) = self.chunks.first_key_value() else {
                break;
            };
            if offset > self.next_offset {
                break;
            }
            let Some((_, mut bytes)) = self.chunks.remove_entry(&offset) else {
                break;
            };
            self.buffered_bytes = self.buffered_bytes.saturating_sub(bytes.len());
            if offset < self.next_offset {
                let trim = (self.next_offset - offset) as usize;
                if trim >= bytes.len() {
                    continue;
                }
                bytes = bytes.slice(trim..);
            }
            let n = bytes.len().min(buf.remaining());
            buf.put_slice(&bytes[..n]);
            self.next_offset = self.next_offset.saturating_add(n as u64);
            if n < bytes.len() {
                let rest = bytes.slice(n..);
                self.buffered_bytes = self.buffered_bytes.saturating_add(rest.len());
                self.chunks.insert(self.next_offset, rest);
            }
        }
        buf.filled().len().saturating_sub(before)
    }
}

pub(super) struct TuicChunkRelayStream {
    recv: quinn::RecvStream,
    send: TuicTcpStartupWriter<quinn::SendStream>,
    rx: OrderedQuicChunkAssembler,
    recv_eof: bool,
    diag_meta: Option<TuicTcpStreamDiagMeta>,
    last_staging_log_at: Option<Instant>,
    unordered_chunks: u64,
    unordered_bytes: u64,
    out_of_order_chunks: u64,
    max_gap_bytes: u64,
    staging_cap_hits: u64,
}

impl TuicChunkRelayStream {
    pub(super) fn new(
        recv: quinn::RecvStream,
        send: TuicTcpStartupWriter<quinn::SendStream>,
        diag_meta: Option<TuicTcpStreamDiagMeta>,
    ) -> Self {
        Self {
            recv,
            send,
            rx: OrderedQuicChunkAssembler::default(),
            recv_eof: false,
            diag_meta,
            last_staging_log_at: None,
            unordered_chunks: 0,
            unordered_bytes: 0,
            out_of_order_chunks: 0,
            max_gap_bytes: 0,
            staging_cap_hits: 0,
        }
    }

    fn finish_recv(&mut self) -> Poll<io::Result<()>> {
        if self.rx.buffered_bytes() != 0 {
            // Retain the original nonzero inventory for the caller's failure
            // settlement; a FIN cannot normalize an unresolved gap to clean.
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tuic unordered EOF with unresolved reassembly gap",
            )));
        }
        self.recv_eof = true;
        Poll::Ready(Ok(()))
    }

    fn read_error(err: quinn::ReadError) -> io::Error {
        io::Error::other(format!("tuic unordered read: {err}"))
    }

    fn note_staging_cap_hit(&mut self, now: Instant) {
        self.staging_cap_hits = self.staging_cap_hits.saturating_add(1);
        self.maybe_log_staging("cap", now, self.rx.next_offset(), 0, 0);
    }

    fn note_chunk_staged(&mut self, now: Instant, offset: u64, chunk_len: usize) {
        let expected = self.rx.next_offset();
        let gap = offset.saturating_sub(expected);
        self.unordered_chunks = self.unordered_chunks.saturating_add(1);
        self.unordered_bytes = self.unordered_bytes.saturating_add(chunk_len as u64);
        if gap > 0 {
            self.out_of_order_chunks = self.out_of_order_chunks.saturating_add(1);
            self.max_gap_bytes = self.max_gap_bytes.max(gap);
        }
        self.maybe_log_staging("chunk", now, offset, chunk_len, gap);
    }

    fn maybe_log_staging(
        &mut self,
        reason: &'static str,
        now: Instant,
        chunk_offset: u64,
        chunk_len: usize,
        gap_bytes: u64,
    ) {
        let should_log = reason == "cap" || gap_bytes > 0;
        if !should_log {
            return;
        }
        let Some(meta) = self.diag_meta.as_ref() else {
            return;
        };
        let rate_limited = self
            .last_staging_log_at
            .map(|last| {
                now.saturating_duration_since(last).as_millis() < TUIC_TCP_UNORDERED_STAGING_LOG_MS
            })
            .unwrap_or(false);
        if rate_limited {
            return;
        }
        self.last_staging_log_at = Some(now);
        println!(
            "{}",
            format_tuic_tcp_unordered_staging_line(
                meta,
                reason,
                self.rx.next_offset(),
                chunk_offset,
                chunk_len,
                self.rx.buffered_bytes(),
                self.rx.buffered_chunks(),
                self.rx.max_buffered_bytes(),
                self.unordered_chunks,
                self.unordered_bytes,
                self.out_of_order_chunks,
                self.max_gap_bytes,
                gap_bytes,
                self.staging_cap_hits,
                TUIC_TCP_UNORDERED_REASSEMBLY_MAX_BYTES,
            )
        );
    }
}

impl AsyncRead for TuicChunkRelayStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.rx.drain_into(buf) > 0 {
            return Poll::Ready(Ok(()));
        }
        if self.recv_eof {
            return Poll::Ready(Ok(()));
        }

        let mut polls = 0usize;
        loop {
            if self.rx.buffered_bytes() >= TUIC_TCP_UNORDERED_REASSEMBLY_MAX_BYTES {
                self.note_staging_cap_hit(Instant::now());
                let expected = self.rx.next_offset();
                let max_length = buf.remaining().min(TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES);
                match self.recv.poll_read_chunk_at(cx, expected, max_length) {
                    Poll::Ready(Ok(Some(chunk))) => {
                        // Quinn gates offset and length under its original lock.
                        // QUIC offsets are <2^62, so this bounded prefix cannot
                        // overflow next. No await, allocation or fallible step
                        // separates extraction from transfer to this ReadBuf.
                        let len = chunk.bytes.len();
                        buf.put_slice(&chunk.bytes);
                        self.rx.next_offset = expected.saturating_add(len as u64);
                        self.note_chunk_staged(Instant::now(), chunk.offset, len);
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Ok(None)) => return self.finish_recv(),
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(Self::read_error(err))),
                    Poll::Pending => return Poll::Pending,
                }
            }
            let read_len = TUIC_TCP_UNORDERED_CHUNK_READ_MAX_BYTES
                .min(TUIC_TCP_UNORDERED_REASSEMBLY_MAX_BYTES - self.rx.buffered_bytes());
            let chunk = {
                let fut = self.recv.read_chunk(read_len, false);
                tokio::pin!(fut);
                match fut.poll(cx) {
                    Poll::Ready(Ok(chunk)) => chunk,
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(Self::read_error(err))),
                    Poll::Pending => return Poll::Pending,
                }
            };
            match chunk {
                Some(chunk) => {
                    let now = Instant::now();
                    let offset = chunk.offset;
                    let chunk_len = chunk.bytes.len();
                    self.rx.push_chunk(offset, chunk.bytes);
                    self.note_chunk_staged(now, offset, chunk_len);
                    if self.rx.drain_into(buf) > 0 {
                        return Poll::Ready(Ok(()));
                    }
                }
                None => return self.finish_recv(),
            }
            polls += 1;
            if polls >= 32 {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
    }
}

impl AsyncWrite for TuicChunkRelayStream {
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

#[cfg(test)]
pub(super) mod tests;

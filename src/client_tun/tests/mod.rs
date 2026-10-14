use super::*;
use crate::dns;
use crate::upstream::NativeTcpReader;
use compatibility_lifecycle::{DnsInjectRecorder, NoopTestUpstream};
use smoltcp::iface::SocketSet;
use std::sync::atomic::{AtomicU64, AtomicUsize};

/// 刀8/刀9：上游选择器——reality→Reality、failover→Failover（大小写/空白不敏感）；
/// 其余（含 tuic/缺省/未知）→ Tuic（零回归，failover opt-in）。
#[test]
fn upstream_kind_selector() {
    assert_eq!(select_upstream_kind(Some("reality")), UpstreamKind::Reality);
    assert_eq!(
        select_upstream_kind(Some("  REALITY ")),
        UpstreamKind::Reality
    );
    assert_eq!(
        select_upstream_kind(Some("failover")),
        UpstreamKind::Failover
    );
    assert_eq!(
        select_upstream_kind(Some(" Failover ")),
        UpstreamKind::Failover
    );
    assert_eq!(select_upstream_kind(Some("tuic")), UpstreamKind::Tuic);
    assert_eq!(
        select_upstream_kind(None),
        UpstreamKind::Tuic,
        "缺省 → TUIC（failover opt-in）"
    );
    assert_eq!(
        select_upstream_kind(Some("bogus")),
        UpstreamKind::Tuic,
        "未知 → TUIC（零回归）"
    );
}

// ---- relay lifecycle timers ----
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

/// A fully-open idle stream whose termination belongs to its lifecycle owner.
struct IdleStream {
    shutdown_called: Arc<AtomicBool>,
}
impl tokio::io::AsyncRead for IdleStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}
impl tokio::io::AsyncWrite for IdleStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len())) // 上行 write 即成（活动 → 重置 idle）
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

struct RecordingWriteStream {
    shutdown_called: Arc<AtomicBool>,
    writes: Arc<Mutex<Vec<Vec<u8>>>>,
}
impl tokio::io::AsyncRead for RecordingWriteStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}
impl tokio::io::AsyncWrite for RecordingWriteStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.writes.lock().unwrap().push(buf.to_vec());
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

struct FlushGatedReadableStream {
    shutdown_called: Arc<AtomicBool>,
    flushed: Arc<AtomicBool>,
    read_waker: Arc<Mutex<Option<Waker>>>,
    writes: Arc<Mutex<Vec<Vec<u8>>>>,
    read_sent: bool,
}
impl tokio::io::AsyncRead for FlushGatedReadableStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.read_sent {
            return Poll::Pending;
        }
        if !self.flushed.load(Ordering::SeqCst) {
            *self.read_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        buf.put_slice(b"remote-after-flush");
        self.read_sent = true;
        Poll::Ready(Ok(()))
    }
}
impl tokio::io::AsyncWrite for FlushGatedReadableStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.writes.lock().unwrap().push(buf.to_vec());
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.flushed.store(true, Ordering::SeqCst);
        if let Some(waker) = self.read_waker.lock().unwrap().take() {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

struct BurstReadableStream {
    shutdown_called: Arc<AtomicBool>,
    chunks: std::collections::VecDeque<Vec<u8>>,
    eof_after_chunks: bool,
}
impl tokio::io::AsyncRead for BurstReadableStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let Some(mut chunk) = self.chunks.pop_front() else {
            if self.eof_after_chunks {
                return Poll::Ready(Ok(()));
            }
            return Poll::Pending;
        };
        let take = chunk.len().min(buf.remaining());
        if take == 0 {
            self.chunks.push_front(chunk);
            return Poll::Pending;
        }
        buf.put_slice(&chunk[..take]);
        if take < chunk.len() {
            chunk.drain(..take);
            self.chunks.push_front(chunk);
        }
        Poll::Ready(Ok(()))
    }
}
impl tokio::io::AsyncWrite for BurstReadableStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

struct BurstNativeReader {
    chunks: std::collections::VecDeque<NativeTcpChunk>,
    eof_after_chunks: bool,
}

impl NativeTcpReader for BurstNativeReader {
    fn poll_read_chunk(
        &mut self,
        _cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        let Some(mut chunk) = self.chunks.pop_front() else {
            if self.eof_after_chunks {
                return Poll::Ready(Ok(None));
            }
            return Poll::Pending;
        };
        let take = chunk.bytes.len().min(max_len.max(1));
        if take < chunk.bytes.len() {
            let rest = NativeTcpChunk {
                offset: chunk.offset.saturating_add(take as u64),
                bytes: chunk.bytes.slice(take..),
            };
            chunk.bytes = chunk.bytes.slice(..take);
            self.chunks.push_front(rest);
        }
        Poll::Ready(Ok(Some(chunk)))
    }
}

struct PendingD16NativeReader {
    entered: Arc<AtomicBool>,
}

impl NativeTcpReader for PendingD16NativeReader {
    fn poll_read_chunk(
        &mut self,
        _cx: &mut Context<'_>,
        _max_len: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        self.entered.store(true, Ordering::SeqCst);
        Poll::Pending
    }
}

struct PanickingD16NativeReader;

impl NativeTcpReader for PanickingD16NativeReader {
    fn poll_read_chunk(
        &mut self,
        _cx: &mut Context<'_>,
        _max_len: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        panic!("injected D16 reader task failure")
    }
}

struct WakeableD16NativeReader {
    ready: Arc<AtomicBool>,
    read_waker: Arc<Mutex<Option<Waker>>>,
    sent: bool,
}

impl NativeTcpReader for WakeableD16NativeReader {
    fn poll_read_chunk(
        &mut self,
        cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        if self.sent || !self.ready.load(Ordering::SeqCst) {
            *self.read_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let payload = Bytes::from_static(b"after-reservation-rearm");
        assert!(payload.len() <= max_len);
        self.sent = true;
        Poll::Ready(Ok(Some(NativeTcpChunk {
            offset: 0,
            bytes: payload,
        })))
    }
}

struct NoopNativeWriter {
    shutdown_called: Arc<AtomicBool>,
}

impl tokio::io::AsyncWrite for NoopNativeWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

struct CreditWakeReadableStream {
    shutdown_called: Arc<AtomicBool>,
    poll_count: Arc<AtomicU64>,
    ready: Arc<AtomicBool>,
    read_waker: Arc<Mutex<Option<std::task::Waker>>>,
    read_sent: bool,
}
impl tokio::io::AsyncRead for CreditWakeReadableStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.poll_count.fetch_add(1, Ordering::SeqCst);
        if !self.ready.load(Ordering::SeqCst) {
            *self.read_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        if !self.read_sent {
            buf.put_slice(b"after-egress-wake");
            self.read_sent = true;
            return Poll::Ready(Ok(()));
        }
        Poll::Ready(Ok(()))
    }
}
impl tokio::io::AsyncWrite for CreditWakeReadableStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

/// 一条上行写立即失败的 mock 上游流，用来锁住 remote_write_failed 也会通知主循环 rearm。
struct FailingWriteStream {
    shutdown_called: Arc<AtomicBool>,
}
impl tokio::io::AsyncRead for FailingWriteStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

/// A remote write that never completes, used to lock the stalled-write guard.
struct PendingWriteStream {
    shutdown_called: Arc<AtomicBool>,
}
impl tokio::io::AsyncRead for PendingWriteStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

struct StepwisePendingWriteStream {
    allowed_steps: Arc<AtomicUsize>,
    completed_steps: usize,
    write_waker: Arc<Mutex<Option<Waker>>>,
    shutdown_called: Arc<AtomicBool>,
}

impl tokio::io::AsyncRead for StepwisePendingWriteStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

/// 写侧 Pending 后读侧才产数据：锁住 relay 读写必须真正并发。
struct PendingWriteThenReadableStream {
    write_polled: Arc<AtomicBool>,
    shutdown_called: Arc<AtomicBool>,
    read_sent: bool,
}
impl tokio::io::AsyncRead for PendingWriteThenReadableStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.read_sent {
            return Poll::Pending;
        }
        if !self.write_polled.load(Ordering::SeqCst) {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        buf.put_slice(b"remote-progress");
        self.read_sent = true;
        Poll::Ready(Ok(()))
    }
}
impl tokio::io::AsyncWrite for PendingWriteThenReadableStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.write_polled.store(true, Ordering::SeqCst);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

/// 写半边 shutdown 后读侧才产数据：锁住本地 FIN 不应终止 remote-to-local 方向。
struct LocalFinishThenReadableStream {
    shutdown_called: Arc<AtomicBool>,
    read_sent: bool,
}
impl tokio::io::AsyncRead for LocalFinishThenReadableStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if !self.shutdown_called.load(Ordering::SeqCst) {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        if self.read_sent {
            return Poll::Ready(Ok(()));
        }
        buf.put_slice(b"after-local-finish");
        self.read_sent = true;
        Poll::Ready(Ok(()))
    }
}
impl tokio::io::AsyncWrite for LocalFinishThenReadableStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}
impl tokio::io::AsyncWrite for PendingWriteStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Pending
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}
impl tokio::io::AsyncWrite for StepwisePendingWriteStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if self.completed_steps < self.allowed_steps.load(Ordering::SeqCst) && !buf.is_empty() {
            self.completed_steps = self.completed_steps.saturating_add(1);
            return Poll::Ready(Ok(1));
        }
        *self.write_waker.lock().unwrap() = Some(cx.waker().clone());
        Poll::Pending
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}
impl tokio::io::AsyncWrite for FailingWriteStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "test write reset",
        )))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.shutdown_called.store(true, Ordering::SeqCst);
        Poll::Ready(Ok(()))
    }
}

fn mk_test_handle(sockets: &mut SocketSet<'static>) -> SocketHandle {
    sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }))
}

fn relay_read_credit_channel() -> (
    watch::Sender<RelayReadCredit>,
    watch::Receiver<RelayReadCredit>,
) {
    watch::channel(RelayReadCredit::default())
}

#[test]
fn relay_ready_batch_limit_tapers_with_global_rx_queue_pressure() {
    assert_eq!(
        relay_remote_ready_batch_byte_limit_for_queue(0, RELAY_CHANNEL_CAPACITY),
        RELAY_REMOTE_READ_BURST_MAX_BYTES
    );
    assert_eq!(
        relay_remote_ready_batch_byte_limit_for_queue(
            RELAY_CHANNEL_CAPACITY / 4,
            RELAY_CHANNEL_CAPACITY
        ),
        RELAY_REMOTE_READ_BURST_MAX_BYTES
    );

    let mid_limit = relay_remote_ready_batch_byte_limit_for_queue(
        RELAY_CHANNEL_CAPACITY / 2,
        RELAY_CHANNEL_CAPACITY,
    );
    assert!(
        mid_limit > RELAY_REMOTE_READ_MIN_BATCH_BYTES
            && mid_limit < RELAY_REMOTE_READ_BURST_MAX_BYTES,
        "middle queue pressure should reduce but not clamp the read batch: {mid_limit}"
    );
    assert_eq!(
        relay_remote_ready_batch_byte_limit_for_queue(
            RELAY_CHANNEL_CAPACITY * 3 / 4,
            RELAY_CHANNEL_CAPACITY
        ),
        RELAY_REMOTE_READ_MIN_BATCH_BYTES
    );
    assert_eq!(
        relay_remote_ready_batch_byte_limit_for_queue(
            RELAY_CHANNEL_CAPACITY,
            RELAY_CHANNEL_CAPACITY
        ),
        0
    );
}

#[test]
fn relay_initial_read_credit_keeps_quic_receive_window_open() {
    let initial = RelayReadCredit::initial();

    assert!(!initial.paused);
    assert_eq!(initial.max_batch_bytes, RELAY_REMOTE_READ_BURST_MAX_BYTES);
    assert!(
        initial.max_batch_bytes >= RELAY_REMOTE_READ_BURST_MAX_BYTES,
        "relay startup must keep QUIC receive cadence healthy; local dispatch is segmented separately"
    );
}

#[test]
fn relay_read_credit_tapers_and_pauses_for_local_egress() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    assert_eq!(
        relay_read_credit_for_local_egress(0, 0, false, false, cfg, tun_mtu),
        RelayReadCredit {
            paused: false,
            max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
        }
    );
    assert_eq!(
        relay_read_credit_for_local_egress(cfg.high_bytes, 0, false, false, cfg, tun_mtu),
        RelayReadCredit {
            paused: false,
            max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
        }
    );

    let mid = relay_read_credit_for_local_egress(
        (cfg.high_bytes + tx_queue_flush_threshold(cfg)) / 2,
        0,
        false,
        false,
        cfg,
        tun_mtu,
    );
    assert!(!mid.paused);
    assert!(
        mid.max_batch_bytes > pressure_floor
            && mid.max_batch_bytes < RELAY_REMOTE_READ_BURST_MAX_BYTES,
        "mid-credit send queue should taper read credit: {mid:?}"
    );

    assert_eq!(
        relay_read_credit_for_local_egress(
            tx_queue_flush_threshold(cfg),
            0,
            false,
            false,
            cfg,
            tun_mtu,
        ),
        RelayReadCredit {
            paused: false,
            max_batch_bytes: ack_drain_floor,
        }
    );
    assert_eq!(
        relay_read_credit_for_local_egress(
            tx_queue_credit_spend_threshold(cfg),
            0,
            false,
            false,
            cfg,
            tun_mtu,
        ),
        RelayReadCredit {
            paused: false,
            max_batch_bytes: ack_drain_floor,
        }
    );
    assert_eq!(
        relay_read_credit_for_local_egress(0, cfg.high_bytes, false, false, cfg, tun_mtu),
        RelayReadCredit {
            paused: false,
            max_batch_bytes: pressure_floor,
        }
    );
    assert_eq!(
        relay_read_credit_for_local_egress(0, 0, true, false, cfg, tun_mtu),
        RelayReadCredit {
            paused: false,
            max_batch_bytes: pressure_floor,
        },
        "active egress debt should keep QUIC reads draining with minimum credit, not hard-pause them"
    );
    assert_eq!(
        relay_read_credit_for_local_egress(
            0,
            downlink_receive_window_high(cfg),
            false,
            false,
            cfg,
            tun_mtu,
        ),
        RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        }
    );
    assert_eq!(
        relay_read_credit_for_local_egress(0, 0, false, true, cfg, tun_mtu),
        RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        }
    );
}

#[test]
fn relay_pressure_floor_tracks_tun_mtu_below_legacy_min_batch() {
    let pressure_floor = relay_remote_read_pressure_floor_bytes(1200);
    let mut controller = StreamServiceController::new();

    assert_eq!(
        pressure_floor,
        1200 * RELAY_REMOTE_READ_PRESSURE_MIN_PACKETS
    );
    assert!(pressure_floor < RELAY_REMOTE_READ_MIN_BATCH_BYTES);
    assert_eq!(
        controller.next_remote_read_len(false, pressure_floor, 65_536,),
        pressure_floor,
        "the first awaited remote read must obey pressure credit, not the old 64 KiB buffer size"
    );
    assert_eq!(
        controller.next_remote_read_len(true, pressure_floor, 65_536,),
        0
    );
}

#[test]
fn projected_relay_read_credit_accounts_for_incoming_payload() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);

    assert_eq!(
        relay_read_credit_for_projected_local_egress(
            cfg.low_bytes,
            0,
            RELAY_REMOTE_READ_MIN_BATCH_BYTES,
            false,
            false,
            cfg,
            tun_mtu,
        ),
        RelayReadCredit {
            paused: false,
            max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
        },
        "small incoming payloads below the high watermark keep QUIC receive cadence open"
    );

    let tapered = relay_read_credit_for_projected_local_egress(
        cfg.high_bytes.saturating_sub(1),
        0,
        RELAY_REMOTE_READ_MIN_BATCH_BYTES,
        false,
        false,
        cfg,
        tun_mtu,
    );
    assert!(!tapered.paused);
    assert!(
        tapered.max_batch_bytes < RELAY_REMOTE_READ_BURST_MAX_BYTES,
        "projected pressure should reduce the next relay batch before the current payload is flushed: {tapered:?}"
    );

    assert_eq!(
        relay_read_credit_for_projected_local_egress(
            tx_queue_flush_threshold(cfg),
            0,
            RELAY_REMOTE_READ_BURST_MAX_BYTES,
            false,
            false,
            cfg,
            tun_mtu,
        ),
        RelayReadCredit {
            paused: false,
            max_batch_bytes: ack_drain_floor,
        },
        "a projected credit-edge overshoot should keep draining the QUIC stream with a tiny ACK/window batch"
    );
}

#[test]
fn relay_read_credit_caps_batch_to_clean_headroom_after_high_watermark() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 327_272,
        low_bytes: 81_818,
    };
    let tun_mtu = 1200;
    let local_pressure = cfg.high_bytes + 1;
    let clean_headroom = tx_queue_flush_threshold(cfg).saturating_sub(local_pressure);

    let credit = relay_read_credit_for_local_egress(local_pressure, 0, false, false, cfg, tun_mtu);

    assert!(
        !credit.paused,
        "clean-headroom capped credit should keep the stream serviced"
    );
    assert_eq!(
        credit.max_batch_bytes, clean_headroom,
        "read-credit must not permit a single relay batch larger than remaining clean egress headroom"
    );
    assert!(
        credit.max_batch_bytes < RELAY_REMOTE_READ_BURST_MAX_BYTES,
        "Knife14ez active failure shape must not keep publishing 512KiB batches"
    );
}

#[test]
fn relay_read_credit_preserves_low_pressure_quic_read_window() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 327_272,
        low_bytes: 81_818,
    };
    let tun_mtu = 1200;

    let credit = relay_read_credit_for_local_egress(0, 0, false, false, cfg, tun_mtu);

    assert_eq!(
        credit.max_batch_bytes, RELAY_REMOTE_READ_BURST_MAX_BYTES,
        "low-pressure read-credit must not starve TUIC stream polling"
    );
    assert!(
        credit.max_batch_bytes > tx_queue_flush_threshold(cfg),
        "local protection belongs to staged dispatch, not the QUIC receive window"
    );
}

#[test]
fn projected_relay_read_credit_shrinks_below_pressure_floor_near_drop_edge() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let credit = relay_read_credit_for_projected_local_egress(
        tx_queue_flush_threshold(cfg).saturating_sub(1),
        0,
        1,
        false,
        false,
        cfg,
        tun_mtu,
    );

    assert!(
        !credit.paused,
        "local pressure should keep a tiny ACK/window drain instead of hard-pausing QUIC reads"
    );
    assert!(
        credit.max_batch_bytes >= ack_drain_floor && credit.max_batch_bytes < pressure_floor,
        "near the local egress edge, projected credit must stay useful but below the old pressure floor: {credit:?}, ack_floor={ack_drain_floor}, pressure_floor={pressure_floor}"
    );
}

#[test]
fn relay_read_credit_keeps_useful_floor_just_below_flush_edge() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let remaining_headroom = 24;
    let credit = relay_read_credit_for_local_egress(
        tx_queue_flush_threshold(cfg).saturating_sub(remaining_headroom),
        0,
        false,
        false,
        cfg,
        tun_mtu,
    );

    assert_eq!(
        credit,
        RelayReadCredit {
            paused: false,
            max_batch_bytes: ack_drain_floor,
        },
        "read credit below the flush edge must not fragment remote reads into tiny residual-headroom batches"
    );
}

#[test]
fn relay_read_credit_preserves_quic_window_drain_until_staging_is_full() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);

    assert_eq!(
        relay_read_credit_for_local_egress(
            tx_queue_credit_spend_threshold(cfg),
            0,
            false,
            false,
            cfg,
            tun_mtu,
        ),
        RelayReadCredit {
            paused: false,
            max_batch_bytes: ack_drain_floor,
        },
        "local egress pressure should slow relay reads without starving the QUIC stream"
    );
    assert_eq!(
        relay_read_credit_for_local_egress(
            tx_queue_credit_spend_threshold(cfg),
            cfg.high_bytes,
            false,
            false,
            cfg,
            tun_mtu,
        ),
        RelayReadCredit {
            paused: false,
            max_batch_bytes: ack_drain_floor,
        },
        "pending staging below the receive window should keep minimum drain credit"
    );
    assert_eq!(
        relay_read_credit_for_local_egress(
            tx_queue_credit_spend_threshold(cfg),
            downlink_receive_window_high(cfg),
            false,
            false,
            cfg,
            tun_mtu,
        ),
        RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        },
        "the bounded staging receive window remains the actual relay-read pause edge"
    );
}

#[test]
fn terminal_no_recv_relay_read_credit_caps_staging_at_ack_floor() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let closewait_no_recv = SocketCloseSnapshot {
        tcp_state: TcpState::CloseWait,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };

    assert_eq!(
        relay_read_credit_for_terminal_no_recv_window(closewait_no_recv, 0, cfg, tun_mtu),
        Some(RelayReadCredit {
            paused: false,
            max_batch_bytes: ack_drain_floor,
        }),
        "terminal no-recv pressure should allow only one ACK/window-sized staging drain"
    );
    assert_eq!(
        relay_read_credit_for_terminal_no_recv_window(
            closewait_no_recv,
            ack_drain_floor,
            cfg,
            tun_mtu
        ),
        Some(RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        }),
        "once the terminal staging cap is occupied, relay reads must pause instead of expanding pending"
    );
    assert_eq!(
        relay_read_credit_for_terminal_no_recv_window(
            SocketCloseSnapshot {
                send_queue: tx_queue_flush_threshold(cfg).saturating_sub(1),
                ..closewait_no_recv
            },
            0,
            cfg,
            tun_mtu
        ),
        None,
        "a no-recv close tail below the egress flush edge can keep the normal relay credit"
    );
    assert_eq!(
        relay_read_credit_for_terminal_no_recv_window(
            SocketCloseSnapshot {
                tcp_state: TcpState::Established,
                may_recv: true,
                ..closewait_no_recv
            },
            ack_drain_floor,
            cfg,
            tun_mtu
        ),
        None,
        "active receive-capable flows must not inherit terminal staging caps"
    );
}

#[test]
fn downlink_credit_controller_shrinks_staging_after_headroom_without_progress() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let staging_limit = downlink_pressure_staging_limit(cfg, tun_mtu);
    let mut controller = DownlinkCreditController::default();
    let stalled = DownlinkFlushLimit {
        len: 0,
        headroom_limited: true,
        headroom_deferred_bytes: 64,
        clean_headroom_bytes: 0,
        drain_credit_granted_bytes: 0,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 3,
        hard_edge_guard_deferred_bytes: 64,
    };

    controller.note_flush_feedback(stalled, cfg, tun_mtu);
    controller.note_flush_feedback(stalled, cfg, tun_mtu);

    assert!(controller.headroom_debt_bytes > 0);
    assert!(controller.no_egress_progress_streak >= 2);
    assert_eq!(
        controller.read_credit_for(RelayReadCredit::default(), staging_limit, cfg, tun_mtu),
        RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        },
        "stalled egress may keep only bounded staging and must not keep expanding pending"
    );
    let budget = controller.flush_budget(262_144, cfg, tun_mtu);
    assert!(
        budget >= ack_drain_floor && budget < pressure_floor,
        "headroom deferral should shrink the next flush budget below the old pressure floor: budget={budget}, ack_floor={ack_drain_floor}, pressure_floor={pressure_floor}"
    );
}

#[test]
fn downlink_credit_controller_keeps_high_water_staging_while_egress_progresses() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let low_pressure_staging = downlink_pressure_staging_limit(cfg, tun_mtu);
    let controller = DownlinkCreditController {
        headroom_debt_bytes: cfg.high_bytes,
        no_egress_progress_streak: 0,
        ..DownlinkCreditController::default()
    };

    let credit = controller.read_credit_for(
        RelayReadCredit::default(),
        low_pressure_staging.saturating_add(ack_drain_floor),
        cfg,
        tun_mtu,
    );

    assert!(
        !credit.paused,
        "headroom debt alone should not collapse staging to the low watermark while egress is still making progress: credit={credit:?}, low_pressure_staging={low_pressure_staging}, high={}",
        cfg.high_bytes
    );
    assert!(credit.max_batch_bytes >= ack_drain_floor);
}

#[test]
fn downlink_credit_controller_pauses_instead_of_publishing_sub_mtu_staging_credit() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let controller = DownlinkCreditController::default();

    let credit = controller.read_credit_for(
        RelayReadCredit::default(),
        downlink_receive_window_high(cfg).saturating_sub(ack_drain_floor / 2),
        cfg,
        tun_mtu,
    );

    assert_eq!(
        credit,
        RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        },
        "bounded staging should not publish tiny residual read credits below the ACK/window floor"
    );
}

#[test]
fn downlink_credit_controller_uses_full_clean_batch_before_pressure_feedback() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let controller = DownlinkCreditController::default();

    let clean = controller.read_credit_for(RelayReadCredit::default(), 0, cfg, tun_mtu);

    assert_eq!(
        clean,
        RelayReadCredit {
            paused: false,
            max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
        },
        "clean empty staging should drain QUIC with the base relay batch instead of waiting for egress feedback to grow the initial 64KiB ceiling"
    );

    let staged = controller.read_credit_for(
        RelayReadCredit::default(),
        relay_remote_read_ack_drain_floor_bytes(tun_mtu),
        cfg,
        tun_mtu,
    );
    assert_eq!(
        staged.max_batch_bytes, DOWNLINK_CREDIT_INITIAL_BATCH_BYTES,
        "once staging is non-empty, the initial controller ceiling still bounds local pending growth"
    );
}

#[test]
fn downlink_credit_controller_grows_credit_from_observed_egress_progress() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let mut controller = DownlinkCreditController::default();
    let stalled = DownlinkFlushLimit {
        len: 0,
        headroom_limited: true,
        headroom_deferred_bytes: 64,
        clean_headroom_bytes: 0,
        drain_credit_granted_bytes: 0,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 3,
        hard_edge_guard_deferred_bytes: 64,
    };
    controller.note_flush_feedback(stalled, cfg, tun_mtu);
    let shrunk = controller.flush_budget(262_144, cfg, tun_mtu);

    let progressed = DownlinkFlushLimit {
        drain_credit_granted_bytes: pressure_floor,
        headroom_limited: false,
        headroom_deferred_bytes: 0,
        hard_edge_guard_deferred_bytes: 0,
        ..stalled
    };
    controller.note_flush_feedback(progressed, cfg, tun_mtu);

    assert_eq!(controller.no_egress_progress_streak, 0);
    assert!(
        controller.flush_budget(262_144, cfg, tun_mtu) > shrunk,
        "observed send_queue drain should reopen the local flush budget additively"
    );
    let credit = controller.read_credit_for(RelayReadCredit::default(), 0, cfg, tun_mtu);
    assert!(
        credit.max_batch_bytes > pressure_floor,
        "observed egress progress should grow relay read credit"
    );
}

#[test]
fn downlink_credit_controller_grows_pressure_ack_drain_from_clean_egress_progress() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let mut controller = DownlinkCreditController::default();
    let progressed = DownlinkFlushLimit {
        len: pressure_floor,
        headroom_limited: false,
        headroom_deferred_bytes: 0,
        clean_headroom_bytes: pressure_floor,
        drain_credit_granted_bytes: pressure_floor,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 0,
        hard_edge_guard_deferred_bytes: 0,
    };

    controller.note_flush_feedback(progressed, cfg, tun_mtu);
    controller.note_flush_feedback(progressed, cfg, tun_mtu);

    let local_pressure_bytes = tx_queue_flush_threshold(cfg);
    let pressure_ack_drain_bytes =
        controller.adaptive_ack_drain_floor_for(local_pressure_bytes, cfg, tun_mtu);
    let base = relay_read_credit_for_local_egress_with_ack_drain_floor(
        local_pressure_bytes,
        0,
        false,
        false,
        cfg,
        tun_mtu,
        pressure_ack_drain_bytes,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
    );
    let credit = controller.read_credit_for(base, 0, cfg, tun_mtu);

    assert!(
        credit.max_batch_bytes > ack_drain_floor,
        "clean egress progress should reopen pressure ACK/window drain above the tiny floor: credit={credit:?}, ack_floor={ack_drain_floor}"
    );
    assert!(
        credit.max_batch_bytes >= pressure_floor,
        "adaptive pressure drain should include at least the old pressure floor: credit={credit:?}, pressure_floor={pressure_floor}"
    );
    assert!(
        credit.max_batch_bytes <= RELAY_REMOTE_READ_MIN_BATCH_BYTES,
        "adaptive pressure drain must stay bounded to a useful compressed batch: credit={credit:?}, min_batch={}",
        RELAY_REMOTE_READ_MIN_BATCH_BYTES
    );
}

#[test]
fn downlink_credit_controller_progressing_headroom_deferral_keeps_useful_batch() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let mut controller = DownlinkCreditController::default();
    let progressing_deferral = DownlinkFlushLimit {
        len: RELAY_REMOTE_READ_MIN_BATCH_BYTES,
        headroom_limited: true,
        headroom_deferred_bytes: RELAY_REMOTE_READ_MIN_BATCH_BYTES,
        clean_headroom_bytes: 0,
        drain_credit_granted_bytes: RELAY_REMOTE_READ_MIN_BATCH_BYTES,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 0,
        hard_edge_guard_deferred_bytes: 0,
    };

    controller.note_flush_feedback(progressing_deferral, cfg, tun_mtu);
    controller.note_flush_feedback(progressing_deferral, cfg, tun_mtu);

    assert_eq!(
        controller.no_egress_progress_streak, 0,
        "headroom deferral with accepted egress bytes is pressure, not a stall"
    );
    let local_pressure_bytes = tx_queue_egress_target_threshold(cfg);
    let pressure_ack_drain_bytes =
        controller.adaptive_ack_drain_floor_for(local_pressure_bytes, cfg, tun_mtu);
    let base = relay_read_credit_for_local_egress_with_ack_drain_floor(
        local_pressure_bytes,
        0,
        false,
        false,
        cfg,
        tun_mtu,
        pressure_ack_drain_bytes,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
    );
    let credit =
        controller.read_credit_for_local_pressure(base, 0, local_pressure_bytes, cfg, tun_mtu);

    assert!(
        credit.max_batch_bytes >= RELAY_REMOTE_READ_MIN_BATCH_BYTES,
        "progressing headroom deferral should keep a useful compressed batch instead of collapsing to the 19KB pressure floor: credit={credit:?}, pressure_floor={pressure_floor}"
    );
}

#[test]
fn downlink_credit_controller_treats_accepted_flush_bytes_as_egress_progress() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let mut controller = DownlinkCreditController::default();
    let accepted_deferral = DownlinkFlushLimit {
        len: pressure_floor,
        headroom_limited: true,
        headroom_deferred_bytes: pressure_floor,
        clean_headroom_bytes: pressure_floor,
        drain_credit_granted_bytes: 0,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 0,
        hard_edge_guard_deferred_bytes: pressure_floor,
    };

    controller.note_flush_feedback_with_accepted(accepted_deferral, pressure_floor, cfg, tun_mtu);
    controller.note_flush_feedback_with_accepted(accepted_deferral, pressure_floor, cfg, tun_mtu);

    assert_eq!(
        controller.no_egress_progress_streak, 0,
        "bytes accepted by smoltcp are useful local egress progress, not a stalled flush"
    );
    assert!(
        controller.egress_progress_generation >= 2,
        "accepted flush progress must wake relay read-credit publishers"
    );
    let local_pressure_bytes = tx_queue_credit_spend_threshold(cfg);
    let pressure_ack_drain_bytes =
        controller.adaptive_ack_drain_floor_for(local_pressure_bytes, cfg, tun_mtu);
    let base = relay_read_credit_for_local_egress_with_ack_drain_floor(
        local_pressure_bytes,
        0,
        false,
        false,
        cfg,
        tun_mtu,
        pressure_ack_drain_bytes,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
    );
    let credit = controller.read_credit_for(base, 0, cfg, tun_mtu);

    assert!(
        !credit.paused && credit.max_batch_bytes > ack_drain_floor,
        "accepted flush progress should keep a useful read-credit cadence at the credit edge: credit={credit:?}, ack_floor={ack_drain_floor}"
    );
}

#[test]
fn downlink_credit_controller_earns_payload_tokens_from_egress_progress() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let target_edge = tx_queue_egress_target_threshold(cfg);
    let payload_edge = target_edge.saturating_sub(ack_drain_floor.saturating_mul(2));
    let mut controller = DownlinkCreditController::default();
    let progressed = DownlinkFlushLimit {
        len: 64 * 1024,
        headroom_limited: false,
        headroom_deferred_bytes: 0,
        clean_headroom_bytes: 64 * 1024,
        drain_credit_granted_bytes: 64 * 1024,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 0,
        hard_edge_guard_deferred_bytes: 0,
    };

    controller.note_flush_feedback(progressed, cfg, tun_mtu);
    let base = relay_read_credit_for_local_egress_with_ack_drain_floor(
        payload_edge,
        0,
        false,
        false,
        cfg,
        tun_mtu,
        ack_drain_floor,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
    );
    let credit = controller.read_credit_for_local_pressure(base, 0, payload_edge, cfg, tun_mtu);

    assert!(
        credit.max_batch_bytes > ack_drain_floor,
        "egress-earned payload tokens should reopen useful payload read credit below the egress target: credit={credit:?}, ack_floor={ack_drain_floor}"
    );
    assert!(
        credit.max_batch_bytes <= target_edge - payload_edge,
        "payload token credit must stay inside the egress target headroom"
    );

    let at_target = relay_read_credit_for_local_egress_with_ack_drain_floor(
        target_edge,
        0,
        false,
        false,
        cfg,
        tun_mtu,
        ack_drain_floor,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
    );
    let at_target =
        controller.read_credit_for_local_pressure(at_target, 0, target_edge, cfg, tun_mtu);
    assert_eq!(
        at_target.max_batch_bytes, ack_drain_floor,
        "at the egress target, ordinary payload tokens must yield to the ACK/window floor"
    );
}

#[test]
fn downlink_credit_controller_spends_payload_tokens_only_under_projected_pressure() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let mut controller = DownlinkCreditController::default();
    let progressed = DownlinkFlushLimit {
        len: 64 * 1024,
        headroom_limited: false,
        headroom_deferred_bytes: 0,
        clean_headroom_bytes: 64 * 1024,
        drain_credit_granted_bytes: 64 * 1024,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 0,
        hard_edge_guard_deferred_bytes: 0,
    };

    controller.note_flush_feedback(progressed, cfg, tun_mtu);
    let earned = controller.egress_payload_credit_bytes;
    assert!(earned > 0, "progress should earn payload tokens");

    controller.note_payload_read_for_projected_pressure(
        earned / 2,
        tx_queue_flush_threshold(cfg).saturating_sub(1),
        cfg,
    );
    assert_eq!(
        controller.egress_payload_credit_bytes, earned,
        "below the pressure region, ordinary payload reads should not spend the egress token reservoir"
    );

    controller.note_payload_read_for_projected_pressure(
        earned / 2,
        tx_queue_flush_threshold(cfg),
        cfg,
    );
    assert_eq!(
        controller.egress_payload_credit_bytes,
        earned - earned / 2,
        "projected pressure payload reads must spend the reservoir they used"
    );
}

#[test]
fn downlink_credit_controller_tokens_do_not_override_pause_or_staging() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let mut controller = DownlinkCreditController::default();
    let progressed = DownlinkFlushLimit {
        len: 64 * 1024,
        headroom_limited: false,
        headroom_deferred_bytes: 0,
        clean_headroom_bytes: 64 * 1024,
        drain_credit_granted_bytes: 64 * 1024,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 0,
        hard_edge_guard_deferred_bytes: 0,
    };

    controller.note_flush_feedback(progressed, cfg, tun_mtu);
    let base = RelayReadCredit {
        paused: false,
        max_batch_bytes: ack_drain_floor,
    };
    let at_pause = controller.read_credit_for_local_pressure(
        base,
        0,
        tx_queue_pause_threshold(cfg),
        cfg,
        tun_mtu,
    );
    assert_eq!(
        at_pause, base,
        "payload tokens must not expand relay credit at or beyond the hard pause edge"
    );

    let staged = controller.read_credit_for_local_pressure(
        RelayReadCredit::default(),
        downlink_receive_window_high(cfg),
        tx_queue_flush_threshold(cfg),
        cfg,
        tun_mtu,
    );
    assert_eq!(
        staged,
        RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        },
        "payload tokens must not override the bounded staging limit"
    );
}

#[test]
fn downlink_credit_controller_keeps_adaptive_ack_drain_at_credit_edge_while_progressing() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let pause_edge = tx_queue_pause_threshold(cfg);
    let mut controller = DownlinkCreditController::default();
    let progressed = DownlinkFlushLimit {
        len: pressure_floor,
        headroom_limited: false,
        headroom_deferred_bytes: 0,
        clean_headroom_bytes: pressure_floor,
        drain_credit_granted_bytes: pressure_floor,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 0,
        hard_edge_guard_deferred_bytes: 0,
    };

    controller.note_flush_feedback(progressed, cfg, tun_mtu);
    controller.note_flush_feedback(progressed, cfg, tun_mtu);

    let local_pressure_bytes = tx_queue_credit_spend_threshold(cfg);
    let pressure_ack_drain_bytes =
        controller.adaptive_ack_drain_floor_for(local_pressure_bytes, cfg, tun_mtu);
    let base = relay_read_credit_for_local_egress_with_ack_drain_floor(
        local_pressure_bytes,
        0,
        false,
        false,
        cfg,
        tun_mtu,
        pressure_ack_drain_bytes,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
    );
    let credit = controller.read_credit_for(base, 0, cfg, tun_mtu);

    assert!(
        !credit.paused && credit.max_batch_bytes > ack_drain_floor,
        "clean egress progress should keep useful ACK/window drain at the credit edge: credit={credit:?}, ack_floor={ack_drain_floor}"
    );
    assert!(
        credit.max_batch_bytes <= pressure_floor,
        "credit edge ACK/window drain must stay pressure bounded: credit={credit:?}, pressure_floor={pressure_floor}"
    );
    assert!(
        credit.max_batch_bytes <= pause_edge - local_pressure_bytes,
        "credit edge ACK/window drain must not project past the hard pause edge"
    );
}

#[test]
fn downlink_credit_controller_shrinks_adaptive_ack_drain_after_no_progress() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let mut controller = DownlinkCreditController::default();
    let progressed = DownlinkFlushLimit {
        len: pressure_floor,
        headroom_limited: false,
        headroom_deferred_bytes: 0,
        clean_headroom_bytes: pressure_floor,
        drain_credit_granted_bytes: pressure_floor,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 0,
        hard_edge_guard_deferred_bytes: 0,
    };
    let stalled = DownlinkFlushLimit {
        len: 0,
        headroom_limited: true,
        headroom_deferred_bytes: pressure_floor,
        clean_headroom_bytes: 0,
        drain_credit_granted_bytes: 0,
        drain_credit_planned_bytes: 0,
        drop_credit_debt_bytes: 0,
        drop_credit_debt_paid_bytes: 0,
        drop_credit_blocked_bytes: 0,
        pressure_credit_debt_bytes: 0,
        pressure_credit_debt_paid_bytes: 0,
        pressure_credit_blocked_bytes: 0,
        hard_edge_guard_bytes: 0,
        hard_edge_guard_deferred_bytes: 0,
    };

    controller.note_flush_feedback(progressed, cfg, tun_mtu);
    controller.note_flush_feedback(progressed, cfg, tun_mtu);
    assert!(
        controller.adaptive_ack_drain_floor_for(tx_queue_flush_threshold(cfg), cfg, tun_mtu)
            > ack_drain_floor
    );

    controller.note_flush_feedback(stalled, cfg, tun_mtu);
    controller.note_flush_feedback(stalled, cfg, tun_mtu);

    assert_eq!(
        controller.adaptive_ack_drain_floor_for(tx_queue_flush_threshold(cfg), cfg, tun_mtu),
        ack_drain_floor,
        "consecutive no-progress feedback should shrink adaptive drain back to the tiny floor"
    );
}

#[test]
fn relay_read_credit_uses_cadence_boost_between_credit_and_pause_edge() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let credit_edge = tx_queue_credit_spend_threshold(cfg);
    let pause_edge = tx_queue_pause_threshold(cfg);
    let local_pressure = credit_edge + (pause_edge - credit_edge) / 2;

    let credit = relay_read_credit_for_local_pressure_with_ack_drain_floor(
        local_pressure,
        0,
        false,
        false,
        cfg,
        tun_mtu,
        pressure_floor,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
    );

    assert!(
        credit.max_batch_bytes > ack_drain_floor,
        "gap-driven ACK/window cadence should keep a bounded multi-MTU read credit until the hard pause edge: credit={credit:?}, ack_floor={ack_drain_floor}"
    );
    assert!(
        credit.max_batch_bytes <= pressure_floor,
        "cadence boost must stay below the pressure floor"
    );
    assert!(
        credit.max_batch_bytes <= pause_edge - local_pressure,
        "cadence boost must not project beyond the hard pause edge"
    );
}

#[test]
fn downlink_credit_controller_gap_hint_boost_is_short_lived_and_hard_pause_safe() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let mut controller = DownlinkCreditController::default();

    controller.note_ack_cadence_gap_hint(tx_queue_flush_threshold(cfg), false, cfg, tun_mtu);
    assert!(
        controller.adaptive_ack_drain_floor_for(tx_queue_flush_threshold(cfg), cfg, tun_mtu)
            > ack_drain_floor,
        "a relay read gap with healthy egress should grant a short-lived ACK/window cadence boost"
    );

    let boosted =
        controller.adaptive_ack_drain_floor_for(tx_queue_credit_spend_threshold(cfg), cfg, tun_mtu);
    assert!(
        boosted > ack_drain_floor && boosted <= pressure_floor,
        "cadence boost should remain useful at the credit edge but bounded: boosted={boosted}"
    );

    let first = controller.consume_ack_cadence_boost_for_publish(
        tx_queue_flush_threshold(cfg),
        false,
        cfg,
        tun_mtu,
    );
    assert!(
        first > ack_drain_floor,
        "first publish should consume the boost"
    );
    let second = controller.consume_ack_cadence_boost_for_publish(
        tx_queue_flush_threshold(cfg),
        false,
        cfg,
        tun_mtu,
    );
    assert_eq!(
        second, ack_drain_floor,
        "cadence boost should be one-shot unless another gap hint arrives"
    );

    controller.note_ack_cadence_gap_hint(tx_queue_flush_threshold(cfg), false, cfg, tun_mtu);
    controller.note_ack_cadence_gap_hint(tx_queue_pause_threshold(cfg), true, cfg, tun_mtu);
    assert_eq!(
        controller.adaptive_ack_drain_floor_for(tx_queue_flush_threshold(cfg), cfg, tun_mtu),
        ack_drain_floor,
        "hard pause feedback must clear cadence boost"
    );
}

#[test]
fn downlink_credit_controller_gap_hint_at_credit_edge_keeps_headroom_sized_boost() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let credit_edge = tx_queue_credit_spend_threshold(cfg);
    let pause_headroom = tx_queue_pause_threshold(cfg).saturating_sub(credit_edge);
    let mut controller = DownlinkCreditController::default();

    controller.note_ack_cadence_gap_hint(credit_edge, false, cfg, tun_mtu);
    let boosted = controller.adaptive_ack_drain_floor_for(credit_edge, cfg, tun_mtu);

    assert!(
        boosted > ack_drain_floor,
        "a relay-read gap observed at the credit edge should not collapse to the single-MTU ACK floor: boosted={boosted}, ack_floor={ack_drain_floor}"
    );
    assert_eq!(
        boosted,
        pressure_floor.min(pause_headroom.max(ack_drain_floor)),
        "credit-edge gap boost should spend only the remaining pause headroom"
    );
}

#[test]
fn relay_gap_hint_boost_requires_current_relay_context() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let ack_drain_floor = relay_remote_read_ack_drain_floor_bytes(tun_mtu);
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 4096]),
        TcpSocketBuffer::new(vec![0; 4096]),
    ));
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 7;
    let mut ctxs = HashMap::from([(handle, ctx)]);

    assert_eq!(
        note_ack_cadence_gap_hint_for_handle(handle, 6, &sockets, &mut ctxs, cfg, tun_mtu, false,),
        None,
        "stale relay gap hints must not alter the current flow"
    );
    assert_eq!(
        ctxs.get(&handle)
            .unwrap()
            .downlink_credit_controller
            .adaptive_ack_drain_floor_for(0, cfg, tun_mtu),
        ack_drain_floor
    );

    let boosted =
        note_ack_cadence_gap_hint_for_handle(handle, 7, &sockets, &mut ctxs, cfg, tun_mtu, false)
            .expect("current relay gap hint should report a cadence floor");
    assert!(
        boosted > ack_drain_floor,
        "current relay gap hint should activate a bounded cadence boost"
    );

    ctxs.get_mut(&handle).unwrap().state = SocketState::Listening;
    assert_eq!(
        note_ack_cadence_gap_hint_for_handle(handle, 7, &sockets, &mut ctxs, cfg, tun_mtu, false,),
        None,
        "non-relay slots must ignore late gap hints"
    );
}

#[tokio::test]
async fn relay_read_credit_publish_wakes_on_egress_progress_even_when_credit_is_unchanged() {
    let cfg = DownlinkBackpressureConfig::default();
    let tun_mtu = 1200;
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 4096]),
        TcpSocketBuffer::new(vec![0; 4096]),
    ));
    let (credit_tx, mut credit_rx) = watch::channel(RelayReadCredit::default());
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.relay_read_credit_tx = Some(credit_tx);
    ctx.downlink_credit_controller.note_flush_feedback(
        DownlinkFlushLimit {
            len: 0,
            headroom_limited: false,
            headroom_deferred_bytes: 0,
            clean_headroom_bytes: 0,
            drain_credit_granted_bytes: relay_remote_read_pressure_floor_bytes(tun_mtu),
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        },
        cfg,
        tun_mtu,
    );
    let mut ctxs = HashMap::from([(handle, ctx)]);

    publish_relay_read_credit_for_handle(
        handle,
        &sockets,
        &mut ctxs,
        cfg,
        tun_mtu,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        false,
        false,
        BufferedDownlinkConfig::disabled(cfg, DEFAULT_TUN_POOL_SIZE),
        0,
    );

    tokio::time::timeout(std::time::Duration::from_millis(20), credit_rx.changed())
        .await
        .expect("egress progress must wake the relay reader even if credit bytes stay open")
        .expect("relay read-credit channel should remain open");
    assert_eq!(*credit_rx.borrow_and_update(), RelayReadCredit::default());
}

#[test]
fn listener_socket_uses_local_virtual_link_tcp_policy() {
    let socket = build_listener_socket(&ListenerSpec { local_port: 12345 });

    assert!(
        !socket.nagle_enabled(),
        "local virtual-link TCP sockets should not add Nagle latency"
    );
    assert!(
        socket.ack_delay().is_none(),
        "local virtual-link TCP sockets should not delay ACKs"
    );
}

/// Established relay lifetime belongs to socket/transport lifecycle, not payload silence.
#[tokio::test(start_paused = true)]
async fn relay_full_duplex_idle_survives_legacy_timeout() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let flag = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(IdleStream {
        shutdown_called: flag.clone(),
    });
    let (tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();
    let task = tokio::spawn(run_relay(handle, 7, stream, rx, back_tx, credit_rx));

    tokio::task::yield_now().await;
    tokio::time::advance(RELAY_STALLED_WRITE_TIMEOUT + std::time::Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "a healthy full-duplex relay must survive the former payload-idle deadline"
    );
    assert!(
        !flag.load(Ordering::SeqCst),
        "payload silence must not shutdown an Established stream"
    );
    assert!(back_rx.try_recv().is_err());

    drop(tx);
    task.await.unwrap();
    assert!(
        flag.load(Ordering::SeqCst),
        "real command-channel lifecycle closure must still shutdown the stream"
    );
    match back_rx
        .try_recv()
        .expect("channel closure should notify main loop")
    {
        (h, RelayEvent::Closed(close)) => {
            assert_eq!(h, handle);
            assert_eq!(close.epoch, 7);
            assert_eq!(close.direction, "local_to_remote");
            assert_eq!(close.reason, "local_channel_closed");
        }
        other => panic!("expected relay close event, got {other:?}"),
    }
}

#[test]
fn coalesced_relay_payload_preserves_finish_order() {
    let (tx, mut rx) = mpsc::channel(4);
    tx.try_send(RelayCommand::Data(b"two".to_vec())).unwrap();
    tx.try_send(RelayCommand::Finish).unwrap();

    let coalesced = coalesce_relay_payload(b"one".to_vec(), &mut rx);

    assert_eq!(coalesced.payload, b"onetwo");
    assert!(
        coalesced.finish_after,
        "queued Finish must run after queued data"
    );
    assert!(coalesced.deferred.is_none());
    assert!(
        rx.try_recv().is_err(),
        "Finish should be consumed into finish_after"
    );
}

#[test]
fn coalesced_relay_payload_defers_data_that_would_exceed_cap() {
    let (tx, mut rx) = mpsc::channel(4);
    tx.try_send(RelayCommand::Data(vec![2; 2])).unwrap();

    let coalesced = coalesce_relay_payload(vec![1; RELAY_WRITER_COALESCE_MAX_BYTES - 1], &mut rx);

    assert_eq!(coalesced.payload.len(), RELAY_WRITER_COALESCE_MAX_BYTES - 1);
    match coalesced
        .deferred
        .expect("oversized next data should be deferred")
    {
        RelayCommand::Data(bytes) => assert_eq!(bytes, vec![2; 2]),
        other => panic!("expected deferred data, got {other:?}"),
    }
}

#[tokio::test]
async fn relay_writer_coalesces_queued_data_before_single_write() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(Mutex::new(Vec::new()));
    let stream: RelayStream = Box::new(RecordingWriteStream {
        shutdown_called: shutdown_called.clone(),
        writes: writes.clone(),
    });
    let (tx, rx) = mpsc::channel::<RelayCommand>(8);
    tx.send(RelayCommand::Data(b"one".to_vec())).await.unwrap();
    tx.send(RelayCommand::Data(b"two".to_vec())).await.unwrap();
    tx.send(RelayCommand::Data(b"three".to_vec()))
        .await
        .unwrap();
    drop(tx);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();

    let task = tokio::spawn(run_relay(handle, 41, stream, rx, back_tx, credit_rx));
    task.await.unwrap();

    assert_eq!(
        *writes.lock().unwrap(),
        vec![b"onetwothree".to_vec()],
        "queued data should become one upstream write batch"
    );
    assert!(
        shutdown_called.load(Ordering::SeqCst),
        "local channel close still shuts down writer"
    );
    match back_rx
        .try_recv()
        .expect("local channel close should notify main loop")
    {
        (h, RelayEvent::Closed(close)) => {
            assert_eq!(h, handle);
            assert_eq!(close.epoch, 41);
            assert_eq!(close.direction, "local_to_remote");
            assert_eq!(close.reason, "local_channel_closed");
        }
        other => panic!("expected relay close event, got {other:?}"),
    }
}

#[tokio::test]
async fn relay_writer_flushes_payload_to_release_remote_response() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let flushed = Arc::new(AtomicBool::new(false));
    let read_waker = Arc::new(Mutex::new(None));
    let writes = Arc::new(Mutex::new(Vec::new()));
    let stream: RelayStream = Box::new(FlushGatedReadableStream {
        shutdown_called: shutdown_called.clone(),
        flushed: flushed.clone(),
        read_waker,
        writes: writes.clone(),
        read_sent: false,
    });
    let (tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();
    let task = tokio::spawn(run_relay(handle, 43, stream, rx, back_tx, credit_rx));

    tx.send(RelayCommand::Data(b"reverse-control".to_vec()))
        .await
        .unwrap();

    let event = tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("remote response gated on flush should arrive promptly")
        .expect("relay should still be alive");

    assert!(
        flushed.load(Ordering::SeqCst),
        "payload writes must be flushed after write_all"
    );
    assert_eq!(*writes.lock().unwrap(), vec![b"reverse-control".to_vec()]);
    match event {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window: _,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 43);
            assert_eq!(bytes, b"remote-after-flush");
        }
        other => panic!("expected remote response after flushed payload, got {other:?}"),
    }

    drop(tx);
    task.await.unwrap();
    assert!(
        shutdown_called.load(Ordering::SeqCst),
        "local channel close still shuts down writer"
    );
}

#[tokio::test]
async fn thin_relay_reader_stages_payloads_without_global_rx_admission() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunk_len = 16 * 1024;
    let chunks_per_dispatch = RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES / chunk_len;
    let chunks: std::collections::VecDeque<_> = std::iter::repeat_with(|| vec![7; chunk_len])
        .take(THIN_TCP_RELAY_STAGING_CAPACITY * chunks_per_dispatch + chunks_per_dispatch)
        .collect();
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
        chunks,
        eof_after_chunks: false,
    });
    let (remote_reader, _remote_writer) = tokio::io::split(stream);
    let (thin_tx, mut thin_rx) = mpsc::channel(THIN_TCP_RELAY_STAGING_CAPACITY);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 16 * 1024,
    });
    let (signal_tx, _signal_rx) = mpsc::channel(RELAY_CHANNEL_CAPACITY);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let local_finish_seen = Arc::new(AtomicBool::new(false));

    let task = tokio::spawn(run_relay_thin_reader(
        handle,
        91,
        remote_reader,
        thin_tx,
        credit_rx,
        signal_tx,
        local_finish_seen,
        stop_rx,
    ));

    tokio::time::timeout(std::time::Duration::from_millis(200), async {
        while thin_rx.len() < THIN_TCP_RELAY_STAGING_CAPACITY {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("thin reader should fill its bounded staging queue without global_rx admission");

    assert_eq!(
        thin_rx.len(),
        THIN_TCP_RELAY_STAGING_CAPACITY,
        "thin staging capacity is the explicit reader-side pressure edge"
    );
    task.abort();
    let _ = task.await;

    let mut staged = 0;
    while thin_rx.try_recv().is_ok() {
        staged += 1;
    }
    assert_eq!(staged, THIN_TCP_RELAY_STAGING_CAPACITY);
}

#[tokio::test]
async fn continuous_relay_reader_ignores_read_credit_pause_until_byte_queue_full() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: shutdown_called.clone(),
        chunks: std::collections::VecDeque::from([b"after-paused-credit".to_vec()]),
        eof_after_chunks: true,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: true,
        max_batch_bytes: 0,
    });
    let task = tokio::spawn(run_relay_with_engine(
        handle,
        92,
        stream,
        rx,
        back_tx,
        credit_rx,
        TcpRelayEngine::ContinuousPump,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES,
    ));

    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("continuous pump should not wait for normal read-credit resume")
        .expect("relay should still be alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 92);
            assert_eq!(bytes, b"after-paused-credit");
            assert_eq!(
                stream_service_window.remote_read_bytes,
                b"after-paused-credit".len() as u64
            );
        }
        other => panic!("expected data despite paused read credit, got {other:?}"),
    }

    task.await.unwrap();
    assert!(
        shutdown_called.load(Ordering::SeqCst),
        "remote EOF path should stop the writer half"
    );
}

#[tokio::test]
async fn continuous_relay_reader_stops_only_at_byte_queue_full_edge() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunk_len = 64 * 1024;
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
        chunks: std::iter::repeat_with(|| vec![7; chunk_len])
            .take(4)
            .collect(),
        eof_after_chunks: false,
    });
    let (remote_reader, _remote_writer) = tokio::io::split(stream);
    let queue = AsyncByteBoundedFlowQueue::new(128 * 1024).unwrap();
    let (_credit_tx, credit_rx) = relay_read_credit_channel();
    let (signal_tx, mut signal_rx) = mpsc::channel(RELAY_CHANNEL_CAPACITY);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let local_finish_seen = Arc::new(AtomicBool::new(false));

    let task = tokio::spawn(run_relay_continuous_reader(
        handle,
        93,
        remote_reader,
        queue.clone(),
        credit_rx,
        signal_tx,
        local_finish_seen,
        stop_rx,
    ));

    tokio::time::timeout(std::time::Duration::from_millis(200), async {
        while queue.snapshot().await.queued_bytes < 128 * 1024 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("continuous reader should fill the byte queue");

    let mut last_reader_diag = None;
    while let Ok(Some(signal)) =
        tokio::time::timeout(std::time::Duration::from_millis(1), signal_rx.recv()).await
    {
        if let RelayReaderSignal::Progress { diag } = signal {
            last_reader_diag = Some(diag);
        }
    }
    let last_reader_diag = last_reader_diag.expect("reader should report progress");
    assert_eq!(
        last_reader_diag.remote_reads, 2,
        "reader must stop after filling the 128KiB byte queue"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), signal_rx.recv())
            .await
            .is_err(),
        "no extra read progress should appear while the byte queue is full"
    );

    let drained = queue.pop_up_to(64 * 1024).await.unwrap();
    assert_eq!(drained.len(), 64 * 1024);
    match tokio::time::timeout(std::time::Duration::from_millis(200), signal_rx.recv())
        .await
        .expect("draining capacity should re-arm the reader")
        .expect("reader should still be alive")
    {
        RelayReaderSignal::Progress { diag } => {
            assert_eq!(diag.remote_reads, 3);
            assert!(
                diag.continuous_queue_wait_events >= 1,
                "queue-full wait must be visible in relay diagnostics: {diag:?}"
            );
            assert_eq!(queue.snapshot().await.queued_bytes, 128 * 1024);
        }
        other => panic!("expected reader progress after queue drain, got {other:?}"),
    }

    let _ = stop_tx.send(());
    task.await.unwrap();
}

#[test]
fn native_tcp_chunk_assembler_waits_for_gap_then_drains_contiguous_prefix() {
    let mut assembler = NativeTcpChunkAssembler::default();

    assembler.push_chunk(NativeTcpChunk {
        offset: 4,
        bytes: Bytes::from_static(b"efgh"),
    });
    assert!(
        assembler.drain_ordered_up_to(8).is_none(),
        "out-of-order bytes must not cross the missing prefix"
    );
    assert_eq!(assembler.buffered_bytes(), 4);

    assembler.push_chunk(NativeTcpChunk {
        offset: 0,
        bytes: Bytes::from_static(b"abcd"),
    });
    let drained = assembler.drain_ordered_up_to(8).unwrap();
    assert_eq!(&drained[..], b"abcdefgh");
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[test]
fn native_tcp_chunk_assembler_keeps_remainder_and_ignores_duplicates() {
    let mut assembler = NativeTcpChunkAssembler::default();

    assembler.push_chunk(NativeTcpChunk {
        offset: 0,
        bytes: Bytes::from_static(b"abcdef"),
    });
    let first = assembler.drain_ordered_up_to(3).unwrap();
    assert_eq!(&first[..], b"abc");
    assert_eq!(assembler.buffered_bytes(), 3);

    assembler.push_chunk(NativeTcpChunk {
        offset: 0,
        bytes: Bytes::from_static(b"abcdef"),
    });
    let second = assembler.drain_ordered_up_to(16).unwrap();
    assert_eq!(&second[..], b"def");
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[test]
fn native_permit_queue_capacity_tracks_egress_high_watermark() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 327_272,
        low_bytes: 81_818,
    };

    assert_eq!(native_tcp_permit_queue_capacity_bytes(cfg), cfg.high_bytes);
}

#[test]
fn native_tcp_read_credit_limit_respects_pause_and_batch_cap() {
    assert_eq!(
        native_tcp_read_credit_limit(RelayReadCredit {
            paused: true,
            max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
        }),
        0
    );
    assert_eq!(
        native_tcp_read_credit_limit(RelayReadCredit {
            paused: false,
            max_batch_bytes: 0,
        }),
        0
    );
    assert_eq!(
        native_tcp_read_credit_limit(RelayReadCredit {
            paused: false,
            max_batch_bytes: 16 * 1024,
        }),
        16 * 1024
    );
    assert_eq!(
        native_tcp_read_credit_limit(RelayReadCredit {
            paused: false,
            max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES * 2,
        }),
        RELAY_REMOTE_READ_BURST_MAX_BYTES
    );
}

#[test]
fn native_tcp_read_service_floor_lifts_tiny_credit_only_when_enabled() {
    let tiny = RelayReadCredit {
        paused: false,
        max_batch_bytes: relay_remote_read_ack_drain_floor_bytes(1200),
    };

    assert_eq!(
        native_tcp_read_service_limit(tiny, false),
        tiny.max_batch_bytes,
        "default native read credit semantics must remain unchanged"
    );
    assert_eq!(
        native_tcp_read_service_limit(tiny, true),
        RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES,
        "H10d15 native read floor should keep a service-sized QUIC read armed"
    );
    assert_eq!(
        native_tcp_read_service_limit(
            RelayReadCredit {
                paused: true,
                max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
            },
            true,
        ),
        0,
        "hard pause must still stop native remote reads"
    );
    assert_eq!(
        native_tcp_read_service_limit(
            RelayReadCredit {
                paused: false,
                max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES * 2,
            },
            true,
        ),
        RELAY_REMOTE_READ_BURST_MAX_BYTES,
        "native read floor must not expand beyond the existing burst cap"
    );
}

#[tokio::test]
async fn relay_reader_progress_signal_full_does_not_block_data_plane() {
    let (signal_tx, mut signal_rx) = mpsc::channel(1);
    signal_tx
        .try_send(RelayReaderSignal::Progress {
            diag: RelayTaskDiag::default(),
        })
        .unwrap();

    assert_eq!(
        send_relay_reader_progress(&signal_tx, &RelayTaskDiag::default()),
        true,
        "progress notifications are supervisory; a full progress channel must not stall the reader"
    );
    assert!(signal_rx.recv().await.is_some());
}

#[tokio::test]
async fn downlink_pending_releases_downstream_permits_on_admission_and_clear() {
    let queue = AsyncLeasedByteFlowQueue::new(128 * 1024).unwrap();
    queue.push(Bytes::from(vec![1; 64 * 1024])).await.unwrap();
    let leased = queue.recv_up_to(64 * 1024).await.unwrap();
    let (bytes, permit) = leased.into_parts();

    let mut ctx = SocketCtx::new(12345);
    ctx.append_downlink_pending_with_permit(bytes.as_ref(), Some(permit));
    assert_eq!(ctx.downlink_pending.len(), 64 * 1024);
    assert_eq!(queue.snapshot().await.leased_bytes, 64 * 1024);

    ctx.consume_downlink_pending_prefix(24 * 1024);
    assert_eq!(ctx.downlink_pending.len(), 40 * 1024);
    assert_eq!(queue.snapshot().await.leased_bytes, 40 * 1024);

    ctx.clear_downlink_pending();
    assert!(ctx.downlink_pending.is_empty());
    assert_eq!(queue.snapshot().await.leased_bytes, 0);
}

#[tokio::test]
async fn d6_downlink_admission_retains_permit_until_egress_drain() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        128 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    queue.push(Bytes::from(vec![1; 64 * 1024])).await.unwrap();
    let leased = queue.recv_up_to(64 * 1024).await.unwrap();
    let (bytes, permit) = leased.into_parts();

    let mut ctx = SocketCtx::new(12345);
    ctx.append_downlink_pending_with_permit(bytes.as_ref(), Some(permit));
    assert_eq!(ctx.downlink_pending.len(), 64 * 1024);
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 0);
    assert_eq!(queue.snapshot().await.leased_bytes, 64 * 1024);

    ctx.admit_downlink_pending_prefix(24 * 1024);
    assert_eq!(ctx.downlink_pending.len(), 40 * 1024);
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 24 * 1024);
    assert_eq!(
        queue.snapshot().await.leased_bytes,
        64 * 1024,
        "admission into smoltcp must not release D6 downstream capacity"
    );

    assert_eq!(ctx.release_downlink_inflight_permits(12 * 1024), 12 * 1024);
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 12 * 1024);
    assert_eq!(queue.snapshot().await.leased_bytes, 52 * 1024);

    ctx.clear_downlink_pending();
    assert!(ctx.downlink_pending.is_empty());
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 0);
    assert_eq!(
        queue.snapshot().await.leased_bytes,
        0,
        "terminal clear must release both pending and in-flight D6 permits"
    );
}

#[tokio::test]
async fn d6_flushed_tcp_payload_releases_matching_flow_permit() {
    let mut device = DnsInjectRecorder::default();
    let mut iface = Interface::new(
        SmolConfig::new(smoltcp::wire::HardwareAddress::Ip),
        &mut device,
        smoltcp::time::Instant::now(),
    );
    iface.update_ip_addrs(|addrs| {
        addrs
            .push(IpCidr::new(IpAddress::v4(10, 0, 0, 2), 24))
            .unwrap();
    });

    let mut tcp_socket = TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 64 * 1024]),
        TcpSocketBuffer::new(vec![0; 64 * 1024]),
    );
    configure_local_tcp_socket(&mut tcp_socket);
    tcp_socket
        .connect(
            iface.context(),
            IpEndpoint::new(IpAddress::v4(10, 0, 0, 2), 53124),
            IpEndpoint::new(IpAddress::v4(203, 0, 113, 9), 443),
        )
        .unwrap();
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(tcp_socket);

    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        128 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    queue.push(Bytes::from(vec![1; 64 * 1024])).await.unwrap();
    let leased = queue.recv_up_to(64 * 1024).await.unwrap();
    let (bytes, permit) = leased.into_parts();

    let mut ctx = SocketCtx::new(443);
    ctx.append_downlink_pending_with_permit(bytes.as_ref(), Some(permit));
    ctx.admit_downlink_pending_prefix(16 * 1024);
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 16 * 1024);
    let mut ctxs = HashMap::from([(handle, ctx)]);

    let release = release_downlink_inflight_permits_for_flushed_tcp_packets(
        vec![TunFlushedTcpPacket {
            src_addr: Ipv4Addr::new(203, 0, 113, 9),
            dst_addr: Ipv4Addr::new(10, 0, 0, 2),
            src_port: 443,
            dst_port: 53124,
            payload_bytes: 1200,
        }],
        &sockets,
        &mut ctxs,
    );

    assert_eq!(release.flushed_tcp_payload_bytes, 1200);
    assert_eq!(release.released_inflight_permit_bytes, 1200);
    assert_eq!(
        ctxs.get(&handle).unwrap().downlink_inflight_permit_bytes(),
        16 * 1024 - 1200
    );
    assert_eq!(queue.snapshot().await.leased_bytes, 64 * 1024 - 1200);

    let release = release_downlink_inflight_permits_for_flushed_tcp_packets(
        vec![TunFlushedTcpPacket {
            src_addr: Ipv4Addr::new(203, 0, 113, 9),
            dst_addr: Ipv4Addr::new(10, 0, 0, 99),
            src_port: 443,
            dst_port: 53124,
            payload_bytes: 1200,
        }],
        &sockets,
        &mut ctxs,
    );
    assert_eq!(release.flushed_tcp_payload_bytes, 1200);
    assert_eq!(
        release.released_inflight_permit_bytes, 0,
        "non-matching packet summaries must not release another flow's permit"
    );
}

#[tokio::test]
async fn permit_relay_reader_rearms_only_after_downstream_permit_release() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunk_len = 64 * 1024;
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
        chunks: std::iter::repeat_with(|| vec![7; chunk_len])
            .take((CONTINUOUS_TCP_RELAY_QUEUE_BYTES / chunk_len) + 4)
            .collect(),
        eof_after_chunks: false,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(256);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: true,
        max_batch_bytes: 0,
    });
    let task = tokio::spawn(run_relay_with_engine(
        handle,
        94,
        stream,
        rx,
        back_tx,
        credit_rx,
        TcpRelayEngine::PermitPump,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES,
    ));

    let mut leased_payload_bytes = 0usize;
    let mut permits = Vec::new();
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while leased_payload_bytes < CONTINUOUS_TCP_RELAY_QUEUE_BYTES {
            match back_rx.recv().await.expect("permit pump should emit data") {
                (
                    h,
                    RelayEvent::Data {
                        epoch,
                        bytes,
                        downstream_permit,
                        ..
                    },
                ) => {
                    assert_eq!(h, handle);
                    assert_eq!(epoch, 94);
                    leased_payload_bytes = leased_payload_bytes.saturating_add(bytes.len());
                    permits.push(
                        downstream_permit.expect("permit pump data must carry downstream permit"),
                    );
                }
                other => panic!("expected permit-pump data, got {other:?}"),
            }
        }
    })
    .await
    .expect("permit pump should fill the leased capacity");
    assert_eq!(leased_payload_bytes, CONTINUOUS_TCP_RELAY_QUEUE_BYTES);

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), back_rx.recv())
            .await
            .is_err(),
        "permit pump must not emit more data while all queue bytes are leased"
    );

    assert_eq!(permits[0].release(chunk_len), chunk_len);
    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("releasing downstream capacity should re-arm remote reads")
        .expect("relay should remain alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 94);
            assert_eq!(bytes.len(), chunk_len);
            assert!(
                downstream_permit.is_some(),
                "newly re-armed data must carry its own permit"
            );
        }
        other => panic!("expected data after permit release, got {other:?}"),
    }

    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn ordered_egress_permit_reader_respects_paused_read_credit() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunk_len = 16 * 1024;
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
        chunks: std::collections::VecDeque::from([vec![7; chunk_len]]),
        eof_after_chunks: false,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: true,
        max_batch_bytes: 0,
    });
    let task = tokio::spawn(run_relay_ordered_egress_permit(
        handle,
        96,
        stream,
        rx,
        back_tx,
        credit_rx,
        128 * 1024,
    ));

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), back_rx.recv())
            .await
            .is_err(),
        "ordered egress permit reader must not read ready remote bytes while credit is paused"
    );

    credit_tx
        .send(RelayReadCredit {
            paused: false,
            max_batch_bytes: chunk_len,
        })
        .unwrap();
    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("remote data should arrive after ordered egress credit resumes")
        .expect("relay should still be alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 96);
            assert_eq!(bytes.len(), chunk_len);
            let permit =
                downstream_permit.expect("ordered egress permit data must carry downstream permit");
            assert_eq!(
                permit.release_mode(),
                DownstreamPermitReleaseMode::OnEgressDrain
            );
        }
        other => {
            panic!("expected ordered egress permit data after credit resumes, got {other:?}")
        }
    }

    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn ordered_egress_permit_rearms_only_after_egress_permit_release() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunk_len = 64 * 1024;
    let queue_capacity = 128 * 1024;
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
        chunks: std::iter::repeat_with(|| vec![7; chunk_len])
            .take((queue_capacity / chunk_len) + 4)
            .collect(),
        eof_after_chunks: false,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(256);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: chunk_len,
    });
    let task = tokio::spawn(run_relay_ordered_egress_permit(
        handle,
        97,
        stream,
        rx,
        back_tx,
        credit_rx,
        queue_capacity,
    ));

    let mut leased_payload_bytes = 0usize;
    let mut permits = Vec::new();
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while leased_payload_bytes < queue_capacity {
            match back_rx
                .recv()
                .await
                .expect("ordered egress permit pump should emit data")
            {
                (
                    h,
                    RelayEvent::Data {
                        epoch,
                        bytes,
                        downstream_permit,
                        ..
                    },
                ) => {
                    assert_eq!(h, handle);
                    assert_eq!(epoch, 97);
                    leased_payload_bytes = leased_payload_bytes.saturating_add(bytes.len());
                    let permit = downstream_permit
                        .expect("ordered egress permit data must carry downstream permit");
                    assert_eq!(
                        permit.release_mode(),
                        DownstreamPermitReleaseMode::OnEgressDrain
                    );
                    permits.push(permit);
                }
                other => panic!("expected ordered egress permit data, got {other:?}"),
            }
        }
    })
    .await
    .expect("ordered egress permit pump should fill leased capacity");
    assert_eq!(leased_payload_bytes, queue_capacity);

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), back_rx.recv())
            .await
            .is_err(),
        "ordered egress permit pump must stop while all bytes are leased downstream"
    );

    assert_eq!(permits[0].release(chunk_len), chunk_len);
    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("egress permit release should re-arm ordered remote reads")
        .expect("relay should remain alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 97);
            assert_eq!(bytes.len(), chunk_len);
            assert!(
                downstream_permit.is_some(),
                "newly re-armed ordered data must carry a downstream permit"
            );
        }
        other => panic!("expected ordered data after permit release, got {other:?}"),
    }

    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn ordered_egress_permit_clamps_read_len_to_reader_buffer() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let queue_capacity = 327_272;
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
        chunks: std::collections::VecDeque::from([vec![7; 65_536]]),
        eof_after_chunks: false,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: queue_capacity,
    });
    let task = tokio::spawn(run_relay_ordered_egress_permit(
        handle,
        98,
        stream,
        rx,
        back_tx,
        credit_rx,
        queue_capacity,
    ));

    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("ordered egress permit reader should not stall with large credit")
        .expect("relay should remain alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 98);
            assert_eq!(
                bytes.len(),
                65_536,
                "large read credit must be clamped to the fixed reader buffer"
            );
            let permit = downstream_permit.expect("ordered egress permit data must carry permit");
            assert_eq!(
                permit.release_mode(),
                DownstreamPermitReleaseMode::OnEgressDrain
            );
        }
        other => panic!("expected first clamped ordered permit data, got {other:?}"),
    }

    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn native_permit_relay_respects_paused_read_credit() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunk_len = 16 * 1024;
    let queue_capacity = 128 * 1024;
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks: std::collections::VecDeque::from([NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from(vec![7; chunk_len]),
        }]),
        eof_after_chunks: false,
    });
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: true,
        max_batch_bytes: 0,
    });
    let task = tokio::spawn(run_relay_native_permit(
        handle,
        94,
        reader,
        writer,
        rx,
        back_tx,
        credit_rx,
        queue_capacity,
        false,
    ));

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), back_rx.recv())
            .await
            .is_err(),
        "paused native permit read credit must keep ready remote bytes unread"
    );

    credit_tx
        .send(RelayReadCredit {
            paused: false,
            max_batch_bytes: chunk_len,
        })
        .unwrap();
    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("remote data should arrive after native permit credit resumes")
        .expect("relay should still be alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 94);
            assert_eq!(bytes.len(), chunk_len);
            assert!(downstream_permit.is_some());
        }
        other => panic!("expected native permit data after credit resumes, got {other:?}"),
    }

    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn native_permit_relay_read_floor_services_full_quantum_from_tiny_credit() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let floor = RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES;
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks: std::collections::VecDeque::from([NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from(vec![7; floor]),
        }]),
        eof_after_chunks: false,
    });
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: relay_remote_read_ack_drain_floor_bytes(1200),
    });
    let task = tokio::spawn(run_relay_native_permit(
        handle, 96, reader, writer, rx, back_tx, credit_rx, floor, true,
    ));

    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("native read floor should keep remote reads service-sized")
        .expect("relay should still be alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 96);
            assert_eq!(
                bytes.len(),
                floor,
                "tiny read credit should be lifted to the service floor"
            );
            assert!(downstream_permit.is_some());
        }
        other => panic!("expected native floor data, got {other:?}"),
    }

    task.abort();
    let _ = task.await;
}

#[tokio::test]
async fn d6_native_permit_relay_rearms_only_after_egress_permit_release() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let chunk_len = 64 * 1024;
    let queue_capacity = 128 * 1024;
    let chunks = (0..((queue_capacity / chunk_len) + 4))
        .map(|idx| NativeTcpChunk {
            offset: (idx * chunk_len) as u64,
            bytes: Bytes::from(vec![7; chunk_len]),
        })
        .collect();
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks,
        eof_after_chunks: false,
    });
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(256);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: chunk_len,
    });
    let task = tokio::spawn(run_relay_native_permit(
        handle,
        95,
        reader,
        writer,
        rx,
        back_tx,
        credit_rx,
        queue_capacity,
        false,
    ));

    let mut leased_payload_bytes = 0usize;
    let mut permits = Vec::new();
    tokio::time::timeout(std::time::Duration::from_millis(500), async {
        while leased_payload_bytes < queue_capacity {
            match back_rx
                .recv()
                .await
                .expect("native permit pump should emit data")
            {
                (
                    h,
                    RelayEvent::Data {
                        epoch,
                        bytes,
                        downstream_permit,
                        ..
                    },
                ) => {
                    assert_eq!(h, handle);
                    assert_eq!(epoch, 95);
                    leased_payload_bytes = leased_payload_bytes.saturating_add(bytes.len());
                    let permit =
                        downstream_permit.expect("native D6 data must carry downstream permit");
                    assert_eq!(
                        permit.release_mode(),
                        DownstreamPermitReleaseMode::OnEgressDrain
                    );
                    permits.push(permit);
                }
                other => panic!("expected native permit data, got {other:?}"),
            }
        }
    })
    .await
    .expect("native permit pump should fill leased capacity");
    assert_eq!(leased_payload_bytes, queue_capacity);

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), back_rx.recv())
            .await
            .is_err(),
        "native permit pump must stop while all bytes are leased downstream"
    );

    assert_eq!(permits[0].release(chunk_len), chunk_len);
    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("egress permit release should re-arm native remote reads")
        .expect("relay should remain alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                downstream_permit,
                ..
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 95);
            assert_eq!(bytes.len(), chunk_len);
            assert!(
                downstream_permit.is_some(),
                "newly re-armed native data must carry a downstream permit"
            );
        }
        other => panic!("expected native data after permit release, got {other:?}"),
    }

    task.abort();
    let _ = task.await;
}

#[test]
fn d16_actor_mode_rejects_non_actor_downlink_admission() {
    assert!(!DownlinkAdmissionOrigin::ControlOnly.allows_d16_admission());
    assert!(DownlinkAdmissionOrigin::EgressActor.allows_d16_admission());
}

#[test]
fn protocol_owned_flow_joins_existing_local_egress_actor_without_d16_queue() {
    let legacy = SocketCtx::new(443);
    assert!(!legacy.uses_owned_egress_actor());

    let d16 = d16_admission_test_ctx(0);
    assert!(d16.uses_owned_egress_actor());

    let mut resumable = SocketCtx::new(443);
    resumable.downlink_d3_actor_flow = true;
    assert!(resumable.uses_owned_egress_actor());
    let mut called = false;
    let blocked = with_downlink_admission(
        &mut resumable,
        DownlinkAdmissionOrigin::ControlOnly,
        |_, _| {
            called = true;
            DownlinkFlushOutcome {
                accepted_bytes: 1,
                headroom_limited: false,
            }
        },
    );
    assert_eq!(blocked, DownlinkFlushOutcome::NO_ADMISSION);
    assert!(
        !called,
        "ControlOnly must not reach protocol-owned send_slice"
    );

    let admitted = with_downlink_admission(
        &mut resumable,
        DownlinkAdmissionOrigin::EgressActor,
        |_, queue_flow| {
            assert!(!queue_flow, "resumable bytes stay in reducer ownership");
            DownlinkFlushOutcome {
                accepted_bytes: 1,
                headroom_limited: false,
            }
        },
    );
    assert_eq!(admitted.accepted_bytes, 1);
    assert_eq!(resumable.downlink_diag.actor_admitted_bytes, 1);
    assert_eq!(resumable.downlink_diag.actor_bypass_admitted_bytes, 0);
}

#[test]
fn d16_remote_eof_requires_closed_and_empty_queue() {
    assert!(!remote_eof_ready_for_local_close(false, true, 0, 0));
    assert!(!remote_eof_ready_for_local_close(true, false, 1, 0));
    assert!(remote_eof_ready_for_local_close(true, true, 0, 0));
}

fn d16_admission_test_ctx(pending_bytes: usize) -> SocketCtx {
    let mut ctx = SocketCtx::new(443);
    ctx.d16_downlink_queue = Some(
        AsyncLeasedByteFlowQueue::new_with_release_mode(
            512 * 1024,
            DownstreamPermitReleaseMode::OnEgressDrain,
        )
        .unwrap(),
    );
    ctx.downlink_pending.resize(pending_bytes, 7);
    ctx
}

#[test]
fn d16_timer_control_pass_does_not_call_send_slice() {
    let mut ctx = d16_admission_test_ctx(4096);
    let mut admission_called = false;
    let outcome =
        with_downlink_admission(&mut ctx, DownlinkAdmissionOrigin::ControlOnly, |ctx, _| {
            admission_called = true;
            ctx.downlink_diag.note_send_slice_ok(4096, 0);
            DownlinkFlushOutcome {
                accepted_bytes: 4096,
                headroom_limited: false,
            }
        });

    assert!(!admission_called);
    assert_eq!(outcome, DownlinkFlushOutcome::NO_ADMISSION);
    assert_eq!(ctx.downlink_pending.len(), 4096);
    assert_eq!(ctx.downlink_diag.send_slice_calls, 0);
    assert_eq!(ctx.downlink_diag.send_slice_accepted_bytes, 0);
    assert_eq!(ctx.downlink_diag.actor_bypass_admitted_bytes, 0);
}

#[test]
fn d16_tun_rx_control_pass_does_not_call_send_slice() {
    let mut ctx = d16_admission_test_ctx(8192);
    let outcome =
        with_downlink_admission(&mut ctx, DownlinkAdmissionOrigin::ControlOnly, |ctx, _| {
            let accepted = ctx.downlink_pending.len();
            ctx.admit_downlink_pending_prefix(accepted);
            ctx.downlink_diag.note_send_slice_ok(accepted, 0);
            DownlinkFlushOutcome {
                accepted_bytes: accepted,
                headroom_limited: false,
            }
        });

    assert_eq!(outcome.accepted_bytes, 0);
    assert_eq!(ctx.downlink_pending.len(), 8192);
    assert_eq!(ctx.downlink_diag.send_slice_calls, 0);
    assert_eq!(ctx.downlink_diag.actor_bypass_admitted_bytes, 0);
}

#[test]
fn d16_actor_pass_admits_pending_bytes() {
    let mut ctx = d16_admission_test_ctx(16 * 1024);
    let outcome =
        with_downlink_admission(&mut ctx, DownlinkAdmissionOrigin::EgressActor, |ctx, _| {
            let accepted = ctx.downlink_pending.len();
            ctx.admit_downlink_pending_prefix(accepted);
            ctx.downlink_diag
                .note_send_slice_ok(accepted, ctx.downlink_pending.len());
            DownlinkFlushOutcome {
                accepted_bytes: accepted,
                headroom_limited: false,
            }
        });

    assert_eq!(outcome.accepted_bytes, 16 * 1024);
    assert!(ctx.downlink_pending.is_empty());
    assert_eq!(ctx.downlink_diag.send_slice_calls, 1);
    assert_eq!(ctx.downlink_diag.actor_admitted_bytes, 16 * 1024);
    assert_eq!(ctx.downlink_diag.actor_bypass_admitted_bytes, 0);
}

#[test]
fn d16_actor_admitted_equals_total_d16_send_slice_accepted() {
    let mut ctx = d16_admission_test_ctx(24 * 1024);
    for accepted in [16 * 1024, 8 * 1024] {
        let outcome =
            with_downlink_admission(&mut ctx, DownlinkAdmissionOrigin::EgressActor, |ctx, _| {
                ctx.admit_downlink_pending_prefix(accepted);
                ctx.downlink_diag
                    .note_send_slice_ok(accepted, ctx.downlink_pending.len());
                DownlinkFlushOutcome {
                    accepted_bytes: accepted,
                    headroom_limited: false,
                }
            });
        assert_eq!(outcome.accepted_bytes, accepted);
    }

    assert_eq!(
        ctx.downlink_diag.actor_admitted_bytes,
        ctx.downlink_diag.send_slice_accepted_bytes
    );
    assert_eq!(ctx.downlink_diag.actor_bypass_admitted_bytes, 0);
}

#[tokio::test]
async fn d16_readiness_keeps_dirty_until_actor_drain() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        512 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    reservation.commit(Bytes::from(vec![9; 64 * 1024])).unwrap();
    let mut ctx = SocketCtx::new(443);
    ctx.d16_downlink_queue = Some(queue);

    assert!(d16_queue_has_actor_work(&ctx));
    let drained = drain_d16_owned_queue_into_pending(&mut ctx, 64 * 1024);
    assert_eq!(drained.bytes, 64 * 1024);
    assert!(!d16_queue_has_actor_work(&ctx));
}

#[test]
fn d16_drain_only_does_not_read_or_admit() {
    let mut ctx = d16_admission_test_ctx(4096);
    ctx.d16_egress_phase = EgressPhase::DrainOnly;

    assert_eq!(
        d16_relay_read_credit(&ctx, false),
        Some(RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        })
    );
    let mut admission_called = false;
    let outcome =
        with_downlink_admission(&mut ctx, DownlinkAdmissionOrigin::ControlOnly, |_, _| {
            admission_called = true;
            DownlinkFlushOutcome {
                accepted_bytes: 4096,
                headroom_limited: false,
            }
        });
    assert!(!admission_called);
    assert_eq!(outcome, DownlinkFlushOutcome::NO_ADMISSION);
}

#[test]
fn d16_read_credit_follows_phase_quantum() {
    let mut ctx = d16_admission_test_ctx(0);
    assert_eq!(
        d16_relay_read_credit(&ctx, false),
        Some(RelayReadCredit {
            paused: false,
            max_batch_bytes: 512 * 1024,
        })
    );

    ctx.d16_egress_phase = EgressPhase::Recovery { clean_cycles: 2 };
    assert_eq!(
        d16_relay_read_credit(&ctx, false),
        Some(RelayReadCredit {
            paused: false,
            max_batch_bytes: 128 * 1024,
        })
    );
}

#[test]
fn d16_tun_rx_backlog_pauses_new_running_flow_read_credit() {
    let ctx = d16_admission_test_ctx(0);

    assert_eq!(
        d16_relay_read_credit(&ctx, true),
        Some(RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        }),
        "a new Running flow must not publish remote read credit while device backlog is active"
    );
}

#[test]
fn d16_new_relay_channel_starts_paused_during_tun_rx_backlog() {
    assert_eq!(
        initial_relay_read_credit(true),
        RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        }
    );
    assert_eq!(initial_relay_read_credit(false), RelayReadCredit::initial());
}

#[tokio::test]
async fn d16_recovery_caps_an_armed_running_reservation_to_one_quantum() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        D16_PER_FLOW_BYTE_CAPACITY,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let reservation = queue
        .reserve_read_up_to(D16_PER_FLOW_BYTE_CAPACITY)
        .await
        .unwrap();
    let mut ctx = SocketCtx::new(443);
    ctx.d16_downlink_queue = Some(queue);
    ctx.d16_egress_phase = EgressPhase::Recovery { clean_cycles: 0 };

    assert_eq!(
        d16_relay_read_credit(&ctx, false),
        Some(RelayReadCredit {
            paused: false,
            max_batch_bytes: D16_ACTOR_QUANTUM_BYTES,
        }),
        "Recovery must resize, not permanently pause, an armed Running reservation"
    );
    drop(reservation);
}

#[tokio::test]
async fn d16_single_readiness_drains_coalesced_queue_into_pending() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        512 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let first = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert!(first.commit(Bytes::from(vec![1; 64 * 1024])).unwrap());
    let second = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert!(!second.commit(Bytes::from(vec![2; 64 * 1024])).unwrap());
    let third = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert!(!third.commit(Bytes::from(vec![3; 64 * 1024])).unwrap());
    let fourth = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert!(!fourth.commit(Bytes::from(vec![4; 64 * 1024])).unwrap());

    let mut ctx = SocketCtx::new(443);
    ctx.conn_epoch = 7;
    ctx.d16_downlink_queue = Some(queue.clone());
    let first_drain = drain_d16_owned_queue_into_pending(&mut ctx, 128 * 1024);
    let second_drain = drain_d16_owned_queue_into_pending(&mut ctx, 128 * 1024);

    assert_eq!(first_drain.bytes, 128 * 1024);
    assert!(!first_drain.queue_closed);
    assert_eq!(second_drain.bytes, 128 * 1024);
    assert!(!second_drain.queue_closed);
    assert_eq!(ctx.downlink_pending.len(), 256 * 1024);
    assert_eq!(ctx.downlink_pending_permits.len(), 2);
    let snapshot = queue.snapshot().await;
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.leased_bytes, 256 * 1024);
    assert_eq!(snapshot.reserved_bytes, 0);
    assert!(snapshot.owned_bytes() <= 512 * 1024);

    let next = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    assert!(
        next.commit(Bytes::from(vec![5; 64 * 1024])).unwrap(),
        "an exact-quantum drain must rearm readiness after observing open-empty"
    );
}

#[tokio::test]
async fn d16_pending_read_pause_refunds_reservation_before_delivery() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let entered = Arc::new(AtomicBool::new(false));
    let reader: NativeTcpReadHalf = Box::new(PendingD16NativeReader {
        entered: entered.clone(),
    });
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        128 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let (credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 64 * 1024,
    });
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run_d16_native_reader(
        handle,
        7,
        reader,
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
    ));

    tokio::time::timeout(std::time::Duration::from_millis(200), async {
        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("D16 reader should arm the pending remote read");
    assert_eq!(queue.snapshot().await.reserved_bytes, 64 * 1024);

    credit_tx
        .send(RelayReadCredit {
            paused: true,
            max_batch_bytes: 0,
        })
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(200), async {
        while queue.snapshot().await.reserved_bytes != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pause must cancel the pending read and refund its reservation");
    assert!(back_rx.try_recv().is_err());

    let _ = stop_tx.send(());
    task.await.unwrap();
}

#[tokio::test]
async fn d16_running_credit_preserves_its_pending_read_reservation() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let ready = Arc::new(AtomicBool::new(false));
    let read_waker = Arc::new(Mutex::new(None));
    let reader: NativeTcpReadHalf = Box::new(WakeableD16NativeReader {
        ready: ready.clone(),
        read_waker: read_waker.clone(),
        sent: false,
    });
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        D16_PER_FLOW_BYTE_CAPACITY,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let initial_credit = RelayReadCredit::initial();
    let (credit_tx, credit_rx) = watch::channel(initial_credit);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run_d16_native_reader(
        handle,
        11,
        reader,
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
    ));

    tokio::time::timeout(std::time::Duration::from_millis(200), async {
        while queue.snapshot_now().reserved_bytes != D16_PER_FLOW_BYTE_CAPACITY
            || read_waker.lock().unwrap().is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("D16 reader should hold one full-capacity pending reservation");

    credit_tx
        .send(RelayReadCredit {
            paused: false,
            max_batch_bytes: D16_ACTOR_QUANTUM_BYTES,
        })
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert_eq!(
        queue.snapshot_now().reserved_bytes,
        D16_PER_FLOW_BYTE_CAPACITY,
        "a quantitative Running credit update must not cancel or resize the already-armed owned read"
    );

    let mut ctx = SocketCtx::new(443);
    ctx.d16_downlink_queue = Some(queue.clone());
    let republished_credit =
        d16_relay_read_credit(&ctx, false).expect("D16 flow should publish read feedback");
    assert_eq!(
        republished_credit, initial_credit,
        "a Running read reservation is the already-armed opportunity, not a reason to pause itself"
    );
    if *credit_tx.borrow() != republished_credit {
        credit_tx.send(republished_credit).unwrap();
    }

    ready.store(true, Ordering::SeqCst);
    read_waker
        .lock()
        .unwrap()
        .take()
        .expect("pending D16 read should have registered a waker")
        .wake();
    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
            .await
            .expect("remote readability must reach the readiness-only queue"),
        Some((h, RelayEvent::DataReady { epoch: 11 })) if h == handle
    ));
    let snapshot = queue.snapshot_now();
    assert_eq!(snapshot.queued_bytes, b"after-reservation-rearm".len());
    assert!(snapshot.owned_bytes() <= D16_PER_FLOW_BYTE_CAPACITY);

    let _ = stop_tx.send(());
    task.await.unwrap();
}

#[tokio::test]
async fn d16_real_quinn_reader_sustains_byte_owned_queue_progress() {
    let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
    const PAYLOAD_BYTES: usize = 32 * 1024 * 1024;
    const WRITE_BYTES: usize = 64 * 1024;

    let (server_endpoint, client_endpoint, server_addr) = crate::tuic::d16_quinn_test_endpoints();

    let (client_read_tx, client_read_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let mut marker = [0u8; 1];
        recv.read_exact(&mut marker).await.unwrap();
        assert_eq!(marker, [0x5c]);
        let payload = vec![0x7e; WRITE_BYTES];
        for _ in 0..PAYLOAD_BYTES / WRITE_BYTES {
            send.write_all(&payload).await.unwrap();
        }
        send.finish().unwrap();
        let _ = client_read_rx.await;
    });

    let connection = client_endpoint
        .connect(server_addr, "example.com")
        .unwrap()
        .await
        .unwrap();
    let (mut send, recv) = connection.open_bi().await.unwrap();
    send.write_all(&[0x5c]).await.unwrap();
    let reader = crate::tuic::d16_native_ordered_reader_for_test(recv, connection);

    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        D16_PER_FLOW_BYTE_CAPACITY,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit::initial());
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let reader_task = tokio::spawn(run_d16_native_reader(
        handle,
        29,
        reader,
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
    ));

    let started = std::time::Instant::now();
    let mut received = 0usize;
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let Some((event_handle, event)) = back_rx.recv().await else {
                break;
            };
            assert_eq!(event_handle, handle);
            assert!(matches!(event, RelayEvent::DataReady { epoch: 29 }));
            loop {
                match queue.try_recv_up_to(D16_ACTOR_QUANTUM_BYTES) {
                    LeasedQueuePoll::Data(bytes) => {
                        received = received.saturating_add(bytes.bytes().len());
                        drop(bytes);
                    }
                    LeasedQueuePoll::Empty => break,
                    LeasedQueuePoll::Closed => return,
                }
            }
        }
    })
    .await
    .expect("the byte-owned D16 reader must not develop a multi-second queue service gap");
    reader_task.await.unwrap();
    let elapsed = started.elapsed();
    let receiver_mbps = received as f64 * 8.0 / elapsed.as_secs_f64() / 1_000_000.0;

    assert_eq!(received, PAYLOAD_BYTES);
    assert!(queue.snapshot_now().closure.is_closed());
    assert_eq!(queue.snapshot_now().owned_bytes(), 0);
    assert!(
        receiver_mbps >= 170.0,
        "real Quinn plus D16 reservation/readiness queue must have plausible Gate B capacity: elapsed={elapsed:?} rate={receiver_mbps:.1}M"
    );

    let _ = client_read_tx.send(());
    server_task.await.unwrap();
    client_endpoint.close(0u32.into(), b"test complete");
}

#[tokio::test]
async fn h10d16_native_byte_owned_variant_selects_readiness_only_relay() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks: std::collections::VecDeque::from([NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from(vec![7; 32 * 1024]),
        }]),
        eof_after_chunks: true,
    });
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown.clone(),
    });
    let (uplink_tx, uplink_rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);

    let spawned = spawn_remote_relay(
        &RelayFamilyPolicy::default(),
        handle,
        19,
        OpenedTcpRelay::NativeByteOwned(NativeTcpRelayStream { reader, writer }),
        uplink_rx,
        back_tx,
        &Metrics::new(),
        DownlinkBackpressureConfig::default(),
        initial_relay_read_credit(false),
    );
    let queue = spawned
        .d16_downlink_queue
        .expect("the single D16 relay variant must attach its owned byte queue");

    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
            .await
            .expect("the D16 reader must wake the actor")
            .expect("the D16 relay must remain connected"),
        (h, RelayEvent::DataReady { epoch: 19 }) if h == handle
    ));
    assert!(
        back_rx.try_recv().is_err(),
        "D16 composition must not place payload bytes or clean EOF on global_rx"
    );

    tokio::time::timeout(std::time::Duration::from_millis(200), async {
        while !queue.snapshot_now().closure.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("clean EOF must close the owned queue");
    let snapshot = queue.snapshot_now();
    assert_eq!(snapshot.queued_bytes, 32 * 1024);
    assert_eq!(snapshot.reserved_bytes, 0);

    drop(uplink_tx);
    tokio::time::timeout(std::time::Duration::from_millis(200), async {
        while !writer_shutdown.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("closing the uplink command channel must stop the D16 writer");
}

#[tokio::test]
async fn d16_terminal_writer_error_refunds_pending_read_before_close_event() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let entered = Arc::new(AtomicBool::new(false));
    let reader: NativeTcpReadHalf = Box::new(PendingD16NativeReader {
        entered: entered.clone(),
    });
    let writer: NativeTcpWriteHalf = Box::new(FailingWriteStream {
        shutdown_called: Arc::new(AtomicBool::new(false)),
    });
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        128 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let (uplink_tx, uplink_rx) = mpsc::channel::<RelayCommand>(1);
    let (back_tx, mut back_rx) = mpsc::channel(1);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 64 * 1024,
    });
    let task = tokio::spawn(run_relay_d16(
        handle,
        23,
        reader,
        writer,
        uplink_rx,
        back_tx,
        credit_rx,
        queue.clone(),
    ));

    tokio::time::timeout(std::time::Duration::from_millis(200), async {
        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the D16 reader must hold one pending reserved read");
    assert_eq!(queue.snapshot_now().reserved_bytes, 64 * 1024);

    uplink_tx.send(RelayCommand::Data(vec![1])).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(200), async {
        while !queue.snapshot_now().closure.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("writer failure must close the D16 queue");
    assert_eq!(
        queue.snapshot_now().reserved_bytes,
        0,
        "terminal publication must wait for pending-read RAII refund"
    );
    assert_eq!(
        queue.snapshot_now().closure.terminal_cause(),
        Some(crate::tcp_downlink_pump::LeasedByteQueueTerminalCause {
            direction: "local_to_remote",
            reason: "remote_write_failed",
        })
    );

    assert!(matches!(
        tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
            .await
            .expect("writer failure must publish one terminal event"),
        Some((h, RelayEvent::Closed(RelayClose {
            epoch: 23,
            direction: "local_to_remote",
            reason: "remote_write_failed",
        }))) if h == handle
    ));
    task.await.unwrap();
    assert!(back_rx.try_recv().is_err());
}

#[tokio::test]
async fn d16_reader_task_failure_is_terminal_without_payload_idle_timer() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let reader: NativeTcpReadHalf = Box::new(PanickingD16NativeReader);
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown.clone(),
    });
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        D16_PER_FLOW_BYTE_CAPACITY,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let (_uplink_tx, uplink_rx) = mpsc::channel::<RelayCommand>(1);
    let (back_tx, mut back_rx) = mpsc::channel(1);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 64 * 1024,
    });

    run_relay_d16(
        handle,
        26,
        reader,
        writer,
        uplink_rx,
        back_tx,
        credit_rx,
        queue.clone(),
    )
    .await;

    assert!(writer_shutdown.load(Ordering::SeqCst));
    assert_eq!(
        queue.snapshot_now().closure.terminal_cause(),
        Some(crate::tcp_downlink_pump::LeasedByteQueueTerminalCause {
            direction: "remote_to_local",
            reason: "reader_task_failed",
        })
    );
    assert!(matches!(
        back_rx.recv().await,
        Some((h, RelayEvent::Closed(RelayClose {
            epoch: 26,
            direction: "remote_to_local",
            reason: "reader_task_failed",
        }))) if h == handle
    ));
}

#[tokio::test(start_paused = true)]
async fn d16_full_duplex_idle_survives_legacy_timeout() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let reader: NativeTcpReadHalf = Box::new(PendingD16NativeReader {
        entered: Arc::new(AtomicBool::new(false)),
    });
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown.clone(),
    });
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        D16_PER_FLOW_BYTE_CAPACITY,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let (uplink_tx, uplink_rx) = mpsc::channel::<RelayCommand>(1);
    let (back_tx, mut back_rx) = mpsc::channel(1);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: true,
        max_batch_bytes: 0,
    });
    let task = tokio::spawn(run_relay_d16(
        handle,
        24,
        reader,
        writer,
        uplink_rx,
        back_tx,
        credit_rx,
        queue.clone(),
    ));

    tokio::task::yield_now().await;
    tokio::time::advance(RELAY_STALLED_WRITE_TIMEOUT + std::time::Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "D16 must not reap a healthy full-duplex relay because payload is quiet"
    );
    assert!(!writer_shutdown.load(Ordering::SeqCst));
    assert!(!queue.snapshot_now().closure.is_closed());
    assert!(back_rx.try_recv().is_err());

    drop(uplink_tx);
    task.await.unwrap();
    assert!(writer_shutdown.load(Ordering::SeqCst));
    assert!(matches!(
        back_rx.recv().await,
        Some((h, RelayEvent::Closed(RelayClose {
            epoch: 24,
            direction: "local_to_remote",
            reason: "local_channel_closed",
        }))) if h == handle
    ));
    assert!(back_rx.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn d16_half_closed_idle_waits_for_owned_payload_to_drain() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks: std::collections::VecDeque::from([NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from(vec![7; 64 * 1024]),
        }]),
        eof_after_chunks: false,
    });
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown.clone(),
    });
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        64 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let (uplink_tx, uplink_rx) = mpsc::channel::<RelayCommand>(1);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 64 * 1024,
    });
    let task = tokio::spawn(run_relay_d16(
        handle,
        25,
        reader,
        writer,
        uplink_rx,
        back_tx,
        credit_rx,
        queue.clone(),
    ));

    assert!(matches!(
        back_rx.recv().await,
        Some((h, RelayEvent::DataReady { epoch: 25 })) if h == handle
    ));
    assert_eq!(queue.snapshot_now().queued_bytes, 64 * 1024);

    uplink_tx.send(RelayCommand::Finish).await.unwrap();
    while !writer_shutdown.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(RELAY_HALF_CLOSED_IDLE_TIMEOUT).await;
    tokio::task::yield_now().await;

    assert!(
        !task.is_finished(),
        "half-close timeout must not terminate while useful D16 payload ownership remains"
    );
    let snapshot = queue.snapshot_now();
    assert_eq!(snapshot.queued_bytes, 64 * 1024);
    assert_eq!(snapshot.leased_bytes, 0);
    assert_eq!(
        snapshot.closure,
        crate::tcp_downlink_pump::LeasedByteQueueClosure::Open
    );
    assert!(back_rx.try_recv().is_err());

    let leased = match queue.try_recv_up_to(64 * 1024) {
        LeasedQueuePoll::Data(leased) => leased,
        other => panic!("expected owned D16 payload, got {other:?}"),
    };
    tokio::time::advance(RELAY_HALF_CLOSED_IDLE_TIMEOUT).await;
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "half-close timeout must also preserve payload leased into pending/inflight ownership"
    );
    let snapshot = queue.snapshot_now();
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.leased_bytes, 64 * 1024);
    assert_eq!(
        snapshot.closure,
        crate::tcp_downlink_pump::LeasedByteQueueClosure::Open
    );
    assert!(back_rx.try_recv().is_err());

    let (bytes, mut permit) = leased.into_parts();
    assert_eq!(bytes.len(), 64 * 1024);
    assert_eq!(permit.release(bytes.len()), bytes.len());
    drop(permit);

    tokio::time::advance(RELAY_HALF_CLOSED_IDLE_TIMEOUT).await;
    task.await.unwrap();
    assert_eq!(queue.snapshot_now().owned_bytes(), 0);
    assert!(matches!(
        back_rx.recv().await,
        Some((h, RelayEvent::Closed(RelayClose {
            epoch: 25,
            direction: "timer",
            reason: "half_closed_idle_timeout",
        }))) if h == handle
    ));
    assert!(back_rx.try_recv().is_err());
}

#[tokio::test(start_paused = true)]
async fn d16_half_closed_idle_owned_payload_without_local_progress_is_bounded() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks: std::collections::VecDeque::from([NativeTcpChunk {
            offset: 0,
            bytes: Bytes::from(vec![7; 64 * 1024]),
        }]),
        eof_after_chunks: false,
    });
    let writer_shutdown = Arc::new(AtomicBool::new(false));
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: writer_shutdown.clone(),
    });
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        64 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let (uplink_tx, uplink_rx) = mpsc::channel::<RelayCommand>(1);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 64 * 1024,
    });
    let task = tokio::spawn(run_relay_d16(
        handle,
        26,
        reader,
        writer,
        uplink_rx,
        back_tx,
        credit_rx,
        queue.clone(),
    ));

    assert!(matches!(
        back_rx.recv().await,
        Some((h, RelayEvent::DataReady { epoch: 26 })) if h == handle
    ));
    assert_eq!(queue.snapshot_now().queued_bytes, 64 * 1024);

    uplink_tx.send(RelayCommand::Finish).await.unwrap();
    while !writer_shutdown.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }

    tokio::time::advance(RELAY_HALF_CLOSED_IDLE_TIMEOUT).await;
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "the first half-close deadline must preserve useful D16 ownership"
    );

    tokio::time::advance(RELAY_HALF_CLOSED_IDLE_TIMEOUT).await;
    tokio::task::yield_now().await;
    assert!(
        task.is_finished(),
        "unchanged D16 ownership must not re-arm the half-close timer forever"
    );
    task.await.unwrap();
    assert!(matches!(
        back_rx.recv().await,
        Some((h, RelayEvent::Closed(RelayClose {
            epoch: 26,
            direction: "timer",
            reason: "half_closed_idle_timeout",
        }))) if h == handle
    ));
}

#[tokio::test]
async fn d16_local_terminal_cause_survives_writer_shutdown() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks: std::collections::VecDeque::new(),
        eof_after_chunks: true,
    });
    let writer: NativeTcpWriteHalf = Box::new(NoopNativeWriter {
        shutdown_called: Arc::new(AtomicBool::new(false)),
    });
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        D16_PER_FLOW_BYTE_CAPACITY,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    queue.close_and_drop_queued_for_terminal("local_to_remote", "local_socket_terminal");
    let (uplink_tx, uplink_rx) = mpsc::channel::<RelayCommand>(1);
    let (back_tx, mut back_rx) = mpsc::channel(1);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit::initial());
    drop(uplink_tx);

    run_relay_d16(
        handle, 24, reader, writer, uplink_rx, back_tx, credit_rx, queue,
    )
    .await;

    assert!(matches!(
        back_rx.recv().await,
        Some((h, RelayEvent::Closed(RelayClose {
            epoch: 24,
            direction: "local_to_remote",
            reason: "local_socket_terminal",
        }))) if h == handle
    ));
    assert!(back_rx.try_recv().is_err());
}

#[tokio::test]
async fn d16_reader_emits_one_readiness_and_no_payload_event_before_closed_queue_drain() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let reader: NativeTcpReadHalf = Box::new(BurstNativeReader {
        chunks: std::collections::VecDeque::from([
            NativeTcpChunk {
                offset: 0,
                bytes: Bytes::from(vec![1; 32 * 1024]),
            },
            NativeTcpChunk {
                offset: 32 * 1024,
                bytes: Bytes::from(vec![2; 32 * 1024]),
            },
        ]),
        eof_after_chunks: true,
    });
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        512 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: 128 * 1024,
    });
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_stop_tx, stop_rx) = tokio::sync::oneshot::channel();

    run_d16_native_reader(
        handle,
        9,
        reader,
        queue.clone(),
        credit_rx,
        back_tx,
        None,
        stop_rx,
    )
    .await;

    assert!(matches!(
        back_rx.recv().await,
        Some((h, RelayEvent::DataReady { epoch: 9 })) if h == handle
    ));
    assert!(
        back_rx.try_recv().is_err(),
        "coalesced D16 chunks and clean EOF must not emit Data or Closed payload events"
    );

    let mut ctx = SocketCtx::new(443);
    ctx.conn_epoch = 9;
    ctx.d16_downlink_queue = Some(queue.clone());
    let drained = drain_d16_owned_queue_into_pending(&mut ctx, 128 * 1024);
    assert_eq!(drained.bytes, 64 * 1024);
    assert!(drained.queue_closed);
    assert_eq!(ctx.downlink_pending.len(), 64 * 1024);
    let snapshot = queue.snapshot().await;
    assert!(snapshot.closure.is_closed());
    assert_eq!(snapshot.closure.terminal_cause(), None);
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.leased_bytes, 64 * 1024);
    assert_eq!(snapshot.reserved_bytes, 0);
}

#[tokio::test]
async fn d16_data_ready_payload_is_drained_before_remote_eof() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        512 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    reservation.commit(Bytes::from(vec![3; 64 * 1024])).unwrap();
    queue.close_for_remote_eof_and_mark_ready().await;
    let mut ctx = SocketCtx::new(443);
    ctx.conn_epoch = 12;
    ctx.d16_downlink_queue = Some(queue.clone());

    assert!(
        !install_d16_remote_eof_if_ready(&mut ctx, 7),
        "queued payload must keep clean EOF invisible"
    );
    let drained = drain_d16_owned_queue_into_pending(&mut ctx, 128 * 1024);
    assert_eq!(drained.bytes, 64 * 1024);
    assert!(drained.queue_closed);
    assert!(install_d16_remote_eof_if_ready(&mut ctx, 7));
    assert_eq!(ctx.downlink_pending.len(), 64 * 1024);
    assert_eq!(ctx.downlink_pending_permits.len(), 1);
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 0);
    assert_eq!(
        ctx.pending_relay_close.map(|close| close.reason),
        Some("remote_eof")
    );
    assert!(!remote_eof_ready_for_local_close(
        true,
        true,
        ctx.downlink_pending.len(),
        ctx.downlink_inflight_permit_bytes(),
    ));
    let snapshot = queue.snapshot_now();
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.leased_bytes, 64 * 1024);
    assert_eq!(snapshot.reserved_bytes, 0);
}

#[tokio::test]
async fn d16_remote_eof_waits_for_inflight_tun_permit_release() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        512 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
    reservation.commit(Bytes::from(vec![4; 64 * 1024])).unwrap();
    queue.close_for_remote_eof_and_mark_ready().await;
    let mut ctx = SocketCtx::new(443);
    ctx.conn_epoch = 14;
    ctx.d16_downlink_queue = Some(queue.clone());
    let drained = drain_d16_owned_queue_into_pending(&mut ctx, 128 * 1024);
    assert_eq!(drained.bytes, 64 * 1024);
    assert!(drained.queue_closed);
    assert!(install_d16_remote_eof_if_ready(&mut ctx, 10));
    assert_eq!(ctx.admit_downlink_pending_prefix(64 * 1024), 64 * 1024);
    assert!(ctx.downlink_pending.is_empty());
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 64 * 1024);

    let mut pool = FakeIpPool::new();
    assert!(finish_deferred_relay_close_if_drained(
        handle,
        sockets.get_mut::<TcpSocket>(handle),
        &mut ctx,
        &mut pool,
        11,
        DownlinkBackpressureConfig::default(),
        false,
    ));
    assert!(ctx.d16_downlink_queue.is_some());
    assert!(!ctx.local_eof_sent);
    assert_eq!(queue.snapshot_now().leased_bytes, 64 * 1024);

    assert_eq!(ctx.release_downlink_inflight_permits(64 * 1024), 64 * 1024);
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 0);
    assert_eq!(queue.snapshot_now().leased_bytes, 0);
    let (queue_closed, queue_empty) = d16_remote_eof_queue_state(&ctx).unwrap();
    assert!(remote_eof_ready_for_local_close(
        queue_closed,
        queue_empty,
        ctx.downlink_pending.len(),
        ctx.downlink_inflight_permit_bytes(),
    ));
}

#[tokio::test]
async fn d16_local_fin_keeps_useful_reverse_queue_alive() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        512 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    let reservation = queue.reserve_read_up_to(32 * 1024).await.unwrap();
    reservation.commit(Bytes::from(vec![5; 32 * 1024])).unwrap();
    let (tx, mut rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.uplink_tx = Some(tx);
    ctx.d16_downlink_queue = Some(queue.clone());

    assert!(matches!(
        pump_established_uplink(&mut ctx, TcpState::CloseWait, 10, || None),
        EstablishedUplink::Handled
    ));
    assert!(matches!(
        pump_established_uplink(
            &mut ctx,
            TcpState::CloseWait,
            10 + LOCAL_FINISH_DEFER_SECS,
            || None,
        ),
        EstablishedUplink::Handled
    ));
    assert!(matches!(rx.try_recv(), Ok(RelayCommand::Finish)));
    assert!(ctx.local_fin_sent);
    assert!(ctx.d16_downlink_queue.is_some());
    assert!(d16_queue_has_actor_work(&ctx));

    let drained = drain_d16_owned_queue_into_pending(&mut ctx, 128 * 1024);
    assert_eq!(drained.bytes, 32 * 1024);
    assert_eq!(ctx.downlink_pending.len(), 32 * 1024);
    assert_eq!(ctx.downlink_pending_permits.len(), 1);
    assert_eq!(queue.snapshot_now().leased_bytes, 32 * 1024);
}

#[tokio::test]
async fn d16_terminal_local_close_releases_exact_owned_bytes_once() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        512 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    for fill in [6, 7] {
        let reservation = queue.reserve_read_up_to(64 * 1024).await.unwrap();
        reservation
            .commit(Bytes::from(vec![fill; 64 * 1024]))
            .unwrap();
    }
    let mut ctx = SocketCtx::new(443);
    ctx.d16_downlink_queue = Some(queue.clone());
    let drained = drain_d16_owned_queue_into_pending(&mut ctx, 64 * 1024);
    assert_eq!(drained.bytes, 64 * 1024);
    assert_eq!(ctx.admit_downlink_pending_prefix(32 * 1024), 32 * 1024);
    assert_eq!(ctx.downlink_pending.len(), 32 * 1024);
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 32 * 1024);
    assert_eq!(queue.snapshot_now().owned_bytes(), 128 * 1024);

    let dropped = drop_d16_owned_for_terminal(&mut ctx, "local_to_remote", "local_socket_terminal");
    assert_eq!(dropped, 128 * 1024);
    assert!(ctx.downlink_pending.is_empty());
    assert_eq!(ctx.downlink_inflight_permit_bytes(), 0);
    let snapshot = queue.snapshot_now();
    assert!(snapshot.closure.is_closed());
    assert_eq!(snapshot.queued_bytes, 0);
    assert_eq!(snapshot.leased_bytes, 0);
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(ctx.downlink_diag.permit_terminal_drop_bytes, 128 * 1024);
    assert_eq!(ctx.downlink_diag.permit_terminal_drop_events, 1);

    assert_eq!(
        drop_d16_owned_for_terminal(&mut ctx, "local_to_remote", "local_socket_terminal",),
        0
    );
    assert_eq!(ctx.downlink_diag.permit_terminal_drop_bytes, 128 * 1024);
    assert_eq!(ctx.downlink_diag.permit_terminal_drop_events, 1);
}

#[tokio::test]
async fn relay_ready_burst_delivers_data_before_eof_close() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: shutdown_called.clone(),
        chunks: std::collections::VecDeque::from([b"one".to_vec(), b"two".to_vec()]),
        eof_after_chunks: true,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();

    run_relay(handle, 78, stream, rx, back_tx, credit_rx).await;

    match back_rx
        .recv()
        .await
        .expect("coalesced data should arrive first")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window: _,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 78);
            assert_eq!(bytes, b"onetwo");
        }
        other => panic!("expected data before close, got {other:?}"),
    }
    match back_rx
        .recv()
        .await
        .expect("EOF should notify main loop after data")
    {
        (h, RelayEvent::Closed(close)) => {
            assert_eq!(h, handle);
            assert_eq!(close.epoch, 78);
            assert_eq!(close.direction, "remote_to_local");
            assert_eq!(close.reason, "remote_eof");
        }
        other => panic!("expected close after data, got {other:?}"),
    }
    assert!(
        shutdown_called.load(Ordering::SeqCst),
        "EOF path should stop the writer half"
    );
    assert!(back_rx.try_recv().is_err());
}

#[tokio::test]
async fn relay_read_credit_pause_stops_remote_reads_until_resumed() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: shutdown_called.clone(),
        chunks: std::collections::VecDeque::from([b"after-credit".to_vec()]),
        eof_after_chunks: true,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: true,
        max_batch_bytes: 0,
    });
    let task = tokio::spawn(run_relay(handle, 81, stream, rx, back_tx, credit_rx));

    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), back_rx.recv())
            .await
            .is_err(),
        "paused read credit must keep ready remote bytes unread"
    );

    credit_tx.send(RelayReadCredit::default()).unwrap();
    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("remote data should arrive after credit resumes")
        .expect("relay should still be alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 81);
            assert_eq!(bytes, b"after-credit");
            assert_eq!(
                stream_service_window.remote_read_bytes,
                b"after-credit".len() as u64
            );
            assert!(
                stream_service_window.remote_poll_ticks >= 1,
                "resumed credit should have driven at least one remote poll: {stream_service_window:?}"
            );
        }
        other => panic!("expected data after read credit resumes, got {other:?}"),
    }
    task.await.unwrap();
    assert!(
        shutdown_called.load(Ordering::SeqCst),
        "remote EOF after resumed read should stop the writer half"
    );
}

#[tokio::test]
async fn relay_read_credit_update_keeps_inflight_remote_read_armed() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let poll_count = Arc::new(AtomicU64::new(0));
    let ready = Arc::new(AtomicBool::new(false));
    let read_waker = Arc::new(Mutex::new(None));
    let stream: RelayStream = Box::new(CreditWakeReadableStream {
        shutdown_called: shutdown_called.clone(),
        poll_count: poll_count.clone(),
        ready: ready.clone(),
        read_waker: read_waker.clone(),
        read_sent: false,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (credit_tx, credit_rx) = relay_read_credit_channel();
    let task = tokio::spawn(run_relay(handle, 83, stream, rx, back_tx, credit_rx));

    tokio::task::yield_now().await;
    assert_eq!(
        poll_count.load(Ordering::SeqCst),
        1,
        "test stream should have one pending read before egress wake"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), back_rx.recv())
            .await
            .is_err(),
        "a pending remote read should not progress before the remote stream wakes it"
    );

    credit_tx
        .send(RelayReadCredit {
            paused: false,
            max_batch_bytes: relay_remote_read_pressure_floor_bytes(1200),
        })
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), back_rx.recv())
            .await
            .is_err(),
        "non-pausing credit updates must not cancel and recreate an in-flight remote read"
    );
    assert_eq!(
        poll_count.load(Ordering::SeqCst),
        1,
        "non-pausing credit updates should be applied before the next remote read, not by repolling the current one"
    );

    ready.store(true, Ordering::SeqCst);
    read_waker
        .lock()
        .unwrap()
        .take()
        .expect("pending remote read should have registered a waker")
        .wake();
    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("remote stream wake should complete the pending read")
        .expect("relay should still be alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window: _,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 83);
            assert_eq!(bytes, b"after-egress-wake");
        }
        other => panic!("expected remote data after read-credit wake, got {other:?}"),
    }
    task.await.unwrap();
    assert!(
        shutdown_called.load(Ordering::SeqCst),
        "EOF after the test payload should stop the writer half"
    );
}

#[tokio::test]
async fn local_pressure_credit_does_not_cap_ordered_stream_poll_below_dispatch_window() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let pressure_floor = relay_remote_read_pressure_floor_bytes(1200);
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(BurstReadableStream {
        shutdown_called: shutdown_called.clone(),
        chunks: std::iter::repeat_with(|| vec![9; 16 * 1024])
            .take(16)
            .collect(),
        eof_after_chunks: true,
    });
    let (_tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = watch::channel(RelayReadCredit {
        paused: false,
        max_batch_bytes: pressure_floor,
    });
    let task = tokio::spawn(run_relay(handle, 82, stream, rx, back_tx, credit_rx));

    match tokio::time::timeout(std::time::Duration::from_millis(200), back_rx.recv())
        .await
        .expect("stream-poll-service first remote read should arrive")
        .expect("relay should still be alive")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window: _,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 82);
            assert_eq!(
                bytes.len(),
                RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES,
                "local pressure credit should not cap ordered stream polling below one dispatch/egress window"
            );
        }
        other => panic!("expected pressure-limited data batch, got {other:?}"),
    }

    task.await.unwrap();
    assert!(
        shutdown_called.load(Ordering::SeqCst),
        "EOF path should stop the writer half"
    );
}

#[tokio::test]
async fn relay_remote_write_failed_notifies_main_loop() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let flag = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(FailingWriteStream {
        shutdown_called: flag.clone(),
    });
    let (tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();
    let task = tokio::spawn(run_relay(handle, 11, stream, rx, back_tx, credit_rx));

    tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
    task.await.unwrap();

    assert!(
        flag.load(Ordering::SeqCst),
        "write failure should still call stream.shutdown"
    );
    match back_rx
        .try_recv()
        .expect("write failure should notify main loop")
    {
        (h, RelayEvent::Closed(close)) => {
            assert_eq!(h, handle);
            assert_eq!(close.epoch, 11);
            assert_eq!(close.direction, "local_to_remote");
            assert_eq!(close.reason, "remote_write_failed");
        }
        other => panic!("expected relay close event, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn relay_pending_remote_write_uses_stalled_write_timeout() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let flag = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(PendingWriteStream {
        shutdown_called: flag.clone(),
    });
    let (tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();
    let task = tokio::spawn(run_relay(handle, 17, stream, rx, back_tx, credit_rx));

    tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(RELAY_WRITER_STOP_TIMEOUT - std::time::Duration::from_secs(1)).await;
    assert!(
        !task.is_finished(),
        "relay should not close before the former per-write timeout"
    );
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert!(
        !task.is_finished(),
        "normal upstream backpressure must not close the relay after a fixed per-write timeout"
    );
    assert!(
        back_rx.try_recv().is_err(),
        "pending write should not emit remote_write_timeout"
    );

    tokio::time::advance(RELAY_STALLED_WRITE_TIMEOUT).await;
    task.await.unwrap();

    assert!(
        flag.load(Ordering::SeqCst),
        "stalled-write cleanup should still call stream.shutdown"
    );
    match back_rx
        .try_recv()
        .expect("stalled-write timeout should notify main loop")
    {
        (h, RelayEvent::Closed(close)) => {
            assert_eq!(h, handle);
            assert_eq!(close.epoch, 17);
            assert_eq!(close.direction, "timer");
            assert_eq!(close.reason, "stalled_write_timeout");
        }
        other => panic!("expected relay close event, got {other:?}"),
    }
}

#[tokio::test(start_paused = true)]
async fn relay_partial_write_progress_resets_stalled_write_timeout() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let allowed_steps = Arc::new(AtomicUsize::new(0));
    let write_waker = Arc::new(Mutex::new(None));
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(StepwisePendingWriteStream {
        allowed_steps: allowed_steps.clone(),
        completed_steps: 0,
        write_waker: write_waker.clone(),
        shutdown_called: shutdown_called.clone(),
    });
    let (tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();
    let task = tokio::spawn(run_relay(handle, 18, stream, rx, back_tx, credit_rx));

    tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
    while write_waker.lock().unwrap().is_none() {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(RELAY_STALLED_WRITE_TIMEOUT - std::time::Duration::from_secs(1)).await;
    allowed_steps.store(1, Ordering::SeqCst);
    write_waker
        .lock()
        .unwrap()
        .take()
        .expect("pending partial write must retain its waker")
        .wake();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    tokio::time::advance(RELAY_STALLED_WRITE_TIMEOUT - std::time::Duration::from_secs(1)).await;
    assert!(
        !task.is_finished(),
        "one real partial write must reset the stalled-write no-progress window"
    );
    assert!(back_rx.try_recv().is_err());

    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    task.await.unwrap();
    assert!(shutdown_called.load(Ordering::SeqCst));
    assert!(matches!(
        back_rx.try_recv(),
        Ok((h, RelayEvent::Closed(RelayClose {
            epoch: 18,
            direction: "timer",
            reason: "stalled_write_timeout",
        }))) if h == handle
    ));
}

#[tokio::test(start_paused = true)]
async fn relay_remote_read_progresses_while_local_write_is_pending() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let write_polled = Arc::new(AtomicBool::new(false));
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(PendingWriteThenReadableStream {
        write_polled: write_polled.clone(),
        shutdown_called: shutdown_called.clone(),
        read_sent: false,
    });
    let (tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();
    let task = tokio::spawn(run_relay(handle, 23, stream, rx, back_tx, credit_rx));

    tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    assert!(
        write_polled.load(Ordering::SeqCst),
        "test stream should have entered the pending write"
    );
    match back_rx
        .try_recv()
        .expect("remote read should not wait for write timeout")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window: _,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 23);
            assert_eq!(bytes, b"remote-progress");
        }
        other => panic!("expected remote data before write timeout, got {other:?}"),
    }
    assert!(
        !shutdown_called.load(Ordering::SeqCst),
        "relay should still be alive while the pending write is not blocking remote reads"
    );

    tokio::time::advance(RELAY_STALLED_WRITE_TIMEOUT).await;
    task.await.unwrap();
}

#[tokio::test]
async fn relay_local_finish_keeps_remote_read_open() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(LocalFinishThenReadableStream {
        shutdown_called: shutdown_called.clone(),
        read_sent: false,
    });
    let (tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();
    let task = tokio::spawn(run_relay(handle, 29, stream, rx, back_tx, credit_rx));

    tx.send(RelayCommand::Finish).await.unwrap();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    assert!(
        shutdown_called.load(Ordering::SeqCst),
        "local finish should shutdown only the remote write half"
    );
    match back_rx
        .recv()
        .await
        .expect("remote data should still reach main loop")
    {
        (
            h,
            RelayEvent::Data {
                epoch,
                bytes,
                stream_service_window: _,
                downstream_permit: _,
            },
        ) => {
            assert_eq!(h, handle);
            assert_eq!(epoch, 29);
            assert_eq!(bytes, b"after-local-finish");
        }
        other => panic!("expected remote data after local finish, got {other:?}"),
    }

    task.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn relay_local_finish_without_remote_progress_uses_half_closed_idle_timeout() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let shutdown_called = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(IdleStream {
        shutdown_called: shutdown_called.clone(),
    });
    let (tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();
    let task = tokio::spawn(run_relay(handle, 31, stream, rx, back_tx, credit_rx));

    tx.send(RelayCommand::Finish).await.unwrap();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    assert!(
        shutdown_called.load(Ordering::SeqCst),
        "Finish should shutdown the remote write half"
    );

    tokio::time::advance(RELAY_HALF_CLOSED_IDLE_TIMEOUT - std::time::Duration::from_secs(1)).await;
    assert!(
        !task.is_finished(),
        "half-closed relay should stay open before the short idle timeout"
    );
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    task.await.unwrap();

    match back_rx
        .try_recv()
        .expect("half-closed idle should notify main loop")
    {
        (h, RelayEvent::Closed(close)) => {
            assert_eq!(h, handle);
            assert_eq!(close.epoch, 31);
            assert_eq!(close.direction, "timer");
            assert_eq!(close.reason, "half_closed_idle_timeout");
        }
        other => panic!("expected half-closed relay close event, got {other:?}"),
    }
}

/// A completed write returns the full-duplex relay to a disarmed lifecycle state.
#[tokio::test(start_paused = true)]
async fn relay_completed_write_disarms_stalled_write_timer() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let flag = Arc::new(AtomicBool::new(false));
    let stream: RelayStream = Box::new(IdleStream {
        shutdown_called: flag.clone(),
    });
    let (tx, rx) = mpsc::channel::<RelayCommand>(8);
    let (back_tx, mut back_rx) = mpsc::channel(8);
    let (_credit_tx, credit_rx) = relay_read_credit_channel();
    let task = tokio::spawn(run_relay(handle, 13, stream, rx, back_tx, credit_rx));

    tx.send(RelayCommand::Data(vec![1, 2, 3])).await.unwrap();
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;
    tokio::time::advance(RELAY_STALLED_WRITE_TIMEOUT + std::time::Duration::from_secs(1)).await;
    assert!(
        !task.is_finished(),
        "a completed write must disarm the stalled-write timer"
    );
    assert!(back_rx.try_recv().is_err());

    drop(tx);
    task.await.unwrap();
    assert!(flag.load(Ordering::SeqCst));
}

#[test]
fn target_from_endpoint_builds_ipv4_target() {
    let ep = smoltcp::wire::IpEndpoint::new(IpAddress::v4(93, 184, 216, 34), 80);
    let target = target_from_endpoint(ep);
    assert_eq!(target.to_wire_string(), "93.184.216.34:80");
}

/// MetricsSink 契约：自定义 sink 能覆写默认空实现，逐回调被调用、listener 计数透传。
/// 中文要点：锁住 run_event_loop 的插桩接缝形状（生产 NoopSink 零开销、harness 可记录）。
#[derive(Default)]
struct CountingSink {
    poll_enters: usize,
    relay_enters: usize,
    last_listeners: usize,
}
impl MetricsSink for CountingSink {
    fn enter_poll(&mut self) {
        self.poll_enters += 1;
    }
    fn enter_relay(&mut self) {
        self.relay_enters += 1;
    }
    fn note_listeners(&mut self, n: usize) {
        self.last_listeners = n;
    }
}

#[test]
fn metrics_sink_records_per_phase_calls() {
    let mut sink = CountingSink::default();
    // 模拟一个 tick：poll 段 + relay 段（遍历 7 个 listener）。
    sink.enter_poll();
    sink.leave_poll();
    sink.enter_relay();
    sink.note_listeners(7);
    sink.leave_relay();
    assert_eq!(sink.poll_enters, 1);
    assert_eq!(sink.relay_enters, 1);
    assert_eq!(sink.last_listeners, 7);
}

#[test]
fn noop_sink_is_zero_state() {
    // NoopSink 全空实现：可被反复调用且无副作用（生产热路径零开销的依据）。
    let mut sink = NoopSink;
    sink.enter_poll();
    sink.leave_poll();
    sink.enter_relay();
    sink.note_listeners(1024);
    sink.leave_relay();
}

/// Build a minimal IPv4+TCP packet with the requested flags for SYN-inspector tests.
fn build_ipv4_tcp(
    src: [u8; 4],
    dst: [u8; 4],
    src_port: u16,
    dst_port: u16,
    syn: bool,
    ack: bool,
) -> Vec<u8> {
    let b = etherparse::PacketBuilder::ipv4(src, dst, 64).tcp(src_port, dst_port, 0, 1024);
    let b = if syn { b.syn() } else { b };
    let b = if ack { b.ack(0) } else { b };
    let mut buf = Vec::new();
    let payload: [u8; 0] = [];
    b.write(&mut buf, &payload).unwrap();
    buf
}

#[test]
fn inspect_inbound_tcp_flags_clean_syn() {
    // 干净 SYN → (端口, is_clean_syn=true)。
    let syn = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 443, true, false);
    assert_eq!(inspect_inbound_tcp(&syn), Some((443, true)));
    // SYN-ACK → 端口仍返回（用于标脏），但非干净 SYN（不建池）。
    let synack = build_ipv4_tcp([1, 1, 1, 1], [10, 0, 0, 1], 443, 60000, true, true);
    assert_eq!(inspect_inbound_tcp(&synack), Some((60000, false)));
    // 纯 ACK / data 包 → 端口返回，非 SYN（首包数据让 listener can_recv 的那一刻）。
    let ack = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 80, false, true);
    assert_eq!(inspect_inbound_tcp(&ack), Some((80, false)));
}

#[test]
fn inspect_inbound_tcp_rejects_non_tcp_and_garbage() {
    // 非 TCP（UDP）/ 垃圾 → None。
    assert_eq!(inspect_inbound_tcp(&udp_pkt([8, 8, 8, 8], 53)), None);
    assert_eq!(inspect_inbound_tcp(&[0u8; 4]), None);
}

#[test]
fn registry_ensure_port_is_idempotent_and_capped() {
    let mut sockets = SocketSet::new(vec![]);
    let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
    let mut reg = ListenerRegistry::new(2);

    for i in 0..MAX_INTERCEPTED_PORTS as u16 {
        reg.ensure_port(i + 1, &mut sockets, &mut ctxs).unwrap();
    }
    assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
    // pool_size * port_count handles registered
    assert_eq!(ctxs.len(), 2 * MAX_INTERCEPTED_PORTS);

    // idempotent: re-adding an existing port does not grow the registry
    reg.ensure_port(1, &mut sockets, &mut ctxs).unwrap();
    assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
    assert_eq!(ctxs.len(), 2 * MAX_INTERCEPTED_PORTS);

    // capped: a new port beyond the cap is rejected, existing state preserved
    let err = reg.ensure_port(9999, &mut sockets, &mut ctxs).unwrap_err();
    assert!(matches!(err, RegistryError::Capped));
    assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
}

/// #2 弹性扩容：端口 Listening 槽不足 min_spare 时按需补建，已够则幂等不动，
/// 未注册端口 no-op；rearm 回 Listening 的槽计入空闲、可复用。
#[test]
fn ensure_spare_listeners_grows_and_is_idempotent() {
    let mut sockets = SocketSet::new(vec![]);
    let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
    let mut reg = ListenerRegistry::new(2);
    reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
    assert_eq!(reg.handles_for_port(443).len(), 2);

    // 占满已有 2 槽（abort → 离开 Listen 状态，模拟被 accept 占用）→ 无空闲 listening。
    let hs: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
    for h in &hs {
        sockets.get_mut::<TcpSocket>(*h).abort();
    }
    // 要求 ≥2 空闲 → 补建 2 个。
    reg.ensure_spare_listeners(443, 2, &mut sockets, &mut ctxs)
        .unwrap();
    assert_eq!(reg.handles_for_port(443).len(), 4);
    // 幂等：已有 2 空闲，不再建。
    reg.ensure_spare_listeners(443, 2, &mut sockets, &mut ctxs)
        .unwrap();
    assert_eq!(reg.handles_for_port(443).len(), 4);
    // 未注册端口 → no-op（建池仍由 ensure_port 负责）。
    reg.ensure_spare_listeners(8080, 2, &mut sockets, &mut ctxs)
        .unwrap();
    assert!(reg.handles_for_port(8080).is_empty());
}

/// review #1/#2：reap_dead_slots 回收已用过且已死的槽（abort→Closed），rearm 回 Listen +
/// release 其 fake-IP；空闲 Listen 槽（ctx.state==Listening）不被回收。
#[test]
fn reap_dead_slots_rearms_closed_slot_and_releases_fake_ip() {
    let mut sockets = SocketSet::new(vec![]);
    let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
    let mut reg = ListenerRegistry::new(2);
    reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("x.com", 0).expect("fake-IP capacity");
    pool.acquire(ip, 0);

    let handles: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
    let dead = handles[0];
    let idle_listen = handles[1];
    // dead 槽：模拟被用过后本地关闭（abort → Closed）+ 持有 fake-IP。
    sockets.get_mut::<TcpSocket>(dead).abort();
    {
        let ctx = ctxs.get_mut(&dead).unwrap();
        ctx.state = SocketState::Relaying;
        ctx.fake_ip = Some(ip);
    }
    // idle_listen 槽：空闲监听（ctx.state 默认 Listening）→ 不该被回收。

    let reaped = reap_dead_slots(&reg, &mut sockets, &mut ctxs, &mut pool, 1);
    assert_eq!(reaped, 1, "只回收 1 个死槽");
    let dead_ctx = ctxs.get(&dead).unwrap();
    assert_eq!(dead_ctx.state, SocketState::Listening, "死槽回 Listening");
    assert!(dead_ctx.fake_ip.is_none(), "死槽 fake-IP 已 release");
    assert_eq!(pool.sweep(1000, 300), 1, "release 后映射可回收");
    assert_eq!(
        ctxs.get(&idle_listen).unwrap().state,
        SocketState::Listening,
        "空闲 Listen 槽不动"
    );
}

#[test]
fn reap_predicate_preserves_active_async_open() {
    let mut pending = SocketCtx::new(443);
    pending.state = SocketState::HandshakePending;
    assert!(
        !should_reap_slot(&pending, TcpState::Established, true, true, 0),
        "活跃 async-open 槽不能因 uplink_tx=None 被误 reap"
    );

    let mut opening = SocketCtx::new(443);
    opening.state = SocketState::OpeningRemote;
    assert!(
        should_reap_slot(&opening, TcpState::Established, true, true, 0),
        "旧 inline OpeningRemote 且无 uplink_tx 仍应视为卡住可 reap"
    );

    let listening = SocketCtx::new(443);
    assert!(
        !should_reap_slot(&listening, TcpState::Listen, false, false, 0),
        "真实 Listen 的空闲槽永不 reap"
    );
    assert!(
        should_reap_slot(&listening, TcpState::Closed, false, false, 0),
        "logical Listening over a closed zero-payload incarnation must be reclaimed"
    );
}

#[test]
fn owned_half_close_uses_lifecycle_not_tx_capacity() {
    let snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: false,
        can_recv: true,
        may_send: true,
        may_recv: true,
        send_capacity: 0,
        send_queue: TCP_SOCKET_BUFFER_SIZE,
        recv_queue: 0,
    };
    assert_eq!(
        owned_half_close_disposition(snapshot, false, false),
        OwnedHalfCloseDisposition::Apply,
        "a full tx buffer must not turn a legal smoltcp close into flow failure"
    );
    assert_eq!(
        owned_half_close_disposition(snapshot, true, false),
        OwnedHalfCloseDisposition::WaitForSink
    );
}

#[test]
fn resumable_local_source_eof_covers_both_fin_orders_once_buffered_data_drains() {
    let snapshot = |tcp_state, may_recv| SocketCloseSnapshot {
        tcp_state,
        active: !matches!(tcp_state, TcpState::TimeWait | TcpState::Closed),
        can_send: false,
        can_recv: may_recv,
        may_send: false,
        may_recv,
        send_capacity: 0,
        send_queue: 0,
        recv_queue: usize::from(may_recv),
    };
    assert!(resumable_local_source_eof(snapshot(
        TcpState::CloseWait,
        false
    )));
    assert!(resumable_local_source_eof(snapshot(
        TcpState::LastAck,
        false
    )));
    assert!(resumable_local_source_eof(snapshot(
        TcpState::Closing,
        false
    )));
    assert!(resumable_local_source_eof(snapshot(
        TcpState::TimeWait,
        false
    )));
    assert!(!resumable_local_source_eof(snapshot(
        TcpState::FinWait2,
        true
    )));
    assert!(!resumable_local_source_eof(snapshot(
        TcpState::CloseWait,
        true
    )));
}

#[test]
fn missing_owned_terminal_wait_is_bounded() {
    let mut started = None;
    assert!(should_defer_owned_terminal_reap(
        true,
        false,
        &mut started,
        10
    ));
    assert_eq!(started, Some(10));
    assert!(should_defer_owned_terminal_reap(
        true,
        false,
        &mut started,
        14
    ));
    assert!(!should_defer_owned_terminal_reap(
        true,
        false,
        &mut started,
        15
    ));

    started = Some(1);
    assert!(!should_defer_owned_terminal_reap(
        false,
        false,
        &mut started,
        99
    ));
    assert_eq!(
        started, None,
        "one-sided/reset closure must not get clean grace"
    );
}

#[test]
fn exact_owned_terminal_reaps_inactive_immediately_and_bounds_active_fin_drain() {
    let active_last_ack = SocketCloseSnapshot {
        tcp_state: TcpState::LastAck,
        active: true,
        can_send: false,
        can_recv: false,
        may_send: false,
        may_recv: false,
        send_capacity: 0,
        send_queue: 0,
        recv_queue: 0,
    };
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.resumable_terminal = Some(TerminalAction::FlowFinished(FlowFinishReason::Graceful));

    assert!(!should_reap_slot_with_snapshot(
        &mut ctx,
        active_last_ack,
        10
    ));
    assert_eq!(ctx.owned_terminal_wait_since_secs, Some(10));
    assert!(!should_reap_slot_with_snapshot(
        &mut ctx,
        active_last_ack,
        14
    ));
    assert!(should_reap_slot_with_snapshot(
        &mut ctx,
        active_last_ack,
        15
    ));

    let mut inactive = active_last_ack;
    inactive.tcp_state = TcpState::Closed;
    inactive.active = false;
    let mut inactive_ctx = SocketCtx::new(443);
    inactive_ctx.state = SocketState::Relaying;
    inactive_ctx.resumable_terminal =
        Some(TerminalAction::FlowFinished(FlowFinishReason::Graceful));
    assert!(should_reap_slot_with_snapshot(
        &mut inactive_ctx,
        inactive,
        10
    ));
    assert_eq!(inactive_ctx.owned_terminal_wait_since_secs, None);
}

#[test]
fn retired_owned_action_waits_for_terminal_delivery_but_cannot_leak_a_slot() {
    for active in [true, false] {
        let snapshot = SocketCloseSnapshot {
            tcp_state: if active {
                TcpState::Established
            } else {
                TcpState::Closed
            },
            active,
            can_send: active,
            can_recv: false,
            may_send: active,
            may_recv: false,
            send_capacity: usize::from(active),
            send_queue: 0,
            recv_queue: 0,
        };
        let mut ctx = SocketCtx::new(443);
        ctx.state = SocketState::Relaying;
        ctx.owned_terminal_pending = true;

        assert!(!should_reap_slot_with_snapshot(&mut ctx, snapshot, 10));
        assert_eq!(ctx.owned_terminal_wait_since_secs, Some(10));
        assert!(!should_reap_slot_with_snapshot(&mut ctx, snapshot, 14));
        assert!(should_reap_slot_with_snapshot(&mut ctx, snapshot, 15));
        assert_eq!(ctx.owned_terminal_wait_since_secs, None);
    }
}

#[test]
fn reap_predicate_preserves_active_closewait_pending_downlink() {
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.downlink_pending = vec![1, 2, 3];

    assert!(
        !should_reap_slot(&ctx, TcpState::CloseWait, true, true, 0),
        "active CloseWait with pending downlink must keep flushing instead of aborting"
    );

    ctx.downlink_pending.clear();
    assert!(
        should_reap_slot(&ctx, TcpState::CloseWait, true, true, 0),
        "CloseWait is reapable again once pending downlink drains"
    );

    ctx.downlink_pending = vec![1, 2, 3];
    ctx.downlink_pending_last_progress_secs = Some(10);
    ctx.downlink_pending_last_pending_bytes = 3;
    assert!(
        should_reap_slot(&ctx, TcpState::Closed, false, false, 14),
        "inactive pending that cannot be sent is undeliverable and should reap immediately"
    );
    assert!(
        !should_reap_slot(&ctx, TcpState::Closed, false, true, 14),
        "inactive pending with send capacity and recent progress should get a short flush grace"
    );
    assert!(
        should_reap_slot(&ctx, TcpState::Closed, false, true, 15),
        "inactive send-capable pending remains bounded after the no-progress grace"
    );
}

#[test]
fn reap_predicate_preserves_active_closewait_live_relay() {
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.uplink_tx = Some(tx);

    assert!(
        !should_reap_slot(&ctx, TcpState::CloseWait, true, true, 0),
        "active CloseWait with a live relay may still receive reverse/downlink bytes"
    );

    ctx.uplink_tx = None;
    assert!(
        should_reap_slot(&ctx, TcpState::CloseWait, true, true, 0),
        "CloseWait is reapable once there is no live relay and no pending downlink"
    );
}

#[test]
fn reap_predicate_preserves_closewait_after_local_finish() {
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.local_fin_sent = true;
    ctx.uplink_tx = None;

    assert!(
        !should_reap_slot(&ctx, TcpState::CloseWait, true, true, 0),
        "post-Finish CloseWait must wait for the relay close event instead of dead-slot reap"
    );
}

#[test]
fn reap_predicate_preserves_deferred_close_egress_from_dead_reap() {
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Closing;
    ctx.pending_relay_close = Some(RelayClose {
        epoch: 7,
        direction: "local_to_remote",
        reason: "uplink_channel_closed",
    });
    ctx.pending_relay_close_since_secs = Some(10);
    ctx.pending_relay_close_last_egress_progress_secs = Some(10);
    ctx.pending_relay_close_last_egress_queue_bytes = 524_288;

    let snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::CloseWait,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 524_288,
        recv_queue: 0,
    };

    assert!(
        !should_reap_slot_with_snapshot(&mut ctx, snapshot, 14),
        "deferred egress close should not be reaped inside the bounded grace"
    );
    assert!(
        !should_reap_slot_with_snapshot(&mut ctx, snapshot, 15),
        "active send-capable close egress remains deliverable after the defer grace expires"
    );
}

#[test]
fn reap_predicate_preserves_deferred_close_egress_after_drop_guard() {
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Closing;
    ctx.pending_relay_close = Some(RelayClose {
        epoch: 7,
        direction: "local_to_remote",
        reason: "uplink_channel_closed",
    });
    ctx.pending_relay_close_since_secs = Some(10);
    ctx.pending_relay_close_last_egress_progress_secs = Some(10);
    ctx.pending_relay_close_last_egress_queue_bytes = 524_288;
    let snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::CloseWait,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 524_288,
        recv_queue: 0,
    };

    assert!(
        !should_reap_slot_with_snapshot(&mut ctx, snapshot, 24),
        "drop/pressure close-drain gets the extended bounded grace"
    );
    assert!(
        !should_reap_slot_with_snapshot(&mut ctx, snapshot, 25),
        "drop/pressure close-drain expiry does not authorize dead reap while the socket can still send"
    );

    ctx.pending_relay_close_last_egress_progress_secs = Some(39);
    ctx.pending_relay_close_last_egress_queue_bytes = 400_000;
    assert!(
        !should_reap_slot_with_snapshot(&mut ctx, snapshot, 40),
        "the hard close-egress defer bound belongs to finish/rearm, not data-dropping dead reap"
    );
}

#[test]
fn reap_predicate_preserves_active_send_capable_deferred_close_tail() {
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Closing;
    ctx.downlink_pending = vec![0; 376_912];
    ctx.pending_relay_close = Some(RelayClose {
        epoch: 7,
        direction: "local_to_remote",
        reason: "uplink_channel_closed",
    });
    ctx.pending_relay_close_since_secs = Some(10);
    ctx.pending_relay_close_last_progress_secs = Some(38);
    ctx.pending_relay_close_last_pending_bytes = 548_944;
    ctx.pending_relay_close_last_egress_progress_secs = Some(38);
    ctx.pending_relay_close_last_egress_queue_bytes = 892_928;
    let snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: true,
        send_capacity: 1_048_576,
        send_queue: 892_928,
        recv_queue: 0,
    };

    assert!(
        !should_reap_slot_with_snapshot(&mut ctx, snapshot, 68),
        "active send-capable deferred close still has deliverable tail bytes and must not be dead-reaped"
    );
}

#[test]
fn reap_predicate_graces_deferred_close_pending_downlink() {
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Closing;
    ctx.downlink_pending = vec![1, 2, 3];
    ctx.pending_relay_close = Some(RelayClose {
        epoch: 9,
        direction: "remote_to_local",
        reason: "remote_eof",
    });
    ctx.pending_relay_close_since_secs = Some(10);
    ctx.pending_relay_close_last_progress_secs = Some(13);
    ctx.pending_relay_close_last_pending_bytes = 3;

    assert!(
        should_reap_slot(&ctx, TcpState::Closed, false, false, 14),
        "deferred close pending without local send capacity should reap immediately"
    );
    assert!(
        !should_reap_slot(&ctx, TcpState::Closed, false, true, 14),
        "deferred close pending should get a short grace window to flush"
    );
    assert!(
        !should_reap_slot(&ctx, TcpState::Closed, false, true, 15),
        "deferred close pending should keep draining when recent progress exists"
    );
    assert!(
        should_reap_slot(&ctx, TcpState::Closed, false, true, 18),
        "deferred close pending should still be bounded after no-progress grace"
    );

    ctx.pending_relay_close = None;
    ctx.pending_relay_close_since_secs = None;
    ctx.pending_relay_close_last_progress_secs = None;
    ctx.pending_relay_close_last_pending_bytes = 0;
    ctx.downlink_pending_last_progress_secs = Some(13);
    ctx.downlink_pending_last_pending_bytes = 3;
    assert!(
        should_reap_slot(&ctx, TcpState::Closed, false, false, 14),
        "non-deferred inactive pending without local send capacity should reap immediately"
    );
    assert!(
        !should_reap_slot(&ctx, TcpState::Closed, false, true, 14),
        "non-deferred inactive pending should use the same recent-progress grace"
    );
    assert!(
        should_reap_slot(&ctx, TcpState::Closed, false, true, 18),
        "non-deferred inactive pending should still be bounded"
    );
}

#[test]
fn deferred_close_drain_progress_tracks_only_pending_decrease() {
    let mut ctx = SocketCtx::new(443);
    ctx.downlink_pending = vec![0; 100];
    ctx.pending_relay_close = Some(RelayClose {
        epoch: 1,
        direction: "remote_to_local",
        reason: "remote_eof",
    });
    ctx.pending_relay_close_since_secs = Some(10);
    ctx.pending_relay_close_last_progress_secs = Some(10);
    ctx.pending_relay_close_last_pending_bytes = 100;

    note_deferred_close_drain_progress(&mut ctx, 11, 0);
    assert_eq!(ctx.pending_relay_close_last_progress_secs, Some(10));
    assert_eq!(ctx.pending_relay_close_last_pending_bytes, 100);

    ctx.downlink_pending.truncate(60);
    note_deferred_close_drain_progress(&mut ctx, 12, 0);
    assert_eq!(ctx.pending_relay_close_last_progress_secs, Some(12));
    assert_eq!(ctx.pending_relay_close_last_pending_bytes, 60);

    ctx.downlink_pending.resize(80, 0);
    note_deferred_close_drain_progress(&mut ctx, 13, 0);
    assert_eq!(
        ctx.pending_relay_close_last_progress_secs,
        Some(12),
        "growth must not fake drain progress"
    );
    assert_eq!(ctx.pending_relay_close_last_pending_bytes, 80);

    ctx.pending_relay_close_last_pending_bytes = 0;
    ctx.pending_relay_close_last_progress_secs = Some(1);
    note_deferred_close_drain_progress(&mut ctx, 14, 0);
    assert_eq!(
        ctx.pending_relay_close_last_progress_secs,
        Some(14),
        "missing baseline should restart from the first observed pending size"
    );
    assert_eq!(ctx.pending_relay_close_last_pending_bytes, 80);
}

#[test]
fn downlink_pending_progress_tracks_observation_accept_and_growth() {
    let mut ctx = SocketCtx::new(443);
    ctx.downlink_pending = vec![0; 100];
    note_downlink_pending_progress(&mut ctx, 10, 0);
    assert_eq!(ctx.downlink_pending_last_progress_secs, Some(10));
    assert_eq!(ctx.downlink_pending_last_pending_bytes, 100);

    note_downlink_pending_progress(&mut ctx, 11, 0);
    assert_eq!(
        ctx.downlink_pending_last_progress_secs,
        Some(10),
        "unchanged pending must not fake new progress"
    );
    assert_eq!(ctx.downlink_pending_last_pending_bytes, 100);

    ctx.downlink_pending.resize(150, 0);
    note_downlink_pending_progress(&mut ctx, 12, 0);
    assert_eq!(
        ctx.downlink_pending_last_progress_secs,
        Some(10),
        "growth without accepted bytes must not refresh progress"
    );
    assert_eq!(ctx.downlink_pending_last_pending_bytes, 150);

    ctx.downlink_pending.resize(180, 0);
    note_downlink_pending_progress(&mut ctx, 13, 25);
    assert_eq!(
        ctx.downlink_pending_last_progress_secs,
        Some(13),
        "accepted bytes are progress even if final pending grew"
    );
    assert_eq!(ctx.downlink_pending_last_pending_bytes, 180);

    ctx.downlink_pending.clear();
    note_downlink_pending_progress(&mut ctx, 14, 0);
    assert!(ctx.downlink_pending_last_progress_secs.is_none());
    assert_eq!(ctx.downlink_pending_last_pending_bytes, 0);
}

#[test]
fn close_pending_accounting_classifies_pending_close_taxonomy() {
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.downlink_pending = vec![0; 224_765];

    let terminal = SocketCloseSnapshot {
        tcp_state: TcpState::Closed,
        active: false,
        can_send: false,
        can_recv: false,
        may_send: false,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 0,
        recv_queue: 0,
    };
    let accounting = close_pending_accounting(&ctx, terminal);
    assert_eq!(accounting.class, ClosePendingClass::TerminalClosedNoSend);
    assert_eq!(accounting.pending_bytes, 224_765);
    assert_eq!(accounting.terminal_pending_reap_bytes, 224_765);

    let active_no_send = SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: false,
        may_send: true,
        may_recv: true,
        send_queue: 1_048_576,
        ..terminal
    };
    let accounting = close_pending_accounting(&ctx, active_no_send);
    assert_eq!(accounting.class, ClosePendingClass::ActiveNoSend);
    assert_eq!(accounting.pending_bytes, 224_765);
    assert_eq!(accounting.terminal_pending_reap_bytes, 0);

    let active_send_capable = SocketCloseSnapshot {
        can_send: true,
        ..active_no_send
    };
    let accounting = close_pending_accounting(&ctx, active_send_capable);
    assert_eq!(accounting.class, ClosePendingClass::ActiveSendCapable);
    assert_eq!(accounting.terminal_pending_reap_bytes, 0);

    let inactive_send_capable = SocketCloseSnapshot {
        active: false,
        can_send: true,
        ..terminal
    };
    let accounting = close_pending_accounting(&ctx, inactive_send_capable);
    assert_eq!(accounting.class, ClosePendingClass::InactiveSendCapable);
    assert_eq!(accounting.terminal_pending_reap_bytes, 0);

    let inactive_no_send = SocketCloseSnapshot {
        tcp_state: TcpState::CloseWait,
        active: false,
        can_send: false,
        can_recv: false,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 1_048_576,
        recv_queue: 0,
    };
    let accounting = close_pending_accounting(&ctx, inactive_no_send);
    assert_eq!(accounting.class, ClosePendingClass::InactiveNoSend);
    assert_eq!(accounting.terminal_pending_reap_bytes, 0);

    ctx.downlink_pending.clear();
    let accounting = close_pending_accounting(&ctx, terminal);
    assert_eq!(accounting.class, ClosePendingClass::NoPending);
    assert_eq!(accounting.pending_bytes, 0);
    assert_eq!(accounting.terminal_pending_reap_bytes, 0);
}

#[test]
fn close_egress_accounting_reports_send_queue_without_app_pending() {
    let knife14bu_close = SocketCloseSnapshot {
        tcp_state: TcpState::CloseWait,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 524_288,
        recv_queue: 0,
    };

    let accounting = close_egress_accounting(knife14bu_close);

    assert_eq!(accounting.class, CloseEgressClass::ActiveSendCapable);
    assert_eq!(accounting.bytes, 524_288);
    assert!(accounting.drain_candidate);
}

#[test]
fn tcp_lifecycle_transition_diag_reports_previous_state_and_context() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.uplink_tx = Some(tx);
    ctx.downlink_pending = vec![0; 41_208];
    ctx.downlink_diag.note_remote_payload(221_884, 41_208);
    ctx.downlink_diag.note_send_slice_ok(180_676, 41_208);

    let established = SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: true,
        can_recv: true,
        may_send: true,
        may_recv: true,
        send_capacity: 1_048_576,
        send_queue: 65_536,
        recv_queue: 0,
    };
    assert!(
        observe_tcp_lifecycle(handle, &mut ctx, established, 10, "remote_payload").is_none(),
        "first observation seeds previous state without logging a transition"
    );

    let closed = SocketCloseSnapshot {
        tcp_state: TcpState::Closed,
        active: false,
        can_send: false,
        can_recv: false,
        may_send: false,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 0,
        recv_queue: 0,
    };
    let line = observe_tcp_lifecycle(handle, &mut ctx, closed, 15, "dead_slot_reap")
        .expect("state transition should produce a diagnostic line");

    assert!(line.contains("tcp-lifecycle-transition"), "{line}");
    assert!(line.contains("source=dead_slot_reap"), "{line}");
    assert!(line.contains("prev_source=remote_payload"), "{line}");
    assert!(
        line.contains("prev_state=Established state=Closed"),
        "{line}"
    );
    assert!(line.contains("prev_active=true active=false"), "{line}");
    assert!(line.contains("ctx_state=Relaying"), "{line}");
    assert!(line.contains("uplink_tx=true"), "{line}");
    assert!(line.contains("local_fin_sent=false"), "{line}");
    assert!(line.contains("pending=41208"), "{line}");
    assert!(line.contains("remote_to_global_rx_bytes=221884"), "{line}");
    assert!(line.contains("send_slice_accepted=180676"), "{line}");
    assert!(line.contains("terminal_candidate=true"), "{line}");
}

#[test]
fn remote_payload_allowed_after_local_finish_without_uplink_sender() {
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.local_fin_sent = true;
    ctx.uplink_tx = None;
    let active_closewait = SocketCloseSnapshot {
        tcp_state: TcpState::CloseWait,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 0,
        recv_queue: 0,
    };

    assert!(
        relay_allows_remote_payload(&ctx),
        "post-Finish read-only relay must still accept remote payloads"
    );
    assert_eq!(
        remote_payload_disposition(&ctx, active_closewait),
        RemotePayloadDisposition::Accept,
        "post-Finish read-only relay must still accept remote payloads"
    );
}

#[test]
fn remote_payload_rejected_after_terminal_no_send_socket() {
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.uplink_tx = Some(tx);
    let terminal = SocketCloseSnapshot {
        tcp_state: TcpState::Closed,
        active: false,
        can_send: false,
        can_recv: false,
        may_send: false,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 29_568,
        recv_queue: 0,
    };

    assert_eq!(
        remote_payload_disposition(&ctx, terminal),
        RemotePayloadDisposition::TerminalNoSend,
        "terminal no-send socket must not accumulate late remote payload in downlink_pending"
    );
}

#[test]
fn d3_egress_actor_parser_is_opt_in() {
    assert!(!parse_d3_egress_actor(None));
    assert!(!parse_d3_egress_actor(Some("0")));
    assert!(!parse_d3_egress_actor(Some("false")));
    assert!(parse_d3_egress_actor(Some("1")));
    assert!(parse_d3_egress_actor(Some(" true ")));
}

#[test]
fn h10d16_byte_owned_egress_parser_is_opt_in() {
    for value in [None, Some("0"), Some("false"), Some("1"), Some(" true ")] {
        let expected = matches!(value, Some("1" | " true "));
        let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            d16_byte_owned: value,
            ..Default::default()
        }));
        let config = TunRuntimeConfig::from_sources(None)
            .unwrap()
            .with_relay_policy(Arc::clone(&policy));
        assert_eq!(config.relay_policy().d16_byte_owned(), expected);
        let (dispatch, _) = OpenDispatch::channel(1, None);
        let dispatch = dispatch.with_relay_policy(Arc::clone(config.relay_policy()));
        assert!(Arc::ptr_eq(dispatch.relay_policy(), &policy));
        let window = config.tcp_socket_buffers.receive_window_limit;
        assert_eq!(window.is_some(), expected);
    }
}

#[test]
fn h10d16_gate_a_test_profile_matches_approved_safe1200_runtime() {
    let config = TunRuntimeConfig::h10d16_gate_a_for_test();

    assert_eq!(config.listener.pool_size, 2);
    assert_eq!(config.tun_mtu, 1_200);
    assert_eq!(
        config.downlink_backpressure,
        default_downlink_backpressure_for_tun_egress(1_200, DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE,)
    );
    assert!(config.relay_policy.d16_byte_owned());
    assert!(!config.d3_egress_actor);
    assert!(!config.d3_egress_actor_legacy_credit);
    assert!(!config.d3_egress_actor_self_wake);
    assert!(!config.d4_stalled_read_service);
    assert!(!config.d5_capacity_backpressure);
    assert!(config.relay_policy.native_engine() == NativeTcpRelayEngine::ChunkPump);
}

#[test]
fn h10d16_startup_line_reports_fixed_ownership_caps() {
    assert_eq!(
        format_h10d16_startup_line(true),
        "H10d16 byte-owned egress: enabled per_flow_cap=524288 global_cap=67108864 quantum=131072"
    );
    assert_eq!(
        format_h10d16_startup_line(false),
        "H10d16 byte-owned egress: disabled per_flow_cap=524288 global_cap=67108864 quantum=131072"
    );
}

#[test]
fn d3_egress_actor_adaptive_credit_parser_is_opt_in() {
    assert!(!parse_d3_egress_actor_adaptive_credit(None));
    assert!(!parse_d3_egress_actor_adaptive_credit(Some("0")));
    assert!(!parse_d3_egress_actor_adaptive_credit(Some("false")));
    assert!(parse_d3_egress_actor_adaptive_credit(Some("1")));
    assert!(parse_d3_egress_actor_adaptive_credit(Some(" true ")));
}

#[test]
fn d3_egress_actor_legacy_credit_parser_is_opt_in() {
    assert!(!parse_d3_egress_actor_legacy_credit(None));
    assert!(!parse_d3_egress_actor_legacy_credit(Some("0")));
    assert!(!parse_d3_egress_actor_legacy_credit(Some("false")));
    assert!(parse_d3_egress_actor_legacy_credit(Some("1")));
    assert!(parse_d3_egress_actor_legacy_credit(Some(" true ")));
}

#[test]
fn d3_egress_actor_self_wake_parser_is_opt_in() {
    assert!(!parse_d3_egress_actor_self_wake(None));
    assert!(!parse_d3_egress_actor_self_wake(Some("0")));
    assert!(!parse_d3_egress_actor_self_wake(Some("false")));
    assert!(parse_d3_egress_actor_self_wake(Some("1")));
    assert!(parse_d3_egress_actor_self_wake(Some(" true ")));
}

#[test]
fn d3_egress_actor_pending_marks_local_egress_work() {
    let mut ctx = SocketCtx::new(443);
    assert!(!remote_payload_left_local_egress_work(Some(&ctx), 0));

    ctx.downlink_pending.extend_from_slice(&[1, 2, 3]);
    assert!(
        remote_payload_left_local_egress_work(Some(&ctx), 0),
        "actor mode returns zero inline-accepted bytes, but pending bytes must still wake local egress"
    );
    ctx.downlink_pending.clear();
    assert!(remote_payload_left_local_egress_work(Some(&ctx), 16));
    assert!(!remote_payload_left_local_egress_work(None, 16));
}

#[tokio::test]
async fn d3_egress_actor_remote_payload_appends_without_inline_flush() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));

    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 3;
    ctx.uplink_tx = Some(tx);
    let mut ctxs = HashMap::from([(handle, ctx)]);
    let mut pool = FakeIpPool::new();
    let mut device = DnsInjectRecorder::default();
    let mut iface = Interface::new(
        SmolConfig::new(smoltcp::wire::HardwareAddress::Ip),
        &mut device,
        smoltcp::time::Instant::now(),
    );
    iface.update_ip_addrs(|addrs| {
        addrs
            .push(IpCidr::new(IpAddress::v4(10, 0, 0, 2), 24))
            .unwrap();
    });
    let mut pacer = DownlinkEgressPacer::new(DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES);
    let mut drop_debt = DownlinkEgressDropDebt::default();
    let cfg = DownlinkBackpressureConfig::default();

    let accepted = handle_remote_payload(
        handle,
        3,
        vec![9; 4096],
        &mut sockets,
        &mut ctxs,
        &mut iface,
        &mut device,
        &mut pool,
        77,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        &mut pacer,
        &mut drop_debt,
        cfg,
        1200,
        false,
        None,
        true,
        false,
        false,
        false,
    )
    .await
    .unwrap();

    let ctx = ctxs.get(&handle).unwrap();
    assert_eq!(accepted, 0);
    assert_eq!(ctx.state, SocketState::Relaying);
    assert_eq!(ctx.downlink_pending.len(), 4096);
    assert_eq!(ctx.downlink_diag.flush_attempts, 0);
    assert_eq!(ctx.downlink_diag.send_slice_calls, 0);
    assert_eq!(ctx.downlink_diag.tun_flush_tx_calls, 0);
    assert_eq!(ctx.downlink_pending_last_progress_secs, Some(77));
    assert_eq!(ctx.downlink_pending_last_pending_bytes, 4096);
    assert!(
        ctx.downlink_d3_actor_flow,
        "D3 actor flows must use actor-owned local egress pacing"
    );
    assert!(
        !ctx.downlink_d3_actor_adaptive_credit,
        "adaptive credit remains diagnostic opt-in"
    );
    assert!(
        !ctx.downlink_d3_actor_legacy_credit,
        "legacy credit remains diagnostic opt-in"
    );
}

#[tokio::test]
async fn terminal_remote_payload_rearms_slot_and_stales_followup_payloads() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
    sockets.get_mut::<TcpSocket>(handle).abort();

    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 9;
    ctx.uplink_tx = Some(tx);
    let mut ctxs = HashMap::from([(handle, ctx)]);
    let mut pool = FakeIpPool::new();
    let mut device = DnsInjectRecorder::default();
    let mut iface = Interface::new(
        SmolConfig::new(smoltcp::wire::HardwareAddress::Ip),
        &mut device,
        smoltcp::time::Instant::now(),
    );
    iface.update_ip_addrs(|addrs| {
        addrs
            .push(IpCidr::new(IpAddress::v4(10, 0, 0, 2), 24))
            .unwrap();
    });
    let mut pacer = DownlinkEgressPacer::new(DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES);
    let mut drop_debt = DownlinkEgressDropDebt::default();
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 524_288,
        low_bytes: 131_072,
    };

    let accepted = handle_remote_payload(
        handle,
        9,
        vec![7; 4096],
        &mut sockets,
        &mut ctxs,
        &mut iface,
        &mut device,
        &mut pool,
        42,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        &mut pacer,
        &mut drop_debt,
        cfg,
        1200,
        false,
        None,
        false,
        false,
        false,
        false,
    )
    .await
    .unwrap();

    let ctx = ctxs.get(&handle).unwrap();
    assert_eq!(accepted, 0);
    assert_eq!(ctx.state, SocketState::Listening);
    assert_eq!(
        ctx.conn_epoch, 10,
        "terminal late payload must retire the old relay epoch"
    );
    assert!(ctx.uplink_tx.is_none());
    assert!(ctx.downlink_pending.is_empty());

    let accepted = handle_remote_payload(
        handle,
        9,
        vec![8; 4096],
        &mut sockets,
        &mut ctxs,
        &mut iface,
        &mut device,
        &mut pool,
        43,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        &mut pacer,
        &mut drop_debt,
        cfg,
        1200,
        false,
        None,
        false,
        false,
        false,
        false,
    )
    .await
    .unwrap();

    let ctx = ctxs.get(&handle).unwrap();
    assert_eq!(accepted, 0);
    assert_eq!(ctx.state, SocketState::Listening);
    assert_eq!(
        ctx.conn_epoch, 10,
        "old relay payloads after terminal rearm must stay stale"
    );
    assert!(ctx.downlink_pending.is_empty());
}

/// 刀14d：本地已关闭的 in-flight open 必须回收、bump epoch，并让迟到 `HandshakeDone`
/// 被 epoch guard 丢弃。
#[tokio::test]
async fn reap_dead_async_open_drops_stale_result() {
    let mut sockets = SocketSet::new(vec![]);
    let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
    let mut reg = ListenerRegistry::new(1);
    reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
    let handles: Vec<SocketHandle> = reg.handles_for_port(443).to_vec();
    let closed_pending = handles[0];

    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("x.com", 0).expect("fake-IP capacity");
    pool.acquire(ip, 0);

    {
        let ctx = ctxs.get_mut(&closed_pending).unwrap();
        ctx.state = SocketState::HandshakePending;
        ctx.conn_epoch = 9;
        ctx.fake_ip = Some(ip);
    }
    sockets.get_mut::<TcpSocket>(closed_pending).abort();

    let reaped = reap_dead_slots(&reg, &mut sockets, &mut ctxs, &mut pool, 1);
    assert_eq!(reaped, 1, "应回收本地已关闭的 async-open 槽");

    let closed_ctx = ctxs.get(&closed_pending).unwrap();
    assert_eq!(closed_ctx.state, SocketState::Listening);
    assert_eq!(
        closed_ctx.conn_epoch, 10,
        "reap 应 bump epoch 让迟到 open 结果失效"
    );
    assert!(
        closed_ctx.fake_ip.is_none(),
        "reap 应释放 in-flight flow fake-IP"
    );
    assert_eq!(pool.sweep(1000, 300), 1, "release 后映射可回收");

    let (global_tx, _grx) = mpsc::channel(8);
    let (owned_action_tx, _owned_action_rx) = mpsc::channel(8);
    let stale = HandshakeDone {
        handle: closed_pending,
        epoch: 9,
        result: Ok(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(Box::new(
            tokio::io::duplex(64).0,
        )))),
    };
    assert!(!handle_handshake_done(
        stale,
        &mut sockets,
        &mut ctxs,
        &global_tx,
        &owned_action_tx,
        &mut pool,
        2,
        &Metrics::new(),
        DownlinkBackpressureConfig::default(),
        false,
    ));
    let closed_ctx = ctxs.get(&closed_pending).unwrap();
    assert_eq!(
        closed_ctx.state,
        SocketState::Listening,
        "迟到 open 结果不应装到 rearm 后的新一代 socket"
    );
    assert!(closed_ctx.uplink_tx.is_none());
}

#[test]
fn relay_closed_epoch_guard_drops_stale_close() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(12345);
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 22;
    ctx.uplink_tx = Some(tx);
    let mut socket_ctxs = HashMap::new();
    socket_ctxs.insert(handle, ctx);
    let mut pool = FakeIpPool::new();

    handle_relay_closed(
        handle,
        RelayClose {
            epoch: 21,
            direction: "timer",
            reason: "stalled_write_timeout",
        },
        &mut sockets,
        &mut socket_ctxs,
        &mut pool,
        1,
        DownlinkBackpressureConfig::default(),
    );

    let ctx = socket_ctxs.get(&handle).unwrap();
    assert_eq!(
        ctx.state,
        SocketState::Relaying,
        "stale close must not rearm a new epoch"
    );
    assert_eq!(ctx.conn_epoch, 22);
    assert!(ctx.uplink_tx.is_some());
}

#[test]
fn relay_closed_with_pending_downlink_defers_rearm() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(12345);
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 5;
    ctx.uplink_tx = Some(tx);
    ctx.downlink_pending = vec![1, 2, 3];
    let mut socket_ctxs = HashMap::new();
    socket_ctxs.insert(handle, ctx);
    let mut pool = FakeIpPool::new();

    let keep_dirty = handle_relay_closed(
        handle,
        RelayClose {
            epoch: 5,
            direction: "remote_to_local",
            reason: "remote_eof",
        },
        &mut sockets,
        &mut socket_ctxs,
        &mut pool,
        1,
        DownlinkBackpressureConfig::default(),
    );

    let ctx = socket_ctxs.get_mut(&handle).unwrap();
    assert!(
        keep_dirty,
        "pending downlink must stay dirty until it drains"
    );
    assert_eq!(ctx.state, SocketState::Closing);
    assert!(ctx.uplink_tx.is_none());
    assert_eq!(ctx.downlink_pending, vec![1, 2, 3]);
    assert!(ctx.pending_relay_close.is_some());
    assert_eq!(ctx.pending_relay_close_since_secs, Some(1));
    assert_eq!(ctx.pending_relay_close_last_progress_secs, Some(1));
    assert_eq!(ctx.pending_relay_close_last_pending_bytes, 3);
    assert_eq!(ctx.downlink_pending_last_progress_secs, Some(1));
    assert_eq!(ctx.downlink_pending_last_pending_bytes, 3);
    assert_eq!(
        ctx.conn_epoch, 5,
        "deferred close must not bump epoch before drain"
    );

    ctx.downlink_pending.clear();
    let socket = sockets.get_mut::<TcpSocket>(handle);
    assert!(finish_deferred_relay_close_if_drained(
        handle,
        socket,
        ctx,
        &mut pool,
        2,
        DownlinkBackpressureConfig::default(),
        false
    ));
    assert_eq!(ctx.state, SocketState::Listening);
    assert!(ctx.pending_relay_close.is_none());
    assert!(ctx.pending_relay_close_since_secs.is_none());
    assert!(ctx.pending_relay_close_last_progress_secs.is_none());
    assert_eq!(ctx.pending_relay_close_last_pending_bytes, 0);
    assert!(ctx.downlink_pending_last_progress_secs.is_none());
    assert_eq!(ctx.downlink_pending_last_pending_bytes, 0);
    assert_eq!(ctx.conn_epoch, 6);
}

#[test]
fn deferred_close_egress_starts_for_send_capable_closewait_queue() {
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(12345);
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 5;
    ctx.uplink_tx = Some(tx);
    let snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::CloseWait,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 524_288,
        recv_queue: 0,
    };
    let close = RelayClose {
        epoch: 5,
        direction: "local_to_remote",
        reason: "uplink_channel_closed",
    };
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 524_288,
        low_bytes: 131_072,
    };

    assert!(start_deferred_close_for_egress_if_needed(
        &mut ctx, close, snapshot, cfg, 10
    ));
    assert_eq!(ctx.state, SocketState::Closing);
    assert!(ctx.uplink_tx.is_none());
    assert_eq!(
        ctx.pending_relay_close.map(|c| c.reason),
        Some("uplink_channel_closed")
    );
    assert_eq!(ctx.pending_relay_close_since_secs, Some(10));
    assert_eq!(ctx.pending_relay_close_last_egress_progress_secs, Some(10));
    assert_eq!(ctx.pending_relay_close_last_egress_queue_bytes, 524_288);
}

#[test]
fn deferred_close_egress_starts_for_send_capable_tail_above_low_watermark() {
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(12345);
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 5;
    ctx.uplink_tx = Some(tx);
    let snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::CloseWait,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 217_725,
        recv_queue: 0,
    };
    let close = RelayClose {
        epoch: 5,
        direction: "local_to_remote",
        reason: "uplink_channel_closed",
    };
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 524_288,
        low_bytes: 131_072,
    };

    assert!(
        start_deferred_close_for_egress_if_needed(&mut ctx, close, snapshot, cfg, 10),
        "send-capable close tails above the drain-complete low watermark must enter close-drain even when below high pressure"
    );
    assert_eq!(ctx.state, SocketState::Closing);
    assert!(ctx.uplink_tx.is_none());
    assert_eq!(
        ctx.pending_relay_close.map(|c| c.reason),
        Some("uplink_channel_closed")
    );
    assert_eq!(ctx.pending_relay_close_since_secs, Some(10));
    assert_eq!(ctx.pending_relay_close_last_egress_progress_secs, Some(10));
    assert_eq!(ctx.pending_relay_close_last_egress_queue_bytes, 217_725);
}

#[test]
fn pending_downlink_close_deferral_is_direction_agnostic() {
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(12345);
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 5;
    ctx.uplink_tx = Some(tx);
    ctx.downlink_pending = vec![1, 2, 3, 4];
    let close = RelayClose {
        epoch: 5,
        direction: "local_to_remote",
        reason: "uplink_channel_closed",
    };

    assert!(start_deferred_close_for_pending_downlink(
        &mut ctx, close, 12
    ));
    assert_eq!(ctx.state, SocketState::Closing);
    assert!(ctx.uplink_tx.is_none());
    assert_eq!(
        ctx.pending_relay_close.map(|c| c.reason),
        Some("uplink_channel_closed")
    );
    assert_eq!(ctx.pending_relay_close_since_secs, Some(12));
    assert_eq!(ctx.pending_relay_close_last_progress_secs, Some(12));
    assert_eq!(ctx.pending_relay_close_last_pending_bytes, 4);
    assert_eq!(ctx.downlink_pending_last_progress_secs, Some(12));
    assert_eq!(ctx.downlink_pending_last_pending_bytes, 4);
    assert_eq!(
        ctx.conn_epoch, 5,
        "pending deferral must not rearm or bump epoch"
    );
}

#[test]
fn pending_downlink_close_deferral_ignores_empty_pending() {
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(12345);
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 5;
    ctx.uplink_tx = Some(tx);
    let close = RelayClose {
        epoch: 5,
        direction: "local_to_remote",
        reason: "uplink_channel_closed",
    };

    assert!(!start_deferred_close_for_pending_downlink(
        &mut ctx, close, 12
    ));
    assert_eq!(ctx.state, SocketState::Relaying);
    assert!(ctx.uplink_tx.is_some());
    assert!(ctx.pending_relay_close.is_none());
    assert!(ctx.pending_relay_close_since_secs.is_none());
    assert!(ctx.downlink_pending_last_progress_secs.is_none());
}

#[test]
fn deferred_close_egress_waits_until_low_or_grace() {
    let mut ctx = SocketCtx::new(12345);
    ctx.state = SocketState::Closing;
    ctx.pending_relay_close = Some(RelayClose {
        epoch: 5,
        direction: "local_to_remote",
        reason: "uplink_channel_closed",
    });
    ctx.pending_relay_close_since_secs = Some(10);
    ctx.pending_relay_close_last_egress_progress_secs = Some(10);
    ctx.pending_relay_close_last_egress_queue_bytes = 524_288;
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 524_288,
        low_bytes: 131_072,
    };
    let mut snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::CloseWait,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 524_288,
        recv_queue: 0,
    };

    assert!(
        should_keep_deferred_close_for_egress(&mut ctx, snapshot, cfg, 14, false),
        "high egress queue within grace should keep the close deferred"
    );

    snapshot.send_queue = 300_000;
    assert!(
        should_keep_deferred_close_for_egress(&mut ctx, snapshot, cfg, 14, false),
        "queue drain progress should refresh the bounded grace"
    );
    assert_eq!(ctx.pending_relay_close_last_egress_progress_secs, Some(14));
    assert_eq!(ctx.pending_relay_close_last_egress_queue_bytes, 300_000);

    assert!(
        should_keep_deferred_close_for_egress(&mut ctx, snapshot, cfg, 18, false),
        "still above low but within refreshed grace should keep waiting"
    );
    assert!(
        !should_keep_deferred_close_for_egress(&mut ctx, snapshot, cfg, 19, false),
        "above low without progress must remain bounded"
    );

    snapshot.send_queue = 131_072;
    assert!(
        !should_keep_deferred_close_for_egress(&mut ctx, snapshot, cfg, 20, false),
        "low watermark means queued egress drained enough to finish close"
    );
}

#[test]
fn deferred_close_sends_local_eof_after_pending_drains() {
    let mut ctx = SocketCtx::new(12345);
    ctx.state = SocketState::Closing;
    ctx.pending_relay_close = Some(RelayClose {
        epoch: 5,
        direction: "remote_to_local",
        reason: "remote_eof",
    });
    ctx.downlink_pending = vec![1, 2, 3];
    let snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: true,
        send_capacity: 1_048_576,
        send_queue: 720_896,
        recv_queue: 0,
    };

    assert!(
        !should_send_local_eof_for_deferred_close(&ctx, snapshot),
        "remote EOF must not send local FIN before terminal pending bytes enter smoltcp"
    );

    ctx.downlink_pending.clear();
    assert!(
        should_send_local_eof_for_deferred_close(&ctx, snapshot),
        "once terminal pending is accepted, local FIN should be queued behind the egress tail"
    );

    ctx.local_eof_sent = true;
    assert!(
        !should_send_local_eof_for_deferred_close(&ctx, snapshot),
        "local FIN must be idempotent while the TCP state machine drains"
    );
}

// ---- 刀9 M3：握手并发化（epoch 防串话 / buffer 上限 / flush 保序 / 失败 rearm）----

/// buffer_uplink 256KB 硬上限：填满后溢出包被丢弃、buffer 不变。
#[test]
fn uplink_buffer_enforces_cap() {
    let mut ctx = SocketCtx::new(80);
    assert!(ctx.buffer_uplink(&[0u8; 1000]));
    assert_eq!(ctx.uplink_buffer.len(), 1000);
    let fill = vec![0u8; MAX_UPLINK_BUFFER - 1000];
    assert!(ctx.buffer_uplink(&fill), "恰好填到上限应接受");
    assert_eq!(ctx.uplink_buffer.len(), MAX_UPLINK_BUFFER);
    assert!(!ctx.buffer_uplink(&[0u8; 1]), "超上限 1B 应丢弃");
    assert_eq!(
        ctx.uplink_buffer.len(),
        MAX_UPLINK_BUFFER,
        "丢弃不改变 buffer"
    );
}

#[test]
fn established_uplink_batch_drains_multiple_payloads_in_one_pass() {
    use std::collections::VecDeque;

    let (tx, mut rx) = mpsc::channel(8);
    let mut ctx = SocketCtx::new(80);
    ctx.uplink_tx = Some(tx);
    let mut payloads: VecDeque<Vec<u8>> =
        VecDeque::from([b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]);

    assert!(matches!(
        pump_established_uplink(&mut ctx, TcpState::Established, 0, || payloads.pop_front()),
        EstablishedUplink::Handled
    ));
    assert!(
        payloads.is_empty(),
        "one dirty pass should drain the readable batch"
    );
    assert_eq!(ctx.state, SocketState::Relaying);

    for expected in [b"one".as_slice(), b"two".as_slice(), b"three".as_slice()] {
        match rx.try_recv().expect("payload should be forwarded") {
            RelayCommand::Data(got) => assert_eq!(got, expected),
            other => panic!("expected data command, got {other:?}"),
        }
    }
    assert!(rx.try_recv().is_err());
}

#[test]
fn established_uplink_batch_stops_at_fairness_cap() {
    use std::collections::VecDeque;

    let (tx, mut rx) = mpsc::channel(MAX_ESTABLISHED_UPLINK_BATCH + 2);
    let mut ctx = SocketCtx::new(80);
    ctx.uplink_tx = Some(tx);
    let mut payloads: VecDeque<Vec<u8>> = (0..=MAX_ESTABLISHED_UPLINK_BATCH)
        .map(|i| vec![i as u8])
        .collect();

    assert!(matches!(
        pump_established_uplink(&mut ctx, TcpState::Established, 0, || payloads.pop_front()),
        EstablishedUplink::Handled
    ));
    assert_eq!(
        payloads.len(),
        1,
        "batch cap should leave remaining readable data for the next dirty pass"
    );
    for _ in 0..MAX_ESTABLISHED_UPLINK_BATCH {
        assert!(matches!(
            rx.try_recv().expect("batch payload should be sent"),
            RelayCommand::Data(_)
        ));
    }
    assert!(rx.try_recv().is_err());
}

#[test]
fn established_uplink_full_channel_does_not_consume_payload() {
    use std::collections::VecDeque;

    let (tx, _rx) = mpsc::channel(1);
    tx.try_send(RelayCommand::Data(b"occupied".to_vec()))
        .unwrap();
    let mut ctx = SocketCtx::new(80);
    ctx.uplink_tx = Some(tx);
    let mut payloads: VecDeque<Vec<u8>> = VecDeque::from([b"must-stay".to_vec()]);

    assert!(matches!(
        pump_established_uplink(&mut ctx, TcpState::Established, 0, || payloads.pop_front()),
        EstablishedUplink::Handled
    ));
    assert_eq!(
        payloads.len(),
        1,
        "mpsc full must preserve smoltcp bytes for TCP-window backpressure"
    );
}

#[test]
fn resumable_uplink_treats_flow_and_session_byte_and_segment_exhaustion_as_backpressure() {
    for error in [
        FlowPortError::UplinkMessageLaneFull,
        FlowPortError::UplinkByteBudgetExhausted {
            requested: 97,
            available: 0,
        },
        FlowPortError::UplinkGlobalByteBudgetExhausted {
            requested: 97,
            available: 0,
        },
        FlowPortError::UplinkSegmentBudgetExhausted { available: 0 },
        FlowPortError::UplinkGlobalSegmentBudgetExhausted { available: 0 },
    ] {
        assert!(
            resumable_uplink_admission_is_backpressure(&error),
            "{error} must preserve unread smoltcp bytes instead of closing the flow"
        );
    }

    assert!(!resumable_uplink_admission_is_backpressure(
        &FlowPortError::Closed
    ));
}

#[test]
fn established_uplink_defers_finish_after_payloads_drain() {
    let (tx, mut rx) = mpsc::channel(4);
    let mut ctx = SocketCtx::new(80);
    ctx.uplink_tx = Some(tx);

    assert!(matches!(
        pump_established_uplink(&mut ctx, TcpState::CloseWait, 10, || None),
        EstablishedUplink::Handled
    ));
    assert!(!ctx.local_fin_sent);
    assert_eq!(ctx.local_fin_pending_since_secs, Some(10));
    assert!(
        rx.try_recv().is_err(),
        "initial CloseWait should defer Finish"
    );

    assert!(matches!(
        pump_established_uplink(&mut ctx, TcpState::CloseWait, 14, || None),
        EstablishedUplink::Handled
    ));
    assert!(!ctx.local_fin_sent);
    assert!(rx.try_recv().is_err(), "defer window has not expired");

    assert!(matches!(
        pump_established_uplink(&mut ctx, TcpState::CloseWait, 15, || None),
        EstablishedUplink::Handled
    ));
    assert!(ctx.local_fin_sent);
    assert!(matches!(
        rx.try_recv().expect("finish should be sent"),
        RelayCommand::Finish
    ));
    assert!(rx.try_recv().is_err());
}

#[test]
fn established_uplink_remote_progress_extends_finish_defer() {
    let (tx, mut rx) = mpsc::channel(4);
    let mut ctx = SocketCtx::new(80);
    ctx.uplink_tx = Some(tx);

    assert!(matches!(
        pump_established_uplink(&mut ctx, TcpState::CloseWait, 10, || None),
        EstablishedUplink::Handled
    ));
    note_local_finish_remote_progress(&mut ctx, 13);

    assert!(matches!(
        pump_established_uplink(&mut ctx, TcpState::CloseWait, 15, || None),
        EstablishedUplink::Handled
    ));
    assert!(!ctx.local_fin_sent);
    assert!(
        rx.try_recv().is_err(),
        "remote progress should extend Finish defer"
    );

    assert!(matches!(
        pump_established_uplink(&mut ctx, TcpState::CloseWait, 18, || None),
        EstablishedUplink::Handled
    ));
    assert!(ctx.local_fin_sent);
    assert!(matches!(
        rx.try_recv()
            .expect("finish should be sent after remote quiet"),
        RelayCommand::Finish
    ));
}

fn mk_pending_ctx(epoch: u64) -> SocketCtx {
    let mut ctx = SocketCtx::new(12345);
    ctx.state = SocketState::HandshakePending;
    ctx.conn_epoch = epoch;
    ctx.pending_open_mode = Some(TcpOwnershipMode::LegacyRelay);
    ctx
}

fn resumable_adapter_test_pair() -> (
    crate::owned_upstream::ResumableTcpFlow,
    crate::owned_upstream::ResumableTcpDriver,
    LocalFlow,
) {
    let flow = LocalFlow::for_adapter_test(
        crate::resumable::SessionId::new([0x51; 16]).unwrap(),
        crate::resumable::SessionFlowId::new(1).unwrap(),
        crate::resumable::Direction::ClientToTarget,
        1,
    );
    let factory = crate::owned_upstream::ResumableTcpPortFactory::new_unbound_for_test(
        crate::owned_upstream::FlowPortConfig::new(64, 2, 2, 2).unwrap(),
        64,
        2,
        2,
    )
    .unwrap();
    let (endpoint, driver) = factory.open_flow(flow).unwrap();
    (endpoint, driver, flow)
}

#[tokio::test]
async fn closed_handshake_receiver_abandons_uninstalled_resumable_flow() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
    let (endpoint, mut driver, expected_flow) = resumable_adapter_test_pair();
    let (tx, rx) = mpsc::channel(1);
    drop(rx);

    publish_handshake_done(
        &tx,
        HandshakeDone {
            handle,
            epoch: 1,
            result: Ok(OpenedOwnedTcp::Resumable(endpoint)),
        },
    )
    .await;

    let Some(crate::owned_upstream::DriverInput::Control(
        crate::resumable::SessionEvent::LocalReset { flow, reason },
    )) = driver.try_recv_next().unwrap()
    else {
        panic!("closed completion receiver must preserve exact terminal ownership")
    };
    assert_eq!(flow, expected_flow);
    assert_eq!(reason, ResetReason::LocalAbandon);
}

#[test]
fn stale_resumable_handshake_result_abandons_old_epoch_without_installing() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
    let mut ctx = mk_pending_ctx(5);
    ctx.pending_open_mode = Some(TcpOwnershipMode::Resumable);
    let mut socket_ctxs = HashMap::from([(handle, ctx)]);
    let (global_tx, _global_rx) = mpsc::channel(1);
    let (owned_action_tx, _owned_action_rx) = mpsc::channel(1);
    let mut pool = FakeIpPool::new();
    let (endpoint, mut driver, expected_flow) = resumable_adapter_test_pair();

    assert!(!handle_handshake_done(
        HandshakeDone {
            handle,
            epoch: 4,
            result: Ok(OpenedOwnedTcp::Resumable(endpoint)),
        },
        &mut sockets,
        &mut socket_ctxs,
        &global_tx,
        &owned_action_tx,
        &mut pool,
        0,
        &Metrics::new(),
        DownlinkBackpressureConfig::default(),
        false,
    ));
    let ctx = socket_ctxs.get(&handle).unwrap();
    assert_eq!(ctx.conn_epoch, 5);
    assert!(ctx.resumable_uplink.is_none());

    let Some(crate::owned_upstream::DriverInput::Control(
        crate::resumable::SessionEvent::LocalReset { flow, reason },
    )) = driver.try_recv_next().unwrap()
    else {
        panic!("stale resumable open must leave exact LocalAbandon for its supervisor")
    };
    assert_eq!(flow, expected_flow);
    assert_eq!(reason, ResetReason::LocalAbandon);
}

/// epoch 防串话：迟到结果（epoch 不匹配）丢弃、不装到 socket；匹配 + Ok → 安装 relay。
#[tokio::test]
async fn handshake_done_epoch_guard_drops_stale_then_installs_match() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
    let mut socket_ctxs = HashMap::new();
    socket_ctxs.insert(handle, mk_pending_ctx(5));
    let (global_tx, _grx) = mpsc::channel(8);
    let (owned_action_tx, _owned_action_rx) = mpsc::channel(8);
    let mut pool = FakeIpPool::new();

    // 迟到 epoch 3 ≠ 5 → 丢弃，状态/uplink_tx 不变。
    let stale = HandshakeDone {
        handle,
        epoch: 3,
        result: Ok(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(Box::new(
            tokio::io::duplex(64).0,
        )))),
    };
    assert!(!handle_handshake_done(
        stale,
        &mut sockets,
        &mut socket_ctxs,
        &global_tx,
        &owned_action_tx,
        &mut pool,
        0,
        &Metrics::new(),
        DownlinkBackpressureConfig::default(),
        false,
    ));
    let ctx = socket_ctxs.get(&handle).unwrap();
    assert_eq!(
        ctx.state,
        SocketState::HandshakePending,
        "迟到 epoch → 不装、状态不变"
    );
    assert!(ctx.uplink_tx.is_none(), "迟到 epoch → 不安装 uplink_tx");

    // 匹配 epoch 5 + Ok → 安装 relay。
    let ok = HandshakeDone {
        handle,
        epoch: 5,
        result: Ok(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(Box::new(
            tokio::io::duplex(64).0,
        )))),
    };
    assert!(!handle_handshake_done(
        ok,
        &mut sockets,
        &mut socket_ctxs,
        &global_tx,
        &owned_action_tx,
        &mut pool,
        0,
        &Metrics::new(),
        DownlinkBackpressureConfig::default(),
        false,
    ));
    let ctx = socket_ctxs.get(&handle).unwrap();
    assert_eq!(
        ctx.state,
        SocketState::Relaying,
        "匹配 epoch → 安装 relay 进 Relaying"
    );
    assert!(ctx.uplink_tx.is_some(), "匹配 epoch → 安装 uplink_tx");
}

/// Async open 失败（匹配 epoch + Err）→ rearm：回 Listening、释放 spawn 时 acquire 的 fake-IP、清 buffer。
#[tokio::test]
async fn handshake_done_failure_rearms_and_releases_fakeip() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("x.com", 0).expect("fake-IP capacity");
    pool.acquire(ip, 0); // 模拟 spawn 路径已 acquire
    let mut ctx = mk_pending_ctx(2);
    ctx.fake_ip = Some(ip);
    ctx.buffer_uplink(b"buffered");
    let mut socket_ctxs = HashMap::new();
    socket_ctxs.insert(handle, ctx);
    let (global_tx, _grx) = mpsc::channel(8);
    let (owned_action_tx, _owned_action_rx) = mpsc::channel(8);

    let err = HandshakeDone {
        handle,
        epoch: 2,
        result: Err(ClientError::Reality("open 失败".into())),
    };
    handle_handshake_done(
        err,
        &mut sockets,
        &mut socket_ctxs,
        &global_tx,
        &owned_action_tx,
        &mut pool,
        1,
        &Metrics::new(),
        DownlinkBackpressureConfig::default(),
        false,
    );
    let ctx = socket_ctxs.get(&handle).unwrap();
    assert_eq!(
        ctx.state,
        SocketState::Listening,
        "失败 → rearm 回 Listening"
    );
    assert!(
        ctx.fake_ip.is_none(),
        "失败 rearm 释放 fake-IP（平衡 spawn 的 acquire）"
    );
    assert!(ctx.uplink_buffer.is_empty(), "rearm 清 buffer");
    assert_eq!(
        pool.sweep(1000, 300),
        1,
        "refcount 归零 → 可回收（acquire/release 平衡）"
    );
}

/// Async open 成功 → 按序 flush open 期间缓存的上行字节到 relay 流。
#[tokio::test]
async fn handshake_done_flushes_buffered_uplink_in_order() {
    use tokio::io::AsyncReadExt;
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 12345 }));
    let mut ctx = mk_pending_ctx(1);
    ctx.buffer_uplink(b"HELLO-BUFFERED");
    let mut socket_ctxs = HashMap::new();
    socket_ctxs.insert(handle, ctx);
    let (global_tx, _grx) = mpsc::channel(8);
    let (owned_action_tx, _owned_action_rx) = mpsc::channel(8);
    let mut pool = FakeIpPool::new();

    // near = relay 写入端（上游流）；far = 测试读端（模拟出口收到的上行）。
    let (near, mut far) = tokio::io::duplex(1024);
    let ok = HandshakeDone {
        handle,
        epoch: 1,
        result: Ok(OpenedOwnedTcp::Legacy(OpenedTcpRelay::Generic(Box::new(
            near,
        )))),
    };
    handle_handshake_done(
        ok,
        &mut sockets,
        &mut socket_ctxs,
        &global_tx,
        &owned_action_tx,
        &mut pool,
        0,
        &Metrics::new(),
        DownlinkBackpressureConfig::default(),
        false,
    );

    let mut got = vec![0u8; b"HELLO-BUFFERED".len()];
    far.read_exact(&mut got).await.unwrap();
    assert_eq!(
        &got, b"HELLO-BUFFERED",
        "open 成功后按序 flush 缓存的上行字节"
    );
}

#[test]
fn tun_runtime_config_defaults_match_stage9_behavior() {
    let config = TunRuntimeConfig::from_sources(None).expect("config should load");
    // Stage 9 drops local_port; pool_size default lowered to 2 (per-port now).
    assert_eq!(config.listener.pool_size, 2);
    assert_eq!(config.tun_mtu, DEFAULT_TUN_MTU);
    // 刀11：from_sources（harness/测试路径）恒用默认快照周期。
    assert_eq!(config.metrics_secs, METRICS_SNAPSHOT_SECS);
    assert_eq!(
        config.tcp_socket_buffer_bytes(),
        (TCP_SOCKET_BUFFER_SIZE, TCP_SOCKET_BUFFER_SIZE)
    );
    assert_eq!(
        config.downlink_flush_max_bytes,
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES
    );
    assert_eq!(
        config.downlink_egress_immediate_bytes,
        DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES
    );
    assert_eq!(config.tun_rx_drain_budget, DEFAULT_TUN_RX_DRAIN_BUDGET);
    assert_eq!(
        config.tun_rx_active_flow_timer_ms, DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS,
        "Knife14ex promotes a bounded active-flow ACK/window service lane to the default"
    );
    assert!(
        config.tun_rx_active_flow_timer_ms > 0,
        "the default service lane must not silently disable active-flow ACK/window drain"
    );
    assert!(
        !config.bounded_global_rx_receive_window,
        "Knife14cg bounded receive window is opt-in after VPS rejected it as a default"
    );
    assert!(
        !config.d3_egress_actor_adaptive_credit,
        "H10d15 adaptive credit-edge is diagnostic opt-in, not default D3 behavior"
    );
    assert!(
        !config.d3_egress_actor_legacy_credit,
        "H10d25 legacy credit replay is diagnostic opt-in, not default D3 behavior"
    );
    assert!(
        !config.d3_egress_actor_self_wake,
        "H10d27 self-wake is diagnostic opt-in, not default D3 behavior"
    );
}

/// 刀11：MINI_VPN_METRICS_SECS 解析——有效正整数采用；0/非数字/缺失回落默认（防 interval panic）。
#[test]
fn parse_metrics_secs_clamps_and_defaults() {
    assert_eq!(parse_metrics_secs(Some("5")), 5);
    assert_eq!(parse_metrics_secs(Some("  10 ")), 10);
    assert_eq!(
        parse_metrics_secs(Some("0")),
        METRICS_SNAPSHOT_SECS,
        "0 必回落（否则 interval panic）"
    );
    assert_eq!(parse_metrics_secs(Some("abc")), METRICS_SNAPSHOT_SECS);
    assert_eq!(parse_metrics_secs(Some("")), METRICS_SNAPSHOT_SECS);
    assert_eq!(parse_metrics_secs(None), METRICS_SNAPSHOT_SECS);
}

#[test]
fn parse_profile_loop_only_truthy_enables() {
    assert!(parse_profile_loop(Some("1")));
    assert!(parse_profile_loop(Some("true")));
    assert!(parse_profile_loop(Some(" TRUE ")));
    assert!(!parse_profile_loop(Some("0")));
    assert!(!parse_profile_loop(Some("false")));
    assert!(!parse_profile_loop(Some("yes")));
    assert!(!parse_profile_loop(Some("")));
    assert!(
        !parse_profile_loop(None),
        "缺省关——默认 NoopSink 零开销路径"
    );
}

/// 刀14c：TUN MTU 解析。1200 是 US-client suite 的测试基准；默认仍 1500 以保持零惊喜。
#[test]
fn parse_tun_mtu_accepts_safe_range_and_defaults() {
    assert_eq!(parse_tun_mtu(Some("1200")), 1200);
    assert_eq!(parse_tun_mtu(Some("  1500 ")), 1500);
    assert_eq!(parse_tun_mtu(Some("576")), 576);
    assert_eq!(parse_tun_mtu(Some("9000")), 9000);
    assert_eq!(parse_tun_mtu(Some("0")), DEFAULT_TUN_MTU);
    assert_eq!(parse_tun_mtu(Some("575")), DEFAULT_TUN_MTU);
    assert_eq!(parse_tun_mtu(Some("9001")), DEFAULT_TUN_MTU);
    assert_eq!(parse_tun_mtu(Some("abc")), DEFAULT_TUN_MTU);
    assert_eq!(parse_tun_mtu(Some("")), DEFAULT_TUN_MTU);
    assert_eq!(parse_tun_mtu(None), DEFAULT_TUN_MTU);
}

#[test]
fn parse_tun_tx_queue_len_accepts_positive_estimates_and_defaults() {
    assert_eq!(parse_tun_tx_queue_len(Some("5000")), 5000);
    assert_eq!(parse_tun_tx_queue_len(Some(" 500 ")), 500);
    assert_eq!(
        parse_tun_tx_queue_len(Some("0")),
        DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE
    );
    assert_eq!(
        parse_tun_tx_queue_len(Some("abc")),
        DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE
    );
    assert_eq!(
        parse_tun_tx_queue_len(None),
        DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE
    );
}

#[test]
fn parse_tun_rx_drain_budget_allows_zero_and_bounds() {
    assert_eq!(parse_tun_rx_drain_budget(None), DEFAULT_TUN_RX_DRAIN_BUDGET);
    assert_eq!(
        parse_tun_rx_drain_budget(Some("abc")),
        DEFAULT_TUN_RX_DRAIN_BUDGET
    );
    assert_eq!(parse_tun_rx_drain_budget(Some("0")), 0);
    assert_eq!(parse_tun_rx_drain_budget(Some(" 8 ")), 8);
    assert_eq!(
        parse_tun_rx_drain_budget(Some("64")),
        MAX_TUN_RX_DRAIN_BUDGET
    );
    assert_eq!(
        parse_tun_rx_drain_budget(Some("65")),
        DEFAULT_TUN_RX_DRAIN_BUDGET
    );
}

#[test]
fn parse_tun_rx_active_flow_timer_ms_defaults_to_service_lane_and_is_bounded() {
    assert_eq!(
        parse_tun_rx_active_flow_timer_ms(None),
        DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS
    );
    assert_eq!(
        parse_tun_rx_active_flow_timer_ms(Some("abc")),
        DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS
    );
    assert_eq!(parse_tun_rx_active_flow_timer_ms(Some("0")), 0);
    assert_eq!(parse_tun_rx_active_flow_timer_ms(Some(" 250 ")), 250);
    assert_eq!(
        parse_tun_rx_active_flow_timer_ms(Some("1000")),
        MAX_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS
    );
    assert_eq!(
        parse_tun_rx_active_flow_timer_ms(Some("1001")),
        DEFAULT_TUN_RX_ACTIVE_FLOW_TIMER_DRAIN_MS
    );
}

#[test]
fn tun_mtu_for_config_preserves_runtime_mtu() {
    assert_eq!(tun_mtu_for_config(1200), 1200);
    assert_eq!(tun_mtu_for_config(DEFAULT_TUN_MTU), 1500);
    assert_eq!(tun_mtu_for_config(MAX_TUN_MTU), 9000);
}

#[test]
fn tcp_downlink_diag_tracks_pending_acceptance_and_flush_failures() {
    let mut diag = TcpDownlinkDiag::default();
    diag.note_remote_payload(100, 100);
    diag.note_terminal_late_remote_payload(25);
    diag.note_send_slice_ok(60, 40);
    diag.note_send_slice_ok(0, 40);
    diag.note_send_slice_error();
    diag.note_tun_flush(true);
    diag.note_tun_flush(false);

    assert_eq!(diag.remote_to_global_rx_bytes, 100);
    assert_eq!(diag.terminal_late_remote_payload_bytes, 25);
    assert_eq!(diag.terminal_late_remote_payload_events, 1);
    assert_eq!(diag.downlink_pending_high_water, 100);
    assert_eq!(diag.send_slice_calls, 3);
    assert_eq!(diag.send_slice_accepted_bytes, 60);
    assert_eq!(diag.send_slice_zero, 1);
    assert_eq!(diag.send_slice_errors, 1);
    assert_eq!(diag.tun_flush_tx_calls, 2);
    assert_eq!(diag.tun_flush_tx_failures, 1);
}

#[test]
fn tcp_downlink_flush_progress_diag_tracks_budget_and_capacity() {
    let mut diag = TcpDownlinkDiag::default();
    diag.note_flush_attempt(600_000, 262_144);
    diag.note_send_slice_ok(262_144, 337_856);
    diag.note_flush_attempt(337_856, 262_144);
    diag.note_no_send_capacity(337_856);
    diag.note_flush_attempt(337_856, 262_144);
    diag.note_send_slice_ok(0, 337_856);
    diag.note_flush_attempt(337_856, 262_144);
    diag.note_send_slice_error();

    assert_eq!(diag.flush_attempts, 4);
    assert_eq!(diag.send_slice_no_capacity, 1);
    assert_eq!(diag.send_slice_budget_limited, 4);
    assert_eq!(diag.send_slice_calls, 3);
    assert_eq!(diag.send_slice_zero, 1);
    assert_eq!(diag.send_slice_errors, 1);
    assert_eq!(diag.send_slice_accepted_bytes, 262_144);
    assert_eq!(diag.send_slice_max_accepted_bytes, 262_144);
    assert_eq!(diag.downlink_pending_high_water, 600_000);
}

#[test]
fn tcp_downlink_diag_tracks_headroom_limited_flushes() {
    let mut diag = TcpDownlinkDiag::default();
    diag.note_flush_limit(
        64,
        64,
        DownlinkFlushLimit {
            len: 2,
            headroom_limited: true,
            headroom_deferred_bytes: 62,
            clean_headroom_bytes: 2,
            drain_credit_granted_bytes: 0,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 13,
            drop_credit_debt_paid_bytes: 5,
            drop_credit_blocked_bytes: 8,
            pressure_credit_debt_bytes: 21,
            pressure_credit_debt_paid_bytes: 3,
            pressure_credit_blocked_bytes: 11,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        },
    );

    assert_eq!(diag.flush_attempts, 1);
    assert_eq!(diag.send_slice_budget_limited, 0);
    assert_eq!(diag.send_slice_headroom_limited, 1);
    assert_eq!(diag.send_slice_headroom_deferred_bytes, 62);
    assert_eq!(diag.send_slice_drop_credit_debt_bytes, 13);
    assert_eq!(diag.send_slice_drop_credit_debt_paid_bytes, 5);
    assert_eq!(diag.send_slice_drop_credit_blocked_bytes, 8);
    assert_eq!(diag.send_slice_pressure_credit_debt_bytes, 21);
    assert_eq!(diag.send_slice_pressure_credit_debt_paid_bytes, 3);
    assert_eq!(diag.send_slice_pressure_credit_blocked_bytes, 11);
    assert_eq!(diag.send_slice_hard_edge_guard_limited, 0);
    assert_eq!(diag.send_slice_hard_edge_guard_deferred_bytes, 0);
    assert_eq!(diag.downlink_pending_high_water, 64);
}

#[test]
fn tcp_downlink_flush_aggregate_formats_progress_signal() {
    let mut a = SocketCtx::new(443);
    a.downlink_pending = vec![0; 10];
    a.downlink_diag.note_remote_payload(100, 100);
    a.downlink_diag.note_downstream_permit(100, 100);
    a.downlink_diag.note_downstream_permit_release(64, 36);
    a.downlink_diag.note_flush_attempt(100, 64);
    a.downlink_diag.note_send_window(
        TcpSendWindowSnapshot {
            can_send: true,
            may_send: true,
            may_recv: true,
            send_capacity: 1_048_576,
            send_queue: 1_048_576,
            recv_queue: 4096,
        },
        100,
    );
    a.downlink_diag.note_send_slice_ok(64, 36);
    a.downlink_diag.note_tun_flush(true);
    a.downlink_diag.note_tun_flush_deferred();
    a.downlink_credit_controller.egress_payload_credit_bytes = 1234;

    let mut b = SocketCtx::new(443);
    b.downlink_pending = vec![0; 25];
    b.downlink_diag.note_remote_payload(200, 200);
    b.downlink_diag.note_downstream_permit(200, 200);
    b.downlink_diag.note_downstream_permit_release(25, 175);
    b.downlink_diag.note_terminal_late_remote_payload(10);
    b.downlink_diag.note_terminal_late_remote_payload(15);
    b.downlink_diag.note_flush_attempt(200, 64);
    b.downlink_diag.note_send_window(
        TcpSendWindowSnapshot {
            can_send: false,
            may_send: false,
            may_recv: false,
            send_capacity: 0,
            send_queue: 0,
            recv_queue: 0,
        },
        200,
    );
    b.downlink_diag.note_no_send_capacity(200);
    b.downlink_diag.note_tun_flush(false);

    let aggregate = tcp_downlink_aggregate([&a, &b]);
    let line = format_tcp_downlink_flush_diag(&aggregate, 2);

    assert!(line.contains("tcp-downlink-flush"));
    assert!(line.contains("pending_total=35"));
    assert!(line.contains("pending_max=25"));
    assert!(line.contains("pending_high=200"));
    assert!(line.contains("remote_to_global_rx_bytes=300"));
    assert!(line.contains("downstream_permit_bytes=300"));
    assert!(line.contains("downstream_permit_released_bytes=89"));
    assert!(line.contains("downstream_permit_pending_high=200"));
    assert!(line.contains("terminal_late_remote_payload_bytes=25"));
    assert!(line.contains("terminal_late_remote_payload_events=2"));
    assert!(line.contains("flush_attempts=2"));
    assert!(line.contains("no_send_capacity=1"));
    assert!(line.contains("send_slice_calls=1"));
    assert!(line.contains("send_slice_accepted=64"));
    assert!(line.contains("budget_limited_calls=2"));
    assert!(line.contains("headroom_limited_calls=0"));
    assert!(line.contains("headroom_deferred_bytes=0"));
    assert!(line.contains("egress_payload_credit_bytes=1234"));
    assert!(line.contains("drop_credit_debt_bytes=0"));
    assert!(line.contains("drop_credit_debt_paid_bytes=0"));
    assert!(line.contains("drop_credit_blocked_bytes=0"));
    assert!(line.contains("pressure_credit_debt_bytes=0"));
    assert!(line.contains("pressure_credit_debt_paid_bytes=0"));
    assert!(line.contains("pressure_credit_blocked_bytes=0"));
    assert!(line.contains("hard_edge_guard_bytes=0"));
    assert!(line.contains("hard_edge_guard_limited_calls=0"));
    assert!(line.contains("hard_edge_guard_deferred_bytes=0"));
    assert!(line.contains("send_slice_max_accepted=64"));
    assert!(line.contains("send_window_samples=2"));
    assert!(line.contains("send_capacity_min=0"));
    assert!(line.contains("send_capacity_max=1048576"));
    assert!(line.contains("send_queue_max=1048576"));
    assert!(line.contains("recv_queue_max=4096"));
    assert!(line.contains("may_send_false=1"));
    assert!(line.contains("may_recv_false=1"));
    assert!(line.contains("no_send_capacity_streak_max=1"));
    assert!(line.contains("no_send_capacity_pending_max=200"));
    assert!(line.contains("tun_flush_tx_calls=2"));
    assert!(line.contains("tun_flush_tx_failures=1"));
    assert!(line.contains("tun_flush_deferred=1"));
    assert!(line.contains("dirty_handles=2"));
}

#[test]
fn tun_egress_drop_sampler_tracks_first_delta_and_reset() {
    let mut sampler = TunEgressDropSampler::new(Some("tun0"));

    assert_eq!(
        sampler.sample_with(|iface| {
            assert_eq!(iface, "tun0");
            Ok("100\n".to_string())
        }),
        TunEgressDropSample::First { total: 100 }
    );
    assert_eq!(
        sampler.sample_with(|iface| {
            assert_eq!(iface, "tun0");
            Ok("1523\n".to_string())
        }),
        TunEgressDropSample::Delta {
            total: 1523,
            delta: 1423,
        }
    );
    assert_eq!(
        sampler.sample_with(|iface| {
            assert_eq!(iface, "tun0");
            Ok("9\n".to_string())
        }),
        TunEgressDropSample::Reset {
            previous: 1523,
            total: 9,
        }
    );
}

#[test]
fn tun_egress_drop_sampler_reports_unavailable_without_behavior_pressure() {
    let mut no_name = TunEgressDropSampler::new(None);
    assert_eq!(
        no_name.sample_with(|_| unreachable!("no interface should not read sysfs")),
        TunEgressDropSample::Unavailable {
            reason: "no_interface"
        }
    );

    let mut invalid = TunEgressDropSampler::new(Some("../tun0"));
    assert_eq!(
        invalid.sample_with(|_| unreachable!("invalid interface should not read sysfs")),
        TunEgressDropSample::Unavailable {
            reason: "invalid_interface"
        }
    );

    let mut parent_dir = TunEgressDropSampler::new(Some(".."));
    assert_eq!(
        parent_dir.sample_with(|_| unreachable!("parent dir should not read sysfs")),
        TunEgressDropSample::Unavailable {
            reason: "invalid_interface"
        }
    );

    let mut read_err = TunEgressDropSampler::new(Some("tun0"));
    assert_eq!(
        read_err.sample_with(|_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "missing stat",
            ))
        }),
        TunEgressDropSample::Unavailable {
            reason: "read_error"
        }
    );

    let mut parse_err = TunEgressDropSampler::new(Some("tun0"));
    assert_eq!(
        parse_err.sample_with(|_| Ok("not-a-counter\n".to_string())),
        TunEgressDropSample::Unavailable {
            reason: "parse_error"
        }
    );
}

#[test]
fn tun_egress_diag_formats_drop_delta_with_downlink_context() {
    let aggregate = TcpDownlinkAggregate {
        pending_total: 10,
        pending_max: 7,
        pending_high: 100,
        remote_to_global_rx_bytes: 4096,
        tun_flush_tx_calls: 12,
        ..TcpDownlinkAggregate::default()
    };
    let line = format_tun_egress_diag(
        "tun0",
        &TunEgressDropSample::Delta {
            total: 1523,
            delta: 1423,
        },
        &aggregate,
        2,
        true,
    );

    assert!(line.contains("tcp-tun-egress"));
    assert!(line.contains("if=tun0"));
    assert!(line.contains("status=delta"));
    assert!(line.contains("tx_dropped_total=1523"));
    assert!(line.contains("tx_dropped_delta=1423"));
    assert!(line.contains("global_rx_paused=true"));
    assert!(line.contains("pending_total=10"));
    assert!(line.contains("pending_high=100"));
    assert!(line.contains("remote_to_global_rx_bytes=4096"));
    assert!(line.contains("tun_flush_tx_calls=12"));
    assert!(line.contains("dirty_handles=2"));
}

#[test]
fn local_egress_service_diag_reports_capacity_progress_and_stop_reason() {
    let mut diag = LocalEgressServiceDiag::default();
    let progress = LocalEgressServiceProgress {
        cycles: 3,
        accepted_bytes: LOCAL_EGRESS_SERVICE_TARGET_BYTES_PER_WINDOW,
        egress_drain_bytes: 4096,
        tun_rx_packets: 12,
        flush_tx_calls: 3,
        flush_tx_failures: 0,
        dirty_passes: 3,
        stop_reason: LocalEgressServiceStopReason::TargetReached,
    };

    diag.note_window(progress);
    let line = format_local_egress_service_diag(&diag, 0);

    assert_eq!(diag.windows, 1);
    assert_eq!(diag.cycles, 3);
    assert_eq!(
        diag.accepted_bytes,
        LOCAL_EGRESS_SERVICE_TARGET_BYTES_PER_WINDOW as u64
    );
    assert_eq!(diag.target_reached, 1);
    assert!(line.contains("tcp-local-egress-service"));
    assert!(line.contains("windows=1"));
    assert!(line.contains("cycles=3"));
    assert!(line.contains("accepted_bytes=131072"));
    assert!(line.contains("egress_drain_bytes=4096"));
    assert!(line.contains("tun_rx_packets=12"));
    assert!(line.contains("target_reached=1"));
    assert!(line.contains("no_progress=0"));
    assert!(line.contains("cycle_budget=0"));
}

#[test]
fn local_egress_service_diag_reports_egress_actor_subset() {
    let mut diag = LocalEgressServiceDiag::default();
    diag.note_egress_actor_immediate_wake();
    diag.note_egress_actor_self_wake();
    diag.note_egress_actor_window(LocalEgressServiceProgress {
        cycles: 2,
        accepted_bytes: 8192,
        egress_drain_bytes: 4096,
        dirty_passes: 2,
        stop_reason: LocalEgressServiceStopReason::NoProgress,
        ..LocalEgressServiceProgress::default()
    });

    let line = format_local_egress_service_diag(&diag, 0);

    assert_eq!(diag.windows, 1);
    assert_eq!(diag.egress_actor_windows, 1);
    assert_eq!(diag.egress_actor_cycles, 2);
    assert_eq!(diag.egress_actor_admitted_bytes, 8192);
    assert_eq!(diag.egress_actor_drain_bytes, 4096);
    assert_eq!(diag.egress_actor_no_progress, 1);
    assert_eq!(diag.egress_actor_immediate_wake, 1);
    assert_eq!(diag.egress_actor_self_wake, 1);
    assert_eq!(diag.egress_actor_external_wait, 1);
    assert!(line.contains("egress_actor_windows=1"));
    assert!(line.contains("egress_actor_cycles=2"));
    assert!(line.contains("egress_actor_admitted_bytes=8192"));
    assert!(line.contains("actor_bypass_admitted_bytes=0"));
    assert!(line.contains("egress_actor_drain_bytes=4096"));
    assert!(line.contains("egress_actor_no_progress=1"));
    assert!(line.contains("egress_actor_immediate_wake=1"));
    assert!(line.contains("egress_actor_self_wake=1"));
    assert!(line.contains("egress_actor_external_wait=1"));
}

#[test]
fn d3_actor_self_wake_requires_actor_flag_dirty_work_and_progress() {
    let mut config = TunRuntimeConfig::from_sources(None).expect("config should load");
    config.d3_egress_actor_self_wake = true;
    let progress = LocalEgressServiceProgress {
        accepted_bytes: 1,
        ..LocalEgressServiceProgress::default()
    };

    assert!(
        !d3_egress_actor_self_wake_enabled(&config),
        "SELF_WAKE alone must not affect the legacy local-egress loop"
    );
    assert!(!should_rearm_d3_egress_actor_self_wake(
        d3_egress_actor_self_wake_enabled(&config),
        true,
        progress
    ));

    config.d3_egress_actor = true;
    assert!(d3_egress_actor_self_wake_enabled(&config));
    assert!(should_rearm_d3_egress_actor_self_wake(
        d3_egress_actor_self_wake_enabled(&config),
        true,
        progress
    ));
    assert!(!should_rearm_d3_egress_actor_self_wake(
        d3_egress_actor_self_wake_enabled(&config),
        false,
        progress
    ));
    assert!(!should_rearm_d3_egress_actor_self_wake(
        d3_egress_actor_self_wake_enabled(&config),
        true,
        LocalEgressServiceProgress::default()
    ));
}

#[test]
fn d4_stalled_read_service_requires_d3_actor_and_expires_per_handle() {
    let mut config = TunRuntimeConfig::from_sources(None).expect("config should load");
    config.d4_stalled_read_service = true;
    assert!(
        !d4_stalled_read_service_enabled(&config),
        "D4 alone must not affect the legacy local-egress loop"
    );

    config.d3_egress_actor = true;
    assert!(d4_stalled_read_service_enabled(&config));
    assert!(!parse_d4_stalled_read_service(None));
    assert!(parse_d4_stalled_read_service(Some("1")));

    let mut sockets = SocketSet::new(vec![]);
    let first = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
    let second = sockets.add(build_listener_socket(&ListenerSpec { local_port: 8443 }));
    let now = std::time::Instant::now();
    let mut stalled = HashMap::new();

    assert!(arm_d4_stalled_read_service(
        d4_stalled_read_service_enabled(&config),
        &mut stalled,
        first,
        now
    ));
    assert!(has_d4_stalled_read_service(&mut stalled, now));
    assert!(clear_d4_stalled_read_service(&mut stalled, first));
    assert!(!has_d4_stalled_read_service(&mut stalled, now));

    arm_d4_stalled_read_service(true, &mut stalled, first, now);
    arm_d4_stalled_read_service(true, &mut stalled, second, now);
    assert_eq!(stalled.len(), 2);
    assert!(clear_d4_stalled_read_service(&mut stalled, first));
    assert_eq!(
        stalled.len(),
        1,
        "read recovery for one flow must not clear another stalled flow"
    );
    assert!(!has_d4_stalled_read_service(
        &mut stalled,
        now + std::time::Duration::from_millis(D4_STALLED_READ_SERVICE_HOLD_MS + 1)
    ));
    assert!(stalled.is_empty());
}

#[test]
fn d5_capacity_backpressure_requires_d3_actor() {
    let mut config = TunRuntimeConfig::from_sources(None).expect("config should load");
    config.d5_capacity_backpressure = true;
    assert!(
        !d5_capacity_backpressure_enabled(&config),
        "D5 alone must not affect the legacy local-egress loop"
    );
    assert!(!parse_d5_capacity_backpressure(None));
    assert!(parse_d5_capacity_backpressure(Some("1")));

    config.d3_egress_actor = true;
    assert!(d5_capacity_backpressure_enabled(&config));
}

#[test]
fn tun_rx_drain_diag_reports_stalled_read_timer_attempts() {
    let mut diag = TunRxDrainDiag::default();
    diag.note_attempt(TUN_RX_DRAIN_SOURCE_TIMER_STALLED_READ);
    let line = format_tun_rx_drain_diag(&diag);

    assert_eq!(diag.timer_stalled_read_attempts, 1);
    assert!(line.contains("timer_stalled_read_attempts=1"), "{line}");
}

#[test]
fn local_egress_service_progress_counts_tun_tx_drain_as_progress() {
    let progress = LocalEgressServiceProgress {
        egress_drain_bytes: 1200,
        ..LocalEgressServiceProgress::default()
    };

    assert!(
        progress.has_progress(),
        "TUN TX drain is useful local egress progress even when no new socket bytes were accepted"
    );
}

#[test]
fn local_egress_pressure_snapshot_counts_ack_drain_before_flush_window() {
    let before = LocalEgressPressureSnapshot {
        queued_tx_bytes: 0,
        send_queue_bytes: 65_536,
    };
    let after = LocalEgressPressureSnapshot {
        queued_tx_bytes: 0,
        send_queue_bytes: 1_408,
    };

    assert_eq!(before.drain_bytes_to(after), 64_128);
}

#[test]
fn local_egress_pressure_snapshot_counts_tun_tx_flush_drain() {
    let before = LocalEgressPressureSnapshot {
        queued_tx_bytes: 32_768,
        send_queue_bytes: 0,
    };
    let after = LocalEgressPressureSnapshot {
        queued_tx_bytes: 1_200,
        send_queue_bytes: 0,
    };

    assert_eq!(before.drain_bytes_to(after), 31_568);
}

#[test]
fn local_egress_pressure_snapshot_uses_net_drain_not_growth() {
    let before = LocalEgressPressureSnapshot {
        queued_tx_bytes: 0,
        send_queue_bytes: 8_192,
    };
    let after = LocalEgressPressureSnapshot {
        queued_tx_bytes: 0,
        send_queue_bytes: 16_384,
    };

    assert_eq!(before.drain_bytes_to(after), 0);
}

#[test]
fn local_egress_service_default_capacity_floor_covers_100mbit_path() {
    let cfg = LocalEgressServiceConfig::default();
    let bytes_per_second = cfg.target_bytes_per_window * 1000 / 5;

    assert_eq!(
        cfg.target_bytes_per_window,
        LOCAL_EGRESS_SERVICE_TARGET_BYTES_PER_WINDOW
    );
    assert_eq!(cfg.target_bytes_per_window, 128 * 1024);
    assert!(
        bytes_per_second >= 13_000_000,
        "G5 code-level floor must exceed the 100M path requirement: {bytes_per_second}B/s"
    );
    assert!(
        cfg.max_cycles >= 2,
        "service lane must be allowed to repeat inside one active window"
    );
}

#[test]
fn downlink_egress_pacer_allows_budget_then_defers_until_timer_reset() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 1_000_000,
        low_bytes: 250_000,
    };
    let mut pacer = DownlinkEgressPacer::new(65_536);

    assert!(
        pacer.allow_remote_payload_flush(32_768, 0, 0, cfg),
        "first accepted slice should flush immediately while budget remains"
    );
    assert!(
        pacer.allow_remote_payload_flush(32_768, 0, 0, cfg),
        "budget may be consumed exactly"
    );
    assert!(
        !pacer.allow_remote_payload_flush(1, 0, 0, cfg),
        "extra accepted bytes in the same interval should defer to timer flush"
    );

    pacer.on_timer_tick();
    assert!(
        pacer.allow_remote_payload_flush(1, 0, 0, cfg),
        "timer reset should reopen the immediate flush budget"
    );
}

#[test]
fn downlink_egress_pacer_zero_budget_defers_only_when_no_backlog_remains() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 1_000_000,
        low_bytes: 250_000,
    };
    let mut pacer = DownlinkEgressPacer::new(0);

    assert!(
        !pacer.allow_remote_payload_flush(1, 0, 0, cfg),
        "zero budget should defer when the current payload left no backlog"
    );
    assert!(
        !pacer.allow_remote_payload_flush(0, 0, 0, cfg),
        "zero accepted bytes should never force an immediate TUN flush"
    );
    assert!(
        pacer.allow_remote_payload_flush(0, 1, 0, cfg),
        "pending backlog should force an immediate flush attempt even with zero budget"
    );
    pacer.on_timer_tick();
    assert!(
        !pacer.allow_remote_payload_flush(65_536, 0, 0, cfg),
        "timer reset should preserve an explicitly disabled no-backlog budget"
    );
}

#[test]
fn downlink_egress_pacer_forces_flush_while_pending_remains() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 1_000_000,
        low_bytes: 250_000,
    };
    let mut pacer = DownlinkEgressPacer::new(65_536);

    assert!(
        pacer.allow_remote_payload_flush(65_537, 1, 0, cfg),
        "pending backlog must force immediate egress even when accepted bytes exceed budget"
    );
}

#[test]
fn downlink_egress_pacer_uses_tx_queue_headroom_when_no_pending_remains() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut pacer = DownlinkEgressPacer::new(100);

    assert!(
        pacer.allow_remote_payload_flush(32, 0, 100, cfg),
        "tx-queue-only pressure at soft high should still allow immediate flush"
    );
    assert_eq!(
        pacer.remaining_immediate_bytes, 68,
        "allowed no-pending flush should consume the immediate budget"
    );

    pacer.on_timer_tick();
    assert!(
        pacer.allow_remote_payload_flush(32, 0, tx_queue_flush_threshold(cfg) - 1, cfg),
        "tx-queue-only pressure below the bounded flush cap should still allow immediate flush"
    );

    pacer.on_timer_tick();
    assert!(
        !pacer.allow_remote_payload_flush(32, 0, tx_queue_flush_threshold(cfg), cfg),
        "tx-queue-only pressure at the bounded flush cap should defer immediate flush"
    );
    assert_eq!(
        pacer.remaining_immediate_bytes, 68,
        "bytes accepted into smoltcp should still consume budget when deferred"
    );
}

#[test]
fn downlink_egress_pacer_bounds_no_pending_flush_before_hard_cap() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut pacer = DownlinkEgressPacer::new(100);

    assert!(
        pacer.allow_remote_payload_flush(16, 0, tx_queue_flush_threshold(cfg) - 1, cfg),
        "no-pending flush should keep some headroom above soft high"
    );

    pacer.on_timer_tick();
    assert!(
        !pacer.allow_remote_payload_flush(16, 0, tx_queue_flush_threshold(cfg), cfg),
        "no-pending flush should stop at the bounded midpoint before hard cap"
    );
    assert_eq!(
        pacer.remaining_immediate_bytes, 84,
        "bytes accepted into smoltcp should still consume budget when deferred"
    );
}

#[test]
fn downlink_egress_pacer_defers_pending_flush_at_send_queue_high_watermark() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut pacer = DownlinkEgressPacer::new(100);

    assert!(
        !pacer.allow_remote_payload_flush(32, 1, 100, cfg),
        "local send queue at high watermark should defer remote-payload TUN flush"
    );
    assert_eq!(
        pacer.remaining_immediate_bytes, 68,
        "bytes accepted into smoltcp should still consume the immediate budget"
    );

    pacer.on_timer_tick();
    assert!(
        pacer.allow_remote_payload_flush(0, 1, 99, cfg),
        "pending backlog can still force progress while send queue is below high"
    );
}

#[test]
fn downlink_pending_stats_track_max_and_total() {
    let mut a = SocketCtx::new(443);
    a.downlink_pending = vec![0; 10];
    let mut b = SocketCtx::new(443);
    b.downlink_pending = vec![0; 25];

    let stats = downlink_pending_stats([&a, &b]);
    assert_eq!(stats.max_pending, 25);
    assert_eq!(stats.total_pending, 35);
    assert_eq!(stats.max_tx_queue, 0);
    assert_eq!(stats.total_tx_queue, 0);
    assert_eq!(stats.max_pressure(), 25);
    assert_eq!(stats.total_pressure(), 35);
}

#[test]
fn global_pending_scan_runs_only_for_buffered_credit() {
    let scans = std::cell::Cell::new(0usize);
    let disabled_pending = global_pending_for_buffered_credit(false, || {
        scans.set(scans.get() + 1);
        4096
    });
    assert_eq!(disabled_pending, 0);
    assert_eq!(
        scans.get(),
        0,
        "D16/non-buffered relay passes must not scan every dirty flow and socket"
    );

    let enabled_pending = global_pending_for_buffered_credit(true, || {
        scans.set(scans.get() + 1);
        4096
    });
    assert_eq!(enabled_pending, 4096);
    assert_eq!(
        scans.get(),
        1,
        "buffered credit may evaluate the aggregate only once per dirty pass"
    );
}

#[test]
fn buffered_downlink_config_derives_bounded_watermarks_from_backpressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let buffered = BufferedDownlinkConfig::from_backpressure(true, cfg, 3);

    assert!(buffered.enabled);
    assert_eq!(buffered.per_flow_low_bytes, 100);
    assert_eq!(buffered.per_flow_high_bytes, 400);
    assert_eq!(buffered.per_flow_hard_bytes, 800);
    assert_eq!(buffered.global_low_bytes, 300);
    assert_eq!(buffered.global_high_bytes, 1200);
    assert_eq!(buffered.global_hard_bytes, 2400);
}

#[test]
fn buffered_downlink_read_credit_ignores_transient_local_headroom_pressure() {
    let cfg = BufferedDownlinkConfig::from_backpressure(
        true,
        DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        },
        1,
    );

    let decision = buffered_downlink_read_credit(false, 0, 0, false, false, cfg);

    assert_eq!(decision.reason, BufferedDownlinkCreditReason::Open);
    assert_eq!(
        decision.credit,
        RelayReadCredit {
            paused: false,
            max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES,
        }
    );
}

#[test]
fn buffered_downlink_soft_high_keeps_useful_read_floor() {
    let cfg = BufferedDownlinkConfig::from_backpressure(
        true,
        DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        },
        1,
    );

    let decision =
        buffered_downlink_read_credit(false, cfg.per_flow_high_bytes, 0, false, false, cfg);

    assert_eq!(decision.reason, BufferedDownlinkCreditReason::SoftHigh);
    assert_eq!(
        decision.credit,
        RelayReadCredit {
            paused: false,
            max_batch_bytes: RELAY_REMOTE_READ_MIN_BATCH_BYTES,
        },
        "soft buffered pressure must not collapse remote reads to MTU-sized 1200B service"
    );
}

#[test]
fn buffered_downlink_pauses_only_at_hard_watermarks_and_resumes_at_low() {
    let cfg = BufferedDownlinkConfig::from_backpressure(
        true,
        DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        },
        2,
    );

    assert!(
        !buffered_downlink_read_credit(false, cfg.per_flow_hard_bytes - 1, 0, false, false, cfg)
            .credit
            .paused
    );
    let hard = buffered_downlink_read_credit(false, cfg.per_flow_hard_bytes, 0, false, false, cfg);
    assert_eq!(hard.reason, BufferedDownlinkCreditReason::PerFlowHard);
    assert!(hard.credit.paused);
    assert!(
        buffered_downlink_read_credit(true, cfg.per_flow_low_bytes + 1, 0, false, false, cfg)
            .credit
            .paused,
        "paused flow stays paused above low watermark"
    );
    assert!(
        !buffered_downlink_read_credit(true, cfg.per_flow_low_bytes, 0, false, false, cfg)
            .credit
            .paused,
        "flow resumes once buffered bytes drain to low watermark"
    );
}

#[test]
fn buffered_downlink_global_hard_protects_memory_without_tx_queue_pressure() {
    let cfg = BufferedDownlinkConfig::from_backpressure(
        true,
        DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        },
        2,
    );

    let hard = buffered_downlink_read_credit(
        false,
        cfg.per_flow_low_bytes,
        cfg.global_hard_bytes,
        false,
        false,
        cfg,
    );
    assert_eq!(hard.reason, BufferedDownlinkCreditReason::GlobalHard);
    assert!(hard.credit.paused);
}

#[test]
fn buffered_global_rx_window_uses_buffer_watermarks_not_tx_queue_pressure() {
    let cfg = BufferedDownlinkConfig::from_backpressure(
        true,
        DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        },
        2,
    );
    let tx_queue_only = DownlinkPressureStats::new(0, 0, usize::MAX / 4, usize::MAX / 4);

    assert!(
        !next_buffered_global_rx_receive_backpressure(false, tx_queue_only, cfg),
        "buffered global receive is governed by pending bytes, not smoltcp tx_queue-only pressure"
    );
    assert!(
        next_buffered_global_rx_receive_backpressure(
            false,
            DownlinkPressureStats::pending_only(cfg.per_flow_hard_bytes, cfg.global_low_bytes),
            cfg,
        ),
        "per-flow hard buffered bytes pause global_rx receive"
    );
    assert!(
        next_buffered_global_rx_receive_backpressure(
            false,
            DownlinkPressureStats::pending_only(cfg.per_flow_low_bytes, cfg.global_hard_bytes),
            cfg,
        ),
        "global hard buffered bytes pause global_rx receive"
    );
}

#[test]
fn buffered_downlink_credit_diag_reports_attribution_fields() {
    let cfg = BufferedDownlinkConfig::from_backpressure(
        true,
        DownlinkBackpressureConfig {
            high_bytes: 100,
            low_bytes: 40,
        },
        2,
    );
    let mut sockets = SocketSet::new(vec![]);
    let handle = mk_test_handle(&mut sockets);
    let decision = buffered_downlink_read_credit(false, 401, 500, false, false, cfg);

    let line = format_buffered_downlink_read_credit_diag(handle, 7, decision, 401, 500, cfg);

    assert!(line.contains("tcp-buffered-downlink-credit"), "{line}");
    assert!(line.contains("epoch=7"), "{line}");
    assert!(line.contains("paused=false"), "{line}");
    assert!(line.contains("max_batch_bytes=65536"), "{line}");
    assert!(line.contains("reason=soft_high"), "{line}");
    assert!(line.contains("per_flow_pending=401"), "{line}");
    assert!(line.contains("global_pending=500"), "{line}");
    assert!(line.contains("per_flow_hard=800"), "{line}");
    assert!(line.contains("global_hard=1600"), "{line}");
}

#[test]
fn downlink_backpressure_uses_high_low_hysteresis() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };

    assert!(
        !next_downlink_backpressure(false, DownlinkPressureStats::pending_only(99, 99), cfg),
        "below high watermark keeps global_rx enabled"
    );
    assert!(
        next_downlink_backpressure(false, DownlinkPressureStats::pending_only(100, 100), cfg),
        "hitting high watermark pauses global_rx"
    );
    assert!(
        next_downlink_backpressure(true, DownlinkPressureStats::pending_only(41, 41), cfg),
        "while paused, stay paused until below low watermark"
    );
    assert!(
        !next_downlink_backpressure(true, DownlinkPressureStats::pending_only(40, 40), cfg),
        "low watermark resumes global_rx"
    );
}

#[test]
fn downlink_backpressure_uses_tx_queue_pressure_when_pending_is_empty() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };

    assert!(
        !next_downlink_backpressure(
            false,
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_pause_threshold(cfg) - 1,
                tx_queue_pause_threshold(cfg) - 1
            ),
            cfg
        ),
        "below the tx-queue-only hard cap keeps global_rx enabled when app pending is empty"
    );
    assert!(
        next_downlink_backpressure(
            false,
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_pause_threshold(cfg),
                tx_queue_pause_threshold(cfg)
            ),
            cfg
        ),
        "hitting the tx-queue-only hard cap pauses global_rx"
    );
    assert!(
        next_downlink_backpressure(true, DownlinkPressureStats::new(0, 0, 101, 101), cfg),
        "while paused, tx queue pressure stays paused above the soft high watermark"
    );
    assert!(
        !next_downlink_backpressure(true, DownlinkPressureStats::new(0, 0, 100, 100), cfg),
        "soft high watermark resumes global_rx when app pending is empty"
    );
}

#[test]
fn downlink_backpressure_uses_tx_queue_headroom_before_pausing() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };

    assert!(
        !next_downlink_backpressure(false, DownlinkPressureStats::new(0, 0, 100, 100), cfg),
        "tx-queue-only pressure at the soft high watermark should not pause immediately"
    );
    assert!(
        !next_downlink_backpressure(
            false,
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_pause_threshold(cfg) - 1,
                tx_queue_pause_threshold(cfg) - 1
            ),
            cfg
        ),
        "bounded tx-queue-only headroom should keep global_rx enabled"
    );
    assert!(
        next_downlink_backpressure(
            false,
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_pause_threshold(cfg),
                tx_queue_pause_threshold(cfg)
            ),
            cfg
        ),
        "tx-queue-only pressure still pauses at the derived hard cap"
    );
    assert!(
        next_downlink_backpressure(true, DownlinkPressureStats::new(0, 0, 101, 101), cfg),
        "while paused by tx queue pressure, stay paused above the soft high watermark"
    );
    assert!(
        !next_downlink_backpressure(true, DownlinkPressureStats::new(0, 0, 100, 100), cfg),
        "tx-queue-only pressure resumes once it drains to the soft high watermark"
    );
}

#[test]
fn d5_capacity_backpressure_does_not_pause_on_h10d32_static_pressure() {
    let cfg =
        default_downlink_backpressure_for_tun_egress(1_200, DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE);
    let h10d32_pressure = DownlinkPressureStats::new(329_128, 329_128, 557_386, 557_386);

    assert!(
        next_downlink_backpressure(false, h10d32_pressure, cfg),
        "legacy/D3 static pressure gate pauses at the H10d32 burst/idle edge"
    );
    assert!(
        !next_downlink_backpressure_for_runtime(true, false, h10d32_pressure, cfg, false),
        "D5 must keep reading while pending is below the bounded receive window and no TUN debt exists"
    );
    assert!(
        next_downlink_backpressure_for_runtime(true, false, h10d32_pressure, cfg, true),
        "active TUN/drop debt remains a hard pause even in D5"
    );

    let full_pending = DownlinkPressureStats::pending_only(
        downlink_receive_window_high(cfg),
        downlink_receive_window_high(cfg),
    );
    assert!(
        next_downlink_backpressure_for_runtime(true, false, full_pending, cfg, false),
        "D5 still pauses when app pending reaches its bounded receive window"
    );
}

#[test]
fn downlink_backpressure_active_credit_debt_pauses_at_credit_edge() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let credit_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg),
        tx_queue_credit_spend_threshold(cfg),
    );
    let recovered = DownlinkPressureStats::new(0, 0, 100, 100);

    assert!(
        !next_downlink_backpressure(false, credit_edge, cfg),
        "without active credit debt, the tx-queue credit edge remains below the hard pause edge"
    );
    assert!(
        next_downlink_backpressure_with_credit_debt(false, credit_edge, cfg, true),
        "active credit debt should pause remote receive at the credit edge before more pending accumulates"
    );
    assert!(
        !next_downlink_backpressure_with_credit_debt(true, recovered, cfg, true),
        "once pressure reaches the ordinary resume threshold, active debt should not deadlock receive"
    );
}

#[test]
fn downlink_backpressure_holds_recent_high_tx_queue_after_flush() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut feedback = TunEgressFeedbackState::default();
    let now = std::time::Instant::now();

    feedback.observe_pressure_at(DownlinkPressureStats::new(0, 0, 160, 160), cfg, now);

    let held = feedback.effective_downlink_pressure_at(
        DownlinkPressureStats::default(),
        now + std::time::Duration::from_millis(TUN_EGRESS_PRESSURE_HOLD_MS - 1),
    );
    assert_eq!(held.max_pressure(), 160);
    assert!(
        next_downlink_backpressure(true, held, cfg),
        "recent hard egress pressure keeps global_rx paused briefly after raw pressure drains"
    );

    let expired = feedback.effective_downlink_pressure_at(
        DownlinkPressureStats::default(),
        now + std::time::Duration::from_millis(TUN_EGRESS_PRESSURE_HOLD_MS + 1),
    );
    assert_eq!(expired.max_pressure(), 0);
    assert!(
        !next_downlink_backpressure(true, expired, cfg),
        "after the bounded hold expires, ordinary low watermark resume applies"
    );
}

#[test]
fn downlink_backpressure_does_not_hold_soft_tx_queue_pressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut feedback = TunEgressFeedbackState::default();
    let now = std::time::Instant::now();

    feedback.observe_pressure_at(DownlinkPressureStats::new(0, 0, 100, 100), cfg, now);

    let effective = feedback.effective_downlink_pressure_at(
        DownlinkPressureStats::default(),
        now + std::time::Duration::from_millis(TUN_EGRESS_PRESSURE_HOLD_MS - 1),
    );
    assert_eq!(
        effective,
        DownlinkPressureStats::default(),
        "soft tx-queue-only pressure should not install an egress hold"
    );
}

#[test]
fn downlink_backpressure_diag_reports_raw_and_effective_pressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let raw = DownlinkPressureStats::default();
    let effective = DownlinkPressureStats::new(0, 0, 100, 100);

    let line = format_tcp_downlink_backpressure_diag(true, raw, effective, cfg);

    assert!(line.contains("tcp-downlink-backpressure"), "{line}");
    assert!(line.contains("paused=true"), "{line}");
    assert!(line.contains("max_tx_queue=100"), "{line}");
    assert!(line.contains("tx_queue_flush_high=130"), "{line}");
    assert!(line.contains("tx_queue_pause_high=160"), "{line}");
    assert!(line.contains("tx_queue_resume_high=100"), "{line}");
    assert!(line.contains("raw_max_tx_queue=0"), "{line}");
    assert!(line.contains("effective_max_tx_queue=100"), "{line}");
    assert!(line.contains("held_pressure=true"), "{line}");
}

#[test]
fn global_rx_receive_window_ignores_tx_queue_only_pressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let tx_queue_only_pressure = DownlinkPressureStats::new(0, 0, 160, 160);

    assert!(
        next_downlink_backpressure(false, tx_queue_only_pressure, cfg),
        "local egress backpressure should still pause at the tx-queue hard cap"
    );
    assert!(
        !next_global_rx_receive_backpressure(false, tx_queue_only_pressure, cfg),
        "tx-queue-only pressure must not stop bounded relay receive progress"
    );
}

#[test]
fn global_rx_receive_window_uses_pending_high_low_hysteresis() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };

    assert_eq!(downlink_receive_window_high(cfg), 400);
    assert_eq!(downlink_receive_window_low(cfg), 100);

    assert!(
        !next_global_rx_receive_backpressure(
            false,
            DownlinkPressureStats::pending_only(399, 399),
            cfg,
        ),
        "pending below the receive-window high watermark should keep relay receive enabled"
    );
    assert!(
        next_global_rx_receive_backpressure(
            false,
            DownlinkPressureStats::pending_only(400, 400),
            cfg,
        ),
        "pending at the bounded receive-window high watermark should pause relay receive"
    );
    assert!(
        next_global_rx_receive_backpressure(
            true,
            DownlinkPressureStats::pending_only(101, 101),
            cfg,
        ),
        "while paused, receive stays paused above the receive low watermark"
    );
    assert!(
        !next_global_rx_receive_backpressure(
            true,
            DownlinkPressureStats::pending_only(100, 100),
            cfg,
        ),
        "receive resumes after pending drains to the receive low watermark"
    );
}

#[test]
fn global_rx_receive_diag_reports_local_egress_and_feedback_state() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let stats = DownlinkPressureStats::new(150, 275, 160, 160);

    let line = format_tcp_global_rx_backpressure_diag(true, stats, cfg, true, true);

    assert!(line.contains("tcp-global-rx-backpressure"), "{line}");
    assert!(line.contains("paused=true"), "{line}");
    assert!(line.contains("max_pending=150"), "{line}");
    assert!(line.contains("total_pending=275"), "{line}");
    assert!(line.contains("max_tx_queue=160"), "{line}");
    assert!(line.contains("receive_high=400"), "{line}");
    assert!(line.contains("receive_low=100"), "{line}");
    assert!(line.contains("local_egress_paused=true"), "{line}");
    assert!(line.contains("tun_feedback_paused=true"), "{line}");
}

#[test]
fn tun_egress_feedback_pauses_on_drop_delta_with_local_pressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 4_194_304,
        low_bytes: 1_048_576,
    };
    let mut feedback = TunEgressFeedbackState::default();
    let event = feedback.update(
        &TunEgressDropSample::Delta {
            total: 28_123,
            delta: 14_167,
        },
        DownlinkPressureStats::new(0, 0, 3_786_786, 3_786_786),
        cfg,
    );

    assert!(feedback.is_paused());
    assert_eq!(feedback.drop_events, 1);
    assert_eq!(feedback.drop_delta_total, 14_167);
    assert_eq!(feedback.max_drop_delta, 14_167);
    assert_eq!(feedback.pause_edges, 1);
    assert_eq!(feedback.resume_edges, 0);
    assert_eq!(
        event,
        Some(TunEgressFeedbackEvent {
            paused: true,
            reason: TunEgressFeedbackReason::DropDelta,
            tx_dropped_delta: 14_167,
            max_pressure: 3_786_786,
            total_pressure: 3_786_786,
        })
    );
}

#[test]
fn tun_egress_feedback_pauses_on_drop_delta_with_recent_high_pressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 4_194_304,
        low_bytes: 1_048_576,
    };
    let mut feedback = TunEgressFeedbackState::default();
    feedback.observe_pressure(DownlinkPressureStats::new(0, 0, 3_786_786, 3_786_786), cfg);
    feedback.observe_pressure(DownlinkPressureStats::new(0, 0, 4_194_304, 4_194_304), cfg);

    let event = feedback.update(
        &TunEgressDropSample::Delta {
            total: 28_123,
            delta: 14_167,
        },
        DownlinkPressureStats::default(),
        cfg,
    );

    assert!(feedback.is_paused());
    assert_eq!(feedback.drop_events, 1);
    assert_eq!(feedback.pause_edges, 1);
    assert_eq!(
        event,
        Some(TunEgressFeedbackEvent {
            paused: true,
            reason: TunEgressFeedbackReason::DropDelta,
            tx_dropped_delta: 14_167,
            max_pressure: 4_194_304,
            total_pressure: 4_194_304,
        }),
        "drop sampled after pressure drains should still be attributed to recent high pressure"
    );
}

#[test]
fn tun_egress_feedback_resumes_after_pressure_drains_to_low() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 4_194_304,
        low_bytes: 1_048_576,
    };
    let mut feedback = TunEgressFeedbackState::default();
    assert!(
        feedback
            .update(
                &TunEgressDropSample::Delta {
                    total: 10,
                    delta: 10,
                },
                DownlinkPressureStats::new(0, 0, 2_000_000, 2_000_000),
                cfg,
            )
            .is_some()
    );

    let repeated_drop = feedback.update(
        &TunEgressDropSample::Delta {
            total: 15,
            delta: 5,
        },
        DownlinkPressureStats::new(0, 0, 1_500_000, 1_500_000),
        cfg,
    );
    assert_eq!(
        repeated_drop,
        Some(TunEgressFeedbackEvent {
            paused: true,
            reason: TunEgressFeedbackReason::DropDelta,
            tx_dropped_delta: 5,
            max_pressure: 1_500_000,
            total_pressure: 1_500_000,
        }),
        "drops while already paused should remain visible"
    );
    assert_eq!(
        feedback.pause_edges, 1,
        "already-paused drops must not add pause edges"
    );

    assert_eq!(
        feedback.update(
            &TunEgressDropSample::Delta {
                total: 15,
                delta: 0,
            },
            DownlinkPressureStats::new(0, 0, 1_048_577, 1_048_577),
            cfg,
        ),
        None,
        "stay paused above low watermark"
    );

    let event = feedback.update(
        &TunEgressDropSample::Delta {
            total: 15,
            delta: 0,
        },
        DownlinkPressureStats::new(0, 0, 1_048_576, 1_048_576),
        cfg,
    );
    assert!(!feedback.is_paused());
    assert_eq!(feedback.pause_edges, 1);
    assert_eq!(feedback.resume_edges, 1);
    assert_eq!(
        event,
        Some(TunEgressFeedbackEvent {
            paused: false,
            reason: TunEgressFeedbackReason::PressureLow,
            tx_dropped_delta: 0,
            max_pressure: 1_048_576,
            total_pressure: 1_048_576,
        })
    );
}

#[test]
fn d16_global_drop_episode_pays_cross_sample_drain_once_before_recovery() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut feedback = TunEgressFeedbackState::default();
    let mut debt = DownlinkEgressDropDebt::default();
    let mut episode = D16GlobalDropEpisode::default();

    let drop_event = feedback
        .update(
            &TunEgressDropSample::Delta {
                total: 2_029,
                delta: 2_029,
            },
            DownlinkPressureStats::new(0, 0, 70, 160),
            cfg,
        )
        .expect("drop under pressure must start the global episode");
    assert_eq!(
        apply_d16_global_tun_feedback(&mut episode, Some(drop_event), cfg, &mut debt,),
        D16GlobalRecoveryEvidence::default()
    );
    let installed_debt = debt.debt_bytes;
    assert!(installed_debt > 0);
    assert!(debt.has_active_drop_debt());

    let low_event = feedback
        .update(
            &TunEgressDropSample::Delta {
                total: 2_029,
                delta: 0,
            },
            DownlinkPressureStats::default(),
            cfg,
        )
        .expect("raw pressure-low must end the feedback pause");
    let recovery = apply_d16_global_tun_feedback(&mut episode, Some(low_event), cfg, &mut debt);

    assert_eq!(recovery.observed_drain_bytes, 160);
    assert_eq!(recovery.debt_paid_bytes, installed_debt);
    assert!(recovery.clean_drain);
    assert!(!debt.has_active_drop_debt());
    assert_eq!(
        transition_egress_phase(
            EgressPhase::DrainOnly,
            false,
            debt.has_active_drop_debt(),
            true,
            recovery.clean_drain,
        ),
        EgressPhase::Recovery { clean_cycles: 0 }
    );

    assert_eq!(
        apply_d16_global_tun_feedback(&mut episode, Some(low_event), cfg, &mut debt),
        D16GlobalRecoveryEvidence::default(),
        "one pressure-low edge must not pay or publish recovery twice"
    );
}

#[test]
fn d16_global_recovery_rearms_every_eligible_flow_even_when_not_dirty() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut sockets = SocketSet::new(vec![]);
    let first = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
    let second = sockets.add(build_listener_socket(&ListenerSpec { local_port: 8443 }));
    let mut socket_ctxs = HashMap::new();
    for (handle, port) in [(first, 443), (second, 8443)] {
        let mut ctx = SocketCtx::new(port);
        ctx.d16_downlink_queue = Some(
            AsyncLeasedByteFlowQueue::new_with_release_mode(
                D16_PER_FLOW_BYTE_CAPACITY,
                DownstreamPermitReleaseMode::OnEgressDrain,
            )
            .unwrap(),
        );
        ctx.d16_egress_phase = EgressPhase::DrainOnly;
        socket_ctxs.insert(handle, ctx);
    }
    let mut dirty = HashSet::new();

    let transitioned = apply_d16_global_recovery_evidence(
        &mut dirty,
        &sockets,
        &mut socket_ctxs,
        cfg,
        D16GlobalRecoveryEvidence {
            observed_drain_bytes: 160,
            debt_paid_bytes: 30,
            clean_drain: true,
        },
    );

    assert_eq!(transitioned, 2);
    for handle in [first, second] {
        let ctx = socket_ctxs.get(&handle).unwrap();
        assert_eq!(
            ctx.d16_egress_phase,
            EgressPhase::Recovery { clean_cycles: 0 }
        );
        assert_eq!(ctx.downlink_diag.d16_egress_phase_transitions, 1);
        assert!(dirty.contains(&handle));
    }
}

#[test]
fn d16_tun_rx_ack_completion_barrier_isolated_per_flow() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let snapshot = |send_queue| SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: true,
        can_recv: true,
        may_send: true,
        may_recv: true,
        send_capacity: 1024,
        send_queue,
        recv_queue: 0,
    };
    let mut slow = SocketCtx::new(443);
    slow.d16_egress_phase = EgressPhase::DrainOnly;
    let mut ready = SocketCtx::new(8443);
    ready.d16_egress_phase = EgressPhase::DrainOnly;
    arm_d16_tun_rx_ack_barrier(&mut slow, snapshot(1));
    arm_d16_tun_rx_ack_barrier(&mut ready, snapshot(0));

    let slow_next =
        next_d16_egress_phase_for_snapshot(&mut slow, snapshot(1), cfg, false, false, true);
    let ready_next =
        next_d16_egress_phase_for_snapshot(&mut ready, snapshot(0), cfg, false, false, true);

    assert_eq!(slow_next, EgressPhase::DrainOnly);
    assert!(slow.d16_tun_rx_ack_barrier);
    assert_eq!(ready_next, EgressPhase::Recovery { clean_cycles: 0 });
    assert!(!ready.d16_tun_rx_ack_barrier);
}

#[test]
fn d16_ack_completion_evidence_survives_hard_pressure_until_clean_snapshot() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let snapshot = |send_queue| SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: true,
        can_recv: true,
        may_send: true,
        may_recv: true,
        send_capacity: 1024,
        send_queue,
        recv_queue: 0,
    };
    let mut ctx = SocketCtx::new(443);
    ctx.d16_egress_phase = EgressPhase::DrainOnly;
    arm_d16_tun_rx_ack_barrier(&mut ctx, snapshot(1));

    let still_hard =
        next_d16_egress_phase_for_snapshot(&mut ctx, snapshot(0), cfg, true, false, false);
    assert_eq!(still_hard, EgressPhase::DrainOnly);
    assert!(
        ctx.d16_tun_rx_ack_barrier,
        "a hard-pressure-dominated zero snapshot must not consume ACK-completion evidence"
    );

    ctx.d16_egress_phase = still_hard;
    let clean = next_d16_egress_phase_for_snapshot(&mut ctx, snapshot(0), cfg, false, false, false);
    assert_eq!(clean, EgressPhase::Recovery { clean_cycles: 0 });
    assert!(!ctx.d16_tun_rx_ack_barrier);
}

#[test]
fn d16_ack_completion_recovers_eight_flows_without_weakening_stop_conditions() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let established = |send_queue| SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: true,
        can_recv: true,
        may_send: true,
        may_recv: true,
        send_capacity: 1024,
        send_queue,
        recv_queue: 0,
    };
    let mut flows: Vec<_> = (0..8)
        .map(|port| {
            let mut ctx = SocketCtx::new(10_000 + port);
            ctx.d16_egress_phase = EgressPhase::DrainOnly;
            arm_d16_tun_rx_ack_barrier(&mut ctx, established(1));
            ctx
        })
        .collect();

    for ctx in &mut flows {
        ctx.d16_egress_phase =
            next_d16_egress_phase_for_snapshot(ctx, established(0), cfg, true, false, false);
        assert_eq!(ctx.d16_egress_phase, EgressPhase::DrainOnly);
        assert!(ctx.d16_tun_rx_ack_barrier);
    }
    for ctx in &mut flows {
        ctx.d16_egress_phase =
            next_d16_egress_phase_for_snapshot(ctx, established(0), cfg, false, false, false);
        assert_eq!(
            ctx.d16_egress_phase,
            EgressPhase::Recovery { clean_cycles: 0 }
        );
        assert!(!ctx.d16_tun_rx_ack_barrier);
    }

    let mut no_barrier = SocketCtx::new(443);
    no_barrier.d16_egress_phase = EgressPhase::DrainOnly;
    assert_eq!(
        next_d16_egress_phase_for_snapshot(
            &mut no_barrier,
            established(0),
            cfg,
            false,
            false,
            false,
        ),
        EgressPhase::DrainOnly,
        "a zero snapshot without prior nonzero ownership must not invent progress"
    );

    let mut debt = SocketCtx::new(8443);
    debt.d16_egress_phase = EgressPhase::DrainOnly;
    arm_d16_tun_rx_ack_barrier(&mut debt, established(1));
    assert_eq!(
        next_d16_egress_phase_for_snapshot(&mut debt, established(0), cfg, false, true, false),
        EgressPhase::DrainOnly
    );
    assert!(debt.d16_tun_rx_ack_barrier);

    let mut terminal = SocketCtx::new(9443);
    terminal.d16_egress_phase = EgressPhase::DrainOnly;
    arm_d16_tun_rx_ack_barrier(&mut terminal, established(1));
    let terminal_snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::Closed,
        active: false,
        can_send: false,
        can_recv: false,
        may_send: false,
        may_recv: false,
        send_capacity: 0,
        send_queue: 0,
        recv_queue: 0,
    };
    assert_eq!(
        next_d16_egress_phase_for_snapshot(
            &mut terminal,
            terminal_snapshot,
            cfg,
            false,
            false,
            false,
        ),
        EgressPhase::DrainOnly
    );
    assert!(terminal.d16_tun_rx_ack_barrier);
}

#[test]
fn d16_zero_byte_actor_cycle_does_not_advance_recovery() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
    let mut ctx = SocketCtx::new(443);
    ctx.d16_downlink_queue = Some(
        AsyncLeasedByteFlowQueue::new_with_release_mode(
            D16_PER_FLOW_BYTE_CAPACITY,
            DownstreamPermitReleaseMode::OnEgressDrain,
        )
        .unwrap(),
    );
    ctx.d16_egress_phase = EgressPhase::DrainOnly;
    let mut socket_ctxs = HashMap::from([(handle, ctx)]);
    let dirty = HashSet::from([handle]);

    transition_dirty_d16_egress_phases(
        &dirty,
        &sockets,
        &mut socket_ctxs,
        cfg,
        false,
        false,
        false,
        Some(0),
    );

    let ctx = socket_ctxs.get(&handle).unwrap();
    assert_eq!(ctx.d16_egress_phase, EgressPhase::DrainOnly);
    assert_eq!(ctx.downlink_diag.d16_drain_only_cycles, 1);
    assert_eq!(ctx.downlink_diag.d16_drain_only_drain_bytes, 0);
    assert_eq!(ctx.downlink_diag.d16_egress_phase_transitions, 0);
}

#[test]
fn d16_tun_rx_backlog_remains_drain_only_after_positive_egress_drain() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
    let mut ctx = SocketCtx::new(443);
    ctx.d16_downlink_queue = Some(
        AsyncLeasedByteFlowQueue::new_with_release_mode(
            D16_PER_FLOW_BYTE_CAPACITY,
            DownstreamPermitReleaseMode::OnEgressDrain,
        )
        .unwrap(),
    );
    ctx.d16_egress_phase = EgressPhase::DrainOnly;
    let mut socket_ctxs = HashMap::from([(handle, ctx)]);
    let dirty = HashSet::from([handle]);

    transition_dirty_d16_egress_phases(
        &dirty,
        &sockets,
        &mut socket_ctxs,
        cfg,
        false,
        true,
        false,
        Some(128),
    );

    let ctx = socket_ctxs.get(&handle).unwrap();
    assert_eq!(
        ctx.d16_egress_phase,
        EgressPhase::DrainOnly,
        "a proven device backlog must dominate positive per-flow drain evidence"
    );
}

#[test]
fn tun_egress_feedback_resumes_on_raw_low_while_hold_is_active() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut feedback = TunEgressFeedbackState::default();
    let now = std::time::Instant::now();

    assert!(
        feedback
            .update(
                &TunEgressDropSample::Delta {
                    total: 10,
                    delta: 10,
                },
                DownlinkPressureStats::new(0, 0, 160, 160),
                cfg,
            )
            .is_some()
    );
    assert!(feedback.is_paused());

    feedback.observe_pressure_at(DownlinkPressureStats::new(0, 0, 160, 160), cfg, now);
    let held = feedback.effective_downlink_pressure_at(
        DownlinkPressureStats::default(),
        now + std::time::Duration::from_millis(1),
    );
    assert_eq!(
        held.max_pressure(),
        160,
        "ordinary downlink backpressure still gets the short egress hold"
    );

    let event = feedback.update(
        &TunEgressDropSample::Delta {
            total: 10,
            delta: 0,
        },
        DownlinkPressureStats::default(),
        cfg,
    );

    assert!(!feedback.is_paused());
    assert_eq!(feedback.resume_edges, 1);
    assert_eq!(
        event,
        Some(TunEgressFeedbackEvent {
            paused: false,
            reason: TunEgressFeedbackReason::PressureLow,
            tx_dropped_delta: 0,
            max_pressure: 0,
            total_pressure: 0,
        }),
        "feedback pause resumes from raw low pressure even while held pressure remains available for backpressure"
    );
}

#[test]
fn tun_egress_feedback_ignores_unavailable_reset_first_and_zero_pressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut feedback = TunEgressFeedbackState::default();
    let samples = [
        TunEgressDropSample::Unavailable {
            reason: "read_error",
        },
        TunEgressDropSample::First { total: 5 },
        TunEgressDropSample::Reset {
            previous: 10,
            total: 1,
        },
        TunEgressDropSample::Delta {
            total: 10,
            delta: 0,
        },
        TunEgressDropSample::Delta {
            total: 20,
            delta: 10,
        },
    ];
    let stats = [
        DownlinkPressureStats::new(0, 0, 99, 99),
        DownlinkPressureStats::new(0, 0, 99, 99),
        DownlinkPressureStats::new(0, 0, 99, 99),
        DownlinkPressureStats::new(0, 0, 99, 99),
        DownlinkPressureStats::default(),
    ];

    for (sample, stats) in samples.iter().zip(stats) {
        assert_eq!(feedback.update(sample, stats, cfg), None);
    }
    assert!(!feedback.is_paused());
    assert_eq!(
        feedback.drop_events, 1,
        "zero-pressure positive drops are counted but do not pause"
    );
    assert_eq!(feedback.pause_edges, 0);
}

#[test]
fn tun_egress_feedback_diag_formats_transition_counters() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 4_194_304,
        low_bytes: 1_048_576,
    };
    let mut feedback = TunEgressFeedbackState::default();
    let event = feedback
        .update(
            &TunEgressDropSample::Delta {
                total: 28_123,
                delta: 14_167,
            },
            DownlinkPressureStats::new(0, 0, 3_786_786, 3_786_786),
            cfg,
        )
        .expect("drop delta should pause feedback");
    let mut drop_debt = DownlinkEgressDropDebt::default();
    drop_debt.note_tun_drop(cfg);
    let line = format_tun_egress_feedback_diag(event, &feedback, cfg, drop_debt);

    assert!(line.contains("tcp-tun-egress-feedback"));
    assert!(line.contains("paused=true"));
    assert!(line.contains("reason=drop_delta"));
    assert!(line.contains("tx_dropped_delta=14167"));
    assert!(line.contains("max_pressure=3786786"));
    assert!(line.contains("high=4194304"));
    assert!(line.contains("low=1048576"));
    assert!(line.contains("drop_events=1"));
    assert!(line.contains("drop_delta_total=14167"));
    assert!(line.contains("max_delta=14167"));
    assert!(line.contains("pause_edges=1"));
    assert!(line.contains("resume_edges=0"));
    assert!(line.contains("egress_credit_generation=1"));
    assert!(line.contains("egress_credit_debt_bytes=1572864"));
    assert!(line.contains("drop_credit_debt_bytes=1572864"));
    assert!(line.contains("pressure_credit_debt_bytes=0"));
    assert!(line.contains("drop_credit_events=1"));
    assert!(line.contains("pressure_credit_events=0"));
}

#[test]
fn tun_rx_drain_diag_tracks_budget_and_packet_kinds() {
    let mut diag = TunRxDrainDiag::default();

    diag.note_attempt(TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_PRE);
    diag.note_attempt(TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD);
    diag.note_attempt(TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED);
    diag.note_attempt(TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED_DELAYED);
    diag.note_attempt(TUN_RX_DRAIN_SOURCE_REMOTE_PAYLOAD_DEFERRED_PRESSURE);
    diag.note_attempt(TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT);
    diag.note_attempt(TUN_RX_DRAIN_SOURCE_RELAY_GAP_HINT_FOLLOWUP);
    diag.note_attempt(TUN_RX_DRAIN_SOURCE_TIMER_PRESSURE);
    diag.note_attempt(TUN_RX_DRAIN_SOURCE_TIMER_ACTIVE_FLOW);
    diag.note_attempt("other");
    diag.note_packet(TunRxPacketKind::Tcp);
    diag.note_packet(TunRxPacketKind::Dns);
    diag.note_packet(TunRxPacketKind::Udp);
    diag.note_tcp_batch_poll_flush(1);
    diag.note_tcp_batch_dirty_relay_pass(1);
    diag.note_budget_exhausted();
    diag.note_would_block();
    diag.note_error();

    let line = format_tun_rx_drain_diag(&diag);
    assert!(line.contains("tcp-tun-rx-drain"));
    assert!(line.contains("attempts=10"), "{line}");
    assert!(line.contains("pre_payload_attempts=1"), "{line}");
    assert!(line.contains("remote_payload_attempts=1"), "{line}");
    assert!(
        line.contains("remote_payload_deferred_attempts=1"),
        "{line}"
    );
    assert!(
        line.contains("remote_payload_deferred_delayed_attempts=1"),
        "{line}"
    );
    assert!(
        line.contains("remote_payload_deferred_pressure_attempts=1"),
        "{line}"
    );
    assert!(line.contains("relay_gap_hint_attempts=1"), "{line}");
    assert!(
        line.contains("relay_gap_hint_followup_attempts=1"),
        "{line}"
    );
    assert!(line.contains("maintenance_attempts=1"), "{line}");
    assert!(line.contains("timer_active_flow_attempts=1"), "{line}");
    assert!(line.contains("other_attempts=1"), "{line}");
    assert!(line.contains("packets=3"), "{line}");
    assert!(line.contains("tcp=1"), "{line}");
    assert!(line.contains("dns=1"), "{line}");
    assert!(line.contains("udp=1"), "{line}");
    assert!(line.contains("tcp_batches=1"), "{line}");
    assert!(line.contains("batch_dirty_relay_passes=1"), "{line}");
    assert!(line.contains("batch_iface_polls=1"), "{line}");
    assert!(line.contains("batch_flushes=1"), "{line}");
    assert!(line.contains("tcp_batch_packets_high_water=1"), "{line}");
    assert!(line.contains("avoided_dirty_relay_passes=0"), "{line}");
    assert!(line.contains("budget_exhausted=1"), "{line}");
    assert!(line.contains("would_block=1"), "{line}");
    assert!(line.contains("errors=1"), "{line}");
}

#[test]
fn tun_rx_backlog_recovery_waits_for_control_epoch_and_later_clean_probe() {
    let mut guard = TunRxBacklogGuard::default();

    guard.observe(TunRxBacklogObservation::BacklogProven);
    guard.observe(TunRxBacklogObservation::CleanWouldBlock);
    assert!(
        guard.is_active(),
        "the first clean edge must only arm recovery"
    );
    assert_eq!(guard.resume_edges(), 0);

    guard.note_control_epoch_completed();
    guard.observe(TunRxBacklogObservation::CleanWouldBlock);
    assert!(!guard.is_active());
    assert_eq!(guard.resume_edges(), 1);
}

#[test]
fn tun_rx_backlog_guard_restarts_recovery_after_renewed_backlog() {
    let mut guard = TunRxBacklogGuard::default();

    guard.observe(TunRxBacklogObservation::BacklogProven);
    assert!(guard.is_active());
    assert_eq!(guard.pause_edges(), 1);

    guard.observe(TunRxBacklogObservation::ReadError);
    assert!(guard.is_active(), "read errors cannot forgive backlog");

    guard.observe(TunRxBacklogObservation::CleanWouldBlock);
    assert!(guard.is_active());
    guard.note_control_epoch_completed();

    guard.observe(TunRxBacklogObservation::BacklogProven);
    assert!(
        guard.is_active(),
        "renewed backlog must cancel armed recovery"
    );
    assert_eq!(guard.pause_edges(), 1);

    guard.observe(TunRxBacklogObservation::CleanWouldBlock);
    assert!(guard.is_active());
    guard.note_control_epoch_completed();
    guard.observe(TunRxBacklogObservation::CleanWouldBlock);
    assert!(!guard.is_active());
    assert_eq!(guard.resume_edges(), 1);
}

#[test]
fn tun_rx_backlog_guard_coalesces_repeated_edges() {
    let mut guard = TunRxBacklogGuard::default();

    guard.observe(TunRxBacklogObservation::BacklogProven);
    guard.observe(TunRxBacklogObservation::BacklogProven);
    assert_eq!(guard.pause_edges(), 1);

    guard.observe(TunRxBacklogObservation::CleanWouldBlock);
    guard.note_control_epoch_completed();
    guard.observe(TunRxBacklogObservation::CleanWouldBlock);
    guard.observe(TunRxBacklogObservation::CleanWouldBlock);
    assert_eq!(guard.resume_edges(), 1);
}

#[test]
fn tun_rx_transient_backlog_episode_recovers_only_after_later_clean_epoch() {
    let before = TunRxBacklogGuard::default();
    let mut after = before;
    after.observe(TunRxBacklogObservation::BacklogProven);
    after.observe(TunRxBacklogObservation::CleanWouldBlock);

    assert_eq!(
        tun_rx_backlog_action(before, after),
        TunRxBacklogAction::ForceDrainOnly,
        "the enlarged bounded drain may arm recovery but must remain DrainOnly"
    );

    after.note_control_epoch_completed();
    let before_recovery = after;
    after.observe(TunRxBacklogObservation::CleanWouldBlock);
    assert_eq!(
        tun_rx_backlog_action(before_recovery, after),
        TunRxBacklogAction::Recover,
    );
}

#[test]
fn tun_rx_drain_after_remote_payload_tracks_acceptance_or_pending() {
    assert!(should_drain_tun_rx_after_remote_payload(None, 1));
    assert!(!should_drain_tun_rx_after_remote_payload(None, 0));

    let mut ctx = SocketCtx::new(443);
    assert!(!should_drain_tun_rx_after_remote_payload(Some(&ctx), 0));

    ctx.downlink_pending.extend_from_slice(&[1, 2, 3]);
    assert!(should_drain_tun_rx_after_remote_payload(Some(&ctx), 0));
}

#[test]
fn relay_gap_ack_drain_state_coalesces_budget_exhausted_followups() {
    let mut state = RelayGapAckDrainState::default();

    assert!(!state.is_armed());
    assert!(state.arm_after_budget_exhausted());
    assert!(state.is_armed());
    assert!(
        !state.arm_after_budget_exhausted(),
        "additional exhausted gap drains should share the already armed follow-up"
    );
    assert_eq!(
        state.take_due(),
        Some(RelayGapAckDrainFollowup::BudgetExhausted)
    );
    assert!(!state.is_armed());
    assert!(
        state.take_due().is_none(),
        "a consumed follow-up should not fire again without another exhausted drain"
    );
    assert!(state.arm_after_budget_exhausted());
}

#[test]
fn relay_gap_ack_drain_state_probes_after_empty_hints_then_stops() {
    let mut state = RelayGapAckDrainState::default();

    assert!(state.arm_after_hint_drain(0, 240));
    assert_eq!(state.take_due(), Some(RelayGapAckDrainFollowup::Probe));

    for remaining in (0..=RELAY_GAP_ACK_DRAIN_EMPTY_FOLLOWUPS).rev() {
        let should_rearm = remaining > 0;
        assert_eq!(
            state.arm_after_followup_drain(RelayGapAckDrainFollowup::Probe, 0, 240),
            should_rearm,
            "empty relay-gap probes must be bounded"
        );
        if should_rearm {
            assert_eq!(state.take_due(), Some(RelayGapAckDrainFollowup::Probe));
        }
    }
    assert!(!state.is_armed());
}

#[test]
fn relay_gap_ack_drain_state_keeps_progress_probes_short() {
    let mut state = RelayGapAckDrainState::default();

    assert!(state.arm_after_hint_drain(1, 240));
    assert_eq!(state.take_due(), Some(RelayGapAckDrainFollowup::Probe));
    assert!(state.arm_after_followup_drain(RelayGapAckDrainFollowup::Probe, 0, 240));
    assert_eq!(state.take_due(), Some(RelayGapAckDrainFollowup::Probe));
    assert!(
        !state.arm_after_followup_drain(RelayGapAckDrainFollowup::Probe, 0, 240),
        "a progress probe gets one trailing empty retry, not an unbounded timer"
    );
}

#[test]
fn relay_gap_ack_drain_state_escalates_probe_back_to_budget_followup() {
    let mut state = RelayGapAckDrainState::default();

    assert!(state.arm_after_hint_drain(0, 240));
    assert_eq!(state.take_due(), Some(RelayGapAckDrainFollowup::Probe));
    assert!(state.arm_after_followup_drain(RelayGapAckDrainFollowup::Probe, 240, 240));
    assert_eq!(
        state.take_due(),
        Some(RelayGapAckDrainFollowup::BudgetExhausted),
        "a probe that drains a full budget must switch to the existing pressure follow-up path"
    );
}

#[test]
fn deferred_ack_drain_state_coalesces_remote_payload_bursts() {
    let mut state = DeferredAckDrainState::default();

    assert!(!state.is_armed());
    assert!(state.arm_after_remote_payload());
    assert!(state.is_armed());
    assert!(
        !state.arm_after_remote_payload(),
        "additional remote payloads should share the already armed one-shot drain"
    );
    assert_eq!(state.take_due(), Some(DeferredAckDrainMode::ActiveFlow));
    assert!(state.arm_after_delayed_ack_probe());
    assert_eq!(state.take_due(), Some(DeferredAckDrainMode::DelayedAck));
    assert!(
        state.take_due().is_none(),
        "a delayed ACK probe is a bounded second chance, not a repeating timer"
    );
    assert!(!state.is_armed());
    assert!(
        state.take_due().is_none(),
        "a consumed one-shot should not fire again without new downlink work"
    );
    assert!(state.arm_after_remote_payload());
}

#[test]
fn deferred_ack_drain_state_escalates_after_budget_exhaustion() {
    let mut state = DeferredAckDrainState::default();

    assert!(state.take_due().is_none());
    assert!(state.arm_after_remote_payload());
    assert_eq!(state.take_due(), Some(DeferredAckDrainMode::ActiveFlow));

    assert!(state.arm_after_budget_exhausted());
    assert_eq!(state.take_due(), Some(DeferredAckDrainMode::Pressure));
    assert!(state.arm_after_delayed_ack_probe());
    assert_eq!(state.take_due(), Some(DeferredAckDrainMode::DelayedAck));
    assert!(state.arm_after_budget_exhausted());
    assert_eq!(
        state.take_due(),
        Some(DeferredAckDrainMode::Pressure),
        "a delayed ACK probe that exhausts its budget should reuse the bounded pressure path"
    );
    assert!(state.arm_after_budget_exhausted());
    assert_eq!(
        state.take_due(),
        Some(DeferredAckDrainMode::Pressure),
        "pressure follow-up should stay pressure-sized while it continues exhausting"
    );

    assert!(state.arm_after_remote_payload());
    assert_eq!(
        state.take_due(),
        Some(DeferredAckDrainMode::ActiveFlow),
        "a fresh remote payload starts with the normal active-flow budget"
    );
}

#[test]
fn deferred_ack_drain_budget_tapers_with_egress_headroom() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 524_288,
        low_bytes: 131_072,
    };
    let healthy = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_flush_threshold(cfg).saturating_sub(1),
        tx_queue_flush_threshold(cfg).saturating_sub(1),
    );
    let near_credit = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
        tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
    );
    let credit_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg),
        tx_queue_credit_spend_threshold(cfg),
    );
    let pause_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_pause_threshold(cfg),
        tx_queue_pause_threshold(cfg),
    );

    assert_eq!(
        tun_rx_drain_budget_for_deferred_ack_drain(
            DeferredAckDrainMode::ActiveFlow,
            healthy,
            false,
            0,
            cfg,
            1_200,
            false
        ),
        active_flow_tun_rx_drain_budget(cfg, 1_200),
        "deferred ACK drain may run at full budget while egress has flush headroom"
    );
    assert_eq!(
        tun_rx_drain_budget_for_deferred_ack_drain(
            DeferredAckDrainMode::ActiveFlow,
            near_credit,
            false,
            0,
            cfg,
            1_200,
            false
        ),
        1,
        "deferred ACK drain should taper to a single ACK/window packet near the credit edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_deferred_ack_drain(
            DeferredAckDrainMode::ActiveFlow,
            credit_edge,
            false,
            0,
            cfg,
            1_200,
            false
        ),
        1,
        "credit edge still needs a trickle ACK drain to avoid TUIC stream starvation"
    );
    assert_eq!(
        tun_rx_drain_budget_for_deferred_ack_drain(
            DeferredAckDrainMode::ActiveFlow,
            pause_edge,
            false,
            0,
            cfg,
            1_200,
            false
        ),
        0,
        "deferred ACK drain must stop at the hard local egress pause edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_deferred_ack_drain(
            DeferredAckDrainMode::ActiveFlow,
            healthy,
            true,
            0,
            cfg,
            1_200,
            false
        ),
        0,
        "TUN egress feedback pause should suppress active ACK drain"
    );
    assert_eq!(
        tun_rx_drain_budget_for_deferred_ack_drain(
            DeferredAckDrainMode::ActiveFlow,
            healthy,
            false,
            9,
            cfg,
            1_200,
            false
        ),
        9,
        "explicit operator budget still controls the healthy maximum"
    );
    assert_eq!(
        tun_rx_drain_budget_for_deferred_ack_drain(
            DeferredAckDrainMode::ActiveFlow,
            credit_edge,
            false,
            9,
            cfg,
            1_200,
            false
        ),
        1,
        "explicit operator budget is still clamped by the safety headroom curve"
    );
    assert_eq!(
        tun_rx_drain_budget_for_deferred_ack_drain(
            DeferredAckDrainMode::DelayedAck,
            healthy,
            false,
            0,
            cfg,
            1_200,
            false
        ),
        active_flow_tun_rx_drain_budget(cfg, 1_200),
        "delayed ACK probe uses the same small active-flow budget as the immediate deferred drain"
    );
    assert_eq!(
        tun_rx_drain_budget_for_deferred_ack_drain(
            DeferredAckDrainMode::Pressure,
            healthy,
            false,
            0,
            cfg,
            1_200,
            false
        ),
        pressure_tun_rx_drain_budget(cfg, 1_200),
        "a budget-exhausted deferred drain escalates to pressure budget"
    );
}

#[test]
fn d5_capacity_ack_drain_ignores_static_pause_edge_without_tun_feedback() {
    let cfg =
        default_downlink_backpressure_for_tun_egress(1_200, DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE);
    let pause_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_pause_threshold(cfg),
        tx_queue_pause_threshold(cfg),
    );

    assert_eq!(
        tun_rx_drain_budget_for_recent_active_flow(
            true, 10, pause_edge, false, 0, cfg, 1_200, false
        ),
        0,
        "old ACK/window service stops at the static send_queue pause edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_recent_active_flow(
            true, 10, pause_edge, false, 0, cfg, 1_200, true
        ),
        active_flow_tun_rx_drain_budget(cfg, 1_200),
        "D5 keeps ACK/window service active unless real TUN feedback pauses it"
    );
    assert_eq!(
        tun_rx_drain_budget_for_recent_active_flow(true, 10, pause_edge, true, 0, cfg, 1_200, true),
        0,
        "TUN feedback pause still suppresses ACK/window service in D5"
    );
}

#[test]
fn relay_gap_hint_tun_rx_drain_budget_requires_current_relay_epoch() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 524_288,
        low_bytes: 131_072,
    };
    let healthy = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_flush_threshold(cfg).saturating_sub(1),
        tx_queue_flush_threshold(cfg).saturating_sub(1),
    );
    let near_credit = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
        tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
    );
    let credit_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg),
        tx_queue_credit_spend_threshold(cfg),
    );
    let pause_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_pause_threshold(cfg),
        tx_queue_pause_threshold(cfg),
    );
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.conn_epoch = 7;

    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 6, healthy, false, 0, cfg, 1_200, false),
        0,
        "stale relay gap hints must not scan TUN RX"
    );

    ctx.state = SocketState::Listening;
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 7, healthy, false, 0, cfg, 1_200, false),
        0,
        "non-relay slots must ignore late gap hints"
    );

    ctx.state = SocketState::Relaying;
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 7, healthy, false, 0, cfg, 1_200, false),
        pressure_tun_rx_drain_budget(cfg, 1_200),
        "current relay read gaps are a strong ACK/window starvation signal"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_hint(
            Some(&ctx),
            7,
            near_credit,
            false,
            0,
            cfg,
            1_200,
            false
        ),
        1,
        "relay gap hints should taper to a single packet near the credit edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_hint(
            Some(&ctx),
            7,
            credit_edge,
            false,
            0,
            cfg,
            1_200,
            false
        ),
        1,
        "relay gap hints must keep a trickle ACK drain at the credit edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_hint(
            Some(&ctx),
            7,
            pause_edge,
            false,
            0,
            cfg,
            1_200,
            false
        ),
        0,
        "relay gap hints should stop once local egress reaches the hard pause edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 7, healthy, true, 0, cfg, 1_200, false),
        0,
        "TUN feedback pause should suppress relay gap hints"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_hint(Some(&ctx), 7, healthy, false, 9, cfg, 1_200, false),
        9,
        "explicit operator drain budget still controls the healthy maximum"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_hint(
            Some(&ctx),
            7,
            credit_edge,
            false,
            9,
            cfg,
            1_200,
            false
        ),
        1,
        "explicit operator drain budget is still clamped by the safety headroom curve"
    );
}

#[test]
fn relay_gap_followup_tun_rx_drain_budget_tapers_with_egress_headroom() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 524_288,
        low_bytes: 131_072,
    };
    let healthy = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_flush_threshold(cfg).saturating_sub(1),
        tx_queue_flush_threshold(cfg).saturating_sub(1),
    );
    let near_credit = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
        tx_queue_credit_spend_threshold(cfg).saturating_sub(1),
    );
    let credit_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg),
        tx_queue_credit_spend_threshold(cfg),
    );
    let pause_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_pause_threshold(cfg),
        tx_queue_pause_threshold(cfg),
    );

    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_followup(healthy, false, 0, cfg, 1_200, false),
        pressure_tun_rx_drain_budget(cfg, 1_200),
        "gap follow-up should continue at pressure budget while egress is healthy"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_followup(near_credit, false, 0, cfg, 1_200, false),
        1,
        "gap follow-up should taper to a single ACK/window packet near credit edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_followup(credit_edge, false, 0, cfg, 1_200, false),
        1,
        "credit edge still keeps a trickle ACK drain for window recovery"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_followup(pause_edge, false, 0, cfg, 1_200, false),
        0,
        "gap follow-up must stop at the hard local egress pause edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_followup(healthy, true, 0, cfg, 1_200, false),
        0,
        "TUN feedback pause should suppress gap follow-up"
    );
    assert_eq!(
        tun_rx_drain_budget_for_relay_gap_followup(healthy, false, 9, cfg, 1_200, false),
        9,
        "explicit operator budget still controls the healthy maximum"
    );
}

#[test]
fn tun_rx_drain_budget_keeps_explicit_override() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    assert_eq!(
        tun_rx_drain_budget_after_remote_payload(false, 0, 7, cfg, 1200, false),
        0,
        "explicit budget should not drain when the payload has no local work"
    );
    assert_eq!(
        tun_rx_drain_budget_after_remote_payload(true, 0, 7, cfg, 1200, false),
        7,
        "explicit MINI_VPN_TUN_RX_DRAIN_BUDGET remains an override"
    );
    assert_eq!(
        tun_rx_drain_budget_after_remote_payload(
            true,
            tx_queue_credit_spend_threshold(cfg),
            7,
            cfg,
            1200,
            false,
        ),
        7,
        "explicit override should not be reshaped by adaptive pressure"
    );
}

#[test]
fn tun_rx_drain_budget_stays_off_without_downlink_work() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let credit_edge = tx_queue_credit_spend_threshold(cfg);

    assert_eq!(
        tun_rx_drain_budget_after_remote_payload(false, credit_edge, 0, cfg, 1200, false),
        0,
        "pressure alone should not scan TUN RX without downlink work"
    );
}

#[test]
fn tun_rx_drain_budget_enables_active_flow_below_credit_edge() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let credit_edge = tx_queue_credit_spend_threshold(cfg);

    assert_eq!(
        tun_rx_drain_budget_after_remote_payload(
            true,
            credit_edge.saturating_sub(1),
            0,
            cfg,
            1200,
            false,
        ),
        active_flow_tun_rx_drain_budget(cfg, 1200),
        "active reverse payload should get a bounded ACK/window drain before pressure"
    );
}

#[test]
fn tun_rx_drain_budget_enables_adaptive_at_credit_edge() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    assert_eq!(tx_queue_credit_guard_bytes(cfg), 3);
    assert_eq!(tx_queue_credit_spend_threshold(cfg), 157);

    assert_eq!(
        tun_rx_drain_budget_after_remote_payload(
            true,
            tx_queue_credit_spend_threshold(cfg),
            0,
            cfg,
            1200,
            false,
        ),
        1,
        "small guards should still get one pressure ACK drain packet"
    );
}

#[test]
fn pre_payload_tun_rx_drain_budget_requires_current_accepting_payload() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx::new(443);
    ctx.state = SocketState::Relaying;
    ctx.uplink_tx = Some(tx);
    ctx.conn_epoch = 7;
    let active_pressure = SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: true,
        can_recv: true,
        may_send: true,
        may_recv: true,
        send_capacity: 1_048_576,
        send_queue: tx_queue_credit_spend_threshold(cfg),
        recv_queue: 0,
    };

    assert_eq!(
        tun_rx_drain_budget_before_remote_payload(
            Some(&ctx),
            active_pressure,
            7,
            1_200,
            0,
            cfg,
            1_200,
            false,
        ),
        1,
        "current accepting payload at credit edge should pre-drain ACKs"
    );
    assert_eq!(
        tun_rx_drain_budget_before_remote_payload(
            Some(&ctx),
            active_pressure,
            7,
            0,
            0,
            cfg,
            1_200,
            false,
        ),
        0,
        "empty payload should not scan TUN RX"
    );
    assert_eq!(
        tun_rx_drain_budget_before_remote_payload(
            Some(&ctx),
            active_pressure,
            6,
            1_200,
            0,
            cfg,
            1_200,
            false,
        ),
        0,
        "stale epoch must not pre-drain for a rearmed socket"
    );
    assert_eq!(
        tun_rx_drain_budget_before_remote_payload(
            None,
            active_pressure,
            7,
            1_200,
            0,
            cfg,
            1_200,
            false,
        ),
        0,
        "missing context cannot prove lifecycle eligibility"
    );

    let terminal = SocketCloseSnapshot {
        tcp_state: TcpState::Closed,
        active: false,
        can_send: false,
        can_recv: false,
        may_send: false,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: tx_queue_credit_spend_threshold(cfg),
        recv_queue: 0,
    };
    assert_eq!(
        tun_rx_drain_budget_before_remote_payload(
            Some(&ctx),
            terminal,
            7,
            1_200,
            0,
            cfg,
            1_200,
            false,
        ),
        0,
        "terminal no-send socket must not pre-drain before rejecting late payload"
    );
}

#[test]
fn maintenance_tun_rx_drain_budget_requires_dirty_pressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let below_target = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_egress_target_threshold(cfg).saturating_sub(1),
        tx_queue_egress_target_threshold(cfg).saturating_sub(1),
    );
    let target_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_egress_target_threshold(cfg),
        tx_queue_egress_target_threshold(cfg),
    );
    let credit_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg),
        tx_queue_credit_spend_threshold(cfg),
    );
    let pending_with_low_tx = DownlinkPressureStats::new(cfg.high_bytes, cfg.high_bytes, 1, 1);
    let pending_with_flush_edge = DownlinkPressureStats::new(
        cfg.high_bytes,
        cfg.high_bytes,
        tx_queue_flush_threshold(cfg),
        tx_queue_flush_threshold(cfg),
    );
    let pending_tail_below_high_at_flush_edge = DownlinkPressureStats::new(
        cfg.low_bytes,
        cfg.low_bytes,
        tx_queue_flush_threshold(cfg),
        tx_queue_flush_threshold(cfg),
    );

    assert_eq!(
        tun_rx_drain_budget_for_dirty_pressure(false, false, credit_edge, 0, cfg, 1_200, false),
        0,
        "pressure without dirty downlink work should not scan TUN RX"
    );
    assert_eq!(
        tun_rx_drain_budget_for_dirty_pressure(true, false, below_target, 0, cfg, 1_200, false),
        0,
        "maintenance drain should stay off below the egress target"
    );
    assert_eq!(
        tun_rx_drain_budget_for_dirty_pressure(true, true, below_target, 0, cfg, 1_200, false),
        1,
        "deferred close drain should keep ACK/window service even below the pressure edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_dirty_pressure(true, false, target_edge, 0, cfg, 1_200, false),
        0,
        "maintenance drain should treat the egress target as soft pressure"
    );
    assert_eq!(
        tun_rx_drain_budget_for_dirty_pressure(true, false, credit_edge, 0, cfg, 1_200, false),
        1,
        "the old credit edge remains pressure-eligible"
    );
    assert_eq!(
        tun_rx_drain_budget_for_dirty_pressure(true, false, credit_edge, 9, cfg, 1_200, false),
        9,
        "explicit drain budget remains an operator override once pressure is eligible"
    );
    assert_eq!(
        tun_rx_drain_budget_for_dirty_pressure(
            true,
            false,
            pending_with_low_tx,
            0,
            cfg,
            1_200,
            false
        ),
        0,
        "pending-only pressure should not drain ACKs without egress queue pressure"
    );
    assert_eq!(
        tun_rx_drain_budget_for_dirty_pressure(
            true,
            false,
            pending_with_flush_edge,
            0,
            cfg,
            1_200,
            false
        ),
        1,
        "pending pressure plus flush-edge tx queue should maintain ACK drain"
    );
    assert_eq!(
        tun_rx_drain_budget_for_dirty_pressure(
            true,
            false,
            pending_tail_below_high_at_flush_edge,
            0,
            cfg,
            1_200,
            false
        ),
        1,
        "a dirty tail with pending below high but tx queue at the flush edge should keep ACK/window maintenance alive"
    );
}

#[test]
fn dirty_close_drain_tracks_pending_or_egress_tail() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(build_listener_socket(&ListenerSpec { local_port: 443 }));
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut dirty = HashSet::from([handle]);
    let mut ctxs = HashMap::from([(handle, SocketCtx::new(443))]);

    assert!(
        !has_dirty_close_drain(&dirty, &ctxs, cfg),
        "ordinary dirty handles should not trigger close-drain maintenance"
    );

    {
        let ctx = ctxs.get_mut(&handle).unwrap();
        ctx.pending_relay_close = Some(RelayClose {
            epoch: 1,
            direction: "local_to_remote",
            reason: "uplink_channel_closed",
        });
        ctx.downlink_pending = vec![1, 2, 3];
    }
    assert!(
        has_dirty_close_drain(&dirty, &ctxs, cfg),
        "pending deferred close should keep ACK/window maintenance active"
    );

    {
        let ctx = ctxs.get_mut(&handle).unwrap();
        ctx.downlink_pending.clear();
        ctx.pending_relay_close_last_egress_queue_bytes = cfg.low_bytes + 1;
    }
    assert!(
        has_dirty_close_drain(&dirty, &ctxs, cfg),
        "egress-only deferred close above low watermark should keep maintenance active"
    );

    ctxs.get_mut(&handle)
        .unwrap()
        .pending_relay_close_last_egress_queue_bytes = cfg.low_bytes;
    assert!(
        !has_dirty_close_drain(&dirty, &ctxs, cfg),
        "close-drain maintenance can stop once pending is empty and egress reaches low"
    );

    dirty.clear();
    assert!(
        !has_dirty_close_drain(&dirty, &ctxs, cfg),
        "non-dirty close state should not scan TUN RX"
    );
}

#[test]
fn recent_active_timer_drain_budget_requires_recent_downlink_work() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let healthy = DownlinkPressureStats::default();
    let credit_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg),
        tx_queue_credit_spend_threshold(cfg),
    );
    let pause_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_pause_threshold(cfg),
        tx_queue_pause_threshold(cfg),
    );

    assert_eq!(
        tun_rx_drain_budget_for_recent_active_flow(
            false, 250, healthy, false, 0, cfg, 1_200, false
        ),
        0,
        "timer drain must stay off with no recent downlink work"
    );
    assert_eq!(
        tun_rx_drain_budget_for_recent_active_flow(true, 0, healthy, false, 0, cfg, 1_200, false),
        0,
        "operator override 0 should still disable the active-flow service lane"
    );
    assert_eq!(
        tun_rx_drain_budget_for_recent_active_flow(true, 250, healthy, false, 0, cfg, 1_200, false),
        active_flow_tun_rx_drain_budget(cfg, 1_200),
        "recent active downlink should get the bounded active-flow budget while egress is healthy"
    );
    assert_eq!(
        tun_rx_drain_budget_for_recent_active_flow(
            true,
            250,
            credit_edge,
            false,
            9,
            cfg,
            1_200,
            false
        ),
        1,
        "active-flow service lane must taper to one packet at the credit edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_recent_active_flow(
            true, 250, pause_edge, false, 9, cfg, 1_200, false
        ),
        0,
        "active-flow service lane must stop at the hard local egress pause edge"
    );
    assert_eq!(
        tun_rx_drain_budget_for_recent_active_flow(true, 250, healthy, true, 9, cfg, 1_200, false),
        0,
        "TUN egress feedback pause must suppress the active-flow service lane"
    );
    assert_eq!(
        tun_rx_drain_budget_for_recent_active_flow(true, 250, healthy, false, 9, cfg, 1_200, false),
        9,
        "explicit drain budget remains the healthy maximum inside the recent-active window"
    );
}

#[test]
fn tun_rx_pressure_drain_budget_is_ack_sized_and_capped() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 512 * 1024,
        low_bytes: 128 * 1024,
    };
    assert_eq!(tx_queue_credit_guard_bytes(cfg), 24 * 1024);
    assert_eq!(
        pressure_tun_rx_drain_budget(cfg, 1200),
        TUN_RX_PRESSURE_DRAIN_MAX_PACKETS,
        "default guard should be ACK-sized and capped, not MTU-sized"
    );
    assert_eq!(
        pressure_tun_rx_drain_budget(cfg, 1),
        TUN_RX_PRESSURE_DRAIN_MAX_PACKETS,
        "adaptive pressure drain remains bounded even with tiny MTU input"
    );
}

#[test]
fn tun_rx_active_flow_drain_budget_is_smaller_than_pressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 512 * 1024,
        low_bytes: 128 * 1024,
    };

    assert_eq!(
        pressure_tun_rx_drain_budget(cfg, 1200),
        TUN_RX_PRESSURE_DRAIN_MAX_PACKETS
    );
    assert_eq!(
        active_flow_tun_rx_drain_budget(cfg, 1200),
        TUN_RX_ACTIVE_FLOW_DRAIN_MAX_PACKETS,
        "active-flow drain should be useful but smaller than pressure drain"
    );
    assert!(active_flow_tun_rx_drain_budget(cfg, 1200) < pressure_tun_rx_drain_budget(cfg, 1200));
}

#[test]
fn downlink_backpressure_defaults_match_knife14ao_ab() {
    let default = DownlinkBackpressureConfig::default();
    assert_eq!(default.high_bytes, 512 * 1024);
    assert_eq!(default.low_bytes, 128 * 1024);
}

#[test]
fn downlink_flush_budget_caps_each_pass() {
    assert_eq!(
        bounded_downlink_flush_len(2_000_000, 262_144),
        262_144,
        "large pending should be split across multiple flush passes"
    );
    assert_eq!(
        bounded_downlink_flush_len(16_384, 262_144),
        16_384,
        "small pending should flush in one pass"
    );
    assert_eq!(
        bounded_downlink_flush_len(16_384, 0),
        1,
        "defensive zero budget must still make bounded progress"
    );
}

#[test]
fn downlink_flush_len_respects_remaining_egress_headroom() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 64,
        send_queue: tx_queue_flush_threshold(cfg) - 2,
        recv_queue: 0,
    };

    assert_eq!(
        bounded_downlink_flush_len_for_window(64, 64, send_window, cfg),
        2,
        "flush should only accept the remaining clean tx_queue headroom"
    );
}

#[test]
fn auto_tx_queue_thresholds_fit_default_tun_egress_capacity() {
    let tun_mtu = 1_200;
    let tun_tx_queue_len = DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE;
    let cfg = default_downlink_backpressure_for_tun_egress(tun_mtu, tun_tx_queue_len);

    assert!(
        cfg.high_bytes < DownlinkBackpressureConfig::default().high_bytes,
        "1200-byte MTU with Linux default qlen must cap the old fixed high watermark"
    );
    assert!(
        tx_queue_flush_threshold(cfg) <= tun_egress_safe_flush_threshold(tun_mtu, tun_tx_queue_len),
        "clean flush edge must stay below the safe TUN qdisc fill level"
    );
    assert!(
        tx_queue_pause_threshold(cfg) <= tun_egress_capacity_bytes(tun_mtu, tun_tx_queue_len),
        "hard pause edge must fit inside the estimated TUN qdisc capacity"
    );
}

#[test]
fn close_drain_flush_limit_accepts_terminal_tail_inside_credit_guard() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 524_288,
        low_bytes: 131_072,
    };
    let mut clock = DownlinkEgressClock::default();
    let mut drop_debt = DownlinkEgressDropDebt::default();
    drop_debt.install(EgressCreditDebtSource::Pressure, 36_431);
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 1_048_576,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };
    let normal = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        56_911,
        262_144,
        send_window,
        cfg,
        &mut clock,
        &mut drop_debt,
    );

    assert_eq!(
        normal.len, 0,
        "ordinary downlink flush stays blocked at clean high while pressure debt is active"
    );

    let close_drain =
        close_drain_terminal_pending_flush_limit(56_911, 262_144, send_window, cfg, normal);
    assert_eq!(
        close_drain.len, 56_911,
        "terminal close-drain may accept the small tail so FIN can follow real data"
    );
    assert!(
        send_window.send_queue + close_drain.len <= tx_queue_credit_spend_threshold(cfg),
        "close-drain tail remains below the hard credit guard"
    );
}

#[test]
fn close_drain_large_tail_stays_below_egress_target() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 327_272,
        low_bytes: 81_818,
    };
    let mut clock = DownlinkEgressClock::default();
    let mut drop_debt = DownlinkEgressDropDebt::default();
    drop_debt.install(EgressCreditDebtSource::Pressure, 99_847);
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576,
        send_queue: 496_761,
        recv_queue: 0,
    };
    let normal = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        91_438,
        262_144,
        send_window,
        cfg,
        &mut clock,
        &mut drop_debt,
    );

    assert_eq!(
        normal.len, 0,
        "ordinary flush must not expand while pressure debt is still active"
    );

    let close_drain =
        close_drain_terminal_pending_flush_limit(91_438, 262_144, send_window, cfg, normal);
    assert_eq!(
        send_window.send_queue + close_drain.len,
        tx_queue_egress_target_threshold(cfg),
        "large close-tail drain must stop at the egress target instead of the old credit edge"
    );
    assert!(
        send_window.send_queue + close_drain.len < tx_queue_credit_spend_threshold(cfg),
        "Knife14ey failure shape must not be allowed to refill to the old credit edge"
    );
    assert!(
        close_drain.hard_edge_guard_deferred_bytes > 0,
        "target-limited close-drain should feed hard feedback into the controller"
    );
}

#[test]
fn downlink_flush_len_uses_observed_egress_drain_credit() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut clock = DownlinkEgressClock::default();
    let at_clean_cap = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };

    let first =
        bounded_downlink_flush_limit_for_window_with_clock(100, 100, at_clean_cap, cfg, &mut clock);
    assert_eq!(first.len, 0, "no observed drain means no extra credit");
    clock.note_flush_result(at_clean_cap.send_queue, 0, first);

    let after_drain = TcpSendWindowSnapshot {
        send_queue: 100,
        ..at_clean_cap
    };
    let second =
        bounded_downlink_flush_limit_for_window_with_clock(100, 100, after_drain, cfg, &mut clock);
    assert_eq!(
        after_drain.send_queue + second.len,
        tx_queue_credit_spend_threshold(cfg),
        "observed drain should grant one-shot credit up to the credit edge"
    );
    assert_eq!(second.drain_credit_granted_bytes, 30);
    assert_eq!(
        second.drain_credit_planned_bytes,
        tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
    );
    assert_eq!(
        second.hard_edge_guard_bytes,
        tx_queue_pause_threshold(cfg) - tx_queue_credit_spend_threshold(cfg)
    );
    assert_eq!(
        second.hard_edge_guard_deferred_bytes,
        tx_queue_pause_threshold(cfg) - tx_queue_credit_spend_threshold(cfg)
    );
    clock.note_flush_result(after_drain.send_queue, second.len, second);

    let at_hard_pause = TcpSendWindowSnapshot {
        send_queue: tx_queue_pause_threshold(cfg),
        ..after_drain
    };
    let third = bounded_downlink_flush_limit_for_window_with_clock(
        100,
        100,
        at_hard_pause,
        cfg,
        &mut clock,
    );
    assert_eq!(
        third.len, 0,
        "drain credit must not let a pass plan beyond the hard pause threshold"
    );
}

#[test]
fn downlink_flush_len_spends_clean_drain_credit_to_credit_edge() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };

    let limit =
        bounded_downlink_flush_limit_for_window_with_clock(100, 100, send_window, cfg, &mut clock);

    assert_eq!(
        send_window.send_queue + limit.len,
        tx_queue_credit_spend_threshold(cfg),
        "without active debt or drops, clean drain credit should spend up to the credit edge"
    );
    assert_eq!(
        limit.drain_credit_planned_bytes,
        tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
    );
}

#[test]
fn d3_actor_clean_headroom_policy_does_not_spend_drain_credit_to_credit_edge() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: 100,
        recv_queue: 0,
    };

    let mut legacy_clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut legacy_drop_debt = DownlinkEgressDropDebt::default();
    let legacy = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        100,
        100,
        send_window,
        cfg,
        &mut legacy_clock,
        &mut legacy_drop_debt,
        DownlinkFlushPolicy::LegacyCredit,
    );
    assert_eq!(
        send_window.send_queue + legacy.len,
        tx_queue_credit_spend_threshold(cfg)
    );
    assert!(legacy.drain_credit_planned_bytes > 0);

    let mut actor_clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut actor_drop_debt = DownlinkEgressDropDebt::default();
    let actor = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        100,
        100,
        send_window,
        cfg,
        &mut actor_clock,
        &mut actor_drop_debt,
        DownlinkFlushPolicy::CleanHeadroomOnly,
    );
    assert_eq!(
        send_window.send_queue + actor.len,
        tx_queue_flush_threshold(cfg),
        "D3 actor pacing should refill only clean TUN headroom"
    );
    assert_eq!(actor.drain_credit_planned_bytes, 0);
    assert!(
        actor.headroom_limited,
        "bytes above clean headroom should remain pending for a later owner cycle"
    );
}

#[test]
fn d3_actor_hybrid_policy_spends_recent_drain_credit_to_target_edge() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };
    let mut clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut drop_debt = DownlinkEgressDropDebt::default();

    let actor = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        100,
        100,
        send_window,
        cfg,
        &mut clock,
        &mut drop_debt,
        DownlinkFlushPolicy::ActorHybridCredit,
    );

    assert_eq!(
        send_window.send_queue + actor.len,
        tx_queue_egress_target_threshold(cfg),
        "D3 actor hybrid pacing should spend bounded drain credit only to the target edge"
    );
    assert_eq!(
        actor.drain_credit_planned_bytes,
        tx_queue_egress_target_threshold(cfg) - tx_queue_flush_threshold(cfg)
    );
    assert!(
        actor.headroom_limited,
        "bytes above the target edge should remain pending for later TUN drain"
    );
    assert_eq!(
        actor.hard_edge_guard_bytes,
        tx_queue_pause_threshold(cfg) - tx_queue_egress_target_threshold(cfg)
    );
}

#[test]
fn d3_actor_hybrid_policy_blocks_drain_credit_with_active_drop_debt() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };
    let mut clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut drop_debt = DownlinkEgressDropDebt::default();
    drop_debt.note_tun_drop(cfg);

    let actor = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        100,
        100,
        send_window,
        cfg,
        &mut clock,
        &mut drop_debt,
        DownlinkFlushPolicy::ActorHybridCredit,
    );

    assert_eq!(
        send_window.send_queue + actor.len,
        tx_queue_flush_threshold(cfg),
        "active drop debt should force D3 actor hybrid back to clean headroom"
    );
    assert_eq!(actor.drain_credit_planned_bytes, 0);
    assert!(
        actor.drop_credit_blocked_bytes > 0,
        "the stale actor credit should be reported as blocked by drop debt"
    );
}

#[test]
fn d3_actor_adaptive_policy_spends_recent_drain_credit_to_credit_edge() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };
    let mut clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut drop_debt = DownlinkEgressDropDebt::default();

    let actor = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        100,
        100,
        send_window,
        cfg,
        &mut clock,
        &mut drop_debt,
        DownlinkFlushPolicy::ActorAdaptiveCredit,
    );

    assert_eq!(
        send_window.send_queue + actor.len,
        tx_queue_credit_spend_threshold(cfg),
        "adaptive actor credit should restore the H10d credit-edge capacity band"
    );
    assert_eq!(
        actor.drain_credit_planned_bytes,
        tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
    );
    assert!(
        actor.headroom_limited,
        "bytes above the credit edge must remain pending for a later owner cycle"
    );
    assert_eq!(
        actor.hard_edge_guard_bytes,
        tx_queue_pause_threshold(cfg) - tx_queue_credit_spend_threshold(cfg)
    );
}

#[test]
fn d3_actor_adaptive_policy_blocks_drain_credit_with_active_drop_debt() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };
    let mut clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut drop_debt = DownlinkEgressDropDebt::default();
    drop_debt.note_tun_drop(cfg);

    let actor = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        100,
        100,
        send_window,
        cfg,
        &mut clock,
        &mut drop_debt,
        DownlinkFlushPolicy::ActorAdaptiveCredit,
    );

    assert_eq!(
        send_window.send_queue + actor.len,
        tx_queue_flush_threshold(cfg),
        "active drop debt must force adaptive actor credit back to clean headroom"
    );
    assert_eq!(actor.drain_credit_planned_bytes, 0);
    assert!(
        actor.drop_credit_blocked_bytes > 0,
        "drop debt should invalidate stale actor drain credit"
    );
}

#[test]
fn d3_actor_adaptive_policy_keeps_close_drain_expansion_disabled() {
    assert!(
        !DownlinkFlushPolicy::ActorAdaptiveCredit.allows_close_drain_expansion(),
        "adaptive actor credit must not restore legacy close-drain expansion"
    );
}

#[test]
fn d3_actor_runtime_flush_policy_uses_hybrid_target_edge() {
    let mut ctx = SocketCtx::new(443);
    assert_eq!(
        downlink_flush_policy_for_ctx(&ctx),
        DownlinkFlushPolicy::LegacyCredit
    );

    ctx.downlink_d3_actor_flow = true;
    assert_eq!(
        downlink_flush_policy_for_ctx(&ctx),
        DownlinkFlushPolicy::ActorHybridCredit,
        "H10d13 showed credit-edge admission can clear local headroom attribution while worsening TUIC read starvation; keep hybrid as the default runtime policy"
    );
}

#[test]
fn d3_actor_runtime_flush_policy_can_opt_into_adaptive_credit_edge() {
    let mut ctx = SocketCtx::new(443);
    ctx.downlink_d3_actor_flow = true;
    ctx.downlink_d3_actor_adaptive_credit = true;

    assert_eq!(
        downlink_flush_policy_for_ctx(&ctx),
        DownlinkFlushPolicy::ActorAdaptiveCredit,
        "H10d15 isolates ordered-chunk + credit-edge as an opt-in discriminator"
    );
}

#[test]
fn d3_actor_runtime_flush_policy_can_replay_legacy_credit_edge() {
    let mut ctx = SocketCtx::new(443);
    ctx.downlink_d3_actor_flow = true;
    ctx.downlink_d3_actor_adaptive_credit = true;
    ctx.downlink_d3_actor_legacy_credit = true;

    assert_eq!(
        downlink_flush_policy_for_ctx(&ctx),
        DownlinkFlushPolicy::LegacyCredit,
        "H10d25 legacy replay must override newer actor credit policies for A/B isolation"
    );
}

#[test]
fn d5_capacity_policy_overrides_d3_credit_policy_for_flow() {
    let mut ctx = SocketCtx::new(443);
    ctx.downlink_d3_actor_flow = true;
    ctx.downlink_d3_actor_adaptive_credit = true;
    ctx.downlink_d3_actor_legacy_credit = true;
    ctx.downlink_d5_capacity_backpressure = true;

    assert_eq!(
        downlink_flush_policy_for_ctx(&ctx),
        DownlinkFlushPolicy::ActorCapacityCredit,
        "D5 is the capacity-model policy under test and must override older D3 A/B credit modes"
    );
}

#[test]
fn d5_capacity_flush_uses_socket_capacity_but_stays_below_safe_egress_edge() {
    let cfg =
        default_downlink_backpressure_for_tun_egress(1_200, DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE);
    let h10d32_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: false,
        send_capacity: 1_048_576usize.saturating_sub(557_386),
        send_queue: 557_386,
        recv_queue: 0,
    };
    let mut adaptive_clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut adaptive_debt = DownlinkEgressDropDebt::default();
    let adaptive = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        256 * 1024,
        256 * 1024,
        h10d32_window,
        cfg,
        &mut adaptive_clock,
        &mut adaptive_debt,
        DownlinkFlushPolicy::ActorAdaptiveCredit,
    );

    assert_eq!(
        adaptive.len, 0,
        "H10d32 pressure sits above the old D3 adaptive credit edge, so old policy can zero-write"
    );

    let mut capacity_clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut capacity_debt = DownlinkEgressDropDebt::default();
    let capacity = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        256 * 1024,
        256 * 1024,
        h10d32_window,
        cfg,
        &mut capacity_clock,
        &mut capacity_debt,
        DownlinkFlushPolicy::ActorCapacityCredit,
    );

    assert_eq!(
        capacity.len, 0,
        "D7 Gate A showed capacity-only admission reaches the TUN drop edge; D5 must now use socket capacity only below the safe egress target"
    );

    let safe_window = TcpSendWindowSnapshot {
        send_queue: tx_queue_flush_threshold(cfg),
        send_capacity: MAX_TCP_SOCKET_BUFFER_BYTES,
        ..h10d32_window
    };
    let mut safe_clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut safe_debt = DownlinkEgressDropDebt::default();
    let safe_capacity = bounded_downlink_flush_limit_for_window_with_clock_drop_and_policy(
        256 * 1024,
        256 * 1024,
        safe_window,
        cfg,
        &mut safe_clock,
        &mut safe_debt,
        DownlinkFlushPolicy::ActorCapacityCredit,
    );

    assert_eq!(
        safe_window.send_queue + safe_capacity.len,
        tx_queue_egress_target_threshold(cfg),
        "D5 should use available socket capacity only up to the measured safe egress target"
    );
    assert!(
        safe_capacity.len <= safe_window.send_capacity,
        "D5 remains bounded by smoltcp send capacity"
    );
}

#[test]
fn d3_actor_hybrid_policy_allows_bounded_close_drain_expansion() {
    assert!(
        DownlinkFlushPolicy::ActorHybridCredit.allows_close_drain_expansion(),
        "D3 actor owns local egress, so remote EOF must still drain pending bytes before close"
    );
}

#[test]
fn downlink_drain_credit_keeps_guard_below_hard_pause_edge() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut clock = DownlinkEgressClock::default();
    let at_clean_cap = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };

    let first =
        bounded_downlink_flush_limit_for_window_with_clock(100, 100, at_clean_cap, cfg, &mut clock);
    clock.note_flush_result(at_clean_cap.send_queue, 0, first);

    let after_drain = TcpSendWindowSnapshot {
        send_queue: 100,
        ..at_clean_cap
    };
    let second =
        bounded_downlink_flush_limit_for_window_with_clock(100, 100, after_drain, cfg, &mut clock);

    assert_eq!(
        after_drain.send_queue + second.len,
        tx_queue_credit_spend_threshold(cfg),
        "credit should leave a guard below hard pause"
    );
    assert_eq!(
        after_drain.send_queue + second.len,
        tx_queue_credit_spend_threshold(cfg),
        "planned send queue may reach the credit edge without active debt"
    );
}

#[test]
fn downlink_credit_guard_never_reduces_clean_headroom() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 2,
        low_bytes: 1,
    };
    let mut clock = DownlinkEgressClock::default();
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 10,
        send_queue: 0,
        recv_queue: 0,
    };

    let limit =
        bounded_downlink_flush_limit_for_window_with_clock(10, 10, send_window, cfg, &mut clock);

    assert_eq!(
        tx_queue_credit_guard_bytes(cfg),
        0,
        "zero-width credit spans cannot reserve a guard below clean headroom"
    );
    assert_eq!(
        limit.len,
        tx_queue_flush_threshold(cfg),
        "clean headroom must remain usable even when credit span is tiny"
    );
    assert_eq!(limit.hard_edge_guard_deferred_bytes, 0);
}

#[test]
fn downlink_egress_clock_consumes_only_accepted_credit() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut clock = DownlinkEgressClock::default();
    let at_clean_cap = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };
    let first =
        bounded_downlink_flush_limit_for_window_with_clock(100, 100, at_clean_cap, cfg, &mut clock);
    clock.note_flush_result(at_clean_cap.send_queue, 0, first);

    let after_drain = TcpSendWindowSnapshot {
        send_queue: 100,
        ..at_clean_cap
    };
    let second =
        bounded_downlink_flush_limit_for_window_with_clock(100, 100, after_drain, cfg, &mut clock);
    assert_eq!(
        after_drain.send_queue + second.len,
        tx_queue_credit_spend_threshold(cfg)
    );
    assert_eq!(
        second.drain_credit_planned_bytes,
        tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
    );
    assert_eq!(
        clock.note_flush_result(after_drain.send_queue, 40, second),
        10,
        "only bytes accepted above clean headroom should consume credit"
    );

    let after_partial_accept = TcpSendWindowSnapshot {
        send_queue: 140,
        ..at_clean_cap
    };
    let third = bounded_downlink_flush_limit_for_window_with_clock(
        100,
        100,
        after_partial_accept,
        cfg,
        &mut clock,
    );
    assert_eq!(
        after_partial_accept.send_queue + third.len,
        tx_queue_credit_spend_threshold(cfg),
        "unused drain credit should remain available up to the credit edge"
    );
    assert_eq!(third.drain_credit_planned_bytes, third.len);
    assert_eq!(
        third.hard_edge_guard_deferred_bytes,
        tx_queue_pause_threshold(cfg) - tx_queue_credit_spend_threshold(cfg)
    );
}

#[test]
fn downlink_drop_credit_debt_blocks_existing_credit_after_tun_drop() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: 27,
        drop_debt_generation_seen: 0,
    };
    let mut drop_debt = DownlinkEgressDropDebt::default();
    assert_eq!(drop_debt.note_tun_drop(cfg), 30);

    let after_drop_drain = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: 100,
        recv_queue: 0,
    };
    let limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        100,
        100,
        after_drop_drain,
        cfg,
        &mut clock,
        &mut drop_debt,
    );

    assert_eq!(
        limit.len, 30,
        "drop debt must leave clean headroom usable but block extra credit"
    );
    assert_eq!(limit.clean_headroom_bytes, 30);
    assert_eq!(limit.drain_credit_granted_bytes, 0);
    assert_eq!(limit.drain_credit_planned_bytes, 0);
    assert_eq!(limit.drop_credit_debt_paid_bytes, 30);
    assert_eq!(limit.drop_credit_debt_bytes, 0);
    assert_eq!(
        limit.drop_credit_blocked_bytes, 57,
        "stale credit plus drop-paid drain should be visible as blocked credit"
    );
}

#[test]
fn downlink_drop_credit_debt_must_be_paid_before_new_credit() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_pause_threshold(cfg)),
        drain_credit_bytes: 0,
        drop_debt_generation_seen: 0,
    };
    let mut drop_debt = DownlinkEgressDropDebt::default();
    drop_debt.note_tun_drop(cfg);

    let debt_payment_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_flush_threshold(cfg),
        recv_queue: 0,
    };
    let debt_payment = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        100,
        100,
        debt_payment_window,
        cfg,
        &mut clock,
        &mut drop_debt,
    );
    assert_eq!(debt_payment.drop_credit_debt_paid_bytes, 30);
    assert_eq!(debt_payment.drain_credit_granted_bytes, 0);
    assert_eq!(debt_payment.len, 0);
    clock.note_flush_result(debt_payment_window.send_queue, 0, debt_payment);

    let clean_drain_window = TcpSendWindowSnapshot {
        send_queue: 100,
        ..debt_payment_window
    };
    let clean_drain = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        100,
        100,
        clean_drain_window,
        cfg,
        &mut clock,
        &mut drop_debt,
    );
    assert_eq!(
        clean_drain.drain_credit_granted_bytes, 30,
        "only drain observed after drop debt is paid can become credit"
    );
    assert_eq!(clean_drain.drop_credit_debt_paid_bytes, 0);
    assert_eq!(
        clean_drain.drain_credit_planned_bytes,
        tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
    );
    assert_eq!(
        clean_drain_window.send_queue + clean_drain.len,
        tx_queue_credit_spend_threshold(cfg)
    );
}

#[test]
fn downlink_pressure_credit_debt_treats_single_target_edge_as_soft_pressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let clean_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_flush_threshold(cfg),
        tx_queue_flush_threshold(cfg),
    );
    let credit_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg),
        tx_queue_credit_spend_threshold(cfg),
    );
    let target_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_egress_target_threshold(cfg),
        tx_queue_egress_target_threshold(cfg),
    );
    let pending_with_low_tx_queue = DownlinkPressureStats::new(100, 100, 1, 1);
    let pending_with_flush_edge = DownlinkPressureStats::new(
        100,
        100,
        tx_queue_flush_threshold(cfg),
        tx_queue_flush_threshold(cfg),
    );

    assert!(!should_install_downlink_pressure_credit_debt(
        false, true, clean_edge, cfg
    ));
    assert!(
        !should_install_downlink_pressure_credit_debt(false, true, target_edge, cfg),
        "a single target-edge sample should not install pressure debt without stronger pressure evidence"
    );
    assert!(should_install_downlink_pressure_credit_debt(
        false,
        true,
        credit_edge,
        cfg
    ));
    assert!(!should_install_downlink_pressure_credit_debt(
        false,
        true,
        pending_with_low_tx_queue,
        cfg
    ));
    assert!(should_install_downlink_pressure_credit_debt(
        false,
        true,
        pending_with_flush_edge,
        cfg
    ));
    assert!(
        !should_install_downlink_pressure_credit_debt(true, true, credit_edge, cfg),
        "already-paused pressure should not keep installing new debt every loop"
    );
}

#[test]
fn d3_actor_runtime_skips_soft_pressure_credit_debt() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let credit_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg),
        tx_queue_credit_spend_threshold(cfg),
    );

    assert!(should_install_downlink_pressure_credit_debt_for_runtime(
        false,
        false,
        false,
        false,
        true,
        credit_edge,
        cfg
    ));
    assert!(
        !should_install_downlink_pressure_credit_debt_for_runtime(
            true,
            false,
            false,
            false,
            true,
            credit_edge,
            cfg
        ),
        "D3 actor path must not reinstall legacy soft pressure debt without a real TUN drop"
    );
    assert!(
        !should_install_downlink_pressure_credit_debt_for_runtime(
            false,
            false,
            true,
            false,
            true,
            credit_edge,
            cfg
        ),
        "buffered downlink keeps its existing pressure ownership"
    );
    assert!(
        should_install_downlink_pressure_credit_debt_for_runtime(
            true,
            true,
            false,
            false,
            true,
            credit_edge,
            cfg
        ),
        "legacy replay must restore the pre-H10d2 soft pressure-debt behavior for D3 A/B"
    );
}

#[test]
fn downlink_pressure_credit_debt_waits_for_credit_edge_after_target() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let target_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_egress_target_threshold(cfg),
        tx_queue_egress_target_threshold(cfg),
    );

    assert!(
        !should_install_downlink_pressure_credit_debt(false, false, target_edge, cfg),
        "target-edge pressure is a soft warning; stronger pressure should install debt"
    );
}

#[test]
fn post_flush_projected_pressure_debt_ignores_fully_accepted_transient_payload() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let post_flush_send_queue = tx_queue_pause_threshold(cfg).saturating_add(1);

    assert_eq!(
        post_flush_projected_pressure_debt_bytes(post_flush_send_queue, 0, 64 * 1024, cfg),
        0,
        "payload bytes already accepted by smoltcp are real egress progress, not residual pending debt"
    );
}

#[test]
fn post_flush_projected_pressure_debt_installs_for_residual_pending_pressure() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let post_flush_send_queue = tx_queue_credit_spend_threshold(cfg);
    let pending_bytes = 17;
    let debt = post_flush_projected_pressure_debt_bytes(
        post_flush_send_queue,
        pending_bytes,
        64 * 1024,
        cfg,
    );

    assert!(
        debt > 0,
        "residual pending after a flush is still local pressure and must keep pressure debt protection"
    );
}

#[test]
fn downlink_pressure_credit_debt_repeated_credit_edge_is_idempotent() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let credit_edge = DownlinkPressureStats::new(
        0,
        0,
        tx_queue_credit_spend_threshold(cfg),
        tx_queue_credit_spend_threshold(cfg),
    );
    let mut credit_debt = DownlinkEgressCreditDebt::default();
    let expected_debt = downlink_egress_pressure_debt_bytes(credit_edge, cfg);

    assert_eq!(
        credit_debt.note_egress_pressure(credit_edge, cfg),
        expected_debt
    );
    assert_eq!(credit_debt.generation, 1);
    assert_eq!(credit_debt.pressure_events, 1);
    assert_eq!(credit_debt.note_egress_pressure(credit_edge, cfg), 0);
    assert_eq!(
        credit_debt.generation, 1,
        "same-sized pressure debt should not create a new generation"
    );
    assert_eq!(
        credit_debt.pressure_events, 1,
        "diagnostic pressure events count debt installs, not repeated observations"
    );
}

#[test]
fn downlink_pressure_credit_debt_sizes_from_guard_and_overshoot() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    assert_eq!(downlink_egress_credit_span(cfg), 30);
    assert_eq!(tx_queue_credit_guard_bytes(cfg), 3);
    assert_eq!(tx_queue_egress_target_threshold(cfg), 143);
    assert_eq!(tx_queue_credit_spend_threshold(cfg), 157);

    assert_eq!(
        downlink_egress_pressure_debt_bytes(
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_egress_target_threshold(cfg) - 1,
                tx_queue_egress_target_threshold(cfg) - 1,
            ),
            cfg,
        ),
        0,
        "below the egress target should not install pressure debt"
    );
    assert_eq!(
        downlink_egress_pressure_debt_bytes(
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_egress_target_threshold(cfg),
                tx_queue_egress_target_threshold(cfg),
            ),
            cfg,
        ),
        0,
        "the egress target is a soft warning and does not install pressure debt"
    );
    assert_eq!(
        downlink_egress_pressure_debt_bytes(
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_credit_spend_threshold(cfg),
                tx_queue_credit_spend_threshold(cfg),
            ),
            cfg,
        ),
        17,
        "the old credit edge now counts as target overshoot"
    );
    assert_eq!(
        downlink_egress_pressure_debt_bytes(
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_pause_threshold(cfg) + 5,
                tx_queue_pause_threshold(cfg) + 5,
            ),
            cfg,
        ),
        25,
        "pressure overshoot should scale debt above the guard"
    );
    assert_eq!(
        downlink_egress_pressure_debt_bytes(
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_pause_threshold(cfg) + 50,
                tx_queue_pause_threshold(cfg) + 50,
            ),
            cfg,
        ),
        30,
        "pressure debt remains bounded by the original full credit span"
    );
    assert_eq!(
        downlink_egress_pressure_debt_bytes(
            DownlinkPressureStats::new(
                110,
                110,
                tx_queue_flush_threshold(cfg),
                tx_queue_flush_threshold(cfg),
            ),
            cfg,
        ),
        13,
        "pending pressure at the flush edge should also scale the early debt"
    );
}

#[test]
fn projected_pressure_debt_sizes_from_combined_send_queue_and_pending() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };

    assert_eq!(
        downlink_egress_projected_pressure_debt_bytes(
            tx_queue_egress_target_threshold(cfg) - 1,
            cfg,
        ),
        0,
        "projected pressure below the egress target should not install debt"
    );
    assert_eq!(
        downlink_egress_projected_pressure_debt_bytes(tx_queue_egress_target_threshold(cfg), cfg,),
        0,
        "the projected target edge is a soft warning and does not install pressure debt"
    );
    assert_eq!(
        downlink_egress_projected_pressure_debt_bytes(tx_queue_credit_spend_threshold(cfg), cfg,),
        tx_queue_credit_guard_bytes(cfg).saturating_add(
            tx_queue_credit_spend_threshold(cfg)
                .saturating_sub(tx_queue_egress_target_threshold(cfg))
        ),
        "the projected credit edge installs bounded pressure debt"
    );
    assert_eq!(
        downlink_egress_projected_pressure_debt_bytes(
            tx_queue_pause_threshold(cfg).saturating_add(50),
            cfg,
        ),
        downlink_egress_credit_span(cfg),
        "projected pressure debt remains bounded by the credit span"
    );
}

#[test]
fn projected_payload_target_edge_does_not_block_stale_drain_credit_before_flush() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut credit_debt = DownlinkEgressCreditDebt::default();
    let send_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_flush_threshold(cfg) - 1,
        recv_queue: 0,
    };
    let pending_after_payload =
        tx_queue_egress_target_threshold(cfg).saturating_sub(send_window.send_queue);
    let projected_pressure =
        downlink_projected_local_pressure_bytes(send_window.send_queue, pending_after_payload);

    assert_eq!(
        credit_debt.note_projected_egress_pressure(projected_pressure, cfg),
        0
    );
    let limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        pending_after_payload,
        100,
        send_window,
        cfg,
        &mut clock,
        &mut credit_debt,
    );

    assert_eq!(
        limit.len,
        tx_queue_egress_target_threshold(cfg).saturating_sub(send_window.send_queue),
        "target-edge projected pressure may still use drain credit up to the current hard target"
    );
    assert_eq!(limit.drain_credit_granted_bytes, 1);
    assert_eq!(
        limit.drain_credit_planned_bytes,
        tx_queue_egress_target_threshold(cfg).saturating_sub(tx_queue_flush_threshold(cfg))
    );
    assert_eq!(limit.pressure_credit_debt_bytes, 0);
}

#[test]
fn downlink_pressure_credit_debt_preserves_healthy_credit_before_tun_drop() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: 27,
        drop_debt_generation_seen: 0,
    };
    let mut credit_debt = DownlinkEgressCreditDebt::default();
    assert_eq!(
        credit_debt.note_egress_pressure(
            DownlinkPressureStats::new(
                0,
                0,
                tx_queue_credit_spend_threshold(cfg),
                tx_queue_credit_spend_threshold(cfg),
            ),
            cfg
        ),
        17
    );

    let after_pressure_drain = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: 100,
        recv_queue: 0,
    };
    let limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        100,
        100,
        after_pressure_drain,
        cfg,
        &mut clock,
        &mut credit_debt,
    );

    assert_eq!(limit.len, 57);
    assert_eq!(limit.clean_headroom_bytes, 30);
    assert_eq!(limit.drain_credit_granted_bytes, 0);
    assert_eq!(limit.drain_credit_planned_bytes, 27);
    assert_eq!(limit.pressure_credit_debt_paid_bytes, 17);
    assert_eq!(limit.pressure_credit_debt_bytes, 0);
    assert_eq!(
        limit.pressure_credit_blocked_bytes, 17,
        "the drain that pays pressure debt is still attributed, but existing healthy credit remains usable before an actual TUN drop"
    );
    assert_eq!(limit.drop_credit_blocked_bytes, 0);
}

#[test]
fn downlink_pressure_debt_keeps_healthy_drain_credit_usable() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut pressure_clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut pressure_debt = DownlinkEgressCreditDebt::default();
    pressure_debt.note_egress_pressure(
        DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg),
            tx_queue_credit_spend_threshold(cfg),
        ),
        cfg,
    );
    let pressure_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: tx_queue_egress_target_threshold(cfg) + 2,
        recv_queue: 0,
    };
    let pressure_limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        100,
        100,
        pressure_window,
        cfg,
        &mut pressure_clock,
        &mut pressure_debt,
    );

    assert_eq!(
        pressure_limit.len,
        tx_queue_credit_spend_threshold(cfg) - pressure_window.send_queue,
        "soft pressure debt must not turn healthy drain credit into a zero-write freeze"
    );
    assert_eq!(
        pressure_limit.drain_credit_planned_bytes,
        pressure_limit.len
    );
    assert_eq!(pressure_limit.drop_credit_blocked_bytes, 0);

    let mut drop_clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_flush_threshold(cfg)),
        drain_credit_bytes: downlink_egress_credit_span(cfg),
        drop_debt_generation_seen: 0,
    };
    let mut drop_debt = DownlinkEgressCreditDebt::default();
    drop_debt.note_tun_drop(cfg);
    let drop_limit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        100,
        100,
        pressure_window,
        cfg,
        &mut drop_clock,
        &mut drop_debt,
    );

    assert_eq!(
        drop_limit.len, 0,
        "real drop debt still blocks credit once the queue is already beyond the target edge"
    );
    assert_eq!(
        drop_limit.drop_credit_blocked_bytes,
        downlink_egress_credit_span(cfg)
    );
}

#[test]
fn pressure_debt_payment_with_progress_does_not_train_blocking_feedback() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let tun_mtu = 1200;
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let initial_ceiling = pressure_floor * 4;
    let initial_boost = pressure_floor;
    let mut controller = DownlinkCreditController {
        read_credit_ceiling_bytes: initial_ceiling,
        adaptive_ack_drain_bytes: initial_boost,
        ack_cadence_boost_bytes: initial_boost,
        egress_payload_credit_bytes: 0,
        egress_progress_generation: 0,
        headroom_debt_bytes: 0,
        no_egress_progress_streak: 0,
    };

    controller.note_flush_feedback(
        DownlinkFlushLimit {
            len: 30,
            headroom_limited: false,
            headroom_deferred_bytes: 0,
            clean_headroom_bytes: 30,
            drain_credit_granted_bytes: 0,
            drain_credit_planned_bytes: 0,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 17,
            pressure_credit_blocked_bytes: 17,
            hard_edge_guard_bytes: 0,
            hard_edge_guard_deferred_bytes: 0,
        },
        cfg,
        tun_mtu,
    );

    assert_eq!(controller.egress_progress_generation, 1);
    assert_eq!(
        controller.headroom_debt_bytes, 0,
        "soft pressure debt that was paid by observed egress progress is not deferred backlog"
    );
    assert!(
        controller.read_credit_ceiling_bytes >= initial_ceiling,
        "progress must not immediately train the read-credit ceiling downward"
    );
    assert_eq!(
        controller.ack_cadence_boost_bytes, initial_boost,
        "a paid soft-pressure sample must not clear the cadence boost as if it were a hard block"
    );
    assert_eq!(controller.no_egress_progress_streak, 0);
}

#[test]
fn hard_edge_guard_with_progress_does_not_train_stalled_read_credit() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let tun_mtu = 1200;
    let pressure_floor = relay_remote_read_pressure_floor_bytes(tun_mtu);
    let initial_ceiling = pressure_floor * 4;
    let initial_boost = pressure_floor;
    let mut controller = DownlinkCreditController {
        read_credit_ceiling_bytes: initial_ceiling,
        adaptive_ack_drain_bytes: initial_boost,
        ack_cadence_boost_bytes: initial_boost,
        egress_payload_credit_bytes: 0,
        egress_progress_generation: 0,
        headroom_debt_bytes: 0,
        no_egress_progress_streak: 0,
    };

    controller.note_flush_feedback(
        DownlinkFlushLimit {
            len: pressure_floor,
            headroom_limited: true,
            headroom_deferred_bytes: 0,
            clean_headroom_bytes: 0,
            drain_credit_granted_bytes: pressure_floor,
            drain_credit_planned_bytes: pressure_floor,
            drop_credit_debt_bytes: 0,
            drop_credit_debt_paid_bytes: 0,
            drop_credit_blocked_bytes: 0,
            pressure_credit_debt_bytes: 0,
            pressure_credit_debt_paid_bytes: 0,
            pressure_credit_blocked_bytes: 0,
            hard_edge_guard_bytes: 3,
            hard_edge_guard_deferred_bytes: pressure_floor,
        },
        cfg,
        tun_mtu,
    );

    assert_eq!(controller.egress_progress_generation, 1);
    assert_eq!(
        controller.headroom_debt_bytes, 0,
        "a bounded guard deferral with observed drain is not a stalled-egress backlog"
    );
    assert!(
        controller.read_credit_ceiling_bytes >= initial_ceiling,
        "the guard should keep local safety without training the TUIC read side downward while egress drains"
    );
    assert_eq!(controller.ack_cadence_boost_bytes, initial_boost);
    assert_eq!(controller.no_egress_progress_streak, 0);
}

#[test]
fn downlink_credit_debt_overpay_requires_later_clean_drain_for_new_credit() {
    let cfg = DownlinkBackpressureConfig {
        high_bytes: 100,
        low_bytes: 40,
    };
    let mut clock = DownlinkEgressClock {
        last_send_queue: Some(tx_queue_pause_threshold(cfg)),
        drain_credit_bytes: 0,
        drop_debt_generation_seen: 0,
    };
    let mut credit_debt = DownlinkEgressCreditDebt::default();
    credit_debt.note_egress_pressure(
        DownlinkPressureStats::new(
            0,
            0,
            tx_queue_credit_spend_threshold(cfg),
            tx_queue_credit_spend_threshold(cfg),
        ),
        cfg,
    );

    let large_drain_window = TcpSendWindowSnapshot {
        can_send: true,
        may_send: true,
        may_recv: true,
        send_capacity: 100,
        send_queue: 100,
        recv_queue: 0,
    };
    let debt_payment = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        100,
        100,
        large_drain_window,
        cfg,
        &mut clock,
        &mut credit_debt,
    );
    assert_eq!(debt_payment.pressure_credit_debt_paid_bytes, 17);
    assert_eq!(debt_payment.drain_credit_granted_bytes, 0);
    assert_eq!(
        debt_payment.len, 30,
        "the same observed drain that pays debt may use clean headroom only"
    );
    clock.note_flush_result(large_drain_window.send_queue, 30, debt_payment);

    let later_clean_drain = TcpSendWindowSnapshot {
        send_queue: 100,
        ..large_drain_window
    };
    let clean_credit = bounded_downlink_flush_limit_for_window_with_clock_and_drop(
        100,
        100,
        later_clean_drain,
        cfg,
        &mut clock,
        &mut credit_debt,
    );
    assert_eq!(clean_credit.drain_credit_granted_bytes, 30);
    assert_eq!(
        clean_credit.drain_credit_planned_bytes,
        tx_queue_credit_spend_threshold(cfg) - tx_queue_flush_threshold(cfg)
    );
    assert_eq!(
        later_clean_drain.send_queue + clean_credit.len,
        tx_queue_credit_spend_threshold(cfg)
    );
}

#[test]
fn parse_downlink_backpressure_config_defaults_and_rejects_bad_low() {
    let default = DownlinkBackpressureConfig::default();
    assert_eq!(
        parse_downlink_backpressure_config(None, None),
        default,
        "missing env uses defaults"
    );
    assert_eq!(
        parse_downlink_backpressure_config(Some("abc"), Some("0")),
        default,
        "invalid or zero values fall back to defaults"
    );

    let custom = parse_downlink_backpressure_config(Some("4096"), Some("1024"));
    assert_eq!(custom.high_bytes, 4096);
    assert_eq!(custom.low_bytes, 1024);

    let repaired = parse_downlink_backpressure_config(Some("4194304"), Some("4194304"));
    assert_eq!(repaired.high_bytes, 4194304);
    assert_eq!(repaired.low_bytes, default.low_bytes);
}

#[test]
fn parse_downlink_backpressure_config_caps_auto_defaults_to_tun_egress() {
    let auto = parse_downlink_backpressure_config_for_tx_buffer(
        None,
        None,
        1_048_576,
        1_200,
        DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE,
    );
    assert_eq!(
        auto,
        default_downlink_backpressure_for_tun_egress(1_200, DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE)
    );
    assert!(
        tx_queue_flush_threshold(auto)
            <= tun_egress_safe_flush_threshold(1_200, DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE)
    );
    assert!(
        tx_queue_pause_threshold(auto)
            <= tun_egress_capacity_bytes(1_200, DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE)
    );

    let legacy = parse_downlink_backpressure_config_for_tx_buffer(
        None,
        None,
        65_535,
        1_200,
        DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE,
    );
    assert_eq!(
        legacy, auto,
        "TCP tx buffer size must not drive auto defaults"
    );

    let explicit = parse_downlink_backpressure_config_for_tx_buffer(
        Some("4096"),
        Some("1024"),
        1_048_576,
        1_200,
        DEFAULT_TUN_TX_QUEUE_LEN_ESTIMATE,
    );
    assert_eq!(explicit.high_bytes, 4096);
    assert_eq!(explicit.low_bytes, 1024);
}

#[test]
fn parse_tcp_socket_buffer_config_defaults_and_bounds() {
    let default = TcpSocketBufferConfig::default();
    assert_eq!(parse_tcp_socket_buffer_config(None, None), default);
    assert_eq!(
        parse_tcp_socket_buffer_config(Some("abc"), Some("0")),
        default,
        "invalid and below-minimum values fall back to defaults"
    );

    let custom = parse_tcp_socket_buffer_config(Some("1048576"), Some("2097152"));
    assert_eq!(custom.rx_bytes, 1_048_576);
    assert_eq!(custom.tx_bytes, 2_097_152);

    let above_max = parse_tcp_socket_buffer_config(Some("33554432"), Some("33554432"));
    assert_eq!(
        above_max, default,
        "oversized socket buffers must not be accepted silently"
    );
}

#[test]
fn parse_downlink_flush_max_bytes_defaults_and_bounds() {
    assert_eq!(
        parse_downlink_flush_max_bytes(None),
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES
    );
    assert_eq!(
        parse_downlink_flush_max_bytes(Some("abc")),
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES
    );
    assert_eq!(
        parse_downlink_flush_max_bytes(Some("0")),
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES
    );
    assert_eq!(
        parse_downlink_flush_max_bytes(Some("1024")),
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        "below-minimum values should not silently underfeed the local path"
    );
    assert_eq!(parse_downlink_flush_max_bytes(Some("1048576")), 1_048_576);
    assert_eq!(
        parse_downlink_flush_max_bytes(Some("33554432")),
        DEFAULT_DOWNLINK_FLUSH_MAX_BYTES,
        "oversized values should not disable the burst bound by accident"
    );
}

#[test]
fn parse_downlink_egress_immediate_bytes_defaults_and_bounds() {
    assert_eq!(
        DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES, MAX_DOWNLINK_EGRESS_IMMEDIATE_BYTES,
        "Knife14ar default must preserve old immediate-flush behavior; small budgets are explicit A/B only"
    );
    assert_eq!(
        parse_downlink_egress_immediate_bytes(None),
        DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES
    );
    assert_eq!(
        parse_downlink_egress_immediate_bytes(Some("abc")),
        DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES
    );
    assert_eq!(
        parse_downlink_egress_immediate_bytes(Some("0")),
        0,
        "zero explicitly disables immediate remote-payload TUN flushes"
    );
    assert_eq!(parse_downlink_egress_immediate_bytes(Some("65536")), 65_536);
    assert_eq!(
        parse_downlink_egress_immediate_bytes(Some("33554432")),
        DEFAULT_DOWNLINK_EGRESS_IMMEDIATE_BYTES,
        "oversized values should not silently remove pacing"
    );
}

#[test]
fn listener_registry_uses_configured_socket_buffers() {
    let buffers = TcpSocketBufferConfig {
        rx_bytes: 8192,
        tx_bytes: 16384,
        receive_window_limit: None,
    };
    let mut registry = ListenerRegistry::with_socket_buffers(1, buffers);
    let mut sockets = SocketSet::new(vec![]);
    let mut ctxs = HashMap::new();

    registry
        .ensure_port(443, &mut sockets, &mut ctxs)
        .expect("port should be registered");
    let handle = registry.handles_for_port(443)[0];
    let socket = sockets.get::<TcpSocket>(handle);

    assert_eq!(socket.recv_capacity(), buffers.rx_bytes);
    assert_eq!(socket.send_capacity(), buffers.tx_bytes);
    assert_eq!(socket.receive_window_limit(), None);
}

#[test]
fn h10d16_listener_keeps_storage_and_limits_local_uplink_window() {
    let config = TunRuntimeConfig::h10d16_gate_a_for_test();
    assert_eq!(endpoint_window_v1_local_uplink_window_bytes(), 368_640);
    assert_eq!(config.tcp_socket_buffers.rx_bytes, 1024 * 1024);
    assert_eq!(
        config.tcp_socket_buffers.receive_window_limit,
        Some(368_640)
    );

    let socket = build_listener_socket_with_buffers(
        &ListenerSpec { local_port: 443 },
        config.tcp_socket_buffers,
    );
    assert_eq!(socket.recv_capacity(), 1024 * 1024);
    assert_eq!(socket.receive_window_limit(), Some(368_640));
}

#[test]
fn relay_task_diag_tracks_bytes_and_global_rx_pressure() {
    let mut diag = RelayTaskDiag::default();
    diag.note_uplink_write(40);
    diag.note_remote_read(100);
    diag.note_local_finish();
    diag.note_remote_read(28);
    diag.note_remote_batch(2, 128);
    diag.note_continuous_queue_wait(
        std::time::Duration::from_micros(40),
        std::time::Duration::from_micros(10),
    );
    diag.note_continuous_queue_wait(
        std::time::Duration::from_micros(3),
        std::time::Duration::from_micros(10),
    );
    diag.note_global_rx_wait(
        std::time::Duration::from_micros(25),
        std::time::Duration::from_micros(10),
    );
    diag.note_global_rx_wait(
        std::time::Duration::from_micros(5),
        std::time::Duration::from_micros(10),
    );
    diag.note_global_rx_queue(7, 1024);
    diag.note_global_rx_queue(3, 1024);
    diag.note_local_write_wait(
        std::time::Duration::from_micros(30),
        std::time::Duration::from_micros(10),
    );
    diag.note_local_write_wait(
        std::time::Duration::from_micros(7),
        std::time::Duration::from_micros(10),
    );

    assert_eq!(diag.uplink_bytes, 40);
    assert_eq!(diag.remote_to_global_rx_bytes, 128);
    assert_eq!(diag.remote_reads, 2);
    assert_eq!(diag.remote_batches, 1);
    assert_eq!(diag.remote_batch_bytes_max, 128);
    assert_eq!(diag.remote_batch_chunks_max, 2);
    assert_eq!(diag.continuous_queue_wait_max_micros, 40);
    assert_eq!(diag.continuous_queue_wait_events, 1);
    assert_eq!(diag.remote_after_local_finish_bytes, 28);
    assert_eq!(diag.remote_after_local_finish_reads, 1);
    assert_eq!(diag.global_rx_wait_max_micros, 25);
    assert_eq!(diag.global_rx_pressure_events, 1);
    assert_eq!(diag.global_rx_queue_used_max, 7);
    assert_eq!(diag.global_rx_queue_capacity, 1024);
    assert_eq!(diag.local_write_wait_max_micros, 30);
    assert_eq!(diag.local_write_pressure_events, 1);
}

#[test]
fn relay_live_diag_line_includes_directional_counters() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 16]),
        TcpSocketBuffer::new(vec![0; 16]),
    ));
    let mut diag = RelayTaskDiag::default();
    diag.note_uplink_write(64);
    diag.note_local_finish();
    diag.note_remote_read(128);
    diag.note_remote_read_service_tick(65_536);
    diag.note_remote_batch(1, 128);
    diag.note_continuous_queue_wait(
        std::time::Duration::from_micros(11),
        std::time::Duration::from_micros(10),
    );
    diag.note_global_rx_queue(5, 1024);
    diag.note_ack_drain_hint_due();
    diag.note_ack_drain_hint_sent();

    let line = format_relay_live_diag(handle, 7, true, true, &diag);

    assert!(line.contains("tcp-relay-live"), "{line}");
    assert!(line.contains("epoch=7"), "{line}");
    assert!(line.contains("writer_done=true"), "{line}");
    assert!(line.contains("read_only_after_local_finish=true"), "{line}");
    assert!(line.contains("uplink_bytes=64"), "{line}");
    assert!(line.contains("remote_to_global_rx_bytes=128"), "{line}");
    assert!(line.contains("remote_reads=1"), "{line}");
    assert!(line.contains("remote_read_service_ticks=1"), "{line}");
    assert!(line.contains("remote_read_service_len_min=65536"), "{line}");
    assert!(line.contains("remote_read_service_len_max=65536"), "{line}");
    assert!(line.contains("remote_batches=1"), "{line}");
    assert!(line.contains("remote_batch_bytes_max=128"), "{line}");
    assert!(line.contains("remote_batch_chunks_max=1"), "{line}");
    assert!(line.contains("continuous_queue_wait_max_us=11"), "{line}");
    assert!(line.contains("continuous_queue_wait_events=1"), "{line}");
    assert!(
        line.contains("remote_after_local_finish_bytes=128"),
        "{line}"
    );
    assert!(line.contains("remote_after_local_finish_reads=1"), "{line}");
    assert!(line.contains("ack_drain_hint_due=1"), "{line}");
    assert!(line.contains("ack_drain_hint_sent=1"), "{line}");
    assert!(line.contains("ack_drain_hint_dropped=0"), "{line}");
    assert!(line.contains("global_rx_queue_used_max=5"), "{line}");
    assert!(line.contains("global_rx_queue_capacity=1024"), "{line}");
}

#[test]
fn relay_read_service_diag_records_awaited_read_window_range() {
    let mut diag = RelayTaskDiag::default();

    diag.note_remote_read_service_tick(65_536);
    diag.note_remote_read_service_tick(0);
    diag.note_remote_read_service_tick(8_192);
    diag.note_remote_read_service_tick(131_072);

    assert_eq!(diag.remote_read_service_ticks, 3);
    assert_eq!(diag.remote_read_service_len_min, 8_192);
    assert_eq!(diag.remote_read_service_len_max, 131_072);
}

#[test]
fn stream_service_window_diag_line_includes_remote_and_local_progress() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 16]),
        TcpSocketBuffer::new(vec![0; 16]),
    ));
    let mut window = StreamServiceWindow::default();
    window.note_remote_poll(65_536);
    window.note_remote_read(65_536);
    window.note_local_admission(
        LocalAdmissionProgress {
            accepted_bytes: 32_768,
            egress_drain_bytes: 4096,
            tun_rx_packets: 2,
            flush_tx_calls: 1,
            dirty_passes: 1,
            ..LocalAdmissionProgress::default()
        },
        StreamServiceBlockedReason::None,
    );

    let line = format_stream_service_window_diag(handle, 9, &window);

    assert!(line.contains("tcp-stream-service-window"), "{line}");
    assert!(line.contains("epoch=9"), "{line}");
    assert!(line.contains("remote_poll_ticks=1"), "{line}");
    assert!(line.contains("remote_read_bytes=65536"), "{line}");
    assert!(line.contains("local_accepted_bytes=32768"), "{line}");
    assert!(line.contains("local_egress_drain_bytes=4096"), "{line}");
    assert!(line.contains("last_blocked_reason=none"), "{line}");
}

#[test]
fn relay_dispatch_segment_covers_one_local_egress_service_window() {
    const _: () = assert!(
        RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES >= LOCAL_EGRESS_SERVICE_TARGET_BYTES_PER_WINDOW
    );
    const _: () =
        assert!(RELAY_REMOTE_DISPATCH_SEGMENT_MAX_BYTES <= DEFAULT_DOWNLINK_FLUSH_MAX_BYTES);
}

#[test]
fn relay_live_diag_line_includes_remote_read_timing() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 16]),
        TcpSocketBuffer::new(vec![0; 16]),
    ));
    let start = std::time::Instant::now();
    let mut diag = RelayTaskDiag::new(start);
    diag.note_remote_read_at(100, start + std::time::Duration::from_millis(750));
    diag.note_remote_read_at(50, start + std::time::Duration::from_millis(2_250));

    let line = format_relay_live_diag_at(
        handle,
        7,
        false,
        false,
        &diag,
        start + std::time::Duration::from_millis(3_500),
    );

    assert!(line.contains("first_remote_read_ms=750"), "{line}");
    assert!(line.contains("max_remote_read_gap_ms=1500"), "{line}");
    assert!(line.contains("current_remote_read_gap_ms=1250"), "{line}");
}

#[test]
fn relay_live_diag_splits_remote_read_gaps_around_local_finish() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 16]),
        TcpSocketBuffer::new(vec![0; 16]),
    ));
    let start = std::time::Instant::now();
    let mut diag = RelayTaskDiag::new(start);
    diag.note_remote_read_at(10, start + std::time::Duration::from_millis(100));
    diag.note_remote_read_at(20, start + std::time::Duration::from_millis(900));
    diag.note_local_finish_at(start + std::time::Duration::from_millis(1_200));
    diag.note_remote_read_at(30, start + std::time::Duration::from_millis(2_700));
    diag.note_remote_read_at(40, start + std::time::Duration::from_millis(3_200));

    let line = format_relay_live_diag_at(
        handle,
        9,
        true,
        true,
        &diag,
        start + std::time::Duration::from_millis(3_700),
    );

    assert_eq!(diag.local_finish_events, 1);
    assert_eq!(
        diag.first_local_finish_after_first_remote_read_millis(),
        1_100
    );
    assert_eq!(diag.max_remote_read_gap_millis, 1_800);
    assert_eq!(diag.max_remote_read_gap_before_local_finish_millis, 800);
    assert_eq!(diag.max_remote_read_gap_after_local_finish_millis, 1_800);
    assert_eq!(diag.remote_after_local_finish_bytes, 70);
    assert_eq!(diag.remote_after_local_finish_reads, 2);
    assert!(line.contains("local_finish_events=1"), "{line}");
    assert!(
        line.contains("first_local_finish_after_first_remote_read_ms=1100"),
        "{line}"
    );
    assert!(
        line.contains("max_remote_read_gap_before_local_finish_ms=800"),
        "{line}"
    );
    assert!(
        line.contains("max_remote_read_gap_after_local_finish_ms=1800"),
        "{line}"
    );
}

#[test]
fn relay_ack_drain_hint_services_low_byte_ordered_stream_gap() {
    let start = std::time::Instant::now();
    let mut diag = RelayTaskDiag::new(start);
    let low_byte_progress = 60_704usize;

    assert!(
        (low_byte_progress as u64) < RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES,
        "fixture must stay below the historical 64KiB ACK/window service gate"
    );

    diag.note_remote_read_at(low_byte_progress, start);
    diag.note_remote_batch(5, low_byte_progress);
    diag.note_remote_read_service_tick(RELAY_REMOTE_READ_BURST_MAX_BYTES);

    let due = relay_ack_drain_hint_due(
        &diag,
        start + std::time::Duration::from_millis(RELAY_ACK_DRAIN_HINT_GAP_MS as u64 + 50),
        None,
    )
    .expect("active ordered-stream gaps below 64KiB should still request ACK/window service");

    assert_eq!(due.gap_ms, RELAY_ACK_DRAIN_HINT_GAP_MS + 50);
    assert_eq!(due.remote_to_global_rx_bytes, low_byte_progress as u64);
}

#[test]
fn relay_ack_drain_hint_polling_includes_low_byte_active_read_service() {
    let start = std::time::Instant::now();
    let mut diag = RelayTaskDiag::new(start);
    diag.note_remote_read_at(60_704, start);
    diag.note_remote_batch(5, 60_704);
    diag.note_remote_read_service_tick(RELAY_REMOTE_READ_BURST_MAX_BYTES);

    assert!(
        should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
        "payload-shaped active relays should keep the remote read probe armed"
    );
    assert!(
        should_poll_relay_ack_drain_hint(&diag, false),
        "low-byte active ordered stream gaps must reach the ACK/window hint branch"
    );
}

#[test]
fn relay_read_probe_services_native_stream_without_batch_accounting() {
    let start = std::time::Instant::now();
    let mut diag = RelayTaskDiag::new(start);
    diag.note_remote_read_at(RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES as usize, start);
    diag.note_remote_read_service_tick(RELAY_REMOTE_READ_BURST_MAX_BYTES);

    assert_eq!(
        diag.remote_batch_bytes_max, 0,
        "native chunk pump does not use relay batch accounting"
    );
    assert!(
        should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
        "data-bearing native streams must still arm read-gap probes"
    );
    assert!(
        should_poll_relay_ack_drain_hint(&diag, false),
        "native streams above the data threshold must reach ACK/window hints"
    );
}

#[test]
fn relay_ack_drain_hint_services_low_byte_native_raw_stream_gap() {
    let start = std::time::Instant::now();
    let mut diag = RelayTaskDiag::new(start);
    diag.note_remote_read_at(9_884, start);
    diag.note_remote_raw_read(58_386);
    diag.note_remote_read_service_tick(RELAY_REMOTE_READ_BURST_MAX_BYTES);

    assert_eq!(
        diag.remote_batch_bytes_max, 0,
        "native permit/chunk pump does not use ordered batch accounting"
    );
    assert!(
        diag.remote_to_global_rx_bytes < RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES,
        "fixture must stay below the historical 64KiB delivered-byte gate"
    );
    assert!(
        diag.remote_raw_read_bytes >= RELAY_RAW_READ_ACK_DRAIN_MIN_BYTES,
        "fixture must model native raw reader progress like D8 repeat2"
    );
    assert!(
        should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
        "native raw progress must keep the remote-read probe armed before 64KiB is delivered"
    );
    assert!(
        should_poll_relay_ack_drain_hint(&diag, false),
        "native raw progress must reach the bounded ACK/window hint path"
    );

    let due = relay_ack_drain_hint_due(
        &diag,
        start + std::time::Duration::from_millis(RELAY_ACK_DRAIN_HINT_GAP_MS as u64 + 50),
        None,
    )
    .expect("low-byte native raw stream gaps should request ACK/window service");
    assert_eq!(due.gap_ms, RELAY_ACK_DRAIN_HINT_GAP_MS + 50);
    assert_eq!(due.remote_to_global_rx_bytes, 9_884);
}

#[test]
fn relay_ack_drain_hint_ignores_tiny_native_raw_control_stream() {
    let start = std::time::Instant::now();
    let mut diag = RelayTaskDiag::new(start);
    diag.note_remote_read_at(1_412, start);
    diag.note_remote_raw_read(1_412);
    diag.note_remote_read_service_tick(RELAY_REMOTE_READ_BURST_MAX_BYTES);

    assert!(
        diag.remote_raw_read_bytes < RELAY_RAW_READ_ACK_DRAIN_MIN_BYTES,
        "fixture must remain below the native raw data-stream floor"
    );
    assert!(
        !should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
        "tiny native control streams must not arm periodic read probes"
    );
    assert!(
        !should_poll_relay_ack_drain_hint(&diag, false),
        "tiny native control streams must not scan TUN RX"
    );
}

#[test]
fn relay_ack_drain_hint_only_fires_for_rate_limited_remote_read_gap() {
    let start = std::time::Instant::now();
    let mut diag = RelayTaskDiag::new(start);
    let gap_due = start + std::time::Duration::from_millis(150);
    let mut last_hint_at = None;

    assert_eq!(
        relay_ack_drain_hint_due(&diag, gap_due, last_hint_at),
        None,
        "a relay with no remote data should not request ACK drain"
    );
    assert!(
        !should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
        "idle relays must not wake periodically before the first remote payload"
    );

    diag.note_remote_read_at(512, start);
    diag.note_remote_batch(1, 512);
    assert!(
        !should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
        "tiny control streams must not wake periodic remote-read probing"
    );
    assert!(
        !should_poll_relay_ack_drain_hint(&diag, false),
        "tiny control streams must not wake periodic ACK-drain hint polling"
    );
    assert_eq!(
        relay_ack_drain_hint_due(&diag, gap_due, last_hint_at),
        None,
        "tiny control-stream gaps should not scan TUN RX"
    );

    diag.note_remote_read_at(1412, start);
    diag.note_remote_batch(1, 1412);
    assert!(
        should_poll_relay_remote_read_probe(&diag, RelayReadCredit::default()),
        "payload-shaped streams should keep the remote read future polled before ACK hints are due"
    );
    assert!(
        should_poll_relay_remote_read_probe(
            &diag,
            RelayReadCredit {
                paused: false,
                max_batch_bytes: 1
            }
        ),
        "remote-read probing should respect the same non-paused credit gate as awaited reads"
    );
    assert!(
        !should_poll_relay_remote_read_probe(
            &diag,
            RelayReadCredit {
                paused: true,
                max_batch_bytes: RELAY_REMOTE_READ_BURST_MAX_BYTES
            }
        ),
        "paused receive credit must suppress probe wakeups"
    );
    assert!(
        !should_poll_relay_remote_read_probe(
            &diag,
            RelayReadCredit {
                paused: false,
                max_batch_bytes: 0
            }
        ),
        "zero receive credit must suppress probe wakeups"
    );
    assert!(
        !should_poll_relay_ack_drain_hint(&diag, false),
        "early payload-shaped streams should probe reads without emitting ACK-drain hints"
    );

    let remaining = (RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES - diag.remote_to_global_rx_bytes) as usize;
    diag.note_remote_read_at(remaining, start);
    diag.note_remote_batch(1, remaining);
    assert!(
        should_poll_relay_ack_drain_hint(&diag, false),
        "data-bearing relays may poll read-gap hints"
    );
    assert!(
        should_poll_relay_ack_drain_hint(&diag, true),
        "half-closed reverse streams still need ACK/window maintenance until remote EOF"
    );
    assert_eq!(
        relay_ack_drain_hint_due(
            &diag,
            start + std::time::Duration::from_millis(99),
            last_hint_at
        ),
        None,
        "remote read gaps below the short ACK/window service threshold should not scan TUN RX"
    );

    let due = relay_ack_drain_hint_due(&diag, gap_due, last_hint_at)
        .expect("short relay read gaps should request one ACK/window drain");
    assert_eq!(due.gap_ms, 150);
    assert_eq!(
        due.remote_to_global_rx_bytes,
        RELAY_ACK_DRAIN_HINT_MIN_DATA_BYTES
    );
    last_hint_at = Some(gap_due);

    assert_eq!(
        relay_ack_drain_hint_due(
            &diag,
            gap_due + std::time::Duration::from_millis(99),
            last_hint_at
        ),
        None,
        "hint emission must be rate-limited while the same stream remains stalled"
    );
    assert!(
        relay_ack_drain_hint_due(
            &diag,
            gap_due + std::time::Duration::from_millis(100),
            last_hint_at
        )
        .is_some(),
        "once the rate limit expires, a still-stalled stream may request another bounded drain"
    );
}

#[test]
fn relay_close_diag_line_includes_post_finish_remote_counters() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 16]),
        TcpSocketBuffer::new(vec![0; 16]),
    ));
    let mut diag = RelayTaskDiag::default();
    diag.note_uplink_write(37);
    diag.note_local_finish();
    diag.note_remote_read(43_772);
    diag.note_remote_read_service_tick(16_384);
    diag.note_remote_batch(3, 43_772);
    diag.note_continuous_queue_wait(
        std::time::Duration::from_micros(19),
        std::time::Duration::from_micros(10),
    );
    diag.note_global_rx_queue(9, 1024);

    let line = format_relay_close_diag(handle, "timer", "half_closed_idle_timeout", &diag);

    assert!(line.contains("tcp-relay-close"), "{line}");
    assert!(line.contains("direction=timer"), "{line}");
    assert!(line.contains("reason=half_closed_idle_timeout"), "{line}");
    assert!(
        line.contains("remote_after_local_finish_bytes=43772"),
        "{line}"
    );
    assert!(line.contains("remote_after_local_finish_reads=1"), "{line}");
    assert!(line.contains("remote_read_service_ticks=1"), "{line}");
    assert!(line.contains("remote_read_service_len_min=16384"), "{line}");
    assert!(line.contains("remote_read_service_len_max=16384"), "{line}");
    assert!(line.contains("remote_batches=1"), "{line}");
    assert!(line.contains("remote_batch_bytes_max=43772"), "{line}");
    assert!(line.contains("remote_batch_chunks_max=3"), "{line}");
    assert!(line.contains("continuous_queue_wait_max_us=19"), "{line}");
    assert!(line.contains("continuous_queue_wait_events=1"), "{line}");
    assert!(line.contains("first_remote_read_ms="), "{line}");
    assert!(line.contains("max_remote_read_gap_ms="), "{line}");
    assert!(line.contains("current_remote_read_gap_ms="), "{line}");
    assert!(line.contains("global_rx_queue_used_max=9"), "{line}");
    assert!(line.contains("global_rx_queue_capacity=1024"), "{line}");
}

#[test]
fn reverse_window_diag_line_reports_live_tcp_window_state() {
    let mut sockets = SocketSet::new(vec![]);
    let handle = sockets.add(TcpSocket::new(
        TcpSocketBuffer::new(vec![0; 16]),
        TcpSocketBuffer::new(vec![0; 16]),
    ));
    let snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: true,
        send_capacity: 1_048_576,
        send_queue: 65_536,
        recv_queue: 0,
    };

    let line = format_tcp_reverse_window_diag(
        handle,
        TcpReverseWindowDiag {
            ctx_state: SocketState::Relaying,
            payload_bytes: 65_536,
            accepted_bytes: 65_536,
            pending: 0,
            remote_to_global_rx_bytes: 4_194_304,
            local_fin_sent: false,
            snapshot,
        },
    );

    assert!(line.contains("tcp-reverse-window"), "{line}");
    assert!(line.contains("payload_bytes=65536"), "{line}");
    assert!(line.contains("accepted_bytes=65536"), "{line}");
    assert!(line.contains("pending=0"), "{line}");
    assert!(line.contains("remote_to_global_rx_bytes=4194304"), "{line}");
    assert!(line.contains("ctx_state=Relaying"), "{line}");
    assert!(line.contains("local_fin_sent=false"), "{line}");
    assert!(line.contains("tcp_state=Established"), "{line}");
    assert!(line.contains("active=true"), "{line}");
    assert!(line.contains("can_send=true"), "{line}");
    assert!(line.contains("may_recv=true"), "{line}");
    assert!(line.contains("send_capacity=1048576"), "{line}");
    assert!(line.contains("send_queue=65536"), "{line}");
}

#[test]
fn reverse_window_diag_rate_limiter_logs_first_and_interval() {
    let mut ctx = SocketCtx::new(5201);
    let snapshot = SocketCloseSnapshot {
        tcp_state: TcpState::Established,
        active: true,
        can_send: true,
        can_recv: false,
        may_send: true,
        may_recv: true,
        send_capacity: 1_048_576,
        send_queue: 0,
        recv_queue: 0,
    };

    assert!(
        should_log_tcp_reverse_window(&mut ctx, snapshot, 10),
        "first reverse payload should log the local TCP window"
    );
    assert!(
        !should_log_tcp_reverse_window(&mut ctx, snapshot, 14),
        "steady high-throughput reverse payloads should be rate-limited"
    );
    assert!(
        should_log_tcp_reverse_window(&mut ctx, snapshot, 15),
        "diagnostics should refresh after the bounded interval"
    );
}

/// 刀13 ①：MINI_VPN_TRACE 解析——`1`/`true`（去空白、不区分大小写）开；其它/缺省关
/// （默认关 = 主循环热路径零 stdout；翻 flag 恢复全部诊断打印）。
#[test]
fn parse_trace_only_truthy_enables() {
    assert!(parse_trace(Some("1")));
    assert!(parse_trace(Some("true")));
    assert!(parse_trace(Some(" TRUE ")));
    assert!(
        !parse_trace(Some("0")),
        "0 必须关——否则 MINI_VPN_TRACE=0 反而开是 footgun"
    );
    assert!(!parse_trace(Some("false")));
    assert!(!parse_trace(Some("yes")));
    assert!(!parse_trace(Some("")));
    assert!(!parse_trace(None), "缺省关——热路径默认静默");
}

#[test]
fn tcp_relay_engine_selector_keeps_continuous_opt_in_and_priority() {
    use TcpRelayEngine::*;
    assert_eq!(select_tcp_relay_engine(None, None, None, None), Legacy);
    assert_eq!(
        select_tcp_relay_engine(None, None, None, Some("1")),
        ThinStaging
    );
    assert_eq!(
        select_tcp_relay_engine(None, None, Some("1"), None),
        ContinuousPump
    );
    assert_eq!(
        select_tcp_relay_engine(None, None, Some("1"), Some("1")),
        ContinuousPump,
        "continuous pump must win when both experimental relay engines are enabled"
    );
    assert_eq!(
        select_tcp_relay_engine(None, Some("1"), Some("1"), Some("1")),
        PermitPump,
        "D2.3 permit pump must win over continuous/thin when enabled"
    );
    assert_eq!(
        select_tcp_relay_engine(Some("1"), Some("1"), Some("1"), Some("1")),
        OrderedEgressPermitPump,
        "D11 ordered egress permit pump must win when explicitly enabled"
    );
    assert_eq!(
        select_tcp_relay_engine(Some("false"), Some("false"), Some("false"), Some("true")),
        ThinStaging
    );
}

#[test]
fn native_tcp_relay_engine_selector_keeps_d6_opt_in() {
    use NativeTcpRelayEngine::*;
    for value in [None, Some("0")] {
        assert_eq!(select_native_tcp_relay_engine(value), ChunkPump);
    }
    assert_eq!(select_native_tcp_relay_engine(Some("1")), PermitPump);
    assert_eq!(PermitPump.as_str(), "native_permit_pump");
}

#[test]
fn tun_runtime_config_defaults_profile_loop_off() {
    // from_sources（harness/测试）恒关；只有 from_env 读 MINI_VPN_PROFILE_LOOP。
    let cfg = TunRuntimeConfig::from_sources(None).expect("valid config");
    assert!(!cfg.profile_loop);
}

#[test]
fn tun_runtime_config_rejects_zero_pool_size() {
    let err = TunRuntimeConfig::from_sources(Some("0")).expect_err("zero pool size should fail");
    assert!(err.to_string().contains("at least 1"));
}

#[test]
fn tun_runtime_config_accepts_pool_size_override() {
    let config = TunRuntimeConfig::from_sources(Some("3")).expect("valid config should load");
    assert_eq!(config.listener.pool_size, 3);
}

#[test]
fn tun_runtime_config_disables_buffered_downlink_by_default() {
    let config = TunRuntimeConfig::from_sources(Some("3")).expect("valid config should load");

    assert!(!config.buffered_downlink.enabled);
    assert_eq!(
        config.buffered_downlink.per_flow_low_bytes,
        config.downlink_backpressure.high_bytes
    );
    assert_eq!(
        config.buffered_downlink.global_hard_bytes,
        config
            .buffered_downlink
            .per_flow_hard_bytes
            .saturating_mul(3)
            .min(MAX_TCP_SOCKET_BUFFER_BYTES)
    );
}

fn udp_pkt(dst: [u8; 4], dst_port: u16) -> Vec<u8> {
    let mut v = Vec::new();
    etherparse::PacketBuilder::ipv4([10, 0, 0, 1], dst, 64)
        .udp(50000, dst_port)
        .write(&mut v, &[])
        .unwrap();
    v
}

/// 刀5：classify_inbound 把**任意** :53 路由到 Dns（裸包伪造），:853/:443 仍走 UdpRelay。
#[test]
fn classify_routes_dns_relay_and_other() {
    // 任意 resolver 的 :53 → Dns（不再只限 198.18.0.1）。
    assert_eq!(
        classify_inbound(&udp_pkt([198, 18, 0, 1], 53)),
        Inbound::Dns
    );
    assert_eq!(classify_inbound(&udp_pkt([8, 8, 8, 8], 53)), Inbound::Dns);
    assert_eq!(classify_inbound(&udp_pkt([1, 1, 1, 1], 53)), Inbound::Dns);
    // 其它 UDP（含 DoT/DoQ :853、DoH3/视频 :443）→ UdpRelay（刀4 Block 由 resolve_target 判）。
    assert_eq!(
        classify_inbound(&udp_pkt([198, 18, 0, 5], 443)),
        Inbound::UdpRelay
    );
    assert_eq!(
        classify_inbound(&udp_pkt([1, 1, 1, 1], 853)),
        Inbound::UdpRelay
    );
    // TCP SYN / 垃圾 → Other。
    let pkt = build_ipv4_tcp([10, 0, 0, 1], [1, 1, 1, 1], 60000, 443, true, false);
    assert_eq!(classify_inbound(&pkt), Inbound::Other);
    assert_eq!(classify_inbound(&[0u8; 4]), Inbound::Other);
}

/// 构造一个最小 DNS 查询(单 question, RD=1, QCLASS=IN)——刀5 forge 测试用。
fn dns_query(id: u16, qname: &str, qtype: u16) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&id.to_be_bytes());
    v.extend_from_slice(&0x0100u16.to_be_bytes()); // RD=1
    v.extend_from_slice(&1u16.to_be_bytes()); // QDCOUNT
    v.extend_from_slice(&[0u8; 6]); // AN/NS/AR COUNT = 0
    for label in qname.split('.') {
        v.push(label.len() as u8);
        v.extend_from_slice(label.as_bytes());
    }
    v.push(0);
    v.extend_from_slice(&qtype.to_be_bytes());
    v.extend_from_slice(&1u16.to_be_bytes()); // QCLASS IN
    v
}

/// 取一个伪造回包的 A 记录 RDATA（响应末 4 字节 = fake-IP）。
fn reply_rdata_ip(reply: &[u8]) -> Ipv4Addr {
    let g = parse_inbound_udp(reply).expect("回包应是合法 IPv4/UDP");
    let p = g.payload;
    Ipv4Addr::new(
        p[p.len() - 4],
        p[p.len() - 3],
        p[p.len() - 2],
        p[p.len() - 1],
    )
}

mod compatibility_lifecycle;
mod dns_characterization;
mod relay_reader_batch;

mod local_egress_service;

/// 刀11 T4：`handle_dns_hijack` 把 forge/drop 结局映射到 dns_forged/dns_dropped 计数。
#[tokio::test]
async fn handle_dns_hijack_counts_forge_and_drop() {
    let metrics = Metrics::new();
    let mut pool = FakeIpPool::new();
    let mut dev = DnsInjectRecorder::default();
    let app = (Ipv4Addr::new(10, 0, 0, 2), 50000);
    let res = (Ipv4Addr::new(8, 8, 8, 8), 53);

    // 可解析 A 查询 → forge++、注入一个回包。
    let q = build_udp_ip_packet(app, res, &dns_query(1, "example.com", dns::QTYPE_A))
        .expect("valid UDP packet");
    handle_dns_hijack(&q, &mut pool, &mut dev, 0, &metrics).await;
    let s = metrics.snapshot(0, 0);
    assert_eq!((s.dns_forged, s.dns_dropped), (1, 0));
    assert_eq!(dev.injected.len(), 1, "forge 应注入一个回包");

    // 不可解析 payload（截断）→ drop++、不注入。
    let bad = build_udp_ip_packet(app, res, &[0u8; 4]).expect("valid UDP packet");
    handle_dns_hijack(&bad, &mut pool, &mut dev, 0, &metrics).await;
    let s = metrics.snapshot(0, 0);
    assert_eq!((s.dns_forged, s.dns_dropped), (1, 1), "drop 不增 forge");
    assert_eq!(dev.injected.len(), 1, "drop 不注入");
}

/// 刀11 T5：`publish_gauges` 从 loop-owned 状态重算 gauge 并发布——
/// active_relays 只数 Relaying、fake-IP (total,active)、failover_leg 哨兵 → None。
#[test]
fn publish_gauges_samples_relaying_pool_and_leg() {
    use crate::metrics::{FailoverLegView, NO_FAILOVER};
    let mk = |s: SocketState| {
        let mut c = SocketCtx::new(0);
        c.state = s;
        c
    };
    let ctxs = [
        mk(SocketState::Relaying),
        mk(SocketState::Relaying),
        mk(SocketState::Listening),
        mk(SocketState::OpeningRemote), // 在飞、未成 relay → 不计 active
    ];
    let mut pool = FakeIpPool::new();
    let ip = pool.alloc("a.com", 0).expect("fake-IP capacity");
    pool.acquire(ip, 0); // 1 个有活跃 flow
    let _ = pool.alloc("b.com", 0).expect("fake-IP capacity"); // 在册未 acquire

    let metrics = Metrics::new();
    publish_gauges(&metrics, ctxs.iter(), &pool, 1); // leg=Reality
    let s = metrics.snapshot(0, 0);
    assert_eq!(s.active_relays, 2, "只数 state==Relaying");
    assert_eq!((s.fake_ip_total, s.fake_ip_active), (2, 1));
    assert_eq!(s.failover_leg, FailoverLegView::Reality);

    // 非 failover 单腿上游传哨兵 → snapshot 视图 None。
    publish_gauges(&metrics, ctxs.iter(), &pool, NO_FAILOVER);
    assert_eq!(metrics.snapshot(0, 0).failover_leg, FailoverLegView::None);
}

/// 刀4：resolve_target 对加密 DNS 端点返回 Block，且精确不误伤普通 :443 / 零回归 Refuse。
#[test]
fn resolve_target_blocks_encrypted_dns() {
    use smoltcp::wire::IpEndpoint;
    let mut pool = FakeIpPool::new();
    let ep =
        |ip: [u8; 4], port: u16| IpEndpoint::new(IpAddress::v4(ip[0], ip[1], ip[2], ip[3]), port);

    // DoT/DoQ :853（任意 IP）→ Block。
    assert!(matches!(
        resolve_target(ep([1, 1, 1, 1], 853), &pool),
        TargetResolve::Block
    ));
    assert!(matches!(
        resolve_target(ep([93, 184, 216, 34], 853), &pool),
        TargetResolve::Block
    ));

    // 刀5：TCP :53（明文 DNS over TCP，任意 IP）→ Block（RST，逼回落 UDP :53）。
    // 不变量：UDP :53 已被 classify_inbound 截到 Dns 路径、不到 resolve_target，故 port==53 只命中 TCP。
    assert!(matches!(
        resolve_target(ep([8, 8, 8, 8], 53), &pool),
        TargetResolve::Block
    ));
    assert!(matches!(
        resolve_target(ep([198, 18, 0, 1], 53), &pool),
        TargetResolve::Block
    ));

    // DoH 经 fake-IP：dns.google:443 → Block；普通域名:443 → Direct；DoH 域名但 :80 → Direct（仅 :443）。
    let doh_fake = pool.alloc("dns.google", 0).expect("fake-IP capacity");
    assert!(matches!(
        resolve_target(ep(doh_fake.octets(), 443), &pool),
        TargetResolve::Block
    ));
    assert!(matches!(
        resolve_target(ep(doh_fake.octets(), 80), &pool),
        TargetResolve::Direct { .. }
    ));
    let normal_fake = pool.alloc("example.com", 0).expect("fake-IP capacity");
    assert!(matches!(
        resolve_target(ep(normal_fake.octets(), 443), &pool),
        TargetResolve::Direct { .. }
    ));

    // DoH 硬编 IP 1.1.1.1:443 → Block；普通真实 IP:443 → Direct。
    assert!(matches!(
        resolve_target(ep([1, 1, 1, 1], 443), &pool),
        TargetResolve::Block
    ));
    assert!(matches!(
        resolve_target(ep([93, 184, 216, 34], 443), &pool),
        TargetResolve::Direct { .. }
    ));

    // fake-IP 段内无映射 → Refuse（零回归，不被 Block 吞掉）。
    assert!(matches!(
        resolve_target(IpEndpoint::new(IpAddress::v4(198, 18, 99, 99), 443), &pool),
        TargetResolve::Refuse
    ));
}

/// #1 脏集合驱动：`handles_for_port` 返回该端口 pool 的全部 handle；未注册端口空 slice。
#[test]
fn handles_for_port_returns_pool_handles() {
    let mut sockets = SocketSet::new(vec![]);
    let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
    let mut reg = ListenerRegistry::new(3);
    reg.ensure_port(443, &mut sockets, &mut ctxs).unwrap();
    assert_eq!(reg.handles_for_port(443).len(), 3);
    assert!(reg.handles_for_port(8080).is_empty());
}

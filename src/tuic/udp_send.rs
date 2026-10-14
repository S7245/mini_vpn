//! Owns the complete UDP uplink transport choice and per-packet stream settlement.
//! The stable `TuicUpstream::send_udp` entry remains transport-specific; this child does not
//! own connection recovery, downstream pumps, association routing, or TCP scheduling.
//! Each stream call scans at most four of 256 persistent custody slots, admits only established
//! 1-RTT, and makes one write poll: full Packet FIN, otherwise RESET. Weak retirement witnesses
//! keep FIN/RESET tails charged across generations until physical removal or completed destruction.
//! No application queue or task is created; native datagrams bypass this Authority. Tests live below.

use super::{
    Connection, MAX_TUIC_PACKET_BYTES, Ordering, TuicUpstream, UdpRelayMode, UdpSend, udp_send_plan,
};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

const STREAM_CUSTODY_SLOTS: usize = 256;
const STREAM_SCAN_LIMIT: usize = 4;

pub(super) struct UdpStreamSender {
    slots: Box<[Option<quinn::SendStreamRetirement>; STREAM_CUSTODY_SLOTS]>,
    cursor: usize,
}

impl Default for UdpStreamSender {
    fn default() -> Self {
        Self {
            slots: Box::new(std::array::from_fn(|_| None)),
            cursor: 0,
        }
    }
}

impl UdpStreamSender {
    /// Terminal control path only: preserve every unretired physical witness.
    pub(super) fn settle_all(&mut self) -> bool {
        for slot in self.slots.iter_mut() {
            if slot
                .as_ref()
                .is_some_and(quinn::SendStreamRetirement::is_retired)
            {
                *slot = None;
            }
        }
        self.slots.iter().all(Option::is_none)
    }
    fn write_packet(&mut self, conn: &Connection, packet: &[u8]) -> bool {
        let Some(slot) = self.available_slot() else {
            return false;
        };
        let Some((stream, retirement)) = conn.try_open_uni_1rtt_with_retirement() else {
            return false;
        };
        self.slots[slot] = Some(retirement);
        // No fallible work or suspension separates physical admission, custody, and this guard.
        let mut guard = ResetOnDrop {
            stream,
            finished: false,
        };
        let mut context = Context::from_waker(Waker::noop());
        if let Poll::Ready(Ok(written)) =
            Pin::new(&mut guard.stream).poll_write(&mut context, packet)
            && written == packet.len()
            && guard.stream.finish().is_ok()
        {
            guard.finished = true;
            return true;
        }
        // Drop explicitly RESETs every incomplete write, including panic unwinding.
        false
    }

    fn available_slot(&mut self) -> Option<usize> {
        for _ in 0..STREAM_SCAN_LIMIT {
            let slot = self.cursor;
            self.cursor = (self.cursor + 1) % STREAM_CUSTODY_SLOTS;
            if self.slots[slot]
                .as_ref()
                .is_none_or(quinn::SendStreamRetirement::is_retired)
            {
                self.slots[slot] = None;
                return Some(slot);
            }
        }
        None
    }
}

struct ResetOnDrop {
    stream: quinn::SendStream,
    finished: bool,
}

impl Drop for ResetOnDrop {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.stream.reset(0u32.into());
        }
    }
}

impl TuicUpstream {
    /// 取当前活连接克隆——**非阻塞、不重连**（刀9，ADR-0011 §3b）。锁被占（后台 start_udp 正在重连，持锁
    /// 数秒）→ `None`，**绝不 await 锁**（否则在主循环 inline 的 send_udp 会被 stall）。连接已死 → `None`。
    /// 重连交给后台 `start_udp` 自愈循环（背景退避重连），与主循环解耦。
    fn current_conn(&self) -> Option<Connection> {
        let guard = self.tcp_pool_slots.first()?.state.try_lock().ok()?;
        if guard.current.transport.close_reason().is_some() {
            None
        } else {
            Some(guard.current.transport.clone())
        }
    }

    /// 发一条**已编码**的 TUIC Packet（刀3：datagram 主路径 + uni-stream 兜底）。
    /// 中文要点：先按 `udp_send_plan(max_datagram_size, len)` 主动分流——装得下走 native datagram，
    /// 超上限/不可用走 per-packet uni-stream。datagram 的 `TooLarge` 竞态也转 stream。
    /// 连接不可用、stream credit 或写入预算不足时，本包计数丢弃；调用方绝不等待上游。
    pub async fn send_udp(&self, datagram: Vec<u8>) {
        if self.udp_runtime.node_stopping() || datagram.len() > MAX_TUIC_PACKET_BYTES {
            self.udp_drops.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // 记录 UDP 活跃时刻：驱动任务据此「仅活跃时发」Heartbeat（纯 TCP 不发，省流量/电量）。
        self.last_udp_activity
            .store(self.clock.elapsed().as_secs(), Ordering::Relaxed);
        // 刀9（真出口 acceptance 修，ADR-0011 §3b）：**绝不在此 inline 重连**——send_udp 在主循环 inline
        // await（handle_tuic_udp_uplink），黑洞期每个 UDP 包触发 5s 重连会 stall 整个事件循环、饿死 DNS 劫持
        // 等所有主循环分支。改用 `current_conn`（try_lock 非阻塞 + 不重连）：连接活就发；锁被占（后台 start_udp
        // 正在重连）或已死 → 快速静默丢（热路径勿刷屏，udp_drops 计数即观测）。重连由后台 start_udp 自愈循环负责。
        let conn = match self.current_conn() {
            Some(c) => c,
            None => {
                self.udp_drops.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        // Bytes：datagram 路径下 `clone()` 为 O(1)（Arc 引用计数），TooLarge 竞态二次兜底不深拷贝。
        let bytes = bytes::Bytes::from(datagram);
        // 按 config 的 relay mode 分流：Quic→恒 uni-stream（首包触发下行镜像 stream）；Native→size-based。
        match udp_send_plan(self.udp_relay_mode, conn.max_datagram_size(), bytes.len()) {
            UdpSend::Datagram => match conn.send_datagram(bytes.clone()) {
                Ok(()) => {}
                Err(quinn::SendDatagramError::TooLarge) => {
                    // MTU 竞态收缩：尝试 stream；无可用预算时按 UDP 压力丢弃。
                    self.udp_stream_fallbacks.fetch_add(1, Ordering::Relaxed);
                    self.send_udp_via_stream(&conn, bytes);
                }
                Err(_) => {
                    self.udp_drops.fetch_add(1, Ordering::Relaxed);
                }
            },
            UdpSend::Stream => {
                // Native 走到这里=超 datagram 上限的**兜底**（计数，acceptance 判 MTU 调优是否够）；
                // Quic 模式 stream 是**主发路径**（非兜底，不计 fallback，否则 `📊 stream 兜底` 会被
                // 误读为 MTU 失败——量级 = 全部包）。主发量看 path.sent_packets。
                if self.udp_relay_mode == UdpRelayMode::Native {
                    self.udp_stream_fallbacks.fetch_add(1, Ordering::Relaxed);
                }
                self.send_udp_via_stream(&conn, bytes);
            }
        }
    }

    /// 在 uni-stream 上发一条完整 TUIC Packet：开单向流 → 写整条已编码 Packet → finish。
    /// 中文要点：TUIC quic-relay-mode 即「一条 uni-stream 承载一个完整 Packet 命令」（`FRAG_TOTAL=1`），
    /// 字节与 datagram 模式完全一致。任一步不可立即完成→计数丢弃，不在热路径同步打印。
    /// fallback 计数由调用方负责（仅 Native 兜底场景计）。
    fn send_udp_via_stream(&self, conn: &Connection, datagram: bytes::Bytes) {
        let sent = self
            .udp_stream_sender
            .try_lock()
            .ok()
            .is_some_and(|mut sender| sender.write_packet(conn, &datagram));
        if !sent {
            self.udp_drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 累计丢弃的上行 UDP datagram 数（可观测性）。
    pub fn udp_drop_count(&self) -> u64 {
        self.udp_drops.load(Ordering::Relaxed)
    }

    /// 累计走 uni-stream 兜底的上行包数（可观测性：MTU 调优是否足够 / 兜底是否过热）。
    pub fn udp_stream_fallback_count(&self) -> u64 {
        self.udp_stream_fallbacks.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests;

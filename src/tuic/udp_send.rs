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

pub(super) mod encoding;

const STREAM_CUSTODY_SLOTS: usize = 256;
const STREAM_SCAN_LIMIT: usize = 4;
use bytes::{BackingBudgetReservation, BackingMeter, MeteredCopyError};
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Mutex, Weak};

type Panic = Box<dyn std::any::Any + Send>;
/// One physical table moves between DATA and the original Node retirement slot.
/// Busy owns no second table. Lost retains the original allocation and its charge.
pub(super) enum UdpStreamSender {
    Empty,
    Ready(Stock),
    Busy(Qualification),
    Disposing(Qualification),
    Broken(Qualification),
    Lost(Stock),
}
pub(super) struct Qualification {
    key: usize,
    meter: Option<BackingMeter>,
    owner: Option<Weak<super::node::StartupResources>>,
}
pub(super) struct Stock {
    table: ManuallyDrop<Box<Table>>,
    meter: Option<BackingMeter>,
    owner: Option<Weak<super::node::StartupResources>>,
}
pub(super) struct Table {
    slots: [Option<quinn::SendStreamRetirement>; STREAM_CUSTODY_SLOTS],
    allowance: ManuallyDrop<Option<BackingBudgetReservation>>,
    cursor: usize,
    // Each opaque value is installed before the first operation that can unwind.
    stream: ManuallyDrop<Option<quinn::SendStream>>,
    packet: ManuallyDrop<Option<bytes::Bytes>>,
    panic: ManuallyDrop<Option<Panic>>,
    failed: bool,
}
impl Table {
    fn empty() -> Self {
        Self {
            slots: std::array::from_fn(|_| None),
            allowance: ManuallyDrop::new(None),
            cursor: 0,
            stream: ManuallyDrop::new(None),
            packet: ManuallyDrop::new(None),
            panic: ManuallyDrop::new(None),
            failed: false,
        }
    }
}
impl Default for UdpStreamSender {
    fn default() -> Self {
        Self::Ready(Stock {
            table: ManuallyDrop::new(Box::new(Table::empty())),
            meter: None,
            owner: None,
        })
    }
}
impl UdpStreamSender {
    pub(super) fn prepare(
        owner: Option<&std::sync::Arc<super::node::StartupResources>>,
    ) -> Result<Self, MeteredCopyError> {
        let Some(owner) = owner else {
            return Ok(Self::default());
        };
        let (meter, _) = owner.udp_cold_context();
        if meter.budget_limit().is_none() {
            return Ok(Self::default());
        }
        let layout = std::alloc::Layout::new::<Table>();
        #[cfg(test)]
        tests::before_cold_prepare(&meter, layout.size());
        let allowance = meter.reserve_cold_layout(layout.size())?;
        let ptr = unsafe { std::alloc::alloc(layout).cast::<Table>() };
        if ptr.is_null() {
            return Err(MeteredCopyError::Allocation);
        }
        // Exact allocation, initialized completely before any publication.
        unsafe {
            ptr.write(Table::empty());
            (*ptr).allowance = ManuallyDrop::new(allowance);
        }
        Ok(Self::Ready(Stock {
            table: ManuallyDrop::new(unsafe { Box::from_raw(ptr) }),
            meter: Some(meter),
            owner: Some(std::sync::Arc::downgrade(owner)),
        }))
    }
    pub(super) fn empty_take(&mut self) -> Self {
        std::mem::replace(self, Self::Empty)
    }
    pub(super) fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
    pub(super) fn begin<F: FnMut(Option<Self>, usize) -> Returned>(
        &mut self,
        restore: F,
    ) -> Option<Turn<F>> {
        if !matches!(self, Self::Ready(_)) {
            return None;
        }
        let Self::Ready(stock) = std::mem::replace(self, Self::Empty) else {
            return None;
        };
        let key = stock.key();
        *self = Self::Busy(Qualification {
            key,
            meter: stock.meter.clone(),
            owner: stock.owner.clone(),
        });
        Some(Turn {
            stock: Some(stock),
            key,
            restore: Some(restore),
        })
    }
    /// Verify original qualification and actual Box identity, including final Empty.
    /// All effects and rejected physical values leave the lock before publication.
    pub(super) fn restore(&mut self, mut next: Self, key: usize, poisoned: bool) -> Returned {
        let qualified = matches!(self, Self::Busy(q) | Self::Disposing(q) | Self::Broken(q) if q.key == key)
            && next.key().is_none_or(|actual| actual == key);
        let failed = poisoned
            || !qualified
            || !matches!(self, Self::Busy(_) | Self::Disposing(_))
            || (next.is_empty() && !matches!(self, Self::Disposing(_)))
            || matches!(next, Self::Lost(_) | Self::Broken(_));
        let owners = [self.owner(), next.owner()];
        if failed {
            self.mark_lost();
            next.mark_lost();
        }
        let rejected = if qualified && !(failed && next.is_empty()) {
            *self = next;
            None
        } else {
            Some(next)
        };
        Returned {
            failed,
            owners,
            rejected,
        }
    }
    pub(super) fn authorize_disposal(&mut self, key: usize, poisoned: bool) -> Returned {
        let failed = poisoned || !matches!(self, Self::Busy(q) if q.key == key);
        let owners = [self.owner(), None];
        if failed {
            self.mark_lost();
        } else if let Self::Busy(q) = std::mem::replace(self, Self::Empty) {
            *self = Self::Disposing(q);
        }
        Returned {
            failed,
            owners,
            rejected: None,
        }
    }
    fn key(&self) -> Option<usize> {
        match self {
            Self::Ready(stock) | Self::Lost(stock) => Some(stock.key()),
            _ => None,
        }
    }
    fn owner(&self) -> Option<Weak<super::node::StartupResources>> {
        match self {
            Self::Ready(stock) | Self::Lost(stock) => stock.owner.clone(),
            Self::Busy(q) | Self::Disposing(q) | Self::Broken(q) => q.owner.clone(),
            Self::Empty => None,
        }
    }
    fn mark_lost(&mut self) {
        match self {
            Self::Ready(stock) | Self::Lost(stock) => {
                stock.table.failed = true;
                stock.mark();
            }
            Self::Busy(q) | Self::Disposing(q) | Self::Broken(q) => {
                if let Some(meter) = &q.meter {
                    meter.quarantine();
                }
            }
            Self::Empty => {}
        }
        if matches!(self, Self::Ready(_) | Self::Busy(_) | Self::Disposing(_)) {
            *self = match std::mem::replace(self, Self::Empty) {
                Self::Ready(stock) => Self::Lost(stock),
                Self::Busy(q) | Self::Disposing(q) => Self::Broken(q),
                other => other,
            };
        }
    }
    pub(super) fn settle_legacy(&mut self) -> bool {
        let Self::Ready(stock) = self else {
            return self.is_empty();
        };
        for slot in &mut stock.table.slots {
            if slot
                .as_ref()
                .is_some_and(quinn::SendStreamRetirement::is_retired)
            {
                *slot = None;
            }
        }
        stock.table.slots.iter().all(Option::is_none)
    }
    fn write_packet(&mut self, conn: &Connection, packet: &[u8]) -> bool {
        let Self::Ready(stock) = self else {
            return false;
        };
        stock.table.write_legacy(conn, packet)
    }
}
impl Drop for Stock {
    fn drop(&mut self) {
        if self.meter.is_some()
            && (!self.valid()
                || self.table.failed
                || self.table.stream.is_some()
                || self.table.panic.is_some()
                || self.table.packet.is_some()
                || self.table.slots.iter().any(Option::is_some))
        {
            self.mark(); // Outer turn/NodeHost publishes quiet outside all owner locks.
            return; // The real Box, every field and the allowance remain physically retained.
        }
        // No opaque field remains. Actual allocation destruction precedes its token refund.
        let allowance = self.table.allowance.take();
        unsafe {
            ManuallyDrop::drop(&mut self.table);
        }
        drop(allowance);
    }
}
impl Stock {
    fn key(&self) -> usize {
        (&**self.table as *const Table) as usize
    }
    fn mark(&self) {
        if let Some(meter) = &self.meter {
            meter.quarantine();
        }
    }
    fn quiet(&self) {
        self.mark();
        if let Some(owner) = self.owner.as_ref().and_then(Weak::upgrade) {
            owner.uplink_quarantine();
        }
    }
    fn attempt<T>(&mut self, f: impl FnOnce(&mut Table) -> T) -> Result<T, ()> {
        match catch_unwind(AssertUnwindSafe(|| f(&mut self.table))) {
            Ok(value) => Ok(value),
            Err(payload) => {
                self.table.failed = true;
                *self.table.panic = Some(payload); // First failure stops all further opaque operations.
                if let Some(owner) = self.owner.as_ref().and_then(Weak::upgrade) {
                    owner.uplink_panic();
                }
                self.quiet();
                Err(())
            }
        }
    }
    fn valid(&self) -> bool {
        self.meter.as_ref().is_none_or(|meter| {
            meter.cold_layout_matches(self.table.allowance.as_ref(), std::mem::size_of::<Table>())
        })
    }
    fn settle(&mut self) -> bool {
        if self.table.failed
            || !self.valid()
            || self.table.stream.is_some()
            || self.table.packet.is_some()
        {
            self.table.failed = true;
            self.quiet();
            return false;
        }
        for index in 0..STREAM_CUSTODY_SLOTS {
            let retired = self.attempt(|table| {
                table.slots[index]
                    .as_ref()
                    .is_some_and(quinn::SendStreamRetirement::is_retired)
            });
            match retired {
                Ok(true) => self.table.slots[index] = None,
                Ok(false) => {}
                Err(()) => return false,
            }
        }
        self.table.stream.is_none() && self.table.slots.iter().all(Option::is_none)
    }
    fn write_native(
        &mut self,
        conn: &Connection,
        packet: bytes::Bytes,
    ) -> Result<bool, MeteredCopyError> {
        if self.table.failed
            || !self.valid()
            || self.table.packet.is_some()
            || self.table.stream.is_some()
        {
            self.table.failed = true;
            self.quiet();
            return Err(MeteredCopyError::Quarantined);
        }
        if let Some(meter) = &self.meter {
            meter.admit_precharged()?;
        }
        *self.table.packet = Some(packet); // Before any opaque retirement query, open or write.
        let result = self.write_native_packet(conn);
        if !self.table.failed {
            drop(self.table.packet.take());
        }
        result
    }
    fn write_native_packet(&mut self, conn: &Connection) -> Result<bool, MeteredCopyError> {
        let packet_len = self.table.packet.as_ref().map_or(0, bytes::Bytes::len);
        let mut selected = None;
        for _ in 0..STREAM_SCAN_LIMIT {
            let slot = self.table.cursor;
            self.table.cursor = (slot + 1) % STREAM_CUSTODY_SLOTS;
            let retired = self
                .attempt(|table| {
                    table.slots[slot]
                        .as_ref()
                        .is_none_or(quinn::SendStreamRetirement::is_retired)
                })
                .map_err(|_| MeteredCopyError::Quarantined)?;
            if retired {
                self.table.slots[slot] = None;
                selected = Some(slot);
                break;
            }
        }
        let Some(slot) = selected else {
            return Ok(false);
        };
        let opened = self
            .attempt(|_| conn.try_open_uni_1rtt_with_retirement())
            .map_err(|_| MeteredCopyError::Quarantined)?;
        let Some((stream, retirement)) = opened else {
            return Ok(false);
        };
        self.table.slots[slot] = Some(retirement);
        *self.table.stream = Some(stream);
        let written = self
            .attempt(|table| {
                #[cfg(test)]
                tests::before_native_poll();
                table
                    .stream
                    .as_mut()
                    .zip(table.packet.as_ref())
                    .map(|(stream, packet)| {
                        Pin::new(stream).poll_write(&mut Context::from_waker(Waker::noop()), packet)
                    })
            })
            .map_err(|_| MeteredCopyError::Quarantined)?;
        let mut admission = match &written {
            Some(Poll::Ready(Err(quinn::WriteError::Admission(error)))) => Some(*error),
            _ => None,
        };
        let complete = if matches!(written, Some(Poll::Ready(Ok(n))) if n == packet_len) {
            let finished = self.attempt(|table| table.stream.as_mut().map(|stream| stream.finish()))
                .map_err(|_| MeteredCopyError::Quarantined)?;
            if let Some(Err(quinn::WriteError::Admission(error))) = &finished { admission = Some(*error); }
            matches!(finished, Some(Ok(())))
        } else { false };
        if admission == Some(MeteredCopyError::Quarantined) {
            self.table.failed = true;
            self.quiet();
            return Err(MeteredCopyError::Quarantined);
        }
        if !complete {
            self.attempt(|table| {
                if let Some(stream) = table.stream.as_mut() {
                    let _ = stream.reset(0u32.into());
                }
            })
            .map_err(|_| MeteredCopyError::Quarantined)?;
        }
        let stream = self.table.stream.take();
        // Actual Drop is a separate opaque site. Preserve a completed FIN outcome even if Drop fails.
        let _ = self.attempt(|_| drop(stream));
        admission.map_or(Ok(complete), Err)
    }
}
/// Rejected ownership travels out of the lock before quiet or any possible final Arc Drop.
/// Failed tables retain their actual Box, including the original reservation token.
pub(super) struct Returned {
    failed: bool,
    owners: [Option<Weak<super::node::StartupResources>>; 2],
    rejected: Option<UdpStreamSender>,
}
impl Returned {
    fn publish(self) -> bool {
        if self.failed {
            for owner in self.owners.iter().flatten().filter_map(Weak::upgrade) {
                owner.uplink_quarantine();
            }
        }
        drop(self.rejected);
        !self.failed
    }
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Retirement {
    Complete,
    Pending,
    Lost,
}
/// The only temporary qualification, shared by normal send and terminal retirement.
pub(super) struct Turn<F: FnMut(Option<UdpStreamSender>, usize) -> Returned> {
    stock: Option<Stock>,
    key: usize,
    restore: Option<F>,
}
impl<F: FnMut(Option<UdpStreamSender>, usize) -> Returned> Turn<F> {
    fn finish(mut self) -> bool {
        if let (Some(stock), Some(mut restore)) = (self.stock.take(), self.restore.take()) {
            return restore(
                Some(if stock.table.failed {
                    UdpStreamSender::Lost(stock)
                } else {
                    UdpStreamSender::Ready(stock)
                }),
                self.key,
            )
            .publish();
        }
        false
    }
    pub(super) fn retire(mut self) -> Retirement {
        let settled = self.stock.as_mut().is_some_and(Stock::settle);
        let failed = self.stock.as_ref().is_none_or(|stock| stock.table.failed);
        if settled {
            let prepared = self
                .restore
                .as_mut()
                .is_some_and(|restore| restore(None, self.key).publish());
            if !prepared {
                if let Some(stock) = self.stock.as_mut() {
                    stock.table.failed = true;
                }
                self.finish();
                return Retirement::Lost;
            }
            drop(self.stock.take()); // Actual Box, then allowance, outside the owner lock.
            if let Some(mut restore) = self.restore.take() {
                if restore(Some(UdpStreamSender::Empty), self.key).publish() {
                    return Retirement::Complete;
                }
            }
            Retirement::Lost
        } else {
            if !self.finish() || failed {
                Retirement::Lost
            } else {
                Retirement::Pending
            }
        }
    }
}
impl<F: FnMut(Option<UdpStreamSender>, usize) -> Returned> Drop for Turn<F> {
    fn drop(&mut self) {
        if let Some(mut stock) = self.stock.take() {
            stock.table.failed = true;
            stock.mark();
            let owner = stock.owner.clone();
            if let Some(mut restore) = self.restore.take() {
                restore(Some(UdpStreamSender::Lost(stock)), self.key).publish();
            }
            if let Some(owner) = owner.and_then(|owner| owner.upgrade()) {
                owner.uplink_quarantine();
            }
        }
    }
}
fn return_sender(
    owner: &Mutex<UdpStreamSender>,
    next: Option<UdpStreamSender>,
    key: usize,
) -> Returned {
    let (mut state, poisoned) = match owner.lock() {
        Ok(state) => (state, false),
        Err(poison) => (poison.into_inner(), true),
    };
    match next {
        Some(next) => state.restore(next, key, poisoned),
        None => state.authorize_disposal(key, poisoned),
    }
}
fn native_send(
    owner: &Mutex<UdpStreamSender>,
    conn: &Connection,
    packet: bytes::Bytes,
) -> Result<bool, (bool, MeteredCopyError)> {
    let turn = match owner.try_lock() {
        Ok(mut state) => state.begin(|next, key| return_sender(owner, next, key)),
        Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
        Err(std::sync::TryLockError::Poisoned(_)) => {
            return Err((false, MeteredCopyError::Quarantined));
        }
    };
    let Some(mut turn) = turn else {
        return Ok(false);
    };
    let result = turn
        .stock
        .as_mut()
        .map_or(Err(MeteredCopyError::Quarantined), |stock| {
            stock.write_native(conn, packet)
        });
    let settled = turn.finish();
    match result {
        Ok(sent) if settled => Ok(sent),
        Ok(sent) => Err((sent, MeteredCopyError::Quarantined)),
        Err(error) => Err((false, error)),
    }
}
impl Table {
    fn write_legacy(&mut self, conn: &Connection, packet: &[u8]) -> bool {
        let Some(slot) = self.available_slot() else {
            return false;
        };
        let Some((stream, retirement)) = conn.try_open_uni_1rtt_with_retirement() else {
            return false;
        };
        self.slots[slot] = Some(retirement);
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

/// Original Connection supplies the exact authority; caller retains error policy.
pub(super) fn send_heartbeat(conn: &Connection) -> Result<(), quinn::SendDatagramError> {
    let meter = conn.receive_backing_meter();
    let data = encoding::heartbeat(meter.as_ref()).map_err(|error| {
        quinn::SendDatagramError::Admission {
            data: bytes::Bytes::from_static(&[super::TUIC_VER, super::CMD_HEARTBEAT]),
            error,
        }
    })?;
    conn.send_datagram(data)
}

impl TuicUpstream {
    pub(super) async fn probe_tcp_pool_connection(
        &self,
        conn: &Connection,
        timeout: std::time::Duration,
    ) -> super::TcpPoolProbeOutcome {
        match super::probe_tcp_pool_connection(conn, timeout).await {
            Ok(outcome) => outcome,
            Err(error) => {
                self.udp_runtime.uplink_error(error);
                super::TcpPoolProbeOutcome::SendFailed
            }
        }
    }

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

    pub(super) fn send_packet(&self, association: u16, target: &super::TargetAddr, payload: &[u8]) {
        if self.udp_runtime.node_stopping() {
            self.udp_drops.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let bytes = match self.udp_runtime.uplink_meter() {
            Some(meter) => match encoding::native_packet(association, target, payload, meter) {
                Ok(bytes) => bytes,
                Err(error) => {
                    self.reject_udp(error);
                    return;
                }
            },
            None => encoding::encode_packet(association, target, payload).into(),
        };
        self.send_udp_bytes(bytes);
    }
    fn reject_udp(&self, error: bytes::MeteredCopyError) {
        self.udp_runtime.uplink_error(error);
        self.udp_drops.fetch_add(1, Ordering::Relaxed);
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
        let bytes = match self.udp_runtime.uplink_meter() {
            Some(meter) => match bytes::BytesMut::try_copy_from_slice_metered(&datagram, meter) {
                Ok(bytes) => bytes.freeze(),
                Err(error) => {
                    self.reject_udp(error);
                    return;
                }
            },
            None => datagram.into(),
        };
        self.send_udp_bytes(bytes);
    }
    fn send_udp_bytes(&self, bytes: bytes::Bytes) {
        if self.udp_runtime.node_stopping() || bytes.len() > MAX_TUIC_PACKET_BYTES {
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
        // 按 config 的 relay mode 分流：Quic→恒 uni-stream（首包触发下行镜像 stream）；Native→size-based。
        match udp_send_plan(self.udp_relay_mode, conn.max_datagram_size(), bytes.len()) {
            UdpSend::Datagram => match conn.send_datagram(bytes.clone()) {
                Ok(()) => {}
                Err(quinn::SendDatagramError::TooLarge) => {
                    // MTU 竞态收缩：尝试 stream；无可用预算时按 UDP 压力丢弃。
                    self.udp_stream_fallbacks.fetch_add(1, Ordering::Relaxed);
                    self.send_udp_via_stream(&conn, bytes);
                }
                Err(quinn::SendDatagramError::Admission { data, error }) => {
                    self.reject_udp(error);
                    drop(data);
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
        let sent = if self.udp_runtime.native_enabled() {
            match native_send(&self.udp_stream_sender, conn, datagram) {
                Ok(sent) => sent,
                Err((sent, error)) => {
                    self.udp_runtime.uplink_error(error);
                    sent
                }
            }
        } else {
            self.udp_stream_sender
                .try_lock()
                .ok()
                .is_some_and(|mut sender| sender.write_packet(conn, &datagram))
        };
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

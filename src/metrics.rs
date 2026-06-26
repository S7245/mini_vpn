//! 数据面可观测性（刀11）：进程级原子计数 + 周期快照契约。
//!
//! 中文要点：数据面是**两个 task** 各持状态（`run_event_loop` 单 task 独占 `socket_ctxs`/`fake_pool`；
//! `TuicUpstream::start_udp` spawn 的下行/统计 task 持 `conn` + udp 原子）。本模块的 [`Metrics`] 是
//! **唯一**能被两 task 各 clone 一份 `Arc<Metrics>` 无锁桥接的载体——它**不**扩 knife1 的 `MetricsSink`
//! （那是逐段**计时**接缝、生产 NoopSink 零开销，且只活在 loop task、看不见 udp 原子）。见 ADR-0012。
//!
//! 两类值：**累计 counter**（`fetch_add(Relaxed)` 于事件点）+ **发布式 gauge**（loop 30s tick 从单写者
//! 状态重算后 `store(Relaxed)`，因 socket_ctxs/fake_pool 无锁、不能跨 task 读）。[`snapshot`](Metrics::snapshot)
//! 一律 `load`、O(1)、任意 task 可调。

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};

/// failover 选腿哨兵：非 failover 上游（纯 TUIC / 纯 REALITY 单腿）无 `FailoverState`、无选腿概念。
pub const NO_FAILOVER: u8 = u8::MAX;

/// 进程级共享 metrics 句柄。两个数据面 task 各 clone 一份 `Arc<Metrics>`：
/// run_event_loop（写 `dns_*`/`relays_spawned` + 发布 gauge）与 TuicUpstream::start_udp
/// （写 `udp_drops_down`/`datagram_pressure_events`）。全字段原子、Relaxed、无锁。
///
/// **上行** `udp_drops_up`/`udp_stream_fallbacks` **不在此 struct**——它们仍是 `TuicUpstream` 的既有
/// `AtomicU64` 字段（零回归），快照时由 caller 经 upstream trait 访问器读出后传入 [`snapshot`](Self::snapshot)。
#[derive(Debug)]
pub struct Metrics {
    // ---- 累计 counter（事件点 fetch_add(Relaxed)）----
    dns_forged: AtomicU64,
    dns_dropped: AtomicU64,
    udp_drops_down: AtomicU64,
    datagram_pressure_events: AtomicU64,
    relays_spawned: AtomicU64,

    // ---- 发布式 gauge（loop 30s tick store；snapshot load）----
    active_relays: AtomicU32,
    fake_ip_active: AtomicU32,
    fake_ip_total: AtomicU32,
    /// 0=Tuic, 1=Reality, 255=[`NO_FAILOVER`]。
    failover_leg: AtomicU8,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// 全 0；`failover_leg` 初值 = [`NO_FAILOVER`]（避免误读为 Tuic）。
    pub fn new() -> Self {
        Self {
            dns_forged: AtomicU64::new(0),
            dns_dropped: AtomicU64::new(0),
            udp_drops_down: AtomicU64::new(0),
            datagram_pressure_events: AtomicU64::new(0),
            relays_spawned: AtomicU64::new(0),
            active_relays: AtomicU32::new(0),
            fake_ip_active: AtomicU32::new(0),
            fake_ip_total: AtomicU32::new(0),
            failover_leg: AtomicU8::new(NO_FAILOVER),
        }
    }

    // ---- counter 写点（热路径，Relaxed；与既有 TuicUpstream.udp_drops 同法）----

    /// DNS 查询成功伪造 fake-IP 回包（A 记录 + NODATA 都算，`Some=forge` 约定）。
    pub fn inc_dns_forged(&self) {
        self.dns_forged.fetch_add(1, Ordering::Relaxed);
    }
    /// 不可解析的 DNS 查询被丢（`forge_dns_reply` 返回 None）。
    pub fn inc_dns_dropped(&self) {
        self.dns_dropped.fetch_add(1, Ordering::Relaxed);
    }
    /// 下行 UDP datagram 丢弃（accept-uni 信号量耗尽 + `read_uni_packet` 解码/读失败）。
    /// 与上行 `udp_drops` 严格分离。
    pub fn inc_udp_drops_down(&self) {
        self.udp_drops_down.fetch_add(1, Ordering::Relaxed);
    }
    /// datagram 背压「集次」——false→true 上升沿计一次（非每 tick，见 [`note_pressure_edge`]）。
    pub fn inc_datagram_pressure_events(&self) {
        self.datagram_pressure_events.fetch_add(1, Ordering::Relaxed);
    }
    /// 累计 relay 启动次数（每条新 TCP flow 一次，`spawn_remote_relay`）。
    pub fn inc_relays_spawned(&self) {
        self.relays_spawned.fetch_add(1, Ordering::Relaxed);
    }

    // ---- gauge 发布点（仅 loop 30s tick 调，Relaxed）----

    /// 发布瞬时活跃 relay 数（`socket_ctxs` 中 `state==Relaying` 计数）。
    pub fn set_active_relays(&self, n: u32) {
        self.active_relays.store(n, Ordering::Relaxed);
    }
    /// 发布 fake-IP 池用量：`total`=在册映射数、`active`=refcount>0 的映射数。
    pub fn set_fake_ip(&self, total: u32, active: u32) {
        self.fake_ip_total.store(total, Ordering::Relaxed);
        self.fake_ip_active.store(active, Ordering::Relaxed);
    }
    /// 发布当前 TCP 选腿（`TcpLeg::as_u8` 或 [`NO_FAILOVER`]）。
    pub fn set_failover_leg(&self, leg_u8: u8) {
        self.failover_leg.store(leg_u8, Ordering::Relaxed);
    }

    /// 组装快照（全 `load(Relaxed)`，O(1)）。
    ///
    /// 上行计数 `udp_drops_up`/`udp_stream_fallbacks` 由 caller 经 upstream trait 访问器读出后传入——
    /// 它们仍住在 `TuicUpstream`（零回归），`Metrics` 不持其句柄（纯 TUIC/REALITY/failover 三态各异）。
    pub fn snapshot(&self, udp_drops_up: u64, udp_stream_fallbacks: u64) -> MetricsSnapshot {
        MetricsSnapshot {
            dns_forged: self.dns_forged.load(Ordering::Relaxed),
            dns_dropped: self.dns_dropped.load(Ordering::Relaxed),
            udp_drops_up,
            udp_drops_down: self.udp_drops_down.load(Ordering::Relaxed),
            udp_stream_fallbacks,
            datagram_pressure_events: self.datagram_pressure_events.load(Ordering::Relaxed),
            relays_spawned: self.relays_spawned.load(Ordering::Relaxed),
            active_relays: self.active_relays.load(Ordering::Relaxed),
            fake_ip_active: self.fake_ip_active.load(Ordering::Relaxed),
            fake_ip_total: self.fake_ip_total.load(Ordering::Relaxed),
            failover_leg: FailoverLegView::from_u8(self.failover_leg.load(Ordering::Relaxed)),
        }
    }
}

/// failover 选腿的快照视图（哨兵 u8 → 类型安全枚举，前端不需懂编码）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailoverLegView {
    Tuic,
    Reality,
    /// 非 failover 上游（纯 TUIC / 纯 REALITY 单腿）——无选腿概念。
    None,
}

impl FailoverLegView {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => FailoverLegView::Tuic,
            1 => FailoverLegView::Reality,
            _ => FailoverLegView::None,
        }
    }
    fn label(self) -> &'static str {
        match self {
            FailoverLegView::Tuic => "TUIC",
            FailoverLegView::Reality => "REALITY",
            FailoverLegView::None => "-",
        }
    }
}

/// 数据面可观测性快照（刀11 前端契约）。纯值、`Copy`、无原子无锁、pub 字段。
///
/// 前端读取通道（IPC/local-control）留前端 session（契约先行）；本刀只导出**结构与值**。
/// serde 派生留前端按需加——struct 形状即契约。测试可 snapshot-before/snapshot-after 断言 delta。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    // --- DNS forge 面 ---
    /// 累计成功伪造的 DNS 回包数（A + NODATA，`Some=forge` 约定）。
    pub dns_forged: u64,
    /// 累计丢弃的不可解析 DNS 查询数。
    pub dns_dropped: u64,

    // --- UDP datagram 面 ---
    /// 上行 datagram 累计丢弃（既有计数器，conflate no-conn/datagram-fail/uni-stream-fail）。
    pub udp_drops_up: u64,
    /// 下行 datagram 累计丢弃（accept-uni 溢出 + 解码失败；本刀新增，与上行分离）。
    pub udp_drops_down: u64,
    /// 上行走 uni-stream 兜底累计次数（既有计数器）。
    pub udp_stream_fallbacks: u64,
    /// datagram 背压上升沿「集次」累计（非 level，每集一次）。
    pub datagram_pressure_events: u64,

    // --- TCP relay 面 ---
    /// 累计 relay 启动次数（每条新 TCP flow）。
    pub relays_spawned: u64,
    /// 瞬时活跃 relay 数（`state==Relaying`，30s tick 采样的最新已发布值）。
    pub active_relays: u32,

    // --- fake-IP 池面 ---
    /// 瞬时活跃映射数（refcount>0，有活跃 flow）。
    pub fake_ip_active: u32,
    /// 瞬时在册映射总数。
    pub fake_ip_total: u32,

    // --- failover 面 ---
    /// 当前 TCP 选腿（`None`=非 failover 单腿模式）。
    pub failover_leg: FailoverLegView,
}

/// 周期 `📊` 快照行（纯 formatter，仿 `tuic::format_udp_stats`，可单测）。
pub fn format_metrics_snapshot(s: &MetricsSnapshot) -> String {
    format!(
        "📊 数据面: DNS forge={}/drop={} | TCP relay 活跃={}/累计={} | \
         fake-IP 活跃={}/在册={} | UDP↓丢={} 背压={} | UDP↑丢={} stream兜底={} | leg={}",
        s.dns_forged,
        s.dns_dropped,
        s.active_relays,
        s.relays_spawned,
        s.fake_ip_active,
        s.fake_ip_total,
        s.udp_drops_down,
        s.datagram_pressure_events,
        s.udp_drops_up,
        s.udp_stream_fallbacks,
        s.failover_leg.label(),
    )
}

/// 背压上升沿判定（纯 helper，供 start_udp 30s 采样调用 + 单测沿语义）。
///
/// `pressured` = 本次 `is_datagram_pressured` 采样值；`prev` = 上次采样（task-local latch）。
/// 返回 `true` 当且仅当 false→true 上升沿（此时调用方 `inc_datagram_pressure_events()`）；
/// 副作用：把 `prev` 更新为本次值。counts distinct episodes，避免一段持续背压每 tick 重复计数。
pub fn note_pressure_edge(pressured: bool, prev: &mut bool) -> bool {
    let rising = pressured && !*prev;
    *prev = pressured;
    rising
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn new_is_zero_and_leg_none() {
        let m = Metrics::new();
        let s = m.snapshot(0, 0);
        assert_eq!(s.dns_forged, 0);
        assert_eq!(s.dns_dropped, 0);
        assert_eq!(s.udp_drops_up, 0);
        assert_eq!(s.udp_drops_down, 0);
        assert_eq!(s.udp_stream_fallbacks, 0);
        assert_eq!(s.datagram_pressure_events, 0);
        assert_eq!(s.relays_spawned, 0);
        assert_eq!(s.active_relays, 0);
        assert_eq!(s.fake_ip_active, 0);
        assert_eq!(s.fake_ip_total, 0);
        assert_eq!(s.failover_leg, FailoverLegView::None);
    }

    #[test]
    fn counters_increment_into_snapshot() {
        let m = Metrics::new();
        m.inc_dns_forged();
        m.inc_dns_forged();
        m.inc_dns_dropped();
        m.inc_udp_drops_down();
        m.inc_datagram_pressure_events();
        m.inc_relays_spawned();
        let s = m.snapshot(7, 3);
        assert_eq!(s.dns_forged, 2);
        assert_eq!(s.dns_dropped, 1);
        assert_eq!(s.udp_drops_down, 1);
        assert_eq!(s.datagram_pressure_events, 1);
        assert_eq!(s.relays_spawned, 1);
        // 上行计数由 caller 传入、原样落字段。
        assert_eq!(s.udp_drops_up, 7);
        assert_eq!(s.udp_stream_fallbacks, 3);
    }

    #[test]
    fn gauges_publish_into_snapshot() {
        let m = Metrics::new();
        m.set_active_relays(5);
        m.set_fake_ip(42, 9);
        m.set_failover_leg(1);
        let s = m.snapshot(0, 0);
        assert_eq!(s.active_relays, 5);
        assert_eq!(s.fake_ip_total, 42);
        assert_eq!(s.fake_ip_active, 9);
        assert_eq!(s.failover_leg, FailoverLegView::Reality);
    }

    #[test]
    fn failover_leg_u8_mapping() {
        let m = Metrics::new();
        m.set_failover_leg(0);
        assert_eq!(m.snapshot(0, 0).failover_leg, FailoverLegView::Tuic);
        m.set_failover_leg(1);
        assert_eq!(m.snapshot(0, 0).failover_leg, FailoverLegView::Reality);
        m.set_failover_leg(NO_FAILOVER);
        assert_eq!(m.snapshot(0, 0).failover_leg, FailoverLegView::None);
        // 任意非法值 → None（防御，永不 panic / 误读）。
        m.set_failover_leg(200);
        assert_eq!(m.snapshot(0, 0).failover_leg, FailoverLegView::None);
    }

    #[test]
    fn concurrent_fetch_add_loses_nothing() {
        let m = Arc::new(Metrics::new());
        let n_threads: u64 = 8;
        let per: u64 = 10_000;
        let mut handles = Vec::new();
        for _ in 0..n_threads {
            let m = Arc::clone(&m);
            handles.push(std::thread::spawn(move || {
                for _ in 0..per {
                    m.inc_dns_forged();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.snapshot(0, 0).dns_forged, n_threads * per);
    }

    #[test]
    fn note_pressure_edge_counts_rising_only() {
        let mut prev = false;
        // 序列 [F,T,T,F,T] → 两个上升沿。
        let seq = [false, true, true, false, true];
        let count = seq.iter().filter(|&&p| note_pressure_edge(p, &mut prev)).count();
        assert_eq!(count, 2);
        // 全 false → 0 沿。
        let mut prev2 = false;
        let none = [false, false, false]
            .iter()
            .filter(|&&p| note_pressure_edge(p, &mut prev2))
            .count();
        assert_eq!(none, 0);
    }

    #[test]
    fn format_contains_all_fields() {
        let m = Metrics::new();
        m.inc_dns_forged();
        m.set_active_relays(3);
        m.set_failover_leg(1);
        let line = format_metrics_snapshot(&m.snapshot(11, 2));
        for needle in [
            "📊",
            "DNS forge=",
            "drop=",
            "活跃=",
            "累计=",
            "fake-IP",
            "UDP↓丢=",
            "背压=",
            "UDP↑丢=",
            "stream兜底=",
            "leg=REALITY",
        ] {
            assert!(line.contains(needle), "缺字段 {needle:?} in {line:?}");
        }
    }
}

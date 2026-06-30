//! 刀9：健康感知 TUIC↔REALITY auto-failover + 分离 TCP/UDP 上游（F2 + F1）。
//!
//! 中文要点：
//! - `FailoverUpstream<T, R>` 包装两条腿（T=TUIC，R=REALITY），impl `ProxyUpstream`（TCP relay 按
//!   健康态选腿 + 不对称切换）+ `DatagramUpstream`（UDP **恒走 tuic**，F2 硬约束在此一处钉死）。
//! - **铁律（spec §0/§2.1）**：failover 状态机只管「TCP relay 选哪条腿」，**绝不约束 `send_udp` 的
//!   TUIC `live_conn` 自愈**——UDP datagram 永久绑 TUIC，是其唯一出口；抑制它会让 REALITY 当班期间
//!   UDP 永久死亡。故 `send_udp` 无条件转发 tuic，不读 active_leg、不看冷却。
//! - **down 不对称（spec §2.3）**：黑洞快路（连接死）1 次失败即切；边缘慢路（连接活但流失败）连续 3 次切。
//! - **up 不对称迟滞（spec §2.4）**：REALITY 当班时后台每 30s 探 TUIC，连续 3 次成功 **且** 距切换 ≥60s 冷却才切回。
//! - 决策方法都收 `now_secs` 参数（纯逻辑、可注入时钟确定性单测，贴合本仓 forge_dns_reply 等惯用法）。
//! - 泛型而非 trait object：贴合本仓 `run_event_loop`/harness 的单态化零开销惯用法，且可注入 mock 单测。

use crate::shared::{ClientError, TargetAddr};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

/// down 慢路（连接活但流失败）连续失败多少次切到 REALITY。
const DOWN_SLOW_CONSEC: u32 = 3;
/// up 切回 TUIC 需连续多少次探针成功。
const UP_PROBE_CONSEC: u32 = 3;
/// up 切回 TUIC 的冷却窗（距切到 REALITY 的时刻），时间维迟滞防 flap。
const UP_COOLDOWN_SECS: u64 = 60;
/// REALITY 当班时后台探 TUIC 的节奏。
const PROBE_INTERVAL_SECS: u64 = 30;
/// 健康任务 tick 间隔（down 黑洞探测 + up 探针共用此 tick；up 探针再按 PROBE_INTERVAL_SECS 限速）。
const HEALTH_TICK_SECS: u64 = 3;
/// TUIC 当班时 `udp_rx.datagrams` 停滞多久判黑洞（真出口 acceptance 修——idle/open-success 检测对
/// QUIC 黑洞不可靠：open 写小 Connect 头乐观成功、keepalive 架空 idle）。用 rx 计数当存活信标：健康连接
/// 每 ~5s 有 keepalive ACK 进来→rx 增长；黑洞连 ACK 都收不到→rx 停滞。10s ≈ 2 个 keepalive 周期，停滞
/// 这么久 = 真黑洞（误判也只是优雅切 REALITY + 冷却切回，自愈）。
const BLACKHOLE_RX_STALE_SECS: u64 = 10;

/// 上游腿的健康探测面（failover 用）。TUIC 实现它；REALITY 腿无需。
///
/// 中文要点：与 `ProxyUpstream`/`DatagramUpstream` 并列，专供 failover 判健康——
/// `probe` 主动探活（QUIC 连接可建立），`is_dead` 区分 down 快路（黑洞，连接死）/慢路（流失败）。
#[async_trait::async_trait]
pub trait HealthProbe: Send + Sync {
    /// 主动探活：QUIC 连接可建立（live_conn 成功，含 QUIC 握手 + TUIC 认证，非浅探）→ true。
    async fn probe(&self) -> bool;
    /// 当前连接是否已死（`close_reason` 有值 = 黑洞/超时打死，重建才知能不能回来）。
    async fn is_dead(&self) -> bool;
    /// 当前连接累计收到的 UDP datagram 数（quinn `stats().udp_rx.datagrams`）。**黑洞存活信标**：
    /// 健康连接每 ~5s 有 keepalive ACK 进来→单调增；黑洞连 ACK 都收不到→停滞。down 探测据此判黑洞。
    /// **非阻塞**：返回 `None`=锁被占（后台正在重连）或不可读 → 调用方跳过本次观察（停滞计时靠 now 累积、不重置）。
    async fn rx_datagrams(&self) -> Option<u64>;
}

/// 黑洞探测器（纯逻辑，可单测）：观察 TUIC 连接的 `udp_rx.datagrams`，停滞 `BLACKHOLE_RX_STALE_SECS`
/// 即判黑洞。中文要点：rx 一变就刷新「上次变化时刻」；连续停滞超窗口 → true（判黑洞一次）。
pub struct BlackholeDetector {
    last_rx: u64,
    last_change_secs: u64,
    primed: bool,
}

impl BlackholeDetector {
    pub fn new() -> Self {
        Self { last_rx: 0, last_change_secs: 0, primed: false }
    }

    /// 观察一次 rx 计数。返回 true=判定黑洞（rx 停滞 ≥ 窗口）。
    pub fn observe(&mut self, rx: u64, now_secs: u64) -> bool {
        if !self.primed || rx != self.last_rx {
            self.last_rx = rx;
            self.last_change_secs = now_secs;
            self.primed = true;
            return false;
        }
        now_secs.saturating_sub(self.last_change_secs) >= BLACKHOLE_RX_STALE_SECS
    }

    /// 复位（切腿后重新计，避免跨腿误判）。
    pub fn reset(&mut self) {
        self.primed = false;
    }
}

impl Default for BlackholeDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// TCP relay 当前走哪条腿（仅 TCP；UDP 恒 TUIC，不在此枚举内）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpLeg {
    Tuic,
    Reality,
}

impl TcpLeg {
    /// 编码为 u8（0=Tuic / 1=Reality）。`pub(crate)` 供刀11 可观测性 `failover_leg_u8()` 复用——
    /// 单一编码源，勿在别处重写 match。
    pub(crate) fn as_u8(self) -> u8 {
        match self {
            TcpLeg::Tuic => 0,
            TcpLeg::Reality => 1,
        }
    }
    fn from_u8(v: u8) -> TcpLeg {
        if v == 1 { TcpLeg::Reality } else { TcpLeg::Tuic }
    }
}

/// 进程级 failover 共享状态。**仅管 TCP relay 选腿**（铁律：不约束 UDP 的 `live_conn` 自愈）。
///
/// 中文要点：决策方法（`record_tuic_*`/`record_probe`）收 `now_secs` 参数纯计算，便于注入时钟单测；
/// 生产经 `now_secs()`（单调 `Instant`）取真实时刻。原子计数用 Relaxed——它们是 advisory 计数，
/// 不保护内存；边界并发竞态只会让切换早/晚一次，benign（无 UB，稳定优先）。
pub struct FailoverState {
    active_tcp_leg: AtomicU8,
    /// 慢路连续失败计数（成功清零、切换清零）。
    tuic_consec_fail: AtomicU32,
    /// REALITY 当班时 TUIC 探针连续成功计数（失败清零、切回清零）。
    probe_consec_ok: AtomicU32,
    /// 切到 REALITY 的单调秒（算 up 冷却窗）。
    reality_switch_at: AtomicU64,
    /// 单调时钟（生产取 now_secs；测试不依赖它、直接传 now）。
    clock: Instant,
}

impl FailoverState {
    pub fn new() -> Self {
        Self {
            active_tcp_leg: AtomicU8::new(TcpLeg::Tuic.as_u8()),
            tuic_consec_fail: AtomicU32::new(0),
            probe_consec_ok: AtomicU32::new(0),
            reality_switch_at: AtomicU64::new(0),
            clock: Instant::now(),
        }
    }

    /// 当前 TCP 选腿（O(1) relaxed load，热路径每条新 TCP 连接读一次）。
    pub fn active_leg(&self) -> TcpLeg {
        TcpLeg::from_u8(self.active_tcp_leg.load(Ordering::Relaxed))
    }

    /// 直接设腿（测试 / 强制；正常切换走 record_* 决策）。
    pub fn set_leg(&self, leg: TcpLeg) {
        self.active_tcp_leg.store(leg.as_u8(), Ordering::Relaxed);
    }

    /// 单调秒（生产决策取此传入 record_*）。
    pub fn now_secs(&self) -> u64 {
        self.clock.elapsed().as_secs()
    }

    /// CAS 切到 REALITY：并发下（option A：失败 open 在多个 spawn task 里跑，record_tuic_failure 可并发）
    /// **只有一个 caller 真正切换**（返回 true）；其余返回 false，不重复触发 seamless 重试 / 不重置计数。
    fn switch_to_reality(&self, now_secs: u64) -> bool {
        if self
            .active_tcp_leg
            .compare_exchange(TcpLeg::Tuic.as_u8(), TcpLeg::Reality.as_u8(), Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return false; // 别人已切（或已不在 TUIC）
        }
        self.reality_switch_at.store(now_secs, Ordering::Relaxed);
        self.tuic_consec_fail.store(0, Ordering::Relaxed);
        self.probe_consec_ok.store(0, Ordering::Relaxed); // up 探针从头计
        true
    }

    /// CAS 切回 TUIC（仅探针任务单线程调用，CAS 仅为与 switch_to_reality 对称 + 防与并发 down 切换打架）。
    fn switch_to_tuic(&self) -> bool {
        if self
            .active_tcp_leg
            .compare_exchange(TcpLeg::Reality.as_u8(), TcpLeg::Tuic.as_u8(), Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        self.tuic_consec_fail.store(0, Ordering::Relaxed);
        self.probe_consec_ok.store(0, Ordering::Relaxed);
        true
    }

    /// TUIC open_tcp 成功 → 清慢路计数（连续性被打断）。
    pub fn record_tuic_success(&self) {
        self.tuic_consec_fail.store(0, Ordering::Relaxed);
    }

    /// TUIC open_tcp 失败 → 不对称 down 判定。`dead`=连接已死（黑洞快路）。
    /// 返回 true=已切到 REALITY（调用方可在 REALITY 重试本连接，seamless failover）。
    pub fn record_tuic_failure(&self, dead: bool, now_secs: u64) -> bool {
        if self.active_leg() != TcpLeg::Tuic {
            return false; // 已不在 TUIC（并发切换），无需重复切
        }
        if dead {
            return self.switch_to_reality(now_secs); // 快路：连接死 + 重建失败 = 黑洞强信号，1 次即切
        }
        let n = self.tuic_consec_fail.fetch_add(1, Ordering::Relaxed) + 1;
        if n >= DOWN_SLOW_CONSEC {
            self.switch_to_reality(now_secs) // 慢路：连接活但连续 3 次流失败
        } else {
            false
        }
    }

    /// REALITY 当班时一次 TUIC 探针结果 → 不对称迟滞切回（连续 3 成功 **且** 距切换 ≥60s）。
    /// 返回 true=已切回 TUIC。
    pub fn record_probe(&self, ok: bool, now_secs: u64) -> bool {
        if self.active_leg() != TcpLeg::Reality {
            return false; // 不在 REALITY，无需探针判定
        }
        if !ok {
            self.probe_consec_ok.store(0, Ordering::Relaxed); // 探针失败 → 连续性清零
            return false;
        }
        let n = self.probe_consec_ok.fetch_add(1, Ordering::Relaxed) + 1;
        let cooled = now_secs.saturating_sub(self.reality_switch_at.load(Ordering::Relaxed)) >= UP_COOLDOWN_SECS;
        if n >= UP_PROBE_CONSEC && cooled {
            self.switch_to_tuic()
        } else {
            false
        }
    }

    /// down 黑洞探测判定黑洞 → 切 REALITY（快路同款，CAS 幂等）。返回 true=本调用真切。
    /// 中文要点：这是**主**检测机制（rx 停滞 ~10s，可靠且不被 keepalive/乐观开流架空）；open_tcp 失败的
    /// 快/慢路是**备**（黑洞下 open 可能乐观成功、不报错，故不能只靠它，见 ADR-0011 §3b）。
    pub fn record_blackhole(&self, now_secs: u64) -> bool {
        if self.active_leg() != TcpLeg::Tuic {
            return false;
        }
        self.switch_to_reality(now_secs)
    }
}

impl Default for FailoverState {
    fn default() -> Self {
        Self::new()
    }
}

/// failover 上游包装：`open_tcp` 按 `active_leg` 选腿 + 不对称切换；`send_udp` 恒走 tuic（F2 硬约束）。
///
/// 中文要点：`T` = TUIC 腿（既是 TCP+UDP 上游，又 impl `HealthProbe`，因 UDP 恒走它 + 探针探它）；
/// `R` = REALITY 腿（仅 TCP）。生产 = `FailoverUpstream<TuicUpstream, RealityUpstream>`；测试 = mock 腿。
pub struct FailoverUpstream<T, R> {
    tuic: Arc<T>,
    reality: Arc<R>,
    state: Arc<FailoverState>,
}

impl<T, R> FailoverUpstream<T, R> {
    pub fn new(tuic: Arc<T>, reality: Arc<R>) -> Self {
        Self { tuic, reality, state: Arc::new(FailoverState::new()) }
    }

    /// 共享状态句柄（acceptance 观测 / 测试用）。
    pub fn state(&self) -> &Arc<FailoverState> {
        &self.state
    }
}

impl<T, R> FailoverUpstream<T, R>
where
    T: HealthProbe + 'static,
    R: Send + Sync + 'static,
{
    /// 启动后台健康任务（down 黑洞探测 + up 恢复探针，统一一个 task，HEALTH_TICK_SECS 节奏）：
    /// - **TUIC 当班**：观察 `udp_rx.datagrams`，停滞 ~10s（连 keepalive ACK 都没有）→ 判黑洞 → 切 REALITY。
    ///   这是**主**检测（可靠、不被 keepalive/乐观开流架空，见 ADR-0011 §3b）。
    /// - **REALITY 当班**：每 PROBE_INTERVAL_SECS 探一次 TUIC（live_conn 非浅探），不对称迟滞切回（spec §2.4）。
    pub fn spawn_health_probe(self: &Arc<Self>) {
        let state = self.state.clone();
        let tuic = self.tuic.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(HEALTH_TICK_SECS));
            let mut detector = BlackholeDetector::new();
            let mut secs_since_up_probe = 0u64;
            loop {
                tick.tick().await;
                match state.active_leg() {
                    TcpLeg::Tuic => {
                        secs_since_up_probe = 0;
                        // rx_datagrams 非阻塞：None=锁被占（重连中）→ 跳过本次观察（停滞计时靠 now 累积、不重置）。
                        if let Some(rx) = tuic.rx_datagrams().await
                            && detector.observe(rx, state.now_secs())
                            && state.record_blackhole(state.now_secs())
                        {
                            println!(
                                "🔀 failover：TUIC 黑洞（{BLACKHOLE_RX_STALE_SECS}s 无收包，连 keepalive ACK 都没有）→ 切到 REALITY 备路",
                            );
                            detector.reset();
                        }
                    }
                    TcpLeg::Reality => {
                        // 每 REALITY tick 复位（幂等）：保证切回 TUIC 时 detector 从干净状态开始计（非仅切换瞬间）。
                        detector.reset();
                        secs_since_up_probe += HEALTH_TICK_SECS;
                        if secs_since_up_probe >= PROBE_INTERVAL_SECS {
                            secs_since_up_probe = 0;
                            let ok = tuic.probe().await;
                            if state.record_probe(ok, state.now_secs()) {
                                println!("🔀 failover：TUIC 探针连续成功 + 冷却已过 → 切回 TUIC 主腿");
                            }
                        }
                    }
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl<T, R> ProxyUpstream for FailoverUpstream<T, R>
where
    T: ProxyUpstream + HealthProbe,
    R: ProxyUpstream,
{
    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
        match self.state.active_leg() {
            TcpLeg::Tuic => match self.tuic.open_tcp(target).await {
                Ok(stream) => {
                    self.state.record_tuic_success();
                    Ok(stream)
                }
                Err(e) => {
                    // 区分 down 快路（连接死=黑洞）/慢路（连接活但流失败）。
                    let dead = self.tuic.is_dead().await;
                    if self.state.record_tuic_failure(dead, self.state.now_secs()) {
                        let why = if dead { "连接死(黑洞快路)" } else { "连续失败(边缘慢路)" };
                        println!("🔀 failover：TUIC {why} → 切到 REALITY 备路并重试本连接 {}", target.to_wire_string());
                        // seamless failover：本连接立即在 REALITY 重试，避免触发切换的这条连接白白失败。
                        self.reality.open_tcp(target).await
                    } else {
                        Err(e) // 慢路未达阈值：本连接失败（应用重试），暂不切腿
                    }
                }
            },
            TcpLeg::Reality => self.reality.open_tcp(target).await,
        }
    }

    /// **恒 false → failover 模式下所有 open 都 spawn 出主循环**（code-review Finding 1 的深修）。
    /// 中文要点：不按 active_leg 动态判（那会在「读 open_is_cheap」与「open_tcp 内再读 leg」之间留 TOCTOU
    /// 窗口 → inline 分支可能真跑 REALITY 握手 stall 主循环）；且**失败模式下 TUIC open 本身也不廉价**——
    /// 它要做黑洞 reconnect（QUIC 握手到被封 server，可阻塞），inline 同样 stall。恒 spawn 把所有 open
    /// （含 down 切换的 seamless 重试、黑洞 reconnect）都移出主循环，彻底消除 stall 与 TOCTOU。
    /// 刀14d 后纯 TUIC 也把 open spawn 出主循环；failover 继续恒 false，避免动态选腿 TOCTOU。
    fn open_is_cheap(&self) -> bool {
        false
    }

    /// 刀11：当前 TCP 选腿（独立周期 Relaxed 读，不在数据路径 → 不破铁律）。
    fn failover_leg_u8(&self) -> u8 {
        self.state.active_leg().as_u8()
    }
}

#[async_trait::async_trait]
impl<T, R> DatagramUpstream for FailoverUpstream<T, R>
where
    T: DatagramUpstream,
    R: Send + Sync,
{
    async fn send_udp(&self, datagram: Vec<u8>) {
        // F2 硬约束（铁律）：UDP datagram 永久绑 TUIC，绝不读 active_leg、绝不看冷却、绝不降级 REALITY。
        // TUIC 不可用时由 TuicUpstream::send_udp 内部优雅丢弃（udp_drops++）+ live_conn 自愈，failover 不干预。
        self.tuic.send_udp(datagram).await;
    }

    // 刀11：上行计数转发 tuic 腿（UDP 恒 TUIC，REALITY 腿无 datagram）。
    fn udp_drops_up(&self) -> u64 {
        self.tuic.udp_drops_up()
    }
    fn udp_stream_fallbacks(&self) -> u64 {
        self.tuic.udp_stream_fallbacks()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    /// 一条可配置 mock 腿：记账 open_tcp/send_udp；fail_open/dead/probe_ok 控制行为。
    #[derive(Default)]
    struct MockLeg {
        tcp: Mutex<Vec<String>>,
        udp: Mutex<Vec<Vec<u8>>>,
        fail_open: AtomicBool,
        dead: AtomicBool,
        probe_ok: AtomicBool,
        rx: std::sync::atomic::AtomicU64,
        drops_up: std::sync::atomic::AtomicU64,
    }

    #[async_trait::async_trait]
    impl ProxyUpstream for MockLeg {
        async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
            self.tcp.lock().unwrap().push(target.to_wire_string());
            if self.fail_open.load(Ordering::Relaxed) {
                Err(ClientError::Reality("mock open_tcp 失败".into()))
            } else {
                Ok(Box::new(tokio::io::duplex(64).0))
            }
        }
    }

    #[async_trait::async_trait]
    impl DatagramUpstream for MockLeg {
        async fn send_udp(&self, datagram: Vec<u8>) {
            self.udp.lock().unwrap().push(datagram);
        }
        fn udp_drops_up(&self) -> u64 {
            self.drops_up.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl HealthProbe for MockLeg {
        async fn probe(&self) -> bool {
            self.probe_ok.load(Ordering::Relaxed)
        }
        async fn is_dead(&self) -> bool {
            self.dead.load(Ordering::Relaxed)
        }
        async fn rx_datagrams(&self) -> Option<u64> {
            Some(self.rx.load(Ordering::Relaxed))
        }
    }

    fn mk() -> (Arc<MockLeg>, Arc<MockLeg>, FailoverUpstream<MockLeg, MockLeg>) {
        let tuic = Arc::new(MockLeg::default());
        let reality = Arc::new(MockLeg::default());
        let fo = FailoverUpstream::new(tuic.clone(), reality.clone());
        (tuic, reality, fo)
    }

    fn target() -> TargetAddr {
        TargetAddr::parse("1.2.3.4:443").unwrap()
    }

    /// 刀11：`failover_leg_u8()` 随 `active_leg` 变（0=Tuic / 1=Reality）。
    #[test]
    fn failover_leg_u8_tracks_active_leg() {
        let (_t, _r, fo) = mk();
        assert_eq!(fo.failover_leg_u8(), 0, "默认 Tuic");
        fo.state().set_leg(TcpLeg::Reality);
        assert_eq!(fo.failover_leg_u8(), 1);
        fo.state().set_leg(TcpLeg::Tuic);
        assert_eq!(fo.failover_leg_u8(), 0);
    }

    /// 刀11：`FailoverUpstream::udp_drops_up()` 转发 tuic 腿（UDP 恒 TUIC）。
    #[test]
    fn udp_drops_up_forwards_to_tuic_leg() {
        let (tuic, reality, fo) = mk();
        tuic.drops_up.store(42, Ordering::Relaxed);
        reality.drops_up.store(999, Ordering::Relaxed); // 不应被读到
        assert_eq!(fo.udp_drops_up(), 42);
        assert_eq!(fo.udp_stream_fallbacks(), 0, "MockLeg 未 override fallbacks → 默认 0");
    }

    /// 刀11：非 failover 单腿上游继承默认（leg=NO_FAILOVER、fallbacks=0）。
    #[test]
    fn non_failover_leg_u8_is_sentinel() {
        let leg = MockLeg::default();
        assert_eq!(leg.failover_leg_u8(), crate::metrics::NO_FAILOVER);
        assert_eq!(leg.udp_stream_fallbacks(), 0);
    }

    /// F2：open_tcp 按 active_leg 选腿；send_udp 恒走 tuic（即使 leg=REALITY）。
    #[tokio::test]
    async fn open_tcp_routes_by_leg_and_udp_pinned_to_tuic() {
        let (tuic, reality, fo) = mk();
        fo.open_tcp(&target()).await.unwrap();
        assert_eq!(tuic.tcp.lock().unwrap().len(), 1, "默认 leg=TUIC → TCP 走 tuic");
        assert!(reality.tcp.lock().unwrap().is_empty());

        fo.send_udp(vec![1, 2, 3]).await;
        assert_eq!(tuic.udp.lock().unwrap().len(), 1, "UDP 走 tuic");

        fo.state().set_leg(TcpLeg::Reality);
        fo.open_tcp(&target()).await.unwrap();
        assert_eq!(reality.tcp.lock().unwrap().len(), 1, "leg=REALITY → TCP 走 reality");

        // F2 硬约束 / 铁律：leg=REALITY 时 UDP 仍恒走 tuic，reality 永不收 UDP。
        fo.send_udp(vec![4, 5]).await;
        assert_eq!(tuic.udp.lock().unwrap().len(), 2, "leg=REALITY 时 UDP 仍恒走 tuic（F2 硬约束/铁律）");
        assert!(reality.udp.lock().unwrap().is_empty(), "reality 腿永不承载 UDP");
    }

    /// F1 down 快路：TUIC 失败 + 连接死（黑洞）→ 1 次即切 REALITY，本连接在 REALITY 重试成功。
    #[tokio::test]
    async fn down_fast_path_switches_on_dead_connection() {
        let (tuic, reality, fo) = mk();
        tuic.fail_open.store(true, Ordering::Relaxed);
        tuic.dead.store(true, Ordering::Relaxed); // 黑洞快路

        let r = fo.open_tcp(&target()).await;
        assert!(r.is_ok(), "切 REALITY 后重试应成功（reality 默认不失败）");
        assert_eq!(fo.state().active_leg(), TcpLeg::Reality, "dead → 1 次即切 REALITY");
        assert_eq!(reality.tcp.lock().unwrap().len(), 1, "触发切换的本连接在 REALITY 重试");
    }

    /// F1 down 慢路：TUIC 失败但连接活 → 前 2 次不切（返回 Err），第 3 次才切 + 重试 REALITY。
    #[tokio::test]
    async fn down_slow_path_switches_after_three_failures() {
        let (tuic, reality, fo) = mk();
        tuic.fail_open.store(true, Ordering::Relaxed);
        tuic.dead.store(false, Ordering::Relaxed); // 连接活、流失败

        assert!(fo.open_tcp(&target()).await.is_err(), "慢路第1次失败不切");
        assert_eq!(fo.state().active_leg(), TcpLeg::Tuic);
        assert!(fo.open_tcp(&target()).await.is_err(), "慢路第2次失败不切");
        assert_eq!(fo.state().active_leg(), TcpLeg::Tuic);

        let r = fo.open_tcp(&target()).await; // 第3次 → 切 + 重试 reality（默认成功）
        assert!(r.is_ok(), "第3次切 REALITY 后重试成功");
        assert_eq!(fo.state().active_leg(), TcpLeg::Reality, "连续 3 次 → 切 REALITY");
        assert_eq!(reality.tcp.lock().unwrap().len(), 1);
    }

    /// F1：TUIC 成功打断慢路连续性 → 计数清零，之后 2 次失败不切。
    #[tokio::test]
    async fn success_resets_slow_path_counter() {
        let (tuic, _reality, fo) = mk();
        tuic.fail_open.store(true, Ordering::Relaxed);
        let _ = fo.open_tcp(&target()).await; // fail 1
        let _ = fo.open_tcp(&target()).await; // fail 2
        tuic.fail_open.store(false, Ordering::Relaxed);
        fo.open_tcp(&target()).await.unwrap(); // success → 清零
        tuic.fail_open.store(true, Ordering::Relaxed);
        let _ = fo.open_tcp(&target()).await; // fail 1（清零后）
        let _ = fo.open_tcp(&target()).await; // fail 2
        assert_eq!(fo.state().active_leg(), TcpLeg::Tuic, "成功清零后仅 2 次失败 → 不切");
    }

    /// F1 up 迟滞（state 级，注入时钟）：连续 3 成功 **且** 距切换 ≥60s 才切回 TUIC；失败清零连续性。
    #[test]
    fn up_switch_back_requires_three_ok_and_cooldown() {
        let s = FailoverState::new();
        assert!(s.record_tuic_failure(true, 100), "dead → 切 REALITY，switch_at=100");
        assert_eq!(s.active_leg(), TcpLeg::Reality);

        assert!(!s.record_probe(true, 130), "ok#1，冷却 30s<60 → 不切");
        assert!(!s.record_probe(true, 140), "ok#2");
        assert!(!s.record_probe(true, 150), "ok#3 但冷却 50s<60 → 不切");
        assert_eq!(s.active_leg(), TcpLeg::Reality);
        assert!(s.record_probe(true, 165), "ok#4 且冷却 65s≥60 + 连续≥3 → 切回 TUIC");
        assert_eq!(s.active_leg(), TcpLeg::Tuic);

        // 失败打断连续性。
        let s2 = FailoverState::new();
        s2.record_tuic_failure(true, 100);
        assert!(!s2.record_probe(true, 200), "ok#1（已冷却）连续1<3");
        assert!(!s2.record_probe(false, 210), "失败 → 连续清零");
        assert!(!s2.record_probe(true, 220), "ok 连续1");
        assert!(!s2.record_probe(true, 230), "ok 连续2");
        assert_eq!(s2.active_leg(), TcpLeg::Reality, "中途失败清零 → 仅 2 连续，不切");
        assert!(s2.record_probe(true, 240), "ok 连续3 + 冷却 → 切回");
        assert_eq!(s2.active_leg(), TcpLeg::Tuic);
    }

    /// CAS/guard 幂等：已切到 REALITY 后再来 dead 失败不重复切、返回 false（并发下只一个 caller 真切，
    /// 单线程经 `active_leg != Tuic` guard 同样保证；Finding 5）。
    #[test]
    fn down_switch_is_idempotent() {
        let s = FailoverState::new();
        assert!(s.record_tuic_failure(true, 10), "首次 dead → 切 REALITY，返回 true");
        assert_eq!(s.active_leg(), TcpLeg::Reality);
        assert!(!s.record_tuic_failure(true, 11), "已在 REALITY → 不重复切，返回 false");
        assert!(!s.record_tuic_failure(false, 12), "已在 REALITY → 慢路也不动，返回 false");
    }

    /// down 黑洞探测（主检测）：rx 停滞 ≥ 窗口 → BlackholeDetector 判黑洞；rx 一增长就复位计时。
    #[test]
    fn blackhole_detector_fires_on_rx_stall() {
        let mut d = BlackholeDetector::new();
        // 首次观察 = prime，不判黑洞。
        assert!(!d.observe(100, 0), "prime");
        // rx 持续增长（健康，keepalive ACK 进来）→ 永不判黑洞。
        assert!(!d.observe(101, 5));
        assert!(!d.observe(102, 10));
        // rx 停滞：从 t=10 起不变；t=10+10=20 达窗口 → 判黑洞。
        assert!(!d.observe(102, 12), "停滞 2s <10s");
        assert!(!d.observe(102, 19), "停滞 9s <10s");
        assert!(d.observe(102, 20), "停滞 10s ≥窗口 → 黑洞");
        // 复位后重新计。
        d.reset();
        assert!(!d.observe(102, 21), "reset 后重新 prime");
        assert!(!d.observe(102, 30), "刚 prime 不到窗口");
        assert!(d.observe(102, 31), "再停滞满窗口");
    }

    /// record_blackhole：TUIC 当班 → 切 REALITY（CAS 幂等）；已在 REALITY → false。
    #[test]
    fn record_blackhole_switches_once() {
        let s = FailoverState::new();
        assert!(s.record_blackhole(100), "TUIC 当班 + 黑洞 → 切 REALITY");
        assert_eq!(s.active_leg(), TcpLeg::Reality);
        assert!(!s.record_blackhole(101), "已在 REALITY → 不重复切");
    }

    /// 铁律：send_udp 永不被 active_leg/冷却 gate——REALITY 当班 + 冷却期内仍恒走 tuic。
    #[tokio::test]
    async fn send_udp_never_gated_by_failover_state() {
        let (tuic, reality, fo) = mk();
        fo.state().record_tuic_failure(true, fo.state().now_secs()); // → REALITY + 冷却中
        assert_eq!(fo.state().active_leg(), TcpLeg::Reality);
        fo.send_udp(vec![9, 9, 9]).await;
        assert_eq!(tuic.udp.lock().unwrap().len(), 1, "REALITY 当班 + 冷却中，UDP 仍恒走 tuic（铁律）");
        assert!(reality.udp.lock().unwrap().is_empty());
    }
}

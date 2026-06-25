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
}

/// TCP relay 当前走哪条腿（仅 TCP；UDP 恒 TUIC，不在此枚举内）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpLeg {
    Tuic,
    Reality,
}

impl TcpLeg {
    fn as_u8(self) -> u8 {
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
    /// 启动后台健康探针任务：REALITY 当班时每 30s 探 TUIC，按不对称迟滞切回（spec §2.4）。
    /// 中文要点：仅 REALITY 当班才探（leg==TUIC 时 open_tcp 失败自会驱动 down 切换，无需探）。
    /// 探针 = `HealthProbe::probe`（live_conn，连接已健康时仅检查 close_reason + clone，廉价）。
    pub fn spawn_health_probe(self: &Arc<Self>) {
        let state = self.state.clone();
        let tuic = self.tuic.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(PROBE_INTERVAL_SECS));
            loop {
                tick.tick().await;
                if state.active_leg() != TcpLeg::Reality {
                    continue; // 仅 REALITY 当班才探 TUIC 是否恢复
                }
                let ok = tuic.probe().await;
                if state.record_probe(ok, state.now_secs()) {
                    println!("🔀 failover：TUIC 探针连续成功 + 冷却已过 → 切回 TUIC 主腿");
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
    /// 纯 TUIC 默认模式（非 failover）走 `TuicUpstream`（open_is_cheap=true）仍 inline，零回归不受影响。
    fn open_is_cheap(&self) -> bool {
        false
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
    }

    #[async_trait::async_trait]
    impl HealthProbe for MockLeg {
        async fn probe(&self) -> bool {
            self.probe_ok.load(Ordering::Relaxed)
        }
        async fn is_dead(&self) -> bool {
            self.dead.load(Ordering::Relaxed)
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

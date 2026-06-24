//! 刀9：健康感知 TUIC↔REALITY auto-failover + 分离 TCP/UDP 上游（F2 + F1）。
//!
//! 中文要点：
//! - `FailoverUpstream<T, R>` 包装两条腿（T=TUIC，R=REALITY），impl `ProxyUpstream`（TCP relay 按
//!   健康态选腿）+ `DatagramUpstream`（UDP **恒走 tuic**，F2 硬约束在此一处钉死）。
//! - **铁律（spec §0/§2.1）**：failover 状态机只管「TCP relay 选哪条腿」，**绝不约束 `send_udp` 的
//!   TUIC `live_conn` 自愈**——UDP datagram 永久绑 TUIC，是其唯一出口；抑制它会让 REALITY 当班期间
//!   UDP 永久死亡。故 `send_udp` 无条件转发 tuic，不读 active_leg、不看冷却。
//! - 泛型而非 trait object：贴合本仓 `run_event_loop`/harness 的单态化零开销惯用法，且可注入 mock 单测。
//! - 本文件 F2 只做「分离 + 选腿分发」；切换判据 / 探针 / 冷却（F1）后续接入（见 spec §2）。

use crate::shared::{ClientError, TargetAddr};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

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
/// 中文要点：F2 只用 `active_tcp_leg`（默认 TUIC，无切换逻辑）。F1 将在此扩计数器/冷却时刻
/// （`tuic_consec_fail` / `probe_consec_ok` / `reality_switch_at`），按 spec §2.3/§2.4 不对称切换。
pub struct FailoverState {
    active_tcp_leg: AtomicU8,
}

impl FailoverState {
    pub fn new() -> Self {
        Self { active_tcp_leg: AtomicU8::new(TcpLeg::Tuic.as_u8()) }
    }

    /// 当前 TCP 选腿（O(1) relaxed load，热路径每条新 TCP 连接读一次）。
    pub fn active_leg(&self) -> TcpLeg {
        TcpLeg::from_u8(self.active_tcp_leg.load(Ordering::Relaxed))
    }

    /// 设置 TCP 选腿（F1 切换 / 测试用）。
    pub fn set_leg(&self, leg: TcpLeg) {
        self.active_tcp_leg.store(leg.as_u8(), Ordering::Relaxed);
    }
}

impl Default for FailoverState {
    fn default() -> Self {
        Self::new()
    }
}

/// failover 上游包装：`open_tcp` 按 `active_leg` 选腿；`send_udp` 恒走 tuic（F2 硬约束）。
///
/// 中文要点：`T` = TUIC 腿（必须同时是 TCP+UDP 上游，因 UDP 恒走它）；`R` = REALITY 腿（仅 TCP）。
/// 生产 = `FailoverUpstream<TuicUpstream, RealityUpstream>`；测试 = mock 腿。
pub struct FailoverUpstream<T, R> {
    tuic: Arc<T>,
    reality: Arc<R>,
    state: Arc<FailoverState>,
}

impl<T, R> FailoverUpstream<T, R> {
    pub fn new(tuic: Arc<T>, reality: Arc<R>) -> Self {
        Self { tuic, reality, state: Arc::new(FailoverState::new()) }
    }

    /// 共享状态句柄（F1 的切换/探针任务、acceptance 观测、测试用）。
    pub fn state(&self) -> &Arc<FailoverState> {
        &self.state
    }
}

#[async_trait::async_trait]
impl<T, R> ProxyUpstream for FailoverUpstream<T, R>
where
    T: ProxyUpstream,
    R: ProxyUpstream,
{
    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
        // F2：按当前选腿分发（F1 将在此前后加成败记账 + 切换判定）。
        match self.state.active_leg() {
            TcpLeg::Tuic => self.tuic.open_tcp(target).await,
            TcpLeg::Reality => self.reality.open_tcp(target).await,
        }
    }
}

#[async_trait::async_trait]
impl<T, R> DatagramUpstream for FailoverUpstream<T, R>
where
    T: DatagramUpstream,
    R: Send + Sync,
{
    async fn send_udp(&self, datagram: Vec<u8>) {
        // F2 硬约束（铁律）：UDP datagram 永久绑 TUIC，绝不随 active_tcp_leg 切换、绝不降级 REALITY。
        // TUIC 不可用时由 TuicUpstream::send_udp 内部优雅丢弃（udp_drops++），failover 不干预。
        self.tuic.send_udp(datagram).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 一条 mock 腿：记账 open_tcp 的 target 与 send_udp 的 datagram；open_tcp 返回一截 duplex 流。
    #[derive(Default)]
    struct MockLeg {
        tcp: Mutex<Vec<String>>,
        udp: Mutex<Vec<Vec<u8>>>,
    }

    #[async_trait::async_trait]
    impl ProxyUpstream for MockLeg {
        async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
            self.tcp.lock().unwrap().push(target.to_wire_string());
            Ok(Box::new(tokio::io::duplex(64).0))
        }
    }

    #[async_trait::async_trait]
    impl DatagramUpstream for MockLeg {
        async fn send_udp(&self, datagram: Vec<u8>) {
            self.udp.lock().unwrap().push(datagram);
        }
    }

    /// F2 核心：open_tcp 按 active_leg 选腿；send_udp 恒走 tuic（即使 leg=REALITY）。
    #[tokio::test]
    async fn open_tcp_routes_by_leg_and_udp_pinned_to_tuic() {
        let tuic = Arc::new(MockLeg::default());
        let reality = Arc::new(MockLeg::default());
        let fo = FailoverUpstream::new(tuic.clone(), reality.clone());

        // 默认 leg=TUIC：TCP 走 tuic，reality 不沾。
        let t1 = TargetAddr::parse("1.2.3.4:80").unwrap();
        fo.open_tcp(&t1).await.unwrap();
        assert_eq!(tuic.tcp.lock().unwrap().as_slice(), ["1.2.3.4:80"], "默认 leg=TUIC → TCP 走 tuic");
        assert!(reality.tcp.lock().unwrap().is_empty(), "TUIC 当班 reality 不开 TCP");

        // UDP 走 tuic。
        fo.send_udp(vec![1, 2, 3]).await;
        assert_eq!(tuic.udp.lock().unwrap().len(), 1, "UDP 走 tuic");

        // 切到 REALITY：TCP 改走 reality。
        fo.state().set_leg(TcpLeg::Reality);
        let t2 = TargetAddr::parse("5.6.7.8:443").unwrap();
        fo.open_tcp(&t2).await.unwrap();
        assert_eq!(reality.tcp.lock().unwrap().as_slice(), ["5.6.7.8:443"], "leg=REALITY → TCP 走 reality");
        assert_eq!(tuic.tcp.lock().unwrap().len(), 1, "TUIC 不再收新 TCP");

        // F2 硬约束：leg=REALITY 时 UDP **仍恒走 tuic**，reality 永不收 UDP。
        fo.send_udp(vec![4, 5]).await;
        assert_eq!(tuic.udp.lock().unwrap().len(), 2, "leg=REALITY 时 UDP 仍恒走 tuic（F2 硬约束）");
        assert!(reality.udp.lock().unwrap().is_empty(), "reality 腿永不承载 UDP");
    }

    /// leg 状态读写自洽（u8 往返）。
    #[test]
    fn leg_state_roundtrip() {
        let s = FailoverState::new();
        assert_eq!(s.active_leg(), TcpLeg::Tuic, "默认 TUIC");
        s.set_leg(TcpLeg::Reality);
        assert_eq!(s.active_leg(), TcpLeg::Reality);
        s.set_leg(TcpLeg::Tuic);
        assert_eq!(s.active_leg(), TcpLeg::Tuic);
    }
}

//! TUIC's Failover health adapter. Owns only observations/probe delegation;
//! NodeHost retains transport, recovery and task custody. Entry: HealthProbe.
//! Characterization: node/tests/failover_health.rs (real QUIC/AUTH/settlement).

use super::TuicUpstream;

/// 刀9 F1：failover 健康探测面。`probe` 主动探活（live_conn=QUIC 握手+TUIC 认证，非浅探）；
/// `is_dead` 区分 down 快路（黑洞，连接被 idle/keepalive 打死）/慢路（流失败）。
#[async_trait::async_trait]
impl crate::failover::HealthProbe for TuicUpstream {
    fn snapshot(&self) -> Option<crate::failover::HealthSnapshot> {
        use crate::failover::{FallbackAdmission as Gate, HealthSnapshot};
        let primary = self.tcp_pool_slots.first()?.state.try_lock().ok()?;
        let fallback = match self.udp_runtime.owner.get() {
            None => Gate::Unowned,
            Some(owner) => match owner.upgrade() {
                Some(owner) => owner.fallback_admission(),
                None => Gate::OwnerLost,
            },
        };
        Some(HealthSnapshot {
            dead: primary.current.transport.close_reason().is_some(),
            rx_datagrams: primary.current.transport.stats().udp_rx.datagrams,
            fallback,
        })
    }

    async fn probe(&self) -> bool {
        self.live_conn().await.is_ok()
    }
    async fn is_dead(&self) -> bool {
        self.tcp_pool_slots
            .first()
            .expect("TuicUpstream always has a primary connection")
            .state
            .lock()
            .await
            .current
            .transport
            .close_reason()
            .is_some()
    }
    /// 累计收到的 UDP datagram 数（quinn `stats().udp_rx.datagrams`）——黑洞存活信标：健康连接每 ~5s
    /// 有 keepalive ACK 进来→单调增；黑洞连 ACK 都收不到→停滞。down 探测据此判黑洞（不被乐观开流/keepalive 架空）。
    /// **非阻塞**（try_lock，同 current_conn）：锁被占（后台 start_udp 正在重连，持锁≤5s）→ `None`，**绝不 await
    /// 锁**——否则健康探针任务被卡 5s、把锁等待当成网络停滞、检测计时偏斜（review finding）。调用方对 None
    /// 跳过本次观察：连接正在重连本就不健康，停滞计时靠 `now` 累积、不重置，下次能读到（仍停滞）即判黑洞。
    async fn rx_datagrams(&self) -> Option<u64> {
        self.snapshot().map(|snapshot| snapshot.rx_datagrams)
    }
}

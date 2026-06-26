//! 刀12：主循环 profiler —— 量化 #4（单核 smoltcp poll）vs #3（单条 QUIC 连接 CC）的判决仪器。
//!
//! 中文要点：这是 knife1 [`MetricsSink`](crate::client_tun::MetricsSink) **计时**接缝的生产可用实现
//! （与 knife11 的 [`Metrics`](crate::metrics::Metrics) 累计计数器/gauge 正交，见 CONTEXT.md「Metrics
//! snapshot」）。生产默认仍传 `NoopSink`（零开销）；`MINI_VPN_PROFILE_LOOP=1` 时传 `LoopProfiler`，
//! 按 `MINI_VPN_METRICS_SECS` 节拍打 🔬 归因行。
//!
//! 三个 wall-fraction（每报告周期）：
//! - poll-fraction        = Σ(poll 段)/wall   —— smoltcp poll(+flush_tx) 占主循环 wall 比例
//! - relay-fraction       = Σ(relay 段)/wall  —— relay 调度段占比（刀2 后应很小）
//! - loop-active-fraction = 1 − park/wall      —— 主循环非空等占比（select! park 之外）
//!
//! 判决：loop-active→~100% 且 poll 占大头 ⇒ #4（分片有理）；loop-active 低（多在 park 空等上游）
//! ⇒ #3（连接池才是杠杆）。详见 docs/tech/2026-06-26-knife12-multicore-quantify-spec.md §3。

use std::time::Duration;

/// 一个报告周期的纯快照（值类型，便于格式化与单测）。各段为周期内累计耗时 + 实测 wall。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LoopProfileSnapshot {
    /// 周期内 poll 段（`iface.poll` + `flush_tx`）累计耗时。
    pub poll: Duration,
    /// 周期内 relay 调度段（`process_dirty_relay`）累计耗时。
    pub relay: Duration,
    /// 周期内主循环 park（停在 `tokio::select!` 空等下一个事件）累计耗时。
    pub park: Duration,
    /// 周期实测 wall（非名义周期，挡 `interval` 漂移）。
    pub wall: Duration,
    /// 周期内 select! 迭代数（旁证负载强度）。
    pub iters: u64,
}

impl LoopProfileSnapshot {
    /// poll 段占 wall 比例 [0,1]；wall=0 → 0（不除零）。
    pub fn poll_fraction(&self) -> f64 {
        ratio(self.poll, self.wall)
    }

    /// relay 段占 wall 比例 [0,1]。
    pub fn relay_fraction(&self) -> f64 {
        ratio(self.relay, self.wall)
    }

    /// park（`select!` 空等）占 wall 比例；时钟抖动下可 >1（诚实暴露，不 clamp）。
    pub fn park_fraction(&self) -> f64 {
        ratio(self.park, self.wall)
    }

    /// 主循环 active（非 park）占 wall 比例 = 1 − park/wall，clamp [0,1]
    /// （park>wall 的抖动 → 0；空周期 wall=0 → 0）。这是 #4-vs-#3 判决的核心信号。
    pub fn loop_active_fraction(&self) -> f64 {
        if self.wall.is_zero() {
            return 0.0;
        }
        (1.0 - ratio(self.park, self.wall)).clamp(0.0, 1.0)
    }
}

/// `num/den` 比例，`den<=0` → 0（挡除零 + 空周期）。结果不 clamp（调用方按语义处理）。
fn ratio(num: Duration, den: Duration) -> f64 {
    let d = den.as_secs_f64();
    if d <= 0.0 {
        0.0
    } else {
        num.as_secs_f64() / d
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const EPS: f64 = 1e-9;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn fractions_basic_split() {
        // poll 600ms + relay 100ms + park 300ms over wall 1000ms。
        let snap = LoopProfileSnapshot {
            poll: ms(600),
            relay: ms(100),
            park: ms(300),
            wall: ms(1000),
            iters: 50,
        };
        assert!((snap.poll_fraction() - 0.6).abs() < EPS);
        assert!((snap.relay_fraction() - 0.1).abs() < EPS);
        assert!((snap.park_fraction() - 0.3).abs() < EPS);
        // loop-active = 1 − park/wall = 0.7。
        assert!((snap.loop_active_fraction() - 0.7).abs() < EPS);
    }

    #[test]
    fn loop_active_high_when_park_low() {
        // CPU-bound 主循环：几乎不 park → loop-active 趋近 1。
        let snap = LoopProfileSnapshot {
            poll: ms(950),
            relay: ms(40),
            park: ms(5),
            wall: ms(1000),
            iters: 9000,
        };
        assert!(snap.loop_active_fraction() > 0.99);
        assert!(snap.poll_fraction() > 0.9);
    }

    #[test]
    fn loop_active_low_when_mostly_parked() {
        // 上游受限：主循环大量 park 在 select! 空等 → loop-active 低（指向 #3）。
        let snap = LoopProfileSnapshot {
            poll: ms(120),
            relay: ms(10),
            park: ms(870),
            wall: ms(1000),
            iters: 1200,
        };
        assert!(snap.loop_active_fraction() < 0.2);
    }

    #[test]
    fn empty_period_is_all_zero_no_divide_by_zero() {
        // wall=0（空周期/无活动）→ 所有 fraction 0，绝不 panic / 除零。
        let snap = LoopProfileSnapshot {
            poll: Duration::ZERO,
            relay: Duration::ZERO,
            park: Duration::ZERO,
            wall: Duration::ZERO,
            iters: 0,
        };
        assert_eq!(snap.poll_fraction(), 0.0);
        assert_eq!(snap.relay_fraction(), 0.0);
        assert_eq!(snap.park_fraction(), 0.0);
        assert_eq!(snap.loop_active_fraction(), 0.0);
    }

    #[test]
    fn park_exceeding_wall_clamps_active_to_zero() {
        // 时钟抖动 / 跨报告边界：park 略大于 wall → loop-active clamp 到 0（不出负数）。
        let snap = LoopProfileSnapshot {
            poll: ms(50),
            relay: ms(0),
            park: ms(1200),
            wall: ms(1000),
            iters: 100,
        };
        assert_eq!(snap.loop_active_fraction(), 0.0);
        // park_fraction 反映原始比例（>1 容许，诚实暴露抖动）。
        assert!(snap.park_fraction() > 1.0);
    }
}

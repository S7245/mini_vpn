# 刀12 plan — 多核逼近 100M：量化定位（TDD 分解）

> 配套 spec：同目录 `2026-06-26-knife12-multicore-quantify-spec.md`。
> 节奏：每任务 红→绿→commit→**push**；一个分支一个 writer；cwd 陷阱——每条 git/cargo 前
> `git branch --show-current` 确认在 `claude/knife12-multicore-100m`、用绝对路径。
> 质量门：lib+harness 全绿、`clippy --all-targets --features harness` 0 warning、release 绿。
> 原则：系统稳定 > 代码漂亮；**默认 NoopSink 路径零回归**。

## 任务依赖

```
T1 LoopProfiler 核心（纯单测：fraction 数学 + 周期重置 + 段累计）
        │
T2 MetricsSink 扩 park/report 接缝 + 主循环接入（9 处插桩，不破 NoopSink 零开销）
        │
T3 env 门控 + 装配（from_env 读 MINI_VPN_PROFILE_LOOP；start_tun_proxy 二选一传 sink）+ 🔬 行格式
        │
T4 harness 接入 + 多核就绪 spike（multi_thread + 注入合成 per-packet CPU；loop-active 饱和断言）
        │
T5 acceptance 配方 + 真出口跑 + ADR + findings/HANDOFF（≥100M 链路）
```

---

## T1 — `LoopProfiler` 核心（纯单测先行）

**目标**：一个不依赖主循环的纯结构，吃 poll/relay/park 累计 `Duration` + 周期 wall，吐三 fraction。

**红**（`src/metrics.rs` 或新 `src/loop_profiler.rs`，纯单测）：
- `LoopProfiler::new()` 各累计为 0；
- 喂 `add_poll(d)/add_relay(d)/add_park(d)` + `wall=W` → `poll_fraction = poll/W`、`relay_fraction`、
  `loop_active_fraction = 1 − park/W`（clamp [0,1]，防时钟抖动 wall<park）；
- `report_and_reset()` 返回快照后**归零**累计、刷新周期起点（下周期独立）；
- wall=0 / 累计=0 边界不 panic、不除零（返回 0 或 None，测定行为）。

**绿**：实现 `LoopProfiler`（持 `Instant` 周期起点 + 三 `Duration` 累计 + iter 计数）。
**commit**：`feat(knife12): LoopProfiler fraction math (T1, pure unit-tested)` → push。

---

## T2 — `MetricsSink` 扩接缝 + 主循环接入（零开销不破）

**红**：
- `MetricsSink` trait 加 default-空方法 `loop_park_begin/loop_park_end/report`（`NoopSink` 自动空）；
- 单测：`LoopProfiler` 经 trait 调用序列 `park_begin → (sleep) → park_end → enter_poll/leave_poll …
  → report` 后 fraction 合理（park 计入空等、poll 计入段内）；
- 编译期保证 `NoopSink` 仍零字段、方法空（`size_of::<NoopSink>()==0`，断言）。

**绿**：
- `run_event_loop`：循环底部调 `metrics.loop_park_begin()`（开始 park）；
  **每个 select! arm 首行**调 `metrics.loop_park_end()`（8 arm：global_rx / wait_for_rx /
  handshake_done_rx / tuic_downlink_rx / udp_sweep / fake_ip_sweep / metrics_tick / timer）；
- `metrics_tick` arm 内 `metrics.report()`（紧挨刀11 `📊` 行）；
- 既有 `enter_poll/leave_poll/enter_relay/leave_relay` 不动。

**核验**：default `NoopSink` 路径——所有新方法空、无 `Instant::now()`；既有 193 测全绿（零行为变化）。
**commit**：`feat(knife12): park/report seams + loop instrumentation (T2)` → push。

---

## T3 — env 门控 + 装配 + `🔬` 行

**红**：
- `parse_profile_loop(Option<&str>) -> bool`（`"1"`/`"true"` 不区分大小写 → true；默认/非法 → false）纯单测；
- `format_loop_profile(&snapshot)` 输出 `🔬 主循环: loop-active=..% | poll=..% relay=..% | park=..% |
  iters=../wall=..ms` 的格式单测（一位小数、边界值）。

**绿**：
- `TunRuntimeConfig` 加 `profile_loop: bool`；`from_env` 读 `MINI_VPN_PROFILE_LOOP`、`from_sources` 恒 false；
- `start_tun_proxy` 三上游 arm：`if cfg.profile_loop { run_event_loop(.., LoopProfiler::new()) } else {
  run_event_loop(.., NoopSink) }`（分支单态化，默认零开销）；
- 开启时启动声明 `🔬 主循环 profiler 已启用（每 {metrics_secs}s）`。

**commit**：`feat(knife12): MINI_VPN_PROFILE_LOOP gate + 🔬 line (T3)` → push。

---

## T4 — harness 接入 + 多核就绪 spike

**红**：
- harness 暴露三 fraction（`Report` 加字段，或 `RecordingSink` 也实现新接缝产出 fraction）；
- `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]` + 注入**可调合成 per-packet CPU 成本**
  （env/参数控的 busy-loop，挂在 mock relay 或 poll 路径）：
  - 断言 **注入成本↑ → loop-active-fraction 单调↑、趋近饱和**（仪器正确性 + 能造单核饱和信号）；
  - 对照 0 成本基线：loop-active 低（主循环多在 park）。

**绿**：实现注入点（feature/env 门控，**不污染生产热路径**）+ harness fraction 读出。
**边界**：明确**不**建 per-shard gate；harness throughput 仍不可信、只断言 fraction 趋势。
**commit**：`test(knife12): harness multi-thread CPU-saturation spike validates loop-active (T4)` → push。

---

## T5 — acceptance 配方 + 真出口 + ADR + 文档

**配方**（写进 findings 续篇 / 复用 `scripts/knife35-acceptance.sh soak`）：
- 起客户端 `sudo -E MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5 ... ./target/release/mini_vpn client-tun`；
- **Probe ①（poll-fraction，归因 #4 vs #3）**：≥100M 链路上推大流量（bulk TCP 下载 + 并行 iperf3 推到
  80–100M），读 `🔬` 的 loop-active/poll fraction；同时 `top -H -pid <pid>` / `sample <pid>` 交叉验证
  run_event_loop 线程 CPU%。判据见 spec §3.2。
- **Probe ②（单连接 CC scaling，证实/证伪 #3）**：1 vs 2 vs 4 并行 flow over 单 QUIC（TCP 或 native UDP），
  读 `start_udp` 30s `📊` 的 cwnd/rtt + 聚合吞吐。聚合随 flow 数**不涨**（卡 ~34-40M）⇒ #3；**线性涨** ⇒
  墙在别处（poll 或 endpoint）。

**ADR**（`docs/adr/0013-*.md`）：记裁决（#4 还是 #3 先到墙）+ 刀13 干预建议 + 路线 (a) 三残留单核点。
**文档**：findings 末节加「刀12」量化结果；HANDOFF 更新「下一刀=刀13（按裁决选连接池/分片）」。
**commit**：`docs(knife12): real-egress attribution + ADR-0013 + HANDOFF (T5)`。

> acceptance 尽力而为、如实记录：若链路 cap 与 #3 缠绕导致裁决模棱，记「未达可信裁决」+ 补测条件
> （诚实优先）。代码部分（T1-T4）的质量门与零回归是**硬门**，必须全过再进 T5。

---

## 收尾（rhythm）

- `/code-review` over diff + 对抗式核验（profiler 零开销不变 / fraction 数学 / 装配分支正确 / 插桩不破不变量）。
- 真出口 acceptance（T5）。
- 一个分支一个 writer；每 commit 后 push。

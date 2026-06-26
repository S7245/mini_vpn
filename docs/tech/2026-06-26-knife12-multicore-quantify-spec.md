# 刀12 spec — 多核逼近 100M：量化定位（quantify-only）

> 分支 `claude/knife12-multicore-100m`（从 main `460a349` 起，刀11 已合）。一刀一 session。
> 配套 plan：同目录 `2026-06-26-knife12-multicore-quantify-plan.md`。
> 北极星：`Rules.md` ③ 大并发 / 高带宽逼近 100M（主战场）。

## 0. 一句话

**本刀只做量化定位，不做架构干预。** 交付一台 #4-vs-#3 判决仪器（`LoopProfiler`：主循环
poll / relay / loop-active 三段 wall-fraction，env 门控、生产+harness 同源），在 **≥100M 真出口链路**
上跑两个 probe 归因「100M 的墙到底是单核 smoltcp poll（#4）还是单条 QUIC 连接的 CC（#3）」，
落 ADR 记裁决。**真正的干预（事件循环分片 / QUIC 连接池）留刀13，按本刀裁决选。**

## 1. 为什么是「先量化」而不是「直接上多核」

### 1.1 瓶颈模型（代码已坐实的部分）

- `run_event_loop`（`src/client_tun.rs:569`）是**一个** tokio task，独占无锁持全部数据面状态
  （`sockets`/`iface`/`registry`/`socket_ctxs`/`dirty`/`fake_pool`/`assoc_table`/`reassembler`/`device`，
  `client_tun.rs:595-642`）。
- per-flow relay 字节泵**已 spawn 出主循环**（`spawn_remote_relay → tokio::spawn(run_relay)`，
  `client_tun.rs:1476`）；`start_udp` 下行泵也是独立 task；生产是裸 `#[tokio::main]` = **多核工作窃取
  runtime**（`src/main.rs:4`）。
- ⇒ 残余单核串行点**精确地**是这一个 `run_event_loop` task——一个 task 永不能与自己并行。
  其 `iface.poll`（`client_tun.rs:753,846`）是 smoltcp 单线程 socket 迭代，刀2 砍掉 #1 全扫后
  **瓶颈已从 relay 段转移到 poll 段**（findings 刀2 节：N=1024 relay 1618ms→70.8ms，poll 段
  ~1030ms 成主导）。**#4（单核中央循环）作为「架构事实」成立。**

### 1.2 但「#4 就是 100M 的墙」是假设、未实测

- 全仓**没有任何 benchmark 在 80–100M offered 负载下隔离过 run_event_loop 的 poll-CPU 占比**。
- 我们手上**唯一的真出口硬数据反而指向 #3**：
  - native datagram 单链路 **~40M / 0.25%**（80M 链路上没跑满；ADR-0005 / findings 刀3.5）；
  - 双 flow 单连接聚合 **~34M**（没随 flow 数线性涨 → 单 cubic 拥塞控制器是墙；findings 刀3.5 T-E）；
  - 单条 QUIC 连接 + 单 UDP socket + 单 quinn `Endpoint`（`src/quic.rs:142`）。
- **这是刀3.5 剧本的重演风险**：当时「5.3M datagram 天花板」被插桩揭穿是 5M 链路 cap 假象
  （ADR-0005「Context correction」）。现在「单核 poll 天花板」同样未证，证据更偏 #3。
- 项目铁律 **「先量化、别凭猜改」**（HANDOFF Rhythm / MEMORY priority-stability-over-elegance）：
  对未证瓶颈做极大改造（路线 a 分片要复制全状态图 N 份 + 前置 demux + TX 串行 + 共享 fake_pool +
  全局 assoc_id 协调）是典型的凭猜改。**先量化归因，刀13 再单点干预。**

### 1.3 路线裁决（为何 quantify-only）

| 路线 | 治 | 可行性 | 工作量 | 本刀取舍 |
|---|---|---|---|---|
| **(d) 先 profile 归因** | 解锁其它 | 高 | 小 | ✅ **本刀** |
| (a) 事件循环分片 | #4 | 中 | 极大 | 留刀13（若 #4 裁决成立） |
| (b) 只卸载 poll 段 | #4 | **被堵死** | — | 否决：smoltcp `Interface` 是 `!Sync` 单属主、无 shared-poll API，给第二线程有用 poll 活=复制全状态图=等于 (a) |
| (c) QUIC 连接池 | #3 | 中 | 大 | 留刀13（若 #3 裁决成立；改动内聚 `TuicUpstream`、不碰无锁主循环、风险更低） |

(a) 分片即便做了仍剩**三个吃掉收益的单核串行点**（见 §5 风险）：单 utun fd 读（macOS 无 multi-queue，
tun 0.6 硬拒）、单 utun fd 写（并发 write 原子性未证）、`fake_pool`（DNS alloc 与 SYN resolve 天然落
不同 shard，无法按 flow 分区）。**没量化证明 poll 是墙之前，不值得赌这个改造。**

## 2. 本刀范围（grill 已拍板）

- **范围 = 量化-only + ADR 定瓶颈**（grill Q1）。
- **压测链路 = ≥100M 真出口可用**（grill Q2）——能真正观察到 100M 处客户端 CPU 是否饱和、吞吐是否卡 #3。
- **不做**：任何事件循环分片 / 连接池 / 热路径行为改动。`LoopProfiler` 是 opt-in，**默认 `NoopSink`
  逐字不变、零开销**（无 `Instant::now()` 进热路径）。

## 3. 判决仪器：`LoopProfiler`

### 3.1 测什么（三个 wall-fraction，每报告周期）

| 指标 | 定义 | 读法 |
|---|---|---|
| **poll-fraction** | `Σ(leave_poll−enter_poll) / wall` | smoltcp poll(+flush_tx) 占主循环 wall 的比例 |
| **relay-fraction** | `Σ(leave_relay−enter_relay) / wall` | relay 调度段占比（刀2 后应很小） |
| **loop-active-fraction** | `1 − park_time/wall` | 主循环**非 park**（在 select! 空等之外）占 wall 的比例 |

- `park_time` = 主循环停在 `tokio::select!` 等下一个事件就绪的时间（无事可做的空等）。
- `loop-active` 包含 arm body 内的 `.await`（如 `flush_tx().await`、`handle_remote_payload().await`），
  因为只要循环卡在 body 里就处理不了其它包——这正是「中央 task 是否串行瓶颈」要测的 wall 占用。

### 3.2 判决逻辑（#4 vs #3）

- **loop-active → ~100% 且 poll-fraction 占大头**：主循环 CPU/IO 饱和、卡在 smoltcp ⇒ **#4 是墙**
  → 刀13 上**路线 (a) 分片**。
- **loop-active 低（如 <40%）而吞吐封顶 ~40M**：主循环大量时间 park 在 select! 空等上游
  （QUIC 流控/下行没来）⇒ **#3 是墙** → 刀13 上**路线 (c) 连接池**。
- **loop-active 高但 poll-fraction 不占大头**：某个非-poll arm（DNS/UDP/downlink）吃 CPU ⇒ 仍是
  #4 家族（中央 task 串行），但干预点不同——如实记录、细化归因。

### 3.3 实现接缝（不破 NoopSink 零开销）

扩 `MetricsSink` trait（`client_tun.rs:38`），**新增方法全部 default 空实现**（`NoopSink` 单态化后零开销）：

```
fn loop_park_begin(&mut self) {}   // 主循环底部：开始 park（一处）
fn loop_park_end(&mut self) {}     // 每个 select! arm 首行：结束 park、开始 active（8 处）
fn report(&mut self) {}            // metrics_tick arm：输出 🔬 行 + 重置周期累计（一处）
```

- `poll`/`relay` 复用既有 `enter_poll/leave_poll`、`enter_relay/leave_relay`（不新增）。
- `LoopProfiler` 内部持 `Instant` 时钟 + 各段累计 `Duration` + 周期起点；`report` 算 fraction、打印、重置。
- **零开销保证**：`NoopSink` 的新方法空实现，`loop_park_begin/end` 不调 `Instant::now()`；只有
  `LoopProfiler`（env 开启时单态化选中）才采时钟。生产默认走 `NoopSink` 分支（见 §3.4），逐字零回归。

### 3.4 env 门控 + 装配

- 新 env `MINI_VPN_PROFILE_LOOP`（`1`/`true` → 开；默认/其它 → 关）。`from_env` 读、`from_sources`
  （harness/测试）恒关——与 `MINI_VPN_METRICS_SECS` 同款纪律（`client_tun.rs:384`）。
- **装配**：`start_tun_proxy` 三个上游 arm 按 env 在 `NoopSink` 与 `LoopProfiler` 间**二选一传入
  `run_event_loop`**（沿用 upstream `match` 的分支单态化惯用法，`client_tun.rs:461/486/533`）。
  默认 `NoopSink` 路径真零开销不变。
- 报告节拍复用 `MINI_VPN_METRICS_SECS`（acceptance 设 5 即秒级看）；`report` 在 `metrics_tick` arm
  调一次（紧挨刀11 的 `📊` 行；两行各司其职、`🔬` 仅 profile 开启时出）。
- 启动声明：开启时打 `🔬 主循环 profiler 已启用（poll/relay/loop-active 占比，每 Ns）`。

### 3.5 `🔬` 行格式（草案）

```
🔬 主循环: loop-active=<pct>% | poll=<pct>% relay=<pct>% | park=<pct>% | iters=<n>/wall=<ms>ms
```

- `pct` 一位小数；`iters` = 周期内 select! 迭代数（旁证负载强度）；`wall` = 实测周期 wall（非名义 Ns，挡 interval 漂移）。

## 4. harness：仪器自检 + 多核就绪 spike（不建分片 gate）

- **接入**：harness 跑 `LoopProfiler`（或令 `RecordingSink` 也产出三 fraction），使段占比在 harness 可读。
- **就绪 spike**：一个 `#[tokio::test(flavor = "multi_thread", worker_threads = N)]` 测，在 relay/poll
  路径注入**可调合成 per-packet CPU 成本**（busy-loop / mock 加密代理），证明：
  - loop-active 信号在已知 CPU-bound 合成负载下 → 趋近饱和（**仪器正确性**）；
  - harness **能造出单核饱和信号**（为刀13 的真分片 gate 铺路）。
- **明确不做**：per-shard `RecordingSink`、分片 SUT、真分片回归 gate——那是刀13 随干预一起建（避免为
  未选定路线预建错 gate）。harness wall/throughput 仍**不可信**（`sleep(200µs)` 节拍污染，findings 刀1
  「harness 局限」）——本刀只信**段 CPU 计时 / fraction**，不拿 harness throughput 当 100M 进度。

## 5. 风险 / 已知边界

- **R1 线程归属**：多核 runtime 下 `run_event_loop` task 可在 worker 间迁移，`top -H` 的「单线程
  CPU%」不严格对应该 task。**`LoopProfiler` 的 in-process loop-active 不受此影响**（量的是 task 自身
  body 占用，与落在哪个 worker 无关）；OS 线程 CPU% 仅作交叉验证、acceptance 如实标注其局限。
- **R2 profiler 自身开销**：开启时每段 2× `Instant::now()`（macOS vDSO 快）；~8k pkt/s 下可忽略，但
  **开启即引入测量扰动**——归因读「趋势/比例」而非绝对值（同 findings 纪律）。默认关、零扰动。
- **R3 链路仍可能是新墙**：≥100M 链路若实际只有 ~100M，100M 处链路 cap 与 #3 难分——probe 设计需
  在**链路明显高于观测吞吐**时读 CPU 饱和信号（同 ADR-0005「先量化」教训）。
- **R4 quantify-only 不直接提吞吐**：本刀**不改善** Rules ③，只交付裁决与仪器。验收口径据此定（§6）。
- **R5 路线 (a) 的三个残留单核点（留刀13 评估，非本刀）**：单 utun fd 读 / 写 / 共享 `fake_pool`
  ——本刀 ADR 记录它们为「若裁决 #4、刀13 必须先解的子问题」。

## 6. 验收口径（quantify-only 的「达标」）

本刀**不以吞吐数字达标**，以「**判决可信 + 仪器就位**」达标：

1. **质量门**：lib+harness 全绿、`clippy --all-targets --features harness` 0 warning、release 绿；
   **默认 NoopSink 路径零回归**（既有 193 测不动 + 新增 profiler 单测）。
2. **仪器正确性**：harness 合成 CPU-bound 负载下 loop-active fraction 随注入成本单调上升、趋近饱和
   （单测/集成测断言）。
3. **真出口归因**（≥100M 链路，尽力而为如实记录）：跑 §3.2 两 probe，得出 **#4 还是 #3 先到墙**的
   明确裁决（含 `🔬` 数据 + cwnd/rtt + 聚合吞吐 + 线程 CPU% 交叉验证）。
4. **ADR 落盘**：裁决 + 刀13 干预建议（#3→连接池 / #4→分片）+ 路线 (a) 三残留点记录。findings 续篇 +
   HANDOFF 更新「下一刀」。

> 注：若真出口数据**模棱**（如链路 cap 与 #3 缠绕、CPU 未饱和也未明显 park），如实记录为「未达可信裁决」
> 并列出补测条件——不强行下结论（诚实优先于漂亮）。

## 7. 不在本刀（defer）

- 事件循环分片（路线 a）、QUIC 连接池（路线 c）、per-shard harness gate、单 fd 多核 demux/TX、
  `fake_pool` 跨 shard 协调、assoc_id 全局协调——**全部刀13**，按本刀裁决选其一。
- 多 endpoint / 多 UDP socket 拓扑评估（路线 c 子问题）——刀13 若选连接池再定。

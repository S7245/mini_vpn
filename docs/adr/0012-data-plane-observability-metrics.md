# 刀11：数据面可观测性 —— 独立 `Arc<Metrics>` 接缝 + counter/published-gauge 拆分 + 背压上升沿

数据面此前「看不见」：DNS forge 无计数、UDP 下行 drop 与 datagram 背压静默、TCP 活跃连接 / fake-IP 池用量 /
failover 当班腿无聚合。刀11 把这些变成**进程级原子计数 + 30s 周期快照**，为后续「多线程逼近 100M」铺量化底座
（量化先行、别凭猜改——刀3/3.5 已印证「插桩揭穿假象」）。

决策日期 2026-06-26（刀11，分支 `claude/knife11-observability`）。设计经 grounding workflow（5 接缝并行核实）+
设计综合（5 开放问题逐一裁决）。配套 spec/plan：`docs/tech/2026-06-26-knife11-observability-{spec,plan}.md`。

## 决策

### 1. 独立 `Arc<Metrics>`，**不**扩 `MetricsSink`（为何第二个 metrics 接缝）
数据面是**两个 task** 各持状态：① `run_event_loop`（单 task、独占无锁 `socket_ctxs`/`fake_pool`）；
② `TuicUpstream::start_udp` spawn 的下行/统计 task（持 `conn` + `udp_drops` 等原子 + 既有 30s `📊`）。
- `MetricsSink`（knife1）是**逐段计时**接缝（`enter_poll/leave_poll/…`），生产 `NoopSink` 单态化后**零开销**。
  扩它装累计计数器会让 NoopSink 长出真状态、**丢掉零开销承诺**；且 `MetricsSink` 只活在 run_event_loop task，
  **结构性看不见 TuicUpstream 的 UDP 原子**。
- → 新 `Arc<Metrics>`（各字段 `Atomic*`）是**唯一**能被两 task 各 clone 一份、无锁桥接的载体，正是既有
  `TuicUpstream.udp_drops:AtomicU64` 模式的推广。`MetricsSink` 原样保留（计时正交，二者并存）。

### 2. counter（fetch_add+load）vs published-gauge（loop 重算+store，snapshot load）
- **累计 counter**（`dns_forged/dns_dropped/udp_drops_down/datagram_pressure_events/relays_spawned`）：事件点
  `fetch_add(Relaxed)`，`snapshot()` 仅 `load`。
- **瞬时 gauge**（`active_relays/fake_ip_active/fake_ip_total/failover_leg`）来自 run_event_loop **单写者独占状态**——
  `socket_ctxs`/`fake_pool` 无锁、**严禁套 `Arc<Mutex>` 给别的 task 读**（会把锁压进每包/每 relay 热路径）。
  → loop 在 **30s tick 重算**（`socket_ctxs.values().filter(Relaying).count()`、`fake_pool.usage()`、
  `upstream.failover_leg_u8()`）后 `store(Relaxed)` 进**同一 `Arc<Metrics>`**；`snapshot()` 读最新已发布值。
  **O(n) 扫描严格限 30s tick，绝不进每包热路径**；snapshot 全 `load`、O(1)、任意 task 可调。
- **上行 `udp_drops`/`udp_stream_fallbacks` 不迁移**（保留 `TuicUpstream`、零回归——它们被 start_udp 门控逻辑读，
  迁移要动既有证过的计数点），snapshot 经 trait 访问器读出。

### 3. 背压「事件」= false→true 上升沿计「集」
`is_datagram_pressured`(space<mtu) 是 **level** 信号；每 30s tick 为真就 ++ 会把一段持续背压重复计数（无意义膨胀）。
→ start_udp task 内持**普通局部 `prev_pressured:bool`**（单 task、无需原子），仅 `pressured && !prev` 时
`datagram_pressure_events++` → counts distinct episodes。enter/exit 双沿被否（混淆「进入」与「恢复」语义）。

### 4. failover leg 经 trait 默认方法采样，degrade gracefully（不破 send_udp 铁律）
给 `ProxyUpstream` 加默认 `failover_leg_u8(&self)->u8 { NO_FAILOVER }`（同构既有 `open_is_cheap` 默认方法），
仅 `FailoverUpstream` override 返回 `state().active_leg().as_u8()`。纯 TUIC / 纯 REALITY 继承哨兵 `NO_FAILOVER`(255)。
- 读发生在 run_event_loop 的 30s tick（`upstream:Arc<U>` 在 scope）= **独立周期 Relaxed 读、不在 UDP 数据路径**
  → 不破 ADR-0011 铁律（`FailoverUpstream::send_udp` 仍无条件转发 tuic、不读 leg）。

### 5. 统一 `📊` 快照行由 run_event_loop 发（不在 start_udp）
- run_event_loop **永远存在**（start_udp 仅 UDP relay 启动后才有；纯 REALITY 模式无 start_udp）→ 让 loop 发快照，
  纯 REALITY / UDP 空闲下仍有可观测输出。
- 既有 start_udp 的 UDP-path `📊` 行（RTT/cwnd/datagram 背压，门控 `udp_active||fb>0||drops>0`）**原样保留**——
  其门控对 UDP-specific 统计正确，但对 TCP/DNS/failover 不适（UDP 空闲时这些仍有意义）。
- → **两行日志、各司其职**；统一快照行**无门控**。两个独立 30s interval 轻微漂移，advisory 指标 benign。

### 6. 前端契约边界 + 不引入外部依赖
本刀只导出 `MetricsSnapshot`（纯值 `Copy` struct、pub 字段）+ `snapshot()` + `📊` 日志。**不建 IPC/local-control 读通道**
（前端 session 主导，契约先行——见 sibling 仓 `mini_vpn_app`）。**无 prometheus/metrics/serde**（serde 派生留前端按需加，
struct 形状即契约；与 ADR-0003 单 rustls / 「external store 不进数据面热路径」一致）。

## Considered / Rejected
- **扩 `MetricsSink` + 加 `snapshot()`**：毁 NoopSink 零开销单态化；M 看不见 start_udp 的 UDP 原子（结构性）→ 否决（§1）。
- **全塞 `TuicUpstream`（如 udp_drops）**：纯 REALITY 路径无 TuicUpstream；TCP/DNS/fake-IP/failover 不归它管 → 否决。
- **gauge 做真 +1/-1 活计数**（spawn 处++、rearm 处--）：每个 spawn 站须配齐每个 teardown 站（EOF/Refuse/Block/握手失败/reap）
  否则漂移；周期重算自纠错、远更稳（稳定优先）→ 否决活计数、取重算（§2）。
- **`Arc<Mutex<MetricsSnapshot>>` 每 tick 写**：在本无锁处引入锁竞争 → 取 atomics + 每 tick `store`（§2）。
- **背压每 tick 计 / 双沿计**：重复计一段持续背压 / 语义混淆 → 取上升沿单沿（§3）。
- **start_udp 发统一快照**：纯 REALITY 无此 task、且 UDP 门控会让 TCP/DNS 在 UDP 空闲时变暗 → 改 run_event_loop 发（§5）。

## Consequences
- 新 `src/metrics.rs`；`run_event_loop` 加 `metrics_handle:Arc<Metrics>` 参（6 调用点）；`TuicUpstream` 加 `metrics` 字段；
  `ProxyUpstream`/`DatagramUpstream` 各加默认观测方法；`TcpLeg::as_u8` 改 `pub(crate)`；`FakeIpPool::usage()` 新访问器。
- `forge_dns_reply` 保持纯（计数在 caller `handle_dns_hijack`）→ 既有 4 单测零改动。
- NODATA（AAAA/other）按 `Some=forge` 计入 `dns_forged`；如需区分留 `dns_nodata`（defer，会破纯性）。
- UDP 下行 drop / 背压计数的 I/O 触发点（真 quinn）harness mock 不覆盖 → 归 tuic 单元（沿 helper）+ 真出口 acceptance（尽力而为）。
- 不破 ADR-0003（单 rustls）/0004（TUIC 数据面）/0011（failover 铁律：send_udp 不读 leg）。本刀为「多线程逼近 100M」量化底座。

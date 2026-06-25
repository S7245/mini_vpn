# 刀11 — 数据面可观测性（observability）seed / scope

> 日期：2026-06-26 ｜ 状态：**grill 已完成，待新 session 冷启动接力（ground → spec → TDD → review → acceptance）**
> 起点：从 `main`（`927e009`，REALITY 刀6→10 收官后）起新分支，新 session（one knife per session）。
> 原则：系统稳定 > 代码漂亮；**先量化、别凭猜改**（本刀正是为后续多线程/100M 性能工作铺量化底座）。

## 0. 为什么是这一刀

主线下一候选有「数据面多线程逼近 100M」与「observability」，用户选 **observability**——小而稳、低风险，让后续性能/多线程工作可量化（量化先行是项目铁律，刀3.5 已印证「先量化别凭猜改」）。

## 1. 范围（grill 裁决，2026-06-26）

- **广度 = 路线图两项 + 统一数据面快照**（用户选「+ 数据面快照」）：
  1. **DNS forge 计数**（当前**无计数器**，是真缺口）。
  2. **datagram drop / 背压可见性**（uplink `udp_drops` 已有；补下行 drop + 背压事件）。
  3. **统一数据面快照**：TCP 活跃连接 / relay 数 + fake-IP 池用量 + failover 当班腿。
- **接口 = 结构化快照 + 周期日志**（用户选「结构化快照 + 周期日志」）：
  - 一个 `MetricsSnapshot` struct（原子计数快照）。
  - **既**喂现有 30s `📊` 周期日志（人读），**又**为未来 `mini_vpn_app` 前端预留**可读契约**（契约先行；core 仓只导出数据，不碰 GUI/backend）。

## 2. 指标清单（来源接缝已核实）

| 指标 | 来源接缝（file:symbol） | 现状 |
|---|---|---|
| DNS forge 成功数 / 丢弃数 | `client_tun.rs:992 forge_dns_reply`（`Some`=forge / `None`=drop）、`:1018 handle_dns_hijack` | **无计数**，需加 |
| UDP datagram uplink 丢弃 | `tuic.rs:671 udp_drops: AtomicU64` + `:929 udp_drop_count()` | 已有（uplink） |
| UDP datagram 下行丢弃 / accept 背压 | `tuic.rs` accept_uni Semaphore（256）、decode 失败点 | 部分隐式，需显式计数 |
| datagram 背压 | `tuic.rs:621 is_datagram_pressured`、`datagram_send_buffer_space`、`:629 format_udp_stats` | 已有信号，需计数/暴露 |
| TCP 活跃连接 / relay 数 | `client_tun.rs socket_ctxs: HashMap<SocketHandle,SocketCtx>`、`spawn_remote_relay`、`MetricsSink::note_listeners` | 部分（note_listeners 仅 listener 数），需活跃 relay 计数 |
| fake-IP 池用量 | `fake_ip.rs:34 FakeIpPool`（`refcount` per 映射；**无 len/usage 访问器**） | 需加 `usage()`（映射总数 / refcount>0 活跃数） |
| failover 当班腿 | `failover.rs:136 active_leg() -> TcpLeg{Tuic,Reality}` | **已可读**，直接采样 |

## 3. 接口设计草案（新 session 在 spec 里定稿）

- 新 `MetricsSnapshot { dns_forged, dns_dropped, udp_drops_up, udp_drops_down, datagram_pressure_events, active_relays, fake_ip_active, fake_ip_total, failover_leg, ... }`（字段以 §2 为准）。
- 背后存储：进程级 `Arc<Metrics>`（各字段 `AtomicU64`/`AtomicU8`），热路径 `fetch_add(Relaxed)`，`snapshot()` 原子读出为 `MetricsSnapshot`（纯值、可序列化、可跨 FFI/契约）。
- 消费两路：① 现有 30s `📊` 周期日志扩一行/合并（`format_*_stats` 风格）；② 暴露 `snapshot()` 供前端契约（暂只导出值，前端读取通道留前端 session）。
- **不引入外部依赖**（无 prometheus/metrics crate）——与「proven solutions over hand-rolled，但 external store 不进数据面热路径」一致（见 [[prefer-mature-frameworks-and-external-stores]]）；原子计数足矣。

## 4. 开放设计问题（新 session spec 阶段拍板）

1. **`Metrics` 聚合归属**：单一 `Arc<Metrics>` 注入 `run_event_loop` + `TuicUpstream` + `FailoverState` + fake-IP/DNS 路径？还是扩 `MetricsSink` trait（现为计时导向 `enter_poll/leave_poll/enter_relay/note_listeners`，加域计数回调）？倾向**独立 `Arc<Metrics>`**（MetricsSink 是 per-call 计时，不适合长期累计计数；但二者可并存）。
2. **快照采样 vs 累计**：计数器（forge/drop）是单调累计；当班腿/池用量/活跃 relay 是**瞬时 gauge**——snapshot 里 gauge 如何取（实时读 vs 周期采样）。
3. **背压「事件」定义**：`is_datagram_pressured` 为真的次数？还是进入/退出背压的沿？避免每 tick 重复计数。
4. **前端契约边界**：本刀只导出 `snapshot()` 值，前端读取通道（local-control/IPC）是否本刀做，还是留前端 session（倾向**留前端**，core 只产数据，契约先行）。
5. **热路径开销**：fetch_add(Relaxed) 可接受；避免 snapshot 在热路径调用（仅周期/按需）。

## 5. TDD 草案（新 session 细化）

- **[单元]** `Metrics::snapshot()` 原子读出与各 `fetch_add` 一致；并发 fetch_add 不丢计数。
- **[单元]** `forge_dns_reply` 返回 `Some`→forge++、`None`→drop++（注入计数后断言）。
- **[单元]** `FakeIpPool::usage()` 返回 (total, active=refcount>0)；alloc/acquire/release/sweep 后数值正确。
- **[loopback/harness]** 跑 `run_event_loop`（mock 上游）驱若干 TCP/UDP/DNS 流，断言 snapshot 各计数随之增长（复用 knife1 harness + `RecordingSink` 模式）。
- **[真出口 acceptance]** 跑真隧道一段，`📊` 日志含新指标且数值随负载变化（forge 数随 DNS 查询、drop/背压随大流量、failover_leg 随切换）。

## 6. 质量门（同项目惯例）

lib+harness 测全绿 ｜ `clippy --all-targets --features harness` 0 warning ｜ release 绿 ｜ 每任务红→绿→commit→**push** ｜ `/code-review` + 对抗式核验 ｜ acceptance 如实记录。

## 7. 冷启动指引（新 session）

读 `Rules.md`、`HANDOFF.md`（本刀指针）、本 seed；ground 在 §2 的接缝（先读 `client_tun.rs run_event_loop`/`fake_ip.rs`/`tuic.rs format_udp_stats`/`failover.rs active_leg`）；按 §4 拍板 spec；按 §5 TDD。一个分支一个 writer，cwd 每条 git/cargo 用绝对路径确认在本 worktree。中文回复（代码/术语/commit 英文）。

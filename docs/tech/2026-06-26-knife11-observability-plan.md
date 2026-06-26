# 刀11 — 数据面可观测性 plan / TDD 分解

> 配套 spec：`2026-06-26-knife11-observability-spec.md`、ADR `docs/adr/0012-data-plane-observability-metrics.md`。
> 分支 `claude/knife11-observability`(从 main `6ba6d42` 起)。每个 Task：写失败测试 → red → 实现 → green →
> `cargo test` + `cargo test --features harness` + `cargo clippy --all-targets --features harness` 绿 → commit → **`git push`**。
> 一个分支一个 writer。**先纯叶子（T1–T3）**，再接线（T4 DNS + run_event_loop param）、loop gauge（T5）、UDP 计数（T6），收尾（T9）。
> **cwd 陷阱**：每条 git/cargo 前确认在本 worktree（`git branch --show-current`=`claude/knife11-observability`），用绝对路径。

## 决策溯源（grounding + 设计综合 workflow，2026-06-26，详见 ADR-0012）

| Q（seed §4） | 决策 | 依据 |
|---|---|---|
| 聚合归属 | **独立 `Arc<Metrics>`**，不扩 MetricsSink | MetricsSink 计时导向 + 只在 loop task；Arc<Metrics> 唯一能桥两 task |
| counter vs gauge | counter `fetch_add`+`load`；gauge loop 30s 重算 `store`、snapshot `load` | socket_ctxs/fake_pool 单写者不能跨 task 读 |
| 背压「事件」 | **false→true 上升沿**（局部 prev latch） | level 信号每 tick++ 会重复计一段持续背压 |
| 前端契约边界 | 只导出 snapshot()/struct，不建 IPC | 契约先行，读通道留前端 session |
| 快照发射 owner | **run_event_loop**（永远存在 + 独占 gauge），start_udp UDP 行原样保留 | 纯 REALITY 无 start_udp；UDP 行门控对 UDP 正确、对 TCP/DNS 不适 |

## 执行顺序与依赖

```
T0(spec/plan/ADR-0012 落库 + CONTEXT 词汇)
 ├─ T1 metrics.rs（Metrics/MetricsSnapshot/snapshot/format）──┐
 ├─ T2 FakeIpPool::usage() ─────────────────────────────────┤
 └─ T3 trait 访问器 + TcpLeg::as_u8 pub(crate) ─────────────┤
                                                            └─→ T4 DNS 计数 + run_event_loop metrics_handle param(6 站)
                                                                  └─→ T5 relays_spawned + 3 gauge + 30s tick + 快照发射
                                                                  └─→ T6 UDP 下行 drop + 背压上升沿（TuicUpstream 字段）
                                                                        └─→ T9 收尾（code-review + 对抗式 + acceptance）
```
T1/T2/T3 互相独立（纯叶子，任意序）；T4 引入 `metrics_handle` param（叶子 T1 就位后）；T5/T6 均建于 T4 的 param/Arc，可任意序（T5 先发快照即便 T6 未接 → udp_drops_down 暂 0，增量正确）。

## Task 0 — spec/plan/ADR-0012 落库 ✅（本 commit）

`docs(knife11): spec + plan + ADR-0012 for data-plane observability (T0)`。含 CONTEXT.md 词汇（`Metrics`/`MetricsSnapshot` 接缝、counter vs published-gauge、failover leg 哨兵）。

## Task 1 — `src/metrics.rs` 模块（TDD，核心叶子）

- **red**：新 `tests`(模块内) ——
  - `Metrics::new()` 全 0、`failover_leg`=`NO_FAILOVER`；各 `inc_*` 后 `snapshot()` 对应字段 +1。
  - **并发不丢**：spawn N=8 task 各 `inc_dns_forged()` K=10000 次 join 后 `snapshot().dns_forged==80000`。
  - gauge：`set_active_relays/ set_fake_ip/ set_failover_leg` 后 snapshot 反映；`failover_leg` u8→`FailoverLegView`（0→Tuic/1→Reality/255→None/其它→None）。
  - `snapshot(up,fb)` 把传入的 `udp_drops_up/udp_stream_fallbacks` 原样落字段。
  - `format_metrics_snapshot` 输出含全字段标签（断言含 `dns_forged=`/`active_relays=`/`leg=` 等关键子串）。
- **green**：实现 `Metrics`(atomics)+`MetricsSnapshot`(纯值 Copy)+`FailoverLegView`+`NO_FAILOVER`+`new/snapshot`+`inc_*/set_*` 薄方法 + `format_metrics_snapshot`(纯)。`lib.rs` 加 `pub mod metrics;`。不接任何线。
- commit：`feat(knife11): metrics.rs — Metrics atomics + MetricsSnapshot contract + format (T1)`。

## Task 2 — `FakeIpPool::usage()`（TDD，纯叶子）

- **red**：`fake_ip.rs` test —— 空池 `(0,0)`；`alloc("a",t)` 后 `(1,0)`（alloc 不改 refcount）；`acquire(ip,t)` 后 `(1,1)`；再 `alloc("b")` `(2,1)`；`release(ip)` 归零后 `(2,0)`；`sweep(t+TTL+1,TTL)` 回收 idle 后 total 降。
- **green**：`usage(&self)->(usize,usize)`（紧邻 `resolve`，spec C2）：`total=ip_to_mapping.len()`、`active=values().filter(refcount>0).count()`。只读、不触 last_used、不分配。
- commit：`feat(knife11): FakeIpPool::usage() (total, active) accessor (T2)`。

## Task 3 — upstream trait 观测访问器 + `TcpLeg::as_u8` pub(crate)（TDD）

- **red**：
  - `failover.rs` test：mock `T`/`R` 上游构 `FailoverUpstream`，`state().set_leg(Reality)` 后 `failover_leg_u8()==1`、`set_leg(Tuic)` 后 `==0`；mock tuic 腿 override `udp_drops_up()` 返定值 → `FailoverUpstream::udp_drops_up()` 转发一致。
  - 一个非 failover mock（只 impl trait、不 override leg）`failover_leg_u8()==NO_FAILOVER`。
- **green**：`upstream.rs` 加默认方法 `ProxyUpstream::failover_leg_u8()->u8`(NO_FAILOVER)、`DatagramUpstream::udp_drops_up()->u64`(0)/`udp_stream_fallbacks()->u64`(0)；`TcpLeg::as_u8` 改 `pub(crate)`；override：`TuicUpstream`(udp 两项→现成访问器)、`FailoverUpstream`(leg→`state().active_leg().as_u8()`、udp 转发 tuic)、`RealityUpstream` 继承默认。
- commit：`feat(knife11): upstream observability accessors (failover_leg_u8 / udp_drops_up / fallbacks) (T3)`。

## Task 4 — DNS forge 计数 + `run_event_loop` `metrics_handle` param（TDD，首接线）

- **red**：`handle_dns_hijack` 计数测——构 mock `TunIo`+`FakeIpPool`+`Metrics`，喂可解析 `8.8.8.8:53` A 查询 → `dns_forged==1`、`dns_dropped==0`；喂截断 payload → `dns_dropped==1`、`dns_forged==0`。
- **green**：
  - `run_event_loop` 加 `metrics_handle: Arc<Metrics>` 参（保留 `mut metrics:M`）。**6 调用点**各传 clone：生产入口（client_tun.rs 约 400 处 match 前 `let metrics = Arc::new(Metrics::new());`，三臂 [:434/451/490] 传 `metrics.clone()`）、harness [:532/678/811] 各构 `Arc::new(Metrics::new())` 传入并在 Report 暴露（见 T5 测）。
  - `handle_dns_hijack` 加 `metrics:&Metrics` 参、Some/else 计数（spec C4，**不碰 forge_dns_reply**），调用点 [:646] 传 `&metrics_handle`。
- commit：`feat(knife11): count dns_forged/dropped + thread Arc<Metrics> into run_event_loop (T4)`。

## Task 5 — `relays_spawned` + 3 gauge + 30s tick + 快照发射（TDD，loop 侧）

- **red**：
  - **gauge 纯 helper**：抽 `publish_gauges(&Metrics, &socket_ctxs, &fake_pool, &U)`（或等价），单测构含若干 `Relaying`/`Listening` 条目的 `socket_ctxs` + alloc/acquire 过的 `fake_pool` + mock upstream → 调后断言 `active_relays`/`fake_ip_total`/`fake_ip_active`/`failover_leg` 原子值正确。
  - **harness 集成**：knife1 TCP scenario 注入 `Arc<Metrics>`、Report 暴露末态 `MetricsSnapshot` → 断言 `relays_spawned>0`、`active_relays` 曾>0、`fake_ip_*` 随负载>0。
- **green**：
  - `spawn_remote_relay` 加 `&Arc<Metrics>` 参 → `relays_spawned.fetch_add(1)`（覆盖 2 spawn 站 [:1228/1303]）。
  - `metrics_tick=interval(30s)`（[:592] 旁）+ select! 臂（[:765] 后，spec C5）：`publish_gauges` → `snapshot(upstream.udp_drops_up(), upstream.udp_stream_fallbacks())` → `println!(format_metrics_snapshot)` 无门控。
  - harness：scenario 构 `Arc<Metrics>` 传 run_event_loop，`.abort()` 后 `metrics.snapshot(0,0)` 入 `Report`。
- commit：`feat(knife11): relays_spawned + gauge publish + 30s MetricsSnapshot 📊 line (T5)`。

## Task 6 — UDP 下行 drop + datagram 背压上升沿（TDD，TuicUpstream 侧）

- **red**：`tuic.rs` test——纯 helper `note_pressure_edge(pressured,&mut prev)`：序列 `[F,T,T,F,T]` → 计 2 次（两个上升沿）；全 F → 0。
- **green**：
  - `TuicUpstream` 加 `metrics:Arc<Metrics>` 字段（[:663]），`connect` 加 `metrics:Arc<Metrics>` 参（[:687]，生产/失败 leg 构造处传 clone）+ `Ok(Self{…})` 初始化。
  - 下行 drop：accept-uni `if let Ok(permit)…{…} else { me.metrics.inc_udp_drops_down(); }`（[:987]）+ `read_uni_packet`→`None` 处计数（[:991]）。
  - 背压：stats tick 持局部 `prev_pressured`，经 `note_pressure_edge` 决定 `inc_datagram_pressure_events()`（[:1020] 旁）。
  - **下行 drop I/O 站归 acceptance**（真 quinn；harness mock echo 不触发，如实记边界）。
- commit：`feat(knife11): udp_drops_down + datagram_pressure_events (rising edge) in TuicUpstream (T6)`。

## Task 9 — 收尾

- `cargo test` + `cargo test --features harness` + `clippy --all-targets --features harness` 0 warning + `cargo build --release` 绿。
- **`/code-review`** over diff（high effort）+ **对抗式核验 workflow**（并发/单写者/铁律专项：确认 gauge 不跨 task 读 socket_ctxs、send_udp 不读 leg、背压不重复计、Arc<Metrics> 无锁）→ 修。
- **真出口 acceptance**（需用户 `MINI_VPN_TUIC_*` + 两腿凭据；尽力而为如实记录）：
  - `📊` 快照行出现且 `dns_forged` 随 `dig`/浏览↑；`active_relays`/`fake_ip_*` 随并发 curl↑。
  - `scripts/knife9-failover-acceptance.sh`：cut TUIC → `failover_leg` 翻 `Reality`；restore → 翻 `Tuic`。
  - `scripts/knife35-acceptance.sh` 大 UDP 负载：观察 `udp_drops_down`/`datagram_pressure_events`（难强制，如实记录；REALITY-only 模式记 UDP 项恒 0）。
  - 续写 knife1 findings 末节（刀11 可观测性结果 + 快照行样例）。
- 更新 HANDOFF（刀11 完成、下一刀指针=主线「多线程逼近 100M」量化底座就位）、seed 标完成。
- ADR-0012 已 T0 落库；acceptance 若暴露字段/语义问题再校准。

## 质量门（同项目惯例）

lib+harness 测全绿 ｜ `clippy --all-targets --features harness` 0 warning ｜ release 绿 ｜ 每任务红→绿→commit→**push** ｜ `/code-review` + 对抗式核验 ｜ acceptance 如实记录。系统稳定 > 代码漂亮；先量化别凭猜改。

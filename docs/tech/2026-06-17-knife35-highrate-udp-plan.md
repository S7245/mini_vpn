# 刀3.5 — 高码率 UDP 硬化（plan / TDD 分解）

> 配套 spec：`2026-06-17-knife35-highrate-udp-spec.md`。分支 `claude/knife35-highrate-udp`（从 main 起）。
> 每个 Task：写失败测试 → red → 实现 → green → `cargo test` + `clippy` 绿 → commit → **`git push`**。
> 一个分支一个 writer。纯逻辑先行（T1–T3），再 config/接线（T4–T6），插桩（T7），harness（T8），收尾（T9）。

## 决策溯源（grill 2026-06-17，对齐结果）

| Q | 决策 | 关键依据 |
|---|---|---|
| Q1 | 分阶段：BBR+插桩先行，stream 默认设量化 gate | 事实先行；机制代码两阶段都小，gate 实定**默认 mode** |
| Q2 | BBR 接现有 `congestion_control` 字段、env 可切、默认 bbr，未知回落 Cubic+告警 | A/B 归因硬需求；高 RTT 跨境主场景 |
| Q3 | 插桩 = 30s `📊` 扩 RTT/cwnd/lost/sent + `send_buffer_space` 代理信号 | 要趋势不要逐包；逐包精确留待主动背压 |
| Q4 | gate 主判据 = 下行 datagram 干净吞吐；≥30M 保 native、<30M 默认翻 quic | 4K(25M) 必跨；下行 datagram sing-box-capped |
| Q5' | 高码率走 stream = **全 UDP 首包即 stream（quic 模式）**，先不做 carve-out | SPEC 首包锁定下行 mode → 事后自适应翻不动下行 |
| Q6 | 连接池 defer；多 flow gate = 1×4K+1×1080p≈33M 单连接成立 | #3 单连接非瓶颈；典型聚合 <50M |
| Q7 | datagram 主动背压 defer，只留可观测 | 高码率已转 stream |
| 数值 | `max_concurrent_uni_streams=4096` | 下行 4K~650 在飞 + 多 flow~850 + 余量；避 #221 |

## 执行顺序与依赖

```
T0（spec/plan 落库）
 ├─ T1 udp_send_plan mode 感知（纯）────┐
 ├─ T2 congestion_factory（纯）─────────┼─→ T4 config 装配 ─→ T6 send_udp 接 mode ─┐
 ├─ T3 env 解析/覆盖（纯）──────────────┘                                          ├─→ T8 harness ─→ T9 收尾+acceptance
 └─ T5 插桩取数纯函数（纯）──────────────────────→ T7 📊 接线 ───────────────────────┘
```
T1/T2/T3/T5 纯逻辑可连续做；T4 依赖 T2；T6 依赖 T1+T4；T7 依赖 T5；T8 依赖 T6；T9 最后。

---

## Task 0 — spec/plan 落库 ✅（本 commit）

`docs(knife35): spec + plan for high-rate UDP hardening`。含 CONTEXT.md「UDP relay mode」术语。

## Task 1 — mode 感知上行分流 `udp_send_plan`（纯，TDD）

- **red**：`tuic.rs` 测——
  - `Native`：`len<=max → Datagram`、`len==max → Datagram`（边界）、`len>max → Stream`、`max=None → Stream`（**保刀3 现行语义，零回归**）。
  - `Quic`：任意 len/max（含 `len<=max`、`max=None`）→ **恒 Stream**。
- **green**：`enum UdpRelayMode { Native, Quic }`；`udp_send_plan(mode: UdpRelayMode, max_datagram: Option<usize>, len: usize) -> UdpSend`。旧调用点改传 mode。
- commit：`feat(knife35): mode-aware udp_send_plan (T1)`。

## Task 2 — CC 名→控制器映射 `congestion_factory`（纯，TDD）

- **red**：`"bbr"`/`"BBR"` → 标记 Bbr；`"cubic"` → Cubic；`""`/未知 → Cubic（回落）+ 可断言"回落发生"（返回带 fallback flag 的结果，或日志计数）。
- **green**：因 `ControllerFactory` 是 trait object 难直接断言类型，纯函数返回 `enum CcChoice { Bbr, Cubic }`（可测）；在 config 装配处 `match` 成 `Box<dyn ControllerFactory>`。映射 + 回落告警在纯函数侧。
- commit：`feat(knife35): congestion controller name mapping (T2)`。

## Task 3 — env 解析与覆盖 `MINI_VPN_TUIC_CC` / `MINI_VPN_TUIC_UDP_MODE`（纯，TDD）

- **red**：`from_sources`/`from_env` 当前 `congestion_control` **硬编默认 bbr、不读 env** → A/B 无法切。测：给定 env/source 覆盖值 → 字段取覆盖值；缺省 → 默认（bbr / native）；非法 mode → 错误或回落。
- **green**：`from_sources` 增 `cc`/`udp_mode` 可选源；`from_env` 读 `MINI_VPN_TUIC_CC`/`MINI_VPN_TUIC_UDP_MODE`。保持 Debug 脱敏不变。
- commit：`feat(knife35): env-overridable CC + udp_relay_mode (T3)`。

## Task 4 — transport config 装 BBR + 抬 uni-stream 配额（config，TDD）

- **red/green**：`quic_transport_config` 增参数（CC choice）：装 `congestion_controller_factory(match choice ...)` + `max_concurrent_uni_streams(4096u32.into())`。`client_quic_config_alpn`/`client_endpoint` 透传。扩 `client_endpoint_binds` 测：bbr/cubic 两种 choice 都 bind 绿。
- commit：`feat(knife35): wire BBR + raise max_concurrent_uni_streams (T4)`。

## Task 5 — 插桩取数纯函数（纯，TDD）

- **red**：`format_udp_stats(rtt, cwnd, lost, sent, fb, drops, buffer_space) -> String`（或结构化）+ `is_datagram_pressured(buffer_space, mtu) -> bool`（< 1 MTU → true）。
- **green**：纯函数化，便于单测；I/O 取数（`conn.stats()`/`datagrams().send_buffer_space()`）在 T7 接线。
- commit：`feat(knife35): pure stats formatting + pressure signal (T5)`。

## Task 6 — `send_udp` 接 mode + `TuicUpstream` 持有 mode（I/O，harness/acceptance 验证）

- `TuicUpstream::connect` 存 `udp_relay_mode`（解析自 cfg）；`send_udp` 调 `udp_send_plan(self.mode, conn.max_datagram_size(), len)`。`Quic` 模式 `TooLarge` 竞态分支不再需要（恒 stream），但保留 `Native` 的二次兜底。
- 下行接收（`accept_uni`/`FragReassembler`）**不改**（刀3 已就绪）。
- 测：mode getter 单测；I/O 路径归 acceptance（同 #3 边界）。clippy 绿。
- commit：`feat(knife35): honor udp_relay_mode in send_udp (T6)`。

## Task 7 — 30s `📊` 插桩接线（I/O）

- `start_udp` 的 `stats.tick()` 分支：取 `conn.stats().path`（rtt/cwnd/lost/sent）+ `conn.datagrams().send_buffer_space()`，调 T5 纯函数打 `📊`（非零/有压力才打，沿用节流）。
- 连上 `📏` 行加打实际生效 CC（确认 BBR 真装上）。
- `datagram_pressure` 计数 + getter（可观测）。
- 测：纯函数已在 T5 测；接线 I/O 归 acceptance。
- commit：`feat(knife35): instrument RTT/cwnd/datagram pressure in stats log (T7)`。

## Task 8 — harness 全 quic 模式回归（TDD）

- `run_udp_throughput_scenario` 复用：以 `Quic` 模式跑——验证全 stream 路径下 UDP 吞吐/逐字节完整性 + UDP 不被 TCP 饿死（mock 上游收 uni-stream 等价字节，重组器走 `FRAG_TOTAL==1` 直通）。
- 注：真 datagram 天花板/真 stream 互通/真 BBR 走 `TuicUpstream`（真 quinn），harness 测不到（同 #3 边界）→ 归 acceptance。
- commit：`test(knife35): quic-mode UDP throughput regression (T8)`。

## Task 9 — 收尾

- `/code-review` over diff → 修。
- **真出口 acceptance**（spec 矩阵 T-A~T-H，需用户 `MINI_VPN_TUIC_*` env）：
  - 先 T-A（CC A/B gate）定**默认 `udp_relay_mode`**；再 T-B/C/D/G 坐实 stream 修下行 + 4K；T-E 多 flow gate；T-F/T-H 决定 carve-out 做不做。
  - 续写 `2026-06-12-knife1-bottleneck-findings.md` 末节（刀3.5 结果 + 裁决）。
- **若 T-A 判 <30M（预期）→ 默认翻 quic**：改 `DEFAULT_TUIC_UDP_MODE` + 补 `docs/adr/0005-default-udp-relay-mode-quic.md`（hard-to-reverse + surprising + 真权衡）。
- **若 T-F/T-H 判 DNS/小流退化 → 补 carve-out**：`udp_send_plan` 增小流/端口例外（datagram），按实测阈值定档。
- 更新 HANDOFF（刀3.5 完成、刀4 入口）。

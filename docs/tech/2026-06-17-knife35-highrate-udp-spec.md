# 刀3.5 — 高码率 UDP 硬化（spec）

> 配套：plan（同目录 `2026-06-17-knife35-highrate-udp-plan.md`）、findings（复用并续写
> `2026-06-12-knife1-bottleneck-findings.md` 的「刀3 真出口 acceptance 裁决」段）。
> 对症刀3 acceptance 实测发现：native QUIC datagram **上/下行两方向都卡 ~5.3Mbps 硬天花板**，
> 而同链路 QUIC stream 跑满 50M。典型直播（≤5M）已达标；本刀把 ② 推到**高码率（1080p60/4K）**。
> 北极星：**4K（~25M）直播必跨**（用户定为本刀必达线）。

## TL;DR

| 项 | 缺口 | 本刀做法 |
|---|---|---|
| **CC 未接** | `congestion_control="bbr"` 字段**存而未用**，真控制器是 quinn 默认 **Cubic**（[tuic.rs:37](../../src/tuic.rs)、[quic.rs:39](../../src/quic.rs)） | 接 `congestion_controller_factory`：`bbr→Bbr`、`cubic→Cubic`、未知→Cubic+告警；env `MINI_VPN_TUIC_CC` 可切（A/B 归因） |
| **量化盲点** | datagram 丢包 quinn 不报错（`send_datagram` 仍 Ok，缓冲溢出丢最老）；无 RTT/cwnd 可见 | 插桩：30s `📊` 行加 `RTT/cwnd/lost/sent` + `send_buffer_space` 代理信号（背压压力可见） |
| **高码率下行** | 下行 datagram 卡 5.3M，**在 sing-box 发送侧、客户端够不着** | 接 `udp_relay_mode` 字段：`quic` 模式全 UDP 首包即走 uni-stream → server **镜像下行也走 stream** → 摆脱天花板 |
| **stream 配额** | `max_concurrent_uni_streams` 默认 **100**，下行 4K 需 ~650 在飞 → 正中 #221 塌缩 | 抬到 **4096** |
| **acceptance** | 刀3 只测上行 stream + datagram 天花板；下行 stream/4K/多 flow/真实场景未测 | 真出口 7+1 项矩阵（含下行 stream、4K 端到端、多 flow gate、真实 soak） |

## 现状（代码事实，已查证）

1. **CC 未接**：`quic_transport_config()`（[quic.rs:39](../../src/quic.rs)）只设 idle/keepalive/MTU，**从不设 `congestion_controller_factory`** → quinn 默认 Cubic（`quinn-proto-0.10.6/config.rs:344`）。`TuicClientConfig.congestion_control`（默认 `"bbr"`）仅在 Debug/test 出现，**从没传到 transport config**。
2. **`udp_relay_mode` 同样未接**：字段默认 `"native"`，仅 Debug/test 引用；`send_udp` 的分流（`udp_send_plan`，[tuic.rs:568](../../src/tuic.rs)）**只看包大小**，与 mode 无关。
3. **datagram 丢包不可见**：`Datagrams::send`（`quinn-proto-0.10.6/connection/datagrams.rs:23`）在 `outgoing_total > datagram_send_buffer_size`（默认 1MB）时**丢最老的 datagram 并返回 Ok** → 我方 `udp_drops` 永远看不到。`send_buffer_space()`（同文件 :82）可读剩余空间，跌到低位 = 正在背压丢包。
4. **下行 stream 接收已就绪（刀3 T5）**：`start_udp` 的 `accept_uni` 分支 + `FragReassembler` + 有界 `Semaphore`（[tuic.rs:805](../../src/tuic.rs)）已能收下行 uni-stream → 本刀**无需重建下行接收**，只需让 server 真的用 stream 发（靠 `quic` 模式触发镜像）+ 抬我方 uni-stream 配额。

## 已查证的传输层真相（决定方案存废，源已读）

### A. quinn-proto 0.10.6 能力（已读 vendored 源码）
- **BBR 可用**：`pub use bbr::{Bbr, BbrConfig}`（`congestion.rs:11`）；API = `TransportConfig::congestion_controller_factory(Arc::new(BbrConfig::default()))`（`config.rs:289`）。
- **datagram drop-oldest 行为 + `send_buffer_space()`**：见上「现状 3」。
- **`stats().path`** 暴露 `rtt / cwnd / lost_packets / congestion_events / sent_packets / black_holes_detected`（`connection/stats.rs:122`）——量化抓手齐备。
- **默认 `max_concurrent_uni_streams = 100`**（`config.rs:322`）——下行 stream 的命门。

### B. TUIC SPEC（已读 dev/SPEC.md，决定字节级/语义互通）
- relay mode：**native = QUIC datagram；quic = uni-stream**。一条 uni-stream 承载一个完整 `Packet`（spec 未定义流复用 → sing-box 实为 one-packet-per-stream，**不能多 Packet 复用一条流**）。`encode_packet` 字节两模式通用。
- **首包锁定下行 mode（关键）**：原文 *"When the server receives the **first** Packet from an UDP relay session (associate ID), it should use the same mode to send back the Packet commands."* → **per-association 绑定、首包决定、不可中途翻转**。
- 下行路由用 `assoc_id`（非 ADDR）；native 大包由 server 应用层分片（`FRAG_TOTAL>1`），quic 模式不分片（流承载任意大小）。

### C. issue #221（per-packet uni-stream 已知坑）
- 每包开一条 uni-stream → 耗尽 uni-stream 配额 → 阻塞 → 吞吐近零/~100% 丢；**影响上下行**。缓解：抬 `max_concurrent_uni_streams`。
- 交叉验证：刀3 acceptance **上行** per-packet uni-stream 已实测 **50M/0.037%**（对的就是本环境 sing-box）→ server 给我方的配额够；**下行**配额是**我方** transport config（默认 100，未抬）→ 走 stream 前**必须抬**，否则正中 #221。

## 设计决策（grill 对齐结果，2026-06-17）

> 完整 grill 决策树见 plan 顶部「决策溯源」。核心 7 条：

- **D1 分阶段 + 量化 gate（事实先行）**：Phase 1 = 接 BBR + 插桩 + 真出口 A/B；Phase 2（`quic` 模式默认）**做不做、是否设为默认**由 Phase 1 数据定。机制代码两阶段都很小，gate 实际决定的是**默认 `udp_relay_mode`（native vs quic）**。
- **D2 BBR 接现有字段、env 可切、默认 bbr**：高 RTT 跨境是本项目主场景，BBR model-based、不因丢包腰斩 cwnd；未知值回落 Cubic + 告警（失败自愈不致命）。
- **D3 插桩 = 30s `📊` 扩字段 + `send_buffer_space` 代理信号**：目标是量化天花板成因 + A/B 归因，要趋势不要逐包精确；逐包精确（发送前比对 buffer）留待真做主动背压时一起上。
- **D4 gate 判据（主判据 = 下行 datagram 干净吞吐）**：≥30M（4K 有余量）→ 保持 `native` 默认、Phase 2 跳过；**<30M → 默认翻成 `quic`**（4K 必跨）。诚实预期：下行 datagram 5.3M 在 sing-box 发送侧、**客户端够不着**，BBR 主要抬上行 → gate **极可能判 <30M → 默认 quic**。
- **D5 高码率走 stream = 全 UDP 首包即 stream（`quic` 模式），不做 carve-out（先）**：因「首包锁定下行 mode」→ 事后自适应翻不动下行；唯一客户端侧杠杆是**首包就 stream**。比 per-assoc 自适应**更简单更稳**（无 pps 状态机/无 mode 翻转抖动，合「系统稳定>优雅」）。代价诚实：放弃「datagram 主路径低延迟」，所有 UDP 走可靠流——对视频直播 OK，对低延迟交互不利，留 `native` 配置出口。**carve-out（DNS/小流走 datagram）先不做，acceptance T-F/T-H 实测延迟退化才补**。
  - 注：TUIC quic 模式**每包一条独立 uni-stream → 无跨包队头阻塞**（A 包重传只卡 A 自己，不挡 B）；代价只剩"丢包变重传迟到"+ 建流开销 + 配额压力。
- **D6 连接池 defer**：#3 已裁决单连接非瓶颈（stream 50M）；4K=25M、典型混合聚合 ~15M 均 <50M。**多 flow gate**（T-E）量出单连接聚合天花板，<目标才在后续刀建池。
- **D7 datagram 主动背压 defer**：高码率已转 stream，datagram 仅低/中速用，5.3M 咬不到；只留 D3 可观测。

## 组件设计

### C1 CC 接线（`tuic.rs` + `quic.rs`）
- 纯函数 `congestion_factory(name: &str) -> Box<dyn ControllerFactory + Send + Sync>`（或返回枚举后在 config 处装配）：`"bbr"→BbrConfig`、`"cubic"→CubicConfig`、其余→Cubic + 一行 `⚠️` 告警。
- `quic_transport_config` 增参数（或 `TuicClientConfig` 透传）：装 `congestion_controller_factory` + `max_concurrent_uni_streams(4096)`。
- env `MINI_VPN_TUIC_CC` 已由 `congestion_control` 字段承载；确认 `from_env` 读取（当前 `from_sources` 硬编默认 bbr——需让 env 可覆盖，A/B 必需）。

### C2 mode 感知上行分流（`tuic.rs`）
- `enum UdpRelayMode { Native, Quic }`（从 `udp_relay_mode` 字段解析）。
- `udp_send_plan(mode, max_datagram, len) -> UdpSend`：
  - `Quic` → 恒 `Stream`（首包起即 uni-stream，触发 server 下行镜像 stream）。
  - `Native` → 现行 size-based（`len<=max → Datagram`，否则 `Stream`，含边界），**零回归**。
- `TuicUpstream` 持有 mode；`send_udp` 按新 plan 分流。下行接收（`accept_uni`/`FragReassembler`）**已就绪，不改**。

### C3 插桩（`tuic.rs`）
- 30s `📊` 行扩展：`rtt / cwnd / lost_packets / sent_packets`（取 `conn.stats().path`）+ `send_buffer_space`（取 `conn.datagrams().send_buffer_space()`）。
- 代理丢包信号：`send_buffer_space` 低于阈值（如 < 1 MTU）时累加 `datagram_pressure` 计数 + 打 warn（非零才打，沿用现节流）。
- 连上时 `📏` 行已记 `max_datagram_size()`；可加 `congestion_control` 实际生效值一并打（确认 BBR 真装上）。

### C4 config（`quic.rs`）
- `max_concurrent_uni_streams(4096)`：下行 4K~650 在飞 + 多 flow~850 + 突发余量；避 #221。按需建流、空闲不预分配。
- `congestion_controller_factory` 装配（见 C1）。
- `datagram_send_buffer_size`/`datagram_receive_buffer_size` **不动**（高码率转 stream；下行 datagram sing-box-capped）。

## 测试边界（诚实分层）

- **纯单元（TDD red→green）**：`udp_send_plan`（mode 感知，全边界）、`congestion_factory`（名→控制器映射）、`UdpRelayMode`/`congestion_control` 的 env 解析与覆盖。本刀逻辑主战场。
- **config 构建测**：transport config 装 BBR + 抬 uni-stream 配额后 `client_endpoint` bind 绿（扩现有测）。
- **harness（真主循环）**：全 `quic` 模式下 UDP 吞吐/完整性回归（复用 `run_udp_throughput_scenario`；quic 不分片 → 重组器直通，验证零回归 + UDP 不被 TCP 饿死）。
- **真出口 acceptance（plan T9）**：CC/datagram 天花板/stream 互通/首包镜像/配额/真实 soak——逻辑测不到真 quinn，同 #3 边界。见下矩阵。

## 真出口 acceptance 矩阵（plan T9 执行；需用户 `MINI_VPN_TUIC_*` env）

环境：sing-box `47.251.188.205:8443`、iperf3 靶 `43.110.37.170`。
**铁律：测试链路带宽 ≥50M（别把 43.x 限到 10M，否则链路成新瓶颈污染判读），靠 `-b` 控码率。**

| # | 测项 | 命令要点 | 判据 |
|---|---|---|---|
| T-A | **CC A/B（gate 主判据）** | `MINI_VPN_TUIC_CC=cubic` vs `bbr`，下行 datagram sweep `-l 1200 -R -b 5/10/20/40M` | BBR 下行 datagram 干净吞吐：**≥30M→保 native 默认、Phase 2 跳过；<30M→默认翻 quic** |
| T-B | **下行 stream 吞吐**（刀3 只测了上行） | `MINI_VPN_TUIC_UDP_MODE=quic`，`-l 1200 -R -b 40M` | 下行经 stream **跑满 offered**（对照 datagram 5.3M）→ 证 stream 修下行 |
| T-C | **上行 datagram + BBR** | `-l 1200 -b 40M`（不 -R） | BBR 是否把上行 datagram 抬过 5.3M（归因 CC vs 无背压） |
| T-D | **4K 端到端（quic 模式）** | `-b 25M` 双向 | 丢包低位、`📊` 兜底/压力计数合理、无映射洪水 |
| T-E | **多 flow gate** | 并行 2 路：1×`-b 25M` + 1×`-b 8M`（≈33M 聚合），quic 模式 | 单连接 stream 聚合 **≥33M 成立**→池 defer；否则池升后续刀首项 |
| T-F | **DNS/小流延迟（carve-out 触发）** | quic 模式 `dig`/小 UDP 往返延迟 vs native 基线 | 明显退化 → **本刀补 DNS/小流 datagram carve-out**；否则不做 |
| T-G | **首包锁定 + 配额验证** | quic 模式起播，日志确认下行真走 stream、无 #221 塌缩 | 坐实"首包 stream→下行镜像 stream"且 4096 配额够 |
| T-H | **真实混合场景长稳 soak**（macOS 深圳） | YouTube 视/直 + TikTok 视/直 + Facebook 网页 + Telegram 客服，30–60min+ | 主观：视频不卡、TG 跟手、FB 翻页不滞；客观：`📊` 合理、重连低、内存不涨、无映射洪水。TG/FB 明显滞 → 触发 carve-out |

## 风险 / 已知边界

- **下行 datagram 天花板客户端够不着**：在 sing-box 发送侧；BBR 主要抬上行。故 gate 极可能判 <30M、默认翻 quic（已在 D4 诚实声明）。
- **全 stream 的低延迟代价**：可靠重传 → 丢包变迟到（视频可容忍，迟到帧丢弃）；交互类（TG/游戏）可能滞 → T-F/T-H 实测，退化才补 carve-out。每包独立流 → 无跨包 HOL（已澄清，弱化此风险）。
- **stream 配额压力**：多路高码率并发 → 在飞 uni-stream 数线性涨；4096 上限 + 多 flow gate（T-E）兜量化。极端超配额 → server 侧丢该 stream（UDP 自愈）。
- **QUIC-over-QUIC**：YT/TK 的 HTTP/3 是 app 自身 QUIC 跑在本隧道上；走 stream = QUIC-over-可靠流（内层 QUIC 见可靠管道，可接受），走 datagram = 内层自理丢包。记录，不阻塞。
- **ADR**：「默认 UDP relay mode = quic（全 UDP 走流，放弃 datagram 为高码率主路径）」是 hard-to-reverse + surprising + 真权衡的决策——**待 T-A gate 实测定档后**（确认默认值）补 `docs/adr/0005-*`，不提前写（决策尚 gated on data）。

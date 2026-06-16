# 刀3 — UDP 直播硬化（spec）

> 配套：plan（同目录 `2026-06-16-knife3-udp-streaming-plan.md`）、findings（复用并续写
> `2026-06-12-knife1-bottleneck-findings.md` 的「#3 真出口 acceptance」段）。
> 对症 `Rules.md` ② UDP 视频直播：当前部分达标，缺口在**大流量**——native QUIC datagram
> 超上限的包直接丢（上行），下行大包（sing-box native 分片）无法重组。
> 北极星：持续大流量 UDP（直播）**不丢包**。

## TL;DR

| 项 | 缺口 | 本刀做法 |
|---|---|---|
| **P0 上行** | `send_udp` 遇 `TooLarge` 直接丢（[tuic.rs:579](../../src/tuic.rs)） | datagram 主路径不变；超上限 → **per-packet uni-stream 兜底**（复用同一 `encode_packet` 字节） |
| **P0 下行** | `decode_packet` 无视 FRAG 字段，假定 `FRAG_TOTAL=1`；server native 模式把大下行包**分片** → 我方无法重组 | 新增 **native 分片重组**（`FragReassembler`）+ **uni-stream 接收器**（`accept_uni`），两路汇入同一下行 channel |
| **MTU** | `initial_mtu=1280` → datagram 上限 ~1242，装不下典型 1400B 视频包 | 维持 1280 floor（不黑洞）；显式抬 `EndpointConfig::max_udp_payload_size`（下行 headroom）；PLPMTUD 抬升 current_mtu；instrument `max_datagram_size()` 供 probe 读真上限 |
| **压测** | harness 只有 UDP liveness（计数），主体吞吐留本刀 | 扩 `run_udp_throughput_scenario` + 分片 mock，量化主循环 UDP 路径吞吐/丢包 + 重组完整性 |
| **acceptance** | #3 单连接在真直播大流量下是否需连接池/多流，未测 | 真 sing-box 持续高码率 UDP probe，复测 #3（续写 findings） |

## 现状（代码事实）

UDP 数据面**全程 native datagram**：
- **上行** `handle_tuic_udp_uplink`（[client_tun.rs:1150](../../src/client_tun.rs)）→ `encode_packet` → `TuicUpstream::send_udp` → `conn.send_datagram()`；`SendDatagramError::TooLarge` 丢 + 计数。
- **下行** `start_udp`（[tuic.rs:600](../../src/tuic.rs)）后台任务 `select{ read_datagram, heartbeat }` → channel → 主循环 `tuic_downlink_rx`（[client_tun.rs:593](../../src/client_tun.rs)）→ `decode_packet`（仅取 `assoc,data`，**跳过 FRAG**）→ `AssocTable.resolve` → `build_udp_ip_packet` → 注入 TUN。

## 规范约束（TUIC v5 SPEC，已查证，决定字节级互通）

1. `encode_packet` 字节布局与规范 Packet 命令**逐字节一致**：`[0x05][0x02][ASSOC:2][PKT_ID:2][FRAG_TOTAL:1][FRAG_ID:1][SIZE:2][ADDR][DATA]`。
2. relay mode：**native = QUIC datagram；quic = 单向流（uni-stream）**。一条 uni-stream 承载一个完整 Packet 命令（`FRAG_TOTAL=1`）。`encode_packet` 的字节**stream 模式原样复用**。
3. **server 按首包 mode 镜像下行**：「server 收到某 assoc 的第一个 Packet 时，用相同 mode 回送」。即首个上行包走 datagram → 该 assoc 下行也走 datagram。
4. native 模式下大下行包由 **server 应用层分片**（`FRAG_TOTAL>1`）：
   - **ADDR 仅在 `FRAG_ID=0`**；后续分片用 `ATYP_NONE(0xff)`（我方 `address_len` 已支持，跳 1 字节）。
   - `SIZE` = **本分片 data chunk 长度**（参考实现惯例；acceptance 对真 sing-box 校验）。
   - 下行路由用 `assoc_id`（非 ADDR），故 ADDR 在下行被跳过、不参与路由。

## quinn 0.10 datagram 真相（已读 vendored 源码，避免 spec 写不存在的旋钮）

- `Connection::send_datagram` 的 `TooLarge` 阈值 = `Datagrams::max_size()` = **`min(MTU 推导, peer.max_datagram_frame_size)`**（`quinn-proto-0.10.6/src/connection/datagrams.rs:57`）。
- **MTU 是主约束**：`current_mtu` 起于 `initial_mtu`（我们 1280）→ max ≈ 1242；PLPMTUD 抬升 current_mtu 至 `MtuDiscoveryConfig` 上限（默认 ~1452）。我们的 `quic_transport_config` 未禁用 MTU discovery → **默认开**（[quic.rs:32](../../src/quic.rs)）。
- `peer.max_datagram_frame_size` 由对端 `datagram_receive_buffer_size` 推出，默认 ≈65535 → **不绑定**。
- `EndpointConfig::max_udp_payload_size`（默认 1472）管**接收侧** headroom（告诉 sing-box 我方能收多大 UDP 载荷），对**发送** datagram 上限作用有限——发送上限只随 PLPMTUD 抬 current_mtu 而长。
- 诚实结论：**抬 `max_udp_payload_size` 主要给下行 headroom**；上行大包能进 datagram 仍靠 PLPMTUD，残余靠 **stream 兜底**；这正是「datagram 主 + 双向尾部硬化」分流策略的成立基础。

## 设计决策（grill 对齐结果，2026-06-16）

- **Q1 分流策略 = datagram 主 + 双向尾部硬化**：保留 datagram 快路径（MTU 调优后承载绝大多数视频包）；上行超限 → per-packet uni-stream 兜底；下行新增 native 分片重组 + uni-stream 接收。快路径不变、尾部不丢、改动聚焦。系统稳定优先。
- **Q2 流复用粒度 = 每包一条新 uni-stream**：开 uni → 写 `encode_packet` → finish。TUIC 规范惯例，sing-box 互通最稳，无需自定义流内 framing。兜底只覆盖尾部大包（MTU 调优后频率低），per-packet 开销可接受。
- **Q3 MTU = 维持 1280 floor + 抬 `max_udp_payload_size` + probe 实测再定档**：不抬 `initial_mtu`（避免真实 PMTU<该值时的黑洞）；显式放开接收 headroom；instrument `max_datagram_size()` 让 acceptance 读真实上限，再决定是否进一步调。

## 组件设计

### C1 上行 stream 兜底（`tuic.rs`）

- 纯函数 `udp_send_plan(max_datagram: Option<usize>, len: usize) -> UdpSend`：
  `len <= max` → `Datagram`；`len > max` 或 datagram 不可用（`None`）→ `Stream`。**先查 `conn.max_datagram_size()` 主动分流**，避免 `TooLarge` 往返。
- `send_udp`：按 plan 走 datagram 或 `send_udp_via_stream`（`open_uni` → `write_all(&datagram)` → `finish`）。datagram 仍返回 `TooLarge`（MTU 竞态收缩）→ 二次 stream 兜底。仅**真失败**才 `udp_drops++`；新增 `udp_stream_fallbacks` 计数（可观测）。

### C2 下行接收 + 重组（`tuic.rs` + `client_tun.rs`）

- `start_udp` 的 select 增 `accept_uni` 分支：收到 uni-stream → **有界**派生任务 `read_to_end(MAX_UDP_PACKET)` → 完整 Packet 字节 → 同一下行 channel（与 datagram 路径同构）。并发上限用 `Semaphore`（如 256），超额直接 drop（reset stream），防 flood 下无界 spawn。
- `decode_packet_meta(buf) -> Option<PacketMeta{ assoc, pkt_id, frag_total, frag_id, data }>`（frag 感知；`decode_packet` 保留为 `frag_total==1` 的薄包装/或主循环改用 meta）。
- `FragReassembler`（**纯状态机**，主循环独占、无锁，与 `AssocTable` 同寿）：
  `accept(assoc, pkt_id, frag_total, frag_id, data, now) -> Option<Vec<u8>>`。
  - `frag_total==1` → 直接 `Some(data.to_vec())`（快路径，不入表）。
  - `frag_total>1` → 按 `(assoc,pkt_id)` 收集分片，集齐按 `frag_id` 序拼接 → `Some(whole)`；未齐 → `None`。
  - `cap` 上限（LRU 驱逐最老未完成）+ `ttl` sweep（丢分片→整包不等，TTL 到期清，保直播 liveness）。
  - 主循环下行分支：`decode_packet_meta` → `reassembler.accept` → `Some` 才 `resolve+inject`；`udp_sweep` tick 调 `reassembler.sweep`。

### C3 MTU / datagram config（`quic.rs`）

- `EndpointConfig::max_udp_payload_size` 显式设（需 `Endpoint::new` + 自定义 `EndpointConfig`，替 `Endpoint::client`）。
- 连上后 `println!` 记 `conn.max_datagram_size()`（acceptance 读真上限）。
- `initial_mtu/min_mtu` 维持 1280（不动）；MTU discovery 维持默认开。

### C4 harness UDP 吞吐（`harness.rs`）

- `run_udp_throughput_scenario(datagrams, payload_len, ...)`：持续高 pps 注入，测主循环 UDP 路径 pps/吞吐/丢包（sent vs echoed-intact）。
- MockUpstream 增「分片回灌」模式：收到 `encode_packet`（payload>阈值）→ 拆成多个 `FRAG_TOTAL>1` datagram 推下行 → **经真主循环 `FragReassembler` 重组** → echo 完整性校验。端到端验证重组逻辑（不需真网络，补 #3 测不到的空白）。
- 注：真 datagram `TooLarge`/真 stream 兜底走 `TuicUpstream`（真 quinn），harness 测不到（同 #3 边界）→ 归 acceptance。

## 测试边界（诚实分层）

- **纯单元（TDD red→green）**：`decode_packet_meta`、`FragReassembler`、`udp_send_plan`。本刀逻辑主战场。
- **harness（真主循环）**：分片重组完整性 + UDP 吞吐不被 TCP 饿死。
- **真出口 acceptance（item 4）**：`send_udp` stream 兜底 / `accept_uni` 接收 / 真分片重组互通 / `max_datagram_size` 真上限 / #3 单连接在真直播大流量下是否需连接池。需用户 `MINI_VPN_TUIC_*` env（HANDOFF「Not in git」）。

## 风险 / 已知边界

- **stream 兜底吞吐**：per-packet uni-stream 在高频大包下开销高于 datagram；依赖 MTU 调优把大包频率压低。若 acceptance 显示典型视频包频繁触发兜底 → 重新评估抬 `initial_mtu` 或 per-assoc quic 模式（留后续刀）。
- **下行 stream 无界 spawn**：Semaphore 兜底；超额 drop（UDP 自愈）。
- **分片丢失放大**：一片丢 → 整包弃（TTL）。视频可容忍（自带 FEC/重传或丢帧），优于无限等待。
- **SIZE 语义**：按「本分片 chunk 长」实现，acceptance 对真 sing-box 校验；若不符则按实测修。

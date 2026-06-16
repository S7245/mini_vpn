# 刀3 — UDP 直播硬化（plan / TDD 分解）

> 配套 spec：`2026-06-16-knife3-udp-streaming-spec.md`。分支 `claude/knife3-udp-streaming`（从 main 起）。
> 每个 Task：写失败测试 → red → 实现 → green → `cargo test` + `clippy` 绿 → commit → **`git push`**。
> 一个分支一个 writer。纯逻辑先行（T1–T3），再 I/O 接线（T4–T6），harness（T7），config（T8），收尾（T9）。

## Task 0 — spec/plan 落库 ✅（本 commit）

`docs(knife3): spec + plan for UDP streaming hardening`。

## Task 1 — frag 感知解码 `decode_packet_meta`（纯，TDD）

- **red**：`tuic.rs` 测——
  - `frag_total==1` 单帧：meta 取出 `assoc/pkt_id/frag_total=1/frag_id=0/data`，与现 `decode_packet` 一致。
  - 非首分片（`ATYP_NONE` 地址）：`frag_id=1`、跳 1 字节地址、`data` 正确。
  - 截断/未知 atyp → `None`。
- **green**：`pub struct PacketMeta{ assoc, pkt_id, frag_total, frag_id, data: &[u8] }` + `decode_packet_meta`。`decode_packet` 改薄包装（或保留，主循环改用 meta）。
- commit：`feat(knife3): frag-aware decode_packet_meta (T1)`。

## Task 2 — 分片重组 `FragReassembler`（纯，TDD）

- **red**：新测——
  - `frag_total==1` → 立即 `Some(data)`，不入表。
  - 2 分片**顺序**到达 → 集齐返回拼接整包；**乱序**（先 frag_id=1 后 0）→ 同样正确拼接。
  - 缺片 → `None`；`sweep` 过 TTL → 清除该未完成项。
  - 重复分片幂等（不重复计数/不破坏）。
  - `cap` 满 → LRU 驱逐最老未完成。
- **green**：`FragReassembler{ partials: HashMap<(u16,u16),Partial>, cap, }`，`accept(...) -> Option<Vec<u8>>` + `sweep(now,ttl)`。
- commit：`feat(knife3): FragReassembler for native downlink fragments (T2)`。

## Task 3 — 上行分流决策 `udp_send_plan`（纯，TDD）

- **red**：`len<=max → Datagram`；`len==max → Datagram`（边界）；`len>max → Stream`；`max=None → Stream`。
- **green**：`enum UdpSend{ Datagram, Stream }` + `fn udp_send_plan(max_datagram: Option<usize>, len: usize) -> UdpSend`。
- commit：`feat(knife3): udp_send_plan transport decision (T3)`。

## Task 4 — 上行 stream 兜底接线（I/O，harness/acceptance 验证）

- 实现 `send_udp_via_stream`（`open_uni`/`write_all`/`finish`）；`send_udp` 按 `udp_send_plan` 分流，`TooLarge` 竞态二次兜底；`udp_stream_fallbacks` 计数 + getter。
- 测：计数器 getter 单测；I/O 路径归 acceptance（同 #3 边界，spec 已声明）。clippy 绿。
- commit：`feat(knife3): quic-stream fallback for oversized UDP uplink (T4)`。

## Task 5 — 下行 uni-stream 接收器（I/O）

- `start_udp` select 增 `accept_uni` 分支：`Semaphore`(cap=256) 有界派生 `read_to_end(MAX_UDP_PACKET)` → 下行 channel；超额 drop。
- 测：`MAX_UDP_PACKET` 常量 + 有界逻辑可抽小纯函数测；I/O 归 acceptance。
- commit：`feat(knife3): accept downlink UDP over uni-stream (T5)`。

## Task 6 — 主循环接 `FragReassembler`（主循环 + harness）

- 下行分支：`decode_packet_meta` → `reassembler.accept` → `Some` 才 `resolve+inject`；`udp_sweep` tick 调 `reassembler.sweep`。`run_event_loop` 内持有 reassembler（独占无锁）。
- 测：harness（T7）覆盖端到端重组。
- commit：`feat(knife3): wire FragReassembler into downlink routing (T6)`。

## Task 7 — harness UDP 吞吐 + 分片 mock（TDD）

- MockUpstream 增「分片回灌」模式（payload>阈值 → 拆 `FRAG_TOTAL>1` 多 datagram）。
- `run_udp_throughput_scenario` → `UdpThroughputReport{ sent, echoed_intact, lost, pps, mbps }`。
- `tests/concurrency_harness.rs`：① 大包经分片→主循环重组→echo 完整（重组正确性）；② 持续 UDP 吞吐、UDP 不被 TCP 饿死。
- commit：`test(knife3): UDP throughput + fragment-reassembly harness (T7)`。

## Task 8 — MTU / datagram config（`quic.rs`）

- `EndpointConfig::max_udp_payload_size` 显式设（`Endpoint::new` + 自定义 EndpointConfig）；连上 log `max_datagram_size()`；`initial_mtu/min_mtu` 不动。
- 测：config 构建 + endpoint bind 绿（现有测扩展）。
- commit：`feat(knife3): raise max_udp_payload_size + log datagram ceiling (T8)`。

## Task 9 — 收尾

- `/code-review` over diff → 修。
- 真出口 acceptance（需用户 `MINI_VPN_TUIC_*` env）：真 sing-box 持续高码率 UDP → 测丢包/重组互通/`max_datagram_size` 真上限 / #3 单连接是否需连接池。续写 findings 末节。
- 更新 HANDOFF（刀3 完成、刀4 入口）。

## 执行顺序与依赖

```
T0 → T1 ┐
        ├→ T6 → T7 ─┐
T2 ─────┘           ├→ T9
T3 → T4             │
T5 ─────────────────┘
T8（独立，随时）
```
T1/T2/T3 纯逻辑可连续做；T6 依赖 T1+T2；T7 依赖 T6；T4 依赖 T3；T5 独立 I/O；T8 独立。

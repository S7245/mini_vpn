# HANDOFF — mini_vpn core 路线（达成 Rules.md 用户使用目标）

给后续 **逐刀接力的新 session**。每刀单独开 session（省 token），按本文件冷启动。

## 当前状态（基线）

- **Stage 13 + 刀1 + 刀2 + 刀3 + 刀3.5 + 刀4 + 刀5 全部已在 `main`**（`e589767`，2026-06-22 fast-forward 合入，与 origin 同步）。
  数据面 = **client-only TUIC over quinn → sing-box**（ADR-0004）；UDP 默认 **native datagram + Cubic**（刀3.5）；
  **拦截加密 DNS** 逼回落明文 → fake-IP（刀4，ADR-0006）；**拦全 :53 裸包 DNS 劫持**——任意 resolver(如 8.8.8.8:53)的明文
  查询都本地伪造 fake-IP(裸包构造,src=被查询的 resolver)、废 smoltcp DNS socket，fake-IP 不再依赖系统 DNS 指向 198.18.0.1
  （刀5，ADR-0007）。见下「刀5 完成」。
- **Stage 13 全部完成**：13a TCP via TUIC Connect ✅、13b UDP via TUIC Packet ✅、13c 按需 heartbeat（0-RTT 撞 quinn 0.10 墙、deferred）✅、13d 退役 legacy（删 yamux/自研 server/双轨开关/6 个依赖）✅。
- **刀1/2/3/3.5 完成**（见下各「已完成」段）：并发压测 harness + 大并发优化（脏集合 + 弹性扩容 + fake-IP 回收）+ UDP 直播硬化（quic-stream 兜底 + 分片重组）+ 高码率 UDP（BBR/Cubic 可切 + quinn 插桩 + quic-relay-mode；**纠偏：刀3「5.3M datagram 天花板」实为链路 cap 假象**）。
- **刀6 进行中（分支 `claude/knife6-reality-transport`，从 main 起，未合 main）**：正交线 A REALITY 第二 Transport
  的**第一片**——离线 auth 密码学 + TLS 1.3 ClientHello（手写，ADR-0008）。REALITY 是 2–3 刀 mini-project（刀6→刀9，见上）。
  **下一刀=刀7**（从 `claude/knife6-reality-transport` 续，或刀6 合 main 后从 main 起）：ServerHello+key schedule+record。
- **新 session 起点（若开主线而非续 REALITY）：从 `main`（`e589767`）起新分支**。拦全:53 已由刀5 达成。
  主线下一候选：高带宽压测+数据面多线程（逼近 100M）/
  observability(DNS forge 计数、datagram drop 背压可观测)——按优先级定。
  **一个分支只能一个 writer**，每次 commit 后立即 `git push`（曾发生过并发会话 clobber commit）。

## 目标（唯一北极星）：`Rules.md`

```
① TCP 连接   ② UDP 视频直播   ③ 大并发连接
```
- ① 基本达标（curl HTTPS 端到端 TLS、~415KB 反复下载无 bad-decrypt）。
- ② 部分：DNS 验证过，但直播是持续大流量 UDP，**native datagram 超上限的包直接丢**（quic-stream fallback 未实现），未压测。
- ③ **未达标，主战场**（见下"已知瓶颈"）。

> 范围边界：前端/桌面/移动 App + 云端 backend 在**独立仓 `mini_vpn_app`**（契约先行，另一个 session 设计架构）。**core 仓只做数据面**，不碰 GUI/backend；library 化 / `local-control` 接入由前端 session 主导，**不在本路线内**——本路线只把 Rules.md 三目标做达标。

## First: ground yourself

- 读 **`Rules.md`**（三目标）、**本 HANDOFF**、`docs/adr/0004-tuic-protocol-data-plane.md`、`TODO.md`（"Scale & reconnection"、"fake-IP / DNS"、"Mobile readiness" 段）、`.learnings/LEARNINGS.md`（尤其 Stage 12 的并发/echo/定位教训）。
- 关键源（用符号定位，行号会变）：
  - `src/client_tun.rs`：`start_tun_proxy`（单 `tokio::select!` 主循环：`global_rx` TCP 回程 / `device.wait_for_rx` rx 分流 / `tuic_downlink_rx` UDP 下行 / `udp_sweep` / `timer`）；`ListenerRegistry`（SYN inspector 动态建端口池，`MAX_INTERCEPTED_PORTS=64`，每端口 `pool_size` 默认 2）；`process_listener_activity` / `handle_local_payload` / `spawn_remote_relay`（TCP relay 通用回程）；`handle_tuic_udp_uplink`（UDP 上行）。
  - `src/tuic.rs`：`TuicUpstream`（**单条** QUIC 连接，`live_conn` 自重连，`open_tcp` 开 Connect bi-stream，`send_udp`，`start_udp` 下行泵+按需 heartbeat）；`AssocTable`（u16 assoc-id per UDP 4-tuple）；`encode_packet`/`decode_packet`。
  - `src/udp_relay.rs`：`FourTuple`/`FlowEntry`/`parse_inbound_udp`/`build_udp_ip_packet`/`MAX_UDP_FLOWS=1024`/`UDP_FLOW_IDLE_SECS=60`。
  - `src/quic.rs`：client QUIC config（keepalive 5s / idle 30s / initial_mtu 1280 / early_data toggle）。
  - `src/fake_ip.rs`：198.18.0.0/15 池，alloc/resolve，**永不回收**。`src/dns.rs`：本地 fake-A 应答（仅 198.18.0.1:53）。

## Core 路线（按此逐刀，每刀新 session）

```
主线（Rules.md 三目标）
 ├─ 刀1  大并发压测 harness（先定位真瓶颈，事实先行）  ✅ 完成（见下「刀1 已完成」）
 ├─ 刀2  大并发优化（#1 脏集合 + #2 弹性扩容 + fake-IP 引用计数回收）  ✅ 完成（见下「刀2 已完成」）
 ├─ 刀3  UDP 直播硬化（quic-stream fallback + 吞吐压测 + MSS/MTU）  ✅ 完成 + 真出口 acceptance（见下「刀3」）
 ├─ 刀3.5 高码率 UDP（quinn 插桩 + CC 调优）  ✅ 完成 + 真出口 acceptance（见下「刀3.5」）；纠偏：5.3M「天花板」实为链路 cap 假象
 ├─ 刀4  连接成功率（拦截加密 DNS DoT/DoH/DoQ/DoH3）  ✅ 完成 + 真出口 acceptance（见下「刀4」）；first-SYN 已确认 knife2 修复、关闭
 ├─ 刀5  拦全:53 裸包 DNS 劫持（任意 resolver 明文→fake-IP，废 smoltcp DNS socket）  ✅ 完成 + 真出口 acceptance（见下「刀5」，ADR-0007）；已合 main
 └─ （主线下一候选）高带宽多线程逼近 100M / DNS·datagram observability

正交线 A（抗封锁韧性，不阻塞主线；QUIC 被 GFW 封时才必需）= VLESS+REALITY 第二 Transport（手写 TLS 1.3，ADR-0008）
 ├─ 刀6  REALITY auth 密码学 + TLS 1.3 ClientHello 构造（sans-IO，100% 离线 TDD）  ✅ 完成（见下「刀6」）；未合 main
 ├─ 刀7  ServerHello 解析 + TLS 1.3 key schedule（RFC 8448 向量）+ record-layer AEAD  ← REALITY 下一片
 ├─ 刀8  server-flight 解密 + HMAC 证书校验 + client Finished + 实 TCP 握手 + VLESS 帧 + RealityUpstream(ProxyUpstream) + env 选择器 + 真出口 acceptance
 └─ 刀9  auto-failover（健康感知 TUIC↔REALITY；分离 TCP/UDP 上游；UDP 留 QUIC）
```
- 优先级与关联：**fake-IP 池回收**属"大并发长稳"（并入刀2）；**DoH 拦截**是"真实场景能连上"的前置（刀4，可视情提前——真机浏览器场景不修则 fake-IP 形同虚设）。**A（REALITY）正交**：当前 QUIC 能连，不阻塞三目标达标；TCP-based，替代不了 UDP 直播。

## 刀1 已完成（2026-06-12）：大并发压测 harness + 瓶颈定位

**交付**（分支 `claude/knife1-concurrency-harness`，从 main 起，已逐 commit push；未合 main）：
- 重构：`start_tun_proxy` 抽成 `run_event_loop<D: TunIo, U: ProxyUpstream+DatagramUpstream, M: MetricsSink>`
  （生产/测试同一份循环，零回归）；新增 `TunIo`(device.rs)/`DatagramUpstream`(upstream.rs)/`MetricsSink`。
  client_tun/device/dns/fake_ip 搬进 library（tests/ 整合测试可达）。
- harness：`src/harness.rs`（feature `harness`）= 内存回环 device + mock echo 上游 + 第二 smoltcp 流量发生器，
  对外高层 `run_tcp_scenario`/`run_udp_echo_scenario` → `Report`。`tests/concurrency_harness.rs` 跑 N sweep。
  跑法：`cargo test --features harness --test concurrency_harness -- [--ignored] --nocapture`。
- **定位结论：`docs/tech/2026-06-12-knife1-bottleneck-findings.md`**（spec/plan 同目录 `2026-06-12-knife1-*`）。

**瓶颈裁决（指向刀2）**：
1. ✅ **P0 #1 `all_handles()` O(总 listener 槽数) 全量 sweep**（主因）：relay/call 线性于 `端口×pool`、与活跃连接无关。
2. ✅ **P0 #2 每端口 `pool_size` 硬并发上限**：单端口 pool=2 下 256 路只完成 2/256（热门端口 stall，与 first-SYN-refused 重叠）。
3. ⏸ **#3 单条 QUIC 连接** mock 测不到（无网络拥塞）→ deferred，findings 附端到端 sing-box probe 配方。
4. ✅ **P1 #4 单线程 select 上限**（吞吐随每-tick 开销跌；与 #1 强耦合）。
5. ✅ **P2 #5 128KB/socket**（2048 槽≈256MB，多为 #1 空扫的空闲槽）。

## 刀2 已完成（2026-06-15）：大并发优化 + fake-IP 引用计数回收

**交付**（分支 `claude/knife2-concurrency-opt`，从 main 起，逐 commit push；未合 main）：
- **#1 脏集合驱动**：主循环 relay 段从每 tick 全量 `all_handles()` O(总槽) sweep → 只处理脏集合
  （`dirty: HashSet<SocketHandle>`，inbound TCP 包按 dst_port 标脏 + 回程残留 pending 标脏）。
- **#2 弹性扩容**：`ensure_spare_listeners` 按需补足空闲 Listen 槽（看 smoltcp `state()==Listen`），
  全局 `MAX_TOTAL_LISTENERS=4096` 兜底；打掉每端口 pool 硬上限。
- **fake-IP 引用计数回收**：`FakeIpPool` 每映射 refcount+last_used；TCP（`SocketCtx.fake_ip`）/
  UDP（`AssocTable` id→fake-IP）两条 flow 打通 acquire/release；周期 sweep（60s tick，TTL=1800s）；
  `reap_dead_slots`（1s tick）回收本地关闭/开远端失败的死槽 → 释放 refcount + 槽复用。
- spec/plan/findings 续篇：`docs/tech/2026-06-15-knife2-concurrency-opt-*` + findings「刀2 优化结果」。

**量化（harness，优化前→后）**：#1 relay 段不再随总槽线性翻倍（N=1024 relay 1618ms→71ms，
吞吐 2.50→6.22 Mb/s）；#2 单端口 256 路 done 2/256(20s stall)→256/256(266ms)。
`/code-review`（high effort）8 条 findings 已全部修复（核心：teardown 死槽回收修 refcount/槽泄漏）。

**真出口 acceptance ✅（2026-06-15，深圳 client → 47.251.188.205 sing-box，IP 直连 1.1.1.1:443）**：
- ① TCP+TLS：curl HTTPS `TLS_verify=0`，三端日志闭环（client `relay→rearm` / server `inbound→outbound to 1.1.1.1:443`）。
- ③ 大并发：200 路并发**全压单端口 :443** → `200 301` 全成功、零 `000` 超时（#2 弹性扩容真实生效；优化前此处 stall 2/256）。
- #3 probe：200 路 `time_total` p50=0.379 / p95=0.491 / max=0.557s（max≈1.47×p50，分布极平）→
  **单条 QUIC 连接在此负载下无队头/拥塞瓶颈，暂不需连接池**；更高负载/真直播大流量再评估（归刀3）。

**未做（deferred）**：#4 多线程化（#1 后 poll/smoltcp 段成新瓶颈，留后续评估）；#3 连接池视刀3 更高负载/真直播再定。
CloseWait+远端 keepalive 的半关闭已被 `reap_dead_slots` 覆盖（CloseWait 视为应用关闭 teardown）。

## 刀3 实现完成（2026-06-16）：UDP 直播硬化（quic-stream fallback + 分片重组 + 吞吐压测）

**交付**（分支 `claude/knife3-udp-streaming`，从 main 起，逐 commit push；未合 main）：
- **上行 stream 兜底**：`send_udp` 按 `udp_send_plan(max_datagram_size, len)` 主动分流——装得下走 native
  datagram，超上限/不可用走 **per-packet uni-stream**（`open_uni`/`write_all`/`finish`，复用同一 `encode_packet`
  字节）；datagram `TooLarge` 竞态二次兜底。新增 `udp_stream_fallbacks` 计数。持续大流量直播不再丢大包。
- **下行接收 + 分片重组**：`start_udp` 增 `accept_uni` 分支（有界 `Semaphore`=256，超额丢弃防 flood）；
  `decode_packet_meta`（frag 感知）+ `FragReassembler`（纯状态机，主循环独占）重组 server native 模式的
  大下行分片（FRAG_TOTAL>1）。datagram + uni-stream 两路汇同一下行 channel，主循环统一 decode+重组。
  重复 frag last-writer-wins（缓解跨重连 pkt_id 复用串味，残余 TTL=10s sweep 兜底）。
- **MTU/datagram**：维持 `initial_mtu/min_mtu=1280` floor（不黑洞）；`client_endpoint` 经 `Endpoint::new`
  显式设 `max_udp_payload_size=1472`（接收 headroom，可调）；连上 log `max_datagram_size()`（真上限）；
  每 30s 打 stream 兜底/丢弃统计行。**诚实结论**：发送 datagram 上限主约束是 MTU/PLPMTUD，非 max_udp_payload_size。
- **harness UDP 吞吐**：MockUpstream 加分片回灌模式（模拟 server 分片）；`run_udp_throughput_scenario`
  逐字节核对完整性。常驻测：分片 4000B/4 帧 + 直通各 16/16 intact；ignored sweep 500/500 intact（含 8000B/7 帧）。
- spec/plan：`docs/tech/2026-06-16-knife3-udp-streaming-{spec,plan}.md`；acceptance 配方续写 knife1 findings 末节。

**质量**：80 测全绿、`clippy --all-targets --features harness` 0 warning、release build 绿。
`/code-review`（high effort，7 角度）findings 已修（last-writer-wins 跨重连、去冗余 frag_total 字段、entry API、harness >255 帧断言）。

**真出口 acceptance ✅（2026-06-17，深圳 client → 47.x sing-box → 43.x iperf3，IP 直连，详见 findings 末节）**：
- ✅ **上行 quic-stream 兜底实锤**：1400B 包（>datagram 上限 N=1332）全走 uni-stream → **50Mbps / 0.037% 丢**
  （改造前这些包 100% 被 TooLarge 丢）。刀3 核心目标达成。
- ✅ **典型直播码率达标**：≤5Mbps native datagram 下行 1.7% 丢（视频可用）。
- ❗ **native datagram 有 ~5.3Mbps 硬天花板**（上/下行两方向都卡，与 offered 无关；stream 同链路跑满 50M）
  → 是 QUIC 不可靠 datagram 的传输特性（高 RTT + 不重传 + 无背压），非客户端小改可解。
  **试过下行批量 flush（摊销每包 syscall）→ 零效果，已 revert**（坐实瓶颈不在我方消费端）。
- **观测盲点**：datagram 丢包 quinn 不报错（`send_datagram` 仍 Ok），`udp_drops` 看不到 → 需后续补背压可观测。

**新发现 → 刀3.5（高码率 UDP）**：高码率（>5M）直播需要「高码率流走 stream / datagram 加 pacing+背压 / 评估连接池」，
带 quinn 级 instrumentation（RTT/cwnd/datagram drop）量化后定方向。**#3 裁决**：单连接非连接数瓶颈（stream 跑满 50M），
瓶颈是 datagram 传输特性。

**harness 边界**：测不到真 quinn 的 datagram TooLarge / stream 兜底 / 真分片 / datagram 吞吐天花板（同 #3，需真出口）。

## 刀3.5 代码完成（2026-06-17）：高码率 UDP 硬化（BBR + 插桩 + quic-relay-mode）

**交付**（分支 `claude/knife35-highrate-udp`，从 main 起，逐 commit push；**已 fast-forward 合入 main `591a629`**）：
- **接 BBR**：`congestion_control` 字段（存而未用）→ `quic_transport_config` 的 `congestion_controller_factory`
  （`bbr→BbrConfig`、`cubic→CubicConfig`、未知→Cubic+告警）；env `MINI_VPN_TUIC_CC` 可切（A/B 归因）。已查证 quinn-proto 0.10.6 导出 BBR。
- **quic-relay-mode 接线**：`UdpRelayMode{Native,Quic}` + mode 感知 `udp_send_plan`（`Quic`→恒 uni-stream；
  `Native`→刀3 size-based）；`udp_relay_mode` 字段（存而未用）→ env `MINI_VPN_TUIC_UDP_MODE` 可切。
  **设计依据**（SPEC 已查证）：server 按 assoc **首包** mode 镜像下行 → `Quic` 全 UDP 首包即 stream → 下行也镜像 stream，摆脱 datagram 天花板。下行接收（`accept_uni`/`FragReassembler`）刀3 已就绪、不改。
- **抬 `max_concurrent_uni_streams` 100→4096**：避 TUIC issue #221（per-packet uni-stream 耗尽配额 → 下行塌缩）。
- **quinn 级插桩**：30s `📊` 行加 `RTT/cwnd/lost/sent`（`conn.stats().path`）+ `send_buffer_space` 背压代理信号
  （补刀3 盲点：datagram 缓冲溢出丢最老不报错）；连上打实际生效 CC + mode。
- spec/plan：`docs/tech/2026-06-17-knife35-highrate-udp-{spec,plan}.md`；acceptance 配方续写 knife1 findings 末节（T-A~T-H）。

**质量**：82 lib 测 + 6 harness 常驻测全绿、`clippy --all-targets --features harness` 0 warning、release build 绿。
`/code-review`（high effort，7 角度）findings 已修 3 条（A: fallback 计数只算 Native 真兜底，避 quic 模式 `📊` 误读；
B: 背压警告门控 Native；C: 去重 MTU floor 常量）。

**真出口 acceptance ✅（2026-06-17，深圳 client → 47.x sing-box → 43.x iperf3，**两端链路升到 80M**）**：
- **🔑 最大纠偏**：刀3「~5.3M datagram 硬天花板」是 **5M VPS 链路 cap 的测量假象**，非 QUIC datagram 限制。
  80M 链路下 native datagram 下行 **39.8M/0.25%**、上行 37.5M/4.5%。**插桩（cwnd/RTT/loss）揭穿真相**（先量化、别凭猜）。
- **CC 裁决**：datagram 数据面 **Cubic 完胜 BBR**（40M 下 0.25% vs 24%；BBR cwnd 暴涨 245K/RTT bufferbloat、
  对不可靠 datagram 过驱）。→ **默认改 cubic**（`MINI_VPN_TUIC_CC=bbr` 仍可显式选）。
- **mode 裁决**：**默认 native（datagram）**——4K(25M) 富余且低延迟；quic 全 stream 模式高码率灾难（40M→7M/71%，cwnd 4.5MB）。
  **quic 模式保留为可配置选项**（代码完成+测过；抗封锁场景或有用，非高码率推荐）。
- **多 flow gate**：2 路并发单连接聚合 ~34M ≥ 33M → **连接池 defer 坐实**。
- **carve-out 不需要**：默认 native → DNS/小流本就走低延迟 datagram。
- ADR：`docs/adr/0005-cubic-over-bbr-datagram.md`（CC 选择 + 天花板假象纠偏）。findings 末节有完整数据表。
- **T-H 真实 soak ✅**（专用测试机，native+cubic）：YouTube **4K 不卡顿**；累计丢包 ~0.31%、`丢弃=0`、RTT ~170ms 稳、
  无重连风暴/映射丢弃洪水；末尾一次 PMTU/拥塞事件被大包 uni-stream 兜底优雅吸收。carve-out 不需要（DNS/小流走 datagram）。
- acceptance helper 入库：`scripts/knife35-acceptance.sh`（可移植，start/soak/stop/soak-stop，凭据读 env）。

**本刀的真实价值**（前提被纠偏后）：① quinn 级插桩（揭穿假象 + 纠正 CC）；② CC 调优（默认 cubic）；
③ 证实 native datagram 本就够高码率、避免上线不必要的全-stream 复杂度；④ quic-relay-mode 能力（备用/抗封锁）。

**code-review defer（非本刀阻塞，后续按需）**：
- `from_sources` 未收 `cc`/`udp_mode` 参数 → 仅 env 可切；**前端/移动端经 file/FFI 注入 config 时需补**（`TuicClientConfig` 字段注释已述 FFI 注入计划）。
- `parse_cc`（返回 `(choice,bool)`）与 `UdpRelayMode::parse`（返回 `Option`）双 idiom + connect() 两段近似 warn 块 → 可统一（纯美化）。
- `max_concurrent_uni_streams=4096` 经共享 `quic_transport_config` 也作用于 legacy `client_quic_config`（仅测试用、无害；ceiling 非预分配）。
- `udp_drops` 混合 datagram-send-fail 与 uni-stream-fail 两类（acceptance 归因时留意）。
- **acceptance 后**：按 gate 定默认 mode → 补 `docs/adr/0005-*`；按 T-F/T-H 定是否补 DNS/小流 carve-out。

## 刀4 代码完成（2026-06-18）：连接成功率（拦截加密 DNS）

**交付**（分支 `claude/knife4-connect-success`，从 main 起，逐 commit push；**已 fast-forward 合入 main `cd9ff62`**）：
- **对症**：浏览器/系统用**加密 DNS**(DoH:443/DoT:853/DoQ:UDP853/DoH3:QUIC443)拿真实 IP → 绕过 fake-IP →
  真实 IP 没进隧道 → GFW 墙 → **连接失败**。
- **做法**：新 `src/dns_block.rs`（`is_encrypted_dns_port`/`is_doh_domain`/`is_doh_ip` + 内置 DoH 域名/IP 名单）；
  `resolve_target` 加 **`Block`** 变体(:853 任意 IP / :443+fake-IP 域名∈DoH名单 / :443+非fake IP∈DoH-IP名单)，
  一处决策天然覆盖 TCP+UDP 两路径。**TCP→RST**(复用 `rearm_socket`)、**UDP→静默丢包**(热路径勿 println)。
  逼应用回落明文 :53 → 我方伪造 fake-IP → 进隧道。:443 **仅按名单精确判**，不碰普通 HTTPS/QUIC。
- **质量**：87 lib 测绿、`clippy --all-targets --features harness` 0 warning。`/code-review`(9 角度)findings 已处理
  （真 bug：UDP Block 逐包 println 洪水 → 改静默丢弃；补 dns.google.com）。
- **设计文档**：`docs/tech/2026-06-18-knife4-connect-success-{spec,plan}.md`；ADR `docs/adr/0006-block-encrypted-dns.md`。

**deferred（grill 决策）**：
- ~~**拦全 :53**（任意 resolver 明文查询都伪造）~~ → ✅ **刀5 已做**（裸包 DNS 路径、废 smoltcp DNS socket、ADR-0007）；
  无缝 on/off 不依赖系统 DNS 的关键拼图就位（配合前端 NE）。
- **first-SYN-to-fresh-fake-IP refused**：静态分析表明已被 **knife2 同帧 `ensure_port`+`ensure_spare_listeners` 修**
  （HANDOFF 原条目疑陈旧）→ 仅 acceptance 探针验证(`curl rc=7≈0`)，复现才回头查。
- **harness Block 端到端**：harness 连固定 TARGET_IP、FakeIpPool 不可注入 DoH 映射 → 降级 acceptance（Block 决策已全分支单测）。

**真出口 acceptance ✅（2026-06-18，深圳测试机）**：
- **K4-A DoH 拦截**：Chrome 开「安全 DNS=Cloudflare」→ `🛡️ 阻断加密 DNS cloudflare-dns.com(@fake-IP:443) → RST` 命中
  (域名识别经 fake-IP 真生效)→ 浏览器回落明文 → fake-IP → 正常上网。
- **K4-C 回归**：DoH 关 → 明文 DNS 健康(FB/IG/YT 全 `🪪→fake-IP`)、无误伤。
- **K4-D first-SYN**：探针 375 总 / rc=7=**0** → 竞态不复现、**确认 knife2 已修**(HANDOFF 原条目陈旧、关闭)。
- 小改：TCP block 日志显**解析域名**(便于核对/调名单)。**→ 刀4 完成**（代码+单测+ADR-0006+acceptance）。

## 刀5 代码完成（2026-06-22）：拦全 :53 裸包 DNS 劫持

**交付**（分支 `claude/knife5-dns-hijack`，从 main 起，逐 commit push；**已 fast-forward 合入 main `e589767`**）：
- **对症**：刀4 逼应用回落明文 DNS，但应用回落到的是**它自己配的 resolver**（如 `8.8.8.8:53`），非 198.18.0.1。
  原 `classify_inbound` 仅伪造 `198.18.0.1:53`、其它 :53 隧道转发真 DNS → 真实 IP 绕过 fake-IP（仅"模型 a 系统 DNS=198.18.0.1"下不漏）。
- **做法**（grill 4 裁决 + ADR-0007）：① **裸包**——`classify_inbound` 任意 `:53`→`Dns`，新 `forge_dns_reply`(纯)
  伪造 fake-IP 回包(`src=被查询的 resolver`)，`handle_dns_hijack` 经 `inject_ip_packet`+`flush_tx` 注入（复用 UDP relay 下行机制，
  smoltcp 无法为无界 resolver IP 设 src）。② **废 smoltcp DNS socket**——删 `dns_handle`/`bind`/接口 IP `198.18.0.1/32`/`drain_dns`/
  `FAKE_DNS_RESOLVER`，统一一条裸包路径（含 198.18.0.1）。③ **全劫持**不按 dst 过滤。④ **TCP :53 → RST**：
  `dns_block::is_dns_relay_port`(53||853) → `resolve_target` Block（不变量：UDP :53 已被 classify 截走 → port==53 只命中 TCP）。
- **质量**：93 lib 测（含 forge_dns_reply 5 测）+ 6 harness 测绿、`clippy --all-targets --features harness` 0 warning、release build 绿。
  `/code-review`(high effort,7 角度) **零正确性 bug**（独立追踪确认 UDP :53 永不到 resolve_target）；唯一动手=在 classify_inbound 标注 load-bearing 不变量。
- **设计文档**：`docs/tech/2026-06-22-knife5-dns-hijack-{spec,plan}.md`；ADR `docs/adr/0007-hijack-all-plaintext-dns.md`；CONTEXT.md 词汇表更新。

**真出口 acceptance ✅（2026-06-22，测试机，native+cubic 全局隧道，系统 DNS=8.8.8.8 非我方 resolver）**：
- **K5-1 核心**：`dig @8.8.8.8 example.com` → `198.18.0.36`(fake-IP) → **系统 DNS≠198.18.0.1 时任意 :53 仍被劫持，北极星达成**。
- **K5-2**：`dig +tcp @8.8.8.8` → connection reset、无 IP（TCP :53 RST，无 real-IP 泄漏）。
- **K5-3**：`curl https://example.com` → HTTP/2 200（fake-IP→DomainPort→隧道）。
- **K5-4**：google/github/cloudflare 全 fake-IP，零逃逸。**K5-5**：apple/icloud/google 等真实 app `🪪→fake-IP`。
- **刀4↔刀5 闭环**：`dns.google` 明文解析→fake-IP，该 fake-IP:443 再命中刀4 DoH Block。
- helper：`scripts/knife35-acceptance.sh soak-knife5`（DNS=8.8.8.8 + alt-resolver 路由进 TUN）。
- **已知限制**（未触发）：split-horizon/内网域名走出口解析、exotic 多 question 查询丢弃(不泄漏)、IPv6 :53 不劫持(crate ipv4-only)。
- **→ 刀5 完成**（代码+单测+ADR-0007+acceptance）。详见 findings 末节「刀5」。

## 刀6 代码完成（2026-06-22）：REALITY 第二 Transport — 离线 auth + ClientHello（mini-project 第一片）

**交付**（分支 `claude/knife6-reality-transport`，从 main 起，逐 commit push；**未合 main**；本片 **sans-IO、100% 离线**，无真握手/无 acceptance）：
- **背景**：正交线 A = 给 Upstream 加第二 Transport（VLESS over REALITY over TCP，抗封锁 fallback）。REALITY 把 auth 藏进 TLS ClientHello `session_id`，stock TLS 库不让写 → **手写 TLS 1.3**（shoes 蓝本），RustCrypto 仅作原语（不破 ADR-0003 单 rustls）。grill 决策：**boring/craftls 均否决**（boring 写不了 session_id 需 patch C；craftls 只给指纹）→ 手写（ADR-0008）。
- **本片做了**（`src/reality/{auth,client_hello}.rs`）：① `auth`：x25519 ECDH(RFC 7748 KAT)、`derive_auth_key`=HKDF-SHA256(salt=random[0..20],info="REALITY",32B)、session_id 16B 布局、`seal/open_session_id`=**AES-256-GCM 完整 32B key**(nonce=random[20..32],AAD=session_id 清零的 ClientHello)、`verify_server_cert`=HMAC-SHA512(同 32B key, ed25519 pubkey)；② `client_hello`：手写 TLS 1.3 ClientHello(Chrome-like:GREASE+X25519 keyshare+ALPN+扩展序;supported_versions 仅 1.3)、`build_authed_client_hello`(建零 session_id→seal→回写 offset 39..71)。
- **质量**：12 reality 单测（含 RFC 7748 KAT + **server-view round-trip**：ECDH→derive→encode→seal→AAD清零→解封全链 + 篡改 ClientHello→解封失败）+ 105 lib 测全绿、clippy 0 warning。`/code-review`(high effort)：零正确性 bug，修了过时 AES-128 文档(实为 AES-256)、命名 session_id 偏移常量、x25519 低阶点安全注记。
- **🔑 查证锁定的互通关键（刀7/8 别再踩）**：REALITY session_id AEAD = **AES-256-GCM + 完整 32B AuthKey（不截断！）**；用 AES-128/截断会让 sing-box 静默拒绝回落 decoy。HKDF salt=random[:20]、info="REALITY"、L=32。AAD = handshake message（含 4B 头），session_id 区 32B 清零。证书校验 HMAC-SHA512 用同一 32B key。
- **设计文档**：`docs/tech/2026-06-22-knife6-reality-transport-{spec,plan}.md`；ADR-0008；CONTEXT.md 加 Transport/VLESS/REALITY。
- **deferred（刀7/8/9）**：ServerHello+key schedule+record（刀7）；server-flight 验证+Finished+实握手+VLESS+RealityUpstream+acceptance（刀8，需服务端 VLESS+REALITY inbound 空 flow）；failover（刀9）。**刀7 起 x25519 用于网络 server keyshare → 必须加 contributory/全零检查**（见 auth.rs 注）。

## Rhythm（每刀都遵守）

1. 新 session → 读本 HANDOFF + `Rules.md` → 先 **grill**（用 `/grill-with-docs` 或 brainstorm，对齐设计与本刀范围）→ 出 **spec + plan**（docs/tech/，TDD 分解）。
2. **TDD per task**：写失败测试 → red → 实现 → green → commit；**每次 commit 后 `git push`**；一个分支一个 writer。
3. 收尾：**`/code-review`** over the diff → 修 → 跨机/压测 **acceptance**。
4. **cwd 陷阱**：Bash cwd 可能在 call 之间被重置到别的 worktree——每条 git/cargo 命令前 `cd` 到本 worktree 并用绝对路径编辑；`git branch --show-current` 应是本分支。
5. 文档/教学叙述（teaching note、LEARNINGS）由用户另行通过代码+commit 生成；**本路线只产 spec/plan + 代码 + commit + 必要的 TODO 状态**（除非用户另说）。
6. 用**中文**回复（代码/术语/commit 保留英文）。

## 已知坑 / deferred（接力时别重新踩）

- **0-RTT**：quinn 0.10 / rustls 0.21 在 0-RTT 阶段无法 `export_keying_material`，TUIC auth 必失败回落 1-RTT → `MINI_VPN_TUIC_ZERO_RTT` 默认关。真 0-RTT 需 quinn 升级（归移动端 stage），见 TODO 13c。
- **quic-stream UDP fallback** 未实现（native datagram 超上限丢弃）——刀3 要做。
- **DoH/DoT 绕过 fake-IP**：浏览器/系统加密 DNS 会拿到真实 IP → 连接失败——刀4。
- ~~**fake-IP 池永不回收**（198.18/15）~~——✅ 刀2 已修（引用计数活跃 flow + 60s sweep + 死槽回收）。
- **first-SYN-to-fresh-fake-IP `connection refused`**（SYN inspector 建池竞态，curl 不重试 refused）——刀4。
- 出口是 VPS datacenter IP → Google/Meta 风控（协议无关，记录即可）。

## Not in git（用户提供；真实/UDP 直播 acceptance 时需要）

- sing-box 互通参数（env）：`MINI_VPN_TUIC_SERVER=<VPS_IP>:8443`、`MINI_VPN_TUIC_UUID=<uuid>`、`MINI_VPN_TUIC_PASSWORD=<pass>`、`MINI_VPN_TUIC_SNI=example.com`、`MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem`、`MINI_VPN_TUIC_ALPN=h3`。（向用户要实际 UUID/password/IP，**勿入库**。）
- 启动：`sudo MINI_VPN_TUIC_* ./target/debug/mini_vpn client-tun`（13d 起 `MINI_VPN_UPSTREAM` 已删，恒 TUIC；`MINI_VPN_TUN_POOL_SIZE` 可调端口池）。
- **刀3.5 新增旋钮**（非凭据，可入库默认；env 覆盖）：`MINI_VPN_TUIC_CC=bbr|cubic`（默认 bbr）、`MINI_VPN_TUIC_UDP_MODE=native|quic`（默认 native，acceptance gate 后可能翻 quic）。
- 刀1 若走 mock-upstream 隔离压测，则**不需要** sing-box。
- **刀5 acceptance**：`sudo -E bash scripts/knife35-acceptance.sh soak-knife5`（设系统 DNS=8.8.8.8 非我方 resolver + 路由进 TUN，
  验证任意 :53 仍被劫持）；`soak-stop` 自动还原。`K5_RES` env 可换 alt-resolver。需同上 `MINI_VPN_TUIC_*` 凭据。

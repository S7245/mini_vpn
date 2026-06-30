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
- **刀6 已在 `main`（`b7785a2`，2026-06-22 fast-forward 合入）**：正交线 A REALITY 第二 Transport 的**第一片**——
  离线 auth 密码学 + TLS 1.3 ClientHello（手写 TLS 1.3，ADR-0008，sans-IO 无 acceptance）。REALITY 是 mini-project（刀6→刀9，见上）。
- **刀7 已在 `main`（`14258e4`，2026-06-23 fast-forward 合入）**：REALITY 第二片——离线 ServerHello 解析 +
  TLS 1.3 key schedule + record AEAD（手写，全程 RFC 8448 §3 KAT 字节级验证，sans-IO 无 acceptance，ADR-0009）。见下「刀7 完成」。
- **刀8 已完成（2026-06-24）+ 真出口 acceptance ✅，已 ff 合入 main（`a9172a0`）**：REALITY 收官片——实 TCP 握手 + 解密 server flight + 证书 HMAC + Finished + VLESS + `RealityUpstream` + env 选择器；**VLESS over REALITY over TCP 在真 sing-box 上端到端跑通**（HTTP 200 三端闭环）。见下「刀8 完成」。
- **刀9 已完成 + 真出口 acceptance ✅，已 ff 合入 main（`831afe3`，2026-06-25）**：REALITY mini-project 收尾 = auto-failover 主链。
  F2 分离 TCP/UDP 上游 + F3 M3 握手并发化 + F1 不对称 failover + F4 idle 超时。全链路 acceptance 通过（~10s 切 REALITY 200 / ~62s 切回 TUIC 200）；
  acceptance 逼出并修了 4 个检测坑（**主动 udp_rx 黑洞探测为主机制**，检测从 >80s→~10s）；两次对抗式 review（零正确性 bug）。**F5 KeyUpdate 拆到刀10**。见下「刀9 完成」。
- **刀10 已完成（2026-06-25）+ ✅ 已 ff 合入 main（`47b69bd`，2026-06-26）**：REALITY mini-project 的最后一片 = F5
  **TLS 1.3 KeyUpdate 密钥轮换**（RFC 8446 §4.6.3/§7.2/§5.3）。`RealityStream` 收到 post-handshake KeyUpdate 从刀8 占位
  loud-fail 改为正确轮换：`HandshakeOutput` 透出 `{s,c}_ap_secret` → 流持两 secret；`decode_one` 内层 `0x16` 逐 message 切，
  KeyUpdate(`0x18`) 调 `on_key_update`（步骤 A 总轮接收 `ExpandLabel(secret,"traffic upd","",32)`；`update_requested(1)`
  则 B1 旧 send key 封回发→B2 轮发送，铁律 B1<B2；`update_not_requested(0)` 只轮接收；非法值前置 Err 零 mutation）。
  poll_read 顶部机会性 flush（AsyncRead 补 `W:AsyncWrite` bound）使纯下载也即时回发。record.rs 不改。
  测试 T16 KAT/T17 时序铁律(crypto-evidence)/T18 recv-only+非法值/T19 端到端 loopback/T20a coalesced record；
  质量门：lib+harness 180 绿 / clippy --all-targets --features harness 0 / release 绿。对抗式 review(5 lens)+/code-review+test-rigor
  **零正确性 bug**（修 1 个 minor：coalesced `[NST][KeyUpdate]` 逐 message 切；1 nit + 注释诚实化）。
  **acceptance**：server-initiated KeyUpdate 不可由客户端诱发、生产服务端极少发 → 以 T19 loopback（真 read/write 路径 + 真
  KeyUpdate + 双向轮换）为高保真替身；真出口 KeyUpdate 未触发，如实记录（brief §8 T20「尽力而为」）。spec=`docs/tech/2026-06-25-knife10-keyupdate-spec.md`，gap 收口见 ADR-0010。
  **刀8 泄漏凭据已服务端轮换（2026-06-26）——安全遗留项关闭。**
- **REALITY mini-project（刀6→刀10）全部完成。刀11 数据面可观测性（observability）✅ 全部完成 + 已 ff 合入 main `9de0604`（2026-06-26，代码 + 两轮 review 零 bug + 真出口 acceptance ✅）**——见下「刀11 完成」。
  **刀12（多核逼近 100M，quantify-only）已完成 + 已 ff 合入 main `68b5e56`（2026-06-27）**（见下「刀12 完成」）——
  LoopProfiler 仪器 + 真出口归因 → **#4（单核 smoltcp poll = 天花板）实测推翻、取消事件循环分片**；当前墙是 WAN 跨太平洋路径，
  100M 此路不可达；#3 连接池留低 RTT 胖链路再测（ADR-0013）。
- **刀13 已完成 + 已 ff 合入 main `8be4141`（2026-06-28）**：主循环热路径净化（见 `docs/tech/2026-06-27-knife13-loop-hotpath-spec.md`）。
  ① 热路径 `println!` 由 `MINI_VPN_TRACE` 门控，默认不再每包/每连接同步写 stdout；② TCP uplink 改非阻塞
  `try_reserve`，Full 时不 `recv`、不分配、保持 smoltcp rx buffer 字节，靠 TCP 窗口端到端背压，修复一条慢流
  HoL 阻塞整个事件循环的问题。质量门：`cargo test` / `cargo test --features harness` / clippy / release 绿；
  新 harness `stalled_tcp_uplink_does_not_block_other_flows` 覆盖慢流不阻塞快流。**下一刀：刀14a 文档收口 + 刀14b 低 RTT 胖链路 #3 量化 gate**
  （spec/plan：`docs/tech/2026-06-28-knife14b-lowrtt-cc-pool-quantify-{spec,plan}.md`；probe：
  `scripts/knife14b-lowrtt-probe.sh`）。
- **2026-06-30 刀14b 真 US-client 测试已跑出决定性结果**：Client=`43.172.75.27`、Exit=`43.153.32.33`、
  Target=`43.130.32.77:5201`，路由和 TUIC 都正确；MTU 1500 forward P1 只有 `476 Kbit/s`，MTU 1200 forward P1
  提升到约 `29-33 Mbit/s`，但 reverse P1 仍只有 `2.06 Mbit/s`，P2+ 出现 iperf result/control reset。
  **裁决：先不要做 connection pool**；P1 reverse 已坏，下一刀应是 **刀14c：TCP downlink/backpressure instrumentation +
  MTU/MSS fix**。证据和任务树见 `docs/tech/2026-06-30-knife14b-usclient-results.md`。
  **一个分支只能一个 writer**，每次 commit 后立即 `git push`（曾发生过并发会话 clobber commit）。

## 目标（唯一北极星）：`Rules.md`

```
① TCP 连接   ② UDP 视频直播   ③ 大并发连接
```
- ① 基本达标（curl HTTPS 端到端 TLS、~415KB 反复下载无 bad-decrypt；TUIC/REALITY 两腿均真出口通过）。
- ② 基本达标于当前真出口条件（刀3/3.5：oversized UDP stream 兜底 + native/cubic 高码率；YouTube 4K soak 通过）。
  仍需在新网络/新服务端上按 acceptance 复测。
- ③ 大并发主路径已修两类 client-side 问题（刀2 脏集合/弹性扩容/fake-IP 回收，刀13 非阻塞 uplink 去跨流 HoL）。
  剩余吞吐杠杆是 **#3 单 QUIC connection / connection pool**，但只在低 RTT、端到端 >100M 胖链路上才值得量化。

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
 ├─ 刀11 数据面可观测性（DNS forge 计数 + datagram drop/背压 + 统一快照 MetricsSnapshot）  ✅ **完成（代码 + 两轮 review 零 bug + 真出口 acceptance ✅）+ 已 ff 合入 main `9de0604`**（见下「刀11 完成」）
 ├─ 刀12 多核逼近 100M：量化定位（quantify-only，LoopProfiler 仪器）  ✅ 完成 + 真出口归因（见下「刀12 完成」，ADR-0013）；**#4 实测推翻、取消分片**
 ├─ 刀13 主循环热路径净化  ✅ 完成 + 已合 main `8be4141`：`MINI_VPN_TRACE` 门控热路径日志 + 非阻塞 TCP uplink（try_reserve，Full 保留 smoltcp 字节）消除跨流 HoL
 ├─ 刀14a 文档/接力收口：把刀13 从候选改为已完成，修 stale TODO/HANDOFF/ADR 指针  ✅ 已完成
 ├─ 刀14b 低 RTT 胖链路 #3 量化 gate：probe/spec/acceptance + US Client VPS 实测  ✅ 已完成；结论是不进 pool
 └─ 刀14c TCP downlink/backpressure instrumentation + MTU/MSS fix：修 reverse P1 2M / P2 reset，再决定 pool

正交线 A（抗封锁韧性，不阻塞主线；QUIC 被 GFW 封时才必需）= VLESS+REALITY 第二 Transport（手写 TLS 1.3，ADR-0008）
 ├─ 刀6  REALITY auth 密码学 + TLS 1.3 ClientHello 构造（sans-IO，100% 离线 TDD）  ✅ 完成（见下「刀6」，ADR-0008）；已合 main
 ├─ 刀7  ServerHello 解析 + TLS 1.3 key schedule（RFC 8448 向量）+ record-layer AEAD  ✅ 完成（见下「刀7」，ADR-0009）；已合 main
 ├─ 刀8  server-flight 解密 + HMAC 证书校验 + client Finished + 实 TCP 握手 + VLESS 帧 + RealityUpstream(ProxyUpstream) + env 选择器 + 真出口 acceptance
 ├─ 刀9  auto-failover（健康感知 TUIC↔REALITY；分离 TCP/UDP 上游；M3 握手并发化；L2 idle）  ✅ 完成 + 真出口 acceptance ✅（已合 main `831afe3`）
 └─ 刀10 KeyUpdate 密钥轮换（拆出，与 failover 主链零耦合）  ✅ 完成 + 已 ff 合入 main `47b69bd`（loopback acceptance，真出口 KeyUpdate 难诱发如实记录）→ REALITY mini-project 收官
```
- 优先级与关联：**fake-IP 池回收**属"大并发长稳"（并入刀2）；**DoH 拦截**是"真实场景能连上"的前置（刀4，可视情提前——真机浏览器场景不修则 fake-IP 形同虚设）。**A（REALITY）正交**：当前 QUIC 能连，不阻塞三目标达标；TCP-based，替代不了 UDP 直播。

## 后续任务池（post-14b）

1. **刀14c TCP downlink/backpressure + MTU/MSS**：基于 2026-06-30 US-client 结果，先修 reverse P1 2M / P2 reset。
2. **复跑 US-client suite**：14c 后用同一环境和脚本复测，目标是 forward 不回退、reverse P1 不再塌缩、P2 不 reset。
3. **#3 connection-pool spike**：仅在 14c 后仍证明 single TUIC/QUIC connection 是墙时开始；否则不写 pool。
4. **移动端/产品化 core 接缝**：packet I/O trait、library config struct、`cc`/`udp_mode` 等旋钮从 env-only 补到可注入配置。
5. **0-RTT / 弱网恢复**：升级 quinn/rustls 以支持 early exporter，并与 adaptive keepalive / mobile radio-sleep 一起评估。
6. **DNS 边界硬化**：IPv6 DNS、split-horizon/internal domains、multi-question DNS、hardcoded-IP app、`hickory-proto` 迁移时机。
7. **抗封锁韧性增强**：QUIC 被封时是否需要 UDP-over-VLESS/TCP fallback；这是延迟/复杂度权衡，不是默认路线。
8. **Scale / Ops**：多 Upstream/service discovery/weighted health/graceful drain/metrics alerting/multi-region。
9. **更远期产品模式**：Multi-Hop、L3 tunnel mode、REALITY Vision flow / 0x1302/0x1303 指纹恢复、出口 IP reputation。

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

**交付**（分支 `claude/knife6-reality-transport`，从 main 起，逐 commit push；**已 fast-forward 合入 main `b7785a2`**；本片 **sans-IO、100% 离线**，无真握手/无 acceptance）：
- **背景**：正交线 A = 给 Upstream 加第二 Transport（VLESS over REALITY over TCP，抗封锁 fallback）。REALITY 把 auth 藏进 TLS ClientHello `session_id`，stock TLS 库不让写 → **手写 TLS 1.3**（shoes 蓝本），RustCrypto 仅作原语（不破 ADR-0003 单 rustls）。grill 决策：**boring/craftls 均否决**（boring 写不了 session_id 需 patch C；craftls 只给指纹）→ 手写（ADR-0008）。
- **本片做了**（`src/reality/{auth,client_hello}.rs`）：① `auth`：x25519 ECDH(RFC 7748 KAT)、`derive_auth_key`=HKDF-SHA256(salt=random[0..20],info="REALITY",32B)、session_id 16B 布局、`seal/open_session_id`=**AES-256-GCM 完整 32B key**(nonce=random[20..32],AAD=session_id 清零的 ClientHello)、`verify_server_cert`=HMAC-SHA512(同 32B key, ed25519 pubkey)；② `client_hello`：手写 TLS 1.3 ClientHello(Chrome-like:GREASE+X25519 keyshare+ALPN+扩展序;supported_versions 仅 1.3)、`build_authed_client_hello`(建零 session_id→seal→回写 offset 39..71)。
- **质量**：12 reality 单测（含 RFC 7748 KAT + **server-view round-trip**：ECDH→derive→encode→seal→AAD清零→解封全链 + 篡改 ClientHello→解封失败）+ 105 lib 测全绿、clippy 0 warning。`/code-review`(high effort)：零正确性 bug，修了过时 AES-128 文档(实为 AES-256)、命名 session_id 偏移常量、x25519 低阶点安全注记。
- **🔑 查证锁定的互通关键（刀7/8 别再踩）**：REALITY session_id AEAD = **AES-256-GCM + 完整 32B AuthKey（不截断！）**；用 AES-128/截断会让 sing-box 静默拒绝回落 decoy。HKDF salt=random[:20]、info="REALITY"、L=32。AAD = handshake message（含 4B 头），session_id 区 32B 清零。证书校验 HMAC-SHA512 用同一 32B key。
- **设计文档**：`docs/tech/2026-06-22-knife6-reality-transport-{spec,plan}.md`；ADR-0008；CONTEXT.md 加 Transport/VLESS/REALITY。
- **deferred（刀7/8/9）**：ServerHello+key schedule+record（刀7）；server-flight 验证+Finished+实握手+VLESS+RealityUpstream+acceptance（刀8，需服务端 VLESS+REALITY inbound 空 flow）；failover（刀9）。**刀7 起 x25519 用于网络 server keyshare → 必须加 contributory/全零检查**（见 auth.rs 注）。

## 刀7 代码完成（2026-06-23）：REALITY 握手核心 — 手写 TLS 1.3 ServerHello/key schedule/record（第二片）

**交付**（分支 `claude/knife7-reality-handshake`，从 main 起，逐 commit push；**已 fast-forward 合入 main `14258e4`**；**sans-IO、100% 离线，无 acceptance**）：
- **设计输入**：understand-phase research **workflow**（5 路并行研究 + 综合，brief 见 session）→ spec/plan/ADR-0009。
- **本片做了**（`src/reality/{key_schedule,record,server_hello}.rs` + `testutil.rs`）：
  - `key_schedule`：HKDF-Expand-Label/Derive-Secret/Extract/transcript_hash；`derive_handshake_keys`（Early→derived→Handshake(from ECDHE)→{c,s}_hs→key/iv，**全零 ECDHE 拒**）；`compute_finished_verify_data`；`derive_application_keys`（derived2→Master→{c,s}_ap）。
  - `record`：AES-128-GCM record AEAD（per-record nonce、5B 头 AAD、inner type+剥尾零、读/写独立 seq）。
  - `server_hello`：`parse_server_hello`（提 cipher/key_share/version + 拒 HRR/downgrade/compression/version/echo/**长度字段**/**cipher≠0x1301**）。
- **质量**：31 reality 单测（全 **RFC 8448 §3 字节级 KAT**：HkdfLabel、握手+应用密钥链、finished_key、server Finished verify_data、**server-flight record open golden KAT**、ServerHello 解析 + tls-parser 交叉验证）+ 124 lib 测全绿、clippy 0 warning。`/code-review` high effort：cipher/长度 guard + ADR-0009 如实化（修了「泛型骨架」overclaim）；x25519 全零检查等经 verify REFUTED。
- **🔑 刀8 别再踩**：TLS 握手 ECDHE = x25519(client 临时, **server 临时** keyshare from SH)，≠ 刀6 AuthKey 的 (client × server **静态** pbk)；**x25519 全零拒已在 `derive_handshake_keys`**；record cipher = AES-128-GCM（≠ 刀6 session_id seal 的 AES-256）；**cipher≠0x1301 已在 parse 层拒**（0x1302/0x1303 是 ADR-0009 gap）。
- **设计文档**：`docs/tech/2026-06-23-knife7-reality-handshake-{spec,plan}.md`；ADR-0009（cipher 范围 + echo≠auth 不变量）。
- **deferred（刀8）**：实 TCP 握手 + 读写循环 + 跳明文 dummy CCS（不进 read-seq）；用 record/key_schedule 解密真 server flight；X.509 DER 提 ed25519 pubkey+sig → 刀6 `verify_server_cert`（REALITY auth 决策）；CertificateVerify ed25519 检；server-Finished MAC 验 + 发 client Finished；app keys（刀7 已就绪）；VLESS 帧（空 flow）；`RealityUpstream`(ProxyUpstream open_tcp) + env 选择器 + 真出口 acceptance（需 sing-box VLESS+REALITY inbound 空 flow）。可能需 x509 parser crate。

## 刀8 代码完成（2026-06-24）：REALITY 收官 — 实握手 + VLESS + RealityUpstream + **真出口 acceptance ✅**

**交付**（分支 `claude/knife8-reality-live-handshake`，从 main 起，逐 commit push；**已 ff 合入 main `a9172a0`**；**REALITY mini-project（刀6→9）的收官片，VLESS over REALITY over TCP 端到端跑通**）：
- **设计输入**：understand-phase 研究 workflow（5 路并行 + 20 条互通-critical 断言对抗验证）→ brief；grill 6 裁决（见 spec §2）。
- **新增**（`src/reality/{handshake,vless,cert}.rs` + `src/reality_upstream.rs`）：
  - `vless`：`encode_vless_request`（空 flow，**PortThenAddress** + ATYP v4=01/domain=02/v6=03，**不复用 tuic**）+ `VlessResponseStripper`（动态 2+addons_len 首读剥）。
  - `cert`：`extract_ed25519_pubkey_and_sig`——**手解 DER** 扫 ed25519 SPKI marker 取裸 32B 公钥 + 取 leaf DER 末 64B 签名（**不碰 Validity**；见下 acceptance 真因）。
  - `handshake`：`drive<S:AsyncRead+AsyncWrite>`（编排 spec §5 时序）+ `RecordReader`（逐 record，跨 read 缓冲）+ `HandshakeReassembler`（内层 0x16 跨 record 重组）。**H1 cert-seen 守卫**：无通过校验的 Certificate 不许完成握手（防 EE+Finished 的 decoy 绕过 auth）。
  - `reality_upstream`：`parse_pbk`（base64url+std→强断言 32B）+ `RealityClientConfig`（脱敏 Debug）+ `RealityStream`（AsyncRead/Write over TLS1.3 app record + VLESS 响应 strip + post-handshake drop + KeyUpdate loud-fail）+ `RealityUpstream`（`ProxyUpstream::open_tcp` 每 TCP 一次完整握手 + 10s 超时；`DatagramUpstream::send_udp` no-op）。
  - `client_tun`：`MINI_VPN_UPSTREAM=tuic|reality` 选择器（默认 tuic，零回归）+ reality 空 downlink channel（持 tx 永不 send）。
- **质量**：161 lib 测全绿（每个互通-critical 字节 KAT；RFC 8448 §3 握手 drive e2e；**loopback 全 REALITY 握手 e2e**=测试内 REALITY server 模拟器走通真 verify_server_cert + VLESS 往返）+ clippy 0 warning + release 绿。`/code-review`（多 agent 对抗式，16 confirmed findings）已修：H1(auth bypass 守卫)/H2(握手超时)/M1(KeyUpdate loud-fail)/M2(relay shutdown)/M4(截断报错)/L1-L7；deferred → 刀9：M3(握手并发化,H2 超时止血)/L2(relay idle 超时)。
- **🔑 真出口 acceptance ✅（2026-06-24，深圳 client → 47.x sing-box VLESS+REALITY inbound，借用站 gateway.icloud.com）**：curl HTTPS 经 REALITY 隧道 **HTTP 200**（cloudflare trace 见 VPS 出口 IP=三端闭环）+ client 日志 `🔐 REALITY 握手成功（证书 HMAC 校验通过）`（**真 HMAC，非 echo 充数**）；多目标并发握手成功；force-reality 下 UDP no-op 符合预期。
- **🔑 acceptance 抓出的两个互通 bug（离线测全绿但真出口才暴露 —— "宽容方收、严格方拒"，坐实真出口纪律必要）**：
  1. **重复 GREASE 扩展类型**：两个 GREASE 扩展都用 type 0x0a0a（违反 RFC 8446 §4.2）→ Apple/tls-parser 宽容但 sing-box 的 Go-tls 严格解析器**拒整个 ClientHello → REALITY auth 前回落 decoy**。修：尾部 GREASE 改 0x1a1a（真 Chrome 同法）+ 回归守卫 `no_duplicate_extension_types`。
  2. **GeneralizedTime 证书**：真 sing-box 临时证书 Validity 用 GeneralizedTime（notAfter≥2050）→ x509-cert 严格 RFC 5280 拒。修：改回**手解 DER 定点提取**（不碰 Validity，反转 grill 裁决 a、印证 brief 原判；去掉 x509-cert 依赖）+ GeneralizedTime fixture 回归测试。
  - **诊断链**（教学价值）：用真服务端私钥在 Rust 证明客户端密码学 100% 正确（AuthKey 匹配、session_id 可解）→ 排除凭据/AAD/keypair → 锁定"CH 被严格 Go 解析器拒" → dump CH 发现重复 GREASE → 修 → 穿过 decoy → 撞 GeneralizedTime → 手解 DER。
- **设计文档**：`docs/tech/2026-06-23-knife8-reality-live-handshake-{spec,plan}.md` + `2026-06-23-knife8-research-brief.md` + `knife8-singbox-server-setup.md`；ADR-00010（CertVerify defer + KeyUpdate gap + cert 提取反转）；ADR-0009 修订（收紧 cipher offer 0x1301）。acceptance helper `scripts/knife8-reality-acceptance.sh`（preflight/soak/smoke/soak-stop + openssl 0x1301 出口预检）。
- **deferred（刀9）**：auto-failover（健康感知 TUIC↔REALITY）；分离 TCP/UDP 上游；UDP-over-VLESS；连接复用（每 TCP 一次握手）；握手并发化（M3，移出主循环 spawn）；relay idle 超时（L2）；KeyUpdate 密钥轮换（ADR-0010 gap）；0x1302/0x1303（ADR-0009 gap）；Vision flow。
- **⚠️ 安全 note**：acceptance 期间一份服务端凭据（reality private_key/uuid/short_id）曾被误提交进 `docs/tech/knife8-singbox-server-setup.md`（commit 5ded2a2）并推 origin → 已 force-push 重写历史清除（HEAD a928125）+ 文件改占位符；**该 keypair 须在服务端轮换**（私钥上过远端=已暴露）。

## 刀9 完成（2026-06-25）：auto-failover 主链 + M3 + L2 + 真出口 acceptance ✅

**交付**（分支 `claude/knife9-auto-failover`，从 main `a9172a0` 起，逐 commit push；**未合 main**；REALITY mini-project 收尾）：
- **设计输入**：understand-phase research **workflow**（5 路并行研究 + 3 路对抗式核验 + 综合落盘 `docs/tech/2026-06-24-knife9-research-brief.md`）+ grill 4 裁决。spec/plan：`docs/tech/2026-06-24-knife9-auto-failover-{spec,plan}.md`。ADR-0011。
- **F2 分离 TCP/UDP 上游**（`src/failover.rs`，commit `423d79d`）：`FailoverUpstream<T,R>`（泛型，贴合本仓单态化惯用法 + 可注入 mock）impl `ProxyUpstream`（open_tcp 选腿）+ `DatagramUpstream`（**send_udp 恒走 tuic**，F2 硬约束一处钉死）。`MINI_VPN_UPSTREAM=failover`（**opt-in**，默认/未设仍纯 TUIC 零回归；`tuic`/`reality` 作强制单腿旁路）。
- **F4 relay idle 超时（L2）**（commit `9ea70f1`）：`spawn_remote_relay` 抽 `run_relay` + select 加 idle 分支（90s 双向静默 → 退出 + `stream.shutdown`，防慢/卡死上游泄漏）。dev-dep tokio 加 `test-util`（start_paused 确定性测；"full" 不含，已知坑）。
- **F1 不对称 auto-failover**（commit `d64d514`）：`HealthProbe` trait（probe=live_conn 非浅探 / is_dead=close_reason）TuicUpstream impl；`FailoverState` 决策方法收 `now_secs`（可注入时钟确定性单测）；**down 快路（连接死 is_dead）1 次切 / 慢路连续 3 次切 + 成功清零**；**up 连续 3 探针成功 + 60s 冷却切回**（不对称迟滞防 flap）；后台 `spawn_health_probe`（仅 REALITY 当班、30s 节奏）。**铁律**：send_udp 永不读 state（结构性）。
- **F3 M3 握手并发化**（commit `d19c482`）：把昂贵的 REALITY 多-RTT 握手 spawn 出单任务 select 主循环。`ProxyUpstream::open_is_cheap()`（默认 true=inline 零回归；REALITY=false；**FailoverUpstream 恒 false**=失败模式 TUIC reconnect 也不廉价 + 消除 TOCTOU，见下 review）。`SocketState::HandshakePending`（spawn 在飞态，与 inline `OpeningRemote` 区分→reap 不误杀在飞握手）+ `conn_epoch` 防串话（进 +1、rearm +1，`handle_handshake_done` **先比 epoch** 再看状态）+ `uplink_buffer`（256KB 上限，握手期上行缓存、成功后按序 flush）+ `HandshakeDone` channel（cap 128）回灌。fake-IP：spawn 时 acquire、rearm 时 release（平衡）。
- **质量**：176 lib 测 + 6 harness 测全绿、clippy `--all-targets --features harness` 0 warning、release 绿。**对抗式 code-review workflow（41 agent / 7 角度 / 1-vote 核验，commit `546e715`）5 findings 全修**：F1 TOCTOU 深修（FailoverUpstream `open_is_cheap` 改恒 false → 所有 open 含 seamless 重试/黑洞 reconnect 都 spawn 出主循环，纯 TUIC 默认仍 inline）；F5 switch 用 `compare_exchange`（恒 spawn 后 record_tuic_failure 可并发）；F2 try_send 失败 log+rearm 不静默丢；F3 spawn 入口 buffer_uplink 检返回；F4 reap 两次 sockets.get() 合一。
- **🔑 真出口 acceptance ✅（2026-06-25，深圳 client → 47.x VPS，TUIC :8443 + REALITY :443 两腿）**：全链路闭环通过——
  ① TUIC 当班 curl HTTPS 200（出口 IP=VPS）；② pfctl 双向封 TUIC UDP → **主动黑洞探测 ~10s** 日志 `🔀 TUIC 黑洞... → 切到 REALITY`；
  ③ 切后 curl **HTTP 200**（`🔐 REALITY 握手成功` + `▶ leg=REALITY`，DNS 不饿死）；④ 恢复 TUIC → **~62s 切回**（冷却迟滞）；
  ⑤ 切回后 curl 200（`▶ leg=TUIC`）。**🔑 acceptance 抓出 4 个离线测不到的检测坑（idle/open-success 对 QUIC 黑洞不可靠）**，
  全修并坐实「主动 udp_rx 探测才是可靠主机制」（见 ADR-0011 §3b + 下「检测修订」）。helper：`scripts/knife9-failover-acceptance.sh`。
- **第二次对抗式 review（检测修复 diff，23 agent / 含并发-死锁专项，commit `a8bfb9f`）**：**零正确性 bug**（try_lock/CAS/检测状态机扛住），4 条 cleanup/altitude 全修（注释陈旧 idle/rx_datagrams 也改 try_lock 非阻塞/常量澄清/reset 注释）。
- **🔑 检测修订（acceptance 复测 4 轮逼出，commit `8287cb5`→`79ef068`）**：idle/open-success 检测被 ① open 写小 Connect 头黑洞下乐观返 Ok
  （重置慢路计数）② keepalive 架空 idle（close_reason >80s，keepalive 不能删=保活长连接）双重架空。**主修=主动黑洞探测**：
  quinn `stats().udp_rx.datagrams` 当存活信标（健康每 ~5s 有 keepalive ACK→rx 增；黑洞→停滞），`BlackholeDetector` rx 停滞 ≥10s
  → 切 REALITY（~10-13s）。配套：重连 5s 超时 + open_tcp 5s 超时 + idle 30s→15s（备机制）；**`send_udp` 改 `current_conn`
  非阻塞**（try_lock+不重连，黑洞期不 stall 主循环饿死 DNS，重连交后台 start_udp）；`spawn_health_probe`=down(rx 停滞)+up(探针)统一任务。
- **（原 runbook 验证项，已全过）**：`MINI_VPN_UPSTREAM=failover` + 两腿凭据 → 跑 client-tun。验证：
  1. **F1 down**：TUIC 正常 curl HTTPS 200 → 人为打断 TUIC（client 侧 pfctl 封 outbound UDP 到 VPS:8443，或 server 侧停 QUIC）→ 日志 `🔀 failover：TUIC ... → 切到 REALITY` + curl 仍 200（cloudflare trace 见 VPS 出口）；
  2. **F1 up**：恢复 TUIC → 60s+ 后日志 `🔀 切回 TUIC 主腿`；
  3. **UDP**：TUIC 当班 `dig` over QUIC datagram 通；REALITY 当班 UDP 丢（符合预期，UDP 永绑 TUIC）；
  4. **F3 不 stall**：REALITY 当班多并发 curl，一条慢握手不拖垮其余（对比 inline 基线）；
  5. **F4 idle**：relay 静默 90s 自动清理。
  helper：**`scripts/knife9-failover-acceptance.sh`**（`soak`/`cut-tuic`/`restore-tuic`/`smoke`/`udp-check`/`status`/`soak-stop`；
  两腿 env + pfctl 按端口阻断 TUIC UDP 不碰 REALITY TCP）。流程印在 `soak` 末尾。
- **deferred（刀10+）**：**F5 KeyUpdate 密钥轮换**（与 failover 主链零耦合、单独成刀；brief §6 有 V1 字节级核验的精确规范：label `"traffic upd"`/seq 归 0/收 update_requested 必回发且**旧 send key 先封装再换密钥**/`AppKeys` 已暴露 c/s_ap_secret）；UDP-over-VLESS；连接复用；指数退避；0x1302/0x1303。

## 刀11 完成（2026-06-26）：数据面可观测性 — Arc<Metrics> + MetricsSnapshot 契约 + 30s 📊 快照

**交付**（分支 `claude/knife11-observability`，从 main `6ba6d42` 起，逐 commit push；**已 ff 合入 main `9de0604`**；主线量化底座）：
- **设计输入**：grounding workflow（5 接缝并行核实）+ 设计综合（seed §4 五开放问题逐一裁决）→ spec/plan/ADR-0012。
- **新 `src/metrics.rs`**：进程级 `Arc<Metrics>`（原子，唯一桥接 run_event_loop task ↔ TuicUpstream::start_udp task）=
  累计 counter（`inc_*` fetch_add Relaxed）+ 发布式 gauge（`set_*` store；loop 30s tick 从单写者 socket_ctxs/fake_pool 重算后发布）；
  `MetricsSnapshot` 纯值 Copy 契约（前端用，无 serde）；`note_pressure_edge` 纯沿 helper。**不扩 MetricsSink**（计时正交，NoopSink 仍零开销）。
- **指标**：DNS `dns_forged`/`dns_dropped`（`handle_dns_hijack`，纯函数 `forge_dns_reply` 不碰）；UDP 下行 `udp_drops_down`
  （accept-uni 溢出 + read None，**与上行 udp_drops 严格分离**）；`datagram_pressure_events`（背压 false→true 上升沿，task-local latch）；
  `relays_spawned`（`spawn_remote_relay` 唯一入口）；gauge `active_relays`（state==Relaying）/`fake_ip_active`/`fake_ip_total`
  （`FakeIpPool::usage()`）/`failover_leg`（`ProxyUpstream::failover_leg_u8()` 默认方法，非 failover→`NO_FAILOVER`）。
- **发射**：run_event_loop 新 30s `metrics_tick` → `publish_gauges` → `snapshot` → **无门控**打统一 `📊` 行；既有 start_udp
  UDP-path `📊` 行原样保留、各司其职（ADR-0012 §5）。
- **质量**：193 lib + harness 测全绿、`clippy --all-targets --features harness` 0 warning、release 绿。**两轮 review 零正确性 bug**：
  对抗式 review workflow（5 维度 × 逐条对抗式核验，28 agent / default-refute → 23 findings 全 not-a-bug）+ `/code-review` high effort
  （8 角度 → 仅 cleanup 建议，逐条权衡后不动：扩 blast radius / 耦合 feature gate / 纯偏好，稳定优先）。
- **设计文档**：`docs/tech/2026-06-26-knife11-observability-{spec,plan,seed}.md`；ADR-0012；CONTEXT.md「Metrics snapshot」词汇；
  findings 末节「刀11」（含 `📊` 行格式 + acceptance 配方）。
- **真出口 acceptance ✅ PASS（2026-06-26，深圳真机 → 47.x sing-box，TUIC+REALITY 两腿）**：`📊` 行真负载下全部指标非 0 且单调/正确——
  `dns_forged` 147→171→210 / 153→176→198、`relays_spawned`(累计) 95→129、`active_relays` 17~43、`fake_ip 在册` 35→60、
  `failover_leg` 纯TUIC=`-`·cut后=`REALITY`(+4 真 REALITY 握手)、`udp_drops_up`=5(cut 封锁窗口吻合)；`udp_drops_down`/背压=0
  （刀3.5 已证 native+cubic datagram 够用、未触发，如实记录）。两处仅采样时机漏（短突发+`sleep<周期`、切回冷却~90s>sleep70），非失败。
  **测试单**=`docs/tech/2026-06-26-knife11-acceptance-checklist.md`；env 旋钮 `MINI_VPN_METRICS_SECS`（默认 30，acceptance 设 5）。
  详见 findings 末节「刀11」。
- **deferred / 已知边界**：① UDP 下行 drop/背压的 I/O 触发点 harness mock 不覆盖 → 归 acceptance；② NODATA（AAAA）按 `Some=forge`
  计入 `dns_forged`，如需区分留 `dns_nodata`（破纯性，defer）；③ 前端读取通道（IPC/local-control）留前端 session（本刀只导出 snapshot 值）。

## 刀12 完成（2026-06-27）：多核逼近 100M 量化定位 — LoopProfiler + 真出口归因（quantify-only）

**交付**（分支 `claude/knife12-multicore-100m`，从 main `460a349` 起，逐 commit push；**已 ff 合入 main `68b5e56`（2026-06-27）**；**纯量化、零热路径行为改动**）：
- **设计输入**：grill 拍板「量化-only + ADR 定瓶颈」（非「量化+干预」）；understand workflow（5 接缝并行深挖 + 路线可行性综合）。
  spec/plan/acceptance：`docs/tech/2026-06-26-knife12-multicore-quantify-{spec,plan}.md` + `2026-06-26-knife12-acceptance-checklist.md`。
- **`LoopProfiler`**（新 `src/loop_profiler.rs`）：knife1 `MetricsSink` **计时**接缝的生产实现，量主循环 **poll/relay/loop-active**
  三段 wall-fraction（loop-active = 1−park/wall）。env `MINI_VPN_PROFILE_LOOP=1` 开 → 每 `MINI_VPN_METRICS_SECS` 打 `🔬` 行；
  **默认 `NoopSink` 零开销逐字不变**（trait 加 `loop_park_begin/end`/`report` default-空方法，8 arm 首行 park_end + 循环底 park_begin
  + metrics_tick report）。harness 多核就绪 spike 证仪器正确（注入 on-loop CPU → loop-active 0.706→0.996/poll 0.170→0.692）。
- **质量**：205 lib + 8 harness 测全绿、`clippy --all-targets --features harness` 0 warning、release 绿。**对抗式 review workflow
  撞 session 限额失败 → 改 inline 逐维度自评（零开销/数学/插桩语义/harness）零正确性 bug**，仅修一处 doc（`enter_relay` 注释陈旧）。
- **🔑 真出口归因（深圳 macOS client → 47.x sing-box，iperf3）→ #4 实测推翻（ADR-0013）**：
  - **poll 段处处 ≤3.8%、多数 0.1%**（直连 + 隧道、各负载）→ **单核 smoltcp poll 不是 100M 天花板**（brief 承重假设证伪，
    同刀3.5 推翻「5.3M 天花板」）。
  - **当前墙是 WAN 跨太平洋路径**（单流 ~22M / 并行聚合 ~46M、重传一次达 63509、RTT 限）→ **100M 此路物理不可达**，
    客户端在任何可达负载下接近空闲（loop-active 多 0.1%、park 99.9%）。
  - 唯一 on-loop 成本是**建连瞬态的 relay 段**（P=4 setup 窗口 relay=66%/poll=3.8%）→ 若主循环有瓶颈是 relay 调度/inline open，非 poll。
  - **早先绕过隧道**（裸跑 client-tun 不配路由 → 流量走 en0；`curl ipinfo.io` 显示本地 IP 是金标准症状）。
- **🔑 干净隧道实测（2026-06-27，`soak` 路由修好，44.5M 隧道）→ 裁决站得住 + 挖出 HoL bug（ADR-0013「Update」）**：
  稳态 `🔬` = **loop-active≈93% / poll≈8% / relay≈81% / park≈7%**。读代码坐实：上行 `tx.send().await`（有界 1024 channel，
  client_tun.rs:1350）在 QUIC 上游拥塞时**阻塞主循环等上游** → relay=81% 是**背压等待非 CPU**，poll=8% 是真 CPU。
  **主循环 upstream-bound 非 CPU-bound，墙仍是 QUIC 上游/WAN（#3），裁决不翻。** 仪器局限：loop-active 混 CPU 与 arm 内
  `.await` 背压（纯 CPU 需 OS `sample`）。**真 bug**：阻塞上行使一条拥塞慢流 **HoL 阻塞整个事件循环**（大并发混合流下慢流拖死快流）。
- **🔑 OS `sample` 确认（2026-06-27，压测期）→ 裁决锁死 + 挖出 println**：栈采样 top `__psynch_cvwait 29149/kevent 7283`
  （parked/空等）压倒，真 CPU 极小（smoltcp poll 仅 17）→ **进程非 CPU-bound、#4 二次证伪、#3 锁死**。**💎 主循环 #1 on-CPU
  成本=热路径 `println!`**（`process_listener_activity→_print→write` ~183 采样；`📬` 行 client_tun.rs:658 等）远超 poll(17)——
  22000 事件/秒每次阻塞 write、纯浪费 + 加重 HoL。
- **裁决（ADR-0013）→ 刀13**：**取消事件循环分片（route a）**（loop 非 CPU-bound，分片无用）；**#3 连接池**留**低 RTT 胖链路**再测。
  刀13 已按 sample 的 cheap→结构性顺序完成：①热路径 `println!` 由 `MINI_VPN_TRACE` 门控；②上行发送改
  `try_reserve`，Full 时留 smoltcp 字节 + 保持 dirty，端到端 TCP 背压，消除跨流 HoL。`LoopProfiler` 留作复测工具。
- **deferred / 可选**：启动首条 `🔬`（`wall≈0ms` tokio interval 首 tick）退化——**已加 guard 跳过**（commit `33d9418`）。

## Rhythm（每刀都遵守）

1. 新 session → 读本 HANDOFF + `Rules.md` → 先 **grill**（用 `/grill-with-docs` 或 brainstorm，对齐设计与本刀范围）→ 出 **spec + plan**（docs/tech/，TDD 分解）。
2. **TDD per task**：写失败测试 → red → 实现 → green → commit；**每次 commit 后 `git push`**；一个分支一个 writer。
3. 收尾：**`/code-review`** over the diff → 修 → 跨机/压测 **acceptance**。
4. **真实数据测试协作模式**：凡是需要用户在真实环境采集性能/连通性数据，优先在 `./scripts/` 增加可复跑脚本，
   同时给出测试步骤、前置检查、日志路径和判据；用户按指导测试并贴回日志后，再基于日志分析和优化。不要让用户手拼长命令。
5. **cwd 陷阱**：Bash cwd 可能在 call 之间被重置到别的 worktree——每条 git/cargo 命令前 `cd` 到本 worktree 并用绝对路径编辑；`git branch --show-current` 应是本分支。
6. 文档/教学叙述（teaching note、LEARNINGS）由用户另行通过代码+commit 生成；**本路线只产 spec/plan + 代码 + commit + 必要的 TODO 状态**（除非用户另说）。
7. 用**中文**回复（代码/术语/commit 保留英文）。

## 已知坑 / deferred（接力时别重新踩）

- **0-RTT**：quinn 0.10 / rustls 0.21 在 0-RTT 阶段无法 `export_keying_material`，TUIC auth 必失败回落 1-RTT → `MINI_VPN_TUIC_ZERO_RTT` 默认关。真 0-RTT 需 quinn 升级（归移动端 stage），见 TODO 13c。
- **quic-stream UDP fallback 已完成**：刀3 已做 oversized packet uni-stream 兜底；刀3.5 后默认仍是 native/cubic。
- **加密 DNS/fake-IP 绕行主链已关闭**：刀4 阻断已知 DoH/DoT/DoQ/DoH3，刀5 拦全 plaintext :53。
- ~~**fake-IP 池永不回收**（198.18/15）~~——✅ 刀2 已修（引用计数活跃 flow + 60s sweep + 死槽回收）。
- **first-SYN-to-fresh-fake-IP refused 竞态已关闭**：刀4 acceptance 确认 knife2 已修。
- 出口是 VPS datacenter IP → Google/Meta 风控（协议无关，记录即可）。

## Not in git（用户提供；真实/UDP 直播 acceptance 时需要）

- sing-box 互通参数（env）：`MINI_VPN_TUIC_SERVER=<VPS_IP>:8443`、`MINI_VPN_TUIC_UUID=<uuid>`、`MINI_VPN_TUIC_PASSWORD=<pass>`、`MINI_VPN_TUIC_SNI=example.com`、`MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem`、`MINI_VPN_TUIC_ALPN=h3`。（向用户要实际 UUID/password/IP，**勿入库**。）
- 启动：`sudo MINI_VPN_TUIC_* ./target/debug/mini_vpn client-tun`（13d 起 `MINI_VPN_UPSTREAM` 已删，恒 TUIC；`MINI_VPN_TUN_POOL_SIZE` 可调端口池）。
- **刀3.5 新增旋钮**（非凭据，可入库默认；env 覆盖）：`MINI_VPN_TUIC_CC=bbr|cubic`（默认 cubic）、`MINI_VPN_TUIC_UDP_MODE=native|quic`（默认 native）。
- 刀1 若走 mock-upstream 隔离压测，则**不需要** sing-box。
- **刀5 acceptance**：`sudo -E bash scripts/knife35-acceptance.sh soak-knife5`（设系统 DNS=8.8.8.8 非我方 resolver + 路由进 TUN，
  验证任意 :53 仍被劫持）；`soak-stop` 自动还原。`K5_RES` env 可换 alt-resolver。需同上 `MINI_VPN_TUIC_*` 凭据。

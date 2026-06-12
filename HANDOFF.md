# HANDOFF — mini_vpn core 路线（达成 Rules.md 用户使用目标）

给后续 **逐刀接力的新 session**。每刀单独开 session（省 token），按本文件冷启动。

## 当前状态（基线）

- **Stage 13 已在 `main`**（TUIC 数据面，ADR-0004）。**刀1 已完成**，在分支
  **`claude/knife1-concurrency-harness`**（从 main 起，逐 commit push，**尚未合 main**）——见下「刀1 已完成」。
- **Stage 13 全部完成**：数据面已是 **client-only TUIC over quinn → sing-box**（ADR-0004）。
  - 13a TCP via TUIC Connect ✅、13b UDP via TUIC Packet ✅、13c 按需 heartbeat + 保活厘清（0-RTT 撞 quinn 0.10 墙、deferred）✅、13d 退役 legacy（删 yamux/自研 server/双轨开关/6 个依赖）✅。
  - 全部跨机签收（深圳 client → US/HK sing-box）；55 单测、clippy 0 warning、release build 绿。
- 新 session 起点（刀2）：从 `claude/knife1-concurrency-harness` 起新分支（或刀1 合 main 后从 main 起）。
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
 ├─ 刀2  大并发优化（按刀1结果对症）+ fake-IP 池 LRU/TTL 回收  ← 下一刀
 ├─ 刀3  UDP 直播硬化（quic-stream fallback + 吞吐压测 + MSS/MTU）
 └─ 刀4  连接成功率（DoH/DoT 拦截 + 拦全 :53；first-SYN-to-fresh-fake-IP refused）

正交线（抗封锁韧性，不阻塞主线；QUIC 被 GFW 封时才必需）
 └─ A   VLESS+REALITY（TCP）+ 协议 auto-failover（TUIC→REALITY）
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

## 下一刀（刀2）：大并发优化（对症刀1）+ fake-IP 池回收

**起点**：从 `claude/knife1-concurrency-harness`（或合 main 后从 main）起新分支。读 findings 文档。
**主攻**（按刀1 优先级）：
- **#1**：主循环别每 tick 全量 sweep `all_handles()`；改"仅处理有 readiness 的 handle"（事件/脏集合驱动）。
  harness 的三段插桩 + N sweep 可直接量化优化效果（优化前后对比 relay/call）。
- **#2**：per-port pool 弹性扩容/复用 + accept backlog；放开 `MAX_INTERCEPTED_PORTS`/`pool_size` 的硬上限。
- 顺带 **fake-IP 池 LRU/TTL 回收**（长稳）。#4/#5 多半随 #1/#2 一起缓解。
**验证**：harness N sweep 优化前后对比 + 跨机 acceptance；#3 用 findings 的 probe 配方判定是否需 QUIC 连接池。

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
- **fake-IP 池永不回收**（198.18/15）——刀2 长稳。
- **first-SYN-to-fresh-fake-IP `connection refused`**（SYN inspector 建池竞态，curl 不重试 refused）——刀4。
- 出口是 VPS datacenter IP → Google/Meta 风控（协议无关，记录即可）。

## Not in git（用户提供；真实/UDP 直播 acceptance 时需要）

- sing-box 互通参数（env）：`MINI_VPN_TUIC_SERVER=<VPS_IP>:8443`、`MINI_VPN_TUIC_UUID=<uuid>`、`MINI_VPN_TUIC_PASSWORD=<pass>`、`MINI_VPN_TUIC_SNI=example.com`、`MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem`、`MINI_VPN_TUIC_ALPN=h3`。（向用户要实际 UUID/password/IP，**勿入库**。）
- 启动：`sudo MINI_VPN_TUIC_* ./target/debug/mini_vpn client-tun`（13d 起 `MINI_VPN_UPSTREAM` 已删，恒 TUIC；`MINI_VPN_TUN_POOL_SIZE` 可调端口池）。
- 刀1 若走 mock-upstream 隔离压测，则**不需要** sing-box。

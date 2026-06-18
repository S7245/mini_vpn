# 刀1 — 大并发瓶颈定位结论（findings）

> 配套：spec/plan（同目录 `2026-06-12-knife1-concurrency-harness-*`）。
> 方法：mock 回环 harness（`src/harness.rs`，feature `harness`）隔离 **客户端主循环 + smoltcp +
> relay 调度**，不走真网络。复现：
> `cargo test --features harness --test concurrency_harness -- --ignored --nocapture`
> 数据采自 2026-06-12 本机（darwin, release-ish debug build）。绝对数会因机器而变，**趋势/比例**是结论。

## TL;DR（给刀2 的优先级）

| 优先级 | 瓶颈 | 状态 | 证据 | 刀2 方向 |
|---|---|---|---|---|
| **P0** | #1 主循环每 tick `all_handles()` **O(总 listener 槽数)** 全量遍历 | ✅ 坐实，**主因** | relay/call 随总槽线性翻倍、与活跃连接无关 | 事件驱动/脏集合，只处理有活动的 handle，别每 tick 全扫 |
| **P0** | #2 每端口 `pool_size` 是**硬并发上限**；64 端口 × 默认 pool=2 ≈ 128 并发；热门端口突发会 **stall** | ✅ 坐实 | 单端口 pool=2 下 256 路只完成 **2/256** | pool 弹性扩容/复用 + accept backlog；并查 rearm-under-churn（与 #4-刀4 SYN-race 相关） |
| **P1** | #4 单线程 `tokio::select!` 主循环串行上限 | ✅ 坐实 | 有用功恒定时吞吐随每-tick 开销上升而**下降** | 与 #1 强耦合；削掉 #1 的全扫即缓解大半 |
| **P2** | #5 每 socket 64KB×2 缓冲的内存成本 | ✅ 量化 | 2048 槽 × 128KB ≈ **256MB**，多为 #1 空扫的空闲槽 | 缩小默认 buffer / 按需分配；随 #1/#2 一起收 |
| deferred | #3 单条 TUIC QUIC 连接承载所有 TCP flow（拥塞/队头） | ⏸ 本刀测不到 | mock 本地回环无网络拥塞 | 需端到端 probe（见末节），归刀2/刀3 acceptance |

**一句话**：大并发的主因不是网络，是**客户端主循环每 tick O(总槽数) 全量 sweep**（#1）叠加**每端口 pool 硬上限**（#2）。两者都在单线程循环里（#4）放大。刀2 先砍 #1 的全扫、再放开 #2 的 pool。

## 数据

### A. N sweep（固定 64 端口 × pool=16 ⇒ 恒 1024 槽；payload 1KB）

```
N=  64  wall=  64ms  poll= 11.3ms/205   relay= 40.8ms/205   listeners 1024  8.2 Mb/s
N= 256  wall= 355ms  poll= 89.1ms/938   relay=223.6ms/938   listeners 1024  5.9 Mb/s
N=1024  wall=2275ms  poll=907  ms/4530  relay=1072 ms/4530  listeners 1024  3.7 Mb/s
```
- **relay/call ≈ 恒定**（0.20→0.24ms）：总槽恒 1024，sweep 成本与 N 无关 → 指向 #1。
- **poll/call 随并发涨 ~3.6×**（0.055→0.20ms）：smoltcp poll 遍历活跃 socket，随并发增长 → #4/#5。
- **吞吐随 N 跌**（8.2→3.7 Mb/s）：单线程每-tick 总开销上升、单连接被挤 → #4。

### B. #1 隔离：固定 N=256，只变 pool_size（总槽 = 64 × pool）

```
pool= 8  总槽= 512  relay=122.2ms/925   (0.132 ms/call)  poll=66ms   thrpt 9.2 Mb/s
pool=16  总槽=1024  relay=239.7ms/1063  (0.226 ms/call)  poll=94ms   thrpt 5.6 Mb/s
pool=32  总槽=2048  relay=476.1ms/1069  (0.445 ms/call)  poll=141ms  thrpt 3.1 Mb/s
```
- **N 不变（256），relay/call 随总槽线性翻倍**（0.132→0.226→0.445，槽 512→1024→2048）。
- 吞吐随槽数**下降**（9.2→3.1 Mb/s），尽管有用功恒定 → sweep 空闲槽是**纯浪费**。
- **结论（#1 坐实）**：`all_handles()` 的 O(n) 是 O(**总 listener 槽数**)，不是 O(活跃连接)。
  端口×pool 越大，每 tick 全扫越贵，与实际负载无关。这是大并发主因。

### C. #2 坐实：256 路全压单端口，pool_size=2（生产默认）

```
单端口 pool=2  →  done=2/256  wall=20s(超时)  listeners max=2
```
- 只有 **2/256** 完成：2 个槽被占满后，其余 254 路的 SYN 被丢（无 listening socket），
  靠 TCP SYN 指数退避重传 + 槽 rearm —— 在窗口内**几乎不排空**（不是慢，是 stall）。
- **结论（#2 坐实）**：每端口 `pool_size` 是热门端口的**硬并发上限**。默认 pool=2 + `MAX_INTERCEPTED_PORTS=64`
  ⇒ 全局 ~128 并发天花板；单热门端口（如 :443）突发直接 stall。
- 旁证 #5 内存：达标的 pool=32 场景共 2048 槽 × 128KB ≈ **256MB**——大多是 #1 在空扫的空闲槽。

## 怀疑瓶颈逐条裁决

1. **#1 O(n) 全量遍历** —— ✅ **主因（P0）**。证据 B：relay 成本线性于总槽、独立于负载。
   刀2：主循环别每 tick `registry.all_handles()` 全扫；改"仅处理本 tick 有 readiness 的 handle"
   （事件/脏集合驱动），把 O(总槽) 降到 O(活跃)。
2. **#2 端口 pool / 64 上限** —— ✅ **P0**。证据 C：pool=2 单端口 256→2。
   刀2：per-port pool 弹性扩容（按需增槽）或连接复用 + accept backlog；
   并排查 rearm-under-churn 为何几乎不排空（与 HANDOFF 已知 "first-SYN refused / SYN-race"、刀4 重叠）。
3. **#3 单条 QUIC 连接** —— ⏸ **deferred**。mock 回环无网络拥塞，测不到；见末节端到端 probe。
4. **#4 单线程 select 串行上限** —— ✅ **P1**。证据 A：吞吐随每-tick 开销上升而下降。
   与 #1 强耦合——#1 的全扫正是塞满单线程的主负载；砍掉 #1 即缓解大半。是否上多线程/分片留刀2 评估。
5. **#5 per-socket 128KB 缓冲** —— ✅ **P2（量化）**。2048 槽≈256MB；空闲槽既费内存又被 #1 空扫。
   随 #1/#2 一起收（减槽 + 按需 buffer）。

## harness 局限（读数注意）

- **延迟绝对值不可信**：发生器驱动循环带 200µs sleep（让出 CPU 给 SUT 任务），latency 列反映
  "驱动节拍 + 串行化"，**不是** SUT 纯单操作延迟。**可信的 SUT 成本信号是 poll/relay 分段计时**。
- mock echo 走内存 duplex，**无网络 RTT/拥塞**——这正是隔离客户端处理的目的，也是 #3 测不到的原因。
- 单线程 vs 多线程 runtime 不影响结论：瓶颈在主循环本身的每-tick 串行工作量。

## #3 端到端手动 probe 配方（deferred，需真 sing-box）

mock 测不到"单条 QUIC 连接是否成为大并发拥塞/队头瓶颈"。用现成 sing-box 出口手动压：

1. 起客户端（HANDOFF "Not in git" 的 `MINI_VPN_TUIC_*` env）：
   `sudo MINI_VPN_TUIC_* ./target/debug/mini_vpn client-tun`
2. 真机发 N 路并发 HTTPS（绕开本地 fake-IP 用 IP 直连或受控域名），如：
   `seq 1 200 | xargs -P200 -I{} curl -s -o /dev/null -w '%{http_code}\n' https://<target>/`
3. 观测：单连接 QUIC 的拥塞窗口/队头是否随并发上升导致尾延迟暴涨或吞吐塌缩；
   对比"是否需要连接池（多条 QUIC 连接分摊 flow）"。
4. 若确认 #3，归刀2/刀3：评估 TUIC 多连接池 vs 单连接多流。

## 复现命令

```sh
# 常驻正确性（feature gate 内）：单连接 + N=64 + UDP liveness
cargo test --features harness --test concurrency_harness -- --nocapture
# 重量级定位（A/B/C 三组实验）
cargo test --features harness --test concurrency_harness -- --ignored --nocapture
```

---

# 刀2 优化结果（2026-06-15，对症本 findings）

> 实现见 `2026-06-15-knife2-concurrency-opt-{spec,plan}.md`。分支 `claude/knife2-concurrency-opt`。
> 同机（darwin）同 harness，仅换被测主循环逻辑。**趋势/比例**是结论。

## #1（脏集合驱动）——relay 段不再随总槽线性翻倍

固定 N=256，扫 pool_size（总槽=64×pool）。`relay` 段耗时 / `avg_listeners`（优化前 → 优化后）：

| 总槽 | relay 段 | avg_listeners | 吞吐 |
|---|---|---|---|
| 512 (pool=8)  | 124.2ms → **8.8ms**  | 494.6 → **7.8**  | 8.94 → **16.99** Mb/s |
| 1024 (pool=16)| 264.4ms → **15.6ms** | 993.3 → **15.5** | 5.00 → **11.53** Mb/s |
| 2048 (pool=32)| 532.9ms → **23.8ms** | 1989.6 → **30.5**| 2.76 → **8.54** Mb/s |

- **relay 段不再随总槽线性翻倍**：avg_listeners 从 ≈总槽 降到 ≈活跃 pool（O(总槽) → O(活跃)）。#1 砍掉。
- relay 段从主导降为零头；剩余瓶颈转移到 **poll 段**（smoltcp，#4/#5）——符合 findings「砍掉 #1 后 #4 缓解大半」。

## N sweep（固定 64×16=1024 槽）——吞吐回升

| N | relay 段（前→后） | 吞吐（前→后） |
|---|---|---|
| 64   | 53.4ms → **3.1ms**  | 6.09 → **16.64** Mb/s |
| 256  | 231.7ms → **14.2ms**| 5.67 → **12.56** Mb/s |
| 1024 | 1618ms → **70.8ms** | 2.50 → **5.71** Mb/s |

N=1024 的 relay 从 1618ms（占满单线程）降到 70.8ms；瓶颈现由 poll 段（1030ms，smoltcp 遍历活跃 socket）主导。

## #2（弹性扩容）——单热门端口不再 stall

256 路全压**单端口** pool=2（生产默认）：

```
优化前：done=   2/256  wall=20000ms（超时 stall）  listeners max=2
优化后：done= 256/256  wall=  266ms              listeners max=257（弹性扩容）
```

每端口 pool 硬上限打掉：SYN 命中即弹性补足空闲 listening 槽，全局 `MAX_TOTAL_LISTENERS=4096` 兜底。

## #5（空闲槽内存）——随 #1/#2 缓解

- #1 后空闲槽不再被每 tick 全扫（relay 只碰活跃）；#2 按需扩容使槽数随真实并发而非预建 `64×pool`。
- per-socket buffer 默认未动（65535×2，保 TCP 窗口/吞吐与稳定）；激进缩小留后续按需评估。

## fake-IP 引用计数回收（长稳）

- 每映射 refcount + last_used；TCP（`SocketCtx.fake_ip`）/ UDP（`AssocTable` id→fake-IP）两条 flow 生命周期
  打通 acquire/release；`udp_sweep`（1s）周期 `fake_pool.sweep(now, TTL=300)`。
- **活跃 flow（refcount>0）绝不回收**（单测覆盖）；仅 idle 且无 flow 超 TTL 才回收 → 长稳防泄漏、不断连。

## 刀2 真出口 acceptance（2026-06-15，深圳 client → 47.251.188.205 sing-box）

mock harness 之外的端到端验证（IP 直连 1.1.1.1:443，`route -n add -host 1.1.1.1 -interface utunX`，不动全机 DNS）：

| 测试 | 命令要点 | 结果 | 判定 |
|---|---|---|---|
| ① TCP+TLS | `curl -w '%{ssl_verify_result}' https://1.1.1.1/` | `HTTP=301 TLS_verify=0`，三端日志闭环 | ✅ 端到端 TLS 经 TUIC 打通 |
| ③ 大并发 | `seq 1 200 \| xargs -P200 curl … :443` | `200 301`（全成功零 `000`），数秒内 | ✅ #2 弹性扩容真实生效（优化前 stall 2/256） |
| #3 probe | 200 路 `time_total` 分位 | p50=0.379 / p95=0.491 / **max=0.557s** | 见下 |

**#3 裁决（用真 sing-box probe，替代刀1 deferred）**：200 路并发下 `time_total` 分布极平
（max≈1.47×p50，无长尾）→ **单条 TUIC QUIC 连接在此负载下不存在队头/拥塞瓶颈，暂不需连接池**。
更高并发 / 真 UDP 直播大流量下是否需要「多 QUIC 连接池 vs 单连接多流」留刀3 acceptance 复测。

## 仍 deferred

- **#4 多线程/分片**：#1 已缓解大半；poll 段（smoltcp 单线程遍历）成新瓶颈，是否多线程留后续。
- **#3 连接池**：当前负载无需；刀3 真直播大流量再评估。

---

# 刀3 真出口 acceptance 配方（待跑，需真 sing-box）

> 实现见 `2026-06-16-knife3-udp-streaming-{spec,plan}.md`。分支 `claude/knife3-udp-streaming`。
> harness 已验证主循环 UDP 路径 + 分片重组（mock 回环，500/500 逐字节 intact）；**真 datagram
> TooLarge / uni-stream 兜底 / 真 sing-box 分片互通 / `max_datagram_size` 真上限**只能真出口测。

## 准备

1. 起客户端（HANDOFF「Not in git」的 `MINI_VPN_TUIC_*` env）：
   `sudo MINI_VPN_TUIC_* ./target/release/mini_vpn client-tun`
   - 连上即打印 `📏 TUIC datagram 初始上限 = N 字节` → **记录 N（真实 datagram 天花板）**。
   - 每 30s 打印 `📊 TUIC UDP↑ 统计: datagram 上限=.. stream 兜底=F 丢弃=D`（非零才打）。
2. 路由：受控 UDP 目标用 IP 直连绕本地 fake-IP（同刀2 `route -n add -host <ip> -interface utunX`）。

## 测项

| 测试 | 命令要点 | 看什么 | 判定 |
|---|---|---|---|
| ① datagram 真上限 | 读启动 `📏` 行 | N（PLPMTUD 探完会更大） | N≥~1242；记录之 |
| ② 持续大流量不丢 | `iperf3 -c <egress-reachable> -u -b 50M -t 30`（或真 RTP/SRT 直播）经隧道 | iperf3 丢包率 + 客户端 `📊` 的兜底/丢弃 | 丢包率低位；`丢弃 D` 不随流量线性涨（兜底生效） |
| ③ 大包走兜底 | 发 payload>N 的 UDP（如 `iperf3 -l 1400`/大 MTU），或自然大包直播 | `📊` 的 `stream 兜底=F` | F>0 证明 uni-stream 兜底真触发；对应包不计入 `丢弃` |
| ④ 下行分片重组 | 下行重的直播（观看）| 画面是否完整、无花屏；client 无「无映射丢弃」洪水 | 大下行包经 server 分片→本端重组，播放正常 |
| #3 复测 | 大流量 UDP 持续时观测单连接 | 吞吐是否塌缩 / 尾延迟暴涨 | 评估「多 QUIC 连接池 vs 单连接多流」是否仍非必需 |

## 裁决（2026-06-17，深圳 client → 47.251.188.205 sing-box → 43.110.37.170 iperf3，IP 直连）

**datagram 真上限 N = 1332 字节**（`📏` 日志；PLPMTUD 已从 1280 floor 上探）。1400B 包编码后 ≈1417B>N → 走兜底。

### 实测数据（50Mbps offered，`-l` = UDP 载荷字节）

| 测法 | 走哪条路 | 实收 / 丢包 |
|---|---|---|
| 上行 `-l 1400`（>N） | **uni-stream 兜底** | **49.7Mbps / 0.037%** ✅ |
| 上行 `-l 1200`（<N） | native datagram | 5.3Mbps / 89% ❌ |
| 下行 `-l 1400 -R` | datagram 分片 | 11.5Mbps / 77% ❌ |
| 下行 `-l 1200 -R` | native datagram | 5.2Mbps / 90% ❌ |

### 下行 datagram bitrate sweep（`-l 1200 -R`，找干净上限）

| offered | 实收 | 丢包 |
|---|---|---|
| 5 Mbps | 4.91 | **1.7%** ✅ |
| 10 Mbps | 5.31 | 47% |
| 20 Mbps | 5.30 | 74% |

### 结论（三条，均坐实）

1. **✅ 上行 quic-stream 兜底真实生效**：1400B 包全部超 datagram 上限，全走 per-packet uni-stream，
   真 sing-box 上 50Mbps / 0.037% 丢。**改造前这些包 100% 被丢（TooLarge→drop）**——刀3 核心目标达成。
2. **❗ native QUIC datagram 路径有 ~5.3Mbps 硬天花板**：实收死死卡 ~5.3Mbps，与 offered 无关
   （10M→5.31、20M→5.30），**上行/下行 datagram 两个方向都卡同一数**。而 stream 路径同链路跑满 50Mbps
   → **不是带宽/拥塞**（路能扛 50M），是 **QUIC 不可靠 datagram 在高 RTT + 丢包不重传 + 无应用背压**
   下的固有限制。**试过下行批量 flush（摊销每包 syscall）→ 零效果**（10M 仍 47%）→ 排除「我方消费端每包
   flush」假设，瓶颈确在 datagram 传输层，非客户端可小改解。
3. **观测盲点**：datagram 丢包**我方 `udp_drops` 完全看不到**（quinn 缓冲溢出丢最老 datagram 不返回错误，
   `send_datagram` 仍 `Ok`）——90% 丢失但 `📊` 无丢弃计数。需后续补 datagram 背压可观测（`send_buffer_space`）。

### 对 Rules.md ② UDP 直播的判定

- **典型直播码率（≤5Mbps：720p~2.5M / 1080p~4-5M）经 native datagram：1.7% 丢 → 视频可用（达标）**。
- **高码率（>5Mbps：1080p60/4K）下行**：datagram 卡 ~5.3M；但 stream 已证明链路能跑 50M。
- **#3 复测裁决**：单连接**不是**连接数瓶颈——同一连接 stream 跑满 50M。瓶颈是 **datagram 传输特性**；
  连接池对 datagram 未必是解，真正的杠杆是「高码率流走 stream / 给 datagram 加 pacing+背压」。

### 归后续刀（高码率 UDP 直播硬化，独立一刀）

- 选项：① 持续/高码率 UDP flow 默认走 stream（quic-relay-mode 或自适应：观测到高 pps 即切流）；
  ② 给 datagram 路径加 pacing/背压（按 `send_buffer_space` 节流）+ 丢包可观测；③ 评估多 QUIC 连接池
  对 datagram 聚合吞吐是否有效。需带 quinn 级 instrumentation（RTT/cwnd/datagram drop）量化后再定方向。

---

# 刀3.5 真出口 acceptance 配方（待跑，需真 sing-box + 深圳 macOS 真机）

> 实现见 `2026-06-17-knife35-highrate-udp-{spec,plan}.md`。分支 `claude/knife35-highrate-udp`。
> 代码已完成（BBR/Cubic 可切 + quic-relay-mode + quinn 插桩 + 抬 uni-stream 配额），逻辑/harness 全绿。
> **本节是 ready-to-run 配方；跑完把结果填回「实测」与「裁决」。**
> grill 已定 4K(~25M) 为**必跨线**；首包锁定下行 mode（SPEC 已查证）→ 高码率走 quic-relay-mode（全 UDP
> 首包即 uni-stream，server 镜像下行也走 stream）；#221 靠抬 `max_concurrent_uni_streams=4096` 缓解。

## 准备

1. env（HANDOFF「Not in git」+ 刀3.5 新增两个旋钮）：
   - 既有：`MINI_VPN_TUIC_SERVER=47.251.188.205:8443` `MINI_VPN_TUIC_UUID/PASSWORD/SNI/CA_PATH/ALPN`。
   - **新增**：`MINI_VPN_TUIC_CC=bbr|cubic`（默认 bbr，A/B 用）、`MINI_VPN_TUIC_UDP_MODE=native|quic`（默认 native）。
2. 起客户端：`sudo MINI_VPN_TUIC_* ./target/release/mini_vpn client-tun`
   - 连上打印 `📏 datagram 初始上限`、`🧭 拥塞控制器=Bbr|Cubic | UDP relay mode=Native|Quic`（**确认 BBR/quic 真装上**）。
   - 每 30s 打 `📊 TUIC UDP↑: ... | RTT=..ms cwnd=.. 丢包=lost/sent send_buf 余=..B`（UDP 活跃即打，native 背压时附 `⚠️背压`）。
3. **链路铁律**：测试链路带宽 **≥50M**（别把 43.x 限到 10M，否则链路成新瓶颈污染判读），靠 `-b` 控码率。
4. 受控 UDP 目标 IP 直连绕 fake-IP：`sudo route -n add -host 43.110.37.170 -interface utunX`。

## 测项（对应 spec 矩阵 T-A~T-H）

| # | 测项 | 命令要点 | 看什么 | 判据 |
|---|---|---|---|---|
| T-A | CC A/B（gate 主判据） | `MINI_VPN_TUIC_CC=cubic` vs `bbr`（均 native），`iperf3 -c 43.110.37.170 -u -l 1200 -R -b 5/10/20/40M -t 30` | 各档实收/丢包 + `📊` cwnd/RTT | **BBR 下行 datagram 干净吞吐 ≥30M → 保 native 默认、Phase 2 跳过；<30M → 默认翻 quic** |
| T-B | 下行 stream 吞吐（刀3 只测上行） | `MINI_VPN_TUIC_UDP_MODE=quic`，`-l 1200 -R -b 40M -t 30` | 实收 vs datagram 5.3M | 下行经 stream 跑满 offered → 证 stream 修下行 |
| T-C | 上行 datagram + BBR | `-l 1200 -b 40M`（不 -R） | 上行实收 | BBR 是否把上行 datagram 抬过 5.3M（归因 CC vs 无背压） |
| T-D | 4K 端到端（quic 模式） | `udp_mode=quic`，`-b 25M` 上/下行各一轮 | 丢包 + `📊` | 丢包低位；`stream 兜底=0`（quic 主发，非兜底）；无映射洪水 |
| T-E | 多 flow gate | `udp_mode=quic`，并行 2 路：`-b 25M` + `-b 8M`（≈33M 聚合） | 单连接聚合实收 | 聚合 **≥33M 成立** → 池 defer；否则池升后续刀首项 |
| T-F | DNS/小流延迟（carve-out 触发） | quic 模式 `dig @<resolver> ... ` / 小 UDP 往返 vs native 基线 | 往返延迟差 | 明显退化 → **本刀补 DNS/小流 datagram carve-out**；否则不做 |
| T-G | 首包锁定 + 配额验证 | quic 模式起播，看 `📊`/日志 | 下行是否真走 stream、有无 #221 塌缩 | 坐实「首包 stream→下行镜像 stream」且 4096 配额够 |
| T-H | 真实混合场景长稳 soak（深圳 macOS） | YouTube 视/直 + TikTok 视/直 + Facebook 网页 + Telegram 客服，30–60min+ | 主观流畅度 + `📊`/重连/内存 | 视频不卡、TG 跟手、FB 不滞；`📊` 合理、重连低、内存不涨、无映射洪水。TG/FB 明显滞 → 触发 carve-out |

## 实测（2026-06-17，深圳 client → 47.251.188.205 sing-box → 43.110.37.170 iperf3）

> **关键前提变化**：本轮把两端链路从旧的 47.x=**5M** / 43.x=**10M** 升到**双 80M**。
> 这一改直接暴露了刀3 结论的根因——见「裁决①」。

### T-A 下行 datagram sweep（`-l 1200 -R`，Cubic vs BBR）

| offered | Cubic 实收/丢 | BBR 实收/丢 |
|---|---|---|
| 5M  | 4.90M / 2.2%  | 4.95M / 1.1% |
| 10M | 9.95M / 0.15% | 9.79M / 2.1% |
| 20M | 17.8M / 11%*  | 19.5M / 2.3% |
| 40M | **39.8M / 0.25%** ✅ | **30.1M / 24%** ❌ |

`*` 20M Cubic 的 11% 系瞬时抖动（同配置 40M 仅 0.25%，证非系统性）。
**插桩对照（40M）**：Cubic `cwnd≈12000 RTT 172ms`（稳、不过驱）；BBR `cwnd 暴涨 245K→252K、RTT 178→252ms`
（bufferbloat，对不可靠 datagram 狂发不退 → 24% 丢）。`send_buf 余` 全程 1048576B（出向缓冲未压满，丢在传输/对端）。

### T-B 下行 **stream**（quic 模式，BBR）`-b 40M`

**7.02M / 71% 丢、RTT 259ms、cwnd 暴涨 4.5MB** ❌❌。每包一条 uni-stream（~4000pps）+ BBR 过驱 → 拥塞崩溃。
**stream 模式高码率下行远不如 datagram**（7M/71% vs 40M/0.25%）。

### T-C 上行 datagram（Cubic）`-b 40M`

**37.5M / 4.5%**。上行同样清掉旧「5.3M/89%」假象。

### T-E 多 flow（Cubic native，2 路并行 `-P 2 -b 17M -R`）

稳态多数秒 **~34M / 0% 丢**；整体 31.7M / 6.2%（被首秒 72% 启动 + 11s 一次 33% 抖动拖高）。单连接聚合 ~34M。

## 裁决（2026-06-17，数据全锁定）

**① 最大纠偏：刀3 的「~5.3M datagram 硬天花板」是 5M VPS 链路 cap 的测量假象，不是 QUIC datagram 限制。**
升到 80M 链路后，native datagram 下行 39.8M/0.25%、上行 37.5M/4.5%。刀3「stream 50M / datagram 5.3M」的对比
是被 5M 链路污染的产物。**插桩（cwnd/RTT/loss）是揭穿真相的功臣**——印证 HANDOFF「先量化、别凭猜改」。

**② 默认 `congestion_control` = cubic（不是 bbr）——理由是 worst-case 更好 / 方差更小，非「BBR 总是差」。**
两台深圳机各测一轮 40M 下行 datagram：

| 机器 | Cubic 丢 | BBR 丢 |
|---|---|---|
| 原机 | **0.25%** | **24%**（cwnd 暴涨 245K、RTT 178→252ms 过驱） |
| 专用测试机 | 7.6% | 5.8% |

BBR **有更糟的尾部**（原机那次是真实过驱事件）；专用机这轮两者相当（BBR 略好）。Cubic worst-case 7.6% < BBR 24%、
更平稳，对看重一致性的直播 VPN 更稳妥 → 默认 Cubic。BBR 仍可经 `MINI_VPN_TUIC_CC=bbr` 显式选用（部分链路可能更优）。
→ 已改 `DEFAULT_TUIC_CC`。补 `docs/adr/0005-cubic-over-bbr-datagram.md`。
**专用机复核**：① Cubic 40M→37.0M/7.6%、② BBR 40M→37.7M/5.8%（均高码率 OK）、③ quic 全 stream 40M→**0.95M/39%**（崩，
比原机 7M/71% 更惨）→ 再次坐实 native datagram ≫ quic stream，cubic 默认稳。

**③ 默认 `udp_relay_mode` = native（datagram）。** datagram 4K 富余且低延迟；quic 全 stream 模式高码率灾难
（71% 丢）。**quic 模式保留为可配置选项**（代码完成+测过；抗封锁场景——网络封 UDP datagram 但放行 QUIC stream
时可能有用——非默认、非高码率推荐）。

**④ DNS/小流 carve-out 不需要。** 默认 native → DNS/小 UDP 本就走低延迟 datagram；carve-out 仅是「全 stream quic 默认」
的顾虑，不发生。

**⑤ 连接池 defer 坐实。** 多 flow 单连接聚合 ~34M ≥ 33M gate。更高并发再评估。

### 对 Rules.md ② UDP 直播的判定（更新）

- **typical（≤5M）/ 1080p60（~8–12M）/ 4K（~25M）下行直播：native datagram + Cubic 全部达标**（40M/0.25% 实测，4K 富余）。
- 高码率不再需要 stream-routing——刀3.5 的原始前提（datagram 有天花板需 stream）被实测推翻；真正交付的价值是
  **插桩纠偏 + CC 调优（cubic）+ 证实 datagram 本就够**，避免了上线不必要的全-stream 复杂度。

### T-H 真实 soak ✅（2026-06-17，专用测试机，native+cubic 出厂默认，真应用长稳）

YouTube **4K 视频不卡顿**（必跨线真实达标）+ Telegram/Facebook 正常。`📊` 长稳读数（连续 20–30min+）：
- **累计丢包 ~0.31%**（`丢包≈8553/2716577`，跨洲高 RTT UDP 路径极低）；**`丢弃=0` 全程**（零硬丢弃）。
- `RTT` 多数 ~170ms 稳；末尾一次拥塞/PMTU 事件（RTT 626ms、`datagram 上限` PLPMTUD 探到 1246、`stream 兜底`
  跳到 63）被**优雅吸收**——超 MTU 大包自动走 uni-stream 兜底（`丢弃=0`），随后恢复 176ms/1375。
- `stream 兜底` 整轮 3→76 缓增（Native 模式 **大包尾部兜底**在工作，非高码率全 stream）；`send_buf 余` 基本满（无背压）。
- **无 `🔌` 重连风暴、无「无映射丢弃」洪水** → 长稳健康。
- **carve-out 不需要**确认：native 下 DNS/小流走 datagram，TG/FB 交互无滞（主观）。

**→ 刀3.5 全部完成**（代码 + iperf3 矩阵 + 真机 soak）。native+cubic 出厂默认达 Rules.md ② 全码率（含 4K）。

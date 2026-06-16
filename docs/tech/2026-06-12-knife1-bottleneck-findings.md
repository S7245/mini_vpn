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

## 裁决（待填）

- datagram 真上限 N = ___；stream 兜底触发频率 = ___；#3 单连接在真直播大流量下 = 够 / 需池。

# 刀2 — 大并发优化（spec）

> Core 路线第二刀。对症刀1 findings（`2026-06-12-knife1-bottleneck-findings.md`）：
> **P0 #1**（主循环每 tick `all_handles()` O(总槽) 全量 sweep）、**P0 #2**（每端口 `pool_size` 硬并发上限），
> 顺带 **fake-IP 池回收**（长稳）与 **P2 #5**（空闲槽内存）。纪律：先 grill 对齐 → spec → TDD → harness 量化前后。
>
> 北极星：`Rules.md` ③「大并发连接」。本刀把 #1/#2 砍掉，让 N 路并发不再被 O(总槽) sweep + pool 硬上限拖死。

## 目标

1. **#1 事件/脏集合驱动主循环**：relay 调度段从遍历 `registry.all_handles()`（O(端口×pool) 总槽）
   改为只遍历**本 tick 真正有活动的 handle**（脏集合）。把 relay 段成本从 O(总槽) 降到 O(活跃)。
2. **#2 per-port pool 弹性扩容**：listen 槽**按需增长**（保证命中端口恒有空闲 listening 槽吸收突发），
   放开 `pool_size` 固定上限；配**全局总槽上限**兜底防 SYN flood。热门端口（:443）突发不再 stall。
3. **fake-IP 引用计数回收**：每个 fake-IP 映射跟踪**活跃 flow 引用计数 + last_used**，
   仅 `refcount==0 且 idle>TTL` 才回收 → **绝不回收仍有活跃 TCP/UDP flow 的映射**（不断连）。
4. **#5 顺带**：空闲槽内存主要靠 #2 的按需扩容（不再预建一大堆空闲槽）自然缓解；不激进改默认 buffer（保吞吐/稳定）。

## 非目标（本刀不做）

- **#4 单线程主循环多线程化/分片**：findings 说削掉 #1 全扫即缓解大半；留后续评估。本刀维持单 `tokio::select!`。
- **#3 单条 QUIC 连接（拥塞/队头）连接池**：mock 测不到，需真 sing-box probe，归刀2/刀3 acceptance。
- quic-stream UDP fallback / MSS-MTU（刀3）；DoH 拦截 / first-SYN-refused（刀4）。
- fake-IP 不引入 LRU（用户拍板用**引用计数活跃 flow**，比纯 TTL/LRU 更安全）；容量耗尽兜底用 sweep 触发。

## grill 对齐结果（4 项主决策）

1. **#1 = 脏 handle 集合**（dst_port 驱动 + pending 标脏），不用 smoltcp `register_recv_waker`
   —— 更简单、易测、无 waker 并发状态/竞态，契合「稳定 > 漂亮」。relay O(活跃端口×pool + 待 flush)。
2. **#2 上限 = 全局总槽上限**（per-port 按需扩容，但全局 listener socket 总数封顶 `MAX_TOTAL_LISTENERS`）。
   单旋钮、内存可控。
3. **fake-IP = 引用计数活跃 flow**：`refcount==0 且 idle>TTL` 才回收。resolve/alloc 持续 touch；
   TCP flow（`SocketCtx`）与 UDP flow（`AssocTable`）两条生命周期都打通 acquire/release。
4. **范围 = #1/#2/fake-IP 为主，#5 顺带**；#4/#3 deferred。

## 设计

### A. #1 脏集合驱动（client_tun.rs `run_event_loop`）

主循环内新增主循环独占的 `dirty: HashSet<SocketHandle>`（无锁，单线程）。

**入集（标脏）**：
- **新首包**：rx 热路径中，走 smoltcp 的入站 TCP 包 → 解析其 TCP `dst_port` → 把该端口 pool 的所有 handle 入集。
  （任何去往拦截端口的 TCP 包都标脏其端口；覆盖 SYN 后的首个 data 包让 listener `can_recv` 的时刻。）
- **下行 pending**：`handle_remote_payload` 中 flush 后仍有 `downlink_pending` 残留 → 该 handle 入集。

**出集**：relay 段处理一个 handle 后，若该 handle **无 `downlink_pending` 且不再 `can_recv`** → 出集；
仍有 pending（tx buffer 满）则保留，下个 tick 继续 flush。

**两个分支统一**：rx 分支与 timer 分支的 relay 段都改为 `for handle in &dirty { process_listener_activity }`（快照后处理，避免借用冲突）。
`iface.poll`（超时重传等）在 timer 分支仍每 tick 调，**不动**——只把 O(总槽) 的 relay sweep 换成 O(dirty)。

`MetricsSink::note_listeners(n)` 的语义从「全量 handle 数」变为「本 tick 处理的 dirty 数」——harness 直接量化 #1 优化效果（relay_calls/avg_listeners 应从 O(总槽) 掉到 O(活跃)）。

辅助：`ListenerRegistry::handles_for_port(port) -> &[SocketHandle]`。
解析任意 TCP 包 dst_port：扩 `inspect_inbound_syn` 旁加 `inbound_tcp_dst_port(pkt) -> Option<u16>`（任意 TCP 包，非仅 SYN）。

### B. #2 per-port 弹性扩容（`ListenerRegistry`）

- `ensure_port`：首建 `pool_size` 个 listen 槽（保留）。
- 新增 `ensure_spare_listeners(port, min_spare, sockets, socket_ctxs)`：保证该端口当前 **Listening 状态槽 ≥ min_spare**，
  不足则 `sockets.add(build_listener_socket)` 补建并登记 `SocketCtx`，**受全局总槽上限约束**。
- **全局上限**：`MAX_TOTAL_LISTENERS`（默认 4096）。registry 维护 `total_handles`；达上限 `ensure_spare` 返回 `Capped`，
  退回旧行为（不扩，记 warning），不 panic。
- **触发**：rx 热路径 SYN inspector 命中端口（新建或已存在）后、`iface.poll` 之前调 `ensure_spare_listeners(port, MIN_SPARE)`，
  使本帧 accept 前恒有空闲 listening 槽。连续 SYN 渐进扩容到匹配并发。
- Listening 槽计数：查该端口 handle 的 `SocketCtx.state == Listening` 数（rearm 后恢复 Listening，可复用，不无限增长）。
- `MAX_INTERCEPTED_PORTS=64`（端口数上限）维持不变（端口数 × 弹性 pool 的总量由 `MAX_TOTAL_LISTENERS` 兜底）。

### C. fake-IP 引用计数回收（`fake_ip.rs` + 两条 flow 生命周期）

`FakeIpPool` 每个映射增加 `refcount: u32` + `last_used: u64`（秒，外部注入 clock，保持可测）：
- `alloc(domain, now)`：分配/复用，touch `last_used`（**不**改 refcount——DNS 查询不是 flow）。
- `acquire(ip, now)`：flow 开始，`refcount += 1`，touch。
- `release(ip, now)`：flow 结束，`refcount -= 1`（饱和减），touch。
- `sweep(now, ttl) -> usize`：回收所有 `refcount==0 且 now-last_used > ttl` 的映射（清 `domain_to_ip`/`ip_to_domain`），返回回收数。
- `resolve(ip)`：维持只读查询（不 touch；touch 由 acquire/release 在 flow 边界做）。

**TCP flow 打通**（client_tun.rs）：
- `SocketCtx` 增加 `fake_ip: Option<Ipv4Addr>`。
- `handle_local_payload` **首次开远端**（`uplink_tx.is_none()` 分支）且 target 来自 fake-IP → `fake_pool.acquire(ip)`，记入 `ctx.fake_ip`。
- `rearm_socket` 时若 `ctx.fake_ip` 有值 → `fake_pool.release(ip)`，清空。
  （`rearm_socket`/`process_listener_activity`/`handle_local_payload` 需要 `&mut FakeIpPool` 入参。）
- 注意：`resolve_target` 命中 fake 时返回 `(target, Option<fake_ip>)`，把 fake-IP 透出给上层 acquire。

**UDP flow 打通**（tuic.rs `AssocTable` + client_tun.rs）：
- `AssocTable` 的 entry 增加 `fake_ip: Option<Ipv4Addr>`。
- `handle_tuic_udp_uplink` 中 `is_new` 且 target 来自 fake-IP → 记入 entry + `fake_pool.acquire(ip)`。
- `AssocTable::sweep` 回收 entry 时返回被回收 entry 的 `fake_ip` 列表 → 主循环对每个 `fake_pool.release(ip)`。

**主循环周期 sweep**：复用现有 `udp_sweep`（1s tick）或新增 fake-IP sweep tick，调 `fake_pool.sweep(now, FAKE_IP_TTL)`。
`FAKE_IP_TTL` 默认远大于 DNS A TTL（5s），取 **300s**（idle 且无 flow 才回收）。

环形游标回绕坑顺带修：回绕跳过仍 `refcount>0` 或未过期的占用地址（或回绕前先 sweep）。

### D. #5 顺带

不改 `TCP_SOCKET_BUFFER_SIZE` 默认（65535，保 TCP 窗口/吞吐与稳定）。#2 按需扩容后，空闲 listen 槽数量
随真实并发增长而非一次性预建 `64×pool_size`，**空闲槽内存自然下降**。在 findings 表里以 harness 的
`per_sock_buf × max_listeners` 量化优化前后槽数差。

## 数据结构改造摘要

| 结构 | 改动 |
|---|---|
| `run_event_loop` 局部 | 新增 `dirty: HashSet<SocketHandle>`；relay 段遍历 dirty 而非 all_handles |
| `ListenerRegistry` | `total_handles` 计数；`handles_for_port`；`ensure_spare_listeners`；`MAX_TOTAL_LISTENERS` |
| `SocketCtx` | 新增 `fake_ip: Option<Ipv4Addr>` |
| `FakeIpPool` | 每映射 `refcount`+`last_used`；`acquire`/`release`/`sweep`；`alloc(domain, now)` |
| `AssocTable`/entry | entry 增 `fake_ip`；`sweep` 返回被回收 fake_ip 列表 |
| `MetricsSink` | `note_listeners` 语义改为「本 tick dirty 数」（接口不变） |

## harness 量化（优化前后对比）

复用 `tests/concurrency_harness.rs`（feature `harness`），无需改 harness 形态：
- **#1 验证**：`pool_size_isolates_sweep_cost`（N=256，扫 pool_size）—— 优化后 `relay` 段耗时/avg_listeners
  应**不再随总槽线性翻倍**，而随真实活跃连接走。`concurrency_sweep_report`（N∈{64,256,1024}）吞吐应回升。
- **#2 验证**：`small_pool_stalls_hot_port`（256 路单端口 pool=2）—— 优化后应从 `done=2/256` 变为
  **接近 N/N**（弹性扩容把单热门端口的并发墙打掉）。新增/改断言为 `completed` 大幅提升。
- 量化记录写入 findings 续篇或本 spec 同目录的「knife2 优化结果」小节。

跑法：`cargo test --features harness --test concurrency_harness -- --ignored --nocapture`（优化前先存基线，优化后对比）。

## 验收（acceptance）

1. `cargo test` 绿；现有 55 单测无回归；`cargo clippy` 0 warning；release build 绿。
2. 生产路径（`start_tun_proxy`）行为等价（人工 diff 复核 + 跨机 smoke）。
3. harness：`small_pool_stalls_hot_port` 单端口 pool 弹性扩容后 completed 大幅提升（接近 N/N）；
   `pool_size_isolates_sweep_cost` 的 relay 段不再随总槽线性翻倍。
4. fake-IP：引用计数下活跃 flow 不被回收（单测覆盖 acquire/release/sweep 边界）；idle 映射可回收。
5. 产出本刀优化前后对比数据（harness 三段表）+ 更新 HANDOFF/TODO 状态。

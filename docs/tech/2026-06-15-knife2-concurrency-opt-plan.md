# 刀2 — 大并发优化（TDD plan）

> 配套 spec：`2026-06-15-knife2-concurrency-opt-spec.md`。
> 纪律：每 task 先写失败测试 → red → 实现 → green → commit → **立即 push**（一分支一 writer）。
> 分支：`claude/knife2-concurrency-opt`（从 main 起）。每个 commit 后 `git push`。

## Task 0 — 基线（不改代码）

跑现有 harness 重量级实验，**存优化前基线**（写入本 plan 末「基线数据」或 findings 续篇）：
```
cargo test --features harness --test concurrency_harness -- --ignored --nocapture
```
关注：`small_pool_stalls_hot_port`（应 done≈2/256）、`pool_size_isolates_sweep_cost`（relay 随总槽线性翻倍）、
`concurrency_sweep_report`（吞吐随 N 跌）。

## Task 1 — #1 脏集合驱动主循环

**1a**（纯函数，可单测）：新增 `inbound_tcp_dst_port(pkt) -> Option<u16>`（任意 IPv4+TCP 包返回 dst_port，
非仅 SYN）。测：SYN/data/ACK 包都返回 dst_port；非 TCP/垃圾返回 None。
**1b**（registry）：`ListenerRegistry::handles_for_port(port) -> &[SocketHandle]`。测：返回该端口全部 handle，未知端口空。
**1c**（主循环重构）：`run_event_loop` 内新增 `dirty: HashSet<SocketHandle>`；
- rx 热路径：走 smoltcp 的 TCP 包 → `inbound_tcp_dst_port` → `handles_for_port` 全部入 dirty。
- `handle_remote_payload` 残留 `downlink_pending` → 该 handle 入 dirty（透出信号给主循环）。
- relay 段（rx + timer 两分支）：遍历 `dirty` 快照而非 `all_handles()`；处理后无 pending 且不 `can_recv` → 出集。
- `note_listeners` 传 dirty 数。
保正确性：harness `single_tcp_connection_round_trips` / `concurrent_64_all_complete` 仍绿（N/N）。
commit：`perf(knife2): event-driven dirty-set relay scheduling (#1)`

## Task 2 — #2 per-port 弹性扩容

**2a**（registry，单测）：`total_handles` 计数 + `MAX_TOTAL_LISTENERS` + `ensure_spare_listeners(port, min_spare, ...)`。
测：① 端口 Listening 槽不足 min_spare 时补建；② 已够则不动（幂等）；③ 全局达 `MAX_TOTAL_LISTENERS` 返回 `Capped`；
④ rearm 回 Listening 的槽计入空闲、可复用不重复建。
**2b**（主循环接线）：SYN inspector 命中端口后、`iface.poll` 前调 `ensure_spare_listeners(port, MIN_SPARE)`。
harness `small_pool_stalls_hot_port`：256 路单端口从 `done=2/256` → 接近 N/N；改断言为 `completed` 大幅提升（如 ≥250）。
commit：`perf(knife2): elastic per-port listener pool with global cap (#2)`

## Task 3 — fake-IP 引用计数回收

**3a**（fake_ip.rs，纯数据结构单测）：每映射 `refcount`+`last_used`；`acquire`/`release`/`sweep(now,ttl)`；
`alloc(domain, now)`。测：① acquire 后 sweep 不回收（refcount>0）；② release 归零 + idle>ttl 才回收；
③ idle 未到 ttl 不回收；④ release 饱和减不下溢；⑤ 回绕跳过占用地址。
commit：`feat(knife2): refcount + TTL fake-IP reclamation in pool (3a)`

**3b**（TCP flow 打通）：`SocketCtx.fake_ip`；`resolve_target` 透出 fake_ip；`handle_local_payload` 首开远端 acquire；
`rearm_socket` release（相关 fn 加 `&mut FakeIpPool`）。harness TCP 回归绿。
commit：`feat(knife2): wire TCP flow lifecycle to fake-IP refcount (3b)`

**3c**（UDP flow 打通）：`AssocTable` entry 增 `fake_ip`；`handle_tuic_udp_uplink` 新 assoc acquire；
`AssocTable::sweep` 返回被回收 fake_ip 列表 → 主循环 release。测 AssocTable sweep 返回值；harness UDP 回归绿。
commit：`feat(knife2): wire UDP assoc lifecycle to fake-IP refcount (3c)`

**3d**（主循环周期 sweep）：`udp_sweep`（或新 tick）调 `fake_pool.sweep(now, FAKE_IP_TTL=300)`。
commit：`feat(knife2): periodic fake-IP sweep in event loop (3d)`

## Task 4 — 收尾

1. harness 优化后数据；写 findings 续篇「knife2 优化结果」（#1 relay 不再线性、#2 单端口接近 N/N、#5 槽数下降）。
   commit：`docs(knife2): post-optimization harness results`
2. `/code-review` over diff → 修。
3. 跨机/压测 acceptance（用户配合 sing-box env，#3 probe 视情）。
4. 更新 HANDOFF / TODO 状态（刀2 完成、刀3 入口）。

## 基线数据（Task 0，2026-06-15 本机 darwin）

优化前（`--ignored --nocapture`）：

```
# #1 隔离（N=256，扫 pool_size）：relay 随总槽线性翻倍、avg_listeners≈总槽
pool= 8 总槽= 512  relay=124.2ms/ 926  avg_listeners= 494.6  thrpt=8.94Mb/s
pool=16 总槽=1024  relay=264.4ms/1052  avg_listeners= 993.3  thrpt=5.00Mb/s
pool=32 总槽=2048  relay=532.9ms/1104  avg_listeners=1989.6  thrpt=2.76Mb/s

# N sweep（固定 64×16=1024 槽）：吞吐随 N 跌
N=  64  relay= 53.4ms/ 209  avg_listeners= 869.7  thrpt=6.09Mb/s
N= 256  relay=231.7ms/ 942  avg_listeners= 989.8  thrpt=5.67Mb/s
N=1024  relay=1618ms/6322  avg_listeners=1018.9  thrpt=2.50Mb/s

# #2 单端口 pool=2（生产默认）：硬上限 stall
单端口 pool=2  done=2/256  wall=20000ms(超时)  listeners max=2
```

判据（优化后应达成）：
- #1：relay 段 / avg_listeners **不再随总槽线性翻倍**，而随活跃连接走。
- #2：单端口 256 路 **done 接近 N/N**（弹性扩容打掉硬上限）。
- 吞吐 N 曲线回升（relay 不再塞满单线程）。

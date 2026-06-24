# 刀9 plan — TDD 任务分解（F2 → F3 → F1 → F4）

> 配套 [spec](2026-06-24-knife9-auto-failover-spec.md) / [brief](2026-06-24-knife9-research-brief.md)。
> 节奏：每任务 写失败测试 → red → 实现 → green → commit → `git push`。一个分支一个 writer。
> 标注：**[loopback]** 离线单元/集成测可达 ｜ **[acceptance]** 需真出口跨机。

实现顺序按依赖：**F2 地基 → F3 并发接缝 → F1 failover 选腿 → F4 idle**（F1 受益于 F3 让 REALITY 切换不 stall，故 F3 先于 F1）。

## 阶段 0：脚手架
- T0. `FailoverState` 状态结构（AtomicU8/U32/U64 + 单调时钟）放 `src/failover.rs`（新文件）或 `reality_upstream.rs` 旁；纯逻辑无 IO，便于单测。先建空骨架 + mod 注册。

## 阶段 F2：分离 TCP/UDP 上游
- T1. **[loopback]** 失败测试：mock `ProxyUpstream`（捕获 open_tcp target）+ mock `DatagramUpstream`（捕获 send_udp），经 `FailoverUpstream` 断言 TCP 走选中腿、UDP 恒走 tuic 腿（沿用 upstream.rs:74 `CapturingDatagramUpstream` 模式）。
- T2. 实现 `FailoverUpstream`（持 tuic/reality/state；`send_udp` 恒 tuic；`open_tcp` 暂只读 active_tcp_leg 选腿，切换逻辑留 F1）。
- T3. 接线 `start_tun_proxy`：建两上游 + `tuic.start_udp()` 下行 rx + `FailoverUpstream::new` → `run_event_loop`。保留 `MINI_VPN_UPSTREAM` force-tuic/force-reality 旁路，默认 failover。
- T4. **[loopback]** 回归：UDP 下行 `tuic_downlink_rx` 注入路径不被破坏（V2 断言 a）；`select_upstream_kind` 单测更新（新增 failover 默认）。
- ✅ green + clippy + `cargo test` → commit + push。

## 阶段 F3：M3 握手并发化（仅 REALITY 腿 spawn）
- T5. `SocketCtx` 加字段 `conn_epoch: u64`、`uplink_buffer: Vec<u8>`（或 `VecDeque<Vec<u8>>` 保序）+ 256KB 字节上限常量；`SocketCtx::new` 初始化（spec §4.3 补丁 5）。
- T6. **[loopback]** 失败测试（不 stall）：mock upstream open_tcp sleep 5s，主循环并发另一 flow 首包，断言第二 flow 不被 stall（用 harness 风格驱动 run_event_loop）。
- T7. **[loopback]** 失败测试（epoch 防串话）：spawn 握手 → 结果回来前 rearm（epoch++）→ 注入旧 epoch 的 HandshakeDone，断言被丢弃、不装到新代 socket。
- T8. **[loopback]** 失败测试（顺序 + buffer 上限）：握手期连发 A、B → 完成后 uplink 收到顺序 A、B；buffer 超 256KB 溢出包被丢。
- T9. 实现：`handshake_done` mpsc + `HandshakeDone` 事件；`handle_local_payload` REALITY 腿改 spawn（TUIC 仍 inline）；主循环 select 新分支 `handle_handshake_done`（成功建 uplink+flush buffer+spawn_remote_relay；失败 rearm）；spec §4.3 五处补丁（含 rearm 清 buffer/丢结果、epoch 先于状态检查、reap_dead_slots 回收卡住 OpeningRemote）。
- ✅ green + clippy + release → commit + push。

## 阶段 F1：auto-failover（不对称 down快/up慢）
- T10. **[loopback]** 失败测试（down）：mock tuic 腿 live_conn 重连失败 1 次（快路）→ 断言 active_tcp_leg 切 REALITY；连接活但 open_tcp 失败连续 3 次（慢路）才切。
- T11. **[loopback]** 失败测试（up 迟滞）：REALITY 当班 + mock 探针连续 3 次成功 + 时钟过 60s → 切回 TUIC；不足 60s 或不足 3 次不切。
- T12. **[loopback]** 失败测试（F2 硬约束不被破坏）：active_tcp_leg=REALITY 时 send_udp 仍走 tuic（V2 断言 b）。
- T13. **[loopback]** 失败测试（铁律）：failover 冷却期内 send_udp→live_conn 重连不被抑制（注入「冷却中」状态，断言 tuic.send_udp 仍尝试 live_conn）。
- T14. 实现：`FailoverUpstream::open_tcp` 选腿后记成败（区分快路 live_conn 重连失败 / 慢路连续失败）+ down 切换；后台 up 探针任务（30s 节奏，连续 3 + 60s 冷却切回）；探活=握手完成+首字节。注入式时钟便于单测。
- T15. **[acceptance]** TUIC 打断 → 切 REALITY HTTP 200；恢复 → 60s+ 切回；UDP TUIC 当班通/REALITY 当班丢。
- ✅ green + clippy + release → commit + push。

## 阶段 F4：L2 relay idle 超时
- T16. **[loopback]** 失败测试：建 relay，两端 90s 无数据 → relay task 退出 + `stream.shutdown` 被调；中途活动则重置不退出。
- T17. 实现：`spawn_remote_relay` select 加 idle_timer 分支（90s，活动重置）。
- ✅ green + clippy → commit + push。

## 收尾
- `/code-review`（high/对抗式）over diff → 修。
- 真出口 acceptance（F1/F3/F4，见 spec §7）helper：扩 `scripts/knife8-reality-acceptance.sh` 或新建 knife9 脚本（打断 TUIC 的防火墙规则 + 切换观测）。
- 更新 HANDOFF「刀9 完成」段 + ADR（若 failover 策略值得立 ADR：`docs/adr/0011-tuic-reality-auto-failover.md`）。
- 记忆更新：core-roadmap（刀9 done，next 刀10=KeyUpdate 轮换）。

## 不做（本刀边界，防 scope creep）
- F5 KeyUpdate（刀10）；UDP-over-VLESS；TUIC 腿 spawn 化；连接复用；指数退避；新增 QUIC transport config。

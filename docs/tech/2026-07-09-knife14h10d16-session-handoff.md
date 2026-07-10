# Knife14h10d16 New Session Handoff

## Current Resolution Addendum (2026-07-10)

The implementation prompt below is historical: Tasks 1-10 and mandatory Task
11A are locally closed, including 50 capacity-qualified 64 MiB repeats at about
`224 Mbit/s`. The single authorized ACK-capacity Gate A then failed at
`19.2/17.9 Mbit/s` sender/receiver. TUN drops, actor bypass, local pressure,
send/flush failures, and QUIC loss/congestion/blocking were zero, but ordered
data reads had gaps up to `3548ms`. The data flow remained active at the final
snapshot, so natural EOF/close was not established despite zero observed tail
counters.

Do not restart Task 1 or Gate B. Start a diagnose/TDD sustained same-stream
discriminator at
`QuinnDirectOrderedNativeChunkRecv -> TuicNativeOrderedReader -> D16 reader`.
Preserve the byte-owned architecture and distinguish same-stream contiguous
availability from connection-global frame progress before editing production
code. Current result:
`docs/tech/2026-07-10-knife14h10d16-ack-capacity-gate-a-results.md`.

Use the following prompt to start the implementation session.

```text
继续 mini_vpn Knife14，执行已经批准的 H10d16 byte-owned TCP/TUN egress 架构计划。

项目路径：
/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn

启动后先完整阅读：
- AGENTS.md
- Rules.md
- HANDOFF.md 顶部 Current Override
- TODO.md 顶部 Approved next stage
- .learnings/LEARNINGS.md 最新 H10/H10d15 段
- .learnings/ERRORS.md 最新 H10/H10d15 段
- docs/tech/2026-07-09-knife14h10d16-byte-owned-egress-architecture-spec.md
- docs/tech/2026-07-09-knife14h10d16-byte-owned-egress-implementation-plan.md
- docs/tech/2026-07-09-knife14h10-tcp-tun-egress-actor-spec.md
- docs/tech/2026-07-09-knife14h9-d2-stream-egress-coupling-design.md
- docs/tech/2026-07-09-knife14h4-ordered-chunk-results.md

已确认的目标：
- 保留 H10d15 已证明的 185 Mbit/s 独立 read-pump 能力。
- 最终 Gate B：3 次 reverse-first P1 每次 >150 Mbit/s，median 目标 >=170 Mbit/s；若同窗口 sing-box 本身低于 170M，则 mini_vpn median 至少达到其 90%，同时每次仍 >150M。
- 所有 accepted run 必须 tx_dropped_delta=0、close_egress_bytes=0、terminal pending/reap=0，且无 terminal_closed_no_send。

当前阶段：
- 名义上仍在 1～11 计划的阶段 8：容量半门已过，clean Gate A 未过。
- 必须先重新闭合阶段 3～7 的本地合同，不能直接进入阶段 9，也不能先跑 VPS。

H10d15 关键证据：
- reverse-first P1 sender 186 Mbit/s，receiver 185 Mbit/s。
- native permit path + native_read_floor=true 生效。
- data read gap 已降到亚秒级。
- 尾部失败：tx_dropped_delta=229、global_rx_paused=true、terminal_closed_no_send、close_egress_bytes=327272。
- 本地 bundle：/tmp/mini_vpn_knife14h10d15_native_read_floor/mvpn_knife14h10d15_native_read_floor_gatea_usclient_suite_20260710_070433.tar.gz
- 远端 bundle：/tmp/conn/mvpn_knife14h10d15_native_read_floor_gatea_usclient_suite_20260710_070433.tar.gz

已确认的代码级架构断点：
1. src/tuic.rs 的 TuicNativeOrderedPumpReader 在 D6 byte queue 之前使用 message-count payload channel，QuinnOrderedNativeChunkSource 还忽略 _max_len；byte ownership 没从 Quinn read 前开始。
2. src/client_tun.rs 的 timer/TUN RX/process_dirty_relay 路径仍可在 actor 外调用 flush_downlink/send_slice；D3 actor 不是唯一 admission owner。
3. service_local_egress_until 在 hard pause/drop debt 时会在 ACK/TUN RX、iface.poll、flush_tx 之前返回；它停止了恢复所需的 drain。
4. native read pending 时没有同时 select read_credit_rx.changed()，pause 不能及时取消并退还 reservation。
5. clean EOF 可能与 dispatcher payload 通过不同 producer 竞争；D16 必须让 EOF 在同一个 owned queue 排空后才对本地生命周期可见。

批准的架构：
- 每条 flow 一个 RecvStream owner task；禁止 Arc<Mutex<RecvStream>>。
- Quinn read 前先取得 per-flow/global RAII byte reservation；读取不得超过 reservation，未使用容量立即退还。
- payload 只进入一个 per-flow leased byte queue；global_rx 只发 coalesced DataReady(handle, epoch) 和 close/error readiness，不再成为第二 payload reservoir。
- smoltcp Interface/SocketSet/TUN 继续由主循环单 owner。
- D16 模式所有 downlink send_slice 只能由 egress actor 发起。
- 背压状态为 Running / DrainOnly / Recovery：DrainOnly 停 read/admit，但继续 ACK/TUN RX、poll、flush、permit release。
- per-flow cap 512 KiB，process-wide cap 64 MiB，actor quantum 128 KiB，bounded/fair。
- remote EOF 只有在 queue closed+empty、pending=0、inflight permit=0 后才能推进本地 FIN/close。

执行要求：
1. 按 implementation plan 从 Task 1 开始，使用 diagnose + TDD；一次只完成一个 coherent task。
2. 每个 task 先写/确认 red test，再最小实现、green、局部门禁、code review。
3. 先完成 pre-read reservation、direct bounded ordered reader、readiness-only queue、actor-exclusive admission、DrainOnly、EOF ordering、integrated harness。
4. 本地关键门未全部通过前禁止 VPS。
5. Gate A 只跑一次 20s reverse-first P1，门槛 >150M 且 drop/tail 全 0；失败先按指标归因并给修改计划，等用户确认后再改。
6. Gate A 通过后才跑 Gate B 的一次 sing-box control + 三次 mini_vpn parity repeat。
7. 不再优先考虑 VPS/iperf3/MTU/PLPMTUD/stale pool/broad QUIC windows/chunk size/self-wake。
8. 不要承诺必到 170M；按可证伪门槛推进。

工作区注意：
- 当前分支 codex/knife14d-downlink-reap-open，工作区有大量既有脏文件。
- H4/H10/D3/D6/D15 实验 diff 与 D16 将修改的文件重叠；不要回滚用户改动。
- 可以直接实现和测试，但第一次 commit 前必须检查每个文件的完整 diff，向用户列出会包含的 pre-D16 改动并确认基线/提交策略；不能盲目 git add 整个重叠文件。
- 与 Knife14 无关的 Cargo、REALITY、DNS、failover 等既有脏文件不要碰。

安全规则：
- 不得把 .env 内容、TUIC UUID、TUIC 密码、私钥或 sudo 密码写入命令、脚本、docs、learning、日志或总结。
- .27 运行 suite 前只执行 . ./.env，不输出内容。
- sudo suite 必须从一开始使用工具 tty=true + ssh -tt；密码只在 prompt 输入。
- Slack 上次因第三方数据策略被阻止；没有用户新的明确批准不要重试或绕过。

VPS 环境（只在本地 Gate 完成后使用）：
- Client .27: 43.172.75.27，repo /home/ubuntu/mini_vpn，cargo 先 . "$HOME/.cargo/env"
- Exit .33: 43.153.32.33，sing-box；只做 service/sysctl preflight，不调参
- Target .77: 43.130.32.77，iperf3
- SSH: ssh -i ~/.ssh/vpn ubuntu@<host>

开始时先汇报：
- 当前 Task 1 的具体 red tests；
- 会修改的文件；
- 本地验收命令；
- 为什么该 task 是 170M 架构的必要条件。

然后执行 Task 1，不要重新发散设计，也不要先跑 VPS。
```

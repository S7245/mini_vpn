# N1 R5 — S6-MoveLayout 原 event-loop 归属抽取与 fixture 布局提案

状态：ProposedNotAuthorized。本文只冻结下一次最小范围修订；写入 Rust、Cargo/rustc、测试执行和生产行为改动均为 0。

## 请求的决定

请求批准 S6-MoveLayout。它只授权两个行为保持的 Move：

1. 把 open-custody 的共享 fixture 支撑迁入一个新的私有测试叶；
2. 把原 event-loop 中 relay-event 接收/处置切片迁入一个深生产 Module。

它不授权 stale-event 行为修复、cfg 测试 bridge、cheap W128、FIN、DNS/UDP、公平性、真实 reap 或完整 W 验收。纯 Move 绿到绿、独立审查通过后，bridge 和其第一个运行时 tracer 必须以新的精确范围提出；该 tracer 的任何非预期结果仍适用 repair-test stop rule。

## 冻结基线与停止原因

| 事实 | 当前值 |
| --- | --- |
| HEAD | 64f027fc2fe3e123bcc8dafc7c482cd286896f4f |
| Git index entries | 978 |
| 受治理 Rust 源码 | 461 |
| client_tun.rs | 16,166 / shrink-only 16,192 |
| compatibility_runtime.rs | 1,279 / 1,500 |
| compatibility_open_custody.rs | 693 / 800 |
| compatibility_open_pressure.rs | 409 / 800 |
| compatibility_open_settlement.rs | 794 / 800 |
| compatibility_relay_custody.rs | 754 / 800 |

R4 已局部接受，且只改变 settlement 叶 787 到 794 行。R5 是写前容量和可达性停止，而不是新的运行时失败：剩余 pressure 容量为 391 行、父 fixture 容量为 107 行，无法在一个可信叶中容纳完整 W 矩阵。R5 的原始证据保留在 docs/current/n1-20260918-evidence/l1e-r5-pressure-capacity-stop.json。

同时，源审计显示原 global_rx 接收边界尚未在所有副作用之前统一验证 epoch：

| 事件 | 已验证的事实 | 当前仍可能发生的旧 epoch 副作用 |
| --- | --- | --- |
| AckDrainHint | cadence helper 拒绝旧 epoch | 外层仍可能 arm D4、发布当前 relay credit |
| Closed | close helper 拒绝旧 epoch | 外层已清 D4 且无条件移除 dirty |
| Data | 下游 payload 路径会验证 | 在验证前已清 D4 |
| DataReady | 已有局部 epoch/state guard | 尚未纳入一个统一接收分类入口 |

这是静态预测，不是本次 Move 的行为失败结论。S6 必须保留这些当前分支顺序，不能暗中修正它们。

## S6 的精确 Rust 范围

唯一 Rust Writer 的允许路径和职责如下。

| 路径 | 允许的工作 | 行数/边界 |
| --- | --- | --- |
| src/client_tun/tests/compatibility_open_custody.rs | 只迁出共享 fixture 支撑；保留三个直接测试和现有 Module 身份 | 必须实际缩小，目标约 300 行 |
| src/client_tun/tests/compatibility_open_fixture.rs | 新私有无测试 fixture 叶 | 不超过 800 行 |
| src/client_tun.rs | 仅增加 Module 声明/必要导入，并迁出所列 relay-event 切片 | 必须净缩小；不新增 lint allowance |
| src/client_tun/legacy_event_loop/mod.rs | 新深 Module 根；写紧凑所有权文档和私有 re-export | 不超过 1,500 行 |
| src/client_tun/legacy_event_loop/relay_events.rs | 新深 Module；承接所列 event-lane 实现 | 不超过 1,500 行 |

S6 Writer 冻结后，Integrator 可单独改动 tests/code_growth_governance.rs，且只可把 src/client_tun.rs 的既有 shrink-only 数字下调到实际行数。不得新增 grandfathered 路径、allowance、例外或行为断言。tests/repository_test_module_provenance.rs 不在 S6 范围内。

以下 Rust 文件必须逐字不变：src/client_tun/compatibility_runtime.rs、src/client_tun/tests/compatibility_open_pressure.rs、src/client_tun/tests/compatibility_open_settlement.rs、src/client_tun/tests/compatibility_relay_custody.rs、src/client_tun/tests/mod.rs 和 tests/repository_test_module_provenance.rs。预期受治理源码仅因两个生产 Module 与一个 fixture 测试叶从 461 变为 464。

| S6 文件 | 可信后置预算 | 越界处置 |
| --- | --- | --- |
| compatibility_open_custody.rs | 约 276–300 行 | 不删测试/断言；停止 |
| compatibility_open_fixture.rs | 约 420–460 行，仅纯 Move | 不塞入新行为；停止 |
| legacy_event_loop/mod.rs | 约 20–60 行 | 不把实现摊回 root 或加新路径；停止 |
| legacy_event_loop/relay_events.rs | 约 1,050–1,300 行 | 不超过 1,500；否则停止 |
| client_tun.rs | 净缩约 800–900 行 | 不能用空白/无关移动凑数；停止 |

这些是写前冻结后的容量预期，不是豁免。任何实际行数、源图或依赖超出它们，Writer 必须停止并提出新的精确范围，不能借用 S7 路径或扩大现有文件。

## 必须保持的所有权与移动边界

run_borrowed_event_loop 继续是唯一总 owner，仍独占 TUN device、SocketSet、SocketCtx 表、dirty、fake-IP、UDP、iface、所有物理 timer/Sleep、session authority 和所有 sender 的生命周期。它在栈上本地唯一拥有一个 RelayEventLane；该值不产生 task、allocation、lock 或独立生命周期。

RelayEventLane 在 S6 中只保存原 global_rx、原 handle-only D4 HashMap、RelayGapAckDrainState 和 global_rx_receive_paused。它不保存 Sleep、TUN、SocketSet、SocketCtx、dirty、fake-IP、UDP、timer、session 或 sender。relay_gap_ack_drain_sleep 仍是 root 的原 Pin<Sleep> 局部变量，避免以 Box/新分配替代它。

root 保留两个 tokio::select! 触发表达式，而不是声称迁走整个 select：global_rx arm 只改为 lane.recv()，relay-gap follow-up arm 仍等待原 sleep。root 在进入 select 前以值读取 let global_rx_paused = lane.receive_paused() 和 follow-up guard，保持原 arm 次序，避免同一 select 对 lane 的可变 recv 与不可变 guard 借用冲突。事件选中后才以短暂私有 RelayEventTurn 调用 lane 的处置方法。

RelayEventTurn 固定为两个私有、非逃逸借用组。socket-state 组持有该 branch 已有的 SocketSet、SocketCtx 表与 dirty 借用；ingress/loop-state 组持有该 branch 已有的 device、assoc table、fake-IP、UDP clock、upstream、registry、iface、metrics、handshake/global/owned-action sender 和诊断/预算借用。后者还必须逐项借入而不取得所有权：

- deferred_ack_drain 与 deferred_ack_drain_sleep；
- d3_egress_actor_self_wake_armed 与 d3_egress_actor_self_wake_sleep；
- active_flow_tun_rx_drain_until；
- 既有 relay_gap_ack_drain_sleep。

lane handler 仅通过这些非逃逸可变借用在原相对位置 reset/arm timer 或写状态；不返回新的 timer effect interface，也不保存任何借用。所有 global_rx_receive_paused 的读写均变为 lane accessor。每秒 D4 timer arm 也留在 root、仍在原位置，只调用 lane 的 has/clear/arm accessors。source-order map 必须逐项证明上述四类 timer/state、D4 retain、select guard 和 branch effect 的原顺序。

因此 S6 迁出的是 global_rx 与 follow-up 的 branch disposition，不迁出 select 的物理等待/调度。新 Module 不能创建 task、全局 registry、动态分发、额外 queue、锁或 DATA 热路径分配，也不能长期保存 root 借入的 socket/TUN 状态。

冻结的移动清单以写前 source map 为准，当前锚点是：

- RelayClose 和 RelayEvent；
- note_ack_cadence_gap_hint_for_handle；
- D4 handle-only map helper；
- RelayGapAckDrainState 与相关 budget helper；
- 原 global_rx branch disposition 与 receiver state；
- relay-gap follow-up branch disposition 与 state；
- handle_relay_closed。

下列行为均须保留：HashMap<SocketHandle, Instant> 的 D4 表达、channel 容量、sender 寿命、select 顺序、timer/reap 时钟、dirty 更新、read credit、rearm、task/queue/lock 数量，以及 client_tun.rs 中已有 33 个 too_many_arguments allowance 的计数。不得迁出或改写带 allowance 的 publish_projected_relay_read_credit_for_payload。

legacy_event_loop/mod.rs 的私有 pub(super) re-export inventory 必须精确包含 RelayClose、RelayEvent、note_ack_cadence_gap_hint_for_handle、d4_stalled_read_service_deadline、arm_d4_stalled_read_service、clear_d4_stalled_read_service、has_d4_stalled_read_service、RelayGapAckDrainFollowup、RelayGapAckDrainState、tun_rx_drain_budget_for_relay_gap_hint、tun_rx_drain_budget_for_relay_gap_followup 和 handle_relay_closed。root 以同名私有 imports 提供既有 test Module 的解析；src/client_tun/tests/mod.rs 不改。

新 Module 根的文档必须说明：它拥有的实现切片、它不拥有的 root 状态、一个 event turn 的借用不逃逸、所有测试在外部 test leaf、以及该 Move 不建立 stale-event 隔离或运行时接受。

## Fixture Move 与身份保持

新布局只新增 fixture 叶：

~~~text
compatibility_open_custody.rs
├── fixture                         new, no test items
├── pressure                        existing, body unchanged
└── settlement                      existing, body unchanged
~~~

fixture 叶只接收 PendingOpens、OpenBehavior、RealApps、capture、ready、turn、cleanup、fixture、fixture_with_rx、OpenInventory、inventory，以及为该移动所需的 OpenCustody::observe_blocked 可见性收紧。三个直接 open 测试仍留在父文件；pressure 和 settlement 不移动、不改名、不删断言。

父文件的固定接线是：

~~~rust
#[path = "compatibility_open_fixture.rs"]
mod fixture;
use fixture::{
    capture, cleanup, fixture, fixture_with_rx, inventory, ready, turn,
    FixtureCleanup, OpenBehavior, OpenGuard, OpenInventory, PendingOpens, RealApps,
};
~~~

新 fixture 叶必须把以上跨 child 使用的名称声明为 pub(super)，只对 open_custody 父 Module 可见。详细可见性清单为：

- PendingOpens、它的 new 方法，以及 entered、dropped、gates、fail_with_drop_panic、error_dropped、behavior、peer 字段；
- OpenBehavior 的全部既有 variant；
- OpenGuard 及其 tuple 构造字段；
- RealApps 的 device、iface、sockets、handles、sent、windows、targets、send_batch 字段，以及 turn、queued、established 方法；
- FixtureCleanup 的 state、entered、dropped、timed_out 字段；
- OpenInventory 的 slots、workers、live、announced、dropped 字段；
- capture、ready、turn、cleanup、fixture、fixture_with_rx、inventory。

其余仅由 fixture 内部使用的辅助类型维持私有。移动后的 impl 必须精确写成 pub(in crate::client_tun::compatibility_runtime) fn observe_blocked(...)；旧的 pub(in super::super) 在新增 fixture 层后不能到达 OpenDispatch 调用点。这个可见性收紧不扩大 production/test API。

以下 11 个现有测试的完整 FQN 与函数体都必须保持：

- open_custody::open_admission_retains_socket_bytes_until_custody_available
- open_custody::open_stop_joins_all_workers_after_observer_cancellation_without_gate_release
- open_custody::open_error_drop_panic_retains_completed_worker_until_notice_join_and_disposition
- open_custody::pressure::open_pending_byte_caps_retain_original_rx_and_append_keeps_absolute_deadline
- open_custody::settlement 下现有七项

event Move 还必须保持现有 relay_closed_epoch_guard_drops_stale_close、relay_gap_hint_boost_requires_current_relay_context、relay_gap_hint_tun_rx_drain_budget_requires_current_relay_epoch、d4_stalled_read_service_requires_d3_actor_and_expires_per_handle，以及相关 relay-gap/reap 测试身份。

## 绿到绿执行与停止线

1. Writer 先冻结 461-source manifest、HEAD/index、11 个 open FQN/body map 与 event-helper body map；event map 必须逐项覆盖 global_rx pause guard、两个 select arm 的顺序、channel capacity、sender lifetime、D4 retain、relay-gap deadline/reset 和 handler 前后 effect 顺序；不写 bridge 或新行为测试。
2. 只完成 fixture Move。先后运行当前 all-feature open-custody prefix（11 项）、default compatibility-runtime custody prefix（7 项）和相关 root event-helper tests。任何非预期失败立即停止。
3. 独立冻结 fixture 后的源图，再只完成 event-lane Move；重复同一相关命令，确认原顺序和所有行为保持。
4. Writer 冻结后，Integrator 才能下调一个治理数字；随后跑 code-growth、repository-module-provenance、限定格式检查和 git diff --check。
5. 独立 Reviewer 只在 Writer 冻结时审查。只有 P0/P1/P2 为 0/0/0 才能接受 S6 的纯 Move；任何失败先保存 archive/source/log 并等待具体 repair 提案的用户确认。

S6 的退出不是完整 W，也不是 stale-event 已修复。它只产生一个可审计、仍由原 loop 唯一 owner 持有的深实现边界和一个不再占满父测试叶的 fixture 布局。

## 已审计但明确延后的 S7 bridge 合同

S6 不实现 cfg-only bridge，也不放未使用 anchor。可用 bridge 会新增测试状态、liveness 和 API，因此不是纯 Move。后续 S7 如获单独批准，候选设计是：

- 仅 cfg(all(test, feature = "harness")) 下的 RelayEventTestBridge；
- 通过 TunRuntimeConfig 的 test-only 私有字段进入原 loop，不改变 run_borrowed_event_loop 的生产签名，也不改 compatibility_runtime.rs；
- lane 只安装 global_tx 的 WeakSender；test 只能 upgrade 后 try_send 一条真实 RelayEvent，不能看到 receiver、强 sender、SocketCtx、dirty 或 timer；
- 每次真实事件处置后最多覆盖写入一份标量 snapshot，含 generation、event kind/epoch、当前 context 摘要、dirty membership、D4 deadline 与 relay-gap 状态；
- loop drop 后 upgrade 必须失败；bridge 不得延长 session、channel 或 relay 生命周期。

该合同仍需 S6 后的独立审查和新的精确路径授权。首次 tracer 要逐事件注入 old AckDrainHint 或 old Closed，并在其任何运行时结果后依据 repair-test stop rule 停止；不得把 Tokio 虚拟时间当作 std::Instant grace/reap 事实。

## 不变的非目标

S6 不推进 P3、N2–N5、条件 S3、F/S1、cold owner、production cutover、外部/TUN/VPS/WAN/throughput/M2 或完成邮件；不 commit/push/reset/clean/stash；不丢弃任何脏工作、R4 失败/修复证据或 R5 停止证据。

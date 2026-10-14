# mini_vpn Current Work

Last refreshed: 2026-10-14.

## 后续规划：N3 自动节点切换（2026-10-09 用户决定）

- [ ] N3：自动启动备用节点、运行中 TCP/UDP 自动换出口与自动回切。已移出本轮必做范围，未来单独规划和授权；节点启动失败由用户自行选择其他节点。

本轮规划顺序为 N1 → N2 → N4 → N5，保留所选节点的自动重连与故障恢复。用户接受恢复新通信、不保证原应用连接无缝续传的边界。精确范围与完成判据见[执行指南](docs/tech/2026-09-14-compatibility-recovery-execution-guide.md)顶部修订；当前 N1 接受位置与执行授权不变。

N4/N5 范围已于2026-10-09确认：N4 在 N2 后验证所选节点的并发、长时、循环故障和资源收敛；N5 在 N4/readiness/外部授权后验证深圳↔US启动、同节点恢复与真实链路质量。自动 A/B 切换/回切矩阵全部归延期 N3；同节点代际隔离、健康流保护和关闭验收保留。人工选节点耗时与程序恢复时间分开。完整矩阵以[执行指南§5](docs/tech/2026-09-14-compatibility-recovery-execution-guide.md#5-唯一剩余工作清单)为准。

2026-10-13 已批准并经独立审查的分层恢复修订生效：先修接入丢包，再贯通 Generic 同节点重连与服务owner下的受控会话重建，随后补齐family/S1和完整本地验收。旧资源未结算/未知债/Q禁止后继，最多一份活会话；120s失败边界不放宽原测试期限。唯一执行清单为 docs/current/n1-f1-implementation.md，窄范围契约为 docs/current/n1-session-rebuild-contract-20261013.md；其余禁止项不变。

## Current: N1 closed auxiliary recovery accepted; full Step B remains open

Current result: docs/current/n1-20260918-evidence/f1-aux-recovery-checkpoint-20261014.json. Actual legacy/staged Generic opens recover a closed auxiliary through one saved NodeHost transaction: original slot/preparation/lease custody, fixed first15s/at most2 attempts/at least500ms, whole-generation publication, actual predecessor/stats retirement and sticky failure/stop accounting. Typed configuration rejection stops retries within this NodeHost. Independent /root/n1_aux_final_review P0/P1/P2=0/0/0 accepts ClosedAuxiliaryRecovery only.

Final v3 source:576 total/178 vendor/398 governed, SHA2d16b0d0981962c24a7592617e306cd9432763aaf795fe1acb29fcdc5d7c63d1;8 existing changed+2 new,566 unchanged,0 deleted. All216 test executions on that final source pass (not necessarily unique tests), plus all-features lib/bin check, scopedfmt and diff. HEAD/index/locks/certs/device/harness/r8 guards remain unchanged. All REDs, rejected TLS retry oracle and v2 startup-cleanup regression remain in the journal.

Next: complete primary/UDP generation custody and healthy auxiliary replacement in the same root, preserving C=N+3 and original predecessor obligations. Then migrate four real production constructors with continuous saved-business/node driving and Failover health stop/join, followed by Generic same-node service/session rebuild. Production constructors are still legacy; full Step B/NodeSettled/Generic recovery are not accepted. Families/S1/fullW/F/cold/public/fullN1-L/R remain mandatory. Single Writer, independent Reviewer and evidence-based repair authority continue without renewed user approval. No commit/push/destructive cleanup/external/TUN/VPS/WAN/throughput/M2/P3/N2–N5.

## Historical N1 evidence and superseded continuation boundaries

The records below preserve evidence and past scope decisions. They do not extend the current action; the executable continuation is the current section above.

S7b 已局部接受。两个独立只读 Reviewer 均为 P0/P1/P2=0/0/0：`RelayEventLane` 在任何分支副作用前只接受当前 `SocketCtx` 的精确 epoch；实际 bounded bridge 的 2×4 tracer 覆盖旧 epoch/缺失 context × `Data`、`DataReady`、`AckDrainHint`、`Closed`，并锁定 D4、context/read-credit、dirty、gap 与 custody。治理 7/7、精确 tracer 1/1、三条 helper 回归、scoped rustfmt 和 diff 均 GREEN；唯一 ratchet 已在用户确认后由 15351 降至现有 15312。证据见 `l1e-s7b-final-independent-review.json`，RED、治理 stop 与 GREEN 均保留。

S8 已局部接受：两位独立最终 Reviewer 均为 P0/P1/P2=0/0/0。两处测试 Rust 改动使同一 private fixture 在构造 saved session 前设置 `cheap=true`，并以 129 条真实 TCP socket 锁定 W128、原始 4096B、完成但未被原 loop 消费的 slot、原 handle/new epoch 重新准入、observer cancellation 与 cleanup 前 shutdown join。精确 tracer、两条既有 W 回归、治理 7/7 与 scoped fmt/diff 均 GREEN；首次 E0277 compile stop、用户确认的仅诊断输出修复和重跑 GREEN 全部保留。动态 tracer 只证明完成 worker 在原 loop 下次消费前仍被 W 持有；handler→`consumed` 是同一 select arm 的源码顺序。证据：`l1e-s8-final-independent-review.json`、`l1e-s8-writer-green.json`、`l1e-s8-cheap-w128-unexpected-stop.json`。下一步只做 F4096/five-role flow custody 的结构收敛和容量预案及独立审查；不自动写 F/S1 Rust 或进入外部 gate。

F4096/five-role revision 3/4 review history 均保留。revision 5（SHA `a1caad5e…adeb84`）的三项独立复审均 P0/P1/P2=0/0/0，用户已批准该范围。L0 只允许 listener registry/test pure Move、characterization 与治理来源证明；F4096 admission、4352 slot、RX/TX 限额、fixture/RED-GREEN、flow custody 和外部 gate 仍留给后续 gate。

L0 已本地接受：`l1e-f4096-revision-5-l0-final-independent-review.json` 记录最终治理 P0/P1/P2=0/0/0。baseline green、E0425 stop、用户确认的唯一 `trace_enabled` import 修复与 1/1 green、provenance 579/575 stop、revision-1 reject、revision-2 accept、governance 7/7/provenance 20/20 green 全部保留。历史记录中的 467 是排除 `target` 后的仓库 Rust 文件总数；增长治理实际排除 `third_party` 的 173 份，当前 governed inventory 为 294。该术语更正及独立 P0/P1/P2=0/0/0 接受分别见 `l1e-f4096-revision-5-source-count-terminology-addendum.json` 与 `l1e-f4096-revision-5-source-count-terminology-independent-review.json`，不推翻 L0 行为或 7/7 结果。L1 写前映射（SHA `9002f723…ab6f3`）已由两位独立 Reviewer 以 P0/P1/P2=0/0/0 接受；其 L1a 预检与五条 baseline 均已完成，L1a 也已本地接受。

L1a、L1b 与 L1c RED 证据均已保留。用户确认后，历史 `ensure_spare_listeners_respects_global_cap` 已保持身份地更新为 zero-partial-write（2），其 abort 槽计数注释亦已更正；它与三个 direct tracer 均为 1/1 green。最终测试范围独立复核 P0/P1/P2=0/0/0，唯一 P1 已关闭。S0-G map 已独立 P0/P1/P2=0/0/0 接受。

S0-G 与 F0 的接受边界不变。G0-T 的三条物理 rearm characterizer 仍留在 private `compatibility_rearm_custody.rs`，G0-M 的 Generic dispatch pure Move 结论也保持。G0 gateway 现已本地接受：新 private `flow_custody/rearm.rs` 是唯一 no-F `request_close_or_rearm` mint 与物理 abort/clear/fake-IP-release/epoch/relisten sink；握手、reaper、remote payload、deferred close、owned action 和实际 `RelayEvent::Closed` 都经原 loop 的 borrowed gate。`ReapPlan` 先只读 inspect、先 gate、后 apply；当前 Closed bridge tracer 还在 rearm 后经同一被动 bridge 注入旧 epoch `DataReady` 并锁定拒绝。custody 与治理两位最终独立 Reviewer 均为 P0/P1/P2=0/0/0；rearm child 6/6、flow-custody 8/8、七组受影响 legacy filters、growth 7/7、provenance 20/20、scoped Rust-2024 fmt 与 diff 均 green。root shrink-only ratchet 为 14531、`too_many_arguments` 为 32；库存为 472 total / 173 third-party excluded / 299 governed。首次全仓 fmt 的非本范围脏格式失败、一次 test-only re-export compile stop 与用户确认的一行 `legacy_event_loop/mod.rs` repair 均保留。见 `l1e-f4096-revision-5-g0-final-acceptance.json`。这只接受 legacy no-F rearm gateway。其后 F1-T-CAP 的历史容量停止已由接受的 map-free allocation 取代：S4 条件已满足，且仅分配 settlement child 拓扑；在该 capacity-allocation boundary，当时的下一门是文档-only exact Writer map，而不是新的 capacity decision。该 map 现已接受；F1 Rust 仍须等待明确 scope/topology decision；P3/N2–N5 和外部验收未开始。

S6 的纯 Move 已本地接受（P0/P1/P2=0/0/0）。用户已确认最小 S7a：先把三个只供 event lane 的 helper 独立 Move 并 green-to-green，再通过被动 harness bridge 驱动一条实际旧 epoch `Closed` tracer。S7a 不修复行为；首个 tracer 结果必须留证并停在 S7b 之前。见 `l1e-s7a-bridge-tracer-proposal.json`。

R4已获用户确认并完成：唯一测试叶`compatibility_open_settlement.rs`从787到794行（+7，仍≤800），其余460份Rust源码不变。四个批准命令按序全部GREEN，`1 + 1 + 1 + 11 = 14`次测试执行通过、0失败；最终独立Reviewer结论P0/P1/P2=0/0/0，见`l1e-r4-final-independent-review.json`。461份源码、HEAD `64f027fc`及978项index保持冻结。此前L1e-F4的失败日志、快照、提案和stop verification均保留；R4说明它是outer saved driver的终态等待缝，不把旧失败抹除或改写为生产缺陷。

R5三份独立只读审计已在写Rust/Cargo前停止：pressure叶409/800只余391行，shared fixture父693/800只余107行；完整cheap/FIN/stale/reap/DNS-UDP/32-control/cleanup矩阵没有可信单叶闭合预算。更关键的是`global_tx/global_rx`、dirty和D4都属于原event loop局部状态，现有PressureProbe不能证明旧epoch `Closed`/`AckDrainHint`没有副作用；DNS/UDP和真实FIN也缺fixture入口/回包oracle。Reviewer结论P0/P1/P2=0/3/2。见`l1e-r5-pressure-capacity-stop.json`；这不是新的测试失败或repair，未运行Cargo、未写Rust，所有现有证据保持。

R5 whole-scope review 已收敛为 S6-MoveLayout，独立 Reviewer 复核后 P0/P1/P2=0/0/0。提案只请求：新 private fixture 叶、new legacy_event_loop/mod.rs 与 relay_events.rs，以及 src/client_tun.rs/现有 open-custody 父的两次纯 Move；461 个 Rust 源预计只增至 464。root 仍唯一拥有 loop/Sleep/所有 sender，栈上 lane 仅存 global_rx、现有 handle-only D4、gap state 和 pause bit；select 触发顺序和所有行为保持。bridge、stale tracer/repair、cheap W128、FIN、reap、DNS/UDP、公平性及完整 W 均明确留给后续范围。见 l1e-r5-s6-movelayout-proposal.json、docs/tech/2026-09-20-n1-r5-event-owner-move-layout-proposal.md 和 l1e-r5-s6-movelayout-independent-review.json。

用户已确认的 S6 已完成：/root Writer 与 Integrator 均已冻结，/root/n1_s6_independent_reviewer 最终复核为 0/0/0。完整 W、条件 S3、F/S1、cold 启动、生产切换和 TUIC 恢复仍未接受；全部脏工作与失败证据保留，禁止项不变。

S2开工前的预检：继续原N1/S1时458份Rust与L1d最终清单逐项相同，HEAD64f027fc/978stage字节不变，未发现并行Writer/Cargo。两位只读预检确认：runtime余832行；两条session测试叶余59/93，首个完整W128真实socket tracer估计240–395行，现有fixture不能覆盖。具体S2仅请求新增compatibility_open_custody.rs与compatibility_flow_custody.rs两条私有测试叶（各≤800），原生产scope/预算/完整矩阵/治理不变。用户已回复“确认”批准S2，见s2-approval.json；恢复原N1/S1实施，无需重批。证据见当前N1结果最新L1e及n1-20260918-evidence/l1e-test-scope-proposal.json；前一容量预检零Rust/零Cargo/零测试；本次实施另记证据，L1d接受结论不变。

### Historical accepted L1d boundary — open/flow custody provenance

用户在多Agent执行说明后回复“继续”，本次按冻结N1卡推进；本次权限仅N1。
[当前N1执行记录](docs/tech/2026-09-18-compatibility-recovery-n1-results.md)拥有实际改动、命令、失败记录、Writer与审查结论。
[指南§5](docs/tech/2026-09-14-compatibility-recovery-execution-guide.md#5-唯一剩余工作清单)仍是兼容路线唯一剩余工作清单。

M0/M1/M2均获原独立Reviewer的P0/P1/P2=0/0/0 Approve，仅接受纯Move。
M0外置device/main原测试；M1迁出完整TUIC reader slice，父6984/child1048；
M2迁出原336行启动壳，父16223/child360，DATA loop保持不变。治理同步收紧。
三步共1636次测试执行通过（541+570+525），不是1636个唯一测试，零测试失败。
原工具调用错误及一次格式检查失败保留，最终相关门禁通过。
N1-M接受时453份Rust中原449份有6份变化、443份不变，新增4份、没有删除。
N1-L的L0有界错误转换已独立0/0/0接受：record1128B、8case两模式通过，
共533次执行通过；仅值转换。L1a真实device pump生命周期也已独立0/0/0接受：
原8＋新11项两模式通过，累计561通过、8预期TDD失败、零非预期失败。
仅pump局部接受。L1b纯Move＋R2来源门禁修正已获原Reviewer独立0/0/0接受：
父13779/child458，原453行逆变换一致；保持338项。R2的20命令、509次执行通过。
L1b累计570次通过；原漏计child和R1漏识tokio属性的两次真实失败完整保留。
L1c共同整包pump＋显式设备stop/join已独立0/0/0接受：581次通过，
6次预期接口compile RED、2次预期行为RED，零非预期失败。device1171，harness4099，
两test叶707/749；实际Loopback与pump的退出/错误/压力已验证，Virtual只测已停止壳。
L1d0原loop wrapper纯Move已独立0/0/0接受：529次通过。
L1d持久原loop/device custody已获新独立Reviewer 0/0/0接受：最终源码591次通过，
累计694次（含中间版本，非唯一测试数）；2次接口compile RED、3次行为RED保留，零非预期失败。
外层保存device、内层原DATA借用；取消观察、poll/析构panic、固定关闭deadline与错误delta已测。
Running状态无新增DATA控制锁；仅局部DeviceSettled，不是整个session/TUIC clean。
最终458份源码无漂移，父16192/runtime668/新custody叶741；生产入口仍未切换。
下一门为同一root的W128 open/Flow4096五角色custody，再接已批准S1载荷与TUIC lifecycle。
N1-L整体与N1-R均未接受，TUIC恢复尚未实现；N1/S1原授权继续有效，无需重复申请。
先预算既定test叶（剩59/93行）及runtime（剩832行），不得增未授权路径/豁免或压缩空白凑限额。

S1是用户已明确批准的最小范围/API修订：将src/upstream.rs的NativeTcpRelayStream
加入N1-L范围，携带尚未启动的opaque pump控制载荷；session持久custody统一
登记与实际join。公开结构体构造方式改变，8处既有调用适配，任务/timer/error
预算不增加。两位只读Agent已核实缺口；授权原文见s1-approval.json。

[N0已接受结果与唯一N1卡](docs/tech/2026-09-18-compatibility-recovery-n0-results.md)保留D1–D4、D5-B、checkpoint、
542次baseline执行和2次历史fixture失败。N0接受时449份Rust未变；N1现在已有
上述纯Move改动，不能继续把“N1未授权/当前Rust未改”当作当前状态。

HEAD64f027fc、978个Git stage entries和原P3脏WIP保留。不reset/clean/stash，
不盲目add。L1c冷启动457份源码及Git逐项一致；L1d新增1份已授权test叶成为458份。
上述L1d接受时/root/n1_writer冻结，/root/n1_reviewer独立只读最终0/0/0；历史/root/wip_audit身份不冒用。
L1e曾由同一Writer续做、/root/n1_capacity_review独立审查；这是一项已结束的历史阶段事实。当前
S6-MoveLayout 已局部接受，Writer/Integrator/独立Reviewer 均已完成；
后续 Rust 范围须先有新的明确 proposal 与用户决定，见 authority.json。
真实repair-test失败须先证据/具体方案并等用户确认下一次修复。
不含N2–N5、P3行为、commit/push、外部/TUN/VPS/WAN/增强M2验收或完成通知。
P3 aggregateP1、五项51B、Case C/R7/Task4及旧M2义务全部保留。
下方P3记录只作历史位置，不能据其旧授权自动继续P3。

Historical completed-task chronology is archived at
`docs/archive/project-memory/2026-08-25/TODO.full.md`.

## Historical P3 remaining-work checklist — frozen during current N1 F1 stage

The material below is retained as provenance only. The current executable continuation is the F1 Writer map at the top of this file.

The historical executable checklist was joint contract section51. It was refreshed against real GREEN evidence, not the historical runtime RED. Full consumer/P3/Case C/R7 remain incomplete.

- [x] Resolve private C expiry test-leaf permission and establish its real
  behavior-bearing route. Parent797/child284; same Heisenberg SOURCE0/0/0.
  `/tmp/mini-vpn-terminal-consumer.gnAZqG/c-expiry-index.json` owns395 hashes,
  original parent inverse,35 command records including route E0583, and review.
- [x] Normal multi-Z: actual original facts/actions and C/output/Sleep expiry;
  all four insertion rotations, removal/FIFO/free partition and exact reuse.
  Exact1/1+10 repeats EACH mode, related/provenance/growth/scoped checks green.
  No production edit; not capacity-exhaustion/pressure/Active-Root acceptance.
- [x] Narrow shutdown-observer retention: real join block before observer drop;
  actual C/timer stay held; actor-only reentry remains nonzero for whole root.
- [x] Real BoundInactive HalfClose pressure and original expiry retention:
  child522/parent797;394/395 paths unchanged; no production/governance change.
  `/tmp/mini-vpn-terminal-consumer.gnAZqG/c-inactive-pressure.json`,
  same SOURCE01a0a41a-0f2e-7550-90d3-47f9415ec8ed:0/0/0. Exact1/1+
  10 repeats EACH mode and related/provenance/growth/scoped gates pass.
  Original completion1 and Executor Flow records2 remain after A expiry.
  Not waiter membership, duplicate registration, selected retry or wire acceptance.
- [x] Map existing actual single-waiter RESET repressure to same-resource double
  registration: inner/outer source path plus real3-round retention/handoff/final
  output; same SOURCE0/0/0, exact1+10 EACH mode. No new Rust; all395 hashes equal.
  existing-waiter-coverage.json; not HalfClose-specific repeated-pressure proof.
- [x] Matrix2 real output-pressure across deadline and automatic recovery:
  four actual actor permits, fifth FIFO ACK, same-clock101ms retention, original
  release, exact5 nonfinal/1 final ACK, successor expiry/park. Parent798/child232;
  physical-pressure.json, exact+10 EACH mode, related/growth/scoped gates pass;
  same Heisenberg SOURCE0/0/0 ONLY this test. No production/fixture change.
- [x] Matrix1 HalfClose-specific selected repeated-pressure payload/association:
  completion.rs268->640, original268-line prefix exact;395 other Rust paths
  unchanged. Three actual competitorZ rejections preserve whole SinkHalfClose,
  Source identity, waiter, transit0/HalfClose1; final ACK/terminal once.
  half-close-pressure.json; exact+10 EACH mode and related/growth/scoped gates,
  same Heisenberg SOURCE0/0/0 ONLY this test. No production/new path change.
- [x] Matrix5 actual post-terminal late wake and independent Flow DATA:
  completed two-path grant, parent721->722 private route/new child431;
  old parent inverse exact and395 other Rust paths unchanged. Original A
  ReadReady Pending -> RESET/FlowFinished/publication end -> old publisher;
  legal pre-B dispatch excludes reads and preserves Target snapshot. B actual
  read/Driver/inbox/reduction yields exact DATA once, preserving Source identity,
  A TerminalSettling/Z and B unACKed Replay11bytes/1segment. terminal-late-wake.json;
  exact+10 EACH mode/related/provenance/growth/scoped checks green, same SOURCE
  01a0a7ec-8cc3-7c51-b560-7077ba7bb844:0/0/0. Component invariant only.
- [x] Matrix3 direct TargetAndProgress fault-injection custody: completed user
  four-path grant; same Heisenberg SOURCE01a0a81b-9117-7261-b0d6-edc28a989ead0/0/0.
  /tmp/mini-vpn-returned-error.405z5b/evidence.json: expected runtime RED->GREEN,
  both-mode10 repeats/related/provenance/growth/check/scoped checks pass.
  Original caller error/C bundle/window/Executor tail remain, two rejected
  reentries stable; no non-test behavior or governance change. Not natural
  double-failure reachability, unknown recovery, nested payload or full matrix3.
- [x] Matrix3 defensive nested-observation test: completed single-file grant,
  returned_error.rs140->326, original test byte-exact;397 other Rust paths equal.
  Real RESET error/fact with injected wrapper; known caller/unknown C keep both
  Box identities/full fact/window/nonzero tail across two rejected reentries.
  Direct GREEN+10 EACH mode; related/provenance/growth/check/scoped gates pass.
  Same SOURCE01a0a98f-df19-76a3-b110-1237761c6eb3:0/0/0 only this component;
  /tmp/mini-vpn-nested-observation.9o5AQA/evidence.json and2026-09-16 local result.
  No production/route change, natural-conflict or recursive-inventory claim.
- [x] Source-only next-axis selection: real checked recovery FIFO-pressure is
  applicable; natural nested conflict/unknown callback still lack a witness.
  Aggregate cold-peak proof gap retained as SOURCE P1, not a confirmed OOM bug.
- [x] Direct production cold Layout-rejection/exact-plan test: user single-file
  grant completed, process_plan.rs415->433, old prefix exact;397 other Rust
  paths unchanged. Direct GREEN+10 each mode/full scoped gates; same Heisenberg
  SOURCE01a0ad1d-21b4-7b22-96ec-caba625f586b:0/0/0, only this characterization.
  /tmp/mini-vpn-process-cold.YMBNAt/evidence.json and2026-09-16 local result.
  Aggregate cold-peak P1 is NOT closed by this actual cold-entry rejection.
- [x] Checked-Bound C recovery FIFO/cursor pressure: real FIFO1/E1, three rejected
  publications, exact acceptance/OpenResult delivery and retained HandoffReady.
  Test-only706, SOURCE0/0/0; both-mode exact10, fixture9/Controller61 and gates.
  `/tmp/mini-vpn-p3-recovery.HLxB8L/evidence.json`; not full nested/unknown closure.
- [x] joint51E pure Move prerequisite: complete original C/owner inventory impls
  and provenance guard, exact ratchets; same SOURCE0/0/0 and both-mode gates.
  /tmp/mini-vpn-p3-nested-inventory.q0LWgl/move.json; no nested behavior yet.
- [x] joint51E finite error family: actual nested fact RED->GREEN, iterative
  borrow, typed Uncovered/overflow, original C and Root failure ledger; same
  SOURCE0/0/0. Both-mode C64/owner9/cont17/CurrentOnly13/server25/gates and40
  focused repeats. behavior/behavior-gates.json; full carrying inventory open.
- [x] joint51E recovery error cursor/resume family: real FIFO3 pressure RED->GREEN,
  original cursor retry/peer results, pending/ready/retained array capacities,
  ten checked metric axes and explicit diagnostic counterexamples; same SOURCE
  0/0/0. Both Target171/C65/related gates,20 exact repeats; cursor/cursor-gates.json.
  Not C normal recovery inventory, natural unknown, aggregate peak or P3 closure.
- [x] joint51F complete current carrying-error traversal, same SOURCE0/0/0.
  Actual holder/byte-view/permit/array/token audit; lower nominal owner locality,
  checked overflow/invalid-frame fail-closed, no audit mutation/allocation/clone.
  423 hashes/18 paths;32 final gates/80 repeats green; no old debt growth.
  remaining{,-gates,-repeats}.json; not normal C recovery/aggregate peak/P3.
- [x] joint51G normal C recovery inventory, same SOURCE0/0/0: real checked
  FIFO1/E1 false-zero RED->GREEN; original retries/delivery preserved. Borrowed
  six recovery/two status/high-water/quarantine/aborted, typed outer error.
  normal-c*.json:424 hashes/8 paths/416 unchanged,24 gates/60 repeats; C3007,
  parent516/new501/C test693; no debt edit. Not matrix3/physical/P3 closure.
- [x] Current first-path matrix3 applicable closure: real checked typed fatal
  retains original Executor cursor/output/effect and stable C/FIFO across two
  refused reentries. Test-only575; SOURCE0/0/0,24 gates/40 repeats,424 hashes.
  No natural unknown callback/await identified in current recovery alphabet:
  SOURCE N/A-now, not runtimePASS; future51B/P4–7 obligations are not waived.
- [x] joint51I original sender descriptor backing: capacity4/1 RED->GREEN,
  real bounded1/2/4/5 and wrapped move, actual old+new Layout, original empty/
  nonempty Drop/refusal/retry, ACK retained capacity, no effect-hook conflation.
  tcp.rs1995/no ratchet edit; SOURCE0/0/0,34 gates/40 repeats,426 hashes/8paths.
  Only this descriptor transaction, not receive/maps/Target/aggregate P1.
- [x] 51J preparatory preview_receive pure Move:195 exact lines into existing
  receiver, tcp1995->1800/receive603, sole Integrator debt updated. Same SOURCE
  0/0/0;426 hashes/3changed/423unchanged, three inverse proofs,20 gates+strict7.
  No new tree or Session Offer behavior; receiver-move{,-gates}.json.
- [x] 51J complete receiver/Session Offer local closure, same Heisenberg
  SOURCE0/0/0.430 hashes/12 paths/418 unchanged;31 gates+40 module repeats
  (220 test executions) rerun after the sole extend-to-push review repair;
  `/tmp/mini-vpn-p3-receive.T55xyv/closure-*.json` and review.json own evidence.
  Nodes/Offer/partial accept/both OPEN/input Drop/global priorities covered;
  no aggregate peak or whole-root closure. tcp1770/Session5585 ratchets exact.
- [x] 51K preparatory Session lifecycle pure Move, SOURCE0/0/0;432 hashes,
  six body hashes/both parent inverses exact;12 gates, Session5585->5380.
- [x] 51K complete Session node/terminal lifecycle local closure:434 hashes/9paths,
  31 gates/40 repeats/160 executions green; same Heisenberg SOURCE0/0/0 Approve.
  Session5364 ratchet; source.json and gates/repeats own exact evidence.
- [x] 51L M1 seven original DATA/receipt tests pure Move:435 hashes/3paths,
  17 green commands, same Heisenberg SOURCE0/0/0. New private test493.
- [x] 51L M2 complete original receipt/inventory/replay-release implementation
  Moves:436 hashes/6paths/430 unchanged;21 green commands after equivalent
  visibility repair, same Heisenberg SOURCE0/0/0. Supervisor4849/TCP2766 ratchets.
- [x] 51L M3 original Supervisor node/candidate-scratch transaction:439 hashes/
  11paths/428 unchanged,21 final gates/40 repeats/240 executions; same final
  Heisenberg SOURCE0/0/0. Supervisor4834 ratchet; actual original DATA refusal,
  prefix/permits/terminal/expiry/reuse and native node identities covered locally.
  behavior-*.json owns evidence; no Bound/whole-root/P3 closure inferred.
- [x] 51M actual Target retained-backing characterization: one real test,
  19 command records, same Heisenberg SOURCE0/0/0. No production behavior fix.
- [x] 51M complete seven-method Target inherent impl pure Move:208 lines
  byte-exact, both whole-file inverses; parent1289/child918/test58. Same
  SOURCE0/0/0;16 post-Move gates and20 exact repeats green,440 hashes.
- [x] 51M original Target return-publication transaction: real wake cold0-vs8
  RED->GREEN; concrete pending output, exact Reserve retry, partial/full
  terminal/expiry and actual B-writer rearm. Same Heisenberg SOURCE0/0/0;
  440 hashes/five paths/435 unchanged,20 gates/120 repeated test executions.
  output-*.json owns evidence. Not full Target backing/aggregate/P3 closure.
- [x] 51M original write/feed/directive Copy-buffer preparation: four real
  tests, including feed input Drop summed4/stored0 RED->GREEN; SOURCE0/0/0.
  440 hashes/three paths/437 unchanged,20 gates/200 repeated executions.
  buffer-*.json owns evidence; wrapped order, clamp, Reserve and reuse tested.
  Actual whole-Target backing/map/address peak remains mandatory.
- [x] 51M original Target address preparation/transfer: same Heisenberg0/0/0;
  441 hashes/three paths/438 unchanged,20 gates/60 repeated executions.
  address-*.json owns Reserve/caller identity/retry/join/raw semantics evidence.
  Native-node closure follows below; address/retained aggregate peak remains.
- [x] 51M complete737-line TargetIo pure Move: memory_io372/flow_io745;
  whole-parent inverse/body hash exact,442 hashes/two paths/440 unchanged.
  Same Heisenberg SOURCE0/0/0;20 gates/460 repeated executions green.
  node-move-*.json owns that behavior-preserving boundary.
- [x] 51M original native Target node transactions: same Heisenberg0/0/0;
  443 hashes/seven paths/436 unchanged;20 final gates/160 repeated executions.
  Eight tests include same-Box join/retire, refusal, actual Waker interruption
  and8192-node release on128KiB stack. native-*.json owns evidence; no ratchet.
- [x] 51M Target requested-backing peak: same Heisenberg0/0/0;444 hashes,
  six paths/438 unchanged. Two runtime RED->GREEN and five new tests;
  20 final gates/720 repeated test executions green. peak-*.json owns evidence.
  Original address overlap is checked before node allocation; readiness exact
  backing and cold independent capacity sums are covered. Not RSS or Root peak.
- [x] 51N complete progress Implementation Move:442 original lines,
  parent2789/child453/Integrator ratchet2789; body/parent/governance inverse exact.
  Same SOURCE0/0/0;445 hashes/three paths/442 unchanged,20 gates/680 repeats green.
  move-*.json in /tmp/mini-vpn-p3-progress.nYWfZO/ owns evidence; storage unchanged.
- [x] 51N complete capacity/Legacy cold-construction pure Moves: reverse2770/
  progress472/C2956/construction207; Integrator-only exact two ratchets.
  Same final SOURCE0/0/0;445 hashes/five paths/440 unchanged,20 gates/
  40 repeats/2020 executions green. construction-move-*.json owns evidence.
  Preparatory only; subsequent arena behavior closure is recorded below.
- [x] 51N original progress cold arena: final same Heisenberg SOURCE0/0/0,
  448 hashes/12 paths/436 unchanged;20 post-repair gates/2180 repeated executions.
  Real checked BoundInactive Replay/RESET/output/timer retirement; seven native
  tests preserve ordering/capacity/error/scan/corruption and actual header+Slot
  Layout. Review P1 combined header omission repaired before final acceptance.
  reverse2768/progress877/C2956; tests2393/native320/Cchild179; Integrator-only
  two exact ratchets. arena-repair-*.json and arena-review.json own final proof.
  No Active selected-read, naturally reachable C Layout failure or Root peak claim.
- [x] 51O real Root cold aggregate before the first reserve, final same
  Heisenberg SOURCE0/0/0;449 hashes/six paths/443 unchanged. Concrete generic
  header/resident/Ready/Arc/OwnedNotified composition; exact array backing and
  single pushes. Safe actual Layout/injected Reserve/full-plan/same-root start
  plus three native tests.22 gates/40 repeats/1080 executions, no warning delta.
  Root construction300/Ready1167/process_plan494/new leaf109; governance unchanged.
  /tmp/mini-vpn-p3-root-peak.iroUDm/{source,sequence,gates,repeats,review}.json.
  Empty cold phase only: C/physical/admitted/growth aggregate remains mandatory.
- [ ] Complete matrix4: actual Layout/cold peak/call charges/capacity/topology
  and independent combined consumer closure.
  Receiver51J/Session51K/Supervisor51L and Target51M local closures are above.
  Target requested-backing peak and original progress ordered storage are accepted;
  Root cold first phase is also accepted. Whole-scope review retains aggregateP1;
  source-pinned original-owner bounds need lifetime/overlap/release proofs.
  Temporary offline channel diagnostics: normal debug/release8/8 each plus20
  isolated defensive near-wrap observations, not natural lifetime/product bugs.
  Diagnostic same SOURCE0/0/0 and actual-route producer map are closed:
  checked Bound mailbox/TCP5/Actor3 serial, including privately held DELIVERED.
  Next original serial-push lifetime/overlap/close/wrap proof; not a guessed
  bound or concrete repair mandate. No Target port first, global queue rewrite or added
  message-lifetime cap. Generic Target/original Executor maps, channel/task and
  C/physical/runtime composition remain mandatory, with actual call charges/all51B.
  Outer Peer OPEN input retry is not supplied by51K; retain full51B duties.
  No scratch-only fix, sorted-Vec complexity
  relaxation, guessed std-node Layout or new unsafe site. Existing aggregateP1
  remains open; sender51I and receiver51J are accepted local components only.
  User CONFIRMED
  explicit original-owner capacity/reserve/growth; direction is no longer blocked.
  SOURCE01a0ad58-2d62-7b31-80c4-b7701cdaa081 accepts original-owner controlled
  contiguous Vec/VecDeque backing. First package: producer effects -> original
  Supervisor journal -> Executor current/suffix -> raw output. Joint51D now
  lists the exact requested packet; SOURCE01a0ad6e-ea3c-7270-8a07-0a0f2da40f62
  approved its scope; the complete packet is now locally SOURCE-approved by the
  same Heisenberg0/0/0. Original producer/journal/work/raw-output path, real
  registered/catch-up refusal/retry, fault custody, both-mode regression/repeats
  and exact shrink ratchets pass; progress.json owns411 hashes and actual gates.
  User grants continuous P3 repairs without repeated in-scope permission pauses.
  This does not close aggregate backing or remaining matrix3/4. Logical K/E,
  raw E+1, retained backing and transfer scratch remain separate obligations.
  Count moved backing once, simultaneous distinct backing separately, and no
  duplicate inline-header charge. No acceptance duty is removed.
- [ ] Complete all five later full-P3 packages in joint51B, not P4/P5/P6:
  OPEN/static progress; concrete TCP/DNS; all scheduler/capacity;
  Legless/full association; final lifecycle matrix/review.

- [x] Authorized terminal-read prerequisite: original finish_target_flow and strict
  Inactive/Active suspend_flow_reads; original service helper/validator unchanged.
  Same Heisenberg SOURCE01a0a3ab-62be-74e0-a6ee-3eb7fa065a56:0/0/0 Approve.
  /tmp/mini-vpn-terminal-consumer.gnAZqG/terminal-read-repair.json owns evidence.
  Real RESET-wake clearing and exact B nonfinal ACK/actor cleanup1/1+10 repeats,
  both-mode related tests/provenance/growth/scoped checks pass. No inventory edit.
  At that preceding checkpoint the consumer still timed out. Retain
  post-suspension late-wake runtime evidence as a full-consumer obligation.

- [x] C normal returned-page journal and narrow Knife count repair accepted:
  `/tmp/mini-vpn-terminal-c-journal.aT5m5e/accepted.json`, same SOURCE0/0/0.
- [x] Real nonempty ACK interruption test accepted, same SOURCE0/0/0:
  `/tmp/mini-vpn-terminal-frame-journal.V8Ix9v/accepted.json`.
- [x] Real Bound Target-error source characterized, same SOURCE0/0/0:
  `/tmp/mini-vpn-terminal-error-source.HR1f5q/accepted.json`.
  Only leaf789 changes; exact/repeats and both-mode related/provenance/growth/
  scoped checks pass. Known error is returned, not journal-held through unwind.
- [x] Obtain and implement narrow Knife one-function inventory authority: add only
  retained_target_errors to expected_inventory_fields, same-function whitespace
  for net-zero; preserve all existing AST/count/field checks. No scanner,
  threshold, governance or other-function edit. Prior count-only grant is settled.
- [x] C returned-error+bundle owning journal and actual error-owner inventory:
  `/tmp/mini-vpn-terminal-error-custody.fALxW9/accepted.json`, same SOURCE0/0/0.
  Real unwind RED->GREEN and known-error cleanup; four files3268/688/786/2624,
  both-mode related/provenance/growth/scoped checks green. Not nested payload,
  recovery interruption or direct TargetAndProgress coverage at that checkpoint;
  the latter now has the separate injection evidence recorded above.
- [x] Supervisor committed receipt journal: same SOURCE0/0/0, original Vec/
  current/observations retained through real Bound RESET interruption;
  `/tmp/mini-vpn-supervisor-receipt.g2BFGs/accepted.json`, three paths5439/192/794.
  Runtime RED->GREEN/ten repeats and both-mode scoped gates green.
- [x] Real filtered-ACK suffix and actual permit-wake interruption evidence:
  /tmp/mini-vpn-receipt-suffix.5optQG/accepted.json, same SOURCE0/0/0.
  Two direct-GREEN Unbound tests, ten repeats each; original Target retained,
  actual four permits0 and poisoned reentry. No new production behavior.
- [x] Authorized TCP borrowed bind and complete Supervisor DATA/caller-fact journal:
  same SOURCE01a0a329-6b89-71b2-bbfb-6f72b6f368fb,0/0/0/local9/10;
  `/tmp/mini-vpn-owner-data-journal.Rb7JPH/accepted.json`.
  Five paths2828/5439/422/217/669,388 unchanged. Actual DATA RED->GREEN,
  original-receipt rejection and real SinkAccepted custody; both-mode scoped
  regressions/growth/provenance/checks green. No second K backing or old API change.
- [ ] Mandatory first-complete-path real C action/Z/Sleep/expiry/Target/
  indexed Completion/C ACK-FIFO/successor, following the now-reviewed original
  Supervisor/Executor owning journal prerequisites. Existing revision2
  scope remains granted, no repeated borrowed-bind authority request.
  Preserve pressure/cancellation/error/cost and actual physical inventory duties.
  Component approvals do not establish Bound Root or full P3 acceptance.
  - [x] Falsify original Executor custody with actual MemoryTarget RESET/Read-Waker
    interruption: runtime RED guard0-vs-1, test leaf318 only;392 unchanged.
    `/tmp/mini-vpn-executor-journal.L4vAOl/checkpoint.json` records pre-repair RED.
  - [x] Pure Move the four complete original callers into existing observed_turn:
    parent8518/child1077, exact ratchet; same SOURCE0/0/0. Current checkpoint
    `/tmp/mini-vpn-executor-callers-move.lUd8El/accepted.json`.
    Old153 tests green, same new RED retained; this does not close behavior.
  - [x] Original Executor entered journal locally accepted, same SOURCE
    01a0a36a-82c8-78d3-811e-c1268afdc0ee,0/0/0/local9/10:
    `/tmp/mini-vpn-executor-full-journal.jttB1H/accepted.json`.
    Inline current/original suffix/facts/outcome/Releasing/recovery; no extra
    queue backing. Real RESET RED->GREEN and two callback cases each10 repeats;
    full Target156 both modes without skips plus scoped related gates pass.
    Nine Rust paths/384 unchanged, parent8515/exact ratchet; no new paths.
    Three exact old-test migrations retain original identities and counts.

JoinReady is now locally accepted: same Heisenberg SOURCE0/0/0, exact1/1 plus
ten repeats, OwnerTarget148/CurrentOnly8 both modes and scoped broad/growth/
provenance/check gates green. Prior JoinReady388 hashes and exact evidence:
`/tmp/mini-vpn-p3-join-readiness.ZFRrST/accepted.json`.
Original terminal retains first operational/typed readiness cause and refuses
repoll/join/release. Runtime custody proof and SOURCE exact-cause proof differ.

TerminalGrace revision2's full-token borrowing Interface P1 is now locally
closed after explicit authority: same SOURCE01a09f27-d218-7f62-b306-303226a7def1
and header supplement0/0/0. Real Executor original-fact/delayed-output pairing
RED->GREEN1; both-mode OwnerTarget148/Controller52/Supervisor48/Client37 and
scoped shared/provenance/growth/check gates pass. Four files5439/167/8841/406,
389 unchanged; current393 checkpoint:
`/tmp/mini-vpn-terminal-fact.gB8Hna/accepted.json`.
Consuming transfer, original-time/window, journals and complete consumer remain.
The real BoundInactive deferred-birth characterization is SOURCE-approved0/0/0;
Controller45 both modes/provenance/growth/check gates pass, not timer acceptance.
Preceding independent prerequisite is same-clock checked deadline preflight:
SOURCE0/0/0, six tests and Controller51/clock2/CurrentOnly8 both modes green;
clock100/test282 only. Preceding393-path checkpoint:
`/tmp/mini-vpn-terminal-deadline.G56H7j/accepted.json`.
It has no production C caller and does not discharge the owning consumer.
All four pure Moves are SOURCE-approved0/0/0 (5439/169,8841/754,3268/280,
990/698; exact ratchets and output entry removed), with actual related/
provenance/growth/check gates green. Current392-path evidence:
`/tmp/mini-vpn-terminal-grace-output-move.YMWwGL/accepted.json`.
No preparatory Move remains. Borrowed-full-token seam is accepted; implement the complete
normal consumer, then actual physical Completion/C-output. Unbound expiry auto-retry
invalidates the old prebind-Z-release shortcut. No mask-only/count-only substitute.
Inactive owning, Returned/Failed and physical Retirement normal path remain
accepted components, not complete selected service or full P3/Case C/R7.

- [x] Retain actual terminal/source through Target join/late DATA and exact Z
  expiry; preserve original cold Open/resume and Unbound finish/join evidence.
- [x] Preserve actual Driver rejection charge through original retry1->1->0.
- [x] Verify repeated actual Z rejection, cold Open charge isolation, private
  unchanged cold_open Move and independent real EOF/CLOSE disposition.
- [x] Authorize tcp.rs counted-ownership and poison errors, TCP private refusal
  leaf/route, and private bound_failure route using unchanged FailOnceTarget.
- [x] Execute ordinary revoke RED->GREEN and close poison review P1 with real
  poison RED->GREEN, exact reservation/completion/action/identity/isolation tests,
  broad gates and same Heisenberg0/0/0 SOURCE approval. Legacy helper unchanged.
- [x] Retain actual Failed work/cause through staging, Driver rejection and
  reduction until real FlowFinished; unchanged FailOnceTarget route now used.
- [x] Prove read-reservation release and RESET-staging progress-wake custody.
- [x] Close one-step early peer RESET sink-release P1: original Offer/backing
  and Target remain until actual terminal-first finish; same Reviewer0/0/0.
- [x] Prove actual cold/local Failure Driver-charge isolation through real
  Unbound failure/Z retry and independent Bound DATA; same Reviewer0/0/0.
- [x] Obtain exact private bound_failure/sink.rs test-path grant and complete
  pure Move before behavior: parent679/child128, exact122-line body/inverse,
  same Reviewer0/0/0, both-mode focused/full and provenance/governance green.
- [x] Complete incoming independent Write|HalfClose single-slot representation
  and known-Failed DATA/CLOSE admission via two real RED/GREEN tracers; same
  Reviewer0/0/0. This does not close every sink lifecycle/pressure branch.
- [x] Prove Owner terminal-revocation progress-wake custody with real queued
  FlowFinished and a fresh waiter; direct GREEN, same Heisenberg0/0/0. Original
  terminal/sink/Target/source/charge remain; recovery is not proved by retention.
- [x] Record real selected EOFs independently in original TerminalFlowSettlement;
  runtime RED->GREEN, real late-wake/reselection, same Reviewer0/0/0. EOF does
  not change transit/read credit or release Source/F/publishers.
- [x] Publish both original Driver tickets after successful terminal close;
  real parked Ordered selector RED->GREEN and exact EOF without a manual wake.
  Same Reviewer0/0/0; original BoundInactive due semantics remain intact.
- [x] Prove independent idle-Control first-wake causality: second fresh fixture
  row has no local RESET/read/receipt; Control-only mutation fails exactly in
  that row, original production SHA restored, same Reviewer0/0/0.
- [x] Preserve real cold-Retiring terminal proof at original Flow before Target
  work; actual Z2/Target tombstone1 installation RED->GREEN, ten repeats,
  Unbound-compatible admission and same Heisenberg SOURCE0/0/0.
- [ ] Complete actual selected Retiring service, completion/terminal disposition/
  join/retry and F/publisher retirement using that original Cold proof.
  Initial real revoke, full normal Source/F release, owning cold Open retry
  and one-vacancy/two-EOF-waiter tests pass, but do not close this checkbox.
  - [x] Authorize private bound_failure/completion.rs plus parent route and
    existing provenance; no fixture/helper/visibility or inventory expansion.
  - [x] Move145 original lines preserving the two known REDs, then migrate
    both real selected calls with unchanged assertions. Parent653/child244;
    both-mode14/14, exact repeats10 each, same Reviewer0/0/0 including format.
  - [x] Grant private flow_lifecycle/completion.rs, parent route/whole70-line
    test Move and existing provenance route/one-symbol lock; parent791->721.
    Also permit child-local pre-bind population via unchanged lifecycle_executor
    at original F4/Target4 for three real expiry/refill/selected-rejection cycles.
    No post-bind dynamic OPEN, shared fixture/config change or fake F/wake.
  - [x] Execute preserved-RED Move and three actual selected pressure rejections,
    final once-only success, regression gates and same independent review.
    Parent721/child268/provenance670; exact10 repeats, OwnerTarget145 both modes,
    repository19/Knife162, both growth7, scoped fmt/whitespace; same0/0/0.
  - [x] Close terminal-held Driver Returned charge and explicit Failed work
    release: actual Reduced/Returned/late-receipt rows; exact cause retained
    under Releasing/Released; Source/F reclaim separately. Same Reviewer0/0/0.
    Legal work has no user callback. Guard source proof is not runtime panic
    evidence; introducing callback-bearing work reopens this specific audit.
  - [x] Retain exact Join readiness failure cause on the original terminal.
    Both operational/typed faults use the granted Adapter at actual selected
    Join, same valid publication retries reject without repoll/join. SOURCE
    verifies private cause ownership; same Heisenberg0/0/0 and full scoped gates.
  - [x] Preserve lawful BoundInactive owning requests without weakening the
    strictly active selected-receipt validator.
    Granted pure Move accepted1095/395/provenance773 with original17/18 RED.
    Repair now515/private test555/provenance792; original RED->GREEN, plus
    real inactive Root-index contradiction RED->GREEN and seven private tests.
    Both-mode OwnerReady25/25, Target145/145, ten serial25/25 and scoped gates
    pass; same Heisenberg behavior SOURCE0/0/0 Approve. No new scope grant needed.
  - [ ] Enable completed Completion/Retirement in the original legal physical
    source-service plan and prove actual C/output selection, without changing
    pending-lifecycle/Page/continuation guards.
    - [x] Retirement normal path: actual cold terminal/backlog/expiry, unchanged
      checked C/CurrentOnly/adopt/start, real Root RED0vs1->GREEN1; sole actual
      RetirementReleased result and park. Same Reviewer0/0/0, no shared fixture
      changes/getter or postbind lifecycle claim; continuation1254/test600.
    - [ ] Completion: independent actual C/output evidence; not enabled or
      discharged by the Retirement tracer. Keep original lifecycle gate.
      Blocker: no lawful Bound Z-release entry. Explicit TerminalGrace-only
      scope must first connect real deadline->expiry->indexed completion->ACK
      and successor terminal custody; concrete paths/symbols require freeze.
  - [ ] After production repairs, run full regressions/provenance/growth/fmt/
    check and same Reviewer. The received_reset pressure test itself is now
    closed above; its old144/145 stop is historical, not the current test state.
- [x] Grant and close private retirement Moves: completion971/child510,
  bound700/child108, original495/100-line blocks inversely exact, existing
  provenance618. Same Heisenberg0/0/0; full gates green, no ratchet.
- [ ] Freeze and implement real C/root lifecycle installer/clock/deadline/due
  consumer. TerminalGrace complete normal consumption is explicitly granted;
  concrete owner/capacity/disposition/write freeze precedes Rust. Mandatory P3;
  no settlement-only goal is selected and other P3 lifecycle duties remain.
  - [x] Real checked BoundInactive terminal-before-deferred-output characterization;
    same Reviewer0/0/0, original APIs and self-contained private fixture.
  - [x] Independent mandatory prerequisite: one same-clock deadline preflight;
    positive/exact interval, checked relative/native additions, no allocation.
    SOURCE0/0/0 and six tests close primitive only; C wiring remains below.
  - [x] Mandatory first-path prerequisite: authorize the two original wrappers'
    borrowed complete TerminalGrace view; user "确定授权" received. Bundle
    consumption alone cannot supply exact install/expiry identity.
  - [x] Implement that borrowed full-token seam without changing scalar callers;
    real original Executor bundle/delayed-output equality and same SOURCE0/0/0.
  - [x] First C success-time component: immutable original window/full fact,
    checked positive policy/exact returns and cold Z backing. Same SOURCE0/0/0;
    real delayed-output/Missing continuation/policy tests and related gates.
    Evidence: terminal-window.dMNA9p/accepted.json; not full consumer acceptance.
  - [ ] Retain actual original observations through Supervisor/Executor/C
    precommit/known-error/unknown journals. Complete Z/install/timer/expiry and
    real Completion/C ACK-FIFO/successor, pressure/cancel/error tracers. The
    accepted success-time component does not discharge these obligations.
- [ ] Complete remaining joint51 A/B capacity/physical/credit/full review.

Prior accepted Move evidence only (not current WIP gates): five Rust paths
changed/new,383 final hashes. Parent9581 and
governance752 unchanged. Main: moved exact+10 repeats,
original Unbound1; Bound failure14/lifecycle10/OwnerTarget143 both modes;
OwnerReady18/TCP74/Root24/continuation16/Bound1; Knife162/repository17,
both governance7, fmt/diff/check and79untracked checks green. Existing warnings.
No inventory edit, public cutover, P4-P7, commit/push or external.
Component approval is not full P3 acceptance.

## Authorized review repairs, public path first

- [x] Implement and locally test fake-IP full-pool rejection and DNS SERVFAIL;
  independent portion review P0/P1/P2 `0/0/0`.
- [x] Implement fallible IPv4 UDP construction and exact bounded reassembly;
  independent portion reviews P0/P1/P2 `0/0/0`.
- [x] Close the non-suspending TUIC sender's P2 RESET-retirement capacity gap.
  Capacity RED/GREEN, current UDP `22/22` and all planned lifecycle tracers have
  passed. Isolated Quinn passed 49 unit and 1 doc test (4 ignored); the Writer
  has frozen implementation; original Popper returned P0/P1/P2 `0/0/0`, Approve.
- [x] Complete public gates and promote the combined public result. Both
  governance routes, provenance, selected public tests, format/diff and final
  all-feature library check pass. Four exact shrink entries are promoted at
  `16561/7991/14230/5390`; final source fingerprints and stop history are retained.
- [x] Independently accept the narrow client-root scan/stall-decision design.
- [ ] Implement complete driver coverage before root park.
- [ ] Implement retained eligible stall-decision coalescing after late ACK.
  Both root Rust repairs remain explicitly frozen; neither has started. P2T
  now continues with the coordinating task after completed public closure.

Exact evidence: `docs/tech/2026-09-05-public-path-capacity-repair-local-results.md`.

## Knife16 execution map

- [x] Task 1: freeze Knife15 and the protocol vocabulary.
- [x] Task 2: resumable protocol model and checked capacity characterization.
- [x] Task 3: deep `OwnedUpstream` seam locally accepted while the public entry
  deliberately remains legacy.
- [ ] Task 4: deterministic two-leg transport harness and replacement transaction.
  - [x] R1-R5 local protocol, ownership, scheduling, and baseline slices.
  - [x] R6a-d local two-leg lifecycle, late-record, recovery-capacity, and
    production-root prerequisites.
  - [x] R7 prerequisite: actor-captured same-clock inbound arrival timestamp
    carried through queue/publication and used for deadline evidence.
  - [x] R7 local policy kernel: bounded owner progress/hint retry, client
    rejection/sequence handling, and one epoch/nonce for all trigger orderings.
    This is typed value-level evidence, not physical production composition.
  - [x] R7 test-only physical Move/exact cleanup: private B-only adapter, exact
    scalar-equal/pressure settlement, and focused/governance/provenance review.
  - [x] R7 architecture-only convergence: close initial review Request Changes
    P0/P1/P2 `0/5/1` and closure re-review Approve `0/0/0` over the failure tree,
    two endpoint ledgers plus harness aggregate, `OwnerPhysicalSessionRoot<T>`,
    no-ACK terminal transaction, capacity/restart correction, Writer ownership,
    and stop rules.
  - [x] Correct the static necessary-only owner scan manifest from `2 * F` to
    checked `2 * (F + Z)` through typed `OwnerProgressCapacity`; characterize
    checked completed-scan plus revision-reset scenario width and every
    addition/multiplication/maximum/zero-scan boundary.
  - [x] Lock the nested private capacity/progress-scan Module route with the
    shared code-aware repository provenance scanner.
  - [ ] Add the later root-owned live revision-reset credit transaction:
    reserve before mutation, return on rejection/no revision change, and commit
    only after a successful revision advance. Install it only after one
    physical root owns both controller progress and `OwnerSwitchHintIssuer`;
    CurrentOnly must represent the absence as `NotInstalled`, never scalar zero.
  - [x] In a separate controller/output slice, reject a retained correlated
    `progress_entries` value as exact zero, with exact raw-peer terminal/FIFO
    fixture settlement and independent review.
  - [x] Behavior-preservingly extract the hard-terminal Implementation from the
    shrink-only `continuation.rs` under the 2026-09-02 narrow architecture
    amendment; change production state/order only in a later green-to-green
    slice. Exact A-D evidence is owned by
    `docs/tech/2026-09-02-knife16-r7-hard-terminal-extraction-local-results.md`.
    - [x] Freeze the parent-Interface/child-Implementation amendment and close
      the same independent Reviewer at P0/P1/P2 `0/0/0`.
    - [x] Move the existing Knife16 continuation provenance test unchanged into
      one private child and promote the smaller exact parent ratchet.
    - [x] Add the code-aware private/unconditional/default-path production-child
      route and symbol-ownership RED.
    - [x] Purely move the private state machine, correlation/quarantine, guard,
      terminal method bodies, transaction implementation, and `Drop`, while
      stable terminal Interface and root storage/mixed inventory remain in the
      parent.
    - [x] Promote the `continuation.rs` shrink from `1698` to `971` only after focused,
      regression, provenance, governance, format/diff/check, and independent
      review are green.
    - [x] Preserve production terminal behavior as a separately gated later
      slice; extraction itself proves neither exact zero nor reachability.
  - [x] Close the post-extraction owner-root ordering amendment:
    `CurrentOnly root Authority -> production terminal preservation ->
    root-owned no-ACK finalizer`.
    - [x] Repair the constructor commit edge, add the dated A-D evidence owner,
      and name the complete missing root-plus-constructor RED seam.
    - [x] Obtain independent architecture closure: the same Reviewer returned
      P0/P1/P2 `0/0/0`, Approve, with Clean Architecture and System Design
      `10/10`.
  - [x] Confirm by symbol-first audit that the existing initial-A seam can build
    the settled seed without a preparatory fixture/Authority Move.
  - [x] Execute the first constructor tracer through its expected `E0433` RED,
    focused `1/1` GREEN, and ten serial `1/1` repeats. The resulting
    join-custody P1 was an intermediate review stop; the later direct-join
    implementation result supersedes that prototype without erasing the
    failure evidence.
  - [ ] Close the complete CurrentOnly actor-resident lifecycle, including the
    still-open Case C parent-custody branch.
    - [x] Freeze the A/B/C distinction and unique `JoinCustody` contract at the
      `docs/tech/2026-09-03-knife16-r7-current-only-join-custody-amendment.md`
      writer boundary.
    - [x] Obtain independent architecture closure: the same Reviewer reported
      no findings, P0/P1/P2 `0/0/0`, Approve, closed the prior phase-ownership
      P1, and scored Clean Architecture/System Design `10/10`. This does not
      authorize Rust repair.
    - [x] Freeze the direct-join superseding architecture that keeps the exact
      actor in CurrentOnly and removes the extra spawn/JoinHandle transfer.
    - [x] Obtain independent direct-join architecture closure: the sole exact
      error P1 is closed; final review is P0/P1/P2 `0/0/0`, Approve, with Clean
      Architecture/System Design `10/10`. This does not authorize Rust TDD.
    - [x] Close living-root A/B through the two-file direct borrowed join:
      observer cancellation resumes the same actor, and pre-await unwind permits
      only same-actor roll-forward.
    - [x] Retain the complete direct-join result root-first, keep the exact
      modeled error in `Failed` for stable borrowed observations, run the
      focused/repeat/regression/provenance/governance gates, and obtain
      independent implementation closure at P0/P1/P2 `0/0/0`, Approve. Exact
      evidence is in
      `docs/tech/2026-09-03-knife16-r7-current-only-direct-join-local-results.md`.
    - [ ] Before production exposure or caller cutover, implement the accepted
      `OwnerServerRoot` parent so one owning slot accepts the entire CurrentOnly
      root through a consuming move; a whole-root Drop test cannot close this
      ultimate-destruction branch.
      - [x] Select the Case C process-parent architecture, whole-root adoption,
        bounded slot/capacity domains, and shutdown contract in
        `docs/tech/2026-09-04-knife16-r7-case-c-owner-server-root-architecture.md`.
      - [x] Close independent architecture review after the original five P1 and
        two P2 findings: final P0/P1/P2 `0/0/0`, Approve, architecture-only
        correctness `10/10`; implementation readiness remains `4/10`.
      - [x] Freeze the exact Rust route/privacy, non-test production caller,
        bootstrap-custody seam, allowed files/symbols, Writer ownership, line
        counts, and TDD filters in a separately authorized plan-only gate.
      - [x] Obtain independent implementation-plan closure after six P1 and one
        P2 findings: final P0/P1/P2 `0/0/0`, Approve. This accepts the plan only
        and authorizes no Rust.
      - [x] Freeze and independently accept the P1 architecture-only Target-open
        and ready-scheduler progress amendment with Rust zero. Heisenberg's
        replacement independent closure is P0/P1/P2 `0/0/0`, Approve; the
        2026-09-04 amendment owns exact evidence, not runtime acceptance.
      - [x] Enter authorized P2T: freeze six blocks/routes/hashes and execute the
        first pre-write governance check. It stopped `4/6` on independent
        public-path shrink debt; the 2026-09-05 P2T entry owns exact evidence.
      - [x] Close the independent public-path governance dependency through
        its own Reviewer/Integrator, four exact shrinks and both `6/6` routes.
      - [x] Revalidate P2T frozen source hashes, pass fresh governance `6/6`,
        and run all six pre-Move exact baselines, each `1/1` green.
      - [x] Complete and independently accept the four-file P2T pure Move,
        immutable-block/new-identity checks, full gates and Integrator-only
        shrink ratchets. Mencius was the sole Writer; same Heisenberg approved
        P0/P1/P2 `0/0/0`, findings none.
        Six formatted-tree exact tests, full provenance `159/159` and `13/13`,
        fmt/diff/check and both governance `6/6` pass. Only the two expected
        ratchets were lowered to `15818/2626`; new leaves are `480/374`.
      - [x] Close final P2T status-review P2: user-confirmed single-sentence
        correction precisely attributes Reviewer result/excerpt inspection.
        Same Heisenberg approved `0/0/0` and allowed final handoff; no Rust or
        tests changed or ran in that doc-only closure.
      - [x] Authorize and freeze P2A parameter-object convergence under plan
        Sections 6/12; autonomous in-scope red repair applies to this node only.
      - [x] Complete P2A: preserve bootstrap input/cleanup identity and order,
        pass the allowance tracer, stay within 7,001 lines, independently
        ratchet 7→6 and review. Implementation/characterization are green at
        7,001 lines; both governance routes 6/6, same Heisenberg final `0/0/0`,
        Approve. The dated P2A result owns exact commands/hashes/evidence.
      - [x] Authorize P2B's allowance-free bootstrap Move under Section 6.
      - [x] Close its separately confirmed test Move/path repair: 614/205,
        mapped expected RED, old-three GREEN, preparation gates and same
        Heisenberg 0/0/0 Approve. This accepts no production Move.
      - [x] Complete P2B implementation: production Move is 6769/243; source and real bootstrap
        gates/repeats GREEN. Confirmed factory source repair stays 2626; full
        Knife16 162/162, repository 13/13 and fmt/diff/check pass. Expected
        actor-only shrink is independently ratcheted; both governance routes
        pass 6/6. Same Heisenberg full review 0/0/0 Approve, no necessary
        optimization finding. P2T/P2A/P2B are now committed in `532d766`, after
        independently reviewed public prerequisite `5fca1ed`; no push occurred.
      - [x] Authorize complete P3 with autonomous causal in-scope repair;
        preserve architecture/growth/review gates and first-packet scope freeze.
      - [ ] Complete P3 root command custody, ready/fair scheduler and concrete
        split-phase TCP/DNS Target, not merely its first foothold. The initial
        command route, mailbox notification/prefetch, Control rearm,
        preparatory Moves and real Owner source characterization are accepted.
        Source placement and conditional Owner-local credit protocol passed
        independent0/0/0, including compact-doc closure. Target readiness
        protocol review found only a P2 source anchor, now repaired; final
        document/build-prerequisite closure passed0/0/0. Real registration, complete release-entry/
        guard coverage, cold pairing, generation/backing and per-Flow/K/Layout
        bounds still block binding implementation. See the 2026-09-07
        source-service decision, owner-credit and target-readiness contracts.
        The separate pure Target Implementation Move has green gates at1445/582,
        with production debt removed; same-Reviewer closure passed0/0/0. It does
        not implement readiness. Complete test Adapter Move has green focused/
        scoped regressions at10206/726/133; exact ratchet, both governance,
        provenance and all-feature check pass. Same-Reviewer final closure
        passed0/0/0 Approve; target-adapter-move-plan owns this accepted scope.
        Actual binding preflight passed review0/0/0 (binding-preflight.md): the
        cross-generation backing/pressure/safe-drop-wake lifecycle must close with the
        cold callers, guard and K/Layout. No Rust scope or binding acceptance;
        the full coupled freeze remains open, not an implemented route.
        The controlled-wake-contract.md passed conditional closure0/0/0:
        standard Wake/inert Drop, finite Driver defer exclusion, same-turn
        unlocked finalization and single-waiter notifier fault ordering.
        Actual holder/cold/guard/K/Layout/symbol freeze remains open; no Rust
        grant or full-P3 completion follows from protocol acceptance.
        Construction/feedback direction passed0/0/0 (binding-construction-phase-
        decision.md): same cold ready domain before Controller; preserve live/
        recovery inputs; do not wait for RESET under its own input exclusion.
        Only direction/document evidence is accepted. The follow-up Flow
        completion protocol passed0/0/0 (flow-completion-contract.md), after
        repairing source/Target absence handling for valid cold Open failure.
        Late commands need actual terminal disposition and real transit/drain
        closure. Closed carrier/owning-turn selection subsequently passed
        same-Reviewer0/0/0 (binding-turn-contract.md): actual Driver pairing/
        full LocalFlow, three existing-root turn modes and thirteen categories.
        This is selection/protocol only. Joint same-domain placement/cold
        returns, guard/real ports/expiry, work and physical Layout remain open.
        The single binding-joint-contract.md records expiry/guard and OPEN
        resident/admission-order choices reviewed0/0/0, not whole joint
        acceptance. One model classification precedes new F/O reservation;
        duplicates need neither. Metadata/test locality selection passed0/0/0:
        inward metadata/geometry, outer policy and12 Root tests, nine private
        metadata recipes. Finite test-operation/short-borrow/Wake lifetimes also
        passed0/0/0; exact Rust signatures are not compiled. ReadyGeometry value
        Interface and18-call/return facts also passed0/0/0. Closed Owner source
        correlation selection now passed0/0/0; origin and actual admission gate
        differ from SessionId/capacity. Cold-slot protocol also passed0/0/0
        after explicit AssociatedPreparing and active/startup holder repairs;
        Idle/Pending retains its cell lease, not a second runtime claim.
        Constructor ownership-form selection passed0/0/0 after uncancellable
        Cold P1 and falsifier P2 repairs: actual Root/build-failure custody,
        no self-borrow, P4 table or false Free. Pair/route selection passed
        0/0/0: checked inbox/Bound commands, consuming install, explicit nested
        Drop and indirect legacy RESET isolation; no shutdown or Rust claim.
        Initial-work/lane-poll protocol passed0/0/0: internal publication,
        live-source import probes and registration-or-real-publication; Due is
        not readiness/count. Actual shared owning operation/guarded consumer,
        global activation, complete Move Interfaces and build/confinement/exit
        proof remain open; fixture assembly cannot implement scheduler service.
        Real-port hypotheses subsequently failed review0/2/0: universal Running
        and Applied/Fatal predicate/rebuild shortcuts. Joint41–42's selected
        first output adoption uses the actual post-join whole Parts, not an
        old Fatal root. Written entry/retention review0/0/0 (9/9/8/8).
        Joint43–44 selects semantic S/checked G/D, A<=D eligibility and Installed
        commit; written closure0/0/0 after request/status P2 repair. D does not bound
        settlement/retention. Joint45–47 now selects whole-holder initial A /
        nested original C+B migration and normal no-ACK→real reporter→FIFO/
        slot/stamp→direct join ordering; written closure0/0/0 after one P2.
        Joint48–50 accepts the preparatory placement plan:15 complete impls
        below retained initial declarations, then initial under owner; two source
        routes, six effective scopes and one owner route exception are explicit.
        Written review0/0/0 (9/9/8/8); no Rust grant. Joint51 now consolidates
        the next order: Active real source→paired inbox→observed reduction→
        physical output, after its concrete binding/readiness/Layout/lifecycle
        and scope gate. Full P3 later OPEN/TCP/DNS/all-class/expiry/B obligations
        stay unchecked; P4+ is separate. Scope review0/0/0; no documentary GREEN
        counts as runtime. Subsequent user confirmation permits early P3
        whole-CurrentOnly residency/opaque shared service, not P4–P6. The real
        pre-opened fixture is implemented and reviewed0/0/0:744/219 test lines,
        6/6 plus10/10 repeats, provenance162/13, both governance6/6 and
        fmt/diff/all-feature check green. The next six-file production pure
        Move is also accepted0/0/0: construction parent/child3565/71 and
        completion parent/child10061/164, exact inverse bodies and ratchets,
        related159/159, Active repeats10/10, provenance162/13 and all gates.
        This changes placement, not Bound behavior. Test-only Prototype owning
        representation is selected; finite test operations and the concrete
        A1–A4 shared implementation/bound-route RED/GREEN remain unchecked.
        The private complete command-visit extraction is accepted0/0/0:
        Root857, unchanged56/56 plus five10/10 repeats and all gates green.
        It supplies the shared command operation, not inward placement/binding.
        One concrete A1–A4 packet is now written at joint51's end. First review
        0/3/0 required inactive activation, publication-failure/unwind and real
        Actor output-pressure wake repairs. Second review0/1/0 closed the first
        two; the outer-root exit repair now uses existing JoinSet completion
        and original result custody. Final review0/0/0 approves the finite Rust
        sequence; no implemented binding/test or full-P3 checkbox is promoted.
        Four cold-construction Moves now have default/all-feature275/275,
        exact integrated ratchets/all final gates and code review0/0/0 Approve.
        Actor fixture cleanup is now accepted0/0/0 with all final gates; the
        characterized Actor Move has since closed0/0/0 with all gates. Neither
        prerequisite completes the actual Bound route.
        Independently accepted: continuation16-test pure co-Move to
        transactions.rs407, parent728→330, default/all-feature before/after16/16,
        all final gates and same Reviewer0/0/0. Initial-acceptance is also
        accepted0/0/0: parent603/child574, both-mode post52/52 and final gates;
        actual source-guard domain preserved and old1168 entry removed.
        Physical attach is also accepted0/0/0: parent668/scenarios747, nine
        unchanged tests, both-mode post53/53 and final gates; old1407 removed.
        All three test co-Moves close, not actor repair or binding completion.
        Canonical geometry preparation also closes0/0/0: both-mode57/57,
        57×10 repeats and final gates; no formula/error-order change. Separate
        foreign Layout cold-return characterization is now user-authorized and
        accepted0/0/0: two real rejection cases, original ten unchanged, leaf448,
        both-mode Root59/59 plus59×10 and final gates. Limited scope/coverage P1
        closes. Separate inner-check removal now closes0/0/0: scheduler690->688,
        sole current caller's earlier checks/reserve dominate both deleted
        checks; exact cold cases, plan12, Root59 both modes,59×10/final gates
        pass. No other source changed; full A3 Layout remains open.
        All9 metadata-private recipes and the separate outward12-scenario
        Move close0/0/0. All49 original items, including12 tests and7 native
        Wake/Drop bodies, remain exact; real scenarios now have outer FQs.
        Route provenance exact RED->GREEN, outer12/12, both-mode Root59/59,
        59×10, Knife162/repository14 and final gates pass; no production or
        inventory change in that Move. Inner Layout removal and complete
        metadata placement have since closed0/0/0: owner_ready735/Root846,
        24 implementation items and six recipe files exact, seven old files
        removed; all outer59 tests/FQs unchanged. New AST route RED->GREEN,
        Root59 both modes/59×10, Knife162/repository15 and final gates pass.
        The subsequent independent command-Move freeze returns0/2/0: close
        actual admission/masks/quarantine and nominal/result placement, plus
        original pre-operation Full Box capture, within the same A1–A4 packet.
        Couple the complete command/35-test migration to real Current/Bound
        GREEN; no temporary production Prototype or new sidecar. The resumed
        goal's four Actor fixture repairs now close0/0/0: index15814/leaf136/
        arrival513, original body inverses, repeats and all final gates pass.
        That fixture packet left production unchanged. The subsequent pure
        Actor Move is accepted0/0/0: parent6583, children88/122, provenance507,
        both-mode24/24,80 repeats, Root59 both modes and all final gates.
        Active owning-fixture extraction is now accepted0/0/0:754/244 lines,
        both-mode CurrentOnly6/Root59, exact10 repeats and all final gates;
        original resource lifetimes/assertions survive, production unchanged.
        The user now authorized the same-packet Root bound_source test-route
        addition and complete seven-method Target retirement Move. That Move
        is now accepted0/0/0: parent9635/completion411/provenance591, exact
        inverses, both-mode Target74/74x10 and final gates/ratchet pass.
        The real Bound tracer and fixed Root test route are written.
        Original constructor extraction passed Active1/1 and CurrentOnly6/6.
        Cold custody and actual factory identity are source-reviewed0/0/0.
        Complete command/35-test inward migration, fresh empty pair and generic
        whole resident are written; same source Reviewer0/0/0, NOT Move GREEN.
        Actual consuming checked installation and original five-input failure
        return are now written with real backing/publishers/pair. Source review
        0/4/1 is causally repaired and re-reviewed0/0/0, not runtime acceptance.
        All18 checked callers retain their real domains and exact retry/cancel;
        remaining13-source review0/0/0. Only10 authorized test files changed.
        Metadata tests are unrun; exact Bound compile RED4 has only actual
        path references, no signature/privacy error. No runtime GREEN claim.
        Fmt/diff/standalone governance6/6 pass; Executor9635/C3536 unchanged.
        The total owning constructor-quarantine exit, actual Root build caller
        and consuming start are implemented. Inventory-classification repair
        source-closes0/0/0. Five-source selection and22 exact test-line migrations
        are source-reviewed0/0/0; sends stay two-lane. Runtime remains unrun.
        Whole-current adoption/activation is written and source-reviewed0/0/0;
        actual actor/FIFO/handoff/C/Flow identity precedes the final binding admit
        and reserved whole-root placement. Active/ENABLED classification repair
        source-closes0/0/0. Two new metadata tests remain unrun.
        Actor Output port/registration/producer edges and exact nursery retention
        are written in three existing files6583/130/223. Same compile4 and
        fmt/diff/governance6/6; same Reviewer SOURCE0/0/0. Poison remains and
        child/root errors reach the original nursery despite LocalShutdown's
        suppressed void-call record. Actual caller and poison/close/pressure
        lifecycle tracers remain required; no runtime acceptance follows.
        Target's seven-file implementation has conditional SOURCE0/0/0;
        new test unrun behind the same4 Bound compilation gaps. The user-authorized
        governance predicate/regression fix is reviewed0/0/0 and standalone7/7;
        governance752, all limits/inventories unchanged, authority blocker resolved.
        Production-mode compilation exposed a test-only current_leg call;
        source_factory176 now uses original phase/generation+full contract checks,
        same-Reviewer SOURCE0/0/0. Non-test library retains2 physical service gaps.
        Metadata publication admission/finish now SOURCE-reviewed0/0/0 in
        ready1041/flow755/test-flow591; five new tracers remain unrun. Default/
        all-feature lib and Cargo governance retain2 real physical gaps, lib-test4;
        fmt/diff/standalone7/7. Actual Root must store Completed before Notify,
        retain original ready.finish after later fault, and own quarantine before
        closing an interrupted interval. Complete real Target/Actor selected
        receipt/service/output and its runtime evidence. Selected inbox/TCP
        primitives are SOURCE-reviewed0/0/0 (session938/driver167/new test470),
        with three real-port tests written but unrun; fmt/diff/standalone7/7.
        Owner-only DATA scope is authorized and implemented, SOURCE-reviewed0/0/0:
        original staging/Replay/map custody, error/unwind poison and Client/legacy
        preservation. Supervisor5597/child337/test207; Integrator ratchet5597,
        Main standalone7/7. Three real-Owner tests remain unrun; initial Vec
        capacity and complete Bound C/Root integration still need acceptance.
        C selected receipt consumer is SOURCE0/0/0: Held-before-clock, original
        observed reduction/ordinary admission and real Flow pressure retention;
        active validation is distinct from cold binding. Executor9634/service367,
        C3536/service140, flow830/continuation1037; standalone7/7, fmt/diff.
        Selected actual Driver producer now SOURCE0/0/0: indexed original Flow,
        checked transit/Held/full LocalFlow, separate pull/transfer and preserved
        Full/observer; Retiring due stays retained but ineligible for ordinary
        Driver. Ready1059/flow1028/test648, commands694/bound tests693/old leaf771.
        Four added tests remain unrun behind old4; standalone7/7 and fmt/diff.
        Implement C guarded source caller, real Target/category/read-RESET
        cutover and physical/Root completion next; do not check P3.
        Preserve
        actual claim/completion on Current quarantine failure. Cold regression
        remains mandatory once the crate compiles; do not substitute metadata
        or source review for runtime acceptance. Command coupling and real
        Bound service remain unchecked, without another design phase.
        Exact field/
        phase placement, producer closure and bounded decision/expiry work
        remain before callable expiry; no P4 table or fake initial cold seed.
        Pre-join and both general P1s remain open. Actor-input retry/eligibility,
        then ports/consumer/masks/work/Layout remain open. Old model live-F rejection
        is not converted to held retry. CurrentOnly service scope is now
        authorized; binding/activation is written, not runtime accepted; P6 is not granted.
        Lane receive, Phase A and conditional Phase B remain accepted. Persistent
        capacity registration also passes: session896/progress480, test parent88/
        leaf344; root35/35 over ten rounds and full regressions/gates green,
        same Reviewer0/0/0 Approve. Root405, prior B leaf771, Driver148,
        scheduler51/Control604/tcp2883/client wrapper/ratchet stay unchanged.
        The pure SessionCommands Rename is implementation-accepted0/0/0;
        four inverse hashes and the same full gates pass, no behavior/size change.
        The process-only arithmetic prerequisite is now accepted0/0/0:
        nine exact tests, root44/44 and full scoped gates; root504/test parent89/
        new leaf115, existing bodies/other sources/governance unchanged.
        The selected-send Extract Method is now accepted0/0/0: root508,
        exact four-line inverse and full scoped gates; no ready behavior.
        Actual-ready design's two P1s closed0/0/0. Its first real dispatch
        tracer now passes exact+ten repeats and full scoped gates; root823,
        scheduler598/route90/leaf261 are independently accepted0/0/0. This is not
        full readiness/lifecycle/fairness acceptance. The later §7 claim/finish
        root-fatal mapping repair is also accepted0/0/0: real second-session-
        send RED, exact+ten repeats/root46/full scoped gates GREEN;
        root825/leaf333, other Rust/governance unchanged. Publisher-fatal and
        runtime finish-error retention remain separate obligations. The next
        remaining-Full re-registration test is independently accepted0/0/0:
        leaf528, root47 and exact+ten/full scoped gates, no production
        change. Publisher-failure design and pure test Move are accepted0/0/0;
        first actual revision-overflow RED→GREEN passes exact+ten/root48/full
        scoped gates. Quarantine's check-before-lock review P1 is repaired with
        repeated green gates; re-review0/0/0 Approve. Other fatal branches stay open.
        The separate process-revision characterization is now directly GREEN:
        leaf215, exact+ten/root49/full scoped gates; independent review0/0/0 Approve,
        no production change. The separately approved test-only fixture extraction
        is accepted0/0/0 at209 lines, both tests/root49/all scoped gates. Slot-poison
        publication is directly GREEN at288 lines: exact+ten/root50/all scoped
        gates, same Reviewer0/0/0 Approve. The original209-line fixture prefix
        and other Rust remain exact. Source-unwind quarantine is directly GREEN
        at446 lines: exact+ten/root51/all scoped gates, same Reviewer0/0/0 Approve. Its real
        mailbox receipt is not duplicated by Unknown; original claim remains held.
        Ordered first-cause retention is directly GREEN at575 lines: exact+ten/
        root52/all scoped gates, same Reviewer0/0/0 Approve; this is not
        concurrent-race proof. Admitted-work completion is directly GREEN at743:
        exact+ten/root53/all scoped gates, same Reviewer0/0/0 Approve; actual finish accounting
        is checked, not merely mailbox enqueue. No production change.
        Notify-unwind is directly GREEN: actual Pending, fault/unlock before
        callback panic, same-root re-entry and one prefetched receipt; exact+ten/
        root54/all scoped gates pass. Parent745/new child175; same Reviewer0/0/0
        Approve. Healthy source-unknown isolation now directly GREENs: gate Open,
        only A quarantined, actual B send succeeds, A Unknown/real receipts retained.
        Exact+ten/root55/all scoped gates pass; parent746/new child255, same Reviewer
        0/0/0 Approve. Re-park/fatal is directly GREEN: real old-Waker Drop/dequeue
        inside root poll produces Rescan, then sticky stop/exact B retention;
        faulted park_epoch rejects without dirty-epoch mutation. Exact+ten/root56/
        all scoped gates pass, parent747/new child217; same Reviewer0/0/0 Approve.
        Independent final epoch/gate, first registration/full park/concurrency
        remain open. Actual Executor source service is the selected next direction.
        Preparatory read Move is accepted at parent10965/new164 with exact inverse,
        all scoped gates and Integrator-only ratchet; same-Reviewer0/0/0 Approve.
        Persistent association,
        exclusive routing and the full two-lane ledger item remain open.
        The 2026-09-07 P3 result owns the full acceptance
        ledger; this parent stays unchecked.
      - [ ] Implement and independently accept the production root, caller,
        bounded pre-auth custody/table, TCP Target Adapter, and whole-root
        settlement. UDP retains its separate architecture gate.
  - [x] Construct `OwnerPhysicalSessionRoot<T>::CurrentOnly` by consuming exactly
    one `QuinnOwnerInitialPhysicalRootSeed<T>`, exact-returning it before commit,
    recording restart credit as `NotInstalled`, and owning structural current-A
    living-root shutdown. This closes only the private necessary-only
    constructor/A/B/error subgate; it does not close Case C, production cutover,
    terminal/no-ACK settlement, or whole-root exact zero.
  - [ ] Repair the independent actor fixture debt only in a later separate
    behavior-preserving slice: move the exact ordered-delivery fixture to
    `join_fifo_inventory.rs`, promote any shrink through the Integrator, and
    settle the real `LocalCancelled` terminal plus FIFO ownership. Do not mix
    its `ResidualOwnership { messages: 1, bytes: 0 }` with direct-join repair.
  - [ ] Absorb and cut over the existing initial-A terminal/join/shutdown
    Authority only after ultimate root destruction has a longer-lived unique
    custodian; then connect the private non-copyable committed outcome to
    production terminal preservation.
  - [ ] Close hard, cancellation, and shutdown through one
    CurrentOnly-root-owned no-ACK transaction, including actor joins and
    controller/output high-water/stamp finalization before whole-root exact
    zero; unknown unwind stays sticky nonzero or exact known inventory.
  - [ ] Add authenticated B topology without ungating the test-only B adapter;
    only after root ownership converges may runtime restart credit be installed.
  - [ ] R7a: a reverse-only A DATA/ACK blackout may leave A's probe healthy;
    without an authenticated B hint the client does not switch.
  - [ ] R7b: reducer-owned Target-to-client application-ACK age causes one B
    hint, one client attach, exact replay, and first B application ACK inside the
    combined horizon.
  - [ ] R7c: wrong seal/session/generation/nonce/sequence/direction/cause/offset,
    idle flow, or stale B health cannot switch; duplicate/lower hints cannot
    start a second attach.
  - [ ] R7d: simultaneous hard failure, local stall, and hint consume one switch
    epoch and one nonce and commit at most one generation.
  - [ ] R8: deterministic fault/fairness matrix.
  - [ ] R9: real-smoltcp replacement parity and production-TUN readiness.
- [ ] Task 5: bounded UDP two-leg delivery/path switching.
- [ ] Task 6: single-owner server/upstreamd and Target ownership.
- [ ] Task 7: local real-socket `>170 Mbit/s` capacity gate after reachability math.
- [ ] Task 8: security/protocol review and release hardening.
- [ ] Task 9: bounded qualification over two independent ingress paths.
- [ ] Task 10: long macOS/M2 acceptance only after all prerequisites pass.

## Historical R7 acceptance checklist

- [x] Write the bounded R7 spec/plan before production edits.
- [x] Inventory exact actor read -> queue -> publication -> controller path.
- [x] Define the same-clock timestamp type, checked conversion, and ownership.
- [x] Add deterministic before/equal/after-deadline tests before accepting policy.
- [x] Prove the value-level R7c rejection kernel is snapshot-inert and cannot
  mint a first/second attach. Physical B provenance/composition remains open.
- [x] Prove the value-level arbiter consumes one epoch/nonce and commits at most
  one generation for all trigger orderings. Production routing remains open.
- [ ] Prove queue, bytes, ACK, replay, timers, actor, endpoint, and session
  ownership return to exact zero for the complete physical R7 composition on
  success, rejection, pressure, fatal, cancellation, and shutdown.
- [x] Run proportionate focused/repeat, broad local, provenance, release,
  current-baseline Clippy, rustdoc, vendored, fmt/diff, and secret gates for the
  prerequisite/policy-kernel checkpoint.
- [x] Complete independent P0/P1/P2 review and checkpoint result/status/learnings.
- [ ] Repeat the full gate after physical owner/B/hint/arbiter composition is
  production-reachable and close R7a-d end to end.

## Standing stop rules

- The public TUN entry remains legacy.
- R7 prerequisite/policy-kernel acceptance is necessary-only; do not claim
  physical R7a-d, Task 4, throughput, or M2 completion.
- Do not run macOS TUN, VPS, WAN, throughput, or M2 yet.
- If capacity math, reachability, or a deterministic seam is missing, stop and
  deepen the architecture instead of tuning a parameter.
- After a failed repair test, analyze and propose the next plan, then wait for
  user confirmation.

## Repository-maintenance backlog

- [x] Split auto-loaded project memory into compact current files plus exact
  read-on-demand archives.
- [x] Establish the two-tier Rust test-layout rule: private unit-test bodies live
  under `src/<module>/tests/`, root `tests/` owns integration/public-contract and
  repository-provenance tests, and the exact legacy inline-Module allowlist is
  shrink-only.
- [x] Install fail-closed code-growth governance for all future Rust changes:
  `1,500` production lines, `800` test-leaf lines, `300` test-index lines,
  exact shrink-only legacy debt, and no new `clippy::too_many_arguments`
  suppressions. Cover positive-cfg external test Modules, close every `#[path]`
  target inside the governed inventory, and reject source `include!` or broad
  `clippy::all`/`allow(warnings)` bypasses. Keep Module Depth and Authority
  Locality as mandatory review gates rather than treating line count as a
  splitting algorithm.
- [ ] Reduce grandfathered production/test debt only in later coherent,
  behavior-preserving stages. Do not combine mechanical movement with behavior
  changes or split one success/rejection/pressure/fatal/cancel/shutdown
  transaction across Authorities merely to lower a line count.
- [ ] In a separate behavior-preserving stage, extract oversized embedded Rust
  test modules behind stable module boundaries. Do not combine that mechanical
  move with R7 behavior changes; preserve provenance sentinels and `include_str!`
  paths, and rerun the full affected test gates.
  - [x] Move the `quinn_leg_actor.rs` inline test Implementation into the private
    `quinn_leg_actor/tests/mod.rs` child Module while preserving all `106` test
    identities and the production prefix.
  - [x] Move the `client_tun.rs` inline test Implementation into the private
    `client_tun/tests/mod.rs` child Module while preserving all `338` test
    identities and the production prefix.
  - [x] Move the `tuic.rs` inline test Implementation into the private
    `tuic/tests/mod.rs` child Module while preserving all `162` test identities,
    the production prefix, and the separate direct-probe Module.
  - [x] Move the `owned_upstream/tcp.rs` inline test Implementation into the
    private `owned_upstream/tcp/tests/mod.rs` child Module while preserving all
    `65` test identities, the production prefix, and its three parent-local
    test-support helpers.
  - [x] Move the `owned_upstream/two_leg_harness.rs` inline test Implementation
    into the private `owned_upstream/two_leg_harness/tests/mod.rs` child Module
    while preserving all `9` test identities and the normalized extracted
    body. The parent file's separately tested R7 controller-API adaptation
    remains disclosed rather than folded into a whole-prefix identity claim.

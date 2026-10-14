# mini_vpn Handoff

Last refreshed: 2026-10-14.

2026-10-09 用户范围修订：N3 自动节点切换移至[后续待办](TODO.md)；启动失败由用户自行选节点。规划顺序为 N1 → N2 → N4 → N5（不新增执行授权）；保留所选节点自动重连，接受恢复新通信而不保证原应用连接无缝续传。当前 N1 进度不变。N4/N5 的所选节点本地组合验收与深圳↔US真实验收范围已同步确认，详见[执行指南§5](docs/tech/2026-09-14-compatibility-recovery-execution-guide.md#5-唯一剩余工作清单)；自动 A/B 切换/回切矩阵归延期 N3。

2026-10-13 已批准并经独立审查的分层恢复修订生效：先修接入丢包，再贯通 Generic 同节点重连与服务owner下的受控会话重建，随后补齐family/S1和完整本地验收。旧资源未结算/未知债/Q禁止后继，最多一份活会话；120s失败边界不放宽原测试期限。唯一执行清单为 docs/current/n1-f1-implementation.md，窄范围契约为 docs/current/n1-session-rebuild-contract-20261013.md；其余禁止项不变。

## Current: N1 closed auxiliary recovery accepted; full Step B remains open

Current result: docs/current/n1-20260918-evidence/f1-aux-recovery-checkpoint-20261014.json. Actual legacy/staged Generic opens recover a closed auxiliary through one saved NodeHost transaction: original slot/preparation/lease custody, fixed first15s/at most2 attempts/at least500ms, whole-generation publication, actual predecessor/stats retirement and sticky failure/stop accounting. Typed configuration rejection stops retries within this NodeHost. Independent /root/n1_aux_final_review P0/P1/P2=0/0/0 accepts ClosedAuxiliaryRecovery only.

Final v3 source:576 total/178 vendor/398 governed, SHA2d16b0d0981962c24a7592617e306cd9432763aaf795fe1acb29fcdc5d7c63d1;8 existing changed+2 new,566 unchanged,0 deleted. All216 test executions on that final source pass (not necessarily unique tests), plus all-features lib/bin check, scopedfmt and diff. HEAD/index/locks/certs/device/harness/r8 guards remain unchanged. All REDs, rejected TLS retry oracle and v2 startup-cleanup regression remain in the journal.

Next: complete primary/UDP generation custody and healthy auxiliary replacement in the same root, preserving C=N+3 and original predecessor obligations. Then migrate four real production constructors with continuous saved-business/node driving and Failover health stop/join, followed by Generic same-node service/session rebuild. Production constructors are still legacy; full Step B/NodeSettled/Generic recovery are not accepted. Families/S1/fullW/F/cold/public/fullN1-L/R remain mandatory. Single Writer, independent Reviewer and evidence-based repair authority continue without renewed user approval. No commit/push/destructive cleanup/external/TUN/VPS/WAN/throughput/M2/P3/N2–N5.

### Ready-to-copy continuation at the accepted closed-auxiliary boundary

Continue authorized N1 Step B in /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz. Cold read AGENTS→Rules→STATUS→HANDOFF/TODO→n1-f1-implementation.md, n1-session-rebuild-contract-20261013.md and f1-aux-recovery-checkpoint-20261014.json. Root is sole Rust Writer; final independent /root/n1_aux_final_review accepted ClosedAuxiliaryRecovery only,0/0/0. Final576/178/398 source2d16b0d0…c63d1; manifest f1-aux-recovery-final-v3-sources-20261014.json SHAc93a2814…8a156. HEAD64f027fc/index978 SHA01816/cached0; no Cargo/TLS residue. Final216 executions plus check/fmt/growth/provenance passed on v3; preserve every RED and rejected oracle.

Actual closed auxiliary legacy/staged reconnect, candidate/predecessor/original permits/lease/stats custody and canceled run observation now work under one saved NodeHost. First ticket is registered under actual slot guard; root cannot handshake before original preparation/full lease return. Typed TLS/config rejection is sticky per NodeHost. Initial unpublished startup must capture/validate current generations before physical retirement; only published recovery candidates use early retirement. Never infer logical settlement from physical witness or numeric zero after invalid inventory.

Next mandatory B2 seam is primary/UDP producer, queued Vec payload/partial/children generation custody plus healthy degraded/probe replacement. Start with exact callers/owner and code/test capacity, independent review and one real falsifiable tracer; keep original C=N+3,15s/2 attempts/500ms. Current node978/recovery747/maintenance635/runtime887/staged800/startup402; recovery tests744/800, running652/startup tests612. Do not grow test leaf beyond800 or split a transaction merely for length. All four constructors remain legacy: TUN Tuic/Failover, main probe, direct probe. Migrate only after root recovery closes all enabled routes; drive saved business and node continuously, own actual Failover health stop/join. Then service/session successor preserves one TUN/pump/fake-IP namespace/clock, propagates configuration rejection and forbids successors under old debt/Q/Incomplete. No replay of unknown TCP or expired UDP. Family/S1/fullW/F/cold/public/fullN1 remain mandatory. No commit/push/destructive cleanup/external/TUN/VPS/WAN/throughput/M2/P3/N2–N5. Existing authority covers concrete diagnosed/reviewed repairs without repeat confirmation; preserve repair-test stop evidence. No user input/email pending.

## Historical accepted fill observation boundary

继续唯一目录 /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz 的N1，goal ACTIVE。读AGENTS/Rules/STATUS、n1-f1-implementation.md、f1-fill-observation-checkpoint-20261012.json；census前置见其previousAccepted。554/176/378源SHAb2f65bf449bc4798b81725b7bad75256adc00a4fb9621121930f563e428a0bcb，Reviewer /root/n1_ingress_review最终0/0/0。全部Cargo终态、TLS恢复、无并行Writer；HEAD64f027fc/index978/cache0/dirty173/root10845/harness4099/device1179/r8stop保持。

下一finite(2)沿用五文件map：open_custody304≤310（child seam），新真实ACKrounds叶≤750，open_fixture469≤500（UDP-aware接收），transaction/tests/observation190≤250（原Waiting身份/期限/attempt），receipt_gate170≤235（exact-id gate，不调用仅容一个sibling的旧helper）。A真实2MiB零窗积压、B健康TCP/DNS/nativePacket、C实际3join/provider/receipt；original128Waiting的身份/期限和factory唯一性跨≥2真实ACK轮次保持，正常/实际坏receipt双变体。原20s/4s/15s不变。先确切scope/预算review再Rust；有限(2)尚未实现。

生产partial-write全环限制、旧组合时序失败保持，不能把测试投影称production调度修复。完整剩余清单不减，全部禁止项保持；无邮件/用户输入待办，不重复例行申请。

## Historical N1 evidence and superseded continuation boundaries

The records below preserve evidence and past scope decisions. They do not extend the current action; the executable continuation is the current section above.

S7b 已局部接受。两个独立只读 Reviewer 均为 P0/P1/P2=0/0/0：`RelayEventLane` 在任何分支副作用前只接受当前 `SocketCtx` 的精确 epoch；实际 bounded bridge 的 2×4 tracer 覆盖旧 epoch/缺失 context × `Data`、`DataReady`、`AckDrainHint`、`Closed`，并锁定 D4、context/read-credit、dirty、gap 与 custody。治理 7/7、精确 tracer 1/1、三条 helper 回归、scoped rustfmt 和 diff 均 GREEN；唯一 ratchet 已在用户确认后由 15351 降至现有 15312。证据见 `l1e-s7b-final-independent-review.json`，RED、治理 stop 与 GREEN 均保留。

S8 已局部接受：两位独立最终 Reviewer 均为 P0/P1/P2=0/0/0。两处测试 Rust 改动使同一 private fixture 在构造 saved session 前设置 `cheap=true`，并以 129 条真实 TCP socket 锁定 W128、原始 4096B、完成但未被原 loop 消费的 slot、原 handle/new epoch 重新准入、observer cancellation 与 cleanup 前 shutdown join。精确 tracer、两条既有 W 回归、治理 7/7 与 scoped fmt/diff 均 GREEN；首次 E0277 compile stop、用户确认的仅诊断输出修复和重跑 GREEN 全部保留。动态 tracer 只证明完成 worker 在原 loop 下次消费前仍被 W 持有；handler→`consumed` 是同一 select arm 的源码顺序。证据：`l1e-s8-final-independent-review.json`、`l1e-s8-writer-green.json`、`l1e-s8-cheap-w128-unexpected-stop.json`。下一步只做 F4096/five-role flow custody 的结构收敛和容量预案及独立审查；不自动写 F/S1 Rust 或进入外部 gate。

revision 3/4 的审查与发现均已保留。revision 5（SHA `a1caad5e…adeb84`）的三项独立复审均为 P0/P1/P2=0/0/0，用户已批准该范围。L0 只做 listener registry/test 的 pure Move、characterization 与治理来源证明；F4096 admission、4352 slot、RX/TX 限额、fixture/RED-GREEN、flow custody 和外部 gate 尚未开始。

L0 已本地接受，最终独立治理 Reviewer 为 P0/P1/P2=0/0/0，证据为 `l1e-f4096-revision-5-l0-final-independent-review.json`。baseline green 后，迁移的 characterizer 以 `E0425` 停止；用户确认后仅将 `trace_enabled` 加入 child 的既有 `super` import，同一条 characterizer 以 1/1 green 完成。Integrator 随后先在 provenance 579/575 停止，revision-1 压缩图被拒；revision-2 经独立审查后，治理 7/7 与 provenance 20/20 都 green。历史记录中的 467 是排除 `target` 后的仓库 Rust 文件总数；增长治理实际排除 `third_party` 的 173 份，当前 governed inventory 为 294。该术语更正及独立 P0/P1/P2=0/0/0 接受分别见 `l1e-f4096-revision-5-source-count-terminology-addendum.json` 与 `l1e-f4096-revision-5-source-count-terminology-independent-review.json`，不推翻 L0 行为或 7/7 结果。L1 写前映射（SHA `9002f723…ab6f3`）已由两位独立 Reviewer 以 P0/P1/P2=0/0/0 接受，L1a 预检 `l1e-f4096-revision-5-l1a-preflight.json` 已冻结 HEAD、162 条脏记录、467/173/294 三个计数、预算、hash 与无 Cargo/rustc 进程。当时的下一项是顺序运行五条既有 baseline characterizer；该限制已完成，并由本文件顶部的 F1 Writer-map 门禁取代。

L1a 的五条冻结 baseline 均为 1/1 green，源 hash、HEAD、162 条脏记录与 `git diff --check` 保持不变，见 `l1e-f4096-revision-5-l1a-baseline-green.json`。当时只允许将四个配置/解析符号从 `src/client_tun.rs` 移到现有 `listener_registry.rs`，保留根 public re-export 与 private imports；不改测试、runtime、profile/cap/validator/allocation 或治理文件。该范围已完成，现由本文件顶部的 F1 Writer-map 门禁取代。

L1a、L1b 与 L1c RED 证据均保留。用户确认后，遗留 `ensure_spare_listeners_respects_global_cap` 保持身份，预期从历史 2→3 partial expansion 更新为零部分扩容（2），并更正 abort 槽的 Closed 计数注释。该 characterizer 与三个 direct tracer 均为 1/1 green；最终测试范围独立复核 P0/P1/P2=0/0/0，唯一 P1 已关闭。容量 preflight 仅是 F4096 前置条件。S0-G map 已由独立 Reviewer 以 P0/P1/P2=0/0/0 接受。

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

## Historical ready-to-copy continuation at accepted revision-5 L1a preflight boundary

~~~text
继续 N1：标准 TUIC 兼容恢复。唯一目录：
/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz
按 AGENTS 冷启动顺序读取 Rules、STATUS、HANDOFF/TODO、authority.json、revision-5 proposal、L0 baseline/compile-stop/repair/Integrator evidence、L1 prewrite map/review 与相关当前 learnings/errors；保留全部失败证据。
revision-5 范围已获用户批准。L0 的 pure Move 已完成：`listener_registry.rs` 与 private listener-capacity test leaf 已加入，`client_tun.rs`/tests parent debt 降至 15122/13759；移动后的 characterizer 首先以 E0425 停止，用户确认的唯一 `trace_enabled` import 修复后同一命令 1/1 green。Integrator 的 provenance 579/575 stop、revision-1 rejected map、revision-2 approved map、治理 7/7 与 provenance 20/20 green 全部保留；467 是排除 target 的仓库 Rust 总数，增长治理实际 inventory 为 294，third_party 排除 173，详见 source-count terminology addendum；route count 341 已核对。
L0 已获本地接受：`l1e-f4096-revision-5-l0-final-independent-review.json` 记录最终治理 P0/P1/P2=0/0/0；两个历史文档 findings 仍保存在 `l1e-f4096-revision-5-l0-final-governance-document-stop.json` 与 `l1e-f4096-revision-5-l0-document-reconciliation-review-1-findings.json`。L1 map（SHA `9002f723…ab6f3`）及 `l1e-f4096-revision-5-l1-prewrite-map-independent-review.json` 已接受，source-count terminology addendum 也已由独立 Reviewer 以 P0/P1/P2=0/0/0 接受。现在先做新的 Writer/Git/source/process 预检，再且仅再跑五条冻结 L1a baseline，随后仅移动配置。预检前不得 Rust/Cargo；L1b selector/observation/RED、validator/cap/atomic、flow custody、完整 N1/TUIC、P3、N2-N5、生产切换和外部验收仍禁止。
更新：L1a 已由 `l1e-f4096-revision-5-l1a-final-acceptance.json` 本地接受，L1b 命名 RED 与 L1c 两条 direct `with_max_total` tracer 均已独立复核为 P0/P1/P2=0/0/0。L1c 的部分写入证据见 `l1e-f4096-revision-5-l1c-atomicity-reds-independent-review.json`；现在等用户确认 map 的一个 coherent Green repair，不能自行 Green/flow/治理/外部门。
禁止 commit/push、reset/clean/stash、P3/N2-N5、外部/TUN/VPS/WAN/吞吐/M2和完成邮件；保留全部脏工作与失败证据。
~~~

## Historical S6 proposal-boundary continuation (superseded)

~~~text
继续已授权 N1：标准 TUIC 兼容恢复。唯一目录：
/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz
按 AGENTS 冷启动顺序读取 Rules、STATUS、HANDOFF/TODO、指南§5、N1结果最新R4/R5、authority.json、l1e-r5-s6-movelayout-proposal.json、其独立review及相关当前 learnings。保留所有 R4/R5 失败与停止证据。
当前 HEAD64f027fc、978 index entries、461 Rust 源；无 Rust/Cargo Writer。R4局部接受和原 N1/S1/S2/S4/S5/条件S3授权保持，但 S6-MoveLayout 尚未获用户明确确认，不能写 Rust 或运行 Cargo/测试。
S6 已获独立 proposal review P0/P1/P2=0/0/0，proposal SHA ecd8f07a5bfefb94dea39d6c5b027221cfa5586db74007a61a22076c67951066。它只请求三个新路径：src/client_tun/legacy_event_loop/mod.rs、src/client_tun/legacy_event_loop/relay_events.rs、src/client_tun/tests/compatibility_open_fixture.rs；现有写入只限 src/client_tun.rs 与 compatibility_open_custody.rs。
确认后，只允许单 Writer 先迁移 shared fixture（父约276–300，新叶约420–460）并完整 green-to-green，再迁移 event lane（root净缩约800–900；child约1050–1300；mod约20–60）。run_borrowed_event_loop 仍唯一 owner；它在栈上持有 lane，lane 仅有 global_rx、handle-only D4、gap state、pause bit。root 保留 select、Sleep、sender、socket/TUN/dirty 和所有生命周期。
S6 不包括 bridge、stale tracer/repair、cheap W128、FIN、reap、DNS/UDP、公平、完整 W、S3、F/S1、cold owner 或 production cutover。任何非预期失败立即归档并等待具体 repair 确认。Writer冻结后才允许 Integrator 仅下调 client_tun.rs 的既有治理数字。
禁止 commit/push、reset/clean/stash、P3/N2–N5、外部/TUN/VPS/WAN/吞吐/M2和完成邮件；保留全部脏工作和失败证据。
~~~

下方 R4 复制块同为历史边界；当前接续以文件顶部的 F1 capacity-allocation boundary 为准。

## Historical ready-to-copy continuation at accepted R4 boundary

```text
继续N1：标准TUIC兼容恢复。唯一目录：
/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz
按AGENTS冷启动顺序读STATUS、指南§5、N1结果最新F4/R4、authority.json及原失败/精确提案/最终独立review。
R4已获用户确认并已局部接受：l1e-r4-final-independent-review.json记录P0/P1/P2=0/0/0；四个批准命令为1+1+1+11共14次通过、0失败。唯一Rust改动是settlement叶787→794（+7，≤800），461源中其余460不变；HEAD64f027fc/978stage冻结。原N1/S1/S2/S4/S5和条件S3继续有效，不重新申请已有范围。
失败log96d1ba092dc1ca328e2daa8bf4deee9a94f401e0e5643b6ad73688873ebaa90e、v2失败快照、提案及stop verification必须保留。R4只证明actualWorker0/deviceReady之后仍须推进同一saved outer driver到DeviceFailedSettled，并不抹除原失败或建立生产缺陷。
唯一Writer先做只读压力余项完整容量/可达性计划；独立Reviewer预审后才可开始一个冻结且≤800的下一测试包。计划必须共同覆盖cheaptrue actual W128、两个FIN路径、stale Closed/AckDrainHint、actual reap、W128下DNS/UDP回返与不相关pending TCP公平、32 control/每turn≤1 Connect、以及真实stop/join cleanup。若完整方案不能留在已批准pressure叶和共享fixture容量内，停止并提出最小范围修订，绝不压缩断言或擅自转移路径。
父16166（治理仍16192）/runtime1279/relay754/open693/settlement794/pressure409；不能超1500/800。相关治理仅在整pre-F W冻结后由Integrator独占执行。S3须完整pre-F W/review/related/growth绿与容量重算；F+colddesign审计1454–1754仅runtime1275时点估计，实际W/S3后且首次F行为前必须关闭1500风险。真实W→F、Native lease/五角色实际join、健康已安装TCP公平及cold owner先于async启动仍为N1必需。
禁止commit/push、外部/TUN/VPS/WAN/吞吐/M2、P3/N2–N5、完成邮件；保全部dirty与失败。不将局部tests当完整W或TUIC恢复。
```

下方 L1d 复制块为历史；当前接续以文件顶部的 F1 capacity-allocation boundary 为准。

## Historical ready-to-copy continuation at the accepted L1d boundary

```text
继续已授权 N1：标准 TUIC 兼容恢复。唯一工作目录：
/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz
按 AGENTS 冷启动顺序读取 Rules、STATUS、HANDOFF/TODO，然后读指南§5、N0唯一N1卡A–D、N1结果最新L1d结论、authority.json及相关当前learnings。
N1/S1沿用既有授权，无需重新申请。M0/M1/M2、L0、L1a、L1b+R2、L1c、L1d0、L1d均为各自局部独立0/0/0；完整N1-L/R未完成。
先核对458份Rust及HEAD64f027fc2fe3e123bcc8dafc7c482cd286896f4f/978stage和并行Writer状态。
最终manifest：docs/current/n1-20260918-evidence/l1d-source-final.json，SHA256 5a0becfac3b31089336c8f84efc91b895de2ef3e4cbd55a6ac41d332abbc805b。
l1d-validation-summary.json、l1d-independent-review.json、l1d-writer-handoff.json和原gates/log拥有证据：当前最终591通过；累计694（非唯一数），2接口compile RED/3行为RED，零非预期失败；L1d0另529通过。
Writer已冻结，无Cargo。上一Writer /root/n1_writer；新独立Reviewer /root/n1_reviewer；历史 /root/wip_audit不可冒用。先核对代理，再保持一个Rust Writer，Reviewer只读，Integrator独占治理。
私有compatibility_runtime::ClientTunSession持久保存外层future和device；run_borrowed_event_loop借用原device，取消run/observe_shutdown不重建。poll/drop panic、device实际join、有界首错/delta、固定15s deadline与unknown非零保留已有10项tracer；DeviceSettled不代表全session clean。
生产startup/main/probe/harness尚未接session，冷owner尚未早于TUN/pump/connect。不要启用半套owner；不要复制DATA loop或创建第二control authority。
下一可执行门：在同一root实现W128完整open custody与F4096五角色custody；读l1d-custody-readmap.json并以当前符号重新定位。原channel128不是worker上限；process_listener_activity必须先准入再extract_socket_payload，满时原smoltcp recv bytes保留。W涵盖等待/worker/结果/late cleanup。
首候选tracer open_admission_retains_socket_bytes_until_custody_available：真实session/原loop/内存TunIo，128个受控pending opens，129th payload留原socket，无额外worker/copy；结果未消费/late cleanup继续占W。此pending故障fixture不冒充TUIC恢复。
spawn_remote_relay四类supervisor不能再独占可取消栈中的child handles。预留F/角色及完成可失败准备后，同步spawn→登记唯一handle→发布；顶任务panic/stop仍保留孩子，全部实际join才归还F，四类engine完整处置仍必测。
S1已允许NativeTcpRelayStream不可克隆、未启动prepared pump载荷及8处构造适配；TUIC prepare，session登记第五ordered-pump角色后发布，真实TcpPoolSlotLease保留到pump join。四处detached self-wake仍待改reader-owned Sleep，三个上游trait签名不改。
先做真实行数/测试容量预算：runtime668/1500、custody leaf741/800、compatibility_lifecycle707/800、父16192 shrink-only。获准test路径见N0卡C，不能自行加leaf/allow、删空白凑行、砍矩阵或拆散一条settlement；若确认无法闭合，给出最小明确scope修订，而非重问N1/S1授权。
单Writer每个可编译边界走预声明TDD、独立review、原两模式相关回归及501治理。原341来源及新增10个私有Tokio test身份不可丢，syn gate识别tokio属性。真实repair-test失败立即停，保留源/日志，先证据+具体方案，用户确认后才下一次修复。
保留全部dirty与失败证据；禁止commit/push、reset/clean/stash/add-A、P3行为、N2–N5、外部/TUN/VPS/WAN/吞吐/增强M2及完成邮件。原P3/Case C/R7/Task4义务不减。
```

## Historical P3 handoff — pre-N0 position, frozen during current N1 F1 stage

The material below is retained only as provenance and cannot authorize P3.

### Historical immediate P3 handoff

Canonical executable checklist: joint contract section51. Full P3 incomplete.
Latest accepted:51O production Root cold aggregate, same Heisenberg final
SOURCE0/0/0 Approve; local Module/Clean/System/DDIA9/9/9/9. Actual generic Root,
resident[N], Ready slots/four trees/directories/two Arc allocations and one
OwnedNotified are composed before the first reserve. Exact backing uses the
existing allocator with proven-capacity single pushes. No new unsafe or hot-path
audit. Vec Reserve remains recoverable; Arc/Box abort policy is unchanged.

Actual cold tracer safely proves individual-valid/aggregate-invalid Layout
before allocation, then injected Reserve/full-plan return, then small successful
construction and same-backing start. Three native tests lock concrete metadata
capacities/geometry, InvalidResident precedence and the separate effect fault
domain. Expected missing-layout compile RED and one test-only E0624/private
helper correction are preserved in sequence.json; no natural OOM is claimed.

449 source hashes/six changed paths/443 unchanged, final drift0. Ready1167,
flow1130, Root construction300, process_plan494, Ready test index127/new leaf109.
No oversized growth/shrink or governance edit.22 final gates and40 module repeats/
1080 executed tests pass in default/all modes: Ready28/Root26/C68/CurrentOnly13/
Executor185/Knife220/repository20/growth7/check, scopedfmt/diff/strict standalone7.
Warning hashes unchanged (smoltcp22/lib7/libtest1/Knife151).
/tmp/mini-vpn-p3-root-peak.iroUDm/{baseline,source,sequence,gates,repeats,review}.json
owns exact evidence. No globalfmt/full-library-suite/external/commit/push claim.

Next executable gate: close the remaining channel lifetime premise before a
new Target Interface or C/physical aggregate implementation. Whole-scope SOURCE
retains aggregate P1 and selects original-owner composition with source-pinned
conservative backing proofs; private upstream Layout guesses are not accepted.
Temporary offline Tokio diagnostics now distinguish normal retention/candidate
peaks from a defensive near-wrap interleaving: debug/release normal tests8/8 each,
isolated exact observations10 each. The latter exits without resuming stopped
pointer users: not clean shutdown, natural MAX lifetime or a product bug claim.
449 product Rust hashes unchanged; no dependency or network change. Evidence is
/tmp/mini-vpn-p3-channel.C4qwtI/{profile,causal-first-observation,final-runs}.json.
Same Heisenberg diagnostic closure is SOURCE0/0/0; aggregateP1 stays open.
Bounded producer audit now closes checked Bound mailbox/TCP5/Actor3 as serial
in their actual owning routes, not their general type APIs. Next prove original
serial-push backing with receiver/close/reuse/wrap in existing51O; no additional
generic sidecars, lifetime-message cap or blanket queue rewrite. Matrix4 and all
five51B/P3/Case C/R7 duties remain; standing full-P3 execution authority applies.

Preceding51N progress arena remains accepted SOURCE0/0/0: real checked-C
BoundInactive Replay/RESET/output-owned expiry and seven native tests,20 final
gates/2180 repeated executions. Concrete owner-header+slot Layout precedes its
allocation; pure2(F+Z) arithmetic remains separate. reverse2768/progress877/C2956;
Integrator-only ratchets and all448 prior hashes are retained in
/tmp/mini-vpn-p3-progress.nYWfZO/arena-repair-*.json and arena-review.json.
Neither local closure proves whole-root zero or complete Root runtime peak.

Preceding51L M3 original Supervisor storage/receipt transaction is accepted,
same Heisenberg final SOURCE0/0/0, local9/9/9/9.439 hashes/11paths/428 unchanged;
21 final gates and40 module repeats/240 executions. behavior-{source,sequence,
gates,repeats,source-review,review}.json in /tmp/mini-vpn-p3-supervisor.73tG3G/
owns closure; existing local-results latest M3 entry summarizes exact limits.
Supervisor4834 ratchet; driver1169, TCP replay84/cold159, error709; tests621/398.
Legacy/Client outer health policy is unchanged; exact-input return applies
only to storage admission. Defensive ledger/Layout tests are not a natural
Bound/whole-root witness. Full P3 remains open; no permission pause or external/git.
Target's requested-backing closure follows the accepted native/buffer/output
transactions. Continue original progress storage, then phased Root accounting.
Preceding51K accepted: Session original flow/terminal nodes and complete lifecycle
implementation,434 hashes/9 paths/425 unchanged,31 green gates and40 module
repeats/160 executions. Same Heisenberg SOURCE0/0/0 Approve; Rust frozen.
/tmp/mini-vpn-p3-session.xmyomq/{source,sequence,gates,repeats,review}.json owns closure;
move-* owns preceding accepted0/0/0 Move. Session5364/lifecycle859/test380+168.
Next remaining matrix4 Target/progress/aggregate and all five51B. Outer Peer OPEN new storage errors
remain ordinary Reducer with consumed input, not an exact-input retry contract.
No further user permission pause in this existing P3 scope; no external/git.
Preceding accepted51J: original AVL ranges plus complete Session Offer preparation,
same Heisenberg SOURCE0/0/0 Approve. Rust frozen at the accepted boundary.
`/tmp/mini-vpn-p3-receive.T55xyv/closure-{source,gates,repeats}.json`
owns430 hashes/12 paths/418 unchanged,31 green gates and40 module repeats/220
test executions. Both TCP36/storage33/Session47/Supervisor48/Target179/C67/
owner9/continuation17/CurrentOnly13/server25/Knife212/repo20/growth7/check;
strict7/scopedfmt/diff/untracked checks pass. tcp1770/Session5585 exact ratchets;
receive819/new storage386/model521/storage tests191. Prepared nodes/actual
Offers, two original input owners, global accounting and output order have
local test/review evidence; clear1410->256 visits preserves ordered complexity.
Sole review P1: bulk extend -> capacity-proven single push,819 lines unchanged;
review.json owns inverse proof/closure.31 gates/40 repeats all rerun after repair.
Session51K above covers the following original flow/tombstone transaction;
combined consumer/five51B and aggregateP1/P3 remain open; no new
permission pause or external/git work. No blanket map migration.
Preceding accepted:51J pure195-line Move, SOURCE0/0/0,20 gates;
receiver-move*.json in the51I artifact directory owns that prior boundary.
Preceding accepted: joint51I sender descriptor transaction, SOURCE0/0/0 by same
Heisenberg. /tmp/mini-vpn-p3-descriptors.FVL93c/{baseline,source,gates,repeats}.json
owns426 hashes/8 paths/418 unchanged,34 gates/40 exact repeats green. tcp.rs1995,
send397/new tests186+index1; no debt edit. Actual capacity4/1 RED->GREEN;
prepared replacement, descriptor/effect fault-domain separation, original Drop
ordering, nonempty real Model refusal/retry and unchanged Executor gate pass.
That stage accepted sender backing only. Receiver closure now follows above;
other maps/Target/aggregate/call charges remain matrix4/P1, then combined
consumer and five51B. P3 remains open; no guessed std-node Layout or new unsafe.
Preceding accepted: checked-Bound fatal-return test, same Heisenberg SOURCE0/0/0.
Only child501->575 changed,424 hashes/423 unchanged;24 gates/40 repeats green.
fatal{,-repeats}.json in the directory below owns evidence. Current matrix3
first-path applicable duties covered; no natural unknown callback/await found
is SOURCE N/A-now, not runtimePASS or a waiver of future51B/P4–7 obligations.
Next aggregate matrix4, combined consumer and five51B; P1/P3 still open.
Preceding accepted: joint51G normal C recovery audit, same Heisenberg SOURCE0/0/0.
`/tmp/mini-vpn-p3-nested-inventory.q0LWgl/normal-c{,-gates,-repeats}.json`
owns424 hashes,24 final green gates and60 exact repeats.8 paths (7 old+1new),
416 unchanged; no ratchet edit. Actual C-held Publish false-zero RED->GREEN;
normal payload and error-only audit stay separate, typed failure escapes C.
Both Target178/C67/owner9/cont17/CurrentOnly13/server25/Knife195/repo20/growth7/
check/lower5/strict standalone7; scoped8fmt/diff/121 text checks pass. C3007,
test parent516/new501/C leaf693. Original checked pressure/retry/peer assertions
remain; defensive holders do not prove natural interruption or physical zero.
Preceding51F error audit remains accepted in remaining*.json, no coverage removed.
No permission pause; implementation remains subject to source/growth/review gates.

Preceding accepted: checked-Bound recovery FIFO-pressure component, same Heisenberg
SOURCE0/0/0; test-only effect_storage.rs450->706, original prefix/410 Rust hashes
unchanged. `/tmp/mini-vpn-p3-recovery.HLxB8L/evidence.json` owns both-mode gates
and10 exact repeats each. Continue applicable matrix3 unknown/nested inventory,
then matrix4/five51B; component success does not close full matrix3/P3.

Preceding accepted: joint51D original backing packet, same Heisenberg0/0/0 SOURCE.
Artifact `/tmp/mini-vpn-p3-effects.AGjIGL/progress.json` owns411 hashes,31 paths,
30 Cargo gate records/20 fixture repetitions and exact ratchets. Its follow-on
checked-Bound recovery/FIFO/cursor characterization is now accepted above.
No repeated grant request; full P3 remains incomplete.

Preceding accepted: one production cold Layout-rejection/exact-plan-return test,
user's process_plan.rs grant completed. Same Heisenberg SOURCE
01a0ad1d-21b4-7b22-96ec-caba625f586b:0/0/0 Approve ONLY this characterization.
Baseline/evidence: /tmp/mini-vpn-process-cold.YMBNAt/{baseline,evidence}.json;
durable2026-09-16 production-cold entry in existing local-results.

Leaf415->433 with old prefix exact;397 other Rust paths unchanged,398 total.
No production/API/route/helper/governance/new path. Actual MemoryTarget resident
Layout plus valid process arithmetic exercises production cold, not from_commands,
returning exact Layout cause and every original plan field before reserve.
Both exact+10 repeats, Root25/Ready25/CurrentOnly13; Knife162/repo20/growth7,
both checks, strict standalone pre/post7 and scoped fmt/diff/95untracked pass.
All33 Cargo invocations succeed; Reviewer ran no tests, verified398 hashes.
Existing warnings/old14 service fmt hunks are not newly accepted or repaired.

Aggregate cold-peak P1 remains open, not a confirmed OOM/memory-safety defect.
SOURCE follow-up01a0ad2a-fa99-7e50-b365-f80a228cac16 retains0/1/0: logical K/E
are not retained allocation capacity; raw E+1 and transfer scratch overlap need
explicit accounting. Seven real phases are mapped in the existing local result.
User has now CONFIRMED original-owner explicit capacity/reserve/growth discipline;
no repeated direction request. Same SOURCE01a0ad48-5cb8-7ed0-81b7-bf821b6188b3
independently confirms shared-core and MemoryTarget gaps; original P1 stays0/1/0.
Selected representation: original-owner managed contiguous Vec/VecDeque backing.
Same SOURCE01a0ad58-2d62-7b31-80c4-b7701cdaa081 accepts this, not full P1 or code.
First package spans original producer effects, Supervisor journal, Executor
current/suffix and raw output; Executor-only reserve misses producer allocation.
The current envelope is joint51D:18 production paths plus listed tests,
one new private unsafe allocation site. Same SOURCE
01a0ad6e-ea3c-7270-8a07-0a0f2da40f62 approves this proposal0/0/0, not code/P1.
SOURCE01a0ad69-8e9a-7790-b4b8-5ab781313ad7 resolves DATA spare/batch ownership
and exact recovery/restore return seams. User now explicitly grants the full
packet/limited unsafe and continuous P3 execution, including in-scope repairs
without another permission pause. Next freeze the real full fixture/first RED
and implement; preserve architecture/growth/review gates and the398-path baseline.
The real full fixture has progressed from cold-work0/3 RED to GREEN through
original Model/Supervisor/Executor backing, registered recovery and ordinary
reuse; injected real recovery Reserve returns the same received ATTACH for
successful retry. Mutation-start evidence prevents empty committed work from
being rolled back as uncommitted. Same Heisenberg approved these local source
seams0/0/0, not whole51D. Both modes storage25/Session47/Supervisor48/
OwnerTarget166/Controller61/Client37/CurrentOnly13/Root25/Ready25 pass;
innerTCP27/continuation16 and Knife190/repository20 pass in both modes.
Knife now loads the production resumable tree; prior flattened path assembly
could not resolve the new private children. No raw capability guard changed.
Eight packet fixtures pass10 repeats EACH mode; scoped31-path fmt/diff/checks,
Integrator shrink ratchets and strict standalone/both Cargo governance7 pass.
Two packet P1s have actual RED/GREEN repairs; final same-Reviewer51D closure
is Approve0/0/0, not an aggregate P1/P3 completion claim. All
Moves are already applied; never reapply stored original in-place/Move patches.
No global allocator/container rewrite or premature P1/P3 completion claim.
Matrix3 applicable checked recovery/cursor/unknown/full inventory, matrix4
call charges/capacity/topology and combined consumer review remain mandatory.
Select the actual falsifier and exact new write scope under the confirmed
direction. Earlier nested-observation/matrix1/2/5 evidence remains accepted.
All five51B packages/P3/Case C/R7/Task4 stay open; P4/P5/P6 separate.
No external, cutover, commit/push; do not reopen completed path grants.

## Preceding component evidence (current gate is above)

Historical evidence has moved to the [exact pre-cleanup snapshot](
docs/archive/project-memory/2026-09-18/HANDOFF.full.md#preceding-component-evidence-current-gate-is-above).
The current gate above and all remaining obligations are unchanged; this
archive pointer does not promote the historical slice or waive acceptance.

## Public-path repairs: local closure accepted

Original Popper approved the sender implementation and final format delta at
P0/P1/P2 `0/0/0`. Current UDP/Quinn, both governance routes, both provenance
suites, public regressions, scoped format, diff and all-feature library check
are green. The four exact shrink entries are promoted at
`16561/7991/14230/5390`; original sole Writer Herschel has stopped writing.

Exact commands, counts, final fingerprints and preserved historical stops:
`docs/tech/2026-09-05-public-path-capacity-repair-local-results.md`.
The coordinating task owns the separate P2T work below. Both client-root
repairs remain frozen; that public slice changed no R7/P2T Rust, and no public
route change, commit/push or macOS TUN/VPS/WAN/throughput/M2 action occurred.

The canonical state is `docs/current/STATUS.md`. This handoff is intentionally
small; the full pre-split handoff is preserved at
`docs/archive/project-memory/2026-08-25/HANDOFF.full.md`.

## Accepted position

Knife16 Task 4 has advanced through a locally accepted **necessary-only** R7
prerequisite/policy-kernel checkpoint. R6d's checked recovery budget,
real-Quinn client root, lifecycle cutoff, and zero witness remain closed. R7 now
also has one shared endpoint-session clock, actor-captured immutable inbound
arrival, exact semantic timer arbitration, a stable first-fatal terminal stamp,
typed positive owner application-ACK observations, and a bounded value-only
hint/rejection/switch-arbiter kernel.

The R7 checkpoint gates include session clock `2/2`, reverse-switch kernel
`8/8`, actor/composition `117/117`, root `16/16`, delayed-root B ACK `10/10`,
default library `1309 + 3 ignored`, all-feature library `1323 + 3 ignored`,
Knife16 provenance `145/145`, repository-layout provenance `6/6`, release,
current-baseline Clippy, rustdoc, exact vendored Quinn/proto, fmt/diff/secret,
and independent review P0/P1/P2/layout `0/0/0/0`.

The later test-only physical-Move boundary is also accepted: physical attach
`9/9`, legacy B `2/2`, terminal `5/5`, Knife16 provenance `158/158`, standalone
and Cargo governance `6/6`, repository test provenance `6/6`, fmt/diff/check
green, and the same Reviewer reports P0/P1/P2 `0/0/0`; Approve. Exact lines are
`owner.rs 2062`, test-only `owner/physical_root.rs 702`,
`r7_physical_attach_permit.rs 1407`, and its private `cleanup.rs 195` child.
This closes fixture ownership only.

The architecture-only cutover is independently accepted. The first review
returned Request Changes, P0/P1/P2 `0/5/1`; after reconciliation the same
Reviewer returned Approve, P0/P1/P2 `0/0/0`. Subsequent reviewed slices closed
the static `2 * (F + Z)` and checked `S + R` capacity boundaries, their exact
overflow/maximum/zero-scan characterizations, nested capacity provenance, and
the controller/output `progress_entries` false-zero predicate. Live
revision-reset reserve/return/commit remains a later root-ledger transaction.

The hard-terminal extraction amendment and A-D Move are now independently
accepted. Stable terminal Interface and root storage/mixed-inventory anchors
remain in `continuation`; the state machine, correlation/quarantine, guard,
terminal method bodies, and `Drop` live in its deep private child with no
behavior or visibility widening. Exact production lines are
`continuation.rs 971` and `hard_terminal.rs 779`; closure review is P0/P1/P2
`0/0/0`, Approve, structural quality `10/10`. The authoritative A-D evidence is
`docs/tech/2026-09-02-knife16-r7-hard-terminal-extraction-local-results.md`.

The post-extraction owner-root amendment is independently accepted. It revises
the order to `CurrentOnly root Authority -> production terminal preservation ->
root-owned no-ACK finalizer` and fuses successful output-root adoption with
infallible CurrentOnly publication. After Request Changes at `0/2/1` and
`0/1/0`, the same Reviewer returned P0/P1/P2 `0/0/0`, Approve, with Clean
Architecture and System Design `10/10`; all findings in that architecture
review are closed.

The direct-join sidecar at
`docs/tech/2026-09-03-knife16-r7-current-only-direct-join-amendment.md` is
independently accepted. It narrowly supersedes the predecessor's extra spawn,
root-level JoinHandle, and actor-transfer clauses. Its sole exact-error P1 is
closed; architecture review is P0/P1/P2 `0/0/0`, Approve, with Clean
Architecture/System Design `10/10`.

The separately authorized CurrentOnly constructor and actor-resident
living-root A/B/error implementation are now independently accepted as
necessary-only. The old `891/590` actor-extraction RED is retained only as the
causal disproof of the extra-spawn seam. At that acceptance, exact lines were
`owner.rs 2062`, `physical_session_root.rs 722`, and `current_only.rs 691`.
Focused `5/5`, A/B/error repeats `10/10` each, direct regressions, Knife16
`159/159`, repository provenance `13/13`, format/diff/check, and both governance
`6/6` gates are green. The same Reviewer returned P0/P1/P2 `0/0/0`, Approve,
with scoped Clean Architecture, System Design, and Reliability/lifecycle
`10/10`. The exact ledger is
`docs/tech/2026-09-03-knife16-r7-current-only-direct-join-local-results.md`.

The Case C `OwnerServerRoot` architecture boundary is independently accepted.
The same Reviewer closed its original five P1 and two P2 findings and returned
P0/P1/P2 `0/0/0`, Approve. Architecture-only correctness is `10/10`;
implementation readiness remains `4/10`. The accepted sidecar selects the
process parent, whole-root adoption, bounded ownership, and shutdown contract.
The conditional implementation plan is separately accepted as plan-only after
the same Reviewer closed six P1 and one P2 findings and returned P0/P1/P2
`0/0/0`, Approve. It freezes route/privacy direction, phased ownership, Writer
boundaries, and stop rules, but accepts no Rust implementation or production
caller and does not close Case C.

The older actor `ResidualOwnership { messages: 1, bytes: 0 }` pending
ordered-delivery failure was deliberately excluded and remains separate fixture
debt. Production still requires the `OwnerServerRoot` route and caller,
root-retained bootstrap custody, bounded owner-session table, real TCP Target
Adapter, separate UDP Interface, initial-A Authority absorption, terminal
preservation, the no-ACK transaction, and a whole-root ledger. The B-only
adapter remains `#[cfg(test)]`.

This is not physical R7a-d or Task 4 completion. `QuinnClientRoot` still lacks
production B/hint/ACK-stall/arbiter categories, the owner ACK observation has no
production owner-controller consumer, and physical B probe/hint provenance is
unwired. `client_tun::run_event_loop` still selects `LegacyOwnedUpstream`; there
is no production owner/server or second ingress. WAN, throughput, and the M2
one-second test are not solved.

After R6d, one behavior-preserving maintenance stage moved all `106` actor tests
from the `21,627`-line `quinn_leg_actor.rs` into its private external test Module;
the production prefix and test identities are unchanged. Current provenance is
`145/145`. The maintenance result records a pre-existing strict-Clippy baseline
discrepancy without changing unrelated production code.

A second isolated maintenance stage moved all `338` client-TUN tests from the
`31,059`-line `client_tun.rs` into `client_tun/tests/mod.rs`. Its `16,611`-line
production prefix and all fully qualified test identities are unchanged; the
production file is now `16,613 lines / 648,986B`. A separate repository-layout
provenance target passes `1/1`. This is source-locality work only and does not
change the accepted Knife16 or M2 position.

A third isolated maintenance stage moved all `162` TUIC tests from the
`13,696`-line `tuic.rs` into `tuic/tests/mod.rs`. Its `8,170`-line production
prefix, separate direct-probe declaration, and normalized test identities are
unchanged; the production file is now `8,172 lines / 311,114B`. The repository
layout target passes `4/4` and enforces an exact `45`-entry shrink-only legacy
backlog for directly declared positive-cfg inline test Modules. Standalone test
items and `cfg_attr`/macro-generated test code remain forbidden by policy and
review rather than claimed as lexically proven absent. New private unit-test
bodies belong under `src/<module>/tests/`; root `tests/` remains the
integration/public-contract and repository-provenance boundary. This changes no
data-plane or M2 behavior.

A fourth isolated stage moved all `65` owned-upstream TCP tests from the
`5,823`-line `owned_upstream/tcp.rs` into its private
`owned_upstream/tcp/tests/mod.rs` child Module. The `2,810`-line production
prefix, three parent-local test-support helpers, and fully qualified test
identities are unchanged; the production file is now `2,812 lines / 98,389B`.
The repository-layout target passes `5/5` and the shrink-only legacy backlog is
exactly `44`. This also changes no data-plane or M2 behavior.

A fifth isolated stage moved all `9` two-leg harness tests from
`owned_upstream/two_leg_harness.rs` into its private
`owned_upstream/two_leg_harness/tests/mod.rs` child Module. The normalized
extracted body and fully qualified test identities are exact; the parent also
contains separately tested R7 controller-API adaptation, so this stage makes no
whole-prefix identity claim. The repository-layout target now passes `6/6`, and
the shrink-only legacy backlog is exactly `43`. This is source-locality work
only. See
`docs/tech/2026-08-26-owned-upstream-two-leg-harness-test-module-extraction-local-results.md`.

Future Rust changes are now guarded by an executable shrink-only ratchet before
the remaining large files are refactored: new production/test leaf/test index
limits are `1,500/800/300`, existing line and `too_many_arguments` debt is exact,
and new debt is forbidden. The gate scans all current-repository Rust, follows
external cfg-test Modules, closes `#[path]`, rejects source `include!`, and
blocks broad lint-suppression bypasses. `AGENTS.md` additionally requires deep
private Modules, Authority-local transactions, bounded symbol-first reading,
and one Writer per overlapping Module. Existing oversized files remain later
behavior-preserving work; see
`docs/tech/2026-09-01-code-growth-and-ai-navigability-governance-local-results.md`.

## Historical next gate at that time: P3 remaining ready and lifecycle tracers

User authorization now covers full P3 and autonomous causal in-scope repairs;
architectural hard stops and review/growth gates remain. Public and P2 chain
commits are `5fca1ed` and `532d766`, local only. The private route, mailbox
notification/prefetch, single Control rearm, preparatory Moves and real Owner
source characterization remain accepted. B conditional two-cell lifecycle stays
accepted at root405/source-parent476/leaf771. Persistent capacity registration
is now also accepted: session896/progress480, test parent88/new leaf344; five
real cancellation/replacement/close/Drop/wake-panic tests. Root35/35 over ten
rounds, mailbox18/18, TCP68/68, client1/1+3/3, provenance162/162+13/13,
fmt/diff/check and governance6/6+6/6 pass; same Reviewer0/0/0 Approve.
CAS precedes allocation; cached registration drops before its sole lease.
Driver148/scheduler51/Control604, tcp2883/client wrapper and governance699
are unchanged. One persistent mailbox registration is not a global park proof.

The one-session prototype's pure SessionCommands Rename is implementation-
accepted at0/0/0; four inverse hashes, unchanged line counts and the same full
gates pass. The private Debug type label changes; no process root is created.
Per-flow Drivers stay under OwnerTargetExecutor, not a new parallel owner.
The subsequent process-only arithmetic plan is also independently accepted:
nine exact tests, root44/44 and full scoped gates pass, no repeat claim;
then root504/test parent89/new leaf115, other bodies/sources and governance frozen.
It checks every class's service product including exact MAX, but proves no
allocation, physical envelope or executable fairness. A subsequent one-file
selected-send Extract Method is independently accepted0/0/0: root508, exact
four-line inverse, other20 paths frozen; before/after44 and full scoped gates
pass. Its helper requires validated Held+Ready, and keeps the entire transfer.
The actual-ready proposal's two P1s were independently closed0/0/0:
rearm Ready with remaining Full must republish registration work; summary
clear/epoch acknowledgement use explicit acquiring RMW chains. The first real
dispatch implementation now has exact1/1, ten serial repeats, root45 and the
same scoped regression/provenance/format/check/governance gates green.
Root823/scheduler598/route90/new leaf261 are independently accepted0/0/0 for
this first handoff only. A teardown-only cached receiver-Waker failure was corrected
after the critical handoff proof, without changing production or rescuing B.
Complete cold-peak byte capacity remains unproved. A subsequent §7 audit found
claim/finish scheduler faults wrongly isolated to one resident: exact RED sent
B on the next poll. The minimal root-fatal mapping and terminal-first ordering
are now independently accepted0/0/0; root825/leaf333, exact+ten repeats/root46
and all scoped gates green, other Rust/ratchet unchanged. Source unknown still
isolates only its resident; finish-error preservation is source evidence only.
The subsequent remaining-Full/UnrelatedLane test is accepted0/0/0:
leaf333→528 only, root47/exact+ten repeats/full scoped gates pass. It
uses actual Ordered then Control capacity events and consumes real receipts;
all production and inventory remain unchanged. Initial fixture E0599 was
repaired before the first runtime GREEN, not a scheduler failure.
Publisher-failure design and preparatory three-test Move are now independently
accepted0/0/0; the Move preserves the complete528-line leaf. First revision-
overflow RED reproduced B's fault followed by A send. The single gate now
passes exact+ten repeats/root48/all scoped gates. Review's check-before-lock
P1 was repaired by moving quarantine's gate check under the slot guard; all
gates pass again, same-Reviewer re-review0/0/0 Approve. That race is source proof.
Root825/scheduler687/moved leaf530/new leaf107; source transactions and debt
inventory unchanged. Dedicated poison/quarantine, mid-unit and other fault
tracers remain open; see the 2026-09-07 ready-failure Authority plan.
Next process-revision characterization directly GREENed: source publication
succeeds before aggregate exhaustion, with the fault observed before root
selection. Only leaf107→215; exact+ten/root49/all scoped gates pass, same
Reviewer0/0/0 Approve. Production and inventory unchanged. The approved private
fixture extraction is green: leaf209, inverse215-line hash exact, both tests/
root49/all scoped gates pass, same Reviewer0/0/0 Approve. Actual slot-poison
publication is now directly GREEN: fault observed only after real capacity
dequeue, before root selection. Exact+ten/root50/all scoped gates pass; only
leaf209→288, same Reviewer0/0/0 Approve. Poisoned source-unwind recovery is now
directly GREEN: real enqueue/receiver panic, gate Open before recovery, then
Poisoned with the original claim/metadata retained and one real receipt consumed.
Exact+ten/root51/all scoped gates pass; leaf446 only, same Reviewer0/0/0 Approve.
Ordered first-cause retention is now directly GREEN: two real publications,
Poisoned then process exhaustion, first cause unchanged. Exact+ten/root52/all
scoped gates pass; only leaf446→575, same Reviewer0/0/0 Approve. No concurrency-race
claim. Admitted-work completion now directly GREENs using unchanged
DequeueDuringSend: A's actual send/finish completes despite B's mid-unit fault;
later polls stop and B stays held. Exact+ten/root53/all scoped gates pass,
leaf743 only, same Reviewer0/0/0 Approve. Notify-unwind now directly GREENs through
actual root Pending, fault-before-Waker and unlocked metadata, then same-root
re-entry and exactly one real receipt. Exact+ten/root54/all scoped gates pass;
parent745/private child175, other Rust/governance frozen. Same Reviewer0/0/0
Approve. One-waiter evidence is not full park or concurrency proof. The healthy
source-unknown tracer now directly GREENs: only A is quarantined after its send
unwinds, gate stays Open, and B's next actual send succeeds before either receipt
is drained. Exact+ten/root55/all scoped gates pass; parent746/private child255,
all18 other Rust/governance frozen. Same Reviewer0/0/0 Approve. The retained
A Unknown is not clean zero. Re-park/fatal now directly GREENs through actual
cached-Waker replacement/dequeue: same-poll Rescan, later Quarantined, exact B
retained and faulted park_epoch rejects without mutation. Exact+ten/root56/all
scoped gates pass; parent747/private child217, other19 Rust/governance frozen.
Same Reviewer0/0/0 Approve. This is not independent final epoch/gate, initial
registration or CAS-concurrency evidence. The alignment audit selected actual
Executor source service. A preparatory Move is now accepted: parent10965/new164,
exact inverse hash, same Owner74/Controller44/continuation16, existing P3 matrix,
provenance, fmt/diff/check and both governance. Integrator lowered only the
parent ratchet; same-Reviewer implementation/document closure is0/0/0 Approve.
Scoped Module/Clean/System/DDIA9/9/9/9. The new
`docs/tech/2026-09-07-knife16-r7-p3-owner-source-move-plan.md` owns this freeze.
The source-service placement decision is independently accepted0/0/0 in
`docs/tech/2026-09-07-knife16-r7-p3-owner-source-service-decision.md`.
The conditional Owner-local credit protocol and compact-doc closure passed0/0/0:
`docs/tech/2026-09-07-knife16-r7-p3-owner-credit-rearm-contract.md`.
The Target readiness contract is at
`docs/tech/2026-09-07-knife16-r7-p3-target-readiness-contract.md`: demanded Flow
polling plus bounded shared notification on the same TargetIo. First review's
sole P2 source anchor is repaired; final protocol/document closure passed0/0/0,
including the build-variant prerequisite. The separately plan-approved Target
Implementation Move is now green at parent1445/new memory_io582, unchanged
573-line block; Integrator removed the production debt entry, governance698.
Original/caller regressions, provenance, fmt/diff/check and both governance are
green; same-Reviewer implementation/document closure passed0/0/0 (structure9/10). See
`docs/tech/2026-09-07-knife16-r7-p3-target-move-plan.md` and the latest P3 result.
The complete test Adapter Move now matches its reviewed plan at10206/726/133;
private fixture sets7/1, unchanged total Owner74 and the same scoped regressions/
fmt/diff are green. Integrator lowered only owner_target10965→10206; both
governance6/6, provenance162/13 and all-feature check pass. Final same independent
implementation/document review passed0/0/0 Approve, scoped8/10, no necessary
optimization; this pure Move is accepted, not binding behavior or full P3.
See `docs/tech/2026-09-07-knife16-r7-p3-target-adapter-move-plan.md`.
Actual binding source preflight passed same independent review0/0/0 at
`docs/tech/2026-09-07-knife16-r7-p3-binding-preflight.md`; no Rust grant.
Next: close old-generation publisher reclamation/pressure and wake liveness,
coupled to the real cold callers, release-entry/guard and K/Layout/symbol freeze.
F live slots and registration Drop alone do not bound old Arc backing.
Direct raw-drop Notify also introduces lock-held/unwind callback hazards;
the storage candidate is not a complete or approved notification protocol.
The controlled-wake-contract.md passed conditional protocol review: standard Wake,
inert Drop, real unregister/consuming-release edges, bounded primitive defer
exclusion and Owner-local publication. The sole P1's single Root Notify waiter
and exact inner-cleanup/guard/outer-cleanup ordering are now frozen; same
Reviewer0/0/0 Approve, scoped8/9/7/8. No Rust grant or coupled binding acceptance;
next is actual holder/cold/guard/K/Layout/symbol freeze, not another pure Move.
The construction/feedback direction passed independent0/0/0 Approve at
`docs/tech/2026-09-07-knife16-r7-p3-binding-construction-phase-decision.md`:
same ready domain before checked construction, exact reservation return,
preserved real live/recovery inputs, and Flow completion custody rather than
waiting for RESET under an input-excluding continuation. Only direction and
document evidence are accepted; no Rust grant, defaults or late binding.
The follow-up Flow completion protocol now passes same-Reviewer0/0/0 Approve:
`docs/tech/2026-09-07-knife16-r7-p3-flow-completion-contract.md`.
It selects actual-terminal late-command disposition, bounded revoke/drain and
transit settlement. Its sole P1 repair distinguishes proven source/Target
absence in cold Open failures from actual drain/join. The following selection
also passed same-Reviewer0/0/0 Approve (scoped8/9/7/8):
`docs/tech/2026-09-07-knife16-r7-p3-binding-turn-contract.md`.
Existing carrier is selected through a closed Driver/full-LocalFlow graph,
not a trusted variant tag; three existing-root turn modes and thirteen service
categories include actual output/lifecycle and recovery. Work/ports can still
require K/formula revision. CurrentOnly has no such service caller or physical
lifecycle slots; existing service remains in the test-only B-adapter. The single
working joint packet is now `docs/tech/2026-09-07-knife16-r7-p3-binding-joint-contract.md`.
Its expiry choice and actor-guard constraint passed review0/0/0: retain
the original fact in Controller, dispose actual late commands, defer record
deletion until physical settlement, and produce completion once. Joint acceptance
remains open. Its subsequent OPEN selection also passed0/0/0 (8/9/7/8): actual
F resident owns admission; one model classification precedes F/O reservation
and request commit; duplicates need neither. The sole P2 source anchor was
corrected. Old live-F held retry can deadlock behind a later RESET/ACK and is
not included in that selection. Metadata/test placement then passed0/0/0
(9/9/8/8): inward metadata/canonical geometry, outer budgets/policy and all12
Root scenarios, nine using private metadata recipes. Their finite operation/
short-borrow/Wake lifetime contract also passed0/0/0 (9/9/8/8): exactly two
actual claim comparisons, no extra ownership or unified teardown. Exact Rust
signatures are not compiled. ReadyGeometry value Interface and18-call/return
inventory then passed0/0/0 (9/9/8/8). All calls are tests; live Target and the
caller-held recovery carrier remain valid inputs, not cold-zero. Closed Owner
correlation selection/source facts then passed0/0/0 (9/9/8/8): installed
Supervisor/factory origin plus actual admission gate, not remint/getter or a
detached proof. Cold-slot selection then closed two P1s at0/0/0 (9/9/8/8):
cell leases stay charged while Idle/Pending; AssociatedPreparing retains one
bind right but no cold-cancel right; cold handles cannot escape startup/active
synchronous Root custody and its exit checks. Constructor ownership forms then
passed0/0/0 (9/9/8/8): opaque short borrow, lifetime-free owning state, scoped
signatures and whole Drop-bearing custody. Uncancellable Cold retains original
inputs in the actual Root's single fatal execution arm or unpublished build
failure; P1 and falsifier P2 closed, no P4 table or false Free. Pair/route field
selection then passed0/0/0 (9/9/8/8): sole checked inbox, complete inward Bound
commands, one consuming pair installation and explicit nested Drop order.
Generic write/HalfClose/actor/recovery/continuation must not reach old mixed
RESET intake. Drop is not shutdown. Initial-work/lane-poll protocol then passed
0/0/0 (9/9/8/8): closed internal publication, live-source import probes,
recoverable/close work and registration-or-real-publication Pending. Due is
not queue count or eligibility; raw-poll wrapper needs actual build proof.
Real-port review then rejected two hypotheses at0/2/0: Running as a universal
local-work prerequisite, and predicate/ungate/rebuild shortcuts for Applied
plus stopped output. Joint41–42 now selects the existing post-join whole Parts
for first output adoption, retaining original joined/stamp and Controller/
high-water. Written entry/retention review0/0/0 (9/9/8/8) is accepted, not a
callable expiry freeze. No Fatal reset/new parent. Joint43–44 now selects
semantic LegLost S, checked explicit G/D and same-clock pre-D eligibility;
Installed is the winner, not acceptance enqueue. Written closure0/0/0
(9/9/7/8) after request/status wording P2 repair. D is not a retention bound. Next close
whole-holder migration and candidate-set Authority before callable expiry.
Joint45–47 selects existing initial ownership of A with original C/B association
inside its output subroot; normal no-ACK consumes input before reporter, then
real terminal/FIFO/slot/stamp settlement and direct join. Written closure0/0/0
(9/9/7/8) after the old Cancelled/fail_output mapping P2 repair;
not bounded-pressure or private Interface approval. No retained seed plus
duplicate C, orphan A, new parent/P4 table, or invented non-initial seed. Exact
placement/producer closure, decision/expiry work and due/shared ports remain.
Joint48–50's precise preparatory Move plan passed written0/0/0 (9/9/8/8):
declaration anchors remain initial,15 complete impls move private, then initial
moves under owner. The21 internal caller closures, six old composition scopes,
five outward names, two source checks and one route-only owner exception are
explicit. A preliminary structural-test Move avoids growing its1168-line leaf.
That Initial-placement Move remains unexecuted. Consolidation in joint51
now owns the next order: one complete Active source/receipt/output vertical,
after its actual binding/readiness/Layout/lifecycle/write freeze. Initial Move
and Legless/B design remain P3 work when causally needed, not universal first-
path prerequisites. Do not resume the superseded all-joint-before-any-Rust loop.
AGENTS.md now requires a finite design exit and separate code/test evidence;
see the compact STATUS action and joint51 checklist. Scope review passed0/0/0;
the next deliverable is one concrete A1–A4 shared implementation packet.
The user subsequently authorized early P3 whole-CurrentOnly residency and
opaque owner-scoped service, without P4–P6. Its real pre-opened fixture baseline
is now implemented: current_only.rs744 and private active_source.rs219,
6/6 tests and10/10 exact repeats, provenance162/13, both governance6/6,
fmt/diff/all-feature check green; same Reviewer0/0/0. That fixture left production unchanged.
This proves real OPEN/RESULT/feed can reach original CurrentOnly construction,
not Bound service or whole-zero. Do not reopen the residency authorization or
replace the next shared-code RED/GREEN with more documentary prerequisites.
The subsequent six-file pure Move is accepted0/0/0: checked construction is
private construction.rs71/parent3565; Target failure/completion is private
completion.rs164/parent10061. Both original implementations inverse-reconstruct
exactly; source guards scan parent and child. Integrator ratchets3565/10061,
related159/159, Active repeats10/10, provenance162/13, both governance6/6 and
fmt/diff/check pass. No bound behavior changed. The test-only opaque Prototype
resident choice is approved, not implemented; its finite same-point assertions
and the remaining A1–A4 shared route are next, not another residency question.
The complete command visit is now an existing private SessionCommands operation,
not Root cell inspection: server_root.rs857, same56/56 tests and five10/10
repeats, provenance162/13, both governance6/6, fmt/diff/check; same Reviewer0/0/0.
Claim/budget/finish/park and exact Full/Unknown custody are unchanged. This is
one preparatory Extract Method, not the inward Move, Prototype or Bound route.
A1–A4 now has one concrete candidate appended to joint51: actual preparation/
adoption, paired receipt/service, real Target registration/backing, same-slot
publication interval,18-call lifetime migration and finite write set. Same
Reviewer first returned0/3/0, then0/1/0: inactive activation and publication
failure/unwind closed; entire actor-root exit remained. The same candidate now
uses the existing JoinSet completion registration/original result ledger, not
new Notify in Drop. Final same-Reviewer review0/0/0 approves this finite Rust
sequence, Module/Clean/System/DDIA9/9/8/8, not implemented binding/runtime.
Baseline/necessary characterized Moves and compile/TDD now follow that scope,
without another prerequisite sidecar or a gate waiver.
Four cold-construction Moves have now executed, with complete-body/full-parent
inverse proof, four exact ratchets and default/all-feature275/275 plus all gates.
Same-Reviewer code closure is0/0/0 Approve; this is not implemented Bound service.
The Actor Move baseline originally exposed a retained undelivered FIFO wait1/0
at join. The resumed goal's finite fixture-cleanup packet now closes that gap
and three related fixture dispositions, with all gates and same Reviewer0/0/0.
That fixture repair left production unchanged; the subsequent characterized
Actor Move is now accepted below, without runtime behavior changes.
The independent continuation test co-Move is accepted0/0/0: original16 tests
and399 lines are unchanged in transactions.rs407; parent330, exact ratchet
integrated. Before/after default/all-feature16/16 and all final gates pass.
Initial-acceptance co-Move is also accepted0/0/0: parent603/child574, four tests
and sole helper unchanged; negative source guard now covers parent plus child.
Both-mode post regressions52/52 and final gates pass; its old1168 debt entry is
removed. Physical attach's nine unchanged tests are now in scenarios.rs747,
parent668; both-mode post53/53, final gates and same Reviewer0/0/0 pass, with
its old1407 entry removed. All three finite test co-Moves are accepted; actor P1
and its narrow repair grant stay open, not hidden by these results.
Canonical geometry is independently accepted0/0/0: owner_ready55, Root848,
scheduler690 and then-plan tests123. Both-mode57/57 plus57×10 and final gates pass;
only immutable capacity ownership changed. User subsequently granted two cold
rejection/return cases in that leaf: original ten unchanged, current448 lines,
both-mode Root59/59,59×10/final gates and same Reviewer0/0/0. That finite
scope/coverage P1 closes. The separate two-line Layout removal now also
closes0/0/0: scheduler690->688, sole Root caller's resident Layout and successful
Vec<Waker> reserve dominate both checks. Two exact cases, plan12, both-mode
Root59,59×10/final gates pass; every other source is unchanged. This is not a
complete A3 Layout or legal-layout allocator-failure proof.
All nine metadata-private recipes and the separate outward12-scenario Move
are accepted0/0/0. Real Root/mailbox/command scenarios now live in the outer
tests/ready_dispatch tree; all49 original items, including12 test bodies and
7 native Wake/Drop implementations, are byte-identical. Root tests index2;
outer leaves540/572/120/188/167, inward index124 and leaves7/150/69/48/190.
New route provenance exact RED->GREEN; outer12/12, both-mode Root59/59,
59×10, Knife162/repository14 and final gates pass. No production/inventory
change in that Move. Layout removal and complete metadata placement have since
closed0/0/0: owner_ready735, Root846, six recipe files moved verbatim and seven
old files removed. All24 implementation items and all outer59 tests/FQs stay
exact. AST route RED->GREEN, Root59 both modes/59×10, Knife162/repository15
and final gates pass; no scanner/inventory edit. Subsequent symbol-first review
returns0/2/0 on the independent command-Move boundary, not the accepted code:
production admission/masks/quarantine and nominal/result placement lack a
closed intermediate interface; finite Prototype signatures cannot yet capture
the original Full Box address before outer operations. Couple the complete
command/35-test migration to real Current/Bound GREEN in the same A1–A4 packet,
closing those findings rather than creating a temporary production Prototype.
The resumed goal subsequently closed four Actor fixture cleanups0/0/0:
one private Adapter owns three exact terminal/FIFO/join cases, while arrival
uses its existing real-delivery wait after time assertions. Index15814,
leaf136, arrival513; Integrator only lowered the index ratchet, governance690.
Original tests/FQs/assertions survive exact inverses; production is unchanged.
Terminal cases10 repeats each, arrival10/both-mode8, related regressions,
Root59, Knife162/repository15 and final fmt/diff/check/governance all pass.
The characterized Actor Readiness/Output Move now closes0/0/0: parent6583,
children88/122, provenance507 and exact ratchet. Strict parent/block inverses,
both-mode24/24,80 repeated passes, both-mode Root59, Knife162/repository16 and
all final gates pass. One review P2 extended the original four negative inbound
checks to both children; no Actor tests, storage/callers or runtime behavior
changed. The subsequent authorized Active owning-fixture extraction closes
source review0/0/0 and final gates: current_only754/active_source244; exact1,
repeat10, both-mode CurrentOnly6/Root59, Knife162/repository16, fmt/diff/check
and standalone/Cargo governance6. Production is unchanged; named retained
bindings preserve the original peer/attachment/LocalFlow/payload lifetime.
The user's explicit authorization now covers the private Root bound_source
test route and complete seven-method Target retirement Move in joint51.
The characterized Target Move now closes0/0/0: parent9635/completion411/
provenance591, exact inverses, Target74 both modes/74x10, CurrentOnly6,
Controller44, Root59 both modes, Knife162/repository17 and final gates pass.
Integrator alone lowered9877->9635; governance690 and both6/6 pass.
The first real Bound tracer and fixed Root test operations are present.
Original constructor extraction passed Active1/1 and CurrentOnly6/6 before RED.
Cold custody and original-factory identity code are source-reviewed0/0/0.
Complete commands/35 byte-identical tests now live inward; fresh pair/observer/
Vacant cells and generic whole Current-or-cfg(test)-Prototype Root are written.
The same Reviewer approves this source delta0/0/0, not standalone Move GREEN.
The consuming checked installer is now written: original capacity/progress and
source identity precede real F backing/publishers/one paired mailbox and final
bind. C retains its inbox/receipt; rejection returns all five original inputs.
Source review0/4/1 is repaired and re-reviewed0/0/0, not runtime acceptance.
The same publication word now uses accepted ENABLED to retain committed
activation across finish; activation is now written, while finish and runtime
validation remain incomplete.
All18 checked callers are migrated, retaining real domains through original C
lifetimes and exact retry/cancel. Same Reviewer accepts the remaining13-source
closure0/0/0; only10 authorized tests changed, continuation index stays330.
Metadata tests remain unrun. Bound exact compile RED4 contains only actual
path references; no signature/privacy error or runtime acceptance.
Fmt/diff/standalone governance6/6 pass. The total constructor-quarantine exit
now has its real build_checked_controller caller and one Root failure destination;
consuming start also exists. Resident classification repair source-closes0/0/0:
registered inventory mismatches are fatal; pending unpublished bindings are not.
Cold tests236 remain unrun. Five-source claim/Visited and22 exact test-line
migrations are source-reviewed0/0/0; sends remain two-lane. No runtime acceptance.
Whole-current adoption/activation now traverses actual actor/FIFO/handoff,
live output/checked C, Supervisor/factory and both directions of Flow backing.
Original binding activation is the final fallible edge; reserved whole-root
placement follows. Same Reviewer source0/0/0; Active/ENABLED preflight mismatch
repair also source-closes0/0/0. Two new metadata tests remain unrun.
Actor's existing parent/readiness/output now contain the selected Output port,
explicit release/close edges, single controlled registration and retained exact
nursery outcome. Lines6583/130/223; same compile4, fmt/diff/governance6/6.
Same Reviewer submission01a0869b-a0f4-7760-a5af-716042fc8b61 approves SOURCE0/0/0,
not runtime. Poison remains and child/root Err reaches original nursery even
when LocalShutdown suppresses void cancellation's record; that call itself
does not retain the new cause. Required real poison/close/pressure tests are open.
The explicitly authorized governance fix is now reviewed0/0/0: root-only build
target exclusion plus actual inventory/classification RED->GREEN; standalone7/7.
Governance752, limits/inventories/other exclusions unchanged. No remaining
governance authority blocker. Target's seven files stay conditional SOURCE only.
Cargo exposed source_factory's cfg(test)-only current_leg misuse; its176-line
file now uses model phase/generation plus exact installed contract; same Reviewer
approves SOURCE0/0/0. Non-test lib checks have2 missing physical methods; exact
Bound test compilation retains4 references. No runtime or Cargo gate GREEN.
Publication admission/finish is now SOURCE-reviewed0/0/0: original binding,
sticky ACTIVE, deferred Notify and borrowed completion; five new metadata tests
remain unrun behind the old4 lib-test references. Both non-test modes/Cargo
governance lack2; fmt/diff/standalone7/7. Actual Root completion-before-Notify,
ready.finish after later fault, and physical quarantine-before-close remain.
See results' "Admitted publication interval, conditional SOURCE closure".
Selected receive primitives now close SOURCE0/0/0: session938/driver167,
commands test index3/new bound470. Three actual-port tests remain unrun behind
the same four compilation gaps; fmt/diff/standalone7/7. Old mixed paths unchanged.
Owner-only DATA authority is now granted and implemented, SOURCE-reviewed0/0/0.
Original Supervisor storage retains staging before healthy/reduction and through
Replay/map handoff; Drop only poisons. Client/legacy release is unchanged.
New child337 and test207; three real-Owner tests remain unrun behind old4.
The initial Vec allocation, actual Bound integration and runtime matrix remain
required. Selected receipt consumer now has same-Reviewer SOURCE0/0/0: actual
C Held before validation/clock, original observed reduction/ordinary admission,
actual Flow completion retention on pressure, active-only binding validation.
Actual Executor selected Driver producer now has same-Reviewer SOURCE0/0/0:
one indexed actual Flow, checked transit, immediate Held, full LocalFlow,
separate pull/transfer, original Full/observer, revision recheck and slot-credit
request. Retiring retains due/association but not ordinary Driver eligibility;
the review P1 is closed without reclaim/zero. Four new tests remain unrun.
Resume guarded C source caller, actual Target/category work, Bound read/RESET
cutover and physical output under joint51, not another sidecar. Executor9634/
C3536/Supervisor5597 exact; ready1059/cold668/flow1028/test-flow648;
commands694/tests-bound693/send_cells771; Executor service367, Root474/
construction241, physical741/service59, C service140/continuation1037.
No ratchet changed in this increment. Main fresh standalone7/7, fmt/diff pass;
both lib modes and Cargo governance still lack2, Bound lib-test4. See latest
result "Selected actual Driver producer and retained send service, SOURCE only";
checkpoint /tmp/mini-vpn-p3-driver-producer.cEj9Ex/driver-producer.json.
On actual Current quarantine failure retain Running claim/completion, not Idle.
These are coupled implementation obligations, not four small compiler fixes.
No standalone command Move, new sidecar, unapproved authority or P3 acceptance.
Local results' Actor Output checkpoint remains preceding accepted SOURCE evidence.
Join has no grace-relative completion bound: pre-join responsibility and both
general P1s remain open. No prior acceptance is revoked. A fixture assembles
actual objects, not scheduler/finalizer business
logic. Lifecycle action storage/stop discard is not timer-due consumption.
Registration implementation and global activation/lookup remain open.
Complete Move Interfaces/deletion and executable confinement remain later gates.
Input custody/release retry/consumer/masks/
work/Layout and implementation remain open; this is not P4/P5 integration.
Internal completion/grace/output must remain eligible. No P6 or full-P3 acceptance.
Full-P3 execution and the joint51 coupled binding write set are authorized;
generic TCP/Client rollback stays unchanged. The executed finite fixture
exception above is not a blanket grant to rewrite unrelated old tests.
The STATUS current-action section owns the next tracer sequence. Neither this
repair nor the first handoff closes full readiness/lifecycle/fairness or P3.
No extra queue/generic admission/wrapper may replace source binding. Borrowed
Driver is conditional, not persistent production association/exclusive caller
proof; that full-ledger item stays unchecked. Dynamic scheduling, budgets and
Target remain open. Do not grow the lifecycle leaf past800 or begin P4.
The complete ledger, reviewed boundaries and current packet audit are in
`docs/tech/2026-09-07-knife16-r7-p3-owner-progress-local-results.md`.

The two-file CurrentOnly constructor and living-root direct-join slice is
closed by the dated local result, and the Case C sidecar has independently
accepted `OwnerServerRoot` as the longer-lived process parent. Its conditional
route/privacy and caller implementation plan is now independently accepted as
plan-only. The P1 Target-open/ready-scheduler amendment is now independently
architecture-accepted: replacement Reviewer Heisenberg closed all six findings,
P0/P1/P2 `0/0/0`, Approve. Exact review scope and candidate hash are in
`docs/tech/2026-09-04-knife16-r7-target-open-ready-scheduler-amendment.md`.

Case C remains open. Actor/JoinSet `Drop`, abort, detach, or a whole-root Drop
test is not settlement. The user authorized the complete P2T node after the
public task's own closure. The historical entry `4/6` stop remains in
`docs/tech/2026-09-05-knife16-r7-p2t-bootstrap-test-move-entry.md`.
The P2T Integrator has now revalidated frozen source hashes, passed fresh
standalone governance `6/6`, and run six pre-Move exact baselines, each `1/1`.
Mencius owned the four-file pure Move and post-Move focused checks;
Integrator owned full gates, exact shrink ratchets and compact promotion.
The four Rust files are frozen after the separately confirmed audit-boundary
and net-zero import-format repairs. Six formatted-tree exact tests pass `1/1`,
full provenance is `159/159` and `13/13`, and fmt/diff/check are green. The
Integrator lowered only the expected two test-parent ratchets to `15818/2626`;
standalone and all-feature governance each pass `6/6`. New leaves are `480/374`.
The same Heisenberg independently approved P2T at P0/P1/P2 `0/0/0`, findings
none; scoped structure/depth/locality scores are `9/9/10`. The user-confirmed
single-sentence evidence correction closed the only final status-review P2;
same Heisenberg returned `0/0/0`, Approve and allowed final handoff. That last
doc-only closure changed no Rust and reran no tests. **P2T is complete.**
Current evidence and logs are in
`docs/tech/2026-09-05-knife16-r7-p2t-bootstrap-test-move-local-results.md`.
P2A's separately authorized persistent goal closed the private non-Clone
`run_server_bootstrap` parameter aggregate and existing allowance removal,
under the accepted plan Sections 6/12. Routine in-scope red repair no longer
required another confirmation during that node. Main owned feature source/tests;
Mencius independently promoted only allowance 7→6; both governance routes
passed 6/6. Actor production stays 7,001 lines with the callee body unchanged.
Real identity repeat 10/10, bootstrap 6/6, full provenance 161/161 and 13/13,
fmt/diff/check are green. Same Heisenberg closed design and final implementation
review at `0/0/0`, Approve; scoped structure/depth/locality are `9/9/10`.
P2A evidence: `docs/tech/2026-09-05-knife16-r7-p2a-bootstrap-parameters-local-results.md`.
P2A is complete. P2B's confirmed binding-source repair now passes exact 1/1,
source 4/4, full Knife16 162/162 and repository 13/13, with fmt/diff/check and
source/whitespace checks green. Root remains 2626 and only its one assertion
changed. Production Move remains actor 6769/private child 243, index 15818;
its real bootstrap 6/6, ten rounds each 6/6 and all-features 6/6 stay applicable
to unchanged hashes. Standalone governance stopped solely at actor 7001→6769.
Mencius promoted only that value; fresh standalone/Cargo governance each pass
6/6. Same Heisenberg approved full P2B at 0/0/0, closing the source-owner P2;
its read-only nine-check audit passes. No independent Cargo/test rerun is
claimed. The accepted P2 Rust is committed; P3 now has separate user authority.
Earlier stops remain historical evidence; necessary optimization findings none.
See
`docs/tech/2026-09-05-knife16-r7-p2b-bootstrap-implementation-move-local-results.md`.
P2B implementation is locally accepted. For P3's current acceptance and next
gate, use the Next gate section above and docs/current/STATUS.md, not this
historical P2B paragraph. Production exposure, caller cutover and external
acceptance remain frozen.
P2A's repair exception did not continue into P2B; P3 has its own user authorization.

Stop on a route/privacy plan that widens the Interface, invents a second
lifecycle Authority, copies custody, lacks non-test caller storage, or infers
Case C completion from the accepted architecture or living-root A/B/error
evidence. Do not mix in terminal/no-ACK/B/runtime-credit/public routing.

The old actor `ResidualOwnership { messages: 1, bytes: 0 }` failure is a
separate ordered-delivery fixture debt. Its later pure Move and real
`LocalCancelled` terminal/FIFO settlement cannot be mixed into the A/B repair.

Beyond P2A, separately authorized slices must follow
the accepted phased plan to implement and review the production root/caller,
bootstrap custody, bounded table, and TCP Target Adapter. It then remains:
absorb/cut over initial-A
terminal/join/shutdown Authority;
connect a private root-minted outcome to terminal preservation; close the
root-owned no-ACK finalizer; build authenticated B topology; install runtime
credit only after controller progress and `OwnerSwitchHintIssuer` share the
root; then wire physical observations/probe/hint/arbiter and the no-oracle
harness.

R7a-d acceptance still requires:

1. R7a: prove a reverse-only A DATA/ACK blackout may leave A's probe healthy;
   without an authenticated B hint the client does not switch.
2. R7b: wire reducer-owned Target-to-client outstanding/terminal observations,
   the owner controller, physical B probe freshness, and one authenticated hint;
   then prove application-ACK age causes one B
   hint, one client attach, exact replay, and a first B application ACK inside
   the combined horizon.
3. R7c: adapt the physical B hint into the accepted rejection kernel and reject
   wrong seal, session, generation, nonce, sequence, direction,
   cause, offset, idle flow, or stale B health; duplicate/lower hints cannot
   start a second attach.
4. R7d: route the existing hard/local paths and authenticated hint through the
   accepted single arbiter, proving one switch epoch/nonce/generation.
5. Add the no-oracle byte harness and exact zero-ownership closure for all
   success, rejection, pressure, fatal, cancellation, and shutdown paths.

Do not treat the trusted-input policy kernel as physical provenance or
production reachability. Full R7 acceptance requires the composition above.

## Historical R7/Knife16 cold-start reading

At that historical stage, read only:

1. `AGENTS.md`
2. `Rules.md`
3. `docs/current/STATUS.md`
4. `docs/tech/2026-09-04-knife16-r7-case-c-owner-server-root-architecture.md`
5. `docs/tech/2026-09-04-knife16-r7-case-c-owner-server-root-implementation-plan.md`
6. `docs/tech/2026-09-03-knife16-r7-current-only-direct-join-local-results.md`
7. `docs/tech/2026-09-03-knife16-r7-current-only-direct-join-amendment.md`
8. `docs/tech/2026-09-03-knife16-r7-current-only-join-custody-amendment.md`
9. `docs/tech/2026-09-02-knife16-r7-post-extraction-owner-root-cutover-amendment.md`
10. `docs/tech/2026-09-02-knife16-r7-hard-terminal-extraction-local-results.md`
11. `docs/tech/2026-09-02-knife16-r7-hard-terminal-extraction-architecture-amendment.md`
12. `docs/tech/2026-09-01-knife16-r7-physical-root-move-and-architecture-convergence-local-results.md`
13. `docs/tech/2026-08-26-knife16-r7-no-oracle-switch-architecture-and-plan.md`
14. `docs/tech/2026-08-26-knife16-r7-policy-kernel-and-arrival-prerequisite-local-results.md`
15. the R7 portions of the Knife16 architecture, implementation plan, and harness
   linked from the status file
16. the top current entries of `.learnings/LEARNINGS.md` and
   `.learnings/ERRORS.md`

Read archived memory only for a specific provenance question.

## Non-goals and stop rules

- Do not switch the public TUN entry during R7.
- Do not implement UDP, owner/Target, R8/R9, security qualification, or WAN as
  incidental R7 work.
- Do not run macOS TUN, VPS, WAN, throughput, or M2 yet.
- Preserve all unrelated dirty-worktree changes.
- If a repair test fails, analyze it, publish the next modification plan, and
  wait for user confirmation before another repair.

No current commit/push claim is made by this handoff; inspect Git state before
any commit. External runs and completion notification are not authorized by the
current stage gate.

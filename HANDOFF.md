# N1-NATIVE 当前实现入口

root唯一Rust Writer；全部runner终态；保dirty/raw，未commit/push。checkout `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz`，branch codex/knife14d-downlink-reap-open，HEAD bd18abb62a7e82abccc2297f642bf3920cfdd006。用户已授权诊断后自主修复；不重复确认，不调额度/并发/idle/5/15/PTO。

读AGENTS、Rules、[STATUS](docs/current/STATUS.md)、[DECISIONS](docs/current/DECISIONS.md)（R0：自主决策记此处后直接开发）、**[A 出口修订](docs/current/n1-a-exit-amendment-20261104.md)（2026-11-04 用户批准核心，现行 A 合同）**、[唯一计划](docs/current/n1-f1-implementation.md)顶部及[结果顶部](docs/current/n1-resume-20261017-evidence/b560-result.md)。A = A1 自有库存闭合清单 + A2 配置上界/preflight + A3 计数分配器仅测试 + A4 历史 Unknown 不阻塞；旧"完整整链/Direct652/653 归因"出口失效。A全闭才B再C；18/23/37、两guard CLOSED不变。

**修订后不要继续**：第三方协议栈逐分配预扣、Connection 整包接收事务扩展、为 Service 整链六处置补平行映射。这些代码保留在 dirty 中不删，待用户授权后移独立分支/feature。下一门按 STATUS 1–4 执行（A2 先）。2026-11-04 晚：STATUS 剩余(1) 已闭（D10 roles 用 `app_tier_view`），D12 failstop 已落地（D14），联合门改为 D13 组成（652+tracer 联合 + 2 串行）。放大系数 k/c 已实测并接入 preflight（D16–D18，STATUS"A2 k/c 实测"）。A1 闭合清单已闭（D19，`docs/current/n1-a1-closed-list-20261105.md`，门 `n1-a1-gate-20261105-*`）。F4096 Node 归因已测（D22 标签域：peer 独立 Foreign 线程、App/peer 轮询 Foreign；单跑 `A3_NODE_ATTRIBUTION=1 --exact --test-threads=1` 才断言）：Node 域峰值 1,721,410,626B > 硬额 816,603,436B → D23 差额；用户批准修订 (a)，`I_cfg`（`honest_bound::instance_bound`、`native_budget` 独立总内存 preflight）落地，§5.3 已闭（D25，门 `n1-a3icfg-gate-20261105-*`）；A §5.5 独立只读审查待做；Proto fork 间歇失败已闭（D20/D21：admission fixture 固定 RNG 种子，Proto 20/20）。原 `/tmp/n1-current-run.py`/`n1-d13-gate.json` 已随重启丢失；同格式重建 runner 在会话 scratch `n1-gate.py`（PREFIX NAME CMDJSON，命令取 raw `n1-d12-gate-20261105-*-commands.json`），新门证据 `n1-a2-gate-20261105-*`。2026-11-04 用户选路线 2 + 测试注入小窗口；两层水位预算已接入 Service（见 STATUS"已落地并实测"），联合 653/653。`certs/dev/server-key.pem` 不在仓库，依赖它的旧 fixture 本 checkout 恒 NotFound。以下为修订前背景，仍作证据。

多早期packet/Stock已完成实际边界：8项Full提交、9项Full不认证，exact原16-slot slab后STREAM Full精确取消；真实early ACK+PING与CID/STREAM、双key、原peer重传/另一流、duplicate/replay、原idle退出通过。TLS provider第1/4/8次真实输出错误/展开保prefix/首错/非零Q，后续DATA/CID/key不提交。新helper只请求原writer合法PING，非合成加密或虚拟成功。

共同运行时失败是旧CID重传的retire_prior_to>sequence，按RFC9000§19.15在原populate_packet编码处min即可；不改节点、retire_seq或Some lifetime范围。qualified-matrix7/9、diagnosis2/4的真实FRAME_ENCODING_ERROR保留，retransmit-green及interruption9/9；早期两编译错误不冒生产反例。

最终同源 Proto685+Doc3、Quinn97（原3 ignored）、原默认联合653/653（68.74s）、增长7/来源20/production check/diff全过；1304源码与磁盘逐项一致。双F4096/W128/Cfull各20480 JOIN/原期限charged0/nonQ，峰793411673/791117913B、硬额793542656B不变。

raw n1-coalesced-stock-final-proto/quinn及n1-coalesced-stock-final-suite-{joint,governance,production,diff}，汇总n1-coalesced-stock-acceptance.json；7变更路径/1304同源。不要因文档重跑。

（修订前的"下一：按 prepare_factory→…整链映射六处置"已失效。）A1 收尾时 native_complete/terminal.rs、send_inventory.rs、direct/backing_pressure.rs及hard_native_*loaded_*仅用于引用 A1 闭合清单条目证据，逐assert核实际，不按名字签。旧Direct/额外lost包形保Unknown（A4）；packet_review平台automatic review拒绝，独审改走另一只读途径，自审不代签。

只offline cargo/真实loopback。无外部/TUN/VPS/WAN/M2/commit/push/邮件/新key，公开hard CLI拒绝/TUN legacy保持。复用 `/tmp/n1-current-run.py PREFIX JSONCMD`、`/tmp/n1-current-suite.py PREFIX /tmp/n1-leased-repaired-suite.json`；raw根 `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4`，Proto下n1-capacity-proto-run/Cargo.toml、Quinn下n1-a1-rejection-cause-quinn-run/Cargo.toml，CARGO_TARGET_DIR=target/CARGO_INCREMENTAL=0。runner活跃冻结全部Rust；Proto child过再联合，wrapper0不冒cargo通过；核6 maps/磁盘、不覆盖prefix、不拼epoch。

**2026-11-05 A 关闭（D27–D29，门 `n1-a-o1o4-gate2-20261105-*`）**：O1/O4 已闭；联合门为单一联合 661 项（命令见 raw `n1-a-o1o4-gate2-20261105-joint-commands.json`，D13 串行组取消，isolated 仅附加证据）。**下一：阶段 B（独立会话）**。开放项 O2、O3、O5、O6。

**2026-11-05 B 开始（D30–D33）**：B1 已闭（`native_rebuild.rs`，联合门 662 项=原 661+该 tracer，命令见 raw `n1-b1-gate-20261105-joint-commands.json`）。下一 B2（计划顶部"B 出口"表）。

## 新会话交接（2026-11-06，N1-NATIVE 阶段 B，B1–B7 已闭；下一 B8 判定 + 独立审查）

```text
继续 N1-NATIVE 阶段 B 收尾。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（D1–D43）→ HANDOFF/TODO → 计划 docs/current/n1-f1-implementation.md 顶部"B 出口"。
已接受：B1–B7（D31,D34–D38,D41–D43），全部无生产改动；rebuild.rs sha 0bdc64da…。tracer：tuic_service/tests/native_rebuild.rs（B1–B3）、native_rebuild_fatal.rs（B4）、native_rebuild_cancel.rs（B5）、native_rebuild_loaded_stop.rs（B6）、native_rebuild_loaded_cut.rs（B7，230 行）。
下一：(1) B8 判定记录：重叠 AUTH/第二 endpoint 仅当串行 C+D+H 超原 5s 才必需；loopback 实测≈0.1s（D31）→ 记录为 Deferred 交 C 高 RTT 串行门判定（写 D44，计划表 B8 行注明判定依据与 C 的测量公式），不写代码。(2) 独立只读审查（新只读会话/子代理）：按计划"B 出口"核 B1–B7 tracer 实际断言 vs 六处置（成功/拒绝/压力/fatal/取消/shutdown）与冻结语义，产出审查文档（仿 n1-a-review-20261105.md），无 FAIL 即 B 完成证据 → STATUS 宣告 B 关闭，下一阶段 C（独立会话）。
冻结语义：同前（后继配置拒绝归 Node disposition；resume Err typed 非 Q；Service 终态 used≠0→Q；Expired→TimedOut 非 Q；旧 drain Q 仅持 cut Device 83,255B；stop 持 cut 保原期限 rebuilds 0；stop 于后继握手 rebuilds 1；终 stop 序 Device JOIN→Drop→driver_complete→Node drain；B7：cut 前旧 business 结清、旧 Node 仅空闲残差 (2,300,428)）。
门：runner 在 /private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/bed87e46-21aa-4bd2-a3ed-f24b66e0dd0c/scratchpad/{n1-gate.py,run-gate.sh,guard.sh}；最新门 raw /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-b7-gate-20261106-*（联合 675 项，命令 n1-b7-gate-20261106-joint-commands.json）。
规则：自主决策记 D44+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；无 commit/push、外部/TUN/VPS/WAN/吞吐/M2、新 key、邮件；公开 hard CLI 拒绝/TUN legacy 不变。O2/O3/O5/O6 不阻塞 B。
```

## 新会话交接（2026-11-06，F0 已闭 D46–D50；下一阶段 C）

```text
开始 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（D1–D50；重点 D44 B8 交 C、D45 F1–F7、D46–D50 F0）→ HANDOFF/TODO → 计划 docs/current/n1-f1-implementation.md 顶部 "C" 段。
已接受：A、B、F0。F0 修复：quinn-proto connection/sent_history.rs（ACK-only 不入冷环，release_untracked），tests/admission/{sent_ack_only.rs(新), sent_capacity.rs(修订)}；observe_window.rs::progress_window 仅用于 native_complete/pressure.rs 恢复阶段。
门组成：联合 678（B7 联合 cmd + `--skip native_complete::capacity::original_`）+ capacity-serial（uptime + 两项 F4096 --exact --test-threads=1 + counting_alloc + uptime，两者都必须通过，并记录 load 与 stop 余量）+ Proto/Quinn/honest/isolated/governance/production/diff。命令 raw /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-f0r-gate-20261106-*-commands.json；runner 为 n1-gate.py/guard.sh（/private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/bed87e46-21aa-4bd2-a3ed-f24b66e0dd0c/scratchpad/）；负载复现：raw n1-f0-diag-20261106/par.sh。
C 前置/开放：F7 高 RTT 时间公式 T=C+3·PTO+H ≤5s（D44 B8 由此判定）；F1/F2 Node disposition 注入（属 Node owner，需另授写轴）；F3 tick_until；F4/F5 取消/带载矩阵；新风险：ordinary=1024 ae 在途上限（200ms 时约 60Mbit/s）、ACK-only 以外的 rate×RTT 跨度，C 吞吐门须测量。
规则：自主决策记 D51+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；无 commit/push、外部/TUN/VPS/WAN/吞吐/M2 运行、新 key、邮件，除非 STATUS 明确就绪且用户授权；修复测试意外失败即停并提方案。
```

## 新会话交接（2026-11-06，C1+C2 已闭 D51–D54；下一 C3）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（D1–D54；重点 D51 容量、D52/D53 时间公式与 B8 判定、D54 门）→ HANDOFF/TODO → 计划 docs/current/n1-f1-implementation.md 顶部 "C 出口"（唯一清单 C1–C9）。
已接受：C1（tuic_service/tests/native_rebuild_high_rtt.rs，测试专用 RttRelay FIFO 单向延迟，无生产改动；公式 C∈[r,r+25ms]、D′∈[3(r+26ms),3(3r+25ms)]+300ms、H∈[2.5r,3.5r+300ms]）；C2（D53：无丢包 r≤390ms 串行恒≤5s，B8 不需要）。
下一 C3：给 RttRelay 加确定性丢包（例如按方向第 k 个包丢弃，计数，有界），r=200ms 下串行重建/stop：先写可证伪预测（握手包丢失加 PTO：Initial 用 initial_rtt 333ms 起，后续 srtt+4var+0；关闭包丢失不影响客户端 3·PTO 排空但影响服务端 closed() 观测——改用 relay 侧时间戳而非 server closed），再 RED/GREEN；同轴补 F4（stop 期限断言、握手后 AUTH 前 stop）、F5（带载不退休超期）。C5 F3 tick_until 可顺手；C4 需 Node owner 写轴授权（勿自行）；C6 离线上行 Bulk 观测 sent-history 跨度峰值验证 C-I8。
门组成：联合 680（命令 raw /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-c1-gate-20261106-joint-commands.json；新增 tracer 须加过滤器）+ capacity-serial（两者必过；记录 load 与 stop 余量——D54 余量已降至≈27–33%，负载 5.78）+ Proto/Quinn/honest/isolated/governance/production/diff；runner n1-gate.py/guard.sh 在 /private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/6a7db04e-6643-46db-8418-2be2ec8ca7e5/scratchpad/（输入 c1g/*-in.json；副本 /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-c1-20261106/）。
规则：自主决策记 D55+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；探针 guard.sh 输出写文件；每语句只取一次 ledger()；增长门；无 commit/push、外部/TUN/VPS/WAN/吞吐/M2、新 key、邮件；公开 hard CLI 拒绝/TUN legacy 不变；修复测试意外失败即停并提方案。C8 外部门须停下给一次性清单等用户授权。
```

## 新会话交接（2026-11-06，C3 丢包片+F4 已闭 D55–D59；下一 F5）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（D1–D59；重点 D55 丢包预测、D56 实测、D57 B8 重判、D58 F4、D59 门）→ HANDOFF/TODO → 计划 docs/current/n1-f1-implementation.md 顶部 "C 出口"（唯一清单 C1–C9）。
已接受（均无生产改动）：C1/C2；C3 丢包片：native_rebuild_high_rtt.rs 中 RttRelay+Tap（fixed (gen,Dir,ordinal) 丢包、blackhole、有界日志 8192），serial_rebuild_behind(rtt, limit, Loss) → (timing, log)；native_rebuild_lossy.rs 4 测（P1/P2 单握手丢包 +PTO_i 999ms；P3 Initial+两探针 +3·PTO_i，T 4.75–5.18s 仅记录；P4 旧 close 黑洞）。F4：native_rebuild_cancel.rs stop_before_successor_resume(Hold::{Handshake,Auth})，新 15s stop 期限断言，Auth=peer 换 0-uni ServerConfig 使 authenticate 卡在 open_uni。
待用户：B8 是否打开（D57：取决于产品 RTT×握手丢包目标；现串行包络 无丢 r≲390ms / 单丢 r≲312ms / 三连丢 r≲152ms）。
下一 F5：带载（Chunk+Permit）旧 peer 不退休直到原业务期限的 rebuild（paused clock；参照 native_rebuild_fatal.rs B4b hold_cut + native_rebuild_loaded_cut.rs B7），先写预测（Expired⇒typed TimedOut、无后继 accept/DATA、rebuilds 0、旧流 JOIN/receipt 与 Expired 并存、used 0 非 Q），RED 变异再 GREEN。然后 C5（native_rebuild_fatal.rs resume-panic 测试 drive_successor→tick_until）、C6（中继下上行 Bulk 观测 sent-history 跨度峰值验证 C-I8）。C4 需 Node owner 写轴授权（勿自行）。
门组成：联合 685（命令 raw /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-c3-gate-20261106-joint-commands.json；新增 tracer 须加过滤器）+ capacity-serial（两者必过；记录 load 与 stop 余量，D59 29–34% @load 7.04）+ Proto 687/Quinn 97/honest 13/isolated 2/governance 7+20/production check/diff；runner n1-gate.py、guard.sh、c3-gate.sh 与 *-in.json 副本在 /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-c3-20261106/（c3-gate.sh 内路径指向旧 scratchpad，复制后改 SP）。
规则：自主决策记 D60+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；cargo test 多过滤器须放在 -- 之后；探针输出写文件；每语句只取一次 ledger()；增长门；无 commit/push、外部/TUN/VPS/WAN/吞吐/M2、新 key、邮件；公开 hard CLI 拒绝/TUN legacy 不变；修复测试意外失败即停并提方案。C8 外部门须停下给一次性清单等用户授权。约 50% 上下文时收尾当前片并更新本段。
```

## 新会话交接（2026-11-06，F5 跑过 D60–D61；待用户批准 D61 修复）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（D1–D61；重点 D60 预测、D61 证伪+发现+修复提案）→ HANDOFF/TODO → 计划 docs/current/n1-f1-implementation.md 顶部 "C 出口"（唯一清单 C1–C9）。
F5 结果（仅测试）：旧 peer 持流不退休 ⇒ 业务永不结算，原期限到即 Incomplete+粘性 Phase Q（同单代 StopHeld）。D61-S 产品发现：OpenDispatch::poll_flows（flow_custody/transaction.rs）32 槽分块重扫 4096 槽，take_control 耗尽返回 OpenEvent::Yield ≈2万/s，loop 永不 park；debug CPU 稳态带载 0.28、持有排空与 Q 后 0.78（Q 后无限）。tracer tuic_service/tests/native_rebuild_loaded_expiry.rs（#[ignore]，--include-ignored 跑：处置 GREEN，CPU≤0.10 RED）；B7 加载段已抽为 native_rebuild_loaded_cut::load_original（B7 2/2 绿）。
若用户批准 D61 修复：写轴 flow_custody root（transaction.rs 的 poll_flows/opens 插删点 + shutdown census 冷路径双向核对）；R-a 占用槽位图 [u64;64] 只访问占用槽、空槽不耗 control；R-b 无到期流（physical_check next>now）时 Pending 不自唤醒，依赖既有 5ms 定时器。先 F5 RED（已有）→GREEN，记录稳态 CPU 前后，再 F5 改 paused clock、去 ignore、入联合过滤器；全门（联合 685+F5、capacity-serial、Proto 687/Quinn 97/honest 13/isolated 2/governance 7+20/check/diff）。transaction.rs 若在增长 grandfather 清单，先查限额。修复测试意外失败即停并提方案。
若未批准：做 C5（native_rebuild_fatal.rs drive_successor 改为每 tick 检查 done，或 tick_until 加 accept 臂）、C6（需 quinn-proto 跨 crate 可见的 sent-history occupied 观测——现 event_capacity_probe 为 pub(crate)，需 feature 门控接口，属第三方写轴，先提方案）。C4 需 Node 写轴授权；B8 待用户（D57）。
门组成与 runner：见上一段（n1-c3-20261106/ 下 n1-gate.py、guard.sh、c3-gate.sh）。原始 F5 证据 /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-f5-20261106/。
规则：自主决策记 D62+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；cargo test 多过滤器放 -- 之后；探针输出写文件；生产探针必须恢复并 sha 校验；无 commit/push、外部/TUN/VPS/WAN/吞吐/M2、新 key、邮件。约 50% 上下文收尾并更新本段。
```

## 新会话交接（2026-11-06，D61 修复已获用户批准 D62；实施待新会话）

```text
继续 N1-NATIVE 阶段 C（独立会话）：实施 D61 修复（用户 "Approve D61"，D62）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（D1–D62；重点 D61 发现/提案、D62 批准与附加验收）→ HANDOFF/TODO → 计划 n1-f1-implementation.md "C 出口"。
问题：OpenDispatch::poll_flows（src/client_tun/compatibility_runtime/flow_custody/transaction.rs，~1150–1345）以 32 槽分块重扫 FLOW_SLOT_LIMIT(4096) 槽，take_control 耗尽→OpenEvent::Yield（≈2万/s），!wrapped 自唤醒；loop 永不 park。debug CPU 占比稳态带载 0.28、持有排空/Q 后 0.78。
授权写轴：flow_custody root（poll_flows、opens 槽插入/移除点、shutdown census 冷路径双向核对）。R-a 占用槽位图 [u64;64]（同 root 锁下维护，只访问占用槽、空槽不耗 control）；R-b 无到期流（physical_check next>now）时 Pending 不自唤醒，依赖既有 5ms loop 定时器。transaction.rs 1379 行（上限 1500，非 grandfather）⇒ 位图放私有子模块并写模块文档。不得 DATA 热路径全局锁/逐包分配。
验收：(1) F5 tracer tuic_service/tests/native_rebuild_loaded_expiry.rs 现 #[ignore] RED（CPU 0.78）→ GREEN ≤0.10；若 auto-advance 可用改 paused clock，去 ignore，联合加过滤器 `tuic_service::tests::native_rebuild_loaded_expiry::`；(2) 无丢唤醒 tracer：R-b 停泊后经四条外部路径（新流发布、relay 数据到达、close/reset、stop 请求）使流就绪，各须在一个 5ms tick+余量内被观测；变异去掉一条路径唤醒/登记 ⇒ RED；(3) 测稳态带载 CPU 前后（前 0.28）记 D；(4) 全门新前缀：联合 685(+F5)、capacity-serial、Proto 687、Quinn 97、honest 13、isolated 2、governance 7+20、production check、diff；生产已改 ⇒ 重跑归因并确认 I_cfg 上界；位图 +512B 冷布局 ⇒ 若在范围更新 src/quic/honest_bound.rs instance_bound。
先测/先 RED：F5 已 RED；无丢唤醒 tracer 须在修复前写好（修复前应 GREEN 因自唤醒——记录此事实，再以变异证明判别力）。修复测试意外失败即停并提方案。
门 runner：/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-c3-20261106/（n1-gate.py、guard.sh、c3-gate.sh，复制后改 SP）。F5 证据 …/n1-f5-20261106/。
之后：C5（drive_successor 每 tick 检查 done）、C6（需 quinn-proto feature 门控 sent-history 观测，先提方案）。C4 需 Node 写轴授权；B8 待用户（D57）。
规则：自主决策记 D63+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；cargo test 多过滤器放 -- 之后；探针输出写文件；生产探针恢复并 sha 校验；无 commit/push、外部/TUN/VPS/WAN/吞吐/M2、新 key、邮件。约 50% 上下文收尾并更新本段。
```

## 新会话交接（2026-11-06，D61 R-a/R-b 已实施未提交 D63；F5 残差待批准方案）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D61/D62/D63）→ HANDOFF/TODO。
已做（D63）：SlotTable（transaction/slot_table.rs，[u64;66]，FLOW_SLOT_LIMIT=4224）+ poll_flows 只访问占用槽、过表尾 Pending 不自唤醒；census 双向占用核对；无丢唤醒 tracer native_park_wakeup.rs 修复前 GREEN（空闲 ~140 polls/tick）修复后 GREEN（~1.1）。
停在：F5 CPU 0.78→0.12–0.16，仍 > 0.10 ⇒ 按规则停，D63 PLAN 待用户确认：(1) 归因探针（census 5ms 全表、drain、service_flow、同线程 QUIC driver、无流空闲基线）；(2) 依归因降 census 频率/O(66) 占用核对，或修订 F5 阈值为 polls/tick。之后：变异 3 项、F5 去 ignore（paused clock 若可）、稳态带载 CPU 前后、全门（联合 685+2 新过滤器 slot_table / native_park_wakeup、capacity-serial、Proto/Quinn/honest/isolated/governance/production/diff）、honest_bound instance_bound +528B。
注意：certs/dev/server-key.pem 缺失（未跟踪）⇒ 门外 33 测试失败，不得生成新 key。
规则：自主决策记 D64+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；无 commit/push、外部运行、新 key、邮件；修复测试意外失败即停并提方案。
```

## 新会话交接（2026-11-06，D64 归因完成 + census 50ms；F5 Permit 残差待用户选 A/B）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D61–D64）→ HANDOFF/TODO。
已做：D63 SlotTable+poll_flows 停驻；D64 归因（探针已还原、sha 校验）：census 5ms 占 ≈0.09 → SHUTDOWN_CENSUS_CADENCE=50ms（live>0；live==0 每轮不变），census 0.013。F5 Chunk 过；Permit draining 0.114 仍 > 0.10；稳态加载基线 0.085–0.100（≈250µs/驱动 poll 未归因）。
待用户：(A 推荐) F5 判据改为测试侧 Service polls/5ms tick ≤2（GateWake 包装，实测 1.0–1.64，修复前 ≈140），CPU 仅打印+粗界 ≤0.25；或 (B) 先归因稳态每 poll 基础成本（需 flow_custody 外写轴）。
之后：变异（bit-on-put、stopping 忽略、physical_check 不重武装）、F5 去 ignore（能则 paused clock）+ 联合过滤器（含 slot_table、native_park_wakeup）、全门（joint/capacity-serial/Proto/Quinn/honest/isolated/governance/production/diff）、honest_bound instance_bound +528B、C5、C6（先提方案）。
注意：certs/dev/server-key.pem 缺失 ⇒ 门外 33 测试失败，不得生成新 key。规则：决策记 D65+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；无 commit/push、外部运行、新 key、邮件；修复测试意外失败即停并提方案；仅父会话开会话。
```

## 新会话交接（2026-11-06，D61 修复 D63–D66 完成待全门结果；未提交）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D63–D66）→ HANDOFF/TODO。
已做：SlotTable+poll_flows 停驻（D63）；census 50ms（D64）；F5 判据 wakes/5ms tick ≤2 + CPU ≤0.25，去 ignore（D65）；变异 M1/M2 RED、M3 存活（D66 开放项 C-M3）；I_cfg +528B（ROOT_GEOMETRY 第 5 项 → InstanceShape.flow_index）。
全门：/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d66-gate-20261106/gate-*.{log,-commands.json}（done 文件=跑完）；runner 在 scratchpad d66g（joint 加 4 过滤器）。若未记录结果：读各 commands.json，记 D67。
开放：C-B（稳态 ≈250µs/唤醒 未归因，需新写轴）；C-M3（physical_check 周期复探无判别 tracer）；然后 C5、C6（先提方案）。
注意：certs/dev/server-key.pem 缺失 ⇒ 门外 33 测试失败，不得生成新 key。规则：决策记 D67+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；无 commit/push、外部运行、新 key、邮件；修复测试意外失败即停并提方案；仅父会话开会话。
```

## 新会话交接（2026-11-06，D67 联合门 RED：重建/successor DATA 5s 窗口回归，待批准 D67 计划）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D63–D67）→ HANDOFF/TODO。
状态：D63–D66 已实施（SlotTable 停驻、census 50ms、F5 判据 wakes/tick、I_cfg +528B）。全门除联合外全绿；联合 679/689，10 个重建/successor 测试 "DATA within 5s" 间歇失败（starved≈200ms），隔离复现；恢复自唤醒更糟 ⇒ 非 R-b 丢唤醒；源差异全为本修复。
下一（待用户批准 D67 计划）：(1) 重建 pre-D63 生产基线跑失败模块×3 证明因果；(2) 临时探针定位会话线程 >10ms 同步 poll；(3) 修复后联合×2。
证据：/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d66-gate-20261106/（gate-*、rerun10.txt、bisect-selfwake.txt）。开放：C-B、C-M3、C5、C6。
注意：certs/dev/server-key.pem 缺失 ⇒ 门外测试失败，不得生成新 key。规则：决策记 D68+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；无 commit/push、外部运行、新 key、邮件；修复测试意外失败即停并提方案；仅父会话开会话。
```

## 新会话交接（2026-11-06，D68：D67 已批准「先探针」；未开始执行）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D63–D68）→ HANDOFF/TODO。
现状：D63–D66 生产/测试改动在工作树（SlotTable 停驻、census 50ms、F5 wakes/tick 判据、I_cfg +528B）。D67 联合门 679/689：10 个重建/successor 测试 "DATA within 5s" 间歇失败（starved≈200ms），隔离复现；恢复 R-b 自唤醒更糟；源差异全为本修复。
D66 状态 sha（探针还原基准）：先 `shasum -a 256` 记录 src/client_tun/compatibility_runtime/flow_custody/{transaction.rs,transaction/slot_table.rs,rearm/shutdown.rs} 与 src/client_tun/legacy_event_loop/{driver.rs,activity.rs}、tuic_service.rs、src/quic/honest_bound.rs 再改。
用户顺序（D68）：(1) 先临时 cfg(test) 探针：会话线程任一 poll >10ms 即打印所在 flow_custody 调用（poll_flows 每次 visits/next_occupied cursor/take_control yield、service_flow、census、drain）+ 耗时 + slots/live；在 driver.rs execution.poll 外包裹计时（参考 D64 探针做法）；用 guard.sh（…/n1-c3-20261106/guard.sh）跑 native_rebuild_fatal::hard_native_resume_error、native_rebuild_lossy::、native_rebuild_high_rtt::、native_rebuild_loaded_cut::、native_rebuild_loaded_stop::、native_chunk_loaded_reset::hard_；还原并 sha 校验。(2) 仅当探针未定位：在 raw/scratch 下复制工作树（排除 target/）重建 pre-D63，失败模块×3；绝不原地。(3) 修复后联合门×2 全绿再声明，然后其余全门（runner：/private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/33c8cbdf-5489-4684-88b0-907407328791/scratchpad/d66g/gate.sh，改输出前缀）。修复意外失败即停并提方案。
证据：/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d66-gate-20261106/。开放：C-B、C-M3、C5、C6。
注意：certs/dev/server-key.pem 缺失 ⇒ 门外测试失败，不得生成新 key。规则：决策记 D69+；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；无 commit/push、外部运行、新 key、邮件；监控磁盘；仅父会话开会话。
```

## 新会话交接（2026-11-06，D69–D70：根因已定位，修复尝试 1 失败，待用户确认 D70 计划）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D63–D70）→ HANDOFF/TODO。
现状：工作树 = D66 状态（17 文件 sha 已校验，清单 /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d69-probe-20261106/d66-sha.txt）；无探针残留。
根因（D69）：native dispatcher remote_eof（FlowRoles::closed）→ drive Closing → physical_flow.rs Closing 分支 uplink_tx=None，而最后 DATA 仍在 relay lane → remote_payload StaleRelayReset 未记账 → finish_event 失配 → mirror Q。D63 只是暴露。
D70：Closing 处 published 计数延迟守卫失败（patch 同目录 fix-attempt1-closing-guard.rs.txt），已还原。待用户确认计划：(1) Closing 处临时探针 published()/dispatcher 状态/mirror.data_messages（还原+sha）；(2) 选准确次序见证或改为 native reader 经 lane 发 Closed（不得静默丢回包字节）；(3) 确定性 tracer（lane 暂停，dispatcher 先 EOF）RED→GREEN；(4) focused 6 模块×3、联合×2、全门（runner …/33c8cbdf-…/scratchpad/d66g/gate.sh 改前缀）。
注意：server-key.pem 缺失（门外失败，不得生成）；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；zsh 不分词，多文件命令用 bash -c；决策记 D71+；无 commit/push、外部运行、新 key、邮件；修复意外失败即停并提方案；仅父会话开会话。开放 C-B、C-M3、C5、C6。
```

## 新会话交接（2026-11-06，D71：D70 计划已批准；未开始执行）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D63–D71）→ HANDOFF/TODO。
现状：工作树 = D66 状态（17 文件 sha 清单 /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d69-probe-20261106/d66-sha.txt，先 `bash -c "shasum -a 256 -c <清单>"` 确认）。根因见 D69；尝试 1 失败见 D70（patch 同目录）。
用户已批准 D70 计划（D71），按序：(1) physical_flow.rs `if flow.stage == FlowStage::Closing {` 处临时 cfg(test) 探针：FlowRoles::published()（Some/None, messages, bytes）、native dispatcher/monitor Role 状态、mirror.data_messages/data_bytes、remote_payload StaleRelayReset 与 finish_event 失配标记；跑 focused 6 模块（native_rebuild_fatal::hard_native_resume_error native_rebuild_lossy:: native_rebuild_high_rtt:: native_rebuild_loaded_cut:: native_rebuild_loaded_stop:: native_chunk_loaded_reset::hard_，guard.sh …/n1-c3-20261106/guard.sh）；还原 + sha 校验。(2) 据证据选修复，父会话倾向：native reader 的 remote_eof 经 relay lane 在其 DATA 之后作为 RelayEvent::Closed 投递（构造保序、对齐 legacy），而非计数守卫；记录选择与理由。禁止静默丢回包字节。(3) 先写确定性 tracer：暂停 lane，让 reader 先 EOF，再放行 ⇒ App 收到字节且无 Q；在当前树 RED，修复后 GREEN。(4) focused×3、联合×2 全绿、再其余全门（capacity-serial、Proto、Quinn、honest、isolated、governance、production、diff；runner /private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/33c8cbdf-5489-4684-88b0-907407328791/scratchpad/d66g/gate.sh 改输出前缀），更新 STATUS/DECISIONS。
注意：zsh 不分词（多文件命令用 bash -c）；heredoc 含反引号用 python 写；server-key.pem 缺失（门外失败，不得生成）；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；决策记 D72+；无 commit/push、外部运行、新 key、邮件；修复意外失败即停并提方案；监控磁盘；仅父会话开会话。开放 C-B、C-M3、C5、C6。
```

## 新会话交接（2026-11-06，D72–D73：修复已入树，focused 门 loaded_reset 仍红，待用户确认 D73 计划）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D69–D73）→ HANDOFF/TODO。
现状：工作树 = D66 + D72（sha 清单 /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d72-20261106/d72-sha.txt，先 `bash -c "shasum -a 256 -c <清单>"`）。D72 守卫在 physical_flow.rs hard-source Closing 块之前；tracer tuic_service/tests/native_eof_lane_order.rs（RED→GREEN 已证）；harness 接收保持 = test_bridge.rs relay_hold + window.rs test_receive_held + driver.rs poll_fn 内 `|| test_receive_held()`；observation.rs flow_close_for_test。
阻塞（D73）：native_chunk_loaded_reset::hard_native_ordered_chunk_loaded_reset_preserves_source_first_and_receipt 于 :378 report.unwrap() Elapsed（本地 reset 负载下未按 original_deadline 结算），2/9 → 3/3。待用户确认 D73 计划：(1) 临时 cfg(test) 探针（D72 守卫每次 return true、hard-source Closing/Q 分支、remote_payload Stale/TerminalNoSend、失败时 flow 状态），还原 + sha；(2) 仅 loaded_reset ×5 A/B：守卫开/强制关、test_receive_held 开/去 ⇒ 归因；(3) 若 published()==None 分支延迟则限为 Some，否则在其 seam 修并加 tracer；(4) focused×3（命令 /private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/993c8375-5a71-4d8d-b1ae-1f855e4ab7d5/scratchpad/focused.sh OUT native_eof_lane_order，等价于：guard.sh 1500 OUT cargo test --offline --features harness --lib -- native_rebuild_fatal::hard_native_resume_error native_rebuild_lossy:: native_rebuild_high_rtt:: native_rebuild_loaded_cut:: native_rebuild_loaded_stop:: native_chunk_loaded_reset::hard_ native_eof_lane_order），联合×2（runner /private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/33c8cbdf-5489-4684-88b0-907407328791/scratchpad/d66g/gate.sh 改前缀，并把 native_eof_lane_order 加入 joint 过滤器），再其余全门；更新 STATUS/DECISIONS（D74+）。
注意：zsh 不分词（多文件用 bash -c）；heredoc 含反引号用 python；server-key.pem 缺失（UdpPair::server_config 测试门外失败，不得生成；tracer 用 ServiceProfile 内存证书）；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；无 commit/push、外部运行、新 key、邮件；修复意外失败即停并提方案；仅父会话开会话。开放 C-B、C-M3、C5、C6。
```

## 新会话交接（2026-11-06，D74：A/B 已排除 D72；loaded_reset 独立 seam 待探针）

```text
继续 N1-NATIVE 阶段 C（独立会话）。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D69–D74）→ HANDOFF/TODO。
现状：工作树 = D66 + D72（先 `bash -c "shasum -a 256 -c /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d72-20261106/d72-sha.txt"`，8/8 OK）。用户已批准 D73 计划（D74 已执行其第 2 步）。A/B（单独 ×5）：D72 1/5、守卫关 1/5、hook 去 2/5，失败均为 native_chunk_loaded_reset.rs:378:29 report.unwrap() Elapsed ⇒ 与 D72 守卫/tracer hook 无关，独立 seam。
下一（按序）：(1) 临时 cfg(test) 探针（还原 + sha vs d72-sha.txt）：在 :378 超时处转储 flow stage/close_reason/producer_stopped/mirror(data,transferred,discarded,event_in_flight)/roles join+running/provider phase/ledger q/stopping/census 原因，定位截止时哪个结算门未完成（release_flow、roles.settled、dispose_pending、census）；单独跑 `cargo test --offline --features harness --lib -- native_chunk_loaded_reset::hard_` ×5 以捕获 1–2 次失败（guard.sh 600 OUT …）。(2) 在该 seam 修并加确定性 tracer（RED→GREEN）。(3) focused×3（/private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/993c8375-5a71-4d8d-b1ae-1f855e4ab7d5/scratchpad/focused.sh OUT native_eof_lane_order），联合×2（/private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/33c8cbdf-5489-4684-88b0-907407328791/scratchpad/d66g/gate.sh 改前缀，joint 加 native_eof_lane_order），其余全门；STATUS/DECISIONS D75+。修复意外失败即停并提方案。
注意：zsh 不分词（多文件 bash -c）；heredoc 含反引号用 python；server-key.pem 缺失（不得生成）；仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；无 commit/push、外部运行、新 key、邮件；监控磁盘；仅父会话开会话。开放 C-B、C-M3、C5、C6。
```

## 新会话交接（2026-11-06，D75：D61 修复全门绿，未提交）

```text
继续 N1-NATIVE 阶段 C。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D69–D75）→ HANDOFF/TODO。
现状：工作树 = D66 + D72 + D75（sha：/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d75-20261106/d75-sha.txt，physical_flow.rs + native_eof_lane_order.rs；其余 d72-sha.txt 6 项不变）。全门绿（/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d75-gate-20261106/gate-*）。
下一：用户决定是否提交 D63–D75（未授权 commit/push）；然后 C5/C6。开放 C-B、C-M3；B8 待产品 RTT×握手丢包目标（D57）。
注意：仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；server-key.pem 缺失不得生成；无外部运行/新 key/邮件；joint runner 副本 /private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/975b111b-c69c-4d2f-a046-1442b23337ca/scratchpad/d75g/gate.sh（含 native_eof_lane_order）。
```

## 新会话交接（2026-11-07，D76–D79：C5/C-M3 全门绿，C6 提案待批，未提交）

```text
继续 N1-NATIVE 阶段 C。checkout /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz，branch codex/knife14d-downlink-reap-open，未 commit。
冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D72–D79）→ HANDOFF/TODO。
现状：工作树 = D75 + D76（native_rebuild_fatal.rs）+ D77（native_physical_reprobe.rs + tests/mod.rs 一行）；sha /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d76-20261107/d76-sha.txt。全门绿 n1-d76-gate-20261107/gate-*。
下一：用户批准/修改 C6 提案（docs/current/n1-c6-proposal-20261107.md，推荐路线 B）后实施 C6 → C7 → C9。C4/C8/C-B/B8 均待授权或产品决策。提交 D63–D79 待用户。
注意：仅 offline cargo（--features harness，CARGO_INCREMENTAL=0，CARGO_TARGET_DIR=target）；server-key.pem 缺失不得生成；无外部运行/新 key/邮件；joint runner 副本 /private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/362001a7-468f-410b-b863-27b373b7843a/scratchpad/d76g/gate.sh（含 native_physical_reprobe）。
```

## 新会话交接（2026-11-07，D80–D82：C6 已实施，预测证伪停止，未提交）

冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D79–D82）→ HANDOFF/TODO。
已做：C6 路线 B 实施（quinn-proto `sent-history-observe`，`connection/sent_history/observe.rs` + 单测；mini_vpn `harness` 经 quinn 转发；测试叶 `tuic_service/tests/native_uplink_span.rs`，三测 #[ignore]，单独进程 `--ignored --exact --test-threads=1` 运行）。sha：/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d80-20261107/d80-sha.txt；前态 pre-c6-sha.txt。
结果：r=400ms 冻结预测证伪（peak 949<960、blocked 0、2.57MB/s），r=800ms 同（954、1.29MB/s），r=50ms 14.4MB/s。约束 = 夹具 send_window 1MiB，不是环。停止规则生效：M-a/M-b、全门、uplink-span 组均未跑。
下一（需用户选）：P1 叶注入 send_window ≥4MiB 测试形状（夹具 service 路径需接受 shape 与 native_reserve_for），D79 预测原样重测 → M-a/M-b → 全门 + uplink-span；或 P2 接受 D81 并以新预测 C-I8w 改写断言。C7 依 C6；C4/C8/C-B/B8 仍待授权/产品决策；提交 D63–D82 待用户。禁止：外部/TUN/VPS/WAN/M2、commit/push、新密钥。

## 新会话交接（2026-11-07，D83–D85：C6 P1 重测再证伪，停止，未提交）

冷启动：AGENTS.md → Rules.md → STATUS → DECISIONS（重点 D79–D85）→ HANDOFF/TODO。
已做：叶 `native_uplink_span.rs` 注入 `c6_shape()`（send_window 4MiB）；`fixture/native.rs` Hard 预留 = (32<<20)+native_reserve_for(&cfg.transport_shape())。sha /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d80-20261107/d84-sha.txt。
结果：环约束成立（peak 1025、blocked>0、rate×RTT≈1.5MB 两 RTT 一致），速率 400ms 3.53MB/s、800ms 1.80MB/s 超 C-I8(P=1230) ×1.18；50ms peak 1025 违预测。M-a/M-b、全门、uplink-span 未跑。
下一（需用户选）：Q1 P=1472 无 +ack 项重新冻结（400ms ≤3.77MB/s、800ms ≤1.88MB/s、50ms 仅记录）→ M-b、M-a → 全门 + uplink-span；或 Q2 锁 Safe1200 保留 D79 数字。C7 依 C6。禁止：外部/TUN/VPS/WAN/M2、commit/push、新密钥。

## 新会话交接（2026-11-07，D86–D88：C6 已闭，全门绿，未提交）

Knife/阶段：N1-NATIVE stage C。冷启动：AGENTS.md → Rules.md → docs/current/STATUS.md → docs/current/DECISIONS.md（重点 D79–D88）→ HANDOFF/TODO。
已接受位置：C5、C-M3、C6 已闭（C6 = necessary-only：上行 ≤ 1024·1472/RTT，M-b ×2.01 证实环为约束者）。全门 `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d86-gate-20261107/gate-*` 全绿；终态 sha /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d80-20261107/d88-sha.txt。
C6 写范围：quinn-proto feature `sent-history-observe`（`connection/sent_history/observe.rs` + tests/observe.rs + 3 处 cfg re-export）、quinn/mini_vpn feature 转发（harness）、叶 `tuic_service/tests/native_uplink_span.rs`（3 测 #[ignore]，gate 组 uplink-span 以 `--ignored --exact` 单进程运行）、`fixture/native.rs` Hard 预留按 `cfg.transport_shape()` 计算、`RttRelay.dropped` pub(super)。
下一（已授权、离线）：C7 原 full matrix 默认并行 + 固定 18 联合；随后 C9 独立评审。gate runner 模板 /private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/362001a7-468f-410b-b863-27b373b7843a/scratchpad/d86g/gate.sh（新前缀）。
仍待授权/决策：C4（Node 写轴）、C8（外部）、C-B（新写轴）、B8（产品 RTT 目标 D57）、提交 D63–D88。禁止：外部/TUN/VPS/WAN/M2、commit/push、新密钥、server-key.pem。规则：离线 cargo（--features harness、CARGO_INCREMENTAL=0、CARGO_TARGET_DIR=target），失败先停并给计划，日志 D89+。

## 新会话交接（2026-11-07，D89–D90：C7 BLOCKED-ENV，待 K1/K2 决策，未提交）

Knife/阶段：N1-NATIVE stage C。冷启动同上，重点 DECISIONS D86–D90。
已接受位置：C1–C3、F4、C5、C-M3、C6 已闭（全门 n1-d86-gate-20261107 绿）。C7 按 D89 预注册跑过：原 full matrix（`cargo test --offline --features harness --lib full_f4096_w128`，默认并行 30 项）0/30、固定 18 联合（B588 18 selector 单进程 --exact 默认并行）0/18；全部 `tests/tuic_service/full_fixture.rs:54` server-key.pem NotFound = D9；0 NEW；源未变（1337）。raw /Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-c7-gate-20261107/gate-*；runner /private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/5309990c-aaa9-4318-8a47-b10cc5cc7322/scratchpad/c7g/gate.sh（full30/fixed18 输入 json 同目录）。
阻塞：C7 无可运行证据，需用户选 K2（建议：`full_fixture.rs::server()/config()` 改用进程内 rcgen 身份 + 临时 ca.pem，同 `tuic_service/tests/fixture/native.rs`；测试 fixture 写轴）或 K1（一次性临时两层测试 PKI 换入 certs/dev，运行后按 SHA 复原、删 key）。批准后按 D89 原样重跑；D11 类超时仅以隔离 1/1 重跑归类；NEW ⇒ 停并给计划。
下一：C9 独立只读审查（brief：/Users/liushan/Library/Application Support/Claude/side-session-notes/local_6e17b7d9-0a21-4d14-b9cb-e8f16e494939/c9-brief-20261107.md），依 C7。
仍待授权/决策：K1/K2、C4、C8、C-B、B8（D57）、提交 D63–D90。禁止：外部/TUN/VPS/WAN/M2、commit/push、新密钥（K1 须用户明确授权）、server-key.pem。规则：离线 cargo，失败先停给计划，日志 D91+。

## 新会话交接（2026-11-07，D91–D92：K2 已实施，C7 重跑 STOP 栈溢出，待批 D92 计划，未提交）

Knife/阶段：N1-NATIVE stage C。冷启动同上，重点 DECISIONS D89–D92。
K2（D91）：`tests/tuic_service/full_fixture.rs` `server()`→`Server`（Endpoint + 进程内 rcgen 身份 + 每 server 临时 ca.pem，Drop 删除；Deref/BorrowMut 保 34 调用点不变）、`full_rejection.rs:183` 改 `server.rejected_alpn()`；sha `n1-c7r-gate-20261107/d91-sha.txt`。
C7 重跑（D92）：D9 消除；full30 47s、fixed18 0.4s 均因 `full_reset_retry::*_continuous_health_overlaps_*` 栈溢出 abort；并行快速 FAIL 6 例消息丢失；permit clean_rebuild 隔离绿；legacy continuous_health 隔离溢出、RUST_MIN_STACK=16MiB 隔离绿。中止进程残留临时 CA 目录 18 个已删。全门未跑。
下一（待用户批准 D92）：S1 Box 身份判别 → S2 必要时 Box::pin 场景 future（不用 RUST_MIN_STACK）→ S3 D89 加 --nocapture 重跑并归类 → S4 全门（d86g 新前缀）→ C9（brief：/Users/liushan/Library/Application Support/Claude/side-session-notes/local_6e17b7d9-0a21-4d14-b9cb-e8f16e494939/c9-brief-20261107.md，父会话开）。
仍待授权/决策：D92、C4、C8、C-B、B8（D57）、提交 D63–D92。禁止同前。日志 D93+。

## 新会话交接（2026-11-07，D93–D95：栈溢出已消，C7 S3 STOP（D91 临时目录碰撞），待批 D95，未提交）

Knife/阶段：N1-NATIVE stage C。冷启动同上，重点 DECISIONS D89–D95。
改动（测试专用，均未跟踪用户文件）：`tests/tuic_service/full_fixture.rs`（Server = Endpoint + Box<(rcgen 身份, CaDir)>）、`full_rejection.rs:183`、`full_reset_retry.rs` 10 个入口 `Box::pin(exercise(..))`；sha `n1-c7s3b-gate-20261107/d94-sha.txt`。
S3 结果：full30 16/30、fixed18 3/18；25 例 5s 观察超时（16 名隔离 16/16 绿，D11）；NEW 4 例 `full_fixture.rs:116` AlreadyExists（static NEXT 在 #[path] 多次包含文件中）。先前"6 例快速 FAIL"非中止连带，是超时类。
下一（待批 D95）：F1 建目录有界重试 + module_path → F2 按 D89(+--nocapture) 重跑 → F3 全门（d86g 新前缀）；随后用户决定 C7 判定（D11 记录 + 隔离全绿 vs 结构 R-FULL-MATRIX-RUNNER）。C9 brief：/Users/liushan/Library/Application Support/Claude/side-session-notes/local_6e17b7d9-0a21-4d14-b9cb-e8f16e494939/c9-brief-20261107.md（父会话开）。
禁止同前；日志 D96+。

## 新会话交接（2026-11-07，D96–D97：C7 仅 D11 类、全门绿，待用户定 C7 判定，未提交）

Knife/阶段：N1-NATIVE stage C。冷启动同上，重点 DECISIONS D89–D97。
测试专用改动（未跟踪用户文件）：`tests/tuic_service/full_fixture.rs`（Server：Endpoint + Box<(rcgen 身份, CaDir)>，CA 目录名含 module_path + 有界重试，Drop 删除）、`full_rejection.rs:183`、`full_reset_retry.rs` 10 入口 Box::pin；sha `n1-c7f2-gate-20261107/d96-sha.txt`。
C7：full30 16/30、fixed18 4/18，28 例全为 `full_fixture.rs:43` 5s 观察超时，15/15 隔离绿 ⇒ D11 记录，0 NEW。全门 `n1-d96-gate-20261107/gate-*` 全绿。
下一：用户定 C7 (i) D11 记录关闭 → 父会话开 C9（brief：/Users/liushan/Library/Application Support/Claude/side-session-notes/local_6e17b7d9-0a21-4d14-b9cb-e8f16e494939/c9-brief-20261107.md）；或 (ii) 结构 R-FULL-MATRIX-RUNNER。
仍待：C4、C8、C-B、B8（D57）、提交 D63–D97。禁止同前；日志 D98+。

## 新会话交接（2026-11-07，D98：C7 以 D11 记录关闭，下一 C9，未提交）

C7 已闭（D98，用户接受 D11 记录，非 PASS）；证据 D96–D97，全门 n1-d96-gate-20261107 绿，树 sha n1-c7f2-gate-20261107/d96-sha.txt + n1-d80-20261107/d88-sha.txt。下一 C9 独立只读审查（父会话开；brief：/Users/liushan/Library/Application Support/Claude/side-session-notes/local_6e17b7d9-0a21-4d14-b9cb-e8f16e494939/c9-brief-20261107.md）。仍待：C4、C8、C-B、B8（D57）、提交 D63–D98。日志 D99+。

## 新会话交接（2026-11-07，D99–D100：C9 无 FAIL，F1/F4 已修，A–C 已本地提交）

C9 独立只读审查（D99，[审查](docs/current/n1-c-review-20261107.md)）：C1–C7 无 FAIL；C6 necessary-only；C7 D11 记录非 PASS。D100：门 runner 加 production-plain 组（无 feature `cargo check --offline --lib`；runner `/private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/4a52bef9-4cdf-4ed3-95bf-22b157914faa/scratchpad/d100g/gate.sh`）；计划 C-I8=1024·1472/RTT、C3/C9 行、STATUS 重复项已同步；全门 `n1-d100-gate-20261107/gate-*`。bd18abb 以来整个 N1 dirty 树（D19 前基础 + A–C D19–D100）已单一本地 commit（用户"Commit whole tree"；未 push）。仍待：C4、C8、C-B、B8（D57）、F2/F3；push 未授权。日志 D101+。

## 新会话交接（2026-11-07，D101–D103：C4 已闭，未提交）

C4（B-review F1/F2）：cfg(test) seam `src/tuic/node/tests/service_faults.rs`（`NodeHost::C4_*`）+ Service `tuic_service.rs`/`tuic_service/rebuild.rs` cfg(test) 字段与 4 处 hook + 新叶 `tuic_service/tests/native_rebuild_node_disposition.rs`（已入联合门）。生产 node.rs、node/rebuild.rs sha 不变。证据：DECISIONS D102（冻结预测）、D103（结果/变异/全门），raw `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-c4-20261107/`、`n1-c4-gate-20261107/gate-*`；runner 副本 `/private/tmp/claude-501/-Users-liushan-Documents-Personal-Languages-Rust-mini-vpn-recovered-20260918-jIMbzz/72bd0648-fb12-44cc-9640-45b4ff3798cf/scratchpad/c4g/gate.sh`（joint 696 项）。
下一：用户决定是否提交 C4；C8 外部、C-B、B8（D57）仍待授权/决策。禁止同前；日志 D104+。

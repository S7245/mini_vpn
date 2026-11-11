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

## 新会话交接（2026-11-07，D104–D106：N1 公开入口本地完成，未提交）

完成：D104 审计、D105 设计出口、D106 结果（见 DECISIONS）。生产改动：`compatibility_runtime.rs`（CLI hard 工厂）、`client_tun.rs`（`create_tun_device` 同步，行数不变）、`runtime_environment.rs`（O3 天花板）、`tuic_service.rs`（O3 门 + SIGHUP）、`tuic_service/rebuild.rs`（`bounded_drain`，O-C4b）、`device/cold.rs`（注释）。测试：新叶 `tuic_service/tests/native_public_recovery.rs`、`cold_factory.rs` O3 tracer、C4 T1 Incomplete 断言。证据 `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-n1-20261107/`（mut/、n1g/ runner 副本、n1-sha.txt）与 `n1-n1-gate-20261107/{gate,post}-*`。
下一（按序）：(1) N1 独立只读审查（父会话开）；(2) N2：先冻结验收条件再写码。N2 起点事实：QUIC idle 15s/keep-alive 5s（`src/quic.rs:34-38`）、`TUIC_RECONNECT_TIMEOUT` 5s、endpoint recovery 停滞界 clamp(8×RTT, 2s..7s) 采样 250ms（`src/tuic.rs:2609-2620`，`node_runtime.rs:953` monitor）、closed-slot 恢复首 15s ≤2 次 ≥500ms、会话重建每 Service 生命周期 1 次（`rebuild.stopping` 粘性）、Node 失败后无后继（Failed/Q/config 合同）；黑洞/丢包注入复用 C3 测试中继 `Tap`（`native_rebuild_lossy.rs`）。(3) N4 需 N2 完整接受。禁止同前（无外部/TUN/sudo/M2/邮件/新 key/commit/push）；日志 D107+。

## N1 审查修复交接（2026-11-07，D108–D109，未提交）

D107 F1–F6/F8 已修复（F7 信息项不变）。改动：tests `native_public_recovery.rs`（F1/F4/F6）、`fixture.rs`（`Apps::udp_from`）、`native_rebuild_node_disposition.rs`（F3/F5 断言）、`cold_factory.rs`（F2 选择器）；生产 `tuic_service.rs`（`hangup_rebuild`、`Handle::changed_from`、`report_verdicts`、`serve_with(service, hangup_rebuild)`）、`rebuild.rs`（`bounded_drain` 返回 `expiry_recorded`）。证据见 DECISIONS D109；sha `n1-r1-20261107/r1-sha.txt`。下一：父会话开 N1 独立复审；不要启动 N2。仍未覆盖：真实 TUN/SIGHUP 运行、O2 标定。无 commit/push/外部运行。

### 2026-11-07 N-2 SynBurst（D111–D113，本地，未提交）
完成：D111 内存测试身份（`src/tuic/tests/tcp_fixture.rs`：`test_server_identity`/`test_roots`/`test_ca_path`），`tuic.rs` d16 helper（4003 行不变），`tuic/tests/mod.rs` 内联 sibling（4945 行不变），21 处 `Some(test_ca_path().as_str())`。certs/dev 未动，key 不落盘，临时 CA 目录随测试线程退出删除（运行后残留 0）。D112 运行规则先冻结。D113：SynBurst 1/5 PASS，f_full:: 3/3。失败只出现在端点 drain 清理（10s），接入 drops 全为 0。raw `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-d112-synburst-20261107/`，runner 在 scratch `9cc9c68d-…/scratchpad/d112/run.sh`（guard.sh 复制自 n1g）。按 D112 停止：没有修复，全门没跑（D111 之后的树还没过 14 组门）。下一：等用户批准 D113 方案；N2 不启动。仍为 D9：quinn_leg_actor 等自带磁盘 key 的 fixture。
D114–D115 诊断（探针已回退，5 文件 sha==pre-probe，lib 测试二进制 sha 29c3554c 与 D112 相同）：3×PTO 已证实（失败时 UdpPair 预连接的 3×PTO 为 11.95s，通过时 ≤7.43s）。停顿来自 RX 分支：一次 poll 处理 4225–7936 个包（1.3–3.8s），loopback 设备不消耗 coop 预算，驱动也没有每次 poll 的上限。raw `n1-d114-probe-20261107/`。下一：等批准 (b)（b1+b2，含 tracer）或 (a)。D111 树尚未跑 14 组门。N2 不启动。

### 2026-11-07 N-2 公平性修复（D116–D117，本地，未提交）— 停止点
改动：`src/client_tun/legacy_event_loop/driver.rs`（RX 分支 `consume_budget`，b2，生产）、`src/harness/loopback.rs`（b1）、新 tracer `src/client_tun/tests/compatibility_targeted_ingress/rx_fairness.rs`，加上父文件中的 `#[path]` 声明。TDD 先 RED 后 GREEN；SynBurst 5/5；f_full 3×3/3；drain <0.41s；吞吐通过（native 1.143×，接近上限）。P1 按冻结口径被证伪（poll 105–207ms，多数在 stop 阶段，属非 RX 分支），因此停止：全门和 governance 都没跑（runner 在 scratch `9cc9c68d-…/scratchpad/d117g/gate.sh`，前缀 `n1-d117-gate-20261107`）。下一：等批准 D117 方案（探针归因 → 重定 P1 口径或改 N=64）。冷启动：AGENTS → STATUS → DECISIONS D111–D117 → 本段。仅限本地；不 commit/push；不持久化 key；N2 不启动。
D118–D119：探针归因完成并已回退（sha 与二进制都一致）。≥100ms 的 poll 中 RX 均为 0，剩下的是 udp_sweep（stop 阶段，108–181ms）和 timer.tick（114ms）的单次调用。下一：用户决定是否把 P1 重定为 RX 口径；之后跑 governance 与全门（scratch d117g/gate.sh，前缀 n1-d117-gate-20261107）。
D120–D121：P1 按 RX 口径重定（用户决定）。全门 13/14 通过；joint 第 1 次 699/700，`native_chunk_loaded_reset::…preserves_source_first_and_receipt` :223 join 超时，joint2 700/700 通过。判为 NEW，已停止。下一：等批准 A/B 诊断（有无 b2，×20 joint 负载 + ×20 隔离）。b1+b2+tracer 仍在 dirty 中，未提交。
D122–D123：A/B 诊断已完成（raw `n1-d122-ab-20261108`）。joint 负载下有 b2 1/20、无 b2 0/20（p=0.50），隔离两边 0/20，判为既有负载 flake，不归因 b2。树与二进制已 sha 校验，和 D116 一致；没有代码改动。下一：等用户决定是否按 joint2 700/700 + A/B 计全门通过（之后才能宣告 N1，并开 N2）；:223 load-sensitivity 另列后续项。b1+b2+tracer 仍在 dirty 中，未提交。

## 新会话交接（N2，2026-11-08，D126）
Knife: compatibility recovery N2（仅本机 loopback）。已接受位置：N1 ACCEPTED（D125，commit 974367b 已推送）。本会话：D126 设计出口与冻结验收 N2-A1..A7，加上测试探针（`native_failure_probe.rs` 新叶、`native_rebuild_high_rtt.rs` 中的 `Tap::outage`/`Seen.initial`、`tests/mod.rs` 一行）；生产代码未改，未提交。raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n2-d126-probe-20261108/`（含 guard.sh、sha）。
冷启动：AGENTS → STATUS 末段 → DECISIONS D125–D126 → 本段 → `src/tuic/node/recovery.rs`（`request_aux_recovery` :374、`expire_demands` :212、`recover` 循环 :795–825）和 `node_runtime.rs:540-565,850-860`（主槽在 UDP 断开时立即发起恢复）。
下一步（已授权的本地动作）：(1) 修改探针，不再在 window 到期时取消 `service.run()`，改为保持轮询并按时间采样 phase/ledger，归因会话 Q 的来源（O-N2e）和 stop 超过 20s（O-N2f）。若 Q 是正确的终态（例如 flow 不确定），设计改为先修 Q 来源，再做 Waiting。(2) D126 的合同修订（干净的到期 ⇒ Waiting，可重试）放宽了用户批准的 N1 约束，**须由父会话/用户确认**后再写生产代码。(3) TDD：先写 A2 RED（25s 断网后恢复），再在 node/recovery.rs 增加 Waiting/再探测（可放私有子模块，≤1500 行），然后补 A3–A7 和变异，最后跑 D117 全门（新前缀，capacity-serial 含 SynBurst，joint 含 rx_fairness 与新 N2 叶；探针叶的 60s 用例不进 joint）。
运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`，测试二进制在 guard.sh 下运行；探针 3 例并行约 108s。
冻结非目标/禁止：N3 自动换节点、N4、N5、真实 TUN、VPS/WAN/M2、sudo、新 key 落盘、commit/push、邮件。修复测试意外失败时先停，并给出具体计划。

## 新会话交接（N2，2026-11-08，D127–D129）
Knife: compatibility recovery N2（仅本机 loopback）。已接受：N1 ACCEPTED（974367b 已推送）。本会话：D127 合同修订（用户批准）、D128 探针归因（Q/预算/stop 均为夹具假象，另记 F-N2a）、D129 Waiting 实施 + A1/A2/A6/A7 绿 + 5 个变异 RED。全部未提交。
生产改动：`src/tuic/node/recovery/waiting.rs`（新，225）、`recovery.rs`（1171）、`node.rs`（`handshake_as`，1303）、`node_runtime.rs`（UDP 主槽改用 `await_closed_recovery`，1 行）。测试：新叶 `tuic_service/tests/native_waiting.rs`（≈410，rig/drive/Storm 可复用）、`tests/mod.rs` +1 行、`native_rebuild_high_rtt.rs` Tap 加 `loss_every`/`initials`/`new_dcids`、`node/tests/mod.rs` 加 `WaitingSample`（`tuic.rs`/`node.rs` re-export）、探针叶 `native_failure_probe.rs` 重写为不取消 run()（6 例，仅测试，不进 joint）。raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n2-trace-20261108/`、`.../n2-waiting-20261108/`（g1–g6、mut/{mutate.py,build.sh,bin-sha.txt,*.log}）。guard：`.../n2-d126-probe-20261108/guard.sh`。
下一步（已授权本地动作，按序）：(1) 按 D127 修订 `src/tuic/node/tests/recovery.rs` 的 `actual_transient_refusal_retries_twice_with_original_spacing` 与 `original_deadlines_expire_while_candidate_drain_waits_without_ticket_reset`（旧断言 first.is_some/FailedSettled/1s 内无新尝试；新合同：2 次后 Waiting、≥1s 后一次再探测、无 first failure）；先跑确认 RED，再改断言并记录。(2) A3（单程 200ms + `Tap::loss_every(50)`，30s 稳定 TCP+UDP：首连后 0 新 DCID、0 rebuild、无 first_failure；变异：QUIC idle 15→1s 或关闭 keep-alive）、A4（空闲 20s：0 新 DCID，同连接 TCP+UDP；变异：关闭 keep-alive）、A5（两并发 Connect，对端 reset 其一：另一完成，0 新 DCID，无 first_failure；变异需找"流失败升级为关连接"的点）；放在新叶 `native_waiting_steady.rs`（≤800），复用 `native_waiting::{rig, drive, stop_polling, settle_flows, assert_clean_stop}`。(3) `tests/code_growth_governance.rs` + fmt + 无 feature production check；(4) D117 全门（新前缀 n2-d129-gate-20261108，capacity-serial 含 SynBurst，joint 含 rx_fairness 与 native_waiting::；native_failure_probe:: 与 60s 用例不进 joint，单独串行跑），失败分类 D9/D11 vs NEW。(5) 更新 STATUS/HANDOFF/TODO/LEARNINGS/DECISIONS，停；独立 N2 审查由父会话开。
构建/运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`；二进制 `target/debug/deps/mini_vpn-38e8a4ca41212e00`，`guard.sh 300 <out> <bin> tuic_service::tests::native_waiting:: --nocapture --test-threads=6` 约 61s。注意：zsh 不按空格拆分 `$F`，多文件备份/还原必须用 bash 数组（本会话两次踩坑，已全部 sha 还原）。
冻结非目标/禁止：N3 自动换节点、N4、N5、真实 TUN、VPS/WAN/M2、sudo、新 key 落盘（勿建 certs/dev/server-key.pem）、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。修复测试意外失败时先停，给出具体计划。

## 新会话交接（N2，2026-11-08，D130–D132）
Knife: compatibility recovery N2（仅本机 loopback）。N1 ACCEPTED（974367b 已推送）。D127–D129 之上本会话完成：D130 N1 tracer `node/tests/recovery.rs::actual_transient_refusal_retries_twice_with_original_spacing` 按 D127 修订（GREEN）；新叶 `tuic_service/tests/native_waiting_steady.rs`（A3/A4/A5 GREEN，变异 MA3e/MA4/MA5b RED，MA3a–d/MA5 GREEN 记为不可达证据）；四个 N2 新文件已 rustfmt；governance 7+20 绿。D131：F-N2b（生产缺口，见下）、F-N2a、F-N2c。D132：全门 `n2-d130-gate-20261108` 12/14，joint/joint2 各 716/717，唯一失败 `udp_source::recovery::actual_hard_node_recovery_refusal_is_quiet_in_session_before_later_io`（第三个 N1 旧规则 tracer，确定性，NEW）。全部未提交。
raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n2-steady-20261108/`（n1-red/n1-amended、s1–s3/base、mut/{mutate.py,build.sh,bin-sha.txt,MA*.log}、udp-refusal-iso-*、fmt-*、governance.log）和 `.../n2-d130-gate-20261108/`；gate runner 在本会话 scratchpad `n2g/`（gate.sh、*-in.json）。
F-N2b：`recovery.rs::expire_demands` 中 `retry = active && waiting::hold(..)`，非 active 的过期 closed demand 被 `record` 为终态。默认 tcp_pool=2 时，>15s 断网后辅助槽永久丢失，Node first failure 被记录。证据：deadline tracer 显示 waits 1、retries_held 1、first_failure true。
下一（**需用户/父会话批准**，修复规则）：P1 按 D127 修订 udp refusal tracer（Waiting、Node 与会话都不记录、后续会话 IO 仍记录、RunningResourcesSettled），并检查其它 FailedSettled/first.is_some 旧规则 tracer；P2 修 F-N2b：对所有干净的 closed demand 执行 hold/park，修订 `original_deadlines_expire_while_candidate_drain_waits_without_ticket_reset` 为两槽 Waiting，加 tcp_pool=2 的 A2-60/A6-60 变体与变异"仅 active"⇒RED；P3 重跑 joint×2 + capacity-serial（同 runner，新前缀）。随后父会话开独立 N2 审查，再问用户是否提交；N4 在后。
冻结非目标/禁止：N3、N4、N5、真实 TUN、VPS/WAN/M2、sudo、新 key 落盘（勿建 certs/dev/server-key.pem）、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。修复测试意外失败先停，给出计划。

## 新会话交接（N2，2026-11-09，D133）
Knife: compatibility recovery N2（仅本机 loopback）。用户已批准 P1–P3（D132 计划）。已完成：P1（udp refusal tracer 按 D127 修订，GREEN）；P2 生产代码（`src/tuic/node/recovery.rs::expire_demands`：所有干净的过期 closed demand 进入 Waiting，非 active 的在 custody 结算后 park，未结算的仍是终态）。未完成：P2 tracer 与 P3。全部未提交。
停止原因：`tuic::node::tests::` 共 7 个失败（`n2-fix-20261109/p2-node-red.log`）。其中 6 个在修复前的 D129 树上就已存在（`p2-node-prefix.log`，二进制为 `n2-steady-20261108/mut/bin-base`），D132 门禁没选中它们。它们是：failover_rejection 2 个、failover_staged 1 个、primary_recovery::missing_consumer_seal（均为期望 FailedSettled 实得 RunningResourcesSettled）；primary_recovery::guarded_udp_panic（:46 endpoint_drained=false）；primary_recovery::shutdown_only_udp_inventory（:568 shutdown 超时）。第 7 个是预期的 deadline tracer。
冷启动：AGENTS → STATUS 末段 → DECISIONS D126–D133 → 本段 → `src/tuic/node/recovery/waiting.rs`（`node_clean`/`hold`/`retryable`/`park`）、`recovery.rs::expire_demands`，以及上述测试文件。
下一（已授权的本地动作）：(a) 扩大门禁 joint 过滤，加入 `tuic::node::tests::` 和 `native_complete::udp_source::`（runner 在旧会话 scratchpad `n2g/`，也可从 `.../n2-d130-gate-20261108/*-commands.json` 重建）；(b) 先诊断 2 个非断言失败。假设：`node_clean` 没把"已记录 panic/不变量/inventory 失败"当终态，所以 Waiting 行或再探测让 endpoint/UDP inventory 在 shutdown 时仍存活。证伪方法：在这些测试中观察 WaitingSample。若成立，按 D127"不变量错误仍为终态"收紧 hold/rearm 条件，修完后再看那 4 个是否随之转绿，否则按旧规则断言修订；(c) 补 P2 tracer：deadline tracer 改为两槽 Waiting/RunningResourcesSettled，加 tcp_pool=2 的 A2-60/A6-60，变异"仅 active"⇒RED；(d) P3：新前缀全门。修复测试意外失败先停，给出计划。
构建/运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`；二进制 `target/debug/deps/mini_vpn-38e8a4ca41212e00`；guard `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n2-d126-probe-20261108/guard.sh`。
冻结非目标/禁止：N3、N4、N5、真实 TUN、VPS/WAN/M2、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。


## 新会话交接（N2，2026-11-09，D134–D139）
Knife: compatibility recovery N2（仅本机 loopback），用户已批准 P1–P3（D132）。全部未提交。
已完成：(a) joint 过滤扩大（runner：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n2-p3-20261109/runner/`，gate.sh 的前缀占位为 `n2-p3gate-PREFIX`，运行前替换）；(b) 6 个失败已分类并全部转绿（D134 wait_sealed fail-closed 修复 + tracer；D135 guarded 旧断言；D136 主槽 UDP 未 seal ⇒ 终态，`waiting::udp_settled`；D137 failover 旧规则）；(c) deadline tracer 两槽 Waiting 绿、A2-pool2 绿、变异 M-P2 记录（D138）。
停止原因：A6-pool2 RED，最慢 open 14.05s > 6s（`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n2-p3-20261109/p2-pool-base.log`）⇒ F-N2d：`src/tuic/tcp_open/staged.rs` 中 `State::Recovering`（:381）只轮询 `observe_aux_recovery`，请求期限（:324）对它不生效，open 会等 aux 原 15s demand。
冷启动：AGENTS → STATUS 末段 → DECISIONS D134–D139 → 本段 → `tcp_open/staged.rs`（Recovering、tighten_deadline、note_cancel）→ `native_waiting_pool.rs`。
下一（需用户/父会话确认后执行）：D139 计划 (1) tcp_open 聚焦 RED tracer → (2) Recovering 到期 typed 失败，demand 留给 Node（先确认 Recovering 不持有 lease/custody）→ (3) A6-pool2 + 变异 → (4) 可选：带排队 aux demand 的 Service 判别器 → (5) P3 全 14 组门，失败按 D9/D11 或 NEW 分类。之后再更新文档；不开 N2 审查。
构建/运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`，二进制 `target/debug/deps/mini_vpn-38e8a4ca41212e00`。HEAD 974367b 的对照二进制在 `target/headbuild/`（1.4G，可删）。
冻结非目标/禁止：N3、N4、N5、真实 TUN、VPS/WAN/M2、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动；只对单个叶文件跑 rustfmt。

**N2 F-N2d 复核（2026-11-09，D140，无代码改动）：** "Recovering 不受期限约束"的假设被证伪。flow 的 open 期限是 20s，staged.rs 正确遵守；A6-pool2 中 8 个慢 open 等的是它们在断网期间为 aux 槽新铸的原始 15s demand，属于 N1 原有语义。**待用户选：(A) 修订 A6-pool2 断言（新 demand 上的 open ≤16s，其余 ≤6s，仅测试）；(B) 生产修改（Node 已有干净 Waiting 时，其他槽的新 closed demand 直接进入 Waiting）。** P3 未跑。

## 新会话交接（N2，2026-11-09，D141–D142）
Knife: compatibility recovery N2（仅本机 loopback）。P1–P3 已全部本地完成，全门 `n2-p3gate-20261109` 14/14 绿（joint ×2 790/790）。全部未提交。
本轮改动：D134 `owned_upstream/udp/generation.rs::wait_sealed` fail-closed；D136 `waiting::udp_settled`；D141 `waiting::join_parked`/`join_queued`（recovery.rs 两处调用）；测试 D135/D137/D138 修订，新叶 `node/tests/waiting_join.rs`、`tuic_service/tests/native_waiting_pool.rs`。
下一（需父会话/用户决定）：开 N2 独立只读审查（证据见 DECISIONS D126–D142，原始日志在 `n2-p3-20261109/`、`n2-p3gate-20261109/`）；是否提交。开放项：F-N2a（非 N2）、F-N2c（N4）、O-N2a–f。
冻结非目标/禁止：N3、N4、N5、真实 TUN、VPS/WAN/M2、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。`target/headbuild/`（1.4G，HEAD 对照二进制）可删。

## 新会话交接（N2，2026-11-09，D143–D144）

- 位置：N2 Waiting + 审查修复全部在工作树，**未提交**（HEAD 974367b，分支 `codex/knife14d-downlink-reap-open`）。全门 `n2-fixgate-20261109` 14/14 绿；审查报告 `docs/current/n2-review-20261109.md` 末尾有逐项修复状态。
- 本次改动：`src/tuic/node/recovery/waiting.rs`（retryable 去 `held`、`holds`、`exhausted`、模块文档）、`src/tuic/node/recovery.rs`（终态清行、本地错误记录标记、Waiting 文案）、`src/tuic/node.rs`（`WaitingFailure`、`is_recorded`）、新叶 `src/tuic/node/tests/waiting_terminal.rs` + `src/tuic/node/tests/mod.rs` 一行、`native_waiting_pool.rs` DCID 界。
- 下一：用户决定 commit（整树还是分批；`certs/dev/ca-cert.srl` 不要动、不要提交）；之后 N4。F-N2a 应在 N4/真实门前单独决定（F11）。
- 不授权：TUN、VPS、WAN、M2、sudo、新 key、push。

## 新会话交接（N4，2026-11-09，D145–D146）

- 位置：N2 已接受并提交 `91ff731`。N4 设计出口 D145（`docs/current/n4-design-20261109.md`）。只改了测试代码：`tuic_service/tests/native_n4_harness.rs`、`native_n4_isolation.rs`、`tests/mod.rs` +2 行，**未提交**。raw 在 `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/`。
- 停止原因：F-N2a（M5(iii)）RED 已表征，关闭它要改 flow custody 关闭尾合同（`transaction.rs` 的 close/drive：15s 到期 ⇒ Quarantined）。**待用户/父会话从 D146 的 (A)/(A')/(B)/(C) 中选择**，建议 (A')。
- 获批后：(1) 用临时探针确认到期时卡住的确切子条件（roles.data_disposed、本地 socket 活跃或 Rearming），sha 还原；(2) 按所选方案修 flow custody，M5(iii) RED→GREEN，加变异；(3) 归因 M1 stop 后多出的 1 个 task；(4) M1 跑冻结的 120s，补 M2–M7 及各自变异（M3 慢段需要 RttRelay 运行时可调延迟）；(5) 全 14 组门 + `native_n4_` 组（capacity-serial，新前缀 `n4-gate-`），失败分 D9/D11 或 NEW；O-N2b release 对比。
- 运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`；二进制 `target/debug/deps/mini_vpn-38e8a4ca41212e00 native_n4_ --nocapture --test-threads=1`（本机没有 `timeout` 命令）；`N4_SPAN_SECS` 只能缩短 M1 的探索运行。zsh 下 heredoc 必须加引号（`<<'EOF'`），否则反引号会被当命令执行。
- 不授权：N3、N5/C8、TUN、VPS、WAN、M2、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-09，D147–D149）

- 位置：F-N2a 已按用户 (A') 关闭（D147，合同修订）：关闭尾 15s 从最后一次 App 进度起算；到期时若最近一次物理检查只在等 App，则只 RST 该 flow 并正常释放，其它到期仍 sticky Q。生产改动只在 flow custody：`flow_custody/transaction.rs`（`CloseTail`、`app_tail_wait`、drive 到期判定）、`rearm/physical_flow.rs`（3 个观测点）、`downlink_custody/ledger.rs` `remaining_bytes` + `downlink_custody.rs` 委托。tracer：M5(iii)、慢读者、单元叶 `flow_custody/transaction/tests/close_tail.rs`；4 个变异各自 RED。M1 task 残留归因为测试 RttRelay（D148），M1 120s 绿。全门 `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-gate-20261109/` 14 组 + n4 组绿（D149）。**未提交。**
- 下一（按顺序）：(1) M1 补 8MiB 下行批量流吞吐记录 + M1 变异（短流 release 跳过）；(2) M2–M7 按 D145 冻结阈值与变异实现（M3 需要 RttRelay 运行时可调延迟/丢包，M4 复测 F6，M6 16×4MiB，M7 DNS 4000）；(3) O-N2b release n=5；(4) 全门重跑（前缀 `n4-gate2-`，runner 复制 `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-d147-20261109/runner/`）；(5) 父会话开 N4 独立审查。
- 运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`；二进制 `target/debug/deps/mini_vpn-38e8a4ca41212e00 native_n4_ --nocapture --test-threads=1`。raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/d147/`。
- 不授权：N3、N5/C8、TUN、VPS、WAN、M2 验收、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-09，D150）

- 位置：只改了测试代码，未提交。新叶 `tuic_service/tests/native_n4_peer.rs`、`native_n4_recovery.rs`、`native_n4_pressure.rs`；改 `native_n4_harness.rs`（拆分后 604 行）、`native_n4_isolation.rs`（M1 加 8MiB 批量流）、`native_waiting.rs`（`rig_links`、`Rig::to_service`）、`native_rebuild_high_rtt.rs`（`Tap::slow`）、`tests/mod.rs` +3。生产代码与 D147 相同。raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/m27/`。
- 停止原因（D150）：F-N4a（M2 跨代分片重组泄漏，真实）、F-N4b（M3/M4 负载下 Waiting 不再探测、stop 不完成，跨三方 ⇒ 结构门）。**修生产前需用户/父会话批准 D150 计划。**
- 获批后按顺序：(1) F-N4b 临时探针（M4c，恢复循环每轮 old/primary_old/next/due、active_leases、reap_stats/finish_predecessor 耗时），sha 还原，给设计出口；(2) F-N4a 用 M2 单周期 tracer 定位 retire/discard 是否在 peer-close 恢复上执行；(3) 测试修正：M7 link（遵守设备上限或分批）、M5(ii) 阈值从真实窗口链推导、M3 重跑（completion 度量，slow 段用 peer connections）；(4) 各格变异（mutate.py 配方见 `n2-waiting-20261108/mut/`、`n2-steady-20261108/mut/`；M1 = release.rs `opens.slots.take` 跳过，M2 = native_fragments `retire_generation` 不清，M5 = WINDOW_BYTES 去界，M6 = ReaderWindow 不停顿，M7 = 去掉 driver.rs b2 `consume_budget`）；(5) M1 120s、O-N2b release n=5、governance、全门 `n4-gate2-`。
- 运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`；`target/debug/deps/mini_vpn-38e8a4ca41212e00 native_n4_ --nocapture --test-threads=1`。governance 与 fmt 本轮未跑（新文件已 rustfmt）。
- 不授权：N3、N5/C8、TUN、VPS、WAN、M2 验收、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-09，D151）

- 位置：仅探针，**无生产改动**，测试代码同 D150，未提交。探针全部还原（6 个文件 sha 与原值一致，`[DEBUG-n4b]` 0 处）。raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/fn4b/`。
- F-N4b 已归因（D151），D150 的"恢复循环卡在 predecessor"假设被证伪：Node 循环正常 park/idle；stop 时 `request_stop` 按设计丢弃 running。真因：黑洞中经 relay 半关闭 10s 守卫关闭的短流，其 TUIC open 处于 Retiring，lease 要等 `quinn::BiStreamRetirement::is_retired()`；死连接的存储一直挂在 Node 槽的 `current` 上（Waiting 期间无界），见证永不退休 ⇒ 15s 关闭尾到期（非 App-only）⇒ flow Q ⇒ 会话 sticky Q ⇒ 47 条 Quarantined 行保留 ⇒ stop 永不完成。M3 推断同因（未单独探针）。
- 停止原因：跨 flow custody / Node 槽生命周期 / TUIC open+quinn 见证三方 ⇒ 结构门。**待用户/父会话从 D151 (A)/(B)/(C)/(D) 中选择，建议 (A)=A1 provider-only 到期一次性延长 PROVIDER_LOSS_BOUND（QUIC idle 15s+5s）+ A2 Node 在 Waiting/终态时释放死连接存储。**
- 获批后：T1 Node 叶 RED→GREEN（停在 Waiting 后 ≤1s 见证退休、activity 0）、T2 close_tail 单元、T3 M4c/M3；变异：去 A2 ⇒ T1+M4c RED，去 A1 ⇒ T2 RED。之后接着 D150 计划 (2) F-N4a M2 单周期 tracer、(3) M7 link/M5(ii) 窗口链阈值/M3 8s 重跑、(4) 各格变异、M1 120s+批量、O-N2b release n=5、governance、全门 `n4-gate2-`。
- 运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`；`target/debug/deps/mini_vpn-38e8a4ca41212e00 n4_m4_stop_during_waiting_under_load_completes --nocapture --test-threads=1`（约 90s，RED）。
- 不授权：N3、N5/C8、TUN、VPS、WAN、M2 验收、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-09，D152–D153）

- 位置：用户选 "Option A (A1+A2)"，记为 D152 合同修订；D153 实现与证据。**未提交。** 生产改动：flow custody `transaction.rs`（A1）、`quic.rs`（pub(crate) 常量）；Node `tuic/generation_slot/released.rs`（新，SlotConnection）、`recovery/waiting.rs::release_settled`、`recovery.rs`、`node.rs`，以及 maintenance/node_runtime/startup/probe/staged/udp_send 的机械 `live()/connection()/.into()`；`tuic.rs` 保持 4003 行。测试：新叶 `tuic/node/tests/waiting_release.rs`（T1）、`close_tail.rs` +5（T2）、node recovery/drive_panic/fatal_settle 期望按合同更新、`native_n4_recovery.rs` task 归因（2×relay 客户端地址）。raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/d152/`。
- 结果：T1/T2/M4c 绿；m-a2 ⇒ T1+M4c RED，m-a1 ⇒ T2 RED（sha 已还原）；governance 7/7；units+Node 27/27；M3 30s 黑洞 3.6s 恢复。8 个既有未门禁失败经基线副本复现（D153 列表），不属本次。
- 未完成（按顺序）：(1) F-N4a：写 M2 单周期 tracer（旧连接 frag0 pkt7 → close → 新连接 frag1 pkt7 期望 0；同代 pkt9 两片作正对照期望 1）；若证实为测试伪迹，M2 每周期用不同 packet id；(2) M3：慢段 dcids 改用 peer connections（D150 计划），重跑 8s 黑洞，若仍 >2s，先用 QUIC stats 验证 PTO 退避假设，再报告（不放宽）；(3) **F-N4d 需父会话/用户决定**：原始 pending demand 期间的 open 等满 2×5s（M4a/b 10.07s > F6 6s；D150 已存在）——选项：open 在槽已知关闭时快速失败（Node 生产改动）或把界定义为 demand 预算（阈值修订）；(4) M5(ii) 由窗口链推导、M7 burst 方法；(5) 各格变异、M1 120s+批量、O-N2b release n=5、fmt、全门 `n4-gate2-`（14 组 + n4），失败按 D9/D11 类或 NEW 分类；(6) 父会话开 N4 独立审查。
- 运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`；`target/debug/deps/mini_vpn-38e8a4ca41212e00 native_n4_recovery --nocapture --test-threads=1`（约 4 分钟）。rustfmt 只检查了新文件。
- 不授权：N3、N5/C8、TUN、VPS、WAN、M2 验收、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-09，D154–D155）

- 位置：**未提交。** 生产改动只多了 D154：`tuic/tcp_open/staged.rs`（`recovery_bound`、`open_bound()`；sha 4d9e96cb…）。测试：`native_waiting.rs`（Storm::aligned + 新 tracer `n2_a6_storm_aligned_original_attempt_opens_settle_within_6s`）、`node/tests/mod.rs`（NativeConnectionSample.closed）、`native_n4_recovery.rs`（F-N4a 单周期 tracer、M2 每周期 packet id、M3 慢段改 peer connections）、`native_n4_harness.rs`（foreign_heads）、`native_n4_pressure.rs`（M5(ii) 窗口链界）。探针全部移除。raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/d154/`。
- 结果：F-N4d 绿 + 变异 RED；回归 tcp_open 24/24、node 100/100、native_waiting 12/12；M2 绿、M4a/b 绿、M5 绿；M3 只剩 8s 黑洞 2s 界 RED（4.2–5.9s）。
- **停止原因：M3 冻结 2s 界在负载下物理不可达（RFC 9002 PTO 退避，证据 m3probe/m3-probe.log）。需用户批准修订（提议 ≤H+1s=9s，并断言抬起后首包为 PTO 探测），或选择其它选项（D155）。**
- 获批后按顺序：改 M3 阈值并重跑；M7 link（`rig_links(…,512)` 启动即 DeviceFailedSettled，原因未查——先找 PacketLink/设备容量上限）与 burst 方法；各格变异（M2 = retire_generation 不清，用单周期 tracer 验证）；M1 120s+批量；O-N2b release n=5；governance + fmt；全门 `n4-gate2-`（14 组 + n4），按 D9/D11 类或 NEW 分类，并核对 D153 的 8 个既有失败；更新文档；父会话开 N4 审查。
- 运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`；`target/debug/deps/mini_vpn-38e8a4ca41212e00 n4_m3_slow --nocapture --test-threads=1`（约 100s）。
- 不授权：N3、N5/C8、TUN、VPS、WAN、M2 验收、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-10，D156）

- 位置：同 D154–D155 交接块（未提交；生产改动 = D152/D153 + D154 `staged.rs`）。D156 已记录用户决定（M3 8s 黑洞界 ≤H+1s + "抬起后首包是 PTO 探测"断言），**尚未实现**。
- 下一步（按顺序）：
  1. M3：`native_n4_recovery.rs` 8s 断言改 `<= outage + 1s`；在 `Tap`（`native_rebuild_high_rtt.rs`）加一个有界、只记时间/方向/长度的 post-lift 记录（**勿**复用 D155 的 20 万条探针；它在 Tap 锁下拖垮过 30s 恢复），断言：每方向 lift 后第一批数据报 ≤2 个、在对端首次回包之前发出，且 lift 到该批之间两向 0 包。先给出可观测定义写进 D157，再找一个让它变红的生产变异（候选：Node 在 path 恢复/定时器上主动发非探测流量，例如把 keep-alive 设到 ≤100ms 后验证是否仍被 cwnd 挡住；若无可行变异，按停止规则报告）。
  2. M7：`rig_links(…,1,512)` 启动即 `DeviceFailedSettled`（原因未查：先在 PacketLink/设备启动路径找容量上限或 preflight `native_app_cold` 随 link 增长被拒），修测试并定 burst 方法。
  3. 各格变异（配方 `n2-waiting-20261108/mut/`、`n2-steady-20261108/mut/`；M2 用单周期 tracer 验证 retire_generation 变异）；M1 120s+批量；O-N2b release n=5；governance + fmt（`staged.rs` 827/848 与 node/tests 子模块的 fmt 差异非本轮引入）；全门 `n4-gate2-`（14 组 + n4），失败分 D9/D11 类或 NEW，并核对 D153 的 8 个既有失败。
  4. 更新 STATUS/HANDOFF/TODO/LEARNINGS/DECISIONS/设计 §10，停止，父会话开 N4 独立审查。
- raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/d154/`。运行命令同上块。
- 不授权：N3、N5/C8、TUN、VPS、WAN、M2 验收、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-10，D157–D158）

- 位置：**未提交。** 生产数据面无新改动（同 D154）。D157：M3 按 D156 实现（`Tap::trace_window` 有界 4096 记录 + `native_n4_recovery::loss_probe`；8s 黑洞 ≤H+1s；首个抬起后突发 ≤2 且退避比 ≥1.5），GREEN 4.68/5.49s；变异 quinn-proto `MAX_BACKOFF_EXPONENT` 16→0 ⇒ 只有探测断言 RED，sha 已复原。D158：M7 启动修复（fixture `service_slots`，link=slots=512）+ RX 计量改为 harness `with_test_rx_counter`；M7 除"单 poll RX ≤128"外全绿，实测 214（来源 `timer_active_flow`/`remote_payload_deferred_delayed` 的 drain，每次上限 256，非 coop）。
- **停止原因：D145 M7 "单 poll RX ≤128" 前提不成立，需用户/父会话选 D158 (A)/(B)/(C)。** 推荐 (A)。
- 获批后按顺序：M7 按选项实现并验证变异；各格变异（配方 `n2-waiting-20261108/mut/`、`n2-steady-20261108/mut/`；M2 用单周期 tracer）；M1 120s+批量；O-N2b release n=5；governance（7/7 当前绿）+ fmt；全门 `n4-gate2-`（14 组 + n4），失败分 D9/D11 类或 NEW，核对 D153 的 8 个既有失败；更新 STATUS/TODO/LEARNINGS/设计 §11 格表。
- raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/d157/`（m3-green-1.log、mut/、m7-2.log、m7probe/）。运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`；`target/debug/deps/mini_vpn-38e8a4ca41212e00 n4_m7 --nocapture --test-threads=1`（30s）、`n4_m3_slow`（100s）。
- 不授权：N3、N5/C8、TUN、VPS、WAN、M2 验收、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-10，D159）

- 位置：**未提交。** 生产数据面无新改动（同 D154 `staged.rs`）。D157 M3 绿 + 变异 RED；D158/D159 M7 绿（主接收 ≤128、总 ≤512 槽；用户 "Option A"）+ 去 b2 变异 RED（driver.rs sha 已复原）。测试/harness 改动：`native_n4_recovery.rs`（loss_probe）、`native_rebuild_high_rtt.rs`（trace_window）、`native_n4_pressure.rs`（M7 计量/断言）、`native_waiting.rs`（rig_links slots + Rig::rx_installed）、`fixture/native.rs`（build/service_slots）、`harness/loopback.rs` + `loopback/tests/egress.rs`（cfg(test) RX 计数钩子）。governance 7/7；以上文件 fmt 只动了本轮行。
- 下一（按顺序，各格变异每个只让本格 RED、恢复后核 sha；位置见设计 §4 与 D150）：M1（短流 release 跳过）、M2（retire_generation 保留残片，用单周期 tracer `n4_m2_single_cycle_fragment_generation_tracer` 核）、M4（MA7b Waiting 忽略 stop；MA6 ⇒ DCID）、M5（每流窗口去界 / MA5b）、M6（去源停顿）；M1 120s+8MiB 批量；O-N2b 同测试 `--release` n=5；全门 `n4-gate2-`（runner 复制 `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-d147-20261109/runner/`，14 组 + n4），失败分 D9/D11 类或 NEW，核 D153 的 8 个既有失败；更新 STATUS/TODO/LEARNINGS/DECISIONS/设计 §11；停，父会话开 N4 独立审查。
- 运行：`CARGO_INCREMENTAL=0 CARGO_TARGET_DIR=target cargo test --offline --features harness --lib --no-run`；`target/debug/deps/mini_vpn-38e8a4ca41212e00 <filter> --nocapture --test-threads=1`（n4_m3_slow ≈100s，n4_m7 30s）。raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/d159/`、`…/n4-20261109/d157/`。
- 不授权：N3、N5/C8、TUN、VPS、WAN、M2 验收、sudo、新 key 落盘、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-10，D160）

- 位置：**未提交。** 无生产改动（只有 N4 触及语句的 fmt，governance 7/7，可编译）。D160：变异扫描 + O-N2b 完成；**停止**：MA6 存活、M6 变异位置无效。
- 结果：m2-keepfrag/m5-MA5b 只红本格；m1-norelease、m4-MA7b 本格红，但因停止/释放路径共享，全部 11 个都红（构造使然）；**m4-MA6 全绿**（30s 黑洞受握手约束，dcids 3=界 3）；**m6-nostall 无效**（停止超时 + 循环饥饿，M1/M5 连带；Native 源停顿不是 64KiB reader 窗口）。O-N2b release n=5：比值 1.006 ≤1.15 通过。
- 待批（D160 提议）：(1) M4 以 MA7b 为变异，MA6 记为 30s 窗口内等价、由 N2 A6-60 覆盖；(2) M6 先探针找 Native 源停顿的 owner，再在该处变异并重扫；(3) 接受 m1/MA7b 的跨格红。之后：M1 120s → `n4-gate2-`（runner `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-gate2-20261110/runner/gate.sh`，含 preexisting 组）→ 最终文档 → 父会话开 N4 审查。
- raw：/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/fin/。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-10，D162）

- 位置：**未提交。** 无生产改动。D161 获批执行：M1 120s+批量 **绿**；gate `n4-gate2-` 跑完；M6 源停顿 owner = QUIC 连接级 MAX_DATA（探针），m6-nocredit 只红 M6 但通过 delivery 失败，且对端仍在 29.5MB 停顿。
- **停止原因**：(a) joint/joint2 10/9 个 NEW 类失败，已归因 D152 A1（去 A1 全过，`fin/a1attr/`）：测试仍按旧"到期即 Q"计时；(b) M6 在干净树上 1 次 delivery 停顿（一条流 2.37MB/4MiB），其它 4 次通过 ⇒ 间歇。
- 待批（D162 计划）：(1) 10 个测试的期望 Q 时间改成 D152 合同（deadline+PROVIDER_LOSS_BOUND），用 m-a1 验证会变红；(2) M6 重跑 ×5，带每流 drain 统计，复现则先探针定 owner；(3) 之后再定 M6 变异；(4) 重跑 joint×2 + n4，写最终文档。
- raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-20261109/fin/`、`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-gate2-20261110/`。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-10，D164）

- 位置：**未提交。** 无生产改动（只改测试；m-a1 和探针都已 sha 还原）。D163 = "Approve D162" 已记录。
- 已完成：7 个硬 Native 测试对齐 A1 的 Q 时钟（父期限时不 Q，Q ≥ provider deadline，且确用了一次延长）。基线 ×4 全绿；m-a1 下 7/7 红。M6 单独跑 ×5 全绿（drain 12.7–13.7s），未复现。
- **停止原因**：(a) direct_local_cancel ×2 在 A1 下变成干净结算（DeviceSettled，预算 0，无 first failure），与"仍断言 Q"冲突，已还原到 HEAD；(b) chunk_loaded_reset 的 Q 时间不确定（+0.6ms 到 +20s）：致命 Native disposal 按值拷贝了 `flow.stop_deadline`，与 A1 延长竞态（跨 owner），已还原到 HEAD。
- 待批：D164 计划 (a)–(d)。
- raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-d163-20261110/`。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-10，D166）

- 位置：**未提交。** D165 = 用户 "Settle rules first"。D166 已完成：关闭尾结算模型 `docs/current/n4-close-tail-model-20261110.md`（T1–T9）；生产改动只在 flow custody + roles/failure：新增 `transaction/stop_deadline.rs`（StopDeadline，只读 Deref；open/rearm/grant/limit），8 处 clamp 都改走 `limit`，去掉 OriginalDisposal 的期限拷贝。close_tail 新增 2 个 tracer（红→绿，m-immune 红；m-a1 下 4 行红）。reset 硬 ack-debt 行接受 T8\|T9，10/10 绿。governance 7/7。
- **停止原因**：D164(a) direct_local_cancel 硬 "≤P 干净结算" 不确定（2/7）。失败结局：P 时 Q，flow 与 roles 都已 Q，provider 仍 Retiring+lease，first=None；Q 的时间和 owner 还没探针确认（假设 T8）。该文件已还原到 HEAD。
- 待批：D166 计划 (1) 探针；(2a) T8\|T7 析取，或 (2b) 合同修订，让 egress 期限让给 A1；(3) 非硬 ×6 基线失败另立。之后 M6（门负载）+ 变异，joint×2 + 全门（新前缀）+ 最终裁决。
- raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-d165-20261110/`。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-10，D168）

- 位置：**未提交。** D167 = 用户 "2a: accept either"（已记录）。D168：探针（源码已还原到 HEAD，sha OK），无生产/测试改动。
- **停止原因**：direct 硬 held 行 12 次：T7 11/12（parent+10.5s 干净结算）；1/12 在 ≈P（parent+19.99s）由 `rebuild::drive` 发布 Q，provider 仍 Retiring+lease ⇒ 模型 T6，不是 T8/T9。D166 的 T8 假设被证伪。为何 provider 租约超过 P（QUIC idle 只有 15s）未定位。
- 附带：没有任何 gate 组选中 6 个非硬失败测试；HEAD 归因未跑。
- 待批：D168 计划 (1) 探针定位 T6 持有者（QUIC idle/close 时间 vs 租约释放；含并行负载）；(2) H1 ⇒ direct ×3 接受 {T7,T6}；H2 ⇒ 缺陷，先测后修。之后 D167 步骤 3–5。
- raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-d167-20261110/`。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-10，D170）

- 位置：**未提交。** D169 = 用户 "Approve D168"。D170：探针定为 H1（单向丢包时，对端重传会合法地重置客户端 QUIC idle 计时，再加上 A2 约 10.5s 的释放 ⇒ 退役晚于 P ⇒ T6 合法）。唯一的测试改动：`tuic_service/tests/native_direct_local_cancel.rs` 的硬 held ×2 只接受 T7\|T6，20/20 绿（T7 ×14，T6 ×6）；m-a1 下 9/10 硬行红；governance 7/7。所有探针 sha 已还原；HEAD scratch 构建已删除。
- 非硬 ×6：3 个在 HEAD 91ff731 上就失败（reset joins_with_original_first_error、direct 两个 retires_peer，并入 D153 列表）；3 个由 A1 引起（direct 两个 holds_unretired、reset keeps_original_debt_to_parent_q，m-a1 下通过）。没有任何 gate 组选中这 6 个。由 A1 引起的结局：退役时 `rebuild.rs:340` 因 Session 状态为 Incomplete 发布 Q（≤P），模型里还没有这一行。
- 下一步（D169 已授权）：(a) 读懂 legacy 状态为何是 Incomplete，然后补模型行 T10 并对齐 3 个非硬行（×10，m-a1 红）；若不合法则停下并给出修复计划；(b) 在完整 `native_n4_` 组内跑 M6 ×5，然后做 m6-nocredit；(c) joint ×2 + 全门（新前缀）+ 最终裁决文档（design §12、STATUS、TODO、LEARNINGS）。
- raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-d169-20261110/`。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-10，D171）

- 位置：**未提交。** D171：探针（源码已还原，sha OK），无生产/测试改动，只有文档。
- **停止原因**：非硬 Incomplete 结局不合法。`release.rs` 的 `Turn::drop → quarantine_release` 做软 Q，因为 `roles/native.rs::release_ready` 要求 `producers == Some(true)`，而只有硬（有预算）source 才会取走并处置 producers。非硬 Native flow 永远不能干净释放（a9e7f2f 起就存在）。A1 只是把非硬 held 行带过 parent 期限，撞上这个缝。HEAD 失败的 `app_abort_retires_peer` 也是同一个缝。3 个 HEAD 失败行已记入 D153 列表。
- 待批：**R1（推荐）** 只改 `native.rs::release_ready`：硬 ⇒ `producers == Some(true)`，非硬 ⇒ `producers.is_none()`（hard = `backing_meter.budget_limit().is_some()`）；先红后绿（direct 非硬 retires_peer ×2），m-r1 变异红；再探并对齐 3 个 A1 非硬行（T7|T6，reset 另含 parent 前 Q）×10，m-a1 红。**R2**：rebuild 校验拒绝非硬 Native（产品决定）。之后 D169 步骤 2–3（M6 门负载 ×5 + m6-nocredit；joint ×2 + 全门 + 裁决，含 PROVIDER_LOSS_BOUND 复审点）。
- raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-d171-20261110/`。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-11，D174）

- 位置：**未提交。** D172 = 用户 "R1: fix release check"。生产改动（N4 累计）= D147/D152/D154/D165 + D172 `roles/native.rs::release_ready`（按 profile 双向检查 producers）。测试改动：direct_local_cancel 非硬 held 并入 T7\|T6 分支；chunk_loaded_reset 非硬 keeps_original_debt 接受 E0\|T7\|T6。
- 结果：retires_peer ×2 红→绿（m-r1 = 回退 ⇒ 红）；3 个 A1 非硬行 ×10 绿，m-a1 下 direct 行红（reset 行 E0 不敏感）；M6 组内 ×5 绿，m6-nocredit 红；全门 `n4-gate3-20261110` joint ×2 843/843 绿，n4 10/11（M6 间歇 drain 停顿，NEW 既有），preexisting = D153 的 8 个，nonhard 5/6（`joins_with_original_first_error` 留在 D153 列表）。
- 裁决：设计 §13。M1–M5、M7 接受；M6 交付开放。开放项：M6 停顿 owner 探针（65535B = 一个 64KiB 窗口）；PROVIDER_LOSS_BOUND 复审。
- 下一：父会话开独立 N4 审查。raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-d172-20261110/`、`.../n4-gate3-20261110/`。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4 审查跟进，2026-11-11，D175–D178）

- 位置：**未提交。** 只改测试与文档，无生产改动；两个临时探针（`rearm/shutdown.rs` [DEBUG-p1]、`native_chunk_loaded_reset.rs` [DEBUG-f3]）已删除，sha 还原核对通过。
- 测试改动：`native_waiting::Rig.links`；`native_n4_harness::Bulk.handle` pub(super)；M6 打印链路丢包与未完成流的 App 视图；`native_rebuild_high_rtt::Tap::one_way`；`native_n4_peer::DELAYED_RESET_PORT`；新叶 `native_n4_zero_window.rs`（F1 RED tracer #[ignore] + 对照）与 `native_n4_one_way.rs`（F2 RED tracer #[ignore]）。
- 结论：F1 确认（Service 端 smoltcp 无 persist）；F2 在对端 RESET 触发的关闭尾上确认（关闭+35s sticky Q）；F3 不是丢包形态，是健康连接上 bi-stream 退役不完成的缺陷候选。
- 待用户：F1 选 (a)/(b)；F2 选 (1)/(2)；F3 下一探针（quinn 流两半状态）。F4/F9 小修未做。
- raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-probes-20261111/`。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4 修复，2026-11-11，D179–D185）

- 位置：**未提交。** 生产改动：`third_party/smoltcp-0.10.0/src/socket/tcp.rs`（零窗口持续探测 + 单元测试）、`flow_custody/transaction.rs` PROVIDER_LOSS_BOUND 推导（1479 行）、`transaction/stop_deadline.rs`（F4）、`tuic.rs` TUIC_RECONNECT_TIMEOUT 改 pub(crate)（同行）。测试：zero_window 与 one_way 去掉 #[ignore]，close_tail 新 3 个单元 + 访问器，fixture a1_q_deadline 读生产界。
- 门 gate4：见 D185。停止原因：NEW-1（确定性，D181 引起）与 NEW-2（间歇，未归因）。
- 下一（需父会话/用户确认）：(1) NEW-1：探针确认结局是 T7（released、budget 0、deadline、first failure），是则按 D167 规则让该行接受精确 {T7, T6}，m-a1 验红；否则当缺陷停。(2) NEW-2：f_full 在 capacity-serial 负载下 ×10，对比去掉 D180 的二进制，归因后再定。(3) F3：对首个 Quarantined 写入点做回溯探针 → 确定性 tracer → 修复。然后重跑 joint×2、capacity-serial、nonhard。
- raw：`/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-probes-20261111/`、`.../n4-gate4-20261111/`。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-11，D186–D190）

- 位置：**未提交。** 本轮只改测试（`native_direct_loaded_reset.rs` AckHeld 分支对齐 {T7,T6}）与文档；所有探针已删、sha 核对通过。
- 结果：NEW-1 闭；NEW-2 非回归；gate4b joint 845/845、capacity-serial 19/19、nonhard 5/6、joint2 844/845（收尾 flake）。
- 待用户：F3 选 (A) 或 (B)（D189）。批准后：先写确定性 RED tracer（非硬 reset，读者终态后再到一条 lane DATA），再修，再跑 `native_eof_lane_order`、硬 ordered 行、nonhard、joint ×2。
- 不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

## 新会话交接（N4，2026-11-11，D191–D192，gate5 运行中）

- 位置：**未提交。** 本会话（D175–D192）生产改动：smoltcp 零窗口持续探测（D180）、PROVIDER_LOSS_BOUND 推导 41s（D181）、StopDeadline 最小 limit（D182）、F3 接缝 `FlowMirror::discard_late_data` + `remote_payload.rs` StaleRelayReset 调用（D191/D192）。测试：zero_window、one_way、lane-order 非硬 tracer、AckHeld 对齐 {T7,T6}、close_tail 新单元、M6 打印。所有探针/变异已还原（sha 核对）。
- 当前：全门 `n4-gate5-20261111` 在后台跑（raw `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n4-gate5-20261111/`，nonhard 组新增非硬 lane-order tracer）。完成后按组报告：joint/joint2 中 `native_rebuild_fatal::hard_native_old_drain_past_deadline_expires_without_successor` 即"joint ×2"复查（单跑 10/10 绿）。
- 已知门外既有失败（D172 基线同样失败，不在任何门组）：非硬 `native_direct_loaded_reset_holds_local_retirement_without_return_ack`、`native_permit_loaded_app_abort_with_held_peer_keeps_charged_root`。
- 下一：读 gate5 结果 → 更新 STATUS/DECISIONS（D193）→ 报告父会话；之后由用户决定提交。不授权：N3、N5/C8、TUN、VPS、WAN、sudo、新 key、commit/push、邮件；`certs/dev/ca-cert.srl` 不动。

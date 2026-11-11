# N1-NATIVE 当前导航

A 出口已于 2026-11-04 按用户批准改为 A1–A4，见 [A 出口修订](docs/current/n1-a-exit-amendment-20261104.md)；当前边界与下一门见 [STATUS](docs/current/STATUS.md)，唯一清单在[原计划](docs/current/n1-f1-implementation.md)顶部，exact/失败见[结果](docs/current/n1-resume-20261017-evidence/b560-result.md)，继续见[HANDOFF](HANDOFF.md)。A全闭才B再C；18/23/37、两guard CLOSED不变。

路线 2（诚实上界）已选，用户选测试注入小窗口。已完成并实测：两层水位预算、Node 协议层、shape 驱动传输配置、Service typed preflight、测试改写；联合 653/653。直连/roles 下行 App 层门控已闭（D10，app_tier_view）。已闭：D12 failstop（D14）、k/c 实测与 A2_MEASURED preflight（D16–D18）。A1 闭合清单已闭（D19，[结果](docs/current/n1-a1-closed-list-20261105.md)）。F4096 Node 归因已测（D22）：Node 域峰值 1.72GB > 硬额 816.6MB（D23，未计费 smoltcp socket 载荷 553.9MB 等）→ 用户选 (a)：§5.3 上界加 I_cfg 已闭（D25，单跑余量≈101MB）；A §5.5 独立只读审查（含 D22 方法）；Proto fork 间歇失败已闭（D20 根因随机 RNG、D21 admission fixture 固定种子，20/20）；空闲机器复跑联合确认 D11 pressure 负载敏感。

**2026-11-05 A 关闭（D27–D29，门 `n1-a-o1o4-gate2-20261105-*`）**：O1/O4 已闭；联合门为单一联合 661 项（命令见 raw `n1-a-o1o4-gate2-20261105-joint-commands.json`，D13 串行组取消，isolated 仅附加证据）。**下一：阶段 B（独立会话）**。开放项 O2、O3、O5、O6。

**2026-11-06 B3+B4a 已闭（D35–D36）**：联合门 667 项（b4 joint 命令 = b3 joint + 过滤器 `tuic_service::tests::native_rebuild_fatal::`，见 raw `n1-b4-gate-20261106-joint-commands.json`）。下一 B4b，再 B5–B7（计划顶部"B 出口"）。

**2026-11-06 B1–B7 已闭（D43）**：联合门 675 项（命令见 raw `n1-b7-gate-20261106-joint-commands.json`）。下一 B8 判定记录（交 C）+ 独立只读审查（见 HANDOFF 新会话交接）。

**2026-11-06 C1+C2 已闭（D51–D54）**：联合门 680 项（命令 raw `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-c1-gate-20261106-joint-commands.json`）。下一 C3（见 HANDOFF 新会话交接）。

**2026-11-06 C3 丢包片+F4 已闭（D55–D59）**：联合门 685 项（命令 raw `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/n1-c3-gate-20261106-joint-commands.json`）。下一 F5，再 C5/C6（见 HANDOFF 新会话交接）；B8 是否打开待用户定产品 RTT/丢包目标（D57）。

**2026-11-06 F5 跑过（D60–D61）**：处置=粘性 Q（同 StopHeld）；D61-S session loop 自唤醒空转（poll_flows O(4096)/tick，debug CPU 0.28 稳态/0.78 持有与 Q 后）。下一：用户批准 D61 修复（R-a 槽位图 + R-b 无到期 Pending）→ F5 RED→GREEN + 全门；然后 C5/C6（见 HANDOFF 新会话交接）。
- 2026-11-06 D62：用户批准 D61 修复（R-a/R-b + 无丢唤醒 tracer + CPU 前后 + 全门/归因）；实施交新会话（HANDOFF 末段）。

**2026-11-06 D61 修复（D63–D65，未提交）**：SlotTable 停驻 + census 50ms；F5 判据改为 wake-ups/5ms tick ≤2（D65，CPU 粗界 ≤0.25）。剩：变异、全门、I_cfg +528B（见 DECISIONS 后续条目）。**开放项 C-B**（未实施，需 flow_custody 外写轴授权）：稳态每次 session loop 唤醒 ≈250µs debug CPU 未归因（D64），任何 WAN 吞吐声明前须归因。然后 C5/C6。
- 2026-11-06 D75：loaded_reset 间歇 = 读者 remote_read_failed 越过 lane DATA（D72 同类）；守卫扩为两种读者终态 + reset tracer；全门绿。剩：用户决定提交；C5/C6；C-B、C-M3。
- 2026-11-07 D89–D90：C7 跑过 ⇒ BLOCKED-ENV（full30 0/30、fixed18 0/18，全部 D9 server-key.pem NotFound，0 NEW）。待用户选 K2（fixture 改进程内 rcgen，建议）/K1（临时 PKI）后按 D89 原样重跑；C9 brief 已备（依 C7）。
- 2026-11-07 D91–D92：K2 已实施（full_fixture 进程内 rcgen + 临时 ca.pem）；C7 重跑 STOP：栈溢出中止（2MiB，16MiB 绿）+ 并行快速 FAIL（消息丢失）。待批 D92 S1（Box 身份判别）→S2（Box::pin 场景 future）→S3（加 --nocapture 重跑 D89）→S4 全门。
- 2026-11-07 D93–D95：S1+S2 消除栈溢出；S3 STOP：D11 类 25 例（16/16 隔离绿）+ NEW 4 例 AlreadyExists（D91 临时目录命名缺陷）。待批 D95 F1（重试建目录+module_path）→F2 重跑→F3 全门；再定 C7 判定（D11 记录 vs 结构 R-FULL-MATRIX-RUNNER）。
- 2026-11-07 D96–D97：F1 临时目录修复；C7 仅 D11 类（28 例，15/15 隔离绿），0 NEW；全门 n1-d96-gate 绿。待用户定 C7：(i) D11 记录关闭 → C9；(ii) 结构 R-FULL-MATRIX-RUNNER。
- 2026-11-07 D98：C7 以 D11 记录关闭（用户接受）。下一 C9（父会话开）。
- 2026-11-07 D99：C9 独立只读审查完成，C1–C7 无 FAIL（[审查](docs/current/n1-c-review-20261107.md)）；C6 necessary-only，C7 D11 记录非 PASS。
- 2026-11-07 D100：F1 门加无 feature production check、F4 文档同步；全门 n1-d100-gate-20261107；bd18abb 以来整个 N1 dirty 树单一本地提交（用户"Commit whole tree"，无 push）。仍待：C4（Node 写轴授权）、C8（外部授权）、C-B、B8（D57）、F2/F3（下次触碰 full_* 夹具）；push 未授权。日志 D101+。
- 2026-11-07 D101–D103：C4 已闭（Node 写轴仅 C4 授权；4 tracer + 变异 RED；全门 n1-c4-gate-20261107 joint 696/696；生产语义不变，未提交）。仍待：C8、C-B、B8（D57）、F2/F3；commit/push 未授权。日志 D104+。
- 2026-11-08 D127–D129：N2 合同修订（干净到期 ⇒ Waiting）；探针 Q 为夹具假象（F-N2a 另记）；Waiting 已实施，A1/A2/A6/A7 绿 + 变异 RED。剩：两个旧规则 N1 tracer 修订、A3–A5、governance、14 组全门（见 HANDOFF 末段）。
- 2026-11-08 D130–D132：N1 transient tracer 按 D127 修订；A3/A4/A5 绿 + 变异 RED；governance 绿；全门 12/14（joint ×2 716/717，唯一失败为第三个旧规则 tracer `udp_source::recovery::actual_hard_node_recovery_refusal_is_quiet…`，NEW 确定性）。待批：P1 修订该 tracer、P2 修 F-N2b（非 active closed demand 也进入 Waiting；pool=2 tracer）、P3 重跑 joint×2 + capacity-serial。开放：F-N2a（flow 关闭尾 Q，非 N2）、F-N2c（per-flow 失败记为会话 first_failure，N4）。
- 2026-11-09 D133：P1 绿；F-N2b 生产修复已写（tracer 未补）；停止：D129 树上 6 个 node tracer 早已失败但不在门禁里（2 个疑似真实生命周期缺陷：primary_recovery endpoint 未 drain、shutdown 超时）。下一：扩大 joint 过滤 → 诊断 → 分类 → P2 tracer → P3。
- 2026-11-09 D134–D139：门 joint 过滤已扩大；6 个 node 失败全部分类转绿（2 个既有 N1 缺陷：wait_sealed 修复 + guarded 旧断言；主槽 UDP 未 seal ⇒ 终态；3 个 failover 旧规则）；P2 deadline tracer/A2-pool2 绿，A6-pool2 RED ⇒ F-N2d（aux open 在 Recovering 中等原 15s demand）。待确认：D139 计划 → P3 全门。
- 2026-11-09 D140：F-N2d 假设证伪（flow open 期限 20s；慢 open 等的是 aux 新铸的原始 15s demand，属 N1 语义）。待用户选 (A) 修订 A6-pool2 断言或 (B) Node 已 Waiting 时新 closed demand 直接 Waiting；然后 P3。
- 2026-11-09 D141–D142：(B) 已实施：路径已知断开时，新的/排队的 closed demand 直接进入 Waiting；A6-pool2 4.92s；P3 全门 14/14 绿（joint ×2 790/790）。下一：N2 独立审查（待决定），提交待授权。
- 2026-11-09 D143–D144：独立审查 PASS-with-open-items；F1/F2/F3/F4/F5/F7/F12 文案已修，全门 n2-fixgate 14/14 绿（joint ×2 821/821），A6 负载下最慢 5.51s。下一：用户决定提交；开放 F6/F8–F11、F-N2a、F-N2c。
- 2026-11-09 D145–D146：N2 已接受（91ff731）。N4 设计出口已冻结；harness + M1/M5(iii) 测试已加；F-N2a 无需断网即复现（FIN+14.8s 会话 Q）⇒ 停止，待从 D146 的 (A)/(A')/(B)/(C) 中选择。M1 stop 后多 1 个 task，未归因。
- 2026-11-09 D147–D149：F-N2a 按 (A') 关闭（进度型关闭尾 + 单 flow 中止，其它到期仍 Q）；M5(iii)/慢读者/单元 tracer + 4 变异；M1 120s 绿、task 残留归因为测试 RttRelay；全门 14+n4 绿。剩：M1 批量流+变异、M2–M7、O-N2b release、全门重跑、N4 独立审查；提交未授权。
- 2026-11-09 D150：M2–M7 首轮。绿：M1 批量流（30s）、M6。RED：F-N4a M2 跨代分片重组泄漏（真实）；F-N4b M3/M4 负载下 Waiting 不恢复、stop 不完成（真实，结构性）；M5(ii) 自定阈值；M7 测试 link 问题。待批：D150 计划 (1)–(3)。未做：全部变异、M1 120s+批量、O-N2b、全门。
- 2026-11-09 D151：F-N4b 归因（证伪 predecessor 假设）：黑洞中关闭的短流 Retiring lease 等 quinn 见证，死连接存储挂在 Node 槽 `current`（Waiting 无界）⇒ 15s 关闭尾 ⇒ 会话 Q ⇒ stop 不完成。结构门：待选 D151 (A1+A2)/(B)/(C)/(D)。之后 F-N4a、测试修正、变异、O-N2b、全门 n4-gate2-。
- 2026-11-09 D152–D153：用户选 A1+A2（合同修订）。已实现：A1 provider-only 到期一次延长 QUIC idle+5s（provider 期限在首次 cancel 固定）；A2 Node 在 Failed∧settled 时释放死连接存储 + 物理条目（SlotConnection 墓碑），并修复 stable_id 地址复用。T1/T2/M4c 绿，变异 m-a1/m-a2 RED。M3 30s 恢复 3.6s。剩：F-N4a 单周期 tracer（假设：M2 测试同代配对伪迹）；M3 慢段 dcids 2 + 8s 恢复 >2s；F-N4d（M4a/b 原始 demand 期间 open 等 10s，需决定）；M5(ii)、M7；各格变异、M1 120s、O-N2b、全门 n4-gate2-。8 个基线复现的既有未门禁失败另立。
- 2026-11-09 D154–D155：F-N4d 已修（Node open 5s 界）；F-N4a 关闭（测试伪迹）；M2/M4/M5 绿；M3 只剩 8s 黑洞 2s 界（PTO 物理不可达）⇒ 待批修订 ≤H+1s。之后：M7 link、各格变异、M1 120s+批量、O-N2b n=5、governance/fmt、全门 `n4-gate2-`、文档、父会话开 N4 审查。
- 2026-11-10 D157–D159：M3 按 D156 实现（≤H+1s + PTO 探测断言，变异 RED）；M7 启动修复 + 判据按用户 A 修订（主接收 ≤128、总 ≤512），绿 + 去 b2 变异 RED；O-N2a 补记 timer/deferred drain ≤256。剩：M1/M2/M4/M5/M6 变异、M1 120s+批量、O-N2b release n=5、fmt、全门 `n4-gate2-`、文档、N4 独立审查。
- 2026-11-10 D163–D164：D162 已批。7 个 joint tracer 对齐 A1，m-a1 下红；M6 单独跑 ×5 绿（未复现）。停止待批：(a) direct_local_cancel ×2 改为期望干净结算；(b) A1 的 `certain` 排除已取致命 disposal 的 flow（transaction.rs）；(c) M6 在 n4 组负载下跑 ×5，再选变异；(d) joint×2 + 全门 + 终版文档。
- 2026-11-10 D165–D166：关闭尾模型 T1–T9 + `StopDeadline` 单 owner（TDD + 变异）；reset 硬行 T8\|T9 10/10 绿。停止待批：(1) 探 direct 失败结局的 Q 时间与 owner；(2a) T8\|T7 析取，或 (2b) 修订合同，让 egress 期限让给 A1；(3) 非硬 ×6 基线失败另立。之后 M6（门负载）+ 变异，joint×2 + 全门 + 最终 N4 裁决。
- 2026-11-10 D171：非硬 Incomplete = `native.rs::release_ready` 要求 `producers == Some(true)`，非硬 Native flow 永远不能干净释放（既有缝，A1 暴露）。3 个 HEAD 失败行并入 D153 列表。停止待批 R1（按 profile 双向 producers 谓词）或 R2（拒绝非硬 Native）。之后 M6 门负载 + 变异、joint ×2 + 全门 + 裁决。
- 2026-11-11 D172–D174：R1 已落地（非硬 Native 可干净释放），3 个 A1 非硬行对齐 ×10，M6 组内 ×5 + m6-nocredit 红，全门 n4-gate3 joint ×2 843/843。裁决：M1–M5、M7 接受，M6 交付开放（间歇 drain 停顿）。待独立 N4 审查；开放：M6 停顿 owner、PROVIDER_LOSS_BOUND 复审、`joins_with_original_first_error` + D153 的 8 个。
